use crate::*;

use aws_smithy_runtime_api::box_error::BoxError;
use aws_smithy_runtime_api::client::interceptors::Intercept;
use aws_smithy_runtime_api::client::interceptors::context::Input;
use aws_smithy_runtime_api::client::interceptors::context::{
    BeforeDeserializationInterceptorContextMut, BeforeSerializationInterceptorContextMut,
    BeforeTransmitInterceptorContextMut, BeforeTransmitInterceptorContextRef,
};
use aws_smithy_runtime_api::client::runtime_components::RuntimeComponents;
use aws_smithy_types::config_bag::ConfigBag;
use aws_smithy_types::config_bag::{Storable, StoreReplace};
use std::collections::HashMap;
use std::sync::Arc;
use url::Url;

use crate::keyrouting::affinity_config::KeyRouteAffinityConfig;
use crate::keyrouting::classifier;
use crate::keyrouting::hasher;
use crate::keyrouting::resolver;

/// Driver's main interceptor
///
/// Is added by [AlternatorClient] to its inner Dynamodb client on construction.
///
/// Handles request compression, response compression negotiation and
/// decompression, header stripping, final user-agent handling, and request URI
/// selection from query plans.
///
/// Also checks [ConfigBag] for per-operation compression overrides left by
/// [AlternatorOverrideInterceptor] and for signing state used to preserve SigV4
/// headers when header stripping is enabled.
#[derive(Debug)]
pub(crate) struct AlternatorInterceptor {
    request_compression: RequestCompression,
    response_compression: ResponseCompression,
    optimize_headers: bool,
    user_agent: UserAgent,
    preserve_auth_headers: bool,
    preserve_float32_vectors: bool,
}
impl AlternatorInterceptor {
    pub fn new(
        request_compression: RequestCompression,
        response_compression: ResponseCompression,
        optimize_headers: bool,
        user_agent: UserAgent,
        preserve_auth_headers: bool,
        preserve_float32_vectors: bool,
    ) -> Self {
        Self {
            request_compression,
            response_compression,
            optimize_headers,
            user_agent,
            preserve_auth_headers,
            preserve_float32_vectors,
        }
    }
}
impl Intercept for AlternatorInterceptor {
    fn name(&self) -> &'static str {
        "AlternatorInterceptor"
    }

    fn modify_before_retry_loop(
        &self,
        context: &mut BeforeTransmitInterceptorContextMut,
        _: &RuntimeComponents,
        cfg: &mut ConfigBag,
    ) -> Result<(), BoxError> {
        // Inject any vector-search request extras (e.g. CreateTable's
        // VectorIndexes) into the serialized JSON body, and rewrite any
        // FLOAT32VECTOR marker binaries into `{"FLOAT32VECTOR": [...]}`.
        // This must happen before compression, so the order is always:
        // serialized JSON -> vector rewrite -> optional compression -> signing.
        let vector_request = cfg
            .interceptor_state()
            .load::<VectorRequestStore>()
            .cloned();
        let body_may_have_marker = context
            .request()
            .body()
            .bytes()
            .map(|b| {
                let s = String::from_utf8_lossy(b);
                s.contains(crate::float32_vector::quick_base64_signature())
            })
            .unwrap_or(false);
        if vector_request.is_some() || body_may_have_marker {
            rewrite_vector_request_body(context, vector_request.as_ref(), body_may_have_marker)?;
        }

        // check for overrides
        let request_compression = cfg
            .interceptor_state()
            .load::<RequestCompressionStore>()
            .map(|store| store.request_compression.clone())
            .unwrap_or(self.request_compression.clone());

        // message must be compressed before signing, but it's more efficient to do it before retry loop
        if let Some((algorithm, level, threshold)) = request_compression.get() {
            compress_request(context.request_mut(), algorithm, level, threshold);
        }

        // Insert Accept-Encoding header if response compression is enabled
        let response_compression = cfg
            .interceptor_state()
            .load::<ResponseCompressionStore>()
            .map(|store| store.response_compression.clone())
            .unwrap_or(self.response_compression.clone());

        if let Some(algorithms) = response_compression.get() {
            let value = accept_encoding_header_value(algorithms);
            context
                .request_mut()
                .headers_mut()
                .insert("accept-encoding", value);
        }

        Ok(())
    }

    fn modify_before_transmit(
        &self,
        context: &mut BeforeTransmitInterceptorContextMut,
        _: &RuntimeComponents,
        cfg: &mut ConfigBag,
    ) -> Result<(), BoxError> {
        let preserve_auth_headers = cfg
            .interceptor_state()
            .load::<PreserveAuthHeadersStore>()
            .map(|store| store.preserve_auth_headers)
            .unwrap_or(self.preserve_auth_headers);
        // optimize headers
        if self.optimize_headers {
            strip_headers(context.request_mut(), preserve_auth_headers);
        }
        apply_user_agent(context.request_mut(), &self.user_agent)?;

        Ok(())
    }

    fn read_after_signing(
        &self,
        context: &BeforeTransmitInterceptorContextRef<'_>,
        _: &RuntimeComponents,
        cfg: &mut ConfigBag,
    ) -> Result<(), BoxError> {
        let headers = context.request().headers();
        if headers.contains_key("authorization") && headers.contains_key("x-amz-date") {
            cfg.interceptor_state().store_put(PreserveAuthHeadersStore {
                preserve_auth_headers: true,
            });
        }

        Ok(())
    }

    fn modify_before_signing(
        &self,
        context: &mut BeforeTransmitInterceptorContextMut<'_>,
        _: &RuntimeComponents,
        cfg: &mut ConfigBag,
    ) -> Result<(), BoxError> {
        // Take the next node from the query plan and override the request URI.
        if let Some(query_plan) = cfg.interceptor_state().load::<QueryPlan>()
            && let Some(next_node) = query_plan.next_node()
        {
            let request = context.request_mut();
            let mut current = url::Url::parse(request.uri())?;
            current
                .set_scheme(next_node.scheme())
                .map_err(|_| "cannot set scheme")?;
            current
                .set_host(next_node.host_str())
                .map_err(|_| "cannot set host")?;
            current
                .set_port(next_node.port())
                .map_err(|_| "cannot set port")?;

            request.set_uri(current.as_str())?;
        }

        Ok(())
    }

    fn modify_before_deserialization(
        &self,
        context: &mut BeforeDeserializationInterceptorContextMut<'_>,
        _: &RuntimeComponents,
        cfg: &mut ConfigBag,
    ) -> Result<(), BoxError> {
        let response = context.response_mut();

        // Collect all Content-Encoding header values (may be repeated headers
        // or comma-separated within a single header value).
        let mut algorithms = Vec::new();
        for header_value in response.headers().get_all("content-encoding") {
            for token in header_value.split(',').map(|s| s.trim()) {
                if token.is_empty() {
                    continue;
                }
                match ResponseCompressionAlgorithm::from_content_encoding(token) {
                    Some(algo) => algorithms.push(algo),
                    None => {
                        return Err(format!(
                            "unsupported Content-Encoding: '{}'. Supported encodings are: gzip, deflate",
                            token
                        )
                        .into());
                    }
                }
            }
        }

        let was_compressed = !algorithms.is_empty();

        if was_compressed {
            // Take the body and wrap it with decompression
            let body = std::mem::replace(
                response.body_mut(),
                aws_smithy_types::body::SdkBody::empty(),
            );
            let decompressed_body = crate::decompression::wrap_decompressed_body(body, algorithms)?;
            *response.body_mut() = decompressed_body;

            // Strip Content-Encoding and Content-Length headers
            response.headers_mut().remove("content-encoding");
            response.headers_mut().remove("content-length");
        }

        // Vector response transformation: rewrite FLOAT32VECTOR attributes
        // (to L/N by default, or marker B when preserving) and extract
        // Scores/VectorIndexes into any registered per-request response
        // holder. Only successful responses are eligible; error bodies are
        // left untouched so a service error is never turned into a local
        // JSON error.
        if response.status().is_success() {
            let holder = cfg
                .interceptor_state()
                .load::<VectorResponseStore>()
                .map(|store| store.0.clone());

            // Precedence: per-operation override, then client configuration,
            // then `false`.
            let preserve_float32_vectors = cfg
                .interceptor_state()
                .load::<PreserveFloat32VectorsStore>()
                .map(|store| store.preserve_float32_vectors)
                .unwrap_or(self.preserve_float32_vectors);

            let body = std::mem::replace(
                response.body_mut(),
                aws_smithy_types::body::SdkBody::empty(),
            );
            let transformed = crate::vector_response::wrap_vector_response_body(
                body,
                preserve_float32_vectors,
                holder,
            );
            *response.body_mut() = transformed;

            // The transformed body's byte length (and thus any prior
            // Content-Length) and checksums are now stale regardless of
            // whether a rewrite actually occurred, since that is only
            // known once the body is fully buffered.
            response.headers_mut().remove("content-length");
            response.headers_mut().remove("x-amz-crc32");
            response.headers_mut().remove("x-amz-crc32c");
            response.headers_mut().remove("x-amz-checksum-crc32");
            response.headers_mut().remove("x-amz-checksum-crc32c");
            response.headers_mut().remove("x-amz-checksum-sha1");
            response.headers_mut().remove("x-amz-checksum-sha256");
            response.headers_mut().remove("x-amz-checksum-crc64nvme");
        }

        Ok(())
    }
}

/// Identifies the DynamoDB/Alternator operation for a request from its
/// `x-amz-target` header, e.g. `DynamoDB_20120810.CreateTable`.
fn operation_name_from_target(target: &str) -> Option<&str> {
    target.split('.').next_back()
}

/// Injects vector-search request extras (`CreateTable.VectorIndexes`,
/// `UpdateTable.VectorIndexUpdates`, `Query.VectorSearch`) into the
/// serialized JSON request body, and recursively rewrites any
/// `FLOAT32VECTOR` marker binaries into `{"FLOAT32VECTOR": [...]}`.
///
/// If vector extras are attached to an operation that does not support
/// them, this returns a clear local error instead of silently ignoring
/// caller intent. If neither extras nor marker binaries apply, the body is
/// left byte-for-byte unchanged.
fn rewrite_vector_request_body(
    context: &mut BeforeTransmitInterceptorContextMut,
    vector_request: Option<&VectorRequestStore>,
    body_may_have_marker: bool,
) -> Result<(), BoxError> {
    let target = context
        .request()
        .headers()
        .get("x-amz-target")
        .unwrap_or_default()
        .to_string();
    let operation = operation_name_from_target(&target)
        .unwrap_or_default()
        .to_string();

    let body = context
        .request_mut()
        .body_mut()
        .bytes()
        .ok_or("body not collected")?
        .to_vec();

    let mut json: serde_json::Value =
        serde_json::from_slice(&body).map_err(|e| format!("failed to parse JSON body: {e}"))?;

    let mut changed = false;

    if let Some(vector_request) = vector_request {
        if let Some(vector_indexes) = vector_request.vector_indexes.as_ref() {
            if operation != "CreateTable" {
                return Err(format!(
                    "vector_indexes() was set on a customize() call for operation '{operation}', \
                     but VectorIndexes is only supported on CreateTable requests"
                )
                .into());
            }
            let indexes: Vec<serde_json::Value> = vector_indexes
                .iter()
                .map(|idx| {
                    let json_idx: crate::vector::VectorIndexJson = idx.into();
                    serde_json::to_value(&json_idx)
                        .expect("VectorIndexJson serialization should not fail")
                })
                .collect();
            json["VectorIndexes"] = serde_json::Value::Array(indexes);
            changed = true;
        }

        if let Some(updates) = vector_request.vector_index_updates.as_ref() {
            if operation != "UpdateTable" {
                return Err(format!(
                    "vector_index_updates() was set on a customize() call for operation \
                     '{operation}', but VectorIndexUpdates is only supported on UpdateTable requests"
                )
                .into());
            }
            if updates.len() != 1 {
                return Err(format!(
                    "UpdateTable.VectorIndexUpdates must contain exactly one update per \
                     request, got {}",
                    updates.len()
                )
                .into());
            }
            if json
                .get("GlobalSecondaryIndexUpdates")
                .and_then(|v| v.as_array())
                .is_some_and(|arr| !arr.is_empty())
            {
                return Err(
                    "vector-index updates cannot be combined with GlobalSecondaryIndexUpdates \
                     in the same UpdateTable request"
                        .into(),
                );
            }
            let updates: Vec<serde_json::Value> = updates
                .iter()
                .map(|u| {
                    let json_u: crate::vector::VectorIndexUpdateJson = u.into();
                    serde_json::to_value(&json_u)
                        .expect("VectorIndexUpdateJson serialization should not fail")
                })
                .collect();
            json["VectorIndexUpdates"] = serde_json::Value::Array(updates);
            changed = true;
        }

        if let Some(search) = vector_request.vector_search.as_ref() {
            if operation != "Query" {
                return Err(format!(
                    "vector_search() was set on a customize() call for operation '{operation}', \
                     but VectorSearch is only supported on Query requests"
                )
                .into());
            }
            let json_search: crate::vector::VectorSearchJson = search.into();
            json["VectorSearch"] = serde_json::to_value(&json_search)
                .expect("VectorSearchJson serialization should not fail");
            changed = true;

            validate_vector_query_request(&json, search.return_scores.is_some())?;
        }
    }

    // The compact query-vector form injects its own marker binary (see
    // `VectorSearch::new`) as part of `json` above, which the initial
    // `body_may_have_marker` scan (taken before that insertion) cannot have
    // seen, so it must also trigger the marker rewrite pass.
    let vector_search_may_have_marker =
        vector_request.is_some_and(|vector_request| vector_request.vector_search.is_some());

    if body_may_have_marker || vector_search_may_have_marker {
        let before = json.clone();
        crate::float32_vector::rewrite_request_json_markers(&mut json);
        if json != before {
            changed = true;
        }
    }

    if !changed {
        return Ok(());
    }

    let new_body =
        serde_json::to_vec(&json).map_err(|e| format!("failed to serialize JSON body: {e}"))?;

    context
        .request_mut()
        .headers_mut()
        .insert("content-length", new_body.len().to_string());
    *context.request_mut().body_mut() = new_body.into();

    Ok(())
}

/// Validates a generated `Query` request body when `VectorSearch` is
/// present, returning a descriptive local error instead of letting an
/// unsupported combination reach the server. Schema-dependent validation
/// (index existence, dimensionality) remains server-owned.
fn validate_vector_query_request(
    json: &serde_json::Value,
    has_return_scores: bool,
) -> Result<(), BoxError> {
    let index_name = json.get("IndexName").and_then(|v| v.as_str());
    if index_name.is_none_or(str::is_empty) {
        return Err("vector-search queries require IndexName".into());
    }

    let limit = json.get("Limit").and_then(|v| v.as_i64());
    match limit {
        None => return Err("vector-search queries require Limit".into()),
        Some(limit) if !(1..=1000).contains(&limit) => {
            return Err(format!(
                "vector-search queries require Limit between 1 and 1000, got {limit}"
            )
            .into());
        }
        _ => {}
    }

    if json.get("ConsistentRead").and_then(|v| v.as_bool()) == Some(true) {
        return Err("vector-search queries do not support ConsistentRead(true)".into());
    }

    if json.get("ExclusiveStartKey").is_some_and(|v| !v.is_null()) {
        return Err("vector-search queries do not support ExclusiveStartKey".into());
    }

    if json.get("ScanIndexForward").is_some() {
        return Err("vector-search queries do not support ScanIndexForward".into());
    }

    if json.get("QueryFilter").is_some_and(|v| !v.is_null()) {
        return Err("vector-search queries do not support the legacy QueryFilter".into());
    }

    if has_return_scores && json.get("Select").and_then(|v| v.as_str()) == Some("COUNT") {
        return Err("ReturnScores::Similarity is not supported with Select::Count".into());
    }

    Ok(())
}

#[derive(Debug, Clone)]
pub(crate) struct RequestCompressionStore {
    request_compression: RequestCompression,
}
impl Storable for RequestCompressionStore {
    type Storer = StoreReplace<Self>;
}

#[derive(Debug, Clone)]
pub(crate) struct ResponseCompressionStore {
    pub(crate) response_compression: ResponseCompression,
}
impl Storable for ResponseCompressionStore {
    type Storer = StoreReplace<Self>;
}

#[derive(Debug, Clone)]
pub(crate) struct PreserveAuthHeadersStore {
    preserve_auth_headers: bool,
}
impl Storable for PreserveAuthHeadersStore {
    type Storer = StoreReplace<Self>;
}

#[derive(Debug, Clone)]
pub(crate) struct PreserveFloat32VectorsStore {
    pub(crate) preserve_float32_vectors: bool,
}
impl Storable for PreserveFloat32VectorsStore {
    type Storer = StoreReplace<Self>;
}

/// An interceptor used to carry one per-operation Alternator override.
///
/// Adds the specified override value to [ConfigBag], so that
/// [AlternatorInterceptor] can apply it later in the request lifecycle.
///
/// Is used by [AlternatorCustomizableOperation] for per-operation compression
/// customization.
#[derive(Debug)]
pub(crate) struct AlternatorOverrideInterceptor<T: Storable<Storer = StoreReplace<T>> + Clone> {
    store: T,
}
impl<T: Storable<Storer = StoreReplace<T>> + Clone> Intercept for AlternatorOverrideInterceptor<T> {
    fn name(&self) -> &'static str {
        "AlternatorOverrideInterceptor"
    }

    fn modify_before_serialization(
        &self,
        _: &mut BeforeSerializationInterceptorContextMut,
        _: &RuntimeComponents,
        cfg: &mut ConfigBag,
    ) -> Result<(), BoxError> {
        // update config bag, so that AlternatorInterceptor will later include the override
        cfg.interceptor_state().store_put(self.store.clone());

        Ok(())
    }
}
impl AlternatorOverrideInterceptor<RequestCompressionStore> {
    pub(crate) fn for_request_compression(request_compression: RequestCompression) -> Self {
        AlternatorOverrideInterceptor {
            store: RequestCompressionStore {
                request_compression,
            },
        }
    }
}
impl AlternatorOverrideInterceptor<ResponseCompressionStore> {
    pub(crate) fn for_response_compression(response_compression: ResponseCompression) -> Self {
        AlternatorOverrideInterceptor {
            store: ResponseCompressionStore {
                response_compression,
            },
        }
    }
}
impl AlternatorOverrideInterceptor<PreserveFloat32VectorsStore> {
    pub(crate) fn for_preserve_float32_vectors(preserve_float32_vectors: bool) -> Self {
        AlternatorOverrideInterceptor {
            store: PreserveFloat32VectorsStore {
                preserve_float32_vectors,
            },
        }
    }
}
/// An interceptor that adds a round-robin [QueryPlan] to the config bag before request serialization,
/// so that [AlternatorInterceptor] can later use it to determine which node to send the request to.
#[derive(Debug)]
pub(crate) struct RoundRobinQueryPlanInterceptor {
    live_nodes: Arc<LiveNodes>,
}

impl RoundRobinQueryPlanInterceptor {
    pub fn new(live_nodes: Arc<LiveNodes>) -> Self {
        Self { live_nodes }
    }
}

impl Intercept for RoundRobinQueryPlanInterceptor {
    fn name(&self) -> &'static str {
        "RoundRobinQueryPlanInterceptor"
    }

    /// This hook is triggered exactly once per request, before the first attempt is serialized.
    /// Query plan, put here, is then used before every attempt by [`AlternatorInterceptor`] in `modify_before_signing`
    /// hook to determine which node the request should be sent to.
    /// This allows for tracking which nodes have already been tried in the current request and implementing a round-robin strategy.
    fn modify_before_serialization(
        &self,
        _: &mut BeforeSerializationInterceptorContextMut,
        _: &RuntimeComponents,
        cfg: &mut ConfigBag,
    ) -> Result<(), BoxError> {
        let query_plan = QueryPlan::new_basic(self.live_nodes.clone());
        cfg.interceptor_state().store_put(query_plan);

        Ok(())
    }
}

#[derive(Debug)]
pub(crate) struct AffinityQueryPlanInterceptor {
    config: KeyRouteAffinityConfig,
    live_nodes: Arc<LiveNodes>,
    resolver: Arc<resolver::PartitionKeyResolver>,
}

/// An interceptor that builds a partition-key-aware [QueryPlan] for
/// qualifying requests, or a round-robin [QueryPlan] as a fallback.
///
/// On the first attempt of each request, [`modify_before_serialization`]
/// inspects the operation type, extracts the partition key if one applies,
/// and constructs an affinity [QueryPlan] so that subsequent retries prefer
/// related coordinators. Requests that don't qualify (read operations,
/// missing partition key info, unsupported PK types) get a basic round-robin
/// plan instead.
///
/// Partition key names are resolved lazily via [`PartitionKeyResolver`].
/// On a cache miss, the request falls back to round-robin and discovery
/// is triggered in the background for next time.
impl AffinityQueryPlanInterceptor {
    /// Creates the interceptor and pre-populates the cache with any static
    /// mappings provided by the user in the AlternatorConfig.
    pub fn new(
        config: KeyRouteAffinityConfig,
        live_nodes: Arc<LiveNodes>,
        resolver: Arc<resolver::PartitionKeyResolver>,
    ) -> Self {
        Self {
            config,
            live_nodes,
            resolver,
        }
    }

    fn candidate_partition_key_hash(
        &self,
        candidate: &classifier::PartitionKeyCandidate<'_>,
    ) -> Option<u64> {
        let pk_name = match self.resolver.get_partition_key(candidate.table_name) {
            Some(cached_name) => cached_name,
            None => {
                // CACHE MISS: Trigger background discovery.
                self.resolver.trigger_discovery(candidate.table_name);
                return None;
            }
        };

        let pk_value = candidate.attributes.get(pk_name.as_ref())?;
        hasher::hash_attribute_value(pk_value)
    }

    /// Tries to build an affinity-routed plan for `input`. Returns `None`
    /// when affinity doesn't apply or no usable partition key can be found. On
    /// cache miss this also triggers background PK discovery as a side effect.
    pub fn try_affinity_plan(&self, input: &Input) -> Option<QueryPlan> {
        if !self.config.is_enabled() {
            return None;
        }

        let op = classifier::DynamoOp::from_input(input)?;

        if !op.should_apply(self.config.affinity_type) {
            return None;
        }

        let is_batch_write = matches!(&op, classifier::DynamoOp::BatchWrite(_));
        let candidates = op.partition_key_candidates();

        if !is_batch_write {
            for candidate in candidates {
                let Some(hash) = self.candidate_partition_key_hash(&candidate) else {
                    continue;
                };

                return Some(QueryPlan::new_with_hash(self.live_nodes.clone(), hash));
            }

            return None;
        }

        let affinity_nodes = QueryPlan::sorted_affinity_nodes(&self.live_nodes);
        let mut votes: HashMap<Arc<Url>, usize> = HashMap::new();

        for candidate in candidates {
            let Some(hash) = self.candidate_partition_key_hash(&candidate) else {
                continue;
            };

            let preferred_node = affinity_nodes.preferred_node_for_hash(hash)?;
            *votes.entry(preferred_node).or_insert(0) += 1;
        }

        let preferred_nodes = vote_preference_order(votes)?;
        Some(QueryPlan::new_with_preferred_nodes(
            self.live_nodes.clone(),
            preferred_nodes,
        ))
    }

    /// Builds the [`QueryPlan`] for this request. Falls back to round-robin
    /// when affinity doesn't apply for any reason
    fn get_query_plan(&self, input: &Input) -> QueryPlan {
        self.try_affinity_plan(input)
            .unwrap_or_else(|| QueryPlan::new_basic(self.live_nodes.clone()))
    }
}

fn vote_preference_order(votes: HashMap<Arc<Url>, usize>) -> Option<Vec<Arc<Url>>> {
    let mut voted_nodes: Vec<_> = votes.into_iter().collect();
    if voted_nodes.is_empty() {
        return None;
    }

    voted_nodes.sort_unstable_by(|(left_node, left_count), (right_node, right_count)| {
        right_count
            .cmp(left_count)
            .then_with(|| left_node.as_str().cmp(right_node.as_str()))
    });

    Some(voted_nodes.into_iter().map(|(node, _)| node).collect())
}

impl Intercept for AffinityQueryPlanInterceptor {
    fn name(&self) -> &'static str {
        "AffinityQueryPlanInterceptor"
    }

    fn modify_before_serialization(
        &self,
        context: &mut BeforeSerializationInterceptorContextMut<'_>,
        _runtime_components: &RuntimeComponents,
        cfg: &mut ConfigBag,
    ) -> Result<(), aws_smithy_runtime_api::box_error::BoxError> {
        let input = context.input();
        let query_plan = self.get_query_plan(input);

        cfg.interceptor_state().store_put(query_plan);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::keyrouting::KeyRouteAffinityType;
    use aws_sdk_dynamodb::config::{BehaviorVersion, Region};
    use aws_sdk_dynamodb::operation::batch_write_item::BatchWriteItemInput;
    use aws_sdk_dynamodb::types::{AttributeValue, DeleteRequest, PutRequest, WriteRequest};
    use std::collections::HashMap;

    #[tokio::test]
    async fn vector_indexes_are_injected_before_compression_on_create_table() {
        use aws_sdk_dynamodb::types::{
            AttributeDefinition, BillingMode, KeySchemaElement, KeyType, ScalarAttributeType,
        };
        use aws_smithy_runtime::client::http::test_util::{ReplayEvent, StaticReplayClient};
        use aws_smithy_types::body::SdkBody;
        use std::io::Read;

        let http_client = StaticReplayClient::new(vec![ReplayEvent::new(
            http::Request::builder()
                .uri("http://127.0.0.1:1/")
                .body(SdkBody::empty())
                .unwrap(),
            http::Response::builder()
                .status(200)
                .body(SdkBody::from("{\"TableDescription\":{}}"))
                .unwrap(),
        )]);

        let config = aws_sdk_dynamodb::Config::builder()
            .behavior_version(BehaviorVersion::latest())
            .region(Region::new("us-east-1"))
            .endpoint_url("http://127.0.0.1:1")
            .credentials_provider(
                aws_sdk_dynamodb::config::Credentials::for_tests_with_session_token(),
            )
            .http_client(http_client.clone())
            .interceptor(AlternatorInterceptor::new(
                RequestCompression::enabled(
                    crate::compression::CompressionAlgorithm::Gzip,
                    crate::compression::CompressionLevel::default(),
                    0,
                ),
                ResponseCompression::disabled(),
                false,
                UserAgent::default(),
                true,
                false,
            ))
            .build();
        let client = aws_sdk_dynamodb::Client::from_conf(config);

        let va = crate::vector::VectorAttribute::builder()
            .attribute_name("embedding")
            .dimensions(128)
            .build()
            .unwrap();
        let vi = crate::vector::VectorIndex::builder()
            .index_name("vec_idx")
            .vector_attribute(va)
            .build()
            .unwrap();

        client
            .create_table()
            .table_name("test_table")
            .attribute_definitions(
                AttributeDefinition::builder()
                    .attribute_name("pk")
                    .attribute_type(ScalarAttributeType::S)
                    .build()
                    .unwrap(),
            )
            .key_schema(
                KeySchemaElement::builder()
                    .attribute_name("pk")
                    .key_type(KeyType::Hash)
                    .build()
                    .unwrap(),
            )
            .billing_mode(BillingMode::PayPerRequest)
            .customize()
            .vector_indexes(vec![vi])
            .send()
            .await
            .expect("request should succeed against the replay client");

        let requests = http_client.actual_requests().collect::<Vec<_>>();
        assert_eq!(requests.len(), 1, "exactly one request should be sent");
        let sent_request = &requests[0];

        // Request compression must have applied: content-encoding is set and
        // the body bytes are actually gzip-compressed.
        assert_eq!(
            sent_request.headers().get("content-encoding").unwrap(),
            "gzip"
        );

        let compressed = sent_request.body().bytes().expect("body collected");
        let mut decompressed = Vec::new();
        flate2::read::GzDecoder::new(compressed)
            .read_to_end(&mut decompressed)
            .expect("valid gzip body");

        let json: serde_json::Value =
            serde_json::from_slice(&decompressed).expect("decompressed body should be valid JSON");

        // The vector rewrite happened before compression: VectorIndexes is
        // present in the *decompressed* body.
        assert_eq!(json["VectorIndexes"][0]["IndexName"], "vec_idx");
        assert_eq!(
            json["VectorIndexes"][0]["VectorAttribute"]["AttributeName"],
            "embedding"
        );
        assert_eq!(json["TableName"], "test_table");
    }

    fn make_replay_client(
        response_body: &str,
    ) -> (
        aws_sdk_dynamodb::Client,
        aws_smithy_runtime::client::http::test_util::StaticReplayClient,
    ) {
        use aws_smithy_runtime::client::http::test_util::{ReplayEvent, StaticReplayClient};
        use aws_smithy_types::body::SdkBody;

        let http_client = StaticReplayClient::new(vec![ReplayEvent::new(
            http::Request::builder()
                .uri("http://127.0.0.1:1/")
                .body(SdkBody::empty())
                .unwrap(),
            http::Response::builder()
                .status(200)
                .body(SdkBody::from(response_body))
                .unwrap(),
        )]);

        let config = aws_sdk_dynamodb::Config::builder()
            .behavior_version(BehaviorVersion::latest())
            .region(Region::new("us-east-1"))
            .endpoint_url("http://127.0.0.1:1")
            .credentials_provider(
                aws_sdk_dynamodb::config::Credentials::for_tests_with_session_token(),
            )
            .http_client(http_client.clone())
            .interceptor(AlternatorInterceptor::new(
                RequestCompression::disabled(),
                ResponseCompression::disabled(),
                false,
                UserAgent::default(),
                true,
                false,
            ))
            .build();
        (aws_sdk_dynamodb::Client::from_conf(config), http_client)
    }

    #[tokio::test]
    async fn vector_index_updates_are_injected_on_update_table() {
        let (client, http_client) = make_replay_client("{\"TableDescription\":{}}");

        let va = crate::vector::VectorAttribute::builder()
            .attribute_name("embedding")
            .dimensions(128)
            .build()
            .unwrap();
        let vi = crate::vector::VectorIndex::builder()
            .index_name("vec_idx")
            .vector_attribute(va)
            .build()
            .unwrap();

        client
            .update_table()
            .table_name("test_table")
            .customize()
            .vector_index_updates(vec![crate::vector::VectorIndexUpdate::Create(vi)])
            .send()
            .await
            .expect("request should succeed against the replay client");

        let requests = http_client.actual_requests().collect::<Vec<_>>();
        let json: serde_json::Value =
            serde_json::from_slice(requests[0].body().bytes().unwrap()).unwrap();
        assert_eq!(
            json["VectorIndexUpdates"][0]["Create"]["IndexName"],
            "vec_idx"
        );
    }

    // `vector_index_updates()` is only available on `UpdateTable`'s
    // `customize()` (see the operation-specific traits in
    // `vector_interceptor.rs`), so calling it on `CreateTable` is a compile
    // error. See the compile-fail doctest on `UpdateTableVectorExt` for
    // coverage.

    #[tokio::test]
    async fn vector_search_is_injected_on_query() {
        let (client, http_client) = make_replay_client("{\"Items\":[],\"Count\":0}");

        let search = crate::vector::VectorSearch::new(vec![1.0, 2.0, 3.0])
            .unwrap()
            .with_return_scores(crate::vector::ReturnScores::Similarity);

        client
            .query()
            .table_name("test_table")
            .index_name("embedding_idx")
            .limit(10)
            .customize()
            .vector_search(search)
            .send()
            .await
            .expect("request should succeed against the replay client");

        let requests = http_client.actual_requests().collect::<Vec<_>>();
        let json: serde_json::Value =
            serde_json::from_slice(requests[0].body().bytes().unwrap()).unwrap();
        assert_eq!(
            json["VectorSearch"]["QueryVector"]["FLOAT32VECTOR"],
            serde_json::json!([1.0, 2.0, 3.0])
        );
        assert_eq!(json["VectorSearch"]["ReturnScores"], "SIMILARITY");
    }

    #[tokio::test]
    async fn vector_index_delete_is_injected_on_update_table() {
        let (client, http_client) = make_replay_client("{\"TableDescription\":{}}");

        client
            .update_table()
            .table_name("test_table")
            .customize()
            .vector_index_updates(vec![crate::vector::VectorIndexUpdate::Delete {
                index_name: "vec_idx".to_string(),
            }])
            .send()
            .await
            .expect("request should succeed against the replay client");

        let requests = http_client.actual_requests().collect::<Vec<_>>();
        let json: serde_json::Value =
            serde_json::from_slice(requests[0].body().bytes().unwrap()).unwrap();
        assert_eq!(
            json["VectorIndexUpdates"][0]["Delete"]["IndexName"],
            "vec_idx"
        );
        assert!(json["VectorIndexUpdates"][0].get("Create").is_none());
    }

    #[tokio::test]
    async fn vector_search_from_query_vector_uses_standard_list_on_wire() {
        let (client, http_client) = make_replay_client("{\"Items\":[],\"Count\":0}");

        let search = crate::vector::VectorSearch::from_query_vector(vec![
            AttributeValue::N("1".to_string()),
            AttributeValue::N("2".to_string()),
            AttributeValue::N("3".to_string()),
        ])
        .unwrap();

        client
            .query()
            .table_name("test_table")
            .index_name("embedding_idx")
            .limit(10)
            .customize()
            .vector_search(search)
            .send()
            .await
            .expect("request should succeed against the replay client");

        let requests = http_client.actual_requests().collect::<Vec<_>>();
        let json: serde_json::Value =
            serde_json::from_slice(requests[0].body().bytes().unwrap()).unwrap();
        assert_eq!(
            json["VectorSearch"]["QueryVector"]["L"],
            serde_json::json!([{ "N": "1" }, { "N": "2" }, { "N": "3" }])
        );
        assert!(
            json["VectorSearch"]["QueryVector"]
                .get("FLOAT32VECTOR")
                .is_none()
        );
    }

    #[tokio::test]
    async fn vector_query_rejects_missing_index_name() {
        let (client, http_client) = make_replay_client("{\"Items\":[],\"Count\":0}");
        let search = crate::vector::VectorSearch::new(vec![1.0]).unwrap();

        let err = client
            .query()
            .table_name("test_table")
            .limit(10)
            .customize()
            .vector_search(search)
            .send()
            .await
            .expect_err("request should be rejected locally");
        assert!(format!("{err:?}").contains("require IndexName"));
        assert!(http_client.actual_requests().next().is_none());
    }

    #[tokio::test]
    async fn vector_query_rejects_missing_limit() {
        let (client, http_client) = make_replay_client("{\"Items\":[],\"Count\":0}");
        let search = crate::vector::VectorSearch::new(vec![1.0]).unwrap();

        let err = client
            .query()
            .table_name("test_table")
            .index_name("embedding_idx")
            .customize()
            .vector_search(search)
            .send()
            .await
            .expect_err("request should be rejected locally");
        assert!(format!("{err:?}").contains("require Limit"));
        assert!(http_client.actual_requests().next().is_none());
    }

    #[tokio::test]
    async fn vector_query_rejects_limit_out_of_range() {
        for limit in [0i32, 1001i32] {
            let (client, http_client) = make_replay_client("{\"Items\":[],\"Count\":0}");
            let search = crate::vector::VectorSearch::new(vec![1.0]).unwrap();

            let err = client
                .query()
                .table_name("test_table")
                .index_name("embedding_idx")
                .limit(limit)
                .customize()
                .vector_search(search)
                .send()
                .await
                .expect_err("request should be rejected locally");
            assert!(
                format!("{err:?}").contains("require Limit between 1 and 1000"),
                "limit {limit} should be rejected, got: {err:?}"
            );
            assert!(http_client.actual_requests().next().is_none());
        }
    }

    #[tokio::test]
    async fn vector_query_rejects_consistent_read_true() {
        let (client, http_client) = make_replay_client("{\"Items\":[],\"Count\":0}");
        let search = crate::vector::VectorSearch::new(vec![1.0]).unwrap();

        let err = client
            .query()
            .table_name("test_table")
            .index_name("embedding_idx")
            .limit(10)
            .consistent_read(true)
            .customize()
            .vector_search(search)
            .send()
            .await
            .expect_err("request should be rejected locally");
        assert!(format!("{err:?}").contains("ConsistentRead(true)"));
        assert!(http_client.actual_requests().next().is_none());
    }

    #[tokio::test]
    async fn vector_query_rejects_exclusive_start_key() {
        let (client, http_client) = make_replay_client("{\"Items\":[],\"Count\":0}");
        let search = crate::vector::VectorSearch::new(vec![1.0]).unwrap();

        let err = client
            .query()
            .table_name("test_table")
            .index_name("embedding_idx")
            .limit(10)
            .exclusive_start_key("pk", s("row1"))
            .customize()
            .vector_search(search)
            .send()
            .await
            .expect_err("request should be rejected locally");
        assert!(format!("{err:?}").contains("ExclusiveStartKey"));
        assert!(http_client.actual_requests().next().is_none());
    }

    #[tokio::test]
    async fn vector_query_rejects_scan_index_forward() {
        let (client, http_client) = make_replay_client("{\"Items\":[],\"Count\":0}");
        let search = crate::vector::VectorSearch::new(vec![1.0]).unwrap();

        let err = client
            .query()
            .table_name("test_table")
            .index_name("embedding_idx")
            .limit(10)
            .scan_index_forward(false)
            .customize()
            .vector_search(search)
            .send()
            .await
            .expect_err("request should be rejected locally");
        assert!(format!("{err:?}").contains("ScanIndexForward"));
        assert!(http_client.actual_requests().next().is_none());
    }

    #[tokio::test]
    async fn vector_query_rejects_return_scores_similarity_with_select_count() {
        let (client, http_client) = make_replay_client("{\"Items\":[],\"Count\":0}");
        let search = crate::vector::VectorSearch::new(vec![1.0])
            .unwrap()
            .with_return_scores(crate::vector::ReturnScores::Similarity);

        let err = client
            .query()
            .table_name("test_table")
            .index_name("embedding_idx")
            .limit(10)
            .select(aws_sdk_dynamodb::types::Select::Count)
            .customize()
            .vector_search(search)
            .send()
            .await
            .expect_err("request should be rejected locally");
        assert!(
            format!("{err:?}")
                .contains("ReturnScores::Similarity is not supported with Select::Count")
        );
        assert!(http_client.actual_requests().next().is_none());
    }

    #[tokio::test]
    async fn marker_binaries_are_rewritten_to_float32vector_in_put_item() {
        let (client, http_client) = make_replay_client("{}");

        let av =
            crate::float32_vector::Float32Vector::to_attribute_value(vec![1.0, 2.0, 3.0]).unwrap();

        client
            .put_item()
            .table_name("test_table")
            .item("pk", s("row1"))
            .item("embedding", av)
            .send()
            .await
            .expect("request should succeed against the replay client");

        let requests = http_client.actual_requests().collect::<Vec<_>>();
        let json: serde_json::Value =
            serde_json::from_slice(requests[0].body().bytes().unwrap()).unwrap();
        assert_eq!(
            json["Item"]["embedding"]["FLOAT32VECTOR"],
            serde_json::json!([1.0, 2.0, 3.0])
        );
        assert_eq!(json["Item"]["pk"]["S"], "row1");
    }

    #[tokio::test]
    async fn ordinary_binary_values_are_left_unchanged_in_put_item() {
        let (client, http_client) = make_replay_client("{}");

        client
            .put_item()
            .table_name("test_table")
            .item("pk", s("row1"))
            .item(
                "blob",
                AttributeValue::B(aws_sdk_dynamodb::primitives::Blob::new(vec![1, 2, 3, 4])),
            )
            .send()
            .await
            .expect("request should succeed against the replay client");

        let requests = http_client.actual_requests().collect::<Vec<_>>();
        let json: serde_json::Value =
            serde_json::from_slice(requests[0].body().bytes().unwrap()).unwrap();
        assert!(json["Item"]["blob"].get("FLOAT32VECTOR").is_none());
        assert!(json["Item"]["blob"].get("B").is_some());
    }

    #[test]
    fn operation_name_from_target_extracts_operation() {
        assert_eq!(
            operation_name_from_target("DynamoDB_20120810.CreateTable"),
            Some("CreateTable")
        );
        assert_eq!(
            operation_name_from_target("DynamoDB_20120810.PutItem"),
            Some("PutItem")
        );
        assert_eq!(operation_name_from_target(""), Some(""));
    }

    fn s(value: &str) -> AttributeValue {
        AttributeValue::S(value.to_string())
    }

    fn make_client() -> aws_sdk_dynamodb::Client {
        let config = aws_sdk_dynamodb::Config::builder()
            .behavior_version(BehaviorVersion::latest())
            .region(Region::new("us-east-1"))
            .endpoint_url("http://127.0.0.1:1")
            .build();
        aws_sdk_dynamodb::Client::from_conf(config)
    }

    fn make_live_nodes() -> Arc<LiveNodes> {
        let seed_hosts: Vec<_> = (1..=10)
            .rev()
            .map(|i| format!("node{i}.example.com"))
            .collect();
        let config = AlternatorConfig::builder()
            .scheme("http")
            .port(8000)
            .seed_hosts(seed_hosts)
            .build();

        LiveNodes::new(&config).expect("live nodes")
    }

    fn make_interceptor(
        pk_info: impl IntoIterator<Item = (&'static str, &'static str)>,
    ) -> (AffinityQueryPlanInterceptor, Arc<LiveNodes>) {
        let mut config_builder =
            KeyRouteAffinityConfig::builder().with_type(KeyRouteAffinityType::AnyWrite);
        for (table, pk) in pk_info {
            config_builder = config_builder.with_pk_info(table, pk);
        }
        let config = config_builder.build();
        let live_nodes = make_live_nodes();
        let resolver = Arc::new(resolver::PartitionKeyResolver::new(
            make_client(),
            config.pk_info_per_table.clone(),
        ));
        let interceptor = AffinityQueryPlanInterceptor::new(config, live_nodes.clone(), resolver);

        (interceptor, live_nodes)
    }

    fn put_write(pk_name: &str, pk_value: AttributeValue, payload: &str) -> WriteRequest {
        let put = PutRequest::builder()
            .item(pk_name, pk_value)
            .item("payload", s(payload))
            .build()
            .unwrap();
        WriteRequest::builder().put_request(put).build()
    }

    fn delete_write(pk_name: &str, pk_value: AttributeValue) -> WriteRequest {
        let delete = DeleteRequest::builder()
            .key(pk_name, pk_value)
            .build()
            .unwrap();
        WriteRequest::builder().delete_request(delete).build()
    }

    fn batch_input(table_name: &str, writes: Vec<WriteRequest>) -> Input {
        let input = BatchWriteItemInput::builder()
            .request_items(table_name, writes)
            .build()
            .unwrap();
        Input::erase(input)
    }

    fn multi_table_batch_input(
        first_table: &str,
        first_writes: Vec<WriteRequest>,
        second_table: &str,
        second_writes: Vec<WriteRequest>,
    ) -> Input {
        let input = BatchWriteItemInput::builder()
            .request_items(first_table, first_writes)
            .request_items(second_table, second_writes)
            .build()
            .unwrap();
        Input::erase(input)
    }

    fn short_name(url: &Url) -> String {
        let host = url.host_str().expect("url has host");
        host.strip_suffix(".example.com")
            .unwrap_or(host)
            .to_string()
    }

    fn first_node(plan: QueryPlan) -> String {
        short_name(&plan.next_node().expect("plan has first node"))
    }

    fn node_sequence(plan: &QueryPlan, count: usize) -> Vec<String> {
        let mut out = Vec::with_capacity(count);
        for _ in 0..count {
            let Some(node) = plan.next_node() else {
                break;
            };
            out.push(short_name(&node));
        }
        out
    }

    fn sorted_live_node_names(live_nodes: &Arc<LiveNodes>) -> Vec<String> {
        let mut nodes = live_nodes.get_live_nodes();
        nodes.sort_unstable_by(|left, right| left.as_str().cmp(right.as_str()));
        nodes.iter().map(|node| short_name(node)).collect()
    }

    fn expected_preferred_order(
        live_nodes: &Arc<LiveNodes>,
        voted_nodes: impl IntoIterator<Item = String>,
    ) -> Vec<String> {
        let mut expected: Vec<_> = voted_nodes.into_iter().collect();
        for node in sorted_live_node_names(live_nodes) {
            if !expected.contains(&node) {
                expected.push(node);
            }
        }
        expected
    }

    fn preferred_node_for_key(live_nodes: &Arc<LiveNodes>, key: &str) -> String {
        let nodes = QueryPlan::sorted_affinity_nodes(live_nodes);
        let hash = hasher::hash_attribute_value(&s(key)).expect("string key is supported");
        let node = nodes
            .preferred_node_for_hash(hash)
            .expect("nodes are present");
        short_name(&node)
    }

    fn find_two_keys_on_one_node_and_one_on_another(
        live_nodes: &Arc<LiveNodes>,
    ) -> (String, String, String, String) {
        let mut buckets: HashMap<String, Vec<String>> = HashMap::new();

        for i in 0..1000 {
            let key = format!("key-{i}");
            let node = preferred_node_for_key(live_nodes, &key);
            buckets.entry(node).or_default().push(key);
        }

        let (majority_node, majority_keys) = buckets
            .iter()
            .find(|(_, keys)| keys.len() >= 2)
            .expect("test key space should contain two keys for one node");
        let other_key = buckets
            .iter()
            .find(|(node, keys)| *node != majority_node && !keys.is_empty())
            .map(|(_, keys)| keys[0].clone())
            .expect("test key space should contain another node");

        (
            majority_keys[0].clone(),
            majority_keys[1].clone(),
            other_key,
            majority_node.clone(),
        )
    }

    #[test]
    fn batch_write_majority_preferred_node_wins() {
        let (interceptor, live_nodes) = make_interceptor([("orders", "pk")]);
        let (majority_key_1, majority_key_2, other_key, expected_node) =
            find_two_keys_on_one_node_and_one_on_another(&live_nodes);
        let input = batch_input(
            "orders",
            vec![
                put_write("pk", s(&majority_key_1), "a"),
                put_write("pk", s(&other_key), "b"),
                put_write("pk", s(&majority_key_2), "c"),
            ],
        );

        let plan = interceptor
            .try_affinity_plan(&input)
            .expect("majority should select an affinity plan");

        assert_eq!(first_node(plan), expected_node);
    }

    #[test]
    fn batch_write_majority_votes_drive_full_retry_order() {
        let (interceptor, live_nodes) = make_interceptor([("orders", "pk")]);
        let (majority_key_1, majority_key_2, other_key, majority_node) =
            find_two_keys_on_one_node_and_one_on_another(&live_nodes);
        let other_node = preferred_node_for_key(&live_nodes, &other_key);
        let input = batch_input(
            "orders",
            vec![
                put_write("pk", s(&majority_key_1), "a"),
                put_write("pk", s(&other_key), "b"),
                put_write("pk", s(&majority_key_2), "c"),
            ],
        );

        let plan = interceptor
            .try_affinity_plan(&input)
            .expect("usable votes should select an affinity plan");

        let expected = expected_preferred_order(&live_nodes, [majority_node, other_node]);
        assert_eq!(node_sequence(&plan, expected.len()), expected);
    }

    #[test]
    fn batch_write_equal_top_votes_use_deterministic_tie_break() {
        let (interceptor, live_nodes) = make_interceptor([("orders", "pk")]);
        let (majority_key, _, other_key, _) =
            find_two_keys_on_one_node_and_one_on_another(&live_nodes);
        let mut tied_nodes = vec![
            preferred_node_for_key(&live_nodes, &majority_key),
            preferred_node_for_key(&live_nodes, &other_key),
        ];
        tied_nodes.sort_unstable();
        let input = batch_input(
            "orders",
            vec![
                put_write("pk", s(&majority_key), "a"),
                put_write("pk", s(&other_key), "b"),
            ],
        );

        let plan = interceptor
            .try_affinity_plan(&input)
            .expect("tied usable votes should still select an affinity plan");

        let expected = expected_preferred_order(&live_nodes, tied_nodes);
        assert_eq!(node_sequence(&plan, expected.len()), expected);
    }

    #[test]
    fn batch_write_delete_majority_preferred_node_wins() {
        let (interceptor, live_nodes) = make_interceptor([("orders", "pk")]);
        let (majority_key_1, majority_key_2, other_key, expected_node) =
            find_two_keys_on_one_node_and_one_on_another(&live_nodes);
        let input = batch_input(
            "orders",
            vec![
                delete_write("pk", s(&majority_key_1)),
                delete_write("pk", s(&other_key)),
                delete_write("pk", s(&majority_key_2)),
            ],
        );

        let plan = interceptor
            .try_affinity_plan(&input)
            .expect("delete majority should select an affinity plan");

        assert_eq!(first_node(plan), expected_node);
    }

    #[test]
    fn batch_write_mixed_put_and_delete_votes_select_majority() {
        let (interceptor, live_nodes) = make_interceptor([("orders", "pk")]);
        let (majority_key_1, majority_key_2, other_key, expected_node) =
            find_two_keys_on_one_node_and_one_on_another(&live_nodes);
        let input = batch_input(
            "orders",
            vec![
                put_write("pk", s(&majority_key_1), "a"),
                delete_write("pk", s(&other_key)),
                delete_write("pk", s(&majority_key_2)),
            ],
        );

        let plan = interceptor
            .try_affinity_plan(&input)
            .expect("mixed majority should select an affinity plan");

        assert_eq!(first_node(plan), expected_node);
    }

    #[test]
    fn batch_write_voting_is_invariant_to_write_request_order() {
        let (interceptor, live_nodes) = make_interceptor([("orders", "pk")]);
        let (majority_key_1, majority_key_2, other_key, expected_node) =
            find_two_keys_on_one_node_and_one_on_another(&live_nodes);
        let first = batch_input(
            "orders",
            vec![
                put_write("pk", s(&majority_key_1), "a"),
                delete_write("pk", s(&other_key)),
                put_write("pk", s(&majority_key_2), "c"),
            ],
        );
        let second = batch_input(
            "orders",
            vec![
                put_write("pk", s(&majority_key_2), "c"),
                put_write("pk", s(&majority_key_1), "a"),
                delete_write("pk", s(&other_key)),
            ],
        );

        let first_plan = interceptor.try_affinity_plan(&first).unwrap();
        let second_plan = interceptor.try_affinity_plan(&second).unwrap();

        assert_eq!(first_node(first_plan), expected_node);
        assert_eq!(first_node(second_plan), expected_node);
    }

    #[test]
    fn batch_write_voting_is_invariant_to_table_insertion_order() {
        let (interceptor, live_nodes) = make_interceptor([("a_orders", "pk"), ("z_orders", "pk")]);
        let (majority_key_1, majority_key_2, other_key, expected_node) =
            find_two_keys_on_one_node_and_one_on_another(&live_nodes);
        let first = multi_table_batch_input(
            "a_orders",
            vec![put_write("pk", s(&majority_key_1), "a")],
            "z_orders",
            vec![
                delete_write("pk", s(&other_key)),
                put_write("pk", s(&majority_key_2), "c"),
            ],
        );
        let second = multi_table_batch_input(
            "z_orders",
            vec![
                put_write("pk", s(&majority_key_2), "c"),
                delete_write("pk", s(&other_key)),
            ],
            "a_orders",
            vec![put_write("pk", s(&majority_key_1), "a")],
        );

        let first_plan = interceptor.try_affinity_plan(&first).unwrap();
        let second_plan = interceptor.try_affinity_plan(&second).unwrap();

        assert_eq!(first_node(first_plan), expected_node);
        assert_eq!(first_node(second_plan), expected_node);
    }

    #[test]
    fn batch_write_non_key_attributes_do_not_change_selected_node() {
        let (interceptor, live_nodes) = make_interceptor([("orders", "pk")]);
        let (majority_key_1, majority_key_2, other_key, expected_node) =
            find_two_keys_on_one_node_and_one_on_another(&live_nodes);
        let first = batch_input(
            "orders",
            vec![
                put_write("pk", s(&majority_key_1), "payload-a"),
                put_write("pk", s(&other_key), "payload-b"),
                put_write("pk", s(&majority_key_2), "payload-c"),
            ],
        );
        let second = batch_input(
            "orders",
            vec![
                put_write("pk", s(&majority_key_1), "zzz"),
                put_write("pk", s(&other_key), "aaa"),
                put_write("pk", s(&majority_key_2), "mmm"),
            ],
        );

        let first_plan = interceptor.try_affinity_plan(&first).unwrap();
        let second_plan = interceptor.try_affinity_plan(&second).unwrap();

        assert_eq!(first_node(first_plan), expected_node);
        assert_eq!(first_node(second_plan), expected_node);
    }

    #[test]
    fn batch_write_unknown_table_metadata_does_not_block_known_candidate() {
        let (interceptor, live_nodes) = make_interceptor([("z_known", "pk")]);
        let known_key = "known-key";
        let expected_node = preferred_node_for_key(&live_nodes, known_key);
        let input = multi_table_batch_input(
            "a_unknown",
            vec![put_write("pk", s("unknown-key"), "a")],
            "z_known",
            vec![put_write("pk", s(known_key), "b")],
        );

        let plan = interceptor
            .try_affinity_plan(&input)
            .expect("known candidate should still be usable");

        assert_eq!(first_node(plan), expected_node);
    }

    #[test]
    fn batch_write_unsupported_partition_key_type_is_skipped() {
        let (interceptor, live_nodes) = make_interceptor([("orders", "pk")]);
        let supported_key = "supported-key";
        let expected_node = preferred_node_for_key(&live_nodes, supported_key);
        let input = batch_input(
            "orders",
            vec![
                put_write("pk", AttributeValue::Bool(true), "unsupported"),
                put_write("pk", s(supported_key), "supported"),
            ],
        );

        let plan = interceptor
            .try_affinity_plan(&input)
            .expect("supported candidate should still be usable");

        assert_eq!(first_node(plan), expected_node);
    }
}
