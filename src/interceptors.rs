use crate::*;

use aws_smithy_runtime_api::box_error::BoxError;
use aws_smithy_runtime_api::client::interceptors::Intercept;
use aws_smithy_runtime_api::client::interceptors::context::Input;
use aws_smithy_runtime_api::client::interceptors::context::{
    BeforeDeserializationInterceptorContextMut, BeforeSerializationInterceptorContextMut,
    BeforeTransmitInterceptorContextMut,
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
/// Uses [strip_headers] and [compress_request].
///
/// Also checks [ConfigBag] for config overrides that could have been left by [AlternatorOverrideInterceptor].
#[derive(Debug)]
pub(crate) struct AlternatorInterceptor {
    request_compression: RequestCompression,
    optimize_headers: bool,
}
impl AlternatorInterceptor {
    pub fn new(request_compression: RequestCompression, optimize_headers: bool) -> Self {
        Self {
            request_compression,
            optimize_headers,
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

        Ok(())
    }

    fn modify_before_transmit(
        &self,
        context: &mut BeforeTransmitInterceptorContextMut,
        _: &RuntimeComponents,
        cfg: &mut ConfigBag,
    ) -> Result<(), BoxError> {
        // check for overrides
        let optimize_headers = cfg
            .interceptor_state()
            .load::<OptimizeHeadersStore>()
            .map(|store| store.optimize_headers)
            .unwrap_or(self.optimize_headers);

        // optimize headers
        if optimize_headers {
            strip_headers(context.request_mut());
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
        _cfg: &mut ConfigBag,
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

        if algorithms.is_empty() {
            return Ok(());
        }

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

        Ok(())
    }
}

#[derive(Debug, Clone)]
pub(crate) struct RequestCompressionStore {
    request_compression: RequestCompression,
}
impl Storable for RequestCompressionStore {
    type Storer = StoreReplace<Self>;
}

#[derive(Debug, Clone)]
pub(crate) struct OptimizeHeadersStore {
    optimize_headers: bool,
}
impl Storable for OptimizeHeadersStore {
    type Storer = StoreReplace<Self>;
}

/// An interceptor used to override [AlternatorClient]'s config.
///
/// Adds specified config overrides to [ConfigBag], so that [AlternatorInterceptor] can later look for it.
///
/// Is used by [AlternatorCustomizableOperation] to allow per-operation customization.
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
impl AlternatorOverrideInterceptor<OptimizeHeadersStore> {
    pub(crate) fn for_optimize_headers(optimize_headers: bool) -> Self {
        AlternatorOverrideInterceptor {
            store: OptimizeHeadersStore { optimize_headers },
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
