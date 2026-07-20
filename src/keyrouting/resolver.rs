use aws_sdk_dynamodb::error::{ProvideErrorMetadata, SdkError};
use aws_sdk_dynamodb::operation::describe_table::DescribeTableError;
use dashmap::{DashMap, DashSet};
use rand::RngExt;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::Instant;

// Constants mapping to the Java implementation
const MAX_RETRIES: u32 = 3;
const INITIAL_RETRY_DELAY_MS: u64 = 100;
const MAX_RETRY_DELAY_MS: u64 = 2000;
const PERMANENT_FAILURE_COOLDOWN_MS: u64 = 5 * 60 * 1000; // 5 minutes
const MAX_JITTER_PERCENT: f64 = 0.2;

pub(crate) type PkCache = Arc<DashMap<String, Arc<str>>>;

#[derive(Debug)]
struct FailureRecord {
    timestamp: Instant,
    permanent: bool,
}

impl FailureRecord {
    fn can_retry(&self) -> bool {
        if !self.permanent {
            return true; // Transient failures can always be retried when a new request comes
        }
        self.timestamp.elapsed().as_millis() as u64 > PERMANENT_FAILURE_COOLDOWN_MS
    }
}

/// Resolves partition key attribute names for DynamoDB tables, mirroring the Java implementation.
///
/// Caches results to avoid repeated `DescribeTable` calls. Supports both
/// pre-configured partition key info (via [`PartitionKeyResolver::new`]) and
/// asynchronous discovery on demand (via [`trigger_discovery`]).
///
/// # Retry behavior
///
/// Transient failures (network errors, throttling, server errors) are retried
/// with exponential backoff up to [`MAX_RETRIES`] times, with jitter to avoid
/// thundering herd. Permanent failures (table not found, access denied,
/// validation errors) are not retried, but become eligible for another
/// discovery attempt after [`PERMANENT_FAILURE_COOLDOWN_MS`] elapses.
///
/// # Resource management
///
/// Discovery tasks are spawned on the ambient tokio runtime via
/// [`tokio::spawn`] and detached. There is no explicit shutdown: when the
/// runtime is dropped, in-flight discoveries are cancelled at their next
/// `.await` point and any RAII guards (e.g. the in-progress marker) run on
/// drop. PK discovery is an optimization, so cancellation mid-flight is safe
/// — the affected request falls back to round-robin and the next call to
/// [`trigger_discovery`] respawns.
///
/// # Concurrency
///
/// Thread-safe for concurrent access. The cache and failure-tracking maps
/// use [`DashMap`], and [`trigger_discovery`] uses a double-checked-locking
/// pattern (an in-progress [`DashSet`]) to ensure at most one discovery task
/// runs per table at a time.
///
/// [`trigger_discovery`]: PartitionKeyResolver::trigger_discovery
/// [`MAX_RETRIES`]: self::MAX_RETRIES
/// [`PERMANENT_FAILURE_COOLDOWN_MS`]: self::PERMANENT_FAILURE_COOLDOWN_MS
#[derive(Debug)]
pub struct PartitionKeyResolver {
    client: aws_sdk_dynamodb::Client,
    cache: PkCache,
    in_progress: Arc<DashSet<String>>,
    failed_tables: Arc<DashMap<String, FailureRecord>>,
}

impl PartitionKeyResolver {
    pub fn new(client: aws_sdk_dynamodb::Client, pk_info: HashMap<String, String>) -> Self {
        let cache: PkCache = Arc::new(DashMap::new());
        for (k, v) in pk_info {
            cache.insert(k, Arc::from(v));
        }
        Self {
            client,
            cache,
            in_progress: Arc::new(DashSet::new()),
            failed_tables: Arc::new(DashMap::new()),
        }
    }

    /// Triggers asynchronous discovery of the partition key for `table_name`.
    ///
    /// No-op if the table is already cached, if discovery is currently in
    /// progress, or if the table is in failure cooldown. Otherwise spawns a
    /// detached task on the ambient tokio runtime that calls `DescribeTable`,
    /// retries transient failures with exponential backoff, and writes the
    /// result to the cache on success.
    pub fn trigger_discovery(self: &Arc<Self>, table_name: &str) {
        if self.cache.contains_key(table_name) {
            return;
        }

        if let Some(rec) = self.failed_tables.get(table_name)
            && !rec.can_retry()
        {
            return;
        }

        // If no Tokio runtime is available, we can't spawn the discovery task.
        if tokio::runtime::Handle::try_current().is_err() {
            return;
        }

        if !self.in_progress.insert(table_name.to_string()) {
            return;
        }

        // Double-check after claiming the in-progress slot.
        if self.cache.contains_key(table_name) {
            self.in_progress.remove(table_name);
            return;
        }

        self.failed_tables.remove(table_name);

        let resolver = Arc::clone(self);
        let name = table_name.to_string();
        tokio::spawn(async move {
            resolver.discover_with_retry(name).await;
        });
    }

    /// Returns the cached partition key attribute name for `table`, or
    /// `None` if the table has never been resolved. To trigger discovery
    /// on a miss, call [`PartitionKeyResolver::trigger_discovery`] separately.
    pub fn get_partition_key(&self, table: &str) -> Option<Arc<str>> {
        self.cache.get(table).map(|v| v.clone())
    }

    async fn discover_with_retry(&self, table_name: String) {
        let mut attempt = 0;
        let mut delay = INITIAL_RETRY_DELAY_MS;

        // Clean up the in_progress marker no matter how this function exits
        struct InProgressCleanup(Arc<DashSet<String>>, String);
        impl Drop for InProgressCleanup {
            fn drop(&mut self) {
                self.0.remove(&self.1);
            }
        }
        let _cleanup = InProgressCleanup(self.in_progress.clone(), table_name.clone());

        loop {
            match self
                .client
                .describe_table()
                .table_name(&table_name)
                .send()
                .await
            {
                Ok(response) => {
                    if let Some(table) = response.table
                        && let Some(schema) = table.key_schema
                        && let Some(pk_element) = schema
                            .into_iter()
                            .find(|k| k.key_type == aws_sdk_dynamodb::types::KeyType::Hash)
                    {
                        let pk_name: Arc<str> = Arc::from(pk_element.attribute_name());
                        self.cache.insert(table_name, pk_name);
                        return; // Success!
                    }
                    // tracing::warn!("Table {} has no HASH key in schema", table_name);
                    self.failed_tables.insert(
                        table_name,
                        FailureRecord {
                            timestamp: Instant::now(),
                            permanent: true,
                        },
                    );
                    return;
                }
                Err(err) => {
                    if is_permanent_failure(&err) {
                        // tracing::warn!("Permanent failure discovering table {}: {:?}", table_name, err);
                        self.failed_tables.insert(
                            table_name,
                            FailureRecord {
                                timestamp: Instant::now(),
                                permanent: true,
                            },
                        );
                        return;
                    }

                    attempt += 1;
                    if attempt > MAX_RETRIES {
                        // tracing::warn!("Failed to discover PK for table {} after {} attempts.", table_name, MAX_RETRIES + 1);
                        self.failed_tables.insert(
                            table_name,
                            FailureRecord {
                                timestamp: Instant::now(),
                                permanent: false,
                            },
                        );
                        return;
                    }

                    let jittered_delay = calculate_jittered_delay(delay);
                    // tracing::debug!("Transient error discovering table {}, retry {}/{} after {}ms",
                    //     table_name, attempt, MAX_RETRIES, jittered_delay);

                    tokio::time::sleep(Duration::from_millis(jittered_delay)).await;
                    delay = std::cmp::min(delay.saturating_mul(2), MAX_RETRY_DELAY_MS);
                }
            }
        }
    }
}

fn is_permanent_failure(err: &SdkError<DescribeTableError>) -> bool {
    // Modeled errors first — ResourceNotFound is permanent.
    if let Some(svc) = err.as_service_error() {
        if svc.is_resource_not_found_exception() {
            return true;
        }
        match svc.code() {
            Some("AccessDeniedException") | Some("ValidationException") => return true,
            _ => {}
        }
    }
    // Bare 403 with no service error attached (network appliance, proxy, etc.)
    err.raw_response()
        .map(|r| r.status().as_u16() == 403)
        .unwrap_or(false)
}

fn calculate_jittered_delay(base_delay: u64) -> u64 {
    let jitter_range = (base_delay as f64 * MAX_JITTER_PERCENT) as i64;
    if jitter_range == 0 {
        return base_delay;
    }
    let jitter = rand::rng().random_range(-jitter_range..=jitter_range);
    (base_delay as i64 + jitter).max(1) as u64
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        AffinityQueryPlanInterceptor, AlternatorClient, AlternatorConfig, AlternatorInterceptor,
        LiveNodes, RequestCompression, ResponseCompression, RoundRobinQueryPlanInterceptor,
        UserAgent,
    };
    use aws_sdk_dynamodb::config::{BehaviorVersion, Credentials, Region};
    use aws_sdk_dynamodb::types::AttributeValue;
    use aws_smithy_runtime_api::client::http::{
        HttpClient, HttpConnector, HttpConnectorFuture, HttpConnectorSettings, SharedHttpConnector,
    };
    use aws_smithy_runtime_api::client::orchestrator::{HttpRequest, HttpResponse};
    use aws_smithy_runtime_api::client::runtime_components::RuntimeComponents;
    use aws_smithy_types::body::SdkBody;
    use std::fmt;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tokio::sync::watch;

    const DESCRIBE_TABLE_TARGET: &str = "DynamoDB_20120810.DescribeTable";
    const DESCRIBE_TABLE_RESPONSE: &str =
        r#"{"Table":{"KeySchema":[{"AttributeName":"pk","KeyType":"HASH"}]}}"#;

    // ----- Test helpers -----

    /// Builds a real but unused dynamodb client. The synchronous tests in
    /// this module never trigger the spawn path that actually uses the
    /// client; tests that *do* trigger a spawn use a bogus endpoint so the
    /// background `DescribeTable` fails fast off-thread.
    fn make_client() -> aws_sdk_dynamodb::Client {
        let config = aws_sdk_dynamodb::Config::builder()
            .behavior_version(BehaviorVersion::latest())
            .region(Region::new("us-east-1"))
            .endpoint_url("http://127.0.0.1:1") // unreachable on purpose
            .build();
        aws_sdk_dynamodb::Client::from_conf(config)
    }

    fn make_resolver() -> Arc<PartitionKeyResolver> {
        Arc::new(PartitionKeyResolver::new(make_client(), HashMap::new()))
    }

    #[derive(Clone)]
    struct MockDynamoHttp {
        state: Arc<MockDynamoHttpState>,
    }

    struct MockDynamoHttpState {
        describe_tables: AtomicUsize,
        data_requests: AtomicUsize,
        hold_first_describe: bool,
        first_describe_release: watch::Sender<bool>,
    }

    impl fmt::Debug for MockDynamoHttp {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.debug_struct("MockDynamoHttp").finish_non_exhaustive()
        }
    }

    impl MockDynamoHttp {
        fn new() -> Self {
            Self::with_first_describe_held(false)
        }

        fn with_held_first_describe() -> Self {
            Self::with_first_describe_held(true)
        }

        fn with_first_describe_held(hold_first_describe: bool) -> Self {
            let (first_describe_release, _) = watch::channel(false);
            Self {
                state: Arc::new(MockDynamoHttpState {
                    describe_tables: AtomicUsize::new(0),
                    data_requests: AtomicUsize::new(0),
                    hold_first_describe,
                    first_describe_release,
                }),
            }
        }

        fn describe_tables(&self) -> usize {
            self.state.describe_tables.load(Ordering::SeqCst)
        }

        async fn wait_for_describe_tables(&self, expected: usize) {
            tokio::time::timeout(Duration::from_secs(1), async {
                loop {
                    if self.describe_tables() >= expected {
                        break;
                    }
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            })
            .await
            .expect("timed out waiting for DescribeTable request");
        }

        fn release_first_describe(&self) {
            self.state.first_describe_release.send_replace(true);
        }
    }

    impl HttpClient for MockDynamoHttp {
        fn http_connector(
            &self,
            _: &HttpConnectorSettings,
            _: &RuntimeComponents,
        ) -> SharedHttpConnector {
            SharedHttpConnector::new(self.clone())
        }
    }

    impl HttpConnector for MockDynamoHttp {
        fn call(&self, request: HttpRequest) -> HttpConnectorFuture {
            let request = request
                .try_into_http1x()
                .expect("mock connector should receive an HTTP/1.x request");
            let target = request
                .headers()
                .get("x-amz-target")
                .and_then(|h| h.to_str().ok())
                .unwrap_or_default()
                .to_string();

            if target == DESCRIBE_TABLE_TARGET {
                let state = Arc::clone(&self.state);
                let request_number = state.describe_tables.fetch_add(1, Ordering::SeqCst) + 1;
                return HttpConnectorFuture::new(async move {
                    if state.hold_first_describe && request_number == 1 {
                        let mut release = state.first_describe_release.subscribe();
                        while !*release.borrow_and_update() {
                            release
                                .changed()
                                .await
                                .expect("release sender must outlive first DescribeTable");
                        }
                    }
                    Ok(json_response(DESCRIBE_TABLE_RESPONSE))
                });
            }

            self.state.data_requests.fetch_add(1, Ordering::SeqCst);
            HttpConnectorFuture::ready(Ok(json_response("{}")))
        }
    }

    fn json_response(body: &'static str) -> HttpResponse {
        let response = http::Response::builder()
            .status(200)
            .header("content-type", "application/x-amz-json-1.0")
            .header("x-amzn-requestid", "test-request-id")
            .body(SdkBody::from(body))
            .expect("valid mock HTTP response");
        HttpResponse::try_from(response).expect("valid Smithy HTTP response")
    }

    fn make_affinity_client(
        http_client: MockDynamoHttp,
        affinity_config: crate::keyrouting::KeyRouteAffinityConfig,
    ) -> (aws_sdk_dynamodb::Client, Arc<PartitionKeyResolver>) {
        let live_nodes_config = AlternatorConfig::builder()
            .behavior_version_latest()
            .endpoint_url("http://127.0.0.1:1")
            .build();
        let live_nodes = LiveNodes::new(&live_nodes_config).expect("seed node config is valid");

        let base_builder = aws_sdk_dynamodb::Config::builder()
            .behavior_version(BehaviorVersion::latest())
            .region(Region::new("us-east-1"))
            .credentials_provider(Credentials::for_tests_with_session_token())
            .endpoint_url("http://127.0.0.1:1")
            .http_client(http_client)
            .interceptor(AlternatorInterceptor::new(
                RequestCompression::disabled(),
                ResponseCompression::disabled(),
                true,
                UserAgent::default(),
                true,
            ));

        let discovery_client = aws_sdk_dynamodb::Client::from_conf(
            base_builder
                .clone()
                .interceptor(RoundRobinQueryPlanInterceptor::new(live_nodes.clone()))
                .build(),
        );
        let resolver = Arc::new(PartitionKeyResolver::new(
            discovery_client,
            affinity_config.pk_info_per_table.clone(),
        ));

        let client = aws_sdk_dynamodb::Client::from_conf(
            base_builder
                .interceptor(AffinityQueryPlanInterceptor::new(
                    affinity_config,
                    live_nodes,
                    resolver.clone(),
                ))
                .build(),
        );

        (client, resolver)
    }

    async fn put_item(
        client: &aws_sdk_dynamodb::Client,
        table_name: &str,
        pk_name: &str,
        pk_value: &str,
    ) {
        client
            .put_item()
            .table_name(table_name)
            .item(pk_name, AttributeValue::S(pk_value.to_string()))
            .send()
            .await
            .expect("mocked PutItem should succeed");
    }

    async fn wait_for_partition_key(
        resolver: &PartitionKeyResolver,
        table_name: &str,
        expected_pk: &str,
    ) {
        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                if resolver
                    .get_partition_key(table_name)
                    .is_some_and(|pk| pk.as_ref() == expected_pk)
                {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("timed out waiting for partition-key discovery");
    }

    // ----- FailureRecord -----

    #[test]
    fn transient_failure_can_always_retry() {
        let rec = FailureRecord {
            timestamp: Instant::now(),
            permanent: false,
        };
        assert!(rec.can_retry());
    }

    #[test]
    fn fresh_permanent_failure_cannot_retry() {
        let rec = FailureRecord {
            timestamp: Instant::now(),
            permanent: true,
        };
        assert!(!rec.can_retry());
    }

    #[test]
    fn old_permanent_failure_can_retry() {
        // Subtract more than the cooldown window from "now".
        let past = Instant::now()
            .checked_sub(Duration::from_millis(PERMANENT_FAILURE_COOLDOWN_MS + 100))
            .expect("clock must have run for at least the cooldown duration");
        let rec = FailureRecord {
            timestamp: past,
            permanent: true,
        };
        assert!(rec.can_retry());
    }

    // ----- Constructor / preconfigured map -----

    #[test]
    fn preconfigured_pk_info_populates_cache() {
        let mut pk_info = HashMap::new();
        pk_info.insert("users".to_string(), "user_id".to_string());
        pk_info.insert("orders".to_string(), "order_id".to_string());

        let resolver = PartitionKeyResolver::new(make_client(), pk_info);

        assert_eq!(
            resolver.get_partition_key("users").as_deref(),
            Some("user_id"),
        );
        assert_eq!(
            resolver.get_partition_key("orders").as_deref(),
            Some("order_id"),
        );
        assert!(resolver.get_partition_key("unknown").is_none());
    }

    #[test]
    fn empty_preconfigured_map_yields_empty_cache() {
        let resolver = PartitionKeyResolver::new(make_client(), HashMap::new());
        assert!(resolver.get_partition_key("anything").is_none());
    }

    // ----- trigger_discovery synchronous short-circuits -----

    #[tokio::test]
    async fn trigger_discovery_no_op_when_cached() {
        let resolver = make_resolver();
        resolver
            .cache
            .insert("users".to_string(), Arc::from("user_id"));

        resolver.trigger_discovery("users");

        // No spawn happened — in_progress stays empty, failure map untouched.
        assert!(!resolver.in_progress.contains("users"));
        assert!(resolver.failed_tables.is_empty());
    }

    #[tokio::test]
    async fn trigger_discovery_no_op_when_in_cooldown() {
        let resolver = make_resolver();
        resolver.failed_tables.insert(
            "users".to_string(),
            FailureRecord {
                timestamp: Instant::now(),
                permanent: true,
            },
        );

        resolver.trigger_discovery("users");

        // Cooldown gate held — no spawn, failure record preserved.
        assert!(!resolver.in_progress.contains("users"));
        assert!(resolver.failed_tables.contains_key("users"));
    }

    #[tokio::test]
    async fn trigger_discovery_no_op_when_already_in_progress() {
        let resolver = make_resolver();
        // Simulate a discovery already running by manually claiming the slot.
        resolver.in_progress.insert("users".to_string());

        resolver.trigger_discovery("users");

        // The original claim is preserved; the second call did not clobber state.
        assert!(resolver.in_progress.contains("users"));
        assert!(resolver.failed_tables.is_empty());
    }

    #[tokio::test]
    async fn expired_cooldown_clears_failure_and_proceeds() {
        let resolver = make_resolver();
        let past = Instant::now()
            .checked_sub(Duration::from_millis(PERMANENT_FAILURE_COOLDOWN_MS + 100))
            .unwrap();
        resolver.failed_tables.insert(
            "users".to_string(),
            FailureRecord {
                timestamp: past,
                permanent: true,
            },
        );

        resolver.trigger_discovery("users");

        // We proceeded past the cooldown gate — failure record cleared.
        // The spawned task tries DescribeTable against an unreachable endpoint
        // and will eventually re-record a transient failure asynchronously,
        // but that happens off-thread and is not what this test asserts.
        assert!(!resolver.failed_tables.contains_key("users"));
    }

    #[tokio::test]
    async fn concurrent_triggers_for_same_table_dedup() {
        let resolver = make_resolver();

        // Fire many triggers in quick succession. The first one claims the
        // in_progress slot and spawns a task; the rest should no-op.
        for _ in 0..10 {
            resolver.trigger_discovery("users");
        }

        // Whether the spawned task has run yet is racy, but the synchronous
        // bookkeeping is not: exactly one in_progress entry, no others.
        assert_eq!(resolver.in_progress.len(), 1);
        assert!(resolver.in_progress.contains("users"));
    }

    #[tokio::test]
    async fn affinity_request_discovers_partition_key_once_and_reuses_cache() {
        let http_client = MockDynamoHttp::with_held_first_describe();
        let affinity_config = crate::keyrouting::KeyRouteAffinityConfig::builder()
            .with_type(crate::keyrouting::KeyRouteAffinityType::AnyWrite)
            .build();
        let (client, resolver) = make_affinity_client(http_client.clone(), affinity_config);

        put_item(&client, "users", "id", "first").await;
        http_client.wait_for_describe_tables(1).await;

        for i in 0..5 {
            put_item(&client, "users", "id", &format!("in-flight-{i}")).await;
        }
        assert_eq!(
            http_client.describe_tables(),
            1,
            "same-table cache misses while discovery is in flight must be deduplicated",
        );

        http_client.release_first_describe();
        wait_for_partition_key(&resolver, "users", "pk").await;

        for i in 0..3 {
            put_item(&client, "users", "pk", &format!("cached-{i}")).await;
        }
        assert_eq!(
            http_client.describe_tables(),
            1,
            "cached partition-key metadata should avoid repeated DescribeTable calls",
        );
    }

    #[tokio::test]
    async fn alternator_client_from_conf_wires_affinity_partition_key_discovery() {
        let http_client = MockDynamoHttp::new();
        let client = AlternatorClient::from_conf(
            AlternatorConfig::builder()
                .credentials_provider(Credentials::for_tests_with_session_token())
                .behavior_version_latest()
                .endpoint_url("http://127.0.0.1:1")
                .http_client(http_client.clone())
                .key_route_affinity(crate::keyrouting::KeyRouteAffinityType::AnyWrite)
                .build(),
        );

        client
            .put_item()
            .table_name("users")
            .item("id", AttributeValue::S("first".to_string()))
            .send()
            .await
            .expect("mocked PutItem should succeed");

        http_client.wait_for_describe_tables(1).await;
        assert_eq!(
            http_client.describe_tables(),
            1,
            "AlternatorClient::from_conf should wire affinity PK discovery",
        );
    }

    #[tokio::test]
    async fn affinity_request_with_preconfigured_partition_key_skips_discovery() {
        let http_client = MockDynamoHttp::new();
        let affinity_config = crate::keyrouting::KeyRouteAffinityConfig::builder()
            .with_type(crate::keyrouting::KeyRouteAffinityType::AnyWrite)
            .with_pk_info("users", "pk")
            .build();
        let (client, resolver) = make_affinity_client(http_client.clone(), affinity_config);

        assert_eq!(resolver.get_partition_key("users").as_deref(), Some("pk"));

        for i in 0..5 {
            put_item(&client, "users", "pk", &format!("configured-{i}")).await;
        }

        assert_eq!(
            http_client.describe_tables(),
            0,
            "preconfigured partition-key metadata should skip DescribeTable",
        );
    }

    // ----- calculate_jittered_delay -----

    #[test]
    fn jittered_delay_within_bounds() {
        let base = 100u64;
        for _ in 0..1000 {
            let d = calculate_jittered_delay(base);
            assert!(d >= 1, "delay must be at least 1ms, got {d}");
            assert!(d <= 120, "delay must be within +20%, got {d}");
            assert!(d >= 80, "delay must be within -20%, got {d}");
        }
    }

    #[test]
    fn jittered_delay_handles_small_base() {
        // For base values where base * 0.2 truncates to 0, we return base
        // unchanged rather than dividing by zero in random_range.
        assert_eq!(calculate_jittered_delay(1), 1);
        assert_eq!(calculate_jittered_delay(4), 4);
    }

    #[test]
    fn jittered_delay_never_returns_zero() {
        // Even with negative jitter, the minimum is clamped to 1.
        for base in [1, 2, 4, 8, 16, 100, 1000] {
            let d = calculate_jittered_delay(base);
            assert!(d >= 1, "base {base} produced zero delay");
        }
    }
}

/// Verifies that when making a request without a Tokio runtime,
/// spawning the resolver task does not panic.
///
/// The default client can't make a request without a Tokio runtime without panicking,
/// however it is possible, when using a custom HTTP client, whose connector doesn't require Tokio,
/// and a custom sleep implementation that doesn't require Tokio.
#[cfg(test)]
mod tests_no_tokio_runtime {
    use crate::keyrouting::KeyRouteAffinityType;
    use crate::{AlternatorClient, AlternatorConfig};
    use aws_smithy_async::rt::sleep::{AsyncSleep, Sleep};
    use aws_smithy_runtime_api::client::http::{
        HttpClient, HttpConnector, HttpConnectorFuture, HttpConnectorSettings, SharedHttpConnector,
    };
    use aws_smithy_runtime_api::client::orchestrator::{HttpRequest, HttpResponse};
    use aws_smithy_runtime_api::client::runtime_components::RuntimeComponents;
    use aws_smithy_runtime_api::http::StatusCode;
    use aws_smithy_types::body::SdkBody;

    /// A sleep impl that requires no Tokio runtime, by never actually sleeping.
    #[derive(Debug, Clone)]
    struct NoTokioSleep;
    impl AsyncSleep for NoTokioSleep {
        fn sleep(&self, _dur: std::time::Duration) -> Sleep {
            Sleep::new(std::future::pending())
        }
    }

    /// An HTTP client that requires no Tokio runtime, by doing no I/O.
    ///     
    /// It answers every request with an empty HTTP 400 - enough to let the
    /// request proceed past the connector, never a real reply from a cluster.
    #[derive(Debug, Clone)]
    struct NoTokioHttp;
    impl HttpClient for NoTokioHttp {
        fn http_connector(
            &self,
            _: &HttpConnectorSettings,
            _: &RuntimeComponents,
        ) -> SharedHttpConnector {
            SharedHttpConnector::new(self.clone())
        }
    }
    impl HttpConnector for NoTokioHttp {
        fn call(&self, _req: HttpRequest) -> HttpConnectorFuture {
            HttpConnectorFuture::ready(Ok(HttpResponse::new(
                StatusCode::try_from(400).unwrap(),
                SdkBody::empty(),
            )))
        }
    }

    #[test]
    fn request_without_tokio_runtime_does_not_panic() {
        // A client with both Tokio dependencies swapped out, and affinity set to
        // `KeyRouteAffinityType::AnyWrite` without any preconfigured PK info, so that the resolution
        // trigger must be attempted.
        let client = AlternatorClient::from_conf(
            AlternatorConfig::builder()
                .credentials_provider(
                    aws_sdk_dynamodb::config::Credentials::for_tests_with_session_token(),
                )
                .behavior_version_latest()
                .endpoint_url("http://127.0.0.1:8000")
                .http_client(NoTokioHttp)
                .sleep_impl(NoTokioSleep)
                .key_route_affinity(KeyRouteAffinityType::AnyWrite)
                .build(),
        );

        // `send()` builds the future, `block_on` polls it with no Tokio runtime.
        // This request is a `PutItem`, so in `AffinityQueryPlanInterceptor` it
        // qualifies for affinity routing, misses the cache, and triggers discovery.
        // Without the `Handle::try_current()` check in `trigger_discovery()`, this would
        // panic trying to spawn the resolver task with no Tokio runtime present.
        let _ = futures::executor::block_on(client.put_item().table_name("fake-table").send());
    }
}
