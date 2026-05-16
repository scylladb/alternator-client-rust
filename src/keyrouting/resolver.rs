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
    use aws_sdk_dynamodb::config::{BehaviorVersion, Region};

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
