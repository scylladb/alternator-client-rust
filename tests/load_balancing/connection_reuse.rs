//! HTTP keep-alive (connection reuse) tests.
//!
//! These verify the client keeps TCP/HTTP connections alive across requests
//! instead of reopening them. Counting proxies sit between the client and the
//! cluster nodes and record accepted (`connects`) and closed (`disconnects`)
//! connections via the proxy's `on_client_connect` / `on_client_disconnect`
//! hooks, alongside GET/POST request counts.
//!
//! Two scenarios:
//!
//! - Load balancing **off** (`seed_hosts([])`, as in the http_content tests):
//!   no discovery, so a single data connection carries everything and the
//!   assertion can be made directly after the calls.
//!
//! - Load balancing **on**: background `/localnodes` discovery runs alongside
//!   operations. Each node then sees at most one data connection (POSTs) plus
//!   one discovery connection (GETs).
//!   Invariant to verify: `(posts > 0) + (gets > 0) == connects`.
//!   Discovery bumps `connects` at accept-time slightly before its
//!   GET is counted, so that equality is polled until it settles (within
//!   `POLLING_TIMEOUT`).

use crate::ccm_wrapper::cluster::*;
use crate::load_balancing::cluster_utils::*;
use crate::load_balancing::scope_utils;
use alternator_driver::AlternatorClient;

use std::time::Duration;

// A client that talks directly to `endpoint_url` with load balancing disabled,
// using an HTTP connection pool with the given idle timeout.
fn create_lb_disabled_client(
    cluster: &Cluster,
    pool_idle_timeout: Option<Duration>,
) -> AlternatorClient {
    // `None` keeps Hyper's default idle timeout (~90s); `Some(d)` pins it to `d`.
    let mut http_builder = aws_smithy_http_client::Builder::new();
    if let Some(timeout) = pool_idle_timeout {
        http_builder = http_builder.pool_idle_timeout(Some(timeout));
    }
    let http_client = http_builder.build_http();
    AlternatorClient::from_conf(
        minimal_builder()
            .http_client(http_client)
            .endpoint_url(default_endpoint_url(cluster))
            .seed_hosts(Vec::<String>::new())
            .build(),
    )
}

// Enough requests that "one connection per request" would be unmistakable.
const REQUESTS: usize = 50;
// A reused connection should yield one connect, allow a tiny margin for any
// pool churn while staying far below REQUESTS.
const MAX_CONNECTIONS: usize = 2;
// A short idle gap: well under Hyper's default pool idle timeout (~90s) so the
// default keeps the connection, but above POOL_TIMEOUT_SHORT so that expires it.
const IDLE_PERIOD: Duration = Duration::from_secs(3);
// Pool idle timeout below IDLE_PERIOD: the connection expires during the idle
// gap, forcing exactly one reconnect.
const POOL_TIMEOUT_SHORT: Duration = Duration::from_secs(1);

/// Many sequential requests to a single node should reuse one
/// connection, not one connection per request.
#[tokio::test]
#[cfg_attr(not(ccm_tests), ignore)]
async fn connection_kept_alive_test() {
    let mut guard = get_cluster().await;
    let cluster = &mut *guard;

    let request_counter = RequestCounter::from_cluster(cluster);
    start_proxies(cluster, PROXY_PORT, &request_counter).await;

    let client = create_lb_disabled_client(cluster, None);

    make_n_calls(&client, REQUESTS).await;

    assert!(
        request_counter.total_connects() >= 1,
        "expected at least one connection to the node"
    );
    assert!(
        request_counter.total_connects() <= MAX_CONNECTIONS,
        "connections were not reused: {} connects for {} requests",
        request_counter.total_connects(),
        REQUESTS,
    );
}

/// Verify the connection is reused if the idle period is shorter than pool lifetime.
/// With a pool idle timeout well above the idle gap, the pooled connection
/// survives the gap and is reused - no new connect, no disconnect.
#[tokio::test]
#[cfg_attr(not(ccm_tests), ignore)]
async fn connection_reused_after_idle_within_pool_lifetime_test() {
    let mut guard = get_cluster().await;
    let cluster = &mut *guard;

    let request_counter = RequestCounter::from_cluster(cluster);
    start_proxies(cluster, PROXY_PORT, &request_counter).await;

    let client = create_lb_disabled_client(cluster, None);

    make_n_calls(&client, REQUESTS).await;
    let connects_before = request_counter.total_connects();
    assert!(
        connects_before >= 1,
        "expected an established connection before idling"
    );

    tokio::time::sleep(IDLE_PERIOD).await;
    make_n_calls(&client, REQUESTS).await;

    assert_eq!(
        request_counter.total_connects(),
        connects_before,
        "connection was not reused across an idle gap within pool lifetime"
    );
    assert_eq!(
        request_counter.total_disconnects(),
        0,
        "connection was unexpectedly closed across the idle gap"
    );
}

/// Verify the connection reconnects only once if pool expiry is expected.
/// With a pool idle timeout below the idle gap, the pooled connection expires
/// during the gap, forcing exactly one reconnect.
#[tokio::test]
#[cfg_attr(not(ccm_tests), ignore)]
async fn connection_reconnects_once_after_pool_expiry_test() {
    let mut guard = get_cluster().await;
    let cluster = &mut *guard;

    let request_counter = RequestCounter::from_cluster(cluster);
    start_proxies(cluster, PROXY_PORT, &request_counter).await;

    let client = create_lb_disabled_client(cluster, Some(POOL_TIMEOUT_SHORT));

    make_n_calls(&client, REQUESTS).await;
    let connects_before = request_counter.total_connects();
    assert!(
        connects_before >= 1,
        "expected an established connection before idling"
    );

    tokio::time::sleep(IDLE_PERIOD).await;
    make_n_calls(&client, REQUESTS).await;

    // Exactly one reconnect across the idle gap.
    assert_eq!(
        request_counter.total_connects(),
        connects_before + 1,
        "expected exactly one reconnect after pool expiry"
    );

    tokio::time::timeout(POLLING_TIMEOUT, async {
        loop {
            if request_counter.total_disconnects() == connects_before {
                break;
            }
            tokio::time::sleep(POLLING_INTERVAL).await;
        }
    })
    .await
    .unwrap_or_else(|_| {
        panic!(
            "expected {} disconnect(s) after pool expiry, got {}",
            connects_before,
            request_counter.total_disconnects()
        )
    });
}

/// With load balancing and discovery enabled, repeated operations should keep
/// succeeding while background `/localnodes` polling runs, without opening a new
/// connection per GET or POST: each node ends up with at most one data
/// connection (POSTs) plus one discovery connection (GETs).
#[tokio::test]
#[cfg_attr(not(ccm_tests), ignore)]
async fn connection_reused_across_discovery_test() {
    let mut guard = get_cluster().await;
    let cluster = &mut *guard;

    let request_counter = RequestCounter::from_cluster(cluster);
    start_proxies(cluster, PROXY_PORT, &request_counter).await;

    let scope = scope_utils::datacenter_scope_from_index(cluster, 1);

    // Short discovery interval so the GET threshold below is reached quickly.
    let client =
        create_client_with_scope_and_interval(cluster, scope.clone(), Duration::from_millis(5));
    let target_gets_number = 10;

    wait_until_live_nodes_match(
        &client,
        scope_utils::working_nodes_ips_in_scope(cluster, &scope),
    )
    .await;

    make_n_calls(&client, REQUESTS).await;

    // Each in-scope node should hold at most one data connection (if it got
    // POSTs) plus one discovery connection (if it was polled).
    // Discovery bumps `connects` at accept-time a touch
    // before its GET is counted, so poll until the steady state settles.
    let in_scope_ips = scope_utils::ips_in_scope(cluster, &scope);

    tokio::time::timeout(POLLING_TIMEOUT, async {
        loop {
            let settled = in_scope_ips.iter().all(|ip| {
                let c = request_counter.get(ip);
                (c.posts() > 0) as usize + (c.gets() > 0) as usize == c.connects()
            });
            if settled && request_counter.total_gets() >= target_gets_number {
                break;
            }
            tokio::time::sleep(POLLING_INTERVAL).await;
        }
    })
    .await
    .unwrap_or_else(|_| {
        let detail: Vec<String> = in_scope_ips
            .iter()
            .map(|ip| {
                let c = request_counter.get(ip);
                format!(
                    "{}: posts={} gets={} connects={}",
                    ip,
                    c.posts(),
                    c.gets(),
                    c.connects()
                )
            })
            .collect();
        panic!(
            "connection reuse did not settle within {:?}; per-node state: [{}]",
            POLLING_TIMEOUT,
            detail.join(", ")
        )
    });

    // Sanity: the operations actually flowed through the proxies.
    assert!(
        request_counter.total_posts() >= REQUESTS,
        "expected at least {} POSTs through the proxies, got {}",
        REQUESTS,
        request_counter.total_posts()
    );
}
