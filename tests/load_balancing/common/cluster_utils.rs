// Copyright ScyllaDB, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Shared test utils for the multi-node CCM cluster: a once-created cluster
//! reused across tests, per-node counting proxies, and helpers for building
//! scoped clients and waiting on discovery.

use crate::ccm_wrapper::ccm::*;
use crate::ccm_wrapper::cluster::*;
use crate::ccm_wrapper::topology_spec::*;
use crate::load_balancing::proxy;

use alternator_driver::RoutingScope;
use alternator_driver::{AlternatorBuilder, AlternatorClient, AlternatorConfig};

use hyper::Method;
use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use ctor::dtor;
use tokio::sync::{Mutex, MutexGuard};

pub(crate) const PROXY_PORT: u16 = 7999;
pub(crate) const ALTERNATOR_PORT: u16 = 8000;

const DYNAMODB_SCAN_TARGET: &str = "DynamoDB_20120810.Scan";
const SYSTEM_LOCAL_TABLE: &str = ".scylla.alternator.system.local";
const SYSTEM_PEERS_TABLE: &str = ".scylla.alternator.system.peers";

pub(crate) const POLLING_TIMEOUT: Duration = Duration::from_secs(5);
pub(crate) const POLLING_INTERVAL: Duration = Duration::from_millis(50);

fn is_topology_scan(target: Option<&str>, body: &[u8]) -> bool {
    target == Some(DYNAMODB_SCAN_TARGET)
        && serde_json::from_slice::<serde_json::Value>(body)
            .ok()
            .and_then(|body| {
                body.get("TableName")
                    .and_then(serde_json::Value::as_str)
                    .map(|table_name| matches!(table_name, SYSTEM_LOCAL_TABLE | SYSTEM_PEERS_TABLE))
            })
            .unwrap_or(false)
}

// Since cluster creation is expensive, we create it once and reuse it for every test.
// Before a test gets access to the cluster, we make sure that all nodes are up and their ports are set to default.
// The first node in datacenter 1 is meant to never be shut down. Its address will be used as a seed address
// for clients and as a redirect target for requests directed to shut down nodes.
static CLUSTER: OnceLock<Mutex<Cluster>> = OnceLock::new();
pub(crate) async fn get_cluster() -> MutexGuard<'static, Cluster> {
    let mut cluster = CLUSTER
        .get_or_init(|| {
            let topology = TopologySpecBuilder::new()
                .datacenter(DatacenterSpec::new().rack(3))
                .datacenter(DatacenterSpec::new().rack(1).rack(2))
                .datacenter(DatacenterSpec::new().rack(2).rack(1))
                .build()
                .unwrap();
            let ip_prefix = IpPrefix::new("127.0.1.").unwrap();
            let cluster_name = format!("test_cluster_{}", uuid::Uuid::new_v4());
            let scylla_version = std::env::var("CCM_SCYLLA_VERSION")
                .unwrap_or_else(|_| String::from("release:2025.1.16"));
            let cluster = Ccm::create_cluster(
                cluster_name,
                &topology,
                ip_prefix,
                ALTERNATOR_PORT,
                scylla_version,
            )
            .unwrap();
            Mutex::new(cluster)
        })
        .lock()
        .await;

    Ccm::start_cluster(&mut cluster).unwrap();
    cluster.update_all_nodes_port(ALTERNATOR_PORT);
    cluster
}

// Since the cluster is static, it never drops, so we use a destructor.
#[dtor]
fn clean_up_cluster() {
    if let Some(cluster_mutex) = CLUSTER.get() {
        let mut cluster = cluster_mutex.blocking_lock();
        Ccm::remove_cluster(&mut cluster);
    }
}

pub(crate) fn default_seed_host(cluster: &Cluster) -> String {
    cluster.datacenters()[0].racks()[0].nodes()[0].ip.clone()
}

pub(crate) fn default_seed_port(cluster: &Cluster) -> u16 {
    cluster.datacenters()[0].racks()[0].nodes()[0].alternator_port
}

// Struct for counting connections accepted / closed and requests made to the proxy.
// GETs, data POSTs, and DescribeTable requests are counted separately. Topology
// discovery scans are excluded from the data POST count.
#[derive(Debug)]
pub(crate) struct NodeCounter {
    posts: AtomicUsize,
    topology_scans: AtomicUsize,
    gets: AtomicUsize,
    describe_tables: AtomicUsize,
    connects: AtomicUsize,
    disconnects: AtomicUsize,
}

impl NodeCounter {
    fn new() -> Self {
        Self {
            posts: AtomicUsize::new(0),
            topology_scans: AtomicUsize::new(0),
            gets: AtomicUsize::new(0),
            describe_tables: AtomicUsize::new(0),
            connects: AtomicUsize::new(0),
            disconnects: AtomicUsize::new(0),
        }
    }

    pub(crate) fn posts(&self) -> usize {
        self.posts.load(Ordering::Relaxed)
    }

    pub(crate) fn connects(&self) -> usize {
        self.connects.load(Ordering::Relaxed)
    }

    pub(crate) fn topology_scans(&self) -> usize {
        self.topology_scans.load(Ordering::Relaxed)
    }

    fn reset_posts(&self) {
        self.posts.store(0, Ordering::Relaxed);
    }

    fn reset(&self) {
        self.reset_posts();
        self.topology_scans.store(0, Ordering::Relaxed);
        self.gets.store(0, Ordering::Relaxed);
        self.describe_tables.store(0, Ordering::Relaxed);
        self.connects.store(0, Ordering::Relaxed);
        self.disconnects.store(0, Ordering::Relaxed);
    }
}

// Struct with a hashmap underneath for holding and monitoring the counters for multiple nodes.
#[derive(Debug)]
pub(crate) struct RequestCounter {
    counter: HashMap<String, Arc<NodeCounter>>,
}

impl RequestCounter {
    pub(crate) fn from_cluster(cluster: &Cluster) -> Self {
        let counter = cluster
            .nodes()
            .iter()
            .map(|node| (node.ip.clone(), Arc::new(NodeCounter::new())))
            .collect();
        Self { counter }
    }

    pub(crate) fn get(&self, ip: &str) -> Arc<NodeCounter> {
        Arc::clone(self.counter.get(ip).unwrap())
    }

    pub(crate) fn reset_posts(&self) {
        for c in self.counter.values() {
            c.reset_posts();
        }
    }

    pub(crate) fn reset(&self) {
        for c in self.counter.values() {
            c.reset();
        }
    }

    pub(crate) fn total_posts(&self) -> usize {
        self.counter
            .values()
            .map(|c| c.posts.load(Ordering::Relaxed))
            .sum()
    }

    pub(crate) fn total_topology_scans(&self) -> usize {
        self.counter
            .values()
            .map(|c| c.topology_scans.load(Ordering::Relaxed))
            .sum()
    }

    pub(crate) fn total_connects(&self) -> usize {
        self.counter
            .values()
            .map(|c| c.connects.load(Ordering::Relaxed))
            .sum()
    }

    pub(crate) fn total_disconnects(&self) -> usize {
        self.counter
            .values()
            .map(|c| c.disconnects.load(Ordering::Relaxed))
            .sum()
    }

    pub(crate) fn get_posts_to_ips(&self, ips: &[&str]) -> usize {
        ips.iter()
            .map(|ip| self.counter.get(*ip).unwrap().posts.load(Ordering::Relaxed))
            .sum()
    }

    pub(crate) fn get_posts_to_other_ips(&self, ips: &[&str]) -> usize {
        self.counter
            .iter()
            .filter(|(ip, _)| !ips.contains(&ip.as_str()))
            .map(|(_, c)| c.posts.load(Ordering::Relaxed))
            .sum()
    }

    pub(crate) fn total_describe_tables(&self) -> usize {
        self.counter
            .values()
            .map(|c| c.describe_tables.load(Ordering::Relaxed))
            .sum()
    }
}

pub(crate) async fn start_counting_proxy(
    listen_addr: String,
    connect_addr: String,
    request_counter: Arc<NodeCounter>,
) {
    // Count every accepted / closed TCP connection.
    let connect_counter = Arc::clone(&request_counter);
    let on_connect: Box<dyn Fn(std::net::SocketAddr) + Send + Sync> = Box::new(move |_addr| {
        connect_counter.connects.fetch_add(1, Ordering::Relaxed);
    });
    let disconnect_counter = Arc::clone(&request_counter);
    let on_disconnect: Box<dyn Fn() + Send + Sync> = Box::new(move || {
        disconnect_counter
            .disconnects
            .fetch_add(1, Ordering::Relaxed);
    });

    let proxy = proxy::Proxy::start(
        listen_addr,
        connect_addr,
        move |req, send| {
            let node_counter = Arc::clone(&request_counter);
            async move {
                let (parts, body) = proxy::collect_request(req).await;
                let is_describe_table = parts
                    .headers
                    .get("x-amz-target")
                    .is_some_and(|h| h == "DynamoDB_20120810.DescribeTable");
                let target = parts
                    .headers
                    .get("x-amz-target")
                    .and_then(|target| target.to_str().ok());
                let is_topology_scan = is_topology_scan(target, &body);

                if parts.method == Method::POST {
                    if is_describe_table {
                        node_counter.describe_tables.fetch_add(1, Ordering::Relaxed);
                    } else if !is_topology_scan {
                        node_counter.posts.fetch_add(1, Ordering::Relaxed);
                    }
                } else if parts.method == Method::GET {
                    node_counter.gets.fetch_add(1, Ordering::Relaxed);
                }
                let (parts, body) = proxy::collect_received_response(parts, body, send).await;
                if is_topology_scan {
                    node_counter.topology_scans.fetch_add(1, Ordering::Relaxed);
                }
                proxy::build_response(parts, body)
            }
        },
        Some(on_connect),
        Some(on_disconnect),
    )
    .await;

    // Avoid a dead-code warning.
    let _ = proxy.address();

    // We deliberately detach the proxy task here.
    // Each test has its own Tokio runtime, so dropping the runtime will abort
    // this task and cleanly release the listener and connection resources.
    // Only one test at a time can use the cluster, so old proxies will be dropped before new ones are created.
    tokio::spawn(async move {
        proxy.run().await;
    });
}

// This is the proxy that calls to the node go through. Used to count calls and ensure that client calls the correct nodes.
pub(crate) async fn start_proxy_on_node(
    node: Node,
    proxy_port: u16,
    request_counter: Arc<NodeCounter>,
) {
    let listen_addr = format!("{}:{}", node.ip, proxy_port);
    let connect_addr = format!("{}:{}", node.ip, ALTERNATOR_PORT);
    start_counting_proxy(listen_addr, connect_addr, request_counter).await;
}

pub(crate) async fn start_proxies(
    cluster: &mut Cluster,
    proxy_port: u16,
    request_counter: &RequestCounter,
) {
    cluster.update_all_nodes_port(proxy_port);
    let counter = &request_counter.counter;
    for node in cluster.nodes() {
        if node.is_up {
            let node_counter = Arc::clone(counter.get(&node.ip).unwrap());
            start_proxy_on_node(node.clone(), proxy_port, node_counter).await;
        }
    }
}

// Make calls without caring about the result, used to count where calls are directed.
pub(crate) async fn make_n_calls(client: &AlternatorClient, n: usize) {
    for _ in 0..n {
        let _ = client.list_tables().send().await;
    }
}

// Base builder to avoid same code in different constructors.
pub(crate) fn minimal_builder() -> AlternatorBuilder {
    AlternatorConfig::builder()
        .credentials_provider(aws_sdk_dynamodb::config::Credentials::for_tests_with_session_token())
        .region(aws_sdk_dynamodb::config::Region::new("eu-central-1"))
}

// Create a basic client with scope.
pub(crate) fn create_client_with_scope(cluster: &Cluster, scope: RoutingScope) -> AlternatorClient {
    AlternatorClient::from_conf(
        minimal_builder()
            .seed_hosts([default_seed_host(cluster)])
            .port(default_seed_port(cluster))
            .routing_scope(scope)
            .build(),
    )
}

// Like `create_client_with_scope`, but with a custom discovery active interval.
pub(crate) fn create_client_with_scope_and_interval(
    cluster: &Cluster,
    scope: RoutingScope,
    active_interval: Duration,
) -> AlternatorClient {
    AlternatorClient::from_conf(
        minimal_builder()
            .seed_hosts([default_seed_host(cluster)])
            .port(default_seed_port(cluster))
            .routing_scope(scope)
            .active_interval(active_interval)
            .build(),
    )
}

// Poll until requests are routed to exactly the expected nodes, or timeout.
//
// Each attempt compares POST deltas instead of clearing counters so callers can
// retain GET and connection history. Sending one request per counter entry is
// enough to traverse every node in any round-robin routing scope represented by
// the counter.
pub(crate) async fn wait_until_requests_routed_to(
    client: &AlternatorClient,
    request_counter: &RequestCounter,
    mut expected_ips: Vec<&str>,
) {
    assert!(
        !request_counter.counter.is_empty(),
        "cannot observe request routing without node counters"
    );
    for ip in &expected_ips {
        assert!(
            request_counter.counter.contains_key(*ip),
            "expected routing node {ip} has no request counter"
        );
    }
    expected_ips.sort_unstable();
    expected_ips.dedup();

    let requests_per_attempt = request_counter.counter.len();
    let mut last_observed = Vec::new();
    let result = tokio::time::timeout(POLLING_TIMEOUT, async {
        loop {
            let posts_before: HashMap<&str, usize> = request_counter
                .counter
                .iter()
                .map(|(ip, counter)| (ip.as_str(), counter.posts()))
                .collect();

            make_n_calls(client, requests_per_attempt).await;

            last_observed = request_counter
                .counter
                .iter()
                .filter_map(|(ip, counter)| {
                    let new_posts = counter.posts() - posts_before[ip.as_str()];
                    (new_posts > 0).then_some((ip.as_str(), new_posts))
                })
                .collect();
            last_observed.sort_unstable_by_key(|(ip, _)| *ip);
            let total_new_posts: usize = last_observed.iter().map(|(_, posts)| posts).sum();

            if total_new_posts == requests_per_attempt
                && last_observed.len() == expected_ips.len()
                && last_observed
                    .iter()
                    .zip(&expected_ips)
                    .all(|((observed, _), expected)| observed == expected)
            {
                break;
            }
            tokio::time::sleep(POLLING_INTERVAL).await;
        }
    })
    .await;

    result.unwrap_or_else(|_| {
        panic!(
            "request routing did not converge within {:?}; expected nodes {:?}, last POST deltas {:?}",
            POLLING_TIMEOUT, expected_ips, last_observed
        )
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn recognizes_only_system_topology_scans() {
        for table_name in [SYSTEM_LOCAL_TABLE, SYSTEM_PEERS_TABLE] {
            let body = format!(r#"{{"TableName":"{table_name}"}}"#);
            assert!(is_topology_scan(
                Some(DYNAMODB_SCAN_TARGET),
                body.as_bytes()
            ));
        }

        assert!(!is_topology_scan(
            Some(DYNAMODB_SCAN_TARGET),
            br#"{"TableName":"user_table"}"#,
        ));
        assert!(!is_topology_scan(
            Some("DynamoDB_20120810.DescribeTable"),
            br#"{"TableName":".scylla.alternator.system.local"}"#,
        ));
    }
}
