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

pub(crate) const POLLING_TIMEOUT: Duration = Duration::from_secs(5);
pub(crate) const POLLING_INTERVAL: Duration = Duration::from_millis(50);

// Since cluster creation is expensive, we create it once and reuse it for every test.
// Before a test gets access to the cluster, we make sure that all nodes are up and their ports are set to default.
// Datacenter 1 is a single node which is meant to never be shut down. Its address will be used as a seed address
// for clients and as a redirect target for requests directed to shut down nodes.
static CLUSTER: OnceLock<Mutex<Cluster>> = OnceLock::new();
pub(crate) async fn get_cluster() -> MutexGuard<'static, Cluster> {
    let mut cluster = CLUSTER
        .get_or_init(|| {
            let topology = TopologySpecBuilder::new()
                .datacenter(DatacenterSpec::new().rack(1))
                .datacenter(DatacenterSpec::new().rack(1).rack(2))
                .datacenter(DatacenterSpec::new().rack(2).rack(1))
                .build()
                .unwrap();
            let ip_prefix = IpPrefix::new("127.0.1.").unwrap();
            let cluster_name = format!("test_cluster_{}", uuid::Uuid::new_v4());
            let scylla_version = String::from("release:2025.1");
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

pub(crate) fn default_endpoint_url(cluster: &Cluster) -> String {
    cluster.datacenters()[0].racks()[0].nodes()[0].address()
}

// Struct for counting requests made to the proxy. GETs, POSTs, and describe_tables are counted separately.
// GETs are service discovery calls, POSTs are the actual calls to DB, and describe_table calls
// are from PartitionKeyResolver.
#[derive(Debug)]
pub(crate) struct NodeCounter {
    posts: AtomicUsize,
    gets: AtomicUsize,
    describe_tables: AtomicUsize,
}

impl NodeCounter {
    fn new() -> Self {
        Self {
            posts: AtomicUsize::new(0),
            gets: AtomicUsize::new(0),
            describe_tables: AtomicUsize::new(0),
        }
    }

    pub(crate) fn posts(&self) -> usize {
        self.posts.load(Ordering::Relaxed)
    }

    fn reset(&self) {
        self.posts.store(0, Ordering::Relaxed);
        self.gets.store(0, Ordering::Relaxed);
        self.describe_tables.store(0, Ordering::Relaxed);
    }
}

// Struct with a hashmap underneath for holding and monitoring the counters for multiple nodes.
#[derive(Debug)]
pub(crate) struct RequestCounter {
    counter: HashMap<String, Arc<NodeCounter>>,
}

impl RequestCounter {
    pub(crate) fn new() -> Self {
        Self {
            counter: HashMap::new(),
        }
    }

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

    pub(crate) fn add(&mut self, ip: String) {
        self.counter.insert(ip, Arc::new(NodeCounter::new()));
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
    let proxy = proxy::Proxy::start(
        listen_addr,
        connect_addr,
        move |req, send| {
            let node_counter = Arc::clone(&request_counter);
            async move {
                {
                    let is_describe_table = req
                        .headers()
                        .get("x-amz-target")
                        .is_some_and(|h| h == "DynamoDB_20120810.DescribeTable");

                    match *req.method() {
                        Method::POST => {
                            if is_describe_table {
                                node_counter.describe_tables.fetch_add(1, Ordering::Relaxed);
                            } else {
                                node_counter.posts.fetch_add(1, Ordering::Relaxed);
                            }
                        }
                        Method::GET => {
                            node_counter.gets.fetch_add(1, Ordering::Relaxed);
                        }
                        _ => {}
                    };
                }
                proxy::forward_on_request(req, send).await
            }
        },
        None,
        None,
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
        .behavior_version_latest()
}

// Create a basic client with scope.
pub(crate) fn create_client_with_scope(cluster: &Cluster, scope: RoutingScope) -> AlternatorClient {
    AlternatorClient::from_conf(
        minimal_builder()
            .endpoint_url(default_endpoint_url(cluster))
            .routing_scope(scope)
            .build(),
    )
}

// Poll until the client's live nodes match the given IPs, or timeout.
pub(crate) async fn wait_until_live_nodes_match(client: &AlternatorClient, ips: Vec<&str>) {
    let live_nodes = client.config().live_nodes().unwrap().clone();
    tokio::time::timeout(POLLING_TIMEOUT, async {
        loop {
            let nodes = live_nodes.get_live_nodes();
            let node_ips: Vec<&str> = nodes.iter().map(|url| url.host_str().unwrap()).collect();
            if node_ips.len() == ips.len() && node_ips.iter().all(|ip| ips.contains(ip)) {
                break;
            }
            tokio::time::sleep(POLLING_INTERVAL).await;
        }
    })
    .await
    .unwrap_or_else(|_| {
        panic!(
            "failed to update nodes\nexpected nodes {:?}, timed out after {:?}",
            ips, POLLING_TIMEOUT
        )
    });
}
