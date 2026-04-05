use crate::ccm_wrapper::ccm::*;
use crate::ccm_wrapper::cluster::*;
use crate::ccm_wrapper::topology_spec::*;
use crate::load_balancing::proxy;
use crate::load_balancing::scope_utils;

use alternator_driver::AlternatorClient;
use alternator_driver::AlternatorConfig;
use alternator_driver::RoutingScope;
use hyper::Method;
use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use ctor::dtor;
use tokio::sync::{Mutex, MutexGuard};

const PROXY_PORT: u16 = 7999;
const ALTERNATOR_PORT: u16 = 8000;

// The client will use different refresh intervals for idle mode (when nothing happened for a while)
// and active mode, when calls are being made. Exact value might change later.
const ACTIVE_INTERVAL: Duration = Duration::from_secs(1);

// Since cluster creation is expensive, we create it once and reuse it for every test.
// Before a test gets access to the cluster, we make sure that all nodes are up and their ports are set to default.
// Datacenter 1 is a single node which is meant to never be shut down. Its address will be used as a seed address
// for clients and as a redirect target for requests directed to shut down nodes.
static CLUSTER: OnceLock<Mutex<Cluster>> = OnceLock::new();
async fn get_cluster() -> MutexGuard<'static, Cluster> {
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

fn default_endpoint_url(cluster: &Cluster) -> String {
    cluster.datacenters()[0].racks()[0].nodes()[0].address()
}

fn redirect_target_node(cluster: &Cluster) -> Node {
    cluster.datacenters()[0].racks()[0].nodes()[0].clone()
}

// Struct for counting requests made to the proxy. GETs and POSTs are counted separately.
// GETs are service discovery calls, and POSTs are the actual calls to DB.
#[derive(Debug)]
pub(crate) struct NodeCounter {
    posts: AtomicUsize,
    gets: AtomicUsize,
}

impl NodeCounter {
    fn new() -> Self {
        Self {
            posts: AtomicUsize::new(0),
            gets: AtomicUsize::new(0),
        }
    }

    pub(crate) fn posts(&self) -> usize {
        self.posts.load(Ordering::Relaxed)
    }

    fn reset(&self) {
        self.posts.store(0, Ordering::Relaxed);
        self.gets.store(0, Ordering::Relaxed);
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
                    match *req.method() {
                        Method::POST => node_counter.posts.fetch_add(1, Ordering::Relaxed),
                        Method::GET => node_counter.gets.fetch_add(1, Ordering::Relaxed),
                        _ => 0,
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

// This proxy is used in tests where we check if calls are made to a node that is down.
// Redirecting calls to a different node allows us to count calls to the stopped node,
// while also allowing the client to use it for discovery without connection errors.
pub(crate) async fn start_redirecting_proxy(
    from: &Node,
    to: &Node,
    request_counter: Arc<NodeCounter>,
) {
    let listen_addr = format!("{}:{}", from.ip.clone(), from.alternator_port);
    let connect_addr = format!("{}:{}", to.ip.clone(), ALTERNATOR_PORT);
    start_counting_proxy(listen_addr, connect_addr, request_counter).await;
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

pub(crate) async fn start_redirecting_proxies(
    cluster: &mut Cluster,
    proxy_port: u16,
    request_counter: &RequestCounter,
) {
    cluster.update_all_nodes_port(proxy_port);
    let to = &redirect_target_node(cluster);
    let counter = &request_counter.counter;
    for node in cluster.nodes() {
        let node_counter = Arc::clone(counter.get(&node.ip).unwrap());
        start_redirecting_proxy(node, to, node_counter).await;
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
}

// Make calls without caring about the result, used to count where calls are directed.
async fn make_n_calls(client: &AlternatorClient, n: usize) {
    for _ in 0..n {
        let _ = client.list_tables().send().await;
    }
}

// Create a basic client with scope.
fn create_client_with_scope(cluster: &Cluster, scope: RoutingScope) -> AlternatorClient {
    AlternatorClient::from_conf(
        AlternatorConfig::builder()
            .credentials_provider(
                aws_sdk_dynamodb::config::Credentials::for_tests_with_session_token(),
            )
            .region(aws_sdk_dynamodb::config::Region::new("eu-central-1"))
            .behavior_version_latest()
            .endpoint_url(default_endpoint_url(cluster))
            .routing_scope(scope)
            .build(),
    )
}

#[tokio::test]
#[cfg_attr(not(ccm_tests), ignore)]
async fn calls_correct_datacenter_scope_test() {
    let mut guard = get_cluster().await;
    let cluster = &mut *guard;

    let request_counter = RequestCounter::from_cluster(cluster);
    start_proxies(cluster, PROXY_PORT, &request_counter).await;

    let scope = scope_utils::datacenter_scope_from_index(cluster, 1);
    let client = create_client_with_scope(cluster, scope.clone());

    tokio::time::sleep(ACTIVE_INTERVAL).await;
    let n = 20;
    make_n_calls(&client, n).await;

    // Check if it called its scope, and only its scope.
    assert!(
        request_counter.get_posts_to_ips(scope_utils::ips_in_scope(cluster, &scope).as_slice())
            >= n
    );
    assert_eq!(
        request_counter
            .get_posts_to_other_ips(scope_utils::ips_in_scope(cluster, &scope).as_slice()),
        0
    );
}

#[tokio::test]
#[cfg_attr(not(ccm_tests), ignore)]
async fn calls_correct_rack_scope_test() {
    let mut guard = get_cluster().await;
    let cluster = &mut *guard;

    let request_counter = RequestCounter::from_cluster(cluster);
    start_proxies(cluster, PROXY_PORT, &request_counter).await;

    let scope = scope_utils::rack_scope_from_index(cluster, 1, 1);
    let client = create_client_with_scope(cluster, scope.clone());

    tokio::time::sleep(ACTIVE_INTERVAL).await;
    let n = 20;
    make_n_calls(&client, n).await;

    assert!(
        request_counter.get_posts_to_ips(scope_utils::ips_in_scope(cluster, &scope).as_slice())
            >= n
    );
    assert_eq!(
        request_counter
            .get_posts_to_other_ips(scope_utils::ips_in_scope(cluster, &scope).as_slice()),
        0
    );
}

#[tokio::test]
#[cfg_attr(not(ccm_tests), ignore)]
async fn node_shut_down_test() {
    let mut guard = get_cluster().await;
    let cluster = &mut *guard;

    let scope = scope_utils::datacenter_scope_from_index(cluster, 1);
    let redirect_node = redirect_target_node(cluster);

    let client = create_client_with_scope(cluster, scope.clone());

    // This counter holds all nodes that were shut down, sum of its counters should always be 0.
    let mut request_counter = RequestCounter::new();
    while let Some(node) = scope_utils::scope_first_working_node_mut(cluster, &scope) {
        tokio::time::sleep(ACTIVE_INTERVAL).await;
        make_n_calls(&client, 10).await;
        assert_eq!(request_counter.total_posts(), 0);
        Ccm::stop_node(node).unwrap();
        request_counter.add(node.ip.clone());
        start_redirecting_proxy(node, &redirect_node, request_counter.get(&node.ip)).await;
    }
}

// In this test we check that when the scope fails during client work,
// the client starts using only the fallback scope.
// This tests fallback behavior when localnodes returns an empty list.
// Here every call to every node is redirected to the redirect node, because we want to monitor both calls to
// working and not working nodes.
#[tokio::test]
#[cfg_attr(not(ccm_tests), ignore)]
async fn scope_fallback_test() {
    let mut guard = get_cluster().await;
    let cluster = &mut *guard;

    let request_counter = RequestCounter::from_cluster(cluster);

    start_redirecting_proxies(cluster, PROXY_PORT, &request_counter).await;

    let fallback_scope = scope_utils::datacenter_scope_from_index(cluster, 1);
    let scope =
        scope_utils::rack_scope_from_index(cluster, 1, 1).with_fallback(fallback_scope.clone());

    let client = create_client_with_scope(cluster, scope.clone());
    make_n_calls(&client, 5).await;

    scope_utils::shut_down_scope(cluster, &scope);
    tokio::time::sleep(ACTIVE_INTERVAL).await;
    request_counter.reset();

    let n = 20;
    make_n_calls(&client, n).await;
    let ips = scope_utils::working_nodes_ips_in_scope(cluster, &fallback_scope);
    assert!(request_counter.get_posts_to_ips(ips.as_slice()) >= n);
    assert_eq!(request_counter.get_posts_to_other_ips(ips.as_slice()), 0);
}

// Test if client switches to higher priority fallback once it starts working.
#[tokio::test]
#[cfg_attr(not(ccm_tests), ignore)]
async fn primary_scope_recover_test() {
    let mut guard = get_cluster().await;
    let cluster = &mut *guard;

    let fallback_scope = scope_utils::datacenter_scope_from_index(cluster, 1);
    let scope =
        scope_utils::rack_scope_from_index(cluster, 1, 1).with_fallback(fallback_scope.clone());

    scope_utils::shut_down_scope(cluster, &scope);

    let request_counter = RequestCounter::from_cluster(cluster);
    start_redirecting_proxies(cluster, PROXY_PORT, &request_counter).await;

    let client = create_client_with_scope(cluster, scope.clone());
    tokio::time::sleep(ACTIVE_INTERVAL).await;

    let n = 20;
    make_n_calls(&client, n).await;

    let ips = scope_utils::ips_in_scope(cluster, &fallback_scope);
    assert!(request_counter.get_posts_to_ips(ips.as_slice()) >= n);
    assert_eq!(request_counter.get_posts_to_other_ips(ips.as_slice()), 0);

    // Start one node in main scope.
    let mut nodes = scope_utils::nodes_in_scope_mut(cluster, &scope);
    let node_to_start = &mut nodes[0];
    Ccm::start_node(node_to_start).unwrap();
    tokio::time::sleep(ACTIVE_INTERVAL).await;

    request_counter.reset();
    make_n_calls(&client, n).await;

    let ip = node_to_start.ip.as_str();
    assert!(request_counter.get_posts_to_ips(&[ip]) >= n);
    assert_eq!(request_counter.get_posts_to_other_ips(&[ip]), 0);
}

// If a bad scope is given, the client should call only the seed node.
#[tokio::test]
#[cfg_attr(not(ccm_tests), ignore)]
async fn bad_scope_test() {
    let mut guard = get_cluster().await;
    let cluster = &mut *guard;

    let request_counter = RequestCounter::from_cluster(cluster);
    start_proxies(cluster, PROXY_PORT, &request_counter).await;

    let scope = RoutingScope::from_datacenter("fake_dc".to_string());
    let client = create_client_with_scope(cluster, scope.clone());
    let n = 20;
    make_n_calls(&client, n).await;
    // With a bad scope, the client should call only the seed.
    let seed_url = default_endpoint_url(cluster);
    let seed_ip = seed_url
        .strip_prefix("http://")
        .unwrap()
        .split(':')
        .next()
        .unwrap();

    assert!(request_counter.get_posts_to_ips(&[seed_ip]) >= n);
    assert_eq!(request_counter.get_posts_to_other_ips(&[seed_ip]), 0);
}

// Check only if the restarted node gets requests from client.
#[tokio::test]
#[cfg_attr(not(ccm_tests), ignore)]
async fn node_restart_test() {
    let mut guard = get_cluster().await;
    let cluster = &mut *guard;

    let scope = scope_utils::datacenter_scope_from_index(cluster, 1);

    // Store only its IP, so the borrow checker doesn't complain.
    let stopped_node_ip = {
        let node_to_stop = scope_utils::scope_first_working_node_mut(cluster, &scope).unwrap();
        let ip = node_to_stop.ip.clone();
        Ccm::stop_node(node_to_stop).unwrap();
        ip
    };

    let request_counter = RequestCounter::from_cluster(cluster);
    start_proxies(cluster, PROXY_PORT, &request_counter).await;

    let client = create_client_with_scope(cluster, scope.clone());
    make_n_calls(&client, 5).await;

    // Start the node and its proxy.
    let restarted_node = {
        let nodes = cluster.nodes_mut();
        let restarted_node = nodes
            .into_iter()
            .find(|node| node.ip == stopped_node_ip)
            .unwrap();
        Ccm::start_node(restarted_node).unwrap();
        restarted_node
    };
    let counter = request_counter.get(&stopped_node_ip);
    start_proxy_on_node(restarted_node.clone(), PROXY_PORT, counter.clone()).await;

    tokio::time::sleep(ACTIVE_INTERVAL).await;
    make_n_calls(&client, 20).await;
    assert!(counter.posts() > 0);
}

#[tokio::test]
#[cfg_attr(not(ccm_tests), ignore)]
async fn round_robin_test() {
    let mut guard = get_cluster().await;
    let cluster = &mut *guard;

    let request_counter = RequestCounter::from_cluster(cluster);
    start_proxies(cluster, PROXY_PORT, &request_counter).await;

    let scope = scope_utils::datacenter_scope_from_index(cluster, 1);
    let client = create_client_with_scope(cluster, scope.clone());
    tokio::time::sleep(ACTIVE_INTERVAL).await;

    let n = 10;

    let node_ips: Vec<&str> = scope_utils::nodes_in_scope(cluster, &scope)
        .iter()
        .map(|node| node.ip.as_str())
        .collect();

    let nodes_count = node_ips.len();

    for _ in 0..n {
        // Make one request per node in the scope
        make_n_calls(&client, nodes_count).await;

        // Verify each node was called exactly once
        for ip in &node_ips {
            assert_eq!(request_counter.get(ip).posts(), 1);
        }

        request_counter.reset();
    }
}
