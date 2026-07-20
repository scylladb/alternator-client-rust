use crate::ccm_wrapper::ccm::*;
use crate::ccm_wrapper::cluster::*;
use crate::load_balancing::cluster_utils::*;
use crate::load_balancing::proxy;
use crate::load_balancing::scope_utils;

use alternator_driver::AlternatorClient;
use alternator_driver::RoutingScope;
use alternator_driver::keyrouting::affinity_config::{
    KeyRouteAffinityConfig, KeyRouteAffinityType,
};
use alternator_driver::keyrouting::{go_rand::GoRand, hasher};
use aws_sdk_dynamodb::types::{
    AttributeAction, AttributeValue, AttributeValueUpdate, DeleteRequest, KeysAndAttributes,
    PutRequest, ReturnValue, Select, WriteRequest,
};
use std::collections::HashMap;
use std::sync::Arc;

fn redirect_target_node(cluster: &Cluster) -> Node {
    cluster.datacenters()[0].racks()[0].nodes()[0].clone()
}

// This proxy is used in tests where we check if calls are made to a node that is down.
// Redirecting calls to a different node allows us to count calls to the stopped node,
// while also allowing the client to use it for discovery without connection errors.
async fn start_redirecting_proxy(from: &Node, to: &Node, request_counter: Arc<NodeCounter>) {
    let listen_addr = format!("{}:{}", from.ip.clone(), from.alternator_port);
    let connect_addr = format!("{}:{}", to.ip.clone(), ALTERNATOR_PORT);
    start_counting_proxy(listen_addr, connect_addr, request_counter).await;
}

async fn start_redirecting_proxies(
    cluster: &mut Cluster,
    proxy_port: u16,
    request_counter: &RequestCounter,
) {
    cluster.update_all_nodes_port(proxy_port);
    let to = &redirect_target_node(cluster);
    for node in cluster.nodes() {
        let node_counter = request_counter.get(&node.ip);
        start_redirecting_proxy(node, to, node_counter).await;
    }
}

async fn create_table(client: &AlternatorClient, table_name: &str) {
    client
        .create_table()
        .table_name(table_name)
        .attribute_definitions(
            aws_sdk_dynamodb::types::AttributeDefinition::builder()
                .attribute_name("id")
                .attribute_type(aws_sdk_dynamodb::types::ScalarAttributeType::S)
                .build()
                .unwrap(),
        )
        .key_schema(
            aws_sdk_dynamodb::types::KeySchemaElement::builder()
                .attribute_name("id")
                .key_type(aws_sdk_dynamodb::types::KeyType::Hash)
                .build()
                .unwrap(),
        )
        .billing_mode(aws_sdk_dynamodb::types::BillingMode::PayPerRequest)
        .send()
        .await
        .unwrap();
}

async fn put_item(client: &AlternatorClient, table_name: &str, item: &str) {
    let _ = client
        .put_item()
        .table_name(table_name)
        .item(
            "id",
            aws_sdk_dynamodb::types::AttributeValue::S(item.to_string()),
        )
        .send()
        .await;
}

async fn delete_item(client: &AlternatorClient, table_name: &str, item: &str) {
    let _ = client
        .delete_item()
        .table_name(table_name)
        .key(
            "id",
            aws_sdk_dynamodb::types::AttributeValue::S(item.to_string()),
        )
        .send()
        .await;
}

async fn update_item(client: &AlternatorClient, table_name: &str, item: &str, new: &str) {
    let _ = client
        .update_item()
        .table_name(table_name)
        .key(
            "id",
            aws_sdk_dynamodb::types::AttributeValue::S(item.to_string()),
        )
        .update_expression("SET val = :v")
        .expression_attribute_values(
            ":v",
            aws_sdk_dynamodb::types::AttributeValue::S(new.to_string()),
        )
        .send()
        .await;
}

fn create_client_with_scope_and_seed_hosts(
    cluster: &Cluster,
    scope: RoutingScope,
    seed_hosts: Vec<String>,
) -> AlternatorClient {
    AlternatorClient::from_conf(
        minimal_builder()
            .scheme("http")
            .port(cluster.nodes()[0].alternator_port)
            .seed_hosts(seed_hosts)
            .routing_scope(scope)
            .build(),
    )
}

fn first_working_seed_per_datacenter(cluster: &Cluster) -> Vec<String> {
    cluster
        .datacenters()
        .iter()
        .filter_map(|datacenter| {
            datacenter
                .racks()
                .iter()
                .flat_map(|rack| rack.nodes().iter())
                .find(|node| node.is_up)
                .map(|node| node.ip.clone())
        })
        .collect()
}

fn create_client_with_scope_and_affinity(
    cluster: &Cluster,
    scope: RoutingScope,
    affinity_config: KeyRouteAffinityConfig,
) -> AlternatorClient {
    AlternatorClient::from_conf(
        minimal_builder()
            .endpoint_url(default_endpoint_url(cluster))
            .routing_scope(scope)
            .key_route_affinity(affinity_config)
            .build(),
    )
}

fn expected_first_node<'a>(nodes: &'a [&str], partition_key_value: &str) -> &'a str {
    let pk = AttributeValue::S(partition_key_value.to_string());
    let hash = hasher::hash_attribute_value(&pk).unwrap();
    let mut nodes = nodes.to_vec();
    nodes.sort_unstable();

    let mut rng = GoRand::new(hash as i64);
    let idx = rng.intn(nodes.len() as i32) as usize;
    nodes[idx]
}

fn find_two_keys_on_one_node_and_one_on_another(
    nodes: &[&str],
    key_prefix: &str,
) -> (String, String, String, String) {
    let mut buckets: HashMap<String, Vec<String>> = HashMap::new();

    for i in 0..1000 {
        let key = format!("{key_prefix}_{i}");
        let node = expected_first_node(nodes, &key).to_string();
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ExpectedRouting {
    Affinity,
    RoundRobin,
}

#[derive(Debug, Clone, Copy)]
enum MatrixOperation {
    PutPlain,
    PutConditional,
    DeletePlain,
    DeleteConditional,
    UpdateLegacyPut,
    UpdateLegacyAdd,
    UpdateLegacyDeleteWithValue,
    UpdateExpression,
    UpdateLegacyPutReturnUpdatedNew,
    UpdateLegacyPutReturnAllNew,
    BatchWritePut,
    BatchWriteDelete,
    GetItem,
    BatchGetItem,
    Query,
    Scan,
    ListTables,
}

impl MatrixOperation {
    const ALL: [Self; 17] = [
        MatrixOperation::PutPlain,
        MatrixOperation::PutConditional,
        MatrixOperation::DeletePlain,
        MatrixOperation::DeleteConditional,
        MatrixOperation::UpdateLegacyPut,
        MatrixOperation::UpdateLegacyAdd,
        MatrixOperation::UpdateLegacyDeleteWithValue,
        MatrixOperation::UpdateExpression,
        MatrixOperation::UpdateLegacyPutReturnUpdatedNew,
        MatrixOperation::UpdateLegacyPutReturnAllNew,
        MatrixOperation::BatchWritePut,
        MatrixOperation::BatchWriteDelete,
        MatrixOperation::GetItem,
        MatrixOperation::BatchGetItem,
        MatrixOperation::Query,
        MatrixOperation::Scan,
        MatrixOperation::ListTables,
    ];

    fn name(self) -> &'static str {
        match self {
            MatrixOperation::PutPlain => "put_plain",
            MatrixOperation::PutConditional => "put_conditional",
            MatrixOperation::DeletePlain => "delete_plain",
            MatrixOperation::DeleteConditional => "delete_conditional",
            MatrixOperation::UpdateLegacyPut => "update_legacy_put",
            MatrixOperation::UpdateLegacyAdd => "update_legacy_add",
            MatrixOperation::UpdateLegacyDeleteWithValue => "update_legacy_delete_with_value",
            MatrixOperation::UpdateExpression => "update_expression",
            MatrixOperation::UpdateLegacyPutReturnUpdatedNew => {
                "update_legacy_put_return_updated_new"
            }
            MatrixOperation::UpdateLegacyPutReturnAllNew => "update_legacy_put_return_all_new",
            MatrixOperation::BatchWritePut => "batch_write_put",
            MatrixOperation::BatchWriteDelete => "batch_write_delete",
            MatrixOperation::GetItem => "get_item",
            MatrixOperation::BatchGetItem => "batch_get_item",
            MatrixOperation::Query => "query",
            MatrixOperation::Scan => "scan",
            MatrixOperation::ListTables => "list_tables",
        }
    }
}

#[derive(Debug, Clone, Copy)]
struct OperationMatrixCase {
    mode: KeyRouteAffinityType,
    operation: MatrixOperation,
    expected_routing: ExpectedRouting,
}

fn mode_name(mode: KeyRouteAffinityType) -> &'static str {
    match mode {
        KeyRouteAffinityType::None => "none",
        KeyRouteAffinityType::Rmw => "rmw",
        KeyRouteAffinityType::AnyWrite => "any_write",
    }
}

fn s(value: impl Into<String>) -> AttributeValue {
    AttributeValue::S(value.into())
}

fn key_map(key: &str) -> HashMap<String, AttributeValue> {
    HashMap::from([("id".to_string(), s(key))])
}

async fn send_matrix_operation(
    client: &AlternatorClient,
    table_name: &str,
    operation: MatrixOperation,
    key: &str,
    iteration: usize,
) {
    match operation {
        MatrixOperation::PutPlain => {
            let _ = client
                .put_item()
                .table_name(table_name)
                .item("id", s(key))
                .item("val", s(format!("value_{iteration}")))
                .send()
                .await;
        }
        MatrixOperation::PutConditional => {
            let _ = client
                .put_item()
                .table_name(table_name)
                .item("id", s(key))
                .item("val", s(format!("value_{iteration}")))
                .condition_expression("attribute_not_exists(missing_attr)")
                .send()
                .await;
        }
        MatrixOperation::DeletePlain => {
            let _ = client
                .delete_item()
                .table_name(table_name)
                .key("id", s(key))
                .send()
                .await;
        }
        MatrixOperation::DeleteConditional => {
            let _ = client
                .delete_item()
                .table_name(table_name)
                .key("id", s(key))
                .condition_expression("attribute_not_exists(missing_attr)")
                .send()
                .await;
        }
        MatrixOperation::UpdateLegacyPut => {
            let _ = client
                .update_item()
                .table_name(table_name)
                .key("id", s(key))
                .attribute_updates(
                    "val",
                    AttributeValueUpdate::builder()
                        .action(AttributeAction::Put)
                        .value(s(format!("value_{iteration}")))
                        .build(),
                )
                .send()
                .await;
        }
        MatrixOperation::UpdateLegacyAdd => {
            let _ = client
                .update_item()
                .table_name(table_name)
                .key("id", s(key))
                .attribute_updates(
                    "counter",
                    AttributeValueUpdate::builder()
                        .action(AttributeAction::Add)
                        .value(AttributeValue::N("1".to_string()))
                        .build(),
                )
                .send()
                .await;
        }
        MatrixOperation::UpdateLegacyDeleteWithValue => {
            let _ = client
                .update_item()
                .table_name(table_name)
                .key("id", s(key))
                .attribute_updates(
                    "tags",
                    AttributeValueUpdate::builder()
                        .action(AttributeAction::Delete)
                        .value(AttributeValue::Ss(vec!["tag".to_string()]))
                        .build(),
                )
                .send()
                .await;
        }
        MatrixOperation::UpdateExpression => {
            let _ = client
                .update_item()
                .table_name(table_name)
                .key("id", s(key))
                .update_expression("SET val = :v")
                .expression_attribute_values(":v", s(format!("value_{iteration}")))
                .send()
                .await;
        }
        MatrixOperation::UpdateLegacyPutReturnUpdatedNew => {
            let _ = client
                .update_item()
                .table_name(table_name)
                .key("id", s(key))
                .attribute_updates(
                    "val",
                    AttributeValueUpdate::builder()
                        .action(AttributeAction::Put)
                        .value(s(format!("value_{iteration}")))
                        .build(),
                )
                .return_values(ReturnValue::UpdatedNew)
                .send()
                .await;
        }
        MatrixOperation::UpdateLegacyPutReturnAllNew => {
            let _ = client
                .update_item()
                .table_name(table_name)
                .key("id", s(key))
                .attribute_updates(
                    "val",
                    AttributeValueUpdate::builder()
                        .action(AttributeAction::Put)
                        .value(s(format!("value_{iteration}")))
                        .build(),
                )
                .return_values(ReturnValue::AllNew)
                .send()
                .await;
        }
        MatrixOperation::BatchWritePut => {
            let put = PutRequest::builder()
                .item("id", s(key))
                .item("val", s(format!("value_{iteration}")))
                .build()
                .unwrap();
            let write = WriteRequest::builder().put_request(put).build();

            let _ = client
                .batch_write_item()
                .request_items(table_name, vec![write])
                .send()
                .await;
        }
        MatrixOperation::BatchWriteDelete => {
            let delete = DeleteRequest::builder().key("id", s(key)).build().unwrap();
            let write = WriteRequest::builder().delete_request(delete).build();

            let _ = client
                .batch_write_item()
                .request_items(table_name, vec![write])
                .send()
                .await;
        }
        MatrixOperation::GetItem => {
            let _ = client
                .get_item()
                .table_name(table_name)
                .key("id", s(key))
                .consistent_read(true)
                .send()
                .await;
        }
        MatrixOperation::BatchGetItem => {
            let keys = KeysAndAttributes::builder()
                .keys(key_map(key))
                .consistent_read(true)
                .build()
                .unwrap();

            let _ = client
                .batch_get_item()
                .request_items(table_name, keys)
                .send()
                .await;
        }
        MatrixOperation::Query => {
            let _ = client
                .query()
                .table_name(table_name)
                .key_condition_expression("id = :id")
                .expression_attribute_values(":id", s(key))
                .select(Select::Count)
                .send()
                .await;
        }
        MatrixOperation::Scan => {
            let _ = client
                .scan()
                .table_name(table_name)
                .select(Select::Count)
                .send()
                .await;
        }
        MatrixOperation::ListTables => {
            let _ = client.list_tables().send().await;
        }
    }
}

fn assert_affinity_counts(
    request_counter: &RequestCounter,
    node_ips: &[&str],
    expected_ip: &str,
    expected_requests: usize,
    case_label: &str,
) {
    assert_eq!(
        request_counter.total_posts(),
        expected_requests,
        "{case_label}: unexpected total POST count"
    );

    for ip in node_ips {
        let expected = if *ip == expected_ip {
            expected_requests
        } else {
            0
        };
        assert_eq!(
            request_counter.get(ip).posts(),
            expected,
            "{case_label}: unexpected POST count for {ip}"
        );
    }
}

fn assert_round_robin_counts(
    request_counter: &RequestCounter,
    node_ips: &[&str],
    case_label: &str,
) {
    assert_eq!(
        request_counter.total_posts(),
        node_ips.len(),
        "{case_label}: unexpected total POST count"
    );

    for ip in node_ips {
        assert_eq!(
            request_counter.get(ip).posts(),
            1,
            "{case_label}: round-robin request did not visit {ip} exactly once"
        );
    }

    assert_eq!(
        request_counter.get_posts_to_other_ips(node_ips),
        0,
        "{case_label}: request escaped the selected routing scope"
    );
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

    wait_until_live_nodes_match(
        &client,
        scope_utils::working_nodes_ips_in_scope(cluster, &scope),
    )
    .await;
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

    wait_until_live_nodes_match(
        &client,
        scope_utils::working_nodes_ips_in_scope(cluster, &scope),
    )
    .await;
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
async fn calls_correct_cluster_scope_test() {
    let mut guard = get_cluster().await;
    let cluster = &mut *guard;

    let request_counter = RequestCounter::from_cluster(cluster);
    start_proxies(cluster, PROXY_PORT, &request_counter).await;

    let scope = RoutingScope::from_cluster();
    let client = create_client_with_scope_and_seed_hosts(
        cluster,
        scope.clone(),
        first_working_seed_per_datacenter(cluster),
    );
    let live_node_ips = scope_utils::working_nodes_ips_in_scope(cluster, &scope);

    wait_until_live_nodes_match(&client, live_node_ips.clone()).await;

    request_counter.reset();
    make_n_calls(&client, live_node_ips.len()).await;

    assert_round_robin_counts(&request_counter, &live_node_ips, "cluster scope");
}

#[tokio::test]
#[cfg_attr(not(ccm_tests), ignore)]
async fn dns_entrypoint_discovers_live_cluster_nodes_test() {
    let mut guard = get_cluster().await;
    let cluster = &mut *guard;
    let request_counter = RequestCounter::from_cluster(cluster);
    let target_ip = cluster
        .nodes()
        .into_iter()
        .find(|node| node.is_up)
        .unwrap()
        .ip
        .clone();
    let dns_entrypoint_proxy = proxy::Proxy::start(
        "localhost:0".to_string(),
        format!("{target_ip}:{ALTERNATOR_PORT}"),
        |request, send| async move { proxy::forward_on_request(request, send).await },
        None,
        None,
    )
    .await;
    let proxy_port = dns_entrypoint_proxy.address().port();
    tokio::spawn(async move {
        dns_entrypoint_proxy.run().await;
    });
    start_proxies(cluster, proxy_port, &request_counter).await;

    let client = AlternatorClient::from_conf(
        minimal_builder()
            .scheme("http")
            .port(proxy_port)
            .seed_hosts(vec!["localhost".to_string()])
            .routing_scope(RoutingScope::from_cluster())
            .build(),
    );
    let live_nodes = client.config().live_nodes().unwrap().clone();

    live_nodes.update_live_nodes().await;

    let discovered = live_nodes.get_live_nodes();
    let hosts: Vec<&str> = discovered.iter().filter_map(|url| url.host_str()).collect();
    assert!(!hosts.is_empty(), "DNS entrypoint should discover nodes");
    assert!(
        hosts.iter().all(|host| *host != "localhost"),
        "DNS entrypoint should be replaced by live cluster node records, got {hosts:?}"
    );

    request_counter.reset();
    make_n_calls(&client, hosts.len()).await;
    assert_round_robin_counts(&request_counter, &hosts, "DNS entrypoint cluster scope");
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
    loop {
        let ips_owned: Vec<String> = scope_utils::working_nodes_ips_in_scope(cluster, &scope)
            .into_iter()
            .map(str::to_owned)
            .collect();
        let Some(node) = scope_utils::scope_first_working_node_mut(cluster, &scope) else {
            break;
        };
        let ips: Vec<&str> = ips_owned.iter().map(String::as_str).collect();
        wait_until_live_nodes_match(&client, ips).await;
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
    wait_until_live_nodes_match(
        &client,
        scope_utils::working_nodes_ips_in_scope(cluster, &fallback_scope),
    )
    .await;

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
    wait_until_live_nodes_match(
        &client,
        scope_utils::working_nodes_ips_in_scope(cluster, &fallback_scope),
    )
    .await;

    let n = 20;
    make_n_calls(&client, n).await;

    let ips = scope_utils::ips_in_scope(cluster, &fallback_scope);
    assert!(request_counter.get_posts_to_ips(ips.as_slice()) >= n);
    assert_eq!(request_counter.get_posts_to_other_ips(ips.as_slice()), 0);

    // Start one node in main scope.
    let mut nodes = scope_utils::nodes_in_scope_mut(cluster, &scope);
    let node_to_start = &mut nodes[0];
    Ccm::start_node(node_to_start).unwrap();
    wait_until_live_nodes_match(&client, vec![node_to_start.ip.as_str()]).await;

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

    wait_until_live_nodes_match(
        &client,
        scope_utils::working_nodes_ips_in_scope(cluster, &scope),
    )
    .await;
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
    wait_until_live_nodes_match(
        &client,
        scope_utils::working_nodes_ips_in_scope(cluster, &scope),
    )
    .await;

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

/// Test if describe table is called exactly once when partition key info is not provided.
#[tokio::test]
#[cfg_attr(not(ccm_tests), ignore)]
async fn describe_table_called_exactly_once_without_config() {
    let mut guard = get_cluster().await;
    let cluster = &mut *guard;

    let scope = scope_utils::datacenter_scope_from_index(cluster, 1);
    let request_counter = RequestCounter::from_cluster(cluster);
    start_proxies(cluster, PROXY_PORT, &request_counter).await;

    let table_name = format!("test_table_{}", uuid::Uuid::new_v4());
    let affinity_config = KeyRouteAffinityConfig::builder()
        .with_type(KeyRouteAffinityType::AnyWrite)
        .build();
    let client = create_client_with_scope_and_affinity(cluster, scope.clone(), affinity_config);
    create_table(&client, &table_name).await;

    assert_eq!(request_counter.total_describe_tables(), 0);

    for i in 0..5 {
        put_item(&client, &table_name, &format!("key_{}", i + 1)).await;
    }

    tokio::time::timeout(POLLING_TIMEOUT, async {
        loop {
            if request_counter.total_describe_tables() == 1 {
                break;
            }
            tokio::time::sleep(POLLING_INTERVAL).await;
        }
    })
    .await
    .unwrap_or_else(|_| {
        panic!(
            "DescribeTable was not called exactly once; got {}",
            request_counter.total_describe_tables()
        )
    });

    for _ in 0..3 {
        for j in 0..=5 {
            let item = format!("key_{}", j);
            update_item(&client, &table_name, &item, &format!("new_{}", item)).await;
        }
    }

    assert_eq!(request_counter.total_describe_tables(), 1);

    for i in 0..5 {
        delete_item(&client, &table_name, &format!("key_{}", i + 1)).await;
    }

    assert_eq!(request_counter.total_describe_tables(), 1);
}

#[tokio::test]
#[cfg_attr(not(ccm_tests), ignore)]
async fn describe_table_not_called_with_config() {
    let mut guard = get_cluster().await;
    let cluster = &mut *guard;

    let scope = scope_utils::datacenter_scope_from_index(cluster, 1);
    let request_counter = RequestCounter::from_cluster(cluster);
    start_proxies(cluster, PROXY_PORT, &request_counter).await;

    let table_name = format!("test_table_{}", uuid::Uuid::new_v4());
    let affinity_config = KeyRouteAffinityConfig::builder()
        .with_type(KeyRouteAffinityType::AnyWrite)
        .with_pk_info(&table_name, "id")
        .build();
    let client = create_client_with_scope_and_affinity(cluster, scope.clone(), affinity_config);
    create_table(&client, &table_name).await;

    assert_eq!(request_counter.total_describe_tables(), 0);

    for i in 0..5 {
        put_item(&client, &table_name, &format!("key_{}", i + 1)).await;
    }

    assert_eq!(request_counter.total_describe_tables(), 0);

    for _ in 0..3 {
        for j in 0..=5 {
            let item = format!("key_{}", j);
            update_item(&client, &table_name, &item, &format!("new_{}", item)).await;
        }
    }

    assert_eq!(request_counter.total_describe_tables(), 0);

    for i in 0..5 {
        delete_item(&client, &table_name, &format!("key_{}", i + 1)).await;
    }

    assert_eq!(request_counter.total_describe_tables(), 0);
}

/// Matrix test for which DynamoDB operations use key-route affinity in each mode.
#[tokio::test]
#[cfg_attr(not(ccm_tests), ignore)]
async fn key_route_affinity_operation_matrix_test() {
    let mut guard = get_cluster().await;
    let cluster = &mut *guard;

    let scope = scope_utils::datacenter_scope_from_index(cluster, 1);
    let request_counter = RequestCounter::from_cluster(cluster);
    start_proxies(cluster, PROXY_PORT, &request_counter).await;

    let table_name = format!("test_table_{}", uuid::Uuid::new_v4());
    {
        // Drop the setup client before creating the three matrix clients: the
        // test proxy accepts up to three concurrent client connections.
        let setup_client = create_client_with_scope(cluster, scope.clone());
        wait_until_live_nodes_match(
            &setup_client,
            scope_utils::working_nodes_ips_in_scope(cluster, &scope),
        )
        .await;
        create_table(&setup_client, &table_name).await;
    }

    let none_client = create_client_with_scope_and_affinity(
        cluster,
        scope.clone(),
        KeyRouteAffinityConfig::builder()
            .with_type(KeyRouteAffinityType::None)
            .with_pk_info(&table_name, "id")
            .build(),
    );
    let rmw_client = create_client_with_scope_and_affinity(
        cluster,
        scope.clone(),
        KeyRouteAffinityConfig::builder()
            .with_type(KeyRouteAffinityType::Rmw)
            .with_pk_info(&table_name, "id")
            .build(),
    );
    let any_write_client = create_client_with_scope_and_affinity(
        cluster,
        scope.clone(),
        KeyRouteAffinityConfig::builder()
            .with_type(KeyRouteAffinityType::AnyWrite)
            .with_pk_info(&table_name, "id")
            .build(),
    );
    let node_ips = scope_utils::working_nodes_ips_in_scope(cluster, &scope);

    wait_until_live_nodes_match(&none_client, node_ips.clone()).await;
    wait_until_live_nodes_match(&rmw_client, node_ips.clone()).await;
    wait_until_live_nodes_match(&any_write_client, node_ips.clone()).await;

    let mut cases = vec![
        OperationMatrixCase {
            mode: KeyRouteAffinityType::Rmw,
            operation: MatrixOperation::PutPlain,
            expected_routing: ExpectedRouting::RoundRobin,
        },
        OperationMatrixCase {
            mode: KeyRouteAffinityType::Rmw,
            operation: MatrixOperation::PutConditional,
            expected_routing: ExpectedRouting::Affinity,
        },
        OperationMatrixCase {
            mode: KeyRouteAffinityType::Rmw,
            operation: MatrixOperation::DeletePlain,
            expected_routing: ExpectedRouting::RoundRobin,
        },
        OperationMatrixCase {
            mode: KeyRouteAffinityType::Rmw,
            operation: MatrixOperation::DeleteConditional,
            expected_routing: ExpectedRouting::Affinity,
        },
        OperationMatrixCase {
            mode: KeyRouteAffinityType::Rmw,
            operation: MatrixOperation::UpdateLegacyPut,
            expected_routing: ExpectedRouting::RoundRobin,
        },
        OperationMatrixCase {
            mode: KeyRouteAffinityType::Rmw,
            operation: MatrixOperation::UpdateLegacyAdd,
            expected_routing: ExpectedRouting::Affinity,
        },
        OperationMatrixCase {
            mode: KeyRouteAffinityType::Rmw,
            operation: MatrixOperation::UpdateLegacyDeleteWithValue,
            expected_routing: ExpectedRouting::Affinity,
        },
        OperationMatrixCase {
            mode: KeyRouteAffinityType::Rmw,
            operation: MatrixOperation::UpdateExpression,
            expected_routing: ExpectedRouting::Affinity,
        },
        OperationMatrixCase {
            mode: KeyRouteAffinityType::Rmw,
            operation: MatrixOperation::UpdateLegacyPutReturnUpdatedNew,
            expected_routing: ExpectedRouting::RoundRobin,
        },
        OperationMatrixCase {
            mode: KeyRouteAffinityType::Rmw,
            operation: MatrixOperation::UpdateLegacyPutReturnAllNew,
            expected_routing: ExpectedRouting::Affinity,
        },
        OperationMatrixCase {
            mode: KeyRouteAffinityType::Rmw,
            operation: MatrixOperation::BatchWritePut,
            expected_routing: ExpectedRouting::RoundRobin,
        },
        OperationMatrixCase {
            mode: KeyRouteAffinityType::Rmw,
            operation: MatrixOperation::BatchWriteDelete,
            expected_routing: ExpectedRouting::RoundRobin,
        },
        OperationMatrixCase {
            mode: KeyRouteAffinityType::Rmw,
            operation: MatrixOperation::GetItem,
            expected_routing: ExpectedRouting::RoundRobin,
        },
        OperationMatrixCase {
            mode: KeyRouteAffinityType::Rmw,
            operation: MatrixOperation::BatchGetItem,
            expected_routing: ExpectedRouting::RoundRobin,
        },
        OperationMatrixCase {
            mode: KeyRouteAffinityType::Rmw,
            operation: MatrixOperation::Query,
            expected_routing: ExpectedRouting::RoundRobin,
        },
        OperationMatrixCase {
            mode: KeyRouteAffinityType::Rmw,
            operation: MatrixOperation::Scan,
            expected_routing: ExpectedRouting::RoundRobin,
        },
        OperationMatrixCase {
            mode: KeyRouteAffinityType::Rmw,
            operation: MatrixOperation::ListTables,
            expected_routing: ExpectedRouting::RoundRobin,
        },
        OperationMatrixCase {
            mode: KeyRouteAffinityType::AnyWrite,
            operation: MatrixOperation::PutPlain,
            expected_routing: ExpectedRouting::Affinity,
        },
        OperationMatrixCase {
            mode: KeyRouteAffinityType::AnyWrite,
            operation: MatrixOperation::DeletePlain,
            expected_routing: ExpectedRouting::Affinity,
        },
        OperationMatrixCase {
            mode: KeyRouteAffinityType::AnyWrite,
            operation: MatrixOperation::UpdateLegacyPut,
            expected_routing: ExpectedRouting::Affinity,
        },
        OperationMatrixCase {
            mode: KeyRouteAffinityType::AnyWrite,
            operation: MatrixOperation::UpdateLegacyAdd,
            expected_routing: ExpectedRouting::Affinity,
        },
        OperationMatrixCase {
            mode: KeyRouteAffinityType::AnyWrite,
            operation: MatrixOperation::UpdateLegacyDeleteWithValue,
            expected_routing: ExpectedRouting::Affinity,
        },
        OperationMatrixCase {
            mode: KeyRouteAffinityType::AnyWrite,
            operation: MatrixOperation::UpdateLegacyPutReturnUpdatedNew,
            expected_routing: ExpectedRouting::Affinity,
        },
        OperationMatrixCase {
            mode: KeyRouteAffinityType::AnyWrite,
            operation: MatrixOperation::UpdateExpression,
            expected_routing: ExpectedRouting::Affinity,
        },
        OperationMatrixCase {
            mode: KeyRouteAffinityType::AnyWrite,
            operation: MatrixOperation::BatchWritePut,
            expected_routing: ExpectedRouting::Affinity,
        },
        OperationMatrixCase {
            mode: KeyRouteAffinityType::AnyWrite,
            operation: MatrixOperation::BatchWriteDelete,
            expected_routing: ExpectedRouting::Affinity,
        },
        OperationMatrixCase {
            mode: KeyRouteAffinityType::AnyWrite,
            operation: MatrixOperation::GetItem,
            expected_routing: ExpectedRouting::RoundRobin,
        },
        OperationMatrixCase {
            mode: KeyRouteAffinityType::AnyWrite,
            operation: MatrixOperation::BatchGetItem,
            expected_routing: ExpectedRouting::RoundRobin,
        },
        OperationMatrixCase {
            mode: KeyRouteAffinityType::AnyWrite,
            operation: MatrixOperation::Query,
            expected_routing: ExpectedRouting::RoundRobin,
        },
        OperationMatrixCase {
            mode: KeyRouteAffinityType::AnyWrite,
            operation: MatrixOperation::Scan,
            expected_routing: ExpectedRouting::RoundRobin,
        },
        OperationMatrixCase {
            mode: KeyRouteAffinityType::AnyWrite,
            operation: MatrixOperation::ListTables,
            expected_routing: ExpectedRouting::RoundRobin,
        },
    ];

    cases.extend(
        MatrixOperation::ALL
            .into_iter()
            .map(|operation| OperationMatrixCase {
                mode: KeyRouteAffinityType::None,
                operation,
                expected_routing: ExpectedRouting::RoundRobin,
            }),
    );

    request_counter.reset();

    for case in cases {
        let client = match case.mode {
            KeyRouteAffinityType::None => &none_client,
            KeyRouteAffinityType::Rmw => &rmw_client,
            KeyRouteAffinityType::AnyWrite => &any_write_client,
        };
        let case_label = format!("{}_{}", mode_name(case.mode), case.operation.name());
        let key = format!("matrix_key_{case_label}");

        request_counter.reset();

        for iteration in 0..node_ips.len() {
            send_matrix_operation(client, &table_name, case.operation, &key, iteration).await;
        }

        match case.expected_routing {
            ExpectedRouting::Affinity => {
                let expected_ip = expected_first_node(node_ips.as_slice(), &key);
                assert_affinity_counts(
                    &request_counter,
                    node_ips.as_slice(),
                    expected_ip,
                    node_ips.len(),
                    &case_label,
                );
            }
            ExpectedRouting::RoundRobin => {
                assert_round_robin_counts(&request_counter, node_ips.as_slice(), &case_label);
            }
        }
    }
}

/// Test that multi-table BatchWriteItem affinity is deterministic for repeated
/// requests constructed the same way, including when table entries are inserted
/// in different orders.
#[tokio::test]
#[cfg_attr(not(ccm_tests), ignore)]
async fn batch_write_affinity_multitable_routing_is_deterministic_test() {
    let mut guard = get_cluster().await;
    let cluster = &mut *guard;

    let scope = scope_utils::datacenter_scope_from_index(cluster, 1);
    let request_counter = RequestCounter::from_cluster(cluster);

    start_proxies(cluster, PROXY_PORT, &request_counter).await;

    let test_id = uuid::Uuid::new_v4();
    let a_table_name = format!("a_batch_test_{test_id}");
    let z_table_name = format!("z_batch_test_{test_id}");
    let client = create_client_with_scope_and_affinity(
        cluster,
        scope.clone(),
        KeyRouteAffinityConfig::builder()
            .with_type(KeyRouteAffinityType::AnyWrite)
            .with_pk_info(&a_table_name, "id")
            .with_pk_info(&z_table_name, "id")
            .build(),
    );
    let node_ips = scope_utils::working_nodes_ips_in_scope(cluster, &scope);

    wait_until_live_nodes_match(&client, node_ips.clone()).await;
    create_table(&client, &a_table_name).await;
    create_table(&client, &z_table_name).await;

    let (a_key_1, a_key_2, z_key, expected_ip) =
        find_two_keys_on_one_node_and_one_on_another(node_ips.as_slice(), "batch_multi_key");

    for z_table_first in [true, false] {
        request_counter.reset();

        for iteration in 0..node_ips.len() {
            let a_put_1 = PutRequest::builder()
                .item("id", s(&a_key_1))
                .item("val", s(format!("a_value_1_{z_table_first}_{iteration}")))
                .build()
                .unwrap();
            let a_put_2 = PutRequest::builder()
                .item("id", s(&a_key_2))
                .item("val", s(format!("a_value_2_{z_table_first}_{iteration}")))
                .build()
                .unwrap();
            let z_put = PutRequest::builder()
                .item("id", s(&z_key))
                .item("val", s(format!("z_value_{z_table_first}_{iteration}")))
                .build()
                .unwrap();
            let a_write_1 = WriteRequest::builder().put_request(a_put_1).build();
            let a_write_2 = WriteRequest::builder().put_request(a_put_2).build();
            let z_write = WriteRequest::builder().put_request(z_put).build();

            let builder = client.batch_write_item();
            let builder = if z_table_first {
                builder
                    .request_items(z_table_name.as_str(), vec![z_write])
                    .request_items(a_table_name.as_str(), vec![a_write_1, a_write_2])
            } else {
                builder
                    .request_items(a_table_name.as_str(), vec![a_write_2, a_write_1])
                    .request_items(z_table_name.as_str(), vec![z_write])
            };

            builder.send().await.unwrap();
        }

        let case_label = format!("batch_write_multitable_z_first_{z_table_first}");
        assert_affinity_counts(
            &request_counter,
            node_ips.as_slice(),
            &expected_ip,
            node_ips.len(),
            &case_label,
        );
    }
}

/// Test if the routing is deterministic, and all calls go to the same node.
#[tokio::test]
#[cfg_attr(not(ccm_tests), ignore)]
async fn affinity_deterministic_routing_test() {
    let mut guard = get_cluster().await;
    let cluster = &mut *guard;

    let scope = scope_utils::datacenter_scope_from_index(cluster, 1);
    let request_counter = RequestCounter::from_cluster(cluster);

    start_proxies(cluster, PROXY_PORT, &request_counter).await;
    let table_name = format!("test_table_{}", uuid::Uuid::new_v4());
    let affinity_config = KeyRouteAffinityConfig::builder()
        .with_type(KeyRouteAffinityType::AnyWrite)
        .with_pk_info(&table_name, "id")
        .build();
    let client = create_client_with_scope_and_affinity(cluster, scope.clone(), affinity_config);
    wait_until_live_nodes_match(
        &client,
        scope_utils::working_nodes_ips_in_scope(cluster, &scope),
    )
    .await;
    create_table(&client, &table_name).await;

    for i in 0..5 {
        request_counter.reset();

        let item = format!("key_{}", i + 1);
        put_item(&client, &table_name, &item).await;
        for _ in 0..=5 {
            update_item(&client, &table_name, &item, &format!("new_{}", item)).await;
        }
        delete_item(&client, &table_name, &item).await;

        let ips_in_scope = scope_utils::ips_in_scope(cluster, &scope);

        let called_node_ip = ips_in_scope
            .iter()
            .find(|ip| request_counter.get(ip).posts() > 0)
            .unwrap();

        assert_eq!(request_counter.get_posts_to_other_ips(&[called_node_ip]), 0);

        let expected_ip = expected_first_node(ips_in_scope.as_slice(), &item);
        assert_eq!(*called_node_ip, expected_ip);
    }
}
