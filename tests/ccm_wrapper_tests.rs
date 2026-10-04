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

mod ccm_wrapper;

use crate::ccm_wrapper::ccm::*;
use crate::ccm_wrapper::cluster::*;
use crate::ccm_wrapper::topology_spec::*;
use serde_json::{Value, json};

// GET request on node with alternator can only return "healthy".
// Therefore if it did not refuse the connection - it is up.
async fn is_node_up(node: &Node) -> Result<bool, reqwest::Error> {
    Ok(reqwest::get(node.address()).await.is_ok())
}

#[derive(Debug, Eq, Ord, PartialEq, PartialOrd)]
struct TopologyNode {
    rpc_address: String,
    data_center: String,
    rack: String,
}

fn topology_attribute(item: &Value, attribute: &str) -> Option<String> {
    item.get(attribute)
        .and_then(|value| value.get("S"))
        .and_then(Value::as_str)
        .map(str::to_owned)
}

async fn scan_topology_table(
    client: &reqwest::Client,
    endpoint: &str,
    table_name: &str,
) -> Result<Vec<TopologyNode>, Box<dyn std::error::Error>> {
    let mut nodes = Vec::new();
    let mut exclusive_start_key = None;

    loop {
        let mut request = json!({
            "TableName": table_name,
            "ProjectionExpression": "rpc_address,data_center,rack",
        });
        if let Some(key) = exclusive_start_key {
            request["ExclusiveStartKey"] = key;
        }

        let response = client
            .post(endpoint)
            .header("X-Amz-Target", "DynamoDB_20120810.Scan")
            .header("Content-Type", "application/x-amz-json-1.0")
            .json(&request)
            .send()
            .await?
            .error_for_status()?
            .json::<Value>()
            .await?;

        let items = response
            .get("Items")
            .and_then(Value::as_array)
            .ok_or_else(|| {
                std::io::Error::other(format!(
                    "Scan of {table_name} returned no Items array: {response}"
                ))
            })?;
        nodes.extend(items.iter().filter_map(|item| {
            Some(TopologyNode {
                rpc_address: topology_attribute(item, "rpc_address")?,
                data_center: topology_attribute(item, "data_center")?,
                rack: topology_attribute(item, "rack")?,
            })
        }));

        exclusive_start_key = response
            .get("LastEvaluatedKey")
            .and_then(Value::as_object)
            .filter(|key| !key.is_empty())
            .cloned()
            .map(Value::Object);
        if exclusive_start_key.is_none() {
            return Ok(nodes);
        }
    }
}

// Test to verify if cluster matches the given topology.
fn verify_correctness_with_topology(
    cluster: &Cluster,
    topology_spec: &TopologySpec,
) -> Result<(), String> {
    if cluster.datacenters().len() != topology_spec.datacenters.len() {
        return Err(format!(
            "Datacenter count mismatch. cluster: {}, topology: {}",
            cluster.datacenters().len(),
            topology_spec.datacenters.len()
        ));
    }

    for (datacenter_idx, datacenter) in cluster.datacenters().iter().enumerate() {
        if datacenter.racks().len() != topology_spec.datacenters[datacenter_idx].racks.len() {
            return Err(format!(
                "Rack count mismatch in {}. cluster: {}, topology: {}",
                datacenter.name,
                datacenter.racks().len(),
                topology_spec.datacenters[datacenter_idx].racks.len()
            ));
        }
        for (rack_idx, rack) in datacenter.racks().iter().enumerate() {
            if rack.nodes().len() != topology_spec.datacenters[datacenter_idx].racks[rack_idx] {
                return Err(format!(
                    "Node count mismatch in {}/{}. cluster: {}, topology: {}",
                    datacenter.name,
                    rack.name,
                    rack.nodes().len(),
                    topology_spec.datacenters[datacenter_idx].racks[rack_idx]
                ));
            }
        }
    }
    Ok(())
}

// Test that the topology exposed through Alternator system tables matches the cluster struct.
async fn verify_correctness_with_system_tables(
    cluster: &Cluster,
) -> Result<(), Box<dyn std::error::Error>> {
    let endpoint = cluster
        .nodes()
        .into_iter()
        .find(|node| node.is_up)
        .ok_or_else(|| std::io::Error::other("cluster has no running node"))?
        .address();
    let client = reqwest::Client::new();

    let mut actual =
        scan_topology_table(&client, &endpoint, ".scylla.alternator.system.local").await?;
    actual
        .extend(scan_topology_table(&client, &endpoint, ".scylla.alternator.system.peers").await?);

    let mut expected = cluster
        .datacenters()
        .iter()
        .flat_map(|datacenter| {
            datacenter.racks().iter().flat_map(|rack| {
                rack.nodes().iter().map(|node| TopologyNode {
                    rpc_address: node.ip.clone(),
                    data_center: datacenter.name.clone(),
                    rack: rack.name.clone(),
                })
            })
        })
        .collect::<Vec<_>>();

    actual.sort();
    expected.sort();
    if actual != expected {
        return Err(format!(
            "system-table topology mismatch.\n Cluster structure: {expected:?},\n system.local + system.peers: {actual:?}"
        )
        .into());
    }

    Ok(())
}

// Check if the actual state of nodes corresponds to their node.is_up value.
async fn check_if_correct_nodes_are_up(
    cluster: &Cluster,
) -> Result<(), Box<dyn std::error::Error>> {
    for node in cluster.nodes().iter() {
        let is_really_up = is_node_up(node).await?;

        if node.is_up != is_really_up {
            return Err(format!(
                "{} inconsistency found, node.is_up is {}, but should be {}",
                node.name, node.is_up, is_really_up
            )
            .into());
        }
    }
    Ok(())
}

#[tokio::test]
// Tests that are using ccm are marked with this attribute.
// They are ignored by default, and only are run when the ccm_tests flag is set:
// RUSTFLAGS='--cfg ccm_tests' cargo test
// It allows running simpler tests, ones that do not need a special cluster setup to be run without involving ccm.
#[cfg_attr(not(ccm_tests), ignore)]
async fn ccm_wrapper_test_cluster() -> Result<(), Box<dyn std::error::Error>> {
    // The driver's reqwest dependency intentionally leaves rustls provider
    // selection to each client. These standalone HTTP helpers use reqwest's
    // default client, so install the same AWS-LC provider first.
    let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();

    let topology = TopologySpecBuilder::new()
        .datacenter(DatacenterSpec::new().rack(1))
        .datacenter(DatacenterSpec::new().rack(1).rack(2))
        .build()?;

    let ip_prefix = IpPrefix::new("127.0.1.")?;
    let cluster_name = uuid::Uuid::new_v4().to_string();
    let scylla_version =
        std::env::var("CCM_SCYLLA_VERSION").unwrap_or_else(|_| String::from("release:2025.1.16"));

    let mut cluster = ClusterGuard(Ccm::create_cluster(
        cluster_name,
        &topology,
        ip_prefix,
        8000,
        scylla_version,
    )?);

    verify_correctness_with_topology(&cluster, &topology)?;
    check_if_correct_nodes_are_up(&cluster).await?;

    Ccm::start_cluster(&mut cluster)?;

    verify_correctness_with_system_tables(&cluster).await?;
    check_if_correct_nodes_are_up(&cluster).await?;

    let node1_1_1 = cluster.node_mut(0, 0, 0).unwrap();
    Ccm::stop_node(node1_1_1)?;

    let node2_2_1 = cluster.node_mut(1, 1, 0).unwrap();
    Ccm::stop_node(node2_2_1)?;

    check_if_correct_nodes_are_up(&cluster).await?;

    let node1_1_1 = cluster.node_mut(0, 0, 0).unwrap();
    Ccm::start_node(node1_1_1)?;

    check_if_correct_nodes_are_up(&cluster).await?;

    let node2_2_1 = cluster.node_mut(1, 1, 0).unwrap();
    Ccm::start_node(node2_2_1)?;

    check_if_correct_nodes_are_up(&cluster).await?;

    Ccm::stop_cluster(&mut cluster)?;

    check_if_correct_nodes_are_up(&cluster).await?;

    Ok(())
}

#[test]
#[cfg_attr(not(ccm_tests), ignore)]
fn ccm_wrapper_test_invalid_topology() {
    // Empty cluster.
    let result = TopologySpecBuilder::new().build();
    assert!(result.is_err());

    // Empty datacenter.
    let result = TopologySpecBuilder::new()
        .datacenter(DatacenterSpec::new())
        .datacenter(DatacenterSpec::new().rack(1).rack(2))
        .build();
    assert!(result.is_err());

    // Empty rack
    let result = TopologySpecBuilder::new()
        .datacenter(DatacenterSpec::new().rack(1))
        .datacenter(DatacenterSpec::new().rack(1).rack(0))
        .build();
    assert!(result.is_err());

    // Too many nodes.
    let result = TopologySpecBuilder::new()
        .datacenter(DatacenterSpec::new().rack(20000))
        .datacenter(DatacenterSpec::new().rack(1).rack(2))
        .build();
    assert!(result.is_err());
}
