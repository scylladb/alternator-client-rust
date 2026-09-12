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

// GET request on node with alternator can only return "healthy".
// Therefore if it did not refuse the connection - it is up.
async fn is_node_up(node: &Node) -> Result<bool, reqwest::Error> {
    Ok(reqwest::get(node.address()).await.is_ok())
}

async fn get_localnodes(url: &str) -> Result<Vec<String>, reqwest::Error> {
    let response = reqwest::get(url).await?;
    let nodes = response.json::<Vec<String>>().await?;
    Ok(nodes)
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

// Test to see if real cluster matches the cluster struct, using localnodes API.
async fn verify_correctness_with_localnodes(
    cluster: &Cluster,
) -> Result<(), Box<dyn std::error::Error>> {
    for datacenter in cluster.datacenters().iter() {
        let dc_localnodes_url = format!(
            "{}/localnodes?dc={}",
            // Take the first node address for the localnodes url.
            datacenter.racks()[0].nodes()[0].address(),
            datacenter.name
        );

        let localnodes: Vec<String> = get_localnodes(&dc_localnodes_url).await?;

        let mut localnodes: Vec<&str> = localnodes.iter().map(|s| s.as_str()).collect();
        let mut dc_node_ips: Vec<&str> = datacenter.node_ips();

        dc_node_ips.sort();
        localnodes.sort();

        if dc_node_ips != localnodes {
            return Err(format!(
                "mismatch in {}.\n Nodes in the cluster structure: {:?},\n list returned by localnodes: {:?}",
                datacenter.name, dc_node_ips, localnodes
            ).into());
        }

        for rack in datacenter.racks().iter() {
            let rack_localnodes_url = format!(
                "{}/localnodes?dc={}&rack={}",
                rack.nodes()[0].address(),
                datacenter.name,
                rack.name
            );

            let localnodes: Vec<String> = get_localnodes(&rack_localnodes_url).await?;

            let mut localnodes: Vec<&str> = localnodes.iter().map(|s| s.as_str()).collect();
            let mut rack_node_ips: Vec<&str> = rack.node_ips();

            rack_node_ips.sort();
            localnodes.sort();

            if rack_node_ips != localnodes {
                return Err(format!(
                    "mismatch in {}/{}.\n Nodes in the cluster structure: {:?},\n list returned by localnodes: {:?}",
                    datacenter.name, rack.name, rack_node_ips, localnodes
                ).into());
            }
        }
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
    let scylla_version = String::from("release:2025.1");

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

    verify_correctness_with_localnodes(&cluster).await?;
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
