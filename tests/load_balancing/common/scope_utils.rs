//! Utility functions for working with RoutingScope in tests.

use alternator_driver::RoutingScope;

use crate::ccm_wrapper::ccm::Ccm;
use crate::ccm_wrapper::cluster::{Cluster, Node};

pub(crate) fn nodes_in_scope<'a>(cluster: &'a Cluster, scope: &RoutingScope) -> Vec<&'a Node> {
    match (scope.dc(), scope.rack()) {
        (None, None) => cluster.nodes(),
        (Some(dc), None) => cluster
            .datacenters()
            .iter()
            .find(|datacenter| datacenter.name == dc)
            .map_or_else(Vec::new, |datacenter| {
                datacenter
                    .racks()
                    .iter()
                    .flat_map(|rack| rack.nodes().iter())
                    .collect()
            }),
        (Some(dc), Some(rack)) => cluster
            .datacenters()
            .iter()
            .find(|datacenter| datacenter.name == dc)
            .and_then(|datacenter| {
                datacenter
                    .racks()
                    .iter()
                    .find(|rack_struct| rack_struct.name == rack)
            })
            .map_or_else(Vec::new, |rack_struct| rack_struct.nodes().iter().collect()),
        (None, Some(_)) => unreachable!("rack without dc — invariant violated"),
    }
}

pub(crate) fn nodes_in_scope_mut<'a>(
    cluster: &'a mut Cluster,
    scope: &RoutingScope,
) -> Vec<&'a mut Node> {
    match (scope.dc(), scope.rack()) {
        (None, None) => cluster.nodes_mut(),
        (Some(dc), None) => cluster
            .datacenters_mut()
            .iter_mut()
            .find(|datacenter| datacenter.name == dc)
            .map_or_else(Vec::new, |datacenter| {
                datacenter
                    .racks_mut()
                    .iter_mut()
                    .flat_map(|rack| rack.nodes_mut().iter_mut())
                    .collect()
            }),
        (Some(dc), Some(rack)) => cluster
            .datacenters_mut()
            .iter_mut()
            .find(|datacenter| datacenter.name == dc)
            .and_then(|datacenter| {
                datacenter
                    .racks_mut()
                    .iter_mut()
                    .find(|rack_struct| rack_struct.name == rack)
            })
            .map_or_else(Vec::new, |rack_struct| {
                rack_struct.nodes_mut().iter_mut().collect()
            }),
        (None, Some(_)) => unreachable!("rack without dc — invariant violated"),
    }
}

pub(crate) fn ips_in_scope<'a>(cluster: &'a Cluster, scope: &RoutingScope) -> Vec<&'a str> {
    nodes_in_scope(cluster, scope)
        .iter()
        .map(|node| node.ip.as_str())
        .collect()
}

pub(crate) fn working_nodes_ips_in_scope<'a>(
    cluster: &'a Cluster,
    scope: &RoutingScope,
) -> Vec<&'a str> {
    nodes_in_scope(cluster, scope)
        .iter()
        .filter(|node| node.is_up)
        .map(|node| node.ip.as_str())
        .collect()
}

pub(crate) fn scope_first_working_node_mut<'a>(
    cluster: &'a mut Cluster,
    scope: &RoutingScope,
) -> Option<&'a mut Node> {
    nodes_in_scope_mut(cluster, scope)
        .into_iter()
        .find(|node| node.is_up)
}

pub(crate) fn shut_down_scope(cluster: &mut Cluster, scope: &RoutingScope) {
    for node in nodes_in_scope_mut(cluster, scope) {
        Ccm::stop_node(node).unwrap();
    }
}

pub(crate) fn datacenter_scope_from_index(cluster: &Cluster, index: usize) -> RoutingScope {
    RoutingScope::from_datacenter(cluster.datacenters()[index].name.to_string())
}

pub(crate) fn rack_scope_from_index(
    cluster: &Cluster,
    datacenter_index: usize,
    rack_index: usize,
) -> RoutingScope {
    RoutingScope::from_rack(
        cluster.datacenters()[datacenter_index].name.to_string(),
        cluster.datacenters()[datacenter_index].racks()[rack_index]
            .name
            .to_string(),
    )
}
