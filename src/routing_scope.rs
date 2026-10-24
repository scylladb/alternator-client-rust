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

//! Routing scope for directing requests to specific subsets of nodes in a cluster.
//!
//! Routing scopes allow user to specify which nodes should be used for load balancing,
//! with optional fallback to a wider scope if no nodes are available in the preferred one.

/// Selects the preferred cluster nodes and an optional fallback chain for routing.
///
/// A scope can target the whole cluster, one datacenter, or one rack within a
/// datacenter. If no live nodes exist in that scope, the client tries each
/// scope added with [`Self::with_fallback`] in order.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RoutingScope {
    dc: Option<String>,
    rack: Option<String>,
    fallback: Option<Box<RoutingScope>>,
}

/// Identifies a discovery query together with its seed aggregation policy.
/// Scoped queries use the first usable response; cluster queries aggregate.
/// Both affinity-chain deduplication and per-refresh reuse use this identity.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) enum DiscoveryOperation {
    Cluster,
    Datacenter(String),
    Rack { dc: String, rack: String },
}

impl DiscoveryOperation {
    fn build_localnodes_url(&self, mut base_url: url::Url) -> url::Url {
        base_url.set_path("/localnodes");
        match self {
            Self::Cluster => {}
            Self::Datacenter(dc) => {
                base_url.query_pairs_mut().append_pair("dc", dc);
            }
            Self::Rack { dc, rack } => {
                base_url
                    .query_pairs_mut()
                    .append_pair("dc", dc)
                    .append_pair("rack", rack);
            }
        }
        base_url
    }
}

impl RoutingScope {
    /// Routes across the whole cluster.
    ///
    /// The client queries `/localnodes` on configured seed hosts and
    /// already-known live nodes, then unions the returned node lists.
    /// Provide at least one working seed host from every datacenter that should
    /// receive traffic.
    pub fn from_cluster() -> Self {
        Self {
            dc: None,
            rack: None,
            fallback: None,
        }
    }

    /// Routes requests to nodes in `dc`.
    ///
    /// An empty datacenter name is treated as [`Self::from_cluster`].
    pub fn from_datacenter(dc: String) -> Self {
        if dc.is_empty() {
            Self::from_cluster()
        } else {
            Self {
                dc: Some(dc),
                ..Self::from_cluster()
            }
        }
    }

    /// Routes requests to nodes in `rack` within `dc`.
    ///
    /// Eligible key-affinity requests use all racks in the selected datacenter.
    /// Rack restrictions in configured fallbacks widen to their datacenters.
    /// Equivalent widened scopes collapse to their first occurrence, preserving
    /// fallback order. Other requests retain the configured rack scope.
    ///
    /// An empty datacenter name is treated as [`Self::from_cluster`]. An empty
    /// rack name is treated as [`Self::from_datacenter`].
    pub fn from_rack(dc: String, rack: String) -> Self {
        if dc.is_empty() {
            Self::from_cluster()
        } else if rack.is_empty() {
            Self::from_datacenter(dc)
        } else {
            Self {
                dc: Some(dc),
                rack: Some(rack),
                ..Self::from_cluster()
            }
        }
    }

    /// Sets a fallback for the routing scope that is used if no nodes are available in the preferred scope.
    ///
    /// This function can be called multiple times to create a chain of fallback scopes.
    /// Each call of this function adds the new fallback scope at the end of the existing fallback chain.
    /// Requests are always routed to the most preferred scope in the chain that has available nodes.
    ///
    /// Keep in mind that subsequent fallback scope should ideally be broader than or equal to the
    /// previous one, e.g., (rack -> datacenter -> cluster) or (rack -> another rack -> datacenter -> cluster).
    /// Making a fallback narrower, e.g., (datacenter -> rack) or (cluster -> datacenter),
    /// may be redundant if the set of nodes in the next scope is a subset of the previous one.
    pub fn with_fallback(mut self, new_fallback: RoutingScope) -> Self {
        let mut tail = &mut self.fallback;
        while let Some(boxed) = tail {
            tail = &mut boxed.fallback;
        }
        *tail = Some(Box::new(new_fallback));
        self
    }

    /// Appends the datacenter and rack parameters to the given URL as query parameters, if they are set in the scope.
    /// append_pair performs URL encoding.
    pub(crate) fn build_localnodes_url(&self, base_url: url::Url) -> url::Url {
        self.discovery_operation().build_localnodes_url(base_url)
    }

    pub(crate) fn is_cluster(&self) -> bool {
        self.dc.is_none() && self.rack.is_none()
    }

    pub(crate) fn discovery_operation(&self) -> DiscoveryOperation {
        match (&self.dc, &self.rack) {
            (Some(dc), Some(rack)) => DiscoveryOperation::Rack {
                dc: dc.clone(),
                rack: rack.clone(),
            },
            (Some(dc), None) => DiscoveryOperation::Datacenter(dc.clone()),
            (None, _) => DiscoveryOperation::Cluster,
        }
    }

    /// Returns whether this scope or any fallback restricts routing to a rack.
    pub(crate) fn has_rack_restriction(&self) -> bool {
        std::iter::successors(Some(self), |scope| scope.fallback())
            .any(|scope| scope.rack.is_some())
    }

    /// Returns the same scope and fallback chain without rack restrictions.
    ///
    /// Key-route affinity needs every rack in each selected datacenter so
    /// clients in different racks derive the same coordinator for a partition.
    /// Datacenter locality is unchanged. Equivalent widened scopes collapse to
    /// their first occurrence, preserving the remaining fallback order.
    pub(crate) fn without_rack(&self) -> Self {
        let mut seen = std::collections::HashSet::new();
        let mut resolved = None;
        let mut tail = &mut resolved;
        for current in std::iter::successors(Some(self), |scope| scope.fallback()) {
            let widened = Self {
                dc: current.dc.clone(),
                rack: None,
                fallback: None,
            };
            if seen.insert(widened.discovery_operation()) {
                tail = &mut tail.insert(Box::new(widened)).fallback;
            }
        }
        *resolved.expect("a routing chain contains its first scope")
    }

    /// Returns the next scope in the fallback chain, if one is configured.
    pub fn fallback(&self) -> Option<&RoutingScope> {
        self.fallback.as_deref()
    }

    /// Returns the targeted datacenter, or [`None`] for cluster-wide routing.
    pub fn dc(&self) -> Option<&str> {
        self.dc.as_deref()
    }

    /// Returns the targeted rack, or [`None`] when routing is not rack-specific.
    pub fn rack(&self) -> Option<&str> {
        self.rack.as_deref()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cluster_scope() {
        let scope = RoutingScope::from_cluster();
        assert_eq!(scope.dc(), None);
        assert_eq!(scope.rack(), None);
        assert!(scope.fallback().is_none());
        let url = url::Url::parse("http://localhost/").unwrap();
        assert_eq!(
            scope.build_localnodes_url(url).as_str(),
            "http://localhost/localnodes"
        );
    }

    #[test]
    fn without_rack_keeps_first_equivalent_operation_and_explicit_order() {
        let scope = RoutingScope::from_rack("A".into(), "r1".into())
            .with_fallback(RoutingScope::from_rack("A".into(), "r1".into()))
            .with_fallback(RoutingScope::from_datacenter("A".into()))
            .with_fallback(RoutingScope::from_rack("B".into(), "r2".into()))
            .with_fallback(RoutingScope::from_cluster())
            .with_fallback(RoutingScope::from_datacenter("B".into()));
        let widened = scope.without_rack();
        let operations = std::iter::successors(Some(&widened), |scope| scope.fallback())
            .map(|scope| (scope.dc(), scope.rack(), scope.is_cluster()))
            .collect::<Vec<_>>();
        assert_eq!(
            operations,
            [
                (Some("A"), None, false),
                (Some("B"), None, false),
                (None, None, true)
            ]
        );
        assert_eq!(scope.rack(), Some("r1"));
        assert_eq!(
            std::iter::successors(Some(&scope), |scope| scope.fallback()).count(),
            6
        );
    }

    #[test]
    fn without_rack_widens_every_scope_and_preserves_datacenters() {
        let scope = RoutingScope::from_rack("dc1".to_string(), "rack1".to_string())
            .with_fallback(RoutingScope::from_rack(
                "dc2".to_string(),
                "rack2".to_string(),
            ))
            .with_fallback(RoutingScope::from_cluster());

        assert!(scope.has_rack_restriction());

        let widened = scope.without_rack();

        assert_eq!(widened.dc(), Some("dc1"));
        assert_eq!(widened.rack(), None);
        let dc2 = widened
            .fallback()
            .expect("datacenter fallback is preserved");
        assert_eq!(dc2.dc(), Some("dc2"));
        assert_eq!(dc2.rack(), None);
        let cluster = dc2.fallback().expect("cluster fallback is preserved");
        assert_eq!(cluster.dc(), None);
        assert_eq!(cluster.rack(), None);
        assert!(!widened.has_rack_restriction());
    }

    #[test]
    fn rack_restriction_is_detected_in_a_fallback() {
        let scope = RoutingScope::from_datacenter("dc1".to_string()).with_fallback(
            RoutingScope::from_rack("dc2".to_string(), "rack2".to_string()),
        );

        assert!(scope.has_rack_restriction());
        assert!(!scope.without_rack().has_rack_restriction());
    }

    #[test]
    fn test_datacenter_scope() {
        let scope = RoutingScope::from_datacenter("dc1".to_string());
        assert_eq!(scope.dc(), Some("dc1"));
        assert_eq!(scope.rack(), None);
        assert!(scope.fallback().is_none());
        let url = url::Url::parse("http://localhost/").unwrap();
        assert_eq!(
            scope.build_localnodes_url(url).as_str(),
            "http://localhost/localnodes?dc=dc1"
        );
    }

    #[test]
    fn test_rack_scope() {
        let scope = RoutingScope::from_rack("dc1".to_string(), "rack1".to_string());
        assert_eq!(scope.dc(), Some("dc1"));
        assert_eq!(scope.rack(), Some("rack1"));
        assert!(scope.fallback().is_none());
        let url = url::Url::parse("http://localhost/").unwrap();
        assert_eq!(
            scope.build_localnodes_url(url).as_str(),
            "http://localhost/localnodes?dc=dc1&rack=rack1"
        );
    }

    #[test]
    fn test_with_fallback() {
        let scope = RoutingScope::from_rack("dc1".to_string(), "rack1".to_string())
            .with_fallback(RoutingScope::from_datacenter("dc1".to_string()))
            .with_fallback(RoutingScope::from_cluster());

        assert_eq!(scope.dc(), Some("dc1"));
        assert_eq!(scope.rack(), Some("rack1"));

        let first_fallback = scope.fallback().expect("Should have a fallback");
        assert_eq!(first_fallback.dc(), Some("dc1"));
        assert_eq!(first_fallback.rack(), None);

        let second_fallback = first_fallback
            .fallback()
            .expect("Should have a second fallback");
        assert_eq!(second_fallback.dc(), None);
        assert_eq!(second_fallback.rack(), None);
        assert!(second_fallback.fallback().is_none());
    }

    #[test]
    fn test_localnodes_query_encoding() {
        let scope = RoutingScope::from_rack("dc 1".to_string(), "rack&1".to_string());
        let url = url::Url::parse("http://localhost/").unwrap();
        assert_eq!(
            scope.build_localnodes_url(url).as_str(),
            "http://localhost/localnodes?dc=dc+1&rack=rack%261"
        );
    }

    #[test]
    fn test_impossible_to_create_cyclic_fallback() {
        // Because `RoutingScope` uses `Box` which has exclusive ownership, it is impossible to create a self-referential cycle.
        // Even if a user attempts to create a "cycle" by cloning the scope and passing it
        // to itself, it creates a finite, linear chain of completely separate allocations.

        let base = RoutingScope::from_rack("dc1".to_string(), "rack1".to_string());

        let scope = base.clone().with_fallback(base);

        // We can prove it's not a cycle because by showing there is None at the end of the fallback chain.
        assert!(scope.fallback().unwrap().fallback().is_none());
    }

    #[test]
    fn test_fallback_associativity() {
        let rs1 = RoutingScope::from_rack("dc1".to_string(), "rack1".to_string());
        let rs2 = RoutingScope::from_datacenter("dc1".to_string());
        let rs3 = RoutingScope::from_rack("dc2".to_string(), "rack2".to_string());
        let rs4 = RoutingScope::from_datacenter("dc2".to_string());
        let rs5 = RoutingScope::from_cluster();

        // Chain 1: rs1.with_fallback(rs2.with_fallback(rs3)).with_fallback(rs4.with_fallback(rs5))
        let chain1 = rs1
            .clone()
            .with_fallback(rs2.clone().with_fallback(rs3.clone()))
            .with_fallback(rs4.clone().with_fallback(rs5.clone()));

        // Chain 2: rs1.with_fallback(rs2).with_fallback(rs3).with_fallback(rs4).with_fallback(rs5)
        let chain2 = rs1
            .clone()
            .with_fallback(rs2.clone())
            .with_fallback(rs3.clone())
            .with_fallback(rs4.clone())
            .with_fallback(rs5.clone());

        // Chain 3: rs1.with_fallback(rs2.with_fallback(rs3.with_fallback(rs4.with_fallback(rs5))))
        let chain3 = rs1.clone().with_fallback(
            rs2.clone().with_fallback(
                rs3.clone()
                    .with_fallback(rs4.clone().with_fallback(rs5.clone())),
            ),
        );

        // Chain 4: rs1.with_fallback(rs2).with_fallback(rs3.with_fallback(rs4)).with_fallback(rs5)
        let chain4 = rs1
            .clone()
            .with_fallback(
                rs2.clone()
                    .with_fallback(rs3.clone().with_fallback(rs4.clone())),
            )
            .with_fallback(rs5.clone());

        assert_eq!(chain1, chain2);
        assert_eq!(chain2, chain3);
        assert_eq!(chain3, chain4);
    }
}
