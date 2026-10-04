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
/// datacenter. If no discovered nodes exist in that scope, the client tries each
/// scope added with [`Self::with_fallback`] in order.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RoutingScope {
    dc: Option<String>,
    rack: Option<String>,
    fallback: Option<Box<RoutingScope>>,
}

impl RoutingScope {
    /// Routes across the whole cluster.
    ///
    /// The client reads ScyllaDB's `system.local` and `system.peers` tables
    /// through the DynamoDB API and routes across every discovered datacenter.
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
    /// When key-route affinity is enabled, requests with an affinity plan use
    /// every discovered node in `dc` so clients in different racks derive the same
    /// coordinator. Other requests remain restricted to `rack`.
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
    /// Requests are routed to the most preferred scope containing discovered nodes.
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

    /// Returns whether a topology row belongs to this scope.
    pub(crate) fn contains(&self, dc: &str, rack: &str) -> bool {
        self.dc.as_deref().is_none_or(|expected| expected == dc)
            && self.rack.as_deref().is_none_or(|expected| expected == rack)
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
    /// Datacenter locality and fallback order remain unchanged.
    pub(crate) fn without_rack(&self) -> Self {
        let mut scope = self.clone();
        let mut current = &mut scope;
        loop {
            current.rack = None;
            let Some(fallback) = current.fallback.as_deref_mut() else {
                break;
            };
            current = fallback;
        }
        scope
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
        assert!(scope.contains("dc1", "rack1"));
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
        assert!(scope.contains("dc1", "rack1"));
        assert!(!scope.contains("dc2", "rack1"));
    }

    #[test]
    fn test_rack_scope() {
        let scope = RoutingScope::from_rack("dc1".to_string(), "rack1".to_string());
        assert_eq!(scope.dc(), Some("dc1"));
        assert_eq!(scope.rack(), Some("rack1"));
        assert!(scope.fallback().is_none());
        assert!(scope.contains("dc1", "rack1"));
        assert!(!scope.contains("dc1", "rack2"));
        assert!(!scope.contains("dc2", "rack1"));
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
