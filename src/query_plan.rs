//! Query plan for Alternator requests.
//!
//! The object is stored in the config and is used on each request to determine
//! which node to send the request to.

use crate::keyrouting::go_rand::GoRand;
use crate::live_nodes::LiveNodes;
use aws_smithy_types::config_bag::{Storable, StoreReplace};
use std::collections::{HashSet, VecDeque};
use std::sync::{Arc, Mutex};
use url::Url;

#[derive(Debug)]
pub(crate) struct QueryPlan {
    live_nodes: Arc<LiveNodes>,
    state: Mutex<QueryPlanState>,
}

#[derive(Debug)]
pub(crate) struct SortedAffinityNodes {
    nodes: Vec<Arc<Url>>,
}

#[derive(Debug)]
enum QueryPlanState {
    /// Non-seeded fallback state (Round-Robin)
    RoundRobin { used_nodes: HashSet<Arc<Url>> },
    /// Seeded deterministic state for Key Route Affinity
    Affinity {
        // Boxed to prevent "large size difference between variants" warning
        go_rand: Box<GoRand>,
        remaining_nodes: Option<Vec<Arc<Url>>>,
    },
    /// Deterministic order with selected nodes before the rest.
    PreferredNodes {
        preferred_nodes: Vec<Arc<Url>>,
        remaining_nodes: Option<VecDeque<Arc<Url>>>,
    },
}

impl Storable for QueryPlan {
    type Storer = StoreReplace<Self>;
}

impl QueryPlan {
    /// Creates a round-robin query plan
    pub fn new_basic(live_nodes: Arc<LiveNodes>) -> Self {
        Self {
            live_nodes,
            state: Mutex::new(QueryPlanState::RoundRobin {
                used_nodes: HashSet::new(),
            }),
        }
    }

    /// Creates a seeded affinity query plan using GoRand
    pub fn new_with_hash(live_nodes: Arc<LiveNodes>, seed: u64) -> Self {
        Self {
            live_nodes,
            state: Mutex::new(QueryPlanState::Affinity {
                go_rand: Box::new(GoRand::new(seed as i64)),
                remaining_nodes: None,
            }),
        }
    }

    /// Creates a query plan that tries `preferred_nodes` first, then the
    /// remaining live nodes in sorted order.
    pub(crate) fn new_with_preferred_nodes(
        live_nodes: Arc<LiveNodes>,
        preferred_nodes: Vec<Arc<Url>>,
    ) -> Self {
        Self {
            live_nodes,
            state: Mutex::new(QueryPlanState::PreferredNodes {
                preferred_nodes,
                remaining_nodes: None,
            }),
        }
    }

    /// Returns the current live-node list in the canonical order used by
    /// affinity routing.
    pub(crate) fn sorted_affinity_nodes(live_nodes: &Arc<LiveNodes>) -> SortedAffinityNodes {
        SortedAffinityNodes::from_live_nodes(live_nodes)
    }

    /// Gets the next node to use in this query plan, or `None` if the plan is exhausted.
    ///
    /// With round-robin, on every attempt, the first node that hasn't been used yet in this request is returned.
    /// Search begins from the last used node in the live nodes list, so that requests are distributed evenly across the cluster.
    ///
    /// With affinity, the next node is selected from the remaining nodes using the pick-and-remove algorithm with GoRand.
    pub fn next_node(&self) -> Option<Arc<Url>> {
        let mut state = self.state.lock().unwrap();

        match &mut *state {
            QueryPlanState::RoundRobin { used_nodes } => {
                let node = self.live_nodes.get_next_node_round_robin(used_nodes)?;
                used_nodes.insert(node.clone());
                Some(node)
            }
            QueryPlanState::Affinity {
                go_rand,
                remaining_nodes,
            } => {
                let remaining = remaining_nodes.get_or_insert_with(|| {
                    let mut nodes = self.live_nodes.get_live_nodes();
                    sort_node_urls(&mut nodes);
                    nodes
                });

                if remaining.is_empty() {
                    return None;
                }

                // Pick-and-Remove Algorithm.
                let idx = go_rand.intn(remaining.len() as i32) as usize;
                let selected_node = remaining[idx].clone();
                let last_idx = remaining.len() - 1;

                remaining[idx] = remaining[last_idx].clone();
                remaining.pop();

                Some(selected_node)
            }
            QueryPlanState::PreferredNodes {
                preferred_nodes,
                remaining_nodes,
            } => {
                let remaining = remaining_nodes.get_or_insert_with(|| {
                    let mut nodes = self.live_nodes.get_live_nodes();
                    sort_node_urls(&mut nodes);

                    let mut ordered = VecDeque::with_capacity(nodes.len());
                    for preferred_node in preferred_nodes.iter() {
                        if let Some(index) = nodes.iter().position(|node| node == preferred_node) {
                            ordered.push_back(nodes.remove(index));
                        }
                    }
                    ordered.extend(nodes);
                    ordered
                });

                remaining.pop_front()
            }
        }
    }
}

impl SortedAffinityNodes {
    fn from_live_nodes(live_nodes: &Arc<LiveNodes>) -> Self {
        let mut nodes = live_nodes.get_live_nodes();
        sort_node_urls(&mut nodes);
        Self { nodes }
    }

    /// Returns the first node the seeded affinity plan would select without
    /// consuming a full query plan.
    pub(crate) fn preferred_node_for_hash(&self, seed: u64) -> Option<Arc<Url>> {
        if self.nodes.is_empty() {
            return None;
        }

        let mut go_rand = GoRand::new(seed as i64);
        let idx = go_rand.intn(self.nodes.len() as i32) as usize;
        Some(self.nodes[idx].clone())
    }
}

fn sort_node_urls(nodes: &mut [Arc<Url>]) {
    nodes.sort_unstable_by(|left, right| left.as_str().cmp(right.as_str()));
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::AlternatorConfig;
    use std::sync::Arc;
    use url::Url;

    fn make_live_nodes(count: usize) -> Arc<LiveNodes> {
        let seed_hosts: Vec<String> = (1..=count)
            .map(|i| format!("node{i}.example.com"))
            .collect();

        let config = AlternatorConfig::builder()
            .scheme("http")
            .port(8000)
            .seed_hosts(seed_hosts)
            .build();

        let live = LiveNodes::new(&config).unwrap();

        // Sanity check.
        let urls = live.get_live_nodes();
        assert_eq!(
            urls.len(),
            count,
            "LiveNodes lost or duplicated seed entries"
        );
        let mut expected_hosts = (1..=count)
            .map(|i| format!("node{i}.example.com"))
            .collect::<Vec<_>>();
        expected_hosts.sort_unstable();
        for (url, expected_host) in urls.iter().zip(expected_hosts) {
            assert_eq!(url.host_str(), Some(expected_host.as_str()));
        }

        live
    }

    /// Extract "node6" from "http://node6.example.com:8000".
    fn short_name(url: &Url) -> String {
        let host = url.host_str().expect("url has host");
        host.strip_suffix(".example.com")
            .unwrap_or(host)
            .to_string()
    }

    /// Draw `count` nodes from the plan as short names. Stops early on exhaustion.
    fn sequence(plan: &QueryPlan, count: usize) -> Vec<String> {
        let mut out = Vec::with_capacity(count);
        for _ in 0..count {
            match plan.next_node() {
                Some(node) => out.push(short_name(&node)),
                None => break,
            }
        }
        out
    }

    // ----- Cross-language test vectors -----
    //
    // These vectors use the canonical lexicographic live-node order.

    #[test]
    fn cross_lang_seed_42_10_nodes() {
        let plan = QueryPlan::new_with_hash(make_live_nodes(10), 42);
        assert_eq!(
            sequence(&plan, 6),
            vec!["node5", "node8", "node4", "node10", "node6", "node1"],
        );
    }

    #[test]
    fn cross_lang_seed_123_10_nodes() {
        let plan = QueryPlan::new_with_hash(make_live_nodes(10), 123);
        assert_eq!(
            sequence(&plan, 6),
            vec!["node5", "node1", "node3", "node2", "node9", "node4"],
        );
    }

    #[test]
    fn cross_lang_seed_999_10_nodes() {
        let plan = QueryPlan::new_with_hash(make_live_nodes(10), 999);
        assert_eq!(
            sequence(&plan, 6),
            vec!["node4", "node9", "node3", "node1", "node10", "node2"],
        );
    }

    #[test]
    fn cross_lang_seed_0_10_nodes() {
        let plan = QueryPlan::new_with_hash(make_live_nodes(10), 0);
        assert_eq!(
            sequence(&plan, 6),
            vec!["node4", "node1", "node10", "node9", "node5", "node7"],
        );
    }

    #[test]
    fn cross_lang_seed_neg1_10_nodes() {
        // Seed -1 as a u64 is 0xFFFF_FFFF_FFFF_FFFF; inside new_with_hash it's
        // cast back to i64 = -1, matching Go's int64 seed semantics.
        let plan = QueryPlan::new_with_hash(make_live_nodes(10), u64::MAX);
        assert_eq!(
            sequence(&plan, 6),
            vec!["node10", "node4", "node1", "node2", "node5", "node9"],
        );
    }

    #[test]
    fn cross_lang_seed_42_6_active_nodes() {
        let plan = QueryPlan::new_with_hash(make_live_nodes(6), 42);
        assert_eq!(
            sequence(&plan, 6),
            vec!["node6", "node3", "node1", "node4", "node2", "node5"],
        );
    }

    #[test]
    fn cross_lang_seed_12345_10_nodes() {
        let plan = QueryPlan::new_with_hash(make_live_nodes(10), 12345);
        assert_eq!(
            sequence(&plan, 6),
            vec!["node3", "node4", "node1", "node6", "node5", "node7"],
        );
    }

    #[test]
    fn cross_lang_seed_max_i64_10_nodes() {
        // i64::MAX = 0x7FFF_FFFF_FFFF_FFFF — the largest positive int64.
        let plan = QueryPlan::new_with_hash(make_live_nodes(10), i64::MAX as u64);
        assert_eq!(
            sequence(&plan, 6),
            vec!["node10", "node6", "node7", "node1", "node9", "node3"],
        );
    }

    #[test]
    fn preferred_nodes_plan_uses_selected_nodes_first_then_sorted_remaining() {
        let live_nodes = make_live_nodes(5);
        let preferred_3 = live_nodes
            .get_live_nodes()
            .into_iter()
            .find(|node| node.host_str() == Some("node3.example.com"))
            .expect("preferred node exists");
        let preferred_5 = live_nodes
            .get_live_nodes()
            .into_iter()
            .find(|node| node.host_str() == Some("node5.example.com"))
            .expect("preferred node exists");
        let plan = QueryPlan::new_with_preferred_nodes(live_nodes, vec![preferred_3, preferred_5]);

        assert_eq!(
            sequence(&plan, 5),
            vec!["node3", "node5", "node1", "node2", "node4"],
        );
    }

    // ----- Property tests -----

    #[test]
    fn affinity_plan_exhausts_all_nodes_without_duplicates() {
        let plan = QueryPlan::new_with_hash(make_live_nodes(10), 42);
        let all = sequence(&plan, 100); // ask for more than exist
        assert_eq!(all.len(), 10, "should produce exactly 10 nodes");
        let unique: std::collections::HashSet<_> = all.iter().collect();
        assert_eq!(unique.len(), 10, "all nodes should be distinct");
        assert!(plan.next_node().is_none(), "plan should be exhausted");
    }

    #[test]
    fn affinity_plan_is_deterministic_for_same_seed() {
        let p1 = QueryPlan::new_with_hash(make_live_nodes(10), 42);
        let p2 = QueryPlan::new_with_hash(make_live_nodes(10), 42);
        assert_eq!(sequence(&p1, 10), sequence(&p2, 10));
    }
}
