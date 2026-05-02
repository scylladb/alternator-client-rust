//! Query plan for Alternator requests.
//!
//! The object is put in the config and on each requests is used to determine which node to send the request to.

use crate::live_nodes::LiveNodes;
use aws_smithy_types::config_bag::{Storable, StoreReplace};
use std::collections::HashSet;
use std::sync::{Arc, Mutex};
use url::Url;
#[derive(Debug)]
pub(crate) struct QueryPlan {
    live_nodes: Arc<LiveNodes>,
    used_nodes: Mutex<HashSet<Arc<Url>>>,
}

impl Storable for QueryPlan {
    type Storer = StoreReplace<Self>;
}

impl QueryPlan {
    pub fn new(live_nodes: Arc<LiveNodes>) -> Self {
        Self {
            live_nodes,
            used_nodes: Mutex::new(HashSet::new()),
        }
    }

    /// On every attempt, the first node that hasn't been used yet in this request is returned.
    /// Search begins from the last used node in the live nodes list, so that requests are distributed evenly across the cluster.
    pub fn next_node(&self) -> Option<Arc<Url>> {
        let mut used_nodes = self.used_nodes.lock().unwrap();
        let node = self.live_nodes.get_next_node_round_robin(&used_nodes)?;
        used_nodes.insert(node.clone());
        Some(node)
    }
}
