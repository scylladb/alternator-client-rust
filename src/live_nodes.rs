//! Maintains and updates a list of known live Alternator nodes using the `/localnodes` endpoint.
//!
//! # Overview
//!
//! [`LiveNodes`] is constructed from an [`AlternatorConfig`] and seeded with a list of hosts.
//! Once [`start`] is called, a background Tokio task
//! periodically calls the [`update_live_nodes`] function which requests the known
//! nodes in a random order to get an updated list of live nodes. After a
//! successful refresh, the list is updated to nodes from the highest available
//! scope in the fallback chain provided by the user.
//! Underneath it uses a basic [`reqwest::Client`] with timeouts.
//!
//! # Polling cadence
//!
//! The refresh loop has two cadences:
//!
//! - **Active** ([`active_interval`]): used while the client is being called
//!   regularly. Polls run frequently to keep the view fresh under load.
//! - **Idle** ([`idle_interval`]): used when no caller has touched
//!   [`LiveNodes`] recently. An incoming request wakes the loop early via a [`Notify`].
//!
//! Activity is tracked through [`mark_activity`], which every read path calls.
//!
//! # Discovery mechanism
//!
//! Each refresh starts from the highest scope in the fallback chain, shuffles
//! the current node list, and walks it as a candidate queue:
//! - If a node responds with a non-empty list, the list is used as the new live nodes list,
//!   and the refresh ends.
//! - If a node responds with an empty list, it is put back at the end of the queue,
//!   and the next node is tried, with the next fallback scope.
//! - A network error causes the node to be dropped from the queue, but the next nodes are
//!   tried with the same scope.
//! - If the queue is exhausted without a successful response, it is populated with
//!   the seed nodes, and the process repeats. If the seeds are exhausted without success, the refresh ends with no changes.
//!
//! For cluster-wide scope, the refresh queries `/localnodes` from configured
//! seed nodes and already-known live nodes, then unions the responses. To cover
//! all datacenters, the initial configuration must include at least one working
//! seed host from every datacenter that should receive traffic.
//!
//! Once it successfully gets a non-empty response, it atomically updates the [`live_nodes`] list using [`ArcSwap`].
//!
//!  # Lifetime
//!
//! The background task holds a [`Weak`] reference to its [`LiveNodes`], so it
//! terminates on its own once the last external [`Arc`] is dropped. [`Drop`]
//! additionally aborts the task to avoid waiting out the current sleep.
//!
//! # Start-up
//!
//! The task is launched via [`tokio::spawn`], which requires an active Tokio runtime on the calling thread or else it panics.
//! The client's [`from_conf`] constructor, however, is synchronous and can be called from anywhere.
//! It is handled by funneling start-up through a single idempotent entry point:
//! [`ensure_discovery_started`]. It does three things, in order:
//!
//! 1. If discovery is already running, return immediately (an atomic load, essentially free).
//! 2. Runtime check: if no Tokio runtime is available on the current thread, return without spawning.
//!    The task will be started lazily on the first [`get_next_node_round_robin`] or [`get_live_nodes`] call,
//!    which is typically invoked from within the request pipeline and therefore from within a runtime.
//! 3. A `compare_exchange` on `discovery_started` ensures that
//!    exactly one caller wins the right to spawn the task, even under
//!    concurrent first-access from multiple threads.
//!
//! [`AlternatorConfig`]: crate::config::AlternatorConfig
//! [`RoutingScope`]: crate::routing_scope::RoutingScope
//! [`ArcSwap`]: arc_swap::ArcSwap
//! [`Notify`]: tokio::sync::Notify
//! [`Weak`]: std::sync::Weak
//! [`Arc`]: std::sync::Arc
//! [`active_interval`]: LiveNodes::active_interval
//! [`idle_interval`]: LiveNodes::idle_interval
//! [`mark_activity`]: LiveNodes::mark_activity
//! [`ensure_discovery_started`]: LiveNodes::ensure_discovery_started
//! [`start`]: LiveNodes::start
//! [`update_live_nodes`]: LiveNodes::update_live_nodes
//! [`get_next_node_round_robin`]: LiveNodes::get_next_node_round_robin
//! [`get_live_nodes`]: LiveNodes::get_live_nodes
//! [`live_nodes`]: LiveNodes::live_nodes
//! [`from_conf`]: crate::client::AlternatorClient::from_conf

use crate::routing_scope::RoutingScope;
use arc_swap::ArcSwap;
use rand::seq::SliceRandom;
use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::runtime::Handle;
use url::Url;

const DEFAULT_ACTIVE_REFRESH_INTERVAL: Duration = Duration::from_secs(1);
const DEFAULT_IDLE_REFRESH_INTERVAL: Duration = Duration::from_secs(60);

#[derive(Debug)]
pub struct LiveNodes {
    routing_scope: RoutingScope,
    active_interval: Duration,
    idle_interval: Duration,
    counter: Arc<AtomicUsize>,
    live_nodes: ArcSwap<Vec<Arc<Url>>>,
    seed_urls: Vec<Arc<Url>>,
    alternator_scheme: String,
    port: Option<u16>,
    client: reqwest::Client,
    last_activity: Arc<Mutex<Instant>>,
    notify: Arc<tokio::sync::Notify>,
    bg_task: std::sync::Mutex<Option<tokio::task::AbortHandle>>,
    discovery_started: AtomicBool,
}

impl LiveNodes {
    pub fn new(config: &crate::config::AlternatorConfig) -> Option<Arc<Self>> {
        let active_interval = config
            .active_interval()
            .unwrap_or(DEFAULT_ACTIVE_REFRESH_INTERVAL);
        let idle_interval = config
            .idle_interval()
            .unwrap_or(DEFAULT_IDLE_REFRESH_INTERVAL);
        let routing_scope = config
            .routing_scope()
            .unwrap_or(RoutingScope::from_cluster());
        let alternator_scheme = config.scheme().unwrap_or("http".to_string());
        let port = config.port();
        let seed_nodes = config.seed_hosts().unwrap_or_default();

        let mut seed_urls = seed_nodes
            .iter()
            .filter_map(|addr| {
                let mut url = Url::parse(&format!("{}://{}", alternator_scheme, addr)).ok()?;
                url.set_port(port).ok()?;
                Some(Arc::new(url))
            })
            .collect::<Vec<_>>();
        seed_urls.sort_unstable_by(|left, right| left.as_str().cmp(right.as_str()));
        if seed_urls.is_empty() {
            return None;
        }

        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(5))
            .connect_timeout(Duration::from_secs(2))
            .build()
            .ok()?;

        Some(Arc::new(Self {
            routing_scope,
            active_interval,
            idle_interval,
            counter: Arc::new(AtomicUsize::new(0)),
            live_nodes: ArcSwap::from_pointee(seed_urls.clone()),
            seed_urls,
            alternator_scheme,
            port,
            client,
            last_activity: Arc::new(Mutex::new(Instant::now())),
            notify: Arc::new(tokio::sync::Notify::new()),
            bg_task: std::sync::Mutex::new(None),
            discovery_started: AtomicBool::new(false),
        }))
    }

    fn host_to_uri(&self, addr: &str) -> Result<Url, url::ParseError> {
        let mut url = Url::parse(&format!("{}://{}", self.alternator_scheme, addr))?;
        url.set_port(self.port)
            .map_err(|()| url::ParseError::InvalidPort)?;
        Ok(url)
    }

    async fn fetch_live_nodes_for_scope(
        &self,
        scope: &RoutingScope,
        node_addr: &Url,
    ) -> Option<Vec<Arc<Url>>> {
        let url = scope.build_localnodes_url(node_addr.clone());
        let mut nodes = self
            .client
            .get(url)
            .send()
            .await
            .ok()?
            .json::<Vec<String>>()
            .await
            .ok()?;

        nodes.sort();
        Some(
            nodes
                .into_iter()
                .filter_map(|addr| self.host_to_uri(&addr).ok().map(Arc::new))
                .collect(),
        )
    }

    fn cluster_discovery_candidates(&self) -> Vec<Arc<Url>> {
        let mut candidates = self.live_nodes.load().as_ref().clone();
        candidates.extend(self.seed_urls.iter().cloned());
        candidates.sort_by(|a, b| a.as_str().cmp(b.as_str()));
        candidates.dedup_by(|a, b| a.as_str() == b.as_str());
        candidates.shuffle(&mut rand::rng());
        candidates
    }

    async fn discover_cluster_live_nodes(&self) -> Option<Vec<Arc<Url>>> {
        let scope = RoutingScope::from_cluster();
        let mut new_nodes = Vec::new();
        let mut got_response = false;

        for node_addr in self.cluster_discovery_candidates() {
            if node_is_in_list(&node_addr, &new_nodes) {
                continue;
            }

            if let Some(mut nodes) = self.fetch_live_nodes_for_scope(&scope, &node_addr).await {
                got_response = true;
                new_nodes.append(&mut nodes);
                new_nodes.sort_by(|a, b| a.as_str().cmp(b.as_str()));
                new_nodes.dedup_by(|a, b| a.as_str() == b.as_str());
            }
        }

        if !got_response {
            return None;
        }

        new_nodes.sort_by(|a, b| a.as_str().cmp(b.as_str()));
        new_nodes.dedup_by(|a, b| a.as_str() == b.as_str());
        Some(new_nodes)
    }

    /// Ensures the background discovery task is running.
    ///
    /// Idempotent and safe to call from any context: returns immediately if
    /// discovery is already started, or if no Tokio runtime is available.
    pub fn ensure_discovery_started(self: &Arc<Self>) {
        if self.discovery_started.load(Ordering::Acquire) {
            return;
        }

        if Handle::try_current().is_err() {
            return;
        }

        if self
            .discovery_started
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
        {
            Arc::clone(self).start();
        }
    }

    fn start(self: Arc<Self>) {
        let weak_self = Arc::downgrade(&self);
        let notify = self.notify.clone();

        self.mark_activity();
        let handle = tokio::spawn(async move {
            loop {
                let (idle_interval, active_interval, is_idle) = {
                    let Some(strong_self) = weak_self.upgrade() else {
                        break;
                    };

                    strong_self.update_live_nodes().await;

                    let last = *strong_self.last_activity.lock().unwrap();
                    (
                        strong_self.idle_interval,
                        strong_self.active_interval,
                        last.elapsed() >= strong_self.idle_interval,
                    )
                };

                if !is_idle {
                    tokio::time::sleep(active_interval).await;
                } else {
                    tokio::select! {
                        _ = tokio::time::sleep(idle_interval) => {}
                        _ = notify.notified() => {}
                    }
                }
            }
        });

        if let Ok(mut guard) = self.bg_task.lock() {
            *guard = Some(handle.abort_handle());
        }
    }

    fn mark_activity(&self) {
        let now = Instant::now();
        let mut last = self.last_activity.lock().unwrap();
        let was_idle = now.duration_since(*last) > self.idle_interval;
        *last = now;
        if was_idle {
            self.notify.notify_one();
        }
    }

    /// Returns a list of all current live nodes and updates the last activity timestamp.
    pub fn get_live_nodes(self: &Arc<Self>) -> Vec<Arc<Url>> {
        self.ensure_discovery_started();
        self.mark_activity();
        self.live_nodes.load().as_ref().clone()
    }

    /// Returns the first live node not in `used_nodes` starting with the next node in round-robin order.
    /// Used by [`crate::QueryPlan`] round-robin strategy.
    pub fn get_next_node_round_robin(
        self: &Arc<Self>,
        used_nodes: &std::collections::HashSet<Arc<Url>>,
    ) -> Option<Arc<Url>> {
        self.ensure_discovery_started();
        self.mark_activity();
        let live_nodes = self.live_nodes.load();

        let len = live_nodes.len();
        if len == 0 {
            return None;
        }

        let start = self.counter.fetch_add(1, Ordering::Relaxed) % len;
        for i in 0..len {
            let idx = (start + i) % len;
            let node = &live_nodes[idx];
            if !used_nodes.contains(node) {
                return Some(node.clone());
            }
        }
        None
    }

    pub async fn update_live_nodes(&self) {
        let mut scope = &self.routing_scope;
        // Live nodes in a random order.
        let mut nodes = self.live_nodes.load().as_ref().clone();
        nodes.shuffle(&mut rand::rng());
        let mut candidates: VecDeque<Arc<Url>> = nodes.into();
        let mut using_seeds = false;

        while let Some(node_addr) = candidates.pop_front() {
            if scope.is_cluster() {
                let Some(new_nodes) = self.discover_cluster_live_nodes().await else {
                    return;
                };

                if new_nodes.is_empty() {
                    let Some(fallback) = scope.fallback() else {
                        return;
                    };
                    scope = fallback;
                    candidates.push_back(node_addr);
                    continue;
                }

                if **self.live_nodes.load() != new_nodes {
                    self.live_nodes.store(Arc::new(new_nodes));
                }
                return;
            }

            let result = self.fetch_live_nodes_for_scope(scope, &node_addr).await;

            // Request failed: try the next candidate, or fall back to seeds.
            let Some(new_nodes) = result else {
                if candidates.is_empty() && !using_seeds {
                    using_seeds = true;
                    candidates = self.seed_urls.clone().into();
                }
                continue;
            };

            // Empty result: retry under a fallback scope if one exists.
            if new_nodes.is_empty() {
                let Some(fallback) = scope.fallback() else {
                    return;
                };
                scope = fallback;
                candidates.push_back(node_addr);
                continue;
            }

            if **self.live_nodes.load() != new_nodes {
                self.live_nodes.store(Arc::new(new_nodes));
            }
            return;
        }
    }
}

fn node_is_in_list(node: &Url, nodes: &[Arc<Url>]) -> bool {
    nodes.iter().any(|known| {
        known.host_str() == node.host_str()
            && known.port_or_known_default() == node.port_or_known_default()
    })
}

impl Drop for LiveNodes {
    fn drop(&mut self) {
        if let Ok(mut guard) = self.bg_task.lock()
            && let Some(task) = guard.take()
        {
            task.abort();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::AlternatorConfig;

    fn test_config() -> AlternatorConfig {
        AlternatorConfig::builder()
            .behavior_version_latest()
            .endpoint_url("http://127.0.0.1:1".to_string())
            .build()
    }

    #[test]
    fn start_without_runtime_does_not_panic() {
        let nodes = LiveNodes::new(&test_config()).unwrap();
        LiveNodes::ensure_discovery_started(&nodes);
        assert!(!nodes.discovery_started.load(Ordering::Acquire));
    }

    #[tokio::test]
    async fn start_with_runtime_starts_correctly() {
        let nodes = LiveNodes::new(&test_config()).unwrap();
        LiveNodes::ensure_discovery_started(&nodes);
        assert!(nodes.discovery_started.load(Ordering::Acquire));
    }

    #[test]
    fn start_on_first_access_round_robin() {
        let nodes = LiveNodes::new(&test_config()).unwrap();
        LiveNodes::ensure_discovery_started(&nodes);
        assert!(!nodes.discovery_started.load(Ordering::Acquire));

        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let _ = nodes.get_next_node_round_robin(&std::collections::HashSet::new());
        });
        assert!(nodes.discovery_started.load(Ordering::Acquire));
    }

    #[test]
    fn start_on_first_access() {
        let nodes = LiveNodes::new(&test_config()).unwrap();
        LiveNodes::ensure_discovery_started(&nodes);
        assert!(!nodes.discovery_started.load(Ordering::Acquire));

        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let _ = nodes.get_live_nodes();
        });
        assert!(nodes.discovery_started.load(Ordering::Acquire));
    }

    #[test]
    fn ipv6_address_parsing() {
        let config = AlternatorConfig::builder()
            .behavior_version_latest()
            .endpoint_url("http://[::1]:8000".to_string())
            .build();
        let nodes = LiveNodes::new(&config).unwrap();
        assert_eq!(nodes.seed_urls[0].scheme(), "http");
        assert_eq!(nodes.seed_urls[0].host_str(), Some("[::1]"));
        assert_eq!(nodes.seed_urls[0].port(), Some(8000));
        assert_eq!(nodes.seed_urls[0].to_string(), "http://[::1]:8000/");
    }
}
