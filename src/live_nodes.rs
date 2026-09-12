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
//! The task is launched on the current Tokio runtime, which requires an active
//! runtime on the calling thread.
//! The client's [`from_conf`] constructor, however, is synchronous and can be called from anywhere.
//! It is handled by funneling start-up through a single idempotent entry point:
//! [`ensure_discovery_started`]. It does three things, in order:
//!
//! 1. If discovery is already running on the caller's runtime, return through
//!    a lock-free fast path. If another runtime owns it, retain that owner while
//!    a lightweight probe shows the runtime still accepts work.
//! 2. If no Tokio runtime is available on the current thread, return without spawning.
//!    The task will be started lazily on the first [`get_next_node_round_robin`] or [`get_live_nodes`] call,
//!    which is typically invoked from within the request pipeline and therefore from within a runtime.
//! 3. An atomic registration plus a mutex on the cold start or confirmed
//!    shutdown path ensures that exactly one caller starts the task. A
//!    task-owned guard clears only its registration when it exits.
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
use arc_swap::{ArcSwap, ArcSwapOption};
use futures_util::FutureExt;
use rand::seq::SliceRandom;
use std::collections::VecDeque;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, Weak};
use std::time::{Duration, Instant};
use tokio::runtime::Handle;
use url::Url;

const DEFAULT_ACTIVE_REFRESH_INTERVAL: Duration = Duration::from_secs(1);
const DEFAULT_IDLE_REFRESH_INTERVAL: Duration = Duration::from_secs(60);

/// An error encountered while constructing live-node discovery state.
#[derive(Debug)]
pub(crate) enum LiveNodesBuildError {
    MissingRoutingTarget,
    InvalidSeedHost {
        seed_host: String,
        source: url::ParseError,
    },
    InvalidScheme(String),
    TlsConfiguration(String),
    HttpClient(reqwest::Error),
}

impl std::fmt::Display for LiveNodesBuildError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::MissingRoutingTarget => formatter.write_str(
                "no Alternator routing target configured; set endpoint_url or non-empty seed_hosts",
            ),
            Self::InvalidSeedHost { seed_host, source } => {
                write!(formatter, "invalid seed host {seed_host:?}: {source}")
            }
            Self::InvalidScheme(scheme) => write!(
                formatter,
                "invalid Alternator transport scheme {scheme:?}: expected http or https"
            ),
            Self::TlsConfiguration(message) => {
                write!(formatter, "failed to configure discovery TLS: {message}")
            }
            Self::HttpClient(source) => {
                write!(
                    formatter,
                    "failed to build the HTTP client for live-node discovery: {source}"
                )
            }
        }
    }
}

impl std::error::Error for LiveNodesBuildError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::MissingRoutingTarget | Self::InvalidScheme(_) => None,
            Self::InvalidSeedHost { source, .. } => Some(source),
            Self::TlsConfiguration(_) => None,
            Self::HttpClient(source) => Some(source),
        }
    }
}

#[cfg(test)]
fn discovery_http_client_builder(
    scheme: &str,
) -> Result<reqwest::ClientBuilder, LiveNodesBuildError> {
    discovery_http_client_builder_with_root_status(scheme).map(|(builder, _)| builder)
}

fn discovery_http_client_builder_with_root_status(
    scheme: &str,
) -> Result<(reqwest::ClientBuilder, bool), LiveNodesBuildError> {
    let (roots, errors) = load_native_root_store();
    let native_roots_usable = !roots.is_empty();
    if scheme.eq_ignore_ascii_case("https") && !native_roots_usable {
        return Err(LiveNodesBuildError::TlsConfiguration(
            unusable_native_roots_message(errors),
        ));
    }

    let provider = Arc::new(rustls::crypto::aws_lc_rs::default_provider());
    let tls_config = rustls::ClientConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()
        .map_err(|error| LiveNodesBuildError::TlsConfiguration(error.to_string()))?
        .with_root_certificates(roots)
        .with_no_client_auth();

    Ok((
        reqwest::Client::builder().use_preconfigured_tls(tls_config),
        native_roots_usable,
    ))
}

fn load_native_root_store() -> (rustls::RootCertStore, Vec<String>) {
    let load_results = rustls_native_certs::load_native_certs();
    let mut roots = rustls::RootCertStore::empty();
    let mut errors = load_results
        .errors
        .into_iter()
        .map(|error| error.to_string())
        .collect::<Vec<_>>();

    for certificate in load_results.certs {
        if let Err(error) = roots.add(certificate) {
            errors.push(error.to_string());
        }
    }

    (roots, errors)
}

pub(crate) fn native_roots_are_usable() -> bool {
    let (roots, _) = load_native_root_store();
    !roots.is_empty()
}

pub(crate) fn ensure_native_roots_are_usable() -> Result<(), String> {
    let (roots, errors) = load_native_root_store();
    if roots.is_empty() {
        Err(unusable_native_roots_message(errors))
    } else {
        Ok(())
    }
}

fn unusable_native_roots_message(errors: Vec<String>) -> String {
    if errors.is_empty() {
        "no usable native CA certificates were found".to_string()
    } else {
        errors.join("; ")
    }
}

fn build_discovery_http_client(
    scheme: &str,
) -> Result<(reqwest::Client, bool), LiveNodesBuildError> {
    let (builder, native_roots_usable) = discovery_http_client_builder_with_root_status(scheme)?;
    let client = builder
        .timeout(Duration::from_secs(5))
        .connect_timeout(Duration::from_secs(2))
        .build()
        .map_err(LiveNodesBuildError::HttpClient)?;
    Ok((client, native_roots_usable))
}

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
    native_roots_usable: bool,
    last_activity: Arc<Mutex<Instant>>,
    notify: Arc<tokio::sync::Notify>,
    bg_task: std::sync::Mutex<Option<tokio::task::AbortHandle>>,
    discovery_runtime: ArcSwapOption<DiscoveryRuntime>,
}

#[derive(Debug)]
struct DiscoveryRuntime {
    id: tokio::runtime::Id,
    handle: Handle,
}

impl DiscoveryRuntime {
    fn is_shutdown(&self) -> bool {
        // A live blocking pool either runs this closure or leaves it pending.
        // Once runtime shutdown starts, spawn_blocking rejects it synchronously
        // with a cancelled JoinError. Unlike an async probe, this still works
        // while an async worker cannot drop the discovery task's guard.
        matches!(
            self.handle.spawn_blocking(|| ()).now_or_never(),
            Some(Err(error)) if error.is_cancelled()
        )
    }
}

struct DiscoveryTaskGuard {
    live_nodes: Weak<LiveNodes>,
    runtime: Arc<DiscoveryRuntime>,
}

impl Drop for DiscoveryTaskGuard {
    fn drop(&mut self) {
        if let Some(live_nodes) = self.live_nodes.upgrade() {
            // Clear only this task's exact registration so cleanup can never
            // erase a newer registration.
            drop(
                live_nodes
                    .discovery_runtime
                    .compare_and_swap(&self.runtime, None),
            );
        }
    }
}

impl LiveNodes {
    /// Creates discovery state from the configured seed hosts.
    ///
    /// Returns [`None`] when an SDK endpoint URL is configured and discovery is
    /// explicitly disabled with an empty seed-host list.
    ///
    /// # Panics
    ///
    /// Panics if routing configuration is missing or invalid, or if the
    /// discovery HTTP client cannot be constructed. Invalid routing
    /// configuration fails closed instead of falling back to an unrelated SDK
    /// endpoint.
    pub fn new(config: &crate::config::AlternatorConfig) -> Option<Arc<Self>> {
        Self::try_new(config)
            .unwrap_or_else(|error| panic!("failed to construct LiveNodes: {error}"))
    }

    pub(crate) fn try_new(
        config: &crate::config::AlternatorConfig,
    ) -> Result<Option<Arc<Self>>, LiveNodesBuildError> {
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
        let Some(seed_nodes) = config.seed_hosts() else {
            return Err(LiveNodesBuildError::MissingRoutingTarget);
        };

        if seed_nodes.is_empty() {
            let Some(endpoint_url) = config.endpoint_url() else {
                return Err(LiveNodesBuildError::MissingRoutingTarget);
            };
            if config.http_client().is_none() {
                let endpoint = Url::parse(endpoint_url)
                    .map_err(|_| LiveNodesBuildError::MissingRoutingTarget)?;
                if !endpoint.scheme().eq_ignore_ascii_case("http")
                    && !endpoint.scheme().eq_ignore_ascii_case("https")
                {
                    return Err(LiveNodesBuildError::InvalidScheme(
                        endpoint.scheme().to_string(),
                    ));
                }
            }
            return Ok(None);
        }

        if !alternator_scheme.eq_ignore_ascii_case("http")
            && !alternator_scheme.eq_ignore_ascii_case("https")
        {
            return Err(LiveNodesBuildError::InvalidScheme(alternator_scheme));
        }

        let mut seed_urls = seed_nodes
            .iter()
            .map(|seed_host| {
                build_seed_url(&alternator_scheme, seed_host, port)
                    .map(Arc::new)
                    .map_err(|source| LiveNodesBuildError::InvalidSeedHost {
                        seed_host: seed_host.clone(),
                        source,
                    })
            })
            .collect::<Result<Vec<_>, _>>()?;
        seed_urls.sort_unstable_by(|left, right| left.as_str().cmp(right.as_str()));
        let (client, native_roots_usable) = build_discovery_http_client(seed_urls[0].scheme())?;

        Ok(Some(Arc::new(Self {
            routing_scope,
            active_interval,
            idle_interval,
            counter: Arc::new(AtomicUsize::new(0)),
            live_nodes: ArcSwap::from_pointee(seed_urls.clone()),
            seed_urls,
            alternator_scheme,
            port,
            client,
            native_roots_usable,
            last_activity: Arc::new(Mutex::new(Instant::now())),
            notify: Arc::new(tokio::sync::Notify::new()),
            bg_task: std::sync::Mutex::new(None),
            discovery_runtime: ArcSwapOption::empty(),
        })))
    }

    pub(crate) fn scheme(&self) -> &str {
        self.seed_urls[0].scheme()
    }

    pub(crate) fn has_usable_native_roots(&self) -> bool {
        self.native_roots_usable
    }

    fn host_to_uri(&self, addr: &str) -> Result<Url, url::ParseError> {
        build_node_url(&self.alternator_scheme, addr, self.port)
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
    /// discovery is already running on a live Tokio runtime, or if no runtime
    /// is available. A caller on another runtime keeps a healthy owner stable,
    /// but takes ownership when a probe confirms that the old runtime has shut
    /// down.
    pub fn ensure_discovery_started(self: &Arc<Self>) {
        let Ok(handle) = Handle::try_current() else {
            return;
        };
        let runtime_id = handle.id();
        // Requests on the owning runtime never take the start/transfer mutex.
        // A caller on another runtime transfers only after a probe spawned on
        // the owner is synchronously rejected because its scheduler is closed.
        if self
            .discovery_runtime
            .load()
            .as_ref()
            .is_some_and(|active| active.id == runtime_id || !active.is_shutdown())
        {
            return;
        }

        let mut bg_task = self.bg_task.lock().unwrap_or_else(|err| err.into_inner());
        // Another caller may have completed the cold start or transfer while
        // this caller waited for the mutex.
        if self
            .discovery_runtime
            .load()
            .as_ref()
            .is_some_and(|active| active.id == runtime_id || !active.is_shutdown())
        {
            return;
        }

        if let Some(old_task) = bg_task.take() {
            old_task.abort();
        }

        let runtime = Arc::new(DiscoveryRuntime {
            id: runtime_id,
            handle: handle.clone(),
        });
        self.discovery_runtime.store(Some(runtime.clone()));
        let weak_self = Arc::downgrade(self);
        let notify = self.notify.clone();
        let task_guard = DiscoveryTaskGuard {
            live_nodes: weak_self.clone(),
            runtime: runtime.clone(),
        };

        self.mark_activity();
        let task = handle.spawn(async move {
            let _task_guard = task_guard;
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
        *bg_task = Some(task.abort_handle());
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

fn build_seed_url(scheme: &str, addr: &str, port: Option<u16>) -> Result<Url, url::ParseError> {
    let unbracketed = addr
        .strip_prefix('[')
        .and_then(|addr| addr.strip_suffix(']'))
        .unwrap_or(addr);
    if unbracketed.parse::<std::net::Ipv6Addr>().is_err() {
        url::Host::parse(addr)?;
    }
    build_node_url(scheme, unbracketed, port)
}

fn build_node_url(scheme: &str, addr: &str, port: Option<u16>) -> Result<Url, url::ParseError> {
    let authority = if addr.parse::<std::net::Ipv6Addr>().is_ok() {
        format!("[{addr}]")
    } else {
        addr.to_string()
    };
    let mut url = Url::parse(&format!("{scheme}://{authority}"))?;
    url.set_port(port)
        .map_err(|()| url::ParseError::InvalidPort)?;
    Ok(url)
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
    use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

    fn discovery_is_running(nodes: &LiveNodes) -> bool {
        nodes.discovery_runtime.load().is_some()
    }

    fn test_config() -> AlternatorConfig {
        AlternatorConfig::builder()
            .behavior_version_latest()
            .endpoint_url("http://127.0.0.1:1".to_string())
            .build()
    }

    async fn start_localnodes_server(body: &'static str) -> (u16, tokio::task::JoinHandle<()>) {
        start_localnodes_server_on("127.0.0.1:0", "localhost", body).await
    }

    async fn start_localnodes_server_on(
        bind_address: &str,
        expected_host: &str,
        body: &'static str,
    ) -> (u16, tokio::task::JoinHandle<()>) {
        let listener = TcpListener::bind(bind_address).await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let expected_host = expected_host.to_string();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut buffer = [0; 1024];
            let n = stream.read(&mut buffer).await.unwrap();
            let request = String::from_utf8_lossy(&buffer[..n]);
            assert!(request.starts_with("GET /localnodes HTTP/1.1"));
            assert!(
                request.contains(&format!("host: {expected_host}:{port}"))
                    || request.contains(&format!("Host: {expected_host}:{port}"))
            );

            let response = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                body.len(),
                body
            );
            stream.write_all(response.as_bytes()).await.unwrap();
        });

        (port, server)
    }

    fn start_runtime_restart_server() -> (u16, Arc<AtomicUsize>, std::thread::JoinHandle<()>) {
        use std::io::{Read, Write};

        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        let request_count = Arc::new(AtomicUsize::new(0));
        let server_request_count = request_count.clone();
        let server = std::thread::spawn(move || {
            for body in [r#"["127.0.0.1"]"#, r#"["127.0.0.2"]"#] {
                let (mut stream, _) = listener.accept().unwrap();
                let mut buffer = Vec::new();
                while !buffer.windows(4).any(|window| window == b"\r\n\r\n") {
                    let mut chunk = [0; 512];
                    let length = stream.read(&mut chunk).unwrap();
                    assert!(length > 0, "request ended before its headers");
                    buffer.extend_from_slice(&chunk[..length]);
                }
                let request = String::from_utf8_lossy(&buffer);
                assert!(request.starts_with("GET /localnodes HTTP/1.1"));

                let response = format!(
                    "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                    body.len(),
                    body
                );
                stream.write_all(response.as_bytes()).unwrap();
                server_request_count.fetch_add(1, Ordering::SeqCst);
            }
        });

        (port, request_count, server)
    }

    #[test]
    fn start_without_runtime_does_not_panic() {
        let nodes = LiveNodes::new(&test_config()).unwrap();
        LiveNodes::ensure_discovery_started(&nodes);
        assert!(!discovery_is_running(&nodes));
    }

    #[tokio::test]
    async fn start_with_runtime_starts_correctly() {
        let nodes = LiveNodes::new(&test_config()).unwrap();
        LiveNodes::ensure_discovery_started(&nodes);
        assert!(discovery_is_running(&nodes));
    }

    #[test]
    fn start_on_first_access_round_robin() {
        let nodes = LiveNodes::new(&test_config()).unwrap();
        LiveNodes::ensure_discovery_started(&nodes);
        assert!(!discovery_is_running(&nodes));

        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let _ = nodes.get_next_node_round_robin(&std::collections::HashSet::new());
        });
        assert!(discovery_is_running(&nodes));
    }

    #[test]
    fn start_on_first_access() {
        let nodes = LiveNodes::new(&test_config()).unwrap();
        LiveNodes::ensure_discovery_started(&nodes);
        assert!(!discovery_is_running(&nodes));

        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let _ = nodes.get_live_nodes();
        });
        assert!(discovery_is_running(&nodes));
    }

    #[test]
    fn discovery_restarts_after_its_runtime_is_dropped() {
        let (port, request_count, server) = start_runtime_restart_server();
        let config = AlternatorConfig::builder()
            .behavior_version_latest()
            .endpoint_url(format!("http://127.0.0.1:{port}"))
            .active_interval(Duration::from_secs(60 * 60))
            .idle_interval(Duration::from_secs(60 * 60))
            .build();
        let nodes = LiveNodes::new(&config).unwrap();

        {
            let runtime = tokio::runtime::Runtime::new().unwrap();
            runtime.block_on(async {
                nodes.ensure_discovery_started();
                tokio::time::timeout(Duration::from_secs(2), async {
                    while request_count.load(Ordering::SeqCst) < 1 {
                        tokio::time::sleep(Duration::from_millis(10)).await;
                    }
                })
                .await
                .expect("first runtime did not perform discovery");
            });
            drop(runtime);
        }

        {
            let runtime = tokio::runtime::Runtime::new().unwrap();
            runtime.block_on(async {
                nodes.ensure_discovery_started();
                tokio::time::timeout(Duration::from_secs(2), async {
                    loop {
                        let current_nodes = nodes.get_live_nodes();
                        if request_count.load(Ordering::SeqCst) >= 2
                            && current_nodes[0].host_str() == Some("127.0.0.2")
                        {
                            break;
                        }
                        tokio::time::sleep(Duration::from_millis(10)).await;
                    }
                })
                .await
                .expect("second runtime did not restart discovery");
            });
            drop(runtime);
        }

        server.join().unwrap();
    }

    #[test]
    fn healthy_discovery_owner_is_stable_across_runtimes() {
        let nodes = LiveNodes::new(&test_config()).unwrap();
        let first_runtime = tokio::runtime::Runtime::new().unwrap();
        let second_runtime = tokio::runtime::Runtime::new().unwrap();

        first_runtime.block_on(async {
            nodes.ensure_discovery_started();
        });
        let original_registration = nodes.discovery_runtime.load_full().unwrap();

        second_runtime.block_on(async {
            for _ in 0..10 {
                let _ = nodes.get_live_nodes();
            }
        });

        let current_registration = nodes.discovery_runtime.load_full().unwrap();
        assert!(Arc::ptr_eq(&original_registration, &current_registration));
    }

    #[test]
    fn first_access_hands_discovery_off_after_background_runtime_shutdown() {
        let (port, request_count, server) = start_runtime_restart_server();
        let config = AlternatorConfig::builder()
            .behavior_version_latest()
            .endpoint_url(format!("http://127.0.0.1:{port}"))
            .active_interval(Duration::from_secs(60 * 60))
            .idle_interval(Duration::from_secs(60 * 60))
            .build();
        let nodes = LiveNodes::new(&config).unwrap();

        let first_runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()
            .unwrap();
        first_runtime.block_on(async {
            nodes.ensure_discovery_started();
            tokio::time::timeout(Duration::from_secs(2), async {
                while request_count.load(Ordering::SeqCst) < 1 {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            })
            .await
            .expect("first runtime did not perform discovery");
        });

        // Keep the only worker occupied so shutdown_background returns before
        // it can drop the discovery future and run its task guard.
        let (blocker_started_tx, blocker_started_rx) = std::sync::mpsc::sync_channel(0);
        let (release_blocker_tx, release_blocker_rx) = std::sync::mpsc::sync_channel(0);
        first_runtime.spawn(async move {
            blocker_started_tx.send(()).unwrap();
            release_blocker_rx.recv().unwrap();
        });
        blocker_started_rx
            .recv_timeout(Duration::from_secs(2))
            .expect("runtime worker was not blocked");

        let second_runtime = tokio::runtime::Runtime::new().unwrap();
        first_runtime.shutdown_background();
        second_runtime.block_on(async {
            // This one access must be enough to transfer to the replacement
            // runtime even though the old task guard cannot run yet.
            let _ = nodes.get_live_nodes();
        });

        let restarted = second_runtime.block_on(async {
            tokio::time::timeout(Duration::from_secs(2), async {
                while request_count.load(Ordering::SeqCst) < 2 {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            })
            .await
        });
        release_blocker_tx.send(()).unwrap();
        restarted.expect("discovery was not handed off to the replacement runtime");

        server.join().unwrap();
    }

    #[test]
    fn stale_task_guard_does_not_clear_replacement_registration() {
        let nodes = LiveNodes::new(&test_config()).unwrap();
        let first_runtime = tokio::runtime::Runtime::new().unwrap();
        let second_runtime = tokio::runtime::Runtime::new().unwrap();
        let first = Arc::new(DiscoveryRuntime {
            id: first_runtime.handle().id(),
            handle: first_runtime.handle().clone(),
        });
        let second = Arc::new(DiscoveryRuntime {
            id: second_runtime.handle().id(),
            handle: second_runtime.handle().clone(),
        });

        nodes.discovery_runtime.store(Some(first.clone()));
        let stale_guard = DiscoveryTaskGuard {
            live_nodes: Arc::downgrade(&nodes),
            runtime: first,
        };
        nodes.discovery_runtime.store(Some(second.clone()));

        drop(stale_guard);

        let active = nodes.discovery_runtime.load_full().unwrap();
        assert!(Arc::ptr_eq(&active, &second));

        drop(DiscoveryTaskGuard {
            live_nodes: Arc::downgrade(&nodes),
            runtime: second,
        });
        assert!(nodes.discovery_runtime.load().is_none());
    }

    #[test]
    fn empty_seed_hosts_with_an_endpoint_disable_discovery() {
        let config = AlternatorConfig::builder()
            .behavior_version_latest()
            .endpoint_url("http://127.0.0.1:8000")
            .seed_hosts(Vec::<String>::new())
            .build();

        assert!(LiveNodes::try_new(&config).unwrap().is_none());
        assert!(crate::AlternatorClient::try_from_conf(config).is_ok());
    }

    #[test]
    fn direct_endpoint_validation_uses_the_endpoint_scheme() {
        for (endpoint, discovery_scheme, expected_scheme) in
            [("ftp://host", "http", "ftp"), ("ws://host", "https", "ws")]
        {
            let config = AlternatorConfig::builder()
                .behavior_version_latest()
                .endpoint_url(endpoint)
                .seed_hosts(Vec::<String>::new())
                .scheme(discovery_scheme)
                .build();

            assert!(matches!(
                LiveNodes::try_new(&config),
                Err(LiveNodesBuildError::InvalidScheme(scheme))
                    if scheme == expected_scheme
            ));
            assert!(crate::AlternatorClient::try_from_conf(config).is_err());
        }

        let direct_http = AlternatorConfig::builder()
            .behavior_version_latest()
            .endpoint_url("http://host")
            .seed_hosts(Vec::<String>::new())
            .scheme("ftp")
            .build();
        assert!(LiveNodes::try_new(&direct_http).unwrap().is_none());
        assert!(crate::AlternatorClient::try_from_conf(direct_http).is_ok());
    }

    #[test]
    fn custom_http_client_defers_direct_scheme_validation() {
        let config = AlternatorConfig::builder()
            .behavior_version_latest()
            .endpoint_url("custom://host")
            .seed_hosts(Vec::<String>::new())
            .http_client(aws_smithy_http_client::Builder::new().build_http())
            .build();

        assert!(LiveNodes::try_new(&config).unwrap().is_none());
        assert!(crate::AlternatorClient::try_from_conf(config).is_ok());
    }

    #[test]
    fn missing_seed_hosts_and_endpoint_are_rejected() {
        let config = AlternatorConfig::builder()
            .behavior_version_latest()
            .build();

        assert!(matches!(
            LiveNodes::try_new(&config),
            Err(LiveNodesBuildError::MissingRoutingTarget)
        ));
    }

    #[test]
    #[should_panic(expected = "invalid seed host")]
    fn malformed_seed_host_fails_closed_even_when_another_seed_is_valid() {
        let config = AlternatorConfig::builder()
            .behavior_version_latest()
            .scheme("http")
            .seed_hosts(["127.0.0.1", "127.0.0.1:invalid"])
            .build();

        let _ = LiveNodes::new(&config);
    }

    #[test]
    fn seed_hosts_reject_url_components_and_ports() {
        for seed_host in [
            "127.0.0.1@dynamodb.us-east-1.amazonaws.com",
            "example.com/path",
            "example.com?query",
            "example.com#fragment",
            "example.com:8000",
        ] {
            let config = AlternatorConfig::builder()
                .behavior_version_latest()
                .scheme("http")
                .seed_hosts([seed_host])
                .build();

            assert!(matches!(
                LiveNodes::try_new(&config),
                Err(LiveNodesBuildError::InvalidSeedHost { .. })
            ));
        }
    }

    #[test]
    fn discovery_rejects_unsupported_and_url_shaped_schemes() {
        for scheme in ["ftp", "https://dynamodb.us-east-1.amazonaws.com/x"] {
            let config = AlternatorConfig::builder()
                .behavior_version_latest()
                .scheme(scheme)
                .seed_hosts(["127.0.0.1"])
                .build();

            assert!(matches!(
                LiveNodes::try_new(&config),
                Err(LiveNodesBuildError::InvalidScheme(invalid)) if invalid == scheme
            ));
        }
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

    #[test]
    fn raw_ipv6_seed_host_is_bracketed() {
        let config = AlternatorConfig::builder()
            .behavior_version_latest()
            .scheme("http")
            .port(8000)
            .seed_hosts(["::1"])
            .build();
        let nodes = LiveNodes::new(&config).unwrap();

        assert_eq!(nodes.seed_urls[0].host_str(), Some("[::1]"));
        assert_eq!(nodes.seed_urls[0].to_string(), "http://[::1]:8000/");
    }

    #[tokio::test]
    async fn raw_ipv6_seed_discovers_raw_ipv6_node() {
        let (port, server) = start_localnodes_server_on("[::1]:0", "[::1]", r#"["::1"]"#).await;
        let config = AlternatorConfig::builder()
            .behavior_version_latest()
            .scheme("http")
            .port(port)
            .seed_hosts(["::1"])
            .build();
        let nodes = LiveNodes::new(&config).unwrap();

        nodes.update_live_nodes().await;

        server.await.unwrap();
        assert_eq!(
            nodes.live_nodes.load()[0].as_str(),
            format!("http://[::1]:{port}/")
        );
    }

    #[tokio::test]
    async fn dns_entrypoint_discovers_dns_node_records() {
        let (port, server) = start_localnodes_server(r#"["localhost","node-a.internal"]"#).await;
        let config = AlternatorConfig::builder()
            .behavior_version_latest()
            .scheme("http")
            .port(port)
            .seed_hosts(vec!["localhost".to_string()])
            .active_interval(std::time::Duration::from_millis(10))
            .idle_interval(std::time::Duration::from_secs(10))
            .build();
        let nodes = LiveNodes::new(&config).unwrap();

        nodes.update_live_nodes().await;

        server.await.unwrap();
        let snapshot = nodes.live_nodes.load();
        let hosts = snapshot
            .iter()
            .map(|url| url.host_str().unwrap().to_string())
            .collect::<Vec<_>>();
        assert_eq!(hosts, vec!["localhost", "node-a.internal"]);
    }

    #[tokio::test]
    async fn dns_entrypoint_applies_configured_port_to_dns_node_records() {
        let (port, server) =
            start_localnodes_server(r#"["node-a.internal:9000","node-b.internal"]"#).await;
        let config = AlternatorConfig::builder()
            .behavior_version_latest()
            .scheme("http")
            .port(port)
            .seed_hosts(vec!["localhost".to_string()])
            .active_interval(std::time::Duration::from_millis(10))
            .idle_interval(std::time::Duration::from_secs(10))
            .build();
        let nodes = LiveNodes::new(&config).unwrap();

        nodes.update_live_nodes().await;

        server.await.unwrap();
        let snapshot = nodes.live_nodes.load();
        let hosts_and_ports = snapshot
            .iter()
            .map(|url| (url.host_str().unwrap().to_string(), url.port()))
            .collect::<Vec<_>>();
        assert_eq!(
            hosts_and_ports,
            vec![
                ("node-a.internal".to_string(), Some(port)),
                ("node-b.internal".to_string(), Some(port)),
            ]
        );
    }

    #[tokio::test]
    async fn dns_entrypoint_supports_single_family_and_cross_family_fallback() {
        assert_dns_discovery("127.0.0.1:0", &[IpAddr::V4(Ipv4Addr::LOCALHOST)]).await;
        assert_dns_discovery("[::1]:0", &[IpAddr::V6(Ipv6Addr::LOCALHOST)]).await;
        assert_dns_discovery(
            "127.0.0.1:0",
            &[
                IpAddr::V6(Ipv6Addr::LOCALHOST),
                IpAddr::V4(Ipv4Addr::LOCALHOST),
            ],
        )
        .await;
        assert_dns_discovery(
            "[::1]:0",
            &[
                IpAddr::V4(Ipv4Addr::LOCALHOST),
                IpAddr::V6(Ipv6Addr::LOCALHOST),
            ],
        )
        .await;
        assert_dns_discovery(
            "127.0.0.1:0",
            &[
                IpAddr::V4(Ipv4Addr::new(127, 0, 0, 2)),
                IpAddr::V4(Ipv4Addr::new(127, 0, 0, 3)),
                IpAddr::V4(Ipv4Addr::LOCALHOST),
            ],
        )
        .await;
    }

    #[tokio::test]
    async fn all_unavailable_dns_records_return_without_clearing_seed() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        drop(listener);
        let mut nodes = dns_live_nodes(
            port,
            &[
                IpAddr::V6(Ipv6Addr::LOCALHOST),
                IpAddr::V4(Ipv4Addr::LOCALHOST),
            ],
        );
        Arc::get_mut(&mut nodes).unwrap().client = discovery_http_client_builder("http")
            .unwrap()
            .timeout(Duration::from_millis(200))
            .connect_timeout(Duration::from_millis(100))
            .resolve_to_addrs(
                "dual.test",
                &[
                    SocketAddr::new(IpAddr::V6(Ipv6Addr::LOCALHOST), port),
                    SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), port),
                ],
            )
            .build()
            .unwrap();

        tokio::time::timeout(Duration::from_secs(1), nodes.update_live_nodes())
            .await
            .expect("discovery must not hang when both address families are unavailable");

        assert_eq!(nodes.live_nodes.load()[0].host_str(), Some("dual.test"));
    }

    #[tokio::test]
    async fn refresh_recovers_through_original_raw_ipv6_seed() {
        let (port, server) = start_localnodes_server_on("[::1]:0", "[::1]", r#"["::1"]"#).await;
        let config = AlternatorConfig::builder()
            .behavior_version_latest()
            .scheme("http")
            .port(port)
            .seed_hosts(["::1"])
            .build();
        let nodes = LiveNodes::new(&config).unwrap();
        nodes.live_nodes.store(Arc::new(vec![Arc::new(
            Url::parse(&format!("http://127.0.0.1:{port}/")).unwrap(),
        )]));

        nodes.update_live_nodes().await;

        server.await.unwrap();
        assert_eq!(
            nodes.live_nodes.load()[0].as_str(),
            format!("http://[::1]:{port}/")
        );
    }

    #[tokio::test]
    async fn refresh_recovers_through_all_original_dns_seed_addresses() {
        let (port, server) =
            start_localnodes_server_on("127.0.0.1:0", "dual.test", r#"["recovered.internal"]"#)
                .await;
        let nodes = dns_live_nodes(
            port,
            &[
                IpAddr::V4(Ipv4Addr::new(127, 0, 0, 2)),
                IpAddr::V4(Ipv4Addr::LOCALHOST),
            ],
        );
        nodes.live_nodes.store(Arc::new(vec![Arc::new(
            Url::parse(&format!("http://127.0.0.3:{port}/")).unwrap(),
        )]));

        nodes.update_live_nodes().await;

        tokio::time::timeout(Duration::from_secs(1), server)
            .await
            .expect("recovery never reached a usable seed address")
            .unwrap();
        assert_eq!(
            nodes.live_nodes.load()[0].as_str(),
            format!("http://recovered.internal:{port}/")
        );
    }

    async fn assert_dns_discovery(bind_address: &str, resolved_ips: &[IpAddr]) {
        let (port, server) =
            start_localnodes_server_on(bind_address, "dual.test", r#"["dual.test"]"#).await;
        let nodes = dns_live_nodes(port, resolved_ips);

        nodes.update_live_nodes().await;

        tokio::time::timeout(Duration::from_secs(1), server)
            .await
            .expect("discovery never reached a usable seed address")
            .unwrap();
        assert_eq!(nodes.live_nodes.load()[0].host_str(), Some("dual.test"));
    }

    fn dns_live_nodes(port: u16, resolved_ips: &[IpAddr]) -> Arc<LiveNodes> {
        let config = AlternatorConfig::builder()
            .behavior_version_latest()
            .scheme("http")
            .port(port)
            .seed_hosts(["dual.test"])
            .build();
        let mut nodes = LiveNodes::new(&config).unwrap();
        let addresses = resolved_ips
            .iter()
            .map(|ip| SocketAddr::new(*ip, port))
            .collect::<Vec<_>>();
        Arc::get_mut(&mut nodes).unwrap().client = discovery_http_client_builder("http")
            .unwrap()
            .timeout(Duration::from_secs(1))
            .connect_timeout(Duration::from_millis(500))
            .resolve_to_addrs("dual.test", &addresses)
            .build()
            .unwrap();
        nodes
    }
}
