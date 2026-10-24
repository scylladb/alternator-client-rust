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
//! When [`ensure_discovery_started`] finds an active Tokio runtime, it launches a
//! background task which periodically calls [`update_live_nodes`] to request the known
//! nodes in a random order to get an updated list of live nodes. After a
//! successful refresh, the list is updated to nodes from the highest available
//! scope in the fallback chain provided by the user.
//! Discovery reuses a configured AWS SDK HTTP client so custom transport and
//! TLS settings also apply to `/localnodes`. Without one, it uses a basic
//! [`reqwest::Client`] with timeouts and native CA roots.
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
//! seed host from every datacenter that should receive traffic. Each non-empty
//! response is published as a union with the current snapshot so responsive
//! datacenters become routable without waiting for every stale candidate. The
//! completed union replaces that partial snapshot after the pass finishes.
//!
//! Once it successfully gets a non-empty response, it atomically updates the [`live_nodes`] list using [`ArcSwap`].
//!
//!  # Lifetime
//!
//! The background task holds a [`Weak`] reference to its [`LiveNodes`], so it
//! terminates on its own once the last owning [`Arc`] is dropped. [`Drop`]
//! additionally aborts the task to avoid waiting out the current sleep. The
//! client lifecycle owns all discovery tasks and
//! explicitly stops and joins them when the client is shut down.
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
//!    a fast path that checks the atomic stopped state and avoids the
//!    start/transfer mutex. If another runtime owns it, retain that owner while
//!    the discovery task acknowledges a liveness request.
//! 2. If no Tokio runtime is available on the current thread, return without spawning.
//!    The task will be started lazily on the first [`get_next_node_round_robin`] or [`get_live_nodes`] call,
//!    which is typically invoked from within the request pipeline and therefore from within a runtime.
//! 3. An atomic registration plus a mutex on the cold-start or handoff path
//!    ensures that exactly one caller starts the task. A
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
//! [`update_live_nodes`]: LiveNodes::update_live_nodes
//! [`get_next_node_round_robin`]: LiveNodes::get_next_node_round_robin
//! [`get_live_nodes`]: LiveNodes::get_live_nodes
//! [`live_nodes`]: LiveNodes::live_nodes
//! [`from_conf`]: crate::client::AlternatorClient::from_conf

use crate::routing_scope::RoutingScope;
use arc_swap::{ArcSwap, ArcSwapOption};
use aws_sdk_dynamodb::config::{SharedAsyncSleep, SharedHttpClient};
use aws_smithy_async::time::SharedTimeSource;
use aws_smithy_runtime::client::orchestrator::operation::Operation;
use aws_smithy_runtime_api::client::orchestrator::{HttpRequest, OrchestratorError};
use aws_smithy_types::timeout::TimeoutConfig;
use rand::seq::SliceRandom;
use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
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
            Self::MissingRoutingTarget => formatter
                .write_str("no Alternator routing target configured; set non-empty seed_hosts"),
            Self::InvalidSeedHost { seed_host, source } => {
                write!(formatter, "invalid seed host {seed_host:?}: {source}")
            }
            Self::InvalidScheme(scheme) => write!(
                formatter,
                "invalid Alternator transport scheme {scheme:?}: expected http or https, or a valid custom URI scheme for direct routing with a custom HTTP client"
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

fn build_default_discovery_http_client(
    scheme: &str,
) -> Result<(DiscoveryHttpClient, bool), LiveNodesBuildError> {
    let (builder, native_roots_usable) = discovery_http_client_builder_with_root_status(scheme)?;
    let client = builder
        .timeout(Duration::from_secs(5))
        .connect_timeout(Duration::from_secs(2))
        .build()
        .map_err(LiveNodesBuildError::HttpClient)?;
    Ok((DiscoveryHttpClient::Reqwest(client), native_roots_usable))
}

fn build_discovery_http_client(
    config: &crate::config::AlternatorConfig,
    scheme: &str,
) -> Result<(DiscoveryHttpClient, bool), LiveNodesBuildError> {
    match config.http_client() {
        Some(http_client) => Ok((
            DiscoveryHttpClient::Smithy {
                http_client,
                sleep_impl: config.sleep_impl(),
                time_source: config.time_source(),
            },
            false,
        )),
        None => build_default_discovery_http_client(scheme),
    }
}

#[derive(Debug)]
enum DiscoveryHttpClient {
    Reqwest(reqwest::Client),
    Smithy {
        http_client: SharedHttpClient,
        sleep_impl: Option<SharedAsyncSleep>,
        time_source: Option<SharedTimeSource>,
    },
}

impl DiscoveryHttpClient {
    async fn get_live_nodes(&self, url: &Url) -> Option<Vec<String>> {
        match self {
            Self::Reqwest(client) => client
                .get(url.clone())
                .send()
                .await
                .ok()?
                .json::<Vec<String>>()
                .await
                .ok(),
            Self::Smithy {
                http_client,
                sleep_impl,
                time_source,
            } => {
                let endpoint_url = url.origin().ascii_serialization();
                let mut builder = Operation::builder()
                    .service_name("alternator")
                    .operation_name("DiscoverLiveNodes")
                    .behavior_version(crate::config::ALTERNATOR_BEHAVIOR_VERSION())
                    .http_client(http_client.clone())
                    .endpoint_url(&endpoint_url)
                    .no_auth()
                    .no_retry()
                    .timeout_config(
                        TimeoutConfig::builder()
                            .connect_timeout(Duration::from_secs(2))
                            .operation_timeout(Duration::from_secs(5))
                            .build(),
                    )
                    .with_connection_poisoning();
                if let Some(sleep_impl) = sleep_impl {
                    builder = builder.sleep_impl(sleep_impl.clone());
                }
                if let Some(time_source) = time_source {
                    builder = builder.time_source(time_source.clone());
                }
                builder
                    .serializer(|url: Url| HttpRequest::get(url.as_str()).map_err(Into::into))
                    .deserializer::<_, std::convert::Infallible>(|response| {
                        let body = response.body().bytes().ok_or_else(|| {
                            OrchestratorError::other("discovery response body was not buffered")
                        })?;
                        serde_json::from_slice(body).map_err(OrchestratorError::other)
                    })
                    .build()
                    .invoke(url.clone())
                    .await
                    .ok()
            }
        }
    }
}

#[derive(Debug)]
pub(crate) struct LiveNodes {
    pub(crate) tasks: Arc<crate::discovery::DiscoveryTasks>,
    routing_scope: RoutingScope,
    active_interval: Duration,
    idle_interval: Duration,
    counter: Arc<AtomicUsize>,
    live_nodes: ArcSwap<Vec<Arc<Url>>>,
    refresh_state: Mutex<RefreshState>,
    seed_urls: Vec<Arc<Url>>,
    alternator_scheme: String,
    port: Option<u16>,
    client: DiscoveryHttpClient,
    native_roots_usable: bool,
    last_activity: Arc<Mutex<Instant>>,
    notify: Arc<tokio::sync::Notify>,
    discovery_task: Mutex<Option<tokio::task::AbortHandle>>,
    discovery_runtime: ArcSwapOption<DiscoveryRuntime>,
}

/// How long another runtime waits for the discovery task to acknowledge a
/// liveness request. No background polling is added while the client is idle.
const SHUTDOWN_PROBE_INTERVAL: Duration = Duration::from_millis(250);

#[derive(Debug)]
struct DiscoveryRuntime {
    id: tokio::runtime::Id,
    stopped: AtomicBool,
    handoff_required: AtomicBool,
    shutdown_probe: Mutex<ShutdownProbe>,
    probe_requested: tokio::sync::Notify,
}

#[derive(Debug, Default)]
struct ShutdownProbe {
    last_started: Option<Instant>,
    pending: bool,
    recheck_task: Option<tokio::task::AbortHandle>,
}

impl ShutdownProbe {
    fn is_due(&self, now: Instant) -> bool {
        self.last_started
            .is_none_or(|started| now.saturating_duration_since(started) >= SHUTDOWN_PROBE_INTERVAL)
    }
}

#[derive(Debug, Default)]
struct RefreshState {
    next_generation: u64,
    latest_published_generation: u64,
}

impl DiscoveryRuntime {
    fn new(id: tokio::runtime::Id) -> Self {
        Self {
            id,
            stopped: AtomicBool::new(false),
            handoff_required: AtomicBool::new(false),
            shutdown_probe: Mutex::new(ShutdownProbe::default()),
            probe_requested: tokio::sync::Notify::new(),
        }
    }

    fn should_handoff(&self) -> bool {
        self.should_handoff_at(Instant::now())
    }

    fn should_handoff_at(&self, now: Instant) -> bool {
        if self.stopped.load(Ordering::Acquire) {
            return true;
        }
        let mut probe = self
            .shutdown_probe
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        if probe.pending {
            if probe.is_due(now) {
                self.handoff_required.store(true, Ordering::Release);
            }
            return self.handoff_required.load(Ordering::Acquire);
        }
        if probe.is_due(now) {
            probe.last_started = Some(now);
            probe.pending = true;
            self.probe_requested.notify_one();
        }
        false
    }

    fn acknowledge_probe(&self) {
        let mut probe = self
            .shutdown_probe
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        probe.pending = false;
        self.handoff_required.store(false, Ordering::Release);
        if let Some(recheck) = probe.recheck_task.take() {
            recheck.abort();
        }
    }

    /// Keep the same I/O or sleep future pinned while acknowledging demand
    /// from another runtime. The discovery task itself proves that its owner
    /// is being driven; no probe is queued in either Tokio task pool.
    async fn with_probes<T>(&self, future: impl std::future::Future<Output = T>) -> T {
        tokio::pin!(future);
        loop {
            tokio::select! {
                biased;
                _ = self.probe_requested.notified() => self.acknowledge_probe(),
                result = &mut future => return result,
            }
        }
    }
}

struct DiscoveryTaskGuard {
    live_nodes: Weak<LiveNodes>,
    runtime: Arc<DiscoveryRuntime>,
}

impl Drop for DiscoveryTaskGuard {
    fn drop(&mut self) {
        self.runtime.stopped.store(true, Ordering::Release);
        if let Some(live_nodes) = self.live_nodes.upgrade() {
            // Clear only this task's registration, preserving any replacement.
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
    /// Returns [`None`] when discovery is turned off with
    /// [`AlternatorBuilder::without_discovery`](crate::config::AlternatorBuilder::without_discovery),
    /// in which case requests go straight to the first seed host.
    ///
    /// # Panics
    ///
    /// Panics if routing configuration is missing or invalid, or if the
    /// discovery HTTP client cannot be constructed. Invalid routing
    /// configuration fails closed instead of falling back to an unrelated SDK
    /// endpoint.
    #[cfg(test)]
    pub(crate) fn new(config: &crate::config::AlternatorConfig) -> Option<Arc<Self>> {
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
        let Some(first_seed) = seed_nodes.first() else {
            return Err(LiveNodesBuildError::MissingRoutingTarget);
        };

        if config.without_discovery() {
            // Requests go to the seed host itself, so it has to be a usable
            // target even though nothing is discovered through it. A custom
            // HTTP client may speak a scheme this driver does not know.
            if !is_valid_uri_scheme(&alternator_scheme)
                || (config.http_client().is_none()
                    && !alternator_scheme.eq_ignore_ascii_case("http")
                    && !alternator_scheme.eq_ignore_ascii_case("https"))
            {
                return Err(LiveNodesBuildError::InvalidScheme(alternator_scheme));
            }
            build_seed_url(&alternator_scheme, first_seed, port).map_err(|source| {
                LiveNodesBuildError::InvalidSeedHost {
                    seed_host: first_seed.clone(),
                    source,
                }
            })?;
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
        let (client, native_roots_usable) =
            build_discovery_http_client(config, seed_urls[0].scheme())?;

        Ok(Some(Arc::new(Self {
            tasks: Arc::default(),
            routing_scope,
            active_interval,
            idle_interval,
            counter: Arc::new(AtomicUsize::new(0)),
            live_nodes: ArcSwap::from_pointee(seed_urls.clone()),
            refresh_state: Mutex::new(RefreshState::default()),
            seed_urls,
            alternator_scheme,
            port,
            client,
            native_roots_usable,
            last_activity: Arc::new(Mutex::new(Instant::now())),
            notify: Arc::new(tokio::sync::Notify::new()),
            discovery_task: Mutex::new(None),
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
        let mut nodes = self.client.get_live_nodes(&url).await?;

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

    fn begin_refresh(&self) -> u64 {
        let mut state = self
            .refresh_state
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        state.next_generation = state
            .next_generation
            .checked_add(1)
            .expect("live-node refresh generation overflowed");
        state.next_generation
    }

    async fn discover_cluster_live_nodes(&self, generation: u64) -> Option<Vec<Arc<Url>>> {
        self.discover_cluster_live_nodes_from(generation, self.cluster_discovery_candidates())
            .await
    }

    fn publish_partial_cluster_live_nodes(&self, generation: u64, discovered: &[Arc<Url>]) {
        let mut state = self
            .refresh_state
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        if generation < state.latest_published_generation {
            return;
        }

        let current = self.live_nodes.load_full();
        let mut partial = Vec::with_capacity(current.len() + discovered.len());
        partial.extend(current.iter().cloned());
        partial.extend(discovered.iter().cloned());
        partial.sort_by(|a, b| a.as_str().cmp(b.as_str()));
        partial.dedup_by(|a, b| a.as_str() == b.as_str());

        if current.as_ref() != &partial {
            self.live_nodes.store(Arc::new(partial));
        }
        state.latest_published_generation = generation;
    }

    async fn discover_cluster_live_nodes_from(
        &self,
        generation: u64,
        candidates: Vec<Arc<Url>>,
    ) -> Option<Vec<Arc<Url>>> {
        let scope = RoutingScope::from_cluster();
        let mut new_nodes = Vec::new();
        let mut got_response = false;

        for node_addr in candidates {
            if node_is_in_list(&node_addr, &new_nodes) {
                continue;
            }

            if let Some(mut nodes) = self.fetch_live_nodes_for_scope(&scope, &node_addr).await {
                got_response = true;
                let response_was_nonempty = !nodes.is_empty();
                new_nodes.append(&mut nodes);
                new_nodes.sort_by(|a, b| a.as_str().cmp(b.as_str()));
                new_nodes.dedup_by(|a, b| a.as_str() == b.as_str());
                if response_was_nonempty {
                    self.publish_partial_cluster_live_nodes(generation, &new_nodes);
                }
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
    /// but takes ownership when its discovery task exits or cannot acknowledge
    /// a liveness request within the bounded interval.
    pub(crate) fn ensure_discovery_started(self: &Arc<Self>) {
        if self.tasks.is_stopped() {
            return;
        }
        let Ok(handle) = Handle::try_current() else {
            return;
        };
        let runtime_id = handle.id();
        // Requests on the owning runtime never take the start/transfer mutex.
        // A caller on another runtime transfers only after the owner exits or
        // leaves a liveness request pending beyond the bounded interval.
        if self
            .discovery_runtime
            .load()
            .as_ref()
            .is_some_and(|active| self.retain_discovery_owner(active, &handle))
        {
            return;
        }

        let mut discovery_task = self
            .discovery_task
            .lock()
            .unwrap_or_else(|err| err.into_inner());
        // Another caller may have completed the cold start or transfer while
        // this caller waited for the mutex.
        let active_runtime = self.discovery_runtime.load_full();
        if active_runtime
            .as_ref()
            .is_some_and(|active| self.retain_discovery_owner(active, &handle))
        {
            return;
        }

        if let Some(old_task) = discovery_task.take() {
            old_task.abort();
        }
        let runtime = Arc::new(DiscoveryRuntime::new(runtime_id));
        self.discovery_runtime.store(Some(runtime.clone()));
        let weak_self = Arc::downgrade(self);
        let notify = self.notify.clone();
        let task_guard = DiscoveryTaskGuard {
            live_nodes: weak_self.clone(),
            runtime: runtime.clone(),
        };

        self.mark_activity();
        let task = self.tasks.spawn(&handle, async move {
            let _task_guard = task_guard;
            loop {
                let (idle_interval, active_interval, is_idle) = {
                    let Some(strong_self) = weak_self.upgrade() else {
                        break;
                    };

                    runtime.with_probes(strong_self.update_live_nodes()).await;

                    let last = *strong_self.last_activity.lock().unwrap();
                    (
                        strong_self.idle_interval,
                        strong_self.active_interval,
                        last.elapsed() >= strong_self.idle_interval,
                    )
                };

                let interval = if is_idle {
                    idle_interval
                } else {
                    active_interval
                };
                runtime
                    .with_probes(async {
                        tokio::select! {
                            _ = tokio::time::sleep(interval) => {}
                            _ = notify.notified() => {}
                        }
                    })
                    .await;
            }
        });
        *discovery_task = task;
    }

    fn retain_discovery_owner(
        self: &Arc<Self>,
        owner: &Arc<DiscoveryRuntime>,
        handle: &Handle,
    ) -> bool {
        if owner.id == handle.id() {
            return true;
        }
        if owner.should_handoff() {
            return false;
        }
        let mut probe = owner
            .shutdown_probe
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        if !probe.pending
            || probe
                .recheck_task
                .as_ref()
                .is_some_and(|task| !task.is_finished())
        {
            return true;
        }
        let deadline = probe.last_started.unwrap() + SHUTDOWN_PROBE_INTERVAL;
        let weak_self = Arc::downgrade(self);
        let owner = owner.clone();
        // One access must arrange a later check when a stopped runtime's
        // blocked worker cannot drop its discovery guard. Reuse the client
        // task registry so shutdown cancels and joins this bounded follow-up.
        probe.recheck_task = self.tasks.spawn(handle, async move {
            tokio::time::sleep_until(tokio::time::Instant::from_std(deadline)).await;
            let pending = owner
                .shutdown_probe
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .pending;
            if pending && let Some(nodes) = weak_self.upgrade() {
                let active = nodes.discovery_runtime.load_full();
                if active
                    .as_ref()
                    .is_none_or(|active| Arc::ptr_eq(active, &owner))
                {
                    nodes.ensure_discovery_started();
                }
            }
        });
        true
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
    pub(crate) fn get_live_nodes(self: &Arc<Self>) -> Vec<Arc<Url>> {
        self.ensure_discovery_started();
        self.mark_activity();
        self.live_nodes.load().as_ref().clone()
    }

    /// Returns the first live node not in `used_nodes` starting with the next node in round-robin order.
    /// Used by [`crate::QueryPlan`] round-robin strategy.
    pub(crate) fn get_next_node_round_robin(
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

        // Checking an exhausted query plan must not advance the shared
        // round-robin position. The caller may clear `used_nodes` and retry,
        // and that retry should consume the next position itself.
        if live_nodes.iter().all(|node| used_nodes.contains(node)) {
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

    async fn update_live_nodes(&self) {
        let generation = self.begin_refresh();
        let mut scope = &self.routing_scope;
        // Live nodes in a random order.
        let mut nodes = self.live_nodes.load().as_ref().clone();
        nodes.shuffle(&mut rand::rng());
        let mut candidates: VecDeque<Arc<Url>> = nodes.into();
        let mut using_seeds = false;

        while let Some(node_addr) = candidates.pop_front() {
            if scope.is_cluster() {
                let Some(new_nodes) = self.discover_cluster_live_nodes(generation).await else {
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

                self.publish_live_nodes(generation, new_nodes);
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

            self.publish_live_nodes(generation, new_nodes);
            return;
        }
    }

    fn publish_live_nodes(&self, generation: u64, new_nodes: Vec<Arc<Url>>) {
        let mut state = self
            .refresh_state
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        if generation < state.latest_published_generation {
            return;
        }
        if **self.live_nodes.load() != new_nodes {
            self.live_nodes.store(Arc::new(new_nodes));
        }
        state.latest_published_generation = generation;
    }
}

pub(crate) fn build_seed_url(
    scheme: &str,
    addr: &str,
    port: Option<u16>,
) -> Result<Url, url::ParseError> {
    let unbracketed = addr
        .strip_prefix('[')
        .and_then(|addr| addr.strip_suffix(']'))
        .unwrap_or(addr);
    if unbracketed.parse::<std::net::Ipv6Addr>().is_err() {
        url::Host::parse(addr)?;
    }
    build_node_url(scheme, unbracketed, port)
}

/// Whether `scheme` is a syntactically valid bare URI scheme.
///
/// Validate before calling [`Url::parse`]. WHATWG URL parsing strips ASCII
/// tabs, newlines, carriage returns, and surrounding control characters, so
/// relying on it alone could silently turn malformed input into another valid
/// transport scheme.
fn is_valid_uri_scheme(scheme: &str) -> bool {
    let mut bytes = scheme.bytes();

    matches!(bytes.next(), Some(first) if first.is_ascii_alphabetic())
        && bytes.all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'+' | b'-' | b'.'))
}

fn build_node_url(scheme: &str, addr: &str, port: Option<u16>) -> Result<Url, url::ParseError> {
    if !is_valid_uri_scheme(scheme) {
        return Err(url::ParseError::RelativeUrlWithoutBase);
    }
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
        if let Ok(mut state) = self.discovery_task.lock()
            && let Some(task) = state.take()
        {
            task.abort();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::AlternatorConfig;
    use aws_smithy_runtime_api::client::http::{
        HttpClient, HttpConnector, HttpConnectorFuture, HttpConnectorSettings, SharedHttpConnector,
    };
    use aws_smithy_runtime_api::client::orchestrator::HttpResponse;
    use aws_smithy_runtime_api::client::runtime_components::RuntimeComponents;
    use aws_smithy_runtime_api::http::StatusCode;
    use aws_smithy_types::body::SdkBody;
    use futures_util::FutureExt;
    use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

    fn discovery_is_running(nodes: &LiveNodes) -> bool {
        nodes.discovery_runtime.load().is_some()
    }

    fn test_config() -> AlternatorConfig {
        AlternatorConfig::builder()
            .seed_hosts(["127.0.0.1"])
            .port(1)
            .build()
    }

    #[derive(Clone, Debug)]
    struct DiscoveryResponseHttpClient(Arc<Mutex<Vec<String>>>);

    impl HttpClient for DiscoveryResponseHttpClient {
        fn http_connector(
            &self,
            _: &HttpConnectorSettings,
            _: &RuntimeComponents,
        ) -> SharedHttpConnector {
            SharedHttpConnector::new(self.clone())
        }
    }

    impl HttpConnector for DiscoveryResponseHttpClient {
        fn call(&self, request: HttpRequest) -> HttpConnectorFuture {
            assert_eq!(request.method(), "GET");
            assert!(request.uri().contains("/localnodes"));
            self.0
                .lock()
                .unwrap()
                .push(format!("{} {}", request.method(), request.uri()));
            HttpConnectorFuture::ready(Ok(HttpResponse::new(
                StatusCode::try_from(200).unwrap(),
                SdkBody::from(r#"["127.0.0.2"]"#),
            )))
        }
    }

    #[derive(Clone, Debug)]
    struct CoordinatedDiscoveryHttpClient {
        stalled: Arc<tokio::sync::Notify>,
        release: Arc<tokio::sync::Notify>,
    }

    impl HttpClient for CoordinatedDiscoveryHttpClient {
        fn http_connector(
            &self,
            _: &HttpConnectorSettings,
            _: &RuntimeComponents,
        ) -> SharedHttpConnector {
            SharedHttpConnector::new(self.clone())
        }
    }

    impl HttpConnector for CoordinatedDiscoveryHttpClient {
        fn call(&self, request: HttpRequest) -> HttpConnectorFuture {
            assert_eq!(request.method(), "GET");

            match request.uri() {
                "http://responsive.test/localnodes" => {
                    HttpConnectorFuture::ready(Ok(HttpResponse::new(
                        StatusCode::try_from(200).unwrap(),
                        SdkBody::from(r#"["healthy.test"]"#),
                    )))
                }
                "http://stale.test/localnodes" => {
                    let stalled = self.stalled.clone();
                    let release = self.release.clone();
                    HttpConnectorFuture::new(async move {
                        stalled.notify_one();
                        release.notified().await;
                        Ok(HttpResponse::new(
                            StatusCode::try_from(200).unwrap(),
                            SdkBody::from("[]"),
                        ))
                    })
                }
                uri => panic!("unexpected discovery request URI: {uri}"),
            }
        }
    }

    async fn start_localnodes_server(body: &'static str) -> (u16, tokio::task::JoinHandle<()>) {
        start_localnodes_server_on("127.0.0.1:0", "localhost", body).await
    }

    #[tokio::test]
    async fn custom_http_client_is_used_for_discovery() {
        let requests = Arc::new(Mutex::new(Vec::new()));
        let config = AlternatorConfig::builder()
            .seed_hosts(["127.0.0.1"])
            .port(8000)
            .http_client(DiscoveryResponseHttpClient(requests.clone()))
            .build();
        let nodes = LiveNodes::new(&config).unwrap();

        nodes.update_live_nodes().await;

        assert_eq!(
            requests.lock().unwrap().as_slice(),
            ["GET http://127.0.0.1:8000/localnodes"]
        );
        assert_eq!(
            nodes.live_nodes.load()[0].as_str(),
            "http://127.0.0.2:8000/"
        );
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
            .seed_hosts(["127.0.0.1"])
            .port(port)
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
        let original = nodes.discovery_runtime.load_full().unwrap();
        second_runtime.block_on(async {
            for _ in 0..10 {
                original.shutdown_probe.lock().unwrap().last_started = None;
                nodes.ensure_discovery_started();
                tokio::time::timeout(Duration::from_secs(1), async {
                    while original.shutdown_probe.lock().unwrap().pending {
                        tokio::task::yield_now().await;
                    }
                })
                .await
                .expect("healthy owner must acknowledge without a blocking-pool probe");
                assert!(Arc::ptr_eq(
                    &nodes.discovery_runtime.load_full().unwrap(),
                    &original
                ));
                tokio::time::timeout(Duration::from_secs(1), async {
                    while nodes.tasks.active_task_count() != 1 {
                        tokio::task::yield_now().await;
                    }
                })
                .await
                .expect("acknowledgement must cancel the observer's follow-up");
            }
            tokio::time::timeout(Duration::from_secs(1), nodes.tasks.shutdown())
                .await
                .expect("shutdown must join discovery on its healthy owner");
        });
    }

    #[test]
    fn shutdown_probe_interval_is_rate_limited() {
        let started = Instant::now();
        let probe = ShutdownProbe {
            last_started: Some(started),
            ..Default::default()
        };

        assert!(!probe.is_due(started));
        assert!(!probe.is_due(started + SHUTDOWN_PROBE_INTERVAL - Duration::from_nanos(1)));
        assert!(probe.is_due(started + SHUTDOWN_PROBE_INTERVAL));
    }

    #[derive(Clone, Debug)]
    struct HeldFirstTopology {
        requests: Arc<AtomicUsize>,
        entered: Arc<tokio::sync::Semaphore>,
        dropped: Arc<tokio::sync::Semaphore>,
        following: DiscoveryResponseHttpClient,
    }

    struct DropPermit(Arc<tokio::sync::Semaphore>);
    impl Drop for DropPermit {
        fn drop(&mut self) {
            self.0.add_permits(1);
        }
    }

    impl HttpClient for HeldFirstTopology {
        fn http_connector(
            &self,
            _: &HttpConnectorSettings,
            _: &RuntimeComponents,
        ) -> SharedHttpConnector {
            SharedHttpConnector::new(self.clone())
        }
    }

    impl HttpConnector for HeldFirstTopology {
        fn call(&self, request: HttpRequest) -> HttpConnectorFuture {
            if self.requests.fetch_add(1, Ordering::SeqCst) == 0 {
                let entered = self.entered.clone();
                let dropped = self.dropped.clone();
                return HttpConnectorFuture::new(async move {
                    let _guard = DropPermit(dropped);
                    entered.add_permits(1);
                    std::future::pending().await
                });
            }
            self.following.call(request)
        }
    }

    #[test]
    fn demand_probes_preserve_io_and_stalled_owner_cannot_publish_after_handoff() {
        struct ReleaseOnDrop(Option<std::sync::mpsc::Sender<()>>);
        impl Drop for ReleaseOnDrop {
            fn drop(&mut self) {
                if let Some(release) = self.0.take() {
                    let _ = release.send(());
                }
            }
        }
        let owner = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()
            .unwrap();
        let observer = tokio::runtime::Runtime::new().unwrap();
        let http = HeldFirstTopology {
            requests: Arc::default(),
            entered: Arc::new(tokio::sync::Semaphore::new(0)),
            dropped: Arc::new(tokio::sync::Semaphore::new(0)),
            following: DiscoveryResponseHttpClient(Arc::default()),
        };
        let config = AlternatorConfig::builder()
            .seed_hosts(["127.0.0.1"])
            .port(8000)
            .http_client(http.clone())
            .active_interval(Duration::from_secs(3600))
            .idle_interval(Duration::from_secs(3600))
            .build();
        let nodes = LiveNodes::new(&config).unwrap();
        owner.block_on(async {
            nodes.ensure_discovery_started();
        });
        let original = nodes.discovery_runtime.load_full().unwrap();
        observer.block_on(async {
            tokio::time::timeout(Duration::from_secs(1), http.entered.acquire())
                .await
                .unwrap()
                .unwrap()
                .forget();
            for _ in 0..2 {
                tokio::time::sleep(SHUTDOWN_PROBE_INTERVAL).await;
                nodes.ensure_discovery_started();
                tokio::time::timeout(Duration::from_secs(1), async {
                    while original.shutdown_probe.lock().unwrap().pending {
                        tokio::task::yield_now().await;
                    }
                })
                .await
                .unwrap();
            }
            assert_eq!(http.requests.load(Ordering::SeqCst), 1);
            assert_eq!(
                http.dropped.available_permits(),
                0,
                "liveness demand must not restart pending SDK I/O"
            );
            // Let the actual rate limit expire before stopping the owner.
            tokio::time::sleep(SHUTDOWN_PROBE_INTERVAL).await;
        });
        let (started, started_rx) = std::sync::mpsc::sync_channel(0);
        let (release, release_rx) = std::sync::mpsc::channel();
        let release = ReleaseOnDrop(Some(release));
        let worker = owner.spawn(async move {
            started.send(()).unwrap();
            let _ = release_rx.recv();
        });
        started_rx.recv_timeout(Duration::from_secs(1)).unwrap();
        owner.shutdown_background();
        // A stopped runtime whose async worker is blocked cannot acknowledge.
        // Its existing I/O is still owned and must be dropped once it resumes.
        observer.block_on(async {
            let cached = nodes.get_live_nodes();
            assert_eq!(cached[0].host_str(), Some("127.0.0.1"));
            tokio::time::timeout(Duration::from_secs(1), async {
                while nodes.live_nodes.load()[0].host_str() != Some("127.0.0.2") {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .expect("one observing access must restart discovery without later demand");
        });
        let replacement = nodes.discovery_runtime.load_full().unwrap();
        assert_eq!(replacement.id, observer.handle().id());
        drop(release);
        observer.block_on(async {
            let _ = tokio::time::timeout(Duration::from_secs(1), worker)
                .await
                .unwrap();
            tokio::time::timeout(Duration::from_secs(1), http.dropped.acquire())
                .await
                .unwrap()
                .unwrap()
                .forget();
            tokio::time::timeout(Duration::from_secs(1), async {
                while !original.stopped.load(Ordering::Acquire) {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .unwrap();
            assert!(Arc::ptr_eq(
                &nodes.discovery_runtime.load_full().unwrap(),
                &replacement
            ));
            assert_eq!(nodes.live_nodes.load()[0].host_str(), Some("127.0.0.2"));
            assert_eq!(
                http.requests.load(Ordering::SeqCst),
                2,
                "cancelled old refresh must not resume discovery or publish"
            );
            tokio::time::timeout(Duration::from_secs(1), nodes.tasks.shutdown())
                .await
                .unwrap();
        });
        assert_eq!(nodes.tasks.active_task_count(), 0);
    }

    #[test]
    fn shutdown_probe_handoff_is_latched_until_owner_acknowledges() {
        let runtime = tokio::runtime::Runtime::new().unwrap();
        let owner = DiscoveryRuntime::new(runtime.handle().id());
        let started = Instant::now();
        assert!(!owner.should_handoff_at(started));
        assert!(owner.should_handoff_at(started + SHUTDOWN_PROBE_INTERVAL));
        assert!(owner.should_handoff_at(started));
        owner.acknowledge_probe();
        assert!(!owner.should_handoff_at(started));
        owner.stopped.store(true, Ordering::Release);
        assert!(owner.should_handoff_at(started));
    }

    #[test]
    fn parked_owner_hands_off_and_shutdown_joins_when_owner_is_driven_again() {
        let owner = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let observer = tokio::runtime::Runtime::new().unwrap();
        let nodes = LiveNodes::new(&test_config()).unwrap();
        owner.block_on(async {
            nodes.ensure_discovery_started();
            tokio::task::yield_now().await;
        });
        let registration = nodes.discovery_runtime.load_full().unwrap();
        let started = Instant::now();
        assert!(!registration.should_handoff_at(started));
        assert!(registration.should_handoff_at(started + SHUTDOWN_PROBE_INTERVAL));
        observer.block_on(async {
            nodes.ensure_discovery_started();
            assert_eq!(
                nodes.discovery_runtime.load_full().unwrap().id,
                observer.handle().id()
            );
            // Tokio cannot destroy an aborted task on a current-thread runtime
            // which is not being driven. Its join remains owned and pending.
            assert!(nodes.tasks.shutdown().now_or_never().is_none());
        });
        owner.block_on(tokio::task::yield_now());
        observer.block_on(async {
            tokio::time::timeout(Duration::from_secs(1), nodes.tasks.shutdown())
                .await
                .unwrap();
        });
        assert_eq!(nodes.tasks.active_task_count(), 0);
    }

    #[test]
    fn repeated_handoffs_do_not_accumulate_discovery_tasks() {
        let first = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let second = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let nodes = LiveNodes::new(&test_config()).unwrap();
        first.block_on(async {
            nodes.ensure_discovery_started();
            tokio::task::yield_now().await;
        });
        for index in 0..8 {
            let previous = nodes.discovery_runtime.load_full().unwrap();
            let (next, old) = if index % 2 == 0 {
                (&second, &first)
            } else {
                (&first, &second)
            };
            next.block_on(async {
                std::thread::scope(|scope| {
                    for _ in 0..8 {
                        scope.spawn(|| {
                            let _entered = next.enter();
                            nodes.ensure_discovery_started();
                        });
                    }
                });
                assert_eq!(nodes.tasks.active_task_count(), 2);
                tokio::time::timeout(Duration::from_secs(1), async {
                    while nodes.discovery_runtime.load_full().unwrap().id != next.handle().id() {
                        tokio::task::yield_now().await;
                    }
                })
                .await
                .expect("one bounded follow-up must transfer ownership without more access");
            });
            old.block_on(tokio::task::yield_now());
            assert_eq!(
                nodes.discovery_runtime.load_full().unwrap().id,
                next.handle().id()
            );
            assert!(previous.stopped.load(Ordering::Acquire));
            assert_eq!(nodes.tasks.active_task_count(), 1);
            // Completed tasks leave the tracker without a later spawn.
            assert!(nodes.tasks.registered_task_count() <= 3);
        }
        let last_owner = nodes.discovery_runtime.load_full().unwrap();
        second.block_on(async {
            for _ in 0..8 {
                nodes.ensure_discovery_started();
            }
            assert_eq!(nodes.tasks.active_task_count(), 2);
            assert!(nodes.tasks.shutdown().now_or_never().is_none());
            tokio::time::sleep(SHUTDOWN_PROBE_INTERVAL * 2).await;
            assert!(Arc::ptr_eq(
                &nodes.discovery_runtime.load_full().unwrap(),
                &last_owner
            ));
            assert_eq!(nodes.tasks.active_task_count(), 1);
        });
        first.block_on(tokio::task::yield_now());
        first.block_on(async {
            tokio::time::timeout(Duration::from_secs(1), nodes.tasks.shutdown())
                .await
                .unwrap();
        });
        assert_eq!(nodes.tasks.active_task_count(), 0);
    }

    #[test]
    fn stale_task_guard_does_not_clear_replacement_registration() {
        let nodes = LiveNodes::new(&test_config()).unwrap();
        let first_runtime = tokio::runtime::Runtime::new().unwrap();
        let second_runtime = tokio::runtime::Runtime::new().unwrap();
        let first = Arc::new(DiscoveryRuntime::new(first_runtime.handle().id()));
        let second = Arc::new(DiscoveryRuntime::new(second_runtime.handle().id()));

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
    fn without_discovery_disables_discovery() {
        let config = AlternatorConfig::builder()
            .seed_hosts(["127.0.0.1"])
            .port(8000)
            .without_discovery()
            .build();

        assert!(LiveNodes::try_new(&config).unwrap().is_none());
        assert!(crate::AlternatorClient::try_from_conf(config).is_ok());
    }

    #[test]
    fn direct_routing_validates_the_configured_scheme() {
        for unsupported_scheme in ["ftp", "ws", "https://dynamodb.us-east-1.amazonaws.com/x"] {
            let config = AlternatorConfig::builder()
                .seed_hosts(["host"])
                .without_discovery()
                .scheme(unsupported_scheme)
                .build();

            assert!(matches!(
                LiveNodes::try_new(&config),
                Err(LiveNodesBuildError::InvalidScheme(scheme))
                    if scheme == unsupported_scheme
            ));
            assert!(crate::AlternatorClient::try_from_conf(config).is_err());
        }

        let direct_http = AlternatorConfig::builder()
            .seed_hosts(["host"])
            .without_discovery()
            .scheme("https")
            .build();
        assert!(LiveNodes::try_new(&direct_http).unwrap().is_none());
        assert!(crate::AlternatorClient::try_from_conf(direct_http).is_ok());
    }

    #[test]
    fn without_discovery_needs_a_seed_host() {
        for config in [
            AlternatorConfig::builder().without_discovery().build(),
            AlternatorConfig::builder()
                .seed_hosts(Vec::<String>::new())
                .without_discovery()
                .build(),
        ] {
            assert!(matches!(
                LiveNodes::try_new(&config),
                Err(LiveNodesBuildError::MissingRoutingTarget)
            ));
        }
    }

    #[test]
    fn custom_http_client_supports_valid_direct_schemes() {
        for (configured, stored, endpoint) in [
            ("custom", "custom", "custom://host"),
            ("CUSTOM", "CUSTOM", "custom://host"),
            ("a+b.c-1", "a+b.c-1", "a+b.c-1://host"),
            ("custom:", "custom", "custom://host"),
            ("custom://", "custom", "custom://host"),
        ] {
            let config = AlternatorConfig::builder()
                .scheme(configured)
                .seed_hosts(["host"])
                .without_discovery()
                .http_client(aws_smithy_http_client::Builder::new().build_http())
                .build();

            assert_eq!(config.scheme().as_deref(), Some(stored));
            assert_eq!(config.endpoint_url().as_deref(), Some(endpoint));
            assert!(LiveNodes::try_new(&config).unwrap().is_none());
            assert!(crate::AlternatorClient::try_from_conf(config).is_ok());
        }
    }

    #[test]
    fn custom_http_client_does_not_allow_malformed_direct_schemes() {
        for malformed in [
            "\nhttp",
            "ht\ntp",
            "\0http",
            "http\t",
            "1custom",
            "+custom",
            "éhttp",
            "http::",
            "http:/",
            "http///",
            "http:://",
            "https://dynamodb.us-east-1.amazonaws.com/x",
        ] {
            let config = AlternatorConfig::builder()
                .scheme(malformed)
                .seed_hosts(["host"])
                .without_discovery()
                .http_client(aws_smithy_http_client::Builder::new().build_http())
                .build();

            assert_eq!(config.endpoint_url(), None, "accepted {malformed:?}");
            assert!(matches!(
                LiveNodes::try_new(&config),
                Err(LiveNodesBuildError::InvalidScheme(_))
            ));
            assert!(crate::AlternatorClient::try_from_conf(config).is_err());
        }
    }

    #[test]
    fn discovery_rejects_custom_schemes_even_with_a_custom_http_client() {
        let config = AlternatorConfig::builder()
            .scheme("custom")
            .seed_hosts(["host"])
            .http_client(aws_smithy_http_client::Builder::new().build_http())
            .build();

        assert!(matches!(
            LiveNodes::try_new(&config),
            Err(LiveNodesBuildError::InvalidScheme(scheme)) if scheme == "custom"
        ));
        assert!(crate::AlternatorClient::try_from_conf(config).is_err());
    }

    #[test]
    fn missing_seed_hosts_are_rejected() {
        let config = AlternatorConfig::builder().build();

        assert!(matches!(
            LiveNodes::try_new(&config),
            Err(LiveNodesBuildError::MissingRoutingTarget)
        ));
    }

    #[test]
    #[should_panic(expected = "invalid seed host")]
    fn malformed_seed_host_fails_closed_even_when_another_seed_is_valid() {
        let config = AlternatorConfig::builder()
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
            .scheme("http")
            .port(8000)
            .seed_hosts(["::1"])
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
    async fn cluster_discovery_publishes_safe_partial_union_before_stalled_candidate_finishes() {
        let stalled = Arc::new(tokio::sync::Notify::new());
        let release = Arc::new(tokio::sync::Notify::new());

        let config = AlternatorConfig::builder()
            .scheme("http")
            .seed_hosts(["responsive.test"])
            .http_client(CoordinatedDiscoveryHttpClient {
                stalled: stalled.clone(),
                release: release.clone(),
            })
            .build();
        let nodes = LiveNodes::new(&config).unwrap();
        let stale = Arc::new(Url::parse("http://stale.test/").unwrap());
        let healthy = Arc::new(Url::parse("http://healthy.test/").unwrap());
        nodes.live_nodes.store(Arc::new(vec![stale.clone()]));
        let candidates = vec![nodes.seed_urls[0].clone(), stale.clone()];
        let generation = nodes.begin_refresh();
        let discovery_nodes = nodes.clone();
        let discovery = tokio::spawn(async move {
            discovery_nodes
                .discover_cluster_live_nodes_from(generation, candidates)
                .await
        });

        tokio::time::timeout(Duration::from_secs(1), stalled.notified())
            .await
            .expect("cluster discovery never reached the stale candidate");
        assert!(
            !discovery.is_finished(),
            "cluster discovery unexpectedly finished while a candidate was stalled"
        );
        assert_eq!(
            nodes.live_nodes.load().as_ref(),
            &[healthy.clone(), stale.clone()],
            "partial publication must add newly validated nodes without dropping last-known-good nodes"
        );

        release.notify_one();
        let discovered = tokio::time::timeout(Duration::from_secs(1), discovery)
            .await
            .expect("cluster discovery did not finish after releasing the stale candidate")
            .unwrap()
            .unwrap();
        assert_eq!(discovered, vec![healthy]);
    }

    #[tokio::test]
    async fn older_concurrent_refresh_does_not_overwrite_newer_result() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let config = AlternatorConfig::builder()
            .scheme("http")
            .port(port)
            .seed_hosts(["127.0.0.1"])
            .build();
        let nodes = LiveNodes::new(&config).unwrap();

        let first_nodes = nodes.clone();
        let first_update = tokio::spawn(async move { first_nodes.update_live_nodes().await });
        let (mut first_request, _) =
            tokio::time::timeout(Duration::from_secs(1), listener.accept())
                .await
                .expect("first refresh did not connect")
                .unwrap();
        let mut buffer = [0; 1024];
        assert!(first_request.read(&mut buffer).await.unwrap() > 0);

        let second_nodes = nodes.clone();
        let second_update = tokio::spawn(async move { second_nodes.update_live_nodes().await });
        let (mut second_request, _) =
            tokio::time::timeout(Duration::from_secs(1), listener.accept())
                .await
                .expect("newer refresh did not overlap the stalled pass")
                .unwrap();
        assert!(second_request.read(&mut buffer).await.unwrap() > 0);

        let newer_body = r#"["127.0.0.2"]"#;
        let newer_response = format!(
            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
            newer_body.len(),
            newer_body
        );
        second_request
            .write_all(newer_response.as_bytes())
            .await
            .unwrap();
        second_update.await.unwrap();

        let older_body = r#"["127.0.0.3"]"#;
        let older_response = format!(
            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
            older_body.len(),
            older_body
        );
        first_request
            .write_all(older_response.as_bytes())
            .await
            .unwrap();
        first_update.await.unwrap();

        let snapshot = nodes.live_nodes.load();
        let hosts = snapshot
            .iter()
            .map(|node| node.host_str().unwrap())
            .collect::<Vec<_>>();
        assert_eq!(
            hosts,
            ["127.0.0.2"],
            "older refresh must not alter the newer completed topology"
        );
    }

    #[test]
    fn newer_refresh_without_a_result_does_not_suppress_older_result() {
        let nodes = LiveNodes::new(&test_config()).unwrap();
        let older_generation = nodes.begin_refresh();
        let _newer_generation = nodes.begin_refresh();
        let older_result = vec![Arc::new(Url::parse("http://127.0.0.2:1/").unwrap())];

        nodes.publish_live_nodes(older_generation, older_result.clone());

        assert_eq!(nodes.live_nodes.load().as_ref(), &older_result);
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
        Arc::get_mut(&mut nodes).unwrap().client = DiscoveryHttpClient::Reqwest(
            discovery_http_client_builder("http")
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
                .unwrap(),
        );

        tokio::time::timeout(Duration::from_secs(1), nodes.update_live_nodes())
            .await
            .expect("discovery must not hang when both address families are unavailable");

        assert_eq!(nodes.live_nodes.load()[0].host_str(), Some("dual.test"));
    }

    #[tokio::test]
    async fn refresh_recovers_through_original_raw_ipv6_seed() {
        let (port, server) = start_localnodes_server_on("[::1]:0", "[::1]", r#"["::1"]"#).await;
        let config = AlternatorConfig::builder()
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
            .scheme("http")
            .port(port)
            .seed_hosts(["dual.test"])
            .build();
        let mut nodes = LiveNodes::new(&config).unwrap();
        let addresses = resolved_ips
            .iter()
            .map(|ip| SocketAddr::new(*ip, port))
            .collect::<Vec<_>>();
        Arc::get_mut(&mut nodes).unwrap().client = DiscoveryHttpClient::Reqwest(
            discovery_http_client_builder("http")
                .unwrap()
                .timeout(Duration::from_secs(1))
                .connect_timeout(Duration::from_millis(500))
                .resolve_to_addrs("dual.test", &addresses)
                .build()
                .unwrap(),
        );
        nodes
    }
}
