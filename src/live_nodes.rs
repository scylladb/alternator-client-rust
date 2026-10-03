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
use futures_util::FutureExt;
use rand::seq::SliceRandom;
use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, Weak};
use std::time::{Duration, Instant};
use tokio::runtime::Handle;
use url::Url;

const DEFAULT_ACTIVE_REFRESH_INTERVAL: Duration = Duration::from_secs(1);
const DEFAULT_IDLE_REFRESH_INTERVAL: Duration = Duration::from_secs(60);
const DISCOVERY_HTTP_OPERATION_TIMEOUT: Duration = Duration::from_secs(5);
const INITIAL_DISCOVERY_WAIT_SLACK: Duration = Duration::from_secs(1);

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
        .timeout(DISCOVERY_HTTP_OPERATION_TIMEOUT)
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
                            .operation_timeout(DISCOVERY_HTTP_OPERATION_TIMEOUT)
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
    initial_discovery_complete: AtomicBool,
    initial_discovery_notify: tokio::sync::Notify,
    discovery_tasks: Mutex<DiscoveryTaskState>,
    discovery_runtime: ArcSwapOption<DiscoveryRuntime>,
}

/// How long a successful or pending probe keeps the owning runtime eligible.
///
/// [`LiveNodes::ensure_discovery_started`] runs on every request, so an
/// unconditional probe would put a blocking-pool task on the routing path of
/// every request made from a runtime that does not own discovery. Handing
/// discovery over this much later than shutdown is harmless next to refresh
/// intervals measured in seconds. A probe still pending after this interval
/// also triggers handoff, bounding the blocking queue under saturation.
const SHUTDOWN_PROBE_INTERVAL: Duration = Duration::from_millis(250);

#[derive(Debug)]
struct DiscoveryRuntime {
    id: tokio::runtime::Id,
    handle: Handle,
    /// A timed-out probe makes this registration eligible for handoff. If its
    /// pool recovers before a replacement takes over, the registration can be
    /// retained without queuing a second unresolved probe.
    handoff_required: AtomicBool,
    shutdown_probe: Mutex<ShutdownProbe>,
}

#[derive(Debug, Default)]
struct ShutdownProbe {
    last_started: Option<Instant>,
    pending: Option<tokio::task::JoinHandle<()>>,
}

#[derive(Debug, Default)]
struct DiscoveryTaskState {
    active_task: Option<tokio::task::AbortHandle>,
    /// Probes that outlived their ownership registration. Keeping their join
    /// handles lets us prevent the same runtime from queuing another probe
    /// until the blocking pool has removed the original one.
    retired_probes: HashMap<tokio::runtime::Id, tokio::task::JoinHandle<()>>,
}

impl DiscoveryTaskState {
    fn retire_probe(&mut self, runtime_id: tokio::runtime::Id, probe: tokio::task::JoinHandle<()>) {
        let previous = self.retired_probes.insert(runtime_id, probe);
        assert!(
            previous.is_none(),
            "a runtime must not own more than one unresolved discovery probe"
        );
    }
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
    #[cfg(test)]
    fn new(id: tokio::runtime::Id, handle: Handle) -> Self {
        Self::with_probe(id, handle, ShutdownProbe::default())
    }

    fn with_probe(id: tokio::runtime::Id, handle: Handle, shutdown_probe: ShutdownProbe) -> Self {
        Self {
            id,
            handle,
            handoff_required: AtomicBool::new(false),
            shutdown_probe: Mutex::new(shutdown_probe),
        }
    }

    fn should_handoff(&self) -> bool {
        self.should_handoff_at(Instant::now())
    }

    fn should_handoff_at(&self, now: Instant) -> bool {
        let mut probe_state = self
            .shutdown_probe
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        let mut handoff_required = self.handoff_required.load(Ordering::Acquire);
        if let Some(result) = probe_state
            .pending
            .as_mut()
            .and_then(|probe| probe.now_or_never())
        {
            probe_state.pending = None;
            if matches!(result, Err(error) if error.is_cancelled()) {
                self.handoff_required.store(true, Ordering::Release);
                return true;
            }

            // The pool recovered before an eligible replacement took over.
            // Keep the current owner and permit a later bounded probe so a
            // subsequent shutdown can still be distinguished from saturation.
            if handoff_required {
                self.handoff_required.store(false, Ordering::Release);
                handoff_required = false;
            }
        } else if probe_state.pending.is_some() {
            if handoff_required {
                return true;
            }
            if !probe_state.is_due(now) {
                return false;
            }

            // A pre-shutdown probe can remain queued forever behind a blocking
            // task. Keep it attached to this registration: ownership transfer
            // moves it into LiveNodes' retired-probe set so this runtime cannot
            // enqueue another probe until the blocking pool removes this one.
            self.handoff_required.store(true, Ordering::Release);
            return true;
        }

        // A transfer may already have detached the timed-out probe while this
        // registration is still visible through ArcSwap.
        if handoff_required {
            return true;
        }

        if !probe_state.is_due(now) {
            return false;
        }
        probe_state.last_started = Some(now);

        // A live blocking pool either runs this closure or leaves it pending.
        // Once runtime shutdown starts, spawn_blocking rejects it synchronously
        // with a cancelled JoinError. Unlike an async probe, this still works
        // while an async worker cannot drop the discovery task's guard.
        let mut probe = self.handle.spawn_blocking(|| ());
        let shutdown = match (&mut probe).now_or_never() {
            Some(Err(error)) if error.is_cancelled() => true,
            Some(_) => false,
            None => {
                probe_state.pending = Some(probe);
                false
            }
        };
        if shutdown {
            self.handoff_required.store(true, Ordering::Release);
        }

        shutdown
    }

    fn take_pending_probe(&self) -> Option<tokio::task::JoinHandle<()>> {
        self.shutdown_probe
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .pending
            .take()
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
        Self::try_new_with_scope(config, None)
    }

    /// Creates discovery state using `routing_scope` instead of the scope in
    /// `config` while retaining all other transport and refresh settings.
    pub(crate) fn try_new_for_scope(
        config: &crate::config::AlternatorConfig,
        routing_scope: RoutingScope,
    ) -> Result<Option<Arc<Self>>, LiveNodesBuildError> {
        Self::try_new_with_scope(config, Some(routing_scope))
    }

    fn try_new_with_scope(
        config: &crate::config::AlternatorConfig,
        routing_scope: Option<RoutingScope>,
    ) -> Result<Option<Arc<Self>>, LiveNodesBuildError> {
        let active_interval = config
            .active_interval()
            .unwrap_or(DEFAULT_ACTIVE_REFRESH_INTERVAL);
        let idle_interval = config
            .idle_interval()
            .unwrap_or(DEFAULT_IDLE_REFRESH_INTERVAL);
        let routing_scope = routing_scope
            .or_else(|| config.routing_scope())
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
            initial_discovery_complete: AtomicBool::new(false),
            initial_discovery_notify: tokio::sync::Notify::new(),
            discovery_tasks: Mutex::new(DiscoveryTaskState::default()),
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
        self.initial_discovery_notify.notify_waiters();
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
    /// but takes ownership when a probe confirms that the old runtime has shut
    /// down or cannot service the probe within the bounded interval.
    pub(crate) fn ensure_discovery_started(self: &Arc<Self>) {
        let Ok(handle) = Handle::try_current() else {
            return;
        };
        let runtime_id = handle.id();
        // Requests on the owning runtime never take the start/transfer mutex.
        // A caller on another runtime transfers only after the owner rejects a
        // probe or leaves it pending beyond the bounded liveness interval.
        if self
            .discovery_runtime
            .load()
            .as_ref()
            .is_some_and(|active| active.id == runtime_id || !active.should_handoff())
        {
            return;
        }

        let mut discovery_tasks = self
            .discovery_tasks
            .lock()
            .unwrap_or_else(|err| err.into_inner());
        // Another caller may have completed the cold start or transfer while
        // this caller waited for the mutex.
        let active_runtime = self.discovery_runtime.load_full();
        if active_runtime
            .as_ref()
            .is_some_and(|active| active.id == runtime_id || !active.should_handoff())
        {
            return;
        }

        // Aborting a timed-out probe would not remove it from Tokio's blocking
        // queue. Reattach this runtime's retained probe on takeover instead of
        // enqueueing another one.
        discovery_tasks
            .retired_probes
            .retain(|_, probe| !probe.is_finished());
        let retained_probe = discovery_tasks.retired_probes.remove(&runtime_id);

        if let Some(active_runtime) = active_runtime
            && let Some(probe) = active_runtime.take_pending_probe()
        {
            discovery_tasks.retire_probe(active_runtime.id, probe);
        }

        if let Some(old_task) = discovery_tasks.active_task.take() {
            old_task.abort();
        }

        // If no serviceable owner remains, restart immediately even when this
        // runtime has an unresolved retired probe. Reattach it to the new
        // registration so the one-probe bound is preserved without leaving
        // discovery stopped.
        let shutdown_probe =
            retained_probe.map_or_else(ShutdownProbe::default, |pending| ShutdownProbe {
                last_started: Some(Instant::now()),
                pending: Some(pending),
            });
        let runtime = Arc::new(DiscoveryRuntime::with_probe(
            runtime_id,
            handle.clone(),
            shutdown_probe,
        ));
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
        discovery_tasks.active_task = Some(task.abort_handle());
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

    fn mark_initial_discovery_complete(&self) {
        if !self.initial_discovery_complete.swap(true, Ordering::AcqRel) {
            self.initial_discovery_notify.notify_waiters();
        }
    }

    /// Waits until discovery has published its first non-empty topology.
    ///
    /// Rack-scoped clients use this before their first affinity-routed request
    /// so the deterministic coordinator is derived from every rack rather
    /// than from whichever bootstrap seeds one client happened to receive.
    pub(crate) async fn wait_for_initial_discovery(self: &Arc<Self>) -> bool {
        self.ensure_discovery_started();

        loop {
            let notified = self.initial_discovery_notify.notified();
            if self.initial_discovery_complete.load(Ordering::Acquire) {
                return true;
            }
            if tokio::time::timeout(self.initial_discovery_wait_timeout(), notified)
                .await
                .is_err()
            {
                return false;
            }
        }
    }

    fn initial_discovery_wait_timeout(&self) -> Duration {
        let seed_count = u32::try_from(self.seed_urls.len()).unwrap_or(u32::MAX);
        let live_count = u32::try_from(self.live_nodes.load().len()).unwrap_or(u32::MAX);
        let scope_count = u32::try_from(
            std::iter::successors(Some(&self.routing_scope), |scope| scope.fallback()).count(),
        )
        .unwrap_or(u32::MAX);

        // A failed scoped pass can visit the current candidates and then all
        // original seeds. A cluster fallback can visit the union of those two
        // sets. Add one request per configured scope for empty-result
        // fallback traversal, then a small scheduling margin.
        let candidate_budget = seed_count.saturating_add(live_count);
        let request_budget = candidate_budget
            .saturating_mul(2)
            .saturating_add(scope_count);
        DISCOVERY_HTTP_OPERATION_TIMEOUT
            .saturating_mul(request_budget)
            .saturating_add(INITIAL_DISCOVERY_WAIT_SLACK)
    }

    #[cfg(test)]
    async fn wait_for_initial_discovery_with_timeout(self: &Arc<Self>, timeout: Duration) -> bool {
        self.ensure_discovery_started();

        tokio::time::timeout(timeout, async {
            loop {
                let notified = self.initial_discovery_notify.notified();
                if self.initial_discovery_complete.load(Ordering::Acquire) {
                    return;
                }
                notified.await;
            }
        })
        .await
        .is_ok()
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
        self.mark_initial_discovery_complete();
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
        if let Ok(mut state) = self.discovery_tasks.lock()
            && let Some(task) = state.active_task.take()
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
    use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

    struct BlockingPoolBlockers(Vec<Option<std::sync::mpsc::SyncSender<()>>>);

    impl Drop for BlockingPoolBlockers {
        fn drop(&mut self) {
            for release in &mut self.0 {
                if let Some(release) = release.take() {
                    let _ = release.send(());
                }
            }
        }
    }

    fn saturate_blocking_pools(runtimes: &[&tokio::runtime::Runtime]) -> BlockingPoolBlockers {
        let mut releases = Vec::with_capacity(runtimes.len());
        for runtime in runtimes {
            let (started_tx, started_rx) = std::sync::mpsc::sync_channel(0);
            let (release_tx, release_rx) = std::sync::mpsc::sync_channel(0);
            runtime.spawn_blocking(move || {
                started_tx.send(()).unwrap();
                release_rx.recv().unwrap();
            });
            started_rx
                .recv_timeout(Duration::from_secs(2))
                .expect("blocking pool was not saturated");
            releases.push(Some(release_tx));
        }
        BlockingPoolBlockers(releases)
    }

    #[tokio::test]
    async fn initial_discovery_waits_for_a_published_topology() {
        let config = AlternatorConfig::builder()
            .seed_hosts(["127.0.0.1"])
            .port(1)
            .active_interval(Duration::from_secs(60))
            .build();
        let nodes = LiveNodes::new(&config).expect("live nodes");
        let waiter = tokio::spawn({
            let nodes = nodes.clone();
            async move { nodes.wait_for_initial_discovery().await }
        });

        tokio::task::yield_now().await;
        assert!(!waiter.is_finished());

        let generation = nodes.begin_refresh();
        nodes.publish_live_nodes(
            generation,
            vec![Arc::new(Url::parse("http://127.0.0.2:1").unwrap())],
        );

        assert!(
            tokio::time::timeout(Duration::from_secs(1), waiter)
                .await
                .expect("waiter should observe the topology")
                .expect("waiter task should complete")
        );
    }

    #[tokio::test]
    async fn initial_discovery_wait_is_bounded() {
        let config = AlternatorConfig::builder()
            .seed_hosts(["127.0.0.1"])
            .port(1)
            .active_interval(Duration::from_secs(60))
            .build();
        let nodes = LiveNodes::new(&config).expect("live nodes");

        assert!(
            !nodes
                .wait_for_initial_discovery_with_timeout(Duration::from_millis(10))
                .await
        );
    }

    #[test]
    fn initial_discovery_timeout_accounts_for_seeds_and_fallback_scopes() {
        let config = AlternatorConfig::builder()
            .seed_hosts(["127.0.0.1", "127.0.0.2"])
            .port(1)
            .routing_scope(
                RoutingScope::from_rack("dc1".to_string(), "rack1".to_string())
                    .with_fallback(RoutingScope::from_datacenter("dc1".to_string()))
                    .with_fallback(RoutingScope::from_cluster()),
            )
            .build();
        let nodes = LiveNodes::new(&config).expect("live nodes");

        assert_eq!(
            nodes.initial_discovery_wait_timeout(),
            Duration::from_secs(56)
        );

        nodes.live_nodes.store(Arc::new(
            (1..=5)
                .map(|index| Arc::new(Url::parse(&format!("http://node{index}.test:1")).unwrap()))
                .collect(),
        ));
        assert_eq!(
            nodes.initial_discovery_wait_timeout(),
            Duration::from_secs(86)
        );
    }

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
            .seed_hosts(["127.0.0.1"])
            .port(port)
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

    #[test]
    fn shutdown_probe_handoff_is_latched() {
        let runtime = tokio::runtime::Runtime::new().unwrap();
        let discovery_runtime =
            DiscoveryRuntime::new(runtime.handle().id(), runtime.handle().clone());

        runtime.shutdown_background();

        assert!(discovery_runtime.should_handoff());
        assert!(discovery_runtime.handoff_required.load(Ordering::Acquire));
        assert!(discovery_runtime.should_handoff());
    }

    #[test]
    fn pending_shutdown_probe_is_bounded_and_does_not_block_handoff() {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .max_blocking_threads(1)
            .enable_all()
            .build()
            .unwrap();
        let (blocker_started_tx, blocker_started_rx) = std::sync::mpsc::sync_channel(0);
        let (release_blocker_tx, release_blocker_rx) = std::sync::mpsc::sync_channel(0);
        let (blocker_done_tx, blocker_done_rx) = std::sync::mpsc::sync_channel(0);
        let _blocker = runtime.spawn_blocking(move || {
            blocker_started_tx.send(()).unwrap();
            release_blocker_rx.recv().unwrap();
            blocker_done_tx.send(()).unwrap();
        });
        blocker_started_rx
            .recv_timeout(Duration::from_secs(2))
            .expect("blocking pool was not saturated");

        let discovery_runtime =
            DiscoveryRuntime::new(runtime.handle().id(), runtime.handle().clone());
        let started = Instant::now();
        assert!(!discovery_runtime.should_handoff_at(started));
        let first_probe_id = discovery_runtime
            .shutdown_probe
            .lock()
            .unwrap()
            .pending
            .as_ref()
            .expect("probe should be queued behind the blocker")
            .id();

        for _ in 0..10 {
            assert!(!discovery_runtime.should_handoff_at(started));
        }
        let current_probe_id = discovery_runtime
            .shutdown_probe
            .lock()
            .unwrap()
            .pending
            .as_ref()
            .expect("one shutdown probe should remain queued")
            .id();
        assert_eq!(
            first_probe_id, current_probe_id,
            "a saturated blocking pool must retain one probe instead of queuing replacements"
        );

        runtime.shutdown_background();
        assert!(
            discovery_runtime.should_handoff_at(started + SHUTDOWN_PROBE_INTERVAL),
            "a stale pre-shutdown probe must not prevent runtime handoff"
        );

        release_blocker_tx.send(()).unwrap();
        blocker_done_rx
            .recv_timeout(Duration::from_secs(2))
            .expect("blocking task did not finish");
        let deadline = Instant::now() + Duration::from_secs(2);
        loop {
            assert!(
                discovery_runtime.should_handoff_at(started + SHUTDOWN_PROBE_INTERVAL),
                "a timed-out registration must remain eligible for handoff"
            );
            if discovery_runtime
                .shutdown_probe
                .lock()
                .unwrap()
                .pending
                .is_none()
            {
                break;
            }
            assert!(
                Instant::now() < deadline,
                "shutdown probe did not finish after the blocking pool recovered"
            );
            std::thread::yield_now();
        }
    }

    #[test]
    fn saturated_runtimes_reuse_probes_across_discovery_handoffs() {
        let first_runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .max_blocking_threads(1)
            .enable_all()
            .build()
            .unwrap();
        let second_runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .max_blocking_threads(1)
            .enable_all()
            .build()
            .unwrap();

        // Release the blockers before dropping either runtime, including while
        // unwinding from a failed assertion.
        let _blocking_pools = saturate_blocking_pools(&[&first_runtime, &second_runtime]);

        let nodes = LiveNodes::new(&test_config()).unwrap();
        first_runtime.block_on(async {
            nodes.ensure_discovery_started();
        });
        let first_owner = nodes.discovery_runtime.load_full().unwrap();

        // Queue a probe on the first runtime, make it stale without sleeping,
        // and hand discovery to the second runtime.
        second_runtime.block_on(async {
            nodes.ensure_discovery_started();
        });
        let (first_probe_started, first_probe_id) = {
            let probe = first_owner.shutdown_probe.lock().unwrap();
            (
                probe
                    .last_started
                    .expect("first runtime should have a queued probe"),
                probe.pending.as_ref().unwrap().id(),
            )
        };
        assert!(first_owner.should_handoff_at(first_probe_started + SHUTDOWN_PROBE_INTERVAL));
        second_runtime.block_on(async {
            nodes.ensure_discovery_started();
        });
        let second_owner = nodes.discovery_runtime.load_full().unwrap();
        assert_eq!(second_owner.id, second_runtime.handle().id());
        assert_eq!(
            nodes
                .discovery_tasks
                .lock()
                .unwrap()
                .retired_probes
                .get(&first_runtime.handle().id())
                .expect("first runtime's probe should remain tracked")
                .id(),
            first_probe_id
        );

        // Queue and age a probe on the second runtime too. The first runtime
        // can take discovery back by reattaching its retained probe while the
        // second runtime's probe becomes the sole retired probe.
        first_runtime.block_on(async {
            nodes.ensure_discovery_started();
        });
        let (second_probe_started, second_probe_id) = {
            let probe = second_owner.shutdown_probe.lock().unwrap();
            (
                probe
                    .last_started
                    .expect("second runtime should have a queued probe"),
                probe.pending.as_ref().unwrap().id(),
            )
        };
        assert!(second_owner.should_handoff_at(second_probe_started + SHUTDOWN_PROBE_INTERVAL));
        first_runtime.block_on(async {
            nodes.ensure_discovery_started();
        });

        let first_owner_again = nodes.discovery_runtime.load_full().unwrap();
        assert_eq!(first_owner_again.id, first_runtime.handle().id());
        assert_eq!(
            first_owner_again
                .shutdown_probe
                .lock()
                .unwrap()
                .pending
                .as_ref()
                .expect("first runtime's retained probe should be reattached")
                .id(),
            first_probe_id
        );
        {
            let tasks = nodes.discovery_tasks.lock().unwrap();
            assert_eq!(tasks.retired_probes.len(), 1);
            assert_eq!(
                tasks
                    .retired_probes
                    .get(&second_runtime.handle().id())
                    .expect("second runtime's probe should become retired")
                    .id(),
                second_probe_id
            );
        }

        // A further handoff reuses the second runtime's retained probe and
        // retires the first runtime's original probe again. No new blocking
        // probe is added for either runtime.
        let first_probe_started = first_owner_again
            .shutdown_probe
            .lock()
            .unwrap()
            .last_started
            .expect("reattached probe should retain liveness timing");
        assert!(first_owner_again.should_handoff_at(first_probe_started + SHUTDOWN_PROBE_INTERVAL));
        second_runtime.block_on(async {
            nodes.ensure_discovery_started();
        });
        let second_owner_again = nodes.discovery_runtime.load_full().unwrap();
        assert_eq!(second_owner_again.id, second_runtime.handle().id());
        assert_eq!(
            second_owner_again
                .shutdown_probe
                .lock()
                .unwrap()
                .pending
                .as_ref()
                .expect("second runtime's retained probe should be reattached")
                .id(),
            second_probe_id
        );
        let tasks = nodes.discovery_tasks.lock().unwrap();
        assert_eq!(tasks.retired_probes.len(), 1);
        assert_eq!(
            tasks
                .retired_probes
                .get(&first_runtime.handle().id())
                .expect("first runtime's probe should become retired again")
                .id(),
            first_probe_id
        );
    }

    #[test]
    fn ownerless_discovery_reuses_an_unfinished_retired_probe() {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .max_blocking_threads(1)
            .enable_all()
            .build()
            .unwrap();
        let _blocking_pool = saturate_blocking_pools(&[&runtime]);

        let nodes = LiveNodes::new(&test_config()).unwrap();
        let runtime_id = runtime.handle().id();
        let pending = runtime.handle().spawn_blocking(|| ());
        let probe_id = pending.id();
        nodes
            .discovery_tasks
            .lock()
            .unwrap()
            .retire_probe(runtime_id, pending);
        assert!(nodes.discovery_runtime.load().is_none());

        runtime.block_on(async {
            nodes.ensure_discovery_started();
        });

        let owner = nodes.discovery_runtime.load_full().unwrap();
        assert_eq!(owner.id, runtime_id);
        assert_eq!(
            owner
                .shutdown_probe
                .lock()
                .unwrap()
                .pending
                .as_ref()
                .expect("the unresolved retired probe should be reused")
                .id(),
            probe_id
        );
        assert!(
            nodes
                .discovery_tasks
                .lock()
                .unwrap()
                .retired_probes
                .is_empty(),
            "the reused probe must no longer be tracked as retired"
        );
    }

    #[test]
    fn retained_probe_runtime_replaces_shutdown_saturated_owner() {
        let first_runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .max_blocking_threads(1)
            .enable_all()
            .build()
            .unwrap();
        let second_runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .max_blocking_threads(1)
            .enable_all()
            .build()
            .unwrap();
        let _blocking_pools = saturate_blocking_pools(&[&first_runtime, &second_runtime]);

        let nodes = LiveNodes::new(&test_config()).unwrap();
        first_runtime.block_on(async {
            nodes.ensure_discovery_started();
        });
        let first_owner = nodes.discovery_runtime.load_full().unwrap();

        // Hand discovery from A to B while A's blocking pool keeps its probe
        // queued. This leaves that exact probe retained for A.
        second_runtime.block_on(async {
            nodes.ensure_discovery_started();
        });
        let (first_probe_started, first_probe_id) = {
            let probe = first_owner.shutdown_probe.lock().unwrap();
            (
                probe
                    .last_started
                    .expect("first runtime should have a queued probe"),
                probe.pending.as_ref().unwrap().id(),
            )
        };
        assert!(first_owner.should_handoff_at(first_probe_started + SHUTDOWN_PROBE_INTERVAL));
        second_runtime.block_on(async {
            nodes.ensure_discovery_started();
        });
        let second_owner = nodes.discovery_runtime.load_full().unwrap();
        assert_eq!(second_owner.id, second_runtime.handle().id());

        // Queue and age B's probe before occupying its only async worker. Once
        // shutdown starts, neither B's task guard nor its blocking probe can
        // update discovery ownership.
        first_runtime.block_on(async {
            nodes.ensure_discovery_started();
        });
        let (second_probe_started, second_probe_id) = {
            let probe = second_owner.shutdown_probe.lock().unwrap();
            (
                probe
                    .last_started
                    .expect("second runtime should have a queued probe"),
                probe.pending.as_ref().unwrap().id(),
            )
        };
        assert!(second_owner.should_handoff_at(second_probe_started + SHUTDOWN_PROBE_INTERVAL));

        let (worker_started_tx, worker_started_rx) = std::sync::mpsc::sync_channel(0);
        let (release_worker_tx, release_worker_rx) = std::sync::mpsc::sync_channel::<()>(0);
        second_runtime.spawn(async move {
            worker_started_tx.send(()).unwrap();
            let _ = release_worker_rx.recv();
        });
        worker_started_rx
            .recv_timeout(Duration::from_secs(2))
            .expect("second runtime worker was not saturated");
        second_runtime.shutdown_background();
        assert!(Arc::ptr_eq(
            &nodes.discovery_runtime.load_full().unwrap(),
            &second_owner
        ));
        assert!(
            second_owner
                .shutdown_probe
                .lock()
                .unwrap()
                .pending
                .as_ref()
                .is_some_and(|probe| !probe.is_finished()),
            "the saturated owner's probe must remain pending before takeover"
        );

        // One request-side check by A must transfer ownership immediately,
        // reusing A's retained probe and retiring B's pending probe.
        first_runtime.block_on(async {
            nodes.ensure_discovery_started();
        });

        let owner = nodes.discovery_runtime.load_full().unwrap();
        assert_eq!(owner.id, first_runtime.handle().id());
        assert_eq!(
            owner
                .shutdown_probe
                .lock()
                .unwrap()
                .pending
                .as_ref()
                .expect("first runtime's retained probe should be reused")
                .id(),
            first_probe_id
        );
        let tasks = nodes.discovery_tasks.lock().unwrap();
        assert_eq!(tasks.retired_probes.len(), 1);
        assert_eq!(
            tasks
                .retired_probes
                .get(&second_owner.id)
                .expect("second runtime's pending probe should be retired")
                .id(),
            second_probe_id
        );
        drop(tasks);
        drop(release_worker_tx);
    }

    #[test]
    fn stale_task_guard_does_not_clear_replacement_registration() {
        let nodes = LiveNodes::new(&test_config()).unwrap();
        let first_runtime = tokio::runtime::Runtime::new().unwrap();
        let second_runtime = tokio::runtime::Runtime::new().unwrap();
        let first = Arc::new(DiscoveryRuntime::new(
            first_runtime.handle().id(),
            first_runtime.handle().clone(),
        ));
        let second = Arc::new(DiscoveryRuntime::new(
            second_runtime.handle().id(),
            second_runtime.handle().clone(),
        ));

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
        assert!(
            !nodes.initial_discovery_complete.load(Ordering::Acquire),
            "a partial cluster union must not release affinity requests"
        );

        release.notify_one();
        let discovered = tokio::time::timeout(Duration::from_secs(1), discovery)
            .await
            .expect("cluster discovery did not finish after releasing the stale candidate")
            .unwrap()
            .unwrap();
        assert_eq!(discovered, vec![healthy]);
        nodes.publish_live_nodes(generation, discovered);
        assert!(nodes.initial_discovery_complete.load(Ordering::Acquire));
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
