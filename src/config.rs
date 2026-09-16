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

use crate::*;

/// Storage for alternator-specific settings chosen by the user for [AlternatorConfig].
///
/// Each field is an Option, as the user may have not chosen a value.
///
/// It is important to store them separately from Dynamodb's config,
/// because they are consumed by Alternator-specific client construction and
/// request interceptors.
#[derive(Clone, Debug, Default)]
pub(crate) struct AlternatorExtensions {
    pub(crate) request_compression: Option<RequestCompression>,
    pub(crate) response_compression: Option<ResponseCompression>,
    pub(crate) optimize_headers: Option<bool>,
    pub(crate) user_agent: Option<UserAgent>,
    pub(crate) has_credentials_provider: bool,
    pub(crate) require_auth: bool,
    pub(crate) allow_no_auth: bool,
    pub(crate) active_interval: Option<std::time::Duration>,
    pub(crate) idle_interval: Option<std::time::Duration>,
    pub(crate) routing_scope: Option<RoutingScope>,
    pub(crate) scheme: Option<String>,
    pub(crate) port: Option<u16>,
    pub(crate) seed_hosts: Option<Vec<String>>,
    /// Whether to send every request straight to the configured seed host
    /// instead of discovering live cluster nodes through it.
    pub(crate) without_discovery: bool,
    pub(crate) live_nodes: Option<ConfiguredLiveNodes>,
    pub(crate) key_route_affinity: Option<keyrouting::affinity_config::KeyRouteAffinityConfig>,
}

#[derive(Clone, Debug)]
pub(crate) enum ConfiguredLiveNodes {
    AutoCreated(std::sync::Arc<LiveNodes>),
    ExplicitlyShared(std::sync::Arc<LiveNodes>),
}

impl ConfiguredLiveNodes {
    fn nodes(&self) -> std::sync::Arc<LiveNodes> {
        match self {
            Self::AutoCreated(nodes) | Self::ExplicitlyShared(nodes) => nodes.clone(),
        }
    }
}

impl AlternatorExtensions {
    /// The SDK endpoint URL this configuration implies: the first seed host,
    /// with the configured scheme and port.
    ///
    /// Seed hosts are the single source of routing configuration, so the AWS
    /// SDK endpoint follows from them instead of being set on its own. A seed
    /// host that is not a usable authority yields [`None`] here and is
    /// reported by `LiveNodes::try_new` as `InvalidSeedHost`.
    pub(crate) fn endpoint_url(&self) -> Option<String> {
        let seed_host = self.seed_hosts.as_ref()?.first()?;
        let scheme = self.scheme.as_deref().unwrap_or("http");
        crate::live_nodes::build_seed_url(scheme, seed_host, self.port)
            .ok()
            .map(String::from)
    }

    fn invalidate_auto_live_nodes(&mut self) {
        if matches!(self.live_nodes, Some(ConfiguredLiveNodes::AutoCreated(_))) {
            self.live_nodes = None;
        }
    }
}

/// The AWS SDK behavior major version every client is built on.
///
/// The SDK groups its defaults - retries, timeouts, transport, proxy handling -
/// into dated behavior major versions, and asks applications to pick the one
/// they validated against so an SDK upgrade never changes those defaults
/// silently. Alternator's API is fixed and has nothing to do with those
/// bundles, so this driver makes the choice once, for the version it is tested
/// against, rather than passing it on to callers. Retry, timeout and HTTP
/// client settings stay individually configurable on the builder.
pub(crate) const ALTERNATOR_BEHAVIOR_VERSION: fn() -> aws_sdk_dynamodb::config::BehaviorVersion =
    aws_sdk_dynamodb::config::BehaviorVersion::v2026_01_12;

const INCOMPATIBLE_AUTH_OPTIONS_MESSAGE: &str = "require_auth() cannot be combined with allow_no_auth(): require_auth() makes missing credentials fail before sending an unsigned request, while allow_no_auth() explicitly permits unsigned requests.";

fn incompatible_auth_options() -> ! {
    panic!("{INCOMPATIBLE_AUTH_OPTIONS_MESSAGE}");
}

/// [AlternatorClient]'s config
///
/// Stores the AWS DynamoDB config used internally by the driver plus
/// Alternator-specific routing, auth, and request/response settings.
///
/// Build this explicitly with [`AlternatorConfig::builder()`]. Shared
/// `aws_types::SdkConfig` values are not imported wholesale because they can
/// contain AWS auth and endpoint options that Alternator does not support.
/// There is intentionally no `AlternatorConfig::new(&SdkConfig)` or
/// `From<&SdkConfig>` conversion; copy only the supported SDK settings you need
/// onto the builder.
///
/// It is used to construct [AlternatorClient] like so:
///
/// ```
/// use alternator_driver::{AlternatorClient, AlternatorConfig};
///
/// let config =
///     AlternatorConfig::builder()
///     .seed_hosts(["127.0.0.1"])
///     .port(8000)
///     // ...
///     .build();
///
/// let client = AlternatorClient::from_conf(config);
/// ```
///
/// Shared SDK config imports are intentionally unsupported:
///
/// ```compile_fail
/// let sdk_config = aws_types::SdkConfig::builder().build();
/// let _ = alternator_driver::AlternatorConfig::from(&sdk_config);
/// ```
#[derive(Clone, Debug)]
pub struct AlternatorConfig {
    pub(crate) dynamodb_config: aws_sdk_dynamodb::Config,
    pub(crate) alternator_ext: AlternatorExtensions,
}
impl AlternatorConfig {
    pub fn builder() -> AlternatorBuilder {
        AlternatorBuilder::default()
    }

    pub fn operation_builder() -> AlternatorOperationBuilder {
        AlternatorOperationBuilder::default()
    }

    pub fn to_builder(&self) -> AlternatorBuilder {
        AlternatorBuilder {
            dynamodb_builder: self.dynamodb_config.to_builder(),
            alternator_ext: self.alternator_ext.clone(),
        }
    }

    /// Before sending each request, strip headers that Alternator does not use
    /// from the request.
    ///
    /// This is done by an interceptor in `modify_before_transmit` hook.
    ///
    /// Take note, that this may break your own interceptors,
    /// if they happened to look inside these headers after this happens.
    ///
    /// Turned on by default.
    pub fn optimize_headers(&self) -> Option<bool> {
        self.alternator_ext.optimize_headers
    }

    /// Gets the configured final `User-Agent` behavior.
    ///
    /// If this is [`None`], the client sends [`DEFAULT_USER_AGENT`].
    pub fn user_agent(&self) -> Option<&UserAgent> {
        self.alternator_ext.user_agent.as_ref()
    }

    /// Enable / disable request compression.
    ///
    /// This must be done before the request is signed,
    /// and is done by an interceptor in `modify_before_retry_loop` hook.
    ///
    /// Take note, that this may break your own interceptors,
    /// if they happened to look inside the body after this happens.
    ///
    /// Turned off by default.
    pub fn request_compression(&self) -> Option<RequestCompression> {
        self.alternator_ext.request_compression.clone()
    }

    /// Configures which response encodings the client advertises via `Accept-Encoding`.
    ///
    /// This only controls what the client *requests* from the server.
    /// The server may still return uncompressed responses regardless of this setting.
    /// Response decompression is based on the `Content-Encoding` header
    /// and is independent of this configuration.
    ///
    /// Not set by default (disabled).
    pub fn response_compression(&self) -> Option<ResponseCompression> {
        self.alternator_ext.response_compression.clone()
    }

    pub(crate) fn has_credentials_provider(&self) -> bool {
        self.alternator_ext.has_credentials_provider
    }

    /// Returns whether this config requires every request to resolve credentials.
    ///
    /// When this is enabled, a client built from this config will not add the
    /// driver's implicit no-auth fallback for missing default credentials.
    pub fn requires_auth(&self) -> bool {
        self.alternator_ext.require_auth
    }

    pub(crate) fn allows_no_auth(&self) -> bool {
        self.alternator_ext.allow_no_auth
    }

    /// Gets the active interval for refreshing the list of known nodes when the client is active.
    ///
    /// While the client is sending requests to the cluster, the node list is refreshed at
    /// this interval to quickly detect topology changes.
    ///
    /// The client is considered active when it has sent a request within the last `idle_interval`.
    ///
    /// The default value is 1 second.
    pub fn active_interval(&self) -> Option<std::time::Duration> {
        self.alternator_ext.active_interval
    }

    /// Gets the idle interval for refreshing the list of known nodes when the client is idle.
    ///
    /// While no requests are being made to the cluster, the node list is refreshed at this
    /// longer interval to reduce unnecessary network traffic while still keeping the list
    /// reasonably up-to-date.
    ///
    /// The client is considered idle when it hasn't sent a request within the last `idle_interval`.
    ///
    /// The default value is 1 minute.
    pub fn idle_interval(&self) -> Option<std::time::Duration> {
        self.alternator_ext.idle_interval
    }

    /// Get the client's routing scope.
    ///
    /// This is used by the client to route requests to a chosen subset of nodes in the cluster,
    /// based on the routing scope parameters set - datacenter and rack, see [RoutingScope].
    ///
    /// A routing scope can have a fallback scope set by [RoutingScope::with_fallback], which is used if no nodes are available in the preferred scope.
    /// This function can be used multiple times to create a chain of fallback scopes.
    /// Requests will always be routed to the most preferred scope in the chain with available nodes.
    ///
    /// If this is not provided, the client will use the cluster scope, meaning load balancing will happen across live nodes in all discovered datacenters.
    /// Cluster scope requires at least one working seed host from every datacenter that should receive traffic.
    ///
    /// Keep in mind that subsequent fallback scope should ideally be broader than or equal to the
    /// previous one, e.g., (rack -> datacenter -> cluster) or (rack -> another rack -> datacenter -> cluster).
    /// Making a fallback narrower, e.g., (datacenter -> rack) or (cluster -> datacenter),
    /// may be redundant if the set of nodes in the next scope is a subset of the previous one.
    pub fn routing_scope(&self) -> Option<RoutingScope> {
        self.alternator_ext.routing_scope.clone()
    }

    /// Gets the URI scheme (http or https).
    pub fn scheme(&self) -> Option<String> {
        self.alternator_ext.scheme.clone()
    }

    /// Port number for alternator connections.
    pub fn port(&self) -> Option<u16> {
        self.alternator_ext.port
    }

    /// Get the list of seed hosts for cluster discovery.
    ///
    /// The seed hosts are the initial endpoints (IP addresses or hostnames) used to discover the full cluster topology.
    /// Use with [`AlternatorBuilder::scheme`] and [`AlternatorBuilder::port`] to construct the endpoint URIs.
    /// They are also where the AWS SDK endpoint comes from, so this is the
    /// whole routing configuration.
    pub fn seed_hosts(&self) -> Option<Vec<String>> {
        self.alternator_ext.seed_hosts.clone()
    }

    /// Whether client-side discovery and load balancing are turned off.
    ///
    /// See [`AlternatorBuilder::without_discovery`].
    pub fn without_discovery(&self) -> bool {
        self.alternator_ext.without_discovery
    }

    /// The URL the AWS SDK is pointed at, derived from the first seed host.
    ///
    /// With discovery on, requests are rewritten to the live node chosen for
    /// them, so this only decides where a request goes when routing is turned
    /// off through [`AlternatorBuilder::without_discovery`].
    pub fn endpoint_url(&self) -> Option<String> {
        self.alternator_ext.endpoint_url()
    }

    /// The [`LiveNodes`] instance shared into this config, if any.
    ///
    /// On a config you have built but not yet used, this is [`None`] unless you
    /// set it via [`live_nodes`], and [`None`] means a client built from it will
    /// construct its own discovery state. On the config a client stores
    /// ([`config()`]), this is populated with that automatically created state
    /// unless discovery was turned off with
    /// [`without_discovery`](AlternatorBuilder::without_discovery).
    ///
    /// Rebuilding a client-stored config preserves automatically created state
    /// while changing unrelated settings. Changing a discovery setting (seed
    /// hosts, scheme, port, routing scope, or refresh intervals)
    /// invalidates automatically created state so the next client constructs a
    /// matching instance. A [`LiveNodes`] value supplied explicitly through
    /// [`live_nodes`] remains shared and keeps its own discovery settings.
    ///
    /// [`live_nodes`]: Self::live_nodes
    /// [`config()`]: AlternatorClient::config
    pub fn live_nodes(&self) -> Option<std::sync::Arc<LiveNodes>> {
        self.alternator_ext
            .live_nodes
            .as_ref()
            .map(ConfiguredLiveNodes::nodes)
    }

    /// Gets the key route affinity configuration.
    ///
    /// For more information see [keyrouting::affinity_config::KeyRouteAffinityConfig] and [keyrouting::affinity_config::KeyRouteAffinityType].
    pub fn key_route_affinity(
        &self,
    ) -> Option<keyrouting::affinity_config::KeyRouteAffinityConfig> {
        self.alternator_ext.key_route_affinity.clone()
    }
}

/// Builder for Alternator compression settings that can be overridden for one operation.
///
/// This intentionally exposes only request and response compression. Use
/// [`AlternatorConfig::builder()`] for client construction settings such as
/// header stripping, user-agent handling, and routing. Use the AWS SDK's
/// `config_override(...)` for SDK-level per-operation overrides.
///
/// ```no_run
/// # tokio::runtime::Runtime::new().unwrap().block_on(async {
/// use alternator_driver::{
///     AlternatorClient,
///     AlternatorConfig,
///     AlternatorCustomizableOperation,
///     RequestCompression,
/// };
///
/// let client = AlternatorClient::from_conf(
///     AlternatorConfig::builder()
///         .seed_hosts(["127.0.0.1"])
///         .port(8000)
///         .build(),
/// );
///
/// client
///     .list_tables()
///     .customize()
///     .alternator_config_override(
///         AlternatorConfig::operation_builder()
///             .request_compression(RequestCompression::disabled()),
///     )
///     .send()
///     .await
///     .unwrap();
/// # });
/// ```
///
/// Client-level settings are not available here:
///
/// ```compile_fail
/// use alternator_driver::AlternatorConfig;
///
/// let _ = AlternatorConfig::operation_builder().user_agent("orders-service/1.0");
/// ```
///
/// ```compile_fail
/// use alternator_driver::AlternatorConfig;
///
/// let _ = AlternatorConfig::operation_builder().optimize_headers(false);
/// ```
#[derive(Clone, Debug, Default)]
pub struct AlternatorOperationBuilder {
    pub(crate) request_compression: Option<RequestCompression>,
    pub(crate) response_compression: Option<ResponseCompression>,
}

impl AlternatorOperationBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    /// Enable / disable request compression for this request.
    pub fn request_compression(mut self, request_compression: RequestCompression) -> Self {
        self.request_compression = Some(request_compression);
        self
    }

    /// Configure which response encodings this request advertises via `Accept-Encoding`.
    pub fn response_compression(mut self, response_compression: ResponseCompression) -> Self {
        self.response_compression = Some(response_compression);
        self
    }
}

/// Builder for [AlternatorConfig]
///
/// Builder for the supported Alternator configuration surface.
///
/// This includes Alternator-specific options plus AWS SDK settings that remain
/// meaningful for Alternator clients. AWS-specific auth schemes, auth scheme
/// preferences, custom endpoint resolvers, FIPS endpoints, dual-stack
/// endpoints, and account ID endpoint mode are intentionally not exposed.
/// Neither is the SDK endpoint URL: discovery and client-side routing are
/// configured with the Alternator-specific [`scheme`](Self::scheme),
/// [`port`](Self::port) and [`seed_hosts`](Self::seed_hosts) settings, and the
/// endpoint the SDK sends to follows from them.
///
/// It is used to construct [AlternatorClient] like so:
///
/// ```
/// use alternator_driver::{AlternatorClient, AlternatorConfig};
///
/// let config =
///     AlternatorConfig::builder()
///     .seed_hosts(["127.0.0.1"])
///     .port(8000)
///     // ...
///     .build();
///
/// let client = AlternatorClient::from_conf(config);
/// ```
#[derive(Clone, Debug, Default)]
pub struct AlternatorBuilder {
    pub(crate) dynamodb_builder: aws_sdk_dynamodb::config::Builder,
    pub(crate) alternator_ext: AlternatorExtensions,
}
impl AlternatorBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn build(mut self) -> AlternatorConfig {
        self.dynamodb_builder
            .set_behavior_version(Some(ALTERNATOR_BEHAVIOR_VERSION()));
        // The seed hosts are the only routing configuration there is, so the
        // SDK endpoint is derived from them rather than set by the caller. It
        // is what unrouted requests use, and a placeholder the routing
        // interceptor overwrites per request when discovery is on.
        self.dynamodb_builder
            .set_endpoint_url(self.alternator_ext.endpoint_url());

        AlternatorConfig {
            dynamodb_config: self.dynamodb_builder.build(),
            alternator_ext: self.alternator_ext,
        }
    }

    /// Before sending each request, strip the headers from them which are not used by the Alternator.
    ///
    /// This is done by an interceptor in `modify_before_transmit` hook.
    ///
    /// Take note, that this may break your own interceptors,
    /// if they happened to look inside these headers after this happens.
    ///
    /// Turned on by default.
    pub fn optimize_headers(mut self, optimize: bool) -> Self {
        self.set_optimize_headers(optimize);
        self
    }

    /// Before sending each request, strip the headers from them which are not used by the Alternator.
    ///
    /// This is done by an interceptor in `modify_before_transmit` hook.
    ///
    /// Take note, that this may break your own interceptors,
    /// if they happened to look inside these headers after this happens.
    ///
    /// Turned on by default.
    pub fn set_optimize_headers(&mut self, optimize: bool) -> &mut Self {
        self.alternator_ext.optimize_headers = Some(optimize);
        self
    }

    /// Configure the final `User-Agent` header sent by the client.
    ///
    /// By default, the client sends [`DEFAULT_USER_AGENT`]. Passing a string
    /// replaces it exactly. Use [`UserAgent::transform`] to derive a custom
    /// value from the default, or [`UserAgent::disabled`] to send no
    /// `User-Agent` header.
    pub fn user_agent(mut self, user_agent: impl Into<UserAgent>) -> Self {
        self.set_user_agent(Some(user_agent.into()));
        self
    }

    /// Configure the final `User-Agent` header sent by the client.
    ///
    /// Setting this to [`None`] restores the default behavior.
    pub fn set_user_agent(&mut self, user_agent: Option<UserAgent>) -> &mut Self {
        self.alternator_ext.user_agent = user_agent;
        self
    }

    /// Disable the final `User-Agent` header sent by the client.
    pub fn without_user_agent(mut self) -> Self {
        self.set_without_user_agent();
        self
    }

    /// Disable the final `User-Agent` header sent by the client.
    pub fn set_without_user_agent(&mut self) -> &mut Self {
        self.alternator_ext.user_agent = Some(UserAgent::disabled());
        self
    }

    /// Enable / disable request compression.
    ///
    /// This must be done before the request is signed,
    /// and is done by an interceptor in `modify_before_retry_loop` hook.
    ///
    /// Take note, that this may break your own interceptors,
    /// if they happened to look inside the body after this happens.
    ///
    /// Turned off by default.
    pub fn request_compression(mut self, request_compression: RequestCompression) -> Self {
        self.set_request_compression(request_compression);
        self
    }

    /// Enable / disable request compression.
    ///
    /// This must be done before the request is signed,
    /// and is done by an interceptor in `modify_before_retry_loop` hook.
    ///
    /// Take note, that this may break your own interceptors,
    /// if they happened to look inside the body after this happens.
    ///
    /// Turned off by default.
    pub fn set_request_compression(
        &mut self,
        request_compression: RequestCompression,
    ) -> &mut Self {
        self.alternator_ext.request_compression = Some(request_compression);
        self
    }

    /// Configure which response encodings the client advertises via `Accept-Encoding`.
    ///
    /// This only controls what the client *requests* from the server.
    /// The server may still return uncompressed responses regardless of this setting.
    /// Response decompression is based on the `Content-Encoding` header
    /// and is independent of this configuration.
    ///
    /// Not set by default (disabled).
    pub fn response_compression(mut self, response_compression: ResponseCompression) -> Self {
        self.set_response_compression(response_compression);
        self
    }

    /// Configure which response encodings the client advertises via `Accept-Encoding`.
    ///
    /// This only controls what the client *requests* from the server.
    /// The server may still return uncompressed responses regardless of this setting.
    /// Response decompression is based on the `Content-Encoding` header
    /// and is independent of this configuration.
    ///
    /// Not set by default (disabled).
    pub fn set_response_compression(
        &mut self,
        response_compression: ResponseCompression,
    ) -> &mut Self {
        self.alternator_ext.response_compression = Some(response_compression);
        self
    }

    /// Sets the active interval for refreshing the list of known nodes when the client is active.
    ///
    /// While the client is sending requests to the cluster, the node list is refreshed at
    /// this interval to quickly detect topology changes.
    ///
    /// The client is considered active when it has sent a request within the last `idle_interval`.
    ///
    /// The default value is 1 second.
    pub fn active_interval(mut self, active_interval: std::time::Duration) -> Self {
        self.set_active_interval(active_interval);
        self
    }

    /// Sets the active interval for refreshing the list of known nodes when the client is active.
    ///
    /// While the client is sending requests to the cluster, the node list is refreshed at
    /// this interval to quickly detect topology changes.
    ///
    /// The client is considered active when it has sent a request within the last `idle_interval`.
    ///
    /// The default value is 1 second.
    pub fn set_active_interval(&mut self, active_interval: std::time::Duration) -> &mut Self {
        self.alternator_ext.invalidate_auto_live_nodes();
        self.alternator_ext.active_interval = Some(active_interval);
        self
    }

    /// Sets the idle interval for refreshing the list of known nodes when the client is idle.
    ///
    /// While no requests are being made to the cluster, the node list is refreshed at this
    /// longer interval to reduce unnecessary network traffic while still keeping the list
    /// reasonably up-to-date.
    ///
    /// The client is considered idle when it hasn't sent a request within the last `idle_interval`.
    ///
    /// The default value is 1 minute.
    pub fn idle_interval(mut self, idle_interval: std::time::Duration) -> Self {
        self.set_idle_interval(idle_interval);
        self
    }

    /// Sets the idle interval for refreshing the list of known nodes when the client is idle.
    ///
    /// While no requests are being made to the cluster, the node list is refreshed at this
    /// longer interval to reduce unnecessary network traffic while still keeping the list
    /// reasonably up-to-date.
    ///
    /// The client is considered idle when it hasn't sent a request within the last `idle_interval`.
    ///
    /// The default value is 1 minute.
    pub fn set_idle_interval(&mut self, idle_interval: std::time::Duration) -> &mut Self {
        self.alternator_ext.invalidate_auto_live_nodes();
        self.alternator_ext.idle_interval = Some(idle_interval);
        self
    }

    /// Set the routing scope for the client.
    ///
    /// This is used by the client to route requests to a chosen subset of nodes in the cluster,
    /// based on the routing scope parameters set - datacenter and rack, see [RoutingScope].
    ///
    /// A routing scope can have a fallback scope set by [RoutingScope::with_fallback], which is used if no nodes are available in the preferred scope.
    /// This function can be used multiple times to create a chain of fallback scopes.
    /// Requests will always be routed to the most preferred scope in the chain with available nodes.
    ///
    /// If this is not provided, the client will use the cluster scope, meaning load balancing will happen across live nodes in all discovered datacenters.
    /// Cluster scope requires at least one working seed host from every datacenter that should receive traffic.
    ///
    /// Keep in mind that subsequent fallback scope should ideally be broader than or equal to the
    /// previous one, e.g., (rack -> datacenter -> cluster) or (rack -> another rack -> datacenter -> cluster).
    /// Making a fallback narrower, e.g., (datacenter -> rack) or (cluster -> datacenter),
    /// may be redundant if the set of nodes in the next scope is a subset of the previous one.
    pub fn routing_scope(mut self, routing_scope: RoutingScope) -> Self {
        self.set_routing_scope(routing_scope);
        self
    }

    /// Set the routing scope for the client.
    ///
    /// This is used by the client to route requests to a chosen subset of nodes in the cluster,
    /// based on the routing scope parameters set - datacenter and rack, see [RoutingScope].
    ///
    /// A routing scope can have a fallback scope set by [RoutingScope::with_fallback], which is used if no nodes are available in the preferred scope.
    /// This function can be used multiple times to create a chain of fallback scopes.
    /// Requests will always be routed to the most preferred scope in the chain with available nodes.
    ///
    /// If this is not provided, the client will use the cluster scope, meaning load balancing will happen across live nodes in all discovered datacenters.
    /// Cluster scope requires at least one working seed host from every datacenter that should receive traffic.
    ///
    /// Keep in mind that subsequent fallback scope should ideally be broader than or equal to the
    /// previous one, e.g., (rack -> datacenter -> cluster) or (rack -> another rack -> datacenter -> cluster).
    /// Making a fallback narrower, e.g., (datacenter -> rack) or (cluster -> datacenter),
    /// may be redundant if the set of nodes in the next scope is a subset of the previous one.
    pub fn set_routing_scope(&mut self, routing_scope: RoutingScope) -> &mut Self {
        self.alternator_ext.invalidate_auto_live_nodes();
        self.alternator_ext.routing_scope = Some(routing_scope);
        self
    }

    /// Sets the URI scheme (http or https).
    ///
    /// Accepts for example "http", "http:", "http://" — stores just "http", same with "https".
    /// Other values are rejected when constructing an [`AlternatorClient`].
    pub fn scheme(mut self, scheme: impl Into<String>) -> Self {
        self.set_scheme(scheme);
        self
    }

    /// Sets the URI scheme (http or https).
    ///
    /// Accepts for example "http", "http:", "http://" — stores just "http", same with "https".
    /// Other values are rejected when constructing an [`AlternatorClient`].
    pub fn set_scheme(&mut self, scheme: impl Into<String>) -> &mut Self {
        self.alternator_ext.invalidate_auto_live_nodes();
        let s = scheme.into();

        let normalized = s.trim_end_matches('/').trim_end_matches(':').to_string();
        self.alternator_ext.scheme = Some(normalized);
        self
    }

    /// Port number for alternator connections
    pub fn port(mut self, port: u16) -> Self {
        self.set_port(port);
        self
    }

    /// Port number for alternator connections
    pub fn set_port(&mut self, port: u16) -> &mut Self {
        self.alternator_ext.invalidate_auto_live_nodes();
        self.alternator_ext.port = Some(port);
        self
    }

    /// Set the list of seed hosts for cluster discovery.
    ///
    /// The seed hosts are the initial endpoints (IP addresses or hostnames) used to discover the full cluster topology.
    /// Use with [`AlternatorBuilder::scheme`] and [`AlternatorBuilder::port`] to construct the endpoint URIs.
    /// They are the only routing configuration this driver takes: the AWS SDK
    /// endpoint follows from the first of them, so there is no separate
    /// endpoint URL to keep in step with them.
    ///
    /// To send requests through a proxy or an external load balancer instead
    /// of discovering and balancing across cluster nodes, give its address as
    /// the single seed host and turn discovery off with
    /// [`AlternatorBuilder::without_discovery`].
    pub fn seed_hosts<I, S>(mut self, seed_hosts: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.set_seed_hosts(seed_hosts.into_iter().map(Into::into).collect());
        self
    }

    /// Set the list of seed hosts for cluster discovery.
    ///
    /// See [`AlternatorBuilder::seed_hosts`].
    pub fn set_seed_hosts(&mut self, seed_hosts: Vec<String>) -> &mut Self {
        self.alternator_ext.invalidate_auto_live_nodes();
        self.alternator_ext.seed_hosts = Some(seed_hosts);
        self
    }

    /// Send every request straight to the configured seed host instead of
    /// discovering live cluster nodes through it.
    ///
    /// Use this when a proxy or an external load balancer sits in front of the
    /// cluster and is the only address this client should talk to. Requests go
    /// to the first seed host, with the configured
    /// [`scheme`](AlternatorBuilder::scheme) and [`port`](AlternatorBuilder::port),
    /// and no `/localnodes` discovery runs. Without a seed host to send them
    /// to, building a client fails rather than routing anywhere unintended.
    pub fn without_discovery(mut self) -> Self {
        self.set_without_discovery();
        self
    }

    /// Send every request straight to the configured seed host instead of
    /// discovering live cluster nodes through it.
    ///
    /// See [`AlternatorBuilder::without_discovery`].
    pub fn set_without_discovery(&mut self) -> &mut Self {
        self.alternator_ext.invalidate_auto_live_nodes();
        self.alternator_ext.without_discovery = true;
        self
    }

    /// Share an existing [`LiveNodes`] instance across clients (optional).
    ///
    /// By default you never need to call this. Every client built from a config
    /// constructs its own [`LiveNodes`] and spawns a background task that discovers
    /// and periodically refreshes the set of live cluster nodes. If you run several
    /// clients with the same load-balancing settings, each one spawns its own task,
    /// and they all do identical work.
    ///
    /// Setting this lets those clients share a single [`LiveNodes`], and therefore
    /// a single discovery task instead of each running their own.
    ///
    /// The discovery task is owned by the [`LiveNodes`] instance: it lives as long
    /// as the instance does and stops once the last [`Arc`] to it is dropped. The
    /// instance is shared through an [`Arc`], so handing the same one to several
    /// clients is exactly how they come to share that one task.
    ///
    /// When you pass a shared instance, these settings on this config are
    /// ignored - the shared [`LiveNodes`] already has its own, fixed at the
    /// time it was constructed:
    /// - active and idle refresh intervals
    /// - routing scope
    /// - seed hosts
    /// - scheme and port
    ///
    /// Construct the instance to share with [`LiveNodes::new`], then pass it here.
    /// If you already have a built [`AlternatorConfig`],
    /// [`AlternatorClient::from_conf_with_live_nodes`] takes both at once, so you
    /// don't have to rebuild the config yourself to inject the instance.
    ///
    /// For more information, see [`LiveNodes`].
    ///
    /// [`Arc`]: std::sync::Arc
    pub fn live_nodes(mut self, live_nodes: std::sync::Arc<LiveNodes>) -> Self {
        self.set_live_nodes(live_nodes);
        self
    }

    /// Share an existing [`LiveNodes`] instance across clients (optional).
    ///
    /// By default you never need to call this. Every client built from a config
    /// constructs its own [`LiveNodes`] and spawns a background task that discovers
    /// and periodically refreshes the set of live cluster nodes. If you run several
    /// clients with the same load-balancing settings, each one spawns its own task,
    /// and they all do identical work.
    ///
    /// Setting this lets those clients share a single [`LiveNodes`], and therefore
    /// a single discovery task instead of each running their own.
    ///
    /// The discovery task is owned by the [`LiveNodes`] instance: it lives as long
    /// as the instance does and stops once the last [`Arc`] to it is dropped. The
    /// instance is shared through an [`Arc`], so handing the same one to several
    /// clients is exactly how they come to share that one task.
    ///
    /// When you pass a shared instance, these settings on this config are
    /// ignored - the shared [`LiveNodes`] already has its own, fixed at the
    /// time it was constructed:
    /// - active and idle refresh intervals
    /// - routing scope
    /// - seed hosts
    /// - scheme and port
    ///
    /// Construct the instance to share with [`LiveNodes::new`], then pass it here.
    /// If you already have a built [`AlternatorConfig`],
    /// [`AlternatorClient::from_conf_with_live_nodes`] takes both at once, so you
    /// don't have to rebuild the config yourself to inject the instance.
    ///
    /// For more information, see [`LiveNodes`].
    ///
    /// [`Arc`]: std::sync::Arc
    pub fn set_live_nodes(&mut self, live_nodes: std::sync::Arc<LiveNodes>) -> &mut Self {
        self.alternator_ext.live_nodes = Some(ConfiguredLiveNodes::ExplicitlyShared(live_nodes));
        self
    }

    pub(crate) fn auto_created_live_nodes(mut self, live_nodes: std::sync::Arc<LiveNodes>) -> Self {
        self.alternator_ext.live_nodes = Some(ConfiguredLiveNodes::AutoCreated(live_nodes));
        self
    }

    /// Sets the key route affinity configuration.
    ///
    /// Use it either with a pre-constructed [keyrouting::affinity_config::KeyRouteAffinityConfig]
    /// or with a [keyrouting::affinity_config::KeyRouteAffinityType] for simpler
    /// use cases. Calling with
    /// [keyrouting::affinity_config::KeyRouteAffinityType::None] is equivalent
    /// to not setting the affinity at all.
    ///
    /// For more information, see
    /// [keyrouting::affinity_config::KeyRouteAffinityConfig] and
    /// [keyrouting::affinity_config::KeyRouteAffinityType].
    pub fn key_route_affinity(
        mut self,
        key_route_affinity: impl Into<keyrouting::affinity_config::KeyRouteAffinityConfig>,
    ) -> Self {
        self.set_key_route_affinity(key_route_affinity.into());
        self
    }

    /// Sets the key route affinity configuration.
    ///
    /// Use it either with a pre-constructed [keyrouting::affinity_config::KeyRouteAffinityConfig]
    /// or with a [keyrouting::affinity_config::KeyRouteAffinityType] for simpler
    /// use cases. Calling with
    /// [keyrouting::affinity_config::KeyRouteAffinityType::None] is equivalent
    /// to not setting the affinity at all.
    ///
    /// For more information, see
    /// [keyrouting::affinity_config::KeyRouteAffinityConfig] and
    /// [keyrouting::affinity_config::KeyRouteAffinityType].
    pub fn set_key_route_affinity(
        &mut self,
        key_route_affinity: impl Into<keyrouting::affinity_config::KeyRouteAffinityConfig>,
    ) -> &mut Self {
        self.alternator_ext.key_route_affinity = Some(key_route_affinity.into());
        self
    }
}

// All implementations below this point are supported AWS SDK passthroughs.

impl AlternatorConfig {
    pub fn stalled_stream_protection(
        &self,
    ) -> Option<&aws_sdk_dynamodb::config::StalledStreamProtectionConfig> {
        self.dynamodb_config.stalled_stream_protection()
    }

    pub fn http_client(&self) -> Option<aws_sdk_dynamodb::config::SharedHttpClient> {
        self.dynamodb_config.http_client()
    }

    pub fn retry_config(&self) -> Option<&aws_smithy_types::retry::RetryConfig> {
        self.dynamodb_config.retry_config()
    }

    pub fn sleep_impl(&self) -> Option<aws_sdk_dynamodb::config::SharedAsyncSleep> {
        self.dynamodb_config.sleep_impl()
    }

    pub fn timeout_config(&self) -> Option<&aws_smithy_types::timeout::TimeoutConfig> {
        self.dynamodb_config.timeout_config()
    }

    pub fn retry_partition(&self) -> Option<&aws_smithy_runtime::client::retries::RetryPartition> {
        self.dynamodb_config.retry_partition()
    }

    pub fn identity_cache(&self) -> Option<aws_sdk_dynamodb::config::SharedIdentityCache> {
        self.dynamodb_config.identity_cache()
    }

    pub fn interceptors(
        &self,
    ) -> impl Iterator<Item = aws_sdk_dynamodb::config::SharedInterceptor> {
        self.dynamodb_config.interceptors()
    }

    pub fn time_source(&self) -> Option<aws_smithy_async::time::SharedTimeSource> {
        self.dynamodb_config.time_source()
    }

    pub fn retry_classifiers(
        &self,
    ) -> impl Iterator<Item = aws_smithy_runtime_api::client::retries::classifiers::SharedRetryClassifier>
    {
        self.dynamodb_config.retry_classifiers()
    }

    pub fn app_name(&self) -> Option<&aws_types::app_name::AppName> {
        self.dynamodb_config.app_name()
    }

    pub fn framework_metadata(&self) -> Vec<&aws_sdk_dynamodb::config::FrameworkMetadata> {
        self.dynamodb_config.framework_metadata()
    }

    pub fn disable_clock_skew_correction(&self) -> Option<bool> {
        self.dynamodb_config.disable_clock_skew_correction()
    }

    pub fn invocation_id_generator(
        &self,
    ) -> Option<aws_runtime::invocation_id::SharedInvocationIdGenerator> {
        self.dynamodb_config.invocation_id_generator()
    }

    pub fn signing_name(&self) -> &'static str {
        self.dynamodb_config.signing_name()
    }

    pub fn region(&self) -> Option<&aws_sdk_dynamodb::config::Region> {
        self.dynamodb_config.region()
    }
}

impl AlternatorBuilder {
    pub fn emit_input_attributes(
        mut self,
        names: impl IntoIterator<Item = impl Into<String>>,
    ) -> Self {
        self.dynamodb_builder = self.dynamodb_builder.emit_input_attributes(names);
        self
    }

    pub fn capture_input_attributes(
        mut self,
        names: impl IntoIterator<Item = impl Into<String>>,
    ) -> Self {
        self.dynamodb_builder = self.dynamodb_builder.capture_input_attributes(names);
        self
    }

    pub fn stalled_stream_protection(
        mut self,
        stalled_stream_protection_config: aws_sdk_dynamodb::config::StalledStreamProtectionConfig,
    ) -> Self {
        self.dynamodb_builder = self
            .dynamodb_builder
            .stalled_stream_protection(stalled_stream_protection_config);
        self
    }

    pub fn set_stalled_stream_protection(
        &mut self,
        stalled_stream_protection_config: Option<
            aws_sdk_dynamodb::config::StalledStreamProtectionConfig,
        >,
    ) -> &mut Self {
        self.dynamodb_builder
            .set_stalled_stream_protection(stalled_stream_protection_config);
        self
    }

    pub fn http_client(
        mut self,
        http_client: impl aws_sdk_dynamodb::config::HttpClient + 'static,
    ) -> Self {
        self.dynamodb_builder = self.dynamodb_builder.http_client(http_client);
        self
    }

    pub fn set_http_client(
        &mut self,
        http_client: Option<aws_sdk_dynamodb::config::SharedHttpClient>,
    ) -> &mut Self {
        self.dynamodb_builder.set_http_client(http_client);
        self
    }

    /// Require every request made by a client built from this config to use credentials.
    ///
    /// By default, an Alternator client with no default credentials enables the AWS SDK's no-auth
    /// mode automatically, because many Alternator deployments do not require signing. Use
    /// `require_auth()` for clients that intentionally have no default credentials but must still
    /// be signed by per-request credentials supplied with `customize().config_override(...)`.
    ///
    /// When this option is set and no credentials are available for an operation, the AWS SDK
    /// fails auth resolution before the request is sent instead of falling back to an unsigned
    /// request. This is a client construction option; it cannot remove no-auth from an already
    /// constructed client as a per-operation override.
    ///
    /// Panics if combined with [`allow_no_auth`](Self::allow_no_auth), because that option
    /// explicitly permits unsigned requests.
    pub fn require_auth(mut self) -> Self {
        self.set_require_auth(true);
        self
    }

    /// Sets whether this config requires credentials for every request.
    ///
    /// See [`require_auth`](Self::require_auth) for the behavior and intended use case.
    pub fn set_require_auth(&mut self, require_auth: bool) -> &mut Self {
        if require_auth && self.alternator_ext.allow_no_auth {
            incompatible_auth_options();
        }
        self.alternator_ext.require_auth = require_auth;
        self
    }

    /// Explicitly permit unsigned requests.
    ///
    /// This mirrors the AWS SDK no-auth escape hatch. It cannot be combined with
    /// [`require_auth`](Self::require_auth), which intentionally makes missing credentials fail.
    pub fn allow_no_auth(mut self) -> Self {
        if self.alternator_ext.require_auth {
            incompatible_auth_options();
        }
        self.alternator_ext.allow_no_auth = true;
        self.dynamodb_builder = self.dynamodb_builder.allow_no_auth();
        self
    }

    /// Explicitly permit unsigned requests.
    ///
    /// See [`allow_no_auth`](Self::allow_no_auth).
    pub fn set_allow_no_auth(&mut self) -> &mut Self {
        if self.alternator_ext.require_auth {
            incompatible_auth_options();
        }
        self.alternator_ext.allow_no_auth = true;
        self.dynamodb_builder.set_allow_no_auth();
        self
    }

    pub fn retry_config(mut self, retry_config: aws_smithy_types::retry::RetryConfig) -> Self {
        self.dynamodb_builder = self.dynamodb_builder.retry_config(retry_config);
        self
    }

    pub fn set_retry_config(
        &mut self,
        retry_config: Option<aws_smithy_types::retry::RetryConfig>,
    ) -> &mut Self {
        self.dynamodb_builder.set_retry_config(retry_config);
        self
    }
    pub fn sleep_impl(
        mut self,
        sleep_impl: impl aws_sdk_dynamodb::config::AsyncSleep + 'static,
    ) -> Self {
        self.dynamodb_builder = self.dynamodb_builder.sleep_impl(sleep_impl);
        self
    }

    pub fn set_sleep_impl(
        &mut self,
        sleep_impl: Option<aws_sdk_dynamodb::config::SharedAsyncSleep>,
    ) -> &mut Self {
        self.dynamodb_builder.set_sleep_impl(sleep_impl);
        self
    }

    pub fn timeout_config(
        mut self,
        timeout_config: aws_smithy_types::timeout::TimeoutConfig,
    ) -> Self {
        self.dynamodb_builder = self.dynamodb_builder.timeout_config(timeout_config);
        self
    }

    pub fn set_timeout_config(
        &mut self,
        timeout_config: Option<aws_smithy_types::timeout::TimeoutConfig>,
    ) -> &mut Self {
        self.dynamodb_builder.set_timeout_config(timeout_config);
        self
    }

    pub fn retry_partition(
        mut self,
        retry_partition: aws_smithy_runtime::client::retries::RetryPartition,
    ) -> Self {
        self.dynamodb_builder = self.dynamodb_builder.retry_partition(retry_partition);
        self
    }

    pub fn set_retry_partition(
        &mut self,
        retry_partition: Option<aws_smithy_runtime::client::retries::RetryPartition>,
    ) -> &mut Self {
        self.dynamodb_builder.set_retry_partition(retry_partition);
        self
    }

    pub fn identity_cache(
        mut self,
        identity_cache: impl aws_sdk_dynamodb::config::ResolveCachedIdentity + 'static,
    ) -> Self {
        self.dynamodb_builder = self.dynamodb_builder.identity_cache(identity_cache);
        self
    }

    pub fn set_identity_cache(
        &mut self,
        identity_cache: impl aws_sdk_dynamodb::config::ResolveCachedIdentity + 'static,
    ) -> &mut Self {
        self.dynamodb_builder.set_identity_cache(identity_cache);
        self
    }
    pub fn interceptor(
        mut self,
        interceptor: impl aws_sdk_dynamodb::config::Intercept + 'static,
    ) -> Self {
        self.dynamodb_builder = self.dynamodb_builder.interceptor(interceptor);
        self
    }

    pub fn push_interceptor(
        &mut self,
        interceptor: aws_sdk_dynamodb::config::SharedInterceptor,
    ) -> &mut Self {
        self.dynamodb_builder.push_interceptor(interceptor);
        self
    }

    pub fn set_interceptors(
        &mut self,
        interceptors: impl IntoIterator<Item = aws_sdk_dynamodb::config::SharedInterceptor>,
    ) -> &mut Self {
        self.dynamodb_builder.set_interceptors(interceptors);
        self
    }

    pub fn time_source(
        mut self,
        time_source: impl aws_smithy_async::time::TimeSource + 'static,
    ) -> Self {
        self.dynamodb_builder = self.dynamodb_builder.time_source(time_source);
        self
    }

    pub fn set_time_source(
        &mut self,
        time_source: Option<aws_smithy_async::time::SharedTimeSource>,
    ) -> &mut Self {
        self.dynamodb_builder.set_time_source(time_source);
        self
    }

    pub fn retry_classifier(
        mut self,
        retry_classifier: impl aws_smithy_runtime_api::client::retries::classifiers::ClassifyRetry
        + 'static,
    ) -> Self {
        self.dynamodb_builder = self.dynamodb_builder.retry_classifier(retry_classifier);
        self
    }

    pub fn push_retry_classifier(
        &mut self,
        retry_classifier: aws_smithy_runtime_api::client::retries::classifiers::SharedRetryClassifier,
    ) -> &mut Self {
        self.dynamodb_builder
            .push_retry_classifier(retry_classifier);
        self
    }

    pub fn set_retry_classifiers(
        &mut self,
        retry_classifiers: impl IntoIterator<
            Item = aws_smithy_runtime_api::client::retries::classifiers::SharedRetryClassifier,
        >,
    ) -> &mut Self {
        self.dynamodb_builder
            .set_retry_classifiers(retry_classifiers);
        self
    }

    pub fn app_name(mut self, app_name: aws_types::app_name::AppName) -> Self {
        self.dynamodb_builder = self.dynamodb_builder.app_name(app_name);
        self
    }

    pub fn set_app_name(&mut self, app_name: Option<aws_types::app_name::AppName>) -> &mut Self {
        self.dynamodb_builder.set_app_name(app_name);
        self
    }

    pub fn framework_metadata(
        mut self,
        framework_metadata: aws_sdk_dynamodb::config::FrameworkMetadata,
    ) -> Self {
        self.dynamodb_builder = self.dynamodb_builder.framework_metadata(framework_metadata);
        self
    }

    pub fn push_framework_metadata(
        &mut self,
        framework_metadata: aws_sdk_dynamodb::config::FrameworkMetadata,
    ) -> &mut Self {
        self.dynamodb_builder
            .push_framework_metadata(framework_metadata);
        self
    }

    pub fn disable_clock_skew_correction(
        mut self,
        disable_clock_skew_correction: impl Into<Option<bool>>,
    ) -> Self {
        self.dynamodb_builder = self
            .dynamodb_builder
            .disable_clock_skew_correction(disable_clock_skew_correction);
        self
    }

    pub fn set_disable_clock_skew_correction(
        &mut self,
        disable_clock_skew_correction: Option<bool>,
    ) -> &mut Self {
        self.dynamodb_builder
            .set_disable_clock_skew_correction(disable_clock_skew_correction);
        self
    }

    pub fn invocation_id_generator(
        mut self,
        generator: impl aws_runtime::invocation_id::InvocationIdGenerator + 'static,
    ) -> Self {
        self.dynamodb_builder = self.dynamodb_builder.invocation_id_generator(generator);
        self
    }

    pub fn set_invocation_id_generator(
        &mut self,
        generator: Option<aws_runtime::invocation_id::SharedInvocationIdGenerator>,
    ) -> &mut Self {
        self.dynamodb_builder.set_invocation_id_generator(generator);
        self
    }

    pub fn region(mut self, region: impl Into<Option<aws_sdk_dynamodb::config::Region>>) -> Self {
        self.dynamodb_builder = self.dynamodb_builder.region(region);
        self
    }

    pub fn set_region(&mut self, region: Option<aws_sdk_dynamodb::config::Region>) -> &mut Self {
        self.dynamodb_builder.set_region(region);
        self
    }

    pub fn credentials_provider(
        mut self,
        credentials_provider: impl aws_sdk_dynamodb::config::ProvideCredentials + 'static,
    ) -> Self {
        self.alternator_ext.has_credentials_provider = true;
        self.dynamodb_builder = self
            .dynamodb_builder
            .credentials_provider(credentials_provider);
        self
    }

    pub fn set_credentials_provider(
        &mut self,
        credentials_provider: Option<aws_sdk_dynamodb::config::SharedCredentialsProvider>,
    ) -> &mut Self {
        if credentials_provider.is_some() {
            self.alternator_ext.has_credentials_provider = true;
        }
        self.dynamodb_builder
            .set_credentials_provider(credentials_provider);
        self
    }
}

#[cfg(test)]
mod test {

    use itertools::Itertools;

    use super::*;

    #[test]
    fn config_remembers_builder_and_vice_versa() {
        let config = AlternatorConfig::builder()
            .request_compression(RequestCompression::enabled(
                CompressionAlgorithm::Deflate,
                CompressionLevel::default(),
                0,
            ))
            .user_agent("custom-client/1.2.3")
            .build();

        assert!(config.optimize_headers().is_none());

        assert_eq!(
            config
                .request_compression()
                .expect("request compression is not set"),
            RequestCompression::enabled(
                CompressionAlgorithm::Deflate,
                CompressionLevel::default(),
                0
            )
        );
        assert!(matches!(
            config.user_agent().expect("user agent is not set"),
            UserAgent::Value(value) if value == "custom-client/1.2.3"
        ));

        let config = config.to_builder().build();

        assert!(config.optimize_headers().is_none());

        assert_eq!(
            config
                .request_compression()
                .expect("request compression is not set"),
            RequestCompression::enabled(
                CompressionAlgorithm::Deflate,
                CompressionLevel::default(),
                0
            )
        );
        assert!(matches!(
            config.user_agent().expect("user agent is not set"),
            UserAgent::Value(value) if value == "custom-client/1.2.3"
        ));
    }

    #[test]
    fn config_does_not_add_hooks() {
        let config = AlternatorConfig::builder().optimize_headers(true).build();

        assert!(
            config
                .interceptors()
                .try_len()
                .expect("does not have length")
                == 0
        );
    }

    #[test]
    fn operation_builder_records_fluent_methods() {
        let request_compression = RequestCompression::disabled();
        let response_compression =
            ResponseCompression::enabled(ResponseCompressionAlgorithm::Deflate);

        let builder = AlternatorConfig::operation_builder()
            .request_compression(request_compression.clone())
            .response_compression(response_compression.clone());

        assert_eq!(builder.request_compression, Some(request_compression));
        assert_eq!(builder.response_compression, Some(response_compression));
    }

    #[test]
    fn explicit_builder_credentials_provider_is_remembered() {
        let config = AlternatorConfig::builder()
            .credentials_provider(
                aws_sdk_dynamodb::config::Credentials::for_tests_with_session_token(),
            )
            .build();

        assert!(config.has_credentials_provider());
    }

    #[test]
    fn require_auth_requires_credentials() {
        let config = AlternatorConfig::builder().require_auth().build();

        assert!(config.requires_auth());
    }

    #[test]
    fn set_require_auth_false_restores_implicit_no_auth() {
        let mut builder = AlternatorConfig::builder().require_auth();

        builder.set_require_auth(false);
        let config = builder.build();

        assert!(!config.requires_auth());
    }

    #[test]
    fn allow_no_auth_is_remembered() {
        let config = AlternatorConfig::builder().allow_no_auth().build();

        assert!(config.allows_no_auth());
    }

    #[test]
    #[should_panic(expected = "require_auth() cannot be combined with allow_no_auth()")]
    fn require_auth_after_allow_no_auth_panics() {
        let _ = AlternatorConfig::builder().allow_no_auth().require_auth();
    }

    #[test]
    #[should_panic(expected = "require_auth() cannot be combined with allow_no_auth()")]
    fn allow_no_auth_after_require_auth_panics() {
        let _ = AlternatorConfig::builder().require_auth().allow_no_auth();
    }

    #[test]
    fn from_conf_does_not_panic_without_runtime() {
        let config = AlternatorConfig::builder()
            .seed_hosts(["127.0.0.1"])
            .port(8000)
            .build();
        let _ = AlternatorClient::from_conf(config);
    }

    #[test]
    fn sdk_endpoint_follows_the_seed_hosts() {
        let config = AlternatorConfig::builder()
            .seed_hosts(["127.0.0.1"])
            .port(8000)
            .build();

        assert_eq!(config.seed_hosts(), Some(vec!["127.0.0.1".to_string()]));
        assert_eq!(config.port(), Some(8000));
        assert_eq!(
            config.endpoint_url().as_deref(),
            Some("http://127.0.0.1:8000/")
        );

        // Seed hosts are the only routing configuration, so retargeting is a
        // matter of setting them - and it carries through a to_builder() round
        // trip with no second source to disagree with.
        let retargeted = config
            .to_builder()
            .scheme("https")
            .seed_hosts(["new-cluster", "other-cluster"])
            .port(9000)
            .build();

        assert_eq!(
            retargeted.seed_hosts(),
            Some(vec!["new-cluster".to_string(), "other-cluster".to_string()])
        );
        assert_eq!(retargeted.scheme(), Some("https".to_string()));
        assert_eq!(
            retargeted.endpoint_url().as_deref(),
            Some("https://new-cluster:9000/")
        );
    }

    #[test]
    fn without_discovery_routes_to_the_seed_host() {
        let direct = AlternatorConfig::builder()
            .scheme("https")
            .seed_hosts(["load-balancer.example.com"])
            .port(8043)
            .without_discovery()
            .build();

        assert!(direct.without_discovery());
        assert_eq!(
            direct.endpoint_url().as_deref(),
            Some("https://load-balancer.example.com:8043/")
        );
        assert!(LiveNodes::try_new(&direct).unwrap().is_none());

        // Without a seed host there is nothing to send requests to, so the
        // client fails closed instead of falling back to an AWS endpoint.
        let no_target = AlternatorConfig::builder().without_discovery().build();

        assert_eq!(no_target.endpoint_url(), None);
        assert!(LiveNodes::try_new(&no_target).is_err());
    }

    #[test]
    fn setting_scheme_test() {
        let config = AlternatorConfig::builder().scheme("https://").build();

        assert_eq!(config.scheme(), Some("https".to_string()));

        let config = AlternatorConfig::builder().scheme("http:").build();

        assert_eq!(config.scheme(), Some("http".to_string()));

        let config = AlternatorConfig::builder().scheme("http").build();

        assert_eq!(config.scheme(), Some("http".to_string()));
    }

    #[test]
    fn test_live_nodes_sharing() {
        let config = AlternatorConfig::builder()
            .seed_hosts(["127.0.0.1"])
            .port(8000)
            .build();

        let live_nodes = LiveNodes::new(&config).unwrap();

        // The client can be built with the provided live_nodes instance.
        let client1 = AlternatorClient::from_conf_with_live_nodes(config, live_nodes.clone());

        // live_nodes can also be insterted into a new config.
        let config2 = AlternatorConfig::builder()
            .live_nodes(live_nodes.clone())
            .build();

        let client2 = AlternatorClient::from_conf(config2);

        // Assert that they point to the exact same Arc.
        assert!(std::sync::Arc::ptr_eq(
            &client1.config().live_nodes().unwrap(),
            &client2.config().live_nodes().unwrap()
        ));
    }

    #[test]
    fn discovery_setters_invalidate_auto_created_live_nodes() {
        let client = AlternatorClient::from_conf(
            AlternatorConfig::builder()
                .seed_hosts(["127.0.0.1"])
                .port(8000)
                .build(),
        );
        let original = client.config().live_nodes().unwrap();

        let rebuilt_configs = [
            client
                .config()
                .to_builder()
                .active_interval(std::time::Duration::from_secs(2))
                .build(),
            client
                .config()
                .to_builder()
                .idle_interval(std::time::Duration::from_secs(120))
                .build(),
            client
                .config()
                .to_builder()
                .routing_scope(RoutingScope::from_datacenter("dc1".to_string()))
                .build(),
            client.config().to_builder().scheme("https").build(),
            client.config().to_builder().port(9000).build(),
            client
                .config()
                .to_builder()
                .seed_hosts(["127.0.0.2"])
                .port(9000)
                .build(),
        ];

        for rebuilt_config in rebuilt_configs {
            assert!(rebuilt_config.live_nodes().is_none());
            let rebuilt_client = AlternatorClient::from_conf(rebuilt_config);
            let rebuilt = rebuilt_client.config().live_nodes().unwrap();
            assert!(!std::sync::Arc::ptr_eq(&original, &rebuilt));
        }
    }

    #[test]
    fn client_derived_config_retargets_or_disables_discovery() {
        let client = AlternatorClient::from_conf(
            AlternatorConfig::builder()
                .seed_hosts(["127.0.0.1"])
                .port(8000)
                .build(),
        );

        let retargeted = AlternatorClient::from_conf(
            client
                .config()
                .to_builder()
                .seed_hosts(["127.0.0.2"])
                .port(9000)
                .build(),
        );
        let retargeted_nodes = retargeted.config().live_nodes().unwrap().get_live_nodes();
        assert_eq!(retargeted_nodes[0].as_str(), "http://127.0.0.2:9000/");

        let direct = AlternatorClient::from_conf(
            client
                .config()
                .to_builder()
                .seed_hosts(["load-balancer.example.com"])
                .port(8043)
                .without_discovery()
                .build(),
        );
        assert!(direct.config().live_nodes().is_none());
    }

    #[test]
    fn discovery_setters_preserve_explicitly_shared_live_nodes() {
        let discovery_config = AlternatorConfig::builder()
            .seed_hosts(["127.0.0.1"])
            .port(8000)
            .build();
        let shared = LiveNodes::new(&discovery_config).unwrap();
        let client = AlternatorClient::from_conf_with_live_nodes(discovery_config, shared.clone());

        let rebuilt_config = client
            .config()
            .to_builder()
            .active_interval(std::time::Duration::from_secs(2))
            .idle_interval(std::time::Duration::from_secs(120))
            .routing_scope(RoutingScope::from_datacenter("dc1".to_string()))
            .scheme("https")
            .port(9000)
            .seed_hosts(["127.0.0.2"])
            .build();

        assert!(std::sync::Arc::ptr_eq(
            &shared,
            &rebuilt_config.live_nodes().unwrap()
        ));
        let rebuilt_client = AlternatorClient::from_conf(rebuilt_config);
        assert!(std::sync::Arc::ptr_eq(
            &shared,
            &rebuilt_client.config().live_nodes().unwrap()
        ));
    }

    #[test]
    fn config_remembers_response_compression() {
        let config = AlternatorConfig::builder()
            .response_compression(ResponseCompression::enabled(
                ResponseCompressionAlgorithm::Gzip,
            ))
            .build();

        assert_eq!(
            config
                .response_compression()
                .expect("response_compression not set"),
            ResponseCompression::enabled(ResponseCompressionAlgorithm::Gzip)
        );

        // round-trip through to_builder
        let config = config.to_builder().build();

        assert_eq!(
            config
                .response_compression()
                .expect("response_compression not set after round-trip"),
            ResponseCompression::enabled(ResponseCompressionAlgorithm::Gzip)
        );
    }

    #[test]
    fn config_response_compression_disabled_roundtrip() {
        let config = AlternatorConfig::builder()
            .response_compression(ResponseCompression::disabled())
            .build();

        assert_eq!(
            config
                .response_compression()
                .expect("response_compression not set"),
            ResponseCompression::disabled()
        );

        let config = config.to_builder().build();

        assert_eq!(
            config
                .response_compression()
                .expect("response_compression not set after round-trip"),
            ResponseCompression::disabled()
        );
    }

    #[test]
    fn config_response_compression_unset_is_none() {
        let config = AlternatorConfig::builder().build();

        assert!(config.response_compression().is_none());
    }

    #[test]
    fn response_compression_default_is_disabled() {
        let rc = ResponseCompression::default();
        assert_eq!(rc.get(), None);
    }

    #[test]
    fn response_compression_enabled_many() {
        let rc = ResponseCompression::enabled_many([
            ResponseCompressionAlgorithm::Gzip,
            ResponseCompressionAlgorithm::Deflate,
        ]);
        assert_eq!(
            rc.get(),
            Some(
                [
                    ResponseCompressionAlgorithm::Gzip,
                    ResponseCompressionAlgorithm::Deflate,
                ]
                .as_slice()
            )
        );
    }
}
