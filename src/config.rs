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
    pub(crate) credentials_provider: Option<aws_sdk_dynamodb::config::SharedCredentialsProvider>,
    pub(crate) require_auth: bool,
    pub(crate) allow_no_auth: bool,
    pub(crate) active_interval: Option<std::time::Duration>,
    pub(crate) idle_interval: Option<std::time::Duration>,
    pub(crate) routing_scope: Option<RoutingScope>,
    pub(crate) scheme: Option<String>,
    pub(crate) port: Option<u16>,
    pub(crate) seed_hosts: Option<Vec<String>>,
    /// Whether to send every request straight to the configured seed host
    /// instead of discovering cluster topology through it.
    pub(crate) without_discovery: bool,
    pub(crate) key_route_affinity: Option<KeyRouteAffinityConfig>,
    pub(crate) stalled_stream_protection_explicitly_unset: bool,
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
}

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
/// This type stores declarative settings only. When discovery is enabled, each
/// [`AlternatorClient`] constructed from a config creates and owns its private
/// discovery state and background refresh task.
///
/// It is used to construct [AlternatorClient] like so:
///
/// ```
/// use alternator_driver::{AlternatorClient, AlternatorConfig};
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
    /// Creates a builder for a client-wide Alternator configuration.
    ///
    /// The builder starts without imported shared AWS SDK settings. Configure
    /// the supported settings explicitly before calling
    /// [`AlternatorBuilder::build`].
    pub fn builder() -> AlternatorBuilder {
        AlternatorBuilder::default()
    }

    /// Creates a builder for compression overrides on one operation.
    ///
    /// Pass the result to
    /// [`AlternatorCustomizableOperation::alternator_config_override`].
    pub fn operation_builder() -> AlternatorOperationBuilder {
        AlternatorOperationBuilder::default()
    }

    /// Converts this configuration into a builder for further changes.
    ///
    /// Both the underlying AWS SDK settings and all Alternator-specific
    /// settings are preserved.
    pub fn to_builder(&self) -> AlternatorBuilder {
        AlternatorBuilder {
            dynamodb_builder: self.dynamodb_config.to_builder(),
            alternator_ext: self.alternator_ext.clone(),
        }
    }

    /// Before sending each request, apply the compact header allowlist.
    ///
    /// This is done by an interceptor in `modify_before_transmit` hook.
    ///
    /// Take note, that this may break your own interceptors,
    /// if they happened to look inside these headers after this happens.
    ///
    /// Signed requests retain every header named by the actual SigV4
    /// `SignedHeaders` value, including custom headers added before signing. If
    /// the authorization format cannot be parsed, the request is left intact.
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

    /// Configures response compression negotiation and decompression limits.
    ///
    /// Accepted encodings control what the client requests from the server. Supported
    /// `Content-Encoding` values are still decompressed when negotiation is disabled,
    /// and the configured decompression limits still apply.
    ///
    /// Not set by default (negotiation disabled and default limits used).
    pub fn response_compression(&self) -> Option<ResponseCompression> {
        self.alternator_ext.response_compression.clone()
    }

    pub(crate) fn credentials_provider(
        &self,
    ) -> Option<aws_sdk_dynamodb::config::SharedCredentialsProvider> {
        self.alternator_ext.credentials_provider.clone()
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
    /// Requests are routed to the most preferred scope containing discovered nodes.
    ///
    /// If this is not provided, the client will use the cluster scope, meaning load balancing will happen across topology nodes in all discovered datacenters.
    /// One working seed can discover the full cluster topology; additional
    /// seeds provide startup and refresh resilience.
    ///
    /// Keep in mind that subsequent fallback scope should ideally be broader than or equal to the
    /// previous one, e.g., (rack -> datacenter -> cluster) or (rack -> another rack -> datacenter -> cluster).
    /// Making a fallback narrower, e.g., (datacenter -> rack) or (cluster -> datacenter),
    /// may be redundant if the set of nodes in the next scope is a subset of the previous one.
    pub fn routing_scope(&self) -> Option<RoutingScope> {
        self.alternator_ext.routing_scope.clone()
    }

    /// Gets the configured URI scheme.
    ///
    /// Discovery and the built-in HTTP client support `http` and `https`.
    /// Direct routing may use another valid URI scheme when a custom HTTP
    /// client that supports it is configured.
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
    /// With discovery on, requests are rewritten to the topology node chosen for
    /// them, so this only decides where a request goes when routing is turned
    /// off through [`AlternatorBuilder::without_discovery`].
    pub fn endpoint_url(&self) -> Option<String> {
        self.alternator_ext.endpoint_url()
    }

    /// Gets the key route affinity configuration.
    ///
    /// For more information see [`KeyRouteAffinityConfig`] and [`KeyRouteAffinityType`].
    pub fn key_route_affinity(&self) -> Option<KeyRouteAffinityConfig> {
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
    /// Creates an empty per-operation compression override.
    pub fn new() -> Self {
        Self::default()
    }

    /// Enable / disable request compression for this request.
    pub fn request_compression(mut self, request_compression: RequestCompression) -> Self {
        self.request_compression = Some(request_compression);
        self
    }

    /// Configure response compression negotiation and decompression limits for this request.
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
    /// Creates a builder with default Alternator and AWS SDK settings.
    ///
    /// Unlike a builder derived from [`AlternatorConfig::to_builder`], this
    /// does not inherit settings from an existing configuration.
    pub fn new() -> Self {
        Self::default()
    }

    /// Builds an [`AlternatorConfig`] from the configured settings.
    ///
    /// The AWS SDK behavior version is pinned to the version validated by this
    /// driver, and the SDK endpoint URL is derived from the first configured
    /// seed host, scheme, and port.
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

    /// Before sending each request, apply the compact header allowlist.
    ///
    /// This is done by an interceptor in `modify_before_transmit` hook.
    ///
    /// Take note, that this may break your own interceptors,
    /// if they happened to look inside these headers after this happens.
    ///
    /// Signed requests retain every header named by the actual SigV4
    /// `SignedHeaders` value, including custom headers added before signing. If
    /// the authorization format cannot be parsed, the request is left intact.
    ///
    /// Turned on by default.
    pub fn optimize_headers(mut self, optimize: bool) -> Self {
        self.set_optimize_headers(optimize);
        self
    }

    /// Sets whether to strip headers before transmission.
    ///
    /// See [`Self::optimize_headers`] for SigV4 restrictions. This setting is
    /// on by default.
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

    /// Configure response compression negotiation and decompression limits.
    ///
    /// Accepted encodings control what the client requests from the server. Supported
    /// `Content-Encoding` values are still decompressed when negotiation is disabled,
    /// and the configured decompression limits still apply.
    ///
    /// Not set by default (negotiation disabled and default limits used).
    pub fn response_compression(mut self, response_compression: ResponseCompression) -> Self {
        self.set_response_compression(response_compression);
        self
    }

    /// Configure response compression negotiation and decompression limits.
    ///
    /// Accepted encodings control what the client requests from the server. Supported
    /// `Content-Encoding` values are still decompressed when negotiation is disabled,
    /// and the configured decompression limits still apply.
    ///
    /// Not set by default (negotiation disabled and default limits used).
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
    /// Requests are routed to the most preferred scope containing discovered nodes.
    ///
    /// If this is not provided, the client will use the cluster scope, meaning load balancing will happen across topology nodes in all discovered datacenters.
    /// One working seed can discover the full cluster topology; additional
    /// seeds provide startup and refresh resilience.
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
    /// Requests are routed to the most preferred scope containing discovered nodes.
    ///
    /// If this is not provided, the client will use the cluster scope, meaning load balancing will happen across topology nodes in all discovered datacenters.
    /// One working seed can discover the full cluster topology; additional
    /// seeds provide startup and refresh resilience.
    ///
    /// Keep in mind that subsequent fallback scope should ideally be broader than or equal to the
    /// previous one, e.g., (rack -> datacenter -> cluster) or (rack -> another rack -> datacenter -> cluster).
    /// Making a fallback narrower, e.g., (datacenter -> rack) or (cluster -> datacenter),
    /// may be redundant if the set of nodes in the next scope is a subset of the previous one.
    pub fn set_routing_scope(&mut self, routing_scope: RoutingScope) -> &mut Self {
        self.alternator_ext.routing_scope = Some(routing_scope);
        self
    }

    /// Sets the URI scheme.
    ///
    /// Accepts a bare URI scheme, optionally followed by `:` or `://`, and
    /// stores the bare scheme. Discovery and the built-in HTTP client support
    /// `http` and `https`. With
    /// [`without_discovery`](Self::without_discovery) and a custom HTTP client,
    /// any syntactically valid URI scheme is accepted.
    ///
    /// Malformed schemes and schemes unsupported by the selected routing and
    /// HTTP-client configuration are rejected when constructing an
    /// [`AlternatorClient`].
    pub fn scheme(mut self, scheme: impl Into<String>) -> Self {
        self.set_scheme(scheme);
        self
    }

    /// Sets the URI scheme.
    ///
    /// See [`AlternatorBuilder::scheme`].
    pub fn set_scheme(&mut self, scheme: impl Into<String>) -> &mut Self {
        let s = scheme.into();

        // Accept only the two documented wrappers. Repeated or partial
        // delimiters must remain visible so construction can reject them as a
        // malformed scheme instead of silently repairing them.
        let normalized = s
            .strip_suffix("://")
            .or_else(|| s.strip_suffix(':'))
            .unwrap_or(&s)
            .to_string();
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
        self.alternator_ext.seed_hosts = Some(seed_hosts);
        self
    }

    /// Send every request straight to the configured seed host instead of
    /// discovering cluster topology through it.
    ///
    /// Use this when a proxy or an external load balancer sits in front of the
    /// cluster and is the only address this client should talk to. Requests go
    /// to the first seed host, with the configured
    /// [`scheme`](AlternatorBuilder::scheme) and [`port`](AlternatorBuilder::port),
    /// and no system-table discovery runs. Without a seed host to send them
    /// to, building a client fails rather than routing anywhere unintended.
    pub fn without_discovery(mut self) -> Self {
        self.set_without_discovery(true);
        self
    }

    /// Sets whether every request goes straight to the configured seed host.
    ///
    /// Passing `true` disables discovery as described by
    /// [`AlternatorBuilder::without_discovery`]. Passing `false` enables
    /// discovery again, which is useful when rebuilding a direct-routing
    /// configuration through [`AlternatorConfig::to_builder`].
    pub fn set_without_discovery(&mut self, without_discovery: bool) -> &mut Self {
        self.alternator_ext.without_discovery = without_discovery;
        self
    }
    /// Sets the key route affinity configuration.
    ///
    /// Use it either with a pre-constructed [`KeyRouteAffinityConfig`]
    /// or with a [`KeyRouteAffinityType`] for simpler
    /// use cases. Calling with
    /// [`KeyRouteAffinityType::None`] is equivalent
    /// to not setting the affinity at all.
    ///
    /// For more information, see
    /// [`KeyRouteAffinityConfig`] and [`KeyRouteAffinityType`].
    pub fn key_route_affinity(
        mut self,
        key_route_affinity: impl Into<KeyRouteAffinityConfig>,
    ) -> Self {
        self.set_key_route_affinity(key_route_affinity.into());
        self
    }

    /// Sets the key route affinity configuration.
    ///
    /// Use it either with a pre-constructed [`KeyRouteAffinityConfig`]
    /// or with a [`KeyRouteAffinityType`] for simpler
    /// use cases. Calling with
    /// [`KeyRouteAffinityType::None`] is equivalent
    /// to not setting the affinity at all.
    ///
    /// For more information, see
    /// [`KeyRouteAffinityConfig`] and [`KeyRouteAffinityType`].
    pub fn set_key_route_affinity(
        &mut self,
        key_route_affinity: impl Into<KeyRouteAffinityConfig>,
    ) -> &mut Self {
        self.alternator_ext.key_route_affinity = Some(key_route_affinity.into());
        self
    }
}

// All implementations below this point are supported AWS SDK passthroughs.

impl AlternatorConfig {
    /// Returns the configured stalled-stream protection settings, if any.
    ///
    /// When this is [`None`], the pinned AWS SDK behavior version supplies its
    /// default settings unless the builder explicitly removed them.
    pub fn stalled_stream_protection(
        &self,
    ) -> Option<&aws_sdk_dynamodb::config::StalledStreamProtectionConfig> {
        self.dynamodb_config.stalled_stream_protection()
    }

    /// Returns the HTTP client used for both Alternator API requests and
    /// topology discovery.
    pub fn http_client(&self) -> Option<aws_sdk_dynamodb::config::SharedHttpClient> {
        self.dynamodb_config.http_client()
    }

    /// Returns the configured request retry policy, if one was provided.
    pub fn retry_config(&self) -> Option<&aws_smithy_types::retry::RetryConfig> {
        self.dynamodb_config.retry_config()
    }

    /// Returns the configured asynchronous sleep implementation, if any.
    ///
    /// The SDK uses this implementation for delays such as retry backoff.
    pub fn sleep_impl(&self) -> Option<aws_sdk_dynamodb::config::SharedAsyncSleep> {
        self.dynamodb_config.sleep_impl()
    }

    /// Returns the configured operation and connection timeouts, if any.
    pub fn timeout_config(&self) -> Option<&aws_smithy_types::timeout::TimeoutConfig> {
        self.dynamodb_config.timeout_config()
    }

    /// Returns the explicitly configured partition for retry-related state.
    ///
    /// Default partitions with the same name share retry token buckets and
    /// adaptive rate limiters. Custom partitions share that state only when
    /// the partition or its components are cloned. When absent, the SDK
    /// creates its service default.
    pub fn retry_partition(&self) -> Option<&aws_smithy_runtime::client::retries::RetryPartition> {
        self.dynamodb_config.retry_partition()
    }

    /// Returns the identity cache used during authentication, if configured.
    pub fn identity_cache(&self) -> Option<aws_sdk_dynamodb::config::SharedIdentityCache> {
        self.dynamodb_config.identity_cache()
    }

    /// Iterates over user-configured AWS SDK interceptors.
    ///
    /// Driver-owned routing and request-processing interceptors are installed
    /// when an [`AlternatorClient`] is constructed and are not included here.
    pub fn interceptors(
        &self,
    ) -> impl Iterator<Item = aws_sdk_dynamodb::config::SharedInterceptor> {
        self.dynamodb_config.interceptors()
    }

    /// Returns the time source used by the AWS SDK runtime, if configured.
    pub fn time_source(&self) -> Option<aws_smithy_async::time::SharedTimeSource> {
        self.dynamodb_config.time_source()
    }

    /// Iterates over the user-configured retry classifiers.
    pub fn retry_classifiers(
        &self,
    ) -> impl Iterator<Item = aws_smithy_runtime_api::client::retries::classifiers::SharedRetryClassifier>
    {
        self.dynamodb_config.retry_classifiers()
    }

    /// Returns the application name stored in the underlying SDK config.
    ///
    /// This metadata identifies an application in SDK-generated user-agent
    /// data. [`AlternatorBuilder::user_agent`] controls the final HTTP
    /// `User-Agent` header sent by this driver.
    pub fn app_name(&self) -> Option<&aws_types::app_name::AppName> {
        self.dynamodb_config.app_name()
    }

    /// Returns framework metadata stored in the underlying SDK config.
    ///
    /// Entries are returned in first-seen order. This metadata supports SDK
    /// integrations; [`AlternatorBuilder::user_agent`] controls the final HTTP
    /// `User-Agent` header sent by this driver.
    pub fn framework_metadata(&self) -> Vec<&aws_sdk_dynamodb::config::FrameworkMetadata> {
        self.dynamodb_config.framework_metadata()
    }

    /// Returns whether SDK clock-skew correction was explicitly disabled.
    ///
    /// [`None`] means the pinned AWS SDK behavior default applies.
    pub fn disable_clock_skew_correction(&self) -> Option<bool> {
        self.dynamodb_config.disable_clock_skew_correction()
    }

    /// Returns the configured SDK invocation ID generator, if any.
    ///
    /// Invocation IDs populate the `amz-sdk-invocation-id` request header.
    pub fn invocation_id_generator(
        &self,
    ) -> Option<aws_runtime::invocation_id::SharedInvocationIdGenerator> {
        self.dynamodb_config.invocation_id_generator()
    }

    /// Returns the SigV4 service name used in credential scopes.
    ///
    /// DynamoDB and Alternator requests use `dynamodb`.
    pub fn signing_name(&self) -> &'static str {
        self.dynamodb_config.signing_name()
    }

    /// Returns the configured signing region, if one was provided.
    ///
    /// Client construction supplies `us-east-1` when no region is configured,
    /// because SigV4 requires a region even though Alternator routing does not.
    pub fn region(&self) -> Option<&aws_sdk_dynamodb::config::Region> {
        self.dynamodb_config.region()
    }
}

impl AlternatorBuilder {
    /// Captures named operation-input members and emits their values as labels
    /// on the AWS SDK's built-in metrics.
    ///
    /// Emitted attributes are also available for in-process inspection.
    /// Names must identify non-sensitive, string-valued Smithy input members;
    /// unsupported names have no effect. Avoid high-cardinality values because
    /// each distinct value creates another metric label value.
    pub fn emit_input_attributes(
        mut self,
        names: impl IntoIterator<Item = impl Into<String>>,
    ) -> Self {
        self.dynamodb_builder = self.dynamodb_builder.emit_input_attributes(names);
        self
    }

    /// Captures named operation-input members for in-process inspection
    /// without emitting them as metric labels.
    ///
    /// Names follow the eligibility rules described by
    /// [`Self::emit_input_attributes`]. This is suitable for high-cardinality
    /// values that should not become metric labels.
    pub fn capture_input_attributes(
        mut self,
        names: impl IntoIterator<Item = impl Into<String>>,
    ) -> Self {
        self.dynamodb_builder = self.dynamodb_builder.capture_input_attributes(names);
        self
    }

    /// Configures AWS SDK protection against stalled upload and download streams.
    ///
    /// This replaces the settings supplied by the driver's pinned SDK behavior
    /// version.
    pub fn stalled_stream_protection(
        mut self,
        stalled_stream_protection_config: aws_sdk_dynamodb::config::StalledStreamProtectionConfig,
    ) -> Self {
        self.set_stalled_stream_protection(Some(stalled_stream_protection_config));
        self
    }

    /// Sets or explicitly removes stalled-stream protection settings.
    ///
    /// Passing [`Some`] overrides the pinned AWS SDK default. Passing [`None`]
    /// explicitly removes that required default, so constructing an
    /// [`AlternatorClient`] from the resulting config returns an error (or
    /// panics through [`AlternatorClient::from_conf`]) unless a replacement is
    /// subsequently configured.
    pub fn set_stalled_stream_protection(
        &mut self,
        stalled_stream_protection_config: Option<
            aws_sdk_dynamodb::config::StalledStreamProtectionConfig,
        >,
    ) -> &mut Self {
        self.alternator_ext
            .stalled_stream_protection_explicitly_unset =
            stalled_stream_protection_config.is_none();
        self.dynamodb_builder
            .set_stalled_stream_protection(stalled_stream_protection_config);
        self
    }

    /// Configures the HTTP client used for both Alternator API requests and
    /// topology discovery.
    ///
    /// This lets custom transport settings, including TLS trust stores and
    /// client certificates, apply consistently to topology discovery scans.
    pub fn http_client(
        mut self,
        http_client: impl aws_sdk_dynamodb::config::HttpClient + 'static,
    ) -> Self {
        self.dynamodb_builder = self.dynamodb_builder.http_client(http_client);
        self
    }

    /// Sets the HTTP client used for both Alternator API requests and
    /// topology discovery.
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

    /// Configures the AWS SDK retry mode, maximum attempts, and backoff.
    ///
    /// Retry token-bucket and adaptive rate-limiter state are controlled
    /// separately by [`Self::retry_partition`].
    pub fn retry_config(mut self, retry_config: aws_smithy_types::retry::RetryConfig) -> Self {
        self.dynamodb_builder = self.dynamodb_builder.retry_config(retry_config);
        self
    }

    /// Applies an AWS SDK retry configuration when one is provided.
    ///
    /// Passing [`None`] has no effect; it does not remove a configuration that
    /// was set earlier on this builder.
    pub fn set_retry_config(
        &mut self,
        retry_config: Option<aws_smithy_types::retry::RetryConfig>,
    ) -> &mut Self {
        self.dynamodb_builder.set_retry_config(retry_config);
        self
    }

    /// Configures the asynchronous sleep implementation used by the AWS SDK.
    ///
    /// The SDK uses this implementation for delays such as retry backoff.
    pub fn sleep_impl(
        mut self,
        sleep_impl: impl aws_sdk_dynamodb::config::AsyncSleep + 'static,
    ) -> Self {
        self.dynamodb_builder = self.dynamodb_builder.sleep_impl(sleep_impl);
        self
    }

    /// Sets or removes the custom asynchronous sleep implementation.
    ///
    /// Passing [`None`] clears the explicitly configured implementation,
    /// allowing the pinned AWS SDK behavior default to apply.
    pub fn set_sleep_impl(
        &mut self,
        sleep_impl: Option<aws_sdk_dynamodb::config::SharedAsyncSleep>,
    ) -> &mut Self {
        self.dynamodb_builder.set_sleep_impl(sleep_impl);
        self
    }

    /// Configures AWS SDK operation and connection timeouts.
    pub fn timeout_config(
        mut self,
        timeout_config: aws_smithy_types::timeout::TimeoutConfig,
    ) -> Self {
        self.dynamodb_builder = self.dynamodb_builder.timeout_config(timeout_config);
        self
    }

    /// Applies AWS SDK timeout settings when one is provided.
    ///
    /// Passing [`None`] has no effect. To turn off timeouts, pass
    /// [`aws_smithy_types::timeout::TimeoutConfig::disabled`] inside [`Some`].
    pub fn set_timeout_config(
        &mut self,
        timeout_config: Option<aws_smithy_types::timeout::TimeoutConfig>,
    ) -> &mut Self {
        self.dynamodb_builder.set_timeout_config(timeout_config);
        self
    }

    /// Sets the partition that owns shared retry state.
    ///
    /// Default partitions with the same name share retry token buckets and
    /// adaptive rate limiters. Separately built custom partitions remain
    /// isolated even when their names match; clone a custom partition or its
    /// components to share that state. Most clients can use the SDK's default
    /// DynamoDB partition.
    pub fn retry_partition(
        mut self,
        retry_partition: aws_smithy_runtime::client::retries::RetryPartition,
    ) -> Self {
        self.dynamodb_builder = self.dynamodb_builder.retry_partition(retry_partition);
        self
    }

    /// Applies a retry-state partition when one is provided.
    ///
    /// Passing [`None`] has no effect; it does not remove a partition that was
    /// set earlier on this builder.
    pub fn set_retry_partition(
        &mut self,
        retry_partition: Option<aws_smithy_runtime::client::retries::RetryPartition>,
    ) -> &mut Self {
        self.dynamodb_builder.set_retry_partition(retry_partition);
        self
    }

    /// Configures the cache used to resolve authentication identities.
    ///
    /// The SDK normally uses a lazy cache. Pass an alternative implementation
    /// to change caching behavior, including an SDK-provided no-cache
    /// implementation when credentials must be resolved for every request.
    pub fn identity_cache(
        mut self,
        identity_cache: impl aws_sdk_dynamodb::config::ResolveCachedIdentity + 'static,
    ) -> Self {
        self.dynamodb_builder = self.dynamodb_builder.identity_cache(identity_cache);
        self
    }

    /// Replaces the cache used to resolve authentication identities.
    ///
    /// This mutable-builder form has the same behavior as
    /// [`Self::identity_cache`].
    pub fn set_identity_cache(
        &mut self,
        identity_cache: impl aws_sdk_dynamodb::config::ResolveCachedIdentity + 'static,
    ) -> &mut Self {
        self.dynamodb_builder.set_identity_cache(identity_cache);
        self
    }

    /// Adds an AWS SDK interceptor to the request execution pipeline.
    ///
    /// User interceptors run alongside the driver-owned routing, compression,
    /// decompression, user-agent, and header-optimization interceptors that are
    /// installed when the client is constructed.
    pub fn interceptor(
        mut self,
        interceptor: impl aws_sdk_dynamodb::config::Intercept + 'static,
    ) -> Self {
        self.dynamodb_builder = self.dynamodb_builder.interceptor(interceptor);
        self
    }

    /// Adds an already shared AWS SDK interceptor.
    ///
    /// This is the mutable-builder counterpart to [`Self::interceptor`].
    pub fn push_interceptor(
        &mut self,
        interceptor: aws_sdk_dynamodb::config::SharedInterceptor,
    ) -> &mut Self {
        self.dynamodb_builder.push_interceptor(interceptor);
        self
    }

    /// Replaces all user-configured AWS SDK interceptors with `interceptors`.
    ///
    /// Driver-owned interceptors are installed later during client
    /// construction and are not replaced by this method.
    pub fn set_interceptors(
        &mut self,
        interceptors: impl IntoIterator<Item = aws_sdk_dynamodb::config::SharedInterceptor>,
    ) -> &mut Self {
        self.dynamodb_builder.set_interceptors(interceptors);
        self
    }

    /// Configures the time source used by the AWS SDK runtime.
    ///
    /// Custom time sources are primarily useful for deterministic tests.
    pub fn time_source(
        mut self,
        time_source: impl aws_smithy_async::time::TimeSource + 'static,
    ) -> Self {
        self.dynamodb_builder = self.dynamodb_builder.time_source(time_source);
        self
    }

    /// Sets or removes the custom AWS SDK time source.
    ///
    /// Passing [`None`] clears the explicitly configured source, allowing the
    /// pinned AWS SDK behavior default to apply.
    pub fn set_time_source(
        &mut self,
        time_source: Option<aws_smithy_async::time::SharedTimeSource>,
    ) -> &mut Self {
        self.dynamodb_builder.set_time_source(time_source);
        self
    }

    /// Adds a classifier that decides whether operation results are retryable.
    ///
    /// Classifiers run according to their AWS SDK retry-classifier priority.
    pub fn retry_classifier(
        mut self,
        retry_classifier: impl aws_smithy_runtime_api::client::retries::classifiers::ClassifyRetry
        + 'static,
    ) -> Self {
        self.dynamodb_builder = self.dynamodb_builder.retry_classifier(retry_classifier);
        self
    }

    /// Adds an already shared retry classifier.
    ///
    /// This is the mutable-builder counterpart to [`Self::retry_classifier`].
    pub fn push_retry_classifier(
        &mut self,
        retry_classifier: aws_smithy_runtime_api::client::retries::classifiers::SharedRetryClassifier,
    ) -> &mut Self {
        self.dynamodb_builder
            .push_retry_classifier(retry_classifier);
        self
    }

    /// Replaces all user-configured retry classifiers with `retry_classifiers`.
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

    /// Sets application metadata in the underlying AWS SDK configuration.
    ///
    /// The SDK uses this value when constructing SDK user-agent metadata.
    /// [`Self::user_agent`] controls the final HTTP `User-Agent` header sent by
    /// this driver.
    pub fn app_name(mut self, app_name: aws_types::app_name::AppName) -> Self {
        self.dynamodb_builder = self.dynamodb_builder.app_name(app_name);
        self
    }

    /// Sets or removes application metadata in the underlying SDK config.
    ///
    /// Passing [`None`] removes a name set earlier on this builder.
    pub fn set_app_name(&mut self, app_name: Option<aws_types::app_name::AppName>) -> &mut Self {
        self.dynamodb_builder.set_app_name(app_name);
        self
    }

    /// Appends framework metadata to the underlying AWS SDK configuration.
    ///
    /// SDK integrations use entries to identify frameworks or libraries.
    /// Multiple calls append entries; the SDK de-duplicates equal name and
    /// version pairs. [`Self::user_agent`] controls the final HTTP `User-Agent`
    /// header sent by this driver.
    pub fn framework_metadata(
        mut self,
        framework_metadata: aws_sdk_dynamodb::config::FrameworkMetadata,
    ) -> Self {
        self.dynamodb_builder = self.dynamodb_builder.framework_metadata(framework_metadata);
        self
    }

    /// Appends framework metadata using a mutable builder reference.
    ///
    /// See [`Self::framework_metadata`] for ordering and user-agent behavior.
    pub fn push_framework_metadata(
        &mut self,
        framework_metadata: aws_sdk_dynamodb::config::FrameworkMetadata,
    ) -> &mut Self {
        self.dynamodb_builder
            .push_framework_metadata(framework_metadata);
        self
    }

    /// Sets whether the AWS SDK should skip clock-skew correction.
    ///
    /// Pass `true` to disable correction, `false` to explicitly enable it, or
    /// [`None`] to clear an earlier explicit choice and use the SDK default.
    pub fn disable_clock_skew_correction(
        mut self,
        disable_clock_skew_correction: impl Into<Option<bool>>,
    ) -> Self {
        self.dynamodb_builder = self
            .dynamodb_builder
            .disable_clock_skew_correction(disable_clock_skew_correction);
        self
    }

    /// Sets or clears whether the AWS SDK should skip clock-skew correction.
    ///
    /// Passing [`None`] restores the pinned AWS SDK behavior default.
    pub fn set_disable_clock_skew_correction(
        &mut self,
        disable_clock_skew_correction: Option<bool>,
    ) -> &mut Self {
        self.dynamodb_builder
            .set_disable_clock_skew_correction(disable_clock_skew_correction);
        self
    }

    /// Overrides the generator for `amz-sdk-invocation-id` header values.
    ///
    /// The SDK uses random UUIDs by default. A deterministic generator can be
    /// useful when testing emitted HTTP requests.
    pub fn invocation_id_generator(
        mut self,
        generator: impl aws_runtime::invocation_id::InvocationIdGenerator + 'static,
    ) -> Self {
        self.dynamodb_builder = self.dynamodb_builder.invocation_id_generator(generator);
        self
    }

    /// Sets or removes a custom SDK invocation ID generator.
    ///
    /// Passing [`None`] restores the SDK's default random generator.
    pub fn set_invocation_id_generator(
        &mut self,
        generator: Option<aws_runtime::invocation_id::SharedInvocationIdGenerator>,
    ) -> &mut Self {
        self.dynamodb_builder.set_invocation_id_generator(generator);
        self
    }

    /// Sets or clears the region used for SigV4 request signing.
    ///
    /// The value may be a region directly or an [`Option`] containing one.
    /// When no region remains configured, client construction supplies
    /// `us-east-1` because SigV4 requires a region even though Alternator
    /// routing does not.
    pub fn region(mut self, region: impl Into<Option<aws_sdk_dynamodb::config::Region>>) -> Self {
        self.dynamodb_builder = self.dynamodb_builder.region(region);
        self
    }

    /// Sets or clears the region used for SigV4 request signing.
    ///
    /// Passing [`None`] removes an explicit region; client construction then
    /// supplies `us-east-1`.
    pub fn set_region(&mut self, region: Option<aws_sdk_dynamodb::config::Region>) -> &mut Self {
        self.dynamodb_builder.set_region(region);
        self
    }

    /// Configures the default credentials provider used to sign requests.
    ///
    /// When header optimization is enabled, protected headers are derived from
    /// the request's actual SigV4 `SignedHeaders` value.
    pub fn credentials_provider(
        mut self,
        credentials_provider: impl aws_sdk_dynamodb::config::ProvideCredentials + 'static,
    ) -> Self {
        self.alternator_ext.credentials_provider = Some(
            aws_sdk_dynamodb::config::SharedCredentialsProvider::new(credentials_provider),
        );
        self
    }

    /// Sets or removes the default credentials provider.
    ///
    /// Passing `None` removes a previously configured provider. Unless
    /// [`require_auth`](Self::require_auth) is enabled, clients built from the
    /// resulting configuration permit unsigned requests automatically.
    /// When header optimization is enabled, protected headers are derived from
    /// the request's actual SigV4 `SignedHeaders` value.
    pub fn set_credentials_provider(
        &mut self,
        credentials_provider: Option<aws_sdk_dynamodb::config::SharedCredentialsProvider>,
    ) -> &mut Self {
        self.alternator_ext.credentials_provider = credentials_provider;
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

        assert!(config.credentials_provider().is_some());
    }

    #[test]
    fn removing_credentials_provider_clears_the_provider_marker() {
        let config = AlternatorConfig::builder()
            .credentials_provider(
                aws_sdk_dynamodb::config::Credentials::for_tests_with_session_token(),
            )
            .build();
        let mut builder = config.to_builder();

        builder.set_credentials_provider(None);
        let rebuilt = builder.build();

        assert!(rebuilt.credentials_provider().is_none());
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

        // A scheme shaped like a URL would supply the authority of the
        // formatted endpoint, so it yields no endpoint instead of one pointing
        // somewhere else entirely.
        let url_shaped_scheme = config
            .to_builder()
            .scheme("https://dynamodb.us-east-1.amazonaws.com/x")
            .build();

        assert_eq!(url_shaped_scheme.endpoint_url(), None);
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
        assert!(LiveNodes::try_new_for_test(&direct).unwrap().is_none());

        // Without a seed host there is nothing to send requests to, so the
        // client fails closed instead of falling back to an AWS endpoint.
        let no_target = AlternatorConfig::builder().without_discovery().build();

        assert_eq!(no_target.endpoint_url(), None);
        assert!(LiveNodes::try_new_for_test(&no_target).is_err());
    }

    #[test]
    fn discovery_can_be_reenabled_when_rebuilding_a_direct_config() {
        let direct = AlternatorConfig::builder()
            .seed_hosts(["load-balancer.example.com"])
            .port(8000)
            .without_discovery()
            .build();

        for set_discovery_first in [false, true] {
            let mut builder = direct.to_builder();
            if set_discovery_first {
                builder.set_without_discovery(false);
            }
            builder
                .set_seed_hosts(vec!["node-1".to_string(), "node-2".to_string()])
                .set_port(9000);
            if !set_discovery_first {
                builder.set_without_discovery(false);
            }

            let discovery = builder.build();
            assert!(!discovery.without_discovery());
            assert_eq!(
                discovery.seed_hosts(),
                Some(vec!["node-1".to_string(), "node-2".to_string()])
            );
            assert!(LiveNodes::try_new_for_test(&discovery).unwrap().is_some());
        }
    }

    #[test]
    fn scheme_normalization_accepts_only_documented_wrappers() {
        for (scheme, expected) in [
            ("https://", "https"),
            ("http:", "http"),
            ("http", "http"),
            ("custom://", "custom"),
            ("custom:", "custom"),
        ] {
            let config = AlternatorConfig::builder().scheme(scheme).build();
            assert_eq!(config.scheme().as_deref(), Some(expected));
        }

        for malformed in ["http::", "http:/", "http///", "http:://"] {
            let config = AlternatorConfig::builder()
                .scheme(malformed)
                .seed_hosts(["host"])
                .build();
            assert_eq!(config.endpoint_url(), None, "accepted {malformed:?}");
        }
    }

    #[test]
    fn config_remembers_response_compression() {
        let response_compression = ResponseCompression::enabled(ResponseCompressionAlgorithm::Gzip)
            .with_max_encoding_layers(7)
            .with_max_decompressed_bytes(64 * 1024 * 1024);
        let config = AlternatorConfig::builder()
            .response_compression(response_compression.clone())
            .build();

        assert_eq!(
            config
                .response_compression()
                .expect("response_compression not set"),
            response_compression
        );

        // round-trip through to_builder
        let config = config.to_builder().build();

        assert_eq!(
            config
                .response_compression()
                .expect("response_compression not set after round-trip"),
            response_compression
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
