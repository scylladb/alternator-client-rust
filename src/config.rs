use crate::*;

/// Storage for alternator-specific settings chosen by the user for [AlternatorConfig].
///
/// Each field is an Option, as the user may have not chosen a value.
///
/// It is important to store them separately from Dynamodb's config,
/// as [AlternatorConfig] is also used in overriding [AlternatorClient]'s config at per-operation level,
/// when the override includes only settings selected by the user.
/// (see [AlternatorCustomizableOperation])
#[derive(Clone, Debug, Default)]
pub(crate) struct AlternatorExtensions {
    pub(crate) request_compression: Option<RequestCompression>,
    pub(crate) response_compression: Option<ResponseCompression>,
    pub(crate) optimize_headers: Option<bool>,
    pub(crate) has_credentials_provider: bool,
    pub(crate) disable_implicit_no_auth: bool,
    pub(crate) active_interval: Option<std::time::Duration>,
    pub(crate) idle_interval: Option<std::time::Duration>,
    pub(crate) routing_scope: Option<RoutingScope>,
    pub(crate) scheme: Option<String>,
    pub(crate) port: Option<u16>,
    pub(crate) seed_hosts: Option<Vec<String>>,
    pub(crate) live_nodes: Option<std::sync::Arc<LiveNodes>>,
    pub(crate) key_route_affinity: Option<keyrouting::affinity_config::KeyRouteAffinityConfig>,
}

const UNSUPPORTED_AUTH_API_MESSAGE: &str = "Alternator supports only SigV4 with static credentials or no-auth. Use credentials_provider(...), per-request config_override(...credentials_provider(...)), auth_scheme_preference([aws_runtime::auth::sigv4::SCHEME_ID]) for strict signed requests, or allow_no_auth(). Custom AWS SDK auth schemes and resolvers are not supported.";

fn unsupported_auth_api(api: &str) -> ! {
    panic!("{api} is not supported by alternator-driver: {UNSUPPORTED_AUTH_API_MESSAGE}");
}

fn is_supported_auth_scheme(
    scheme_id: &aws_smithy_runtime_api::client::auth::AuthSchemeId,
) -> bool {
    scheme_id == &aws_runtime::auth::sigv4::SCHEME_ID
        || scheme_id == &aws_smithy_runtime::client::auth::no_auth::NO_AUTH_SCHEME_ID
}

fn normalize_auth_scheme_preference(
    preference: aws_smithy_runtime_api::client::auth::AuthSchemePreference,
) -> (
    Vec<aws_smithy_runtime_api::client::auth::AuthSchemeId>,
    bool,
) {
    let preference: Vec<_> = preference.into_iter().collect();
    if let Some(scheme_id) = preference.iter().find(|id| !is_supported_auth_scheme(id)) {
        panic!(
            "unsupported auth scheme preference {:?}: {UNSUPPORTED_AUTH_API_MESSAGE}",
            scheme_id.inner()
        );
    }
    let disable_implicit_no_auth = !preference.is_empty()
        && !preference
            .iter()
            .any(|id| id == &aws_smithy_runtime::client::auth::no_auth::NO_AUTH_SCHEME_ID);
    (preference, disable_implicit_no_auth)
}

/// [AlternatorClient]'s config
///
/// A simple wrapper around [aws_sdk_dynamodb::Config], that also includes alternator-specific optimizations.
///
/// It is used to construct [AlternatorClient] like so:
///
/// ```
/// use alternator_driver::{AlternatorClient, AlternatorConfig};
/// let config =
///     AlternatorConfig::builder()
///     .behavior_version_latest()
///     // ...
///     .build();
///
/// let client = AlternatorClient::from_conf(config);
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

    pub fn to_builder(&self) -> AlternatorBuilder {
        AlternatorBuilder {
            dynamodb_builder: self.dynamodb_config.to_builder(),
            alternator_ext: self.alternator_ext.clone(),
        }
    }

    pub fn new(config: &aws_types::sdk_config::SdkConfig) -> Self {
        AlternatorBuilder::from(config).build()
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

    pub(crate) fn disable_implicit_no_auth(&self) -> bool {
        self.alternator_ext.disable_implicit_no_auth
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
    pub fn seed_hosts(&self) -> Option<Vec<String>> {
        self.alternator_ext.seed_hosts.clone()
    }

    /// The [`LiveNodes`] instance shared into this config, if any.
    ///
    /// On a config you have built but not yet used, this is [`None`] unless you
    /// set it via [`live_nodes`], and [`None`] means a client built from it will
    /// construct its own. On the config a client stores ([`config()`]), this is
    /// populated: the constructor sets the instance it created so the stored config
    /// reflects what the client actually uses. It is [`None`] there only if
    /// constructing [`LiveNodes`] failed.
    ///
    /// [`live_nodes`]: Self::live_nodes
    /// [`config()`]: AlternatorClient::config
    pub fn live_nodes(&self) -> Option<std::sync::Arc<LiveNodes>> {
        self.alternator_ext.live_nodes.clone()
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

/// Builder for [AlternatorConfig]
///
/// A simple wrapper around [aws_sdk_dynamodb::config::Builder], that also includes alternator-specific optimizations.
///
/// It is used to construct [AlternatorClient] like so:
///
/// ```
/// use alternator_driver::{AlternatorClient, AlternatorConfig};
/// let config =
///     AlternatorConfig::builder()
///    .behavior_version_latest()
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

    pub fn build(self) -> AlternatorConfig {
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
        self.alternator_ext.routing_scope = Some(routing_scope);
        self
    }

    /// Sets the URI scheme (http or https).
    ///
    /// Accepts for example "http", "http:", "http://" — stores just "http", same with "https".
    pub fn scheme(mut self, scheme: impl Into<String>) -> Self {
        self.set_scheme(scheme);
        self
    }

    /// Sets the URI scheme (http or https).
    ///
    /// Accepts for example "http", "http:", "http://" — stores just "http", same with "https".
    pub fn set_scheme(&mut self, scheme: impl Into<String>) -> &mut Self {
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
        self.alternator_ext.port = Some(port);
        self
    }

    /// Set the list of seed hosts for cluster discovery.
    ///
    /// The seed hosts are the initial endpoints (IP addresses or hostnames) used to discover the full cluster topology.
    /// Use with [`AlternatorBuilder::scheme`] and [`AlternatorBuilder::port`] to construct the endpoint URIs.
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
    /// The seed hosts are the initial endpoints (IP addresses or hostnames) used to discover the full cluster topology.
    /// Use with [`AlternatorBuilder::scheme`] and [`AlternatorBuilder::port`] to construct the endpoint URIs.
    pub fn set_seed_hosts(&mut self, seed_hosts: Vec<String>) -> &mut Self {
        self.alternator_ext.seed_hosts = Some(seed_hosts);
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
        self.alternator_ext.live_nodes = Some(live_nodes);
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

impl From<&aws_types::sdk_config::SdkConfig> for AlternatorBuilder {
    fn from(sdk_config: &aws_types::sdk_config::SdkConfig) -> Self {
        let has_credentials_provider = sdk_config.credentials_provider().is_some();
        let disable_implicit_no_auth = sdk_config
            .auth_scheme_preference()
            .cloned()
            .map(|preference| normalize_auth_scheme_preference(preference).1)
            .unwrap_or(false);
        AlternatorBuilder {
            dynamodb_builder: aws_sdk_dynamodb::config::Builder::from(sdk_config),
            alternator_ext: AlternatorExtensions {
                has_credentials_provider,
                disable_implicit_no_auth,
                ..Default::default()
            },
        }
    }
}

impl From<&aws_types::sdk_config::SdkConfig> for AlternatorConfig {
    fn from(sdk_config: &aws_types::sdk_config::SdkConfig) -> Self {
        AlternatorBuilder::from(sdk_config).build()
    }
}

// All implementations below this point should only be simple wrappers around dynamodb methods

impl AlternatorConfig {
    pub fn stalled_stream_protection(
        &self,
    ) -> Option<&aws_sdk_dynamodb::config::StalledStreamProtectionConfig> {
        self.dynamodb_config.stalled_stream_protection()
    }

    pub fn http_client(&self) -> Option<aws_sdk_dynamodb::config::SharedHttpClient> {
        self.dynamodb_config.http_client()
    }

    pub fn auth_schemes(
        &self,
    ) -> impl Iterator<Item = aws_smithy_runtime_api::client::auth::SharedAuthScheme> {
        self.dynamodb_config.auth_schemes()
    }

    pub fn auth_scheme_resolver(
        &self,
    ) -> Option<aws_smithy_runtime_api::client::auth::SharedAuthSchemeOptionResolver> {
        self.dynamodb_config.auth_scheme_resolver()
    }

    pub fn auth_scheme_preference(
        &self,
    ) -> Option<&aws_smithy_runtime_api::client::auth::AuthSchemePreference> {
        self.dynamodb_config.auth_scheme_preference()
    }

    pub fn endpoint_resolver(
        &self,
    ) -> aws_smithy_runtime_api::client::endpoint::SharedEndpointResolver {
        self.dynamodb_config.endpoint_resolver()
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

    pub fn push_auth_scheme(
        self,
        _auth_scheme: impl aws_smithy_runtime_api::client::auth::AuthScheme + 'static,
    ) -> Self {
        unsupported_auth_api("push_auth_scheme")
    }

    pub fn auth_scheme_resolver(
        self,
        _auth_scheme_resolver: impl aws_sdk_dynamodb::config::auth::ResolveAuthScheme + 'static,
    ) -> Self {
        unsupported_auth_api("auth_scheme_resolver")
    }

    pub fn set_auth_scheme_resolver(
        &mut self,
        _auth_scheme_resolver: impl aws_sdk_dynamodb::config::auth::ResolveAuthScheme + 'static,
    ) -> &mut Self {
        unsupported_auth_api("set_auth_scheme_resolver")
    }

    pub fn allow_no_auth(mut self) -> Self {
        self.dynamodb_builder = self.dynamodb_builder.allow_no_auth();
        self
    }

    pub fn set_allow_no_auth(&mut self) -> &mut Self {
        self.dynamodb_builder.set_allow_no_auth();
        self
    }

    pub fn auth_scheme_preference(
        mut self,
        preference: impl Into<aws_smithy_runtime_api::client::auth::AuthSchemePreference>,
    ) -> Self {
        let (preference, disable_implicit_no_auth) =
            normalize_auth_scheme_preference(preference.into());
        self.alternator_ext.disable_implicit_no_auth = disable_implicit_no_auth;
        self.dynamodb_builder = self.dynamodb_builder.auth_scheme_preference(preference);
        self
    }

    pub fn set_auth_scheme_preference(
        &mut self,
        preference: Option<aws_smithy_runtime_api::client::auth::AuthSchemePreference>,
    ) -> &mut Self {
        let preference = preference.map(|preference| {
            let (preference, disable_implicit_no_auth) =
                normalize_auth_scheme_preference(preference);
            self.alternator_ext.disable_implicit_no_auth = disable_implicit_no_auth;
            preference.into()
        });
        if preference.is_none() {
            self.alternator_ext.disable_implicit_no_auth = false;
        }
        self.dynamodb_builder.set_auth_scheme_preference(preference);
        self
    }

    pub fn endpoint_resolver(
        mut self,
        endpoint_resolver: impl aws_sdk_dynamodb::config::endpoint::ResolveEndpoint + 'static,
    ) -> Self {
        self.dynamodb_builder = self.dynamodb_builder.endpoint_resolver(endpoint_resolver);
        self
    }

    pub fn set_endpoint_resolver(
        &mut self,
        endpoint_resolver: Option<aws_smithy_runtime_api::client::endpoint::SharedEndpointResolver>,
    ) -> &mut Self {
        self.dynamodb_builder
            .set_endpoint_resolver(endpoint_resolver);
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

    pub fn account_id_endpoint_mode(
        mut self,
        account_id_endpoint_mode: aws_types::endpoint_config::AccountIdEndpointMode,
    ) -> Self {
        self.dynamodb_builder = self
            .dynamodb_builder
            .account_id_endpoint_mode(account_id_endpoint_mode);
        self
    }

    pub fn set_account_id_endpoint_mode(
        &mut self,
        account_id_endpoint_mode: Option<aws_types::endpoint_config::AccountIdEndpointMode>,
    ) -> &mut Self {
        self.dynamodb_builder
            .set_account_id_endpoint_mode(account_id_endpoint_mode);
        self
    }

    pub fn endpoint_url(mut self, endpoint_url: impl Into<String>) -> Self {
        self.set_endpoint_url(Some(endpoint_url.into()));
        self
    }

    pub fn set_endpoint_url(&mut self, endpoint_url: Option<String>) -> &mut Self {
        // Reset everything upfront to avoid stale fields.
        self.alternator_ext.seed_hosts = None;
        self.alternator_ext.scheme = None;
        self.alternator_ext.port = None;

        if let Some(url_str) = endpoint_url.as_deref()
            && let Ok(url) = url::Url::parse(url_str)
            && let Some(host) = url.host_str()
        {
            self.set_seed_hosts(vec![host.to_string()]);
            self.set_scheme(url.scheme());
            if let Some(port) = url.port() {
                self.set_port(port);
            }
        }
        self.dynamodb_builder.set_endpoint_url(endpoint_url);
        self
    }

    pub fn use_dual_stack(mut self, use_dual_stack: impl Into<bool>) -> Self {
        self.dynamodb_builder = self.dynamodb_builder.use_dual_stack(use_dual_stack);
        self
    }

    pub fn set_use_dual_stack(&mut self, use_dual_stack: Option<bool>) -> &mut Self {
        self.dynamodb_builder.set_use_dual_stack(use_dual_stack);
        self
    }

    pub fn use_fips(mut self, use_fips: impl Into<bool>) -> Self {
        self.dynamodb_builder = self.dynamodb_builder.use_fips(use_fips);
        self
    }

    pub fn set_use_fips(&mut self, use_fips: Option<bool>) -> &mut Self {
        self.dynamodb_builder.set_use_fips(use_fips);
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

    pub fn behavior_version(
        mut self,
        behavior_version: aws_sdk_dynamodb::config::BehaviorVersion,
    ) -> Self {
        self.dynamodb_builder = self.dynamodb_builder.behavior_version(behavior_version);
        self
    }

    pub fn set_behavior_version(
        &mut self,
        behavior_version: Option<aws_sdk_dynamodb::config::BehaviorVersion>,
    ) -> &mut Self {
        self.dynamodb_builder.set_behavior_version(behavior_version);
        self
    }

    pub fn behavior_version_latest(mut self) -> Self {
        self.dynamodb_builder = self.dynamodb_builder.behavior_version_latest();
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
            .behavior_version_latest()
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
    }

    #[test]
    fn config_does_not_add_hooks() {
        let config = AlternatorConfig::builder()
            .optimize_headers(true)
            .behavior_version_latest()
            .build();

        assert!(
            config
                .interceptors()
                .try_len()
                .expect("does not have length")
                == 0
        );
    }

    #[test]
    fn shared_sdk_config_credentials_provider_is_remembered() {
        let sdk_config = aws_types::SdkConfig::builder()
            .credentials_provider(aws_sdk_dynamodb::config::SharedCredentialsProvider::new(
                aws_sdk_dynamodb::config::Credentials::for_tests_with_session_token(),
            ))
            .build();

        let config = AlternatorConfig::from(&sdk_config);

        assert!(config.has_credentials_provider());
    }

    #[test]
    fn shared_sdk_config_without_credentials_provider_is_no_auth() {
        let sdk_config = aws_types::SdkConfig::builder().build();

        let config = AlternatorConfig::from(&sdk_config);

        assert!(!config.has_credentials_provider());
    }

    #[test]
    fn shared_sdk_config_auth_scheme_preference_is_remembered() {
        let sdk_config = aws_types::SdkConfig::builder()
            .auth_scheme_preference([aws_runtime::auth::sigv4::SCHEME_ID])
            .build();

        let config = AlternatorConfig::from(&sdk_config);

        assert!(config.disable_implicit_no_auth());
    }

    #[test]
    fn sigv4_auth_scheme_preference_disables_implicit_no_auth() {
        let config = AlternatorConfig::builder()
            .auth_scheme_preference([aws_runtime::auth::sigv4::SCHEME_ID])
            .behavior_version_latest()
            .build();

        assert!(config.disable_implicit_no_auth());
    }

    #[test]
    fn no_auth_scheme_preference_keeps_implicit_no_auth_enabled() {
        let config = AlternatorConfig::builder()
            .auth_scheme_preference([aws_smithy_runtime::client::auth::no_auth::NO_AUTH_SCHEME_ID])
            .behavior_version_latest()
            .build();

        assert!(!config.disable_implicit_no_auth());
    }

    #[test]
    fn clearing_auth_scheme_preference_restores_implicit_no_auth() {
        let mut builder = AlternatorConfig::builder()
            .auth_scheme_preference([aws_runtime::auth::sigv4::SCHEME_ID]);

        builder.set_auth_scheme_preference(None);
        let config = builder.behavior_version_latest().build();

        assert!(!config.disable_implicit_no_auth());
    }

    #[test]
    #[should_panic(expected = "unsupported auth scheme preference")]
    fn unsupported_auth_scheme_preference_panics() {
        let _ = AlternatorConfig::builder()
            .auth_scheme_preference([aws_smithy_runtime_api::client::auth::AuthSchemeId::new(
                "sigv4a",
            )])
            .behavior_version_latest()
            .build();
    }

    #[test]
    #[should_panic(expected = "Alternator supports only SigV4 with static credentials or no-auth")]
    fn unsupported_auth_scheme_resolver_panics() {
        let _ = AlternatorConfig::builder().auth_scheme_resolver(
            aws_sdk_dynamodb::config::auth::DefaultAuthSchemeResolver::default(),
        );
    }

    #[test]
    fn from_conf_does_not_panic_without_runtime() {
        let config = AlternatorConfig::builder()
            .behavior_version_latest()
            .build();
        let _ = AlternatorClient::from_conf(config);
    }

    #[test]
    fn endpoint_url_sets_and_clears_correctly() {
        let config = AlternatorConfig::builder()
            .endpoint_url("http://127.0.0.1:8000")
            .behavior_version_latest()
            .build();
        assert_eq!(config.seed_hosts(), Some(vec!["127.0.0.1".to_string()]));
        assert_eq!(config.scheme(), Some("http".to_string()));
        assert_eq!(config.port(), Some(8000));

        let mut new_builder = config.to_builder();
        new_builder.set_endpoint_url(None);
        let new_config = new_builder.build();

        assert_eq!(new_config.seed_hosts(), None);
        assert_eq!(new_config.scheme(), None);
        assert_eq!(new_config.port(), None);
    }

    #[test]
    fn setting_scheme_test() {
        let config = AlternatorConfig::builder()
            .scheme("https://")
            .behavior_version_latest()
            .build();

        assert_eq!(config.scheme(), Some("https".to_string()));

        let config = AlternatorConfig::builder()
            .scheme("http:")
            .behavior_version_latest()
            .build();

        assert_eq!(config.scheme(), Some("http".to_string()));

        let config = AlternatorConfig::builder()
            .scheme("http")
            .behavior_version_latest()
            .build();

        assert_eq!(config.scheme(), Some("http".to_string()));
    }

    #[test]
    fn test_live_nodes_sharing() {
        let config = AlternatorConfig::builder()
            .behavior_version_latest()
            .endpoint_url("http://127.0.0.1:8000")
            .build();

        let live_nodes = LiveNodes::new(&config).unwrap();

        // The client can be built with the provided live_nodes instance.
        let client1 = AlternatorClient::from_conf_with_live_nodes(config, live_nodes.clone());

        // live_nodes can also be insterted into a new config.
        let config2 = AlternatorConfig::builder()
            .live_nodes(live_nodes.clone())
            .behavior_version_latest()
            .build();

        let client2 = AlternatorClient::from_conf(config2);

        // Assert that they point to the exact same Arc.
        assert!(std::sync::Arc::ptr_eq(
            &client1.config().live_nodes().unwrap(),
            &client2.config().live_nodes().unwrap()
        ));
    }

    #[test]
    fn config_remembers_response_compression() {
        let config = AlternatorConfig::builder()
            .response_compression(ResponseCompression::enabled(
                ResponseCompressionAlgorithm::Gzip,
            ))
            .behavior_version_latest()
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
            .behavior_version_latest()
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
        let config = AlternatorConfig::builder()
            .behavior_version_latest()
            .build();

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
