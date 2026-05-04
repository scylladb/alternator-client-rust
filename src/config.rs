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
    pub(crate) enforce_header_whitelist: Option<bool>,
    pub(crate) routing_scope: Option<RoutingScope>,
}

/// [AlternatorClient]'s config
///
/// A simple wrapper around [aws_sdk_dynamodb::Config], that also includes alternator-specific optimizations.
///
/// It is used to construct [AlternatorClient] like so:
///
/// ```ignore
/// let config =
///     AlternatorConfig::builder()
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

    /// Before sending each request, strip them from headers that are not used by the Alternator.
    ///
    /// This is done by an interceptor in `modify_before_transmit` hook.
    ///
    /// Take note, that this may break your own interceptors,
    /// if they happened to look inside these headers after this happens.
    ///
    /// Turned on by default.
    pub fn enforce_header_whitelist(&self) -> Option<bool> {
        self.alternator_ext.enforce_header_whitelist
    }

    /// Enable / disable request compression.
    ///
    /// This must be done before the request is signed,
    /// and is done by an interceptor in `modify_before_retry_loop` hook.
    ///
    /// Take note, that this may break your own interceptors,
    /// if they happened to look inside the body after this happens.
    ///
    /// By default, Gzip compression is used, with 1024 threshold and level 6 of compression.
    pub fn request_compression(&self) -> Option<RequestCompression> {
        self.alternator_ext.request_compression.clone()
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
    /// If this is not provided, the client will use the cluster scope, meaning load balancing will happen across nodes in the datacenter of the seed host.
    /// If multiple seed hosts are provided, it will use the datacenter of one of the seed hosts, falling back to a different one if needed.
    ///
    /// Keep in mind that subsequent fallback scope should ideally be broader than or equal to the
    /// previous one, e.g., (rack -> datacenter -> cluster) or (rack -> another rack -> datacenter -> cluster).
    /// Making a fallback narrower, e.g., (datacenter -> rack) or (cluster -> datacenter),
    /// may be redundant if the set of nodes in the next scope is a subset of the previous one.
    pub fn routing_scope(&self) -> Option<RoutingScope> {
        self.alternator_ext.routing_scope.clone()
    }
}

/// Builder for [AlternatorConfig]
///
/// A simple wrapper around [aws_sdk_dynamodb::config::Builder], that also includes alternator-specific optimizations.
///
/// It is used to construct [AlternatorClient] like so:
///
/// ```ignore
/// let config =
///     AlternatorConfig::builder()
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

    /// Before sending each request, strip them from headers that are not used by the Alternator.
    ///
    /// This is done by an interceptor in `modify_before_transmit` hook.
    ///
    /// Take note, that this may break your own interceptors,
    /// if they happened to look inside these headers after this happens.
    ///
    /// Turned on by default.
    pub fn enforce_header_whitelist(mut self, enforce: bool) -> Self {
        self.set_enforce_header_whitelist(enforce);
        self
    }

    /// Before sending each request, strip them from headers that are not used by the Alternator.
    ///
    /// This is done by an interceptor in `modify_before_transmit` hook.
    ///
    /// Take note, that this may break your own interceptors,
    /// if they happened to look inside these headers after this happens.
    ///
    /// Turned on by default.
    pub fn set_enforce_header_whitelist(&mut self, enforce: bool) -> &mut Self {
        self.alternator_ext.enforce_header_whitelist = Some(enforce);
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
    /// By default, Gzip compression is used, with 1024 threshold and level 6 of compression.
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
    /// By default, Gzip compression is used, with 1024 threshold and level 6 of compression.
    pub fn set_request_compression(
        &mut self,
        request_compression: RequestCompression,
    ) -> &mut Self {
        self.alternator_ext.request_compression = Some(request_compression);
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
    /// If this is not provided, the client will use the cluster scope, meaning load balancing will happen across nodes in the datacenter of the seed host.
    /// If multiple seed hosts are provided, it will use the datacenter of one of the seed hosts, falling back to a different one if needed.
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
    /// If this is not provided, the client will use the cluster scope, meaning load balancing will happen across nodes in the datacenter of the seed host.
    /// If multiple seed hosts are provided, it will use the datacenter of one of the seed hosts, falling back to a different one if needed.
    ///
    /// Keep in mind that subsequent fallback scope should ideally be broader than or equal to the
    /// previous one, e.g., (rack -> datacenter -> cluster) or (rack -> another rack -> datacenter -> cluster).
    /// Making a fallback narrower, e.g., (datacenter -> rack) or (cluster -> datacenter),
    /// may be redundant if the set of nodes in the next scope is a subset of the previous one.
    pub fn set_routing_scope(&mut self, routing_scope: RoutingScope) -> &mut Self {
        self.alternator_ext.routing_scope = Some(routing_scope);
        self
    }
}

impl From<&aws_types::sdk_config::SdkConfig> for AlternatorBuilder {
    fn from(sdk_config: &aws_types::sdk_config::SdkConfig) -> Self {
        AlternatorBuilder {
            dynamodb_builder: aws_sdk_dynamodb::config::Builder::from(sdk_config),
            alternator_ext: AlternatorExtensions::default(),
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
        mut self,
        auth_scheme: impl aws_smithy_runtime_api::client::auth::AuthScheme + 'static,
    ) -> Self {
        self.dynamodb_builder = self.dynamodb_builder.push_auth_scheme(auth_scheme);
        self
    }

    pub fn auth_scheme_resolver(
        mut self,
        auth_scheme_resolver: impl aws_sdk_dynamodb::config::auth::ResolveAuthScheme + 'static,
    ) -> Self {
        self.dynamodb_builder = self
            .dynamodb_builder
            .auth_scheme_resolver(auth_scheme_resolver);
        self
    }

    pub fn set_auth_scheme_resolver(
        &mut self,
        auth_scheme_resolver: impl aws_sdk_dynamodb::config::auth::ResolveAuthScheme + 'static,
    ) -> &mut Self {
        self.dynamodb_builder
            .set_auth_scheme_resolver(auth_scheme_resolver);
        self
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
        self.dynamodb_builder = self.dynamodb_builder.auth_scheme_preference(preference);
        self
    }

    pub fn set_auth_scheme_preference(
        &mut self,
        preference: Option<aws_smithy_runtime_api::client::auth::AuthSchemePreference>,
    ) -> &mut Self {
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
        self.dynamodb_builder = self.dynamodb_builder.endpoint_url(endpoint_url);
        self
    }

    pub fn set_endpoint_url(&mut self, endpoint_url: Option<String>) -> &mut Self {
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
        self.dynamodb_builder = self
            .dynamodb_builder
            .credentials_provider(credentials_provider);
        self
    }

    pub fn set_credentials_provider(
        &mut self,
        credentials_provider: Option<aws_sdk_dynamodb::config::SharedCredentialsProvider>,
    ) -> &mut Self {
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
                CompressionAlgorithm::Zlib,
                CompressionLevel::default(),
                0,
            ))
            .behavior_version_latest()
            .build();

        assert!(config.enforce_header_whitelist().is_none());

        assert_eq!(
            config
                .request_compression()
                .expect("request compression is not set"),
            RequestCompression::enabled(CompressionAlgorithm::Zlib, CompressionLevel::default(), 0)
        );

        let config = config.to_builder().build();

        assert!(config.enforce_header_whitelist().is_none());

        assert_eq!(
            config
                .request_compression()
                .expect("request compression is not set"),
            RequestCompression::enabled(CompressionAlgorithm::Zlib, CompressionLevel::default(), 0)
        );
    }

    #[test]
    fn config_does_not_add_hooks() {
        let config = AlternatorConfig::builder()
            .enforce_header_whitelist(true)
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
}
