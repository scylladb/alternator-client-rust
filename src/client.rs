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

use crate::live_nodes::{
    LiveNodes, LiveNodesBuildError, ensure_native_roots_are_usable, native_roots_are_usable,
};
use crate::*;
use aws_smithy_runtime_api::{
    box_error::BoxError,
    client::{
        connector_metadata::ConnectorMetadata,
        endpoint::{
            EndpointFuture, EndpointResolverParams, ResolveEndpoint, SharedEndpointResolver,
        },
        http::{HttpClient, HttpConnectorSettings, SharedHttpClient, SharedHttpConnector},
        identity::{
            IdentityFuture, ResolveCachedIdentity, SharedIdentityCache, SharedIdentityResolver,
        },
        runtime_components::{RuntimeComponents, RuntimeComponentsBuilder},
    },
};
use aws_smithy_types::config_bag::ConfigBag;

/// Alternator driver's client
///
/// A client wrapper around [aws_sdk_dynamodb::Client] that adds Alternator
/// routing, auth defaults, and request/response optimizations.
///
/// By default:
/// - enables round-robin load balancing
/// - strips headers that Alternator does not use from all requests
/// - sets a default AWS region, as Alternator does not require a specific one
/// - does not use request compression
///
/// Can be built with [AlternatorConfig] like so:
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
/// Shared `aws_types::SdkConfig` imports are intentionally unsupported. Build
/// an [`AlternatorConfig`] explicitly and pass it to [`from_conf`](Self::from_conf).
///
/// ```compile_fail
/// let sdk_config = aws_types::SdkConfig::builder().build();
/// let _ = alternator_driver::AlternatorClient::new(&sdk_config);
/// ```
#[derive(Clone, Debug)]
pub struct AlternatorClient {
    dynamodb_client: aws_sdk_dynamodb::Client,
    config: AlternatorConfig,
}

/// Error returned when an [`AlternatorClient`] cannot be constructed safely.
#[derive(Debug)]
pub struct AlternatorClientBuildError {
    kind: AlternatorClientBuildErrorKind,
}

#[derive(Debug)]
enum AlternatorClientBuildErrorKind {
    SdkConfiguration(String),
    TlsConfiguration(String),
    LiveNodes(LiveNodesBuildError),
}

impl std::fmt::Display for AlternatorClientBuildError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self.kind {
            AlternatorClientBuildErrorKind::SdkConfiguration(message) => {
                write!(formatter, "failed to configure AWS SDK client: {message}")
            }
            AlternatorClientBuildErrorKind::TlsConfiguration(message) => {
                write!(formatter, "failed to configure SDK TLS: {message}")
            }
            AlternatorClientBuildErrorKind::LiveNodes(source) => {
                write!(
                    formatter,
                    "failed to configure Alternator routing: {source}"
                )
            }
        }
    }
}

impl std::error::Error for AlternatorClientBuildError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match &self.kind {
            AlternatorClientBuildErrorKind::SdkConfiguration(_) => None,
            AlternatorClientBuildErrorKind::TlsConfiguration(_) => None,
            AlternatorClientBuildErrorKind::LiveNodes(source) => Some(source),
        }
    }
}

impl From<LiveNodesBuildError> for AlternatorClientBuildError {
    fn from(source: LiveNodesBuildError) -> Self {
        Self {
            kind: AlternatorClientBuildErrorKind::LiveNodes(source),
        }
    }
}

fn validate_sdk_default_config(
    stalled_stream_protection_explicitly_unset: bool,
) -> Result<(), AlternatorClientBuildError> {
    if stalled_stream_protection_explicitly_unset {
        return Err(AlternatorClientBuildError {
            kind: AlternatorClientBuildErrorKind::SdkConfiguration(
                "The default stalled stream protection config was removed, and no other config was put in its place."
                    .to_owned(),
            ),
        });
    }

    Ok(())
}

/// Defers endpoint resolution for affinity-eligible operations until the
/// cross-rack topology view has completed its first successful refresh.
#[derive(Debug)]
struct AffinityDiscoveryEndpointResolver {
    inner: SharedEndpointResolver,
    affinity_nodes: std::sync::Arc<LiveNodes>,
}

impl ResolveEndpoint for AffinityDiscoveryEndpointResolver {
    fn resolve_endpoint<'a>(&'a self, params: &'a EndpointResolverParams) -> EndpointFuture<'a> {
        EndpointFuture::new(async move {
            if params.get_property::<AffinityDiscoveryRequired>().is_some()
                && !self.affinity_nodes.wait_for_initial_discovery().await
            {
                return Err("timed out waiting for the cross-rack affinity topology".into());
            }
            self.inner.resolve_endpoint(params).await
        })
    }

    fn finalize_params<'a>(
        &'a self,
        params: &'a mut EndpointResolverParams,
    ) -> Result<(), BoxError> {
        self.inner.finalize_params(params)
    }
}

#[derive(Clone, Debug)]
struct SdkConfigValidation {
    error: std::sync::Arc<std::sync::OnceLock<String>>,
    http_client: SharedHttpClient,
    identity_cache: SharedIdentityCache,
}

impl SdkConfigValidation {
    fn restore_base_components(
        &self,
        runtime_components: &RuntimeComponentsBuilder,
    ) -> RuntimeComponentsBuilder {
        let mut runtime_components = runtime_components.clone();
        runtime_components.set_http_client(Some(self.http_client.clone()));
        runtime_components.set_identity_cache(Some(self.identity_cache.clone()));
        runtime_components
    }

    fn validate_http_final_config(
        &self,
        runtime_components: &RuntimeComponents,
        config: &ConfigBag,
    ) -> Result<(), BoxError> {
        // Validate the original HTTP client while leaving the selected identity
        // cache behavior intact. The outer SDK validation will validate that
        // cache once, after this adapter returns.
        let mut components = runtime_components.to_builder();
        components.set_http_client(Some(self.http_client.clone()));
        components.set_identity_cache(Some(UnvalidatedIdentityCache(
            runtime_components.identity_cache(),
        )));
        components.build()?.validate_final_config(config)
    }

    fn validate_identity_cache_final_config(
        &self,
        runtime_components: &RuntimeComponents,
        config: &ConfigBag,
    ) -> Result<(), BoxError> {
        // Symmetrically validate the original cache without revalidating or
        // replacing a per-operation HTTP-client override.
        let mut components = runtime_components.to_builder();
        if let Some(http_client) = runtime_components.http_client() {
            components.set_http_client(Some(UnvalidatedHttpClient(http_client)));
        }
        components.set_identity_cache(Some(self.identity_cache.clone()));
        components.build()?.validate_final_config(config)
    }

    fn record_error(&self, error: BoxError) {
        let _ = self.error.set(error.to_string());
    }
}

impl HttpClient for SdkConfigValidation {
    fn http_connector(
        &self,
        settings: &HttpConnectorSettings,
        runtime_components: &RuntimeComponents,
    ) -> SharedHttpConnector {
        self.http_client
            .http_connector(settings, runtime_components)
    }

    fn validate_base_client_config(
        &self,
        runtime_components: &RuntimeComponentsBuilder,
        config: &ConfigBag,
    ) -> Result<(), BoxError> {
        if let Err(error) = self
            .restore_base_components(runtime_components)
            .validate_base_client_config(config)
        {
            self.record_error(error);
        }
        Ok(())
    }

    fn validate_final_config(
        &self,
        runtime_components: &RuntimeComponents,
        config: &ConfigBag,
    ) -> Result<(), BoxError> {
        self.validate_http_final_config(runtime_components, config)
    }

    fn connector_metadata(&self) -> Option<ConnectorMetadata> {
        self.http_client.connector_metadata()
    }
}

#[derive(Clone, Debug)]
struct UnvalidatedHttpClient(SharedHttpClient);

impl HttpClient for UnvalidatedHttpClient {
    fn http_connector(
        &self,
        settings: &HttpConnectorSettings,
        runtime_components: &RuntimeComponents,
    ) -> SharedHttpConnector {
        self.0.http_connector(settings, runtime_components)
    }

    fn connector_metadata(&self) -> Option<ConnectorMetadata> {
        self.0.connector_metadata()
    }
}

#[derive(Clone, Debug)]
struct UnvalidatedIdentityCache(SharedIdentityCache);

impl ResolveCachedIdentity for UnvalidatedIdentityCache {
    fn resolve_cached_identity<'a>(
        &'a self,
        resolver: SharedIdentityResolver,
        runtime_components: &'a RuntimeComponents,
        config: &'a ConfigBag,
    ) -> IdentityFuture<'a> {
        self.0
            .resolve_cached_identity(resolver, runtime_components, config)
    }
}

#[derive(Clone, Debug)]
struct ValidatedIdentityCache(SdkConfigValidation);

impl ResolveCachedIdentity for ValidatedIdentityCache {
    fn resolve_cached_identity<'a>(
        &'a self,
        resolver: SharedIdentityResolver,
        runtime_components: &'a RuntimeComponents,
        config: &'a ConfigBag,
    ) -> IdentityFuture<'a> {
        self.0
            .identity_cache
            .resolve_cached_identity(resolver, runtime_components, config)
    }

    fn validate_final_config(
        &self,
        runtime_components: &RuntimeComponents,
        config: &ConfigBag,
    ) -> Result<(), BoxError> {
        self.0
            .validate_identity_cache_final_config(runtime_components, config)
    }
}

fn try_dynamodb_client_from_conf(
    config: aws_sdk_dynamodb::Config,
) -> Result<aws_sdk_dynamodb::Client, AlternatorClientBuildError> {
    let http_client = config
        .http_client()
        .ok_or_else(|| AlternatorClientBuildError {
            kind: AlternatorClientBuildErrorKind::SdkConfiguration(
                "no HTTP client was selected".to_owned(),
            ),
        })?;
    let identity_cache = config
        .identity_cache()
        .unwrap_or_else(|| aws_smithy_runtime::client::identity::IdentityCache::lazy().build());
    let validation_error = std::sync::Arc::new(std::sync::OnceLock::new());
    let validation = SdkConfigValidation {
        error: validation_error.clone(),
        http_client,
        identity_cache,
    };

    // The SDK's constructor converts validation errors into panics. Let it run
    // validation against the exact components and config bag it assembled, but
    // capture the error in an HTTP-client adapter so `try_from_conf` remains
    // usable with aborting panic hooks and `panic = "abort"`.
    //
    // SDK-owned config validators run before the HTTP client. They cannot fail
    // for configurations exposed here: rt-tokio always supplies sleep, config
    // construction supplies time, and callers cannot add custom runtime plugins.
    let mut builder = config.to_builder();
    builder.set_http_client(Some(SharedHttpClient::new(validation.clone())));
    // The HTTP adapter validates the original cache together with all other
    // components. This delegating cache prevents the SDK from validating it a
    // second time and turning the same error into a panic.
    builder.set_identity_cache(ValidatedIdentityCache(validation));

    let client = aws_sdk_dynamodb::Client::from_conf(builder.build());
    let error = validation_error.get().cloned();
    match error {
        Some(message) => Err(AlternatorClientBuildError {
            kind: AlternatorClientBuildErrorKind::SdkConfiguration(message),
        }),
        None => Ok(client),
    }
}

/// Builds an auxiliary client while deferring user-component validation to
/// the main client constructed from the same base configuration.
///
/// AWS validates HTTP clients and identity caches every time a service client
/// is constructed. Topology discovery needs a separate client without routing
/// interceptors, but custom components may require exactly-once validation.
/// The main client remains the authoritative validation boundary.
fn auxiliary_dynamodb_client_from_conf(
    config: aws_sdk_dynamodb::Config,
) -> Result<aws_sdk_dynamodb::Client, AlternatorClientBuildError> {
    let http_client = config
        .http_client()
        .ok_or_else(|| AlternatorClientBuildError {
            kind: AlternatorClientBuildErrorKind::SdkConfiguration(
                "no HTTP client was selected".to_owned(),
            ),
        })?;
    let identity_cache = config
        .identity_cache()
        .unwrap_or_else(|| aws_smithy_runtime::client::identity::IdentityCache::lazy().build());

    let mut builder = config.to_builder();
    builder.set_http_client(Some(SharedHttpClient::new(UnvalidatedHttpClient(
        http_client,
    ))));
    builder.set_identity_cache(UnvalidatedIdentityCache(identity_cache));
    Ok(aws_sdk_dynamodb::Client::from_conf(builder.build()))
}

/// Builds the modern connector selected by the pinned behavior version.
///
/// Supplying it explicitly lets the fallible constructor capture SDK config
/// validation failures. TLS is omitted for plaintext endpoints on hosts with
/// no usable native root store.
fn sdk_http_client(use_tls: bool) -> aws_sdk_dynamodb::config::SharedHttpClient {
    aws_smithy_http_client::Builder::new().build_with_connector_fn(
        move |settings, runtime_components| {
            let mut connector = aws_smithy_http_client::ConnectorBuilder::default();
            connector.set_connector_settings(settings.cloned());
            if let Some(components) = runtime_components {
                connector.set_sleep_impl(components.sleep_impl());
            }

            connector
                .set_proxy_config(Some(aws_smithy_http_client::proxy::ProxyConfig::from_env()));

            if use_tls {
                connector
                    .tls_provider(aws_smithy_http_client::tls::Provider::Rustls(
                        aws_smithy_http_client::tls::rustls_provider::CryptoMode::AwsLc,
                    ))
                    .build()
            } else {
                connector.build_http()
            }
        },
    )
}

impl AlternatorClient {
    /// Constructs a client, panicking if its routing configuration is invalid.
    ///
    /// Use [`Self::try_from_conf`] when configuration comes from a fallible or
    /// externally supplied source.
    ///
    /// # Panics
    ///
    /// Panics when routing configuration is missing or invalid, or when
    /// required HTTP or TLS resources cannot be built.
    pub fn from_conf(config: AlternatorConfig) -> Self {
        Self::try_from_conf(config)
            .unwrap_or_else(|error| panic!("failed to construct AlternatorClient: {error}"))
    }

    /// Tries to construct a client after validating required SDK and routing
    /// configuration.
    pub fn try_from_conf(config: AlternatorConfig) -> Result<Self, AlternatorClientBuildError> {
        let dynamodb_config = config.dynamodb_config.clone();
        let extensions = config.alternator_ext.clone();
        let stalled_stream_protection_explicitly_unset =
            extensions.stalled_stream_protection_explicitly_unset;

        let request_compression = extensions
            .request_compression
            .unwrap_or(RequestCompression::disabled());
        let response_compression = extensions.response_compression.unwrap_or_default();
        let optimize_headers = extensions.optimize_headers.unwrap_or(true);
        let user_agent = extensions.user_agent.unwrap_or_default();
        let credentials_provider = config.credentials_provider();
        let has_credentials_provider = credentials_provider.is_some();
        let has_region = dynamodb_config.region().is_some();

        let mut builder = dynamodb_config.to_builder();

        if let Some(credentials_provider) = credentials_provider {
            builder.set_credentials_provider(Some(credentials_provider));
        }

        if !has_credentials_provider && !config.requires_auth() && !config.allows_no_auth() {
            builder = builder.allow_no_auth();
        }

        builder = builder.interceptor(AlternatorInterceptor::new(
            request_compression,
            response_compression,
            optimize_headers,
            user_agent,
            has_credentials_provider,
        ));

        // With discovery off nothing rewrites the request, so the transport
        // is decided by the scheme configured for the seed host itself.
        let alternator_scheme = config.scheme().unwrap_or_else(|| "http".to_string());
        let uses_plaintext_transport = alternator_scheme.eq_ignore_ascii_case("http");
        let uses_https_transport = alternator_scheme.eq_ignore_ascii_case("https");
        let has_custom_http_client = dynamodb_config.http_client().is_some();

        if uses_https_transport && !has_custom_http_client {
            ensure_native_roots_are_usable().map_err(|message| AlternatorClientBuildError {
                kind: AlternatorClientBuildErrorKind::TlsConfiguration(message),
            })?;
        }

        if !has_custom_http_client {
            // A TLS-capable connector eagerly validates native roots even for
            // an HTTP endpoint, so use an HTTP-only connector when no roots
            // are available.
            let needs_rootless_plaintext_transport =
                uses_plaintext_transport && !native_roots_are_usable();

            // Supplying the pinned behavior's connector explicitly gives the
            // fallible constructor a component through which it can run the
            // SDK's complete validation without relying on a panic.
            builder.set_http_client(Some(sdk_http_client(!needs_rootless_plaintext_transport)));
        }

        if !has_region {
            builder = builder.region(Some(aws_sdk_dynamodb::config::Region::from_static(
                "us-east-1",
            )));
        }

        validate_sdk_default_config(stalled_stream_protection_explicitly_unset)?;

        // Topology discovery uses ordinary DynamoDB Scan operations against
        // Scylla's virtual system tables. Keep its client free of routing
        // interceptors so every pair of system.local/system.peers scans can be
        // pinned to the candidate node being inspected.
        let topology_client = auxiliary_dynamodb_client_from_conf(builder.clone().build())?;
        let live_nodes = LiveNodes::try_new(&config, topology_client.clone())?;
        let affinity_config = config
            .key_route_affinity()
            .filter(|config| config.is_enabled());
        let affinity_live_nodes = match (
            live_nodes.as_ref(),
            affinity_config.as_ref(),
            config.routing_scope(),
        ) {
            (Some(_), Some(_), Some(scope)) if scope.has_rack_restriction() => {
                LiveNodes::try_new_for_scope(
                    &config,
                    scope.without_rack(),
                    topology_client.clone(),
                )?
            }
            _ => live_nodes.clone(),
        };
        if let (Some(routing_nodes), Some(affinity_nodes)) =
            (live_nodes.as_ref(), affinity_live_nodes.as_ref())
            && !std::sync::Arc::ptr_eq(routing_nodes, affinity_nodes)
        {
            builder.set_endpoint_resolver(Some(SharedEndpointResolver::new(
                AffinityDiscoveryEndpointResolver {
                    inner: dynamodb_config.endpoint_resolver(),
                    affinity_nodes: affinity_nodes.clone(),
                },
            )));
        }

        let routing_interceptor: Option<aws_sdk_dynamodb::config::SharedInterceptor> =
            match (live_nodes.as_ref(), affinity_config.as_ref()) {
                (None, _) => None,
                (Some(nodes), None) => Some(aws_sdk_dynamodb::config::SharedInterceptor::new(
                    RoundRobinQueryPlanInterceptor::new(nodes.clone()),
                )),
                (Some(nodes), Some(cfg)) => {
                    let affinity_nodes = affinity_live_nodes
                        .as_ref()
                        .expect("discovery state exists for affinity routing");
                    // The affinity interceptor needs a PartitionKeyResolver, which needs a client
                    // to make DescribeTable calls. Using the main client for that would create a
                    // cycle: main client -> affinity interceptor -> resolver -> DescribeTable
                    // -> main client. Build a separate discovery client from the same base config
                    // but with round-robin routing only.
                    let pk_discovery_client = auxiliary_dynamodb_client_from_conf(
                        builder
                            .clone()
                            .interceptor(RoundRobinQueryPlanInterceptor::new(nodes.clone()))
                            .build(),
                    )?;
                    let resolver =
                        std::sync::Arc::new(keyrouting::resolver::PartitionKeyResolver::new(
                            pk_discovery_client,
                            cfg.pk_info_per_table.clone(),
                        ));
                    Some(aws_sdk_dynamodb::config::SharedInterceptor::new(
                        AffinityQueryPlanInterceptor::new(
                            cfg.clone(),
                            nodes.clone(),
                            affinity_nodes.clone(),
                            resolver,
                        ),
                    ))
                }
            };

        if let Some(interceptor) = routing_interceptor {
            builder = builder.interceptor(interceptor);
        }

        let dynamodb_config = builder.build();

        let dynamodb_client = try_dynamodb_client_from_conf(dynamodb_config)?;

        if let Some(nodes) = &live_nodes {
            nodes.ensure_discovery_started();
        }
        if let Some(nodes) = &affinity_live_nodes {
            nodes.ensure_discovery_started();
        }

        Ok(Self {
            dynamodb_client,
            config,
        })
    }

    /// Returns the configuration used to construct this client.
    pub fn config(&self) -> &AlternatorConfig {
        &self.config
    }
}

// All implementations below this point should only be simple wrappers around dynamodb methods

impl AlternatorClient {
    /// Constructs a fluent builder for the DynamoDB `BatchExecuteStatement` operation.
    pub fn batch_execute_statement(&self) -> aws_sdk_dynamodb::operation::batch_execute_statement::builders::BatchExecuteStatementFluentBuilder{
        self.dynamodb_client.batch_execute_statement()
    }

    /// Constructs a fluent builder for the DynamoDB `BatchGetItem` operation.
    pub fn batch_get_item(
        &self,
    ) -> aws_sdk_dynamodb::operation::batch_get_item::builders::BatchGetItemFluentBuilder {
        self.dynamodb_client.batch_get_item()
    }

    /// Constructs a fluent builder for the DynamoDB `BatchWriteItem` operation.
    pub fn batch_write_item(
        &self,
    ) -> aws_sdk_dynamodb::operation::batch_write_item::builders::BatchWriteItemFluentBuilder {
        self.dynamodb_client.batch_write_item()
    }

    /// Constructs a fluent builder for the DynamoDB `CreateBackup` operation.
    pub fn create_backup(
        &self,
    ) -> aws_sdk_dynamodb::operation::create_backup::builders::CreateBackupFluentBuilder {
        self.dynamodb_client.create_backup()
    }

    /// Constructs a fluent builder for the DynamoDB `CreateGlobalTable` operation.
    pub fn create_global_table(
        &self,
    ) -> aws_sdk_dynamodb::operation::create_global_table::builders::CreateGlobalTableFluentBuilder
    {
        self.dynamodb_client.create_global_table()
    }

    /// Constructs a fluent builder for the DynamoDB `CreateTable` operation.
    pub fn create_table(
        &self,
    ) -> aws_sdk_dynamodb::operation::create_table::builders::CreateTableFluentBuilder {
        self.dynamodb_client.create_table()
    }

    /// Constructs a fluent builder for the DynamoDB `DeleteBackup` operation.
    pub fn delete_backup(
        &self,
    ) -> aws_sdk_dynamodb::operation::delete_backup::builders::DeleteBackupFluentBuilder {
        self.dynamodb_client.delete_backup()
    }

    /// Constructs a fluent builder for the DynamoDB `DeleteItem` operation.
    pub fn delete_item(
        &self,
    ) -> aws_sdk_dynamodb::operation::delete_item::builders::DeleteItemFluentBuilder {
        self.dynamodb_client.delete_item()
    }

    /// Constructs a fluent builder for the DynamoDB `DeleteResourcePolicy` operation.
    pub fn delete_resource_policy(&self) -> aws_sdk_dynamodb::operation::delete_resource_policy::builders::DeleteResourcePolicyFluentBuilder{
        self.dynamodb_client.delete_resource_policy()
    }

    /// Constructs a fluent builder for the DynamoDB `DeleteTable` operation.
    pub fn delete_table(
        &self,
    ) -> aws_sdk_dynamodb::operation::delete_table::builders::DeleteTableFluentBuilder {
        self.dynamodb_client.delete_table()
    }

    /// Constructs a fluent builder for the DynamoDB `DescribeBackup` operation.
    pub fn describe_backup(
        &self,
    ) -> aws_sdk_dynamodb::operation::describe_backup::builders::DescribeBackupFluentBuilder {
        self.dynamodb_client.describe_backup()
    }

    /// Constructs a fluent builder for the DynamoDB `DescribeContinuousBackups` operation.
    pub fn describe_continuous_backups(&self) -> aws_sdk_dynamodb::operation::describe_continuous_backups::builders::DescribeContinuousBackupsFluentBuilder{
        self.dynamodb_client.describe_continuous_backups()
    }

    /// Constructs a fluent builder for the DynamoDB `DescribeContributorInsights` operation.
	pub fn describe_contributor_insights(
        &self,
    ) -> aws_sdk_dynamodb::operation::describe_contributor_insights::builders::DescribeContributorInsightsFluentBuilder{
        self.dynamodb_client.describe_contributor_insights()
    }

    /// Constructs a fluent builder for the DynamoDB `DescribeEndpoints` operation.
    pub fn describe_endpoints(
        &self,
    ) -> aws_sdk_dynamodb::operation::describe_endpoints::builders::DescribeEndpointsFluentBuilder
    {
        self.dynamodb_client.describe_endpoints()
    }

    /// Constructs a fluent builder for the DynamoDB `DescribeExport` operation.
    pub fn describe_export(
        &self,
    ) -> aws_sdk_dynamodb::operation::describe_export::builders::DescribeExportFluentBuilder {
        self.dynamodb_client.describe_export()
    }

    /// Constructs a fluent builder for the DynamoDB `DescribeGlobalTable` operation.
    pub fn describe_global_table(&self) -> aws_sdk_dynamodb::operation::describe_global_table::builders::DescribeGlobalTableFluentBuilder{
        self.dynamodb_client.describe_global_table()
    }

    /// Constructs a fluent builder for the DynamoDB `DescribeGlobalTableSettings` operation.
	pub fn describe_global_table_settings(
        &self,
    ) -> aws_sdk_dynamodb::operation::describe_global_table_settings::builders::DescribeGlobalTableSettingsFluentBuilder{
        self.dynamodb_client.describe_global_table_settings()
    }

    /// Constructs a fluent builder for the DynamoDB `DescribeImport` operation.
    pub fn describe_import(
        &self,
    ) -> aws_sdk_dynamodb::operation::describe_import::builders::DescribeImportFluentBuilder {
        self.dynamodb_client.describe_import()
    }

    /// Constructs a fluent builder for the DynamoDB `DescribeKinesisStreamingDestination` operation.
	pub fn describe_kinesis_streaming_destination(
        &self,
    ) -> aws_sdk_dynamodb::operation::describe_kinesis_streaming_destination::builders::DescribeKinesisStreamingDestinationFluentBuilder{
        self.dynamodb_client
            .describe_kinesis_streaming_destination()
    }

    /// Constructs a fluent builder for the DynamoDB `DescribeLimits` operation.
    pub fn describe_limits(
        &self,
    ) -> aws_sdk_dynamodb::operation::describe_limits::builders::DescribeLimitsFluentBuilder {
        self.dynamodb_client.describe_limits()
    }

    /// Constructs a fluent builder for the DynamoDB `DescribeTable` operation.
    pub fn describe_table(
        &self,
    ) -> aws_sdk_dynamodb::operation::describe_table::builders::DescribeTableFluentBuilder {
        self.dynamodb_client.describe_table()
    }

    /// Constructs a fluent builder for the DynamoDB `DescribeTableReplicaAutoScaling` operation.
	pub fn describe_table_replica_auto_scaling(
        &self,
    ) -> aws_sdk_dynamodb::operation::describe_table_replica_auto_scaling::builders::DescribeTableReplicaAutoScalingFluentBuilder{
        self.dynamodb_client.describe_table_replica_auto_scaling()
    }

    /// Constructs a fluent builder for the DynamoDB `DescribeTimeToLive` operation.
    pub fn describe_time_to_live(
        &self,
    ) -> aws_sdk_dynamodb::operation::describe_time_to_live::builders::DescribeTimeToLiveFluentBuilder
    {
        self.dynamodb_client.describe_time_to_live()
    }

    /// Constructs a fluent builder for the DynamoDB `DisableKinesisStreamingDestination` operation.
	pub fn disable_kinesis_streaming_destination(
        &self,
    ) -> aws_sdk_dynamodb::operation::disable_kinesis_streaming_destination::builders::DisableKinesisStreamingDestinationFluentBuilder{
        self.dynamodb_client.disable_kinesis_streaming_destination()
    }

    /// Constructs a fluent builder for the DynamoDB `EnableKinesisStreamingDestination` operation.
	pub fn enable_kinesis_streaming_destination(
        &self,
    ) -> aws_sdk_dynamodb::operation::enable_kinesis_streaming_destination::builders::EnableKinesisStreamingDestinationFluentBuilder{
        self.dynamodb_client.enable_kinesis_streaming_destination()
    }

    /// Constructs a fluent builder for the DynamoDB `ExecuteStatement` operation.
    pub fn execute_statement(
        &self,
    ) -> aws_sdk_dynamodb::operation::execute_statement::builders::ExecuteStatementFluentBuilder
    {
        self.dynamodb_client.execute_statement()
    }

    /// Constructs a fluent builder for the DynamoDB `ExecuteTransaction` operation.
    pub fn execute_transaction(
        &self,
    ) -> aws_sdk_dynamodb::operation::execute_transaction::builders::ExecuteTransactionFluentBuilder
    {
        self.dynamodb_client.execute_transaction()
    }

    /// Constructs a fluent builder for the DynamoDB `ExportTableToPointInTime` operation.
    pub fn export_table_to_point_in_time(&self) -> aws_sdk_dynamodb::operation::export_table_to_point_in_time::builders::ExportTableToPointInTimeFluentBuilder{
        self.dynamodb_client.export_table_to_point_in_time()
    }

    /// Constructs a fluent builder for the DynamoDB `GetItem` operation.
    pub fn get_item(
        &self,
    ) -> aws_sdk_dynamodb::operation::get_item::builders::GetItemFluentBuilder {
        self.dynamodb_client.get_item()
    }

    /// Constructs a fluent builder for the DynamoDB `GetResourcePolicy` operation.
    pub fn get_resource_policy(
        &self,
    ) -> aws_sdk_dynamodb::operation::get_resource_policy::builders::GetResourcePolicyFluentBuilder
    {
        self.dynamodb_client.get_resource_policy()
    }

    /// Constructs a fluent builder for the DynamoDB `ImportTable` operation.
    pub fn import_table(
        &self,
    ) -> aws_sdk_dynamodb::operation::import_table::builders::ImportTableFluentBuilder {
        self.dynamodb_client.import_table()
    }

    /// Constructs a fluent builder for the DynamoDB `ListBackups` operation.
    pub fn list_backups(
        &self,
    ) -> aws_sdk_dynamodb::operation::list_backups::builders::ListBackupsFluentBuilder {
        self.dynamodb_client.list_backups()
    }

    /// Constructs a fluent builder for the DynamoDB `ListContributorInsights` operation.
    pub fn list_contributor_insights(&self) -> aws_sdk_dynamodb::operation::list_contributor_insights::builders::ListContributorInsightsFluentBuilder{
        self.dynamodb_client.list_contributor_insights()
    }

    /// Constructs a fluent builder for the DynamoDB `ListExports` operation.
    pub fn list_exports(
        &self,
    ) -> aws_sdk_dynamodb::operation::list_exports::builders::ListExportsFluentBuilder {
        self.dynamodb_client.list_exports()
    }

    /// Constructs a fluent builder for the DynamoDB `ListGlobalTables` operation.
    pub fn list_global_tables(
        &self,
    ) -> aws_sdk_dynamodb::operation::list_global_tables::builders::ListGlobalTablesFluentBuilder
    {
        self.dynamodb_client.list_global_tables()
    }

    /// Constructs a fluent builder for the DynamoDB `ListImports` operation.
    pub fn list_imports(
        &self,
    ) -> aws_sdk_dynamodb::operation::list_imports::builders::ListImportsFluentBuilder {
        self.dynamodb_client.list_imports()
    }

    /// Constructs a fluent builder for the DynamoDB `ListTables` operation.
    pub fn list_tables(
        &self,
    ) -> aws_sdk_dynamodb::operation::list_tables::builders::ListTablesFluentBuilder {
        self.dynamodb_client.list_tables()
    }

    /// Constructs a fluent builder for the DynamoDB `ListTagsOfResource` operation.
    pub fn list_tags_of_resource(
        &self,
    ) -> aws_sdk_dynamodb::operation::list_tags_of_resource::builders::ListTagsOfResourceFluentBuilder
    {
        self.dynamodb_client.list_tags_of_resource()
    }

    /// Constructs a fluent builder for the DynamoDB `PutItem` operation.
    pub fn put_item(
        &self,
    ) -> aws_sdk_dynamodb::operation::put_item::builders::PutItemFluentBuilder {
        self.dynamodb_client.put_item()
    }

    /// Constructs a fluent builder for the DynamoDB `PutResourcePolicy` operation.
    pub fn put_resource_policy(
        &self,
    ) -> aws_sdk_dynamodb::operation::put_resource_policy::builders::PutResourcePolicyFluentBuilder
    {
        self.dynamodb_client.put_resource_policy()
    }

    /// Constructs a fluent builder for the DynamoDB `Query` operation.
    pub fn query(&self) -> aws_sdk_dynamodb::operation::query::builders::QueryFluentBuilder {
        self.dynamodb_client.query()
    }

    /// Constructs a fluent builder for the DynamoDB `RestoreTableFromBackup` operation.
    pub fn restore_table_from_backup(&self) -> aws_sdk_dynamodb::operation::restore_table_from_backup::builders::RestoreTableFromBackupFluentBuilder{
        self.dynamodb_client.restore_table_from_backup()
    }

    /// Constructs a fluent builder for the DynamoDB `RestoreTableToPointInTime` operation.
	pub fn restore_table_to_point_in_time(
        &self,
    ) -> aws_sdk_dynamodb::operation::restore_table_to_point_in_time::builders::RestoreTableToPointInTimeFluentBuilder{
        self.dynamodb_client.restore_table_to_point_in_time()
    }

    /// Constructs a fluent builder for the DynamoDB `Scan` operation.
    pub fn scan(&self) -> aws_sdk_dynamodb::operation::scan::builders::ScanFluentBuilder {
        self.dynamodb_client.scan()
    }

    /// Constructs a fluent builder for the DynamoDB `TagResource` operation.
    pub fn tag_resource(
        &self,
    ) -> aws_sdk_dynamodb::operation::tag_resource::builders::TagResourceFluentBuilder {
        self.dynamodb_client.tag_resource()
    }

    /// Constructs a fluent builder for the DynamoDB `TransactGetItems` operation.
    pub fn transact_get_items(
        &self,
    ) -> aws_sdk_dynamodb::operation::transact_get_items::builders::TransactGetItemsFluentBuilder
    {
        self.dynamodb_client.transact_get_items()
    }

    /// Constructs a fluent builder for the DynamoDB `TransactWriteItems` operation.
    pub fn transact_write_items(
        &self,
    ) -> aws_sdk_dynamodb::operation::transact_write_items::builders::TransactWriteItemsFluentBuilder
    {
        self.dynamodb_client.transact_write_items()
    }

    /// Constructs a fluent builder for the DynamoDB `UntagResource` operation.
    pub fn untag_resource(
        &self,
    ) -> aws_sdk_dynamodb::operation::untag_resource::builders::UntagResourceFluentBuilder {
        self.dynamodb_client.untag_resource()
    }

    /// Constructs a fluent builder for the DynamoDB `UpdateContinuousBackups` operation.
    pub fn update_continuous_backups(&self) -> aws_sdk_dynamodb::operation::update_continuous_backups::builders::UpdateContinuousBackupsFluentBuilder{
        self.dynamodb_client.update_continuous_backups()
    }

    /// Constructs a fluent builder for the DynamoDB `UpdateContributorInsights` operation.
    pub fn update_contributor_insights(&self) -> aws_sdk_dynamodb::operation::update_contributor_insights::builders::UpdateContributorInsightsFluentBuilder{
        self.dynamodb_client.update_contributor_insights()
    }

    /// Constructs a fluent builder for the DynamoDB `UpdateGlobalTable` operation.
    pub fn update_global_table(
        &self,
    ) -> aws_sdk_dynamodb::operation::update_global_table::builders::UpdateGlobalTableFluentBuilder
    {
        self.dynamodb_client.update_global_table()
    }

    /// Constructs a fluent builder for the DynamoDB `UpdateGlobalTableSettings` operation.
    pub fn update_global_table_settings(&self) -> aws_sdk_dynamodb::operation::update_global_table_settings::builders::UpdateGlobalTableSettingsFluentBuilder{
        self.dynamodb_client.update_global_table_settings()
    }

    /// Constructs a fluent builder for the DynamoDB `UpdateItem` operation.
    pub fn update_item(
        &self,
    ) -> aws_sdk_dynamodb::operation::update_item::builders::UpdateItemFluentBuilder {
        self.dynamodb_client.update_item()
    }

    /// Constructs a fluent builder for the DynamoDB `UpdateKinesisStreamingDestination` operation.
	pub fn update_kinesis_streaming_destination(
        &self,
    ) -> aws_sdk_dynamodb::operation::update_kinesis_streaming_destination::builders::UpdateKinesisStreamingDestinationFluentBuilder{
        self.dynamodb_client.update_kinesis_streaming_destination()
    }

    /// Constructs a fluent builder for the DynamoDB `UpdateTable` operation.
    pub fn update_table(
        &self,
    ) -> aws_sdk_dynamodb::operation::update_table::builders::UpdateTableFluentBuilder {
        self.dynamodb_client.update_table()
    }

    /// Constructs a fluent builder for the DynamoDB `UpdateTableReplicaAutoScaling` operation.
	pub fn update_table_replica_auto_scaling(
        &self,
    ) -> aws_sdk_dynamodb::operation::update_table_replica_auto_scaling::builders::UpdateTableReplicaAutoScalingFluentBuilder{
        self.dynamodb_client.update_table_replica_auto_scaling()
    }

    /// Constructs a fluent builder for the DynamoDB `UpdateTimeToLive` operation.
    pub fn update_time_to_live(
        &self,
    ) -> aws_sdk_dynamodb::operation::update_time_to_live::builders::UpdateTimeToLiveFluentBuilder
    {
        self.dynamodb_client.update_time_to_live()
    }
}

impl aws_sdk_dynamodb::client::Waiters for AlternatorClient {
    fn wait_until_contributor_insights_enabled(&self) -> aws_sdk_dynamodb::waiters::contributor_insights_enabled::ContributorInsightsEnabledFluentBuilder{
        self.dynamodb_client
            .wait_until_contributor_insights_enabled()
    }

    fn wait_until_export_completed(
        &self,
    ) -> aws_sdk_dynamodb::waiters::export_completed::ExportCompletedFluentBuilder {
        self.dynamodb_client.wait_until_export_completed()
    }

    fn wait_until_import_completed(
        &self,
    ) -> aws_sdk_dynamodb::waiters::import_completed::ImportCompletedFluentBuilder {
        self.dynamodb_client.wait_until_import_completed()
    }

    fn wait_until_kinesis_streaming_destination_active(
        &self,
    ) -> aws_sdk_dynamodb::waiters::kinesis_streaming_destination_active::KinesisStreamingDestinationActiveFluentBuilder{
        self.dynamodb_client
            .wait_until_kinesis_streaming_destination_active()
    }

    fn wait_until_table_exists(
        &self,
    ) -> aws_sdk_dynamodb::waiters::table_exists::TableExistsFluentBuilder {
        self.dynamodb_client.wait_until_table_exists()
    }

    fn wait_until_table_not_exists(
        &self,
    ) -> aws_sdk_dynamodb::waiters::table_not_exists::TableNotExistsFluentBuilder {
        self.dynamodb_client.wait_until_table_not_exists()
    }
}

#[cfg(test)]
mod tests {

    use super::*;
    use aws_sdk_dynamodb::config::Intercept;
    use aws_smithy_runtime_api::{
        box_error::BoxError,
        client::{
            http::{
                HttpClient, HttpConnector, HttpConnectorFuture, HttpConnectorSettings,
                SharedHttpClient, SharedHttpConnector,
            },
            orchestrator::{HttpRequest, HttpResponse},
            runtime_components::{RuntimeComponents, RuntimeComponentsBuilder},
        },
        http::StatusCode,
    };
    use aws_smithy_types::body::SdkBody;
    use aws_smithy_types::config_bag::ConfigBag;
    use itertools::Itertools;

    fn error_chain(error: &(dyn std::error::Error + 'static)) -> String {
        let mut messages = vec![error.to_string()];
        let mut source = error.source();
        while let Some(error) = source {
            messages.push(error.to_string());
            source = error.source();
        }
        messages.join(": ")
    }

    #[derive(Debug)]
    struct InvalidHttpClient;

    impl HttpClient for InvalidHttpClient {
        fn http_connector(
            &self,
            _: &HttpConnectorSettings,
            _: &RuntimeComponents,
        ) -> SharedHttpConnector {
            unreachable!("an invalid HTTP client must not be used")
        }

        fn validate_base_client_config(
            &self,
            _: &RuntimeComponentsBuilder,
            _: &ConfigBag,
        ) -> Result<(), BoxError> {
            Err(std::io::Error::other("invalid test HTTP client configuration").into())
        }
    }

    #[derive(Debug)]
    struct SingleValidationHttpClient(std::sync::Arc<std::sync::atomic::AtomicUsize>);

    impl HttpClient for SingleValidationHttpClient {
        fn http_connector(
            &self,
            _: &HttpConnectorSettings,
            _: &RuntimeComponents,
        ) -> SharedHttpConnector {
            unreachable!("the validation-only client must not send requests")
        }

        fn validate_base_client_config(
            &self,
            _: &RuntimeComponentsBuilder,
            _: &ConfigBag,
        ) -> Result<(), BoxError> {
            match self.0.fetch_add(1, std::sync::atomic::Ordering::Relaxed) {
                0 => Ok(()),
                _ => Err(std::io::Error::other("HTTP client was validated twice").into()),
            }
        }
    }

    #[derive(Debug)]
    struct InvalidIdentityCache;

    impl ResolveCachedIdentity for InvalidIdentityCache {
        fn resolve_cached_identity<'a>(
            &'a self,
            _: SharedIdentityResolver,
            _: &'a RuntimeComponents,
            _: &'a ConfigBag,
        ) -> IdentityFuture<'a> {
            unreachable!("an invalid identity cache must not be used")
        }

        fn validate_base_client_config(
            &self,
            _: &RuntimeComponentsBuilder,
            _: &ConfigBag,
        ) -> Result<(), BoxError> {
            Err(std::io::Error::other("invalid test identity cache configuration").into())
        }
    }

    #[derive(Debug)]
    struct FinalValidationIdentityCache(&'static str);

    impl ResolveCachedIdentity for FinalValidationIdentityCache {
        fn resolve_cached_identity<'a>(
            &'a self,
            _: SharedIdentityResolver,
            _: &'a RuntimeComponents,
            _: &'a ConfigBag,
        ) -> IdentityFuture<'a> {
            unreachable!("final validation must fail before identity resolution")
        }

        fn validate_final_config(
            &self,
            _: &RuntimeComponents,
            _: &ConfigBag,
        ) -> Result<(), BoxError> {
            Err(std::io::Error::other(self.0).into())
        }
    }

    #[derive(Debug)]
    struct RuntimeComponentsHttpClient(std::sync::Arc<std::sync::atomic::AtomicUsize>);

    impl HttpClient for RuntimeComponentsHttpClient {
        fn http_connector(
            &self,
            _: &HttpConnectorSettings,
            runtime_components: &RuntimeComponents,
        ) -> SharedHttpConnector {
            self.0.store(
                runtime_components as *const RuntimeComponents as usize,
                std::sync::atomic::Ordering::SeqCst,
            );
            SharedHttpConnector::new(UnusedHttpConnector)
        }
    }

    #[derive(Debug)]
    struct UnusedHttpConnector;

    impl HttpConnector for UnusedHttpConnector {
        fn call(&self, _: HttpRequest) -> HttpConnectorFuture {
            unreachable!("the adapter delegation test does not send a request")
        }
    }

    #[derive(Debug, Default)]
    struct CapturedRequest {
        calls: std::sync::atomic::AtomicUsize,
        authorization: std::sync::atomic::AtomicBool,
        amz_date: std::sync::atomic::AtomicBool,
        content_type: std::sync::atomic::AtomicBool,
        amz_user_agent: std::sync::atomic::AtomicBool,
        security_token: std::sync::atomic::AtomicBool,
    }

    #[derive(Clone, Debug)]
    struct SuccessfulHttpClient(std::sync::Arc<CapturedRequest>);

    impl HttpClient for SuccessfulHttpClient {
        fn http_connector(
            &self,
            _: &HttpConnectorSettings,
            _: &RuntimeComponents,
        ) -> SharedHttpConnector {
            SharedHttpConnector::new(self.clone())
        }
    }

    impl HttpConnector for SuccessfulHttpClient {
        fn call(&self, request: HttpRequest) -> HttpConnectorFuture {
            self.0
                .calls
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            self.0.authorization.store(
                request.headers().contains_key("authorization"),
                std::sync::atomic::Ordering::SeqCst,
            );
            self.0.amz_date.store(
                request.headers().contains_key("x-amz-date"),
                std::sync::atomic::Ordering::SeqCst,
            );
            self.0.content_type.store(
                request.headers().contains_key("content-type"),
                std::sync::atomic::Ordering::SeqCst,
            );
            self.0.amz_user_agent.store(
                request.headers().contains_key("x-amz-user-agent"),
                std::sync::atomic::Ordering::SeqCst,
            );
            self.0.security_token.store(
                request.headers().contains_key("x-amz-security-token"),
                std::sync::atomic::Ordering::SeqCst,
            );
            HttpConnectorFuture::ready(Ok(HttpResponse::new(
                StatusCode::try_from(200).unwrap(),
                SdkBody::from(r#"{"TableNames":[]}"#),
            )))
        }
    }

    #[derive(Clone, Debug)]
    struct CountingCredentialsProvider(std::sync::Arc<std::sync::atomic::AtomicUsize>);

    impl aws_credential_types::provider::ProvideCredentials for CountingCredentialsProvider {
        fn provide_credentials<'a>(
            &'a self,
        ) -> aws_credential_types::provider::future::ProvideCredentials<'a>
        where
            Self: 'a,
        {
            self.0.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            aws_credential_types::provider::future::ProvideCredentials::ready(Ok(
                aws_sdk_dynamodb::config::Credentials::for_tests_with_session_token(),
            ))
        }
    }

    fn removable_credentials_builder(
        captured: std::sync::Arc<CapturedRequest>,
        provider_calls: std::sync::Arc<std::sync::atomic::AtomicUsize>,
    ) -> AlternatorBuilder {
        AlternatorConfig::builder()
            .seed_hosts(["127.0.0.1"])
            .port(8000)
            .without_discovery()
            .optimize_headers(false)
            .http_client(SuccessfulHttpClient(captured))
            .credentials_provider(CountingCredentialsProvider(provider_calls))
    }

    fn assert_unsigned_request(captured: &CapturedRequest) {
        assert_eq!(captured.calls.load(std::sync::atomic::Ordering::SeqCst), 1);
        assert!(
            !captured
                .authorization
                .load(std::sync::atomic::Ordering::SeqCst)
        );
        assert!(!captured.amz_date.load(std::sync::atomic::Ordering::SeqCst));
        assert!(
            !captured
                .security_token
                .load(std::sync::atomic::Ordering::SeqCst)
        );
    }

    #[tokio::test]
    async fn optimized_unsigned_request_uses_base_header_allowlist() {
        let captured = std::sync::Arc::new(CapturedRequest::default());
        let client = AlternatorClient::try_from_conf(
            AlternatorConfig::builder()
                .seed_hosts(["127.0.0.1"])
                .port(8000)
                .without_discovery()
                .optimize_headers(true)
                .http_client(SuccessfulHttpClient(captured.clone()))
                .build(),
        )
        .unwrap();

        let result = client.list_tables().send().await;

        assert!(result.is_ok(), "request failed: {result:?}");
        assert_eq!(captured.calls.load(std::sync::atomic::Ordering::SeqCst), 1);
        assert!(
            captured
                .content_type
                .load(std::sync::atomic::Ordering::SeqCst)
        );
        assert!(
            !captured
                .authorization
                .load(std::sync::atomic::Ordering::SeqCst)
        );
        assert!(!captured.amz_date.load(std::sync::atomic::Ordering::SeqCst));
        assert!(
            !captured
                .amz_user_agent
                .load(std::sync::atomic::Ordering::SeqCst)
        );
        assert!(
            !captured
                .security_token
                .load(std::sync::atomic::Ordering::SeqCst)
        );
    }

    #[tokio::test]
    async fn optimized_signed_request_uses_authenticated_header_allowlist() {
        let captured = std::sync::Arc::new(CapturedRequest::default());
        let client = AlternatorClient::try_from_conf(
            AlternatorConfig::builder()
                .seed_hosts(["127.0.0.1"])
                .port(8000)
                .without_discovery()
                .optimize_headers(true)
                .http_client(SuccessfulHttpClient(captured.clone()))
                .credentials_provider(
                    aws_sdk_dynamodb::config::Credentials::for_tests_with_session_token(),
                )
                .build(),
        )
        .unwrap();

        let result = client.list_tables().send().await;

        assert!(result.is_ok(), "request failed: {result:?}");
        assert_eq!(captured.calls.load(std::sync::atomic::Ordering::SeqCst), 1);
        assert!(
            captured
                .authorization
                .load(std::sync::atomic::Ordering::SeqCst)
        );
        assert!(captured.amz_date.load(std::sync::atomic::Ordering::SeqCst));
        assert!(
            captured
                .content_type
                .load(std::sync::atomic::Ordering::SeqCst)
        );
        assert!(
            captured
                .amz_user_agent
                .load(std::sync::atomic::Ordering::SeqCst)
        );
        assert!(
            captured
                .security_token
                .load(std::sync::atomic::Ordering::SeqCst)
        );
    }

    #[tokio::test]
    async fn topology_discovery_uses_configured_credentials() {
        let captured = std::sync::Arc::new(CapturedRequest::default());
        let client = AlternatorClient::try_from_conf(
            AlternatorConfig::builder()
                .seed_hosts(["127.0.0.1"])
                .port(8000)
                .active_interval(std::time::Duration::from_secs(60))
                .optimize_headers(true)
                .http_client(SuccessfulHttpClient(captured.clone()))
                .credentials_provider(
                    aws_sdk_dynamodb::config::Credentials::for_tests_with_session_token(),
                )
                .build(),
        )
        .unwrap();

        tokio::time::timeout(std::time::Duration::from_secs(1), async {
            while captured.calls.load(std::sync::atomic::Ordering::SeqCst) < 2 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("topology discovery should scan system.local and system.peers");
        drop(client);

        assert!(
            captured
                .authorization
                .load(std::sync::atomic::Ordering::SeqCst)
        );
        assert!(captured.amz_date.load(std::sync::atomic::Ordering::SeqCst));
        assert!(
            captured
                .security_token
                .load(std::sync::atomic::Ordering::SeqCst)
        );
    }

    #[derive(Debug)]
    struct RuntimeComponentsIdentityCache(std::sync::Arc<std::sync::atomic::AtomicUsize>);

    impl ResolveCachedIdentity for RuntimeComponentsIdentityCache {
        fn resolve_cached_identity<'a>(
            &'a self,
            _: SharedIdentityResolver,
            runtime_components: &'a RuntimeComponents,
            _: &'a ConfigBag,
        ) -> IdentityFuture<'a> {
            self.0.store(
                runtime_components as *const RuntimeComponents as usize,
                std::sync::atomic::Ordering::SeqCst,
            );
            IdentityFuture::ready(Err(std::io::Error::other("test identity stop").into()))
        }
    }

    #[test]
    fn validation_adapters_reuse_components_in_runtime_delegates() {
        let http_components = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let identity_components = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let validation = SdkConfigValidation {
            error: Default::default(),
            http_client: SharedHttpClient::new(RuntimeComponentsHttpClient(
                http_components.clone(),
            )),
            identity_cache: SharedIdentityCache::new(RuntimeComponentsIdentityCache(
                identity_components.clone(),
            )),
        };
        let runtime_components_builder = RuntimeComponentsBuilder::for_tests();
        let identity_resolver = runtime_components_builder
            .identity_resolver(&aws_smithy_runtime_api::client::auth::AuthSchemeId::new(
                "fake",
            ))
            .unwrap();
        let runtime_components = runtime_components_builder.build().unwrap();
        let expected = &runtime_components as *const RuntimeComponents as usize;

        let _connector = validation.http_connector(
            &HttpConnectorSettings::builder().build(),
            &runtime_components,
        );
        let config = ConfigBag::base();
        let identity_cache = ValidatedIdentityCache(validation);
        let _identity =
            identity_cache.resolve_cached_identity(identity_resolver, &runtime_components, &config);

        assert_eq!(
            http_components.load(std::sync::atomic::Ordering::SeqCst),
            expected
        );
        assert_eq!(
            identity_components.load(std::sync::atomic::Ordering::SeqCst),
            expected
        );
    }

    #[test]
    fn test_client_adds_hooks_to_inner_client() {
        let client = AlternatorClient::from_conf(
            AlternatorConfig::builder()
                .seed_hosts(["127.0.0.1"])
                .port(8000)
                .build(),
        );

        let inner_config = client.dynamodb_client.config();

        assert!(inner_config.region().is_some());

        assert!(
            inner_config
                .interceptors()
                .filter(|interceptor| interceptor.name() == "AlternatorInterceptor")
                .exactly_one()
                .is_ok()
        );
    }

    #[tokio::test]
    async fn removing_credentials_provider_from_rebuilt_config_sends_unsigned() {
        let captured = std::sync::Arc::new(CapturedRequest::default());
        let provider_calls = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let config =
            removable_credentials_builder(captured.clone(), provider_calls.clone()).build();
        let mut builder = config.to_builder();
        builder.set_credentials_provider(None);

        let client = AlternatorClient::try_from_conf(builder.build()).unwrap();
        let result = client.list_tables().send().await;

        assert!(result.is_ok(), "request failed: {result:?}");
        assert_eq!(provider_calls.load(std::sync::atomic::Ordering::SeqCst), 0);
        assert_unsigned_request(&captured);
    }

    #[tokio::test]
    async fn removing_credentials_provider_from_same_builder_sends_unsigned() {
        let captured = std::sync::Arc::new(CapturedRequest::default());
        let provider_calls = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let mut builder = removable_credentials_builder(captured.clone(), provider_calls.clone());
        builder.set_credentials_provider(None);

        let client = AlternatorClient::try_from_conf(builder.build()).unwrap();
        let result = client.list_tables().send().await;

        assert!(result.is_ok(), "request failed: {result:?}");
        assert_eq!(provider_calls.load(std::sync::atomic::Ordering::SeqCst), 0);
        assert_unsigned_request(&captured);
    }

    #[tokio::test]
    async fn require_auth_after_removing_credentials_fails_before_transport() {
        let captured = std::sync::Arc::new(CapturedRequest::default());
        let provider_calls = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let mut builder = removable_credentials_builder(captured.clone(), provider_calls.clone());
        builder
            .set_credentials_provider(None)
            .set_require_auth(true);

        let client = AlternatorClient::try_from_conf(builder.build()).unwrap();
        let result = client.list_tables().send().await;

        assert!(result.is_err());
        assert_eq!(provider_calls.load(std::sync::atomic::Ordering::SeqCst), 0);
        assert_eq!(captured.calls.load(std::sync::atomic::Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn per_request_credentials_work_after_removing_default_credentials() {
        let captured = std::sync::Arc::new(CapturedRequest::default());
        let provider_calls = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let mut builder = removable_credentials_builder(captured.clone(), provider_calls.clone());
        builder
            .set_credentials_provider(None)
            .set_require_auth(true);

        let client = AlternatorClient::try_from_conf(builder.build()).unwrap();
        let result = client
            .list_tables()
            .customize()
            .config_override(aws_sdk_dynamodb::Config::builder().credentials_provider(
                aws_sdk_dynamodb::config::Credentials::for_tests_with_session_token(),
            ))
            .send()
            .await;

        assert!(result.is_ok(), "request failed: {result:?}");
        assert_eq!(provider_calls.load(std::sync::atomic::Ordering::SeqCst), 0);
        assert_eq!(captured.calls.load(std::sync::atomic::Ordering::SeqCst), 1);
        assert!(
            captured
                .authorization
                .load(std::sync::atomic::Ordering::SeqCst)
        );
        assert!(captured.amz_date.load(std::sync::atomic::Ordering::SeqCst));
        assert!(
            captured
                .security_token
                .load(std::sync::atomic::Ordering::SeqCst)
        );
    }

    #[test]
    fn test_client_stores_his_config_for_reference_only() {
        let client = AlternatorClient::from_conf(
            AlternatorConfig::builder()
                .optimize_headers(true)
                .seed_hosts(["127.0.0.1"])
                .port(8000)
                .build(),
        );

        let reference_config = client.config();

        assert_eq!(
            reference_config
                .interceptors()
                .try_len()
                .expect("does not have length"),
            0
        )
    }

    /// The driver pins the SDK behavior major version itself, so a config that
    /// never mentions one still builds instead of hitting the SDK's "a behavior
    /// major version must be set" panic.
    #[test]
    fn try_from_conf_pins_the_behavior_version_itself() {
        let client = AlternatorClient::try_from_conf(
            AlternatorConfig::builder()
                .seed_hosts(["127.0.0.1"])
                .port(8000)
                .without_discovery()
                .build(),
        );

        assert!(client.is_ok());
    }

    #[test]
    #[should_panic(expected = "A behavior major version must be set")]
    fn dependency_does_not_default_behavior_version_for_direct_sdk_clients() {
        let _ = aws_sdk_dynamodb::Client::from_conf(aws_sdk_dynamodb::Config::builder().build());
    }

    #[test]
    fn try_from_conf_returns_sdk_validation_errors() {
        for affinity in [
            None,
            Some(crate::keyrouting::KeyRouteAffinityType::AnyWrite),
        ] {
            let mut builder = AlternatorConfig::builder()
                .scheme("http")
                .port(8000)
                .seed_hosts(["127.0.0.1"])
                .http_client(InvalidHttpClient);
            if let Some(affinity) = affinity {
                builder.set_key_route_affinity(affinity);
            }

            let error = AlternatorClient::try_from_conf(builder.build()).unwrap_err();
            assert_eq!(
                error.to_string(),
                "failed to configure AWS SDK client: invalid test HTTP client configuration"
            );
        }
    }

    #[test]
    fn try_from_conf_returns_identity_cache_validation_errors() {
        let error = AlternatorClient::try_from_conf(
            AlternatorConfig::builder()
                .seed_hosts(["127.0.0.1"])
                .port(8000)
                .without_discovery()
                .identity_cache(InvalidIdentityCache)
                .build(),
        )
        .unwrap_err();

        assert_eq!(
            error.to_string(),
            "failed to configure AWS SDK client: invalid test identity cache configuration"
        );
    }

    #[test]
    fn try_from_conf_does_not_trigger_panic_hook_for_sdk_validation_errors() {
        const CHILD_ENV: &str = "ALTERNATOR_TEST_ABORT_ON_SDK_VALIDATION_PANIC";

        if std::env::var_os(CHILD_ENV).is_some() {
            std::panic::set_hook(Box::new(|panic_info| {
                eprintln!("unexpected panic during fallible construction: {panic_info}");
                std::process::abort();
            }));

            let base_builder = || {
                AlternatorConfig::builder()
                    .seed_hosts(["127.0.0.1"])
                    .port(8000)
                    .without_discovery()
            };
            let mut stalled_stream_config_unset = base_builder();
            stalled_stream_config_unset.set_stalled_stream_protection(None);
            let error =
                AlternatorClient::try_from_conf(stalled_stream_config_unset.build()).unwrap_err();
            assert!(
                error
                    .to_string()
                    .contains("The default stalled stream protection config was removed"),
                "unexpected SDK config error: {error}"
            );

            let error = AlternatorClient::try_from_conf(
                AlternatorConfig::builder()
                    .seed_hosts(["127.0.0.1"])
                    .port(8000)
                    .without_discovery()
                    .http_client(InvalidHttpClient)
                    .build(),
            )
            .unwrap_err();
            assert_eq!(
                error.to_string(),
                "failed to configure AWS SDK client: invalid test HTTP client configuration"
            );

            let validation_count = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
            let client = AlternatorClient::try_from_conf(
                AlternatorConfig::builder()
                    .seed_hosts(["127.0.0.1"])
                    .port(8000)
                    .without_discovery()
                    .http_client(SingleValidationHttpClient(validation_count.clone()))
                    .build(),
            )
            .unwrap();
            assert_eq!(
                validation_count.load(std::sync::atomic::Ordering::Relaxed),
                1
            );
            drop(client);

            let discovery_validation_count =
                std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
            let client = AlternatorClient::try_from_conf(
                AlternatorConfig::builder()
                    .seed_hosts(["127.0.0.1"])
                    .port(8000)
                    .http_client(SingleValidationHttpClient(
                        discovery_validation_count.clone(),
                    ))
                    .build(),
            )
            .unwrap();
            assert_eq!(
                discovery_validation_count.load(std::sync::atomic::Ordering::Relaxed),
                1
            );
            drop(client);

            let affinity_validation_count =
                std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
            let client = AlternatorClient::try_from_conf(
                AlternatorConfig::builder()
                    .seed_hosts(["127.0.0.1"])
                    .port(8000)
                    .http_client(SingleValidationHttpClient(
                        affinity_validation_count.clone(),
                    ))
                    .key_route_affinity(crate::keyrouting::KeyRouteAffinityType::AnyWrite)
                    .build(),
            )
            .unwrap();
            assert_eq!(
                affinity_validation_count.load(std::sync::atomic::Ordering::Relaxed),
                1
            );
            drop(client);

            let mut restored_sdk_defaults = base_builder();
            // These SDK setters intentionally treat None as a no-op.
            restored_sdk_defaults.set_retry_config(None);
            restored_sdk_defaults.set_timeout_config(None);
            restored_sdk_defaults.set_stalled_stream_protection(None);
            restored_sdk_defaults.set_stalled_stream_protection(Some(
                aws_sdk_dynamodb::config::StalledStreamProtectionConfig::disabled(),
            ));
            AlternatorClient::try_from_conf(restored_sdk_defaults.build()).unwrap();
            return;
        }

        let output = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "client::tests::try_from_conf_does_not_trigger_panic_hook_for_sdk_validation_errors",
                "--nocapture",
            ])
            .env(CHILD_ENV, "1")
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "child process failed\nstdout:\n{}\nstderr:\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr),
        );
    }

    #[tokio::test]
    async fn validation_adapters_preserve_per_operation_component_overrides() {
        let client = AlternatorClient::try_from_conf(
            AlternatorConfig::builder()
                .seed_hosts(["127.0.0.1"])
                .port(8000)
                .without_discovery()
                .identity_cache(FinalValidationIdentityCache("base identity cache used"))
                .build(),
        )
        .unwrap();

        let identity_override_error = client
            .list_tables()
            .customize()
            .config_override(aws_sdk_dynamodb::Config::builder().identity_cache(
                FinalValidationIdentityCache("operation identity cache used"),
            ))
            .send()
            .await
            .unwrap_err();
        let identity_override_error = error_chain(&identity_override_error);
        assert!(
            identity_override_error.contains("operation identity cache used"),
            "unexpected operation error: {identity_override_error}"
        );
        assert!(!identity_override_error.contains("base identity cache used"));

        let http_override_error = client
            .list_tables()
            .customize()
            .config_override(
                aws_sdk_dynamodb::Config::builder()
                    .http_client(aws_smithy_http_client::Builder::new().build_http()),
            )
            .send()
            .await
            .unwrap_err();
        let http_override_error = error_chain(&http_override_error);
        assert!(
            http_override_error.contains("base identity cache used"),
            "unexpected operation error: {http_override_error}"
        );
    }

    #[test]
    fn try_from_conf_rejects_missing_or_invalid_routing_configuration() {
        let missing =
            AlternatorClient::try_from_conf(AlternatorConfig::builder().build()).unwrap_err();
        assert!(missing.to_string().contains("no Alternator routing target"));

        let invalid = AlternatorClient::try_from_conf(
            AlternatorConfig::builder()
                .scheme("http")
                .seed_hosts(["127.0.0.1:invalid"])
                .build(),
        )
        .unwrap_err();
        assert!(invalid.to_string().contains("invalid seed host"));

        let invalid_scheme = AlternatorClient::try_from_conf(
            AlternatorConfig::builder()
                .scheme("https://dynamodb.us-east-1.amazonaws.com/x")
                .seed_hosts(["127.0.0.1"])
                .build(),
        )
        .unwrap_err();
        assert!(
            invalid_scheme
                .to_string()
                .contains("invalid Alternator transport scheme")
        );

        let invalid_direct = AlternatorClient::try_from_conf(
            AlternatorConfig::builder().without_discovery().build(),
        )
        .unwrap_err();
        assert!(
            invalid_direct
                .to_string()
                .contains("no Alternator routing target")
        );

        for scheme in ["ftp", "ws"] {
            let unsupported_direct = AlternatorClient::try_from_conf(
                AlternatorConfig::builder()
                    .scheme(scheme)
                    .seed_hosts(["host"])
                    .without_discovery()
                    .build(),
            )
            .unwrap_err();
            assert!(
                unsupported_direct
                    .to_string()
                    .contains("invalid Alternator transport scheme")
            );
        }
    }
}
