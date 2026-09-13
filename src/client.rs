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
    LiveNodesBuildError, ensure_native_roots_are_usable, native_roots_are_usable,
};
use crate::*;

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
///    .behavior_version_latest()
///    .endpoint_url("http://127.0.0.1:8000")
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
    TlsConfiguration(String),
    LiveNodes(LiveNodesBuildError),
}

impl std::fmt::Display for AlternatorClientBuildError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self.kind {
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

fn sdk_http_client(
    behavior_version: aws_sdk_dynamodb::config::BehaviorVersion,
    use_tls: bool,
) -> aws_sdk_dynamodb::config::SharedHttpClient {
    aws_smithy_http_client::Builder::new().build_with_connector_fn(
        move |settings, runtime_components| {
            let mut connector = aws_smithy_http_client::ConnectorBuilder::default();
            connector.set_connector_settings(settings.cloned());
            if let Some(components) = runtime_components {
                connector.set_sleep_impl(components.sleep_impl());
            }

            #[allow(deprecated)]
            let proxy_config = if behavior_version
                .is_at_least(aws_sdk_dynamodb::config::BehaviorVersion::v2025_08_07())
            {
                aws_smithy_http_client::proxy::ProxyConfig::from_env()
            } else {
                aws_smithy_http_client::proxy::ProxyConfig::disabled()
            };
            connector.set_proxy_config(Some(proxy_config));

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
        // The SDK's behavior-version-latest feature supplies this default
        // during client construction. Mirror it here for transport selection,
        // while preserving any explicitly configured older version.
        let behavior_version = config
            .behavior_version()
            .unwrap_or_else(aws_sdk_dynamodb::config::BehaviorVersion::latest);

        let dynamodb_config = config.dynamodb_config.clone();
        let extensions = config.alternator_ext.clone();

        let request_compression = extensions
            .request_compression
            .unwrap_or(RequestCompression::disabled());
        let response_compression = extensions.response_compression.unwrap_or_default();
        let optimize_headers = extensions.optimize_headers.unwrap_or(true);
        let user_agent = extensions.user_agent.unwrap_or_default();
        let has_credentials_provider = config.has_credentials_provider();
        let has_region = dynamodb_config.region().is_some();

        let mut builder = dynamodb_config.to_builder();

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

        // If live nodes are not in config - create new config with live nodes.
        let (config, live_nodes) = if let Some(nodes) = config.live_nodes() {
            (config, Some(nodes))
        } else if let Some(nodes) = LiveNodes::try_new(&config)? {
            let config = config
                .to_builder()
                .auto_created_live_nodes(nodes.clone())
                .build();
            (config, Some(nodes))
        } else {
            (config, None)
        };

        let uses_plaintext_transport = live_nodes
            .as_ref()
            .is_some_and(|nodes| nodes.scheme() == "http")
            || live_nodes.is_none()
                && config
                    .endpoint_url()
                    .and_then(|endpoint| url::Url::parse(endpoint).ok())
                    .is_some_and(|endpoint| endpoint.scheme() == "http");
        let uses_direct_https_transport = live_nodes.is_none()
            && config
                .endpoint_url()
                .and_then(|endpoint| url::Url::parse(endpoint).ok())
                .is_some_and(|endpoint| endpoint.scheme() == "https");
        let has_custom_http_client = dynamodb_config.http_client().is_some();

        if uses_direct_https_transport && !has_custom_http_client {
            ensure_native_roots_are_usable().map_err(|message| AlternatorClientBuildError {
                kind: AlternatorClientBuildErrorKind::TlsConfiguration(message),
            })?;
        }

        if !has_custom_http_client {
            // The SDK only selects its modern default connector for v2026 and
            // newer behavior versions. Supply that connector explicitly for
            // older versions instead of re-enabling the legacy Hyper stack.
            let needs_pre_2026_transport = !behavior_version
                .is_at_least(aws_sdk_dynamodb::config::BehaviorVersion::v2026_01_12());
            // A TLS-capable connector eagerly validates native roots even for
            // an HTTP endpoint, so use an HTTP-only connector when no roots
            // are available.
            let needs_rootless_plaintext_transport = uses_plaintext_transport
                && !live_nodes
                    .as_ref()
                    .map(|nodes| nodes.has_usable_native_roots())
                    .unwrap_or_else(native_roots_are_usable);

            if needs_pre_2026_transport || needs_rootless_plaintext_transport {
                builder.set_http_client(Some(sdk_http_client(
                    behavior_version,
                    !needs_rootless_plaintext_transport,
                )));
            }
        }

        if !has_region {
            builder = builder.region(Some(aws_sdk_dynamodb::config::Region::from_static(
                "us-east-1",
            )));
        }

        let routing_interceptor: Option<aws_sdk_dynamodb::config::SharedInterceptor> = match (
            live_nodes.as_ref(),
            config.key_route_affinity().filter(|c| c.is_enabled()),
        ) {
            (None, _) => None,
            (Some(nodes), None) => Some(aws_sdk_dynamodb::config::SharedInterceptor::new(
                RoundRobinQueryPlanInterceptor::new(nodes.clone()),
            )),
            (Some(nodes), Some(cfg)) => {
                // The affinity interceptor needs a PartitionKeyResolver, which needs a client
                // to make DescribeTable calls. Using the main client for that would create a
                // cycle: main client -> affinity interceptor -> resolver -> DescribeTable
                // -> main client. Build a separate discovery client from the same base config
                // but with round-robin routing only..
                let pk_discovery_client = aws_sdk_dynamodb::Client::from_conf(
                    builder
                        .clone()
                        .interceptor(RoundRobinQueryPlanInterceptor::new(nodes.clone()))
                        .build(),
                );
                let resolver =
                    std::sync::Arc::new(keyrouting::resolver::PartitionKeyResolver::new(
                        pk_discovery_client,
                        cfg.pk_info_per_table.clone(),
                    ));
                Some(aws_sdk_dynamodb::config::SharedInterceptor::new(
                    AffinityQueryPlanInterceptor::new(cfg.clone(), nodes.clone(), resolver),
                ))
            }
        };

        if let Some(interceptor) = routing_interceptor {
            builder = builder.interceptor(interceptor);
        }

        let dynamodb_config = builder.build();

        let dynamodb_client = aws_sdk_dynamodb::Client::from_conf(dynamodb_config);

        if let Some(nodes) = live_nodes {
            nodes.ensure_discovery_started();
        }

        Ok(Self {
            dynamodb_client,
            config,
        })
    }

    pub fn from_conf_with_live_nodes(
        config: AlternatorConfig,
        live_nodes: std::sync::Arc<LiveNodes>,
    ) -> Self {
        Self::from_conf(config.to_builder().live_nodes(live_nodes).build())
    }

    pub fn config(&self) -> &AlternatorConfig {
        &self.config
    }
}

// All implementations below this point should only be simple wrappers around dynamodb methods

impl AlternatorClient {
    pub fn batch_execute_statement(&self) -> aws_sdk_dynamodb::operation::batch_execute_statement::builders::BatchExecuteStatementFluentBuilder{
        self.dynamodb_client.batch_execute_statement()
    }

    pub fn batch_get_item(
        &self,
    ) -> aws_sdk_dynamodb::operation::batch_get_item::builders::BatchGetItemFluentBuilder {
        self.dynamodb_client.batch_get_item()
    }

    pub fn batch_write_item(
        &self,
    ) -> aws_sdk_dynamodb::operation::batch_write_item::builders::BatchWriteItemFluentBuilder {
        self.dynamodb_client.batch_write_item()
    }

    pub fn create_backup(
        &self,
    ) -> aws_sdk_dynamodb::operation::create_backup::builders::CreateBackupFluentBuilder {
        self.dynamodb_client.create_backup()
    }

    pub fn create_global_table(
        &self,
    ) -> aws_sdk_dynamodb::operation::create_global_table::builders::CreateGlobalTableFluentBuilder
    {
        self.dynamodb_client.create_global_table()
    }

    pub fn create_table(
        &self,
    ) -> aws_sdk_dynamodb::operation::create_table::builders::CreateTableFluentBuilder {
        self.dynamodb_client.create_table()
    }

    pub fn delete_backup(
        &self,
    ) -> aws_sdk_dynamodb::operation::delete_backup::builders::DeleteBackupFluentBuilder {
        self.dynamodb_client.delete_backup()
    }

    pub fn delete_item(
        &self,
    ) -> aws_sdk_dynamodb::operation::delete_item::builders::DeleteItemFluentBuilder {
        self.dynamodb_client.delete_item()
    }

    pub fn delete_resource_policy(&self) -> aws_sdk_dynamodb::operation::delete_resource_policy::builders::DeleteResourcePolicyFluentBuilder{
        self.dynamodb_client.delete_resource_policy()
    }

    pub fn delete_table(
        &self,
    ) -> aws_sdk_dynamodb::operation::delete_table::builders::DeleteTableFluentBuilder {
        self.dynamodb_client.delete_table()
    }

    pub fn describe_backup(
        &self,
    ) -> aws_sdk_dynamodb::operation::describe_backup::builders::DescribeBackupFluentBuilder {
        self.dynamodb_client.describe_backup()
    }

    pub fn describe_continuous_backups(&self) -> aws_sdk_dynamodb::operation::describe_continuous_backups::builders::DescribeContinuousBackupsFluentBuilder{
        self.dynamodb_client.describe_continuous_backups()
    }

	pub fn describe_contributor_insights(
        &self,
    ) -> aws_sdk_dynamodb::operation::describe_contributor_insights::builders::DescribeContributorInsightsFluentBuilder{
        self.dynamodb_client.describe_contributor_insights()
    }

    pub fn describe_endpoints(
        &self,
    ) -> aws_sdk_dynamodb::operation::describe_endpoints::builders::DescribeEndpointsFluentBuilder
    {
        self.dynamodb_client.describe_endpoints()
    }

    pub fn describe_export(
        &self,
    ) -> aws_sdk_dynamodb::operation::describe_export::builders::DescribeExportFluentBuilder {
        self.dynamodb_client.describe_export()
    }

    pub fn describe_global_table(&self) -> aws_sdk_dynamodb::operation::describe_global_table::builders::DescribeGlobalTableFluentBuilder{
        self.dynamodb_client.describe_global_table()
    }

	pub fn describe_global_table_settings(
        &self,
    ) -> aws_sdk_dynamodb::operation::describe_global_table_settings::builders::DescribeGlobalTableSettingsFluentBuilder{
        self.dynamodb_client.describe_global_table_settings()
    }

    pub fn describe_import(
        &self,
    ) -> aws_sdk_dynamodb::operation::describe_import::builders::DescribeImportFluentBuilder {
        self.dynamodb_client.describe_import()
    }

	pub fn describe_kinesis_streaming_destination(
        &self,
    ) -> aws_sdk_dynamodb::operation::describe_kinesis_streaming_destination::builders::DescribeKinesisStreamingDestinationFluentBuilder{
        self.dynamodb_client
            .describe_kinesis_streaming_destination()
    }

    pub fn describe_limits(
        &self,
    ) -> aws_sdk_dynamodb::operation::describe_limits::builders::DescribeLimitsFluentBuilder {
        self.dynamodb_client.describe_limits()
    }

    pub fn describe_table(
        &self,
    ) -> aws_sdk_dynamodb::operation::describe_table::builders::DescribeTableFluentBuilder {
        self.dynamodb_client.describe_table()
    }

	pub fn describe_table_replica_auto_scaling(
        &self,
    ) -> aws_sdk_dynamodb::operation::describe_table_replica_auto_scaling::builders::DescribeTableReplicaAutoScalingFluentBuilder{
        self.dynamodb_client.describe_table_replica_auto_scaling()
    }

    pub fn describe_time_to_live(
        &self,
    ) -> aws_sdk_dynamodb::operation::describe_time_to_live::builders::DescribeTimeToLiveFluentBuilder
    {
        self.dynamodb_client.describe_time_to_live()
    }

	pub fn disable_kinesis_streaming_destination(
        &self,
    ) -> aws_sdk_dynamodb::operation::disable_kinesis_streaming_destination::builders::DisableKinesisStreamingDestinationFluentBuilder{
        self.dynamodb_client.disable_kinesis_streaming_destination()
    }

	pub fn enable_kinesis_streaming_destination(
        &self,
    ) -> aws_sdk_dynamodb::operation::enable_kinesis_streaming_destination::builders::EnableKinesisStreamingDestinationFluentBuilder{
        self.dynamodb_client.enable_kinesis_streaming_destination()
    }

    pub fn execute_statement(
        &self,
    ) -> aws_sdk_dynamodb::operation::execute_statement::builders::ExecuteStatementFluentBuilder
    {
        self.dynamodb_client.execute_statement()
    }

    pub fn execute_transaction(
        &self,
    ) -> aws_sdk_dynamodb::operation::execute_transaction::builders::ExecuteTransactionFluentBuilder
    {
        self.dynamodb_client.execute_transaction()
    }

    pub fn export_table_to_point_in_time(&self) -> aws_sdk_dynamodb::operation::export_table_to_point_in_time::builders::ExportTableToPointInTimeFluentBuilder{
        self.dynamodb_client.export_table_to_point_in_time()
    }

    pub fn get_item(
        &self,
    ) -> aws_sdk_dynamodb::operation::get_item::builders::GetItemFluentBuilder {
        self.dynamodb_client.get_item()
    }

    pub fn get_resource_policy(
        &self,
    ) -> aws_sdk_dynamodb::operation::get_resource_policy::builders::GetResourcePolicyFluentBuilder
    {
        self.dynamodb_client.get_resource_policy()
    }

    pub fn import_table(
        &self,
    ) -> aws_sdk_dynamodb::operation::import_table::builders::ImportTableFluentBuilder {
        self.dynamodb_client.import_table()
    }

    pub fn list_backups(
        &self,
    ) -> aws_sdk_dynamodb::operation::list_backups::builders::ListBackupsFluentBuilder {
        self.dynamodb_client.list_backups()
    }

    pub fn list_contributor_insights(&self) -> aws_sdk_dynamodb::operation::list_contributor_insights::builders::ListContributorInsightsFluentBuilder{
        self.dynamodb_client.list_contributor_insights()
    }

    pub fn list_exports(
        &self,
    ) -> aws_sdk_dynamodb::operation::list_exports::builders::ListExportsFluentBuilder {
        self.dynamodb_client.list_exports()
    }

    pub fn list_global_tables(
        &self,
    ) -> aws_sdk_dynamodb::operation::list_global_tables::builders::ListGlobalTablesFluentBuilder
    {
        self.dynamodb_client.list_global_tables()
    }

    pub fn list_imports(
        &self,
    ) -> aws_sdk_dynamodb::operation::list_imports::builders::ListImportsFluentBuilder {
        self.dynamodb_client.list_imports()
    }

    pub fn list_tables(
        &self,
    ) -> aws_sdk_dynamodb::operation::list_tables::builders::ListTablesFluentBuilder {
        self.dynamodb_client.list_tables()
    }

    pub fn list_tags_of_resource(
        &self,
    ) -> aws_sdk_dynamodb::operation::list_tags_of_resource::builders::ListTagsOfResourceFluentBuilder
    {
        self.dynamodb_client.list_tags_of_resource()
    }

    pub fn put_item(
        &self,
    ) -> aws_sdk_dynamodb::operation::put_item::builders::PutItemFluentBuilder {
        self.dynamodb_client.put_item()
    }

    pub fn put_resource_policy(
        &self,
    ) -> aws_sdk_dynamodb::operation::put_resource_policy::builders::PutResourcePolicyFluentBuilder
    {
        self.dynamodb_client.put_resource_policy()
    }

    pub fn query(&self) -> aws_sdk_dynamodb::operation::query::builders::QueryFluentBuilder {
        self.dynamodb_client.query()
    }

    pub fn restore_table_from_backup(&self) -> aws_sdk_dynamodb::operation::restore_table_from_backup::builders::RestoreTableFromBackupFluentBuilder{
        self.dynamodb_client.restore_table_from_backup()
    }

	pub fn restore_table_to_point_in_time(
        &self,
    ) -> aws_sdk_dynamodb::operation::restore_table_to_point_in_time::builders::RestoreTableToPointInTimeFluentBuilder{
        self.dynamodb_client.restore_table_to_point_in_time()
    }

    pub fn scan(&self) -> aws_sdk_dynamodb::operation::scan::builders::ScanFluentBuilder {
        self.dynamodb_client.scan()
    }

    pub fn tag_resource(
        &self,
    ) -> aws_sdk_dynamodb::operation::tag_resource::builders::TagResourceFluentBuilder {
        self.dynamodb_client.tag_resource()
    }

    pub fn transact_get_items(
        &self,
    ) -> aws_sdk_dynamodb::operation::transact_get_items::builders::TransactGetItemsFluentBuilder
    {
        self.dynamodb_client.transact_get_items()
    }

    pub fn transact_write_items(
        &self,
    ) -> aws_sdk_dynamodb::operation::transact_write_items::builders::TransactWriteItemsFluentBuilder
    {
        self.dynamodb_client.transact_write_items()
    }

    pub fn untag_resource(
        &self,
    ) -> aws_sdk_dynamodb::operation::untag_resource::builders::UntagResourceFluentBuilder {
        self.dynamodb_client.untag_resource()
    }

    pub fn update_continuous_backups(&self) -> aws_sdk_dynamodb::operation::update_continuous_backups::builders::UpdateContinuousBackupsFluentBuilder{
        self.dynamodb_client.update_continuous_backups()
    }

    pub fn update_contributor_insights(&self) -> aws_sdk_dynamodb::operation::update_contributor_insights::builders::UpdateContributorInsightsFluentBuilder{
        self.dynamodb_client.update_contributor_insights()
    }

    pub fn update_global_table(
        &self,
    ) -> aws_sdk_dynamodb::operation::update_global_table::builders::UpdateGlobalTableFluentBuilder
    {
        self.dynamodb_client.update_global_table()
    }

    pub fn update_global_table_settings(&self) -> aws_sdk_dynamodb::operation::update_global_table_settings::builders::UpdateGlobalTableSettingsFluentBuilder{
        self.dynamodb_client.update_global_table_settings()
    }

    pub fn update_item(
        &self,
    ) -> aws_sdk_dynamodb::operation::update_item::builders::UpdateItemFluentBuilder {
        self.dynamodb_client.update_item()
    }

	pub fn update_kinesis_streaming_destination(
        &self,
    ) -> aws_sdk_dynamodb::operation::update_kinesis_streaming_destination::builders::UpdateKinesisStreamingDestinationFluentBuilder{
        self.dynamodb_client.update_kinesis_streaming_destination()
    }

    pub fn update_table(
        &self,
    ) -> aws_sdk_dynamodb::operation::update_table::builders::UpdateTableFluentBuilder {
        self.dynamodb_client.update_table()
    }

	pub fn update_table_replica_auto_scaling(
        &self,
    ) -> aws_sdk_dynamodb::operation::update_table_replica_auto_scaling::builders::UpdateTableReplicaAutoScalingFluentBuilder{
        self.dynamodb_client.update_table_replica_auto_scaling()
    }

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
    use itertools::Itertools;

    #[test]
    fn test_client_adds_hooks_to_inner_client() {
        let client = AlternatorClient::from_conf(
            AlternatorConfig::builder()
                .behavior_version_latest()
                .endpoint_url("http://127.0.0.1:8000")
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

    #[test]
    fn test_client_stores_his_config_for_reference_only() {
        let client = AlternatorClient::from_conf(
            AlternatorConfig::builder()
                .optimize_headers(true)
                .behavior_version_latest()
                .endpoint_url("http://127.0.0.1:8000")
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

    #[test]
    fn try_from_conf_honors_sdk_behavior_version_default() {
        let client = AlternatorClient::try_from_conf(
            AlternatorConfig::builder()
                .endpoint_url("http://127.0.0.1:8000")
                .seed_hosts(Vec::<String>::new())
                .build(),
        );

        assert!(client.is_ok());
    }

    #[test]
    fn try_from_conf_rejects_missing_or_invalid_routing_configuration() {
        let missing = AlternatorClient::try_from_conf(
            AlternatorConfig::builder()
                .behavior_version_latest()
                .build(),
        )
        .unwrap_err();
        assert!(missing.to_string().contains("no Alternator routing target"));

        let invalid = AlternatorClient::try_from_conf(
            AlternatorConfig::builder()
                .behavior_version_latest()
                .scheme("http")
                .seed_hosts(["127.0.0.1:invalid"])
                .build(),
        )
        .unwrap_err();
        assert!(invalid.to_string().contains("invalid seed host"));

        let invalid_scheme = AlternatorClient::try_from_conf(
            AlternatorConfig::builder()
                .behavior_version_latest()
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
            AlternatorConfig::builder()
                .behavior_version_latest()
                .endpoint_url("not a URL")
                .seed_hosts(Vec::<String>::new())
                .build(),
        )
        .unwrap_err();
        assert!(
            invalid_direct
                .to_string()
                .contains("no Alternator routing target")
        );

        for endpoint in ["ftp://host", "ws://host"] {
            let unsupported_direct = AlternatorClient::try_from_conf(
                AlternatorConfig::builder()
                    .behavior_version_latest()
                    .endpoint_url(endpoint)
                    .seed_hosts(Vec::<String>::new())
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
