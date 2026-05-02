use crate::*;

/// Alternator driver's client
///
/// A simple wrapper around [aws_sdk_dynamodb::Client], that adds hooks with alternator-specific optimizations.
///
/// By default, enables gzip request compression, strips requests from headers that are not used by alternator,
/// and chooses an arbitrary aws region, as alternator doesn't require one.
///
/// Can be build using [AlternatorConfig] like so:
/// ```ignore
/// let config =
///     AlternatorConfig::builder()
///     // ...
///     .build();
///
/// let client = AlternatorClient::from_conf(config);
/// ```
#[derive(Clone, Debug)]
pub struct AlternatorClient {
    dynamodb_client: aws_sdk_dynamodb::Client,
    config: AlternatorConfig,
}
impl AlternatorClient {
    pub fn from_conf(config: AlternatorConfig) -> Self {
        let dynamodb_config = config.dynamodb_config.clone();
        let extensions = config.alternator_ext.clone();

        let request_compression = extensions.request_compression.unwrap_or_default();
        let enforce_header_whitelist = extensions.enforce_header_whitelist.unwrap_or(true);
        let has_region = dynamodb_config.region().is_some();

        let mut dynamodb_config =
            dynamodb_config
                .to_builder()
                .interceptor(AlternatorInterceptor::new(
                    request_compression,
                    enforce_header_whitelist,
                ));

        let live_nodes = LiveNodes::new(&config);
        if let Some(nodes) = &live_nodes {
            dynamodb_config =
                dynamodb_config.interceptor(RoundRobinQueryPlanInterceptor::new(nodes.clone()));
        }

        if !has_region {
            dynamodb_config.set_region(Some(aws_sdk_dynamodb::config::Region::from_static(
                "us-east-1",
            )));
        }

        let dynamodb_config = dynamodb_config.build();
        let dynamodb_client = aws_sdk_dynamodb::Client::from_conf(dynamodb_config);

        if let Some(nodes) = live_nodes {
            nodes.ensure_discovery_started();
        }

        Self {
            dynamodb_client,
            config,
        }
    }

    pub fn new(sdk_config: &aws_types::sdk_config::SdkConfig) -> Self {
        Self::from_conf(AlternatorConfig::from(sdk_config))
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
                .enforce_header_whitelist(true)
                .behavior_version_latest()
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
}
