use crate::*;

use aws_sdk_dynamodb::client::customize::CustomizableOperation;

/// Extension trait for DynamoDB's [CustomizableOperation].
///
/// Build the client with a full [`AlternatorConfig`], then use a partial
/// [`AlternatorConfig::builder()`] value to override Alternator settings for a
/// single operation:
/// ```no_run
/// # tokio::runtime::Runtime::new().unwrap().block_on(async {
/// use alternator_driver::{
///     AlternatorClient,
///     AlternatorConfig,
///     AlternatorCustomizableOperation,
///     RequestCompression,
/// };
///
/// let config = AlternatorConfig::builder()
///     .behavior_version_latest()
///     .build();
///
/// let client = AlternatorClient::from_conf(config);
///
/// client
///     .create_table()
///     // ...
///     .customize()
///     .alternator_config_override(
///         // Per-operation overrides take a builder, not a built config.
///         AlternatorConfig::builder()
///             .optimize_headers(false)
///             .request_compression(RequestCompression::disabled())
///             .behavior_version_latest(),
///     )
///     .send()
///     .await
///     .unwrap();
/// # });
/// ```
pub trait AlternatorCustomizableOperation<T, E, B> {
    fn alternator_config_override(self, config_override: impl Into<AlternatorBuilder>) -> Self;
}

impl<T, E, B> AlternatorCustomizableOperation<T, E, B> for CustomizableOperation<T, E, B> {
    fn alternator_config_override(self, config_override: impl Into<AlternatorBuilder>) -> Self {
        let config_override: AlternatorBuilder = config_override.into();
        let mut this = self.config_override(config_override.dynamodb_builder);

        if let Some(request_compression) = config_override.alternator_ext.request_compression {
            this = this.interceptor(AlternatorOverrideInterceptor::for_request_compression(
                request_compression,
            ));
        }

        if let Some(optimize_headers) = config_override.alternator_ext.optimize_headers {
            this = this.interceptor(AlternatorOverrideInterceptor::for_optimize_headers(
                optimize_headers,
            ));
        }

        if let Some(response_compression) = config_override.alternator_ext.response_compression {
            this = this.interceptor(AlternatorOverrideInterceptor::for_response_compression(
                response_compression,
            ));
        }

        this
    }
}
