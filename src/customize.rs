use crate::*;

use aws_sdk_dynamodb::client::customize::CustomizableOperation;

/// Trait to be implemented by Dynamodb's [CustomizableOperation].
///
/// It allows us to override [AlternatorConfig] at per-operation level, like so:
/// ```ignore
/// client
///     .create_table()
///     // ...
///     .customize()
///     .alternator_config_override(
///         AlternatorConfig::builder()
///             .optimize_headers(false)
///             .request_compression(RequestCompression::disabled())
///             .behavior_version_latest()
///             .build()
///     )
///     .send()
///     .await
///     .unwrap();
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

        this
    }
}
