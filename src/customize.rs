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

use aws_sdk_dynamodb::client::customize::CustomizableOperation;

/// Extension trait for DynamoDB's [CustomizableOperation].
///
/// Build the client with a full [`AlternatorConfig`], then use
/// [`AlternatorConfig::operation_builder()`] to override Alternator compression
/// settings for a single operation:
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
///     .endpoint_url("http://127.0.0.1:8000")
///     .build();
///
/// let client = AlternatorClient::from_conf(config);
///
/// client
///     .create_table()
///     // ...
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
/// Only request and response compression are supported here. Use the AWS SDK's
/// `config_override(...)` separately for supported SDK-level per-operation
/// overrides.
///
/// A full client config builder is rejected:
///
/// ```compile_fail
/// use alternator_driver::{
///     AlternatorClient,
///     AlternatorConfig,
///     AlternatorCustomizableOperation,
/// };
///
/// let client = AlternatorClient::from_conf(
///     AlternatorConfig::builder()
///         .behavior_version_latest()
///         .endpoint_url("http://127.0.0.1:8000")
///         .build(),
/// );
///
/// let _ = client
///     .list_tables()
///     .customize()
///     .alternator_config_override(AlternatorConfig::builder());
/// ```
pub trait AlternatorCustomizableOperation<T, E, B> {
    fn alternator_config_override(
        self,
        config_override: impl Into<AlternatorOperationBuilder>,
    ) -> Self;
}

impl<T, E, B> AlternatorCustomizableOperation<T, E, B> for CustomizableOperation<T, E, B> {
    fn alternator_config_override(
        self,
        config_override: impl Into<AlternatorOperationBuilder>,
    ) -> Self {
        let config_override: AlternatorOperationBuilder = config_override.into();
        let mut this = self;

        if let Some(request_compression) = config_override.request_compression {
            this = this.interceptor(AlternatorOverrideInterceptor::for_request_compression(
                request_compression,
            ));
        }

        if let Some(response_compression) = config_override.response_compression {
            this = this.interceptor(AlternatorOverrideInterceptor::for_response_compression(
                response_compression,
            ));
        }

        this
    }
}
