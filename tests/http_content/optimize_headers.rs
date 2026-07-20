//! Header whitelist tests.
//!
//! This module verifies that the driver strips headers that Alternator does not
//! use from outgoing requests. A proxy is used to intercept messages exchanged
//! between the driver and Alternator.
//!
//! There are eleven test cases:
//! 1. Without credentials:
//!    Disable credentials and verify that requests follow this whitelist:
//!    ["host", "x-amz-target", "content-length", "accept-encoding", "content-encoding", "user-agent"]
//! 2. Without credentials, with injected auth headers:
//!    Disable credentials, inject auth headers before header stripping, and
//!    verify that requests still follow the no-auth whitelist.
//! 3. With per-request credentials:
//!    Disable global credentials, provide credentials through a single SDK
//!    operation override, prefer SigV4 auth, and verify that SigV4 headers are
//!    preserved.
//! 4. Without per-request credentials:
//!    Disable global credentials, prefer SigV4 auth, and verify that a missing
//!    per-request credentials override fails locally instead of being sent
//!    unsigned.
//! 5. With credentials:
//!    Enable credentials and verify that requests follow this whitelist:
//!    ["host", "x-amz-target", "content-length", "accept-encoding", "content-encoding", "user-agent", "authorization", "x-amz-date"]
//! 6. Whitelist needed:
//!    Enable credentials, disable header stripping, and verify that
//!    unnecessary headers are present, confirming that stripping is useful.
//! 7. Enabled by per-request customization:
//!    Disable header stripping in the client config, override it for one call,
//!    and then make another non-customized call to verify that the override
//!    does not persist.
//! 8. Disabled by per-request customization:
//!    Enable header stripping in the client config, override it for one call,
//!    and then make another non-customized call to verify that the override
//!    does not persist.
//! 9. Custom User-Agent: Replace the default User-Agent and verify the final optimized request.
//! 10. Disabled User-Agent: Disable User-Agent and verify the final optimized request omits it.
//! 11. User-Agent by per-request customization:
//!     Override User-Agent for one call, then verify a plain call uses the
//!     client default.
//!
//! All tests share the same cleanup function. Several tests share helper
//! functions for common driver calls.
//!
use crate::http_content::driver_utils::*;
use crate::http_content::http_test::*;
use crate::http_content::proxy::*;

use http_body_util::Full;
use hyper::body::{Bytes, Incoming};
use hyper::client::conn::http1::SendRequest;
use hyper::{Request, Response};

use std::sync::Arc;
use std::time::Duration;
use test_context::test_context;
use tokio::sync::Mutex;
use uuid::Uuid;

use aws_smithy_runtime_api::box_error::BoxError;
use aws_smithy_runtime_api::client::interceptors::context::BeforeTransmitInterceptorContextMut;
use aws_smithy_runtime_api::client::runtime_components::RuntimeComponents;
use aws_smithy_types::config_bag::ConfigBag;

use aws_sdk_dynamodb::client::Waiters;
use aws_sdk_dynamodb::types::{
    AttributeDefinition, AttributeValue, BillingMode, KeySchemaElement, KeyType,
    ScalarAttributeType,
};

use alternator_driver::*;

fn request_credentials() -> aws_sdk_dynamodb::config::Credentials {
    aws_sdk_dynamodb::config::Credentials::for_tests()
}

#[derive(Debug)]
struct InjectAuthHeadersInterceptor;
impl aws_sdk_dynamodb::config::Intercept for InjectAuthHeadersInterceptor {
    fn name(&self) -> &'static str {
        "InjectAuthHeadersInterceptor"
    }

    fn modify_before_transmit(
        &self,
        context: &mut BeforeTransmitInterceptorContextMut<'_>,
        _: &RuntimeComponents,
        _: &mut ConfigBag,
    ) -> Result<(), BoxError> {
        let headers = context.request_mut().headers_mut();
        headers.insert("authorization", "AWS4-HMAC-SHA256 fake");
        headers.insert("x-amz-date", "20260626T120000Z");
        Ok(())
    }
}

async fn cleanup_calls(resources: Vec<String>, alternator_address: &str) {
    let client = aws_sdk_dynamodb::Client::from_conf(
        aws_sdk_dynamodb::Config::builder()
            .endpoint_url(format!("http://{}", alternator_address))
            .region(aws_sdk_dynamodb::config::Region::new("eu-central-1"))
            .behavior_version(aws_sdk_dynamodb::config::BehaviorVersion::latest())
            .credentials_provider(aws_sdk_dynamodb::config::Credentials::for_tests())
            .build(),
    );

    for resource in resources {
        delete_table_cleanup(&client, &resource).await;
    }
}

async fn make_calls(client: &AlternatorClient, ctx: &mut HttpTestContext<impl HttpTestConfig>) {
    // perform driver calls, register any tables to cleanup later
    let table_name = format!("table_{}", Uuid::new_v4());
    ctx.register_resource(table_name.clone());

    client
        .create_table()
        .table_name(&table_name)
        .attribute_definitions(
            AttributeDefinition::builder()
                .attribute_name("ExampleKey")
                .attribute_type(ScalarAttributeType::S)
                .build()
                .unwrap(),
        )
        .key_schema(
            KeySchemaElement::builder()
                .attribute_name("ExampleKey")
                .key_type(KeyType::Hash)
                .build()
                .unwrap(),
        )
        .billing_mode(BillingMode::PayPerRequest)
        .send()
        .await
        .unwrap();

    client
        .put_item()
        .table_name(&table_name)
        .item(
            "ExampleKey",
            AttributeValue::S("ExampleItemKey".to_string()),
        )
        .item(
            "ExampleAttribute",
            AttributeValue::S("ExampleItem".to_string()),
        )
        .send()
        .await
        .unwrap();

    client
        .update_item()
        .table_name(&table_name)
        .key(
            "ExampleKey",
            AttributeValue::S("ExampleItemKey".to_string()),
        )
        .update_expression("SET #d = :v")
        .expression_attribute_names("#d", "ExampleAttribute")
        .expression_attribute_values(":v", AttributeValue::S("ExampleItemUpdated".to_string()))
        .send()
        .await
        .unwrap();

    client
        .get_item()
        .table_name(&table_name)
        .key(
            "ExampleKey",
            AttributeValue::S("ExampleItemKey".to_string()),
        )
        .send()
        .await
        .unwrap();

    client
        .delete_table()
        .table_name(&table_name)
        .send()
        .await
        .unwrap();

    client
        .wait_until_table_not_exists()
        .table_name(&table_name)
        .wait(Duration::from_secs(1))
        .await
        .unwrap();
}

struct WithoutCredentialsConfig;
impl HttpTestConfig for WithoutCredentialsConfig {
    async fn on_request(
        request: Request<Incoming>,
        sender: Arc<Mutex<SendRequest<Full<Bytes>>>>,
    ) -> Response<Full<Bytes>> {
        let (parts, body) = collect_request(request).await;

        // allow only whitelisted headers
        let whitelist = [
            "host",
            "x-amz-target",
            "content-length",
            "accept-encoding",
            "content-encoding",
            "user-agent",
        ];

        let rogue = parts
            .headers
            .keys()
            .find(|header| !whitelist.contains(&header.as_str()));

        assert!(
            rogue.is_none(),
            "Header {:?} not in whitelist: {:#?}",
            rogue.unwrap(),
            whitelist
        );
        assert_eq!(parts.headers.get("user-agent").unwrap(), DEFAULT_USER_AGENT);
        assert!(!parts.headers.contains_key("authorization"));
        assert!(!parts.headers.contains_key("x-amz-date"));

        // forward
        let (parts, body) = collect_received_response(parts, body, sender).await;
        build_response(parts, body)
    }

    async fn cleanup(resources: Vec<String>, alternator_address: &str) {
        cleanup_calls(resources, alternator_address).await;
    }
}

struct CustomUserAgentConfig;
impl HttpTestConfig for CustomUserAgentConfig {
    async fn on_request(
        request: Request<Incoming>,
        sender: Arc<Mutex<SendRequest<Full<Bytes>>>>,
    ) -> Response<Full<Bytes>> {
        let (parts, body) = collect_request(request).await;

        assert_eq!(
            parts.headers.get("user-agent").unwrap(),
            "orders-service/1.0"
        );

        let (parts, body) = collect_received_response(parts, body, sender).await;
        build_response(parts, body)
    }

    async fn cleanup(resources: Vec<String>, alternator_address: &str) {
        cleanup_calls(resources, alternator_address).await;
    }
}

#[test_context(HttpTestContext<CustomUserAgentConfig>)]
#[tokio::test]
pub async fn test_custom_user_agent(ctx: &mut HttpTestContext<CustomUserAgentConfig>) {
    let client = AlternatorClient::from_conf(
        AlternatorConfig::builder()
            .endpoint_url(format!("http://{}", ctx.get_proxy_address()))
            .seed_hosts(Vec::<String>::new())
            .behavior_version(aws_sdk_dynamodb::config::BehaviorVersion::latest())
            .optimize_headers(true)
            .user_agent("orders-service/1.0")
            .build(),
    );

    make_calls(&client, ctx).await;
}

struct DisabledUserAgentConfig;
impl HttpTestConfig for DisabledUserAgentConfig {
    async fn on_request(
        request: Request<Incoming>,
        sender: Arc<Mutex<SendRequest<Full<Bytes>>>>,
    ) -> Response<Full<Bytes>> {
        let (parts, body) = collect_request(request).await;

        assert!(!parts.headers.contains_key("user-agent"));

        let (parts, body) = collect_received_response(parts, body, sender).await;
        build_response(parts, body)
    }

    async fn cleanup(resources: Vec<String>, alternator_address: &str) {
        cleanup_calls(resources, alternator_address).await;
    }
}

#[test_context(HttpTestContext<DisabledUserAgentConfig>)]
#[tokio::test]
pub async fn test_without_user_agent(ctx: &mut HttpTestContext<DisabledUserAgentConfig>) {
    let client = AlternatorClient::from_conf(
        AlternatorConfig::builder()
            .endpoint_url(format!("http://{}", ctx.get_proxy_address()))
            .seed_hosts(Vec::<String>::new())
            .behavior_version(aws_sdk_dynamodb::config::BehaviorVersion::latest())
            .optimize_headers(true)
            .without_user_agent()
            .build(),
    );

    make_calls(&client, ctx).await;
}

#[test_context(HttpTestContext<WithoutCredentialsConfig>)]
#[tokio::test]
pub async fn test_without_credentials(ctx: &mut HttpTestContext<WithoutCredentialsConfig>) {
    // construct client with credentials disabled
    let client = AlternatorClient::from_conf(
        AlternatorConfig::builder()
            .endpoint_url(format!("http://{}", ctx.get_proxy_address()))
            .seed_hosts(Vec::<String>::new())
            .behavior_version(aws_sdk_dynamodb::config::BehaviorVersion::latest())
            .optimize_headers(true)
            .build(),
    );

    // perform calls to alternator, use proxy to peek and forward requests
    // proxy ensures all requests to have headers stripped according to the whitelist in WithoutCredentialsConfig
    make_calls(&client, ctx).await;
}

#[test_context(HttpTestContext<WithoutCredentialsConfig>)]
#[tokio::test]
pub async fn test_without_credentials_drops_injected_auth_headers(
    ctx: &mut HttpTestContext<WithoutCredentialsConfig>,
) {
    // construct client with credentials disabled
    let client = AlternatorClient::from_conf(
        AlternatorConfig::builder()
            .endpoint_url(format!("http://{}", ctx.get_proxy_address()))
            .seed_hosts(Vec::<String>::new())
            .behavior_version(aws_sdk_dynamodb::config::BehaviorVersion::latest())
            .optimize_headers(true)
            .interceptor(InjectAuthHeadersInterceptor)
            .build(),
    );

    // perform calls to alternator, use proxy to peek and forward requests
    // proxy ensures the injected auth headers are dropped according to the no-auth whitelist
    make_calls(&client, ctx).await;
}

#[test_context(HttpTestContext<WithCredentialsConfig>)]
#[tokio::test]
pub async fn test_per_request_credentials_preserve_signed_headers(
    ctx: &mut HttpTestContext<WithCredentialsConfig>,
) {
    let client = AlternatorClient::from_conf(
        AlternatorConfig::builder()
            .endpoint_url(format!("http://{}", ctx.get_proxy_address()))
            .seed_hosts(Vec::<String>::new())
            .behavior_version(aws_sdk_dynamodb::config::BehaviorVersion::latest())
            .optimize_headers(true)
            .require_auth()
            .build(),
    );

    client
        .list_tables()
        .customize()
        .config_override(
            aws_sdk_dynamodb::Config::builder()
                .credentials_provider(request_credentials())
                .region(aws_sdk_dynamodb::config::Region::new("eu-central-1")),
        )
        .send()
        .await
        .unwrap();
}

#[test_context(HttpTestContext<PerRequestCustomizationConfig>)]
#[tokio::test]
pub async fn test_missing_per_request_credentials_fails_before_no_auth_fallback(
    ctx: &mut HttpTestContext<PerRequestCustomizationConfig>,
) {
    let client = AlternatorClient::from_conf(
        AlternatorConfig::builder()
            .endpoint_url(format!("http://{}", ctx.get_proxy_address()))
            .seed_hosts(Vec::<String>::new())
            .behavior_version(aws_sdk_dynamodb::config::BehaviorVersion::latest())
            .optimize_headers(true)
            .require_auth()
            .build(),
    );

    let result = client.list_tables().send().await;

    assert!(
        result.is_err(),
        "missing per-request credentials should fail before sending an unsigned request"
    );
}

struct WithCredentialsConfig;
impl HttpTestConfig for WithCredentialsConfig {
    async fn on_request(
        request: Request<Incoming>,
        sender: Arc<Mutex<SendRequest<Full<Bytes>>>>,
    ) -> Response<Full<Bytes>> {
        let (parts, body) = collect_request(request).await;

        // allow only whitelisted headers
        let whitelist = [
            "host",
            "x-amz-target",
            "content-length",
            "accept-encoding",
            "content-encoding",
            "authorization",
            "x-amz-date",
            "user-agent",
        ];

        let rogue = parts
            .headers
            .keys()
            .find(|header| !whitelist.contains(&header.as_str()));

        assert!(
            rogue.is_none(),
            "Header {:?} not in whitelist: {:#?}",
            rogue.unwrap(),
            whitelist
        );
        assert!(parts.headers.contains_key("authorization"));
        assert!(parts.headers.contains_key("x-amz-date"));
        assert_eq!(parts.headers.get("user-agent").unwrap(), DEFAULT_USER_AGENT);

        // forward
        let (parts, body) = collect_received_response(parts, body, sender).await;
        build_response(parts, body)
    }

    async fn cleanup(resources: Vec<String>, alternator_address: &str) {
        cleanup_calls(resources, alternator_address).await;
    }
}

#[test_context(HttpTestContext<WithCredentialsConfig>)]
#[tokio::test]
pub async fn test_with_credentials(ctx: &mut HttpTestContext<WithCredentialsConfig>) {
    // construct client with credentials enabled
    let client = AlternatorClient::from_conf(
        AlternatorConfig::builder()
            .endpoint_url(format!("http://{}", ctx.get_proxy_address()))
            .seed_hosts(Vec::<String>::new())
            .behavior_version(aws_sdk_dynamodb::config::BehaviorVersion::latest())
            .optimize_headers(true)
            .credentials_provider(aws_sdk_dynamodb::config::Credentials::for_tests())
            .build(),
    );

    // perform calls to alternator, use proxy to peek and forward requests
    // proxy ensures all requests to have headers stripped according to the whitelist in WithCredentialsConfig
    make_calls(&client, ctx).await;
}

struct WhitelistNeededConfig;
impl HttpTestConfig for WhitelistNeededConfig {
    async fn on_request(
        request: Request<Incoming>,
        sender: Arc<Mutex<SendRequest<Full<Bytes>>>>,
    ) -> Response<Full<Bytes>> {
        let (parts, body) = collect_request(request).await;

        // check whitelist
        let whitelist = [
            "host",
            "x-amz-target",
            "content-length",
            "accept-encoding",
            "content-encoding",
            "authorization",
            "x-amz-date",
            "user-agent",
        ];

        let rogue = parts
            .headers
            .keys()
            .find(|header| !whitelist.contains(&header.as_str()));

        assert!(
            rogue.is_some(),
            "All headers are in whitelist: {:#?}",
            whitelist
        );

        // forward
        let (parts, body) = collect_received_response(parts, body, sender).await;
        build_response(parts, body)
    }

    async fn cleanup(resources: Vec<String>, alternator_address: &str) {
        cleanup_calls(resources, alternator_address).await;
    }
}

#[test_context(HttpTestContext<WhitelistNeededConfig>)]
#[tokio::test]
pub async fn test_whitelist_needed(ctx: &mut HttpTestContext<WhitelistNeededConfig>) {
    // construct client with header stripping disabled
    let client = AlternatorClient::from_conf(
        AlternatorConfig::builder()
            .endpoint_url(format!("http://{}", ctx.get_proxy_address()))
            .seed_hosts(Vec::<String>::new())
            .behavior_version(aws_sdk_dynamodb::config::BehaviorVersion::latest())
            .optimize_headers(false)
            .credentials_provider(aws_sdk_dynamodb::config::Credentials::for_tests())
            .build(),
    );

    // perform calls to alternator, use proxy to peek and forward requests
    // proxy ensures that requests use headers not in whitelist by default (without header stripping enabled)
    make_calls(&client, ctx).await;
}

struct PerRequestCustomizationConfig;
impl HttpTestConfig for PerRequestCustomizationConfig {
    async fn cleanup(resources: Vec<String>, alternator_address: &str) {
        cleanup_calls(resources, alternator_address).await;
    }
}

async fn make_customized_calls(
    client: &AlternatorClient,
    ctx: &mut HttpTestContext<PerRequestCustomizationConfig>,
) {
    let client_strips_headers = client
        .config()
        .optimize_headers()
        .expect("optimize_headers not set while constructing client");

    // in the first call we assert that we can customize an operation to override client's config
    // proxy ensures that a whitelist is used (WithCredentialsConfig) or it isn't (WhitelistNeededConfig)
    // depending on client's value
    if client_strips_headers {
        ctx.set_on_request(WhitelistNeededConfig::on_request).await;
    } else {
        ctx.set_on_request(WithCredentialsConfig::on_request).await;
    }

    let table_name = format!("table_{}", Uuid::new_v4());
    ctx.register_resource(table_name.clone());

    client
        .create_table()
        .table_name(&table_name)
        .attribute_definitions(
            AttributeDefinition::builder()
                .attribute_name("ExampleKey")
                .attribute_type(ScalarAttributeType::S)
                .build()
                .unwrap(),
        )
        .key_schema(
            KeySchemaElement::builder()
                .attribute_name("ExampleKey")
                .key_type(KeyType::Hash)
                .build()
                .unwrap(),
        )
        .billing_mode(BillingMode::PayPerRequest)
        .customize()
        .alternator_config_override(
            AlternatorConfig::builder().optimize_headers(!client_strips_headers),
        )
        .send()
        .await
        .unwrap();

    // in the second call, we assert that previous customization doesn't last
    if client_strips_headers {
        ctx.set_on_request(WithCredentialsConfig::on_request).await;
    } else {
        ctx.set_on_request(WhitelistNeededConfig::on_request).await;
    }

    let table_name = format!("table_{}", Uuid::new_v4());
    ctx.register_resource(table_name.clone());

    client
        .create_table()
        .table_name(&table_name)
        .attribute_definitions(
            AttributeDefinition::builder()
                .attribute_name("ExampleKey")
                .attribute_type(ScalarAttributeType::S)
                .build()
                .unwrap(),
        )
        .key_schema(
            KeySchemaElement::builder()
                .attribute_name("ExampleKey")
                .key_type(KeyType::Hash)
                .build()
                .unwrap(),
        )
        .billing_mode(BillingMode::PayPerRequest)
        .customize()
        .send()
        .await
        .unwrap();
}

#[test_context(HttpTestContext<PerRequestCustomizationConfig>)]
#[tokio::test]
pub async fn test_per_request_user_agent_customization_does_not_persist(
    ctx: &mut HttpTestContext<PerRequestCustomizationConfig>,
) {
    let client = AlternatorClient::from_conf(
        AlternatorConfig::builder()
            .endpoint_url(format!("http://{}", ctx.get_proxy_address()))
            .seed_hosts(Vec::<String>::new())
            .behavior_version(aws_sdk_dynamodb::config::BehaviorVersion::latest())
            .optimize_headers(true)
            .build(),
    );

    ctx.set_on_request(CustomUserAgentConfig::on_request).await;

    client
        .list_tables()
        .customize()
        .alternator_config_override(AlternatorConfig::builder().user_agent("orders-service/1.0"))
        .send()
        .await
        .unwrap();

    ctx.set_on_request(WithoutCredentialsConfig::on_request)
        .await;

    client.list_tables().send().await.unwrap();
}

#[test_context(HttpTestContext<PerRequestCustomizationConfig>)]
#[tokio::test]
pub async fn test_enabled_by_per_request_customization(
    ctx: &mut HttpTestContext<PerRequestCustomizationConfig>,
) {
    // construct client with header stripping disabled
    let client = AlternatorClient::from_conf(
        AlternatorConfig::builder()
            .endpoint_url(format!("http://{}", ctx.get_proxy_address()))
            .seed_hosts(Vec::<String>::new())
            .behavior_version(aws_sdk_dynamodb::config::BehaviorVersion::latest())
            .credentials_provider(aws_sdk_dynamodb::config::Credentials::for_tests())
            .optimize_headers(false)
            .build(),
    );

    // perform 2 calls to alternator, use proxy to peek and forward requests
    //
    // first call overrides config's optimize_headers setting,
    // then proxy checks if it is stripped according to WithCredentialsConfig whitelist
    //
    // second call does not override the config
    // then proxy checks if it has not been stripped
    // (regular call to assert that customization does not last after operation)
    make_customized_calls(&client, ctx).await;
}
#[test_context(HttpTestContext<PerRequestCustomizationConfig>)]
#[tokio::test]
pub async fn test_disabled_by_per_request_customization(
    ctx: &mut HttpTestContext<PerRequestCustomizationConfig>,
) {
    // construct client with header stripping enabled
    let client = AlternatorClient::from_conf(
        AlternatorConfig::builder()
            .endpoint_url(format!("http://{}", ctx.get_proxy_address()))
            .seed_hosts(Vec::<String>::new())
            .behavior_version(aws_sdk_dynamodb::config::BehaviorVersion::latest())
            .credentials_provider(aws_sdk_dynamodb::config::Credentials::for_tests())
            .optimize_headers(true)
            .build(),
    );

    // perform 2 calls to alternator, use proxy to peek and forward requests
    //
    // first call overrides config's optimize_headers setting,
    // then proxy checks if request has not been stripped
    //
    // second call does not override the config
    // then proxy checks if it is stripped according to WithCredentialsConfig whitelist
    // (regular call to assert that customization does not last after operation)
    make_customized_calls(&client, ctx).await;
}
