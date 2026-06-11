//! Header whitelist tests.
//!
//! This module verifies that the driver strips headers that Alternator does not
//! use from outgoing requests. A proxy is used to intercept messages exchanged
//! between the driver and Alternator.
//!
//! There are five test cases:
//! 1. Without credentials:
//!    Disable credentials and verify that requests follow this whitelist:
//!    ["host", "x-amz-target", "content-length", "accept-encoding", "content-encoding"]
//! 2. With credentials:
//!    Enable credentials and verify that requests follow this whitelist:
//!    ["host", "x-amz-target", "content-length", "accept-encoding", "content-encoding", "authorization", "x-amz-date"]
//! 3. Whitelist needed:
//!    Enable credentials, disable header stripping, and verify that
//!    unnecessary headers are present, confirming that stripping is useful.
//! 4. Enabled by per-request customization:
//!    Disable header stripping in the client config, override it for one call,
//!    and then make another non-customized call to verify that the override
//!    does not persist.
//! 5. Disabled by per-request customization:
//!    Enable header stripping in the client config, override it for one call,
//!    and then make another non-customized call to verify that the override
//!    does not persist.
//!
//! All tests share the same cleanup function. The first three also share the
//! same set of driver calls, and the last two share another set of driver
//! calls.
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

use aws_sdk_dynamodb::client::Waiters;
use aws_sdk_dynamodb::types::{
    AttributeDefinition, AttributeValue, BillingMode, KeySchemaElement, KeyType,
    ScalarAttributeType,
};

use alternator_driver::*;

async fn cleanup_calls(resources: Vec<String>, alternator_address: &str) {
    let client = aws_sdk_dynamodb::Client::from_conf(
        aws_sdk_dynamodb::Config::builder()
            .endpoint_url(format!("http://{}", alternator_address))
            .region(aws_sdk_dynamodb::config::Region::new("eu-central-1"))
            .behavior_version(aws_sdk_dynamodb::config::BehaviorVersion::latest())
            .credentials_provider(
                aws_sdk_dynamodb::config::Credentials::for_tests_with_session_token(),
            )
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

        // forward
        let (parts, body) = collect_received_response(parts, body, sender).await;
        build_response(parts, body)
    }

    async fn cleanup(resources: Vec<String>, alternator_address: &str) {
        cleanup_calls(resources, alternator_address).await;
    }
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
            .allow_no_auth()
            .build(),
    );

    // perform calls to alternator, use proxy to peek and forward requests
    // proxy ensures all requests to have headers stripped according to the whitelist in WithoutCredentialsConfig
    make_calls(&client, ctx).await;
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
            .credentials_provider(
                aws_sdk_dynamodb::config::Credentials::for_tests_with_session_token(),
            )
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
            .credentials_provider(
                aws_sdk_dynamodb::config::Credentials::for_tests_with_session_token(),
            )
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
pub async fn test_enabled_by_per_request_customization(
    ctx: &mut HttpTestContext<PerRequestCustomizationConfig>,
) {
    // construct client with header stripping disabled
    let client = AlternatorClient::from_conf(
        AlternatorConfig::builder()
            .endpoint_url(format!("http://{}", ctx.get_proxy_address()))
            .seed_hosts(Vec::<String>::new())
            .behavior_version(aws_sdk_dynamodb::config::BehaviorVersion::latest())
            .credentials_provider(
                aws_sdk_dynamodb::config::Credentials::for_tests_with_session_token(),
            )
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
            .credentials_provider(
                aws_sdk_dynamodb::config::Credentials::for_tests_with_session_token(),
            )
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
