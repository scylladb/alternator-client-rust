//! Body Compression Tests
//! In this module we assert that the driver is able to compress requests.
//! We use a proxy to intercept messages sent between driver and alternator.
//!
//! There are 4 test cases:
//! 1. Gzip request compression
//!    Enable gzip compression in client, assert request is correctly compressed
//!
//! 2. Zlib request compression
//!    Enable zlib compression in client, assert request is correctly compressed
//!
//! 3. Enabled by per-request customization
//!    Disable compression in client, then assert request is not compressed,
//!    then customize a call to enable compression and assert it is correctly compressed,
//!    then perform an uncustomized call to assert the customization doesn't last
//!
//! 4. Disabled by per-request customization
//!    Enable compression in client, then customize a call to disable the compression,
//!    then assert the request was not compressed
//!
use crate::http_content::driver_utils::*;
use crate::http_content::http_test::*;
use crate::http_content::proxy::*;

use http_body_util::Full;
use hyper::body::{Bytes, Incoming};
use hyper::client::conn::http1::SendRequest;
use hyper::{Request, Response};

use flate2::read::{GzDecoder, ZlibDecoder};
use std::io::Read;

use std::sync::Arc;
use test_context::test_context;
use tokio::sync::Mutex;
use uuid::Uuid;

use aws_sdk_dynamodb::types::{
    AttributeDefinition, BillingMode, KeySchemaElement, KeyType, ScalarAttributeType,
};

use alternator_driver::*;

struct Config;
impl HttpTestConfig for Config {
    async fn cleanup(resources: Vec<String>, alternator_address: &str) {
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
}

async fn assert_correct_gzip_on_request(
    request: Request<Incoming>,
    sender: Arc<Mutex<SendRequest<Full<Bytes>>>>,
) -> Response<Full<Bytes>> {
    let (parts, body) = collect_request(request).await;

    // check encoding
    let encoding = parts
        .headers
        .get("content-encoding")
        .expect("content-encoding header");

    assert_eq!(encoding, "gzip");

    // check length
    let length: usize = parts
        .headers
        .get("content-length")
        .expect("content-length header")
        .to_str()
        .expect("valid header")
        .parse()
        .expect("unsigned integer in content-length header");

    assert_eq!(length, body.len());

    // check decompression
    let mut decoded = Vec::new();
    let mut decoder = GzDecoder::new(body.as_ref());
    decoder
        .read_to_end(&mut decoded)
        .expect("can decompress body");

    // forward
    let (parts, body) = collect_received_response(parts, body, sender).await;
    build_response(parts, body)
}

async fn assert_correct_zlib_on_request(
    request: Request<Incoming>,
    sender: Arc<Mutex<SendRequest<Full<Bytes>>>>,
) -> Response<Full<Bytes>> {
    let (parts, body) = collect_request(request).await;

    // check encoding
    let encoding = parts
        .headers
        .get("content-encoding")
        .expect("content-encoding header");

    assert_eq!(encoding, "deflate");

    // check length
    let length: usize = parts
        .headers
        .get("content-length")
        .expect("content-length header")
        .to_str()
        .expect("valid header")
        .parse()
        .expect("unsigned integer in content-length header");

    assert_eq!(length, body.len());

    // check decompression
    let mut decoded = Vec::new();
    let mut decoder = ZlibDecoder::new(body.as_ref());
    decoder
        .read_to_end(&mut decoded)
        .expect("can decompress body");

    // forward
    let (parts, body) = collect_received_response(parts, body, sender).await;
    build_response(parts, body)
}

#[test_context(HttpTestContext<Config>)]
#[tokio::test]
pub async fn test_request_compression_gzip(ctx: &mut HttpTestContext<Config>) {
    // construct client with gzip request compression enabled
    let client = AlternatorClient::from_conf(
        AlternatorConfig::builder()
            .endpoint_url(format!("http://{}", ctx.get_proxy_address()))
            .seed_hosts(Vec::<String>::new())
            .behavior_version(aws_sdk_dynamodb::config::BehaviorVersion::latest())
            .credentials_provider(
                aws_sdk_dynamodb::config::Credentials::for_tests_with_session_token(),
            )
            .request_compression(RequestCompression::enabled(
                CompressionAlgorithm::Gzip,
                CompressionLevel::default(),
                0,
            ))
            .build(),
    );

    // set the proxy to assert that driver has sent a correctly compressed request
    ctx.set_on_request(assert_correct_gzip_on_request).await;

    // perform a call, alongside the proxy, register the table for later cleanup
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
}

#[test_context(HttpTestContext<Config>)]
#[tokio::test]
pub async fn test_request_compression_zlib(ctx: &mut HttpTestContext<Config>) {
    // construct client with zlib request compression enabled
    let client = AlternatorClient::from_conf(
        AlternatorConfig::builder()
            .endpoint_url(format!("http://{}", ctx.get_proxy_address()))
            .seed_hosts(Vec::<String>::new())
            .behavior_version(aws_sdk_dynamodb::config::BehaviorVersion::latest())
            .credentials_provider(
                aws_sdk_dynamodb::config::Credentials::for_tests_with_session_token(),
            )
            .request_compression(RequestCompression::enabled(
                CompressionAlgorithm::Zlib,
                CompressionLevel::default(),
                0,
            ))
            .build(),
    );

    // set the proxy to assert that driver has sent a correctly compressed request
    ctx.set_on_request(assert_correct_zlib_on_request).await;

    // perform a call, alongside the proxy, register the table for later cleanup
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
}

async fn assert_not_compressed_on_request(
    request: Request<Incoming>,
    sender: Arc<Mutex<SendRequest<Full<Bytes>>>>,
) -> Response<Full<Bytes>> {
    let (parts, body) = collect_request(request).await;

    // check encoding
    let encoding = parts.headers.get("content-encoding");

    assert!(encoding.is_none());

    // check gzip decompression
    let mut decoded = Vec::new();
    let mut decoder = GzDecoder::new(body.as_ref());
    let cant_decompress = decoder.read_to_end(&mut decoded).is_err();

    assert!(cant_decompress);

    // check zlib decompression
    let mut decoder = ZlibDecoder::new(body.as_ref());
    let cant_decompress = decoder.read_to_end(&mut decoded).is_err();

    assert!(cant_decompress);

    // forward
    let (parts, body) = collect_received_response(parts, body, sender).await;
    build_response(parts, body)
}

#[test_context(HttpTestContext<Config>)]
#[tokio::test]
pub async fn test_enabled_by_per_request_customization(ctx: &mut HttpTestContext<Config>) {
    // construct client with request compression disabled
    let client = AlternatorClient::from_conf(
        AlternatorConfig::builder()
            .endpoint_url(format!("http://{}", ctx.get_proxy_address()))
            .seed_hosts(Vec::<String>::new())
            .behavior_version(aws_sdk_dynamodb::config::BehaviorVersion::latest())
            .credentials_provider(
                aws_sdk_dynamodb::config::Credentials::for_tests_with_session_token(),
            )
            .request_compression(RequestCompression::disabled())
            .build(),
    );

    // set the proxy to assert that driver has sent an uncompressed request
    ctx.set_on_request(assert_not_compressed_on_request).await;

    // perform a call, alongside the proxy, register the table for later cleanup
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

    // set the proxy to assert that driver has sent a correctly compressed request
    ctx.set_on_request(assert_correct_gzip_on_request).await;

    // perform a call, alongside the proxy, register the table for later cleanup
    // enable compression by customizing the call
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
        .alternator_config_override(AlternatorConfig::builder().request_compression(
            RequestCompression::enabled(CompressionAlgorithm::Gzip, CompressionLevel::default(), 0),
        ))
        .send()
        .await
        .unwrap();

    // once again, set the proxy to assert that driver has sent an uncompressed request
    // we want to be sure that the customization doesn't last
    ctx.set_on_request(assert_not_compressed_on_request).await;

    // perform a call, alongside the proxy, register the table for later cleanup
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
}

#[test_context(HttpTestContext<Config>)]
#[tokio::test]
pub async fn test_disabled_by_per_request_customization(ctx: &mut HttpTestContext<Config>) {
    // construct client with request compression enabled
    let client = AlternatorClient::from_conf(
        AlternatorConfig::builder()
            .endpoint_url(format!("http://{}", ctx.get_proxy_address()))
            .seed_hosts(Vec::<String>::new())
            .behavior_version(aws_sdk_dynamodb::config::BehaviorVersion::latest())
            .credentials_provider(
                aws_sdk_dynamodb::config::Credentials::for_tests_with_session_token(),
            )
            .request_compression(RequestCompression::enabled(
                CompressionAlgorithm::Gzip,
                CompressionLevel::default(),
                0,
            ))
            .build(),
    );

    // set the proxy to assert that driver has sent an uncompressed request
    ctx.set_on_request(assert_not_compressed_on_request).await;

    // perform a call, alongside the proxy, register the table for later cleanup
    // disable compression by customizing the call
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
            AlternatorConfig::builder().request_compression(RequestCompression::disabled()),
        )
        .send()
        .await
        .unwrap();
}
