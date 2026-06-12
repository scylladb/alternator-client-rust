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

// =============================================================================
// Response Decompression Tests
//
// These tests verify that the driver can correctly handle compressed HTTP
// responses based on the Content-Encoding header, independent of whether
// the client requested compression via Accept-Encoding.
//
// Server setup (configured in docker-compose.yml):
//   - ScyllaDB 2026.1.0+ with Alternator enabled
//   - --alternator-response-gzip-compression-level 6
//   - --alternator-response-compression-threshold-in-bytes 1
//
// With threshold=1, the server will compress all responses when the client
// sends Accept-Encoding. Both gzip and deflate are supported server-side.
//
// The proxy is used to:
//   - Inject Accept-Encoding into outgoing requests (to trigger server compression)
//   - Assert Content-Encoding on responses from the server
//   - In some cases, compress responses itself (for "unexpected compression" test)
// =============================================================================

/// Proxy handler that injects `Accept-Encoding: gzip` into the request
/// before forwarding, then asserts the response has `Content-Encoding: gzip`.
async fn inject_accept_encoding_gzip(
    request: Request<Incoming>,
    sender: Arc<Mutex<SendRequest<Full<Bytes>>>>,
) -> Response<Full<Bytes>> {
    let (mut parts, body) = collect_request(request).await;

    parts
        .headers
        .insert("accept-encoding", "gzip".parse().unwrap());

    let (resp_parts, body) = collect_received_response(parts, body, sender).await;

    // Assert the server actually compressed the response
    let content_encoding = resp_parts
        .headers
        .get("content-encoding")
        .expect("server must return content-encoding: gzip when accept-encoding: gzip is sent");
    assert_eq!(
        content_encoding, "gzip",
        "expected content-encoding: gzip, got: {:?}",
        content_encoding
    );

    build_response(resp_parts, body)
}

/// Proxy handler that injects `Accept-Encoding: deflate` into the request
/// before forwarding, then asserts the response has `Content-Encoding: deflate`.
async fn inject_accept_encoding_deflate(
    request: Request<Incoming>,
    sender: Arc<Mutex<SendRequest<Full<Bytes>>>>,
) -> Response<Full<Bytes>> {
    let (mut parts, body) = collect_request(request).await;

    parts
        .headers
        .insert("accept-encoding", "deflate".parse().unwrap());

    let (resp_parts, body) = collect_received_response(parts, body, sender).await;

    // Assert the server actually compressed the response
    let content_encoding = resp_parts.headers.get("content-encoding").expect(
        "server must return content-encoding: deflate when accept-encoding: deflate is sent",
    );
    assert_eq!(
        content_encoding, "deflate",
        "expected content-encoding: deflate, got: {:?}",
        content_encoding
    );

    build_response(resp_parts, body)
}

/// Proxy handler that compresses the response with gzip regardless of what
/// the client or server negotiated. This simulates "unexpected compression".
async fn force_gzip_on_response(
    request: Request<Incoming>,
    sender: Arc<Mutex<SendRequest<Full<Bytes>>>>,
) -> Response<Full<Bytes>> {
    use flate2::write::GzEncoder;
    use std::io::Write;

    let (parts, body) = collect_request(request).await;
    let (mut resp_parts, resp_body) = collect_received_response(parts, body, sender).await;

    let mut encoder = GzEncoder::new(Vec::new(), flate2::Compression::default());
    encoder.write_all(&resp_body).unwrap();
    let compressed = Bytes::from(encoder.finish().unwrap());

    resp_parts
        .headers
        .insert("content-encoding", "gzip".parse().unwrap());
    resp_parts.headers.insert(
        "content-length",
        compressed.len().to_string().parse().unwrap(),
    );

    build_response(resp_parts, compressed)
}

#[test_context(HttpTestContext<Config>)]
#[tokio::test]
pub async fn test_response_decompression_gzip(ctx: &mut HttpTestContext<Config>) {
    // The proxy injects Accept-Encoding so the server returns a gzip response.
    // If the driver correctly decompresses the gzip response, this call succeeds.
    let client = AlternatorClient::from_conf(
        AlternatorConfig::builder()
            .endpoint_url(format!("http://{}", ctx.get_proxy_address()))
            .seed_hosts(Vec::<String>::new())
            .behavior_version(aws_sdk_dynamodb::config::BehaviorVersion::latest())
            .credentials_provider(
                aws_sdk_dynamodb::config::Credentials::for_tests_with_session_token(),
            )
            .build(),
    );

    ctx.set_on_request(inject_accept_encoding_gzip).await;

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
pub async fn test_response_decompression_deflate(ctx: &mut HttpTestContext<Config>) {
    // The proxy injects Accept-Encoding: deflate so the server returns a deflate response.
    let client = AlternatorClient::from_conf(
        AlternatorConfig::builder()
            .endpoint_url(format!("http://{}", ctx.get_proxy_address()))
            .seed_hosts(Vec::<String>::new())
            .behavior_version(aws_sdk_dynamodb::config::BehaviorVersion::latest())
            .credentials_provider(
                aws_sdk_dynamodb::config::Credentials::for_tests_with_session_token(),
            )
            .build(),
    );

    ctx.set_on_request(inject_accept_encoding_deflate).await;

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
pub async fn test_response_decompression_uncompressed_still_works(
    ctx: &mut HttpTestContext<Config>,
) {
    // The proxy does NOT inject Accept-Encoding, so the server returns uncompressed.
    // This verifies that decompression logic does not break normal responses.
    let client = AlternatorClient::from_conf(
        AlternatorConfig::builder()
            .endpoint_url(format!("http://{}", ctx.get_proxy_address()))
            .seed_hosts(Vec::<String>::new())
            .behavior_version(aws_sdk_dynamodb::config::BehaviorVersion::latest())
            .credentials_provider(
                aws_sdk_dynamodb::config::Credentials::for_tests_with_session_token(),
            )
            .build(),
    );

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
pub async fn test_response_decompression_unexpected_compression(ctx: &mut HttpTestContext<Config>) {
    // The proxy does NOT inject Accept-Encoding into the request, but it
    // force-compresses the response body with gzip and sets Content-Encoding.
    // This simulates a scenario where the server (or an intermediary) compresses
    // the response even though the client did not advertise support.
    // The driver should still decompress based on Content-Encoding.
    let client = AlternatorClient::from_conf(
        AlternatorConfig::builder()
            .endpoint_url(format!("http://{}", ctx.get_proxy_address()))
            .seed_hosts(Vec::<String>::new())
            .behavior_version(aws_sdk_dynamodb::config::BehaviorVersion::latest())
            .credentials_provider(
                aws_sdk_dynamodb::config::Credentials::for_tests_with_session_token(),
            )
            .build(),
    );

    ctx.set_on_request(force_gzip_on_response).await;

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

// =============================================================================
// Response Compression Negotiation Tests (Accept-Encoding)
//
// These tests verify that the driver sends the correct Accept-Encoding header
// based on the response_compression configuration.
//
// They also verify interaction with optimize_headers (Accept-Encoding must
// survive header filtering).
// =============================================================================

/// Proxy handler that asserts the request contains Accept-Encoding: gzip.
async fn assert_accept_encoding_gzip_on_request(
    request: Request<Incoming>,
    sender: Arc<Mutex<SendRequest<Full<Bytes>>>>,
) -> Response<Full<Bytes>> {
    let (parts, body) = collect_request(request).await;

    let accept_encoding = parts
        .headers
        .get("accept-encoding")
        .expect("accept-encoding header must be present");

    assert!(
        accept_encoding.to_str().unwrap().contains("gzip"),
        "accept-encoding must contain gzip, got: {:?}",
        accept_encoding
    );

    let (parts, body) = collect_received_response(parts, body, sender).await;
    build_response(parts, body)
}

/// Proxy handler that asserts the request does NOT contain Accept-Encoding
/// with a compression algorithm (may contain "identity" or be absent).
async fn assert_no_compression_accept_encoding_on_request(
    request: Request<Incoming>,
    sender: Arc<Mutex<SendRequest<Full<Bytes>>>>,
) -> Response<Full<Bytes>> {
    let (parts, body) = collect_request(request).await;

    if let Some(accept_encoding) = parts.headers.get("accept-encoding") {
        let value = accept_encoding.to_str().unwrap();
        assert!(
            !value.contains("gzip") && !value.contains("deflate"),
            "accept-encoding must not contain gzip or deflate when disabled, got: {:?}",
            value
        );
    }

    let (parts, body) = collect_received_response(parts, body, sender).await;
    build_response(parts, body)
}

#[test_context(HttpTestContext<Config>)]
#[tokio::test]
pub async fn test_response_compression_sends_accept_encoding_gzip(
    ctx: &mut HttpTestContext<Config>,
) {
    let client = AlternatorClient::from_conf(
        AlternatorConfig::builder()
            .endpoint_url(format!("http://{}", ctx.get_proxy_address()))
            .seed_hosts(Vec::<String>::new())
            .behavior_version(aws_sdk_dynamodb::config::BehaviorVersion::latest())
            .credentials_provider(
                aws_sdk_dynamodb::config::Credentials::for_tests_with_session_token(),
            )
            .response_compression(ResponseCompression::enabled(
                ResponseCompressionAlgorithm::Gzip,
            ))
            .build(),
    );

    ctx.set_on_request(assert_accept_encoding_gzip_on_request)
        .await;

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
pub async fn test_response_compression_disabled_does_not_send_accept_encoding(
    ctx: &mut HttpTestContext<Config>,
) {
    let client = AlternatorClient::from_conf(
        AlternatorConfig::builder()
            .endpoint_url(format!("http://{}", ctx.get_proxy_address()))
            .seed_hosts(Vec::<String>::new())
            .behavior_version(aws_sdk_dynamodb::config::BehaviorVersion::latest())
            .credentials_provider(
                aws_sdk_dynamodb::config::Credentials::for_tests_with_session_token(),
            )
            .response_compression(ResponseCompression::disabled())
            .build(),
    );

    ctx.set_on_request(assert_no_compression_accept_encoding_on_request)
        .await;

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
pub async fn test_response_compression_enabled_by_per_request_customization(
    ctx: &mut HttpTestContext<Config>,
) {
    let client = AlternatorClient::from_conf(
        AlternatorConfig::builder()
            .endpoint_url(format!("http://{}", ctx.get_proxy_address()))
            .seed_hosts(Vec::<String>::new())
            .behavior_version(aws_sdk_dynamodb::config::BehaviorVersion::latest())
            .credentials_provider(
                aws_sdk_dynamodb::config::Credentials::for_tests_with_session_token(),
            )
            .response_compression(ResponseCompression::disabled())
            .build(),
    );

    ctx.set_on_request(assert_accept_encoding_gzip_on_request)
        .await;

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
        .alternator_config_override(AlternatorConfig::builder().response_compression(
            ResponseCompression::enabled(ResponseCompressionAlgorithm::Gzip),
        ))
        .send()
        .await
        .unwrap();
}

#[test_context(HttpTestContext<Config>)]
#[tokio::test]
pub async fn test_response_compression_disabled_by_per_request_customization(
    ctx: &mut HttpTestContext<Config>,
) {
    let client = AlternatorClient::from_conf(
        AlternatorConfig::builder()
            .endpoint_url(format!("http://{}", ctx.get_proxy_address()))
            .seed_hosts(Vec::<String>::new())
            .behavior_version(aws_sdk_dynamodb::config::BehaviorVersion::latest())
            .credentials_provider(
                aws_sdk_dynamodb::config::Credentials::for_tests_with_session_token(),
            )
            .response_compression(ResponseCompression::enabled(
                ResponseCompressionAlgorithm::Gzip,
            ))
            .build(),
    );

    ctx.set_on_request(assert_no_compression_accept_encoding_on_request)
        .await;

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
            AlternatorConfig::builder().response_compression(ResponseCompression::disabled()),
        )
        .send()
        .await
        .unwrap();
}

#[test_context(HttpTestContext<Config>)]
#[tokio::test]
pub async fn test_response_compression_with_optimize_headers(ctx: &mut HttpTestContext<Config>) {
    // Accept-Encoding must survive header filtering when optimize_headers is enabled.
    let client = AlternatorClient::from_conf(
        AlternatorConfig::builder()
            .endpoint_url(format!("http://{}", ctx.get_proxy_address()))
            .seed_hosts(Vec::<String>::new())
            .behavior_version(aws_sdk_dynamodb::config::BehaviorVersion::latest())
            .credentials_provider(
                aws_sdk_dynamodb::config::Credentials::for_tests_with_session_token(),
            )
            .optimize_headers(true)
            .response_compression(ResponseCompression::enabled(
                ResponseCompressionAlgorithm::Gzip,
            ))
            .build(),
    );

    ctx.set_on_request(assert_accept_encoding_gzip_on_request)
        .await;

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
