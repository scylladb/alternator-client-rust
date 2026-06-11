//! Correct Request Line Test
//! This test verifies that the driver generates only requests with the correct
//! request line: method = POST, URI = "/".
//! We use a proxy to intercept messages sent between the driver and Alternator.
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

struct Config;
impl HttpTestConfig for Config {
    async fn on_request(
        request: Request<Incoming>,
        sender: Arc<Mutex<SendRequest<Full<Bytes>>>>,
    ) -> Response<Full<Bytes>> {
        let (parts, body) = collect_request(request).await;

        // check HTTP line correctness: POST /
        assert_eq!(
            parts.method.as_str(),
            "POST",
            "Unexpected HTTP request method"
        );
        assert_eq!(
            parts.uri,
            http::Uri::from_static("/"),
            "Unexpected HTTP request URI"
        );

        // forward
        let (parts, body) = collect_received_response(parts, body, sender).await;
        build_response(parts, body)
    }

    async fn cleanup(resources: Vec<String>, alternator_address: &str) {
        let client = aws_sdk_dynamodb::Client::from_conf(
            aws_sdk_dynamodb::Config::builder()
                .endpoint_url(format!("http://{}", alternator_address))
                .behavior_version(aws_sdk_dynamodb::config::BehaviorVersion::latest())
                .region(aws_sdk_dynamodb::config::Region::new("eu-central-1"))
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

#[test_context(HttpTestContext<Config>)]
#[tokio::test]
pub async fn test(ctx: &mut HttpTestContext<ContextConfig>) {
    // create client
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
