//! Basic HTTPS smoke test for regular Alternator API traffic.

use crate::https_test::https_test_context::*;

use alternator_driver::AlternatorClient;
use alternator_driver::AlternatorConfig;
use aws_sdk_dynamodb::config::{BehaviorVersion, Credentials};
use serial_test::serial;
use test_context::test_context;

#[test_context(HttpsTestContext)]
#[tokio::test(flavor = "current_thread")]
#[serial]
async fn test_https_main_api(ctx: &mut HttpsTestContext) {
    // Discovery is disabled here so the request path only exercises the main API.
    let client = AlternatorClient::from_conf(
        AlternatorConfig::builder()
            .endpoint_url(format!("https://{}", ctx.get_proxy_address()))
            .seed_hosts(Vec::<String>::new())
            .behavior_version(BehaviorVersion::latest())
            .credentials_provider(Credentials::for_tests_with_session_token())
            .build(),
    );

    let result = client.list_tables().send().await;
    assert!(result.is_ok(), "ListTables failed: {:?}", result.err());
}
