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

//! Basic HTTPS smoke test for regular Alternator API traffic.

use crate::https_test::https_test_context::*;

use alternator_driver::AlternatorClient;
use alternator_driver::AlternatorConfig;
use aws_sdk_dynamodb::config::Credentials;
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
            .credentials_provider(Credentials::for_tests_with_session_token())
            .build(),
    );

    let result = client.list_tables().send().await;
    assert!(result.is_ok(), "ListTables failed: {:?}", result.err());
}
