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

//! HTTPS test for system-table discovery plus a follow-up API call.

use crate::https_test::https_test_context::*;
use crate::https_test::proxy::{
    build_response, collect_received_response, collect_request, forward_on_request,
};

use alternator_driver::AlternatorClient;
use alternator_driver::AlternatorConfig;
use aws_sdk_dynamodb::config::Credentials;
use aws_smithy_http_client::tls::rustls_provider::CryptoMode;
use aws_smithy_http_client::tls::{Provider, TlsContext, TrustStore};
use http_body_util::Full;
use hyper::body::{Bytes, Incoming};
use hyper::client::conn::http1::SendRequest;
use hyper::{Method, Request, Response};
use serial_test::serial;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;
use test_context::test_context;
use tokio::sync::Mutex;

const POLLING_TIMEOUT: Duration = Duration::from_secs(5);
const POLLING_INTERVAL: Duration = Duration::from_millis(50);
const SCAN_TARGET: &str = "DynamoDB_20120810.Scan";
const SYSTEM_LOCAL_TABLE: &str = ".scylla.alternator.system.local";
const SYSTEM_PEERS_TABLE: &str = ".scylla.alternator.system.peers";

struct RequestCounts {
    system_local_scans: Arc<AtomicUsize>,
    system_peers_scans: Arc<AtomicUsize>,
    api_posts: Arc<AtomicUsize>,
}

fn scan_response(rpc_address: &str) -> Response<Full<Bytes>> {
    let body = serde_json::json!({
        "Items": [{
            "rpc_address": {"S": rpc_address},
            "data_center": {"S": "datacenter1"},
            "rack": {"S": "rack1"}
        }],
        "Count": 1,
        "ScannedCount": 1
    });

    Response::builder()
        .header("content-type", "application/x-amz-json-1.0")
        .body(Full::new(Bytes::from(body.to_string())))
        .unwrap()
}

async fn configure_discovery_proxy(ctx: &HttpsTestContext) -> RequestCounts {
    let proxy_host = ctx.get_proxy_host();
    let system_local_scans = Arc::new(AtomicUsize::new(0));
    let system_peers_scans = Arc::new(AtomicUsize::new(0));
    let api_posts = Arc::new(AtomicUsize::new(0));
    let system_local_scans_for_proxy = system_local_scans.clone();
    let system_peers_scans_for_proxy = system_peers_scans.clone();
    let api_posts_for_proxy = api_posts.clone();

    ctx.set_on_request(
        move |request: Request<Incoming>, sender: Arc<Mutex<SendRequest<Full<Bytes>>>>| {
            let proxy_host = proxy_host.clone();
            let system_local_scans = system_local_scans_for_proxy.clone();
            let system_peers_scans = system_peers_scans_for_proxy.clone();
            let api_posts = api_posts_for_proxy.clone();
            async move {
                let is_scan = request
                    .headers()
                    .get("x-amz-target")
                    .and_then(|value| value.to_str().ok())
                    == Some(SCAN_TARGET);

                if is_scan {
                    let (parts, body) = collect_request(request).await;
                    let table_name = serde_json::from_slice::<serde_json::Value>(&body)
                        .ok()
                        .and_then(|value| {
                            value
                                .get("TableName")
                                .and_then(|name| name.as_str())
                                .map(str::to_owned)
                        });

                    match table_name.as_deref() {
                        Some(SYSTEM_LOCAL_TABLE) => {
                            system_local_scans.fetch_add(1, Ordering::Relaxed);
                            return scan_response(&proxy_host);
                        }
                        Some(SYSTEM_PEERS_TABLE) => {
                            system_peers_scans.fetch_add(1, Ordering::Relaxed);
                            return scan_response(&proxy_host);
                        }
                        _ => {
                            api_posts.fetch_add(1, Ordering::Relaxed);
                            let (parts, body) =
                                collect_received_response(parts, body, sender).await;
                            return build_response(parts, body);
                        }
                    }
                }

                if request.method() == Method::POST && request.uri().path() == "/" {
                    api_posts.fetch_add(1, Ordering::Relaxed);
                }
                forward_on_request(request, sender).await
            }
        },
    )
    .await;

    RequestCounts {
        system_local_scans,
        system_peers_scans,
        api_posts,
    }
}

async fn assert_discovery_and_api_succeed(client: AlternatorClient, counts: RequestCounts) {
    // Poll for a complete discovery round instead of sleeping for an arbitrary interval.
    tokio::time::timeout(POLLING_TIMEOUT, async {
        loop {
            if counts.system_local_scans.load(Ordering::Relaxed) > 0
                && counts.system_peers_scans.load(Ordering::Relaxed) > 0
            {
                break;
            }
            tokio::time::sleep(POLLING_INTERVAL).await;
        }
    })
    .await
    .unwrap_or_else(|_| {
        panic!(
            "timed out waiting for system-table discovery after {:?}; local scans={}, peer scans={}",
            POLLING_TIMEOUT,
            counts.system_local_scans.load(Ordering::Relaxed),
            counts.system_peers_scans.load(Ordering::Relaxed),
        )
    });

    let result = client.list_tables().send().await;
    assert!(
        result.is_ok(),
        "ListTables after discovery failed: {:?}",
        result.err()
    );
    // Confirm a regular Alternator API request also went through the HTTPS proxy.
    assert!(
        counts.api_posts.load(Ordering::Relaxed) > 0,
        "expected at least one API POST request through the proxy"
    );
}

#[test_context(HttpsTestContext)]
#[tokio::test(flavor = "current_thread")]
#[serial]
async fn test_https_discovery(ctx: &mut HttpsTestContext) {
    let counts = configure_discovery_proxy(ctx).await;
    let client = AlternatorClient::from_conf(
        AlternatorConfig::builder()
            .scheme("https")
            .seed_hosts([ctx.get_proxy_host()])
            .port(ctx.get_proxy_port())
            .credentials_provider(Credentials::for_tests_with_session_token())
            .build(),
    );

    assert_discovery_and_api_succeed(client, counts).await;
}

#[test_context(HttpsTestContext)]
#[tokio::test(flavor = "current_thread")]
#[serial]
async fn test_custom_tls_client_applies_to_https_discovery(ctx: &mut HttpsTestContext) {
    let counts = configure_discovery_proxy(ctx).await;
    let tls_context = TlsContext::builder()
        .with_trust_store(TrustStore::empty().with_pem_certificate(ctx.get_ca_pem()))
        .build()
        .expect("custom TLS context should be valid");
    let http_client = aws_smithy_http_client::Builder::new()
        .tls_provider(Provider::Rustls(CryptoMode::AwsLc))
        .tls_context(tls_context)
        .build_https();

    // If discovery ignores the custom HTTP client, this generated CA is unknown
    // to its native trust store and the topology scans cannot succeed.
    ctx.remove_ca_from_native_roots();
    let client = AlternatorClient::from_conf(
        AlternatorConfig::builder()
            .scheme("https")
            .seed_hosts([ctx.get_proxy_host()])
            .port(ctx.get_proxy_port())
            .http_client(http_client)
            .credentials_provider(Credentials::for_tests_with_session_token())
            .build(),
    );

    assert_discovery_and_api_succeed(client, counts).await;
}
