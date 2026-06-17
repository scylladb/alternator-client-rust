//! HTTPS test for the `/localnodes` discovery path plus a follow-up API call.

use crate::https_test::https_test_context::*;
use crate::https_test::proxy::forward_on_request;

use alternator_driver::AlternatorClient;
use alternator_driver::AlternatorConfig;
use aws_sdk_dynamodb::config::{BehaviorVersion, Credentials};
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

#[test_context(HttpsTestContext)]
#[tokio::test(flavor = "current_thread")]
#[serial]
async fn test_https_discovery(ctx: &mut HttpsTestContext) {
    let proxy_local = ctx.get_proxy_address();
    let localnodes_gets = Arc::new(AtomicUsize::new(0));
    let api_posts = Arc::new(AtomicUsize::new(0));
    let localnodes_gets_for_proxy = localnodes_gets.clone();
    let api_posts_for_proxy = api_posts.clone();

    ctx.set_on_request(
        move |request: Request<Incoming>, sender: Arc<Mutex<SendRequest<Full<Bytes>>>>| {
            let proxy_local = proxy_local.clone();
            let localnodes_gets = localnodes_gets_for_proxy.clone();
            let api_posts = api_posts_for_proxy.clone();
            async move {
                if request.method() == Method::GET && request.uri().path() == "/localnodes" {
                    // Return the proxy itself as the discovered node so both discovery and
                    // the subsequent API call stay on the HTTPS test path.
                    localnodes_gets.fetch_add(1, Ordering::Relaxed);
                    let body = format!("[\"{}\"]", proxy_local);
                    Response::new(Full::new(Bytes::from(body)))
                } else {
                    if request.method() == Method::POST && request.uri().path() == "/" {
                        api_posts.fetch_add(1, Ordering::Relaxed);
                    }
                    forward_on_request(request, sender).await
                }
            }
        },
    )
    .await;

    let client = AlternatorClient::from_conf(
        AlternatorConfig::builder()
            .endpoint_url(format!("https://{}", ctx.get_proxy_address()))
            .behavior_version(BehaviorVersion::latest())
            .credentials_provider(Credentials::for_tests_with_session_token())
            .build(),
    );

    // Poll for the discovery request instead of sleeping for an arbitrary interval.
    tokio::time::timeout(POLLING_TIMEOUT, async {
        loop {
            if localnodes_gets.load(Ordering::Relaxed) > 0 {
                break;
            }
            tokio::time::sleep(POLLING_INTERVAL).await;
        }
    })
    .await
    .unwrap_or_else(|_| {
        panic!(
            "timed out waiting for /localnodes after {:?}",
            POLLING_TIMEOUT
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
        api_posts.load(Ordering::Relaxed) > 0,
        "expected at least one API POST request through the proxy"
    );
}
