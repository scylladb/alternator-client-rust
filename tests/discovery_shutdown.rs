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

//! Deterministic discovery shutdown tests using a programmable SDK connector.

use alternator_driver::{AlternatorClient, AlternatorConfig, KeyRouteAffinityType};
use aws_smithy_runtime_api::client::http::{
    HttpClient, HttpConnector, HttpConnectorFuture, HttpConnectorSettings, SharedHttpConnector,
};
use aws_smithy_runtime_api::client::orchestrator::{HttpRequest, HttpResponse};
use aws_smithy_runtime_api::client::runtime_components::RuntimeComponents;
use aws_smithy_types::body::SdkBody;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::Semaphore;

#[derive(Clone, Debug)]
struct DiscoveryHttp {
    hold_topology: bool,
    hold_partition_key: bool,
    topology: &'static str,
    hold_next_user_request: Arc<AtomicBool>,
    release_user_request: Arc<Semaphore>,
    user_endpoint: Arc<Mutex<Option<String>>>,
    localnodes_requests: Arc<AtomicUsize>,
    describes: Arc<AtomicUsize>,
    entered: Arc<Semaphore>,
    dropped: Arc<Semaphore>,
}

impl DiscoveryHttp {
    fn new(hold_topology: bool, hold_partition_key: bool) -> Self {
        Self {
            hold_topology,
            hold_partition_key,
            topology: r#"["127.0.0.1"]"#,
            hold_next_user_request: Arc::default(),
            release_user_request: Arc::new(Semaphore::new(0)),
            user_endpoint: Arc::default(),
            localnodes_requests: Arc::default(),
            describes: Arc::default(),
            entered: Arc::new(Semaphore::new(0)),
            dropped: Arc::new(Semaphore::new(0)),
        }
    }

    fn config(&self) -> AlternatorConfig {
        AlternatorConfig::builder()
            .seed_hosts(["127.0.0.1"])
            .port(8000)
            .http_client(self.clone())
            .key_route_affinity(KeyRouteAffinityType::AnyWrite)
            .active_interval(Duration::from_secs(3600))
            .idle_interval(Duration::from_secs(3600))
            .build()
    }

    fn client(&self) -> AlternatorClient {
        AlternatorClient::from_conf(self.config())
    }

    fn last_user_endpoint(&self) -> String {
        self.user_endpoint.lock().unwrap().as_ref().unwrap().clone()
    }

    async fn wait_entered(&self) {
        tokio::time::timeout(Duration::from_secs(1), self.entered.acquire())
            .await
            .expect("held request must enter transport")
            .unwrap()
            .forget();
    }

    async fn wait_dropped(&self) {
        tokio::time::timeout(Duration::from_secs(1), self.dropped.acquire())
            .await
            .expect("shutdown must drop in-flight transport")
            .unwrap()
            .forget();
    }
}

impl HttpClient for DiscoveryHttp {
    fn http_connector(
        &self,
        _: &HttpConnectorSettings,
        _: &RuntimeComponents,
    ) -> SharedHttpConnector {
        SharedHttpConnector::new(self.clone())
    }
}

struct OnDrop(Arc<Semaphore>);

impl Drop for OnDrop {
    fn drop(&mut self) {
        self.0.add_permits(1);
    }
}

impl HttpConnector for DiscoveryHttp {
    fn call(&self, request: HttpRequest) -> HttpConnectorFuture {
        let target = request.headers().get("x-amz-target").unwrap_or_default();
        let (hold, response) = if request.method() == "GET" {
            assert!(request.uri().contains("/localnodes"));
            self.localnodes_requests.fetch_add(1, Ordering::SeqCst);
            (self.hold_topology, self.topology)
        } else if target.ends_with(".DescribeTable") {
            self.describes.fetch_add(1, Ordering::SeqCst);
            (
                self.hold_partition_key,
                r#"{"Table":{"KeySchema":[{"AttributeName":"pk","KeyType":"HASH"}]}}"#,
            )
        } else {
            *self.user_endpoint.lock().unwrap() = Some(request.uri().to_string());
            (false, "{}")
        };
        let hold_user = target.ends_with(".PutItem")
            && self.hold_next_user_request.swap(false, Ordering::SeqCst);
        let release_user = self.release_user_request.clone();
        let entered = self.entered.clone();
        let dropped = self.dropped.clone();
        HttpConnectorFuture::new(async move {
            if hold {
                let _on_drop = OnDrop(dropped);
                entered.add_permits(1);
                std::future::pending::<()>().await;
            }
            if hold_user {
                entered.add_permits(1);
                release_user.acquire().await.unwrap().forget();
            }
            Ok(HttpResponse::try_from(
                http::Response::builder()
                    .status(200)
                    .header("content-type", "application/x-amz-json-1.0")
                    .body(SdkBody::from(response))
                    .unwrap(),
            )
            .unwrap())
        })
    }
}

async fn put_item(client: &AlternatorClient, table: &str) {
    client
        .put_item()
        .table_name(table)
        .item(
            "pk",
            aws_sdk_dynamodb::types::AttributeValue::S("key".into()),
        )
        .send()
        .await
        .unwrap();
}

#[tokio::test(start_paused = true)]
async fn shutdown_from_clones_joins_topology_io_and_isolates_other_clients() {
    tokio::time::timeout(Duration::from_secs(5), async {
        let http = DiscoveryHttp::new(true, false);
        let client = http.client();
        let clone = client.clone();
        http.wait_entered().await;
        let independent_http = DiscoveryHttp::new(true, false);
        let independent = independent_http.client();
        independent_http.wait_entered().await;

        tokio::time::timeout(Duration::from_secs(1), async {
            tokio::join!(client.shutdown(), clone.shutdown());
        })
        .await
        .expect("shutdown must interrupt topology I/O");
        http.wait_dropped().await;
        let localnodes_requests = http.localnodes_requests.load(Ordering::SeqCst);
        put_item(&clone, "after_shutdown").await;
        tokio::time::timeout(Duration::from_secs(1), clone.shutdown())
            .await
            .expect("repeated shutdown must complete");
        assert_eq!(
            http.localnodes_requests.load(Ordering::SeqCst),
            localnodes_requests
        );
        assert_eq!(http.describes.load(Ordering::SeqCst), 0);

        assert!(
            tokio::time::timeout(Duration::from_secs(1), independent_http.dropped.acquire())
                .await
                .is_err(),
            "another client's shutdown must preserve existing discovery I/O"
        );
        put_item(&independent, "other_client").await;
        assert_eq!(tokio::spawn(async { 42 }).await.unwrap(), 42);
        tokio::time::timeout(Duration::from_secs(1), independent.shutdown())
            .await
            .expect("independent client shutdown must interrupt its own topology I/O");
        independent_http.wait_dropped().await;
    })
    .await
    .expect("client isolation checks must complete");
}

#[tokio::test]
async fn shutdown_joins_partition_key_io_and_blocks_new_discovery() {
    tokio::time::timeout(Duration::from_secs(3), async {
        let http = DiscoveryHttp::new(false, true);
        let client = http.client();
        put_item(&client, "first_table").await;
        http.wait_entered().await;
        tokio::time::timeout(Duration::from_secs(1), client.shutdown())
            .await
            .expect("shutdown must interrupt DescribeTable I/O");
        http.wait_dropped().await;
        assert_eq!(http.describes.load(Ordering::SeqCst), 1);
        put_item(&client, "second_table").await;
        assert_eq!(http.describes.load(Ordering::SeqCst), 1);
    })
    .await
    .expect("partition-key shutdown checks and user requests must complete");
}

#[tokio::test]
async fn shutdown_preserves_pending_user_io_and_warmed_routing() {
    tokio::time::timeout(Duration::from_secs(3), async {
        let mut http = DiscoveryHttp::new(false, false);
        // Distinct from the seed, with two nodes to distinguish affinity from
        // round-robin routing using only observed request destinations.
        http.topology = r#"["127.0.0.2","127.0.0.3"]"#;
        let client = http.client();
        let mut endpoint = String::new();
        let mut consecutive = 0;
        loop {
            put_item(&client, "users").await;
            let current = http.last_user_endpoint();
            consecutive = if current == endpoint {
                consecutive + 1
            } else {
                1
            };
            endpoint = current;
            if consecutive >= 4 && !endpoint.contains("127.0.0.1:") {
                break;
            }
            tokio::task::yield_now().await;
        }
        assert_eq!(http.describes.load(Ordering::SeqCst), 1);

        http.hold_next_user_request.store(true, Ordering::SeqCst);
        let pending = put_item(&client, "users");
        tokio::pin!(pending);
        tokio::select! {
            _ = http.wait_entered() => {}
            _ = &mut pending => panic!("user I/O must remain held until released"),
        }
        client.shutdown().await;
        assert!(
            futures::poll!(&mut pending).is_pending(),
            "shutdown must leave the pending user request waiting for its response"
        );
        http.release_user_request.add_permits(1);
        pending.await;
        assert_eq!(http.last_user_endpoint(), endpoint);
        let topology_requests = http.localnodes_requests.load(Ordering::SeqCst);

        for _ in 0..4 {
            put_item(&client, "users").await;
            assert_eq!(http.last_user_endpoint(), endpoint);
        }

        // A request missing the cached partition-key attribute falls back to
        // round-robin, proving the stable destinations above used that cache.
        let mut fallback_endpoints = Vec::new();
        for _ in 0..2 {
            client
                .put_item()
                .table_name("users")
                .item(
                    "other",
                    aws_sdk_dynamodb::types::AttributeValue::S("key".into()),
                )
                .send()
                .await
                .unwrap();
            fallback_endpoints.push(http.last_user_endpoint());
        }
        assert_ne!(fallback_endpoints[0], fallback_endpoints[1]);
        for endpoint in fallback_endpoints {
            assert!(
                endpoint.contains("127.0.0.2:") || endpoint.contains("127.0.0.3:"),
                "cached discovered topology must remain usable: {endpoint}"
            );
        }
        assert_eq!(http.describes.load(Ordering::SeqCst), 1);
        assert_eq!(
            http.localnodes_requests.load(Ordering::SeqCst),
            topology_requests
        );
    })
    .await
    .expect("pending requests and cached routing must survive discovery shutdown");
}

#[tokio::test(start_paused = true)]
async fn last_client_drop_stops_discovery_but_dropping_one_clone_keeps_it_running() {
    let http = DiscoveryHttp::new(true, false);
    let client = http.client();
    let clone = client.clone();
    http.wait_entered().await;
    drop(client);
    assert!(
        tokio::time::timeout(Duration::from_secs(1), http.dropped.acquire())
            .await
            .is_err(),
        "dropping one clone must keep in-flight discovery running"
    );
    drop(clone);
    http.wait_dropped().await;
}

#[test]
fn shutdown_can_be_awaited_on_a_different_runtime() {
    let http = DiscoveryHttp::new(true, false);
    let owner = tokio::runtime::Runtime::new().unwrap();
    let client = owner.block_on(async {
        let client = http.client();
        http.wait_entered().await;
        client
    });
    let waiter = tokio::runtime::Runtime::new().unwrap();
    waiter.block_on(async {
        tokio::time::timeout(Duration::from_secs(1), client.shutdown())
            .await
            .unwrap();
        http.wait_dropped().await;
    });
}

#[test]
fn shutdown_before_first_runtime_prevents_lazy_discovery() {
    let http = DiscoveryHttp::new(true, true);
    let client = http.client();
    let runtime = tokio::runtime::Runtime::new().unwrap();
    runtime.block_on(async {
        tokio::time::timeout(Duration::from_secs(3), async {
            client.shutdown().await;
            put_item(&client, "users").await;
            client.shutdown().await;
        })
        .await
        .expect("shutdown before the first runtime must prevent lazy discovery");
    });
    assert_eq!(http.localnodes_requests.load(Ordering::SeqCst), 0);
    assert_eq!(http.describes.load(Ordering::SeqCst), 0);
}

#[test]
fn shutdown_does_not_wait_for_an_unrelated_saturated_blocking_pool() {
    struct ReleaseOnDrop(Option<std::sync::mpsc::Sender<()>>);
    impl Drop for ReleaseOnDrop {
        fn drop(&mut self) {
            if let Some(release) = self.0.take() {
                let _ = release.send(());
            }
        }
    }
    let owner = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .max_blocking_threads(1)
        .enable_all()
        .build()
        .unwrap();
    let observer = tokio::runtime::Runtime::new().unwrap();
    let (started, started_rx) = std::sync::mpsc::sync_channel(0);
    let (release, release_rx) = std::sync::mpsc::channel();
    // Declared after the owner so failed assertions release its blocker before
    // runtime destruction. The successful path releases it only AFTER shutdown.
    let release = ReleaseOnDrop(Some(release));
    let blocker = owner.spawn_blocking(move || {
        started.send(()).unwrap();
        let _ = release_rx.recv();
    });
    started_rx.recv_timeout(Duration::from_secs(1)).unwrap();
    let http = DiscoveryHttp::new(true, false);
    let client = owner.block_on(async {
        let client = http.client();
        http.wait_entered().await;
        client
    });
    observer.block_on(async {
        tokio::time::timeout(Duration::from_secs(3), async {
            // Routing a request from another runtime requests an owner liveness
            // acknowledgement while its blocking pool has no available worker.
            put_item(&client, "users").await;
            tokio::time::timeout(Duration::from_secs(1), client.shutdown())
                .await
                .expect("discovery shutdown must finish before releasing unrelated work");
            http.wait_dropped().await;
            assert!(!blocker.is_finished());
        })
        .await
        .expect("discovery shutdown checks must finish while unrelated blocking work is held");
    });
    drop(release);
    observer.block_on(async {
        tokio::time::timeout(Duration::from_secs(1), blocker)
            .await
            .unwrap()
            .unwrap();
    });
}

#[tokio::test(start_paused = true)]
async fn partition_key_discovery_uses_explicit_short_and_long_deadlines() {
    for discovery_timeout in [Duration::from_millis(250), Duration::from_secs(10)] {
        let http = DiscoveryHttp::new(false, true);
        let client = AlternatorClient::from_conf(
            http.config()
                .to_builder()
                .timeout_config(
                    aws_smithy_types::timeout::TimeoutConfig::builder()
                        .operation_timeout(Duration::from_secs(1))
                        .operation_attempt_timeout(Duration::from_secs(1))
                        .build(),
                )
                .discovery_timeout(discovery_timeout)
                .build(),
        );
        put_item(&client, "users").await;
        http.wait_entered().await;
        let before_deadline = if discovery_timeout > Duration::from_secs(5) {
            Duration::from_secs(6)
        } else {
            discovery_timeout / 2
        };
        tokio::time::advance(before_deadline).await;
        assert_eq!(
            http.dropped.available_permits(),
            0,
            "explicit discovery deadline must replace inherited data deadline"
        );
        assert_eq!(http.describes.load(Ordering::SeqCst), 1);
        tokio::time::advance(discovery_timeout - before_deadline).await;
        // Remain runnable so paused time cannot advance through retry backoff.
        for _ in 0..1000 {
            if http.dropped.available_permits() > 0 {
                break;
            }
            tokio::task::yield_now().await;
        }
        assert_eq!(http.dropped.available_permits(), 1);
        assert_eq!(http.describes.load(Ordering::SeqCst), 1);
        tokio::time::timeout(Duration::from_secs(1), client.shutdown())
            .await
            .expect("deadline test cleanup must finish");
    }
}

#[tokio::test(start_paused = true)]
async fn unset_discovery_timeout_keeps_partition_key_sdk_deadline() {
    let http = DiscoveryHttp::new(false, true);
    let client = AlternatorClient::from_conf(
        http.config()
            .to_builder()
            .timeout_config(
                aws_smithy_types::timeout::TimeoutConfig::builder()
                    .operation_timeout(Duration::from_secs(8))
                    .build(),
            )
            .build(),
    );
    assert_eq!(client.config().discovery_timeout(), None);
    put_item(&client, "users").await;
    http.wait_entered().await;
    tokio::time::advance(Duration::from_secs(6)).await;
    assert_eq!(
        http.dropped.available_permits(),
        0,
        "legacy PK timeout must not gain topology's default5s cap"
    );
    tokio::time::advance(Duration::from_secs(2)).await;
    for _ in 0..1000 {
        if http.dropped.available_permits() > 0 {
            break;
        }
        tokio::task::yield_now().await;
    }
    assert_eq!(http.dropped.available_permits(), 1);
    tokio::time::timeout(Duration::from_secs(1), client.shutdown())
        .await
        .expect("deadline test cleanup must finish");
}

#[test]
fn invalid_discovery_timeouts_fail_before_network() {
    let http = DiscoveryHttp::new(true, true);
    for timeout in [Duration::ZERO, Duration::MAX] {
        let error = AlternatorClient::try_from_conf(
            http.config()
                .to_builder()
                .discovery_timeout(timeout)
                .build(),
        )
        .unwrap_err();
        assert!(error.to_string().contains("discovery_timeout"));
    }
    assert_eq!(http.localnodes_requests.load(Ordering::SeqCst), 0);
    assert_eq!(http.describes.load(Ordering::SeqCst), 0);
    assert_eq!(http.entered.available_permits(), 0);
}
