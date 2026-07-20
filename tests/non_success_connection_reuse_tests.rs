use alternator_driver::{AlternatorClient, AlternatorConfig};
use http_body_util::{BodyExt, Full};
use hyper::body::{Bytes, Incoming};
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper::{Method, Request, Response, StatusCode};
use hyper_util::rt::TokioIo;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;
use tokio::net::TcpListener;
use tokio::sync::Notify;
use tokio::task::{JoinHandle, JoinSet};

const POLL_INTERVAL: Duration = Duration::from_millis(10);
const REQUEST_TIMEOUT: Duration = Duration::from_secs(5);
const DISCOVERY_INTERVAL: Duration = Duration::from_millis(100);

#[derive(Clone)]
struct MockResponse {
    status: StatusCode,
    content_type: &'static str,
    body: &'static str,
}

struct KeepAliveTestServer {
    address: SocketAddr,
    connections: Arc<AtomicUsize>,
    requests: Arc<AtomicUsize>,
    task: JoinHandle<()>,
}

impl KeepAliveTestServer {
    fn connection_count(&self) -> usize {
        self.connections.load(Ordering::SeqCst)
    }

    fn request_count(&self) -> usize {
        self.requests.load(Ordering::SeqCst)
    }

    async fn wait_for_requests(&self, expected: usize) {
        tokio::time::timeout(REQUEST_TIMEOUT, async {
            while self.request_count() < expected {
                tokio::time::sleep(POLL_INTERVAL).await;
            }
        })
        .await
        .unwrap_or_else(|_| panic!("timed out waiting for {expected} requests"));
    }

    async fn finish(self) {
        let mut task = self.task;
        tokio::select! {
            result = &mut task => result.unwrap(),
            _ = tokio::time::sleep(REQUEST_TIMEOUT) => {
                task.abort();
                let _ = task.await;
                panic!("timed out waiting for test server shutdown");
            }
        }
    }
}

async fn start_keep_alive_server(
    expected_method: Method,
    expected_path: &'static str,
    responses: Vec<MockResponse>,
) -> KeepAliveTestServer {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let responses = Arc::new(responses);
    let expected_method = Arc::new(expected_method);
    let connections = Arc::new(AtomicUsize::new(0));
    let requests = Arc::new(AtomicUsize::new(0));
    let done = Arc::new(Notify::new());

    let task = {
        let connections = Arc::clone(&connections);
        let requests = Arc::clone(&requests);
        let responses = Arc::clone(&responses);
        let done = Arc::clone(&done);
        tokio::spawn(async move {
            let mut connection_tasks = JoinSet::new();

            while requests.load(Ordering::SeqCst) < responses.len() {
                let (stream, _) = tokio::select! {
                    result = listener.accept() => result.unwrap(),
                    _ = done.notified() => continue,
                };
                connections.fetch_add(1, Ordering::SeqCst);

                let requests = Arc::clone(&requests);
                let responses = Arc::clone(&responses);
                let expected_method = Arc::clone(&expected_method);
                let done = Arc::clone(&done);
                connection_tasks.spawn(async move {
                    let completion_requests = Arc::clone(&requests);
                    let expected_requests = responses.len();
                    let service = service_fn(move |request: Request<Incoming>| {
                        let requests = Arc::clone(&requests);
                        let responses = Arc::clone(&responses);
                        let expected_method = Arc::clone(&expected_method);

                        async move {
                            assert_eq!(request.method(), expected_method.as_ref());
                            assert_eq!(request.uri().path(), expected_path);

                            let _ = request.into_body().collect().await?;
                            let index = requests.fetch_add(1, Ordering::SeqCst);
                            let response = responses
                                .get(index)
                                .unwrap_or_else(|| panic!("unexpected request {index}"))
                                .clone();

                            let mut builder = Response::builder()
                                .status(response.status)
                                .header("content-type", response.content_type);
                            if index + 1 == responses.len() {
                                builder = builder.header("connection", "close");
                            }

                            Ok::<_, hyper::Error>(
                                builder
                                    .body(Full::new(Bytes::from_static(response.body.as_bytes())))
                                    .unwrap(),
                            )
                        }
                    });

                    http1::Builder::new()
                        .keep_alive(true)
                        .serve_connection(TokioIo::new(stream), service)
                        .await
                        .unwrap();

                    if completion_requests.load(Ordering::SeqCst) >= expected_requests {
                        done.notify_waiters();
                    }
                });
            }

            connection_tasks.abort_all();
            while let Some(result) = connection_tasks.join_next().await {
                if let Err(error) = result {
                    assert!(
                        error.is_cancelled(),
                        "connection task failed before shutdown: {error}"
                    );
                }
            }
        })
    };

    KeepAliveTestServer {
        address,
        connections,
        requests,
        task,
    }
}

#[tokio::test]
async fn alternator_discovery_non_success_responses_keep_connection_reusable() {
    let server = start_keep_alive_server(
        Method::GET,
        "/localnodes",
        vec![
            MockResponse {
                status: StatusCode::INTERNAL_SERVER_ERROR,
                content_type: "application/json",
                body: r#"{"message":"temporary failure"}"#,
            },
            MockResponse {
                status: StatusCode::SERVICE_UNAVAILABLE,
                content_type: "application/json",
                body: r#"{"message":"busy"}"#,
            },
            MockResponse {
                status: StatusCode::OK,
                content_type: "application/json",
                body: r#"["127.0.0.1"]"#,
            },
        ],
    )
    .await;

    let client = AlternatorClient::from_conf(
        AlternatorConfig::builder()
            .behavior_version_latest()
            .scheme("http")
            .port(server.address.port())
            .seed_hosts(vec![server.address.ip().to_string()])
            .active_interval(DISCOVERY_INTERVAL)
            .idle_interval(Duration::from_secs(10))
            .build(),
    );

    server.wait_for_requests(3).await;

    assert_eq!(
        server.connection_count(),
        1,
        "discovery connection was not reused after non-success responses"
    );

    drop(client);
    server.finish().await;
}

#[tokio::test]
async fn dynamodb_non_success_responses_keep_connection_reusable() {
    let server = start_keep_alive_server(
        Method::POST,
        "/",
        vec![
            MockResponse {
                status: StatusCode::BAD_REQUEST,
                content_type: "application/x-amz-json-1.0",
                body: r#"{"__type":"com.amazonaws.dynamodb.v20120810#ValidationException","message":"first failure"}"#,
            },
            MockResponse {
                status: StatusCode::BAD_REQUEST,
                content_type: "application/x-amz-json-1.0",
                body: r#"{"__type":"com.amazonaws.dynamodb.v20120810#ValidationException","message":"second failure"}"#,
            },
            MockResponse {
                status: StatusCode::OK,
                content_type: "application/x-amz-json-1.0",
                body: r#"{"TableNames":[]}"#,
            },
        ],
    )
    .await;

    let http_client = aws_smithy_http_client::Builder::new().build_http();
    let client = AlternatorClient::from_conf(
        AlternatorConfig::builder()
            .behavior_version_latest()
            .region(aws_sdk_dynamodb::config::Region::new("eu-central-1"))
            .credentials_provider(
                aws_sdk_dynamodb::config::Credentials::for_tests_with_session_token(),
            )
            .http_client(http_client)
            .endpoint_url(format!("http://{}", server.address))
            .seed_hosts(Vec::<String>::new())
            .build(),
    );

    assert!(client.list_tables().send().await.is_err());
    assert!(client.list_tables().send().await.is_err());
    client.list_tables().send().await.unwrap();

    assert_eq!(server.request_count(), 3);
    assert_eq!(
        server.connection_count(),
        1,
        "DynamoDB connection was not reused after non-success responses"
    );

    drop(client);
    server.finish().await;
}
