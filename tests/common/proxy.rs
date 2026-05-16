//! Start the proxy by awaiting `Proxy::start`.
//! It binds to a specified address and connects to the server.
//! Then await `Proxy::run`.
//!
//! The proxy is expected to live no longer than the server.
//! When the server is closed, the future finishes.
//! The proxy can accept many clients during its lifetime, up to 3 at a time.
//!
//! It comes with three hooks:
//! - `on_request`
//! - `on_client_connect`
//! - `on_client_disconnect`
//!
//! `on_request` takes the incoming request and a sender as arguments.
//! It is expected to send the request by itself and return the response.
//! To simplify the process, you can use the following helper functions:
//! `collect_request`, `collect_response`, `build_request`, `build_response`,
//! `send_request`, `collect_received_response`.
//!
//! A basic `on_request` looks like this:
//! ```
//! async |request, sender| {
//!     let (parts, body) = collect_request(request).await;
//!     let (parts, body) = collect_received_response(parts, body, sender).await;
//!     build_response(parts, body)
//! }
//! ```

use http_body_util::{BodyExt, Full};
use hyper::body::{Bytes, Incoming};
use hyper::client::conn::http1 as hyper_client;
use hyper::server::conn::http1 as hyper_server;
use hyper::service::service_fn;
use hyper::{Request, Response};
use hyper_client::SendRequest;
use hyper_util::rt::TokioIo;

use futures::{FutureExt, future::Fuse};

use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Arc;

use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{Mutex, Semaphore, watch};

pub struct Proxy<'a> {
    task: Pin<Box<dyn Future<Output = ()> + Send + 'a>>,
    listen_address: SocketAddr,
}

impl<'a> Proxy<'a> {
    pub async fn start<F, Fut>(
        listen_address: String,
        connect_address: String,
        on_request: F,
        on_client_connect: Option<Box<dyn Fn(SocketAddr) + Send + Sync>>,
        on_client_disconnect: Option<Box<dyn Fn() + Send + Sync>>,
    ) -> Proxy<'a>
    where
        F: Fn(Request<Incoming>, Arc<Mutex<SendRequest<Full<Bytes>>>>) -> Fut
            + Send
            + Sync
            + 'static,
        Fut: Future<Output = Response<Full<Bytes>>> + Send + 'static,
    {
        let on_request = Arc::new(on_request);
        let on_client_connect: Option<Arc<dyn Fn(SocketAddr) + Send + Sync>> =
            on_client_connect.map(Arc::from);
        let on_client_disconnect: Option<Arc<dyn Fn() + Send + Sync>> =
            on_client_disconnect.map(Arc::from);
        let (listener, listen_address) = Proxy::bind_listener(listen_address.clone()).await;
        let (sender, server_connection) = Proxy::connect_server(connect_address.clone()).await;

        let task = async move {
            let (shutdown_tx, mut shutdown_rx) = watch::channel(false);

            tokio::spawn(async move {
                let result = server_connection.await;
                let _ = shutdown_tx.send(true);
                result.expect("Server finished with an error");
            });

            let connection_limit = Arc::new(Semaphore::new(3));

            loop {
                tokio::select! {
                    changed = shutdown_rx.changed() => {
                        if changed.is_ok() && *shutdown_rx.borrow() {
                            break;
                        }
                    }
                    permit = connection_limit.clone().acquire_owned() => {
                        let permit = permit.unwrap();

                        tokio::select! {
                            changed = shutdown_rx.changed() => {
                                if changed.is_ok() && *shutdown_rx.borrow() {
                                    break;
                                }
                            }
                            accepted = listener.accept() => {
                                let (stream, client_address) = accepted.unwrap();
                                let stream = TokioIo::new(stream);

                                if let Some(on_client_connect) = &on_client_connect {
                                    on_client_connect(client_address);
                                }

                                let server_sender = sender.clone();
                                let on_request = on_request.clone();
                                let on_client_disconnect = on_client_disconnect.clone();

                                tokio::spawn(async move {
                                    let service = service_fn(move |request| {
                                        let server_sender = server_sender.clone();
                                        let on_request = on_request.clone();
                                        async move {
                                            let response = on_request(request, server_sender).await;
                                            Ok::<_, hyper::Error>(response)
                                        }
                                    });

                                    let handler = hyper_server::Builder::new();
                                    let connection = handler.serve_connection(stream, service);
                                    let result = connection.await;
                                    drop(permit);
                                    result.expect("Client finished with an error");

                                    if let Some(on_client_disconnect) = on_client_disconnect {
                                        on_client_disconnect();
                                    }
                                });
                            }
                        }
                    }
                }
            }
        };

        Self {
            task: Box::pin(task),
            listen_address,
        }
    }

    pub async fn run(self) {
        self.task.await;
    }

    pub fn address(&self) -> SocketAddr {
        self.listen_address
    }

    async fn bind_listener(address: String) -> (TcpListener, SocketAddr) {
        let listener = TcpListener::bind(address).await.unwrap();
        let listen_address = listener.local_addr().unwrap();
        (listener, listen_address)
    }

    async fn connect_server(
        address: String,
    ) -> (
        Arc<Mutex<SendRequest<Full<Bytes>>>>,
        Pin<Box<Fuse<impl Future<Output = Result<(), hyper::Error>>>>>,
    ) {
        let stream = TcpStream::connect(address).await.unwrap();
        let stream = TokioIo::new(stream);
        let client = hyper_client::Builder::new();
        let (sender, connection) = client.handshake::<_, Full<Bytes>>(stream).await.unwrap();
        let sender = Arc::new(Mutex::new(sender));

        (sender, Box::pin(connection.fuse()))
    }
}

// on_request helpers:

pub async fn collect_request(request: Request<Incoming>) -> (http::request::Parts, Bytes) {
    let (parts, body) = request.into_parts();
    let body = body.collect().await.unwrap().to_bytes();
    (parts, body)
}

pub async fn collect_response(response: Response<Incoming>) -> (http::response::Parts, Bytes) {
    let (parts, body) = response.into_parts();
    let body = body.collect().await.unwrap().to_bytes();
    (parts, body)
}

pub fn build_request(parts: http::request::Parts, body: Bytes) -> Request<Full<Bytes>> {
    let body = Full::new(body);
    Request::from_parts(parts, body)
}

pub fn build_response(parts: http::response::Parts, body: Bytes) -> Response<Full<Bytes>> {
    let body = Full::new(body);
    Response::from_parts(parts, body)
}

pub async fn send_request(
    request: Request<Full<Bytes>>,
    sender: Arc<Mutex<SendRequest<Full<Bytes>>>>,
) -> Response<Incoming> {
    let mut sender = sender.lock().await;
    sender.send_request(request).await.unwrap()
}

pub async fn collect_received_response(
    parts: http::request::Parts,
    body: Bytes,
    sender: Arc<Mutex<SendRequest<Full<Bytes>>>>,
) -> (http::response::Parts, Bytes) {
    let request = build_request(parts, body);
    let response = send_request(request, sender).await;
    collect_response(response).await
}

pub async fn forward_on_request(
    request: Request<Incoming>,
    sender: Arc<Mutex<SendRequest<Full<Bytes>>>>,
) -> Response<Full<Bytes>> {
    let (parts, body) = collect_request(request).await;
    let (parts, body) = collect_received_response(parts, body, sender).await;
    build_response(parts, body)
}
