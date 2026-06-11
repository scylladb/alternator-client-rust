//! # HTTP Content Tests Context
//!
//! All HTTP content tests perform driver calls to Alternator.
//! A proxy server sits in the middle of the traffic, reads forwarded messages,
//! validates them, and can optionally modify them.
//!
//! `HttpTestContext` provides a framework for such tests and integrates with
//! the `test_context` crate.
//!
//! ### Making tests
//! 1. Define a cleanup function for your test and optionally the initial on_request
//!    function to be called inside the proxy whenever a new request goes through.
//!
//!    Do this by writing a custom `MyConfig` struct that implements `HttpTestConfig`.
//!
//! 2. Use HttpTestContext<MyConfig> as context for test_context crate, so that the proxy
//!    is set up and cleanup is performed at the end.
//!
//! 3. Inside the test function, construct a new driver client with
//!    `HttpTestContext::get_proxy_address()` as the endpoint.
//!
//! 4. Whenever you create a resource that requires cleanup,
//!    register it with `HttpTestContext::register_resource()`.
//!
//! 5. You can override the initial `on_request` with
//!    `HttpTestContext::set_on_request()`.
//!
//! **Note:** Since load balancing is enabled automatically, the GET requests
//! from discovery may interfere with the test logic. To work around this, you
//! can disable load balancing by setting an empty list of seed hosts in the
//! client configuration with `.seed_hosts(Vec::<String>::new())`.

use crate::http_content::proxy::*;

use test_context::AsyncTestContext;

use http_body_util::Full;
use hyper::body::{Bytes, Incoming};
use hyper::client::conn::http1::SendRequest;
use hyper::{Request, Response};

use std::marker::PhantomData;
use std::sync::Arc;

use tokio::sync::Mutex;
use tokio::task::JoinHandle;

use futures::FutureExt;
use futures::future::BoxFuture;

const ALTERNATOR_ADDRESS: &str = "localhost:8000";

pub trait HttpTestConfig: Send {
    fn cleanup(resources: Vec<String>, alternator_address: &str)
    -> impl Future<Output = ()> + Send;

    fn on_request(
        request: Request<Incoming>,
        sender: Arc<Mutex<SendRequest<Full<Bytes>>>>,
    ) -> impl Future<Output = Response<Full<Bytes>>> + Send + 'static {
        forward_on_request(request, sender)
    }
}

type OnRequest = Box<
    dyn Fn(
            Request<Incoming>,
            Arc<Mutex<SendRequest<Full<Bytes>>>>,
        ) -> BoxFuture<'static, Response<Full<Bytes>>>
        + Send
        + Sync,
>;

pub struct HttpTestContext<Config: HttpTestConfig> {
    on_request: Arc<Mutex<OnRequest>>,
    proxy_handle: JoinHandle<()>,
    proxy_address: String,
    resources: Vec<String>,
    _pd: PhantomData<Config>,
}
impl<Config: HttpTestConfig> AsyncTestContext for HttpTestContext<Config> {
    async fn setup() -> Self {
        // swappable on_request
        let initial: OnRequest =
            Box::new(|request, sender| Config::on_request(request, sender).boxed());
        let inner = Arc::new(Mutex::new(initial));
        let on_request = {
            let inner = inner.clone();
            move |request, sender| {
                let inner = inner.clone();
                async move { inner.lock().await(request, sender).await }
            }
        };

        // start proxy
        let proxy = Proxy::start(
            "localhost:0".to_string(), // let OS choose port
            ALTERNATOR_ADDRESS.to_string(),
            on_request,
            None,
            None,
        )
        .await;

        // create context
        let address = proxy.address().to_string();
        let handle = tokio::spawn(proxy.run());

        HttpTestContext {
            on_request: inner,
            proxy_handle: handle,
            proxy_address: address,
            resources: vec![],
            _pd: PhantomData,
        }
    }

    async fn teardown(self) {
        Config::cleanup(self.resources, ALTERNATOR_ADDRESS).await;
        self.proxy_handle.abort();
    }
}
impl<Config: HttpTestConfig> HttpTestContext<Config> {
    #![allow(dead_code)]
    pub async fn set_on_request<F, Fut>(&self, new: F)
    where
        F: Fn(Request<Incoming>, Arc<Mutex<SendRequest<Full<Bytes>>>>) -> Fut
            + Send
            + Sync
            + 'static,
        Fut: Future<Output = Response<Full<Bytes>>> + Send + 'static,
    {
        *self.on_request.lock().await =
            Box::new(move |request, sender| new(request, sender).boxed());
    }

    pub fn get_proxy_address(&self) -> String {
        self.proxy_address.clone()
    }

    pub fn register_resource(&mut self, resource_name: String) {
        self.resources.push(resource_name);
    }
}
