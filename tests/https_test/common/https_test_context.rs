//! Shared HTTPS test context.
//!
//! Each test gets a TLS-terminating proxy in front of the real Alternator HTTP port.
//! The generated CA is published through `SSL_CERT_FILE` so both the SDK path and
//! the reqwest-based discovery path trust the proxy certificate.

use crate::https_test::proxy::forward_on_request;
use crate::https_test::proxy::*;

use test_context::AsyncTestContext;

use http_body_util::Full;
use hyper::body::{Bytes, Incoming};
use hyper::client::conn::http1::SendRequest;
use hyper::{Request, Response};

use std::path::PathBuf;
use std::sync::Arc;

use tokio::sync::Mutex;
use tokio::task::JoinHandle;

use futures::FutureExt;
use futures::future::BoxFuture;

use rcgen::{BasicConstraints, CertificateParams, IsCa, KeyPair};
use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use tokio_rustls::TlsAcceptor;
use uuid::Uuid;

const ALTERNATOR_ADDRESS: &str = "localhost:8000";

struct Fixture {
    ca_pem: String,
    acceptor: TlsAcceptor,
}

static FIXTURE: std::sync::LazyLock<Fixture> = std::sync::LazyLock::new(|| {
    let ca_key = KeyPair::generate().unwrap();
    let mut ca_params = CertificateParams::new(vec!["Test CA".to_string()]).unwrap();
    ca_params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    ca_params.key_usages = vec![rcgen::KeyUsagePurpose::KeyCertSign];
    let ca_cert = ca_params.self_signed(&ca_key).unwrap();

    let server_key = KeyPair::generate().unwrap();
    let server_params = CertificateParams::new(vec!["localhost".to_string()]).unwrap();
    let server_cert = server_params
        .signed_by(&server_key, &ca_cert, &ca_key)
        .unwrap();

    let certs_chain = vec![CertificateDer::from(server_cert.der().to_vec())];
    let priv_key = PrivateKeyDer::try_from(server_key.serialize_der()).unwrap();
    let server_config = rustls::ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(certs_chain, priv_key)
        .unwrap();

    Fixture {
        ca_pem: ca_cert.pem(),
        acceptor: TlsAcceptor::from(Arc::new(server_config)),
    }
});

fn write_ca_and_set_env(ca_pem: &str) -> PathBuf {
    let path = std::env::temp_dir().join(format!("https_test_ca_{}.pem", Uuid::new_v4()));
    std::fs::write(&path, ca_pem).unwrap();
    // SAFETY: These HTTPS tests are marked `#[serial]` and use Tokio's
    // `current_thread` runtime, so this test process runs only one of these
    // env-mutating test contexts at a time during setup/teardown.
    unsafe { std::env::set_var("SSL_CERT_FILE", &path) };
    path
}

type OnRequest = Box<
    dyn Fn(
            Request<Incoming>,
            Arc<Mutex<SendRequest<Full<Bytes>>>>,
        ) -> BoxFuture<'static, Response<Full<Bytes>>>
        + Send
        + Sync,
>;

pub struct HttpsTestContext {
    on_request: Arc<Mutex<OnRequest>>,
    proxy_handle: JoinHandle<()>,
    proxy_address: String,
    cert_path: PathBuf,
}

impl AsyncTestContext for HttpsTestContext {
    async fn setup() -> Self {
        let cert_path = write_ca_and_set_env(&FIXTURE.ca_pem);

        let initial: OnRequest =
            Box::new(|request, sender| forward_on_request(request, sender).boxed());
        let inner = Arc::new(Mutex::new(initial));
        let on_request = {
            let inner = inner.clone();
            move |request, sender| {
                let inner = inner.clone();
                async move { inner.lock().await(request, sender).await }
            }
        };

        let proxy: Proxy = Proxy::start_tls(
            "localhost:0".to_string(),
            ALTERNATOR_ADDRESS.to_string(),
            on_request,
            None,
            None,
            FIXTURE.acceptor.clone(),
        )
        .await;

        let proxy_address = format!("localhost:{}", proxy.address().port());
        let proxy_handle = tokio::spawn(proxy.run());

        HttpsTestContext {
            on_request: inner,
            proxy_handle,
            proxy_address,
            cert_path,
        }
    }

    async fn teardown(self) {
        self.proxy_handle.abort();
        std::fs::remove_file(&self.cert_path).ok();
        // SAFETY: These HTTPS tests are marked `#[serial]` and use Tokio's
        // `current_thread` runtime, so this test process runs only one of these
        // env-mutating test contexts at a time during setup/teardown.
        unsafe { std::env::remove_var("SSL_CERT_FILE") };
    }
}

impl HttpsTestContext {
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
}
