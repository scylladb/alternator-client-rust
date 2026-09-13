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

use alternator_driver::{AlternatorClient, AlternatorConfig};
use aws_sdk_dynamodb::config::BehaviorVersion;
use http_body_util::Full;
use hyper::body::{Bytes, Incoming};
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper::{Request, Response};
use hyper_util::rt::TokioIo;
use rcgen::{BasicConstraints, CertificateParams, IsCa, KeyPair};
use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use std::convert::Infallible;
use std::ffi::OsString;
use std::sync::Arc;
use std::time::Duration;
use tokio::net::TcpListener;
use tokio_rustls::TlsAcceptor;
use uuid::Uuid;

fn list_tables_response(
    _request: Request<Incoming>,
) -> impl Future<Output = Result<Response<Full<Bytes>>, Infallible>> {
    std::future::ready(Ok(Response::new(Full::new(Bytes::from_static(
        br#"{"TableNames":[]}"#,
    )))))
}

async fn serve_http(listener: TcpListener) {
    loop {
        let (stream, _) = listener.accept().await.unwrap();
        tokio::spawn(async move {
            http1::Builder::new()
                .keep_alive(false)
                .serve_connection(TokioIo::new(stream), service_fn(list_tables_response))
                .await
                .unwrap();
        });
    }
}

async fn serve_https(listener: TcpListener, acceptor: TlsAcceptor) {
    loop {
        let (stream, _) = listener.accept().await.unwrap();
        let acceptor = acceptor.clone();
        tokio::spawn(async move {
            let stream = acceptor.accept(stream).await.unwrap();
            http1::Builder::new()
                .keep_alive(false)
                .serve_connection(TokioIo::new(stream), service_fn(list_tables_response))
                .await
                .unwrap();
        });
    }
}

fn tls_fixture() -> (String, TlsAcceptor) {
    let ca_key = KeyPair::generate().unwrap();
    let mut ca_params = CertificateParams::new(vec!["Transport test CA".to_string()]).unwrap();
    ca_params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    ca_params.key_usages = vec![rcgen::KeyUsagePurpose::KeyCertSign];
    let ca_cert = ca_params.self_signed(&ca_key).unwrap();

    let server_key = KeyPair::generate().unwrap();
    let server_params = CertificateParams::new(vec!["localhost".to_string()]).unwrap();
    let server_cert = server_params
        .signed_by(&server_key, &ca_cert, &ca_key)
        .unwrap();
    let server_config = rustls::ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(
            vec![CertificateDer::from(server_cert.der().to_vec())],
            PrivateKeyDer::try_from(server_key.serialize_der()).unwrap(),
        )
        .unwrap();

    (ca_cert.pem(), TlsAcceptor::from(Arc::new(server_config)))
}

unsafe fn restore_var(name: &str, value: Option<OsString>) {
    match value {
        Some(value) => unsafe { std::env::set_var(name, value) },
        None => unsafe { std::env::remove_var(name) },
    }
}

#[tokio::test(flavor = "current_thread")]
async fn pre_2026_behavior_versions_can_send_http_and_https_requests() {
    let (ca_pem, acceptor) = tls_fixture();
    let cert_path = std::env::temp_dir().join(format!(
        "alternator-driver-transport-ca-{}.pem",
        Uuid::new_v4()
    ));
    let cert_dir = std::env::temp_dir().join(format!(
        "alternator-driver-transport-ca-dir-{}",
        Uuid::new_v4()
    ));
    std::fs::write(&cert_path, ca_pem).unwrap();
    std::fs::create_dir(&cert_dir).unwrap();

    let previous_file = std::env::var_os("SSL_CERT_FILE");
    let previous_dir = std::env::var_os("SSL_CERT_DIR");
    let previous_no_proxy = std::env::var_os("NO_PROXY");
    let previous_lower_no_proxy = std::env::var_os("no_proxy");
    // SAFETY: This integration-test binary contains one test. The variables
    // are set before any client or task is created and restored below.
    unsafe {
        std::env::set_var("SSL_CERT_FILE", &cert_path);
        std::env::set_var("SSL_CERT_DIR", &cert_dir);
        std::env::set_var("NO_PROXY", "localhost,127.0.0.1");
        std::env::set_var("no_proxy", "localhost,127.0.0.1");
    }

    let http_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let http_port = http_listener.local_addr().unwrap().port();
    let https_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let https_port = https_listener.local_addr().unwrap().port();
    let http_server = tokio::spawn(serve_http(http_listener));
    let https_server = tokio::spawn(serve_https(https_listener, acceptor));

    #[allow(deprecated)]
    let behavior_versions = [
        ("v2023_11_09", BehaviorVersion::v2023_11_09()),
        ("v2024_03_28", BehaviorVersion::v2024_03_28()),
        ("v2025_01_17", BehaviorVersion::v2025_01_17()),
        ("v2025_08_07", BehaviorVersion::v2025_08_07()),
    ];
    let endpoints = [
        format!("http://localhost:{http_port}"),
        format!("https://localhost:{https_port}"),
    ];
    let mut failures = Vec::new();

    for (version_name, behavior_version) in behavior_versions {
        for endpoint in &endpoints {
            let client = match AlternatorClient::try_from_conf(
                AlternatorConfig::builder()
                    .endpoint_url(endpoint)
                    .seed_hosts(Vec::<String>::new())
                    .behavior_version(behavior_version)
                    .build(),
            ) {
                Ok(client) => client,
                Err(error) => {
                    failures.push(format!("{version_name} {endpoint}: build failed: {error}"));
                    continue;
                }
            };

            match tokio::time::timeout(Duration::from_secs(5), client.list_tables().send()).await {
                Ok(Ok(_)) => {}
                Ok(Err(error)) => {
                    failures.push(format!(
                        "{version_name} {endpoint}: request failed: {error}"
                    ));
                }
                Err(_) => failures.push(format!("{version_name} {endpoint}: request timed out")),
            }
        }
    }

    http_server.abort();
    https_server.abort();
    // SAFETY: Restore the process environment after all clients and requests
    // have finished and before checking the accumulated failures.
    unsafe {
        restore_var("SSL_CERT_FILE", previous_file);
        restore_var("SSL_CERT_DIR", previous_dir);
        restore_var("NO_PROXY", previous_no_proxy);
        restore_var("no_proxy", previous_lower_no_proxy);
    }
    std::fs::remove_file(cert_path).unwrap();
    std::fs::remove_dir(cert_dir).unwrap();

    assert!(failures.is_empty(), "{}", failures.join("\n"));
}
