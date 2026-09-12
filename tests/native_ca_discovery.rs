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
use std::path::Path;

type BuildOutcome = Result<(), String>;

fn build_with_native_ca_paths(cert_file: &Path, cert_dir: &Path) -> [BuildOutcome; 4] {
    // SAFETY: This integration-test binary contains one test, and no work is
    // spawned before these variables are restored below.
    unsafe {
        std::env::set_var("SSL_CERT_FILE", cert_file);
        std::env::set_var("SSL_CERT_DIR", cert_dir);
    }

    let http_discovery = AlternatorClient::try_from_conf(
        AlternatorConfig::builder()
            .behavior_version_latest()
            .scheme("http")
            .port(8000)
            .seed_hosts(["127.0.0.1"])
            .build(),
    );
    let https_discovery = AlternatorClient::try_from_conf(
        AlternatorConfig::builder()
            .behavior_version_latest()
            .scheme("https")
            .port(8043)
            .seed_hosts(["127.0.0.1"])
            .build(),
    );
    let direct_https = AlternatorClient::try_from_conf(
        AlternatorConfig::builder()
            .behavior_version_latest()
            .endpoint_url("https://127.0.0.1:8043")
            .seed_hosts(Vec::<String>::new())
            .build(),
    );
    let custom_direct_https = AlternatorClient::try_from_conf(
        AlternatorConfig::builder()
            .behavior_version_latest()
            .endpoint_url("https://127.0.0.1:8043")
            .seed_hosts(Vec::<String>::new())
            .http_client(aws_smithy_http_client::Builder::new().build_http())
            .build(),
    );

    [
        http_discovery,
        https_discovery,
        direct_https,
        custom_direct_https,
    ]
    .map(|outcome| outcome.map(|_| ()).map_err(|error| error.to_string()))
}

fn assert_tls_outcomes(outcomes: &[BuildOutcome; 4]) {
    assert!(outcomes[0].is_ok(), "{:?}", outcomes[0]);

    let discovery_error = outcomes[1]
        .as_ref()
        .expect_err("HTTPS discovery must reject unusable native roots");
    assert!(
        discovery_error.contains("failed to configure discovery TLS"),
        "{discovery_error}"
    );

    let direct_error = outcomes[2]
        .as_ref()
        .expect_err("direct HTTPS must reject unusable native roots");
    assert!(
        direct_error.contains("failed to configure SDK TLS"),
        "{direct_error}"
    );

    assert!(outcomes[3].is_ok(), "{:?}", outcomes[3]);
}

#[test]
fn unusable_native_roots_fail_https_construction_without_panicking() {
    let missing_path = std::env::temp_dir().join(format!(
        "alternator-driver-missing-ca-{}",
        uuid::Uuid::new_v4()
    ));
    assert!(!missing_path.exists());
    let empty_file = std::env::temp_dir().join(format!(
        "alternator-driver-empty-ca-{}",
        uuid::Uuid::new_v4()
    ));
    let empty_dir = std::env::temp_dir().join(format!(
        "alternator-driver-empty-ca-dir-{}",
        uuid::Uuid::new_v4()
    ));
    std::fs::write(&empty_file, []).unwrap();
    std::fs::create_dir(&empty_dir).unwrap();

    let previous_file = std::env::var_os("SSL_CERT_FILE");
    let previous_dir = std::env::var_os("SSL_CERT_DIR");

    let missing_outcomes = build_with_native_ca_paths(&missing_path, &missing_path);
    let empty_outcomes = build_with_native_ca_paths(&empty_file, &empty_dir);

    // SAFETY: Restore process environment before running assertions.
    unsafe {
        match previous_file {
            Some(value) => std::env::set_var("SSL_CERT_FILE", value),
            None => std::env::remove_var("SSL_CERT_FILE"),
        }
        match previous_dir {
            Some(value) => std::env::set_var("SSL_CERT_DIR", value),
            None => std::env::remove_var("SSL_CERT_DIR"),
        }
    }
    std::fs::remove_file(empty_file).unwrap();
    std::fs::remove_dir(empty_dir).unwrap();

    assert_tls_outcomes(&missing_outcomes);
    assert_tls_outcomes(&empty_outcomes);

    let empty_discovery_error = empty_outcomes[1].as_ref().unwrap_err();
    assert!(
        empty_discovery_error.contains("no usable native CA certificates were found"),
        "{empty_discovery_error}"
    );
}
