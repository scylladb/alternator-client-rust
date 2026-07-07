//! DynamoDB interface compatibility tests
//!
//! The Alternator driver introduces several wrappers around DynamoDB's utilities,
//! e.g. AlternatorClient, AlternatorConfig, and AlternatorBuilder.
//!
//! The following 3 tests (one for each) assert that our wrappers include all
//! of the necessary methods from DynamoDB.
//!
//! Note that there are some exceptional methods that we omit.
//!
//! Test workflow:
//!     1. Using the `rustdoc` command, we generate .json documentation files for
//!         the alternator-driver and aws-sdk-dynamodb crates.
//!
//!     2. We load these crates into memory using `rustdoc_types::Crate`.
//!
//!     3. We find the corresponding wrapper struct, collect its methods,
//!         along with the traits that introduce them (None if a method is an orphan).
//!
//!     4. We assert that all methods implemented by DynamoDB's driver are also
//!         implemented by our driver, with some exceptions.
//!
use rustdoc_types::*;
use std::collections::HashSet;
use std::process::Command;
use std::sync::LazyLock;

/// Use `rustdoc` to generate .json documentation files for
/// aws-sdk-dynamodb and alternator-driver crates.
fn generate_json_docs() {
    // alternator-driver
    let output = Command::new("cargo")
        .args([
            "+nightly",
            "rustdoc",
            "--",
            "-Z",
            "unstable-options",
            "--output-format",
            "json",
        ])
        .output()
        .expect("Failed to execute command");

    assert!(
        output.status.success(),
        "Command failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    // aws-sdk-dynamodb
    let output = Command::new("cargo")
        .args([
            "+nightly",
            "rustdoc",
            "-p",
            "aws-sdk-dynamodb",
            "--",
            "-Z",
            "unstable-options",
            "--output-format",
            "json",
        ])
        .output()
        .expect("Failed to execute command");

    assert!(
        output.status.success(),
        "Command failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
}

/// Load the documentation file of a specified crate into a readable rustdoc_types::Crate struct
fn load_json_docs(package_name: &str) -> Crate {
    let path = format!("target/doc/{}.json", package_name.replace('-', "_"));
    let json_content = std::fs::read_to_string(&path)
        .unwrap_or_else(|_| panic!("Could not find JSON file at {}", path));

    serde_json::from_str(&json_content).expect("Failed to parse rustdoc JSON")
}

/// Collect methods of specified struct from specified crate.
///
/// The return value is a set of `(trait_name, method_name)` pairs.
/// `trait_name` is optional, as a method may not come from a trait.
fn collect_struct_methods(crate_: &Crate, struct_name: &str) -> HashSet<(Option<String>, String)> {
    // find the struct
    let struct_ = crate_
        .index
        .values()
        .filter_map(|item| {
            if item.name.as_deref() == Some(struct_name)
                && let ItemEnum::Struct(ref s) = item.inner
            {
                return Some(s);
            }
            None
        })
        .next()
        .expect("struct doesnt exist");

    // find struct's implementations
    let impl_ = struct_
        .impls
        .iter()
        .filter_map(|id| crate_.index.get(id))
        .filter_map(|item| {
            if let ItemEnum::Impl(ref i) = item.inner {
                Some(i)
            } else {
                None
            }
        });

    // list (trait name, method name) pairs implemented by the struct
    let methods = impl_
        .flat_map(|impl_| {
            let trait_name = impl_.trait_.as_ref().map(|path| path.path.clone()).clone();
            impl_.items.iter().map(move |id| (trait_name.clone(), id))
        })
        .filter_map(|(trait_name, id)| {
            let item = crate_.index.get(id)?;
            if let ItemEnum::Function(_) = &item.inner {
                Some((trait_name, item.name.clone().unwrap_or_default()))
            } else {
                None
            }
        });

    // duplicates may arise from generic traits,
    // but we only care for (trait_name, method_name) pairs
    methods.collect()
}

static DOCS: LazyLock<(Crate, Crate)> = LazyLock::new(|| {
    // generate .json docs for alternator-driver and aws-sdk-dynamodb
    generate_json_docs();

    // load json files, and deserialize into readable objects
    (
        load_json_docs("alternator-driver"),
        load_json_docs("aws-sdk-dynamodb"),
    )
});

#[test]
fn test_client() {
    // generate json docs for aws-sdk-dynamodb and alternator-driver (if needed), load them into memory
    let (alternator_driver, dynamodb) = &*DOCS;

    // look up methods implemented by dynamodb but not our driver
    // HashSet<(Option<trait_name>, method_name)>
    let alternator_client_methods = collect_struct_methods(alternator_driver, "AlternatorClient");
    let dynamodb_client_methods = collect_struct_methods(dynamodb, "Client");

    let mut with_exceptions = dynamodb_client_methods;
    with_exceptions.remove(&(None, "new".into())); // use AlternatorClient::from_conf with explicit AlternatorConfig

    let unimplemented = with_exceptions.difference(&alternator_client_methods);
    let all_implemented = unimplemented.clone().next().is_none();
    assert!(
        all_implemented,
        "Not all wanted aws_sdk_dynamodb::Client methods are implemented by AlternatorClient: {:?}",
        unimplemented
    );
}

#[test]
fn test_config() {
    // generate json docs for aws-sdk-dynamodb and alternator-driver (if needed), load them into memory
    let (alternator_driver, dynamodb) = &*DOCS;

    // look up methods implemented by dynamodb but not our driver
    // HashSet<(Option<trait_name>, method_name)>
    let alternator_config_methods = collect_struct_methods(alternator_driver, "AlternatorConfig");
    let dynamodb_config_methods = collect_struct_methods(dynamodb, "Config");

    // allow exceptions
    let mut with_exceptions = dynamodb_config_methods;
    with_exceptions.remove(&(None, "new".into())); // shared SdkConfig imports hide AWS settings that do not map to Alternator
    with_exceptions.remove(&(None, "credentials_provider".into())); // credentials_provider is deprecated and always returns None
    with_exceptions.remove(&(None, "auth_scheme_preference".into())); // use AlternatorBuilder::require_auth instead of AWS auth preference hints
    with_exceptions.remove(&(None, "auth_schemes".into())); // custom AWS auth schemes are not supported by Alternator
    with_exceptions.remove(&(None, "auth_scheme_resolver".into()));
    with_exceptions.remove(&(None, "endpoint_resolver".into())); // custom endpoint resolution conflicts with client-side routing

    let unimplemented = with_exceptions.difference(&alternator_config_methods);
    let all_implemented = unimplemented.clone().next().is_none();
    assert!(
        all_implemented,
        "Not all wanted aws_sdk_dynamodb::Config methods are implemented by AlternatorConfig: {:?}",
        unimplemented
    );
}

#[test]
fn test_builder() {
    // generate json docs for aws-sdk-dynamodb and alternator-driver (if needed), load them into memory
    let (alternator_driver, dynamodb) = &*DOCS;

    // look up methods implemented by dynamodb but not our driver
    // HashSet<(Option<trait_name>, method_name)>
    let alternator_builder_methods = collect_struct_methods(alternator_driver, "AlternatorBuilder");
    let dynamodb_builder_methods = collect_struct_methods(dynamodb, "Builder");

    // allow exceptions
    let mut with_exceptions = dynamodb_builder_methods;

    with_exceptions.remove(&(None, "apply_test_defaults".into())); // only on dynamodb's test-util feature
    with_exceptions.remove(&(None, "apply_test_defaults_v2".into()));
    with_exceptions.remove(&(None, "with_test_defaults".into()));
    with_exceptions.remove(&(None, "with_test_defaults_v2".into()));

    with_exceptions.remove(&(None, "set_idempotency_token_provider".into())); // not supported as IdempotencyTokenProvider is private
    with_exceptions.remove(&(None, "idempotency_token_provider".into()));
    with_exceptions.remove(&(None, "auth_scheme_preference".into())); // use require_auth for strict signed requests
    with_exceptions.remove(&(None, "set_auth_scheme_preference".into()));
    with_exceptions.remove(&(None, "push_auth_scheme".into())); // custom AWS auth schemes are not supported by Alternator
    with_exceptions.remove(&(None, "auth_scheme_resolver".into()));
    with_exceptions.remove(&(None, "set_auth_scheme_resolver".into()));
    with_exceptions.remove(&(None, "endpoint_resolver".into())); // custom endpoint resolution conflicts with client-side routing
    with_exceptions.remove(&(None, "set_endpoint_resolver".into()));
    with_exceptions.remove(&(None, "account_id_endpoint_mode".into())); // AWS account/FIPS/dual-stack endpoint modes do not apply to Alternator
    with_exceptions.remove(&(None, "set_account_id_endpoint_mode".into()));
    with_exceptions.remove(&(None, "use_dual_stack".into()));
    with_exceptions.remove(&(None, "set_use_dual_stack".into()));
    with_exceptions.remove(&(None, "use_fips".into()));
    with_exceptions.remove(&(None, "set_use_fips".into()));

    let unimplemented = with_exceptions.difference(&alternator_builder_methods);
    let all_implemented = unimplemented.clone().next().is_none();
    assert!(
        all_implemented,
        "Not all wanted aws_sdk_dynamodb::Builder methods are implemented by AlternatorBuilder: {:?}",
        unimplemented
    );
}
