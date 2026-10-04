# Rust Alternator client

## Glossary

- Alternator.
A DynamoDB API implemented on top of ScyllaDB backend.
Unlike AWS DynamoDB’s single endpoint, Alternator is distributed across multiple nodes.
Could be deployed anywhere: locally, on AWS, on any cloud provider.

- Client-side load balancing.
A method where the client selects which server (node) to send requests to,
rather than relying on a load balancing service.

- DynamoDB.
A managed NoSQL database service by AWS, typically accessed via a single regional endpoint.

- AWS Rust SDK.
The official AWS SDK for the Rust programming language, used to interact with AWS services like DynamoDB. Available [here](https://github.com/awslabs/aws-sdk-rust/tree/main/sdk/dynamodb).

- DynamoDB/Alternator Endpoint.
The base URL a client connects to.
In AWS DynamoDB, this is typically something like http://dynamodb.us-east-1.amazonaws.com.
In Alternator, it is the address of any node in the cluster.

- Datacenter (DC).
A physical or logical grouping of racks.
On Scylla Cloud in regular setup it represents cloud provider region where nodes are deployed.

- Rack.
A logical grouping akin to an availability zone within a datacenter.
On Scylla Cloud in regular setup it represents cloud provider availability zone where nodes are deployed.

## Introduction

This crate is a thin wrapper for the AWS Rust SDK that builds DynamoDB clients which load-balance across Alternator nodes.
It adds client-side discovery and load balancing, routing-scope controls, optional key-route affinity for LWT-heavy workloads, request/response compression, header stripping, and no-auth defaults for Alternator deployments.

## Using the crate

Add the crate to your `Cargo.toml`:

```toml
[dependencies]
alternator-client = "1.0"
aws-sdk-dynamodb = { version = "1.124", default-features = false }
tokio = { version = "1.49", features = ["macros", "rt-multi-thread", "sync", "time"] }
```

The crates.io package is named `alternator-client`; its Rust library and import
path remain `alternator_driver`.

For unreleased development versions, depend on the GitHub repository instead:

```toml
alternator-client = { git = "https://github.com/scylladb/alternator-client-rust" }
```

The direct `aws-sdk-dynamodb` dependency should use a version requirement compatible with the version selected by the client. Cargo will normally resolve one compatible `aws-sdk-dynamodb` 1.x and one compatible Tokio 1.x version for both your application and this crate.

This crate uses Rust 2024 edition and requires Rust 1.94.1 or newer. Your application can use a different Rust edition, but the toolchain must be new enough to compile this crate.

Keep the direct `aws-sdk-dynamodb` version aligned with the client and disable
its default features. The client enables the current AWS SDK HTTPS client;
enabling the SDK's legacy `rustls` feature adds an obsolete transport stack.

The AWS SDK groups its defaults into dated behavior major versions and normally asks each application to pick one. This client pins the version it is built and tested against, so there is nothing to choose and nothing to keep in sync for the clients it builds: Alternator's API does not vary with those bundles. Retry, timeout, and HTTP client settings remain individually configurable on the builder. The pin does not cover clients your application builds itself: the Alternator client deliberately does not enable the SDK's `behavior-version-latest` feature, so any `aws_sdk_dynamodb` client you construct directly still has to set a behavior version with `.behavior_version(...)` or enable that feature on your own `aws-sdk-dynamodb` dependency.

Because the Alternator Client follows the AWS SDK for DynamoDB operation builder interface for Alternator-supported features, migration usually starts by replacing `aws_sdk_dynamodb::Client` and its config type, like so:

```rust
use alternator_driver::*;              // <-- new import
use aws_sdk_dynamodb::types::*;

#[tokio::main]
async fn main() {
    // Build an AlternatorConfig instead of an aws_sdk_dynamodb::Config.
    let config = AlternatorConfig::builder() // <-- was aws_sdk_dynamodb::Config::builder()
        .seed_hosts(["localhost"])
        .port(8000)
        .build();

    // Build an AlternatorClient instead of an aws_sdk_dynamodb::Client.
    let client = AlternatorClient::from_conf(config); // <-- was aws_sdk_dynamodb::Client::from_conf

    // From here on, use the AWS SDK operation builders for Alternator-supported operations.
    client
        .put_item()
        .table_name("ExampleTable")
        .item("ExampleKey", AttributeValue::S("key".into()))
        .item("ExampleAttribute", AttributeValue::S("value".into()))
        .send()
        .await
        .unwrap();
}
```

When no credentials provider is configured, `AlternatorClient` enables no-auth automatically. Clients with a credentials provider continue to sign requests through the AWS SDK.

Alternator supports no-auth and SigV4 signing through configured or per-request credentials. Custom AWS SDK auth schemes, auth scheme preferences, and auth scheme resolvers are not exposed. Use `allow_no_auth()` when you want to make unsigned access explicit. Use `require_auth()` when a client without default credentials should require signed per-request credentials instead of falling back to no-auth.

[ScyllaDB's Alternator authentication documentation](https://docs.scylladb.com/manual/stable/alternator/compatibility.html#authentication-and-authorization) explains how to enable request validation with `alternator_enforce_authorization: true`. Alternator uses CQL role credentials rather than AWS IAM credentials: the role name is the access key ID and its `salted_hash` is the secret access key.

Header optimization detects SigV4 requests and retains every header named by the actual `SignedHeaders` value, including custom headers added before signing. [Alternator's verifier reconstructs and validates every signed header](https://github.com/scylladb/scylladb/blob/942e15ba0173594b919849a42590550c338c6731/alternator/server.cc#L303-L454), so the optimizer leaves a signed request intact if it cannot parse that value safely.

This client targets ScyllaDB Alternator. It does not guarantee that Alternator-specific configuration, no-auth defaults, or request optimizations remain compatible with AWS DynamoDB itself.

### Supported configuration surface

Build clients with `AlternatorConfig::builder()` and configure Alternator explicitly. The client intentionally does not import shared `aws_types::SdkConfig` values, because shared SDK config can contain AWS-specific auth and endpoint settings that do not map cleanly to Alternator.

There is no `AlternatorClient::new(&SdkConfig)`, `AlternatorConfig::new(&SdkConfig)`, or `AlternatorConfig::from(&SdkConfig)` shortcut. Start from `AlternatorConfig::builder()` and copy only the supported SDK settings your client needs, such as `region(...)`, `credentials_provider(...)`, `retry_config(...)`, `timeout_config(...)`, `http_client(...)`, `app_name(...)`, `framework_metadata(...)`, or `interceptor(...)`.

Supported auth modes are:
- no-auth, enabled automatically when no credentials provider is configured, or explicitly with `allow_no_auth()`
- SigV4 with a credentials provider configured through `credentials_provider(...)`
- SigV4 with per-request credentials, usually with a client built using `require_auth()`

The client does not expose AWS custom auth schemes, auth scheme resolvers, auth scheme preferences, account ID endpoint mode, FIPS endpoints, dual-stack endpoints, custom endpoint resolvers, or an SDK `endpoint_url(...)` builder setter. These APIs are intentionally absent rather than accepted and ignored. Use the Alternator-specific `scheme(...)`, `port(...)`, and `seed_hosts(...)` settings for discovery and client-side routing; the SDK endpoint follows from them. Use `user_agent(...)` for Alternator client identification.

Advanced SDK knobs such as retry settings, timeout settings, HTTP clients, identity cache, framework metadata, and interceptors remain available as escape hatches. Framework metadata is passed through to the underlying DynamoDB config for SDK integrations, while `user_agent(...)` controls the client's final Alternator identification. Interceptors run alongside the client's routing, compression, decompression, and header optimization interceptors, so keep ordering effects in mind when using them.

A configured HTTP client is also used for `/localnodes` discovery, so custom TLS trust stores, client certificates, proxies, and other transport settings apply to both discovery and regular Alternator API requests.

Operation builders are DynamoDB SDK passthroughs for source compatibility, but Alternator support is server-dependent. AWS-only surfaces such as backup/PITR/export/import, global tables, Kinesis streaming destinations, contributor insights, resource policies, tagging, `describe_endpoints`, `describe_limits`, PartiQL, and replica auto-scaling may fail against Alternator unless the server explicitly supports them.

## Load balancing

A single Alternator cluster typically consists of multiple nodes, any of which can serve any request. This crate distributes requests across the live nodes of the cluster rather than sending everything to one address. There's no separate load-balancer process, routing happens entirely client-side.

### Seed hosts

Unlike the AWS SDK configuration builder, `AlternatorBuilder` has no `endpoint_url(...)` setter. Requests go to cluster nodes it discovers for itself, so what it takes is *seed hosts*, together with the Alternator scheme and port. The endpoint the AWS SDK is pointed at follows from them, so there is no second setting to keep in step:

```rust
use alternator_driver::AlternatorConfig;

let config = AlternatorConfig::builder()
    .seed_hosts(["10.0.0.1"])
    .port(8043)
    .build();
```

For datacenter and rack scopes, the client calls `/localnodes` with the configured scope parameters. For the default cluster-wide scope, the client calls bare `/localnodes` on configured seed hosts and already-known live nodes, then unions the returned node lists. With discovery enabled, data-plane requests are rewritten to discovered live nodes after a routing target is selected.

To give the client multiple candidates for initial discovery, or for deployments where a seed node might be down at startup time, pass multiple seed addresses directly along with the Alternator scheme and port:

```rust
use alternator_driver::AlternatorConfig;

let config = AlternatorConfig::builder()
    .scheme("http")
    .port(8043)
    .seed_hosts([
        "10.0.0.1",
        "10.0.0.2",
        "10.0.0.3",
    ])
    .build();
```

For cluster-wide scope, provide at least one working seed host from every datacenter that should receive traffic. If a datacenter has no working seed in the configuration, the client cannot reliably discover and refresh live Alternator nodes from that datacenter.

To disable client-side discovery and load balancing, for example when sending through a proxy or an external load balancer, give that address as the seed host and turn discovery off:

```rust
use alternator_driver::AlternatorConfig;

let config = AlternatorConfig::builder()
    .seed_hosts(["load-balancer.example.com"])
    .port(8043)
    .without_discovery()
    .build();
```

In this mode every request goes to that address as it is, with no `/localnodes` discovery and no rewriting. Without a seed host to send them to, building a client fails rather than falling back to an AWS endpoint.

`AlternatorClient` instances are immutable. To retarget a client at another cluster, copy its configuration into a mutable builder, replace the routing settings, explicitly enable discovery, and construct a new client:

```rust
use alternator_driver::AlternatorClient;

// `client` is an existing AlternatorClient.
let mut builder = client.config().to_builder();
builder
    .set_seed_hosts(vec!["new-cluster".to_owned()])
    .set_port(8043)
    .set_without_discovery(false);

let retargeted = AlternatorClient::from_conf(builder.build());
```

### AWS SDK region

The AWS Rust SDK keeps a region in the DynamoDB configuration even when the
configured seed hosts point at Alternator instead of an AWS DynamoDB regional
endpoint. Alternator does not use this value for routing; this crate discovers
live nodes through `/localnodes` and rewrites requests to those nodes. The
region can still appear in SDK diagnostics, traces, metrics, and signing
metadata.

When no region is supplied, `AlternatorClient::from_conf` sets `us-east-1` as a
stable placeholder so the AWS SDK does not try to resolve a region from the
environment and fail before the client is built. If that placeholder is
misleading for your deployment, set an explicit region on the
`AlternatorConfig` builder:

```rust
use alternator_driver::AlternatorConfig;
use aws_sdk_dynamodb::config::Region;

let config = AlternatorConfig::builder()
    .seed_hosts(["10.0.0.1"])
    .port(8043)
    .region(Region::new("eu-central-1"))
    .build();
```

Choose the deployment or Scylla Cloud region that is useful for operators. This
does not change Alternator node discovery or load balancing.

### Node discovery

The client maintains a list of live nodes, which it refreshes in the background. The refresh has two cadences:

Discovery state is an internal client implementation detail. `AlternatorConfig`
stores only declarative settings; constructing multiple clients from cloned
configurations gives each client independent discovery state and a separate
refresh task.

- **Active** (default 1s): used while the client is being called regularly.
- **Idle** (default 60s): used when no caller has touched the client recently.

Both intervals are configurable:

```rust
use alternator_driver::AlternatorConfig;
use std::time::Duration;

let config = AlternatorConfig::builder()
    .seed_hosts(["10.0.0.1"])
    .port(8043)
    .active_interval(Duration::from_millis(500))
    .idle_interval(Duration::from_secs(30))
    .build();
```

The refresh task runs in the background for the lifetime of the client. It terminates automatically when the client is dropped.

### Routing scope

By default, the client uses every live Alternator node it discovers across the cluster. For deployments spanning multiple datacenters or racks, you usually want requests to stay within a specific datacenter — or within a specific rack of a specific datacenter — to minimize cross-zone latency and bandwidth.

This is configured via `RoutingScope`:

```rust
use alternator_driver::{AlternatorConfig, RoutingScope};

// Restrict to a single datacenter:
let scope = RoutingScope::from_datacenter("dc1".to_string());

// Restrict to a specific rack within a datacenter:
let scope = RoutingScope::from_rack("dc1".to_string(), "rack1".to_string());

// Don't restrict (the default)
let scope = RoutingScope::from_cluster();

let config = AlternatorConfig::builder()
    .seed_hosts(["10.0.0.1"])
    .port(8043)
    .routing_scope(scope)
    .build();
```

Key-route affinity is an intentional exception to rack-local routing. When it
is enabled with a rack scope, requests for which the client can build an
affinity plan select from every live node in the rack's datacenter. This lets
clients in different racks choose the same coordinator, but the selected node
may be in another rack even while the local rack is healthy, adding cross-zone
latency and bandwidth. Reads and other requests without an affinity plan keep
using the configured rack scope and fallback chain.

Before sending the first cross-rack affinity plan, the client waits for a
completed datacenter-wide topology refresh. If discovery cannot complete
within its bounded attempt budget, the request fails instead of routing from
an incomplete topology.

### Scope fallbacks

A scope can be narrow enough that no nodes match it — for example, a specific rack that has no live nodes at the moment. In that case the client uses the configured fallback scope instead. Fallbacks are explicit and chainable:

```rust
use alternator_driver::RoutingScope;

// Rack -> Datacenter -> Cluster fallback chain
let scope = RoutingScope::from_rack("dc1".to_string(), "rack1".to_string())
    .with_fallback(RoutingScope::from_datacenter("dc1".to_string()))
    .with_fallback(RoutingScope::from_cluster());

// Rack -> Another Rack -> Datacenter -> Cluster
let scope = RoutingScope::from_rack("dc1".to_string(), "rack1".to_string())
    .with_fallback(RoutingScope::from_rack("dc1".to_string(), "rack2".to_string()))
    .with_fallback(RoutingScope::from_datacenter("dc1".to_string()))
    .with_fallback(RoutingScope::from_cluster());
```
The first one says:
- prefer `rack1` of `dc1`
- if no nodes there, use any node in `dc1`
- if still nothing, use any live node discovered in the cluster

The client walks the chain from preferred to broadest, picking the first scope that has live nodes.

Each `.with_fallback(...)` call appends to the end of the chain, so the order in code matches the order of preference.

### Load balancing strategies

For every request, the client picks a node and rewrites the request URI to point at that node before signing. The default strategy is round-robin across the live nodes. Requests and retries share the same rotation. Retries skip nodes already tried for the current request until every live node has been tried, then start another pass through the plan.

Round-robin is the right default for the vast majority of workloads. For workloads that perform many LWTs against the same partition keys, see [Key route affinity](#key-route-affinity) below.

## Key route affinity

When using Lightweight Transactions (LWT) in ScyllaDB/Alternator, routing requests for the same partition key to the same coordinator node can significantly improve performance. This is because LWT operations require consensus among replicas, and using the same coordinator reduces coordination overhead. KeyRouteAffinity is a way to reduce this overhead by ensuring that two queries targeting the same partition key will be routed to the same coordinator. Instead of round-robin selection of nodes, it provides a deterministic mapping from partition key to coordinator.

### Configuration options

There are three KeyRouteAffinity modes:

1. **`KeyRouteAffinityType::None`** (default): Disabled. Requests are distributed using round-robin across nodes.
2. **`KeyRouteAffinityType::Rmw`**: Enables route affinity for conditional write operations, operations that need read before write.
3. **`KeyRouteAffinityType::AnyWrite`**: Enables route affinity for all write operations.


### When to use KeyRouteAffinity

Enable KeyRouteAffinity when:
- You perform conditional updates/deletes on the same items repeatedly
- You want to optimize LWT performance by ensuring the same coordinator handles requests for the same partition key

Which `KeyRouteAffinity` mode to use depends on your cluster's `alternator_write_isolation` setting. The table shows the maximum effective type for each mode. Narrower types are always valid too (e.g. `Rmw` or `None` on an `always` cluster if only conditional writes repeat or the writes are uniform):

| `alternator_write_isolation` | Description | Maximum effective `KeyRouteAffinityType` |
| --- | --- | --- |
| `only_rmw_uses_lwt` | Only RMW operations (conditional updates/deletes) use LWT. | `Rmw` |
| `always` | All writes use LWT. | `AnyWrite` |
| `forbid_rmw` | LWTs are completely disabled. Conditional operations will fail. | `None` |
| `unsafe_rmw` | Does not use LWT for RMW operations. | `None` |


### Automatic partition key discovery

When a request targets a table whose partition key the client hasn't seen before, the client calls `DescribeTable` once in the background to retrieve the partition key name. Subsequent requests for that table use the cached name. While discovery is in flight, that table's requests fall back to round-robin routing — they're not delayed waiting for the partition key to be discovered.

To skip discovery for a known set of tables, pre-configure their partition key names — see the configuration examples below.

### Configuring affinity

The simplest case: pass an affinity mode directly to the client builder.

```rust
use alternator_driver::{AlternatorConfig, AlternatorClient, KeyRouteAffinityType};

let client = AlternatorClient::from_conf(
    AlternatorConfig::builder()
        .seed_hosts(["10.0.0.1"])
        .port(8043)
        .key_route_affinity(KeyRouteAffinityType::Rmw)
        .build(),
);
```

This enables affinity in RMW mode with no pre-configured tables. The client discovers partition key names on first use of each table.

To pre-configure the partition key names for specific tables and skip the initial `DescribeTable` lookup, build a `KeyRouteAffinityConfig` and pass that instead:

```rust
use alternator_driver::{AlternatorConfig, AlternatorClient, KeyRouteAffinityConfig, KeyRouteAffinityType};

let affinity = KeyRouteAffinityConfig::builder()
    .with_type(KeyRouteAffinityType::Rmw)
    .with_pk_info("users", "user_id")
    .with_pk_info("orders", "order_id")
    .build();

let client = AlternatorClient::from_conf(
    AlternatorConfig::builder()
        .seed_hosts(["10.0.0.1"])
        .port(8043)
        .key_route_affinity(affinity)
        .build(),
);
```
`with_pk_info` can be called multiple times to register more tables. Tables not pre-configured will be discovered on first use as usual.

`.key_route_affinity(...)` accepts either a `KeyRouteAffinityType` (for the simple case) or a full `KeyRouteAffinityConfig` (for pre-configured tables). The two forms are interchangeable at the call site — pick whichever matches your needs.

## User-Agent

By default, the client replaces the AWS SDK `User-Agent` header with an Alternator client token:

```text
scylladb-alternator-client-rust/<version>
```

You can replace it exactly:

```rust
use alternator_driver::{AlternatorConfig, AlternatorClient};

let client = AlternatorClient::from_conf(
    AlternatorConfig::builder()
        .seed_hosts(["10.0.0.1"])
        .port(8043)
        .user_agent("orders-service/1.0")
        .build(),
);
```

You can derive a value from the default:

```rust
use alternator_driver::{AlternatorConfig, AlternatorClient, UserAgent};

let client = AlternatorClient::from_conf(
    AlternatorConfig::builder()
        .seed_hosts(["10.0.0.1"])
        .port(8043)
        .user_agent(UserAgent::transform(|default| {
            format!("{default} orders-service/1.0")
        }))
        .build(),
);
```

Or disable it:

```rust
use alternator_driver::{AlternatorConfig, AlternatorClient};

let client = AlternatorClient::from_conf(
    AlternatorConfig::builder()
        .seed_hosts(["10.0.0.1"])
        .port(8043)
        .without_user_agent()
        .build(),
);
```

## Header stripping

By default, the AWS Rust SDK attaches a number of headers to every DynamoDB request — some are required for signed requests (`Host`, `Authorization`, `X-Amz-Date`, etc.), while others are SDK metadata that unsigned requests do not need (`User-Agent` flavors, internal telemetry, retry information). For a small client-side optimization, this crate applies a compact allowlist before transmission, then writes the configured final `User-Agent`. Optimized requests keep only:
- `host`
- `x-amz-target`
- `content-length`
- `content-type`
- `accept-encoding`
- `content-encoding`
- `user-agent` unless disabled with `without_user_agent()`

When a request is signed, the optimizer also keeps:
- `authorization`
- `x-amz-date`
- `x-amz-user-agent`
- `x-amz-security-token` when session credentials are used
- every header named by the request's `Authorization` `SignedHeaders` value

This preserves signatures across compatible AWS SDK updates and for custom headers added before signing, while still removing unrelated unsigned metadata. If the optimizer cannot understand a signed request's authorization value, it skips stripping for that request rather than risk invalidating the signature.

## Request compression

Alternator accepts compressed requests to reduce bandwidth for write-heavy workloads (such as BatchWriteItem and large PutItem payloads).

You can enable compression in `AlternatorConfig`, like so:

```rust
use alternator_driver::{
    AlternatorClient,
    AlternatorConfig,
    CompressionAlgorithm,
    CompressionLevel,
    RequestCompression,
};

let client = AlternatorClient::from_conf(
    AlternatorConfig::builder()
        .seed_hosts(["10.0.0.1"])
        .port(8043)
        .request_compression(RequestCompression::enabled(
            CompressionAlgorithm::Gzip,
            CompressionLevel::default(),
            1024, // body-size threshold in bytes
        ))
        .build(),
);
```
or by using `.customize().alternator_config_override(...)` with `AlternatorConfig::operation_builder()` to override it for a specific client call.

Currently, the client supports two algorithms: Gzip and Deflate. For either one, you can specify a compression level (default: 6). Compression is applied to requests whose body size exceeds the configured threshold; if the threshold is 0, every request is compressed.

## Response compression

The client transparently decompresses gzip and deflate responses based on the `Content-Encoding` header. To request compressed responses, configure response compression in `AlternatorConfig`:

```rust
use alternator_driver::{
    AlternatorClient,
    AlternatorConfig,
    ResponseCompression,
    ResponseCompressionAlgorithm,
};

let client = AlternatorClient::from_conf(
    AlternatorConfig::builder()
        .seed_hosts(["10.0.0.1"])
        .port(8043)
        .response_compression(ResponseCompression::enabled(
            ResponseCompressionAlgorithm::Gzip,
        ))
        .allow_no_auth()
        .build(),
);
```

or by using `.customize().alternator_config_override(...)` with `AlternatorConfig::operation_builder()` to override it for a specific client call.

The default is `disabled()`; use `enabled()`, `enabled_many()`, or `enabled_all()` to advertise the desired encodings.

For safety, the default accepts at most five stacked content encodings and lets each decoding layer expand to at most 32 MiB. Responses that exceed either limit fail before deserialization. These limits also apply to compressed responses that were not negotiated. Use `with_max_encoding_layers()` and `with_max_decompressed_bytes()` when a deployment needs different limits.

## Per-operation override

To override an Alternator-specific setting for one request, use the same `.customize()` pattern that DynamoDB uses.

```rust
use alternator_driver::*; // Includes AlternatorCustomizableOperation.
use aws_sdk_dynamodb::types::*;
// ...
client
    .put_item()
    .table_name("ExampleTable")
    .item("ExampleKey", AttributeValue::S("ExampleItemKey".into()))
    .item("ExampleAttribute", AttributeValue::S("ExampleItem".into()))

    .customize()
    .alternator_config_override(
        AlternatorConfig::operation_builder()
            .request_compression(RequestCompression::disabled())
    )
    .send()
    .await
    .unwrap();
```

`alternator_config_override` currently applies only Alternator-specific compression settings: request compression and response compression. Use the AWS SDK's `config_override` separately for supported SDK-level per-operation overrides.

> **Note**: load-balancing, routing, and header stripping settings cannot be overridden per-operation. They take effect only when the client is constructed. Per-operation override is limited to request/response compression settings.

## Development

Run local static checks with:

```sh
make lint
```

Run unit tests that do not require ScyllaDB with:

```sh
make test-unit
```

Run the integration tests against a CCM-managed ScyllaDB node with:

```sh
make test-integration
```

Run the complete regular, topology, and load-balancing test suite with:

```sh
make test-all
```

The integration and complete test targets require `scylla-ccm` to be installed and available on `PATH`. They create a temporary `alternator-client-rust` CCM cluster and remove it when the tests finish. Use `make scylla-rm` to remove that cluster manually if a run is interrupted.

Release maintainers should follow the
[artifact-first release runbook](https://github.com/scylladb/alternator-client-rust/blob/main/docs/releasing.md).
Do not publish from a development checkout of `main`.
