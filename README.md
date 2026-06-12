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
Includes optimizations for Lightweight Transactions (LWTs), request compression, and header stripping.

## Using the crate


Add the crate to your `Cargo.toml`:

```toml
[dependencies]
alternator-driver = { git = "https://github.com/scylladb/alternator-client-rust" }
aws-sdk-dynamodb = "1"
tokio = { version = "1.18", features = ["macros", "rt-multi-thread", "sync", "time"] }
```
> **Note**: This crate is not yet published to crates.io. Depend on it via the GitHub URL.

Because the Alternator Client is designed with an interface identical to the AWS SDK for DynamoDB, developers can seamlessly swap out aws_sdk_dynamodb::Client in their projects, like so:

```rust
use alternator_driver::*;              // <-- new import
use aws_sdk_dynamodb::types::*;

#[tokio::main]
async fn main() {
    // Build an AlternatorConfig instead of an aws_sdk_dynamodb::Config.
    let config = AlternatorConfig::builder() // <-- was aws_sdk_dynamodb::Config::builder()
        .endpoint_url("http://localhost:8000")
        .behavior_version_latest()
        .allow_no_auth()
        .build();

    // Build an AlternatorClient instead of an aws_sdk_dynamodb::Client.
    let client = AlternatorClient::from_conf(config); // <-- was aws_sdk_dynamodb::Client::from_conf

    // From here on, the API is identical to the AWS SDK.
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

## Load balancing

A single Alternator cluster typically consists of multiple nodes, any of which can serve any request. This crate distributes requests across the live nodes of the cluster rather than sending everything to one address. There's no separate load-balancer process, routing happens entirely client-side.

### Seed hosts vs endpoint URL

The simplest way to construct a client is with `endpoint_url`, the same field the AWS SDK uses:

```rust
use alternator_driver::AlternatorConfig;

let config = AlternatorConfig::builder()
    .endpoint_url("http://10.0.0.1:8043")
    .behavior_version_latest()
    .allow_no_auth()
    .build();
```

The host in the URL is treated as a *seed*. For datacenter and rack scopes, the client calls `/localnodes` with the configured scope parameters. For the default cluster-wide scope, the client calls bare `/localnodes` on configured seed hosts and already-known live nodes, then unions the returned node lists. The endpoint URL is never used for actual data-plane traffic after discovery completes.

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
    .behavior_version_latest()
    .allow_no_auth()
    .build();
```

For cluster-wide scope, provide at least one working seed host from every datacenter that should receive traffic. If a datacenter has no working seed in the configuration, the client cannot reliably discover and refresh live Alternator nodes from that datacenter.

### Node discovery

The client maintains a list of live nodes, which it refreshes in the background. The refresh has two cadences:

- **Active** (default 1s): used while the client is being called regularly.
- **Idle** (default 60s): used when no caller has touched the client recently.

Both intervals are configurable:

```rust

.active_interval(std::time::Duration::from_millis(500))
.idle_interval(std::time::Duration::from_secs(30))
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
    .endpoint_url("http://10.0.0.1:8043")
    .routing_scope(scope)
    .behavior_version_latest()
    .allow_no_auth()
    .build();
```

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

For every request, the client picks a node and rewrites the request URI to point at that node before signing. The default strategy is round-robin across the live nodes. Requests and retries share the same rotation, and retries skip nodes already tried for the current request.

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

When a request targets a table whose partition key the driver hasn't seen before, the driver calls `DescribeTable` once in the background to retrieve the partition key name. Subsequent requests for that table use the cached name. While discovery is in flight, that table's requests fall back to round-robin routing — they're not delayed waiting for the partition key to be discovered.

To skip discovery for a known set of tables, pre-configure their partition key names — see the configuration examples below.

### Configuring affinity

The simplest case: pass an affinity mode directly to the client builder.

```rust
use alternator_driver::{AlternatorConfig, AlternatorClient, KeyRouteAffinityType};

let client = AlternatorClient::from_conf(
    AlternatorConfig::builder()
        .endpoint_url("http://10.0.0.1:8043")
        .key_route_affinity(KeyRouteAffinityType::Rmw)
        .behavior_version_latest()
        .allow_no_auth()
        .build(),
);
```

This enables affinity in RMW mode with no pre-configured tables. The driver discovers partition key names on first use of each table.

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
        .endpoint_url("http://10.0.0.1:8043")
        .key_route_affinity(affinity)
        .behavior_version_latest()
        .allow_no_auth()
        .build(),
);
```
`with_pk_info` can be called multiple times to register more tables. Tables not pre-configured will be discovered on first use as usual.

`.key_route_affinity(...)` accepts either a `KeyRouteAffinityType` (for the simple case) or a full `KeyRouteAffinityConfig` (for pre-configured tables). The two forms are interchangeable at the call site — pick whichever matches your needs.

## Header stripping

By default, the AWS Rust SDK attaches a number of headers to every DynamoDB request — some are required (`Host`, `Authorization`, `X-Amz-Date`, etc.), others are SDK metadata that Alternator doesn't use (`User-Agent` flavors, internal telemetry, retry information). For a small client-side optimization, this crate strips non-essential headers before transmission, keeping only the ones Alternator actually needs:
- `host`
- `x-amz-target`
- `content-length`
- `accept-encoding`
- `content-encoding`
- `authorization`
- `x-amz-date`

This is on by default, you can disable it if needed:

```rust
use alternator_driver::{AlternatorConfig, AlternatorClient};

let client = AlternatorClient::from_conf(
    AlternatorConfig::builder()
        .endpoint_url("http://10.0.0.1:8043")
        .optimize_headers(false)
        .behavior_version_latest()
        .allow_no_auth()
        .build(),
);
```

## Request compression

Alternator accepts compressed requests to reduce bandwidth for write-heavy workloads (such as BatchWriteItem and large PutItem payloads).

You can enable compression in `AlternatorConfig`, like so:
```rust
use alternator_driver::{AlternatorConfig, AlternatorClient, RequestCompression, CompressionAlgorithm, CompressionLevel};

let client = AlternatorClient::from_conf(
    AlternatorConfig::builder()
        .endpoint_url("http://10.0.0.1:8043")
        .request_compression(RequestCompression::enabled(
            CompressionAlgorithm::Gzip,
            CompressionLevel::default(),
            1024, // body-size threshold in bytes
        ))
        .behavior_version_latest()
        .allow_no_auth()
        .build(),
);
```
or by using `.customize().alternator_config_override()` to enable it for a specific driver call.

Currently, the driver supports two algorithms: Gzip and Deflate. For either one, you can specify a compression level (default: 6). Compression is applied to requests whose body size exceeds the configured threshold; if the threshold is 0, every request is compressed.

## Response compression

The driver transparently decompresses gzip and deflate responses based on the `Content-Encoding` header. To request compressed responses, configure response compression in `AlternatorConfig`:

```rust
use alternator_driver::{AlternatorConfig, AlternatorClient, ResponseCompression, ResponseCompressionAlgorithm};

let client = AlternatorClient::from_conf(
    AlternatorConfig::builder()
        .endpoint_url("http://10.0.0.1:8043")
        .response_compression(ResponseCompression::enabled(
            ResponseCompressionAlgorithm::Gzip,
        ))
        .behavior_version_latest()
        .allow_no_auth()
        .build(),
);
```

or by using `.customize().alternator_config_override()` to enable it for a specific driver call.

The default is `disabled()`; use `enabled()`, `enabled_many()`, or `enabled_all()` to advertise the desired encodings.

## Per-operation override

In case an Alternator-specific setting is to be overridden for a specified driver call, you can use the same `.customize()` pattern that DynamoDB uses.

```rust
use alternator_driver::*; // Include AlternatorCustomizableOperation - trait responsible for customization
use aws_sdk_dynamodb::types::*;
// ...
client
    .put_item()
    .table_name("ExampleTable")
    .item("ExampleKey", AttributeValue::S("ExampleItemKey".into()))
    .item("ExampleAttribute", AttributeValue::S("ExampleItem".into()))

    .customize()
    .alternator_config_override(    // <-- Instead of config_override
        AlternatorConfig::builder() // <-- Instead of aws_sdk_dynamodb::Config
            .request_compression(RequestCompression::disabled())
    )
    .send()
    .await
    .unwrap();
```

`alternator_config_override` is a direct extension of `config_override`, it also allows the developer to override all DynamoDB settings.

> **Note**: load-balancing and endpoint settings cannot be overridden per-operation. They take effect only when the client is constructed. Per-operation override is for settings that apply to individual request processing — compression and header stripping.
