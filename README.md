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

Because the Alternator Client follows the AWS SDK for DynamoDB operation builder interface for Alternator-supported features, migration usually starts by replacing `aws_sdk_dynamodb::Client` and its config type, like so:

```rust
use alternator_driver::*;              // <-- new import
use aws_sdk_dynamodb::types::*;

#[tokio::main]
async fn main() {
    // Build an AlternatorConfig instead of an aws_sdk_dynamodb::Config.
    let config = AlternatorConfig::builder() // <-- was aws_sdk_dynamodb::Config::builder()
        .endpoint_url("http://localhost:8000")
        .behavior_version_latest()
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

When no credentials provider is configured, `AlternatorClient` enables no-auth automatically. Clients with a credentials provider continue to sign requests through the AWS SDK. Alternator supports no-auth and SigV4 signing through configured or per-request credentials; custom AWS SDK auth schemes, auth scheme preferences, and auth scheme resolvers are not exposed. Use `require_auth()` when a client without default credentials should require signed per-request credentials instead of falling back to no-auth.

This client targets ScyllaDB Alternator. It does not guarantee that Alternator-specific configuration, no-auth defaults, or request optimizations remain compatible with AWS DynamoDB itself.

### Supported configuration surface

Build clients with `AlternatorConfig::builder()` and set Alternator behavior explicitly. The driver intentionally does not import shared `aws_types::SdkConfig` values, because shared SDK config can contain AWS-specific auth and endpoint settings that do not map cleanly to Alternator.

There is no `AlternatorClient::new(&SdkConfig)`, `AlternatorConfig::new(&SdkConfig)`, or `AlternatorConfig::from(&SdkConfig)` shortcut. Start from `AlternatorConfig::builder()` and copy only the supported SDK settings your client needs, such as `region(...)`, `credentials_provider(...)`, `retry_config(...)`, `timeout_config(...)`, `http_client(...)`, `app_name(...)`, or `interceptor(...)`.

Supported auth modes are:
- no-auth, enabled automatically when no credentials provider is configured, or explicitly with `allow_no_auth()`
- SigV4 with a credentials provider configured through `credentials_provider(...)`
- SigV4 with per-request credentials, usually with a client built using `require_auth()`

The driver does not expose AWS custom auth schemes, auth scheme resolvers, auth scheme preferences, account ID endpoint mode, FIPS endpoints, dual-stack endpoints, or custom endpoint resolvers. These APIs are intentionally absent rather than accepted and ignored. Use `endpoint_url(...)` or the Alternator-specific `scheme(...)`, `port(...)`, and `seed_hosts(...)` settings for discovery and client-side routing.

Advanced SDK knobs such as retry settings, timeout settings, HTTP clients, identity cache, and interceptors remain available as escape hatches. Interceptors run alongside the driver's routing, compression, decompression, and header optimization interceptors, so keep ordering effects in mind when using them.

Operation builders are DynamoDB SDK passthroughs for source compatibility, but Alternator support is server-dependent. AWS-only surfaces such as backup/PITR/export/import, global tables, Kinesis streaming destinations, contributor insights, resource policies, tagging, `describe_endpoints`, `describe_limits`, PartiQL, and replica auto-scaling may fail against Alternator unless the server explicitly supports them.

## Load balancing

A single Alternator cluster typically consists of multiple nodes, any of which can serve any request. This crate distributes requests across the live nodes of the cluster rather than sending everything to one address. There's no separate load-balancer process, routing happens entirely client-side.

### Seed hosts vs endpoint URL

The simplest way to construct a client is with `endpoint_url`, the same field the AWS SDK uses:

```rust
use alternator_driver::AlternatorConfig;

let config = AlternatorConfig::builder()
    .endpoint_url("http://10.0.0.1:8043")
    .behavior_version_latest()
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
    .build();
```

For cluster-wide scope, provide at least one working seed host from every datacenter that should receive traffic. If a datacenter has no working seed in the configuration, the client cannot reliably discover and refresh live Alternator nodes from that datacenter.

### AWS SDK region

The AWS Rust SDK keeps a region in the DynamoDB configuration even when
`endpoint_url` points at Alternator instead of an AWS DynamoDB regional
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
use aws_sdk_dynamodb::config::Region;
use alternator_driver::AlternatorConfig;

let config = AlternatorConfig::builder()
    .endpoint_url("http://10.0.0.1:8043")
    .region(Region::new("eu-central-1"))
    .build();
```

Choose the deployment or Scylla Cloud region that is useful for operators. This
does not change Alternator node discovery or load balancing.

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
        .endpoint_url("http://10.0.0.1:8043")
        .user_agent("orders-service/1.0")
        .behavior_version_latest()
        .build(),
);
```

You can derive a value from the default:

```rust
use alternator_driver::{AlternatorConfig, AlternatorClient, UserAgent};

let client = AlternatorClient::from_conf(
    AlternatorConfig::builder()
        .endpoint_url("http://10.0.0.1:8043")
        .user_agent(UserAgent::transform(|default| {
            format!("{default} orders-service/1.0")
        }))
        .behavior_version_latest()
        .build(),
);
```

Or disable it:

```rust
use alternator_driver::{AlternatorConfig, AlternatorClient};

let client = AlternatorClient::from_conf(
    AlternatorConfig::builder()
        .endpoint_url("http://10.0.0.1:8043")
        .without_user_agent()
        .behavior_version_latest()
        .build(),
);
```

## Header stripping

By default, the AWS Rust SDK attaches a number of headers to every DynamoDB request — some are required for signed requests (`Host`, `Authorization`, `X-Amz-Date`, etc.), others are SDK metadata that Alternator doesn't use (`User-Agent` flavors, internal telemetry, retry information). For a small client-side optimization, this crate strips non-essential headers before transmission, then writes the configured final `User-Agent`. Optimized requests keep only:
- `host`
- `x-amz-target`
- `content-length`
- `accept-encoding`
- `content-encoding`
- `user-agent` unless disabled with `without_user_agent()`

For signed requests, it also keeps:
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
        .build(),
);
```
or by using `.customize().alternator_config_override(...)` with `AlternatorConfig::operation_builder()` to override it for a specific driver call.

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

or by using `.customize().alternator_config_override(...)` with `AlternatorConfig::operation_builder()` to override it for a specific driver call.

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
        AlternatorConfig::operation_builder()
            .request_compression(RequestCompression::disabled())
    )
    .send()
    .await
    .unwrap();
```

`alternator_config_override` currently applies only Alternator-specific settings: request compression, response compression, and `preserve_float32_vectors`. Use the AWS SDK's `config_override` separately for supported SDK-level per-operation overrides.

> **Note**: load-balancing, endpoint, and header stripping settings cannot be overridden per-operation. They take effect only when the client is constructed. Per-operation override is limited to request/response compression and `preserve_float32_vectors` settings.

## Vector search

Alternator's vector-search extensions (`VectorIndexes`, `VectorIndexUpdates`, `VectorSearch`, `Scores`, `FLOAT32VECTOR`) are exposed through extension traits (`CreateTableVectorExt`, `UpdateTableVectorExt`, `QueryVectorExt`, `DescribeTableVectorExt`) implemented directly on the ordinary AWS SDK fluent builders, without forking or patching `aws-sdk-dynamodb`. `FLOAT32VECTOR` response conversion applies to every successful operation performed through [`AlternatorClient`], regardless of whether a vector extension is used. The extension methods are needed only to inject Alternator-specific request fields (`VectorIndexes`, `VectorIndexUpdates`, `VectorSearch`) or to obtain non-SDK response metadata (`Scores`, parsed `VectorIndexes`).

Ordinary AWS SDK request setters (`.table_name(...)`, `.key_condition_expression(...)`, etc.) must precede the vector extension method call: the extension is the boundary after which normal generated builder configuration ends. After that point, `.send()` returns a small response-aware wrapper for `CreateTable`, `Query`, and `DescribeTable` (`UpdateTable` has no extended response and keeps returning the generated `UpdateTableOutput`).

### Creating and describing a table with vector indexes

```rust
use alternator_driver::*;
use aws_sdk_dynamodb::types::{AttributeDefinition, BillingMode, KeySchemaElement, KeyType, ScalarAttributeType};

# async fn example(client: AlternatorClient) -> Result<(), Box<dyn std::error::Error>> {
let vector_attribute = VectorAttribute::builder()
    .attribute_name("embedding")
    .dimensions(128)
    .build()?;

let index = VectorIndex::builder()
    .index_name("embedding_idx")
    .vector_attribute(vector_attribute)
    .similarity_function(SimilarityFunction::Cosine)
    .build()?;

let created = client
    .create_table()
    .table_name("Documents")
    .attribute_definitions(
        AttributeDefinition::builder()
            .attribute_name("pk")
            .attribute_type(ScalarAttributeType::S)
            .build()?,
    )
    .key_schema(
        KeySchemaElement::builder()
            .attribute_name("pk")
            .key_type(KeyType::Hash)
            .build()?,
    )
    .billing_mode(BillingMode::PayPerRequest)
    .vector_indexes(vec![index])   // <-- vector extension boundary
    .send()
    .await?;

println!("created indexes: {:?}", created.vector_indexes);

let described = client
    .describe_table()
    .table_name("Documents")
    .with_vector_indexes()
    .send()
    .await?;
for index in &described.vector_indexes {
    println!("{}: status={:?}", index.index_name, index.index_status);
}
# Ok(())
# }
```

A `VectorIndex` with no `.projection(...)` configured omits the `Projection` field entirely, so the server applies its own default; this is distinct from explicitly requesting `Projection::keys_only()`.

### Adding or deleting indexes with UpdateTable

`UpdateTable.VectorIndexUpdates` must contain exactly one update per request; an empty list or more than one update fails locally before a request is sent. Vector-index updates also cannot be combined with generated `GlobalSecondaryIndexUpdates` in the same request (and should not be combined with Alternator Streams changes, though the driver cannot always detect that combination locally; the server enforces it). Issue a separate `UpdateTable` call for each index change:

```rust
use alternator_driver::*;

# async fn example(client: AlternatorClient) -> Result<(), Box<dyn std::error::Error>> {
let new_index = VectorIndex::builder()
    .index_name("summary_idx")
    .vector_attribute(
        VectorAttribute::builder()
            .attribute_name("summary_embedding")
            .dimensions(64)
            .build()?,
    )
    .build()?;

client
    .update_table()
    .table_name("Documents")
    .vector_index_updates(vec![VectorIndexUpdate::Create(new_index)])
    .send()
    .await?;

client
    .update_table()
    .table_name("Documents")
    .vector_index_updates(vec![VectorIndexUpdate::Delete { index_name: "old_idx".to_string() }])
    .send()
    .await?;
# Ok(())
# }
```

Wait for a newly created index's `index_status` to become `IndexStatus::Active` (via `.describe_table().with_vector_indexes()`) before querying it; the driver does not wait for this automatically.

### Querying by vector similarity

```rust
use alternator_driver::*;

# async fn example(client: AlternatorClient) -> Result<(), Box<dyn std::error::Error>> {
let search = VectorSearch::new(vec![0.1, 0.2, 0.3, /* ... */])?
    .with_return_scores(ReturnScores::Similarity);

let result = client
    .query()
    .table_name("Documents")
    .index_name("embedding_idx")
    .limit(10)
    .vector_search(search)
    .send()
    .await?;

if let Some(scores) = &result.scores {
    println!("scores: {:?}", scores);
}
for item in result.as_inner().items() {
    println!("{:?}", item);
}

// Extended output types do not convert implicitly to their generated
// counterparts; use `.as_inner()`/`.into_inner()` (or `.into()`, where
// `From` is implemented) to obtain the plain generated type explicitly.
let plain_output: aws_sdk_dynamodb::operation::query::QueryOutput = result.into_inner();
# let _ = plain_output;
# Ok(())
# }
```

`VectorQueryOutput` always contains the normal `QueryOutput` plus `scores: Option<Vec<f64>>`; there is no separate overload for score/no-score queries. `client.query().send()`, without `.vector_search(...)`, is unaffected and continues to return the AWS SDK's own `QueryOutput`.

On the wire, the query vector is nested under `VectorSearch.QueryVector`, alongside `ReturnScores` when requested:

```json
{ "VectorSearch": { "QueryVector": { "FLOAT32VECTOR": [0.1, 0.2, 0.3] }, "ReturnScores": "SIMILARITY" } }
```

`VectorSearch::new` builds the compact `FLOAT32VECTOR` form shown above. To send a standard DynamoDB list query vector (`{"L": [{"N": "..."}, ...]}`) instead, use `VectorSearch::from_query_vector` with `AttributeValue::N` values:

```rust
use alternator_driver::*;
use aws_sdk_dynamodb::types::AttributeValue;

# fn example() -> Result<(), Box<dyn std::error::Error>> {
let search = VectorSearch::from_query_vector(vec![
    AttributeValue::N("0.1".to_string()),
    AttributeValue::N("0.2".to_string()),
    AttributeValue::N("0.3".to_string()),
])?;
# let _ = search;
# Ok(())
# }
```

A vector-search query is validated locally before it is ever sent, and fails with a local error (no request reaches the server) when:

- `IndexName` is missing.
- `Limit` is missing, `0`, or greater than `1000` (vector-search queries always require an explicit `Limit` in `1..=1000`).
- `ConsistentRead(true)` is set.
- `ExclusiveStartKey` is set.
- `ScanIndexForward` is set.
- The legacy `QueryFilter` is set.
- `ReturnScores::Similarity` is combined with `Select::Count`.

Schema-dependent validation, such as whether the named index actually exists or query-vector dimensionality matches the index, remains server-owned: the driver only rejects combinations it can determine are invalid without contacting the server. `Projection = INCLUDE` and `KeyConditionExpression` support (as a projected-attribute pre-filter) are server-version-dependent; consult your Alternator/Vector Store version's capabilities.

The driver never waits for an index to become ready on your behalf: after `CreateTable`/`UpdateTable`, poll `.describe_table().with_vector_indexes()` yourself until `index_status` is `IndexStatus::Active` before querying it.

### Per-request configuration with `.customize()`

The direct form shown above is equivalent to calling `.customize()` first. Use the `.customize()` form only when you also need a per-operation override, such as `alternator_config_override(...)`:

```rust
use alternator_driver::*;

# async fn example(client: AlternatorClient) -> Result<(), Box<dyn std::error::Error>> {
let search = VectorSearch::new(vec![0.1, 0.2, 0.3])?;

let result = client
    .query()
    .table_name("Documents")
    .customize()
    .vector_search(search)
    .alternator_config_override(
        AlternatorConfig::operation_builder().preserve_float32_vectors(true),
    )
    .send()
    .await?;
# let _ = result;
# Ok(())
# }
```

`preserve_float32_vectors` is set through the general `alternator_config_override(...)` mechanism (see [Per-operation override](#per-operation-override)) rather than a vector-specific setter, and the response-aware wrappers (`VectorQueryOperation`, `VectorCreateTableOperation`, `VectorDescribeTableOperation`) expose it too, so it composes with the direct form:

```rust
use alternator_driver::*;

# async fn example(client: AlternatorClient) -> Result<(), Box<dyn std::error::Error>> {
let search = VectorSearch::new(vec![0.1, 0.2, 0.3])?;

let result = client
    .query()
    .table_name("Documents")
    .vector_search(search)
    .alternator_config_override(
        AlternatorConfig::operation_builder().preserve_float32_vectors(true),
    )
    .send()
    .await?;
# let _ = result;
# Ok(())
# }
```

### Writing and reading `FLOAT32VECTOR` attributes

Write compact vectors with `Float32Vector::to_attribute_value`, which produces an ordinary `AttributeValue::B` that the driver recognizes and rewrites to `FLOAT32VECTOR` on the wire. It returns an error if any value is not finite (NaN or infinity), since Alternator serializes vector elements as JSON numbers:

```rust
use alternator_driver::*;
use aws_sdk_dynamodb::types::AttributeValue;

# async fn example(client: AlternatorClient) -> Result<(), Box<dyn std::error::Error>> {
client
    .put_item()
    .table_name("Documents")
    .item("pk", AttributeValue::S("doc-1".into()))
    .item(
        "embedding",
        Float32Vector::to_attribute_value(vec![0.1, 0.2, 0.3])?,
    )
    .send()
    .await?;
# Ok(())
# }
```

By default, reading a `FLOAT32VECTOR` attribute back gives you an ordinary `AttributeValue::L` of `AttributeValue::N` values, matching the Java driver's default and requiring no Alternator-specific types. This conversion applies to every successful operation through `AlternatorClient`, whether or not a vector extension was used to build the request:

```rust
use alternator_driver::*;
use aws_sdk_dynamodb::types::AttributeValue;

# async fn example(client: AlternatorClient) -> Result<(), Box<dyn std::error::Error>> {
let output = client
    .get_item()
    .table_name("Documents")
    .key("pk", AttributeValue::S("doc-1".into()))
    .send()
    .await?;

// `embedding` is AttributeValue::L([N("0.1"), N("0.2"), N("0.3")])
let embedding = output.item().unwrap().get("embedding").unwrap();
# let _ = embedding;
# Ok(())
# }
```

Ordinary DynamoDB `L` lists of `N` values are never inferred to be vectors: a caller who inserts a plain decimal list gets ordinary DynamoDB list behavior back, never a `FLOAT32VECTOR` conversion.

### Preserving compact storage for read-modify-write

If your application understands optimized vectors and needs a lossless read-modify-write round trip, enable `preserve_float32_vectors`:

```rust
use alternator_driver::{AlternatorClient, AlternatorConfig};

let client = AlternatorClient::from_conf(
    AlternatorConfig::builder()
        .endpoint_url("http://10.0.0.1:8043")
        .preserve_float32_vectors(true)
        .behavior_version_latest()
        .build(),
);
```

With this enabled, `FLOAT32VECTOR` responses are decoded into a marker `AttributeValue::B` instead of `L`/`N`. Read it with the `Float32VectorExt` extension trait:

```rust
use alternator_driver::*;
use aws_sdk_dynamodb::types::AttributeValue;

# fn example(embedding: &AttributeValue) -> Result<(), Box<dyn std::error::Error>> {
if embedding.is_float32_vector() {
    let values: Vec<f32> = embedding.float32_vector()?;
    println!("{:?}", values);
}
# Ok(())
# }
```

An item fetched this way can be written back unchanged (e.g. via `PutItem`, `UpdateItem`, or `BatchWriteItem`) and will retain compact `FLOAT32VECTOR` storage, because the marker binary round-trips through the same rewrite the driver applies to values built with `Float32Vector::to_attribute_value`. Without `preserve_float32_vectors`, the default `L`/`N` conversion is not reversible into compact storage: writing back a converted list stores an ordinary DynamoDB list, not a vector.

### Index and vector limits

`VectorAttribute::builder().dimensions(...)` accepts `1..=16000`; values above `16000` are rejected locally when the index is built, before any request is sent.

### Real Vector Store end-to-end tests

`tests/vector_store_e2e.rs` exercises vector search against a real Vector Store instance provisioned through CCM. It is gated behind `--cfg ccm_tests` and is not part of the default `make test`/CI run: it requires Docker (Vector Store itself runs in a container with `--network host`) plus the environment variables `SCYLLA_VECTOR_STORE_IMAGE`, `SCYLLA_VECTOR_STORE_PORT`, `SCYLLA_VECTOR_STORE_SCYLLA_VERSION` (a CCM-resolvable Scylla version with native Vector/Scores support, e.g. `unstable/master:latest`; standard `release:*` versions do not have it yet), and `SCYLLA_VECTOR_STORE_SCYLLA_CONFIG`. Run it with:

```bash
make vector-store-e2e
```

See the module doc comment at the top of `tests/vector_store_e2e.rs` for full setup details.

