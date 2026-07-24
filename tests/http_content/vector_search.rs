use crate::http_content::driver_utils::*;
use crate::http_content::http_test::*;
use crate::http_content::proxy::*;

use http_body_util::Full;
use hyper::body::{Bytes, Incoming};
use hyper::client::conn::http1::SendRequest;
use hyper::{Request, Response, StatusCode};

use aws_sdk_dynamodb::error::ProvideErrorMetadata;
use aws_sdk_dynamodb::types::{
    AttributeDefinition, BillingMode, KeySchemaElement, KeyType, ScalarAttributeType,
};
use std::sync::Arc;
use test_context::test_context;
use tokio::sync::Mutex as TokioMutex;
use uuid::Uuid;

use alternator_driver::*;

async fn cleanup_calls(resources: Vec<String>, alternator_address: &str) {
    let client = aws_sdk_dynamodb::Client::from_conf(
        aws_sdk_dynamodb::Config::builder()
            .endpoint_url(format!("http://{}", alternator_address))
            .region(aws_sdk_dynamodb::config::Region::new("eu-central-1"))
            .behavior_version(aws_sdk_dynamodb::config::BehaviorVersion::latest())
            .credentials_provider(
                aws_sdk_dynamodb::config::Credentials::for_tests_with_session_token(),
            )
            .build(),
    );

    for resource in resources {
        delete_table_cleanup(&client, &resource).await;
    }
}

struct VectorSearchConfig;

impl HttpTestConfig for VectorSearchConfig {
    async fn on_request(
        request: Request<Incoming>,
        sender: Arc<TokioMutex<SendRequest<Full<Bytes>>>>,
    ) -> Response<Full<Bytes>> {
        let (parts, body) = collect_request(request).await;

        let is_create_table = parts
            .headers
            .get("x-amz-target")
            .map(|v| v.as_bytes() == b"DynamoDB_20120810.CreateTable")
            .unwrap_or(false);

        if is_create_table {
            let json: serde_json::Value = serde_json::from_slice(&body).unwrap();

            assert!(
                json.get("VectorIndexes").is_some(),
                "VectorIndexes should be present in CreateTable request"
            );

            let indexes = json["VectorIndexes"].as_array().unwrap();
            assert_eq!(indexes.len(), 1, "Should have 1 vector index");
            assert_eq!(indexes[0]["IndexName"], "vec_idx");
            assert_eq!(
                indexes[0]["VectorAttribute"]["AttributeName"],
                "vector_attr"
            );
            // VectorAttribute has no VectorType field in the API
            assert_eq!(indexes[0]["VectorAttribute"]["Dimensions"], 128);

            // Strip VectorIndexes before forwarding to Alternator
            let mut stripped = json.clone();
            stripped.as_object_mut().unwrap().remove("VectorIndexes");
            let new_body = serde_json::to_vec(&stripped).unwrap();

            let mut mod_parts = parts.clone();
            mod_parts.headers.insert(
                "content-length",
                new_body.len().to_string().try_into().unwrap(),
            );

            let (parts, body) =
                collect_received_response(mod_parts, Bytes::from(new_body), sender).await;
            build_response(parts, body)
        } else {
            let (parts, body) = collect_received_response(parts, body, sender).await;
            build_response(parts, body)
        }
    }

    async fn cleanup(resources: Vec<String>, alternator_address: &str) {
        cleanup_calls(resources, alternator_address).await;
    }
}

#[test_context(HttpTestContext<VectorSearchConfig>)]
#[tokio::test]
pub async fn test_create_table_with_vector_indexes(ctx: &mut HttpTestContext<VectorSearchConfig>) {
    let client = AlternatorClient::from_conf(
        AlternatorConfig::builder()
            .endpoint_url(format!("http://{}", ctx.get_proxy_address()))
            .seed_hosts(Vec::<String>::new())
            .behavior_version(aws_sdk_dynamodb::config::BehaviorVersion::latest())
            .allow_no_auth()
            .build(),
    );

    let table_name = format!("table_vec_{}", Uuid::new_v4());
    ctx.register_resource(table_name.clone());

    let va = VectorAttribute::builder()
        .attribute_name("vector_attr")
        .dimensions(128)
        .build()
        .unwrap();

    let vi = VectorIndex::builder()
        .index_name("vec_idx")
        .vector_attribute(va)
        .build()
        .unwrap();

    let created = client
        .create_table()
        .table_name(&table_name)
        .attribute_definitions(
            AttributeDefinition::builder()
                .attribute_name("pk")
                .attribute_type(ScalarAttributeType::S)
                .build()
                .unwrap(),
        )
        .key_schema(
            KeySchemaElement::builder()
                .attribute_name("pk")
                .key_type(KeyType::Hash)
                .build()
                .unwrap(),
        )
        .billing_mode(BillingMode::PayPerRequest)
        .vector_indexes(vec![vi])
        .send()
        .await
        .unwrap();

    // as_inner()/into_inner() convert to the generated CreateTableOutput.
    assert!(created.as_inner().table_description().is_some());

    // The forwarding proxy strips VectorIndexes, so only ordinary table
    // metadata is available from this real-Scylla request.
    let desc = client
        .describe_table()
        .table_name(&table_name)
        .send()
        .await
        .unwrap();

    assert_eq!(desc.table().unwrap().table_name().unwrap(), &table_name);
    let _: aws_sdk_dynamodb::operation::create_table::CreateTableOutput = created.into_inner();

    // Cleanup is done by teardown
}

/// Config used by the tests below: instead of forwarding to the real
/// backend, it returns a caller-supplied synthetic 200 JSON response and
/// captures the last request body, both stored per-test via
/// [HttpTestContext::set_on_request]. This lets us assert on HTTP JSON
/// content and client-side response decoding without depending on the
/// backend supporting Alternator vector search.
struct SyntheticResponseConfig;

impl HttpTestConfig for SyntheticResponseConfig {
    async fn on_request(
        request: Request<Incoming>,
        _sender: Arc<TokioMutex<SendRequest<Full<Bytes>>>>,
    ) -> Response<Full<Bytes>> {
        let (_parts, _body) = collect_request(request).await;
        Response::builder()
            .status(200)
            .header("content-type", "application/x-amz-json-1.0")
            .body(Full::new(Bytes::from(b"{}".to_vec())))
            .unwrap()
    }

    async fn cleanup(_resources: Vec<String>, _alternator_address: &str) {}
}

/// Sets up `ctx`'s `on_request` hook to record every request body into
/// `last_body` and reply with `response_body`, for the current test only.
async fn capture_and_respond(
    ctx: &HttpTestContext<SyntheticResponseConfig>,
    status: StatusCode,
    response_body: serde_json::Value,
    last_body: Arc<TokioMutex<Option<serde_json::Value>>>,
) {
    let response_bytes = serde_json::to_vec(&response_body).unwrap();
    ctx.set_on_request(move |request, _sender| {
        let last_body = last_body.clone();
        let response_bytes = response_bytes.clone();
        async move {
            let (_parts, body) = collect_request(request).await;
            if let Ok(json) = serde_json::from_slice::<serde_json::Value>(&body) {
                *last_body.lock().await = Some(json);
            }
            Response::builder()
                .status(status)
                .header("content-type", "application/x-amz-json-1.0")
                .body(Full::new(Bytes::from(response_bytes)))
                .unwrap()
        }
    })
    .await;
}

fn synthetic_client(ctx: &HttpTestContext<SyntheticResponseConfig>) -> AlternatorClient {
    AlternatorClient::from_conf(
        AlternatorConfig::builder()
            .endpoint_url(format!("http://{}", ctx.get_proxy_address()))
            .seed_hosts(Vec::<String>::new())
            .behavior_version(aws_sdk_dynamodb::config::BehaviorVersion::latest())
            .allow_no_auth()
            .build(),
    )
}

#[test_context(HttpTestContext<SyntheticResponseConfig>)]
#[tokio::test]
pub async fn test_update_table_sends_vector_index_updates(
    ctx: &mut HttpTestContext<SyntheticResponseConfig>,
) {
    let last_body = Arc::new(TokioMutex::new(None));
    capture_and_respond(
        ctx,
        StatusCode::OK,
        serde_json::json!({ "TableDescription": {} }),
        last_body.clone(),
    )
    .await;
    let client = synthetic_client(ctx);

    let va = VectorAttribute::builder()
        .attribute_name("vector_attr")
        .dimensions(64)
        .build()
        .unwrap();
    let vi = VectorIndex::builder()
        .index_name("new_idx")
        .vector_attribute(va)
        .build()
        .unwrap();

    client
        .update_table()
        .table_name("some_table")
        .vector_index_updates(vec![VectorIndexUpdate::Create(vi)])
        .send()
        .await
        .unwrap();

    let body = last_body.lock().await.take().unwrap();
    let updates = body["VectorIndexUpdates"].as_array().unwrap();
    assert_eq!(updates.len(), 1);
    assert_eq!(updates[0]["Create"]["IndexName"], "new_idx");
}

#[test_context(HttpTestContext<SyntheticResponseConfig>)]
#[tokio::test]
pub async fn test_query_sends_vector_search_and_exposes_scores(
    ctx: &mut HttpTestContext<SyntheticResponseConfig>,
) {
    let last_body = Arc::new(TokioMutex::new(None));
    capture_and_respond(
        ctx,
        StatusCode::OK,
        serde_json::json!({
            "Items": [{}, {}],
            "Count": 2,
            "ScannedCount": 2,
            "Scores": [0.95, 0.42]
        }),
        last_body.clone(),
    )
    .await;
    let client = synthetic_client(ctx);

    let search = VectorSearch::new(vec![1.0, 2.0, 3.0])
        .unwrap()
        .with_return_scores(ReturnScores::Similarity);

    let result = client
        .query()
        .table_name("some_table")
        .index_name("embedding_idx")
        .limit(10)
        .expression_attribute_names("#pk", "pk")
        .key_condition_expression("#pk = :pk")
        .vector_search(search)
        .send()
        .await
        .unwrap();

    let body = last_body.lock().await.take().unwrap();
    assert_eq!(
        body["VectorSearch"]["QueryVector"]["FLOAT32VECTOR"],
        serde_json::json!([1.0, 2.0, 3.0])
    );
    assert_eq!(body["VectorSearch"]["ReturnScores"], "SIMILARITY");
    assert_eq!(result.scores, Some(vec![0.95, 0.42]));

    // as_inner()/into_inner() convert to the generated QueryOutput.
    assert_eq!(result.as_inner().count(), 2);
    let plain: aws_sdk_dynamodb::operation::query::QueryOutput = result.into_inner();
    assert_eq!(plain.count(), 2);
}

#[test_context(HttpTestContext<SyntheticResponseConfig>)]
#[tokio::test]
pub async fn test_vector_response_wrappers_parse_metadata_and_absent_fields(
    ctx: &mut HttpTestContext<SyntheticResponseConfig>,
) {
    let last_body = Arc::new(TokioMutex::new(None));
    capture_and_respond(
        ctx,
        StatusCode::OK,
        serde_json::json!({
            "TableDescription": {
                "VectorIndexes": [{
                    "IndexName": "vec_idx",
                    "VectorAttribute": { "AttributeName": "embedding", "Dimensions": 3 },
                    "Projection": { "ProjectionType": "INCLUDE", "NonKeyAttributes": ["title"] },
                    "SimilarityFunction": "COSINE",
                    "IndexStatus": "ACTIVE",
                    "Backfilling": false
                }]
            }
        }),
        last_body.clone(),
    )
    .await;
    let client = synthetic_client(ctx);
    let index = VectorIndex::builder()
        .index_name("vec_idx")
        .vector_attribute(
            VectorAttribute::builder()
                .attribute_name("embedding")
                .dimensions(3)
                .build()
                .unwrap(),
        )
        .build()
        .unwrap();

    let created = client
        .create_table()
        .table_name("some_table")
        .vector_indexes(vec![index])
        .send()
        .await
        .unwrap();
    assert_eq!(created.vector_indexes.len(), 1);
    let parsed = &created.vector_indexes[0];
    assert_eq!(parsed.vector_attribute.attribute_name, "embedding");
    assert_eq!(parsed.vector_attribute.dimensions, 3);
    assert_eq!(parsed.similarity_function, Some(SimilarityFunction::Cosine));
    assert_eq!(parsed.index_status, Some(IndexStatus::Active));
    assert_eq!(parsed.backfilling, Some(false));

    capture_and_respond(
        ctx,
        StatusCode::OK,
        serde_json::json!({ "Table": {} }),
        last_body,
    )
    .await;
    let described = client
        .describe_table()
        .table_name("some_table")
        .with_vector_indexes()
        .send()
        .await
        .unwrap();
    assert!(described.vector_indexes.is_empty());
}

#[test_context(HttpTestContext<SyntheticResponseConfig>)]
#[tokio::test]
pub async fn test_vector_response_wrappers_preserve_service_errors(
    ctx: &mut HttpTestContext<SyntheticResponseConfig>,
) {
    let last_body = Arc::new(TokioMutex::new(None));
    let error = serde_json::json!({
        "__type": "com.amazonaws.dynamodb.v20120810#ValidationException",
        "message": "Vector index is not ready",
        "Scores": [0.95]
    });
    capture_and_respond(
        ctx,
        StatusCode::BAD_REQUEST,
        error.clone(),
        last_body.clone(),
    )
    .await;
    let client = synthetic_client(ctx);

    let query_error = client
        .query()
        .table_name("some_table")
        .index_name("embedding_idx")
        .limit(10)
        .vector_search(VectorSearch::new([1.0]).unwrap())
        .send()
        .await
        .unwrap_err();
    let service_error = query_error.as_service_error().unwrap();
    assert_eq!(service_error.code(), Some("ValidationException"));
    assert_eq!(service_error.message(), Some("Vector index is not ready"));

    capture_and_respond(ctx, StatusCode::INTERNAL_SERVER_ERROR, error, last_body).await;
    let index = VectorIndex::builder()
        .index_name("vec_idx")
        .vector_attribute(
            VectorAttribute::builder()
                .attribute_name("embedding")
                .dimensions(3)
                .build()
                .unwrap(),
        )
        .build()
        .unwrap();
    let create_error = client
        .create_table()
        .table_name("some_table")
        .vector_indexes(vec![index])
        .send()
        .await
        .unwrap_err();
    let service_error = create_error.as_service_error().unwrap();
    assert_eq!(service_error.code(), Some("ValidationException"));
    assert_eq!(service_error.message(), Some("Vector index is not ready"));
}

#[test_context(HttpTestContext<SyntheticResponseConfig>)]
#[tokio::test]
pub async fn test_query_customize_form_coexists_with_config_override(
    ctx: &mut HttpTestContext<SyntheticResponseConfig>,
) {
    let last_body = Arc::new(TokioMutex::new(None));
    capture_and_respond(
        ctx,
        StatusCode::OK,
        serde_json::json!({
            "Items": [{}, {}],
            "Count": 2,
            "ScannedCount": 2,
            "Scores": [0.95, 0.42]
        }),
        last_body.clone(),
    )
    .await;
    let client = synthetic_client(ctx);

    let search = VectorSearch::new(vec![1.0, 2.0, 3.0])
        .unwrap()
        .with_return_scores(ReturnScores::Similarity);

    // The `.customize()` form is equivalent to the direct form, and can be
    // combined with a per-request `alternator_config_override(...)`.
    let result = client
        .query()
        .table_name("some_table")
        .index_name("embedding_idx")
        .limit(10)
        .customize()
        .vector_search(search)
        .alternator_config_override(
            AlternatorConfig::operation_builder().preserve_float32_vectors(true),
        )
        .send()
        .await
        .unwrap();

    let body = last_body.lock().await.take().unwrap();
    assert_eq!(
        body["VectorSearch"]["QueryVector"]["FLOAT32VECTOR"],
        serde_json::json!([1.0, 2.0, 3.0])
    );
    assert_eq!(result.scores, Some(vec![0.95, 0.42]));
}

#[test_context(HttpTestContext<SyntheticResponseConfig>)]
#[tokio::test]
pub async fn test_ordinary_query_remains_aws_sdk_compatible(
    ctx: &mut HttpTestContext<SyntheticResponseConfig>,
) {
    let last_body = Arc::new(TokioMutex::new(None));
    capture_and_respond(
        ctx,
        StatusCode::OK,
        serde_json::json!({ "Items": [], "Count": 0, "ScannedCount": 0 }),
        last_body,
    )
    .await;
    let client = synthetic_client(ctx);

    // `query()` still returns the AWS SDK's own `QueryFluentBuilder`, not a
    // vector builder, and its output is the generated `QueryOutput`.
    let output: aws_sdk_dynamodb::operation::query::QueryOutput = client
        .query()
        .table_name("some_table")
        .send()
        .await
        .unwrap();
    assert_eq!(output.count(), 0);
}

#[test_context(HttpTestContext<SyntheticResponseConfig>)]
#[tokio::test]
pub async fn test_preserve_float32_vectors_operation_override_takes_precedence(
    ctx: &mut HttpTestContext<SyntheticResponseConfig>,
) {
    let last_body = Arc::new(TokioMutex::new(None));
    capture_and_respond(
        ctx,
        StatusCode::OK,
        serde_json::json!({
            "Item": {
                "pk": { "S": "row1" },
                "embedding": { "FLOAT32VECTOR": [1.0, 2.0, 3.0] }
            }
        }),
        last_body,
    )
    .await;

    // Client default is `false` (ordinary L/N conversion); a per-operation
    // override of `true` should take precedence and return a marker binary.
    let client = AlternatorClient::from_conf(
        AlternatorConfig::builder()
            .endpoint_url(format!("http://{}", ctx.get_proxy_address()))
            .seed_hosts(Vec::<String>::new())
            .behavior_version(aws_sdk_dynamodb::config::BehaviorVersion::latest())
            .allow_no_auth()
            .preserve_float32_vectors(false)
            .build(),
    );

    let output = client
        .get_item()
        .table_name("some_table")
        .key(
            "pk",
            aws_sdk_dynamodb::types::AttributeValue::S("row1".into()),
        )
        .customize()
        .alternator_config_override(
            AlternatorConfig::operation_builder().preserve_float32_vectors(true),
        )
        .send()
        .await
        .unwrap();

    let item = output.item().unwrap();
    let embedding = item.get("embedding").unwrap();
    assert!(embedding.is_float32_vector());
    assert_eq!(embedding.float32_vector().unwrap(), vec![1.0, 2.0, 3.0]);
}

#[test_context(HttpTestContext<SyntheticResponseConfig>)]
#[tokio::test]
pub async fn test_preserve_float32_vectors_operation_override_false_beats_client_true(
    ctx: &mut HttpTestContext<SyntheticResponseConfig>,
) {
    let last_body = Arc::new(TokioMutex::new(None));
    capture_and_respond(
        ctx,
        StatusCode::OK,
        serde_json::json!({
            "Item": {
                "pk": { "S": "row1" },
                "embedding": { "FLOAT32VECTOR": [1.0, 2.0, 3.0] }
            }
        }),
        last_body,
    )
    .await;

    // Client default is `true`; a per-operation override of `false` should
    // take precedence and return an ordinary L/N representation.
    let client = AlternatorClient::from_conf(
        AlternatorConfig::builder()
            .endpoint_url(format!("http://{}", ctx.get_proxy_address()))
            .seed_hosts(Vec::<String>::new())
            .behavior_version(aws_sdk_dynamodb::config::BehaviorVersion::latest())
            .allow_no_auth()
            .preserve_float32_vectors(true)
            .build(),
    );

    let output = client
        .get_item()
        .table_name("some_table")
        .key(
            "pk",
            aws_sdk_dynamodb::types::AttributeValue::S("row1".into()),
        )
        .customize()
        .alternator_config_override(
            AlternatorConfig::operation_builder().preserve_float32_vectors(false),
        )
        .send()
        .await
        .unwrap();

    let item = output.item().unwrap();
    let embedding = item.get("embedding").unwrap();
    assert!(matches!(
        embedding,
        aws_sdk_dynamodb::types::AttributeValue::L(_)
    ));
}

#[test_context(HttpTestContext<SyntheticResponseConfig>)]
#[tokio::test]
pub async fn test_put_item_rewrites_marker_binary_to_float32vector(
    ctx: &mut HttpTestContext<SyntheticResponseConfig>,
) {
    let last_body = Arc::new(TokioMutex::new(None));
    capture_and_respond(
        ctx,
        StatusCode::OK,
        serde_json::json!({}),
        last_body.clone(),
    )
    .await;
    let client = synthetic_client(ctx);

    let av = Float32Vector::to_attribute_value(vec![1.0, 2.0, 3.0]).unwrap();

    client
        .put_item()
        .table_name("some_table")
        .item(
            "pk",
            aws_sdk_dynamodb::types::AttributeValue::S("row1".into()),
        )
        .item("embedding", av)
        .send()
        .await
        .unwrap();

    let body = last_body.lock().await.take().unwrap();
    assert_eq!(
        body["Item"]["embedding"]["FLOAT32VECTOR"],
        serde_json::json!([1.0, 2.0, 3.0])
    );
}

#[test_context(HttpTestContext<SyntheticResponseConfig>)]
#[tokio::test]
pub async fn test_get_item_response_converts_float32vector_to_l_n_by_default(
    ctx: &mut HttpTestContext<SyntheticResponseConfig>,
) {
    let last_body = Arc::new(TokioMutex::new(None));
    capture_and_respond(
        ctx,
        StatusCode::OK,
        serde_json::json!({
            "Item": {
                "pk": { "S": "row1" },
                "embedding": { "FLOAT32VECTOR": [1.0, 2.0, 3.0] }
            }
        }),
        last_body,
    )
    .await;
    let client = synthetic_client(ctx);

    let output = client
        .get_item()
        .table_name("some_table")
        .key(
            "pk",
            aws_sdk_dynamodb::types::AttributeValue::S("row1".into()),
        )
        .send()
        .await
        .unwrap();

    let item = output.item().unwrap();
    let embedding = item.get("embedding").unwrap();
    assert!(matches!(
        embedding,
        aws_sdk_dynamodb::types::AttributeValue::L(_)
    ));
    if let aws_sdk_dynamodb::types::AttributeValue::L(list) = embedding {
        assert_eq!(list.len(), 3);
        assert!(matches!(
            &list[0],
            aws_sdk_dynamodb::types::AttributeValue::N(_)
        ));
    }
}

#[test_context(HttpTestContext<SyntheticResponseConfig>)]
#[tokio::test]
pub async fn test_get_item_response_preserves_marker_binary_when_enabled(
    ctx: &mut HttpTestContext<SyntheticResponseConfig>,
) {
    let last_body = Arc::new(TokioMutex::new(None));
    capture_and_respond(
        ctx,
        StatusCode::OK,
        serde_json::json!({
            "Item": {
                "pk": { "S": "row1" },
                "embedding": { "FLOAT32VECTOR": [1.0, 2.0, 3.0] }
            }
        }),
        last_body,
    )
    .await;

    let client = AlternatorClient::from_conf(
        AlternatorConfig::builder()
            .endpoint_url(format!("http://{}", ctx.get_proxy_address()))
            .seed_hosts(Vec::<String>::new())
            .behavior_version(aws_sdk_dynamodb::config::BehaviorVersion::latest())
            .allow_no_auth()
            .preserve_float32_vectors(true)
            .build(),
    );

    let output = client
        .get_item()
        .table_name("some_table")
        .key(
            "pk",
            aws_sdk_dynamodb::types::AttributeValue::S("row1".into()),
        )
        .send()
        .await
        .unwrap();

    let item = output.item().unwrap();
    let embedding = item.get("embedding").unwrap();
    assert!(embedding.is_float32_vector());
    assert_eq!(embedding.float32_vector().unwrap(), vec![1.0, 2.0, 3.0]);
}
