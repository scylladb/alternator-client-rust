use crate::vector::{
    CreateTableWithVectorIndexes, DescribeTableWithVectorIndexes, VectorIndex, VectorIndexUpdate,
    VectorQueryOutput, VectorSearch,
};
use crate::{AlternatorCustomizableOperation, AlternatorOperationBuilder};

use aws_sdk_dynamodb::client::customize::CustomizableOperation;
use aws_sdk_dynamodb::operation::create_table::CreateTableError;
use aws_sdk_dynamodb::operation::create_table::builders::CreateTableFluentBuilder;
use aws_sdk_dynamodb::operation::describe_table::DescribeTableError;
use aws_sdk_dynamodb::operation::describe_table::builders::DescribeTableFluentBuilder;
use aws_sdk_dynamodb::operation::query::QueryError;
use aws_sdk_dynamodb::operation::query::builders::QueryFluentBuilder;
use aws_sdk_dynamodb::operation::update_table::UpdateTableError;
use aws_sdk_dynamodb::operation::update_table::builders::UpdateTableFluentBuilder;
use aws_smithy_runtime_api::box_error::BoxError;
use aws_smithy_runtime_api::client::interceptors::Intercept;
use aws_smithy_runtime_api::client::interceptors::context::BeforeSerializationInterceptorContextMut;
use aws_smithy_runtime_api::client::orchestrator::HttpResponse;
use aws_smithy_runtime_api::client::result::SdkError;
use aws_smithy_runtime_api::client::runtime_components::RuntimeComponents;
use aws_smithy_types::config_bag::{ConfigBag, Storable, StoreReplace};
use std::sync::{Arc, Mutex};

/// State carried through [ConfigBag] describing vector-search request
/// extras that [crate::AlternatorInterceptor] should inject into the
/// serialized JSON body immediately before compression.
///
/// This is populated by [VectorRequestStoreInterceptor] in
/// `modify_before_serialization` and is retry-safe: it never inspects or
/// mutates the HTTP body itself, only carries caller intent.
#[derive(Debug, Clone, Default)]
pub(crate) struct VectorRequestStore {
    pub(crate) vector_indexes: Option<Vec<VectorIndex>>,
    pub(crate) vector_index_updates: Option<Vec<VectorIndexUpdate>>,
    pub(crate) vector_search: Option<VectorSearch>,
}

impl Storable for VectorRequestStore {
    type Storer = StoreReplace<Self>;
}

/// Per-operation interceptor that stashes vector-search request state into
/// [ConfigBag], to be picked up later by [crate::AlternatorInterceptor]
/// before compression.
#[derive(Debug, Clone)]
pub(crate) struct VectorRequestStoreInterceptor {
    store: VectorRequestStore,
}

impl VectorRequestStoreInterceptor {
    pub(crate) fn for_vector_indexes(vector_indexes: Vec<VectorIndex>) -> Self {
        Self {
            store: VectorRequestStore {
                vector_indexes: Some(vector_indexes),
                ..Default::default()
            },
        }
    }

    pub(crate) fn for_vector_index_updates(updates: Vec<VectorIndexUpdate>) -> Self {
        Self {
            store: VectorRequestStore {
                vector_index_updates: Some(updates),
                ..Default::default()
            },
        }
    }

    pub(crate) fn for_vector_search(search: VectorSearch) -> Self {
        Self {
            store: VectorRequestStore {
                vector_search: Some(search),
                ..Default::default()
            },
        }
    }
}

impl Intercept for VectorRequestStoreInterceptor {
    fn name(&self) -> &'static str {
        "VectorRequestStoreInterceptor"
    }

    fn modify_before_serialization(
        &self,
        _: &mut BeforeSerializationInterceptorContextMut<'_>,
        _: &RuntimeComponents,
        cfg: &mut ConfigBag,
    ) -> Result<(), BoxError> {
        cfg.interceptor_state().store_put(self.store.clone());

        Ok(())
    }
}

/// Per-operation vector-search response data extracted by
/// [crate::AlternatorInterceptor] from the raw response JSON, before
/// generated SDK deserialization.
///
/// Never client-global: each request creates its own holder in [ConfigBag]
/// state, so concurrent vector requests cannot observe each other's scores
/// or index metadata. If a server response omits an optional field, the
/// corresponding holder field remains `None`, not stale state from a prior
/// request.
#[derive(Debug, Clone, Default)]
pub(crate) struct VectorResponseHolder {
    pub(crate) scores: Option<Vec<f64>>,
    pub(crate) vector_indexes: Option<Vec<VectorIndex>>,
}

/// [ConfigBag] wrapper carrying the shared, per-operation
/// [VectorResponseHolder] that [crate::AlternatorInterceptor] fills in
/// during `modify_before_deserialization`.
#[derive(Debug, Clone)]
pub(crate) struct VectorResponseStore(pub(crate) Arc<Mutex<VectorResponseHolder>>);

impl Storable for VectorResponseStore {
    type Storer = StoreReplace<Self>;
}

/// Per-operation interceptor that attaches a fresh [VectorResponseHolder]
/// to [ConfigBag], to be filled in later by [crate::AlternatorInterceptor]
/// after response decompression and before generated SDK deserialization.
#[derive(Debug, Clone, Default)]
pub(crate) struct VectorResponseHolderInterceptor {
    holder: Arc<Mutex<VectorResponseHolder>>,
}

impl VectorResponseHolderInterceptor {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    pub(crate) fn holder(&self) -> Arc<Mutex<VectorResponseHolder>> {
        self.holder.clone()
    }
}

impl Intercept for VectorResponseHolderInterceptor {
    fn name(&self) -> &'static str {
        "VectorResponseHolderInterceptor"
    }

    fn modify_before_serialization(
        &self,
        _: &mut BeforeSerializationInterceptorContextMut<'_>,
        _: &RuntimeComponents,
        cfg: &mut ConfigBag,
    ) -> Result<(), BoxError> {
        cfg.interceptor_state()
            .store_put(VectorResponseStore(self.holder.clone()));

        Ok(())
    }
}

/// Extension trait that adds `VectorIndexes` support to `CreateTable`.
///
/// Implemented for both the generated
/// [`CreateTableFluentBuilder`] and its
/// [`CustomizableOperation`](aws_sdk_dynamodb::client::customize::CustomizableOperation),
/// so misuse on other operations is a compile error rather than a runtime
/// one. The fluent-builder form is the preferred direct syntax:
///
/// ```no_run
/// # tokio::runtime::Runtime::new().unwrap().block_on(async {
/// use alternator_driver::{AlternatorClient, AlternatorConfig, CreateTableVectorExt, VectorAttribute, VectorIndex};
///
/// let client = AlternatorClient::from_conf(
///     AlternatorConfig::builder().behavior_version_latest().build(),
/// );
///
/// let index = VectorIndex::builder()
///     .index_name("vec_idx")
///     .vector_attribute(
///         VectorAttribute::builder()
///             .attribute_name("embedding")
///             .dimensions(128)
///             .build()
///             .unwrap(),
///     )
///     .build()
///     .unwrap();
///
/// let created = client
///     .create_table()
///     .table_name("Documents")
///     .vector_indexes(vec![index])
///     .send()
///     .await
///     .unwrap();
/// println!("{:?}", created.vector_indexes);
/// # });
/// ```
///
/// The `.customize()` form remains available to combine with
/// `.alternator_config_override(...)`:
///
/// ```compile_fail
/// # tokio::runtime::Runtime::new().unwrap().block_on(async {
/// use alternator_driver::{AlternatorClient, AlternatorConfig, UpdateTableVectorExt, VectorIndexUpdate};
///
/// let client = AlternatorClient::from_conf(
///     AlternatorConfig::builder().behavior_version_latest().build(),
/// );
///
/// // `vector_index_updates` is only defined for UpdateTable, not CreateTable.
/// let _ = client
///     .create_table()
///     .table_name("t")
///     .vector_index_updates(Vec::<VectorIndexUpdate>::new());
/// # });
/// ```
pub trait CreateTableVectorExt {
    /// The type returned by [`vector_indexes`](Self::vector_indexes).
    type Output;

    fn vector_indexes(self, indexes: Vec<VectorIndex>) -> Self::Output;
}

impl CreateTableVectorExt for CreateTableFluentBuilder {
    type Output = VectorCreateTableOperation;

    fn vector_indexes(self, indexes: Vec<VectorIndex>) -> Self::Output {
        self.customize().vector_indexes(indexes)
    }
}

impl CreateTableVectorExt
    for CustomizableOperation<
        aws_sdk_dynamodb::operation::create_table::CreateTableOutput,
        CreateTableError,
        CreateTableFluentBuilder,
    >
{
    type Output = VectorCreateTableOperation;

    fn vector_indexes(self, indexes: Vec<VectorIndex>) -> Self::Output {
        let response_interceptor = VectorResponseHolderInterceptor::new();
        let holder = response_interceptor.holder();
        let inner = self
            .interceptor(response_interceptor)
            .interceptor(VectorRequestStoreInterceptor::for_vector_indexes(indexes));
        VectorCreateTableOperation { inner, holder }
    }
}

/// Extension trait that adds `VectorIndexUpdates` support to `UpdateTable`.
///
/// Implemented for both the generated [`UpdateTableFluentBuilder`] and its
/// `CustomizableOperation`. `UpdateTable` has no extended response output,
/// so this continues to return the generated customizable operation
/// unchanged, and `.send()` returns the generated `UpdateTableOutput`.
pub trait UpdateTableVectorExt {
    type Output;

    fn vector_index_updates(self, updates: Vec<VectorIndexUpdate>) -> Self::Output;
}

impl UpdateTableVectorExt for UpdateTableFluentBuilder {
    type Output = CustomizableOperation<
        aws_sdk_dynamodb::operation::update_table::UpdateTableOutput,
        UpdateTableError,
        UpdateTableFluentBuilder,
    >;

    fn vector_index_updates(self, updates: Vec<VectorIndexUpdate>) -> Self::Output {
        self.customize().vector_index_updates(updates)
    }
}

impl<E, B> UpdateTableVectorExt
    for CustomizableOperation<aws_sdk_dynamodb::operation::update_table::UpdateTableOutput, E, B>
{
    type Output = Self;

    fn vector_index_updates(self, updates: Vec<VectorIndexUpdate>) -> Self::Output {
        self.interceptor(VectorRequestStoreInterceptor::for_vector_index_updates(
            updates,
        ))
    }
}

/// Extension trait that adds `VectorSearch` support to `Query`.
///
/// Implemented for both the generated [`QueryFluentBuilder`] and its
/// `CustomizableOperation`. `.send()` returns [`VectorQueryOutput`], which
/// wraps the generated `QueryOutput` and adds similarity scores.
pub trait QueryVectorExt {
    type Output;

    fn vector_search(self, search: VectorSearch) -> Self::Output;
}

impl QueryVectorExt for QueryFluentBuilder {
    type Output = VectorQueryOperation;

    fn vector_search(self, search: VectorSearch) -> Self::Output {
        self.customize().vector_search(search)
    }
}

impl QueryVectorExt
    for CustomizableOperation<
        aws_sdk_dynamodb::operation::query::QueryOutput,
        QueryError,
        QueryFluentBuilder,
    >
{
    type Output = VectorQueryOperation;

    fn vector_search(self, search: VectorSearch) -> Self::Output {
        let response_interceptor = VectorResponseHolderInterceptor::new();
        let holder = response_interceptor.holder();
        let inner = self
            .interceptor(response_interceptor)
            .interceptor(VectorRequestStoreInterceptor::for_vector_search(search));
        VectorQueryOperation { inner, holder }
    }
}

/// Extension trait that adds vector-index response metadata to
/// `DescribeTable`. `DescribeTable` has no vector-only request field, so
/// this trait only affects the response.
pub trait DescribeTableVectorExt {
    type Output;

    /// Requests parsed `Table.VectorIndexes` metadata in the response.
    fn with_vector_indexes(self) -> Self::Output;
}

impl DescribeTableVectorExt for DescribeTableFluentBuilder {
    type Output = VectorDescribeTableOperation;

    fn with_vector_indexes(self) -> Self::Output {
        self.customize().with_vector_indexes()
    }
}

impl DescribeTableVectorExt
    for CustomizableOperation<
        aws_sdk_dynamodb::operation::describe_table::DescribeTableOutput,
        DescribeTableError,
        DescribeTableFluentBuilder,
    >
{
    type Output = VectorDescribeTableOperation;

    fn with_vector_indexes(self) -> Self::Output {
        let response_interceptor = VectorResponseHolderInterceptor::new();
        let holder = response_interceptor.holder();
        let inner = self.interceptor(response_interceptor);
        VectorDescribeTableOperation { inner, holder }
    }
}

/// Response-aware wrapper returned by [`QueryVectorExt::vector_search`].
///
/// Wraps the generated `Query` [`CustomizableOperation`]; ordinary AWS SDK
/// request setters must precede `.vector_search(...)`, since this wrapper
/// only forwards [`alternator_config_override`](Self::alternator_config_override)
/// on top of the wrapped operation.
pub struct VectorQueryOperation {
    inner: CustomizableOperation<
        aws_sdk_dynamodb::operation::query::QueryOutput,
        QueryError,
        QueryFluentBuilder,
    >,
    holder: Arc<Mutex<VectorResponseHolder>>,
}

impl VectorQueryOperation {
    /// Applies a per-request [`AlternatorOperationBuilder`] override, such as
    /// `preserve_float32_vectors`, delegating to the wrapped
    /// `CustomizableOperation`.
    pub fn alternator_config_override(
        mut self,
        config_override: impl Into<AlternatorOperationBuilder>,
    ) -> Self {
        self.inner = self.inner.alternator_config_override(config_override);
        self
    }

    /// Sends the request, returning [`VectorQueryOutput`].
    pub async fn send(self) -> Result<VectorQueryOutput, SdkError<QueryError, HttpResponse>> {
        let holder = self.holder;
        let output = self.inner.send().await?;
        let scores = holder.lock().expect("holder mutex poisoned").scores.take();
        Ok(VectorQueryOutput::new(output, scores))
    }
}

/// Response-aware wrapper returned by
/// [`CreateTableVectorExt::vector_indexes`].
pub struct VectorCreateTableOperation {
    inner: CustomizableOperation<
        aws_sdk_dynamodb::operation::create_table::CreateTableOutput,
        CreateTableError,
        CreateTableFluentBuilder,
    >,
    holder: Arc<Mutex<VectorResponseHolder>>,
}

impl VectorCreateTableOperation {
    pub fn alternator_config_override(
        mut self,
        config_override: impl Into<AlternatorOperationBuilder>,
    ) -> Self {
        self.inner = self.inner.alternator_config_override(config_override);
        self
    }

    /// Sends the request, returning [`CreateTableWithVectorIndexes`].
    pub async fn send(
        self,
    ) -> Result<CreateTableWithVectorIndexes, SdkError<CreateTableError, HttpResponse>> {
        let holder = self.holder;
        let output = self.inner.send().await?;
        let vector_indexes = holder
            .lock()
            .expect("holder mutex poisoned")
            .vector_indexes
            .take()
            .unwrap_or_default();
        Ok(CreateTableWithVectorIndexes::new(output, vector_indexes))
    }
}

/// Response-aware wrapper returned by
/// [`DescribeTableVectorExt::with_vector_indexes`].
pub struct VectorDescribeTableOperation {
    inner: CustomizableOperation<
        aws_sdk_dynamodb::operation::describe_table::DescribeTableOutput,
        DescribeTableError,
        DescribeTableFluentBuilder,
    >,
    holder: Arc<Mutex<VectorResponseHolder>>,
}

impl VectorDescribeTableOperation {
    pub fn alternator_config_override(
        mut self,
        config_override: impl Into<AlternatorOperationBuilder>,
    ) -> Self {
        self.inner = self.inner.alternator_config_override(config_override);
        self
    }

    /// Sends the request, returning [`DescribeTableWithVectorIndexes`].
    pub async fn send(
        self,
    ) -> Result<DescribeTableWithVectorIndexes, SdkError<DescribeTableError, HttpResponse>> {
        let holder = self.holder;
        let output = self.inner.send().await?;
        let vector_indexes = holder
            .lock()
            .expect("holder mutex poisoned")
            .vector_indexes
            .take()
            .unwrap_or_default();
        Ok(DescribeTableWithVectorIndexes::new(output, vector_indexes))
    }
}

#[cfg(test)]
mod tests {
    use crate::vector::{VectorAttribute, VectorIndexJson};

    /// Test that our vector index serialization produces correct JSON
    /// that the AlternatorInterceptor would inject into the CreateTable body.
    #[test]
    fn test_vector_index_json_structure() {
        let va = VectorAttribute::builder()
            .attribute_name("embedding")
            .dimensions(128)
            .build()
            .unwrap();

        let idx = crate::vector::VectorIndex::builder()
            .index_name("vec_idx")
            .vector_attribute(va)
            .build()
            .unwrap();

        let json_idx: VectorIndexJson = (&idx).into();
        let value = serde_json::to_value(&json_idx).unwrap();

        assert_eq!(value["IndexName"], "vec_idx");
        // No KeySchema — automatically inherits from base table
        assert!(value.get("KeySchema").is_none());
        // No projection configured, so the field is omitted entirely.
        assert!(value.get("Projection").is_none());
        assert_eq!(value["VectorAttribute"]["AttributeName"], "embedding");
        assert_eq!(value["VectorAttribute"]["Dimensions"], 128);
    }
}
