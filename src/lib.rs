mod client;
mod compression;
mod config;
mod customize;
mod decompression;
pub mod float32_vector;
mod interceptors;
pub mod keyrouting;
mod live_nodes;
mod optimize_headers;
mod query_plan;
mod routing_scope;
mod user_agent;
pub mod vector;
mod vector_interceptor;
mod vector_response;

pub use crate::client::*;
pub use crate::compression::*;
pub use crate::config::*;
pub use crate::customize::*;
pub use crate::float32_vector::{Float32Vector, Float32VectorError, Float32VectorExt};
pub(crate) use crate::interceptors::*;
pub use crate::keyrouting::{KeyRouteAffinityConfig, KeyRouteAffinityType};
pub(crate) use crate::live_nodes::*;
pub(crate) use crate::optimize_headers::*;
pub(crate) use crate::query_plan::*;
pub use crate::routing_scope::*;
pub use crate::user_agent::*;
pub use crate::vector::{
    CreateTableWithVectorIndexes, DescribeTableWithVectorIndexes, IndexStatus, Projection,
    ProjectionType, ReturnScores, SimilarityFunction, VectorAttribute, VectorIndex,
    VectorIndexUpdate, VectorQueryOutput, VectorSearch,
};
pub(crate) use crate::vector_interceptor::VectorRequestStore;
pub(crate) use crate::vector_interceptor::VectorResponseStore;
pub use crate::vector_interceptor::{
    CreateTableVectorExt, DescribeTableVectorExt, QueryVectorExt, UpdateTableVectorExt,
    VectorCreateTableOperation, VectorDescribeTableOperation, VectorQueryOperation,
};
