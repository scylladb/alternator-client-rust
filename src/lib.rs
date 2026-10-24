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

//! Client-side discovery and load balancing for ScyllaDB Alternator.
//!
//! This crate wraps the AWS SDK for DynamoDB with Alternator-specific configuration,
//! node discovery, request routing, compression, and optional key-route affinity.
//! Build an [`AlternatorConfig`] with [`AlternatorConfig::builder`], then create an
//! [`AlternatorClient`] to access DynamoDB operation builders.

#![warn(missing_docs)]

mod client;
mod compression;
mod config;
mod customize;
mod decompression;
mod discovery;
mod interceptors;
mod keyrouting;
mod live_nodes;
mod optimize_headers;
mod query_plan;
mod routing_scope;
mod user_agent;

pub use crate::client::*;
pub use crate::compression::*;
pub use crate::config::*;
pub use crate::customize::*;
pub(crate) use crate::interceptors::*;
pub use crate::keyrouting::{
    KeyRouteAffinityConfig, KeyRouteAffinityConfigBuilder, KeyRouteAffinityType,
};
pub(crate) use crate::live_nodes::*;
pub(crate) use crate::optimize_headers::*;
pub(crate) use crate::query_plan::*;
pub use crate::routing_scope::*;
pub use crate::user_agent::*;
