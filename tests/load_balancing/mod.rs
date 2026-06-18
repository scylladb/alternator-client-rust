//! Integration tests using a real multi-node CCM cluster: load-balancing and
//! routing, driven through counting proxies.

#[path = "../common/proxy.rs"]
pub mod proxy;

#[path = "common/scope_utils.rs"]
pub mod scope_utils;

#[path = "common/cluster_utils.rs"]
pub mod cluster_utils;

pub mod load_balancing_tests;
