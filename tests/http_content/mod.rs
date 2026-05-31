#[path = "../common/proxy.rs"]
mod proxy;

#[path = "common/http_test.rs"]
mod http_test;

#[path = "common/driver_utils.rs"]
mod driver_utils;

pub mod body_compression;
pub mod correct_line;
pub mod optimize_headers;
