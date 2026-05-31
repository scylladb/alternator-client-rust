use aws_smithy_runtime_api::http::Request;

/// To be used in an interceptor
///
/// Removes unwanted headers from the given request
pub(crate) fn strip_headers(request: &mut Request) {
    let headers = request.headers_mut();

    const WHITELIST: [&str; 7] = [
        "host",
        "x-amz-target",
        "content-length",
        "accept-encoding",
        "content-encoding",
        "authorization",
        "x-amz-date",
    ];

    let unallowed_keys: Vec<String> = headers
        .iter()
        .map(|(key, _)| key.to_string())
        .filter(|key| !WHITELIST.contains(&key.as_str()))
        .collect();

    for key in unallowed_keys {
        headers.remove(key);
    }
}
