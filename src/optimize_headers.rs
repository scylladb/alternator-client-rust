use aws_smithy_runtime_api::http::Request;

/// To be used in an interceptor
///
/// Removes unwanted headers from the given request
pub(crate) fn strip_headers(request: &mut Request, preserve_auth_headers: bool) {
    let headers = request.headers_mut();

    const WHITELIST: [&str; 5] = [
        "host",
        "x-amz-target",
        "content-length",
        "accept-encoding",
        "content-encoding",
    ];
    const AUTH_WHITELIST: [&str; 2] = ["authorization", "x-amz-date"];

    let unallowed_keys: Vec<String> = headers
        .iter()
        .map(|(key, _)| key.to_string())
        .filter(|key| {
            !(WHITELIST.contains(&key.as_str())
                || preserve_auth_headers && AUTH_WHITELIST.contains(&key.as_str()))
        })
        .collect();

    for key in unallowed_keys {
        headers.remove(key);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn request_with_headers() -> Request {
        let mut request = Request::empty();
        request.headers_mut().insert("host", "localhost:8000");
        request
            .headers_mut()
            .insert("x-amz-target", "DynamoDB_20120810.PutItem");
        request.headers_mut().insert("content-length", "10");
        request.headers_mut().insert("accept-encoding", "gzip");
        request.headers_mut().insert("content-encoding", "gzip");
        request.headers_mut().insert("authorization", "signature");
        request
            .headers_mut()
            .insert("x-amz-date", "20260626T120000Z");
        request.headers_mut().insert("user-agent", "test");
        request.headers_mut().insert("amz-sdk-request", "attempt=1");
        request
    }

    #[test]
    fn no_auth_allowlist_drops_auth_headers_and_keeps_compression_headers() {
        let mut request = request_with_headers();

        strip_headers(&mut request, false);

        assert!(request.headers().contains_key("host"));
        assert!(request.headers().contains_key("x-amz-target"));
        assert!(request.headers().contains_key("content-length"));
        assert!(request.headers().contains_key("accept-encoding"));
        assert!(request.headers().contains_key("content-encoding"));
        assert!(!request.headers().contains_key("authorization"));
        assert!(!request.headers().contains_key("x-amz-date"));
        assert!(!request.headers().contains_key("user-agent"));
        assert!(!request.headers().contains_key("amz-sdk-request"));
    }

    #[test]
    fn signed_allowlist_preserves_auth_headers_and_compression_headers() {
        let mut request = request_with_headers();

        strip_headers(&mut request, true);

        assert!(request.headers().contains_key("host"));
        assert!(request.headers().contains_key("x-amz-target"));
        assert!(request.headers().contains_key("content-length"));
        assert!(request.headers().contains_key("accept-encoding"));
        assert!(request.headers().contains_key("content-encoding"));
        assert!(request.headers().contains_key("authorization"));
        assert!(request.headers().contains_key("x-amz-date"));
        assert!(!request.headers().contains_key("user-agent"));
        assert!(!request.headers().contains_key("amz-sdk-request"));
    }
}
