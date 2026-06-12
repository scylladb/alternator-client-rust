use std::io::prelude::Read;

use aws_smithy_runtime_api::http::Request;

use flate2::read::{GzDecoder, GzEncoder, ZlibDecoder, ZlibEncoder};

pub use flate2::Compression as CompressionLevel;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompressionAlgorithm {
    Gzip,
    Zlib,
}

/// Represents information on how to compress requests
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RequestCompression {
    compression_with_threshold: Option<(CompressionAlgorithm, CompressionLevel, usize)>,
}
impl RequestCompression {
    /// Represents request compression that is applied when the HTTP body size is
    /// at least the given threshold.
    ///
    /// When threshold is zero, compression is always applied.
    pub fn enabled(
        algorithm: CompressionAlgorithm,
        level: CompressionLevel,
        threshold: usize,
    ) -> Self {
        Self {
            compression_with_threshold: Some((algorithm, level, threshold)),
        }
    }

    /// Represents no request compression
    pub fn disabled() -> Self {
        Self {
            compression_with_threshold: None,
        }
    }

    /// Returns `None` if request compression is disabled,
    /// otherwise returns the algorithm, level and threshold
    pub fn get(&self) -> Option<(CompressionAlgorithm, CompressionLevel, usize)> {
        self.compression_with_threshold
    }
}
impl Default for RequestCompression {
    fn default() -> Self {
        Self::enabled(
            CompressionAlgorithm::Gzip,
            CompressionLevel::default(),
            1024,
        )
    }
}

fn compress_gzip(content: &[u8], level: CompressionLevel) -> Option<Vec<u8>> {
    let mut compressed = Vec::new();
    let mut encoder = GzEncoder::new(content, level);
    encoder.read_to_end(&mut compressed).ok()?;
    Some(compressed)
}

#[allow(dead_code)]
fn decompress_gzip(content: &[u8]) -> Option<Vec<u8>> {
    let mut decompressed = Vec::new();
    let mut decoder = GzDecoder::new(content);
    decoder.read_to_end(&mut decompressed).ok()?;
    Some(decompressed)
}

fn compress_zlib(content: &[u8], level: CompressionLevel) -> Option<Vec<u8>> {
    let mut compressed = Vec::new();
    let mut encoder = ZlibEncoder::new(content, level);
    encoder.read_to_end(&mut compressed).ok()?;
    Some(compressed)
}

#[allow(dead_code)]
fn decompress_zlib(content: &[u8]) -> Option<Vec<u8>> {
    let mut decompressed = Vec::new();
    let mut decoder = ZlibDecoder::new(content);
    decoder.read_to_end(&mut decompressed).ok()?;
    Some(decompressed)
}

/// Algorithm for response compression negotiation.
///
/// Used to specify which encodings the client is willing to accept
/// in the `Accept-Encoding` HTTP header.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ResponseCompressionAlgorithm {
    Gzip,
    Deflate,
}

impl ResponseCompressionAlgorithm {
    /// Returns the HTTP `Accept-Encoding` token for this algorithm.
    pub(crate) fn accept_encoding_token(&self) -> &'static str {
        match self {
            Self::Gzip => "gzip",
            Self::Deflate => "deflate",
        }
    }

    /// Parses a `Content-Encoding` token into an algorithm.
    /// Case-insensitive, trims whitespace and ignores parameters (e.g. `; q=1.0`).
    pub(crate) fn from_content_encoding(token: &str) -> Option<Self> {
        // Strip parameters (e.g. "gzip; q=1.0" -> "gzip")
        let token = token.split(';').next().unwrap_or(token).trim();
        match token.to_ascii_lowercase().as_str() {
            "gzip" | "x-gzip" => Some(Self::Gzip),
            "deflate" => Some(Self::Deflate),
            _ => None,
        }
    }

    /// Returns all supported algorithms.
    pub(crate) fn all() -> &'static [Self] {
        &[Self::Gzip, Self::Deflate]
    }
}

/// Builds the `Accept-Encoding` header value from a list of algorithms.
pub(crate) fn accept_encoding_header_value(algorithms: &[ResponseCompressionAlgorithm]) -> String {
    algorithms
        .iter()
        .map(|a| a.accept_encoding_token())
        .collect::<Vec<_>>()
        .join(", ")
}

/// Configures which response encodings the client advertises via `Accept-Encoding`.
///
/// This only controls what the client *requests* from the server.
/// The server may still return uncompressed responses regardless of this setting.
/// Response decompression itself is based on the `Content-Encoding` header
/// and is independent of this configuration.
///
/// The accepted encodings are sent in order of preference.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ResponseCompression {
    accepted_algorithms: Option<Vec<ResponseCompressionAlgorithm>>,
}

impl ResponseCompression {
    /// Advertise a single accepted encoding.
    pub fn enabled(algorithm: ResponseCompressionAlgorithm) -> Self {
        Self {
            accepted_algorithms: Some(vec![algorithm]),
        }
    }

    /// Advertise multiple accepted encodings in order of preference.
    ///
    /// Duplicates are removed, preserving the first occurrence.
    /// An empty iterator means all supported algorithms (convenience).
    pub fn enabled_many(
        algorithms: impl IntoIterator<Item = ResponseCompressionAlgorithm>,
    ) -> Self {
        let mut seen = Vec::new();
        for algo in algorithms {
            if !seen.contains(&algo) {
                seen.push(algo);
            }
        }
        if seen.is_empty() {
            Self::enabled_all()
        } else {
            Self {
                accepted_algorithms: Some(seen),
            }
        }
    }

    /// Advertise all supported response encodings.
    pub fn enabled_all() -> Self {
        Self {
            accepted_algorithms: Some(ResponseCompressionAlgorithm::all().to_vec()),
        }
    }

    /// Do not advertise any accepted response encoding.
    pub fn disabled() -> Self {
        Self {
            accepted_algorithms: None,
        }
    }

    /// Returns `None` if disabled, otherwise the ordered list of accepted algorithms.
    pub fn get(&self) -> Option<&[ResponseCompressionAlgorithm]> {
        self.accepted_algorithms.as_deref()
    }
}

impl Default for ResponseCompression {
    fn default() -> Self {
        Self::disabled()
    }
}

/// To be used in an interceptor
///
/// Checks whether the body size meets the specified threshold.
/// If so, tries to compress the body and modify `content-encoding` and `content-length` headers accordingly.
///
/// If compression failed or request already contained the `content-encoding` header, leaves the request untouched.
pub(crate) fn compress_request(
    request: &mut Request,
    algorithm: CompressionAlgorithm,
    level: CompressionLevel,
    threshold: usize,
) {
    // already compressed
    if request.headers().contains_key("content-encoding") {
        return;
    }

    // dynamodb serializers are expected to produce collected bodies
    if let Some(body) = request.body_mut().bytes() {
        // check threshold
        if body.len() < threshold {
            return;
        }

        // compress body
        let (compressed, http_code) = match algorithm {
            CompressionAlgorithm::Gzip => (compress_gzip(body, level), "gzip"),
            CompressionAlgorithm::Zlib => (compress_zlib(body, level), "deflate"),
        };

        if let Some(compressed) = compressed {
            // modify request
            request.headers_mut().append("content-encoding", http_code);
            request
                .headers_mut()
                .insert("content-length", compressed.len().to_string());

            *request.body_mut() = compressed.into();
        }
    }
}

#[cfg(test)]
mod tests {

    use super::*;
    use aws_smithy_types::body::SdkBody;

    fn example_request() -> Request {
        let request = http::Request::builder()
            .method("GET")
            .uri("/")
            .header("content-type", "application/x-amz-json-1.0")
            .header("x-amz-target", "DynamoDB_20120810.CreateTable")
            .header("content-length", "210")
            .header("host", "localhost:7999")
            .body(SdkBody::from(r#"{"AttributeDefinitions":[{"AttributeName":"ExampleAttribute","AttributeType":"S"}],"TableName":"ExampleTable","KeySchema":[{"AttributeName":"ExampleAttribute","KeyType":"HASH"}],"BillingMode":"PAY_PER_REQUEST"}"#))
            .expect("invalid request");

        Request::try_from(request).expect("invalid conversion")
    }

    #[test]
    fn test_gzip_request_compression() {
        let original_request = example_request();
        let mut request = example_request();

        compress_request(
            &mut request,
            CompressionAlgorithm::Gzip,
            CompressionLevel::default(),
            0,
        );

        assert_eq!(
            request
                .headers()
                .get("content-encoding")
                .expect("content-encoding header exists"),
            "gzip"
        );

        assert_eq!(
            decompress_gzip(request.body().bytes().expect("cant access body bytes"))
                .expect("decompression unsuccessful"),
            original_request
                .body()
                .bytes()
                .expect("cant access body bytes")
        );
    }

    #[test]
    fn test_zlib_request_compression() {
        let original_request = example_request();
        let mut request = example_request();

        compress_request(
            &mut request,
            CompressionAlgorithm::Zlib,
            CompressionLevel::default(),
            0,
        );

        assert_eq!(
            request
                .headers()
                .get("content-encoding")
                .expect("content-encoding header does not exist"),
            "deflate"
        );

        assert_eq!(
            decompress_zlib(request.body().bytes().expect("cant access body bytes"))
                .expect("decompression unsuccessful"),
            original_request
                .body()
                .bytes()
                .expect("cant access body bytes")
        );
    }

    #[test]
    fn test_request_compression_threshold_exceeded() {
        let original_request = example_request();
        let mut request = example_request();

        let content_length = original_request
            .body()
            .content_length()
            .expect("cant access content length") as usize;

        compress_request(
            &mut request,
            CompressionAlgorithm::Gzip,
            CompressionLevel::default(),
            content_length + 1,
        );

        assert!(!request.headers().contains_key("content-encoding"));

        assert_eq!(
            request.body().bytes().expect("cant access body bytes"),
            original_request
                .body()
                .bytes()
                .expect("cant access body bytes")
        );
    }

    #[test]
    fn test_request_compression_threshold_within() {
        let original_request = example_request();
        let mut request = example_request();

        let content_length = original_request
            .body()
            .content_length()
            .expect("cant access content length") as usize;

        compress_request(
            &mut request,
            CompressionAlgorithm::Gzip,
            CompressionLevel::default(),
            content_length,
        );

        assert_eq!(
            request
                .headers()
                .get("content-encoding")
                .expect("content-encoding header does not exist"),
            "gzip"
        );

        assert_eq!(
            decompress_gzip(request.body().bytes().expect("cant access body bytes"))
                .expect("decompression unsuccessful"),
            original_request
                .body()
                .bytes()
                .expect("cant access body bytes")
        );
    }

    #[test]
    fn test_request_compression_no_double_compression() {
        let original_request = example_request();
        let mut request = example_request();

        request.headers_mut().insert("content-encoding", "deflate");

        compress_request(
            &mut request,
            CompressionAlgorithm::Gzip,
            CompressionLevel::default(),
            0,
        );

        assert_eq!(
            request
                .headers()
                .get("content-encoding")
                .expect("content-encoding header does not exist"),
            "deflate"
        );

        assert_eq!(
            request.body().bytes().expect("cant access body bytes"),
            original_request
                .body()
                .bytes()
                .expect("cant access body bytes")
        );
    }

    // --- ResponseCompression API tests ---

    #[test]
    fn response_compression_default_is_disabled() {
        let rc = ResponseCompression::default();
        assert_eq!(rc.get(), None);
    }

    #[test]
    fn response_compression_enabled_gzip() {
        let rc = ResponseCompression::enabled(ResponseCompressionAlgorithm::Gzip);
        assert_eq!(
            rc.get(),
            Some([ResponseCompressionAlgorithm::Gzip].as_slice())
        );
    }

    #[test]
    fn response_compression_enabled_deflate() {
        let rc = ResponseCompression::enabled(ResponseCompressionAlgorithm::Deflate);
        assert_eq!(
            rc.get(),
            Some([ResponseCompressionAlgorithm::Deflate].as_slice())
        );
    }

    #[test]
    fn response_compression_enabled_many_preserves_order() {
        let rc = ResponseCompression::enabled_many([
            ResponseCompressionAlgorithm::Gzip,
            ResponseCompressionAlgorithm::Deflate,
        ]);
        assert_eq!(
            rc.get(),
            Some(
                [
                    ResponseCompressionAlgorithm::Gzip,
                    ResponseCompressionAlgorithm::Deflate,
                ]
                .as_slice()
            )
        );
    }

    #[test]
    fn response_compression_enabled_many_reverse_order() {
        let rc = ResponseCompression::enabled_many([
            ResponseCompressionAlgorithm::Deflate,
            ResponseCompressionAlgorithm::Gzip,
        ]);
        assert_eq!(
            rc.get(),
            Some(
                [
                    ResponseCompressionAlgorithm::Deflate,
                    ResponseCompressionAlgorithm::Gzip,
                ]
                .as_slice()
            )
        );
    }

    #[test]
    fn response_compression_enabled_many_deduplicates() {
        let rc = ResponseCompression::enabled_many([
            ResponseCompressionAlgorithm::Gzip,
            ResponseCompressionAlgorithm::Gzip,
            ResponseCompressionAlgorithm::Deflate,
        ]);
        assert_eq!(
            rc.get(),
            Some(
                [
                    ResponseCompressionAlgorithm::Gzip,
                    ResponseCompressionAlgorithm::Deflate,
                ]
                .as_slice()
            )
        );
    }

    #[test]
    fn response_compression_enabled_many_empty_means_all() {
        let rc = ResponseCompression::enabled_many(std::iter::empty());
        assert_eq!(rc.get(), Some(ResponseCompressionAlgorithm::all()));
    }

    #[test]
    fn response_compression_enabled_all() {
        let rc = ResponseCompression::enabled_all();
        assert_eq!(rc.get(), Some(ResponseCompressionAlgorithm::all()));
    }

    #[test]
    fn accept_encoding_header_value_gzip_deflate() {
        let value = accept_encoding_header_value(&[
            ResponseCompressionAlgorithm::Gzip,
            ResponseCompressionAlgorithm::Deflate,
        ]);
        assert_eq!(value, "gzip, deflate");
    }

    #[test]
    fn from_content_encoding_gzip() {
        assert_eq!(
            ResponseCompressionAlgorithm::from_content_encoding("gzip"),
            Some(ResponseCompressionAlgorithm::Gzip)
        );
    }

    #[test]
    fn from_content_encoding_deflate() {
        assert_eq!(
            ResponseCompressionAlgorithm::from_content_encoding("deflate"),
            Some(ResponseCompressionAlgorithm::Deflate)
        );
    }

    #[test]
    fn from_content_encoding_case_insensitive() {
        assert_eq!(
            ResponseCompressionAlgorithm::from_content_encoding("GZIP"),
            Some(ResponseCompressionAlgorithm::Gzip)
        );
    }

    #[test]
    fn from_content_encoding_unsupported() {
        assert_eq!(
            ResponseCompressionAlgorithm::from_content_encoding("br"),
            None
        );
    }

    #[test]
    fn from_content_encoding_with_parameters() {
        assert_eq!(
            ResponseCompressionAlgorithm::from_content_encoding("gzip; q=1.0"),
            Some(ResponseCompressionAlgorithm::Gzip)
        );
    }

    #[test]
    fn request_compression_deflate_sends_deflate_header() {
        let mut request = example_request();
        compress_request(
            &mut request,
            CompressionAlgorithm::Zlib,
            CompressionLevel::default(),
            0,
        );
        assert_eq!(
            request
                .headers()
                .get("content-encoding")
                .expect("content-encoding header"),
            "deflate"
        );
    }
}
