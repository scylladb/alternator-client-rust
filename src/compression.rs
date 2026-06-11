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
}
