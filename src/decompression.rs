//! Streaming response decompression.
//!
//! Wraps an `SdkBody` with lazy decompression based on `Content-Encoding` tokens.
//! The decompression is streaming: it does not buffer the entire compressed body
//! before producing decompressed output.

use aws_smithy_runtime_api::box_error::BoxError;
use aws_smithy_types::body::SdkBody;
use bytes::Bytes;
use futures_util::stream::TryStreamExt;
use http_body::Frame;
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::io::AsyncRead;
use tokio_util::io::{ReaderStream, StreamReader};

use crate::ResponseCompressionAlgorithm;

/// Wraps an `SdkBody` with streaming decompression for the given encodings.
///
/// Encodings are listed in HTTP `Content-Encoding` application order (outermost first).
/// Decoding reverses this: the last listed encoding is decoded first from the raw bytes.
pub(crate) fn wrap_decompressed_body(
    body: SdkBody,
    encodings: Vec<ResponseCompressionAlgorithm>,
) -> Result<SdkBody, BoxError> {
    // Convert SdkBody into an AsyncRead via http-body -> Stream -> StreamReader
    let body_stream = http_body_util::BodyStream::new(body)
        .try_filter_map(|frame| async move { Ok(frame.into_data().ok()) });

    let reader: Pin<Box<dyn AsyncRead + Send + Sync>> = Box::pin(StreamReader::new(
        body_stream.map_err(std::io::Error::other),
    ));

    // Decode in reverse order: Content-Encoding lists outermost first,
    // so we unwrap from the innermost layer outward.
    let reader = encodings
        .into_iter()
        .rev()
        .fold(reader, |r, algo| match algo {
            ResponseCompressionAlgorithm::Gzip => Box::pin(
                async_compression::tokio::bufread::GzipDecoder::new(tokio::io::BufReader::new(r)),
            ),
            ResponseCompressionAlgorithm::Deflate => Box::pin(
                async_compression::tokio::bufread::ZlibDecoder::new(tokio::io::BufReader::new(r)),
            ),
        });

    // Convert the decoded AsyncRead back into an SdkBody
    let decoded_stream = ReaderStream::new(reader);
    let body_impl = StreamingDecompressedBody::new(decoded_stream);

    Ok(SdkBody::from_body_1_x(body_impl))
}

/// A custom `http_body::Body` implementation that wraps a `ReaderStream`
/// producing decompressed `Bytes` chunks.
struct StreamingDecompressedBody {
    inner: ReaderStream<Pin<Box<dyn AsyncRead + Send + Sync>>>,
}

impl StreamingDecompressedBody {
    fn new(stream: ReaderStream<Pin<Box<dyn AsyncRead + Send + Sync>>>) -> Self {
        Self { inner: stream }
    }
}

impl http_body::Body for StreamingDecompressedBody {
    type Data = Bytes;
    type Error = Box<dyn std::error::Error + Send + Sync>;

    fn poll_frame(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
        use futures_util::Stream;

        let this = self.get_mut();
        let inner = Pin::new(&mut this.inner);

        match inner.poll_next(cx) {
            Poll::Ready(Some(Ok(bytes))) => Poll::Ready(Some(Ok(Frame::data(bytes)))),
            Poll::Ready(Some(Err(e))) => Poll::Ready(Some(Err(
                Box::new(e) as Box<dyn std::error::Error + Send + Sync>
            ))),
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }
}
