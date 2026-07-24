//! Post-decompression, pre-deserialization vector response transformation.
//!
//! Wraps an `SdkBody` with a lazy transformer that buffers the full
//! response, then (for successful, JSON, `FLOAT32VECTOR`-bearing bodies)
//! rewrites `FLOAT32VECTOR` attributes and extracts `Scores` /
//! `VectorIndexes` into a per-request [VectorResponseHolder] before handing
//! transformed bytes to the generated SDK deserializer.
//!
//! Errors from the inner body are propagated unchanged. Empty bodies,
//! non-JSON bodies, and JSON parse failures are passed through unchanged
//! rather than turned into a local error: only a successful, safe
//! transformation replaces the original bytes.

use crate::float32_vector::rewrite_response_json_markers;
use crate::vector::vector_index_from_json;
use crate::vector_interceptor::VectorResponseHolder;

use aws_smithy_types::body::SdkBody;
use bytes::Bytes;
use futures_util::stream::Stream;
use http_body::Frame;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};

/// Wraps `body` with a lazy transformer. See module docs for behavior.
pub(crate) fn wrap_vector_response_body(
    body: SdkBody,
    preserve: bool,
    holder: Option<Arc<Mutex<VectorResponseHolder>>>,
) -> SdkBody {
    let stream = http_body_util::BodyStream::new(body);

    let body_impl = VectorTransformBody {
        inner: Box::pin(stream),
        buffer: Vec::new(),
        trailers: Vec::new(),
        preserve,
        holder,
        done: false,
        emitted_data: false,
        emitted_trailers: false,
    };

    SdkBody::from_body_1_x(body_impl)
}

type BoxedError = Box<dyn std::error::Error + Send + Sync>;

struct VectorTransformBody {
    inner: Pin<Box<dyn Stream<Item = Result<Frame<Bytes>, BoxedError>> + Send + Sync>>,
    buffer: Vec<u8>,
    /// Non-data frames (i.e. trailers) observed on the inner stream, to be
    /// re-emitted unchanged after the transformed data frame.
    trailers: Vec<Frame<Bytes>>,
    preserve: bool,
    holder: Option<Arc<Mutex<VectorResponseHolder>>>,
    done: bool,
    emitted_data: bool,
    emitted_trailers: bool,
}

impl http_body::Body for VectorTransformBody {
    type Data = Bytes;
    type Error = BoxedError;

    fn poll_frame(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
        let this = self.get_mut();

        loop {
            if !this.emitted_data {
                if !this.done {
                    let inner = this.inner.as_mut();
                    match inner.poll_next(cx) {
                        Poll::Ready(Some(Ok(frame))) => match frame.into_data() {
                            Ok(bytes) => {
                                this.buffer.extend_from_slice(&bytes);
                                continue;
                            }
                            Err(frame) => {
                                // A non-data frame (trailers): stash it to
                                // be emitted after the transformed data
                                // frame, since a single data frame must be
                                // emitted whole after full buffering.
                                this.trailers.push(frame);
                                continue;
                            }
                        },
                        Poll::Ready(Some(Err(e))) => {
                            // Propagate upstream errors as-is; do not
                            // attempt a transform on a partial/failed body.
                            this.emitted_data = true;
                            this.emitted_trailers = true;
                            return Poll::Ready(Some(Err(e)));
                        }
                        Poll::Ready(None) => {
                            this.done = true;
                        }
                        Poll::Pending => return Poll::Pending,
                    }
                }

                this.emitted_data = true;
                return Poll::Ready(Some(
                    match transform_bytes(&this.buffer, this.preserve, this.holder.as_ref()) {
                        Ok(transformed) => Ok(Frame::data(Bytes::from(transformed))),
                        Err(error) => Err(error.into()),
                    },
                ));
            }

            if let Some(frame) = this.trailers.pop() {
                return Poll::Ready(Some(Ok(frame)));
            }
            this.emitted_trailers = true;
            return Poll::Ready(None);
        }
    }
}

/// Transforms a fully-buffered response body: extracts `Scores` and
/// `VectorIndexes` into `holder` (if present) and rewrites `FLOAT32VECTOR`
/// attributes. Returns the original bytes unchanged for empty, non-JSON,
/// or unparsable bodies.
fn transform_bytes(
    bytes: &[u8],
    preserve: bool,
    holder: Option<&Arc<Mutex<VectorResponseHolder>>>,
) -> Result<Vec<u8>, String> {
    if bytes.is_empty() {
        return Ok(bytes.to_vec());
    }

    // Fast path: skip JSON parsing entirely when there is nothing for us
    // to do (no response holder registered, and no FLOAT32VECTOR content
    // present in the raw bytes).
    if holder.is_none() && !contains_subslice(bytes, b"FLOAT32VECTOR") {
        return Ok(bytes.to_vec());
    }

    let Ok(mut json) = serde_json::from_slice::<serde_json::Value>(bytes) else {
        return Ok(bytes.to_vec());
    };

    if let Some(holder) = holder {
        if let Some(scores_value) = json.get("Scores") {
            let scores = scores_value
                .as_array()
                .ok_or("Scores response field must be an array")?
                .iter()
                .map(|value| {
                    value
                        .as_f64()
                        .ok_or("Scores response field must contain numbers")
                })
                .collect::<Result<Vec<_>, _>>()?;

            if let Some(items) = json.get("Items").and_then(serde_json::Value::as_array)
                && items.len() != scores.len()
            {
                return Err("Items and Scores response fields must have the same length".into());
            }

            holder.lock().expect("holder mutex poisoned").scores = Some(scores);
        }

        for key in ["Table", "TableDescription"] {
            if let Some(indexes_value) = json.get(key).and_then(|t| t.get("VectorIndexes")) {
                let indexes = indexes_value
                    .as_array()
                    .ok_or("VectorIndexes response field must be an array")?;
                let parsed: Vec<_> = indexes
                    .iter()
                    .map(vector_index_from_json)
                    .collect::<Result<_, _>>()?;
                holder.lock().expect("holder mutex poisoned").vector_indexes = Some(parsed);
            }
        }
    }

    rewrite_response_json_markers(&mut json, preserve);

    Ok(serde_json::to_vec(&json).unwrap_or_else(|_| bytes.to_vec()))
}

fn contains_subslice(haystack: &[u8], needle: &[u8]) -> bool {
    if needle.is_empty() || haystack.len() < needle.len() {
        return needle.is_empty();
    }
    haystack.windows(needle.len()).any(|w| w == needle)
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures_util::stream;
    use http_body::Body as _;
    use http_body_util::BodyExt;
    use http_body_util::StreamBody;

    async fn drain(body: SdkBody) -> Vec<u8> {
        let collected = body.collect().await.expect("body should not error");
        collected.to_bytes().to_vec()
    }

    /// Builds an [SdkBody] from multiple raw chunks, each delivered as its
    /// own `http_body::Frame::data`, to exercise the transformer's
    /// cross-frame buffering.
    fn multi_frame_body(chunks: Vec<&'static [u8]>) -> SdkBody {
        let frames: Vec<Result<Frame<Bytes>, BoxedError>> = chunks
            .into_iter()
            .map(|c| Ok(Frame::data(Bytes::from_static(c))))
            .collect();
        SdkBody::from_body_1_x(StreamBody::new(stream::iter(frames)))
    }

    #[tokio::test]
    async fn transforms_correctly_across_multiple_input_frames() {
        let original = serde_json::json!({
            "Item": { "embedding": { "FLOAT32VECTOR": [1.0, 2.0] } }
        });
        let bytes = serde_json::to_vec(&original).unwrap();
        // Split the JSON into two chunks delivered as separate frames.
        let mid = bytes.len() / 2;
        let first: &'static [u8] = Box::leak(bytes[..mid].to_vec().into_boxed_slice());
        let second: &'static [u8] = Box::leak(bytes[mid..].to_vec().into_boxed_slice());

        let body = wrap_vector_response_body(multi_frame_body(vec![first, second]), false, None);
        let out = drain(body).await;
        let parsed: serde_json::Value = serde_json::from_slice(&out).unwrap();
        assert_eq!(
            parsed["Item"]["embedding"]["L"],
            serde_json::json!([{ "N": "1" }, { "N": "2" }])
        );
    }

    #[tokio::test]
    async fn preserves_trailers_across_transformation() {
        let original = serde_json::json!({
            "Item": { "embedding": { "FLOAT32VECTOR": [1.0, 2.0] } }
        });
        let bytes = serde_json::to_vec(&original).unwrap();

        let mut trailer_map = http::HeaderMap::new();
        trailer_map.insert(
            "x-amz-trailer-test",
            http::HeaderValue::from_static("present"),
        );

        let frames: Vec<Result<Frame<Bytes>, BoxedError>> = vec![
            Ok(Frame::data(Bytes::from(bytes))),
            Ok(Frame::trailers(trailer_map)),
        ];
        let input = SdkBody::from_body_1_x(StreamBody::new(stream::iter(frames)));

        let mut body = wrap_vector_response_body(input, false, None);
        let mut saw_trailers = false;
        loop {
            match std::future::poll_fn(|cx| Pin::new(&mut body).poll_frame(cx)).await {
                Some(Ok(frame)) => {
                    if frame.is_trailers() {
                        saw_trailers = true;
                    }
                }
                Some(Err(e)) => panic!("body should not error: {e}"),
                None => break,
            }
        }

        assert!(
            saw_trailers,
            "trailers must survive vector response transformation"
        );
    }

    #[tokio::test]
    async fn passes_through_empty_body_unchanged() {
        let body = wrap_vector_response_body(SdkBody::empty(), false, None);
        assert_eq!(drain(body).await, Vec::<u8>::new());
    }

    #[tokio::test]
    async fn passes_through_non_json_body_unchanged() {
        let body = wrap_vector_response_body(SdkBody::from("not json"), false, None);
        assert_eq!(drain(body).await, b"not json".to_vec());
    }

    #[tokio::test]
    async fn passes_through_body_without_marker_unchanged() {
        let original = serde_json::json!({ "Item": { "pk": { "S": "x" } } });
        let bytes = serde_json::to_vec(&original).unwrap();
        let body = wrap_vector_response_body(SdkBody::from(bytes.clone()), false, None);
        assert_eq!(drain(body).await, bytes);
    }

    #[tokio::test]
    async fn converts_float32vector_to_l_n_by_default() {
        let original = serde_json::json!({
            "Item": { "embedding": { "FLOAT32VECTOR": [1.0, 2.0] } }
        });
        let bytes = serde_json::to_vec(&original).unwrap();
        let body = wrap_vector_response_body(SdkBody::from(bytes), false, None);
        let out = drain(body).await;
        let parsed: serde_json::Value = serde_json::from_slice(&out).unwrap();
        assert_eq!(
            parsed["Item"]["embedding"]["L"],
            serde_json::json!([{ "N": "1" }, { "N": "2" }])
        );
    }

    #[tokio::test]
    async fn converts_float32vector_to_marker_binary_when_preserving() {
        let original = serde_json::json!({
            "Item": { "embedding": { "FLOAT32VECTOR": [1.0, 2.0] } }
        });
        let bytes = serde_json::to_vec(&original).unwrap();
        let body = wrap_vector_response_body(SdkBody::from(bytes), true, None);
        let out = drain(body).await;
        let parsed: serde_json::Value = serde_json::from_slice(&out).unwrap();
        assert!(parsed["Item"]["embedding"].get("B").is_some());
    }

    #[tokio::test]
    async fn extracts_scores_into_holder() {
        let original = serde_json::json!({ "Items": [{}, {}], "Scores": [0.9, 0.1] });
        let bytes = serde_json::to_vec(&original).unwrap();
        let holder = Arc::new(Mutex::new(VectorResponseHolder::default()));
        let body = wrap_vector_response_body(SdkBody::from(bytes), false, Some(holder.clone()));
        let _ = drain(body).await;
        assert_eq!(holder.lock().unwrap().scores, Some(vec![0.9, 0.1]));
    }

    #[tokio::test]
    async fn rejects_scores_with_mismatched_items() {
        let original = serde_json::json!({ "Items": [], "Scores": [0.9] });
        let bytes = serde_json::to_vec(&original).unwrap();
        let holder = Arc::new(Mutex::new(VectorResponseHolder::default()));
        let body = wrap_vector_response_body(SdkBody::from(bytes), false, Some(holder));

        let error = body.collect().await.unwrap_err();
        assert_eq!(
            error.to_string(),
            "Items and Scores response fields must have the same length"
        );
    }

    #[tokio::test]
    async fn extracts_vector_indexes_from_table_description_into_holder() {
        let original = serde_json::json!({
            "TableDescription": {
                "VectorIndexes": [{
                    "IndexName": "vec_idx",
                    "VectorAttribute": { "AttributeName": "embedding", "Dimensions": 4 },
                    "IndexStatus": "ACTIVE",
                    "Backfilling": false
                }]
            }
        });
        let bytes = serde_json::to_vec(&original).unwrap();
        let holder = Arc::new(Mutex::new(VectorResponseHolder::default()));
        let body = wrap_vector_response_body(SdkBody::from(bytes), false, Some(holder.clone()));
        let _ = drain(body).await;
        let indexes = holder.lock().unwrap().vector_indexes.clone().unwrap();
        assert_eq!(indexes.len(), 1);
        assert_eq!(indexes[0].index_name, "vec_idx");
    }

    #[tokio::test]
    async fn rejects_non_array_vector_indexes() {
        let original = serde_json::json!({ "Table": { "VectorIndexes": {} } });
        let bytes = serde_json::to_vec(&original).unwrap();
        let holder = Arc::new(Mutex::new(VectorResponseHolder::default()));
        let body = wrap_vector_response_body(SdkBody::from(bytes), false, Some(holder));

        let error = body.collect().await.unwrap_err();
        assert_eq!(
            error.to_string(),
            "VectorIndexes response field must be an array"
        );
    }
}
