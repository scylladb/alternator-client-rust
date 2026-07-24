//! Compact `FLOAT32VECTOR` transport marker encoding.
//!
//! Alternator's `FLOAT32VECTOR` attribute type has no equivalent in the
//! generated AWS SDK's `AttributeValue` union. To let callers construct and
//! (optionally) preserve compact vector values using only standard SDK
//! types, this module encodes a `FLOAT32VECTOR` payload as an ordinary
//! `AttributeValue::B` binary value carrying a private magic prefix followed
//! by big-endian IEEE-754 `f32` values.
//!
//! This marker is purely an internal transport representation:
//! [`AlternatorInterceptor`](crate) rewrites marker binaries into
//! `{"FLOAT32VECTOR": [...]}` in outgoing request JSON, and can rewrite
//! `FLOAT32VECTOR` response fields back into marker binaries when
//! `preserve_float32_vectors` is enabled. Ordinary binary values that do not
//! match the exact magic prefix and a payload length divisible by
//! `size_of::<f32>()` are left untouched and are never misinterpreted as
//! vectors.

use aws_sdk_dynamodb::primitives::Blob;
use aws_sdk_dynamodb::types::AttributeValue;
use base64::Engine as _;
use std::fmt;
use std::sync::OnceLock;

/// Private magic prefix identifying a `FLOAT32VECTOR` marker binary.
///
/// Chosen to be exceedingly unlikely to collide with real-world binary
/// attribute values: 8 bytes, non-printable, versioned.
const MAGIC_PREFIX: &[u8; 8] = &[0xAC, b'A', b'V', b'1', 0xF3, 0x2E, 0x00, 0x7F];

/// Error returned when decoding a value as a `FLOAT32VECTOR` marker fails.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Float32VectorError {
    /// The value is not an `AttributeValue::B` at all.
    NotBinary,
    /// The binary value does not start with the marker's magic prefix.
    NotAMarker,
    /// The marker payload length is not a multiple of `size_of::<f32>()`.
    InvalidPayloadLength { len: usize },
    /// A value to be encoded was not finite (NaN or +/-infinity). Alternator
    /// serializes `FLOAT32VECTOR` elements as JSON numbers, which cannot
    /// represent non-finite values (they would be emitted as `null`).
    NonFiniteValue,
}

impl fmt::Display for Float32VectorError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Float32VectorError::NotBinary => write!(f, "value is not an AttributeValue::B"),
            Float32VectorError::NotAMarker => {
                write!(f, "binary value is not a FLOAT32VECTOR marker")
            }
            Float32VectorError::InvalidPayloadLength { len } => write!(
                f,
                "FLOAT32VECTOR marker payload length {len} is not a multiple of 4 bytes"
            ),
            Float32VectorError::NonFiniteValue => write!(
                f,
                "FLOAT32VECTOR values must be finite (no NaN or infinity)"
            ),
        }
    }
}

impl std::error::Error for Float32VectorError {}

/// Constructs `FLOAT32VECTOR` marker attribute values.
///
/// This is a namespace type; use [`Float32Vector::to_attribute_value`] to
/// build a marker `AttributeValue::B` from `f32` values, and
/// [`Float32VectorExt`] to read one back.
#[derive(Debug, Clone, Copy)]
pub struct Float32Vector;

impl Float32Vector {
    /// Encodes `values` as a marker `AttributeValue::B`. Returns
    /// [`Float32VectorError::NonFiniteValue`] if any value is NaN or
    /// infinite, since Alternator serializes `FLOAT32VECTOR` elements as
    /// JSON numbers, which cannot represent non-finite values (they would
    /// be emitted as `null`).
    ///
    /// The result should only be sent as part of a request: the driver's
    /// interceptor recognizes the marker and rewrites it to
    /// `{"FLOAT32VECTOR": [...]}` in the serialized JSON body immediately
    /// before compression.
    pub fn to_attribute_value(
        values: impl IntoIterator<Item = f32>,
    ) -> Result<AttributeValue, Float32VectorError> {
        let mut bytes = Vec::with_capacity(MAGIC_PREFIX.len());
        bytes.extend_from_slice(MAGIC_PREFIX);
        for v in values {
            if !v.is_finite() {
                return Err(Float32VectorError::NonFiniteValue);
            }
            bytes.extend_from_slice(&v.to_be_bytes());
        }
        Ok(AttributeValue::B(Blob::new(bytes)))
    }

    /// Returns whether `bytes` is a validly-formed marker payload (correct
    /// prefix and a length divisible by `size_of::<f32>()`).
    pub(crate) fn is_marker_bytes(bytes: &[u8]) -> bool {
        bytes.len() >= MAGIC_PREFIX.len()
            && &bytes[..MAGIC_PREFIX.len()] == MAGIC_PREFIX
            && (bytes.len() - MAGIC_PREFIX.len()).is_multiple_of(std::mem::size_of::<f32>())
    }

    /// Decodes marker `bytes` (prefix included) into `f32` values.
    ///
    /// Panics-free: callers must have already validated the bytes with
    /// [`Float32Vector::is_marker_bytes`], as done internally by
    /// [`Float32VectorExt`].
    pub(crate) fn decode_marker_bytes(bytes: &[u8]) -> Vec<f32> {
        bytes[MAGIC_PREFIX.len()..]
            .chunks_exact(std::mem::size_of::<f32>())
            .map(|chunk| f32::from_be_bytes(chunk.try_into().expect("chunk is exactly 4 bytes")))
            .collect()
    }
}

/// A base64 substring guaranteed to be present whenever a marker binary
/// value's raw bytes are base64-encoded into JSON, used as a fast
/// pre-check before paying the cost of a full JSON parse + recursive scan.
///
/// Only the first 6 bytes of the 8-byte magic prefix are used, since
/// base64 3-byte group boundaries guarantee those 6 bytes always encode
/// to the same 8 leading base64 characters regardless of what follows.
pub(crate) fn quick_base64_signature() -> &'static str {
    static SIG: OnceLock<String> = OnceLock::new();
    SIG.get_or_init(|| base64::engine::general_purpose::STANDARD.encode(&MAGIC_PREFIX[..6]))
}

/// Recursively rewrites marker `{"B": "<base64>"}` attribute-value JSON
/// objects into `{"FLOAT32VECTOR": [...]}` throughout `value`, including
/// nested maps, lists, and batch/transaction payloads. Non-marker binary
/// values and all other JSON are left unchanged.
pub(crate) fn rewrite_request_json_markers(value: &mut serde_json::Value) {
    match value {
        serde_json::Value::Object(map) => {
            if map.len() == 1
                && let Some(serde_json::Value::String(b64)) = map.get("B")
                && let Ok(bytes) = base64::engine::general_purpose::STANDARD.decode(b64)
                && Float32Vector::is_marker_bytes(&bytes)
            {
                let values = Float32Vector::decode_marker_bytes(&bytes);
                *value = serde_json::json!({ "FLOAT32VECTOR": values });
                return;
            }
            for v in map.values_mut() {
                rewrite_request_json_markers(v);
            }
        }
        serde_json::Value::Array(arr) => {
            for v in arr.iter_mut() {
                rewrite_request_json_markers(v);
            }
        }
        _ => {}
    }
}

/// Converts the generated SDK's attribute-value representation into its wire
/// JSON shape so extension fields can embed an attribute value directly.
pub(crate) fn attribute_value_to_json(value: &AttributeValue) -> serde_json::Value {
    match value {
        AttributeValue::B(blob) => serde_json::json!({
            "B": base64::engine::general_purpose::STANDARD.encode(blob.as_ref())
        }),
        AttributeValue::N(number) => serde_json::json!({ "N": number }),
        AttributeValue::L(values) => serde_json::json!({
            "L": values.iter().map(attribute_value_to_json).collect::<Vec<_>>()
        }),
        _ => unreachable!("VectorSearch accepts only FLOAT32VECTOR markers or numeric lists"),
    }
}

/// Extension trait providing read access to `FLOAT32VECTOR` marker values
/// without mutating the received `AttributeValue`.
pub trait Float32VectorExt {
    /// Returns whether this value is a valid `FLOAT32VECTOR` marker binary.
    fn is_float32_vector(&self) -> bool;

    /// Decodes this value as a `FLOAT32VECTOR` marker, returning its `f32`
    /// components. This is the inverse of
    /// [`Float32Vector::to_attribute_value`].
    fn float32_vector(&self) -> Result<Vec<f32>, Float32VectorError>;

    /// Decodes this value as a `FLOAT32VECTOR` marker and returns its
    /// components as a standard DynamoDB list of `AttributeValue::N`,
    /// matching the shape of a default (non-preserving) response.
    fn float32_vector_list(&self) -> Result<Vec<AttributeValue>, Float32VectorError>;
}

impl Float32VectorExt for AttributeValue {
    fn is_float32_vector(&self) -> bool {
        matches!(self, AttributeValue::B(blob) if Float32Vector::is_marker_bytes(blob.as_ref()))
    }

    fn float32_vector(&self) -> Result<Vec<f32>, Float32VectorError> {
        let AttributeValue::B(blob) = self else {
            return Err(Float32VectorError::NotBinary);
        };
        let bytes = blob.as_ref();
        if !Float32Vector::is_marker_bytes(bytes) {
            if bytes.len() < MAGIC_PREFIX.len() || &bytes[..MAGIC_PREFIX.len()] != MAGIC_PREFIX {
                return Err(Float32VectorError::NotAMarker);
            }
            return Err(Float32VectorError::InvalidPayloadLength {
                len: bytes.len() - MAGIC_PREFIX.len(),
            });
        }
        Ok(Float32Vector::decode_marker_bytes(bytes))
    }

    fn float32_vector_list(&self) -> Result<Vec<AttributeValue>, Float32VectorError> {
        Ok(self
            .float32_vector()?
            .into_iter()
            .map(|v| AttributeValue::N(v.to_string()))
            .collect())
    }
}

/// Recursively rewrites `{"FLOAT32VECTOR": [...]}` response JSON objects
/// throughout `value` before generated SDK deserialization.
///
/// By default (`preserve == false`) each match becomes a standard
/// `{"L": [{"N": "..."}, ...]}` DynamoDB list, matching the Java driver's
/// default and requiring no Alternator-specific types to read vector data.
/// When `preserve == true`, each match instead becomes a marker
/// `{"B": "<base64>"}` binary value that can be written back unchanged to
/// retain compact storage.
///
/// Ordinary `L` and `B` values elsewhere in the document are never
/// inferred to be vectors and are left unchanged.
pub(crate) fn rewrite_response_json_markers(value: &mut serde_json::Value, preserve: bool) {
    match value {
        serde_json::Value::Object(map) => {
            if map.len() == 1
                && let Some(serde_json::Value::Array(nums)) = map.get("FLOAT32VECTOR")
                && let Some(values) = nums.iter().map(|n| n.as_f64()).collect::<Option<Vec<_>>>()
            {
                *value = if preserve {
                    // `serde_json::Value` numbers are always finite (JSON
                    // has no NaN/infinity), so encoding cannot fail here.
                    let marker =
                        Float32Vector::to_attribute_value(values.into_iter().map(|v| v as f32))
                            .expect("response FLOAT32VECTOR values are always finite JSON numbers");
                    let AttributeValue::B(blob) = &marker else {
                        unreachable!()
                    };
                    let b64 = base64::engine::general_purpose::STANDARD.encode(blob.as_ref());
                    serde_json::json!({ "B": b64 })
                } else {
                    let list: Vec<serde_json::Value> = values
                        .into_iter()
                        .map(|v| serde_json::json!({ "N": format_n(v) }))
                        .collect();
                    serde_json::json!({ "L": list })
                };
                return;
            }
            for v in map.values_mut() {
                rewrite_response_json_markers(v, preserve);
            }
        }
        serde_json::Value::Array(arr) => {
            for v in arr.iter_mut() {
                rewrite_response_json_markers(v, preserve);
            }
        }
        _ => {}
    }
}

/// Formats an f64 (originally an f32) the way DynamoDB `N` values are
/// conventionally rendered: without unnecessary trailing zeros, but never
/// in scientific notation for the magnitudes vectors use.
fn format_n(v: f64) -> String {
    v.to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trips_values() {
        let values = vec![1.0f32, -2.5, 0.0, f32::MAX, f32::MIN];
        let av = Float32Vector::to_attribute_value(values.clone()).unwrap();
        assert!(av.is_float32_vector());
        assert_eq!(av.float32_vector().unwrap(), values);
    }

    #[test]
    fn empty_vector_round_trips() {
        let av = Float32Vector::to_attribute_value(Vec::<f32>::new()).unwrap();
        assert!(av.is_float32_vector());
        assert_eq!(av.float32_vector().unwrap(), Vec::<f32>::new());
    }

    #[test]
    fn float32_vector_list_converts_to_n_values() {
        let av = Float32Vector::to_attribute_value(vec![1.0, 2.0]).unwrap();
        let list = av.float32_vector_list().unwrap();
        assert_eq!(
            list,
            vec![AttributeValue::N("1".into()), AttributeValue::N("2".into())]
        );
    }

    #[test]
    fn rejects_non_binary_values() {
        let av = AttributeValue::S("hello".into());
        assert!(!av.is_float32_vector());
        assert_eq!(av.float32_vector(), Err(Float32VectorError::NotBinary));
    }

    #[test]
    fn rejects_ordinary_binary_without_prefix() {
        let av = AttributeValue::B(Blob::new(vec![1, 2, 3, 4]));
        assert!(!av.is_float32_vector());
        assert_eq!(av.float32_vector(), Err(Float32VectorError::NotAMarker));
    }

    #[test]
    fn rejects_marker_prefix_with_invalid_payload_length() {
        let mut bytes = MAGIC_PREFIX.to_vec();
        bytes.extend_from_slice(&[1, 2, 3]); // not a multiple of 4
        let av = AttributeValue::B(Blob::new(bytes));
        assert!(!av.is_float32_vector());
        assert_eq!(
            av.float32_vector(),
            Err(Float32VectorError::InvalidPayloadLength { len: 3 })
        );
    }

    #[test]
    fn to_attribute_value_rejects_non_finite_values() {
        assert_eq!(
            Float32Vector::to_attribute_value(vec![1.0, f32::NAN]),
            Err(Float32VectorError::NonFiniteValue)
        );
        assert_eq!(
            Float32Vector::to_attribute_value(vec![f32::INFINITY]),
            Err(Float32VectorError::NonFiniteValue)
        );
        assert_eq!(
            Float32Vector::to_attribute_value(vec![f32::NEG_INFINITY]),
            Err(Float32VectorError::NonFiniteValue)
        );
    }

    #[test]
    fn is_marker_bytes_accepts_prefix_only() {
        assert!(Float32Vector::is_marker_bytes(MAGIC_PREFIX));
    }

    #[test]
    fn rewrite_request_json_markers_replaces_marker_binary() {
        let av = Float32Vector::to_attribute_value(vec![1.0, 2.0]).unwrap();
        let AttributeValue::B(blob) = &av else {
            unreachable!()
        };
        let b64 = base64::engine::general_purpose::STANDARD.encode(blob.as_ref());
        let mut json = serde_json::json!({
            "Item": {
                "embedding": { "B": b64 },
                "name": { "S": "hello" }
            }
        });
        rewrite_request_json_markers(&mut json);
        assert_eq!(
            json["Item"]["embedding"],
            serde_json::json!({ "FLOAT32VECTOR": [1.0, 2.0] })
        );
        assert_eq!(json["Item"]["name"], serde_json::json!({ "S": "hello" }));
    }

    #[test]
    fn rewrite_request_json_markers_leaves_ordinary_binary_unchanged() {
        let b64 = base64::engine::general_purpose::STANDARD.encode([1, 2, 3, 4]);
        let mut json = serde_json::json!({ "attr": { "B": b64.clone() } });
        rewrite_request_json_markers(&mut json);
        assert_eq!(json["attr"], serde_json::json!({ "B": b64 }));
    }

    #[test]
    fn quick_base64_signature_matches_encoded_marker() {
        let av = Float32Vector::to_attribute_value(vec![1.0]).unwrap();
        let AttributeValue::B(blob) = &av else {
            unreachable!()
        };
        let b64 = base64::engine::general_purpose::STANDARD.encode(blob.as_ref());
        assert!(b64.starts_with(quick_base64_signature()));
    }
}
