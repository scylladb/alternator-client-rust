//! Hashes DynamoDB AttributeValue objects using MurmurHash3.
//!
//! Supports the partition key types allowed by ScyllaDB Alternator:
//!
//! * S (String) - Type prefix 0x01 + UTF-8 bytes
//! * N (Number) - Type prefix 0x02 + UTF-8 bytes of string representation
//! * B (Binary) - Type prefix 0x03 + raw bytes
//!
//! Other DynamoDB types (BOOL, NULL, SS, NS, BS, L, M) are not supported as partition keys in
//! Alternator and will return `None`.
//!
//! # Composite Partition Keys
//!
//! This hasher operates on individual `AttributeValue` objects. For tables with composite
//! keys (partition key + sort key), only the partition key should be hashed for routing purposes,
//! since DynamoDB partitions data by partition key only. The sort key determines ordering within a
//! partition but does not affect which node stores the data.
//!
//! Example for a table with composite key (user_id, timestamp):
//!
//! ```rust,ignore
//! // Only hash the partition key (user_id)
//! let partition_key = AttributeValue::S("user_123".to_string());
//! let hash = hash_attribute_value(&partition_key);
//! ```
//!
//! # Number Representation
//!
//! Number values (N type) are hashed using their exact string representation as stored in
//! DynamoDB. This means that numerically equivalent values with different representations will
//! produce different hashes:
//!
//! * `"42"` and `"42.0"` produce different hashes
//! * `"1e2"` and `"100"` produce different hashes
//! * `"1.0"` and `"1.00"` produce different hashes
//!
//! This behavior preserves the exact representation stored in DynamoDB and matches how DynamoDB
//! itself handles number comparisons in certain contexts.
//!
//! # Cross-Language compatibility
//!
//! This hashing implementation is designed to be compatible with other Alternator client
//! libraries (e.g., the Go client). For clients to produce identical hashes for the same partition
//! key values, all implementations must follow the same encoding format:
//!
//! * Type prefixes must use the exact byte values (0x01 for S, 0x02 for N, 0x03 for B)
//! * Strings must be encoded as UTF-8 bytes
//! * The MurmurHash3 implementation must use the x64_128 variant with seed 0, returning the
//!   first 64 bits
//!
//! If you are implementing a compatible hasher in another language, ensure your implementation
//! passes the same test vectors as this Rust implementation.
//!
//! # Performance Characteristics
//!
//! Time complexity is O(n) for all supported types where n is the byte length.
//!
//! Space complexity is O(n) as the entire value is converted to bytes before hashing.

use crate::keyrouting::murmurhash3;
use aws_sdk_dynamodb::types::AttributeValue;

// Type prefix constants to match Java/Go Alternator hashing
const TYPE_STRING: u8 = 0x01;
const TYPE_NUMBER: u8 = 0x02;
const TYPE_BINARY: u8 = 0x03;

/// Computes the cross-language compatible hash for a DynamoDB AttributeValue partition key.
pub fn hash_attribute_value(value: &AttributeValue) -> Option<u64> {
    let (prefix, bytes): (u8, &[u8]) = match value {
        AttributeValue::S(s) => (TYPE_STRING, s.as_bytes()),
        AttributeValue::N(n) => (TYPE_NUMBER, n.as_bytes()),
        AttributeValue::B(b) => (TYPE_BINARY, b.as_ref()),
        _ => return None,
    };
    let mut data = Vec::with_capacity(1 + bytes.len());
    data.push(prefix);
    data.extend_from_slice(bytes);
    Some(murmurhash3::hash(&data))
}

/// Cross-language compatibility tests for hasher.
/// These tests use exact expected hash values from the cross-language specification to ensure
/// Compatibility with implementations in other languages (e.g., Go).
//
// Only S (String), N (Number), and B (Binary) types are tested as these are the only partition
// key types supported by ScyllaDB Alternator.
//
// Test vectors from: https://github.com/scylladb/alternator-load-balancing/issues/165
#[cfg(test)]
mod tests {
    use super::*;
    use aws_sdk_dynamodb::primitives::Blob;
    use aws_sdk_dynamodb::types::AttributeValue;

    // Helper to compare against the spec's signed int64 hash values.
    fn hash_eq(value: &AttributeValue, expected_signed: i64) {
        let actual = hash_attribute_value(value).expect("supported type");
        assert_eq!(
            actual as i64, expected_signed,
            "got {:#018x}, expected {:#018x}",
            actual, expected_signed as u64
        );
    }

    // ----- Strings (partition key supported) -----

    #[test]
    fn spec_string_hello() {
        hash_eq(&AttributeValue::S("hello".into()), 8815023923555918238);
    }

    #[test]
    fn spec_string_empty() {
        hash_eq(&AttributeValue::S("".into()), 8849112093580131862);
    }

    #[test]
    fn spec_string_user_123() {
        hash_eq(&AttributeValue::S("user_123".into()), -4025731529809423594);
    }

    #[test]
    fn spec_string_unicode() {
        hash_eq(
            &AttributeValue::S("こんにちは".into()),
            -8746014667889746860,
        );
    }

    // ----- Numbers (partition key supported) -----

    #[test]
    fn spec_number_42() {
        hash_eq(&AttributeValue::N("42".into()), -5061732451827723051);
    }

    #[test]
    fn spec_number_negative() {
        hash_eq(&AttributeValue::N("-12345".into()), 2496798676881075539);
    }

    #[test]
    fn spec_number_decimal() {
        hash_eq(&AttributeValue::N("3.14159".into()), 2139945193071104172);
    }

    #[test]
    fn spec_number_scientific() {
        hash_eq(&AttributeValue::N("1.23E10".into()), -8571981415737439826);
    }

    // ----- Binary (partition key supported) -----

    #[test]
    fn spec_binary_bytes() {
        hash_eq(
            &AttributeValue::B(Blob::new(vec![0x01, 0x02, 0x03])),
            5026299041734804437,
        );
    }

    #[test]
    fn spec_binary_empty() {
        hash_eq(&AttributeValue::B(Blob::new(vec![])), 8244620721157455449);
    }

    #[test]
    fn spec_binary_high_bytes() {
        hash_eq(
            &AttributeValue::B(Blob::new(vec![0xFF, 0x00, 0x80])),
            14533934253577680,
        );
    }

    // ----- Type collision prevention -----

    #[test]
    fn spec_string_12345_distinct_from_number_12345() {
        // Same bytes, different type prefix → different hash.
        hash_eq(&AttributeValue::S("12345".into()), -6122888897254035317);
        hash_eq(&AttributeValue::N("12345".into()), -3190731486301745196);
    }

    #[test]
    fn spec_binary_12345_distinct_from_string() {
        hash_eq(
            &AttributeValue::B(Blob::new(b"12345".to_vec())),
            -3752463870508600385,
        );
    }

    // ----- Minimal-implementation behavior -----
    // We do not support BOOL/NULL/SS/NS/BS/L/M. Verify they return None.

    #[test]
    fn unsupported_types_return_none() {
        assert!(hash_attribute_value(&AttributeValue::Bool(true)).is_none());
        assert!(hash_attribute_value(&AttributeValue::Null(true)).is_none());
        assert!(hash_attribute_value(&AttributeValue::Ss(vec!["a".into()])).is_none());
        assert!(hash_attribute_value(&AttributeValue::Ns(vec!["1".into()])).is_none());
        assert!(hash_attribute_value(&AttributeValue::Bs(vec![Blob::new(vec![0])])).is_none());
        assert!(hash_attribute_value(&AttributeValue::L(vec![])).is_none());
        assert!(hash_attribute_value(&AttributeValue::M(Default::default())).is_none());
    }

    #[test]
    fn deterministic() {
        let v = AttributeValue::S("alice".into());
        assert_eq!(hash_attribute_value(&v), hash_attribute_value(&v));
    }
}
