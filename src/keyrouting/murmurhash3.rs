//! MurmurHash3 x64 128-bit implementation returning the first 64 bits.
//!
//! This implementation is used to hash partition keys for deterministic node routing. It produces
//! the same hash values as the Go implementation for compatibility with other Alternator clients.
//!
//! Based on the original MurmurHash3 algorithm by Austin Appleby and the gocql implementation
//! (Apache 2.0 / BSD-3-Clause licensed)

const C1: u64 = 0x87c37b91114253d5;
const C2: u64 = 0x4cf5ad432745937f;
const FMIX1: u64 = 0xff51afd7ed558ccd;
const FMIX2: u64 = 0xc4ceb9fe1a85ec53;

/// Computes MurmurHash3 x64 128-bit hash and returns the first 64 bits (h1).
pub fn hash(data: &[u8]) -> u64 {
    let mut h1: u64 = 0;
    let mut h2: u64 = 0;

    let mut chunks = data.chunks_exact(16);

    // Body - process 16-byte blocks
    for chunk in chunks.by_ref() {
        // Read 8 bytes in little-endian order for k1 and k2
        let mut k1 = u64::from_le_bytes(chunk[0..8].try_into().unwrap());
        let mut k2 = u64::from_le_bytes(chunk[8..16].try_into().unwrap());

        k1 = k1.wrapping_mul(C1);
        k1 = k1.rotate_left(31);
        k1 = k1.wrapping_mul(C2);
        h1 ^= k1;

        h1 = h1.rotate_left(27);
        h1 = h1.wrapping_add(h2);
        h1 = h1.wrapping_mul(5).wrapping_add(0x52dce729);

        k2 = k2.wrapping_mul(C2);
        k2 = k2.rotate_left(33);
        k2 = k2.wrapping_mul(C1);
        h2 ^= k2;

        h2 = h2.rotate_left(31);
        h2 = h2.wrapping_add(h1);
        h2 = h2.wrapping_mul(5).wrapping_add(0x38495ab5);
    }

    // Tail - handle remaining bytes
    let tail = chunks.remainder();
    let mut k1: u64 = 0;
    let mut k2: u64 = 0;

    if tail.len() >= 9 {
        // Handle bytes 8 through 14 for k2
        for (i, &b) in tail[8..].iter().enumerate() {
            k2 ^= (b as u64) << (i * 8);
        }

        k2 = k2.wrapping_mul(C2);
        k2 = k2.rotate_left(33);
        k2 = k2.wrapping_mul(C1);
        h2 ^= k2;
    }

    if !tail.is_empty() {
        // Handle bytes 0 through 7 for k1
        let k1_bytes = &tail[..tail.len().min(8)];
        for (i, &b) in k1_bytes.iter().enumerate() {
            k1 ^= (b as u64) << (i * 8);
        }

        k1 = k1.wrapping_mul(C1);
        k1 = k1.rotate_left(31);
        k1 = k1.wrapping_mul(C2);
        h1 ^= k1;
    }

    // Finalization
    let len = data.len() as u64;
    h1 ^= len;
    h2 ^= len;

    h1 = h1.wrapping_add(h2);
    h2 = h2.wrapping_add(h1);

    h1 = fmix64(h1);
    h2 = fmix64(h2);

    h1 = h1.wrapping_add(h2);
    // h2 = h2.wrapping_add(h1); (omitted since h2 is discarded)

    h1
}

fn fmix64(mut k: u64) -> u64 {
    k ^= k >> 33;
    k = k.wrapping_mul(FMIX1);
    k ^= k >> 33;
    k = k.wrapping_mul(FMIX2);
    k ^= k >> 33;
    k
}

/// Tests to verify that Rust implementation produces the same output as the Go reference implementation.
#[cfg(test)]
mod tests {
    use super::hash;

    // ----- Cross-language compatibility (verified against Go murmur3) -----

    #[test]
    fn go_compatibility_empty_input() {
        // Empty input with seed 0 produces h1 = 0.
        assert_eq!(hash(&[]), 0);
    }

    #[test]
    fn go_compatibility_test() {
        assert_eq!(hash(b"test"), 0xac7d28cc74bde19d_u64);
    }

    #[test]
    fn go_compatibility_hello() {
        assert_eq!(hash(b"hello"), 0xcbd8a7b341bd9b02_u64);
    }

    #[test]
    fn go_compatibility_user_123() {
        assert_eq!(hash(b"user_123"), 0x104832bf621f0137_u64);
    }

    #[test]
    fn go_compatibility_exactly_16_bytes() {
        assert_eq!(hash(b"0123456789abcdef"), 0x4be06d94cf4ad1a7_u64,);
    }

    #[test]
    fn go_compatibility_high_bytes() {
        let data = [0xFF, 0x80, 0x7F, 0x00];
        assert_eq!(hash(&data), 0x3408b0fbe4cb130c_u64);
    }

    // ----- Block / tail boundary coverage -----

    #[test]
    fn empty_byte_array() {
        assert_eq!(hash(&[]), 0);
    }

    #[test]
    fn single_byte() {
        assert_ne!(hash(&[0x42]), 0);
    }

    #[test]
    fn short_string() {
        assert_ne!(hash(b"hello"), 0);
    }

    #[test]
    fn exactly_16_bytes_is_one_block() {
        let data = b"0123456789abcdef";
        assert_eq!(data.len(), 16);
        assert_ne!(hash(data), 0);
    }

    #[test]
    fn seventeen_bytes_one_block_plus_tail() {
        let data = b"0123456789abcdefg";
        assert_eq!(data.len(), 17);
        assert_ne!(hash(data), 0);
    }

    #[test]
    fn longer_string() {
        assert_ne!(hash(b"this is a longer string that exceeds 16 bytes"), 0);
    }

    #[test]
    fn thirty_two_bytes_two_full_blocks() {
        let data = b"01234567890123456789012345678901";
        assert_eq!(data.len(), 32);
        assert_ne!(hash(data), 0);
    }

    #[test]
    fn forty_eight_bytes_three_full_blocks() {
        let data = b"012345678901234567890123456789012345678901234567";
        assert_eq!(data.len(), 48);
        assert_ne!(hash(data), 0);
    }

    #[test]
    fn all_tail_lengths_one_through_fifteen() {
        // Exercise every tail-handling code path: a full 16-byte block
        // followed by 1, 2, 3, ..., 15 extra bytes.
        for tail_len in 1..16 {
            let data: Vec<u8> = (0..(16 + tail_len) as u8).collect();
            assert_ne!(
                hash(&data),
                0,
                "tail length {tail_len} should produce non-zero hash",
            );
        }
    }

    #[test]
    fn binary_data_with_null_and_high_bytes() {
        let data = [0x00, 0x01, 0x02, 0xFF, 0xFE, 0xFD];
        assert_ne!(hash(&data), 0);
    }

    #[test]
    fn quick_brown_fox() {
        let data = b"The quick brown fox jumps over the lazy dog";
        let h1 = hash(data);
        let h2 = hash(data);
        assert_eq!(h1, h2);
    }

    // ----- Properties -----

    #[test]
    fn deterministic() {
        let data = b"partition_key_value";
        assert_eq!(hash(data), hash(data));
    }

    #[test]
    fn different_inputs_produce_different_hashes() {
        assert_ne!(hash(b"user_123"), hash(b"user_456"));
    }

    #[test]
    fn partition_key_simulation() {
        for key in [
            "user_id12345",
            "order_98765",
            "pk_abc_123",
            "session_xyz_789",
        ] {
            let h = hash(key.as_bytes());
            assert_ne!(h, 0, "key {key:?} produced zero hash");
        }
    }
}
