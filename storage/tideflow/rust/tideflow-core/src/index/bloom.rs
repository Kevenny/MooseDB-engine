//! Per-chunk Bloom filter over series ids.

use crate::bytes::{put_u32, put_u64, put_u8, ByteReader};
use crate::error::{corrupt, Result};

const MAX_HASHES: u8 = 16;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Bloom {
    hashes: u8,
    words: Vec<u64>,
}

/// SplitMix64 finalizer: cheap, well-distributed mixing of 64-bit keys.
fn mix(mut z: u64) -> u64 {
    z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    z ^ (z >> 31)
}

impl Bloom {
    /// Sized for `n` keys at false-positive rate `fpr` (clamped to [1e-6, 0.5]).
    pub fn with_capacity(n: usize, fpr: f64) -> Bloom {
        let fpr = fpr.clamp(1e-6, 0.5);
        let n = n.max(1) as f64;
        let ln2 = std::f64::consts::LN_2;
        let bits = (-(n * fpr.ln()) / (ln2 * ln2)).ceil().max(64.0);
        let hashes = ((bits / n) * ln2).round().clamp(1.0, f64::from(MAX_HASHES)) as u8;
        let words = vec![0u64; (bits as usize).div_ceil(64)];
        Bloom { hashes, words }
    }

    fn bit_positions(&self, key: u64) -> impl Iterator<Item = usize> {
        // Kirsch–Mitzenmacher double hashing.
        let h1 = mix(key);
        let h2 = mix(h1 ^ 0x9e37_79b9_7f4a_7c15) | 1;
        let nbits = (self.words.len() * 64) as u64;
        (0..u64::from(self.hashes)).map(move |i| (h1.wrapping_add(i.wrapping_mul(h2)) % nbits) as usize)
    }

    pub fn insert(&mut self, key: u64) {
        let positions: Vec<usize> = self.bit_positions(key).collect();
        for p in positions {
            if let Some(w) = self.words.get_mut(p / 64) {
                *w |= 1 << (p % 64);
            }
        }
    }

    pub fn may_contain(&self, key: u64) -> bool {
        self.bit_positions(key).all(|p| self.words.get(p / 64).is_some_and(|w| w & (1 << (p % 64)) != 0))
    }

    pub(crate) fn encode(&self, buf: &mut Vec<u8>) {
        put_u8(buf, self.hashes);
        put_u32(buf, self.words.len() as u32);
        for w in &self.words {
            put_u64(buf, *w);
        }
    }

    pub(crate) fn decode(r: &mut ByteReader<'_>) -> Result<Bloom> {
        let hashes = r.u8()?;
        let n = r.u32()? as usize;
        if hashes == 0 || hashes > MAX_HASHES || n == 0 || n.saturating_mul(8) > r.remaining() {
            return Err(corrupt("invalid bloom filter header"));
        }
        let words = (0..n).map(|_| r.u64()).collect::<Result<Vec<_>>>()?;
        Ok(Bloom { hashes, words })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn no_false_negatives_and_low_fpr() {
        let mut b = Bloom::with_capacity(1000, 0.01);
        for k in 0..1000u64 {
            b.insert(k * 7919);
        }
        assert!((0..1000u64).all(|k| b.may_contain(k * 7919)));
        let fp = (1_000_000..1_010_000u64).filter(|&k| b.may_contain(k)).count();
        assert!(fp < 300, "false positives: {fp}/10000");
    }

    #[test]
    fn encode_roundtrip() {
        let mut b = Bloom::with_capacity(10, 0.01);
        b.insert(42);
        let mut buf = Vec::new();
        b.encode(&mut buf);
        let d = Bloom::decode(&mut ByteReader::new(&buf)).unwrap();
        assert_eq!(d, b);
        assert!(d.may_contain(42));
    }
}
