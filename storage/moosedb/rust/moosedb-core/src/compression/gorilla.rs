//! Gorilla XOR encoding of floating point values (Pelkonen et al., VLDB 2015).
//!
//! The first value is stored verbatim; each following value is XORed with
//! its predecessor. Identical values cost one bit; slowly changing values
//! share sign/exponent bits and only the "meaningful" middle bits are stored.
//! Works on the IEEE-754 bit pattern, so every value (NaN payloads, -0.0,
//! infinities) round-trips exactly. `width` is 64 for f64 and 32 for f32.

use crate::error::{corrupt, Result};

struct BitWriter {
    buf: Vec<u8>,
    /// Bits already used in the last byte (0 = start a new byte).
    used: u32,
}

impl BitWriter {
    fn bit(&mut self, b: bool) {
        if self.used == 0 {
            self.buf.push(0);
        }
        if b {
            if let Some(last) = self.buf.last_mut() {
                *last |= 0x80 >> self.used;
            }
        }
        self.used = (self.used + 1) % 8;
    }

    fn bits(&mut self, v: u64, n: u32) {
        for i in (0..n).rev() {
            self.bit((v >> i) & 1 == 1);
        }
    }
}

struct BitReader<'a> {
    data: &'a [u8],
    pos: usize,
}

impl BitReader<'_> {
    fn bit(&mut self) -> Result<bool> {
        let byte = self.data.get(self.pos / 8).ok_or_else(|| corrupt("gorilla stream truncated"))?;
        let b = byte & (0x80 >> (self.pos % 8)) != 0;
        self.pos += 1;
        Ok(b)
    }

    fn bits(&mut self, n: u32) -> Result<u64> {
        let mut v = 0u64;
        for _ in 0..n {
            v = (v << 1) | u64::from(self.bit()?);
        }
        Ok(v)
    }
}

pub(crate) fn encode(values: &[u64], width: u32, out: &mut Vec<u8>) {
    let Some(&first) = values.first() else { return };
    let mut w = BitWriter { buf: Vec::with_capacity(values.len() * 2), used: 0 };
    w.bits(first, width);
    let mut prev = first;
    let mut window: Option<(u32, u32)> = None; // (leading, trailing) of the last stored XOR
    for &v in &values[1..] {
        let x = v ^ prev;
        prev = v;
        if x == 0 {
            w.bit(false);
            continue;
        }
        w.bit(true);
        let lead = (x.leading_zeros() - (64 - width)).min(31);
        let trail = x.trailing_zeros();
        match window {
            Some((pl, pt)) if lead >= pl && trail >= pt => {
                w.bit(false);
                w.bits(x >> pt, width - pl - pt);
            }
            _ => {
                w.bit(true);
                let len = width - lead - trail;
                w.bits(u64::from(lead), 5);
                w.bits(u64::from(len - 1), 6);
                w.bits(x >> trail, len);
                window = Some((lead, trail));
            }
        }
    }
    out.extend_from_slice(&w.buf);
}

pub(crate) fn decode(data: &[u8], count: usize, width: u32) -> Result<Vec<u64>> {
    if count == 0 {
        return if data.is_empty() { Ok(Vec::new()) } else { Err(corrupt("gorilla stream for zero values")) };
    }
    let mut r = BitReader { data, pos: 0 };
    let mut out = Vec::with_capacity(count.min(data.len() * 8));
    let mut prev = r.bits(width)?;
    out.push(prev);
    let mut window: Option<(u32, u32)> = None;
    while out.len() < count {
        if r.bit()? {
            let (lead, len) = if r.bit()? {
                let lead = r.bits(5)? as u32;
                let len = r.bits(6)? as u32 + 1;
                if lead + len > width {
                    return Err(corrupt("gorilla window exceeds value width"));
                }
                window = Some((lead, width - lead - len));
                (lead, len)
            } else {
                let (pl, pt) = window.ok_or_else(|| corrupt("gorilla reuses a window before defining one"))?;
                (pl, width - pl - pt)
            };
            let trail = width - lead - len;
            prev ^= r.bits(len)? << trail;
        }
        out.push(prev);
    }
    // Only zero padding may follow the last value.
    if r.pos.div_ceil(8) != data.len() {
        return Err(corrupt("trailing bytes after gorilla stream"));
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rt64(values: &[f64]) -> usize {
        let bits: Vec<u64> = values.iter().map(|v| v.to_bits()).collect();
        let mut buf = Vec::new();
        encode(&bits, 64, &mut buf);
        assert_eq!(decode(&buf, bits.len(), 64).unwrap(), bits);
        buf.len()
    }

    #[test]
    fn slowly_changing_series_compress() {
        let vals: Vec<f64> = (0..10_000).map(|i| 20.0 + ((i / 50) as f64) * 0.25).collect();
        let n = rt64(&vals);
        assert!(n < 10_000 * 8 / 20, "{n} bytes");
    }

    #[test]
    fn special_values_roundtrip() {
        rt64(&[]);
        rt64(&[1.0]);
        rt64(&[f64::NAN, -0.0, 0.0, f64::INFINITY, f64::NEG_INFINITY, f64::MIN_POSITIVE, f64::MAX, 1e-300]);
        rt64(&(0..1000).map(|i| (i as f64).sin()).collect::<Vec<_>>());
    }

    #[test]
    fn f32_width() {
        let vals: Vec<u64> =
            [1.5f32, 1.5, -2.25, f32::NAN, 0.1, 3.4e38].iter().map(|v| u64::from(v.to_bits())).collect();
        let mut buf = Vec::new();
        encode(&vals, 32, &mut buf);
        assert_eq!(decode(&buf, vals.len(), 32).unwrap(), vals);
    }

    #[test]
    fn truncation_detected() {
        let bits: Vec<u64> = (0..100).map(|i| (i as f64 * 1.1).to_bits()).collect();
        let mut buf = Vec::new();
        encode(&bits, 64, &mut buf);
        assert!(decode(&buf[..buf.len() / 2], bits.len(), 64).is_err());
    }
}
