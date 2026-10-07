//! Run-length encoding of boolean sequences (BOOL columns, NULL bitmaps).
//!
//! ```text
//! stream := varint(run)*     runs alternate false, true, false, ... (first may be 0)
//! ```

use crate::bytes::{put_varint, ByteReader};
use crate::error::{corrupt, Result};

pub(crate) fn encode(bits: impl IntoIterator<Item = bool>, out: &mut Vec<u8>) {
    let mut current = false;
    let mut run = 0u64;
    for b in bits {
        if b != current {
            put_varint(out, run);
            current = b;
            run = 0;
        }
        run += 1;
    }
    if run > 0 {
        put_varint(out, run);
    }
}

pub(crate) fn decode(r: &mut ByteReader<'_>, count: usize) -> Result<Vec<bool>> {
    // `count` comes from the file: start small and let the runs grow the vector,
    // and never grow past the decode limit (a run of a few bytes can be huge).
    if count > super::MAX_DECODE_BYTES {
        return Err(corrupt(format!("RLE stream claims {count} values")));
    }
    let mut out = Vec::with_capacity(count.min(r.remaining().saturating_mul(64)));
    let mut current = false;
    while out.len() < count {
        let run = usize::try_from(r.varint()?).map_err(|_| corrupt("RLE run too long"))?;
        if out.len().checked_add(run).map_or(true, |end| end > count) {
            return Err(corrupt("RLE runs exceed the value count"));
        }
        out.resize(out.len() + run, current);
        current = !current;
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rt(bits: &[bool]) -> usize {
        let mut buf = Vec::new();
        encode(bits.iter().copied(), &mut buf);
        let mut r = ByteReader::new(&buf);
        assert_eq!(decode(&mut r, bits.len()).unwrap(), bits);
        assert_eq!(r.remaining(), 0);
        buf.len()
    }

    #[test]
    fn roundtrips() {
        rt(&[]);
        rt(&[true]);
        rt(&[false, false, true, true, true, false]);
        assert_eq!(rt(&vec![false; 100_000]), 3);
        assert_eq!(rt(&[true; 10]), 2);
    }

    #[test]
    fn forged_runs_and_counts_are_rejected() {
        // A single run of 128M against a small expected count.
        let mut buf = Vec::new();
        put_varint(&mut buf, 0);
        put_varint(&mut buf, 128 << 20);
        assert!(decode(&mut ByteReader::new(&buf), 1000).is_err());
        // An absurd expected count with a tiny stream must fail, not allocate.
        assert!(decode(&mut ByteReader::new(&buf[..1]), usize::MAX).is_err());
        assert!(decode(&mut ByteReader::new(&[]), usize::MAX).is_err());
    }

    #[test]
    fn overlong_runs_rejected() {
        let mut buf = Vec::new();
        encode([false; 10], &mut buf);
        assert!(decode(&mut ByteReader::new(&buf), 5).is_err());
    }
}
