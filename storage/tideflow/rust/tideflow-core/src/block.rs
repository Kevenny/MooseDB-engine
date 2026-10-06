//! Column blocks: one per (series, data column) inside a chunk.
//!
//! ```text
//! block   := encoding:u8 codec:u8 reserved:u16 raw_len:u32 crc32:u32 stored
//! stored  := codec(payload)       crc32 covers the 8 bytes before it + `stored`
//! payload := 0 values                            (no NULLs)
//!          | 1 varint(len) rle(null flags) values
//! ```
//!
//! `values` holds only the non-NULL values, encoded by type:
//!
//! | type             | encoding                         |
//! |------------------|----------------------------------|
//! | TIMESTAMP        | delta-of-delta + Simple8b        |
//! | INT64            | delta + Simple8b                 |
//! | FLOAT64 / FLOAT32| Gorilla XOR                      |
//! | BOOL             | RLE                              |
//! | VARCHAR / DECIMAL| length-prefixed bytes            |
//!
//! The codec (LZ4/ZSTD) is applied only when it makes the block smaller.

use crate::bytes::{put_bytes, put_u16, put_u32, put_u8, put_varint, ByteReader};
use crate::cache::BlockPayload;
use crate::compression::{self, delta, gorilla, rle, Codec, CODEC_NONE};
use crate::error::{corrupt, invalid, Result};
use crate::schema::{ColumnType, Value};

pub(crate) const HEADER_LEN: usize = 12;

const ENC_PLAIN: u8 = 0;
const ENC_DELTA_OF_DELTA: u8 = 1;
const ENC_DELTA: u8 = 2;
const ENC_GORILLA64: u8 = 3;
const ENC_GORILLA32: u8 = 4;
const ENC_BOOL_RLE: u8 = 5;

pub(crate) struct EncodedBlock {
    pub bytes: Vec<u8>,
    /// Size the column would take as a plain, uncompressed array (statistics).
    pub plain_len: usize,
}

fn type_mismatch(ty: ColumnType, v: &Value) -> crate::error::Error {
    invalid(format!("cannot encode {v:?} in a {ty:?} block"))
}

/// Encodes `values` (all of type `ty`, NULLs allowed) and compresses with `codec`.
pub(crate) fn encode(ty: ColumnType, values: &[&Value], codec: Codec) -> Result<EncodedBlock> {
    let mut payload = Vec::new();
    let has_nulls = values.iter().any(|v| v.is_null());
    if has_nulls {
        put_u8(&mut payload, 1);
        let mut runs = Vec::new();
        rle::encode(values.iter().map(|v| v.is_null()), &mut runs);
        put_varint(&mut payload, runs.len() as u64);
        payload.extend_from_slice(&runs);
    } else {
        put_u8(&mut payload, 0);
    }
    let present: Vec<&Value> = values.iter().copied().filter(|v| !v.is_null()).collect();
    let mut plain_len = values.len().div_ceil(8);

    let encoding = match ty {
        ColumnType::Timestamp | ColumnType::Int64 => {
            let ints = present
                .iter()
                .map(|v| match (ty, v) {
                    (ColumnType::Timestamp, Value::Timestamp(x)) | (ColumnType::Int64, Value::Int(x)) => Ok(*x),
                    _ => Err(type_mismatch(ty, v)),
                })
                .collect::<Result<Vec<i64>>>()?;
            plain_len += ints.len() * 8;
            if ty == ColumnType::Timestamp {
                delta::encode_delta_of_delta(&ints, &mut payload);
                ENC_DELTA_OF_DELTA
            } else {
                delta::encode_delta(&ints, &mut payload);
                ENC_DELTA
            }
        }
        ColumnType::Float64 => {
            let bits = present
                .iter()
                .map(|v| if let Value::Float64(f) = v { Ok(f.to_bits()) } else { Err(type_mismatch(ty, v)) })
                .collect::<Result<Vec<u64>>>()?;
            plain_len += bits.len() * 8;
            gorilla::encode(&bits, 64, &mut payload);
            ENC_GORILLA64
        }
        ColumnType::Float32 => {
            let bits = present
                .iter()
                .map(|v| if let Value::Float32(f) = v { Ok(u64::from(f.to_bits())) } else { Err(type_mismatch(ty, v)) })
                .collect::<Result<Vec<u64>>>()?;
            plain_len += bits.len() * 4;
            gorilla::encode(&bits, 32, &mut payload);
            ENC_GORILLA32
        }
        ColumnType::Bool => {
            let bools = present
                .iter()
                .map(|v| if let Value::Bool(b) = v { Ok(*b) } else { Err(type_mismatch(ty, v)) })
                .collect::<Result<Vec<bool>>>()?;
            plain_len += bools.len();
            rle::encode(bools, &mut payload);
            ENC_BOOL_RLE
        }
        ColumnType::Varchar | ColumnType::Decimal => {
            for v in &present {
                let Value::Bytes(b) = v else { return Err(type_mismatch(ty, v)) };
                plain_len += 4 + b.len();
                put_bytes(&mut payload, b);
            }
            ENC_PLAIN
        }
        ColumnType::Tag => return Err(invalid("TAG columns are not stored in data blocks")),
    };

    let raw_len = u32::try_from(payload.len()).map_err(|_| invalid("column block exceeds 4GiB"))?;
    let (codec_id, stored) = match codec {
        Codec::None => (CODEC_NONE, payload),
        c => {
            let compressed = compression::compress(c, &payload)?;
            if compressed.len() < payload.len() {
                (c.id(), compressed)
            } else {
                (CODEC_NONE, payload)
            }
        }
    };
    let mut bytes = Vec::with_capacity(HEADER_LEN + stored.len());
    put_u8(&mut bytes, encoding);
    put_u8(&mut bytes, codec_id);
    put_u16(&mut bytes, 0);
    put_u32(&mut bytes, raw_len);
    let mut h = crc32fast::Hasher::new();
    h.update(&bytes);
    h.update(&stored);
    put_u32(&mut bytes, h.finalize());
    bytes.extend_from_slice(&stored);
    Ok(EncodedBlock { bytes, plain_len })
}

/// Verifies and decompresses a block read from disk (already decrypted).
pub(crate) fn unpack(bytes: &[u8]) -> Result<BlockPayload> {
    let mut r = ByteReader::new(bytes);
    let encoding = r.u8()?;
    let codec = r.u8()?;
    r.u16()?;
    let raw_len = r.u32()? as usize;
    let crc = r.u32()?;
    let stored = r.take(r.remaining())?;
    let mut h = crc32fast::Hasher::new();
    h.update(&bytes[..8]);
    h.update(stored);
    if h.finalize() != crc {
        return Err(corrupt("column block checksum mismatch (corrupt data or wrong key)"));
    }
    let data = compression::decompress(codec, stored, raw_len)?;
    Ok(BlockPayload { encoding, data })
}

/// Decodes `n` values of type `ty` from an unpacked block.
pub(crate) fn decode(ty: ColumnType, n: usize, block: &BlockPayload) -> Result<Vec<Value>> {
    let mut r = ByteReader::new(&block.data);
    let nulls = match r.u8()? {
        0 => None,
        1 => {
            let len = usize::try_from(r.varint()?).map_err(|_| corrupt("null map too large"))?;
            let mut nr = ByteReader::new(r.take(len)?);
            Some(rle::decode(&mut nr, n)?)
        }
        f => return Err(corrupt(format!("invalid null flag {f}"))),
    };
    let present = nulls.as_ref().map_or(n, |m| m.iter().filter(|b| !**b).count());
    let expect = |e: u8| {
        if block.encoding == e {
            Ok(())
        } else {
            Err(corrupt(format!("encoding {} does not match column type {ty:?}", block.encoding)))
        }
    };

    let values: Vec<Value> = match ty {
        ColumnType::Timestamp => {
            expect(ENC_DELTA_OF_DELTA)?;
            delta::decode_delta_of_delta(&mut r, present)?.into_iter().map(Value::Timestamp).collect()
        }
        ColumnType::Int64 => {
            expect(ENC_DELTA)?;
            delta::decode_delta(&mut r, present)?.into_iter().map(Value::Int).collect()
        }
        ColumnType::Float64 => {
            expect(ENC_GORILLA64)?;
            gorilla::decode(r.take(r.remaining())?, present, 64)?
                .into_iter()
                .map(|b| Value::Float64(f64::from_bits(b)))
                .collect()
        }
        ColumnType::Float32 => {
            expect(ENC_GORILLA32)?;
            gorilla::decode(r.take(r.remaining())?, present, 32)?
                .into_iter()
                .map(|b| Value::Float32(f32::from_bits(b as u32)))
                .collect()
        }
        ColumnType::Bool => {
            expect(ENC_BOOL_RLE)?;
            rle::decode(&mut r, present)?.into_iter().map(Value::Bool).collect()
        }
        ColumnType::Varchar | ColumnType::Decimal => {
            expect(ENC_PLAIN)?;
            (0..present).map(|_| Ok(Value::Bytes(r.bytes()?.to_vec()))).collect::<Result<_>>()?
        }
        ColumnType::Tag => return Err(corrupt("tag column stored as data block")),
    };
    if r.remaining() != 0 {
        return Err(corrupt("trailing bytes in column block"));
    }
    if values.len() != present {
        return Err(corrupt("column block value count mismatch"));
    }

    let Some(nulls) = nulls else { return Ok(values) };
    let mut it = values.into_iter();
    nulls
        .into_iter()
        .map(|is_null| if is_null { Ok(Value::Null) } else { it.next().ok_or_else(|| corrupt("null map mismatch")) })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rt(ty: ColumnType, values: Vec<Value>, codec: Codec) -> EncodedBlock {
        let refs: Vec<&Value> = values.iter().collect();
        let b = encode(ty, &refs, codec).unwrap();
        let p = unpack(&b.bytes).unwrap();
        assert_eq!(decode(ty, values.len(), &p).unwrap(), values, "{ty:?} {codec:?}");
        b
    }

    #[test]
    fn every_type_roundtrips_with_every_codec() {
        for codec in [Codec::None, Codec::Lz4, Codec::Zstd(3)] {
            rt(
                ColumnType::Timestamp,
                vec![Value::Timestamp(5), Value::Timestamp(-7), Value::Timestamp(i64::MAX)],
                codec,
            );
            rt(ColumnType::Int64, vec![Value::Int(1), Value::Null, Value::Int(i64::MIN), Value::Null], codec);
            rt(ColumnType::Float64, vec![Value::Float64(1.5), Value::Null, Value::Float64(-0.0)], codec);
            rt(ColumnType::Float32, vec![Value::Float32(1.5), Value::Float32(f32::MAX)], codec);
            rt(ColumnType::Bool, vec![Value::Bool(true), Value::Null, Value::Bool(false), Value::Bool(false)], codec);
            rt(ColumnType::Varchar, vec![Value::Bytes(b"abc".to_vec()), Value::Null, Value::Bytes(vec![])], codec);
            rt(ColumnType::Decimal, vec![Value::Bytes(b"-1.50".to_vec())], codec);
            rt(ColumnType::Float64, vec![Value::Null, Value::Null], codec);
            rt(ColumnType::Int64, vec![], codec);
        }
    }

    #[test]
    fn typical_metrics_compress_well() {
        let n = 10_000;
        let ts: Vec<Value> = (0..n).map(|i| Value::Timestamp(1_700_000_000_000_000 + i * 10_000_000)).collect();
        let vals: Vec<Value> = (0..n).map(|i| Value::Float64(50.0 + ((i / 7) % 20) as f64 * 0.5)).collect();
        let t = rt(ColumnType::Timestamp, ts, Codec::Zstd(3));
        let v = rt(ColumnType::Float64, vals, Codec::Zstd(3));
        assert!(t.bytes.len() * 100 < t.plain_len, "timestamps: {} of {}", t.bytes.len(), t.plain_len);
        assert!(v.bytes.len() * 10 < v.plain_len, "values: {} of {}", v.bytes.len(), v.plain_len);
    }

    #[test]
    fn corruption_and_type_errors() {
        let vals = [Value::Int(1), Value::Int(2)];
        let refs: Vec<&Value> = vals.iter().collect();
        let mut b = encode(ColumnType::Int64, &refs, Codec::None).unwrap().bytes;
        let last = b.len() - 1;
        b[last] ^= 1;
        assert!(unpack(&b).is_err());
        assert!(encode(ColumnType::Int64, &[&Value::Float64(1.0)], Codec::None).is_err());
        let good = encode(ColumnType::Int64, &refs, Codec::None).unwrap().bytes;
        assert!(decode(ColumnType::Float64, 2, &unpack(&good).unwrap()).is_err());
        assert!(decode(ColumnType::Int64, 3, &unpack(&good).unwrap()).is_err());
    }
}
