//! On-disk chunk format (`.tfl`).
//!
//! ```text
//! ┌─────────────────────────── Header (64 bytes) ────────────────────────────┐
//! │ 0  magic "TFLW"        4  version:u16      6  flags:u16                  │
//! │ 8  ts_min:i64          16 ts_max:i64       24 row_count:u64              │
//! │ 32 series_count:u32    36 column_count:u32 40 chunk_id:u64               │
//! │ 48 wal_seq:u64         56 schema_fp:u32    60 header_crc:u32 (of 0..60)  │
//! ├──────────────────────────── Data blocks ─────────────────────────────────┤
//! │ per series, one column block per data (non-tag) column, schema order     │
//! ├──────────────────────────── Series index ────────────────────────────────┤
//! │ raw_bytes:u64 bucket_start:i64 bucket_end:i64 series_count:u32           │
//! │ per series: id:u64 rows:u32 ts_min:i64 ts_max:i64                        │
//! │             tag_count:u16 tag values (codec)                             │
//! │             block_count:u16 (offset:u64 len:u32) * block_count          │
//! ├──────────────────────────── Bloom filter ────────────────────────────────┤
//! │ hashes:u8 word_count:u32 words:u64*                                      │
//! ├──────────────────────────── Footer (24 bytes) ───────────────────────────┤
//! │ index_offset:u64 bloom_offset:u64 file_crc:u32 (of 0..footer) "TFLE"     │
//! └──────────────────────────────────────────────────────────────────────────┘
//! ```
//!
//! Column block:
//!
//! ```text
//! encoding:u8 compression:u8 raw_len:u32 payload
//! payload (PLAIN, uncompressed) := null_bitmap[ceil(n/8)] values-of-non-null-rows
//! ```

use std::path::PathBuf;

use crate::bytes::{put_bytes, put_i64, put_u32, put_u64, put_u8, ByteReader};
use crate::error::{corrupt, invalid, Result};
use crate::index::bloom::Bloom;
use crate::schema::{ColumnType, Value};

pub(crate) const MAGIC: &[u8; 4] = b"TFLW";
pub(crate) const FOOTER_MAGIC: &[u8; 4] = b"TFLE";
pub(crate) const VERSION: u16 = 1;
pub(crate) const HEADER_LEN: usize = 64;
pub(crate) const FOOTER_LEN: usize = 24;

pub mod flags {
    pub const COMPRESSED: u16 = 1 << 0;
    pub const ENCRYPTED: u16 = 1 << 1;
    pub const SEALED: u16 = 1 << 2;
}

const ENCODING_PLAIN: u8 = 0;
const COMPRESSION_NONE: u8 = 0;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ChunkHeader {
    pub version: u16,
    pub flags: u16,
    pub ts_min: i64,
    pub ts_max: i64,
    pub row_count: u64,
    pub series_count: u32,
    pub column_count: u32,
    pub chunk_id: u64,
    pub wal_seq: u64,
    pub schema_fingerprint: u32,
}

impl ChunkHeader {
    pub(crate) fn encode(&self) -> [u8; HEADER_LEN] {
        let mut b = Vec::with_capacity(HEADER_LEN);
        b.extend_from_slice(MAGIC);
        b.extend_from_slice(&self.version.to_le_bytes());
        b.extend_from_slice(&self.flags.to_le_bytes());
        put_i64(&mut b, self.ts_min);
        put_i64(&mut b, self.ts_max);
        put_u64(&mut b, self.row_count);
        put_u32(&mut b, self.series_count);
        put_u32(&mut b, self.column_count);
        put_u64(&mut b, self.chunk_id);
        put_u64(&mut b, self.wal_seq);
        put_u32(&mut b, self.schema_fingerprint);
        let crc = crc32fast::hash(&b);
        put_u32(&mut b, crc);
        let mut out = [0u8; HEADER_LEN];
        out.copy_from_slice(&b);
        out
    }

    pub(crate) fn decode(b: &[u8]) -> Result<ChunkHeader> {
        if b.len() < HEADER_LEN {
            return Err(corrupt("chunk header truncated"));
        }
        let b = &b[..HEADER_LEN];
        if crc32fast::hash(&b[..60]).to_le_bytes() != b[60..64] {
            return Err(corrupt("chunk header checksum mismatch"));
        }
        let mut r = ByteReader::new(b);
        if r.take(4)? != MAGIC {
            return Err(corrupt("chunk bad magic"));
        }
        let h = ChunkHeader {
            version: r.u16()?,
            flags: r.u16()?,
            ts_min: r.i64()?,
            ts_max: r.i64()?,
            row_count: r.u64()?,
            series_count: r.u32()?,
            column_count: r.u32()?,
            chunk_id: r.u64()?,
            wal_seq: r.u64()?,
            schema_fingerprint: r.u32()?,
        };
        if h.version != VERSION {
            return Err(corrupt(format!("chunk format version {} not supported", h.version)));
        }
        Ok(h)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct BlockRef {
    pub offset: u64,
    pub len: u32,
}

#[derive(Clone, Debug, PartialEq)]
pub struct SeriesEntry {
    pub series_id: u64,
    pub row_count: u32,
    pub ts_min: i64,
    pub ts_max: i64,
    /// Tag values in schema tag order.
    pub tags: Vec<Value>,
    /// One block per data column, in `Schema::data_indices` order.
    pub blocks: Vec<BlockRef>,
}

/// Everything about a chunk except its data blocks. Kept in memory for every
/// live chunk.
#[derive(Clone, Debug)]
pub struct ChunkMeta {
    pub id: u64,
    pub path: PathBuf,
    pub header: ChunkHeader,
    /// Half-open time bucket `[start, end)` this chunk belongs to.
    pub bucket: (i64, i64),
    /// Size of the column payloads before compression.
    pub raw_bytes: u64,
    pub file_size: u64,
    pub series: Vec<SeriesEntry>,
    pub bloom: Bloom,
}

impl ChunkMeta {
    pub fn file_name(&self) -> String {
        self.path.file_name().map(|n| n.to_string_lossy().into_owned()).unwrap_or_default()
    }

    pub fn overlaps(&self, lo: i64, hi: i64) -> bool {
        self.header.ts_min <= hi && self.header.ts_max >= lo
    }
}

pub(crate) fn chunk_file_name(bucket: (i64, i64), id: u64) -> String {
    format!("chunk_{}_{}_{id:06}.tfl", crate::time::format_compact(bucket.0), crate::time::format_compact(bucket.1))
}

/// Extracts the chunk id from a file name produced by `chunk_file_name`.
pub(crate) fn parse_chunk_id(name: &str) -> Option<u64> {
    let stem = name.strip_prefix("chunk_")?.strip_suffix(".tfl")?;
    stem.rsplit('_').next()?.parse().ok()
}

pub(crate) fn encode_series_entry(buf: &mut Vec<u8>, e: &SeriesEntry) {
    put_u64(buf, e.series_id);
    put_u32(buf, e.row_count);
    put_i64(buf, e.ts_min);
    put_i64(buf, e.ts_max);
    buf.extend_from_slice(&(e.tags.len() as u16).to_le_bytes());
    for t in &e.tags {
        crate::codec::encode_value(buf, t);
    }
    buf.extend_from_slice(&(e.blocks.len() as u16).to_le_bytes());
    for b in &e.blocks {
        put_u64(buf, b.offset);
        put_u32(buf, b.len);
    }
}

pub(crate) fn decode_series_entry(r: &mut ByteReader<'_>) -> Result<SeriesEntry> {
    let series_id = r.u64()?;
    let row_count = r.u32()?;
    let ts_min = r.i64()?;
    let ts_max = r.i64()?;
    let ntags = r.u16()? as usize;
    let tags = (0..ntags).map(|_| crate::codec::decode_value(r)).collect::<Result<Vec<_>>>()?;
    let nblocks = r.u16()? as usize;
    let blocks = (0..nblocks).map(|_| Ok(BlockRef { offset: r.u64()?, len: r.u32()? })).collect::<Result<Vec<_>>>()?;
    Ok(SeriesEntry { series_id, row_count, ts_min, ts_max, tags, blocks })
}

/// Encodes one column of `values` (all of type `ty`) as a PLAIN block.
/// Returns the block bytes and the raw payload size.
pub(crate) fn encode_block<'a>(
    ty: ColumnType,
    values: impl ExactSizeIterator<Item = &'a Value>,
) -> Result<(Vec<u8>, usize)> {
    let n = values.len();
    let mut nulls = vec![0u8; n.div_ceil(8)];
    let mut data = Vec::new();
    for (i, v) in values.enumerate() {
        match (ty, v) {
            (_, Value::Null) => {
                if let Some(byte) = nulls.get_mut(i / 8) {
                    *byte |= 1 << (i % 8);
                }
            }
            (ColumnType::Timestamp, Value::Timestamp(x)) | (ColumnType::Int64, Value::Int(x)) => put_i64(&mut data, *x),
            (ColumnType::Float64, Value::Float64(x)) => put_u64(&mut data, x.to_bits()),
            (ColumnType::Float32, Value::Float32(x)) => put_u32(&mut data, x.to_bits()),
            (ColumnType::Bool, Value::Bool(x)) => put_u8(&mut data, u8::from(*x)),
            (ColumnType::Varchar | ColumnType::Decimal, Value::Bytes(b)) => put_bytes(&mut data, b),
            (ty, v) => return Err(invalid(format!("cannot encode {v:?} in a {ty:?} block"))),
        }
    }
    let raw_len = nulls.len() + data.len();
    let raw_len_u32 = u32::try_from(raw_len).map_err(|_| invalid("column block exceeds 4GiB"))?;
    let mut block = Vec::with_capacity(6 + raw_len);
    put_u8(&mut block, ENCODING_PLAIN);
    put_u8(&mut block, COMPRESSION_NONE);
    put_u32(&mut block, raw_len_u32);
    block.extend_from_slice(&nulls);
    block.extend_from_slice(&data);
    Ok((block, raw_len))
}

/// Decodes a block of `n` values of type `ty`.
pub(crate) fn decode_block(ty: ColumnType, n: usize, block: &[u8]) -> Result<Vec<Value>> {
    let mut r = ByteReader::new(block);
    let encoding = r.u8()?;
    let compression = r.u8()?;
    let raw_len = r.u32()? as usize;
    if encoding != ENCODING_PLAIN || compression != COMPRESSION_NONE {
        return Err(corrupt(format!("unknown block encoding {encoding}/{compression}")));
    }
    if raw_len != r.remaining() {
        return Err(corrupt("block length mismatch"));
    }
    let nulls = r.take(n.div_ceil(8))?;
    let mut out = Vec::with_capacity(n);
    for i in 0..n {
        if nulls[i / 8] & (1 << (i % 8)) != 0 {
            out.push(Value::Null);
            continue;
        }
        out.push(match ty {
            ColumnType::Timestamp => Value::Timestamp(r.i64()?),
            ColumnType::Int64 => Value::Int(r.i64()?),
            ColumnType::Float64 => Value::Float64(f64::from_bits(r.u64()?)),
            ColumnType::Float32 => Value::Float32(f32::from_bits(r.u32()?)),
            ColumnType::Bool => Value::Bool(r.u8()? != 0),
            ColumnType::Varchar | ColumnType::Decimal => Value::Bytes(r.bytes()?.to_vec()),
            ColumnType::Tag => return Err(corrupt("tag column stored as data block")),
        });
    }
    if r.remaining() != 0 {
        return Err(corrupt("trailing bytes in block"));
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn header_roundtrip_and_crc() {
        let h = ChunkHeader {
            version: VERSION,
            flags: flags::SEALED,
            ts_min: -5,
            ts_max: 99,
            row_count: 1234,
            series_count: 3,
            column_count: 4,
            chunk_id: 77,
            wal_seq: 12,
            schema_fingerprint: 0xdead_beef,
        };
        let mut b = h.encode();
        assert_eq!(ChunkHeader::decode(&b).unwrap(), h);
        b[9] ^= 1;
        assert!(ChunkHeader::decode(&b).is_err());
    }

    #[test]
    fn block_roundtrip_with_nulls() {
        let vals = vec![Value::Float64(1.5), Value::Null, Value::Float64(-0.0), Value::Null, Value::Float64(f64::MAX)];
        let (block, raw) = encode_block(ColumnType::Float64, vals.iter()).unwrap();
        assert_eq!(raw, 1 + 3 * 8);
        assert_eq!(decode_block(ColumnType::Float64, vals.len(), &block).unwrap(), vals);

        let strs = vec![Value::Bytes(b"a".to_vec()), Value::Bytes(vec![]), Value::Null];
        let (block, _) = encode_block(ColumnType::Varchar, strs.iter()).unwrap();
        assert_eq!(decode_block(ColumnType::Varchar, 3, &block).unwrap(), strs);
    }

    #[test]
    fn block_type_mismatch_rejected() {
        assert!(encode_block(ColumnType::Int64, [Value::Float64(1.0)].iter()).is_err());
    }

    #[test]
    fn truncated_block_is_corrupt() {
        let (block, _) = encode_block(ColumnType::Int64, [Value::Int(1), Value::Int(2)].iter()).unwrap();
        assert!(decode_block(ColumnType::Int64, 2, &block[..block.len() - 1]).is_err());
        assert!(decode_block(ColumnType::Int64, 3, &block).is_err());
    }

    #[test]
    fn file_names() {
        let name = chunk_file_name((0, crate::time::MICROS_PER_DAY), 42);
        assert_eq!(name, "chunk_19700101T000000_19700102T000000_000042.tfl");
        assert_eq!(parse_chunk_id(&name), Some(42));
        assert_eq!(parse_chunk_id("chunk_x.tfl.corrupt"), None);
        assert_eq!(parse_chunk_id("MANIFEST"), None);
    }
}
