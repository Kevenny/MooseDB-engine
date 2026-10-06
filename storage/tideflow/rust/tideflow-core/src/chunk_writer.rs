//! Serialization of a group of series into a sealed chunk file.

use std::collections::BTreeMap;
use std::fs::{self, File};
use std::io::Write;
use std::path::Path;

use crate::bytes::{put_i64, put_u32, put_u64};
use crate::chunk::{
    chunk_file_name, encode_block, encode_series_entry, flags, BlockRef, ChunkHeader, ChunkMeta, SeriesEntry,
    FOOTER_MAGIC, HEADER_LEN, VERSION,
};
use crate::error::{invalid, Result};
use crate::index::bloom::Bloom;
use crate::index::series::row_tags;
use crate::schema::{Row, Schema};

pub(crate) const BLOOM_FPR: f64 = 0.01;

pub(crate) struct ChunkSpec<'a> {
    pub id: u64,
    pub wal_seq: u64,
    pub bucket: (i64, i64),
    /// series_id → rows sorted by timestamp. Must not be empty.
    pub series: &'a BTreeMap<u64, Vec<&'a Row>>,
}

/// Writes the chunk to `<dir>/<name>.tmp`, fsyncs it and renames it into
/// place. The caller is responsible for syncing the directory and for
/// publishing the chunk through the MANIFEST.
pub(crate) fn write_chunk(dir: &Path, schema: &Schema, spec: &ChunkSpec<'_>) -> Result<ChunkMeta> {
    if spec.series.is_empty() {
        return Err(invalid("refusing to write an empty chunk"));
    }
    let mut buf = vec![0u8; HEADER_LEN];
    let mut entries = Vec::with_capacity(spec.series.len());
    let mut bloom = Bloom::with_capacity(spec.series.len(), BLOOM_FPR);
    let (mut ts_min, mut ts_max, mut row_count, mut raw_bytes) = (i64::MAX, i64::MIN, 0u64, 0u64);

    for (&series_id, rows) in spec.series {
        let Some(first) = rows.first() else { continue };
        let s_min = schema.row_ts(first);
        let s_max = rows.last().map_or(s_min, |r| schema.row_ts(r));
        let n = u32::try_from(rows.len()).map_err(|_| invalid("too many rows in one series chunk"))?;
        let mut blocks = Vec::with_capacity(schema.data_indices().len());
        for &col in schema.data_indices() {
            let (block, raw) = encode_block(schema.columns()[col].ty, rows.iter().map(|r| &r[col]))?;
            blocks.push(BlockRef {
                offset: buf.len() as u64,
                len: u32::try_from(block.len()).map_err(|_| invalid("column block exceeds 4GiB"))?,
            });
            buf.extend_from_slice(&block);
            raw_bytes += raw as u64;
        }
        bloom.insert(series_id);
        entries.push(SeriesEntry {
            series_id,
            row_count: n,
            ts_min: s_min,
            ts_max: s_max,
            tags: row_tags(schema, first),
            blocks,
        });
        ts_min = ts_min.min(s_min);
        ts_max = ts_max.max(s_max);
        row_count += u64::from(n);
    }

    let index_offset = buf.len() as u64;
    put_u64(&mut buf, raw_bytes);
    put_i64(&mut buf, spec.bucket.0);
    put_i64(&mut buf, spec.bucket.1);
    put_u32(&mut buf, entries.len() as u32);
    for e in &entries {
        encode_series_entry(&mut buf, e);
    }
    let bloom_offset = buf.len() as u64;
    bloom.encode(&mut buf);

    let header = ChunkHeader {
        version: VERSION,
        flags: flags::SEALED,
        ts_min,
        ts_max,
        row_count,
        series_count: entries.len() as u32,
        column_count: schema.columns().len() as u32,
        chunk_id: spec.id,
        wal_seq: spec.wal_seq,
        schema_fingerprint: schema.fingerprint(),
    };
    buf[..HEADER_LEN].copy_from_slice(&header.encode());
    put_u64(&mut buf, index_offset);
    put_u64(&mut buf, bloom_offset);
    let file_crc = crc32fast::hash(&buf);
    put_u32(&mut buf, file_crc);
    buf.extend_from_slice(FOOTER_MAGIC);

    let name = chunk_file_name(spec.bucket, spec.id);
    let path = dir.join(&name);
    let tmp = dir.join(format!("{name}.tmp"));
    {
        let mut f = File::create(&tmp)?;
        f.write_all(&buf)?;
        f.sync_all()?;
    }
    fs::rename(&tmp, &path)?;

    Ok(ChunkMeta {
        id: spec.id,
        path,
        header,
        bucket: spec.bucket,
        raw_bytes,
        file_size: buf.len() as u64,
        series: entries,
        bloom,
    })
}
