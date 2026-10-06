//! Incremental construction of a sealed chunk file.
//!
//! Series are added one at a time (already sorted by timestamp); their column
//! blocks are encoded and compressed immediately, so building a chunk only
//! holds the *compressed* output plus one series in memory. This is what lets
//! compaction merge large time buckets.

use std::fs::{self, File};
use std::io::Write;
use std::path::Path;

use crate::block;
use crate::bytes::{put_i64, put_u32, put_u64};
use crate::chunk::{
    chunk_file_name, chunk_flags, encode_series_entry, series_offsets, BlockRef, ChunkFile, ChunkHeader, ChunkMeta,
    SeriesEntry, FOOTER_MAGIC, HEADER_LEN, VERSION,
};
use crate::compression::Codec;
use crate::crypto::CipherParams;
use crate::error::{invalid, Result};
use crate::index::bloom::Bloom;
use crate::schema::{Row, Schema, Value};

pub(crate) struct ChunkBuilder<'a> {
    schema: &'a Schema,
    codec: Codec,
    cipher: Option<CipherParams>,
    data_start: u64,
    /// Data section bytes (plaintext until `finish`).
    data: Vec<u8>,
    entries: Vec<SeriesEntry>,
    raw_bytes: u64,
}

impl<'a> ChunkBuilder<'a> {
    pub(crate) fn new(schema: &'a Schema, codec: Codec, cipher: Option<CipherParams>) -> ChunkBuilder<'a> {
        let data_start = (HEADER_LEN + if cipher.is_some() { crate::chunk::ENC_HEADER_LEN } else { 0 }) as u64;
        ChunkBuilder { schema, codec, cipher, data_start, data: Vec::new(), entries: Vec::new(), raw_bytes: 0 }
    }

    /// Appends one series. `rows` must be sorted by timestamp and non-empty.
    pub(crate) fn add_series(&mut self, series_id: u64, tags: &[Value], rows: &[&Row]) -> Result<()> {
        let (Some(first), Some(last)) = (rows.first(), rows.last()) else {
            return Err(invalid("empty series"));
        };
        let n = u32::try_from(rows.len()).map_err(|_| invalid("too many rows in one series chunk"))?;
        if u64::from(n) > crate::compression::MAX_SERIES_ROWS {
            return Err(invalid(format!("{n} rows in one series chunk exceed the limit")));
        }
        let mut blocks = Vec::with_capacity(self.schema.data_indices().len());
        let mut column: Vec<&Value> = Vec::with_capacity(rows.len());
        for &col in self.schema.data_indices() {
            column.clear();
            column.extend(rows.iter().map(|r| &r[col]));
            let b = block::encode(self.schema.columns()[col].ty, &column, self.codec)?;
            blocks.push(BlockRef {
                offset: self.data_start + self.data.len() as u64,
                len: u32::try_from(b.bytes.len()).map_err(|_| invalid("column block exceeds 4GiB"))?,
            });
            self.data.extend_from_slice(&b.bytes);
            self.raw_bytes += b.plain_len as u64;
        }
        self.entries.push(SeriesEntry {
            series_id,
            row_count: n,
            ts_min: self.schema.row_ts(first),
            ts_max: self.schema.row_ts(last),
            tags: tags.to_vec(),
            blocks,
        });
        Ok(())
    }

    /// Writes `<dir>/<name>.tmp`, fsyncs it and renames it into place. The
    /// caller syncs the directory and publishes the chunk via the MANIFEST.
    pub(crate) fn finish(self, dir: &Path, id: u64, wal_seq: u64, bucket: (i64, i64)) -> Result<ChunkMeta> {
        if self.entries.is_empty() {
            return Err(invalid("refusing to write an empty chunk"));
        }
        let mut bloom = Bloom::with_capacity(self.entries.len(), crate::settings::get().bloom_fpr());
        for e in &self.entries {
            bloom.insert(e.series_id);
        }
        let ts_min = self.entries.iter().map(|e| e.ts_min).min().unwrap_or(0);
        let ts_max = self.entries.iter().map(|e| e.ts_max).max().unwrap_or(0);
        let row_count: u64 = self.entries.iter().map(|e| u64::from(e.row_count)).sum();

        let start = self.data_start as usize;
        let mut buf = vec![0u8; start];
        buf.extend_from_slice(&self.data);
        let index_offset = buf.len() as u64;
        put_u64(&mut buf, self.raw_bytes);
        put_i64(&mut buf, bucket.0);
        put_i64(&mut buf, bucket.1);
        put_u32(&mut buf, self.entries.len() as u32);
        for e in &self.entries {
            encode_series_entry(&mut buf, e);
        }
        let bloom_offset = buf.len() as u64;
        bloom.encode(&mut buf);

        let header = ChunkHeader {
            version: VERSION,
            flags: chunk_flags(self.codec, self.cipher.is_some()),
            ts_min,
            ts_max,
            row_count,
            series_count: self.entries.len() as u32,
            column_count: self.schema.columns().len() as u32,
            chunk_id: id,
            wal_seq,
            schema_fingerprint: self.schema.fingerprint(),
        };
        buf[..HEADER_LEN].copy_from_slice(&header.encode());
        if let Some(c) = &self.cipher {
            buf[HEADER_LEN..start].copy_from_slice(&c.encode());
            c.apply(&mut buf[start..], 0);
        }
        put_u64(&mut buf, index_offset);
        put_u64(&mut buf, bloom_offset);
        let file_crc = crc32fast::hash(&buf);
        put_u32(&mut buf, file_crc);
        buf.extend_from_slice(FOOTER_MAGIC);

        let name = chunk_file_name(bucket, id);
        let path = dir.join(&name);
        let tmp = dir.join(format!("{name}.tmp"));
        {
            let mut f = File::create(&tmp)?;
            f.write_all(&buf)?;
            crate::fsutil::sync_all(&f)?;
        }
        fs::rename(&tmp, &path)?;

        Ok(ChunkMeta {
            id,
            file: ChunkFile::new(path),
            header,
            bucket,
            raw_bytes: self.raw_bytes,
            file_size: buf.len() as u64,
            series_offsets: series_offsets(&self.entries),
            series: self.entries,
            bloom,
            cipher: self.cipher,
        })
    }
}
