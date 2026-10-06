//! Reading chunk metadata and decoding series data.

use std::fs::File;
use std::io::{Read, Seek, SeekFrom};
use std::path::Path;

use crate::bytes::ByteReader;
use crate::chunk::{decode_block, decode_series_entry, ChunkHeader, ChunkMeta, FOOTER_LEN, FOOTER_MAGIC, HEADER_LEN};
use crate::error::{corrupt, Result};
use crate::index::bloom::Bloom;
use crate::schema::{Row, Schema, Value};

struct Footer {
    index_offset: u64,
    bloom_offset: u64,
    file_crc: u32,
}

fn decode_footer(b: &[u8]) -> Result<Footer> {
    let mut r = ByteReader::new(b);
    let f = Footer { index_offset: r.u64()?, bloom_offset: r.u64()?, file_crc: r.u32()? };
    if r.take(4)? != FOOTER_MAGIC {
        return Err(corrupt("chunk bad footer magic"));
    }
    Ok(f)
}

/// Reads header, series index and Bloom filter without touching data blocks.
/// Validates the header checksum and the structural sanity of the index; the
/// full-file checksum is verified separately by [`verify_file`] / [`load_data`].
pub(crate) fn read_meta(path: &Path, id: u64) -> Result<ChunkMeta> {
    let mut f = File::open(path)?;
    let file_size = f.metadata()?.len();
    if file_size < (HEADER_LEN + FOOTER_LEN) as u64 {
        return Err(corrupt(format!("{} is too small to be a chunk", path.display())));
    }
    let mut hb = [0u8; HEADER_LEN];
    f.read_exact(&mut hb)?;
    let header = ChunkHeader::decode(&hb)?;
    if header.chunk_id != id {
        return Err(corrupt(format!("{} claims chunk id {}", path.display(), header.chunk_id)));
    }

    let footer_start = file_size - FOOTER_LEN as u64;
    let mut fb = [0u8; FOOTER_LEN];
    f.seek(SeekFrom::Start(footer_start))?;
    f.read_exact(&mut fb)?;
    let footer = decode_footer(&fb)?;
    if !(HEADER_LEN as u64 <= footer.index_offset
        && footer.index_offset <= footer.bloom_offset
        && footer.bloom_offset <= footer_start)
    {
        return Err(corrupt(format!("{} has invalid section offsets", path.display())));
    }

    let mut meta_bytes = vec![0u8; (footer_start - footer.index_offset) as usize];
    f.seek(SeekFrom::Start(footer.index_offset))?;
    f.read_exact(&mut meta_bytes)?;
    let split = (footer.bloom_offset - footer.index_offset) as usize;
    let (index_bytes, bloom_bytes) = meta_bytes.split_at(split);

    let mut r = ByteReader::new(index_bytes);
    let raw_bytes = r.u64()?;
    let bucket = (r.i64()?, r.i64()?);
    let n = r.u32()? as usize;
    if n != header.series_count as usize || n > r.remaining() {
        return Err(corrupt(format!("{} series count mismatch", path.display())));
    }
    let series = (0..n).map(|_| decode_series_entry(&mut r)).collect::<Result<Vec<_>>>()?;
    for e in &series {
        for b in &e.blocks {
            let end = b.offset.checked_add(u64::from(b.len));
            if b.offset < HEADER_LEN as u64 || end.map_or(true, |end| end > footer.index_offset) {
                return Err(corrupt(format!("{} block outside data section", path.display())));
            }
        }
    }
    let bloom = Bloom::decode(&mut ByteReader::new(bloom_bytes))?;

    Ok(ChunkMeta { id, path: path.to_path_buf(), header, bucket, raw_bytes, file_size, series, bloom })
}

fn check_crc(path: &Path, data: &[u8]) -> Result<()> {
    if data.len() < HEADER_LEN + FOOTER_LEN {
        return Err(corrupt(format!("{} truncated", path.display())));
    }
    let footer_start = data.len() - FOOTER_LEN;
    let footer = decode_footer(&data[footer_start..])?;
    // The CRC covers everything up to and including both footer offsets.
    if crc32fast::hash(&data[..footer_start + 16]) != footer.file_crc {
        return Err(corrupt(format!("{} checksum mismatch", path.display())));
    }
    Ok(())
}

/// Full-file CRC32 verification (CHECK TABLE, recovery of the newest chunk).
pub(crate) fn verify_file(path: &Path) -> Result<()> {
    let data = std::fs::read(path)?;
    check_crc(path, &data)
}

/// Reads and checksums the entire chunk file.
pub(crate) fn load_data(meta: &ChunkMeta) -> Result<Vec<u8>> {
    let data = std::fs::read(&meta.path)?;
    if data.len() as u64 != meta.file_size {
        return Err(corrupt(format!("{} changed size since it was opened", meta.path.display())));
    }
    check_crc(&meta.path, &data)?;
    Ok(data)
}

/// Decodes all rows of series entry `idx` of a chunk whose bytes are `data`.
pub(crate) fn decode_series(schema: &Schema, meta: &ChunkMeta, data: &[u8], idx: usize) -> Result<Vec<Row>> {
    let entry = meta.series.get(idx).ok_or_else(|| corrupt("series entry index out of range"))?;
    let n = entry.row_count as usize;
    if entry.blocks.len() != schema.data_indices().len() || entry.tags.len() != schema.tag_indices().len() {
        return Err(corrupt(format!("{} does not match the table schema", meta.path.display())));
    }
    let mut columns: Vec<Option<std::vec::IntoIter<Value>>> = vec![None; schema.columns().len()];
    for (&col, b) in schema.data_indices().iter().zip(&entry.blocks) {
        let start = b.offset as usize;
        let block = data.get(start..start + b.len as usize).ok_or_else(|| corrupt("block outside chunk data"))?;
        let values = decode_block(schema.columns()[col].ty, n, block)?;
        columns[col] = Some(values.into_iter());
    }
    let tag_of: Vec<Option<usize>> = (0..schema.columns().len()).map(|c| schema.tag_position(c)).collect();
    let mut rows = Vec::with_capacity(n);
    for _ in 0..n {
        let mut row = Vec::with_capacity(columns.len());
        for (col, it) in columns.iter_mut().enumerate() {
            let v = match (it, tag_of[col]) {
                (Some(it), _) => it.next().ok_or_else(|| corrupt("column block too short"))?,
                (None, Some(t)) => entry.tags[t].clone(),
                (None, None) => return Err(corrupt("column without data")),
            };
            row.push(v);
        }
        rows.push(row);
    }
    Ok(rows)
}
