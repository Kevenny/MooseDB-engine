//! Reading chunk metadata and individual column blocks.

use std::fs::File;
use std::io::{Read, Seek, SeekFrom};
use std::path::Path;
use std::sync::Arc;

use crate::block;
use crate::bytes::ByteReader;
use crate::cache::{self, BlockPayload};
use crate::chunk::{
    decode_series_entry, series_offsets, BlockRef, ChunkFile, ChunkHeader, ChunkMeta, ENC_HEADER_LEN, FOOTER_LEN,
    FOOTER_MAGIC, HEADER_LEN,
};
use crate::crypto::CipherParams;
use crate::error::{corrupt, Result};
use crate::index::bloom::Bloom;
use crate::schema::{Row, Schema};

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

/// Reads header, series index and Bloom filter (decrypting them if needed)
/// without touching data blocks. Validates header CRC and the structure of
/// the index; full-file CRC is checked by [`verify_file`].
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
    let cipher = if header.is_encrypted() {
        crate::crypto::ensure_available()?;
        let mut eb = [0u8; ENC_HEADER_LEN];
        f.read_exact(&mut eb)?;
        Some(CipherParams::decode(&eb)?)
    } else {
        None
    };
    let data_start = header.data_start();

    let footer_start = file_size - FOOTER_LEN as u64;
    let mut fb = [0u8; FOOTER_LEN];
    f.seek(SeekFrom::Start(footer_start))?;
    f.read_exact(&mut fb)?;
    let footer = decode_footer(&fb)?;
    if !(data_start <= footer.index_offset
        && footer.index_offset <= footer.bloom_offset
        && footer.bloom_offset <= footer_start)
    {
        return Err(corrupt(format!("{} has invalid section offsets", path.display())));
    }

    let mut meta_bytes = vec![0u8; (footer_start - footer.index_offset) as usize];
    f.seek(SeekFrom::Start(footer.index_offset))?;
    f.read_exact(&mut meta_bytes)?;
    if let Some(c) = &cipher {
        c.apply(&mut meta_bytes, footer.index_offset - data_start);
    }
    let split = (footer.bloom_offset - footer.index_offset) as usize;
    let (index_bytes, bloom_bytes) = meta_bytes.split_at(split);

    let bad_index = |what: &str| corrupt(format!("{}: {what} (corrupt file or wrong encryption key)", path.display()));
    let mut r = ByteReader::new(index_bytes);
    let raw_bytes = r.u64()?;
    let bucket = (r.i64()?, r.i64()?);
    let n = r.u32()? as usize;
    if n != header.series_count as usize || n > r.remaining() {
        return Err(bad_index("series count mismatch"));
    }
    let series = (0..n)
        .map(|_| decode_series_entry(&mut r))
        .collect::<Result<Vec<_>>>()
        .map_err(|_| bad_index("unreadable series index"))?;
    for e in &series {
        for b in &e.blocks {
            let end = b.offset.checked_add(u64::from(b.len));
            if b.offset < data_start || end.map_or(true, |end| end > footer.index_offset) {
                return Err(bad_index("block outside data section"));
            }
        }
    }
    let bloom = Bloom::decode(&mut ByteReader::new(bloom_bytes)).map_err(|_| bad_index("unreadable bloom filter"))?;

    Ok(ChunkMeta {
        id,
        file: ChunkFile::new(path.to_path_buf()),
        header,
        bucket,
        raw_bytes,
        file_size,
        series_offsets: series_offsets(&series),
        series,
        bloom,
        cipher,
    })
}

/// Full-file CRC32 verification (CHECK TABLE, recovery of the newest chunk).
/// Works on the stored bytes, so it needs no encryption key.
pub(crate) fn verify_file(path: &Path) -> Result<()> {
    let data = std::fs::read(path)?;
    if data.len() < HEADER_LEN + FOOTER_LEN {
        return Err(corrupt(format!("{} truncated", path.display())));
    }
    let footer_start = data.len() - FOOTER_LEN;
    let footer = decode_footer(&data[footer_start..])?;
    if crc32fast::hash(&data[..footer_start + 16]) != footer.file_crc {
        return Err(corrupt(format!("{} checksum mismatch", path.display())));
    }
    Ok(())
}

/// Reads, decrypts, verifies and decompresses one column block, through the
/// process-wide block cache.
pub(crate) fn read_block(meta: &ChunkMeta, b: BlockRef) -> Result<Arc<BlockPayload>> {
    let uid = meta.file.uid;
    if let Some(p) = cache::get_block(uid, b.offset) {
        return Ok(p);
    }
    let file = cache::open_file(uid, meta.path())?;
    let mut bytes = vec![0u8; b.len as usize];
    cache::read_at(&file, &mut bytes, b.offset)?;
    if let Some(c) = &meta.cipher {
        c.apply(&mut bytes, b.offset - meta.header.data_start());
    }
    let payload = Arc::new(
        block::unpack(&bytes).map_err(|e| corrupt(format!("{} at offset {}: {e}", meta.path().display(), b.offset)))?,
    );
    cache::put_block(uid, b.offset, payload.clone());
    Ok(payload)
}

/// Decodes all rows of series entry `idx`.
pub(crate) fn decode_series(schema: &Schema, meta: &ChunkMeta, idx: usize) -> Result<Vec<Row>> {
    let entry = meta.series.get(idx).ok_or_else(|| corrupt("series entry index out of range"))?;
    let n = entry.row_count as usize;
    if entry.blocks.len() != schema.data_indices().len() || entry.tags.len() != schema.tag_indices().len() {
        return Err(corrupt(format!("{} does not match the table schema", meta.path().display())));
    }
    let mut columns: Vec<Option<std::vec::IntoIter<crate::schema::Value>>> = vec![None; schema.columns().len()];
    for (&col, &b) in schema.data_indices().iter().zip(&entry.blocks) {
        let payload = read_block(meta, b)?;
        let values = block::decode(schema.columns()[col].ty, n, &payload)
            .map_err(|e| corrupt(format!("{} at offset {}: {e}", meta.path().display(), b.offset)))?;
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
