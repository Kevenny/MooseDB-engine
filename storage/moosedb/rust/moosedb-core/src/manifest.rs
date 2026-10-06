//! The MANIFEST is the single source of truth for which chunk files are live
//! and how much of the WAL they cover. Every structural change (flush,
//! truncate, retention, compaction) follows the same protocol:
//!
//! 1. write the new files (chunks) and fsync them,
//! 2. atomically replace the MANIFEST (the commit point),
//! 3. delete what the new MANIFEST no longer references.
//!
//! Recovery deletes chunk files not listed in the MANIFEST and WAL segments
//! with `seq < replay_seq`, so a crash at any step leaves a consistent table.
//!
//! ```text
//! v2: magic "TFLM" | version:u16 | reserved:u16 | wal_seq:u64 | next_chunk_id:u64
//!     | replay_seq:u64 | chunk_count:u32 | chunk_id:u64 * chunk_count | crc32:u32
//! v1: same without `replay_seq` (read as `wal_seq + 1`)
//! ```
//!
//! `wal_seq` is the flush point: every batch whose COMMIT lies in a segment
//! `<= wal_seq` is already in `chunks`. `replay_seq` is where replay starts:
//! it trails `wal_seq + 1` while a batch that logged rows before the flush
//! is still open, because its rows precede the flush point but its COMMIT
//! will follow it.

use std::fs;
use std::path::Path;

use crate::bytes::{put_u16, put_u32, put_u64, ByteReader};
use crate::error::{corrupt, Error, Result};
use crate::fsutil::write_atomic;

pub(crate) const MANIFEST_FILE: &str = "MANIFEST";
const MAGIC: &[u8; 4] = b"TFLM";
const VERSION: u16 = 2;

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Manifest {
    /// Every batch committed in a WAL segment with `seq <= wal_seq` is fully
    /// reflected in `chunks` (the flush point).
    pub wal_seq: u64,
    /// First WAL segment recovery must read; always `<= wal_seq + 1`.
    pub replay_seq: u64,
    pub next_chunk_id: u64,
    /// Live chunk ids, ascending.
    pub chunks: Vec<u64>,
}

impl Manifest {
    pub(crate) fn empty() -> Manifest {
        Manifest { wal_seq: 0, replay_seq: 1, next_chunk_id: 1, chunks: Vec::new() }
    }

    fn encode(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(40 + self.chunks.len() * 8);
        buf.extend_from_slice(MAGIC);
        put_u16(&mut buf, VERSION);
        put_u16(&mut buf, 0);
        put_u64(&mut buf, self.wal_seq);
        put_u64(&mut buf, self.next_chunk_id);
        put_u64(&mut buf, self.replay_seq);
        put_u32(&mut buf, self.chunks.len() as u32);
        for id in &self.chunks {
            put_u64(&mut buf, *id);
        }
        let crc = crc32fast::hash(&buf);
        put_u32(&mut buf, crc);
        buf
    }

    fn decode(data: &[u8]) -> Result<Manifest> {
        if data.len() < 4 + 4 {
            return Err(corrupt("MANIFEST too short"));
        }
        let (body, crc) = data.split_at(data.len() - 4);
        if crc32fast::hash(body).to_le_bytes() != crc {
            return Err(corrupt("MANIFEST checksum mismatch"));
        }
        let mut r = ByteReader::new(body);
        if r.take(4)? != MAGIC {
            return Err(corrupt("MANIFEST bad magic"));
        }
        let version = r.u16()?;
        if version != VERSION && version != 1 {
            return Err(corrupt(format!("MANIFEST version {version} not supported")));
        }
        r.u16()?;
        let wal_seq = r.u64()?;
        let next_chunk_id = r.u64()?;
        let after_flush = wal_seq.saturating_add(1);
        let replay_seq = if version >= 2 { r.u64()?.min(after_flush) } else { after_flush };
        let n = r.u32()? as usize;
        if n.checked_mul(8) != Some(r.remaining()) {
            return Err(corrupt("MANIFEST chunk list length mismatch"));
        }
        let chunks = (0..n).map(|_| r.u64()).collect::<Result<Vec<_>>>()?;
        Ok(Manifest { wal_seq, replay_seq, next_chunk_id, chunks })
    }

    pub(crate) fn load(dir: &Path) -> Result<Manifest> {
        match fs::read(dir.join(MANIFEST_FILE)) {
            Ok(data) => Manifest::decode(&data),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                Err(Error::NotFound(format!("{} has no MANIFEST", dir.display())))
            }
            Err(e) => Err(e.into()),
        }
    }

    pub(crate) fn store(&self, dir: &Path) -> Result<()> {
        write_atomic(&dir.join(MANIFEST_FILE), &self.encode())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let m = Manifest { wal_seq: 9, replay_seq: 7, next_chunk_id: 12, chunks: vec![3, 7, 11] };
        m.store(dir.path()).unwrap();
        assert_eq!(Manifest::load(dir.path()).unwrap(), m);
    }

    #[test]
    fn reads_v1_manifests() {
        let mut buf = Vec::new();
        buf.extend_from_slice(MAGIC);
        put_u16(&mut buf, 1);
        put_u16(&mut buf, 0);
        put_u64(&mut buf, 5); // wal_seq
        put_u64(&mut buf, 9); // next_chunk_id
        put_u32(&mut buf, 1);
        put_u64(&mut buf, 4);
        let crc = crc32fast::hash(&buf);
        put_u32(&mut buf, crc);
        let m = Manifest::decode(&buf).unwrap();
        assert_eq!(m, Manifest { wal_seq: 5, replay_seq: 6, next_chunk_id: 9, chunks: vec![4] });
    }

    #[test]
    fn detects_corruption() {
        let mut data = Manifest::empty().encode();
        data[10] ^= 0xff;
        assert!(matches!(Manifest::decode(&data), Err(Error::Corrupt(_))));
        assert!(matches!(Manifest::decode(&[1, 2]), Err(Error::Corrupt(_))));
    }

    #[test]
    fn missing_is_not_found() {
        let dir = tempfile::tempdir().unwrap();
        assert!(matches!(Manifest::load(dir.path()), Err(Error::NotFound(_))));
    }
}
