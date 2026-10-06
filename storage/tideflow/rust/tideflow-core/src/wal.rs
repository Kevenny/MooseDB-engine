//! Write-ahead log.
//!
//! The WAL is a sequence of segment files `wal_NNNNNN.tfl.wal`. Each segment is
//! a series of entries:
//!
//! ```text
//! entry := crc32:u32 len:u32 payload[len]      crc32 covers len || payload
//! ```
//!
//! The payload is a row in the `codec` format. Appends are buffered; durability
//! is reached by `sync`, which the handler calls at statement end.
//!
//! A segment number is also the checkpoint unit: once every row of segments
//! `<= N` is in sealed chunks, the manifest records `wal_seq = N` and those
//! segments are deleted.

use std::fs::{self, File, OpenOptions};
use std::io::{BufWriter, Read, Write};
use std::path::{Path, PathBuf};

use crate::bytes::ByteReader;
use crate::error::{corrupt, Result};
use crate::fsutil::sync_dir;

const WAL_PREFIX: &str = "wal_";
const WAL_SUFFIX: &str = ".tfl.wal";
const ENTRY_HEADER: usize = 8;
/// Sanity bound on a single entry; anything larger is treated as corruption.
const MAX_ENTRY: usize = 1 << 30;

pub(crate) fn segment_path(dir: &Path, seq: u64) -> PathBuf {
    dir.join(format!("{WAL_PREFIX}{seq:06}{WAL_SUFFIX}"))
}

pub(crate) fn parse_segment_name(name: &str) -> Option<u64> {
    name.strip_prefix(WAL_PREFIX)?.strip_suffix(WAL_SUFFIX)?.parse().ok()
}

/// Sorted sequence numbers of the WAL segments present in `dir`.
pub(crate) fn list_segments(dir: &Path) -> Result<Vec<u64>> {
    let mut seqs = Vec::new();
    for entry in fs::read_dir(dir)? {
        let entry = entry?;
        if let Some(seq) = entry.file_name().to_str().and_then(parse_segment_name) {
            seqs.push(seq);
        }
    }
    seqs.sort_unstable();
    Ok(seqs)
}

pub(crate) struct Wal {
    seq: u64,
    out: BufWriter<File>,
    /// Whether bytes were appended since the last `sync`.
    dirty: bool,
}

impl Wal {
    /// Creates a brand-new, empty segment. Fails if it already exists.
    pub(crate) fn create(dir: &Path, seq: u64) -> Result<Wal> {
        let file = OpenOptions::new().write(true).create_new(true).open(segment_path(dir, seq))?;
        sync_dir(dir)?;
        Ok(Wal { seq, out: BufWriter::with_capacity(1 << 20, file), dirty: false })
    }

    pub(crate) fn seq(&self) -> u64 {
        self.seq
    }

    pub(crate) fn append(&mut self, payload: &[u8]) -> Result<()> {
        let len = u32::try_from(payload.len())
            .ok()
            .filter(|&l| (l as usize) <= MAX_ENTRY)
            .ok_or_else(|| crate::error::invalid("WAL entry too large"))?;
        let len_bytes = len.to_le_bytes();
        let mut h = crc32fast::Hasher::new();
        h.update(&len_bytes);
        h.update(payload);
        self.out.write_all(&h.finalize().to_le_bytes())?;
        self.out.write_all(&len_bytes)?;
        self.out.write_all(payload)?;
        self.dirty = true;
        Ok(())
    }

    /// Pushes buffered entries to the OS and, if `durable`, to stable storage.
    pub(crate) fn sync(&mut self, durable: bool) -> Result<()> {
        if !self.dirty {
            return Ok(());
        }
        self.out.flush()?;
        if durable {
            self.out.get_ref().sync_data()?;
        }
        self.dirty = false;
        Ok(())
    }
}

pub(crate) struct ReplayOutcome {
    pub entries: u64,
    /// Bytes at the end of the segment that did not form a valid entry.
    pub torn_bytes: u64,
}

/// Feeds every valid entry of a segment to `apply`, in order.
///
/// A segment is allowed to end in a torn (partially written) entry — the
/// expected state after a crash mid-append. When `truncate_torn` is set the
/// garbage tail is cut off so the segment is clean for subsequent replays.
/// A checksum mismatch *followed by more valid-looking data* cannot be told
/// apart from a torn tail without risk, so it is also treated as the end.
pub(crate) fn replay(
    dir: &Path,
    seq: u64,
    truncate_torn: bool,
    mut apply: impl FnMut(&[u8]) -> Result<()>,
) -> Result<ReplayOutcome> {
    let path = segment_path(dir, seq);
    let mut data = Vec::new();
    File::open(&path)?.read_to_end(&mut data)?;

    let mut pos = 0usize;
    let mut entries = 0u64;
    while data.len() - pos >= ENTRY_HEADER {
        let mut r = ByteReader::new(&data[pos..]);
        let crc = r.u32()?;
        let len = r.u32()? as usize;
        if len > MAX_ENTRY || len > r.remaining() {
            break;
        }
        let payload = r.take(len)?;
        let mut h = crc32fast::Hasher::new();
        h.update(&data[pos + 4..pos + 8]);
        h.update(payload);
        if h.finalize() != crc {
            break;
        }
        apply(payload)?;
        entries += 1;
        pos += ENTRY_HEADER + len;
    }

    let torn_bytes = (data.len() - pos) as u64;
    if torn_bytes > 0 && truncate_torn {
        let f = OpenOptions::new().write(true).open(&path)?;
        f.set_len(pos as u64)?;
        f.sync_all()?;
    }
    if torn_bytes > 0 && !truncate_torn {
        return Err(corrupt(format!(
            "WAL segment {} has {torn_bytes} invalid bytes before later segments",
            path.display()
        )));
    }
    Ok(ReplayOutcome { entries, torn_bytes })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn collect(dir: &Path, seq: u64, truncate: bool) -> Result<(Vec<Vec<u8>>, ReplayOutcome)> {
        let mut got = Vec::new();
        let out = replay(dir, seq, truncate, |p| {
            got.push(p.to_vec());
            Ok(())
        })?;
        Ok((got, out))
    }

    #[test]
    fn append_and_replay() {
        let dir = tempfile::tempdir().unwrap();
        let mut w = Wal::create(dir.path(), 1).unwrap();
        w.append(b"hello").unwrap();
        w.append(b"").unwrap();
        w.append(b"world").unwrap();
        w.sync(true).unwrap();
        let (got, out) = collect(dir.path(), 1, false).unwrap();
        assert_eq!(got, vec![b"hello".to_vec(), vec![], b"world".to_vec()]);
        assert_eq!(out.torn_bytes, 0);
        assert_eq!(list_segments(dir.path()).unwrap(), vec![1]);
        assert!(Wal::create(dir.path(), 1).is_err(), "segments are never reused");
    }

    #[test]
    fn torn_tail_is_truncated() {
        let dir = tempfile::tempdir().unwrap();
        let mut w = Wal::create(dir.path(), 3).unwrap();
        w.append(b"one").unwrap();
        w.append(b"two").unwrap();
        w.sync(true).unwrap();
        drop(w);
        let path = segment_path(dir.path(), 3);
        let full = fs::metadata(&path).unwrap().len();
        // Simulate a crash in the middle of the second entry.
        OpenOptions::new().write(true).open(&path).unwrap().set_len(full - 2).unwrap();

        assert!(collect(dir.path(), 3, false).is_err(), "torn data in a non-final segment is corruption");
        let (got, out) = collect(dir.path(), 3, true).unwrap();
        assert_eq!(got, vec![b"one".to_vec()]);
        assert_eq!(out.torn_bytes, 9);
        assert_eq!(fs::metadata(&path).unwrap().len(), 11);
    }

    #[test]
    fn bit_flip_stops_replay() {
        let dir = tempfile::tempdir().unwrap();
        let mut w = Wal::create(dir.path(), 1).unwrap();
        w.append(b"aaaa").unwrap();
        w.append(b"bbbb").unwrap();
        w.sync(true).unwrap();
        drop(w);
        let path = segment_path(dir.path(), 1);
        let mut data = fs::read(&path).unwrap();
        data[ENTRY_HEADER + 4 + ENTRY_HEADER] ^= 0x01; // first payload byte of entry 2
        fs::write(&path, &data).unwrap();
        let (got, _) = collect(dir.path(), 1, true).unwrap();
        assert_eq!(got, vec![b"aaaa".to_vec()]);
    }

    #[test]
    fn segment_names() {
        assert_eq!(parse_segment_name("wal_000042.tfl.wal"), Some(42));
        assert_eq!(parse_segment_name("wal_x.tfl.wal"), None);
        assert_eq!(parse_segment_name("chunk_1.tfl"), None);
    }
}
