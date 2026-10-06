//! Write-ahead log.
//!
//! The WAL is a sequence of segment files `wal_NNNNNN.tfl.wal`:
//!
//! ```text
//! segment := header entry*
//! header  := "TFWL" version:u16 flags:u16 [cipher params (32 bytes) if ENCRYPTED]
//! entry   := crc32:u32 len:u32 payload[len]     crc32 covers len || payload (as stored)
//! ```
//!
//! The payload is a row in the `codec` format, encrypted with AES-256-CTR
//! (keystream offset = file offset) when the table is encrypted. Appends are
//! buffered; durability is reached by `sync`, which the handler calls at
//! statement end.
//!
//! A segment number is also the checkpoint unit: once every row of segments
//! `<= N` is in sealed chunks, the MANIFEST records `wal_seq = N` and those
//! segments are deleted.

use std::fs::{self, File, OpenOptions};
use std::io::{BufWriter, Read, Write};
use std::path::{Path, PathBuf};

use crate::bytes::ByteReader;
use crate::crypto::{CipherParams, PARAMS_LEN};
use crate::error::{corrupt, Error, Result};
use crate::fsutil::{self, sync_dir};

const WAL_PREFIX: &str = "wal_";
const WAL_SUFFIX: &str = ".tfl.wal";
const MAGIC: &[u8; 4] = b"TFWL";
const VERSION: u16 = 1;
const FLAG_ENCRYPTED: u16 = 1;
const BASE_HEADER: usize = 8;
const ENTRY_HEADER: usize = 8;
/// Sanity bound on a single entry; anything larger is treated as corruption.
pub(crate) const MAX_ENTRY: usize = 1 << 30;

#[cfg(test)]
thread_local! {
    static ENTRY_LIMIT: std::cell::Cell<usize> = const { std::cell::Cell::new(MAX_ENTRY) };
}

/// Largest entry accepted by `append` (lowerable per thread in tests).
pub(crate) fn max_entry() -> usize {
    #[cfg(test)]
    return ENTRY_LIMIT.with(std::cell::Cell::get);
    #[cfg(not(test))]
    MAX_ENTRY
}

/// Test hook: lowers the entry limit of the current thread.
#[cfg(test)]
pub(crate) fn set_max_entry(n: usize) {
    ENTRY_LIMIT.with(|l| l.set(n));
}

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
    cipher: Option<CipherParams>,
    /// Logical file offset of the next byte to append.
    offset: u64,
    /// Whether bytes were appended since the last `sync`.
    dirty: bool,
    /// Set by the first failed write/flush/fsync. The buffered data may be
    /// lost or half-written and a later fsync could report success without
    /// the data being durable, so the segment never accepts or confirms
    /// anything again.
    failed: bool,
}

impl Wal {
    /// Creates a brand-new, empty segment (header fsynced). Fails if it exists.
    pub(crate) fn create(dir: &Path, seq: u64, encryption_key: Option<u32>) -> Result<Wal> {
        let cipher = encryption_key.map(CipherParams::for_new_file).transpose()?;
        let path = segment_path(dir, seq);
        let mut file = OpenOptions::new().write(true).create_new(true).open(&path)?;
        let mut header = Vec::with_capacity(BASE_HEADER + PARAMS_LEN);
        header.extend_from_slice(MAGIC);
        header.extend_from_slice(&VERSION.to_le_bytes());
        header.extend_from_slice(&(if cipher.is_some() { FLAG_ENCRYPTED } else { 0 }).to_le_bytes());
        if let Some(c) = &cipher {
            header.extend_from_slice(&c.encode());
        }
        let written = file.write_all(&header).map_err(Error::from).and_then(|()| fsutil::sync_all(&file));
        if let Err(e) = written {
            // A segment with a partial header is useless; do not leave it behind.
            drop(file);
            let _ = fs::remove_file(&path);
            return Err(e);
        }
        sync_dir(dir)?;
        Ok(Wal {
            seq,
            out: BufWriter::with_capacity(1 << 20, file),
            cipher,
            offset: header.len() as u64,
            dirty: false,
            failed: false,
        })
    }

    pub(crate) fn seq(&self) -> u64 {
        self.seq
    }

    pub(crate) fn append(&mut self, payload: &[u8]) -> Result<()> {
        self.check_usable()?;
        let len = u32::try_from(payload.len())
            .ok()
            .filter(|&l| (l as usize) <= max_entry())
            .ok_or_else(|| crate::error::invalid("WAL entry too large"))?;
        let len_bytes = len.to_le_bytes();
        let mut stored;
        let body = match &self.cipher {
            Some(c) => {
                stored = payload.to_vec();
                c.apply(&mut stored, self.offset + ENTRY_HEADER as u64);
                &stored[..]
            }
            None => payload,
        };
        let mut h = crc32fast::Hasher::new();
        h.update(&len_bytes);
        h.update(body);
        let written = self
            .out
            .write_all(&h.finalize().to_le_bytes())
            .and_then(|()| self.out.write_all(&len_bytes))
            .and_then(|()| self.out.write_all(body));
        if let Err(e) = written {
            self.failed = true;
            return Err(e.into());
        }
        self.offset += (ENTRY_HEADER + body.len()) as u64;
        self.dirty = true;
        Ok(())
    }

    /// Pushes buffered entries to the OS and, if `durable`, to stable storage.
    pub(crate) fn sync(&mut self, durable: bool) -> Result<()> {
        self.check_usable()?;
        if !self.dirty {
            return Ok(());
        }
        let synced = self.out.flush().map_err(Error::from).and_then(|()| {
            if durable {
                fsutil::sync_data(self.out.get_ref())
            } else {
                Ok(())
            }
        });
        if let Err(e) = synced {
            self.failed = true;
            return Err(e);
        }
        self.dirty = false;
        Ok(())
    }

    fn check_usable(&self) -> Result<()> {
        if self.failed {
            return Err(Error::ReadOnly("WAL segment failed earlier; the table must be reopened".into()));
        }
        Ok(())
    }
}

/// What to do with an invalid tail at the end of a segment.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum TornTail {
    /// Last segment during recovery: cut the garbage off.
    Truncate,
    /// A segment followed by newer ones: invalid bytes mean corruption.
    Error,
    /// Read-only inspection: count what is valid, change nothing.
    Ignore,
}

pub(crate) struct ReplayOutcome {
    /// The segment never held anything (header missing or incomplete): safe to delete.
    pub unused: bool,
    pub entries: u64,
    /// Bytes at the end of the segment that did not form a valid entry.
    pub torn_bytes: u64,
}

/// Feeds every valid entry of a segment to `apply`, in order.
///
/// A segment may end in a torn (partially written) entry — the expected
/// state after a crash mid-append. With `truncate_torn` the garbage tail is
/// cut off; otherwise (a segment followed by newer ones) it is corruption.
/// A segment whose header itself is incomplete was never used and holds no
/// rows.
pub(crate) fn replay(
    dir: &Path,
    seq: u64,
    torn: TornTail,
    mut apply: impl FnMut(&[u8]) -> Result<()>,
) -> Result<ReplayOutcome> {
    let path = segment_path(dir, seq);
    let mut data = Vec::new();
    File::open(&path)?.read_to_end(&mut data)?;

    let mut r = ByteReader::new(&data);
    // A crash while the header was being written leaves a short (or, on some
    // file systems, zero-filled) file. Nothing was ever appended to it, in any
    // position among the segments: it is empty, never corruption.
    let never_used = |n: usize| ReplayOutcome { unused: true, entries: 0, torn_bytes: n as u64 };
    let header_ok = data.len() >= BASE_HEADER && &data[..4] == MAGIC;
    if !header_ok {
        if data.len() < BASE_HEADER || (data.len() <= BASE_HEADER + PARAMS_LEN && data.iter().all(|&b| b == 0)) {
            return Ok(never_used(data.len()));
        }
        return Err(corrupt(format!("{} has an invalid header", path.display())));
    }
    r.take(4)?;
    let version = r.u16()?;
    let flags = r.u16()?;
    if version != VERSION {
        return Err(corrupt(format!("{}: WAL version {version} not supported", path.display())));
    }
    let cipher = if flags & FLAG_ENCRYPTED != 0 {
        crate::crypto::ensure_available()?;
        match r.take(PARAMS_LEN) {
            Ok(b) => Some(CipherParams::decode(b)?),
            Err(_) => return Ok(never_used(data.len())),
        }
    } else {
        None
    };

    let mut pos = r.position();
    let mut entries = 0u64;
    let mut plain = Vec::new();
    while data.len() - pos >= ENTRY_HEADER {
        let mut er = ByteReader::new(&data[pos..]);
        let crc = er.u32()?;
        let len = er.u32()? as usize;
        if len > MAX_ENTRY || len > er.remaining() {
            break;
        }
        let body = er.take(len)?;
        let mut h = crc32fast::Hasher::new();
        h.update(&data[pos + 4..pos + 8]);
        h.update(body);
        if h.finalize() != crc {
            break;
        }
        let payload = match &cipher {
            Some(c) => {
                plain.clear();
                plain.extend_from_slice(body);
                c.apply(&mut plain, (pos + ENTRY_HEADER) as u64);
                &plain[..]
            }
            None => body,
        };
        apply(payload)?;
        entries += 1;
        pos += ENTRY_HEADER + len;
    }

    let torn_bytes = (data.len() - pos) as u64;
    if torn_bytes > 0 {
        if torn == TornTail::Error {
            return Err(corrupt(format!(
                "WAL segment {} has {torn_bytes} invalid bytes before later segments",
                path.display()
            )));
        }
        if torn == TornTail::Truncate {
            let f = OpenOptions::new().write(true).open(&path)?;
            f.set_len(pos as u64)?;
            f.sync_all()?;
        }
    }
    Ok(ReplayOutcome { unused: false, entries, torn_bytes })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn collect(dir: &Path, seq: u64, truncate: bool) -> Result<(Vec<Vec<u8>>, ReplayOutcome)> {
        let mut got = Vec::new();
        let out = replay(dir, seq, if truncate { TornTail::Truncate } else { TornTail::Error }, |p| {
            got.push(p.to_vec());
            Ok(())
        })?;
        Ok((got, out))
    }

    #[test]
    fn append_and_replay() {
        let dir = tempfile::tempdir().unwrap();
        let mut w = Wal::create(dir.path(), 1, None).unwrap();
        w.append(b"hello").unwrap();
        w.append(b"").unwrap();
        w.append(b"world").unwrap();
        w.sync(true).unwrap();
        let (got, out) = collect(dir.path(), 1, false).unwrap();
        assert_eq!(got, vec![b"hello".to_vec(), vec![], b"world".to_vec()]);
        assert_eq!(out.torn_bytes, 0);
        assert_eq!(list_segments(dir.path()).unwrap(), vec![1]);
        assert!(Wal::create(dir.path(), 1, None).is_err(), "segments are never reused");
    }

    #[test]
    fn torn_tail_is_truncated() {
        let dir = tempfile::tempdir().unwrap();
        let mut w = Wal::create(dir.path(), 3, None).unwrap();
        w.append(b"one").unwrap();
        w.append(b"two").unwrap();
        w.sync(true).unwrap();
        drop(w);
        let path = segment_path(dir.path(), 3);
        let full = fs::metadata(&path).unwrap().len();
        OpenOptions::new().write(true).open(&path).unwrap().set_len(full - 2).unwrap();

        assert!(collect(dir.path(), 3, false).is_err(), "torn data in a non-final segment is corruption");
        let (got, out) = collect(dir.path(), 3, true).unwrap();
        assert_eq!(got, vec![b"one".to_vec()]);
        assert_eq!(out.torn_bytes, 9);
        assert_eq!(fs::metadata(&path).unwrap().len(), (BASE_HEADER + 11) as u64);
    }

    #[test]
    fn bit_flip_stops_replay() {
        let dir = tempfile::tempdir().unwrap();
        let mut w = Wal::create(dir.path(), 1, None).unwrap();
        w.append(b"aaaa").unwrap();
        w.append(b"bbbb").unwrap();
        w.sync(true).unwrap();
        drop(w);
        let path = segment_path(dir.path(), 1);
        let mut data = fs::read(&path).unwrap();
        data[BASE_HEADER + ENTRY_HEADER + 4 + ENTRY_HEADER] ^= 0x01;
        fs::write(&path, &data).unwrap();
        let (got, _) = collect(dir.path(), 1, true).unwrap();
        assert_eq!(got, vec![b"aaaa".to_vec()]);
    }

    #[test]
    fn empty_or_headerless_segment() {
        let dir = tempfile::tempdir().unwrap();
        fs::write(segment_path(dir.path(), 7), b"TF").unwrap();
        let (got, out) = collect(dir.path(), 7, true).unwrap();
        assert!(got.is_empty());
        assert_eq!(out.torn_bytes, 2);
        fs::write(segment_path(dir.path(), 8), b"garbage-garbage").unwrap();
        assert!(collect(dir.path(), 8, true).is_err());
    }

    #[test]
    fn partial_header_segments_are_unused_in_any_position() {
        let dir = tempfile::tempdir().unwrap();
        for (seq, bytes) in [(1u64, &b""[..]), (2, b"TFW"), (3, &[0u8; 20][..])] {
            fs::write(segment_path(dir.path(), seq), bytes).unwrap();
            // Even when newer segments exist (strict mode), it is just empty.
            let (got, out) = collect(dir.path(), seq, false).unwrap();
            assert!(got.is_empty() && out.unused, "segment of {} bytes", bytes.len());
        }
    }

    #[test]
    fn failed_header_write_leaves_no_segment() {
        let dir = tempfile::tempdir().unwrap();
        fsutil::fail_fsync_after(0);
        let r = Wal::create(dir.path(), 1, None);
        fsutil::restore_fsync();
        assert!(r.is_err());
        assert!(list_segments(dir.path()).unwrap().is_empty());
        assert!(Wal::create(dir.path(), 1, None).is_ok());
    }

    #[test]
    fn fsync_failure_is_sticky() {
        let dir = tempfile::tempdir().unwrap();
        let mut w = Wal::create(dir.path(), 1, None).unwrap();
        w.append(b"row").unwrap();
        fsutil::fail_fsync_after(0);
        let first = w.sync(true);
        fsutil::restore_fsync();
        assert!(first.is_err());
        assert!(fsutil::is_fsync_error(&first.unwrap_err()));
        // The disk "recovered", but success can no longer be vouched for.
        assert!(w.sync(true).is_err());
        assert!(w.sync(false).is_err());
        assert!(w.append(b"more").is_err());
    }

    #[cfg(feature = "encryption")]
    #[test]
    fn encrypted_segments() {
        crate::crypto::test_keys::install();
        let dir = tempfile::tempdir().unwrap();
        let mut w = Wal::create(dir.path(), 1, Some(5)).unwrap();
        w.append(b"secret row one").unwrap();
        w.append(b"secret row two").unwrap();
        w.sync(true).unwrap();
        drop(w);
        let raw = fs::read(segment_path(dir.path(), 1)).unwrap();
        assert!(!raw.windows(6).any(|w| w == b"secret"), "plaintext leaked into the WAL");
        let (got, _) = collect(dir.path(), 1, true).unwrap();
        assert_eq!(got, vec![b"secret row one".to_vec(), b"secret row two".to_vec()]);
    }

    #[test]
    fn segment_names() {
        assert_eq!(parse_segment_name("wal_000042.tfl.wal"), Some(42));
        assert_eq!(parse_segment_name("wal_x.tfl.wal"), None);
        assert_eq!(parse_segment_name("chunk_1.tfl"), None);
    }
}
