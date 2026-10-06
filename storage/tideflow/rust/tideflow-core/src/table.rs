//! A TideFlow table: one directory holding the MANIFEST, WAL segments and chunks.
//!
//! All mutable state lives behind one mutex. Writers are already serialized
//! by MariaDB's table lock, so finer-grained locking would buy little; scans
//! only hold the mutex while taking their snapshot.

use std::collections::{HashMap, HashSet};
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, MutexGuard};

use crate::bytes::ByteReader;
use crate::chunk::{parse_chunk_id, ChunkMeta};
use crate::chunk_reader::{decode_series, load_data, read_meta, verify_file};
use crate::chunk_writer::{write_chunk, ChunkSpec};
use crate::codec::{decode_row, encode_row};
use crate::error::{corrupt, invalid, Error, Result};
use crate::fsutil::{remove_if_exists, sync_dir};
use crate::index::series::{row_tags, SeriesIndex};
use crate::log::warn;
use crate::manifest::Manifest;
use crate::memtable::MemTable;
use crate::options::TableConfig;
use crate::scan::{Position, ResolvedFilter, Scan, ScanFilter, MEM_SOURCE};
use crate::schema::{Row, Schema};
use crate::wal::{self, list_segments, segment_path, Wal};

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct TableStats {
    pub row_count: u64,
    /// Uncompressed payload size (chunks + MemTable estimate).
    pub data_bytes: u64,
    /// Bytes on disk used by chunk files.
    pub compressed_bytes: u64,
    pub chunk_count: u32,
    pub memtable_rows: u64,
    pub series_count: u64,
}

struct Inner {
    manifest: Manifest,
    /// Live chunks, ascending id.
    chunks: Vec<Arc<ChunkMeta>>,
    wal: Wal,
    memtable: MemTable,
    series: SeriesIndex,
    /// Bumped whenever MemTable rows move or vanish, invalidating MemTable positions.
    mem_generation: u64,
    /// Set when a failure left disk and memory possibly inconsistent; the
    /// table then refuses writes until reopened (which runs recovery).
    poisoned: Option<String>,
}

impl Inner {
    fn check_writable(&self) -> Result<()> {
        match &self.poisoned {
            Some(why) => Err(Error::ReadOnly(format!("table must be reopened after an earlier failure: {why}"))),
            None => Ok(()),
        }
    }

    fn poison(&mut self, why: String) {
        warn(&format!("table switched to read-only: {why}"));
        self.poisoned = Some(why);
    }

    /// Publishes `manifest`. If the store fails we cannot know whether the
    /// rename reached the disk, so the table is poisoned; reopening resolves
    /// the ambiguity through recovery.
    fn commit_manifest(&mut self, dir: &Path, manifest: Manifest) -> Result<()> {
        if let Err(e) = manifest.store(dir) {
            self.poison(format!("MANIFEST update failed: {e}"));
            return Err(e);
        }
        self.manifest = manifest;
        Ok(())
    }

    /// Starts a fresh WAL segment after a checkpoint. Writing to the old
    /// segment would be unsafe (recovery discards it), hence poison on failure.
    fn rotate_wal(&mut self, dir: &Path) -> Result<()> {
        match Wal::create(dir, self.manifest.wal_seq + 1) {
            Ok(w) => {
                self.wal = w;
                Ok(())
            }
            Err(e) => {
                self.poison(format!("cannot create WAL segment: {e}"));
                Err(e)
            }
        }
    }
}

pub struct Table {
    config: TableConfig,
    schema: Arc<Schema>,
    inner: Mutex<Inner>,
}

/// Deletes WAL segments `<= upto`, logging (not failing) on errors: leftover
/// segments are ignored by recovery anyway.
fn delete_wal_upto(dir: &Path, upto: u64) {
    match list_segments(dir) {
        Ok(segs) => {
            for seq in segs.into_iter().filter(|&s| s <= upto) {
                if let Err(e) = remove_if_exists(&segment_path(dir, seq)) {
                    warn(&format!("cannot delete WAL segment {seq}: {e}"));
                }
            }
        }
        Err(e) => warn(&format!("cannot list WAL segments: {e}")),
    }
}

fn delete_chunk_files<'a>(chunks: impl IntoIterator<Item = &'a Arc<ChunkMeta>>) {
    for c in chunks {
        if let Err(e) = remove_if_exists(&c.path) {
            warn(&format!("cannot delete {}: {e}", c.path.display()));
        }
    }
}

fn quarantine(path: &Path, why: &str) -> Result<()> {
    let mut target = path.as_os_str().to_owned();
    target.push(".corrupt");
    warn(&format!("{} is corrupt ({why}); moved aside to {}", path.display(), PathBuf::from(&target).display()));
    fs::rename(path, &target)?;
    Ok(())
}

impl Table {
    /// Creates the table directory with an empty MANIFEST.
    pub fn create(config: &TableConfig) -> Result<()> {
        let dir = &config.dir;
        match fs::create_dir(dir) {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {
                if fs::read_dir(dir)?.next().is_some() {
                    return Err(invalid(format!("{} already exists and is not empty", dir.display())));
                }
            }
            Err(e) => return Err(e.into()),
        }
        Manifest::empty().store(dir)?;
        if let Some(parent) = dir.parent() {
            sync_dir(parent)?;
        }
        Ok(())
    }

    /// Removes the table directory and everything in it.
    pub fn drop_table(dir: &Path) -> Result<()> {
        match fs::remove_dir_all(dir) {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                Err(Error::NotFound(format!("{} does not exist", dir.display())))
            }
            Err(e) => Err(e.into()),
        }
    }

    pub fn rename(from: &Path, to: &Path) -> Result<()> {
        if to.exists() {
            return Err(invalid(format!("{} already exists", to.display())));
        }
        fs::rename(from, to).map_err(|e| match e.kind() {
            std::io::ErrorKind::NotFound => Error::NotFound(format!("{} does not exist", from.display())),
            _ => e.into(),
        })?;
        if let Some(parent) = to.parent() {
            sync_dir(parent)?;
        }
        Ok(())
    }

    /// Opens the table, running crash recovery:
    /// 1. load the MANIFEST;
    /// 2. delete temp files and chunk files the MANIFEST does not list;
    /// 3. load metadata of live chunks, quarantining corrupt ones
    ///    (`.tfl.corrupt`), and fully verify the newest chunk's CRC;
    /// 4. rebuild the series index from chunk metadata;
    /// 5. drop WAL segments covered by the checkpoint and replay the rest
    ///    into the MemTable (a torn tail in the last segment is truncated);
    /// 6. start a new WAL segment.
    pub fn open(config: TableConfig) -> Result<Table> {
        let dir = config.dir.clone();
        if !dir.is_dir() {
            return Err(Error::NotFound(format!("table directory {} does not exist", dir.display())));
        }
        let schema = Arc::new(config.schema.clone());
        let mut manifest = Manifest::load(&dir)?;

        let mut chunk_files: HashMap<u64, PathBuf> = HashMap::new();
        let mut wal_segs = Vec::new();
        for entry in fs::read_dir(&dir)? {
            let entry = entry?;
            let path = entry.path();
            let Some(name) = entry.file_name().to_str().map(str::to_owned) else { continue };
            if name.ends_with(".tmp") {
                remove_if_exists(&path)?;
            } else if let Some(id) = parse_chunk_id(&name) {
                chunk_files.insert(id, path);
            } else if let Some(seq) = wal::parse_segment_name(&name) {
                wal_segs.push(seq);
            }
        }
        wal_segs.sort_unstable();

        let live: HashSet<u64> = manifest.chunks.iter().copied().collect();
        for (id, path) in &chunk_files {
            if !live.contains(id) {
                remove_if_exists(path)?;
            }
        }

        let mut chunks = Vec::with_capacity(manifest.chunks.len());
        let mut dropped = false;
        for &id in &manifest.chunks {
            let Some(path) = chunk_files.get(&id) else {
                warn(&format!("chunk {id} listed in MANIFEST is missing from {}", dir.display()));
                dropped = true;
                continue;
            };
            match read_meta(path, id) {
                Ok(meta) => chunks.push(meta),
                Err(Error::Corrupt(why)) => {
                    quarantine(path, &why)?;
                    dropped = true;
                }
                Err(e) => return Err(e),
            }
        }
        chunks.sort_by_key(|c| c.id);
        if let Some(newest) = chunks.last() {
            match verify_file(&newest.path) {
                Ok(()) => {}
                Err(Error::Corrupt(why)) => {
                    quarantine(&newest.path, &why)?;
                    chunks.pop();
                    dropped = true;
                }
                Err(e) => return Err(e),
            }
        }
        let fingerprint = schema.fingerprint();
        if let Some(c) = chunks.iter().find(|c| c.header.schema_fingerprint != fingerprint) {
            return Err(invalid(format!("{} was written with a different table schema", c.path.display())));
        }
        if dropped {
            manifest.chunks = chunks.iter().map(|c| c.id).collect();
            manifest.store(&dir)?;
        }

        let mut series = SeriesIndex::default();
        for c in &chunks {
            for e in &c.series {
                series.register(e.series_id, &e.tags)?;
            }
        }

        let mut memtable = MemTable::default();
        for &seq in wal_segs.iter().filter(|&&s| s <= manifest.wal_seq) {
            remove_if_exists(&segment_path(&dir, seq))?;
        }
        let replay: Vec<u64> = wal_segs.iter().copied().filter(|&s| s > manifest.wal_seq).collect();
        for (i, &seq) in replay.iter().enumerate() {
            let is_last = i + 1 == replay.len();
            let outcome = wal::replay(&dir, seq, is_last, |payload| {
                let row = decode_row(&mut ByteReader::new(payload))?;
                schema.validate_row(&row).map_err(|e| {
                    corrupt(format!("WAL segment {seq} holds a row that does not match the schema: {e}"))
                })?;
                let sid = series.get_or_insert(&row_tags(&schema, &row));
                memtable.push(sid, row);
                Ok(())
            })?;
            if outcome.torn_bytes > 0 {
                warn(&format!(
                    "WAL segment {seq}: discarded {} torn bytes after {} valid entries",
                    outcome.torn_bytes, outcome.entries
                ));
            }
        }

        let next_seq = wal_segs.last().copied().unwrap_or(0).max(manifest.wal_seq) + 1;
        let wal = Wal::create(&dir, next_seq)?;
        Ok(Table {
            config,
            schema,
            inner: Mutex::new(Inner {
                manifest,
                chunks: chunks.into_iter().map(Arc::new).collect(),
                wal,
                memtable,
                series,
                mem_generation: 0,
                poisoned: None,
            }),
        })
    }

    fn lock(&self) -> Result<MutexGuard<'_, Inner>> {
        self.inner.lock().map_err(|_| Error::ReadOnly("table state poisoned by a panic".into()))
    }

    pub fn schema(&self) -> &Schema {
        &self.schema
    }

    pub fn config(&self) -> &TableConfig {
        &self.config
    }

    /// Appends a row: WAL first, then MemTable. The row is durable once
    /// `sync_wal(true)` returns.
    pub fn write(&self, row: Row) -> Result<()> {
        self.schema.validate_row(&row)?;
        let mut payload = Vec::with_capacity(16 * row.len());
        encode_row(&mut payload, &row);
        let tags = row_tags(&self.schema, &row);

        let mut guard = self.lock()?;
        let inner = &mut *guard;
        inner.check_writable()?;
        let limit = usize::try_from(self.config.memtable_size_bytes).unwrap_or(usize::MAX);
        if inner.memtable.bytes() >= limit.saturating_mul(2) {
            // Flushes have been failing; refuse new rows rather than grow without bound.
            self.flush_locked(inner).map_err(|e| Error::Full(format!("MemTable is full and flushing failed: {e}")))?;
        }
        if let Err(e) = inner.wal.append(&payload) {
            inner.poison(format!("WAL append failed: {e}"));
            return Err(e);
        }
        let sid = inner.series.get_or_insert(&tags);
        inner.memtable.push(sid, row);
        if inner.memtable.bytes() >= limit {
            // The row is already safe in the WAL, so a failed flush must not fail
            // the write; it is retried on the next write.
            if let Err(e) = self.flush_locked(inner) {
                warn(&format!("MemTable flush failed, will retry: {e}"));
            }
        }
        Ok(())
    }

    /// Statement-end hook: pushes buffered WAL entries to the OS and, if
    /// `durable`, to stable storage.
    pub fn sync_wal(&self, durable: bool) -> Result<()> {
        let mut inner = self.lock()?;
        inner.wal.sync(durable)
    }

    /// Seals the MemTable into chunks (one per time bucket).
    pub fn flush(&self) -> Result<()> {
        let mut guard = self.lock()?;
        let inner = &mut *guard;
        inner.check_writable()?;
        self.flush_locked(inner)
    }

    fn flush_locked(&self, inner: &mut Inner) -> Result<()> {
        if inner.memtable.is_empty() {
            return Ok(());
        }
        let dir = &self.config.dir;
        let covered = inner.wal.seq();
        inner.wal.sync(true)?;

        let mut written = Vec::new();
        let mut next_id = inner.manifest.next_chunk_id;
        let result = {
            let groups = inner.memtable.group(&self.schema, |ts| self.config.chunk_interval.bucket(ts));
            groups
                .iter()
                .try_for_each(|(bucket, series)| {
                    let spec = ChunkSpec { id: next_id, wal_seq: covered, bucket: *bucket, series };
                    written.push(write_chunk(dir, &self.schema, &spec)?);
                    next_id += 1;
                    Ok(())
                })
                .and_then(|()| sync_dir(dir))
        };
        if let Err(e) = result {
            for m in &written {
                let _ = remove_if_exists(&m.path);
            }
            return Err(e);
        }

        let mut manifest = inner.manifest.clone();
        manifest.wal_seq = covered;
        manifest.next_chunk_id = next_id;
        manifest.chunks.extend(written.iter().map(|m| m.id));
        inner.commit_manifest(dir, manifest)?;

        inner.chunks.extend(written.into_iter().map(Arc::new));
        inner.memtable.clear();
        inner.mem_generation += 1;
        inner.rotate_wal(dir)?;
        delete_wal_upto(dir, covered);
        Ok(())
    }

    /// Removes every row. Crash-safe: the MANIFEST switch is the commit point.
    pub fn truncate(&self) -> Result<()> {
        let mut guard = self.lock()?;
        let inner = &mut *guard;
        inner.check_writable()?;
        let dir = &self.config.dir;
        let old_seq = inner.wal.seq();

        let mut manifest = inner.manifest.clone();
        manifest.wal_seq = old_seq;
        manifest.chunks.clear();
        inner.commit_manifest(dir, manifest)?;

        let old_chunks = std::mem::take(&mut inner.chunks);
        inner.memtable.clear();
        inner.series.clear();
        inner.mem_generation += 1;
        inner.rotate_wal(dir)?;
        delete_chunk_files(&old_chunks);
        delete_wal_upto(dir, old_seq);
        Ok(())
    }

    /// Drops every chunk whose newest row is older than the retention cutoff
    /// relative to `now` (µs). Returns the number of chunks removed.
    pub fn apply_retention(&self, now: i64) -> Result<usize> {
        let Some(cutoff) = self.config.retention.cutoff(now) else { return Ok(0) };
        let mut guard = self.lock()?;
        let inner = &mut *guard;
        inner.check_writable()?;
        let (expired, kept): (Vec<_>, Vec<_>) = inner.chunks.iter().cloned().partition(|c| c.header.ts_max < cutoff);
        if expired.is_empty() {
            return Ok(0);
        }
        let mut manifest = inner.manifest.clone();
        manifest.chunks = kept.iter().map(|c| c.id).collect();
        inner.commit_manifest(&self.config.dir, manifest)?;
        inner.chunks = kept;
        delete_chunk_files(&expired);
        Ok(expired.len())
    }

    /// Opens a scan over a snapshot of the table. With `sorted`, rows come in
    /// timestamp order and the scan supports backward iteration.
    pub fn scan(&self, filter: &ScanFilter, sorted: bool) -> Result<Scan> {
        let filter = ResolvedFilter::new(&self.schema, filter)?;
        let inner = self.lock()?;
        let mut bloom_key = None;
        let empty = filter.is_empty_range()
            || match filter.exact_series_tags(self.schema.tag_indices().len()) {
                Some(tags) if !tags.is_empty() => match inner.series.lookup(&tags) {
                    Some(id) => {
                        bloom_key = Some(id);
                        false
                    }
                    None => true,
                },
                _ => false,
            };
        if empty {
            drop(inner);
            return Scan::new(self.schema.clone(), filter, None, Vec::new(), Vec::new(), sorted);
        }

        let source = MEM_SOURCE | inner.mem_generation;
        let mem: Vec<(Position, Row)> = inner
            .memtable
            .rows()
            .iter()
            .enumerate()
            .filter(|(_, m)| {
                bloom_key.map_or(true, |k| k == m.series_id)
                    && filter.ts_matches(self.schema.row_ts(&m.row))
                    && inner.series.tags(m.series_id).is_some_and(|t| filter.tags_match(t))
            })
            .map(|(i, m)| (Position { source, ordinal: i as u64 }, m.row.clone()))
            .collect();
        let chunks = inner.chunks.clone();
        drop(inner);
        Scan::new(self.schema.clone(), filter, bloom_key, chunks, mem, sorted)
    }

    /// Re-reads the row at `pos` (handler `rnd_pos`).
    pub fn fetch(&self, pos: Position) -> Result<Row> {
        let stale = || Error::NotFound("row position is no longer valid".into());
        let inner = self.lock()?;
        if pos.source & MEM_SOURCE != 0 {
            if pos.source & !MEM_SOURCE != inner.mem_generation {
                return Err(stale());
            }
            let idx = usize::try_from(pos.ordinal).map_err(|_| stale())?;
            return inner.memtable.rows().get(idx).map(|m| m.row.clone()).ok_or_else(stale);
        }
        let meta = inner
            .chunks
            .binary_search_by_key(&pos.source, |c| c.id)
            .ok()
            .and_then(|i| inner.chunks.get(i).cloned())
            .ok_or_else(stale)?;
        drop(inner);

        let mut base = 0u64;
        for (idx, e) in meta.series.iter().enumerate() {
            let n = u64::from(e.row_count);
            if pos.ordinal < base + n {
                let data = load_data(&meta)?;
                let rows = decode_series(&self.schema, &meta, &data, idx)?;
                return rows.into_iter().nth((pos.ordinal - base) as usize).ok_or_else(stale);
            }
            base += n;
        }
        Err(stale())
    }

    pub fn stats(&self) -> Result<TableStats> {
        let inner = self.lock()?;
        let mut s = TableStats {
            chunk_count: inner.chunks.len() as u32,
            memtable_rows: inner.memtable.len() as u64,
            series_count: inner.series.len() as u64,
            ..TableStats::default()
        };
        for c in &inner.chunks {
            s.row_count += c.header.row_count;
            s.data_bytes += c.raw_bytes;
            s.compressed_bytes += c.file_size;
        }
        s.row_count += s.memtable_rows;
        s.data_bytes += inner.memtable.bytes() as u64;
        Ok(s)
    }

    /// Estimated number of rows with `lo <= ts <= hi`, for the optimizer.
    pub fn estimate_rows(&self, lo: i64, hi: i64) -> Result<u64> {
        if lo > hi {
            return Ok(0);
        }
        let inner = self.lock()?;
        let mut total = 0u64;
        for c in inner.chunks.iter().filter(|c| c.overlaps(lo, hi)) {
            let (cmin, cmax) = (c.header.ts_min, c.header.ts_max);
            let span = (cmax as i128 - cmin as i128).max(1);
            let overlap = (hi.min(cmax) as i128 - lo.max(cmin) as i128 + 1).clamp(1, span);
            total += ((c.header.row_count as i128 * overlap / span) as u64).max(1);
        }
        total +=
            inner.memtable.rows().iter().filter(|m| (lo..=hi).contains(&self.schema.row_ts(&m.row))).count() as u64;
        Ok(total)
    }

    /// Verifies the CRC32 of every live chunk. Returns `(chunk file, problem)`
    /// for each failure; an empty list means the table is healthy.
    pub fn check(&self) -> Result<Vec<(String, String)>> {
        let chunks = self.lock()?.chunks.clone();
        let mut problems = Vec::new();
        for c in &chunks {
            match verify_file(&c.path) {
                Ok(()) => {}
                Err(Error::Corrupt(why)) => problems.push((c.file_name(), why)),
                Err(e) => problems.push((c.file_name(), e.to_string())),
            }
        }
        Ok(problems)
    }

    /// Snapshot of the live chunks' metadata (diagnostics).
    pub fn chunks(&self) -> Result<Vec<Arc<ChunkMeta>>> {
        Ok(self.lock()?.chunks.clone())
    }
}
