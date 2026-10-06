//! Statement-level atomic batches.
//!
//! A [`Batch`] collects the rows of one statement privately. Nothing reaches
//! the shared MemTable until [`Batch::commit`], which publishes every row at
//! once under the table mutex: a snapshot taken before sees none of them, one
//! taken after sees all.
//!
//! Durability, two paths:
//!
//! * **WAL path** (small batches): every row is logged as a ROW entry tagged
//!   with the batch id, commit appends one COMMIT entry. Replay applies the
//!   rows only if it finds the COMMIT, so a crash before the commit leaves no
//!   row of the batch. While the batch is open and has logged rows, flushes
//!   keep `replay_seq` at or before the segment of its first row.
//! * **Spill path** (the buffer reached `memtable_size`): the buffered rows
//!   are written as *staged* chunks (`.tmp` → fsync → rename, ids reserved,
//!   one per time bucket) that the MANIFEST does not list, and the batch stops
//!   logging. Commit stages the remainder and swaps the MANIFEST once to list
//!   all of them — that swap is the commit point; no COMMIT is written, so
//!   replay drops the batch's WAL rows. Staged chunks of a batch that never
//!   commits are unlisted orphans: removed on abort, by recovery after a
//!   crash. Memory stays bounded by the spill limit whatever the batch size.
//!
//! A TRUNCATE invalidates open batches (their commit fails): otherwise the
//! rows logged before it could come back at replay.

use std::collections::HashMap;
use std::sync::Arc;

use crate::chunk::ChunkMeta;
use crate::chunk_writer::ChunkBuilder;
use crate::crypto::CipherParams;
use crate::error::{invalid, Error, Result};
use crate::fsutil::{is_fsync_error, sync_dir};
use crate::index::series::{row_tags, series_key};
use crate::log::warn;
use crate::memtable::{group_rows, row_bytes, MemRow};
use crate::schema::{Row, Schema, Value};
use crate::table::Table;
use crate::wal;

pub struct Batch {
    table: Arc<Table>,
    id: u64,
    /// `Inner::epoch` when the batch began.
    epoch: u64,
    /// Buffered rows; `series_id` is an index into `local_tags` until commit.
    rows: Vec<MemRow>,
    bytes: usize,
    local_ids: HashMap<Vec<u8>, u64>,
    local_tags: Vec<Vec<Value>>,
    /// Rows were logged to the WAL and the batch is registered as open there.
    logged: bool,
    /// The batch moved to the spill path: no more WAL logging.
    spilled: bool,
    /// Staged chunks, not listed in the MANIFEST.
    staged: Vec<ChunkMeta>,
    spill_limit: usize,
    /// Commit reached its point of no return, or the batch was cleaned up.
    finished: bool,
}

fn stale() -> Error {
    invalid("the table was truncated while the batch was open")
}

impl Table {
    /// Starts a batch (one statement's worth of rows). See [`Batch`].
    pub fn begin_batch(self: &Arc<Self>) -> Result<Batch> {
        let mut guard = self.lock()?;
        let inner = &mut *guard;
        inner.check_writable()?;
        let id = inner.next_batch_id;
        inner.next_batch_id += 1;
        let epoch = inner.epoch;
        drop(guard);
        let spill_limit = usize::try_from(self.config.opts.memtable_size_bytes).unwrap_or(usize::MAX).max(1);
        Ok(Batch {
            table: self.clone(),
            id,
            epoch,
            rows: Vec::new(),
            bytes: 0,
            local_ids: HashMap::new(),
            local_tags: Vec::new(),
            logged: false,
            spilled: false,
            staged: Vec::new(),
            spill_limit,
            finished: false,
        })
    }
}

impl Batch {
    pub fn schema(&self) -> &Schema {
        &self.table.schema
    }

    /// Buffers one row (and logs it unless the batch has spilled). The row is
    /// invisible to readers until `commit`.
    pub fn write(&mut self, row: Row) -> Result<()> {
        if self.finished {
            return Err(invalid("the batch is already finished"));
        }
        let table = self.table.clone();
        table.schema.validate_row(&row)?;
        // Spill *before* taking the row: a failed spill then fails this row
        // alone instead of reporting an error for a row that was kept.
        if self.bytes >= self.spill_limit && !self.rows.is_empty() {
            self.spill()?;
        }
        let payload = if self.spilled {
            None
        } else {
            let p = wal::row_entry(self.id, &row);
            // An oversized row is the caller's mistake, not a storage failure.
            if p.len() > wal::max_entry() {
                return Err(invalid(format!("row of {} bytes exceeds the {} byte limit", p.len(), wal::max_entry())));
            }
            Some(p)
        };
        let tags = row_tags(&table.schema, &row);
        if let Some(p) = payload {
            let mut guard = table.lock()?;
            let inner = &mut *guard;
            inner.check_writable()?;
            if inner.epoch != self.epoch {
                return Err(stale());
            }
            if let Err(e) = inner.wal.append(&p) {
                if !matches!(e, Error::InvalidArg(_)) {
                    inner.poison(format!("WAL append failed: {e}"));
                }
                return Err(e);
            }
            let seg = inner.wal.seq();
            inner.open_batches.entry(self.id).or_insert(seg);
            self.logged = true;
        }
        let key = series_key(&tags);
        let local = match self.local_ids.get(&key) {
            Some(&l) => l,
            None => {
                let l = self.local_tags.len() as u64;
                self.local_tags.push(tags);
                self.local_ids.insert(key, l);
                l
            }
        };
        self.bytes += row_bytes(&row);
        self.rows.push(MemRow { series_id: local, row });
        Ok(())
    }

    /// Writes the buffered rows as staged chunks and drops them from memory.
    fn spill(&mut self) -> Result<()> {
        let metas = stage_rows(&self.table, &self.rows, &self.local_tags, self.epoch)?;
        self.staged.extend(metas);
        self.rows = Vec::new();
        self.bytes = 0;
        self.local_ids = HashMap::new();
        self.local_tags = Vec::new();
        if !self.spilled {
            self.spilled = true;
            if self.logged {
                // The rows logged so far will never be committed through the
                // WAL, so they no longer pin the replay start.
                if let Ok(mut g) = self.table.lock() {
                    g.open_batches.remove(&self.id);
                }
            }
        }
        Ok(())
    }

    /// Publishes every row of the batch at once. With `sync` the commit is
    /// durable on return (WAL fsync, or — spill path — the MANIFEST swap,
    /// which is always fsynced). The batch is consumed, also on error.
    pub fn commit(mut self, sync: bool) -> Result<()> {
        self.commit_inner(sync)
    }

    fn commit_inner(&mut self, sync: bool) -> Result<()> {
        let table = self.table.clone();
        if !self.spilled {
            if self.rows.is_empty() {
                self.cleanup();
                return Ok(());
            }
            if !self.logged {
                return Err(invalid("internal error: batch rows were not logged"));
            }
            let limit = usize::try_from(table.config.opts.memtable_size_bytes).unwrap_or(usize::MAX);
            let mut guard = table.lock()?;
            let inner = &mut *guard;
            inner.check_writable()?;
            if inner.epoch != self.epoch {
                return Err(stale());
            }
            if inner.memtable.bytes() >= limit.saturating_mul(2) {
                // Flushes have been failing; refuse rather than grow without bound.
                table
                    .flush_locked(inner)
                    .map_err(|e| Error::Full(format!("MemTable is full and flushing failed: {e}")))?;
            }
            if let Err(e) = inner.wal.append(&wal::commit_entry(self.id)) {
                if !matches!(e, Error::InvalidArg(_)) {
                    inner.poison(format!("WAL append failed: {e}"));
                }
                return Err(e);
            }
            // The COMMIT is in the log buffer. Make it reach the OS/disk before
            // publishing, all under the table mutex (no flush can slip in between):
            // if the sync fails the statement errors and no reader ever sees the rows.
            if let Err(e) = inner.wal.sync(sync) {
                inner.poison(format!("WAL sync failed: {e}"));
                return Err(e);
            }
            self.finished = true;
            inner.open_batches.remove(&self.id);
            let ids: Vec<u64> = self.local_tags.iter().map(|tags| inner.series.get_or_insert(tags)).collect();
            let mut rows = std::mem::take(&mut self.rows);
            for r in &mut rows {
                r.series_id = ids[r.series_id as usize];
            }
            inner.memtable.append(rows, self.bytes);
            inner.changed();
            if inner.memtable.bytes() >= limit {
                // The rows are safe in the WAL: a failed flush is retried later.
                if let Err(e) = table.flush_locked(inner) {
                    warn(&format!("MemTable flush failed, will retry: {e}"));
                }
            }
            return Ok(());
        }

        if !self.rows.is_empty() {
            self.spill()?;
        }
        if self.staged.is_empty() {
            self.cleanup();
            return Ok(());
        }
        let mut guard = table.lock()?;
        let inner = &mut *guard;
        inner.check_writable()?;
        if inner.epoch != self.epoch {
            return Err(stale());
        }
        let mut manifest = inner.manifest.clone();
        manifest.chunks.extend(self.staged.iter().map(|m| m.id));
        manifest.chunks.sort_unstable();
        // The files now belong to this MANIFEST swap. If it fails we cannot
        // know whether it reached the disk (the table is poisoned), so they
        // are neither listed in memory nor deleted: recovery decides.
        let staged = std::mem::take(&mut self.staged);
        self.finished = true;
        inner.commit_manifest(&table.config.dir, manifest)?;
        let mut chunks = inner.chunks.clone();
        chunks.extend(staged.into_iter().map(Arc::new));
        inner.replace_chunks(chunks);
        Ok(())
    }

    /// Discards the batch: its rows never become visible. (Dropping the batch
    /// without committing does the same.)
    pub fn abort(self) {}

    /// Releases everything an unfinished batch holds.
    fn cleanup(&mut self) {
        self.finished = true;
        for m in &self.staged {
            m.file.mark_obsolete();
        }
        self.staged.clear();
        self.rows = Vec::new();
        if self.logged {
            if let Ok(mut g) = self.table.lock() {
                g.open_batches.remove(&self.id);
            }
        }
    }
}

impl Drop for Batch {
    fn drop(&mut self) {
        if !self.finished {
            self.cleanup();
        }
    }
}

/// Writes `rows` (whose `series_id` indexes `local_tags`) as staged chunks,
/// one per time bucket. The files are fsynced and renamed into place but not
/// listed in the MANIFEST. Memory: the chunk being built plus the compressed
/// output of the others — never a copy of all the rows.
fn stage_rows(table: &Table, rows: &[MemRow], local_tags: &[Vec<Value>], epoch: u64) -> Result<Vec<ChunkMeta>> {
    let opts = &table.config.opts;
    let dir = &table.config.dir;
    let groups = group_rows(rows.iter(), &table.schema, |ts| opts.chunk_interval.bucket(ts));
    if groups.is_empty() {
        return Ok(Vec::new());
    }
    // Series ids and chunk ids come from shared state: take them in one short
    // critical section. (Series registered for a batch that later aborts stay
    // in the index, without rows; harmless, and gone after a reopen.)
    let (global, first_id, wal_seq) = {
        let mut guard = table.lock()?;
        let inner = &mut *guard;
        inner.check_writable()?;
        if inner.epoch != epoch {
            return Err(stale());
        }
        let mut global: HashMap<u64, u64> = HashMap::new();
        for series in groups.values() {
            for &local in series.keys() {
                global.entry(local).or_insert_with(|| inner.series.get_or_insert(&local_tags[local as usize]));
            }
        }
        let first = inner.manifest.next_chunk_id;
        inner.manifest.next_chunk_id += groups.len() as u64;
        (global, first, inner.wal.seq())
    };

    let now = crate::time::now_micros();
    let mut written: Vec<ChunkMeta> = Vec::with_capacity(groups.len());
    let result = (|| -> Result<()> {
        for (i, (bucket, series)) in groups.iter().enumerate() {
            let cipher = opts.encryption_key_id.map(CipherParams::for_new_file).transpose()?;
            let mut b = ChunkBuilder::new(&table.schema, opts.codec_for(bucket.1, now), cipher);
            let mut order: Vec<(u64, u64)> = series.keys().map(|&l| (global[&l], l)).collect();
            order.sort_unstable();
            for (sid, local) in order {
                b.add_series(sid, &local_tags[local as usize], &series[&local])?;
            }
            written.push(b.finish(dir, first_id + i as u64, wal_seq, *bucket)?);
        }
        sync_dir(dir)
    })();
    if let Err(e) = result {
        for m in &written {
            m.file.mark_obsolete();
        }
        if is_fsync_error(&e) {
            if let Ok(mut g) = table.lock() {
                g.poison(format!("fsync of a staged chunk failed: {e}"));
            }
        }
        return Err(e);
    }
    Ok(written)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fsutil::{fail_fsync_after, restore_fsync};
    use crate::options::{RawOptions, TableConfig};
    use crate::schema::{Column, ColumnType};

    fn open(dir: &std::path::Path) -> Arc<Table> {
        let schema = Schema::new(
            vec![
                Column { name: "ts".into(), ty: ColumnType::Timestamp },
                Column { name: "host".into(), ty: ColumnType::Tag },
            ],
            0,
        )
        .unwrap();
        let cfg = TableConfig::new(dir, schema, &RawOptions::default()).unwrap();
        Table::create(&cfg).unwrap();
        Arc::new(Table::open(cfg).unwrap())
    }

    fn row(i: i64) -> Row {
        vec![Value::Timestamp(i), Value::Bytes(b"h".to_vec())]
    }

    #[test]
    fn commit_fsync_failure_poisons_the_table() {
        let dir = tempfile::tempdir().unwrap();
        let t = open(&dir.path().join("t"));
        let mut b = t.begin_batch().unwrap();
        b.write(row(1)).unwrap();
        fail_fsync_after(0);
        let r = b.commit(true);
        restore_fsync();
        assert!(r.is_err());
        assert_eq!(t.stats().unwrap().row_count, 0, "rows of a failed commit are never visible");
        assert!(matches!(t.begin_batch(), Err(Error::ReadOnly(_))));
        assert!(matches!(t.write(row(2)), Err(Error::ReadOnly(_))));
    }

    #[test]
    fn oversized_batch_row_is_rejected_without_poisoning() {
        let dir = tempfile::tempdir().unwrap();
        let t = open(&dir.path().join("t"));
        let mut b = t.begin_batch().unwrap();
        b.write(row(1)).unwrap();
        wal::set_max_entry(16);
        let err = b.write(vec![Value::Timestamp(2), Value::Bytes(vec![b'x'; 100])]).unwrap_err();
        wal::set_max_entry(1 << 30);
        assert!(matches!(err, Error::InvalidArg(_)));
        b.write(row(3)).unwrap();
        b.commit(true).unwrap();
        assert_eq!(t.stats().unwrap().row_count, 2);
    }

    #[test]
    fn empty_batch_commit_is_a_noop() {
        let dir = tempfile::tempdir().unwrap();
        let t = open(&dir.path().join("t"));
        t.begin_batch().unwrap().commit(true).unwrap();
        assert_eq!(t.stats().unwrap().row_count, 0);
    }
}
