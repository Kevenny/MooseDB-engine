//! Compaction: merges the chunks of one time bucket into a single chunk and
//! re-encodes chunks that turned cold with the table's cold codec (ZSTD).
//!
//! Every flush adds one chunk per touched bucket, so a busy bucket
//! accumulates many small chunks; merging them makes series contiguous
//! (better compression, fewer blocks to read per query).
//!
//! Protocol (crash-safe, concurrent with reads and writes):
//! 1. under `maint`: pick the inputs and reserve an id (short `inner` lock);
//! 2. build the output chunk *without* holding `inner`, one series at a time;
//! 3. take `inner`, check the inputs are all still live (retention or
//!    TRUNCATE may have removed some meanwhile — then the output is
//!    discarded), switch the MANIFEST, mark the inputs obsolete.
//!
//! A crash before step 3 leaves an unlisted output file, deleted by recovery.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::atomic::Ordering;
use std::sync::Arc;

use crate::chunk::ChunkMeta;
use crate::chunk_reader::decode_series;
use crate::chunk_writer::ChunkBuilder;
use crate::crypto::CipherParams;
use crate::error::{Error, Result};
use crate::fsutil::sync_dir;
use crate::schema::Row;
use crate::table::Table;

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct CompactionReport {
    /// Buckets rewritten.
    pub groups: usize,
    pub chunks_in: usize,
    pub chunks_out: usize,
}

impl Table {
    /// Explicit compaction (OPTIMIZE TABLE, `CALL tideflow_compact`): every
    /// bucket overlapping `[lo, hi]` that holds more than one chunk, or a
    /// chunk not yet in the cold codec, is rewritten.
    pub fn compact(&self, lo: i64, hi: i64, now: i64) -> Result<CompactionReport> {
        self.run_compaction(now, |bucket| bucket.0 <= hi && bucket.1 > lo, true)
    }

    /// Background compaction: buckets with at least
    /// `tideflow_compaction_trigger_chunks` chunks, and cold re-encoding.
    pub fn compact_auto(&self, now: i64) -> Result<CompactionReport> {
        self.run_compaction(now, |_| true, false)
    }

    /// Whether `compact_auto` would do anything (cheap; scheduler use).
    pub(crate) fn compaction_pending(&self, now: i64) -> bool {
        self.lock().map(|inner| !self.plan(&inner.chunks, now, &|_| true, false).is_empty()).unwrap_or(false)
    }

    fn plan(
        &self,
        chunks: &[Arc<ChunkMeta>],
        now: i64,
        in_scope: &dyn Fn((i64, i64)) -> bool,
        force: bool,
    ) -> Vec<Vec<Arc<ChunkMeta>>> {
        let opts = &self.config.opts;
        let trigger = crate::settings::get().compaction_trigger_chunks() as usize;
        let mut by_bucket: BTreeMap<(i64, i64), Vec<Arc<ChunkMeta>>> = BTreeMap::new();
        for c in chunks.iter().filter(|c| in_scope(c.bucket)) {
            by_bucket.entry(c.bucket).or_default().push(c.clone());
        }
        by_bucket
            .into_iter()
            .filter(|(bucket, group)| {
                let cold = !opts.is_hot(bucket.1, now);
                let recode = cold && group.iter().any(|c| c.header.codec_id() != opts.cold_codec().id());
                group.len() >= trigger || (force && group.len() > 1) || recode
            })
            .map(|(_, g)| g)
            .collect()
    }

    fn run_compaction(&self, now: i64, in_scope: impl Fn((i64, i64)) -> bool, force: bool) -> Result<CompactionReport> {
        let _maint = self.maint.lock().map_err(|_| Error::ReadOnly("maintenance lock poisoned".into()))?;
        if self.defunct.load(Ordering::Acquire) {
            return Ok(CompactionReport::default());
        }
        let groups = {
            let inner = self.lock()?;
            inner.check_writable()?;
            self.plan(&inner.chunks, now, &in_scope, force)
        };
        let mut report = CompactionReport::default();
        for group in groups {
            let n = group.len();
            if self.compact_group(group, now)? {
                report.groups += 1;
                report.chunks_in += n;
                report.chunks_out += 1;
            }
        }
        Ok(report)
    }

    /// Returns false when the inputs changed concurrently and nothing was done.
    fn compact_group(&self, group: Vec<Arc<ChunkMeta>>, now: i64) -> Result<bool> {
        let ids: Vec<u64> = group.iter().map(|c| c.id).collect();
        let id = {
            let mut inner = self.lock()?;
            inner.check_writable()?;
            if !ids.iter().all(|i| inner.chunks.iter().any(|c| c.id == *i)) {
                return Ok(false);
            }
            let id = inner.manifest.next_chunk_id;
            inner.manifest.next_chunk_id += 1;
            inner.compacting.extend(ids.iter().copied());
            id
        };

        let built = self.build_merged(&group, id, now);

        let mut inner = self.lock()?;
        for i in &ids {
            inner.compacting.remove(i);
        }
        let meta = built?;
        let still_live = ids.iter().all(|i| inner.chunks.iter().any(|c| c.id == *i));
        if !still_live || self.defunct.load(Ordering::Acquire) {
            meta.file.mark_obsolete();
            return Ok(false);
        }
        let mut chunks: Vec<Arc<ChunkMeta>> = inner.chunks.iter().filter(|c| !ids.contains(&c.id)).cloned().collect();
        let mut manifest = inner.manifest.clone();
        manifest.chunks = chunks.iter().map(|c| c.id).chain(std::iter::once(meta.id)).collect();
        manifest.chunks.sort_unstable();
        inner.commit_manifest(&self.config.dir, manifest)?;
        chunks.push(Arc::new(meta));
        inner.replace_chunks(chunks);
        Ok(true)
    }

    fn build_merged(&self, group: &[Arc<ChunkMeta>], id: u64, now: i64) -> Result<ChunkMeta> {
        let opts = &self.config.opts;
        let bucket = group[0].bucket;
        let cipher = opts.encryption_key_id.map(CipherParams::for_new_file).transpose()?;
        let mut builder = ChunkBuilder::new(&self.schema, opts.codec_for(bucket.1, now), cipher);

        let positions: Vec<HashMap<u64, usize>> =
            group.iter().map(|c| c.series.iter().enumerate().map(|(i, e)| (e.series_id, i)).collect()).collect();
        let series_ids: BTreeSet<u64> = positions.iter().flat_map(|m| m.keys().copied()).collect();
        let ts = self.schema.ts_index();
        for sid in series_ids {
            let mut rows: Vec<Row> = Vec::new();
            let mut tags = Vec::new();
            for (c, pos) in group.iter().zip(&positions) {
                if let Some(&idx) = pos.get(&sid) {
                    rows.extend(decode_series(&self.schema, c, idx)?);
                    tags.clone_from(&c.series[idx].tags);
                }
            }
            // Inputs are in id (= write) order and each is sorted, so a stable
            // sort keeps arrival order among equal timestamps.
            rows.sort_by_key(|r| match r.get(ts) {
                Some(crate::schema::Value::Timestamp(t)) => *t,
                _ => i64::MIN,
            });
            let refs: Vec<&Row> = rows.iter().collect();
            builder.add_series(sid, &tags, &refs)?;
        }
        let wal_seq = group.iter().map(|c| c.header.wal_seq).max().unwrap_or(0);
        let meta = builder.finish(&self.config.dir, id, wal_seq, bucket)?;
        if let Err(e) = sync_dir(&self.config.dir) {
            meta.file.mark_obsolete();
            return Err(e);
        }
        Ok(meta)
    }
}
