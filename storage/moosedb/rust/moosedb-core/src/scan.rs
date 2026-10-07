//! Table scans over immutable snapshots.
//!
//! A [`Snapshot`] is the list of live chunks plus a frozen view of the
//! MemTable. Chunk files are deleted only when the last snapshot referencing
//! them is dropped, and MemTable segments are shared, so a snapshot can read
//! every row it covers for as long as it lives — this is what makes row
//! positions (`rnd_pos`) safe while other sessions write, flush or compact.
//!
//! Scan modes:
//! * **stream** — chunk by chunk, series by series, then MemTable rows.
//!   Forward only; holds one decoded series at a time.
//! * **merge** — timestamp-ordered k-way merge of every (chunk, series) run
//!   and the sorted MemTable rows, ascending or descending. A run is decoded
//!   only when it reaches the top of the heap (its metadata bounds stand in
//!   until then), so `ORDER BY ts DESC LIMIT n` touches only the newest data.
//! * **materialized** — fallback when a merge changes direction mid-scan
//!   (`index_next` after `index_prev`): the range is collected and sorted.
//!
//! Rows are totally ordered by `(ts, source, ordinal)`.

use std::cmp::Ordering;
use std::collections::{BinaryHeap, HashSet, VecDeque};
use std::sync::Arc;

use crate::chunk::ChunkMeta;
use crate::chunk_reader::{decode_series, fetch_row};
use crate::error::{invalid, Error, Result};
use crate::memtable::MemSnapshot;
use crate::schema::{Row, Schema, Value};

pub const POSITION_LEN: usize = 16;
/// High bit of `Position::source` marks MemTable rows; the remaining bits
/// carry the MemTable generation the ordinal belongs to.
pub(crate) const MEM_SOURCE: u64 = 1 << 63;

/// Stable reference to a row within a snapshot.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Position {
    /// Chunk id, or `MEM_SOURCE | generation` for MemTable rows.
    pub source: u64,
    /// Row number within the source.
    pub ordinal: u64,
}

impl Position {
    pub fn to_bytes(self) -> [u8; POSITION_LEN] {
        let mut b = [0u8; POSITION_LEN];
        b[..8].copy_from_slice(&self.source.to_le_bytes());
        b[8..].copy_from_slice(&self.ordinal.to_le_bytes());
        b
    }

    pub fn from_bytes(b: &[u8; POSITION_LEN]) -> Position {
        let mut s = [0u8; 8];
        let mut o = [0u8; 8];
        s.copy_from_slice(&b[..8]);
        o.copy_from_slice(&b[8..]);
        Position { source: u64::from_le_bytes(s), ordinal: u64::from_le_bytes(o) }
    }
}

/// Total order key of a row.
type Key = (i64, u64, u64);

fn key_of(ts: i64, p: Position) -> Key {
    (ts, p.source, p.ordinal)
}

fn pos_of(k: Key) -> Position {
    Position { source: k.1, ordinal: k.2 }
}

/// Predicate pushed down to the storage layer. Rows outside it are never
/// returned; the SQL layer still re-evaluates its full WHERE clause.
#[derive(Clone, Debug)]
pub struct ScanFilter {
    /// Inclusive timestamp bounds.
    pub ts_min: i64,
    pub ts_max: i64,
    /// `(schema column index, value)` equality predicates on TAG columns.
    pub tags: Vec<(usize, Value)>,
    /// Restrict to these series ids (from `Table::series`), if set.
    pub series: Option<Vec<u64>>,
}

impl Default for ScanFilter {
    fn default() -> Self {
        ScanFilter { ts_min: i64::MIN, ts_max: i64::MAX, tags: Vec::new(), series: None }
    }
}

impl ScanFilter {
    pub fn range(ts_min: i64, ts_max: i64) -> ScanFilter {
        ScanFilter { ts_min, ts_max, ..ScanFilter::default() }
    }
}

/// `ScanFilter` resolved against the series index: tag predicates become a
/// set of series ids.
#[derive(Clone, Debug)]
pub(crate) struct ResolvedFilter {
    pub ts_min: i64,
    pub ts_max: i64,
    pub series: Option<HashSet<u64>>,
}

/// Bloom filters are consulted when at most this many series are wanted.
const BLOOM_PROBE_LIMIT: usize = 64;

impl ResolvedFilter {
    pub(crate) fn is_empty(&self) -> bool {
        self.ts_min > self.ts_max || self.series.as_ref().is_some_and(HashSet::is_empty)
    }

    pub(crate) fn ts_matches(&self, ts: i64) -> bool {
        self.ts_min <= ts && ts <= self.ts_max
    }

    pub(crate) fn series_matches(&self, id: u64) -> bool {
        self.series.as_ref().map_or(true, |s| s.contains(&id))
    }

    fn chunk_wanted(&self, c: &ChunkMeta) -> bool {
        if !c.overlaps(self.ts_min, self.ts_max) {
            return false;
        }
        match &self.series {
            Some(s) if s.len() <= BLOOM_PROBE_LIMIT => s.iter().any(|&id| c.bloom.may_contain(id)),
            _ => true,
        }
    }
}

/// Immutable view of a table: live chunks and frozen MemTable.
pub struct Snapshot {
    pub(crate) schema: Arc<Schema>,
    pub(crate) chunks: Vec<Arc<ChunkMeta>>,
    pub(crate) mem: MemSnapshot,
    pub(crate) version: u64,
}

impl Snapshot {
    /// Changes whenever the table's contents change.
    pub fn version(&self) -> u64 {
        self.version
    }

    pub fn schema(&self) -> &Schema {
        &self.schema
    }

    /// Re-reads the row at `pos`; `NotFound` if this snapshot does not cover it.
    pub fn fetch(&self, pos: Position) -> Result<Row> {
        let missing = || Error::NotFound("row position is not covered by this snapshot".into());
        if pos.source & MEM_SOURCE != 0 {
            if pos.source & !MEM_SOURCE != self.mem.generation {
                return Err(missing());
            }
            return self.mem.get(pos.ordinal).map(|m| m.row.clone()).ok_or_else(missing);
        }
        let meta = self
            .chunks
            .binary_search_by_key(&pos.source, |c| c.id)
            .ok()
            .and_then(|i| self.chunks.get(i))
            .ok_or_else(missing)?;
        let idx = meta.series_for_ordinal(pos.ordinal).ok_or_else(missing)?;
        // Not `decode_series(..).nth(..)`: that decodes the whole series per
        // fetch (O(series) per `rnd_pos`); this goes through the series cache.
        let within = usize::try_from(pos.ordinal - meta.series_offsets[idx]).map_err(|_| missing())?;
        fetch_row(&self.schema, meta, idx, within)?.ok_or_else(missing)
    }
}

// ─── stream mode ────────────────────────────────────────────────────────────

struct StreamState {
    next_chunk: usize,
    next_series: usize,
    buffer: VecDeque<(Position, Row)>,
    mem_next: u64,
}

// ─── merge mode ─────────────────────────────────────────────────────────────

enum Run {
    /// A series of a chunk; `rows` is filled when the run first reaches the top.
    Chunk {
        chunk: usize,
        series: usize,
        rows: Option<VecDeque<(Key, Row)>>,
    },
    Mem {
        rows: VecDeque<(Key, Row)>,
    },
}

struct HeapEntry {
    key: Key,
    run: usize,
    desc: bool,
}

impl PartialEq for HeapEntry {
    fn eq(&self, o: &Self) -> bool {
        self.cmp(o) == Ordering::Equal
    }
}
impl Eq for HeapEntry {}
impl PartialOrd for HeapEntry {
    fn partial_cmp(&self, o: &Self) -> Option<Ordering> {
        Some(self.cmp(o))
    }
}
impl Ord for HeapEntry {
    /// BinaryHeap is a max-heap: ascending merges invert the order.
    fn cmp(&self, o: &Self) -> Ordering {
        let c = self.key.cmp(&o.key).then(self.run.cmp(&o.run));
        if self.desc {
            c
        } else {
            c.reverse()
        }
    }
}

struct MergeState {
    desc: bool,
    runs: Vec<Run>,
    heap: BinaryHeap<HeapEntry>,
    /// Key of the row returned last.
    last: Option<Key>,
}

enum Mode {
    Stream(StreamState),
    Merge(MergeState),
    Materialized { rows: Vec<(Key, Row)>, cursor: isize },
}

pub struct Scan {
    snap: Arc<Snapshot>,
    filter: ResolvedFilter,
    mode: Mode,
}

impl Scan {
    pub(crate) fn new(snap: Arc<Snapshot>, filter: ResolvedFilter, sorted: bool) -> Result<Scan> {
        let mut scan = Scan {
            snap,
            filter,
            mode: Mode::Stream(StreamState { next_chunk: 0, next_series: 0, buffer: VecDeque::new(), mem_next: 0 }),
        };
        if sorted {
            scan.mode = Mode::Merge(scan.new_merge(false));
        }
        Ok(scan)
    }

    pub fn snapshot(&self) -> &Arc<Snapshot> {
        &self.snap
    }

    pub fn is_sorted(&self) -> bool {
        !matches!(self.mode, Mode::Stream(_))
    }

    /// Next row in scan order, or `None` at the end.
    pub fn next_row(&mut self) -> Result<Option<(Position, Row)>> {
        match &self.mode {
            Mode::Stream(_) => self.stream_next(),
            Mode::Merge(m) if !m.desc => self.merge_next(),
            Mode::Merge(_) => {
                self.materialize()?;
                self.materialized_step(1)
            }
            Mode::Materialized { .. } => self.materialized_step(1),
        }
    }

    /// Previous row (sorted scans only).
    pub fn prev_row(&mut self) -> Result<Option<(Position, Row)>> {
        match &self.mode {
            Mode::Stream(_) => Err(Error::Unsupported("backward iteration on an unsorted scan".into())),
            Mode::Merge(m) if m.desc => self.merge_next(),
            Mode::Merge(_) => {
                self.materialize()?;
                self.materialized_step(-1)
            }
            Mode::Materialized { .. } => self.materialized_step(-1),
        }
    }

    /// Positions a sorted scan after its last row: `prev_row` then yields
    /// rows in descending order.
    pub fn seek_end(&mut self) -> Result<()> {
        if matches!(self.mode, Mode::Stream(_)) {
            return Err(Error::Unsupported("seek on an unsorted scan".into()));
        }
        self.mode = Mode::Merge(self.new_merge(true));
        Ok(())
    }

    // ── stream ──

    fn stream_next(&mut self) -> Result<Option<(Position, Row)>> {
        let Mode::Stream(st) = &mut self.mode else { return Ok(None) };
        let snap = &self.snap;
        let f = &self.filter;
        loop {
            if let Some(r) = st.buffer.pop_front() {
                return Ok(Some(r));
            }
            if let Some(meta) = snap.chunks.get(st.next_chunk) {
                if st.next_series == 0 && !f.chunk_wanted(meta) {
                    st.next_chunk += 1;
                    continue;
                }
                let Some(entry) = meta.series.get(st.next_series) else {
                    st.next_chunk += 1;
                    st.next_series = 0;
                    continue;
                };
                let idx = st.next_series;
                st.next_series += 1;
                if entry.ts_min <= f.ts_max && entry.ts_max >= f.ts_min && f.series_matches(entry.series_id) {
                    let base = meta.series_offsets[idx];
                    for (j, row) in decode_series(&snap.schema, meta, idx)?.into_iter().enumerate() {
                        if f.ts_matches(snap.schema.row_ts(&row)) {
                            st.buffer.push_back((Position { source: meta.id, ordinal: base + j as u64 }, row));
                        }
                    }
                }
                continue;
            }
            let source = MEM_SOURCE | snap.mem.generation;
            while let Some(m) = snap.mem.get(st.mem_next) {
                let ordinal = st.mem_next;
                st.mem_next += 1;
                if f.series_matches(m.series_id) && f.ts_matches(snap.schema.row_ts(&m.row)) {
                    return Ok(Some((Position { source, ordinal }, m.row.clone())));
                }
            }
            return Ok(None);
        }
    }

    // ── merge ──

    fn new_merge(&self, desc: bool) -> MergeState {
        let f = &self.filter;
        let mut runs = Vec::new();
        let mut heap = BinaryHeap::new();
        if !f.is_empty() {
            for (ci, c) in self.snap.chunks.iter().enumerate() {
                if !f.chunk_wanted(c) {
                    continue;
                }
                for (si, e) in c.series.iter().enumerate() {
                    if e.ts_min > f.ts_max || e.ts_max < f.ts_min || !f.series_matches(e.series_id) {
                        continue;
                    }
                    let base = c.series_offsets[si];
                    // A bound that sorts no later than the run's real first row.
                    let key = if desc {
                        (e.ts_max.min(f.ts_max), c.id, (base + u64::from(e.row_count)).saturating_sub(1))
                    } else {
                        (e.ts_min.max(f.ts_min), c.id, base)
                    };
                    heap.push(HeapEntry { key, run: runs.len(), desc });
                    runs.push(Run::Chunk { chunk: ci, series: si, rows: None });
                }
            }
            let source = MEM_SOURCE | self.snap.mem.generation;
            let mut mem: Vec<(Key, Row)> = self
                .snap
                .mem
                .iter()
                .filter(|(_, m)| f.series_matches(m.series_id) && f.ts_matches(self.snap.schema.row_ts(&m.row)))
                .map(|(o, m)| (key_of(self.snap.schema.row_ts(&m.row), Position { source, ordinal: o }), m.row.clone()))
                .collect();
            mem.sort_by_key(|(k, _)| *k);
            if desc {
                mem.reverse();
            }
            if let Some((k, _)) = mem.first() {
                heap.push(HeapEntry { key: *k, run: runs.len(), desc });
                runs.push(Run::Mem { rows: mem.into() });
            }
        }
        MergeState { desc, runs, heap, last: None }
    }

    fn merge_next(&mut self) -> Result<Option<(Position, Row)>> {
        let Mode::Merge(m) = &mut self.mode else { return Ok(None) };
        let snap = &self.snap;
        let f = &self.filter;
        while let Some(top) = m.heap.pop() {
            let desc = m.desc;
            let Some(run) = m.runs.get_mut(top.run) else { continue };
            let rows = match run {
                Run::Chunk { chunk, series, rows } => {
                    if rows.is_none() {
                        let meta = &snap.chunks[*chunk];
                        let base = meta.series_offsets[*series];
                        let mut decoded: VecDeque<(Key, Row)> = decode_series(&snap.schema, meta, *series)?
                            .into_iter()
                            .enumerate()
                            .map(|(j, r)| {
                                (
                                    key_of(
                                        snap.schema.row_ts(&r),
                                        Position { source: meta.id, ordinal: base + j as u64 },
                                    ),
                                    r,
                                )
                            })
                            .filter(|(k, _)| f.ts_matches(k.0))
                            .collect();
                        if desc {
                            decoded = decoded.into_iter().rev().collect();
                        }
                        let first = decoded.front().map(|(k, _)| *k);
                        *rows = Some(decoded);
                        if let Some(k) = first {
                            m.heap.push(HeapEntry { key: k, run: top.run, desc });
                        }
                        continue;
                    }
                    rows.as_mut()
                }
                Run::Mem { rows } => Some(rows),
            };
            let Some(rows) = rows else { continue };
            let Some((key, row)) = rows.pop_front() else { continue };
            if let Some((next, _)) = rows.front() {
                m.heap.push(HeapEntry { key: *next, run: top.run, desc });
            }
            m.last = Some(key);
            return Ok(Some((pos_of(key), row)));
        }
        Ok(None)
    }

    // ── materialized ──

    /// Switches a merge to materialized mode, keeping the cursor on the row
    /// returned last.
    fn materialize(&mut self) -> Result<()> {
        let Mode::Merge(m) = &self.mode else { return Ok(()) };
        let (desc, last) = (m.desc, m.last);
        let fresh = self.new_merge(false);
        self.mode = Mode::Merge(fresh);
        let mut rows = Vec::new();
        while let Some((p, r)) = self.merge_next()? {
            let ts = self.snap.schema.row_ts(&r);
            rows.push((key_of(ts, p), r));
        }
        let cursor = match last {
            Some(k) => rows.binary_search_by_key(&k, |(rk, _)| *rk).map_or_else(|i| i as isize, |i| i as isize),
            None if desc => rows.len() as isize,
            None => -1,
        };
        self.mode = Mode::Materialized { rows, cursor };
        Ok(())
    }

    fn materialized_step(&mut self, step: isize) -> Result<Option<(Position, Row)>> {
        let Mode::Materialized { rows, cursor } = &mut self.mode else { return Ok(None) };
        let len = rows.len() as isize;
        *cursor = (*cursor + step).clamp(-1, len);
        Ok(usize::try_from(*cursor).ok().and_then(|i| rows.get(i)).map(|(k, r)| (pos_of(*k), r.clone())))
    }
}

pub(crate) fn check_tag_columns(schema: &Schema, f: &ScanFilter) -> Result<()> {
    for (col, _) in &f.tags {
        if schema.tag_position(*col).is_none() {
            return Err(invalid(format!("column {col} is not a TAG column")));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn position_roundtrip() {
        let p = Position { source: MEM_SOURCE | 3, ordinal: 99 };
        assert_eq!(Position::from_bytes(&p.to_bytes()), p);
    }

    #[test]
    fn heap_order() {
        let mut h = BinaryHeap::new();
        for (i, ts) in [5, 1, 3].iter().enumerate() {
            h.push(HeapEntry { key: (*ts, 0, 0), run: i, desc: false });
        }
        assert_eq!(h.pop().unwrap().key.0, 1);
        let mut h = BinaryHeap::new();
        for (i, ts) in [5, 1, 3].iter().enumerate() {
            h.push(HeapEntry { key: (*ts, 0, 0), run: i, desc: true });
        }
        assert_eq!(h.pop().unwrap().key.0, 5);
    }
}
