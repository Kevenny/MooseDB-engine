//! Table scans.
//!
//! A scan works on a snapshot taken at open time: the list of live chunks plus
//! a copy of the matching MemTable rows. Chunks are immutable, so the scan
//! never observes concurrent flushes, retention or truncation.
//!
//! Two modes exist:
//! * **streaming** — chunk by chunk, series by series, then MemTable rows.
//!   Forward only, memory bounded by one chunk. Used for full table scans.
//! * **sorted** — every matching row materialized and ordered by timestamp,
//!   with a bidirectional cursor. Used for index (range) scans, which must
//!   honour `HA_READ_ORDER`.

use std::collections::VecDeque;
use std::sync::Arc;

use crate::chunk::ChunkMeta;
use crate::chunk_reader::{decode_series, load_data};
use crate::error::{invalid, Error, Result};
use crate::schema::{Row, Schema, Value};

pub const POSITION_LEN: usize = 16;
/// High bit of `Position::source` marks MemTable rows; the remaining bits
/// carry the MemTable generation so stale positions are detected after a flush.
pub(crate) const MEM_SOURCE: u64 = 1 << 63;

/// Stable reference to a row, valid until the row's source is flushed,
/// compacted, expired or truncated.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
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

/// Predicate pushed down to the storage layer. Rows outside it are never
/// returned; the SQL layer still re-evaluates its full WHERE clause.
#[derive(Clone, Debug)]
pub struct ScanFilter {
    /// Inclusive timestamp bounds.
    pub ts_min: i64,
    pub ts_max: i64,
    /// `(schema column index, value)` equality predicates on TAG columns.
    pub tags: Vec<(usize, Value)>,
}

impl Default for ScanFilter {
    fn default() -> Self {
        ScanFilter { ts_min: i64::MIN, ts_max: i64::MAX, tags: Vec::new() }
    }
}

impl ScanFilter {
    pub fn range(ts_min: i64, ts_max: i64) -> ScanFilter {
        ScanFilter { ts_min, ts_max, tags: Vec::new() }
    }
}

/// `ScanFilter` with tag columns translated to positions in the tag list.
#[derive(Clone, Debug)]
pub(crate) struct ResolvedFilter {
    pub ts_min: i64,
    pub ts_max: i64,
    pub tags: Vec<(usize, Value)>,
}

impl ResolvedFilter {
    pub(crate) fn new(schema: &Schema, f: &ScanFilter) -> Result<ResolvedFilter> {
        let tags = f
            .tags
            .iter()
            .map(|(col, v)| {
                let pos =
                    schema.tag_position(*col).ok_or_else(|| invalid(format!("column {col} is not a TAG column")))?;
                Ok((pos, v.clone()))
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(ResolvedFilter { ts_min: f.ts_min, ts_max: f.ts_max, tags })
    }

    pub(crate) fn is_empty_range(&self) -> bool {
        self.ts_min > self.ts_max
    }

    pub(crate) fn ts_matches(&self, ts: i64) -> bool {
        self.ts_min <= ts && ts <= self.ts_max
    }

    pub(crate) fn tags_match(&self, series_tags: &[Value]) -> bool {
        self.tags.iter().all(|(pos, v)| series_tags.get(*pos) == Some(v))
    }

    /// The full tag tuple, if every tag column is constrained (in tag order).
    pub(crate) fn exact_series_tags(&self, tag_count: usize) -> Option<Vec<Value>> {
        let mut out = vec![None; tag_count];
        for (pos, v) in &self.tags {
            match out.get_mut(*pos) {
                Some(slot @ None) => *slot = Some(v.clone()),
                // The same tag constrained twice to different values matches nothing,
                // but that is rare enough to just fall back to the generic path.
                _ => return None,
            }
        }
        out.into_iter().collect()
    }
}

struct OpenChunk {
    meta: Arc<ChunkMeta>,
    data: Vec<u8>,
    next_series: usize,
    next_ordinal: u64,
}

enum Mode {
    Stream {
        chunks: VecDeque<Arc<ChunkMeta>>,
        current: Option<OpenChunk>,
        buffer: VecDeque<(Position, Row)>,
        mem: std::vec::IntoIter<(Position, Row)>,
    },
    Sorted {
        rows: Vec<(Position, Row)>,
        /// Index of the current row; `-1` = before first, `len` = after last.
        cursor: isize,
    },
}

pub struct Scan {
    schema: Arc<Schema>,
    filter: ResolvedFilter,
    /// Series id that every returned row must belong to, if known (Bloom pruning).
    bloom_key: Option<u64>,
    mode: Mode,
}

impl Scan {
    pub(crate) fn new(
        schema: Arc<Schema>,
        filter: ResolvedFilter,
        bloom_key: Option<u64>,
        chunks: Vec<Arc<ChunkMeta>>,
        mem: Vec<(Position, Row)>,
        sorted: bool,
    ) -> Result<Scan> {
        let mut scan = Scan {
            schema,
            filter,
            bloom_key,
            mode: Mode::Stream { chunks: chunks.into(), current: None, buffer: VecDeque::new(), mem: mem.into_iter() },
        };
        if sorted {
            let mut rows = Vec::new();
            while let Some(r) = scan.stream_next()? {
                rows.push(r);
            }
            let ts = scan.schema.ts_index();
            rows.sort_by_key(|(_, r)| match r.get(ts) {
                Some(Value::Timestamp(t)) => *t,
                _ => i64::MIN,
            });
            scan.mode = Mode::Sorted { rows, cursor: -1 };
        }
        Ok(scan)
    }

    pub fn is_sorted(&self) -> bool {
        matches!(self.mode, Mode::Sorted { .. })
    }

    /// Next row in scan order, or `None` at the end.
    pub fn next_row(&mut self) -> Result<Option<(Position, Row)>> {
        match &mut self.mode {
            Mode::Stream { .. } => self.stream_next(),
            Mode::Sorted { rows, cursor } => {
                let len = rows.len() as isize;
                *cursor = (*cursor + 1).min(len);
                Ok(rows.get(*cursor as usize).cloned())
            }
        }
    }

    /// Previous row (sorted scans only).
    pub fn prev_row(&mut self) -> Result<Option<(Position, Row)>> {
        match &mut self.mode {
            Mode::Stream { .. } => Err(Error::Unsupported("backward iteration on an unsorted scan".into())),
            Mode::Sorted { rows, cursor } => {
                *cursor = (*cursor - 1).max(-1);
                Ok(if *cursor < 0 { None } else { rows.get(*cursor as usize).cloned() })
            }
        }
    }

    /// Moves a sorted cursor past the last row, so `prev_row` yields the last row.
    pub fn seek_end(&mut self) -> Result<()> {
        match &mut self.mode {
            Mode::Stream { .. } => Err(Error::Unsupported("seek on an unsorted scan".into())),
            Mode::Sorted { rows, cursor } => {
                *cursor = rows.len() as isize;
                Ok(())
            }
        }
    }

    fn stream_next(&mut self) -> Result<Option<(Position, Row)>> {
        loop {
            let Mode::Stream { chunks, current, buffer, mem } = &mut self.mode else {
                return Ok(None);
            };
            if let Some(r) = buffer.pop_front() {
                return Ok(Some(r));
            }
            if let Some(open) = current {
                if open.next_series < open.meta.series.len() {
                    let idx = open.next_series;
                    open.next_series += 1;
                    let entry = &open.meta.series[idx];
                    let base = open.next_ordinal;
                    open.next_ordinal += u64::from(entry.row_count);
                    let wanted = entry.ts_min <= self.filter.ts_max
                        && entry.ts_max >= self.filter.ts_min
                        && self.filter.tags_match(&entry.tags)
                        && self.bloom_key.map_or(true, |k| k == entry.series_id);
                    if wanted {
                        let rows = decode_series(&self.schema, &open.meta, &open.data, idx)?;
                        let ts = self.schema.ts_index();
                        for (j, row) in rows.into_iter().enumerate() {
                            if let Some(Value::Timestamp(t)) = row.get(ts) {
                                if self.filter.ts_matches(*t) {
                                    buffer
                                        .push_back((Position { source: open.meta.id, ordinal: base + j as u64 }, row));
                                }
                            }
                        }
                    }
                    continue;
                }
                *current = None;
            }
            if let Some(meta) = chunks.pop_front() {
                let skip = !meta.overlaps(self.filter.ts_min, self.filter.ts_max)
                    || self.bloom_key.is_some_and(|k| !meta.bloom.may_contain(k));
                if !skip {
                    let data = load_data(&meta)?;
                    *current = Some(OpenChunk { meta, data, next_series: 0, next_ordinal: 0 });
                }
                continue;
            }
            return Ok(mem.next());
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn position_roundtrip() {
        let p = Position { source: MEM_SOURCE | 3, ordinal: 99 };
        assert_eq!(Position::from_bytes(&p.to_bytes()), p);
    }
}
