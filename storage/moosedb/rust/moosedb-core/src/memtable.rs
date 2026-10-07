//! In-memory buffer of rows not yet sealed into a chunk.
//!
//! Rows are kept in arrival order in immutable, reference-counted segments
//! plus one mutable "active" segment. Taking a snapshot freezes the active
//! segment (O(1), no copying), so a scan sees a stable view and row positions
//! stay resolvable even after the MemTable is flushed: the snapshot keeps its
//! segments alive. Grouping by bucket/series and sorting happen at flush.

use std::collections::BTreeMap;
use std::mem;
use std::sync::Arc;

use crate::schema::{Row, Schema, Value};

pub(crate) struct MemRow {
    pub series_id: u64,
    pub row: Row,
}

/// Frozen segments are merged once there are more than this many, so a
/// workload alternating single-row writes and scans stays efficient.
const MAX_FROZEN: usize = 64;

/// Row count and timestamp range of one segment, kept as rows arrive so the
/// optimizer range estimate never has to visit the rows.
#[derive(Clone, Copy)]
struct SegStats {
    len: usize,
    ts_min: i64,
    ts_max: i64,
}

impl SegStats {
    const EMPTY: SegStats = SegStats { len: 0, ts_min: i64::MAX, ts_max: i64::MIN };

    fn add(&mut self, len: usize, ts_min: i64, ts_max: i64) {
        self.len += len;
        self.ts_min = self.ts_min.min(ts_min);
        self.ts_max = self.ts_max.max(ts_max);
    }

    /// Rows with `lo <= ts <= hi`, assuming they are spread evenly over the
    /// range of the segment (the same model used for chunks).
    fn estimate(&self, lo: i64, hi: i64) -> u64 {
        if self.len == 0 || hi < self.ts_min || lo > self.ts_max {
            return 0;
        }
        if lo <= self.ts_min && hi >= self.ts_max {
            return self.len as u64;
        }
        let span = (i128::from(self.ts_max) - i128::from(self.ts_min)).max(1);
        let overlap = (i128::from(hi.min(self.ts_max)) - i128::from(lo.max(self.ts_min)) + 1).clamp(1, span);
        ((self.len as i128 * overlap / span) as u64).max(1)
    }
}

pub(crate) struct MemTable {
    frozen: Vec<Arc<Vec<MemRow>>>,
    /// Statistics of each `frozen` segment, same order.
    frozen_stats: Vec<SegStats>,
    active: Vec<MemRow>,
    active_stats: SegStats,
    len: usize,
    bytes: usize,
}

impl Default for MemTable {
    fn default() -> Self {
        MemTable {
            frozen: Vec::new(),
            frozen_stats: Vec::new(),
            active: Vec::new(),
            active_stats: SegStats::EMPTY,
            len: 0,
            bytes: 0,
        }
    }
}

/// Immutable view of the MemTable at one instant.
#[derive(Clone, Default)]
pub(crate) struct MemSnapshot {
    pub generation: u64,
    segments: Vec<Arc<Vec<MemRow>>>,
}

impl MemSnapshot {
    /// `(ordinal, row)` in arrival order.
    pub(crate) fn iter(&self) -> impl Iterator<Item = (u64, &MemRow)> {
        self.segments.iter().flat_map(|s| s.iter()).enumerate().map(|(i, r)| (i as u64, r))
    }

    pub(crate) fn get(&self, ordinal: u64) -> Option<&MemRow> {
        let mut idx = usize::try_from(ordinal).ok()?;
        for s in &self.segments {
            if idx < s.len() {
                return s.get(idx);
            }
            idx -= s.len();
        }
        None
    }

    #[cfg(test)]
    pub(crate) fn len(&self) -> usize {
        self.segments.iter().map(|s| s.len()).sum()
    }
}

/// Accounting size of one row.
pub(crate) fn row_bytes(row: &[Value]) -> usize {
    mem::size_of::<MemRow>() + row.iter().map(Value::mem_size).sum::<usize>()
}

/// Batches at least this long become their own segment instead of being
/// copied into the active one.
const OWN_SEGMENT_ROWS: usize = 512;

impl MemTable {
    /// Appends one row whose timestamp is `ts`.
    pub(crate) fn push(&mut self, ts: i64, series_id: u64, row: Row) {
        self.bytes += row_bytes(&row);
        self.len += 1;
        self.active_stats.add(1, ts, ts);
        self.active.push(MemRow { series_id, row });
    }

    fn freeze_active(&mut self) {
        if !self.active.is_empty() {
            self.frozen.push(Arc::new(mem::take(&mut self.active)));
            self.frozen_stats.push(mem::replace(&mut self.active_stats, SegStats::EMPTY));
        }
    }

    /// Appends the rows of a committed batch at once, in order. `bytes` is
    /// the sum of `row_bytes` over `rows`, `ts_min`/`ts_max` their timestamp
    /// range (both computed by the batch outside the lock). The caller holds
    /// the table lock, so no snapshot can see a part of the batch.
    pub(crate) fn append(&mut self, rows: Vec<MemRow>, bytes: usize, ts_min: i64, ts_max: i64) {
        self.len += rows.len();
        self.bytes += bytes;
        if rows.len() >= OWN_SEGMENT_ROWS {
            self.freeze_active();
            let mut stats = SegStats::EMPTY;
            stats.add(rows.len(), ts_min, ts_max);
            self.frozen.push(Arc::new(rows));
            self.frozen_stats.push(stats);
        } else {
            self.active_stats.add(rows.len(), ts_min, ts_max);
            self.active.extend(rows);
        }
    }

    pub(crate) fn len(&self) -> usize {
        self.len
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.len == 0
    }

    pub(crate) fn bytes(&self) -> usize {
        self.bytes
    }

    pub(crate) fn clear(&mut self) {
        *self = MemTable::default();
    }

    fn rows(&self) -> impl Iterator<Item = &MemRow> {
        self.frozen.iter().flat_map(|s| s.iter()).chain(self.active.iter())
    }

    /// Freezes the active segment and returns a view sharing all segments.
    pub(crate) fn snapshot(&mut self, generation: u64) -> MemSnapshot {
        self.freeze_active();
        if self.frozen.len() > MAX_FROZEN {
            // Copy into one segment; snapshots holding the old ones are unaffected
            // and ordinals are preserved because order is preserved.
            let merged: Vec<MemRow> =
                self.rows().map(|m| MemRow { series_id: m.series_id, row: m.row.clone() }).collect();
            let mut stats = SegStats::EMPTY;
            for s in &self.frozen_stats {
                stats.add(s.len, s.ts_min, s.ts_max);
            }
            self.frozen = vec![Arc::new(merged)];
            self.frozen_stats = vec![stats];
        }
        MemSnapshot { generation, segments: self.frozen.clone() }
    }

    /// Estimated rows with `lo <= ts <= hi`, for the optimizer. Costs
    /// O(segments) from per-segment statistics, never a walk over the rows
    /// (it runs under the table mutex, once per `records_in_range`).
    pub(crate) fn estimate_in_range(&self, lo: i64, hi: i64) -> u64 {
        self.frozen_stats.iter().chain(std::iter::once(&self.active_stats)).map(|s| s.estimate(lo, hi)).sum()
    }

    /// Groups rows by `bucket(ts)` and then by series, each series sorted by
    /// timestamp (stable, so equal timestamps keep arrival order).
    pub(crate) fn group(
        &self,
        schema: &Schema,
        bucket: impl Fn(i64) -> (i64, i64),
    ) -> BTreeMap<(i64, i64), BTreeMap<u64, Vec<&Row>>> {
        group_rows(self.rows(), schema, bucket)
    }
}

/// Groups rows by `bucket(ts)` and then by `series_id`, each series sorted by
/// timestamp (stable, so equal timestamps keep arrival order).
pub(crate) fn group_rows<'a>(
    rows: impl Iterator<Item = &'a MemRow>,
    schema: &Schema,
    bucket: impl Fn(i64) -> (i64, i64),
) -> BTreeMap<(i64, i64), BTreeMap<u64, Vec<&'a Row>>> {
    let mut out: BTreeMap<(i64, i64), BTreeMap<u64, Vec<&Row>>> = BTreeMap::new();
    for r in rows {
        out.entry(bucket(schema.row_ts(&r.row))).or_default().entry(r.series_id).or_default().push(&r.row);
    }
    for series in out.values_mut() {
        for rows in series.values_mut() {
            rows.sort_by_key(|r| schema.row_ts(r));
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn row(i: i64) -> Row {
        vec![Value::Timestamp(i)]
    }

    #[test]
    fn range_estimates_come_from_segment_statistics() {
        let mut m = MemTable::default();
        for i in 0..1000 {
            m.push(i, 0, row(i));
        }
        assert_eq!(m.estimate_in_range(i64::MIN, i64::MAX), 1000);
        assert_eq!(m.estimate_in_range(2000, 3000), 0);
        let half = m.estimate_in_range(0, 499);
        assert!((450..=550).contains(&half), "{half}");
        // A frozen segment, a batch segment and the active one.
        let _ = m.snapshot(0);
        let big: Vec<MemRow> = (2000..2600).map(|i| MemRow { series_id: 0, row: row(i) }).collect();
        m.append(big, 0, 2000, 2599);
        m.push(5000, 0, row(5000));
        assert_eq!(m.estimate_in_range(i64::MIN, i64::MAX), 1601);
        assert_eq!(m.estimate_in_range(2000, 2599), 600);
        assert_eq!(m.estimate_in_range(4000, 6000), 1);
        // Merging segments keeps the statistics.
        for i in 0..100 {
            m.push(6000 + i, 0, row(6000 + i));
            let _ = m.snapshot(0);
        }
        assert_eq!(m.estimate_in_range(i64::MIN, i64::MAX), 1701);
        m.clear();
        assert_eq!(m.estimate_in_range(i64::MIN, i64::MAX), 0);
    }

    #[test]
    fn snapshots_are_stable_and_share_segments() {
        let mut m = MemTable::default();
        m.push(1, 1, row(1));
        m.push(2, 1, row(2));
        let s1 = m.snapshot(0);
        m.push(3, 2, row(3));
        let s2 = m.snapshot(0);
        assert_eq!(s1.len(), 2);
        assert_eq!(s2.len(), 3);
        assert_eq!(s2.get(2).unwrap().row, row(3));
        m.clear();
        assert_eq!(s2.get(0).unwrap().row, row(1), "snapshot outlives the MemTable contents");
        assert!(s2.get(3).is_none());
    }

    #[test]
    fn many_snapshots_merge_segments_without_renumbering() {
        let mut m = MemTable::default();
        let mut snaps = Vec::new();
        for i in 0..200 {
            m.push(i, 0, row(i));
            snaps.push(m.snapshot(0));
        }
        assert!(m.frozen.len() <= MAX_FROZEN);
        let last = m.snapshot(0);
        for (k, s) in snaps.iter().enumerate() {
            assert_eq!(s.get(k as u64).unwrap().row, row(k as i64));
            assert_eq!(last.get(k as u64).unwrap().row, row(k as i64));
        }
    }
}
