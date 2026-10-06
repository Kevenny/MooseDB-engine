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

#[derive(Default)]
pub(crate) struct MemTable {
    frozen: Vec<Arc<Vec<MemRow>>>,
    active: Vec<MemRow>,
    len: usize,
    bytes: usize,
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

impl MemTable {
    pub(crate) fn push(&mut self, series_id: u64, row: Row) {
        self.bytes += mem::size_of::<MemRow>() + row.iter().map(Value::mem_size).sum::<usize>();
        self.len += 1;
        self.active.push(MemRow { series_id, row });
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
        if !self.active.is_empty() {
            self.frozen.push(Arc::new(mem::take(&mut self.active)));
        }
        if self.frozen.len() > MAX_FROZEN {
            // Copy into one segment; snapshots holding the old ones are unaffected
            // and ordinals are preserved because order is preserved.
            let merged: Vec<MemRow> =
                self.rows().map(|m| MemRow { series_id: m.series_id, row: m.row.clone() }).collect();
            self.frozen = vec![Arc::new(merged)];
        }
        MemSnapshot { generation, segments: self.frozen.clone() }
    }

    /// Rows with `lo <= ts <= hi`, for optimizer estimates.
    pub(crate) fn count_in_range(&self, schema: &Schema, lo: i64, hi: i64) -> usize {
        self.rows().filter(|m| (lo..=hi).contains(&schema.row_ts(&m.row))).count()
    }

    /// Groups rows by `bucket(ts)` and then by series, each series sorted by
    /// timestamp (stable, so equal timestamps keep arrival order).
    pub(crate) fn group(
        &self,
        schema: &Schema,
        bucket: impl Fn(i64) -> (i64, i64),
    ) -> BTreeMap<(i64, i64), BTreeMap<u64, Vec<&Row>>> {
        let mut out: BTreeMap<(i64, i64), BTreeMap<u64, Vec<&Row>>> = BTreeMap::new();
        for r in self.rows() {
            out.entry(bucket(schema.row_ts(&r.row))).or_default().entry(r.series_id).or_default().push(&r.row);
        }
        for series in out.values_mut() {
            for rows in series.values_mut() {
                rows.sort_by_key(|r| schema.row_ts(r));
            }
        }
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn row(i: i64) -> Row {
        vec![Value::Timestamp(i)]
    }

    #[test]
    fn snapshots_are_stable_and_share_segments() {
        let mut m = MemTable::default();
        m.push(1, row(1));
        m.push(1, row(2));
        let s1 = m.snapshot(0);
        m.push(2, row(3));
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
            m.push(0, row(i));
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
