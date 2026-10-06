//! In-memory buffer of rows not yet sealed into a chunk.
//!
//! Rows are kept in arrival order; grouping by bucket/series and sorting by
//! timestamp happen once, at flush time. Every row is also in the WAL, so the
//! MemTable can always be rebuilt by replay.

use std::collections::BTreeMap;

use crate::schema::{Row, Schema, Value};

pub(crate) struct MemRow {
    pub series_id: u64,
    pub row: Row,
}

#[derive(Default)]
pub(crate) struct MemTable {
    rows: Vec<MemRow>,
    bytes: usize,
}

impl MemTable {
    pub(crate) fn push(&mut self, series_id: u64, row: Row) {
        self.bytes += std::mem::size_of::<MemRow>() + row.iter().map(Value::mem_size).sum::<usize>();
        self.rows.push(MemRow { series_id, row });
    }

    pub(crate) fn len(&self) -> usize {
        self.rows.len()
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.rows.is_empty()
    }

    pub(crate) fn bytes(&self) -> usize {
        self.bytes
    }

    pub(crate) fn rows(&self) -> &[MemRow] {
        &self.rows
    }

    pub(crate) fn clear(&mut self) {
        self.rows = Vec::new();
        self.bytes = 0;
    }

    /// Groups rows by `bucket(ts)` and then by series, each series sorted by
    /// timestamp (stable, so equal timestamps keep arrival order).
    pub(crate) fn group(
        &self,
        schema: &Schema,
        bucket: impl Fn(i64) -> (i64, i64),
    ) -> BTreeMap<(i64, i64), BTreeMap<u64, Vec<&Row>>> {
        let mut out: BTreeMap<(i64, i64), BTreeMap<u64, Vec<&Row>>> = BTreeMap::new();
        for r in &self.rows {
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
