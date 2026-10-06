//! Integration tests for the TideFlow storage core and its C ABI.
//!
//! Crash scenarios are simulated by dropping a `Table` without a clean
//! shutdown and then tampering with files exactly the way an interrupted
//! process would leave them.

#![forbid(unsafe_code)]

#[cfg(test)]
mod helpers {
    use std::path::Path;

    use tideflow_core::time::{days_from_civil, MICROS_PER_DAY, MICROS_PER_SEC};
    use tideflow_core::{Column, ColumnType, RawOptions, Row, Schema, Table, TableConfig, Value};

    /// 2026-10-01 00:00:00 UTC.
    pub fn base_ts() -> i64 {
        days_from_civil(2026, 10, 1) * MICROS_PER_DAY
    }

    pub fn schema() -> Schema {
        Schema::new(
            vec![
                Column { name: "ts".into(), ty: ColumnType::Timestamp },
                Column { name: "host".into(), ty: ColumnType::Tag },
                Column { name: "metric".into(), ty: ColumnType::Tag },
                Column { name: "value".into(), ty: ColumnType::Float64 },
                Column { name: "value_int".into(), ty: ColumnType::Int64 },
                Column { name: "note".into(), ty: ColumnType::Varchar },
            ],
            0,
        )
        .unwrap()
    }

    pub fn config(dir: &Path, opts: RawOptions<'_>) -> TableConfig {
        TableConfig::new(dir, schema(), &opts).unwrap()
    }

    pub fn row(sec: i64, host: &str, value: f64) -> Row {
        vec![
            Value::Timestamp(base_ts() + sec * MICROS_PER_SEC),
            Value::Bytes(host.as_bytes().to_vec()),
            Value::Bytes(b"cpu".to_vec()),
            Value::Float64(value),
            Value::Int(sec),
            if sec % 3 == 0 { Value::Null } else { Value::Bytes(format!("n{sec}").into_bytes()) },
        ]
    }

    pub fn create_open(dir: &Path, opts: RawOptions<'_>) -> Table {
        let cfg = config(dir, opts);
        Table::create(&cfg).unwrap();
        Table::open(cfg).unwrap()
    }

    pub fn all_rows(t: &Table) -> Vec<Row> {
        let mut s = t.scan(&Default::default(), false).unwrap();
        let mut out = Vec::new();
        while let Some((_, r)) = s.next_row().unwrap() {
            out.push(r);
        }
        out
    }

    pub fn sorted(mut rows: Vec<Row>) -> Vec<Row> {
        rows.sort_by(|a, b| format!("{a:?}").cmp(&format!("{b:?}")));
        rows
    }
}

#[cfg(test)]
mod table_tests {
    use super::helpers::*;
    use tideflow_core::time::{MICROS_PER_DAY, MICROS_PER_HOUR, MICROS_PER_SEC};
    use tideflow_core::{Error, RawOptions, ScanFilter, Table, Value};

    #[test]
    fn write_scan_memtable_and_chunks() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("t1");
        let t = create_open(&path, RawOptions::default());
        let mut expected = Vec::new();
        for i in 0..100 {
            let r = row(i, if i % 2 == 0 { "srv01" } else { "srv02" }, i as f64 * 0.5);
            t.write(r.clone()).unwrap();
            expected.push(r);
        }
        assert_eq!(sorted(all_rows(&t)), sorted(expected.clone()));
        t.flush().unwrap();
        assert_eq!(t.stats().unwrap().chunk_count, 1);
        assert_eq!(t.stats().unwrap().memtable_rows, 0);
        assert_eq!(sorted(all_rows(&t)), sorted(expected.clone()));

        for i in 100..150 {
            let r = row(i, "srv03", 1.0);
            t.write(r.clone()).unwrap();
            expected.push(r);
        }
        let s = t.stats().unwrap();
        assert_eq!((s.row_count, s.series_count), (150, 3));
        assert_eq!(sorted(all_rows(&t)), sorted(expected));
    }

    #[test]
    fn rows_split_into_time_buckets() {
        let dir = tempfile::tempdir().unwrap();
        let t = create_open(&dir.path().join("t"), RawOptions { chunk_interval: Some("1 HOUR"), ..Default::default() });
        for h in 0..5 {
            t.write(row(h * 3600 + 10, "a", 1.0)).unwrap();
        }
        t.flush().unwrap();
        let chunks = t.chunks().unwrap();
        assert_eq!(chunks.len(), 5);
        for c in &chunks {
            assert_eq!(c.bucket.1 - c.bucket.0, MICROS_PER_HOUR);
            assert!(c.bucket.0 <= c.header.ts_min && c.header.ts_max < c.bucket.1);
        }
    }

    #[test]
    fn auto_flush_on_memtable_threshold() {
        let dir = tempfile::tempdir().unwrap();
        let t = create_open(&dir.path().join("t"), RawOptions { memtable_size_bytes: 8 * 1024, ..Default::default() });
        for i in 0..2000 {
            t.write(row(i, "a", 1.0)).unwrap();
        }
        let s = t.stats().unwrap();
        assert!(s.chunk_count > 1, "expected automatic flushes, got {s:?}");
        assert_eq!(s.row_count, 2000);
        assert_eq!(all_rows(&t).len(), 2000);
    }

    #[test]
    fn range_scan_sorted_with_tags_and_prev() {
        let dir = tempfile::tempdir().unwrap();
        let t = create_open(&dir.path().join("t"), RawOptions::default());
        // Interleave writes and flushes so the range spans several chunks + MemTable,
        // and write out of order to check sorting.
        for i in (0..60).rev() {
            t.write(row(i, if i % 3 == 0 { "x" } else { "y" }, i as f64)).unwrap();
            if i % 20 == 0 {
                t.flush().unwrap();
            }
        }
        let lo = base_ts() + 10 * MICROS_PER_SEC;
        let hi = base_ts() + 40 * MICROS_PER_SEC;
        let mut f = ScanFilter::range(lo, hi);
        f.tags.push((1, Value::Bytes(b"x".to_vec())));
        let mut s = t.scan(&f, true).unwrap();
        let mut got = Vec::new();
        while let Some((_, r)) = s.next_row().unwrap() {
            got.push(r);
        }
        let secs: Vec<i64> = got.iter().map(|r| if let Value::Int(i) = r[4] { i } else { -1 }).collect();
        assert_eq!(secs, vec![12, 15, 18, 21, 24, 27, 30, 33, 36, 39]);

        // Backwards from the end.
        s.seek_end().unwrap();
        let (_, last) = s.prev_row().unwrap().unwrap();
        assert_eq!(last[4], Value::Int(39));
        let (_, before) = s.prev_row().unwrap().unwrap();
        assert_eq!(before[4], Value::Int(36));

        // Unknown series: empty result without touching chunks.
        let mut f = ScanFilter::range(i64::MIN, i64::MAX);
        f.tags.push((1, Value::Bytes(b"nope".to_vec())));
        f.tags.push((2, Value::Bytes(b"cpu".to_vec())));
        assert!(t.scan(&f, true).unwrap().next_row().unwrap().is_none());

        // Non-tag columns cannot be used as tag filters.
        let mut f = ScanFilter::default();
        f.tags.push((3, Value::Float64(1.0)));
        assert!(matches!(t.scan(&f, false), Err(Error::InvalidArg(_))));
    }

    #[test]
    fn positions_and_fetch() {
        let dir = tempfile::tempdir().unwrap();
        let t = create_open(&dir.path().join("t"), RawOptions::default());
        for i in 0..10 {
            t.write(row(i, "a", i as f64)).unwrap();
        }
        t.flush().unwrap();
        for i in 10..20 {
            t.write(row(i, "b", i as f64)).unwrap();
        }
        let mut s = t.scan(&Default::default(), false).unwrap();
        let mut seen = Vec::new();
        while let Some((p, r)) = s.next_row().unwrap() {
            seen.push((p, r));
        }
        assert_eq!(seen.len(), 20);
        for (p, r) in &seen {
            assert_eq!(&t.fetch(*p).unwrap(), r);
        }
        // MemTable positions go stale after a flush; chunk positions survive.
        t.flush().unwrap();
        let mem_pos = seen.iter().find(|(p, _)| p.source >> 63 == 1).unwrap().0;
        assert!(matches!(t.fetch(mem_pos), Err(Error::NotFound(_))));
        assert_eq!(t.fetch(seen[0].0).unwrap(), seen[0].1);
    }

    #[test]
    fn truncate_and_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("t");
        let t = create_open(&path, RawOptions::default());
        for i in 0..10 {
            t.write(row(i, "a", 1.0)).unwrap();
        }
        t.flush().unwrap();
        t.write(row(99, "a", 1.0)).unwrap();
        t.truncate().unwrap();
        assert_eq!(t.stats().unwrap().row_count, 0);
        t.write(row(5, "z", 2.0)).unwrap();
        t.sync_wal(true).unwrap();
        drop(t);
        let t = Table::open(config(&path, RawOptions::default())).unwrap();
        assert_eq!(all_rows(&t), vec![row(5, "z", 2.0)]);
        let chunk_files = std::fs::read_dir(&path)
            .unwrap()
            .filter(|e| e.as_ref().unwrap().file_name().to_string_lossy().ends_with(".tfl"))
            .count();
        assert_eq!(chunk_files, 0);
    }

    #[test]
    fn retention_drops_old_chunks() {
        let dir = tempfile::tempdir().unwrap();
        let t = create_open(
            &dir.path().join("t"),
            RawOptions { retention_period: Some("2 DAYS"), chunk_interval: Some("1 DAY"), ..Default::default() },
        );
        for d in 0..5 {
            t.write(row(d * 86_400 + 1, "a", 1.0)).unwrap();
        }
        t.flush().unwrap();
        assert_eq!(t.chunks().unwrap().len(), 5);
        // "now" = start of day 5 → cutoff = start of day 3: days 0,1,2 expire.
        let now = base_ts() + 5 * MICROS_PER_DAY;
        assert_eq!(t.apply_retention(now).unwrap(), 3);
        assert_eq!(t.stats().unwrap().row_count, 2);
        assert_eq!(t.apply_retention(now).unwrap(), 0);
    }

    #[test]
    fn check_detects_corruption() {
        let dir = tempfile::tempdir().unwrap();
        let t = create_open(&dir.path().join("t"), RawOptions::default());
        t.write(row(1, "a", 1.0)).unwrap();
        t.flush().unwrap();
        assert!(t.check().unwrap().is_empty());
        let path = t.chunks().unwrap()[0].path.clone();
        let mut data = std::fs::read(&path).unwrap();
        data[70] ^= 0xff;
        std::fs::write(&path, data).unwrap();
        assert_eq!(t.check().unwrap().len(), 1);
        assert!(matches!(t.scan(&Default::default(), false).unwrap().next_row(), Err(Error::Corrupt(_))));
    }

    #[test]
    fn estimate_rows_is_reasonable() {
        let dir = tempfile::tempdir().unwrap();
        let t = create_open(&dir.path().join("t"), RawOptions::default());
        for i in 0..1000 {
            t.write(row(i, "a", 1.0)).unwrap();
        }
        t.flush().unwrap();
        let est = t.estimate_rows(base_ts(), base_ts() + 99 * MICROS_PER_SEC).unwrap();
        assert!((80..=120).contains(&est), "estimate {est}");
        assert_eq!(t.estimate_rows(5, 4).unwrap(), 0);
    }

    #[test]
    fn create_drop_rename() {
        let dir = tempfile::tempdir().unwrap();
        let a = dir.path().join("a");
        let b = dir.path().join("b");
        let cfg = config(&a, RawOptions::default());
        Table::create(&cfg).unwrap();
        assert!(Table::create(&cfg).is_err(), "MANIFEST makes the directory non-empty");
        Table::rename(&a, &b).unwrap();
        assert!(Table::open(config(&a, RawOptions::default())).is_err());
        Table::open(config(&b, RawOptions::default())).unwrap();
        Table::drop_table(&b).unwrap();
        assert!(matches!(Table::drop_table(&b), Err(Error::NotFound(_))));
    }

    #[test]
    fn schema_mismatch_is_rejected() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("t");
        let t = create_open(&path, RawOptions::default());
        t.write(row(1, "a", 1.0)).unwrap();
        t.flush().unwrap();
        drop(t);
        let other = tideflow_core::Schema::new(
            vec![tideflow_core::Column { name: "ts".into(), ty: tideflow_core::ColumnType::Timestamp }],
            0,
        )
        .unwrap();
        let cfg = tideflow_core::TableConfig::new(&path, other, &RawOptions::default()).unwrap();
        assert!(matches!(Table::open(cfg), Err(Error::InvalidArg(_))));
    }
}

#[cfg(test)]
mod recovery_tests {
    use std::fs::{self, OpenOptions};
    use std::path::Path;

    use super::helpers::*;
    use tideflow_core::{RawOptions, Table};

    fn files_with_suffix(dir: &Path, suffix: &str) -> Vec<std::path::PathBuf> {
        let mut v: Vec<_> = fs::read_dir(dir)
            .unwrap()
            .map(|e| e.unwrap().path())
            .filter(|p| p.to_string_lossy().ends_with(suffix))
            .collect();
        v.sort();
        v
    }

    fn reopen(path: &Path) -> Table {
        Table::open(config(path, RawOptions::default())).unwrap()
    }

    #[test]
    fn synced_rows_survive_crash() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("t");
        let t = create_open(&path, RawOptions::default());
        let rows: Vec<_> = (0..50).map(|i| row(i, "a", i as f64)).collect();
        for r in &rows[..30] {
            t.write(r.clone()).unwrap();
        }
        t.flush().unwrap();
        for r in &rows[30..] {
            t.write(r.clone()).unwrap();
        }
        t.sync_wal(true).unwrap();
        drop(t); // "kill -9": no flush of the MemTable

        let t = reopen(&path);
        assert_eq!(sorted(all_rows(&t)), sorted(rows.clone()));
        // Recovery is idempotent.
        drop(t);
        let t = reopen(&path);
        assert_eq!(sorted(all_rows(&t)), sorted(rows));
    }

    #[test]
    fn torn_wal_tail_loses_only_the_torn_row() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("t");
        let t = create_open(&path, RawOptions::default());
        for i in 0..10 {
            t.write(row(i, "a", 1.0)).unwrap();
        }
        t.sync_wal(true).unwrap();
        drop(t);
        let wal = files_with_suffix(&path, ".tfl.wal").pop().unwrap();
        let len = fs::metadata(&wal).unwrap().len();
        OpenOptions::new().write(true).open(&wal).unwrap().set_len(len - 3).unwrap();

        let t = reopen(&path);
        let got = all_rows(&t);
        assert_eq!(sorted(got), sorted((0..9).map(|i| row(i, "a", 1.0)).collect()));
        // New writes after recovery land in a fresh segment and survive another crash.
        t.write(row(100, "b", 2.0)).unwrap();
        t.sync_wal(true).unwrap();
        drop(t);
        assert_eq!(all_rows(&reopen(&path)).len(), 10);
    }

    #[test]
    fn interrupted_flush_leaves_no_duplicates() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("t");
        let t = create_open(&path, RawOptions::default());
        for i in 0..20 {
            t.write(row(i, "a", 1.0)).unwrap();
        }
        t.flush().unwrap();
        // Snapshot the state *before* the flush commits: copy the directory,
        // flush in the original, then put the new chunk file next to the old
        // MANIFEST — exactly what a crash between chunk rename and MANIFEST
        // switch looks like.
        for i in 20..40 {
            t.write(row(i, "a", 1.0)).unwrap();
        }
        t.sync_wal(true).unwrap();
        let before = dir.path().join("before");
        fs::create_dir(&before).unwrap();
        for f in fs::read_dir(&path).unwrap() {
            let f = f.unwrap().path();
            fs::copy(&f, before.join(f.file_name().unwrap())).unwrap();
        }
        t.flush().unwrap();
        let new_chunk = files_with_suffix(&path, ".tfl").pop().unwrap();
        fs::copy(&new_chunk, before.join(new_chunk.file_name().unwrap())).unwrap();
        fs::write(before.join("garbage.tfl.tmp"), b"partial").unwrap();
        drop(t);

        let t = reopen(&before);
        assert_eq!(sorted(all_rows(&t)), sorted((0..40).map(|i| row(i, "a", 1.0)).collect()));
        assert_eq!(files_with_suffix(&before, ".tfl").len(), 1, "orphan chunk must be deleted");
        assert!(files_with_suffix(&before, ".tmp").is_empty());
    }

    #[test]
    fn corrupt_newest_chunk_is_quarantined() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("t");
        let t = create_open(&path, RawOptions::default());
        for i in 0..5 {
            t.write(row(i, "a", 1.0)).unwrap();
        }
        t.flush().unwrap();
        for i in 5..10 {
            t.write(row(i, "a", 1.0)).unwrap();
        }
        t.flush().unwrap();
        drop(t);
        let newest = files_with_suffix(&path, ".tfl").pop().unwrap();
        let mut data = fs::read(&newest).unwrap();
        data[80] ^= 0x55; // inside the data blocks: header still valid, file CRC not
        fs::write(&newest, data).unwrap();

        let t = reopen(&path);
        assert_eq!(sorted(all_rows(&t)), sorted((0..5).map(|i| row(i, "a", 1.0)).collect()));
        assert_eq!(files_with_suffix(&path, ".tfl.corrupt").len(), 1);
        drop(t);
        // The MANIFEST was rewritten, so the next open is clean.
        assert_eq!(all_rows(&reopen(&path)).len(), 5);
    }
}
