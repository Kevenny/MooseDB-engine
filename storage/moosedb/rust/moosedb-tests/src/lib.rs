//! Integration tests for the MooseDB storage core and its C ABI.
//!
//! Crash scenarios are simulated by dropping a `Table` without a clean
//! shutdown and then tampering with files exactly the way an interrupted
//! process would leave them.

#![forbid(unsafe_code)]

#[cfg(test)]
mod helpers {
    use std::path::Path;

    use moosedb_core::time::{days_from_civil, MICROS_PER_DAY, MICROS_PER_SEC};
    use moosedb_core::{Column, ColumnType, RawOptions, Row, Schema, Table, TableConfig, Value};

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
    use moosedb_core::time::{MICROS_PER_DAY, MICROS_PER_HOUR, MICROS_PER_SEC};
    use moosedb_core::{Error, RawOptions, ScanFilter, Table, Value};

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
        // After a flush the *current* contents no longer cover MemTable
        // positions, but the scan's snapshot still resolves every position.
        t.flush().unwrap();
        let mem_pos = seen.iter().find(|(p, _)| p.source >> 63 == 1).unwrap().0;
        assert!(matches!(t.fetch(mem_pos), Err(Error::NotFound(_))));
        assert_eq!(t.fetch(seen[0].0).unwrap(), seen[0].1);
        for (p, r) in &seen {
            assert_eq!(&s.snapshot().fetch(*p).unwrap(), r);
        }
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
        let path = t.chunks().unwrap()[0].path().to_path_buf();
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
        let other = moosedb_core::Schema::new(
            vec![moosedb_core::Column { name: "ts".into(), ty: moosedb_core::ColumnType::Timestamp }],
            0,
        )
        .unwrap();
        let cfg = moosedb_core::TableConfig::new(&path, other, &RawOptions::default()).unwrap();
        assert!(matches!(Table::open(cfg), Err(Error::InvalidArg(_))));
    }
}

#[cfg(test)]
mod recovery_tests {
    use std::fs::{self, OpenOptions};
    use std::path::Path;

    use super::helpers::*;
    use moosedb_core::{RawOptions, Table};

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
    fn partial_wal_header_survives_repeated_restarts() {
        for stub in [&b""[..], b"TFW"] {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("t");
            let t = create_open(&path, RawOptions::default());
            for i in 0..10 {
                t.write(row(i, "a", 1.0)).unwrap();
            }
            t.sync_wal(true).unwrap();
            drop(t);
            // Crash right after a new segment was created, before its header was complete.
            let last =
                files_with_suffix(&path, ".tfl.wal").pop().unwrap().file_name().unwrap().to_string_lossy().into_owned();
            let seq: u64 = last.trim_start_matches("wal_").trim_end_matches(".tfl.wal").parse().unwrap();
            fs::write(path.join(format!("wal_{:06}.tfl.wal", seq + 1)), stub).unwrap();

            for restart in 0..3 {
                let t = reopen(&path);
                assert_eq!(
                    all_rows(&t).len(),
                    10 + restart as usize,
                    "restart {restart} (stub of {} bytes)",
                    stub.len()
                );
                t.write(row(100 + restart, "b", 2.0)).unwrap();
                t.sync_wal(true).unwrap();
                drop(t);
            }
            assert_eq!(all_rows(&reopen(&path)).len(), 13);
        }
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

#[cfg(test)]
mod compression_tests {
    use super::helpers::*;
    use moosedb_core::compression::{CODEC_LZ4, CODEC_ZSTD};
    use moosedb_core::time::{days_from_civil, MICROS_PER_DAY, MICROS_PER_SEC};
    use moosedb_core::{RawOptions, Value};

    /// Realistic metrics: 10 s interval, 20 hosts, slowly moving gauges and
    /// monotonic counters.
    fn metric_row(i: i64, host: usize, base: i64) -> moosedb_core::Row {
        vec![
            Value::Timestamp(base + i * 10 * MICROS_PER_SEC),
            Value::Bytes(format!("srv{host:02}").into_bytes()),
            Value::Bytes(b"cpu".to_vec()),
            Value::Float64(40.0 + ((i / 30 + host as i64) % 25) as f64 * 0.5),
            Value::Int(1_000_000 * host as i64 + i * 17),
            Value::Null,
        ]
    }

    #[test]
    fn cold_numeric_data_is_under_15_percent() {
        let dir = tempfile::tempdir().unwrap();
        let t = create_open(&dir.path().join("t"), RawOptions { hot_threshold: Some("1 DAY"), ..Default::default() });
        let base = days_from_civil(2020, 1, 1) * MICROS_PER_DAY; // long cold
        for i in 0..8640 {
            for h in 0..20 {
                t.write(metric_row(i, h, base)).unwrap();
            }
        }
        t.flush().unwrap();
        let s = t.stats().unwrap();
        let ratio = s.compressed_bytes as f64 / s.data_bytes as f64;
        assert!(ratio < 0.15, "ratio {ratio:.3} ({} of {} bytes)", s.compressed_bytes, s.data_bytes);
        assert!(t.chunks().unwrap().iter().all(|c| c.header.codec_id() == CODEC_ZSTD));
        assert_eq!(all_rows(&t).len(), 8640 * 20);
    }

    #[test]
    fn hot_data_uses_lz4() {
        let dir = tempfile::tempdir().unwrap();
        let t = create_open(&dir.path().join("t"), RawOptions::default());
        let now = moosedb_core::time::now_micros();
        for i in 0..100 {
            t.write(metric_row(i, 0, now - MICROS_PER_DAY)).unwrap();
        }
        t.flush().unwrap();
        assert!(t.chunks().unwrap().iter().all(|c| c.header.codec_id() == CODEC_LZ4));
    }
}

#[cfg(test)]
mod compaction_tests {
    use super::helpers::*;
    use moosedb_core::compression::{CODEC_LZ4, CODEC_ZSTD};
    use moosedb_core::time::{MICROS_PER_DAY, MICROS_PER_SEC};
    use moosedb_core::{maintenance, settings, RawOptions, ScanFilter, Table, Value};

    #[test]
    fn merges_each_bucket_into_one_chunk() {
        let dir = tempfile::tempdir().unwrap();
        let t = create_open(&dir.path().join("t"), RawOptions::default());
        let mut expected = Vec::new();
        // 6 flushes, each touching two days, written out of order.
        for f in 0..6 {
            for i in (0..50).rev() {
                for day in 0..2 {
                    let r = row(day * 86_400 + f * 1000 + i, ["a", "b", "c"][(i % 3) as usize], i as f64);
                    t.write(r.clone()).unwrap();
                    expected.push(r);
                }
            }
            t.flush().unwrap();
        }
        assert_eq!(t.chunks().unwrap().len(), 12);
        let now = base_ts() + 365 * MICROS_PER_DAY;
        let rep = t.compact(i64::MIN, i64::MAX, now).unwrap();
        assert_eq!((rep.groups, rep.chunks_in, rep.chunks_out), (2, 12, 2));
        let chunks = t.chunks().unwrap();
        assert_eq!(chunks.len(), 2);
        assert!(chunks.iter().all(|c| c.header.codec_id() == CODEC_ZSTD), "old data re-encoded cold");
        for c in &chunks {
            assert_eq!(c.series.len(), 3, "each series is contiguous after the merge");
        }
        assert_eq!(sorted(all_rows(&t)), sorted(expected.clone()));
        // Sorted scans still come out in timestamp order.
        let mut s = t.scan(&ScanFilter::default(), true).unwrap();
        let mut last = i64::MIN;
        while let Some((_, r)) = s.next_row().unwrap() {
            let Value::Timestamp(ts) = r[0] else { panic!() };
            assert!(ts >= last);
            last = ts;
        }
        // Nothing left to do; and the result survives a reopen.
        assert_eq!(t.compact(i64::MIN, i64::MAX, now).unwrap().groups, 0);
        drop(t);
        let t = Table::open(config(&dir.path().join("t"), RawOptions::default())).unwrap();
        assert_eq!(sorted(all_rows(&t)), sorted(expected));
    }

    #[test]
    fn range_limits_the_buckets_compacted() {
        let dir = tempfile::tempdir().unwrap();
        let t = create_open(&dir.path().join("t"), RawOptions::default());
        for f in 0..3 {
            for day in 0..3 {
                t.write(row(day * 86_400 + f, "a", 1.0)).unwrap();
            }
            t.flush().unwrap();
        }
        let now = base_ts();
        let rep = t.compact(base_ts() + MICROS_PER_DAY, base_ts() + MICROS_PER_DAY + 5 * MICROS_PER_SEC, now).unwrap();
        assert_eq!(rep.groups, 1);
        assert_eq!(t.chunks().unwrap().len(), 9 - 3 + 1, "only day 1 merged");
    }

    #[test]
    fn readers_keep_old_files_alive() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("t");
        let t = create_open(&path, RawOptions::default());
        for f in 0..3 {
            for i in 0..10 {
                t.write(row(f * 100 + i, "a", 1.0)).unwrap();
            }
            t.flush().unwrap();
        }
        let old_files: Vec<_> = t.chunks().unwrap().iter().map(|c| c.path().to_path_buf()).collect();
        let mut scan = t.scan(&ScanFilter::default(), false).unwrap();
        t.compact(i64::MIN, i64::MAX, base_ts()).unwrap();
        assert!(old_files.iter().all(|p| p.exists()), "an open scan still needs them");
        let mut n = 0;
        while scan.next_row().unwrap().is_some() {
            n += 1;
        }
        assert_eq!(n, 30);
        drop(scan);
        assert!(old_files.iter().all(|p| !p.exists()), "deleted once the last reader is gone");
    }

    #[test]
    fn cold_reencoding_and_background_trigger() {
        let dir = tempfile::tempdir().unwrap();
        let cfg = config(&dir.path().join("t"), RawOptions { hot_threshold: Some("1 DAY"), ..Default::default() });
        Table::create(&cfg).unwrap();
        let t = maintenance::open_shared(cfg).unwrap();
        let now = moosedb_core::time::now_micros();
        for i in 0..10 {
            t.write(vec![
                Value::Timestamp(now - 3 * MICROS_PER_DAY + i),
                Value::Bytes(b"h".to_vec()),
                Value::Bytes(b"m".to_vec()),
                Value::Float64(1.0),
                Value::Int(i),
                Value::Null,
            ])
            .unwrap();
        }
        t.flush().unwrap();
        // Old data flushed late: written cold right away.
        assert_eq!(t.chunks().unwrap()[0].header.codec_id(), CODEC_ZSTD);

        // Background trigger: 4 chunks in one hot bucket with trigger=3.
        settings::get().set_compaction_trigger_chunks(3);
        let shared = {
            let cfg = config(&dir.path().join("v"), RawOptions::default());
            Table::create(&cfg).unwrap();
            maintenance::open_shared(cfg).unwrap()
        };
        let hot = moosedb_core::time::now_micros();
        for f in 0..4 {
            shared
                .write(vec![
                    Value::Timestamp(hot + f),
                    Value::Bytes(b"h".to_vec()),
                    Value::Bytes(b"m".to_vec()),
                    Value::Null,
                    Value::Null,
                    Value::Null,
                ])
                .unwrap();
            shared.flush().unwrap();
        }
        assert_eq!(shared.chunks().unwrap().len(), 4);
        maintenance::run_once();
        let chunks = shared.chunks().unwrap();
        assert_eq!(chunks.len(), 1);
        assert_eq!(chunks[0].header.codec_id(), CODEC_LZ4, "hot bucket keeps the hot codec");
        settings::get().set_compaction_trigger_chunks(10);
        assert_eq!(all_rows(&shared).len(), 4);
    }
}

#[cfg(test)]
mod merge_tests {
    use super::helpers::*;
    use moosedb_core::{Position, RawOptions, ScanFilter, Value};

    /// Deterministic pseudo-random sequence (no extra dependencies).
    fn lcg(seed: &mut u64) -> u64 {
        *seed = seed.wrapping_mul(6_364_136_223_846_793_005).wrapping_add(1_442_695_040_888_963_407);
        *seed >> 33
    }

    fn ts(r: &[Value]) -> i64 {
        match r[0] {
            Value::Timestamp(t) => t,
            _ => panic!("no timestamp"),
        }
    }

    #[test]
    fn merge_matches_a_naive_sort_both_directions() {
        let dir = tempfile::tempdir().unwrap();
        let t = create_open(&dir.path().join("t"), RawOptions { chunk_interval: Some("1 HOUR"), ..Default::default() });
        let mut seed = 7;
        for batch in 0..5 {
            for _ in 0..400 {
                let sec = (lcg(&mut seed) % 20_000) as i64; // duplicates included
                let host = ["a", "b", "c", "d"][(lcg(&mut seed) % 4) as usize];
                t.write(row(sec, host, batch as f64)).unwrap();
            }
            if batch < 4 {
                t.flush().unwrap();
            }
        }
        let collect = |f: &ScanFilter, desc: bool| {
            let mut s = t.scan(f, true).unwrap();
            let mut out = Vec::new();
            if desc {
                s.seek_end().unwrap();
                while let Some(r) = s.prev_row().unwrap() {
                    out.push(r);
                }
            } else {
                while let Some(r) = s.next_row().unwrap() {
                    out.push(r);
                }
            }
            out
        };
        let lo = base_ts() + 3_000 * 1_000_000;
        let hi = base_ts() + 15_000 * 1_000_000;
        let mut f = ScanFilter::range(lo, hi);
        f.tags.push((1, Value::Bytes(b"b".to_vec())));
        let asc = collect(&f, false);
        let desc = collect(&f, true);
        let naive: Vec<_> = all_rows(&t)
            .into_iter()
            .filter(|r| (lo..=hi).contains(&ts(r)) && r[1] == Value::Bytes(b"b".to_vec()))
            .collect();
        assert!(asc.windows(2).all(|w| ts(&w[0].1) <= ts(&w[1].1)));
        assert_eq!(asc.len(), naive.len());
        let a: Vec<_> = asc.iter().map(|(_, r)| r.clone()).collect();
        assert_eq!(sorted(a), sorted(naive));
        let rev: Vec<Position> = desc.iter().map(|(p, _)| *p).collect();
        let fwd: Vec<Position> = asc.iter().rev().map(|(p, _)| *p).collect();
        assert_eq!(rev, fwd, "descending is exactly the reverse of ascending");
    }

    #[test]
    fn direction_change_mid_scan() {
        let dir = tempfile::tempdir().unwrap();
        let t = create_open(&dir.path().join("t"), RawOptions::default());
        for i in 0..20 {
            t.write(row(i, "a", i as f64)).unwrap();
            if i == 9 {
                t.flush().unwrap();
            }
        }
        let sec = |r: Option<(Position, moosedb_core::Row)>| r.map(|(_, r)| (ts(&r) - base_ts()) / 1_000_000);
        let mut s = t.scan(&ScanFilter::default(), true).unwrap();
        for i in 0..6 {
            assert_eq!(sec(s.next_row().unwrap()), Some(i));
        }
        assert_eq!(sec(s.prev_row().unwrap()), Some(4));
        assert_eq!(sec(s.prev_row().unwrap()), Some(3));
        assert_eq!(sec(s.next_row().unwrap()), Some(4));
        s.seek_end().unwrap();
        assert_eq!(sec(s.prev_row().unwrap()), Some(19));
        assert_eq!(sec(s.prev_row().unwrap()), Some(18));
        assert_eq!(sec(s.next_row().unwrap()), Some(19));
        assert_eq!(sec(s.next_row().unwrap()), None);
    }

    #[test]
    fn series_filter_and_empty_results() {
        let dir = tempfile::tempdir().unwrap();
        let t = create_open(&dir.path().join("t"), RawOptions::default());
        for i in 0..30 {
            t.write(row(i, ["a", "b", "c"][(i % 3) as usize], 1.0)).unwrap();
        }
        t.flush().unwrap();
        let series = t.series().unwrap();
        assert_eq!(series.len(), 3);
        let b = series.iter().find(|(_, tags)| tags[0] == Value::Bytes(b"b".to_vec())).unwrap().0;
        let f = ScanFilter { series: Some(vec![b]), ..ScanFilter::default() };
        let first = t.scan(&f, false).unwrap().next_row().unwrap().map(|(_, r)| r[1].clone());
        assert_eq!(first, Some(Value::Bytes(b"b".to_vec())));
        let mut n = 0;
        let mut s = t.scan(&f, true).unwrap();
        while s.next_row().unwrap().is_some() {
            n += 1;
        }
        assert_eq!(n, 10);
        let none = ScanFilter { series: Some(vec![]), ..ScanFilter::default() };
        assert!(t.scan(&none, true).unwrap().next_row().unwrap().is_none());
        assert!(t.scan(&none, false).unwrap().next_row().unwrap().is_none());
    }
}

#[cfg(test)]
mod encryption_tests {
    use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
    use std::sync::{Arc, Once};

    use super::helpers::*;
    use moosedb_core::crypto::{set_key_provider, KEY_LEN};
    use moosedb_core::{Error, RawOptions, Table};

    static LATEST: AtomicU32 = AtomicU32::new(1);
    /// While set, key id 55 resolves to different key bytes ("wrong key").
    static WRONG_KEY_55: AtomicBool = AtomicBool::new(false);

    fn install() {
        static ONCE: Once = Once::new();
        ONCE.call_once(|| {
            set_key_provider(Some(Arc::new(|id: u32, version: Option<u32>| {
                if id == 404 {
                    return Err(Error::NotFound("no such key".into()));
                }
                let v = version.unwrap_or_else(|| LATEST.load(Ordering::SeqCst));
                if id == 55 && WRONG_KEY_55.load(Ordering::SeqCst) {
                    return Ok((v, [0xEE; KEY_LEN]));
                }
                Ok((v, [(id as u8).wrapping_mul(31) ^ v as u8; KEY_LEN]))
            })));
        });
    }

    fn enc(key: u32) -> RawOptions<'static> {
        RawOptions { encryption_key_id: Some(key), ..Default::default() }
    }

    fn contains(dir: &std::path::Path, needle: &[u8]) -> bool {
        std::fs::read_dir(dir).unwrap().any(|e| {
            let data = std::fs::read(e.unwrap().path()).unwrap();
            data.windows(needle.len()).any(|w| w == needle)
        })
    }

    #[test]
    fn files_are_unreadable_without_the_key_and_survive_rotation() {
        install();
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("t");
        let t = create_open(&path, enc(7));
        let secret = |i| row(i, "TOPSECRETHOST", 1.0);
        for i in 0..50 {
            t.write(secret(i)).unwrap();
        }
        t.sync_wal(true).unwrap();
        assert!(!contains(&path, b"TOPSECRETHOST"), "WAL leaks plaintext");
        t.flush().unwrap();
        LATEST.store(2, Ordering::SeqCst); // key rotation
        for i in 50..60 {
            t.write(secret(i)).unwrap();
        }
        t.flush().unwrap();
        assert!(!contains(&path, b"TOPSECRETHOST"), "chunks leak plaintext");
        assert!(!contains(&path, b"n17"), "non-tag values leak");
        assert!(t.chunks().unwrap().iter().all(|c| c.header.is_encrypted()));
        assert!(t.check().unwrap().is_empty(), "CHECK works on ciphertext");
        // Both key versions decrypt after a reopen, and compaction re-encrypts.
        t.write(secret(99)).unwrap();
        t.sync_wal(true).unwrap();
        drop(t);
        let t = Table::open(config(&path, enc(7))).unwrap();
        assert_eq!(all_rows(&t).len(), 61);
        assert_eq!(t.compact(i64::MIN, i64::MAX, base_ts()).unwrap().groups, 1);
        assert_eq!(all_rows(&t).len(), 61);
        assert!(!contains(&path, b"TOPSECRETHOST"));
    }

    fn manifest_and_corrupt_files(dir: &std::path::Path) -> (Vec<u8>, usize) {
        let corrupt = std::fs::read_dir(dir)
            .unwrap()
            .filter(|e| e.as_ref().unwrap().file_name().to_string_lossy().ends_with(".corrupt"))
            .count();
        (std::fs::read(dir.join("MANIFEST")).unwrap(), corrupt)
    }

    #[test]
    fn wrong_key_fails_closed_without_quarantine_or_manifest_rewrite() {
        install();
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("t");
        let t = create_open(&path, enc(55));
        for i in 0..40 {
            t.write(row(i, "a", i as f64)).unwrap();
        }
        t.flush().unwrap();
        for i in 40..50 {
            t.write(row(i, "b", i as f64)).unwrap();
        }
        t.flush().unwrap();
        for i in 50..55 {
            t.write(row(i, "c", i as f64)).unwrap();
        }
        t.sync_wal(true).unwrap();
        drop(t);
        let before = manifest_and_corrupt_files(&path);

        WRONG_KEY_55.store(true, Ordering::SeqCst);
        let first = Table::open(config(&path, enc(55)));
        let second = Table::open(config(&path, enc(55)));
        WRONG_KEY_55.store(false, Ordering::SeqCst);
        assert!(first.is_err() && second.is_err(), "a wrong key must fail the open");
        assert_eq!(manifest_and_corrupt_files(&path), before, "nothing quarantined, MANIFEST untouched");

        let t = Table::open(config(&path, enc(55))).unwrap();
        assert_eq!(all_rows(&t).len(), 55, "every row is back with the right key");
    }

    #[test]
    fn encryption_setting_must_match_the_chunks() {
        install();
        let dir = tempfile::tempdir().unwrap();
        let plain = dir.path().join("plain");
        let t = create_open(&plain, RawOptions::default());
        t.write(row(1, "a", 1.0)).unwrap();
        t.flush().unwrap();
        drop(t);
        assert!(matches!(Table::open(config(&plain, enc(7))), Err(Error::InvalidArg(_))));
        assert_eq!(all_rows(&Table::open(config(&plain, RawOptions::default())).unwrap()).len(), 1);

        let encrypted = dir.path().join("enc");
        let t = create_open(&encrypted, enc(7));
        t.write(row(1, "a", 1.0)).unwrap();
        t.flush().unwrap();
        drop(t);
        assert!(matches!(Table::open(config(&encrypted, RawOptions::default())), Err(Error::InvalidArg(_))));
        assert_eq!(all_rows(&Table::open(config(&encrypted, enc(7))).unwrap()).len(), 1);
    }

    #[test]
    fn missing_key_is_rejected_at_create() {
        install();
        let dir = tempfile::tempdir().unwrap();
        let cfg = config(&dir.path().join("t"), enc(404));
        assert!(matches!(Table::create(&cfg), Err(Error::InvalidArg(_))));
    }
}

#[cfg(test)]
mod registry_tests {
    use std::sync::Arc;

    use super::helpers::*;
    use moosedb_core::inspect::{inspect, ChunkStatus};
    use moosedb_core::time::MICROS_PER_DAY;
    use moosedb_core::{maintenance, RawOptions, Table};

    #[test]
    fn one_instance_per_directory() {
        let dir = tempfile::tempdir().unwrap();
        let cfg = config(&dir.path().join("t"), RawOptions::default());
        Table::create(&cfg).unwrap();
        let a = maintenance::open_shared(cfg.clone()).unwrap();
        let b = maintenance::open_shared(cfg.clone()).unwrap();
        assert!(Arc::ptr_eq(&a, &b));
        assert!(maintenance::lookup(&dir.path().join("t")).is_some());
        drop((a, b));
        assert!(maintenance::lookup(&dir.path().join("t")).is_none());
    }

    #[test]
    fn inspect_open_and_closed_tables() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("t");
        let raw = RawOptions { retention_period: Some("400 DAYS"), hot_threshold: Some("1 DAY"), ..Default::default() };
        let cfg = config(&path, raw);
        Table::create(&cfg).unwrap();
        let t = maintenance::open_shared(cfg.clone()).unwrap();
        for i in 0..10 {
            t.write(row(i, "a", 1.0)).unwrap();
        }
        t.flush().unwrap();
        for i in 10..15 {
            t.write(row(i, "b", 1.0)).unwrap();
        }
        t.sync_wal(true).unwrap();
        let now = base_ts() + 2 * MICROS_PER_DAY;
        let open = inspect(&path, now).unwrap();
        assert!(open.is_open);
        assert_eq!((open.row_count, open.pending_rows, open.series_count), (15, 5, 2));
        assert_eq!(open.chunks[0].status, ChunkStatus::Cold);
        assert_eq!(open.options.as_ref().unwrap().retention_text, "400 DAYS");
        drop(t);

        let closed = inspect(&path, now).unwrap();
        assert!(!closed.is_open);
        assert_eq!((closed.row_count, closed.pending_rows), (15, 5));
        let far = base_ts() + 500 * MICROS_PER_DAY;
        assert_eq!(inspect(&path, far).unwrap().count(ChunkStatus::Expired), 1);
        let early = base_ts();
        assert_eq!(inspect(&path, early).unwrap().count(ChunkStatus::Hot), 1);
    }
}

#[cfg(test)]
mod concurrency_tests {
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Arc;
    use std::thread;

    use super::helpers::*;
    use moosedb_core::{maintenance, RawOptions, ScanFilter, Table};

    #[test]
    fn readers_writers_and_compaction_in_parallel() {
        let dir = tempfile::tempdir().unwrap();
        let cfg = config(&dir.path().join("t"), RawOptions { memtable_size_bytes: 16 * 1024, ..Default::default() });
        Table::create(&cfg).unwrap();
        let t = maintenance::open_shared(cfg).unwrap();
        let done = Arc::new(AtomicBool::new(false));

        let writer = {
            let t = t.clone();
            thread::spawn(move || {
                for i in 0..5000 {
                    t.write(row(i, ["a", "b"][(i % 2) as usize], i as f64)).unwrap();
                }
                t.sync_wal(true).unwrap();
            })
        };
        let compactor = {
            let (t, done) = (t.clone(), done.clone());
            thread::spawn(move || {
                while !done.load(Ordering::Acquire) {
                    t.compact(i64::MIN, i64::MAX, base_ts()).unwrap();
                }
            })
        };
        let readers: Vec<_> = (0..3)
            .map(|k| {
                let (t, done) = (t.clone(), done.clone());
                thread::spawn(move || {
                    let mut last = 0usize;
                    while !done.load(Ordering::Acquire) {
                        let mut s = t.scan(&ScanFilter::default(), k == 0).unwrap();
                        let mut rows = Vec::new();
                        while let Some(r) = s.next_row().unwrap() {
                            rows.push(r);
                        }
                        assert!(rows.len() >= last, "a later snapshot never sees fewer rows");
                        last = rows.len();
                        // Every position resolves through the scan's snapshot,
                        // whatever flushes/compactions happened meanwhile.
                        for (p, r) in rows.iter().step_by(97) {
                            assert_eq!(&s.snapshot().fetch(*p).unwrap(), r);
                        }
                    }
                })
            })
            .collect();
        writer.join().unwrap();
        done.store(true, Ordering::Release);
        compactor.join().unwrap();
        for r in readers {
            r.join().unwrap();
        }
        assert_eq!(all_rows(&t).len(), 5000);
        t.compact(i64::MIN, i64::MAX, base_ts()).unwrap();
        assert_eq!(t.chunks().unwrap().len(), 1, "one bucket, fully merged");
        assert_eq!(all_rows(&t).len(), 5000);
    }
}

#[cfg(test)]
mod batch_tests;
