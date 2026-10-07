//! M3 (series index shared without copying) and L2 (cheap MemTable range
//! estimates). Own test binary: timings.

#![forbid(unsafe_code)]

use std::sync::Arc;
use std::time::Instant;

use moosedb_core::options::MAX_MEMTABLE_SIZE;
use moosedb_core::{Column, ColumnType, RawOptions, ScanFilter, Schema, Table, TableConfig, Value};

fn schema() -> Schema {
    Schema::new(
        vec![
            Column { name: "ts".into(), ty: ColumnType::Timestamp },
            Column { name: "host".into(), ty: ColumnType::Tag },
            Column { name: "v".into(), ty: ColumnType::Float64 },
        ],
        0,
    )
    .unwrap()
}

fn row(ts: i64, host: &str) -> Vec<Value> {
    vec![Value::Timestamp(ts), Value::Bytes(host.as_bytes().to_vec()), Value::Float64(ts as f64)]
}

fn open(dir: &std::path::Path, opts: RawOptions<'_>) -> Arc<Table> {
    let cfg = TableConfig::new(dir, schema(), &opts).unwrap();
    Table::create(&cfg).unwrap();
    Arc::new(Table::open(cfg).unwrap())
}

#[test]
fn series_snapshot_is_o1_versioned_and_never_changes_under_the_reader() {
    let dir = tempfile::tempdir().unwrap();
    let t = open(&dir.path().join("t"), RawOptions { memtable_size_bytes: MAX_MEMTABLE_SIZE, ..Default::default() });
    let n = 200_000usize;
    let mut b = t.begin_batch().unwrap();
    for i in 0..n {
        b.write(row(1_800_000_000_000_000 + i as i64, &format!("host-{i:06}"))).unwrap();
    }
    b.commit(false).unwrap();

    let v0 = t.series_version();
    let start = Instant::now();
    let snap = t.series_snapshot().unwrap();
    let snapshot_us = start.elapsed().as_nanos() as f64 / 1000.0;
    assert_eq!(snap.len(), n);
    assert_eq!(snap.version(), v0);
    let start = Instant::now();
    let copied = t.series().unwrap();
    let copy_ms = start.elapsed().as_millis();
    eprintln!("SERIES_200K snapshot_us={snapshot_us:.2} full_copy_ms={copy_ms}");
    assert!(snapshot_us < 5_000.0, "taking the snapshot copied the index: {snapshot_us} us");
    assert_eq!(copied.len(), n);
    assert!(copied.windows(2).all(|w| w[0].0 < w[1].0), "Table::series stays sorted by id");

    // New series change the version, never the snapshot already handed out.
    t.write(row(1_900_000_000_000_000, "brand-new")).unwrap();
    assert!(t.series_version() > v0);
    assert_eq!(snap.len(), n);
    assert_eq!(t.series_snapshot().unwrap().len(), n + 1);
    // A known series does not change it.
    let v1 = t.series_version();
    t.write(row(1_900_000_000_000_001, "brand-new")).unwrap();
    assert_eq!(t.series_version(), v1);

    // Entries are the tag values by series id, in registration order.
    let by_id: std::collections::HashMap<u64, Vec<Value>> = copied.into_iter().collect();
    for (id, tags) in snap.iter().take(1000) {
        assert_eq!(by_id[&id], tags.to_vec());
    }

    // Tag filters still resolve (they now walk the snapshot, not the index).
    let mut scan = t
        .scan(&ScanFilter { tags: vec![(1, Value::Bytes(b"host-000123".to_vec()))], ..Default::default() }, false)
        .unwrap();
    let mut hits = 0;
    while scan.next_row().unwrap().is_some() {
        hits += 1;
    }
    assert_eq!(hits, 1);

    // TRUNCATE: the version moves on, and the old snapshot still reads.
    let v2 = t.series_version();
    t.truncate().unwrap();
    assert!(t.series_version() > v2);
    assert!(t.series_snapshot().unwrap().is_empty());
    assert_eq!(snap.len(), n);
    assert_eq!(snap.get(5).unwrap().1.len(), 1);
}

#[test]
fn estimate_rows_does_not_walk_the_memtable() {
    let dir = tempfile::tempdir().unwrap();
    let t = open(&dir.path().join("t"), RawOptions { memtable_size_bytes: MAX_MEMTABLE_SIZE, ..Default::default() });
    let per_batch = 50_000i64;
    let batches = 12i64;
    let base = 1_800_000_000_000_000i64;
    for k in 0..batches {
        let mut b = t.begin_batch().unwrap();
        for i in 0..per_batch {
            b.write(row(base + (k * per_batch + i) * 1000, "h")).unwrap();
        }
        b.commit(false).unwrap();
    }
    assert_eq!(t.stats().unwrap().memtable_rows as i64, per_batch * batches, "all rows still in the MemTable");

    let iters = 2000;
    let start = Instant::now();
    let mut total = 0u64;
    for k in 0..iters {
        let lo = base + (k % 500) * 1_000_000;
        total += t.estimate_rows(lo, lo + 99_999_000).unwrap();
    }
    let per_call_us = start.elapsed().as_nanos() as f64 / 1000.0 / iters as f64;
    eprintln!("ESTIMATE_ROWS_600K per_call_us={per_call_us:.3} (sum {total})");
    assert!(per_call_us < 300.0, "{per_call_us} us per call");

    // Whole-segment ranges are exact; the whole table is exact.
    let all = t.estimate_rows(i64::MIN, i64::MAX).unwrap();
    assert_eq!(all as i64, per_batch * batches);
    let one = t.estimate_rows(base + 50_000 * 1000, base + 99_999 * 1000).unwrap();
    assert_eq!(one as i64, per_batch);
    assert_eq!(t.estimate_rows(0, 10).unwrap(), 0);
    // A partial range of a uniform segment is close.
    let part = t.estimate_rows(base, base + 24_999 * 1000).unwrap() as i64;
    assert!((24_000..=26_000).contains(&part), "{part}");
}
