//! A2: `rnd_pos` (Snapshot::fetch) on a big series must not decode the whole
//! series on every call. Own test binary: timing and the process-wide block
//! cache setting must not be disturbed by other tests.

#![forbid(unsafe_code)]

use std::time::Instant;

use moosedb_core::{settings, Column, ColumnType, RawOptions, Schema, Table, TableConfig, Value};

const ROWS: i64 = 200_000;

fn schema() -> Schema {
    Schema::new(
        vec![
            Column { name: "ts".into(), ty: ColumnType::Timestamp },
            Column { name: "host".into(), ty: ColumnType::Tag },
            Column { name: "value".into(), ty: ColumnType::Float64 },
            Column { name: "n".into(), ty: ColumnType::Int64 },
            Column { name: "note".into(), ty: ColumnType::Varchar },
        ],
        0,
    )
    .unwrap()
}

fn row(i: i64) -> Vec<Value> {
    vec![
        Value::Timestamp(1_800_000_000_000_000 + i * 1000),
        Value::Bytes(b"h1".to_vec()),
        Value::Float64(i as f64 * 0.25),
        if i % 7 == 0 { Value::Null } else { Value::Int(i * 3) },
        if i % 5 == 0 { Value::Null } else { Value::Bytes(format!("note-{}", i % 1000).into_bytes()) },
    ]
}

#[test]
fn random_fetch_in_a_200k_row_series() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("t");
    let cfg = TableConfig::new(&path, schema(), &RawOptions::default()).unwrap();
    Table::create(&cfg).unwrap();
    let t = Table::open(cfg).unwrap();
    for i in 0..ROWS {
        t.write(row(i)).unwrap();
    }
    t.flush().unwrap();

    let mut scan = t.scan(&Default::default(), false).unwrap();
    let mut positions = Vec::with_capacity(ROWS as usize);
    let mut expect = std::collections::HashMap::new();
    while let Some((p, r)) = scan.next_row().unwrap() {
        if positions.len() % 997 == 0 {
            expect.insert(p, r);
        }
        positions.push(p);
    }
    assert_eq!(positions.len() as i64, ROWS);
    let snap = t.snapshot().unwrap();

    // Correctness: the fetched row equals the scanned row.
    for (p, r) in &expect {
        assert_eq!(&snap.fetch(*p).unwrap(), r);
    }

    let mut x = 0x9E37_79B9_7F4A_7C15u64;
    let mut next = move || {
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        x
    };
    let n = 300usize;
    let start = Instant::now();
    for _ in 0..n {
        let p = positions[(next() % positions.len() as u64) as usize];
        std::hint::black_box(snap.fetch(p).unwrap());
    }
    let per = start.elapsed().as_nanos() / n as u128;
    eprintln!("RND_FETCH_NS_PER_FETCH {per} (n={n}, rows={ROWS})");
    // Warm path: many more fetches over the same series. (The baseline run of
    // this test, before the series cache existed, skips it: 100k x 20 ms.)
    if std::env::var_os("FETCH_PERF_SHORT").is_none() {
        let n2 = 100_000usize;
        let start = Instant::now();
        for _ in 0..n2 {
            let p = positions[(next() % positions.len() as u64) as usize];
            std::hint::black_box(snap.fetch(p).unwrap());
        }
        let per2 = start.elapsed().as_nanos() / n2 as u128;
        eprintln!("RND_FETCH_NS_PER_FETCH_LONG {per2} (n={n2})");
        assert!(per2 < 200_000, "random fetch costs {per2} ns");
    }

    // The decoded series live in the block cache and obey its budget.
    let limit = settings::get().chunk_cache_bytes();
    assert!(moosedb_core::chunk_cache_used_bytes() as u64 <= limit);
    assert!(moosedb_core::chunk_cache_used_bytes() > 0, "the series was cached");
    // A cache too small for a decoded series keeps nothing of it, and fetches
    // stay correct (each one decodes again).
    settings::get().set_chunk_cache_bytes(1 << 20);
    for (p, r) in expect.iter().take(20) {
        assert_eq!(&snap.fetch(*p).unwrap(), r);
    }
    assert!(moosedb_core::chunk_cache_used_bytes() <= 1 << 20);
    // Disabled: nothing is kept at all.
    settings::get().set_chunk_cache_bytes(0);
    let (p, r) = expect.iter().next().unwrap();
    assert_eq!(&snap.fetch(*p).unwrap(), r);
    settings::get().set_chunk_cache_bytes(limit);
}
