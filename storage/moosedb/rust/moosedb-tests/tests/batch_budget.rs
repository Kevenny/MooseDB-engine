//! M2: the buffers of all open batches of the process stay within one global
//! budget. Own test binary: the budget is a process-wide setting.

#![forbid(unsafe_code)]

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use moosedb_core::batch::batch_memory_in_use;
use moosedb_core::{settings, Column, ColumnType, RawOptions, Schema, Table, TableConfig, Value};

const THREADS: usize = 8;
const ROWS: usize = 40_000;
const PAYLOAD: usize = 400;

fn schema() -> Schema {
    Schema::new(
        vec![
            Column { name: "ts".into(), ty: ColumnType::Timestamp },
            Column { name: "host".into(), ty: ColumnType::Tag },
            Column { name: "note".into(), ty: ColumnType::Varchar },
        ],
        0,
    )
    .unwrap()
}

/// Every thread writes one big statement (one batch) of `ROWS` rows spread
/// over many series; returns the highest process-wide batch memory seen.
fn run(dir: &std::path::Path) -> u64 {
    let cfg = TableConfig::new(dir, schema(), &RawOptions::default()).unwrap();
    Table::create(&cfg).unwrap();
    let t = Arc::new(Table::open(cfg).unwrap());
    let peak = Arc::new(AtomicU64::new(0));
    let handles: Vec<_> = (0..THREADS)
        .map(|th| {
            let (t, peak) = (t.clone(), peak.clone());
            std::thread::spawn(move || {
                let mut b = t.begin_batch().unwrap();
                for i in 0..ROWS {
                    let row = vec![
                        Value::Timestamp(1_800_000_000_000_000 + (th * ROWS + i) as i64 * 1000),
                        Value::Bytes(format!("host-{th}-{}", i % 2000).into_bytes()),
                        Value::Bytes(vec![b'x'; PAYLOAD]),
                    ];
                    b.write(row).unwrap();
                    peak.fetch_max(batch_memory_in_use(), Ordering::Relaxed);
                }
                b.commit(false).unwrap();
            })
        })
        .collect();
    for h in handles {
        h.join().unwrap();
    }
    assert_eq!(batch_memory_in_use(), 0, "every batch gave its share back");
    let stats = t.stats().unwrap();
    assert_eq!(stats.row_count as usize, THREADS * ROWS, "no row lost to the early spills");
    assert!(t.check().unwrap().is_empty());
    let mut scan = t.scan(&Default::default(), false).unwrap();
    let mut n = 0usize;
    while scan.next_row().unwrap().is_some() {
        n += 1;
    }
    assert_eq!(n, THREADS * ROWS);
    peak.load(Ordering::Relaxed)
}

#[test]
fn many_concurrent_batches_stay_within_the_global_budget() {
    let g = settings::get();

    // Control: a budget nobody reaches. Each batch buffers its whole statement.
    g.set_batch_memory_budget_bytes(1 << 40);
    let dir = tempfile::tempdir().unwrap();
    let unbounded = run(&dir.path().join("control"));

    let budget: u64 = 8 << 20;
    g.set_batch_memory_budget_bytes(budget);
    let bounded = run(&dir.path().join("bounded"));

    // Worst case: the budget, plus what each batch may still hold below the
    // minimum a batch needs to be worth spilling (1 MiB), plus a row.
    let allowed = budget + THREADS as u64 * ((1 << 20) + 4096);
    eprintln!("BATCH_PEAK_BYTES unbounded={unbounded} budget={budget} bounded={bounded} allowed={allowed}");
    assert!(unbounded > 100 << 20, "control should buffer everything: {unbounded}");
    assert!(bounded <= allowed, "peak {bounded} exceeds {allowed}");
    assert!(bounded < unbounded / 4);
}
