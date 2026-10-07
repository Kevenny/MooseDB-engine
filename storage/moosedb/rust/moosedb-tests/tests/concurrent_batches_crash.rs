//! Concurrent batches that all logged their rows before any of them
//! committed, then a crash after the COMMITs: the table must open with every
//! row. (Replay keeps `(segment, offset)` per waiting row, not payloads, so
//! the volume waiting for COMMITs has no byte limit.)

#![forbid(unsafe_code)]

use std::sync::{Arc, Barrier};

use moosedb_core::options::MAX_MEMTABLE_SIZE;
use moosedb_core::{Column, ColumnType, RawOptions, Schema, Table, TableConfig, Value};

const BATCHES: usize = 8;
const ROWS: usize = 3_000;
const PAYLOAD: usize = 4096;

fn config(dir: &std::path::Path) -> TableConfig {
    let schema = Schema::new(
        vec![
            Column { name: "ts".into(), ty: ColumnType::Timestamp },
            Column { name: "host".into(), ty: ColumnType::Tag },
            Column { name: "note".into(), ty: ColumnType::Varchar },
        ],
        0,
    )
    .unwrap();
    TableConfig::new(dir, schema, &RawOptions { memtable_size_bytes: MAX_MEMTABLE_SIZE, ..Default::default() }).unwrap()
}

#[test]
fn all_rows_survive_a_crash_after_concurrent_batches_committed() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("t");
    let cfg = config(&path);
    Table::create(&cfg).unwrap();
    let t = Arc::new(Table::open(cfg).unwrap());
    let logged = Arc::new(Barrier::new(BATCHES));
    let turn = Arc::new(std::sync::Mutex::new(0usize));
    let handles: Vec<_> = (0..BATCHES)
        .map(|b| {
            let (t, logged, turn) = (t.clone(), logged.clone(), turn.clone());
            std::thread::spawn(move || {
                let mut batch = t.begin_batch().unwrap();
                for i in 0..ROWS {
                    batch
                        .write(vec![
                            Value::Timestamp(1_800_000_000_000_000 + (b * ROWS + i) as i64),
                            Value::Bytes(format!("h{b}").into_bytes()),
                            Value::Bytes(vec![b'x'; PAYLOAD]),
                        ])
                        .unwrap();
                }
                // Every batch has logged all of its rows before the first COMMIT.
                logged.wait();
                loop {
                    let mut g = turn.lock().unwrap();
                    if *g == b {
                        batch.commit(true).unwrap();
                        *g += 1;
                        break;
                    }
                }
            })
        })
        .collect();
    for h in handles {
        h.join().unwrap();
    }
    std::mem::forget(t); // a crash: nothing is flushed or cleaned up

    let t = Table::open(config(&path)).unwrap();
    assert_eq!(t.stats().unwrap().row_count as usize, BATCHES * ROWS);
    let mut scan = t.scan(&Default::default(), false).unwrap();
    let mut n = 0;
    while scan.next_row().unwrap().is_some() {
        n += 1;
    }
    assert_eq!(n, BATCHES * ROWS);
}
