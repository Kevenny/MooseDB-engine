//! Registry of open tables and the background maintenance pool.
//!
//! * The **registry** guarantees one `Table` instance per directory in the
//!   process (two instances would each own a WAL and corrupt each other), and
//!   lets diagnostics (INFORMATION_SCHEMA, UDFs) reach open tables by path.
//! * The **pool** — one scheduler plus `moosedb_compaction_threads` workers —
//!   runs retention sweeps every `moosedb_retention_check_interval` seconds
//!   and compaction when a bucket accumulates
//!   `moosedb_compaction_trigger_chunks` chunks or a chunk turns cold.

use std::collections::VecDeque;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::path::{Path, PathBuf};
use std::sync::atomic::Ordering;
use std::sync::{Arc, Condvar, Mutex, MutexGuard, PoisonError, Weak};
use std::thread::JoinHandle;
use std::time::Duration;

use crate::error::{invalid, Result};
use crate::log::warn;
use crate::options::TableConfig;
use crate::table::Table;
use crate::time::{now_micros, MICROS_PER_SEC};

static REGISTRY: Mutex<Vec<(PathBuf, Weak<Table>)>> = Mutex::new(Vec::new());

/// Locks a mutex whose data stays consistent whatever a panicking holder was
/// doing (registry list, job queue, unit guards): a poisoned lock must not
/// switch maintenance off for the life of the process.
fn lock_recover<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    m.lock().unwrap_or_else(PoisonError::into_inner)
}

/// Registry key: absolute, symlink-free path (falls back to the given path).
fn key(dir: &Path) -> PathBuf {
    std::fs::canonicalize(dir).unwrap_or_else(|_| dir.to_path_buf())
}

/// Opens a table, or returns the instance already open for that directory.
pub fn open_shared(config: TableConfig) -> Result<Arc<Table>> {
    let k = key(&config.dir);
    let mut reg = lock_recover(&REGISTRY);
    reg.retain(|(_, w)| w.strong_count() > 0);
    if let Some(t) = reg.iter().find(|(p, _)| *p == k).and_then(|(_, w)| w.upgrade()) {
        if t.schema().fingerprint() != config.schema.fingerprint() {
            return Err(invalid(format!("{} is already open with a different schema", config.dir.display())));
        }
        return Ok(t);
    }
    let t = Arc::new(Table::open(config)?);
    reg.push((k, Arc::downgrade(&t)));
    Ok(t)
}

/// The open instance for `dir`, if any.
pub fn lookup(dir: &Path) -> Option<Arc<Table>> {
    let k = key(dir);
    lock_recover(&REGISTRY).iter().find(|(p, _)| *p == k).and_then(|(_, w)| w.upgrade())
}

fn live_tables() -> Vec<Arc<Table>> {
    lock_recover(&REGISTRY).iter().filter_map(|(_, w)| w.upgrade()).collect()
}

/// Called before a table directory is dropped or renamed: stops maintenance
/// on any instance still alive (e.g. held by a running job), waits for the
/// running job to finish, and forgets the instance.
pub(crate) fn retire(dir: &Path) {
    let k = key(dir);
    let t = {
        let mut r = lock_recover(&REGISTRY);
        let t = r.iter().find(|(p, _)| *p == k).and_then(|(_, w)| w.upgrade());
        r.retain(|(p, _)| *p != k);
        t
    };
    if let Some(t) = t {
        t.defunct.store(true, Ordering::Release);
        drop(lock_recover(&t.maint));
    }
}

impl Table {
    fn retention_due(&self, now: i64) -> bool {
        let interval = crate::settings::get().retention_check_interval_secs();
        interval > 0
            && self.config.opts.retention.cutoff(now).is_some()
            && now / MICROS_PER_SEC - self.last_retention.load(Ordering::Relaxed) >= interval as i64
    }

    /// One maintenance pass: retention sweep (if due) then compaction.
    pub fn run_maintenance(&self, now: i64) -> Result<()> {
        if self.defunct.load(Ordering::Acquire) {
            return Ok(());
        }
        if self.retention_due(now) {
            let _m = lock_recover(&self.maint);
            if !self.defunct.load(Ordering::Acquire) {
                self.apply_retention(now)?;
            }
        }
        self.compact_auto(now)?;
        Ok(())
    }
}

/// Runs one maintenance pass over every open table, synchronously.
pub fn run_once() {
    let now = now_micros();
    for t in live_tables() {
        if let Err(e) = t.run_maintenance(now) {
            warn(&format!("maintenance of {} failed: {e}", t.dir().display()));
        }
    }
}

struct Shared {
    queue: Mutex<VecDeque<Weak<Table>>>,
    work: Condvar,
    stop: Mutex<bool>,
    wake: Condvar,
}

struct Pool {
    shared: Arc<Shared>,
    threads: Vec<JoinHandle<()>>,
}

static POOL: Mutex<Option<Pool>> = Mutex::new(None);

const TICK: Duration = Duration::from_secs(1);

fn stopped(s: &Shared) -> bool {
    *lock_recover(&s.stop)
}

/// Whether a table has background work: a retention sweep or a compaction.
fn has_work(t: &Table, now: i64) -> bool {
    t.retention_due(now) || t.compaction_pending(now)
}

/// Queues `t` for a worker if `due` says it has work. The probe runs under
/// `catch_unwind` and per table: a panic (a bug in one table's code path)
/// must neither kill the scheduler thread, which would end all maintenance of
/// every table for the life of the process, nor skip the other tables.
fn schedule(s: &Shared, t: &Arc<Table>, now: i64, due: &dyn Fn(&Table, i64) -> bool) {
    match catch_unwind(AssertUnwindSafe(|| !t.defunct.load(Ordering::Acquire) && due(t, now))) {
        Ok(true) => {
            if !t.queued.swap(true, Ordering::AcqRel) {
                lock_recover(&s.queue).push_back(Arc::downgrade(t));
                s.work.notify_one();
            }
        }
        Ok(false) => {}
        Err(_) => warn(&format!("maintenance check of {} panicked; skipped this tick", t.dir().display())),
    }
}

/// One scheduler tick over `tables`.
fn tick(s: &Shared, tables: &[Arc<Table>], now: i64, due: &dyn Fn(&Table, i64) -> bool) {
    for t in tables {
        schedule(s, t, now, due);
    }
}

fn scheduler(s: Arc<Shared>) {
    loop {
        {
            let g = lock_recover(&s.stop);
            if *g {
                return;
            }
            let _ = s.wake.wait_timeout(g, TICK);
        }
        if stopped(&s) {
            return;
        }
        let now = now_micros();
        let ticked = catch_unwind(AssertUnwindSafe(|| tick(&s, &live_tables(), now, &has_work)));
        if ticked.is_err() {
            warn("maintenance scheduler tick panicked; continuing");
        }
    }
}

fn worker(s: Arc<Shared>) {
    loop {
        let job = {
            let mut q = lock_recover(&s.queue);
            loop {
                if stopped(&s) {
                    return;
                }
                if let Some(j) = q.pop_front() {
                    break j;
                }
                q = match s.work.wait_timeout(q, TICK) {
                    Ok((q, _)) => q,
                    Err(poisoned) => poisoned.into_inner().0,
                };
            }
        };
        let Some(t) = job.upgrade() else { continue };
        let result = catch_unwind(AssertUnwindSafe(|| t.run_maintenance(now_micros())));
        match result {
            Ok(Ok(())) => {}
            Ok(Err(e)) => warn(&format!("maintenance of {} failed: {e}", t.dir().display())),
            Err(_) => warn(&format!("maintenance of {} panicked", t.dir().display())),
        }
        t.queued.store(false, Ordering::Release);
    }
}

/// Starts the background pool with `threads` workers (idempotent).
pub fn start(threads: usize) {
    let Ok(mut pool) = POOL.lock() else { return };
    if pool.is_some() {
        return;
    }
    let shared = Arc::new(Shared {
        queue: Mutex::new(VecDeque::new()),
        work: Condvar::new(),
        stop: Mutex::new(false),
        wake: Condvar::new(),
    });
    let mut handles = Vec::new();
    let s = shared.clone();
    match std::thread::Builder::new().name("moosedb-sched".into()).spawn(move || scheduler(s)) {
        Ok(h) => handles.push(h),
        Err(e) => {
            warn(&format!("cannot start maintenance scheduler: {e}"));
            return;
        }
    }
    for i in 0..threads.max(1) {
        let s = shared.clone();
        match std::thread::Builder::new().name(format!("moosedb-maint-{i}")).spawn(move || worker(s)) {
            Ok(h) => handles.push(h),
            Err(e) => warn(&format!("cannot start maintenance worker: {e}")),
        }
    }
    *pool = Some(Pool { shared, threads: handles });
}

/// Stops the pool and waits for running jobs to finish.
pub fn stop() {
    let Some(pool) = POOL.lock().ok().and_then(|mut p| p.take()) else { return };
    *lock_recover(&pool.shared.stop) = true;
    pool.shared.wake.notify_all();
    pool.shared.work.notify_all();
    for h in pool.threads {
        let _ = h.join();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::options::RawOptions;
    use crate::schema::{Column, ColumnType, Schema};

    fn table(dir: &Path) -> Arc<Table> {
        let schema = Schema::new(vec![Column { name: "ts".into(), ty: ColumnType::Timestamp }], 0).unwrap();
        let cfg = TableConfig::new(dir, schema, &RawOptions::default()).unwrap();
        Table::create(&cfg).unwrap();
        Arc::new(Table::open(cfg).unwrap())
    }

    fn shared() -> Shared {
        Shared {
            queue: Mutex::new(VecDeque::new()),
            work: Condvar::new(),
            stop: Mutex::new(false),
            wake: Condvar::new(),
        }
    }

    #[test]
    fn a_panicking_probe_skips_one_table_not_the_tick() {
        let dir = tempfile::tempdir().unwrap();
        let (a, b) = (table(&dir.path().join("a")), table(&dir.path().join("b")));
        let s = shared();
        let bad = a.dir().to_path_buf();
        let due = move |t: &Table, _now: i64| -> bool {
            if t.dir() == bad {
                panic!("boom");
            }
            true
        };
        tick(&s, &[a.clone(), b.clone()], 0, &due);
        let queued = lock_recover(&s.queue).len();
        assert_eq!(queued, 1, "the healthy table is still scheduled");
        assert!(!a.queued.load(Ordering::Acquire));
        assert!(b.queued.load(Ordering::Acquire));
    }

    #[test]
    fn poisoned_scheduler_state_is_recovered() {
        let s = Arc::new(shared());
        let s2 = s.clone();
        let _ = std::thread::spawn(move || {
            let _g = s2.queue.lock().unwrap();
            let _h = s2.stop.lock().unwrap();
            panic!("poison both");
        })
        .join();
        assert!(s.queue.is_poisoned() && s.stop.is_poisoned());
        assert!(!stopped(&s));
        assert!(lock_recover(&s.queue).is_empty());
    }
}
