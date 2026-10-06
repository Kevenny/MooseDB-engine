//! Registry of open tables and the background maintenance pool.
//!
//! * The **registry** guarantees one `Table` instance per directory in the
//!   process (two instances would each own a WAL and corrupt each other), and
//!   lets diagnostics (INFORMATION_SCHEMA, UDFs) reach open tables by path.
//! * The **pool** — one scheduler plus `tideflow_compaction_threads` workers —
//!   runs retention sweeps every `tideflow_retention_check_interval` seconds
//!   and compaction when a bucket accumulates
//!   `tideflow_compaction_trigger_chunks` chunks or a chunk turns cold.

use std::collections::VecDeque;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::path::{Path, PathBuf};
use std::sync::atomic::Ordering;
use std::sync::{Arc, Condvar, Mutex, Weak};
use std::thread::JoinHandle;
use std::time::Duration;

use crate::error::{invalid, Result};
use crate::log::warn;
use crate::options::TableConfig;
use crate::table::Table;
use crate::time::{now_micros, MICROS_PER_SEC};

static REGISTRY: Mutex<Vec<(PathBuf, Weak<Table>)>> = Mutex::new(Vec::new());

/// Registry key: absolute, symlink-free path (falls back to the given path).
fn key(dir: &Path) -> PathBuf {
    std::fs::canonicalize(dir).unwrap_or_else(|_| dir.to_path_buf())
}

/// Opens a table, or returns the instance already open for that directory.
pub fn open_shared(config: TableConfig) -> Result<Arc<Table>> {
    let k = key(&config.dir);
    let mut reg = REGISTRY.lock().map_err(|_| invalid("table registry poisoned"))?;
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
    REGISTRY.lock().ok()?.iter().find(|(p, _)| *p == k).and_then(|(_, w)| w.upgrade())
}

fn live_tables() -> Vec<Arc<Table>> {
    REGISTRY.lock().map(|r| r.iter().filter_map(|(_, w)| w.upgrade()).collect()).unwrap_or_default()
}

/// Called before a table directory is dropped or renamed: stops maintenance
/// on any instance still alive (e.g. held by a running job), waits for the
/// running job to finish, and forgets the instance.
pub(crate) fn retire(dir: &Path) {
    let k = key(dir);
    let t = REGISTRY.lock().ok().and_then(|mut r| {
        let t = r.iter().find(|(p, _)| *p == k).and_then(|(_, w)| w.upgrade());
        r.retain(|(p, _)| *p != k);
        t
    });
    if let Some(t) = t {
        t.defunct.store(true, Ordering::Release);
        drop(t.maint.lock());
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
            let _m = self.maint.lock();
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
    s.stop.lock().map(|g| *g).unwrap_or(true)
}

fn scheduler(s: Arc<Shared>) {
    loop {
        if let Ok(g) = s.stop.lock() {
            if *g {
                return;
            }
            let _ = s.wake.wait_timeout(g, TICK);
        }
        if stopped(&s) {
            return;
        }
        let now = now_micros();
        for t in live_tables() {
            let due = !t.defunct.load(Ordering::Acquire) && (t.retention_due(now) || t.compaction_pending(now));
            if due && !t.queued.swap(true, Ordering::AcqRel) {
                if let Ok(mut q) = s.queue.lock() {
                    q.push_back(Arc::downgrade(&t));
                }
                s.work.notify_one();
            }
        }
    }
}

fn worker(s: Arc<Shared>) {
    loop {
        let job = {
            let Ok(mut q) = s.queue.lock() else { return };
            loop {
                if stopped(&s) {
                    return;
                }
                if let Some(j) = q.pop_front() {
                    break j;
                }
                q = match s.work.wait_timeout(q, TICK) {
                    Ok((q, _)) => q,
                    Err(_) => return,
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
    match std::thread::Builder::new().name("tideflow-sched".into()).spawn(move || scheduler(s)) {
        Ok(h) => handles.push(h),
        Err(e) => {
            warn(&format!("cannot start maintenance scheduler: {e}"));
            return;
        }
    }
    for i in 0..threads.max(1) {
        let s = shared.clone();
        match std::thread::Builder::new().name(format!("tideflow-maint-{i}")).spawn(move || worker(s)) {
            Ok(h) => handles.push(h),
            Err(e) => warn(&format!("cannot start maintenance worker: {e}")),
        }
    }
    *pool = Some(Pool { shared, threads: handles });
}

/// Stops the pool and waits for running jobs to finish.
pub fn stop() {
    let Some(pool) = POOL.lock().ok().and_then(|mut p| p.take()) else { return };
    if let Ok(mut g) = pool.shared.stop.lock() {
        *g = true;
    }
    pool.shared.wake.notify_all();
    pool.shared.work.notify_all();
    for h in pool.threads {
        let _ = h.join();
    }
}
