//! C ABI of the TideFlow storage core.
//!
//! This crate only marshals arguments between C and `tideflow-core`; it holds
//! no storage logic. Every entry point:
//! * validates pointers before dereferencing them,
//! * runs the body under `catch_unwind`, so a Rust panic becomes
//!   `TF_ERR_INTERNAL` instead of unwinding into C++ (undefined behaviour),
//! * records a human-readable message retrievable with `tideflow_last_error`.
//!
//! Threading: a `TideFlowTable` may be used from many threads concurrently.
//! A `TideFlowScan` must be used by one thread at a time.
//!
//! Lifetimes: values returned through `TFRow` (including string pointers) are
//! owned by the scan and stay valid until the next call on that scan.

#![deny(unsafe_op_in_unsafe_fn)]

use std::cell::RefCell;
use std::ffi::{c_char, CStr, CString};
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::ptr;
use std::sync::Arc;

use tideflow_core::{
    Column, ColumnType, Error, Position, RawOptions, Row, Scan, ScanFilter, Schema, Table, TableConfig, Value,
    POSITION_LEN,
};

/// Size in bytes of a row position (`handler::ref_length`).
pub const TF_POSITION_LEN: usize = 16;
// Spelled as a literal so cbindgen can emit it; kept in sync at compile time.
const _: () = assert!(TF_POSITION_LEN == POSITION_LEN);

#[allow(non_camel_case_types)]
#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TFStatus {
    TF_OK = 0,
    TF_ERR_IO = 1,
    TF_ERR_CORRUPT = 2,
    TF_ERR_FULL = 3,
    TF_ERR_NOT_FOUND = 4,
    TF_ERR_INVALID_ARG = 5,
    TF_ERR_OOM = 6,
    TF_ERR_READONLY = 7,
    /// A Rust panic was caught at the boundary.
    TF_ERR_INTERNAL = 8,
    /// The operation is declared but not implemented yet.
    TF_ERR_UNSUPPORTED = 9,
}

/// Column type codes used in `TFTableConfig::column_types` and `TFValue::kind`.
#[allow(non_camel_case_types)]
#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TFColumnType {
    TF_COL_TIMESTAMP = 0,
    TF_COL_INT64 = 1,
    TF_COL_FLOAT64 = 2,
    TF_COL_FLOAT32 = 3,
    TF_COL_BOOL = 4,
    TF_COL_VARCHAR = 5,
    TF_COL_TAG = 6,
    TF_COL_DECIMAL = 7,
}

#[repr(C)]
#[derive(Clone, Copy)]
pub struct TFStr {
    pub ptr: *const c_char,
    pub len: u32,
}

#[repr(C)]
#[derive(Clone, Copy)]
pub union TFValueData {
    /// Microseconds since the Unix epoch (TF_COL_TIMESTAMP).
    pub ts_us: i64,
    pub int_val: i64,
    pub float_val: f64,
    pub float32_val: f32,
    /// 0 or 1. A byte rather than `bool` so any value written by C is valid.
    pub bool_val: u8,
    /// TF_COL_VARCHAR, TF_COL_TAG and TF_COL_DECIMAL (canonical string form).
    pub str_val: TFStr,
}

#[repr(C)]
#[derive(Clone, Copy)]
pub struct TFValue {
    /// A `TFColumnType` code; must match the column's declared type.
    pub kind: u8,
    pub is_null: bool,
    pub data: TFValueData,
}

#[repr(C)]
pub struct TFRow {
    pub col_count: u32,
    pub values: *mut TFValue,
}

/// Table configuration. String options may be NULL to select defaults.
#[repr(C)]
pub struct TFTableConfig {
    /// Overrides the table directory (DATA DIRECTORY); NULL = use `name`.
    pub data_dir: *const c_char,
    pub chunk_interval: *const c_char,
    pub retention_period: *const c_char,
    pub compression: *const c_char,
    /// 0 = default (3).
    pub compression_level: u8,
    pub hot_threshold: *const c_char,
    /// 0 = default (64 MiB).
    pub memtable_size_bytes: u64,
    pub ts_column_index: u32,
    pub column_count: u32,
    /// May be NULL; otherwise `column_count` NUL-terminated names.
    pub column_names: *const *const c_char,
    /// `column_count` `TFColumnType` codes.
    pub column_types: *const u8,
}

/// Opaque table handle.
pub struct TideFlowTable {
    table: Arc<Table>,
}

/// Opaque scan handle.
pub struct TideFlowScan {
    table: Arc<Table>,
    scan: Scan,
    current: Row,
    position: Option<Position>,
    out: Vec<TFValue>,
}

// ─── error plumbing ──────────────────────────────────────────────────────────

thread_local! {
    static LAST_ERROR: RefCell<Option<CString>> = const { RefCell::new(None) };
}

fn set_last_error(msg: &str) {
    let c = CString::new(msg.replace('\0', " ")).unwrap_or_default();
    LAST_ERROR.with(|e| *e.borrow_mut() = Some(c));
}

fn status_of(e: &Error) -> TFStatus {
    match e {
        Error::Io(io) if io.kind() == std::io::ErrorKind::OutOfMemory => TFStatus::TF_ERR_OOM,
        Error::Io(io) if io.kind() == std::io::ErrorKind::NotFound => TFStatus::TF_ERR_NOT_FOUND,
        Error::Io(_) => TFStatus::TF_ERR_IO,
        Error::Corrupt(_) => TFStatus::TF_ERR_CORRUPT,
        Error::Full(_) => TFStatus::TF_ERR_FULL,
        Error::NotFound(_) => TFStatus::TF_ERR_NOT_FOUND,
        Error::InvalidArg(_) => TFStatus::TF_ERR_INVALID_ARG,
        Error::ReadOnly(_) => TFStatus::TF_ERR_READONLY,
        Error::Unsupported(_) => TFStatus::TF_ERR_UNSUPPORTED,
    }
}

fn guard(f: impl FnOnce() -> Result<(), Error>) -> TFStatus {
    match catch_unwind(AssertUnwindSafe(f)) {
        Ok(Ok(())) => TFStatus::TF_OK,
        Ok(Err(e)) => {
            set_last_error(&e.to_string());
            status_of(&e)
        }
        Err(payload) => {
            let msg = payload
                .downcast_ref::<&str>()
                .map(|s| s.to_string())
                .or_else(|| payload.downcast_ref::<String>().cloned())
                .unwrap_or_else(|| "unknown panic".into());
            set_last_error(&format!("internal error (panic): {msg}"));
            TFStatus::TF_ERR_INTERNAL
        }
    }
}

fn invalid(msg: &str) -> Error {
    Error::InvalidArg(msg.into())
}

// ─── pointer helpers ─────────────────────────────────────────────────────────

/// # Safety
/// `p` must be NULL or point to a NUL-terminated string valid for `'a`.
unsafe fn opt_str<'a>(p: *const c_char, what: &str) -> Result<Option<&'a str>, Error> {
    if p.is_null() {
        return Ok(None);
    }
    // SAFETY: non-null and NUL-terminated per the caller contract.
    let s = unsafe { CStr::from_ptr(p) };
    s.to_str().map(Some).map_err(|_| invalid(&format!("{what} is not valid UTF-8")))
}

/// # Safety
/// Same as [`opt_str`].
unsafe fn req_str<'a>(p: *const c_char, what: &str) -> Result<&'a str, Error> {
    // SAFETY: forwarded caller contract.
    unsafe { opt_str(p, what) }?.ok_or_else(|| invalid(&format!("{what} is NULL")))
}

/// # Safety
/// `p` must be NULL (only if `n == 0`) or point to `n` initialized `T`s valid for `'a`.
unsafe fn slice<'a, T>(p: *const T, n: usize, what: &str) -> Result<&'a [T], Error> {
    if n == 0 {
        return Ok(&[]);
    }
    if p.is_null() {
        return Err(invalid(&format!("{what} is NULL")));
    }
    // SAFETY: non-null, `n` elements per the caller contract.
    Ok(unsafe { std::slice::from_raw_parts(p, n) })
}

/// # Safety
/// `config` must be NULL or point to a valid `TFTableConfig` whose pointers obey
/// the documented contract.
unsafe fn config_from_c(name: *const c_char, config: *const TFTableConfig) -> Result<TableConfig, Error> {
    // SAFETY: caller contract.
    let cfg = unsafe { config.as_ref() }.ok_or_else(|| invalid("config is NULL"))?;
    // SAFETY: caller contract for every pointer below.
    unsafe {
        let dir = match opt_str(cfg.data_dir, "data_dir")? {
            Some(d) => d,
            None => req_str(name, "table name")?,
        };
        let n = cfg.column_count as usize;
        let types = slice(cfg.column_types, n, "column_types")?;
        let names: Vec<String> = if cfg.column_names.is_null() {
            (0..n).map(|i| format!("c{i}")).collect()
        } else {
            slice(cfg.column_names, n, "column_names")?
                .iter()
                .map(|&p| req_str(p, "column name").map(str::to_owned))
                .collect::<Result<_, _>>()?
        };
        let columns = names
            .into_iter()
            .zip(types)
            .map(|(name, &t)| {
                let ty = ColumnType::from_u8(t).ok_or_else(|| invalid(&format!("unknown column type {t}")))?;
                Ok(Column { name, ty })
            })
            .collect::<Result<Vec<_>, Error>>()?;
        let schema = Schema::new(columns, cfg.ts_column_index as usize)?;
        let raw = RawOptions {
            chunk_interval: opt_str(cfg.chunk_interval, "chunk_interval")?,
            retention_period: opt_str(cfg.retention_period, "retention_period")?,
            compression: opt_str(cfg.compression, "compression")?,
            compression_level: cfg.compression_level,
            hot_threshold: opt_str(cfg.hot_threshold, "hot_threshold")?,
            memtable_size_bytes: cfg.memtable_size_bytes,
        };
        TableConfig::new(dir, schema, &raw)
    }
}

/// # Safety
/// `v` must be a valid `TFValue`; string pointers must reference `len` bytes.
unsafe fn value_from_c(v: &TFValue, ty: ColumnType) -> Result<Value, Error> {
    if v.is_null {
        return Ok(Value::Null);
    }
    if v.kind != ty as u8 {
        return Err(invalid(&format!("value kind {} does not match column type {:?}", v.kind, ty)));
    }
    // SAFETY: `kind` tells which union member C initialized.
    unsafe {
        Ok(match ty {
            ColumnType::Timestamp => Value::Timestamp(v.data.ts_us),
            ColumnType::Int64 => Value::Int(v.data.int_val),
            ColumnType::Float64 => Value::Float64(v.data.float_val),
            ColumnType::Float32 => Value::Float32(v.data.float32_val),
            ColumnType::Bool => Value::Bool(v.data.bool_val != 0),
            ColumnType::Varchar | ColumnType::Tag | ColumnType::Decimal => {
                let s = v.data.str_val;
                Value::Bytes(slice(s.ptr.cast::<u8>(), s.len as usize, "string value")?.to_vec())
            }
        })
    }
}

fn value_to_c(v: &Value, ty: ColumnType) -> TFValue {
    let kind = ty as u8;
    let (is_null, data) = match v {
        Value::Null => (true, TFValueData { int_val: 0 }),
        Value::Timestamp(t) => (false, TFValueData { ts_us: *t }),
        Value::Int(i) => (false, TFValueData { int_val: *i }),
        Value::Float64(f) => (false, TFValueData { float_val: *f }),
        Value::Float32(f) => (false, TFValueData { float32_val: *f }),
        Value::Bool(b) => (false, TFValueData { bool_val: u8::from(*b) }),
        Value::Bytes(b) => (false, TFValueData { str_val: TFStr { ptr: b.as_ptr().cast(), len: b.len() as u32 } }),
    };
    TFValue { kind, is_null, data }
}

impl TideFlowScan {
    fn new(table: Arc<Table>, scan: Scan) -> TideFlowScan {
        TideFlowScan { table, scan, current: Vec::new(), position: None, out: Vec::new() }
    }

    /// Publishes `next` through `out`, keeping the backing storage alive in `self`.
    fn emit(&mut self, next: Option<(Position, Row)>, out: &mut TFRow, end: &mut bool) {
        match next {
            None => {
                *end = true;
                self.position = None;
                out.col_count = 0;
                out.values = ptr::null_mut();
            }
            Some((pos, row)) => {
                *end = false;
                self.current = row;
                self.position = Some(pos);
                let cols = self.table.schema().columns();
                self.out = self.current.iter().zip(cols).map(|(v, c)| value_to_c(v, c.ty)).collect();
                out.col_count = self.out.len() as u32;
                out.values = self.out.as_mut_ptr();
            }
        }
    }
}

fn table_ref<'a>(t: *const TideFlowTable) -> Result<&'a TideFlowTable, Error> {
    // SAFETY: handles come from `tideflow_table_open` and stay valid until
    // `tideflow_table_close`, per the API contract.
    unsafe { t.as_ref() }.ok_or_else(|| invalid("table handle is NULL"))
}

fn scan_mut<'a>(s: *mut TideFlowScan) -> Result<&'a mut TideFlowScan, Error> {
    // SAFETY: as `table_ref`; scans are used by one thread at a time.
    unsafe { s.as_mut() }.ok_or_else(|| invalid("scan handle is NULL"))
}

fn out_mut<'a, T>(p: *mut T, what: &str) -> Result<&'a mut T, Error> {
    // SAFETY: the caller passes a valid, writable out-parameter or NULL.
    unsafe { p.as_mut() }.ok_or_else(|| invalid(&format!("{what} is NULL")))
}

// ─── table lifecycle ─────────────────────────────────────────────────────────

/// Creates the on-disk structure of a new table.
///
/// # Safety
/// `name` must be a NUL-terminated path; `config` must be valid.
#[no_mangle]
pub unsafe extern "C" fn tideflow_table_create(name: *const c_char, config: *const TFTableConfig) -> TFStatus {
    guard(|| {
        // SAFETY: caller contract.
        let cfg = unsafe { config_from_c(name, config) }?;
        Table::create(&cfg)
    })
}

/// Opens a table (running crash recovery). On success `*out_table` receives a
/// handle to release with `tideflow_table_close`.
///
/// # Safety
/// As `tideflow_table_create`; `out_table` must be writable.
#[no_mangle]
pub unsafe extern "C" fn tideflow_table_open(
    name: *const c_char,
    config: *const TFTableConfig,
    out_table: *mut *mut TideFlowTable,
) -> TFStatus {
    guard(|| {
        let out = out_mut(out_table, "out_table")?;
        *out = ptr::null_mut();
        // SAFETY: caller contract.
        let cfg = unsafe { config_from_c(name, config) }?;
        let table = Table::open(cfg)?;
        *out = Box::into_raw(Box::new(TideFlowTable { table: Arc::new(table) }));
        Ok(())
    })
}

/// Releases a table handle. Pending WAL data is flushed to the OS.
///
/// # Safety
/// `table` must come from `tideflow_table_open` and not be used afterwards.
/// Every scan opened on it should be closed first.
#[no_mangle]
pub unsafe extern "C" fn tideflow_table_close(table: *mut TideFlowTable) -> TFStatus {
    guard(|| {
        if table.is_null() {
            return Ok(());
        }
        // SAFETY: ownership is transferred back from C, exactly once.
        let t = unsafe { Box::from_raw(table) };
        t.table.sync_wal(true)
    })
}

/// # Safety
/// `name` must be a NUL-terminated path.
#[no_mangle]
pub unsafe extern "C" fn tideflow_table_drop(name: *const c_char) -> TFStatus {
    guard(|| {
        // SAFETY: caller contract.
        let dir = unsafe { req_str(name, "table name") }?;
        Table::drop_table(dir.as_ref())
    })
}

/// # Safety
/// `from` and `to` must be NUL-terminated paths.
#[no_mangle]
pub unsafe extern "C" fn tideflow_table_rename(from: *const c_char, to: *const c_char) -> TFStatus {
    guard(|| {
        // SAFETY: caller contract.
        let (from, to) = unsafe { (req_str(from, "from")?, req_str(to, "to")?) };
        Table::rename(from.as_ref(), to.as_ref())
    })
}

// ─── writes ──────────────────────────────────────────────────────────────────

/// Appends one row (WAL + MemTable). Durable after `tideflow_sync_wal(.., true)`.
///
/// # Safety
/// `row` must point to `col_count` valid values matching the table's columns.
#[no_mangle]
pub unsafe extern "C" fn tideflow_write_row(table: *mut TideFlowTable, row: *const TFRow) -> TFStatus {
    guard(|| {
        let t = table_ref(table)?;
        // SAFETY: caller contract.
        let row = unsafe { row.as_ref() }.ok_or_else(|| invalid("row is NULL"))?;
        // SAFETY: caller contract.
        let values = unsafe { slice(row.values, row.col_count as usize, "row values") }?;
        let cols = t.table.schema().columns();
        if values.len() != cols.len() {
            return Err(invalid(&format!("row has {} values, table has {} columns", values.len(), cols.len())));
        }
        let row = values
            .iter()
            .zip(cols)
            // SAFETY: values come from the caller-validated slice.
            .map(|(v, c)| unsafe { value_from_c(v, c.ty) })
            .collect::<Result<Vec<_>, _>>()?;
        t.table.write(row)
    })
}

/// Statement-end hook: flushes buffered WAL entries to the OS and, when
/// `durable`, fsyncs them.
///
/// # Safety
/// `table` must be a valid handle.
#[no_mangle]
pub unsafe extern "C" fn tideflow_sync_wal(table: *mut TideFlowTable, durable: bool) -> TFStatus {
    guard(|| table_ref(table)?.table.sync_wal(durable))
}

/// Seals the MemTable into chunk files.
///
/// # Safety
/// `table` must be a valid handle.
#[no_mangle]
pub unsafe extern "C" fn tideflow_flush(table: *mut TideFlowTable) -> TFStatus {
    guard(|| table_ref(table)?.table.flush())
}

/// Removes all rows (TRUNCATE TABLE / DELETE without WHERE).
///
/// # Safety
/// `table` must be a valid handle.
#[no_mangle]
pub unsafe extern "C" fn tideflow_truncate(table: *mut TideFlowTable) -> TFStatus {
    guard(|| table_ref(table)?.table.truncate())
}

// ─── scans ───────────────────────────────────────────────────────────────────

fn open_scan(
    table: *mut TideFlowTable,
    filter: &ScanFilter,
    sorted: bool,
    out_scan: *mut *mut TideFlowScan,
) -> TFStatus {
    guard(|| {
        let out = out_mut(out_scan, "out_scan")?;
        *out = ptr::null_mut();
        let t = table_ref(table)?;
        let scan = t.table.scan(filter, sorted)?;
        *out = Box::into_raw(Box::new(TideFlowScan::new(t.table.clone(), scan)));
        Ok(())
    })
}

/// Opens an unordered full-table scan (forward only).
///
/// # Safety
/// `table` must be a valid handle; `out_scan` writable.
#[no_mangle]
pub unsafe extern "C" fn tideflow_scan_open(table: *mut TideFlowTable, out_scan: *mut *mut TideFlowScan) -> TFStatus {
    open_scan(table, &ScanFilter::default(), false, out_scan)
}

/// Opens a scan of rows with `ts_start_us <= ts <= ts_end_us` whose TAG columns
/// equal the given values, ordered by timestamp. Supports `tideflow_scan_prev`.
///
/// # Safety
/// `tag_col_indices` and `tag_values` must hold `tag_count` elements each.
#[no_mangle]
pub unsafe extern "C" fn tideflow_range_scan_open(
    table: *mut TideFlowTable,
    ts_start_us: i64,
    ts_end_us: i64,
    tag_col_indices: *const u32,
    tag_values: *const TFValue,
    tag_count: u32,
    out_scan: *mut *mut TideFlowScan,
) -> TFStatus {
    let mut filter = ScanFilter::range(ts_start_us, ts_end_us);
    let status = guard(|| {
        let n = tag_count as usize;
        // SAFETY: caller contract.
        let (cols, vals) =
            unsafe { (slice(tag_col_indices, n, "tag_col_indices")?, slice(tag_values, n, "tag_values")?) };
        for (&c, v) in cols.iter().zip(vals) {
            // SAFETY: caller contract.
            filter.tags.push((c as usize, unsafe { value_from_c(v, ColumnType::Tag) }?));
        }
        Ok(())
    });
    if status != TFStatus::TF_OK {
        return status;
    }
    open_scan(table, &filter, true, out_scan)
}

/// Advances the scan. At the end, `*out_eof` is set and `out_row` emptied.
///
/// # Safety
/// `scan` must be valid; `out_row` and `out_eof` writable.
#[no_mangle]
pub unsafe extern "C" fn tideflow_scan_next(
    scan: *mut TideFlowScan,
    out_row: *mut TFRow,
    out_eof: *mut bool,
) -> TFStatus {
    guard(|| {
        let s = scan_mut(scan)?;
        let (row, eof) = (out_mut(out_row, "out_row")?, out_mut(out_eof, "out_eof")?);
        let next = s.scan.next_row()?;
        s.emit(next, row, eof);
        Ok(())
    })
}

/// Steps backwards (range scans only). Before the first row, `*out_bof` is set.
///
/// # Safety
/// As `tideflow_scan_next`.
#[no_mangle]
pub unsafe extern "C" fn tideflow_scan_prev(
    scan: *mut TideFlowScan,
    out_row: *mut TFRow,
    out_bof: *mut bool,
) -> TFStatus {
    guard(|| {
        let s = scan_mut(scan)?;
        let (row, bof) = (out_mut(out_row, "out_row")?, out_mut(out_bof, "out_bof")?);
        let prev = s.scan.prev_row()?;
        s.emit(prev, row, bof);
        Ok(())
    })
}

/// Positions a range scan after its last row (for `index_last`).
///
/// # Safety
/// `scan` must be valid.
#[no_mangle]
pub unsafe extern "C" fn tideflow_scan_seek_end(scan: *mut TideFlowScan) -> TFStatus {
    guard(|| scan_mut(scan)?.scan.seek_end())
}

/// Writes the `TF_POSITION_LEN`-byte position of the row last returned.
///
/// # Safety
/// `out_pos` must have room for `TF_POSITION_LEN` bytes.
#[no_mangle]
pub unsafe extern "C" fn tideflow_scan_position(scan: *mut TideFlowScan, out_pos: *mut u8) -> TFStatus {
    guard(|| {
        let s = scan_mut(scan)?;
        let pos = s.position.ok_or_else(|| invalid("no current row"))?;
        let out = out_mut(out_pos.cast::<[u8; POSITION_LEN]>(), "out_pos")?;
        *out = pos.to_bytes();
        Ok(())
    })
}

/// Re-reads the row at `pos` into `out_row` (handler `rnd_pos`). The scan's
/// current row becomes that row.
///
/// # Safety
/// `pos` must point to `TF_POSITION_LEN` readable bytes.
#[no_mangle]
pub unsafe extern "C" fn tideflow_scan_fetch(scan: *mut TideFlowScan, pos: *const u8, out_row: *mut TFRow) -> TFStatus {
    guard(|| {
        let s = scan_mut(scan)?;
        // SAFETY: caller contract.
        let pos = unsafe { pos.cast::<[u8; POSITION_LEN]>().as_ref() }.ok_or_else(|| invalid("pos is NULL"))?;
        let pos = Position::from_bytes(pos);
        let row = s.table.fetch(pos)?;
        let out = out_mut(out_row, "out_row")?;
        let mut end = false;
        s.emit(Some((pos, row)), out, &mut end);
        Ok(())
    })
}

/// # Safety
/// `scan` must come from a `*_scan_open` call and not be used afterwards.
#[no_mangle]
pub unsafe extern "C" fn tideflow_scan_close(scan: *mut TideFlowScan) -> TFStatus {
    guard(|| {
        if !scan.is_null() {
            // SAFETY: ownership is transferred back from C, exactly once.
            drop(unsafe { Box::from_raw(scan) });
        }
        Ok(())
    })
}

// ─── maintenance ─────────────────────────────────────────────────────────────

/// Merges chunks in a time range. Not implemented yet: returns `TF_ERR_UNSUPPORTED`.
///
/// # Safety
/// `table` must be a valid handle.
#[no_mangle]
pub unsafe extern "C" fn tideflow_compact(table: *mut TideFlowTable, ts_start_us: i64, ts_end_us: i64) -> TFStatus {
    guard(|| {
        table_ref(table)?;
        Err(Error::Unsupported(format!("compaction of [{ts_start_us}, {ts_end_us}] is not implemented yet")))
    })
}

/// Deletes chunks entirely older than RETENTION_PERIOD.
///
/// # Safety
/// `table` must be a valid handle.
#[no_mangle]
pub unsafe extern "C" fn tideflow_apply_retention(table: *mut TideFlowTable) -> TFStatus {
    guard(|| table_ref(table)?.table.apply_retention(tideflow_core::time::now_micros()).map(drop))
}

/// Verifies the CRC32 of every chunk. `*out_bad_chunks` receives the number of
/// damaged chunks; when non-zero, `tideflow_last_error` describes them.
///
/// # Safety
/// `table` must be valid; `out_bad_chunks` writable.
#[no_mangle]
pub unsafe extern "C" fn tideflow_check(table: *mut TideFlowTable, out_bad_chunks: *mut u32) -> TFStatus {
    guard(|| {
        let out = out_mut(out_bad_chunks, "out_bad_chunks")?;
        let problems = table_ref(table)?.table.check()?;
        *out = problems.len() as u32;
        if !problems.is_empty() {
            let msg: Vec<String> = problems.iter().map(|(f, why)| format!("{f}: {why}")).collect();
            set_last_error(&msg.join("; "));
        }
        Ok(())
    })
}

// ─── info ────────────────────────────────────────────────────────────────────

/// Any out pointer may be NULL if the caller is not interested.
///
/// # Safety
/// `table` must be valid; non-NULL out pointers writable.
#[no_mangle]
pub unsafe extern "C" fn tideflow_table_stats(
    table: *mut TideFlowTable,
    out_row_count: *mut u64,
    out_data_bytes: *mut u64,
    out_compressed_bytes: *mut u64,
    out_chunk_count: *mut u32,
) -> TFStatus {
    guard(|| {
        let s = table_ref(table)?.table.stats()?;
        // SAFETY: caller contract (NULL allowed).
        unsafe {
            if let Some(p) = out_row_count.as_mut() {
                *p = s.row_count;
            }
            if let Some(p) = out_data_bytes.as_mut() {
                *p = s.data_bytes;
            }
            if let Some(p) = out_compressed_bytes.as_mut() {
                *p = s.compressed_bytes;
            }
            if let Some(p) = out_chunk_count.as_mut() {
                *p = s.chunk_count;
            }
        }
        Ok(())
    })
}

/// Optimizer estimate of rows with `ts_start_us <= ts <= ts_end_us`.
///
/// # Safety
/// `table` must be valid; `out_rows` writable.
#[no_mangle]
pub unsafe extern "C" fn tideflow_estimate_rows(
    table: *mut TideFlowTable,
    ts_start_us: i64,
    ts_end_us: i64,
    out_rows: *mut u64,
) -> TFStatus {
    guard(|| {
        let out = out_mut(out_rows, "out_rows")?;
        *out = table_ref(table)?.table.estimate_rows(ts_start_us, ts_end_us)?;
        Ok(())
    })
}

// ─── memory ──────────────────────────────────────────────────────────────────

/// Message of the last error raised on the calling thread, or NULL. The
/// caller owns the result and must release it with `tideflow_free_str`.
#[no_mangle]
pub extern "C" fn tideflow_last_error() -> *mut c_char {
    LAST_ERROR.with(|e| e.borrow().as_ref().map_or(ptr::null_mut(), |c| c.clone().into_raw()))
}

/// Releases a string allocated by this library. NULL is ignored.
///
/// # Safety
/// `ptr` must be NULL or a pointer returned by this library, freed only once.
#[no_mangle]
pub unsafe extern "C" fn tideflow_free_str(ptr: *mut c_char) {
    if !ptr.is_null() {
        // SAFETY: produced by `CString::into_raw` in this library.
        drop(unsafe { CString::from_raw(ptr) });
    }
}
