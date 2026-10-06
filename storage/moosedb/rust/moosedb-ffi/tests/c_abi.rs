//! Exercises the C ABI from Rust, the same way `ha_moosedb.cc` drives it.

use std::ffi::{c_char, CStr, CString};
use std::ptr;

use moosedb::*;

type FfiResult<T> = Result<T, (TFStatus, String)>;

fn check(s: TFStatus) -> FfiResult<()> {
    if s == TFStatus::TF_OK {
        return Ok(());
    }
    let p = moosedb_last_error();
    let msg = if p.is_null() { String::new() } else { unsafe { CStr::from_ptr(p) }.to_string_lossy().into_owned() };
    unsafe { moosedb_free_str(p) };
    Err((s, msg))
}

/// Owns every allocation a `TFTableConfig` points into.
struct Config {
    _strings: Vec<CString>,
    _name_ptrs: Vec<*const c_char>,
    _types: Vec<u8>,
    cfg: TFTableConfig,
}

fn config() -> Config {
    let strings: Vec<CString> =
        ["ts", "host", "value", "1 HOUR", "7 DAYS"].iter().map(|s| CString::new(*s).unwrap()).collect();
    let name_ptrs: Vec<_> = strings[..3].iter().map(|c| c.as_ptr()).collect();
    let types =
        vec![TFColumnType::TF_COL_TIMESTAMP as u8, TFColumnType::TF_COL_TAG as u8, TFColumnType::TF_COL_FLOAT64 as u8];
    let cfg = TFTableConfig {
        data_dir: ptr::null(),
        chunk_interval: strings[3].as_ptr(),
        retention_period: strings[4].as_ptr(),
        compression: ptr::null(),
        compression_level: 0,
        hot_threshold: ptr::null(),
        memtable_size_bytes: 0,
        ts_column_index: 0,
        column_count: 3,
        column_names: name_ptrs.as_ptr(),
        column_types: types.as_ptr(),
        encryption_key_id: 0,
    };
    Config { _strings: strings, _name_ptrs: name_ptrs, _types: types, cfg }
}

fn tag(s: &str) -> TFValue {
    TFValue {
        kind: TFColumnType::TF_COL_TAG as u8,
        is_null: false,
        data: TFValueData { str_val: TFStr { ptr: s.as_ptr().cast(), len: s.len() as u32 } },
    }
}

struct Handle(*mut MooseDBTable);

impl Drop for Handle {
    fn drop(&mut self) {
        check(unsafe { moosedb_table_close(self.0) }).unwrap();
    }
}

fn read_rows(scan: *mut MooseDBScan, backwards: bool, limit: usize) -> FfiResult<Vec<(i64, f64)>> {
    let mut out = Vec::new();
    let mut row = TFRow { col_count: 0, values: ptr::null_mut() };
    let mut end = false;
    while out.len() < limit {
        let s = unsafe {
            if backwards {
                moosedb_scan_prev(scan, &mut row, &mut end)
            } else {
                moosedb_scan_next(scan, &mut row, &mut end)
            }
        };
        check(s)?;
        if end {
            break;
        }
        let vals = unsafe { std::slice::from_raw_parts(row.values, row.col_count as usize) };
        out.push(unsafe { (vals[0].data.ts_us, vals[2].data.float_val) });
    }
    Ok(out)
}

impl Handle {
    fn create_and_open(path: &str) -> FfiResult<Handle> {
        let c = config();
        let p = CString::new(path).unwrap();
        check(unsafe { moosedb_table_create(p.as_ptr(), &c.cfg) })?;
        let mut t = ptr::null_mut();
        check(unsafe { moosedb_table_open(p.as_ptr(), &c.cfg, &mut t) })?;
        Ok(Handle(t))
    }

    fn write_values(&self, mut vals: [TFValue; 3]) -> FfiResult<()> {
        let row = TFRow { col_count: 3, values: vals.as_mut_ptr() };
        check(unsafe { moosedb_write_row(self.0, &row) })
    }

    fn write(&self, ts: i64, host: &str, v: f64) -> FfiResult<()> {
        self.write_values([
            TFValue { kind: TFColumnType::TF_COL_TIMESTAMP as u8, is_null: false, data: TFValueData { ts_us: ts } },
            tag(host),
            TFValue { kind: TFColumnType::TF_COL_FLOAT64 as u8, is_null: false, data: TFValueData { float_val: v } },
        ])
    }

    fn count(&self) -> FfiResult<u64> {
        let mut n = 0;
        check(unsafe { moosedb_table_stats(self.0, &mut n, ptr::null_mut(), ptr::null_mut(), ptr::null_mut()) })?;
        Ok(n)
    }

    fn range(&self, host: Option<&str>, lo: i64, hi: i64, sorted_desc_first: bool) -> FfiResult<Vec<(i64, f64)>> {
        let cols = [1u32];
        let tags: Vec<TFValue> = host.map(tag).into_iter().collect();
        let mut scan = ptr::null_mut();
        check(unsafe {
            moosedb_range_scan_open(self.0, lo, hi, cols.as_ptr(), tags.as_ptr(), tags.len() as u32, &mut scan)
        })?;
        let rows = if sorted_desc_first {
            check(unsafe { moosedb_scan_seek_end(scan) }).and_then(|()| read_rows(scan, true, 1))
        } else {
            read_rows(scan, false, usize::MAX)
        };
        check(unsafe { moosedb_scan_close(scan) })?;
        rows
    }
}

#[test]
fn end_to_end_through_c_abi() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("t_ffi");
    let path = path.to_str().unwrap();
    let t = Handle::create_and_open(path).unwrap();

    t.write(1_000_000, "srv01", 42.5).unwrap();
    t.write(2_000_000, "srv01", 43.1).unwrap();
    t.write(3_000_000, "srv02", 11.2).unwrap();
    assert_eq!(t.count().unwrap(), 3);
    check(unsafe { moosedb_flush(t.0) }).unwrap();

    assert_eq!(t.range(Some("srv01"), 0, 10_000_000, false).unwrap(), vec![(1_000_000, 42.5), (2_000_000, 43.1)]);
    assert_eq!(t.range(None, 0, 2_500_000, true).unwrap(), vec![(2_000_000, 43.1)]);

    // Full scan + position round trip (rnd_pos).
    let mut scan = ptr::null_mut();
    check(unsafe { moosedb_scan_open(t.0, &mut scan) }).unwrap();
    let mut row = TFRow { col_count: 0, values: ptr::null_mut() };
    let mut eof = false;
    check(unsafe { moosedb_scan_next(scan, &mut row, &mut eof) }).unwrap();
    assert!(!eof);
    let mut pos = [0u8; TF_POSITION_LEN];
    check(unsafe { moosedb_scan_position(scan, pos.as_mut_ptr()) }).unwrap();
    let first_ts = unsafe { (*row.values).data.ts_us };
    check(unsafe { moosedb_scan_next(scan, &mut row, &mut eof) }).unwrap();
    let mut snap = ptr::null_mut();
    check(unsafe { moosedb_scan_snapshot(scan, &mut snap) }).unwrap();
    check(unsafe { moosedb_scan_close(scan) }).unwrap();
    check(unsafe { moosedb_flush(t.0) }).unwrap(); // the snapshot still resolves the position
    check(unsafe { moosedb_snapshot_fetch(snap, pos.as_ptr(), &mut row) }).unwrap();
    assert_eq!(unsafe { (*row.values).data.ts_us }, first_ts);
    check(unsafe { moosedb_snapshot_close(snap) }).unwrap();

    // Series listing (TAG pushdown support).
    let mut list = ptr::null_mut();
    check(unsafe { moosedb_series_list(t.0, &mut list) }).unwrap();
    assert_eq!(unsafe { moosedb_series_list_len(list) }, 2);
    let mut id = 0u64;
    check(unsafe { moosedb_series_list_get(list, 1, &mut id, &mut row) }).unwrap();
    let tag = unsafe { (*row.values).data.str_val };
    assert_eq!(unsafe { std::slice::from_raw_parts(tag.ptr.cast::<u8>(), tag.len as usize) }, b"srv02");
    check(unsafe { moosedb_series_list_close(list) }).unwrap();
    let mut scan = ptr::null_mut();
    check(unsafe { moosedb_scan_open_filtered(t.0, i64::MIN, i64::MAX, &id, 1, true, &mut scan) }).unwrap();
    check(unsafe { moosedb_scan_next(scan, &mut row, &mut eof) }).unwrap();
    assert_eq!(unsafe { (*row.values).data.ts_us }, 3_000_000);
    check(unsafe { moosedb_scan_next(scan, &mut row, &mut eof) }).unwrap();
    assert!(eof);
    check(unsafe { moosedb_scan_close(scan) }).unwrap();

    // Diagnostics.
    let mut info = ptr::null_mut();
    let cpath = CString::new(path).unwrap();
    check(unsafe { moosedb_inspect(cpath.as_ptr(), &mut info) }).unwrap();
    let ti = unsafe { &*moosedb_info_table(info) };
    assert!(ti.is_open);
    assert_eq!(ti.row_count, 3);
    assert_eq!(unsafe { CStr::from_ptr(ti.chunk_interval) }.to_str().unwrap(), "1 HOUR");
    assert_eq!(unsafe { moosedb_info_chunk_count(info) }, ti.chunk_count);
    assert!(unsafe { moosedb_info_chunk(info, 99) }.is_null());
    unsafe { moosedb_info_close(info) };

    let mut bad = u32::MAX;
    check(unsafe { moosedb_check(t.0, &mut bad) }).unwrap();
    assert_eq!(bad, 0);

    check(unsafe { moosedb_truncate(t.0) }).unwrap();
    assert_eq!(t.count().unwrap(), 0);
    drop(t);

    let p = CString::new(path).unwrap();
    check(unsafe { moosedb_table_drop(p.as_ptr()) }).unwrap();
    assert_eq!(unsafe { moosedb_table_drop(p.as_ptr()) }, TFStatus::TF_ERR_NOT_FOUND);
}

#[test]
fn errors_are_reported_not_panicked() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("t_err");
    let t = Handle::create_and_open(path.to_str().unwrap()).unwrap();

    // Wrong value kind for the timestamp column.
    let v = TFValue { kind: TFColumnType::TF_COL_INT64 as u8, is_null: false, data: TFValueData { int_val: 1 } };
    let err = t.write_values([v, tag("a"), v]).unwrap_err();
    assert_eq!(err.0, TFStatus::TF_ERR_INVALID_ARG);
    assert!(err.1.contains("does not match"), "{}", err.1);

    // NULL timestamp.
    let null_ts = TFValue { kind: 0, is_null: true, data: TFValueData { int_val: 0 } };
    assert_eq!(t.write_values([null_ts, tag("a"), null_ts]).unwrap_err().0, TFStatus::TF_ERR_INVALID_ARG);

    // NULL handles and out-pointers.
    assert_eq!(unsafe { moosedb_flush(ptr::null_mut()) }, TFStatus::TF_ERR_INVALID_ARG);
    assert_eq!(unsafe { moosedb_scan_open(t.0, ptr::null_mut()) }, TFStatus::TF_ERR_INVALID_ARG);
    assert_eq!(unsafe { moosedb_table_open(ptr::null(), ptr::null(), ptr::null_mut()) }, TFStatus::TF_ERR_INVALID_ARG);

    // Unsorted scans cannot go backwards.
    let mut scan = ptr::null_mut();
    check(unsafe { moosedb_scan_open(t.0, &mut scan) }).unwrap();
    assert_eq!(read_rows(scan, true, 1).unwrap_err().0, TFStatus::TF_ERR_UNSUPPORTED);
    check(unsafe { moosedb_scan_close(scan) }).unwrap();

    assert_eq!(unsafe { moosedb_compact(t.0, 0, 1) }, TFStatus::TF_OK);
}

#[test]
fn invalid_options_are_rejected() {
    let dir = tempfile::tempdir().unwrap();
    let p = CString::new(dir.path().join("t_opt").to_str().unwrap()).unwrap();
    let bad_interval = CString::new("2 DAY").unwrap();
    let mut c = config();
    c.cfg.chunk_interval = bad_interval.as_ptr();
    let err = check(unsafe { moosedb_table_create(p.as_ptr(), &c.cfg) }).unwrap_err();
    assert_eq!(err.0, TFStatus::TF_ERR_INVALID_ARG);
    assert!(err.1.contains("CHUNK_INTERVAL"), "{}", err.1);
}
