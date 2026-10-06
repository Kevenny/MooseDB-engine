//! TideFlow storage core.
//!
//! Everything that touches bytes on disk lives here, in safe Rust. The
//! `tideflow-ffi` crate is a thin C ABI wrapper around this crate.
//!
//! Write path: `Table::write` → WAL append → MemTable; when the MemTable
//! exceeds its threshold it is sealed into one chunk per time bucket and the
//! MANIFEST is switched to include them (see `manifest` for the protocol).

#![forbid(unsafe_code)]

mod bytes;
pub mod chunk;
mod chunk_reader;
mod chunk_writer;
mod codec;
pub mod error;
mod fsutil;
pub mod index;
mod log;
mod manifest;
mod memtable;
pub mod options;
pub mod scan;
pub mod schema;
pub mod table;
pub mod time;
mod wal;

pub use error::{Error, Result};
pub use options::{RawOptions, TableConfig};
pub use scan::{Position, Scan, ScanFilter, POSITION_LEN};
pub use schema::{Column, ColumnType, Row, Schema, Value};
pub use table::{Table, TableStats};
