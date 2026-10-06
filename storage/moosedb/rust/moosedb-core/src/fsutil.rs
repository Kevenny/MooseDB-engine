//! Durable file-system primitives.

use std::fs::{self, File};
use std::io::{self, Write};
use std::path::Path;

use crate::error::{Error, Result};

/// Marker wrapped inside the `io::Error` of a failed fsync so callers can
/// recognise it (see [`is_fsync_error`]). After a failed fsync the kernel may
/// have dropped the dirty pages, so a retry that "succeeds" proves nothing:
/// the only safe reaction is to stop writing until recovery has run.
#[derive(Debug)]
struct FsyncFailed(String);

impl std::fmt::Display for FsyncFailed {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "fsync failed: {}", self.0)
    }
}

impl std::error::Error for FsyncFailed {}

fn fsync_error(e: io::Error) -> Error {
    Error::Io(io::Error::new(e.kind(), FsyncFailed(e.to_string())))
}

/// Whether `e` came from a failed fsync (file or directory).
pub(crate) fn is_fsync_error(e: &Error) -> bool {
    matches!(e, Error::Io(io) if io.get_ref().is_some_and(|inner| inner.is::<FsyncFailed>()))
}

#[cfg(test)]
thread_local! {
    /// Test hook: `Some(n)` lets `n` more fsyncs of this thread succeed, then
    /// fails every one after them.
    static FSYNC_BUDGET: std::cell::Cell<Option<u32>> = const { std::cell::Cell::new(None) };
}

/// Test hook: fail every fsync of the current thread after `ok` more succeed.
#[cfg(test)]
pub(crate) fn fail_fsync_after(ok: u32) {
    FSYNC_BUDGET.with(|b| b.set(Some(ok)));
}

/// Test hook: fsyncs work again.
#[cfg(test)]
pub(crate) fn restore_fsync() {
    FSYNC_BUDGET.with(|b| b.set(None));
}

#[cfg(test)]
fn injected_failure() -> Result<()> {
    match FSYNC_BUDGET.with(std::cell::Cell::get) {
        Some(0) => Err(fsync_error(io::Error::other("injected fsync failure"))),
        Some(n) => {
            FSYNC_BUDGET.with(|b| b.set(Some(n - 1)));
            Ok(())
        }
        None => Ok(()),
    }
}

#[cfg(not(test))]
fn injected_failure() -> Result<()> {
    Ok(())
}

/// `File::sync_all` with failures tagged as fsync errors.
pub(crate) fn sync_all(f: &File) -> Result<()> {
    injected_failure()?;
    f.sync_all().map_err(fsync_error)
}

/// `File::sync_data` with failures tagged as fsync errors.
pub(crate) fn sync_data(f: &File) -> Result<()> {
    injected_failure()?;
    f.sync_data().map_err(fsync_error)
}

/// Makes directory entry changes (create/rename/unlink) durable.
pub(crate) fn sync_dir(dir: &Path) -> Result<()> {
    #[cfg(unix)]
    sync_all(&File::open(dir)?)?;
    #[cfg(not(unix))]
    let _ = dir; // Directories cannot be opened for sync on Windows; NTFS journals metadata.
    Ok(())
}

/// Atomically replaces `path` with `data`: write to a temp file, fsync it,
/// rename over the target, fsync the directory.
pub(crate) fn write_atomic(path: &Path, data: &[u8]) -> Result<()> {
    let tmp = path.with_extension("tmp");
    {
        let mut f = File::create(&tmp)?;
        f.write_all(data)?;
        sync_all(&f)?;
    }
    fs::rename(&tmp, path)?;
    if let Some(dir) = path.parent() {
        sync_dir(dir)?;
    }
    Ok(())
}

/// Removes a file, treating "already gone" as success.
pub(crate) fn remove_if_exists(path: &Path) -> Result<()> {
    match fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(e) => Err(e.into()),
    }
}
