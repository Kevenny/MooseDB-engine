//! Durable file-system primitives.

use std::fs::{self, File};
use std::io::Write;
use std::path::Path;

use crate::error::Result;

/// Makes directory entry changes (create/rename/unlink) durable.
pub(crate) fn sync_dir(dir: &Path) -> Result<()> {
    #[cfg(unix)]
    File::open(dir)?.sync_all()?;
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
        f.sync_all()?;
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
