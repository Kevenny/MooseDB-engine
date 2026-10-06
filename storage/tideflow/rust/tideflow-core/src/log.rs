//! Minimal diagnostics sink. mysqld redirects stderr to its error log, so
//! messages written here end up next to the server's own entries.

pub(crate) fn warn(msg: &str) {
    eprintln!("[TideFlow] Warning: {msg}");
}
