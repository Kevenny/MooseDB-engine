//! Process-wide LRU caches: decoded chunk blocks (and decoded series, for
//! random row fetches) and open chunk files.
//!
//! All are keyed by a chunk file's `uid`, a process-unique number assigned
//! when the file is opened, so entries never alias across tables or across a
//! deleted-and-recreated path. Blocks and decoded series share one LRU and one
//! byte budget (`moosedb_chunk_cache_size`).

use std::collections::{BTreeMap, HashMap};
use std::fs::File;
use std::hash::Hash;
use std::path::Path;
use std::sync::{Arc, Mutex, OnceLock};

use crate::block::ColumnData;
use crate::error::Result;
use crate::settings;

struct Lru<K, V> {
    map: HashMap<K, (V, u64, usize)>,
    order: BTreeMap<u64, K>,
    tick: u64,
    used: usize,
}

impl<K: Hash + Eq + Clone, V: Clone> Lru<K, V> {
    fn new() -> Self {
        Lru { map: HashMap::new(), order: BTreeMap::new(), tick: 0, used: 0 }
    }

    fn get(&mut self, k: &K) -> Option<V> {
        self.tick += 1;
        let tick = self.tick;
        let (v, t, _) = self.map.get_mut(k)?;
        self.order.remove(t);
        *t = tick;
        self.order.insert(tick, k.clone());
        Some(v.clone())
    }

    fn remove(&mut self, k: &K) {
        if let Some((_, t, size)) = self.map.remove(k) {
            self.order.remove(&t);
            self.used -= size;
        }
    }

    fn insert(&mut self, k: K, v: V, size: usize, capacity: usize) {
        self.remove(&k);
        if size > capacity {
            // Also honours a capacity that was lowered since the last insert.
            self.shrink(capacity);
            return;
        }
        self.tick += 1;
        self.order.insert(self.tick, k.clone());
        self.map.insert(k, (v, self.tick, size));
        self.used += size;
        self.shrink(capacity);
    }

    fn shrink(&mut self, capacity: usize) {
        while self.used > capacity {
            let Some((_, k)) = self.order.pop_first() else { break };
            if let Some((_, _, size)) = self.map.remove(&k) {
                self.used -= size;
            }
        }
    }

    fn retain(&mut self, mut keep: impl FnMut(&K) -> bool) {
        let dead: Vec<K> = self.map.keys().filter(|k| !keep(k)).cloned().collect();
        for k in dead {
            self.remove(&k);
        }
    }
}

/// A decoded (decrypted, checksummed, decompressed) column block.
pub(crate) struct BlockPayload {
    pub encoding: u8,
    pub data: Vec<u8>,
}

/// One series of a chunk with every data column decoded into native-width
/// arrays: a row is then built from them in O(columns), whatever the series
/// length. `columns[c]` is `None` for TAG columns (constant per series).
pub(crate) struct DecodedSeries {
    pub columns: Vec<Option<ColumnData>>,
}

impl DecodedSeries {
    fn size(&self) -> usize {
        64 + self.columns.iter().flatten().map(|c| c.heap_bytes() + 64).sum::<usize>()
    }
}

#[derive(Clone, PartialEq, Eq, Hash)]
enum CacheKey {
    /// (file uid, block offset)
    Block(u64, u64),
    /// (file uid, series entry index)
    Series(u64, u32),
}

impl CacheKey {
    fn uid(&self) -> u64 {
        match self {
            CacheKey::Block(uid, _) | CacheKey::Series(uid, _) => *uid,
        }
    }
}

#[derive(Clone)]
enum Cached {
    Block(Arc<BlockPayload>),
    Series(Arc<DecodedSeries>),
}

fn blocks() -> &'static Mutex<Lru<CacheKey, Cached>> {
    static C: OnceLock<Mutex<Lru<CacheKey, Cached>>> = OnceLock::new();
    C.get_or_init(|| Mutex::new(Lru::new()))
}

fn files() -> &'static Mutex<Lru<u64, Arc<File>>> {
    static C: OnceLock<Mutex<Lru<u64, Arc<File>>>> = OnceLock::new();
    C.get_or_init(|| Mutex::new(Lru::new()))
}

pub(crate) fn get_block(uid: u64, offset: u64) -> Option<Arc<BlockPayload>> {
    match blocks().lock().ok()?.get(&CacheKey::Block(uid, offset))? {
        Cached::Block(b) => Some(b),
        Cached::Series(_) => None,
    }
}

fn capacity() -> usize {
    usize::try_from(settings::get().chunk_cache_bytes()).unwrap_or(usize::MAX)
}

pub(crate) fn put_block(uid: u64, offset: u64, block: Arc<BlockPayload>) {
    let capacity = capacity();
    if capacity == 0 {
        return;
    }
    let size = block.data.len() + 64;
    if let Ok(mut c) = blocks().lock() {
        c.insert(CacheKey::Block(uid, offset), Cached::Block(block), size, capacity);
    }
}

pub(crate) fn get_series(uid: u64, index: u32) -> Option<Arc<DecodedSeries>> {
    match blocks().lock().ok()?.get(&CacheKey::Series(uid, index))? {
        Cached::Series(s) => Some(s),
        Cached::Block(_) => None,
    }
}

/// Keeps a decoded series; one bigger than the whole cache is not kept.
pub(crate) fn put_series(uid: u64, index: u32, series: Arc<DecodedSeries>) {
    let capacity = capacity();
    if capacity == 0 {
        return;
    }
    let size = series.size();
    if let Ok(mut c) = blocks().lock() {
        c.insert(CacheKey::Series(uid, index), Cached::Series(series), size, capacity);
    }
}

/// Applies a new capacity at once: entries over it are evicted now, not at
/// the next insert (a lowered `moosedb_chunk_cache_size` must free memory).
pub(crate) fn resize(capacity: u64) {
    if let Ok(mut c) = blocks().lock() {
        c.shrink(usize::try_from(capacity).unwrap_or(usize::MAX));
    }
}

/// Bytes currently held by the block/series cache (diagnostics, tests).
pub fn used_bytes() -> usize {
    blocks().lock().map_or(0, |c| c.used)
}

/// Opens (or reuses) a read handle for a chunk file.
pub(crate) fn open_file(uid: u64, path: &Path) -> Result<Arc<File>> {
    if let Some(f) = files().lock().ok().and_then(|mut c| c.get(&uid)) {
        return Ok(f);
    }
    let f = Arc::new(File::open(path)?);
    if let Ok(mut c) = files().lock() {
        c.insert(uid, f.clone(), 1, settings::get().max_open_chunks() as usize);
    }
    Ok(f)
}

/// Drops every cached entry of a chunk file (it is being deleted).
pub(crate) fn forget_file(uid: u64) {
    if let Ok(mut c) = files().lock() {
        c.remove(&uid);
    }
    if let Ok(mut c) = blocks().lock() {
        c.retain(|k| k.uid() != uid);
    }
}

/// Reads exactly `buf.len()` bytes at `offset` without moving a shared cursor.
pub(crate) fn read_at(file: &File, buf: &mut [u8], offset: u64) -> std::io::Result<()> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::FileExt;
        file.read_exact_at(buf, offset)
    }
    #[cfg(windows)]
    {
        use std::os::windows::fs::FileExt;
        let mut done = 0;
        while done < buf.len() {
            match file.seek_read(&mut buf[done..], offset + done as u64)? {
                0 => return Err(std::io::ErrorKind::UnexpectedEof.into()),
                n => done += n,
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lru_evicts_least_recently_used() {
        let mut l: Lru<u32, u32> = Lru::new();
        l.insert(1, 10, 1, 2);
        l.insert(2, 20, 1, 2);
        assert_eq!(l.get(&1), Some(10));
        l.insert(3, 30, 1, 2);
        assert_eq!(l.get(&2), None, "2 was least recently used");
        assert_eq!(l.get(&1), Some(10));
        assert_eq!(l.get(&3), Some(30));
        l.insert(4, 40, 5, 2);
        assert_eq!(l.get(&4), None, "entries larger than the cache are not kept");
        l.retain(|k| *k != 1);
        assert_eq!(l.get(&1), None);
        assert_eq!(l.used, 1);
    }
}
