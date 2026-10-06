//! Mapping from tag values to `series_id`.
//!
//! The map is not persisted separately: every chunk stores the tag values of
//! the series it contains, and the WAL stores full rows, so the index is
//! rebuilt on open from chunk metadata plus WAL replay. Ids are assigned
//! sequentially; since rebuilding visits chunks in id order and then replays
//! the WAL in order, the assignment is deterministic.

use std::collections::HashMap;

use crate::codec::encode_value;
use crate::error::{corrupt, Result};
use crate::schema::{Schema, Value};

/// Canonical byte encoding of a series' tag values.
pub(crate) fn series_key(tags: &[Value]) -> Vec<u8> {
    let mut key = Vec::new();
    for v in tags {
        encode_value(&mut key, v);
    }
    key
}

/// Tag values of a full row, in tag order.
pub(crate) fn row_tags(schema: &Schema, row: &[Value]) -> Vec<Value> {
    schema.tag_indices().iter().map(|&i| row[i].clone()).collect()
}

#[derive(Default)]
pub(crate) struct SeriesIndex {
    by_key: HashMap<Vec<u8>, u64>,
    tags_by_id: HashMap<u64, Vec<Value>>,
    next_id: u64,
}

impl SeriesIndex {
    pub(crate) fn len(&self) -> usize {
        self.tags_by_id.len()
    }

    #[cfg(test)]
    pub(crate) fn lookup(&self, tags: &[Value]) -> Option<u64> {
        self.by_key.get(&series_key(tags)).copied()
    }

    pub(crate) fn tags(&self, id: u64) -> Option<&[Value]> {
        self.tags_by_id.get(&id).map(Vec::as_slice)
    }

    /// Returns the id of the series with these tags, assigning a new one if needed.
    pub(crate) fn get_or_insert(&mut self, tags: &[Value]) -> u64 {
        let key = series_key(tags);
        if let Some(&id) = self.by_key.get(&key) {
            return id;
        }
        let id = self.next_id;
        self.next_id += 1;
        self.by_key.insert(key, id);
        self.tags_by_id.insert(id, tags.to_vec());
        id
    }

    /// Registers a series found in a chunk. The same series may appear in many
    /// chunks, but an id must always map to the same tags and vice versa.
    pub(crate) fn register(&mut self, id: u64, tags: &[Value]) -> Result<()> {
        let key = series_key(tags);
        match self.by_key.get(&key) {
            Some(&existing) if existing == id => return Ok(()),
            Some(&existing) => {
                return Err(corrupt(format!("series tags map to both id {existing} and id {id}")));
            }
            None => {}
        }
        if self.tags_by_id.contains_key(&id) {
            return Err(corrupt(format!("series id {id} used for two different tag sets")));
        }
        self.by_key.insert(key, id);
        self.tags_by_id.insert(id, tags.to_vec());
        self.next_id = self.next_id.max(id + 1);
        Ok(())
    }

    pub(crate) fn iter(&self) -> impl Iterator<Item = (u64, &[Value])> {
        self.tags_by_id.iter().map(|(id, t)| (*id, t.as_slice()))
    }

    pub(crate) fn clear(&mut self) {
        *self = SeriesIndex::default();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tags(s: &str) -> Vec<Value> {
        vec![Value::Bytes(s.as_bytes().to_vec())]
    }

    #[test]
    fn assigns_stable_ids() {
        let mut idx = SeriesIndex::default();
        let a = idx.get_or_insert(&tags("a"));
        let b = idx.get_or_insert(&tags("b"));
        assert_ne!(a, b);
        assert_eq!(idx.get_or_insert(&tags("a")), a);
        assert_eq!(idx.lookup(&tags("b")), Some(b));
        assert_eq!(idx.len(), 2);
    }

    #[test]
    fn register_rejects_conflicts() {
        let mut idx = SeriesIndex::default();
        idx.register(5, &tags("a")).unwrap();
        idx.register(5, &tags("a")).unwrap();
        assert!(idx.register(6, &tags("a")).is_err());
        assert!(idx.register(5, &tags("b")).is_err());
        assert_eq!(idx.get_or_insert(&tags("c")), 6, "next id continues after registered ids");
    }

    #[test]
    fn null_and_empty_tags_are_distinct() {
        let mut idx = SeriesIndex::default();
        let n = idx.get_or_insert(&[Value::Null]);
        let e = idx.get_or_insert(&[Value::Bytes(vec![])]);
        assert_ne!(n, e);
    }
}
