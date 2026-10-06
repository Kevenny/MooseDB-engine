//! Little-endian encoding helpers shared by the WAL, chunk and manifest formats.

use crate::error::{corrupt, Result};

pub(crate) fn put_u8(buf: &mut Vec<u8>, v: u8) {
    buf.push(v);
}

pub(crate) fn put_u16(buf: &mut Vec<u8>, v: u16) {
    buf.extend_from_slice(&v.to_le_bytes());
}

pub(crate) fn put_u32(buf: &mut Vec<u8>, v: u32) {
    buf.extend_from_slice(&v.to_le_bytes());
}

pub(crate) fn put_u64(buf: &mut Vec<u8>, v: u64) {
    buf.extend_from_slice(&v.to_le_bytes());
}

pub(crate) fn put_i64(buf: &mut Vec<u8>, v: i64) {
    buf.extend_from_slice(&v.to_le_bytes());
}

/// Length-prefixed byte string. Callers must have validated that
/// `b.len() <= u32::MAX` (see `Schema::validate_row`).
pub(crate) fn put_bytes(buf: &mut Vec<u8>, b: &[u8]) {
    put_u32(buf, b.len() as u32);
    buf.extend_from_slice(b);
}

/// Bounds-checked cursor over a byte slice. Every read that would run past
/// the end returns `Error::Corrupt` instead of panicking.
pub(crate) struct ByteReader<'a> {
    buf: &'a [u8],
    pos: usize,
}

impl<'a> ByteReader<'a> {
    pub(crate) fn new(buf: &'a [u8]) -> Self {
        ByteReader { buf, pos: 0 }
    }

    pub(crate) fn remaining(&self) -> usize {
        self.buf.len() - self.pos
    }

    pub(crate) fn take(&mut self, n: usize) -> Result<&'a [u8]> {
        let end = self
            .pos
            .checked_add(n)
            .filter(|&e| e <= self.buf.len())
            .ok_or_else(|| corrupt(format!("unexpected end of data at offset {}", self.pos)))?;
        let s = &self.buf[self.pos..end];
        self.pos = end;
        Ok(s)
    }

    fn array<const N: usize>(&mut self) -> Result<[u8; N]> {
        let mut a = [0u8; N];
        a.copy_from_slice(self.take(N)?);
        Ok(a)
    }

    pub(crate) fn u8(&mut self) -> Result<u8> {
        Ok(self.array::<1>()?[0])
    }

    pub(crate) fn u16(&mut self) -> Result<u16> {
        Ok(u16::from_le_bytes(self.array()?))
    }

    pub(crate) fn u32(&mut self) -> Result<u32> {
        Ok(u32::from_le_bytes(self.array()?))
    }

    pub(crate) fn u64(&mut self) -> Result<u64> {
        Ok(u64::from_le_bytes(self.array()?))
    }

    pub(crate) fn i64(&mut self) -> Result<i64> {
        Ok(i64::from_le_bytes(self.array()?))
    }

    pub(crate) fn bytes(&mut self) -> Result<&'a [u8]> {
        let n = self.u32()? as usize;
        self.take(n)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip_and_bounds() {
        let mut buf = Vec::new();
        put_u8(&mut buf, 7);
        put_u16(&mut buf, 0xBEEF);
        put_i64(&mut buf, -42);
        put_bytes(&mut buf, b"abc");
        let mut r = ByteReader::new(&buf);
        assert_eq!(r.u8().unwrap(), 7);
        assert_eq!(r.u16().unwrap(), 0xBEEF);
        assert_eq!(r.i64().unwrap(), -42);
        assert_eq!(r.bytes().unwrap(), b"abc");
        assert_eq!(r.remaining(), 0);
        assert!(r.u8().is_err());
    }

    #[test]
    fn oversized_length_prefix_is_corrupt_not_panic() {
        let mut buf = Vec::new();
        put_u32(&mut buf, u32::MAX);
        let mut r = ByteReader::new(&buf);
        assert!(r.bytes().is_err());
    }
}
