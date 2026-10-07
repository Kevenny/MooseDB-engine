//! Encryption at rest: AES-256-CTR.
//!
//! Keys come from the server's key management plugin through a provider
//! callback installed by the FFI layer. Every encrypted file carries the key
//! id, key version and a random 128-bit IV in a plaintext header, so files
//! stay readable after key rotation (old versions remain fetchable) and two
//! files never share a keystream. CTR is seekable, so any byte range — a
//! single column block, a WAL entry — is decrypted independently.

use std::sync::{Arc, RwLock};

use crate::bytes::{put_u32, ByteReader};
use crate::error::{corrupt, Error, Result};

pub const KEY_LEN: usize = 32;
pub const IV_LEN: usize = 16;
/// Serialized size of [`CipherParams`] (key id, version, IV, CRC).
pub const PARAMS_LEN: usize = 32;

/// `(key_id, version)` → `(version, key)`. `version == None` asks for the latest.
pub type KeyProvider = dyn Fn(u32, Option<u32>) -> Result<(u32, [u8; KEY_LEN])> + Send + Sync;

static PROVIDER: RwLock<Option<Arc<KeyProvider>>> = RwLock::new(None);

pub fn set_key_provider(p: Option<Arc<KeyProvider>>) {
    if let Ok(mut slot) = PROVIDER.write() {
        *slot = p;
    }
}

fn fetch_key(key_id: u32, version: Option<u32>) -> Result<(u32, [u8; KEY_LEN])> {
    let provider = PROVIDER
        .read()
        .ok()
        .and_then(|p| p.clone())
        .ok_or_else(|| Error::Unsupported("no encryption key provider (load a key management plugin)".into()))?;
    provider(key_id, version)
}

/// Latest key version the provider offers for `key_id`.
pub(crate) fn latest_version(key_id: u32) -> Result<u32> {
    fetch_key(key_id, None).map(|(v, _)| v)
}

/// Encryption parameters of one file, plus the resolved key.
#[derive(Clone)]
pub struct CipherParams {
    pub key_id: u32,
    pub key_version: u32,
    iv: [u8; IV_LEN],
    key: [u8; KEY_LEN],
}

impl std::fmt::Debug for CipherParams {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CipherParams").field("key_id", &self.key_id).field("key_version", &self.key_version).finish()
    }
}

impl CipherParams {
    /// Parameters for a new file: latest key version, fresh random IV.
    pub(crate) fn for_new_file(key_id: u32) -> Result<CipherParams> {
        let (key_version, key) = fetch_key(key_id, None)?;
        let mut iv = [0u8; IV_LEN];
        getrandom::getrandom(&mut iv).map_err(|e| Error::Io(std::io::Error::other(e.to_string())))?;
        Ok(CipherParams { key_id, key_version, iv, key })
    }

    pub(crate) fn encode(&self) -> [u8; PARAMS_LEN] {
        let mut b = Vec::with_capacity(PARAMS_LEN);
        put_u32(&mut b, self.key_id);
        put_u32(&mut b, self.key_version);
        b.extend_from_slice(&self.iv);
        put_u32(&mut b, 0);
        let crc = crc32fast::hash(&b);
        put_u32(&mut b, crc);
        let mut out = [0u8; PARAMS_LEN];
        out.copy_from_slice(&b);
        out
    }

    /// Parses a header written by `encode` and fetches the matching key.
    pub(crate) fn decode(b: &[u8]) -> Result<CipherParams> {
        if b.len() < PARAMS_LEN || crc32fast::hash(&b[..28]).to_le_bytes() != b[28..32] {
            return Err(corrupt("encryption header checksum mismatch"));
        }
        let mut r = ByteReader::new(b);
        let key_id = r.u32()?;
        let version = r.u32()?;
        let mut iv = [0u8; IV_LEN];
        iv.copy_from_slice(r.take(IV_LEN)?);
        let (key_version, key) = fetch_key(key_id, Some(version))?;
        Ok(CipherParams { key_id, key_version, iv, key })
    }

    /// XORs `data` with the keystream starting at byte `offset` of the stream.
    #[cfg(feature = "encryption")]
    pub(crate) fn apply(&self, data: &mut [u8], offset: u64) {
        use aes::cipher::{KeyIvInit, StreamCipher, StreamCipherSeek};
        let mut c = ctr::Ctr128BE::<aes::Aes256>::new(&self.key.into(), &self.iv.into());
        c.seek(offset);
        c.apply_keystream(data);
    }

    #[cfg(not(feature = "encryption"))]
    pub(crate) fn apply(&self, _data: &mut [u8], _offset: u64) {
        unreachable!("CipherParams cannot be built without the encryption feature")
    }
}

/// Fails early when encryption was requested but is not compiled in.
pub(crate) fn ensure_available() -> Result<()> {
    if cfg!(feature = "encryption") {
        Ok(())
    } else {
        Err(Error::Unsupported("MooseDB was built without the `encryption` feature".into()))
    }
}

#[cfg(test)]
pub(crate) mod test_keys {
    //! Deterministic key provider for tests: key = [id ^ version; 32].
    use super::*;

    pub(crate) fn install() {
        set_key_provider(Some(Arc::new(|id: u32, version: Option<u32>| {
            if id == 99 {
                return Err(Error::NotFound(format!("key {id}")));
            }
            let v = version.unwrap_or(2);
            Ok((v, [(id ^ v) as u8; KEY_LEN]))
        })));
    }
}

#[cfg(all(test, feature = "encryption"))]
mod tests {
    use super::*;

    #[test]
    fn ctr_is_seekable_and_reversible() {
        test_keys::install();
        let p = CipherParams::for_new_file(1).unwrap();
        assert_eq!(p.key_version, 2);
        let plain: Vec<u8> = (0..1000u32).map(|i| i as u8).collect();
        let mut whole = plain.clone();
        p.apply(&mut whole, 0);
        assert_ne!(whole, plain);
        // Decrypting a middle slice on its own gives the same plaintext.
        let mut part = whole[333..777].to_vec();
        p.apply(&mut part, 333);
        assert_eq!(part, &plain[333..777]);

        let q = CipherParams::decode(&p.encode()).unwrap();
        let mut back = whole.clone();
        q.apply(&mut back, 0);
        assert_eq!(back, plain);

        let mut bad = p.encode();
        bad[5] ^= 1;
        assert!(CipherParams::decode(&bad).is_err());
        assert!(CipherParams::for_new_file(99).is_err());
    }
}
