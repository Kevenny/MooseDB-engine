//! Column encodings (type-aware, lossless) and general-purpose codecs.
//!
//! A column block is first *encoded* according to its type (delta-of-delta
//! for timestamps, Gorilla for floats, ...) and the result is then
//! *compressed* with LZ4 (hot chunks) or ZSTD (cold chunks).

pub(crate) mod delta;
pub(crate) mod gorilla;
pub(crate) mod rle;
pub(crate) mod simple8b;

use crate::error::{corrupt, Error, Result};

/// General-purpose byte codec applied on top of the column encoding.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Codec {
    None,
    Lz4,
    /// ZSTD with the given level (1-19).
    Zstd(i32),
}

pub const CODEC_NONE: u8 = 0;
pub const CODEC_LZ4: u8 = 1;
pub const CODEC_ZSTD: u8 = 2;

impl Codec {
    pub fn id(self) -> u8 {
        match self {
            Codec::None => CODEC_NONE,
            Codec::Lz4 => CODEC_LZ4,
            Codec::Zstd(_) => CODEC_ZSTD,
        }
    }

    pub fn name_of(id: u8) -> &'static str {
        match id {
            CODEC_NONE => "NONE",
            CODEC_LZ4 => "LZ4",
            CODEC_ZSTD => "ZSTD",
            _ => "UNKNOWN",
        }
    }
}

pub(crate) fn compress(codec: Codec, data: &[u8]) -> Result<Vec<u8>> {
    match codec {
        Codec::None => Ok(data.to_vec()),
        #[cfg(feature = "compression-lz4")]
        Codec::Lz4 => Ok(lz4_flex::block::compress(data)),
        #[cfg(feature = "compression-zstd")]
        Codec::Zstd(level) => zstd::bulk::compress(data, level).map_err(Error::Io),
        #[allow(unreachable_patterns)]
        other => Err(Error::Unsupported(format!("codec {other:?} not compiled in"))),
    }
}

pub(crate) fn decompress(id: u8, data: &[u8], raw_len: usize) -> Result<Vec<u8>> {
    let out = match id {
        CODEC_NONE => data.to_vec(),
        #[cfg(feature = "compression-lz4")]
        CODEC_LZ4 => lz4_flex::block::decompress(data, raw_len).map_err(|e| corrupt(format!("LZ4: {e}")))?,
        #[cfg(feature = "compression-zstd")]
        CODEC_ZSTD => zstd::bulk::decompress(data, raw_len).map_err(|e| corrupt(format!("ZSTD: {e}")))?,
        other => return Err(Error::Unsupported(format!("codec {} not available", Codec::name_of(other)))),
    };
    if out.len() != raw_len {
        return Err(corrupt(format!("decompressed {} bytes, expected {raw_len}", out.len())));
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn codecs_roundtrip() {
        let data: Vec<u8> = (0..10_000u32).flat_map(|i| (i % 97).to_le_bytes()).collect();
        for codec in [Codec::None, Codec::Lz4, Codec::Zstd(3), Codec::Zstd(19)] {
            let c = compress(codec, &data).unwrap();
            if codec != Codec::None {
                assert!(c.len() < data.len() / 4, "{codec:?}: {}", c.len());
            }
            assert_eq!(decompress(codec.id(), &c, data.len()).unwrap(), data);
        }
        assert!(decompress(CODEC_ZSTD, b"garbage", 10).is_err());
        assert!(decompress(9, b"", 0).is_err());
    }
}
