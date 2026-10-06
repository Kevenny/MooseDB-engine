//! Self-describing row encoding used by the WAL and for tag values in chunks.
//!
//! ```text
//! row   := col_count:u16 value*
//! value := 0                      NULL
//!        | 1 i64                  TIMESTAMP (µs)
//!        | 2 i64                  INT64
//!        | 3 u64                  FLOAT64 (IEEE-754 bits)
//!        | 4 u32                  FLOAT32 (IEEE-754 bits)
//!        | 5 u8                   BOOL
//!        | 6 len:u32 bytes        VARCHAR / TAG / DECIMAL
//! ```

use crate::bytes::{put_bytes, put_i64, put_u16, put_u32, put_u64, put_u8, ByteReader};
use crate::error::{corrupt, Result};
use crate::schema::{Row, Value};

pub(crate) fn encode_value(buf: &mut Vec<u8>, v: &Value) {
    match v {
        Value::Null => put_u8(buf, 0),
        Value::Timestamp(t) => {
            put_u8(buf, 1);
            put_i64(buf, *t);
        }
        Value::Int(i) => {
            put_u8(buf, 2);
            put_i64(buf, *i);
        }
        Value::Float64(f) => {
            put_u8(buf, 3);
            put_u64(buf, f.to_bits());
        }
        Value::Float32(f) => {
            put_u8(buf, 4);
            put_u32(buf, f.to_bits());
        }
        Value::Bool(b) => {
            put_u8(buf, 5);
            put_u8(buf, u8::from(*b));
        }
        Value::Bytes(b) => {
            put_u8(buf, 6);
            put_bytes(buf, b);
        }
    }
}

pub(crate) fn decode_value(r: &mut ByteReader<'_>) -> Result<Value> {
    Ok(match r.u8()? {
        0 => Value::Null,
        1 => Value::Timestamp(r.i64()?),
        2 => Value::Int(r.i64()?),
        3 => Value::Float64(f64::from_bits(r.u64()?)),
        4 => Value::Float32(f32::from_bits(r.u32()?)),
        5 => Value::Bool(r.u8()? != 0),
        6 => Value::Bytes(r.bytes()?.to_vec()),
        t => return Err(corrupt(format!("unknown value tag {t}"))),
    })
}

/// Encodes a row. `row.len()` must be `<= MAX_COLUMNS` (enforced by `Schema`).
pub(crate) fn encode_row(buf: &mut Vec<u8>, row: &[Value]) {
    put_u16(buf, row.len() as u16);
    for v in row {
        encode_value(buf, v);
    }
}

pub(crate) fn decode_row(r: &mut ByteReader<'_>) -> Result<Row> {
    let n = r.u16()? as usize;
    // Every value needs at least one byte, so `n` cannot exceed what is left;
    // checking first avoids a huge allocation driven by a corrupt count.
    if n > r.remaining() {
        return Err(corrupt(format!("row claims {n} values but only {} bytes remain", r.remaining())));
    }
    let mut row = Vec::with_capacity(n);
    for _ in 0..n {
        row.push(decode_value(r)?);
    }
    Ok(row)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn row_roundtrip() {
        let row = vec![
            Value::Timestamp(1_700_000_000_000_000),
            Value::Bytes(b"srv01".to_vec()),
            Value::Float64(42.5),
            Value::Float32(-1.25),
            Value::Int(-7),
            Value::Bool(true),
            Value::Null,
        ];
        let mut buf = Vec::new();
        encode_row(&mut buf, &row);
        let mut r = ByteReader::new(&buf);
        assert_eq!(decode_row(&mut r).unwrap(), row);
        assert_eq!(r.remaining(), 0);
    }

    #[test]
    fn nan_bits_preserved() {
        let mut buf = Vec::new();
        encode_value(&mut buf, &Value::Float64(f64::NAN));
        match decode_value(&mut ByteReader::new(&buf)).unwrap() {
            Value::Float64(f) => assert!(f.is_nan()),
            v => panic!("unexpected {v:?}"),
        }
    }

    #[test]
    fn garbage_is_corrupt() {
        assert!(decode_row(&mut ByteReader::new(&[0xff, 0xff, 9])).is_err());
        assert!(decode_value(&mut ByteReader::new(&[42])).is_err());
    }
}
