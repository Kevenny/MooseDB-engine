//! Parsing and semantics of the TideFlow TABLE OPTIONS.

use std::path::PathBuf;

use crate::compression::Codec;
use crate::error::{invalid, Result};
use crate::schema::Schema;
use crate::time::{add_months, month_ordinal, month_start, MICROS_PER_DAY, MICROS_PER_HOUR, MICROS_PER_WEEK};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Unit {
    Hour,
    Day,
    Week,
    Month,
    Year,
}

/// Parses `"<N> <UNIT>"` where UNIT accepts singular and plural forms,
/// case-insensitively.
fn parse_amount(s: &str) -> Result<(i64, Unit)> {
    let mut parts = s.split_whitespace();
    let (Some(n), Some(unit), None) = (parts.next(), parts.next(), parts.next()) else {
        return Err(invalid(format!("expected '<N> <UNIT>', got '{s}'")));
    };
    let n: i64 = n.parse().map_err(|_| invalid(format!("invalid amount in '{s}'")))?;
    if n <= 0 {
        return Err(invalid(format!("amount must be positive in '{s}'")));
    }
    let unit = match unit.to_ascii_uppercase().as_str() {
        "HOUR" | "HOURS" => Unit::Hour,
        "DAY" | "DAYS" => Unit::Day,
        "WEEK" | "WEEKS" => Unit::Week,
        "MONTH" | "MONTHS" => Unit::Month,
        "YEAR" | "YEARS" => Unit::Year,
        _ => return Err(invalid(format!("unknown time unit in '{s}'"))),
    };
    Ok((n, unit))
}

/// A calendar-aware length of time.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Period {
    Micros(i64),
    Months(i64),
}

impl Period {
    pub fn parse(s: &str) -> Result<Period> {
        let (n, unit) = parse_amount(s)?;
        let overflow = || invalid(format!("period '{s}' is too large"));
        Ok(match unit {
            Unit::Hour => Period::Micros(n.checked_mul(MICROS_PER_HOUR).ok_or_else(overflow)?),
            Unit::Day => Period::Micros(n.checked_mul(MICROS_PER_DAY).ok_or_else(overflow)?),
            Unit::Week => Period::Micros(n.checked_mul(MICROS_PER_WEEK).ok_or_else(overflow)?),
            Unit::Month => Period::Months(n),
            Unit::Year => Period::Months(n.checked_mul(12).ok_or_else(overflow)?),
        })
    }

    /// `ts - self`, saturating.
    pub fn before(self, ts: i64) -> i64 {
        match self {
            Period::Micros(m) => ts.saturating_sub(m),
            Period::Months(n) => add_months(ts, -n),
        }
    }
}

/// How rows are grouped into chunk files by time.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ChunkInterval {
    /// Fixed-width buckets aligned to `align` (epoch for hours/days, Monday for weeks).
    Fixed { width: i64, align: i64 },
    /// One calendar month.
    Month,
}

impl ChunkInterval {
    pub const DEFAULT: &'static str = "1 DAY";

    /// Accepts exactly the values listed in the spec: 1/6/12 HOUR, 1 DAY,
    /// 1 WEEK, 1 MONTH (plural unit names are tolerated).
    pub fn parse(s: &str) -> Result<ChunkInterval> {
        // 1970-01-05 was the first Monday after the epoch: ISO weeks.
        const MONDAY: i64 = 4 * MICROS_PER_DAY;
        Ok(match parse_amount(s)? {
            (n @ (1 | 6 | 12), Unit::Hour) => ChunkInterval::Fixed { width: n * MICROS_PER_HOUR, align: 0 },
            (1, Unit::Day) => ChunkInterval::Fixed { width: MICROS_PER_DAY, align: 0 },
            (1, Unit::Week) => ChunkInterval::Fixed { width: MICROS_PER_WEEK, align: MONDAY },
            (1, Unit::Month) => ChunkInterval::Month,
            _ => {
                return Err(invalid(format!(
                    "CHUNK_INTERVAL '{s}' not supported; use 1 HOUR, 6 HOUR, 12 HOUR, 1 DAY, 1 WEEK or 1 MONTH"
                )))
            }
        })
    }

    /// Half-open bucket `[start, end)` containing `ts`.
    pub fn bucket(self, ts: i64) -> (i64, i64) {
        match self {
            ChunkInterval::Fixed { width, align } => {
                let start = (ts - align).div_euclid(width) * width + align;
                (start, start.saturating_add(width))
            }
            ChunkInterval::Month => {
                let ord = month_ordinal(ts);
                (month_start(ord), month_start(ord + 1))
            }
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Retention {
    Forever,
    Keep(Period),
}

impl Retention {
    pub fn parse(s: &str) -> Result<Retention> {
        if s.trim().eq_ignore_ascii_case("FOREVER") {
            Ok(Retention::Forever)
        } else {
            Ok(Retention::Keep(Period::parse(s)?))
        }
    }

    /// Rows with `ts < cutoff(now)` are expired. `None` means nothing expires.
    pub fn cutoff(self, now: i64) -> Option<i64> {
        match self {
            Retention::Forever => None,
            Retention::Keep(p) => Some(p.before(now)),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Compression {
    None,
    Lz4,
    Zstd,
}

impl Compression {
    pub fn parse(s: &str) -> Result<Compression> {
        match s.trim().to_ascii_uppercase().as_str() {
            "NONE" => Ok(Compression::None),
            "LZ4" => Ok(Compression::Lz4),
            "ZSTD" => Ok(Compression::Zstd),
            _ => Err(invalid(format!("COMPRESSION '{s}' not supported; use ZSTD, LZ4 or NONE"))),
        }
    }
}

pub const DEFAULT_MEMTABLE_SIZE: u64 = 64 << 20;
/// Small enough for tests to force flushes, large enough to avoid pathological chunk counts.
pub const MIN_MEMTABLE_SIZE: u64 = 4 << 10;
pub const DEFAULT_COMPRESSION_LEVEL: u8 = 3;

/// Raw, unvalidated option strings as they arrive from SQL. `None` selects the default.
#[derive(Clone, Debug, Default)]
pub struct RawOptions<'a> {
    pub chunk_interval: Option<&'a str>,
    pub retention_period: Option<&'a str>,
    pub compression: Option<&'a str>,
    pub compression_level: u8,
    pub hot_threshold: Option<&'a str>,
    pub memtable_size_bytes: u64,
    /// Key id of the server's key management plugin; `None` = not encrypted.
    pub encryption_key_id: Option<u32>,
}

fn normalize(s: &str) -> String {
    s.split_whitespace().collect::<Vec<_>>().join(" ").to_ascii_uppercase()
}

/// Validated TABLE OPTIONS, independent of the column layout.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TableOptions {
    pub chunk_interval: ChunkInterval,
    pub retention: Retention,
    pub compression: Compression,
    pub compression_level: u8,
    pub hot_threshold: Period,
    pub memtable_size_bytes: u64,
    pub encryption_key_id: Option<u32>,
    /// Normalized option texts, for display (INFORMATION_SCHEMA).
    pub chunk_interval_text: String,
    pub retention_text: String,
    pub hot_threshold_text: String,
}

impl TableOptions {
    pub fn parse(raw: &RawOptions<'_>) -> Result<TableOptions> {
        let compression_level = match raw.compression_level {
            0 => DEFAULT_COMPRESSION_LEVEL,
            l @ 1..=19 => l,
            l => return Err(invalid(format!("COMPRESSION_LEVEL {l} out of range 1-19"))),
        };
        let memtable_size_bytes = match raw.memtable_size_bytes {
            0 => DEFAULT_MEMTABLE_SIZE,
            s if s < MIN_MEMTABLE_SIZE => {
                return Err(invalid(format!("MEMTABLE_SIZE must be at least {MIN_MEMTABLE_SIZE} bytes")))
            }
            s => s,
        };
        let ci = raw.chunk_interval.unwrap_or(ChunkInterval::DEFAULT);
        let rp = raw.retention_period.unwrap_or("FOREVER");
        let ht = raw.hot_threshold.unwrap_or("7 DAYS");
        Ok(TableOptions {
            chunk_interval: ChunkInterval::parse(ci)?,
            retention: Retention::parse(rp)?,
            compression: Compression::parse(raw.compression.unwrap_or("ZSTD"))?,
            compression_level,
            hot_threshold: Period::parse(ht)?,
            memtable_size_bytes,
            encryption_key_id: raw.encryption_key_id,
            chunk_interval_text: normalize(ci),
            retention_text: normalize(rp),
            hot_threshold_text: normalize(ht),
        })
    }

    pub fn compression_name(&self) -> &'static str {
        match self.compression {
            Compression::None => "NONE",
            Compression::Lz4 => "LZ4",
            Compression::Zstd => "ZSTD",
        }
    }

    /// Codec for chunks older than HOT_THRESHOLD.
    pub fn cold_codec(&self) -> Codec {
        match self.compression {
            Compression::None => Codec::None,
            Compression::Lz4 => Codec::Lz4,
            Compression::Zstd => Codec::Zstd(i32::from(self.compression_level)),
        }
    }

    /// Codec for recent chunks: cheap LZ4 unless compression is disabled.
    pub fn hot_codec(&self) -> Codec {
        match self.compression {
            Compression::None => Codec::None,
            _ => Codec::Lz4,
        }
    }

    /// Whether a bucket ending at `bucket_end` is still "hot" at `now`.
    pub fn is_hot(&self, bucket_end: i64, now: i64) -> bool {
        bucket_end > self.hot_threshold.before(now)
    }

    pub fn codec_for(&self, bucket_end: i64, now: i64) -> Codec {
        if self.is_hot(bucket_end, now) {
            self.hot_codec()
        } else {
            self.cold_codec()
        }
    }

    /// `key=value` text persisted in the table's OPTIONS file.
    pub fn to_text(&self) -> String {
        let mut s = format!(
            "chunk_interval={}\nretention_period={}\ncompression={}\ncompression_level={}\nhot_threshold={}\nmemtable_size={}\n",
            self.chunk_interval_text,
            self.retention_text,
            self.compression_name(),
            self.compression_level,
            self.hot_threshold_text,
            self.memtable_size_bytes
        );
        if let Some(k) = self.encryption_key_id {
            s.push_str(&format!("encryption_key_id={k}\n"));
        }
        s
    }

    pub fn from_text(text: &str) -> Result<TableOptions> {
        let mut map = std::collections::HashMap::new();
        for line in text.lines() {
            if let Some((k, v)) = line.split_once('=') {
                map.insert(k.trim(), v.trim());
            }
        }
        let num = |k: &str| -> Result<u64> {
            map.get(k).map_or(Ok(0), |v| v.parse().map_err(|_| invalid(format!("OPTIONS: bad {k}"))))
        };
        TableOptions::parse(&RawOptions {
            chunk_interval: map.get("chunk_interval").copied(),
            retention_period: map.get("retention_period").copied(),
            compression: map.get("compression").copied(),
            compression_level: u8::try_from(num("compression_level")?).unwrap_or(0),
            hot_threshold: map.get("hot_threshold").copied(),
            memtable_size_bytes: num("memtable_size")?,
            encryption_key_id: match map.get("encryption_key_id") {
                Some(v) => Some(v.parse().map_err(|_| invalid("OPTIONS: bad encryption_key_id"))?),
                None => None,
            },
        })
    }
}

/// Fully validated configuration of one table.
#[derive(Clone, Debug)]
pub struct TableConfig {
    pub dir: PathBuf,
    pub schema: Schema,
    pub opts: TableOptions,
}

impl TableConfig {
    pub fn new(dir: impl Into<PathBuf>, schema: Schema, raw: &RawOptions<'_>) -> Result<TableConfig> {
        Ok(TableConfig { dir: dir.into(), schema, opts: TableOptions::parse(raw)? })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::time::days_from_civil;

    #[test]
    fn chunk_interval_values() {
        for ok in ["1 HOUR", "6 hours", "12 HOUR", "1 DAY", "1 week", "1 MONTH"] {
            assert!(ChunkInterval::parse(ok).is_ok(), "{ok}");
        }
        for bad in ["2 HOUR", "2 DAY", "1 YEAR", "DAY", "1", "-1 DAY", "1 DAY extra"] {
            assert!(ChunkInterval::parse(bad).is_err(), "{bad}");
        }
    }

    #[test]
    fn buckets() {
        let day = ChunkInterval::parse("1 DAY").unwrap();
        let t = days_from_civil(2026, 10, 1) * MICROS_PER_DAY + 5;
        assert_eq!(day.bucket(t), (t - 5, t - 5 + MICROS_PER_DAY));
        // Thursday 2026-10-01 → week starts Monday 2026-09-28.
        let week = ChunkInterval::parse("1 WEEK").unwrap();
        assert_eq!(week.bucket(t).0, days_from_civil(2026, 9, 28) * MICROS_PER_DAY);
        let month = ChunkInterval::parse("1 MONTH").unwrap();
        assert_eq!(
            month.bucket(t),
            (days_from_civil(2026, 10, 1) * MICROS_PER_DAY, days_from_civil(2026, 11, 1) * MICROS_PER_DAY)
        );
        // Negative timestamps bucket downward, not toward zero.
        assert_eq!(day.bucket(-1).0, -MICROS_PER_DAY);
    }

    #[test]
    fn retention() {
        assert_eq!(Retention::parse("forever").unwrap(), Retention::Forever);
        assert_eq!(Retention::parse("90 DAYS").unwrap().cutoff(100 * MICROS_PER_DAY), Some(10 * MICROS_PER_DAY));
        let now = days_from_civil(2026, 10, 5) * MICROS_PER_DAY;
        assert_eq!(
            Retention::parse("1 YEAR").unwrap().cutoff(now),
            Some(days_from_civil(2025, 10, 5) * MICROS_PER_DAY)
        );
        assert!(Retention::parse("0 DAYS").is_err());
    }

    #[test]
    fn config_defaults_and_limits() {
        let schema =
            Schema::new(vec![crate::schema::Column { name: "ts".into(), ty: crate::schema::ColumnType::Timestamp }], 0)
                .unwrap();
        let c = TableConfig::new("/tmp/x", schema.clone(), &RawOptions::default()).unwrap();
        assert_eq!(c.opts.memtable_size_bytes, DEFAULT_MEMTABLE_SIZE);
        assert_eq!(c.opts.compression, Compression::Zstd);
        assert_eq!(c.opts.retention, Retention::Forever);
        assert_eq!(TableOptions::from_text(&c.opts.to_text()).unwrap(), c.opts);
        let hot = TableOptions::parse(&RawOptions { hot_threshold: Some("1 day"), ..Default::default() }).unwrap();
        assert_eq!(hot.codec_for(10 * MICROS_PER_DAY, 10 * MICROS_PER_DAY), Codec::Lz4);
        assert_eq!(hot.codec_for(MICROS_PER_DAY, 10 * MICROS_PER_DAY), Codec::Zstd(3));
        assert_eq!(hot.hot_threshold_text, "1 DAY");
        let bad = RawOptions { compression_level: 20, ..Default::default() };
        assert!(TableConfig::new("/tmp/x", schema, &bad).is_err());
    }
}
