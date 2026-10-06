//! Calendar arithmetic on Unix microseconds (proleptic Gregorian, UTC).
//!
//! Timestamps are timezone-less: DATETIME values are interpreted as UTC
//! wall-clock time, which keeps chunk boundaries stable regardless of the
//! server's `time_zone`.

pub const MICROS_PER_SEC: i64 = 1_000_000;
pub const MICROS_PER_HOUR: i64 = 3_600 * MICROS_PER_SEC;
pub const MICROS_PER_DAY: i64 = 24 * MICROS_PER_HOUR;
pub const MICROS_PER_WEEK: i64 = 7 * MICROS_PER_DAY;

/// Days since 1970-01-01 → (year, month 1-12, day 1-31). Howard Hinnant's algorithm.
pub fn civil_from_days(z: i64) -> (i64, u32, u32) {
    let z = z + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1_460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = (doy - (153 * mp + 2) / 5 + 1) as u32;
    let m = (if mp < 10 { mp + 3 } else { mp - 9 }) as u32;
    let y = yoe + era * 400;
    (if m <= 2 { y + 1 } else { y }, m, d)
}

/// (year, month 1-12, day 1-31) → days since 1970-01-01.
pub fn days_from_civil(y: i64, m: u32, d: u32) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = y.div_euclid(400);
    let yoe = y.rem_euclid(400);
    let m = i64::from(m);
    let mp = if m > 2 { m - 3 } else { m + 9 };
    let doy = (153 * mp + 2) / 5 + i64::from(d) - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146_097 + doe - 719_468
}

fn days_in_month(y: i64, m: u32) -> u32 {
    match m {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        _ if (y % 4 == 0 && y % 100 != 0) || y % 400 == 0 => 29,
        _ => 28,
    }
}

/// Month ordinal (`year * 12 + month - 1`) containing `ts`.
pub fn month_ordinal(ts: i64) -> i64 {
    let (y, m, _) = civil_from_days(ts.div_euclid(MICROS_PER_DAY));
    y * 12 + i64::from(m) - 1
}

/// First microsecond of the month with the given ordinal.
pub fn month_start(ordinal: i64) -> i64 {
    let y = ordinal.div_euclid(12);
    let m = ordinal.rem_euclid(12) as u32 + 1;
    days_from_civil(y, m, 1).saturating_mul(MICROS_PER_DAY)
}

/// Shifts `ts` by `months`, clamping the day to the target month's length
/// (e.g. Mar 31 − 1 month = Feb 28/29).
pub fn add_months(ts: i64, months: i64) -> i64 {
    let days = ts.div_euclid(MICROS_PER_DAY);
    let tod = ts.rem_euclid(MICROS_PER_DAY);
    let (y, m, d) = civil_from_days(days);
    // Beyond ~400 000 years the result cannot be represented in i64 microseconds
    // anyway; saturate instead of overflowing the calendar arithmetic (which
    // used to wrap into a cutoff in the future).
    const LIMIT: i128 = 400_000 * 12;
    let ord = i128::from(y) * 12 + i128::from(m) - 1 + i128::from(months);
    if ord > LIMIT {
        return i64::MAX;
    }
    if ord < -LIMIT {
        return i64::MIN;
    }
    let ord = ord as i64;
    let (ny, nm) = (ord.div_euclid(12), ord.rem_euclid(12) as u32 + 1);
    let nd = d.min(days_in_month(ny, nm));
    days_from_civil(ny, nm, nd).saturating_mul(MICROS_PER_DAY).saturating_add(tod)
}

/// `YYYYMMDDTHHMMSS`, used in chunk file names.
pub fn format_compact(ts: i64) -> String {
    let days = ts.div_euclid(MICROS_PER_DAY);
    let secs = ts.rem_euclid(MICROS_PER_DAY) / MICROS_PER_SEC;
    let (y, m, d) = civil_from_days(days);
    format!("{y:04}{m:02}{d:02}T{:02}{:02}{:02}", secs / 3600, secs / 60 % 60, secs % 60)
}

/// Current wall-clock time in Unix microseconds.
pub fn now_micros() -> i64 {
    match std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH) {
        Ok(d) => i64::try_from(d.as_micros()).unwrap_or(i64::MAX),
        Err(e) => -i64::try_from(e.duration().as_micros()).unwrap_or(i64::MAX),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn civil_roundtrip() {
        for z in [-800_000, -1, 0, 1, 59, 60, 10_957, 20_727, 2_932_896] {
            let (y, m, d) = civil_from_days(z);
            assert_eq!(days_from_civil(y, m, d), z, "day {z}");
        }
        assert_eq!(civil_from_days(0), (1970, 1, 1));
        assert_eq!(civil_from_days(20_727), (2026, 10, 1));
    }

    #[test]
    fn compact_format() {
        let ts = days_from_civil(2026, 10, 1) * MICROS_PER_DAY + 10 * MICROS_PER_HOUR + 5 * MICROS_PER_SEC;
        assert_eq!(format_compact(ts), "20261001T100005");
        assert_eq!(format_compact(-1), "19691231T235959");
    }

    #[test]
    fn add_months_saturates_instead_of_overflowing() {
        let now = days_from_civil(2026, 10, 1) * MICROS_PER_DAY;
        assert_eq!(add_months(now, i64::MIN), i64::MIN);
        assert_eq!(add_months(now, -475_894_274_072_643_224), i64::MIN);
        assert_eq!(add_months(now, i64::MAX), i64::MAX);
        assert!(add_months(now, -12 * 10_000) < now);
    }

    #[test]
    fn month_arithmetic() {
        let mar31 = days_from_civil(2024, 3, 31) * MICROS_PER_DAY;
        assert_eq!(add_months(mar31, -1), days_from_civil(2024, 2, 29) * MICROS_PER_DAY);
        assert_eq!(add_months(mar31, -13), days_from_civil(2023, 2, 28) * MICROS_PER_DAY);
        let ord = month_ordinal(mar31);
        assert_eq!(month_start(ord), days_from_civil(2024, 3, 1) * MICROS_PER_DAY);
        assert_eq!(month_start(ord + 10), days_from_civil(2025, 1, 1) * MICROS_PER_DAY);
    }
}
