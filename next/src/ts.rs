//! Timestamps: F1 `YYYY-MM-DDTHH:MM:SSZ`, F2 `YYYY-MM-DDTHH:MM:SS+HH:MM`,
//! F3 digits only (epoch milliseconds, leading zeros allowed).

pub const DAY_MS: i64 = 86_400_000;

pub fn days_from_civil(y: i64, m: i64, d: i64) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = y.div_euclid(400);
    let yoe = y - era * 400;
    let doy = (153 * ((m + 9) % 12) + 2) / 5 + d - 1;
    era * 146_097 + yoe * 365 + yoe / 4 - yoe / 100 + doy - 719_468
}

pub fn civil_from_days(z: i64) -> (i64, i64, i64) {
    let z = z + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146_096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    (if m <= 2 { y + 1 } else { y }, m, d)
}

fn leap(y: i64) -> bool {
    y % 4 == 0 && (y % 100 != 0 || y % 400 == 0)
}

pub fn month_days(y: i64, m: i64) -> i64 {
    match m {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        2 => {
            if leap(y) {
                29
            } else {
                28
            }
        }
        _ => 0,
    }
}

/// Days since the epoch of a valid calendar date, by table near the window.
pub struct Clock {
    table: Vec<i32>,
    y0: i64,
    years: i64,
}

const INVALID_DAY: i32 = i32::MIN;

impl Clock {
    /// Covers every date within a few days of [lo_ms, hi_ms).
    pub fn new(lo_ms: i64, hi_ms: i64) -> Clock {
        let (y_lo, _, _) = civil_from_days((lo_ms - 3 * DAY_MS).div_euclid(DAY_MS));
        let (y_hi, _, _) = civil_from_days((hi_ms + 3 * DAY_MS).div_euclid(DAY_MS));
        let y0 = y_lo.max(0);
        let years = (y_hi.min(9999) - y0 + 1).max(1);
        let mut table = vec![INVALID_DAY; (years * 512) as usize];
        for yi in 0..years {
            let y = y0 + yi;
            for m in 1..=12 {
                for d in 1..=month_days(y, m) {
                    table[(yi * 512 + m * 32 + d) as usize] = days_from_civil(y, m, d) as i32;
                }
            }
        }
        Clock { table, y0, years }
    }

    #[inline(always)]
    pub fn days(&self, y: i64, m: i64, d: i64) -> Option<i64> {
        let yi = y - self.y0;
        if (yi as u64) < self.years as u64 && (m as u64) < 16 && (d as u64) < 32 {
            let v = unsafe { *self.table.get_unchecked((yi * 512 + m * 32 + d) as usize) };
            if v == INVALID_DAY {
                None
            } else {
                Some(v as i64)
            }
        } else {
            Self::days_slow(y, m, d)
        }
    }

    #[cold]
    fn days_slow(y: i64, m: i64, d: i64) -> Option<i64> {
        if !(1..=12).contains(&m) || d < 1 || d > month_days(y, m) {
            return None;
        }
        Some(days_from_civil(y, m, d))
    }
}

const ZEROS: u64 = 0x3030_3030_3030_3030;

/// Non-zero when some byte selected by `mask` (0xFF per byte) is not an ASCII digit.
#[inline(always)]
fn nondigit(x: u64, mask: u64) -> u64 {
    let t = x ^ ZEROS;
    let hi = t & 0xF0F0_F0F0_F0F0_F0F0;
    let lo = ((t & 0x0F0F_0F0F_0F0F_0F0F) + 0x0606_0606_0606_0606) & 0x1010_1010_1010_1010;
    (hi | lo) & mask
}

#[inline(always)]
fn byte(x: u64, i: u32) -> i64 {
    ((x >> (8 * i)) & 0xff) as i64
}

#[inline(always)]
unsafe fn load(p: *const u8) -> u64 {
    std::ptr::read_unaligned(p as *const u64)
}

/// F1 or F2 at `p` with `len` 20 or 25. Milliseconds since the epoch.
#[inline(always)]
pub unsafe fn parse_f12(p: *const u8, len: usize, clock: &Clock) -> Option<i64> {
    let a = load(p); // YYYY-MM-
    let b = load(p.add(8)); // DDTHH:MM
    let c = load(p.add(16)); // :SSZ or :SS+HH:M
    const SEP_A_MASK: u64 = 0xFF00_00FF_0000_0000;
    const SEP_A: u64 = 0x2D00_002D_0000_0000;
    const SEP_B_MASK: u64 = 0x0000_FF00_00FF_0000;
    const SEP_B: u64 = 0x0000_3A00_0054_0000;
    let bad_sep = ((a & SEP_A_MASK) ^ SEP_A) | ((b & SEP_B_MASK) ^ SEP_B) | ((c & 0xFF) ^ 0x3A);
    let bad_digit = nondigit(a, 0x00FF_FF00_FFFF_FFFF) | nondigit(b, 0xFFFF_00FF_FF00_FFFF) | nondigit(c, 0x0000_0000_00FF_FF00);
    if (bad_sep | bad_digit) != 0 {
        return None;
    }
    let ta = a ^ ZEROS;
    let tb = b ^ ZEROS;
    let tc = c ^ ZEROS;
    let y = byte(ta, 0) * 1000 + byte(ta, 1) * 100 + byte(ta, 2) * 10 + byte(ta, 3);
    let mo = byte(ta, 5) * 10 + byte(ta, 6);
    let d = byte(tb, 0) * 10 + byte(tb, 1);
    let h = byte(tb, 3) * 10 + byte(tb, 4);
    let mi = byte(tb, 6) * 10 + byte(tb, 7);
    let s = byte(tc, 1) * 10 + byte(tc, 2);
    if h > 23 || mi > 59 || s > 59 {
        return None;
    }
    let days = clock.days(y, mo, d)?;
    let ms = days * DAY_MS + (h * 3600 + mi * 60 + s) * 1000;
    if len == 20 {
        if byte(c, 3) != b'Z' as i64 {
            return None;
        }
        Some(ms)
    } else {
        // c bytes: 0 ':' 1 S 2 S 3 sign 4 H 5 H 6 ':' 7 M, and p[24] M
        let sign = byte(c, 3);
        let m2 = *p.add(24) as i64;
        if (sign != b'+' as i64 && sign != b'-' as i64) || byte(c, 6) != b':' as i64 {
            return None;
        }
        if nondigit(c, 0xFF00_FFFF_0000_0000) != 0 || !(b'0' as i64..=b'9' as i64).contains(&m2) {
            return None;
        }
        let oh = byte(tc, 4) * 10 + byte(tc, 5);
        let om = byte(tc, 7) * 10 + (m2 - b'0' as i64);
        if oh > 23 || om > 59 {
            return None;
        }
        let off = (oh * 60 + om) * 60_000;
        Some(if sign == b'+' as i64 { ms - off } else { ms + off })
    }
}

/// Eight ASCII digits (validated) to their value; first digit in the low byte.
#[inline(always)]
fn parse8(v: u64) -> u64 {
    let v = v ^ ZEROS;
    let v = (v * 10) + (v >> 8);
    let v = (((v & 0x0000_00FF_0000_00FF) * (100 + (1_000_000 << 32)))
        + (((v >> 16) & 0x0000_00FF_0000_00FF) * (1 + (10_000 << 32))))
        >> 32;
    v
}

/// F3: digits only. Returns None for any non-digit, an empty value, or a value
/// that does not fit in i64 (which can only be outside any window).
#[inline(always)]
pub unsafe fn parse_f3(p: *const u8, len: usize) -> Option<i64> {
    if len == 13 {
        let a = load(p);
        let b = load(p.add(5)); // bytes 5..13; bytes 5..8 overlap a
        if (nondigit(a, u64::MAX) | nondigit(b, u64::MAX)) != 0 {
            return None;
        }
        // value = first 8 digits * 10^5 + last 5 digits; last five are bytes 8..13 = b bytes 3..8
        let hi = parse8(a);
        let lo5 = parse8((b & 0xFFFF_FFFF_FF00_0000) | 0x0000_0000_0030_3030);
        return Some((hi * 100_000 + lo5) as i64);
    }
    parse_f3_slow(p, len)
}

#[cold]
#[inline(never)]
unsafe fn parse_f3_slow(p: *const u8, len: usize) -> Option<i64> {
    if len == 0 {
        return None;
    }
    let s = std::slice::from_raw_parts(p, len);
    if !s.iter().all(|c| c.is_ascii_digit()) {
        return None;
    }
    let mut i = 0;
    while i < len && s[i] == b'0' {
        i += 1;
    }
    if len - i > 19 {
        return None;
    }
    let mut v: u64 = 0;
    for &c in &s[i..] {
        v = v * 10 + (c - b'0') as u64;
    }
    if v > i64::MAX as u64 {
        return None;
    }
    Some(v as i64)
}

/// Any form. Milliseconds since the epoch.
#[inline(always)]
pub unsafe fn parse_ts(p: *const u8, len: usize, clock: &Clock) -> Option<i64> {
    if (len == 20 || len == 25) && *p.add(4) == b'-' {
        parse_f12(p, len, clock)
    } else {
        parse_f3(p, len)
    }
}

/// Reference parser over a slice (used by the generic pipeline and tests).
pub fn parse_ts_slice(s: &[u8]) -> Option<i64> {
    if s.is_empty() {
        return None;
    }
    if s.iter().all(|c| c.is_ascii_digit()) {
        let mut i = 0;
        while i < s.len() && s[i] == b'0' {
            i += 1;
        }
        if s.len() - i > 19 {
            return None;
        }
        let mut v: u128 = 0;
        for &c in &s[i..] {
            v = v * 10 + (c - b'0') as u128;
        }
        return if v > i64::MAX as u128 { None } else { Some(v as i64) };
    }
    if s.len() != 20 && s.len() != 25 {
        return None;
    }
    let dig = |i: usize| -> Option<i64> {
        let c = s[i];
        if c.is_ascii_digit() {
            Some((c - b'0') as i64)
        } else {
            None
        }
    };
    let num = |a: usize, n: usize| -> Option<i64> {
        let mut v = 0;
        for i in a..a + n {
            v = v * 10 + dig(i)?;
        }
        Some(v)
    };
    if s[4] != b'-' || s[7] != b'-' || s[10] != b'T' || s[13] != b':' || s[16] != b':' {
        return None;
    }
    let y = num(0, 4)?;
    let mo = num(5, 2)?;
    let d = num(8, 2)?;
    let h = num(11, 2)?;
    let mi = num(14, 2)?;
    let sec = num(17, 2)?;
    if !(1..=12).contains(&mo) || d < 1 || d > month_days(y, mo) || h > 23 || mi > 59 || sec > 59 {
        return None;
    }
    let ms = (days_from_civil(y, mo, d) * 86400 + h * 3600 + mi * 60 + sec) * 1000;
    if s.len() == 20 {
        return if s[19] == b'Z' { Some(ms) } else { None };
    }
    let sign = s[19];
    if (sign != b'+' && sign != b'-') || s[22] != b':' {
        return None;
    }
    let oh = num(20, 2)?;
    let om = num(23, 2)?;
    if oh > 23 || om > 59 {
        return None;
    }
    let off = (oh * 60 + om) * 60_000;
    Some(if sign == b'+' { ms - off } else { ms + off })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fast(s: &[u8], clock: &Clock) -> Option<i64> {
        let mut buf = vec![b'#'; s.len() + 64];
        buf[..s.len()].copy_from_slice(s);
        unsafe { parse_ts(buf.as_ptr(), s.len(), clock) }
    }

    #[test]
    fn forms() {
        let lo = parse_ts_slice(b"2026-10-01T00:00:00Z").unwrap();
        let clock = Clock::new(lo - 7 * DAY_MS, lo + 14 * DAY_MS);
        let cases: Vec<&[u8]> = vec![
            b"2026-10-03T23:58:10Z",
            b"2026-10-03T19:58:10-04:00",
            b"1791071890000",
            b"0001791071890000",
            b"2026-10-03T23:58:10z",
            b"2026-10-03t23:58:10Z",
            b"2026-10-03 23:58:10Z",
            b"2026-10-03T23:58:10",
            b"2026-10-03T23:58:10.123Z",
            b"1791071890000.0",
            b"1.79e12",
            b"-5",
            b"2026-13-03T23:58:10Z",
            b"2026-02-30T23:58:10Z",
            b"2027-02-29T23:58:10Z",
            b"2028-02-29T23:58:10Z",
            b"2026-10-03T24:58:10Z",
            b"2026-10-03T23:58:60Z",
            b"2026-10-03T23:58:10+24:00",
            b"2026-10-03T23:58:10+05:60",
            b"2026-10-03T23:58:10+0200",
            b"2026-10-03T23:58:10-00:00",
            b"2026-10-03T23:58:10+23:59",
            b"86400000",
            b"",
            b"123456789012345678901",
            b"9223372036854775807",
            b"9223372036854775808",
            b"00000000000000000000000001791071890000",
            "١٧٩١٠٧١٨٩٠٠٠٠".as_bytes(),
            "２０２６-10-03T23:58:10Z".as_bytes(),
            b"2026-10-0aT23:58:10Z",
            b"1791071890000\r",
            b"2026-10-03T23:58:10Z\r",
            b"0000000000000",
            b"9999999999999",
            b"1999-12-31T23:59:59Z",
            b"9999-12-31T23:59:59Z",
            b"0000-01-01T00:00:00Z",
        ];
        for c in cases {
            assert_eq!(fast(c, &clock), parse_ts_slice(c), "{:?}", String::from_utf8_lossy(c));
        }
        assert_eq!(parse_ts_slice(b"2026-10-03T19:58:10-04:00"), parse_ts_slice(b"2026-10-03T23:58:10Z"));
        assert_eq!(parse_ts_slice(b"1791071890000"), Some(1_791_071_890_000));
    }
}

impl Clock {
    /// Days for a year and `month * 32 + day` (both from validated digits).
    #[inline(always)]
    pub fn days_md(&self, y: i64, md: i64) -> Option<i64> {
        let yi = y - self.y0;
        if (yi as u64) < self.years as u64 {
            let v = unsafe { *self.table.get_unchecked((yi * 512 + md) as usize) };
            if v == INVALID_DAY {
                None
            } else {
                Some(v as i64)
            }
        } else {
            Self::days_slow(y, md >> 5, md & 31)
        }
    }
}

/// F1 (`len` 20) or F2 (`len` 25) with SSE. Same results as `parse_f12`.
#[cfg(target_feature = "avx2")]
#[inline(always)]
pub unsafe fn parse_f12_simd(p: *const u8, len: usize, clock: &Clock) -> Option<i64> {
    use std::arch::x86_64::*;
    let v = _mm_loadu_si128(p as *const __m128i);
    let tmpl = _mm_setr_epi8(48, 48, 48, 48, 45, 48, 48, 45, 48, 48, 84, 48, 48, 58, 48, 48);
    let x = _mm_xor_si128(v, tmpl);
    let maxv = _mm_setr_epi8(9, 9, 9, 9, 0, 9, 9, 0, 9, 9, 0, 9, 9, 0, 9, 9);
    let ok1 = _mm_movemask_epi8(_mm_cmpeq_epi8(_mm_max_epu8(x, maxv), maxv));
    let xs = _mm_shuffle_epi8(x, _mm_setr_epi8(0, 1, 2, 3, 5, 6, 8, 9, 11, 12, 14, 15, -1, -1, -1, -1));
    let pairs = _mm_maddubs_epi16(xs, _mm_set1_epi16(0x010A));
    let lim = _mm_setr_epi16(99, 99, 12, 31, 23, 59, 0, 0);
    let ok2 = _mm_movemask_epi8(_mm_cmpgt_epi16(pairs, lim));
    let vals = _mm_madd_epi16(pairs, _mm_setr_epi16(100, 1, 32, 1, 60, 1, 0, 0));
    let w = std::ptr::read_unaligned(p.add(16) as *const u64); // : S S (Z | sign H H : M)
    let t = w ^ 0x303A_3030_3030_303A; // ':' '0' '0' x '0' '0' ':' '0'
    // bytes 0 must be 0 (':'), 1..2 digits
    let s1 = (t >> 8) & 0xff;
    let s2 = (t >> 16) & 0xff;
    let sec = s1 * 10 + s2;
    let mut bad = (t & 0xff) | ((s1 > 9) as u64) | ((s2 > 9) as u64) | ((sec > 59) as u64);
    bad |= (ok1 != 0xffff) as u64 | (ok2 != 0) as u64;
    let year = _mm_cvtsi128_si32(vals) as i64;
    let md = _mm_extract_epi32::<1>(vals) as i64;
    let hm = _mm_extract_epi32::<2>(vals) as i64;
    let mut off = 0i64;
    if len == 20 {
        bad |= (((w >> 24) & 0xff) != b'Z' as u64) as u64;
    } else {
        let sign = (w >> 24) & 0xff;
        let h1 = (t >> 32) & 0xff;
        let h2 = (t >> 40) & 0xff;
        let m1 = (t >> 56) & 0xff;
        let m2 = (*p.add(24)).wrapping_sub(b'0') as u64;
        let oh = h1 * 10 + h2;
        let om = m1 * 10 + m2;
        bad |= ((sign != b'+' as u64) & (sign != b'-' as u64)) as u64
            | ((t >> 48) & 0xff)
            | ((h1 > 9) as u64) | ((h2 > 9) as u64) | ((m1 > 9) as u64) | ((m2 > 9) as u64)
            | ((oh > 23) as u64) | ((om > 59) as u64);
        let o = ((oh * 60 + om) * 60_000) as i64;
        off = if sign == b'+' as u64 { o } else { -o };
    }
    if bad != 0 {
        return None;
    }
    let days = clock.days_md(year, md)?;
    Some(days * DAY_MS + (hm * 60 + sec as i64) * 1000 - off)
}

/// Thirteen-digit F3 with SSE; any other length goes to the general parser.
#[cfg(target_feature = "avx2")]
#[inline(always)]
pub unsafe fn parse_f3_simd(p: *const u8, len: usize) -> Option<i64> {
    use std::arch::x86_64::*;
    if len != 13 {
        return parse_f3_slow(p, len);
    }
    let v = _mm_loadu_si128(p as *const __m128i);
    let d = _mm_sub_epi8(v, _mm_set1_epi8(48));
    // shift right by 3 bytes so the 13 digits occupy bytes 3..16, zeros in 0..3
    let d = _mm_shuffle_epi8(d, _mm_setr_epi8(-1, -1, -1, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12));
    let nine = _mm_set1_epi8(9);
    let ok = _mm_movemask_epi8(_mm_cmpeq_epi8(_mm_min_epu8(d, nine), d));
    if ok != 0xffff {
        return None;
    }
    let p2 = _mm_maddubs_epi16(d, _mm_set1_epi16(0x010A)); // 8 x 2-digit
    let p4 = _mm_madd_epi16(p2, _mm_setr_epi16(100, 1, 100, 1, 100, 1, 100, 1)); // 4 x 4-digit
    let p4 = _mm_packus_epi32(p4, p4);
    let p8 = _mm_madd_epi16(p4, _mm_setr_epi16(10000, 1, 10000, 1, 10000, 1, 10000, 1)); // 2 x 8-digit
    let hi = _mm_cvtsi128_si32(p8) as u32 as i64;
    let lo = _mm_extract_epi32::<1>(p8) as u32 as i64;
    Some(hi * 100_000_000 + lo)
}

#[inline(always)]
pub unsafe fn parse_ts_fast(p: *const u8, len: usize, clock: &Clock) -> Option<i64> {
    if (len == 20 || len == 25) && *p.add(4) == b'-' {
        parse_f12_simd(p, len, clock)
    } else {
        parse_f3_simd(p, len)
    }
}

#[cfg(not(target_feature = "avx2"))]
#[inline(always)]
pub unsafe fn parse_f12_simd(p: *const u8, len: usize, clock: &Clock) -> Option<i64> {
    parse_f12(p, len, clock)
}

#[cfg(not(target_feature = "avx2"))]
#[inline(always)]
pub unsafe fn parse_ts_fast(p: *const u8, len: usize, clock: &Clock) -> Option<i64> {
    parse_ts(p, len, clock)
}
