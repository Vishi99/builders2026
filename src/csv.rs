//! Byte-oriented input for the Shopping Sessions CSV dialect.
use std::ffi::c_void;
use std::fs::{self, File};
use std::io;
use std::ops::Deref;
use std::os::fd::AsRawFd;
use std::os::unix::ffi::OsStrExt;
use std::path::Path;
use std::ptr::{self, NonNull};
use std::slice;

unsafe extern "C" {
    fn mmap(
        addr: *mut c_void,
        len: usize,
        prot: i32,
        flags: i32,
        fd: i32,
        offset: i64,
    ) -> *mut c_void;
    fn munmap(addr: *mut c_void, len: usize) -> i32;
    fn memchr(start: *const c_void, byte: i32, len: usize) -> *const c_void;
}

/// A read-only view of an immutable scorer input file.
pub struct Mapped {
    ptr: NonNull<u8>,
    len: usize,
}

// The mappings are read-only and live until all borrowed ranges are dropped.
unsafe impl Send for Mapped {}
unsafe impl Sync for Mapped {}

impl Mapped {
    pub fn open(path: &Path) -> io::Result<Self> {
        let file = File::open(path)?;
        let len = usize::try_from(file.metadata()?.len())
            .ok()
            .filter(|&n| n <= isize::MAX as usize)
            .ok_or_else(|| io::Error::other("input file is too large to map"))?;
        if len == 0 {
            return Ok(Self {
                ptr: NonNull::dangling(),
                len,
            });
        }

        const PROT_READ: i32 = 1;
        const MAP_PRIVATE: i32 = 2;
        let addr = unsafe {
            mmap(
                ptr::null_mut(),
                len,
                PROT_READ,
                MAP_PRIVATE,
                file.as_raw_fd(),
                0,
            )
        };
        if addr as isize == -1 {
            return Err(io::Error::last_os_error());
        }
        let Some(ptr) = NonNull::new(addr.cast()) else {
            unsafe { munmap(addr, len) };
            return Err(io::Error::other("file was mapped at address zero"));
        };
        Ok(Self { ptr, len })
    }
}

impl Deref for Mapped {
    type Target = [u8];

    fn deref(&self) -> &[u8] {
        unsafe { slice::from_raw_parts(self.ptr.as_ptr(), self.len) }
    }
}

impl Drop for Mapped {
    fn drop(&mut self) {
        if self.len != 0 {
            unsafe { munmap(self.ptr.as_ptr().cast(), self.len) };
        }
    }
}

pub fn map_dir(dir: &Path) -> io::Result<Vec<Mapped>> {
    let mut files = Vec::new();
    let entries = match fs::read_dir(dir) {
        Ok(entries) => entries,
        Err(err) if err.kind() == io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(err) => return Err(err),
    };
    for entry in entries {
        let entry = entry?;
        if entry.file_name().as_bytes().ends_with(b".csv") {
            let path = entry.path();
            files.push(path);
        }
    }
    files.sort_unstable();
    files.iter().map(|path| Mapped::open(path)).collect()
}

#[inline]
fn find(bytes: &[u8], byte: u8) -> Option<usize> {
    let start = bytes.as_ptr();
    let end = unsafe { memchr(start.cast(), i32::from(byte), bytes.len()) };
    (!end.is_null()).then(|| unsafe { end.cast::<u8>().offset_from(start) as usize })
}

pub fn after_header(bytes: &[u8]) -> &[u8] {
    find(bytes, b'\n').map_or(&[], |header| &bytes[header + 1..])
}

/// Workers receive disjoint slices aligned to complete physical rows.
pub fn chunks(inputs: &[Mapped], target_size: usize) -> Vec<&[u8]> {
    let mut ranges = Vec::new();
    for file in inputs {
        let mut remaining = after_header(file);
        while !remaining.is_empty() {
            let stop = target_size.max(1).min(remaining.len());
            let end = if stop == remaining.len() || remaining[stop - 1] == b'\n' {
                stop
            } else {
                find(&remaining[stop..], b'\n').map_or(remaining.len(), |n| stop + n + 1)
            };
            let (task, rest) = remaining.split_at(end);
            ranges.push(task);
            remaining = rest;
        }
    }
    // Give any large task a chance to start before the tail of small shards.
    ranges.sort_unstable_by_key(|range| std::cmp::Reverse(range.len()));
    ranges
}

/// Census the first field. The caller uses it for a conservative filter, so
/// structurally malformed rows may be included and only false negatives matter.
pub fn event_ids<'a>(bytes: &'a [u8], mut visit: impl FnMut(&'a [u8])) {
    let mut start = 0;

    #[cfg(target_arch = "x86_64")]
    if std::is_x86_feature_detected!("avx2") {
        start = unsafe { avx2_event_ids(bytes, &mut visit) };
    }

    while start < bytes.len() {
        let newline = find(&bytes[start..], b'\n').map_or(bytes.len(), |n| start + n);
        if let Some(id) = first_id(&bytes[start..newline]) {
            visit(id);
        }
        start = newline + 1;
    }
}

#[inline]
fn unquote_id(mut id: &[u8]) -> Option<&[u8]> {
    if let Some(inner) = id.strip_prefix(b"\"").and_then(|v| v.strip_suffix(b"\"")) {
        id = inner;
    }
    (!id.is_empty()).then_some(id)
}

#[inline]
fn first_id(line: &[u8]) -> Option<&[u8]> {
    let comma = find(line, b',')?;
    unquote_id(&line[..comma])
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2")]
unsafe fn avx2_event_ids<'a>(bytes: &'a [u8], visit: &mut impl FnMut(&'a [u8])) -> usize {
    use std::arch::x86_64::*;
    let mut start = 0;
    let newline = unsafe { _mm256_set1_epi8(b'\n' as i8) };
    let comma = unsafe { _mm_set1_epi8(b',' as i8) };
    for (block_index, block) in bytes.chunks_exact(32).enumerate() {
        let raw = unsafe { _mm256_loadu_si256(block.as_ptr().cast()) };
        let mut newlines = unsafe { _mm256_movemask_epi8(_mm256_cmpeq_epi8(raw, newline)) } as u32;
        while newlines != 0 {
            let end = block_index * 32 + newlines.trailing_zeros() as usize;
            newlines &= newlines - 1;
            let line = &bytes[start..end];
            let id = if line.len() >= 16 {
                let prefix = unsafe { _mm_loadu_si128(line.as_ptr().cast()) };
                let hits = unsafe { _mm_movemask_epi8(_mm_cmpeq_epi8(prefix, comma)) } as u32;
                if hits == 0 {
                    first_id(line)
                } else {
                    unquote_id(&line[..hits.trailing_zeros() as usize])
                }
            } else {
                first_id(line)
            };
            if let Some(id) = id {
                visit(id);
            }
            start = end + 1;
        }
    }
    start
}

#[inline]
fn scalar_fields<const N: usize>(row: &[u8]) -> Option<[&[u8]; N]> {
    if row.is_empty() {
        return None;
    }
    let mut fields = [&[] as &[u8]; N];
    let mut values = row.split(|&b| b == b',');
    for field in &mut fields {
        let mut bytes = values.next()?;
        if let Some(tail) = bytes.strip_prefix(b"\"") {
            bytes = tail.strip_suffix(b"\"")?;
        }
        if find(bytes, b'"').is_some() {
            return None;
        }
        *field = bytes;
    }
    values.next().is_none().then_some(fields)
}

fn scalar_rows<'a, const N: usize>(bytes: &'a [u8], mut visit: impl FnMut([&'a [u8]; N])) {
    let mut start = 0;
    while start < bytes.len() {
        let newline = find(&bytes[start..], b'\n').map_or(bytes.len(), |n| start + n);
        if let Some(fields) = scalar_fields(&bytes[start..newline]) {
            visit(fields);
        }
        start = newline + 1;
    }
}

#[cfg(target_arch = "x86_64")]
#[derive(Clone, Copy, Default)]
struct Block {
    newline: u64,
    comma: u64,
    quote: u64,
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2")]
unsafe fn avx2_block(ptr: *const u8) -> Block {
    use std::arch::x86_64::*;

    let first = unsafe { _mm256_loadu_si256(ptr.cast()) };
    let second = unsafe { _mm256_loadu_si256(ptr.add(32).cast()) };
    let compare = |byte: u8| unsafe {
        let needle = _mm256_set1_epi8(byte as i8);
        let lo = _mm256_movemask_epi8(_mm256_cmpeq_epi8(first, needle)) as u32;
        let hi = _mm256_movemask_epi8(_mm256_cmpeq_epi8(second, needle)) as u32;
        u64::from(lo) | (u64::from(hi) << 32)
    };
    Block {
        newline: compare(b'\n'),
        comma: compare(b','),
        quote: compare(b'"'),
    }
}

#[cfg(target_arch = "x86_64")]
#[inline(always)]
fn avx512_block(ptr: *const u8) -> Block {
    let newline;
    let comma;
    let quote;
    // Dispatch checks AVX-512BW with OS support before this is used.
    // Each call receives a complete 64-byte region.
    unsafe {
        std::arch::asm!(
            "vmovdqu64 zmm0, [{ptr}]",
            "vpbroadcastb zmm1, {lf:e}",
            "vpcmpeqb k1, zmm0, zmm1",
            "kmovq {newline}, k1",
            "vpbroadcastb zmm1, {separator:e}",
            "vpcmpeqb k1, zmm0, zmm1",
            "kmovq {comma}, k1",
            "vpbroadcastb zmm1, {double_quote:e}",
            "vpcmpeqb k1, zmm0, zmm1",
            "kmovq {quote}, k1",
            ptr = in(reg) ptr,
            lf = in(reg) u32::from(b'\n'),
            separator = in(reg) u32::from(b','),
            double_quote = in(reg) u32::from(b'"'),
            newline = out(reg) newline,
            comma = out(reg) comma,
            quote = out(reg) quote,
            lateout("zmm0") _,
            lateout("zmm1") _,
            lateout("k1") _,
            options(nostack, readonly, pure, preserves_flags),
        );
    }
    Block {
        newline,
        comma,
        quote,
    }
}

#[cfg(target_arch = "x86_64")]
#[inline(always)]
fn fields_from_masks<const N: usize>(
    row: &[u8],
    mut commas: u128,
    quotes: u128,
) -> Option<[&[u8]; N]> {
    if row.is_empty() || commas.count_ones() as usize + 1 != N {
        return None;
    }

    let starters = (commas << 1) | 1;
    let closers = (commas >> 1) | (1 << (row.len() - 1));
    if quotes & !(starters | closers) != 0 {
        return None;
    }

    let mut result = [&[] as &[u8]; N];
    let mut start = 0;
    for (i, field) in result.iter_mut().enumerate() {
        let end = if i + 1 == N {
            row.len()
        } else {
            commas.trailing_zeros() as usize
        };
        let quoted_field = quotes != 0 && start < end;
        let quote_at_start = quoted_field && unsafe { *row.get_unchecked(start) == b'"' };
        let quote_at_end = quoted_field && unsafe { *row.get_unchecked(end - 1) == b'"' };
        if quote_at_start != quote_at_end || (quote_at_start && end - start < 2) {
            return None;
        }
        *field = unsafe {
            row.get_unchecked(start + usize::from(quote_at_start)..end - usize::from(quote_at_end))
        };
        commas &= commas.wrapping_sub(1);
        start = end + 1;
    }
    Some(result)
}

#[cfg(target_arch = "x86_64")]
#[inline(always)]
fn vector_rows<'a, const N: usize>(
    bytes: &'a [u8],
    mut visit: impl FnMut([&'a [u8]; N]),
    classify: impl Fn(*const u8) -> Block,
) {
    let mut pos = 0;
    while bytes.len() - pos >= 128 {
        let first = classify(unsafe { bytes.as_ptr().add(pos) });
        let n0 = first.newline.trailing_zeros() as usize;
        let (length, commas, quotes) = if n0 < 64 {
            (n0, first.comma as u128, first.quote as u128)
        } else {
            let next = classify(unsafe { bytes.as_ptr().add(pos + 64) });
            let n1 = next.newline.trailing_zeros() as usize;
            if n1 >= 64 {
                let end = find(&bytes[pos + 128..], b'\n').map_or(bytes.len(), |n| pos + 128 + n);
                if let Some(fields) = scalar_fields(&bytes[pos..end]) {
                    visit(fields);
                }
                pos = end + 1;
                if pos > bytes.len() {
                    return;
                }
                continue;
            }
            (
                n1 + 64,
                first.comma as u128 | ((next.comma as u128) << 64),
                first.quote as u128 | ((next.quote as u128) << 64),
            )
        };
        let mask = (1u128 << length) - 1;
        if let Some(fields) =
            fields_from_masks(&bytes[pos..pos + length], commas & mask, quotes & mask)
        {
            visit(fields);
        }
        pos += length + 1;
    }
    scalar_rows(&bytes[pos..], visit);
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2")]
unsafe fn avx2_rows<'a, const N: usize>(bytes: &'a [u8], visit: impl FnMut([&'a [u8]; N])) {
    vector_rows(bytes, visit, |ptr| unsafe { avx2_block(ptr) });
}

/// Decode whole fields from each structurally valid row. No data is copied.
pub fn rows<'a, const N: usize>(bytes: &'a [u8], visit: impl FnMut([&'a [u8]; N])) {
    #[cfg(target_arch = "x86_64")]
    {
        if std::is_x86_feature_detected!("avx512bw") {
            return vector_rows(bytes, visit, avx512_block);
        }
        if std::is_x86_feature_detected!("avx2") {
            return unsafe { avx2_rows(bytes, visit) };
        }
    }
    scalar_rows(bytes, visit);
}

pub fn decimal(bytes: &[u8], max: u32) -> Option<u32> {
    if bytes.is_empty() {
        return None;
    }
    let mut value = 0u32;
    for &byte in bytes {
        let digit = byte.wrapping_sub(b'0');
        if digit > 9 {
            return None;
        }
        value = value.checked_mul(10)?.checked_add(u32::from(digit))?;
    }
    (value <= max).then_some(value)
}

#[inline]
fn two(bytes: &[u8]) -> Option<i32> {
    let tens = bytes[0].wrapping_sub(b'0');
    let ones = bytes[1].wrapping_sub(b'0');
    (tens <= 9 && ones <= 9).then(|| i32::from(tens) * 10 + i32::from(ones))
}

#[inline]
fn eight_digits(raw: u64) -> Option<u32> {
    if raw & 0xf0f0_f0f0_f0f0_f0f0 != 0x3030_3030_3030_3030
        || raw.wrapping_add(0x0606_0606_0606_0606) & 0x4040_4040_4040_4040 != 0
    {
        return None;
    }
    let digits = raw & 0x0f0f_0f0f_0f0f_0f0f;
    let pairs = (digits.wrapping_mul(0x0a01) >> 8) & 0x00ff_00ff_00ff_00ff;
    let quads = (pairs.wrapping_mul(0x0064_0001) >> 16) & 0x0000_ffff_0000_ffff;
    Some((quads.wrapping_mul(0x0000_2710_0000_0001) >> 32) as u32)
}

pub(crate) fn milliseconds(bytes: &[u8]) -> Option<i64> {
    if bytes.len() == 13 {
        let head = eight_digits(u64::from_le_bytes(bytes[..8].try_into().ok()?))?;
        let tail = eight_digits(u64::from_le_bytes(bytes[5..].try_into().ok()?))? % 100_000;
        return Some(i64::from(head) * 100_000 + i64::from(tail));
    }
    if bytes.is_empty() {
        return None;
    }
    let mut result = 0i64;
    for &byte in bytes {
        let digit = byte.wrapping_sub(b'0');
        if digit > 9 {
            return None;
        }
        result = result.checked_mul(10)?.checked_add(i64::from(digit))?;
    }
    Some(result)
}

#[inline]
fn time_within_day(bytes: &[u8]) -> Option<i64> {
    let raw = u64::from_le_bytes(bytes[11..19].try_into().ok()?);
    let separators = 0x0000_ff00_00ff_0000;
    if raw & separators != 0x0000_3a00_003a_0000 {
        return None;
    }
    let time = raw & !separators;
    if time & 0xf0f0_f0f0_f0f0_f0f0 != 0x3030_0030_3000_3030
        || time.wrapping_add(0x0606_0006_0600_0606) & 0x4040_4040_4040_4040 != 0
    {
        return None;
    }
    let digits = time & 0x0f0f_0f0f_0f0f_0f0f;
    let pairs = digits.wrapping_mul(0x0a01) >> 8;
    let hour = pairs & 0xff;
    let minute = pairs >> 24 & 0xff;
    let second = pairs >> 48 & 0xff;
    if hour > 23 || minute > 59 || second > 59 {
        return None;
    }
    Some((hour * 3600 + minute * 60 + second) as i64)
}

#[derive(Clone, Copy)]
struct Date {
    year_month: u64,
    day: u16,
    midnight: i64,
}

pub struct Clock {
    days: [Date; 32],
}

impl Clock {
    pub fn new() -> Self {
        Self {
            days: [Date {
                year_month: 0,
                day: 0,
                midnight: 0,
            }; 32],
        }
    }

    #[inline]
    pub(crate) fn midnight(&mut self, bytes: &[u8]) -> Option<i64> {
        let year_month = u64::from_le_bytes(bytes[..8].try_into().ok()?);
        let day = u16::from_le_bytes([bytes[8], bytes[9]]);
        let slot = (usize::from(bytes[8]) * 10 + usize::from(bytes[9])) & 31;
        let cached = &mut self.days[slot];
        if cached.year_month == year_month && cached.day == day && year_month != 0 {
            return Some(cached.midnight);
        }
        let midnight = parse_date(bytes)?;
        *cached = Date {
            year_month,
            day,
            midnight,
        };
        Some(midnight)
    }

    #[inline]
    pub fn timestamp(&mut self, bytes: &[u8]) -> Option<i64> {
        let offset = match bytes.len() {
            20 if bytes[19] == b'Z' => 0,
            25 if matches!(bytes[19], b'+' | b'-') && bytes[22] == b':' => {
                let hours = two(&bytes[20..22])?;
                let minutes = two(&bytes[23..25])?;
                if hours > 23 || minutes > 59 {
                    return None;
                }
                let minutes = hours * 60 + minutes;
                if bytes[19] == b'-' {
                    -minutes
                } else {
                    minutes
                }
            }
            _ => return milliseconds(bytes),
        };
        if bytes[10] != b'T' {
            return None;
        }
        let day = self.midnight(bytes)?;
        let clock = time_within_day(bytes)?;
        Some(day + (clock - i64::from(offset) * 60) * 1000)
    }
}

pub fn timestamp(bytes: &[u8]) -> Option<i64> {
    Clock::new().timestamp(bytes)
}

fn parse_date(bytes: &[u8]) -> Option<i64> {
    if bytes[4] != b'-' || bytes[7] != b'-' {
        return None;
    }
    let year = two(&bytes[..2])? * 100 + two(&bytes[2..4])?;
    let month = two(&bytes[5..7])?;
    let day = two(&bytes[8..10])?;
    let limit = match month {
        4 | 6 | 9 | 11 => 30,
        2 if year % 4 == 0 && (year % 100 != 0 || year % 400 == 0) => 29,
        2 => 28,
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        _ => return None,
    };
    if day == 0 || day > limit {
        return None;
    }

    // Count days in the proleptic Gregorian calendar from a March-based
    // year so the leap day sits at the end of the 400-year cycle.
    let adjusted = year - i32::from(month <= 2);
    let era = adjusted.div_euclid(400);
    let year_of_era = adjusted.rem_euclid(400);
    let month_of_year = if month <= 2 { month + 9 } else { month - 3 };
    let day_of_year = (153 * month_of_year + 2) / 5 + day - 1;
    let day_of_era = year_of_era * 365 + year_of_era / 4 - year_of_era / 100 + day_of_year;
    let day = era * 146_097 + day_of_era - 719_468;
    Some(i64::from(day) * 86_400_000)
}

/// Lazily projects the event fields from compact delimiter offsets. Oversized
/// rows use an ordinary exact view whose borrowed fields point into the input.
pub struct EventRow<'a, 'row>(EventFields<'a, 'row>);

enum EventFields<'a, 'row> {
    Short {
        bytes: &'a [u8],
        cuts: u64,
        quoted: u8,
    },
    Wide(&'row [&'a [u8]; 7]),
}

#[derive(Clone, Copy)]
pub(crate) struct ShortEvent<'a> {
    bytes: &'a [u8],
    cuts: u64,
    quoted: u8,
}

impl<'a> ShortEvent<'a> {
    #[inline(always)]
    pub(crate) fn field<const INDEX: usize>(self) -> &'a [u8] {
        let begin = if INDEX == 0 {
            0
        } else {
            ((self.cuts >> ((INDEX - 1) * 8)) & 255) as usize + 1
        };
        let end = ((self.cuts >> (INDEX * 8)) & 255) as usize;
        let skip = usize::from(self.quoted >> INDEX & 1);
        // The structural scanner checked that the offsets are ordered and
        // quotes occupy whole fields.
        unsafe { self.bytes.get_unchecked(begin + skip..end - skip) }
    }

    pub(crate) fn row(self) -> EventRow<'a, 'a> {
        EventRow(EventFields::Short {
            bytes: self.bytes,
            cuts: self.cuts,
            quoted: self.quoted,
        })
    }
}

impl<'a> EventRow<'a, '_> {
    #[inline(always)]
    pub(crate) fn short(&self) -> Option<ShortEvent<'a>> {
        match self.0 {
            EventFields::Short {
                bytes,
                cuts,
                quoted,
            } => Some(ShortEvent {
                bytes,
                cuts,
                quoted,
            }),
            EventFields::Wide(_) => None,
        }
    }

    #[inline(always)]
    pub fn field<const INDEX: usize>(&self) -> &'a [u8] {
        assert!(INDEX < 7);
        match self.0 {
            EventFields::Wide(fields) => fields[INDEX],
            EventFields::Short {
                bytes,
                cuts,
                quoted,
            } => {
                let begin = if INDEX == 0 {
                    0
                } else {
                    ((cuts >> ((INDEX - 1) * 8)) & 255) as usize + 1
                };
                let end = ((cuts >> (INDEX * 8)) & 255) as usize;
                let skip = usize::from(quoted >> INDEX & 1);
                // Packed offsets come from the ordered delimiter mask. Checked
                // quote pairs guarantee that both trims fit in their field.
                unsafe { bytes.get_unchecked(begin + skip..end - skip) }
            }
        }
    }
}

#[cfg(all(target_arch = "x86_64", target_feature = "bmi2"))]
#[inline(always)]
fn event_fields(bytes: &[u8], first: Block, second: Block) -> Option<EventRow<'_, '_>> {
    use std::arch::x86_64::_pext_u64;

    let length = bytes.len();
    if length == 0 {
        return None;
    }
    let low_mask = |n| if n >= 64 { u64::MAX } else { (1u64 << n) - 1 };
    let a = low_mask(length);
    let b = low_mask(length.saturating_sub(64));
    let mut c0 = first.comma & a;
    let mut c1 = second.comma & b;
    let n0 = c0.count_ones();
    if n0 + c1.count_ones() != 6 {
        return None;
    }

    let q0 = first.quote & a;
    let q1 = second.quote & b;
    let quoted = if q0 | q1 == 0 {
        0
    } else {
        let end0 = if length <= 64 {
            1u64 << (length - 1)
        } else {
            0
        };
        let end1 = if length > 64 {
            1u64 << (length - 65)
        } else {
            0
        };
        let start_mask0 = (c0 << 1) | 1;
        let start_mask1 = (c1 << 1) | (c0 >> 63);
        let close_mask0 = (c0 >> 1) | (c1 << 63) | end0;
        let close_mask1 = (c1 >> 1) | end1;
        if (q0 & !(start_mask0 | close_mask0)) | (q1 & !(start_mask1 | close_mask1)) != 0 {
            return None;
        }

        let (open0, open1, close0, close1) = unsafe {
            (
                _pext_u64((q0 >> 1) | (q1 << 63), c0),
                _pext_u64(q1 >> 1, c1),
                _pext_u64(q0 << 1, c0),
                _pext_u64((q1 << 1) | (q0 >> 63), c1),
            )
        };
        let begins = ((open0 | (open1 << n0)) << 1) | (q0 & 1);
        let final_end = (q0 & end0) | (q1 & end1) != 0;
        let ends = close0 | (close1 << n0) | (u64::from(final_end) << 6);
        // An isolated quote in a one-byte field can match both positions.
        // Count the actual quote bytes so this malformed case is rejected.
        if begins != ends || q0.count_ones() + q1.count_ones() != 2 * begins.count_ones() {
            return None;
        }
        begins as u8
    };

    #[inline(always)]
    fn pull(mask: &mut u64) -> u64 {
        let i = u64::from(mask.trailing_zeros());
        *mask &= mask.wrapping_sub(1);
        i
    }
    #[inline(always)]
    fn bulk<const N: usize>(mut mask: u64) -> u64 {
        let mut cuts = 0;
        for i in 0..N {
            cuts |= pull(&mut mask) << (8 * i);
        }
        cuts
    }

    let cuts = if n0 >= 3 {
        // Compute each half's offsets independently. Only the count of the
        // low-half delimiters is needed to stitch the two packed fields.
        let low = bulk::<6>(c0) & ((1u64 << (8 * n0)) - 1);
        let high = (bulk::<3>(c1) | 0x0040_4040) << (8 * n0);
        (low | high) & 0xffff_ffff_ffff | ((length as u64) << 48)
    } else {
        let mut cuts = (length as u64) << 48;
        for i in 0..6 {
            let end = if c0 != 0 {
                pull(&mut c0)
            } else {
                pull(&mut c1) + 64
            };
            cuts |= end << (8 * i);
        }
        cuts
    };
    Some(EventRow(EventFields::Short {
        bytes,
        cuts,
        quoted,
    }))
}

#[cfg(all(target_arch = "x86_64", target_feature = "bmi2"))]
#[inline(always)]
fn vector_event_rows<'a>(
    bytes: &'a [u8],
    mut visit: impl FnMut(EventRow<'a, '_>),
    classify: impl Fn(*const u8) -> Block,
) {
    let mut pos = 0;
    while bytes.len() - pos >= 128 {
        let first = classify(unsafe { bytes.as_ptr().add(pos) });
        let n0 = first.newline.trailing_zeros() as usize;
        let (length, second) = if n0 < 64 {
            (n0, Block::default())
        } else {
            let next = classify(unsafe { bytes.as_ptr().add(pos + 64) });
            let n1 = next.newline.trailing_zeros() as usize;
            if n1 >= 64 {
                let end = find(&bytes[pos + 128..], b'\n').map_or(bytes.len(), |n| pos + 128 + n);
                if let Some(fields) = scalar_fields(&bytes[pos..end]) {
                    visit(EventRow(EventFields::Wide(&fields)));
                }
                if end == bytes.len() {
                    return;
                }
                pos = end + 1;
                continue;
            }
            (n1 + 64, next)
        };
        if let Some(row) = event_fields(&bytes[pos..pos + length], first, second) {
            visit(row);
        }
        pos += length + 1;
    }
    scalar_rows(&bytes[pos..], |fields| {
        visit(EventRow(EventFields::Wide(&fields)))
    });
}

#[cfg(all(target_arch = "x86_64", target_feature = "bmi2"))]
#[target_feature(enable = "avx2")]
unsafe fn avx2_event_rows<'a>(bytes: &'a [u8], visit: impl FnMut(EventRow<'a, '_>)) {
    vector_event_rows(bytes, visit, |ptr| unsafe { avx2_block(ptr) });
}

pub fn event_rows<'a>(bytes: &'a [u8], mut visit: impl FnMut(EventRow<'a, '_>)) {
    #[cfg(all(target_arch = "x86_64", target_feature = "bmi2"))]
    {
        if std::is_x86_feature_detected!("avx512bw") {
            return vector_event_rows(bytes, visit, avx512_block);
        }
        if std::is_x86_feature_detected!("avx2") {
            return unsafe { avx2_event_rows(bytes, visit) };
        }
    }
    scalar_rows(bytes, |fields| visit(EventRow(EventFields::Wide(&fields))));
}
