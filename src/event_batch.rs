#![cfg(all(
    target_arch = "x86_64",
    target_feature = "avx2",
    target_feature = "bmi2"
))]
use crate::csv::{self, Clock, EventRow, ShortEvent};
use crate::rank::Layout;
use crate::user_rank::UserLayout;

const LANES: usize = 16;
const CAPTURE_LIST: [Option<ShortEvent<'_>>; LANES] = [None; LANES];
const MIDNIGHT: u64 = u64::from_le_bytes(*b"00:00:00");
const ID_MASK: u64 = (1 << 40) - 1;
const STAMP_LIMIT: i64 = 1 << 21;
const PROJECTION_MULTIPLIER: u64 = 0xa090_5881_e9;
const RANK_MULTIPLIER: u64 = 0x3dee_2192_63;

pub(crate) enum Hit<'a, 'r> {
    Packed {
        rank: usize,
        code: u32,
        time: u32,
        stamp: u32,
    },
    Exact(EventRow<'a, 'r>),
}

pub(crate) struct Window {
    pub start: i64,
    pub end: i64,
    pub ingest_base: i64,
}

#[derive(Clone, Copy)]
enum RankKernel {
    Scalar,
    Avx2,
    Avx512,
}

impl RankKernel {
    fn from_environment() -> Self {
        let requested = std::env::var_os("J2_BATCH_RANK");
        if requested.as_deref() == Some(std::ffi::OsStr::new("avx512"))
            && std::arch::is_x86_feature_detected!("avx512f")
            && std::arch::is_x86_feature_detected!("avx512bw")
            && std::arch::is_x86_feature_detected!("avx512dq")
        {
            Self::Avx512
        } else if requested.is_some() {
            Self::Avx2
        } else {
            Self::Scalar
        }
    }
}

struct Columns<'a> {
    rows: [Option<ShortEvent<'a>>; LANES],
    count: usize,
    events: [u64; LANES],
    users: [u64; LANES],
    event_hms: [u64; LANES],
    ingest_hms: [u64; LANES],
    event_ms: [i64; LANES],
    ingest_seconds: [i64; LANES],
}

impl<'a> Columns<'a> {
    fn new() -> Self {
        Self {
            rows: CAPTURE_LIST,
            count: 0,
            events: [0; LANES],
            users: [0; LANES],
            event_hms: [MIDNIGHT; LANES],
            ingest_hms: [MIDNIGHT; LANES],
            event_ms: [0; LANES],
            ingest_seconds: [0; LANES],
        }
    }

    #[inline(always)]
    fn capture(&mut self, row: ShortEvent<'a>, clock: &mut Clock) -> Option<()> {
        let id = row.field::<0>();
        let user = row.field::<1>();
        let ingest = row.field::<3>();
        let kind = row.field::<4>();
        if id.len() != 9
            || user.len() != 9
            || id[0] != b'e'
            || user[0] != b'u'
            || id[1] == b'0'
            || user[1] == b'0'
            || ingest.len() != 20
            || ingest[10] != b'T'
            || ingest[19] != b'Z'
            || !(kind == b"view" || kind == b"cart")
            || row.field::<5>().is_empty()
        {
            return None;
        }

        let stamp = row.field::<2>();
        let ingest_date = clock.midnight(ingest)?;
        let (event_date, event_hms) = match stamp.len() {
            20 if stamp[10] == b'T' && stamp[19] == b'Z' => {
                let day = if stamp[..10] == ingest[..10] {
                    ingest_date
                } else {
                    clock.midnight(stamp)?
                };
                (day, word(&stamp[11..19]))
            }
            13 => (csv::milliseconds(stamp)?, MIDNIGHT),
            _ => return None,
        };

        let lane = self.count;
        self.rows[lane] = Some(row);
        self.events[lane] = word(&id[1..]);
        self.users[lane] = word(&user[1..]);
        self.event_hms[lane] = event_hms;
        self.ingest_hms[lane] = word(&ingest[11..19]);
        self.event_ms[lane] = event_date;
        self.ingest_seconds[lane] = ingest_date / 1000;
        self.count += 1;
        Some(())
    }

    #[inline(never)]
    #[cfg(all(target_arch = "x86_64", target_feature = "avx2"))]
    fn flush(
        &mut self,
        window: &Window,
        layout: Layout,
        user_layout: UserLayout,
        emit: &mut impl for<'r> FnMut(Hit<'a, 'r>),
        rank_kernel: RankKernel,
    ) {
        match rank_kernel {
            RankKernel::Scalar => {}
            RankKernel::Avx2 => return self.flush_vector(window, layout, user_layout, emit),
            RankKernel::Avx512 => {
                // Dispatch has checked AVX-512F, BW, and DQ support.
                return unsafe { self.flush512(window, layout, user_layout, emit) };
            }
        }
        for first in (0..self.count).step_by(4) {
            // Fixed arrays contain every full SIMD load, even for the tail.
            let (events, valid_events) = unsafe { base36x4(self.events.as_ptr().add(first)) };
            let (users, valid_users) = unsafe { base36x4(self.users.as_ptr().add(first)) };
            let (event_hms, valid_event_hms) = unsafe { hmsx4(self.event_hms.as_ptr().add(first)) };
            let (ingest_hms, valid_ingest_hms) =
                unsafe { hmsx4(self.ingest_hms.as_ptr().add(first)) };
            let valid = valid_events & valid_users & valid_event_hms & valid_ingest_hms;
            let count = (self.count - first).min(4);

            for offset in 0..count {
                let lane = first + offset;
                let valid_lane = (valid >> (offset * 8)) as u8 == 255;
                if valid_lane {
                    let event = events[offset];
                    let raw_user = users[offset];
                    if (event | raw_user) <= ID_MASK {
                        let event = crate::keys::signature_projection(event);
                        let raw_user = crate::keys::signature_projection(raw_user);
                        let seconds = self.ingest_seconds[lane] + ingest_hms[offset] as i64;
                        let stamp = seconds - window.ingest_base;
                        if (0..STAMP_LIMIT).contains(&stamp) {
                            if let Some(rank) = layout.rank_projected(event) {
                                let time = self.event_ms[lane] + event_hms[offset] as i64 * 1000;
                                if !(window.start..window.end).contains(&time)
                                    || seconds * 1000 - time > crate::DAY
                                {
                                    // A syntactically valid row still supplies its earliest
                                    // ingest and ID when validation removes its payload.
                                    emit(Hit::Packed {
                                        rank,
                                        code: 0,
                                        time: 0,
                                        stamp: stamp as u32,
                                    });
                                    continue;
                                }
                                if let Some(code) = user_layout.encode_projected(raw_user) {
                                    if code < 1 << 28 {
                                        emit(Hit::Packed {
                                            rank,
                                            code,
                                            time: (time - window.start) as u32,
                                            stamp: stamp as u32,
                                        });
                                        continue;
                                    }
                                }
                            }
                        }
                    }
                }
                emit(Hit::Exact(self.rows[lane].unwrap().row()));
            }
        }
        self.count = 0;
    }
    #[cfg(all(target_arch = "x86_64", target_feature = "avx2"))]
    #[inline(never)]
    fn flush_vector(
        &mut self,
        window: &Window,
        layout: Layout,
        user_layout: UserLayout,
        emit: &mut impl for<'r> FnMut(Hit<'a, 'r>),
    ) {
        let (event_key, rank_count) = layout.batch_parts();
        let (user_key, group_count) = user_layout.batch_parts();
        for first in (0..self.count).step_by(4) {
            let packed =
                unsafe { self.pack4(window, first, event_key, rank_count, user_key, group_count) };
            for offset in 0..(self.count - first).min(4) {
                if packed.ok & (1 << offset) != 0 {
                    emit(Hit::Packed {
                        rank: packed.ranks[offset] as usize,
                        code: packed.codes[offset] as u32,
                        time: packed.times[offset] as u32,
                        stamp: packed.stamps[offset] as u32,
                    });
                } else {
                    emit(Hit::Exact(self.rows[first + offset].unwrap().row()));
                }
            }
        }
        self.count = 0;
    }

    #[cfg(all(target_arch = "x86_64", target_feature = "avx2"))]
    #[target_feature(enable = "avx512f,avx512bw,avx512dq")]
    unsafe fn flush512(
        &mut self,
        window: &Window,
        layout: Layout,
        user_layout: UserLayout,
        emit: &mut impl for<'r> FnMut(Hit<'a, 'r>),
    ) {
        use std::arch::x86_64::*;
        let (event_key, rank_count) = layout.batch_parts();
        let (user_key, group_count) = user_layout.batch_parts();
        let word = |v| _mm512_set1_epi64(v);
        let zero = _mm512_setzero_si512();
        for first in (0..self.count).step_by(8) {
            let (event_number, event_valid) = base36x8(self.events.as_ptr().add(first));
            let (user_number, user_valid) = base36x8(self.users.as_ptr().add(first));
            let (event_clock, event_clock_valid) = hmsx8(self.event_hms.as_ptr().add(first));
            let (ingest_clock, ingest_clock_valid) = hmsx8(self.ingest_hms.as_ptr().add(first));
            let ascii_ok = event_valid & user_valid & event_clock_valid & ingest_clock_valid;
            let number_ok = _mm512_cmpeq_epi64_mask(
                _mm512_srli_epi64::<40>(_mm512_or_si512(event_number, user_number)),
                zero,
            );
            let rank = ordinals8(event_number, event_key);
            let user_ordinal = ordinals8(user_number, user_key);
            let group = _mm512_srli_epi64::<9>(user_ordinal);
            let alias = _mm512_and_si512(user_ordinal, word(511));
            let code = _mm512_add_epi64(
                _mm512_or_si512(_mm512_slli_epi64::<3>(group), alias),
                word(1),
            );
            let rank_ok = _mm512_cmplt_epi64_mask(rank, word(rank_count as i64));
            let code_ok = _mm512_cmplt_epi64_mask(group, word(group_count as i64))
                & _mm512_cmplt_epi64_mask(alias, word(8))
                & _mm512_cmplt_epi64_mask(code, word(1 << 28));

            let event_ms = _mm512_add_epi64(
                _mm512_loadu_si512(self.event_ms.as_ptr().add(first).cast()),
                _mm512_mul_epu32(event_clock, word(1000)),
            );
            let ingest_seconds = _mm512_add_epi64(
                _mm512_loadu_si512(self.ingest_seconds.as_ptr().add(first).cast()),
                ingest_clock,
            );
            let stamp = _mm512_sub_epi64(ingest_seconds, word(window.ingest_base));
            let stamp_ok = _mm512_cmpge_epi64_mask(stamp, zero)
                & _mm512_cmplt_epi64_mask(stamp, word(STAMP_LIMIT));
            let late = _mm512_sub_epi64(_mm512_mullo_epi64(ingest_seconds, word(1000)), event_ms);
            let alive = _mm512_cmpge_epi64_mask(event_ms, word(window.start))
                & _mm512_cmplt_epi64_mask(event_ms, word(window.end))
                & _mm512_cmple_epi64_mask(late, word(crate::DAY));
            let ok = ascii_ok & number_ok & rank_ok & stamp_ok & (code_ok | !alive);
            let codes: [u32; 8] =
                std::mem::transmute(_mm512_cvtepi64_epi32(_mm512_maskz_mov_epi64(alive, code)));
            let times: [u32; 8] = std::mem::transmute(_mm512_cvtepi64_epi32(
                _mm512_maskz_sub_epi64(alive, event_ms, word(window.start)),
            ));
            let ranks: [u32; 8] = std::mem::transmute(_mm512_cvtepi64_epi32(rank));
            let stamps: [u32; 8] = std::mem::transmute(_mm512_cvtepi64_epi32(stamp));
            for offset in 0..(self.count - first).min(8) {
                if ok & (1 << offset) != 0 {
                    emit(Hit::Packed {
                        rank: ranks[offset] as usize,
                        code: codes[offset],
                        time: times[offset],
                        stamp: stamps[offset],
                    });
                } else {
                    emit(Hit::Exact(self.rows[first + offset].unwrap().row()));
                }
            }
        }
        self.count = 0;
    }

    #[cfg(all(target_arch = "x86_64", target_feature = "avx2"))]
    #[inline(always)]
    unsafe fn pack4(
        &self,
        window: &Window,
        first: usize,
        event_key: u64,
        rank_count: u64,
        user_key: u64,
        group_count: u64,
    ) -> Packed {
        use std::arch::x86_64::*;
        let word = |n| _mm256_set1_epi64x(n);
        let zero = _mm256_setzero_si256();
        let event_data = base36x4(self.events.as_ptr().add(first));
        let user_data = base36x4(self.users.as_ptr().add(first));
        let event_clock = hmsx4(self.event_hms.as_ptr().add(first));
        let ingest_clock = hmsx4(self.ingest_hms.as_ptr().add(first));
        let ascii = event_data.1 & user_data.1 & event_clock.1 & ingest_clock.1;
        // Collapsing one bit per lane after comparing all 8 ASCII bytes
        // retains exact validity even when an invalid lane has good arithmetic.
        let ascii_lanes = (_pext_u32(
            ((ascii & (ascii >> 4)) & ((ascii & (ascii >> 4)) >> 2))
                & (((ascii & (ascii >> 4)) & ((ascii & (ascii >> 4)) >> 2)) >> 1),
            0x0101_0101,
        )) as u32;
        let event_number: __m256i = std::mem::transmute(event_data.0);
        let user_number: __m256i = std::mem::transmute(user_data.0);
        let number_ok = _mm256_cmpeq_epi64(
            _mm256_srli_epi64::<40>(_mm256_or_si256(event_number, user_number)),
            zero,
        );
        let ranks = ordinals4(event_number, event_key);
        let user_ordinals = ordinals4(user_number, user_key);
        let groups = _mm256_srli_epi64::<9>(user_ordinals);
        let aliases = _mm256_and_si256(user_ordinals, word(511));
        let codes = _mm256_add_epi64(
            _mm256_or_si256(_mm256_slli_epi64::<3>(groups), aliases),
            word(1),
        );
        let rank_ok = _mm256_cmpgt_epi64(word(rank_count as i64), ranks);
        let user_ok = _mm256_and_si256(
            _mm256_cmpgt_epi64(word(group_count as i64), groups),
            _mm256_and_si256(
                _mm256_cmpgt_epi64(word(8), aliases),
                _mm256_cmpgt_epi64(word(1 << 28), codes),
            ),
        );
        let event_clock: __m256i = std::mem::transmute(event_clock.0);
        let ingest_clock: __m256i = std::mem::transmute(ingest_clock.0);
        let event_ms = _mm256_add_epi64(
            _mm256_loadu_si256(self.event_ms.as_ptr().add(first).cast()),
            _mm256_mul_epu32(event_clock, word(1000)),
        );
        let ingest_seconds = _mm256_add_epi64(
            _mm256_loadu_si256(self.ingest_seconds.as_ptr().add(first).cast()),
            ingest_clock,
        );
        let stamps = _mm256_sub_epi64(ingest_seconds, word(window.ingest_base));
        let stamp_ok = _mm256_and_si256(
            _mm256_cmpgt_epi64(stamps, word(-1)),
            _mm256_cmpgt_epi64(word(STAMP_LIMIT), stamps),
        );
        let arrival_ms = mul_const4(ingest_seconds, 1000);
        let event_ok = _mm256_and_si256(
            _mm256_andnot_si256(
                _mm256_cmpgt_epi64(word(window.start), event_ms),
                _mm256_cmpgt_epi64(word(window.end), event_ms),
            ),
            _mm256_cmpgt_epi64(word(crate::DAY + 1), _mm256_sub_epi64(arrival_ms, event_ms)),
        );
        let predicate = _mm256_and_si256(
            _mm256_and_si256(number_ok, rank_ok),
            _mm256_and_si256(
                stamp_ok,
                _mm256_or_si256(user_ok, _mm256_xor_si256(event_ok, word(-1))),
            ),
        );
        let times = _mm256_and_si256(_mm256_sub_epi64(event_ms, word(window.start)), event_ok);
        Packed {
            ok: ascii_lanes & (_mm256_movemask_pd(_mm256_castsi256_pd(predicate)) as u32),
            ranks: std::mem::transmute(ranks),
            codes: std::mem::transmute(_mm256_and_si256(codes, event_ok)),
            times: std::mem::transmute(times),
            stamps: std::mem::transmute(stamps),
        }
    }
}

#[inline(always)]
fn word(bytes: &[u8]) -> u64 {
    u64::from_le_bytes(bytes.try_into().unwrap())
}

#[cfg(all(target_arch = "x86_64", target_feature = "avx2"))]
pub(crate) fn scan<'a>(
    input: &'a [u8],
    window: &Window,
    layout: Layout,
    user_layout: UserLayout,
    mut emit: impl for<'r> FnMut(Hit<'a, 'r>),
) {
    let rank_kernel = RankKernel::from_environment();
    let mut columns = Columns::new();
    let mut clock = Clock::new();
    csv::event_rows(input, |row| {
        if let Some(short) = row.short() {
            if columns.capture(short, &mut clock).is_some() {
                if columns.count == LANES {
                    columns.flush(window, layout, user_layout, &mut emit, rank_kernel);
                }
                return;
            }
        }
        // Equal-ingest rows have identical contents under the problem rules.
        // A slow path may therefore complete before preceding buffered rows.
        emit(Hit::Exact(row));
    });
    columns.flush(window, layout, user_layout, &mut emit, rank_kernel);
}

#[cfg(all(target_arch = "x86_64", target_feature = "avx2"))]
#[inline(always)]
unsafe fn base36x4(src: *const u64) -> ([u64; 4], u32) {
    use std::arch::x86_64::*;
    let raw = _mm256_loadu_si256(src.cast());
    let digits = _mm256_sub_epi8(raw, _mm256_set1_epi8(b'0' as i8));
    let letters = _mm256_sub_epi8(raw, _mm256_set1_epi8(b'a' as i8));
    let digit = _mm256_cmpeq_epi8(digits, _mm256_min_epu8(digits, _mm256_set1_epi8(9)));
    let letter = _mm256_cmpeq_epi8(letters, _mm256_min_epu8(letters, _mm256_set1_epi8(25)));
    let valid = _mm256_movemask_epi8(_mm256_or_si256(digit, letter)) as u32;
    let values = _mm256_blendv_epi8(
        _mm256_add_epi8(letters, _mm256_set1_epi8(10)),
        digits,
        digit,
    );
    let pairs = _mm256_maddubs_epi16(values, _mm256_set1_epi16(0x0124));
    let halves = _mm256_madd_epi16(pairs, _mm256_set1_epi32((1 << 16) + 1296));
    let upper = _mm256_mul_epu32(halves, _mm256_set1_epi32(1_679_616));
    let numbers = _mm256_add_epi64(upper, _mm256_srli_epi64::<32>(halves));
    (std::mem::transmute(numbers), valid)
}

#[cfg(all(target_arch = "x86_64", target_feature = "avx2"))]
#[inline(always)]
unsafe fn hmsx4(src: *const u64) -> ([u64; 4], u32) {
    use std::arch::x86_64::*;
    let raw = _mm256_loadu_si256(src.cast());
    let digits = _mm256_sub_epi8(raw, _mm256_set1_epi8(b'0' as i8));
    let digit = _mm256_cmpeq_epi8(digits, _mm256_min_epu8(digits, _mm256_set1_epi8(9)));
    let colon = _mm256_cmpeq_epi8(raw, _mm256_set1_epi8(b':' as i8));
    let check = _mm256_blendv_epi8(digit, colon, _mm256_set1_epi64x(0x0000_ff00_00ff_0000));
    let select = _mm256_set_epi64x(
        0x8080_0f0e_0c0b_0908u64 as i64,
        0x8080_0706_0403_0100u64 as i64,
        0x8080_0f0e_0c0b_0908u64 as i64,
        0x8080_0706_0403_0100u64 as i64,
    );
    let ordered = _mm256_shuffle_epi8(digits, select);
    let pairs = _mm256_maddubs_epi16(ordered, _mm256_set1_epi16(0x010a));
    let hours_minutes_seconds = _mm256_set1_epi64x(0x0000_003b_003b_0017);
    let ranges = _mm256_cmpeq_epi16(pairs, _mm256_min_epu16(pairs, hours_minutes_seconds));
    let valid = _mm256_movemask_epi8(_mm256_and_si256(check, ranges)) as u32;
    let partial = _mm256_madd_epi16(pairs, _mm256_set1_epi64x(0x0000_0001_003c_0e10));
    let total = _mm256_and_si256(
        _mm256_add_epi32(partial, _mm256_srli_epi64::<32>(partial)),
        _mm256_set1_epi64x(0xffff_ffff),
    );
    (std::mem::transmute(total), valid)
}

#[cfg(all(target_arch = "x86_64", target_feature = "avx2"))]
struct Packed {
    ok: u32,
    ranks: [u64; 4],
    codes: [u64; 4],
    times: [u64; 4],
    stamps: [u64; 4],
}

#[cfg(all(target_arch = "x86_64", target_feature = "avx2"))]
#[inline(always)]
unsafe fn mul_const4(value: std::arch::x86_64::__m256i, factor: u64) -> std::arch::x86_64::__m256i {
    use std::arch::x86_64::*;
    let low = _mm256_set1_epi64x(factor as u32 as i64);
    let high = _mm256_set1_epi64x((factor >> 32) as i64);
    let low_product = _mm256_mul_epu32(value, low);
    let cross = _mm256_add_epi64(
        _mm256_mul_epu32(_mm256_srli_epi64::<32>(value), low),
        _mm256_mul_epu32(value, high),
    );
    _mm256_add_epi64(low_product, _mm256_slli_epi64::<32>(cross))
}

#[cfg(all(target_arch = "x86_64", target_feature = "avx2"))]
#[inline(always)]
unsafe fn ordinals4(numbers: std::arch::x86_64::__m256i, key: u64) -> std::arch::x86_64::__m256i {
    use std::arch::x86_64::*;
    let mask = _mm256_set1_epi64x(ID_MASK as i64);
    let value = _mm256_and_si256(mul_const4(numbers, PROJECTION_MULTIPLIER), mask);
    let projected = _mm256_xor_si256(
        _mm256_xor_si256(value, _mm256_srli_epi64::<17>(value)),
        _mm256_srli_epi64::<34>(value),
    );
    let mixed = _mm256_xor_si256(projected, _mm256_set1_epi64x(key as i64));
    _mm256_and_si256(mul_const4(mixed, RANK_MULTIPLIER), mask)
}

#[cfg(all(target_arch = "x86_64", target_feature = "avx2"))]
#[target_feature(enable = "avx512f,avx512bw,avx512dq")]
#[inline]
unsafe fn base36x8(src: *const u64) -> (std::arch::x86_64::__m512i, u8) {
    use std::arch::x86_64::*;
    let raw = _mm512_loadu_si512(src.cast());
    let digits = _mm512_sub_epi8(raw, _mm512_set1_epi8(b'0' as i8));
    let letters = _mm512_sub_epi8(raw, _mm512_set1_epi8(b'a' as i8));
    let digit = _mm512_cmple_epu8_mask(digits, _mm512_set1_epi8(9));
    let letter = _mm512_cmple_epu8_mask(letters, _mm512_set1_epi8(25));
    let values = _mm512_mask_blend_epi8(
        digit,
        _mm512_add_epi8(letters, _mm512_set1_epi8(10)),
        digits,
    );
    let pairs = _mm512_maddubs_epi16(values, _mm512_set1_epi16(0x0124));
    let halves = _mm512_madd_epi16(pairs, _mm512_set1_epi32((1 << 16) + 1296));
    let upper = _mm512_mul_epu32(halves, _mm512_set1_epi32(1_679_616));
    let numbers = _mm512_add_epi64(upper, _mm512_srli_epi64::<32>(halves));
    (numbers, compress_byte_validity(digit | letter))
}

#[cfg(all(target_arch = "x86_64", target_feature = "avx2"))]
#[target_feature(enable = "avx512f,avx512bw,avx512dq")]
#[inline]
unsafe fn hmsx8(src: *const u64) -> (std::arch::x86_64::__m512i, u8) {
    use std::arch::x86_64::*;
    let raw = _mm512_loadu_si512(src.cast());
    let digits = _mm512_sub_epi8(raw, _mm512_set1_epi8(b'0' as i8));
    let expected_colon = 0x2424_2424_2424_2424_u64;
    let digit = _mm512_cmple_epu8_mask(digits, _mm512_set1_epi8(9));
    let colon = _mm512_cmpeq_epi8_mask(raw, _mm512_set1_epi8(b':' as i8));
    let check = (digit & !expected_colon) | (colon & expected_colon);
    let lo = 0x8080_0706_0403_0100u64 as i64;
    let hi = 0x8080_0f0e_0c0b_0908u64 as i64;
    let ordered = _mm512_shuffle_epi8(digits, _mm512_set_epi64(hi, lo, hi, lo, hi, lo, hi, lo));
    let pairs = _mm512_maddubs_epi16(ordered, _mm512_set1_epi16(0x010a));
    let ranges = _mm512_cmple_epu16_mask(pairs, _mm512_set1_epi64(0x0000_003b_003b_0017));
    let bits = ranges & (ranges >> 2);
    let lane_ranges = _pext_u32(bits & (bits >> 1), 0x1111_1111) as u8;
    let partial = _mm512_madd_epi16(pairs, _mm512_set1_epi64(0x0000_0001_003c_0e10));
    let total = _mm512_and_si512(
        _mm512_add_epi32(partial, _mm512_srli_epi64::<32>(partial)),
        _mm512_set1_epi64(0xffff_ffff),
    );
    (total, compress_byte_validity(check) & lane_ranges)
}

#[cfg(all(target_arch = "x86_64", target_feature = "avx2"))]
#[inline]
unsafe fn compress_byte_validity(bits: u64) -> u8 {
    let bits = bits & (bits >> 4);
    let bits = bits & (bits >> 2);
    let bits = bits & (bits >> 1);
    std::arch::x86_64::_pext_u64(bits, 0x0101_0101_0101_0101) as u8
}

#[cfg(all(target_arch = "x86_64", target_feature = "avx2"))]
#[target_feature(enable = "avx512f,avx512bw,avx512dq")]
#[inline]
unsafe fn ordinals8(numbers: std::arch::x86_64::__m512i, key: u64) -> std::arch::x86_64::__m512i {
    use std::arch::x86_64::*;
    let mask = _mm512_set1_epi64(ID_MASK as i64);
    let value = _mm512_and_si512(
        _mm512_mullo_epi64(numbers, _mm512_set1_epi64(PROJECTION_MULTIPLIER as i64)),
        mask,
    );
    let projected = _mm512_xor_si512(
        _mm512_xor_si512(value, _mm512_srli_epi64::<17>(value)),
        _mm512_srli_epi64::<34>(value),
    );
    let mixed = _mm512_xor_si512(projected, _mm512_set1_epi64(key as i64));
    _mm512_and_si512(
        _mm512_mullo_epi64(mixed, _mm512_set1_epi64(RANK_MULTIPLIER as i64)),
        mask,
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn trusted_hms(bytes: u64) -> Option<u64> {
        let text = bytes.to_le_bytes();
        let mut stamp = *b"1970-01-01T00:00:00Z";
        stamp[11..19].copy_from_slice(&text);
        csv::timestamp(&stamp).map(|n| n as u64 / 1000)
    }

    fn read_pairs(bytes: u64) -> Option<u64> {
        let text = bytes.to_le_bytes();
        let mut value = 0u64;
        for byte in text {
            let digit = match byte {
                b'0'..=b'9' => byte - b'0',
                b'a'..=b'z' => byte - b'a' + 10,
                _ => return None,
            };
            value = value * 36 + u64::from(digit);
        }
        Some(value)
    }

    fn utc_word(time: usize) -> u64 {
        let h = time / 3600;
        let m = (time / 60) % 60;
        let s = time % 60;
        u64::from_le_bytes([
            b'0' + (h / 10) as u8,
            b'0' + (h % 10) as u8,
            b':',
            b'0' + (m / 10) as u8,
            b'0' + (m % 10) as u8,
            b':',
            b'0' + (s / 10) as u8,
            b'0' + (s % 10) as u8,
        ])
    }

    #[test]
    fn typed_parsing_preserves_ascii_and_timestamp_validity() {
        let ids: [u64; 8] = [
            *b"12345678",
            *b"abcdefgh",
            *b"ABcd0009",
            *b"09abcXYZ",
            *b"zyxw9876",
            *b"!xw81876",
            *b"zfxwa)^1",
            *b"0123456a",
        ]
        .map(u64::from_le_bytes);

        let have_512 = std::arch::is_x86_feature_detected!("avx512f")
            && std::arch::is_x86_feature_detected!("avx512bw")
            && std::arch::is_x86_feature_detected!("avx512dq");
        for group in ids.chunks_exact(4) {
            let (values, valid) = unsafe { base36x4(group.as_ptr()) };
            for lane in 0..4 {
                assert_eq!(
                    (((valid >> (lane * 8)) as u8) == 255).then_some(values[lane]),
                    read_pairs(group[lane]),
                );
            }
        }
        if have_512 {
            let (values, valid) = unsafe { base36x8(ids.as_ptr()) };
            let values: [u64; 8] = unsafe { std::mem::transmute(values) };
            for lane in 0..8 {
                assert_eq!(
                    (valid & (1 << lane) != 0).then_some(values[lane]),
                    read_pairs(ids[lane]),
                );
            }
        }

        for group in (0..86_400).step_by(8) {
            let words = std::array::from_fn::<_, 8, _>(|lane| {
                let word = utc_word(group + lane);
                match lane {
                    0..=4 => word,
                    5 => word ^ (0xff << 16),
                    6 => word ^ (0xff << 48),
                    _ => word ^ 0x0f,
                }
            });
            for part in words.chunks_exact(4) {
                let (values, valid) = unsafe { hmsx4(part.as_ptr()) };
                for lane in 0..4 {
                    assert_eq!(
                        (((valid >> (lane * 8)) as u8) == 255).then_some(values[lane]),
                        trusted_hms(part[lane]),
                    );
                }
            }
            if have_512 {
                let (values, valid) = unsafe { hmsx8(words.as_ptr()) };
                let values: [u64; 8] = unsafe { std::mem::transmute(values) };
                for lane in 0..8 {
                    assert_eq!(
                        (valid & (1 << lane) != 0).then_some(values[lane]),
                        trusted_hms(words[lane]),
                    );
                }
            }
        }
    }
}
