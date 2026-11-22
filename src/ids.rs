//! Exact 64-bit keys for identifiers.
//!
//! An ID of 1 to 10 bytes drawn from `0-9A-Za-z` packs into a "short" key:
//! six bits per byte, first byte highest, zero padding. Integer order of
//! short keys is byte-wise order of the strings. Any other non-empty ID gets
//! a "long" key: bit 63 set and a 63-bit hash; its bytes are kept so that
//! comparisons can fall back to the bytes and so that hash collisions are
//! detected (which sends the run to the exact generic pipeline).

pub const LONG: u64 = 1 << 63;

/// Code for each byte: 1..=62 for `0-9A-Za-z`, 0 otherwise.
pub static CODE: [u8; 256] = {
    let mut t = [0u8; 256];
    let mut i = 0;
    while i < 10 {
        t[b'0' as usize + i] = 1 + i as u8;
        i += 1;
    }
    i = 0;
    while i < 26 {
        t[b'A' as usize + i] = 11 + i as u8;
        t[b'a' as usize + i] = 37 + i as u8;
        i += 1;
    }
    t
};

static CHAR: [u8; 64] = {
    let mut t = [0u8; 64];
    let mut i = 0;
    while i < 10 {
        t[1 + i] = b'0' + i as u8;
        i += 1;
    }
    i = 0;
    while i < 26 {
        t[11 + i] = b'A' + i as u8;
        t[37 + i] = b'a' + i as u8;
        i += 1;
    }
    t
};

#[inline(always)]
pub fn hash_bytes(b: &[u8]) -> u64 {
    // FNV-1a then a strong finalizer; only used for rare long IDs.
    let mut h: u64 = 0xcbf2_9ce4_8422_2325;
    for &x in b {
        h ^= x as u64;
        h = h.wrapping_mul(0x0000_0100_0000_01b3);
    }
    h ^= b.len() as u64;
    h = (h ^ (h >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    h = (h ^ (h >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    h ^ (h >> 31)
}

/// Scalar packing. Returns 0 when the ID is not short.
#[inline(always)]
pub fn pack_scalar(b: &[u8]) -> u64 {
    if b.is_empty() || b.len() > 10 {
        return 0;
    }
    let mut k = 0u64;
    for &x in b {
        let c = CODE[x as usize];
        if c == 0 {
            return 0;
        }
        k = (k << 6) | c as u64;
    }
    k << (6 * (10 - b.len()))
}

/// SIMD packing of `len` bytes at `p`; reads 16 bytes. Returns 0 when not short.
#[cfg(target_feature = "avx2")]
#[inline(always)]
pub unsafe fn pack16(p: *const u8, len: usize) -> u64 {
    use std::arch::x86_64::*;
    if len.wrapping_sub(1) >= 10 {
        return 0;
    }
    let v = _mm_loadu_si128(p as *const __m128i);
    let idx = _mm_setr_epi8(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15);
    let inlen = _mm_cmpgt_epi8(_mm_set1_epi8(len as i8), idx);
    let hi = _mm_and_si128(_mm_srli_epi16(v, 4), _mm_set1_epi8(0x0f));
    let lo = _mm_and_si128(v, _mm_set1_epi8(0x0f));
    // Offsets by high nibble: 3 -> -47, 4/5 -> -54, 6/7 -> -60.
    let off = _mm_shuffle_epi8(
        _mm_setr_epi8(0, 0, 0, -47, -54, -54, -60, -60, 0, 0, 0, 0, 0, 0, 0, 0),
        hi,
    );
    // Invalid classes: lo classes bit0 = {0}, bit1 = {1..9}, bit2 = {A}, bit3 = {B..F}.
    let lo_class = _mm_shuffle_epi8(
        _mm_setr_epi8(1, 2, 2, 2, 2, 2, 2, 2, 2, 2, 4, 8, 8, 8, 8, 8),
        lo,
    );
    let hi_bad = _mm_shuffle_epi8(
        _mm_setr_epi8(15, 15, 15, 12, 1, 8, 1, 8, 15, 15, 15, 15, 15, 15, 15, 15),
        hi,
    );
    let bad = _mm_and_si128(_mm_and_si128(lo_class, hi_bad), inlen);
    // Bytes >= 0x80 give hi >= 8 via the 16-bit shift only within the nibble mask, so they are bad.
    if _mm_movemask_epi8(_mm_cmpeq_epi8(bad, _mm_setzero_si128())) != 0xffff {
        return 0;
    }
    let code = _mm_and_si128(_mm_add_epi8(v, off), inlen);
    let t = _mm_maddubs_epi16(code, _mm_set1_epi16(0x0140));
    let u = _mm_madd_epi16(t, _mm_set1_epi32(0x0001_1000));
    let q0 = _mm_cvtsi128_si64(u) as u64;
    let q1 = _mm_extract_epi64::<1>(u) as u64;
    ((q0 & 0xff_ffff) << 36) | ((q0 >> 32) << 12) | ((q1 & 0xff_ffff) >> 12)
}

#[cfg(not(target_feature = "avx2"))]
#[inline(always)]
pub unsafe fn pack16(p: *const u8, len: usize) -> u64 {
    pack_scalar(std::slice::from_raw_parts(p, len))
}

/// Length of a short key's ID.
#[inline(always)]
pub fn short_len(k: u64) -> usize {
    10 - (k.trailing_zeros() as usize / 6)
}

fn short_into(k: u64, buf: &mut [u8; 10]) -> &[u8] {
    let mut n = 0;
    for i in 0..10 {
        let c = (k >> (54 - 6 * i)) & 63;
        if c == 0 {
            break;
        }
        buf[i] = CHAR[c as usize];
        n += 1;
    }
    &buf[..n]
}

/// Bytes of a short key.
pub fn short_bytes(k: u64, out: &mut Vec<u8>) {
    for i in 0..10 {
        let c = (k >> (54 - 6 * i)) & 63;
        if c == 0 {
            break;
        }
        out.push(CHAR[c as usize]);
    }
}

/// Two independent 64-bit hashes of a byte string (any length). Reads up to
/// 15 bytes past the end; callers guarantee padding.
#[inline(always)]
pub unsafe fn hash2(p: *const u8, len: usize) -> (u64, u64) {
    #[inline(always)]
    fn mum(a: u64, b: u64) -> u64 {
        let r = (a as u128) * (b as u128);
        (r as u64) ^ ((r >> 64) as u64)
    }
    const K0: u64 = 0xa076_1d64_78bd_642f;
    const K1: u64 = 0xe703_7ed1_a0b4_28db;
    const K2: u64 = 0x8ebc_6af0_9c88_c6e3;
    const K3: u64 = 0x5899_65cc_7537_4cc3;
    let mut a = K0 ^ len as u64;
    let mut b = K1.wrapping_add(len as u64);
    let mut i = 0usize;
    while i + 16 <= len {
        let x = std::ptr::read_unaligned(p.add(i) as *const u64);
        let y = std::ptr::read_unaligned(p.add(i + 8) as *const u64);
        a = mum(a ^ x, K2 ^ y);
        b = mum(b ^ y, K3 ^ x).rotate_left(23);
        i += 16;
    }
    let rest = len - i;
    if rest > 0 {
        let x = std::ptr::read_unaligned(p.add(i) as *const u64);
        let y = std::ptr::read_unaligned(p.add(i + 8) as *const u64);
        let (x, y) = if rest >= 8 {
            (x, if rest == 8 { 0 } else { y & (u64::MAX >> (8 * (16 - rest))) })
        } else {
            (x & (u64::MAX >> (8 * (8 - rest))), 0)
        };
        a = mum(a ^ x, K2 ^ y);
        b = mum(b ^ y, K3 ^ x).rotate_left(23);
    }
    (mum(a ^ K3, b ^ K1), mum(b ^ K0, a ^ K2))
}

/// Long IDs seen by one thread. Every first sighting (per a small
/// direct-mapped cache) is recorded as (key, pointer | len << 48) in the
/// partition that owns the key, so a later phase compares bytes of every
/// distinct spelling and can look bytes up by key.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct LongRec {
    pub k: u64,
    pub v: u64,
    pub h2: u64,
}

pub struct LongIds {
    cache: Vec<(u64, u64)>,
    pub parts: Vec<crate::stream::Stream>,
    pub evparts: Vec<crate::stream::Stream>,
    pool: crate::stream::Pool,
    bits: u32,
}

impl Default for LongIds {
    fn default() -> Self {
        LongIds::new(4)
    }
}

pub const LCACHE: usize = 1 << 15;
pub const LBITS: u32 = 8;

impl LongIds {
    pub fn new(bits: u32) -> LongIds {
        LongIds {
            cache: vec![(0, 0); LCACHE],
            parts: vec![crate::stream::Stream::new(); 1usize << bits],
            evparts: vec![crate::stream::Stream::new(); 1usize << bits],
            pool: crate::stream::Pool::new(16 << 20),
            bits,
        }
    }

    pub fn finish(&mut self) {
        for s in self.parts.iter_mut().chain(self.evparts.iter_mut()) {
            s.finish();
        }
    }

    #[inline(always)]
    unsafe fn record(&mut self, k: u64, h2: u64, p: *const u8, len: usize) {
        let part = (k.wrapping_mul(0x9E37_79B9_7F4A_7C15) >> (64 - self.bits)) as usize;
        let rec = LongRec { k, v: (p as u64) | ((len as u64) << 48), h2 };
        self.parts.get_unchecked_mut(part).push(&mut self.pool, rec);
    }

    /// A long ID that usually occurs once and whose bytes are needed later
    /// (click IDs).
    #[inline(never)]
    pub unsafe fn key(&mut self, p: *const u8, len: usize) -> u64 {
        let (h1, h2) = hash2(p, len);
        let k = LONG | (h1 >> 1);
        self.record(k, h2, p, len);
        k
    }

    /// A long event ID: recorded only for the shared-key check.
    #[inline(never)]
    pub unsafe fn key_event(&mut self, p: *const u8, len: usize) -> u64 {
        let (h1, h2) = hash2(p, len);
        let k = LONG | (h1 >> 1);
        let part = (k.wrapping_mul(0x9E37_79B9_7F4A_7C15) >> (64 - self.bits)) as usize;
        let rec = LongRec { k, v: 0, h2 };
        self.evparts.get_unchecked_mut(part).push(&mut self.pool, rec);
        k
    }

    /// A long ID that repeats (user IDs): a small cache skips repeats.
    #[inline(never)]
    pub unsafe fn key_cached(&mut self, p: *const u8, len: usize) -> u64 {
        let (h1, h2) = hash2(p, len);
        let k = LONG | (h1 >> 1);
        let slot = (h2 as usize) & (LCACHE - 1);
        let c = self.cache.get_unchecked_mut(slot);
        if c.0 != k || c.1 != h2 {
            *c = (k, h2);
            self.record(k, h2, p, len);
        }
        k
    }
}

/// The key for a non-empty ID that usually occurs once.
#[inline(always)]
pub unsafe fn id_key(p: *const u8, len: usize, longs: &mut LongIds) -> u64 {
    let k = pack16(p, len);
    if k != 0 {
        k
    } else {
        longs.key(p, len)
    }
}

/// The key for a non-empty event ID.
#[inline(always)]
pub unsafe fn event_key(p: *const u8, len: usize, longs: &mut LongIds) -> u64 {
    let k = pack16(p, len);
    if k != 0 {
        k
    } else {
        longs.key_event(p, len)
    }
}

/// True when no event key is shared by two spellings (by second hash).
/// `scratch` is reused across calls; entries are (key, h2, generation).
pub fn check_events(recs: &[&crate::stream::Stream], scratch: &mut Vec<(u64, u64)>, gens: &mut Vec<u32>, gen: u32) -> bool {
    let n: usize = recs.iter().map(|r| r.count).sum();
    let cap = (n * 2).max(16).next_power_of_two();
    if scratch.len() < cap {
        *scratch = vec![(0, 0); cap];
        *gens = vec![0; cap];
    }
    let mask = cap - 1;
    let mut ok = true;
    for r in recs {
        r.for_each_block::<LongRec>(|b| {
            for &LongRec { k, h2, .. } in b {
                let mut h = (k.wrapping_mul(0xC4CE_B9FE_1A85_EC53) >> 20) as usize & mask;
                loop {
                    let g = unsafe { gens.get_unchecked_mut(h) };
                    let s = unsafe { scratch.get_unchecked_mut(h) };
                    if *g != gen {
                        *g = gen;
                        *s = (k, h2);
                        break;
                    }
                    if s.0 == k {
                        ok &= s.1 == h2;
                        break;
                    }
                    h = (h + 1) & mask;
                }
            }
        });
    }
    ok
}

/// The key for a non-empty user ID.
#[inline(always)]
pub unsafe fn user_key(p: *const u8, len: usize, longs: &mut LongIds) -> u64 {
    let k = pack16(p, len);
    if k != 0 {
        k
    } else {
        longs.key_cached(p, len)
    }
}

#[inline(always)]
unsafe fn span(v: u64) -> &'static [u8] {
    std::slice::from_raw_parts((v & ((1 << 48) - 1)) as *const u8, (v >> 48) as usize)
}

/// One partition of all long IDs: key -> bytes.
#[derive(Default)]
pub struct LongPart {
    slots: Vec<(u64, u64)>,
    bytes: Vec<u8>,
}

impl LongPart {
    /// Build from all threads' records; false on a key shared by two spellings.
    /// Keys are 63 bits of one hash; a second independent 64-bit hash is
    /// kept per spelling, and two spellings with one key but different
    /// second hashes are reported (the caller then runs the exact generic
    /// pipeline). Bytes of the first spelling are kept for lookups.
    pub fn build(recs: &[&crate::stream::Stream]) -> (LongPart, bool) {
        let n: usize = recs.iter().map(|r| r.count).sum();
        let cap = (n * 2).max(4).next_power_of_two();
        let mut slots = vec![(0u64, 0u64); cap];
        let mut check = vec![0u64; cap];
        let mut ok = true;
        for r in recs {
            r.for_each_block::<LongRec>(|b| {
                for &LongRec { k, v, h2 } in b {
                    let mut h = (k.wrapping_mul(0xC4CE_B9FE_1A85_EC53) >> 20) as usize & (cap - 1);
                    loop {
                        let s = unsafe { slots.get_unchecked_mut(h) };
                        if s.0 == 0 {
                            *s = (k, v);
                            check[h] = h2;
                            break;
                        }
                        if s.0 == k {
                            if check[h] != h2 {
                                ok = false;
                            }
                            break;
                        }
                        h = (h + 1) & (cap - 1);
                    }
                }
            });
        }
        // Own the bytes so the input mapping can be released later.
        let total: usize = slots.iter().filter(|s| s.0 != 0).map(|s| (s.1 >> 48) as usize).sum();
        let mut bytes: Vec<u8> = Vec::with_capacity(total);
        let base = bytes.as_ptr() as u64;
        for s in slots.iter_mut().filter(|s| s.0 != 0) {
            let b = unsafe { span(s.1) };
            let off = bytes.len() as u64;
            bytes.extend_from_slice(b);
            s.1 = (base + off) | ((b.len() as u64) << 48);
        }
        debug_assert_eq!(bytes.as_ptr() as u64, base);
        (LongPart { slots, bytes }, ok)
    }

    fn get(&self, k: u64) -> &'static [u8] {
        let cap = self.slots.len();
        let mut h = (k.wrapping_mul(0xC4CE_B9FE_1A85_EC53) >> 20) as usize & (cap - 1);
        loop {
            let s = self.slots[h];
            if s.0 == k {
                return unsafe { span(s.1) };
            }
            assert!(s.0 != 0, "unknown long id");
            h = (h + 1) & (cap - 1);
        }
    }
}

/// All long IDs, partitioned like `LongIds::parts`.
pub struct LongTable {
    pub parts: Vec<LongPart>,
    pub bits: u32,
    pub any: bool,
}

impl LongTable {
    fn get(&self, k: u64) -> &[u8] {
        let part = (k.wrapping_mul(0x9E37_79B9_7F4A_7C15) >> (64 - self.bits)) as usize;
        self.parts[part].get(k)
    }

    pub fn len_of(&self, k: u64) -> usize {
        if k & LONG == 0 {
            short_len(k)
        } else {
            self.get(k).len()
        }
    }

    pub fn bytes(&self, k: u64, out: &mut Vec<u8>) {
        if k & LONG == 0 {
            short_bytes(k, out);
        } else {
            out.extend_from_slice(self.get(k));
        }
    }

    /// Byte-wise comparison of two keys.
    pub fn cmp(&self, a: u64, b: u64) -> std::cmp::Ordering {
        if (a | b) & LONG == 0 {
            return a.cmp(&b);
        }
        let mut xa = [0u8; 10];
        let mut xb = [0u8; 10];
        let sa: &[u8] = if a & LONG == 0 { short_into(a, &mut xa) } else { self.get(a) };
        let sb: &[u8] = if b & LONG == 0 { short_into(b, &mut xb) } else { self.get(b) };
        sa.cmp(sb)
    }

    pub fn is_empty(&self) -> bool {
        !self.any
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pack_order_and_roundtrip() {
        let ids: Vec<&[u8]> = vec![
            b"0", b"9", b"A", b"Z", b"a", b"z", b"a0", b"aZ", b"u1", b"U1", b"e42huhoug", b"zzzzzzzzzz",
            b"0000000000", b"e", b"ea", b"eb", b"u", b"u10",
        ];
        let mut keys = Vec::new();
        for id in &ids {
            let mut buf = [0u8; 32];
            buf[..id.len()].copy_from_slice(id);
            buf[id.len()..].fill(b'x');
            let k = unsafe { pack16(buf.as_ptr(), id.len()) };
            assert_eq!(k, pack_scalar(id), "{:?}", std::str::from_utf8(id));
            assert_ne!(k, 0);
            let mut out = Vec::new();
            short_bytes(k, &mut out);
            assert_eq!(&out[..], *id);
            keys.push((k, *id));
        }
        for a in &keys {
            for b in &keys {
                assert_eq!(a.0.cmp(&b.0), a.1.cmp(b.1));
            }
        }
        for bad in [&b"a b"[..], b"a\tb", b"\xffa", b"a-b", b"a_b", b"12345678901", b"\"a", b"a,"] {
            let mut buf = [0u8; 32];
            buf[..bad.len()].copy_from_slice(bad);
            assert_eq!(unsafe { pack16(buf.as_ptr(), bad.len()) }, 0, "{:?}", bad);
            assert_eq!(pack_scalar(bad), 0);
        }
        // Every single byte value.
        for x in 0..=255u8 {
            let buf = [x; 16];
            let k = unsafe { pack16(buf.as_ptr(), 1) };
            assert_eq!(k, pack_scalar(&[x]), "byte {x}");
        }
    }
}
