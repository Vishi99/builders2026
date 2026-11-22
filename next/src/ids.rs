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

/// Long IDs seen by one thread, with their keys.
#[derive(Default)]
pub struct LongIds {
    pub list: Vec<(u64, Box<[u8]>)>,
}

impl LongIds {
    #[cold]
    #[inline(never)]
    pub fn key(&mut self, b: &[u8]) -> u64 {
        let k = LONG | (hash_bytes(b) >> 1);
        self.list.push((k, b.into()));
        k
    }
}

/// The key for a non-empty ID.
#[inline(always)]
pub unsafe fn id_key(p: *const u8, len: usize, longs: &mut LongIds) -> u64 {
    let k = pack16(p, len);
    if k != 0 {
        k
    } else {
        longs.key(std::slice::from_raw_parts(p, len))
    }
}

/// All long IDs, merged and checked for hash collisions.
pub struct LongTable {
    map: std::collections::HashMap<u64, Box<[u8]>>,
    pub collision: bool,
}

impl LongTable {
    pub fn build(parts: Vec<Vec<(u64, Box<[u8]>)>>) -> LongTable {
        let mut map: std::collections::HashMap<u64, Box<[u8]>> = std::collections::HashMap::new();
        let mut collision = false;
        for part in parts {
            for (k, b) in part {
                match map.get(&k) {
                    Some(old) => {
                        if **old != *b {
                            collision = true;
                        }
                    }
                    None => {
                        map.insert(k, b);
                    }
                }
            }
        }
        LongTable { map, collision }
    }

    pub fn len_of(&self, k: u64) -> usize {
        if k & LONG == 0 {
            short_len(k)
        } else {
            self.map[&k].len()
        }
    }

    pub fn bytes(&self, k: u64, out: &mut Vec<u8>) {
        if k & LONG == 0 {
            short_bytes(k, out);
        } else {
            out.extend_from_slice(&self.map[&k]);
        }
    }

    /// Byte-wise comparison of two keys.
    pub fn cmp(&self, a: u64, b: u64) -> std::cmp::Ordering {
        if (a | b) & LONG == 0 {
            return a.cmp(&b);
        }
        let mut x = Vec::new();
        let mut y = Vec::new();
        self.bytes(a, &mut x);
        self.bytes(b, &mut y);
        x.cmp(&y)
    }

    pub fn is_empty(&self) -> bool {
        self.map.is_empty()
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
