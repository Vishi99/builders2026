use std::cmp::Ordering;
use std::collections::HashMap;
use std::hash::{BuildHasherDefault, Hasher};
use std::sync::Mutex;

const LONG_ID: u64 = 1 << 63;

#[inline]
pub fn spread(mut bits: u64) -> u64 {
    bits ^= bits >> 32;
    bits = bits.wrapping_mul(0xd6e8_feb8_6659_fd93);
    bits ^ (bits >> 32)
}

// Input data is local. Full equality always resolves collisions.
#[derive(Default)]
pub struct KeyHasher(u64);

impl Hasher for KeyHasher {
    #[inline]
    fn finish(&self) -> u64 {
        spread(self.0)
    }

    #[inline]
    fn write_u64(&mut self, value: u64) {
        self.0 = self.0.rotate_left(19) ^ value;
    }

    #[inline]
    fn write_u32(&mut self, value: u32) {
        self.write_u64(u64::from(value));
    }

    #[inline]
    fn write_i32(&mut self, value: i32) {
        self.write_u32(value as u32);
    }

    #[inline]
    fn write_usize(&mut self, value: usize) {
        self.write_u64(value as u64);
    }

    #[inline]
    fn write(&mut self, bytes: &[u8]) {
        let mut chunks = bytes.chunks_exact(8);
        for chunk in &mut chunks {
            self.write_u64(u64::from_le_bytes(chunk.try_into().unwrap()));
        }
        let mut tail = [0; 8];
        tail[..chunks.remainder().len()].copy_from_slice(chunks.remainder());
        self.write_u64(u64::from_le_bytes(tail));
        self.write_usize(bytes.len());
    }
}

pub type FastMap<K, V> = HashMap<K, V, BuildHasherDefault<KeyHasher>>;

#[derive(Default)]
struct LongNames {
    ids: FastMap<Vec<u8>, u64>,
    names: Vec<Vec<u8>>,
}

/// Zero is reserved for the empty ID. Short IDs preserve byte order.
#[derive(Default)]
pub struct Names(Mutex<LongNames>);

impl Names {
    #[inline]
    pub fn key(&self, bytes: &[u8]) -> u64 {
        if let Some(key) = short_key(bytes) {
            return key;
        }
        self.intern(bytes)
    }

    fn intern(&self, bytes: &[u8]) -> u64 {
        let mut names = self.0.lock().unwrap();
        if let Some(&key) = names.ids.get(bytes) {
            return key;
        }
        let key = LONG_ID | names.names.len() as u64;
        let owned = bytes.to_vec();
        names.names.push(owned.clone());
        names.ids.insert(owned, key);
        key
    }

    #[inline]
    pub fn compare(&self, left: u64, right: u64) -> Ordering {
        if (left | right) & LONG_ID == 0 {
            return left.cmp(&right);
        }
        let mut first = Vec::new();
        let mut second = Vec::new();
        self.append(left, &mut first);
        self.append(right, &mut second);
        first.cmp(&second)
    }

    pub fn append(&self, id: u64, to: &mut Vec<u8>) {
        if id & LONG_ID != 0 {
            to.extend_from_slice(&self.0.lock().unwrap().names[(id ^ LONG_ID) as usize]);
            return;
        }
        for shift in (0..=56).rev().step_by(7) {
            let byte = (id >> shift) as u8 & 127;
            if byte == 0 {
                return;
            }
            to.push(byte);
        }
    }
}

#[inline]
fn short_key(bytes: &[u8]) -> Option<u64> {
    if bytes.len() > 9 {
        return None;
    }
    #[cfg(all(target_arch = "x86_64", target_feature = "bmi2"))]
    if bytes.len() >= 8 {
        let prefix = u64::from_be_bytes(bytes[..8].try_into().unwrap());
        let tail = bytes.get(8).copied().unwrap_or(0);
        let high = 0x8080_8080_8080_8080;
        if ((prefix | prefix.wrapping_sub(0x0101_0101_0101_0101)) & high) != 0
            || (bytes.len() == 9 && (tail == 0 || tail >= 128))
        {
            return None;
        }
        // SAFETY: This path is compiled only when the target supports BMI2.
        let packed = unsafe { std::arch::x86_64::_pext_u64(prefix, !high) };
        return Some((packed << 7) | u64::from(tail));
    }
    let mut result = 0u64;
    for &byte in bytes {
        if byte == 0 || byte >= 128 {
            return None;
        }
        result = (result << 7) | u64::from(byte);
    }
    Some(result << ((9 - bytes.len()) * 7))
}

/// An exact short key with a dense low-bit projection for generated IDs.
/// The length and case distinguish leading zeros and uppercase spellings.
#[inline]
pub fn ordered_event_key(bytes: &[u8], names: &Names) -> u64 {
    let (first, suffix) = bytes.split_first().unwrap();
    let number = match *first {
        b'e' => compact_number(suffix),
        b'E' => upper_number(suffix),
        _ => None,
    };
    if let Some(number) = number.filter(|&number| number < (1u64 << 40)) {
        let shape = ((suffix.len() as u64 - 1) << 1) | u64::from(*first == b'E');
        return signature_projection(number) | (shape << 40);
    }
    // The internal name is exact and the multiplier preserves uniqueness.
    // Mix the interned index so uncommon IDs use all event partitions.
    let index = names.intern(bytes) & !LONG_ID;
    let overflow_bit = 1u64 << 44;
    assert!(index < overflow_bit, "too many uncommon event IDs");
    overflow_bit | (index.wrapping_mul(0x9e37_79b9_7f4a_7c15) & (overflow_bit - 1))
}

fn upper_number(bytes: &[u8]) -> Option<u64> {
    if bytes.is_empty() || bytes.len() > 8 {
        return None;
    }
    let mut value = 0u64;
    for &byte in bytes {
        let digit = match byte {
            b'0'..=b'9' => byte - b'0',
            b'A'..=b'Z' => byte - b'A' + 10,
            _ => return None,
        };
        value = value * 36 + u64::from(digit);
    }
    Some(value)
}

#[inline]
fn signature_projection(number: u64) -> u64 {
    let value = number.wrapping_mul(0xa0_9058_81e9) & ((1u64 << 40) - 1);
    value ^ (value >> 17) ^ (value >> 34)
}

/// Used only to mark possible duplicate rows. All candidates are compared
/// using the original input ID. Collisions cost work and preserve correctness.
#[inline]
pub fn event_signature(bytes: &[u8]) -> u64 {
    if let [b'e', suffix @ ..] = bytes {
        if let Some(number) = compact_number(suffix) {
            // A reversible shuffle of the 40-bit numeric domain. Inverting
            // the generator's public final mixing step improves locality in
            // a power-of-two bitmap without recovering any generator keys.
            return signature_projection(number);
        }
    }
    let mut hash = KeyHasher::default();
    hash.write(bytes);
    hash.finish()
}

#[inline]
fn compact_number(bytes: &[u8]) -> Option<u64> {
    #[cfg(all(target_arch = "x86_64", target_feature = "avx2"))]
    if bytes.len() == 8 {
        return unsafe { numeric_eight(bytes) };
    }
    if bytes.is_empty() || bytes.len() > 8 {
        return None;
    }
    let mut value = 0u64;
    for &byte in bytes {
        let digit = match byte {
            b'0'..=b'9' => byte - b'0',
            b'a'..=b'z' => byte - b'a' + 10,
            _ => return None,
        };
        value = value * 36 + u64::from(digit);
    }
    Some(value)
}

#[cfg(all(target_arch = "x86_64", target_feature = "avx2"))]
#[inline]
unsafe fn numeric_eight(bytes: &[u8]) -> Option<u64> {
    use std::arch::x86_64::*;
    let value = _mm_cvtsi64_si128(i64::from_le_bytes(bytes.try_into().unwrap()));
    let digits = _mm_sub_epi8(value, _mm_set1_epi8(b'0' as i8));
    let letters = _mm_sub_epi8(value, _mm_set1_epi8(b'a' as i8));
    let digit = _mm_cmpeq_epi8(digits, _mm_min_epu8(digits, _mm_set1_epi8(9)));
    let letter = _mm_cmpeq_epi8(letters, _mm_min_epu8(letters, _mm_set1_epi8(25)));
    if _mm_movemask_epi8(_mm_or_si128(digit, letter)) & 255 != 255 {
        return None;
    }
    let values = _mm_blendv_epi8(_mm_add_epi8(letters, _mm_set1_epi8(10)), digits, digit);
    let pairs = _mm_maddubs_epi16(values, _mm_set1_epi16(0x0124));
    let halves = _mm_madd_epi16(pairs, _mm_set1_epi32((1 << 16) + 1296));
    Some(_mm_cvtsi128_si32(halves) as u64 * 1_679_616 + _mm_extract_epi32::<1>(halves) as u64)
}
