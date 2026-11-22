use std::cmp::Ordering;
use std::collections::HashMap;
use std::hash::{BuildHasherDefault, Hasher};
use std::sync::{Arc, Mutex};

const LONG_ID: u64 = 1 << 63;
const NAME_SHARDS: usize = 64;
const NAME_SHARD_BITS: u32 = NAME_SHARDS.trailing_zeros();

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
    ids: FastMap<Arc<[u8]>, u64>,
    names: Vec<Arc<[u8]>>,
}

/// Zero is reserved for the empty ID. Short IDs preserve byte order.
pub struct Names([Mutex<LongNames>; NAME_SHARDS]);

impl Default for Names {
    fn default() -> Self {
        Self(std::array::from_fn(|_| Mutex::default()))
    }
}

impl Names {
    #[inline]
    pub fn key(&self, bytes: &[u8]) -> u64 {
        if let Some(key) = short_key(bytes) {
            return key;
        }
        self.intern(bytes)
    }

    fn intern(&self, bytes: &[u8]) -> u64 {
        let shard = byte_hash(bytes) as usize & (NAME_SHARDS - 1);
        let mut names = self.0[shard].lock().unwrap();
        if let Some(&key) = names.ids.get(bytes) {
            return key;
        }
        let key = LONG_ID | ((names.names.len() as u64) << NAME_SHARD_BITS) | shard as u64;
        let owned = Arc::from(bytes);
        names.names.push(Arc::clone(&owned));
        names.ids.insert(owned, key);
        key
    }

    #[inline]
    pub fn compare(&self, left: u64, right: u64) -> Ordering {
        if left == right || (left | right) & LONG_ID == 0 {
            return left.cmp(&right);
        }
        let a = (left & LONG_ID == 0).then(|| unpack_short(left));
        let b = (right & LONG_ID == 0).then(|| unpack_short(right));
        let a_long = a.is_none().then(|| self.long_name(left));
        let b_long = b.is_none().then(|| self.long_name(right));
        let a = a
            .as_ref()
            .map_or_else(|| a_long.as_deref().unwrap(), |(bytes, len)| &bytes[..*len]);
        let b = b
            .as_ref()
            .map_or_else(|| b_long.as_deref().unwrap(), |(bytes, len)| &bytes[..*len]);
        a.cmp(b)
    }

    pub fn append(&self, id: u64, to: &mut Vec<u8>) {
        if id & LONG_ID != 0 {
            let (shard, index) = long_position(id);
            to.extend_from_slice(&self.0[shard].lock().unwrap().names[index]);
            return;
        }
        let (bytes, length) = unpack_short(id);
        to.extend_from_slice(&bytes[..length]);
    }

    fn long_name(&self, id: u64) -> Arc<[u8]> {
        let (shard, index) = long_position(id);
        Arc::clone(&self.0[shard].lock().unwrap().names[index])
    }
}

fn long_position(id: u64) -> (usize, usize) {
    (
        id as usize & (NAME_SHARDS - 1),
        ((id ^ LONG_ID) >> NAME_SHARD_BITS) as usize,
    )
}

fn unpack_short(id: u64) -> ([u8; 9], usize) {
    let mut bytes = [0; 9];
    for (i, byte) in bytes.iter_mut().enumerate() {
        *byte = (id >> (56 - i * 7)) as u8 & 127;
        if *byte == 0 {
            return (bytes, i);
        }
    }
    (bytes, 9)
}

#[inline]
pub(crate) fn short_key(bytes: &[u8]) -> Option<u64> {
    if bytes.len() > 9 {
        return None;
    }
    #[cfg(any(
        target_arch = "aarch64",
        all(target_arch = "x86_64", target_feature = "bmi2")
    ))]
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
        #[cfg(target_arch = "x86_64")]
        let packed = unsafe { std::arch::x86_64::_pext_u64(prefix, !high) };
        #[cfg(target_arch = "aarch64")]
        let packed = {
            // Close the top-bit gaps in groups of two, four, then eight
            // bytes. All input bytes already passed the ASCII check.
            let pairs = (prefix & 0x007f_007f_007f_007f) | ((prefix & 0x7f00_7f00_7f00_7f00) >> 1);
            let fours = (pairs & 0x0000_3fff_0000_3fff) | ((pairs & 0x3fff_0000_3fff_0000) >> 2);
            (fours & 0x0000_0000_0fff_ffff) | ((fours & 0x0fff_ffff_0000_0000) >> 4)
        };
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
    LONG_ID | (index.wrapping_mul(0x9e37_79b9_7f4a_7c15) & !LONG_ID)
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
pub(crate) fn signature_projection(number: u64) -> u64 {
    let value = number.wrapping_mul(0xa0_9058_81e9) & ((1u64 << 40) - 1);
    value ^ (value >> 17) ^ (value >> 34)
}

/// Used only to mark possible duplicate rows. All candidates are compared
/// using the original input ID. Collisions cost work and preserve correctness.
#[inline]
pub fn byte_hash(bytes: &[u8]) -> u64 {
    let mut hash = KeyHasher::default();
    hash.write(bytes);
    hash.finish()
}

#[inline]
pub(crate) fn compact_number(bytes: &[u8]) -> Option<u64> {
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

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn projected_keys_preserve_exact_identifier_spelling() {
        let names = Names::default();
        let values: &[&[u8]] = &[
            b"e0",
            b"e00",
            b"e01",
            b"e1",
            b"E1",
            b"eA",
            b"eabc",
            b"EABC",
            b"e abc",
            b"unusually-long-event",
        ];
        let keys: std::collections::HashSet<_> = values
            .iter()
            .map(|key| ordered_event_key(key, &names))
            .collect();
        assert_eq!(keys.len(), values.len());
        for name in values {
            assert_eq!(
                ordered_event_key(name, &names),
                ordered_event_key(name, &names)
            );
        }
    }
}
