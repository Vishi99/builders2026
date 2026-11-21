//! Optional post-sampling layout for exact generated-form event IDs.
//! Recovery is a performance hint. Failure or an out-of-range ordinal must
//! use the normal exact key path.

use crate::csv;
use crate::keys::FastMap;

const MASK: u64 = (1 << 40) - 1;
const INVERSE_A: u64 = 0x003d_ee21_9263;

#[derive(Clone, Copy, Debug)]
pub struct Layout {
    key: u64,
    pub slots: usize,
}

impl Layout {
    #[inline]
    pub(crate) fn batch_parts(self) -> (u64, u64) {
        (self.key, self.slots as u64)
    }

    #[inline]
    pub fn rank(&self, event_id: &[u8]) -> Option<usize> {
        self.rank_projected(projection(event_id, b'e')?)
    }

    // Call only for exact canonical lowercase event IDs with a <=40-bit
    // base-36 value. Any distinct spelling belongs to the exact fallback.
    #[inline]
    pub fn rank_projected(&self, projected: u64) -> Option<usize> {
        let rank = rank(projected, self.key) as usize;
        (rank < self.slots).then_some(rank)
    }
}

#[inline]
fn rank(projected: u64, key: u64) -> u64 {
    (projected ^ key).wrapping_mul(INVERSE_A) & MASK
}

#[inline]
pub(crate) fn projection(id: &[u8], prefix: u8) -> Option<u64> {
    if id.len() < 2 || id.len() > 9 || id[0] != prefix || (id.len() > 2 && id[1] == b'0') {
        return None;
    }
    let number = crate::keys::compact_number(&id[1..])?;
    (number <= MASK).then(|| crate::keys::signature_projection(number))
}

#[cfg(all(target_arch = "x86_64", target_feature = "avx2"))]
#[inline(always)]
pub(crate) fn projected_pair(event: &[u8], user: &[u8]) -> Option<(u64, u64)> {
    use core::arch::x86_64::*;
    if event.len() != 9
        || user.len() != 9
        || event[0] != b'e'
        || user[0] != b'u'
        || event[1] == b'0'
        || user[1] == b'0'
    {
        return None;
    }
    let e = i64::from_le_bytes(event[1..].try_into().unwrap());
    let u = i64::from_le_bytes(user[1..].try_into().unwrap());
    let pair = unsafe {
        let values = _mm_set_epi64x(u, e);
        let digits = _mm_sub_epi8(values, _mm_set1_epi8(b'0' as i8));
        let letters = _mm_sub_epi8(values, _mm_set1_epi8(b'a' as i8));
        let digit = _mm_cmpeq_epi8(digits, _mm_min_epu8(digits, _mm_set1_epi8(9)));
        let letter = _mm_cmpeq_epi8(letters, _mm_min_epu8(letters, _mm_set1_epi8(25)));
        if _mm_movemask_epi8(_mm_or_si128(digit, letter)) != 65535 {
            return None;
        }
        let values = _mm_blendv_epi8(_mm_add_epi8(letters, _mm_set1_epi8(10)), digits, digit);
        let pairs = _mm_maddubs_epi16(values, _mm_set1_epi16(0x0124));
        let halves = _mm_madd_epi16(pairs, _mm_set1_epi32((1 << 16) + 1296));
        let radix = _mm_set1_epi32(1_679_616);
        _mm_add_epi64(_mm_mul_epu32(halves, radix), _mm_srli_epi64::<32>(halves))
    };
    let first = unsafe { _mm_cvtsi128_si64(pair) } as u64;
    let second = unsafe { _mm_extract_epi64::<1>(pair) } as u64;
    if (first | second) > MASK {
        return None;
    }
    Some((
        crate::keys::signature_projection(first),
        crate::keys::signature_projection(second),
    ))
}

#[cfg(not(all(target_arch = "x86_64", target_feature = "avx2")))]
#[inline(always)]
pub(crate) fn projected_pair(event: &[u8], user: &[u8]) -> Option<(u64, u64)> {
    let _ = (event, user);
    None
}

#[derive(Clone, Copy, Default)]
struct Observation {
    id: u64,
    time: u64,
}

#[derive(Default)]
struct Seen {
    count: usize,
    observations: [Observation; 4],
}

fn f1_time(bytes: &[u8]) -> Option<u64> {
    if bytes.len() != 20 || bytes[10] != b'T' || bytes[19] != b'Z' {
        return None;
    }
    let mut time = 0;
    for index in [0, 1, 2, 3, 5, 6, 8, 9, 11, 12, 14, 15, 17, 18] {
        let digit = bytes[index].wrapping_sub(b'0');
        if digit > 9 {
            return None;
        }
        time = 10 * time + u64::from(digit);
    }
    Some(time)
}

fn score(key: u64, mask: u64, pairs: &[(u64, u64)]) -> u64 {
    pairs
        .iter()
        .map(|&(first, last)| {
            let delta = (last ^ key).wrapping_sub(first ^ key);
            (delta.wrapping_mul(INVERSE_A) & mask).min(2_048)
        })
        .sum()
}

fn extend(pairs: &[(u64, u64)]) -> Vec<(u64, u64)> {
    const FIRST_BITS: usize = 16;
    const BEAM: usize = 40;
    let mut candidates = (0..1 << FIRST_BITS)
        .map(|key| (score(key, (1 << FIRST_BITS) - 1, pairs), key))
        .collect::<Vec<_>>();
    candidates.select_nth_unstable(BEAM);
    candidates.truncate(BEAM);
    for bit in FIRST_BITS..40 {
        let high = 1 << bit;
        let mask = (high << 1) - 1;
        for i in 0..BEAM {
            let key = candidates[i].1;
            candidates[i].0 = score(key, mask, pairs);
            let next = key | high;
            candidates.push((score(next, mask, pairs), next));
        }
        candidates.select_nth_unstable(BEAM);
        candidates.truncate(BEAM);
    }
    candidates
}

/// Sample one newline-aligned event range from the actual input. Candidate
/// low bits keep temporal neighbors for the same raw user in short ordinal
/// ranges. The observed ID range identifies the compact offset. Neither of
/// these properties is required for exact identifier equality.
pub fn infer(sample: &[u8], input_bytes: usize) -> Option<Layout> {
    const PAIRS: usize = 48;
    const ROWS: usize = 24_000;
    const SAMPLE_BYTES: usize = 3 << 20;
    if input_bytes < 2 << 20 {
        return None;
    }
    let prefix = &sample[..sample.len().min(SAMPLE_BYTES)];
    let prefix = match prefix.iter().rposition(|&b| b == b'\n') {
        Some(last) => &prefix[..=last],
        None => return None,
    };

    let mut users = FastMap::<u64, Seen>::default();
    let mut ids = Vec::with_capacity(ROWS);
    let mut rows = 0;
    csv::event_rows(prefix, |row| {
        rows += 1;
        if rows > ROWS {
            return;
        }
        let Some(id) = projection(row.field::<0>(), b'e') else {
            return;
        };
        ids.push(id);
        let Some(user) = projection(row.field::<1>(), b'u') else {
            return;
        };
        let Some(time) = f1_time(row.field::<2>()) else {
            return;
        };
        let user = users.entry(user).or_default();
        if user.count < user.observations.len() {
            user.observations[user.count] = Observation { id, time };
        }
        user.count += 1;
    });

    let mut pairs = Vec::with_capacity(3 * PAIRS);
    for (_, mut user) in users {
        if !(2..=4).contains(&user.count) {
            continue;
        }
        let observations = &mut user.observations[..user.count];
        observations.sort_unstable_by_key(|item| item.time);
        if let Some(pair) = observations
            .windows(2)
            .find(|pair| pair[0].id != pair[1].id && pair[0].time < pair[1].time)
        {
            pairs.push((pair[0].id, pair[1].id));
            if pairs.len() == pairs.capacity() {
                break;
            }
        }
    }

    // Actual dense ranks are widely distributed in a shuffled sample. If
    // every candidate maps the sample outside a plausible space, do not
    // allocate a speculative dense buffer.
    let bound = (input_bytes / 32).min(u32::MAX as usize);
    let mut best = (ids.len(), 0u64);
    for ensemble in pairs.chunks_exact(PAIRS) {
        for (_, key) in extend(ensemble) {
            let mut outside = 0;
            for &id in &ids {
                if rank(id, key) >= bound as u64 {
                    outside += 1;
                    if outside >= best.0 {
                        break;
                    }
                }
            }
            if outside < best.0 {
                best = (outside, key);
            }
        }
    }
    if best.0 > ids.len() / 20 {
        return None;
    }
    let mut observed = ids
        .iter()
        .map(|&id| rank(id, best.1) as usize)
        .filter(|&rank| rank < bound)
        .collect::<Vec<_>>();
    if observed.is_empty() {
        return None;
    }
    // Trim the highest 0.5% of observed ranks so isolated valid IDs in
    // unusual ranges cannot cause speculative over-allocation.
    let cutoff = observed.len() - 1 - observed.len() / 200;
    let (_, maximum, _) = observed.select_nth_unstable(cutoff);
    let observed = *maximum + 1;
    let slots = observed.saturating_add((observed / 32).max(512)).min(bound);
    Some(Layout { key: best.1, slots })
}
