//! Optional exact compact representation of common lowercase user aliases.

use crate::csv;
use crate::keys::Names;
use crate::rank::projection;

const MASK: u64 = (1 << 40) - 1;
const A: u64 = 0x9e37_79b9_7f4b;
const B: u64 = 0xd6e8_feb8_6659;
const INVERSE_A: u64 = 0x3dee_2192_63;
const ALIAS_BITS: u32 = 9;
const CODE_ALIASES: u64 = 8;

const fn digit(value: usize) -> u8 {
    if value < 10 {
        b'0' + value as u8
    } else {
        b'a' + (value - 10) as u8
    }
}

const fn digit_pairs() -> [[u8; 2]; 1296] {
    let mut pairs = [[0; 2]; 1296];
    let mut n = 0;
    while n < pairs.len() {
        pairs[n] = [digit(n / 36), digit(n % 36)];
        n += 1;
    }
    pairs
}

static PAIRS: [[u8; 2]; 1296] = digit_pairs();

#[derive(Clone, Copy, Debug)]
pub struct UserLayout {
    key: u64,
    pub groups: usize,
}

impl UserLayout {
    #[inline]
    pub(crate) fn batch_parts(self) -> (u64, u64) {
        (self.key, self.groups as u64)
    }

    #[inline]
    fn ordinal(&self, projected: u64) -> u64 {
        ordinal(projected, self.key)
    }

    /// Return a compact code for an exact canonical lowercase ID. Distinct
    /// accepted ID strings always have distinct codes. Reserve zero for the
    /// caller's empty sentinel. Send every other ID through the exact path.
    #[inline]
    pub fn encode(&self, id: &[u8]) -> Option<u32> {
        self.encode_projected(projection(id, b'u')?)
    }

    /// Call only for an exact lowercase user ID with a <=40-bit base-36
    /// value and without redundant leading zeros.
    #[inline]
    pub fn encode_projected(&self, projected: u64) -> Option<u32> {
        let ordinal = self.ordinal(projected);
        let group = ordinal >> ALIAS_BITS;
        let alias = ordinal & ((1 << ALIAS_BITS) - 1);
        if group >= self.groups as u64 || alias >= CODE_ALIASES {
            return None;
        }
        Some((group * CODE_ALIASES + alias + 1) as u32)
    }

    /// Invert a code returned from this layout using the ordinary exact
    /// name encoding. No per-row or per-code lookup map is needed.
    #[inline]
    pub fn name(&self, code: u32, names: &Names) -> u64 {
        let mut bytes = [0u8; 9];
        names.key(self.spelling(code, &mut bytes))
    }

    pub fn append(&self, code: u32, out: &mut Vec<u8>) {
        let mut bytes = [0u8; 9];
        out.extend_from_slice(self.spelling(code, &mut bytes));
    }

    fn spelling<'b>(&self, code: u32, bytes: &'b mut [u8; 9]) -> &'b [u8] {
        let mut number = self.number(code) as usize;
        let mut index = bytes.len();
        loop {
            index -= 2;
            bytes[index..index + 2].copy_from_slice(&PAIRS[number % 1296]);
            number /= 1296;
            if number == 0 {
                break;
            }
        }
        index += usize::from(bytes[index] == b'0');
        index -= 1;
        bytes[index] = b'u';
        &bytes[index..]
    }
    #[inline]
    fn number(&self, code: u32) -> u64 {
        let index = u64::from(code - 1);
        let ordinal = ((index / CODE_ALIASES) << ALIAS_BITS) | (index % CODE_ALIASES);
        let mixed = (ordinal.wrapping_mul(A) ^ self.key) & MASK;
        (mixed ^ (mixed >> 17)).wrapping_mul(B) & MASK
    }

    /// The base-36 ASCII lexical order without formatting the complete name.
    #[inline]
    pub(crate) fn lexical(&self, code: u32) -> u64 {
        const SEVEN: u64 = 78_364_164_096; // 36^7
        let mut left_aligned = self.number(code);
        let mut width = 8;
        while left_aligned < SEVEN && width > 1 {
            left_aligned *= 36;
            width -= 1;
        }
        (left_aligned << 4) | width
    }
}

#[inline]
fn ordinal(projected: u64, key: u64) -> u64 {
    (projected ^ key).wrapping_mul(INVERSE_A) & MASK
}

fn score(key: u64, mask: u64, edges: &[(u64, u64)]) -> u64 {
    edges
        .iter()
        .map(|&(left, right)| {
            let a = ordinal(left, key) & mask;
            let b = ordinal(right, key) & mask;
            if a >> ALIAS_BITS == b >> ALIAS_BITS {
                a.abs_diff(b)
            } else {
                1024
            }
        })
        .sum()
}

fn lift(edges: &[(u64, u64)]) -> Vec<(u64, u64)> {
    const BITS: usize = 16;
    const BEAM: usize = 80;
    let mut candidates = (0..1 << BITS)
        .map(|key| (score(key, (1 << BITS) - 1, edges), key))
        .collect::<Vec<_>>();
    candidates.select_nth_unstable(BEAM);
    candidates.truncate(BEAM);
    for bit in BITS..40 {
        let high = 1 << bit;
        let mask = (high << 1) - 1;
        for i in 0..BEAM {
            let key = candidates[i].1;
            candidates[i].0 = score(key, mask, edges);
            let next = key | high;
            candidates.push((score(next, mask, edges), next));
        }
        candidates.select_nth_unstable(BEAM);
        candidates.truncate(BEAM);
    }
    candidates
}

/// Infer a layout from a newline-aligned range of actual parsed links. The
/// link disjoint-set remains authoritative; ordinal groups do not establish
/// person identity. The ceiling only bounds supplemental dense storage.
pub fn infer(sample: &[u8], event_input_bytes: usize) -> Option<UserLayout> {
    const ENSEMBLES: usize = 3;
    const PAIRS: usize = 48;
    const LINKS: usize = 4096;
    const SAMPLE_BYTES: usize = 256 << 10;
    if event_input_bytes < 2 << 20 {
        return None;
    }
    let prefix = &sample[..sample.len().min(SAMPLE_BYTES)];
    let prefix = &prefix[..=prefix.iter().rposition(|&byte| byte == b'\n')?];
    let mut edges = Vec::with_capacity(LINKS);
    csv::rows::<2>(prefix, |[a, b]| {
        if a == b || edges.len() == LINKS {
            return;
        }
        if let (Some(a), Some(b)) = (projection(a, b'u'), projection(b, b'u')) {
            edges.push((a, b));
        }
    });

    if edges.len() < ENSEMBLES * PAIRS {
        return None;
    }
    let bound = (event_input_bytes / 32).min((u32::MAX as usize) / CODE_ALIASES as usize);
    let mut best = (edges.len() * 2, edges.len(), 0, 0);
    for ensemble in 0..ENSEMBLES {
        let pairs = (0..PAIRS)
            .map(|i| edges[(i * ENSEMBLES + ensemble) * edges.len() / (PAIRS * ENSEMBLES)])
            .collect::<Vec<_>>();
        for (_, key) in lift(&pairs) {
            let mut fail = 0;
            let mut crosses = 0;
            let mut max_group = 0;
            for &(a, b) in &edges {
                let left = (ordinal(a, key) >> ALIAS_BITS) as usize;
                let right = (ordinal(b, key) >> ALIAS_BITS) as usize;
                crosses += usize::from(left != right);
                for group in [left, right] {
                    if group < bound {
                        max_group = max_group.max(group);
                    } else {
                        fail += 1;
                    }
                }
                if fail > best.0 {
                    break;
                }
            }
            best = best.min((fail, crosses, max_group, key));
        }
    }

    let (outside, _, maximum, key) = best;
    if outside > edges.len() / 5 {
        return None;
    }
    let observed = maximum + 1;
    let groups = observed.saturating_add((observed / 32).max(64)).min(bound);
    Some(UserLayout { key, groups })
}
