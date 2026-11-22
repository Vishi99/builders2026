//! CSV row splitting for the contest dialect.
//!
//! Rows end only at `\n`. A row with the wrong number of fields, or a quote
//! anywhere but around a whole field, is malformed and dropped. Each field is
//! reported with its quotes stripped.
//!
//! Blocks of 64 bytes are classified once into comma, quote and newline bit
//! masks; each row then reads a 128-bit window of each mask at its start.

pub const SUB: usize = 32 << 10;
pub const MASK_WORDS: usize = SUB / 64 + 8;

pub struct Masks {
    pub c: Vec<u64>,
    pub q: Vec<u64>,
    pub n: Vec<u64>,
}

impl Masks {
    pub fn new() -> Masks {
        Masks { c: vec![0; MASK_WORDS], q: vec![0; MASK_WORDS], n: vec![0; MASK_WORDS] }
    }
}

#[cfg(target_feature = "avx2")]
#[inline(always)]
unsafe fn classify(p: *const u8, nb: usize, m: &mut Masks) {
    use std::arch::x86_64::*;
    let vc = _mm256_set1_epi8(b',' as i8);
    let vq = _mm256_set1_epi8(b'"' as i8);
    let vn = _mm256_set1_epi8(b'\n' as i8);
    let (cp, qp, np) = (m.c.as_mut_ptr(), m.q.as_mut_ptr(), m.n.as_mut_ptr());
    for i in 0..nb {
        let a = _mm256_loadu_si256(p.add(64 * i) as *const __m256i);
        let b = _mm256_loadu_si256(p.add(64 * i + 32) as *const __m256i);
        let c = (_mm256_movemask_epi8(_mm256_cmpeq_epi8(a, vc)) as u32 as u64)
            | ((_mm256_movemask_epi8(_mm256_cmpeq_epi8(b, vc)) as u32 as u64) << 32);
        let q = (_mm256_movemask_epi8(_mm256_cmpeq_epi8(a, vq)) as u32 as u64)
            | ((_mm256_movemask_epi8(_mm256_cmpeq_epi8(b, vq)) as u32 as u64) << 32);
        let n = (_mm256_movemask_epi8(_mm256_cmpeq_epi8(a, vn)) as u32 as u64)
            | ((_mm256_movemask_epi8(_mm256_cmpeq_epi8(b, vn)) as u32 as u64) << 32);
        *cp.add(i) = c;
        *qp.add(i) = q;
        *np.add(i) = n;
    }
}

#[cfg(not(target_feature = "avx2"))]
#[inline(always)]
unsafe fn classify(p: *const u8, nb: usize, m: &mut Masks) {
    for i in 0..nb {
        let (mut c, mut q, mut n) = (0u64, 0u64, 0u64);
        for j in 0..64 {
            let x = *p.add(64 * i + j);
            c |= ((x == b',') as u64) << j;
            q |= ((x == b'"') as u64) << j;
            n |= ((x == b'\n') as u64) << j;
        }
        m.c[i] = c;
        m.q[i] = q;
        m.n[i] = n;
    }
}

pub unsafe fn classify_only(p: *const u8, nb: usize, m: &mut Masks) -> u64 {
    classify(p, nb, m);
    let mut acc = 0u64;
    for i in 0..nb {
        acc = acc.wrapping_add(m.n[i] ^ m.c[i] ^ m.q[i]);
    }
    acc
}

#[inline(always)]
fn funnel(m: &[u64], b: usize, o: u32) -> u128 {
    unsafe {
        let w0 = *m.get_unchecked(b) as u128;
        let w1 = *m.get_unchecked(b + 1) as u128;
        let w2 = *m.get_unchecked(b + 2) as u128;
        let lo = ((w0 | (w1 << 64)) >> o) as u64;
        let hi = ((w1 | (w2 << 64)) >> o) as u64;
        (lo as u128) | ((hi as u128) << 64)
    }
}

#[cfg(target_feature = "pclmulqdq")]
#[inline(always)]
fn pxor64(x: u64) -> u64 {
    use std::arch::x86_64::*;
    unsafe {
        let r = _mm_clmulepi64_si128(_mm_set_epi64x(0, x as i64), _mm_set1_epi8(-1), 0);
        _mm_cvtsi128_si64(r) as u64
    }
}

#[cfg(not(target_feature = "pclmulqdq"))]
#[inline(always)]
fn pxor64(mut x: u64) -> u64 {
    x ^= x << 1;
    x ^= x << 2;
    x ^= x << 4;
    x ^= x << 8;
    x ^= x << 16;
    x ^= x << 32;
    x
}

/// Inclusive prefix XOR over 128 bits: bit i is the parity of bits 0..=i.
#[inline(always)]
fn pxor128(q: u128) -> u128 {
    let lo = q as u64;
    let hi = (q >> 64) as u64;
    let plo = pxor64(lo);
    let phi = pxor64(hi) ^ 0u64.wrapping_sub((lo.count_ones() & 1) as u64);
    (plo as u128) | ((phi as u128) << 64)
}

/// Field content bounds within a row, quotes stripped.
pub type Bounds<const N: usize> = ([u32; N], [u32; N]);

/// Split every row of `[base, base+len)` into N fields and call `f` for each
/// well-formed row. `base` starts at a row; memory must be readable 320 bytes
/// past `len`. A row that does not end with `\n` before `len` must not exist.
#[inline(always)]
pub unsafe fn scan_rows<const N: usize>(
    base: *const u8,
    len: usize,
    m: &mut Masks,
    mut f: impl FnMut(*const u8, &[u32; N], &[u32; N]),
) {
    let mut pos = 0usize;
    while pos < len {
        let sub_len = SUB.min(len - pos);
        let nb = sub_len.div_ceil(64) + 3;
        let sub = base.add(pos);
        classify(sub, nb, m);
        let mut s = 0usize;
        while s < sub_len {
            let b = s >> 6;
            let o = (s & 63) as u32;
            let n = funnel(&m.n, b, o);
            if n == 0 {
                let rl = find_nl(sub.add(s), base.add(len));
                slow_row::<N>(sub.add(s), rl, &mut f);
                s += rl + 1;
                continue;
            }
            let rl = n.trailing_zeros() as usize;
            s_row::<N>(sub.add(s), rl, funnel(&m.c, b, o), funnel(&m.q, b, o), &mut f);
            s += rl + 1;
        }
        pos += s;
    }
}

#[inline(always)]
unsafe fn s_row<const N: usize>(
    row: *const u8,
    rl: usize,
    cw: u128,
    qw: u128,
    f: &mut impl FnMut(*const u8, &[u32; N], &[u32; N]),
) {
    if rl == 0 {
        return;
    }
    let rowmask: u128 = (1u128 << rl) - 1;
    let c = cw & rowmask;
    if c.count_ones() as usize != N - 1 {
        return;
    }
    let q = qw & rowmask;
    let mut st = [0u32; N];
    let mut en = [0u32; N];
    let mut cur = c;
    for i in 0..N - 1 {
        let p = cur.trailing_zeros();
        cur &= cur - 1;
        en[i] = p;
        st[i + 1] = p + 1;
    }
    en[N - 1] = rl as u32;
    {
        let r = pxor128(q);
        let first = 1u128 | (c << 1);
        let last = (c >> 1) | (1u128 << (rl - 1));
        let bad = (c & r) | ((r >> (rl - 1)) & 1) | (q & r & !first) | (q & !r & !last);
        if bad != 0 {
            return;
        }
        for i in 0..N {
            let quoted = (*row.add(st[i] as usize) == b'"') as u32;
            st[i] += quoted;
            en[i] -= quoted;
        }
    }
    f(row, &st, &en);
}

#[cold]
#[inline(never)]
unsafe fn find_nl(p: *const u8, end: *const u8) -> usize {
    let n = end as usize - p as usize;
    let s = std::slice::from_raw_parts(p, n);
    match s.iter().position(|&x| x == b'\n') {
        Some(i) => i,
        None => n,
    }
}

/// Scalar split of one row of any length.
#[cold]
#[inline(never)]
unsafe fn slow_row<const N: usize>(row: *const u8, rl: usize, f: &mut impl FnMut(*const u8, &[u32; N], &[u32; N])) {
    let s = std::slice::from_raw_parts(row, rl);
    if let Some((st, en)) = split_slice::<N>(s) {
        f(row, &st, &en);
    }
}

/// Reference splitter over one row (no newline). Used by the slow path,
/// small files and the generic pipeline.
pub fn split_slice<const N: usize>(s: &[u8]) -> Option<([u32; N], [u32; N])> {
    if s.is_empty() {
        return None;
    }
    let mut st = [0u32; N];
    let mut en = [0u32; N];
    let mut k = 0usize;
    let mut start = 0usize;
    for i in 0..=s.len() {
        if i == s.len() || s[i] == b',' {
            if k >= N {
                return None;
            }
            let mut a = start;
            let mut b = i;
            let field = &s[a..b];
            let quotes = field.iter().filter(|&&x| x == b'"').count();
            if quotes != 0 {
                if quotes != 2 || field.len() < 2 || field[0] != b'"' || field[field.len() - 1] != b'"' {
                    return None;
                }
                a += 1;
                b -= 1;
            }
            st[k] = a as u32;
            en[k] = b as u32;
            k += 1;
            start = i + 1;
        }
    }
    if k != N {
        return None;
    }
    Some((st, en))
}

pub struct Masks2 {
    pub c: Vec<u64>,
    pub q: Vec<u64>,
    pub nl: Vec<u32>,
}

impl Masks2 {
    pub fn new() -> Masks2 {
        Masks2 { c: vec![0; MASK_WORDS + 4], q: vec![0; MASK_WORDS + 4], nl: vec![0; SUB + 512] }
    }
}

/// Classify blocks, store comma and quote masks, and flatten newline
/// positions. Returns the number of newlines.
#[cfg(target_feature = "avx2")]
#[inline(always)]
unsafe fn classify2(p: *const u8, nb: usize, m: &mut Masks2) -> usize {
    use std::arch::x86_64::*;
    let vc = _mm256_set1_epi8(b',' as i8);
    let vq = _mm256_set1_epi8(b'"' as i8);
    let vn = _mm256_set1_epi8(b'\n' as i8);
    let (cp, qp, np) = (m.c.as_mut_ptr(), m.q.as_mut_ptr(), m.nl.as_mut_ptr());
    let mut k = 0usize;
    for i in 0..nb {
        let a = _mm256_loadu_si256(p.add(64 * i) as *const __m256i);
        let b = _mm256_loadu_si256(p.add(64 * i + 32) as *const __m256i);
        let c = (_mm256_movemask_epi8(_mm256_cmpeq_epi8(a, vc)) as u32 as u64)
            | ((_mm256_movemask_epi8(_mm256_cmpeq_epi8(b, vc)) as u32 as u64) << 32);
        let q = (_mm256_movemask_epi8(_mm256_cmpeq_epi8(a, vq)) as u32 as u64)
            | ((_mm256_movemask_epi8(_mm256_cmpeq_epi8(b, vq)) as u32 as u64) << 32);
        let mut n = (_mm256_movemask_epi8(_mm256_cmpeq_epi8(a, vn)) as u32 as u64)
            | ((_mm256_movemask_epi8(_mm256_cmpeq_epi8(b, vn)) as u32 as u64) << 32);
        *cp.add(i) = c;
        *qp.add(i) = q;
        let base = (64 * i) as u32;
        let cnt = n.count_ones() as usize;
        let out = np.add(k);
        *out = base + n.trailing_zeros();
        n &= n.wrapping_sub(1);
        *out.add(1) = base + n.trailing_zeros();
        n &= n.wrapping_sub(1);
        if cnt > 2 {
            let mut j = 2;
            while n != 0 {
                *out.add(j) = base + n.trailing_zeros();
                n &= n - 1;
                j += 1;
            }
        }
        k += cnt;
    }
    k
}

#[cfg(not(target_feature = "avx2"))]
#[inline(always)]
unsafe fn classify2(p: *const u8, nb: usize, m: &mut Masks2) -> usize {
    let mut k = 0;
    for i in 0..nb {
        let (mut c, mut q) = (0u64, 0u64);
        for j in 0..64 {
            let x = *p.add(64 * i + j);
            c |= ((x == b',') as u64) << j;
            q |= ((x == b'"') as u64) << j;
            if x == b'\n' {
                m.nl[k] = (64 * i + j) as u32;
                k += 1;
            }
        }
        m.c[i] = c;
        m.q[i] = q;
    }
    k
}

#[inline(always)]
unsafe fn win(m: &[u64], bit: usize) -> u128 {
    let p = (m.as_ptr() as *const u8).add(bit >> 3);
    let lo = std::ptr::read_unaligned(p as *const u64);
    let hi = std::ptr::read_unaligned(p.add(8) as *const u64);
    (lo as u128) | ((hi as u128) << 64)
}

/// Like `scan_rows`, but row boundaries come from flattened newline
/// positions, so rows are independent of each other.
#[inline(always)]
pub unsafe fn scan_rows2<const N: usize>(
    base: *const u8,
    len: usize,
    m: &mut Masks2,
    mut f: impl FnMut(*const u8, &[u32; N], &[u32; N]),
) {
    let mut pos = 0usize;
    while pos < len {
        let sub_len = SUB.min(len - pos);
        let nb = sub_len.div_ceil(64) + 3;
        let sub = base.add(pos);
        let nn = classify2(sub, nb, m);
        let mut s = 0usize;
        let mut r = 0usize;
        while s < sub_len {
            if r >= nn {
                // Row longer than the lookahead.
                let rl = find_nl(sub.add(s), base.add(len));
                slow_row::<N>(sub.add(s), rl, &mut f);
                s += rl + 1;
                break;
            }
            let e = *m.nl.get_unchecked(r) as usize;
            r += 1;
            let wb = s & !7;
            let d = (s & 7) as u32;
            let rel = e - wb;
            if rel > 127 {
                slow_row::<N>(sub.add(s), e - s, &mut f);
                s = e + 1;
                continue;
            }
            let rowmask: u128 = ((1u128 << rel) - 1) & (!0u128 << d);
            let c = win(&m.c, wb) & rowmask;
            let q = win(&m.q, wb) & rowmask;
            row2::<N>(sub.add(wb), d, rel as u32, c, q, &mut f);
            s = e + 1;
        }
        pos += s;
    }
}


#[inline(always)]
unsafe fn row2<const N: usize>(
    wbase: *const u8,
    d: u32,
    rel: u32,
    c: u128,
    q: u128,
    f: &mut impl FnMut(*const u8, &[u32; N], &[u32; N]),
) {
    if rel == d || c.count_ones() as usize != N - 1 {
        return;
    }
    let mut st = [0u32; N];
    let mut en = [0u32; N];
    st[0] = d;
    let mut cur = c;
    for i in 0..N - 1 {
        let p = cur.trailing_zeros();
        cur &= cur - 1;
        en[i] = p;
        st[i + 1] = p + 1;
    }
    en[N - 1] = rel;
    let q_lo = q as u64;
    let q_hi = (q >> 64) as u64;
    if (q_lo | q_hi) != 0 {
        let c_lo = c as u64;
        let c_hi = (c >> 64) as u64;
        let r_lo = pxor64(q_lo);
        let r_hi = pxor64(q_hi) ^ 0u64.wrapping_sub((q_lo.count_ones() & 1) as u64);
        let f_lo = (1u64 << d) | (c_lo << 1);
        let f_hi = (c_hi << 1) | (c_lo >> 63);
        let b = rel - 1;
        let bit = 1u64 << (b & 63);
        let in_lo = b < 64;
        let l_lo = (c_lo >> 1) | (c_hi << 63) | if in_lo { bit } else { 0 };
        let l_hi = (c_hi >> 1) | if in_lo { 0 } else { bit };
        let rb = (if in_lo { r_lo } else { r_hi } >> (b & 63)) & 1;
        let bad = (c_lo & r_lo)
            | (c_hi & r_hi)
            | rb
            | (q_lo & r_lo & !f_lo)
            | (q_hi & r_hi & !f_hi)
            | (q_lo & !r_lo & !l_lo)
            | (q_hi & !r_hi & !l_hi);
        if bad != 0 {
            return;
        }
        for i in 0..N {
            let quoted = (*wbase.add(st[i] as usize) == b'"') as u32;
            st[i] += quoted;
            en[i] -= quoted;
        }
    }
    f(wbase, &st, &en);
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fast_rows<const N: usize>(text: &[u8]) -> Vec<Vec<Vec<u8>>> {
        let mut buf = text.to_vec();
        buf.extend_from_slice(&[0u8; 512]);
        let mut m = Masks::new();
        let mut out = Vec::new();
        unsafe {
            scan_rows::<N>(buf.as_ptr(), text.len(), &mut m, |row, st, en| {
                let mut r = Vec::new();
                for i in 0..N {
                    r.push(std::slice::from_raw_parts(row.add(st[i] as usize), (en[i] - st[i]) as usize).to_vec());
                }
                out.push(r);
            });
        }
        out
    }

    fn fast_rows2<const N: usize>(text: &[u8]) -> Vec<Vec<Vec<u8>>> {
        let mut buf = text.to_vec();
        buf.extend_from_slice(&[0u8; 512]);
        let mut m = Masks2::new();
        let mut out = Vec::new();
        unsafe {
            scan_rows2::<N>(buf.as_ptr(), text.len(), &mut m, |row, st, en| {
                let mut r = Vec::new();
                for i in 0..N {
                    r.push(std::slice::from_raw_parts(row.add(st[i] as usize), (en[i] - st[i]) as usize).to_vec());
                }
                out.push(r);
            });
        }
        out
    }

    fn ref_rows<const N: usize>(text: &[u8]) -> Vec<Vec<Vec<u8>>> {
        let mut out = Vec::new();
        let body = text.strip_suffix(b"\n").unwrap_or(text);
        for line in body.split(|&c| c == b'\n') {
            if let Some((st, en)) = split_slice::<N>(line) {
                out.push((0..N).map(|i| line[st[i] as usize..en[i] as usize].to_vec()).collect());
            }
        }
        out
    }

    #[test]
    fn splitting_matches_reference() {
        let mut rng = 0x1234_5678_9abc_def0u64;
        let mut next = || {
            rng ^= rng << 13;
            rng ^= rng >> 7;
            rng ^= rng << 17;
            rng
        };
        let alphabet = b"ab,\"\"\"x\r ,,\"y\"zz";
        for round in 0..3000 {
            let mut text = Vec::new();
            let rows = 1 + next() % 40;
            for _ in 0..rows {
                let len = (next() % if round % 7 == 0 { 300 } else { 40 }) as usize;
                for _ in 0..len {
                    text.push(alphabet[(next() % alphabet.len() as u64) as usize]);
                }
                text.push(b'\n');
            }
            assert_eq!(fast_rows::<3>(&text), ref_rows::<3>(&text));
            assert_eq!(fast_rows2::<3>(&text), ref_rows::<3>(&text));
            assert_eq!(fast_rows2::<7>(&text), ref_rows::<7>(&text));
            assert_eq!(fast_rows2::<2>(&text), ref_rows::<2>(&text));
            assert_eq!(fast_rows::<7>(&text), ref_rows::<7>(&text));
            assert_eq!(fast_rows::<2>(&text), ref_rows::<2>(&text));
        }
        let t = b"a,b\n\"a\",\"\"\n\"a\"b,c\n\",\nx,\"\n\"\",\"\"\nq\"\",x\n";
        assert_eq!(fast_rows::<2>(t), ref_rows::<2>(t));
    }
}
