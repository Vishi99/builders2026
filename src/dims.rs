//! Products, campaigns and the manifest window.

use crate::text::split_slice;
use std::collections::HashMap;
use std::io;
use std::path::Path;

pub struct Dims {
    /// Category names in byte order; index 0 is unused (no purchase).
    pub categories: Vec<Vec<u8>>,
    pub unknown: u32,
    /// `p<digits>` without leading zeros: dense category by number.
    product_dense: Vec<u8>,
    product_map: HashMap<Vec<u8>, u32>,
    /// Campaign ids by index; index 0 is `organic`.
    pub campaign_ids: Vec<Vec<u8>>,
    pub channels: Vec<Vec<u8>>,
    campaign_dense: Vec<u32>,
    campaign_map: HashMap<Vec<u8>, u32>,
}

fn for_rows<'a, const N: usize>(file: &'a [u8], mut f: impl FnMut([&'a [u8]; N])) {
    let body = match file.iter().position(|&c| c == b'\n') {
        Some(i) => &file[i + 1..],
        None => return,
    };
    let body = body.strip_suffix(b"\n").unwrap_or(body);
    if body.is_empty() {
        return;
    }
    for line in body.split(|&c| c == b'\n') {
        if let Some((st, en)) = split_slice::<N>(line) {
            f(std::array::from_fn(|i| &line[st[i] as usize..en[i] as usize]));
        }
    }
}

#[allow(dead_code)]
fn rows<const N: usize>(file: &[u8]) -> Vec<[Vec<u8>; N]> {
    let mut out = Vec::new();
    let body = match file.iter().position(|&c| c == b'\n') {
        Some(i) => &file[i + 1..],
        None => &[][..],
    };
    let body = body.strip_suffix(b"\n").unwrap_or(body);
    if body.is_empty() && !file.ends_with(b"\n\n") {
        return out;
    }
    for line in body.split(|&c| c == b'\n') {
        if let Some((st, en)) = split_slice::<N>(line) {
            out.push(std::array::from_fn(|i| line[st[i] as usize..en[i] as usize].to_vec()));
        }
    }
    out
}

#[inline(always)]
fn dense_number(id: &[u8], prefix: &[u8], max_digits: usize) -> Option<usize> {
    if id.len() <= prefix.len() || &id[..prefix.len()] != prefix {
        return None;
    }
    let d = &id[prefix.len()..];
    if d.len() > max_digits || d[0] == b'0' {
        return None;
    }
    let mut v = 0usize;
    for &c in d {
        if !c.is_ascii_digit() {
            return None;
        }
        v = v * 10 + (c - b'0') as usize;
    }
    Some(v)
}

impl Dims {
    pub fn load(dir: &Path) -> io::Result<Dims> {
        let pfile = std::fs::read(dir.join("products.csv"))?;
        // (product, category) slices without per-row allocation.
        let mut pairs: Vec<(&[u8], &[u8])> = Vec::with_capacity(pfile.len() / 12);
        for_rows::<2>(&pfile, |f| pairs.push((f[0], f[1])));
        let mut names: Vec<&[u8]> = Vec::new();
        {
            let mut seen: HashMap<&[u8], ()> = HashMap::new();
            for &(_, c) in &pairs {
                if seen.insert(c, ()).is_none() {
                    names.push(c);
                }
            }
            if !seen.contains_key(&b"unknown"[..]) {
                names.push(b"unknown");
            }
        }
        names.sort_unstable();
        let mut categories = vec![Vec::new()];
        categories.extend(names.iter().map(|n| n.to_vec()));
        let index: HashMap<&[u8], u32> = names.iter().enumerate().map(|(i, n)| (*n, i as u32 + 1)).collect();
        let unknown = index[&b"unknown"[..]];
        let mut product_dense = Vec::new();
        let mut product_map = HashMap::new();
        for &(p, c) in &pairs {
            let cat = index[c];
            if p.is_empty() {
                continue;
            }
            match dense_number(p, b"p", 6) {
                Some(n) if cat < 256 => {
                    if product_dense.len() <= n {
                        product_dense.resize((n + 1).max(product_dense.len() * 2), 0);
                    }
                    product_dense[n] = cat as u8;
                }
                _ => {
                    product_map.insert(p.to_vec(), cat);
                }
            }
        }
        let cfile = std::fs::read(dir.join("campaigns.csv"))?;
        let mut campaign_ids = vec![b"organic".to_vec()];
        let mut channels = vec![b"organic".to_vec()];
        let mut campaign_dense = Vec::new();
        let mut campaign_map = HashMap::new();
        for_rows::<2>(&cfile, |f| {
            let (c, ch) = (f[0], f[1]);
            if c.is_empty() {
                return;
            }
            let idx = campaign_ids.len() as u32;
            match dense_number(c, b"c_", 7) {
                Some(n) => {
                    if campaign_dense.len() <= n {
                        campaign_dense.resize((n + 1).max(campaign_dense.len() * 2), 0);
                    }
                    if campaign_dense[n] != 0 {
                        return;
                    }
                    campaign_dense[n] = idx;
                }
                None => {
                    if campaign_map.contains_key(c) {
                        return;
                    }
                    campaign_map.insert(c.to_vec(), idx);
                }
            }
            campaign_ids.push(c.to_vec());
            channels.push(ch.to_vec());
        });
        Ok(Dims {
            categories,
            unknown,
            product_dense,
            product_map,
            campaign_ids,
            channels,
            campaign_dense,
            campaign_map,
        })
    }

    /// Category index of a non-empty product id.
    #[inline(always)]
    pub fn category(&self, p: &[u8]) -> u32 {
        if p.len() >= 2 && p[0] == b'p' && p[1] != b'0' && p.len() <= 7 {
            let mut v = 0usize;
            let mut ok = true;
            for &c in &p[1..] {
                let d = c.wrapping_sub(b'0');
                ok &= d <= 9;
                v = v * 10 + d as usize;
            }
            if ok {
                return match self.product_dense.get(v) {
                    Some(&c) if c != 0 => c as u32,
                    _ => self.product_map.get(p).copied().unwrap_or(self.unknown),
                };
            }
        }
        self.product_map.get(p).copied().unwrap_or(self.unknown)
    }

    /// Campaign index (>= 1) of a known campaign id, or 0.
    #[inline(always)]
    pub fn campaign(&self, c: &[u8]) -> u32 {
        if c.len() >= 3 && c[0] == b'c' && c[1] == b'_' && c[2] != b'0' && c.len() <= 9 {
            let mut v = 0usize;
            let mut ok = true;
            for &x in &c[2..] {
                let d = x.wrapping_sub(b'0');
                ok &= d <= 9;
                v = v * 10 + d as usize;
            }
            if ok {
                return self.campaign_dense.get(v).copied().unwrap_or(0);
            }
        }
        self.campaign_map.get(c).copied().unwrap_or(0)
    }
}

/// The two manifest instants in epoch milliseconds.
pub fn manifest(dir: &Path) -> io::Result<(i64, i64)> {
    let text = std::fs::read(dir.join("manifest.json"))?;
    let get = |key: &[u8]| -> Option<i64> {
        let pos = text.windows(key.len()).position(|w| w == key)? + key.len();
        let rest = &text[pos..];
        let colon = rest.iter().position(|&c| c == b':')?;
        let rest = &rest[colon + 1..];
        let q0 = rest.iter().position(|&c| c == b'"')?;
        let rest = &rest[q0 + 1..];
        let q1 = rest.iter().position(|&c| c == b'"')?;
        crate::ts::parse_ts_slice(&rest[..q1])
    };
    match (get(b"\"window_start\""), get(b"\"window_end\"")) {
        (Some(a), Some(b)) => Ok((a, b)),
        _ => Err(io::Error::other("bad manifest")),
    }
}
