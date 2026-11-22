//! The exact generic pipeline: plain maps over byte strings, one thread.
//! Used when an input breaks an assumption of the fast pipeline (and as a
//! test oracle with SS_GENERIC=1).

use crate::pq;
use crate::text::split_slice;
use crate::ts::parse_ts_slice;
use std::collections::{BTreeMap, HashMap};
use std::io;
use std::path::Path;

const DAY: i64 = 86_400_000;
const WEEK: i64 = 7 * DAY;
const GAP: i64 = 1_800_000;

fn csv_files(dir: &Path) -> Vec<Vec<u8>> {
    let mut paths: Vec<_> = std::fs::read_dir(dir)
        .map(|rd| {
            rd.filter_map(|e| e.ok())
                .filter(|e| {
                    let n = e.file_name();
                    let n = n.to_string_lossy();
                    n.ends_with(".csv") && !n.starts_with('.')
                })
                .map(|e| e.path())
                .collect()
        })
        .unwrap_or_default();
    paths.sort();
    paths.iter().filter_map(|p| std::fs::read(p).ok()).collect()
}

/// Data lines of a CSV file (header skipped, final newline optional).
fn lines(file: &[u8]) -> impl Iterator<Item = &[u8]> {
    let body = match file.iter().position(|&c| c == b'\n') {
        Some(i) => &file[i + 1..],
        None => &file[file.len()..],
    };
    let body = body.strip_suffix(b"\n").unwrap_or(body);
    let empty = body.is_empty();
    body.split(|&c| c == b'\n').filter(move |_| !empty)
}

fn fields<const N: usize>(line: &[u8]) -> Option<[&[u8]; N]> {
    let (st, en) = split_slice::<N>(line)?;
    Some(std::array::from_fn(|i| &line[st[i] as usize..en[i] as usize]))
}

fn find(parent: &mut Vec<usize>, mut x: usize) -> usize {
    while parent[x] != x {
        parent[x] = parent[parent[x]];
        x = parent[x];
    }
    x
}

fn price(s: &[u8]) -> Option<i64> {
    if s.is_empty() || !s.iter().all(|c| c.is_ascii_digit()) {
        return None;
    }
    let s = &s[s.iter().position(|&c| c != b'0').unwrap_or(s.len())..];
    if s.len() > 8 {
        return None;
    }
    let mut v = 0i64;
    for &c in s {
        v = v * 10 + (c - b'0') as i64;
    }
    if (1..=10_000_000).contains(&v) {
        Some(v)
    } else {
        None
    }
}

struct Copy<'a> {
    ingest: i64,
    user: &'a [u8],
    ts: &'a [u8],
    kind: &'a [u8],
    product: &'a [u8],
    price: &'a [u8],
}

pub fn run(data: &Path, out: &Path) -> io::Result<()> {
    let (ws, we) = crate::dims::manifest(data)?;
    // Products and campaigns.
    let pfile = std::fs::read(data.join("products.csv"))?;
    let mut product_cat: HashMap<&[u8], &[u8]> = HashMap::new();
    for line in lines(&pfile) {
        if let Some([p, c]) = fields::<2>(line) {
            product_cat.insert(p, c);
        }
    }
    let cfile = std::fs::read(data.join("campaigns.csv"))?;
    let mut campaigns: HashMap<&[u8], &[u8]> = HashMap::new();
    for line in lines(&cfile) {
        if let Some([c, ch]) = fields::<2>(line) {
            if !c.is_empty() {
                campaigns.entry(c).or_insert(ch);
            }
        }
    }
    // Links.
    let lfiles = csv_files(&data.join("links"));
    let mut node: HashMap<&[u8], usize> = HashMap::new();
    let mut names: Vec<&[u8]> = Vec::new();
    let mut parent: Vec<usize> = Vec::new();
    for f in &lfiles {
        for line in lines(f) {
            if let Some([a, b]) = fields::<2>(line) {
                if a.is_empty() || b.is_empty() {
                    continue;
                }
                let ia = *node.entry(a).or_insert_with(|| {
                    names.push(a);
                    parent.push(parent.len());
                    parent.len() - 1
                });
                let ib = *node.entry(b).or_insert_with(|| {
                    names.push(b);
                    parent.push(parent.len());
                    parent.len() - 1
                });
                let ra = find(&mut parent, ia);
                let rb = find(&mut parent, ib);
                if ra != rb {
                    parent[ra] = rb;
                }
            }
        }
    }
    let mut least: HashMap<usize, &[u8]> = HashMap::new();
    for i in 0..names.len() {
        let r = find(&mut parent, i);
        let e = least.entry(r).or_insert(names[i]);
        if names[i] < *e {
            *e = names[i];
        }
    }
    let person = |u: &[u8], node: &HashMap<&[u8], usize>, parent: &mut Vec<usize>| -> Vec<u8> {
        match node.get(u) {
            Some(&i) => {
                let r = find(parent, i);
                least[&r].to_vec()
            }
            None => u.to_vec(),
        }
    };
    // Events: dedup on event_id by smallest ingest_ts, then validate.
    let efiles = csv_files(&data.join("events"));
    let mut best: HashMap<&[u8], Copy> = HashMap::new();
    for f in &efiles {
        for line in lines(f) {
            if let Some([eid, user, ts, ingest, kind, product, pr]) = fields::<7>(line) {
                if eid.is_empty() {
                    continue;
                }
                let Some(ingest) = parse_ts_slice(ingest) else { continue };
                let c = Copy { ingest, user, ts, kind, product, price: pr };
                match best.get_mut(eid) {
                    Some(old) => {
                        if c.ingest < old.ingest {
                            *old = c;
                        }
                    }
                    None => {
                        best.insert(eid, c);
                    }
                }
            }
        }
    }
    // person -> (ts, price, category)
    let mut evs: HashMap<Vec<u8>, Vec<(i64, i64, Vec<u8>)>> = HashMap::new();
    for (_, c) in best {
        if c.user.is_empty() || c.product.is_empty() {
            continue;
        }
        let Some(t) = parse_ts_slice(c.ts) else { continue };
        if t < ws || t >= we {
            continue;
        }
        let purchase = c.kind == b"purchase";
        if !purchase && c.kind != b"view" && c.kind != b"cart" {
            continue;
        }
        let mut p = 0;
        let mut cat = Vec::new();
        if purchase {
            match price(c.price) {
                Some(v) => p = v,
                None => continue,
            }
            cat = product_cat.get(c.product).map(|x| x.to_vec()).unwrap_or_else(|| b"unknown".to_vec());
        }
        if c.ingest - t > DAY {
            continue;
        }
        let pk = person(c.user, &node, &mut parent);
        evs.entry(pk).or_default().push((t, p, cat));
    }
    // Clicks.
    let cfiles = csv_files(&data.join("clicks"));
    let mut seen: HashMap<&[u8], ()> = HashMap::new();
    let mut clicks: HashMap<Vec<u8>, Vec<(i64, Vec<u8>, Vec<u8>)>> = HashMap::new();
    for f in &cfiles {
        for line in lines(f) {
            if let Some([cid, user, camp, ts]) = fields::<4>(line) {
                if cid.is_empty() || seen.contains_key(cid) {
                    continue;
                }
                seen.insert(cid, ());
                if user.is_empty() || !campaigns.contains_key(camp) {
                    continue;
                }
                let Some(t) = parse_ts_slice(ts) else { continue };
                if t < ws - WEEK || t >= we {
                    continue;
                }
                let pk = person(user, &node, &mut parent);
                clicks.entry(pk).or_default().push((t, cid.to_vec(), camp.to_vec()));
            }
        }
    }
    // Sessions.
    let mut udaily: BTreeMap<(Vec<u8>, i64), (i64, i64, i64, BTreeMap<Vec<u8>, i64>)> = BTreeMap::new();
    let mut cdaily: BTreeMap<(Vec<u8>, i64), (i64, i64, i64, i64)> = BTreeMap::new();
    for (pk, mut ev) in evs {
        ev.sort_by_key(|e| e.0);
        let mut cl = clicks.remove(&pk).unwrap_or_default();
        cl.sort();
        let mut i = 0;
        while i < ev.len() {
            let start = ev[i].0;
            let mut last = start;
            let mut n = 0;
            let mut rev = 0;
            let mut cats: BTreeMap<Vec<u8>, i64> = BTreeMap::new();
            while i < ev.len() && ev[i].0 - last <= GAP {
                last = ev[i].0;
                n += 1;
                if ev[i].1 > 0 {
                    rev += ev[i].1;
                    *cats.entry(ev[i].2.clone()).or_default() += ev[i].1;
                }
                i += 1;
            }
            let day = start.div_euclid(DAY);
            let u = udaily.entry((pk.clone(), day)).or_default();
            u.0 += 1;
            u.1 += n;
            u.2 += rev;
            for (c, v) in cats {
                *u.3.entry(c).or_default() += v;
            }
            let window: Vec<&(i64, Vec<u8>, Vec<u8>)> =
                cl.iter().filter(|c| c.0 <= start && start - c.0 <= WEEK).collect();
            let credited: Vec<u8> = match window.iter().max_by(|a, b| a.0.cmp(&b.0).then(b.1.cmp(&a.1))) {
                Some(c) => c.2.clone(),
                None => b"organic".to_vec(),
            };
            let e = cdaily.entry((credited, day)).or_default();
            e.0 += 1;
            e.1 += (rev > 0) as i64;
            e.2 += rev;
            let mut assisted: Vec<&Vec<u8>> = window.iter().map(|c| &c.2).collect();
            assisted.sort();
            assisted.dedup();
            for c in assisted {
                cdaily.entry((c.clone(), day)).or_default().3 += 1;
            }
        }
    }
    // Write.
    let ud = out.join("user_daily");
    let cd = out.join("campaign_daily");
    std::fs::create_dir_all(&ud)?;
    std::fs::create_dir_all(&cd)?;
    let mut offs = vec![0u32];
    let mut data_ = Vec::new();
    let mut day = Vec::new();
    let (mut s, mut e, mut r) = (Vec::new(), Vec::new(), Vec::new());
    let mut catdict: Vec<Vec<u8>> = Vec::new();
    for (_, v) in udaily.iter() {
        for c in v.3.keys() {
            catdict.push(c.clone());
        }
    }
    catdict.sort();
    catdict.dedup();
    if catdict.is_empty() {
        catdict.push(b"unknown".to_vec());
    }
    let mut cats = Vec::new();
    for ((pk, d), v) in &udaily {
        data_.extend_from_slice(pk);
        offs.push(data_.len() as u32);
        day.push(*d as i32);
        s.push(v.0);
        e.push(v.1);
        r.push(v.2);
        let mut top: Option<(&Vec<u8>, i64)> = None;
        for (c, &x) in &v.3 {
            if top.map_or(true, |t| x > t.1) {
                top = Some((c, x));
            }
        }
        cats.push(match top {
            Some((c, _)) => catdict.binary_search(c).unwrap() as u32 + 1,
            None => 0,
        });
    }
    let fields = [
        pq::Field { name: "user_id", col: pq::Col::Str { offs: &offs, data: &data_ } },
        pq::Field { name: "day", col: pq::Col::I32 { v: &day, date: true } },
        pq::Field { name: "sessions", col: pq::Col::I64 { v: &s } },
        pq::Field { name: "events", col: pq::Col::I64 { v: &e } },
        pq::Field { name: "revenue_cents", col: pq::Col::I64 { v: &r } },
        pq::Field { name: "top_category", col: pq::Col::DictStr { ids: &cats, dict: &catdict, optional: true } },
    ];
    pq::write(&ud.join("part-000.parquet"), &pq::encode(&fields, day.len()))?;
    let mut offs = vec![0u32];
    let mut cid = Vec::new();
    let mut chs = Vec::new();
    let mut day = Vec::new();
    let (mut a, mut b, mut c2, mut d2) = (Vec::new(), Vec::new(), Vec::new(), Vec::new());
    let mut chdict: Vec<Vec<u8>> = campaigns.values().map(|x| x.to_vec()).collect();
    chdict.push(b"organic".to_vec());
    chdict.sort();
    chdict.dedup();
    for ((camp, d), v) in &cdaily {
        cid.extend_from_slice(camp);
        offs.push(cid.len() as u32);
        let ch: &[u8] = if camp == b"organic" { b"organic" } else { campaigns[camp.as_slice()] };
        chs.push(chdict.binary_search_by(|x| x.as_slice().cmp(ch)).unwrap() as u32 + 1);
        day.push(*d as i32);
        a.push(v.0);
        b.push(v.1);
        c2.push(v.2);
        d2.push(v.3);
    }
    let fields = [
        pq::Field { name: "campaign_id", col: pq::Col::Str { offs: &offs, data: &cid } },
        pq::Field { name: "channel", col: pq::Col::DictStr { ids: &chs, dict: &chdict, optional: false } },
        pq::Field { name: "day", col: pq::Col::I32 { v: &day, date: true } },
        pq::Field { name: "sessions", col: pq::Col::I64 { v: &a } },
        pq::Field { name: "purchasing_sessions", col: pq::Col::I64 { v: &b } },
        pq::Field { name: "revenue_cents", col: pq::Col::I64 { v: &c2 } },
        pq::Field { name: "assisted_sessions", col: pq::Col::I64 { v: &d2 } },
    ];
    pq::write(&cd.join("part-000.parquet"), &pq::encode(&fields, day.len()))?;
    Ok(())
}
