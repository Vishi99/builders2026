mod dims;
mod generic;
mod ids;
mod links;
mod pq;
mod prof;
mod sess;
mod stream;
mod sys;
mod text;
mod ts;

use dims::Dims;
use ids::{event_key, id_key, user_key, LongIds, LongTable};
use links::{Forest, KRec, KeyPart, EREF_SHIFT};
use sess::{Click, Out, URow, DAY, WEEK};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use stream::{Pool, Stream};
use sys::{Barrier, Mapped, Racy};
use ts::Clock;

extern "C" {
    fn _exit(code: i32) -> !;
}

// ---------------------------------------------------------------- records

/// A parsed event copy, keyed for dedup. `ts` is ms since window start with
/// bit 31 marking a purchase, or INVALID. `ingest` is ms since window start
/// minus 7 days.
#[repr(C)]
#[derive(Clone, Copy)]
struct EvRec {
    ekey: u64,
    ukey: u64,
    ts: u32,
    ingest: u32,
}

#[repr(C)]
#[derive(Clone, Copy)]
struct PurRec {
    ekey: u64,
    ingest: u32,
    pinfo: u32,
}

/// A surviving event: user (or person) key, ts, purchase info. `ts == DEAD`
/// marks a record superseded or moved.
#[repr(C)]
#[derive(Clone, Copy)]
struct WinRec {
    key: u64,
    ts: u32,
    pinfo: u32,
}

#[repr(C)]
#[derive(Clone, Copy)]
struct ClkRec {
    ckey: u64,
    key: u64,
    ts: u32,
    camp: u32,
}

#[repr(C)]
#[derive(Clone, Copy)]
struct MapRec {
    ukey: u64,
    pkey: u64,
}

const INVALID: u32 = u32::MAX;
const DEAD: u32 = u32::MAX;
static mut NOEMIT: bool = false;
static SINK: AtomicUsize = AtomicUsize::new(0);
const PURCHASE: u32 = 1 << 31;

// ------------------------------------------------------------------ input

#[derive(Clone, Copy, PartialEq, Eq)]
enum Kind {
    Events,
    Clicks,
    Links,
}

struct Chunk {
    kind: Kind,
    ptr: *const u8,
    len: usize,
    _tail: Option<Box<[u8]>>,
    /// Whether the start / end is a nominal offset to move to a row start.
    lazy: (bool, bool),
}

/// First row start at or after `p`: `p` itself when the byte before is a
/// newline, else just past the next newline.
unsafe fn row_start(p: *const u8) -> *const u8 {
    let mut q = p.sub(1);
    while *q != b'\n' {
        q = q.add(1);
    }
    q.add(1)
}
unsafe impl Send for Chunk {}
unsafe impl Sync for Chunk {}

fn list_csv(dir: &Path) -> Vec<PathBuf> {
    let mut v: Vec<PathBuf> = match std::fs::read_dir(dir) {
        Ok(rd) => rd
            .filter_map(|e| e.ok())
            .filter(|e| {
                let name = e.file_name();
                let name = name.to_string_lossy();
                name.ends_with(".csv")
                    && !name.starts_with('.')
                    && e.file_type().map(|t| t.is_file() || t.is_symlink()).unwrap_or(false)
            })
            .map(|e| e.path())
            .collect(),
        Err(_) => Vec::new(),
    };
    v.sort();
    v
}

const PAD: usize = 512;

fn chunk_file(kind: Kind, m: &Mapped, target: usize, out: &mut Vec<Chunk>) {
    let b = m.bytes();
    let len = b.len();
    let header_end = match b.iter().position(|&c| c == b'\n') {
        Some(i) => i + 1,
        None => return,
    };
    if header_end >= len {
        return;
    }
    let tail_start = if len - header_end < 4 * PAD {
        header_end
    } else {
        let limit = len - PAD;
        match b[header_end..limit].iter().rposition(|&c| c == b'\n') {
            Some(i) => header_end + i + 1,
            None => header_end,
        }
    };
    // Nominal boundaries; the worker moves each one to the next row start
    // (both neighbours apply the same rule), so no page is touched here.
    let mut start = header_end;
    while start < tail_start {
        let end = (start + target).min(tail_start);
        out.push(Chunk { kind, ptr: unsafe { m.ptr.add(start) }, len: end - start, _tail: None, lazy: (start != header_end, end != tail_start) });
        start = end;
    }
    if tail_start < len {
        let mut t = b[tail_start..].to_vec();
        if *t.last().unwrap() != b'\n' {
            t.push(b'\n');
        }
        let n = t.len();
        t.resize(n + PAD, 0);
        let t = t.into_boxed_slice();
        out.push(Chunk { kind, ptr: t.as_ptr(), len: n, _tail: Some(t), lazy: (false, false) });
    }
}

// ---------------------------------------------------------------- hashing

#[inline(always)]
fn part(k: u64, mul: u64, bits: u32) -> usize {
    (k.wrapping_mul(mul) >> (64 - bits)) as usize
}
const MUL_D: u64 = 0x9E37_79B9_7F4A_7C15;
const MUL_U: u64 = 0xC2B2_AE3D_27D4_EB4F;
const MUL_L: u64 = 0x1656_67B1_9E37_79F9;
const MUL_T: u64 = 0xFF51_AFD7_ED55_8CCD;
const MUL_G: u64 = 0xC4CE_B9FE_1A85_EC53;

// ----------------------------------------------------------- thread state

struct Local {
    pool: Pool,
    masks: text::Masks2,
    longs: LongIds,
    ev: Vec<Stream>,
    pur: Vec<Stream>,
    clk: Vec<Stream>,
    key: Vec<Stream>,
    nedges: usize,
    ends: Vec<u32>,
    map: Vec<Stream>,
    win: Vec<Stream>,
    per: Vec<Stream>,
    pclk: Vec<Stream>,
    out: Option<Out>,
    longmins: Vec<(u32, u64)>,
    exotic: bool,
}

struct Shared {
    nt: usize,
    chunks: Vec<Chunk>,
    dims: Dims,
    ev_clock: Clock,
    ws: i64,
    we: i64,
    bits_d: u32,
    bits_u: u32,
    bits_l: u32,
    locals: Vec<Racy<Local>>,
    keyparts: Vec<Racy<KeyPart>>,
    forest: Racy<Option<Forest>>,
    longtab: Racy<Option<LongTable>>,
    counters: Vec<AtomicUsize>,
    barrier: Barrier,
    lbar: Barrier,
    nlink: usize,
    bits_long: u32,
    ranges: Vec<(usize, usize)>,
    exotic: AtomicBool,
    times: Racy<Vec<(&'static str, f64)>>,
    t0: std::time::Instant,
    order_u: Racy<Vec<usize>>,
    outdir: PathBuf,
}

impl Shared {
    #[allow(clippy::mut_from_ref)]
    fn local(&self, t: usize) -> &mut Local {
        unsafe { self.locals[t].get() }
    }
    fn claim(&self, c: usize) -> usize {
        self.counters[c].fetch_add(1, Ordering::Relaxed)
    }
    fn lap(&self, tid: usize, name: &'static str) {
        if tid == 0 {
            unsafe { self.times.get() }.push((name, self.t0.elapsed().as_secs_f64()));
        }
    }
}

// ------------------------------------------------------------------ parse

#[inline(always)]
unsafe fn load32(p: *const u8) -> u32 {
    std::ptr::read_unaligned(p as *const u32)
}
#[inline(always)]
unsafe fn load64(p: *const u8) -> u64 {
    std::ptr::read_unaligned(p as *const u64)
}

/// Purchase price: digits only, 1..=10,000,000.
#[inline(always)]
unsafe fn price(p: *const u8, l: usize) -> Option<u32> {
    if l == 0 || l > 8 {
        return price_slow(p, l);
    }
    let mut v = 0u32;
    for i in 0..l {
        let d = (*p.add(i)).wrapping_sub(b'0');
        if d > 9 {
            return None;
        }
        v = v * 10 + d as u32;
    }
    if (1..=10_000_000).contains(&v) {
        Some(v)
    } else {
        None
    }
}

#[cold]
unsafe fn price_slow(p: *const u8, l: usize) -> Option<u32> {
    let s = std::slice::from_raw_parts(p, l);
    if s.is_empty() || !s.iter().all(|c| c.is_ascii_digit()) {
        return None;
    }
    let s = &s[s.iter().position(|&c| c != b'0').unwrap_or(s.len())..];
    if s.len() > 8 {
        return None;
    }
    let mut v = 0u32;
    for &c in s {
        v = v * 10 + (c - b'0') as u32;
    }
    if (1..=10_000_000).contains(&v) {
        Some(v)
    } else {
        None
    }
}

const VIEW: u32 = u32::from_le_bytes(*b"view");
const CART: u32 = u32::from_le_bytes(*b"cart");
const PURCHASE_W: u64 = u64::from_le_bytes(*b"purchase");

unsafe fn parse_events(sh: &Shared, lo: &mut Local, base: *const u8, len: usize) {
    let ws = sh.ws;
    let we = sh.we;
    let ibase = ws - WEEK as i64;
    let clock = &sh.ev_clock;
    let dims = &sh.dims;
    let bits_d = sh.bits_d;
    let Local { pool, masks, longs, ev, pur, exotic, .. } = lo;
    let mut acc = 0u64;
    text::scan_rows2::<7>(base, len, masks, |row, st, en| {
        let fp = |i: usize| row.add(st[i] as usize);
        let fl = |i: usize| (en[i] - st[i]) as usize;
        let l0 = fl(0);
        if l0 == 0 {
            return;
        }
        let ekey = event_key(fp(0), l0, longs);
        let (p3, l3) = (fp(3), fl(3));
        let ing = if l3 == 20 { ts::parse_f12_simd(p3, 20, clock) } else { ts::parse_ts(p3, l3, clock) };
        let ing = match ing {
            Some(v) => v,
            None => {
                // ingest_ts is always valid F1 by the rules.
                *exotic = true;
                return;
            }
        };
        let irel = ing - ibase;
        if !(0..u32::MAX as i64).contains(&irel) {
            *exotic = true;
            return;
        }
        let irel = irel as u32;
        let d = part(ekey, MUL_D, bits_d);
        let l1 = fl(1);
        let l4 = fl(4);
        let p4 = fp(4);
        let w4 = load32(p4);
        let is_purchase = l4 == 8 && load64(p4) == PURCHASE_W;
        let type_ok = (l4 == 4 && (w4 == VIEW || w4 == CART)) || is_purchase;
        let mut valid = l1 > 0 && type_ok && fl(5) > 0;
        let mut t = 0i64;
        if valid {
            match ts::parse_ts_fast(fp(2), fl(2), clock) {
                Some(v) if v >= ws && v < we && ing - v <= DAY => t = v,
                _ => valid = false,
            }
        }
        let mut pinfo = 0u32;
        if valid && is_purchase {
            match price(fp(6), fl(6)) {
                Some(pr) => {
                    let cat = dims.category(std::slice::from_raw_parts(fp(5), fl(5)));
                    if cat > 255 {
                        *exotic = true;
                    }
                    pinfo = pr | (cat << 24);
                }
                None => valid = false,
            }
        }
        if NOEMIT {
            let ukey = if valid { user_key(fp(1), l1, longs) } else { 0 };
            acc = acc.wrapping_add(ekey ^ ukey ^ t as u64 ^ pinfo as u64 ^ d as u64);
            return;
        }
        if valid {
            let ukey = user_key(fp(1), l1, longs);
            let mut tr = (t - ws) as u32;
            if is_purchase {
                tr |= PURCHASE;
                pur.get_unchecked_mut(d).push_small(pool, PurRec { ekey, ingest: irel, pinfo });
            }
            ev.get_unchecked_mut(d).push(pool, EvRec { ekey, ukey, ts: tr, ingest: irel });
        } else {
            ev.get_unchecked_mut(d).push(pool, EvRec { ekey, ukey: 0, ts: INVALID, ingest: irel });
        }
    });
    if NOEMIT {
        SINK.fetch_add(acc as usize, Ordering::Relaxed);
    }
}

unsafe fn parse_clicks(sh: &Shared, lo: &mut Local, base: *const u8, len: usize) {
    let cbase = sh.ws - WEEK as i64;
    let we = sh.we;
    let clock = &sh.ev_clock;
    let dims = &sh.dims;
    let bits_u = sh.bits_u;
    let Local { pool, masks, longs, clk, .. } = lo;
    text::scan_rows2::<4>(base, len, masks, |row, st, en| {
        let fp = |i: usize| row.add(st[i] as usize);
        let fl = |i: usize| (en[i] - st[i]) as usize;
        let l0 = fl(0);
        let l1 = fl(1);
        if l0 == 0 || l1 == 0 {
            return;
        }
        let camp = dims.campaign(std::slice::from_raw_parts(fp(2), fl(2)));
        if camp == 0 {
            return;
        }
        let t = match ts::parse_ts_fast(fp(3), fl(3), clock) {
            Some(v) if v >= cbase && v < we => v,
            _ => return,
        };
        let ckey = id_key(fp(0), l0, longs);
        let ukey = user_key(fp(1), l1, longs);
        let u = part(ukey, MUL_U, bits_u);
        clk.get_unchecked_mut(u).push(pool, ClkRec { ckey, key: ukey, ts: (t - cbase) as u32, camp });
    });
}

unsafe fn parse_links(sh: &Shared, tid: usize, lo: &mut Local, base: *const u8, len: usize) {
    let bits_l = sh.bits_l;
    let Local { pool, masks, longs, key, nedges, .. } = lo;
    text::scan_rows2::<2>(base, len, masks, |row, st, en| {
        let l0 = (en[0] - st[0]) as usize;
        let l1 = (en[1] - st[1]) as usize;
        if l0 == 0 || l1 == 0 {
            return;
        }
        let ka = user_key(row.add(st[0] as usize), l0, longs);
        let kb = user_key(row.add(st[1] as usize), l1, longs);
        if ka == kb {
            return;
        }
        let e = *nedges as u32;
        *nedges += 1;
        let r = ((tid as u32) << EREF_SHIFT) | (e << 1);
        key.get_unchecked_mut(part(ka, MUL_L, bits_l)).push_small(pool, KRec { key: ka, eref: r, _pad: 0 });
        key.get_unchecked_mut(part(kb, MUL_L, bits_l)).push_small(pool, KRec { key: kb, eref: r | 1, _pad: 0 });
    });
}

// ----------------------------------------------------------------- phases

fn phase_parse(sh: &Shared, tid: usize) {
    loop {
        let i = sh.claim(0);
        if i >= sh.chunks.len() {
            break;
        }
        let c = &sh.chunks[i];
        let lo = sh.local(tid);
        unsafe {
            let a = if c.lazy.0 { row_start(c.ptr) } else { c.ptr };
            let b = if c.lazy.1 { row_start(c.ptr.add(c.len)) } else { c.ptr.add(c.len) };
            if b <= a {
                continue;
            }
            let (p, n) = (a, b as usize - a as usize);
            match c.kind {
                Kind::Events => parse_events(sh, lo, p, n),
                Kind::Clicks => parse_clicks(sh, lo, p, n),
                Kind::Links => parse_links(sh, tid, lo, p, n),
            }
        }
    }
    let lo = sh.local(tid);
    for s in lo.ev.iter_mut().chain(lo.pur.iter_mut()).chain(lo.clk.iter_mut()).chain(lo.key.iter_mut()) {
        s.finish();
    }
    lo.longs.finish();
    if lo.exotic {
        sh.exotic.store(true, Ordering::Relaxed);
    }
    lo.ends = vec![0u32; 2 * lo.nedges];
}

/// Merge every thread's long-ID sightings per partition, comparing bytes of
/// spellings that share a key (a shared key sends the run to the generic
/// pipeline).
fn phase_longs(sh: &Shared, tid: usize) {
    let nt = sh.nt;
    let nd = 1usize << sh.bits_long;
    if tid == 0 {
        let any = (0..nt).any(|t| sh.local(t).longs.parts.iter().any(|v| v.count != 0));
        let parts = (0..nd).map(|_| ids::LongPart::default()).collect();
        unsafe { *sh.longtab.get() = Some(LongTable { parts, bits: sh.bits_long, any }) };
    }
    sh.lbar.wait();
    let tab = unsafe { sh.longtab.get().as_mut().unwrap() } as *mut LongTable;
    let mut scratch = Vec::new();
    let mut gens = Vec::new();
    let mut gen = 0u32;
    loop {
        let p = sh.claim(8);
        if p >= 2 * nd {
            break;
        }
        if p >= nd {
            let recs: Vec<&Stream> = (0..nt).map(|t| &sh.local(t).longs.evparts[p - nd]).collect();
            gen += 1;
            if !ids::check_events(&recs, &mut scratch, &mut gens, gen) {
                sh.exotic.store(true, Ordering::Relaxed);
            }
            continue;
        }
        let recs: Vec<&Stream> = (0..nt).map(|t| &sh.local(t).longs.parts[p]).collect();
        let (lp, ok) = ids::LongPart::build(&recs);
        if !ok {
            sh.exotic.store(true, Ordering::Relaxed);
        }
        unsafe { *(*tab).parts.as_mut_ptr().add(p) = lp };
    }
}

/// Linked IDs to canonical keys; ends with map records in user partitions.
fn phase_links(sh: &Shared, tid: usize) -> bool {
    let nt = sh.nt;
    let nl = 1usize << sh.bits_l;
    loop {
        let p = sh.claim(1);
        if p >= nl {
            break;
        }
        let n: usize = (0..nt).map(|t| sh.local(t).key[p].count).sum();
        let mut keys = Vec::with_capacity(n);
        for t in 0..nt {
            sh.local(t).key[p].for_each_block::<KRec>(|b| keys.extend(b.iter().map(|r| r.key)));
        }
        let kp = KeyPart::build(&mut keys.into_iter(), n);
        unsafe { *sh.keyparts[p].get() = kp };
    }
    sh.lbar.wait();
    if tid == 0 {
        let mut base = 0u32;
        for p in 0..nl {
            let kp = unsafe { sh.keyparts[p].get() };
            kp.base = base;
            base += kp.keys.len() as u32;
        }
        unsafe { *sh.forest.get() = Some(Forest::new(base as usize)) };
    }
    sh.lbar.wait();
    loop {
        let p = sh.claim(2);
        if p >= nl {
            break;
        }
        let kp = unsafe { sh.keyparts[p].get() };
        for t in 0..nt {
            sh.local(t).key[p].for_each_block::<KRec>(|b| {
                for r in b {
                    let owner = (r.eref >> EREF_SHIFT) as usize;
                    let idx = (r.eref & ((1 << EREF_SHIFT) - 1)) as usize;
                    let id = kp.base + kp.find(r.key);
                    unsafe {
                        let ends = &mut sh.local(owner).ends;
                        *ends.as_mut_ptr().add(idx) = id;
                    }
                }
            });
        }
    }
    sh.lbar.wait();
    sh.lap(tid, "l-ends");
    let forest = unsafe { sh.forest.get().as_ref().unwrap() };
    for t in (tid..nt).step_by(sh.nlink) {
        let ends = &sh.local(t).ends;
        for e in ends.chunks_exact(2) {
            forest.union(e[0], e[1]);
        }
    }
    sh.lbar.wait();
    loop {
        let p = sh.claim(3);
        if p >= nl {
            break;
        }
        let kp = unsafe { sh.keyparts[p].get() };
        for (i, &k) in kp.keys.iter().enumerate() {
            let r = forest.find(kp.base + i as u32);
            if k & ids::LONG == 0 {
                forest.minkey[r as usize].fetch_min(k, Ordering::Relaxed);
            } else {
                sh.local(tid).longmins.push((r, k));
            }
        }
    }
    sh.lbar.wait();
    sh.lap(tid, "l-min");
    {
        // Long keys compete by bytes; a CAS loop keeps the smallest.
        let tab = unsafe { sh.longtab.get().as_ref().unwrap() };
        for &(r, k) in &sh.local(tid).longmins {
            let slot = &forest.minkey[r as usize];
            let mut cur = slot.load(Ordering::Relaxed);
            while cur == u64::MAX || tab.cmp(k, cur).is_lt() {
                match slot.compare_exchange_weak(cur, k, Ordering::AcqRel, Ordering::Relaxed) {
                    Ok(_) => break,
                    Err(x) => cur = x,
                }
            }
        }
    }
    sh.lbar.wait();
    sh.lap(tid, "l-longmin");
    loop {
        let p = sh.claim(4);
        if p >= nl {
            break;
        }
        let kp = unsafe { sh.keyparts[p].get() };
        let lo = sh.local(tid);
        for (i, &k) in kp.keys.iter().enumerate() {
            let r = forest.find(kp.base + i as u32);
            let canon = forest.minkey[r as usize].load(Ordering::Relaxed);
            let u = part(k, MUL_U, sh.bits_u);
            lo.map[u].push_small(&mut lo.pool, MapRec { ukey: k, pkey: canon });
        }
    }
    let lo = sh.local(tid);
    for s in lo.map.iter_mut() {
        s.finish();
    }
    true
}

#[repr(C)]
#[derive(Clone, Copy)]
struct DSlot {
    ekey: u64,
    wptr: *mut WinRec,
    ingest: u32,
    gen: u32,
}

/// Dedup partitions: the copy with the smallest ingest per event key. A
/// valid winner is emitted at once to its user partition; when a later
/// record replaces it, the emitted copy is marked DEAD.
fn phase_dedup(sh: &Shared, tid: usize) {
    let nt = sh.nt;
    let nd = 1usize << sh.bits_d;
    let bits_u = sh.bits_u;
    let mut table: Vec<DSlot> = Vec::new();
    let mut side: Vec<PurRec> = Vec::new();
    let mut gen = 0u32;
    loop {
        let d = sh.claim(5);
        if d >= nd {
            break;
        }
        let n: usize = (0..nt).map(|t| sh.local(t).ev[d].count).sum();
        let bits = ((n * 3 / 2).max(16)).next_power_of_two().trailing_zeros();
        let cap = 1usize << bits;
        let mask = cap - 1;
        if table.len() < cap {
            table = vec![DSlot { ekey: 0, wptr: std::ptr::null_mut(), ingest: 0, gen: 0 }; cap];
            gen = 0;
        }
        gen += 1;
        // Purchase payloads by (event key, ingest).
        let np: usize = (0..nt).map(|t| sh.local(t).pur[d].count).sum();
        let pbits = ((np * 2).max(16)).next_power_of_two().trailing_zeros();
        let pcap = 1usize << pbits;
        side.clear();
        side.resize(pcap, PurRec { ekey: 0, ingest: 0, pinfo: 0 });
        for t in 0..nt {
            sh.local(t).pur[d].for_each_block::<PurRec>(|b| {
                for r in b {
                    let mut h = part(r.ekey ^ r.ingest as u64, MUL_G, pbits);
                    loop {
                        let s = unsafe { side.get_unchecked_mut(h) };
                        if s.ekey == 0 {
                            *s = *r;
                            break;
                        }
                        if s.ekey == r.ekey && s.ingest == r.ingest {
                            break;
                        }
                        h = (h + 1) & (pcap - 1);
                    }
                }
            });
        }
        let lo = sh.local(tid) as *mut Local;
        let lo = unsafe { &mut *lo };
        let side_ref = &side;
        let pinfo_of = |ekey: u64, ingest: u32| -> u32 {
            let mut h = part(ekey ^ ingest as u64, MUL_G, pbits);
            loop {
                let e = unsafe { side_ref.get_unchecked(h) };
                if e.ekey == ekey && e.ingest == ingest {
                    return e.pinfo;
                }
                if e.ekey == 0 {
                    return 0;
                }
                h = (h + 1) & (pcap - 1);
            }
        };
        for t in 0..nt {
            let src = sh.local(t).ev[d];
            let tp = table.as_ptr();
            src.for_each_block::<EvRec>(|b| {
                for (j, r) in b.iter().enumerate() {
                    if let Some(f) = b.get(j + 8) {
                        let fh = part(f.ekey, MUL_T, bits);
                        unsafe { std::arch::x86_64::_mm_prefetch(tp.add(fh) as *const i8, std::arch::x86_64::_MM_HINT_T0) };
                    }
                    let mut h = part(r.ekey, MUL_T, bits);
                    loop {
                        let s = unsafe { table.get_unchecked_mut(h) };
                        if s.gen != gen {
                            s.gen = gen;
                            s.ekey = r.ekey;
                            s.ingest = r.ingest;
                            s.wptr = std::ptr::null_mut();
                            if r.ts != INVALID {
                                let pinfo = if r.ts & PURCHASE != 0 { pinfo_of(r.ekey, r.ingest) } else { 0 };
                                let u = part(r.ukey, MUL_U, bits_u);
                                s.wptr = unsafe { lo.win.get_unchecked_mut(u) }
                                    .push(&mut lo.pool, WinRec { key: r.ukey, ts: r.ts & !PURCHASE, pinfo });
                            }
                            break;
                        }
                        if s.ekey == r.ekey {
                            if r.ingest < s.ingest {
                                if !s.wptr.is_null() {
                                    unsafe { (*s.wptr).ts = DEAD };
                                }
                                s.ingest = r.ingest;
                                s.wptr = std::ptr::null_mut();
                                if r.ts != INVALID {
                                    let pinfo = if r.ts & PURCHASE != 0 { pinfo_of(r.ekey, r.ingest) } else { 0 };
                                    let u = part(r.ukey, MUL_U, bits_u);
                                    s.wptr = unsafe { lo.win.get_unchecked_mut(u) }
                                        .push(&mut lo.pool, WinRec { key: r.ukey, ts: r.ts & !PURCHASE, pinfo });
                                }
                            }
                            break;
                        }
                        h = (h + 1) & mask;
                    }
                }
            });
        }
        for t in 0..nt {
            let src: *mut Stream = &mut sh.local(t).ev[d];
            unsafe { (*src).release(&mut lo.pool) };
        }
    }
    let lo = sh.local(tid);
    for s in lo.win.iter_mut() {
        s.finish();
    }
}

/// Map users to persons. Records whose person partition is this partition
/// stay in place with the person key written in; others move.
fn phase_route(sh: &Shared, tid: usize) {
    // Input bytes are no longer needed (long IDs own their bytes): drop this
    // worker's share of the mapping's page-table entries in parallel.
    {
        const PIECE: usize = 64 << 20;
        let mut k = 0usize;
        for &(p, l) in &sh.ranges {
            let mut off = 0;
            while off < l {
                if k % sh.nt == tid {
                    sys::release_range((p + off) as *const u8, PIECE.min(l - off));
                }
                k += 1;
                off += PIECE;
            }
        }
    }
    let nt = sh.nt;
    let nu = 1usize << sh.bits_u;
    let bits_u = sh.bits_u;
    let mut umap_keys: Vec<u64> = Vec::new();
    let mut umap_vals: Vec<u64> = Vec::new();
    loop {
        let u = sh.claim(6);
        if u >= nu {
            break;
        }
        let n: usize = (0..nt).map(|t| sh.local(t).map[u].count).sum();
        let bits = ((n * 2).max(16)).next_power_of_two().trailing_zeros();
        let cap = 1usize << bits;
        umap_keys.clear();
        umap_keys.resize(cap, 0);
        umap_vals.clear();
        umap_vals.resize(cap, 0);
        for t in 0..nt {
            sh.local(t).map[u].for_each_block::<MapRec>(|b| {
                for r in b {
                    let mut h = part(r.ukey, MUL_G, bits);
                    while umap_keys[h] != 0 {
                        h = (h + 1) & (cap - 1);
                    }
                    umap_keys[h] = r.ukey;
                    umap_vals[h] = r.pkey;
                }
            });
        }
        let lookup = |k: u64| -> u64 {
            let mut h = part(k, MUL_G, bits);
            loop {
                let x = unsafe { *umap_keys.get_unchecked(h) };
                if x == k {
                    return unsafe { *umap_vals.get_unchecked(h) };
                }
                if x == 0 {
                    return k;
                }
                h = (h + 1) & (cap - 1);
            }
        };
        let lo = sh.local(tid) as *mut Local;
        let lo = unsafe { &mut *lo };
        for t in 0..nt {
            let src = sh.local(t).win[u];
            src.for_each_block_mut::<WinRec>(|b| {
                for r in b {
                    if r.ts == DEAD {
                        continue;
                    }
                    let pk = lookup(r.key);
                    let q = part(pk, MUL_U, bits_u);
                    if q == u {
                        r.key = pk;
                    } else {
                        unsafe { lo.per.get_unchecked_mut(q) }.push(&mut lo.pool, WinRec { key: pk, ..*r });
                        r.ts = DEAD;
                    }
                }
            });
            let src = sh.local(t).clk[u];
            src.for_each_block_mut::<ClkRec>(|b| {
                for r in b {
                    let pk = lookup(r.key);
                    let q = part(pk, MUL_U, bits_u);
                    if q == u {
                        r.key = pk;
                    } else {
                        unsafe { lo.pclk.get_unchecked_mut(q) }.push(&mut lo.pool, ClkRec { key: pk, ..*r });
                        r.ts = DEAD;
                    }
                }
            });
        }
    }
    let lo = sh.local(tid);
    for s in lo.per.iter_mut().chain(lo.pclk.iter_mut()) {
        s.finish();
    }
}

#[inline(always)]
fn radix_pass(src: &[u64], dst: &mut [u64], shift: u32) {
    let mut count = [0u32; 2048];
    for &x in src.iter() {
        count[((x >> shift) & 2047) as usize] += 1;
    }
    let mut sum = 0u32;
    for c in count.iter_mut() {
        let t = *c;
        *c = sum;
        sum += t;
    }
    for &x in src.iter() {
        let b = ((x >> shift) & 2047) as usize;
        unsafe { *dst.get_unchecked_mut(count[b] as usize) = x };
        count[b] += 1;
    }
}

#[derive(Clone, Copy, Default)]
#[repr(C)]
struct CSlot {
    pkey: u64,
    min: u32,
    max: u32,
    count: u32,
    phead: u32,
    /// bucket (low 11 bits) | generation << 11
    tag: u32,
    _pad: u32,
}

struct EScratch {
    occ: Vec<u32>,
    keys: Vec<u64>,
    tmp: Vec<u64>,
    slots: Vec<u32>,
    cslot: Vec<u32>,
    cells: Vec<sess::Cell>,
    cpkey: Vec<u64>,
    cbucket: Vec<u32>,
    sorted: Vec<sess::Cell>,
    purch: Vec<(u32, u32)>,
    gkeys: Vec<u64>,
    gcl: Vec<u32>,
    clicks: Vec<(u32, Click)>,
    cbuf: Vec<Click>,
}

#[inline(always)]
fn gid_insert(k: u64, bits: u32, slots: &mut [u32], gkeys: &mut Vec<u64>) -> u32 {
    let mask = slots.len() - 1;
    let mut h = part(k, MUL_G, bits);
    loop {
        let s = unsafe { *slots.get_unchecked(h) };
        if s == 0 {
            gkeys.push(k);
            unsafe { *slots.get_unchecked_mut(h) = gkeys.len() as u32 };
            return gkeys.len() as u32 - 1;
        }
        if unsafe { *gkeys.get_unchecked((s - 1) as usize) } == k {
            return s - 1;
        }
        h = (h + 1) & mask;
    }
}

#[inline(always)]
fn gid_find(k: u64, bits: u32, slots: &[u32], gkeys: &[u64]) -> u32 {
    let mask = slots.len() - 1;
    let mut h = part(k, MUL_G, bits);
    loop {
        let s = unsafe { *slots.get_unchecked(h) };
        if s == 0 {
            return u32::MAX;
        }
        if unsafe { *gkeys.get_unchecked((s - 1) as usize) } == k {
            return s - 1;
        }
        h = (h + 1) & mask;
    }
}

const BUCKET: u32 = 1_800_000;

/// Sessions per person partition. Events are folded into (person, 30-min
/// bucket) cells; all events of one cell share a session. Cells are sorted
/// by (person, bucket) and merged into sessions.
fn phase_sessions(sh: &Shared, tid: usize) {
    let nt = sh.nt;
    let nu = 1usize << sh.bits_u;
    let longtab = unsafe { sh.longtab.get().as_ref().unwrap() };
    let order = unsafe { sh.order_u.get() };
    let out = sh.local(tid).out.as_mut().unwrap() as *mut Out;
    let out = unsafe { &mut *out };
    let mut x = EScratch {
        occ: Vec::new(),
        keys: Vec::new(),
        tmp: Vec::new(),
        slots: Vec::new(),
        cslot: Vec::new(),
        cells: Vec::new(),
        cpkey: Vec::new(),
        cbucket: Vec::new(),
        sorted: Vec::new(),
        purch: Vec::new(),
        gkeys: Vec::new(),
        gcl: Vec::new(),
        clicks: Vec::new(),
        cbuf: Vec::new(),
    };
    let verbose = std::env::var("SS_TIMING").is_ok();
    let mut tm = [0f64; 6];
    let tstart = sys::now();
    let mut ctab: Vec<CSlot> = Vec::new();
    let mut cgen = 0u32;
    loop {
        let i = sh.claim(7);
        if i >= nu {
            break;
        }
        let q = order[i];
        let ta = sys::now();
        let n: usize = (0..nt).map(|t| sh.local(t).win[q].count + sh.local(t).per[q].count).sum();
        if n == 0 {
            continue;
        }
        // Cells keyed by (person key, bucket): generation-tagged slots.
        let cbits = ((n / 2).max(16)).next_power_of_two().trailing_zeros() + 1;
        let ccap = 1usize << cbits;
        if ctab.len() < ccap || cgen >= (1 << 20) - 1 {
            ctab = vec![CSlot::default(); ccap.max(ctab.len())];
            cgen = 0;
        }
        cgen += 1;
        let gtag = cgen << 11;
        x.occ.clear();
        x.purch.clear();
        {
            let ctab_p = ctab.as_mut_ptr();
            let occ = &mut x.occ;
            let purch = &mut x.purch;
            let mut visit = |r: &WinRec| {
                let bucket = r.ts / BUCKET;
                let tb = bucket | gtag;
                let mut h = part(r.key ^ (bucket as u64).wrapping_mul(0x9E37_79B9), MUL_G, cbits);
                loop {
                    let c = unsafe { &mut *ctab_p.add(h) };
                    if c.tag >> 11 != cgen {
                        *c = CSlot { pkey: r.key, min: r.ts, max: r.ts, count: 1, phead: 0, tag: tb, _pad: 0 };
                        occ.push(h as u32);
                        if r.pinfo != 0 {
                            purch.push((0, r.pinfo));
                            c.phead = purch.len() as u32;
                        }
                        break;
                    }
                    if c.pkey == r.key && c.tag == tb {
                        c.min = c.min.min(r.ts);
                        c.max = c.max.max(r.ts);
                        c.count += 1;
                        if r.pinfo != 0 {
                            purch.push((c.phead, r.pinfo));
                            c.phead = purch.len() as u32;
                        }
                        break;
                    }
                    h = (h + 1) & (ccap - 1);
                }
            };
            for t in 0..nt {
                for s in [&sh.local(t).win[q], &sh.local(t).per[q]] {
                    s.for_each_block::<WinRec>(|b| {
                        const AHEAD: usize = 8;
                        for (j, r) in b.iter().enumerate() {
                            if let Some(f) = b.get(j + AHEAD) {
                                let fb = f.ts / BUCKET;
                                let fh = part(f.key ^ (fb as u64).wrapping_mul(0x9E37_79B9), MUL_G, cbits);
                                unsafe {
                                    std::arch::x86_64::_mm_prefetch(ctab_p.add(fh) as *const i8, std::arch::x86_64::_MM_HINT_T0)
                                };
                            }
                            if r.ts != DEAD {
                                visit(r);
                            }
                        }
                    });
                }
            }
        }
        let tb = sys::now();
        // Person ids for cells, then sort cells by (gid, bucket).
        let nc = x.occ.len();
        let gbits = ((nc * 2).max(16)).next_power_of_two().trailing_zeros();
        x.slots.clear();
        x.slots.resize(1 << gbits, 0);
        x.gkeys.clear();
        x.keys.clear();
        for &h in x.occ.iter() {
            let c = &ctab[h as usize];
            let g = gid_insert(c.pkey, gbits, &mut x.slots, &mut x.gkeys) as u64;
            x.keys.push((g << 43) | (((c.tag & 2047) as u64) << 32) | h as u64);
        }
        let ng = x.gkeys.len();
        let gb = (64 - ((ng as u64).max(2) - 1).leading_zeros()).max(1);
        let total_bits = 11 + gb;
        let passes = total_bits.div_ceil(11);
        x.tmp.clear();
        x.tmp.resize(nc, 0);
        let mut in_keys = true;
        for p in 0..passes {
            let shift = 32 + 11 * p;
            if in_keys {
                radix_pass(&x.keys, &mut x.tmp, shift);
            } else {
                radix_pass(&x.tmp, &mut x.keys, shift);
            }
            in_keys = !in_keys;
        }
        if !in_keys {
            std::mem::swap(&mut x.keys, &mut x.tmp);
        }
        x.sorted.clear();
        x.sorted.extend(x.keys.iter().map(|&k| {
            let c = &ctab[(k & 0xffff_ffff) as usize];
            sess::Cell { min: c.min, max: c.max, count: c.count, phead: c.phead }
        }));
        let tc = sys::now();
        // Clicks grouped by gid (persons without events dropped).
        x.clicks.clear();
        for t in 0..nt {
            for s in [&sh.local(t).clk[q], &sh.local(t).pclk[q]] {
                s.for_each_block::<ClkRec>(|b| {
                    for r in b {
                        if r.ts == DEAD {
                            continue;
                        }
                        let g = gid_find(r.key, gbits, &x.slots, &x.gkeys);
                        if g != u32::MAX {
                            x.clicks.push((g, Click { ckey: r.ckey, ts: r.ts, camp: r.camp }));
                        }
                    }
                });
            }
        }
        x.gcl.clear();
        x.gcl.resize(ng + 1, 0);
        for c in &x.clicks {
            x.gcl[c.0 as usize + 1] += 1;
        }
        for g in 0..ng {
            x.gcl[g + 1] += x.gcl[g];
        }
        {
            let mut pos: Vec<u32> = x.gcl[..ng].to_vec();
            x.cbuf.clear();
            x.cbuf.resize(x.clicks.len(), Click { ckey: 0, ts: 0, camp: 0 });
            for c in &x.clicks {
                let p = &mut pos[c.0 as usize];
                x.cbuf[*p as usize] = c.1;
                *p += 1;
            }
        }
        let td = sys::now();
        // Walk persons.
        let mut a = 0usize;
        while a < nc {
            let g = (x.keys[a] >> 43) as usize;
            let mut b = a + 1;
            while b < nc && (x.keys[b] >> 43) as usize == g {
                b += 1;
            }
            let (ca, cb) = (x.gcl[g] as usize, x.gcl[g + 1] as usize);
            let cl = &mut x.cbuf[ca..cb];
            let ncl = sess::sort_clicks_slice(cl, longtab);
            out.person_cells(x.gkeys[g], &x.sorted[a..b], &x.purch, &cl[..ncl]);
            a = b;
        }
        let te = sys::now();
        tm[0] += (tb - ta).as_secs_f64();
        tm[1] += (tc - tb).as_secs_f64();
        tm[2] += (td - tc).as_secs_f64();
        tm[3] += (te - td).as_secs_f64();
    }
    if verbose {
        eprintln!(
            "E t{tid}: total {:.1} cells {:.1} sort {:.1} clicks {:.1} persons {:.1}",
            tstart.elapsed().as_secs_f64() * 1e3,
            tm[0] * 1e3,
            tm[1] * 1e3,
            tm[2] * 1e3,
            tm[3] * 1e3
        );
    }
}

fn rusage() -> (f64, f64, i64) {
    #[repr(C)]
    struct Timeval { s: i64, us: i64 }
    #[repr(C)]
    struct Rusage { utime: Timeval, stime: Timeval, rest: [i64; 14] }
    extern "C" { fn getrusage(who: i32, r: *mut Rusage) -> i32; }
    let mut r = Rusage { utime: Timeval { s: 0, us: 0 }, stime: Timeval { s: 0, us: 0 }, rest: [0; 14] };
    unsafe { getrusage(0, &mut r) };
    (r.utime.s as f64 + r.utime.us as f64 * 1e-6, r.stime.s as f64 + r.stime.us as f64 * 1e-6, r.rest[4])
}

fn prof_from(sh: &Shared, tid: usize, name: &str) {
    if std::env::var("SS_PROF_FROM").map_or(false, |v| v == name) {
        if tid == 0 {
            prof::start();
        }
        sh.barrier.wait();
    }
}

fn stop_after(sh: &Shared, tid: usize, name: &str) {
    if std::env::var("SS_STOP").map_or(false, |v| v == name) {
        sh.barrier.wait();
        if tid == 0 {
            let mut prev = 0.0;
            for (n, t) in unsafe { sh.times.get() }.iter() {
                eprintln!("{n:>10}: {:8.2} ms (+{:7.2})", t * 1e3, (t - prev) * 1e3);
                prev = *t;
            }
            let (u, s, f) = rusage();
            eprintln!("rusage user {u:.2}s sys {s:.2}s minflt {f}");
            if let Ok(t) = std::fs::read_to_string("/proc/self/smaps_rollup") {
                for l in t.lines().filter(|l| l.starts_with("Rss") || l.starts_with("AnonHuge") || l.starts_with("Anonymous")) {
                    eprintln!("  {l}");
                }
            }
            if let Ok(p) = std::env::var("SS_PROF") {
                prof::dump(&p);
            }
            unsafe { _exit(0) };
        }
        loop {
            std::thread::park();
        }
    }
}

fn worker(sh: &Shared, tid: usize) {
    let nu = 1usize << sh.bits_u;
    phase_parse(sh, tid);
    sh.barrier.wait();
    sh.lap(tid, "parse");
    stop_after(sh, tid, "parse");
    if sh.exotic.load(Ordering::Relaxed) {
        return;
    }
    // Link threads resolve long IDs and linked IDs while the others start
    // deduplicating; link threads then join the dedup work.
    if tid < sh.nlink {
        phase_longs(sh, tid);
        sh.lbar.wait();
        sh.lap(tid, "longs");
        phase_links(sh, tid);
        sh.lap(tid, "links");
    }
    phase_dedup(sh, tid);
    sh.barrier.wait();
    sh.lap(tid, "dedup");
    stop_after(sh, tid, "dedup");
    prof_from(sh, tid, "route");
    phase_route(sh, tid);
    sh.barrier.wait();
    if tid == 0 {
        let nt = sh.nt;
        let mut order: Vec<(usize, usize)> = (0..nu)
            .map(|q| ((0..nt).map(|t| sh.local(t).per[q].count + sh.local(t).win[q].count).sum::<usize>(), q))
            .collect();
        order.sort_unstable_by(|a, b| b.cmp(a));
        unsafe { *sh.order_u.get() = order.into_iter().map(|x| x.1).collect() };
    }
    sh.barrier.wait();
    sh.lap(tid, "route");
    stop_after(sh, tid, "route");
    prof_from(sh, tid, "sessions");
    phase_sessions(sh, tid);
    sh.barrier.wait();
    sh.lap(tid, "sessions");
    stop_after(sh, tid, "sessions");
    prof_from(sh, tid, "write");
    if let Err(e) = phase_write(sh, tid, &sh.outdir) {
        eprintln!("write failed: {e}");
        sh.exotic.store(true, Ordering::Relaxed);
    }
    sh.barrier.wait();
    sh.lap(tid, "write");
}

// ----------------------------------------------------------------- output

include!("write.rs");

// ------------------------------------------------------------------- main

fn bits_for(n: usize, per: usize, lo: u32, hi: u32) -> u32 {
    // Nearest power of two to n / per.
    let x = (n / per).max(1);
    let up = x.next_power_of_two();
    let p = if up - x > x - up / 2 { up / 2 } else { up };
    (p.max(1).trailing_zeros()).clamp(lo, hi)
}

fn main() {
    let t0 = sys::now();
    let data = PathBuf::from(std::env::var("DATA_DIR").unwrap_or_else(|_| "/data".into()));
    let outdir = PathBuf::from(std::env::var("OUTPUT_DIR").unwrap_or_else(|_| "/output".into()));
    let nt: usize = std::env::var("NCPU")
        .ok()
        .and_then(|s| s.trim().parse().ok())
        .filter(|&n: &usize| n > 0)
        .unwrap_or_else(|| std::thread::available_parallelism().map(|n| n.get()).unwrap_or(8))
        .min(31);
    let verbose = std::env::var("SS_TIMING").is_ok();
    let prof_path = std::env::var("SS_PROF").ok();
    if prof_path.is_some() && std::env::var("SS_PROF_FROM").is_err() {
        prof::start();
    }
    if std::env::var("SS_NOEMIT").is_ok() {
        unsafe { NOEMIT = true };
    }
    if std::env::var("SS_NOATTR").is_ok() {
        unsafe { sess::NOATTR = true };
    }
    if std::env::var("SS_NOASSIST").is_ok() {
        unsafe { sess::NOASSIST = true };
    }
    if std::env::var("SS_HEAVY").is_ok() {
        unsafe { sess::HEAVY = true };
    }
    if std::env::var("SS_GENERIC").is_ok() {
        generic::run(&data, &outdir).expect("generic pipeline");
        unsafe { _exit(0) };
    }

    let (ws, we) = dims::manifest(&data).expect("manifest");
    let dims_dir = data.clone();
    let dims_job = std::thread::spawn(move || Dims::load(&dims_dir));
    if we - ws > 24 * DAY || we <= ws {
        generic::run(&data, &outdir).expect("generic pipeline");
        unsafe { _exit(0) };
    }
    let t_dims = t0.elapsed().as_secs_f64();
    let mut maps = Vec::new();
    let mut chunks = Vec::new();
    let (mut ev_bytes, mut ck_bytes, mut lk_bytes) = (0usize, 0usize, 0usize);
    for (kind, sub, target) in [
        (Kind::Links, "links", 256 << 10),
        (Kind::Clicks, "clicks", 1 << 20),
        (Kind::Events, "events", 1 << 20),
    ] {
        for path in list_csv(&data.join(sub)) {
            let m = Mapped::open(&path).expect("map input");
            match kind {
                Kind::Events => ev_bytes += m.len,
                Kind::Clicks => ck_bytes += m.len,
                Kind::Links => lk_bytes += m.len,
            }
            chunk_file(kind, &m, target, &mut chunks);
            maps.push(m);
        }
    }
    let dims = dims_job.join().expect("dimensions thread").expect("dimensions");
    if dims.categories.len() > 256 {
        generic::run(&data, &outdir).expect("generic pipeline");
        unsafe { _exit(0) };
    }
    let t_map = t0.elapsed().as_secs_f64();
    let ranges: Vec<(usize, usize)> = maps.iter().filter(|m| m.len > 0).map(|m| (m.ptr as usize, m.len)).collect();
    let bits_d = bits_for(ev_bytes / 78, 9000, 4, 14);
    let bits_u = bits_for(ev_bytes / 80 + ck_bytes / 48, 12000, 4, 14);
    let bits_l = bits_for(lk_bytes / 10, 8000, 2, 12);
    let ncamp = dims.campaign_ids.len();
    let day0 = ws.div_euclid(DAY);
    let ndays = ((we - 1).div_euclid(DAY) - day0 + 1) as usize;
    let nd = 1usize << bits_d;
    let nu = 1usize << bits_u;
    // Expected long-ID sightings per thread and registry part (1 in 8 IDs
    // in generated data), with slack; vectors still grow if needed.
    let bits_long = bits_d.saturating_sub(3).max(2);
    let _long_per_part = ((ev_bytes / 80 + ck_bytes / 48 + lk_bytes / 20) * 2 / 8 / nt / 256) * 5 / 4 + 16;
    let nl = 1usize << bits_l;
    let locals = (0..nt)
        .map(|_| {
            Racy::new(Local {
                pool: Pool::new(64 << 20),
                masks: text::Masks2::new(),
                longs: LongIds::new(bits_long),
                ev: vec![Stream::new(); nd],
                pur: vec![Stream::new(); nd],
                clk: vec![Stream::new(); nu],
                key: vec![Stream::new(); nl],
                nedges: 0,
                ends: Vec::new(),
                map: vec![Stream::new(); nu],
                win: vec![Stream::new(); nu],
                per: vec![Stream::new(); nu],
                pclk: vec![Stream::new(); nu],
                out: Some(Out::new(ncamp, ndays, day0, ws)),
                longmins: Vec::new(),
                exotic: false,
            })
        })
        .collect();
    let nlink = std::env::var("SS_NLINK").ok().and_then(|v| v.parse().ok()).unwrap_or(if nt >= 4 { nt / 2 } else { nt }).clamp(1, nt);
    let sh = Shared {
        nt,
        chunks,
        ev_clock: Clock::new(ws - WEEK as i64 - 2 * DAY, we + 2 * DAY),
        dims,
        ws,
        we,
        bits_d,
        bits_u,
        bits_l,
        locals,
        keyparts: (0..nl).map(|_| Racy::new(KeyPart { slots: Vec::new(), keys: Vec::new(), base: 0 })).collect(),
        forest: Racy::new(None),
        longtab: Racy::new(None),
        counters: (0..16).map(|_| AtomicUsize::new(0)).collect(),
        barrier: Barrier::new(nt),
        lbar: Barrier::new(nlink),
        nlink,
        bits_long,
        ranges,
        exotic: AtomicBool::new(false),
        times: Racy::new(Vec::new()),
        t0,
        order_u: Racy::new(Vec::new()),
        outdir: outdir.clone(),
    };
    sh.lap(0, "setup");
    std::thread::scope(|s| {
        for t in 1..nt {
            let sh = &sh;
            s.spawn(move || worker(sh, t));
        }
        worker(&sh, 0);
    });
    if sh.exotic.load(Ordering::Relaxed) {
        if verbose {
            eprintln!("exotic input: generic pipeline");
        }
        generic::run(&data, &outdir).expect("generic pipeline");
        unsafe { _exit(0) };
    }

    if verbose {
        let (u, s, f) = rusage();
        eprintln!("rusage user {u:.2}s sys {s:.2}s minflt {f}");
        eprintln!("dims {:.2} ms, map+chunk {:.2} ms", t_dims * 1e3, (t_map - t_dims) * 1e3);
        let mut prev = 0.0;
        for (name, t) in unsafe { sh.times.get() }.iter() {
            eprintln!("{name:>10}: {:8.2} ms (+{:7.2})", t * 1e3, (t - prev) * 1e3);
            prev = *t;
        }
        let rows: usize = (0..nt).map(|t| sh.local(t).out.as_ref().unwrap().rows.len()).sum();
        eprintln!("bits d={bits_d} u={bits_u} l={bits_l} user rows={rows}");
    }
    if let Some(p) = prof_path {
        prof::dump(&p);
    }
    unsafe { _exit(0) };
}
