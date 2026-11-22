//! Per-person sessions, attribution, assists and daily aggregation.

use crate::ids::LongTable;

pub const GAP: u32 = 1_800_000;
pub static mut NOATTR: bool = false;
pub static mut NOASSIST: bool = false;
pub static mut HEAVY: bool = false;
pub const WEEK: u32 = 604_800_000;
pub const DAY: i64 = 86_400_000;

#[repr(C)]
#[derive(Clone, Copy)]
pub struct Click {
    pub ckey: u64,
    pub ts: u32,
    pub camp: u32,
}

#[repr(C)]
#[derive(Clone, Copy, Default)]
pub struct URow {
    pub pkey: u64,
    pub revenue: u64,
    pub day: i32,
    pub sessions: u32,
    pub events: u32,
    pub cat: u32,
}

#[derive(Clone, Copy, Default)]
pub struct CampStat {
    pub sessions: u32,
    pub purchasing: u32,
    pub assisted: u32,
    pub revenue: u64,
}

/// Credited totals for one (campaign, day).
#[derive(Clone, Copy, Default)]
pub struct Cred {
    pub sessions: u32,
    pub purchasing: u32,
    pub revenue: u64,
}

/// Events of one person inside one 30-minute bucket: all in one session.
#[derive(Clone, Copy, Default)]
pub struct Cell {
    pub min: u32,
    pub max: u32,
    pub count: u32,
    /// 1-based index of the newest purchase in the cell, 0 when none.
    pub phead: u32,
}

pub struct Out {
    pub rows: Vec<URow>,
    pub camp: Vec<CampStat>,
    pub cred: Vec<Cred>,
    pub assist: Vec<u32>,
    pub stamp: Vec<u32>,
    pub sid: u32,
    pub ndays: usize,
    pub ncamp: usize,
    pub day0: i64,
    pub ws_ms: i64,
    cats: Vec<(u32, u64)>,
    starts: Vec<u32>,
    sdays: Vec<u32>,
    srev: Vec<u64>,
    cnt: Vec<u32>,
    pos: Vec<u32>,
    active: Vec<u32>,
}

impl Out {
    pub fn new(ncamp: usize, ndays: usize, day0: i64, ws_ms: i64, rows_hint: usize) -> Out {
        Out {
            rows: Vec::with_capacity(rows_hint.max(1 << 12)),
            camp: Vec::new(),
            cred: vec![Cred::default(); ncamp * ndays],
            assist: vec![0; ncamp * ndays],
            stamp: vec![0; ncamp],
            sid: 0,
            ndays,
            ncamp,
            day0,
            ws_ms,
            cats: Vec::new(),
            starts: Vec::new(),
            sdays: Vec::new(),
            srev: Vec::new(),
            cnt: vec![0; ncamp],
            pos: vec![0; ncamp],
            active: Vec::new(),
        }
    }

    /// Day index of a session start (ms since window start).
    #[inline(always)]
    fn day_index(&self, start: u32) -> usize {
        ((self.ws_ms + start as i64).div_euclid(DAY) - self.day0) as usize
    }

    #[inline(always)]
    fn flush(&mut self, pkey: u64, day: usize, sessions: u32, events: u32, revenue: u64) {
        let mut cat = 0u32;
        let mut best = 0u64;
        for &(c, s) in &self.cats {
            if s > best || (s == best && c < cat) {
                best = s;
                cat = c;
            }
        }
        self.cats.clear();
        self.rows.push(URow { pkey, revenue, day: (self.day0 + day as i64) as i32, sessions, events, cat });
    }

    /// One person: `ev` sorted by ts (ts << 32 | pinfo), `clicks` sorted by
    /// (ts, click id) without duplicates.
    pub fn person(&mut self, pkey: u64, ev: &[u64], clicks: &[Click]) {
        let n = ev.len();
        let ndays = self.ndays;
        self.starts.clear();
        self.sdays.clear();
        self.srev.clear();
        let mut i = 0usize;
        let mut cur_day = usize::MAX;
        let (mut d_sessions, mut d_events, mut d_revenue) = (0u32, 0u32, 0u64);
        while i < n {
            let start = (ev[i] >> 32) as u32;
            let mut last = start;
            let mut cnt = 0u32;
            let mut rev = 0u64;
            let day = self.day_index(start);
            if day != cur_day {
                if cur_day != usize::MAX {
                    self.flush(pkey, cur_day, d_sessions, d_events, d_revenue);
                }
                cur_day = day;
                d_sessions = 0;
                d_events = 0;
                d_revenue = 0;
            }
            while i < n {
                let x = ev[i];
                let t = (x >> 32) as u32;
                if t - last > GAP {
                    break;
                }
                last = t;
                cnt += 1;
                let p = x as u32;
                if p != 0 {
                    let price = (p & 0x00ff_ffff) as u64;
                    let cat = p >> 24;
                    rev += price;
                    match self.cats.iter_mut().find(|e| e.0 == cat) {
                        Some(e) => e.1 += price,
                        None => self.cats.push((cat, price)),
                    }
                }
                i += 1;
            }
            d_sessions += 1;
            d_events += cnt;
            d_revenue += rev;
            self.starts.push(start);
            self.sdays.push(day as u32);
            self.srev.push(rev);
        }
        if cur_day != usize::MAX {
            self.flush(pkey, cur_day, d_sessions, d_events, d_revenue);
        }
        // Credit: the latest click with start - WEEK <= ts <= start (click
        // time base is ms since window start - 7 days, so ts in [s, s+WEEK]).
        let (mut lo, mut hi) = (0usize, 0usize);
        let mut window_total = 0usize;
        for k in 0..self.starts.len() {
            let start = self.starts[k];
            let day = self.sdays[k] as usize;
            let rev = self.srev[k];
            let sc = start + WEEK;
            while hi < clicks.len() && clicks[hi].ts <= sc {
                hi += 1;
            }
            while lo < hi && clicks[lo].ts < start {
                lo += 1;
            }
            let camp = if hi > lo {
                let t = clicks[hi - 1].ts;
                let mut j = hi - 1;
                while j > lo && clicks[j - 1].ts == t {
                    j -= 1;
                }
                clicks[j].camp as usize
            } else {
                0
            };
            window_total += hi - lo;
            let st = &mut self.cred[day * self.ncamp + camp];
            st.sessions += 1;
            st.revenue += rev;
            st.purchasing += (rev > 0) as u32;
        }
        if window_total == 0 || unsafe { NOASSIST } {
            return;
        }
        if window_total <= 4 * clicks.len() + 64 && !unsafe { HEAVY } {
            let (mut lo, mut hi) = (0usize, 0usize);
            for k in 0..self.starts.len() {
                let start = self.starts[k];
                let day = self.sdays[k] as usize;
                let sc = start + WEEK;
                while hi < clicks.len() && clicks[hi].ts <= sc {
                    hi += 1;
                }
                while lo < hi && clicks[lo].ts < start {
                    lo += 1;
                }
                if hi > lo {
                    self.sid = self.sid.wrapping_add(1);
                    if self.sid == 0 {
                        self.stamp.fill(0);
                        self.sid = 1;
                    }
                    let sid = self.sid;
                    for c in &clicks[lo..hi] {
                        let kk = c.camp as usize;
                        if self.stamp[kk] != sid {
                            self.stamp[kk] = sid;
                            self.assist[day * self.ncamp + kk] += 1;
                        }
                    }
                }
            }
        } else {
            self.assists_heavy(clicks);
        }
    }

    /// One person from its cells in bucket order. `purch[i]` is
    /// (1-based next index, pinfo) for purchase i + 1.
    pub fn person_cells(&mut self, pkey: u64, cells: &[Cell], purch: &[(u32, u32)], clicks: &[Click]) {
        self.starts.clear();
        self.sdays.clear();
        self.srev.clear();
        let mut cur_day = usize::MAX;
        let (mut d_sessions, mut d_events, mut d_revenue) = (0u32, 0u32, 0u64);
        let n = cells.len();
        let mut i = 0usize;
        while i < n {
            let start = cells[i].min;
            let mut last = cells[i].max;
            let mut cnt = 0u32;
            let mut rev = 0u64;
            let day = self.day_index(start);
            if day != cur_day {
                if cur_day != usize::MAX {
                    self.flush(pkey, cur_day, d_sessions, d_events, d_revenue);
                }
                cur_day = day;
                d_sessions = 0;
                d_events = 0;
                d_revenue = 0;
            }
            loop {
                let c = &cells[i];
                cnt += c.count;
                last = last.max(c.max);
                let mut p = c.phead;
                while p != 0 {
                    let (next, pinfo) = purch[(p - 1) as usize];
                    let price = (pinfo & 0x00ff_ffff) as u64;
                    let cat = pinfo >> 24;
                    rev += price;
                    match self.cats.iter_mut().find(|e| e.0 == cat) {
                        Some(e) => e.1 += price,
                        None => self.cats.push((cat, price)),
                    }
                    p = next;
                }
                i += 1;
                if i == n || cells[i].min - last > GAP {
                    break;
                }
            }
            d_sessions += 1;
            d_events += cnt;
            d_revenue += rev;
            self.starts.push(start);
            self.sdays.push(day as u32);
            self.srev.push(rev);
        }
        if cur_day != usize::MAX {
            self.flush(pkey, cur_day, d_sessions, d_events, d_revenue);
        }
        if unsafe { NOATTR } {
            return;
        }
        self.attribute(clicks);
    }

    /// Credit and assists for the sessions in `starts`.
    fn attribute(&mut self, clicks: &[Click]) {
        let ncamp = self.ncamp;
        let heavy = clicks.len() > 2048 || unsafe { HEAVY };
        let sliding = !heavy && !unsafe { NOASSIST };
        let (mut lo, mut hi) = (0usize, 0usize);
        for k in 0..self.starts.len() {
            let start = self.starts[k];
            let day = self.sdays[k] as usize;
            let rev = self.srev[k];
            let sc = start + WEEK;
            while hi < clicks.len() && clicks[hi].ts <= sc {
                if sliding {
                    let c = clicks[hi].camp as usize;
                    let n = unsafe { self.cnt.get_unchecked_mut(c) };
                    if *n == 0 {
                        self.pos[c] = self.active.len() as u32;
                        self.active.push(c as u32);
                    }
                    *n += 1;
                }
                hi += 1;
            }
            while lo < hi && clicks[lo].ts < start {
                if sliding {
                    let c = clicks[lo].camp as usize;
                    let n = unsafe { self.cnt.get_unchecked_mut(c) };
                    *n -= 1;
                    if *n == 0 {
                        let at = self.pos[c] as usize;
                        let last = *self.active.last().unwrap();
                        self.active[at] = last;
                        self.pos[last as usize] = at as u32;
                        self.active.pop();
                    }
                }
                lo += 1;
            }
            let camp = if hi > lo {
                let t = clicks[hi - 1].ts;
                let mut j = hi - 1;
                while j > lo && clicks[j - 1].ts == t {
                    j -= 1;
                }
                clicks[j].camp as usize
            } else {
                0
            };
            let st = &mut self.cred[day * ncamp + camp];
            st.sessions += 1;
            st.revenue += rev;
            st.purchasing += (rev > 0) as u32;
            if sliding {
                let row = &mut self.assist[day * ncamp..(day + 1) * ncamp];
                for &c in &self.active {
                    unsafe { *row.get_unchecked_mut(c as usize) += 1 };
                }
            }
        }
        if sliding {
            for &c in &self.active {
                self.cnt[c as usize] = 0;
            }
            self.active.clear();
        } else if heavy && !unsafe { NOASSIST } && !clicks.is_empty() {
            self.assists_heavy(clicks);
        }
    }

    /// Assists for a person with many clicks: for each campaign, the union of
    /// its clicks' eligible start intervals [ts - WEEK, ts], counted per day.
    fn assists_heavy(&mut self, clicks: &[Click]) {
        let ndays = self.ndays;
        let ncamp = self.ncamp;
        let mut v: Vec<(u32, u32)> = clicks.iter().map(|c| (c.camp, c.ts)).collect();
        v.sort_unstable();
        // First session index of each day.
        let ns = self.starts.len();
        let mut dstart = vec![ns; ndays + 1];
        for k in (0..ns).rev() {
            dstart[self.sdays[k] as usize] = k;
        }
        for d in (0..ndays).rev() {
            if dstart[d] > dstart[d + 1] {
                dstart[d] = dstart[d + 1];
            }
        }
        let starts = &self.starts;
        let sdays = &self.sdays;
        let camp = &mut self.assist;
        let mut flush = |c: u32, a: u32, b: u32| {
            // Sessions with start in [a - WEEK, b] (event time base).
            let lo_s = a.saturating_sub(WEEK);
            let i = starts.partition_point(|&s| s < lo_s);
            let j = starts.partition_point(|&s| s <= b);
            if i >= j {
                return;
            }
            let (d0, d1) = (sdays[i] as usize, sdays[j - 1] as usize);
            for d in d0..=d1 {
                let cnt = j.min(dstart[d + 1]) - i.max(dstart[d]);
                camp[d * ncamp + c as usize] += cnt as u32;
            }
        };
        let mut k = 0;
        while k < v.len() {
            let c = v[k].0;
            let mut a = v[k].1;
            let mut b = a;
            k += 1;
            while k < v.len() && v[k].0 == c {
                let t = v[k].1;
                if t - WEEK <= b || t < WEEK {
                    b = t;
                } else {
                    flush(c, a, b);
                    a = t;
                    b = t;
                }
                k += 1;
            }
            flush(c, a, b);
        }
    }
}

/// Sort u64 values with an LSD radix sort on the low `bits` of the high
/// word (ts), stable; small inputs use insertion sort.
pub fn sort_events(v: &mut [u64], tmp: &mut Vec<u64>) {
    let n = v.len();
    if n <= 48 {
        for i in 1..n {
            let x = v[i];
            let mut j = i;
            while j > 0 && v[j - 1] > x {
                v[j] = v[j - 1];
                j -= 1;
            }
            v[j] = x;
        }
        return;
    }
    if n < 1024 {
        v.sort_unstable();
        return;
    }
    tmp.clear();
    tmp.resize(n, 0);
    // Keys: ts is the high 32 bits; pinfo order does not matter.
    radix_pass(v, tmp, 32);
    radix_pass(tmp, v, 43);
    radix_pass(v, tmp, 54);
    v.copy_from_slice(tmp);
}

#[inline(always)]
fn radix_pass(src: &[u64], dst: &mut [u64], shift: u32) {
    let mut count = [0usize; 2048];
    for &x in src.iter() {
        count[((x >> shift) & 2047) as usize] += 1;
    }
    let mut sum = 0;
    for c in count.iter_mut() {
        let t = *c;
        *c = sum;
        sum += t;
    }
    for &x in src.iter() {
        let b = ((x >> shift) & 2047) as usize;
        unsafe { *dst.get_unchecked_mut(count[b]) = x };
        count[b] += 1;
    }
}

/// Sort clicks by (ts, click id bytes) and drop identical copies.
pub fn sort_clicks(c: &mut Vec<Click>, longs: &LongTable) {
    if c.len() > 1 {
        c.sort_unstable_by(|a, b| (a.ts, a.ckey).cmp(&(b.ts, b.ckey)));
        if !longs.is_empty() && c.iter().any(|x| x.ckey & crate::ids::LONG != 0) {
            c.sort_by(|a, b| a.ts.cmp(&b.ts).then_with(|| longs.cmp(a.ckey, b.ckey)));
        }
        c.dedup_by(|a, b| a.ckey == b.ckey && a.ts == b.ts);
    }
}

/// Sort clicks in place by (ts, click id bytes), drop identical copies, and
/// return the new length.
pub fn sort_clicks_slice(c: &mut [Click], longs: &LongTable) -> usize {
    if c.len() <= 1 {
        return c.len();
    }
    if c.len() <= 16 {
        for i in 1..c.len() {
            let x = c[i];
            let mut j = i;
            while j > 0 && (c[j - 1].ts, c[j - 1].ckey) > (x.ts, x.ckey) {
                c[j] = c[j - 1];
                j -= 1;
            }
            c[j] = x;
        }
    } else {
        c.sort_unstable_by(|a, b| (a.ts, a.ckey).cmp(&(b.ts, b.ckey)));
    }
    if !longs.is_empty() {
        // Re-order only equal-ts runs that contain a long ID.
        let mut i = 0;
        while i < c.len() {
            let mut j = i + 1;
            while j < c.len() && c[j].ts == c[i].ts {
                j += 1;
            }
            if j - i > 1 && c[i..j].iter().any(|x| x.ckey & crate::ids::LONG != 0) {
                c[i..j].sort_by(|a, b| longs.cmp(a.ckey, b.ckey));
            }
            i = j;
        }
    }
    let mut w = 1;
    for r in 1..c.len() {
        if c[r].ckey != c[w - 1].ckey || c[r].ts != c[w - 1].ts {
            c[w] = c[r];
            w += 1;
        }
    }
    w
}
