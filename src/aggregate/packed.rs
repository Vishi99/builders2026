use super::*;

#[derive(Clone, Copy)]
pub(crate) struct PackedRun {
    pub person: u64,
    start: u32,
    end: u32,
    purchase_start: u32,
    purchase_end: u32,
}

#[derive(Clone, Copy)]
struct LocatedPurchase {
    index: u32,
    packed: u32,
}

/// Winners in rank order. Most events consist of one timestamp and purchases
/// are a sparse side stream, each carrying its exact position.
pub(crate) struct PackedTile {
    pub runs: Vec<PackedRun>,
    times: Vec<u32>,
    purchases: Vec<LocatedPurchase>,
}

impl PackedTile {
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            runs: Vec::with_capacity(capacity / 24),
            times: Vec::with_capacity(capacity),
            purchases: Vec::with_capacity(capacity / 20),
        }
    }

    #[inline]
    pub fn begin_run(&mut self, person: u64) -> usize {
        self.seal_run();
        let run = self.runs.len();
        self.runs.push(PackedRun {
            person,
            start: self.times.len() as u32,
            end: 0,
            purchase_start: self.purchases.len() as u32,
            purchase_end: 0,
        });
        run
    }

    #[inline]
    pub fn push(&mut self, activity: Activity<u32>) {
        if activity.purchase != 0 {
            self.purchases.push(LocatedPurchase {
                index: self.times.len() as u32,
                packed: activity.purchase,
            });
        }
        self.times.push(activity.time);
    }

    pub fn seal_run(&mut self) {
        if let Some(run) = self.runs.last_mut() {
            run.end = self.times.len() as u32;
            run.purchase_end = self.purchases.len() as u32;
        }
    }

    fn run_times(&self, run: PackedRun) -> &[u32] {
        &self.times[run.start as usize..run.end as usize]
    }

    fn run_purchases(&self, run: PackedRun) -> &[LocatedPurchase] {
        &self.purchases[run.purchase_start as usize..run.purchase_end as usize]
    }

    /// Only used for nonresident or unordered histories.
    pub fn extend_activities(&self, run: PackedRun, out: &mut Vec<Activity<u32>>) {
        let start = out.len();
        out.extend(
            self.run_times(run)
                .iter()
                .map(|&time| Activity { time, purchase: 0 }),
        );
        for purchase in self.run_purchases(run) {
            out[start + (purchase.index - run.start) as usize].purchase = purchase.packed;
        }
    }
}

#[cfg(all(target_arch = "x86_64", target_feature = "avx2"))]
#[inline]
unsafe fn boundaries8(now: *const u32) -> (u32, u32) {
    use std::arch::x86_64::*;
    unsafe {
        let current = _mm256_loadu_si256(now.cast());
        let previous = _mm256_loadu_si256(now.sub(1).cast());
        let sign = _mm256_set1_epi32(i32::MIN);
        let reverse = _mm256_cmpgt_epi32(
            _mm256_xor_si256(previous, sign),
            _mm256_xor_si256(current, sign),
        );
        let difference = _mm256_sub_epi32(current, previous);
        let separated = _mm256_cmpgt_epi32(
            _mm256_xor_si256(difference, sign),
            _mm256_set1_epi32((GAP as i32) ^ i32::MIN),
        );
        (
            _mm256_movemask_ps(_mm256_castsi256_ps(reverse)) as u32,
            _mm256_movemask_ps(_mm256_castsi256_ps(separated)) as u32,
        )
    }
}

#[cfg(target_arch = "x86_64")]
#[inline]
unsafe fn boundaries16(now: *const u32) -> (u32, u32) {
    use std::arch::asm;
    let reverse: u32;
    let separated: u32;
    // A caller checks AVX-512F and ensures both adjacent windows fit the run.
    unsafe {
        asm!(
            "vmovdqu32 zmm0, [{now}]",
            "vmovdqu32 zmm1, [{now} - 4]",
            "vpsubd zmm2, zmm0, zmm1",
            "vpbroadcastd zmm3, {gap:e}",
            "vpcmpud k1, zmm1, zmm0, 6",
            "vpcmpud k2, zmm2, zmm3, 6",
            "kmovw {reverse:e}, k1",
            "kmovw {separated:e}, k2",
            now = in(reg) now,
            gap = in(reg) GAP as u32,
            reverse = lateout(reg) reverse,
            separated = lateout(reg) separated,
            lateout("zmm0") _, lateout("zmm1") _, lateout("zmm2") _, lateout("zmm3") _,
            lateout("k1") _, lateout("k2") _,
            options(readonly, nostack, preserves_flags),
        );
    }
    (reverse, separated)
}

#[inline]
fn push_cuts(cuts: &mut Vec<u32>, at: usize, mut mask: u32) {
    while mask != 0 {
        cuts.push(at as u32 + mask.trailing_zeros());
        mask &= mask - 1;
    }
}

fn session_cuts(times: &[u32], cuts: &mut Vec<u32>, use_avx512: bool) -> bool {
    cuts.clear();
    cuts.push(0);
    let mut at = 1;
    #[cfg(target_arch = "x86_64")]
    if use_avx512 {
        while at + 16 <= times.len() {
            let (reverse, separated) = unsafe { boundaries16(times.as_ptr().add(at)) };
            if reverse != 0 {
                return false;
            }
            push_cuts(cuts, at, separated);
            at += 16;
        }
    }
    #[cfg(all(target_arch = "x86_64", target_feature = "avx2"))]
    while at + 8 <= times.len() {
        let (reverse, separated) = unsafe { boundaries8(times.as_ptr().add(at)) };
        if reverse != 0 {
            return false;
        }
        push_cuts(cuts, at, separated);
        at += 8;
    }
    while at < times.len() {
        let current = times[at];
        let previous = times[at - 1];
        if current < previous {
            return false;
        }
        if i64::from(current - previous) > GAP {
            cuts.push(at as u32);
        }
        at += 1;
    }
    cuts.push(times.len() as u32);
    true
}

fn from_cuts(
    reducer: &mut Reducer<'_, impl Fn(u64, u64) -> Ordering>,
    tile: &PackedTile,
    run: PackedRun,
    cuts: &[u32],
) {
    reducer.sessions.clear();
    let mut daily = None;
    let mut purchases = tile.run_purchases(run).iter().peekable();
    let times = tile.run_times(run);
    for limits in cuts.windows(2) {
        let first = limits[0];
        let end = limits[1];
        let start = reducer.bases.0 + i64::from(times[first as usize]);
        let day = day_of(start);
        if daily.as_ref().is_some_and(|row: &UserDay| row.day != day) {
            reducer.flush_day(daily.take().unwrap());
        }
        let mut revenue = 0;
        while purchases
            .peek()
            .is_some_and(|purchase| purchase.index < run.start + end)
        {
            reducer.add_purchase(purchases.next().unwrap().packed, &mut revenue);
        }
        reducer.sessions.push(Session {
            start,
            revenue,
            day,
        });
        update_day(&mut daily, run.person, day, end - first, revenue);
    }
    reducer.flush_day(daily.unwrap());
}

fn attribute(
    reducer: &mut Reducer<'_, impl Fn(u64, u64) -> Ordering>,
    clicks: &mut [Interaction<u32>],
) {
    clicks.sort_unstable_by_key(|click| click.time);
    let use_union = reducer.options.assist == Assist::Union
        || (reducer.options.assist == Assist::Hybrid
            && clicks.len() > 16
            && reducer.sessions.len().saturating_mul(clicks.len()) > 512);
    if use_union {
        reducer.credit(clicks);
        reducer.assist_union(clicks);
    } else if clicks.len() <= 12 {
        reducer.credit_and_assist_small(clicks);
    } else {
        reducer.credit_and_assist_window(clicks);
    }
}

pub(crate) fn aggregate_tile(
    tile: &PackedTile,
    clicks: &mut [Vec<Click<u32>>],
    bases: (i64, i64),
    categories: usize,
    overflow_purchases: &[(u32, u32)],
    compare_clicks: &impl Fn(u64, u64) -> Ordering,
    user_days: &mut Vec<UserDay>,
    campaign_days: &mut super::CampaignGrid,
) {
    let mut ends = vec![0; tile.runs.len() + 1];
    for click in clicks.iter().flatten() {
        ends[click.person as usize + 1] += 1;
    }
    let mut total = 0;
    for offset in &mut ends {
        let count = *offset;
        *offset = total;
        total += count;
    }
    let mut interactions = uninitialized(total);
    for click in clicks.iter_mut().flat_map(std::mem::take) {
        let group = &mut ends[click.person as usize + 1];
        interactions[*group].write((&click).into());
        *group += 1;
    }
    // Each retained click lands in exactly one region counted above.
    let interactions = unsafe { initialized(&mut interactions) };
    let mut reducer = Reducer::new(
        bases,
        categories,
        overflow_purchases,
        compare_clicks,
        user_days,
        campaign_days,
    );
    let mut cuts = Vec::new();
    let mut activities = Vec::new();
    #[cfg(target_arch = "x86_64")]
    let use_avx512 = std::arch::is_x86_feature_detected!("avx512f");
    #[cfg(not(target_arch = "x86_64"))]
    let use_avx512 = false;
    for (index, &run) in tile.runs.iter().enumerate() {
        if run.person == 0 {
            continue;
        }
        let clicks = &mut interactions[ends[index]..ends[index + 1]];
        if session_cuts(tile.run_times(run), &mut cuts, use_avx512) {
            from_cuts(&mut reducer, tile, run, &cuts);
            attribute(&mut reducer, clicks);
        } else {
            activities.clear();
            tile.extend_activities(run, &mut activities);
            reducer.person_with_order(run.person, &mut activities, clicks, false, false);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn vector_cuts_use_unsigned_time_and_strict_gap() {
        let mut times: Vec<u32> = (0..32).map(|i| i * 100).collect();
        times.extend((0..32).map(|i| (1u32 << 31) + i * 100));
        let last = *times.last().unwrap();
        times.extend([last + GAP as u32, last + 2 * GAP as u32 + 1]);
        let mut cuts = Vec::new();
        assert!(session_cuts(&times, &mut cuts, false));
        assert_eq!(cuts, vec![0, 32, 65, 66]);
        times.swap(30, 32);
        assert!(!session_cuts(&times, &mut cuts, false));
    }

    #[test]
    fn exact_output_matches_sorted_and_reordered_activity_paths() {
        let day = DAY as u32;
        let gap = GAP as u32;
        let mut packed = PackedTile::with_capacity(64);
        let mut events = Vec::new();
        let mut runs = Vec::new();
        let mut times: Vec<u32> = (0..24).map(|at| day - 1000 + (at * 100)).collect();
        times.extend([day + 1300 + gap, day + 1301 + 2 * gap]);
        let start = events.len();
        packed.begin_run(1);
        for (at, time) in times.iter().enumerate() {
            let purchase = match at {
                0 => (2 << 24) | 123,
                20 => 1 << 31,
                25 => (1 << 24) | 42,
                _ => 0,
            };
            let event = Activity {
                time: *time,
                purchase,
            };
            packed.push(event);
            events.push(event);
        }
        runs.push(Run {
            person: 1,
            start,
            end: events.len(),
        });
        let start = events.len();
        packed.begin_run(2);
        times.reverse();
        for &time in &times {
            let event = Activity {
                time,
                purchase: (3 << 24) | 100,
            };
            packed.push(event);
            events.push(event);
        }
        packed.seal_run();
        runs.push(Run {
            person: 2,
            start,
            end: events.len(),
        });

        let week = WEEK as u32;
        let clicks = vec![
            Click {
                person: 0,
                time: day - 1000,
                id: 5,
                campaign: 1,
            },
            Click {
                person: 0,
                time: week + day - 1000,
                id: 4,
                campaign: 2,
            },
            Click {
                person: 0,
                time: week + day - 1000,
                id: 3,
                campaign: 3,
            },
            Click {
                person: 0,
                time: week + day + 100,
                id: 2,
                campaign: 4,
            },
            Click {
                person: 0,
                time: week + day + 100,
                id: 1,
                campaign: 4,
            },
            Click {
                person: 1,
                time: week + day - 1000,
                id: 5,
                campaign: 3,
            },
        ];
        let mut actual_users = Vec::new();
        let mut actual_campaigns = super::CampaignGrid::default();
        let mut want_users = Vec::new();
        let mut want_campaigns = super::CampaignGrid::default();
        let bounds = (0, -WEEK);
        let overflow = [(317, 129)];
        let compare = |a: u64, b: u64| a.cmp(&b);
        aggregate_tile(
            &packed,
            &mut [clicks.clone()],
            bounds,
            130,
            &overflow,
            &compare,
            &mut actual_users,
            &mut actual_campaigns,
        );
        super::super::aggregate_tile(
            &runs,
            &mut events,
            &mut [clicks],
            bounds,
            130,
            &overflow,
            &compare,
            &mut want_users,
            &mut want_campaigns,
        );
        let user_rows = |rows: Vec<UserDay>| {
            rows.into_iter()
                .map(|x| (x.person, x.day, x.events, x.sessions, x.revenue, x.category))
                .collect::<Vec<_>>()
        };
        let campaign_rows = |rows: super::CampaignGrid| {
            let mut rows: Vec<_> = rows
                .into_rows()
                .map(|(_, row)| row)
                .map(|x| {
                    (
                        x.campaign,
                        x.day,
                        x.sessions,
                        x.purchasing_sessions,
                        x.revenue,
                        x.assisted_sessions,
                    )
                })
                .collect();
            rows.sort_unstable();
            rows
        };
        assert_eq!(user_rows(actual_users), user_rows(want_users));
        assert_eq!(
            campaign_rows(actual_campaigns),
            campaign_rows(want_campaigns)
        );
    }
}

pub(crate) fn aggregate_tile_indexed<'a>(
    tile: &PackedTile,
    clicks: &impl Fn(u64) -> &'a [Interaction<u32>],
    bases: (i64, i64),
    categories: usize,
    overflow_purchases: &[(u32, u32)],
    compare_clicks: &impl Fn(u64, u64) -> Ordering,
    user_days: &mut Vec<UserDay>,
    campaign_days: &mut super::CampaignGrid,
) {
    let mut reducer = Reducer::new(
        bases,
        categories,
        overflow_purchases,
        compare_clicks,
        user_days,
        campaign_days,
    );
    let mut cuts = Vec::new();
    let mut activities = Vec::new();
    let mut scratch = Vec::new();
    #[cfg(target_arch = "x86_64")]
    let use_avx512 = std::arch::is_x86_feature_detected!("avx512f");
    #[cfg(not(target_arch = "x86_64"))]
    let use_avx512 = false;
    for &run in &tile.runs {
        if run.person == 0 {
            continue;
        }
        if session_cuts(tile.run_times(run), &mut cuts, use_avx512) {
            from_cuts(&mut reducer, tile, run, &cuts);
        } else {
            activities.clear();
            tile.extend_activities(run, &mut activities);
            reducer.collect_sessions(run.person, &mut activities, false);
        }
        reducer.attribute_indexed(clicks(run.person), &mut scratch);
    }
}
