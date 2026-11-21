pub(crate) mod packed;

use crate::keys::FastMap;
use crate::output::{CampaignDay, UserDay};
use std::cmp::Ordering;
use std::mem::MaybeUninit;
use std::sync::OnceLock;

const DAY: i64 = 86_400_000;
const WEEK: i64 = 7 * DAY;
const GAP: i64 = 1_800_000;

#[derive(Clone, Copy)]
pub struct Event<T> {
    pub person: u64,
    pub time: T,
    pub purchase: u32,
}

#[derive(Clone, Copy)]
pub struct Click<T> {
    pub person: u64,
    pub id: u64,
    pub time: T,
    pub campaign: u32,
}

#[derive(Clone, Copy)]
pub struct Activity<T> {
    pub time: T,
    pub purchase: u32,
}

#[derive(Clone, Copy)]
pub(crate) struct Interaction<T> {
    pub(crate) id: u64,
    pub(crate) time: T,
    pub(crate) campaign: u32,
}

impl<T: Copy> From<&Event<T>> for Activity<T> {
    fn from(event: &Event<T>) -> Self {
        Self {
            time: event.time,
            purchase: event.purchase,
        }
    }
}

impl<T: Copy> From<&Click<T>> for Interaction<T> {
    fn from(click: &Click<T>) -> Self {
        Self {
            id: click.id,
            time: click.time,
            campaign: click.campaign,
        }
    }
}

#[derive(Default)]
struct Group {
    person: u64,
    event_count: usize,
    click_count: usize,
    event_end: usize,
    click_end: usize,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Assist {
    Hybrid,
    Rolling,
    Union,
}

enum GroupOrder {
    Scatter,
    InPlace,
    Sort,
}

struct Options {
    groups: GroupOrder,
    bins: bool,
    assist: Assist,
}

fn options() -> &'static Options {
    static OPTIONS: OnceLock<Options> = OnceLock::new();
    OPTIONS.get_or_init(|| Options {
        groups: match std::env::var("J2_GROUP").as_deref() {
            Ok("inplace") => GroupOrder::InPlace,
            Ok("sort") => GroupOrder::Sort,
            _ => GroupOrder::Scatter,
        },
        bins: std::env::var("J2_BINS").as_deref() != Ok("0"),
        assist: match std::env::var("J2_ASSIST").as_deref() {
            Ok("window") => Assist::Rolling,
            Ok("union") => Assist::Union,
            _ => Assist::Hybrid,
        },
    })
}

pub fn aggregate<T: Copy + Ord + Into<i64>>(
    events: &mut [Vec<Event<T>>],
    clicks: &mut [Vec<Click<T>>],
    bases: (i64, i64),
    categories: usize,
    overflow_purchases: &[(u32, u32)],
    compare_clicks: &impl Fn(u64, u64) -> Ordering,
    user_days: &mut Vec<UserDay>,
    campaign_days: &mut CampaignGrid,
) {
    if events.iter().all(Vec::is_empty) {
        return;
    }
    let options = options();
    let mut reducer = Reducer::new(
        bases,
        categories,
        overflow_purchases,
        compare_clicks,
        user_days,
        campaign_days,
    );
    match options.groups {
        GroupOrder::Scatter => by_ranges::<false, _>(events, clicks, &mut reducer),
        GroupOrder::InPlace => by_ranges::<true, _>(events, clicks, &mut reducer),
        GroupOrder::Sort => {
            let mut activity = coalesce(events);
            let mut interactions = coalesce(clicks);
            by_sort(&mut activity, &mut interactions, &mut reducer);
        }
    }
}

pub struct Run {
    pub person: u64,
    pub start: usize,
    pub end: usize,
}

const NO_RUN: u32 = u32::MAX;

struct RunLink {
    stripe: u32,
    run: u32,
    next: u32,
}

struct PersonRuns {
    person: u64,
    first: u32,
    last: u32,
    event_count: usize,
    click_count: usize,
    click_end: usize,
}

/// Each run owns a non-empty interval of its stripe's activities. Intervals in
/// a stripe are disjoint. People may occur in multiple runs and stripes.
pub fn aggregate_runs<T: Copy + Ord + Into<i64>>(
    stripes: &mut [(Vec<Run>, Vec<Activity<T>>)],
    clicks: &mut [Vec<Click<T>>],
    bases: (i64, i64),
    categories: usize,
    overflow_purchases: &[(u32, u32)],
    compare_clicks: &impl Fn(u64, u64) -> Ordering,
    user_days: &mut Vec<UserDay>,
    campaign_days: &mut CampaignGrid,
) {
    let (links, mut groups, person_index) = link_person_runs(stripes);
    if groups.is_empty() {
        return;
    }
    let mut click_group = Vec::with_capacity(clicks.iter().map(Vec::len).sum());
    for click in clicks.iter().flatten() {
        let group = person_index.get(&click.person).copied().unwrap_or(NO_RUN);
        if group != NO_RUN {
            groups[group as usize].click_count += 1;
        }
        click_group.push(group);
    }
    drop(person_index);

    let mut next_click = 0;
    for group in &mut groups {
        group.click_end = next_click;
        next_click += group.click_count;
    }
    let mut interactions = uninitialized(next_click);
    for (click, group) in clicks.iter_mut().flat_map(std::mem::take).zip(click_group) {
        if group != NO_RUN {
            let next = &mut groups[group as usize].click_end;
            interactions[*next].write((&click).into());
            *next += 1;
        }
    }
    // The click ranges partition the vector and each position was written once.
    let interactions = unsafe { initialized(&mut interactions) };
    let mut reducer = Reducer::new(
        bases,
        categories,
        overflow_purchases,
        compare_clicks,
        user_days,
        campaign_days,
    );
    let mut combined = Vec::new();
    for group in groups {
        let clicks = &mut interactions[group.click_end - group.click_count..group.click_end];
        let activities = collect_linked_events(
            group.first,
            group.event_count,
            &links,
            stripes,
            &mut combined,
        );
        let ordered = activities.is_sorted_by_key(|event| event.time);
        reducer.person_with_order(group.person, activities, clicks, ordered, false);
    }
}

fn link_person_runs<T>(
    stripes: &[(Vec<Run>, Vec<Activity<T>>)],
) -> (Vec<RunLink>, Vec<PersonRuns>, FastMap<u64, u32>) {
    let run_count = stripes.iter().map(|(runs, _)| runs.len()).sum::<usize>();
    assert!(run_count < NO_RUN as usize);
    let mut links = Vec::<RunLink>::with_capacity(run_count);
    let mut groups = Vec::<PersonRuns>::new();
    let mut person_index = FastMap::default();
    for (stripe, (runs, _)) in stripes.iter().enumerate() {
        for (run, location) in runs.iter().enumerate() {
            let current = links.len() as u32;
            let count = location.end - location.start;
            let position = match person_index.entry(location.person) {
                std::collections::hash_map::Entry::Occupied(entry) => *entry.get(),
                std::collections::hash_map::Entry::Vacant(entry) => {
                    let position = groups.len() as u32;
                    entry.insert(position);
                    groups.push(PersonRuns {
                        person: location.person,
                        first: current,
                        last: NO_RUN,
                        event_count: 0,
                        click_count: 0,
                        click_end: 0,
                    });
                    position
                }
            };
            let group = &mut groups[position as usize];
            if group.last != NO_RUN {
                links[group.last as usize].next = current;
            }
            group.last = current;
            group.event_count += count;
            links.push(RunLink {
                stripe: stripe as u32,
                run: run as u32,
                next: NO_RUN,
            });
        }
    }
    (links, groups, person_index)
}

fn collect_linked_events<'a, T: Copy>(
    first: u32,
    count: usize,
    links: &[RunLink],
    stripes: &'a mut [(Vec<Run>, Vec<Activity<T>>)],
    combined: &'a mut Vec<Activity<T>>,
) -> &'a mut [Activity<T>] {
    let head = &links[first as usize];
    if head.next == NO_RUN {
        let (runs, activity) = &mut stripes[head.stripe as usize];
        let run = &runs[head.run as usize];
        &mut activity[run.start..run.end]
    } else {
        combined.clear();
        combined.reserve(count);
        let mut link = first;
        while link != NO_RUN {
            let node = &links[link as usize];
            let (runs, activity) = &stripes[node.stripe as usize];
            let run = &runs[node.run as usize];
            combined.extend_from_slice(&activity[run.start..run.end]);
            link = node.next;
        }
        combined
    }
}

pub(crate) fn aggregate_runs_indexed<'a, T: Copy + Ord + Into<i64> + 'a>(
    stripes: &mut [(Vec<Run>, Vec<Activity<T>>)],
    clicks: &impl Fn(u64) -> &'a [Interaction<T>],
    bases: (i64, i64),
    categories: usize,
    overflow_purchases: &[(u32, u32)],
    compare_clicks: &impl Fn(u64, u64) -> Ordering,
    user_days: &mut Vec<UserDay>,
    campaign_days: &mut CampaignGrid,
) {
    let (links, groups, _) = link_person_runs(stripes);
    if groups.is_empty() {
        return;
    }
    let mut reducer = Reducer::new(
        bases,
        categories,
        overflow_purchases,
        compare_clicks,
        user_days,
        campaign_days,
    );
    let mut combined = Vec::new();
    let mut scratch = Vec::new();
    for group in groups {
        let activities = collect_linked_events(
            group.first,
            group.event_count,
            &links,
            stripes,
            &mut combined,
        );
        reducer.person_indexed(group.person, activities, clicks(group.person), &mut scratch);
    }
}

pub(crate) fn aggregate_tile_indexed<'a, T: Copy + Ord + Into<i64> + 'a>(
    runs: &[Run],
    activity: &mut [Activity<T>],
    clicks: &impl Fn(u64) -> &'a [Interaction<T>],
    bases: (i64, i64),
    categories: usize,
    overflow_purchases: &[(u32, u32)],
    compare_clicks: &impl Fn(u64, u64) -> Ordering,
    user_days: &mut Vec<UserDay>,
    campaign_days: &mut CampaignGrid,
) {
    let mut reducer = Reducer::new(
        bases,
        categories,
        overflow_purchases,
        compare_clicks,
        user_days,
        campaign_days,
    );
    let mut scratch = Vec::new();
    for run in runs {
        if run.person != 0 {
            reducer.person_indexed(
                run.person,
                &mut activity[run.start..run.end],
                clicks(run.person),
                &mut scratch,
            );
        }
    }
}

pub fn aggregate_tile<T: Copy + Ord + Into<i64>>(
    runs: &[Run],
    activity: &mut [Activity<T>],
    clicks: &mut [Vec<Click<T>>],
    bases: (i64, i64),
    categories: usize,
    overflow_purchases: &[(u32, u32)],
    compare_clicks: &impl Fn(u64, u64) -> Ordering,
    user_days: &mut Vec<UserDay>,
    campaign_days: &mut CampaignGrid,
) {
    #[derive(Clone, Copy, Default)]
    struct ClickRange {
        count: usize,
        end: usize,
    }

    let mut ranges = vec![ClickRange::default(); runs.len()];
    for click in clicks.iter().flatten() {
        ranges[click.person as usize].count += 1;
    }
    let mut total = 0;
    for range in &mut ranges {
        range.end = total;
        total += range.count;
    }
    let mut interactions = Vec::<MaybeUninit<Interaction<T>>>::with_capacity(total);
    interactions.resize_with(total, MaybeUninit::uninit);
    for click in clicks.iter_mut().flat_map(std::mem::take) {
        let group = &mut ranges[click.person as usize];
        interactions[group.end].write((&click).into());
        group.end += 1;
    }
    // The click partitions cover the whole allocation and each slot is
    // initialized exactly once by the count-and-scatter passes above.
    let interactions = unsafe {
        std::slice::from_raw_parts_mut(interactions.as_mut_ptr().cast::<Interaction<T>>(), total)
    };
    let mut reducer = Reducer::new(
        bases,
        categories,
        overflow_purchases,
        compare_clicks,
        user_days,
        campaign_days,
    );
    for (run, range) in runs.iter().zip(ranges) {
        // Exception runs have been handed to the general owner reducer.
        if run.person == 0 {
            continue;
        }
        let events = &mut activity[run.start..run.end];
        let clicks = &mut interactions[range.end - range.count..range.end];
        let sorted = events.is_sorted_by_key(|event| event.time);
        reducer.person_with_order(run.person, events, clicks, sorted, false);
    }
}

fn uninitialized<T>(length: usize) -> Vec<MaybeUninit<T>> {
    let mut out = Vec::with_capacity(length);
    out.resize_with(length, MaybeUninit::uninit);
    out
}

unsafe fn initialized<T>(slice: &mut [MaybeUninit<T>]) -> &mut [T] {
    // SAFETY: The caller must have initialized every element of the slice.
    unsafe { std::slice::from_raw_parts_mut(slice.as_mut_ptr().cast(), slice.len()) }
}

fn coalesce<T>(stripes: &mut [Vec<T>]) -> Vec<T> {
    let mut all = Vec::with_capacity(stripes.iter().map(Vec::len).sum());
    for stripe in stripes {
        all.extend(std::mem::take(stripe));
    }
    all
}

fn by_ranges<const IN_PLACE: bool, T: Copy + Ord + Into<i64>>(
    events: &mut [Vec<Event<T>>],
    clicks: &mut [Vec<Click<T>>],
    reducer: &mut Reducer<'_, impl Fn(u64, u64) -> Ordering>,
) {
    let event_count: usize = events.iter().map(Vec::len).sum();
    let click_count = clicks.iter().map(Vec::len).sum();
    assert!(event_count <= u32::MAX as usize);
    let mut person_index = FastMap::default();
    person_index.reserve(event_count / 24);
    let mut groups = Vec::with_capacity(event_count / 24);
    let mut event_group = Vec::with_capacity(if IN_PLACE { 0 } else { event_count });
    for event in events.iter_mut().flatten() {
        let position = *person_index.entry(event.person).or_insert_with(|| {
            let position = groups.len();
            groups.push(Group {
                person: event.person,
                ..Group::default()
            });
            position
        });
        groups[position].event_count += 1;
        if IN_PLACE {
            event.person = position as u64;
        } else {
            event_group.push(position as u32);
        }
    }
    let mut click_group = Vec::with_capacity(if IN_PLACE { 0 } else { click_count });
    for click in clicks.iter_mut().flatten() {
        let position = person_index
            .get(&click.person)
            .map_or(u32::MAX, |&position| {
                groups[position].click_count += 1;
                position as u32
            });
        if IN_PLACE {
            click.person = u64::from(position);
        } else {
            click_group.push(position);
        }
    }
    drop(person_index);

    let mut next_event = 0;
    let mut next_click = 0;
    for group in &mut groups {
        group.event_end = next_event;
        group.click_end = next_click;
        next_event += group.event_count;
        next_click += group.click_count;
    }
    let mut activity = Vec::<MaybeUninit<Activity<T>>>::with_capacity(next_event);
    activity.resize_with(next_event, MaybeUninit::uninit);
    let mut interactions = Vec::<MaybeUninit<Interaction<T>>>::with_capacity(next_click);
    interactions.resize_with(next_click, MaybeUninit::uninit);

    let mut event_group = event_group.into_iter();
    let mut click_group = click_group.into_iter();
    for event in events.iter_mut().flat_map(std::mem::take) {
        let group = if IN_PLACE {
            event.person as u32
        } else {
            event_group.next().unwrap()
        };
        let next = &mut groups[group as usize].event_end;
        activity[*next].write((&event).into());
        *next += 1;
    }
    for click in clicks.iter_mut().flat_map(std::mem::take) {
        let group = if IN_PLACE {
            click.person as u32
        } else {
            click_group.next().unwrap()
        };
        if group != u32::MAX {
            let next = &mut groups[group as usize].click_end;
            interactions[*next].write((&click).into());
            *next += 1;
        }
    }

    // The count-and-scatter passes write each position exactly once, using
    // non-overlapping consecutive ranges. The backing vectors remain alive.
    let activity = unsafe {
        std::slice::from_raw_parts_mut(activity.as_mut_ptr().cast::<Activity<T>>(), next_event)
    };
    let interactions = unsafe {
        std::slice::from_raw_parts_mut(
            interactions.as_mut_ptr().cast::<Interaction<T>>(),
            next_click,
        )
    };
    for group in groups {
        reducer.person(
            group.person,
            &mut activity[group.event_end - group.event_count..group.event_end],
            &mut interactions[group.click_end - group.click_count..group.click_end],
            false,
        );
    }
}

fn by_sort<T: Copy + Ord + Into<i64>>(
    events: &mut [Event<T>],
    clicks: &mut [Click<T>],
    reducer: &mut Reducer<'_, impl Fn(u64, u64) -> Ordering>,
) {
    events.sort_unstable_by_key(|event| (event.person, event.time));
    clicks.sort_unstable_by_key(|click| (click.person, click.time));
    let mut click_at = 0;
    let mut activity = Vec::new();
    let mut interactions = Vec::new();
    for group in events.chunk_by(|left, right| left.person == right.person) {
        let person = group[0].person;
        click_at += clicks[click_at..].partition_point(|click| click.person < person);
        let click_count = clicks[click_at..].partition_point(|click| click.person == person);
        activity.clear();
        interactions.clear();
        activity.extend(group.iter().map(Activity::from));
        interactions.extend(
            clicks[click_at..click_at + click_count]
                .iter()
                .map(Interaction::from),
        );
        click_at += click_count;
        reducer.person(person, &mut activity, &mut interactions, true);
    }
}

struct Session {
    start: i64,
    revenue: i64,
    day: i32,
}

#[derive(Clone, Default)]
struct Bin {
    first: i64,
    last: i64,
    count: u32,
    last_purchase: usize,
}

struct Purchase {
    packed: u32,
    previous: usize,
}

struct Reducer<'a, Compare> {
    bases: (i64, i64),
    options: &'static Options,
    overflow_purchases: &'a [(u32, u32)],
    compare_clicks: &'a Compare,
    user_days: &'a mut Vec<UserDay>,
    campaign_days: &'a mut CampaignGrid,
    sessions: Vec<Session>,
    category_totals: Vec<i64>,
    used_categories: Vec<u32>,
    bins: Vec<Bin>,
    purchases: Vec<Purchase>,
    active: Vec<u32>,
    window: FastMap<u32, (usize, usize)>,
}

impl<'a, Compare: Fn(u64, u64) -> Ordering> Reducer<'a, Compare> {
    fn new(
        bases: (i64, i64),
        categories: usize,
        overflow_purchases: &'a [(u32, u32)],
        compare_clicks: &'a Compare,
        user_days: &'a mut Vec<UserDay>,
        campaign_days: &'a mut CampaignGrid,
    ) -> Self {
        Self {
            bases,
            options: options(),
            overflow_purchases,
            compare_clicks,
            user_days,
            campaign_days,
            sessions: Vec::new(),
            category_totals: vec![0; categories + 1],
            used_categories: Vec::new(),
            bins: Vec::new(),
            purchases: Vec::new(),
            active: Vec::new(),
            window: FastMap::default(),
        }
    }

    fn person<T: Copy + Ord + Into<i64>>(
        &mut self,
        person: u64,
        events: &mut [Activity<T>],
        clicks: &mut [Interaction<T>],
        sorted: bool,
    ) {
        self.person_with_order(person, events, clicks, sorted, sorted);
    }

    fn person_with_order<T: Copy + Ord + Into<i64>>(
        &mut self,
        person: u64,
        events: &mut [Activity<T>],
        clicks: &mut [Interaction<T>],
        events_sorted: bool,
        clicks_sorted: bool,
    ) {
        self.collect_sessions(person, events, events_sorted);
        if !clicks_sorted {
            clicks.sort_unstable_by_key(|click| click.time);
        }

        let use_union = self.options.assist == Assist::Union
            || (self.options.assist == Assist::Hybrid
                && clicks.len() > 16
                && self.sessions.len().saturating_mul(clicks.len()) > 512);
        if use_union {
            self.credit(clicks);
            self.assist_union(clicks);
        } else if clicks.len() <= 12 {
            self.credit_and_assist_small(clicks);
        } else {
            self.credit_and_assist_window(clicks);
        }
    }

    fn collect_sessions<T: Copy + Ord + Into<i64>>(
        &mut self,
        person: u64,
        events: &mut [Activity<T>],
        events_sorted: bool,
    ) {
        self.sessions.clear();
        if events_sorted || !self.options.bins || events.len() < 2_048 || !self.make_bins(events) {
            if !events_sorted {
                events.sort_unstable_by_key(|event| event.time);
            }
            self.from_sorted(person, events);
        } else {
            self.from_bins(person);
        }
    }

    fn person_indexed<T: Copy + Ord + Into<i64>>(
        &mut self,
        person: u64,
        events: &mut [Activity<T>],
        clicks: &[Interaction<T>],
        scratch: &mut Vec<Interaction<T>>,
    ) {
        let sorted = events.is_sorted_by_key(|event| event.time);
        self.collect_sessions(person, events, sorted);
        self.attribute_indexed(clicks, scratch);
    }

    /// The shared click index is time ordered. The interval-union strategy
    /// takes a private copy because its secondary campaign sort is in place.
    pub(crate) fn attribute_indexed<T: Copy + Ord + Into<i64>>(
        &mut self,
        clicks: &[Interaction<T>],
        scratch: &mut Vec<Interaction<T>>,
    ) {
        let use_union = self.options.assist == Assist::Union
            || (self.options.assist == Assist::Hybrid
                && clicks.len() > 16
                && self.sessions.len().saturating_mul(clicks.len()) > 512);
        if use_union {
            self.credit(clicks);
            scratch.clear();
            scratch.extend_from_slice(clicks);
            self.assist_union(scratch);
        } else if clicks.len() <= 12 {
            self.credit_and_assist_small(clicks);
        } else {
            self.credit_and_assist_window(clicks);
        }
    }

    fn add_purchase(&mut self, packed: u32, revenue: &mut i64) {
        if packed != 0 {
            let (price, category) = if packed >> 31 == 0 {
                (packed & 0x00ff_ffff, packed >> 24)
            } else {
                self.overflow_purchases[(packed & 0x7fff_ffff) as usize]
            };
            *revenue += i64::from(price);
            let total = &mut self.category_totals[category as usize];
            if *total == 0 {
                self.used_categories.push(category);
            }
            *total += i64::from(price);
        }
    }

    fn flush_day(&mut self, mut row: UserDay) {
        let mut high = 0;
        for category in self.used_categories.drain(..) {
            let revenue = std::mem::take(&mut self.category_totals[category as usize]);
            if revenue > high || revenue == high && category < row.category {
                high = revenue;
                row.category = category;
            }
        }
        self.user_days.push(row);
    }

    fn from_sorted<T: Copy + Into<i64>>(&mut self, person: u64, events: &[Activity<T>]) {
        let event_base = self.bases.0;
        let mut at = 0;
        let mut daily = None;
        while at < events.len() {
            let first = at;
            let start = event_base + events[at].time.into();
            let day = day_of(start);
            if daily.as_ref().is_some_and(|row: &UserDay| row.day != day) {
                self.flush_day(daily.take().unwrap());
            }
            let mut revenue = 0;
            loop {
                let previous = events[at];
                self.add_purchase(previous.purchase, &mut revenue);
                at += 1;
                if at == events.len() || events[at].time.into() - previous.time.into() > GAP {
                    break;
                }
            }
            self.sessions.push(Session {
                start,
                revenue,
                day,
            });
            update_day(&mut daily, person, day, (at - first) as u32, revenue);
        }
        self.flush_day(daily.unwrap());
    }

    fn make_bins<T: Copy + Ord + Into<i64>>(&mut self, events: &[Activity<T>]) -> bool {
        let mut first = events[0].time;
        let mut last = first;
        for event in &events[1..] {
            first = first.min(event.time);
            last = last.max(event.time);
        }
        let first = first.into().div_euclid(GAP);
        let last = last.into().div_euclid(GAP);
        let width = (last - first + 1) as usize;
        if width > 4_096 || width > events.len() / 2 {
            return false;
        }
        self.bins.clear();
        self.bins.resize(width, Bin::default());
        self.purchases.clear();
        for event in events {
            let time = event.time.into();
            let bin = &mut self.bins[(time.div_euclid(GAP) - first) as usize];
            if bin.count == 0 {
                bin.first = time;
                bin.last = time;
            } else {
                bin.first = bin.first.min(time);
                bin.last = bin.last.max(time);
            }
            bin.count += 1;
            if event.purchase != 0 {
                self.purchases.push(Purchase {
                    packed: event.purchase,
                    previous: bin.last_purchase,
                });
                bin.last_purchase = self.purchases.len();
            }
        }
        true
    }

    fn from_bins(&mut self, person: u64) {
        let mut daily = None;
        let mut at = 0;
        while at < self.bins.len() {
            if self.bins[at].count == 0 {
                at += 1;
                continue;
            }
            let start = self.bases.0 + self.bins[at].first;
            let day = day_of(start);
            if daily.as_ref().is_some_and(|row: &UserDay| row.day != day) {
                self.flush_day(daily.take().unwrap());
            }
            let mut count = 0;
            let mut revenue = 0;
            loop {
                let bin = &self.bins[at];
                let last = bin.last;
                count += bin.count;
                let mut next_purchase = bin.last_purchase;
                while let Some(index) = next_purchase.checked_sub(1) {
                    let purchase = &self.purchases[index];
                    let packed = purchase.packed;
                    next_purchase = purchase.previous;
                    self.add_purchase(packed, &mut revenue);
                }
                at += 1;
                while at < self.bins.len() && self.bins[at].count == 0 {
                    at += 1;
                }
                if at == self.bins.len() || self.bins[at].first - last > GAP {
                    break;
                }
            }
            self.sessions.push(Session {
                start,
                revenue,
                day,
            });
            update_day(&mut daily, person, day, count, revenue);
        }
        self.flush_day(daily.unwrap());
    }

    fn credit<T: Copy + Ord + Into<i64>>(&mut self, clicks: &[Interaction<T>]) {
        let mut right = 0;
        let mut winner = None;
        for session in &self.sessions {
            advance_credit(
                clicks,
                &mut right,
                &mut winner,
                session.start - self.bases.1,
                self.compare_clicks,
            );
            let campaign = credited_campaign(clicks, winner, session.start - self.bases.1);
            add_credit(self.campaign_days, campaign, session);
        }
    }

    fn credit_and_assist_small<T: Copy + Ord + Into<i64>>(&mut self, clicks: &[Interaction<T>]) {
        let mut right = 0;
        let mut winner = None;
        let mut left = 0;
        for session in &self.sessions {
            let start = session.start - self.bases.1;
            advance_credit(clicks, &mut right, &mut winner, start, self.compare_clicks);
            while left < right && start - clicks[left].time.into() > WEEK {
                left += 1;
            }
            let campaign = credited_campaign(clicks, winner, start);
            add_credit(self.campaign_days, campaign, session);
            let mut distinct = [0_u32; 12];
            let mut used = 0;
            for click in &clicks[left..right] {
                if !distinct[..used].contains(&click.campaign) {
                    distinct[used] = click.campaign;
                    used += 1;
                    campaign_row(self.campaign_days, click.campaign, session.day)
                        .assisted_sessions += 1;
                }
            }
        }
    }

    fn credit_and_assist_window<T: Copy + Ord + Into<i64>>(&mut self, clicks: &[Interaction<T>]) {
        self.window.clear();
        self.active.clear();
        let mut left = 0;
        let mut right = 0;
        let mut winner = None;
        for session in &self.sessions {
            let start = session.start - self.bases.1;
            let previous_right = right;
            advance_credit(clicks, &mut right, &mut winner, start, self.compare_clicks);
            for click in &clicks[previous_right..right] {
                let entry = self.window.entry(click.campaign).or_insert_with(|| {
                    self.active.push(click.campaign);
                    (self.active.len() - 1, 0)
                });
                entry.1 += 1;
            }
            while left < right && start - clicks[left].time.into() > WEEK {
                let campaign = clicks[left].campaign;
                let (index, count) = self.window.get_mut(&campaign).unwrap();
                *count -= 1;
                if *count == 0 {
                    let index = *index;
                    self.window.remove(&campaign);
                    self.active.swap_remove(index);
                    if let Some(replacement) = self.active.get(index) {
                        self.window.get_mut(replacement).unwrap().0 = index;
                    }
                }
                left += 1;
            }
            let campaign = credited_campaign(clicks, winner, start);
            add_credit(self.campaign_days, campaign, session);
            for &campaign in &self.active {
                campaign_row(self.campaign_days, campaign, session.day).assisted_sessions += 1;
            }
        }
    }

    fn assist_union<T: Copy + Ord + Into<i64>>(&mut self, clicks: &mut [Interaction<T>]) {
        clicks.sort_unstable_by_key(|click| (click.campaign, click.time));
        for group in clicks.chunk_by(|left, right| left.campaign == right.campaign) {
            let campaign = group[0].campaign;
            let mut lower = self.bases.1 + group[0].time.into();
            let mut upper = lower + WEEK;
            for click in &group[1..] {
                let time = self.bases.1 + click.time.into();
                if time > upper {
                    self.add_interval(campaign, lower, upper);
                    lower = time;
                }
                upper = time + WEEK;
            }
            self.add_interval(campaign, lower, upper);
        }
    }

    fn add_interval(&mut self, campaign: u32, lower: i64, upper: i64) {
        let first = self
            .sessions
            .partition_point(|session| session.start < lower);
        let last = self
            .sessions
            .partition_point(|session| session.start <= upper);
        let mut session_at = first;
        while session_at < last {
            let day = self.sessions[session_at].day;
            let final_at = session_at
                + self.sessions[session_at..last].partition_point(|session| session.day == day);
            campaign_row(self.campaign_days, campaign, day).assisted_sessions +=
                (final_at - session_at) as u64;
            session_at = final_at;
        }
    }
}

fn day_of(start: i64) -> i32 {
    start.div_euclid(DAY) as i32
}

fn update_day(day: &mut Option<UserDay>, person: u64, date: i32, events: u32, revenue: i64) {
    let row = day.get_or_insert_with(|| UserDay {
        person,
        day: date,
        events: 0,
        sessions: 0,
        revenue: 0,
        category: 0,
    });
    row.events += events;
    row.sessions += 1;
    row.revenue += revenue;
}

fn advance_credit<T: Copy + Ord + Into<i64>>(
    clicks: &[Interaction<T>],
    right: &mut usize,
    winner: &mut Option<usize>,
    start: i64,
    compare_clicks: &impl Fn(u64, u64) -> Ordering,
) {
    while *right < clicks.len() && clicks[*right].time.into() <= start {
        let click = &clicks[*right];
        if winner.is_none_or(|index| {
            click.time > clicks[index].time
                || click.time == clicks[index].time
                    && compare_clicks(click.id, clicks[index].id) == Ordering::Less
        }) {
            *winner = Some(*right);
        }
        *right += 1;
    }
}

fn credited_campaign<T: Copy + Into<i64>>(
    clicks: &[Interaction<T>],
    winner: Option<usize>,
    start: i64,
) -> u32 {
    winner
        .map(|index| &clicks[index])
        .filter(|click| start - click.time.into() <= WEEK)
        .map_or(0, |click| click.campaign)
}

#[derive(Clone, Copy, Default)]
struct Totals {
    sessions: u64,
    purchasing_sessions: u64,
    revenue: i64,
    assisted_sessions: u64,
}

#[derive(Default)]
pub struct CampaignGrid {
    start_day: i64,
    width: usize,
    cells: Vec<Totals>,
    sparse: FastMap<(u32, i32), Totals>,
}

impl CampaignGrid {
    pub fn new(start: i64, end: i64, campaigns: usize) -> Self {
        let first = start.div_euclid(DAY);
        let days = (end - 1).div_euclid(DAY) - first + 1;
        let cells = usize::try_from(days)
            .ok()
            .and_then(|days| days.checked_mul(campaigns))
            .filter(|&n| n <= 65_536)
            .unwrap_or(0);
        Self {
            start_day: first,
            width: campaigns,
            cells: vec![Totals::default(); cells],
            sparse: FastMap::default(),
        }
    }

    pub fn into_rows(self) -> impl Iterator<Item = ((u32, i32), CampaignDay)> {
        let Self {
            start_day,
            width,
            cells,
            sparse,
        } = self;
        let packed = cells
            .into_iter()
            .enumerate()
            .filter_map(move |(at, totals)| {
                if totals.sessions == 0 && totals.assisted_sessions == 0 {
                    return None;
                }
                let key = (
                    (at % width) as u32,
                    (start_day + (at / width) as i64) as i32,
                );
                Some((key, totals))
            });
        packed.chain(sparse).map(|(key, totals)| {
            (
                key,
                CampaignDay {
                    campaign: key.0,
                    day: key.1,
                    sessions: totals.sessions,
                    purchasing_sessions: totals.purchasing_sessions,
                    revenue: totals.revenue,
                    assisted_sessions: totals.assisted_sessions,
                },
            )
        })
    }
}

#[inline]
fn campaign_row(rows: &mut CampaignGrid, campaign: u32, day: i32) -> &mut Totals {
    let offset = i64::from(day) - rows.start_day;
    if offset >= 0 && (campaign as usize) < rows.width && !rows.cells.is_empty() {
        let at = offset as usize * rows.width + campaign as usize;
        if at < rows.cells.len() {
            return &mut rows.cells[at];
        }
    }
    rows.sparse.entry((campaign, day)).or_default()
}

fn add_credit(rows: &mut CampaignGrid, campaign: u32, session: &Session) {
    let row = campaign_row(rows, campaign, session.day);
    row.sessions += 1;
    row.purchasing_sessions += u64::from(session.revenue != 0);
    row.revenue += session.revenue;
}

#[cfg(test)]
mod run_tests {
    use super::*;

    fn random(state: &mut u64) -> u64 {
        *state = state.wrapping_mul(6364136223846793005).wrapping_add(1);
        *state >> 16
    }

    fn snapshot(
        users: Vec<UserDay>,
        campaigns: CampaignGrid,
    ) -> (
        Vec<(u64, i32, u32, u32, i64, u32)>,
        Vec<(u32, i32, u64, u64, i64, u64)>,
    ) {
        let mut users: Vec<_> = users
            .into_iter()
            .map(|r| (r.person, r.day, r.events, r.sessions, r.revenue, r.category))
            .collect();
        let mut campaigns: Vec<_> = campaigns
            .into_rows()
            .map(|(_, row)| row)
            .map(|r| {
                (
                    r.campaign,
                    r.day,
                    r.sessions,
                    r.purchasing_sessions,
                    r.revenue,
                    r.assisted_sessions,
                )
            })
            .collect();
        users.sort_unstable();
        campaigns.sort_unstable();
        (users, campaigns)
    }

    #[test]
    fn ranked_runs_match_scatter_even_when_fragmented_and_unordered() {
        for seed in [1, 17, 33, 619, 1145] {
            let mut state = seed;
            let mut event_parts = Vec::new();
            let mut click_parts = Vec::new();
            let mut stripes = Vec::new();
            for stripe in 0..11 {
                let mut runs = Vec::new();
                let mut activities = Vec::new();
                let mut events = Vec::new();
                let mut clicks = Vec::new();
                for run in 0..130 {
                    let person = match run {
                        0 => 1u64 << 63, // Hot user repeated across all stripes.
                        1 => 1 + stripe, // Local monotonic run.
                        _ => (random(&mut state) % 160 + 30) * 11,
                    };
                    let count = match run {
                        0 => 1193,
                        1 => 105,
                        _ => 1 + random(&mut state) % 35,
                    };
                    let start = activities.len();
                    let nearby = (random(&mut state) % 756) as i64 * GAP / 2;
                    for ordinal in 0..count {
                        let time = nearby
                            + if run == 1 {
                                ordinal as i64 * 100
                            } else {
                                (random(&mut state) % 728) as i64 * GAP / 2
                            };
                        let purchase = match random(&mut state) % 16 {
                            0 => (1 << 24) | 70,
                            1 => (2 << 24) | 70,
                            2 => 1 << 31,
                            _ => 0,
                        };
                        events.push(Event {
                            person,
                            time,
                            purchase,
                        });
                        activities.push(Activity { time, purchase });
                    }
                    runs.push(Run {
                        person,
                        start,
                        end: activities.len(),
                    });
                }
                for i in 0..420 {
                    let person = match i % 7 {
                        0 => 1u64 << 63,
                        1 => 1 + stripe,
                        2 => u64::MAX, // Click-only user.
                        _ => (random(&mut state) % 160 + 30) * 11,
                    };
                    let time = (random(&mut state) % 756) as i64 * GAP / 2 + WEEK / 2;
                    clicks.push(Click {
                        person,
                        id: stripe * 9_000 + i,
                        time,
                        campaign: 1 + (random(&mut state) % 15) as u32,
                    });
                }
                event_parts.push(events);
                stripes.push((runs, activities));
                click_parts.push(clicks);
            }
            let bases = (-DAY * 2, -DAY * 2 - WEEK);
            let compare = |a: u64, b: u64| a.cmp(&b);
            let overflow = [(40_000_000, 3)];
            let mut expected_users = Vec::new();
            let mut expected_campaigns = CampaignGrid::default();
            let mut ranked_clicks = click_parts.clone();
            aggregate(
                &mut event_parts,
                &mut click_parts,
                bases,
                3,
                &overflow,
                &compare,
                &mut expected_users,
                &mut expected_campaigns,
            );
            let mut actual_users = Vec::new();
            let mut actual_campaigns = CampaignGrid::default();
            aggregate_runs(
                &mut stripes,
                &mut ranked_clicks,
                bases,
                3,
                &overflow,
                &compare,
                &mut actual_users,
                &mut actual_campaigns,
            );
            assert_eq!(
                snapshot(expected_users, expected_campaigns),
                snapshot(actual_users, actual_campaigns),
                "seed {seed}",
            );
        }
    }

    #[test]
    fn ranked_runs_skip_unmatched_clicks_when_there_are_no_events() {
        let mut stripes = vec![(Vec::new(), Vec::<Activity<i32>>::new())];
        let mut clicks = vec![vec![Click {
            person: 123,
            id: 1,
            time: 900,
            campaign: 17,
        }]];
        let mut users = Vec::new();
        let mut campaigns = CampaignGrid::default();
        aggregate_runs(
            &mut stripes,
            &mut clicks,
            (0, -WEEK),
            1,
            &[],
            &|a, b| a.cmp(&b),
            &mut users,
            &mut campaigns,
        );
        assert!(users.is_empty());
        assert!(campaigns.into_rows().next().is_none());
    }
}
