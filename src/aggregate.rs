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
struct Activity<T> {
    time: T,
    purchase: u32,
}

#[derive(Clone, Copy)]
struct Interaction<T> {
    id: u64,
    time: T,
    campaign: u32,
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
    campaign_days: &mut FastMap<(u32, i32), CampaignDay>,
) {
    if events.iter().all(Vec::is_empty) {
        return;
    }
    let options = options();
    let mut reducer = Reducer {
        bases,
        options,
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
    };
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
    campaign_days: &'a mut FastMap<(u32, i32), CampaignDay>,
    sessions: Vec<Session>,
    category_totals: Vec<i64>,
    used_categories: Vec<u32>,
    bins: Vec<Bin>,
    purchases: Vec<Purchase>,
    active: Vec<u32>,
    window: FastMap<u32, (usize, usize)>,
}

impl<Compare: Fn(u64, u64) -> Ordering> Reducer<'_, Compare> {
    fn person<T: Copy + Ord + Into<i64>>(
        &mut self,
        person: u64,
        events: &mut [Activity<T>],
        clicks: &mut [Interaction<T>],
        sorted: bool,
    ) {
        self.sessions.clear();
        if sorted || !self.options.bins || events.len() < 2_048 || !self.make_bins(events) {
            if !sorted {
                events.sort_unstable_by_key(|event| event.time);
            }
            self.from_sorted(person, events);
        } else {
            self.from_bins(person);
        }
        if !sorted {
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

fn campaign_row(
    rows: &mut FastMap<(u32, i32), CampaignDay>,
    campaign: u32,
    day: i32,
) -> &mut CampaignDay {
    rows.entry((campaign, day)).or_insert(CampaignDay {
        campaign,
        day,
        sessions: 0,
        purchasing_sessions: 0,
        revenue: 0,
        assisted_sessions: 0,
    })
}

fn add_credit(rows: &mut FastMap<(u32, i32), CampaignDay>, campaign: u32, session: &Session) {
    let row = campaign_row(rows, campaign, session.day);
    row.sessions += 1;
    row.purchasing_sessions += u64::from(session.revenue != 0);
    row.revenue += session.revenue;
}
