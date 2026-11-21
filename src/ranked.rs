use crate::aggregate::{Activity, Run};
use crate::linkdir::{Canonical, LinkDirectory};
use crate::rank::Layout;
use crate::user_rank::UserLayout;
use crate::*;

const SHIFT: usize = 15;
const TILE: usize = 1 << SHIFT;
const MASK: usize = TILE - 1;
const INGEST_BITS: usize = 21;
const CODE_SHIFT: usize = SHIFT + INGEST_BITS;
const COMPACT_KEY: u64 = 3 << 62;

#[derive(Clone, Copy)]
struct Ticket {
    low: u32,
    high: u32,
    time: u32,
}

impl Ticket {
    #[inline]
    fn packed(self) -> u64 {
        u64::from(self.low) | (u64::from(self.high) << 32)
    }
}

const BUNDLE: usize = 16;
const DUMMY: Ticket = Ticket {
    low: 0,
    high: 0,
    time: 0,
};

#[repr(align(64))]
struct Bundle([Ticket; BUNDLE]);

struct Tile {
    filled: Vec<Bundle>,
    pending: [Ticket; BUNDLE],
    count: usize,
}

impl Tile {
    fn new() -> Self {
        Self {
            filled: Vec::new(),
            pending: [DUMMY; BUNDLE],
            count: 0,
        }
    }

    #[inline]
    fn push(&mut self, ticket: Ticket, capacity: usize) {
        self.pending[self.count] = ticket;
        self.count += 1;
        if self.count == BUNDLE {
            if self.filled.capacity() == 0 {
                self.filled.reserve_exact(capacity / BUNDLE);
            }
            self.filled.push(Bundle(self.pending));
            self.count = 0;
        }
    }

    fn is_empty(&self) -> bool {
        self.filled.is_empty() && self.count == 0
    }
}

#[derive(Clone, Copy)]
struct WideTicket {
    raw: u64,
    ingest: i64,
    activity: Activity<u32>,
    slot: u32,
}

#[derive(Clone, Copy)]
struct Winner {
    stamp: u64,
    activity: Activity<u32>,
}

const WIDE_WINNER: u64 = 1 << 63;
const EMPTY: Winner = Winner {
    stamp: u64::MAX,
    activity: Activity {
        time: 0,
        purchase: 0,
    },
};

struct Staged {
    tiles: Vec<Tile>,
    wide: Vec<Vec<WideTicket>>,
    spill: Parts<CopyRecord<u32>>,
    clicks: Batch<u32>,
    clock: csv::Clock,
}

type RunParts = [(Vec<Run>, Vec<Activity<u32>>); LANES];

type RankTile = (Vec<Run>, Vec<Activity<u32>>);

const SHARED: u32 = u32::MAX;
pub const MAX_ATLAS_SLOTS: usize = ((SHARED as usize - 8) >> 3) - TILE;

enum Placement {
    Absent,
    Exception,
    Tile { tile: usize, run: usize },
}

struct TileAtlas {
    groups: Vec<std::sync::atomic::AtomicU32>,
}

impl TileAtlas {
    fn new(groups: usize) -> Self {
        Self {
            groups: (0..groups)
                .map(|_| std::sync::atomic::AtomicU32::new(0))
                .collect(),
        }
    }

    fn code(person: u64) -> Option<(usize, usize)> {
        if person & COMPACT_KEY == COMPACT_KEY {
            let raw = (person as u32 - 1) as usize;
            Some((raw >> 3, raw & 7))
        } else {
            None
        }
    }

    fn record(&self, person: u64, tile: usize, run: usize) {
        let Some((group, alias)) = Self::code(person) else {
            return;
        };
        // A dense state covers all alias components of one latent group.
        // Sharing that state is conservative: two runs or two components
        // both force the exact exception reducer.
        let location = (((tile * TILE + run) << 3) | alias) as u32 + 1;
        if self.groups[group]
            .compare_exchange(0, location, Ordering::Relaxed, Ordering::Relaxed)
            .is_err()
        {
            self.groups[group].store(SHARED, Ordering::Relaxed);
        }
    }

    fn exception(&self, person: u64) {
        if let Some((group, _)) = Self::code(person) {
            self.groups[group].store(SHARED, Ordering::Relaxed);
        }
    }

    fn placement(&self, person: u64) -> Placement {
        let Some((group, alias)) = Self::code(person) else {
            return Placement::Exception;
        };
        match self.groups[group].load(Ordering::Relaxed) {
            0 => Placement::Absent,
            SHARED => Placement::Exception,
            encoded => {
                let encoded = (encoded - 1) as usize;
                if encoded & 7 == alias {
                    Placement::Tile {
                        tile: encoded >> (SHIFT + 3),
                        run: (encoded >> 3) & MASK,
                    }
                } else {
                    Placement::Absent
                }
            }
        }
    }
}

enum ReduceJob {
    Local(RankTile, Vec<Vec<Click<u32>>>),
    General(Vec<RankTile>, Vec<Vec<Click<u32>>>),
}

fn append_exception(parts: &mut RunParts, person: u64, range: &[Activity<u32>]) {
    let (runs, activities) = &mut parts[owner(person)];
    let start = activities.len();
    activities.extend_from_slice(range);
    if let Some(last) = runs.last_mut().filter(|last| last.person == person) {
        last.end = activities.len();
    } else {
        runs.push(Run {
            person,
            start,
            end: activities.len(),
        });
    }
}

fn collect_exception_runs(parts: RunParts, destination: &mut [Vec<RankTile>]) {
    for (stripes, part) in destination.iter_mut().zip(parts) {
        if !part.0.is_empty() {
            stripes.push(part);
        }
    }
}

#[inline]
fn push_run(parts: &mut RunParts, key: Cache, activity: Activity<u32>) {
    let (runs, activities) = &mut parts[key.owner];
    let index = activities.len();
    activities.push(activity);
    if let Some(last) = runs.last_mut().filter(|last| last.person == key.person) {
        last.end += 1;
    } else {
        runs.push(Run {
            person: key.person,
            start: index,
            end: index + 1,
        });
    }
}

#[inline]
fn canonical(raw: u64, cache: &mut [Cache], aliases: &LinkDirectory, names: &Names) -> Cache {
    let slot = &mut cache[spread(raw) as usize & (cache.len() - 1)];
    if slot.raw != raw {
        let person = resolve(raw, aliases, names);
        *slot = Cache {
            raw,
            person,
            owner: owner(person),
        };
    }
    *slot
}

pub fn solve(
    data: Dataset<'_>,
    layout: Layout,
    users: UserLayout,
    link_files: &[csv::Mapped],
    event_chunks: &[&[u8]],
    click_chunks: &[&[u8]],
    threads: usize,
    output: &Path,
    timer: &mut Timer,
) -> io::Result<()> {
    let links = OnceLock::new();
    thread::scope(|scope| {
        let task = scope.spawn(|| {
            let mut timing = Timer(Instant::now());
            let dir = LinkDirectory::build(link_files, users, data.names, threads.div_ceil(2));
            let _ = links.set(dir);
            timing.lap("  Build parallel exact link directory (overlapped)");
        });
        let tile_count = layout.slots.div_ceil(TILE);
        let ingest_base = data.start.div_euclid(1000) - 86_400;
        let batch_capacity = TILE.div_ceil(threads) * 5 / 4;
        let batches = work(
            event_chunks.len() + click_chunks.len(),
            threads,
            || Staged {
                tiles: (0..tile_count).map(|_| Tile::new()).collect(),
                wide: (0..tile_count).map(|_| Vec::new()).collect(),
                spill: array::from_fn(|_| Vec::new()),
                clicks: Batch::new(0),
                clock: csv::Clock::new(),
            },
            |index, batch| {
                if index >= event_chunks.len() {
                    scan_clicks(
                        &data,
                        users,
                        links.get(),
                        click_chunks[index - event_chunks.len()],
                        batch,
                    );
                    return;
                }
                csv::event_rows(event_chunks[index], |row| {
                    let id = row.field::<0>();
                    if id.is_empty() {
                        return;
                    }
                    let Some(ingest) = batch.clock.timestamp(row.field::<3>()) else {
                        return;
                    };
                    let payload = data
                        .event_fields(&row, &mut batch.clock)
                        .filter(|&(time, _)| ingest.saturating_sub(time) <= DAY);
                    let activity = payload.map_or(EMPTY.activity, |(time, purchase)| Activity {
                        time: (time - data.start) as u32,
                        purchase,
                    });
                    let (code, rank) = if payload.is_none() {
                        (Some(0), layout.rank(id))
                    } else {
                        let user = row.field::<1>();
                        if let Some((event, person)) = crate::rank::projected_pair(id, user) {
                            (users.encode_projected(person), layout.rank_projected(event))
                        } else {
                            (users.encode(user), layout.rank(id))
                        }
                    };
                    let raw = || match code {
                        Some(0) => 0,
                        Some(code) => COMPACT_KEY | u64::from(code),
                        None => data.names.key(row.field::<1>()),
                    };
                    if let Some(rank) = rank {
                        let tile = rank >> SHIFT;
                        let slot = (rank & MASK) as u32;
                        let stamp = (ingest.div_euclid(1000) - ingest_base) as u64;
                        if let Some(code) =
                            code.filter(|&code| u64::from(code) < 1 << (64 - CODE_SHIFT))
                        {
                            if stamp < (1 << INGEST_BITS)
                                && ingest % 1000 == 0
                                && activity.purchase == 0
                            {
                                let packed = u64::from(slot)
                                    | (stamp << SHIFT)
                                    | (u64::from(code) << CODE_SHIFT);
                                batch.tiles[tile].push(
                                    Ticket {
                                        low: packed as u32,
                                        high: (packed >> 32) as u32,
                                        time: activity.time,
                                    },
                                    batch_capacity,
                                );
                                return;
                            }
                        }
                        batch.wide[tile].push(WideTicket {
                            raw: raw(),
                            activity,
                            slot,
                            ingest,
                        });
                    } else {
                        let raw = raw();
                        let id = ordered_event_key(id, &data.ids);
                        let event = Event {
                            person: raw,
                            time: activity.time,
                            purchase: activity.purchase,
                        };
                        batch.spill[id as usize & (LANES - 1)].push(CopyRecord {
                            id,
                            ingest,
                            event,
                        });
                    }
                });
            },
        );
        timer.lap("Validate and stage ordinal ranges and clicks");
        let mut tiles: Vec<Vec<_>> = (0..tile_count).map(|_| Vec::new()).collect();
        let mut wide: Vec<Vec<_>> = (0..tile_count).map(|_| Vec::new()).collect();
        let mut spilled = Vec::new();
        let (mut early_clicks, mut late_clicks) = (Vec::new(), Vec::new());
        for batch in batches {
            for (stripes, tickets) in tiles.iter_mut().zip(batch.tiles) {
                if !tickets.is_empty() {
                    stripes.push(tickets);
                }
            }
            for (stripes, tickets) in wide.iter_mut().zip(batch.wide) {
                if !tickets.is_empty() {
                    stripes.push(tickets);
                }
            }
            spilled.push(batch.spill);
            early_clicks.push(batch.clicks.clicks);
            late_clicks.push(batch.clicks.pending_clicks);
        }
        task.join().unwrap();
        timer.lap("Wait for links");
        let aliases = links.get().unwrap();
        let tiles: Vec<_> = tiles
            .into_iter()
            .zip(wide)
            .filter(|(a, b)| !a.is_empty() || !b.is_empty())
            .map(|tile| Mutex::new(Some(tile)))
            .collect();
        let spill: Vec<_> = stripes(spilled)
            .into_iter()
            .filter(|stripes| !stripes.is_empty())
            .map(|stripes| Mutex::new(Some(stripes)))
            .collect();
        let atlas = TileAtlas::new(aliases.layout.groups);
        let tile_out: Vec<_> = (0..tiles.len())
            .map(|_| Mutex::new(None::<RankTile>))
            .collect();
        let exceptions = work(
            tiles.len() + spill.len(),
            threads,
            || {
                (
                    array::from_fn(|_| (Vec::new(), Vec::new())),
                    vec![EMPTY; TILE],
                    vec![ZERO_CACHE; 8192],
                    Vec::<WideTicket>::new(),
                )
            },
            |index, (exceptions, slots, cache, exceptional)| {
                if index < tiles.len() {
                    slots.fill(EMPTY);
                    let (stripes, wide) = tiles[index].lock().unwrap().take().unwrap();
                    for stripe in stripes {
                        let mut take = |ticket: Ticket| {
                            let packed = ticket.packed();
                            let winner = &mut slots[packed as usize & MASK];
                            let stamp = (packed >> SHIFT) & ((1 << INGEST_BITS) - 1);
                            if winner.stamp == u64::MAX
                                || stamp < (winner.stamp & ((1 << INGEST_BITS) - 1))
                            {
                                *winner = Winner {
                                    stamp: packed >> SHIFT,
                                    activity: Activity {
                                        time: ticket.time,
                                        purchase: 0,
                                    },
                                };
                            }
                        };
                        for bundle in stripe.filled {
                            for ticket in bundle.0 {
                                take(ticket);
                            }
                        }
                        for &ticket in &stripe.pending[..stripe.count] {
                            take(ticket);
                        }
                    }
                    exceptional.clear();
                    for stripe in wide {
                        for ticket in stripe {
                            let winner = &mut slots[ticket.slot as usize];
                            let ingest = match winner.stamp {
                                u64::MAX => i64::MAX,
                                index if index >= WIDE_WINNER => {
                                    exceptional[(index ^ WIDE_WINNER) as usize].ingest
                                }
                                stamp => {
                                    (ingest_base + (stamp & ((1 << INGEST_BITS) - 1)) as i64) * 1000
                                }
                            };
                            if ticket.ingest < ingest {
                                let index = exceptional.len();
                                *winner = Winner {
                                    stamp: WIDE_WINNER | index as u64,
                                    activity: ticket.activity,
                                };
                                exceptional.push(ticket);
                            }
                        }
                    }
                    let mut current = ZERO_CACHE;
                    let mut ranges = Vec::<Run>::with_capacity(TILE / 24);
                    let mut activity = Vec::with_capacity(TILE);
                    for winner in slots.iter() {
                        let raw = match winner.stamp {
                            u64::MAX => continue,
                            index if index >= WIDE_WINNER => {
                                exceptional[(index ^ WIDE_WINNER) as usize].raw
                            }
                            stamp => {
                                let code = stamp >> INGEST_BITS;
                                if code == 0 {
                                    continue;
                                }
                                COMPACT_KEY | code
                            }
                        };
                        if raw != 0 {
                            if current.raw != raw {
                                current = canonical(raw, cache, aliases, data.names);
                            }
                            let start = activity.len();
                            if let Some(last) = ranges
                                .last_mut()
                                .filter(|last| last.person == current.person)
                            {
                                last.end += 1;
                            } else {
                                atlas.record(current.person, index, ranges.len());
                                ranges.push(Run {
                                    person: current.person,
                                    start,
                                    end: start + 1,
                                });
                            }
                            activity.push(winner.activity);
                        }
                    }
                    *tile_out[index].lock().unwrap() = Some((ranges, activity));
                } else {
                    let mut stripes = spill[index - tiles.len()].lock().unwrap().take().unwrap();
                    let count = stripes.iter().map(Vec::len).sum();
                    dense_winners(&mut stripes, count);
                    for stripe in stripes {
                        for ticket in stripe {
                            let Event {
                                person: raw,
                                time,
                                purchase,
                            } = ticket.event;
                            if raw != 0 {
                                let key = canonical(raw, cache, aliases, data.names);
                                atlas.exception(key.person);
                                push_run(exceptions, key, Activity { time, purchase });
                            }
                        }
                    }
                }
            },
        );
        timer.lap("Arbitrate ordinal winners and mark exact tile residency");
        let mut fallback: Vec<Vec<RankTile>> = (0..LANES).map(|_| Vec::new()).collect();
        for (parts, _, _, _) in exceptions {
            collect_exception_runs(parts, &mut fallback);
        }
        let click_jobs: Vec<_> = early_clicks
            .into_iter()
            .map(|part| (part, false))
            .chain(late_clicks.into_iter().map(|part| (part, true)))
            .flat_map(|(part, raw)| {
                part.into_iter()
                    .filter(|clicks| !clicks.is_empty())
                    .map(move |clicks| Mutex::new(Some((clicks, raw))))
            })
            .collect();
        let routed = work(
            tile_out.len() + click_jobs.len(),
            threads,
            || {
                (
                    array::from_fn(|_| (Vec::new(), Vec::new())),
                    array::from_fn(|_| Vec::new()),
                    (0..tile_out.len()).map(|_| Vec::new()).collect::<Vec<_>>(),
                    vec![ZERO_CACHE; 8192],
                )
            },
            |index, (exceptions, excess_clicks, tile_clicks, cache)| {
                if index < tile_out.len() {
                    let mut tile = tile_out[index].lock().unwrap();
                    let (runs, events) = tile.as_mut().unwrap();
                    for (at, run) in runs.iter_mut().enumerate() {
                        let stays_here = matches!(
                            atlas.placement(run.person),
                            Placement::Tile { tile, run } if tile == index && run == at
                        );
                        if !stays_here {
                            append_exception(exceptions, run.person, &events[run.start..run.end]);
                            run.person = 0;
                        }
                    }
                } else {
                    let (clicks, raw) = click_jobs[index - tile_out.len()]
                        .lock()
                        .unwrap()
                        .take()
                        .unwrap();
                    for mut click in clicks {
                        if raw {
                            click.person =
                                canonical(click.person, cache, aliases, data.names).person;
                        }
                        match atlas.placement(click.person) {
                            Placement::Absent => {}
                            Placement::Exception => excess_clicks[owner(click.person)].push(click),
                            Placement::Tile { tile, run } => {
                                click.person = run as u64;
                                tile_clicks[tile].push(click);
                            }
                        }
                    }
                }
            },
        );
        timer.lap("Route clicks and exact exceptions to their owners");
        let mut tile_clicks: Vec<Vec<_>> = (0..tile_out.len()).map(|_| Vec::new()).collect();
        let mut fallback_clicks = Vec::new();
        for (parts, clicks, direct_clicks, _) in routed {
            collect_exception_runs(parts, &mut fallback);
            fallback_clicks.push(clicks);
            for (stripe, clicks) in tile_clicks.iter_mut().zip(direct_clicks) {
                if !clicks.is_empty() {
                    stripe.push(clicks);
                }
            }
        }
        let jobs: Vec<_> = fallback
            .into_iter()
            .zip(stripes(fallback_clicks))
            .filter(|(runs, _)| !runs.is_empty())
            .map(|(runs, clicks)| Mutex::new(Some(ReduceJob::General(runs, clicks))))
            .chain(tile_out.into_iter().zip(tile_clicks).map(|(tile, clicks)| {
                Mutex::new(Some(ReduceJob::Local(
                    tile.into_inner().unwrap().unwrap(),
                    clicks,
                )))
            }))
            .collect();
        let overflow = data.overflow.into_inner().unwrap();
        let sequence = AtomicUsize::new(0);
        let append = |key: u64, out: &mut Vec<u8>| {
            if key & COMPACT_KEY == COMPACT_KEY {
                users.append(key as u32, out);
            } else {
                data.names.append(key, out);
            }
        };
        let by_worker = work(
            jobs.len(),
            threads,
            || {
                (
                    Vec::new(),
                    FastMap::default(),
                    output::UserWriter::open(output, sequence.fetch_add(1, Ordering::Relaxed)),
                )
            },
            |index, (users, campaigns, writer)| {
                let Ok(writer_inner) = writer else { return };
                let bases = (data.start, data.start - WEEK);
                let categories = data.dimensions.categories.len();
                let compare = |a, b| data.click_ids.compare(a, b);
                match jobs[index].lock().unwrap().take().unwrap() {
                    ReduceJob::Local((runs, mut activity), mut clicks) => {
                        aggregate::aggregate_tile(
                            &runs,
                            &mut activity,
                            &mut clicks,
                            bases,
                            categories,
                            &overflow,
                            &compare,
                            users,
                            campaigns,
                        );
                    }
                    ReduceJob::General(mut runs, mut clicks) => {
                        aggregate::aggregate_runs(
                            &mut runs,
                            &mut clicks,
                            bases,
                            categories,
                            &overflow,
                            &compare,
                            users,
                            campaigns,
                        );
                    }
                }
                if users.len() >= 65_536 {
                    if let Err(error) =
                        writer_inner.append(users, &append, &data.dimensions.categories)
                    {
                        *writer = Err(error);
                    }
                    users.clear();
                }
            },
        );
        timer.lap("Reduce resident tiles and stream user output");
        let mut campaign = FastMap::<(u32, i32), CampaignDay>::default();
        let jobs: Vec<_> = by_worker
            .into_iter()
            .map(|(rows, campaigns, writer)| {
                for (key, row) in campaigns {
                    let total = campaign.entry(key).or_insert(CampaignDay {
                        campaign: key.0,
                        day: key.1,
                        ..CampaignDay::default()
                    });
                    total.sessions += row.sessions;
                    total.purchasing_sessions += row.purchasing_sessions;
                    total.revenue += row.revenue;
                    total.assisted_sessions += row.assisted_sessions;
                }
                Mutex::new(Some((rows, writer)))
            })
            .collect();
        let campaign: Vec<_> = campaign.into_values().collect();
        let outcomes = work(
            jobs.len() + 1,
            threads,
            || Ok(()),
            |index, state| {
                if state.is_err() {
                    return;
                }
                *state = if index == jobs.len() {
                    output::write_campaigns(
                        output,
                        &campaign,
                        &data.dimensions.campaign_ids,
                        &data.dimensions.channels,
                    )
                } else {
                    (|| {
                        let (rows, writer) = jobs[index].lock().unwrap().take().unwrap();
                        let mut writer = writer?;
                        writer.append(&rows, &append, &data.dimensions.categories)?;
                        writer.finish()
                    })()
                };
            },
        );
        for outcome in outcomes {
            outcome?;
        }
        timer.lap("Finalize both Parquet tables");
        Ok(())
    })
}

#[inline]
fn resolve(raw: u64, aliases: &LinkDirectory, names: &Names) -> u64 {
    if raw & COMPACT_KEY != COMPACT_KEY {
        return aliases.resolve_name(raw);
    }
    match aliases.resolve_code(raw as u32, names) {
        Canonical::Code(code) => COMPACT_KEY | u64::from(code),
        Canonical::Wide(raw) => raw,
    }
}

fn scan_clicks(
    data: &Dataset<'_>,
    users: UserLayout,
    links: Option<&LinkDirectory>,
    input: &[u8],
    batch: &mut Staged,
) {
    let base = data.start - WEEK;
    csv::rows::<4>(input, |[id, user, campaign, stamp]| {
        if id.is_empty() || user.is_empty() {
            return;
        }
        let Some(&campaign) = data.dimensions.campaigns.get(campaign) else {
            return;
        };
        let Some(stamp) = batch.clock.timestamp(stamp) else {
            return;
        };
        if !(base..data.end).contains(&stamp) {
            return;
        }
        let raw = if let Some(code) = users.encode(user) {
            COMPACT_KEY | u64::from(code)
        } else {
            data.names.key(user)
        };
        let (person, output) = if let Some(links) = links {
            (resolve(raw, links, data.names), &mut batch.clicks.clicks)
        } else {
            (raw, &mut batch.clicks.pending_clicks)
        };
        output[owner(person)].push(Click {
            person,
            id: data.click_ids.key(id),
            time: (stamp - base) as u32,
            campaign,
        });
    });
}
