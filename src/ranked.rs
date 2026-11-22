use crate::aggregate::packed::{self, PackedRun, PackedTile};
use crate::aggregate::{Activity, Run};
use crate::click_index::{self, COMPACT_KEY};
use crate::keys::ordered_event_key;
use crate::linkdir::{Canonical, LinkDirectory};
use crate::rank::Layout;
use crate::user_rank::UserLayout;
use crate::*;

const SHIFT: usize = 15;
const TILE: usize = 1 << SHIFT;
const MASK: usize = TILE - 1;
const INGEST_BITS: usize = 21;
const CODE_SHIFT: usize = SHIFT + INGEST_BITS;
const SLOT_BITS: usize = 64 - SHIFT;
const SLOT_PAYLOAD: u64 = (1 << SLOT_BITS) - 1;
const SLOT_TAG: u64 = !(SLOT_PAYLOAD | (1 << 63));
const SLOT_INGEST: u64 = (1 << INGEST_BITS) - 1;

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

const WIDE_WINNER: u64 = 1 << 63;
const EMPTY_ACTIVITY: Activity<u32> = Activity {
    time: 0,
    purchase: 0,
};

struct WinnerSlots {
    headers: Vec<u64>,
    timestamps: Vec<u32>,
}

impl WinnerSlots {
    fn new() -> Self {
        Self {
            headers: vec![0; TILE],
            timestamps: vec![0; TILE],
        }
    }

    #[inline]
    fn update(&mut self, ticket: Ticket, tag: u64) {
        let packed = ticket.packed();
        let index = packed as usize & MASK;
        let header = &mut self.headers[index];
        let payload = packed >> SHIFT;
        if *header & SLOT_TAG != tag || payload & SLOT_INGEST < *header & SLOT_INGEST {
            *header = tag | payload;
            self.timestamps[index] = ticket.time;
        }
    }

    #[inline]
    fn wide(&mut self, ticket: WideTicket, tag: u64, base: i64, exceptions: &mut Vec<WideTicket>) {
        let header = &mut self.headers[ticket.slot as usize];
        let ingest = if *header & SLOT_TAG != tag {
            i64::MAX
        } else if *header & WIDE_WINNER != 0 {
            exceptions[(*header & SLOT_PAYLOAD) as usize].ingest
        } else {
            (base + (*header & SLOT_INGEST) as i64) * 1000
        };
        if ticket.ingest < ingest {
            *header = tag | WIDE_WINNER | exceptions.len() as u64;
            exceptions.push(ticket);
        }
    }

    fn pairs<'a>(
        &'a self,
        tag: u64,
        exceptions: &'a [WideTicket],
    ) -> impl Iterator<Item = (u64, Activity<u32>)> + 'a {
        self.headers
            .iter()
            .copied()
            .zip(self.timestamps.iter().copied())
            .filter_map(move |(header, time)| {
                if header & SLOT_TAG != tag {
                    return None;
                }
                let payload = header & SLOT_PAYLOAD;
                if header & WIDE_WINNER != 0 {
                    let event = exceptions[payload as usize];
                    return (event.raw != 0).then_some((event.raw, event.activity));
                }
                let code = payload >> INGEST_BITS;
                (code != 0).then_some((COMPACT_KEY | code, Activity { time, purchase: 0 }))
            })
    }
}

struct Staged {
    tiles: Vec<Tile>,
    wide: Vec<Vec<WideTicket>>,
    spill: Parts<CopyRecord<u32>>,
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
    Local(PackedTile),
    General(Vec<RankTile>),
}

fn append_exception(parts: &mut RunParts, tile: &PackedTile, run: PackedRun) {
    let (runs, activities) = &mut parts[owner(run.person)];
    let start = activities.len();
    tile.extend_activities(run, activities);
    if let Some(last) = runs.last_mut().filter(|last| last.person == run.person) {
        last.end = activities.len();
    } else {
        runs.push(Run {
            person: run.person,
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
    let tile_count = layout.slots.div_ceil(TILE);
    let ingest_base = data.start.div_euclid(1000) - 86_400;
    let batch_capacity = TILE.div_ceil(threads) * 5 / 4;
    #[cfg(all(target_arch = "x86_64", target_feature = "avx2"))]
    let use_batch_scan = std::env::var_os("J2_BATCH_SCAN").is_some();
    #[cfg(all(target_arch = "x86_64", target_feature = "avx2"))]
    let window = crate::event_batch::Window {
        start: data.start,
        end: data.end,
        ingest_base,
    };
    let (batches, aliases, clicks) = click_index::scan(
        &data,
        users,
        link_files,
        click_chunks,
        event_chunks.len(),
        threads,
        || Staged {
            tiles: (0..tile_count).map(|_| Tile::new()).collect(),
            wide: (0..tile_count).map(|_| Vec::new()).collect(),
            spill: array::from_fn(|_| Vec::new()),
            clock: csv::Clock::new(),
        },
        |index, batch| {
            let exact = |batch: &mut Staged, row: csv::EventRow<'_, '_>| {
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
                let activity = payload.map_or(EMPTY_ACTIVITY, |(time, purchase)| Activity {
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
                    batch.spill[id as usize & (LANES - 1)].push(CopyRecord { id, ingest, event });
                }
            };
            #[cfg(all(target_arch = "x86_64", target_feature = "avx2"))]
            if use_batch_scan {
                crate::event_batch::scan(event_chunks[index], &window, layout, users, |hit| {
                    match hit {
                        crate::event_batch::Hit::Exact(row) => exact(batch, row),
                        crate::event_batch::Hit::Packed {
                            rank,
                            code,
                            time,
                            stamp,
                        } => {
                            let packed = (rank & MASK) as u64
                                | (u64::from(stamp) << SHIFT)
                                | (u64::from(code) << CODE_SHIFT);
                            batch.tiles[rank >> SHIFT].push(
                                Ticket {
                                    low: packed as u32,
                                    high: (packed >> 32) as u32,
                                    time,
                                },
                                batch_capacity,
                            );
                        }
                    }
                });
                return;
            }
            csv::event_rows(event_chunks[index], |row| exact(batch, row));
        },
    );
    timer.lap("Scan ranked events and publish sorted click ranges");
    let mut tiles: Vec<Vec<_>> = (0..tile_count).map(|_| Vec::new()).collect();
    let mut wide: Vec<Vec<_>> = (0..tile_count).map(|_| Vec::new()).collect();
    let mut spilled = Vec::new();
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
    }
    let aliases = &aliases;
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
        .map(|_| Mutex::new(None::<PackedTile>))
        .collect();
    let exceptions = work(
        tiles.len() + spill.len(),
        threads,
        || {
            (
                array::from_fn(|_| (Vec::new(), Vec::new())),
                WinnerSlots::new(),
                vec![ZERO_CACHE; 8192],
                Vec::<WideTicket>::new(),
            )
        },
        |index, (exceptions, slots, cache, exceptional)| {
            if index < tiles.len() {
                let tag = ((index + 1) as u64) << SLOT_BITS;
                let (stripes, wide) = tiles[index].lock().unwrap().take().unwrap();
                for stripe in stripes {
                    for bundle in stripe.filled {
                        for ticket in bundle.0 {
                            slots.update(ticket, tag);
                        }
                    }
                    for &ticket in &stripe.pending[..stripe.count] {
                        slots.update(ticket, tag);
                    }
                }
                exceptional.clear();
                for stripe in wide {
                    for ticket in stripe {
                        slots.wide(ticket, tag, ingest_base, exceptional);
                    }
                }
                let mut current = ZERO_CACHE;
                let mut tile_outcome = PackedTile::with_capacity(TILE);
                let mut last_person = 0;
                for (raw, event) in slots.pairs(tag, exceptional) {
                    if current.raw != raw {
                        current = canonical(raw, cache, aliases, data.names);
                    }
                    if current.person != last_person {
                        let run = tile_outcome.begin_run(current.person);
                        atlas.record(current.person, index, run);
                        last_person = current.person;
                    }
                    tile_outcome.push(event);
                }
                tile_outcome.seal_run();
                *tile_out[index].lock().unwrap() = Some(tile_outcome);
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
    let routed = work(
        tile_out.len(),
        threads,
        || -> RunParts { array::from_fn(|_| (Vec::new(), Vec::new())) },
        |index, exceptions| {
            let mut tile = tile_out[index].lock().unwrap();
            let tile = tile.as_mut().unwrap();
            for at in 0..tile.runs.len() {
                let run = tile.runs[at];
                let stays_here = matches!(
                    atlas.placement(run.person),
                    Placement::Tile { tile, run } if tile == index && run == at
                );
                if !stays_here {
                    append_exception(exceptions, tile, run);
                    tile.runs[at].person = 0;
                }
            }
        },
    );
    timer.lap("Route exact event exceptions");
    for parts in routed {
        collect_exception_runs(parts, &mut fallback);
    }
    let jobs: Vec<_> =
        fallback
            .into_iter()
            .filter(|runs| !runs.is_empty())
            .map(|runs| Mutex::new(Some(ReduceJob::General(runs))))
            .chain(tile_out.into_iter().map(|tile| {
                Mutex::new(Some(ReduceJob::Local(tile.into_inner().unwrap().unwrap())))
            }))
            .collect();
    let click_for = |person| clicks.get(person);
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
                aggregate::CampaignGrid::new(
                    data.start,
                    data.end,
                    data.dimensions.campaign_ids.len(),
                ),
                output::UserWriter::open(output, sequence.fetch_add(1, Ordering::Relaxed)),
            )
        },
        |index, (users, campaigns, writer)| {
            let Ok(writer_inner) = writer else { return };
            let bases = (data.start, data.start - WEEK);
            let categories = data.dimensions.categories.len();
            let compare = |a, b| data.click_ids.compare(a, b);
            match jobs[index].lock().unwrap().take().unwrap() {
                ReduceJob::Local(tile) => {
                    packed::aggregate_tile_indexed(
                        &tile, &click_for, bases, categories, &overflow, &compare, users, campaigns,
                    );
                }
                ReduceJob::General(mut runs) => {
                    aggregate::aggregate_runs_indexed(
                        &mut runs, &click_for, bases, categories, &overflow, &compare, users,
                        campaigns,
                    );
                }
            }
            if users.len() >= 65_536 {
                if let Err(error) = writer_inner.append(users, &append, &data.dimensions.categories)
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
            for (key, row) in campaigns.into_rows() {
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
