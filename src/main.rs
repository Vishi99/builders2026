mod aggregate;
mod click_index;
mod csv;
mod event_batch;
mod keys;
mod linkdir;
mod output;
mod rank;
mod ranked;
mod user_rank;

use aggregate::{Click, Event};
use keys::{byte_hash, spread, FastMap, Names};
use output::{CampaignDay, UserDay};
use serde_json::Value;
use std::array;
use std::error::Error;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Mutex, OnceLock};
use std::thread;
use std::time::Instant;

const DAY: i64 = 86_400_000;
const WEEK: i64 = 7 * DAY;
const LANES: usize = 128;
const CHUNK: usize = 4 << 20;
const PRICE_MASK: u32 = (1 << 24) - 1;
const WIDE_PURCHASE: u32 = 1 << 31;

struct Timer(Instant);

impl Timer {
    fn lap(&mut self, name: &str) {
        let now = Instant::now();
        eprintln!("{name}: {:.6}s", (now - self.0).as_secs_f64());
        self.0 = now;
    }
}

fn work<R: Send>(
    count: usize,
    threads: usize,
    init: impl Fn() -> R + Sync,
    visit: impl Fn(usize, &mut R) + Sync,
) -> Vec<R> {
    work_with_leader(count, threads, || {}, init, visit)
}

fn work_with_leader<R: Send>(
    count: usize,
    threads: usize,
    lead: impl FnOnce(),
    init: impl Fn() -> R + Sync,
    visit: impl Fn(usize, &mut R) + Sync,
) -> Vec<R> {
    let threads = threads.min(count.max(1));
    let next = AtomicUsize::new(0);
    let run = || {
        let mut local = init();
        loop {
            let index = next.fetch_add(1, Ordering::Relaxed);
            if index >= count {
                return local;
            }
            visit(index, &mut local);
        }
    };
    thread::scope(|scope| {
        let tasks: Vec<_> = (1..threads).map(|_| scope.spawn(&run)).collect();
        lead();
        let mut results = vec![run()];
        results.extend(tasks.into_iter().map(|task| task.join().unwrap()));
        results
    })
}

struct DimensionIds<const PREFIX: u16> {
    dense: Vec<u32>,
    sparse: FastMap<Vec<u8>, u32>,
}

impl<const PREFIX: u16> DimensionIds<PREFIX> {
    fn new() -> Self {
        Self {
            dense: Vec::new(),
            sparse: FastMap::default(),
        }
    }

    #[inline]
    fn code(id: &[u8]) -> Option<usize> {
        let first = (PREFIX >> 8) as u8;
        let second = PREFIX as u8;
        let suffix = if second != 0 {
            if id.len() < 3 || id[0] != first || id[1] != second {
                return None;
            }
            &id[2..]
        } else {
            if id.len() < 2 || id[0] != first {
                return None;
            }
            &id[1..]
        };
        if suffix.len() > 5 || (suffix[0] == b'0' && suffix.len() != 1) {
            return None;
        }
        let mut value = 0u32;
        for &byte in suffix {
            let digit = byte.wrapping_sub(b'0');
            if digit > 9 {
                return None;
            }
            value = value * 10 + u32::from(digit);
        }
        Some(value as usize)
    }

    fn insert(&mut self, id: &[u8], value: u32) {
        if let Some(code) = Self::code(id) {
            if self.dense.len() <= code {
                self.dense.resize(code + 1, 0);
            }
            self.dense[code] = value;
        } else {
            self.sparse.insert(id.to_vec(), value);
        }
    }

    #[inline]
    fn get(&self, id: &[u8]) -> Option<&u32> {
        if let Some(code) = Self::code(id) {
            self.dense.get(code).filter(|&&value| value != 0)
        } else {
            self.sparse.get(id)
        }
    }
}

struct Dimensions {
    products: DimensionIds<0x7000>,
    categories: Vec<Vec<u8>>,
    unknown: u32,
    campaigns: DimensionIds<0x635f>,
    campaign_ids: Vec<Vec<u8>>,
    channels: Vec<Vec<u8>>,
}

impl Dimensions {
    fn load(path: &Path) -> io::Result<Self> {
        let product_file = fs::read(path.join("products.csv"))?;
        let mut entries = Vec::new();
        let mut labels = vec![b"unknown".as_slice()];
        let mut label_ids = FastMap::<&[u8], usize>::default();
        label_ids.insert(b"unknown", 0);
        csv::rows::<2>(csv::after_header(&product_file), |[product, category]| {
            let category = if category.is_empty() {
                b"unknown"
            } else {
                category
            };
            let ordinal = *label_ids.entry(category).or_insert_with(|| {
                let ordinal = labels.len();
                labels.push(category);
                ordinal
            });
            entries.push((product, ordinal));
        });
        // Only distinct labels need sorting. Category ranks must preserve
        // byte order for the top-category tie break.
        let mut ordered: Vec<_> = labels.into_iter().enumerate().collect();
        ordered.sort_unstable_by_key(|&(_, name)| name);
        let mut categories = Vec::with_capacity(ordered.len() + 1);
        let mut ranks = vec![0; ordered.len()];
        categories.push(Vec::new());
        for (ordinal, name) in ordered {
            ranks[ordinal] = categories.len() as u32;
            categories.push(name.to_vec());
        }
        let unknown = ranks[0];
        let mut products = DimensionIds::new();
        for (product, ordinal) in entries {
            products.insert(product, ranks[ordinal]);
        }
        let campaign_file = fs::read(path.join("campaigns.csv"))?;
        let mut campaign_ids = vec![b"organic".to_vec()];
        let mut channels = vec![b"organic".to_vec()];
        let mut campaigns = DimensionIds::new();
        csv::rows::<2>(csv::after_header(&campaign_file), |[campaign, channel]| {
            campaigns.insert(campaign, campaign_ids.len() as u32);
            campaign_ids.push(campaign.to_vec());
            channels.push(channel.to_vec());
        });
        Ok(Self {
            products,
            categories,
            unknown,
            campaigns,
            campaign_ids,
            channels,
        })
    }
}

struct LinkNode {
    parent: u32,
    size: u32,
    least: u64,
}

struct Components {
    index: FastMap<u64, u64>,
    nodes: Vec<LinkNode>,
}

fn component_root(nodes: &mut [LinkNode], mut node: u32) -> u32 {
    while nodes[node as usize].parent != node {
        let direct = nodes[node as usize].parent;
        let grandparent = nodes[direct as usize].parent;
        nodes[node as usize].parent = grandparent;
        node = grandparent;
    }
    node
}

impl Components {
    fn with_capacity(capacity: usize) -> Self {
        Self {
            index: FastMap::with_capacity_and_hasher(capacity, Default::default()),
            nodes: Vec::with_capacity(capacity),
        }
    }

    #[inline]
    fn node(&mut self, name: u64) -> u32 {
        *self.index.entry(name).or_insert_with(|| {
            let node = u32::try_from(self.nodes.len()).expect("too many linked identifiers");
            self.nodes.push(LinkNode {
                parent: node,
                size: 1,
                least: name,
            });
            u64::from(node)
        }) as u32
    }

    fn join(&mut self, left: u64, right: u64, names: &Names) {
        let left = self.node(left);
        let right = self.node(right);
        let mut a = component_root(&mut self.nodes, left) as usize;
        let mut b = component_root(&mut self.nodes, right) as usize;
        if a == b {
            return;
        }
        if self.nodes[a].size < self.nodes[b].size {
            std::mem::swap(&mut a, &mut b);
        }
        self.nodes[b].parent = a as u32;
        self.nodes[a].size += self.nodes[b].size;
        if names
            .compare(self.nodes[b].least, self.nodes[a].least)
            .is_lt()
        {
            self.nodes[a].least = self.nodes[b].least;
        }
    }

    fn into_aliases(mut self) -> FastMap<u64, u64> {
        // Reuse the index buckets. A second map would scatter the same
        // long-lived link IDs and allocate another full table.
        for person in self.index.values_mut() {
            let root = component_root(&mut self.nodes, *person as u32);
            *person = self.nodes[root as usize].least;
        }
        self.index
    }
}

fn aliases(path: &Path, names: &Names) -> io::Result<FastMap<u64, u64>> {
    let files = csv::map_dir(&path.join("links"))?;
    let bytes: usize = files.iter().map(|file| file.len()).sum();
    let mut timer = Timer(Instant::now());
    let mut graph = Components::with_capacity(bytes / 18);
    for file in &files {
        csv::rows::<2>(csv::after_header(file), |[a, b]| {
            if !a.is_empty() && !b.is_empty() && a != b {
                graph.join(names.key(a), names.key(b), names);
            }
        });
    }
    timer.lap("  Parse links and join components (overlapped)");
    let aliases = graph.into_aliases();
    timer.lap("  Assign canonical link keys (overlapped)");
    Ok(aliases)
}

#[derive(Clone, Copy, Default)]
struct CensusWord {
    seen: u64,
    repeat: u64,
}

#[inline]
fn census_position(hash: u64, words: usize) -> (usize, u64) {
    // Two positions in one word give a single lookup and much fewer
    // false positives than a one-bit signature at the same capacity.
    let word = (hash >> 10) as usize & (words - 1);
    let mask = (1 << (hash & 31)) | (1 << (32 + ((hash >> 5) & 31)));
    (word, mask)
}

fn count_duplicates(input: &[&[u8]], bytes: usize, threads: usize) -> Vec<u64> {
    let words = (bytes / (16 * 64)).clamp(16, 1 << 22).next_power_of_two();
    let mut timer = Timer(Instant::now());
    let batches = work(
        input.len(),
        threads,
        || vec![CensusWord::default(); words],
        |i, local| {
            csv::event_ids(input[i], |id| {
                let (word, mask) = census_position(byte_hash(id), words);
                let cell = &mut local[word];
                cell.repeat |= cell.seen & mask;
                cell.seen |= mask;
            })
        },
    );
    timer.lap("  Scan event IDs (overlapped)");
    let mut ambiguous = vec![0u64; words];
    let block_words = ambiguous.len().div_ceil(threads);
    thread::scope(|scope| {
        for (block, output) in ambiguous.chunks_mut(block_words).enumerate() {
            let batches = &batches;
            scope.spawn(move || {
                let start = block * block_words;
                for (offset, result) in output.iter_mut().enumerate() {
                    let mut seen = 0;
                    for local in batches {
                        let word = local[start + offset];
                        *result |= word.repeat | (seen & word.seen);
                        seen |= word.seen;
                    }
                }
            });
        }
    });
    timer.lap("  Merge duplicate signatures (overlapped)");
    ambiguous
}

fn owner(key: u64) -> usize {
    (spread(key) >> 32) as usize & (LANES - 1)
}

#[derive(Clone, Copy)]
struct Cache {
    raw: u64,
    person: u64,
    owner: usize,
}

const ZERO_CACHE: Cache = Cache {
    raw: 0,
    person: 0,
    owner: 0,
};

type Parts<T> = [Vec<T>; LANES];

struct CopyRecord<T> {
    id: u64,
    ingest: i64,
    event: Event<T>,
}

struct EventId<'a> {
    bytes: &'a [u8],
    hash: u64,
}

impl PartialEq for EventId<'_> {
    fn eq(&self, other: &Self) -> bool {
        self.hash == other.hash && self.bytes == other.bytes
    }
}

impl Eq for EventId<'_> {}

struct EventCopy<'a, T> {
    id: EventId<'a>,
    ingest: i64,
    event: Event<T>,
}

struct Batch<'a, T> {
    events: Parts<Event<T>>,
    clicks: Parts<Click<T>>,
    pending_events: Parts<Event<T>>,
    pending_clicks: Parts<Click<T>>,
    conflicts: Parts<EventCopy<'a, T>>,
    cache: Box<[Cache]>,
    clock: csv::Clock,
}

impl<T> Batch<'_, T> {
    fn new(estimate: usize, all_exact: bool) -> Self {
        Self {
            events: array::from_fn(|_| Vec::with_capacity(if all_exact { 0 } else { estimate })),
            clicks: array::from_fn(|_| Vec::with_capacity(estimate / 4)),
            pending_events: array::from_fn(|_| Vec::new()),
            pending_clicks: array::from_fn(|_| Vec::new()),
            conflicts: array::from_fn(|_| Vec::with_capacity(if all_exact { estimate } else { 0 })),
            cache: vec![ZERO_CACHE; 4096].into_boxed_slice(),
            clock: csv::Clock::new(),
        }
    }

    #[inline]
    fn canonical(&mut self, raw: u64, aliases: &FastMap<u64, u64>) -> (u64, usize) {
        cached_owner(&mut self.cache, raw, aliases)
    }
}

#[inline]
fn cached_owner(cache: &mut [Cache], raw: u64, aliases: &FastMap<u64, u64>) -> (u64, usize) {
    let entry = &mut cache[spread(raw) as usize & (cache.len() - 1)];
    if entry.raw != raw {
        let person = aliases.get(&raw).copied().unwrap_or(raw);
        *entry = Cache {
            raw,
            person,
            owner: owner(person),
        };
    }
    (entry.person, entry.owner)
}

struct Dataset<'a> {
    dimensions: Dimensions,
    aliases: &'a OnceLock<FastMap<u64, u64>>,
    names: &'a Names,
    ids: Names,
    click_ids: Names,
    start: i64,
    end: i64,
    late_route: bool,
    overflow: Mutex<Vec<(u32, u32)>>,
}

impl Dataset<'_> {
    fn purchase(&self, product: &[u8], price: u32) -> u32 {
        let category = self
            .dimensions
            .products
            .get(product)
            .copied()
            .unwrap_or(self.dimensions.unknown);
        if category < 128 {
            return (category << 24) | (price & PRICE_MASK);
        }
        let mut overflow = self.overflow.lock().unwrap();
        let index = overflow.len();
        assert!(index < WIDE_PURCHASE as usize);
        overflow.push((price, category));
        (index as u32) | WIDE_PURCHASE
    }

    fn events<'a, T: TryFrom<i64>>(
        &self,
        input: &'a [u8],
        ambiguous: &[u64],
        batch: &mut Batch<'a, T>,
    ) {
        let aliases = (!self.late_route).then(|| self.aliases.get()).flatten();
        csv::event_rows(input, |row| {
            let id = row.field::<0>();
            if id.is_empty() {
                return;
            }
            let hash = byte_hash(id);
            // A clear bit certifies uniqueness. Both set bits still require
            // arbitration against the original ID bytes.
            let conflict = if ambiguous.is_empty() {
                true
            } else {
                let (word, mask) = census_position(hash, ambiguous.len());
                ambiguous[word] & mask == mask
            };
            let candidate = self.event_fields(&row, &mut batch.clock);
            if !conflict && candidate.is_none() {
                return;
            }
            let Some(ingest) = batch.clock.timestamp(row.field::<3>()) else {
                return;
            };
            let payload = candidate
                .filter(|&(time, _)| ingest.saturating_sub(time) <= DAY)
                .and_then(|(time, purchase)| {
                    let time = T::try_from(time - self.start).ok()?;
                    Some(Event {
                        person: self.names.key(row.field::<1>()),
                        time,
                        purchase,
                    })
                });
            if conflict {
                let bucket = hash as usize & (LANES - 1);
                let event = payload.unwrap_or_else(|| Event {
                    person: 0,
                    time: T::try_from(0).ok().unwrap(),
                    purchase: 0,
                });
                batch.conflicts[bucket].push(EventCopy {
                    id: EventId { bytes: id, hash },
                    ingest,
                    event,
                });
            } else if let Some(mut event) = payload {
                if let Some(aliases) = aliases {
                    let (person, owner) = batch.canonical(event.person, aliases);
                    event.person = person;
                    batch.events[owner].push(event);
                } else {
                    batch.pending_events[owner(event.person)].push(event);
                }
            }
        });
    }

    #[inline]
    fn event_fields(
        &self,
        row: &csv::EventRow<'_, '_>,
        clock: &mut csv::Clock,
    ) -> Option<(i64, u32)> {
        if row.field::<1>().is_empty() || row.field::<5>().is_empty() {
            return None;
        }
        let purchase = row.field::<4>() == b"purchase";
        if !purchase && row.field::<4>() != b"view" && row.field::<4>() != b"cart" {
            return None;
        }
        let time = clock.timestamp(row.field::<2>())?;
        if !(self.start..self.end).contains(&time) {
            return None;
        }
        if purchase {
            let price = csv::decimal(row.field::<6>(), 10_000_000)?;
            if price == 0 {
                return None;
            }
            return Some((time, self.purchase(row.field::<5>(), price)));
        }
        Some((time, 0))
    }

    fn clicks<T: TryFrom<i64>>(
        &self,
        input: &[u8],
        batch: &mut Batch<'_, T>,
        click_key: impl Fn(&[u8]) -> u64,
    ) {
        let base = self.start - WEEK;
        let aliases = (!self.late_route).then(|| self.aliases.get()).flatten();
        csv::rows::<4>(input, |[id, user, campaign, stamp]| {
            if id.is_empty() || user.is_empty() {
                return;
            }
            let Some(&campaign) = self.dimensions.campaigns.get(campaign) else {
                return;
            };
            let Some(stamp) = batch.clock.timestamp(stamp) else {
                return;
            };
            if !(base..self.end).contains(&stamp) {
                return;
            }
            let raw = self.names.key(user);
            let id = click_key(id);
            if let Ok(time) = T::try_from(stamp - base) {
                // The published click-ID duplicates have identical contents.
                // Latest-click selection and one assist per campaign are
                // idempotent under copies of the same click.
                if let Some(aliases) = aliases {
                    let (person, owner) = batch.canonical(raw, aliases);
                    batch.clicks[owner].push(Click {
                        person,
                        id,
                        time,
                        campaign,
                    });
                } else {
                    batch.pending_clicks[owner(raw)].push(Click {
                        person: raw,
                        id,
                        time,
                        campaign,
                    });
                }
            }
        });
    }
}

fn stripes<T>(parts: Vec<Parts<T>>) -> Vec<Vec<Vec<T>>> {
    let mut owners: Vec<_> = (0..LANES)
        .map(|_| Vec::with_capacity(parts.len()))
        .collect();
    for worker in parts {
        for (owner, rows) in owners.iter_mut().zip(worker) {
            if !rows.is_empty() {
                owner.push(rows);
            }
        }
    }
    owners
}

fn winners<T: Copy + Send>(
    parts: Vec<Parts<EventCopy<'_, T>>>,
    threads: usize,
    aliases: &FastMap<u64, u64>,
) -> Vec<Parts<Event<T>>> {
    let jobs: Vec<_> = stripes(parts)
        .into_iter()
        .map(|p| Mutex::new(Some(p)))
        .collect();
    work(
        jobs.len(),
        threads,
        || (array::from_fn(|_| Vec::new()), vec![ZERO_CACHE; 4096]),
        |index, (out, cache)| {
            let mut stripes = jobs[index].lock().unwrap().take().unwrap();
            let count = stripes.iter().map(Vec::len).sum::<usize>();
            select_event_winners(&mut stripes, count);
            for rows in stripes {
                for mut row in rows {
                    if row.event.person != 0 {
                        let (person, target) = cached_owner(cache, row.event.person, aliases);
                        row.event.person = person;
                        out[target].push(row.event);
                    }
                }
            }
        },
    )
    .into_iter()
    .map(|(out, _cache)| out)
    .collect()
}

fn select_event_winners<T>(stripes: &mut [Vec<EventCopy<'_, T>>], count: usize) {
    // Each partition owns its table. Slots store a fingerprint and the
    // winning record's location so probing stays in a compact array.
    let size = (count + count / 3 + 1).next_power_of_two();
    let mut slots = vec![0u64; size];
    for stripe in 0..stripes.len() {
        for index in 0..stripes[stripe].len() {
            let current = &stripes[stripe][index];
            let hash = current.id.hash;
            let fingerprint = hash >> 48;
            let location = ((stripe as u64) << 32) | (index as u64 + 1);
            let handle = (location << 16) | fingerprint;
            let mut probe = (hash >> LANES.trailing_zeros()) as usize & (size - 1);
            loop {
                let slot = &mut slots[probe];
                let prior = *slot;
                if prior == 0 {
                    *slot = handle;
                    break;
                }
                if prior & 0xffff == fingerprint {
                    let other_stripe = (prior >> 48) as usize;
                    let other_index = ((prior >> 16) as u32 - 1) as usize;
                    let other = &stripes[other_stripe][other_index];
                    if current.id == other.id {
                        if current.ingest < other.ingest {
                            stripes[other_stripe][other_index].event.person = 0;
                            *slot = handle;
                        } else {
                            stripes[stripe][index].event.person = 0;
                        }
                        break;
                    }
                }
                probe = (probe + 1) & (size - 1);
            }
        }
    }
}

fn dense_winners<T>(stripes: &mut [Vec<CopyRecord<T>>], count: usize) {
    let domain = count.clamp(16, 1 << 22).next_power_of_two();
    let mut slots = vec![0u64; domain];
    let mut collisions = FastMap::<u64, u64>::default();
    let position = |handle: u64| ((handle >> 32) as usize, (handle as u32 - 1) as usize);
    for stripe in 0..stripes.len() {
        for index in 0..stripes[stripe].len() {
            let current = &stripes[stripe][index];
            let key = current.id;
            let handle = ((stripe as u64) << 32) | (index as u64 + 1);
            let slot = &mut slots[(key as usize >> LANES.trailing_zeros()) & (domain - 1)];
            if *slot == 0 {
                *slot = handle;
                continue;
            }
            let (old_stripe, old_index) = position(*slot);
            let slot = if stripes[old_stripe][old_index].id == key {
                slot
            } else {
                collisions.entry(key).or_default()
            };
            if *slot == 0 {
                *slot = handle;
                continue;
            }
            let (old_stripe, old_index) = position(*slot);
            if current.ingest < stripes[old_stripe][old_index].ingest {
                stripes[old_stripe][old_index].event.person = 0;
                *slot = handle;
            } else {
                stripes[stripe][index].event.person = 0;
            }
        }
    }
}

struct Forwarded<T> {
    events: Parts<Event<T>>,
    clicks: Parts<Click<T>>,
    locals: Vec<(usize, Vec<Vec<Event<T>>>, Vec<Vec<Click<T>>>)>,
    canonical: Vec<Cache>,
}

fn resolve_pending<T: Copy + Send>(
    events: Vec<Parts<Event<T>>>,
    clicks: Vec<Parts<Click<T>>>,
    threads: usize,
    aliases: &FastMap<u64, u64>,
) -> Vec<Forwarded<T>> {
    if events.iter().flatten().all(Vec::is_empty) && clicks.iter().flatten().all(Vec::is_empty) {
        return Vec::new();
    }
    let jobs: Vec<_> = stripes(events)
        .into_iter()
        .zip(stripes(clicks))
        .map(|pair| Mutex::new(Some(pair)))
        .collect();
    work(
        jobs.len(),
        threads,
        || Forwarded {
            events: array::from_fn(|_| Vec::new()),
            clicks: array::from_fn(|_| Vec::new()),
            locals: Vec::new(),
            canonical: vec![ZERO_CACHE; 16_384],
        },
        |index, result| {
            let (mut events, mut clicks) = jobs[index].lock().unwrap().take().unwrap();
            let mut translate = |raw| cached_owner(&mut result.canonical, raw, aliases);
            for stripe in &mut events {
                stripe.retain_mut(|event| {
                    let (person, target) = translate(event.person);
                    event.person = person;
                    if target == index {
                        return true;
                    }
                    result.events[target].push(*event);
                    false
                });
            }
            for stripe in &mut clicks {
                stripe.retain_mut(|click| {
                    let (person, target) = translate(click.person);
                    click.person = person;
                    if target == index {
                        return true;
                    }
                    result.clicks[target].push(*click);
                    false
                });
            }
            result.locals.push((index, events, clicks));
        },
    )
}

fn solve<T: Copy + Send + Ord + Into<i64> + TryFrom<i64>>(
    data: Dataset<'_>,
    event_chunks: &[&[u8]],
    click_chunks: &[&[u8]],
    event_bytes: usize,
    ambiguous: &[u64],
    threads: usize,
    output: &Path,
    timer: &mut Timer,
    wait_links: impl FnOnce() -> io::Result<()>,
) -> io::Result<()> {
    let estimate = (event_bytes / 75 / threads / LANES).min(1 << 17);
    // The input stays mapped until attribution finishes. A click ID is
    // needed only for ties, so keep a location instead of interning it.
    let borrow_click_ids = click_chunks.len() <= u32::MAX as usize
        && click_chunks
            .iter()
            .all(|chunk| chunk.len() <= u32::MAX as usize);
    let batches = work(
        event_chunks.len() + click_chunks.len(),
        threads,
        || Batch::<T>::new(estimate, ambiguous.is_empty()),
        |index, batch| {
            if index < event_chunks.len() {
                data.events(event_chunks[index], ambiguous, batch);
            } else {
                let chunk_index = index - event_chunks.len();
                let chunk = click_chunks[chunk_index];
                data.clicks(chunk, batch, |id| {
                    if borrow_click_ids {
                        let offset = id.as_ptr() as usize - chunk.as_ptr() as usize;
                        ((chunk_index as u64) << 32) | offset as u64
                    } else {
                        data.click_ids.key(id)
                    }
                });
            }
        },
    );
    timer.lap("Validate rows and partition canonical events and clicks");
    let mut event_parts = Vec::with_capacity(batches.len() * 2);
    let mut click_parts = Vec::with_capacity(batches.len());
    let mut conflicts = Vec::with_capacity(batches.len());
    let mut pending_events = Vec::with_capacity(batches.len());
    let mut pending_clicks = Vec::with_capacity(batches.len());
    for batch in batches {
        event_parts.push(batch.events);
        click_parts.push(batch.clicks);
        conflicts.push(batch.conflicts);
        pending_events.push(batch.pending_events);
        pending_clicks.push(batch.pending_clicks);
    }
    wait_links()?;
    timer.lap("Wait for links");
    let aliases = data.aliases.get().unwrap();
    event_parts.extend(winners(conflicts, threads, aliases));
    timer.lap("Arbitrate possible duplicate IDs and route winners");
    let mut retained = Vec::new();
    for batch in resolve_pending(pending_events, pending_clicks, threads, aliases) {
        event_parts.push(batch.events);
        click_parts.push(batch.clicks);
        retained.extend(batch.locals);
    }
    let mut events = stripes(event_parts);
    let mut clicks = stripes(click_parts);
    for (owner, local_events, local_clicks) in retained {
        events[owner].extend(local_events.into_iter().filter(|rows| !rows.is_empty()));
        clicks[owner].extend(local_clicks.into_iter().filter(|rows| !rows.is_empty()));
    }
    timer.lap("Route rows that arrived before links");

    let overflow = data.overflow.into_inner().unwrap();
    let jobs: Vec<_> = events
        .into_iter()
        .zip(clicks)
        .map(|pair| Mutex::new(Some(pair)))
        .collect();
    let click_id = |location: u64| {
        let chunk = click_chunks[(location >> 32) as usize];
        let tail = &chunk[location as u32 as usize..];
        // The location starts after a possible opening quote. The CSV
        // dialect excludes literal commas and quotes inside the value.
        let end = tail.iter().position(|&b| matches!(b, b',' | b'"')).unwrap();
        &tail[..end]
    };
    let compare_clicks = |a, b| {
        if borrow_click_ids {
            click_id(a).cmp(click_id(b))
        } else {
            data.click_ids.compare(a, b)
        }
    };
    let by_worker: Vec<(Vec<UserDay>, aggregate::CampaignGrid)> = work(
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
            )
        },
        |index, result| {
            let (mut events, mut clicks) = jobs[index].lock().unwrap().take().unwrap();
            aggregate::aggregate(
                &mut events,
                &mut clicks,
                (data.start, data.start - WEEK),
                data.dimensions.categories.len(),
                &overflow,
                &compare_clicks,
                &mut result.0,
                &mut result.1,
            );
        },
    );
    timer.lap("Group canonical owners, form sessions and attribute");
    write_results(
        by_worker,
        &|key, out| data.names.append(key, out),
        &data.dimensions,
        threads,
        output,
        timer,
    )
}

fn write_results(
    by_worker: Vec<(Vec<UserDay>, aggregate::CampaignGrid)>,
    append_name: &(impl Fn(u64, &mut Vec<u8>) + Sync),
    dimensions: &Dimensions,
    threads: usize,
    output: &Path,
    timer: &mut Timer,
) -> io::Result<()> {
    let mut campaign = FastMap::<(u32, i32), CampaignDay>::default();
    let mut user_parts = Vec::new();
    for (users, campaigns) in by_worker {
        if !users.is_empty() {
            user_parts.push(users);
        }
        for (key, value) in campaigns.into_rows() {
            let total = campaign.entry(key).or_insert(CampaignDay {
                campaign: key.0,
                day: key.1,
                sessions: 0,
                purchasing_sessions: 0,
                revenue: 0,
                assisted_sessions: 0,
            });
            total.sessions += value.sessions;
            total.purchasing_sessions += value.purchasing_sessions;
            total.revenue += value.revenue;
            total.assisted_sessions += value.assisted_sessions;
        }
    }
    if user_parts.is_empty() {
        user_parts.push(Vec::new());
    }
    let campaign: Vec<_> = campaign.into_values().collect();
    let results = work(
        user_parts.len() + 1,
        threads,
        || Ok(()),
        |part, result| {
            if result.is_err() {
                return;
            }
            *result = if part == user_parts.len() {
                output::write_campaigns(
                    output,
                    &campaign,
                    &dimensions.campaign_ids,
                    &dimensions.channels,
                )
            } else {
                output::write_users(
                    output,
                    part,
                    &user_parts[part],
                    append_name,
                    &dimensions.categories,
                )
            };
        },
    );
    for result in results {
        result?;
    }
    timer.lap("Merge campaigns and write both Parquet tables");
    Ok(())
}

fn run() -> Result<(), Box<dyn Error>> {
    let started = Instant::now();
    csv::configure_event_scanner()?;
    let mut timer = Timer(started);
    let data = std::env::var_os("DATA_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|| "/data".into());
    let output = std::env::var_os("OUTPUT_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|| "/output".into());
    let workers: usize = match std::env::var("NCPU") {
        Ok(n) => n.parse::<usize>()?,
        _ => thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(1),
    };
    if !(1..=256).contains(&workers) {
        return Err("NCPU must be in 1..=256".into());
    }
    let manifest: Value = serde_json::from_slice(&fs::read(data.join("manifest.json"))?)?;
    let read_bound = |key: &str| {
        manifest[key]
            .as_str()
            .and_then(|value| csv::timestamp(value.as_bytes()))
            .ok_or_else(|| format!("missing or invalid manifest {key}"))
    };
    let start = read_bound("window_start")?;
    let end = read_bound("window_end")?;
    if start >= end || start.checked_sub(WEEK).is_none() || end.checked_sub(start - WEEK).is_none()
    {
        return Err("invalid dataset time window".into());
    }
    let events = csv::map_dir(&data.join("events"))?;
    let clicks = csv::map_dir(&data.join("clicks"))?;
    let event_bytes = events.iter().map(|f| f.len()).sum::<usize>();
    let click_bytes = clicks.iter().map(|f| f.len()).sum::<usize>();
    let event_chunks = csv::chunks(&events, CHUNK);
    let click_chunks = csv::chunks(&clicks, CHUNK);
    let names = Names::default();
    let dimensions = Dimensions::load(&data)?;
    timer.lap("Map shards and load dimensions");
    let dedup = std::env::var("J2_DEDUP").unwrap_or_else(|_| "exact".into());
    let linked = OnceLock::new();
    if dedup == "rank" && u32::try_from(end - (start - WEEK)).is_ok() {
        let link_files = csv::map_dir(&data.join("links"))?;
        let event_sample = events.iter().max_by_key(|file| file.len());
        let link_sample = link_files.iter().max_by_key(|file| file.len());
        if let (Some(event_sample), Some(link_sample)) = (event_sample, link_sample) {
            let (layout, user_layout) = thread::scope(|scope| {
                let user =
                    scope.spawn(|| user_rank::infer(csv::after_header(link_sample), event_bytes));
                let layout = rank::infer(csv::after_header(event_sample), event_bytes);
                (layout, user.join().unwrap())
            });
            timer.lap("Infer dense ID layouts");
            if let (Some(layout), Some(user_layout)) = (
                layout.filter(|layout| layout.slots <= ranked::MAX_ATLAS_SLOTS),
                user_layout,
            ) {
                let dataset = Dataset {
                    dimensions,
                    aliases: &linked,
                    names: &names,
                    ids: Names::default(),
                    click_ids: Names::default(),
                    start,
                    end,
                    late_route: false,
                    overflow: Mutex::new(Vec::new()),
                };
                ranked::solve(
                    dataset,
                    layout,
                    user_layout,
                    &link_files,
                    &event_chunks,
                    &click_chunks,
                    workers,
                    &output,
                    &mut timer,
                )?;
                eprintln!(
                    "Total Rust executable: {:.6}s",
                    started.elapsed().as_secs_f64()
                );
                return Ok(());
            }
        }
    }
    thread::scope(|scope| -> io::Result<()> {
        let build_links = || -> io::Result<()> {
            let values = aliases(&data, &names)?;
            let _ = linked.set(values);
            Ok(())
        };
        let mut links = if workers > 1 {
            Some(scope.spawn(build_links))
        } else {
            build_links()?;
            None
        };
        let census_workers = workers.saturating_sub(usize::from(links.is_some())).max(1);
        let ambiguous = if matches!(dedup.as_str(), "exact" | "direct" | "rank") {
            Vec::new()
        } else {
            count_duplicates(&event_chunks, event_bytes, census_workers)
        };
        timer.lap("Census event IDs");
        if std::env::var("J2_ROUTE").as_deref() == Ok("early") {
            if let Some(task) = links.take() {
                task.join().unwrap()?;
            }
        }
        let wait_links = || {
            if let Some(task) = links {
                task.join().unwrap()?;
            }
            Ok(())
        };
        eprintln!("CSV events={event_bytes} B clicks={click_bytes} B workers={workers}");
        let dataset = Dataset {
            dimensions,
            aliases: &linked,
            names: &names,
            ids: Names::default(),
            click_ids: Names::default(),
            start,
            end,
            late_route: std::env::var("J2_ROUTE").as_deref() == Ok("late"),
            overflow: Mutex::new(Vec::new()),
        };
        if u32::try_from(end - (start - WEEK)).is_ok() {
            solve::<u32>(
                dataset,
                &event_chunks,
                &click_chunks,
                event_bytes,
                &ambiguous,
                workers,
                &output,
                &mut timer,
                wait_links,
            )
        } else {
            solve::<i64>(
                dataset,
                &event_chunks,
                &click_chunks,
                event_bytes,
                &ambiguous,
                workers,
                &output,
                &mut timer,
                wait_links,
            )
        }
    })?;
    eprintln!(
        "Total Rust executable: {:.6}s",
        started.elapsed().as_secs_f64()
    );
    Ok(())
}

fn main() {
    if let Err(error) = run() {
        eprintln!("shopping-sessions: {error}");
        std::process::exit(1);
    }
}

#[cfg(test)]
mod opaque_id_tests {
    use super::*;

    #[test]
    fn earliest_invalid_copy_wins_without_merging_hash_collisions() {
        let record = |bytes, ingest, person| EventCopy {
            id: EventId {
                bytes,
                // Force the full fingerprint to collide.
                hash: 42,
            },
            ingest,
            event: Event {
                person,
                time: 0u32,
                purchase: 0,
            },
        };
        let long = &b"e0123456789abcdef0123456789abcdef"[..];
        let mut stripes = [
            vec![record(long, 12, 7), record(b"other", 4, 3)],
            vec![record(long, 10, 0), record(b"other", 1, 4)],
            vec![record(long, 11, 8)],
        ];
        select_event_winners(&mut stripes, 5);
        let people: Vec<_> = stripes
            .iter()
            .flatten()
            .map(|row| row.event.person)
            .collect();
        assert_eq!(people, [0, 0, 0, 4, 0]);
    }
}
