use crate::aggregate::Interaction;
use crate::linkdir::{Canonical, LinkDirectory};
use crate::user_rank::UserLayout;
use crate::{csv, Click, Dataset, FastMap, Names, Timer, WEEK};
use std::mem::MaybeUninit;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Condvar, Mutex, OnceLock};
use std::thread;
use std::time::Instant;

pub(crate) const COMPACT_KEY: u64 = 3 << 62;

// A shard covers 1,024 independently mapped groups with eight raw names each.
// Keeping a relative code lets the scan carry each ordinary click in 16 bytes.
const SHARD_BITS: usize = 13;
const SHARD_KEYS: usize = 1 << SHARD_BITS;
const SHARD_MASK: usize = SHARD_KEYS - 1;
const CAMPAIGN_LIMIT: u32 = 1 << (31 - SHARD_BITS);
const WIDE: u32 = u32::MAX;

#[derive(Clone, Copy)]
struct RawClick {
    id: u64,
    time: u32,
    campaign_and_name: u32,
}

#[derive(Default)]
struct Shard {
    offsets: Vec<u32>,
    rows: Vec<Interaction<u32>>,
}

impl Shard {
    #[inline]
    fn get(&self, local: usize) -> &[Interaction<u32>] {
        if self.offsets.is_empty() {
            return &[];
        }
        &self.rows[self.offsets[local] as usize..self.offsets[local + 1] as usize]
    }
}

pub(crate) struct ClickIndex {
    shards: Vec<Shard>,
    wide: FastMap<u64, Vec<Interaction<u32>>>,
    oversized_campaigns: FastMap<u32, Vec<Interaction<u32>>>,
}

impl ClickIndex {
    #[inline]
    pub(crate) fn get(&self, person: u64) -> &[Interaction<u32>] {
        if person & COMPACT_KEY != COMPACT_KEY {
            return self.wide.get(&person).map(Vec::as_slice).unwrap_or(&[]);
        }
        let code = person as u32;
        if !self.oversized_campaigns.is_empty() {
            if let Some(rows) = self.oversized_campaigns.get(&code) {
                return rows;
            }
        }
        self.get_compact(code)
    }

    #[inline]
    fn get_compact(&self, code: u32) -> &[Interaction<u32>] {
        let code = (code - 1) as usize;
        self.shards[code >> SHARD_BITS].get(code & SHARD_MASK)
    }
}

struct ClickBatch {
    compact: Vec<Vec<RawClick>>,
    wide: Vec<Click<u32>>,
    processed: usize,
}

impl ClickBatch {
    fn new(shards: usize) -> Self {
        Self {
            compact: (0..shards).map(|_| Vec::new()).collect(),
            wide: Vec::new(),
            processed: 0,
        }
    }

    fn read(
        &mut self,
        data: &Dataset<'_>,
        input: &[u8],
        users: UserLayout,
        clock: &mut csv::Clock,
    ) {
        let base = data.start - WEEK;
        csv::rows::<4>(input, |[id, user, campaign, stamp]| {
            if id.is_empty() || user.is_empty() {
                return;
            }
            let Some(&campaign) = data.dimensions.campaigns.get(campaign) else {
                return;
            };
            let Some(stamp) = clock.timestamp(stamp) else {
                return;
            };
            if !(base..data.end).contains(&stamp) {
                return;
            }
            let id = data.click_ids.key(id);
            let time = (stamp - base) as u32;
            let code = users.encode(user);
            if let Some(code) = code.filter(|_| campaign < CAMPAIGN_LIMIT) {
                let code = (code - 1) as usize;
                let campaign_and_name = (campaign << SHARD_BITS) | (code & SHARD_MASK) as u32;
                self.compact[code >> SHARD_BITS].push(RawClick {
                    id,
                    time,
                    campaign_and_name,
                });
            } else {
                let person = code
                    .map(|code| COMPACT_KEY | u64::from(code))
                    .unwrap_or_else(|| data.names.key(user));
                self.wide.push(Click {
                    person,
                    id,
                    time,
                    campaign,
                });
            }
        });
        self.processed += 1;
    }
}

struct Builder {
    inputs: Vec<Mutex<Vec<Vec<RawClick>>>>,
    output: Vec<OnceLock<Shard>>,
    wide: Mutex<Vec<Vec<Click<u32>>>>,
    // A shard starts only after input publication from all scan workers.
    remaining: AtomicUsize,
    available: (Mutex<()>, Condvar),
}

impl Builder {
    fn new(groups: usize, click_chunks: usize) -> Self {
        let shards = groups.div_ceil(SHARD_KEYS / 8);
        Self {
            inputs: (0..shards).map(|_| Mutex::new(Vec::new())).collect(),
            output: (0..shards).map(|_| OnceLock::new()).collect(),
            wide: Mutex::new(Vec::new()),
            remaining: AtomicUsize::new(click_chunks),
            available: (Mutex::new(()), Condvar::new()),
        }
    }

    fn wake(&self) {
        let _guard = self.available.0.lock().unwrap();
        self.available.1.notify_all();
    }

    fn publish(&self, batch: ClickBatch) {
        if batch.processed == 0 {
            return;
        }
        for (target, stripe) in self.inputs.iter().zip(batch.compact) {
            if !stripe.is_empty() {
                target.lock().unwrap().push(stripe);
            }
        }
        if !batch.wide.is_empty() {
            self.wide.lock().unwrap().push(batch.wide);
        }
        if self.remaining.fetch_sub(batch.processed, Ordering::AcqRel) == batch.processed {
            self.wake();
        }
    }

    #[inline]
    fn parsed(&self) -> bool {
        self.remaining.load(Ordering::Acquire) == 0
    }

    fn wait_until_ready<'a>(&self, links: &'a OnceLock<LinkDirectory>) -> &'a LinkDirectory {
        let mut guard = self.available.0.lock().unwrap();
        loop {
            if self.parsed() {
                if let Some(aliases) = links.get() {
                    return aliases;
                }
            }
            guard = self.available.1.wait(guard).unwrap();
        }
    }

    fn build_shard(
        &self,
        index: usize,
        links: &LinkDirectory,
        names: &Names,
        memo: &mut Vec<u64>,
    ) -> Vec<Click<u32>> {
        let mut inputs = std::mem::take(&mut *self.inputs[index].lock().unwrap());
        if inputs.is_empty() {
            self.output[index].set(Shard::default()).ok().unwrap();
            return Vec::new();
        }

        let first = index * SHARD_KEYS;
        let count = (links.layout.groups * 8 - first).min(SHARD_KEYS);
        memo.clear();
        memo.resize(count, 0);
        let mut offsets = vec![0u32; count + 1];
        let mut crossed = Vec::new();
        for row in inputs.iter_mut().flatten() {
            let local = row.campaign_and_name as usize & SHARD_MASK;
            let key = &mut memo[local];
            if *key == 0 {
                *key = canonical_code(links, (first + local + 1) as u32, names);
            }
            let campaign = row.campaign_and_name >> SHARD_BITS;
            let person = *key;
            if person & COMPACT_KEY == COMPACT_KEY {
                let root = (person as u32 - 1) as usize - first;
                debug_assert!(root < count);
                offsets[root + 1] += 1;
                row.campaign_and_name = (campaign << SHARD_BITS) | root as u32;
            } else {
                crossed.push(Click {
                    person,
                    id: row.id,
                    time: row.time,
                    campaign,
                });
                row.campaign_and_name = WIDE;
            }
        }

        let mut total = 0;
        for offset in &mut offsets[1..] {
            total += *offset;
            *offset = total;
        }
        let mut cursors = offsets[..count].to_vec();
        let mut rows = Vec::<MaybeUninit<Interaction<u32>>>::with_capacity(total as usize);
        rows.resize_with(total as usize, MaybeUninit::uninit);
        for row in inputs.into_iter().flatten() {
            if row.campaign_and_name == WIDE {
                continue;
            }
            let local = row.campaign_and_name as usize & SHARD_MASK;
            let cursor = &mut cursors[local];
            rows[*cursor as usize].write(Interaction {
                id: row.id,
                time: row.time,
                campaign: row.campaign_and_name >> SHARD_BITS,
            });
            *cursor += 1;
        }
        // Each non-spill row was counted once and initializes its own slot.
        let mut rows = unsafe {
            let mut rows = std::mem::ManuallyDrop::new(rows);
            Vec::from_raw_parts(
                rows.as_mut_ptr().cast::<Interaction<u32>>(),
                rows.len(),
                rows.capacity(),
            )
        };
        for offsets in offsets.windows(2) {
            let range = offsets[0] as usize..offsets[1] as usize;
            if range.len() > 1 {
                rows[range].sort_unstable_by_key(|row| row.time);
            }
        }
        self.output[index]
            .set(Shard { offsets, rows })
            .ok()
            .unwrap();
        crossed
    }

    fn finish(
        self,
        links: &LinkDirectory,
        names: &Names,
        crossed: Vec<Vec<Click<u32>>>,
    ) -> ClickIndex {
        let mut index = ClickIndex {
            shards: self
                .output
                .into_iter()
                .map(|shard| shard.into_inner().unwrap())
                .collect(),
            wide: FastMap::default(),
            oversized_campaigns: FastMap::default(),
        };
        let mut extra = FastMap::<u32, Vec<Interaction<u32>>>::default();
        for click in self.wide.into_inner().unwrap().into_iter().flatten() {
            let person = if click.person & COMPACT_KEY == COMPACT_KEY {
                canonical_code(links, click.person as u32, names)
            } else {
                links.resolve_name(click.person)
            };
            if person & COMPACT_KEY == COMPACT_KEY {
                extra
                    .entry(person as u32)
                    .or_default()
                    .push((&click).into());
            } else {
                index.wide.entry(person).or_default().push((&click).into());
            }
        }
        for click in crossed.into_iter().flatten() {
            index
                .wide
                .entry(click.person)
                .or_default()
                .push((&click).into());
        }
        for rows in index.wide.values_mut() {
            rows.sort_unstable_by_key(|row| row.time);
        }
        for (code, rows) in &mut extra {
            rows.extend_from_slice(index.get_compact(*code));
            rows.sort_unstable_by_key(|row| row.time);
        }
        index.oversized_campaigns = extra;
        index
    }
}

#[inline]
fn canonical_code(links: &LinkDirectory, code: u32, names: &Names) -> u64 {
    match links.resolve_code(code, names) {
        Canonical::Code(code) => COMPACT_KEY | u64::from(code),
        Canonical::Wide(name) => name,
    }
}

fn team<T: Send>(workers: usize, run: &(impl Fn() -> T + Sync)) -> Vec<T> {
    thread::scope(|scope| {
        let children = (1..workers).map(|_| scope.spawn(run)).collect::<Vec<_>>();
        let mut output = vec![run()];
        output.extend(children.into_iter().map(|child| child.join().unwrap()));
        output
    })
}

/// The link builder temporarily reserves workers from the same team. Once
/// links publish, that reservation is returned to the event and click queue.
pub(crate) fn scan<T: Send>(
    data: &Dataset<'_>,
    users: UserLayout,
    link_files: &[csv::Mapped],
    clicks: &[&[u8]],
    event_jobs: usize,
    threads: usize,
    init_event: impl Fn() -> T + Sync,
    visit_event: impl Fn(usize, &mut T) + Sync,
) -> (Vec<T>, LinkDirectory, ClickIndex) {
    let threads = threads.max(1);
    let link_workers = (threads / 4).max(1);
    let builder = Builder::new(users.groups, clicks.len());
    let links = OnceLock::new();
    let next_click = AtomicUsize::new(0);
    let next_event = AtomicUsize::new(0);
    let next_shard = AtomicUsize::new(0);

    let build_links = || {
        let mut clock = Timer(Instant::now());
        links
            .set(LinkDirectory::build(
                link_files,
                users,
                data.names,
                link_workers,
            ))
            .ok()
            .unwrap();
        builder.wake();
        clock.lap("  Build parallel exact link directory (overlapped)");
    };
    let run = || {
        if let Some(input) = clicks.get(next_click.fetch_add(1, Ordering::Relaxed)) {
            let mut batch = ClickBatch::new(builder.inputs.len());
            let mut clock = csv::Clock::new();
            batch.read(data, input, users, &mut clock);
            while let Some(input) = clicks.get(next_click.fetch_add(1, Ordering::Relaxed)) {
                batch.read(data, input, users, &mut clock);
            }
            builder.publish(batch);
        }

        let mut staged = init_event();
        let mut events_pending = true;
        let mut shards_pending = !builder.inputs.is_empty();
        let mut crossed = Vec::new();
        let mut memo = Vec::new();
        loop {
            if shards_pending && builder.parsed() {
                if let Some(dir) = links.get() {
                    let shard = next_shard.fetch_add(1, Ordering::Relaxed);
                    if shard < builder.inputs.len() {
                        crossed.extend(builder.build_shard(shard, dir, data.names, &mut memo));
                        continue;
                    }
                    shards_pending = false;
                }
            }
            if events_pending {
                let event = next_event.fetch_add(1, Ordering::Relaxed);
                if event < event_jobs {
                    visit_event(event, &mut staged);
                    continue;
                }
                events_pending = false;
            }
            if !shards_pending {
                break;
            }
            builder.wait_until_ready(&links);
        }
        (staged, crossed)
    };

    let workers = if threads == 1 {
        build_links();
        team(1, &run)
    } else {
        thread::scope(|scope| {
            let linker = scope.spawn(|| {
                build_links();
                team(link_workers, &run)
            });
            let mut workers = team(threads - link_workers, &run);
            workers.extend(linker.join().unwrap());
            workers
        })
    };
    let (staged, crossed): (Vec<_>, Vec<_>) = workers.into_iter().unzip();
    let links = links.into_inner().unwrap();
    let index = builder.finish(&links, data.names, crossed);
    (staged, links, index)
}
