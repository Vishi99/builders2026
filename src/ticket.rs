use crate::aggregate::Event;
use crate::keys::{spread, FastMap};
use crate::{owner, stripes, work, AliasIndex, Cache, CopyRecord, Parts, LANES, ZERO_CACHE};
use std::array;
use std::sync::Mutex;

const OWNER_BITS: u32 = LANES.trailing_zeros();
const KEY_BITS: u32 = 45 - OWNER_BITS;
const KEY_MASK: u64 = (1 << KEY_BITS) - 1;
const WIDE: u32 = 1 << 31;
const STRIPE_BITS: u32 = 24;
const ROW_MASK: u32 = (1 << STRIPE_BITS) - 1;

/// A whole-second ingest close to the window and an exact ID fit next
/// to a compact payload in 24 bytes. The owner supplies the low key bits.
pub(crate) struct Ticket<T> {
    key_rank: u64,
    event: Event<T>,
}

impl<T> Ticket<T> {
    pub fn pack(copy: CopyRecord<T>, base_seconds: i64) -> Result<Self, CopyRecord<T>> {
        debug_assert!(copy.id < 1u64 << 45);
        let rank = (copy.ingest / 1000 - base_seconds) as u64;
        if copy.ingest % 1000 != 0 || rank >= (1 << (64 - KEY_BITS)) {
            return Err(copy);
        }
        Ok(Self {
            key_rank: (rank << KEY_BITS) | copy.id >> OWNER_BITS,
            event: copy.event,
        })
    }

    #[inline]
    fn key(&self) -> u64 {
        self.key_rank & KEY_MASK
    }

    #[inline]
    fn rank(&self) -> u64 {
        self.key_rank >> KEY_BITS
    }
}

struct Slots {
    main: Vec<u32>,
    collisions: FastMap<u64, u32>,
}

impl Slots {
    fn new(count: usize) -> Self {
        let size = count.clamp(16, 1 << 22).next_power_of_two();
        Self {
            main: vec![0; size],
            collisions: FastMap::default(),
        }
    }

    #[inline]
    fn select(&mut self, key: u64, key_at: impl FnOnce(u32) -> u64) -> &mut u32 {
        let index = key as usize & (self.main.len() - 1);
        let slot = &mut self.main[index];
        if *slot == 0 || key_at(*slot) == key {
            slot
        } else {
            self.collisions.entry(key).or_default()
        }
    }
}

#[inline]
fn location(handle: u32) -> (usize, usize) {
    (
        ((handle & !WIDE) >> STRIPE_BITS) as usize,
        ((handle & ROW_MASK) - 1) as usize,
    )
}

#[inline]
fn encode(stripe: usize, index: usize) -> u32 {
    ((stripe as u32) << STRIPE_BITS) | (index as u32 + 1)
}

fn fits_handles<T>(rows: &[Vec<T>]) -> bool {
    rows.len() <= (WIDE >> STRIPE_BITS) as usize
        && rows.iter().all(|stripe| stripe.len() <= ROW_MASK as usize)
}

struct Emitted<'a, T> {
    owners: &'a mut Parts<Event<T>>,
    aliases: &'a AliasIndex,
    cache: &'a mut [Cache],
}

impl<T> Emitted<'_, T> {
    #[inline]
    fn add(&mut self, mut event: Event<T>) {
        let raw = event.person;
        if raw == 0 {
            return;
        }
        let hash = spread(raw);
        let line = &mut self.cache[hash as usize & 8191];
        if raw != line.raw {
            let person = self.aliases.person(raw, hash);
            let owner = if raw == person {
                (hash >> 32) as usize & (LANES - 1)
            } else {
                owner(person)
            };
            *line = Cache { raw, person, owner };
        }
        event.person = line.person;
        let rows = &mut self.owners[line.owner];
        rows.push(event);
    }
}

fn compact_pass<T>(compact: &mut [Vec<Ticket<T>>], slots: &mut Slots) {
    for stripe in 0..compact.len() {
        for index in 0..compact[stripe].len() {
            let row = &compact[stripe][index];
            let slot = slots.select(row.key(), |previous| {
                let (stripe, index) = location(previous);
                compact[stripe][index].key()
            });
            if *slot != 0 {
                let (earlier_stripe, earlier_index) = location(*slot);
                if row.rank() >= compact[earlier_stripe][earlier_index].rank() {
                    compact[stripe][index].event.person = 0;
                    continue;
                }
                compact[earlier_stripe][earlier_index].event.person = 0;
            }
            *slot = encode(stripe, index);
        }
    }
}

fn wide_pass<T>(
    compact: &mut [Vec<Ticket<T>>],
    wide: &mut [Vec<CopyRecord<T>>],
    slots: &mut Slots,
    base_seconds: i64,
) {
    for stripe in 0..wide.len() {
        for index in 0..wide[stripe].len() {
            let row = &wide[stripe][index];
            let key = row.id >> OWNER_BITS;
            let slot = slots.select(key, |previous| {
                let (stripe, index) = location(previous);
                if previous & WIDE == 0 {
                    compact[stripe][index].key()
                } else {
                    wide[stripe][index].id >> OWNER_BITS
                }
            });
            if *slot != 0 {
                let (earlier_stripe, earlier_index) = location(*slot);
                let stamp = if *slot & WIDE == 0 {
                    let entry = &compact[earlier_stripe][earlier_index];
                    (base_seconds + entry.rank() as i64) * 1000
                } else {
                    wide[earlier_stripe][earlier_index].ingest
                };
                if row.ingest >= stamp {
                    wide[stripe][index].event.person = 0;
                    continue;
                }
                if *slot & WIDE == 0 {
                    compact[earlier_stripe][earlier_index].event.person = 0;
                } else {
                    wide[earlier_stripe][earlier_index].event.person = 0;
                }
            }
            *slot = WIDE | encode(stripe, index);
        }
    }
}

fn expanded_pass<T>(
    compact: Vec<Vec<Ticket<T>>>,
    wide: Vec<Vec<CopyRecord<T>>>,
    out: &mut Emitted<'_, T>,
    base_seconds: i64,
) {
    let mut winners = FastMap::<u64, (i64, Event<T>)>::default();
    let compact = compact.into_iter().flatten().map(|entry| {
        (
            entry.key(),
            (base_seconds + entry.rank() as i64) * 1000,
            entry.event,
        )
    });
    let wide = wide
        .into_iter()
        .flatten()
        .map(|entry| (entry.id >> OWNER_BITS, entry.ingest, entry.event));
    for (key, ingest, event) in compact.chain(wide) {
        use std::collections::hash_map::Entry;
        match winners.entry(key) {
            Entry::Vacant(slot) => {
                slot.insert((ingest, event));
            }
            Entry::Occupied(mut slot) if ingest < slot.get().0 => {
                slot.insert((ingest, event));
            }
            _ => {}
        }
    }
    for (_, event) in winners.into_values() {
        out.add(event);
    }
}

pub(crate) fn route<T: Copy + Send>(
    tickets: Vec<Parts<Ticket<T>>>,
    overflow: Vec<Parts<CopyRecord<T>>>,
    threads: usize,
    aliases: &AliasIndex,
    base_seconds: i64,
) -> Vec<Parts<Event<T>>> {
    let planned = tickets.iter().flatten().map(Vec::len).sum::<usize>()
        + overflow.iter().flatten().map(Vec::len).sum::<usize>();
    let reserve = (planned / threads / LANES).saturating_mul(5) / 4;
    let owners = stripes(tickets).into_iter().zip(stripes(overflow));
    let jobs: Vec<_> = owners.map(|lane| Mutex::new(Some(lane))).collect();
    work(
        jobs.len(),
        threads,
        || {
            (
                array::from_fn(|_| Vec::with_capacity(reserve)),
                vec![ZERO_CACHE; 8192],
            )
        },
        |lane, result| {
            let (mut compact, mut wide) = jobs[lane].lock().unwrap().take().unwrap();
            let count = compact.iter().map(Vec::len).sum::<usize>()
                + wide.iter().map(Vec::len).sum::<usize>();
            if count == 0 {
                return;
            }
            let mut emitted = Emitted {
                owners: &mut result.0,
                cache: &mut result.1,
                aliases,
            };
            if !fits_handles(&compact) || !fits_handles(&wide) {
                expanded_pass(compact, wide, &mut emitted, base_seconds);
                return;
            }
            let mut slots = Slots::new(count);
            compact_pass(&mut compact, &mut slots);
            wide_pass(&mut compact, &mut wide, &mut slots, base_seconds);
            for row in compact.into_iter().flatten() {
                emitted.add(row.event);
            }
            for row in wide.into_iter().flatten() {
                emitted.add(row.event);
            }
        },
    )
    .into_iter()
    .map(|(owners, _)| owners)
    .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn exact_keys_and_outside_window_ingests_compete() {
        let lane = 7;
        let key = |suffix: u64| (suffix << OWNER_BITS) | lane as u64;
        let row = |id, ingest, person| CopyRecord {
            id,
            ingest,
            event: Event {
                person,
                time: 12u32,
                purchase: 0,
            },
        };
        let base = 1000;
        let mut compact: Parts<Ticket<u32>> = array::from_fn(|_| Vec::new());
        let mut wide: Parts<CopyRecord<u32>> = array::from_fn(|_| Vec::new());
        for copy in [
            row(key(1), 1_001_000, 10),
            row(key(17), 1_002_000, 20),
            row(key(1), 1_000_000, 0),
            row(key(3), 1_000_000, 30),
            row(key(17), 10_000, 21),
            row(key(3), 20_000, 31),
            row(key(3), 30_000, 32),
        ] {
            match Ticket::pack(copy, base) {
                Ok(ticket) => compact[lane].push(ticket),
                Err(copy) => wide[lane].push(copy),
            }
        }
        assert_eq!(std::mem::size_of::<Ticket<u32>>(), 24);
        let aliases = AliasIndex {
            maps: vec![FastMap::default()],
            filter: Vec::new(),
        };
        let mut winners: Vec<_> = route(vec![compact], vec![wide], 2, &aliases, base)
            .into_iter()
            .flatten()
            .flatten()
            .map(|event| event.person)
            .collect();
        winners.sort_unstable();
        assert_eq!(winners, vec![21, 31]);
    }
}
