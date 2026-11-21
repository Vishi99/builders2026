//! Exact linked-component directory for compact user codes.

use crate::csv;
use crate::keys::{FastMap, Names};
use crate::user_rank::UserLayout;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::thread;

#[derive(Clone, Copy)]
enum Endpoint {
    Code(u32),
    Wide(u64),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Canonical {
    Code(u32),
    Wide(u64),
}

pub struct LinkDirectory {
    pub layout: UserLayout,
    local: Vec<u32>,
    crossed: Vec<u8>,
    cross_canonical: FastMap<u64, u64>,
}

impl LinkDirectory {
    pub fn build(files: &[csv::Mapped], layout: UserLayout, names: &Names, workers: usize) -> Self {
        let workers = workers.max(1);
        let edges = (0..layout.groups)
            .map(|_| AtomicU64::new(0))
            .collect::<Vec<_>>();
        let jobs = csv::chunks(files, 128 << 10);
        let next_job = AtomicUsize::new(0);
        let mut escaped = thread::scope(|scope| {
            let handles = (0..workers)
                .map(|_| {
                    scope.spawn(|| {
                        let mut escaped = Vec::new();
                        loop {
                            let job = next_job.fetch_add(1, Ordering::Relaxed);
                            let Some(input) = jobs.get(job) else {
                                break;
                            };
                            csv::rows::<2>(input, |[a, b]| {
                                if a == b || a.is_empty() || b.is_empty() {
                                    return;
                                }
                                let left = layout.encode(a);
                                let right = layout.encode(b);
                                if let (Some(left), Some(right)) = (left, right) {
                                    let (group, a) = split(left);
                                    let (group_b, b) = split(right);
                                    if group == group_b {
                                        let bits = (1u64 << (a * 8 + b)) | (1u64 << (b * 8 + a));
                                        edges[group].fetch_or(bits, Ordering::Relaxed);
                                        return;
                                    }
                                }
                                let endpoint = |raw, compact: Option<u32>| {
                                    compact.map_or_else(
                                        || Endpoint::Wide(names.key(raw)),
                                        Endpoint::Code,
                                    )
                                };
                                escaped.push((endpoint(a, left), endpoint(b, right)));
                            });
                        }
                        escaped
                    })
                })
                .collect::<Vec<_>>();
            handles
                .into_iter()
                .flat_map(|handle| handle.join().unwrap())
                .collect::<Vec<_>>()
        });

        let mut local = vec![0u32; layout.groups];
        let group_block = layout.groups.div_ceil(workers).max(1);
        thread::scope(|scope| {
            for (block, (output, input)) in local
                .chunks_mut(group_block)
                .zip(edges.chunks(group_block))
                .enumerate()
            {
                scope.spawn(move || {
                    let first = block * group_block;
                    for (offset, (roots, mask)) in output.iter_mut().zip(input).enumerate() {
                        let mask = mask.load(Ordering::Relaxed);
                        if mask != 0 {
                            *roots = packed_components(first + offset, mask, layout);
                        }
                    }
                });
            }
        });
        drop(edges);

        let mut directory = Self {
            layout,
            local,
            crossed: vec![0u8; layout.groups],
            cross_canonical: FastMap::default(),
        };
        let mut cross = CrossComponents::default();
        for (left, right) in escaped.drain(..) {
            let left = directory.local_name(left, names);
            let right = directory.local_name(right, names);
            cross.join(left, right, names);
        }
        directory.cross_canonical = cross.finish();
        directory
    }

    #[inline]
    pub fn resolve_name(&self, raw: u64) -> u64 {
        self.cross_canonical.get(&raw).copied().unwrap_or(raw)
    }

    #[inline]
    pub fn resolve_code(&self, code: u32, names: &Names) -> Canonical {
        let (group, alias) = self.local_component(code);
        let code = combine(group, alias);
        if self.crossed[group] & (1u8 << alias) == 0 {
            Canonical::Code(code)
        } else {
            Canonical::Wide(self.resolve_name(self.layout.name(code, names)))
        }
    }

    #[inline]
    fn local_component(&self, code: u32) -> (usize, u32) {
        let (group, alias) = split(code);
        let representative = (self.local[group] >> (alias * 4)) & 15;
        let alias = if representative == 0 {
            alias
        } else {
            representative - 1
        };
        (group, alias)
    }

    fn local_name(&mut self, endpoint: Endpoint, names: &Names) -> u64 {
        match endpoint {
            Endpoint::Wide(name) => name,
            Endpoint::Code(code) => {
                let (group, alias) = self.local_component(code);
                self.crossed[group] |= 1u8 << alias;
                self.layout.name(combine(group, alias), names)
            }
        }
    }
}

#[inline]
fn split(code: u32) -> (usize, u32) {
    let raw = code - 1;
    ((raw >> 3) as usize, raw & 7)
}

#[inline]
fn combine(group: usize, alias: u32) -> u32 {
    ((group as u32) << 3) + alias + 1
}

fn packed_components(group: usize, mut edges: u64, layout: UserLayout) -> u32 {
    let mut roots = 0;
    while edges != 0 {
        let first = edges.trailing_zeros() / 8;
        let mut queue = 1u8 << first;
        let mut seen = queue;
        let mut representative = first;
        let mut least = layout.lexical(combine(group, first));
        while queue != 0 {
            let alias = queue.trailing_zeros();
            queue &= queue - 1;
            let neighbors = (edges >> (alias * 8)) as u8;
            edges &= !(255u64 << (alias * 8));
            let discover = neighbors & !seen;
            queue |= discover;
            seen |= discover;
            let order = layout.lexical(combine(group, alias));
            if order < least {
                representative = alias;
                least = order;
            }
        }
        while seen != 0 {
            let alias = seen.trailing_zeros();
            seen &= seen - 1;
            roots |= (representative + 1) << (alias * 4);
        }
    }
    roots
}

#[derive(Default)]
struct CrossComponents {
    ids: FastMap<u64, u32>,
    nodes: Vec<CrossNode>,
}

struct CrossNode {
    parent: u32,
    size: u32,
    least: u64,
}

impl CrossComponents {
    fn add(&mut self, name: u64) -> u32 {
        *self.ids.entry(name).or_insert_with(|| {
            let id = self
                .nodes
                .len()
                .try_into()
                .expect("too many cross-group link IDs");
            self.nodes.push(CrossNode {
                parent: id,
                size: 1,
                least: name,
            });
            id
        })
    }

    fn join(&mut self, a: u64, b: u64, names: &Names) {
        let a = self.add(a);
        let b = self.add(b);
        let mut a = cross_find(&mut self.nodes, a) as usize;
        let mut b = cross_find(&mut self.nodes, b) as usize;
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

    fn finish(mut self) -> FastMap<u64, u64> {
        self.ids
            .into_iter()
            .map(|(name, node)| {
                let root = cross_find(&mut self.nodes, node);
                (name, self.nodes[root as usize].least)
            })
            .collect()
    }
}

fn cross_find(nodes: &mut [CrossNode], mut index: u32) -> u32 {
    while nodes[index as usize].parent != index {
        let parent = nodes[index as usize].parent as usize;
        let grandparent = nodes[parent].parent;
        nodes[index as usize].parent = grandparent;
        index = grandparent;
    }
    index
}
