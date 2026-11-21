use crate::keys::{FastMap, Names};
use crate::{component_root, csv, owner, stripes, work, LinkNode, Parts, CHUNK, LANES};
use std::array;
use std::io;
use std::path::Path;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Mutex;

struct Endpoint {
    key: u64,
    slot: u32,
}

type Edges = Vec<(u64, u64)>;

/// Parallelize exact identifier indexing by endpoint owner. The serial
/// union thereafter works on compact integers and makes no hash lookups.
pub(crate) fn indexed_links(
    path: &Path,
    names: &Names,
    threads: usize,
) -> io::Result<Vec<FastMap<u64, u64>>> {
    let files = csv::map_dir(&path.join("links"))?;
    let chunks = csv::chunks(&files, CHUNK / 4);
    let batches: Vec<Edges> = work(chunks.len(), threads, Vec::new, |index, local| {
        csv::rows::<2>(chunks[index], |[a, b]| {
            if !a.is_empty() && !b.is_empty() && a != b {
                local.push((names.key(a), names.key(b)));
            }
        });
    });

    let mut next = 0;
    let batches: Vec<_> = batches
        .into_iter()
        .map(|edges| {
            let start = next;
            next += edges.len() * 2;
            Mutex::new(Some((start, edges)))
        })
        .collect();
    if next >= u32::MAX as usize {
        return Err(io::Error::other("too many link endpoints"));
    }
    let parts: Vec<Parts<Endpoint>> = work(
        batches.len(),
        threads,
        || array::from_fn(|_| Vec::new()),
        |job, parts| {
            let (start, edges) = batches[job].lock().unwrap().take().unwrap();
            for (index, (a, b)) in edges.into_iter().enumerate() {
                let slot = (start + index * 2) as u32;
                parts[owner(a)].push(Endpoint { key: a, slot });
                parts[owner(b)].push(Endpoint {
                    key: b,
                    slot: slot + 1,
                });
            }
        },
    );

    let mut boundaries = Vec::with_capacity(LANES);
    let mut capacity = 0u32;
    let jobs: Vec<_> = stripes(parts)
        .into_iter()
        .map(|endpoints| {
            let offset = capacity;
            capacity += endpoints.iter().map(|rows| rows.len() as u32).sum::<u32>();
            boundaries.push(offset);
            Mutex::new(Some(endpoints))
        })
        .collect();

    let edges: Vec<_> = (0..next).map(|_| AtomicU32::new(0)).collect();
    let indexes = work(jobs.len(), threads, Vec::new, |lane, owners| {
        let endpoints = jobs[lane].lock().unwrap().take().unwrap();
        let count = endpoints.iter().map(Vec::len).sum();
        let mut index = FastMap::with_capacity_and_hasher(count, Default::default());
        let mut fresh = boundaries[lane];
        for row in endpoints.into_iter().flatten() {
            let id = *index.entry(row.key).or_insert_with(|| {
                let id = fresh;
                fresh += 1;
                u64::from(id)
            });
            edges[row.slot as usize].store(id as u32, Ordering::Relaxed);
        }
        owners.push((lane, index));
    });

    let mut nodes: Vec<_> = (0..capacity)
        .map(|id| LinkNode {
            parent: id,
            size: 1,
            least: 0,
        })
        .collect();
    let mut indexes: Vec<_> = indexes.into_iter().flatten().collect();
    for (_, index) in &indexes {
        for (&key, &node) in index {
            nodes[node as usize].least = key;
        }
    }
    for pair in edges.chunks_exact(2) {
        let a = pair[0].load(Ordering::Relaxed);
        let b = pair[1].load(Ordering::Relaxed);
        let mut a = component_root(&mut nodes, a) as usize;
        let mut b = component_root(&mut nodes, b) as usize;
        if a == b {
            continue;
        }
        if nodes[a].size < nodes[b].size {
            std::mem::swap(&mut a, &mut b);
        }
        nodes[b].parent = a as u32;
        nodes[a].size += nodes[b].size;
        if names.compare(nodes[b].least, nodes[a].least).is_lt() {
            nodes[a].least = nodes[b].least;
        }
    }
    drop(edges);

    indexes.sort_unstable_by_key(|(lane, _)| *lane);
    let mut result: Vec<_> = indexes.into_iter().map(|(_, index)| index).collect();
    let piece = result.len().div_ceil(threads);
    thread_finalize(&mut result, &nodes, piece);
    Ok(result)
}

fn thread_finalize(lanes: &mut [FastMap<u64, u64>], nodes: &[LinkNode], piece: usize) {
    std::thread::scope(|scope| {
        for multiple in lanes.chunks_mut(piece) {
            scope.spawn(move || {
                for lane in multiple {
                    for value in lane.values_mut() {
                        let mut node = *value as usize;
                        while nodes[node].parent as usize != node {
                            node = nodes[node].parent as usize;
                        }
                        *value = nodes[node].least;
                    }
                }
            });
        }
    });
}
