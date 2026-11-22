//! Linked IDs: dense node ids per key partition, lock-free union-find, and
//! the byte-wise smallest key of each component.

use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};

#[repr(C)]
#[derive(Clone, Copy)]
pub struct KRec {
    pub key: u64,
    /// Thread (high 5 bits) and endpoint index (edge * 2 + side).
    pub eref: u32,
    pub _pad: u32,
}

pub const EREF_SHIFT: u32 = 27;

/// Keys of one key partition: an open-addressing table and keys by local id.
pub struct KeyPart {
    pub slots: Vec<u32>, // local id + 1, 0 empty
    pub keys: Vec<u64>,
    pub base: u32,
}

#[inline(always)]
pub fn slot_hash(k: u64, bits: u32) -> usize {
    (k.wrapping_mul(0xD6E8_FEB8_6659_FD93) >> (64 - bits)) as usize
}

impl KeyPart {
    pub fn build(recs: &mut dyn Iterator<Item = u64>, n: usize) -> KeyPart {
        let bits = (2 * n.max(8)).next_power_of_two().trailing_zeros();
        let mask = (1usize << bits) - 1;
        let mut slots = vec![0u32; 1 << bits];
        let mut keys = Vec::with_capacity(n);
        for k in recs {
            let mut h = slot_hash(k, bits);
            loop {
                let s = slots[h];
                if s == 0 {
                    keys.push(k);
                    slots[h] = keys.len() as u32;
                    break;
                }
                if keys[(s - 1) as usize] == k {
                    break;
                }
                h = (h + 1) & mask;
            }
        }
        KeyPart { slots, keys, base: 0 }
    }

    #[inline(always)]
    pub fn find(&self, k: u64) -> u32 {
        let bits = self.slots.len().trailing_zeros();
        let mask = self.slots.len() - 1;
        let mut h = slot_hash(k, bits);
        loop {
            let s = self.slots[h];
            debug_assert!(s != 0);
            if self.keys[(s - 1) as usize] == k {
                return s - 1;
            }
            h = (h + 1) & mask;
        }
    }
}

pub struct Forest {
    pub parent: Vec<AtomicU32>,
    pub minkey: Vec<AtomicU64>,
}

impl Forest {
    pub fn new(n: usize) -> Forest {
        Forest {
            parent: (0..n as u32).map(AtomicU32::new).collect(),
            minkey: (0..n).map(|_| AtomicU64::new(u64::MAX)).collect(),
        }
    }

    #[inline(always)]
    pub fn find(&self, mut x: u32) -> u32 {
        loop {
            let p = self.parent[x as usize].load(Ordering::Relaxed);
            if p == x {
                return x;
            }
            let g = self.parent[p as usize].load(Ordering::Relaxed);
            if g != p {
                self.parent[x as usize].store(g, Ordering::Relaxed);
            }
            x = g;
        }
    }

    #[inline(always)]
    pub fn union(&self, a: u32, b: u32) {
        let (mut a, mut b) = (a, b);
        loop {
            a = self.find(a);
            b = self.find(b);
            if a == b {
                return;
            }
            let (hi, lo) = if a > b { (a, b) } else { (b, a) };
            if self.parent[hi as usize]
                .compare_exchange(hi, lo, Ordering::AcqRel, Ordering::Relaxed)
                .is_ok()
            {
                return;
            }
        }
    }
}
