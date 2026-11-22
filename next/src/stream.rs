//! Append-only record streams made of blocks from a thread pool.
//! One thread writes a stream; other threads read it after a barrier.
//!
//! Big streams use fixed 4 KiB blocks that are recycled: a thread that has
//! consumed a partition returns its blocks to its own free list, so later
//! phases write into memory that is already faulted in. Small streams use
//! geometrically growing blocks.

use crate::sys::Arena;

pub const BIG: usize = 4096;
const FIRST_SMALL: usize = 256;
const MAX_SMALL: usize = 16 << 10;
const HEADER: usize = 16;

#[repr(C)]
struct Header {
    next: *mut Header,
    used: usize,
}

pub struct Pool {
    pub arena: Arena,
    pub free: Vec<*mut u8>,
}

unsafe impl Send for Pool {}

impl Pool {
    pub fn new(region: usize) -> Pool {
        Pool { arena: Arena::new(region), free: Vec::new() }
    }
    #[inline]
    fn big(&mut self) -> *mut u8 {
        match self.free.pop() {
            Some(b) => b,
            None => self.arena.alloc(BIG, BIG),
        }
    }
}

#[derive(Clone, Copy)]
pub struct Stream {
    head: *mut Header,
    tail: *mut Header,
    cur: *mut u8,
    end: *mut u8,
    pub count: usize,
}

unsafe impl Send for Stream {}
unsafe impl Sync for Stream {}

impl Default for Stream {
    fn default() -> Self {
        Stream::new()
    }
}

impl Stream {
    pub const fn new() -> Stream {
        Stream {
            head: std::ptr::null_mut(),
            tail: std::ptr::null_mut(),
            cur: std::ptr::null_mut(),
            end: std::ptr::null_mut(),
            count: 0,
        }
    }

    #[inline(always)]
    pub fn push<T: Copy>(&mut self, pool: &mut Pool, value: T) -> *mut T {
        let size = std::mem::size_of::<T>();
        if (self.end as usize).wrapping_sub(self.cur as usize) < size || self.cur.is_null() {
            self.grow(pool, true);
        }
        unsafe {
            let at = self.cur as *mut T;
            std::ptr::write_unaligned(at, value);
            self.cur = self.cur.add(size);
            self.count += 1;
            at
        }
    }

    /// Visit each block's records mutably.
    #[inline]
    pub fn for_each_block_mut<T: Copy>(&self, mut f: impl FnMut(&mut [T])) {
        let size = std::mem::size_of::<T>();
        let mut block = self.head;
        while !block.is_null() {
            unsafe {
                let used = (*block).used;
                let data = (block as *mut u8).add(HEADER) as *mut T;
                f(std::slice::from_raw_parts_mut(data, used / size));
                block = (*block).next;
            }
        }
    }

    #[inline(always)]
    pub fn push_small<T: Copy>(&mut self, pool: &mut Pool, value: T) {
        let size = std::mem::size_of::<T>();
        if (self.end as usize).wrapping_sub(self.cur as usize) < size || self.cur.is_null() {
            self.grow(pool, false);
        }
        unsafe {
            std::ptr::write_unaligned(self.cur as *mut T, value);
            self.cur = self.cur.add(size);
        }
        self.count += 1;
    }

    #[cold]
    #[inline(never)]
    fn grow(&mut self, pool: &mut Pool, big: bool) {
        unsafe {
            if !self.tail.is_null() {
                (*self.tail).used = self.cur as usize - (self.tail as usize + HEADER);
            }
            let (block, size) = if big {
                (pool.big() as *mut Header, BIG)
            } else {
                let size = if self.tail.is_null() {
                    FIRST_SMALL
                } else {
                    ((self.end as usize - self.tail as usize) * 2).min(MAX_SMALL)
                };
                (pool.arena.alloc(size, 64) as *mut Header, size)
            };
            (*block).next = std::ptr::null_mut();
            (*block).used = 0;
            if self.tail.is_null() {
                self.head = block;
            } else {
                (*self.tail).next = block;
            }
            self.tail = block;
            self.cur = (block as *mut u8).add(HEADER);
            self.end = (block as *mut u8).add(size);
        }
    }

    /// Close the open block so readers see its length.
    pub fn finish(&mut self) {
        if !self.tail.is_null() {
            unsafe {
                (*self.tail).used = self.cur as usize - (self.tail as usize + HEADER);
            }
        }
    }

    /// Visit each block's records as a slice.
    #[inline]
    pub fn for_each_block<T: Copy>(&self, mut f: impl FnMut(&[T])) {
        let size = std::mem::size_of::<T>();
        let mut block = self.head;
        while !block.is_null() {
            unsafe {
                let used = (*block).used;
                let data = (block as *const u8).add(HEADER) as *const T;
                f(std::slice::from_raw_parts(data, used / size));
                block = (*block).next;
            }
        }
    }

    /// Hand a consumed big stream's blocks to `pool`.
    pub fn release(&mut self, pool: &mut Pool) {
        let mut block = self.head;
        while !block.is_null() {
            unsafe {
                let next = (*block).next;
                pool.free.push(block as *mut u8);
                block = next;
            }
        }
        *self = Stream::new();
    }
}
