//! Operating-system helpers: file mapping, huge-page arenas, spin barrier.

use std::ffi::c_void;
use std::fs::File;
use std::io;
use std::os::unix::io::AsRawFd;
use std::path::Path;
use std::sync::atomic::{AtomicUsize, Ordering};

extern "C" {
    fn mmap(addr: *mut c_void, len: usize, prot: i32, flags: i32, fd: i32, off: i64) -> *mut c_void;
    fn madvise(addr: *mut c_void, len: usize, advice: i32) -> i32;
}

const PROT_READ: i32 = 1;
const PROT_WRITE: i32 = 2;
const MAP_PRIVATE: i32 = 2;
const MAP_ANONYMOUS: i32 = 0x20;
const MAP_NORESERVE: i32 = 0x4000;
const MADV_HUGEPAGE: i32 = 14;
const MADV_DONTNEED: i32 = 4;

/// Drop page-table entries of a read-only file mapping range (the pages stay
/// in the page cache). Lets workers share the teardown instead of exit.
pub fn release_range(ptr: *const u8, len: usize) {
    let start = (ptr as usize + 4095) & !4095;
    let end = (ptr as usize + len) & !4095;
    if end > start {
        unsafe {
            madvise(start as *mut c_void, end - start, MADV_DONTNEED);
        }
    }
}

/// A read-only mapping of a whole file. Bytes past the end of the file up to
/// the end of its last page read as zero.
pub struct Mapped {
    pub ptr: *const u8,
    pub len: usize,
}

unsafe impl Send for Mapped {}
unsafe impl Sync for Mapped {}

impl Mapped {
    pub fn open(path: &Path) -> io::Result<Mapped> {
        let file = File::open(path)?;
        let len = file.metadata()?.len() as usize;
        if len == 0 {
            return Ok(Mapped { ptr: std::ptr::null(), len: 0 });
        }
        let ptr = unsafe { mmap(std::ptr::null_mut(), len, PROT_READ, MAP_PRIVATE, file.as_raw_fd(), 0) };
        if ptr as isize == -1 {
            return Err(io::Error::last_os_error());
        }
        Ok(Mapped { ptr: ptr as *const u8, len })
    }

    pub fn bytes(&self) -> &[u8] {
        if self.len == 0 {
            &[]
        } else {
            unsafe { std::slice::from_raw_parts(self.ptr, self.len) }
        }
    }
}

/// Reserve `len` bytes of anonymous memory, 2 MiB aligned, with transparent
/// huge pages requested. Pages are only charged when first written.
pub fn reserve(len: usize) -> *mut u8 {
    const HUGE: usize = 2 << 20;
    let total = len + HUGE;
    let ptr = unsafe {
        mmap(
            std::ptr::null_mut(),
            total,
            PROT_READ | PROT_WRITE,
            MAP_PRIVATE | MAP_ANONYMOUS | MAP_NORESERVE,
            -1,
            0,
        )
    };
    if ptr as isize == -1 {
        panic!("mmap of {total} bytes failed: {}", io::Error::last_os_error());
    }
    let aligned = ((ptr as usize + HUGE - 1) & !(HUGE - 1)) as *mut u8;
    unsafe {
        madvise(aligned as *mut c_void, len & !(HUGE - 1), MADV_HUGEPAGE);
    }
    aligned
}

/// A bump allocator over huge-page regions, owned by one thread.
pub struct Arena {
    cur: usize,
    end: usize,
    region: usize,
}

unsafe impl Send for Arena {}

impl Arena {
    pub fn new(region: usize) -> Arena {
        Arena { cur: 0, end: 0, region }
    }

    #[inline]
    pub fn alloc(&mut self, size: usize, align: usize) -> *mut u8 {
        let start = (self.cur + align - 1) & !(align - 1);
        if start + size > self.end {
            return self.refill(size, align);
        }
        self.cur = start + size;
        start as *mut u8
    }

    #[cold]
    fn refill(&mut self, size: usize, align: usize) -> *mut u8 {
        let len = self.region.max(size + align);
        let base = reserve(len) as usize;
        self.cur = base;
        self.end = base + len;
        let start = (self.cur + align - 1) & !(align - 1);
        self.cur = start + size;
        start as *mut u8
    }

    /// A zeroed slice of `n` values. Fresh anonymous memory is zero.
    pub fn slice<T: Copy>(&mut self, n: usize) -> &'static mut [T] {
        let p = self.alloc(n.max(1) * std::mem::size_of::<T>(), 64) as *mut T;
        unsafe { std::slice::from_raw_parts_mut(p, n) }
    }
}

/// A sense-reversing spin barrier. Workers spin briefly, then yield.
pub struct Barrier {
    count: AtomicUsize,
    generation: AtomicUsize,
    n: usize,
}

impl Barrier {
    pub fn new(n: usize) -> Barrier {
        Barrier { count: AtomicUsize::new(0), generation: AtomicUsize::new(0), n }
    }

    pub fn wait(&self) {
        let gen = self.generation.load(Ordering::Acquire);
        if self.count.fetch_add(1, Ordering::AcqRel) + 1 == self.n {
            self.count.store(0, Ordering::Relaxed);
            self.generation.store(gen + 1, Ordering::Release);
            return;
        }
        let mut spins = 0u32;
        while self.generation.load(Ordering::Acquire) == gen {
            if spins < 20_000 {
                std::hint::spin_loop();
                spins += 1;
            } else {
                std::thread::yield_now();
            }
        }
    }
}

/// Shared mutable cell for per-thread state that other threads read only
/// after a barrier.
pub struct Racy<T>(pub std::cell::UnsafeCell<T>);
unsafe impl<T> Sync for Racy<T> {}
unsafe impl<T> Send for Racy<T> {}

impl<T> Racy<T> {
    pub fn new(v: T) -> Self {
        Racy(std::cell::UnsafeCell::new(v))
    }
    #[allow(clippy::mut_from_ref)]
    pub unsafe fn get(&self) -> &mut T {
        &mut *self.0.get()
    }
}

pub fn now() -> std::time::Instant {
    std::time::Instant::now()
}
