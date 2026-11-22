//! Optional SIGPROF sampler (SS_PROF=path): records interrupted instruction
//! pointers and writes them at exit. Diagnostic only.

use std::sync::atomic::{AtomicUsize, Ordering};

const CAP: usize = 1 << 22;
static mut BUF: [u64; CAP] = [0; CAP];
static N: AtomicUsize = AtomicUsize::new(0);

#[repr(C)]
struct SigAction {
    handler: usize,
    mask: [u64; 16],
    flags: i32,
    restorer: usize,
}

#[repr(C)]
struct ItimerVal {
    interval_s: i64,
    interval_us: i64,
    value_s: i64,
    value_us: i64,
}

extern "C" {
    fn sigaction(sig: i32, act: *const SigAction, old: *mut SigAction) -> i32;
    fn setitimer(which: i32, new: *const ItimerVal, old: *mut ItimerVal) -> i32;
}

extern "C" fn handler(_sig: i32, _info: *mut u8, ctx: *mut u8) {
    unsafe {
        // ucontext_t: uc_flags(8) uc_link(8) uc_stack(24) then mcontext gregs[23]; REG_RIP = 16
        let gregs = ctx.add(40) as *const u64;
        let rip = *gregs.add(16);
        let i = N.fetch_add(1, Ordering::Relaxed);
        if i < CAP {
            BUF[i] = rip;
        }
    }
}

pub fn start() {
    unsafe {
        let act = SigAction { handler: handler as usize, mask: [0; 16], flags: 4 /* SA_SIGINFO */ | 0x10000000 /* SA_RESTART */, restorer: 0 };
        sigaction(27 /* SIGPROF */, &act, std::ptr::null_mut());
        let t = ItimerVal { interval_s: 0, interval_us: 200, value_s: 0, value_us: 200 };
        setitimer(2 /* ITIMER_PROF */, &t, std::ptr::null_mut());
    }
}

pub fn dump(path: &str) {
    unsafe {
        let t = ItimerVal { interval_s: 0, interval_us: 0, value_s: 0, value_us: 0 };
        setitimer(2, &t, std::ptr::null_mut());
        let n = N.load(Ordering::Relaxed).min(CAP);
        let mut s = String::with_capacity(n * 17);
        for i in 0..n {
            s.push_str(&format!("{:x}\n", BUF[i]));
        }
        let _ = std::fs::write(path, s);
    }
}
