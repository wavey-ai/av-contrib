//! Memory at start-up: a process that sends or publishes on a deadline pays
//! for its memory before the first epoch, not during it. Each first touch
//! of a page is a fault, and glibc's allocator hands memory back to the
//! kernel when a heap's top grows past its trim threshold and takes it
//! again with fresh faults.
//!
//! - `tune_allocator`: a fixed trim threshold and mmap threshold, so that
//!   freed memory stays in the heaps for the next allocation.
//! - `lock_all`: mlockall(MCL_CURRENT | MCL_FUTURE), for the AEP1 sender (a
//!   small process, run as root on its host): every mapped page is faulted
//!   in now and every later mapping when it is made.

use std::sync::atomic::{AtomicBool, Ordering};

static TUNED: AtomicBool = AtomicBool::new(false);

/// The heap trim threshold set by `tune_allocator`: a heap's top is given
/// back to the kernel only past this.
pub const TRIM_THRESHOLD: i32 = 256 << 20;
/// Allocations from this size on are mmapped (glibc's largest: 32 MB on
/// 64-bit), not the default 128 KB that rises only as such blocks are freed.
pub const MMAP_THRESHOLD: i32 = 32 << 20;

/// Keep freed memory in the allocator's heaps (see the module comment).
/// Returns why it did not.
pub fn tune_allocator() -> Result<(), String> {
    #[cfg(all(target_os = "linux", target_env = "gnu"))]
    {
        // SAFETY: mallopt takes plain integers; it fails by returning 0.
        let trim = unsafe { libc::mallopt(libc::M_TRIM_THRESHOLD, TRIM_THRESHOLD) };
        // SAFETY: as above.
        let mmap = unsafe { libc::mallopt(libc::M_MMAP_THRESHOLD, MMAP_THRESHOLD) };
        if trim == 0 || mmap == 0 {
            return Err(format!("mallopt refused (trim {trim}, mmap {mmap})"));
        }
        TUNED.store(true, Ordering::Relaxed);
        Ok(())
    }
    #[cfg(not(all(target_os = "linux", target_env = "gnu")))]
    {
        Err("mallopt is glibc only".into())
    }
}

/// Whether `tune_allocator` set the thresholds.
pub fn allocator_tuned() -> bool {
    TUNED.load(Ordering::Relaxed)
}

/// Lock every page of the process now and from now on, where it may do so
/// without a limit (RLIMIT_MEMLOCK infinite, or root). Returns why it did
/// not. With a finite limit it is not tried: MCL_FUTURE would make every
/// allocation past the limit fail.
pub fn lock_all() -> Result<(), String> {
    #[cfg(target_os = "linux")]
    {
        let mut limit = libc::rlimit {
            rlim_cur: 0,
            rlim_max: 0,
        };
        // SAFETY: getrlimit writes the struct it is given.
        if unsafe { libc::getrlimit(libc::RLIMIT_MEMLOCK, &mut limit) } != 0 {
            return Err(format!(
                "getrlimit(RLIMIT_MEMLOCK): {}",
                std::io::Error::last_os_error()
            ));
        }
        // SAFETY: geteuid has no preconditions.
        let root = unsafe { libc::geteuid() } == 0;
        if limit.rlim_cur != libc::RLIM_INFINITY && !root {
            return Err(format!(
                "RLIMIT_MEMLOCK is {} bytes and the process is not root",
                limit.rlim_cur
            ));
        }
        // SAFETY: mlockall takes flags only.
        if unsafe { libc::mlockall(libc::MCL_CURRENT | libc::MCL_FUTURE) } != 0 {
            return Err(format!("mlockall: {}", std::io::Error::last_os_error()));
        }
        Ok(())
    }
    #[cfg(not(target_os = "linux"))]
    {
        Err("mlockall is Linux only".into())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn locking_everything_is_refused_under_a_finite_memlock_limit() {
        let mut limit = libc::rlimit {
            rlim_cur: 0,
            rlim_max: 0,
        };
        // SAFETY: getrlimit writes the struct it is given.
        unsafe { libc::getrlimit(libc::RLIMIT_MEMLOCK, &mut limit) };
        // SAFETY: geteuid has no preconditions.
        let may = limit.rlim_cur == libc::RLIM_INFINITY || unsafe { libc::geteuid() } == 0;
        // Where the process may lock, the test does not: it would lock the
        // whole test binary for good.
        if !may {
            let refused = lock_all().unwrap_err();
            assert!(refused.contains("RLIMIT_MEMLOCK"), "{refused}");
        }
    }

    #[test]
    fn the_allocator_thresholds_are_set_once_asked_for() {
        if tune_allocator().is_ok() {
            assert!(allocator_tuned());
        }
    }
}
