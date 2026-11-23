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
//! - `pin_region`: for the contributor, a service under a finite
//!   RLIMIT_MEMLOCK. mlockall cannot lock it (MCL_CURRENT needs the whole
//!   virtual size within the limit, and MCL_FUTURE makes allocations past
//!   it fail), so each ring that is written on a deadline is faulted in
//!   (`--prefault`) and locked by itself (`--mlock rings`) when its owner
//!   builds it. A refused lock is counted and the ring stays faulted in.
//! - `report`: what was done, with the process's sizes, for the status.

use serde::Serialize;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicU8, Ordering};

static TUNED: AtomicBool = AtomicBool::new(false);
/// `--prefault`: whether `pin_region` faults a region in.
static PREFAULT: AtomicBool = AtomicBool::new(true);
/// `--mlock`: the [`LockMode`] of `pin_region`, as its discriminant.
static LOCK_MODE: AtomicU8 = AtomicU8::new(LockMode::Off as u8);
static PREFAULTED_BYTES: AtomicU64 = AtomicU64::new(0);
static LOCKED_BYTES: AtomicU64 = AtomicU64::new(0);
static LOCK_REFUSED_BYTES: AtomicU64 = AtomicU64::new(0);

/// Bytes of a page for the purpose of touching each one.
const PAGE: usize = 4_096;

/// Tests that set the process's mode, or count on it, run one at a time.
#[cfg(test)]
pub(crate) static MODE: std::sync::Mutex<()> = std::sync::Mutex::new(());

/// `--mlock`: what the process locks in memory.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum LockMode {
    /// Nothing.
    Off = 0,
    /// Each ring given to [`pin_region`], with `mlock(ptr, len)`.
    Rings = 1,
}

impl LockMode {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Off => "off",
            Self::Rings => "rings",
        }
    }
}

impl std::str::FromStr for LockMode {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "off" => Ok(Self::Off),
            "rings" => Ok(Self::Rings),
            other => Err(format!("`{other}` is not a lock mode: off or rings")),
        }
    }
}

impl std::fmt::Display for LockMode {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.as_str())
    }
}

/// Set what [`pin_region`] does, before the rings are built. Until it is
/// called a region is faulted in and not locked.
pub fn configure(prefault: bool, mode: LockMode) {
    PREFAULT.store(prefault, Ordering::Relaxed);
    LOCK_MODE.store(mode as u8, Ordering::Relaxed);
}

fn lock_mode() -> LockMode {
    if LOCK_MODE.load(Ordering::Relaxed) == LockMode::Rings as u8 {
        LockMode::Rings
    } else {
        LockMode::Off
    }
}

/// Fault in (`--prefault`) and lock (`--mlock rings`) the `len` bytes at
/// `ptr`, which the caller is about to use as a ring: one zero byte is
/// written to each page, then the range is locked. Returns whether it is
/// locked; the caller unlocks it with [`unpin_region`] before it frees the
/// memory. A refused lock is counted (`lock_refused_bytes`), logged once
/// for the region, and changes nothing else.
///
/// # Safety
/// `ptr..ptr + len` must be valid for writes, and the caller must not need
/// the bytes it holds: spare capacity that is not yet initialized.
pub unsafe fn pin_region(name: &str, ptr: *mut u8, len: usize) -> bool {
    if len == 0 {
        return false;
    }
    if PREFAULT.load(Ordering::Relaxed) {
        let mut offset = 0;
        while offset < len {
            // SAFETY: within the range the caller vouches for. Nothing is
            // read: the memory may be uninitialized.
            unsafe { std::ptr::write_volatile(ptr.add(offset), 0) };
            offset += PAGE;
        }
        // The last page, when the range does not start on a page.
        // SAFETY: as above.
        unsafe { std::ptr::write_volatile(ptr.add(len - 1), 0) };
        PREFAULTED_BYTES.fetch_add(len as u64, Ordering::Relaxed);
    }
    if lock_mode() != LockMode::Rings {
        return false;
    }
    match lock_range(ptr, len) {
        Ok(()) => {
            LOCKED_BYTES.fetch_add(len as u64, Ordering::Relaxed);
            true
        }
        Err(reason) => {
            LOCK_REFUSED_BYTES.fetch_add(len as u64, Ordering::Relaxed);
            tracing::info!(
                region = name,
                bytes = len,
                %reason,
                "a ring could not be locked in memory (--mlock rings): it stays faulted in"
            );
            false
        }
    }
}

/// Unlock a range that [`pin_region`] locked, before its memory is freed.
///
/// # Safety
/// `ptr..ptr + len` must be the range given to `pin_region`, still mapped.
pub unsafe fn unpin_region(ptr: *mut u8, len: usize) {
    #[cfg(target_os = "linux")]
    {
        // SAFETY: the caller's own range.
        unsafe { libc::munlock(ptr.cast(), len) };
    }
    #[cfg(not(target_os = "linux"))]
    {
        let _ = ptr;
    }
    LOCKED_BYTES.fetch_sub(len as u64, Ordering::Relaxed);
}

fn lock_range(ptr: *mut u8, len: usize) -> Result<(), String> {
    #[cfg(target_os = "linux")]
    {
        // SAFETY: mlock only pins the pages of the range; the caller of
        // `pin_region` vouches that it is mapped.
        if unsafe { libc::mlock(ptr.cast(), len) } != 0 {
            return Err(format!("mlock: {}", std::io::Error::last_os_error()));
        }
        Ok(())
    }
    #[cfg(not(target_os = "linux"))]
    {
        let _ = (ptr, len);
        Err("mlock is Linux only".into())
    }
}

/// What the process did with its memory, and its sizes now.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct MemoryReport {
    /// `--mlock`.
    pub mode: &'static str,
    /// Bytes of rings faulted in at start (`--prefault`).
    pub prefaulted_bytes: u64,
    /// Bytes of rings locked now.
    pub locked_bytes: u64,
    /// Bytes of rings whose lock the kernel refused (RLIMIT_MEMLOCK).
    pub lock_refused_bytes: u64,
    /// VmSize, VmRSS and VmLck of `/proc/self/status`.
    pub vm_size_kb: u64,
    pub vm_rss_kb: u64,
    pub vm_lck_kb: u64,
    /// Transparent huge pages on this host: always, madvise or never.
    pub thp: String,
    /// Whether `tune_allocator` set glibc's thresholds (`--malloc-tune`).
    pub allocator_tuned: bool,
}

/// The process's memory, for its status.
pub fn report() -> MemoryReport {
    let status = std::fs::read_to_string("/proc/self/status").unwrap_or_default();
    let thp = std::fs::read_to_string("/sys/kernel/mm/transparent_hugepage/enabled")
        .ok()
        .as_deref()
        .and_then(parse_thp_mode)
        .unwrap_or("unknown")
        .to_owned();
    MemoryReport {
        mode: lock_mode().as_str(),
        prefaulted_bytes: PREFAULTED_BYTES.load(Ordering::Relaxed),
        locked_bytes: LOCKED_BYTES.load(Ordering::Relaxed),
        lock_refused_bytes: LOCK_REFUSED_BYTES.load(Ordering::Relaxed),
        vm_size_kb: parse_status_kb(&status, "VmSize"),
        vm_rss_kb: parse_status_kb(&status, "VmRSS"),
        vm_lck_kb: parse_status_kb(&status, "VmLck"),
        thp,
        allocator_tuned: allocator_tuned(),
    }
}

/// A `Vm*` line of `/proc/self/status`, in kB; 0 when it is not there.
fn parse_status_kb(status: &str, name: &str) -> u64 {
    status
        .lines()
        .find_map(|line| line.strip_prefix(name)?.strip_prefix(':'))
        .and_then(|value| value.split_ascii_whitespace().next()?.parse().ok())
        .unwrap_or(0)
}

/// The mode in brackets of `always [madvise] never`.
fn parse_thp_mode(enabled: &str) -> Option<&str> {
    let from = enabled.find('[')? + 1;
    let to = from + enabled[from..].find(']')?;
    Some(&enabled[from..to])
}

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
            assert!(report().allocator_tuned);
        }
    }

    /// Minor page faults of the calling thread.
    fn thread_faults() -> u64 {
        // SAFETY: a zeroed rusage is valid; getrusage writes it.
        let mut usage: libc::rusage = unsafe { std::mem::zeroed() };
        // SAFETY: as above.
        assert_eq!(
            unsafe { libc::getrusage(libc::RUSAGE_THREAD, &mut usage) },
            0
        );
        u64::try_from(usage.ru_minflt).unwrap()
    }

    /// A block of `len` bytes that nothing has touched: a fresh private
    /// mapping, which no allocator has handed out before.
    struct Fresh {
        ptr: *mut u8,
        len: usize,
    }

    impl Fresh {
        fn new(len: usize) -> Self {
            // SAFETY: an anonymous private mapping of `len` bytes.
            let ptr = unsafe {
                libc::mmap(
                    std::ptr::null_mut(),
                    len,
                    libc::PROT_READ | libc::PROT_WRITE,
                    libc::MAP_PRIVATE | libc::MAP_ANONYMOUS,
                    -1,
                    0,
                )
            };
            assert_ne!(ptr, libc::MAP_FAILED);
            Self {
                ptr: ptr.cast(),
                len,
            }
        }

        /// Write every page, and return the faults that took.
        fn touch(&self) -> u64 {
            let before = thread_faults();
            for offset in (0..self.len).step_by(PAGE) {
                // SAFETY: inside the mapping.
                unsafe { std::ptr::write_volatile(self.ptr.add(offset), 1) };
            }
            thread_faults() - before
        }
    }

    impl Drop for Fresh {
        fn drop(&mut self) {
            // SAFETY: the mapping made in `new`.
            unsafe { libc::munmap(self.ptr.cast(), self.len) };
        }
    }

    fn memlock_limit() -> u64 {
        let mut limit = libc::rlimit {
            rlim_cur: 0,
            rlim_max: 0,
        };
        // SAFETY: getrlimit writes the struct it is given.
        unsafe { libc::getrlimit(libc::RLIMIT_MEMLOCK, &mut limit) };
        limit.rlim_cur
    }

    #[test]
    fn the_ring_is_faulted_in_at_start() {
        let _mode = MODE.lock().unwrap();
        configure(true, LockMode::Off);
        let len = 4 << 20;
        // Untouched memory faults as it is first written...
        let cold = Fresh::new(len);
        assert!(cold.touch() > 0);
        // ...and a pinned region has taken its faults already.
        let ring = Fresh::new(len);
        let before = report().prefaulted_bytes;
        // SAFETY: the mapping is writable and holds nothing.
        let locked = unsafe { pin_region("test ring", ring.ptr, ring.len) };
        assert!(!locked, "--mlock off locks nothing");
        assert!(report().prefaulted_bytes >= before + len as u64);
        let faults = ring.touch();
        assert!(
            faults <= 2,
            "a prefaulted ring took {faults} faults when used"
        );

        // --prefault false leaves the region as it was.
        configure(false, LockMode::Off);
        let untouched = Fresh::new(len);
        // SAFETY: as above.
        unsafe { pin_region("test ring", untouched.ptr, untouched.len) };
        assert!(untouched.touch() > 0);
        configure(true, LockMode::Off);
    }

    #[test]
    fn a_ring_is_locked_under_a_finite_limit_and_a_refusal_is_counted() {
        let _mode = MODE.lock().unwrap();
        configure(true, LockMode::Rings);
        let limit = memlock_limit();
        // 64 KB fits any limit a service is given.
        if limit >= 1 << 20 {
            let ring = Fresh::new(64 << 10);
            let before = report();
            // SAFETY: the mapping is writable and holds nothing.
            let locked = unsafe { pin_region("small ring", ring.ptr, ring.len) };
            assert!(locked);
            let after = report();
            assert_eq!(after.mode, "rings");
            // Other tests' rings come and go meanwhile: at least this one.
            assert!(after.locked_bytes >= before.locked_bytes + ring.len as u64);
            assert!(after.vm_lck_kb >= 64);
            // SAFETY: the range that was pinned.
            unsafe { unpin_region(ring.ptr, ring.len) };
        }
        // A ring larger than the limit is refused, counted, and usable.
        // SAFETY: geteuid has no preconditions.
        let root = unsafe { libc::geteuid() } == 0;
        if limit != libc::RLIM_INFINITY && limit < 1 << 30 && !root {
            let len = usize::try_from(limit).unwrap() + (4 << 20);
            let ring = Fresh::new(len);
            let before = report();
            // SAFETY: as above.
            let locked = unsafe { pin_region("large ring", ring.ptr, ring.len) };
            assert!(!locked);
            let after = report();
            assert!(after.lock_refused_bytes >= before.lock_refused_bytes + len as u64);
            // It was faulted in all the same.
            assert!(ring.touch() <= 2);
        }
        configure(true, LockMode::Off);
    }

    #[test]
    fn the_lock_mode_is_off_or_rings() {
        assert_eq!("rings".parse(), Ok(LockMode::Rings));
        assert_eq!("off".parse(), Ok(LockMode::Off));
        assert!("all".parse::<LockMode>().is_err());
        assert_eq!(LockMode::Rings.to_string(), "rings");
    }

    #[test]
    fn the_report_reads_the_process_s_sizes() {
        let status =
            "Name:\tav-contrib\nVmSize:\t  812340 kB\nVmLck:\t   94208 kB\nVmRSS:\t  120044 kB\n";
        assert_eq!(parse_status_kb(status, "VmSize"), 812_340);
        assert_eq!(parse_status_kb(status, "VmLck"), 94_208);
        assert_eq!(parse_status_kb(status, "VmRSS"), 120_044);
        assert_eq!(parse_status_kb(status, "VmSwap"), 0);
        assert_eq!(parse_thp_mode("always [madvise] never\n"), Some("madvise"));
        assert_eq!(parse_thp_mode("[always] madvise never\n"), Some("always"));
        assert_eq!(parse_thp_mode("never"), None);
        let now = report();
        assert!(now.vm_size_kb > 0 && now.vm_rss_kb > 0);
    }
}
