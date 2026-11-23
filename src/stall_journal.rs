//! The stall journal: a dated record of the times a node did not run.
//!
//! A counter such as "the longest loop gap since start" cannot say when a
//! node stopped, for how long, on which CPU, or whether the node was not run
//! at all or was busy with work of its own. The journal can. It has no
//! dependency on the rest of av-mesh: `needletail-nodecheck` runs it in a
//! process of its own, and av-contrib keeps a copy.
//!
//! **The contributor's kinds** (D-hunt 2.8), recorded by av-contrib only:
//! `src_gap` (an AEP1 epoch's kernel arrival 2,000 us or more after its
//! turn), `rx_wait` (a datagram 1,000 us or more between its kernel stamp
//! and the read), `inline_publish` (2,000 us or more) and `lane_send`
//! (1,000 us or more), on the threads `receive`, `publish` and `lane`.
//!
//! **Events.** A ring of fixed-size events, allocated, faulted in and locked
//! at start. A hot path holds one line ([`note`]): a comparison of a duration
//! it has already measured against the kind's threshold, and nothing else
//! while nothing stalls. An event is one `try_lock` and a copy; a contended
//! record is counted (`dropped`) and never waited for. Consecutive events of
//! one kind and thread less than 10 ms apart merge into one line (`dur_us`
//! the longest, `span_us` first start to last end, `n` the count), so a storm
//! cannot empty the ring.
//!
//! **Tick threads.** One thread per CPU the process may run on, pinned to
//! it, at normal scheduling (it never preempts a real-time class thread). It
//! sleeps to an absolute deadline every tick and books how late it woke,
//! with the time it spent waiting on a run queue for that wake (its own
//! `/proc/thread-self/schedstat`). Late with no run delay: the thread was
//! asleep and its timer came late, so the CPU was not run (`stopped`). Late
//! with run delay: it was runnable and kept waiting (`starved`: overload, or
//! a thread of ours or the kernel held the CPU).
//!
//! **Cause** is worked out when the journal is read ([`classify`]), off
//! every hot path: a class thread cannot know that its CPU was stopped under
//! it, because that CPU's tick thread runs only after the class thread's
//! pass ends. The read joins each event to the stopped ticks of its CPU.
//!
//! **Reading by time.** `t_us` is taken under the ring's lock and is
//! strictly increasing over everything the journal holds, so a reader that
//! asks again with `since_us` = the last `t_us` it saw plus one misses
//! nothing. A merge moves an event's `t_us` forward, so such a reader sees a
//! merged event again, grown: the same event has the same kind, thread and
//! start (`t_us - span_us`), and the later line replaces the earlier.

use serde::Serialize;
use std::fmt::Write as _;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, OnceLock, PoisonError, TryLockError};

/// Events kept unless `--stall-journal-capacity` says otherwise.
pub const DEFAULT_CAPACITY: usize = 4_096;
/// The tick threads' period unless `--stall-tick-us` says otherwise.
pub const DEFAULT_TICK_US: u64 = 1_000;

/// A tick that woke this late is an event.
pub const TICK_LATE_US: u64 = 2_000;
/// A pass of a class thread's loop this long is an event.
pub const CLASS_PASS_US: u64 = 2_000;
/// Two data-bearing wakes of a class thread this far apart are an event.
pub const CLASS_GAP_US: u64 = 5_000;
/// The oldest datagram of a batch waiting this long between its kernel
/// stamp and the drain is an event.
pub const RX_LATE_US: u64 = 2_000;
/// An item reaching the tee this long after its batch's wake is an event.
pub const TEE_LAG_US: u64 = 10_000;
/// One `sendmmsg` taking this long is an event.
pub const SEND_SLOW_US: u64 = 1_000;
/// The contributor: an AEP1 epoch arriving (kernel stamp) this long after
/// its turn, the previous epoch's arrival plus one epoch, is an event.
pub const SRC_GAP_US: u64 = 2_000;
/// The contributor: a datagram waiting this long between its kernel stamp
/// and the read is an event.
pub const RX_WAIT_US: u64 = 1_000;
/// The contributor: an inline audio publication holding its packager this
/// long is an event.
pub const INLINE_PUBLISH_US: u64 = 2_000;
/// The contributor: one send of a part's datagrams on a lane taking this
/// long is an event.
pub const LANE_SEND_US: u64 = 1_000;

/// Events of one kind and thread closer than this merge.
const MERGE_WITHIN_US: u64 = 10_000;
/// A tick this late counts in `stopped_ms` or `starved_ms`.
const LONG_TICK_US: u64 = 20_000;
/// Events copied out under one hold of the lock when the journal is read
/// (48 KB), so that a recorder is refused for no longer than that copy.
const READ_CHUNK: usize = 1_024;
/// CPUs the journal can tell apart (the kernel's CPU_SETSIZE).
const MAX_CPUS: usize = 1_024;
/// `sec` lines kept, as a share of the events kept, and at least.
const SEC_LINES_SHARE: usize = 4;
const SEC_LINES_MIN: usize = 64;
/// A run delay that could not be read (no schedstat on this kernel).
const RUN_DELAY_UNKNOWN: u32 = u32::MAX;

/// What was late.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum StallKind {
    /// A tick thread woke late.
    Tick = 0,
    /// A pass of a class thread's loop was long.
    ClassPass,
    /// Two data-bearing wakes of a class thread were far apart.
    ClassGap,
    /// A batch waited in its socket before the class thread drained it.
    RxLate,
    /// An item waited between the class thread and its tee.
    TeeLag,
    /// One `sendmmsg` was slow.
    SendSlow,
    /// The contributor: an AEP1 epoch arrived late after the one before.
    SrcGap,
    /// The contributor: a datagram waited in its socket before the read.
    RxWait,
    /// The contributor: an inline audio publication was long.
    InlinePublish,
    /// The contributor: a part's send on a lane was slow.
    LaneSend,
}

/// Kinds, for the table of each thread's newest event.
const KINDS: usize = 10;

impl StallKind {
    /// The name in the journal's lines; it must not change.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Tick => "tick",
            Self::ClassPass => "class_pass",
            Self::ClassGap => "class_gap",
            Self::RxLate => "rx_late",
            Self::TeeLag => "tee_lag",
            Self::SendSlow => "send_slow",
            Self::SrcGap => "src_gap",
            Self::RxWait => "rx_wait",
            Self::InlinePublish => "inline_publish",
            Self::LaneSend => "lane_send",
        }
    }

    /// The duration from which the kind is an event.
    pub const fn threshold_us(self) -> u64 {
        match self {
            Self::Tick => TICK_LATE_US,
            Self::ClassPass => CLASS_PASS_US,
            Self::ClassGap => CLASS_GAP_US,
            Self::RxLate => RX_LATE_US,
            Self::TeeLag => TEE_LAG_US,
            Self::SendSlow => SEND_SLOW_US,
            Self::SrcGap => SRC_GAP_US,
            Self::RxWait => RX_WAIT_US,
            Self::InlinePublish => INLINE_PUBLISH_US,
            Self::LaneSend => LANE_SEND_US,
        }
    }

    /// What an event of the kind is when its CPU was running all the while:
    /// the thread's own work, or a wait for something else.
    const fn cause_while_running(self) -> Cause {
        match self {
            Self::ClassPass | Self::SendSlow | Self::InlinePublish | Self::LaneSend => Cause::Own,
            Self::Tick
            | Self::ClassGap
            | Self::RxLate
            | Self::TeeLag
            | Self::SrcGap
            | Self::RxWait => Cause::Waited,
        }
    }
}

/// Which thread recorded an event.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum StallThread {
    /// The tick thread of this CPU.
    Tick(u16),
    /// The audio class thread.
    Audio,
    /// The video class thread.
    Video,
    /// The audio class's tee.
    TeeAudio,
    /// The video class's tee.
    TeeVideo,
    /// The contributor's AEP1 receive and recovery.
    Receive,
    /// The contributor's inline audio publication.
    Publish,
    /// The contributor's lane sends.
    Lane,
}

/// Threads that are not tick threads, for the table of newest events.
const NAMED_THREADS: usize = 7;

impl StallThread {
    fn write_name(self, out: &mut String) {
        match self {
            Self::Tick(cpu) => {
                let _ = write!(out, "tick-{cpu}");
            }
            Self::Audio => out.push_str("audio"),
            Self::Video => out.push_str("video"),
            Self::TeeAudio => out.push_str("tee-audio"),
            Self::TeeVideo => out.push_str("tee-video"),
            Self::Receive => out.push_str("receive"),
            Self::Publish => out.push_str("publish"),
            Self::Lane => out.push_str("lane"),
        }
    }

    /// The class threads read their page faults; their events carry them.
    const fn is_class(self) -> bool {
        matches!(self, Self::Audio | Self::Video)
    }

    /// Where the table of newest events keeps this thread's event of `kind`.
    fn newest_slot(self, kind: StallKind) -> Option<usize> {
        let named = match self {
            Self::Tick(cpu) => {
                return (usize::from(cpu) < MAX_CPUS)
                    .then(|| NAMED_THREADS * KINDS + usize::from(cpu));
            }
            Self::Audio => 0,
            Self::Video => 1,
            Self::TeeAudio => 2,
            Self::TeeVideo => 3,
            Self::Receive => 4,
            Self::Publish => 5,
            Self::Lane => 6,
        };
        Some(named * KINDS + kind as usize)
    }
}

/// One event of the ring: 48 bytes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(C)]
pub struct StallEvent {
    /// CLOCK_REALTIME in microseconds when the event (the last of a merged
    /// run) was recorded. The event covers `[t_us - span_us, t_us]`.
    pub t_us: u64,
    /// How late, long or far apart: the longest of a merged run.
    pub dur_us: u64,
    /// First start to last end of a merged run; `dur_us` for one event.
    pub span_us: u64,
    /// Tick events: the time the thread waited on a run queue for this wake
    /// (of the longest of a merged run).
    run_delay_us: u32,
    /// Events merged into this line.
    pub n: u32,
    /// Class threads: the thread's page faults since its once-a-second
    /// reading (a hint, up to a second wide).
    pub minflt: u32,
    pub majflt: u32,
    /// The CPU the recording thread was on (of the longest of a merged run).
    pub cpu: u16,
    pub thread: StallThread,
    pub kind: StallKind,
}

const _: () = assert!(std::mem::size_of::<StallEvent>() == 48);

impl StallEvent {
    const EMPTY: Self = Self {
        t_us: 0,
        dur_us: 0,
        span_us: 0,
        run_delay_us: 0,
        n: 0,
        minflt: 0,
        majflt: 0,
        cpu: 0,
        thread: StallThread::Audio,
        kind: StallKind::Tick,
    };

    /// The run delay of a tick event; `None` where the kernel does not
    /// report it, or for another kind.
    pub fn run_delay_us(&self) -> Option<u64> {
        (self.kind == StallKind::Tick && self.run_delay_us != RUN_DELAY_UNKNOWN)
            .then_some(u64::from(self.run_delay_us))
    }

    fn start_us(&self) -> u64 {
        self.t_us.saturating_sub(self.span_us)
    }
}

/// Why an event happened, as far as the node can tell.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Cause {
    /// The CPU was not run.
    Stopped,
    /// A tick thread was runnable and kept waiting.
    Starved,
    /// The thread's own work.
    Own,
    /// The thread waited for something else while its CPU ran.
    Waited,
    /// A late tick on a kernel that does not report run delay.
    Unknown,
}

impl Cause {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Stopped => "stopped",
            Self::Starved => "starved",
            Self::Own => "own",
            Self::Waited => "waited",
            Self::Unknown => "unknown",
        }
    }
}

/// Whether a stopped tick was one CPU's or every CPU's.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Scope {
    One,
    All,
}

impl Scope {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::One => "one",
            Self::All => "all",
        }
    }
}

/// An event with its cause.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StallLine {
    pub event: StallEvent,
    pub cause: Cause,
    /// Stopped ticks only.
    pub scope: Option<Scope>,
}

/// Growth of the `/proc/vmstat` counters over a `sec` line's interval.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct VmGrowth {
    pub thp_fault_alloc: u32,
    pub thp_fault_fallback: u32,
    pub compact_stall: u32,
    pub pgfault: u32,
    pub allocstall: u32,
}

/// One CPU over about a second: what the CPU did (`/proc/stat`, in clock
/// ticks) and how its tick thread fared.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct SecLine {
    /// When the line was recorded; it covers `[t_us - span_us, t_us]`.
    pub t_us: u64,
    pub span_us: u32,
    pub cpu: u16,
    /// `user` and `nice` together.
    pub user: u32,
    pub system: u32,
    pub irq: u32,
    pub softirq: u32,
    pub steal: u32,
    /// The latest wake of the CPU's tick thread in the interval.
    pub late_max_us: u32,
    /// Its wakes 2,000 us or more late.
    pub late_over_2ms: u32,
    /// The longest run of the fixed work loop (`TickConfig::detail`).
    pub work_us_max: Option<u32>,
    /// On the first CPU's line (`TickConfig::detail`).
    pub vm: Option<VmGrowth>,
}

/// What a read of the journal returns: each part oldest first.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct StallReport {
    pub events: Vec<StallLine>,
    pub secs: Vec<SecLine>,
}

/// What `/api/mesh/local` reports of the journal. The tick counters are
/// since start, and count every tick whether or not the ring took its event.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize)]
pub struct StallSnapshot {
    pub tick_threads: u64,
    /// Below `tick_threads` (a cpuset refused a pin), `scope` is not to be
    /// trusted.
    pub tick_pinned: u64,
    /// Events recorded (a merged event counts each time).
    pub events: u64,
    /// Records refused because the ring was in use.
    pub dropped: u64,
    pub tick_late_max_us: u64,
    pub tick_late_over_2ms: u64,
    pub tick_late_over_100ms: u64,
    /// Summed lateness of `stopped` ticks of 20 ms or more, averaged over
    /// the tick threads: the time this node was not run.
    pub stopped_ms: u64,
    /// The same of `starved` ticks.
    pub starved_ms: u64,
    /// The longest `rx_late` recorded (waits under its threshold are not
    /// looked at).
    pub rx_late_max_us: u64,
}

#[derive(Debug, Default)]
struct Counters {
    tick_threads: AtomicU64,
    tick_pinned: AtomicU64,
    events: AtomicU64,
    dropped: AtomicU64,
    tick_late_max_us: AtomicU64,
    tick_late_over_2ms: AtomicU64,
    tick_late_over_100ms: AtomicU64,
    stopped_us: AtomicU64,
    starved_us: AtomicU64,
    rx_late_max_us: AtomicU64,
}

#[derive(Debug)]
struct Ring {
    events: Box<[StallEvent]>,
    /// Events ever pushed; the event of absolute index `i` is in slot
    /// `i % capacity` while `i + capacity >= pushed`.
    pushed: u64,
    /// By [`StallThread::newest_slot`]: the absolute index plus one of the
    /// thread's newest event of a kind, zero for none.
    newest: Box<[u64]>,
    /// The last `t_us` given out.
    last_t_us: u64,
    secs: Box<[SecLine]>,
    secs_pushed: u64,
    /// CPUs that recorded an event since the last `sec` lines.
    event_cpus: [u64; MAX_CPUS / 64],
    /// The CPUs that have a tick thread.
    tick_cpus: Vec<u16>,
}

impl Ring {
    /// The next `t_us`: now, and after every `t_us` given out before.
    fn stamp(&mut self, now_us: u64) -> u64 {
        self.last_t_us = now_us.max(self.last_t_us + 1);
        self.last_t_us
    }

    fn put(&mut self, mut event: StallEvent) {
        let capacity = self.events.len() as u64;
        let slot = event.thread.newest_slot(event.kind);
        let start_us = event.t_us.saturating_sub(event.dur_us);
        if let Some(index) = slot.and_then(|slot| self.newest[slot].checked_sub(1)) {
            let newest = &mut self.events[(index % capacity) as usize];
            if index + capacity >= self.pushed && start_us < newest.t_us + MERGE_WITHIN_US {
                let first_start_us = newest.start_us().min(start_us);
                if event.dur_us > newest.dur_us {
                    newest.dur_us = event.dur_us;
                    newest.run_delay_us = event.run_delay_us;
                    newest.cpu = event.cpu;
                }
                newest.minflt = newest.minflt.max(event.minflt);
                newest.majflt = newest.majflt.max(event.majflt);
                newest.n = newest.n.saturating_add(1);
                newest.t_us = event.t_us;
                newest.span_us = event.t_us - first_start_us;
                return;
            }
        }
        event.span_us = event.dur_us;
        event.n = 1;
        self.events[(self.pushed % capacity) as usize] = event;
        self.pushed += 1;
        if let Some(slot) = slot {
            self.newest[slot] = self.pushed;
        }
    }

    fn mark_event_cpu(&mut self, cpu: u16) {
        if let Some(word) = self.event_cpus.get_mut(usize::from(cpu) / 64) {
            *word |= 1 << (cpu % 64);
        }
    }

    fn take_event_cpu(&mut self, cpu: u16) -> bool {
        let Some(word) = self.event_cpus.get_mut(usize::from(cpu) / 64) else {
            return false;
        };
        let bit = 1 << (cpu % 64);
        let had = *word & bit != 0;
        *word &= !bit;
        had
    }
}

/// The journal of one process.
#[derive(Debug)]
pub struct StallJournal {
    node: String,
    capacity: usize,
    ring: Mutex<Ring>,
    /// Whether the rings are locked in memory (`mlock`).
    locked: bool,
    counters: Counters,
}

/// A slice of `len` copies of `value`, every page of it written.
fn faulted<T: Copy>(value: T, len: usize) -> Box<[T]> {
    let mut slice = vec![value; len].into_boxed_slice();
    for slot in slice.iter_mut() {
        // SAFETY: `slot` is a valid, exclusive reference. The volatile
        // write cannot be elided, whatever the allocator handed out.
        unsafe { std::ptr::write_volatile(slot, value) };
    }
    slice
}

impl StallJournal {
    /// A journal that keeps the newest `capacity` events, its memory
    /// faulted in and locked now. Zero keeps none: every record is a no-op.
    pub fn new(node: impl Into<String>, capacity: usize) -> Self {
        let (sec_lines, newest_slots) = if capacity == 0 {
            (0, 0)
        } else {
            (
                (capacity / SEC_LINES_SHARE).max(SEC_LINES_MIN),
                NAMED_THREADS * KINDS + MAX_CPUS,
            )
        };
        let events = faulted(StallEvent::EMPTY, capacity);
        let newest = faulted(0_u64, newest_slots);
        let secs = faulted(SecLine::default(), sec_lines);
        let locked =
            capacity > 0 && os::lock(&events[..]) && os::lock(&newest[..]) && os::lock(&secs[..]);
        Self {
            node: node.into(),
            capacity,
            ring: Mutex::new(Ring {
                events,
                pushed: 0,
                newest,
                last_t_us: 0,
                secs,
                secs_pushed: 0,
                event_cpus: [0; MAX_CPUS / 64],
                tick_cpus: Vec::new(),
            }),
            locked,
            counters: Counters::default(),
        }
    }

    pub fn is_enabled(&self) -> bool {
        self.capacity > 0
    }

    /// Whether the rings are locked in memory. A refusal (a memlock limit
    /// below the rings) changes nothing else: they stay faulted in.
    pub fn ring_locked(&self) -> bool {
        self.locked
    }

    fn lock(&self) -> MutexGuard<'_, Ring> {
        self.ring.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// The ring, unless it is in use: a recorder never waits for it.
    fn try_lock(&self) -> Option<MutexGuard<'_, Ring>> {
        match self.ring.try_lock() {
            Ok(ring) => Some(ring),
            Err(TryLockError::Poisoned(poisoned)) => Some(poisoned.into_inner()),
            Err(TryLockError::WouldBlock) => None,
        }
    }

    /// Record an event of a thread other than a tick thread. One `try_lock`
    /// and a copy; refused and counted when the ring is in use.
    pub fn record(
        &self,
        kind: StallKind,
        thread: StallThread,
        dur_us: u64,
        minflt: u64,
        majflt: u64,
    ) {
        if kind == StallKind::RxLate {
            self.counters
                .rx_late_max_us
                .fetch_max(dur_us, Ordering::Relaxed);
        }
        self.put(
            None,
            StallEvent {
                dur_us,
                minflt: u32::try_from(minflt).unwrap_or(u32::MAX),
                majflt: u32::try_from(majflt).unwrap_or(u32::MAX),
                cpu: os::current_cpu(),
                thread,
                kind,
                ..StallEvent::EMPTY
            },
        );
    }

    /// Book one tick of the tick thread of `cpu` that woke `late_us` late
    /// after `run_delay_us` on a run queue (`None`: not known), and record
    /// it when it is `threshold_us` late or more.
    fn tick(&self, cpu: u16, late_us: u64, run_delay_us: Option<u64>, threshold_us: u64) {
        let counters = &self.counters;
        if late_us >= TICK_LATE_US {
            counters.tick_late_over_2ms.fetch_add(1, Ordering::Relaxed);
        }
        if late_us >= 100_000 {
            counters
                .tick_late_over_100ms
                .fetch_add(1, Ordering::Relaxed);
        }
        if late_us >= LONG_TICK_US {
            let total = match tick_cause(late_us, run_delay_us) {
                Cause::Stopped => Some(&counters.stopped_us),
                Cause::Starved => Some(&counters.starved_us),
                _ => None,
            };
            if let Some(total) = total {
                total.fetch_add(late_us, Ordering::Relaxed);
            }
        }
        if late_us >= threshold_us {
            self.put(
                None,
                StallEvent {
                    dur_us: late_us,
                    run_delay_us: run_delay_us.map_or(RUN_DELAY_UNKNOWN, |delay| {
                        u32::try_from(delay).map_or(RUN_DELAY_UNKNOWN - 1, |delay| {
                            delay.min(RUN_DELAY_UNKNOWN - 1)
                        })
                    }),
                    cpu: os::current_cpu(),
                    thread: StallThread::Tick(cpu),
                    kind: StallKind::Tick,
                    ..StallEvent::EMPTY
                },
            );
        }
    }

    /// Put an event into the ring at `at_us` (`None`: now), unless the ring
    /// is in use.
    fn put(&self, at_us: Option<u64>, mut event: StallEvent) {
        if self.capacity == 0 {
            return;
        }
        let Some(mut ring) = self.try_lock() else {
            self.counters.dropped.fetch_add(1, Ordering::Relaxed);
            return;
        };
        event.t_us = ring.stamp(at_us.unwrap_or_else(realtime_us));
        ring.mark_event_cpu(event.cpu);
        ring.put(event);
        drop(ring);
        self.counters.events.fetch_add(1, Ordering::Relaxed);
    }

    /// Keep the `sec` lines of one reading: every line (`keep_all`), or
    /// those of a CPU that had an event or any steal in the interval. False
    /// when the ring was in use: the caller offers the lines again.
    fn put_secs(&self, at_us: Option<u64>, lines: &mut [SecLine], keep_all: bool) -> bool {
        let Some(mut ring) = self.try_lock() else {
            return false;
        };
        let t_us = ring.stamp(at_us.unwrap_or_else(realtime_us));
        let capacity = ring.secs.len() as u64;
        for line in lines.iter_mut() {
            let had_event = ring.take_event_cpu(line.cpu);
            if capacity == 0 || !(keep_all || had_event || line.steal > 0) {
                continue;
            }
            line.t_us = t_us;
            let slot = (ring.secs_pushed % capacity) as usize;
            ring.secs[slot] = *line;
            ring.secs_pushed += 1;
        }
        true
    }

    fn set_ticks(&self, threads: &[TickThread]) {
        self.lock().tick_cpus = threads.iter().map(|thread| thread.cpu).collect();
        self.counters
            .tick_threads
            .store(threads.len() as u64, Ordering::Relaxed);
        self.counters.tick_pinned.store(
            threads.iter().filter(|thread| thread.pinned).count() as u64,
            Ordering::Relaxed,
        );
    }

    /// Copy one of the rings out, oldest first, a chunk under each hold of
    /// the lock.
    fn copy_out<T: Copy>(&self, ring_of: impl Fn(&Ring) -> (&[T], u64)) -> Vec<T> {
        let mut out = Vec::new();
        // No further than the ring's end when the read began.
        let mut bounds: Option<(u64, u64)> = None;
        loop {
            let ring = self.lock();
            let (slots, pushed) = ring_of(&ring);
            let capacity = slots.len() as u64;
            if capacity == 0 {
                break;
            }
            let base = pushed - pushed.min(capacity);
            let (cursor, limit) = *bounds.get_or_insert((base, pushed));
            // What was overwritten meanwhile is gone.
            let start = cursor.max(base);
            if start >= limit {
                break;
            }
            let end = limit.min(start + READ_CHUNK as u64);
            out.extend((start..end).map(|index| slots[(index % capacity) as usize]));
            drop(ring);
            bounds = Some((end, limit));
        }
        out
    }

    /// The journal's events, each with its cause, and its `sec` lines, both
    /// oldest first: everything, or what was recorded at `since_us` or
    /// later. The cause is worked out over the whole ring. Off the hot
    /// paths: it copies the ring and sorts it.
    pub fn query(&self, since_us: Option<u64>) -> StallReport {
        if self.capacity == 0 {
            return StallReport::default();
        }
        let events = self.copy_out(|ring| (&ring.events[..], ring.pushed));
        let mut secs = self.copy_out(|ring| (&ring.secs[..], ring.secs_pushed));
        let tick_cpus = self.lock().tick_cpus.clone();
        let since_us = since_us.unwrap_or(0);
        let mut events = classify(&events, &tick_cpus);
        events.retain(|line| line.event.t_us >= since_us);
        // A merge moves an event's time forward, out of the ring's order.
        events.sort_by_key(|line| line.event.t_us);
        secs.retain(|line| line.t_us >= since_us);
        StallReport { events, secs }
    }

    /// [`Self::query`] as NDJSON, one line an event or a CPU's second,
    /// oldest first.
    pub fn render_ndjson(&self, since_us: Option<u64>) -> String {
        let report = self.query(since_us);
        let node = serde_json::to_string(&self.node).unwrap_or_else(|_| "\"\"".to_owned());
        let mut body = String::with_capacity((report.events.len() + report.secs.len()) * 192);
        let mut secs = report.secs.iter().peekable();
        for line in &report.events {
            while let Some(sec) = secs.next_if(|sec| sec.t_us < line.event.t_us) {
                write_sec_line(&mut body, &node, sec);
            }
            write_event_line(&mut body, &node, line);
        }
        for sec in secs {
            write_sec_line(&mut body, &node, sec);
        }
        body
    }

    pub fn snapshot(&self) -> StallSnapshot {
        let load = |value: &AtomicU64| value.load(Ordering::Relaxed);
        let counters = &self.counters;
        let tick_threads = load(&counters.tick_threads);
        let per_cpu_ms = |us: &AtomicU64| load(us) / tick_threads.max(1) / 1_000;
        StallSnapshot {
            tick_threads,
            tick_pinned: load(&counters.tick_pinned),
            events: load(&counters.events),
            dropped: load(&counters.dropped),
            tick_late_max_us: load(&counters.tick_late_max_us),
            tick_late_over_2ms: load(&counters.tick_late_over_2ms),
            tick_late_over_100ms: load(&counters.tick_late_over_100ms),
            stopped_ms: per_cpu_ms(&counters.stopped_us),
            starved_ms: per_cpu_ms(&counters.starved_us),
            rx_late_max_us: load(&counters.rx_late_max_us),
        }
    }
}

impl Drop for StallJournal {
    fn drop(&mut self) {
        // Whatever part of them was locked: the allocator gets plain pages
        // back.
        let ring = self.ring.get_mut().unwrap_or_else(PoisonError::into_inner);
        os::unlock(&ring.events[..]);
        os::unlock(&ring.newest[..]);
        os::unlock(&ring.secs[..]);
    }
}

/// Why a tick woke late: its CPU was not run while the thread slept (it
/// spent under a tenth of the lateness on a run queue), or the thread was
/// runnable and kept waiting.
pub fn tick_cause(late_us: u64, run_delay_us: Option<u64>) -> Cause {
    match run_delay_us {
        None => Cause::Unknown,
        Some(delay) if delay.saturating_mul(10) < late_us => Cause::Stopped,
        Some(_) => Cause::Starved,
    }
}

/// The intervals in which each CPU's tick thread found its CPU stopped.
struct Stops {
    /// (CPU, start, end), sorted; a CPU's intervals do not overlap.
    intervals: Vec<(u16, u64, u64)>,
}

impl Stops {
    /// The time of `[start_us, end_us]` that `cpu` was stopped for.
    fn overlap_us(&self, cpu: u16, start_us: u64, end_us: u64) -> u64 {
        let from = self
            .intervals
            .partition_point(|&(other, _, end)| (other, end) <= (cpu, start_us));
        self.intervals[from..]
            .iter()
            .take_while(|&&(other, start, _)| other == cpu && start < end_us)
            .map(|&(_, start, end)| end.min(end_us).saturating_sub(start.max(start_us)))
            .sum()
    }

    /// Whether `cpu` was stopped for half or more of the interval.
    fn covers_half(&self, cpu: u16, start_us: u64, end_us: u64) -> bool {
        let length = end_us.saturating_sub(start_us).max(1);
        self.overlap_us(cpu, start_us, end_us).saturating_mul(2) >= length
    }
}

/// Give each event its cause.
///
/// - A tick is `stopped` or `starved` by its run delay ([`tick_cause`]). A
///   stopped tick has scope `all` when every other CPU of `tick_cpus` was
///   stopped for half or more of it, else `one`.
/// - Any other event is `stopped` when its CPU's tick thread found the CPU
///   stopped for half or more of the event; otherwise it is the thread's
///   `own` work (`class_pass`, `send_slow`) or it `waited` (`class_gap`,
///   `rx_late`, `tee_lag`).
///
/// An event covers `[t_us - span_us, t_us]`.
pub fn classify(events: &[StallEvent], tick_cpus: &[u16]) -> Vec<StallLine> {
    let tick_of = |event: &StallEvent| match event.thread {
        StallThread::Tick(cpu) if event.kind == StallKind::Tick => Some(cpu),
        _ => None,
    };
    let mut intervals: Vec<(u16, u64, u64)> = events
        .iter()
        .filter_map(|event| {
            let cpu = tick_of(event)?;
            (tick_cause(event.dur_us, event.run_delay_us()) == Cause::Stopped)
                .then(|| (cpu, event.start_us(), event.t_us))
        })
        .collect();
    intervals.sort_unstable();
    let stops = Stops { intervals };
    events
        .iter()
        .map(|event| {
            let (start_us, end_us) = (event.start_us(), event.t_us);
            let (cause, scope) = match tick_of(event) {
                Some(cpu) => {
                    let cause = tick_cause(event.dur_us, event.run_delay_us());
                    let scope = (cause == Cause::Stopped).then(|| {
                        let all = tick_cpus
                            .iter()
                            .filter(|&&other| other != cpu)
                            .all(|&other| stops.covers_half(other, start_us, end_us));
                        if all {
                            Scope::All
                        } else {
                            Scope::One
                        }
                    });
                    (cause, scope)
                }
                None if stops.covers_half(event.cpu, start_us, end_us) => (Cause::Stopped, None),
                None => (event.kind.cause_while_running(), None),
            };
            StallLine {
                event: *event,
                cause,
                scope,
            }
        })
        .collect()
}

fn write_event_line(body: &mut String, node_json: &str, line: &StallLine) {
    let event = &line.event;
    let _ = write!(
        body,
        "{{\"node\":{node_json},\"t_us\":{},\"kind\":\"{}\",\"thread\":\"",
        event.t_us,
        event.kind.as_str()
    );
    event.thread.write_name(body);
    let _ = write!(
        body,
        "\",\"cpu\":{},\"dur_us\":{},\"span_us\":{},\"n\":{}",
        event.cpu, event.dur_us, event.span_us, event.n
    );
    if let Some(run_delay_us) = event.run_delay_us() {
        let _ = write!(body, ",\"run_delay_us\":{run_delay_us}");
    }
    if event.thread.is_class() {
        let _ = write!(
            body,
            ",\"minflt\":{},\"majflt\":{}",
            event.minflt, event.majflt
        );
    }
    let _ = write!(body, ",\"cause\":\"{}\"", line.cause.as_str());
    if let Some(scope) = line.scope {
        let _ = write!(body, ",\"scope\":\"{}\"", scope.as_str());
    }
    body.push_str("}\n");
}

fn write_sec_line(body: &mut String, node_json: &str, line: &SecLine) {
    let _ = write!(
        body,
        "{{\"node\":{node_json},\"t_us\":{},\"kind\":\"sec\",\"cpu\":{},\"span_us\":{},\
         \"user\":{},\"system\":{},\"irq\":{},\"softirq\":{},\"steal\":{},\
         \"late_max_us\":{},\"late_over_2ms\":{}",
        line.t_us,
        line.cpu,
        line.span_us,
        line.user,
        line.system,
        line.irq,
        line.softirq,
        line.steal,
        line.late_max_us,
        line.late_over_2ms
    );
    if let Some(work_us_max) = line.work_us_max {
        let _ = write!(body, ",\"work_us_max\":{work_us_max}");
    }
    if let Some(vm) = line.vm {
        let _ = write!(
            body,
            ",\"thp_fault_alloc\":{},\"thp_fault_fallback\":{},\"compact_stall\":{},\
             \"pgfault\":{},\"allocstall\":{}",
            vm.thp_fault_alloc, vm.thp_fault_fallback, vm.compact_stall, vm.pgfault, vm.allocstall
        );
    }
    body.push_str("}\n");
}

/// The journal that the lines on the hot paths record into.
static INSTALLED: OnceLock<Arc<StallJournal>> = OnceLock::new();

/// Tests that record into the process's journal take turns: those of this
/// module and those of the contributor's modules that note events.
#[cfg(test)]
pub(crate) static PROCESS_JOURNAL: Mutex<()> = Mutex::new(());

/// Make `journal` the journal of this process, once; later calls change
/// nothing. Returns the process's journal.
pub fn install(journal: Arc<StallJournal>) -> &'static Arc<StallJournal> {
    INSTALLED.get_or_init(|| journal)
}

/// The journal of this process, once installed.
pub fn installed() -> Option<&'static Arc<StallJournal>> {
    INSTALLED.get()
}

/// The one line a hot path holds: `dur_us` is a duration the caller has
/// measured anyway, and under the kind's threshold this is that comparison
/// and nothing else: no lock, no clock, no system call. At the threshold
/// the event goes to the process's journal. `faults` is a class thread's
/// last once-a-second reading of its own page faults (minor, major): the
/// event carries their growth since.
#[inline]
pub fn note(kind: StallKind, thread: StallThread, dur_us: u64, faults: Option<&[AtomicU64; 2]>) {
    if dur_us >= kind.threshold_us() {
        note_event(kind, thread, dur_us, faults);
    }
}

#[cold]
#[inline(never)]
fn note_event(kind: StallKind, thread: StallThread, dur_us: u64, faults: Option<&[AtomicU64; 2]>) {
    let Some(journal) = INSTALLED.get().filter(|journal| journal.is_enabled()) else {
        return;
    };
    let (minflt, majflt) = faults.map_or((0, 0), |read| {
        let (minor, major) = os::thread_faults();
        (
            minor.saturating_sub(read[0].load(Ordering::Relaxed)),
            major.saturating_sub(read[1].load(Ordering::Relaxed)),
        )
    });
    journal.record(kind, thread, dur_us, minflt, majflt);
}

/// CLOCK_REALTIME in microseconds.
pub fn realtime_us() -> u64 {
    os::realtime_us()
}

/// CLOCK_MONOTONIC in nanoseconds.
pub fn monotonic_ns() -> u64 {
    os::monotonic_ns()
}

/// Clock ticks a second, the unit of a `sec` line's CPU times (USER_HZ).
pub fn clock_ticks_per_second() -> u64 {
    os::clock_ticks_per_second()
}

/// How the tick threads run.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TickConfig {
    /// The period; zero starts no thread.
    pub tick_us: u64,
    /// A tick this late is an event.
    pub threshold_us: u64,
    /// `needletail-nodecheck watch`: every CPU's every second is kept as a
    /// `sec` line, each tick thread times a fixed arithmetic loop every
    /// 100 ms (`work_us_max`), and the first reads `/proc/vmstat`.
    pub detail: bool,
}

impl TickConfig {
    /// A node's tick threads.
    pub const fn every(tick_us: u64) -> Self {
        Self {
            tick_us,
            threshold_us: TICK_LATE_US,
            detail: false,
        }
    }
}

/// One tick thread as it started.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TickThread {
    pub cpu: u16,
    /// The kernel's id of the thread.
    pub tid: i32,
    pub pinned: bool,
    /// Whether the kernel reports the thread's run delay. Without it a late
    /// tick has cause `unknown`.
    pub run_delay: bool,
}

/// The tick threads of a journal. They run until [`Self::stop`], or for the
/// life of the process.
#[derive(Debug)]
pub struct TickThreads {
    stop: Arc<AtomicBool>,
    handles: Vec<std::thread::JoinHandle<()>>,
    threads: Vec<TickThread>,
}

impl TickThreads {
    pub fn threads(&self) -> &[TickThread] {
        &self.threads
    }

    pub fn pinned(&self) -> usize {
        self.threads.iter().filter(|thread| thread.pinned).count()
    }

    /// Stop the threads and wait for them.
    pub fn stop(self) {
        self.stop.store(true, Ordering::Relaxed);
        for handle in self.handles {
            let _ = handle.join();
        }
    }
}

/// What a CPU's tick thread saw in the running second; the first tick
/// thread takes it for the CPU's `sec` line.
#[derive(Debug, Default)]
#[repr(align(64))]
struct CpuSecond {
    late_max_us: AtomicU32,
    late_over_2ms: AtomicU32,
    work_us_max: AtomicU32,
}

/// Start one tick thread for each CPU that the calling thread may run on,
/// named `needletail-tick-<cpu>` and pinned to it where the host allows.
/// None with a zero period, a journal that keeps nothing, or off Linux.
pub fn spawn_ticks(journal: &Arc<StallJournal>, config: TickConfig) -> TickThreads {
    let stop = Arc::new(AtomicBool::new(false));
    let cpus = if config.tick_us == 0 || !journal.is_enabled() {
        Vec::new()
    } else {
        os::allowed_cpus()
    };
    let seconds: Arc<[CpuSecond]> = cpus.iter().map(|_| CpuSecond::default()).collect();
    let (started_tx, started_rx) = std::sync::mpsc::channel();
    let mut handles = Vec::with_capacity(cpus.len());
    for (index, &cpu) in cpus.iter().enumerate() {
        let tick = Tick {
            journal: Arc::clone(journal),
            config,
            cpu,
            index,
            cpus: cpus.clone(),
            seconds: Arc::clone(&seconds),
            stop: Arc::clone(&stop),
        };
        let started = started_tx.clone();
        // A thread that cannot start leaves its CPU without a tick thread:
        // the count of threads says so.
        if let Ok(handle) = std::thread::Builder::new()
            .name(format!("needletail-tick-{cpu}"))
            .spawn(move || tick.run(started))
        {
            handles.push(handle);
        }
    }
    drop(started_tx);
    let mut threads: Vec<TickThread> = started_rx.iter().collect();
    threads.sort_unstable_by_key(|thread| thread.cpu);
    journal.set_ticks(&threads);
    TickThreads {
        stop,
        handles,
        threads,
    }
}

/// Iterations of the fixed work loop: 200 us on an EPYC 7713 core (the
/// dev box; 2.18 ns an iteration measured there).
const WORK_ITERATIONS: u64 = 92_000;
/// How often a tick thread times the work loop (`TickConfig::detail`).
const WORK_EVERY_NS: u64 = 100_000_000;
/// How often the first tick thread reads the CPUs' times.
const SECOND_NS: u64 = 1_000_000_000;

/// A fixed amount of arithmetic: its duration says how fast the CPU is and
/// whether it was taken away meanwhile.
#[inline(never)]
fn fixed_work() -> u64 {
    let mut value = 0x9E37_79B9_7F4A_7C15_u64;
    for step in 0..std::hint::black_box(WORK_ITERATIONS) {
        value = value
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(step);
        value ^= value >> 29;
    }
    std::hint::black_box(value)
}

struct Tick {
    journal: Arc<StallJournal>,
    config: TickConfig,
    cpu: u16,
    /// This thread's place among `cpus`; the first reads the CPUs' times.
    index: usize,
    cpus: Vec<u16>,
    seconds: Arc<[CpuSecond]>,
    stop: Arc<AtomicBool>,
}

impl Tick {
    fn run(self, started: std::sync::mpsc::Sender<TickThread>) {
        let pinned = os::pin(self.cpu);
        os::set_timer_slack_ns(1);
        let mut schedstat = os::RunDelay::open();
        let mut run_delay_ns = schedstat.as_mut().and_then(os::RunDelay::read_ns);
        let mut second = (self.index == 0)
            .then(|| SecondReader::new(&self.cpus, self.config.detail))
            .flatten();
        let _ = started.send(TickThread {
            cpu: self.cpu,
            tid: os::thread_id(),
            pinned,
            run_delay: run_delay_ns.is_some(),
        });
        drop(started);
        let period_ns = self.config.tick_us.saturating_mul(1_000).max(1);
        let cell = &self.seconds[self.index];
        let mut late_max_us = 0_u64;
        let mut now_ns = monotonic_ns();
        let mut due_ns = now_ns + period_ns;
        let mut work_due_ns = now_ns + WORK_EVERY_NS;
        let mut second_due_ns = now_ns + SECOND_NS;
        while !self.stop.load(Ordering::Relaxed) {
            os::sleep_until(due_ns);
            now_ns = monotonic_ns();
            let late_us = now_ns.saturating_sub(due_ns) / 1_000;
            // The growth since the last tick: the delay of this wake only.
            let delay_us = schedstat
                .as_mut()
                .and_then(os::RunDelay::read_ns)
                .map(|total_ns| {
                    let grown = total_ns.saturating_sub(run_delay_ns.unwrap_or(total_ns));
                    run_delay_ns = Some(total_ns);
                    grown / 1_000
                });
            if late_us > late_max_us {
                late_max_us = late_us;
                self.journal
                    .counters
                    .tick_late_max_us
                    .fetch_max(late_us, Ordering::Relaxed);
            }
            let late_us_32 = u32::try_from(late_us).unwrap_or(u32::MAX);
            if late_us_32 > cell.late_max_us.load(Ordering::Relaxed) {
                cell.late_max_us.fetch_max(late_us_32, Ordering::Relaxed);
            }
            if late_us >= TICK_LATE_US {
                cell.late_over_2ms.fetch_add(1, Ordering::Relaxed);
            }
            self.journal
                .tick(self.cpu, late_us, delay_us, self.config.threshold_us);
            // The thread's own work between two ticks is not lateness: the
            // next deadline is taken after it.
            let mut worked = false;
            if self.config.detail && now_ns >= work_due_ns {
                let began_ns = monotonic_ns();
                fixed_work();
                let work_us = monotonic_ns().saturating_sub(began_ns) / 1_000;
                cell.work_us_max.fetch_max(
                    u32::try_from(work_us).unwrap_or(u32::MAX),
                    Ordering::Relaxed,
                );
                work_due_ns = now_ns + WORK_EVERY_NS;
                worked = true;
            }
            if let Some(second) = second.as_mut() {
                if second.pending || now_ns >= second_due_ns {
                    second.read(&self.journal, &self.seconds, now_ns);
                    second_due_ns = now_ns + SECOND_NS;
                    worked = true;
                }
            }
            due_ns += period_ns;
            if worked || due_ns <= now_ns {
                // After a stall, one event: the ticks it covered are not
                // made up.
                due_ns = monotonic_ns() + period_ns;
            }
        }
    }
}

/// The first tick thread's reading, about once a second, of what every
/// tick thread's CPU did (`/proc/stat`) and, in detail, of `/proc/vmstat`.
struct SecondReader {
    stat: os::ProcFile,
    vmstat: Option<os::ProcFile>,
    cpus: Vec<u16>,
    /// Per CPU of `cpus`: user (with nice), system, irq, softirq, steal, as
    /// of the last reading, and the table the next reading is parsed into.
    ticks: Vec<[u64; 5]>,
    fresh: Vec<[u64; 5]>,
    vm: [u64; 5],
    lines: Vec<SecLine>,
    read_ns: u64,
    detail: bool,
    /// The lines of a reading wait for the ring.
    pending: bool,
}

impl SecondReader {
    fn new(cpus: &[u16], detail: bool) -> Option<Self> {
        let mut reader = Self {
            stat: os::ProcFile::open("/proc/stat", 512 + 160 * os::configured_cpus())?,
            vmstat: detail
                .then(|| os::ProcFile::open("/proc/vmstat", 16_384))
                .flatten(),
            cpus: cpus.to_vec(),
            ticks: vec![[0; 5]; cpus.len()],
            fresh: vec![[0; 5]; cpus.len()],
            vm: [0; 5],
            lines: vec![SecLine::default(); cpus.len()],
            read_ns: monotonic_ns(),
            detail,
            pending: false,
        };
        // The first reading is the base of the first second.
        parse_cpu_ticks(reader.stat.read(), &reader.cpus, &mut reader.ticks);
        if let Some(vmstat) = reader.vmstat.as_mut() {
            reader.vm = parse_vmstat(vmstat.read());
        }
        Some(reader)
    }

    fn read(&mut self, journal: &StallJournal, seconds: &[CpuSecond], now_ns: u64) {
        if !self.pending {
            let grown = |now: u64, before: u64| {
                u32::try_from(now.saturating_sub(before)).unwrap_or(u32::MAX)
            };
            let span_us =
                u32::try_from(now_ns.saturating_sub(self.read_ns) / 1_000).unwrap_or(u32::MAX);
            self.read_ns = now_ns;
            // A CPU without a line this time has not moved.
            self.fresh.copy_from_slice(&self.ticks);
            parse_cpu_ticks(self.stat.read(), &self.cpus, &mut self.fresh);
            for (index, &cpu) in self.cpus.iter().enumerate() {
                let (now, before) = (self.fresh[index], self.ticks[index]);
                let cell = &seconds[index];
                self.lines[index] = SecLine {
                    t_us: 0,
                    span_us,
                    cpu,
                    user: grown(now[0], before[0]),
                    system: grown(now[1], before[1]),
                    irq: grown(now[2], before[2]),
                    softirq: grown(now[3], before[3]),
                    steal: grown(now[4], before[4]),
                    late_max_us: cell.late_max_us.swap(0, Ordering::Relaxed),
                    late_over_2ms: cell.late_over_2ms.swap(0, Ordering::Relaxed),
                    work_us_max: self
                        .detail
                        .then(|| cell.work_us_max.swap(0, Ordering::Relaxed)),
                    vm: None,
                };
            }
            std::mem::swap(&mut self.ticks, &mut self.fresh);
            if let Some(vmstat) = self.vmstat.as_mut() {
                let vm = parse_vmstat(vmstat.read());
                let before = std::mem::replace(&mut self.vm, vm);
                self.lines[0].vm = Some(VmGrowth {
                    thp_fault_alloc: grown(vm[0], before[0]),
                    thp_fault_fallback: grown(vm[1], before[1]),
                    compact_stall: grown(vm[2], before[2]),
                    pgfault: grown(vm[3], before[3]),
                    allocstall: grown(vm[4], before[4]),
                });
            }
        }
        self.pending = !journal.put_secs(None, &mut self.lines, self.detail);
    }
}

/// The decimal number at the start of `text`, and what follows it.
fn take_number(text: &[u8]) -> Option<(u64, &[u8])> {
    let digits = text.iter().take_while(|byte| byte.is_ascii_digit()).count();
    if digits == 0 {
        return None;
    }
    let value = text[..digits].iter().fold(0_u64, |value, digit| {
        value.wrapping_mul(10).wrapping_add(u64::from(digit - b'0'))
    });
    Some((value, &text[digits..]))
}

/// Read the `cpuN` lines of `/proc/stat` for the CPUs of `cpus` (sorted)
/// into `out`: user with nice, system, irq, softirq, steal, in clock ticks.
/// A CPU that has no line keeps what `out` held.
fn parse_cpu_ticks(text: &[u8], cpus: &[u16], out: &mut [[u64; 5]]) {
    for line in text.split(|&byte| byte == b'\n') {
        let Some(rest) = line.strip_prefix(b"cpu") else {
            // The CPU lines come first.
            break;
        };
        let Some((cpu, mut rest)) = take_number(rest) else {
            // The line of all CPUs together.
            continue;
        };
        let Ok(index) = cpus.binary_search_by(|&listed| u64::from(listed).cmp(&cpu)) else {
            continue;
        };
        // user nice system idle iowait irq softirq steal
        let mut fields = [0_u64; 8];
        for field in &mut fields {
            let trimmed = rest.strip_prefix(b" ").unwrap_or(rest);
            let Some((value, after)) = take_number(trimmed) else {
                break;
            };
            *field = value;
            rest = after;
        }
        out[index] = [
            fields[0] + fields[1],
            fields[2],
            fields[5],
            fields[6],
            fields[7],
        ];
    }
}

/// `/proc/vmstat`: thp_fault_alloc, thp_fault_fallback, compact_stall,
/// pgfault and allocstall (the sum of its zones' counters).
fn parse_vmstat(text: &[u8]) -> [u64; 5] {
    let mut out = [0_u64; 5];
    for line in text.split(|&byte| byte == b'\n') {
        let Some(space) = line.iter().position(|&byte| byte == b' ') else {
            continue;
        };
        let (key, value) = (&line[..space], &line[space + 1..]);
        let slot = match key {
            b"thp_fault_alloc" => 0,
            b"thp_fault_fallback" => 1,
            b"compact_stall" => 2,
            b"pgfault" => 3,
            _ if key.starts_with(b"allocstall") => 4,
            _ => continue,
        };
        out[slot] += take_number(value).map_or(0, |(value, _)| value);
    }
    out
}

#[cfg(target_os = "linux")]
mod os {
    use std::fs::File;
    use std::os::unix::fs::FileExt;

    fn clock_ns(clock: libc::clockid_t) -> u64 {
        let mut time = libc::timespec {
            tv_sec: 0,
            tv_nsec: 0,
        };
        // SAFETY: clock_gettime writes the timespec it is given.
        unsafe { libc::clock_gettime(clock, &mut time) };
        (time.tv_sec as u64)
            .wrapping_mul(1_000_000_000)
            .wrapping_add(time.tv_nsec as u64)
    }

    pub fn realtime_us() -> u64 {
        clock_ns(libc::CLOCK_REALTIME) / 1_000
    }

    pub fn monotonic_ns() -> u64 {
        clock_ns(libc::CLOCK_MONOTONIC)
    }

    /// Sleep to an absolute CLOCK_MONOTONIC time; a signal does not end it.
    pub fn sleep_until(due_ns: u64) {
        let due = libc::timespec {
            tv_sec: (due_ns / 1_000_000_000) as libc::time_t,
            tv_nsec: (due_ns % 1_000_000_000) as libc::c_long,
        };
        // SAFETY: the timespec is valid and no remainder is asked for.
        while unsafe {
            libc::clock_nanosleep(
                libc::CLOCK_MONOTONIC,
                libc::TIMER_ABSTIME,
                &due,
                std::ptr::null_mut(),
            )
        } == libc::EINTR
        {}
    }

    pub fn current_cpu() -> u16 {
        // SAFETY: sched_getcpu has no preconditions.
        u16::try_from(unsafe { libc::sched_getcpu() }).unwrap_or(0)
    }

    /// The CPUs that the calling thread may run on, in order.
    pub fn allowed_cpus() -> Vec<u16> {
        // SAFETY: a zeroed cpu_set_t is empty; sched_getaffinity fills it.
        let set = unsafe {
            let mut set: libc::cpu_set_t = std::mem::zeroed();
            if libc::sched_getaffinity(0, std::mem::size_of::<libc::cpu_set_t>(), &mut set) != 0 {
                return Vec::new();
            }
            set
        };
        (0..libc::CPU_SETSIZE as usize)
            // SAFETY: the index is inside the set.
            .filter(|&cpu| unsafe { libc::CPU_ISSET(cpu, &set) })
            .map(|cpu| cpu as u16)
            .collect()
    }

    pub fn configured_cpus() -> usize {
        // SAFETY: sysconf takes a name only.
        usize::try_from(unsafe { libc::sysconf(libc::_SC_NPROCESSORS_CONF) }).unwrap_or(1)
    }

    pub fn clock_ticks_per_second() -> u64 {
        // SAFETY: as above.
        u64::try_from(unsafe { libc::sysconf(libc::_SC_CLK_TCK) })
            .unwrap_or(100)
            .max(1)
    }

    /// Pin the calling thread to one CPU.
    pub fn pin(cpu: u16) -> bool {
        // SAFETY: a zeroed cpu_set_t is empty; CPU_SET writes inside it
        // (`cpu` comes from a set of the same size).
        unsafe {
            let mut set: libc::cpu_set_t = std::mem::zeroed();
            libc::CPU_SET(usize::from(cpu), &mut set);
            libc::sched_setaffinity(0, std::mem::size_of::<libc::cpu_set_t>(), &set) == 0
        }
    }

    /// The calling thread's timers fire this close to their time (the
    /// default lets the kernel delay them by 50 us).
    pub fn set_timer_slack_ns(slack_ns: libc::c_ulong) {
        // SAFETY: PR_SET_TIMERSLACK takes one integer.
        unsafe { libc::prctl(libc::PR_SET_TIMERSLACK, slack_ns) };
    }

    pub fn thread_id() -> i32 {
        // SAFETY: gettid has no preconditions.
        (unsafe { libc::syscall(libc::SYS_gettid) }) as i32
    }

    /// Minor and major page faults of the calling thread.
    pub fn thread_faults() -> (u64, u64) {
        // SAFETY: a zeroed rusage is valid; getrusage writes it.
        let usage = unsafe {
            let mut usage: libc::rusage = std::mem::zeroed();
            if libc::getrusage(libc::RUSAGE_THREAD, &mut usage) != 0 {
                return (0, 0);
            }
            usage
        };
        (
            u64::try_from(usage.ru_minflt).unwrap_or(0),
            u64::try_from(usage.ru_majflt).unwrap_or(0),
        )
    }

    /// Lock a slice's pages in memory.
    pub fn lock<T>(slice: &[T]) -> bool {
        // SAFETY: the range is the slice's own memory.
        slice.is_empty()
            || unsafe { libc::mlock(slice.as_ptr().cast(), std::mem::size_of_val(slice)) } == 0
    }

    pub fn unlock<T>(slice: &[T]) {
        if !slice.is_empty() {
            // SAFETY: as above.
            unsafe { libc::munlock(slice.as_ptr().cast(), std::mem::size_of_val(slice)) };
        }
    }

    /// A small `/proc` file, opened once and read whole at each call into
    /// a buffer of its own.
    pub struct ProcFile {
        file: File,
        buffer: Vec<u8>,
    }

    impl ProcFile {
        pub fn open(path: &str, bytes: usize) -> Option<Self> {
            Some(Self {
                file: File::open(path).ok()?,
                buffer: vec![0; bytes],
            })
        }

        /// The start of the file as it is now: as much as the buffer holds.
        pub fn read(&mut self) -> &[u8] {
            let read = self.file.read_at(&mut self.buffer, 0).unwrap_or(0);
            &self.buffer[..read]
        }
    }

    /// The time the calling thread has spent waiting on a run queue: the
    /// second field of its `/proc/thread-self/schedstat`.
    pub struct RunDelay(ProcFile);

    impl RunDelay {
        pub fn open() -> Option<Self> {
            ProcFile::open("/proc/thread-self/schedstat", 96).map(Self)
        }

        pub fn read_ns(&mut self) -> Option<u64> {
            super::parse_run_delay_ns(self.0.read())
        }
    }
}

#[cfg(not(target_os = "linux"))]
mod os {
    use std::time::{Instant, SystemTime, UNIX_EPOCH};

    pub fn realtime_us() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_or(0, |since| since.as_micros() as u64)
    }

    pub fn monotonic_ns() -> u64 {
        static BASE: std::sync::OnceLock<Instant> = std::sync::OnceLock::new();
        BASE.get_or_init(Instant::now).elapsed().as_nanos() as u64
    }

    pub fn sleep_until(due_ns: u64) {
        std::thread::sleep(std::time::Duration::from_nanos(
            due_ns.saturating_sub(monotonic_ns()),
        ));
    }

    pub fn current_cpu() -> u16 {
        0
    }

    /// The tick threads are Linux only.
    pub fn allowed_cpus() -> Vec<u16> {
        Vec::new()
    }

    pub fn configured_cpus() -> usize {
        1
    }

    pub fn clock_ticks_per_second() -> u64 {
        100
    }

    pub fn pin(_cpu: u16) -> bool {
        false
    }

    pub fn set_timer_slack_ns(_slack_ns: u64) {}

    pub fn thread_id() -> i32 {
        0
    }

    pub fn thread_faults() -> (u64, u64) {
        (0, 0)
    }

    pub fn lock<T>(_slice: &[T]) -> bool {
        false
    }

    pub fn unlock<T>(_slice: &[T]) {}

    pub struct ProcFile;

    impl ProcFile {
        pub fn open(_path: &str, _bytes: usize) -> Option<Self> {
            None
        }

        pub fn read(&mut self) -> &[u8] {
            &[]
        }
    }

    pub struct RunDelay;

    impl RunDelay {
        pub fn open() -> Option<Self> {
            None
        }

        pub fn read_ns(&mut self) -> Option<u64> {
            None
        }
    }
}

/// The second field of a `schedstat` line: nanoseconds on a run queue.
#[cfg_attr(not(target_os = "linux"), allow(dead_code))]
fn parse_run_delay_ns(text: &[u8]) -> Option<u64> {
    let (_, rest) = take_number(text)?;
    take_number(rest.strip_prefix(b" ")?).map(|(delay_ns, _)| delay_ns)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    /// An event as the ring holds it once recorded at `t_us`.
    fn at(t_us: u64, event: StallEvent) -> StallEvent {
        StallEvent {
            t_us,
            span_us: event.dur_us,
            n: 1,
            ..event
        }
    }

    fn tick(cpu: u16, dur_us: u64, run_delay_us: u32) -> StallEvent {
        StallEvent {
            dur_us,
            run_delay_us,
            cpu,
            thread: StallThread::Tick(cpu),
            kind: StallKind::Tick,
            ..StallEvent::EMPTY
        }
    }

    fn event(kind: StallKind, thread: StallThread, cpu: u16, dur_us: u64) -> StallEvent {
        StallEvent {
            dur_us,
            cpu,
            thread,
            kind,
            ..StallEvent::EMPTY
        }
    }

    /// A journal whose tick threads are said to run on `cpus`.
    fn journal(capacity: usize, cpus: &[u16]) -> StallJournal {
        let journal = StallJournal::new("lon-edge-1", capacity);
        journal.lock().tick_cpus = cpus.to_vec();
        journal
    }

    fn causes(lines: &[StallLine]) -> Vec<(Cause, Option<Scope>)> {
        lines.iter().map(|line| (line.cause, line.scope)).collect()
    }

    fn json_lines(body: &str) -> Vec<serde_json::Value> {
        body.lines()
            .map(|line| serde_json::from_str(line).expect("each line is one JSON object"))
            .collect()
    }

    #[test]
    fn ring_wraps_and_merges_events_closer_than_10_ms() {
        let journal = journal(4, &[0]);
        // A late tick covers [997_000, 1_000_000].
        journal.put(Some(1_000_000), tick(0, 3_000, 0));
        // Another thread's event between two ticks does not part them.
        journal.put(
            Some(1_000_500),
            event(StallKind::ClassPass, StallThread::Audio, 0, 2_500),
        );
        // It starts 2.5 ms after the first tick ended, and the next 9 ms
        // after this one: one line.
        journal.put(Some(1_005_000), tick(0, 2_500, 0));
        journal.put(Some(1_020_000), tick(0, 6_000, 40));
        // 16 ms after: a line of its own.
        journal.put(Some(1_040_000), tick(0, 4_000, 7));
        let report = journal.query(None);
        let events: Vec<StallEvent> = report.events.iter().map(|line| line.event).collect();
        assert_eq!(
            events,
            vec![
                at(
                    1_000_500,
                    event(StallKind::ClassPass, StallThread::Audio, 0, 2_500)
                ),
                StallEvent {
                    t_us: 1_020_000,
                    // The longest, with its run delay.
                    dur_us: 6_000,
                    run_delay_us: 40,
                    // First start to last end.
                    span_us: 23_000,
                    n: 3,
                    ..tick(0, 0, 0)
                },
                at(1_040_000, tick(0, 4_000, 7)),
            ]
        );
        assert_eq!(journal.snapshot().events, 5);

        // The ring keeps its newest four lines.
        journal.put(
            Some(2_000_000),
            event(StallKind::TeeLag, StallThread::TeeAudio, 1, 12_000),
        );
        journal.put(
            Some(2_100_000),
            event(StallKind::SendSlow, StallThread::Video, 1, 1_500),
        );
        journal.put(
            Some(2_200_000),
            event(StallKind::RxLate, StallThread::Audio, 1, 2_200),
        );
        // The audio thread's last pass is no longer in the ring, and its
        // slot now holds the batch that came late 1 ms ago: a pass is a
        // line of its own, not a part of whatever lies in that slot.
        journal.put(
            Some(2_201_000),
            event(StallKind::ClassPass, StallThread::Audio, 1, 2_500),
        );
        let kinds: Vec<(StallKind, u64, u32)> = journal
            .query(None)
            .events
            .iter()
            .map(|line| (line.event.kind, line.event.t_us, line.event.n))
            .collect();
        assert_eq!(
            kinds,
            vec![
                (StallKind::TeeLag, 2_000_000, 1),
                (StallKind::SendSlow, 2_100_000, 1),
                (StallKind::RxLate, 2_200_000, 1),
                (StallKind::ClassPass, 2_201_000, 1),
            ]
        );
    }

    #[test]
    fn a_late_tick_with_no_run_delay_is_stopped() {
        // The thread slept through it: under a tenth of the lateness on a
        // run queue.
        assert_eq!(tick_cause(673_004, Some(212)), Cause::Stopped);
        assert_eq!(tick_cause(2_000, Some(0)), Cause::Stopped);
        assert_eq!(tick_cause(20_000, Some(1_999)), Cause::Stopped);
        let lines = classify(&[at(5_000_000, tick(0, 673_004, 212))], &[0]);
        // The one CPU the process has was not run: every CPU was stopped.
        assert_eq!(causes(&lines), vec![(Cause::Stopped, Some(Scope::All))]);
    }

    #[test]
    fn a_late_tick_with_run_delay_is_starved() {
        // It was runnable and kept waiting.
        assert_eq!(tick_cause(673_004, Some(600_000)), Cause::Starved);
        assert_eq!(tick_cause(20_000, Some(2_000)), Cause::Starved);
        assert_eq!(tick_cause(2_000, Some(2_000_000)), Cause::Starved);
        // A kernel that does not report run delay cannot say which.
        assert_eq!(tick_cause(673_004, None), Cause::Unknown);
        let lines = classify(
            &[
                at(5_000_000, tick(0, 30_000, 29_500)),
                at(6_000_000, tick(0, 30_000, RUN_DELAY_UNKNOWN)),
            ],
            &[0],
        );
        // Neither has a scope, and neither makes another event `stopped`.
        assert_eq!(
            causes(&lines),
            vec![(Cause::Starved, None), (Cause::Unknown, None)]
        );
    }

    #[test]
    fn a_long_pass_is_own_unless_its_cpu_was_stopped() {
        // A pass of the audio thread on CPU 1: [9_700_000, 10_000_000].
        let pass = at(
            10_000_000,
            event(StallKind::ClassPass, StallThread::Audio, 1, 300_000),
        );
        let cause_with = |ticks: &[StallEvent]| {
            let mut events = vec![pass];
            events.extend_from_slice(ticks);
            classify(&events, &[0, 1])[0].cause
        };
        assert_eq!(cause_with(&[]), Cause::Own);
        // CPU 1's tick thread ran only after the pass ended, and found that
        // its timer had come 299 ms late while it slept.
        assert_eq!(
            cause_with(&[at(10_000_150, tick(1, 299_000, 180))]),
            Cause::Stopped
        );
        // The tick thread was runnable all the while: the pass held the CPU.
        assert_eq!(
            cause_with(&[at(10_000_150, tick(1, 299_000, 298_000))]),
            Cause::Own
        );
        // Another CPU was stopped, not this one.
        assert_eq!(
            cause_with(&[at(10_000_150, tick(0, 299_000, 180))]),
            Cause::Own
        );
        // Stopped for a third of the pass.
        assert_eq!(
            cause_with(&[at(9_900_000, tick(1, 100_000, 10))]),
            Cause::Own
        );
        // Two stops that together cover more than half of it.
        assert_eq!(
            cause_with(&[
                at(9_800_000, tick(1, 90_000, 10)),
                at(9_990_000, tick(1, 80_000, 10)),
            ]),
            Cause::Stopped
        );

        // What each kind is while its CPU runs, and under a stop.
        for (kind, running) in [
            (StallKind::ClassPass, Cause::Own),
            (StallKind::SendSlow, Cause::Own),
            (StallKind::ClassGap, Cause::Waited),
            (StallKind::RxLate, Cause::Waited),
            (StallKind::TeeLag, Cause::Waited),
        ] {
            let late = at(10_000_000, event(kind, StallThread::Video, 1, 50_000));
            assert_eq!(classify(&[late], &[0, 1])[0].cause, running);
            let lines = classify(&[late, at(10_000_010, tick(1, 49_000, 0))], &[0, 1]);
            assert_eq!((lines[0].cause, lines[0].scope), (Cause::Stopped, None));
        }
    }

    #[test]
    fn one_cpu_stopped_is_scope_one() {
        let scope_of = |events: &[StallEvent], cpus: &[u16]| classify(events, cpus)[0].scope;
        let stopped = at(5_000_000, tick(1, 673_004, 212));
        // Only CPU 1 of three.
        assert_eq!(scope_of(&[stopped], &[0, 1, 2]), Some(Scope::One));
        // CPU 0 too, CPU 2 not.
        let cpu_0 = at(5_000_100, tick(0, 672_000, 90));
        assert_eq!(scope_of(&[stopped, cpu_0], &[0, 1, 2]), Some(Scope::One));
        // Every CPU, each for half or more of it.
        let cpu_2 = at(4_800_000, tick(2, 400_000, 0));
        assert_eq!(
            scope_of(&[stopped, cpu_0, cpu_2], &[0, 1, 2]),
            Some(Scope::All)
        );
        // CPU 2 was late at that time because it was busy, not stopped.
        let cpu_2_busy = at(4_800_000, tick(2, 400_000, 390_000));
        assert_eq!(
            scope_of(&[stopped, cpu_0, cpu_2_busy], &[0, 1, 2]),
            Some(Scope::One)
        );
        // CPU 2 was stopped for under half of it.
        let cpu_2_briefly = at(4_500_000, tick(2, 400_000, 0));
        assert_eq!(
            scope_of(&[stopped, cpu_0, cpu_2_briefly], &[0, 1, 2]),
            Some(Scope::One)
        );
        // Each of the two is `all` to a node of two CPUs.
        assert_eq!(
            causes(&classify(&[stopped, cpu_0], &[0, 1])),
            vec![
                (Cause::Stopped, Some(Scope::All)),
                (Cause::Stopped, Some(Scope::All))
            ]
        );
    }

    #[test]
    fn a_contended_record_is_dropped_and_counted() {
        let journal = journal(8, &[0]);
        let ring = journal.lock();
        // A reader (or another recorder) has the ring: nothing waits.
        journal.record(StallKind::ClassPass, StallThread::Audio, 2_500, 0, 0);
        journal.record(StallKind::RxLate, StallThread::Video, 4_000, 0, 0);
        journal.tick(0, 150_000, Some(20), TICK_LATE_US);
        let mut lines = [SecLine {
            steal: 3,
            ..SecLine::default()
        }];
        // The lines of a second are offered again, not counted as lost.
        assert!(!journal.put_secs(None, &mut lines, true));
        drop(ring);
        let snapshot = journal.snapshot();
        assert_eq!((snapshot.events, snapshot.dropped), (0, 3));
        assert!(journal.query(None).events.is_empty());
        // What is counted outside the ring is counted all the same.
        assert_eq!(snapshot.tick_late_over_2ms, 1);
        assert_eq!(snapshot.tick_late_over_100ms, 1);
        assert_eq!(snapshot.tick_late_max_us, 0);
        assert_eq!(snapshot.rx_late_max_us, 4_000);

        journal.record(StallKind::ClassPass, StallThread::Audio, 2_500, 0, 0);
        assert!(journal.put_secs(None, &mut lines, true));
        let snapshot = journal.snapshot();
        assert_eq!((snapshot.events, snapshot.dropped), (1, 3));
        let report = journal.query(None);
        assert_eq!((report.events.len(), report.secs.len()), (1, 1));
    }

    #[test]
    fn a_line_has_the_journal_s_format() {
        let journal = journal(8, &[0, 1]);
        journal.put(Some(1_795_381_726_393_100), tick(0, 673_000, 90));
        journal.put(Some(1_795_381_726_393_150), tick(1, 673_004, 212));
        journal.put(
            Some(1_795_381_726_393_400),
            StallEvent {
                minflt: 17,
                majflt: 1,
                ..event(StallKind::ClassPass, StallThread::Audio, 1, 673_900)
            },
        );
        journal.put(
            Some(1_795_381_726_400_000),
            event(StallKind::TeeLag, StallThread::TeeVideo, 0, 12_000),
        );
        let mut secs = [
            SecLine {
                span_us: 1_000_042,
                cpu: 0,
                user: 3,
                system: 2,
                irq: 0,
                softirq: 1,
                steal: 67,
                late_max_us: 673_000,
                late_over_2ms: 1,
                work_us_max: Some(212),
                vm: Some(VmGrowth {
                    thp_fault_alloc: 1,
                    thp_fault_fallback: 2,
                    compact_stall: 3,
                    pgfault: 4,
                    allocstall: 5,
                }),
                ..SecLine::default()
            },
            SecLine {
                span_us: 1_000_042,
                cpu: 1,
                steal: 66,
                ..SecLine::default()
            },
        ];
        assert!(journal.put_secs(Some(1_795_381_727_000_000), &mut secs, false));
        let body = journal.render_ndjson(None);
        let expected = concat!(
            r#"{"node":"lon-edge-1","t_us":1795381726393100,"kind":"tick","thread":"tick-0","cpu":0,"dur_us":673000,"span_us":673000,"n":1,"run_delay_us":90,"cause":"stopped","scope":"all"}"#,
            "\n",
            r#"{"node":"lon-edge-1","t_us":1795381726393150,"kind":"tick","thread":"tick-1","cpu":1,"dur_us":673004,"span_us":673004,"n":1,"run_delay_us":212,"cause":"stopped","scope":"all"}"#,
            "\n",
            r#"{"node":"lon-edge-1","t_us":1795381726393400,"kind":"class_pass","thread":"audio","cpu":1,"dur_us":673900,"span_us":673900,"n":1,"minflt":17,"majflt":1,"cause":"stopped"}"#,
            "\n",
            r#"{"node":"lon-edge-1","t_us":1795381726400000,"kind":"tee_lag","thread":"tee-video","cpu":0,"dur_us":12000,"span_us":12000,"n":1,"cause":"waited"}"#,
            "\n",
            r#"{"node":"lon-edge-1","t_us":1795381727000000,"kind":"sec","cpu":0,"span_us":1000042,"user":3,"system":2,"irq":0,"softirq":1,"steal":67,"late_max_us":673000,"late_over_2ms":1,"work_us_max":212,"thp_fault_alloc":1,"thp_fault_fallback":2,"compact_stall":3,"pgfault":4,"allocstall":5}"#,
            "\n",
            r#"{"node":"lon-edge-1","t_us":1795381727000000,"kind":"sec","cpu":1,"span_us":1000042,"user":0,"system":0,"irq":0,"softirq":0,"steal":66,"late_max_us":0,"late_over_2ms":0}"#,
            "\n",
        );
        assert_eq!(body, expected);
        assert_eq!(json_lines(&body).len(), 6);
        // Names of the journal's format.
        for (kind, name) in [
            (StallKind::Tick, "tick"),
            (StallKind::ClassPass, "class_pass"),
            (StallKind::ClassGap, "class_gap"),
            (StallKind::RxLate, "rx_late"),
            (StallKind::TeeLag, "tee_lag"),
            (StallKind::SendSlow, "send_slow"),
        ] {
            assert_eq!(kind.as_str(), name);
        }
        // A node name is written as JSON.
        let quoted = StallJournal::new("edge-\"a\"", 8);
        quoted.put(Some(1), tick(0, 2_000, 0));
        assert_eq!(
            json_lines(&quoted.render_ndjson(None))[0]["node"],
            "edge-\"a\""
        );
    }

    #[test]
    fn a_read_by_time_misses_nothing_and_shows_a_grown_event_again() {
        let journal = journal(64, &[0]);
        journal.put(Some(1_000_000), tick(0, 3_000, 0));
        journal.put(
            Some(1_000_000),
            event(StallKind::RxLate, StallThread::Audio, 0, 2_100),
        );
        // Two records in one microsecond, or a clock that stepped back:
        // every time given out is after the one before.
        journal.put(
            Some(900_000),
            event(StallKind::SendSlow, StallThread::Audio, 0, 1_100),
        );
        let first = journal.query(None);
        let times: Vec<u64> = first.events.iter().map(|line| line.event.t_us).collect();
        assert_eq!(times, vec![1_000_000, 1_000_001, 1_000_002]);

        // A reader that comes back for what is after the last line it saw.
        let since = times[2] + 1;
        assert!(journal.query(Some(since)).events.is_empty());
        journal.put(Some(1_004_000), tick(0, 2_500, 0));
        journal.put(
            Some(1_500_000),
            event(StallKind::TeeLag, StallThread::TeeAudio, 0, 10_000),
        );
        let second = journal.query(Some(since));
        let seen: Vec<(StallKind, u64, u64, u32)> = second
            .events
            .iter()
            .map(|line| {
                let event = line.event;
                (event.kind, event.t_us, event.start_us(), event.n)
            })
            .collect();
        // The tick it had seen grew by a merge: the same kind, thread and
        // start, a later time and a higher count.
        assert_eq!(first.events[0].event.start_us(), 997_000);
        assert_eq!(
            seen,
            vec![
                (StallKind::Tick, 1_004_000, 997_000, 2),
                (StallKind::TeeLag, 1_500_000, 1_490_000, 1),
            ]
        );
        // Only the filter is by time: the cause is worked out over the
        // whole ring.
        let stopped_before = journal.query(Some(1_500_000));
        assert_eq!(stopped_before.events.len(), 1);
        let body = journal.render_ndjson(Some(1_500_000));
        assert_eq!(json_lines(&body)[0]["kind"], "tee_lag");
    }

    #[test]
    fn a_cpu_s_second_is_kept_when_it_had_an_event_or_any_steal() {
        let journal = journal(64, &[0, 1, 2]);
        let second = |steal: u32| {
            [0_u16, 1, 2].map(|cpu| SecLine {
                span_us: 1_000_000,
                cpu,
                user: 10,
                steal: if cpu == 2 { steal } else { 0 },
                ..SecLine::default()
            })
        };
        let kept = |journal: &StallJournal| -> Vec<(u64, u16)> {
            journal
                .query(None)
                .secs
                .iter()
                .map(|line| (line.t_us, line.cpu))
                .collect()
        };
        // A quiet second leaves no line.
        assert!(journal.put_secs(Some(1_000_000), &mut second(0), false));
        assert!(kept(&journal).is_empty());
        // Steal on CPU 2, an event recorded on CPU 1.
        journal.put(
            Some(1_600_000),
            event(StallKind::ClassPass, StallThread::Audio, 1, 2_500),
        );
        assert!(journal.put_secs(Some(2_000_000), &mut second(4), false));
        assert_eq!(kept(&journal), vec![(2_000_000, 1), (2_000_000, 2)]);
        // The event counted for its own second only.
        assert!(journal.put_secs(Some(3_000_000), &mut second(0), false));
        assert_eq!(kept(&journal).len(), 2);
        // `needletail-nodecheck watch` keeps every CPU's every second.
        assert!(journal.put_secs(Some(4_000_000), &mut second(0), true));
        assert_eq!(kept(&journal).len(), 5);
        // The lines of one reading share a time, after every event before.
        let report = journal.query(Some(2_000_000));
        assert_eq!(report.events.len(), 0);
        assert_eq!(report.secs.len(), 5);

        // The lines are a ring of their own: steal every second pushes no
        // event out.
        let small = StallJournal::new("n", 8);
        small.put(Some(1), tick(0, 2_000, 0));
        for second_index in 0..1_000_u64 {
            let mut line = [SecLine {
                steal: 1,
                ..SecLine::default()
            }];
            assert!(small.put_secs(Some(10 + second_index), &mut line, false));
        }
        let report = small.query(None);
        assert_eq!(report.events.len(), 1);
        assert_eq!(report.secs.len(), SEC_LINES_MIN);
        assert_eq!(report.secs.last().unwrap().t_us, 1_009);
    }

    #[test]
    fn the_rings_are_faulted_in_and_locked_at_start() {
        let capacity = 16_384;
        let journal = journal(capacity, &[0]);
        let mut limit = libc::rlimit {
            rlim_cur: 0,
            rlim_max: 0,
        };
        // SAFETY: getrlimit writes the struct it is given.
        unsafe { libc::getrlimit(libc::RLIMIT_MEMLOCK, &mut limit) };
        let bytes = capacity * std::mem::size_of::<StallEvent>()
            + capacity / SEC_LINES_SHARE * std::mem::size_of::<SecLine>()
            + (NAMED_THREADS * KINDS + MAX_CPUS) * 8;
        if limit.rlim_cur == libc::RLIM_INFINITY || limit.rlim_cur as usize >= 4 * bytes {
            assert!(journal.ring_locked());
        }
        // Filling the ring and its second lines takes no page fault.
        let (before, _) = os::thread_faults();
        for index in 0..capacity as u64 {
            journal.put(
                Some(1_000_000 + index * 20_000),
                tick((index % 2) as u16, 2_000, 0),
            );
            let mut line = [SecLine {
                steal: 1,
                ..SecLine::default()
            }];
            journal.put_secs(Some(1_000_001 + index * 20_000), &mut line, false);
        }
        let (after, _) = os::thread_faults();
        assert_eq!(journal.snapshot().events, capacity as u64);
        assert!(
            after - before <= 2,
            "recording faulted {} pages in",
            after - before
        );
        // A journal that keeps nothing holds no memory and records nothing.
        let off = StallJournal::new("n", 0);
        assert!(!off.is_enabled() && !off.ring_locked());
        off.record(StallKind::ClassPass, StallThread::Audio, 9_000, 0, 0);
        off.tick(0, 9_000, Some(0), TICK_LATE_US);
        assert_eq!(off.snapshot().events, 0);
        assert!(off.render_ndjson(None).is_empty());
        assert!(spawn_ticks(&Arc::new(off), TickConfig::every(1_000))
            .threads()
            .is_empty());
        // Nor does a zero period start a thread.
        let journal = Arc::new(journal);
        assert!(spawn_ticks(&journal, TickConfig::every(0))
            .threads()
            .is_empty());
    }

    #[test]
    fn cpu_times_and_run_delay_are_read_from_proc_text() {
        let stat =
            b"cpu  36977221 36330474 20783396 716884629 144398 5140543 12197480 266705 0 0\n\
            cpu0 1090311 1100717 622343 22326373 7769 165441 522832 4316 0 0\n\
            cpu1 1214813 1115645 637131 22347522 8950 160882 409192 7923 0 0\n\
            cpu2 5 6 7 8 9 10 11 12 0 0\n\
            cpu10 1 1 1 1 1 1 1 1 0 0\n\
            intr 123 cpu1 9 9 9 9 9 9 9 9\n";
        let mut ticks = [[7_u64; 5]; 3];
        parse_cpu_ticks(stat, &[1, 2, 5], &mut ticks);
        assert_eq!(
            ticks,
            [
                [1_214_813 + 1_115_645, 637_131, 160_882, 409_192, 7_923],
                [11, 7, 10, 11, 12],
                // No line for CPU 5: it keeps what it had.
                [7; 5],
            ]
        );
        assert_eq!(parse_run_delay_ns(b"2423552 149613 22\n"), Some(149_613));
        assert_eq!(parse_run_delay_ns(b"0 0 0\n"), Some(0));
        assert_eq!(parse_run_delay_ns(b""), None);
        let vmstat = b"nr_free_pages 1\nallocstall_dma 0\nallocstall_normal 18\n\
            allocstall_movable 52658\npgfault 5592878374\ncompact_stall 99974\n\
            thp_fault_alloc 5811188\nthp_fault_fallback 448912\nthp_fault_fallback_charge 3\n";
        assert_eq!(
            parse_vmstat(vmstat),
            [5_811_188, 448_912, 99_974, 5_592_878_374, 52_676]
        );
        // This machine's own files have what the reader looks for.
        if cfg!(target_os = "linux") {
            let cpus = os::allowed_cpus();
            assert!(!cpus.is_empty());
            let mut reader = SecondReader::new(&cpus, true).expect("/proc/stat opens");
            assert!(reader.ticks.iter().all(|ticks| ticks[0] + ticks[1] > 0));
            assert!(reader.vm[3] > 0, "pgfault is read");
            assert!(os::RunDelay::open().unwrap().read_ns().is_some());
            let quiet = StallJournal::new("n", 8);
            let seconds: Vec<CpuSecond> = cpus.iter().map(|_| CpuSecond::default()).collect();
            reader.read(&quiet, &seconds, monotonic_ns());
            assert!(!reader.pending);
            assert_eq!(quiet.query(None).secs.len(), cpus.len());
        }
    }

    #[test]
    fn a_line_under_its_threshold_is_a_comparison_and_takes_no_lock() {
        let _turn = PROCESS_JOURNAL
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        let journal = install(Arc::new(StallJournal::new("process", 64)));
        let faults = [AtomicU64::new(0), AtomicU64::new(0)];
        let lines = [
            (StallKind::ClassPass, StallThread::Audio),
            (StallKind::ClassGap, StallThread::Audio),
            (StallKind::RxLate, StallThread::Video),
            (StallKind::SendSlow, StallThread::Video),
            (StallKind::TeeLag, StallThread::TeeAudio),
        ];
        // The ring is held throughout: a line that reached for it would be
        // counted as dropped.
        let ring = journal.lock();
        let before = journal.snapshot();
        let mut best_ns = f64::MAX;
        const CALLS: u64 = 200_000;
        for _ in 0..50 {
            let began = std::time::Instant::now();
            for call in 0..CALLS {
                let (kind, thread) = lines[(call % 5) as usize];
                note(
                    std::hint::black_box(kind),
                    thread,
                    std::hint::black_box(kind.threshold_us() - 1),
                    Some(&faults),
                );
            }
            best_ns = best_ns.min(began.elapsed().as_nanos() as f64 / CALLS as f64);
        }
        assert_eq!(journal.snapshot(), before);
        eprintln!("a quiet stall-journal line: {best_ns:.2} ns");
        let bound_ns = if cfg!(debug_assertions) { 200.0 } else { 10.0 };
        assert!(best_ns < bound_ns, "a quiet line took {best_ns:.2} ns");
        // The same lines at their thresholds do reach the journal.
        for (kind, thread) in lines {
            note(kind, thread, kind.threshold_us(), Some(&faults));
        }
        assert_eq!(journal.snapshot().dropped, before.dropped + 5);
        drop(ring);
        for (kind, thread) in lines {
            note(kind, thread, kind.threshold_us(), Some(&faults));
        }
        assert_eq!(journal.snapshot().events, before.events + 5);
    }

    #[test]
    fn a_class_thread_s_event_carries_its_page_faults_since_its_last_reading() {
        let _turn = PROCESS_JOURNAL
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        let journal = install(Arc::new(StallJournal::new("process", 64)));
        let (minor, major) = os::thread_faults();
        let read = [AtomicU64::new(minor), AtomicU64::new(major)];
        // Fresh pages touched since the thread's reading: a mapping of its
        // own, which the allocator's heap (whatever its thresholds) cannot
        // have touched before.
        let length = 64 * 4_096;
        // SAFETY: an anonymous private mapping, unmapped below.
        let block = unsafe {
            libc::mmap(
                std::ptr::null_mut(),
                length,
                libc::PROT_READ | libc::PROT_WRITE,
                libc::MAP_PRIVATE | libc::MAP_ANONYMOUS,
                -1,
                0,
            )
        };
        assert_ne!(block, libc::MAP_FAILED);
        for page in 0..64 {
            // SAFETY: inside the mapping.
            unsafe { std::ptr::write_volatile(block.cast::<u8>().add(page * 4_096), 1) };
        }
        let since = journal.lock().last_t_us + 1;
        // A duration no other test's event has.
        note(
            StallKind::ClassPass,
            StallThread::Video,
            3_600_000_123,
            Some(&read),
        );
        note(
            StallKind::TeeLag,
            StallThread::TeeVideo,
            3_600_000_123,
            None,
        );
        let report = journal.query(Some(since));
        let of = |kind: StallKind| {
            report
                .events
                .iter()
                .map(|line| line.event)
                .find(|event| event.kind == kind && event.dur_us == 3_600_000_123)
                .expect("the event is in the journal")
        };
        let pass = of(StallKind::ClassPass);
        if cfg!(target_os = "linux") {
            assert!(pass.minflt >= 1, "no fault counted");
            assert_eq!(pass.cpu, os::current_cpu());
        }
        // A tee reads no faults.
        assert_eq!(of(StallKind::TeeLag).minflt, 0);
        // SAFETY: the mapping made above.
        unsafe { libc::munmap(block, length) };
    }

    #[cfg(target_os = "linux")]
    mod threads {
        use super::*;

        fn set_affinity(cpus: &[u16]) {
            // SAFETY: a zeroed cpu_set_t is empty; CPU_SET writes inside it.
            unsafe {
                let mut set: libc::cpu_set_t = std::mem::zeroed();
                for &cpu in cpus {
                    libc::CPU_SET(usize::from(cpu), &mut set);
                }
                libc::sched_setaffinity(0, std::mem::size_of::<libc::cpu_set_t>(), &set);
            }
        }

        /// Run `body` with the calling thread allowed on `count` of its CPUs
        /// only, so that `spawn_ticks` starts that many threads.
        fn on_cpus<T>(count: usize, body: impl FnOnce(&[u16]) -> T) -> T {
            let all = os::allowed_cpus();
            let some = &all[..count.min(all.len())];
            set_affinity(some);
            let out = body(some);
            set_affinity(&all);
            out
        }

        const HOLD_MS: u64 = 300;

        extern "C" fn hold(_signal: libc::c_int) {
            let held = libc::timespec {
                tv_sec: 0,
                tv_nsec: (HOLD_MS * 1_000_000) as libc::c_long,
            };
            // SAFETY: nanosleep is async-signal-safe.
            unsafe { libc::nanosleep(&held, std::ptr::null_mut()) };
        }

        /// Keep one thread from running for `HOLD_MS`, as a stopped CPU
        /// would: it sleeps inside a signal handler, on no run queue.
        fn hold_thread(tid: i32) {
            // SAFETY: the handler only sleeps; the signal goes to one thread
            // of this process.
            unsafe {
                libc::signal(libc::SIGUSR1, hold as *const () as libc::sighandler_t);
                libc::syscall(libc::SYS_tgkill, libc::getpid(), tid, libc::SIGUSR1);
            }
        }

        #[test]
        fn a_held_tick_thread_books_a_stop_on_its_cpu_only() {
            on_cpus(2, |cpus| {
                if cpus.len() < 2 {
                    return;
                }
                let journal = Arc::new(StallJournal::new("n", 1_024));
                let ticks = spawn_ticks(&journal, TickConfig::every(1_000));
                let threads = ticks.threads().to_vec();
                assert_eq!(
                    threads.iter().map(|thread| thread.cpu).collect::<Vec<_>>(),
                    cpus
                );
                assert_eq!(ticks.pinned(), 2);
                let snapshot = journal.snapshot();
                assert_eq!((snapshot.tick_threads, snapshot.tick_pinned), (2, 2));
                let held = threads[1];
                if !held.run_delay {
                    ticks.stop();
                    return;
                }
                // On a busy machine the held thread may also wait for its
                // CPU once it wakes, which is rightly `starved`: the hold is
                // tried a few times.
                let mut stopped = None;
                for _ in 0..4 {
                    let since = realtime_us();
                    std::thread::sleep(Duration::from_millis(30));
                    hold_thread(held.tid);
                    std::thread::sleep(Duration::from_millis(HOLD_MS + 150));
                    let report = journal.query(Some(since));
                    let long: Vec<StallLine> = report
                        .events
                        .iter()
                        .copied()
                        .filter(|line| line.event.dur_us >= HOLD_MS * 1_000 - 2_000)
                        .collect();
                    // The hold is one event, of the held thread.
                    assert_eq!(long.len(), 1, "{long:?}");
                    assert_eq!(long[0].event.thread, StallThread::Tick(held.cpu));
                    assert_eq!(long[0].event.kind, StallKind::Tick);
                    if long[0].cause == Cause::Stopped {
                        stopped = Some(long[0]);
                        break;
                    }
                }
                ticks.stop();
                let stopped = stopped.expect("a held thread reads as stopped");
                // The other CPU's tick thread kept time: one CPU.
                assert_eq!(stopped.scope, Some(Scope::One));
                assert!(stopped.event.run_delay_us().unwrap() * 10 < stopped.event.dur_us);
                let snapshot = journal.snapshot();
                assert!(snapshot.tick_late_over_100ms >= 1);
                assert!(snapshot.tick_late_max_us >= HOLD_MS * 1_000 - 2_000);
                // Averaged over the two tick threads.
                assert!(snapshot.stopped_ms >= HOLD_MS / 2 - 1, "{snapshot:?}");
            });
        }

        #[test]
        fn in_detail_every_cpu_has_a_line_for_every_second() {
            on_cpus(2, |cpus| {
                let journal = Arc::new(StallJournal::new("n", 1_024));
                let ticks = spawn_ticks(
                    &journal,
                    TickConfig {
                        detail: true,
                        ..TickConfig::every(1_000)
                    },
                );
                std::thread::sleep(Duration::from_millis(2_400));
                ticks.stop();
                let report = journal.query(None);
                for &cpu in cpus {
                    let lines: Vec<&SecLine> =
                        report.secs.iter().filter(|line| line.cpu == cpu).collect();
                    assert!(lines.len() >= 2, "{} lines for CPU {cpu}", lines.len());
                    for line in lines {
                        assert!(
                            (900_000..2_000_000).contains(&line.span_us),
                            "a second of {} us",
                            line.span_us
                        );
                        // The fixed work loop ran, and took about what it
                        // was sized for.
                        let work_us = line.work_us_max.expect("the work loop is timed");
                        assert!(work_us >= 20, "the work loop took {work_us} us");
                        assert_eq!(line.vm.is_some(), cpu == cpus[0]);
                    }
                }
                let first = report.secs.iter().find(|line| line.vm.is_some()).unwrap();
                eprintln!(
                    "fixed work loop: {} us at most in a second; the second: {first:?}",
                    first.work_us_max.unwrap()
                );
            });
        }

        /// The thread's CPU time: the first field of its schedstat.
        fn cpu_ns(tid: i32) -> u64 {
            let text = std::fs::read_to_string(format!("/proc/self/task/{tid}/schedstat")).unwrap();
            text.split(' ').next().unwrap().parse().unwrap()
        }

        /// The specification's figure is under 1 % of a CPU for 1,000 wakes
        /// and reads a second. Measured here (an EPYC 7713 guest under
        /// load): 7.1 to 8.9 us of CPU a tick, of which the schedstat read
        /// is 0.9 us and the rest is the sleep and the wake. The bound
        /// leaves room for a loaded machine; the figure is printed.
        #[test]
        fn a_tick_thread_costs_about_one_percent_of_its_cpu_at_most() {
            on_cpus(1, |_| {
                let journal = Arc::new(StallJournal::new("n", 1_024));
                let ticks = spawn_ticks(&journal, TickConfig::every(1_000));
                let thread = ticks.threads()[0];
                std::thread::sleep(Duration::from_millis(200));
                // The best of three seconds: the others' noise is not its cost.
                let mut best_permille = u64::MAX;
                let mut best_tick_us = f64::MAX;
                for _ in 0..3 {
                    let (cpu_before, began) = (cpu_ns(thread.tid), std::time::Instant::now());
                    std::thread::sleep(Duration::from_millis(1_000));
                    let used_ns = cpu_ns(thread.tid) - cpu_before;
                    let wall_ns = began.elapsed().as_nanos() as u64;
                    best_permille = best_permille.min(used_ns * 1_000 / wall_ns);
                    best_tick_us = best_tick_us.min(used_ns as f64 / (wall_ns as f64 / 1_000.0));
                }
                ticks.stop();
                eprintln!(
                    "a tick thread at 1,000 wakes a second: {best_tick_us:.2} us of CPU a tick, \
                     {best_permille} permille of its CPU"
                );
                assert!(best_permille < 15, "{best_permille} permille of a CPU");
            });
        }
    }
}
