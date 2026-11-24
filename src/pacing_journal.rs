//! The sender's lateness journal: when each epoch left against its
//! deadline, as NDJSON, written while the sender runs.
//!
//! The pacing thread hands one [`EpochTiming`] per epoch to a bounded queue
//! and goes on: no lock, no allocation, no system call. A thread of the
//! journal's own takes them out (it polls, so the queue never has to wake
//! it), and writes
//!
//! - one `late` line for every epoch whose first datagram left
//!   `late_threshold_us` or more after its deadline:
//!   `{"kind":"late","epoch":..,"deadline_us":..,"woke_late_us":..,"encode_us":..,
//!   "late_first_us":..,"late_last_us":..}`;
//! - one `sec` line for every Unix second in which an epoch was due:
//!   `{"kind":"sec","unix_s":..,"epochs":..,"late_p50_us":..,"late_p99_us":..,
//!   "late_max_us":..,"woke_late_max_us":..,"encode_p50_us":..,"encode_max_us":..,
//!   "over_1ms":..,"over_5ms":..,"minflt":..,"majflt":..,"nivcsw":..,
//!   "run_delay_us":..,"dropped":..}`.
//!
//! `late_*` is the first datagram's lateness; `woke_late_us` is how long
//! after the deadline the pacer came out of its wait (the scheduler's share
//! of a late epoch) and `encode_us` the time to make the epoch's datagrams
//! (the encoder's). `minflt`, `majflt`, `nivcsw` (involuntary context
//! switches) and `run_delay_us` (time runnable but not running) are the
//! pacing thread's own, grown since the previous `sec` line; the journal
//! thread reads them from `/proc`. `dropped` counts, since the start, the
//! epochs that found the queue full: the pacer never waits for the journal.
//!
//! A stall delays only the epochs it covers and each then leaves at once
//! (the deadlines are absolute), so a hold of n ms reads as n `late` lines
//! whose lateness falls by one epoch a line.

use serde::Serialize;
use std::io::{self, Write};
use std::path::Path;
use std::sync::atomic::{AtomicI32, AtomicU64, Ordering};
use std::sync::mpsc::{sync_channel, Receiver, SyncSender, TryRecvError, TrySendError};
use std::sync::Arc;
use std::thread::JoinHandle;
use std::time::Duration;

/// `--lateness-journal-us`: an epoch this late, or later, gets a `late` line.
pub const DEFAULT_LATE_THRESHOLD_US: u64 = 500;

/// Epochs that wait for the journal's thread at most: four seconds of the
/// shortest epochs (250 us).
pub const QUEUE_EPOCHS: usize = 16_384;

/// How often the journal's thread looks at its queue.
const POLL: Duration = Duration::from_millis(5);

/// One epoch as the pacer saw it. Times are Unix nanoseconds; `encode_ns`
/// is a duration.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EpochTiming {
    pub epoch: u64,
    /// When the epoch was due to leave.
    pub deadline_ns: u64,
    /// When the pacer came out of its wait for the deadline.
    pub woke_ns: u64,
    /// Time to make the epoch's datagrams.
    pub encode_ns: u64,
    /// First and last datagram handed to the kernel.
    pub first_sent_ns: u64,
    pub last_sent_ns: u64,
}

/// What a journal wrote, for the sender's report.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize)]
pub struct JournalTotals {
    /// Epochs journalled.
    pub epochs: u64,
    pub late_lines: u64,
    pub sec_lines: u64,
    /// Epochs that found the queue full and were not journalled.
    pub dropped: u64,
}

/// The pacing thread's end of a journal.
#[derive(Debug, Clone)]
pub struct PacingRecorder {
    queue: SyncSender<EpochTiming>,
    dropped: Arc<AtomicU64>,
    pacer_tid: Arc<AtomicI32>,
}

impl PacingRecorder {
    /// Name the calling thread as the pacing thread: the `sec` lines carry
    /// its faults, context switches and run delay from now on.
    pub fn attach_current_thread(&self) {
        self.pacer_tid.store(current_tid(), Ordering::Release);
    }

    /// Journal one epoch. Never waits: an epoch that finds the queue full
    /// is counted in `dropped` and left out.
    #[inline]
    pub fn record(&self, timing: EpochTiming) {
        if let Err(TrySendError::Full(_)) = self.queue.try_send(timing) {
            self.dropped.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Epochs left out so far.
    pub fn dropped(&self) -> u64 {
        self.dropped.load(Ordering::Relaxed)
    }
}

/// A running journal: the thread that writes it and the way to end it.
#[derive(Debug)]
pub struct PacingJournal {
    recorder: PacingRecorder,
    writer: JoinHandle<io::Result<JournalTotals>>,
}

impl PacingJournal {
    /// Start a journal that writes to the new file `path`.
    pub fn create(path: &Path, late_threshold_us: u64) -> io::Result<Self> {
        let file = std::fs::File::create(path)?;
        Self::start(io::BufWriter::new(file), late_threshold_us, QUEUE_EPOCHS)
    }

    /// Start a journal that writes to `sink` through a queue of
    /// `queue_epochs` places.
    pub fn start<W: Write + Send + 'static>(
        sink: W,
        late_threshold_us: u64,
        queue_epochs: usize,
    ) -> io::Result<Self> {
        let (queue, input) = sync_channel(queue_epochs.max(1));
        let dropped = Arc::new(AtomicU64::new(0));
        let pacer_tid = Arc::new(AtomicI32::new(0));
        let writer = {
            let dropped = Arc::clone(&dropped);
            let pacer_tid = Arc::clone(&pacer_tid);
            std::thread::Builder::new()
                .name("aep1-journal".into())
                .spawn(move || run_writer(sink, input, late_threshold_us, &dropped, &pacer_tid))?
        };
        Ok(Self {
            recorder: PacingRecorder {
                queue,
                dropped,
                pacer_tid,
            },
            writer,
        })
    }

    /// The pacing thread's end.
    pub fn recorder(&self) -> PacingRecorder {
        self.recorder.clone()
    }

    /// Write what is queued, close the last second and stop. Every
    /// [`PacingRecorder`] must be gone first: the journal ends when the
    /// last one is.
    pub fn finish(self) -> io::Result<JournalTotals> {
        let Self { recorder, writer } = self;
        drop(recorder);
        writer
            .join()
            .map_err(|_| io::Error::other("the lateness journal's thread panicked"))?
    }
}

fn run_writer<W: Write>(
    mut sink: W,
    input: Receiver<EpochTiming>,
    late_threshold_us: u64,
    dropped: &AtomicU64,
    pacer_tid: &AtomicI32,
) -> io::Result<JournalTotals> {
    let mut lines = JournalLines::new(late_threshold_us);
    let mut pacer = PacerCounters::default();
    let counters = |pacer: &mut PacerCounters| {
        pacer.growth(pacer_tid.load(Ordering::Acquire), read_thread_counters)
    };
    loop {
        let before = lines.totals.late_lines + lines.totals.sec_lines;
        let mut open = true;
        loop {
            match input.try_recv() {
                Ok(timing) => lines.push(
                    timing,
                    &mut || counters(&mut pacer),
                    dropped.load(Ordering::Relaxed),
                    &mut sink,
                )?,
                Err(TryRecvError::Empty) => break,
                Err(TryRecvError::Disconnected) => {
                    open = false;
                    break;
                }
            }
        }
        if !open {
            let dropped = dropped.load(Ordering::Relaxed);
            lines.finish(&mut || counters(&mut pacer), dropped, &mut sink)?;
            sink.flush()?;
            return Ok(lines.totals);
        }
        if lines.totals.late_lines + lines.totals.sec_lines != before {
            sink.flush()?;
        }
        std::thread::sleep(POLL);
    }
}

#[derive(Serialize)]
struct LateLine {
    kind: &'static str,
    epoch: u64,
    deadline_us: u64,
    woke_late_us: u64,
    encode_us: u64,
    late_first_us: u64,
    late_last_us: u64,
}

#[derive(Serialize)]
struct SecLine {
    kind: &'static str,
    unix_s: u64,
    epochs: u64,
    late_p50_us: u64,
    late_p99_us: u64,
    late_max_us: u64,
    woke_late_max_us: u64,
    encode_p50_us: u64,
    encode_max_us: u64,
    over_1ms: u64,
    over_5ms: u64,
    minflt: u64,
    majflt: u64,
    nivcsw: u64,
    run_delay_us: u64,
    dropped: u64,
}

/// The pacing thread's counters, as `/proc` gives them.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ThreadCounters {
    pub minflt: u64,
    pub majflt: u64,
    /// Involuntary context switches.
    pub nivcsw: u64,
    /// Time runnable and waiting for a CPU.
    pub run_delay_us: u64,
}

/// The growth of the pacing thread's counters between `sec` lines.
#[derive(Debug, Default)]
struct PacerCounters {
    last: Option<(i32, ThreadCounters)>,
}

impl PacerCounters {
    /// The counters' growth since the previous call for the same thread:
    /// zero for the first reading of a thread, and while no thread is
    /// attached (`tid` 0) or its counters cannot be read.
    fn growth(&mut self, tid: i32, read: impl Fn(i32) -> Option<ThreadCounters>) -> ThreadCounters {
        let Some(now) = (tid != 0).then(|| read(tid)).flatten() else {
            return ThreadCounters::default();
        };
        let growth = match self.last {
            Some((last_tid, last)) if last_tid == tid => ThreadCounters {
                minflt: now.minflt.saturating_sub(last.minflt),
                majflt: now.majflt.saturating_sub(last.majflt),
                nivcsw: now.nivcsw.saturating_sub(last.nivcsw),
                run_delay_us: now.run_delay_us.saturating_sub(last.run_delay_us),
            },
            _ => ThreadCounters::default(),
        };
        self.last = Some((tid, now));
        growth
    }
}

/// The lines of a journal, made from the epochs in the order they were due.
struct JournalLines {
    late_threshold_us: u64,
    /// The Unix second being gathered.
    second: Option<u64>,
    late_us: Vec<u64>,
    encode_us: Vec<u64>,
    woke_late_max_us: u64,
    over_1ms: u64,
    over_5ms: u64,
    totals: JournalTotals,
}

impl JournalLines {
    fn new(late_threshold_us: u64) -> Self {
        Self {
            late_threshold_us,
            second: None,
            late_us: Vec::with_capacity(4_096),
            encode_us: Vec::with_capacity(4_096),
            woke_late_max_us: 0,
            over_1ms: 0,
            over_5ms: 0,
            totals: JournalTotals::default(),
        }
    }

    /// Add one epoch: its `late` line if it was late, and before it the
    /// `sec` line of the second that it ends.
    fn push(
        &mut self,
        timing: EpochTiming,
        counters: &mut dyn FnMut() -> ThreadCounters,
        dropped: u64,
        out: &mut impl Write,
    ) -> io::Result<()> {
        let second = timing.deadline_ns / 1_000_000_000;
        if self.second.is_some_and(|open| second > open) {
            self.close_second(counters(), dropped, out)?;
        }
        self.second.get_or_insert(second);
        let late_first_us = timing.first_sent_ns.saturating_sub(timing.deadline_ns) / 1_000;
        let woke_late_us = timing.woke_ns.saturating_sub(timing.deadline_ns) / 1_000;
        let encode_us = timing.encode_ns / 1_000;
        self.late_us.push(late_first_us);
        self.encode_us.push(encode_us);
        self.woke_late_max_us = self.woke_late_max_us.max(woke_late_us);
        self.over_1ms += u64::from(late_first_us >= 1_000);
        self.over_5ms += u64::from(late_first_us >= 5_000);
        self.totals.epochs += 1;
        if late_first_us >= self.late_threshold_us {
            write_line(
                out,
                &LateLine {
                    kind: "late",
                    epoch: timing.epoch,
                    deadline_us: timing.deadline_ns / 1_000,
                    woke_late_us,
                    encode_us,
                    late_first_us,
                    late_last_us: timing.last_sent_ns.saturating_sub(timing.deadline_ns) / 1_000,
                },
            )?;
            self.totals.late_lines += 1;
        }
        Ok(())
    }

    /// Close the second that is open, if one is.
    fn finish(
        &mut self,
        counters: &mut dyn FnMut() -> ThreadCounters,
        dropped: u64,
        out: &mut impl Write,
    ) -> io::Result<()> {
        self.totals.dropped = dropped;
        if self.second.is_some() {
            self.close_second(counters(), dropped, out)?;
        }
        Ok(())
    }

    fn close_second(
        &mut self,
        pacer: ThreadCounters,
        dropped: u64,
        out: &mut impl Write,
    ) -> io::Result<()> {
        let Some(unix_s) = self.second.take() else {
            return Ok(());
        };
        self.late_us.sort_unstable();
        self.encode_us.sort_unstable();
        write_line(
            out,
            &SecLine {
                kind: "sec",
                unix_s,
                epochs: self.late_us.len() as u64,
                late_p50_us: percentile(&self.late_us, 50),
                late_p99_us: percentile(&self.late_us, 99),
                late_max_us: self.late_us.last().copied().unwrap_or(0),
                woke_late_max_us: self.woke_late_max_us,
                encode_p50_us: percentile(&self.encode_us, 50),
                encode_max_us: self.encode_us.last().copied().unwrap_or(0),
                over_1ms: self.over_1ms,
                over_5ms: self.over_5ms,
                minflt: pacer.minflt,
                majflt: pacer.majflt,
                nivcsw: pacer.nivcsw,
                run_delay_us: pacer.run_delay_us,
                dropped,
            },
        )?;
        self.totals.sec_lines += 1;
        self.totals.dropped = dropped;
        self.late_us.clear();
        self.encode_us.clear();
        self.woke_late_max_us = 0;
        self.over_1ms = 0;
        self.over_5ms = 0;
        Ok(())
    }
}

/// The value at `percent` of `sorted` (nearest rank); 0 of nothing.
fn percentile(sorted: &[u64], percent: usize) -> u64 {
    if sorted.is_empty() {
        return 0;
    }
    let rank = (sorted.len() * percent).div_ceil(100).max(1);
    sorted[rank - 1]
}

fn write_line(out: &mut impl Write, line: &impl Serialize) -> io::Result<()> {
    serde_json::to_writer(&mut *out, line).map_err(io::Error::other)?;
    out.write_all(b"\n")
}

/// The calling thread's id, as `/proc/self/task` names it.
fn current_tid() -> i32 {
    #[cfg(target_os = "linux")]
    {
        // SAFETY: gettid has no preconditions.
        unsafe { libc::gettid() }
    }
    #[cfg(not(target_os = "linux"))]
    {
        0
    }
}

/// The counters of thread `tid` of this process.
fn read_thread_counters(tid: i32) -> Option<ThreadCounters> {
    let read = |file: &str| std::fs::read_to_string(format!("/proc/self/task/{tid}/{file}")).ok();
    let (minflt, majflt) = parse_stat_faults(&read("stat")?)?;
    Some(ThreadCounters {
        minflt,
        majflt,
        nivcsw: parse_status_nivcsw(&read("status")?)?,
        run_delay_us: parse_schedstat_run_delay_ns(&read("schedstat")?)? / 1_000,
    })
}

/// `minflt` and `majflt` of a `/proc/<pid>/task/<tid>/stat` line: fields 10
/// and 12, counted after the thread's name, which may hold spaces.
fn parse_stat_faults(stat: &str) -> Option<(u64, u64)> {
    let after_name = &stat[stat.rfind(')')? + 1..];
    let mut fields = after_name.split_ascii_whitespace();
    // After the name: state (3), then 4..9, then minflt (10).
    let minflt = fields.nth(7)?.parse().ok()?;
    let majflt = fields.nth(1)?.parse().ok()?;
    Some((minflt, majflt))
}

fn parse_status_nivcsw(status: &str) -> Option<u64> {
    status
        .lines()
        .find_map(|line| line.strip_prefix("nonvoluntary_ctxt_switches:"))
        .and_then(|value| value.trim().parse().ok())
}

/// The second field of `schedstat`: nanoseconds spent waiting on a run queue.
fn parse_schedstat_run_delay_ns(schedstat: &str) -> Option<u64> {
    schedstat
        .split_ascii_whitespace()
        .nth(1)
        .and_then(|value| value.parse().ok())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicBool;
    use std::sync::{Condvar, Mutex};
    use std::time::Instant;

    const SESSION_NS: u64 = 1_795_380_000_000_000_000;
    const EPOCH_NS: u64 = 1_000_000;

    /// Epoch `epoch` of a 1 ms stream, whose first datagram left `late_us`
    /// after its deadline: the pacer woke `woke_late_us` late and the encode
    /// took 60 us.
    fn epoch(epoch: u64, woke_late_us: u64, late_us: u64) -> EpochTiming {
        let deadline_ns = SESSION_NS + epoch * EPOCH_NS;
        EpochTiming {
            epoch,
            deadline_ns,
            woke_ns: deadline_ns + woke_late_us * 1_000,
            encode_ns: 60_000,
            first_sent_ns: deadline_ns + late_us * 1_000,
            last_sent_ns: deadline_ns + late_us * 1_000 + 9_000,
        }
    }

    /// The journal of `timings`: its text and its lines, parsed.
    fn journal_of(
        timings: impl IntoIterator<Item = EpochTiming>,
    ) -> (String, Vec<serde_json::Value>, JournalTotals) {
        let mut lines = JournalLines::new(DEFAULT_LATE_THRESHOLD_US);
        let mut out = Vec::new();
        let mut counters = || ThreadCounters {
            minflt: 3,
            majflt: 0,
            nivcsw: 2,
            run_delay_us: 150,
        };
        for timing in timings {
            lines.push(timing, &mut counters, 0, &mut out).unwrap();
        }
        lines.finish(&mut counters, 0, &mut out).unwrap();
        let text = String::from_utf8(out).unwrap();
        let parsed = text
            .lines()
            .map(|line| serde_json::from_str(line).unwrap())
            .collect();
        (text, parsed, lines.totals)
    }

    #[test]
    fn a_late_epoch_is_one_late_line() {
        // Three seconds of 1 ms epochs, 70 us late each, and epoch 1,500
        // held: the pacer woke 23 ms after its deadline.
        let (text, lines, totals) = journal_of((0..3_000).map(|id| {
            if id == 1_500 {
                epoch(id, 23_000, 23_070)
            } else {
                epoch(id, 5, 70)
            }
        }));
        let late = text
            .lines()
            .filter(|line| line.contains(r#""kind":"late""#))
            .collect::<Vec<_>>();
        assert_eq!(
            late,
            [format!(
                r#"{{"kind":"late","epoch":1500,"deadline_us":{},"woke_late_us":23000,"encode_us":60,"late_first_us":23070,"late_last_us":23079}}"#,
                (SESSION_NS + 1_500 * EPOCH_NS) / 1_000
            )]
        );
        // One `sec` line a second, in order, and the late line before the
        // line of its second.
        let kinds = lines
            .iter()
            .map(|line| line["kind"].as_str().unwrap())
            .collect::<Vec<_>>();
        assert_eq!(kinds, ["sec", "late", "sec", "sec"]);
        let seconds = lines
            .iter()
            .filter(|line| line["kind"] == "sec")
            .collect::<Vec<_>>();
        let unix_s = SESSION_NS / 1_000_000_000;
        for (index, second) in seconds.iter().enumerate() {
            assert_eq!(second["unix_s"], unix_s + index as u64);
            assert_eq!(second["epochs"], 1_000);
            assert_eq!(second["late_p50_us"], 70);
            assert_eq!(second["late_p99_us"], 70);
            assert_eq!(second["encode_p50_us"], 60);
            assert_eq!(second["encode_max_us"], 60);
            assert_eq!(second["minflt"], 3);
            assert_eq!(second["nivcsw"], 2);
            assert_eq!(second["run_delay_us"], 150);
            assert_eq!(second["dropped"], 0);
            let held = index == 1;
            assert_eq!(second["late_max_us"], if held { 23_070 } else { 70 });
            assert_eq!(second["woke_late_max_us"], if held { 23_000 } else { 5 });
            assert_eq!(second["over_1ms"], u64::from(held));
            assert_eq!(second["over_5ms"], u64::from(held));
        }
        assert_eq!(
            text.lines().next().unwrap(),
            format!(
                r#"{{"kind":"sec","unix_s":{unix_s},"epochs":1000,"late_p50_us":70,"late_p99_us":70,"late_max_us":70,"woke_late_max_us":5,"encode_p50_us":60,"encode_max_us":60,"over_1ms":0,"over_5ms":0,"minflt":3,"majflt":0,"nivcsw":2,"run_delay_us":150,"dropped":0}}"#
            )
        );
        assert_eq!(
            totals,
            JournalTotals {
                epochs: 3_000,
                late_lines: 1,
                sec_lines: 3,
                dropped: 0
            }
        );
    }

    #[test]
    fn a_hold_reads_as_late_lines_falling_one_epoch_a_line() {
        // The pacer is held for 6.2 ms from epoch 100's deadline: epochs
        // 100..=106 are overdue when it wakes and leave at once, 40 us
        // apart; epoch 107 is on its own deadline again.
        let woke_ns = SESSION_NS + 100 * EPOCH_NS + 6_200_000;
        let timings = (0..200).map(|id| {
            if (100..=106).contains(&id) {
                let deadline_ns = SESSION_NS + id * EPOCH_NS;
                let sent_ns = woke_ns + (id - 100) * 40_000 + 30_000;
                EpochTiming {
                    epoch: id,
                    deadline_ns,
                    woke_ns: woke_ns + (id - 100) * 40_000,
                    encode_ns: 25_000,
                    first_sent_ns: sent_ns,
                    last_sent_ns: sent_ns + 5_000,
                }
            } else {
                epoch(id, 5, 70)
            }
        });
        let (_, lines, totals) = journal_of(timings);
        let late = lines
            .iter()
            .filter(|line| line["kind"] == "late")
            .map(|line| {
                (
                    line["epoch"].as_u64().unwrap(),
                    line["late_first_us"].as_u64().unwrap(),
                )
            })
            .collect::<Vec<_>>();
        assert_eq!(
            late,
            [
                (100, 6_230),
                (101, 5_270),
                (102, 4_310),
                (103, 3_350),
                (104, 2_390),
                (105, 1_430),
            ]
        );
        // Epoch 106 left 470 us late: under the threshold, in the second's
        // figures only.
        let second = lines.iter().find(|line| line["kind"] == "sec").unwrap();
        assert_eq!(second["epochs"], 200);
        assert_eq!(second["over_1ms"], 6);
        assert_eq!(second["over_5ms"], 2);
        assert_eq!(second["late_max_us"], 6_230);
        assert_eq!(second["woke_late_max_us"], 6_200);
        assert_eq!(totals.late_lines, 6);
    }

    /// A sink whose writes wait until the gate opens.
    struct GatedSink {
        gate: Arc<(Mutex<bool>, Condvar)>,
        waiting: Arc<AtomicBool>,
        bytes: Arc<Mutex<Vec<u8>>>,
    }

    impl Write for GatedSink {
        fn write(&mut self, buffer: &[u8]) -> io::Result<usize> {
            let (open, changed) = &*self.gate;
            let mut open = open.lock().unwrap();
            while !*open {
                self.waiting.store(true, Ordering::Release);
                open = changed.wait(open).unwrap();
            }
            self.bytes.lock().unwrap().extend_from_slice(buffer);
            Ok(buffer.len())
        }

        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    #[test]
    fn the_journal_never_blocks_the_pacer() {
        let gate = Arc::new((Mutex::new(false), Condvar::new()));
        let waiting = Arc::new(AtomicBool::new(false));
        let bytes = Arc::new(Mutex::new(Vec::new()));
        let sink = GatedSink {
            gate: Arc::clone(&gate),
            waiting: Arc::clone(&waiting),
            bytes: Arc::clone(&bytes),
        };
        // Every epoch is late enough for a line, so the journal's thread
        // writes at once, and its sink does not take the write.
        let journal = PacingJournal::start(sink, 0, 64).unwrap();
        let recorder = journal.recorder();
        recorder.attach_current_thread();
        recorder.record(epoch(0, 5, 70));
        let stuck = Instant::now();
        while !waiting.load(Ordering::Acquire) {
            assert!(stuck.elapsed() < Duration::from_secs(10), "the sink was never written to");
            std::thread::sleep(Duration::from_millis(1));
        }
        // The pacer goes on at full speed while the journal is stuck: the
        // queue fills, and every further epoch is counted and left out.
        let epochs = 200_000_u64;
        let started = Instant::now();
        let mut slowest = Duration::ZERO;
        for id in 1..epochs {
            let before = Instant::now();
            recorder.record(epoch(id, 5, 70));
            slowest = slowest.max(before.elapsed());
        }
        let took = started.elapsed();
        assert!(waiting.load(Ordering::Acquire));
        assert!(!*gate.0.lock().unwrap(), "the journal was not stuck");
        let dropped = recorder.dropped();
        // 64 places and one epoch in the writer's hands.
        assert!(dropped >= epochs - 66, "{dropped} of {epochs} dropped");
        assert!(
            took < Duration::from_secs(5),
            "{epochs} records took {took:?} (slowest {slowest:?})"
        );
        // Once the sink takes writes again the journal has every epoch that
        // found a place, and says how many did not.
        drop(recorder);
        *gate.0.lock().unwrap() = true;
        gate.1.notify_all();
        let totals = journal.finish().unwrap();
        assert_eq!(totals.dropped, dropped);
        assert_eq!(totals.epochs + dropped, epochs);
        assert_eq!(totals.late_lines, totals.epochs);
        let text = String::from_utf8(bytes.lock().unwrap().clone()).unwrap();
        let lines = text
            .lines()
            .map(|line| serde_json::from_str::<serde_json::Value>(line).unwrap())
            .collect::<Vec<_>>();
        let late = lines.iter().filter(|line| line["kind"] == "late").count();
        assert_eq!(late as u64, totals.epochs);
        let last_second = lines.iter().rfind(|line| line["kind"] == "sec").unwrap();
        assert_eq!(last_second["dropped"], dropped);
    }

    #[test]
    fn the_journal_file_has_one_sec_line_a_second_and_ends_when_asked() {
        let path = std::env::temp_dir().join(format!(
            "aep1-lateness-journal-{}-{:?}.ndjson",
            std::process::id(),
            std::thread::current().id()
        ));
        let journal = PacingJournal::create(&path, DEFAULT_LATE_THRESHOLD_US).unwrap();
        let recorder = journal.recorder();
        recorder.attach_current_thread();
        for id in 0..2_500 {
            recorder.record(epoch(id, 5, if id == 2_100 { 900 } else { 70 }));
        }
        drop(recorder);
        let totals = journal.finish().unwrap();
        assert_eq!(
            totals,
            JournalTotals {
                epochs: 2_500,
                late_lines: 1,
                sec_lines: 3,
                dropped: 0
            }
        );
        let text = std::fs::read_to_string(&path).unwrap();
        std::fs::remove_file(&path).unwrap();
        let lines = text
            .lines()
            .map(|line| serde_json::from_str::<serde_json::Value>(line).unwrap())
            .collect::<Vec<_>>();
        assert_eq!(lines.len(), 4);
        assert_eq!(lines[2]["kind"], "late");
        assert_eq!(lines[2]["epoch"], 2_100);
        assert_eq!(lines[3]["epochs"], 500);
    }

    #[test]
    fn the_pacing_thread_s_counters_are_read_from_proc_and_reported_as_growth() {
        // A thread named with spaces and brackets, as the kernel prints it.
        let stat = "812 (aep1 pacer (x)) R 1 812 812 0 -1 4194368 1200 0 7 0 3 4 0 0 20 0 9 0 100";
        assert_eq!(parse_stat_faults(stat), Some((1_200, 7)));
        assert_eq!(parse_stat_faults("812 aep1-pacer R"), None);
        let status = "Name:\taep1-pacer\nvoluntary_ctxt_switches:\t90210\nnonvoluntary_ctxt_switches:\t17\n";
        assert_eq!(parse_status_nivcsw(status), Some(17));
        assert_eq!(parse_schedstat_run_delay_ns("5000000 123456 77\n"), Some(123_456));
        assert_eq!(parse_schedstat_run_delay_ns(""), None);

        // This thread's own counters are there to read.
        let own = read_thread_counters(current_tid()).expect("no counters for this thread");
        let later = read_thread_counters(current_tid()).unwrap();
        assert!(later.minflt >= own.minflt);

        let readings = std::cell::Cell::new(ThreadCounters {
            minflt: 100,
            majflt: 1,
            nivcsw: 10,
            run_delay_us: 1_000,
        });
        let mut pacer = PacerCounters::default();
        // No thread attached yet, then the first reading: nothing to report.
        assert_eq!(pacer.growth(0, |_| Some(readings.get())), ThreadCounters::default());
        assert_eq!(pacer.growth(7, |_| Some(readings.get())), ThreadCounters::default());
        readings.set(ThreadCounters {
            minflt: 104,
            majflt: 1,
            nivcsw: 13,
            run_delay_us: 1_250,
        });
        assert_eq!(
            pacer.growth(7, |_| Some(readings.get())),
            ThreadCounters {
                minflt: 4,
                majflt: 0,
                nivcsw: 3,
                run_delay_us: 250
            }
        );
        // A thread that is gone reads as nothing.
        assert_eq!(pacer.growth(7, |_| None), ThreadCounters::default());
    }

    #[test]
    fn percentiles_are_nearest_rank() {
        let values = (1..=100).collect::<Vec<u64>>();
        assert_eq!(percentile(&values, 50), 50);
        assert_eq!(percentile(&values, 99), 99);
        assert_eq!(percentile(&[7], 99), 7);
        assert_eq!(percentile(&[], 50), 0);
    }
}
