//! What a contributor says about itself at `GET /api/node`: its build, its
//! process, its readiness state, and what it hears, publishes and sends.
//!
//! The schema is `needletail.node.v1`, the one av-mesh serves, with a
//! contributor's blocks (`sources`, `streams`, `lanes`): the roll tooling
//! reads both alike.
//!
//! Three layers:
//! - [`Counters`]: what the hot paths count, in atomics. The AEP1 worker
//!   calls it for each datagram and each recovered epoch, the publish path
//!   for each part: no lock, no allocation and no log line.
//! - [`Snapshot`] and [`evaluate`]: the state machine, a pure function of
//!   one snapshot and of how long the proof of publishing has held.
//! - [`NodeStatus`]: reads the counters ten times a second, works out the
//!   last second's figures, steps the state machine and keeps the report
//!   that the handler serves.

use crate::identity::{Identity, WindowRow, WindowTable};
use arc_swap::ArcSwap;
use music_audio_session::DecodedMultichannelAudioEpoch;
use serde::Serialize;
use sha2::{Digest, Sha256};
use std::collections::{HashMap, VecDeque};
use std::ffi::OsString;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, LazyLock, Mutex, OnceLock};
use std::time::Duration;

pub const SCHEMA: &str = "needletail.node.v1";
pub const ROLE: &str = "contributor";

/// No AEP1 datagram for this long: the contributor is standing by.
pub const SOURCE_SILENT_MS: u64 = 2_000;
/// A stream with a part, or a recovered AEP1 epoch, this recent is one
/// that the proof looks at.
pub const STREAM_LIVE_MS: u64 = 2_000;
/// The oldest that the last part of such a stream may be.
pub const PART_AGE_MAX_MS: u64 = 250;
/// The share of the epochs recovered for a stream in the last second that
/// must have been published or held, in percent.
pub const PUBLISHED_PERCENT: u64 = 90;
/// The oldest loss report (RLR1) that still shows a forwarder hears a lane.
pub const LANE_REPORT_MAX_AGE_MS: u64 = 3_000;
/// How long the proof must hold without a break before `ready`.
pub const PROOF_HOLD_MS: u64 = 1_000;
/// How often [`NodeStatus::run`] samples.
pub const SAMPLE_INTERVAL: Duration = Duration::from_millis(100);

/// A stream is listed while it had a part or a source epoch this recently.
const STREAM_LISTED_MS: u64 = 30_000;
/// A source silent this long is forgotten (as the AEP1 worker forgets its
/// receiver).
const SOURCE_RETIRED_US: u64 = 30_000_000;
/// Epochs whose lateness a source keeps: over a second of the shortest
/// epoch (250 µs).
const LATE_RING: usize = 4_096;
/// An empty place of the lateness ring.
const LATE_EMPTY: u64 = u64::MAX;

/// What the publish path counts for one output stream.
#[derive(Debug, Default)]
struct StreamCounters {
    video: AtomicBool,
    opaque: AtomicBool,
    epoch: AtomicU64,
    head_part: AtomicU64,
    parts: AtomicU64,
    published_epochs: AtomicU64,
    held: AtomicU64,
    held_epochs: AtomicU64,
    recovered: AtomicU64,
    last_source_unix_us: AtomicU64,
    last_part_unix_us: AtomicU64,
    last_published_unix_us: AtomicU64,
    whole_parts: AtomicU64,
    compact_parts: AtomicU64,
    /// The windows of its published parts' envelopes (A 2.6).
    windows: WindowTable,
}

/// One part that went out on the lanes.
#[derive(Debug, Clone, Copy)]
pub struct PublishedPart {
    /// A part with video.
    pub video: bool,
    /// The key epoch it was published under.
    pub epoch: u64,
    /// Its part number.
    pub number: u64,
    /// The AEP1 epochs in an opaque part; `None` for a part that is not
    /// made of epochs as they came (fMP4).
    pub epochs: Option<u64>,
    pub now_unix_us: u64,
}

impl StreamCounters {
    fn published(&self, part: PublishedPart) {
        if part.video {
            self.video.store(true, Ordering::Relaxed);
        }
        if self.epoch.swap(part.epoch, Ordering::Relaxed) == part.epoch {
            self.head_part.fetch_max(part.number, Ordering::Relaxed);
        } else {
            self.head_part.store(part.number, Ordering::Relaxed);
        }
        self.parts.fetch_add(1, Ordering::Relaxed);
        if let Some(epochs) = part.epochs {
            self.opaque.store(true, Ordering::Relaxed);
            self.published_epochs.fetch_add(epochs, Ordering::Relaxed);
        }
        self.last_part_unix_us
            .store(part.now_unix_us, Ordering::Relaxed);
        self.last_published_unix_us
            .store(part.now_unix_us, Ordering::Relaxed);
    }

    fn held(&self, epochs: u64, now_unix_us: u64) {
        self.opaque.store(true, Ordering::Relaxed);
        self.held.fetch_add(1, Ordering::Relaxed);
        self.held_epochs.fetch_add(epochs, Ordering::Relaxed);
        self.last_part_unix_us.store(now_unix_us, Ordering::Relaxed);
    }
}

/// What the AEP1 worker counts for one source address.
#[derive(Debug)]
struct SourceCounters {
    session_id: AtomicU64,
    last_datagram_unix_us: AtomicU64,
    epochs: AtomicU64,
    /// The lateness of the last epochs, each as its arrival (the low 32
    /// bits of Unix milliseconds) above its lateness (µs, signed 32 bits).
    late: Box<[AtomicU64]>,
    late_next: AtomicUsize,
}

impl SourceCounters {
    fn new() -> Self {
        Self {
            session_id: AtomicU64::new(0),
            last_datagram_unix_us: AtomicU64::new(0),
            epochs: AtomicU64::new(0),
            late: (0..LATE_RING).map(|_| AtomicU64::new(LATE_EMPTY)).collect(),
            late_next: AtomicUsize::new(0),
        }
    }

    fn epoch_completed(&self, received_unix_us: u64, epoch: &DecodedMultichannelAudioEpoch) {
        self.session_id.store(epoch.session_id, Ordering::Relaxed);
        self.epochs.fetch_add(1, Ordering::Relaxed);
        let late_us = lateness_us(
            received_unix_us,
            epoch.session_id,
            epoch.pts_samples,
            epoch.sample_rate,
        );
        let arrival_ms = (received_unix_us / 1_000) as u32;
        let place = self.late_next.fetch_add(1, Ordering::Relaxed) % LATE_RING;
        self.late[place].store(
            u64::from(arrival_ms) << 32 | u64::from(late_us as u32),
            Ordering::Relaxed,
        );
    }

    /// The median and the largest lateness of the epochs that arrived in
    /// the second before `now_unix_ms`.
    fn late_last_second(&self, now_unix_ms: u64) -> Option<(i64, i64)> {
        let now = now_unix_ms as u32;
        let mut late = self
            .late
            .iter()
            .map(|place| place.load(Ordering::Relaxed))
            .filter(|entry| *entry != LATE_EMPTY)
            .filter(|entry| now.wrapping_sub((entry >> 32) as u32) <= 1_000)
            .map(|entry| entry as u32 as i32)
            .collect::<Vec<_>>();
        let largest = i64::from(*late.iter().max()?);
        let middle = late.len() / 2;
        let (_, median, _) = late.select_nth_unstable(middle);
        Some((i64::from(*median), largest))
    }
}

/// How long after its own instant an epoch arrived: the arrival less
/// `session_id + pts / rate`, the instant the source's timeline gives it
/// (the session id is the session's start in Unix nanoseconds). Two clocks:
/// the source's and this host's. Negative for an epoch sent ahead of its
/// time, or a source whose clock is behind.
fn lateness_us(
    received_unix_us: u64,
    session_id_ns: u64,
    pts_samples: u64,
    sample_rate: u32,
) -> i32 {
    let rate = u128::from(sample_rate.max(1));
    let instant_ns = u128::from(session_id_ns) + u128::from(pts_samples) * 1_000_000_000 / rate;
    let late = i128::from(received_unix_us) - (instant_ns / 1_000) as i128;
    late.clamp(i128::from(i32::MIN), i128::from(i32::MAX)) as i32
}

/// What the hot paths count for `GET /api/node`: per output stream and per
/// source address. Reads and counts take no lock; a new stream or source
/// writes a new index once.
#[derive(Debug, Default)]
pub struct Counters {
    streams: ArcSwap<HashMap<u64, Arc<StreamCounters>>>,
    sources: ArcSwap<HashMap<SocketAddr, Arc<SourceCounters>>>,
}

static COUNTERS: LazyLock<Arc<Counters>> = LazyLock::new(Arc::default);

/// The counters of this process, which the AEP1 worker and the publish
/// path feed.
pub fn counters() -> &'static Arc<Counters> {
    &COUNTERS
}

impl Counters {
    /// The counters of `stream_id`, made on first use. Never removed.
    fn stream(&self, stream_id: u64) -> Arc<StreamCounters> {
        if let Some(stream) = self.streams.load().get(&stream_id) {
            return Arc::clone(stream);
        }
        self.streams.rcu(|streams| {
            if streams.contains_key(&stream_id) {
                return Arc::clone(streams);
            }
            let mut streams = HashMap::clone(streams);
            streams.insert(stream_id, Arc::default());
            Arc::new(streams)
        });
        Arc::clone(
            self.streams
                .load()
                .get(&stream_id)
                .expect("a stream's counters are never removed"),
        )
    }

    fn with_stream(&self, stream_id: u64, count: impl FnOnce(&StreamCounters)) {
        if let Some(stream) = self.streams.load().get(&stream_id) {
            return count(stream);
        }
        count(&self.stream(stream_id));
    }

    /// An AEP1 epoch of `stream_id` was recovered.
    pub fn epoch_recovered(&self, stream_id: u64, now_unix_us: u64) {
        self.with_stream(stream_id, |stream| {
            stream.recovered.fetch_add(1, Ordering::Relaxed);
            stream
                .last_source_unix_us
                .store(now_unix_us, Ordering::Relaxed);
        });
    }

    /// A part of `stream_id` went out.
    pub fn part_published(&self, stream_id: u64, part: PublishedPart) {
        self.with_stream(stream_id, |stream| stream.published(part));
    }

    /// A part of `epochs` AEP1 epochs of `stream_id` was held back: a
    /// rendition that no forwarder reads.
    pub fn part_held(&self, stream_id: u64, epochs: u64, now_unix_us: u64) {
        self.with_stream(stream_id, |stream| stream.held(epochs, now_unix_us));
    }

    /// An audio part of `stream_id` left compact (RLC1) or whole (RLS1).
    pub fn part_form(&self, stream_id: u64, compact: bool) {
        self.with_stream(stream_id, |stream| {
            if compact {
                &stream.compact_parts
            } else {
                &stream.whole_parts
            }
            .fetch_add(1, Ordering::Relaxed);
        });
    }

    /// Part `number` of key epoch `epoch` of `stream_id` went out;
    /// `envelope_digest` is `xxh3_64` of its canonical object. No lock, no
    /// allocation once the stream is known.
    pub fn part_window(&self, stream_id: u64, epoch: u64, number: u64, envelope_digest: u64) {
        self.with_stream(stream_id, |stream| {
            stream.windows.published(epoch, number, envelope_digest);
        });
    }

    /// An AEP1 datagram of `peer` arrived at `received_unix_us`, and
    /// completed `epoch` if it completed one.
    pub fn source_datagram(
        &self,
        peer: SocketAddr,
        received_unix_us: u64,
        epoch: Option<&DecodedMultichannelAudioEpoch>,
    ) {
        let count = |source: &SourceCounters| {
            source
                .last_datagram_unix_us
                .store(received_unix_us, Ordering::Relaxed);
            if let Some(epoch) = epoch {
                source.epoch_completed(received_unix_us, epoch);
            }
        };
        if let Some(source) = self.sources.load().get(&peer) {
            return count(source);
        }
        let source = Arc::new(SourceCounters::new());
        count(&source);
        self.sources.rcu(|sources| {
            let mut sources = HashMap::clone(sources);
            sources.entry(peer).or_insert_with(|| Arc::clone(&source));
            sources
        });
    }

    /// Forget the sources that have been silent for [`SOURCE_RETIRED_US`].
    fn retire_silent_sources(&self, now_unix_us: u64) {
        let silent = |source: &Arc<SourceCounters>| {
            now_unix_us.saturating_sub(source.last_datagram_unix_us.load(Ordering::Relaxed))
                >= SOURCE_RETIRED_US
        };
        if !self.sources.load().values().any(silent) {
            return;
        }
        self.sources.rcu(|sources| {
            let mut sources = HashMap::clone(sources);
            sources.retain(|_, source| !silent(source));
            sources
        });
    }
}

/// What the opaque packaging counts for one stream against the source
/// contract (A 2.3), since the process started. The packaging keeps these
/// counters itself; [`NodeStatus::contract_counted`] names where to read
/// them, and the sampler reads them ten times a second.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ContractCounts {
    pub stream_id: u64,
    /// Open parts dropped because an epoch of a later part came.
    pub parts_skipped: u64,
    /// Parts published below the highest number, in their own place.
    pub late_fills: u64,
    /// Epochs not published because their number was already used.
    pub numbers_reused: u64,
    /// Epochs not published because their geometry is not their session's.
    pub geometry_changed: u64,
    /// Parts published with a source time over 2 s ahead of this host.
    pub time_ahead: u64,
}

/// One source address, as the state machine and the report see it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct SourceSample {
    pub peer: SocketAddr,
    /// The session of its last completed epoch.
    pub session_id: u64,
    pub datagram_age_ms: u64,
    pub epochs_last_second: u64,
    /// Median and largest lateness of the last second's epochs: arrival
    /// less the epoch's instant on the source's own timeline (two clocks).
    pub late_p50_us: Option<i64>,
    pub late_max_us_1s: Option<i64>,
}

/// One output stream, as the state machine sees it.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct StreamSample {
    pub stream_id: u64,
    pub video: bool,
    /// Its parts are AEP1 epochs as they came: the epochs published and
    /// held are counted against the epochs recovered.
    pub opaque: bool,
    /// Key epoch and number of the newest part that went out.
    pub epoch: Option<u64>,
    pub head_part: Option<u64>,
    /// Since an AEP1 epoch of the stream was last recovered.
    pub source_age_ms: Option<u64>,
    /// Since its last part was published or held.
    pub last_part_age_ms: Option<u64>,
    /// Since its last part went out.
    pub published_age_ms: Option<u64>,
    pub parts_last_second: u64,
    pub held_last_second: u64,
    pub published_epochs_last_second: u64,
    pub held_epochs_last_second: u64,
    pub recovered_last_second: u64,
    pub parts_skipped: u64,
    pub whole_parts: u64,
    pub compact_parts: u64,
    pub late_fills: u64,
    pub numbers_reused: u64,
    pub geometry_changed: u64,
    pub time_ahead: u64,
    /// Its last closed windows of part numbers, oldest first.
    pub windows: Vec<WindowRow>,
}

impl StreamSample {
    /// A part of it went out in the last [`STREAM_LIVE_MS`].
    pub fn publishing(&self) -> bool {
        self.published_age_ms
            .is_some_and(|age| age <= STREAM_LIVE_MS)
    }

    /// A part of it was published or held, or an AEP1 epoch of it was
    /// recovered, in the last [`STREAM_LIVE_MS`]. A stream whose source
    /// still sends stays in the proof when its parts stop; one whose
    /// source stopped sending it leaves the proof 2 s later.
    fn live(&self) -> bool {
        [self.last_part_age_ms, self.source_age_ms]
            .into_iter()
            .flatten()
            .any(|age| age <= STREAM_LIVE_MS)
    }
}

/// One RelaySession lane to a first forwarder.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct LaneSample {
    /// `primary` or `secondary`.
    pub path: &'static str,
    /// The forwarder's node id.
    pub id: String,
    pub target: SocketAddr,
    /// `healthy`, `impaired`, or `unknown` before its first part.
    pub state: &'static str,
    /// Datagrams handed to the kernel on the lane: its ledger (RLL1).
    pub sent: u64,
    /// The loss its forwarder last reported (RLR1); `null` when the report
    /// is older than [`LANE_REPORT_MAX_AGE_MS`], or there was none.
    pub loss_ppm: Option<u64>,
    pub report_age_ms: Option<u64>,
    pub backlog_parts: u64,
}

impl LaneSample {
    /// The lane sends and a forwarder says that it hears it.
    fn carries(&self) -> bool {
        self.state == "healthy"
            && self
                .report_age_ms
                .is_some_and(|age| age <= LANE_REPORT_MAX_AGE_MS)
    }
}

/// Everything the state machine reads, at one instant.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Snapshot {
    pub now_unix_ms: u64,
    /// The lanes' sockets are bound.
    pub lanes_bound: bool,
    /// A stop signal has come.
    pub stopping: bool,
    pub sources: Vec<SourceSample>,
    pub streams: Vec<StreamSample>,
    pub lanes: Vec<LaneSample>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum State {
    /// The lanes are not bound yet.
    Starting,
    /// No source is heard: nothing to publish, nothing proven.
    StandingBy,
    /// A source is heard and the proof of publishing does not hold yet.
    Proving,
    /// Publishing has been proven for [`PROOF_HOLD_MS`] without a break.
    Ready,
    /// A stop signal has come.
    Stopping,
}

/// Since when the proof of publishing has held without a break: the one
/// thing the state machine carries from a snapshot to the next.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Proof {
    since_unix_ms: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Verdict {
    pub state: State,
    /// What keeps the node from `ready`, in words; empty when ready.
    pub not_ready: Vec<String>,
    /// The ids (the forwarder's node id) of the lanes that do not carry
    /// while another does: an ingress is down. Why is in the lane's
    /// `state` and `report_age_ms`.
    pub degraded: Vec<String>,
}

impl Verdict {
    fn of(state: State, not_ready: Vec<String>) -> Self {
        Self {
            state,
            not_ready,
            degraded: Vec::new(),
        }
    }
}

/// The contributor's state at `snapshot`, given how long the proof had held
/// before it, and how long it holds now.
///
/// The proof: at least one stream has a part going out; every stream with a
/// part (published or held) or a recovered epoch in the last 2 s has its
/// last part within 250 ms
/// and, when its parts are AEP1 epochs, published or held nine tenths of the
/// epochs recovered for it in the last second; and at least one lane is
/// healthy with a loss report no older than 3 s. `ready` is the proof held
/// for a second without a break; it is current health, not a latch.
pub fn evaluate(snapshot: &Snapshot, proof: Proof) -> (Proof, Verdict) {
    let unproven = Proof::default();
    if snapshot.stopping {
        let verdict = Verdict::of(State::Stopping, vec!["stopping".to_owned()]);
        return (unproven, verdict);
    }
    if !snapshot.lanes_bound {
        let verdict = Verdict::of(State::Starting, vec!["lanes not bound".to_owned()]);
        return (unproven, verdict);
    }
    let heard = snapshot
        .sources
        .iter()
        .any(|source| source.datagram_age_ms < SOURCE_SILENT_MS);
    if !heard {
        let verdict = Verdict::of(State::StandingBy, vec!["no source heard".to_owned()]);
        return (unproven, verdict);
    }

    let mut failing = Vec::new();
    if !snapshot.streams.iter().any(StreamSample::publishing) {
        failing.push("no stream publishing".to_owned());
    }
    for stream in snapshot.streams.iter().filter(|stream| stream.live()) {
        let id = stream.stream_id;
        match stream.last_part_age_ms {
            Some(age) if age > PART_AGE_MAX_MS => {
                failing.push(format!("stream {id} last part {age} ms ago"));
            }
            Some(_) => {}
            None => failing.push(format!("stream {id} has no part")),
        }
        let done = stream.published_epochs_last_second + stream.held_epochs_last_second;
        if stream.opaque && done * 100 < stream.recovered_last_second * PUBLISHED_PERCENT {
            failing.push(format!(
                "stream {id} published {done} of {} epochs",
                stream.recovered_last_second
            ));
        }
    }
    let carrying = snapshot.lanes.iter().filter(|lane| lane.carries()).count();
    let degraded = if carrying == 0 {
        failing.push("no lane healthy with a loss report".to_owned());
        Vec::new()
    } else {
        snapshot
            .lanes
            .iter()
            .filter(|lane| !lane.carries())
            .map(|lane| lane.id.clone())
            .collect()
    };
    if !failing.is_empty() {
        let verdict = Verdict {
            state: State::Proving,
            not_ready: failing,
            degraded,
        };
        return (unproven, verdict);
    }
    let since = proof.since_unix_ms.unwrap_or(snapshot.now_unix_ms);
    let held = snapshot.now_unix_ms.saturating_sub(since);
    let (state, not_ready) = if held >= PROOF_HOLD_MS {
        (State::Ready, Vec::new())
    } else {
        (
            State::Proving,
            vec![format!("proven for {held} of {PROOF_HOLD_MS} ms")],
        )
    };
    let proof = Proof {
        since_unix_ms: Some(since),
    };
    let verdict = Verdict {
        state,
        not_ready,
        degraded,
    };
    (proof, verdict)
}

/// The `build` block: the fields of `crate::build` in a contributor's names
/// (A 2.6), which the tools read for a node of role `contributor`
/// (`mesh_ops.status_of`: `build.id`, `build.unix`, `build.dirty`). The
/// pair (`id`, `unix`) is what `--version` prints, and names the build.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Build {
    /// The short id: 12 hex of the commit, `-dirty` when so.
    pub id: String,
    /// The whole commit (40 hex).
    pub git: String,
    pub dirty: bool,
    /// When the build was made, in Unix seconds.
    pub unix: u64,
    /// The crate's version.
    #[serde(rename = "crate")]
    pub crate_version: String,
}

impl Build {
    pub fn of_this_binary() -> Self {
        Self {
            id: crate::build::GIT.to_owned(),
            git: crate::build::GIT_FULL.to_owned(),
            dirty: crate::build::DIRTY,
            unix: crate::build::UNIX,
            crate_version: crate::build::CRATE_VERSION.to_owned(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Process {
    pub pid: u32,
    /// The process's start in Unix microseconds: it changes on every
    /// restart.
    pub boot_id: u64,
    pub started_unix_ms: u64,
    pub uptime_ms: u64,
    /// [`args_sha256`] of the process's arguments.
    pub args_sha256: String,
    pub cpus: usize,
    /// CPU time of the process over the last second, in thousandths of all
    /// its CPUs.
    pub cpu_permille: u64,
}

/// One stream's row in the report.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct StreamRow {
    pub stream_id: u64,
    /// `audio` or `video`.
    pub class: &'static str,
    pub publishing: bool,
    pub epoch: Option<u64>,
    pub head_part: Option<u64>,
    pub source_age_ms: Option<u64>,
    pub last_part_age_ms: Option<u64>,
    pub parts_last_second: u64,
    pub held_last_second: u64,
    pub parts_skipped: u64,
    pub whole_parts: u64,
    pub compact_parts: u64,
    pub late_fills: u64,
    pub numbers_reused: u64,
    pub geometry_changed: u64,
    pub time_ahead: u64,
    pub windows: Vec<WindowRow>,
}

impl From<&StreamSample> for StreamRow {
    fn from(stream: &StreamSample) -> Self {
        Self {
            stream_id: stream.stream_id,
            class: if stream.video { "video" } else { "audio" },
            publishing: stream.publishing(),
            epoch: stream.epoch,
            head_part: stream.head_part,
            source_age_ms: stream.source_age_ms,
            last_part_age_ms: stream.last_part_age_ms,
            parts_last_second: stream.parts_last_second,
            held_last_second: stream.held_last_second,
            parts_skipped: stream.parts_skipped,
            whole_parts: stream.whole_parts,
            compact_parts: stream.compact_parts,
            late_fills: stream.late_fills,
            numbers_reused: stream.numbers_reused,
            geometry_changed: stream.geometry_changed,
            time_ahead: stream.time_ahead,
            windows: stream.windows.clone(),
        }
    }
}

/// The contributor's answers to its first forwarders' asks (RLQ1,
/// 4k-repair) and the part store they come from.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize)]
pub struct RelayAnswers {
    pub asks_answered: u64,
    pub answer_symbols: u64,
    /// Asks not from the lane's forwarder, dropped unanswered.
    pub answer_refused_not_admitted: u64,
    /// Asks answered `Busy`: the asker had its share.
    pub answer_refused_limited: u64,
    /// Asks answered `Missing`: the part's answer window is spent.
    pub answer_refused_window_spent: u64,
    /// Answers that panicked, caught on the answer thread.
    pub answer_panics: u64,
    pub part_store_parts: u64,
    /// What the kept parts hold, within `part_store_budget_bytes`
    /// (`--relay-part-store-mb`).
    pub part_store_bytes: u64,
    pub part_store_budget_bytes: u64,
}

/// The body of `GET /api/node`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct NodeReport {
    pub schema: &'static str,
    pub node_id: String,
    pub region: String,
    pub role: &'static str,
    pub build: Build,
    pub process: Process,
    pub state: State,
    /// `state == "ready"`.
    pub ready: bool,
    /// When the process first became ready.
    pub ready_unix_ms: Option<u64>,
    /// Always false: a contributor is never counted ready without its
    /// proof.
    pub ready_forced: bool,
    pub not_ready: Vec<String>,
    pub degraded: Vec<String>,
    /// What names its opaque audio parts (A 2.4).
    pub identity: Identity,
    pub sources: Vec<SourceSample>,
    pub streams: Vec<StreamRow>,
    pub lanes: Vec<LaneSample>,
    /// With RelaySession lanes.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub relay_answers: Option<RelayAnswers>,
}

impl NodeReport {
    pub fn new(
        node: &Node,
        process: Process,
        snapshot: Snapshot,
        verdict: Verdict,
        ready_unix_ms: Option<u64>,
    ) -> Self {
        Self {
            schema: SCHEMA,
            node_id: node.node_id.clone(),
            region: node.region.clone(),
            role: ROLE,
            build: node.build.clone(),
            process,
            state: verdict.state,
            ready: verdict.state == State::Ready,
            ready_unix_ms,
            ready_forced: false,
            not_ready: verdict.not_ready,
            degraded: verdict.degraded,
            identity: node.identity.clone(),
            streams: snapshot.streams.iter().map(StreamRow::from).collect(),
            sources: snapshot.sources,
            lanes: snapshot.lanes,
            relay_answers: None,
        }
    }
}

/// The sha256, in hex, of the process's arguments (the program's name
/// included) joined by a newline: that of the node's arguments file without
/// its last newline, which the tools compare with what they rendered.
pub fn args_sha256(args: impl IntoIterator<Item = OsString>) -> String {
    let mut hash = Sha256::new();
    for (index, argument) in args.into_iter().enumerate() {
        if index > 0 {
            hash.update(b"\n");
        }
        hash.update(argument.as_encoded_bytes());
    }
    hash.finalize()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

/// CPU time of this process so far, in microseconds.
pub fn process_cpu_us() -> u64 {
    let mut time = libc::timespec {
        tv_sec: 0,
        tv_nsec: 0,
    };
    // SAFETY: `time` is a valid timespec for the call to fill.
    if unsafe { libc::clock_gettime(libc::CLOCK_PROCESS_CPUTIME_ID, &mut time) } != 0 {
        return 0;
    }
    (time.tv_sec as u64)
        .saturating_mul(1_000_000)
        .saturating_add(time.tv_nsec as u64 / 1_000)
}

/// What a node is, for the life of its process.
#[derive(Debug, Clone)]
pub struct Node {
    pub node_id: String,
    pub region: String,
    pub build: Build,
    pub pid: u32,
    /// The process's start, in Unix microseconds.
    pub boot_unix_us: u64,
    pub args_sha256: String,
    pub cpus: usize,
    /// The identity self-test's result, taken at start.
    pub identity: Identity,
}

/// Cumulative counters read over time: what they grew by in the last
/// second.
#[derive(Debug)]
struct Window<const N: usize> {
    read: VecDeque<(u64, [u64; N])>,
}

impl<const N: usize> Default for Window<N> {
    fn default() -> Self {
        Self {
            read: VecDeque::new(),
        }
    }
}

impl<const N: usize> Window<N> {
    /// Take the reading `totals` at `now_unix_ms`; return what the counters
    /// grew by since the reading nearest to a second before (the older of
    /// two as near), and over how long. The first reading grew from nothing:
    /// the counters of a stream or a source start when it is first seen.
    fn last_second(&mut self, now_unix_ms: u64, totals: [u64; N]) -> ([u64; N], u64) {
        let from_a_second =
            |read: &(u64, [u64; N])| now_unix_ms.saturating_sub(read.0).abs_diff(1_000);
        while self.read.len() >= 2 && from_a_second(&self.read[1]) < from_a_second(&self.read[0]) {
            self.read.pop_front();
        }
        let (since, before) = self.read.front().copied().unwrap_or((now_unix_ms, [0; N]));
        self.read.push_back((now_unix_ms, totals));
        (
            std::array::from_fn(|index| totals[index].saturating_sub(before[index])),
            now_unix_ms.saturating_sub(since),
        )
    }
}

#[derive(Debug, Default)]
struct Sampler {
    streams: HashMap<u64, Window<5>>,
    sources: HashMap<SocketAddr, Window<1>>,
    cpu: Window<1>,
    proof: Proof,
    ready_unix_ms: Option<u64>,
}

/// What one sample reads beside the counters.
#[derive(Debug, Default)]
struct Reading {
    now_unix_us: u64,
    /// CPU time of the process so far.
    cpu_us: u64,
    stopping: bool,
    /// The lanes; `None` before they are bound.
    lanes: Option<Vec<LaneSample>>,
    /// The packaging's contract counts, by stream.
    contract: Vec<ContractCounts>,
}

impl Sampler {
    /// Read `counters` with `reading`, step the state machine and write the
    /// report.
    fn report(&mut self, node: &Node, counters: &Counters, reading: Reading) -> NodeReport {
        let Reading {
            now_unix_us,
            cpu_us,
            stopping,
            lanes,
            contract,
        } = reading;
        let now_unix_ms = now_unix_us / 1_000;
        let age_ms =
            |unix_us: u64| (unix_us != 0).then(|| now_unix_us.saturating_sub(unix_us) / 1_000);

        counters.retire_silent_sources(now_unix_us);
        let heard = counters.sources.load_full();
        self.sources.retain(|peer, _| heard.contains_key(peer));
        let mut sources = Vec::with_capacity(heard.len());
        for (peer, source) in heard.iter() {
            let ([epochs_last_second], _) = self
                .sources
                .entry(*peer)
                .or_default()
                .last_second(now_unix_ms, [source.epochs.load(Ordering::Relaxed)]);
            let late = source.late_last_second(now_unix_ms);
            sources.push(SourceSample {
                peer: *peer,
                session_id: source.session_id.load(Ordering::Relaxed),
                datagram_age_ms: now_unix_us
                    .saturating_sub(source.last_datagram_unix_us.load(Ordering::Relaxed))
                    / 1_000,
                epochs_last_second,
                late_p50_us: late.map(|(median, _)| median),
                late_max_us_1s: late.map(|(_, largest)| largest),
            });
        }
        sources.sort_by_key(|source| source.peer);

        let counted = counters.streams.load_full();
        let mut streams = Vec::with_capacity(counted.len());
        for (stream_id, stream) in counted.iter() {
            let load = |counter: &AtomicU64| counter.load(Ordering::Relaxed);
            let ([parts, published_epochs, held, held_epochs, recovered], _) =
                self.streams.entry(*stream_id).or_default().last_second(
                    now_unix_ms,
                    [
                        load(&stream.parts),
                        load(&stream.published_epochs),
                        load(&stream.held),
                        load(&stream.held_epochs),
                        load(&stream.recovered),
                    ],
                );
            let source_age_ms = age_ms(load(&stream.last_source_unix_us));
            let last_part_age_ms = age_ms(load(&stream.last_part_unix_us));
            let listed = [source_age_ms, last_part_age_ms]
                .into_iter()
                .flatten()
                .any(|age| age <= STREAM_LISTED_MS);
            if !listed {
                continue;
            }
            let published_age_ms = age_ms(load(&stream.last_published_unix_us));
            let contract = contract
                .iter()
                .find(|counts| counts.stream_id == *stream_id)
                .copied()
                .unwrap_or_default();
            streams.push(StreamSample {
                stream_id: *stream_id,
                video: stream.video.load(Ordering::Relaxed),
                opaque: stream.opaque.load(Ordering::Relaxed),
                epoch: published_age_ms.map(|_| load(&stream.epoch)),
                head_part: published_age_ms.map(|_| load(&stream.head_part)),
                source_age_ms,
                last_part_age_ms,
                published_age_ms,
                parts_last_second: parts,
                held_last_second: held,
                published_epochs_last_second: published_epochs,
                held_epochs_last_second: held_epochs,
                recovered_last_second: recovered,
                parts_skipped: contract.parts_skipped,
                whole_parts: load(&stream.whole_parts),
                compact_parts: load(&stream.compact_parts),
                late_fills: contract.late_fills,
                numbers_reused: contract.numbers_reused,
                geometry_changed: contract.geometry_changed,
                time_ahead: contract.time_ahead,
                windows: stream.windows.closed(),
            });
        }
        streams.sort_by_key(|stream| stream.stream_id);

        let snapshot = Snapshot {
            now_unix_ms,
            lanes_bound: lanes.is_some(),
            stopping,
            sources,
            streams,
            lanes: lanes.unwrap_or_default(),
        };
        let (proof, verdict) = evaluate(&snapshot, self.proof);
        self.proof = proof;
        if verdict.state == State::Ready && self.ready_unix_ms.is_none() {
            self.ready_unix_ms = Some(now_unix_ms);
        }
        let ([cpu_used_us], over_ms) = self.cpu.last_second(now_unix_ms, [cpu_us]);
        let process = Process {
            pid: node.pid,
            boot_id: node.boot_unix_us,
            started_unix_ms: node.boot_unix_us / 1_000,
            uptime_ms: now_unix_us.saturating_sub(node.boot_unix_us) / 1_000,
            args_sha256: node.args_sha256.clone(),
            cpus: node.cpus,
            cpu_permille: cpu_used_us
                .checked_div(over_ms.saturating_mul(node.cpus.max(1) as u64))
                .unwrap_or(0),
        };
        NodeReport::new(node, process, snapshot, verdict, self.ready_unix_ms)
    }
}

type LaneReader = Box<dyn Fn(u64) -> Vec<LaneSample> + Send + Sync>;
type ContractReader = Box<dyn Fn() -> Vec<ContractCounts> + Send + Sync>;
type AnswersReader = Box<dyn Fn() -> RelayAnswers + Send + Sync>;

/// The node's status: samples the counters and the lanes, steps the state
/// machine and keeps the report that `GET /api/node` serves.
pub struct NodeStatus {
    node: Node,
    counters: Arc<Counters>,
    /// Reads the lanes at a Unix time in microseconds; set once they are
    /// bound.
    lanes: OnceLock<LaneReader>,
    /// Reads the packaging's contract counts; set when it starts.
    contract: OnceLock<ContractReader>,
    /// Reads the relay's answers; set with RelaySession lanes.
    answers: OnceLock<AnswersReader>,
    stopping: AtomicBool,
    sampler: Mutex<Sampler>,
    report: ArcSwap<NodeReport>,
}

impl NodeStatus {
    /// The status of a node whose lanes are not bound yet: `starting`.
    pub fn new(node: Node, counters: Arc<Counters>) -> Arc<Self> {
        let mut sampler = Sampler::default();
        let reading = Reading {
            now_unix_us: node.boot_unix_us,
            ..Reading::default()
        };
        let report = sampler.report(&node, &counters, reading);
        Arc::new(Self {
            node,
            counters,
            lanes: OnceLock::new(),
            contract: OnceLock::new(),
            answers: OnceLock::new(),
            stopping: AtomicBool::new(false),
            sampler: Mutex::new(sampler),
            report: ArcSwap::from_pointee(report),
        })
    }

    /// The lanes are bound; `read` gives them at a Unix time in
    /// microseconds.
    pub fn lanes_bound(&self, read: impl Fn(u64) -> Vec<LaneSample> + Send + Sync + 'static) {
        let _ = self.lanes.set(Box::new(read));
    }

    /// The opaque packaging has started; `read` gives its contract counts
    /// by stream.
    pub fn contract_counted(&self, read: impl Fn() -> Vec<ContractCounts> + Send + Sync + 'static) {
        let _ = self.contract.set(Box::new(read));
    }

    /// The relay answers its forwarders' asks; `read` gives its counts.
    pub fn answers_bound(&self, read: impl Fn() -> RelayAnswers + Send + Sync + 'static) {
        let _ = self.answers.set(Box::new(read));
    }

    /// A stop signal has come: the report says `stopping` from now.
    pub fn stop(&self, now_unix_us: u64) {
        self.stopping.store(true, Ordering::Release);
        self.sample(now_unix_us, process_cpu_us());
    }

    /// The last report.
    pub fn report(&self) -> Arc<NodeReport> {
        self.report.load_full()
    }

    /// Sample every [`SAMPLE_INTERVAL`], for the life of the process.
    pub async fn run(self: Arc<Self>) {
        let mut tick = tokio::time::interval(SAMPLE_INTERVAL);
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            tick.tick().await;
            self.sample(crate::hop_journal::now_unix_us(), process_cpu_us());
        }
    }

    /// Read the counters and the lanes at `now_unix_us`, with the process
    /// at `cpu_us` of CPU time, and publish the report.
    pub fn sample(&self, now_unix_us: u64, cpu_us: u64) {
        let mut sampler = self
            .sampler
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let reading = Reading {
            now_unix_us,
            cpu_us,
            stopping: self.stopping.load(Ordering::Acquire),
            lanes: self.lanes.get().map(|read| read(now_unix_us)),
            contract: self.contract.get().map(|read| read()).unwrap_or_default(),
        };
        let mut report = sampler.report(&self.node, &self.counters, reading);
        report.relay_answers = self.answers.get().map(|read| read());
        self.report.store(Arc::new(report));
    }

    /// The node's lines of `GET /metrics`.
    pub fn prometheus_metrics(&self) -> String {
        let report = self.report();
        format!(
            "# HELP av_contrib_build_info The build of this process: the commit and the build time.\n\
             # TYPE av_contrib_build_info gauge\n\
             av_contrib_build_info{{id=\"{}\",unix=\"{}\"}} 1\n\
             # HELP av_contrib_ready Whether publishing is proven (the state of GET /api/node is ready).\n\
             # TYPE av_contrib_ready gauge\n\
             av_contrib_ready {}\n\
             # HELP av_contrib_identity_info The fingerprint of what names this process's opaque audio parts.\n\
             # TYPE av_contrib_identity_info gauge\n\
             av_contrib_identity_info{{fingerprint=\"{}\"}} 1\n",
            report.build.id,
            report.build.unix,
            u8::from(report.ready),
            report.identity.fingerprint,
        )
    }
}

impl std::fmt::Debug for NodeStatus {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("NodeStatus")
            .field("node", &self.node)
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A Unix time in milliseconds for the snapshots.
    const NOW_MS: u64 = 1_763_924_100_000;
    const SESSION_NS: u64 = 1_763_924_000_001_000_000;

    fn source() -> SourceSample {
        SourceSample {
            peer: "10.77.1.4:51234".parse().unwrap(),
            session_id: SESSION_NS,
            datagram_age_ms: 0,
            epochs_last_second: 1_000,
            late_p50_us: Some(40),
            late_max_us_1s: Some(310),
        }
    }

    /// A stream of 1 ms opaque parts that publishes every epoch.
    fn audio(stream_id: u64) -> StreamSample {
        StreamSample {
            stream_id,
            opaque: true,
            epoch: Some(SESSION_NS / 1_000),
            head_part: Some(5_123_456),
            source_age_ms: Some(0),
            last_part_age_ms: Some(1),
            published_age_ms: Some(1),
            parts_last_second: 1_000,
            published_epochs_last_second: 1_000,
            recovered_last_second: 1_000,
            whole_parts: 10,
            compact_parts: 990,
            ..StreamSample::default()
        }
    }

    fn lane(path: &'static str, id: &str, target: &str) -> LaneSample {
        LaneSample {
            path,
            id: id.to_owned(),
            target: target.parse().unwrap(),
            state: "healthy",
            sent: 0,
            loss_ppm: Some(0),
            report_age_ms: Some(400),
            backlog_parts: 0,
        }
    }

    /// A contributor that hears a source, publishes its stream and has both
    /// lanes carrying.
    fn publishing() -> Snapshot {
        Snapshot {
            now_unix_ms: NOW_MS,
            lanes_bound: true,
            stopping: false,
            sources: vec![source()],
            streams: vec![audio(3_001)],
            lanes: vec![
                lane("primary", "lon-dist-a", "10.77.1.11:22001"),
                lane("secondary", "lon-dist-b", "10.77.1.12:22002"),
            ],
        }
    }

    /// The verdict on `snapshot` when the same had been seen `held_ms`
    /// before it.
    fn after(held_ms: u64, snapshot: &Snapshot) -> Verdict {
        let earlier = Snapshot {
            now_unix_ms: snapshot.now_unix_ms - held_ms,
            ..snapshot.clone()
        };
        let (proof, _) = evaluate(&earlier, Proof::default());
        evaluate(snapshot, proof).1
    }

    fn reasons(verdict: &Verdict) -> Vec<&str> {
        verdict.not_ready.iter().map(String::as_str).collect()
    }

    #[test]
    fn a_node_is_starting_until_its_lanes_are_bound() {
        let snapshot = Snapshot {
            lanes_bound: false,
            ..publishing()
        };
        let verdict = after(5_000, &snapshot);
        assert_eq!(verdict.state, State::Starting);
        assert_eq!(reasons(&verdict), ["lanes not bound"]);
    }

    #[test]
    fn a_silent_standby_is_not_ready() {
        // It never heard a source: both lanes are bound and nothing fails.
        let never = Snapshot {
            sources: Vec::new(),
            streams: Vec::new(),
            ..publishing()
        };
        let verdict = after(60_000, &never);
        assert_eq!(verdict.state, State::StandingBy);
        assert_eq!(reasons(&verdict), ["no source heard"]);
        assert!(verdict.degraded.is_empty());

        // Its source went silent: 2,000 ms without a datagram.
        let mut silent = publishing();
        silent.sources[0].datagram_age_ms = SOURCE_SILENT_MS - 1;
        assert_eq!(after(5_000, &silent).state, State::Ready);
        silent.sources[0].datagram_age_ms = SOURCE_SILENT_MS;
        let verdict = after(5_000, &silent);
        assert_eq!(verdict.state, State::StandingBy);
        assert_eq!(reasons(&verdict), ["no source heard"]);
    }

    #[test]
    fn publishing_is_proven_for_a_second_before_ready() {
        let snapshot = publishing();
        let (proof, first) = evaluate(&snapshot, Proof::default());
        assert_eq!(first.state, State::Proving);
        assert_eq!(reasons(&first), ["proven for 0 of 1000 ms"]);
        let later = |ms: u64| Snapshot {
            now_unix_ms: NOW_MS + ms,
            ..snapshot.clone()
        };
        let (proof, almost) = evaluate(&later(999), proof);
        assert_eq!(almost.state, State::Proving);
        assert_eq!(reasons(&almost), ["proven for 999 of 1000 ms"]);
        let (_, ready) = evaluate(&later(1_000), proof);
        assert_eq!(ready.state, State::Ready);
        assert!(ready.not_ready.is_empty() && ready.degraded.is_empty());
    }

    #[test]
    fn a_break_in_the_proof_starts_the_second_again() {
        let good = publishing();
        let (proof, _) = evaluate(&good, Proof::default());
        // 600 ms in, the stream's last part is 300 ms old.
        let mut stalled = good.clone();
        stalled.now_unix_ms = NOW_MS + 600;
        stalled.streams[0].last_part_age_ms = Some(300);
        let (proof, verdict) = evaluate(&stalled, proof);
        assert_eq!(verdict.state, State::Proving);
        assert_eq!(reasons(&verdict), ["stream 3001 last part 300 ms ago"]);
        assert_eq!(proof, Proof::default());
        // It publishes again at 700 ms: ready a second after that, not at 1,000.
        let at = |ms: u64| Snapshot {
            now_unix_ms: NOW_MS + ms,
            ..good.clone()
        };
        let (proof, _) = evaluate(&at(700), proof);
        let (proof, verdict) = evaluate(&at(1_600), proof);
        assert_eq!(verdict.state, State::Proving);
        assert_eq!(evaluate(&at(1_700), proof).1.state, State::Ready);
    }

    #[test]
    fn ready_is_current_health_and_not_a_latch() {
        let mut snapshot = publishing();
        assert_eq!(after(5_000, &snapshot).state, State::Ready);
        snapshot.streams[0].published_epochs_last_second = 400;
        let verdict = after(5_000, &snapshot);
        assert_eq!(verdict.state, State::Proving);
        assert_eq!(
            reasons(&verdict),
            ["stream 3001 published 400 of 1000 epochs"]
        );
    }

    #[test]
    fn one_lane_down_is_ready_and_degraded() {
        // The secondary's ingress is down: its sends are refused.
        let mut snapshot = publishing();
        snapshot.lanes[1].state = "impaired";
        let verdict = after(5_000, &snapshot);
        assert_eq!(verdict.state, State::Ready);
        assert_eq!(verdict.degraded, ["lon-dist-b"]);

        // The primary's forwarder stopped reporting: nothing says it hears.
        let mut snapshot = publishing();
        snapshot.lanes[0].report_age_ms = Some(LANE_REPORT_MAX_AGE_MS + 1);
        snapshot.lanes[0].loss_ppm = None;
        let verdict = after(5_000, &snapshot);
        assert_eq!(verdict.state, State::Ready);
        assert_eq!(verdict.degraded, ["lon-dist-a"]);

        // A report of exactly 3,000 ms still counts.
        let mut snapshot = publishing();
        snapshot.lanes[0].report_age_ms = Some(LANE_REPORT_MAX_AGE_MS);
        assert!(after(5_000, &snapshot).degraded.is_empty());
    }

    #[test]
    fn a_node_with_no_lane_that_carries_is_not_ready() {
        let mut snapshot = publishing();
        snapshot.lanes[0].state = "impaired";
        snapshot.lanes[1].report_age_ms = None;
        let verdict = after(5_000, &snapshot);
        assert_eq!(verdict.state, State::Proving);
        assert_eq!(reasons(&verdict), ["no lane healthy with a loss report"]);
        assert!(verdict.degraded.is_empty(), "no lane carries beside them");

        // A lane that has sent nothing yet is not healthy.
        let mut snapshot = publishing();
        snapshot.lanes.truncate(1);
        snapshot.lanes[0].state = "unknown";
        assert_eq!(after(5_000, &snapshot).state, State::Proving);

        // A contributor without relay lanes proves nothing.
        let mut snapshot = publishing();
        snapshot.lanes.clear();
        assert_eq!(after(5_000, &snapshot).state, State::Proving);
    }

    #[test]
    fn a_held_rendition_counts_as_published() {
        // FLAC 2001 has no reader: every epoch recovered is a part held
        // back. Opus 3001 goes out.
        let held = StreamSample {
            stream_id: 2_001,
            opaque: true,
            source_age_ms: Some(0),
            last_part_age_ms: Some(1),
            held_last_second: 1_000,
            held_epochs_last_second: 1_000,
            recovered_last_second: 1_000,
            ..StreamSample::default()
        };
        let mut snapshot = publishing();
        snapshot.streams.insert(0, held.clone());
        let verdict = after(5_000, &snapshot);
        assert_eq!(verdict.state, State::Ready, "{verdict:?}");
        assert!(!held.publishing());

        // Neither published nor held, it is a stream that does not publish.
        snapshot.streams[0].held_epochs_last_second = 100;
        let verdict = after(5_000, &snapshot);
        assert_eq!(verdict.state, State::Proving);
        assert_eq!(
            reasons(&verdict),
            ["stream 2001 published 100 of 1000 epochs"]
        );

        // Held renditions alone prove nothing: no part goes out.
        let mut snapshot = publishing();
        snapshot.streams = vec![held];
        let verdict = after(5_000, &snapshot);
        assert_eq!(verdict.state, State::Proving);
        assert_eq!(reasons(&verdict), ["no stream publishing"]);
    }

    #[test]
    fn a_stream_must_publish_nine_tenths_of_its_epochs_within_250_ms() {
        let mut snapshot = publishing();
        snapshot.streams[0].published_epochs_last_second = 900;
        assert_eq!(after(5_000, &snapshot).state, State::Ready);
        snapshot.streams[0].published_epochs_last_second = 899;
        let verdict = after(5_000, &snapshot);
        assert_eq!(
            reasons(&verdict),
            ["stream 3001 published 899 of 1000 epochs"]
        );

        // A part of five epochs: 200 parts are 1,000 epochs.
        let mut snapshot = publishing();
        snapshot.streams[0].parts_last_second = 200;
        assert_eq!(after(5_000, &snapshot).state, State::Ready);

        let mut snapshot = publishing();
        snapshot.streams[0].last_part_age_ms = Some(PART_AGE_MAX_MS);
        assert_eq!(after(5_000, &snapshot).state, State::Ready);
        snapshot.streams[0].last_part_age_ms = Some(PART_AGE_MAX_MS + 1);
        let verdict = after(5_000, &snapshot);
        assert_eq!(reasons(&verdict), ["stream 3001 last part 251 ms ago"]);

        // A stream whose last part and last source epoch are over 2 s old
        // has ended: it is not looked at, and another stream's parts keep
        // the node ready.
        let mut ended = audio(4_001);
        ended.last_part_age_ms = Some(STREAM_LIVE_MS + 1);
        ended.published_age_ms = Some(STREAM_LIVE_MS + 1);
        ended.source_age_ms = Some(STREAM_LIVE_MS + 1);
        ended.published_epochs_last_second = 0;
        ended.recovered_last_second = 0;
        let mut snapshot = publishing();
        snapshot.streams.push(ended);
        assert_eq!(after(5_000, &snapshot).state, State::Ready);
    }

    #[test]
    fn a_stream_whose_source_still_sends_stays_in_the_proof() {
        // FLAC's rendition worker wedged: the source still sends 4001, and
        // nothing of it has gone out for 2.5 s. Opus 3001 publishes.
        let mut wedged = audio(4_001);
        wedged.last_part_age_ms = Some(2_500);
        wedged.published_age_ms = Some(2_500);
        wedged.published_epochs_last_second = 0;
        let mut snapshot = publishing();
        snapshot.streams.push(wedged);
        let verdict = after(5_000, &snapshot);
        assert_eq!(verdict.state, State::Proving);
        assert_eq!(
            reasons(&verdict),
            [
                "stream 4001 last part 2500 ms ago",
                "stream 4001 published 0 of 1000 epochs"
            ]
        );

        // Recovered and never published at all.
        snapshot.streams[1].last_part_age_ms = None;
        snapshot.streams[1].published_age_ms = None;
        let verdict = after(5_000, &snapshot);
        assert_eq!(
            reasons(&verdict),
            [
                "stream 4001 has no part",
                "stream 4001 published 0 of 1000 epochs"
            ]
        );

        // The source stopped sending it 2 s ago: it has left the proof.
        snapshot.streams[1].source_age_ms = Some(STREAM_LIVE_MS + 1);
        snapshot.streams[1].recovered_last_second = 0;
        assert_eq!(after(5_000, &snapshot).state, State::Ready);
    }

    #[test]
    fn a_video_stream_is_not_counted_in_epochs() {
        let video = StreamSample {
            stream_id: 1,
            video: true,
            epoch: Some(1_763_924_050_000_000),
            head_part: Some(1_500),
            last_part_age_ms: Some(16),
            published_age_ms: Some(16),
            parts_last_second: 60,
            ..StreamSample::default()
        };
        let mut snapshot = publishing();
        snapshot.streams.insert(0, video);
        assert_eq!(after(5_000, &snapshot).state, State::Ready);
        // Its stall keeps the node from ready like an audio stream's.
        snapshot.streams[0].last_part_age_ms = Some(900);
        let verdict = after(5_000, &snapshot);
        assert_eq!(reasons(&verdict), ["stream 1 last part 900 ms ago"]);
    }

    #[test]
    fn a_signal_makes_it_stopping_whatever_else_holds() {
        let snapshot = Snapshot {
            stopping: true,
            ..publishing()
        };
        let verdict = after(5_000, &snapshot);
        assert_eq!(verdict.state, State::Stopping);
        assert_eq!(reasons(&verdict), ["stopping"]);
    }

    fn node() -> Node {
        Node {
            node_id: "contrib-lon-b".to_owned(),
            region: "london".to_owned(),
            build: Build {
                id: "3f2a9c1d77e0".to_owned(),
                git: "3f2a9c1d77e0b4c1d2e3f4a5b6c7d8e9f0a1b2c3".to_owned(),
                dirty: false,
                unix: 1_763_900_000,
                crate_version: "0.1.0".to_owned(),
            },
            pid: 812,
            boot_unix_us: 1_763_924_000_123_456,
            args_sha256: "9a1f".to_owned(),
            cpus: 4,
            identity: identity(),
        }
    }

    /// A's example identity.
    fn identity() -> Identity {
        Identity {
            fingerprint: "9f2c41aa7d10e6b3".to_owned(),
            object_version: 190_131,
            numbering: "51c0000000000001".to_owned(),
            envelope: "0be2000000000002".to_owned(),
            wire: "77aa000000000003".to_owned(),
            fields: crate::identity::IdentityFields {
                fmt: 2,
                base_stream_id: 2001,
                audio_part: "epoch".to_owned(),
                segment_us: 1_000_000,
                deadline_ms: 1_000,
                clock: "source:aep1",
                clock_error_ns: 5_000_000,
                generation: 1,
                subscription: 1,
                symbol_size: 1_200,
                max_datagram_bytes: 1_400,
                compact: true,
            },
        }
    }

    /// The `identity` block of [`identity`], as JSON.
    const IDENTITY_JSON: &str = concat!(
        r#""identity":{"fingerprint":"9f2c41aa7d10e6b3","object_version":190131,"#,
        r#""numbering":"51c0000000000001","envelope":"0be2000000000002","wire":"77aa000000000003","#,
        r#""fields":{"fmt":2,"base_stream_id":2001,"audio_part":"epoch","segment_us":1000000,"#,
        r#""deadline_ms":1000,"clock":"source:aep1","clock_error_ns":5000000,"generation":1,"#,
        r#""subscription":1,"symbol_size":1200,"max_datagram_bytes":1400,"compact":true}},"#,
    );

    fn process() -> Process {
        Process {
            pid: 812,
            boot_id: 1_763_924_000_123_456,
            started_unix_ms: 1_763_924_000_123,
            uptime_ms: 86_400,
            args_sha256: "9a1f".to_owned(),
            cpus: 4,
            cpu_permille: 60,
        }
    }

    #[test]
    fn node_json_is_stable() {
        let mut snapshot = publishing();
        snapshot.lanes.truncate(1);
        snapshot.streams[0].windows = vec![WindowRow {
            first_part: 5_122_816,
            parts: 256,
            present: 256,
            xor: "5d0c9a11e2b47f03".to_owned(),
        }];
        let verdict = after(5_000, &snapshot);
        let report = NodeReport::new(
            &node(),
            process(),
            snapshot,
            verdict,
            Some(1_763_924_003_400),
        );
        let golden = [
            r#"{"schema":"needletail.node.v1","node_id":"contrib-lon-b","region":"london","role":"contributor","#,
            r#""build":{"id":"3f2a9c1d77e0","git":"3f2a9c1d77e0b4c1d2e3f4a5b6c7d8e9f0a1b2c3","dirty":false,"#,
            r#""unix":1763900000,"crate":"0.1.0"},"#,
            r#""process":{"pid":812,"boot_id":1763924000123456,"started_unix_ms":1763924000123,"uptime_ms":86400,"#,
            r#""args_sha256":"9a1f","cpus":4,"cpu_permille":60},"#,
            r#""state":"ready","ready":true,"ready_unix_ms":1763924003400,"ready_forced":false,"#,
            r#""not_ready":[],"degraded":[],"#,
            IDENTITY_JSON,
            r#""sources":[{"peer":"10.77.1.4:51234","session_id":1763924000001000000,"datagram_age_ms":0,"#,
            r#""epochs_last_second":1000,"late_p50_us":40,"late_max_us_1s":310}],"#,
            r#""streams":[{"stream_id":3001,"class":"audio","publishing":true,"epoch":1763924000001000,"#,
            r#""head_part":5123456,"source_age_ms":0,"last_part_age_ms":1,"parts_last_second":1000,"#,
            r#""held_last_second":0,"parts_skipped":0,"whole_parts":10,"compact_parts":990,"late_fills":0,"#,
            r#""numbers_reused":0,"geometry_changed":0,"time_ahead":0,"#,
            r#""windows":[{"first_part":5122816,"parts":256,"present":256,"xor":"5d0c9a11e2b47f03"}]}],"#,
            r#""lanes":[{"path":"primary","id":"lon-dist-a","target":"10.77.1.11:22001","state":"healthy","#,
            r#""sent":0,"loss_ppm":0,"report_age_ms":400,"backlog_parts":0}]}"#,
        ]
        .concat();
        assert_eq!(serde_json::to_string(&report).unwrap(), golden);
    }

    #[test]
    fn node_json_of_a_standby_with_a_held_rendition_and_a_silent_lane() {
        // What is not known is null, never a number that looks measured.
        let held = StreamSample {
            stream_id: 2_001,
            opaque: true,
            source_age_ms: Some(2_400),
            last_part_age_ms: Some(2_400),
            ..StreamSample::default()
        };
        let mut silent = lane("primary", "lon-dist-a", "10.77.1.11:22001");
        silent.state = "unknown";
        silent.loss_ppm = None;
        silent.report_age_ms = None;
        let mut heard = source();
        heard.datagram_age_ms = 2_400;
        heard.epochs_last_second = 0;
        heard.late_p50_us = None;
        heard.late_max_us_1s = None;
        let snapshot = Snapshot {
            sources: vec![heard],
            streams: vec![held],
            lanes: vec![silent],
            ..publishing()
        };
        let verdict = after(5_000, &snapshot);
        let report = NodeReport::new(&node(), process(), snapshot, verdict, None);
        let json = serde_json::to_string(&report).unwrap();
        let tail = [
            r#""state":"standing_by","ready":false,"ready_unix_ms":null,"ready_forced":false,"#,
            r#""not_ready":["no source heard"],"degraded":[],"#,
            IDENTITY_JSON,
            r#""sources":[{"peer":"10.77.1.4:51234","session_id":1763924000001000000,"datagram_age_ms":2400,"#,
            r#""epochs_last_second":0,"late_p50_us":null,"late_max_us_1s":null}],"#,
            r#""streams":[{"stream_id":2001,"class":"audio","publishing":false,"epoch":null,"#,
            r#""head_part":null,"source_age_ms":2400,"last_part_age_ms":2400,"parts_last_second":0,"#,
            r#""held_last_second":0,"parts_skipped":0,"whole_parts":0,"compact_parts":0,"late_fills":0,"#,
            r#""numbers_reused":0,"geometry_changed":0,"time_ahead":0,"windows":[]}],"#,
            r#""lanes":[{"path":"primary","id":"lon-dist-a","target":"10.77.1.11:22001","state":"unknown","#,
            r#""sent":0,"loss_ppm":null,"report_age_ms":null,"backlog_parts":0}]}"#,
        ]
        .concat();
        assert!(json.ends_with(&tail), "{json}");
    }

    #[test]
    fn the_arguments_hash_is_that_of_the_arguments_file() {
        // sha256 of the three lines without the last newline.
        let arguments = [
            "/opt/needletail/builds/3f2a9c1d77e0-1763900000/av-contrib",
            "--node-id",
            "contrib-lon-b",
        ];
        assert_eq!(
            args_sha256(arguments.map(OsString::from)),
            "c1023d92f2d3e16ea5699b227d1a80f5b7e8a8151b1039c641338fa89e68da99"
        );
        assert_eq!(
            args_sha256([]),
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
        );
    }

    fn epoch(pts_samples: u64) -> DecodedMultichannelAudioEpoch {
        DecodedMultichannelAudioEpoch {
            session_id: SESSION_NS,
            config_generation: 1,
            epoch_id: pts_samples / 48,
            pts_samples,
            sample_rate: 48_000,
            frame_count: 48,
            groups: Vec::new(),
        }
    }

    #[test]
    fn lateness_is_an_epochs_arrival_against_the_sources_own_timeline() {
        let session_us = SESSION_NS / 1_000;
        // Epoch 1,000 of 48 samples at 48 kHz belongs one second in.
        assert_eq!(
            lateness_us(session_us + 1_000_000, SESSION_NS, 48_000, 48_000),
            0
        );
        assert_eq!(
            lateness_us(session_us + 1_000_310, SESSION_NS, 48_000, 48_000),
            310
        );
        // Sent ahead of its time, or a source clock behind this host's.
        assert_eq!(
            lateness_us(session_us + 999_900, SESSION_NS, 48_000, 48_000),
            -100
        );
        // 106 hours of samples do not wrap, and a wild value is clamped.
        let pts = 48_000 * 3_600 * 200;
        let on_time = session_us + 3_600 * 200 * 1_000_000;
        assert_eq!(lateness_us(on_time + 7, SESSION_NS, pts, 48_000), 7);
        assert_eq!(lateness_us(0, SESSION_NS, 0, 48_000), i32::MIN);
        assert_eq!(lateness_us(u64::MAX / 2, 0, 0, 0), i32::MAX);
    }

    #[test]
    fn a_source_shows_its_session_its_rate_and_its_lateness() {
        let counters = Arc::new(Counters::default());
        let status = NodeStatus::new(node(), Arc::clone(&counters));
        let peer: SocketAddr = "10.77.1.4:51234".parse().unwrap();
        let start_us = SESSION_NS / 1_000;
        status.sample(start_us, 0);
        // A second of 1 ms epochs, each with a repair datagram behind it;
        // every tenth epoch arrives 300 µs late, the others 40 µs.
        for index in 0..1_000_u64 {
            let late = if index % 10 == 9 { 300 } else { 40 };
            let arrival = start_us + index * 1_000 + late;
            counters.source_datagram(peer, arrival, Some(&epoch(index * 48)));
            counters.source_datagram(peer, arrival + 20, None);
        }
        status.sample(start_us + 1_000_000, 0);
        let report = status.report();
        assert_eq!(
            report.sources,
            [SourceSample {
                peer,
                session_id: SESSION_NS,
                datagram_age_ms: 0,
                epochs_last_second: 1_000,
                late_p50_us: Some(40),
                late_max_us_1s: Some(300),
            }]
        );

        // Two seconds on with nothing more: no epoch in the last second, so
        // no lateness; the source is still listed, 2 s silent.
        status.sample(start_us + 3_000_000, 0);
        let report = status.report();
        assert_eq!(report.sources[0].datagram_age_ms, 2_000);
        assert_eq!(report.sources[0].epochs_last_second, 0);
        assert_eq!(report.sources[0].late_p50_us, None);
        assert_eq!(report.sources[0].late_max_us_1s, None);

        // After 30 s of silence it is forgotten, and a new one starts clean.
        status.sample(start_us + 31_000_000, 0);
        assert!(status.report().sources.is_empty());
        counters.source_datagram(peer, start_us + 32_000_000, None);
        status.sample(start_us + 32_000_000, 0);
        let report = status.report();
        assert_eq!(report.sources.len(), 1);
        assert_eq!(report.sources[0].session_id, 0);
        assert_eq!(report.sources[0].epochs_last_second, 0);
    }

    /// Feed `counters` a second of 1 ms epochs of `stream_id` from
    /// `from_us`, each published as part `first_part + index`.
    fn publish_a_second(counters: &Counters, stream_id: u64, from_us: u64, first_part: u64) {
        for index in 0..1_000_u64 {
            let now_unix_us = from_us + index * 1_000;
            counters.epoch_recovered(stream_id, now_unix_us);
            counters.part_published(
                stream_id,
                PublishedPart {
                    video: false,
                    epoch: SESSION_NS / 1_000,
                    number: first_part + index,
                    epochs: Some(1),
                    now_unix_us: now_unix_us + 100,
                },
            );
            counters.part_form(stream_id, index % 100 != 0);
        }
    }

    fn row(report: &NodeReport, stream_id: u64) -> StreamRow {
        report
            .streams
            .iter()
            .find(|stream| stream.stream_id == stream_id)
            .cloned()
            .unwrap_or_else(|| panic!("stream {stream_id} is not listed: {:?}", report.streams))
    }

    #[test]
    fn a_stream_shows_its_closed_windows_and_the_metrics_its_fingerprint() {
        let counters = Arc::new(Counters::default());
        let status = NodeStatus::new(node(), Arc::clone(&counters));
        let start_us = SESSION_NS / 1_000;
        publish_a_second(&counters, 3_001, start_us, 0);
        let digest = |number: u64| xxhash_rust::xxh3::xxh3_64(&number.to_le_bytes());
        // Parts 0 to 767 but 100: window 0 is closed by window 2.
        for number in (0..768).filter(|&number| number != 100) {
            counters.part_window(3_001, SESSION_NS / 1_000, number, digest(number));
        }
        status.sample(start_us + 1_000_000, 0);
        let windows = row(&status.report(), 3_001).windows;
        let xor = (0..256)
            .filter(|&number| number != 100)
            .fold(0, |xor, number| xor ^ digest(number));
        assert_eq!(
            windows,
            [WindowRow {
                first_part: 0,
                parts: 256,
                present: 255,
                xor: crate::identity::hex16(xor),
            }]
        );
        assert!(status
            .prometheus_metrics()
            .contains("av_contrib_identity_info{fingerprint=\"9f2c41aa7d10e6b3\"} 1\n"));
    }

    #[test]
    fn a_stream_shows_what_it_published_in_the_last_second() {
        let counters = Arc::new(Counters::default());
        let status = NodeStatus::new(node(), Arc::clone(&counters));
        let start_us = SESSION_NS / 1_000;
        status.sample(start_us, 0);
        assert!(status.report().streams.is_empty());

        publish_a_second(&counters, 3_001, start_us, 5_000);
        // A rendition that nobody reads: every epoch a part held back.
        for index in 0..1_000_u64 {
            counters.epoch_recovered(2_001, start_us + index * 1_000);
            counters.part_held(2_001, 1, start_us + index * 1_000 + 100);
        }
        // A 60 frames-a-second video stream under its own epoch.
        for frame in 0..60_u64 {
            counters.part_published(
                1,
                PublishedPart {
                    video: true,
                    epoch: 1_763_924_050_000_000,
                    number: frame,
                    epochs: None,
                    now_unix_us: start_us + frame * 16_667,
                },
            );
        }
        status.sample(start_us + 1_000_000, 0);
        let report = status.report();
        assert_eq!(
            row(&report, 3_001),
            StreamRow {
                stream_id: 3_001,
                class: "audio",
                publishing: true,
                epoch: Some(SESSION_NS / 1_000),
                head_part: Some(5_999),
                source_age_ms: Some(1),
                last_part_age_ms: Some(0),
                parts_last_second: 1_000,
                held_last_second: 0,
                parts_skipped: 0,
                whole_parts: 10,
                compact_parts: 990,
                late_fills: 0,
                numbers_reused: 0,
                geometry_changed: 0,
                time_ahead: 0,
                windows: Vec::new(),
            }
        );
        let held = row(&report, 2_001);
        assert!(!held.publishing);
        assert_eq!((held.epoch, held.head_part), (None, None));
        assert_eq!((held.parts_last_second, held.held_last_second), (0, 1_000));
        assert_eq!(held.last_part_age_ms, Some(0));
        let video = row(&report, 1);
        assert_eq!(video.class, "video");
        assert_eq!(video.parts_last_second, 60);
        assert_eq!(video.head_part, Some(59));
        assert_eq!(video.source_age_ms, None);

        assert_eq!(row(&report, 3_001).numbers_reused, 0, "nothing counted yet");
        // The packaging's contract counts are read where it keeps them;
        // a stream it does not count shows zeros.
        status.contract_counted(|| {
            vec![
                ContractCounts {
                    stream_id: 3_001,
                    parts_skipped: 1,
                    late_fills: 2,
                    numbers_reused: 3,
                    geometry_changed: 4,
                    time_ahead: 5,
                },
                ContractCounts {
                    stream_id: 9_999,
                    numbers_reused: 7,
                    ..ContractCounts::default()
                },
            ]
        });

        // A late fill below the head leaves the head; a new session's
        // parts start a new head.
        let mut part = PublishedPart {
            video: false,
            epoch: SESSION_NS / 1_000,
            number: 5_100,
            epochs: Some(1),
            now_unix_us: start_us + 1_400_000,
        };
        counters.part_published(3_001, part);
        status.sample(start_us + 1_500_000, 0);
        let stream = row(&status.report(), 3_001);
        assert_eq!(stream.head_part, Some(5_999));
        assert_eq!(stream.last_part_age_ms, Some(100));
        // Its first reading, at 1 s, is the nearest to a second before.
        assert_eq!(stream.parts_last_second, 1, "the parts since that reading");
        assert_eq!(
            (
                stream.parts_skipped,
                stream.late_fills,
                stream.numbers_reused,
                stream.geometry_changed,
                stream.time_ahead
            ),
            (1, 2, 3, 4, 5)
        );
        assert_eq!(row(&status.report(), 2_001).numbers_reused, 0);
        assert!(status
            .report()
            .streams
            .iter()
            .all(|row| row.stream_id != 9_999));
        part.epoch += 9_000_000;
        part.number = 7;
        counters.part_published(3_001, part);
        status.sample(start_us + 1_600_000, 0);
        let stream = row(&status.report(), 3_001);
        assert_eq!(stream.epoch, Some(SESSION_NS / 1_000 + 9_000_000));
        assert_eq!(stream.head_part, Some(7));

        // 2 s after its last part a stream no longer publishes; 30 s after,
        // it is no longer listed.
        status.sample(start_us + 3_500_000, 0);
        assert!(!row(&status.report(), 3_001).publishing);
        status.sample(start_us + 32_000_000, 0);
        assert!(status.report().streams.is_empty());
    }

    #[test]
    fn a_node_goes_from_starting_to_ready_and_keeps_when_it_first_was() {
        let counters = Arc::new(Counters::default());
        let status = NodeStatus::new(node(), Arc::clone(&counters));
        let boot_us = node().boot_unix_us;
        let report = status.report();
        assert_eq!(report.state, State::Starting);
        assert_eq!((report.ready, report.ready_unix_ms), (false, None));
        assert_eq!(report.process.boot_id, boot_us);
        assert_eq!(report.process.started_unix_ms, boot_us / 1_000);

        // The lanes are bound and nothing is heard.
        let report_age_ms = Arc::new(AtomicU64::new(400));
        let age = Arc::clone(&report_age_ms);
        status.lanes_bound(move |_| {
            let mut lane = lane("primary", "lon-dist-a", "10.77.1.11:22001");
            lane.report_age_ms = Some(age.load(Ordering::Relaxed));
            vec![lane]
        });
        status.sample(boot_us + 1_000_000, 0);
        assert_eq!(status.report().state, State::StandingBy);
        assert_eq!(status.report().process.uptime_ms, 1_000);

        // A source starts: every 100 ms the sampler sees 100 more parts.
        let peer: SocketAddr = "10.77.1.4:51234".parse().unwrap();
        let start_us = boot_us + 2_000_000;
        let mut states = Vec::new();
        for tick in 0..=12_u64 {
            let from_us = start_us + tick * 100_000;
            for index in 0..100_u64 {
                let now_unix_us = from_us + index * 1_000;
                counters.source_datagram(
                    peer,
                    now_unix_us,
                    Some(&epoch((tick * 100 + index) * 48)),
                );
                counters.epoch_recovered(3_001, now_unix_us);
                counters.part_published(
                    3_001,
                    PublishedPart {
                        video: false,
                        epoch: SESSION_NS / 1_000,
                        number: tick * 100 + index,
                        epochs: Some(1),
                        now_unix_us,
                    },
                );
            }
            // 240,000 µs of CPU a second on 4 CPUs: 60 thousandths.
            status.sample(from_us + 100_000, (tick + 1) * 24_000);
            states.push(status.report().state);
        }
        assert_eq!(states[..10], [State::Proving; 10]);
        assert_eq!(states[10..], [State::Ready; 3]);
        let ready = status.report();
        let first_ready_ms = (start_us + 1_100_000) / 1_000;
        assert_eq!(ready.ready_unix_ms, Some(first_ready_ms));
        assert_eq!(ready.process.cpu_permille, 60);
        assert_eq!(ready.streams[0].parts_last_second, 1_000);
        assert_eq!(ready.sources[0].epochs_last_second, 1_000);
        assert!(status
            .prometheus_metrics()
            .contains("av_contrib_build_info{id=\"3f2a9c1d77e0\",unix=\"1763900000\"} 1\n"));
        assert!(status.prometheus_metrics().contains("av_contrib_ready 1\n"));

        // The forwarder stops reporting: not ready, and the first time stays.
        report_age_ms.store(LANE_REPORT_MAX_AGE_MS + 1, Ordering::Relaxed);
        status.sample(start_us + 1_400_000, 13 * 24_000);
        let report = status.report();
        assert_eq!(report.state, State::Proving);
        assert_eq!(report.not_ready, ["no lane healthy with a loss report"]);
        assert_eq!(report.ready_unix_ms, Some(first_ready_ms));
        assert!(status.prometheus_metrics().contains("av_contrib_ready 0\n"));

        // A stop signal: `stopping` at once, without waiting for a sample.
        status.stop(start_us + 1_450_000);
        let report = status.report();
        assert_eq!(report.state, State::Stopping);
        assert_eq!(report.not_ready, ["stopping"]);
    }

    #[test]
    fn the_last_second_is_measured_from_the_reading_nearest_a_second_before() {
        let mut window = Window::<1>::default();
        // The first reading grew from nothing.
        assert_eq!(window.last_second(10_000, [5]), ([5], 0));
        assert_eq!(window.last_second(10_400, [45]), ([40], 400));
        assert_eq!(window.last_second(11_000, [105]), ([100], 1_000));
        // The reading at 10,400 is nearer to a second before than 10,000.
        assert_eq!(window.last_second(11_450, [150]), ([105], 1_050));
        // After a gap in the readings the one before it is all there is,
        assert_eq!(window.last_second(20_000, [150]), ([0], 8_550));
        // and the next figure is of the time since the gap, not across it.
        assert_eq!(window.last_second(20_100, [160]), ([10], 100));
        assert_eq!(window.read.len(), 2);
    }

    /// With RelaySession lanes the report carries the relay's answers and
    /// its part store; without, it has no such block.
    #[test]
    fn the_report_carries_the_relay_answers_once_bound() {
        let status = NodeStatus::new(node(), Arc::new(Counters::default()));
        status.sample(node().boot_unix_us + 1_000_000, 0);
        let json = serde_json::to_value(&*status.report()).unwrap();
        assert!(json.get("relay_answers").is_none());
        status.answers_bound(|| RelayAnswers {
            asks_answered: 3,
            answer_symbols: 40,
            part_store_parts: 12,
            part_store_bytes: 15_000_000,
            part_store_budget_bytes: 48 << 20,
            ..RelayAnswers::default()
        });
        status.sample(node().boot_unix_us + 2_000_000, 0);
        let json = serde_json::to_value(&*status.report()).unwrap();
        let answers = &json["relay_answers"];
        assert_eq!(answers["part_store_bytes"], 15_000_000);
        assert_eq!(answers["part_store_budget_bytes"], 48 << 20);
        assert_eq!(answers["answer_refused_limited"], 0);
        assert_eq!(answers["asks_answered"], 3);
    }
}
