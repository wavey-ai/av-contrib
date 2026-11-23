//! Asynchronous AEP1 audio to LL-HLS publication.
//!
//! The UDP receive loop only copies an AEP1 datagram into a bounded queue. FEC
//! recovery happens in the worker owned by this module. Opaque publication does
//! not inspect recovered group bytes. The fMP4 policy boxes supported elementary
//! audio and removes an unencrypted SoundKit header from Opus. Publication uses
//! one worker for each session, group, and audio format.
//!
//! An opaque part is named from its source alone. The epoch with sample count
//! `pts` belongs to part `pts / P` of its session (`P` samples a part), a part
//! is published only with every one of its epochs, and its number, place and
//! times are arithmetic on the AEP1 header ([`SourceAnchor`]). Nothing counts
//! what this process has seen, so a contributor that starts in the middle of
//! a session, or loses an epoch, publishes every other part exactly as one
//! that did not. Input that would put other bytes under a number already
//! published is not published and is counted ([`OpaqueStreamCounts`]).

use crate::fmp4_bridge::{
    Fmp4PartPublisher, Fmp4Segmenter, PublishedOpaquePart, TimestampInput, DEFAULT_MIN_PART_MS,
};
use crate::hop_journal::{now_unix_us, PartSourceTimes};
pub use crate::part_timing::AudioPartSize;
use crate::part_timing::{legacy_ms, part_place, part_samples, samples_to_us, SourceAnchor};
use crate::soundkit_v2::{terminalize_soundkit_group, SoundKitV2StreamRef};
use access_unit::{AccessUnit, PSI_STREAM_AUDIO_OPUS, PSI_STREAM_PRIVATE_DATA};
use boxer::fmp4::{
    opus_packet_info, AudioTrackConfig, OpusAudioConfig, PcmAudioConfig, PcmSampleKind,
};
use bytes::{Bytes, BytesMut};
use frame_header::EncodingFlag;
use music_audio_session::{
    DecodedMultichannelAudioEpoch, DecodedMultichannelAudioGroup, MultichannelAudioReceiver,
    MultichannelAudioSessionConfig,
};
use playlists::Playlists;
use raptorq_datagram_fec::{AudioPayloadKind, AudioSampleFormat};
use raptorq_fec_transport::MultichannelAudioTransportAdapter;
use std::collections::{BTreeMap, HashMap};
use std::hash::{Hash, Hasher};
use std::net::SocketAddr;
use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc,
};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::sync::{mpsc, watch};
use tokio::task::JoinHandle;
use tokio::time::MissedTickBehavior;
use tracing::{debug, info, warn};

/// Sizes below are set in time for the smallest epoch the sources make:
/// 12 samples at 48 kHz, 250 µs, 4,000 epochs a second.
const SMALLEST_EPOCH_US: u64 = 250;
/// AEP1 datagrams waiting for FEC recovery. At 4,000 epochs a second, with
/// three formats of one source and four repair datagrams each (15 datagrams
/// an epoch, 60,000 a second), 32,768 places hold 0.55 s; at 5 ms epochs
/// they hold 11 s.
pub const DEFAULT_AUDIO_EPOCH_HLS_QUEUE_CAPACITY: usize = 32_768;
/// Recovered groups that wait for one rendition worker: one second of the
/// rendition's epochs, and never fewer than this.
const RENDITION_WORKER_QUEUE_MIN_CAPACITY: usize = 1_024;
const RENDITION_WORKER_QUEUE_TIME_US: u64 = 1_000_000;
/// Epochs of one source that can wait for their missing datagrams at once:
/// 320 ms of the smallest epochs. The receiver's default of 64 epochs is
/// 16 ms of 250 µs epochs, shorter than a repair that is a little late.
const AEP1_REORDER_WINDOW_US: u64 = 320_000;
const AEP1_MAX_INFLIGHT_EPOCHS: usize = (AEP1_REORDER_WINDOW_US / SMALLEST_EPOCH_US) as usize;
const AUDIO_EPOCH_IDLE_SWEEP_INTERVAL: Duration = Duration::from_secs(5);
const AUDIO_EPOCH_IDLE_TIMEOUT: Duration = Duration::from_secs(30);
const AUDIO_GROUP_FLAG_DISCONTINUITY: u8 = 1 << 0;
const AUDIO_GROUP_FLAG_ERASURE: u8 = 1 << 1;
const OPUS_RENDITION_STREAM_OFFSET: u64 = 1_000;
const PCM_RENDITION_STREAM_OFFSET: u64 = 2_000;
/// AEP1 blocks (epochs) of one source whose first arrival is remembered for
/// the hop journal, in a table indexed by block id: 512 ms of the smallest
/// epochs, 10 s of 5 ms epochs.
const FIRST_ARRIVAL_BLOCKS: usize = 2_048;
/// Part numbers below a rendition's highest that are remembered one by one:
/// a part completed late inside them still takes its own number, and a number
/// published before is never published again. 8.2 s of 1 ms parts.
const NUMBER_WINDOW_PARTS: u64 = 8_192;
const NUMBER_WINDOW_WORDS: usize = (NUMBER_WINDOW_PARTS / 64) as usize;
/// A part whose source time is this far ahead of this host's clock is
/// counted (`time_ahead`): its session's sample count did not start at 0, or
/// followed a transport position. It is published all the same: no host's
/// clock decides what a contributor publishes.
const SOURCE_TIME_AHEAD_NS: i64 = 2_000_000_000;
static WORKER_DATAGRAMS: AtomicU64 = AtomicU64::new(0);
static WORKER_GROUPS_COMPLETED: AtomicU64 = AtomicU64::new(0);
static WORKER_RAPTORQ_FRAGMENTS_RECOVERED: AtomicU64 = AtomicU64::new(0);
static WORKER_ERRORS: AtomicU64 = AtomicU64::new(0);
static ACTIVE_RECEIVERS: AtomicU64 = AtomicU64::new(0);
static ACTIVE_RENDITION_WORKERS: AtomicU64 = AtomicU64::new(0);
static RETIRED_RECEIVERS: AtomicU64 = AtomicU64::new(0);
static RETIRED_RENDITION_WORKERS: AtomicU64 = AtomicU64::new(0);
static RENDITION_QUEUE_ENQUEUED: AtomicU64 = AtomicU64::new(0);
static RENDITION_QUEUE_DROPPED: AtomicU64 = AtomicU64::new(0);
static RENDITION_QUEUE_DEPTH: AtomicU64 = AtomicU64::new(0);
static RENDITION_QUEUE_MAX_DEPTH: AtomicU64 = AtomicU64::new(0);
static RENDITION_QUEUE_CAPACITY: AtomicU64 =
    AtomicU64::new(RENDITION_WORKER_QUEUE_MIN_CAPACITY as u64);
static RECOVERED_EPOCHS_SHARED: AtomicU64 = AtomicU64::new(0);
static RECOVERED_EPOCHS_DROPPED: AtomicU64 = AtomicU64::new(0);

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct AudioEpochHlsWorkerStats {
    pub datagrams: u64,
    pub groups_completed: u64,
    pub raptorq_fragments_recovered: u64,
    pub errors: u64,
    pub active_receivers: u64,
    pub active_rendition_workers: u64,
    pub retired_receivers: u64,
    pub retired_rendition_workers: u64,
    pub rendition_queue_capacity_per_worker: u64,
    pub rendition_queue_enqueued: u64,
    pub rendition_queue_dropped: u64,
    pub rendition_queue_depth: u64,
    pub rendition_queue_max_depth: u64,
    /// Recovered epochs handed to the recovered-epoch sink (the mesh lane's
    /// SoundKit v2 normaliser with `--aep1-shared-recovery`).
    pub recovered_epochs_shared: u64,
    /// Recovered epochs that the sink's full queue refused.
    pub recovered_epochs_dropped: u64,
}

pub fn worker_stats() -> AudioEpochHlsWorkerStats {
    AudioEpochHlsWorkerStats {
        datagrams: WORKER_DATAGRAMS.load(Ordering::Relaxed),
        groups_completed: WORKER_GROUPS_COMPLETED.load(Ordering::Relaxed),
        raptorq_fragments_recovered: WORKER_RAPTORQ_FRAGMENTS_RECOVERED.load(Ordering::Relaxed),
        errors: WORKER_ERRORS.load(Ordering::Relaxed),
        active_receivers: ACTIVE_RECEIVERS.load(Ordering::Relaxed),
        active_rendition_workers: ACTIVE_RENDITION_WORKERS.load(Ordering::Relaxed),
        retired_receivers: RETIRED_RECEIVERS.load(Ordering::Relaxed),
        retired_rendition_workers: RETIRED_RENDITION_WORKERS.load(Ordering::Relaxed),
        rendition_queue_capacity_per_worker: RENDITION_QUEUE_CAPACITY.load(Ordering::Relaxed),
        rendition_queue_enqueued: RENDITION_QUEUE_ENQUEUED.load(Ordering::Relaxed),
        rendition_queue_dropped: RENDITION_QUEUE_DROPPED.load(Ordering::Relaxed),
        rendition_queue_depth: RENDITION_QUEUE_DEPTH.load(Ordering::Relaxed),
        rendition_queue_max_depth: RENDITION_QUEUE_MAX_DEPTH.load(Ordering::Relaxed),
        recovered_epochs_shared: RECOVERED_EPOCHS_SHARED.load(Ordering::Relaxed),
        recovered_epochs_dropped: RECOVERED_EPOCHS_DROPPED.load(Ordering::Relaxed),
    }
}

/// An AEP1 epoch whose every group the worker's FEC receiver recovered,
/// handed on so that a second consumer need not decode the same datagrams.
#[derive(Debug, Clone)]
pub struct RecoveredAudioEpoch {
    pub peer: SocketAddr,
    pub epoch: DecodedMultichannelAudioEpoch,
    pub recovered_at: Instant,
}

#[derive(Debug, Clone)]
pub struct AudioEpochHlsDatagram {
    pub peer: SocketAddr,
    pub bytes: Bytes,
    /// Unix microseconds at which the ingest socket returned the datagram.
    pub received_unix_us: u64,
}

#[derive(Clone)]
pub struct AudioEpochHlsConfig {
    /// LL-HLS id used by FLAC group zero. Opus and PCM use separate id ranges.
    pub base_stream_id: u64,
    /// Size of a part: one epoch by default.
    pub part: AudioPartSize,
    pub packaging: AudioEpochHlsPackaging,
    pub formats: Vec<AudioPayloadKind>,
    pub playlists: Arc<Playlists>,
    pub publisher: Arc<dyn Fmp4PartPublisher>,
    /// Where every completed epoch goes as well, whatever its formats: the
    /// recovery is then shared with that consumer. The worker never waits
    /// for it; an epoch that does not fit is counted and dropped.
    pub recovered_epochs: Option<mpsc::Sender<RecoveredAudioEpoch>>,
    /// Runtimes that the rendition workers run on, in turn (the audio hot
    /// threads); `None`: the caller's runtime.
    pub rendition_runtimes: Option<RenditionRuntimes>,
    /// What the opaque packaging counts for each stream.
    pub opaque_streams: Arc<OpaqueStreamTable>,
}

/// Counts of the opaque packaging of one stream, since the process started.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct OpaqueStreamCounts {
    pub stream_id: u64,
    /// Parts dropped unpublished because an epoch of a later part arrived
    /// while one of theirs was still missing.
    pub parts_skipped: u64,
    /// Parts published below the stream's highest number: completed late
    /// (an epoch recovered by a late repair, a late track of the source).
    pub late_fills: u64,
    /// Epochs not published because their part number was published before
    /// or is too far below the highest to tell: the source reused a sample
    /// count within a session.
    pub numbers_reused: u64,
    /// Epochs not published because their frame count or sample rate is not
    /// that of their session's first epoch.
    pub geometry_changed: u64,
    /// Parts published whose source time was more than 2 s ahead of this
    /// host's clock.
    pub time_ahead: u64,
}

/// The counters behind [`OpaqueStreamCounts`]: one set a stream, shared by
/// the stream's sessions.
#[derive(Debug, Default)]
struct OpaqueStreamCounters {
    parts_skipped: AtomicU64,
    late_fills: AtomicU64,
    numbers_reused: AtomicU64,
    geometry_changed: AtomicU64,
    time_ahead: AtomicU64,
}

/// Add one to `counter` and return the new count.
fn count(counter: &AtomicU64) -> u64 {
    counter.fetch_add(1, Ordering::Relaxed) + 1
}

/// The opaque streams of one worker and their counters. The lock is taken
/// when a rendition starts and by a reader, never for a part.
#[derive(Debug, Default)]
pub struct OpaqueStreamTable {
    streams: std::sync::Mutex<BTreeMap<u64, Arc<OpaqueStreamCounters>>>,
}

impl OpaqueStreamTable {
    fn stream(&self, stream_id: u64) -> Arc<OpaqueStreamCounters> {
        Arc::clone(
            self.streams
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .entry(stream_id)
                .or_default(),
        )
    }

    /// The counts of every stream that has had an opaque rendition, by
    /// stream id.
    pub fn snapshot(&self) -> Vec<OpaqueStreamCounts> {
        self.streams
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .iter()
            .map(|(stream_id, counters)| OpaqueStreamCounts {
                stream_id: *stream_id,
                parts_skipped: counters.parts_skipped.load(Ordering::Relaxed),
                late_fills: counters.late_fills.load(Ordering::Relaxed),
                numbers_reused: counters.numbers_reused.load(Ordering::Relaxed),
                geometry_changed: counters.geometry_changed.load(Ordering::Relaxed),
                time_ahead: counters.time_ahead.load(Ordering::Relaxed),
            })
            .collect()
    }
}

/// The runtimes of the rendition workers: each new worker goes to the next
/// one, starting with the second, so that with one runtime per format the
/// recovery loop (on the first) and each format's encode and send run on
/// threads of their own, and an Opus part never waits for a FLAC or PCM
/// part's encode.
#[derive(Clone)]
pub struct RenditionRuntimes {
    handles: Arc<[tokio::runtime::Handle]>,
    next: Arc<std::sync::atomic::AtomicUsize>,
}

impl RenditionRuntimes {
    /// `handles` must not be empty.
    pub fn new(handles: Vec<tokio::runtime::Handle>) -> Option<Self> {
        (!handles.is_empty()).then(|| Self {
            handles: handles.into(),
            next: Arc::new(std::sync::atomic::AtomicUsize::new(1)),
        })
    }

    /// The runtime of the next rendition worker.
    pub fn next_handle(&self) -> &tokio::runtime::Handle {
        let index = self.next.fetch_add(1, Ordering::Relaxed) % self.handles.len();
        &self.handles[index]
    }

    fn spawn<F>(&self, work: F) -> JoinHandle<F::Output>
    where
        F: std::future::Future + Send + 'static,
        F::Output: Send + 'static,
    {
        self.next_handle().spawn(work)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AudioEpochHlsPackaging {
    /// Publish the recovered AEP1 payload bytes without inspecting them.
    Opaque,
    /// Convert supported elementary PCM, FLAC, or Opus payloads into CMAF/fMP4.
    Fmp4,
}

impl AudioEpochHlsConfig {
    pub fn new(
        base_stream_id: u64,
        part: AudioPartSize,
        packaging: AudioEpochHlsPackaging,
        formats: Vec<AudioPayloadKind>,
        playlists: Arc<Playlists>,
        publisher: Arc<dyn Fmp4PartPublisher>,
    ) -> Self {
        let formats = formats.into_iter().fold(Vec::new(), |mut unique, format| {
            if !unique.contains(&format) {
                unique.push(format);
            }
            unique
        });
        Self {
            base_stream_id,
            part,
            packaging,
            formats,
            playlists,
            publisher,
            recovered_epochs: None,
            rendition_runtimes: None,
            opaque_streams: Arc::default(),
        }
    }

    /// Run the rendition workers on `runtimes`, in turn.
    #[must_use]
    pub fn with_rendition_runtimes(mut self, runtimes: Option<RenditionRuntimes>) -> Self {
        self.rendition_runtimes = runtimes;
        self
    }

    /// Hand every completed epoch to `sink` too (see `recovered_epochs`).
    #[must_use]
    pub fn with_recovered_epoch_sink(mut self, sink: mpsc::Sender<RecoveredAudioEpoch>) -> Self {
        self.recovered_epochs = Some(sink);
        self
    }
}

/// Offer a completed epoch to the configured sink without waiting.
fn share_recovered_epoch(
    config: &AudioEpochHlsConfig,
    peer: SocketAddr,
    epoch: Option<DecodedMultichannelAudioEpoch>,
) {
    let (Some(sink), Some(epoch)) = (config.recovered_epochs.as_ref(), epoch) else {
        return;
    };
    match sink.try_send(RecoveredAudioEpoch {
        peer,
        epoch,
        recovered_at: Instant::now(),
    }) {
        Ok(()) => {
            RECOVERED_EPOCHS_SHARED.fetch_add(1, Ordering::Relaxed);
        }
        Err(error) => {
            let dropped = RECOVERED_EPOCHS_DROPPED.fetch_add(1, Ordering::Relaxed) + 1;
            if dropped <= 16 || dropped.is_power_of_two() {
                warn!(%peer, dropped, error = %error, "recovered AEP1 epoch sink is full");
            }
        }
    }
}

impl std::fmt::Debug for AudioEpochHlsConfig {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("AudioEpochHlsConfig")
            .field("base_stream_id", &self.base_stream_id)
            .field("part", &self.part)
            .field("packaging", &self.packaging)
            .field("formats", &self.formats)
            .finish_non_exhaustive()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct RenditionFormat {
    config_generation: u32,
    sample_rate: u32,
    channel_count: u16,
    payload_kind: AudioPayloadKind,
    sample_format: AudioSampleFormat,
}

struct RenditionState {
    /// The format of the fMP4 continuity segment, and the sample count its
    /// next group is expected at. Opaque parts are cut on the sample count
    /// itself and read neither.
    format: RenditionFormat,
    expected_pts_samples: Option<u64>,
    packager: RenditionPackager,
    /// The opaque part numbers of the session handed to the publisher.
    numbers: NumberWindow,
    /// Opus groups refused because their frame does not fit the packaging
    /// (an Opus Custom frame for fMP4, or a frame that does not divide the
    /// part). The error is returned for the 1st, 2nd, 4th, 8th... refusal.
    refused_opus_groups: u64,
}

/// Whether Opus groups of `frame_count` samples fit the packaging: fMP4
/// takes standard Opus only (Opus Custom has no OpusHead), and an opaque
/// part of a set size must hold whole frames.
fn opus_frame_fits_packaging(
    packaging: AudioEpochHlsPackaging,
    part: AudioPartSize,
    sample_rate: u32,
    frame_count: u32,
) -> Result<(), String> {
    match packaging {
        AudioEpochHlsPackaging::Fmp4 => crate::opus_custom::refuse_custom_opus(
            sample_rate,
            frame_count,
            "fMP4 packaging (--daw-hls-packaging fmp4)",
        ),
        AudioEpochHlsPackaging::Opaque => match part.target_samples(sample_rate, false) {
            Some(target) if frame_count == 0 || target % u64::from(frame_count) != 0 => {
                Err(format!(
                    "the audio part ({part}, {target} samples at {sample_rate} Hz) does not hold \
                     a whole number of {frame_count}-sample Opus frames; use --audio-part epoch \
                     (one frame per part) or a multiple of {frame_count} samples"
                ))
            }
            _ => Ok(()),
        },
    }
}

enum RenditionPackager {
    Opaque(OpaquePartSegmenter),
    Fmp4(Box<Fmp4Rendition>),
}

/// An fMP4 rendition: the segmenter, and PCM epochs gathered into an access
/// unit of whole milliseconds.
///
/// wavey-boxer writes FLAC and PCM tracks at a timescale of 1,000 and gives
/// each access unit its duration rounded to the millisecond: an access unit
/// of 12 PCM samples (0.25 ms) is 0 ms and is dropped, one of 24 is 1 ms
/// and doubles its time. Until the boxer has a timescale of the sample rate
/// (a boxer change, not made here), PCM epochs that are not whole
/// milliseconds are joined into one access unit that is: four 12-sample
/// epochs make one 1 ms unit. PCM bytes of one format join without a
/// header, so the samples are exact. A FLAC frame cannot be joined; a FLAC
/// epoch that is not whole milliseconds is published as before, rounded by
/// the boxer, with a warning.
struct Fmp4Rendition {
    segmenter: Fmp4Segmenter,
    pcm: PendingPcm,
    warned_fractional_flac: bool,
}

/// PCM epochs waiting to make an access unit of whole milliseconds.
#[derive(Default)]
struct PendingPcm {
    data: BytesMut,
    samples: u64,
    pts_samples: u64,
    sample_rate: u32,
    id: u64,
    times: PartSourceTimes,
}

/// `samples` at `sample_rate` are a whole number of milliseconds, which
/// the boxer's timescale of 1,000 carries exactly.
fn whole_ms(samples: u64, sample_rate: u32) -> bool {
    sample_rate > 0 && (u128::from(samples) * 1_000).is_multiple_of(u128::from(sample_rate))
}

/// Most PCM gathered for one access unit: 100 ms. An epoch size whose
/// samples never add up to whole milliseconds within it is published at
/// this size, rounded by the boxer.
fn max_pending_pcm_samples(sample_rate: u32) -> u64 {
    u64::from(sample_rate / 10).max(1)
}

impl Fmp4Rendition {
    fn new(segmenter: Fmp4Segmenter) -> Self {
        Self {
            segmenter,
            pcm: PendingPcm::default(),
            warned_fractional_flac: false,
        }
    }

    /// Add one PCM epoch. An epoch of whole milliseconds with nothing
    /// pending is one access unit as it is, without a copy.
    async fn push_pcm(
        &mut self,
        payload: Bytes,
        frame_count: u32,
        sample_rate: u32,
        pts_samples: u64,
        id: u64,
        times: PartSourceTimes,
    ) -> Result<(), String> {
        let samples = u64::from(frame_count);
        if self.pcm.samples > 0 && self.pcm.sample_rate != sample_rate {
            self.flush_pcm().await?;
        }
        if self.pcm.samples == 0 && whole_ms(samples, sample_rate) {
            return push_audio_unit(
                &mut self.segmenter,
                payload,
                pts_samples,
                sample_rate,
                id,
                times,
                PSI_STREAM_PRIVATE_DATA,
            )
            .await;
        }
        if self.pcm.samples == 0 {
            self.pcm.pts_samples = pts_samples;
            self.pcm.sample_rate = sample_rate;
            self.pcm.id = id;
        }
        self.pcm.data.extend_from_slice(&payload);
        self.pcm.samples = self.pcm.samples.saturating_add(samples);
        self.pcm.times.merge(&times);
        if whole_ms(self.pcm.samples, sample_rate)
            || self.pcm.samples >= max_pending_pcm_samples(sample_rate)
        {
            self.flush_pcm().await?;
        }
        Ok(())
    }

    async fn flush_pcm(&mut self) -> Result<(), String> {
        if self.pcm.samples == 0 {
            return Ok(());
        }
        let pending = std::mem::take(&mut self.pcm);
        push_audio_unit(
            &mut self.segmenter,
            pending.data.freeze(),
            pending.pts_samples,
            pending.sample_rate,
            pending.id,
            pending.times,
            PSI_STREAM_PRIVATE_DATA,
        )
        .await
    }

    async fn finish(&mut self) -> Result<(), String> {
        let flushed = self.flush_pcm().await;
        self.segmenter.finish().await;
        flushed
    }

    fn reset(&mut self) {
        self.pcm = PendingPcm::default();
        self.segmenter.reset();
    }
}

/// Push one audio access unit at `pts_samples` into the segmenter, which
/// takes millisecond timestamps.
async fn push_audio_unit(
    segmenter: &mut Fmp4Segmenter,
    data: Bytes,
    pts_samples: u64,
    sample_rate: u32,
    id: u64,
    times: PartSourceTimes,
    stream_type: u8,
) -> Result<(), String> {
    segmenter.set_next_source_times(times);
    let pts_ms = samples_to_millis(pts_samples, sample_rate)?;
    segmenter
        .push_access_unit(AccessUnit {
            key: true,
            pts: pts_ms,
            dts: pts_ms,
            data,
            stream_type,
            id,
        })
        .await;
    Ok(())
}

/// Cuts the epochs of one session into parts on absolute boundaries of the
/// session's sample count: the epoch at `pts` is in part `pts / P`.
struct OpaquePartSegmenter {
    output_stream_id: u64,
    output_stream_idx: usize,
    /// The AEP1 session id: the session's start in Unix nanoseconds.
    session_id_ns: u64,
    part: AudioPartSize,
    /// FLAC parts keep FLAC's floor of 32 samples.
    flac: bool,
    publisher: Arc<dyn Fmp4PartPublisher>,
    counters: Arc<OpaqueStreamCounters>,
    /// Set by the session's first epoch.
    geometry: Option<PartGeometry>,
    /// The one part being gathered, when a part is several epochs.
    open: Option<OpenPart>,
}

/// The epoch and part sizes of a session: those of its first epoch, which
/// every later epoch must keep.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct PartGeometry {
    frame_count: u32,
    sample_rate: u32,
    /// Samples of a part: whole epochs.
    part_samples: u64,
}

impl PartGeometry {
    fn epochs_per_part(&self) -> usize {
        usize::try_from(self.part_samples / u64::from(self.frame_count.max(1))).unwrap_or(1)
    }
}

/// A part of several epochs while some are missing.
struct OpenPart {
    number: u64,
    /// The epochs' payloads by place in the part.
    epochs: Vec<Option<Bytes>>,
    missing: usize,
    /// `pts_samples` of the epochs at the first and the last place.
    first_pts_samples: u64,
    last_pts_samples: u64,
    source_times: PartSourceTimes,
}

/// A part with every one of its epochs, ready to publish.
struct GatheredPart {
    anchor: SourceAnchor,
    bytes: Bytes,
    audio_units: usize,
    source_times: PartSourceTimes,
}

/// The part numbers of one session that were handed to the publisher (a part
/// held for want of demand counts): the highest, and which of the
/// [`NUMBER_WINDOW_PARTS`] below it.
struct NumberWindow {
    highest: Option<u64>,
    /// Bit `n % NUMBER_WINDOW_PARTS`: number `n` of the window was handed.
    handed: [u64; NUMBER_WINDOW_WORDS],
}

/// What a part number is to the numbers handed so far.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum NumberVerdict {
    /// Above every number handed so far.
    New,
    /// Inside the window and not handed: a part completed late.
    LateFill,
    /// Handed before, or below the window. Publishing it would put other
    /// bytes under a number the mesh may already hold: the first forwarders
    /// take the first arrival of a number without comparing payloads.
    Reused,
}

impl NumberWindow {
    fn new() -> Self {
        Self {
            highest: None,
            handed: [0; NUMBER_WINDOW_WORDS],
        }
    }

    fn bit(number: u64) -> (usize, u64) {
        let index = number % NUMBER_WINDOW_PARTS;
        ((index / 64) as usize, 1 << (index % 64))
    }

    fn verdict(&self, number: u64) -> NumberVerdict {
        match self.highest {
            None => NumberVerdict::New,
            Some(highest) if number > highest => NumberVerdict::New,
            Some(highest) if number == highest || highest - number > NUMBER_WINDOW_PARTS => {
                NumberVerdict::Reused
            }
            Some(_) => {
                let (word, mask) = Self::bit(number);
                if self.handed[word] & mask == 0 {
                    NumberVerdict::LateFill
                } else {
                    NumberVerdict::Reused
                }
            }
        }
    }

    /// Note that `number` was handed to the publisher.
    fn hand(&mut self, number: u64) {
        match self.highest {
            None => self.highest = Some(number),
            Some(highest) if number > highest => {
                if number - highest > NUMBER_WINDOW_PARTS {
                    // Nothing of the old window is in the new one.
                    self.handed = [0; NUMBER_WINDOW_WORDS];
                } else {
                    // The numbers passed over enter the window not handed,
                    // the old highest handed. Their places are distinct:
                    // fewer than a window of numbers.
                    for skipped in highest + 1..number {
                        let (word, mask) = Self::bit(skipped);
                        self.handed[word] &= !mask;
                    }
                    let (word, mask) = Self::bit(highest);
                    self.handed[word] |= mask;
                }
                self.highest = Some(number);
            }
            Some(highest) if number < highest && highest - number <= NUMBER_WINDOW_PARTS => {
                let (word, mask) = Self::bit(number);
                self.handed[word] |= mask;
            }
            Some(_) => {}
        }
    }
}

/// A recovered AEP1 group and when its datagrams arrived.
type RecoveredGroup = (DecodedMultichannelAudioGroup, PartSourceTimes);

struct RenditionWorker {
    sender: mpsc::Sender<RecoveredGroup>,
    handle: JoinHandle<()>,
    last_seen: Instant,
}

struct ReceiverState {
    receiver: MultichannelAudioReceiver,
    last_seen: Instant,
    /// First arrival of each recent AEP1 block, at `block_id %
    /// FIRST_ARRIVAL_BLOCKS`. A block carries one epoch.
    block_first_us: Box<[Option<(u32, u64)>]>,
}

/// The FEC receiver of one source, with its windows set in time.
fn aep1_session_config() -> MultichannelAudioSessionConfig {
    MultichannelAudioSessionConfig {
        max_inflight_epochs: AEP1_MAX_INFLIGHT_EPOCHS,
        ..MultichannelAudioSessionConfig::default()
    }
}

impl ReceiverState {
    fn new(now: Instant) -> Self {
        Self {
            receiver: MultichannelAudioReceiver::new(aep1_session_config()),
            last_seen: now,
            block_first_us: vec![None; FIRST_ARRIVAL_BLOCKS].into_boxed_slice(),
        }
    }

    /// Note a datagram of `block_id` and return the block's first arrival.
    fn observe_block(&mut self, block_id: u32, received_us: u64) -> u64 {
        let slot = &mut self.block_first_us[block_id as usize % FIRST_ARRIVAL_BLOCKS];
        match slot {
            Some((block, first)) if *block == block_id => *first,
            _ => {
                *slot = Some((block_id, received_us));
                received_us
            }
        }
    }

    /// Recover the groups this datagram completes, with their source times,
    /// and the epoch it completes, if it does.
    fn push(
        &mut self,
        payload: &[u8],
        received_us: u64,
    ) -> Result<(Vec<RecoveredGroup>, Option<DecodedMultichannelAudioEpoch>), String> {
        let first_us = raptorq_datagram_fec::decode_header(payload)
            .map(|header| self.observe_block(header.block_id, received_us))
            .unwrap_or(received_us);
        let outcome = self
            .receiver
            .push_datagram(payload)
            .map_err(|error| error.to_string())?;
        if outcome.completed_groups.is_empty() {
            return Ok((Vec::new(), outcome.completed_epoch));
        }
        let recovered_us = now_unix_us();
        let groups = outcome
            .completed_groups
            .into_iter()
            .map(|group| {
                let times = PartSourceTimes {
                    src_first_us: Some(first_us.min(received_us)),
                    src_last_us: Some(received_us),
                    recovered_us: Some(recovered_us),
                    ..PartSourceTimes::of_epoch(group.epoch_id)
                };
                (group, times)
            })
            .collect();
        Ok((groups, outcome.completed_epoch))
    }
}

impl std::fmt::Debug for RenditionState {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("RenditionState")
            .field("format", &self.format)
            .field("expected_pts_samples", &self.expected_pts_samples)
            .finish_non_exhaustive()
    }
}

impl RenditionPackager {
    async fn finish(&mut self) -> Result<(), String> {
        match self {
            Self::Opaque(segmenter) => {
                segmenter.finish();
                Ok(())
            }
            Self::Fmp4(rendition) => rendition.finish().await,
        }
    }

    fn opaque(&mut self) -> Result<&mut OpaquePartSegmenter, String> {
        match self {
            Self::Opaque(segmenter) => Ok(segmenter),
            Self::Fmp4(_) => Err("opaque AEP1 payload reached the fMP4 packager".to_string()),
        }
    }

    fn fmp4(&mut self) -> Result<&mut Fmp4Rendition, String> {
        match self {
            Self::Fmp4(rendition) => Ok(rendition),
            Self::Opaque(_) => Err("fMP4 AEP1 payload reached the opaque packager".to_string()),
        }
    }
}

impl OpaquePartSegmenter {
    fn new(
        output_stream_id: u64,
        output_stream_idx: usize,
        session_id_ns: u64,
        part: AudioPartSize,
        flac: bool,
        publisher: Arc<dyn Fmp4PartPublisher>,
        counters: Arc<OpaqueStreamCounters>,
    ) -> Self {
        Self {
            output_stream_id,
            output_stream_idx,
            session_id_ns,
            part,
            flac,
            publisher,
            counters,
            geometry: None,
            open: None,
        }
    }

    /// The session's geometry, set by its first epoch, when an epoch of
    /// `frame_count` samples at `sample_rate` keeps it; `None` when it does
    /// not.
    fn geometry_of(&mut self, frame_count: u32, sample_rate: u32) -> Option<PartGeometry> {
        let geometry = *self.geometry.get_or_insert_with(|| PartGeometry {
            frame_count,
            sample_rate,
            part_samples: part_samples(self.part, sample_rate, frame_count, self.flac),
        });
        (geometry.frame_count == frame_count && geometry.sample_rate == sample_rate)
            .then_some(geometry)
    }

    /// Add the recovered epoch at `pts_samples`, which is in part `number`
    /// at `place`, and return the part when this epoch completes it. With
    /// one epoch per part (the default) the part is the epoch's own bytes:
    /// no copy and no allocation.
    ///
    /// One part is open at a time. An epoch of a later part drops the open
    /// one, which is then never published here (another contributor that has
    /// its epochs publishes it); an epoch of an earlier part is dropped.
    fn push(
        &mut self,
        geometry: PartGeometry,
        (number, place): (u64, u64),
        packet: Bytes,
        pts_samples: u64,
        source_times: PartSourceTimes,
    ) -> Option<GatheredPart> {
        let anchor = |first_pts_samples, last_pts_samples| SourceAnchor {
            session_id_ns: self.session_id_ns,
            first_pts_samples,
            last_pts_samples,
            part_samples: geometry.part_samples,
            sample_rate: geometry.sample_rate,
        };
        let places = geometry.epochs_per_part();
        if places <= 1 {
            return Some(GatheredPart {
                anchor: anchor(pts_samples, pts_samples),
                bytes: packet,
                audio_units: 1,
                source_times,
            });
        }
        match self.open.as_ref().map(|open| open.number) {
            Some(open) if number < open => return None,
            Some(open) if number > open => {
                count(&self.counters.parts_skipped);
                self.open = None;
            }
            _ => {}
        }
        let open = self.open.get_or_insert_with(|| OpenPart {
            number,
            epochs: vec![None; places],
            missing: places,
            first_pts_samples: pts_samples,
            last_pts_samples: pts_samples,
            source_times: PartSourceTimes::default(),
        });
        let place = usize::try_from(place).ok()?;
        // The epoch again (a source may send one twice): the first stays.
        let slot = open.epochs.get_mut(place).filter(|slot| slot.is_none())?;
        *slot = Some(packet);
        open.missing -= 1;
        open.source_times.merge(&source_times);
        if place == 0 {
            open.first_pts_samples = pts_samples;
        }
        if place + 1 == places {
            open.last_pts_samples = pts_samples;
        }
        if open.missing > 0 {
            return None;
        }
        let open = self.open.take()?;
        let mut bytes = BytesMut::with_capacity(
            open.epochs
                .iter()
                .map(|epoch| epoch.as_ref().map_or(0, Bytes::len))
                .sum(),
        );
        for epoch in open.epochs.iter().flatten() {
            bytes.extend_from_slice(epoch);
        }
        Some(GatheredPart {
            anchor: anchor(open.first_pts_samples, open.last_pts_samples),
            bytes: bytes.freeze(),
            audio_units: places,
            source_times: open.source_times,
        })
    }

    /// The session ended. A part that lacks an epoch is never published: a
    /// short part would have other bytes than the whole part that another
    /// contributor publishes under the same number.
    fn finish(&mut self) {
        self.open = None;
    }

    /// Publish a whole part, gathered at `packaged_at_unix_ns` by this
    /// host's clock, under the number, epoch and anchor of its source.
    async fn publish(&self, part: GatheredPart, packaged_at_unix_ns: i64) -> Result<(), String> {
        let GatheredPart {
            anchor,
            bytes,
            audio_units,
            source_times,
        } = part;
        let duration_us = samples_to_us(anchor.part_samples, anchor.sample_rate);
        let part = PublishedOpaquePart {
            stream_id: self.output_stream_id,
            stream_idx: self.output_stream_idx,
            source_epoch: Some(anchor.key_epoch()),
            sequence: anchor.part_number(),
            duration_ms: legacy_ms(duration_us).max(1),
            duration_us,
            duration_samples: Some(anchor.part_samples),
            sample_rate: Some(anchor.sample_rate),
            packaged_at_unix_ns,
            published_at_unix_ns: now_unix_ns(),
            bytes,
            audio_units,
            hls: None,
            source_times,
            anchor,
        };
        self.publisher.publish_opaque_part(part).await
    }
}

fn now_unix_ns() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_nanos().min(i64::MAX as u128) as i64)
        .unwrap_or(0)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct RenditionKey {
    session_id: u64,
    group_id: u16,
    payload_kind: AudioPayloadKind,
}

impl Hash for RenditionKey {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.session_id.hash(state);
        self.group_id.hash(state);
        (self.payload_kind as u8).hash(state);
    }
}

pub fn channel(
    capacity: usize,
) -> (
    mpsc::Sender<AudioEpochHlsDatagram>,
    mpsc::Receiver<AudioEpochHlsDatagram>,
) {
    mpsc::channel(capacity.max(1))
}

pub async fn run_audio_epoch_hls_worker(
    config: AudioEpochHlsConfig,
    mut input: mpsc::Receiver<AudioEpochHlsDatagram>,
    mut shutdown_rx: watch::Receiver<()>,
) {
    let transport = MultichannelAudioTransportAdapter::udp(65_535);
    let mut receivers = HashMap::<SocketAddr, ReceiverState>::new();
    let mut rendition_workers = HashMap::<RenditionKey, RenditionWorker>::new();
    let mut idle_sweep = tokio::time::interval(AUDIO_EPOCH_IDLE_SWEEP_INTERVAL);
    idle_sweep.set_missed_tick_behavior(MissedTickBehavior::Delay);

    loop {
        tokio::select! {
            _ = shutdown_rx.changed() => break,
            _ = idle_sweep.tick() => {
                retire_idle_receivers(&mut receivers, Instant::now());
                retire_idle_rendition_workers(&mut rendition_workers, Instant::now()).await;
            }
            message = input.recv() => {
                let Some(message) = message else { break; };
                let payload = match transport.payload(&message.bytes) {
                    Ok(payload) => payload,
                    Err(error) => {
                        warn!(peer = %message.peer, error = %error, "dropping invalid AEP1 datagram before LL-HLS recovery");
                        continue;
                    }
                };
                let now = Instant::now();
                let receiver = receivers.entry(message.peer).or_insert_with(|| {
                    ACTIVE_RECEIVERS.fetch_add(1, Ordering::Relaxed);
                    ReceiverState::new(now)
                });
                receiver.last_seen = now;
                let outcome = receiver.push(payload, message.received_unix_us);
                match outcome {
                    Ok((completed_groups, completed_epoch)) => {
                        WORKER_DATAGRAMS.fetch_add(1, Ordering::Relaxed);
                        share_recovered_epoch(&config, message.peer, completed_epoch);
                        for (group, times) in completed_groups {
                            WORKER_GROUPS_COMPLETED.fetch_add(1, Ordering::Relaxed);
                            WORKER_RAPTORQ_FRAGMENTS_RECOVERED.fetch_add(
                                u64::from(group.raptorq_recovered_fragments),
                                Ordering::Relaxed,
                            );
                            let group = match terminalize_soundkit_group(group) {
                                Ok(group) => group,
                                Err(error) => {
                                    WORKER_ERRORS.fetch_add(1, Ordering::Relaxed);
                                    warn!(peer = %message.peer, error = %error, "rejected non-canonical SoundKit v2 group");
                                    continue;
                                }
                            };
                            if !config.formats.contains(&group.payload_kind) {
                                continue;
                            }
                            if let Err(error) = dispatch_group_to_rendition_worker(
                                &config,
                                &mut rendition_workers,
                                (group, times),
                            ).await {
                                WORKER_ERRORS.fetch_add(1, Ordering::Relaxed);
                                warn!(peer = %message.peer, error = %error, "failed to dispatch recovered AEP1 group to LL-HLS rendition worker");
                            }
                        }
                    }
                    Err(error) => {
                        WORKER_ERRORS.fetch_add(1, Ordering::Relaxed);
                        warn!(peer = %message.peer, error = %error, "failed to recover AEP1 datagram for LL-HLS");
                    }
                }
            }
        }
    }

    while let Ok(message) = input.try_recv() {
        let Ok(payload) = transport.payload(&message.bytes) else {
            continue;
        };
        let now = Instant::now();
        let receiver = receivers.entry(message.peer).or_insert_with(|| {
            ACTIVE_RECEIVERS.fetch_add(1, Ordering::Relaxed);
            ReceiverState::new(now)
        });
        receiver.last_seen = now;
        let outcome = receiver.push(payload, message.received_unix_us);
        if let Ok((completed_groups, completed_epoch)) = outcome {
            WORKER_DATAGRAMS.fetch_add(1, Ordering::Relaxed);
            share_recovered_epoch(&config, message.peer, completed_epoch);
            for (group, times) in completed_groups {
                WORKER_GROUPS_COMPLETED.fetch_add(1, Ordering::Relaxed);
                WORKER_RAPTORQ_FRAGMENTS_RECOVERED.fetch_add(
                    u64::from(group.raptorq_recovered_fragments),
                    Ordering::Relaxed,
                );
                let group = match terminalize_soundkit_group(group) {
                    Ok(group) => group,
                    Err(error) => {
                        WORKER_ERRORS.fetch_add(1, Ordering::Relaxed);
                        warn!(peer = %message.peer, error = %error, "rejected non-canonical SoundKit v2 group");
                        continue;
                    }
                };
                if !config.formats.contains(&group.payload_kind) {
                    continue;
                }
                if let Err(error) = dispatch_group_to_rendition_worker(
                    &config,
                    &mut rendition_workers,
                    (group, times),
                )
                .await
                {
                    WORKER_ERRORS.fetch_add(1, Ordering::Relaxed);
                    warn!(peer = %message.peer, error = %error, "failed to drain AEP1 group into LL-HLS rendition worker");
                }
            }
        } else {
            WORKER_ERRORS.fetch_add(1, Ordering::Relaxed);
        }
    }

    let receiver_count = receivers.len();
    ACTIVE_RECEIVERS.fetch_sub(receiver_count as u64, Ordering::Relaxed);
    let renditions = rendition_workers.len();
    stop_rendition_workers(rendition_workers).await;
    info!(
        receivers = receiver_count,
        renditions, "AEP1 LL-HLS worker stopped"
    );
}

async fn dispatch_group_to_rendition_worker(
    config: &AudioEpochHlsConfig,
    workers: &mut HashMap<RenditionKey, RenditionWorker>,
    recovered: RecoveredGroup,
) -> Result<(), String> {
    let group = &recovered.0;
    let key = RenditionKey {
        session_id: group.session_id,
        group_id: group.group_id,
        payload_kind: group.payload_kind,
    };
    let now = Instant::now();
    let sender = match workers.entry(key) {
        std::collections::hash_map::Entry::Occupied(entry) => {
            let worker = entry.into_mut();
            worker.last_seen = now;
            worker.sender.clone()
        }
        std::collections::hash_map::Entry::Vacant(entry) => {
            let capacity = rendition_queue_capacity(group.frame_count, group.sample_rate);
            RENDITION_QUEUE_CAPACITY.fetch_max(capacity as u64, Ordering::Relaxed);
            let (sender, receiver) = mpsc::channel(capacity);
            let worker = run_rendition_worker(config.clone(), key, receiver);
            let handle = match config.rendition_runtimes.as_ref() {
                Some(runtimes) => runtimes.spawn(worker),
                None => tokio::spawn(worker),
            };
            ACTIVE_RENDITION_WORKERS.fetch_add(1, Ordering::Relaxed);
            entry
                .insert(RenditionWorker {
                    sender: sender.clone(),
                    handle,
                    last_seen: now,
                })
                .sender
                .clone()
        }
    };
    try_handoff_rendition_group(&sender, key, recovered)
}

/// Places in a rendition worker's queue: one second of its epochs, at least
/// [`RENDITION_WORKER_QUEUE_MIN_CAPACITY`]. 4,000 for 250 µs epochs.
fn rendition_queue_capacity(frame_count: u32, sample_rate: u32) -> usize {
    let epoch_us = samples_to_us(u64::from(frame_count), sample_rate).max(SMALLEST_EPOCH_US);
    usize::try_from(RENDITION_WORKER_QUEUE_TIME_US.div_ceil(epoch_us))
        .unwrap_or(usize::MAX)
        .max(RENDITION_WORKER_QUEUE_MIN_CAPACITY)
}

fn try_handoff_rendition_group(
    sender: &mpsc::Sender<RecoveredGroup>,
    key: RenditionKey,
    group: RecoveredGroup,
) -> Result<(), String> {
    match sender.try_send(group) {
        Ok(()) => {
            RENDITION_QUEUE_ENQUEUED.fetch_add(1, Ordering::Relaxed);
            let depth = RENDITION_QUEUE_DEPTH.fetch_add(1, Ordering::Relaxed) + 1;
            RENDITION_QUEUE_MAX_DEPTH.fetch_max(depth, Ordering::Relaxed);
            Ok(())
        }
        Err(mpsc::error::TrySendError::Full(_)) => {
            RENDITION_QUEUE_DROPPED.fetch_add(1, Ordering::Relaxed);
            Err(format!(
                "AEP1 LL-HLS rendition worker {key:?} queue is full; newest group dropped"
            ))
        }
        Err(mpsc::error::TrySendError::Closed(_)) => {
            Err(format!("AEP1 LL-HLS rendition worker {key:?} stopped"))
        }
    }
}

fn retire_idle_receivers(receivers: &mut HashMap<SocketAddr, ReceiverState>, now: Instant) {
    let before = receivers.len();
    receivers.retain(|_, state| {
        now.saturating_duration_since(state.last_seen) < AUDIO_EPOCH_IDLE_TIMEOUT
    });
    let retired = before.saturating_sub(receivers.len());
    if retired > 0 {
        ACTIVE_RECEIVERS.fetch_sub(retired as u64, Ordering::Relaxed);
        RETIRED_RECEIVERS.fetch_add(retired as u64, Ordering::Relaxed);
        debug!(
            retired,
            active = receivers.len(),
            "retired idle AEP1 FEC receivers"
        );
    }
}

async fn retire_idle_rendition_workers(
    workers: &mut HashMap<RenditionKey, RenditionWorker>,
    now: Instant,
) {
    let idle_keys = workers
        .iter()
        .filter_map(|(key, worker)| {
            (now.saturating_duration_since(worker.last_seen) >= AUDIO_EPOCH_IDLE_TIMEOUT)
                .then_some(*key)
        })
        .collect::<Vec<_>>();
    if idle_keys.is_empty() {
        return;
    }

    let mut idle = HashMap::with_capacity(idle_keys.len());
    for key in idle_keys {
        if let Some(worker) = workers.remove(&key) {
            idle.insert(key, worker);
        }
    }
    let retired = idle.len();
    RETIRED_RENDITION_WORKERS.fetch_add(retired as u64, Ordering::Relaxed);
    stop_rendition_workers(idle).await;
    debug!(
        retired,
        active = workers.len(),
        "retired idle AEP1 LL-HLS rendition workers"
    );
}

async fn run_rendition_worker(
    config: AudioEpochHlsConfig,
    key: RenditionKey,
    mut input: mpsc::Receiver<RecoveredGroup>,
) {
    let mut renditions = HashMap::<RenditionKey, RenditionState>::new();
    while let Some((group, times)) = input.recv().await {
        let _ = RENDITION_QUEUE_DEPTH.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |depth| {
            Some(depth.saturating_sub(1))
        });
        if let Err(error) = package_group_with_times(&config, &mut renditions, group, times).await {
            WORKER_ERRORS.fetch_add(1, Ordering::Relaxed);
            warn!(
                session_id = key.session_id,
                group_id = key.group_id,
                payload_kind = ?key.payload_kind,
                error = %error,
                "failed to package recovered AEP1 group into LL-HLS"
            );
        }
    }

    for rendition in renditions.values_mut() {
        if let Err(error) = rendition.packager.finish().await {
            WORKER_ERRORS.fetch_add(1, Ordering::Relaxed);
            warn!(error = %error, "failed to flush AEP1 LL-HLS rendition");
        }
    }
    debug!(
        session_id = key.session_id,
        group_id = key.group_id,
        payload_kind = ?key.payload_kind,
        "AEP1 LL-HLS rendition worker stopped"
    );
}

async fn stop_rendition_workers(workers: HashMap<RenditionKey, RenditionWorker>) {
    let worker_count = workers.len();
    let mut handles = Vec::with_capacity(workers.len());
    for (_, worker) in workers {
        drop(worker.sender);
        handles.push(worker.handle);
    }
    for handle in handles {
        if let Err(error) = handle.await {
            WORKER_ERRORS.fetch_add(1, Ordering::Relaxed);
            warn!(error = %error, "AEP1 LL-HLS rendition worker join failed");
        }
    }
    ACTIVE_RENDITION_WORKERS.fetch_sub(worker_count as u64, Ordering::Relaxed);
}

fn new_rendition_packager(
    config: &AudioEpochHlsConfig,
    output_stream_id: u64,
    output_stream_idx: usize,
    session_id: u64,
    payload_kind: AudioPayloadKind,
    sample_rate: u32,
) -> RenditionPackager {
    let flac = payload_kind == AudioPayloadKind::Flac;
    match config.packaging {
        AudioEpochHlsPackaging::Opaque => RenditionPackager::Opaque(OpaquePartSegmenter::new(
            output_stream_id,
            output_stream_idx,
            session_id,
            config.part,
            flac,
            Arc::clone(&config.publisher),
            config.opaque_streams.stream(output_stream_id),
        )),
        AudioEpochHlsPackaging::Fmp4 => {
            let source_epoch = session_id.saturating_div(1_000).max(1);
            // The boxer takes millisecond timestamps (TimestampInput is
            // MillisAbsolute); the part is closed on its sample count.
            let mut segmenter = Fmp4Segmenter::new_publish_only(
                output_stream_id,
                output_stream_idx,
                Arc::clone(&config.playlists),
                TimestampInput::MillisAbsolute,
                DEFAULT_MIN_PART_MS,
                Arc::clone(&config.publisher),
            );
            segmenter.set_min_part_us(fmp4_min_part_us(config, payload_kind, sample_rate));
            segmenter.set_source_epoch(source_epoch);
            RenditionPackager::Fmp4(Box::new(Fmp4Rendition::new(segmenter)))
        }
    }
}

/// Shortest fMP4 part of a rendition: `0`, one access unit per part, for
/// one epoch per part.
fn fmp4_min_part_us(
    config: &AudioEpochHlsConfig,
    payload_kind: AudioPayloadKind,
    sample_rate: u32,
) -> u64 {
    config
        .part
        .target_samples(sample_rate, payload_kind == AudioPayloadKind::Flac)
        .map_or(0, |samples| samples_to_us(samples, sample_rate))
}

#[cfg(test)]
async fn package_group(
    config: &AudioEpochHlsConfig,
    renditions: &mut HashMap<RenditionKey, RenditionState>,
    group: DecodedMultichannelAudioGroup,
) -> Result<(), String> {
    package_group_with_times(config, renditions, group, PartSourceTimes::default()).await
}

async fn package_group_with_times(
    config: &AudioEpochHlsConfig,
    renditions: &mut HashMap<RenditionKey, RenditionState>,
    group: DecodedMultichannelAudioGroup,
    times: PartSourceTimes,
) -> Result<(), String> {
    if group.flags & AUDIO_GROUP_FLAG_ERASURE != 0 || group.payload.is_empty() {
        return Err(format!(
            "session {} group {} is an explicit audio erasure",
            group.session_id, group.group_id
        ));
    }
    if group.sample_rate == 0 || group.frame_count == 0 || group.channel_count == 0 {
        return Err(format!(
            "session {} group {} has invalid AEP1 timing dimensions",
            group.session_id, group.group_id
        ));
    }
    let format = RenditionFormat {
        config_generation: group.config_generation,
        sample_rate: group.sample_rate,
        channel_count: group.channel_count,
        payload_kind: group.payload_kind,
        sample_format: group.sample_format,
    };
    let key = RenditionKey {
        session_id: group.session_id,
        group_id: group.group_id,
        payload_kind: group.payload_kind,
    };
    let state = match renditions.entry(key) {
        std::collections::hash_map::Entry::Occupied(entry) => entry.into_mut(),
        std::collections::hash_map::Entry::Vacant(entry) => {
            let output_stream_id =
                rendition_stream_id(config.base_stream_id, group.group_id, group.payload_kind)?;
            // The contributor publishes canonical media objects but does not
            // host a viewer cache. Keep the logical stream identity without
            // allocating a local chunk-cache slot for every source session.
            let output_stream_idx = usize::try_from(output_stream_id)
                .map_err(|_| "AEP1 LL-HLS stream id exceeds this platform".to_string())?;
            let state = entry.insert(RenditionState {
                format,
                expected_pts_samples: None,
                refused_opus_groups: 0,
                numbers: NumberWindow::new(),
                packager: new_rendition_packager(
                    config,
                    output_stream_id,
                    output_stream_idx,
                    group.session_id,
                    group.payload_kind,
                    group.sample_rate,
                ),
            });
            info!(
                session_id = group.session_id,
                group_id = group.group_id,
                payload_kind = ?group.payload_kind,
                output_stream_id,
                sample_rate = group.sample_rate,
                channels = group.channel_count,
                packaging = ?config.packaging,
                "created AEP1 LL-HLS rendition"
            );
            state
        }
    };

    if group.payload_kind == AudioPayloadKind::Opus {
        if let Err(error) = opus_frame_fits_packaging(
            config.packaging,
            config.part,
            group.sample_rate,
            group.frame_count,
        ) {
            // Every group of the rendition fails the same way; report the
            // 1st, 2nd, 4th, 8th... and drop the others quietly.
            state.refused_opus_groups = state.refused_opus_groups.saturating_add(1);
            return if state.refused_opus_groups.is_power_of_two() {
                Err(format!(
                    "{error} ({} Opus groups refused)",
                    state.refused_opus_groups
                ))
            } else {
                Ok(())
            };
        }
    }

    if config.packaging == AudioEpochHlsPackaging::Opaque {
        return package_group_as_opaque(state, group, times).await;
    }

    let pts_discontinuity = state
        .expected_pts_samples
        .is_some_and(|expected| expected != group.pts_samples);
    let format_changed = state.format != format;
    if format_changed || pts_discontinuity || group.flags & AUDIO_GROUP_FLAG_DISCONTINUITY != 0 {
        let rendition = state.packager.fmp4()?;
        rendition.finish().await?;
        rendition.reset();
        rendition.segmenter.set_min_part_us(fmp4_min_part_us(
            config,
            group.payload_kind,
            group.sample_rate,
        ));
        state.format = format;
        debug!(
            session_id = group.session_id,
            group_id = group.group_id,
            config_generation = group.config_generation,
            format_changed,
            pts_discontinuity,
            "started a new AEP1 LL-HLS continuity segment"
        );
    }
    state.expected_pts_samples = Some(
        group
            .pts_samples
            .checked_add(u64::from(group.frame_count))
            .ok_or_else(|| "AEP1 audio PTS overflow".to_string())?,
    );
    package_group_as_fmp4(state, group, times).await
}

/// Publish the opaque part that this epoch completes, if it completes one
/// and its source kept the AEP1 source contract.
///
/// The part an epoch belongs to, and everything that names the part, comes
/// from the epoch's session id and sample count: there is no continuity to
/// reset, and a gap in the sample count is a hole in the numbers. Input
/// outside the contract is counted and not published; nothing here is an
/// error, so a source that loops does not log a line an epoch.
async fn package_group_as_opaque(
    state: &mut RenditionState,
    group: DecodedMultichannelAudioGroup,
    times: PartSourceTimes,
) -> Result<(), String> {
    let RenditionState {
        packager, numbers, ..
    } = state;
    let segmenter = packager.opaque()?;
    let stream_id = segmenter.output_stream_id;
    let Some(geometry) = segmenter.geometry_of(group.frame_count, group.sample_rate) else {
        // A session keeps the frame count and rate of its first epoch: part
        // numbers of another geometry would name other samples.
        let dropped = count(&segmenter.counters.geometry_changed);
        if dropped.is_power_of_two() {
            warn!(
                stream_id,
                session_id = group.session_id,
                frame_count = group.frame_count,
                sample_rate = group.sample_rate,
                dropped,
                "AEP1 epoch geometry changed inside a session: not published until a new session"
            );
        }
        return Ok(());
    };
    let (number, place) = part_place(group.pts_samples, geometry.part_samples, group.frame_count);
    if numbers.verdict(number) == NumberVerdict::Reused {
        let dropped = count(&segmenter.counters.numbers_reused);
        if dropped.is_power_of_two() {
            warn!(
                stream_id,
                session_id = group.session_id,
                pts_samples = group.pts_samples,
                part = number,
                dropped,
                "AEP1 sample count reuses a part number of its session: not published"
            );
        }
        return Ok(());
    }
    let Some(part) = segmenter.push(
        geometry,
        (number, place),
        group.payload,
        group.pts_samples,
        times,
    ) else {
        return Ok(());
    };
    if numbers.verdict(number) == NumberVerdict::LateFill {
        count(&segmenter.counters.late_fills);
    }
    numbers.hand(number);
    let packaged_at_unix_ns = now_unix_ns();
    if part
        .anchor
        .source_time_ns()
        .saturating_sub(packaged_at_unix_ns)
        > SOURCE_TIME_AHEAD_NS
    {
        let ahead = count(&segmenter.counters.time_ahead);
        if ahead.is_power_of_two() {
            warn!(
                stream_id,
                session_id = group.session_id,
                pts_samples = group.pts_samples,
                ahead,
                "AEP1 source time is ahead of this host's clock: published all the same"
            );
        }
    }
    segmenter.publish(part, packaged_at_unix_ns).await
}

async fn package_group_as_fmp4(
    state: &mut RenditionState,
    group: DecodedMultichannelAudioGroup,
    times: PartSourceTimes,
) -> Result<(), String> {
    let audio_config = audio_track_config(&group)?;
    let (packets, stream_type) = match group.payload_kind {
        AudioPayloadKind::Flac => (
            vec![ElementaryAudioPacket {
                data: group.payload.clone(),
                frame_count: group.frame_count,
                id: group.epoch_id,
            }],
            PSI_STREAM_PRIVATE_DATA,
        ),
        AudioPayloadKind::Pcm => {
            validate_pcm_group(&group)?;
            let rendition = state.packager.fmp4()?;
            rendition.segmenter.set_audio_track_config(audio_config);
            return rendition
                .push_pcm(
                    group.payload,
                    group.frame_count,
                    group.sample_rate,
                    group.pts_samples,
                    group.epoch_id,
                    times,
                )
                .await;
        }
        AudioPayloadKind::Opus => (raw_opus_packets(&group)?, PSI_STREAM_AUDIO_OPUS),
        AudioPayloadKind::SoundKitV2 => {
            return Err("SoundKit v2 group reached fMP4 before terminal decoding".to_string())
        }
    };
    let rendition = state.packager.fmp4()?;
    if group.payload_kind == AudioPayloadKind::Flac
        && !whole_ms(u64::from(group.frame_count), group.sample_rate)
        && !rendition.warned_fractional_flac
    {
        rendition.warned_fractional_flac = true;
        warn!(
            session_id = group.session_id,
            group_id = group.group_id,
            frame_count = group.frame_count,
            sample_rate = group.sample_rate,
            "FLAC epochs are not whole milliseconds: the fMP4 boxer (timescale 1,000) rounds \
             their durations; use opaque packaging for exact sub-millisecond FLAC"
        );
    }
    let segmenter = &mut rendition.segmenter;
    segmenter.set_audio_track_config(audio_config);
    let mut pts_samples = group.pts_samples;
    let mut times = Some(times);
    for packet in packets {
        push_audio_unit(
            segmenter,
            packet.data,
            pts_samples,
            group.sample_rate,
            packet.id,
            times.take().unwrap_or_default(),
            stream_type,
        )
        .await?;
        pts_samples = pts_samples
            .checked_add(u64::from(packet.frame_count))
            .ok_or_else(|| "AEP1 audio packet PTS overflow".to_string())?;
    }
    Ok(())
}

struct ElementaryAudioPacket {
    data: Bytes,
    frame_count: u32,
    id: u64,
}

fn audio_track_config(
    group: &DecodedMultichannelAudioGroup,
) -> Result<Option<AudioTrackConfig>, String> {
    match group.payload_kind {
        AudioPayloadKind::Flac => Ok(None),
        AudioPayloadKind::Opus if matches!(group.channel_count, 1 | 2) => {
            Ok(Some(AudioTrackConfig::Opus(OpusAudioConfig {
                input_sample_rate: group.sample_rate,
                channel_count: group.channel_count,
                pre_skip: 0,
                output_gain: 0,
            })))
        }
        AudioPayloadKind::Opus => Err("Opus fMP4 supports one or two output channels".to_string()),
        AudioPayloadKind::Pcm => {
            let (sample_size, sample_kind) = match group.sample_format {
                AudioSampleFormat::S16Le => (16, PcmSampleKind::Integer),
                AudioSampleFormat::S24Le => (24, PcmSampleKind::Integer),
                AudioSampleFormat::S32Le => (32, PcmSampleKind::Integer),
                AudioSampleFormat::F32Le => (32, PcmSampleKind::Float),
                AudioSampleFormat::Unspecified => {
                    return Err("PCM AEP1 payload must declare its sample format".to_string())
                }
            };
            Ok(Some(AudioTrackConfig::Pcm(PcmAudioConfig {
                sample_rate: group.sample_rate,
                channel_count: group.channel_count,
                sample_size,
                little_endian: true,
                sample_kind,
            })))
        }
        AudioPayloadKind::SoundKitV2 => Err(
            "SoundKit v2 group reached track configuration before terminal decoding".to_string(),
        ),
    }
}

fn rendition_stream_id(
    base_stream_id: u64,
    group_id: u16,
    payload_kind: AudioPayloadKind,
) -> Result<u64, String> {
    let format_offset = match payload_kind {
        AudioPayloadKind::Flac => 0,
        AudioPayloadKind::Opus => OPUS_RENDITION_STREAM_OFFSET,
        AudioPayloadKind::Pcm => PCM_RENDITION_STREAM_OFFSET,
        AudioPayloadKind::SoundKitV2 => {
            return Err(
                "SoundKit v2 group reached stream routing before terminal decoding".to_string(),
            )
        }
    };
    base_stream_id
        .checked_add(format_offset)
        .and_then(|stream_id| stream_id.checked_add(u64::from(group_id)))
        .ok_or_else(|| "AEP1 LL-HLS stream id overflow".to_string())
}

fn raw_opus_packets(
    group: &DecodedMultichannelAudioGroup,
) -> Result<Vec<ElementaryAudioPacket>, String> {
    let stream = match SoundKitV2StreamRef::parse(&group.payload, false) {
        Ok(stream) => Some(stream),
        Err(_) => None,
    };
    let packets = if let Some(stream) = stream {
        if stream.total_frame_count != u64::from(group.frame_count) {
            return Err("SoundKit Opus frame counts do not add up to the AEP1 group".to_string());
        }
        let mut expected_pts = group.pts_samples;
        let mut packets = Vec::with_capacity(stream.packets.len());
        for packet in stream.packets {
            let header = packet.header;
            if header.encoding() != &EncodingFlag::Opus {
                return Err("AEP1 Opus payload has a non-Opus SoundKit header".to_string());
            }
            if header.is_encrypted() {
                return Err(
                    "encrypted SoundKit Opus cannot be published as public fMP4".to_string()
                );
            }
            if header.sample_rate() != group.sample_rate
                || u16::from(header.channels()) != group.channel_count
            {
                return Err("SoundKit Opus metadata does not match its AEP1 group".to_string());
            }
            crate::opus_custom::refuse_custom_opus(
                header.sample_rate(),
                header.frame_count(),
                "fMP4 packaging (--daw-hls-packaging fmp4)",
            )?;
            if header.pts().is_some_and(|pts| pts != expected_pts) {
                return Err("SoundKit Opus packet PTS is not contiguous".to_string());
            }
            validate_raw_opus_packet(packet.payload, header.frame_count(), group.sample_rate)?;
            packets.push(ElementaryAudioPacket {
                data: Bytes::copy_from_slice(packet.payload),
                frame_count: header.frame_count(),
                id: header.id().unwrap_or(group.epoch_id),
            });
            expected_pts = expected_pts
                .checked_add(u64::from(header.frame_count()))
                .ok_or_else(|| "SoundKit Opus PTS overflow".to_string())?;
        }
        packets
    } else {
        crate::opus_custom::refuse_custom_opus(
            group.sample_rate,
            group.frame_count,
            "fMP4 packaging (--daw-hls-packaging fmp4)",
        )?;
        validate_raw_opus_packet(&group.payload, group.frame_count, group.sample_rate)?;
        vec![ElementaryAudioPacket {
            data: group.payload.clone(),
            frame_count: group.frame_count,
            id: group.epoch_id,
        }]
    };
    Ok(packets)
}

fn validate_raw_opus_packet(
    packet: &[u8],
    frame_count: u32,
    sample_rate: u32,
) -> Result<(), String> {
    let packet_info = opus_packet_info(packet)
        .ok_or_else(|| "AEP1 Opus payload is not one valid raw Opus packet".to_string())?;
    if u64::from(frame_count).saturating_mul(48_000)
        != u64::from(packet_info.duration_samples).saturating_mul(u64::from(sample_rate))
    {
        return Err("raw Opus duration does not match its SoundKit packet".to_string());
    }
    Ok(())
}

fn validate_pcm_group(group: &DecodedMultichannelAudioGroup) -> Result<(), String> {
    let config = match audio_track_config(group)? {
        Some(AudioTrackConfig::Pcm(config)) => config,
        _ => unreachable!("PCM group has PCM config"),
    };
    let expected = usize::try_from(group.frame_count)
        .ok()
        .and_then(|frames| frames.checked_mul(usize::from(group.channel_count)))
        .and_then(|samples| samples.checked_mul(usize::from(config.sample_size / 8)))
        .ok_or_else(|| "PCM AEP1 payload size overflow".to_string())?;
    if group.payload.len() != expected {
        return Err(format!(
            "PCM AEP1 payload has {} bytes; expected {expected}",
            group.payload.len()
        ));
    }
    Ok(())
}

fn samples_to_millis(samples: u64, sample_rate: u32) -> Result<u64, String> {
    if sample_rate == 0 {
        return Err("AEP1 sample rate must be positive".to_string());
    }
    Ok(samples
        .saturating_mul(1_000)
        .saturating_add(u64::from(sample_rate) / 2)
        / u64::from(sample_rate))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fmp4_bridge::{PublishedFmp4Part, PublishedOpaquePart};
    use music_audio_session::MultichannelAudioSender;
    use playlists::Options;
    use raptorq_datagram_fec::{
        MultichannelAudioEpoch, MultichannelAudioFecConfig, MultichannelAudioGroup,
    };
    use std::collections::HashSet;
    use std::sync::Mutex as StdMutex;

    #[derive(Default)]
    struct CapturingPublisher {
        fmp4_parts: StdMutex<Vec<PublishedFmp4Part>>,
        opaque_parts: StdMutex<Vec<PublishedOpaquePart>>,
    }

    #[async_trait::async_trait]
    impl Fmp4PartPublisher for CapturingPublisher {
        async fn publish_fmp4_part(&self, part: PublishedFmp4Part) -> Result<(), String> {
            self.fmp4_parts.lock().unwrap().push(part);
            Ok(())
        }

        async fn publish_opaque_part(&self, part: PublishedOpaquePart) -> Result<(), String> {
            self.opaque_parts.lock().unwrap().push(part);
            Ok(())
        }
    }

    fn test_config(
        base_stream_id: u64,
        min_part_ms: u32,
        packaging: AudioEpochHlsPackaging,
        publisher: Arc<dyn Fmp4PartPublisher>,
    ) -> AudioEpochHlsConfig {
        test_config_with_formats(
            base_stream_id,
            min_part_ms,
            packaging,
            vec![
                AudioPayloadKind::Opus,
                AudioPayloadKind::Flac,
                AudioPayloadKind::Pcm,
            ],
            publisher,
        )
    }

    fn test_config_with_formats(
        base_stream_id: u64,
        min_part_ms: u32,
        packaging: AudioEpochHlsPackaging,
        formats: Vec<AudioPayloadKind>,
        publisher: Arc<dyn Fmp4PartPublisher>,
    ) -> AudioEpochHlsConfig {
        let (playlists, _, _) = Playlists::new(Options {
            num_playlists: 256,
            part_target_us: min_part_ms.saturating_mul(1_000),
            ..Options::default()
        });
        AudioEpochHlsConfig::new(
            base_stream_id,
            AudioPartSize::from_legacy_ms(min_part_ms),
            packaging,
            formats,
            playlists,
            publisher,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn decoded_group(
        epoch_id: u64,
        pts_samples: u64,
        group_id: u16,
        channel_start: u16,
        channel_count: u16,
        payload_kind: AudioPayloadKind,
        sample_format: AudioSampleFormat,
        payload: Bytes,
    ) -> DecodedMultichannelAudioGroup {
        DecodedMultichannelAudioGroup {
            session_id: 101,
            config_generation: 1,
            epoch_id,
            pts_samples,
            sample_rate: 48_000,
            frame_count: 240,
            group_count: 1,
            group_id,
            group_index: 0,
            channel_start,
            channel_count,
            payload_kind,
            sample_format,
            flags: 0,
            payload,
            raptorq_recovered_fragments: 0,
        }
    }

    fn mdat_payload(part: &[u8]) -> Option<&[u8]> {
        let mut offset = 0_usize;
        while offset.checked_add(8)? <= part.len() {
            let size = u32::from_be_bytes(part[offset..offset + 4].try_into().ok()?) as usize;
            if size < 8 || offset.checked_add(size)? > part.len() {
                return None;
            }
            if &part[offset + 4..offset + 8] == b"mdat" {
                return Some(&part[offset + 8..offset + size]);
            }
            offset += size;
        }
        None
    }

    #[tokio::test]
    async fn idle_source_and_rendition_state_is_retired() {
        let now = Instant::now();
        let idle_since = now
            .checked_sub(AUDIO_EPOCH_IDLE_TIMEOUT + Duration::from_secs(1))
            .expect("test instant supports the idle interval");
        let peer: SocketAddr = "127.0.0.1:41999".parse().unwrap();
        let mut receivers = HashMap::from([(peer, ReceiverState::new(idle_since))]);
        ACTIVE_RECEIVERS.fetch_add(1, Ordering::Relaxed);

        retire_idle_receivers(&mut receivers, now);
        assert!(receivers.is_empty());

        let (sender, mut receiver) = mpsc::channel::<RecoveredGroup>(1);
        let handle = tokio::spawn(async move { while receiver.recv().await.is_some() {} });
        let key = RenditionKey {
            session_id: 99,
            group_id: 7,
            payload_kind: AudioPayloadKind::Flac,
        };
        let mut workers = HashMap::from([(
            key,
            RenditionWorker {
                sender,
                handle,
                last_seen: idle_since,
            },
        )]);
        ACTIVE_RENDITION_WORKERS.fetch_add(1, Ordering::Relaxed);

        retire_idle_rendition_workers(&mut workers, now).await;
        assert!(workers.is_empty());
    }

    /// A single-threaded runtime on a thread named `name`, until `stop`.
    fn runtime_thread(
        name: &str,
    ) -> (
        tokio::runtime::Handle,
        tokio::sync::oneshot::Sender<()>,
        std::thread::JoinHandle<()>,
    ) {
        let (handle_tx, handle_rx) = std::sync::mpsc::channel();
        let (stop_tx, stop_rx) = tokio::sync::oneshot::channel::<()>();
        let thread = std::thread::Builder::new()
            .name(name.into())
            .spawn(move || {
                let runtime = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .unwrap();
                handle_tx.send(runtime.handle().clone()).unwrap();
                runtime.block_on(async {
                    let _ = stop_rx.await;
                });
            })
            .unwrap();
        (handle_rx.recv().unwrap(), stop_tx, thread)
    }

    /// Rendition workers go to the runtimes in turn from the second, so that
    /// with one runtime per format each format's encode has a thread of its
    /// own and the first runtime (the recovery loop's) takes the last.
    #[tokio::test]
    async fn rendition_workers_run_on_the_rendition_runtimes_in_turn() {
        let threads = (0..3)
            .map(|index| runtime_thread(&format!("rendition-test-{index}")))
            .collect::<Vec<_>>();
        let runtimes = RenditionRuntimes::new(
            threads
                .iter()
                .map(|(handle, _, _)| handle.clone())
                .collect(),
        )
        .unwrap();
        assert!(RenditionRuntimes::new(Vec::new()).is_none());
        let mut names = Vec::new();
        for _ in 0..4 {
            let name = runtimes
                .spawn(async { std::thread::current().name().map(str::to_owned) })
                .await
                .unwrap();
            names.push(name.unwrap());
        }
        assert_eq!(
            names,
            [
                "rendition-test-1",
                "rendition-test-2",
                "rendition-test-0",
                "rendition-test-1"
            ]
        );

        // A dispatched group starts its worker on the next runtime.
        let captured = Arc::new(CapturingPublisher::default());
        let publisher: Arc<dyn Fmp4PartPublisher> = captured;
        let config = test_config(20, 5, AudioEpochHlsPackaging::Opaque, publisher)
            .with_rendition_runtimes(Some(runtimes.clone()));
        let mut workers = HashMap::new();
        let group = decoded_group(
            0,
            0,
            0,
            0,
            2,
            AudioPayloadKind::Pcm,
            AudioSampleFormat::S24Le,
            Bytes::from_static(b"pcm"),
        );
        dispatch_group_to_rendition_worker(
            &config,
            &mut workers,
            (group, PartSourceTimes::default()),
        )
        .await
        .unwrap();
        assert_eq!(workers.len(), 1);
        // The worker took runtime 2 (the fifth turn): the next is runtime 0.
        let next = runtimes
            .spawn(async { std::thread::current().name().map(str::to_owned) })
            .await
            .unwrap();
        assert_eq!(next.as_deref(), Some("rendition-test-0"));
        stop_rendition_workers(workers).await;
        for (_, stop, thread) in threads {
            let _ = stop.send(());
            thread.join().unwrap();
        }
    }

    #[test]
    fn sample_pts_maps_exactly_at_48khz_epoch_boundaries() {
        assert_eq!(samples_to_millis(0, 48_000).unwrap(), 0);
        assert_eq!(samples_to_millis(240, 48_000).unwrap(), 5);
        assert_eq!(samples_to_millis(720, 48_000).unwrap(), 15);
        assert_eq!(samples_to_millis(48_000, 48_000).unwrap(), 1_000);
    }

    #[tokio::test]
    async fn aep1_recovery_publishes_every_declared_format_as_opaque_bytes() {
        let captured = Arc::new(CapturingPublisher::default());
        let publisher: Arc<dyn Fmp4PartPublisher> = captured.clone();
        let config = test_config(20, 5, AudioEpochHlsPackaging::Opaque, publisher);
        let (tx, rx) = channel(64);
        let (shutdown_tx, shutdown_rx) = watch::channel(());
        let worker = tokio::spawn(run_audio_epoch_hls_worker(config, rx, shutdown_rx));

        // These deliberately are not valid PCM, FLAC, Opus, fMP4, or SoundKit.
        // The publication boundary must neither parse nor alter them.
        let expected = [
            vec![0x00, 0xff, 0x50, 0x43, 0x4d],
            vec![0x46, 0x4c, 0x41, 0x43, 0x00, 0xfe],
            vec![0x4f, 0x50, 0x55, 0x53, 0x81, 0x7f, 0x00],
        ];
        let descriptors = [
            (AudioPayloadKind::Pcm, AudioSampleFormat::S24Le),
            (AudioPayloadKind::Flac, AudioSampleFormat::S24Le),
            (AudioPayloadKind::Opus, AudioSampleFormat::Unspecified),
        ];
        let groups = expected
            .iter()
            .zip(descriptors)
            .enumerate()
            .map(
                |(group_id, (payload, (payload_kind, sample_format)))| MultichannelAudioGroup {
                    group_id: group_id as u16,
                    channel_start: group_id as u16 * 2,
                    channel_count: 2,
                    payload_kind,
                    sample_format,
                    flags: 0,
                    payload,
                },
            )
            .collect::<Vec<_>>();

        let transport = MultichannelAudioTransportAdapter::udp(1_200);
        let fec = transport.prepare_fec_config(MultichannelAudioFecConfig::default());
        let mut sender = MultichannelAudioSender::new(MultichannelAudioSessionConfig {
            fec,
            ..MultichannelAudioSessionConfig::default()
        });
        let encoded = sender
            .encode_epoch(MultichannelAudioEpoch {
                session_id: 99,
                config_generation: 3,
                epoch_id: 0,
                pts_samples: 0,
                sample_rate: 48_000,
                frame_count: 240,
                groups: &groups,
            })
            .unwrap();
        let peer: SocketAddr = "127.0.0.1:41000".parse().unwrap();
        for datagram in transport.wrap_epoch(encoded).unwrap().datagrams {
            tx.send(AudioEpochHlsDatagram {
                peer,
                bytes: datagram.payload,
                received_unix_us: now_unix_us(),
            })
            .await
            .unwrap();
        }
        drop(tx);
        let _ = shutdown_tx.send(());
        worker.await.unwrap();

        assert!(captured.fmp4_parts.lock().unwrap().is_empty());
        let parts = captured.opaque_parts.lock().unwrap();
        assert_eq!(parts.len(), expected.len());
        for (group_id, (bytes, (payload_kind, _))) in expected.iter().zip(descriptors).enumerate() {
            let stream_id = rendition_stream_id(20, group_id as u16, payload_kind).unwrap();
            let part = parts
                .iter()
                .find(|part| part.stream_id == stream_id)
                .expect("one published part per AEP1 group");
            assert_eq!(part.sequence, 0);
            assert_eq!(part.duration_ms, 5);
            assert_eq!(part.audio_units, 1);
            assert_eq!(part.bytes.as_ref(), bytes);
        }
    }

    #[tokio::test]
    async fn format_selection_publishes_flac_and_leaves_opus_on_the_wire() {
        let captured = Arc::new(CapturingPublisher::default());
        let publisher: Arc<dyn Fmp4PartPublisher> = captured.clone();
        let config = test_config_with_formats(
            20,
            5,
            AudioEpochHlsPackaging::Opaque,
            vec![AudioPayloadKind::Flac],
            publisher,
        );
        let (tx, rx) = channel(64);
        let (_shutdown_tx, shutdown_rx) = watch::channel(());
        let worker = tokio::spawn(run_audio_epoch_hls_worker(config, rx, shutdown_rx));
        let transport = MultichannelAudioTransportAdapter::udp(1_200);
        let fec = transport.prepare_fec_config(MultichannelAudioFecConfig::default());
        let mut sender = MultichannelAudioSender::new(MultichannelAudioSessionConfig {
            fec,
            ..MultichannelAudioSessionConfig::default()
        });
        let peer: SocketAddr = "127.0.0.1:41001".parse().unwrap();

        for (payload_kind, sample_format, payload) in [
            (
                AudioPayloadKind::Opus,
                AudioSampleFormat::Unspecified,
                b"opus".as_slice(),
            ),
            (
                AudioPayloadKind::Flac,
                AudioSampleFormat::S24Le,
                b"flac".as_slice(),
            ),
        ] {
            let groups = [MultichannelAudioGroup {
                group_id: 0,
                channel_start: 0,
                channel_count: 2,
                payload_kind,
                sample_format,
                flags: 0,
                payload,
            }];
            let encoded = sender
                .encode_epoch(MultichannelAudioEpoch {
                    session_id: 99,
                    config_generation: 3,
                    epoch_id: 0,
                    pts_samples: 0,
                    sample_rate: 48_000,
                    frame_count: 240,
                    groups: &groups,
                })
                .unwrap();
            for datagram in transport.wrap_epoch(encoded).unwrap().datagrams {
                tx.send(AudioEpochHlsDatagram {
                    peer,
                    bytes: datagram.payload,
                    received_unix_us: now_unix_us(),
                })
                .await
                .unwrap();
            }
        }
        drop(tx);
        worker.await.unwrap();

        let parts = captured.opaque_parts.lock().unwrap();
        assert_eq!(parts.len(), 1);
        assert_eq!(parts[0].bytes.as_ref(), b"flac");
    }

    #[tokio::test]
    async fn explicit_fmp4_policy_boxes_pcm_without_changing_samples() {
        let captured = Arc::new(CapturingPublisher::default());
        let publisher: Arc<dyn Fmp4PartPublisher> = captured.clone();
        let config = test_config(40, 5, AudioEpochHlsPackaging::Fmp4, publisher);
        let mut renditions = HashMap::new();
        let payload = (0..240 * 2)
            .flat_map(|sample| {
                let bytes = (sample * 97_i32).to_le_bytes();
                [bytes[0], bytes[1], bytes[2]]
            })
            .collect::<Vec<_>>();

        package_group(
            &config,
            &mut renditions,
            decoded_group(
                0,
                0,
                0,
                0,
                2,
                AudioPayloadKind::Pcm,
                AudioSampleFormat::S24Le,
                Bytes::from(payload.clone()),
            ),
        )
        .await
        .unwrap();

        assert!(captured.opaque_parts.lock().unwrap().is_empty());
        let parts = captured.fmp4_parts.lock().unwrap();
        assert_eq!(parts.len(), 1);
        assert_eq!(parts[0].audio_codec, Some("pcm_s24le"));
        assert_eq!(mdat_payload(&parts[0].bytes), Some(payload.as_slice()));
        assert!(parts[0]
            .init
            .as_ref()
            .is_some_and(|init| init.windows(4).any(|bytes| bytes == b"ipcm")));
    }

    #[tokio::test]
    async fn sequential_audio_sessions_get_distinct_canonical_source_epochs() {
        let captured = Arc::new(CapturingPublisher::default());
        let publisher: Arc<dyn Fmp4PartPublisher> = captured.clone();
        let config = test_config(40, 5, AudioEpochHlsPackaging::Fmp4, publisher);
        let mut renditions = HashMap::new();
        let payload = Bytes::from(vec![0_u8; 240 * 2 * 3]);

        for session_id in [1_784_151_600_000_000_000, 1_784_151_610_000_000_000] {
            let mut group = decoded_group(
                0,
                0,
                0,
                0,
                2,
                AudioPayloadKind::Pcm,
                AudioSampleFormat::S24Le,
                payload.clone(),
            );
            group.session_id = session_id;
            package_group(&config, &mut renditions, group)
                .await
                .unwrap();
        }

        let parts = captured.fmp4_parts.lock().unwrap();
        assert_eq!(parts.len(), 2);
        assert_eq!(parts[0].stream_id, parts[1].stream_id);
        assert_eq!(parts[0].sequence, 0);
        assert_eq!(parts[1].sequence, 0);
        assert_eq!(parts[0].source_epoch, Some(1_784_151_600_000_000));
        assert_eq!(parts[1].source_epoch, Some(1_784_151_610_000_000));
        assert_ne!(parts[0].source_epoch, parts[1].source_epoch);
    }

    #[tokio::test]
    async fn explicit_fmp4_policy_boxes_raw_opus_without_changing_packet() {
        let captured = Arc::new(CapturingPublisher::default());
        let publisher: Arc<dyn Fmp4PartPublisher> = captured.clone();
        let config = test_config(40, 5, AudioEpochHlsPackaging::Fmp4, publisher);
        let mut renditions = HashMap::new();
        let packet = vec![(17 << 3) | (1 << 2), 0x11, 0x22];

        package_group(
            &config,
            &mut renditions,
            decoded_group(
                0,
                0,
                0,
                0,
                2,
                AudioPayloadKind::Opus,
                AudioSampleFormat::Unspecified,
                Bytes::from(packet.clone()),
            ),
        )
        .await
        .unwrap();

        assert!(captured.opaque_parts.lock().unwrap().is_empty());
        let parts = captured.fmp4_parts.lock().unwrap();
        assert_eq!(parts.len(), 1);
        assert_eq!(parts[0].stream_id, 1_040);
        assert_eq!(parts[0].audio_codec, Some("opus"));
        assert_eq!(mdat_payload(&parts[0].bytes), Some(packet.as_slice()));
        assert!(parts[0].init.as_ref().is_some_and(|init| {
            init.windows(4).any(|bytes| bytes == b"Opus")
                && init.windows(4).any(|bytes| bytes == b"dOps")
        }));
    }

    #[test]
    fn soundkit_opus_stream_yields_one_access_unit_per_v2_packet() {
        let first_raw = vec![(17 << 3) | (1 << 2), 0x11, 0x22];
        let second_raw = vec![(17 << 3) | (1 << 2), 0x33, 0x44];
        let packet = |id, pts, payload: &[u8]| {
            crate::soundkit_v2::frame_soundkit_v2_packet(
                crate::soundkit_v2::SoundKitV2PacketMetadata {
                    encoding: EncodingFlag::Opus,
                    frame_count: 240,
                    sample_rate: 48_000,
                    channels: 2,
                    bits_per_sample: 0,
                    endianness: frame_header::Endianness::LittleEndian,
                    id: Some(id),
                    pts: Some(pts),
                },
                payload,
            )
            .unwrap()
        };
        let first = packet(10, 0, &first_raw);
        let second = packet(11, 240, &second_raw);
        let mut framed = Vec::from(first.as_ref());
        framed.extend_from_slice(&second);
        let mut group = decoded_group(
            0,
            0,
            0,
            0,
            2,
            AudioPayloadKind::Opus,
            AudioSampleFormat::Unspecified,
            Bytes::from(framed),
        );
        group.frame_count = 480;

        let packets = raw_opus_packets(&group).unwrap();
        assert_eq!(packets.len(), 2);
        assert_eq!(packets[0].data.as_ref(), first_raw);
        assert_eq!(packets[0].frame_count, 240);
        assert_eq!(packets[0].id, 10);
        assert_eq!(packets[1].data.as_ref(), second_raw);
        assert_eq!(packets[1].frame_count, 240);
        assert_eq!(packets[1].id, 11);
    }

    #[tokio::test]
    async fn opaque_parts_can_coalesce_twenty_five_ms_units_without_parsing() {
        let captured = Arc::new(CapturingPublisher::default());
        let publisher: Arc<dyn Fmp4PartPublisher> = captured.clone();
        let config = test_config(1, 100, AudioEpochHlsPackaging::Opaque, publisher);
        let mut renditions = HashMap::new();
        let mut expected = Vec::new();

        for epoch_id in 0..20_u64 {
            let payload = vec![epoch_id as u8, 0xa5, 0x5a];
            expected.extend_from_slice(&payload);
            package_group(
                &config,
                &mut renditions,
                decoded_group(
                    epoch_id,
                    epoch_id * 240,
                    0,
                    0,
                    1,
                    AudioPayloadKind::Opus,
                    AudioSampleFormat::Unspecified,
                    Bytes::from(payload),
                ),
            )
            .await
            .unwrap();
        }

        let parts = captured.opaque_parts.lock().unwrap();
        assert_eq!(parts.len(), 1);
        assert_eq!(parts[0].duration_ms, 100);
        assert_eq!(parts[0].audio_units, 20);
        assert_eq!(parts[0].bytes.as_ref(), expected);
        // The part is named by its source: part 0 of session 101, from the
        // sample count of its first epoch to that of its last.
        assert_eq!(parts[0].sequence, 0);
        assert_eq!(
            parts[0].anchor,
            SourceAnchor {
                session_id_ns: 101,
                first_pts_samples: 0,
                last_pts_samples: 19 * 240,
                part_samples: 4_800,
                sample_rate: 48_000,
            }
        );
    }

    /// The AEP1 datagrams of one epoch of one 3,000-byte group.
    fn epoch_datagrams(
        sender: &mut MultichannelAudioSender,
        transport: &MultichannelAudioTransportAdapter,
        epoch_id: u64,
    ) -> Vec<Bytes> {
        let payload = (0..3_000_u32)
            .map(|index| (index % 251) as u8)
            .collect::<Vec<_>>();
        let groups = [MultichannelAudioGroup {
            group_id: 0,
            channel_start: 0,
            channel_count: 2,
            payload_kind: AudioPayloadKind::Pcm,
            sample_format: AudioSampleFormat::S24Le,
            flags: 0,
            payload: &payload,
        }];
        let encoded = sender
            .encode_epoch(MultichannelAudioEpoch {
                session_id: 99,
                config_generation: 1,
                epoch_id,
                pts_samples: epoch_id * 240,
                sample_rate: 48_000,
                frame_count: 240,
                groups: &groups,
            })
            .unwrap();
        transport
            .wrap_epoch(encoded)
            .unwrap()
            .datagrams
            .into_iter()
            .map(|datagram| datagram.payload)
            .collect()
    }

    #[test]
    fn recovered_groups_carry_their_datagram_arrival_times() {
        let transport = MultichannelAudioTransportAdapter::udp(1_200);
        let mut fec = transport.prepare_fec_config(MultichannelAudioFecConfig::default());
        fec.repair_symbols = 2;
        let mut sender = MultichannelAudioSender::new(MultichannelAudioSessionConfig {
            fec,
            ..MultichannelAudioSessionConfig::default()
        });
        let mut receiver = ReceiverState::new(Instant::now());

        // Epoch 7 arrives whole: datagram k at 1,000 + 100 k µs. The group is
        // complete with its last source datagram.
        let datagrams = epoch_datagrams(&mut sender, &transport, 7);
        assert!(datagrams.len() >= 4, "{} datagrams", datagrams.len());
        let mut completed = Vec::new();
        let mut completed_at = None;
        for (index, datagram) in datagrams.iter().enumerate() {
            let payload = transport.payload(datagram).unwrap();
            let received_us = 1_000 + 100 * index as u64;
            let (groups, epoch) = receiver.push(payload, received_us).unwrap();
            if !groups.is_empty() {
                completed_at.get_or_insert(received_us);
                // One group an epoch: the epoch completes with it.
                assert_eq!(epoch.map(|epoch| epoch.epoch_id), Some(7));
            } else {
                assert!(epoch.is_none());
            }
            completed.extend(groups);
        }
        assert_eq!(completed.len(), 1);
        let (group, times) = &completed[0];
        assert_eq!(group.epoch_id, 7);
        assert_eq!(times.first_epoch, Some(7));
        assert_eq!(times.last_epoch, Some(7));
        assert_eq!(times.src_first_us, Some(1_000));
        assert_eq!(times.src_last_us, completed_at);
        assert!(times.recovered_us.unwrap() >= now_unix_us() - 5_000_000);

        // Epoch 8 loses its first source datagram: a repair datagram completes
        // it, and the first arrival is the second source datagram.
        let datagrams = epoch_datagrams(&mut sender, &transport, 8);
        let mut completed = Vec::new();
        for (index, datagram) in datagrams.iter().enumerate().skip(1) {
            let payload = transport.payload(datagram).unwrap();
            let received_us = 5_000 + 100 * index as u64;
            for (group, times) in receiver.push(payload, received_us).unwrap().0 {
                completed.push((group.epoch_id, times, received_us));
            }
        }
        assert_eq!(completed.len(), 1);
        let (epoch_id, times, completed_at) = completed[0];
        assert_eq!(epoch_id, 8);
        assert_eq!(times.src_first_us, Some(5_100));
        assert_eq!(times.src_last_us, Some(completed_at));
        assert!(completed_at > 5_100);
    }

    #[tokio::test]
    async fn opaque_part_spans_the_source_times_of_its_units() {
        let captured = Arc::new(CapturingPublisher::default());
        let publisher: Arc<dyn Fmp4PartPublisher> = captured.clone();
        let config = test_config(1, 25, AudioEpochHlsPackaging::Opaque, publisher);
        let mut renditions = HashMap::new();
        for epoch_id in 0..10_u64 {
            let base = 1_000_000 + epoch_id * 5_000;
            package_group_with_times(
                &config,
                &mut renditions,
                decoded_group(
                    epoch_id,
                    epoch_id * 240,
                    0,
                    0,
                    1,
                    AudioPayloadKind::Opus,
                    AudioSampleFormat::Unspecified,
                    Bytes::from(vec![epoch_id as u8]),
                ),
                PartSourceTimes {
                    src_first_us: Some(base),
                    src_last_us: Some(base + 300),
                    recovered_us: Some(base + 350),
                    ..PartSourceTimes::of_epoch(epoch_id)
                },
            )
            .await
            .unwrap();
        }
        let parts = captured.opaque_parts.lock().unwrap();
        assert_eq!(parts.len(), 2);
        for (index, part) in parts.iter().enumerate() {
            let first_epoch = index as u64 * 5;
            let first = 1_000_000 + first_epoch * 5_000;
            let last = first + 4 * 5_000;
            assert_eq!(
                part.source_times,
                PartSourceTimes {
                    first_epoch: Some(first_epoch),
                    last_epoch: Some(first_epoch + 4),
                    src_first_us: Some(first),
                    src_last_us: Some(last + 300),
                    recovered_us: Some(last + 350),
                }
            );
            // The part's journal events: src_first names its first epoch,
            // the later stages the last, whose last sample ends the part.
            let epochs = part
                .source_times
                .events(part.stream_id, part.sequence)
                .map(|event| (event.stage, event.epoch))
                .collect::<Vec<_>>();
            assert_eq!(
                epochs,
                vec![
                    (crate::hop_journal::HopStage::SrcFirst, Some(first_epoch)),
                    (crate::hop_journal::HopStage::SrcLast, Some(first_epoch + 4)),
                    (
                        crate::hop_journal::HopStage::Recovered,
                        Some(first_epoch + 4)
                    ),
                ]
            );
        }
    }

    #[tokio::test]
    async fn declared_formats_use_independent_streams_and_object_sequences() {
        let captured = Arc::new(CapturingPublisher::default());
        let publisher: Arc<dyn Fmp4PartPublisher> = captured.clone();
        let config = test_config(7, 5, AudioEpochHlsPackaging::Opaque, publisher);
        let mut renditions = HashMap::new();

        package_group(
            &config,
            &mut renditions,
            decoded_group(
                0,
                0,
                0,
                0,
                2,
                AudioPayloadKind::Pcm,
                AudioSampleFormat::S24Le,
                Bytes::from_static(b"first opaque payload"),
            ),
        )
        .await
        .unwrap();
        package_group(
            &config,
            &mut renditions,
            decoded_group(
                1,
                240,
                0,
                0,
                2,
                AudioPayloadKind::Opus,
                AudioSampleFormat::Unspecified,
                Bytes::from_static(b"second opaque payload"),
            ),
        )
        .await
        .unwrap();

        let parts = captured.opaque_parts.lock().unwrap();
        assert_eq!(parts.len(), 2);
        // Each stream numbers its parts from its own sample count: the PCM
        // epoch at 0 is part 0, the Opus epoch at 240 part 1 of its stream.
        assert_eq!(parts[0].sequence, 0);
        assert_eq!(parts[1].sequence, 1);
        assert_eq!(parts[0].stream_id, 2_007);
        assert_eq!(parts[1].stream_id, 1_007);
        assert_eq!(parts[0].bytes.as_ref(), b"first opaque payload");
        assert_eq!(parts[1].bytes.as_ref(), b"second opaque payload");
    }

    #[tokio::test]
    async fn two_hundred_fifty_six_channels_shard_without_media_parsing() {
        let captured = Arc::new(CapturingPublisher::default());
        let publisher: Arc<dyn Fmp4PartPublisher> = captured.clone();
        let config = test_config(100, 5, AudioEpochHlsPackaging::Opaque, publisher);
        let mut renditions = HashMap::new();

        for group_id in 0..32_u16 {
            let payload = Bytes::from(vec![group_id as u8; 37 + usize::from(group_id)]);
            package_group(
                &config,
                &mut renditions,
                decoded_group(
                    0,
                    0,
                    group_id,
                    group_id * 8,
                    8,
                    AudioPayloadKind::Pcm,
                    AudioSampleFormat::S24Le,
                    payload.clone(),
                ),
            )
            .await
            .unwrap();
        }

        let parts = captured.opaque_parts.lock().unwrap();
        assert_eq!(parts.len(), 32);
        let streams = parts
            .iter()
            .map(|part| part.stream_id)
            .collect::<HashSet<_>>();
        assert_eq!(streams, (2_100..2_132).collect::<HashSet<_>>());
        for part in parts.iter() {
            let group_id = usize::try_from(part.stream_id - 2_100).unwrap();
            assert_eq!(part.bytes.as_ref(), vec![group_id as u8; 37 + group_id]);
        }
    }

    #[test]
    fn full_rendition_queue_rejects_newest_group_without_waiting() {
        let (sender, _receiver) = mpsc::channel(1);
        let key = RenditionKey {
            session_id: 99,
            group_id: 7,
            payload_kind: AudioPayloadKind::Opus,
        };
        let group = decoded_group(
            0,
            0,
            7,
            0,
            2,
            AudioPayloadKind::Opus,
            AudioSampleFormat::Unspecified,
            Bytes::from_static(b"opus-packet"),
        );
        try_handoff_rendition_group(&sender, key, (group.clone(), PartSourceTimes::default()))
            .unwrap();

        let error = try_handoff_rendition_group(&sender, key, (group, PartSourceTimes::default()))
            .unwrap_err();
        assert!(error.contains("queue is full"));
        assert_eq!(sender.capacity(), 0);
    }

    fn epoch_config(
        part: AudioPartSize,
        packaging: AudioEpochHlsPackaging,
    ) -> (AudioEpochHlsConfig, Arc<CapturingPublisher>) {
        let captured = Arc::new(CapturingPublisher::default());
        let publisher: Arc<dyn Fmp4PartPublisher> = captured.clone();
        let mut config = test_config(30, 5, packaging, publisher);
        config.part = part;
        (config, captured)
    }

    fn sized_group(
        epoch_id: u64,
        frame_count: u32,
        payload_kind: AudioPayloadKind,
        payload: Vec<u8>,
    ) -> DecodedMultichannelAudioGroup {
        let sample_format = match payload_kind {
            AudioPayloadKind::Opus => AudioSampleFormat::Unspecified,
            _ => AudioSampleFormat::S24Le,
        };
        let mut group = decoded_group(
            epoch_id,
            epoch_id * u64::from(frame_count),
            0,
            0,
            2,
            payload_kind,
            sample_format,
            Bytes::from(payload),
        );
        group.frame_count = frame_count;
        group
    }

    #[tokio::test]
    async fn one_epoch_is_one_part_by_default_with_its_exact_duration() {
        let (config, captured) =
            epoch_config(AudioPartSize::default(), AudioEpochHlsPackaging::Opaque);
        assert_eq!(config.part, AudioPartSize::Epoch);
        let mut renditions = HashMap::new();
        // 2.5 ms Opus epochs (120 samples) and 0.25 ms PCM epochs (12).
        for epoch_id in 0..4_u64 {
            for (frame_count, kind) in [(120, AudioPayloadKind::Opus), (12, AudioPayloadKind::Pcm)]
            {
                package_group(
                    &config,
                    &mut renditions,
                    sized_group(epoch_id, frame_count, kind, vec![epoch_id as u8; 9]),
                )
                .await
                .unwrap();
            }
        }
        let parts = captured.opaque_parts.lock().unwrap();
        assert_eq!(parts.len(), 8);
        let opus = rendition_stream_id(30, 0, AudioPayloadKind::Opus).unwrap();
        for (index, part) in parts.iter().enumerate() {
            assert_eq!(part.sequence, index as u64 / 2);
            assert_eq!(part.audio_units, 1);
            assert_eq!(part.sample_rate, Some(48_000));
            if part.stream_id == opus {
                assert_eq!(part.duration_samples, Some(120));
                assert_eq!(part.duration_us, 2_500);
                assert_eq!(part.duration_ms, 3);
            } else {
                assert_eq!(part.duration_samples, Some(12));
                assert_eq!(part.duration_us, 250);
                assert_eq!(part.duration_ms, 1);
            }
        }
    }

    #[tokio::test]
    async fn one_ms_opus_custom_frames_are_one_part_each_by_default() {
        let (config, captured) =
            epoch_config(AudioPartSize::default(), AudioEpochHlsPackaging::Opaque);
        let mut renditions = HashMap::new();
        for epoch_id in 0..5_u64 {
            package_group(
                &config,
                &mut renditions,
                sized_group(
                    epoch_id,
                    48,
                    AudioPayloadKind::Opus,
                    vec![epoch_id as u8; 32],
                ),
            )
            .await
            .unwrap();
        }
        let parts = captured.opaque_parts.lock().unwrap();
        assert_eq!(parts.len(), 5);
        for (index, part) in parts.iter().enumerate() {
            assert_eq!(part.sequence, index as u64);
            assert_eq!(part.audio_units, 1);
            assert_eq!(part.duration_samples, Some(48));
            assert_eq!(part.duration_us, 1_000);
            assert_eq!(part.duration_ms, 1);
        }
    }

    #[tokio::test]
    async fn opus_custom_parts_hold_whole_frames_or_are_refused() {
        // 64-sample (1.33 ms) frames in 4 ms parts: three frames a part.
        let (config, captured) =
            epoch_config(AudioPartSize::Micros(4_000), AudioEpochHlsPackaging::Opaque);
        let mut renditions = HashMap::new();
        for epoch_id in 0..6_u64 {
            package_group(
                &config,
                &mut renditions,
                sized_group(epoch_id, 64, AudioPayloadKind::Opus, vec![epoch_id as u8]),
            )
            .await
            .unwrap();
        }
        {
            let parts = captured.opaque_parts.lock().unwrap();
            assert_eq!(parts.len(), 2);
            for part in parts.iter() {
                assert_eq!(part.audio_units, 3);
                assert_eq!(part.duration_samples, Some(192));
                assert_eq!(part.duration_us, 4_000);
            }
            assert_eq!(parts[0].bytes.as_ref(), [0, 1, 2]);
        }

        // 64-sample frames do not divide a 5 ms (240-sample) part.
        let (config, captured) =
            epoch_config(AudioPartSize::Micros(5_000), AudioEpochHlsPackaging::Opaque);
        let mut renditions = HashMap::new();
        let error = package_group(
            &config,
            &mut renditions,
            sized_group(0, 64, AudioPayloadKind::Opus, vec![0]),
        )
        .await
        .unwrap_err();
        assert!(
            error.contains("whole number of 64-sample Opus frames"),
            "{error}"
        );
        // The second refusal is reported too, the third is dropped quietly.
        for (epoch_id, reported) in [(1_u64, true), (2, false), (3, true)] {
            let result = package_group(
                &config,
                &mut renditions,
                sized_group(epoch_id, 64, AudioPayloadKind::Opus, vec![0]),
            )
            .await;
            assert_eq!(result.is_err(), reported, "{result:?}");
        }
        assert!(captured.opaque_parts.lock().unwrap().is_empty());

        // 120-sample standard frames fit 5 ms parts; 72-sample frames fit
        // 3 ms parts.
        assert!(opus_frame_fits_packaging(
            AudioEpochHlsPackaging::Opaque,
            AudioPartSize::Micros(5_000),
            48_000,
            120
        )
        .is_ok());
        assert!(opus_frame_fits_packaging(
            AudioEpochHlsPackaging::Opaque,
            AudioPartSize::Samples(144),
            48_000,
            72
        )
        .is_ok());
        assert!(opus_frame_fits_packaging(
            AudioEpochHlsPackaging::Opaque,
            AudioPartSize::Epoch,
            48_000,
            72
        )
        .is_ok());
    }

    #[tokio::test]
    async fn fmp4_packaging_refuses_opus_custom_frames() {
        let (config, captured) =
            epoch_config(AudioPartSize::default(), AudioEpochHlsPackaging::Fmp4);
        let mut renditions = HashMap::new();
        for frame_count in [48_u32, 64, 72] {
            let error = package_group(
                &config,
                &mut renditions,
                sized_group(0, frame_count, AudioPayloadKind::Opus, vec![0; 32]),
            )
            .await
            .unwrap_err();
            assert!(error.contains("Opus Custom"), "{error}");
            assert!(error.contains("--daw-hls-packaging opaque"), "{error}");
            renditions.clear();
        }
        assert!(captured.fmp4_parts.lock().unwrap().is_empty());
        assert!(opus_frame_fits_packaging(
            AudioEpochHlsPackaging::Fmp4,
            AudioPartSize::Epoch,
            48_000,
            120
        )
        .is_ok());
    }

    #[tokio::test]
    async fn a_one_epoch_part_is_the_epoch_payload_without_a_copy() {
        let (config, captured) =
            epoch_config(AudioPartSize::default(), AudioEpochHlsPackaging::Opaque);
        let mut renditions = HashMap::new();
        let mut payloads = Vec::new();
        for epoch_id in 0..3_u64 {
            let group = sized_group(
                epoch_id,
                48,
                AudioPayloadKind::Pcm,
                vec![epoch_id as u8; 288],
            );
            payloads.push(group.payload.clone());
            package_group(&config, &mut renditions, group)
                .await
                .unwrap();
        }
        let parts = captured.opaque_parts.lock().unwrap();
        assert_eq!(parts.len(), 3);
        for (part, payload) in parts.iter().zip(&payloads) {
            // The same buffer, not a copy of it.
            assert_eq!(part.bytes.as_ptr(), payload.as_ptr());
            assert_eq!(part.bytes, *payload);
            assert_eq!(part.audio_units, 1);
            assert_eq!(part.duration_samples, Some(48));
        }
    }

    #[tokio::test]
    async fn a_part_in_samples_takes_whole_epochs_and_flac_keeps_its_floor() {
        let (config, captured) =
            epoch_config(AudioPartSize::Samples(48), AudioEpochHlsPackaging::Opaque);
        let mut renditions = HashMap::new();
        for epoch_id in 0..8_u64 {
            package_group(
                &config,
                &mut renditions,
                sized_group(epoch_id, 12, AudioPayloadKind::Pcm, vec![epoch_id as u8]),
            )
            .await
            .unwrap();
        }
        {
            let parts = captured.opaque_parts.lock().unwrap();
            assert_eq!(parts.len(), 2);
            for (number, part) in parts.iter().enumerate() {
                assert_eq!(part.audio_units, 4);
                assert_eq!(part.duration_samples, Some(48));
                assert_eq!(part.duration_us, 1_000);
                assert_eq!(part.sequence, number as u64);
                assert_eq!(part.anchor.part_samples, 48);
                assert_eq!(part.anchor.first_pts_samples, number as u64 * 48);
                assert_eq!(part.anchor.last_pts_samples, number as u64 * 48 + 36);
            }
            assert_eq!(parts[0].bytes.as_ref(), [0, 1, 2, 3]);
        }

        // 16-sample FLAC blocks with a 12-sample setting: never under 32.
        let (config, captured) =
            epoch_config(AudioPartSize::Samples(12), AudioEpochHlsPackaging::Opaque);
        let mut renditions = HashMap::new();
        for epoch_id in 0..4_u64 {
            package_group(
                &config,
                &mut renditions,
                sized_group(epoch_id, 16, AudioPayloadKind::Flac, vec![epoch_id as u8]),
            )
            .await
            .unwrap();
        }
        let parts = captured.opaque_parts.lock().unwrap();
        assert_eq!(parts.len(), 2);
        assert!(parts
            .iter()
            .all(|part| part.duration_samples == Some(32) && part.duration_us == 667));
    }

    #[tokio::test]
    async fn fmp4_pcm_epochs_of_one_ms_are_one_part_each() {
        let (config, captured) =
            epoch_config(AudioPartSize::default(), AudioEpochHlsPackaging::Fmp4);
        let mut renditions = HashMap::new();
        for epoch_id in 0..3_u64 {
            package_group(
                &config,
                &mut renditions,
                sized_group(
                    epoch_id,
                    48,
                    AudioPayloadKind::Pcm,
                    vec![epoch_id as u8; 48 * 2 * 3],
                ),
            )
            .await
            .unwrap();
        }
        let parts = captured.fmp4_parts.lock().unwrap();
        assert_eq!(parts.len(), 3);
        for part in parts.iter() {
            assert_eq!(part.audio_units, 1);
            assert_eq!(part.duration_us, 1_000);
            assert_eq!(part.duration_ms, 1);
            assert_eq!(part.duration_samples, Some(48));
            assert_eq!(part.sample_rate, Some(48_000));
        }
    }

    #[tokio::test]
    async fn fmp4_pcm_epochs_under_one_ms_join_into_whole_ms_parts() {
        // The boxer's timescale is 1,000: an access unit of 12 samples is
        // 0 ms and was dropped, so 0.25 ms epochs published nothing. Four of
        // them now make one exact 1 ms part, two of 24 samples one more.
        let (config, captured) =
            epoch_config(AudioPartSize::default(), AudioEpochHlsPackaging::Fmp4);
        let mut renditions = HashMap::new();
        for epoch_id in 0..8_u64 {
            let mut group = sized_group(
                epoch_id,
                12,
                AudioPayloadKind::Pcm,
                vec![epoch_id as u8; 12 * 2 * 3],
            );
            group.pts_samples = epoch_id * 12;
            let times = PartSourceTimes {
                src_first_us: Some(1_000 + epoch_id),
                src_last_us: Some(2_000 + epoch_id),
                recovered_us: Some(3_000 + epoch_id),
                ..PartSourceTimes::of_epoch(epoch_id)
            };
            package_group_with_times(&config, &mut renditions, group, times)
                .await
                .unwrap();
        }
        {
            let parts = captured.fmp4_parts.lock().unwrap();
            assert_eq!(parts.len(), 2);
            for (index, part) in parts.iter().enumerate() {
                assert_eq!(part.audio_units, 1);
                assert_eq!(part.duration_samples, Some(48));
                assert_eq!(part.duration_us, 1_000);
                assert_eq!(part.duration_ms, 1);
                // The four epochs' samples, in order, whole.
                let first = index as u8 * 4;
                let joined = (first..first + 4)
                    .flat_map(|epoch| vec![epoch; 12 * 2 * 3])
                    .collect::<Vec<_>>();
                assert!(part
                    .bytes
                    .windows(joined.len())
                    .any(|window| window == joined.as_slice()));
                // src_first of the first epoch, the rest of the last.
                assert_eq!(part.source_times.first_epoch, Some(u64::from(first)));
                assert_eq!(part.source_times.last_epoch, Some(u64::from(first) + 3));
                assert_eq!(
                    part.source_times.src_first_us,
                    Some(1_000 + u64::from(first))
                );
                assert_eq!(
                    part.source_times.src_last_us,
                    Some(2_003 + u64::from(first))
                );
            }
        }

        // 24-sample epochs: two make 1 ms (one alone was rounded to 1 ms).
        // At the end of the stream a partial unit is published, not lost.
        let (config, captured) =
            epoch_config(AudioPartSize::default(), AudioEpochHlsPackaging::Fmp4);
        let mut renditions = HashMap::new();
        for epoch_id in 0..5_u64 {
            let mut group = sized_group(
                epoch_id,
                24,
                AudioPayloadKind::Pcm,
                vec![epoch_id as u8; 24 * 2 * 3],
            );
            group.pts_samples = epoch_id * 24;
            package_group(&config, &mut renditions, group)
                .await
                .unwrap();
        }
        assert_eq!(captured.fmp4_parts.lock().unwrap().len(), 2);
        for rendition in renditions.values_mut() {
            rendition.packager.finish().await.unwrap();
        }
        let parts = captured.fmp4_parts.lock().unwrap();
        assert_eq!(parts.len(), 3);
        assert!(parts[..2]
            .iter()
            .all(|part| part.duration_samples == Some(48) && part.duration_us == 1_000));
        assert_eq!(parts[2].duration_samples, Some(24));
    }

    #[test]
    fn whole_milliseconds_of_samples() {
        assert!(whole_ms(48, 48_000));
        assert!(!whole_ms(12, 48_000));
        assert!(!whole_ms(24, 48_000));
        assert!(whole_ms(441, 44_100));
        assert!(!whole_ms(48, 44_100));
        assert!(!whole_ms(48, 0));
        assert_eq!(max_pending_pcm_samples(48_000), 4_800);
    }

    #[test]
    fn queues_and_windows_are_sized_in_time() {
        // One second of a rendition's epochs, never under 1,024 places.
        assert_eq!(rendition_queue_capacity(12, 48_000), 4_000);
        assert_eq!(rendition_queue_capacity(48, 48_000), 1_024);
        assert_eq!(rendition_queue_capacity(240, 48_000), 1_024);
        // 320 ms of 250 µs epochs may wait for repair.
        assert_eq!(aep1_session_config().max_inflight_epochs, 1_280);
        let queue_us = DEFAULT_AUDIO_EPOCH_HLS_QUEUE_CAPACITY as u64 * 1_000_000 / 60_000;
        assert!(queue_us >= 320_000, "{queue_us} µs");
    }

    #[test]
    fn block_first_arrivals_survive_interleaved_epochs() {
        let mut receiver = ReceiverState::new(Instant::now());
        // Two hundred blocks in flight at once, as 0.25 ms epochs with a
        // late repair make: each keeps its own first arrival.
        for block in 0..200_u32 {
            assert_eq!(
                receiver.observe_block(block, 1_000 + u64::from(block)),
                1_000 + u64::from(block)
            );
        }
        for block in 0..200_u32 {
            assert_eq!(
                receiver.observe_block(block, 9_999),
                1_000 + u64::from(block)
            );
        }
        // A block id that reuses a slot much later is a new block.
        let reused = FIRST_ARRIVAL_BLOCKS as u32;
        assert_eq!(receiver.observe_block(reused, 5), 5);
        assert_eq!(receiver.observe_block(0, 7), 7);
    }

    /// AEP1 datagrams of one epoch whose one group is canonical SoundKit v2
    /// PCM: 48 samples, 1 ms.
    fn soundkit_pcm_epoch(
        sender: &mut MultichannelAudioSender,
        transport: &MultichannelAudioTransportAdapter,
        epoch_id: u64,
    ) -> Vec<Bytes> {
        let pcm = vec![epoch_id as u8; 48 * 2 * 2];
        let framed = crate::soundkit_v2::frame_soundkit_v2_packet(
            crate::soundkit_v2::SoundKitV2PacketMetadata {
                encoding: EncodingFlag::PCMSigned,
                frame_count: 48,
                sample_rate: 48_000,
                channels: 2,
                bits_per_sample: 16,
                endianness: frame_header::Endianness::LittleEndian,
                id: Some(epoch_id),
                pts: Some(epoch_id * 48),
            },
            &pcm,
        )
        .unwrap();
        let groups = [MultichannelAudioGroup {
            group_id: 0,
            channel_start: 0,
            channel_count: 2,
            payload_kind: AudioPayloadKind::SoundKitV2,
            sample_format: AudioSampleFormat::Unspecified,
            flags: 0,
            payload: framed.as_ref(),
        }];
        let encoded = sender
            .encode_epoch(MultichannelAudioEpoch {
                session_id: 77,
                config_generation: 1,
                epoch_id,
                pts_samples: epoch_id * 48,
                sample_rate: 48_000,
                frame_count: 48,
                groups: &groups,
            })
            .unwrap();
        transport
            .wrap_epoch(encoded)
            .unwrap()
            .datagrams
            .into_iter()
            .map(|datagram| datagram.payload)
            .collect()
    }

    #[tokio::test]
    async fn the_worker_shares_each_recovered_epoch_and_never_waits_for_its_sink() {
        let transport = MultichannelAudioTransportAdapter::udp(1_200);
        let fec = transport.prepare_fec_config(MultichannelAudioFecConfig::default());
        let mut sender = MultichannelAudioSender::new(MultichannelAudioSessionConfig {
            fec,
            ..MultichannelAudioSessionConfig::default()
        });
        let peer: SocketAddr = "127.0.0.1:41077".parse().unwrap();

        // The sink takes every epoch, of every format, before the format
        // filter: FLAC only is published here, the PCM epochs still go on.
        let captured = Arc::new(CapturingPublisher::default());
        let publisher: Arc<dyn Fmp4PartPublisher> = captured.clone();
        let (sink_tx, mut sink_rx) = mpsc::channel(16);
        let config = test_config_with_formats(
            50,
            5,
            AudioEpochHlsPackaging::Opaque,
            vec![AudioPayloadKind::Flac],
            publisher,
        )
        .with_recovered_epoch_sink(sink_tx);
        let (tx, rx) = channel(256);
        let (_shutdown_tx, shutdown_rx) = watch::channel(());
        let worker = tokio::spawn(run_audio_epoch_hls_worker(config, rx, shutdown_rx));
        let mut sent = Vec::new();
        for epoch_id in 0..3 {
            for datagram in soundkit_pcm_epoch(&mut sender, &transport, epoch_id) {
                sent.push(datagram.clone());
                tx.send(AudioEpochHlsDatagram {
                    peer,
                    bytes: datagram,
                    received_unix_us: now_unix_us(),
                })
                .await
                .unwrap();
            }
        }
        drop(tx);
        worker.await.unwrap();
        assert!(captured.opaque_parts.lock().unwrap().is_empty());
        let mut shared = Vec::new();
        while let Some(recovered) = sink_rx.recv().await {
            shared.push(recovered);
        }
        assert_eq!(
            shared
                .iter()
                .map(|recovered| recovered.epoch.epoch_id)
                .collect::<Vec<_>>(),
            vec![0, 1, 2]
        );
        // The same epochs that a receiver of its own recovers from the
        // datagrams, group bytes included.
        let mut own = MultichannelAudioReceiver::new(MultichannelAudioSessionConfig::default());
        let mut expected = Vec::new();
        for datagram in &sent {
            if let Some(epoch) = own
                .push_datagram(transport.payload(datagram).unwrap())
                .unwrap()
                .completed_epoch
            {
                expected.push(epoch);
            }
        }
        assert_eq!(expected.len(), 3);
        for (recovered, expected) in shared.iter().zip(&expected) {
            assert_eq!(recovered.peer, peer);
            assert_eq!(recovered.epoch, *expected);
        }

        // A sink that is full does not hold the worker back: PCM parts are
        // published while two of three epochs are refused by the sink.
        let dropped_before = worker_stats().recovered_epochs_dropped;
        let captured = Arc::new(CapturingPublisher::default());
        let publisher: Arc<dyn Fmp4PartPublisher> = captured.clone();
        let (sink_tx, mut sink_rx) = mpsc::channel(1);
        let mut config = test_config_with_formats(
            60,
            5,
            AudioEpochHlsPackaging::Opaque,
            vec![AudioPayloadKind::Pcm],
            publisher,
        )
        .with_recovered_epoch_sink(sink_tx);
        config.part = AudioPartSize::Epoch;
        let (tx, rx) = channel(256);
        let (_shutdown_tx, shutdown_rx) = watch::channel(());
        let worker = tokio::spawn(run_audio_epoch_hls_worker(config, rx, shutdown_rx));
        for epoch_id in 3..6 {
            for datagram in soundkit_pcm_epoch(&mut sender, &transport, epoch_id) {
                tx.send(AudioEpochHlsDatagram {
                    peer,
                    bytes: datagram,
                    received_unix_us: now_unix_us(),
                })
                .await
                .unwrap();
            }
        }
        drop(tx);
        tokio::time::timeout(Duration::from_secs(5), worker)
            .await
            .expect("the worker does not wait for a full sink")
            .unwrap();
        assert_eq!(captured.opaque_parts.lock().unwrap().len(), 3);
        assert_eq!(sink_rx.recv().await.unwrap().epoch.epoch_id, 3);
        assert!(sink_rx.recv().await.is_none());
        assert!(worker_stats().recovered_epochs_dropped >= dropped_before + 2);
    }

    /// Epoch `index` of session `session_id`: `frame_count` samples at
    /// `index * frame_count`, its payload a pattern of the index.
    fn session_group(
        session_id: u64,
        index: u64,
        frame_count: u32,
        payload_kind: AudioPayloadKind,
    ) -> DecodedMultichannelAudioGroup {
        let payload = (0..40_u64)
            .map(|byte| (index * 7 + byte * 13 + 1) as u8)
            .collect();
        let mut group = sized_group(index, frame_count, payload_kind, payload);
        group.session_id = session_id;
        group
    }

    /// What names a part and what it carries: everything of it but this
    /// host's clocks.
    #[derive(Debug, Clone, PartialEq, Eq)]
    struct PartIdentity {
        stream_id: u64,
        source_epoch: Option<u64>,
        sequence: u64,
        duration_us: u64,
        duration_samples: Option<u64>,
        sample_rate: Option<u32>,
        audio_units: usize,
        anchor: SourceAnchor,
        bytes: Bytes,
    }

    fn identities(captured: &CapturingPublisher) -> Vec<PartIdentity> {
        captured
            .opaque_parts
            .lock()
            .unwrap()
            .iter()
            .map(|part| PartIdentity {
                stream_id: part.stream_id,
                source_epoch: part.source_epoch,
                sequence: part.sequence,
                duration_us: part.duration_us,
                duration_samples: part.duration_samples,
                sample_rate: part.sample_rate,
                audio_units: part.audio_units,
                anchor: part.anchor,
                bytes: part.bytes.clone(),
            })
            .collect()
    }

    fn counts(config: &AudioEpochHlsConfig, payload_kind: AudioPayloadKind) -> OpaqueStreamCounts {
        let stream_id = rendition_stream_id(config.base_stream_id, 0, payload_kind).unwrap();
        config
            .opaque_streams
            .snapshot()
            .into_iter()
            .find(|counts| counts.stream_id == stream_id)
            .unwrap_or_default()
    }

    const SESSION: u64 = 1_700_000_000_000_000_000;

    #[test]
    fn the_number_window_remembers_what_was_handed() {
        let mut window = NumberWindow::new();
        assert_eq!(window.verdict(0), NumberVerdict::New);
        assert_eq!(window.verdict(u64::MAX), NumberVerdict::New);
        // A session joined at number 500: everything below is a late fill
        // until it is handed, within the window.
        window.hand(500);
        assert_eq!(window.verdict(500), NumberVerdict::Reused);
        assert_eq!(window.verdict(501), NumberVerdict::New);
        assert_eq!(window.verdict(499), NumberVerdict::LateFill);
        window.hand(499);
        assert_eq!(window.verdict(499), NumberVerdict::Reused);
        // 502 is passed over, then handed late, once.
        window.hand(501);
        window.hand(503);
        assert_eq!(window.verdict(500), NumberVerdict::Reused);
        assert_eq!(window.verdict(501), NumberVerdict::Reused);
        assert_eq!(window.verdict(502), NumberVerdict::LateFill);
        window.hand(502);
        assert_eq!(window.verdict(502), NumberVerdict::Reused);
        assert_eq!(window.verdict(503), NumberVerdict::Reused);

        // The window is the 8,192 numbers below the highest: one more is
        // too far below to tell, and counts as reused.
        let mut window = NumberWindow::new();
        window.hand(10);
        window.hand(11);
        window.hand(11 + NUMBER_WINDOW_PARTS);
        assert_eq!(window.verdict(11), NumberVerdict::Reused);
        assert_eq!(window.verdict(12), NumberVerdict::LateFill);
        assert_eq!(window.verdict(10), NumberVerdict::Reused);
        window.hand(12 + NUMBER_WINDOW_PARTS);
        // 11 left the window; 12 is its lowest number, still not handed.
        assert_eq!(window.verdict(11), NumberVerdict::Reused);
        assert_eq!(window.verdict(12), NumberVerdict::LateFill);
        window.hand(12);
        assert_eq!(window.verdict(12), NumberVerdict::Reused);

        // Every number keeps its own answer as the ring goes round, for
        // steps of one and of several.
        let mut window = NumberWindow::new();
        let mut handed = std::collections::BTreeSet::new();
        let mut number = 0_u64;
        for step in 0..40_000_u64 {
            number += 1 + (step % 7 == 0) as u64 * (step % 5);
            window.hand(number);
            handed.insert(number);
        }
        for below in 0..=NUMBER_WINDOW_PARTS + 4 {
            let candidate = number - below;
            let expected = if below > NUMBER_WINDOW_PARTS || handed.contains(&candidate) {
                NumberVerdict::Reused
            } else {
                NumberVerdict::LateFill
            };
            assert_eq!(window.verdict(candidate), expected, "{below} below");
        }
        // A jump of more than a window forgets the old one: all below the
        // new highest inside the window are late fills.
        window.hand(number + 3 * NUMBER_WINDOW_PARTS);
        assert_eq!(
            window.verdict(number + 2 * NUMBER_WINDOW_PARTS),
            NumberVerdict::LateFill
        );
        assert_eq!(window.verdict(number), NumberVerdict::Reused);
    }

    #[tokio::test]
    async fn two_packagers_one_joining_at_epoch_500_publish_equal_parts() {
        // (part, epoch samples, format, first epoch the late packager
        // gets, its first part).
        for (part, frame_count, kind, joins_at, first_part) in [
            (
                AudioPartSize::Epoch,
                48_u32,
                AudioPayloadKind::Pcm,
                500_u64,
                500_u64,
            ),
            (AudioPartSize::Epoch, 48, AudioPayloadKind::Opus, 500, 500),
            (AudioPartSize::Epoch, 120, AudioPayloadKind::Opus, 500, 500),
            (AudioPartSize::Epoch, 240, AudioPayloadKind::Flac, 500, 500),
            // Parts of five epochs: one that joins inside part 100 (epochs
            // 500..=504) publishes from part 101.
            (
                AudioPartSize::Samples(240),
                48,
                AudioPayloadKind::Pcm,
                502,
                101,
            ),
        ] {
            let (early_config, early) = epoch_config(part, AudioEpochHlsPackaging::Opaque);
            let (late_config, late) = epoch_config(part, AudioEpochHlsPackaging::Opaque);
            let mut early_renditions = HashMap::new();
            let mut late_renditions = HashMap::new();
            for index in 0..1_000_u64 {
                let group = session_group(SESSION, index, frame_count, kind);
                if index >= joins_at {
                    package_group(&late_config, &mut late_renditions, group.clone())
                        .await
                        .unwrap();
                }
                package_group(&early_config, &mut early_renditions, group)
                    .await
                    .unwrap();
            }
            let early = identities(&early);
            let late = identities(&late);
            let epochs_per_part =
                part_samples(part, 48_000, frame_count, kind == AudioPayloadKind::Flac)
                    / u64::from(frame_count);
            assert_eq!(early.len() as u64, 1_000 / epochs_per_part);
            assert_eq!(late[0].sequence, first_part, "{part} {frame_count}");
            assert_eq!(
                late.len() as u64,
                1_000 / epochs_per_part - first_part,
                "{part} {frame_count}"
            );
            // Every part of the late packager is the early one's part of
            // that number, field for field and byte for byte.
            for part in &late {
                assert_eq!(part, &early[part.sequence as usize]);
                assert_eq!(part.source_epoch, Some(SESSION / 1_000));
                assert_eq!(
                    part.anchor.first_pts_samples,
                    part.sequence * epochs_per_part * u64::from(frame_count)
                );
            }
        }
    }

    #[tokio::test]
    async fn a_lost_epoch_leaves_the_other_numbers_alone() {
        for part in [AudioPartSize::Epoch, AudioPartSize::Samples(240)] {
            let (whole_config, whole) = epoch_config(part, AudioEpochHlsPackaging::Opaque);
            let (lossy_config, lossy) = epoch_config(part, AudioEpochHlsPackaging::Opaque);
            let mut whole_renditions = HashMap::new();
            let mut lossy_renditions = HashMap::new();
            for index in 0..40_u64 {
                let group = session_group(SESSION, index, 48, AudioPayloadKind::Pcm);
                if index != 7 {
                    package_group(&lossy_config, &mut lossy_renditions, group.clone())
                        .await
                        .unwrap();
                }
                package_group(&whole_config, &mut whole_renditions, group)
                    .await
                    .unwrap();
            }
            let whole = identities(&whole);
            let lossy = identities(&lossy);
            // Epoch 7 is part 7 of one-epoch parts and in part 1 of
            // five-epoch parts: that number is a hole, and no other part
            // moved or changed.
            let hole = if part == AudioPartSize::Epoch { 7 } else { 1 };
            assert_eq!(lossy.len(), whole.len() - 1);
            assert!(lossy.iter().all(|part| part.sequence != hole));
            for part in &lossy {
                assert_eq!(part, &whole[part.sequence as usize]);
            }
            let skipped = u64::from(part != AudioPartSize::Epoch);
            assert_eq!(
                counts(&lossy_config, AudioPayloadKind::Pcm),
                OpaqueStreamCounts {
                    stream_id: 2_030,
                    parts_skipped: skipped,
                    ..OpaqueStreamCounts::default()
                }
            );
            assert_eq!(
                counts(&whole_config, AudioPayloadKind::Pcm).parts_skipped,
                0
            );
        }
    }

    #[tokio::test]
    async fn a_part_of_five_epochs_is_cut_on_absolute_boundaries() {
        let (config, captured) =
            epoch_config(AudioPartSize::Samples(240), AudioEpochHlsPackaging::Opaque);
        let mut renditions = HashMap::new();
        let payload = |index: u64| session_group(SESSION, index, 48, AudioPayloadKind::Pcm).payload;
        // The first epoch seen is epoch 3: epochs 3 and 4 are the end of
        // part 0, which is never whole. Part 1 is epochs 5..=9, wherever
        // the packager started.
        for index in 3..10_u64 {
            package_group(
                &config,
                &mut renditions,
                session_group(SESSION, index, 48, AudioPayloadKind::Pcm),
            )
            .await
            .unwrap();
        }
        // Part 2 arrives with its epochs out of order (11 completed by a
        // late repair, after 12; 14 before 13): its bytes are in the order
        // of the source all the same.
        for index in [10_u64, 12, 11, 14, 13] {
            package_group(
                &config,
                &mut renditions,
                session_group(SESSION, index, 48, AudioPayloadKind::Pcm),
            )
            .await
            .unwrap();
        }
        let parts = identities(&captured);
        assert_eq!(parts.len(), 2);
        for (part, number) in parts.iter().zip([1_u64, 2]) {
            let first = number * 5;
            assert_eq!(part.sequence, number);
            assert_eq!(part.audio_units, 5);
            assert_eq!(part.duration_samples, Some(240));
            assert_eq!(part.duration_us, 5_000);
            assert_eq!(
                part.anchor,
                SourceAnchor {
                    session_id_ns: SESSION,
                    first_pts_samples: first * 48,
                    last_pts_samples: (first + 4) * 48,
                    part_samples: 240,
                    sample_rate: 48_000,
                }
            );
            let expected = (first..first + 5)
                .flat_map(|index| payload(index))
                .collect::<Vec<_>>();
            assert_eq!(part.bytes.as_ref(), expected);
        }
        assert_eq!(counts(&config, AudioPayloadKind::Pcm).parts_skipped, 1);
    }

    #[tokio::test]
    async fn a_short_part_is_never_flushed() {
        let (config, captured) =
            epoch_config(AudioPartSize::Samples(240), AudioEpochHlsPackaging::Opaque);
        let mut renditions = HashMap::new();
        // Seven epochs: one part of five and two epochs of the next.
        for index in 0..7_u64 {
            package_group(
                &config,
                &mut renditions,
                session_group(SESSION, index, 48, AudioPayloadKind::Pcm),
            )
            .await
            .unwrap();
        }
        assert_eq!(captured.opaque_parts.lock().unwrap().len(), 1);
        // The session ends (the worker stops, or retires the rendition).
        for rendition in renditions.values_mut() {
            rendition.packager.finish().await.unwrap();
        }
        let parts = identities(&captured);
        assert_eq!(parts.len(), 1);
        assert_eq!(parts[0].sequence, 0);
        assert_eq!(parts[0].audio_units, 5);
        // And a worker that drains its queue and stops publishes no more.
        let (config, captured) =
            epoch_config(AudioPartSize::Samples(240), AudioEpochHlsPackaging::Opaque);
        let (sender, receiver) = mpsc::channel(16);
        let key = RenditionKey {
            session_id: SESSION,
            group_id: 0,
            payload_kind: AudioPayloadKind::Pcm,
        };
        for index in 0..7_u64 {
            sender
                .send((
                    session_group(SESSION, index, 48, AudioPayloadKind::Pcm),
                    PartSourceTimes::default(),
                ))
                .await
                .unwrap();
        }
        drop(sender);
        run_rendition_worker(config, key, receiver).await;
        assert_eq!(captured.opaque_parts.lock().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn a_late_epoch_fills_its_own_number() {
        let (config, captured) = epoch_config(AudioPartSize::Epoch, AudioEpochHlsPackaging::Opaque);
        let (ordered_config, ordered) =
            epoch_config(AudioPartSize::Epoch, AudioEpochHlsPackaging::Opaque);
        let mut renditions = HashMap::new();
        let mut ordered_renditions = HashMap::new();
        // Epoch 2 is completed by a late repair, after epochs 3 and 4.
        for index in [0_u64, 1, 3, 4, 2, 5] {
            package_group(
                &config,
                &mut renditions,
                session_group(SESSION, index, 48, AudioPayloadKind::Opus),
            )
            .await
            .unwrap();
        }
        for index in 0..6_u64 {
            package_group(
                &ordered_config,
                &mut ordered_renditions,
                session_group(SESSION, index, 48, AudioPayloadKind::Opus),
            )
            .await
            .unwrap();
        }
        let parts = identities(&captured);
        let ordered = identities(&ordered);
        assert_eq!(
            parts.iter().map(|part| part.sequence).collect::<Vec<_>>(),
            vec![0, 1, 3, 4, 2, 5]
        );
        // The late part is the part a packager that had it in time made.
        for part in &parts {
            assert_eq!(part, &ordered[part.sequence as usize]);
        }
        assert_eq!(
            counts(&config, AudioPayloadKind::Opus),
            OpaqueStreamCounts {
                stream_id: 1_030,
                late_fills: 1,
                ..OpaqueStreamCounts::default()
            }
        );
        assert_eq!(
            counts(&ordered_config, AudioPayloadKind::Opus).late_fills,
            0
        );

        // A part of several epochs is filled late too, when no later part
        // is open: here part 1 after part 2 was published whole.
        let (config, captured) =
            epoch_config(AudioPartSize::Samples(240), AudioEpochHlsPackaging::Opaque);
        let mut renditions = HashMap::new();
        for index in (0..5_u64).chain(10..15).chain(5..10) {
            package_group(
                &config,
                &mut renditions,
                session_group(SESSION, index, 48, AudioPayloadKind::Pcm),
            )
            .await
            .unwrap();
        }
        assert_eq!(
            identities(&captured)
                .iter()
                .map(|part| part.sequence)
                .collect::<Vec<_>>(),
            vec![0, 2, 1]
        );
        assert_eq!(counts(&config, AudioPayloadKind::Pcm).late_fills, 1);
    }

    #[tokio::test]
    async fn a_reused_number_is_not_published() {
        let (config, captured) = epoch_config(AudioPartSize::Epoch, AudioEpochHlsPackaging::Opaque);
        let mut renditions = HashMap::new();
        for index in 0..10_u64 {
            package_group(
                &config,
                &mut renditions,
                session_group(SESSION, index, 48, AudioPayloadKind::Pcm),
            )
            .await
            .unwrap();
        }
        // The source steps its sample count back (a DAW that loops, or
        // follows a locate): the same numbers with other audio. Not an
        // error, and nothing is published.
        for index in 5..8_u64 {
            let mut group = session_group(SESSION, index, 48, AudioPayloadKind::Pcm);
            group.payload = Bytes::from(vec![0xee; 40]);
            package_group(&config, &mut renditions, group)
                .await
                .unwrap();
        }
        assert_eq!(captured.opaque_parts.lock().unwrap().len(), 10);
        assert_eq!(counts(&config, AudioPayloadKind::Pcm).numbers_reused, 3);

        // A step forward of more than the window, then numbers far below
        // it: too old to tell whether they were published, so they are not.
        package_group(
            &config,
            &mut renditions,
            session_group(SESSION, 20_000, 48, AudioPayloadKind::Pcm),
        )
        .await
        .unwrap();
        for index in [10_u64, 11, 20_000 - NUMBER_WINDOW_PARTS - 1] {
            package_group(
                &config,
                &mut renditions,
                session_group(SESSION, index, 48, AudioPayloadKind::Pcm),
            )
            .await
            .unwrap();
        }
        // Inside the window a number that was never published is a late
        // fill, published once.
        for _ in 0..2 {
            package_group(
                &config,
                &mut renditions,
                session_group(SESSION, 19_000, 48, AudioPayloadKind::Pcm),
            )
            .await
            .unwrap();
        }
        let parts = identities(&captured);
        assert_eq!(parts.len(), 12);
        assert_eq!(parts[10].sequence, 20_000);
        assert_eq!(parts[11].sequence, 19_000);
        assert_eq!(
            counts(&config, AudioPayloadKind::Pcm),
            OpaqueStreamCounts {
                stream_id: 2_030,
                numbers_reused: 7,
                late_fills: 1,
                ..OpaqueStreamCounts::default()
            }
        );

        // A part of several epochs: an epoch of a part already published
        // does not open it again.
        let (config, captured) =
            epoch_config(AudioPartSize::Samples(240), AudioEpochHlsPackaging::Opaque);
        let mut renditions = HashMap::new();
        for index in (0..12_u64).chain(5..10).chain(12..15) {
            package_group(
                &config,
                &mut renditions,
                session_group(SESSION, index, 48, AudioPayloadKind::Pcm),
            )
            .await
            .unwrap();
        }
        assert_eq!(
            identities(&captured)
                .iter()
                .map(|part| part.sequence)
                .collect::<Vec<_>>(),
            vec![0, 1, 2]
        );
        let counts = counts(&config, AudioPayloadKind::Pcm);
        assert_eq!(counts.numbers_reused, 5);
        // Part 2, open with epochs 10 and 11, was not dropped for them.
        assert_eq!(counts.parts_skipped, 0);
    }

    #[tokio::test]
    async fn a_changed_frame_count_publishes_nothing_until_a_new_session() {
        let (config, captured) = epoch_config(AudioPartSize::Epoch, AudioEpochHlsPackaging::Opaque);
        let mut renditions = HashMap::new();
        for index in 0..4_u64 {
            package_group(
                &config,
                &mut renditions,
                session_group(SESSION, index, 48, AudioPayloadKind::Pcm),
            )
            .await
            .unwrap();
        }
        // The source doubles its epoch inside the session: from sample 192,
        // epochs of 96 samples. Their numbers (192 / 96 = 2, 3, ...) would
        // name other samples than parts 2 and 3 already published.
        for index in 2..6_u64 {
            package_group(
                &config,
                &mut renditions,
                session_group(SESSION, index, 96, AudioPayloadKind::Pcm),
            )
            .await
            .unwrap();
        }
        // A rate change is the same.
        let mut resampled = session_group(SESSION, 12, 48, AudioPayloadKind::Pcm);
        resampled.sample_rate = 44_100;
        package_group(&config, &mut renditions, resampled)
            .await
            .unwrap();
        assert_eq!(captured.opaque_parts.lock().unwrap().len(), 4);
        assert_eq!(
            counts(&config, AudioPayloadKind::Pcm),
            OpaqueStreamCounts {
                stream_id: 2_030,
                geometry_changed: 5,
                ..OpaqueStreamCounts::default()
            }
        );
        // A new session takes the new epoch size, from its own first epoch.
        let later = SESSION + 5_000_000_000;
        for index in 0..3_u64 {
            package_group(
                &config,
                &mut renditions,
                session_group(later, index, 96, AudioPayloadKind::Pcm),
            )
            .await
            .unwrap();
        }
        let parts = identities(&captured);
        assert_eq!(parts.len(), 7);
        for (index, part) in parts[4..].iter().enumerate() {
            assert_eq!(part.sequence, index as u64);
            assert_eq!(part.source_epoch, Some(later / 1_000));
            assert_eq!(part.duration_samples, Some(96));
            assert_eq!(part.anchor.part_samples, 96);
        }
        assert_eq!(counts(&config, AudioPayloadKind::Pcm).geometry_changed, 5);
    }

    #[tokio::test]
    async fn a_source_ahead_of_the_clock_is_counted_and_published() {
        let (config, captured) = epoch_config(AudioPartSize::Epoch, AudioEpochHlsPackaging::Opaque);
        let mut renditions = HashMap::new();
        let now_ns = u64::try_from(now_unix_ns()).unwrap();
        // A session that began now: its first epochs are not ahead.
        for index in 0..3_u64 {
            package_group(
                &config,
                &mut renditions,
                session_group(now_ns, index, 48, AudioPayloadKind::Pcm),
            )
            .await
            .unwrap();
        }
        assert_eq!(counts(&config, AudioPayloadKind::Pcm).time_ahead, 0);
        // Its sample count then jumps an hour (it follows a transport
        // locate, against the contract): the parts carry a source time an
        // hour ahead. This host's clock does not decide what is published,
        // so they go out, counted.
        let hour = 3_600 * 1_000_u64;
        for index in hour..hour + 3 {
            package_group(
                &config,
                &mut renditions,
                session_group(now_ns, index, 48, AudioPayloadKind::Pcm),
            )
            .await
            .unwrap();
        }
        let parts = identities(&captured);
        assert_eq!(parts.len(), 6);
        assert_eq!(parts[5].sequence, hour + 2);
        assert_eq!(
            parts[5].anchor.source_time_ns(),
            (now_ns + (hour + 2) * 1_000_000) as i64
        );
        assert_eq!(
            counts(&config, AudioPayloadKind::Pcm),
            OpaqueStreamCounts {
                stream_id: 2_030,
                time_ahead: 3,
                ..OpaqueStreamCounts::default()
            }
        );
    }

    /// Thread CPU time, for the benchmark below.
    fn thread_cpu_ns() -> u64 {
        let mut spec = libc::timespec {
            tv_sec: 0,
            tv_nsec: 0,
        };
        // SAFETY: clock_gettime writes the timespec it is given.
        unsafe { libc::clock_gettime(libc::CLOCK_THREAD_CPUTIME_ID, &mut spec) };
        spec.tv_sec as u64 * 1_000_000_000 + spec.tv_nsec as u64
    }

    /// Discards parts: the benchmark times the packaging, not a sink.
    struct CountingPublisher(AtomicU64);

    #[async_trait::async_trait]
    impl Fmp4PartPublisher for CountingPublisher {
        async fn publish_fmp4_part(&self, _part: PublishedFmp4Part) -> Result<(), String> {
            self.0.fetch_add(1, Ordering::Relaxed);
            Ok(())
        }

        async fn publish_opaque_part(&self, _part: PublishedOpaquePart) -> Result<(), String> {
            self.0.fetch_add(1, Ordering::Relaxed);
            Ok(())
        }
    }

    /// Not a check, a benchmark: thread CPU per AEP1 datagram and per part of
    /// the contributor's LL-HLS worker (FEC recovery, SoundKit validation and
    /// opaque packaging, one epoch per part) for the DAW lane's Opus at 1, 2.5
    /// and 5 ms (one 320 kb/s stereo SoundKit Opus packet an epoch, 1 source
    /// and 4 repair datagrams, as daw-nexus sends it) and for 1 ms PCM. The
    /// cost is per datagram and per part, not per millisecond, so a 1 ms
    /// lane costs 2.5 times a 2.5 ms one. Run with
    /// `cargo test --release --lib contributor_cpu_per_part -- --ignored --nocapture`.
    #[tokio::test(flavor = "current_thread")]
    #[ignore = "benchmark"]
    async fn contributor_cpu_per_part_at_each_opus_frame() {
        const EPOCHS: u64 = 20_000;
        // daw-nexus: AUDIO_FEC_MIN_REPAIR_SYMBOLS 4 from 1 ms up.
        const DAW_REPAIR_SYMBOLS: u32 = 4;
        let pcm = vec![0x5a_u8; 48 * 2 * 3];
        for (label, frame_count, opus) in [
            ("opus 1 ms (48)", 48_u32, true),
            ("opus 2.5 ms (120)", 120, true),
            ("opus 5 ms (240)", 240, true),
            ("pcm 1 ms (48)", 48, false),
        ] {
            // daw-nexus frames both lanes as SoundKit v2: Opus, and 24-bit
            // PCM for the lossless lane.
            let (encoding, bits_per_sample, codec) = if opus {
                let mut encoder = crate::opus_custom::new_lane_opus_encoder(
                    48_000,
                    2,
                    frame_count,
                    crate::opus_custom::LANE_DEFAULT_OPUS_BITRATE,
                )
                .unwrap();
                let mut packet = vec![0u8; 1_500];
                let pcm = vec![0x100_i16; frame_count as usize * 2];
                let len = encoder.encode_i16(&pcm, &mut packet).unwrap();
                packet.truncate(len);
                (EncodingFlag::Opus, 0, packet)
            } else {
                (EncodingFlag::PCMSigned, 24, pcm.clone())
            };
            let payload = crate::soundkit_v2::frame_soundkit_v2_packet(
                crate::soundkit_v2::SoundKitV2PacketMetadata {
                    encoding,
                    frame_count,
                    sample_rate: 48_000,
                    channels: 2,
                    bits_per_sample,
                    endianness: frame_header::Endianness::LittleEndian,
                    id: Some(1),
                    pts: None,
                },
                &codec,
            )
            .unwrap();
            let transport = MultichannelAudioTransportAdapter::udp(1_200);
            let mut fec = transport.prepare_fec_config(MultichannelAudioFecConfig::default());
            fec.repair_symbols = DAW_REPAIR_SYMBOLS;
            let mut sender = MultichannelAudioSender::new(MultichannelAudioSessionConfig {
                fec,
                ..MultichannelAudioSessionConfig::default()
            });
            let groups = [MultichannelAudioGroup {
                group_id: 0,
                channel_start: 0,
                channel_count: 2,
                payload_kind: AudioPayloadKind::SoundKitV2,
                sample_format: AudioSampleFormat::Unspecified,
                flags: 0,
                payload: &payload,
            }];
            let encode_started = thread_cpu_ns();
            let datagrams = (0..EPOCHS)
                .map(|epoch_id| {
                    let encoded = sender
                        .encode_epoch(MultichannelAudioEpoch {
                            session_id: 99,
                            config_generation: 1,
                            epoch_id,
                            pts_samples: epoch_id * u64::from(frame_count),
                            sample_rate: 48_000,
                            frame_count,
                            groups: &groups,
                        })
                        .unwrap();
                    transport
                        .wrap_epoch(encoded)
                        .unwrap()
                        .datagrams
                        .into_iter()
                        .map(|datagram| datagram.payload)
                        .collect::<Vec<_>>()
                })
                .collect::<Vec<_>>();
            let encode_ns = thread_cpu_ns() - encode_started;
            let datagram_count = datagrams.iter().map(Vec::len).sum::<usize>() as u64;

            // Recovery and validation, as run_audio_epoch_hls_worker does
            // them for every datagram.
            let mut receiver = ReceiverState::new(Instant::now());
            let mut groups_out = Vec::with_capacity(EPOCHS as usize);
            let recover_started = thread_cpu_ns();
            for epoch in &datagrams {
                for bytes in epoch {
                    let payload = transport.payload(bytes).unwrap();
                    let (completed, _) = receiver.push(payload, now_unix_us()).unwrap();
                    for (group, times) in completed {
                        groups_out.push((terminalize_soundkit_group(group).unwrap(), times));
                    }
                }
            }
            let recover_ns = thread_cpu_ns() - recover_started;
            assert_eq!(groups_out.len() as u64, EPOCHS);

            // Packaging: one epoch per part, to a publisher that drops it.
            let counting = Arc::new(CountingPublisher(AtomicU64::new(0)));
            let publisher: Arc<dyn Fmp4PartPublisher> = counting.clone();
            let mut config = test_config(40, 5, AudioEpochHlsPackaging::Opaque, publisher);
            config.part = AudioPartSize::Epoch;
            let mut renditions = HashMap::new();
            let package_started = thread_cpu_ns();
            for (group, times) in groups_out {
                package_group_with_times(&config, &mut renditions, group, times)
                    .await
                    .unwrap();
            }
            let package_ns = thread_cpu_ns() - package_started;
            assert_eq!(counting.0.load(Ordering::Relaxed), EPOCHS);

            let parts_per_second = 48_000 / u64::from(frame_count);
            let per_part_ns = (recover_ns + package_ns) / EPOCHS;
            eprintln!(
                "{label}: {:.1} datagrams/part, {parts_per_second} parts/s; \
                 recovery {} ns/datagram ({} ns/part), packaging {} ns/part; \
                 contributor {:.1} ms CPU per second of audio; \
                 (source FEC encode {} ns/part)",
                datagram_count as f64 / EPOCHS as f64,
                recover_ns / datagram_count,
                recover_ns / EPOCHS,
                package_ns / EPOCHS,
                (per_part_ns * parts_per_second) as f64 / 1e6,
                encode_ns / EPOCHS,
            );
        }
    }
}
