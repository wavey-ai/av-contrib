//! Asynchronous AEP1 audio to opaque LL-HLS publication.
//!
//! The UDP receive loop only copies an AEP1 datagram into a bounded queue. FEC
//! recovery happens in the worker owned by this module. Recovered group bytes
//! are never inspected or transformed: AEP1 owns timing and continuity, while
//! producers and consumers own any framing within the payload. Publication is
//! sharded by rendition so wide logical streams can use multiple cores while
//! preserving ordering within each rendition.

use crate::fmp4_bridge::{
    Fmp4PartPublisher, Fmp4Segmenter, PublishedOpaquePart, TimestampInput, DEFAULT_MIN_PART_MS,
};
use access_unit::{AccessUnit, PSI_STREAM_PRIVATE_DATA};
use boxer::fmp4::{AudioTrackConfig, PcmAudioConfig, PcmSampleKind};
use bytes::{Bytes, BytesMut};
use music_audio_session::{
    DecodedMultichannelAudioGroup, MultichannelAudioReceiver, MultichannelAudioSessionConfig,
};
use playlists::Playlists;
use raptorq_datagram_fec::{AudioPayloadKind, AudioSampleFormat};
use raptorq_fec_transport::MultichannelAudioTransportAdapter;
use std::collections::HashMap;
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

pub const DEFAULT_AUDIO_EPOCH_HLS_QUEUE_CAPACITY: usize = 4_096;
const RENDITION_WORKER_QUEUE_CAPACITY: usize = 1_024;
const AUDIO_EPOCH_IDLE_SWEEP_INTERVAL: Duration = Duration::from_secs(5);
const AUDIO_EPOCH_IDLE_TIMEOUT: Duration = Duration::from_secs(30);
const AUDIO_GROUP_FLAG_DISCONTINUITY: u8 = 1 << 0;
const AUDIO_GROUP_FLAG_ERASURE: u8 = 1 << 1;
static WORKER_DATAGRAMS: AtomicU64 = AtomicU64::new(0);
static WORKER_GROUPS_COMPLETED: AtomicU64 = AtomicU64::new(0);
static WORKER_RAPTORQ_FRAGMENTS_RECOVERED: AtomicU64 = AtomicU64::new(0);
static WORKER_ERRORS: AtomicU64 = AtomicU64::new(0);
static ACTIVE_RECEIVERS: AtomicU64 = AtomicU64::new(0);
static ACTIVE_RENDITION_WORKERS: AtomicU64 = AtomicU64::new(0);
static RETIRED_RECEIVERS: AtomicU64 = AtomicU64::new(0);
static RETIRED_RENDITION_WORKERS: AtomicU64 = AtomicU64::new(0);

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
    }
}

#[derive(Debug, Clone)]
pub struct AudioEpochHlsDatagram {
    pub peer: SocketAddr,
    pub bytes: Bytes,
}

#[derive(Clone)]
pub struct AudioEpochHlsConfig {
    /// LL-HLS id used by group zero. Each subsequent group uses base + group_id.
    pub base_stream_id: u64,
    pub min_part_ms: u32,
    pub packaging: AudioEpochHlsPackaging,
    pub playlists: Arc<Playlists>,
    pub publisher: Arc<dyn Fmp4PartPublisher>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AudioEpochHlsPackaging {
    /// Publish the recovered AEP1 payload bytes without inspecting them.
    Opaque,
    /// Convert supported elementary PCM or FLAC payloads into CMAF/fMP4.
    Fmp4,
}

impl AudioEpochHlsConfig {
    pub fn new(
        base_stream_id: u64,
        min_part_ms: u32,
        packaging: AudioEpochHlsPackaging,
        playlists: Arc<Playlists>,
        publisher: Arc<dyn Fmp4PartPublisher>,
    ) -> Self {
        Self {
            base_stream_id,
            min_part_ms: min_part_ms.max(1),
            packaging,
            playlists,
            publisher,
        }
    }
}

impl std::fmt::Debug for AudioEpochHlsConfig {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("AudioEpochHlsConfig")
            .field("base_stream_id", &self.base_stream_id)
            .field("min_part_ms", &self.min_part_ms)
            .field("packaging", &self.packaging)
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
    format: RenditionFormat,
    expected_pts_samples: Option<u64>,
    packager: RenditionPackager,
}

enum RenditionPackager {
    Opaque(OpaquePartSegmenter),
    Fmp4(Fmp4Segmenter),
}

struct OpaquePartSegmenter {
    output_stream_id: u64,
    output_stream_idx: usize,
    min_part_ms: u32,
    publisher: Arc<dyn Fmp4PartPublisher>,
    bytes: BytesMut,
    duration_ms: u32,
    audio_units: usize,
    published_parts: u64,
}

struct RenditionWorker {
    sender: mpsc::Sender<DecodedMultichannelAudioGroup>,
    handle: JoinHandle<()>,
    last_seen: Instant,
}

struct ReceiverState {
    receiver: MultichannelAudioReceiver,
    last_seen: Instant,
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
            Self::Opaque(segmenter) => segmenter.finish().await,
            Self::Fmp4(segmenter) => {
                segmenter.finish().await;
                Ok(())
            }
        }
    }

    fn reset(&mut self) {
        match self {
            Self::Opaque(segmenter) => segmenter.reset(),
            Self::Fmp4(segmenter) => segmenter.reset(),
        }
    }

    fn opaque(&mut self) -> Result<&mut OpaquePartSegmenter, String> {
        match self {
            Self::Opaque(segmenter) => Ok(segmenter),
            Self::Fmp4(_) => Err("opaque AEP1 payload reached the fMP4 packager".to_string()),
        }
    }

    fn fmp4(&mut self) -> Result<&mut Fmp4Segmenter, String> {
        match self {
            Self::Fmp4(segmenter) => Ok(segmenter),
            Self::Opaque(_) => Err("fMP4 AEP1 payload reached the opaque packager".to_string()),
        }
    }
}

impl OpaquePartSegmenter {
    fn new(
        output_stream_id: u64,
        output_stream_idx: usize,
        min_part_ms: u32,
        publisher: Arc<dyn Fmp4PartPublisher>,
    ) -> Self {
        Self {
            output_stream_id,
            output_stream_idx,
            min_part_ms: min_part_ms.max(1),
            publisher,
            bytes: BytesMut::new(),
            duration_ms: 0,
            audio_units: 0,
            published_parts: 0,
        }
    }

    async fn push(&mut self, packet: Bytes, duration_ms: u32) -> Result<(), String> {
        self.bytes.extend_from_slice(&packet);
        self.duration_ms = self.duration_ms.saturating_add(duration_ms);
        self.audio_units = self.audio_units.saturating_add(1);
        if self.duration_ms >= self.min_part_ms {
            self.flush().await?;
        }
        Ok(())
    }

    async fn finish(&mut self) -> Result<(), String> {
        self.flush().await
    }

    fn reset(&mut self) {
        self.bytes.clear();
        self.duration_ms = 0;
        self.audio_units = 0;
    }

    async fn flush(&mut self) -> Result<(), String> {
        if self.bytes.is_empty() {
            return Ok(());
        }
        let packaged_at_unix_ns = now_unix_ns();
        let part = PublishedOpaquePart {
            stream_id: self.output_stream_id,
            stream_idx: self.output_stream_idx,
            sequence: self.published_parts,
            duration_ms: self.duration_ms.max(1),
            packaged_at_unix_ns,
            published_at_unix_ns: now_unix_ns(),
            bytes: self.bytes.split().freeze(),
            audio_units: std::mem::take(&mut self.audio_units),
        };
        self.duration_ms = 0;
        self.published_parts = self.published_parts.saturating_add(1);
        self.publisher.publish_opaque_part(part).await
    }
}

fn now_unix_ns() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_nanos().min(i64::MAX as u128) as i64)
        .unwrap_or(0)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
struct RenditionKey {
    session_id: u64,
    group_id: u16,
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
                    ReceiverState {
                        receiver: MultichannelAudioReceiver::new(MultichannelAudioSessionConfig::default()),
                        last_seen: now,
                    }
                });
                receiver.last_seen = now;
                let outcome = receiver.receiver.push_datagram(payload);
                match outcome {
                    Ok(outcome) => {
                        WORKER_DATAGRAMS.fetch_add(1, Ordering::Relaxed);
                        for group in outcome.completed_groups {
                            WORKER_GROUPS_COMPLETED.fetch_add(1, Ordering::Relaxed);
                            WORKER_RAPTORQ_FRAGMENTS_RECOVERED.fetch_add(
                                u64::from(group.raptorq_recovered_fragments),
                                Ordering::Relaxed,
                            );
                            if let Err(error) = dispatch_group_to_rendition_worker(
                                &config,
                                &mut rendition_workers,
                                group,
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
            ReceiverState {
                receiver: MultichannelAudioReceiver::new(MultichannelAudioSessionConfig::default()),
                last_seen: now,
            }
        });
        receiver.last_seen = now;
        let outcome = receiver.receiver.push_datagram(payload);
        if let Ok(outcome) = outcome {
            WORKER_DATAGRAMS.fetch_add(1, Ordering::Relaxed);
            for group in outcome.completed_groups {
                WORKER_GROUPS_COMPLETED.fetch_add(1, Ordering::Relaxed);
                WORKER_RAPTORQ_FRAGMENTS_RECOVERED.fetch_add(
                    u64::from(group.raptorq_recovered_fragments),
                    Ordering::Relaxed,
                );
                if let Err(error) =
                    dispatch_group_to_rendition_worker(&config, &mut rendition_workers, group).await
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
    group: DecodedMultichannelAudioGroup,
) -> Result<(), String> {
    let key = RenditionKey {
        session_id: group.session_id,
        group_id: group.group_id,
    };
    let now = Instant::now();
    let sender = match workers.entry(key) {
        std::collections::hash_map::Entry::Occupied(entry) => {
            let worker = entry.into_mut();
            worker.last_seen = now;
            worker.sender.clone()
        }
        std::collections::hash_map::Entry::Vacant(entry) => {
            let (sender, receiver) = mpsc::channel(RENDITION_WORKER_QUEUE_CAPACITY);
            let handle = tokio::spawn(run_rendition_worker(config.clone(), key, receiver));
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
    sender
        .send(group)
        .await
        .map_err(|_| format!("AEP1 LL-HLS rendition worker {key:?} stopped"))?;
    Ok(())
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
    mut input: mpsc::Receiver<DecodedMultichannelAudioGroup>,
) {
    let mut renditions = HashMap::<RenditionKey, RenditionState>::new();
    while let Some(group) = input.recv().await {
        if let Err(error) = package_group(&config, &mut renditions, group).await {
            WORKER_ERRORS.fetch_add(1, Ordering::Relaxed);
            warn!(
                session_id = key.session_id,
                group_id = key.group_id,
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
) -> RenditionPackager {
    match config.packaging {
        AudioEpochHlsPackaging::Opaque => RenditionPackager::Opaque(OpaquePartSegmenter::new(
            output_stream_id,
            output_stream_idx,
            config.min_part_ms,
            Arc::clone(&config.publisher),
        )),
        AudioEpochHlsPackaging::Fmp4 => RenditionPackager::Fmp4(Fmp4Segmenter::new_publish_only(
            output_stream_id,
            output_stream_idx,
            Arc::clone(&config.playlists),
            TimestampInput::MillisAbsolute,
            config
                .min_part_ms
                .max(DEFAULT_MIN_PART_MS.min(config.min_part_ms)),
            Arc::clone(&config.publisher),
        )),
    }
}

async fn package_group(
    config: &AudioEpochHlsConfig,
    renditions: &mut HashMap<RenditionKey, RenditionState>,
    group: DecodedMultichannelAudioGroup,
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
    };
    let state = match renditions.entry(key) {
        std::collections::hash_map::Entry::Occupied(entry) => entry.into_mut(),
        std::collections::hash_map::Entry::Vacant(entry) => {
            let output_stream_id = config
                .base_stream_id
                .checked_add(u64::from(group.group_id))
                .ok_or_else(|| "AEP1 LL-HLS stream id overflow".to_string())?;
            // The contributor publishes canonical media objects but does not
            // host a viewer cache. Keep the logical stream identity without
            // allocating a local chunk-cache slot for every source session.
            let output_stream_idx = usize::try_from(output_stream_id)
                .map_err(|_| "AEP1 LL-HLS stream id exceeds this platform".to_string())?;
            let state = entry.insert(RenditionState {
                format,
                expected_pts_samples: None,
                packager: new_rendition_packager(config, output_stream_id, output_stream_idx),
            });
            info!(
                session_id = group.session_id,
                group_id = group.group_id,
                output_stream_id,
                sample_rate = group.sample_rate,
                channels = group.channel_count,
                packaging = ?config.packaging,
                "created AEP1 LL-HLS rendition"
            );
            state
        }
    };

    let pts_discontinuity = state
        .expected_pts_samples
        .is_some_and(|expected| expected != group.pts_samples);
    let format_changed = state.format != format;
    if format_changed || pts_discontinuity || group.flags & AUDIO_GROUP_FLAG_DISCONTINUITY != 0 {
        state.packager.finish().await?;
        state.packager.reset();
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

    let duration_ms = u32::try_from(samples_to_millis(
        u64::from(group.frame_count),
        group.sample_rate,
    )?)
    .map_err(|_| "AEP1 group duration exceeds u32 milliseconds".to_string())?
    .max(1);
    match config.packaging {
        AudioEpochHlsPackaging::Opaque => {
            state
                .packager
                .opaque()?
                .push(group.payload, duration_ms)
                .await
        }
        AudioEpochHlsPackaging::Fmp4 => package_group_as_fmp4(state, group).await,
    }
}

async fn package_group_as_fmp4(
    state: &mut RenditionState,
    group: DecodedMultichannelAudioGroup,
) -> Result<(), String> {
    let audio_config = audio_track_config(&group)?;
    let segmenter = state.packager.fmp4()?;
    segmenter.set_audio_track_config(audio_config);
    let audio = match group.payload_kind {
        AudioPayloadKind::Flac => group.payload,
        AudioPayloadKind::Pcm => {
            validate_pcm_group(&group)?;
            group.payload
        }
        AudioPayloadKind::Opus => {
            return Err(
                "AEP1 Opus may contain producer framing or encryption; use opaque packaging"
                    .to_string(),
            )
        }
    };
    let pts_ms = samples_to_millis(group.pts_samples, group.sample_rate)?;
    segmenter
        .push_access_unit(AccessUnit {
            key: true,
            pts: pts_ms,
            dts: pts_ms,
            data: audio,
            stream_type: PSI_STREAM_PRIVATE_DATA,
            id: group.epoch_id,
        })
        .await;
    Ok(())
}

fn audio_track_config(
    group: &DecodedMultichannelAudioGroup,
) -> Result<Option<AudioTrackConfig>, String> {
    match group.payload_kind {
        AudioPayloadKind::Flac => Ok(None),
        AudioPayloadKind::Opus => Err(
            "AEP1 Opus may contain producer framing or encryption; use opaque packaging"
                .to_string(),
        ),
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
    }
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
        let (playlists, _, _) = Playlists::new(Options {
            num_playlists: 256,
            part_target_ms: min_part_ms,
            ..Options::default()
        });
        AudioEpochHlsConfig::new(base_stream_id, min_part_ms, packaging, playlists, publisher)
    }

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
        let mut receivers = HashMap::from([(
            peer,
            ReceiverState {
                receiver: MultichannelAudioReceiver::new(MultichannelAudioSessionConfig::default()),
                last_seen: idle_since,
            },
        )]);
        ACTIVE_RECEIVERS.fetch_add(1, Ordering::Relaxed);

        retire_idle_receivers(&mut receivers, now);
        assert!(receivers.is_empty());

        let (sender, mut receiver) = mpsc::channel::<DecodedMultichannelAudioGroup>(1);
        let handle = tokio::spawn(async move { while receiver.recv().await.is_some() {} });
        let key = RenditionKey {
            session_id: 99,
            group_id: 7,
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
        for (group_id, bytes) in expected.iter().enumerate() {
            let part = parts
                .iter()
                .find(|part| part.stream_id == 20 + group_id as u64)
                .expect("one published part per AEP1 group");
            assert_eq!(part.sequence, 0);
            assert_eq!(part.duration_ms, 5);
            assert_eq!(part.audio_units, 1);
            assert_eq!(part.bytes.as_ref(), bytes);
        }
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
    }

    #[tokio::test]
    async fn declared_format_changes_do_not_reset_object_sequence() {
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
        assert_eq!(parts[0].sequence, 0);
        assert_eq!(parts[1].sequence, 1);
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
        assert_eq!(streams, (100..132).collect::<HashSet<_>>());
        for part in parts.iter() {
            let group_id = usize::try_from(part.stream_id - 100).unwrap();
            assert_eq!(part.bytes.as_ref(), vec![group_id as u8; 37 + group_id]);
        }
    }
}
