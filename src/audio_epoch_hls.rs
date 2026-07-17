//! Asynchronous AEP1 lossless-audio to LL-HLS packaging.
//!
//! The UDP receive loop only copies an AEP1 datagram into a bounded queue. FEC
//! recovery happens in the worker owned by this module. Lossless PCM is copied
//! directly into standardized PCM fMP4, while fMP4 boxing and canonical mesh
//! publication are sharded by LL-HLS rendition so wide logical streams can use
//! multiple cores while preserving ordering within each rendition.

use crate::fmp4_bridge::{Fmp4PartPublisher, Fmp4Segmenter, TimestampInput, DEFAULT_MIN_PART_MS};
use access_unit::{AccessUnit, PSI_STREAM_PRIVATE_DATA};
use boxer::fmp4::{PcmAudioConfig, PcmSampleKind};
use bytes::Bytes;
use music_audio_session::{
    DecodedMultichannelAudioGroup, MultichannelAudioReceiver, MultichannelAudioSessionConfig,
};
use playlists::Playlists;
use raptorq_datagram_fec::{AudioPayloadKind, AudioSampleFormat};
use raptorq_fec_transport::MultichannelAudioTransportAdapter;
use soundkit::audio_packet::Decoder as _;
use soundkit_flac::{FlacFrameConfig, FlacFrameEncoder, FlacProfile};
use soundkit_opus::OpusDecoder;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc,
};
use std::time::{Duration, Instant};
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
    pub playlists: Arc<Playlists>,
    pub publisher: Arc<dyn Fmp4PartPublisher>,
}

impl AudioEpochHlsConfig {
    pub fn new(
        base_stream_id: u64,
        min_part_ms: u32,
        playlists: Arc<Playlists>,
        publisher: Arc<dyn Fmp4PartPublisher>,
    ) -> Self {
        Self {
            base_stream_id,
            min_part_ms: min_part_ms.max(1),
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
    opus_flac_encoder: Option<(FlacFrameConfig, FlacFrameEncoder)>,
    opus_decoder: Option<(u32, u16, OpusDecoder)>,
    segmenter: Fmp4Segmenter,
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
            .field("has_opus_flac_encoder", &self.opus_flac_encoder.is_some())
            .field("has_opus_decoder", &self.opus_decoder.is_some())
            .finish_non_exhaustive()
    }
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
        rendition.segmenter.finish().await;
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

async fn package_group(
    config: &AudioEpochHlsConfig,
    renditions: &mut HashMap<RenditionKey, RenditionState>,
    group: DecodedMultichannelAudioGroup,
) -> Result<(), String> {
    if group.flags & AUDIO_GROUP_FLAG_ERASURE != 0 || group.payload.is_empty() {
        return Err(format!(
            "session {} group {} is an explicit lossless erasure",
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
            // The contributor publishes canonical fMP4 objects but does not
            // host a viewer cache. Keep the logical stream identity without
            // allocating a local chunk-cache slot for every source session.
            let output_stream_idx = usize::try_from(output_stream_id)
                .map_err(|_| "AEP1 LL-HLS stream id exceeds this platform".to_string())?;
            let state = entry.insert(RenditionState {
                format,
                expected_pts_samples: None,
                opus_flac_encoder: None,
                opus_decoder: None,
                segmenter: Fmp4Segmenter::new_publish_only(
                    output_stream_id,
                    output_stream_idx,
                    Arc::clone(&config.playlists),
                    TimestampInput::MillisAbsolute,
                    config
                        .min_part_ms
                        .max(DEFAULT_MIN_PART_MS.min(config.min_part_ms)),
                    Arc::clone(&config.publisher),
                ),
            });
            info!(
                session_id = group.session_id,
                group_id = group.group_id,
                output_stream_id,
                sample_rate = group.sample_rate,
                channels = group.channel_count,
                "created lossless AEP1 LL-HLS rendition"
            );
            state
        }
    };

    let pts_discontinuity = state
        .expected_pts_samples
        .is_some_and(|expected| expected != group.pts_samples);
    let format_changed = state.format != format;
    if format_changed || pts_discontinuity || group.flags & AUDIO_GROUP_FLAG_DISCONTINUITY != 0 {
        state.segmenter.finish().await;
        state.segmenter.reset();
        state.opus_flac_encoder = None;
        state.opus_decoder = None;
        state.format = format;
        debug!(
            session_id = group.session_id,
            group_id = group.group_id,
            config_generation = group.config_generation,
            format_changed,
            pts_discontinuity,
            "started a new lossless AEP1 LL-HLS continuity segment"
        );
    }
    state.expected_pts_samples = Some(
        group
            .pts_samples
            .checked_add(u64::from(group.frame_count))
            .ok_or_else(|| "AEP1 lossless PTS overflow".to_string())?,
    );

    let pcm_config = pcm_audio_config(&group)?;
    state.segmenter.set_pcm_audio_config(pcm_config);
    let audio = match group.payload_kind {
        AudioPayloadKind::Flac => group.payload,
        AudioPayloadKind::Pcm => {
            validate_pcm_group(&group)?;
            group.payload
        }
        AudioPayloadKind::Opus => decode_opus_and_encode_flac(state, &group)?,
    };
    let pts_ms = samples_to_millis(group.pts_samples, group.sample_rate)?;
    state
        .segmenter
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

fn decode_opus_and_encode_flac(
    state: &mut RenditionState,
    group: &DecodedMultichannelAudioGroup,
) -> Result<Bytes, String> {
    if !matches!(group.sample_rate, 8_000 | 12_000 | 16_000 | 24_000 | 48_000) {
        return Err(format!(
            "Opus AEP1 sample rate {} is unsupported",
            group.sample_rate
        ));
    }
    if !(1..=2).contains(&group.channel_count) {
        return Err(format!(
            "Opus AEP1 channel count {} is unsupported",
            group.channel_count
        ));
    }
    if state
        .opus_decoder
        .as_ref()
        .is_none_or(|(sample_rate, channels, _)| {
            *sample_rate != group.sample_rate || *channels != group.channel_count
        })
    {
        state.opus_decoder = Some((
            group.sample_rate,
            group.channel_count,
            OpusDecoder::new(group.sample_rate as usize, group.channel_count as usize),
        ));
    }
    let sample_capacity = usize::try_from(group.frame_count)
        .ok()
        .and_then(|frames| frames.checked_mul(usize::from(group.channel_count)))
        .ok_or_else(|| "Opus AEP1 decoded sample count overflow".to_string())?;
    let mut decoded = vec![0_i16; sample_capacity];
    let decoded_frames = state
        .opus_decoder
        .as_mut()
        .expect("Opus decoder initialized")
        .2
        .decode_i16(&group.payload, &mut decoded, false)?;
    if decoded_frames != group.frame_count as usize {
        return Err(format!(
            "Opus AEP1 packet decoded {decoded_frames} frames; expected {}",
            group.frame_count
        ));
    }

    let encoder_config = FlacFrameConfig::new(
        group.sample_rate,
        group.channel_count,
        16,
        group.frame_count,
        FlacProfile::Realtime,
    )
    .map_err(|error| error.to_string())?;
    if state
        .opus_flac_encoder
        .as_ref()
        .is_none_or(|(current, _)| *current != encoder_config)
    {
        state.opus_flac_encoder = Some((
            encoder_config,
            FlacFrameEncoder::new(encoder_config).map_err(|error| error.to_string())?,
        ));
    }
    let encoded = state
        .opus_flac_encoder
        .as_mut()
        .expect("FLAC encoder initialized")
        .1
        .encode_i16(&decoded)
        .map_err(|error| error.to_string())?;
    Ok(Bytes::from(encoded.payload))
}

fn pcm_audio_config(
    group: &DecodedMultichannelAudioGroup,
) -> Result<Option<PcmAudioConfig>, String> {
    if group.payload_kind != AudioPayloadKind::Pcm {
        return Ok(None);
    }
    let (sample_size, sample_kind) = match group.sample_format {
        AudioSampleFormat::S16Le => (16, PcmSampleKind::Integer),
        AudioSampleFormat::S24Le => (24, PcmSampleKind::Integer),
        AudioSampleFormat::S32Le => (32, PcmSampleKind::Integer),
        AudioSampleFormat::F32Le => (32, PcmSampleKind::Float),
        AudioSampleFormat::Unspecified => {
            return Err("PCM AEP1 payload must declare its sample format".to_string())
        }
    };
    Ok(Some(PcmAudioConfig {
        sample_rate: group.sample_rate,
        channel_count: group.channel_count,
        sample_size,
        little_endian: true,
        sample_kind,
    }))
}

fn validate_pcm_group(group: &DecodedMultichannelAudioGroup) -> Result<(), String> {
    let config = pcm_audio_config(group)?.expect("PCM group has PCM config");
    if config.sample_rate == 0 || config.channel_count == 0 || group.frame_count == 0 {
        return Err("PCM AEP1 dimensions must be positive".to_string());
    }
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
    use crate::fmp4_bridge::PublishedFmp4Part;
    use music_audio_session::MultichannelAudioSender;
    use playlists::Options;
    use raptorq_datagram_fec::{
        MultichannelAudioEpoch, MultichannelAudioFecConfig, MultichannelAudioGroup,
    };
    use soundkit::audio_packet::Encoder as _;
    use soundkit_opus::OpusEncoder;
    use std::collections::HashSet;
    use std::sync::Mutex as StdMutex;

    #[derive(Default)]
    struct CapturingPublisher {
        parts: StdMutex<Vec<PublishedFmp4Part>>,
    }

    #[async_trait::async_trait]
    impl Fmp4PartPublisher for CapturingPublisher {
        async fn publish_fmp4_part(&self, part: PublishedFmp4Part) -> Result<(), String> {
            self.parts.lock().unwrap().push(part);
            Ok(())
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
    async fn every_aep1_pcm_sample_format_reaches_pcm_fmp4_ll_hls_unchanged() {
        let options = Options {
            num_playlists: 8,
            part_target_ms: 5,
            ..Options::default()
        };
        let (playlists, _, _) = Playlists::new(options);
        let captured = Arc::new(CapturingPublisher::default());
        let publisher: Arc<dyn Fmp4PartPublisher> = captured.clone();
        let config = AudioEpochHlsConfig::new(2, 5, playlists.clone(), publisher);
        let (tx, rx) = channel(64);
        let (shutdown_tx, shutdown_rx) = watch::channel(());
        let worker = tokio::spawn(run_audio_epoch_hls_worker(config, rx, shutdown_rx));

        let transport = MultichannelAudioTransportAdapter::udp(1_200);
        let fec = transport.prepare_fec_config(MultichannelAudioFecConfig::default());
        let mut sender = MultichannelAudioSender::new(MultichannelAudioSessionConfig {
            fec,
            ..MultichannelAudioSessionConfig::default()
        });
        let peer: SocketAddr = "127.0.0.1:41000".parse().unwrap();
        let mut expected_payloads = HashMap::<(u64, u64), Vec<u8>>::new();
        for epoch_id in 0..2_u64 {
            let sample_count = 240 * 2;
            let s16 = (0..sample_count)
                .flat_map(|sample| ((sample as i16).wrapping_mul(97)).to_le_bytes())
                .collect::<Vec<_>>();
            let s24 = vec![epoch_id as u8; sample_count * 3];
            let s32 = (0..sample_count)
                .flat_map(|sample| ((sample as i32).wrapping_mul(97_003)).to_le_bytes())
                .collect::<Vec<_>>();
            let f32 = (0..sample_count)
                .flat_map(|sample| (((sample as f32 % 200.0) - 100.0) / 100.0).to_le_bytes())
                .collect::<Vec<_>>();
            let payloads = [s16, s24, s32, f32];
            for (index, payload) in payloads.iter().enumerate() {
                expected_payloads.insert((2 + index as u64, epoch_id), payload.clone());
            }
            let formats = [
                AudioSampleFormat::S16Le,
                AudioSampleFormat::S24Le,
                AudioSampleFormat::S32Le,
                AudioSampleFormat::F32Le,
            ];
            let groups = payloads
                .iter()
                .zip(formats)
                .enumerate()
                .map(|(index, (payload, sample_format))| MultichannelAudioGroup {
                    group_id: index as u16,
                    channel_start: index as u16 * 2,
                    channel_count: 2,
                    payload_kind: AudioPayloadKind::Pcm,
                    sample_format,
                    flags: 0,
                    payload,
                })
                .collect::<Vec<_>>();
            let encoded = sender
                .encode_epoch(MultichannelAudioEpoch {
                    session_id: 99,
                    config_generation: 3,
                    epoch_id,
                    pts_samples: epoch_id * 240,
                    sample_rate: 48_000,
                    frame_count: 240,
                    groups: &groups,
                })
                .unwrap();
            let wrapped = transport.wrap_epoch(encoded).unwrap();
            for datagram in wrapped.datagrams {
                tx.send(AudioEpochHlsDatagram {
                    peer,
                    bytes: datagram.payload,
                })
                .await
                .unwrap();
            }
        }
        drop(tx);
        let _ = shutdown_tx.send(());
        worker.await.unwrap();

        let parts = captured.parts.lock().unwrap();
        assert_eq!(parts.len(), 8);
        assert!(parts.iter().all(|part| (2..=5).contains(&part.stream_id)));
        assert!(parts.iter().all(|part| {
            let expected_codec = match part.stream_id {
                2 => "pcm_s16le",
                3 => "pcm_s24le",
                4 => "pcm_s32le",
                5 => "pcm_f32le",
                _ => unreachable!(),
            };
            part.audio_codec == Some(expected_codec)
        }));
        assert!(parts.iter().all(|part| part.video_units == 0));
        for part in parts.iter() {
            assert_eq!(
                mdat_payload(&part.bytes),
                expected_payloads
                    .get(&(part.stream_id, part.sequence))
                    .map(Vec::as_slice)
            );
            if let Some(init) = &part.init {
                let expected_entry = if part.stream_id == 5 {
                    b"fpcm"
                } else {
                    b"ipcm"
                };
                assert!(init.windows(4).any(|bytes| bytes == expected_entry));
                assert!(!init.windows(4).any(|bytes| bytes == b"fLaC"));
            }
        }
        assert_eq!(playlists.active(), 0);
    }

    #[tokio::test]
    async fn aep1_opus_also_reaches_flac_fmp4_ll_hls() {
        let options = Options {
            num_playlists: 8,
            part_target_ms: 5,
            ..Options::default()
        };
        let (playlists, _, _) = Playlists::new(options);
        let captured = Arc::new(CapturingPublisher::default());
        let publisher: Arc<dyn Fmp4PartPublisher> = captured.clone();
        let config = AudioEpochHlsConfig::new(3, 5, playlists.clone(), publisher);
        let (tx, rx) = channel(64);
        let (shutdown_tx, shutdown_rx) = watch::channel(());
        let worker = tokio::spawn(run_audio_epoch_hls_worker(config, rx, shutdown_rx));

        let transport = MultichannelAudioTransportAdapter::udp(1_200);
        let fec = transport.prepare_fec_config(MultichannelAudioFecConfig::default());
        let mut sender = MultichannelAudioSender::new(MultichannelAudioSessionConfig {
            fec,
            ..MultichannelAudioSessionConfig::default()
        });
        let mut opus = OpusEncoder::new(48_000, 16, 2, 240, 128_000);
        opus.init().unwrap();
        let peer: SocketAddr = "127.0.0.1:41001".parse().unwrap();
        for epoch_id in 0..2_u64 {
            let mut packet = vec![0_u8; 1_275];
            let pcm = (0..480)
                .map(|sample| ((sample * 97 + epoch_id as i32 * 31) % 2_000 - 1_000) as i16)
                .collect::<Vec<_>>();
            let packet_len = opus.encode_i16(&pcm, &mut packet).unwrap();
            packet.truncate(packet_len);
            let groups = [MultichannelAudioGroup {
                group_id: 0,
                channel_start: 0,
                channel_count: 2,
                payload_kind: AudioPayloadKind::Opus,
                sample_format: AudioSampleFormat::Unspecified,
                flags: 0,
                payload: &packet,
            }];
            let encoded = sender
                .encode_epoch(MultichannelAudioEpoch {
                    session_id: 100,
                    config_generation: 1,
                    epoch_id,
                    pts_samples: epoch_id * 240,
                    sample_rate: 48_000,
                    frame_count: 240,
                    groups: &groups,
                })
                .unwrap();
            let wrapped = transport.wrap_epoch(encoded).unwrap();
            for datagram in wrapped.datagrams {
                tx.send(AudioEpochHlsDatagram {
                    peer,
                    bytes: datagram.payload,
                })
                .await
                .unwrap();
            }
        }
        drop(tx);
        let _ = shutdown_tx.send(());
        worker.await.unwrap();

        let parts = captured.parts.lock().unwrap();
        assert!(!parts.is_empty());
        assert!(parts.iter().all(|part| part.stream_id == 3));
        assert!(parts.iter().all(|part| part.audio_codec == Some("flac")));
        assert!(parts.iter().any(|part| part
            .init
            .as_ref()
            .is_some_and(|init| init.windows(4).any(|bytes| bytes == b"fLaC"))));
        assert_eq!(playlists.active(), 0);
    }

    #[tokio::test]
    async fn wide_aep1_pcm_stream_shards_into_parallel_lossless_ll_hls_renditions() {
        let options = Options {
            num_playlists: 64,
            part_target_ms: 5,
            ..Options::default()
        };
        let (playlists, _, _) = Playlists::new(options);
        let captured = Arc::new(CapturingPublisher::default());
        let publisher: Arc<dyn Fmp4PartPublisher> = captured.clone();
        let config = AudioEpochHlsConfig::new(20, 5, playlists.clone(), publisher);
        let (tx, rx) = channel(512);
        let (shutdown_tx, shutdown_rx) = watch::channel(());
        let worker = tokio::spawn(run_audio_epoch_hls_worker(config, rx, shutdown_rx));

        let transport = MultichannelAudioTransportAdapter::udp(1_200);
        let fec = transport.prepare_fec_config(MultichannelAudioFecConfig::default());
        let mut sender = MultichannelAudioSender::new(MultichannelAudioSessionConfig {
            fec,
            ..MultichannelAudioSessionConfig::default()
        });
        let peer: SocketAddr = "127.0.0.1:41002".parse().unwrap();
        for epoch_id in 0..2_u64 {
            let sample_count = 240 * 8;
            let payloads = (0..16_u16)
                .map(|group_id| {
                    (0..sample_count)
                        .flat_map(|sample| {
                            let value = ((sample as i32 * 97)
                                + (i32::from(group_id) * 1_013)
                                + (epoch_id as i32 * 31))
                                % 1_000_000
                                - 500_000;
                            let bytes = value.to_le_bytes();
                            [bytes[0], bytes[1], bytes[2]]
                        })
                        .collect::<Vec<_>>()
                })
                .collect::<Vec<_>>();
            let groups = payloads
                .iter()
                .enumerate()
                .map(|(index, payload)| MultichannelAudioGroup {
                    group_id: index as u16,
                    channel_start: index as u16 * 8,
                    channel_count: 8,
                    payload_kind: AudioPayloadKind::Pcm,
                    sample_format: AudioSampleFormat::S24Le,
                    flags: 0,
                    payload,
                })
                .collect::<Vec<_>>();
            let encoded = sender
                .encode_epoch(MultichannelAudioEpoch {
                    session_id: 128,
                    config_generation: 1,
                    epoch_id,
                    pts_samples: epoch_id * 240,
                    sample_rate: 48_000,
                    frame_count: 240,
                    groups: &groups,
                })
                .unwrap();
            let wrapped = transport.wrap_epoch(encoded).unwrap();
            for datagram in wrapped.datagrams {
                tx.send(AudioEpochHlsDatagram {
                    peer,
                    bytes: datagram.payload,
                })
                .await
                .unwrap();
            }
        }
        drop(tx);
        let _ = shutdown_tx.send(());
        worker.await.unwrap();

        let parts = captured.parts.lock().unwrap();
        assert!(!parts.is_empty());
        let streams = parts
            .iter()
            .map(|part| part.stream_id)
            .collect::<HashSet<_>>();
        assert_eq!(streams, (20..36).collect::<HashSet<_>>());
        assert!(parts
            .iter()
            .all(|part| part.audio_codec == Some("pcm_s24le")));
        assert!(parts.iter().all(|part| part.video_units == 0));
        assert!(parts.iter().any(|part| part
            .init
            .as_ref()
            .is_some_and(|init| init.windows(4).any(|bytes| bytes == b"ipcm"))));
        assert_eq!(playlists.active(), 0);
    }

    async fn assert_long_flac_rendition(part_ms: u32, expected_parts: usize) {
        let options = Options {
            num_playlists: 2,
            max_parts_per_segment: 256,
            segment_min_ms: 1_000,
            part_target_ms: part_ms,
            ..Options::default()
        };
        let (playlists, _, _) = Playlists::new(options);
        let captured = Arc::new(CapturingPublisher::default());
        let publisher: Arc<dyn Fmp4PartPublisher> = captured.clone();
        let config = AudioEpochHlsConfig::new(1, part_ms, playlists, publisher);
        let mut renditions = HashMap::new();
        let frame_config = FlacFrameConfig::new(48_000, 2, 24, 240, FlacProfile::Realtime)
            .expect("valid 48 kHz FLAC frame config");
        let mut encoder = FlacFrameEncoder::new(frame_config).expect("FLAC encoder");
        let pcm = vec![0_u8; 240 * 2 * 3];

        for epoch_id in 0..1_000_u64 {
            let encoded = encoder.encode_s24le(&pcm).expect("encode FLAC frame");
            package_group(
                &config,
                &mut renditions,
                DecodedMultichannelAudioGroup {
                    session_id: 101,
                    config_generation: 1,
                    epoch_id,
                    pts_samples: epoch_id * 240,
                    sample_rate: 48_000,
                    frame_count: 240,
                    group_count: 1,
                    group_id: 0,
                    group_index: 0,
                    channel_start: 0,
                    channel_count: 2,
                    payload_kind: AudioPayloadKind::Flac,
                    sample_format: AudioSampleFormat::S24Le,
                    flags: 0,
                    payload: Bytes::from(encoded.payload),
                    raptorq_recovered_fragments: 0,
                },
            )
            .await
            .expect("package lossless group");
        }
        assert_eq!(
            captured.parts.lock().unwrap().len(),
            expected_parts,
            "known-duration audio parts must close without waiting for another access unit"
        );
        for rendition in renditions.values_mut() {
            rendition.segmenter.finish().await;
        }

        let parts = captured.parts.lock().unwrap();
        assert_eq!(parts.len(), expected_parts);
        assert!(parts.iter().all(|part| part.duration_ms == part_ms));
        assert!(parts
            .iter()
            .enumerate()
            .all(|(sequence, part)| part.sequence == sequence as u64));
    }

    #[tokio::test]
    async fn long_20ms_flac_rendition_keeps_exact_durations_and_all_parts() {
        assert_long_flac_rendition(20, 250).await;
    }

    #[tokio::test]
    async fn long_5ms_flac_rendition_keeps_exact_durations_and_all_parts() {
        assert_long_flac_rendition(5, 1_000).await;
    }
}
