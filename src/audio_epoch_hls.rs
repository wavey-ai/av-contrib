//! Asynchronous AEP1 lossless-audio to LL-HLS packaging.
//!
//! The UDP receive loop only copies an AEP1 datagram into a bounded queue. FEC
//! recovery, optional PCM-to-FLAC encoding, fMP4 boxing, playlist mutation, and
//! canonical mesh publication all happen in the worker owned by this module.

use crate::fmp4_bridge::{Fmp4PartPublisher, Fmp4Segmenter, TimestampInput, DEFAULT_MIN_PART_MS};
use access_unit::{AccessUnit, PSI_STREAM_PRIVATE_DATA};
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
use tokio::sync::{mpsc, watch};
use tracing::{debug, info, warn};

pub const DEFAULT_AUDIO_EPOCH_HLS_QUEUE_CAPACITY: usize = 4_096;
const AUDIO_GROUP_FLAG_DISCONTINUITY: u8 = 1 << 0;
const AUDIO_GROUP_FLAG_ERASURE: u8 = 1 << 1;
static WORKER_DATAGRAMS: AtomicU64 = AtomicU64::new(0);
static WORKER_GROUPS_COMPLETED: AtomicU64 = AtomicU64::new(0);
static WORKER_RAPTORQ_FRAGMENTS_RECOVERED: AtomicU64 = AtomicU64::new(0);
static WORKER_ERRORS: AtomicU64 = AtomicU64::new(0);

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct AudioEpochHlsWorkerStats {
    pub datagrams: u64,
    pub groups_completed: u64,
    pub raptorq_fragments_recovered: u64,
    pub errors: u64,
}

pub fn worker_stats() -> AudioEpochHlsWorkerStats {
    AudioEpochHlsWorkerStats {
        datagrams: WORKER_DATAGRAMS.load(Ordering::Relaxed),
        groups_completed: WORKER_GROUPS_COMPLETED.load(Ordering::Relaxed),
        raptorq_fragments_recovered: WORKER_RAPTORQ_FRAGMENTS_RECOVERED.load(Ordering::Relaxed),
        errors: WORKER_ERRORS.load(Ordering::Relaxed),
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
    pcm_encoder: Option<(FlacFrameConfig, FlacFrameEncoder)>,
    opus_decoder: Option<(u32, u16, OpusDecoder)>,
    segmenter: Fmp4Segmenter,
}

impl std::fmt::Debug for RenditionState {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("RenditionState")
            .field("format", &self.format)
            .field("expected_pts_samples", &self.expected_pts_samples)
            .field("has_pcm_encoder", &self.pcm_encoder.is_some())
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
    let mut receivers = HashMap::<SocketAddr, MultichannelAudioReceiver>::new();
    let mut renditions = HashMap::<RenditionKey, RenditionState>::new();

    loop {
        tokio::select! {
            _ = shutdown_rx.changed() => break,
            message = input.recv() => {
                let Some(message) = message else { break; };
                let payload = match transport.payload(&message.bytes) {
                    Ok(payload) => payload,
                    Err(error) => {
                        warn!(peer = %message.peer, error = %error, "dropping invalid AEP1 datagram before LL-HLS recovery");
                        continue;
                    }
                };
                let outcome = receivers
                    .entry(message.peer)
                    .or_insert_with(|| MultichannelAudioReceiver::new(MultichannelAudioSessionConfig::default()))
                    .push_datagram(payload);
                match outcome {
                    Ok(outcome) => {
                        WORKER_DATAGRAMS.fetch_add(1, Ordering::Relaxed);
                        for group in outcome.completed_groups {
                            WORKER_GROUPS_COMPLETED.fetch_add(1, Ordering::Relaxed);
                            WORKER_RAPTORQ_FRAGMENTS_RECOVERED.fetch_add(
                                u64::from(group.raptorq_recovered_fragments),
                                Ordering::Relaxed,
                            );
                            if let Err(error) = package_group(&config, &mut renditions, group).await {
                                WORKER_ERRORS.fetch_add(1, Ordering::Relaxed);
                                warn!(peer = %message.peer, error = %error, "failed to package recovered AEP1 group into LL-HLS");
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
        let outcome = receivers
            .entry(message.peer)
            .or_insert_with(|| {
                MultichannelAudioReceiver::new(MultichannelAudioSessionConfig::default())
            })
            .push_datagram(payload);
        if let Ok(outcome) = outcome {
            WORKER_DATAGRAMS.fetch_add(1, Ordering::Relaxed);
            for group in outcome.completed_groups {
                WORKER_GROUPS_COMPLETED.fetch_add(1, Ordering::Relaxed);
                WORKER_RAPTORQ_FRAGMENTS_RECOVERED.fetch_add(
                    u64::from(group.raptorq_recovered_fragments),
                    Ordering::Relaxed,
                );
                if let Err(error) = package_group(&config, &mut renditions, group).await {
                    WORKER_ERRORS.fetch_add(1, Ordering::Relaxed);
                    warn!(peer = %message.peer, error = %error, "failed to drain AEP1 group into LL-HLS");
                }
            }
        } else {
            WORKER_ERRORS.fetch_add(1, Ordering::Relaxed);
        }
    }

    for rendition in renditions.values_mut() {
        rendition.segmenter.finish().await;
    }
    info!(renditions = renditions.len(), "AEP1 LL-HLS worker stopped");
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
            let output_stream_idx =
                if output_stream_id < config.playlists.chunk_cache.options.num_playlists as u64 {
                    output_stream_id as usize
                } else {
                    config
                        .playlists
                        .chunk_cache
                        .get_or_create_stream_idx(output_stream_id)
                        .await
                };
            let state = entry.insert(RenditionState {
                format,
                expected_pts_samples: None,
                pcm_encoder: None,
                opus_decoder: None,
                segmenter: Fmp4Segmenter::new_with_publisher(
                    output_stream_id,
                    output_stream_idx,
                    Arc::clone(&config.playlists),
                    TimestampInput::MillisAbsolute,
                    config
                        .min_part_ms
                        .max(DEFAULT_MIN_PART_MS.min(config.min_part_ms)),
                    Some(Arc::clone(&config.publisher)),
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
        state.pcm_encoder = None;
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

    let flac = match group.payload_kind {
        AudioPayloadKind::Flac => group.payload,
        AudioPayloadKind::Pcm => encode_pcm_group(state, &group)?,
        AudioPayloadKind::Opus => decode_opus_and_encode_flac(state, &group)?,
    };
    let pts_ms = samples_to_millis(group.pts_samples, group.sample_rate)?;
    state
        .segmenter
        .push_access_unit(AccessUnit {
            key: true,
            pts: pts_ms,
            dts: pts_ms,
            data: flac,
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
        .pcm_encoder
        .as_ref()
        .is_none_or(|(current, _)| *current != encoder_config)
    {
        state.pcm_encoder = Some((
            encoder_config,
            FlacFrameEncoder::new(encoder_config).map_err(|error| error.to_string())?,
        ));
    }
    let encoded = state
        .pcm_encoder
        .as_mut()
        .expect("FLAC encoder initialized")
        .1
        .encode_i16(&decoded)
        .map_err(|error| error.to_string())?;
    Ok(Bytes::from(encoded.payload))
}

fn encode_pcm_group(
    state: &mut RenditionState,
    group: &DecodedMultichannelAudioGroup,
) -> Result<Bytes, String> {
    let bits_per_sample = match group.sample_format {
        AudioSampleFormat::S16Le => 16,
        AudioSampleFormat::S24Le => 24,
        AudioSampleFormat::S32Le | AudioSampleFormat::F32Le => 24,
        other => {
            return Err(format!(
                "PCM {:?} cannot be represented by the LL-HLS FLAC encoder",
                other
            ))
        }
    };
    let encoder_config = FlacFrameConfig::new(
        group.sample_rate,
        group.channel_count,
        bits_per_sample,
        group.frame_count,
        FlacProfile::Realtime,
    )
    .map_err(|error| error.to_string())?;
    if state
        .pcm_encoder
        .as_ref()
        .is_none_or(|(current, _)| *current != encoder_config)
    {
        state.pcm_encoder = Some((
            encoder_config,
            FlacFrameEncoder::new(encoder_config).map_err(|error| error.to_string())?,
        ));
    }
    let encoder = &mut state
        .pcm_encoder
        .as_mut()
        .expect("PCM encoder initialized")
        .1;
    if group.flags & AUDIO_GROUP_FLAG_DISCONTINUITY != 0 {
        encoder.reset();
    }
    let encoded = match group.sample_format {
        AudioSampleFormat::S16Le => {
            if !group.payload.len().is_multiple_of(2) {
                return Err("S16LE AEP1 payload has an odd byte length".to_string());
            }
            let samples = group
                .payload
                .chunks_exact(2)
                .map(|bytes| i16::from_le_bytes([bytes[0], bytes[1]]))
                .collect::<Vec<_>>();
            encoder.encode_i16(&samples)
        }
        AudioSampleFormat::S24Le => encoder.encode_s24le(&group.payload),
        AudioSampleFormat::S32Le => {
            if !group.payload.len().is_multiple_of(4) {
                return Err("S32LE AEP1 payload is not aligned to four bytes".to_string());
            }
            let samples = group
                .payload
                .chunks_exact(4)
                .map(|bytes| i32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]) >> 8)
                .collect::<Vec<_>>();
            encoder.encode_i32(&samples)
        }
        AudioSampleFormat::F32Le => {
            if !group.payload.len().is_multiple_of(4) {
                return Err("F32LE AEP1 payload is not aligned to four bytes".to_string());
            }
            let samples = group
                .payload
                .chunks_exact(4)
                .map(|bytes| {
                    normalized_f32_to_s24(f32::from_le_bytes([
                        bytes[0], bytes[1], bytes[2], bytes[3],
                    ]))
                })
                .collect::<Vec<_>>();
            encoder.encode_i32(&samples)
        }
        _ => unreachable!("validated above"),
    }
    .map_err(|error| error.to_string())?;
    Ok(Bytes::from(encoded.payload))
}

fn normalized_f32_to_s24(sample: f32) -> i32 {
    if !sample.is_finite() {
        return 0;
    }
    if sample <= -1.0 {
        return -8_388_608;
    }
    if sample >= 1.0 {
        return 8_388_607;
    }
    (f64::from(sample) * 8_388_608.0).round() as i32
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

    #[test]
    fn sample_pts_maps_exactly_at_48khz_epoch_boundaries() {
        assert_eq!(samples_to_millis(0, 48_000).unwrap(), 0);
        assert_eq!(samples_to_millis(240, 48_000).unwrap(), 5);
        assert_eq!(samples_to_millis(720, 48_000).unwrap(), 15);
        assert_eq!(samples_to_millis(48_000, 48_000).unwrap(), 1_000);
        assert_eq!(normalized_f32_to_s24(-1.0), -8_388_608);
        assert_eq!(normalized_f32_to_s24(0.0), 0);
        assert_eq!(normalized_f32_to_s24(1.0), 8_388_607);
        assert_eq!(normalized_f32_to_s24(f32::NAN), 0);
    }

    #[tokio::test]
    async fn every_aep1_pcm_sample_format_reaches_flac_fmp4_ll_hls() {
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
        assert!(!parts.is_empty());
        assert!(parts.iter().all(|part| (2..=5).contains(&part.stream_id)));
        assert!(parts.iter().all(|part| part.audio_codec == Some("flac")));
        assert!(parts.iter().all(|part| part.video_units == 0));
        assert!(parts.iter().any(|part| part
            .init
            .as_ref()
            .is_some_and(|init| init.windows(4).any(|bytes| bytes == b"fLaC"))));
        assert_eq!(playlists.active(), 4);
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
        assert_eq!(playlists.active(), 1);
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
