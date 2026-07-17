use anyhow::{bail, Context, Result};
use av_contrib::audio_epoch_hls::{
    channel as audio_epoch_hls_channel, run_audio_epoch_hls_worker,
    worker_stats as audio_epoch_hls_worker_stats, AudioEpochHlsConfig, AudioEpochHlsDatagram,
    DEFAULT_AUDIO_EPOCH_HLS_QUEUE_CAPACITY,
};
use av_contrib::fmp4_bridge::{
    Fmp4PartPublisher, Fmp4Segmenter, MpegTsContinuityIssue, MpegTsPayloadDrop, PublishedFmp4Part,
    TimestampInput, TsFmp4Bridge, DEFAULT_MIN_PART_MS,
};
use av_contrib::ingress_authorization::{
    decode_envelope_header_bytes, gate_from_bootstrap_path, parse_bearer_header,
    parse_content_length_header, PublishAuthorizationMode, PublishIngressError, PublishIngressGate,
    PublishIngressRequest, PublishLease, PublishRejectionCode, MEDIA_FRAME_ENVELOPE_HEADER,
};
use av_contrib::{codec_name, MediaAccessUnitParams};
use av_hls::{HlsHandler, HlsRouter};
use av_upload_response::{
    PureRistIngest as UploadPureRistIngest, PureRistProfile as UploadPureRistProfile,
    SrtIngest as UploadSrtIngest, TailSlot, UploadResponseConfig, UploadResponseService,
};
use av_web_service::{
    load_default_tls_base64, load_tls_base64_from_paths, BodyStream, H2H3Server, HandlerResponse,
    HandlerResult, Router, Server, ServerBuilder, ServerError, StreamWriter, WebSocketHandler,
    WebTransportHandler,
};
use bytes::{Bytes, BytesMut};
use clap::{Parser, ValueEnum};
use futures_util::StreamExt;
use http::{
    header::{AUTHORIZATION, CONTENT_LENGTH},
    Method, Request, Response, StatusCode,
};
use media_object::{
    ClockConfidence, ClockTimestamp, MediaObject, ObjectKey, ObjectKind, PayloadHash, Stage,
    StageTimestamp,
};
use raptorq_datagram_fec::{
    inspect_multichannel_audio_datagram, source_symbol_count, DatagramFecEncoder,
    DatagramFecHeader, MediaCodec, MediaFecDecoder, MediaFecEncoder, MediaFrame,
    MediaFrameMetadata, DEFAULT_SOURCE_SYMBOLS, DEFAULT_SYMBOL_SIZE, ENCODING_PACKET_HEADER_LEN,
    HEADER_LEN, MAX_SOURCE_SYMBOLS_PER_BLOCK,
};
use relay_session::{
    encoded_datagram_len as relay_datagram_len, EncodedRaptorQObject, MediaDatagramRole,
    MediaDeadline, ObjectAnnouncement, PathMetrics, PrivateUdpConfig, PrivateUdpTransport,
    RaptorQObjectEncoder, RelayLimits, RelayTransport, SubscriptionId, TopologyGeneration,
};
use rtmp_ingress::ingress::start_rtmp_listener;
use rtmp_ingress::{RtmpIngestEvent, RtmpStreamInfo};
use serde::Serialize;
use socket2::SockRef;
use std::collections::{HashMap, HashSet, VecDeque};
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::{
    atomic::{AtomicU32, AtomicU64, Ordering},
    Arc, Mutex as StdMutex,
};
use std::time::Duration;
use tokio::net::UdpSocket;
use tokio::sync::{mpsc, watch, Mutex, RwLock};
use tokio::time::{interval, Instant, MissedTickBehavior};
use tracing::{debug, info, trace, warn};

const DEFAULT_FLOW_ID: u32 = 0x1122_3344;
const MEDIA_ACCESS_UNIT_PATH: &str = "/media/access-unit";
const CONTRIB_STATUS_PATH: &str = "/api/status";
const CONTRIB_STATUS_EVENTS_PATH: &str = "/api/status/events";
const CONTRIB_METRICS_PATH: &str = "/metrics";
const PROMETHEUS_CONTENT_TYPE: &str = "text/plain; version=0.0.4; charset=utf-8";
const DAW_RELAY_SUBSCRIBE_MESSAGE: &[u8] = b"WAVEY-DAW-SUBSCRIBE/1";
const DAW_RELAY_UNSUBSCRIBE_MESSAGE: &[u8] = b"WAVEY-DAW-UNSUBSCRIBE/1";
const DAW_RELAY_SUBSCRIBE_ACK: &[u8] = b"WAVEY-DAW-SUBSCRIBED/1";
const DAW_RELAY_SUBSCRIBE_V2_PREFIX: &[u8] = b"WAVEY-DAW-SUBSCRIBE/2 ";
const DAW_RELAY_UNSUBSCRIBE_V2_PREFIX: &[u8] = b"WAVEY-DAW-UNSUBSCRIBE/2 ";
const DAW_RELAY_SUBSCRIBE_ACK_V2_PREFIX: &[u8] = b"WAVEY-DAW-SUBSCRIBED/2 ";
const DAW_RELAY_TARGET_TTL: Duration = Duration::from_secs(15);
const DAW_RELAY_CLEANUP_INTERVAL: Duration = Duration::from_secs(1);
const DAW_MEDIA_RECEIVE_BUFFER_BYTES: usize = 8 * 1024 * 1024;
const MULTICHANNEL_AUDIO_TRANSPORT_MAGIC: &[u8] = b"AEP1";
const MEDIA_OBJECT_TENANT: &str = "default";
const FMP4_MEDIA_TRACK: &str = "muxed-fmp4";
const FMP4_INITIALIZATION_TRACK: &str = "muxed-fmp4-init";
const MEDIA_OBJECT_VERSION: u32 = 1;
const MESH_FMP4_SLOT_MAGIC: &[u8; 8] = b"AVFMP4S1";
const MESH_FMP4_SLOT_HEADER_LEN: usize = 16;
const AV_CONTRIB_CLOCK_ID: &str = "system:realtime:av-contrib";
const DEFAULT_WALL_CLOCK_ESTIMATED_ERROR_MS: u64 = 1_000;
const DEFAULT_RELAY_DEADLINE_MS: u64 = 1_000;
const DEFAULT_RELAY_TOPOLOGY_GENERATION: u64 = 1;
const DEFAULT_RELAY_SUBSCRIPTION_ID: u64 = 1;
const RELAY_LANE_IMPAIRED_HOLD_MS: u64 = 3_000;
static AUDIO_EPOCH_HLS_QUEUE_CAPACITY: AtomicU64 = AtomicU64::new(0);
static AUDIO_EPOCH_HLS_QUEUE_ENQUEUED: AtomicU64 = AtomicU64::new(0);
static AUDIO_EPOCH_HLS_QUEUE_DROPPED: AtomicU64 = AtomicU64::new(0);
static AUDIO_EPOCH_HLS_QUEUE_MAX_DEPTH: AtomicU64 = AtomicU64::new(0);

fn should_log_audio_epoch_hls_drop(dropped_total: u64) -> bool {
    dropped_total <= 16 || dropped_total.is_power_of_two() || dropped_total.is_multiple_of(10_000)
}

fn encode_mesh_fmp4_slot(init: Option<&Bytes>, media: &Bytes) -> Result<Bytes> {
    let init_len = init.map_or(0, Bytes::len);
    if init_len > u32::MAX as usize {
        bail!("fMP4 init segment too large for mesh slot envelope");
    }
    if media.len() > u32::MAX as usize {
        bail!("fMP4 media fragment too large for mesh slot envelope");
    }

    let mut out = Vec::with_capacity(MESH_FMP4_SLOT_HEADER_LEN + init_len + media.len());
    out.extend_from_slice(MESH_FMP4_SLOT_MAGIC);
    out.extend_from_slice(&(init_len as u32).to_be_bytes());
    out.extend_from_slice(&(media.len() as u32).to_be_bytes());
    if let Some(init) = init {
        out.extend_from_slice(init);
    }
    out.extend_from_slice(media);
    Ok(Bytes::from(out))
}

#[cfg(test)]
fn build_fmp4_initialization_object(
    stream_id: u64,
    source_epoch: u64,
    payload: &[u8],
) -> Result<(MediaObject, u64)> {
    build_fmp4_initialization_object_with_deadline(stream_id, source_epoch, payload, None)
}

fn build_live_fmp4_initialization_object(
    stream_id: u64,
    source_epoch: u64,
    payload: &[u8],
    published_at_unix_ns: i64,
    delivery_budget_ms: u64,
    estimated_clock_error_ns: u64,
) -> Result<(MediaObject, u64)> {
    let deadline_ns = i128::from(published_at_unix_ns)
        .checked_add(i128::from(delivery_budget_ms) * 1_000_000)
        .and_then(|value| i64::try_from(value).ok())
        .context("canonical initialization deadline exceeds Unix-nanosecond range")?;
    let deadline = canonical_contributor_timestamp(deadline_ns, estimated_clock_error_ns)?;
    build_fmp4_initialization_object_with_deadline(stream_id, source_epoch, payload, Some(deadline))
}

fn build_fmp4_initialization_object_with_deadline(
    stream_id: u64,
    source_epoch: u64,
    payload: &[u8],
    deadline: Option<ClockTimestamp>,
) -> Result<(MediaObject, u64)> {
    let payload_hash = PayloadHash::digest(payload);
    let mut epoch_bytes = [0_u8; 8];
    epoch_bytes.copy_from_slice(&payload_hash.as_bytes()[..8]);
    let configuration_epoch = u64::from_be_bytes(epoch_bytes);
    let key = ObjectKey::new(
        MEDIA_OBJECT_TENANT,
        stream_id.to_string(),
        FMP4_INITIALIZATION_TRACK,
        source_epoch,
        configuration_epoch,
        0,
        MEDIA_OBJECT_VERSION,
        payload_hash,
    )
    .context("invalid canonical fMP4 initialization identity")?;
    let mut builder = MediaObject::builder(key, ObjectKind::Initialization, payload.to_vec())
        .with_configuration_epoch(configuration_epoch)
        .with_metadata("container", b"fmp4".to_vec())
        .with_metadata("payload-format", b"fmp4-init-v1".to_vec());
    if let Some(deadline) = deadline {
        builder = builder.with_deadline(deadline);
    }
    let object = builder
        .build()
        .context("invalid canonical fMP4 initialization object")?;
    Ok((object, configuration_epoch))
}

fn build_fmp4_media_object(
    part: &PublishedFmp4Part,
    payload: &[u8],
    initialization_key: ObjectKey,
    configuration_epoch: u64,
    source_epoch: u64,
    delivery_budget_ms: u64,
    estimated_clock_error_ns: u64,
) -> Result<MediaObject> {
    if delivery_budget_ms == 0 {
        bail!("canonical media delivery budget must be positive");
    }
    let key = ObjectKey::for_payload(
        MEDIA_OBJECT_TENANT,
        part.stream_id.to_string(),
        FMP4_MEDIA_TRACK,
        source_epoch,
        0,
        part.sequence,
        MEDIA_OBJECT_VERSION,
        payload,
    )
    .context("invalid canonical fMP4 media identity")?;
    let packaged =
        canonical_contributor_timestamp(part.packaged_at_unix_ns, estimated_clock_error_ns)?;
    let published =
        canonical_contributor_timestamp(part.published_at_unix_ns, estimated_clock_error_ns)?;
    let deadline_ns = i128::from(part.published_at_unix_ns)
        .checked_add(i128::from(delivery_budget_ms) * 1_000_000)
        .and_then(|value| i64::try_from(value).ok())
        .context("canonical media deadline exceeds Unix-nanosecond range")?;
    let deadline = canonical_contributor_timestamp(deadline_ns, estimated_clock_error_ns)?;

    let mut builder = MediaObject::builder(key, ObjectKind::Media, payload.to_vec())
        .with_keyframe(part.keyframe)
        .with_configuration_epoch(configuration_epoch)
        .with_deadline(deadline)
        .with_stage_timestamp(StageTimestamp::new(Stage::Packaged, packaged))
        .with_stage_timestamp(StageTimestamp::new(Stage::Published, published))
        .with_dependency(initialization_key)
        .with_metadata("container", b"fmp4".to_vec())
        .with_metadata("duration-ms", part.duration_ms.to_string().into_bytes())
        .with_metadata("payload-format", b"fmp4-slot-v1".to_vec())
        .with_metadata(
            "scheduler-class",
            fmp4_scheduler_class(part).as_bytes().to_vec(),
        )
        .with_metadata(
            "track-composition",
            fmp4_track_composition(part).as_bytes().to_vec(),
        );
    if let Some(codec) = part.video_codec {
        builder = builder.with_metadata("video-codec", codec.as_bytes().to_vec());
    }
    if let Some(codec) = part.audio_codec {
        builder = builder.with_metadata("audio-codec", codec.as_bytes().to_vec());
    }
    builder
        .build()
        .context("invalid canonical fMP4 media object")
}

fn canonical_contributor_timestamp(
    unix_time_ns: i64,
    estimated_clock_error_ns: u64,
) -> Result<ClockTimestamp> {
    ClockTimestamp::new(
        unix_time_ns,
        AV_CONTRIB_CLOCK_ID,
        ClockConfidence::estimated(estimated_clock_error_ns),
    )
    .context("invalid av-contrib wall-clock timestamp")
}

fn relay_deadline_for_object(object: &MediaObject) -> Result<MediaDeadline> {
    let deadline = object
        .deadline()
        .context("RelaySession live media requires a canonical object deadline")?;
    let deadline_ns = u64::try_from(deadline.unix_time_ns())
        .context("RelaySession wire deadline requires a non-negative Unix timestamp")?;
    Ok(MediaDeadline::from_micros(deadline_ns.div_ceil(1_000)))
}

fn fmp4_scheduler_class(part: &PublishedFmp4Part) -> &'static str {
    if part.video_units > 0 && part.keyframe {
        "video-keyframe"
    } else if part.audio_units > 0 {
        "audio"
    } else if part.video_units > 0 {
        "video-delta"
    } else {
        "data"
    }
}

fn fmp4_track_composition(part: &PublishedFmp4Part) -> &'static str {
    match (part.video_units > 0, part.audio_units > 0) {
        (true, true) => "audio+video",
        (true, false) => "video",
        (false, true) => "audio",
        (false, false) => "empty",
    }
}

fn encode_canonical_media_object(object: &MediaObject) -> Result<Bytes> {
    Ok(Bytes::from(
        media_object::encode(object).context("failed to encode canonical media object")?,
    ))
}

fn is_multichannel_audio_transport_datagram(datagram: &[u8]) -> bool {
    datagram.starts_with(MULTICHANNEL_AUDIO_TRANSPORT_MAGIC)
}
const UPLOAD_RESPONSE_HLS_WORKER_ID: &str = "av-contrib-upload-response-fmp4-bridge";
const HLS_BRIDGE_POLL_MS: u64 = 5;
const DEFAULT_SEGMENT_MS: u32 = 1_000;
const DEFAULT_TARGET_DURATION_MS: u32 = 6_000;
const CONTRIB_ACTIVITY_LIMIT: usize = 64;
const CONTRIB_HLS_RESPONSE_LIMIT: usize = 32;
const CONTRIB_INGEST_SESSION_LIMIT: usize = 48;
const CONTRIB_MIN_STALE_OUTPUT_MS: u64 = 5_000;
const MAX_STREAM_FEC_OBJECT_BYTES: usize = 8 * 1024 * 1024;
const MAX_STREAM_FEC_DATAGRAMS: u32 = 32_768;
const MAX_MEDIA_ACCESS_UNIT_BYTES: usize = 8 * 1024 * 1024;
const DURATION_HISTOGRAM_BUCKETS_US: [u64; 13] = [
    100, 250, 500, 1_000, 2_500, 5_000, 10_000, 25_000, 50_000, 100_000, 250_000, 500_000,
    1_000_000,
];

#[derive(Debug, Parser)]
#[command(
    name = "av-contrib",
    about = "Run a contributor-facing AV service that forwards bytes into av-mesh"
)]
struct Args {
    #[arg(long, default_value_t = 9443)]
    http_port: u16,

    #[arg(long)]
    cert: Option<PathBuf>,

    #[arg(long)]
    key: Option<PathBuf>,

    #[arg(long, default_value = "127.0.0.1:12001")]
    mesh_fec_target: SocketAddr,

    #[arg(long, default_value = "127.0.0.1:12101")]
    mesh_media_fec_target: SocketAddr,

    /// Opt in to RelaySession live-media emission toward the assigned primary.
    #[arg(long, env = "AV_RELAY_PRIMARY_TARGET")]
    relay_primary_target: Option<SocketAddr>,

    /// Stable source endpoint registered as the contributor on the primary relay.
    #[arg(long, env = "AV_RELAY_PRIMARY_BIND")]
    relay_primary_bind: Option<SocketAddr>,

    /// Send initial RaptorQ repair symbols through an independent warm parent.
    #[arg(long, env = "AV_RELAY_SECONDARY_TARGET")]
    relay_secondary_target: Option<SocketAddr>,

    /// Stable source endpoint registered as the contributor on the secondary relay.
    #[arg(long, env = "AV_RELAY_SECONDARY_BIND")]
    relay_secondary_bind: Option<SocketAddr>,

    /// Keep the independent backbone parent warm with the complete source
    /// symbol set as well as its repair lane. Origin fanout remains bounded to
    /// the two compiled backbone parents and promotion needs no object refill.
    #[arg(long, env = "AV_RELAY_SECONDARY_SEED_SOURCE")]
    relay_secondary_seed_source: bool,

    /// Publish live fMP4 objects through RelaySession only. This makes DAG
    /// qualification unable to pass through the older byte-FEC cache lane.
    #[arg(long, env = "AV_RELAY_EXCLUSIVE")]
    relay_exclusive: bool,

    #[arg(long, env = "AV_RELAY_LOCAL_ID", default_value = "av-contrib")]
    relay_local_id: String,

    #[arg(
        long,
        env = "AV_RELAY_PRIMARY_ID",
        default_value = "needletail-relay-primary"
    )]
    relay_primary_id: String,

    #[arg(
        long,
        env = "AV_RELAY_SECONDARY_ID",
        default_value = "needletail-relay-secondary"
    )]
    relay_secondary_id: String,

    #[arg(
        long,
        env = "AV_RELAY_TOPOLOGY_GENERATION",
        default_value_t = DEFAULT_RELAY_TOPOLOGY_GENERATION
    )]
    relay_topology_generation: u64,

    #[arg(
        long,
        env = "AV_RELAY_SUBSCRIPTION_ID",
        default_value_t = DEFAULT_RELAY_SUBSCRIPTION_ID
    )]
    relay_subscription_id: u64,

    /// Canonical fMP4 delivery budget from publication handoff to object expiry.
    #[arg(
        long,
        env = "AV_RELAY_DEADLINE_MS",
        default_value_t = DEFAULT_RELAY_DEADLINE_MS
    )]
    relay_deadline_ms: u64,

    /// Controller-observed loss across the selected source path, as a fraction
    /// from zero through one. This seeds the adaptive RaptorQ policy until live
    /// carrier feedback supplies a newer observation.
    #[arg(long, env = "AV_RELAY_PATH_LOSS_FRACTION", default_value_t = 0.0)]
    relay_path_loss_fraction: f32,

    /// Fastest measured direct origin-to-destination RTT in milliseconds. The
    /// controller compares this baseline with the selected route RTT.
    #[arg(long, env = "AV_RELAY_PATH_BEST_DIRECT_RTT_MS", default_value_t = 0.0)]
    relay_path_best_direct_rtt_ms: f32,

    /// Controller-observed end-to-end source-path RTT in milliseconds.
    #[arg(long, env = "AV_RELAY_PATH_RTT_MS", default_value_t = 0.0)]
    relay_path_rtt_ms: f32,

    /// Controller-observed source-path jitter in milliseconds.
    #[arg(long, env = "AV_RELAY_PATH_JITTER_MS", default_value_t = 0.0)]
    relay_path_jitter_ms: f32,

    /// Controller-observed source-path queue delay in milliseconds.
    #[arg(long, env = "AV_RELAY_PATH_QUEUE_DELAY_MS", default_value_t = 0.0)]
    relay_path_queue_delay_ms: f32,

    /// Wall-clock age anchor supplied with the controller path observation.
    #[arg(long, env = "AV_RELAY_PATH_OBSERVED_AT_UNIX_MS")]
    relay_path_observed_at_unix_ms: Option<u64>,

    /// Controller-observed loss across the independent warm-secondary route.
    #[arg(
        long,
        env = "AV_RELAY_SECONDARY_PATH_LOSS_FRACTION",
        default_value_t = 0.0
    )]
    relay_secondary_path_loss_fraction: f32,

    /// Fastest measured direct origin-to-destination RTT used by the warm route.
    #[arg(
        long,
        env = "AV_RELAY_SECONDARY_PATH_BEST_DIRECT_RTT_MS",
        default_value_t = 0.0
    )]
    relay_secondary_path_best_direct_rtt_ms: f32,

    /// Controller-observed end-to-end RTT through the warm parent.
    #[arg(long, env = "AV_RELAY_SECONDARY_PATH_RTT_MS", default_value_t = 0.0)]
    relay_secondary_path_rtt_ms: f32,

    /// Controller-observed warm-route jitter.
    #[arg(long, env = "AV_RELAY_SECONDARY_PATH_JITTER_MS", default_value_t = 0.0)]
    relay_secondary_path_jitter_ms: f32,

    /// Controller-observed warm-route queue delay.
    #[arg(
        long,
        env = "AV_RELAY_SECONDARY_PATH_QUEUE_DELAY_MS",
        default_value_t = 0.0
    )]
    relay_secondary_path_queue_delay_ms: f32,

    /// Wall-clock age anchor supplied with the warm-route observation.
    #[arg(long, env = "AV_RELAY_SECONDARY_PATH_OBSERVED_AT_UNIX_MS")]
    relay_secondary_path_observed_at_unix_ms: Option<u64>,

    /// Estimated maximum error of the host realtime clock used in MOBJ timestamps.
    #[arg(
        long,
        env = "AV_WALL_CLOCK_ESTIMATED_ERROR_MS",
        default_value_t = DEFAULT_WALL_CLOCK_ESTIMATED_ERROR_MS
    )]
    wall_clock_estimated_error_ms: u64,

    #[arg(long)]
    daw_media_bind: Option<SocketAddr>,

    /// Bounded handoff from the live AEP1 datagram path to lossless LL-HLS packaging.
    #[arg(
        long,
        env = "AV_DAW_HLS_QUEUE_CAPACITY",
        default_value_t = DEFAULT_AUDIO_EPOCH_HLS_QUEUE_CAPACITY
    )]
    daw_hls_queue_capacity: usize,

    #[arg(long, default_value_t = 1)]
    stream_id: u64,

    #[arg(long, default_value_t = 0)]
    rist_stream_id: u64,

    #[arg(long, default_value_t = 6)]
    srt_stream_id: u64,

    #[arg(long, default_value_t = 7)]
    rtmp_stream_id: u64,

    #[arg(long, default_value_t = 1)]
    repair_symbols: u32,

    #[arg(long, default_value_t = DEFAULT_SYMBOL_SIZE)]
    symbol_size: u16,

    #[arg(long)]
    rist_bind: Option<SocketAddr>,

    #[arg(long, value_enum, default_value = "main")]
    rist_profile: RistProfile,

    #[arg(long, value_enum, default_value = "pure")]
    rist_backend: RistBackend,

    #[arg(long, value_parser = parse_u32_auto, default_value_t = DEFAULT_FLOW_ID)]
    rist_flow_id: u32,

    #[arg(long)]
    srt_bind: Option<SocketAddr>,

    #[arg(long)]
    rtmp_bind: Option<SocketAddr>,

    #[arg(long, env = "AV_LL_HLS_PART_MS", default_value_t = DEFAULT_MIN_PART_MS)]
    fmp4_part_ms: u32,

    #[arg(long, env = "AV_LL_HLS_SEGMENT_MS", default_value_t = DEFAULT_SEGMENT_MS)]
    fmp4_segment_ms: u32,

    #[arg(long, env = "AV_LL_HLS_TARGET_DURATION_MS", default_value_t = DEFAULT_TARGET_DURATION_MS)]
    hls_target_duration_ms: u32,

    #[arg(long, default_value_t = 65)]
    playlist_count: usize,

    #[arg(long, default_value_t = 800)]
    playlist_buffer_kb: usize,
}

#[derive(Debug, Clone, Copy, ValueEnum)]
enum RistProfile {
    Simple,
    Main,
}

impl RistProfile {
    fn as_str(self) -> &'static str {
        match self {
            Self::Simple => "simple",
            Self::Main => "main",
        }
    }
}

impl From<RistProfile> for UploadPureRistProfile {
    fn from(profile: RistProfile) -> Self {
        match profile {
            RistProfile::Simple => Self::Simple,
            RistProfile::Main => Self::Main,
        }
    }
}

#[derive(Debug, Clone, Copy, ValueEnum)]
enum RistBackend {
    Pure,
}

impl RistBackend {
    fn as_str(self) -> &'static str {
        match self {
            Self::Pure => "pure",
        }
    }
}

#[derive(Debug, Clone, Copy)]
struct RistIngestConfig {
    bind: SocketAddr,
    profile: RistProfile,
    flow_id: u32,
    output_stream_id: u64,
    output_stream_idx: usize,
    min_part_ms: u32,
}

#[derive(Debug)]
struct RelayPublishOutcome {
    announcement: ObjectAnnouncement,
    source_symbols: usize,
    repair_symbols: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RelayCarrierPath {
    Primary,
    Secondary,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
enum RelayLaneState {
    Unknown = 0,
    Healthy = 1,
    Impaired = 2,
}

impl RelayLaneState {
    fn as_str(self) -> &'static str {
        match self {
            Self::Unknown => "unknown",
            Self::Healthy => "healthy",
            Self::Impaired => "impaired",
        }
    }
}

fn relay_lane_state(
    now_ms: u64,
    last_success_unix_ms: u64,
    last_failure_unix_ms: u64,
) -> RelayLaneState {
    if last_success_unix_ms == 0 && last_failure_unix_ms == 0 {
        RelayLaneState::Unknown
    } else if last_failure_unix_ms > 0
        && (last_success_unix_ms <= last_failure_unix_ms
            || now_ms.saturating_sub(last_failure_unix_ms) < RELAY_LANE_IMPAIRED_HOLD_MS)
    {
        RelayLaneState::Impaired
    } else {
        RelayLaneState::Healthy
    }
}

fn resolve_relay_lane_results(
    primary: Result<()>,
    secondary: Option<Result<()>>,
    secondary_carries_source: bool,
) -> Result<bool> {
    match (primary, secondary) {
        (Ok(()), None | Some(Ok(()))) => Ok(false),
        (Ok(()), Some(Err(_))) => Ok(true),
        (Err(_), Some(Ok(()))) if secondary_carries_source => Ok(true),
        (Err(primary), Some(Ok(()))) => Err(primary).context(
            "primary RelaySession lane failed and the secondary lane is repair-only",
        ),
        (Err(primary), Some(Err(secondary))) => Err(anyhow::anyhow!(
            "all configured RelaySession lanes failed; primary: {primary:#}; secondary: {secondary:#}"
        )),
        (Err(primary), None) => {
            Err(primary).context("the only configured RelaySession lane failed")
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RelayPipelineStage {
    Total,
    EncodeWait,
    Encode,
    Schedule,
    PrimarySourceSend,
    SecondarySourceSend,
    PrimaryRepairSend,
    SecondaryRepairSend,
}

struct RelaySessionPublisher {
    encoder: Mutex<RaptorQObjectEncoder>,
    primary: PrivateUdpTransport,
    secondary: Option<PrivateUdpTransport>,
    secondary_seed_source: bool,
    generation: TopologyGeneration,
    subscription_id: SubscriptionId,
    telemetry: Arc<IngestTelemetry>,
}

impl RelaySessionPublisher {
    async fn new(args: &Args, telemetry: Arc<IngestTelemetry>) -> Result<Option<Self>> {
        let Some(primary_target) = args.relay_primary_target else {
            if args.relay_primary_bind.is_some()
                || args.relay_secondary_target.is_some()
                || args.relay_secondary_bind.is_some()
            {
                bail!("relay bind and secondary settings require --relay-primary-target");
            }
            if relay_path_metrics_configured(args) {
                bail!("relay path observations require --relay-primary-target");
            }
            return Ok(None);
        };
        if args.relay_secondary_target.is_none() && args.relay_secondary_bind.is_some() {
            bail!("--relay-secondary-bind requires --relay-secondary-target");
        }
        if args.relay_secondary_seed_source && args.relay_secondary_target.is_none() {
            bail!("--relay-secondary-seed-source requires --relay-secondary-target");
        }
        if args.relay_secondary_target == Some(primary_target) {
            bail!("relay primary and secondary targets must use distinct socket addresses");
        }
        if args.relay_deadline_ms == 0 {
            bail!("--relay-deadline-ms must be positive");
        }

        let limits = RelayLimits::default();
        let primary_bind = relay_bind_addr(
            "--relay-primary-bind",
            args.relay_primary_bind,
            primary_target,
        )?;
        let secondary_bind = args
            .relay_secondary_target
            .map(|target| {
                relay_bind_addr("--relay-secondary-bind", args.relay_secondary_bind, target)
            })
            .transpose()?;
        let mut endpoints = vec![("primary target", primary_target)];
        if let Some(bind) = args.relay_primary_bind {
            endpoints.push(("primary bind", bind));
        }
        if let Some(target) = args.relay_secondary_target {
            endpoints.push(("secondary target", target));
        }
        if let Some(bind) = args.relay_secondary_bind {
            endpoints.push(("secondary bind", bind));
        }
        for left in 0..endpoints.len() {
            for right in (left + 1)..endpoints.len() {
                if endpoints[left].1 == endpoints[right].1 {
                    bail!(
                        "relay {} and {} must use distinct socket addresses",
                        endpoints[left].0,
                        endpoints[right].0
                    );
                }
            }
        }
        let primary_path_metrics = relay_path_metrics(args)?;
        let secondary_path_metrics = relay_secondary_path_metrics(args)?;
        let primary = PrivateUdpTransport::bind(PrivateUdpConfig::new(
            primary_bind,
            primary_target,
            args.relay_local_id.clone(),
            args.relay_primary_id.clone(),
            limits,
        )?)
        .await
        .with_context(|| {
            format!("failed to bind primary RelaySession UDP carrier for {primary_target}")
        })?;
        primary.set_path_metrics(primary_path_metrics);
        let secondary = if let (Some(secondary_target), Some(secondary_bind)) =
            (args.relay_secondary_target, secondary_bind)
        {
            let transport = PrivateUdpTransport::bind(PrivateUdpConfig::new(
                secondary_bind,
                secondary_target,
                args.relay_local_id.clone(),
                args.relay_secondary_id.clone(),
                limits,
            )?)
            .await
            .with_context(|| {
                format!("failed to bind secondary RelaySession UDP carrier for {secondary_target}")
            })?;
            transport.set_path_metrics(secondary_path_metrics);
            Some(transport)
        } else {
            None
        };

        let mut encoder = RaptorQObjectEncoder::default();
        encoder.update_path_metrics(adaptive_relay_path_metrics(
            primary_path_metrics,
            secondary_path_metrics,
        ));

        Ok(Some(Self {
            encoder: Mutex::new(encoder),
            primary,
            secondary,
            secondary_seed_source: args.relay_secondary_seed_source,
            generation: TopologyGeneration::new(args.relay_topology_generation)?,
            subscription_id: SubscriptionId::new(args.relay_subscription_id)?,
            telemetry,
        }))
    }

    async fn publish_object(&self, object: &MediaObject) -> Result<RelayPublishOutcome> {
        let started = Instant::now();
        let result = self.publish_object_inner(object).await;
        self.telemetry
            .record_relay_session_stage_duration(RelayPipelineStage::Total, started.elapsed());
        result
    }

    async fn publish_object_inner(&self, object: &MediaObject) -> Result<RelayPublishOutcome> {
        let deadline = relay_deadline_for_object(object)?;
        if deadline.is_expired_at(now_unix_us()) {
            self.telemetry
                .relay_expired_objects
                .fetch_add(1, Ordering::Relaxed);
            self.telemetry
                .relay_deadline_misses
                .fetch_add(1, Ordering::Relaxed);
            bail!("canonical media object expired before RelaySession encoding");
        }
        let encode_wait_started = Instant::now();
        let mut encoder = self.encoder.lock().await;
        self.telemetry.record_relay_session_stage_duration(
            RelayPipelineStage::EncodeWait,
            encode_wait_started.elapsed(),
        );
        let encode_started = Instant::now();
        let encoded = encoder.encode_object_with_inferred_priority(
            object,
            self.generation,
            self.subscription_id,
            deadline,
        );
        drop(encoder);
        self.telemetry.record_relay_session_stage_duration(
            RelayPipelineStage::Encode,
            encode_started.elapsed(),
        );
        let EncodedRaptorQObject {
            announcement,
            source_symbols,
            repair_symbols,
        } = match encoded {
            Ok(encoded) => encoded,
            Err(error) => {
                self.telemetry.record_relay_session_encode_error();
                return Err(error).context("failed to RaptorQ-protect canonical media object");
            }
        };

        let schedule_started = Instant::now();
        let source_count = source_symbols.len();
        let repair_count = repair_symbols.len();
        let repair_uses_primary = self.secondary.is_none();
        if repair_uses_primary && repair_count > 0 {
            self.telemetry
                .relay_repair_primary_fallback_objects
                .fetch_add(1, Ordering::Relaxed);
        }
        self.telemetry.record_relay_session_stage_duration(
            RelayPipelineStage::Schedule,
            schedule_started.elapsed(),
        );

        let primary_lane = async {
            for symbol in source_symbols.iter().cloned() {
                self.send_symbol(&self.primary, RelayCarrierPath::Primary, symbol)
                    .await?;
            }
            if repair_uses_primary {
                for symbol in repair_symbols.iter().cloned() {
                    self.send_symbol(&self.primary, RelayCarrierPath::Primary, symbol)
                        .await?;
                }
            }
            Ok::<(), anyhow::Error>(())
        };
        let secondary_lane = async {
            if let Some(secondary) = self.secondary.as_ref() {
                if self.secondary_seed_source {
                    for symbol in source_symbols.iter().cloned() {
                        self.send_symbol(secondary, RelayCarrierPath::Secondary, symbol)
                            .await?;
                    }
                }
                for symbol in repair_symbols.iter().cloned() {
                    self.send_symbol(secondary, RelayCarrierPath::Secondary, symbol)
                        .await?;
                }
            }
            Ok::<(), anyhow::Error>(())
        };
        let secondary_configured = self.secondary.is_some();
        let (primary_result, secondary_result) = tokio::join!(primary_lane, secondary_lane);
        self.telemetry
            .record_relay_session_lane_object(RelayCarrierPath::Primary, primary_result.is_ok());
        if secondary_configured {
            self.telemetry.record_relay_session_lane_object(
                RelayCarrierPath::Secondary,
                secondary_result.is_ok(),
            );
        }
        let survived_lane_failure = match resolve_relay_lane_results(
            primary_result,
            secondary_configured.then_some(secondary_result),
            self.secondary_seed_source,
        ) {
            Ok(survived_lane_failure) => survived_lane_failure,
            Err(error) => {
                self.telemetry
                    .relay_all_lanes_failed_objects
                    .fetch_add(1, Ordering::Relaxed);
                if deadline.is_expired_at(now_unix_us()) {
                    self.telemetry
                        .relay_expired_objects
                        .fetch_add(1, Ordering::Relaxed);
                    self.telemetry
                        .relay_deadline_misses
                        .fetch_add(1, Ordering::Relaxed);
                }
                return Err(error);
            }
        };
        if survived_lane_failure {
            self.telemetry
                .relay_surviving_lane_objects
                .fetch_add(1, Ordering::Relaxed);
        }
        if deadline.is_expired_at(now_unix_us()) {
            self.telemetry
                .relay_expired_objects
                .fetch_add(1, Ordering::Relaxed);
            self.telemetry
                .relay_deadline_misses
                .fetch_add(1, Ordering::Relaxed);
            bail!("canonical media object expired while RelaySession symbols were emitted");
        }

        self.telemetry
            .relay_objects_sent
            .fetch_add(1, Ordering::Relaxed);
        self.telemetry
            .relay_deadline_hits
            .fetch_add(1, Ordering::Relaxed);
        self.telemetry
            .relay_last_deadline_unix_us
            .store(deadline.expires_at_us, Ordering::Relaxed);
        Ok(RelayPublishOutcome {
            announcement,
            source_symbols: source_count,
            repair_symbols: repair_count,
        })
    }

    async fn send_symbol(
        &self,
        transport: &PrivateUdpTransport,
        path: RelayCarrierPath,
        symbol: relay_session::RelayDatagram,
    ) -> Result<()> {
        let started = Instant::now();
        let role = symbol.role;
        let result = if symbol.deadline.is_expired_at(now_unix_us()) {
            self.telemetry.record_relay_session_send_error(role);
            self.telemetry
                .relay_expired_symbols
                .fetch_add(1, Ordering::Relaxed);
            Err(anyhow::anyhow!(
                "RelaySession symbol expired before carrier send"
            ))
        } else {
            let wire_bytes = match relay_datagram_len(&symbol) {
                Ok(bytes) => bytes as u64,
                Err(error) => {
                    self.telemetry.record_relay_session_send_error(role);
                    self.telemetry.record_relay_session_stage_duration(
                        relay_send_stage(path, role),
                        started.elapsed(),
                    );
                    return Err(error).context("failed to measure RelaySession datagram");
                }
            };
            if let Err(error) = transport.send_datagram(symbol).await {
                self.telemetry.record_relay_session_send_error(role);
                Err(error).with_context(|| {
                    format!(
                        "failed to send RelaySession datagram to {}",
                        transport.peer_addr()
                    )
                })
            } else {
                self.telemetry
                    .record_relay_session_send_success(role, wire_bytes);
                Ok(())
            }
        };
        self.telemetry
            .record_relay_session_stage_duration(relay_send_stage(path, role), started.elapsed());
        result
    }
}

fn relay_send_stage(path: RelayCarrierPath, role: MediaDatagramRole) -> RelayPipelineStage {
    match (path, role) {
        (RelayCarrierPath::Primary, MediaDatagramRole::Source) => {
            RelayPipelineStage::PrimarySourceSend
        }
        (RelayCarrierPath::Secondary, MediaDatagramRole::Source) => {
            RelayPipelineStage::SecondarySourceSend
        }
        (RelayCarrierPath::Primary, MediaDatagramRole::Repair) => {
            RelayPipelineStage::PrimaryRepairSend
        }
        (RelayCarrierPath::Secondary, MediaDatagramRole::Repair) => {
            RelayPipelineStage::SecondaryRepairSend
        }
    }
}

fn relay_path_metrics_configured(args: &Args) -> bool {
    args.relay_path_loss_fraction != 0.0
        || args.relay_path_best_direct_rtt_ms != 0.0
        || args.relay_path_rtt_ms != 0.0
        || args.relay_path_jitter_ms != 0.0
        || args.relay_path_queue_delay_ms != 0.0
        || args.relay_path_observed_at_unix_ms.is_some()
}

fn relay_secondary_path_metrics_configured(args: &Args) -> bool {
    args.relay_secondary_path_loss_fraction != 0.0
        || args.relay_secondary_path_best_direct_rtt_ms != 0.0
        || args.relay_secondary_path_rtt_ms != 0.0
        || args.relay_secondary_path_jitter_ms != 0.0
        || args.relay_secondary_path_queue_delay_ms != 0.0
        || args.relay_secondary_path_observed_at_unix_ms.is_some()
}

fn relay_path_metrics(args: &Args) -> Result<PathMetrics> {
    path_metrics_from_values(
        "--relay-path",
        args.relay_path_loss_fraction,
        args.relay_path_best_direct_rtt_ms,
        args.relay_path_rtt_ms,
        args.relay_path_jitter_ms,
        args.relay_path_queue_delay_ms,
        args.relay_path_observed_at_unix_ms,
    )
}

fn relay_secondary_path_metrics(args: &Args) -> Result<PathMetrics> {
    if !relay_secondary_path_metrics_configured(args) {
        return relay_path_metrics(args);
    }
    path_metrics_from_values(
        "--relay-secondary-path",
        args.relay_secondary_path_loss_fraction,
        args.relay_secondary_path_best_direct_rtt_ms,
        args.relay_secondary_path_rtt_ms,
        args.relay_secondary_path_jitter_ms,
        args.relay_secondary_path_queue_delay_ms,
        args.relay_secondary_path_observed_at_unix_ms,
    )
}

fn path_metrics_from_values(
    prefix: &str,
    loss_fraction: f32,
    best_direct_rtt_ms: f32,
    rtt_ms: f32,
    jitter_ms: f32,
    queue_delay_ms: f32,
    observed_at_unix_ms: Option<u64>,
) -> Result<PathMetrics> {
    for (suffix, value) in [
        ("best-direct-rtt-ms", best_direct_rtt_ms),
        ("rtt-ms", rtt_ms),
        ("jitter-ms", jitter_ms),
        ("queue-delay-ms", queue_delay_ms),
    ] {
        if !value.is_finite() || value < 0.0 {
            bail!("{prefix}-{suffix} must be a finite non-negative value");
        }
    }
    if !loss_fraction.is_finite() || !(0.0..=1.0).contains(&loss_fraction) {
        bail!("{prefix}-loss-fraction must be between zero and one");
    }
    if rtt_ms > 0.0 && best_direct_rtt_ms == 0.0 {
        bail!("{prefix}-best-direct-rtt-ms must be positive when route RTT is observed");
    }
    Ok(PathMetrics {
        observed_at_us: observed_at_unix_ms
            .unwrap_or_default()
            .saturating_mul(1_000),
        rtt_ms,
        jitter_ms,
        loss_fraction,
        queue_delay_ms,
        ..PathMetrics::default()
    })
}

fn adaptive_relay_path_metrics(primary: PathMetrics, secondary: PathMetrics) -> PathMetrics {
    PathMetrics {
        observed_at_us: primary.observed_at_us.max(secondary.observed_at_us),
        rtt_ms: primary.rtt_ms.max(secondary.rtt_ms),
        jitter_ms: primary.jitter_ms.max(secondary.jitter_ms),
        loss_fraction: (primary.loss_fraction + secondary.loss_fraction
            - primary.loss_fraction * secondary.loss_fraction)
            .clamp(0.0, 1.0),
        queue_delay_ms: primary.queue_delay_ms.max(secondary.queue_delay_ms),
        goodput_bps: match (primary.goodput_bps, secondary.goodput_bps) {
            (Some(primary), Some(secondary)) => Some(primary.min(secondary)),
            (Some(value), None) | (None, Some(value)) => Some(value),
            (None, None) => None,
        },
        deadline_hit_fraction: primary
            .deadline_hit_fraction
            .min(secondary.deadline_hit_fraction),
    }
}

#[derive(Clone)]
struct MeshForwarder {
    byte_socket: Arc<UdpSocket>,
    byte_target: SocketAddr,
    next_byte_block_id: Arc<AtomicU32>,
    next_byte_packet_sequence: Arc<AtomicU32>,
    repair_symbols: u32,
    symbol_size: u16,
    media_encoder: Arc<Mutex<MediaFecEncoder>>,
    media_socket: Arc<UdpSocket>,
    media_target: SocketAddr,
    audio_epoch_targets: Arc<Vec<SocketAddr>>,
    next_media_sequence: Arc<AtomicU64>,
    source_epoch: u64,
    fmp4_initializations: Arc<Mutex<HashMap<u64, (ObjectKey, u64)>>>,
    delivery_budget_ms: u64,
    estimated_clock_error_ns: u64,
    relay: Option<Arc<RelaySessionPublisher>>,
    relay_exclusive: bool,
    telemetry: Arc<IngestTelemetry>,
}

#[derive(Debug)]
struct AuthorizedDatagramLimitExceeded;

impl std::fmt::Display for AuthorizedDatagramLimitExceeded {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("authorized media datagram limit exceeded")
    }
}

impl std::error::Error for AuthorizedDatagramLimitExceeded {}

impl MeshForwarder {
    async fn new(args: &Args, telemetry: Arc<IngestTelemetry>) -> Result<Self> {
        if args.relay_deadline_ms == 0 {
            bail!("--relay-deadline-ms must be positive");
        }
        if args.wall_clock_estimated_error_ms == 0 {
            bail!("--wall-clock-estimated-error-ms must be positive");
        }
        let estimated_clock_error_ns = args
            .wall_clock_estimated_error_ms
            .checked_mul(1_000_000)
            .context("--wall-clock-estimated-error-ms exceeds nanosecond range")?;
        let relay = RelaySessionPublisher::new(args, Arc::clone(&telemetry))
            .await?
            .map(Arc::new);
        if args.relay_exclusive && relay.is_none() {
            bail!("--relay-exclusive requires --relay-primary-target");
        }
        let byte_socket = UdpSocket::bind(local_sender_addr(args.mesh_fec_target))
            .await
            .with_context(|| {
                format!(
                    "failed to bind mesh byte FEC sender for {}",
                    args.mesh_fec_target
                )
            })?;
        let media_socket = UdpSocket::bind(local_sender_addr(args.mesh_media_fec_target))
            .await
            .with_context(|| {
                format!(
                    "failed to bind mesh media FEC sender for {}",
                    args.mesh_media_fec_target
                )
            })?;

        let source_epoch = now_unix_us().max(1);
        telemetry
            .media_object_source_epoch
            .store(source_epoch, Ordering::Release);

        let mut audio_epoch_targets = vec![args.mesh_media_fec_target];
        if let Some(target) = args.relay_primary_target {
            audio_epoch_targets.push(target);
        }
        if let Some(target) = args.relay_secondary_target {
            audio_epoch_targets.push(target);
        }
        audio_epoch_targets.sort_unstable();
        audio_epoch_targets.dedup();

        Ok(Self {
            byte_socket: Arc::new(byte_socket),
            byte_target: args.mesh_fec_target,
            next_byte_block_id: Arc::new(AtomicU32::new(0)),
            next_byte_packet_sequence: Arc::new(AtomicU32::new(0)),
            repair_symbols: args.repair_symbols,
            symbol_size: args.symbol_size,
            media_encoder: Arc::new(Mutex::new(MediaFecEncoder::default())),
            media_socket: Arc::new(media_socket),
            media_target: args.mesh_media_fec_target,
            audio_epoch_targets: Arc::new(audio_epoch_targets),
            next_media_sequence: Arc::new(AtomicU64::new(0)),
            source_epoch,
            fmp4_initializations: Arc::new(Mutex::new(HashMap::new())),
            delivery_budget_ms: args.relay_deadline_ms,
            estimated_clock_error_ns,
            relay,
            relay_exclusive: args.relay_exclusive,
            telemetry,
        })
    }

    fn allocate_media_sequence(&self) -> u64 {
        self.next_media_sequence.fetch_add(1, Ordering::Relaxed)
    }

    async fn forward_stream_slot(&self, stream_id: u64, bytes: &[u8]) -> Result<usize> {
        self.forward_stream_slot_with_limit(stream_id, bytes, None)
            .await
    }

    async fn forward_stream_slot_with_limit(
        &self,
        stream_id: u64,
        bytes: &[u8],
        max_datagram_bytes: Option<u32>,
    ) -> Result<usize> {
        if bytes.is_empty() {
            return Ok(0);
        }
        let symbol_size = if let Some(maximum) = max_datagram_bytes {
            let overhead = std::mem::size_of::<u64>() + HEADER_LEN + ENCODING_PACKET_HEADER_LEN;
            let maximum = usize::try_from(maximum)
                .ok()
                .and_then(|maximum| maximum.checked_sub(overhead))
                .and_then(|maximum| u16::try_from(maximum).ok())
                .filter(|maximum| *maximum > 0)
                .ok_or_else(|| anyhow::Error::new(AuthorizedDatagramLimitExceeded))?;
            self.symbol_size.min(maximum)
        } else {
            self.symbol_size
        };
        let started = Instant::now();
        let encode_wait_started = Instant::now();
        let geometry = match stream_fec_geometry(bytes.len(), symbol_size, self.repair_symbols) {
            Ok(geometry) => geometry,
            Err(error) => {
                self.telemetry.record_mesh_forward_stage_duration(
                    "stream",
                    "encode_wait",
                    encode_wait_started.elapsed(),
                );
                self.telemetry.record_mesh_forward_error(
                    "stream",
                    stream_id,
                    self.byte_target,
                    &error,
                );
                self.telemetry
                    .record_mesh_forward_duration("stream", started.elapsed());
                return Err(error);
            }
        };
        let block_id = self
            .next_byte_block_id
            .fetch_add(geometry.block_count, Ordering::Relaxed);
        let packet_sequence = self
            .next_byte_packet_sequence
            .fetch_add(geometry.packet_count, Ordering::Relaxed);
        self.telemetry.record_mesh_forward_stage_duration(
            "stream",
            "encode_wait",
            encode_wait_started.elapsed(),
        );
        let encode_started = Instant::now();
        let datagrams = match encode_stream_fec_payload(
            stream_id,
            bytes,
            self.repair_symbols,
            symbol_size,
            block_id,
            packet_sequence,
        ) {
            Ok(datagrams) => datagrams,
            Err(error) => {
                self.telemetry.record_mesh_forward_stage_duration(
                    "stream",
                    "encode",
                    encode_started.elapsed(),
                );
                self.telemetry.record_mesh_forward_error(
                    "stream",
                    stream_id,
                    self.byte_target,
                    &error,
                );
                self.telemetry
                    .record_mesh_forward_duration("stream", started.elapsed());
                return Err(error);
            }
        };
        if let Some(maximum) = max_datagram_bytes {
            if datagrams
                .iter()
                .any(|datagram| datagram.len() > maximum as usize)
            {
                return Err(anyhow::Error::new(AuthorizedDatagramLimitExceeded));
            }
        }
        self.telemetry.record_mesh_forward_stage_duration(
            "stream",
            "encode",
            encode_started.elapsed(),
        );
        let datagram_bytes = datagrams
            .iter()
            .map(|datagram| datagram.len() as u64)
            .sum::<u64>();
        let send_started = Instant::now();
        for datagram in &datagrams {
            if let Err(error) = self
                .byte_socket
                .send_to(datagram, self.byte_target)
                .await
                .with_context(|| format!("failed to forward stream slot to {}", self.byte_target))
            {
                self.telemetry.record_mesh_forward_stage_duration(
                    "stream",
                    "send",
                    send_started.elapsed(),
                );
                self.telemetry.record_mesh_forward_error(
                    "stream",
                    stream_id,
                    self.byte_target,
                    &error,
                );
                self.telemetry
                    .record_mesh_forward_duration("stream", started.elapsed());
                return Err(error);
            }
        }
        self.telemetry
            .record_mesh_forward_stage_duration("stream", "send", send_started.elapsed());
        let telemetry_started = Instant::now();
        self.telemetry.record_mesh_forward_success(
            "stream",
            stream_id,
            self.byte_target,
            bytes.len() as u64,
            datagrams.len() as u64,
            datagram_bytes,
        );
        self.telemetry.record_mesh_forward_stage_duration(
            "stream",
            "telemetry",
            telemetry_started.elapsed(),
        );
        self.telemetry
            .record_mesh_forward_duration("stream", started.elapsed());
        Ok(datagrams.len())
    }

    async fn forward_media_access_unit(
        &self,
        metadata: MediaFrameMetadata,
        payload: &[u8],
        max_datagram_bytes: Option<u32>,
    ) -> Result<usize> {
        let started = Instant::now();
        let stream_id = metadata.stream_id;
        let encode_wait_started = Instant::now();
        let encoded = {
            let mut encoder = self.media_encoder.lock().await;
            self.telemetry.record_mesh_forward_stage_duration(
                "media",
                "encode_wait",
                encode_wait_started.elapsed(),
            );
            let encode_started = Instant::now();
            let encoded = encoder
                .encode_frame(MediaFrame { metadata, payload })
                .context("failed to encode media access unit for mesh RaptorQ-FEC");
            self.telemetry.record_mesh_forward_stage_duration(
                "media",
                "encode",
                encode_started.elapsed(),
            );
            encoded
        };
        let datagrams = match encoded {
            Ok(encoded) => encoded.datagrams,
            Err(error) => {
                self.telemetry.record_mesh_forward_error(
                    "media",
                    stream_id,
                    self.media_target,
                    &error,
                );
                self.telemetry
                    .record_mesh_forward_duration("media", started.elapsed());
                return Err(error);
            }
        };
        if let Some(maximum) = max_datagram_bytes {
            if datagrams
                .iter()
                .any(|datagram| datagram.len() > maximum as usize)
            {
                return Err(anyhow::Error::new(AuthorizedDatagramLimitExceeded));
            }
        }
        let datagram_bytes = datagrams
            .iter()
            .map(|datagram| datagram.len() as u64)
            .sum::<u64>();
        let send_started = Instant::now();
        for datagram in &datagrams {
            if let Err(error) = self
                .media_socket
                .send_to(datagram, self.media_target)
                .await
                .with_context(|| {
                    format!(
                        "failed to forward media access unit to {}",
                        self.media_target
                    )
                })
            {
                self.telemetry.record_mesh_forward_stage_duration(
                    "media",
                    "send",
                    send_started.elapsed(),
                );
                self.telemetry.record_mesh_forward_error(
                    "media",
                    stream_id,
                    self.media_target,
                    &error,
                );
                self.telemetry
                    .record_mesh_forward_duration("media", started.elapsed());
                return Err(error);
            }
        }
        self.telemetry
            .record_mesh_forward_stage_duration("media", "send", send_started.elapsed());
        let telemetry_started = Instant::now();
        self.telemetry.record_mesh_forward_success(
            "media",
            stream_id,
            self.media_target,
            payload.len() as u64,
            datagrams.len() as u64,
            datagram_bytes,
        );
        self.telemetry.record_mesh_forward_stage_duration(
            "media",
            "telemetry",
            telemetry_started.elapsed(),
        );
        self.telemetry
            .record_mesh_forward_duration("media", started.elapsed());
        Ok(datagrams.len())
    }

    async fn forward_audio_epoch_datagram(&self, datagram: &[u8]) -> Result<()> {
        if datagram.is_empty() {
            return Ok(());
        }
        let mut first_error = None;
        for target in self.audio_epoch_targets.iter().copied() {
            match self.media_socket.send_to(datagram, target).await {
                Ok(sent) if sent == datagram.len() => {
                    trace!(
                        %target,
                        datagram_bytes = datagram.len(),
                        "forwarded multichannel audio epoch datagram"
                    );
                }
                Ok(sent) => {
                    first_error.get_or_insert_with(|| {
                        anyhow::anyhow!(
                            "partial audio epoch datagram to {target}: sent {sent} of {} bytes",
                            datagram.len()
                        )
                    });
                }
                Err(error) => {
                    first_error.get_or_insert_with(|| {
                        anyhow::anyhow!(
                            "failed to forward audio epoch datagram to {target}: {error}"
                        )
                    });
                }
            }
        }
        first_error.map_or(Ok(()), Err)
    }
}

#[derive(Debug, Clone, Copy)]
struct DawRelayTarget {
    expires_at: Instant,
    session_id: Option<u64>,
}

type DawRelayTargets = Arc<RwLock<HashMap<SocketAddr, DawRelayTarget>>>;

fn parse_daw_relay_session_message(datagram: &[u8], prefix: &[u8]) -> Option<u64> {
    let value = datagram.strip_prefix(prefix)?;
    let value = std::str::from_utf8(value).ok()?;
    if value.is_empty() || !value.bytes().all(|byte| byte.is_ascii_digit()) {
        return None;
    }
    value.parse().ok()
}

fn daw_relay_session_ack(session_id: u64) -> Vec<u8> {
    let mut ack = Vec::with_capacity(DAW_RELAY_SUBSCRIBE_ACK_V2_PREFIX.len() + 20);
    ack.extend_from_slice(DAW_RELAY_SUBSCRIBE_ACK_V2_PREFIX);
    ack.extend_from_slice(session_id.to_string().as_bytes());
    ack
}

async fn relay_daw_media_datagram(
    socket: &UdpSocket,
    targets: &DawRelayTargets,
    source: SocketAddr,
    datagram: &[u8],
    session_id: Option<u64>,
) {
    let now = Instant::now();
    let relay_targets = {
        let targets = targets.read().await;
        if targets.is_empty() {
            return;
        }
        targets
            .iter()
            .filter_map(|(address, target)| {
                (*address != source
                    && target.expires_at > now
                    && target
                        .session_id
                        .is_none_or(|requested| Some(requested) == session_id))
                .then_some(*address)
            })
            .collect::<Vec<_>>()
    };

    for target in relay_targets {
        if let Err(error) = socket.send_to(datagram, target).await {
            warn!(
                source = %source,
                target = %target,
                error = %error,
                "failed to relay DAW media datagram"
            );
        }
    }
}

fn handoff_audio_epoch_hls_datagram(
    tx: Option<&mpsc::Sender<AudioEpochHlsDatagram>>,
    peer: SocketAddr,
    datagram: &[u8],
) {
    let Some(tx) = tx else {
        return;
    };
    if let Err(error) = tx.try_send(AudioEpochHlsDatagram {
        peer,
        bytes: Bytes::copy_from_slice(datagram),
    }) {
        let dropped_total = AUDIO_EPOCH_HLS_QUEUE_DROPPED.fetch_add(1, Ordering::Relaxed) + 1;
        if should_log_audio_epoch_hls_drop(dropped_total) {
            warn!(
                peer = %peer,
                error = %error,
                dropped_total,
                "AEP1 LL-HLS handoff is full; datagram lanes remain live"
            );
        }
    } else {
        AUDIO_EPOCH_HLS_QUEUE_ENQUEUED.fetch_add(1, Ordering::Relaxed);
        let depth = tx.max_capacity().saturating_sub(tx.capacity()) as u64;
        AUDIO_EPOCH_HLS_QUEUE_MAX_DEPTH.fetch_max(depth, Ordering::Relaxed);
    }
}

async fn run_daw_media_udp_ingest(
    socket: UdpSocket,
    forwarder: Arc<MeshForwarder>,
    targets: DawRelayTargets,
    audio_epoch_hls_tx: Option<mpsc::Sender<AudioEpochHlsDatagram>>,
    mut shutdown_rx: watch::Receiver<()>,
) -> Result<()> {
    let bind = socket.local_addr()?;
    let mut decoders = HashMap::<SocketAddr, MediaFecDecoder>::new();
    let mut audio_block_sessions = HashMap::<(SocketAddr, u32), (u64, Instant)>::new();
    let mut buf = vec![0u8; 65_536];
    let mut cleanup = interval(DAW_RELAY_CLEANUP_INTERVAL);
    cleanup.set_missed_tick_behavior(MissedTickBehavior::Skip);

    loop {
        tokio::select! {
            _ = shutdown_rx.changed() => {
                info!(bind = %bind, "DAW media UDP relay shutting down");
                return Ok(());
            }
            _ = cleanup.tick() => {
                let now = Instant::now();
                let mut targets = targets.write().await;
                targets.retain(|_, target| target.expires_at > now);
                audio_block_sessions.retain(|_, (_, expires_at)| *expires_at > now);
            }
            received = socket.recv_from(&mut buf) => {
                let (len, peer) = received?;
                if len == 0 {
                    continue;
                }
                let datagram = &buf[..len];

                if datagram == DAW_RELAY_SUBSCRIBE_MESSAGE {
                    let expires_at = Instant::now() + DAW_RELAY_TARGET_TTL;
                    targets.write().await.insert(peer, DawRelayTarget {
                        expires_at,
                        session_id: None,
                    });
                    if let Err(error) = socket.send_to(DAW_RELAY_SUBSCRIBE_ACK, peer).await {
                        warn!(peer = %peer, error = %error, "failed to acknowledge DAW relay subscription");
                    }
                    trace!(peer = %peer, "registered DAW relay subscriber");
                    continue;
                }

                if let Some(session_id) = parse_daw_relay_session_message(
                    datagram,
                    DAW_RELAY_SUBSCRIBE_V2_PREFIX,
                ) {
                    let expires_at = Instant::now() + DAW_RELAY_TARGET_TTL;
                    targets.write().await.insert(peer, DawRelayTarget {
                        expires_at,
                        session_id: Some(session_id),
                    });
                    let ack = daw_relay_session_ack(session_id);
                    if let Err(error) = socket.send_to(&ack, peer).await {
                        warn!(peer = %peer, session_id, error = %error, "failed to acknowledge session-scoped DAW relay subscription");
                    }
                    trace!(peer = %peer, session_id, "registered session-scoped DAW relay subscriber");
                    continue;
                }
                if datagram.starts_with(DAW_RELAY_SUBSCRIBE_V2_PREFIX) {
                    warn!(peer = %peer, "ignored malformed session-scoped DAW relay subscription");
                    continue;
                }

                if datagram == DAW_RELAY_UNSUBSCRIBE_MESSAGE {
                    targets.write().await.remove(&peer);
                    decoders.remove(&peer);
                    trace!(peer = %peer, "removed DAW relay subscriber");
                    continue;
                }

                if let Some(session_id) = parse_daw_relay_session_message(
                    datagram,
                    DAW_RELAY_UNSUBSCRIBE_V2_PREFIX,
                ) {
                    let mut targets = targets.write().await;
                    if targets.get(&peer).is_some_and(|target| target.session_id == Some(session_id)) {
                        targets.remove(&peer);
                    }
                    decoders.remove(&peer);
                    trace!(peer = %peer, session_id, "removed session-scoped DAW relay subscriber");
                    continue;
                }
                if datagram.starts_with(DAW_RELAY_UNSUBSCRIBE_V2_PREFIX) {
                    warn!(peer = %peer, "ignored malformed session-scoped DAW relay unsubscription");
                    continue;
                }

                if datagram == DAW_RELAY_SUBSCRIBE_ACK {
                    continue;
                }
                if datagram.starts_with(DAW_RELAY_SUBSCRIBE_ACK_V2_PREFIX) {
                    continue;
                }

                if is_multichannel_audio_transport_datagram(datagram) {
                    if !targets.read().await.is_empty() {
                        let identity = inspect_multichannel_audio_datagram(
                            &datagram[MULTICHANNEL_AUDIO_TRANSPORT_MAGIC.len()..],
                        );
                        let session_id = identity.ok().and_then(|identity| {
                            if let Some(session_id) = identity.session_id {
                                audio_block_sessions.insert(
                                    (peer, identity.block_id),
                                    (session_id, Instant::now() + DAW_RELAY_TARGET_TTL),
                                );
                                Some(session_id)
                            } else {
                                audio_block_sessions
                                    .get(&(peer, identity.block_id))
                                    .map(|(session_id, _)| *session_id)
                            }
                        });
                        relay_daw_media_datagram(&socket, &targets, peer, datagram, session_id).await;
                    }
                    handoff_audio_epoch_hls_datagram(audio_epoch_hls_tx.as_ref(), peer, datagram);
                    if let Err(error) = forwarder.forward_audio_epoch_datagram(datagram).await {
                        warn!(
                            peer = %peer,
                            error = %error,
                            "failed to forward DAW audio epoch datagram to mesh"
                        );
                    }
                    continue;
                }

                relay_daw_media_datagram(&socket, &targets, peer, datagram, None).await;

                let decoded = {
                    let decoder = decoders.entry(peer).or_default();
                    decoder.push_datagram(datagram)
                };
                match decoded {
                    Ok(Some(frame)) => {
                        let stream_id = frame.metadata.stream_id;
                        let sequence = frame.metadata.sequence;
                        let payload_bytes = frame.payload.len();
                        if let Err(error) = forwarder
                            .forward_media_access_unit(frame.metadata, &frame.payload, None)
                            .await
                        {
                            warn!(
                                peer = %peer,
                                stream_id,
                                sequence,
                                error = %error,
                                "failed to forward decoded DAW media access unit to mesh"
                            );
                        } else {
                            trace!(
                                peer = %peer,
                                stream_id,
                                sequence,
                                payload_bytes,
                                "forwarded decoded DAW media access unit to mesh"
                            );
                        }
                    }
                    Ok(None) => {}
                    Err(error) => {
                        warn!(
                            peer = %peer,
                            error = %error,
                            "failed to decode DAW media-FEC datagram"
                        );
                    }
                }
            }
        }
    }
}

async fn bind_daw_media_udp_socket(bind: SocketAddr) -> Result<UdpSocket> {
    let socket = UdpSocket::bind(bind)
        .await
        .with_context(|| format!("failed to bind DAW media UDP relay on {bind}"))?;
    let socket_ref = SockRef::from(&socket);
    socket_ref
        .set_recv_buffer_size(DAW_MEDIA_RECEIVE_BUFFER_BYTES)
        .context("failed to configure DAW media UDP receive buffer")?;
    let receive_buffer_bytes = socket_ref
        .recv_buffer_size()
        .context("failed to read DAW media UDP receive buffer size")?;
    if receive_buffer_bytes < DAW_MEDIA_RECEIVE_BUFFER_BYTES {
        warn!(
            bind = %bind,
            requested_bytes = DAW_MEDIA_RECEIVE_BUFFER_BYTES,
            receive_buffer_bytes,
            "DAW media UDP receive buffer is below the requested size"
        );
    } else {
        info!(
            bind = %bind,
            requested_bytes = DAW_MEDIA_RECEIVE_BUFFER_BYTES,
            receive_buffer_bytes,
            "configured DAW media UDP receive buffer"
        );
    }
    Ok(socket)
}

#[async_trait::async_trait]
impl Fmp4PartPublisher for MeshForwarder {
    async fn publish_fmp4_part(&self, part: PublishedFmp4Part) -> std::result::Result<(), String> {
        let initialization = part
            .init
            .as_ref()
            .map(|init| {
                build_live_fmp4_initialization_object(
                    part.stream_id,
                    self.source_epoch,
                    init,
                    part.published_at_unix_ns,
                    self.delivery_budget_ms,
                    self.estimated_clock_error_ns,
                )
            })
            .transpose()
            .map_err(|error| error.to_string())?;
        let (initialization_key, configuration_epoch) =
            if let Some((object, epoch)) = initialization {
                let initialization_envelope =
                    encode_canonical_media_object(&object).map_err(|error| error.to_string())?;
                if !self.relay_exclusive {
                    self.forward_stream_slot(part.stream_id, &initialization_envelope)
                        .await
                        .map_err(|error| error.to_string())?;
                }
                if let Some(relay) = &self.relay {
                    let outcome = relay
                        .publish_object(&object)
                        .await
                        .map_err(|error| error.to_string())?;
                    trace!(
                        stream_id = part.stream_id,
                        object_key = ?outcome.announcement.key,
                        source_symbols = outcome.source_symbols,
                        repair_symbols = outcome.repair_symbols,
                        "published canonical fMP4 initialization through RelaySession"
                    );
                }
                let key = object.key().clone();
                self.fmp4_initializations
                    .lock()
                    .await
                    .insert(part.stream_id, (key.clone(), epoch));
                (key, epoch)
            } else {
                self.fmp4_initializations
                    .lock()
                    .await
                    .get(&part.stream_id)
                    .cloned()
                    .ok_or_else(|| {
                        format!(
                            "fMP4 media part {} for stream {} has no initialization dependency",
                            part.sequence, part.stream_id
                        )
                    })?
            };

        let bundled_media = encode_mesh_fmp4_slot(part.init.as_ref(), &part.bytes)
            .map_err(|error| error.to_string())?;
        let media_object = build_fmp4_media_object(
            &part,
            &bundled_media,
            initialization_key,
            configuration_epoch,
            self.source_epoch,
            self.delivery_budget_ms,
            self.estimated_clock_error_ns,
        )
        .map_err(|error| error.to_string())?;
        let media =
            encode_canonical_media_object(&media_object).map_err(|error| error.to_string())?;
        if !self.relay_exclusive {
            self.forward_stream_slot(part.stream_id, &media)
                .await
                .map_err(|error| error.to_string())?;
        }

        if let Some(relay) = &self.relay {
            let outcome = relay
                .publish_object(&media_object)
                .await
                .map_err(|error| error.to_string())?;
            trace!(
                stream_id = part.stream_id,
                sequence = part.sequence,
                object_key = ?outcome.announcement.key,
                source_symbols = outcome.source_symbols,
                repair_symbols = outcome.repair_symbols,
                "published canonical fMP4 object through RelaySession"
            );
        }
        Ok(())
    }
}

struct TelemetryFmp4Publisher {
    inner: Arc<dyn Fmp4PartPublisher>,
    telemetry: Arc<IngestTelemetry>,
    canonical_sequences: Mutex<HashMap<u64, u64>>,
}

#[async_trait::async_trait]
impl Fmp4PartPublisher for TelemetryFmp4Publisher {
    async fn publish_fmp4_part(
        &self,
        mut part: PublishedFmp4Part,
    ) -> std::result::Result<(), String> {
        // Segmenters may be recreated when an ingest protocol reconnects.
        // Keep the canonical object sequence at the shared publisher boundary
        // so it remains contiguous for the entire source incarnation.
        part.sequence = {
            let mut sequences = self.canonical_sequences.lock().await;
            let next = sequences.entry(part.stream_id).or_insert(0);
            let sequence = *next;
            *next = next.saturating_add(1);
            sequence
        };
        let stream_id = part.stream_id;
        let stream_idx = part.stream_idx;
        let sequence = part.sequence;
        let bytes = part.bytes.len() as u64;
        let init_bytes = part.init.as_ref().map_or(0, |init| init.len() as u64);
        let video_width = part.video_width;
        let video_height = part.video_height;
        let video_units = part.video_units;
        let audio_units = part.audio_units;
        self.telemetry.record_fmp4_tracks(
            stream_id,
            stream_idx,
            sequence,
            video_width,
            video_height,
            video_units,
            audio_units,
        );
        match self.inner.publish_fmp4_part(part).await {
            Ok(()) => {
                self.telemetry
                    .record_fmp4_part(stream_id, stream_idx, sequence, bytes, init_bytes);
                Ok(())
            }
            Err(error) => {
                self.telemetry
                    .record_fmp4_publish_error(stream_id, stream_idx, sequence, &error);
                Err(error)
            }
        }
    }

    fn record_mpeg_ts_continuity_issue(&self, issue: MpegTsContinuityIssue) {
        self.telemetry.record_mpeg_ts_continuity_issue(issue);
    }

    fn record_mpeg_ts_payload_drop(&self, drop: MpegTsPayloadDrop) {
        self.telemetry.record_mpeg_ts_payload_drop(drop);
    }
}

async fn start_rist_ingest(
    config: RistIngestConfig,
    playlists: Arc<playlists::Playlists>,
    publisher: Arc<dyn Fmp4PartPublisher>,
    telemetry: Arc<IngestTelemetry>,
    shutdown_rx: watch::Receiver<()>,
) -> Result<watch::Sender<()>> {
    let service = Arc::new(UploadResponseService::new(upload_response_config()));
    let rist_shutdown = UploadPureRistIngest::new(service.clone())
        .with_profile(config.profile.into())
        .with_flow_id(config.flow_id)
        .start(config.bind)
        .await
        .map_err(|error| {
            anyhow::anyhow!("failed to bind pure Rust RIST contributor frontend: {error}")
        })?;
    tokio::spawn(run_upload_response_ts_bridge(
        service,
        playlists,
        publisher,
        telemetry,
        "rist",
        config.output_stream_id,
        config.output_stream_idx,
        config.min_part_ms,
        shutdown_rx,
    ));
    info!(
        bind = %config.bind,
        profile = config.profile.as_str(),
        backend = RistBackend::Pure.as_str(),
        flow_id = format_args!("0x{:08x}", config.flow_id),
        output_stream_id = config.output_stream_id,
        output_stream_idx = config.output_stream_idx,
        "RIST contributor frontend listening via upload-response"
    );
    Ok(rist_shutdown)
}

struct UploadTsBridgeState {
    output_stream_id: Option<u64>,
    output_stream_idx: Option<usize>,
    last_seen: usize,
    reader_registered: bool,
    body_slots: u64,
    ended: bool,
    bridge: Option<TsFmp4Bridge>,
}

#[derive(Debug, Clone, Copy)]
struct SrtIngestConfig {
    bind: SocketAddr,
    output_stream_id: u64,
    min_part_ms: u32,
}

async fn start_srt_ingest(
    config: SrtIngestConfig,
    playlists: Arc<playlists::Playlists>,
    publisher: Arc<dyn Fmp4PartPublisher>,
    telemetry: Arc<IngestTelemetry>,
    shutdown_rx: watch::Receiver<()>,
) -> Result<watch::Sender<()>> {
    let service = Arc::new(UploadResponseService::new(upload_response_config()));
    let srt_shutdown = UploadSrtIngest::new(service.clone())
        .start(config.bind)
        .await
        .map_err(|error| anyhow::anyhow!("failed to bind SRT contributor frontend: {error}"))?;
    let output_stream_idx = resolve_output_stream_idx(&playlists, config.output_stream_id).await;
    tokio::spawn(run_upload_response_ts_bridge(
        service,
        playlists,
        publisher,
        telemetry,
        "srt",
        config.output_stream_id,
        output_stream_idx,
        config.min_part_ms,
        shutdown_rx,
    ));
    info!(
        bind = %config.bind,
        output_stream_id = config.output_stream_id,
        output_stream_idx,
        "SRT contributor frontend listening"
    );
    Ok(srt_shutdown)
}

#[allow(clippy::too_many_arguments)]
async fn run_upload_response_ts_bridge(
    service: Arc<UploadResponseService>,
    playlists: Arc<playlists::Playlists>,
    publisher: Arc<dyn Fmp4PartPublisher>,
    telemetry: Arc<IngestTelemetry>,
    protocol: &'static str,
    output_stream_id: u64,
    output_stream_idx: usize,
    min_part_ms: u32,
    mut shutdown_rx: watch::Receiver<()>,
) {
    let mut tick = interval(Duration::from_millis(HLS_BRIDGE_POLL_MS));
    tick.set_missed_tick_behavior(MissedTickBehavior::Skip);
    let mut bridges: HashMap<u64, UploadTsBridgeState> = HashMap::new();

    loop {
        tokio::select! {
            _ = shutdown_rx.changed() => {
                for (stream_id, mut state) in bridges.drain() {
                    if let Some(bridge) = state.bridge.as_mut() {
                        bridge.finish().await;
                    }
                    telemetry.end_ingest_session(protocol, stream_id, "shutdown");
                }
                return;
            }
            _ = tick.tick() => {
                let streams = service.active_streams().await;
                let active: HashSet<u64> = streams.iter().map(|stream| stream.stream_id).collect();
                let stale: Vec<u64> = bridges
                    .keys()
                    .copied()
                    .filter(|stream_id| !active.contains(stream_id))
                    .collect();
                for stream_id in stale {
                    if let Some(mut state) = bridges.remove(&stream_id) {
                        if let Some(bridge) = state.bridge.as_mut() {
                            bridge.finish().await;
                        }
                        if let Some(output_stream_id) = state.output_stream_id {
                            playlists.fin(output_stream_id);
                        }
                        telemetry.end_ingest_session(protocol, stream_id, "inactive");
                    }
                }

                for stream in streams {
                    let stream_id = stream.stream_id;
                    if let std::collections::hash_map::Entry::Vacant(entry) =
                        bridges.entry(stream_id)
                    {
                        entry.insert(UploadTsBridgeState {
                                output_stream_id: None,
                                output_stream_idx: None,
                                last_seen: 0,
                                reader_registered: false,
                                body_slots: 0,
                                ended: false,
                                bridge: None,
                            });
                        telemetry.ensure_ingest_session(protocol, stream_id, None, None, None, None);
                        trace!(stream_id, "tracking upload-response stream");
                    }
                    let has_active_bridge = bridges
                        .iter()
                        .any(|(&id, state)| id != stream_id && state.bridge.is_some() && !state.ended);
                    let state = bridges.get_mut(&stream_id).expect("bridge state");

                    if state.ended {
                        continue;
                    }

                    if stream.request_last <= state.last_seen {
                        trace!(
                            stream_id,
                            last_seen = state.last_seen,
                            request_last = stream.request_last,
                            "upload-response fMP4 bridge has no new request slots"
                        );
                        continue;
                    }

                    let mut stream_ended = false;
                    for slot in (state.last_seen + 1)..=stream.request_last {
                        match service.tail_request(stream_id, slot).await {
                            Some(TailSlot::Headers(headers)) => {
                                let path = Some(String::from_utf8_lossy(&headers.path).into_owned());
                                let peer = headers.headers.iter().find_map(|header| {
                                    let name = String::from_utf8_lossy(&header.name);
                                    if name.eq_ignore_ascii_case("x-peer-addr")
                                        || name.eq_ignore_ascii_case("x-rist-peer-addr")
                                    {
                                        Some(String::from_utf8_lossy(&header.value).into_owned())
                                    } else {
                                        None
                                    }
                                });
                                telemetry.ensure_ingest_session(
                                    protocol,
                                    stream_id,
                                    state.output_stream_id,
                                    state.output_stream_idx,
                                    peer,
                                    path,
                                );
                                trace!(
                                    stream_id,
                                    path = %String::from_utf8_lossy(&headers.path),
                                    "upload-response stream headers"
                                );
                            }
                            Some(TailSlot::Body(data)) => {
                                if state.bridge.is_none() {
                                    let public_stream_id = if has_active_bridge {
                                        stream_id
                                    } else {
                                        output_stream_id
                                    };
                                    let public_stream_idx = if public_stream_id == output_stream_id {
                                        output_stream_idx
                                    } else {
                                        resolve_output_stream_idx(&playlists, public_stream_id).await
                                    };
                                    state.output_stream_id = Some(public_stream_id);
                                    state.output_stream_idx = Some(public_stream_idx);
                                    telemetry.ensure_ingest_session(
                                        protocol,
                                        stream_id,
                                        Some(public_stream_id),
                                        Some(public_stream_idx),
                                        None,
                                        None,
                                    );
                                    state.bridge = Some(TsFmp4Bridge::new_with_publisher(
                                        public_stream_id,
                                        public_stream_idx,
                                        playlists.clone(),
                                        min_part_ms,
                                        Some(publisher.clone()),
                                    ));
                                    debug!(
                                        stream_id,
                                        output_stream_id = public_stream_id,
                                        output_stream_idx = public_stream_idx,
                                        "created upload-response MPEG-TS fMP4 bridge after first body slot"
                                    );
                                }
                                if !state.reader_registered {
                                    service
                                        .register_request_reader(
                                            stream_id,
                                            UPLOAD_RESPONSE_HLS_WORKER_ID,
                                        )
                                        .await;
                                    state.reader_registered = true;
                                    debug!(
                                        stream_id,
                                        worker_id = UPLOAD_RESPONSE_HLS_WORKER_ID,
                                        "registered upload-response fMP4 bridge reader"
                                    );
                                }
                                state.body_slots = state.body_slots.saturating_add(1);
                                telemetry.record_ingest_session_body(protocol, stream_id, data.len());
                                telemetry.record_mpeg_ts_slot(protocol, stream_id, data.len());
                                debug!(
                                    stream_id,
                                    slot,
                                    bytes = data.len(),
                                    "upload-response fMP4 bridge consuming MPEG-TS body slot"
                                );
                                if let Some(bridge) = state.bridge.as_mut() {
                                    bridge.push_ts(data).await;
                                }
                            }
                            Some(TailSlot::End) => {
                                if let Some(bridge) = state.bridge.as_mut() {
                                    debug!(
                                        stream_id,
                                        output_stream_id = state.output_stream_id.unwrap_or_default(),
                                        output_stream_idx = state.output_stream_idx.unwrap_or_default(),
                                        slot,
                                        body_slots = state.body_slots,
                                        "upload-response fMP4 bridge reached stream end"
                                    );
                                    bridge.finish().await;
                                    if let Some(output_stream_id) = state.output_stream_id {
                                        playlists.fin(output_stream_id);
                                    }
                                } else {
                                    trace!(
                                        stream_id,
                                        slot,
                                        "upload-response stream ended without media body slots"
                                    );
                                }
                                telemetry.end_ingest_session(protocol, stream_id, "ended");
                                stream_ended = true;
                            }
                            Some(TailSlot::Control(_)) => {
                                trace!(
                                    stream_id,
                                    slot,
                                    "upload-response fMP4 bridge ignoring request control slot"
                                );
                            }
                            None => {
                                trace!(
                                    stream_id,
                                    slot,
                                    "upload-response fMP4 bridge request slot missing"
                                );
                            }
                        }
                        service
                            .mark_request_reader_position(
                                stream_id,
                                UPLOAD_RESPONSE_HLS_WORKER_ID,
                                slot,
                            )
                            .await;
                    }

                    state.last_seen = stream.request_last;
                    if stream_ended {
                        state.ended = true;
                    }
                }
            }
        }
    }
}

struct RtmpSegmenterState {
    output_stream_id: u64,
    segmenter: Fmp4Segmenter,
}

async fn run_rtmp_hls_bridge(
    mut rx: tokio::sync::mpsc::Receiver<RtmpIngestEvent>,
    playlists: Arc<playlists::Playlists>,
    publisher: Arc<dyn Fmp4PartPublisher>,
    telemetry: Arc<IngestTelemetry>,
    fallback_output_stream_id: u64,
    min_part_ms: u32,
) {
    let mut segmenters: HashMap<u64, RtmpSegmenterState> = HashMap::new();

    while let Some(event) = rx.recv().await {
        match event {
            RtmpIngestEvent::AccessUnit {
                stream,
                access_unit,
            } => {
                let stream_type = access_unit.stream_type;
                let key = access_unit.key;
                let pts = access_unit.pts;
                let dts = access_unit.dts;
                let bytes = access_unit.data.len();
                if !segmenters.contains_key(&stream.id) {
                    let output_stream_id = rtmp_output_stream_id(
                        &stream,
                        fallback_output_stream_id,
                        segmenters.is_empty(),
                    );
                    let output_stream_idx =
                        resolve_output_stream_idx(&playlists, output_stream_id).await;
                    telemetry.ensure_ingest_session(
                        "rtmp",
                        stream.id,
                        Some(output_stream_id),
                        Some(output_stream_idx),
                        None,
                        Some(stream.key.clone()),
                    );
                    tracing::info!(
                        key = %stream.key,
                        rtmp_stream_id = stream.id,
                        output_stream_id,
                        output_stream_idx,
                        "RTMP ingest bridged to fMP4 LL-HLS"
                    );
                    segmenters.insert(
                        stream.id,
                        RtmpSegmenterState {
                            output_stream_id,
                            segmenter: Fmp4Segmenter::new_with_publisher(
                                output_stream_id,
                                output_stream_idx,
                                playlists.clone(),
                                TimestampInput::Millis,
                                min_part_ms,
                                Some(publisher.clone()),
                            ),
                        },
                    );
                }
                telemetry.record_ingest_session_access_unit("rtmp", stream.id, bytes);
                telemetry.record_rtmp_access_unit(stream.id, bytes);

                if let Some(state) = segmenters.get_mut(&stream.id) {
                    tracing::debug!(
                        key = %stream.key,
                        rtmp_stream_id = stream.id,
                        output_stream_id = state.output_stream_id,
                        stream_type = ?stream_type,
                        keyframe = key,
                        pts,
                        dts,
                        bytes,
                        "RTMP access unit forwarded to fMP4 segmenter"
                    );
                    state.segmenter.push_access_unit(access_unit).await;
                }
            }
            RtmpIngestEvent::End { stream } => {
                if let Some(mut state) = segmenters.remove(&stream.id) {
                    state.segmenter.finish().await;
                    playlists.fin(state.output_stream_id);
                    tracing::info!(
                        key = %stream.key,
                        rtmp_stream_id = stream.id,
                        output_stream_id = state.output_stream_id,
                        "RTMP ingest stream ended"
                    );
                }
                telemetry.end_ingest_session("rtmp", stream.id, "ended");
            }
        }
    }
}

fn upload_response_config() -> UploadResponseConfig {
    UploadResponseConfig {
        num_streams: 8,
        slot_size_kb: 47,
        slots_per_stream: 32768,
        response_timeout_ms: 60_000,
    }
}

async fn resolve_output_stream_idx(
    playlists: &Arc<playlists::Playlists>,
    output_stream_id: u64,
) -> usize {
    if output_stream_id < playlists.chunk_cache.options.num_playlists as u64 {
        output_stream_id as usize
    } else {
        playlists
            .chunk_cache
            .get_or_create_stream_idx(output_stream_id)
            .await
    }
}

fn rtmp_output_stream_id(
    stream: &RtmpStreamInfo,
    fallback_output_stream_id: u64,
    is_first_stream: bool,
) -> u64 {
    if is_first_stream || stream.id == 0 {
        fallback_output_stream_id
    } else {
        stream.id
    }
}

fn advertised_hls_stream_id(args: &Args) -> u64 {
    if args.rist_bind.is_some() {
        args.rist_stream_id
    } else if args.srt_bind.is_some() {
        args.srt_stream_id
    } else if args.rtmp_bind.is_some() {
        args.rtmp_stream_id
    } else {
        args.stream_id
    }
}

#[derive(Debug)]
struct AtomicDurationHistogram {
    count: AtomicU64,
    sum_us: AtomicU64,
    max_us: AtomicU64,
    buckets: [AtomicU64; DURATION_HISTOGRAM_BUCKETS_US.len()],
}

impl Default for AtomicDurationHistogram {
    fn default() -> Self {
        Self {
            count: AtomicU64::new(0),
            sum_us: AtomicU64::new(0),
            max_us: AtomicU64::new(0),
            buckets: std::array::from_fn(|_| AtomicU64::new(0)),
        }
    }
}

impl AtomicDurationHistogram {
    fn record(&self, duration: Duration) {
        let duration_us = duration.as_micros().min(u128::from(u64::MAX)) as u64;
        self.count.fetch_add(1, Ordering::Relaxed);
        self.sum_us.fetch_add(duration_us, Ordering::Relaxed);
        self.max_us.fetch_max(duration_us, Ordering::Relaxed);
        for (index, upper_bound_us) in DURATION_HISTOGRAM_BUCKETS_US.iter().enumerate() {
            if duration_us <= *upper_bound_us {
                self.buckets[index].fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    fn snapshot(&self) -> DurationHistogramSnapshot {
        let count = self.count.load(Ordering::Relaxed);
        let buckets = self
            .buckets
            .iter()
            .map(|bucket| bucket.load(Ordering::Relaxed))
            .collect::<Vec<_>>();
        DurationHistogramSnapshot {
            count,
            sum_us: self.sum_us.load(Ordering::Relaxed),
            p95_us: histogram_percentile_upper_bound_us(
                count,
                &buckets,
                95,
                self.max_us.load(Ordering::Relaxed),
            ),
            buckets,
        }
    }
}

fn histogram_percentile_upper_bound_us(
    count: u64,
    buckets: &[u64],
    percentile: u64,
    max_us: u64,
) -> Option<u64> {
    if count == 0 {
        return None;
    }
    let rank = count.saturating_mul(percentile).saturating_add(99) / 100;
    buckets
        .iter()
        .enumerate()
        .find(|(_, bucket_count)| **bucket_count >= rank)
        .map(|(index, _)| DURATION_HISTOGRAM_BUCKETS_US[index])
        .or(Some(max_us))
}

#[derive(Debug, Default)]
struct IngestTelemetry {
    media_object_source_epoch: AtomicU64,
    raw_http_requests: AtomicU64,
    raw_http_chunks: AtomicU64,
    raw_http_bytes: AtomicU64,
    raw_http_datagrams: AtomicU64,
    raw_http_last_unix_ms: AtomicU64,
    media_requests: AtomicU64,
    media_payload_bytes: AtomicU64,
    media_datagrams: AtomicU64,
    media_last_unix_ms: AtomicU64,
    mesh_stream_payloads: AtomicU64,
    mesh_stream_payload_bytes: AtomicU64,
    mesh_stream_datagrams: AtomicU64,
    mesh_stream_datagram_bytes: AtomicU64,
    mesh_stream_errors: AtomicU64,
    mesh_stream_last_unix_ms: AtomicU64,
    mesh_stream_duration: AtomicDurationHistogram,
    mesh_stream_encode_wait_duration: AtomicDurationHistogram,
    mesh_stream_encode_duration: AtomicDurationHistogram,
    mesh_stream_send_duration: AtomicDurationHistogram,
    mesh_stream_telemetry_duration: AtomicDurationHistogram,
    mesh_media_payloads: AtomicU64,
    mesh_media_payload_bytes: AtomicU64,
    mesh_media_datagrams: AtomicU64,
    mesh_media_datagram_bytes: AtomicU64,
    mesh_media_errors: AtomicU64,
    mesh_media_last_unix_ms: AtomicU64,
    mesh_media_duration: AtomicDurationHistogram,
    mesh_media_encode_wait_duration: AtomicDurationHistogram,
    mesh_media_encode_duration: AtomicDurationHistogram,
    mesh_media_send_duration: AtomicDurationHistogram,
    mesh_media_telemetry_duration: AtomicDurationHistogram,
    relay_objects_sent: AtomicU64,
    relay_encode_errors: AtomicU64,
    relay_source_datagrams: AtomicU64,
    relay_source_datagram_bytes: AtomicU64,
    relay_source_errors: AtomicU64,
    relay_repair_datagrams: AtomicU64,
    relay_repair_datagram_bytes: AtomicU64,
    relay_repair_errors: AtomicU64,
    relay_repair_primary_fallback_objects: AtomicU64,
    relay_primary_lane_objects_succeeded: AtomicU64,
    relay_primary_lane_objects_failed: AtomicU64,
    relay_primary_lane_last_success_unix_ms: AtomicU64,
    relay_primary_lane_last_failure_unix_ms: AtomicU64,
    relay_secondary_lane_objects_succeeded: AtomicU64,
    relay_secondary_lane_objects_failed: AtomicU64,
    relay_secondary_lane_last_success_unix_ms: AtomicU64,
    relay_secondary_lane_last_failure_unix_ms: AtomicU64,
    relay_surviving_lane_objects: AtomicU64,
    relay_all_lanes_failed_objects: AtomicU64,
    relay_expired_objects: AtomicU64,
    relay_expired_symbols: AtomicU64,
    relay_deadline_hits: AtomicU64,
    relay_deadline_misses: AtomicU64,
    relay_last_deadline_unix_us: AtomicU64,
    relay_total_duration: AtomicDurationHistogram,
    relay_encode_wait_duration: AtomicDurationHistogram,
    relay_encode_duration: AtomicDurationHistogram,
    relay_schedule_duration: AtomicDurationHistogram,
    relay_primary_source_send_duration: AtomicDurationHistogram,
    relay_secondary_source_send_duration: AtomicDurationHistogram,
    relay_primary_repair_send_duration: AtomicDurationHistogram,
    relay_secondary_repair_send_duration: AtomicDurationHistogram,
    mpeg_ts_slots: AtomicU64,
    mpeg_ts_bytes: AtomicU64,
    mpeg_ts_last_unix_ms: AtomicU64,
    mpeg_ts_continuity_errors: AtomicU64,
    mpeg_ts_continuity_dropped_bytes: AtomicU64,
    mpeg_ts_payload_drops: AtomicU64,
    mpeg_ts_payload_drop_bytes: AtomicU64,
    mpeg_ts_last_error_unix_ms: AtomicU64,
    rtmp_access_units: AtomicU64,
    rtmp_bytes: AtomicU64,
    rtmp_last_unix_ms: AtomicU64,
    fmp4_parts: AtomicU64,
    fmp4_bytes: AtomicU64,
    fmp4_init_bytes: AtomicU64,
    fmp4_publish_errors: AtomicU64,
    fmp4_last_publish_unix_ms: AtomicU64,
    fmp4_video_width: AtomicU64,
    fmp4_video_height: AtomicU64,
    fmp4_video_parts: AtomicU64,
    fmp4_video_access_units: AtomicU64,
    fmp4_audio_parts: AtomicU64,
    fmp4_audio_access_units: AtomicU64,
    hls_responses_total: AtomicU64,
    hls_response_errors: AtomicU64,
    hls_response_not_found: AtomicU64,
    hls_last_response_unix_ms: AtomicU64,
    recent_hls_responses: StdMutex<VecDeque<ContribHlsResponse>>,
    ingest_sessions_started: AtomicU64,
    ingest_sessions_ended: AtomicU64,
    stream_runtime: StdMutex<HashMap<u64, ContribStreamRuntimeRecord>>,
    protocol_runtime: StdMutex<HashMap<&'static str, ProtocolRuntimeRecord>>,
    ingest_sessions: StdMutex<HashMap<String, IngestSessionRecord>>,
    recent_alerts: StdMutex<VecDeque<ContribAlert>>,
    recent_activity: StdMutex<VecDeque<ContribActivity>>,
}

impl IngestTelemetry {
    fn record_relay_session_send_success(&self, role: MediaDatagramRole, wire_bytes: u64) {
        let (datagrams, bytes) = match role {
            MediaDatagramRole::Source => (
                &self.relay_source_datagrams,
                &self.relay_source_datagram_bytes,
            ),
            MediaDatagramRole::Repair => (
                &self.relay_repair_datagrams,
                &self.relay_repair_datagram_bytes,
            ),
        };
        datagrams.fetch_add(1, Ordering::Relaxed);
        bytes.fetch_add(wire_bytes, Ordering::Relaxed);
    }

    fn record_relay_session_send_error(&self, role: MediaDatagramRole) {
        match role {
            MediaDatagramRole::Source => &self.relay_source_errors,
            MediaDatagramRole::Repair => &self.relay_repair_errors,
        }
        .fetch_add(1, Ordering::Relaxed);
    }

    fn record_relay_session_lane_object(&self, path: RelayCarrierPath, succeeded: bool) {
        let (counter, last_success, last_failure) = match (path, succeeded) {
            (RelayCarrierPath::Primary, true) => (
                &self.relay_primary_lane_objects_succeeded,
                &self.relay_primary_lane_last_success_unix_ms,
                &self.relay_primary_lane_last_failure_unix_ms,
            ),
            (RelayCarrierPath::Primary, false) => (
                &self.relay_primary_lane_objects_failed,
                &self.relay_primary_lane_last_success_unix_ms,
                &self.relay_primary_lane_last_failure_unix_ms,
            ),
            (RelayCarrierPath::Secondary, true) => (
                &self.relay_secondary_lane_objects_succeeded,
                &self.relay_secondary_lane_last_success_unix_ms,
                &self.relay_secondary_lane_last_failure_unix_ms,
            ),
            (RelayCarrierPath::Secondary, false) => (
                &self.relay_secondary_lane_objects_failed,
                &self.relay_secondary_lane_last_success_unix_ms,
                &self.relay_secondary_lane_last_failure_unix_ms,
            ),
        };
        counter.fetch_add(1, Ordering::Relaxed);
        let observed_at = now_unix_ms();
        if succeeded {
            last_success.store(observed_at, Ordering::Release);
        } else {
            last_failure.store(observed_at, Ordering::Release);
        }
    }

    fn record_relay_session_encode_error(&self) {
        self.relay_encode_errors.fetch_add(1, Ordering::Relaxed);
    }

    fn record_relay_session_stage_duration(&self, stage: RelayPipelineStage, duration: Duration) {
        let histogram = match stage {
            RelayPipelineStage::Total => &self.relay_total_duration,
            RelayPipelineStage::EncodeWait => &self.relay_encode_wait_duration,
            RelayPipelineStage::Encode => &self.relay_encode_duration,
            RelayPipelineStage::Schedule => &self.relay_schedule_duration,
            RelayPipelineStage::PrimarySourceSend => &self.relay_primary_source_send_duration,
            RelayPipelineStage::SecondarySourceSend => &self.relay_secondary_source_send_duration,
            RelayPipelineStage::PrimaryRepairSend => &self.relay_primary_repair_send_duration,
            RelayPipelineStage::SecondaryRepairSend => &self.relay_secondary_repair_send_duration,
        };
        histogram.record(duration);
    }

    fn record_mesh_forward_duration(&self, kind: &'static str, duration: Duration) {
        if kind == "media" {
            self.mesh_media_duration.record(duration);
        } else {
            self.mesh_stream_duration.record(duration);
        }
    }

    fn record_mesh_forward_stage_duration(
        &self,
        kind: &'static str,
        stage: &'static str,
        duration: Duration,
    ) {
        let histogram = match (kind, stage) {
            ("media", "encode_wait") => &self.mesh_media_encode_wait_duration,
            ("media", "encode") => &self.mesh_media_encode_duration,
            ("media", "send") => &self.mesh_media_send_duration,
            ("media", "telemetry") => &self.mesh_media_telemetry_duration,
            (_, "encode_wait") => &self.mesh_stream_encode_wait_duration,
            (_, "encode") => &self.mesh_stream_encode_duration,
            (_, "send") => &self.mesh_stream_send_duration,
            (_, "telemetry") => &self.mesh_stream_telemetry_duration,
            _ => return,
        };
        histogram.record(duration);
    }

    fn record_raw_http(&self, stream_id: u64, chunks: u64, bytes: u64, datagrams: u64) {
        let now = now_unix_ms();
        let requests = self.raw_http_requests.fetch_add(1, Ordering::Relaxed) + 1;
        self.raw_http_chunks.fetch_add(chunks, Ordering::Relaxed);
        self.raw_http_bytes.fetch_add(bytes, Ordering::Relaxed);
        self.raw_http_datagrams
            .fetch_add(datagrams, Ordering::Relaxed);
        self.raw_http_last_unix_ms.store(now, Ordering::Relaxed);
        self.update_stream_runtime(stream_id, |stream| {
            stream.input_units = stream.input_units.saturating_add(1);
            stream.input_bytes = stream.input_bytes.saturating_add(bytes);
            stream.last_input_unix_ms = Some(now);
        });
        self.push_activity(ContribActivity {
            level: "info",
            code: "raw_http_ingest",
            message: format!(
                "Raw HTTP ingest request {requests} accepted {chunks} chunks for stream {stream_id}."
            ),
            stream_id_text: Some(stream_id.to_string()),
            bytes: Some(bytes),
            datagrams: Some(datagrams),
            sequence: None,
            seen_unix_ms: now_unix_ms(),
        });
        trace!(
            stream_id,
            chunks,
            bytes,
            datagrams,
            "recorded raw HTTP byte ingest"
        );
    }

    fn record_media_access_unit(&self, stream_id: u64, payload_bytes: u64, datagrams: u64) {
        let now = now_unix_ms();
        let requests = self.media_requests.fetch_add(1, Ordering::Relaxed) + 1;
        self.media_payload_bytes
            .fetch_add(payload_bytes, Ordering::Relaxed);
        self.media_datagrams.fetch_add(datagrams, Ordering::Relaxed);
        self.media_last_unix_ms.store(now, Ordering::Relaxed);
        self.update_stream_runtime(stream_id, |stream| {
            stream.input_units = stream.input_units.saturating_add(1);
            stream.input_bytes = stream.input_bytes.saturating_add(payload_bytes);
            stream.last_input_unix_ms = Some(now);
        });
        if should_sample_activity(requests, 100) {
            self.push_activity(ContribActivity {
                level: "info",
                code: "media_access_unit",
                message: format!("Forwarded media access unit {requests} for stream {stream_id}."),
                stream_id_text: Some(stream_id.to_string()),
                bytes: Some(payload_bytes),
                datagrams: Some(datagrams),
                sequence: None,
                seen_unix_ms: now_unix_ms(),
            });
        }
        trace!(
            stream_id,
            payload_bytes,
            datagrams,
            "recorded media access-unit ingest"
        );
    }

    fn record_mesh_forward_success(
        &self,
        kind: &'static str,
        stream_id: u64,
        target: SocketAddr,
        payload_bytes: u64,
        datagrams: u64,
        datagram_bytes: u64,
    ) {
        let now = now_unix_ms();
        let payloads = if kind == "media" {
            let payloads = self.mesh_media_payloads.fetch_add(1, Ordering::Relaxed) + 1;
            self.mesh_media_payload_bytes
                .fetch_add(payload_bytes, Ordering::Relaxed);
            self.mesh_media_datagrams
                .fetch_add(datagrams, Ordering::Relaxed);
            self.mesh_media_datagram_bytes
                .fetch_add(datagram_bytes, Ordering::Relaxed);
            self.mesh_media_last_unix_ms.store(now, Ordering::Relaxed);
            payloads
        } else {
            let payloads = self.mesh_stream_payloads.fetch_add(1, Ordering::Relaxed) + 1;
            self.mesh_stream_payload_bytes
                .fetch_add(payload_bytes, Ordering::Relaxed);
            self.mesh_stream_datagrams
                .fetch_add(datagrams, Ordering::Relaxed);
            self.mesh_stream_datagram_bytes
                .fetch_add(datagram_bytes, Ordering::Relaxed);
            self.mesh_stream_last_unix_ms.store(now, Ordering::Relaxed);
            payloads
        };
        self.update_stream_runtime(stream_id, |stream| {
            stream.mesh_payloads = stream.mesh_payloads.saturating_add(1);
            stream.mesh_payload_bytes = stream.mesh_payload_bytes.saturating_add(payload_bytes);
            stream.mesh_datagrams = stream.mesh_datagrams.saturating_add(datagrams);
            stream.mesh_datagram_bytes = stream.mesh_datagram_bytes.saturating_add(datagram_bytes);
            stream.last_mesh_forward_unix_ms = Some(now);
        });

        if should_sample_activity(payloads, 100) {
            self.push_activity(ContribActivity {
                level: "info",
                code: "mesh_forward",
                message: format!(
                    "Forwarded {kind} payload {payloads} for stream {stream_id} to mesh target {target}."
                ),
                stream_id_text: Some(stream_id.to_string()),
                bytes: Some(payload_bytes),
                datagrams: Some(datagrams),
                sequence: Some(payloads),
                seen_unix_ms: now,
            });
        }
        trace!(
            kind,
            stream_id,
            target = %target,
            payload_bytes,
            datagrams,
            datagram_bytes,
            "recorded mesh forward success"
        );
    }

    fn record_mesh_forward_error(
        &self,
        kind: &'static str,
        stream_id: u64,
        target: SocketAddr,
        error: &anyhow::Error,
    ) {
        let now = now_unix_ms();
        let errors = if kind == "media" {
            self.mesh_media_errors.fetch_add(1, Ordering::Relaxed) + 1
        } else {
            self.mesh_stream_errors.fetch_add(1, Ordering::Relaxed) + 1
        };
        self.update_stream_runtime(stream_id, |stream| {
            stream.mesh_errors = stream.mesh_errors.saturating_add(1);
            stream.last_mesh_forward_unix_ms = Some(now);
        });
        let message = format!(
            "Failed to forward {kind} payload for stream {stream_id} to mesh target {target}: {error}"
        );
        self.push_alert(ContribAlert {
            level: "warn",
            code: "mesh_forward_error",
            message: message.clone(),
            count: errors,
            last_seen_unix_ms: Some(now),
            stream_id_text: Some(stream_id.to_string()),
            protocol: None,
        });
        self.push_activity(ContribActivity {
            level: "warn",
            code: "mesh_forward_error",
            message,
            stream_id_text: Some(stream_id.to_string()),
            bytes: None,
            datagrams: None,
            sequence: Some(errors),
            seen_unix_ms: now,
        });
    }

    fn record_mpeg_ts_slot(&self, protocol: &'static str, stream_id: u64, bytes: usize) {
        let now = now_unix_ms();
        let slots = self.mpeg_ts_slots.fetch_add(1, Ordering::Relaxed) + 1;
        self.mpeg_ts_bytes
            .fetch_add(bytes as u64, Ordering::Relaxed);
        self.mpeg_ts_last_unix_ms.store(now, Ordering::Relaxed);
        self.record_protocol_unit(protocol, bytes as u64);
        self.update_stream_runtime(stream_id, |stream| {
            stream.input_units = stream.input_units.saturating_add(1);
            stream.input_bytes = stream.input_bytes.saturating_add(bytes as u64);
            stream.last_input_unix_ms = Some(now);
        });
        if should_sample_activity(slots, 25) {
            self.push_activity(ContribActivity {
                level: "info",
                code: "mpeg_ts_slot",
                message: format!(
                    "Read MPEG-TS slot {slots} from {protocol} for stream {stream_id}."
                ),
                stream_id_text: Some(stream_id.to_string()),
                bytes: Some(bytes as u64),
                datagrams: None,
                sequence: Some(slots),
                seen_unix_ms: now_unix_ms(),
            });
        }
        trace!(protocol, stream_id, bytes, "recorded MPEG-TS ingest slot");
    }

    fn record_mpeg_ts_continuity_issue(&self, issue: MpegTsContinuityIssue) {
        let now = now_unix_ms();
        let errors = self
            .mpeg_ts_continuity_errors
            .fetch_add(1, Ordering::Relaxed)
            + 1;
        self.mpeg_ts_continuity_dropped_bytes
            .fetch_add(issue.dropped_payload_bytes as u64, Ordering::Relaxed);
        self.mpeg_ts_last_error_unix_ms
            .store(now, Ordering::Relaxed);
        if should_sample_activity(errors, 25) {
            self.push_activity(ContribActivity {
                level: "warn",
                code: "mpeg_ts_continuity_error",
                message: format!(
                    "MPEG-TS continuity error {errors} on {} dropped {} partial payload bytes.",
                    issue.stream_type, issue.dropped_payload_bytes
                ),
                stream_id_text: None,
                bytes: Some(issue.dropped_payload_bytes as u64),
                datagrams: None,
                sequence: Some(errors),
                seen_unix_ms: now,
            });
        }
    }

    fn record_mpeg_ts_payload_drop(&self, drop: MpegTsPayloadDrop) {
        let now = now_unix_ms();
        let drops = self.mpeg_ts_payload_drops.fetch_add(1, Ordering::Relaxed) + 1;
        self.mpeg_ts_payload_drop_bytes
            .fetch_add(drop.bytes as u64, Ordering::Relaxed);
        self.mpeg_ts_last_error_unix_ms
            .store(now, Ordering::Relaxed);
        if should_sample_activity(drops, 25) {
            self.push_activity(ContribActivity {
                level: "warn",
                code: "mpeg_ts_payload_drop",
                message: format!(
                    "Dropped oversized MPEG-TS {} PES payload of {} bytes.",
                    drop.stream_type, drop.bytes
                ),
                stream_id_text: None,
                bytes: Some(drop.bytes as u64),
                datagrams: None,
                sequence: Some(drops),
                seen_unix_ms: now,
            });
        }
    }

    fn record_rtmp_access_unit(&self, stream_id: u64, bytes: usize) {
        let now = now_unix_ms();
        let access_units = self.rtmp_access_units.fetch_add(1, Ordering::Relaxed) + 1;
        self.rtmp_bytes.fetch_add(bytes as u64, Ordering::Relaxed);
        self.rtmp_last_unix_ms.store(now, Ordering::Relaxed);
        self.record_protocol_unit("rtmp", bytes as u64);
        self.update_stream_runtime(stream_id, |stream| {
            stream.input_units = stream.input_units.saturating_add(1);
            stream.input_bytes = stream.input_bytes.saturating_add(bytes as u64);
            stream.last_input_unix_ms = Some(now);
        });
        if should_sample_activity(access_units, 100) {
            self.push_activity(ContribActivity {
                level: "info",
                code: "rtmp_access_unit",
                message: format!(
                    "Forwarded RTMP access unit {access_units} for stream {stream_id}."
                ),
                stream_id_text: Some(stream_id.to_string()),
                bytes: Some(bytes as u64),
                datagrams: None,
                sequence: Some(access_units),
                seen_unix_ms: now_unix_ms(),
            });
        }
        trace!(stream_id, bytes, "recorded RTMP access unit");
    }

    fn record_protocol_unit(&self, protocol: &'static str, bytes: u64) {
        let now = now_unix_ms();
        if let Ok(mut records) = self.protocol_runtime.lock() {
            let record = records
                .entry(protocol)
                .or_insert_with(|| ProtocolRuntimeRecord {
                    ..ProtocolRuntimeRecord::default()
                });
            record.units = record.units.saturating_add(1);
            record.bytes = record.bytes.saturating_add(bytes);
            record.last_seen_unix_ms = Some(now);
        }
    }

    fn record_fmp4_part(
        &self,
        stream_id: u64,
        stream_idx: usize,
        sequence: u64,
        bytes: u64,
        init_bytes: u64,
    ) {
        let now = now_unix_ms();
        let parts = self.fmp4_parts.fetch_add(1, Ordering::Relaxed) + 1;
        self.fmp4_bytes.fetch_add(bytes, Ordering::Relaxed);
        self.fmp4_init_bytes
            .fetch_add(init_bytes, Ordering::Relaxed);
        self.fmp4_last_publish_unix_ms.store(now, Ordering::Relaxed);
        self.update_stream_runtime(stream_id, |stream| {
            stream.fmp4_parts = stream.fmp4_parts.saturating_add(1);
            stream.fmp4_bytes = stream.fmp4_bytes.saturating_add(bytes);
            stream.fmp4_init_bytes = stream.fmp4_init_bytes.saturating_add(init_bytes);
            stream.latest_fmp4_sequence = Some(sequence);
            stream.last_fmp4_unix_ms = Some(now);
        });
        if should_sample_activity(parts, 25) {
            self.push_activity(ContribActivity {
                level: "info",
                code: "fmp4_part_published",
                message: format!(
                    "Published fMP4 part {sequence} for stream {stream_id} idx {stream_idx}."
                ),
                stream_id_text: Some(stream_id.to_string()),
                bytes: Some(bytes),
                datagrams: None,
                sequence: Some(sequence),
                seen_unix_ms: now_unix_ms(),
            });
        }
        trace!(
            stream_id,
            stream_idx,
            sequence,
            bytes,
            init_bytes,
            "recorded fMP4 mesh publish"
        );
    }

    #[allow(clippy::too_many_arguments)]
    fn record_fmp4_tracks(
        &self,
        stream_id: u64,
        stream_idx: usize,
        sequence: u64,
        video_width: Option<u16>,
        video_height: Option<u16>,
        video_units: usize,
        audio_units: usize,
    ) {
        if let (Some(width), Some(height)) = (video_width, video_height) {
            self.fmp4_video_width
                .store(u64::from(width), Ordering::Relaxed);
            self.fmp4_video_height
                .store(u64::from(height), Ordering::Relaxed);
        }
        if video_units > 0 {
            self.fmp4_video_parts.fetch_add(1, Ordering::Relaxed);
            self.fmp4_video_access_units
                .fetch_add(video_units as u64, Ordering::Relaxed);
        }
        if audio_units > 0 {
            self.fmp4_audio_parts.fetch_add(1, Ordering::Relaxed);
            self.fmp4_audio_access_units
                .fetch_add(audio_units as u64, Ordering::Relaxed);
        }
        self.update_stream_runtime(stream_id, |stream| {
            if video_units > 0 {
                stream.video_codec = Some("h264");
                if let (Some(width), Some(height)) = (video_width, video_height) {
                    stream.video_width = Some(width);
                    stream.video_height = Some(height);
                }
                stream.video_parts = stream.video_parts.saturating_add(1);
                stream.video_access_units =
                    stream.video_access_units.saturating_add(video_units as u64);
            }
            if audio_units > 0 {
                stream.audio_codec = Some("aac");
                stream.audio_parts = stream.audio_parts.saturating_add(1);
                stream.audio_access_units =
                    stream.audio_access_units.saturating_add(audio_units as u64);
            }
            stream.latest_fmp4_sequence = Some(sequence);
        });
        trace!(
            stream_id,
            stream_idx,
            sequence,
            video_width,
            video_height,
            video_units,
            audio_units,
            "recorded fMP4 media track metadata"
        );
    }

    fn record_fmp4_publish_error(
        &self,
        stream_id: u64,
        stream_idx: usize,
        sequence: u64,
        error: &str,
    ) {
        let now = now_unix_ms();
        self.fmp4_publish_errors.fetch_add(1, Ordering::Relaxed);
        self.update_stream_runtime(stream_id, |stream| {
            stream.fmp4_publish_errors = stream.fmp4_publish_errors.saturating_add(1);
            stream.latest_fmp4_sequence = Some(sequence);
            stream.last_fmp4_unix_ms = Some(now);
        });
        self.push_alert(ContribAlert {
            level: "warn",
            code: "fmp4_publish_error",
            message: format!(
                "Failed to publish fMP4 part {sequence} for stream {stream_id} idx {stream_idx}: {error}"
            ),
            count: 1,
            last_seen_unix_ms: Some(now),
            stream_id_text: Some(stream_id.to_string()),
            protocol: None,
        });
        self.push_activity(ContribActivity {
            level: "warn",
            code: "fmp4_publish_error",
            message: format!(
                "Failed to publish fMP4 part {sequence} for stream {stream_id} idx {stream_idx}: {error}"
            ),
            stream_id_text: Some(stream_id.to_string()),
            bytes: None,
            datagrams: None,
            sequence: Some(sequence),
            seen_unix_ms: now,
        });
    }

    fn record_hls_response(
        &self,
        method: &Method,
        path: &str,
        query: Option<&str>,
        response: &HandlerResponse,
    ) {
        let unix_ms = now_unix_ms();
        let status = response.status.as_u16();
        let bytes = response
            .body
            .as_ref()
            .map(|body| body.len() as u64)
            .unwrap_or(0);
        let responses = self.hls_responses_total.fetch_add(1, Ordering::Relaxed) + 1;
        if response.status.is_client_error() || response.status.is_server_error() {
            self.hls_response_errors.fetch_add(1, Ordering::Relaxed);
            self.push_activity(ContribActivity {
                level: if status >= 500 { "error" } else { "warn" },
                code: "hls_response_error",
                message: format!("HLS request {method} {path} returned HTTP {status}."),
                stream_id_text: None,
                bytes: Some(bytes),
                datagrams: None,
                sequence: Some(responses),
                seen_unix_ms: unix_ms,
            });
        }
        if response.status == StatusCode::NOT_FOUND {
            self.hls_response_not_found.fetch_add(1, Ordering::Relaxed);
        }
        self.hls_last_response_unix_ms
            .store(unix_ms, Ordering::Relaxed);

        if let Ok(mut recent) = self.recent_hls_responses.lock() {
            recent.push_front(ContribHlsResponse {
                unix_ms,
                method: method.as_str().into(),
                path: path.into(),
                query: query.map(ToOwned::to_owned),
                status,
                bytes,
                content_type: response.content_type.clone(),
            });
            while recent.len() > CONTRIB_HLS_RESPONSE_LIMIT {
                recent.pop_back();
            }
        }
    }

    fn ensure_ingest_session(
        &self,
        protocol: &'static str,
        stream_id: u64,
        output_stream_id: Option<u64>,
        output_stream_idx: Option<usize>,
        peer: Option<String>,
        path: Option<String>,
    ) {
        let now = now_unix_ms();
        let stream_id_text = stream_id.to_string();
        let mut started_count = None;

        if let Ok(mut sessions) = self.ingest_sessions.lock() {
            if let Some(session) = active_ingest_session_mut(&mut sessions, protocol, stream_id) {
                session.last_seen_unix_ms = now;
                if let Some(output_stream_id) = output_stream_id {
                    session.output_stream_id_text = Some(output_stream_id.to_string());
                }
                if output_stream_idx.is_some() {
                    session.output_stream_idx = output_stream_idx;
                }
                if peer.is_some() {
                    session.peer = peer;
                }
                if path.is_some() {
                    session.path = path;
                }
            } else {
                let sequence = self.ingest_sessions_started.fetch_add(1, Ordering::Relaxed) + 1;
                started_count = Some(sequence);
                sessions.insert(
                    ingest_session_key(protocol, stream_id, sequence),
                    IngestSessionRecord {
                        session_id: sequence,
                        protocol,
                        stream_id_text,
                        output_stream_id_text: output_stream_id.map(|id| id.to_string()),
                        output_stream_idx,
                        peer,
                        path,
                        state: "active",
                        started_unix_ms: now,
                        last_seen_unix_ms: now,
                        ended_unix_ms: None,
                        body_slots: 0,
                        bytes: 0,
                        access_units: 0,
                        end_reason: None,
                    },
                );
            }
            prune_ingest_sessions(&mut sessions);
        }

        if let Some(started_count) = started_count {
            self.push_activity(ContribActivity {
                level: "info",
                code: "ingest_session_started",
                message: format!(
                    "{protocol} ingest session {started_count} started for stream {stream_id}."
                ),
                stream_id_text: Some(stream_id.to_string()),
                bytes: None,
                datagrams: None,
                sequence: Some(started_count),
                seen_unix_ms: now,
            });
        }
    }

    fn record_ingest_session_body(&self, protocol: &'static str, stream_id: u64, bytes: usize) {
        self.ensure_ingest_session(protocol, stream_id, None, None, None, None);
        let now = now_unix_ms();
        if let Ok(mut sessions) = self.ingest_sessions.lock() {
            if let Some(session) = active_ingest_session_mut(&mut sessions, protocol, stream_id) {
                session.last_seen_unix_ms = now;
                session.body_slots = session.body_slots.saturating_add(1);
                session.bytes = session.bytes.saturating_add(bytes as u64);
            }
        }
    }

    fn record_ingest_session_access_unit(
        &self,
        protocol: &'static str,
        stream_id: u64,
        bytes: usize,
    ) {
        self.ensure_ingest_session(protocol, stream_id, None, None, None, None);
        let now = now_unix_ms();
        if let Ok(mut sessions) = self.ingest_sessions.lock() {
            if let Some(session) = active_ingest_session_mut(&mut sessions, protocol, stream_id) {
                session.last_seen_unix_ms = now;
                session.access_units = session.access_units.saturating_add(1);
                session.bytes = session.bytes.saturating_add(bytes as u64);
            }
        }
    }

    fn end_ingest_session(&self, protocol: &'static str, stream_id: u64, reason: &'static str) {
        let now = now_unix_ms();
        let mut ended = false;
        if let Ok(mut sessions) = self.ingest_sessions.lock() {
            if let Some(session) = active_ingest_session_mut(&mut sessions, protocol, stream_id) {
                ended = true;
                session.state = "ended";
                session.last_seen_unix_ms = now;
                session.ended_unix_ms = Some(now);
                session.end_reason = Some(reason);
            }
            prune_ingest_sessions(&mut sessions);
        }

        if ended {
            let ended_count = self.ingest_sessions_ended.fetch_add(1, Ordering::Relaxed) + 1;
            self.push_activity(ContribActivity {
                level: "info",
                code: "ingest_session_ended",
                message: format!(
                    "{protocol} ingest session ended for stream {stream_id}: {reason}."
                ),
                stream_id_text: Some(stream_id.to_string()),
                bytes: None,
                datagrams: None,
                sequence: Some(ended_count),
                seen_unix_ms: now,
            });
        }
    }

    fn snapshot(&self) -> IngestRuntimeSnapshot {
        let now_ms = now_unix_ms();
        let ingest_session_records = self
            .ingest_sessions
            .lock()
            .map(|sessions| sessions.values().cloned().collect::<Vec<_>>())
            .unwrap_or_default();
        let active_ingest_sessions = ingest_session_records
            .iter()
            .filter(|session| session.state == "active")
            .count();
        let protocols = self.protocol_snapshots(&ingest_session_records, now_ms);
        let streams = self.stream_snapshots(now_ms);
        let mut recent_ingest_sessions = ingest_session_records
            .into_iter()
            .map(|session| IngestSessionSnapshot::from_record(session, now_ms))
            .collect::<Vec<_>>();
        recent_ingest_sessions.sort_by(|left, right| {
            right
                .last_seen_unix_ms
                .cmp(&left.last_seen_unix_ms)
                .then_with(|| left.protocol.cmp(right.protocol))
                .then_with(|| left.stream_id_text.cmp(&right.stream_id_text))
        });
        recent_ingest_sessions.truncate(CONTRIB_INGEST_SESSION_LIMIT);

        IngestRuntimeSnapshot {
            raw_http: RawHttpRuntimeSnapshot {
                requests: self.raw_http_requests.load(Ordering::Relaxed),
                chunks: self.raw_http_chunks.load(Ordering::Relaxed),
                bytes: self.raw_http_bytes.load(Ordering::Relaxed),
                datagrams: self.raw_http_datagrams.load(Ordering::Relaxed),
                last_seen_unix_ms: nonzero_unix_ms(
                    self.raw_http_last_unix_ms.load(Ordering::Relaxed),
                ),
                last_seen_age_ms: age_from_atomic_ms(now_ms, &self.raw_http_last_unix_ms),
            },
            media_access_units: MediaRuntimeSnapshot {
                requests: self.media_requests.load(Ordering::Relaxed),
                payload_bytes: self.media_payload_bytes.load(Ordering::Relaxed),
                datagrams: self.media_datagrams.load(Ordering::Relaxed),
                last_seen_unix_ms: nonzero_unix_ms(self.media_last_unix_ms.load(Ordering::Relaxed)),
                last_seen_age_ms: age_from_atomic_ms(now_ms, &self.media_last_unix_ms),
            },
            mesh_forward: MeshForwardRuntimeSnapshot {
                stream_payloads: self.mesh_stream_payloads.load(Ordering::Relaxed),
                stream_payload_bytes: self.mesh_stream_payload_bytes.load(Ordering::Relaxed),
                stream_datagrams: self.mesh_stream_datagrams.load(Ordering::Relaxed),
                stream_datagram_bytes: self.mesh_stream_datagram_bytes.load(Ordering::Relaxed),
                stream_errors: self.mesh_stream_errors.load(Ordering::Relaxed),
                stream_last_unix_ms: nonzero_unix_ms(
                    self.mesh_stream_last_unix_ms.load(Ordering::Relaxed),
                ),
                stream_last_age_ms: age_from_atomic_ms(now_ms, &self.mesh_stream_last_unix_ms),
                stream_duration: self.mesh_stream_duration.snapshot(),
                stream_stages: MeshForwardStageRuntimeSnapshot {
                    encode_wait: self.mesh_stream_encode_wait_duration.snapshot(),
                    encode: self.mesh_stream_encode_duration.snapshot(),
                    send: self.mesh_stream_send_duration.snapshot(),
                    telemetry: self.mesh_stream_telemetry_duration.snapshot(),
                },
                media_payloads: self.mesh_media_payloads.load(Ordering::Relaxed),
                media_payload_bytes: self.mesh_media_payload_bytes.load(Ordering::Relaxed),
                media_datagrams: self.mesh_media_datagrams.load(Ordering::Relaxed),
                media_datagram_bytes: self.mesh_media_datagram_bytes.load(Ordering::Relaxed),
                media_errors: self.mesh_media_errors.load(Ordering::Relaxed),
                media_last_unix_ms: nonzero_unix_ms(
                    self.mesh_media_last_unix_ms.load(Ordering::Relaxed),
                ),
                media_last_age_ms: age_from_atomic_ms(now_ms, &self.mesh_media_last_unix_ms),
                media_duration: self.mesh_media_duration.snapshot(),
                media_stages: MeshForwardStageRuntimeSnapshot {
                    encode_wait: self.mesh_media_encode_wait_duration.snapshot(),
                    encode: self.mesh_media_encode_duration.snapshot(),
                    send: self.mesh_media_send_duration.snapshot(),
                    telemetry: self.mesh_media_telemetry_duration.snapshot(),
                },
            },
            relay_session: RelaySessionRuntimeSnapshot {
                objects_sent: self.relay_objects_sent.load(Ordering::Relaxed),
                encode_errors: self.relay_encode_errors.load(Ordering::Relaxed),
                source_datagrams: self.relay_source_datagrams.load(Ordering::Relaxed),
                source_datagram_bytes: self.relay_source_datagram_bytes.load(Ordering::Relaxed),
                source_errors: self.relay_source_errors.load(Ordering::Relaxed),
                repair_datagrams: self.relay_repair_datagrams.load(Ordering::Relaxed),
                repair_datagram_bytes: self.relay_repair_datagram_bytes.load(Ordering::Relaxed),
                repair_errors: self.relay_repair_errors.load(Ordering::Relaxed),
                repair_primary_fallback_objects: self
                    .relay_repair_primary_fallback_objects
                    .load(Ordering::Relaxed),
                primary_lane_objects_succeeded: self
                    .relay_primary_lane_objects_succeeded
                    .load(Ordering::Relaxed),
                primary_lane_objects_failed: self
                    .relay_primary_lane_objects_failed
                    .load(Ordering::Relaxed),
                primary_lane_state: relay_lane_state(
                    now_ms,
                    self.relay_primary_lane_last_success_unix_ms
                        .load(Ordering::Acquire),
                    self.relay_primary_lane_last_failure_unix_ms
                        .load(Ordering::Acquire),
                )
                .as_str(),
                secondary_lane_objects_succeeded: self
                    .relay_secondary_lane_objects_succeeded
                    .load(Ordering::Relaxed),
                secondary_lane_objects_failed: self
                    .relay_secondary_lane_objects_failed
                    .load(Ordering::Relaxed),
                secondary_lane_state: relay_lane_state(
                    now_ms,
                    self.relay_secondary_lane_last_success_unix_ms
                        .load(Ordering::Acquire),
                    self.relay_secondary_lane_last_failure_unix_ms
                        .load(Ordering::Acquire),
                )
                .as_str(),
                surviving_lane_objects: self.relay_surviving_lane_objects.load(Ordering::Relaxed),
                all_lanes_failed_objects: self
                    .relay_all_lanes_failed_objects
                    .load(Ordering::Relaxed),
                expired_objects: self.relay_expired_objects.load(Ordering::Relaxed),
                expired_symbols: self.relay_expired_symbols.load(Ordering::Relaxed),
                deadline_hits: self.relay_deadline_hits.load(Ordering::Relaxed),
                deadline_misses: self.relay_deadline_misses.load(Ordering::Relaxed),
                last_deadline_unix_us: nonzero_unix_us(
                    self.relay_last_deadline_unix_us.load(Ordering::Relaxed),
                ),
                last_deadline_headroom_us: nonzero_unix_us(
                    self.relay_last_deadline_unix_us.load(Ordering::Relaxed),
                )
                .map(|deadline| deadline.saturating_sub(now_ms.saturating_mul(1_000))),
                stages: RelaySessionStageRuntimeSnapshot {
                    total: self.relay_total_duration.snapshot(),
                    encode_wait: self.relay_encode_wait_duration.snapshot(),
                    encode: self.relay_encode_duration.snapshot(),
                    schedule: self.relay_schedule_duration.snapshot(),
                    primary_source_send: self.relay_primary_source_send_duration.snapshot(),
                    secondary_source_send: self.relay_secondary_source_send_duration.snapshot(),
                    primary_repair_send: self.relay_primary_repair_send_duration.snapshot(),
                    secondary_repair_send: self.relay_secondary_repair_send_duration.snapshot(),
                },
            },
            mpeg_ts: MpegTsRuntimeSnapshot {
                slots: self.mpeg_ts_slots.load(Ordering::Relaxed),
                bytes: self.mpeg_ts_bytes.load(Ordering::Relaxed),
                last_seen_unix_ms: nonzero_unix_ms(
                    self.mpeg_ts_last_unix_ms.load(Ordering::Relaxed),
                ),
                last_seen_age_ms: age_from_atomic_ms(now_ms, &self.mpeg_ts_last_unix_ms),
                continuity_errors: self.mpeg_ts_continuity_errors.load(Ordering::Relaxed),
                continuity_dropped_bytes: self
                    .mpeg_ts_continuity_dropped_bytes
                    .load(Ordering::Relaxed),
                payload_drops: self.mpeg_ts_payload_drops.load(Ordering::Relaxed),
                payload_drop_bytes: self.mpeg_ts_payload_drop_bytes.load(Ordering::Relaxed),
                last_error_unix_ms: nonzero_unix_ms(
                    self.mpeg_ts_last_error_unix_ms.load(Ordering::Relaxed),
                ),
                last_error_age_ms: age_from_atomic_ms(now_ms, &self.mpeg_ts_last_error_unix_ms),
            },
            rtmp: RtmpRuntimeSnapshot {
                access_units: self.rtmp_access_units.load(Ordering::Relaxed),
                bytes: self.rtmp_bytes.load(Ordering::Relaxed),
                last_seen_unix_ms: nonzero_unix_ms(self.rtmp_last_unix_ms.load(Ordering::Relaxed)),
                last_seen_age_ms: age_from_atomic_ms(now_ms, &self.rtmp_last_unix_ms),
            },
            fmp4: Fmp4RuntimeSnapshot {
                parts: self.fmp4_parts.load(Ordering::Relaxed),
                bytes: self.fmp4_bytes.load(Ordering::Relaxed),
                init_bytes: self.fmp4_init_bytes.load(Ordering::Relaxed),
                publish_errors: self.fmp4_publish_errors.load(Ordering::Relaxed),
                last_publish_unix_ms: nonzero_unix_ms(
                    self.fmp4_last_publish_unix_ms.load(Ordering::Relaxed),
                ),
                last_publish_age_ms: age_from_atomic_ms(now_ms, &self.fmp4_last_publish_unix_ms),
                video_codec: (self.fmp4_video_width.load(Ordering::Relaxed) > 0).then_some("h264"),
                video_width: nonzero_u16(self.fmp4_video_width.load(Ordering::Relaxed)),
                video_height: nonzero_u16(self.fmp4_video_height.load(Ordering::Relaxed)),
                video_parts: self.fmp4_video_parts.load(Ordering::Relaxed),
                video_access_units: self.fmp4_video_access_units.load(Ordering::Relaxed),
                audio_codec: (self.fmp4_audio_parts.load(Ordering::Relaxed) > 0).then_some("aac"),
                audio_parts: self.fmp4_audio_parts.load(Ordering::Relaxed),
                audio_access_units: self.fmp4_audio_access_units.load(Ordering::Relaxed),
            },
            hls: HlsRuntimeSnapshot {
                responses_total: self.hls_responses_total.load(Ordering::Relaxed),
                response_errors: self.hls_response_errors.load(Ordering::Relaxed),
                response_not_found: self.hls_response_not_found.load(Ordering::Relaxed),
                last_response_unix_ms: nonzero_unix_ms(
                    self.hls_last_response_unix_ms.load(Ordering::Relaxed),
                ),
                last_response_age_ms: age_from_atomic_ms(now_ms, &self.hls_last_response_unix_ms),
                recent_responses: self
                    .recent_hls_responses
                    .lock()
                    .map(|responses| responses.iter().cloned().collect())
                    .unwrap_or_default(),
            },
            ingest_sessions: IngestSessionsRuntimeSnapshot {
                active: active_ingest_sessions,
                started: self.ingest_sessions_started.load(Ordering::Relaxed),
                ended: self.ingest_sessions_ended.load(Ordering::Relaxed),
                recent: recent_ingest_sessions,
            },
            streams,
            protocols,
        }
    }

    fn update_stream_runtime(
        &self,
        stream_id: u64,
        update: impl FnOnce(&mut ContribStreamRuntimeRecord),
    ) {
        if let Ok(mut records) = self.stream_runtime.lock() {
            let record = records
                .entry(stream_id)
                .or_insert_with(|| ContribStreamRuntimeRecord {
                    stream_id_text: stream_id.to_string(),
                    ..ContribStreamRuntimeRecord::default()
                });
            update(record);
        }
    }

    fn stream_snapshots(&self, now_ms: u64) -> Vec<ContribStreamRuntimeSnapshot> {
        let mut snapshots = self
            .stream_runtime
            .lock()
            .map(|records| {
                records
                    .values()
                    .cloned()
                    .map(|record| ContribStreamRuntimeSnapshot::from_record(record, now_ms))
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
        snapshots.sort_by(|left, right| {
            right
                .last_seen_unix_ms()
                .cmp(&left.last_seen_unix_ms())
                .then_with(|| left.stream_id_text.cmp(&right.stream_id_text))
        });
        snapshots.truncate(CONTRIB_INGEST_SESSION_LIMIT);
        snapshots
    }

    fn protocol_snapshots(
        &self,
        sessions: &[IngestSessionRecord],
        now_ms: u64,
    ) -> Vec<ProtocolRuntimeSnapshot> {
        let mut records = self
            .protocol_runtime
            .lock()
            .map(|records| {
                records
                    .iter()
                    .map(|(protocol, record)| (*protocol, record.clone()))
                    .collect::<HashMap<_, _>>()
            })
            .unwrap_or_default();

        for protocol in ["rist", "srt", "rtmp"] {
            records
                .entry(protocol)
                .or_insert_with(|| ProtocolRuntimeRecord {
                    ..ProtocolRuntimeRecord::default()
                });
        }

        let mut snapshots = records
            .into_iter()
            .map(|(protocol, record)| {
                let active_sessions = sessions
                    .iter()
                    .filter(|session| session.protocol == protocol && session.state == "active")
                    .count();
                let ended_sessions = sessions
                    .iter()
                    .filter(|session| session.protocol == protocol && session.state == "ended")
                    .count();
                let latest_session_seen = sessions
                    .iter()
                    .filter(|session| session.protocol == protocol)
                    .map(|session| session.last_seen_unix_ms)
                    .max();
                let last_seen_unix_ms = [record.last_seen_unix_ms, latest_session_seen]
                    .into_iter()
                    .flatten()
                    .max();
                ProtocolRuntimeSnapshot {
                    protocol,
                    units: record.units,
                    bytes: record.bytes,
                    active_sessions,
                    ended_sessions,
                    last_seen_unix_ms,
                    last_seen_age_ms: last_seen_unix_ms.map(|seen| now_ms.saturating_sub(seen)),
                }
            })
            .collect::<Vec<_>>();
        snapshots.sort_by(|left, right| left.protocol.cmp(right.protocol));
        snapshots
    }

    fn recent_alerts(&self) -> Vec<ContribAlert> {
        self.recent_alerts
            .lock()
            .map(|alerts| alerts.iter().cloned().collect())
            .unwrap_or_default()
    }

    fn recent_activity(&self) -> Vec<ContribActivity> {
        self.recent_activity
            .lock()
            .map(|activity| activity.iter().cloned().collect())
            .unwrap_or_default()
    }

    fn push_alert(&self, alert: ContribAlert) {
        if let Ok(mut alerts) = self.recent_alerts.lock() {
            if let Some(existing) = alerts
                .iter_mut()
                .find(|existing| existing.code == alert.code && existing.message == alert.message)
            {
                existing.count = existing.count.saturating_add(1);
                existing.last_seen_unix_ms = alert.last_seen_unix_ms;
                return;
            }
            alerts.push_front(alert);
            while alerts.len() > 32 {
                alerts.pop_back();
            }
        }
    }

    fn push_activity(&self, activity: ContribActivity) {
        if let Ok(mut recent) = self.recent_activity.lock() {
            recent.push_front(activity);
            while recent.len() > CONTRIB_ACTIVITY_LIMIT {
                recent.pop_back();
            }
        }
    }
}

fn should_sample_activity(count: u64, interval: u64) -> bool {
    count <= 3 || count.is_multiple_of(interval)
}

#[derive(Debug, Clone, Default)]
struct ProtocolRuntimeRecord {
    units: u64,
    bytes: u64,
    last_seen_unix_ms: Option<u64>,
}

#[derive(Debug, Clone, Default)]
struct ContribStreamRuntimeRecord {
    stream_id_text: String,
    input_units: u64,
    input_bytes: u64,
    mesh_payloads: u64,
    mesh_payload_bytes: u64,
    mesh_datagrams: u64,
    mesh_datagram_bytes: u64,
    mesh_errors: u64,
    fmp4_parts: u64,
    fmp4_bytes: u64,
    fmp4_init_bytes: u64,
    fmp4_publish_errors: u64,
    latest_fmp4_sequence: Option<u64>,
    video_codec: Option<&'static str>,
    video_width: Option<u16>,
    video_height: Option<u16>,
    video_parts: u64,
    video_access_units: u64,
    audio_codec: Option<&'static str>,
    audio_parts: u64,
    audio_access_units: u64,
    last_input_unix_ms: Option<u64>,
    last_mesh_forward_unix_ms: Option<u64>,
    last_fmp4_unix_ms: Option<u64>,
}

fn nonzero_unix_ms(value: u64) -> Option<u64> {
    (value != 0).then_some(value)
}

fn nonzero_unix_us(value: u64) -> Option<u64> {
    (value != 0).then_some(value)
}

fn nonzero_u16(value: u64) -> Option<u16> {
    (value != 0).then_some(value.min(u64::from(u16::MAX)) as u16)
}

fn age_from_atomic_ms(now_ms: u64, value: &AtomicU64) -> Option<u64> {
    nonzero_unix_ms(value.load(Ordering::Relaxed)).map(|then| now_ms.saturating_sub(then))
}

fn youngest_age(values: impl IntoIterator<Item = Option<u64>>) -> Option<u64> {
    values.into_iter().flatten().min()
}

fn ingest_session_key(protocol: &str, stream_id: u64, session_id: u64) -> String {
    format!("{protocol}:{stream_id}:{session_id}")
}

fn active_ingest_session_mut<'a>(
    sessions: &'a mut HashMap<String, IngestSessionRecord>,
    protocol: &str,
    stream_id: u64,
) -> Option<&'a mut IngestSessionRecord> {
    let stream_id_text = stream_id.to_string();
    sessions.values_mut().find(|session| {
        session.protocol == protocol
            && session.stream_id_text == stream_id_text
            && session.state == "active"
    })
}

fn prune_ingest_sessions(sessions: &mut HashMap<String, IngestSessionRecord>) {
    if sessions.len() <= CONTRIB_INGEST_SESSION_LIMIT * 2 {
        return;
    }

    let mut inactive = sessions
        .iter()
        .filter(|(_, session)| session.state != "active")
        .map(|(key, session)| (key.clone(), session.last_seen_unix_ms))
        .collect::<Vec<_>>();
    inactive.sort_by_key(|(_, last_seen)| *last_seen);

    let remove_count = sessions
        .len()
        .saturating_sub(CONTRIB_INGEST_SESSION_LIMIT * 2);
    for (key, _) in inactive.into_iter().take(remove_count) {
        sessions.remove(&key);
    }
}

#[derive(Debug, Clone)]
struct ContribStatusConfig {
    default_stream_id: String,
    advertised_hls_stream_id: String,
    advertised_hls_path: String,
    mesh: MeshTargetStatus,
    hls: HlsStatus,
    fec: FecStatus,
    listeners: Vec<ListenerStatus>,
    alerts: Vec<ContribAlert>,
    telemetry: Arc<IngestTelemetry>,
}

impl ContribStatusConfig {
    fn from_args(args: &Args, telemetry: Arc<IngestTelemetry>) -> Self {
        let advertised_hls_stream_id = advertised_hls_stream_id(args);
        let listeners = vec![
            ListenerStatus::rist(args),
            ListenerStatus::srt(args),
            ListenerStatus::rtmp(args),
        ];

        let mut alerts = Vec::new();
        if listeners.iter().all(|listener| !listener.enabled) {
            alerts.push(ContribAlert {
                level: "info",
                code: "raw_ingest_only",
                message: "No RIST, SRT, or RTMP listener is enabled; raw HTTP byte ingest remains available.".to_owned(),
                count: 1,
                last_seen_unix_ms: None,
                stream_id_text: None,
                protocol: None,
            });
        }

        Self {
            default_stream_id: args.stream_id.to_string(),
            advertised_hls_stream_id: advertised_hls_stream_id.to_string(),
            advertised_hls_path: format!("/{advertised_hls_stream_id}/stream.m3u8"),
            mesh: MeshTargetStatus {
                byte_fec_target: args.mesh_fec_target.to_string(),
                media_fec_target: args.mesh_media_fec_target.to_string(),
                relay_primary_configured: args.relay_primary_target.is_some(),
                relay_secondary_configured: args.relay_secondary_target.is_some(),
                relay_carrier: args.relay_primary_target.map(|_| "private-udp"),
                relay_trust: args
                    .relay_primary_target
                    .map(|_| "controlled-qualification"),
                relay_primary_id: args
                    .relay_primary_target
                    .map(|_| args.relay_primary_id.clone()),
                relay_primary_target: args.relay_primary_target.map(|target| target.to_string()),
                relay_primary_bind: args.relay_primary_bind.map(|bind| bind.to_string()),
                relay_secondary_id: args
                    .relay_secondary_target
                    .map(|_| args.relay_secondary_id.clone()),
                relay_secondary_target: args
                    .relay_secondary_target
                    .map(|target| target.to_string()),
                relay_secondary_bind: args.relay_secondary_bind.map(|bind| bind.to_string()),
                relay_secondary_source_seeded: args.relay_secondary_seed_source,
                relay_exclusive: args.relay_exclusive,
                relay_topology_generation: args.relay_topology_generation,
                relay_subscription_id: args.relay_subscription_id,
                relay_deadline_ms: args.relay_deadline_ms,
                relay_path_observation_source: if relay_path_metrics_configured(args) {
                    "controller-seeded"
                } else {
                    "default-policy"
                },
                relay_path_loss_fraction: args.relay_path_loss_fraction,
                relay_path_best_direct_rtt_ms: args.relay_path_best_direct_rtt_ms,
                relay_path_rtt_ms: args.relay_path_rtt_ms,
                relay_path_jitter_ms: args.relay_path_jitter_ms,
                relay_path_queue_delay_ms: args.relay_path_queue_delay_ms,
                relay_path_observed_at_unix_ms: args.relay_path_observed_at_unix_ms,
                relay_secondary_path_observation_source: if relay_secondary_path_metrics_configured(
                    args,
                ) {
                    "controller-seeded"
                } else {
                    "primary-policy-fallback"
                },
                relay_secondary_path_loss_fraction: if relay_secondary_path_metrics_configured(args)
                {
                    args.relay_secondary_path_loss_fraction
                } else {
                    args.relay_path_loss_fraction
                },
                relay_secondary_path_best_direct_rtt_ms: if relay_secondary_path_metrics_configured(
                    args,
                ) {
                    args.relay_secondary_path_best_direct_rtt_ms
                } else {
                    args.relay_path_best_direct_rtt_ms
                },
                relay_secondary_path_rtt_ms: if relay_secondary_path_metrics_configured(args) {
                    args.relay_secondary_path_rtt_ms
                } else {
                    args.relay_path_rtt_ms
                },
                relay_secondary_path_jitter_ms: if relay_secondary_path_metrics_configured(args) {
                    args.relay_secondary_path_jitter_ms
                } else {
                    args.relay_path_jitter_ms
                },
                relay_secondary_path_queue_delay_ms: if relay_secondary_path_metrics_configured(
                    args,
                ) {
                    args.relay_secondary_path_queue_delay_ms
                } else {
                    args.relay_path_queue_delay_ms
                },
                relay_secondary_path_observed_at_unix_ms: args
                    .relay_secondary_path_observed_at_unix_ms
                    .or(args.relay_path_observed_at_unix_ms),
                media_object_clock_id: AV_CONTRIB_CLOCK_ID,
                media_object_clock_confidence: "estimated",
                media_object_clock_estimated_error_ms: args.wall_clock_estimated_error_ms,
                media_object_source_epoch: telemetry
                    .media_object_source_epoch
                    .load(Ordering::Acquire),
            },
            hls: HlsStatus {
                part_target_ms: args.fmp4_part_ms,
                segment_target_ms: args.fmp4_segment_ms,
                playlist_target_duration_ms: args.hls_target_duration_ms,
                playlist_count: args.playlist_count,
                playlist_buffer_kb: args.playlist_buffer_kb,
            },
            fec: FecStatus {
                repair_symbols: args.repair_symbols,
                symbol_size: args.symbol_size,
            },
            listeners,
            alerts,
            telemetry,
        }
    }

    fn snapshot(&self) -> ContribStatusSnapshot {
        let runtime = self.telemetry.snapshot();
        let health = derive_contrib_health(&runtime, &self.hls);
        let mut alerts = self.alerts.clone();
        alerts.extend(derive_contrib_alerts(
            &health,
            &runtime,
            &self.advertised_hls_stream_id,
        ));
        alerts.extend(self.telemetry.recent_alerts());
        ContribStatusSnapshot {
            service: "av-contrib",
            status: health.state,
            updated_unix_ms: now_unix_ms(),
            default_stream_id: self.default_stream_id.clone(),
            advertised_hls_stream_id: self.advertised_hls_stream_id.clone(),
            advertised_hls_path: self.advertised_hls_path.clone(),
            mesh: self.mesh.clone(),
            hls: self.hls.clone(),
            fec: self.fec.clone(),
            listeners: self.listeners.clone(),
            runtime,
            alerts,
            health,
            activity: self.telemetry.recent_activity(),
        }
    }

    fn sse_event(&self) -> HandlerResult<Bytes> {
        let json = serde_json::to_vec(&self.snapshot())
            .map_err(|err| ServerError::Handler(Box::new(err)))?;
        let mut event = BytesMut::new();
        event.extend_from_slice(b"event: contrib\n");
        event.extend_from_slice(b"data: ");
        event.extend_from_slice(&json);
        event.extend_from_slice(b"\n\n");
        Ok(event.freeze())
    }

    fn prometheus_metrics(&self) -> Bytes {
        Bytes::from(render_contrib_prometheus_metrics(&self.snapshot()))
    }
}

fn prometheus_label_value(value: &str) -> String {
    value
        .replace('\\', "\\\\")
        .replace('\n', "\\n")
        .replace('"', "\\\"")
}

fn push_prometheus_metric_header(output: &mut String, name: &str, help: &str, metric_type: &str) {
    output.push_str("# HELP ");
    output.push_str(name);
    output.push(' ');
    output.push_str(help);
    output.push('\n');
    output.push_str("# TYPE ");
    output.push_str(name);
    output.push(' ');
    output.push_str(metric_type);
    output.push('\n');
}

fn render_contrib_prometheus_metrics(snapshot: &ContribStatusSnapshot) -> String {
    let runtime = &snapshot.runtime;
    let mut output = String::with_capacity(8 * 1024);
    let audio_hls_worker = audio_epoch_hls_worker_stats();

    for (name, help, metric_type, value) in [
        (
            "av_contrib_audio_epoch_hls_queue_capacity",
            "Configured capacity of the asynchronous lossless AEP1 to LL-HLS handoff.",
            "gauge",
            AUDIO_EPOCH_HLS_QUEUE_CAPACITY.load(Ordering::Relaxed),
        ),
        (
            "av_contrib_audio_epoch_hls_queue_enqueued_total",
            "AEP1 datagrams accepted by the asynchronous LL-HLS handoff.",
            "counter",
            AUDIO_EPOCH_HLS_QUEUE_ENQUEUED.load(Ordering::Relaxed),
        ),
        (
            "av_contrib_audio_epoch_hls_queue_dropped_total",
            "AEP1 datagrams rejected by a full or closed LL-HLS handoff.",
            "counter",
            AUDIO_EPOCH_HLS_QUEUE_DROPPED.load(Ordering::Relaxed),
        ),
        (
            "av_contrib_audio_epoch_hls_queue_max_depth",
            "Maximum observed AEP1 LL-HLS handoff depth since process start.",
            "gauge",
            AUDIO_EPOCH_HLS_QUEUE_MAX_DEPTH.load(Ordering::Relaxed),
        ),
        (
            "av_contrib_audio_epoch_hls_worker_datagrams_total",
            "AEP1 datagrams processed by the asynchronous lossless LL-HLS worker.",
            "counter",
            audio_hls_worker.datagrams,
        ),
        (
            "av_contrib_audio_epoch_hls_groups_completed_total",
            "Lossless AEP1 groups completed for LL-HLS packaging.",
            "counter",
            audio_hls_worker.groups_completed,
        ),
        (
            "av_contrib_audio_epoch_hls_raptorq_fragments_recovered_total",
            "Missing AEP1 source fragments recovered by RaptorQ before LL-HLS packaging.",
            "counter",
            audio_hls_worker.raptorq_fragments_recovered,
        ),
        (
            "av_contrib_audio_epoch_hls_worker_errors_total",
            "AEP1 recovery or LL-HLS packaging errors in the asynchronous worker.",
            "counter",
            audio_hls_worker.errors,
        ),
    ] {
        push_prometheus_metric_header(&mut output, name, help, metric_type);
        output.push_str(&format!("{name} {value}\n"));
    }

    push_prometheus_metric_header(
        &mut output,
        "av_contrib_health",
        "Current contributor service health state.",
        "gauge",
    );
    output.push_str(&format!(
        "av_contrib_health{{state=\"{}\"}} 1\n",
        prometheus_label_value(snapshot.health.state)
    ));
    push_prometheus_metric_header(
        &mut output,
        "av_contrib_llhls_part_target_seconds",
        "Configured LL-HLS part target in seconds.",
        "gauge",
    );
    output.push_str(&format!(
        "av_contrib_llhls_part_target_seconds {}\n",
        f64::from(snapshot.hls.part_target_ms) / 1_000.0
    ));

    push_prometheus_metric_header(
        &mut output,
        "av_contrib_relay_session_carrier_configured",
        "Configured RelaySession live carriers by bounded parent path.",
        "gauge",
    );
    output.push_str(&format!(
        "av_contrib_relay_session_carrier_configured{{path=\"primary\"}} {}\n",
        u8::from(snapshot.mesh.relay_primary_configured)
    ));
    output.push_str(&format!(
        "av_contrib_relay_session_carrier_configured{{path=\"secondary\"}} {}\n",
        u8::from(snapshot.mesh.relay_secondary_configured)
    ));
    push_prometheus_metric_header(
        &mut output,
        "av_contrib_relay_session_deadline_budget_seconds",
        "Configured canonical live-object and RelaySession delivery budget.",
        "gauge",
    );
    output.push_str(&format!(
        "av_contrib_relay_session_deadline_budget_seconds {}\n",
        snapshot.mesh.relay_deadline_ms as f64 / 1_000.0
    ));
    push_prometheus_metric_header(
        &mut output,
        "av_contrib_relay_session_path_observation_info",
        "Origin of the path observation currently driving adaptive RaptorQ policy.",
        "gauge",
    );
    output.push_str(&format!(
        "av_contrib_relay_session_path_observation_info{{source=\"{}\"}} 1\n",
        prometheus_label_value(snapshot.mesh.relay_path_observation_source)
    ));
    for (name, help, value) in [
        (
            "av_contrib_relay_session_path_loss_fraction",
            "Observed selected-source-path loss fraction driving adaptive RaptorQ policy.",
            f64::from(snapshot.mesh.relay_path_loss_fraction),
        ),
        (
            "av_contrib_relay_session_path_best_direct_rtt_seconds",
            "Fastest measured direct origin-to-destination round-trip time used as the path-stretch baseline.",
            f64::from(snapshot.mesh.relay_path_best_direct_rtt_ms) / 1_000.0,
        ),
        (
            "av_contrib_relay_session_path_rtt_seconds",
            "Observed selected-source-path round-trip time driving adaptive RaptorQ policy.",
            f64::from(snapshot.mesh.relay_path_rtt_ms) / 1_000.0,
        ),
        (
            "av_contrib_relay_session_path_jitter_seconds",
            "Observed selected-source-path jitter driving adaptive RaptorQ policy.",
            f64::from(snapshot.mesh.relay_path_jitter_ms) / 1_000.0,
        ),
        (
            "av_contrib_relay_session_path_queue_delay_seconds",
            "Observed selected-source-path queue delay driving adaptive RaptorQ policy.",
            f64::from(snapshot.mesh.relay_path_queue_delay_ms) / 1_000.0,
        ),
    ] {
        push_prometheus_metric_header(&mut output, name, help, "gauge");
        output.push_str(&format!("{name} {value}\n"));
    }
    if snapshot.mesh.relay_path_best_direct_rtt_ms > 0.0 {
        push_prometheus_metric_header(
            &mut output,
            "av_contrib_relay_session_path_stretch_ratio",
            "Selected source-route RTT divided by the fastest measured direct RTT.",
            "gauge",
        );
        output.push_str(&format!(
            "av_contrib_relay_session_path_stretch_ratio {}\n",
            snapshot.mesh.relay_path_rtt_ms / snapshot.mesh.relay_path_best_direct_rtt_ms
        ));
    }
    if let Some(observed_at_unix_ms) = snapshot.mesh.relay_path_observed_at_unix_ms {
        push_prometheus_metric_header(
            &mut output,
            "av_contrib_relay_session_path_observation_age_seconds",
            "Wall-clock age of the controller path observation driving adaptive RaptorQ policy.",
            "gauge",
        );
        output.push_str(&format!(
            "av_contrib_relay_session_path_observation_age_seconds {}\n",
            snapshot.updated_unix_ms.saturating_sub(observed_at_unix_ms) as f64 / 1_000.0
        ));
    }
    for (name, help) in [
        (
            "av_contrib_relay_session_route_observation_info",
            "Controller observation source for each independent relay route.",
        ),
        (
            "av_contrib_relay_session_route_loss_fraction",
            "Observed loss fraction for each independent relay route.",
        ),
        (
            "av_contrib_relay_session_route_rtt_seconds",
            "Observed end-to-end round-trip time for each independent relay route.",
        ),
        (
            "av_contrib_relay_session_route_best_direct_rtt_seconds",
            "Fastest measured direct round-trip time used by each relay route.",
        ),
        (
            "av_contrib_relay_session_route_jitter_seconds",
            "Observed jitter for each independent relay route.",
        ),
        (
            "av_contrib_relay_session_route_queue_delay_seconds",
            "Observed queue delay for each independent relay route.",
        ),
        (
            "av_contrib_relay_session_route_stretch_ratio",
            "Relay-route RTT divided by the fastest measured direct RTT for each path.",
        ),
        (
            "av_contrib_relay_session_route_observation_age_seconds",
            "Wall-clock age of each independent controller route observation.",
        ),
    ] {
        push_prometheus_metric_header(&mut output, name, help, "gauge");
    }
    for (
        path,
        source,
        loss,
        best_direct_rtt_ms,
        rtt_ms,
        jitter_ms,
        queue_delay_ms,
        observed_at_unix_ms,
    ) in [
        (
            "primary",
            snapshot.mesh.relay_path_observation_source,
            snapshot.mesh.relay_path_loss_fraction,
            snapshot.mesh.relay_path_best_direct_rtt_ms,
            snapshot.mesh.relay_path_rtt_ms,
            snapshot.mesh.relay_path_jitter_ms,
            snapshot.mesh.relay_path_queue_delay_ms,
            snapshot.mesh.relay_path_observed_at_unix_ms,
        ),
        (
            "secondary",
            snapshot.mesh.relay_secondary_path_observation_source,
            snapshot.mesh.relay_secondary_path_loss_fraction,
            snapshot.mesh.relay_secondary_path_best_direct_rtt_ms,
            snapshot.mesh.relay_secondary_path_rtt_ms,
            snapshot.mesh.relay_secondary_path_jitter_ms,
            snapshot.mesh.relay_secondary_path_queue_delay_ms,
            snapshot.mesh.relay_secondary_path_observed_at_unix_ms,
        ),
    ] {
        output.push_str(&format!(
            "av_contrib_relay_session_route_observation_info{{path=\"{path}\",source=\"{}\"}} 1\n",
            prometheus_label_value(source)
        ));
        output.push_str(&format!(
            "av_contrib_relay_session_route_loss_fraction{{path=\"{path}\"}} {loss}\n"
        ));
        output.push_str(&format!(
            "av_contrib_relay_session_route_rtt_seconds{{path=\"{path}\"}} {}\n",
            f64::from(rtt_ms) / 1_000.0
        ));
        output.push_str(&format!(
            "av_contrib_relay_session_route_best_direct_rtt_seconds{{path=\"{path}\"}} {}\n",
            f64::from(best_direct_rtt_ms) / 1_000.0
        ));
        output.push_str(&format!(
            "av_contrib_relay_session_route_jitter_seconds{{path=\"{path}\"}} {}\n",
            f64::from(jitter_ms) / 1_000.0
        ));
        output.push_str(&format!(
            "av_contrib_relay_session_route_queue_delay_seconds{{path=\"{path}\"}} {}\n",
            f64::from(queue_delay_ms) / 1_000.0
        ));
        if best_direct_rtt_ms > 0.0 {
            output.push_str(&format!(
                "av_contrib_relay_session_route_stretch_ratio{{path=\"{path}\"}} {}\n",
                rtt_ms / best_direct_rtt_ms
            ));
        }
        if let Some(observed_at_unix_ms) = observed_at_unix_ms {
            output.push_str(&format!(
                "av_contrib_relay_session_route_observation_age_seconds{{path=\"{path}\"}} {}\n",
                snapshot.updated_unix_ms.saturating_sub(observed_at_unix_ms) as f64 / 1_000.0
            ));
        }
    }
    push_prometheus_metric_header(
        &mut output,
        "av_contrib_media_object_source_epoch",
        "Current canonical media-object source incarnation epoch.",
        "gauge",
    );
    output.push_str(&format!(
        "av_contrib_media_object_source_epoch {}\n",
        snapshot.mesh.media_object_source_epoch
    ));
    push_prometheus_metric_header(
        &mut output,
        "av_contrib_media_object_clock_estimated_error_seconds",
        "Configured maximum error estimate for canonical media-object wall-clock timestamps.",
        "gauge",
    );
    output.push_str(&format!(
        "av_contrib_media_object_clock_estimated_error_seconds {}\n",
        snapshot.mesh.media_object_clock_estimated_error_ms as f64 / 1_000.0
    ));
    if let Some(deadline_unix_us) = runtime.relay_session.last_deadline_unix_us {
        push_prometheus_metric_header(
            &mut output,
            "av_contrib_relay_session_last_deadline_seconds",
            "Unix expiry of the latest successfully emitted RelaySession object.",
            "gauge",
        );
        output.push_str(&format!(
            "av_contrib_relay_session_last_deadline_seconds {}\n",
            deadline_unix_us as f64 / 1_000_000.0
        ));
    }
    if let Some(headroom_us) = runtime.relay_session.last_deadline_headroom_us {
        push_prometheus_metric_header(
            &mut output,
            "av_contrib_relay_session_last_deadline_headroom_seconds",
            "Remaining headroom for the latest successfully emitted RelaySession deadline.",
            "gauge",
        );
        output.push_str(&format!(
            "av_contrib_relay_session_last_deadline_headroom_seconds {}\n",
            headroom_us as f64 / 1_000_000.0
        ));
    }

    for (name, help, value) in [
        (
            "av_contrib_raw_http_requests_total",
            "Raw HTTP contributor ingest requests.",
            runtime.raw_http.requests,
        ),
        (
            "av_contrib_raw_http_bytes_total",
            "Raw HTTP contributor ingest payload bytes.",
            runtime.raw_http.bytes,
        ),
        (
            "av_contrib_media_access_units_total",
            "Media access units accepted by contributor ingest.",
            runtime.media_access_units.requests,
        ),
        (
            "av_contrib_media_access_unit_bytes_total",
            "Media access-unit payload bytes accepted by contributor ingest.",
            runtime.media_access_units.payload_bytes,
        ),
        (
            "av_contrib_mpeg_ts_slots_total",
            "MPEG-TS slots accepted from reliable ingest transports.",
            runtime.mpeg_ts.slots,
        ),
        (
            "av_contrib_mpeg_ts_bytes_total",
            "MPEG-TS bytes accepted from reliable ingest transports.",
            runtime.mpeg_ts.bytes,
        ),
        (
            "av_contrib_mpeg_ts_continuity_errors_total",
            "MPEG-TS continuity errors detected at contributor ingest.",
            runtime.mpeg_ts.continuity_errors,
        ),
        (
            "av_contrib_mpeg_ts_dropped_bytes_total",
            "MPEG-TS bytes dropped after continuity damage or oversized payloads.",
            runtime
                .mpeg_ts
                .continuity_dropped_bytes
                .saturating_add(runtime.mpeg_ts.payload_drop_bytes),
        ),
        (
            "av_contrib_rtmp_access_units_total",
            "RTMP access units accepted by contributor ingest.",
            runtime.rtmp.access_units,
        ),
        (
            "av_contrib_rtmp_bytes_total",
            "RTMP access-unit bytes accepted by contributor ingest.",
            runtime.rtmp.bytes,
        ),
        (
            "av_contrib_fmp4_parts_total",
            "CMAF/fMP4 parts published by contributor ingest.",
            runtime.fmp4.parts,
        ),
        (
            "av_contrib_fmp4_bytes_total",
            "CMAF/fMP4 media bytes published by contributor ingest.",
            runtime.fmp4.bytes,
        ),
        (
            "av_contrib_fmp4_publish_errors_total",
            "CMAF/fMP4 publish failures.",
            runtime.fmp4.publish_errors,
        ),
        (
            "av_contrib_hls_responses_total",
            "Contributor LL-HLS responses.",
            runtime.hls.responses_total,
        ),
        (
            "av_contrib_hls_response_errors_total",
            "Contributor LL-HLS non-success responses.",
            runtime.hls.response_errors,
        ),
        (
            "av_contrib_hls_not_found_total",
            "Contributor LL-HLS not-found responses.",
            runtime.hls.response_not_found,
        ),
    ] {
        push_prometheus_metric_header(&mut output, name, help, "counter");
        output.push_str(&format!("{name} {value}\n"));
    }

    push_prometheus_metric_header(
        &mut output,
        "av_contrib_ingest_sessions",
        "Contributor ingest sessions by lifecycle state.",
        "gauge",
    );
    output.push_str(&format!(
        "av_contrib_ingest_sessions{{state=\"active\"}} {}\n",
        runtime.ingest_sessions.active
    ));
    output.push_str(&format!(
        "av_contrib_ingest_sessions{{state=\"ended\"}} {}\n",
        runtime.ingest_sessions.ended
    ));

    for (kind, payloads, payload_bytes, datagrams, datagram_bytes, errors) in [
        (
            "stream",
            runtime.mesh_forward.stream_payloads,
            runtime.mesh_forward.stream_payload_bytes,
            runtime.mesh_forward.stream_datagrams,
            runtime.mesh_forward.stream_datagram_bytes,
            runtime.mesh_forward.stream_errors,
        ),
        (
            "media",
            runtime.mesh_forward.media_payloads,
            runtime.mesh_forward.media_payload_bytes,
            runtime.mesh_forward.media_datagrams,
            runtime.mesh_forward.media_datagram_bytes,
            runtime.mesh_forward.media_errors,
        ),
    ] {
        for (name, help, value) in [
            (
                "av_contrib_mesh_forward_payloads_total",
                "Payloads forwarded from contributor ingest to the mesh.",
                payloads,
            ),
            (
                "av_contrib_mesh_forward_payload_bytes_total",
                "Payload bytes forwarded from contributor ingest to the mesh.",
                payload_bytes,
            ),
            (
                "av_contrib_mesh_forward_datagrams_total",
                "FEC datagrams forwarded from contributor ingest to the mesh.",
                datagrams,
            ),
            (
                "av_contrib_mesh_forward_datagram_bytes_total",
                "FEC datagram bytes forwarded from contributor ingest to the mesh.",
                datagram_bytes,
            ),
            (
                "av_contrib_mesh_forward_errors_total",
                "Contributor-to-mesh forwarding failures.",
                errors,
            ),
        ] {
            if kind == "stream" {
                push_prometheus_metric_header(&mut output, name, help, "counter");
            }
            output.push_str(&format!("{name}{{kind=\"{kind}\"}} {value}\n"));
        }
    }

    for (name, help, value) in [
        (
            "av_contrib_relay_session_objects_total",
            "Canonical media objects emitted through RelaySession.",
            runtime.relay_session.objects_sent,
        ),
        (
            "av_contrib_relay_session_encode_errors_total",
            "Canonical media objects rejected during RelaySession RaptorQ encoding.",
            runtime.relay_session.encode_errors,
        ),
        (
            "av_contrib_relay_session_repair_primary_fallback_objects_total",
            "RelaySession objects whose repair symbols used the primary carrier fallback.",
            runtime.relay_session.repair_primary_fallback_objects,
        ),
    ] {
        push_prometheus_metric_header(&mut output, name, help, "counter");
        output.push_str(&format!("{name} {value}\n"));
    }
    for (role, datagrams, datagram_bytes, errors) in [
        (
            "source",
            runtime.relay_session.source_datagrams,
            runtime.relay_session.source_datagram_bytes,
            runtime.relay_session.source_errors,
        ),
        (
            "repair",
            runtime.relay_session.repair_datagrams,
            runtime.relay_session.repair_datagram_bytes,
            runtime.relay_session.repair_errors,
        ),
    ] {
        for (name, help, value) in [
            (
                "av_contrib_relay_session_datagrams_total",
                "RelaySession datagrams sent by bounded symbol role.",
                datagrams,
            ),
            (
                "av_contrib_relay_session_datagram_bytes_total",
                "RelaySession wire bytes sent by bounded symbol role.",
                datagram_bytes,
            ),
            (
                "av_contrib_relay_session_send_errors_total",
                "RelaySession send failures by bounded symbol role.",
                errors,
            ),
        ] {
            if role == "source" {
                push_prometheus_metric_header(&mut output, name, help, "counter");
            }
            output.push_str(&format!("{name}{{role=\"{role}\"}} {value}\n"));
        }
    }

    push_prometheus_metric_header(
        &mut output,
        "av_contrib_relay_session_lane_objects_total",
        "Canonical RelaySession objects by carrier lane and send outcome.",
        "counter",
    );
    for (path, succeeded, failed) in [
        (
            "primary",
            runtime.relay_session.primary_lane_objects_succeeded,
            runtime.relay_session.primary_lane_objects_failed,
        ),
        (
            "secondary",
            runtime.relay_session.secondary_lane_objects_succeeded,
            runtime.relay_session.secondary_lane_objects_failed,
        ),
    ] {
        output.push_str(&format!(
            "av_contrib_relay_session_lane_objects_total{{path=\"{path}\",outcome=\"success\"}} {succeeded}\n"
        ));
        output.push_str(&format!(
            "av_contrib_relay_session_lane_objects_total{{path=\"{path}\",outcome=\"failure\"}} {failed}\n"
        ));
    }
    push_prometheus_metric_header(
        &mut output,
        "av_contrib_relay_session_lane_health",
        "Current RelaySession carrier-lane health as a one-hot gauge.",
        "gauge",
    );
    for (path, current_state) in [
        ("primary", runtime.relay_session.primary_lane_state),
        ("secondary", runtime.relay_session.secondary_lane_state),
    ] {
        for state in ["unknown", "healthy", "impaired"] {
            output.push_str(&format!(
                "av_contrib_relay_session_lane_health{{path=\"{path}\",state=\"{state}\"}} {}\n",
                u8::from(current_state == state)
            ));
        }
    }
    for (name, help, value) in [
        (
            "av_contrib_relay_session_surviving_lane_objects_total",
            "Canonical objects emitted before deadline while another configured lane failed.",
            runtime.relay_session.surviving_lane_objects,
        ),
        (
            "av_contrib_relay_session_all_lanes_failed_objects_total",
            "Canonical objects for which every complete configured RelaySession lane failed.",
            runtime.relay_session.all_lanes_failed_objects,
        ),
    ] {
        push_prometheus_metric_header(&mut output, name, help, "counter");
        output.push_str(&format!("{name} {value}\n"));
    }

    push_prometheus_metric_header(
        &mut output,
        "av_contrib_relay_session_deadline_objects_total",
        "Canonical RelaySession objects by sender deadline outcome.",
        "counter",
    );
    output.push_str(&format!(
        "av_contrib_relay_session_deadline_objects_total{{outcome=\"hit\"}} {}\n",
        runtime.relay_session.deadline_hits
    ));
    output.push_str(&format!(
        "av_contrib_relay_session_deadline_objects_total{{outcome=\"miss\"}} {}\n",
        runtime.relay_session.deadline_misses
    ));
    push_prometheus_metric_header(
        &mut output,
        "av_contrib_relay_session_expired_objects_total",
        "Canonical objects that expired before the full RelaySession emission completed.",
        "counter",
    );
    output.push_str(&format!(
        "av_contrib_relay_session_expired_objects_total {}\n",
        runtime.relay_session.expired_objects
    ));
    push_prometheus_metric_header(
        &mut output,
        "av_contrib_relay_session_expired_symbols_total",
        "RaptorQ symbols dropped at the contributor after their media deadline.",
        "counter",
    );
    output.push_str(&format!(
        "av_contrib_relay_session_expired_symbols_total {}\n",
        runtime.relay_session.expired_symbols
    ));

    push_prometheus_metric_header(
        &mut output,
        "av_contrib_relay_session_stage_duration_seconds",
        "RelaySession contribution latency by bounded RaptorQ and carrier stage.",
        "histogram",
    );
    for (stage, histogram) in [
        ("total", &runtime.relay_session.stages.total),
        ("encode_wait", &runtime.relay_session.stages.encode_wait),
        ("encode", &runtime.relay_session.stages.encode),
        ("schedule", &runtime.relay_session.stages.schedule),
        (
            "send_primary_source",
            &runtime.relay_session.stages.primary_source_send,
        ),
        (
            "send_secondary_source",
            &runtime.relay_session.stages.secondary_source_send,
        ),
        (
            "send_primary_repair",
            &runtime.relay_session.stages.primary_repair_send,
        ),
        (
            "send_secondary_repair",
            &runtime.relay_session.stages.secondary_repair_send,
        ),
    ] {
        for (upper_bound_us, count) in DURATION_HISTOGRAM_BUCKETS_US
            .iter()
            .zip(histogram.buckets.iter())
        {
            output.push_str(&format!(
                "av_contrib_relay_session_stage_duration_seconds_bucket{{stage=\"{stage}\",le=\"{}\"}} {count}\n",
                *upper_bound_us as f64 / 1_000_000.0
            ));
        }
        output.push_str(&format!(
            "av_contrib_relay_session_stage_duration_seconds_bucket{{stage=\"{stage}\",le=\"+Inf\"}} {}\n",
            histogram.count
        ));
        output.push_str(&format!(
            "av_contrib_relay_session_stage_duration_seconds_sum{{stage=\"{stage}\"}} {}\n",
            histogram.sum_us as f64 / 1_000_000.0
        ));
        output.push_str(&format!(
            "av_contrib_relay_session_stage_duration_seconds_count{{stage=\"{stage}\"}} {}\n",
            histogram.count
        ));
    }

    push_prometheus_metric_header(
        &mut output,
        "av_contrib_mesh_forward_duration_seconds",
        "Time spent encoding and sending one contributor payload to the mesh.",
        "histogram",
    );
    for (kind, histogram) in [
        ("stream", &runtime.mesh_forward.stream_duration),
        ("media", &runtime.mesh_forward.media_duration),
    ] {
        for (upper_bound_us, count) in DURATION_HISTOGRAM_BUCKETS_US
            .iter()
            .zip(histogram.buckets.iter())
        {
            output.push_str(&format!(
                "av_contrib_mesh_forward_duration_seconds_bucket{{kind=\"{kind}\",le=\"{}\"}} {count}\n",
                *upper_bound_us as f64 / 1_000_000.0
            ));
        }
        output.push_str(&format!(
            "av_contrib_mesh_forward_duration_seconds_bucket{{kind=\"{kind}\",le=\"+Inf\"}} {}\n",
            histogram.count
        ));
        output.push_str(&format!(
            "av_contrib_mesh_forward_duration_seconds_sum{{kind=\"{kind}\"}} {}\n",
            histogram.sum_us as f64 / 1_000_000.0
        ));
        output.push_str(&format!(
            "av_contrib_mesh_forward_duration_seconds_count{{kind=\"{kind}\"}} {}\n",
            histogram.count
        ));
    }

    push_prometheus_metric_header(
        &mut output,
        "av_contrib_mesh_forward_stage_duration_seconds",
        "Contributor-to-mesh forwarding time by bounded hot-path stage.",
        "histogram",
    );
    for (kind, stages) in [
        ("stream", &runtime.mesh_forward.stream_stages),
        ("media", &runtime.mesh_forward.media_stages),
    ] {
        for (stage, histogram) in [
            ("encode_wait", &stages.encode_wait),
            ("encode", &stages.encode),
            ("send", &stages.send),
            ("telemetry", &stages.telemetry),
        ] {
            for (upper_bound_us, count) in DURATION_HISTOGRAM_BUCKETS_US
                .iter()
                .zip(histogram.buckets.iter())
            {
                output.push_str(&format!(
                    "av_contrib_mesh_forward_stage_duration_seconds_bucket{{kind=\"{kind}\",stage=\"{stage}\",le=\"{}\"}} {count}\n",
                    *upper_bound_us as f64 / 1_000_000.0
                ));
            }
            output.push_str(&format!(
                "av_contrib_mesh_forward_stage_duration_seconds_bucket{{kind=\"{kind}\",stage=\"{stage}\",le=\"+Inf\"}} {}\n",
                histogram.count
            ));
            output.push_str(&format!(
                "av_contrib_mesh_forward_stage_duration_seconds_sum{{kind=\"{kind}\",stage=\"{stage}\"}} {}\n",
                histogram.sum_us as f64 / 1_000_000.0
            ));
            output.push_str(&format!(
                "av_contrib_mesh_forward_stage_duration_seconds_count{{kind=\"{kind}\",stage=\"{stage}\"}} {}\n",
                histogram.count
            ));
        }
    }

    push_prometheus_metric_header(
        &mut output,
        "av_contrib_last_seen_age_seconds",
        "Age of the most recent contributor pipeline event by stage.",
        "gauge",
    );
    for (stage, age_ms) in [
        ("input", snapshot.health.last_input_age_ms),
        ("fmp4_input", snapshot.health.last_fmp4_input_age_ms),
        ("fmp4_output", snapshot.health.last_output_age_ms),
        ("mesh_stream", runtime.mesh_forward.stream_last_age_ms),
        ("mesh_media", runtime.mesh_forward.media_last_age_ms),
    ] {
        if let Some(age_ms) = age_ms {
            output.push_str(&format!(
                "av_contrib_last_seen_age_seconds{{stage=\"{stage}\"}} {}\n",
                age_ms as f64 / 1_000.0
            ));
        }
    }

    for (name, help, metric_type) in [
        (
            "av_contrib_stream_input_bytes_total",
            "Contributor input bytes by stream.",
            "counter",
        ),
        (
            "av_contrib_stream_mesh_bytes_total",
            "Contributor-to-mesh payload bytes by stream.",
            "counter",
        ),
        (
            "av_contrib_stream_mesh_errors_total",
            "Contributor-to-mesh forwarding errors by stream.",
            "counter",
        ),
        (
            "av_contrib_stream_fmp4_parts_total",
            "CMAF/fMP4 parts published by stream.",
            "counter",
        ),
        (
            "av_contrib_stream_latest_fmp4_sequence",
            "Latest CMAF/fMP4 part sequence published by stream.",
            "gauge",
        ),
        (
            "av_contrib_stream_last_seen_age_seconds",
            "Age of the most recent event by stream and stage.",
            "gauge",
        ),
    ] {
        push_prometheus_metric_header(&mut output, name, help, metric_type);
    }
    for stream in &runtime.streams {
        let stream_id = prometheus_label_value(&stream.stream_id_text);
        let state = prometheus_label_value(stream.state);
        let labels = format!("stream_id=\"{stream_id}\",state=\"{state}\"");
        output.push_str(&format!(
            "av_contrib_stream_input_bytes_total{{{labels}}} {}\n",
            stream.input_bytes
        ));
        output.push_str(&format!(
            "av_contrib_stream_mesh_bytes_total{{{labels}}} {}\n",
            stream.mesh_payload_bytes
        ));
        output.push_str(&format!(
            "av_contrib_stream_mesh_errors_total{{{labels}}} {}\n",
            stream.mesh_errors
        ));
        output.push_str(&format!(
            "av_contrib_stream_fmp4_parts_total{{{labels}}} {}\n",
            stream.fmp4_parts
        ));
        if let Some(sequence) = stream.latest_fmp4_sequence {
            output.push_str(&format!(
                "av_contrib_stream_latest_fmp4_sequence{{{labels}}} {sequence}\n"
            ));
        }
        for (stage, age_ms) in [
            ("input", stream.last_input_age_ms),
            ("mesh_forward", stream.last_mesh_forward_age_ms),
            ("fmp4", stream.last_fmp4_age_ms),
        ] {
            if let Some(age_ms) = age_ms {
                output.push_str(&format!(
                    "av_contrib_stream_last_seen_age_seconds{{{labels},stage=\"{stage}\"}} {}\n",
                    age_ms as f64 / 1_000.0
                ));
            }
        }
    }

    for (name, help, metric_type) in [
        (
            "av_contrib_protocol_units_total",
            "Ingest units received by contributor protocol.",
            "counter",
        ),
        (
            "av_contrib_protocol_bytes_total",
            "Ingest bytes received by contributor protocol.",
            "counter",
        ),
        (
            "av_contrib_protocol_sessions",
            "Ingest sessions by contributor protocol and state.",
            "gauge",
        ),
    ] {
        push_prometheus_metric_header(&mut output, name, help, metric_type);
    }
    for protocol in &runtime.protocols {
        let protocol_name = prometheus_label_value(protocol.protocol);
        output.push_str(&format!(
            "av_contrib_protocol_units_total{{protocol=\"{protocol_name}\"}} {}\n",
            protocol.units
        ));
        output.push_str(&format!(
            "av_contrib_protocol_bytes_total{{protocol=\"{protocol_name}\"}} {}\n",
            protocol.bytes
        ));
        output.push_str(&format!(
            "av_contrib_protocol_sessions{{protocol=\"{protocol_name}\",state=\"active\"}} {}\n",
            protocol.active_sessions
        ));
        output.push_str(&format!(
            "av_contrib_protocol_sessions{{protocol=\"{protocol_name}\",state=\"ended\"}} {}\n",
            protocol.ended_sessions
        ));
    }

    output
}

#[derive(Debug, Clone, Serialize)]
struct ContribStatusSnapshot {
    service: &'static str,
    status: &'static str,
    updated_unix_ms: u64,
    default_stream_id: String,
    advertised_hls_stream_id: String,
    advertised_hls_path: String,
    mesh: MeshTargetStatus,
    hls: HlsStatus,
    fec: FecStatus,
    listeners: Vec<ListenerStatus>,
    runtime: IngestRuntimeSnapshot,
    alerts: Vec<ContribAlert>,
    health: ContribHealthStatus,
    activity: Vec<ContribActivity>,
}

#[derive(Debug, Clone, Serialize)]
struct ContribHealthStatus {
    state: &'static str,
    stale_threshold_ms: u64,
    input_seen: bool,
    fmp4_input_seen: bool,
    output_seen: bool,
    last_input_age_ms: Option<u64>,
    last_fmp4_input_age_ms: Option<u64>,
    last_output_age_ms: Option<u64>,
}

fn derive_contrib_health(runtime: &IngestRuntimeSnapshot, hls: &HlsStatus) -> ContribHealthStatus {
    let stale_threshold_ms = u64::from(hls.segment_target_ms)
        .saturating_mul(3)
        .max(CONTRIB_MIN_STALE_OUTPUT_MS);
    let input_seen = runtime.raw_http.requests > 0
        || runtime.media_access_units.requests > 0
        || runtime.mpeg_ts.slots > 0
        || runtime.rtmp.access_units > 0;
    let fmp4_input_seen = runtime.mpeg_ts.slots > 0 || runtime.rtmp.access_units > 0;
    let output_seen = runtime.fmp4.parts > 0;
    let last_input_age_ms = youngest_age([
        runtime.raw_http.last_seen_age_ms,
        runtime.media_access_units.last_seen_age_ms,
        runtime.mpeg_ts.last_seen_age_ms,
        runtime.rtmp.last_seen_age_ms,
    ]);
    let last_fmp4_input_age_ms = youngest_age([
        runtime.mpeg_ts.last_seen_age_ms,
        runtime.rtmp.last_seen_age_ms,
    ]);
    let output_is_stale = runtime
        .fmp4
        .last_publish_age_ms
        .is_some_and(|age_ms| age_ms > stale_threshold_ms);
    let fmp4_input_is_stale =
        last_fmp4_input_age_ms.is_some_and(|age_ms| age_ms > stale_threshold_ms);
    let mesh_forward_errors =
        runtime.mesh_forward.stream_errors + runtime.mesh_forward.media_errors;
    let relay_lane_impaired = runtime.relay_session.impaired_lane_count() > 0;
    let state = if runtime.fmp4.publish_errors > 0 || mesh_forward_errors > 0 || relay_lane_impaired
    {
        "degraded"
    } else if output_seen && output_is_stale {
        "stale"
    } else if fmp4_input_seen && !output_seen && fmp4_input_is_stale {
        "stalled"
    } else if output_seen {
        "active"
    } else if input_seen {
        "ingesting"
    } else {
        "waiting"
    };

    ContribHealthStatus {
        state,
        stale_threshold_ms,
        input_seen,
        fmp4_input_seen,
        output_seen,
        last_input_age_ms,
        last_fmp4_input_age_ms,
        last_output_age_ms: runtime.fmp4.last_publish_age_ms,
    }
}

fn derive_contrib_alerts(
    health: &ContribHealthStatus,
    runtime: &IngestRuntimeSnapshot,
    advertised_hls_stream_id: &str,
) -> Vec<ContribAlert> {
    let now = now_unix_ms();
    let mut alerts = Vec::new();

    if !health.input_seen {
        alerts.push(ContribAlert {
            level: "info",
            code: "waiting_for_input",
            message: "No contributor input has been observed yet.".to_owned(),
            count: 1,
            last_seen_unix_ms: Some(now),
            stream_id_text: Some(advertised_hls_stream_id.to_owned()),
            protocol: None,
        });
    }

    if health.state == "stale" {
        alerts.push(ContribAlert {
            level: "warn",
            code: "fmp4_output_stale",
            message: format!(
                "fMP4 output has not published for more than {} ms.",
                health.stale_threshold_ms
            ),
            count: 1,
            last_seen_unix_ms: health
                .last_output_age_ms
                .and_then(|age_ms| now.checked_sub(age_ms))
                .or(Some(now)),
            stream_id_text: Some(advertised_hls_stream_id.to_owned()),
            protocol: None,
        });
    }

    if health.state == "stalled" {
        alerts.push(ContribAlert {
            level: "warn",
            code: "fmp4_input_without_output",
            message: format!(
                "MPEG-TS/RTMP input was observed, but no fMP4 output published within {} ms.",
                health.stale_threshold_ms
            ),
            count: 1,
            last_seen_unix_ms: health
                .last_fmp4_input_age_ms
                .and_then(|age_ms| now.checked_sub(age_ms))
                .or(Some(now)),
            stream_id_text: Some(advertised_hls_stream_id.to_owned()),
            protocol: None,
        });
    }

    let impaired_lanes = [
        ("primary", runtime.relay_session.primary_lane_state),
        ("secondary", runtime.relay_session.secondary_lane_state),
    ]
    .into_iter()
    .filter_map(|(path, state)| (state == "impaired").then_some(path))
    .collect::<Vec<_>>();
    if !impaired_lanes.is_empty() {
        alerts.push(ContribAlert {
            level: "warn",
            code: "relay_lane_impaired",
            message: format!(
                "Relay parent lane currently impaired: {}. Delivery continues through each healthy complete lane.",
                impaired_lanes.join(", ")
            ),
            count: impaired_lanes.len() as u64,
            last_seen_unix_ms: Some(now),
            stream_id_text: Some(advertised_hls_stream_id.to_owned()),
            protocol: Some("relay-session"),
        });
    }

    if runtime.hls.response_errors > 0 {
        let latest_error = runtime
            .hls
            .recent_responses
            .iter()
            .find(|response| response.status >= 400);
        let (status, path, last_seen) = latest_error
            .map(|response| {
                (
                    response.status,
                    response.path.clone(),
                    Some(response.unix_ms),
                )
            })
            .unwrap_or((0, "unknown HLS path".to_owned(), Some(now)));
        alerts.push(ContribAlert {
            level: if status >= 500 { "error" } else { "warn" },
            code: "hls_response_errors",
            message: format!(
                "Contributor LL-HLS has returned {} non-success response(s); latest was HTTP {status} for {path}.",
                runtime.hls.response_errors
            ),
            count: runtime.hls.response_errors,
            last_seen_unix_ms: last_seen,
            stream_id_text: Some(advertised_hls_stream_id.to_owned()),
            protocol: None,
        });
    }

    let mpeg_ts_errors = runtime
        .mpeg_ts
        .continuity_errors
        .saturating_add(runtime.mpeg_ts.payload_drops);
    if mpeg_ts_errors > 0 {
        alerts.push(ContribAlert {
            level: "warn",
            code: "mpeg_ts_input_damage",
            message: format!(
                "MPEG-TS input reported {} continuity error(s) and {} oversized payload drop(s), dropping {} bytes total.",
                runtime.mpeg_ts.continuity_errors,
                runtime.mpeg_ts.payload_drops,
                runtime
                    .mpeg_ts
                    .continuity_dropped_bytes
                    .saturating_add(runtime.mpeg_ts.payload_drop_bytes)
            ),
            count: mpeg_ts_errors,
            last_seen_unix_ms: runtime.mpeg_ts.last_error_unix_ms,
            stream_id_text: None,
            protocol: Some("mpeg-ts"),
        });
    }

    alerts
}

#[derive(Debug, Clone, Serialize)]
struct IngestRuntimeSnapshot {
    raw_http: RawHttpRuntimeSnapshot,
    media_access_units: MediaRuntimeSnapshot,
    mesh_forward: MeshForwardRuntimeSnapshot,
    relay_session: RelaySessionRuntimeSnapshot,
    mpeg_ts: MpegTsRuntimeSnapshot,
    rtmp: RtmpRuntimeSnapshot,
    fmp4: Fmp4RuntimeSnapshot,
    hls: HlsRuntimeSnapshot,
    ingest_sessions: IngestSessionsRuntimeSnapshot,
    streams: Vec<ContribStreamRuntimeSnapshot>,
    protocols: Vec<ProtocolRuntimeSnapshot>,
}

#[derive(Debug, Clone, Serialize)]
struct RawHttpRuntimeSnapshot {
    requests: u64,
    chunks: u64,
    bytes: u64,
    datagrams: u64,
    last_seen_unix_ms: Option<u64>,
    last_seen_age_ms: Option<u64>,
}

#[derive(Debug, Clone, Serialize)]
struct MediaRuntimeSnapshot {
    requests: u64,
    payload_bytes: u64,
    datagrams: u64,
    last_seen_unix_ms: Option<u64>,
    last_seen_age_ms: Option<u64>,
}

#[derive(Debug, Clone, Serialize)]
struct MeshForwardRuntimeSnapshot {
    stream_payloads: u64,
    stream_payload_bytes: u64,
    stream_datagrams: u64,
    stream_datagram_bytes: u64,
    stream_errors: u64,
    stream_last_unix_ms: Option<u64>,
    stream_last_age_ms: Option<u64>,
    stream_duration: DurationHistogramSnapshot,
    stream_stages: MeshForwardStageRuntimeSnapshot,
    media_payloads: u64,
    media_payload_bytes: u64,
    media_datagrams: u64,
    media_datagram_bytes: u64,
    media_errors: u64,
    media_last_unix_ms: Option<u64>,
    media_last_age_ms: Option<u64>,
    media_duration: DurationHistogramSnapshot,
    media_stages: MeshForwardStageRuntimeSnapshot,
}

#[derive(Debug, Clone, Serialize)]
struct MeshForwardStageRuntimeSnapshot {
    encode_wait: DurationHistogramSnapshot,
    encode: DurationHistogramSnapshot,
    send: DurationHistogramSnapshot,
    telemetry: DurationHistogramSnapshot,
}

#[derive(Debug, Clone, Serialize)]
struct RelaySessionRuntimeSnapshot {
    objects_sent: u64,
    encode_errors: u64,
    source_datagrams: u64,
    source_datagram_bytes: u64,
    source_errors: u64,
    repair_datagrams: u64,
    repair_datagram_bytes: u64,
    repair_errors: u64,
    repair_primary_fallback_objects: u64,
    primary_lane_objects_succeeded: u64,
    primary_lane_objects_failed: u64,
    primary_lane_state: &'static str,
    secondary_lane_objects_succeeded: u64,
    secondary_lane_objects_failed: u64,
    secondary_lane_state: &'static str,
    surviving_lane_objects: u64,
    all_lanes_failed_objects: u64,
    expired_objects: u64,
    expired_symbols: u64,
    deadline_hits: u64,
    deadline_misses: u64,
    last_deadline_unix_us: Option<u64>,
    last_deadline_headroom_us: Option<u64>,
    stages: RelaySessionStageRuntimeSnapshot,
}

impl RelaySessionRuntimeSnapshot {
    fn impaired_lane_count(&self) -> u64 {
        u64::from(self.primary_lane_state == "impaired")
            + u64::from(self.secondary_lane_state == "impaired")
    }
}

#[derive(Debug, Clone, Serialize)]
struct RelaySessionStageRuntimeSnapshot {
    total: DurationHistogramSnapshot,
    encode_wait: DurationHistogramSnapshot,
    encode: DurationHistogramSnapshot,
    schedule: DurationHistogramSnapshot,
    primary_source_send: DurationHistogramSnapshot,
    secondary_source_send: DurationHistogramSnapshot,
    primary_repair_send: DurationHistogramSnapshot,
    secondary_repair_send: DurationHistogramSnapshot,
}

#[derive(Debug, Clone, Serialize)]
struct DurationHistogramSnapshot {
    count: u64,
    sum_us: u64,
    p95_us: Option<u64>,
    buckets: Vec<u64>,
}

#[derive(Debug, Clone, Serialize)]
struct MpegTsRuntimeSnapshot {
    slots: u64,
    bytes: u64,
    last_seen_unix_ms: Option<u64>,
    last_seen_age_ms: Option<u64>,
    continuity_errors: u64,
    continuity_dropped_bytes: u64,
    payload_drops: u64,
    payload_drop_bytes: u64,
    last_error_unix_ms: Option<u64>,
    last_error_age_ms: Option<u64>,
}

#[derive(Debug, Clone, Serialize)]
struct RtmpRuntimeSnapshot {
    access_units: u64,
    bytes: u64,
    last_seen_unix_ms: Option<u64>,
    last_seen_age_ms: Option<u64>,
}

#[derive(Debug, Clone, Serialize)]
struct Fmp4RuntimeSnapshot {
    parts: u64,
    bytes: u64,
    init_bytes: u64,
    publish_errors: u64,
    last_publish_unix_ms: Option<u64>,
    last_publish_age_ms: Option<u64>,
    video_codec: Option<&'static str>,
    video_width: Option<u16>,
    video_height: Option<u16>,
    video_parts: u64,
    video_access_units: u64,
    audio_codec: Option<&'static str>,
    audio_parts: u64,
    audio_access_units: u64,
}

#[derive(Debug, Clone, Serialize)]
struct HlsRuntimeSnapshot {
    responses_total: u64,
    response_errors: u64,
    response_not_found: u64,
    last_response_unix_ms: Option<u64>,
    last_response_age_ms: Option<u64>,
    recent_responses: Vec<ContribHlsResponse>,
}

#[derive(Debug, Clone, Serialize)]
struct ContribHlsResponse {
    unix_ms: u64,
    method: String,
    path: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    query: Option<String>,
    status: u16,
    bytes: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    content_type: Option<String>,
}

#[derive(Debug, Clone)]
struct IngestSessionRecord {
    session_id: u64,
    protocol: &'static str,
    stream_id_text: String,
    output_stream_id_text: Option<String>,
    output_stream_idx: Option<usize>,
    peer: Option<String>,
    path: Option<String>,
    state: &'static str,
    started_unix_ms: u64,
    last_seen_unix_ms: u64,
    ended_unix_ms: Option<u64>,
    body_slots: u64,
    bytes: u64,
    access_units: u64,
    end_reason: Option<&'static str>,
}

#[derive(Debug, Clone, Serialize)]
struct IngestSessionsRuntimeSnapshot {
    active: usize,
    started: u64,
    ended: u64,
    recent: Vec<IngestSessionSnapshot>,
}

#[derive(Debug, Clone, Serialize)]
struct ContribStreamRuntimeSnapshot {
    stream_id_text: String,
    state: &'static str,
    input_units: u64,
    input_bytes: u64,
    mesh_payloads: u64,
    mesh_payload_bytes: u64,
    mesh_datagrams: u64,
    mesh_datagram_bytes: u64,
    mesh_errors: u64,
    fmp4_parts: u64,
    fmp4_bytes: u64,
    fmp4_init_bytes: u64,
    fmp4_publish_errors: u64,
    latest_fmp4_sequence: Option<u64>,
    video_codec: Option<&'static str>,
    video_width: Option<u16>,
    video_height: Option<u16>,
    video_parts: u64,
    video_access_units: u64,
    audio_codec: Option<&'static str>,
    audio_parts: u64,
    audio_access_units: u64,
    last_input_unix_ms: Option<u64>,
    last_input_age_ms: Option<u64>,
    last_mesh_forward_unix_ms: Option<u64>,
    last_mesh_forward_age_ms: Option<u64>,
    last_fmp4_unix_ms: Option<u64>,
    last_fmp4_age_ms: Option<u64>,
}

impl ContribStreamRuntimeSnapshot {
    fn from_record(record: ContribStreamRuntimeRecord, now_ms: u64) -> Self {
        let state = if record.mesh_errors > 0 || record.fmp4_publish_errors > 0 {
            "degraded"
        } else if record.fmp4_parts > 0 {
            "publishing"
        } else if record.mesh_payloads > 0 {
            "forwarding"
        } else if record.input_units > 0 {
            "ingesting"
        } else {
            "waiting"
        };
        Self {
            stream_id_text: record.stream_id_text,
            state,
            input_units: record.input_units,
            input_bytes: record.input_bytes,
            mesh_payloads: record.mesh_payloads,
            mesh_payload_bytes: record.mesh_payload_bytes,
            mesh_datagrams: record.mesh_datagrams,
            mesh_datagram_bytes: record.mesh_datagram_bytes,
            mesh_errors: record.mesh_errors,
            fmp4_parts: record.fmp4_parts,
            fmp4_bytes: record.fmp4_bytes,
            fmp4_init_bytes: record.fmp4_init_bytes,
            fmp4_publish_errors: record.fmp4_publish_errors,
            latest_fmp4_sequence: record.latest_fmp4_sequence,
            video_codec: record.video_codec,
            video_width: record.video_width,
            video_height: record.video_height,
            video_parts: record.video_parts,
            video_access_units: record.video_access_units,
            audio_codec: record.audio_codec,
            audio_parts: record.audio_parts,
            audio_access_units: record.audio_access_units,
            last_input_age_ms: record
                .last_input_unix_ms
                .map(|seen| now_ms.saturating_sub(seen)),
            last_input_unix_ms: record.last_input_unix_ms,
            last_mesh_forward_age_ms: record
                .last_mesh_forward_unix_ms
                .map(|seen| now_ms.saturating_sub(seen)),
            last_mesh_forward_unix_ms: record.last_mesh_forward_unix_ms,
            last_fmp4_age_ms: record
                .last_fmp4_unix_ms
                .map(|seen| now_ms.saturating_sub(seen)),
            last_fmp4_unix_ms: record.last_fmp4_unix_ms,
        }
    }

    fn last_seen_unix_ms(&self) -> Option<u64> {
        [
            self.last_input_unix_ms,
            self.last_mesh_forward_unix_ms,
            self.last_fmp4_unix_ms,
        ]
        .into_iter()
        .flatten()
        .max()
    }
}

#[derive(Debug, Clone, Serialize)]
struct ProtocolRuntimeSnapshot {
    protocol: &'static str,
    units: u64,
    bytes: u64,
    active_sessions: usize,
    ended_sessions: usize,
    last_seen_unix_ms: Option<u64>,
    last_seen_age_ms: Option<u64>,
}

#[derive(Debug, Clone, Serialize)]
struct IngestSessionSnapshot {
    session_id: u64,
    protocol: &'static str,
    stream_id_text: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    output_stream_id_text: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    output_stream_idx: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    peer: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    path: Option<String>,
    state: &'static str,
    started_unix_ms: u64,
    last_seen_unix_ms: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    ended_unix_ms: Option<u64>,
    age_ms: u64,
    body_slots: u64,
    bytes: u64,
    access_units: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    end_reason: Option<&'static str>,
}

impl IngestSessionSnapshot {
    fn from_record(record: IngestSessionRecord, now_ms: u64) -> Self {
        Self {
            session_id: record.session_id,
            protocol: record.protocol,
            stream_id_text: record.stream_id_text,
            output_stream_id_text: record.output_stream_id_text,
            output_stream_idx: record.output_stream_idx,
            peer: record.peer,
            path: record.path,
            state: record.state,
            started_unix_ms: record.started_unix_ms,
            last_seen_unix_ms: record.last_seen_unix_ms,
            ended_unix_ms: record.ended_unix_ms,
            age_ms: now_ms.saturating_sub(record.last_seen_unix_ms),
            body_slots: record.body_slots,
            bytes: record.bytes,
            access_units: record.access_units,
            end_reason: record.end_reason,
        }
    }
}

#[derive(Debug, Clone, Serialize)]
struct MeshTargetStatus {
    byte_fec_target: String,
    media_fec_target: String,
    relay_primary_configured: bool,
    relay_secondary_configured: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    relay_carrier: Option<&'static str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    relay_trust: Option<&'static str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    relay_primary_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    relay_primary_target: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    relay_primary_bind: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    relay_secondary_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    relay_secondary_target: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    relay_secondary_bind: Option<String>,
    relay_secondary_source_seeded: bool,
    relay_exclusive: bool,
    relay_topology_generation: u64,
    relay_subscription_id: u64,
    relay_deadline_ms: u64,
    relay_path_observation_source: &'static str,
    relay_path_loss_fraction: f32,
    relay_path_best_direct_rtt_ms: f32,
    relay_path_rtt_ms: f32,
    relay_path_jitter_ms: f32,
    relay_path_queue_delay_ms: f32,
    relay_path_observed_at_unix_ms: Option<u64>,
    relay_secondary_path_observation_source: &'static str,
    relay_secondary_path_loss_fraction: f32,
    relay_secondary_path_best_direct_rtt_ms: f32,
    relay_secondary_path_rtt_ms: f32,
    relay_secondary_path_jitter_ms: f32,
    relay_secondary_path_queue_delay_ms: f32,
    relay_secondary_path_observed_at_unix_ms: Option<u64>,
    media_object_clock_id: &'static str,
    media_object_clock_confidence: &'static str,
    media_object_clock_estimated_error_ms: u64,
    media_object_source_epoch: u64,
}

#[derive(Debug, Clone, Serialize)]
struct HlsStatus {
    part_target_ms: u32,
    segment_target_ms: u32,
    playlist_target_duration_ms: u32,
    playlist_count: usize,
    playlist_buffer_kb: usize,
}

#[derive(Debug, Clone, Serialize)]
struct FecStatus {
    repair_symbols: u32,
    symbol_size: u16,
}

#[derive(Debug, Clone, Serialize)]
struct ListenerStatus {
    protocol: &'static str,
    enabled: bool,
    bind: Option<String>,
    output_stream_id: String,
    output_hls_path: String,
    backend: Option<&'static str>,
    profile: Option<&'static str>,
    flow_id: Option<String>,
}

impl ListenerStatus {
    fn rist(args: &Args) -> Self {
        Self {
            protocol: "rist",
            enabled: args.rist_bind.is_some(),
            bind: args.rist_bind.map(|bind| bind.to_string()),
            output_stream_id: args.rist_stream_id.to_string(),
            output_hls_path: format!("/{}/stream.m3u8", args.rist_stream_id),
            backend: Some(args.rist_backend.as_str()),
            profile: Some(args.rist_profile.as_str()),
            flow_id: Some(format!("0x{:08x}", args.rist_flow_id)),
        }
    }

    fn srt(args: &Args) -> Self {
        Self {
            protocol: "srt",
            enabled: args.srt_bind.is_some(),
            bind: args.srt_bind.map(|bind| bind.to_string()),
            output_stream_id: args.srt_stream_id.to_string(),
            output_hls_path: format!("/{}/stream.m3u8", args.srt_stream_id),
            backend: None,
            profile: None,
            flow_id: None,
        }
    }

    fn rtmp(args: &Args) -> Self {
        Self {
            protocol: "rtmp",
            enabled: args.rtmp_bind.is_some(),
            bind: args.rtmp_bind.map(|bind| bind.to_string()),
            output_stream_id: args.rtmp_stream_id.to_string(),
            output_hls_path: format!("/{}/stream.m3u8", args.rtmp_stream_id),
            backend: None,
            profile: None,
            flow_id: None,
        }
    }
}

#[derive(Debug, Clone, Serialize)]
struct ContribAlert {
    level: &'static str,
    code: &'static str,
    message: String,
    count: u64,
    last_seen_unix_ms: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    stream_id_text: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    protocol: Option<&'static str>,
}

#[derive(Debug, Clone, Serialize)]
struct ContribActivity {
    level: &'static str,
    code: &'static str,
    message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    stream_id_text: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    bytes: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    datagrams: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    sequence: Option<u64>,
    seen_unix_ms: u64,
}

struct ContribRouter {
    forwarder: Arc<MeshForwarder>,
    default_stream_id: u64,
    hls_router: Arc<HlsRouter>,
    status: Arc<ContribStatusConfig>,
    publish_ingress_gate: Option<Arc<PublishIngressGate>>,
}

impl ContribRouter {
    fn new(
        forwarder: Arc<MeshForwarder>,
        default_stream_id: u64,
        hls_router: Arc<HlsRouter>,
        status: Arc<ContribStatusConfig>,
        publish_ingress_gate: Option<Arc<PublishIngressGate>>,
    ) -> Self {
        Self {
            forwarder,
            default_stream_id,
            hls_router,
            status,
            publish_ingress_gate,
        }
    }

    async fn route_hls(&self, req: Request<()>) -> HandlerResult<HandlerResponse> {
        let method = req.method().clone();
        let path = req.uri().path().to_owned();
        let query = req.uri().query().map(ToOwned::to_owned);
        let response = self.hls_router.route(req).await?;
        log_hls_response(&method, &path, query.as_deref(), response.status);
        self.status
            .telemetry
            .record_hls_response(&method, &path, query.as_deref(), &response);
        Ok(response)
    }
}

#[derive(Debug, Serialize)]
struct ByteIngestAck {
    stream_id: u64,
    stream_id_text: String,
    chunks: u64,
    bytes: u64,
    datagrams: u64,
}

#[derive(Debug, Serialize)]
struct MediaAck {
    stream_id: u64,
    stream_id_text: String,
    sequence: u64,
    pts_ms: u64,
    dts_ms: Option<u64>,
    duration_ms: u32,
    codec: &'static str,
    flags: u16,
    payload_bytes: usize,
    datagrams: usize,
}

#[async_trait::async_trait]
impl Router for ContribRouter {
    async fn route(&self, req: Request<()>) -> HandlerResult<HandlerResponse> {
        if req.method() == Method::OPTIONS {
            return Ok(response(StatusCode::NO_CONTENT, None, None));
        }
        if req.method() != Method::GET && req.method() != Method::HEAD {
            return Ok(response(StatusCode::METHOD_NOT_ALLOWED, None, None));
        }

        match req.uri().path() {
            "/" => Ok(response(
                StatusCode::OK,
                Some(Bytes::from_static(
                    b"av-contrib\n\nPOST /ingest?stream_id=... publishes arbitrary stream bytes\nPOST /media/access-unit forwards detected media access units\nGET /<stream_id>/stream.m3u8 serves local LL-HLS\nGET /api/status returns service status for Needletail Mission Control\nGET /api/status/events streams service status as SSE\nGET /metrics returns Prometheus metrics\nGET /up checks health\n",
                )),
                Some("text/plain; charset=utf-8"),
            )),
            CONTRIB_STATUS_PATH => {
                let json = serde_json::to_vec(&self.status.snapshot())
                    .map_err(|err| ServerError::Handler(Box::new(err)))?;
                Ok(response(
                    StatusCode::OK,
                    Some(Bytes::from(json)),
                    Some("application/json"),
                ))
            }
            CONTRIB_METRICS_PATH => {
                let mut metrics = self.status.prometheus_metrics().to_vec();
                if let Some(gate) = &self.publish_ingress_gate {
                    metrics.extend_from_slice(gate.prometheus_metrics().as_bytes());
                }
                Ok(response(
                    StatusCode::OK,
                    Some(Bytes::from(metrics)),
                    Some(PROMETHEUS_CONTENT_TYPE),
                ))
            }
            "/up" => Ok(response(
                StatusCode::OK,
                Some(Bytes::from_static(b"OK")),
                Some("text/plain"),
            )),
            "/ingest" => Ok(response(
                StatusCode::METHOD_NOT_ALLOWED,
                Some(Bytes::from_static(b"use POST or PUT /ingest?stream_id=...\n")),
                Some("text/plain"),
            )),
            MEDIA_ACCESS_UNIT_PATH => Ok(response(
                StatusCode::METHOD_NOT_ALLOWED,
                Some(Bytes::from_static(
                    b"use POST or PUT /media/access-unit?stream_id=...&codec=auto\n",
                )),
                Some("text/plain"),
            )),
            _ => self.route_hls(req).await,
        }
    }

    async fn route_body(
        &self,
        req: Request<()>,
        mut body: BodyStream,
    ) -> HandlerResult<HandlerResponse> {
        let path = req.uri().path().to_string();
        if path == "/ingest" {
            if req.method() != Method::POST && req.method() != Method::PUT {
                return Ok(response(
                    StatusCode::METHOD_NOT_ALLOWED,
                    Some(Bytes::from_static(b"use POST or PUT /ingest\n")),
                    Some("text/plain"),
                ));
            }

            if let Some(gate) = &self.publish_ingress_gate {
                if let Err(error) = gate.authorize_legacy_path() {
                    return Ok(publish_authorization_response(&error));
                }
            }

            let stream_id = parse_stream_id_query(req.uri().query(), self.default_stream_id)
                .map_err(ServerError::Config)?;
            let mut chunks = 0u64;
            let mut bytes = 0u64;
            let mut datagrams = 0u64;
            while let Some(next) = body.next().await {
                let chunk = next?;
                if chunk.is_empty() {
                    continue;
                }
                bytes = bytes.saturating_add(chunk.len() as u64);
                chunks = chunks.saturating_add(1);
                let sent = self
                    .forwarder
                    .forward_stream_slot(stream_id, &chunk)
                    .await
                    .map_err(|err| ServerError::Config(err.to_string()))?;
                datagrams = datagrams.saturating_add(sent as u64);
            }

            let ack = ByteIngestAck {
                stream_id,
                stream_id_text: stream_id.to_string(),
                chunks,
                bytes,
                datagrams,
            };
            self.status
                .telemetry
                .record_raw_http(stream_id, chunks, bytes, datagrams);
            let json =
                serde_json::to_vec(&ack).map_err(|err| ServerError::Handler(Box::new(err)))?;
            return Ok(response(
                StatusCode::ACCEPTED,
                Some(Bytes::from(json)),
                Some("application/json"),
            ));
        }

        if path == MEDIA_ACCESS_UNIT_PATH {
            if req.method() != Method::POST && req.method() != Method::PUT {
                return Ok(response(
                    StatusCode::METHOD_NOT_ALLOWED,
                    Some(Bytes::from_static(
                        b"use POST or PUT /media/access-unit?stream_id=...&codec=auto\n",
                    )),
                    Some("text/plain"),
                ));
            }

            if content_length_exceeds(&req, MAX_MEDIA_ACCESS_UNIT_BYTES) {
                return Ok(payload_too_large_response(MAX_MEDIA_ACCESS_UNIT_BYTES));
            }
            let params = MediaAccessUnitParams::parse(
                req.uri().query(),
                self.default_stream_id,
                now_unix_ms(),
            )
            .map_err(ServerError::Config)?;
            let mut admission = None;
            let mut authorized_ack_metadata = None;
            if let Some(gate) = &self.publish_ingress_gate {
                if gate.mode() != PublishAuthorizationMode::Off {
                    let extracted = extract_publish_ingress_request(&req);
                    match extracted {
                        Ok((compact_jws, envelope_json, content_length)) => {
                            let request = PublishIngressRequest {
                                compact_jws,
                                envelope_json: &envelope_json,
                                content_length,
                                legacy_stream_id: params.stream_id,
                                now_unix_seconds: now_unix_seconds(),
                            };
                            match gate.authorize(&request) {
                                Ok(value) => admission = Some(value),
                                Err(error) => {
                                    return Ok(publish_authorization_response(&error));
                                }
                            }
                        }
                        Err(error) => {
                            if let Err(error) = gate.handle_integration_error(error) {
                                return Ok(publish_authorization_response(&error));
                            }
                        }
                    }
                    if gate.mode() == PublishAuthorizationMode::Enforce {
                        let lease = admission
                            .as_ref()
                            .and_then(|value| value.lease())
                            .expect("enforce admission always carries a lease");
                        authorized_ack_metadata =
                            match legacy_ack_metadata_for_publish_lease(&params, lease) {
                                Ok(metadata) => Some(metadata),
                                Err(error) => {
                                    let _ = gate.handle_integration_error(error.clone());
                                    return Ok(publish_authorization_response(&error));
                                }
                            };
                    }
                }
            }
            let body_limit = admission
                .as_ref()
                .and_then(|value| value.lease())
                .filter(|_| {
                    self.publish_ingress_gate
                        .as_ref()
                        .is_some_and(|gate| gate.mode() == PublishAuthorizationMode::Enforce)
                })
                .map_or(MAX_MEDIA_ACCESS_UNIT_BYTES, |lease| {
                    (lease.envelope().payload_bytes() as usize).min(MAX_MEDIA_ACCESS_UNIT_BYTES)
                });
            let Some(payload) = read_body_bytes_limited(&mut body, body_limit).await? else {
                return Ok(payload_too_large_response(body_limit));
            };
            if let (Some(gate), Some(lease)) = (
                self.publish_ingress_gate.as_ref(),
                admission.as_ref().and_then(|value| value.lease()),
            ) {
                if let Err(error) =
                    gate.revalidate_before_forward(lease, payload.len(), now_unix_seconds())
                {
                    return Ok(publish_authorization_response(&error));
                }
            }
            // In enforce mode this reduced metadata exists only for the legacy
            // HTTP acknowledgement. The forwarded bytes are the canonical
            // MOBJ built below and retain the complete authenticated identity.
            let metadata = if let Some(metadata) = authorized_ack_metadata {
                metadata
            } else {
                let sequence = params
                    .sequence
                    .unwrap_or_else(|| self.forwarder.allocate_media_sequence());
                params
                    .metadata_for_payload(sequence, &payload)
                    .map_err(ServerError::Config)?
            };
            let max_datagram_bytes = admission
                .as_ref()
                .and_then(|value| value.lease())
                .filter(|_| {
                    self.publish_ingress_gate
                        .as_ref()
                        .is_some_and(|gate| gate.mode() == PublishAuthorizationMode::Enforce)
                })
                .map(PublishLease::max_datagram_bytes);
            let enforced_lease = admission
                .as_ref()
                .and_then(|value| value.lease())
                .filter(|_| {
                    self.publish_ingress_gate
                        .as_ref()
                        .is_some_and(|gate| gate.mode() == PublishAuthorizationMode::Enforce)
                });
            let forwarded = if let Some(lease) = enforced_lease {
                match lease.canonical_media_object(&payload) {
                    Ok(object) => match media_object::encode(&object) {
                        Ok(encoded) if encoded.len() <= MAX_STREAM_FEC_OBJECT_BYTES => {
                            self.forwarder
                                .forward_stream_slot_with_limit(
                                    lease.carrier_stream_id(),
                                    &encoded,
                                    max_datagram_bytes,
                                )
                                .await
                        }
                        Ok(_) => {
                            let error = PublishIngressError::integration(
                                PublishRejectionCode::EnvelopeTooLarge,
                                "canonical_media_object",
                            );
                            if let Some(gate) = &self.publish_ingress_gate {
                                let _ = gate.handle_integration_error(error.clone());
                            }
                            return Ok(publish_authorization_response(&error));
                        }
                        Err(_) => Err(anyhow::anyhow!(
                            "canonical authorized media object could not be encoded"
                        )),
                    },
                    Err(error) => {
                        if let Some(gate) = &self.publish_ingress_gate {
                            let _ = gate.handle_integration_error(error.clone());
                        }
                        return Ok(publish_authorization_response(&error));
                    }
                }
            } else {
                self.forwarder
                    .forward_media_access_unit(metadata, &payload, None)
                    .await
            };
            let datagrams = match forwarded {
                Ok(datagrams) => datagrams,
                Err(error)
                    if error
                        .downcast_ref::<AuthorizedDatagramLimitExceeded>()
                        .is_some() =>
                {
                    let rejection = PublishIngressError::integration(
                        PublishRejectionCode::DatagramLimit,
                        "max_datagram_bytes",
                    );
                    if let Some(gate) = &self.publish_ingress_gate {
                        let _ = gate.handle_integration_error(rejection.clone());
                    }
                    return Ok(publish_authorization_response(&rejection));
                }
                Err(error) => return Err(ServerError::Config(error.to_string())),
            };
            let ack = MediaAck {
                stream_id: metadata.stream_id,
                stream_id_text: metadata.stream_id.to_string(),
                sequence: metadata.sequence,
                pts_ms: metadata.pts_ms,
                dts_ms: metadata.dts_ms,
                duration_ms: metadata.duration_ms,
                codec: codec_name(metadata.codec),
                flags: metadata.flags.bits(),
                payload_bytes: payload.len(),
                datagrams,
            };
            self.status.telemetry.record_media_access_unit(
                metadata.stream_id,
                payload.len() as u64,
                datagrams as u64,
            );
            let json =
                serde_json::to_vec(&ack).map_err(|err| ServerError::Handler(Box::new(err)))?;
            return Ok(response(
                StatusCode::ACCEPTED,
                Some(Bytes::from(json)),
                Some("application/json"),
            ));
        }

        self.route_hls(req).await
    }

    fn has_body_handler(&self, path: &str) -> bool {
        path == "/ingest" || path == MEDIA_ACCESS_UNIT_PATH
    }

    fn is_streaming(&self, path: &str) -> bool {
        path == CONTRIB_STATUS_EVENTS_PATH || self.hls_router.is_streaming(path)
    }

    async fn route_stream(
        &self,
        req: Request<()>,
        mut stream_writer: Box<dyn StreamWriter>,
    ) -> HandlerResult<()> {
        if req.uri().path() == CONTRIB_STATUS_EVENTS_PATH {
            let response = Response::builder()
                .status(StatusCode::OK)
                .header("content-type", "text/event-stream")
                .header("cache-control", "no-store, max-age=0")
                .body(())
                .map_err(ServerError::Http)?;
            stream_writer.send_response(response).await?;

            let mut ticker = interval(Duration::from_secs(1));
            ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);
            loop {
                stream_writer.send_data(self.status.sse_event()?).await?;
                ticker.tick().await;
            }
        }

        self.hls_router.route_stream(req, stream_writer).await
    }

    fn webtransport_handler(&self) -> Option<&dyn WebTransportHandler> {
        self.hls_router.webtransport_handler()
    }

    fn websocket_handler(&self, path: &str) -> Option<&dyn WebSocketHandler> {
        self.hls_router.websocket_handler(path)
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "av_contrib=info,av_web_service=info".into()),
        )
        .init();

    let args = Args::parse();
    let publish_ingress_gate = load_publish_ingress_gate()?;
    validate_enforced_ingress_adapters(&args, publish_ingress_gate.as_deref())?;
    let (cert, key) = load_tls(&args)?;
    let telemetry = Arc::new(IngestTelemetry::default());
    let forwarder = Arc::new(MeshForwarder::new(&args, Arc::clone(&telemetry)).await?);
    let mesh_publisher: Arc<dyn Fmp4PartPublisher> = forwarder.clone();
    let publisher: Arc<dyn Fmp4PartPublisher> = Arc::new(TelemetryFmp4Publisher {
        inner: mesh_publisher,
        telemetry: telemetry.clone(),
        canonical_sequences: Mutex::new(HashMap::new()),
    });
    let (playlists, chunk_cache, m3u8_cache) = playlists::Playlists::new(playlist_options(&args));
    let hls_router =
        Arc::new(HlsRouter::new().add_handler(Box::new(HlsHandler::new(chunk_cache, m3u8_cache))));
    let status = Arc::new(ContribStatusConfig::from_args(&args, telemetry.clone()));
    let (shutdown_tx, shutdown_rx) = watch::channel(());
    let rist_shutdown = if let Some(bind) = args.rist_bind {
        let output_stream_idx = resolve_output_stream_idx(&playlists, args.rist_stream_id).await;
        Some(
            start_rist_ingest(
                RistIngestConfig {
                    bind,
                    profile: args.rist_profile,
                    flow_id: args.rist_flow_id,
                    output_stream_id: args.rist_stream_id,
                    output_stream_idx,
                    min_part_ms: args.fmp4_part_ms,
                },
                playlists.clone(),
                publisher.clone(),
                telemetry.clone(),
                shutdown_rx.clone(),
            )
            .await?,
        )
    } else {
        None
    };
    let srt_shutdown = if let Some(bind) = args.srt_bind {
        Some(
            start_srt_ingest(
                SrtIngestConfig {
                    bind,
                    output_stream_id: args.srt_stream_id,
                    min_part_ms: args.fmp4_part_ms,
                },
                playlists.clone(),
                publisher.clone(),
                telemetry.clone(),
                shutdown_rx.clone(),
            )
            .await?,
        )
    } else {
        None
    };
    let (rtmp_shutdown, rtmp_finished, rtmp_task) = if let Some(bind) = args.rtmp_bind {
        let (up, finished, shutdown, rx) =
            start_rtmp_listener(key.clone(), bind)
                .await
                .map_err(|error| {
                    anyhow::anyhow!("failed to bind RTMP contributor frontend: {error}")
                })?;
        let _ = up.await;
        info!(
            bind = %bind,
            output_stream_id = args.rtmp_stream_id,
            "RTMP contributor frontend listening"
        );
        let task = tokio::spawn(run_rtmp_hls_bridge(
            rx,
            playlists.clone(),
            publisher.clone(),
            telemetry.clone(),
            args.rtmp_stream_id,
            args.fmp4_part_ms,
        ));
        (Some(shutdown), Some(finished), Some(task))
    } else {
        (None, None, None)
    };
    let (daw_media_task, audio_epoch_hls_task) = if let Some(bind) = args.daw_media_bind {
        let socket = bind_daw_media_udp_socket(bind).await?;
        let local_addr = socket.local_addr()?;
        let targets = Arc::new(RwLock::new(HashMap::new()));
        let (audio_epoch_hls_tx, audio_epoch_hls_rx) =
            audio_epoch_hls_channel(args.daw_hls_queue_capacity);
        AUDIO_EPOCH_HLS_QUEUE_CAPACITY
            .store(audio_epoch_hls_tx.max_capacity() as u64, Ordering::Relaxed);
        let audio_epoch_hls_task = tokio::spawn(run_audio_epoch_hls_worker(
            AudioEpochHlsConfig::new(
                args.stream_id,
                args.fmp4_part_ms,
                playlists.clone(),
                publisher.clone(),
            ),
            audio_epoch_hls_rx,
            shutdown_rx.clone(),
        ));
        info!(
            bind = %local_addr,
            mesh_media_fec_target = %args.mesh_media_fec_target,
            hls_base_stream_id = args.stream_id,
            hls_queue_capacity = args.daw_hls_queue_capacity,
            "DAW media UDP relay listening"
        );
        (
            Some(tokio::spawn(run_daw_media_udp_ingest(
                socket,
                forwarder.clone(),
                targets,
                Some(audio_epoch_hls_tx),
                shutdown_rx.clone(),
            ))),
            Some(audio_epoch_hls_task),
        )
    } else {
        (None, None)
    };
    let router = Box::new(ContribRouter::new(
        forwarder.clone(),
        args.stream_id,
        hls_router,
        status,
        publish_ingress_gate,
    ));
    let server = H2H3Server::builder()
        .with_tls(cert, key)
        .with_port(args.http_port)
        .enable_h2(true)
        .enable_h3(true)
        .with_router(router)
        .build()?;
    let handle = server.start().await?;
    let _ = handle.ready_rx.await;

    info!(
        http_port = args.http_port,
        mesh_fec_target = %args.mesh_fec_target,
        mesh_media_fec_target = %args.mesh_media_fec_target,
        default_stream_id = args.stream_id,
        "av-contrib ready"
    );
    println!("contrib: https://127.0.0.1:{}", args.http_port);
    let advertised_hls_stream_id = advertised_hls_stream_id(&args);
    println!(
        "ll-hls:  https://127.0.0.1:{}/{}/stream.m3u8",
        args.http_port, advertised_hls_stream_id
    );
    println!("bytes:   udp+stream-fec://{}", args.mesh_fec_target);
    println!("media:   udp+media-fec://{}", args.mesh_media_fec_target);
    if let Some(target) = args.relay_primary_target {
        println!(
            "relay-primary:   {} -> udp+relay-session://{target}",
            args.relay_primary_bind
                .map_or_else(|| "ephemeral".to_owned(), |bind| bind.to_string())
        );
    }
    if let Some(target) = args.relay_secondary_target {
        println!(
            "relay-secondary: {} -> udp+relay-session://{target}",
            args.relay_secondary_bind
                .map_or_else(|| "ephemeral".to_owned(), |bind| bind.to_string())
        );
    }
    if let Some(bind) = args.rist_bind {
        println!(
            "rist:    rist://127.0.0.1:{} backend={} profile={} flow_id=0x{:08x} stream_id={}",
            bind.port(),
            args.rist_backend.as_str(),
            args.rist_profile.as_str(),
            args.rist_flow_id,
            args.rist_stream_id
        );
    }
    if let Some(bind) = args.srt_bind {
        println!(
            "srt:     srt://127.0.0.1:{} stream_id={}",
            bind.port(),
            args.srt_stream_id
        );
    }
    if let Some(bind) = args.rtmp_bind {
        println!(
            "rtmp:    rtmp://127.0.0.1:{} stream_id={}",
            bind.port(),
            args.rtmp_stream_id
        );
    }
    if let Some(bind) = args.daw_media_bind {
        println!("daw:     udp+daw-media://{}", bind);
    }
    println!("status:  https://127.0.0.1:{}/api/status", args.http_port);
    println!(
        "events:  https://127.0.0.1:{}/api/status/events",
        args.http_port
    );
    println!("health:  https://127.0.0.1:{}/up", args.http_port);

    tokio::signal::ctrl_c().await?;
    let _ = handle.shutdown_tx.send(());
    let _ = shutdown_tx.send(());
    if let Some(shutdown) = rist_shutdown {
        let _ = shutdown.send(());
    }
    if let Some(shutdown) = srt_shutdown {
        let _ = shutdown.send(());
    }
    if let Some(shutdown) = rtmp_shutdown {
        let _ = shutdown.send(());
    }
    let _ = handle.finished_rx.await;
    if let Some(finished) = rtmp_finished {
        let _ = finished.await;
    }
    if let Some(task) = rtmp_task {
        let _ = task.await;
    }
    if let Some(task) = daw_media_task {
        let _ = task.await;
    }
    if let Some(task) = audio_epoch_hls_task {
        let _ = task.await;
    }
    Ok(())
}

fn response(
    status: StatusCode,
    body: Option<Bytes>,
    content_type: Option<&str>,
) -> HandlerResponse {
    HandlerResponse {
        status,
        body,
        content_type: content_type.map(ToOwned::to_owned),
        ..Default::default()
    }
}

#[derive(Serialize)]
struct PublishAuthorizationErrorBody {
    error: &'static str,
    reason: &'static str,
    field: &'static str,
}

fn publish_authorization_response(error: &PublishIngressError) -> HandlerResponse {
    let status = match error.code() {
        PublishRejectionCode::MissingCapability
        | PublishRejectionCode::MalformedAuthorization
        | PublishRejectionCode::InvalidSignature
        | PublishRejectionCode::CapabilityRejected => StatusCode::UNAUTHORIZED,
        PublishRejectionCode::CapabilityExpired | PublishRejectionCode::RevokedBinding => {
            StatusCode::GONE
        }
        PublishRejectionCode::CapabilityReplay | PublishRejectionCode::FrameReplay => {
            StatusCode::CONFLICT
        }
        PublishRejectionCode::WrongScope
        | PublishRejectionCode::StreamMismatch
        | PublishRejectionCode::LegacyPath
        | PublishRejectionCode::TalkbackIsolation => StatusCode::FORBIDDEN,
        PublishRejectionCode::EnvelopeTooLarge
        | PublishRejectionCode::CapabilityTooLarge
        | PublishRejectionCode::DatagramLimit => StatusCode::PAYLOAD_TOO_LARGE,
        _ => StatusCode::BAD_REQUEST,
    };
    let body = serde_json::to_vec(&PublishAuthorizationErrorBody {
        error: "publish_authorization_rejected",
        reason: error.code().as_str(),
        field: error.field(),
    })
    .unwrap_or_else(|_| b"{\"error\":\"publish_authorization_rejected\"}".to_vec());
    response(status, Some(Bytes::from(body)), Some("application/json"))
}

fn extract_publish_ingress_request(
    req: &Request<()>,
) -> Result<(&str, Vec<u8>, u64), PublishIngressError> {
    if req.headers().get_all(AUTHORIZATION).iter().count() > 1 {
        return Err(PublishIngressError::integration(
            PublishRejectionCode::MalformedAuthorization,
            "authorization",
        ));
    }
    if req
        .headers()
        .get_all(MEDIA_FRAME_ENVELOPE_HEADER)
        .iter()
        .count()
        > 1
    {
        return Err(PublishIngressError::integration(
            PublishRejectionCode::MalformedEnvelope,
            "frame_envelope",
        ));
    }
    if req.headers().get_all(CONTENT_LENGTH).iter().count() > 1 {
        return Err(PublishIngressError::integration(
            PublishRejectionCode::InvalidContentLength,
            "content_length",
        ));
    }
    let compact_jws = parse_bearer_header(req.headers().get(AUTHORIZATION).map(|v| v.as_bytes()))?;
    let envelope_json = decode_envelope_header_bytes(
        req.headers()
            .get(MEDIA_FRAME_ENVELOPE_HEADER)
            .map(|value| value.as_bytes()),
    )?;
    let content_length =
        parse_content_length_header(req.headers().get(CONTENT_LENGTH).map(|v| v.as_bytes()))?;
    Ok((compact_jws, envelope_json, content_length))
}

fn legacy_ack_metadata_for_publish_lease(
    params: &MediaAccessUnitParams,
    lease: &PublishLease,
) -> Result<MediaFrameMetadata, PublishIngressError> {
    if params
        .sequence
        .is_some_and(|sequence| sequence != lease.envelope().sequence())
    {
        return Err(PublishIngressError::integration(
            PublishRejectionCode::ConfigurationMismatch,
            "sequence",
        ));
    }
    let codec = match lease.configuration().payload_format() {
        media_object::MediaFramePayloadFormat::Opus => MediaCodec::Opus,
        media_object::MediaFramePayloadFormat::PcmS24le
        | media_object::MediaFramePayloadFormat::Flac
        | media_object::MediaFramePayloadFormat::Json
        | media_object::MediaFramePayloadFormat::Opaque => MediaCodec::Data,
    };
    if params.codec_explicit && params.codec != codec {
        return Err(PublishIngressError::integration(
            PublishRejectionCode::ConfigurationMismatch,
            "payload_format",
        ));
    }
    let capture_pts = u128::try_from(lease.envelope().capture_pts()).map_err(|_| {
        PublishIngressError::integration(PublishRejectionCode::ConfigurationMismatch, "capture_pts")
    })?;
    let timebase = u128::from(lease.configuration().capture_timebase_hz());
    let pts_ms = capture_pts
        .checked_mul(1_000)
        .and_then(|value| u64::try_from(value / timebase).ok())
        .ok_or_else(|| {
            PublishIngressError::integration(
                PublishRejectionCode::ConfigurationMismatch,
                "capture_pts",
            )
        })?;
    let duration_ms = u128::from(lease.envelope().duration_ticks())
        .checked_mul(1_000)
        .map(|value| value.div_ceil(timebase).max(1))
        .and_then(|value| u32::try_from(value).ok())
        .filter(|value| *value <= u32::from(u16::MAX))
        .ok_or_else(|| {
            PublishIngressError::integration(
                PublishRejectionCode::ConfigurationMismatch,
                "duration_ticks",
            )
        })?;
    let mut secure = params.clone();
    secure.sequence = Some(lease.envelope().sequence());
    secure.pts_ms = pts_ms;
    secure.dts_ms = None;
    secure.duration_ms = duration_ms;
    secure.codec = codec;
    secure.codec_explicit = true;
    secure.metadata(lease.envelope().sequence()).map_err(|_| {
        PublishIngressError::integration(
            PublishRejectionCode::ConfigurationMismatch,
            "media_metadata",
        )
    })
}

fn load_publish_ingress_gate() -> Result<Option<Arc<PublishIngressGate>>> {
    let mode = match std::env::var("AV_MEDIA_CAPABILITY_PUBLISH_MODE")
        .unwrap_or_else(|_| "off".to_owned())
        .to_ascii_lowercase()
        .as_str()
    {
        "off" => PublishAuthorizationMode::Off,
        "observe" | "dark" => PublishAuthorizationMode::Observe,
        "enforce" => PublishAuthorizationMode::Enforce,
        _ => bail!("AV_MEDIA_CAPABILITY_PUBLISH_MODE must be off, observe, or enforce"),
    };
    if mode == PublishAuthorizationMode::Off {
        return Ok(None);
    }
    if mode == PublishAuthorizationMode::Enforce
        && !cfg!(feature = "media_capability_enforce_publish_v1")
    {
        bail!("publish enforcement requires the media_capability_enforce_publish_v1 Cargo feature");
    }
    let path = std::env::var_os("AV_MEDIA_CAPABILITY_PUBLISH_BUNDLE")
        .map(PathBuf::from)
        .context(
            "AV_MEDIA_CAPABILITY_PUBLISH_BUNDLE is required when publish authorization is enabled",
        )?;
    let gate = gate_from_bootstrap_path(&path, mode)
        .context("invalid public publish-authorization bundle")?;
    Ok(Some(Arc::new(gate)))
}

fn validate_enforced_ingress_adapters(
    args: &Args,
    gate: Option<&PublishIngressGate>,
) -> Result<()> {
    if !gate.is_some_and(|gate| gate.mode() == PublishAuthorizationMode::Enforce) {
        return Ok(());
    }
    let mut legacy = Vec::new();
    if args.daw_media_bind.is_some() {
        legacy.push("DAW UDP");
    }
    if args.rist_bind.is_some() {
        legacy.push("RIST");
    }
    if args.srt_bind.is_some() {
        legacy.push("SRT");
    }
    if args.rtmp_bind.is_some() {
        legacy.push("RTMP");
    }
    if !legacy.is_empty() {
        bail!(
            "publish enforcement cannot start unauthenticated legacy adapters: {}",
            legacy.join(", ")
        );
    }
    Ok(())
}

fn log_hls_response(method: &Method, path: &str, query: Option<&str>, status: StatusCode) {
    if status == StatusCode::NOT_FOUND {
        debug!(
            %method,
            %path,
            query = query.unwrap_or(""),
            status = status.as_u16(),
            "HLS request not found"
        );
    } else if status.is_success() {
        debug!(
            %method,
            %path,
            query = query.unwrap_or(""),
            status = status.as_u16(),
            "HLS request completed"
        );
    } else {
        debug!(
            %method,
            %path,
            query = query.unwrap_or(""),
            status = status.as_u16(),
            "HLS request completed with non-success status"
        );
    }
}

fn content_length_exceeds(req: &Request<()>, maximum: usize) -> bool {
    req.headers()
        .get(CONTENT_LENGTH)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.parse::<u64>().ok())
        .is_some_and(|length| length > maximum as u64)
}

fn payload_too_large_response(maximum: usize) -> HandlerResponse {
    response(
        StatusCode::PAYLOAD_TOO_LARGE,
        Some(Bytes::from(format!(
            "media access unit exceeds {maximum} bytes\n"
        ))),
        Some("text/plain; charset=utf-8"),
    )
}

async fn read_body_bytes_limited(
    body: &mut BodyStream,
    maximum: usize,
) -> HandlerResult<Option<Bytes>> {
    let mut bytes = BytesMut::new();
    while let Some(next) = body.next().await {
        let chunk = next?;
        let Some(next_len) = bytes.len().checked_add(chunk.len()) else {
            return Ok(None);
        };
        if next_len > maximum {
            return Ok(None);
        }
        bytes.extend_from_slice(&chunk);
    }
    Ok(Some(bytes.freeze()))
}

fn load_tls(args: &Args) -> Result<(String, String)> {
    match (&args.cert, &args.key) {
        (Some(cert), Some(key)) => load_tls_base64_from_paths(cert, key),
        (None, None) => load_default_tls_base64(),
        _ => bail!("--cert and --key must be provided together"),
    }
}

fn playlist_options(args: &Args) -> playlists::Options {
    playlists::Options {
        max_segments: 32,
        num_playlists: args.playlist_count.max(1),
        max_parts_per_segment: playlist_part_capacity(args.fmp4_segment_ms, args.fmp4_part_ms),
        max_parted_segments: 32,
        segment_min_ms: args.fmp4_segment_ms.max(args.fmp4_part_ms).max(1),
        target_duration_ms: args.hls_target_duration_ms.max(1_000),
        part_target_ms: args.fmp4_part_ms.max(1),
        buffer_size_kb: args.playlist_buffer_kb.max(1),
        init_size_kb: 5,
    }
}

fn playlist_part_capacity(segment_ms: u32, part_ms: u32) -> usize {
    const LEGACY_MINIMUM: u64 = 128;
    const ROLLOVER_MARGIN: u64 = 8;
    const DEFENSIVE_MAXIMUM: u64 = 4_096;

    let part_ms = u64::from(part_ms.max(1));
    let target_parts = u64::from(segment_ms.max(1)).div_ceil(part_ms);
    target_parts
        .saturating_add(ROLLOVER_MARGIN)
        .clamp(LEGACY_MINIMUM, DEFENSIVE_MAXIMUM) as usize
}

fn local_sender_addr(peer: SocketAddr) -> SocketAddr {
    match peer {
        SocketAddr::V4(addr) => {
            let ip = if addr.ip().is_loopback() {
                std::net::Ipv4Addr::LOCALHOST
            } else {
                std::net::Ipv4Addr::UNSPECIFIED
            };
            SocketAddr::new(ip.into(), 0)
        }
        SocketAddr::V6(addr) => {
            let ip = if addr.ip().is_loopback() {
                std::net::Ipv6Addr::LOCALHOST
            } else {
                std::net::Ipv6Addr::UNSPECIFIED
            };
            SocketAddr::new(ip.into(), 0)
        }
    }
}

fn relay_bind_addr(
    flag: &'static str,
    configured: Option<SocketAddr>,
    target: SocketAddr,
) -> Result<SocketAddr> {
    let Some(bind) = configured else {
        return Ok(local_sender_addr(target));
    };
    if bind.port() == 0 {
        bail!("{flag} must use a fixed non-zero port");
    }
    if bind.ip().is_unspecified() {
        bail!("{flag} must use the source IP registered by its relay peer");
    }
    if bind.is_ipv4() != target.is_ipv4() {
        bail!("{flag} and its relay target must use the same address family");
    }
    Ok(bind)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct StreamFecGeometry {
    block_count: u32,
    packet_count: u32,
    repair_symbols: u32,
}

fn stream_fec_geometry(
    payload_len: usize,
    symbol_size: u16,
    repair_symbols_per_legacy_block: u32,
) -> Result<StreamFecGeometry> {
    let symbol_size = symbol_size.max(1);
    let raptorq_max_payload = usize::try_from(MAX_SOURCE_SYMBOLS_PER_BLOCK)
        .context("RaptorQ source-symbol bound does not fit this platform")?
        .checked_mul(usize::from(symbol_size))
        .context("RaptorQ stream object size bound overflowed")?;
    let max_object_payload = raptorq_max_payload.min(MAX_STREAM_FEC_OBJECT_BYTES);
    if payload_len > max_object_payload {
        bail!(
            "stream slot is too large for one RaptorQ source block: got {payload_len} bytes, max is {max_object_payload} bytes for {symbol_size}-byte symbols"
        );
    }

    // The old chunked encoder emitted `repair_symbols` for every four source
    // symbols. Preserve that wire-overhead/recovery budget while protecting the
    // whole application object as one block so the receiver commits one slot.
    let max_block_payload = usize::from(DEFAULT_SOURCE_SYMBOLS) * usize::from(symbol_size);
    let legacy_block_count = payload_len.max(1).div_ceil(max_block_payload);
    let repair_symbols = u32::try_from(
        (legacy_block_count as u64)
            .checked_mul(u64::from(repair_symbols_per_legacy_block))
            .context("RaptorQ repair-symbol count overflowed")?,
    )
    .context("RaptorQ repair-symbol count exceeds the wire sequence space")?;
    let packet_count = u32::from(source_symbol_count(payload_len, symbol_size))
        .checked_add(repair_symbols)
        .context("RaptorQ packet count exceeds the wire sequence space")?;
    if packet_count > MAX_STREAM_FEC_DATAGRAMS {
        bail!(
            "stream slot requires {packet_count} RaptorQ datagrams; limit is {MAX_STREAM_FEC_DATAGRAMS}"
        );
    }

    Ok(StreamFecGeometry {
        block_count: 1,
        packet_count,
        repair_symbols,
    })
}

fn encode_stream_fec_payload(
    stream_id: u64,
    payload: &[u8],
    repair_symbols: u32,
    symbol_size: u16,
    initial_block_id: u32,
    initial_packet_sequence: u32,
) -> Result<Vec<Bytes>> {
    let geometry = stream_fec_geometry(payload.len(), symbol_size, repair_symbols)?;
    let mut encoder = DatagramFecEncoder::new()
        .with_initial_block_id(initial_block_id)
        .with_symbol_size(symbol_size);
    let datagrams = encoder
        .encode_object_with_repair_symbols(payload, geometry.repair_symbols)
        .context("failed to encode stream slot for mesh RaptorQ-FEC")?;
    if encoder.block_id() != initial_block_id.wrapping_add(geometry.block_count)
        || datagrams.len() != geometry.packet_count as usize
    {
        bail!(
            "RaptorQ-FEC geometry changed while reserving concurrent wire ids: expected {} block(s) and {} packet(s), encoded {} block(s) and {} packet(s)",
            geometry.block_count,
            geometry.packet_count,
            encoder.block_id().wrapping_sub(initial_block_id),
            datagrams.len(),
        );
    }
    let stream_prefix = stream_id.to_be_bytes();
    datagrams
        .into_iter()
        .enumerate()
        .map(|(index, mut datagram)| {
            let mut header = DatagramFecHeader::decode(&datagram)
                .context("failed to decode newly encoded RaptorQ-FEC header")?;
            header.packet_sequence = initial_packet_sequence.wrapping_add(index as u32);
            header.packet_crc32 = header
                .compute_packet_crc32(&datagram[HEADER_LEN..])
                .context("failed to update RaptorQ-FEC packet CRC")?;
            header
                .encode(&mut datagram[..HEADER_LEN])
                .context("failed to write updated RaptorQ-FEC header")?;

            let mut framed = Vec::with_capacity(stream_prefix.len() + datagram.len());
            framed.extend_from_slice(&stream_prefix);
            framed.extend_from_slice(&datagram);
            Ok(Bytes::from(framed))
        })
        .collect()
}

fn parse_stream_id_query(
    query: Option<&str>,
    default_stream_id: u64,
) -> std::result::Result<u64, String> {
    let mut stream_id = default_stream_id;
    for (key, value) in form_urlencoded::parse(query.unwrap_or("").as_bytes()) {
        match key.as_ref() {
            "stream_id" | "stream" => {
                stream_id = value
                    .parse::<u64>()
                    .map_err(|error| format!("invalid stream_id `{value}`: {error}"))?;
            }
            _ => {}
        }
    }
    Ok(stream_id)
}

fn now_unix_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.as_millis() as u64)
        .unwrap_or(0)
}

fn now_unix_seconds() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| i64::try_from(duration.as_secs()).unwrap_or(i64::MAX))
        .unwrap_or(0)
}

fn now_unix_us() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.as_micros() as u64)
        .unwrap_or(0)
}

fn parse_u32_auto(value: &str) -> std::result::Result<u32, String> {
    let trimmed = value.trim();
    if let Some(hex) = trimmed
        .strip_prefix("0x")
        .or_else(|| trimmed.strip_prefix("0X"))
    {
        u32::from_str_radix(hex, 16).map_err(|err| err.to_string())
    } else {
        trimmed.parse::<u32>().map_err(|err| err.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use music_audio_session::{MultichannelAudioSender, MultichannelAudioSessionConfig};
    use raptorq_datagram_fec::{
        AudioPayloadKind, AudioSampleFormat, MultichannelAudioEpoch, MultichannelAudioFecConfig,
        MultichannelAudioGroup,
    };
    use raptorq_fec_transport::{
        split_stream_id_prefix, FecDatagramDecoder, MultichannelAudioTransportAdapter,
    };
    use std::net::Ipv4Addr;
    use tokio::time::timeout;

    const TEST_SOURCE_EPOCH: u64 = 1_784_151_600_000_001;

    #[allow(clippy::too_many_arguments)]
    fn test_fmp4_part(
        stream_id: u64,
        sequence: u64,
        keyframe: bool,
        video_units: usize,
        audio_units: usize,
        init: Option<Bytes>,
        bytes: Bytes,
        published_at_unix_ns: i64,
    ) -> PublishedFmp4Part {
        PublishedFmp4Part {
            stream_id,
            stream_idx: 0,
            sequence,
            duration_ms: 67,
            packaged_at_unix_ns: published_at_unix_ns - 2_000_000,
            published_at_unix_ns,
            init,
            bytes,
            keyframe,
            video_codec: (video_units > 0).then_some("h264"),
            video_width: (video_units > 0).then_some(1_280),
            video_height: (video_units > 0).then_some(720),
            video_units,
            audio_codec: (audio_units > 0).then_some("aac"),
            audio_units,
        }
    }

    #[test]
    fn parses_decimal_and_hex_rist_flow_ids() {
        assert_eq!(parse_u32_auto("0x11223344").unwrap(), DEFAULT_FLOW_ID);
        assert_eq!(parse_u32_auto("287454020").unwrap(), DEFAULT_FLOW_ID);
    }

    #[test]
    fn playlist_capacity_supports_five_millisecond_parts() {
        assert_eq!(playlist_part_capacity(1_000, 50), 128);
        assert_eq!(playlist_part_capacity(1_000, 20), 128);
        assert_eq!(playlist_part_capacity(1_000, 10), 128);
        assert_eq!(playlist_part_capacity(1_000, 5), 208);
        assert_eq!(playlist_part_capacity(u32::MAX, 1), 4_096);
    }

    #[test]
    fn audio_epoch_hls_drop_logging_is_sampled_under_overload() {
        assert!((1..=16).all(should_log_audio_epoch_hls_drop));
        assert!(should_log_audio_epoch_hls_drop(32));
        assert!(should_log_audio_epoch_hls_drop(10_000));
        assert!(!should_log_audio_epoch_hls_drop(17));
        assert!(!should_log_audio_epoch_hls_drop(9_999));
    }

    #[test]
    fn parses_stream_id_query_as_u64_string() {
        let snowflake = "9007199254741993";
        assert_eq!(
            parse_stream_id_query(Some(&format!("stream_id={snowflake}")), 1).unwrap(),
            9_007_199_254_741_993
        );
        assert_eq!(parse_stream_id_query(None, 7).unwrap(), 7);
    }

    #[test]
    fn daw_relay_v2_subscription_is_session_scoped() {
        assert_eq!(
            parse_daw_relay_session_message(
                b"WAVEY-DAW-SUBSCRIBE/2 18446744073709551615",
                DAW_RELAY_SUBSCRIBE_V2_PREFIX,
            ),
            Some(u64::MAX)
        );
        assert_eq!(
            parse_daw_relay_session_message(
                b"WAVEY-DAW-SUBSCRIBE/2 all",
                DAW_RELAY_SUBSCRIBE_V2_PREFIX,
            ),
            None
        );
        assert_eq!(daw_relay_session_ack(42), b"WAVEY-DAW-SUBSCRIBED/2 42");
    }

    #[test]
    fn media_access_unit_content_length_is_admitted_within_bound() {
        let accepted = Request::builder()
            .header(CONTENT_LENGTH, MAX_MEDIA_ACCESS_UNIT_BYTES.to_string())
            .body(())
            .unwrap();
        let rejected = Request::builder()
            .header(
                CONTENT_LENGTH,
                (MAX_MEDIA_ACCESS_UNIT_BYTES + 1).to_string(),
            )
            .body(())
            .unwrap();

        assert!(!content_length_exceeds(
            &accepted,
            MAX_MEDIA_ACCESS_UNIT_BYTES
        ));
        assert!(content_length_exceeds(
            &rejected,
            MAX_MEDIA_ACCESS_UNIT_BYTES
        ));
    }

    #[tokio::test]
    async fn media_access_unit_streaming_body_stops_at_bound() {
        let mut accepted: BodyStream = futures_util::stream::iter([
            Ok::<_, ServerError>(Bytes::from_static(b"1234")),
            Ok::<_, ServerError>(Bytes::from_static(b"5678")),
        ])
        .boxed();
        assert_eq!(
            read_body_bytes_limited(&mut accepted, 8)
                .await
                .unwrap()
                .unwrap(),
            Bytes::from_static(b"12345678")
        );

        let mut rejected: BodyStream = futures_util::stream::iter([
            Ok::<_, ServerError>(Bytes::from_static(b"1234")),
            Ok::<_, ServerError>(Bytes::from_static(b"56789")),
        ])
        .boxed();
        assert!(read_body_bytes_limited(&mut rejected, 8)
            .await
            .unwrap()
            .is_none());
    }

    #[test]
    fn canonical_fmp4_object_preserves_dependency_timing_duration_and_exact_envelope() {
        const PUBLISHED_NS: i64 = 1_721_000_000_300_000_000;
        let init = Bytes::from_static(b"ftypmoov");
        let media = Bytes::from_static(b"moofmdat");
        let bundled = encode_mesh_fmp4_slot(Some(&init), &media).unwrap();
        let (initialization, configuration_epoch) =
            build_fmp4_initialization_object(77, TEST_SOURCE_EPOCH, &init).unwrap();
        let part = test_fmp4_part(77, 42, true, 3, 4, Some(init), media, PUBLISHED_NS);
        let original = build_fmp4_media_object(
            &part,
            &bundled,
            initialization.key().clone(),
            configuration_epoch,
            TEST_SOURCE_EPOCH,
            1_000,
            250_000,
        )
        .unwrap();
        let envelope = encode_canonical_media_object(&original).unwrap();
        let object = media_object::decode(&envelope).unwrap();

        assert_eq!(object, original);
        assert_eq!(object.key().stream(), "77");
        assert_eq!(object.key().track(), FMP4_MEDIA_TRACK);
        assert_eq!(object.key().epoch(), TEST_SOURCE_EPOCH);
        assert_eq!(object.key().object(), 42);
        assert_eq!(object.kind(), ObjectKind::Media);
        assert!(object.is_keyframe());
        assert_eq!(object.configuration_epoch(), configuration_epoch);
        assert_eq!(object.dependencies(), &[initialization.key().clone()]);
        assert!(object.capture_timestamp().is_none());
        assert_eq!(object.stage_timestamps().len(), 2);
        assert_eq!(object.stage_timestamps()[0].stage(), Stage::Packaged);
        assert_eq!(
            object.stage_timestamps()[0].timestamp().unix_time_ns(),
            PUBLISHED_NS - 2_000_000
        );
        assert_eq!(object.stage_timestamps()[1].stage(), Stage::Published);
        assert_eq!(
            object.stage_timestamps()[1].timestamp().unix_time_ns(),
            PUBLISHED_NS
        );
        assert_eq!(
            object.deadline().unwrap().unix_time_ns(),
            PUBLISHED_NS + 1_000_000_000
        );
        assert_eq!(
            object.deadline().unwrap().confidence(),
            ClockConfidence::estimated(250_000)
        );
        assert_eq!(object.metadata().get("container").unwrap(), b"fmp4");
        assert_eq!(object.metadata().get("duration-ms").unwrap(), b"67");
        assert_eq!(
            object.metadata().get("payload-format").unwrap(),
            b"fmp4-slot-v1"
        );
        assert_eq!(
            object.metadata().get("scheduler-class").unwrap(),
            b"video-keyframe"
        );
        assert_eq!(
            object.metadata().get("track-composition").unwrap(),
            b"audio+video"
        );
        assert_eq!(object.metadata().get("audio-codec").unwrap(), b"aac");
        assert_eq!(object.metadata().get("video-codec").unwrap(), b"h264");
        assert_eq!(object.payload(), bundled.as_ref());
    }

    #[test]
    fn canonical_fmp4_retry_and_initialization_identity_are_deterministic() {
        let init = Bytes::from_static(b"stable-init");
        let media = Bytes::from_static(b"stable-media");
        let bundled = encode_mesh_fmp4_slot(Some(&init), &media).unwrap();
        let (first_initialization, first_epoch) =
            build_fmp4_initialization_object(9, TEST_SOURCE_EPOCH, &init).unwrap();
        let (retry_initialization, retry_epoch) =
            build_fmp4_initialization_object(9, TEST_SOURCE_EPOCH, &init).unwrap();
        assert_eq!(first_initialization, retry_initialization);
        assert_eq!(first_epoch, retry_epoch);

        let part = test_fmp4_part(
            9,
            12,
            false,
            1,
            1,
            Some(init),
            media,
            1_721_000_000_500_000_000,
        );
        let build = || {
            build_fmp4_media_object(
                &part,
                &bundled,
                first_initialization.key().clone(),
                first_epoch,
                TEST_SOURCE_EPOCH,
                900,
                500_000,
            )
            .unwrap()
        };
        let first = build();
        let retry = build();
        assert_eq!(first, retry);
        assert_eq!(
            media_object::encode(&first).unwrap(),
            media_object::encode(&retry).unwrap()
        );
        assert_eq!(first.dependencies(), &[first_initialization.key().clone()]);
        let (next_incarnation, _) =
            build_fmp4_initialization_object(9, TEST_SOURCE_EPOCH + 1, b"stable-init").unwrap();
        assert_ne!(first_initialization.key(), next_incarnation.key());

        let (live_initialization, live_epoch) = build_live_fmp4_initialization_object(
            9,
            TEST_SOURCE_EPOCH,
            b"stable-init",
            1_721_000_000_500_000_000,
            900,
            500_000,
        )
        .unwrap();
        assert_eq!(live_initialization.key(), first_initialization.key());
        assert_eq!(live_epoch, first_epoch);
        assert_eq!(
            live_initialization.deadline().unwrap().unix_time_ns(),
            1_721_000_001_400_000_000
        );
    }

    #[test]
    fn muxed_audio_video_uses_audio_priority_until_a_keyframe() {
        let init = Bytes::from_static(b"muxed-init");
        let (initialization, epoch) =
            build_fmp4_initialization_object(11, TEST_SOURCE_EPOCH, &init).unwrap();
        let delta = test_fmp4_part(
            11,
            1,
            false,
            2,
            2,
            None,
            Bytes::from_static(b"delta"),
            1_721_000_000_500_000_000,
        );
        let delta = build_fmp4_media_object(
            &delta,
            b"delta",
            initialization.key().clone(),
            epoch,
            TEST_SOURCE_EPOCH,
            1_000,
            500_000,
        )
        .unwrap();
        assert_eq!(delta.metadata().get("scheduler-class").unwrap(), b"audio");
        assert_eq!(
            relay_session::priority_for_object(&delta),
            relay_session::MediaPriority::Audio
        );

        let key = test_fmp4_part(
            11,
            2,
            true,
            2,
            2,
            None,
            Bytes::from_static(b"key"),
            1_721_000_000_600_000_000,
        );
        let key = build_fmp4_media_object(
            &key,
            b"key",
            initialization.key().clone(),
            epoch,
            TEST_SOURCE_EPOCH,
            1_000,
            500_000,
        )
        .unwrap();
        assert_eq!(
            key.metadata().get("scheduler-class").unwrap(),
            b"video-keyframe"
        );
        assert_eq!(
            relay_session::priority_for_object(&key),
            relay_session::MediaPriority::VideoKey
        );
    }

    #[tokio::test]
    async fn relay_session_routes_source_and_secondary_repair_and_recovers_exact_object() {
        let primary_receiver = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let secondary_receiver = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let primary_sender_reservation = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
        let secondary_sender_reservation = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
        let primary_bind = primary_sender_reservation.local_addr().unwrap();
        let secondary_bind = secondary_sender_reservation.local_addr().unwrap();
        drop((primary_sender_reservation, secondary_sender_reservation));
        let mut args = contrib_status_args();
        args.relay_primary_target = Some(primary_receiver.local_addr().unwrap());
        args.relay_primary_bind = Some(primary_bind);
        args.relay_secondary_target = Some(secondary_receiver.local_addr().unwrap());
        args.relay_secondary_bind = Some(secondary_bind);
        args.relay_secondary_seed_source = true;
        args.relay_topology_generation = 7;
        args.relay_subscription_id = 19;
        args.relay_deadline_ms = 5_000;
        args.relay_path_loss_fraction = 0.10;
        args.relay_path_best_direct_rtt_ms = 246.7;
        args.relay_path_rtt_ms = 253.0;
        args.relay_path_jitter_ms = 1.5;
        args.relay_path_queue_delay_ms = 2.0;
        args.relay_path_observed_at_unix_ms = Some(now_unix_ms());
        args.relay_secondary_path_loss_fraction = 0.02;
        args.relay_secondary_path_best_direct_rtt_ms = 246.7;
        args.relay_secondary_path_rtt_ms = 261.5;
        args.relay_secondary_path_jitter_ms = 0.7;
        args.relay_secondary_path_queue_delay_ms = 1.0;
        args.relay_secondary_path_observed_at_unix_ms = Some(now_unix_ms());

        let telemetry = Arc::new(IngestTelemetry::default());
        let relay = RelaySessionPublisher::new(&args, Arc::clone(&telemetry))
            .await
            .unwrap()
            .unwrap();
        let sender_addr = relay.primary.local_addr().unwrap();
        assert_eq!(sender_addr, primary_bind);
        assert_eq!(
            relay.secondary.as_ref().unwrap().local_addr().unwrap(),
            secondary_bind
        );
        assert_eq!(relay.primary.path_metrics().rtt_ms, 253.0);
        assert_eq!(
            relay.secondary.as_ref().unwrap().path_metrics().rtt_ms,
            261.5
        );
        let media = Bytes::from(
            (0..24_000)
                .map(|index| ((index * 31 + 17) % 251) as u8)
                .collect::<Vec<_>>(),
        );
        let init = Bytes::from_static(b"ftypmoov");
        let bundled = encode_mesh_fmp4_slot(Some(&init), &media).unwrap();
        let (initialization, configuration_epoch) =
            build_fmp4_initialization_object(77, TEST_SOURCE_EPOCH, &init).unwrap();
        let part = test_fmp4_part(
            77,
            42,
            true,
            8,
            8,
            Some(init),
            media,
            i64::try_from(now_unix_us().saturating_mul(1_000).saturating_add(123)).unwrap(),
        );
        let object = build_fmp4_media_object(
            &part,
            &bundled,
            initialization.key().clone(),
            configuration_epoch,
            TEST_SOURCE_EPOCH,
            args.relay_deadline_ms,
            1_000_000,
        )
        .unwrap();
        let exact_envelope = media_object::encode(&object).unwrap();

        let outcome = relay.publish_object(&object).await.unwrap();
        let canonical_deadline_ns =
            u64::try_from(object.deadline().unwrap().unix_time_ns()).unwrap();
        assert_eq!(
            outcome.announcement.deadline,
            MediaDeadline::from_micros(canonical_deadline_ns.div_ceil(1_000))
        );
        assert_eq!(relay.primary.local_addr().unwrap(), sender_addr);
        assert!(outcome.source_symbols > 5);
        assert!(outcome.repair_symbols > 0);
        assert!(
            outcome.repair_symbols.saturating_mul(4) >= outcome.source_symbols,
            "10% observed loss should raise keyframe repair protection above 25%"
        );

        async fn receive_symbols(
            socket: &UdpSocket,
            count: usize,
        ) -> Vec<relay_session::RelayDatagram> {
            let limits = RelayLimits::default();
            let mut wire = vec![0u8; limits.max_datagram_bytes];
            let mut symbols = Vec::with_capacity(count);
            for _ in 0..count {
                let (len, _) = timeout(Duration::from_secs(3), socket.recv_from(&mut wire))
                    .await
                    .expect("RelaySession datagram timeout")
                    .expect("RelaySession UDP receive");
                symbols.push(
                    relay_session::decode_datagram(&wire[..len], limits)
                        .expect("RelaySession wire decode"),
                );
            }
            symbols
        }

        let primary = receive_symbols(&primary_receiver, outcome.source_symbols).await;
        let secondary = receive_symbols(
            &secondary_receiver,
            outcome.source_symbols + outcome.repair_symbols,
        )
        .await;
        let (secondary_source, secondary_repair): (Vec<_>, Vec<_>) = secondary
            .into_iter()
            .partition(|symbol| symbol.role == MediaDatagramRole::Source);
        assert!(primary.iter().all(|symbol| {
            symbol.object_key == *object.key()
                && symbol.role == MediaDatagramRole::Source
                && symbol.path_intent == relay_session::MediaPathIntent::PrimarySource
                && symbol.generation.get() == 7
                && symbol.subscription_id.get() == 19
                && !symbol.deadline.is_expired_at(now_unix_us())
        }));
        assert_eq!(secondary_source.len(), primary.len());
        assert!(secondary_source
            .iter()
            .zip(&primary)
            .all(|(warm, primary)| {
                warm.object_key == primary.object_key
                    && warm.role == MediaDatagramRole::Source
                    && warm.path_intent == relay_session::MediaPathIntent::PrimarySource
                    && warm.coding == primary.coding
                    && warm.fec_datagram == primary.fec_datagram
            }));
        assert!(secondary_repair.iter().all(|symbol| {
            symbol.object_key == *object.key()
                && symbol.role == MediaDatagramRole::Repair
                && symbol.path_intent == relay_session::MediaPathIntent::SecondaryRepair
                && symbol.coding == primary[0].coding
                && symbol.deadline == primary[0].deadline
        }));

        let mut assembler =
            relay_session::ObjectAssembler::new(outcome.announcement, RelayLimits::default())
                .unwrap();
        let mut recovered = None;
        for symbol in primary.iter().skip(1) {
            recovered = assembler.push_symbol(symbol).unwrap();
            if recovered.is_some() {
                break;
            }
        }
        for symbol in &secondary_repair {
            if recovered.is_none() {
                recovered = assembler.push_symbol(symbol).unwrap();
            }
        }
        let recovered = recovered.expect("secondary repair recovers the lost source symbol");
        assert_eq!(recovered, object);
        assert_eq!(media_object::encode(&recovered).unwrap(), exact_envelope);

        let snapshot = telemetry.snapshot();
        assert_eq!(snapshot.relay_session.objects_sent, 1);
        assert_eq!(
            snapshot.relay_session.source_datagrams,
            (primary.len() + secondary_source.len()) as u64
        );
        assert_eq!(
            snapshot.relay_session.repair_datagrams,
            secondary_repair.len() as u64
        );
        assert_eq!(snapshot.relay_session.source_errors, 0);
        assert_eq!(snapshot.relay_session.repair_errors, 0);
        assert_eq!(snapshot.relay_session.repair_primary_fallback_objects, 0);
        assert_eq!(snapshot.relay_session.primary_lane_objects_succeeded, 1);
        assert_eq!(snapshot.relay_session.primary_lane_objects_failed, 0);
        assert_eq!(snapshot.relay_session.primary_lane_state, "healthy");
        assert_eq!(snapshot.relay_session.secondary_lane_objects_succeeded, 1);
        assert_eq!(snapshot.relay_session.secondary_lane_objects_failed, 0);
        assert_eq!(snapshot.relay_session.secondary_lane_state, "healthy");
        assert_eq!(snapshot.relay_session.surviving_lane_objects, 0);
        assert_eq!(snapshot.relay_session.all_lanes_failed_objects, 0);
        assert_eq!(snapshot.relay_session.deadline_hits, 1);
        assert_eq!(snapshot.relay_session.deadline_misses, 0);
        assert_eq!(snapshot.relay_session.expired_objects, 0);
        assert_eq!(snapshot.relay_session.expired_symbols, 0);
        assert_eq!(snapshot.relay_session.stages.total.count, 1);
        assert_eq!(snapshot.relay_session.stages.encode_wait.count, 1);
        assert_eq!(snapshot.relay_session.stages.encode.count, 1);
        assert_eq!(snapshot.relay_session.stages.schedule.count, 1);
        assert_eq!(
            snapshot.relay_session.stages.primary_source_send.count,
            primary.len() as u64
        );
        assert_eq!(
            snapshot.relay_session.stages.secondary_source_send.count,
            secondary_source.len() as u64
        );
        assert_eq!(
            snapshot.relay_session.stages.secondary_repair_send.count,
            secondary_repair.len() as u64
        );
        assert_eq!(snapshot.relay_session.stages.primary_repair_send.count, 0);
        assert!(snapshot
            .relay_session
            .last_deadline_unix_us
            .is_some_and(|deadline| deadline > now_unix_us()));
        assert!(snapshot
            .relay_session
            .last_deadline_headroom_us
            .is_some_and(|headroom| headroom > 0));

        telemetry
            .media_object_source_epoch
            .store(TEST_SOURCE_EPOCH, Ordering::Release);
        let status_config = ContribStatusConfig::from_args(&args, Arc::clone(&telemetry));
        let relay_metrics = String::from_utf8(status_config.prometheus_metrics().to_vec()).unwrap();
        assert!(relay_metrics
            .contains("av_contrib_relay_session_carrier_configured{path=\"primary\"} 1\n"));
        assert!(relay_metrics
            .contains("av_contrib_relay_session_carrier_configured{path=\"secondary\"} 1\n"));
        assert!(relay_metrics.contains("av_contrib_relay_session_last_deadline_seconds "));
        assert!(relay_metrics.contains("av_contrib_relay_session_last_deadline_headroom_seconds "));
        assert!(relay_metrics.contains("av_contrib_media_object_clock_estimated_error_seconds 1\n"));
        assert!(relay_metrics.contains(&format!(
            "av_contrib_media_object_source_epoch {TEST_SOURCE_EPOCH}\n"
        )));
        assert!(relay_metrics.contains(
            "av_contrib_relay_session_path_observation_info{source=\"controller-seeded\"} 1\n"
        ));
        let metric_value = |name: &str| {
            relay_metrics
                .lines()
                .find_map(|line| line.strip_prefix(&format!("{name} ")))
                .and_then(|value| value.parse::<f64>().ok())
                .unwrap_or_else(|| panic!("missing numeric metric {name}"))
        };
        assert!((metric_value("av_contrib_relay_session_path_loss_fraction") - 0.1).abs() < 1e-6);
        assert!(
            (metric_value("av_contrib_relay_session_path_stretch_ratio") - (253.0 / 246.7)).abs()
                < 1e-6
        );
        assert!((metric_value("av_contrib_relay_session_path_rtt_seconds") - 0.253).abs() < 1e-6);
        assert!(relay_metrics.contains("av_contrib_relay_session_path_observation_age_seconds "));
        assert!(relay_metrics
            .contains("av_contrib_relay_session_route_rtt_seconds{path=\"primary\"} 0.253\n"));
        assert!(relay_metrics
            .contains("av_contrib_relay_session_route_rtt_seconds{path=\"secondary\"} 0.2615\n"));
        assert!(relay_metrics
            .contains("av_contrib_relay_session_route_loss_fraction{path=\"secondary\"} 0.02\n"));
        assert!(relay_metrics
            .contains("av_contrib_relay_session_deadline_objects_total{outcome=\"hit\"} 1\n"));
        assert!(relay_metrics
            .contains("av_contrib_relay_session_deadline_objects_total{outcome=\"miss\"} 0\n"));
        assert!(relay_metrics.contains(
            "av_contrib_relay_session_stage_duration_seconds_count{stage=\"total\"} 1\n"
        ));
        assert!(relay_metrics.contains(&format!(
            "av_contrib_relay_session_stage_duration_seconds_count{{stage=\"send_secondary_repair\"}} {}\n",
            secondary_repair.len()
        )));
        assert!(relay_metrics.contains(
            "av_contrib_relay_session_lane_objects_total{path=\"primary\",outcome=\"success\"} 1\n"
        ));
        assert!(relay_metrics.contains(
            "av_contrib_relay_session_lane_objects_total{path=\"secondary\",outcome=\"failure\"} 0\n"
        ));
        assert!(relay_metrics.contains(
            "av_contrib_relay_session_lane_health{path=\"primary\",state=\"healthy\"} 1\n"
        ));
        assert!(relay_metrics.contains(
            "av_contrib_relay_session_lane_health{path=\"primary\",state=\"impaired\"} 0\n"
        ));
        assert!(relay_metrics.contains("av_contrib_relay_session_surviving_lane_objects_total 0\n"));
        assert!(
            relay_metrics.contains("av_contrib_relay_session_all_lanes_failed_objects_total 0\n")
        );
        let status = status_config.snapshot();
        assert!(status.mesh.relay_primary_configured);
        assert!(status.mesh.relay_secondary_configured);
        assert_eq!(status.mesh.relay_carrier, Some("private-udp"));
        assert!(status.mesh.relay_secondary_source_seeded);
        assert_eq!(
            status.mesh.relay_primary_target.as_deref(),
            args.relay_primary_target
                .map(|target| target.to_string())
                .as_deref()
        );
        assert_eq!(
            status.mesh.relay_primary_bind.as_deref(),
            Some(primary_bind.to_string()).as_deref()
        );
        assert_eq!(
            status.mesh.relay_secondary_bind.as_deref(),
            Some(secondary_bind.to_string()).as_deref()
        );
        assert_eq!(status.mesh.relay_deadline_ms, 5_000);
        assert_eq!(
            status.mesh.relay_path_observation_source,
            "controller-seeded"
        );
        assert_eq!(status.mesh.relay_path_loss_fraction, 0.10);
        assert_eq!(status.mesh.relay_path_best_direct_rtt_ms, 246.7);
        assert_eq!(status.mesh.relay_path_rtt_ms, 253.0);
        assert!(status.mesh.relay_path_observed_at_unix_ms.is_some());
        assert_eq!(
            status.mesh.relay_secondary_path_observation_source,
            "controller-seeded"
        );
        assert_eq!(status.mesh.relay_secondary_path_loss_fraction, 0.02);
        assert_eq!(status.mesh.relay_secondary_path_rtt_ms, 261.5);
        assert!(status
            .mesh
            .relay_secondary_path_observed_at_unix_ms
            .is_some());
        assert_eq!(status.mesh.media_object_clock_id, AV_CONTRIB_CLOCK_ID);
        assert_eq!(status.mesh.media_object_clock_confidence, "estimated");
        assert_eq!(status.mesh.media_object_clock_estimated_error_ms, 1_000);
    }

    #[test]
    fn relay_session_lane_resolution_preserves_independent_parent_availability() {
        assert!(!resolve_relay_lane_results(Ok(()), Some(Ok(())), true).unwrap());
        assert!(resolve_relay_lane_results(
            Ok(()),
            Some(Err(anyhow::anyhow!("secondary unavailable"))),
            true,
        )
        .unwrap());
        assert!(resolve_relay_lane_results(
            Err(anyhow::anyhow!("primary unavailable")),
            Some(Ok(())),
            true,
        )
        .unwrap());
        assert!(resolve_relay_lane_results(
            Err(anyhow::anyhow!("primary unavailable")),
            Some(Ok(())),
            false,
        )
        .unwrap_err()
        .to_string()
        .contains("repair-only"));
        assert!(resolve_relay_lane_results(
            Err(anyhow::anyhow!("primary unavailable")),
            Some(Err(anyhow::anyhow!("secondary unavailable"))),
            true,
        )
        .unwrap_err()
        .to_string()
        .contains("all configured RelaySession lanes failed"));
        assert!(resolve_relay_lane_results(
            Err(anyhow::anyhow!("primary unavailable")),
            None,
            false,
        )
        .unwrap_err()
        .to_string()
        .contains("only configured RelaySession lane failed"));
    }

    #[test]
    fn relay_path_observations_reject_invalid_controller_inputs() {
        let mut args = contrib_status_args();
        args.relay_path_loss_fraction = 1.01;
        assert!(relay_path_metrics(&args)
            .unwrap_err()
            .to_string()
            .contains("between zero and one"));

        args.relay_path_loss_fraction = 0.0;
        args.relay_path_rtt_ms = f32::NAN;
        assert!(relay_path_metrics(&args)
            .unwrap_err()
            .to_string()
            .contains("finite non-negative"));

        args.relay_path_rtt_ms = 0.0;
        args.relay_secondary_path_loss_fraction = 1.01;
        assert!(relay_secondary_path_metrics(&args)
            .unwrap_err()
            .to_string()
            .contains("between zero and one"));
    }

    #[test]
    fn adaptive_repair_accounts_for_both_independent_paths() {
        let combined = adaptive_relay_path_metrics(
            PathMetrics {
                loss_fraction: 0.02,
                rtt_ms: 244.0,
                jitter_ms: 0.8,
                deadline_hit_fraction: 0.999,
                ..PathMetrics::default()
            },
            PathMetrics {
                loss_fraction: 0.01,
                rtt_ms: 251.0,
                jitter_ms: 0.6,
                deadline_hit_fraction: 0.998,
                ..PathMetrics::default()
            },
        );

        assert!((combined.loss_fraction - 0.0298).abs() < 0.000_001);
        assert_eq!(combined.rtt_ms, 251.0);
        assert_eq!(combined.jitter_ms, 0.8);
        assert_eq!(combined.deadline_hit_fraction, 0.998);
    }

    #[tokio::test]
    async fn relay_session_drops_an_expired_object_before_encoding_and_counts_one_miss() {
        let receiver = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let mut args = contrib_status_args();
        args.relay_primary_target = Some(receiver.local_addr().unwrap());
        let telemetry = Arc::new(IngestTelemetry::default());
        let relay = RelaySessionPublisher::new(&args, Arc::clone(&telemetry))
            .await
            .unwrap()
            .unwrap();
        let payload = Bytes::from_static(b"expired-media");
        let (initialization, configuration_epoch) =
            build_fmp4_initialization_object(8, TEST_SOURCE_EPOCH, b"expired-init").unwrap();
        let published_at_unix_ns = i64::try_from(
            now_unix_us()
                .saturating_sub(2_000_000)
                .saturating_mul(1_000),
        )
        .unwrap();
        let part = test_fmp4_part(
            8,
            1,
            false,
            1,
            0,
            None,
            payload.clone(),
            published_at_unix_ns,
        );
        let object = build_fmp4_media_object(
            &part,
            &payload,
            initialization.key().clone(),
            configuration_epoch,
            TEST_SOURCE_EPOCH,
            1,
            1_000,
        )
        .unwrap();

        let error = relay.publish_object(&object).await.unwrap_err();
        assert!(error
            .to_string()
            .contains("expired before RelaySession encoding"));
        let snapshot = telemetry.snapshot().relay_session;
        assert_eq!(snapshot.objects_sent, 0);
        assert_eq!(snapshot.deadline_hits, 0);
        assert_eq!(snapshot.deadline_misses, 1);
        assert_eq!(snapshot.expired_objects, 1);
        assert_eq!(snapshot.stages.total.count, 1);
        assert_eq!(snapshot.stages.encode.count, 0);
    }

    #[tokio::test]
    async fn relay_session_uses_primary_repair_fallback_for_single_parent() {
        let receiver = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let mut args = contrib_status_args();
        args.relay_primary_target = Some(receiver.local_addr().unwrap());
        let telemetry = Arc::new(IngestTelemetry::default());
        let relay = RelaySessionPublisher::new(&args, Arc::clone(&telemetry))
            .await
            .unwrap()
            .unwrap();
        let payload = vec![0x5a; 12_000];
        let (initialization, configuration_epoch) =
            build_fmp4_initialization_object(8, TEST_SOURCE_EPOCH, b"single-parent-init").unwrap();
        let part = test_fmp4_part(
            8,
            3,
            true,
            3,
            0,
            None,
            Bytes::copy_from_slice(&payload),
            i64::try_from(now_unix_us().saturating_mul(1_000)).unwrap(),
        );
        let object = build_fmp4_media_object(
            &part,
            &payload,
            initialization.key().clone(),
            configuration_epoch,
            TEST_SOURCE_EPOCH,
            5_000,
            1_000_000,
        )
        .unwrap();

        let outcome = relay.publish_object(&object).await.unwrap();
        assert!(outcome.repair_symbols > 0);
        let expected = outcome.source_symbols + outcome.repair_symbols;
        let limits = RelayLimits::default();
        let mut wire = vec![0u8; limits.max_datagram_bytes];
        let mut source = 0;
        let mut repair = 0;
        for _ in 0..expected {
            let (len, _) = timeout(Duration::from_secs(3), receiver.recv_from(&mut wire))
                .await
                .unwrap()
                .unwrap();
            match relay_session::decode_datagram(&wire[..len], limits)
                .unwrap()
                .role
            {
                MediaDatagramRole::Source => source += 1,
                MediaDatagramRole::Repair => repair += 1,
            }
        }
        assert_eq!(source, outcome.source_symbols);
        assert_eq!(repair, outcome.repair_symbols);
        assert_eq!(
            telemetry
                .relay_repair_primary_fallback_objects
                .load(Ordering::Relaxed),
            1
        );
    }

    #[tokio::test]
    async fn relay_session_rejects_unpairable_fixed_endpoints() {
        let mut args = contrib_status_args();
        let endpoint = SocketAddr::from((Ipv4Addr::LOCALHOST, 31_001));
        args.relay_primary_target = Some(endpoint);
        args.relay_primary_bind = Some(endpoint);
        let error = RelaySessionPublisher::new(&args, Arc::new(IngestTelemetry::default()))
            .await
            .err()
            .expect("duplicate bind and target must be rejected");
        assert!(error.to_string().contains("distinct socket addresses"));

        assert!(relay_bind_addr(
            "--relay-primary-bind",
            Some(SocketAddr::from((Ipv4Addr::LOCALHOST, 0))),
            endpoint,
        )
        .unwrap_err()
        .to_string()
        .contains("fixed non-zero port"));
        assert!(relay_bind_addr(
            "--relay-primary-bind",
            Some(SocketAddr::from((Ipv4Addr::UNSPECIFIED, 31_002))),
            endpoint,
        )
        .unwrap_err()
        .to_string()
        .contains("source IP"));
    }

    #[test]
    fn parallel_stream_encoder_reserves_exact_unique_wire_ids() {
        let stream_id = 77;
        let symbol_size = 64;
        let repair_symbols = 2;
        let payload = vec![0x5a; usize::from(DEFAULT_SOURCE_SYMBOLS) * 64 * 2 + 17];
        let geometry = stream_fec_geometry(payload.len(), symbol_size, repair_symbols).unwrap();
        let datagrams =
            encode_stream_fec_payload(stream_id, &payload, repair_symbols, symbol_size, 10, 100)
                .unwrap();

        assert_eq!(geometry.block_count, 1);
        assert_eq!(geometry.packet_count as usize, datagrams.len());
        let mut block_ids = HashSet::new();
        let mut packet_sequences = HashSet::new();
        for datagram in &datagrams {
            let (decoded_stream_id, raw) = split_stream_id_prefix(datagram).unwrap();
            assert_eq!(decoded_stream_id, stream_id);
            let header = DatagramFecHeader::decode(raw).unwrap();
            header.payload(raw).unwrap();
            block_ids.insert(header.block_id);
            packet_sequences.insert(header.packet_sequence);
        }
        assert_eq!(block_ids, HashSet::from([10]));
        assert_eq!(packet_sequences.len(), datagrams.len());
        assert_eq!(packet_sequences.iter().min(), Some(&100));
        assert_eq!(
            packet_sequences.iter().max(),
            Some(&(100 + datagrams.len() as u32 - 1))
        );

        let mut decoder = FecDatagramDecoder::webtransport_with_stream_prefix(stream_id);
        let decoded = datagrams
            .iter()
            .skip(1)
            .filter_map(|datagram| decoder.push_datagram(datagram).unwrap())
            .collect::<Vec<_>>();
        assert_eq!(decoded, vec![payload]);
    }

    #[test]
    fn stream_fec_geometry_rejects_objects_beyond_raptorq_kmax() {
        let error = stream_fec_geometry(
            usize::try_from(MAX_SOURCE_SYMBOLS_PER_BLOCK).unwrap() + 1,
            1,
            1,
        )
        .unwrap_err();
        assert!(error.to_string().contains("too large"));
    }

    #[test]
    fn stream_fec_geometry_enforces_application_object_and_datagram_bounds() {
        let object_error =
            stream_fec_geometry(MAX_STREAM_FEC_OBJECT_BYTES + 1, 1_316, 1).unwrap_err();
        assert!(object_error.to_string().contains("too large"));

        let datagram_error = stream_fec_geometry(32_768, 1, 8).unwrap_err();
        assert!(datagram_error.to_string().contains("RaptorQ datagrams"));
    }

    #[test]
    fn stream_fec_roundtrips_one_large_object_after_source_loss_and_reordering() {
        let stream_id = 77;
        let payload = (0..6_001)
            .map(|index| ((index * 31 + 17) % 251) as u8)
            .collect::<Vec<_>>();
        let datagrams =
            encode_stream_fec_payload(stream_id, &payload, 1, 1_316, 40, 1_000).unwrap();
        assert_eq!(datagrams.len(), 7);
        assert!(datagrams.iter().all(|datagram| {
            let (_, raw) = split_stream_id_prefix(datagram).unwrap();
            DatagramFecHeader::decode(raw).unwrap().block_id == 40
        }));

        let mut decoder = FecDatagramDecoder::webtransport_with_stream_prefix(stream_id);
        let decoded = datagrams
            .iter()
            .skip(1)
            .rev()
            .filter_map(|datagram| decoder.push_datagram(datagram).unwrap())
            .collect::<Vec<_>>();
        assert_eq!(decoded, vec![payload]);
    }

    fn contrib_status_args() -> Args {
        Args {
            http_port: 0,
            cert: None,
            key: None,
            mesh_fec_target: SocketAddr::from((Ipv4Addr::LOCALHOST, 22_001)),
            mesh_media_fec_target: SocketAddr::from((Ipv4Addr::LOCALHOST, 22_101)),
            relay_primary_target: None,
            relay_primary_bind: None,
            relay_secondary_target: None,
            relay_secondary_bind: None,
            relay_secondary_seed_source: false,
            relay_exclusive: false,
            relay_local_id: "av-contrib-test".to_owned(),
            relay_primary_id: "primary-test".to_owned(),
            relay_secondary_id: "secondary-test".to_owned(),
            relay_topology_generation: DEFAULT_RELAY_TOPOLOGY_GENERATION,
            relay_subscription_id: DEFAULT_RELAY_SUBSCRIPTION_ID,
            relay_deadline_ms: DEFAULT_RELAY_DEADLINE_MS,
            relay_path_loss_fraction: 0.0,
            relay_path_best_direct_rtt_ms: 0.0,
            relay_path_rtt_ms: 0.0,
            relay_path_jitter_ms: 0.0,
            relay_path_queue_delay_ms: 0.0,
            relay_path_observed_at_unix_ms: None,
            relay_secondary_path_loss_fraction: 0.0,
            relay_secondary_path_best_direct_rtt_ms: 0.0,
            relay_secondary_path_rtt_ms: 0.0,
            relay_secondary_path_jitter_ms: 0.0,
            relay_secondary_path_queue_delay_ms: 0.0,
            relay_secondary_path_observed_at_unix_ms: None,
            wall_clock_estimated_error_ms: DEFAULT_WALL_CLOCK_ESTIMATED_ERROR_MS,
            daw_media_bind: None,
            daw_hls_queue_capacity: DEFAULT_AUDIO_EPOCH_HLS_QUEUE_CAPACITY,
            stream_id: 1,
            rist_stream_id: 1,
            srt_stream_id: 1,
            rtmp_stream_id: 1,
            repair_symbols: 3,
            symbol_size: DEFAULT_SYMBOL_SIZE,
            rist_bind: Some(SocketAddr::from((Ipv4Addr::LOCALHOST, 27_000))),
            rist_profile: RistProfile::Main,
            rist_backend: RistBackend::Pure,
            rist_flow_id: DEFAULT_FLOW_ID,
            srt_bind: Some(SocketAddr::from((Ipv4Addr::LOCALHOST, 27_001))),
            rtmp_bind: Some(SocketAddr::from((Ipv4Addr::LOCALHOST, 19_350))),
            fmp4_part_ms: 50,
            fmp4_segment_ms: DEFAULT_SEGMENT_MS,
            hls_target_duration_ms: DEFAULT_TARGET_DURATION_MS,
            playlist_count: 65,
            playlist_buffer_kb: 800,
        }
    }

    #[test]
    fn contrib_status_uses_browser_safe_stream_ids() {
        let args = Args {
            http_port: 0,
            cert: None,
            key: None,
            mesh_fec_target: SocketAddr::from((Ipv4Addr::LOCALHOST, 22_001)),
            mesh_media_fec_target: SocketAddr::from((Ipv4Addr::LOCALHOST, 22_101)),
            relay_primary_target: None,
            relay_primary_bind: None,
            relay_secondary_target: None,
            relay_secondary_bind: None,
            relay_secondary_seed_source: false,
            relay_exclusive: false,
            relay_local_id: "av-contrib-test".to_owned(),
            relay_primary_id: "primary-test".to_owned(),
            relay_secondary_id: "secondary-test".to_owned(),
            relay_topology_generation: DEFAULT_RELAY_TOPOLOGY_GENERATION,
            relay_subscription_id: DEFAULT_RELAY_SUBSCRIPTION_ID,
            relay_deadline_ms: DEFAULT_RELAY_DEADLINE_MS,
            relay_path_loss_fraction: 0.0,
            relay_path_best_direct_rtt_ms: 0.0,
            relay_path_rtt_ms: 0.0,
            relay_path_jitter_ms: 0.0,
            relay_path_queue_delay_ms: 0.0,
            relay_path_observed_at_unix_ms: None,
            relay_secondary_path_loss_fraction: 0.0,
            relay_secondary_path_best_direct_rtt_ms: 0.0,
            relay_secondary_path_rtt_ms: 0.0,
            relay_secondary_path_jitter_ms: 0.0,
            relay_secondary_path_queue_delay_ms: 0.0,
            relay_secondary_path_observed_at_unix_ms: None,
            wall_clock_estimated_error_ms: DEFAULT_WALL_CLOCK_ESTIMATED_ERROR_MS,
            daw_media_bind: None,
            daw_hls_queue_capacity: DEFAULT_AUDIO_EPOCH_HLS_QUEUE_CAPACITY,
            stream_id: 9_007_199_254_741_993,
            rist_stream_id: 9_007_199_254_741_994,
            srt_stream_id: 9_007_199_254_741_995,
            rtmp_stream_id: 9_007_199_254_741_996,
            repair_symbols: 3,
            symbol_size: DEFAULT_SYMBOL_SIZE,
            rist_bind: Some(SocketAddr::from((Ipv4Addr::LOCALHOST, 27_000))),
            rist_profile: RistProfile::Main,
            rist_backend: RistBackend::Pure,
            rist_flow_id: DEFAULT_FLOW_ID,
            srt_bind: Some(SocketAddr::from((Ipv4Addr::LOCALHOST, 27_001))),
            rtmp_bind: None,
            fmp4_part_ms: 50,
            fmp4_segment_ms: DEFAULT_SEGMENT_MS,
            hls_target_duration_ms: DEFAULT_TARGET_DURATION_MS,
            playlist_count: 65,
            playlist_buffer_kb: 800,
        };

        let telemetry = Arc::new(IngestTelemetry::default());
        telemetry.record_raw_http(args.stream_id, 2, 4096, 6);
        telemetry.record_media_access_unit(args.stream_id, 2048, 3);
        telemetry.record_mesh_forward_success(
            "stream",
            args.stream_id,
            args.mesh_fec_target,
            4096,
            6,
            6144,
        );
        telemetry.record_mesh_forward_success(
            "media",
            args.stream_id,
            args.mesh_media_fec_target,
            2048,
            3,
            3072,
        );
        telemetry.record_mpeg_ts_slot("srt", args.srt_stream_id, 1316);
        telemetry.record_mpeg_ts_continuity_issue(MpegTsContinuityIssue {
            stream_type: "h264",
            dropped_payload_bytes: 2048,
        });
        telemetry.record_mpeg_ts_payload_drop(MpegTsPayloadDrop {
            stream_type: "adts",
            bytes: 8192,
        });
        telemetry.record_rtmp_access_unit(args.rtmp_stream_id, 1024);
        telemetry.record_fmp4_tracks(args.rist_stream_id, 1, 9, Some(1280), Some(720), 2, 1);
        telemetry.record_fmp4_part(args.rist_stream_id, 1, 9, 8192, 512);
        let status_config = ContribStatusConfig::from_args(&args, telemetry);
        let event = status_config.sse_event().unwrap();
        assert!(event.starts_with(b"event: contrib\ndata: {"));
        assert!(event.ends_with(b"\n\n"));

        let snapshot = status_config.snapshot();

        assert_eq!(snapshot.default_stream_id, "9007199254741993");
        assert_eq!(snapshot.advertised_hls_stream_id, "9007199254741994");
        assert_eq!(
            snapshot.advertised_hls_path,
            "/9007199254741994/stream.m3u8"
        );
        assert_eq!(snapshot.mesh.byte_fec_target, "127.0.0.1:22001");
        assert_eq!(snapshot.hls.part_target_ms, 50);
        assert_eq!(snapshot.fec.repair_symbols, 3);
        assert_eq!(snapshot.runtime.raw_http.requests, 1);
        assert_eq!(snapshot.runtime.raw_http.bytes, 4096);
        assert_eq!(snapshot.runtime.media_access_units.requests, 1);
        assert_eq!(snapshot.runtime.mesh_forward.stream_payloads, 1);
        assert_eq!(snapshot.runtime.mesh_forward.stream_datagrams, 6);
        assert_eq!(snapshot.runtime.mesh_forward.stream_datagram_bytes, 6144);
        assert_eq!(snapshot.runtime.mesh_forward.media_payloads, 1);
        assert_eq!(snapshot.runtime.mesh_forward.media_datagrams, 3);
        assert_eq!(snapshot.runtime.mesh_forward.media_datagram_bytes, 3072);
        assert_eq!(snapshot.runtime.mpeg_ts.slots, 1);
        assert_eq!(snapshot.runtime.mpeg_ts.continuity_errors, 1);
        assert_eq!(snapshot.runtime.mpeg_ts.continuity_dropped_bytes, 2048);
        assert_eq!(snapshot.runtime.mpeg_ts.payload_drops, 1);
        assert_eq!(snapshot.runtime.mpeg_ts.payload_drop_bytes, 8192);
        let srt_protocol = snapshot
            .runtime
            .protocols
            .iter()
            .find(|protocol| protocol.protocol == "srt")
            .expect("missing SRT protocol runtime");
        assert_eq!(srt_protocol.units, 1);
        assert_eq!(srt_protocol.bytes, 1316);
        assert!(srt_protocol.last_seen_age_ms.is_some());
        assert!(
            snapshot
                .alerts
                .iter()
                .any(|alert| alert.code == "mpeg_ts_input_damage"
                    && alert.protocol == Some("mpeg-ts"))
        );
        assert_eq!(snapshot.runtime.rtmp.access_units, 1);
        let rtmp_protocol = snapshot
            .runtime
            .protocols
            .iter()
            .find(|protocol| protocol.protocol == "rtmp")
            .expect("missing RTMP protocol runtime");
        assert_eq!(rtmp_protocol.units, 1);
        assert_eq!(rtmp_protocol.bytes, 1024);
        assert_eq!(snapshot.runtime.fmp4.parts, 1);
        assert_eq!(snapshot.runtime.fmp4.init_bytes, 512);
        assert_eq!(snapshot.runtime.fmp4.video_codec, Some("h264"));
        assert_eq!(snapshot.runtime.fmp4.video_width, Some(1280));
        assert_eq!(snapshot.runtime.fmp4.video_height, Some(720));
        assert_eq!(snapshot.runtime.fmp4.video_parts, 1);
        assert_eq!(snapshot.runtime.fmp4.video_access_units, 2);
        assert_eq!(snapshot.runtime.fmp4.audio_codec, Some("aac"));
        assert_eq!(snapshot.runtime.fmp4.audio_parts, 1);
        assert_eq!(snapshot.runtime.fmp4.audio_access_units, 1);
        let raw_stream = snapshot
            .runtime
            .streams
            .iter()
            .find(|stream| stream.stream_id_text == "9007199254741993")
            .expect("missing raw HTTP stream runtime");
        assert_eq!(raw_stream.state, "forwarding");
        assert_eq!(raw_stream.input_units, 2);
        assert_eq!(raw_stream.input_bytes, 6144);
        assert_eq!(raw_stream.mesh_payloads, 2);
        assert_eq!(raw_stream.mesh_payload_bytes, 6144);
        assert_eq!(raw_stream.mesh_datagrams, 9);
        assert_eq!(raw_stream.mesh_datagram_bytes, 9216);
        let fmp4_stream = snapshot
            .runtime
            .streams
            .iter()
            .find(|stream| stream.stream_id_text == "9007199254741994")
            .expect("missing fMP4 stream runtime");
        assert_eq!(fmp4_stream.state, "publishing");
        assert_eq!(fmp4_stream.fmp4_parts, 1);
        assert_eq!(fmp4_stream.fmp4_bytes, 8192);
        assert_eq!(fmp4_stream.fmp4_init_bytes, 512);
        assert_eq!(fmp4_stream.latest_fmp4_sequence, Some(9));
        assert_eq!(fmp4_stream.video_codec, Some("h264"));
        assert_eq!(fmp4_stream.video_width, Some(1280));
        assert_eq!(fmp4_stream.video_height, Some(720));
        assert_eq!(fmp4_stream.video_parts, 1);
        assert_eq!(fmp4_stream.video_access_units, 2);
        assert_eq!(fmp4_stream.audio_codec, Some("aac"));
        assert_eq!(fmp4_stream.audio_parts, 1);
        assert_eq!(fmp4_stream.audio_access_units, 1);
        assert_eq!(snapshot.status, "active");
        assert_eq!(snapshot.health.state, "active");
        assert!(snapshot.health.input_seen);
        assert!(snapshot.health.output_seen);
        assert!(snapshot
            .activity
            .iter()
            .any(|activity| activity.code == "raw_http_ingest"
                && activity.stream_id_text.as_deref() == Some("9007199254741993")));
        assert!(snapshot
            .activity
            .iter()
            .any(|activity| activity.code == "fmp4_part_published"
                && activity.stream_id_text.as_deref() == Some("9007199254741994")
                && activity.sequence == Some(9)));

        let rist = snapshot
            .listeners
            .iter()
            .find(|listener| listener.protocol == "rist")
            .expect("missing RIST listener status");
        assert!(rist.enabled);
        assert_eq!(rist.output_stream_id, "9007199254741994");
        assert_eq!(rist.flow_id.as_deref(), Some("0x11223344"));
    }

    #[test]
    fn contrib_prometheus_metrics_expose_hot_path_counters_and_stream_health() {
        let args = contrib_status_args();
        let telemetry = Arc::new(IngestTelemetry::default());
        telemetry.record_raw_http(args.stream_id, 2, 4_096, 6);
        telemetry.record_mesh_forward_success(
            "stream",
            args.stream_id,
            args.mesh_fec_target,
            4_096,
            6,
            6_144,
        );
        telemetry.record_mesh_forward_duration("stream", Duration::from_micros(750));
        telemetry.record_mesh_forward_stage_duration(
            "stream",
            "encode_wait",
            Duration::from_micros(250),
        );
        telemetry.relay_objects_sent.fetch_add(1, Ordering::Relaxed);
        telemetry.record_relay_session_send_success(MediaDatagramRole::Source, 1_200);
        telemetry.record_relay_session_send_success(MediaDatagramRole::Repair, 1_200);
        telemetry.record_fmp4_part(args.stream_id, 1, 42, 8_192, 512);
        let status = ContribStatusConfig::from_args(&args, telemetry);

        let metrics = String::from_utf8(status.prometheus_metrics().to_vec()).unwrap();

        assert!(metrics.contains("# TYPE av_contrib_health gauge\n"));
        assert!(metrics.contains("av_contrib_health{state=\"active\"} 1\n"));
        assert!(metrics.contains("av_contrib_raw_http_bytes_total 4096\n"));
        assert!(metrics.contains("av_contrib_mesh_forward_datagrams_total{kind=\"stream\"} 6\n"));
        assert!(metrics.contains(
            "av_contrib_mesh_forward_duration_seconds_bucket{kind=\"stream\",le=\"0.001\"} 1\n"
        ));
        assert!(metrics
            .contains("av_contrib_mesh_forward_duration_seconds_sum{kind=\"stream\"} 0.00075\n"));
        assert!(metrics.contains(
            "av_contrib_mesh_forward_stage_duration_seconds_bucket{kind=\"stream\",stage=\"encode_wait\",le=\"0.00025\"} 1\n"
        ));
        assert!(metrics.contains(
            "av_contrib_stream_latest_fmp4_sequence{stream_id=\"1\",state=\"publishing\"} 42\n"
        ));
        assert!(metrics.contains("av_contrib_relay_session_objects_total 1\n"));
        assert!(
            metrics.contains("av_contrib_relay_session_carrier_configured{path=\"primary\"} 0\n")
        );
        assert!(metrics.contains("av_contrib_relay_session_deadline_budget_seconds 1\n"));
        assert!(metrics.contains("av_contrib_relay_session_datagrams_total{role=\"source\"} 1\n"));
        assert!(metrics.contains("av_contrib_relay_session_datagrams_total{role=\"repair\"} 1\n"));
        assert!(metrics
            .contains("av_contrib_relay_session_datagram_bytes_total{role=\"source\"} 1200\n"));
        assert_eq!(prometheus_label_value("node\\\"\n"), "node\\\\\\\"\\n");
    }

    #[tokio::test]
    async fn contrib_metrics_route_serves_prometheus_exposition() {
        let args = contrib_status_args();
        let telemetry = Arc::new(IngestTelemetry::default());
        telemetry.record_raw_http(args.stream_id, 1, 1_024, 2);
        let forwarder = Arc::new(
            MeshForwarder::new(&args, Arc::clone(&telemetry))
                .await
                .unwrap(),
        );
        let router = ContribRouter::new(
            forwarder,
            args.stream_id,
            Arc::new(HlsRouter::new()),
            Arc::new(ContribStatusConfig::from_args(&args, telemetry)),
            None,
        );
        let req = Request::builder()
            .method(Method::GET)
            .uri(CONTRIB_METRICS_PATH)
            .body(())
            .unwrap();

        let response = router.route(req).await.unwrap();

        assert_eq!(response.status, StatusCode::OK);
        assert_eq!(
            response.content_type.as_deref(),
            Some(PROMETHEUS_CONTENT_TYPE)
        );
        let metrics = String::from_utf8(response.body.unwrap().to_vec()).unwrap();
        assert!(metrics.contains("av_contrib_raw_http_bytes_total 1024\n"));
    }

    #[test]
    fn contrib_status_reports_waiting_stalled_and_stale_health() {
        let args = contrib_status_args();
        let stream_id_text = args.stream_id.to_string();
        let telemetry = Arc::new(IngestTelemetry::default());
        let status_config = ContribStatusConfig::from_args(&args, Arc::clone(&telemetry));

        let waiting = status_config.snapshot();
        assert_eq!(waiting.status, "waiting");
        assert_eq!(waiting.health.state, "waiting");
        assert!(!waiting.health.input_seen);
        assert!(waiting
            .alerts
            .iter()
            .any(|alert| alert.code == "waiting_for_input"
                && alert.stream_id_text.as_deref() == Some(stream_id_text.as_str())));

        telemetry.record_mpeg_ts_slot("srt", args.srt_stream_id, 1316);
        telemetry.mpeg_ts_last_unix_ms.store(
            now_unix_ms().saturating_sub(waiting.health.stale_threshold_ms + 1),
            Ordering::Relaxed,
        );
        let stalled = status_config.snapshot();
        assert_eq!(stalled.status, "stalled");
        assert!(stalled.health.fmp4_input_seen);
        assert!(!stalled.health.output_seen);
        assert!(stalled
            .alerts
            .iter()
            .any(|alert| alert.code == "fmp4_input_without_output"
                && alert.stream_id_text.as_deref() == Some(stream_id_text.as_str())));

        telemetry.record_fmp4_part(args.srt_stream_id, 1, 12, 4096, 512);
        telemetry.fmp4_last_publish_unix_ms.store(
            now_unix_ms().saturating_sub(stalled.health.stale_threshold_ms + 1),
            Ordering::Relaxed,
        );
        let stale = status_config.snapshot();
        assert_eq!(stale.status, "stale");
        assert!(stale.health.output_seen);
        assert!(stale
            .alerts
            .iter()
            .any(|alert| alert.code == "fmp4_output_stale"
                && alert.stream_id_text.as_deref() == Some(stream_id_text.as_str())));
    }

    #[test]
    fn contrib_status_reports_hls_response_errors() {
        let args = contrib_status_args();
        let stream_id_text = args.stream_id.to_string();
        let telemetry = Arc::new(IngestTelemetry::default());
        let status_config = ContribStatusConfig::from_args(&args, Arc::clone(&telemetry));
        let hls_response = response(StatusCode::NOT_FOUND, None, None);

        telemetry.record_hls_response(
            &Method::GET,
            "/1/stream.m3u8",
            Some("_HLS_msn=12&_HLS_part=1"),
            &hls_response,
        );
        let snapshot = status_config.snapshot();

        assert_eq!(snapshot.runtime.hls.responses_total, 1);
        assert_eq!(snapshot.runtime.hls.response_errors, 1);
        assert_eq!(snapshot.runtime.hls.response_not_found, 1);
        assert_eq!(
            snapshot.runtime.hls.recent_responses[0].path,
            "/1/stream.m3u8"
        );
        assert_eq!(snapshot.runtime.hls.recent_responses[0].status, 404);
        assert!(snapshot
            .alerts
            .iter()
            .any(|alert| alert.code == "hls_response_errors"
                && alert.stream_id_text.as_deref() == Some(stream_id_text.as_str())));
        assert!(snapshot
            .activity
            .iter()
            .any(|activity| activity.code == "hls_response_error"));
    }

    #[test]
    fn contrib_status_tracks_current_relay_lane_health_and_recovers() {
        let args = contrib_status_args();
        let telemetry = Arc::new(IngestTelemetry::default());
        let status_config = ContribStatusConfig::from_args(&args, Arc::clone(&telemetry));

        telemetry.record_fmp4_part(args.stream_id, 1, 12, 4_096, 512);
        telemetry.record_relay_session_lane_object(RelayCarrierPath::Primary, true);
        telemetry.record_relay_session_lane_object(RelayCarrierPath::Secondary, true);
        assert_eq!(status_config.snapshot().status, "active");

        telemetry.record_relay_session_send_error(MediaDatagramRole::Source);
        telemetry.record_relay_session_lane_object(RelayCarrierPath::Primary, false);
        let impaired = status_config.snapshot();
        assert_eq!(impaired.status, "degraded");
        assert_eq!(
            impaired.runtime.relay_session.primary_lane_state,
            "impaired"
        );
        assert_eq!(
            impaired.runtime.relay_session.secondary_lane_state,
            "healthy"
        );
        assert!(impaired
            .alerts
            .iter()
            .any(|alert| alert.code == "relay_lane_impaired"));

        telemetry.record_relay_session_lane_object(RelayCarrierPath::Primary, true);
        assert_eq!(status_config.snapshot().status, "degraded");

        telemetry.relay_primary_lane_last_failure_unix_ms.store(
            now_unix_ms().saturating_sub(RELAY_LANE_IMPAIRED_HOLD_MS + 1),
            Ordering::Release,
        );
        telemetry.record_relay_session_lane_object(RelayCarrierPath::Primary, true);
        let recovered = status_config.snapshot();
        assert_eq!(recovered.status, "active");
        assert_eq!(recovered.runtime.relay_session.source_errors, 1);
        assert_eq!(
            recovered.runtime.relay_session.primary_lane_objects_failed,
            1
        );
        assert_eq!(
            recovered.runtime.relay_session.primary_lane_state,
            "healthy"
        );
        assert!(!recovered
            .alerts
            .iter()
            .any(|alert| alert.code == "relay_lane_impaired"));
    }

    #[test]
    fn contrib_status_reports_mesh_forward_errors() {
        let args = contrib_status_args();
        let stream_id_text = args.stream_id.to_string();
        let telemetry = Arc::new(IngestTelemetry::default());
        let status_config = ContribStatusConfig::from_args(&args, Arc::clone(&telemetry));

        telemetry.record_mesh_forward_error(
            "stream",
            args.stream_id,
            args.mesh_fec_target,
            &anyhow::anyhow!("send queue closed"),
        );

        let snapshot = status_config.snapshot();
        assert_eq!(snapshot.status, "degraded");
        assert_eq!(snapshot.health.state, "degraded");
        assert_eq!(snapshot.runtime.mesh_forward.stream_errors, 1);
        assert!(snapshot
            .alerts
            .iter()
            .any(|alert| alert.code == "mesh_forward_error"
                && alert.stream_id_text.as_deref() == Some(stream_id_text.as_str())));
        assert!(snapshot
            .activity
            .iter()
            .any(|activity| activity.code == "mesh_forward_error"));
    }

    #[test]
    fn contrib_status_reports_ingest_sessions() {
        let args = contrib_status_args();
        let telemetry = Arc::new(IngestTelemetry::default());
        let status_config = ContribStatusConfig::from_args(&args, Arc::clone(&telemetry));

        telemetry.ensure_ingest_session(
            "srt",
            42,
            Some(args.srt_stream_id),
            Some(1),
            Some("127.0.0.1:5000".into()),
            Some("/srt/42".into()),
        );
        telemetry.record_ingest_session_body("srt", 42, 1316);
        telemetry.record_mpeg_ts_slot("srt", 42, 1316);
        let active = status_config.snapshot();

        assert_eq!(active.runtime.ingest_sessions.active, 1);
        assert_eq!(active.runtime.ingest_sessions.started, 1);
        assert_eq!(active.runtime.ingest_sessions.ended, 0);
        let protocol = active
            .runtime
            .protocols
            .iter()
            .find(|protocol| protocol.protocol == "srt")
            .expect("missing SRT protocol runtime");
        assert_eq!(protocol.units, 1);
        assert_eq!(protocol.bytes, 1316);
        assert_eq!(protocol.active_sessions, 1);
        assert_eq!(protocol.ended_sessions, 0);
        let session = &active.runtime.ingest_sessions.recent[0];
        assert_eq!(session.session_id, 1);
        assert_eq!(session.protocol, "srt");
        assert_eq!(session.stream_id_text, "42");
        assert_eq!(session.output_stream_id_text.as_deref(), Some("1"));
        assert_eq!(session.peer.as_deref(), Some("127.0.0.1:5000"));
        assert_eq!(session.path.as_deref(), Some("/srt/42"));
        assert_eq!(session.state, "active");
        assert_eq!(session.body_slots, 1);
        assert_eq!(session.bytes, 1316);
        assert!(active
            .activity
            .iter()
            .any(|activity| activity.code == "ingest_session_started"));

        telemetry.end_ingest_session("srt", 42, "ended");
        let ended = status_config.snapshot();
        assert_eq!(ended.runtime.ingest_sessions.active, 0);
        assert_eq!(ended.runtime.ingest_sessions.started, 1);
        assert_eq!(ended.runtime.ingest_sessions.ended, 1);
        let protocol = ended
            .runtime
            .protocols
            .iter()
            .find(|protocol| protocol.protocol == "srt")
            .expect("missing SRT protocol runtime");
        assert_eq!(protocol.active_sessions, 0);
        assert_eq!(protocol.ended_sessions, 1);
        assert_eq!(ended.runtime.ingest_sessions.recent[0].state, "ended");
        assert_eq!(
            ended.runtime.ingest_sessions.recent[0].end_reason,
            Some("ended")
        );
        assert!(ended
            .activity
            .iter()
            .any(|activity| activity.code == "ingest_session_ended"));
    }

    #[tokio::test]
    async fn authorized_stream_slot_forwarder_preserves_canonical_identity_and_datagram_limit() {
        let mesh_socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let mesh_target = mesh_socket.local_addr().unwrap();
        let args = Args {
            http_port: 0,
            cert: None,
            key: None,
            mesh_fec_target: mesh_target,
            mesh_media_fec_target: SocketAddr::from((Ipv4Addr::LOCALHOST, 0)),
            relay_primary_target: None,
            relay_primary_bind: None,
            relay_secondary_target: None,
            relay_secondary_bind: None,
            relay_secondary_seed_source: false,
            relay_exclusive: false,
            relay_local_id: "av-contrib-test".to_owned(),
            relay_primary_id: "primary-test".to_owned(),
            relay_secondary_id: "secondary-test".to_owned(),
            relay_topology_generation: DEFAULT_RELAY_TOPOLOGY_GENERATION,
            relay_subscription_id: DEFAULT_RELAY_SUBSCRIPTION_ID,
            relay_deadline_ms: DEFAULT_RELAY_DEADLINE_MS,
            relay_path_loss_fraction: 0.0,
            relay_path_best_direct_rtt_ms: 0.0,
            relay_path_rtt_ms: 0.0,
            relay_path_jitter_ms: 0.0,
            relay_path_queue_delay_ms: 0.0,
            relay_path_observed_at_unix_ms: None,
            relay_secondary_path_loss_fraction: 0.0,
            relay_secondary_path_best_direct_rtt_ms: 0.0,
            relay_secondary_path_rtt_ms: 0.0,
            relay_secondary_path_jitter_ms: 0.0,
            relay_secondary_path_queue_delay_ms: 0.0,
            relay_secondary_path_observed_at_unix_ms: None,
            wall_clock_estimated_error_ms: DEFAULT_WALL_CLOCK_ESTIMATED_ERROR_MS,
            daw_media_bind: None,
            daw_hls_queue_capacity: DEFAULT_AUDIO_EPOCH_HLS_QUEUE_CAPACITY,
            stream_id: 1,
            rist_stream_id: 0,
            srt_stream_id: 6,
            rtmp_stream_id: 7,
            repair_symbols: 1,
            symbol_size: DEFAULT_SYMBOL_SIZE,
            rist_bind: None,
            rist_profile: RistProfile::Main,
            rist_backend: RistBackend::Pure,
            rist_flow_id: DEFAULT_FLOW_ID,
            srt_bind: None,
            rtmp_bind: None,
            fmp4_part_ms: DEFAULT_MIN_PART_MS,
            fmp4_segment_ms: DEFAULT_SEGMENT_MS,
            hls_target_duration_ms: DEFAULT_TARGET_DURATION_MS,
            playlist_count: 65,
            playlist_buffer_kb: 800,
        };
        let telemetry = Arc::new(IngestTelemetry::default());
        let forwarder = MeshForwarder::new(&args, Arc::clone(&telemetry))
            .await
            .unwrap();
        let stream_id = 77;
        let canonical_payload = b"authenticated-media";
        let key = ObjectKey::for_payload(
            "ten_wire",
            stream_id.to_string(),
            "cfg_wire",
            8,
            17,
            9,
            1,
            canonical_payload,
        )
        .unwrap();
        let object = MediaObject::builder(key, ObjectKind::Media, canonical_payload.to_vec())
            .with_configuration_epoch(23)
            .with_metadata("media-control-contract", b"v1".to_vec())
            .with_metadata("media-frame-configuration-v1", b"canonical-config".to_vec())
            .with_metadata("media-frame-envelope-v1", b"canonical-envelope".to_vec())
            .build()
            .unwrap();
        let wire = media_object::encode(&object).unwrap();
        forwarder
            .forward_stream_slot_with_limit(stream_id, &wire, Some(1_200))
            .await
            .unwrap();

        let mut decoder = FecDatagramDecoder::webtransport_with_stream_prefix(stream_id);
        let mut buf = vec![0u8; 65_536];
        let payload = timeout(Duration::from_secs(3), async {
            loop {
                let (len, _peer) = mesh_socket.recv_from(&mut buf).await.unwrap();
                assert!(len <= 1_200);
                if let Some(payload) = decoder.push_datagram(&buf[..len]).unwrap() {
                    break payload;
                }
            }
        })
        .await
        .unwrap();

        assert_eq!(payload, wire);
        let decoded = media_object::decode(&payload).unwrap();
        assert_eq!(decoded.key().tenant(), "ten_wire");
        assert_eq!(decoded.key().stream(), "77");
        assert_eq!(decoded.key().track(), "cfg_wire");
        assert_eq!(decoded.key().epoch(), 8);
        assert_eq!(decoded.key().group(), 17);
        assert_eq!(decoded.key().object(), 9);
        assert_eq!(decoded.configuration_epoch(), 23);
        let snapshot = telemetry.snapshot();
        assert_eq!(snapshot.mesh_forward.stream_payloads, 1);
        assert!(snapshot.mesh_forward.stream_datagrams > 0);
        assert!(snapshot.mesh_forward.stream_payload_bytes > 0);
        assert!(snapshot.mesh_forward.stream_datagram_bytes > 0);
        assert_eq!(snapshot.mesh_forward.stream_errors, 0);
        assert_eq!(snapshot.mesh_forward.stream_stages.encode_wait.count, 1);
        assert_eq!(snapshot.mesh_forward.stream_stages.encode.count, 1);
        assert_eq!(snapshot.mesh_forward.stream_stages.send.count, 1);
        assert_eq!(snapshot.mesh_forward.stream_stages.telemetry.count, 1);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn stream_slot_forwarder_encodes_same_stream_concurrently_without_wire_id_collisions() {
        let mesh_socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let mut args = contrib_status_args();
        args.mesh_fec_target = mesh_socket.local_addr().unwrap();
        args.mesh_media_fec_target = SocketAddr::from((Ipv4Addr::LOCALHOST, 0));
        let telemetry = Arc::new(IngestTelemetry::default());
        let forwarder = Arc::new(
            MeshForwarder::new(&args, Arc::clone(&telemetry))
                .await
                .unwrap(),
        );
        let stream_id = 77;
        let payload_count = 32u32;
        let mut tasks = Vec::new();
        let mut expected_payloads = HashSet::new();
        for index in 0..payload_count {
            let mut payload = vec![0x5a; 4_096];
            payload[..4].copy_from_slice(&index.to_be_bytes());
            expected_payloads.insert(payload.clone());
            let forwarder = Arc::clone(&forwarder);
            tasks.push(tokio::spawn(async move {
                forwarder
                    .forward_stream_slot(stream_id, &payload)
                    .await
                    .unwrap()
            }));
        }

        let mut expected_datagrams = 0usize;
        for task in tasks {
            expected_datagrams += task.await.unwrap();
        }

        let mut decoder = FecDatagramDecoder::webtransport_with_stream_prefix(stream_id);
        let mut buf = vec![0u8; 65_536];
        let mut block_ids = HashSet::new();
        let mut packet_sequences = HashSet::new();
        let mut decoded_payloads = HashSet::new();
        timeout(Duration::from_secs(3), async {
            for _ in 0..expected_datagrams {
                let (len, _peer) = mesh_socket.recv_from(&mut buf).await.unwrap();
                let datagram = &buf[..len];
                let (decoded_stream_id, raw) = split_stream_id_prefix(datagram).unwrap();
                assert_eq!(decoded_stream_id, stream_id);
                let header = DatagramFecHeader::decode(raw).unwrap();
                header.payload(raw).unwrap();
                block_ids.insert(header.block_id);
                assert!(packet_sequences.insert(header.packet_sequence));
                if let Some(payload) = decoder.push_datagram(datagram).unwrap() {
                    decoded_payloads.insert(payload);
                }
            }
        })
        .await
        .unwrap();

        assert_eq!(block_ids.len(), payload_count as usize);
        assert_eq!(packet_sequences.len(), expected_datagrams);
        assert_eq!(decoded_payloads, expected_payloads);
        let snapshot = telemetry.snapshot();
        assert_eq!(snapshot.mesh_forward.stream_payloads, payload_count as u64);
        assert_eq!(
            snapshot.mesh_forward.stream_stages.encode_wait.count,
            payload_count as u64
        );
    }

    #[tokio::test]
    async fn daw_media_udp_ingest_relays_and_forwards_decoded_media_to_mesh() {
        use raptorq_datagram_fec::{MediaCodec, MediaFecDecoder, MediaFecEncoder, MediaFrame};

        let mesh_media_socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let mesh_media_target = mesh_media_socket.local_addr().unwrap();
        let daw_socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let daw_bind = daw_socket.local_addr().unwrap();
        let args = Args {
            http_port: 0,
            cert: None,
            key: None,
            mesh_fec_target: SocketAddr::from((Ipv4Addr::LOCALHOST, 0)),
            mesh_media_fec_target: mesh_media_target,
            relay_primary_target: None,
            relay_primary_bind: None,
            relay_secondary_target: None,
            relay_secondary_bind: None,
            relay_secondary_seed_source: false,
            relay_exclusive: false,
            relay_local_id: "av-contrib-test".to_owned(),
            relay_primary_id: "primary-test".to_owned(),
            relay_secondary_id: "secondary-test".to_owned(),
            relay_topology_generation: DEFAULT_RELAY_TOPOLOGY_GENERATION,
            relay_subscription_id: DEFAULT_RELAY_SUBSCRIPTION_ID,
            relay_deadline_ms: DEFAULT_RELAY_DEADLINE_MS,
            relay_path_loss_fraction: 0.0,
            relay_path_best_direct_rtt_ms: 0.0,
            relay_path_rtt_ms: 0.0,
            relay_path_jitter_ms: 0.0,
            relay_path_queue_delay_ms: 0.0,
            relay_path_observed_at_unix_ms: None,
            relay_secondary_path_loss_fraction: 0.0,
            relay_secondary_path_best_direct_rtt_ms: 0.0,
            relay_secondary_path_rtt_ms: 0.0,
            relay_secondary_path_jitter_ms: 0.0,
            relay_secondary_path_queue_delay_ms: 0.0,
            relay_secondary_path_observed_at_unix_ms: None,
            wall_clock_estimated_error_ms: DEFAULT_WALL_CLOCK_ESTIMATED_ERROR_MS,
            daw_media_bind: Some(daw_bind),
            daw_hls_queue_capacity: DEFAULT_AUDIO_EPOCH_HLS_QUEUE_CAPACITY,
            stream_id: 1,
            rist_stream_id: 0,
            srt_stream_id: 6,
            rtmp_stream_id: 7,
            repair_symbols: 1,
            symbol_size: DEFAULT_SYMBOL_SIZE,
            rist_bind: None,
            rist_profile: RistProfile::Main,
            rist_backend: RistBackend::Pure,
            rist_flow_id: DEFAULT_FLOW_ID,
            srt_bind: None,
            rtmp_bind: None,
            fmp4_part_ms: DEFAULT_MIN_PART_MS,
            fmp4_segment_ms: DEFAULT_SEGMENT_MS,
            hls_target_duration_ms: DEFAULT_TARGET_DURATION_MS,
            playlist_count: 65,
            playlist_buffer_kb: 800,
        };
        let telemetry = Arc::new(IngestTelemetry::default());
        let forwarder = Arc::new(MeshForwarder::new(&args, telemetry).await.unwrap());
        let targets = Arc::new(RwLock::new(HashMap::new()));
        let (shutdown_tx, shutdown_rx) = watch::channel(());
        let task = tokio::spawn(run_daw_media_udp_ingest(
            daw_socket,
            forwarder,
            targets,
            None,
            shutdown_rx,
        ));

        let subscriber = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        subscriber
            .send_to(DAW_RELAY_SUBSCRIBE_MESSAGE, daw_bind)
            .await
            .unwrap();
        let mut relay_buf = vec![0u8; 65_536];
        let (ack_len, _peer) =
            timeout(Duration::from_secs(3), subscriber.recv_from(&mut relay_buf))
                .await
                .unwrap()
                .unwrap();
        assert_eq!(&relay_buf[..ack_len], DAW_RELAY_SUBSCRIBE_ACK);

        let metadata = MediaFrameMetadata::new(9, 4, 100, MediaCodec::Opus);
        let mut source_encoder = MediaFecEncoder::default();
        let encoded = source_encoder
            .encode_frame(MediaFrame {
                metadata,
                payload: b"opus-soundkit-frame",
            })
            .unwrap();
        let first_source_datagram = encoded.datagrams[0].clone();

        let source = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        for datagram in &encoded.datagrams {
            source.send_to(datagram, daw_bind).await.unwrap();
        }

        let relayed = timeout(Duration::from_secs(3), async {
            loop {
                let (len, _peer) = subscriber.recv_from(&mut relay_buf).await.unwrap();
                if relay_buf[..len] == first_source_datagram {
                    break;
                }
            }
        })
        .await;
        assert!(
            relayed.is_ok(),
            "subscriber did not receive exact relayed DAW datagram"
        );

        let mut mesh_decoder = MediaFecDecoder::new();
        let mut mesh_buf = vec![0u8; 65_536];
        let frame = timeout(Duration::from_secs(3), async {
            loop {
                let (len, _peer) = mesh_media_socket.recv_from(&mut mesh_buf).await.unwrap();
                if let Some(frame) = mesh_decoder.push_datagram(&mesh_buf[..len]).unwrap() {
                    break frame;
                }
            }
        })
        .await
        .unwrap();

        assert_eq!(frame.metadata.stream_id, metadata.stream_id);
        assert_eq!(frame.metadata.pts_ms, metadata.pts_ms);
        assert_eq!(frame.metadata.codec, MediaCodec::Opus);
        assert_eq!(frame.payload, b"opus-soundkit-frame");

        let _ = shutdown_tx.send(());
        task.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn daw_media_udp_ingest_forwards_audio_epoch_datagrams_to_mesh() {
        let mesh_media_socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let mesh_media_target = mesh_media_socket.local_addr().unwrap();
        let daw_socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let daw_bind = daw_socket.local_addr().unwrap();
        let args = Args {
            http_port: 0,
            cert: None,
            key: None,
            mesh_fec_target: SocketAddr::from((Ipv4Addr::LOCALHOST, 0)),
            mesh_media_fec_target: mesh_media_target,
            relay_primary_target: None,
            relay_primary_bind: None,
            relay_secondary_target: None,
            relay_secondary_bind: None,
            relay_secondary_seed_source: false,
            relay_exclusive: false,
            relay_local_id: "av-contrib-test".to_owned(),
            relay_primary_id: "primary-test".to_owned(),
            relay_secondary_id: "secondary-test".to_owned(),
            relay_topology_generation: DEFAULT_RELAY_TOPOLOGY_GENERATION,
            relay_subscription_id: DEFAULT_RELAY_SUBSCRIPTION_ID,
            relay_deadline_ms: DEFAULT_RELAY_DEADLINE_MS,
            relay_path_loss_fraction: 0.0,
            relay_path_best_direct_rtt_ms: 0.0,
            relay_path_rtt_ms: 0.0,
            relay_path_jitter_ms: 0.0,
            relay_path_queue_delay_ms: 0.0,
            relay_path_observed_at_unix_ms: None,
            relay_secondary_path_loss_fraction: 0.0,
            relay_secondary_path_best_direct_rtt_ms: 0.0,
            relay_secondary_path_rtt_ms: 0.0,
            relay_secondary_path_jitter_ms: 0.0,
            relay_secondary_path_queue_delay_ms: 0.0,
            relay_secondary_path_observed_at_unix_ms: None,
            wall_clock_estimated_error_ms: DEFAULT_WALL_CLOCK_ESTIMATED_ERROR_MS,
            daw_media_bind: Some(daw_bind),
            daw_hls_queue_capacity: DEFAULT_AUDIO_EPOCH_HLS_QUEUE_CAPACITY,
            stream_id: 1,
            rist_stream_id: 0,
            srt_stream_id: 6,
            rtmp_stream_id: 7,
            repair_symbols: 1,
            symbol_size: DEFAULT_SYMBOL_SIZE,
            rist_bind: None,
            rist_profile: RistProfile::Main,
            rist_backend: RistBackend::Pure,
            rist_flow_id: DEFAULT_FLOW_ID,
            srt_bind: None,
            rtmp_bind: None,
            fmp4_part_ms: DEFAULT_MIN_PART_MS,
            fmp4_segment_ms: DEFAULT_SEGMENT_MS,
            hls_target_duration_ms: DEFAULT_TARGET_DURATION_MS,
            playlist_count: 65,
            playlist_buffer_kb: 800,
        };
        let telemetry = Arc::new(IngestTelemetry::default());
        let forwarder = Arc::new(MeshForwarder::new(&args, telemetry).await.unwrap());
        let targets = Arc::new(RwLock::new(HashMap::new()));
        let (shutdown_tx, shutdown_rx) = watch::channel(());
        let task = tokio::spawn(run_daw_media_udp_ingest(
            daw_socket,
            forwarder,
            targets,
            None,
            shutdown_rx,
        ));

        let matching = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let other = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        matching
            .send_to(b"WAVEY-DAW-SUBSCRIBE/2 91", daw_bind)
            .await
            .unwrap();
        other
            .send_to(b"WAVEY-DAW-SUBSCRIBE/2 92", daw_bind)
            .await
            .unwrap();
        let mut relay_buf = vec![0u8; 65_536];
        let (matching_ack, _) = timeout(Duration::from_secs(3), matching.recv_from(&mut relay_buf))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(&relay_buf[..matching_ack], b"WAVEY-DAW-SUBSCRIBED/2 91");
        let (other_ack, _) = timeout(Duration::from_secs(3), other.recv_from(&mut relay_buf))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(&relay_buf[..other_ack], b"WAVEY-DAW-SUBSCRIBED/2 92");

        let transport = MultichannelAudioTransportAdapter::udp(1_200);
        let fec = transport.prepare_fec_config(MultichannelAudioFecConfig::default());
        let mut sender = MultichannelAudioSender::new(MultichannelAudioSessionConfig {
            fec,
            ..MultichannelAudioSessionConfig::default()
        });
        let pcm = vec![7_u8; 240 * 2 * 2];
        let groups = [MultichannelAudioGroup {
            group_id: 0,
            channel_start: 0,
            channel_count: 2,
            payload_kind: AudioPayloadKind::Pcm,
            sample_format: AudioSampleFormat::S16Le,
            flags: 0,
            payload: &pcm,
        }];
        let encoded = sender
            .encode_epoch(MultichannelAudioEpoch {
                session_id: 91,
                config_generation: 1,
                epoch_id: 0,
                pts_samples: 0,
                sample_rate: 48_000,
                frame_count: 240,
                groups: &groups,
            })
            .unwrap();
        let wrapped = transport.wrap_epoch(encoded).unwrap();
        let epoch_datagram = wrapped.datagrams[0].payload.clone();
        assert!(is_multichannel_audio_transport_datagram(&epoch_datagram));
        let source = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        for datagram in wrapped.datagrams {
            source.send_to(&datagram.payload, daw_bind).await.unwrap();
        }

        let mut mesh_buf = vec![0u8; 65_536];
        let (len, _peer) = timeout(
            Duration::from_secs(3),
            mesh_media_socket.recv_from(&mut mesh_buf),
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(&mesh_buf[..len], epoch_datagram.as_ref());

        let (relayed_len, _) = timeout(Duration::from_secs(3), matching.recv_from(&mut relay_buf))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(&relay_buf[..relayed_len], epoch_datagram.as_ref());
        assert!(
            timeout(Duration::from_millis(100), other.recv_from(&mut relay_buf))
                .await
                .is_err(),
            "a subscriber for another AEP1 session received a datagram"
        );

        let _ = shutdown_tx.send(());
        task.await.unwrap().unwrap();
    }
}
