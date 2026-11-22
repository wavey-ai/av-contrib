use anyhow::{bail, Context, Result};
use av_contrib::audio_epoch_hls::{
    channel as audio_epoch_hls_channel, run_audio_epoch_hls_worker,
    worker_stats as audio_epoch_hls_worker_stats, AudioEpochHlsConfig, AudioEpochHlsDatagram,
    AudioEpochHlsPackaging, RecoveredAudioEpoch, RenditionRuntimes,
    DEFAULT_AUDIO_EPOCH_HLS_QUEUE_CAPACITY,
};
use av_contrib::class_lanes::{self, ClassSocketOptions};
use av_contrib::fmp4_bridge::{
    AccessUnitSink, Fmp4PartPublisher, Fmp4Segmenter, MpegTsContinuityIssue, MpegTsPayloadDrop,
    PublishedFmp4Part, PublishedOpaquePart, TimestampInput, TsFmp4Bridge,
};
use av_contrib::hls_stamp::StreamStamper;
use av_contrib::hop_journal::{
    self, HopEvent, HopJournal, HopStage, DEFAULT_HOP_JOURNAL_CAPACITY, HOP_JOURNAL_CONTENT_TYPE,
    HOP_JOURNAL_PATH,
};
use av_contrib::ingress_authorization::{
    decode_envelope_header_bytes, gate_from_bootstrap_path, parse_bearer_header,
    parse_content_length_header, PublishAuthorizationMode, PublishIngressError, PublishIngressGate,
    PublishIngressRequest, PublishLease, PublishRejectionCode, MEDIA_FRAME_ENVELOPE_HEADER,
};
use av_contrib::part_timing::{AudioPartSize, VideoPartSize};
#[cfg(all(feature = "quadra-renditions", target_os = "linux"))]
use av_contrib::quadra_renditions::{
    start_quadra_rendition_pipeline, QuadraRenditionConfig, QuadraRenditionSpec,
    QuadraRenditionTarget,
};
use av_contrib::soundkit_v2::SoundKitV2EpochNormalizer;
use av_contrib::source_first::SourceFirstEncoder;
use av_contrib::udp_batch;
use av_contrib::{codec_name, MediaAccessUnitParams};
use bytes::{Bytes, BytesMut};
#[cfg(all(feature = "quadra-renditions", target_os = "linux"))]
use clap::Args as ClapArgs;
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
    source_symbol_count, AudioPayloadKind, DatagramFecEncoder, DatagramFecHeader, MediaCodec,
    MediaFecDecoder, MediaFecEncoder, MediaFrame, MediaFrameMetadata, DEFAULT_SOURCE_SYMBOLS,
    DEFAULT_SYMBOL_SIZE, ENCODING_PACKET_HEADER_LEN, HEADER_LEN, MAX_SOURCE_SYMBOLS_PER_BLOCK,
};
use relay_session::{
    encode_datagram as encode_relay_datagram, AdaptiveFecController, AdaptiveFecPolicy,
    CongestionConfig, EncodedRaptorQObject, MediaDatagramRole, MediaDeadline, ObjectAnnouncement,
    PathMetrics, PrivateUdpConfig, RaptorQObjectEncoder, RelayLimits, SubscriptionId,
    TopologyGeneration,
};
use rtmp_ingress::ingress::start_rtmp_listener;
use rtmp_ingress::{RtmpIngestEvent, RtmpStreamInfo};
use serde::{Deserialize, Serialize};
use socket2::SockRef;
use std::collections::{HashMap, HashSet, VecDeque};
use std::net::SocketAddr;
#[cfg(target_os = "linux")]
use std::os::fd::AsRawFd;
use std::path::PathBuf;
use std::sync::{
    atomic::{AtomicBool, AtomicU32, AtomicU64, AtomicUsize, Ordering},
    Arc, Mutex as StdMutex, OnceLock,
};
use std::time::Duration;
use tokio::net::UdpSocket;
use tokio::sync::{mpsc, watch, Mutex};
use tokio::time::{interval, Instant, MissedTickBehavior};
use tracing::{debug, info, trace, warn};
#[cfg(feature = "srt-ingest")]
use upload_response::SrtIngest as UploadSrtIngest;
use upload_response::{
    CachedResponse, PureRistIngest as UploadRistIngest, PureRistProfile as UploadRistProfile,
    TailSlot, UploadResponseConfig, UploadResponseService,
};
use web_service::{
    load_default_tls_base64, load_tls_base64_from_paths, BodyStream, H2H3Server, HandlerResponse,
    HandlerResult, Router, Server, ServerBuilder, ServerError, StreamWriter, WebSocketHandler,
    WebTransportHandler,
};

const MEDIA_ACCESS_UNIT_PATH: &str = "/media/access-unit";
const NEEDLETAIL_AEP1_INGEST_PATH: &str = "/v1/ingest/aep1";
const NEEDLETAIL_AEP1_CONTENT_TYPE: &str = "application/x-needletail-aep1-stream";
const NEEDLETAIL_AEP1_MAX_DATAGRAM_BYTES: usize = 16 * 1024;
const NEEDLETAIL_AEP1_MAX_BUFFER_BYTES: usize = 256 * 1024;
const CONTRIB_STATUS_PATH: &str = "/api/status";
const CONTRIB_STATUS_EVENTS_PATH: &str = "/api/status/events";
const CONTRIB_METRICS_PATH: &str = "/metrics";
const PROMETHEUS_CONTENT_TYPE: &str = "text/plain; version=0.0.4; charset=utf-8";
const DAW_MEDIA_RECEIVE_BUFFER_BYTES: usize = 64 * 1024 * 1024;
const MULTICHANNEL_AUDIO_TRANSPORT_MAGIC: &[u8] = b"AEP1";
const ENCRYPTED_AUDIO_TRANSPORT_MAGIC: &[u8] = b"ACE1";
const DAW_CONTROL_MAGIC: &[u8] = b"WAVEY-DAW-";
const MEDIA_OBJECT_TENANT: &str = "default";
const FMP4_MEDIA_TRACK: &str = "muxed-fmp4";
const FMP4_INITIALIZATION_TRACK: &str = "muxed-fmp4-init";
const OPAQUE_AUDIO_TRACK: &str = "opaque-audio";
const MEDIA_OBJECT_VERSION: u32 = 1;
const MESH_FMP4_SLOT_MAGIC: &[u8; 8] = b"AVFMP4S1";
const MESH_FMP4_SLOT_HEADER_LEN: usize = 16;
const AV_CONTRIB_CLOCK_ID: &str = "system:realtime:av-contrib";
const DEFAULT_WALL_CLOCK_ESTIMATED_ERROR_MS: u64 = 1_000;
const DEFAULT_RELAY_DEADLINE_MS: u64 = 1_000;
const DEFAULT_RELAY_TOPOLOGY_GENERATION: u64 = 1;
const DEFAULT_RELAY_SUBSCRIPTION_ID: u64 = 1;
const RELAY_LANE_IMPAIRED_HOLD_MS: u64 = 3_000;
const AUDIO_EPOCH_MESH_QUEUE_CAPACITY: usize = 32_768;
const AUDIO_EPOCH_MESH_SEND_QUEUE_CAPACITY: usize = 32_768;
const PUBLICATION_WORKERS: usize = 8;
/// Parts of at least this many source symbols compute their repair
/// alongside their source sends (`--relay-repair-alongside-sources`); for
/// smaller ones the blocking thread's hand-off costs more than the repair.
const REPAIR_ALONGSIDE_MIN_SOURCES: usize = 16;
// Parts that wait for a publication worker, sized in time. A pause of
// 120 ms of the process (measured on a busy host; a shared CPU pauses a
// Nanode in the same way) overflowed 32 places a worker at 5 ms parts, and
// each part that does not fit is a hole in the playlist of every reader.
// Parts are now one epoch: 12-sample PCM parts are 4,000 a second, so 4,096
// places a worker hold one second of such a stream, or 0.36 s of it beside
// 2.5 ms Opus and 1 ms PCM on the same worker (5,400 parts a second).
const PUBLICATION_QUEUE_CAPACITY: usize = 32_768;
const PUBLICATION_QUEUE_CAPACITY_PER_WORKER: usize =
    PUBLICATION_QUEUE_CAPACITY / PUBLICATION_WORKERS;
static PUBLICATION_QUEUE_CAPACITY_METRIC: AtomicU64 = AtomicU64::new(0);
static PUBLICATION_QUEUE_ENQUEUED: AtomicU64 = AtomicU64::new(0);
static PUBLICATION_QUEUE_DROPPED: AtomicU64 = AtomicU64::new(0);
static PUBLICATION_QUEUE_DEPTH: AtomicU64 = AtomicU64::new(0);
static PUBLICATION_QUEUE_MAX_DEPTH: AtomicU64 = AtomicU64::new(0);
static PUBLICATION_COMPLETED: AtomicU64 = AtomicU64::new(0);
static PUBLICATION_ERRORS: AtomicU64 = AtomicU64::new(0);
/// Audio parts published on the packager's task (`--relay-audio-inline`).
static PUBLICATION_INLINE: AtomicU64 = AtomicU64::new(0);
/// Inline audio publications that held their packager longer than
/// [`INLINE_PUBLISH_SLOW`], and the longest hold (µs).
static PUBLICATION_INLINE_SLOW: AtomicU64 = AtomicU64::new(0);
static PUBLICATION_INLINE_MAX_US: AtomicU64 = AtomicU64::new(0);
/// An inline audio publication this long holds every later part of its
/// rendition behind it (the referee saw one hold 1.34 s: 550 parts published
/// late together, and the part held lost to its 1 s deadline): it is logged
/// with the part, so that the stage that held it can be found in the hop
/// journal.
const INLINE_PUBLISH_SLOW: Duration = Duration::from_millis(20);
static AUDIO_EPOCH_HLS_QUEUE_CAPACITY: AtomicU64 = AtomicU64::new(0);
static AUDIO_EPOCH_HLS_QUEUE_ENQUEUED: AtomicU64 = AtomicU64::new(0);
static AUDIO_EPOCH_HLS_QUEUE_DROPPED: AtomicU64 = AtomicU64::new(0);
static AUDIO_EPOCH_HLS_QUEUE_DEPTH: AtomicU64 = AtomicU64::new(0);
static AUDIO_EPOCH_HLS_QUEUE_MAX_DEPTH: AtomicU64 = AtomicU64::new(0);
static AUDIO_EPOCH_MESH_QUEUE_CAPACITY_METRIC: AtomicU64 = AtomicU64::new(0);
static AUDIO_EPOCH_MESH_QUEUE_ENQUEUED: AtomicU64 = AtomicU64::new(0);
static AUDIO_EPOCH_MESH_QUEUE_DROPPED: AtomicU64 = AtomicU64::new(0);
static AUDIO_EPOCH_MESH_QUEUE_DEPTH: AtomicU64 = AtomicU64::new(0);
static AUDIO_EPOCH_MESH_QUEUE_MAX_DEPTH: AtomicU64 = AtomicU64::new(0);
static AUDIO_EPOCH_MESH_FORWARD_ERRORS: AtomicU64 = AtomicU64::new(0);
static AUDIO_EPOCH_NORMALIZED_EPOCHS: AtomicU64 = AtomicU64::new(0);
static AUDIO_EPOCH_NORMALIZATION_ERRORS: AtomicU64 = AtomicU64::new(0);
static AUDIO_EPOCH_MESH_SEND_QUEUE_CAPACITY_METRIC: AtomicU64 = AtomicU64::new(0);
static AUDIO_EPOCH_MESH_SEND_QUEUE_ENQUEUED: AtomicU64 = AtomicU64::new(0);
static AUDIO_EPOCH_MESH_SEND_QUEUE_DROPPED: AtomicU64 = AtomicU64::new(0);
static AUDIO_EPOCH_MESH_SEND_QUEUE_DEPTH: AtomicU64 = AtomicU64::new(0);
static AUDIO_EPOCH_MESH_SEND_QUEUE_MAX_DEPTH: AtomicU64 = AtomicU64::new(0);
static AUDIO_EPOCH_INGRESS_TARGETS: AtomicU64 = AtomicU64::new(0);
static DAW_MEDIA_UDP_SOCKET_INODE: AtomicU64 = AtomicU64::new(0);

fn audio_epoch_ingress_queue_age() -> &'static AtomicDurationHistogram {
    static HISTOGRAM: OnceLock<AtomicDurationHistogram> = OnceLock::new();
    HISTOGRAM.get_or_init(AtomicDurationHistogram::default)
}

fn publication_queue_age() -> &'static AtomicDurationHistogram {
    static HISTOGRAM: OnceLock<AtomicDurationHistogram> = OnceLock::new();
    HISTOGRAM.get_or_init(AtomicDurationHistogram::default)
}

fn publication_duration() -> &'static AtomicDurationHistogram {
    static HISTOGRAM: OnceLock<AtomicDurationHistogram> = OnceLock::new();
    HISTOGRAM.get_or_init(AtomicDurationHistogram::default)
}

#[cfg(any(target_os = "linux", test))]
fn linux_socket_inode(link: &std::path::Path) -> Option<u64> {
    let link = link.to_str()?;
    link.strip_prefix("socket:[")?
        .strip_suffix(']')?
        .parse()
        .ok()
}

fn linux_udp_socket_drops_from_table(table: &str, socket_inode: u64) -> Option<u64> {
    table.lines().skip(1).find_map(|line| {
        let fields = line.split_ascii_whitespace().collect::<Vec<_>>();
        let inode = fields.get(9)?.parse::<u64>().ok()?;
        (inode == socket_inode)
            .then(|| fields.last()?.parse::<u64>().ok())
            .flatten()
    })
}

#[cfg(target_os = "linux")]
fn record_daw_media_udp_socket_inode(socket: &UdpSocket) {
    let fd_link = format!("/proc/self/fd/{}", socket.as_raw_fd());
    match std::fs::read_link(&fd_link)
        .ok()
        .as_deref()
        .and_then(linux_socket_inode)
    {
        Some(inode) => DAW_MEDIA_UDP_SOCKET_INODE.store(inode, Ordering::Relaxed),
        None => warn!(
            fd_link,
            "could not identify DAW media UDP socket for drop metrics"
        ),
    }
}

#[cfg(not(target_os = "linux"))]
fn record_daw_media_udp_socket_inode(_socket: &UdpSocket) {}

fn daw_media_udp_socket_drops() -> Option<u64> {
    let inode = DAW_MEDIA_UDP_SOCKET_INODE.load(Ordering::Relaxed);
    if inode == 0 {
        return None;
    }
    ["/proc/net/udp", "/proc/net/udp6"]
        .into_iter()
        .filter_map(|path| std::fs::read_to_string(path).ok())
        .find_map(|table| linux_udp_socket_drops_from_table(&table, inode))
}

fn should_log_audio_epoch_hls_drop(dropped_total: u64) -> bool {
    dropped_total <= 16 || dropped_total.is_power_of_two() || dropped_total.is_multiple_of(10_000)
}

fn should_log_audio_epoch_mesh_drop(dropped_total: u64) -> bool {
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
        .with_metadata("duration-us", part.duration_us.to_string().into_bytes())
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
    if let Some(width) = part.video_width {
        builder = builder.with_metadata("video-width", width.to_string().into_bytes());
    }
    if let Some(height) = part.video_height {
        builder = builder.with_metadata("video-height", height.to_string().into_bytes());
    }
    if let Some(codec) = part.audio_codec {
        builder = builder.with_metadata("audio-codec", codec.as_bytes().to_vec());
    }
    builder = with_sample_duration_metadata(builder, part.duration_samples, part.sample_rate);
    if let Some(stamp) = part.hls {
        for (key, value) in stamp.metadata() {
            builder = builder.with_metadata(key, value);
        }
    }
    builder
        .build()
        .context("invalid canonical fMP4 media object")
}

fn build_opaque_media_object(
    part: &PublishedOpaquePart,
    source_epoch: u64,
    delivery_budget_ms: u64,
    estimated_clock_error_ns: u64,
) -> Result<MediaObject> {
    if delivery_budget_ms == 0 {
        bail!("canonical media delivery budget must be positive");
    }
    if part.bytes.is_empty() || part.audio_units == 0 || part.duration_us == 0 {
        bail!("opaque audio part must contain timed media units");
    }
    let key = ObjectKey::for_payload(
        MEDIA_OBJECT_TENANT,
        part.stream_id.to_string(),
        OPAQUE_AUDIO_TRACK,
        source_epoch,
        0,
        part.sequence,
        MEDIA_OBJECT_VERSION,
        &part.bytes,
    )
    .context("invalid canonical opaque-media identity")?;
    let packaged =
        canonical_contributor_timestamp(part.packaged_at_unix_ns, estimated_clock_error_ns)?;
    let published =
        canonical_contributor_timestamp(part.published_at_unix_ns, estimated_clock_error_ns)?;
    let deadline_ns = i128::from(part.published_at_unix_ns)
        .checked_add(i128::from(delivery_budget_ms) * 1_000_000)
        .and_then(|value| i64::try_from(value).ok())
        .context("canonical media deadline exceeds Unix-nanosecond range")?;
    let deadline = canonical_contributor_timestamp(deadline_ns, estimated_clock_error_ns)?;

    let mut builder = MediaObject::builder(key, ObjectKind::Media, part.bytes.to_vec())
        .with_deadline(deadline)
        .with_stage_timestamp(StageTimestamp::new(Stage::Packaged, packaged))
        .with_stage_timestamp(StageTimestamp::new(Stage::Published, published))
        .with_metadata("container", b"opaque".to_vec())
        .with_metadata("duration-ms", part.duration_ms.to_string().into_bytes())
        .with_metadata("duration-us", part.duration_us.to_string().into_bytes())
        .with_metadata("payload-format", b"opaque-part-v1".to_vec())
        .with_metadata("scheduler-class", b"audio".to_vec())
        .with_metadata("track-composition", b"audio".to_vec())
        .with_metadata("content-type", b"application/octet-stream".to_vec())
        .with_metadata("file-extension", b"bin".to_vec());
    builder = with_sample_duration_metadata(builder, part.duration_samples, part.sample_rate);
    if let Some(stamp) = part.hls {
        for (key, value) in stamp.metadata() {
            builder = builder.with_metadata(key, value);
        }
    }
    builder
        .build()
        .context("invalid canonical opaque-media object")
}

/// `duration-samples` and `sample-rate` of a part whose sample count is
/// known: caches can then render its duration without rounding.
fn with_sample_duration_metadata(
    builder: media_object::MediaObjectBuilder,
    duration_samples: Option<u64>,
    sample_rate: Option<u32>,
) -> media_object::MediaObjectBuilder {
    match (duration_samples, sample_rate) {
        (Some(samples), Some(rate)) if samples > 0 && rate > 0 => builder
            .with_metadata("duration-samples", samples.to_string().into_bytes())
            .with_metadata("sample-rate", rate.to_string().into_bytes()),
        _ => builder,
    }
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

fn is_audio_transport_datagram(datagram: &[u8]) -> bool {
    datagram.starts_with(MULTICHANNEL_AUDIO_TRANSPORT_MAGIC)
        || datagram.starts_with(ENCRYPTED_AUDIO_TRANSPORT_MAGIC)
}
const UPLOAD_RESPONSE_HLS_WORKER_ID: &str = "av-contrib-upload-response-fmp4-bridge";
const HLS_BRIDGE_POLL_MS: u64 = 5;
const DEFAULT_SEGMENT_MS: u32 = 1_000;
const DEFAULT_TARGET_DURATION_MS: u32 = 6_000;
const CONTRIB_ACTIVITY_LIMIT: usize = 64;
const CONTRIB_INGEST_SESSION_LIMIT: usize = 48;
const CONTRIB_MIN_STALE_OUTPUT_MS: u64 = 5_000;
const MAX_STREAM_FEC_OBJECT_BYTES: usize = 8 * 1024 * 1024;
const MAX_STREAM_FEC_DATAGRAMS: u32 = 32_768;
const MAX_MEDIA_ACCESS_UNIT_BYTES: usize = 8 * 1024 * 1024;
const DURATION_HISTOGRAM_BUCKETS_US: [u64; 13] = [
    100, 250, 500, 1_000, 2_500, 5_000, 10_000, 25_000, 50_000, 100_000, 250_000, 500_000,
    1_000_000,
];

/// `--relay-video-gso-segments`.
const DEFAULT_RELAY_VIDEO_GSO_SEGMENTS: u16 = 16;

/// `--relay-video-gso-pace-mbps`: no faster than the one-datagram-each
/// batch hands a 4K part to the kernel (on a loopback benchmark host, see
/// `video_gso_cpu_and_bursts`: off, 23 to 28 datagrams in any 100 us and
/// 172 to 190 in any 1 ms; paced at 2,000, 24 and 152 to 160).
const DEFAULT_RELAY_VIDEO_GSO_PACE_MBPS: u64 = 2_000;

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

    /// Minimum initial repair symbols for each canonical RelaySession object.
    /// Use a floor at least as large as the small-object source-symbol count
    /// when complete correlated source loss must remain recoverable.
    #[arg(long, env = "AV_RELAY_MIN_REPAIR_SYMBOLS", default_value_t = 0)]
    relay_min_repair_symbols: u32,

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

    /// Dedicated nearest ingress for exact AEP1 source and repair datagrams.
    /// Defaults to --mesh-media-fec-target for transition compatibility.
    #[arg(long, env = "AV_AUDIO_EPOCH_INGRESS_TARGET")]
    audio_epoch_ingress_target: Option<SocketAddr>,

    /// Optional fixed second AEP1 ingress for origin resilience. Relay and
    /// edge services own every geographical and viewer fanout beyond it.
    #[arg(long, env = "AV_AUDIO_EPOCH_REDUNDANT_INGRESS_TARGET")]
    audio_epoch_redundant_ingress_target: Option<SocketAddr>,

    /// Bounded handoff from the live AEP1 datagram path to format-preserving LL-HLS packaging.
    #[arg(
        long,
        env = "AV_DAW_HLS_QUEUE_CAPACITY",
        default_value_t = DEFAULT_AUDIO_EPOCH_HLS_QUEUE_CAPACITY
    )]
    daw_hls_queue_capacity: usize,

    /// Choose whether DAW AEP1 payloads pass through unchanged or are boxed as fMP4.
    #[arg(
        long,
        env = "AV_DAW_HLS_PACKAGING",
        value_enum,
        default_value = "opaque"
    )]
    daw_hls_packaging: DawHlsPackaging,

    /// AEP1 audio formats that this LL-HLS view publishes. FLAC stays in
    /// the default beside PCM and Opus: a source that sends FLAC (the
    /// probe's default payload) keeps its rendition, and a format that the
    /// source does not send costs nothing.
    #[arg(
        long,
        env = "AV_DAW_HLS_FORMATS",
        value_enum,
        value_delimiter = ',',
        default_value = "flac,pcm,opus"
    )]
    daw_hls_formats: Vec<DawHlsFormat>,

    /// First HLS publication stream ID reserved for DAW Soundkit v2 audio.
    #[arg(long, env = "AV_DAW_HLS_BASE_STREAM_ID", default_value_t = 2001)]
    daw_hls_base_stream_id: u64,

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

    #[arg(long)]
    srt_bind: Option<SocketAddr>,

    #[arg(long)]
    rtmp_bind: Option<SocketAddr>,

    /// Size of an audio part: `epoch` (one AEP1 epoch, or one access unit,
    /// per part, whatever its sample count), `<N>samples` or `<N>us`. A
    /// larger part is a setting; the default is the smallest the source
    /// makes. A FLAC part is never under 32 samples.
    #[arg(long, env = "AV_LL_HLS_AUDIO_PART")]
    audio_part: Option<AudioPartSize>,

    /// Old form of `--audio-part <N>ms`: an audio part of at least this many
    /// milliseconds. `--audio-part` wins when both are given. It no longer
    /// sets the video part.
    #[arg(long, env = "AV_LL_HLS_PART_MS")]
    fmp4_part_ms: Option<u32>,

    #[arg(long, env = "AV_LL_HLS_SEGMENT_MS", default_value_t = DEFAULT_SEGMENT_MS)]
    fmp4_segment_ms: u32,

    /// Size of a part of a stream with video, from SRT, RIST or RTMP:
    /// `frame` (one access unit per part, the default) or `<N>ms`. With
    /// frame parts the declared PART-TARGET is the longest frame seen.
    #[arg(long, env = "AV_LL_HLS_VIDEO_PART")]
    video_part: Option<VideoPartSize>,

    /// Old form of `--video-part <N>ms`. `--video-part` wins when both are
    /// given.
    #[arg(long, env = "AV_LL_HLS_VIDEO_PART_MS")]
    fmp4_video_part_ms: Option<u32>,

    /// Shortest segment of a stream with video. A segment ends at the first
    /// key frame after it. It defaults to `--fmp4-segment-ms`. Set it to the
    /// key-frame interval of the encoder.
    #[arg(long, env = "AV_LL_HLS_VIDEO_SEGMENT_MS")]
    fmp4_video_segment_ms: Option<u32>,

    /// Close a video part as soon as the access unit just pushed brings it to
    /// the part target, instead of when the next access unit arrives. With
    /// one-frame parts this removes one frame interval from every part.
    #[arg(
        long,
        env = "AV_FMP4_FLUSH_ON_COMPLETE",
        default_value_t = true,
        action = clap::ArgAction::Set
    )]
    fmp4_flush_on_complete: bool,

    #[arg(long, env = "AV_LL_HLS_TARGET_DURATION_MS", default_value_t = DEFAULT_TARGET_DURATION_MS)]
    hls_target_duration_ms: u32,

    #[arg(long, default_value_t = 65)]
    playlist_count: usize,

    #[arg(long, default_value_t = 800)]
    playlist_buffer_kb: usize,

    /// Events kept in the hop journal served at /api/hops. 0 disables it.
    /// The default, 2^20, holds about 75 s of the default parts (a part
    /// leaves about ten events); the start-up log gives the seconds at the
    /// configured part.
    #[arg(
        long,
        env = "AV_HOP_JOURNAL_CAPACITY",
        default_value_t = DEFAULT_HOP_JOURNAL_CAPACITY
    )]
    hop_journal_capacity: usize,

    /// Give each new stream the next publication worker of its pool, audio
    /// and video in separate pools, instead of `stream_id % 8`, and give
    /// audio and video parts their own RaptorQ encoder, so that an audio part
    /// never waits behind a video part's queue or encode. With `false` the
    /// DAW streams 2001, 3001 and 4001 and video stream 1 share a worker and
    /// every part shares one encoder.
    #[arg(
        long,
        env = "AV_PUBLICATION_SHARD_MIX",
        default_value_t = true,
        action = clap::ArgAction::Set
    )]
    publication_shard_mix: bool,

    /// Hand the RelaySession symbols of one part to the kernel in one
    /// `sendmmsg` call per lane (Linux) instead of one awaited send per
    /// symbol. With `false` every symbol is its own `send`.
    #[arg(
        long,
        env = "AV_RELAY_SEND_BATCH",
        default_value_t = true,
        action = clap::ArgAction::Set
    )]
    relay_send_batch: bool,

    /// Hand a 4K part's RelaySession symbols to the kernel as UDP GSO
    /// messages (UDP_SEGMENT: a run of symbols in one pass through the
    /// stack, cut into the same datagrams by the kernel) on the video lanes,
    /// instead of one `sendmmsg` entry per symbol. It saves the contributor
    /// send cost per symbol only: the wire, the packet rate and every
    /// receiver's cost are the same. A GSO message is one packet to the
    /// qdisc, so its datagrams leave back to back: a `prio` qdisc lets
    /// audio in only between messages, and `fq` pacing
    /// (--relay-video-pacing-mbps) spaces messages, not datagrams. Runs are
    /// kept to --relay-video-gso-segments, and on paced video sockets to
    /// about one millisecond of the pacing rate. Unpaced, GSO hands a whole
    /// 4K part to the kernel in well under a millisecond (4 to 5 times the
    /// one-datagram-each batch's density): with it on the wave-4 lab
    /// contributor went from 33% to 125% of a vCPU at 81 Mbit/s and frames
    /// came late at 122, so the messages are now paced
    /// (--relay-video-gso-pace-mbps). Off until a lab A/B of the paced send
    /// shows lower contributor CPU with frames and audio p99 unchanged. A
    /// host without UDP GSO is detected on the first send and the lanes go
    /// on without it. Needs --relay-send-batch.
    #[arg(
        long,
        env = "AV_RELAY_VIDEO_GSO",
        default_value_t = false,
        action = clap::ArgAction::Set
    )]
    relay_video_gso: bool,

    /// --relay-video-gso: datagrams of one GSO message at most (1-64).
    /// 16 symbols of about 1.2 kB is about 19 kB, 40 us at 4 Gbit/s.
    #[arg(
        long,
        env = "AV_RELAY_VIDEO_GSO_SEGMENTS",
        default_value_t = DEFAULT_RELAY_VIDEO_GSO_SEGMENTS,
        value_parser = clap::value_parser!(u16).range(1..=udp_batch::GSO_MAX_SEGMENTS as i64)
    )]
    relay_video_gso_segments: u16,

    /// --relay-video-gso: hand each lane's GSO messages to the kernel no
    /// faster than this many Mbit/s of payload, from a blocking thread that
    /// sleeps between messages (and messages of at most 50 us of it). 0
    /// hands them over as fast as the kernel takes them, a whole 4K part in
    /// well under a millisecond: the wave-4 lab's CPU and late frames at
    /// 81-122 Mbit/s. Ignored with GSO off.
    #[arg(
        long,
        env = "AV_RELAY_VIDEO_GSO_PACE_MBPS",
        default_value_t = DEFAULT_RELAY_VIDEO_GSO_PACE_MBPS
    )]
    relay_video_gso_pace_mbps: u64,

    /// Recover each AEP1 epoch once: the LL-HLS worker's FEC receiver hands
    /// every completed epoch to the SoundKit v2 normaliser of the mesh lane,
    /// which then only re-frames it. With `false` the normaliser runs its own
    /// FEC receiver over a second copy of every datagram.
    #[arg(
        long,
        env = "AV_AEP1_SHARED_RECOVERY",
        default_value_t = true,
        action = clap::ArgAction::Set
    )]
    aep1_shared_recovery: bool,

    /// Also send each part straight to a distributor of the contributor's
    /// own region, beside the two backbone lanes (repeatable). The
    /// distributor must register --relay-direct-bind as a parent of its own;
    /// it keeps the first copy of an object from any parent. Every direct
    /// lane shares one socket, so a part's symbols reach all of them in one
    /// `sendmmsg` call. A direct lane never fails a part: its errors are
    /// counted only.
    #[arg(
        long = "relay-direct-target",
        env = "AV_RELAY_DIRECT_TARGETS",
        value_delimiter = ','
    )]
    relay_direct_targets: Vec<SocketAddr>,

    /// Source endpoint of every direct lane: the address that each direct
    /// distributor registers for this contributor. Defaults to an ephemeral
    /// port, which a distributor cannot register.
    #[arg(long, env = "AV_RELAY_DIRECT_BIND")]
    relay_direct_bind: Option<SocketAddr>,

    /// Node id of each direct distributor, in the order of the targets
    /// (repeatable; default needletail-distributor-<n>).
    #[arg(
        long = "relay-direct-id",
        env = "AV_RELAY_DIRECT_IDS",
        value_delimiter = ','
    )]
    relay_direct_ids: Vec<String>,

    /// Send the repair symbols on the direct lanes too, not only the source
    /// symbols. The backbone secondary carries them to the distributor
    /// either way.
    #[arg(
        long,
        env = "AV_RELAY_DIRECT_REPAIR",
        default_value_t = false,
        action = clap::ArgAction::Set
    )]
    relay_direct_repair: bool,

    /// Class lanes: each RelaySession lane is a socket pair on its source
    /// endpoint (SO_REUSEPORT), so the parent's binding of the contributor
    /// is unchanged. Audio streams leave on one socket (SO_PRIORITY
    /// --relay-audio-socket-priority) to the parent's port P, video streams
    /// on the other to P + --relay-class-port-offset, and a 4K part's
    /// hundreds of symbols never sit ahead of an audio part in a socket
    /// buffer. A stream is video from its first part with video. With
    /// `false` every stream shares one socket and goes to P.
    ///
    /// Off by default: only a parent that listens on P + the offset (av-mesh
    /// --relay-class-threads with --relay-controlled-local, on Linux) takes
    /// the video class, and any other parent would lose every video part.
    /// The deploy and the lab render it on together with their parents.
    #[arg(
        long,
        env = "AV_RELAY_CLASS_LANES",
        default_value_t = false,
        action = clap::ArgAction::Set
    )]
    relay_class_lanes: bool,

    /// Port offset of the video class: a parent that receives audio on P
    /// receives video on P + this (the av-mesh receiver's convention).
    #[arg(
        long,
        env = "AV_RELAY_CLASS_PORT_OFFSET",
        default_value_t = class_lanes::DEFAULT_CLASS_PORT_OFFSET
    )]
    relay_class_port_offset: u16,

    /// SO_PRIORITY of the audio class sockets (0 leaves it alone). 6 puts
    /// them in band 0 of a `prio` qdisc's default priomap.
    #[arg(
        long,
        env = "AV_RELAY_AUDIO_SOCKET_PRIORITY",
        default_value_t = class_lanes::DEFAULT_AUDIO_SOCKET_PRIORITY
    )]
    relay_audio_socket_priority: u32,

    /// SO_MAX_PACING_RATE of the video class sockets in Mbit/s (0: not
    /// paced). It takes effect under the `fq` qdisc; set it to about 1.3
    /// times the 4K peak rate.
    #[arg(long, env = "AV_RELAY_VIDEO_PACING_MBPS", default_value_t = 0)]
    relay_video_pacing_mbps: u64,

    /// The contributor audio hot path: audio parts are published on the
    /// packager's own task (no hop through a publication worker's queue),
    /// and each lane is handed an audio part's symbols with non-blocking
    /// `sendmmsg`, one lane after the other with no join: what a full
    /// socket buffer does not take waits, in order, in that lane's backlog.
    /// With `false` audio parts queue for a worker and the lanes are joined
    /// per part, as video parts are.
    #[arg(
        long,
        env = "AV_RELAY_AUDIO_INLINE",
        default_value_t = true,
        action = clap::ArgAction::Set
    )]
    relay_audio_inline: bool,

    /// The primary lane carries the repair symbols of audio parts as well
    /// as their source set, so that the primary parent alone can recover a
    /// lost audio source (video parts keep today's rule: repair on the
    /// secondary only). The parent must accept repair on that lane (av-mesh
    /// --relay-primary-promoted, a source_and_repair carrier link), which
    /// av-mesh does not by default: off by default, and the deploy renders
    /// it on together with the parent's flag.
    #[arg(
        long,
        env = "AV_RELAY_REPAIR_BOTH_LANES",
        default_value_t = false,
        action = clap::ArgAction::Set
    )]
    relay_repair_both_lanes: bool,

    /// Source first: a part's source symbols are built straight from its
    /// envelope (RaptorQ is systematic) and handed to the lanes before its
    /// repair symbols are computed, which then use a cached RaptorQ
    /// encoding plan. relay-session's encoder generates the plan for every
    /// object and solves the whole block before the first symbol can leave:
    /// about 40-200 us of an audio part's `packaged` -> `sent` and several
    /// milliseconds of a 4K frame. The datagrams are byte for byte the
    /// same. With `false` every part goes through relay-session's encoder,
    /// source and repair together. Needs --relay-send-batch.
    #[arg(
        long,
        env = "AV_RELAY_SOURCE_FIRST",
        default_value_t = true,
        action = clap::ArgAction::Set
    )]
    relay_source_first: bool,

    /// RaptorQ symbol size of non-audio objects (video parts and
    /// initializations) in bytes, within the carrier's budget (0: the
    /// encoder's default, the largest that fits a 1,472-byte datagram).
    /// 1,088 keeps an edge's fast-lane datagram to a client at 1,128 bytes,
    /// which fits a WebTransport datagram. Audio parts keep their exact-fit
    /// symbol. Needs --relay-source-first.
    #[arg(long, env = "AV_RELAY_VIDEO_SYMBOL_SIZE", default_value_t = 0)]
    relay_video_symbol_size: u16,

    /// Trim a video object's RaptorQ symbol size a little (never above the
    /// configured size) so its source-symbol count K lands on a coarse grid
    /// (exact up to 32, then 16 steps per octave). A 4K part's size, and so
    /// its K, changes almost every frame; the repair's encoding plan is
    /// cached by K, and a plan missed is about 1.2 ms of CPU at K = 380.
    /// On the grid, every part size finds its plan after warm-up, for at
    /// most 1/16 more datagrams per part (about 3 % on average, each a few
    /// bytes shorter). With `false` symbols are the configured size and K
    /// is whatever the part's length gives. Needs --relay-source-first.
    #[arg(
        long,
        env = "AV_RELAY_VIDEO_PLAN_GRID",
        default_value_t = true,
        action = clap::ArgAction::Set
    )]
    relay_video_plan_grid: bool,

    /// Compute a large part's repair symbols on a blocking thread while its
    /// source symbols are being sent, so the repair is ready at about the
    /// later of the two instead of their sum (a 500 kB 4K part: sources
    /// about 2 ms, plan and solve about 2 ms). Parts of fewer than 16
    /// source symbols, and audio on the hot path, compute it inline after
    /// the sources as with `false`. Needs --relay-source-first.
    #[arg(
        long,
        env = "AV_RELAY_REPAIR_ALONGSIDE_SOURCES",
        default_value_t = true,
        action = clap::ArgAction::Set
    )]
    relay_repair_alongside_sources: bool,

    /// Run the AEP1 audio packagers (and, with --relay-audio-inline, the
    /// audio publication) on a thread of their own, with SCHED_FIFO
    /// --audio-hot-fifo-priority and pinned to --audio-hot-cpu where the
    /// host allows it (CAP_SYS_NICE, or root). A thread refused SCHED_FIFO
    /// is not pinned either, and says so in one info line. The DAW media
    /// UDP receive runs there too, so an AEP1 datagram never waits for a
    /// worker of the main runtime. With `false` they are tasks of the main
    /// runtime.
    #[arg(
        long,
        env = "AV_AUDIO_HOT_THREAD",
        default_value_t = true,
        action = clap::ArgAction::Set
    )]
    audio_hot_thread: bool,

    /// CPU of the audio thread (negative: not pinned).
    #[arg(
        long,
        env = "AV_AUDIO_HOT_CPU",
        default_value_t = 2,
        allow_negative_numbers = true
    )]
    audio_hot_cpu: i32,

    /// SCHED_FIFO priority of the audio thread (0: normal scheduling).
    #[arg(long, env = "AV_AUDIO_HOT_FIFO_PRIORITY", default_value_t = 10)]
    audio_hot_fifo_priority: u8,

    /// Audio hot threads, each with a single-threaded runtime of its own,
    /// SCHED_FIFO and thread `i` pinned to --audio-hot-cpu + `i`. The first
    /// runs the AEP1 receive and recovery; the rendition workers (one per
    /// source, group and format: the encode and the sends of its parts) go
    /// to the threads in turn from the second, so formats do not wait for
    /// each other's encodes. 0 (default): one per --daw-hls-formats format,
    /// at most as many as there are CPUs from --audio-hot-cpu on.
    #[arg(long, env = "AV_AUDIO_HOT_WORKERS", default_value_t = 0)]
    audio_hot_workers: usize,

    #[cfg(all(feature = "quadra-renditions", target_os = "linux"))]
    #[command(flatten)]
    quadra: QuadraCliArgs,
}

/// Sample rate at which a part size in samples is read when no stream is at
/// hand (status, the local playlist options).
const NOMINAL_SAMPLE_RATE: u32 = 48_000;
/// The sender's default epoch, 48 samples at 48 kHz: the nominal part of
/// one epoch per part where a number is needed before a stream starts.
const NOMINAL_EPOCH_US: u64 = 1_000;
/// The shortest Opus frame the DAW lane sends: 1 ms Opus Custom (48 samples).
const MIN_OPUS_FRAME_US: u64 = 1_000;

impl Args {
    /// Size of an audio part: `--audio-part`, else `--fmp4-part-ms`, else one
    /// epoch per part.
    fn audio_part(&self) -> AudioPartSize {
        self.audio_part
            .or(self.fmp4_part_ms.map(AudioPartSize::from_legacy_ms))
            .unwrap_or_default()
    }

    /// Size of a part of a stream with video: `--video-part`, else
    /// `--fmp4-video-part-ms`, else one frame per part.
    fn video_part(&self) -> VideoPartSize {
        self.video_part
            .or(self
                .fmp4_video_part_ms
                .map(|ms| VideoPartSize::Millis(ms.max(1))))
            .unwrap_or_default()
    }

    /// `min_part_ms` of the video segmenters: 0 is one frame per part.
    fn video_part_ms(&self) -> u32 {
        self.video_part().min_part_ms()
    }

    /// A warning when the old `--fmp4-part-ms` decides the audio part: the
    /// default is one epoch per part, and a launcher that still passes the
    /// old flag (the local Needletail stack passes 50) makes audio parts 20
    /// to 50 times longer than that without saying so.
    fn legacy_audio_part_warning(&self) -> Option<String> {
        if self.audio_part.is_some() {
            return None;
        }
        let ms = self.fmp4_part_ms?;
        Some(format!(
            "--fmp4-part-ms {ms} (or AV_LL_HLS_PART_MS) sets audio parts of {ms} ms; the default is \
             one AEP1 epoch per part (--audio-part epoch: 1 ms PCM, 1 ms Opus Custom). It no longer \
             sets the video part (--video-part)."
        ))
    }

    /// Parts a second of the DAW formats at the nominal part: a lossless
    /// rendition (PCM or FLAC) at the nominal epoch, Opus at no less than
    /// 1 ms (the lane's default Opus Custom frame; standard Opus stops at
    /// 2.5 ms). For the hop journal's coverage in time.
    fn nominal_audio_parts_per_second(&self) -> u64 {
        let part_us = self.nominal_audio_part_us().max(1);
        let lossless = self
            .daw_hls_formats
            .iter()
            .any(|format| matches!(format, DawHlsFormat::Pcm | DawHlsFormat::Flac));
        let opus = self
            .daw_hls_formats
            .iter()
            .any(|format| matches!(format, DawHlsFormat::Opus));
        u64::from(lossless) * (1_000_000 / part_us)
            + u64::from(opus) * (1_000_000 / part_us.max(MIN_OPUS_FRAME_US))
    }

    /// Shortest audio-only part of a segmenter that counts time (the fMP4
    /// segmenters of the video ingests): `0`, one access unit per part, for
    /// one epoch per part. The stamper declares the same size for these
    /// parts (`stamp_durations().audio_part_us`), so the two agree.
    fn audio_only_part_us(&self) -> u64 {
        self.audio_part().min_part_us(NOMINAL_SAMPLE_RATE)
    }

    /// The audio part in microseconds, with one epoch counted as the
    /// sender's default epoch.
    fn nominal_audio_part_us(&self) -> u64 {
        match self.audio_part() {
            AudioPartSize::Epoch => NOMINAL_EPOCH_US,
            part => part.min_part_us(NOMINAL_SAMPLE_RATE),
        }
    }

    /// The audio part in whole milliseconds, for the local playlist options
    /// and the old status field.
    fn nominal_audio_part_ms(&self) -> u32 {
        av_contrib::part_timing::ceil_ms(self.nominal_audio_part_us())
    }

    /// The durations that the contributor declares in the playlist stamp of
    /// each part. A part of one unit (epoch, frame) or of a number of
    /// samples declares `0`: the stamper takes the part target from the
    /// parts it sees.
    fn stamp_durations(&self) -> StampDurations {
        StampDurations {
            video_part_us: self.video_part().nominal_us(),
            video_segment_us: u64::from(
                self.fmp4_video_segment_ms
                    .unwrap_or(self.fmp4_segment_ms)
                    .max(1),
            ) * 1_000,
            audio_part_us: match self.audio_part() {
                AudioPartSize::Micros(us) => us,
                AudioPartSize::Epoch | AudioPartSize::Samples(_) => 0,
            },
            audio_segment_us: u64::from(self.fmp4_segment_ms.max(1)) * 1_000,
        }
    }
}

#[cfg(all(feature = "quadra-renditions", target_os = "linux"))]
#[derive(Debug, Clone, ClapArgs)]
struct QuadraCliArgs {
    /// Hardware rendition as STREAM_ID:WIDTHxHEIGHT:AVERAGE_BANDWIDTH:PEAK_BANDWIDTH.
    #[arg(
        long = "quadra-rendition",
        env = "AV_QUADRA_RENDITIONS",
        value_delimiter = ','
    )]
    renditions: Vec<QuadraRenditionSpec>,

    #[arg(long, env = "AV_QUADRA_SOURCE_WIDTH", default_value_t = 3_840)]
    source_width: u16,

    #[arg(long, env = "AV_QUADRA_SOURCE_HEIGHT", default_value_t = 2_160)]
    source_height: u16,

    #[arg(
        long,
        env = "AV_QUADRA_FRAME_RATE_MILLIHERTZ",
        default_value_t = 25_000
    )]
    frame_rate_millihertz: u32,

    #[arg(long, env = "AV_QUADRA_HARDWARE_ID")]
    hardware_id: Option<i32>,

    #[arg(long, env = "AV_QUADRA_QUEUE_CAPACITY", default_value_t = 256)]
    queue_capacity: usize,

    /// Publish only the hardware-derived renditions. Use this on a separate
    /// VPU contributor so the primary contributor remains the sole publisher
    /// of the canonical source stream.
    #[arg(long, env = "AV_QUADRA_DERIVED_ONLY")]
    derived_only: bool,
}

#[derive(Debug, Clone, Copy, ValueEnum)]
enum DawHlsPackaging {
    Opaque,
    Fmp4,
}

#[derive(Debug, Clone, Copy, ValueEnum)]
enum DawHlsFormat {
    Opus,
    Flac,
    Pcm,
}

impl From<DawHlsFormat> for AudioPayloadKind {
    fn from(value: DawHlsFormat) -> Self {
        match value {
            DawHlsFormat::Opus => Self::Opus,
            DawHlsFormat::Flac => Self::Flac,
            DawHlsFormat::Pcm => Self::Pcm,
        }
    }
}

impl From<DawHlsPackaging> for AudioEpochHlsPackaging {
    fn from(value: DawHlsPackaging) -> Self {
        match value {
            DawHlsPackaging::Opaque => Self::Opaque,
            DawHlsPackaging::Fmp4 => Self::Fmp4,
        }
    }
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

impl From<RistProfile> for UploadRistProfile {
    fn from(profile: RistProfile) -> Self {
        match profile {
            RistProfile::Simple => Self::Simple,
            RistProfile::Main => Self::Main,
        }
    }
}

#[derive(Debug, Clone, Copy)]
struct RistIngestConfig {
    bind: SocketAddr,
    profile: RistProfile,
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

/// The UDP carrier of one RelaySession parent: a socket bound to the source
/// endpoint that the parent registered for this contributor and connected to
/// the parent. It carries RLS1 datagrams exactly as
/// `relay_session::PrivateUdpTransport` does, and can hand the symbols of a
/// part to the kernel in one batch.
///
/// With `--relay-class-lanes` the lane is a socket pair on the same source
/// endpoint (`SO_REUSEPORT`): `socket` carries the audio streams to the
/// parent's port P with `SO_PRIORITY` 6, `video` carries the video streams
/// to P + `--relay-class-port-offset`. The parent's binding of the
/// contributor (its source address and port) is the same for both.
struct RelayLane {
    /// Audio streams, or every stream without class lanes; connected to
    /// `peer_addr`.
    socket: Arc<UdpSocket>,
    /// Video streams with class lanes; connected to `video_peer_addr`.
    video: Option<Arc<UdpSocket>>,
    peer_addr: SocketAddr,
    video_peer_addr: SocketAddr,
    limits: RelayLimits,
    path_metrics: std::sync::RwLock<PathMetrics>,
    /// Audio parts whose symbols the socket's buffer could not take at once
    /// (`--relay-audio-inline`): sent, in order, by a task of the lane.
    backlog: LaneBacklog,
}

/// The class lane settings: `--relay-class-lanes` and its socket options.
#[derive(Debug, Clone, Copy)]
struct ClassLaneSettings {
    /// `0` when class lanes are off.
    port_offset: u16,
    audio_priority: Option<u32>,
    video_pacing_bytes_per_second: Option<u64>,
}

impl ClassLaneSettings {
    fn from_args(args: &Args) -> Result<Self> {
        if !args.relay_class_lanes {
            return Ok(Self::off());
        }
        if args.relay_class_port_offset == 0 {
            bail!("--relay-class-lanes needs a non-zero --relay-class-port-offset");
        }
        Ok(Self {
            port_offset: args.relay_class_port_offset,
            audio_priority: (args.relay_audio_socket_priority > 0)
                .then_some(args.relay_audio_socket_priority),
            video_pacing_bytes_per_second: (args.relay_video_pacing_mbps > 0)
                .then(|| args.relay_video_pacing_mbps.saturating_mul(1_000_000) / 8),
        })
    }

    const fn off() -> Self {
        Self {
            port_offset: 0,
            audio_priority: None,
            video_pacing_bytes_per_second: None,
        }
    }

    const fn enabled(&self) -> bool {
        self.port_offset > 0
    }

    fn audio_options(&self) -> ClassSocketOptions {
        ClassSocketOptions {
            reuse_port: true,
            priority: self.audio_priority,
            max_pacing_rate_bytes_per_second: None,
        }
    }

    fn video_options(&self) -> ClassSocketOptions {
        ClassSocketOptions {
            reuse_port: true,
            priority: None,
            max_pacing_rate_bytes_per_second: self.video_pacing_bytes_per_second,
        }
    }
}

/// Bind the class socket pair of one source endpoint: audio connected to
/// `audio_peer` (when given), video to `video_peer`.
fn bind_class_pair(
    bind: SocketAddr,
    audio_peer: Option<SocketAddr>,
    video_peer: Option<SocketAddr>,
    classes: ClassLaneSettings,
) -> std::io::Result<(UdpSocket, UdpSocket)> {
    let audio = class_lanes::bind_class_socket(bind, audio_peer, classes.audio_options())?;
    // An ephemeral bind takes the port that the audio socket got.
    let bound = audio.local_addr()?;
    let video = class_lanes::bind_class_socket(bound, video_peer, classes.video_options())?;
    Ok((UdpSocket::from_std(audio)?, UdpSocket::from_std(video)?))
}

impl RelayLane {
    async fn bind(config: PrivateUdpConfig, classes: ClassLaneSettings) -> std::io::Result<Self> {
        let (socket, video, video_peer_addr) = if classes.enabled() {
            let video_peer = class_lanes::video_class_addr(config.peer_addr, classes.port_offset)?;
            let (audio, video) = bind_class_pair(
                config.bind_addr,
                Some(config.peer_addr),
                Some(video_peer),
                classes,
            )?;
            (audio, Some(Arc::new(video)), video_peer)
        } else {
            let socket = UdpSocket::bind(config.bind_addr).await?;
            socket.connect(config.peer_addr).await?;
            (socket, None, config.peer_addr)
        };
        Ok(Self {
            socket: Arc::new(socket),
            video,
            peer_addr: config.peer_addr,
            video_peer_addr,
            limits: config.limits,
            path_metrics: std::sync::RwLock::new(PathMetrics::default()),
            backlog: LaneBacklog::default(),
        })
    }

    #[cfg(test)]
    fn local_addr(&self) -> std::io::Result<SocketAddr> {
        self.socket.local_addr()
    }

    /// The socket and parent address of parts of `class`.
    fn class_socket(&self, class: PublicationClass) -> (&Arc<UdpSocket>, SocketAddr) {
        match (class, &self.video) {
            (PublicationClass::Video, Some(video)) => (video, self.video_peer_addr),
            _ => (&self.socket, self.peer_addr),
        }
    }

    fn set_path_metrics(&self, metrics: PathMetrics) {
        *self
            .path_metrics
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = metrics.normalized();
    }

    #[cfg(test)]
    fn path_metrics(&self) -> PathMetrics {
        *self
            .path_metrics
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

/// The direct lanes to the distributors of the contributor's region: one
/// unconnected socket bound to `--relay-direct-bind`, one target each.
struct RelayDirectLanes {
    /// Audio streams (every stream without class lanes).
    socket: UdpSocket,
    /// Video streams with class lanes, from the same source endpoint, to
    /// each target's port plus the class port offset.
    video: Option<UdpSocket>,
    targets: Vec<SocketAddr>,
    video_targets: Vec<SocketAddr>,
    limits: RelayLimits,
    repair: bool,
}

impl RelayDirectLanes {
    /// The socket and targets of parts of `class`.
    fn class_lane(&self, class: PublicationClass) -> (&UdpSocket, &[SocketAddr]) {
        match (class, &self.video) {
            (PublicationClass::Video, Some(video)) => (video, &self.video_targets),
            _ => (&self.socket, &self.targets),
        }
    }

    async fn bind(
        args: &Args,
        limits: RelayLimits,
        local_id: &str,
        classes: ClassLaneSettings,
    ) -> Result<Option<Self>> {
        let Some(first) = args.relay_direct_targets.first().copied() else {
            if args.relay_direct_bind.is_some() || !args.relay_direct_ids.is_empty() {
                bail!("--relay-direct-bind and --relay-direct-id require --relay-direct-target");
            }
            return Ok(None);
        };
        if !args.relay_direct_ids.is_empty()
            && args.relay_direct_ids.len() != args.relay_direct_targets.len()
        {
            bail!(
                "--relay-direct-id must be given once for each --relay-direct-target ({} ids, {} targets)",
                args.relay_direct_ids.len(),
                args.relay_direct_targets.len()
            );
        }
        let bind = relay_bind_addr("--relay-direct-bind", args.relay_direct_bind, first)?;
        for (index, target) in args.relay_direct_targets.iter().enumerate() {
            if args.relay_direct_targets[..index].contains(target) {
                bail!("--relay-direct-target {target} is given twice");
            }
            if target.is_ipv4() != bind.is_ipv4() {
                bail!("--relay-direct-target {target} and --relay-direct-bind must use the same address family");
            }
            // The carrier identities are validated as the backbone lanes'.
            PrivateUdpConfig::new(bind, *target, local_id, direct_lane_id(args, index), limits)
                .with_context(|| format!("invalid direct RelaySession lane to {target}"))?;
        }
        let (socket, video, video_targets) = if classes.enabled() {
            let video_targets = args
                .relay_direct_targets
                .iter()
                .map(|target| class_lanes::video_class_addr(*target, classes.port_offset))
                .collect::<std::io::Result<Vec<_>>>()
                .context("invalid video class port of a --relay-direct-target")?;
            let (audio, video) = bind_class_pair(bind, None, None, classes).with_context(|| {
                format!("failed to bind direct RelaySession class sockets on {bind}")
            })?;
            (audio, Some(video), video_targets)
        } else {
            let socket = UdpSocket::bind(bind).await.with_context(|| {
                format!("failed to bind direct RelaySession UDP carrier on {bind}")
            })?;
            (socket, None, args.relay_direct_targets.clone())
        };
        Ok(Some(Self {
            socket,
            video,
            targets: args.relay_direct_targets.clone(),
            video_targets,
            limits,
            repair: args.relay_direct_repair,
        }))
    }

    #[cfg(test)]
    fn local_addr(&self) -> std::io::Result<SocketAddr> {
        self.socket.local_addr()
    }
}

/// `--relay-video-gso-pace-mbps` in bytes per second of payload.
fn video_gso_pace_bytes_per_second(args: &Args) -> u64 {
    args.relay_video_gso_pace_mbps.saturating_mul(1_000_000) / 8
}

/// Node id of direct lane `index`.
fn direct_lane_id(args: &Args, index: usize) -> String {
    args.relay_direct_ids
        .get(index)
        .cloned()
        .unwrap_or_else(|| format!("needletail-distributor-{}", index + 1))
}

/// Parts of a lane's audio socket waiting for room in its buffer.
///
/// On the audio hot path (`--relay-audio-inline`) each lane is handed its
/// symbols with non-blocking `sendmmsg` and nothing ever waits for a socket:
/// what a full buffer did not take goes, in order, to the lane's backlog
/// thread, and the part's other lanes and the packager go on. While a lane
/// has a part queued, its next parts queue behind it, so a lane's datagrams
/// always leave in order.
///
/// The backlog is a thread of its own, not a task: it waits for room with
/// `poll(2)` on the socket, so it neither runs on whichever runtime happened
/// to queue the first part (the audio hot thread, where it would drain only
/// when the inline publishes yield) nor waits for the main runtime's I/O
/// driver (busy with 4K encodes) to report the socket writable.
#[derive(Default)]
struct LaneBacklog {
    /// Parts queued and not yet sent.
    pending: Arc<AtomicUsize>,
    sender: std::sync::OnceLock<std::sync::mpsc::SyncSender<BacklogPart>>,
}

/// Parts that one lane's backlog holds before it drops a part.
const LANE_BACKLOG_PARTS: usize = 256;

/// Longest single wait of a backlog for room in its socket's buffer; it
/// looks at the part's deadline again after each.
const LANE_BACKLOG_POLL_MS: u64 = 100;

/// The rest of one part on one lane.
struct BacklogPart {
    wires: Vec<(MediaDatagramRole, Bytes)>,
    /// Source datagrams at the front of `wires`: `stage` is journalled
    /// once they are with the kernel.
    sources: usize,
    deadline: MediaDeadline,
    /// The part whose `stage` the backlog journals, when the lane has not
    /// journalled it yet.
    journal: Option<(u64, u64)>,
    /// The part, for `sent_first`.
    part: Option<(u64, u64)>,
    /// The part's `sent_first` flag, when this lane had sent none of the
    /// part's datagrams: the first lane to send one journals `sent_first`.
    sent_first: Option<Arc<AtomicBool>>,
    stage: HopStage,
    peer: SocketAddr,
    /// The lane, whose object outcome the backlog records when the part is
    /// sent, dropped or expired.
    path: RelayCarrierPath,
    /// Whether this is the lane's last hand-off of the object, which
    /// records the lane's object outcome (with --relay-source-first a lane
    /// can be handed an object's source symbols and then its repair).
    records_outcome: bool,
}

impl LaneBacklog {
    fn busy(&self) -> bool {
        self.pending.load(Ordering::Acquire) > 0
    }

    /// Queue `part` behind the lane's earlier parts. A full queue drops it,
    /// counts it and records the lane's part as failed.
    fn push(&self, socket: &Arc<UdpSocket>, telemetry: &Arc<IngestTelemetry>, part: BacklogPart) {
        let sender = self.sender.get_or_init(|| {
            start_lane_backlog(
                Arc::clone(socket),
                Arc::clone(telemetry),
                Arc::clone(&self.pending),
            )
        });
        self.pending.fetch_add(1, Ordering::AcqRel);
        telemetry
            .relay_lane_backlog_parts
            .fetch_add(1, Ordering::Relaxed);
        if let Err(error) = sender.try_send(part) {
            self.pending.fetch_sub(1, Ordering::AcqRel);
            let part = match error {
                std::sync::mpsc::TrySendError::Full(part)
                | std::sync::mpsc::TrySendError::Disconnected(part) => part,
            };
            if part.records_outcome {
                telemetry.record_relay_session_lane_object(part.path, false);
            }
            let dropped = telemetry
                .relay_lane_backlog_dropped
                .fetch_add(1, Ordering::Relaxed)
                + 1;
            if dropped <= 16 || dropped.is_power_of_two() {
                warn!(
                    dropped,
                    peer = %part.peer,
                    "RelaySession lane backlog is full; dropped the rest of an audio part"
                );
            }
        }
    }
}

/// Start the backlog thread of one lane's socket.
fn start_lane_backlog(
    socket: Arc<UdpSocket>,
    telemetry: Arc<IngestTelemetry>,
    pending: Arc<AtomicUsize>,
) -> std::sync::mpsc::SyncSender<BacklogPart> {
    let (sender, receiver) = std::sync::mpsc::sync_channel::<BacklogPart>(LANE_BACKLOG_PARTS);
    let started = std::thread::Builder::new()
        .name("lane-backlog".into())
        .spawn(move || {
            // Started from an audio hot thread it would inherit that
            // thread's SCHED_FIFO and CPU: it runs normally, on any CPU.
            let _ = class_lanes::reset_current_thread_scheduling();
            while let Ok(part) = receiver.recv() {
                send_backlog_part(&socket, &telemetry, part);
                pending.fetch_sub(1, Ordering::AcqRel);
            }
        });
    if let Err(error) = started {
        // The receiver is gone with the closure: every part is dropped and
        // counted.
        warn!(error = %error, "failed to start a RelaySession lane backlog thread");
    }
    sender
}

/// Send the rest of one part: the source set first, then the repair,
/// waiting for room in the socket's buffer until the part's deadline.
fn send_backlog_part(socket: &UdpSocket, telemetry: &IngestTelemetry, part: BacklogPart) {
    let expire = |unsent: &[(MediaDatagramRole, Bytes)]| {
        telemetry
            .relay_expired_symbols
            .fetch_add(unsent.len() as u64, Ordering::Relaxed);
        for (role, _) in unsent {
            telemetry.record_relay_session_send_error(*role);
        }
        if part.records_outcome {
            telemetry.record_relay_session_lane_object(part.path, false);
        }
    };
    if part.deadline.is_expired_at(now_unix_us()) {
        expire(&part.wires);
        return;
    }
    let messages = part
        .wires
        .iter()
        .map(|(_, wire)| udp_batch::Outbound::connected(wire))
        .collect::<Vec<_>>();
    let sources = part.sources.min(messages.len());
    let journal_sent_first = |sent: bool| {
        if let Some(flag) = part.sent_first.as_ref() {
            if sent && !flag.swap(true, Ordering::AcqRel) {
                journal_part_hop(part.part, HopStage::SentFirst, Some(part.peer));
            }
        }
    };
    let mut sent = 0;
    let mut result = Ok(());
    for (from, to) in [(0, sources), (sources, messages.len())] {
        if from == to {
            continue;
        }
        let outcome = send_until_taken(socket, &messages[from..to], part.deadline);
        sent += outcome.sent;
        journal_sent_first(outcome.sent > 0);
        if let Some(error) = outcome.error {
            result = Err(error);
            break;
        }
        if to == sources || sources == 0 {
            journal_part_hop(part.journal, part.stage, Some(part.peer));
        }
    }
    for (role, wire) in &part.wires[..sent] {
        telemetry.record_relay_session_send_success(*role, wire.len() as u64);
    }
    match result {
        Ok(()) => {
            if part.records_outcome {
                telemetry.record_relay_session_lane_object(part.path, true);
            }
        }
        Err(error) if error.kind() == std::io::ErrorKind::TimedOut => {
            expire(&part.wires[sent..]);
        }
        Err(error) => {
            let role = part
                .wires
                .get(sent)
                .map_or(MediaDatagramRole::Source, |(role, _)| *role);
            telemetry.record_relay_session_send_error(role);
            if part.records_outcome {
                telemetry.record_relay_session_lane_object(part.path, false);
            }
            debug!(error = %error, peer = %part.peer, "RelaySession lane backlog send failed");
        }
    }
}

/// Hand `messages` to the kernel in order, waiting with `poll(2)` for room
/// in the socket's buffer, until `deadline` (then `TimedOut`).
fn send_until_taken(
    socket: &UdpSocket,
    messages: &[udp_batch::Outbound<'_>],
    deadline: MediaDeadline,
) -> udp_batch::TrySent {
    let mut total = udp_batch::TrySent::default();
    while total.sent < messages.len() {
        let outcome = udp_batch::try_send_all(socket, &messages[total.sent..]);
        total.sent += outcome.sent;
        total.calls += outcome.calls;
        if !outcome.would_block() {
            total.error = outcome.error;
            return total;
        }
        let now_us = now_unix_us();
        if deadline.is_expired_at(now_us) {
            total.error = Some(std::io::Error::new(
                std::io::ErrorKind::TimedOut,
                "RelaySession part expired waiting for room in the socket buffer",
            ));
            return total;
        }
        let wait_ms =
            (deadline.expires_at_us.saturating_sub(now_us) / 1_000).clamp(1, LANE_BACKLOG_POLL_MS);
        if let Err(error) = udp_batch::wait_writable(socket, Duration::from_millis(wait_ms)) {
            total.error = Some(error);
            return total;
        }
    }
    total
}

/// Journal `stage` of the part `journal` (stream, sequence), now.
fn journal_part_hop(journal: Option<(u64, u64)>, stage: HopStage, peer: Option<SocketAddr>) {
    if let Some((stream, seq)) = journal {
        hop_journal::record(
            HopEvent::new(stage, stream, seq, hop_journal::now_unix_us()).with_peer(peer),
        );
    }
}

/// How long a stream stays on the video class after its last video part.
/// A muxed stream sends video many times a second; one whose video stopped
/// this long ago (or a stream id reused by an audio-only source) goes back
/// to the audio class.
const VIDEO_CLASS_HOLD_US: u64 = 10_000_000;

/// Streams remembered on the video class before the stale ones are dropped.
const VIDEO_CLASS_STREAMS_MAX: usize = 4_096;

/// The streams that are on the video class (`--relay-class-lanes`): a
/// stream is video from its first part with video, for its source epoch,
/// and while it keeps sending video (see [`VIDEO_CLASS_HOLD_US`]).
///
/// Audio parts only read it, through an `ArcSwap` snapshot: the audio hot
/// path takes no lock that a video publication holds. Video parts refresh
/// their stream's time with an atomic store, and only a stream's first
/// video part (or its first in a new epoch) writes a new snapshot.
#[derive(Default)]
struct VideoStreamClasses {
    streams: arc_swap::ArcSwap<HashMap<u64, Arc<VideoStreamMark>>>,
}

struct VideoStreamMark {
    epoch: u64,
    last_video_unix_us: AtomicU64,
}

impl VideoStreamClasses {
    fn class_of(
        &self,
        stream_id: u64,
        epoch: u64,
        class: PublicationClass,
        now_us: u64,
    ) -> PublicationClass {
        let current = self.streams.load();
        let mark = current.get(&stream_id).filter(|mark| mark.epoch == epoch);
        match class {
            PublicationClass::Video => {
                if let Some(mark) = mark {
                    mark.last_video_unix_us.store(now_us, Ordering::Relaxed);
                    return PublicationClass::Video;
                }
                drop(current);
                self.streams.rcu(|streams| {
                    let mut streams = HashMap::clone(streams);
                    if streams.len() >= VIDEO_CLASS_STREAMS_MAX {
                        streams.retain(|_, mark| {
                            now_us.saturating_sub(mark.last_video_unix_us.load(Ordering::Relaxed))
                                < VIDEO_CLASS_HOLD_US
                        });
                    }
                    streams.insert(
                        stream_id,
                        Arc::new(VideoStreamMark {
                            epoch,
                            last_video_unix_us: AtomicU64::new(now_us),
                        }),
                    );
                    streams
                });
                PublicationClass::Video
            }
            PublicationClass::Audio
                if mark.is_some_and(|mark| {
                    now_us.saturating_sub(mark.last_video_unix_us.load(Ordering::Relaxed))
                        < VIDEO_CLASS_HOLD_US
                }) =>
            {
                PublicationClass::Video
            }
            PublicationClass::Audio => PublicationClass::Audio,
        }
    }
}

/// What one lane did with one part on the audio hot path.
enum LaneHandOff {
    /// Every datagram is with the kernel.
    Sent,
    /// The rest waits in the lane's backlog.
    Queued,
}

struct RelaySessionPublisher {
    /// One RaptorQ encoder, or with `--publication-shard-mix` one for audio
    /// parts and one for video parts, so that an audio part never waits for
    /// the encoder while a video part of hundreds of KB is encoded. The
    /// encoders' block ids and packet sequences are their own; a relay
    /// decodes each object, keyed by its object key, on its own.
    encoders: Vec<Mutex<RaptorQObjectEncoder>>,
    /// `--relay-source-first`: an encoder per entry of `encoders`, with the
    /// same controller, used in their place. Its counters are atomics: no
    /// publishing thread waits for another's encode.
    source_encoders: Option<Vec<SourceFirstEncoder>>,
    /// `--relay-repair-alongside-sources`.
    repair_alongside_sources: bool,
    primary: RelayLane,
    secondary: Option<RelayLane>,
    secondary_seed_source: bool,
    /// `--relay-direct-target`: the distributors of the contributor's own
    /// region, fed beside the backbone.
    direct: Option<RelayDirectLanes>,
    /// `--relay-send-batch`: the symbols of a part leave each lane in one
    /// `sendmmsg` call.
    send_batch: bool,
    /// `--relay-video-gso`: a video part's symbols leave each lane as UDP
    /// GSO messages. Cleared for good when the host refuses UDP GSO.
    video_gso: Arc<AtomicBool>,
    /// The size of those messages: `--relay-video-gso-segments`, one
    /// millisecond of `--relay-video-pacing-mbps` on paced class sockets,
    /// and 50 us of `--relay-video-gso-pace-mbps`.
    video_gso_limits: udp_batch::GsoLimits,
    /// `--relay-video-gso-pace-mbps` in bytes per second (0: unpaced).
    video_gso_pace_bytes_per_second: u64,
    /// `--relay-audio-inline`: an audio part's lanes are each handed their
    /// symbols without waiting, one after the other, with no join.
    audio_inline: bool,
    /// `--relay-repair-both-lanes`: the primary lane carries the repair
    /// symbols too, not only the source set.
    repair_both_lanes: bool,
    /// `--relay-class-lanes`: the class of a part's stream picks its
    /// socket and port. Without class lanes a part's own class is used, as
    /// before.
    class_lanes: bool,
    /// Streams that have published a video part: they stay on the video
    /// class (a muxed stream's audio-only parts before its first video
    /// access unit are the only ones of it on the audio class).
    video_streams: VideoStreamClasses,
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
                || !args.relay_direct_targets.is_empty()
                || args.relay_direct_bind.is_some()
            {
                bail!("relay bind, secondary and direct settings require --relay-primary-target");
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
        for target in &args.relay_direct_targets {
            endpoints.push(("direct target", *target));
        }
        if let Some(bind) = args.relay_direct_bind {
            endpoints.push(("direct bind", bind));
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
        let classes = ClassLaneSettings::from_args(args)?;
        if classes.enabled() {
            // A parent's video class port must not be another endpoint.
            for (label, target) in [
                ("primary target", Some(primary_target)),
                ("secondary target", args.relay_secondary_target),
            ] {
                let Some(target) = target else { continue };
                let video = class_lanes::video_class_addr(target, classes.port_offset)
                    .with_context(|| format!("no video class port for the relay {label}"))?;
                if let Some((other, _)) = endpoints.iter().find(|(_, addr)| *addr == video) {
                    bail!("the video class port {video} of the relay {label} is the relay {other}");
                }
            }
        }
        let primary_path_metrics = relay_path_metrics(args)?;
        let secondary_path_metrics = relay_secondary_path_metrics(args)?;
        let primary = RelayLane::bind(
            PrivateUdpConfig::new(
                primary_bind,
                primary_target,
                args.relay_local_id.clone(),
                args.relay_primary_id.clone(),
                limits,
            )?,
            classes,
        )
        .await
        .with_context(|| {
            format!("failed to bind primary RelaySession UDP carrier for {primary_target}")
        })?;
        primary.set_path_metrics(primary_path_metrics);
        let secondary = if let (Some(secondary_target), Some(secondary_bind)) =
            (args.relay_secondary_target, secondary_bind)
        {
            let transport = RelayLane::bind(
                PrivateUdpConfig::new(
                    secondary_bind,
                    secondary_target,
                    args.relay_local_id.clone(),
                    args.relay_secondary_id.clone(),
                    limits,
                )?,
                classes,
            )
            .await
            .with_context(|| {
                format!("failed to bind secondary RelaySession UDP carrier for {secondary_target}")
            })?;
            transport.set_path_metrics(secondary_path_metrics);
            Some(transport)
        } else {
            None
        };
        let direct = RelayDirectLanes::bind(args, limits, &args.relay_local_id, classes).await?;

        let mut policy = AdaptiveFecPolicy::default();
        if args.relay_min_repair_symbols > policy.max_repair_symbols {
            bail!(
                "--relay-min-repair-symbols cannot exceed {}",
                policy.max_repair_symbols
            );
        }
        policy.min_repair_symbols = args.relay_min_repair_symbols;
        let mut encoder = RaptorQObjectEncoder::new(
            AdaptiveFecController::new(policy, CongestionConfig::default()),
            limits,
        )?;
        encoder.update_path_metrics(adaptive_relay_path_metrics(
            primary_path_metrics,
            secondary_path_metrics,
        ));

        let encoders = if args.publication_shard_mix {
            vec![Mutex::new(encoder.clone()), Mutex::new(encoder.clone())]
        } else {
            vec![Mutex::new(encoder.clone())]
        };
        let source_encoders = if args.relay_send_batch && args.relay_source_first {
            let encoders = (0..encoders.len())
                .map(|_| {
                    SourceFirstEncoder::new(encoder.controller().clone(), limits).map(|fast| {
                        fast.with_video_symbol_size(args.relay_video_symbol_size)
                            .with_video_plan_grid(args.relay_video_plan_grid)
                    })
                })
                .collect::<std::result::Result<Vec<_>, _>>()
                .context("invalid source-first RelaySession encoder")?;
            Some(encoders)
        } else {
            None
        };
        Ok(Some(Self {
            encoders,
            source_encoders,
            repair_alongside_sources: args.relay_repair_alongside_sources,
            primary,
            secondary,
            secondary_seed_source: args.relay_secondary_seed_source,
            direct,
            send_batch: args.relay_send_batch,
            video_gso: Arc::new(AtomicBool::new(
                args.relay_send_batch && args.relay_video_gso,
            )),
            video_gso_limits: udp_batch::GsoLimits::new(
                usize::from(args.relay_video_gso_segments),
                classes.video_pacing_bytes_per_second,
            )
            .paced(video_gso_pace_bytes_per_second(args)),
            video_gso_pace_bytes_per_second: video_gso_pace_bytes_per_second(args),
            audio_inline: args.relay_audio_inline,
            repair_both_lanes: args.relay_repair_both_lanes,
            class_lanes: classes.enabled(),
            video_streams: VideoStreamClasses::default(),
            generation: TopologyGeneration::new(args.relay_topology_generation)?,
            subscription_id: SubscriptionId::new(args.relay_subscription_id)?,
            telemetry,
        }))
    }

    /// Publish an object of stream `stream_id` that is not a media part
    /// (an initialization).
    async fn publish_object(
        &self,
        object: &MediaObject,
        stream_id: u64,
        class: PublicationClass,
    ) -> Result<RelayPublishOutcome> {
        let class = self.stream_class(stream_id, object.key().epoch(), class);
        self.publish_classed_object(object, None, class).await
    }

    /// The class of an object of `stream_id` (source epoch `epoch`) whose
    /// own class is `class`. With class lanes a stream that has published
    /// video stays video (see [`VideoStreamClasses`]); without them the
    /// object's own class is used, as before class lanes.
    fn stream_class(
        &self,
        stream_id: u64,
        epoch: u64,
        class: PublicationClass,
    ) -> PublicationClass {
        if !self.class_lanes {
            return class;
        }
        self.video_streams
            .class_of(stream_id, epoch, class, now_unix_us())
    }

    /// The source-first encoder of parts of `class`, with
    /// `--relay-source-first`.
    fn source_encoder(&self, class: PublicationClass) -> Option<&SourceFirstEncoder> {
        let encoders = self.source_encoders.as_ref()?;
        match class {
            PublicationClass::Audio => encoders.first(),
            PublicationClass::Video => encoders.last(),
        }
    }

    /// The encoder of parts of `class`.
    fn encoder(&self, class: PublicationClass) -> &Mutex<RaptorQObjectEncoder> {
        match class {
            PublicationClass::Audio => &self.encoders[0],
            PublicationClass::Video => self.encoders.last().unwrap_or(&self.encoders[0]),
        }
    }

    /// Publish an object; `part` is the (stream, sequence) of a media part,
    /// whose encode and sends the hop journal records.
    async fn publish_part_object(
        &self,
        object: &MediaObject,
        part: Option<(u64, u64)>,
        class: PublicationClass,
    ) -> Result<RelayPublishOutcome> {
        let class = match part {
            Some((stream_id, _)) => self.stream_class(stream_id, object.key().epoch(), class),
            None => class,
        };
        self.publish_classed_object(object, part, class).await
    }

    async fn publish_classed_object(
        &self,
        object: &MediaObject,
        part: Option<(u64, u64)>,
        class: PublicationClass,
    ) -> Result<RelayPublishOutcome> {
        let started = Instant::now();
        let result = self.publish_object_inner(object, part, class).await;
        self.telemetry
            .record_relay_session_stage_duration(RelayPipelineStage::Total, started.elapsed());
        result
    }

    async fn publish_object_inner(
        &self,
        object: &MediaObject,
        part: Option<(u64, u64)>,
        class: PublicationClass,
    ) -> Result<RelayPublishOutcome> {
        let journal = part.and_then(|part| hop_journal::global().map(|journal| (journal, part)));
        let journal_hop = |stage: HopStage, peer: Option<SocketAddr>| {
            if let Some((journal, (stream, seq))) = journal {
                journal.record(
                    HopEvent::new(stage, stream, seq, hop_journal::now_unix_us()).with_peer(peer),
                );
            }
        };
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
        if let Some(encoder) = self.source_encoder(class) {
            return self
                .publish_source_first(encoder, object, part, class, deadline, &journal_hop)
                .await;
        }
        let encode_wait_started = Instant::now();
        let mut encoder = self.encoder(class).lock().await;
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
        journal_hop(HopStage::Encoded, None);

        let schedule_started = Instant::now();
        let source_count = source_symbols.len();
        let repair_count = repair_symbols.len();
        // Without a secondary the primary is the only lane for the repair
        // symbols; with --relay-repair-both-lanes it carries an audio part's
        // anyway. A 4K part keeps today's rule (source on the primary,
        // repair from the secondary): its repair on the primary would add
        // R/K of 4K load at every hop.
        let repair_uses_primary = self.secondary.is_none()
            || (self.repair_both_lanes && class == PublicationClass::Audio);
        if self.secondary.is_none() && repair_count > 0 {
            self.telemetry
                .relay_repair_primary_fallback_objects
                .fetch_add(1, Ordering::Relaxed);
        }
        self.telemetry.record_relay_session_stage_duration(
            RelayPipelineStage::Schedule,
            schedule_started.elapsed(),
        );
        // Shared with the lanes' backlogs: a lane whose socket buffer was
        // full may send the part's first datagram after the call returns.
        let sent_first = Arc::new(AtomicBool::new(false));
        let journal_sent_first = |peer: SocketAddr| {
            if !sent_first.swap(true, Ordering::AcqRel) {
                journal_hop(HopStage::SentFirst, Some(peer));
            }
        };

        let (primary_result, secondary_result) = if self.audio_inline
            && self.send_batch
            && class == PublicationClass::Audio
        {
            // The audio hot path: each lane in turn is handed its symbols
            // without waiting; what a full socket buffer does not take
            // waits in that lane's backlog, and nothing joins the lanes.
            let primary_groups = if repair_uses_primary {
                vec![source_symbols.as_slice(), repair_symbols.as_slice()]
            } else {
                vec![source_symbols.as_slice()]
            };
            let primary_result = self.hand_lane_its_symbols(
                &self.primary,
                RelayCarrierPath::Primary,
                &primary_groups,
                part,
                Some(HopStage::Sent),
                Some(&sent_first),
                true,
            );
            let secondary_result = match self.secondary.as_ref() {
                Some(secondary) => {
                    let groups = if self.secondary_seed_source {
                        vec![source_symbols.as_slice(), repair_symbols.as_slice()]
                    } else {
                        vec![repair_symbols.as_slice()]
                    };
                    self.hand_lane_its_symbols(
                        secondary,
                        RelayCarrierPath::Secondary,
                        &groups,
                        part,
                        Some(HopStage::SentSecondary),
                        Some(&sent_first),
                        true,
                    )
                }
                None => Ok(LaneHandOff::Sent),
            };
            if let Some(direct) = self.direct.as_ref() {
                self.send_direct(
                    direct,
                    class,
                    &source_symbols,
                    &repair_symbols,
                    &journal_hop,
                    true,
                )
                .await;
            }
            (primary_result, secondary_result)
        } else {
            let primary_lane = async {
                let (_, primary_peer) = self.primary.class_socket(class);
                if self.send_batch {
                    // Source and repair in one batch: the source symbols lead
                    // it, so they are with the kernel when `sent` is
                    // journalled.
                    let symbols = if repair_uses_primary {
                        vec![source_symbols.as_slice(), repair_symbols.as_slice()]
                    } else {
                        vec![source_symbols.as_slice()]
                    };
                    self.send_symbol_batch(
                        &self.primary,
                        class,
                        RelayCarrierPath::Primary,
                        &symbols,
                    )
                    .await?;
                    journal_sent_first(primary_peer);
                    journal_hop(HopStage::Sent, Some(primary_peer));
                    return Ok::<(), anyhow::Error>(());
                }
                for symbol in &source_symbols {
                    self.send_symbol(&self.primary, class, RelayCarrierPath::Primary, symbol)
                        .await?;
                    journal_sent_first(primary_peer);
                }
                journal_hop(HopStage::Sent, Some(primary_peer));
                if repair_uses_primary {
                    for symbol in &repair_symbols {
                        self.send_symbol(&self.primary, class, RelayCarrierPath::Primary, symbol)
                            .await?;
                    }
                }
                Ok::<(), anyhow::Error>(())
            };
            let secondary_lane = async {
                if let Some(secondary) = self.secondary.as_ref() {
                    let (_, secondary_peer) = secondary.class_socket(class);
                    if self.send_batch {
                        let symbols = if self.secondary_seed_source {
                            vec![source_symbols.as_slice(), repair_symbols.as_slice()]
                        } else {
                            vec![repair_symbols.as_slice()]
                        };
                        self.send_symbol_batch(
                            secondary,
                            class,
                            RelayCarrierPath::Secondary,
                            &symbols,
                        )
                        .await?;
                        journal_sent_first(secondary_peer);
                        journal_hop(HopStage::SentSecondary, Some(secondary_peer));
                        return Ok::<(), anyhow::Error>(());
                    }
                    if self.secondary_seed_source {
                        for symbol in &source_symbols {
                            self.send_symbol(secondary, class, RelayCarrierPath::Secondary, symbol)
                                .await?;
                            journal_sent_first(secondary_peer);
                        }
                        journal_hop(HopStage::SentSecondary, Some(secondary_peer));
                    }
                    for symbol in &repair_symbols {
                        self.send_symbol(secondary, class, RelayCarrierPath::Secondary, symbol)
                            .await?;
                        journal_sent_first(secondary_peer);
                    }
                    // A secondary without the source set decodes from its
                    // repair symbols: they are what it needs.
                    if !self.secondary_seed_source {
                        journal_hop(HopStage::SentSecondary, Some(secondary_peer));
                    }
                }
                Ok::<(), anyhow::Error>(())
            };
            let direct_lane = async {
                if let Some(direct) = self.direct.as_ref() {
                    self.send_direct(
                        direct,
                        class,
                        &source_symbols,
                        &repair_symbols,
                        &journal_hop,
                        true,
                    )
                    .await;
                }
            };
            // In this order: the backbone primary, the region's
            // distributors, then the warm secondary.
            let (primary_result, (), secondary_result) =
                tokio::join!(primary_lane, direct_lane, secondary_lane);
            (
                primary_result.map(|()| LaneHandOff::Sent),
                secondary_result.map(|()| LaneHandOff::Sent),
            )
        };
        self.settle_lanes(
            primary_result,
            secondary_result,
            deadline,
            RelayPublishOutcome {
                announcement,
                source_symbols: source_count,
                repair_symbols: repair_count,
            },
        )
    }

    /// `--relay-source-first`: publish `object` with its source symbols
    /// handed to every lane that carries them before its repair symbols are
    /// sent, then the repair to the lanes that carry it. A large part's
    /// repair is computed on a blocking thread while its sources are sent
    /// (`--relay-repair-alongside-sources`), other parts' after. The lanes and
    /// the journal are those of the publication without it: audio parts take
    /// the hot path (each lane handed its symbols without waiting, no join),
    /// other parts the joined batches; `sent` is still the last source
    /// symbol with the kernel on the primary.
    async fn publish_source_first(
        &self,
        encoder: &SourceFirstEncoder,
        object: &MediaObject,
        part: Option<(u64, u64)>,
        class: PublicationClass,
        deadline: MediaDeadline,
        journal_hop: &impl Fn(HopStage, Option<SocketAddr>),
    ) -> Result<RelayPublishOutcome> {
        let encode_started = Instant::now();
        let encoded =
            encoder.encode_sources(object, self.generation, self.subscription_id, deadline);
        let mut encode_time = encode_started.elapsed();
        let encoded = match encoded {
            Ok(encoded) => encoded,
            Err(error) => {
                self.telemetry.record_relay_session_encode_error();
                self.telemetry
                    .record_relay_session_stage_duration(RelayPipelineStage::Encode, encode_time);
                return Err(error).context("failed to RaptorQ-protect canonical media object");
            }
        };
        journal_hop(HopStage::Encoded, None);
        let encoded = Arc::new(encoded);
        let hot = self.audio_inline && class == PublicationClass::Audio;
        // `--relay-repair-alongside-sources`: a large part's repair is
        // computed on a blocking thread while its sources are sent.
        let early_repair = (self.repair_alongside_sources
            && !hot
            && encoded.repair_count() > 0
            && encoded.source_wires().len() >= REPAIR_ALONGSIDE_MIN_SOURCES)
            .then(|| {
                let part = Arc::clone(&encoded);
                tokio::task::spawn_blocking(move || {
                    let started = Instant::now();
                    let repair = part.repair_wires();
                    (repair, started.elapsed())
                })
            });
        // One set of datagrams for every lane: they are the same bytes.
        let sources = encoded
            .source_wires()
            .iter()
            .map(|wire| (MediaDatagramRole::Source, wire.clone()))
            .collect::<Vec<_>>();
        let source_count = sources.len();
        let repair_count = encoded.repair_count() as usize;
        // The lanes of today's rule (see publish_object_inner).
        let repair_uses_primary = self.secondary.is_none()
            || (self.repair_both_lanes && class == PublicationClass::Audio);
        if self.secondary.is_none() && repair_count > 0 {
            self.telemetry
                .relay_repair_primary_fallback_objects
                .fetch_add(1, Ordering::Relaxed);
        }
        let primary_repair = repair_uses_primary && repair_count > 0;
        let secondary_sources = self.secondary_seed_source;
        let secondary_repair = repair_count > 0;
        let direct_repair = self.direct.as_ref().is_some_and(|direct| direct.repair);
        let sent_first = Arc::new(AtomicBool::new(false));
        let journal_sent_first = |peer: SocketAddr| {
            if !sent_first.swap(true, Ordering::AcqRel) {
                journal_hop(HopStage::SentFirst, Some(peer));
            }
        };

        // The source symbols.
        let (mut primary_result, mut secondary_result) = if hot {
            let primary = self.hand_lane_wires(
                &self.primary,
                RelayCarrierPath::Primary,
                sources.clone(),
                source_count,
                deadline,
                part,
                Some(HopStage::Sent),
                Some(&sent_first),
                !primary_repair,
                Instant::now(),
            );
            let secondary = match self.secondary.as_ref() {
                Some(secondary) if secondary_sources => self.hand_lane_wires(
                    secondary,
                    RelayCarrierPath::Secondary,
                    sources.clone(),
                    source_count,
                    deadline,
                    part,
                    Some(HopStage::SentSecondary),
                    Some(&sent_first),
                    !secondary_repair,
                    Instant::now(),
                ),
                _ => Ok(LaneHandOff::Sent),
            };
            if let Some(direct) = self.direct.as_ref() {
                self.send_direct_wires(direct, class, encoded.source_wires(), journal_hop, true)
                    .await;
            }
            (primary, secondary)
        } else {
            let primary_lane = async {
                let (_, peer) = self.primary.class_socket(class);
                self.send_wire_batch(
                    &self.primary,
                    class,
                    RelayCarrierPath::Primary,
                    &sources,
                    deadline,
                )
                .await?;
                journal_sent_first(peer);
                journal_hop(HopStage::Sent, Some(peer));
                Ok::<_, anyhow::Error>(LaneHandOff::Sent)
            };
            let direct_lane = async {
                if let Some(direct) = self.direct.as_ref() {
                    self.send_direct_wires(
                        direct,
                        class,
                        encoded.source_wires(),
                        journal_hop,
                        true,
                    )
                    .await;
                }
            };
            let secondary_lane = async {
                match self.secondary.as_ref() {
                    Some(secondary) if secondary_sources => {
                        let (_, peer) = secondary.class_socket(class);
                        self.send_wire_batch(
                            secondary,
                            class,
                            RelayCarrierPath::Secondary,
                            &sources,
                            deadline,
                        )
                        .await?;
                        journal_sent_first(peer);
                        journal_hop(HopStage::SentSecondary, Some(peer));
                        Ok::<_, anyhow::Error>(LaneHandOff::Sent)
                    }
                    _ => Ok(LaneHandOff::Sent),
                }
            };
            let (primary, (), secondary) = tokio::join!(primary_lane, direct_lane, secondary_lane);
            (primary, secondary)
        };
        drop(sources);

        // The repair symbols, after the sources left (computed alongside
        // them, or now).
        if repair_count > 0 {
            let repair = match early_repair {
                Some(task) => match task.await {
                    Ok((repair, took)) => {
                        encode_time += took;
                        repair.map_err(anyhow::Error::new)
                    }
                    Err(error) => Err(anyhow::Error::new(error)),
                },
                None => {
                    let repair_started = Instant::now();
                    let repair = encoded.repair_wires();
                    encode_time += repair_started.elapsed();
                    repair.map_err(anyhow::Error::new)
                }
            };
            let repair = match repair {
                Ok(repair) => repair,
                Err(error) => {
                    // The sources are out; the lanes that needed repair
                    // fail the part.
                    self.telemetry.record_relay_session_encode_error();
                    let error = error.context("failed to compute RaptorQ repair symbols");
                    if primary_repair {
                        primary_result = Err(anyhow::anyhow!("{error:#}"));
                    }
                    secondary_result = Err(error);
                    Vec::new()
                }
            };
            let repair_wires = repair
                .iter()
                .map(|wire| (MediaDatagramRole::Repair, wire.clone()))
                .collect::<Vec<_>>();
            if !repair_wires.is_empty() {
                if hot {
                    if primary_repair && primary_result.is_ok() {
                        primary_result = self.hand_lane_wires(
                            &self.primary,
                            RelayCarrierPath::Primary,
                            repair_wires.clone(),
                            repair_wires.len(),
                            deadline,
                            part,
                            None,
                            None,
                            true,
                            Instant::now(),
                        );
                    }
                    if let Some(secondary) = self.secondary.as_ref() {
                        if secondary_result.is_ok() {
                            secondary_result = self.hand_lane_wires(
                                secondary,
                                RelayCarrierPath::Secondary,
                                repair_wires.clone(),
                                repair_wires.len(),
                                deadline,
                                part,
                                // A secondary without the source set decodes
                                // from its repair symbols: they are its `sent`.
                                (!secondary_sources).then_some(HopStage::SentSecondary),
                                Some(&sent_first),
                                true,
                                Instant::now(),
                            );
                        }
                    }
                    if direct_repair {
                        if let Some(direct) = self.direct.as_ref() {
                            self.send_direct_wires(direct, class, &repair, journal_hop, false)
                                .await;
                        }
                    }
                } else {
                    let primary_lane = async {
                        if primary_repair && primary_result.is_ok() {
                            self.send_wire_batch(
                                &self.primary,
                                class,
                                RelayCarrierPath::Primary,
                                &repair_wires,
                                deadline,
                            )
                            .await?;
                        }
                        Ok::<_, anyhow::Error>(())
                    };
                    let direct_lane = async {
                        if direct_repair {
                            if let Some(direct) = self.direct.as_ref() {
                                self.send_direct_wires(direct, class, &repair, journal_hop, false)
                                    .await;
                            }
                        }
                    };
                    let secondary_lane = async {
                        match self.secondary.as_ref() {
                            Some(secondary) if secondary_result.is_ok() => {
                                let (_, peer) = secondary.class_socket(class);
                                self.send_wire_batch(
                                    secondary,
                                    class,
                                    RelayCarrierPath::Secondary,
                                    &repair_wires,
                                    deadline,
                                )
                                .await?;
                                journal_sent_first(peer);
                                if !secondary_sources {
                                    journal_hop(HopStage::SentSecondary, Some(peer));
                                }
                                Ok::<_, anyhow::Error>(())
                            }
                            _ => Ok(()),
                        }
                    };
                    let (primary, (), secondary) =
                        tokio::join!(primary_lane, direct_lane, secondary_lane);
                    if let Err(error) = primary {
                        primary_result = Err(error);
                    }
                    if let Err(error) = secondary {
                        secondary_result = Err(error);
                    }
                }
            }
        }
        self.telemetry
            .record_relay_session_stage_duration(RelayPipelineStage::Encode, encode_time);
        self.settle_lanes(
            primary_result,
            secondary_result,
            deadline,
            RelayPublishOutcome {
                announcement: encoded.announcement.clone(),
                source_symbols: source_count,
                repair_symbols: repair_count,
            },
        )
    }

    /// The end of a part's publication: record each lane's outcome (unless
    /// its backlog will), fail the part when no lane carried it, and count
    /// the deadline.
    fn settle_lanes(
        &self,
        primary_result: Result<LaneHandOff>,
        secondary_result: Result<LaneHandOff>,
        deadline: MediaDeadline,
        outcome: RelayPublishOutcome,
    ) -> Result<RelayPublishOutcome> {
        let secondary_configured = self.secondary.is_some();
        // A lane that left part of the part in its backlog records the
        // part's outcome when the backlog has sent it, dropped it or let it
        // expire: queued is not yet delivered.
        let queued = |result: &Result<LaneHandOff>| matches!(result, Ok(LaneHandOff::Queued));
        if !queued(&primary_result) {
            self.telemetry.record_relay_session_lane_object(
                RelayCarrierPath::Primary,
                primary_result.is_ok(),
            );
        }
        if secondary_configured && !queued(&secondary_result) {
            self.telemetry.record_relay_session_lane_object(
                RelayCarrierPath::Secondary,
                secondary_result.is_ok(),
            );
        }
        let primary_result = primary_result.map(|_| ());
        let secondary_result = secondary_result.map(|_| ());
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
        Ok(outcome)
    }

    /// Send a part's symbols to every direct distributor: each symbol to
    /// each target in turn, so every distributor has the first source
    /// symbol before any has the last. A failure is counted, journalled as
    /// no `sent_direct`, and never fails the part: the backbone carries it.
    /// `whole_part` is false for the repair symbols that source first sends
    /// after the sources: they neither journal nor count the part again.
    async fn send_direct(
        &self,
        direct: &RelayDirectLanes,
        class: PublicationClass,
        source_symbols: &[relay_session::RelayDatagram],
        repair_symbols: &[relay_session::RelayDatagram],
        journal_hop: &impl Fn(HopStage, Option<SocketAddr>),
        whole_part: bool,
    ) {
        let (_, targets) = direct.class_lane(class);
        let symbols = if direct.repair {
            source_symbols.len() + repair_symbols.len()
        } else {
            source_symbols.len()
        };
        let mut wires = Vec::with_capacity(symbols);
        for symbol in source_symbols
            .iter()
            .chain(repair_symbols.iter().filter(|_| direct.repair))
        {
            match encode_relay_datagram(symbol, direct.limits) {
                Ok(wire) => wires.push(Bytes::from(wire)),
                Err(error) => {
                    self.telemetry
                        .relay_direct_lane_objects_failed
                        .fetch_add(targets.len() as u64, Ordering::Relaxed);
                    debug!(error = %error, "failed to encode a direct RelaySession datagram");
                    return;
                }
            }
        }
        self.send_direct_wires(direct, class, &wires, journal_hop, whole_part)
            .await;
    }

    /// [`Self::send_direct`] of datagrams already encoded (the repair ones
    /// only when the direct lanes carry repair: the caller's choice).
    async fn send_direct_wires(
        &self,
        direct: &RelayDirectLanes,
        class: PublicationClass,
        wires: &[Bytes],
        journal_hop: &impl Fn(HopStage, Option<SocketAddr>),
        whole_part: bool,
    ) {
        let (socket, targets) = direct.class_lane(class);
        if wires.is_empty() {
            return;
        }
        let mut messages = Vec::with_capacity(wires.len() * targets.len());
        for wire in wires {
            for target in targets {
                messages.push(udp_batch::Outbound::to(*target, wire));
            }
        }
        let started = Instant::now();
        // One distributor that the kernel refuses (no route, a firewall
        // rule) stops only its own datagrams: the others still get theirs.
        let outcome = udp_batch::send_all_per_target(socket, &messages, self.send_batch).await;
        self.telemetry
            .relay_direct_send_duration
            .record(started.elapsed());
        let part_bytes = wires.iter().map(Bytes::len).sum::<usize>() as u64;
        let mut succeeded = 0_u64;
        for target in targets {
            if !outcome.refused(*target) {
                if whole_part {
                    journal_hop(HopStage::SentDirect, Some(*target));
                }
                succeeded += 1;
            }
        }
        self.telemetry
            .relay_direct_datagrams
            .fetch_add(outcome.sent as u64, Ordering::Relaxed);
        self.telemetry
            .relay_direct_datagram_bytes
            .fetch_add(part_bytes * succeeded, Ordering::Relaxed);
        if !whole_part {
            return;
        }
        self.telemetry
            .relay_direct_lane_objects_succeeded
            .fetch_add(succeeded, Ordering::Relaxed);
        for (target, error) in &outcome.refused {
            let failed = self
                .telemetry
                .relay_direct_lane_objects_failed
                .fetch_add(1, Ordering::Relaxed)
                + 1;
            if failed <= 16 || failed.is_power_of_two() {
                warn!(
                    error = %error,
                    target = ?target,
                    failed,
                    "direct RelaySession lane failed; the backbone carries the part"
                );
            }
        }
    }

    /// The audio hot path of one lane: hand the lane the symbols of
    /// `groups` with non-blocking `sendmmsg`, never waiting for the socket.
    /// What its buffer does not take (or everything, while earlier parts
    /// wait there) goes to the lane's backlog. `stage` is journalled when
    /// the leading group (the source set, or the only group) is with the
    /// kernel (`None`: nothing); `sent_first` when the first datagram is.
    /// `records_outcome` is false when the lane will be handed more of the
    /// object (source first: its repair), whose hand-off records it.
    #[allow(clippy::too_many_arguments)]
    fn hand_lane_its_symbols(
        &self,
        lane: &RelayLane,
        path: RelayCarrierPath,
        groups: &[&[relay_session::RelayDatagram]],
        part: Option<(u64, u64)>,
        stage: Option<HopStage>,
        sent_first: Option<&Arc<AtomicBool>>,
        records_outcome: bool,
    ) -> Result<LaneHandOff> {
        let Some(first) = groups.iter().find_map(|group| group.first()) else {
            return Ok(LaneHandOff::Sent);
        };
        let started = Instant::now();
        let deadline = first.deadline;
        let leading = groups
            .iter()
            .find(|group| !group.is_empty())
            .map_or(0, |group| group.len());
        let wires = self.lane_wires(groups, lane.limits)?;
        self.hand_lane_wires(
            lane,
            path,
            wires,
            leading,
            deadline,
            part,
            stage,
            sent_first,
            records_outcome,
            started,
        )
    }

    /// The RLS1 datagrams of `groups`, in order; an expired symbol fails
    /// them all.
    fn lane_wires(
        &self,
        groups: &[&[relay_session::RelayDatagram]],
        limits: RelayLimits,
    ) -> Result<Vec<(MediaDatagramRole, Bytes)>> {
        let now_us = now_unix_us();
        let mut wires = Vec::with_capacity(groups.iter().map(|group| group.len()).sum());
        for symbol in groups.iter().flat_map(|group| group.iter()) {
            if symbol.deadline.is_expired_at(now_us) {
                self.telemetry.record_relay_session_send_error(symbol.role);
                self.telemetry
                    .relay_expired_symbols
                    .fetch_add(1, Ordering::Relaxed);
                bail!("RelaySession symbol expired before carrier send");
            }
            match encode_relay_datagram(symbol, limits) {
                Ok(wire) => wires.push((symbol.role, Bytes::from(wire))),
                Err(error) => {
                    self.telemetry.record_relay_session_send_error(symbol.role);
                    return Err(error).context("failed to encode RelaySession datagram");
                }
            }
        }
        Ok(wires)
    }

    /// [`Self::hand_lane_its_symbols`] of datagrams already encoded: the
    /// first `leading` of `wires` are the leading group.
    #[allow(clippy::too_many_arguments)]
    fn hand_lane_wires(
        &self,
        lane: &RelayLane,
        path: RelayCarrierPath,
        mut wires: Vec<(MediaDatagramRole, Bytes)>,
        leading: usize,
        deadline: MediaDeadline,
        part: Option<(u64, u64)>,
        stage: Option<HopStage>,
        sent_first: Option<&Arc<AtomicBool>>,
        records_outcome: bool,
        started: Instant,
    ) -> Result<LaneHandOff> {
        let journal = stage.and(part);
        let stage = stage.unwrap_or(HopStage::Sent);
        let Some((first_role, _)) = wires.first() else {
            return Ok(LaneHandOff::Sent);
        };
        let send_stage = relay_send_stage(path, *first_role);
        let (socket, peer) = lane.class_socket(PublicationClass::Audio);
        if deadline.is_expired_at(now_unix_us()) {
            for (role, _) in &wires {
                self.telemetry.record_relay_session_send_error(*role);
            }
            self.telemetry
                .relay_expired_symbols
                .fetch_add(wires.len() as u64, Ordering::Relaxed);
            bail!("RelaySession symbol expired before carrier send");
        }
        let backlog_part = |wires, sources, journal, sent_first| BacklogPart {
            wires,
            sources,
            deadline,
            journal,
            part,
            sent_first,
            stage,
            peer,
            path,
            records_outcome,
        };
        if lane.backlog.busy() {
            lane.backlog.push(
                socket,
                &self.telemetry,
                backlog_part(wires, leading, journal, sent_first.map(Arc::clone)),
            );
            self.telemetry
                .record_relay_session_stage_duration(send_stage, started.elapsed());
            return Ok(LaneHandOff::Queued);
        }
        let messages = wires
            .iter()
            .map(|(_, wire)| udp_batch::Outbound::connected(wire))
            .collect::<Vec<_>>();
        let outcome = udp_batch::try_send_all(socket, &messages);
        drop(messages);
        if outcome.sent > 0
            && sent_first.is_some_and(|sent_first| !sent_first.swap(true, Ordering::AcqRel))
        {
            journal_part_hop(part, HopStage::SentFirst, Some(peer));
        }
        let journalled = outcome.sent >= leading;
        if journalled {
            journal_part_hop(journal, stage, Some(peer));
        }
        for (role, wire) in &wires[..outcome.sent] {
            self.telemetry
                .record_relay_session_send_success(*role, wire.len() as u64);
        }
        let result = if outcome.would_block() {
            let rest = wires.split_off(outcome.sent);
            let sources = leading.saturating_sub(outcome.sent);
            lane.backlog.push(
                socket,
                &self.telemetry,
                backlog_part(
                    rest,
                    sources,
                    (!journalled).then_some(journal).flatten(),
                    sent_first.filter(|_| outcome.sent == 0).map(Arc::clone),
                ),
            );
            Ok(LaneHandOff::Queued)
        } else if let Some(error) = outcome.error {
            let role = wires
                .get(outcome.sent)
                .map_or(MediaDatagramRole::Source, |(role, _)| *role);
            self.telemetry.record_relay_session_send_error(role);
            Err(error).with_context(|| format!("failed to send RelaySession datagrams to {peer}"))
        } else {
            Ok(LaneHandOff::Sent)
        };
        self.telemetry
            .record_relay_session_stage_duration(send_stage, started.elapsed());
        result
    }

    /// Send the symbols of `groups`, in order, in as few `sendmmsg` calls as
    /// the kernel allows. Every symbol of an object carries the object's
    /// deadline, so one check covers the batch.
    async fn send_symbol_batch(
        &self,
        lane: &RelayLane,
        class: PublicationClass,
        path: RelayCarrierPath,
        groups: &[&[relay_session::RelayDatagram]],
    ) -> Result<()> {
        let Some(first) = groups.iter().find_map(|group| group.first()) else {
            return Ok(());
        };
        let started = Instant::now();
        let stage = relay_send_stage(path, first.role);
        let result = match self.lane_wires(groups, lane.limits) {
            Ok(wires) => {
                self.send_wire_batch_inner(lane, class, &wires, first.deadline)
                    .await
            }
            Err(error) => Err(error),
        };
        self.telemetry
            .record_relay_session_stage_duration(stage, started.elapsed());
        result
    }

    /// [`Self::send_symbol_batch`] of datagrams already encoded, of an
    /// object with `deadline`.
    async fn send_wire_batch(
        &self,
        lane: &RelayLane,
        class: PublicationClass,
        path: RelayCarrierPath,
        wires: &[(MediaDatagramRole, Bytes)],
        deadline: MediaDeadline,
    ) -> Result<()> {
        let Some((first_role, _)) = wires.first() else {
            return Ok(());
        };
        let started = Instant::now();
        let stage = relay_send_stage(path, *first_role);
        let result = if deadline.is_expired_at(now_unix_us()) {
            for (role, _) in wires {
                self.telemetry.record_relay_session_send_error(*role);
            }
            self.telemetry
                .relay_expired_symbols
                .fetch_add(wires.len() as u64, Ordering::Relaxed);
            Err(anyhow::anyhow!(
                "RelaySession symbol expired before carrier send"
            ))
        } else {
            self.send_wire_batch_inner(lane, class, wires, deadline)
                .await
        };
        self.telemetry
            .record_relay_session_stage_duration(stage, started.elapsed());
        result
    }

    async fn send_wire_batch_inner(
        &self,
        lane: &RelayLane,
        class: PublicationClass,
        wires: &[(MediaDatagramRole, Bytes)],
        deadline: MediaDeadline,
    ) -> Result<()> {
        let (socket, peer) = lane.class_socket(class);
        let messages = wires
            .iter()
            .map(|(_, wire)| udp_batch::Outbound::connected(wire))
            .collect::<Vec<_>>();
        let sent = if class == PublicationClass::Video {
            let sent = if self.video_gso_pace_bytes_per_second > 0
                && self.video_gso.load(Ordering::Relaxed)
            {
                // Paced: on a blocking thread, which sleeps between the
                // messages (a lane's part is a few milliseconds at most).
                drop(messages);
                let socket = Arc::clone(socket);
                let wires = wires
                    .iter()
                    .map(|(_, wire)| wire.clone())
                    .collect::<Vec<_>>();
                let limits = self.video_gso_limits;
                let pace = self.video_gso_pace_bytes_per_second;
                let gso = Arc::clone(&self.video_gso);
                let give_up = std::time::Instant::now()
                    + Duration::from_micros(deadline.expires_at_us.saturating_sub(now_unix_us()));
                tokio::task::spawn_blocking(move || {
                    let messages = wires
                        .iter()
                        .map(|wire| udp_batch::Outbound::connected(wire))
                        .collect::<Vec<_>>();
                    udp_batch::send_all_gso_or_plain_paced(
                        &socket, &messages, limits, pace, &gso, give_up,
                    )
                })
                .await
                .unwrap_or_else(|error| {
                    Err(udp_batch::Stopped {
                        sent: 0,
                        calls: 0,
                        error: std::io::Error::other(error),
                    })
                })
            } else {
                udp_batch::send_all_gso_or_plain(
                    socket,
                    &messages,
                    self.video_gso_limits,
                    &self.video_gso,
                )
                .await
            };
            sent.map(|sent| {
                if let Some(error) = &sent.turned_off {
                    // The rest of this part, and every later part, went
                    // one datagram each.
                    warn!(
                        %error,
                        "UDP GSO is unavailable: video symbols leave one datagram each"
                    );
                }
                self.telemetry
                    .relay_gso_messages
                    .fetch_add(sent.gso_messages as u64, Ordering::Relaxed);
                self.telemetry
                    .relay_gso_datagrams
                    .fetch_add(sent.gso_datagrams as u64, Ordering::Relaxed);
            })
        } else {
            udp_batch::send_all_until_refused(socket, &messages, true)
                .await
                .map(drop)
        };
        if let Err(stopped) = sent {
            for (role, wire) in &wires[..stopped.sent] {
                self.telemetry
                    .record_relay_session_send_success(*role, wire.len() as u64);
            }
            let role = wires
                .get(stopped.sent)
                .map_or(MediaDatagramRole::Source, |(role, _)| *role);
            self.telemetry.record_relay_session_send_error(role);
            return Err(stopped.error)
                .with_context(|| format!("failed to send RelaySession datagrams to {peer}"));
        }
        for (role, wire) in wires {
            self.telemetry
                .record_relay_session_send_success(*role, wire.len() as u64);
        }
        Ok(())
    }

    async fn send_symbol(
        &self,
        transport: &RelayLane,
        class: PublicationClass,
        path: RelayCarrierPath,
        symbol: &relay_session::RelayDatagram,
    ) -> Result<()> {
        let (socket, peer) = transport.class_socket(class);
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
            let wire = match encode_relay_datagram(symbol, transport.limits) {
                Ok(wire) => wire,
                Err(error) => {
                    self.telemetry.record_relay_session_send_error(role);
                    self.telemetry.record_relay_session_stage_duration(
                        relay_send_stage(path, role),
                        started.elapsed(),
                    );
                    return Err(error).context("failed to encode RelaySession datagram");
                }
            };
            let wire_bytes = wire.len() as u64;
            if let Err(error) =
                udp_batch::send_each(socket, &[udp_batch::Outbound::connected(&wire)]).await
            {
                self.telemetry.record_relay_session_send_error(role);
                Err(error)
                    .with_context(|| format!("failed to send RelaySession datagram to {peer}"))
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

type StreamSourceEpoch = (u64, u64);
type Fmp4InitializationObject = (ObjectKey, u64);
type Fmp4InitializationIndex = HashMap<StreamSourceEpoch, Fmp4InitializationObject>;
type Fmp4InitializationPayloadIndex = HashMap<StreamSourceEpoch, Bytes>;

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
    audio_epoch_ingress_targets: Arc<Vec<SocketAddr>>,
    next_media_sequence: Arc<AtomicU64>,
    source_epoch: u64,
    /// Read lock-free (`ArcSwap`): an audio part on the audio hot thread
    /// never waits for a video part's update. Written only when a part
    /// carries an initialization.
    fmp4_initializations: Arc<arc_swap::ArcSwap<Fmp4InitializationIndex>>,
    fmp4_initialization_payloads: Arc<arc_swap::ArcSwap<Fmp4InitializationPayloadIndex>>,
    delivery_budget_ms: u64,
    estimated_clock_error_ns: u64,
    relay: Option<Arc<RelaySessionPublisher>>,
    relay_exclusive: bool,
    telemetry: Arc<IngestTelemetry>,
}

fn configured_audio_epoch_ingress_targets(args: &Args) -> Result<Vec<SocketAddr>> {
    let primary = args
        .audio_epoch_ingress_target
        .unwrap_or(args.mesh_media_fec_target);
    let mut targets = vec![primary];
    if let Some(redundant) = args.audio_epoch_redundant_ingress_target {
        if redundant == primary {
            bail!("AEP1 primary and redundant ingress targets must be distinct");
        }
        targets.push(redundant);
    }
    targets.sort_unstable();
    targets.dedup();
    Ok(targets)
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

        let audio_epoch_ingress_targets = configured_audio_epoch_ingress_targets(args)?;
        AUDIO_EPOCH_INGRESS_TARGETS
            .store(audio_epoch_ingress_targets.len() as u64, Ordering::Relaxed);

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
            audio_epoch_ingress_targets: Arc::new(audio_epoch_ingress_targets),
            next_media_sequence: Arc::new(AtomicU64::new(0)),
            source_epoch,
            fmp4_initializations: Arc::default(),
            fmp4_initialization_payloads: Arc::default(),
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
        for target in self.audio_epoch_ingress_targets.iter().copied() {
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

struct QueuedAudioEpochIngressDatagram {
    peer: SocketAddr,
    bytes: Bytes,
    enqueued_at: Instant,
}

fn handoff_audio_epoch_hls_datagram(
    tx: Option<&mpsc::Sender<AudioEpochHlsDatagram>>,
    peer: SocketAddr,
    datagram: &[u8],
    received_unix_us: u64,
) {
    let Some(tx) = tx else {
        return;
    };
    let depth = tx.max_capacity().saturating_sub(tx.capacity()) as u64;
    AUDIO_EPOCH_HLS_QUEUE_DEPTH.store(depth, Ordering::Relaxed);
    AUDIO_EPOCH_HLS_QUEUE_MAX_DEPTH.fetch_max(depth, Ordering::Relaxed);
    if tx.capacity() == 0 {
        let dropped_total = AUDIO_EPOCH_HLS_QUEUE_DROPPED.fetch_add(1, Ordering::Relaxed) + 1;
        if should_log_audio_epoch_hls_drop(dropped_total) {
            warn!(
                peer = %peer,
                dropped_total,
                queue_depth = depth,
                "AEP1 LL-HLS handoff is full; datagram lanes remain live"
            );
        }
        return;
    }

    if let Err(error) = tx.try_send(AudioEpochHlsDatagram {
        peer,
        bytes: Bytes::copy_from_slice(datagram),
        received_unix_us,
    }) {
        let depth = tx.max_capacity().saturating_sub(tx.capacity()) as u64;
        AUDIO_EPOCH_HLS_QUEUE_DEPTH.store(depth, Ordering::Relaxed);
        AUDIO_EPOCH_HLS_QUEUE_MAX_DEPTH.fetch_max(depth, Ordering::Relaxed);
        let dropped_total = AUDIO_EPOCH_HLS_QUEUE_DROPPED.fetch_add(1, Ordering::Relaxed) + 1;
        if should_log_audio_epoch_hls_drop(dropped_total) {
            warn!(
                peer = %peer,
                error = %error,
                dropped_total,
                queue_depth = depth,
                "AEP1 LL-HLS handoff is full; datagram lanes remain live"
            );
        }
    } else {
        AUDIO_EPOCH_HLS_QUEUE_ENQUEUED.fetch_add(1, Ordering::Relaxed);
        let depth = tx.max_capacity().saturating_sub(tx.capacity()) as u64;
        AUDIO_EPOCH_HLS_QUEUE_DEPTH.store(depth, Ordering::Relaxed);
        AUDIO_EPOCH_HLS_QUEUE_MAX_DEPTH.fetch_max(depth, Ordering::Relaxed);
    }
}

fn handoff_audio_epoch_mesh_datagram(
    tx: Option<&mpsc::Sender<QueuedAudioEpochIngressDatagram>>,
    peer: SocketAddr,
    datagram: &[u8],
) {
    let Some(tx) = tx else {
        return;
    };
    let depth = tx.max_capacity().saturating_sub(tx.capacity()) as u64;
    AUDIO_EPOCH_MESH_QUEUE_DEPTH.store(depth, Ordering::Relaxed);
    AUDIO_EPOCH_MESH_QUEUE_MAX_DEPTH.fetch_max(depth, Ordering::Relaxed);
    if tx.capacity() == 0 {
        let dropped_total = AUDIO_EPOCH_MESH_QUEUE_DROPPED.fetch_add(1, Ordering::Relaxed) + 1;
        if should_log_audio_epoch_mesh_drop(dropped_total) {
            warn!(
                peer = %peer,
                dropped_total,
                queue_depth = depth,
                "AEP1 mesh forward handoff is full; UDP receive remains live"
            );
        }
        return;
    }

    if let Err(error) = tx.try_send(QueuedAudioEpochIngressDatagram {
        peer,
        bytes: Bytes::copy_from_slice(datagram),
        enqueued_at: Instant::now(),
    }) {
        let depth = tx.max_capacity().saturating_sub(tx.capacity()) as u64;
        AUDIO_EPOCH_MESH_QUEUE_DEPTH.store(depth, Ordering::Relaxed);
        AUDIO_EPOCH_MESH_QUEUE_MAX_DEPTH.fetch_max(depth, Ordering::Relaxed);
        let dropped_total = AUDIO_EPOCH_MESH_QUEUE_DROPPED.fetch_add(1, Ordering::Relaxed) + 1;
        if should_log_audio_epoch_mesh_drop(dropped_total) {
            warn!(
                peer = %peer,
                error = %error,
                dropped_total,
                queue_depth = depth,
                "AEP1 mesh forward handoff is full; UDP receive remains live"
            );
        }
    } else {
        AUDIO_EPOCH_MESH_QUEUE_ENQUEUED.fetch_add(1, Ordering::Relaxed);
        let depth = tx.max_capacity().saturating_sub(tx.capacity()) as u64;
        AUDIO_EPOCH_MESH_QUEUE_DEPTH.store(depth, Ordering::Relaxed);
        AUDIO_EPOCH_MESH_QUEUE_MAX_DEPTH.fetch_max(depth, Ordering::Relaxed);
    }
}

fn handoff_canonical_audio_epoch_datagram(
    tx: &mpsc::Sender<QueuedAudioEpochIngressDatagram>,
    peer: SocketAddr,
    bytes: Bytes,
) -> Result<()> {
    match tx.try_send(QueuedAudioEpochIngressDatagram {
        peer,
        bytes,
        enqueued_at: Instant::now(),
    }) {
        Ok(()) => {
            AUDIO_EPOCH_MESH_SEND_QUEUE_ENQUEUED.fetch_add(1, Ordering::Relaxed);
            let depth = AUDIO_EPOCH_MESH_SEND_QUEUE_DEPTH.fetch_add(1, Ordering::Relaxed) + 1;
            AUDIO_EPOCH_MESH_SEND_QUEUE_MAX_DEPTH.fetch_max(depth, Ordering::Relaxed);
            Ok(())
        }
        Err(error) => {
            let dropped = AUDIO_EPOCH_MESH_SEND_QUEUE_DROPPED.fetch_add(1, Ordering::Relaxed) + 1;
            if dropped <= 16 || dropped.is_power_of_two() {
                warn!(%peer, dropped, error = %error, "canonical SoundKit v2 mesh-send queue is full");
            }
            Err(anyhow::anyhow!(
                "canonical SoundKit v2 mesh-send queue rejected datagram: {error}"
            ))
        }
    }
}

async fn run_audio_epoch_soundkit_normalizer(
    mut input: mpsc::Receiver<QueuedAudioEpochIngressDatagram>,
    output: mpsc::Sender<QueuedAudioEpochIngressDatagram>,
    mut shutdown_rx: watch::Receiver<()>,
) {
    let mut normalizer = SoundKitV2EpochNormalizer::udp(1_200);
    loop {
        let datagram = tokio::select! {
            _ = shutdown_rx.changed() => break,
            datagram = input.recv() => datagram,
        };
        let Some(datagram) = datagram else {
            break;
        };
        AUDIO_EPOCH_MESH_QUEUE_DEPTH
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |depth| {
                Some(depth.saturating_sub(1))
            })
            .ok();
        match normalizer.push_datagram(datagram.peer, &datagram.bytes) {
            Ok(Some(datagrams)) => {
                AUDIO_EPOCH_NORMALIZED_EPOCHS.fetch_add(1, Ordering::Relaxed);
                for bytes in datagrams {
                    let _ = handoff_canonical_audio_epoch_datagram(&output, datagram.peer, bytes);
                }
            }
            Ok(None) => {}
            Err(error) => {
                AUDIO_EPOCH_NORMALIZATION_ERRORS.fetch_add(1, Ordering::Relaxed);
                warn!(peer = %datagram.peer, error = %error, "rejected AEP1 epoch during SoundKit v2 normalization");
            }
        }
    }
    while let Some(datagram) = input.recv().await {
        AUDIO_EPOCH_MESH_QUEUE_DEPTH
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |depth| {
                Some(depth.saturating_sub(1))
            })
            .ok();
        if let Ok(Some(datagrams)) = normalizer.push_datagram(datagram.peer, &datagram.bytes) {
            AUDIO_EPOCH_NORMALIZED_EPOCHS.fetch_add(1, Ordering::Relaxed);
            for bytes in datagrams {
                let _ = handoff_canonical_audio_epoch_datagram(&output, datagram.peer, bytes);
            }
        }
    }
}

/// The mesh lane's normaliser with `--aep1-shared-recovery`: it takes the
/// epochs that the LL-HLS worker's FEC receiver recovered, so each AEP1
/// datagram is decoded once, and only re-frames them as SoundKit v2 AEP1.
/// The mesh queue gauges then count epochs, not datagrams.
async fn run_recovered_audio_epoch_normalizer(
    mut input: mpsc::Receiver<RecoveredAudioEpoch>,
    output: mpsc::Sender<QueuedAudioEpochIngressDatagram>,
    mut shutdown_rx: watch::Receiver<()>,
) {
    let mut normalizer = SoundKitV2EpochNormalizer::udp(1_200);
    let mut normalize = |recovered: RecoveredAudioEpoch, waiting: usize| {
        AUDIO_EPOCH_MESH_QUEUE_ENQUEUED.fetch_add(1, Ordering::Relaxed);
        AUDIO_EPOCH_MESH_QUEUE_DEPTH.store(waiting as u64, Ordering::Relaxed);
        AUDIO_EPOCH_MESH_QUEUE_MAX_DEPTH.fetch_max(waiting as u64 + 1, Ordering::Relaxed);
        audio_epoch_ingress_queue_age().record(recovered.recovered_at.elapsed());
        let peer = recovered.peer;
        match normalizer.push_recovered_epoch(recovered.epoch) {
            Ok(datagrams) => {
                AUDIO_EPOCH_NORMALIZED_EPOCHS.fetch_add(1, Ordering::Relaxed);
                for bytes in datagrams {
                    let _ = handoff_canonical_audio_epoch_datagram(&output, peer, bytes);
                }
            }
            Err(error) => {
                AUDIO_EPOCH_NORMALIZATION_ERRORS.fetch_add(1, Ordering::Relaxed);
                warn!(%peer, error = %error, "rejected recovered AEP1 epoch during SoundKit v2 normalization");
            }
        }
    };
    loop {
        let recovered = tokio::select! {
            _ = shutdown_rx.changed() => break,
            recovered = input.recv() => recovered,
        };
        let Some(recovered) = recovered else {
            return;
        };
        normalize(recovered, input.len());
    }
    while let Some(recovered) = input.recv().await {
        normalize(recovered, input.len());
    }
}

async fn run_audio_epoch_mesh_forward_worker(
    forwarder: Arc<MeshForwarder>,
    mut input: mpsc::Receiver<QueuedAudioEpochIngressDatagram>,
    mut shutdown_rx: watch::Receiver<()>,
) {
    loop {
        tokio::select! {
            _ = shutdown_rx.changed() => break,
            datagram = input.recv() => {
                let Some(datagram) = datagram else { break; };
                AUDIO_EPOCH_MESH_SEND_QUEUE_DEPTH.fetch_update(
                    Ordering::Relaxed,
                    Ordering::Relaxed,
                    |depth| Some(depth.saturating_sub(1)),
                ).ok();
                audio_epoch_ingress_queue_age().record(datagram.enqueued_at.elapsed());
                if let Err(error) = forwarder.forward_audio_epoch_datagram(&datagram.bytes).await {
                    AUDIO_EPOCH_MESH_FORWARD_ERRORS.fetch_add(1, Ordering::Relaxed);
                    warn!(
                        error = %error,
                        "failed to forward DAW audio epoch datagram to mesh"
                    );
                }
            }
        }
    }

    while let Some(datagram) = input.recv().await {
        AUDIO_EPOCH_MESH_SEND_QUEUE_DEPTH
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |depth| {
                Some(depth.saturating_sub(1))
            })
            .ok();
        audio_epoch_ingress_queue_age().record(datagram.enqueued_at.elapsed());
        if let Err(error) = forwarder
            .forward_audio_epoch_datagram(&datagram.bytes)
            .await
        {
            AUDIO_EPOCH_MESH_FORWARD_ERRORS.fetch_add(1, Ordering::Relaxed);
            warn!(
                error = %error,
                "failed to drain DAW audio epoch datagram into mesh"
            );
        }
    }
}

async fn run_daw_media_udp_ingest(
    socket: UdpSocket,
    forwarder: Arc<MeshForwarder>,
    audio_epoch_hls_tx: Option<mpsc::Sender<AudioEpochHlsDatagram>>,
    audio_epoch_mesh_tx: Option<mpsc::Sender<QueuedAudioEpochIngressDatagram>>,
    mut shutdown_rx: watch::Receiver<()>,
) -> Result<()> {
    let bind = socket.local_addr()?;
    let mut decoders = HashMap::<SocketAddr, MediaFecDecoder>::new();
    let mut buf = vec![0u8; 65_536];

    loop {
        tokio::select! {
            _ = shutdown_rx.changed() => {
                info!(bind = %bind, "DAW media UDP relay shutting down");
                return Ok(());
            }
            received = socket.recv_from(&mut buf) => {
                let (len, peer) = received?;
                let received_unix_us = hop_journal::now_unix_us();
                if len == 0 {
                    continue;
                }
                let datagram = &buf[..len];

                // A Nexus pointed directly at its contributor can retain the
                // legacy relay registration loop while publishing AEP1. The
                // contributor is an ingest endpoint, not a viewer relay: drop
                // those control messages without acknowledging or fanning out.
                if datagram.starts_with(DAW_CONTROL_MAGIC) {
                    trace!(peer = %peer, "ignored DAW relay control at contributor ingress");
                    continue;
                }

                if is_audio_transport_datagram(datagram) {
                    handoff_audio_epoch_hls_datagram(
                        audio_epoch_hls_tx.as_ref(),
                        peer,
                        datagram,
                        received_unix_us,
                    );
                    handoff_audio_epoch_mesh_datagram(audio_epoch_mesh_tx.as_ref(), peer, datagram);
                    continue;
                }

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
    record_daw_media_udp_socket_inode(&socket);
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
        let class = fmp4_publication_class(&part);
        let source_epoch = part.source_epoch.unwrap_or(self.source_epoch).max(1);
        self.telemetry
            .media_object_source_epoch
            .fetch_max(source_epoch, Ordering::AcqRel);
        let stream_epoch = (part.stream_id, source_epoch);
        let initialization = part
            .init
            .as_ref()
            .map(|init| {
                build_live_fmp4_initialization_object(
                    part.stream_id,
                    source_epoch,
                    init,
                    part.published_at_unix_ns,
                    self.delivery_budget_ms,
                    self.estimated_clock_error_ns,
                )
            })
            .transpose()
            .map_err(|error| error.to_string())?;
        let (initialization_key, configuration_epoch, slot_init) =
            if let Some((object, epoch)) = initialization {
                if !self.relay_exclusive {
                    let initialization_envelope = encode_canonical_media_object(&object)
                        .map_err(|error| error.to_string())?;
                    self.forward_stream_slot(part.stream_id, &initialization_envelope)
                        .await
                        .map_err(|error| error.to_string())?;
                }
                if let Some(relay) = &self.relay {
                    let outcome = relay
                        .publish_object(&object, part.stream_id, class)
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
                self.fmp4_initializations.rcu(|index| {
                    let mut index = HashMap::clone(index);
                    index.insert(stream_epoch, (key.clone(), epoch));
                    index
                });
                let init = part.init.clone().ok_or_else(|| {
                    "fMP4 initialization object was built without bytes".to_string()
                })?;
                self.fmp4_initialization_payloads.rcu(|index| {
                    let mut index = HashMap::clone(index);
                    index.insert(stream_epoch, init.clone());
                    index
                });
                (key, epoch, init)
            } else {
                let (key, epoch) = self
                    .fmp4_initializations
                    .load()
                    .get(&stream_epoch)
                    .cloned()
                    .ok_or_else(|| {
                        format!(
                            "fMP4 media part {} for stream {} has no initialization dependency",
                            part.sequence, part.stream_id
                        )
                    })?;
                let init = self
                    .fmp4_initialization_payloads
                    .load()
                    .get(&stream_epoch)
                    .cloned()
                    .ok_or_else(|| {
                        format!(
                            "fMP4 media part {} for stream {} has no cached initialization payload",
                            part.sequence, part.stream_id
                        )
                    })?;
                (key, epoch, init)
            };

        let bundled_media = encode_mesh_fmp4_slot(Some(&slot_init), &part.bytes)
            .map_err(|error| error.to_string())?;
        let media_object = build_fmp4_media_object(
            &part,
            &bundled_media,
            initialization_key,
            configuration_epoch,
            source_epoch,
            self.delivery_budget_ms,
            self.estimated_clock_error_ns,
        )
        .map_err(|error| error.to_string())?;
        // The envelope is for the byte-FEC cache lane only. With
        // --relay-exclusive it is not made: encoding it validates the object,
        // which hashes the whole payload once more.
        if !self.relay_exclusive {
            let media =
                encode_canonical_media_object(&media_object).map_err(|error| error.to_string())?;
            self.forward_stream_slot(part.stream_id, &media)
                .await
                .map_err(|error| error.to_string())?;
        }

        if let Some(relay) = &self.relay {
            let outcome = relay
                .publish_part_object(&media_object, Some((part.stream_id, part.sequence)), class)
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

    async fn publish_opaque_part(
        &self,
        part: PublishedOpaquePart,
    ) -> std::result::Result<(), String> {
        let source_epoch = part.source_epoch.unwrap_or(self.source_epoch).max(1);
        self.telemetry
            .media_object_source_epoch
            .fetch_max(source_epoch, Ordering::AcqRel);
        let object = build_opaque_media_object(
            &part,
            source_epoch,
            self.delivery_budget_ms,
            self.estimated_clock_error_ns,
        )
        .map_err(|error| error.to_string())?;
        if !self.relay_exclusive {
            let encoded =
                encode_canonical_media_object(&object).map_err(|error| error.to_string())?;
            self.forward_stream_slot(part.stream_id, &encoded)
                .await
                .map_err(|error| error.to_string())?;
        }
        if let Some(relay) = &self.relay {
            let outcome = relay
                .publish_part_object(
                    &object,
                    Some((part.stream_id, part.sequence)),
                    PublicationClass::Audio,
                )
                .await
                .map_err(|error| error.to_string())?;
            trace!(
                stream_id = part.stream_id,
                sequence = part.sequence,
                object_key = ?outcome.announcement.key,
                source_symbols = outcome.source_symbols,
                repair_symbols = outcome.repair_symbols,
                "published canonical opaque audio part through RelaySession"
            );
        }
        Ok(())
    }
}

enum QueuedPublication {
    Fmp4 {
        part: PublishedFmp4Part,
        enqueued_at: Instant,
        /// Held for its drop, when the worker is done with the part.
        #[allow(dead_code)]
        in_flight: InFlight,
    },
    Opaque {
        part: PublishedOpaquePart,
        enqueued_at: Instant,
        /// Held for its drop, when the worker is done with the part.
        #[allow(dead_code)]
        in_flight: InFlight,
    },
}

/// A part of a stream between its hand-off to a publication worker and the
/// end of its publication: dropped when the worker has published it (or
/// when the queue rejects or drops it).
struct InFlight(Arc<AtomicUsize>);

impl InFlight {
    fn new(count: &Arc<AtomicUsize>) -> Self {
        count.fetch_add(1, Ordering::AcqRel);
        Self(Arc::clone(count))
    }
}

impl Drop for InFlight {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::AcqRel);
    }
}

impl QueuedPublication {
    fn stream_id(&self) -> u64 {
        match self {
            Self::Fmp4 { part, .. } => part.stream_id,
            Self::Opaque { part, .. } => part.stream_id,
        }
    }

    fn sequence(&self) -> u64 {
        match self {
            Self::Fmp4 { part, .. } => part.sequence,
            Self::Opaque { part, .. } => part.sequence,
        }
    }

    fn enqueued_at(&self) -> Instant {
        match self {
            Self::Fmp4 { enqueued_at, .. } | Self::Opaque { enqueued_at, .. } => *enqueued_at,
        }
    }
}

/// Part and segment durations that the contributor declares for the parts
/// that it stamps: one pair for streams with video, one for audio.
#[derive(Debug, Clone, Copy)]
struct StampDurations {
    /// `0` for one frame per part.
    video_part_us: u64,
    video_segment_us: u64,
    /// `0` for one epoch per part.
    audio_part_us: u64,
    audio_segment_us: u64,
}

/// Canonical sequence and playlist stamper of one stream.
struct StreamPublication {
    next_sequence: u64,
    stamper: Option<StreamStamper>,
    /// Wall clock that numbers the first segment of a video stream.
    video_started_unix_us: Option<u64>,
    /// Publication worker of the stream, chosen when its first part arrives.
    /// A stream keeps its worker so that its parts leave in order.
    shard: Option<usize>,
    /// Pool of `shard`.
    class: Option<PublicationClass>,
    /// Parts of the stream handed to `shard` and not yet published.
    in_flight: Arc<AtomicUsize>,
}

impl StreamPublication {
    fn new() -> Self {
        Self {
            next_sequence: 0,
            stamper: None,
            video_started_unix_us: None,
            shard: None,
            class: None,
            in_flight: Arc::new(AtomicUsize::new(0)),
        }
    }
}

/// Kind of a stream for the choice of its publication worker.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PublicationClass {
    Audio,
    Video,
}

/// Chooses the publication worker of each stream.
///
/// Without mixing a stream goes to `stream_id % workers`. The DAW streams
/// 2001, 3001 and 4001 and the video stream 1 then all land on worker 1, and a
/// 5 ms audio part waits behind the encode and the sends of a 200 ms video
/// part. With mixing, the workers form two pools, one for audio and one for
/// video, and each new stream takes the next worker of its pool in turn, so
/// the first streams of a pool never share a worker.
#[derive(Debug)]
struct PublicationShards {
    workers: usize,
    mix: bool,
    next_audio: usize,
    next_video: usize,
}

impl PublicationShards {
    fn new(workers: usize, mix: bool) -> Self {
        assert!(workers > 0);
        Self {
            workers,
            mix,
            next_audio: 0,
            next_video: 0,
        }
    }

    /// Workers of the audio pool: the first half, rounded up.
    fn audio_pool(&self) -> std::ops::Range<usize> {
        0..self.workers.div_ceil(2)
    }

    /// Workers of the video pool: the second half. With a single worker the
    /// pools are the same.
    fn video_pool(&self) -> std::ops::Range<usize> {
        let audio = self.audio_pool();
        if audio.end >= self.workers {
            audio
        } else {
            audio.end..self.workers
        }
    }

    fn assign(&mut self, stream_id: u64, class: PublicationClass) -> usize {
        if !self.mix {
            return (stream_id % self.workers as u64) as usize;
        }
        let (pool, next) = match class {
            PublicationClass::Audio => (self.audio_pool(), &mut self.next_audio),
            PublicationClass::Video => (self.video_pool(), &mut self.next_video),
        };
        let shard = pool.start + *next % pool.len();
        *next = next.wrapping_add(1);
        shard
    }
}

/// Class of an fMP4 part's stream: a part with a video track is video.
fn fmp4_publication_class(part: &PublishedFmp4Part) -> PublicationClass {
    if part.video_units > 0 || part.video_codec.is_some() {
        PublicationClass::Video
    } else {
        PublicationClass::Audio
    }
}

/// Video streams that start within this time of each other number their
/// segments from the same instant.
const LADDER_START_WINDOW_US: u64 = 1_000_000;

/// The wall clock that numbers the first segment of a video stream.
///
/// An encoder that sends a ladder starts its renditions together, and the
/// same group of pictures must have the same Media Sequence Number in each
/// rendition. The number of the first segment comes from the wall clock, so
/// two renditions that start a few milliseconds apart, on either side of a
/// segment boundary of the clock, would differ by one for good. A stream
/// that starts within a second of another takes the start of the earliest.
fn shared_video_start(own_unix_us: u64, others: impl Iterator<Item = u64>) -> u64 {
    others
        .filter(|other| own_unix_us.abs_diff(*other) <= LADDER_START_WINDOW_US)
        .min()
        .map_or(own_unix_us, |earliest| earliest.min(own_unix_us))
}

/// The publication state of every stream, without a lock that the streams
/// share: an audio part (on the audio hot thread) never waits for a video
/// part's stamping.
///
/// The index is an `ArcSwap` snapshot, written only when a stream appears;
/// each stream's state has a lock of its own, which only that stream's
/// packager takes. The first-segment clocks of the video streams, which a
/// new video stream reads across streams, have a lock that only a video
/// stream's first part takes.
#[derive(Default)]
struct StreamTable {
    index: arc_swap::ArcSwap<HashMap<u64, Arc<StreamEntry>>>,
    /// `video_started_unix_us` of every video stream, for
    /// [`shared_video_start`].
    video_starts: std::sync::Mutex<HashMap<u64, u64>>,
}

struct StreamEntry {
    publication: std::sync::Mutex<StreamPublication>,
    /// Initialization payloads of the stream's inline (audio hot path)
    /// publication.
    inline_initializations: std::sync::Mutex<InitializationMemory>,
}

impl StreamTable {
    /// The entry of `stream_id`, made on its first part.
    fn entry(&self, stream_id: u64) -> Arc<StreamEntry> {
        if let Some(entry) = self.index.load().get(&stream_id) {
            return Arc::clone(entry);
        }
        self.index.rcu(|index| {
            if index.contains_key(&stream_id) {
                return Arc::clone(index);
            }
            let mut index = HashMap::clone(index);
            index.insert(
                stream_id,
                Arc::new(StreamEntry {
                    publication: std::sync::Mutex::new(StreamPublication::new()),
                    inline_initializations: std::sync::Mutex::default(),
                }),
            );
            Arc::new(index)
        });
        Arc::clone(
            self.index
                .load()
                .get(&stream_id)
                .expect("a stream entry is never removed"),
        )
    }

    fn video_starts(&self) -> std::sync::MutexGuard<'_, HashMap<u64, u64>> {
        self.video_starts
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

impl StreamEntry {
    fn publication(&self) -> std::sync::MutexGuard<'_, StreamPublication> {
        self.publication
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

struct BoundedFmp4Publisher {
    senders: Vec<mpsc::Sender<QueuedPublication>>,
    telemetry: Arc<IngestTelemetry>,
    streams: StreamTable,
    shards: std::sync::Mutex<PublicationShards>,
    durations: StampDurations,
    /// `--relay-audio-inline`: the publisher that audio parts go to on the
    /// packager's own task, without a worker's queue.
    inline_audio: Option<Arc<dyn Fmp4PartPublisher>>,
}

impl BoundedFmp4Publisher {
    /// Whether the stream's next part, of class `class`, is published on
    /// the caller's task: an audio part of a stream that is not video and
    /// has no part waiting for a worker (its parts stay in order).
    fn publishes_inline(&self, stream: &StreamPublication, class: PublicationClass) -> bool {
        self.inline_audio.is_some()
            && class == PublicationClass::Audio
            && stream.class != Some(PublicationClass::Video)
            && stream.in_flight.load(Ordering::Acquire) == 0
    }

    /// Publish `publication` on this task: the audio hot path.
    async fn publish_inline(&self, entry: &StreamEntry, publication: QueuedPublication) {
        let Some(inner) = self.inline_audio.as_ref() else {
            return;
        };
        hop_journal::record(HopEvent::new(
            HopStage::Dequeued,
            publication.stream_id(),
            publication.sequence(),
            hop_journal::now_unix_us(),
        ));
        PUBLICATION_INLINE.fetch_add(1, Ordering::Relaxed);
        let (stream_id, sequence) = (publication.stream_id(), publication.sequence());
        let started = Instant::now();
        publish_publication(
            None,
            inner,
            &self.telemetry,
            InitializationsRef::Shared(&entry.inline_initializations),
            publication,
        )
        .await;
        let held = started.elapsed();
        PUBLICATION_INLINE_MAX_US.fetch_max(held.as_micros() as u64, Ordering::Relaxed);
        if held >= INLINE_PUBLISH_SLOW {
            let slow = PUBLICATION_INLINE_SLOW.fetch_add(1, Ordering::Relaxed) + 1;
            warn!(
                stream_id,
                sequence,
                held_ms = held.as_secs_f64() * 1_000.0,
                slow,
                "an inline audio publication held its packager: later parts of the \
                 rendition waited behind it"
            );
        }
    }

    /// The worker of the stream's next part, of class `class`.
    ///
    /// A stream keeps the worker of its first part, so that its parts leave
    /// in order, with one exception: a muxed stream can publish audio-only
    /// parts before its first video access unit (a TS demuxer often has
    /// audio before the first whole video PES), which would put it in the
    /// audio pool for good. Its first part with video moves it to the video
    /// pool once every part it handed to the audio worker has been
    /// published; until then its parts stay where they are, in order.
    fn shard_of(
        &self,
        stream: &mut StreamPublication,
        stream_id: u64,
        class: PublicationClass,
    ) -> usize {
        let to_video_pool = stream.class == Some(PublicationClass::Audio)
            && class == PublicationClass::Video
            && stream.in_flight.load(Ordering::Acquire) == 0;
        if let Some(shard) = stream.shard.filter(|_| !to_video_pool) {
            return shard;
        }
        let shard = self
            .shards
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .assign(stream_id, class);
        stream.shard = Some(shard);
        stream.class = Some(class);
        shard
    }
}

#[async_trait::async_trait]
impl Fmp4PartPublisher for BoundedFmp4Publisher {
    async fn publish_fmp4_part(
        &self,
        mut part: PublishedFmp4Part,
    ) -> std::result::Result<(), String> {
        let stream_id = part.stream_id;
        let stream_idx = part.stream_idx;
        let entry = self.streams.entry(stream_id);
        // The stream's lock is held only here, never across an await.
        let publication = 'inline: {
            let mut guard = entry.publication();
            let stream = &mut *guard;
            let has_video = part.video_units > 0;
            let published_unix_us = u64::try_from(part.published_at_unix_ns / 1_000).unwrap_or(0);
            let known = stream.stamper.is_some();
            let started_unix_us = if has_video && !known {
                let mut starts = self.streams.video_starts();
                let started = shared_video_start(published_unix_us, starts.values().copied());
                starts.insert(stream_id, started);
                started
            } else {
                published_unix_us
            };
            part.sequence = stream.next_sequence;
            let sequence = part.sequence;
            // A stream with video cuts its segments at key frames. An audio
            // stream has fixed segments, and every part of it can start one.
            let durations = self.durations;
            let before = stream.stamper.clone();
            let started_before = stream.video_started_unix_us;
            if has_video && stream.stamper.is_none() {
                stream.video_started_unix_us = Some(started_unix_us);
            }
            let stamper = stream.stamper.get_or_insert_with(|| {
                if has_video {
                    StreamStamper::new(durations.video_part_us, durations.video_segment_us)
                } else {
                    StreamStamper::new(durations.audio_part_us, durations.audio_segment_us)
                }
            });
            part.hls = Some(stamper.stamp(
                part.duration_us,
                part.keyframe || !has_video,
                started_unix_us,
            ));
            let class = fmp4_publication_class(&part);
            if self.publishes_inline(stream, class) {
                stream.class.get_or_insert(PublicationClass::Audio);
                let publication = QueuedPublication::Fmp4 {
                    in_flight: InFlight::new(&stream.in_flight),
                    enqueued_at: Instant::now(),
                    part,
                };
                if let QueuedPublication::Fmp4 { part, .. } = &publication {
                    journal_enqueued(
                        stream_id,
                        sequence,
                        &part.source_times,
                        part.packaged_at_unix_ns,
                        hop_journal::now_unix_us(),
                    );
                }
                stream.next_sequence = stream.next_sequence.saturating_add(1);
                break 'inline publication;
            }
            let shard = self.shard_of(stream, stream_id, class);
            let source_times = part.source_times;
            let packaged_at_unix_ns = part.packaged_at_unix_ns;
            let enqueued_us = hop_journal::now_unix_us();
            let result = self.senders[shard].try_send(QueuedPublication::Fmp4 {
                part,
                enqueued_at: Instant::now(),
                in_flight: InFlight::new(&stream.in_flight),
            });
            if result.is_ok() {
                journal_enqueued(
                    stream_id,
                    sequence,
                    &source_times,
                    packaged_at_unix_ns,
                    enqueued_us,
                );
                stream.next_sequence = stream.next_sequence.saturating_add(1);
            } else {
                // The part was not published: it has no place in the playlist.
                stream.stamper = before;
                if started_before.is_none() && stream.video_started_unix_us.is_some() {
                    self.streams.video_starts().remove(&stream_id);
                }
                stream.video_started_unix_us = started_before;
            }
            drop(guard);
            return enqueue_publication_result(
                result,
                stream_id,
                stream_idx,
                sequence,
                &self.telemetry,
            );
        };
        self.publish_inline(&entry, publication).await;
        Ok(())
    }

    fn record_mpeg_ts_continuity_issue(&self, issue: MpegTsContinuityIssue) {
        self.telemetry.record_mpeg_ts_continuity_issue(issue);
    }

    fn record_mpeg_ts_payload_drop(&self, drop: MpegTsPayloadDrop) {
        self.telemetry.record_mpeg_ts_payload_drop(drop);
    }

    async fn publish_opaque_part(
        &self,
        mut part: PublishedOpaquePart,
    ) -> std::result::Result<(), String> {
        let stream_id = part.stream_id;
        let entry = self.streams.entry(stream_id);
        // The stream's lock is held only here, never across an await.
        let publication = 'inline: {
            let mut guard = entry.publication();
            let stream = &mut *guard;
            part.sequence = stream.next_sequence;
            let sequence = part.sequence;
            let durations = self.durations;
            let before = stream.stamper.clone();
            let stamper = stream.stamper.get_or_insert_with(|| {
                StreamStamper::new(durations.audio_part_us, durations.audio_segment_us)
            });
            part.hls = Some(stamper.stamp(
                part.duration_us,
                true,
                u64::try_from(part.published_at_unix_ns / 1_000).unwrap_or(0),
            ));
            if self.publishes_inline(stream, PublicationClass::Audio) {
                stream.class.get_or_insert(PublicationClass::Audio);
                journal_enqueued(
                    stream_id,
                    sequence,
                    &part.source_times,
                    part.packaged_at_unix_ns,
                    hop_journal::now_unix_us(),
                );
                let publication = QueuedPublication::Opaque {
                    in_flight: InFlight::new(&stream.in_flight),
                    enqueued_at: Instant::now(),
                    part,
                };
                stream.next_sequence = stream.next_sequence.saturating_add(1);
                break 'inline publication;
            }
            let shard = self.shard_of(stream, stream_id, PublicationClass::Audio);
            let source_times = part.source_times;
            let packaged_at_unix_ns = part.packaged_at_unix_ns;
            let enqueued_us = hop_journal::now_unix_us();
            let result = self.senders[shard].try_send(QueuedPublication::Opaque {
                part,
                enqueued_at: Instant::now(),
                in_flight: InFlight::new(&stream.in_flight),
            });
            if result.is_ok() {
                journal_enqueued(
                    stream_id,
                    sequence,
                    &source_times,
                    packaged_at_unix_ns,
                    enqueued_us,
                );
                stream.next_sequence = stream.next_sequence.saturating_add(1);
            } else {
                stream.stamper = before;
            }
            drop(guard);
            return enqueue_publication_result(
                result,
                stream_id,
                stream_id as usize,
                sequence,
                &self.telemetry,
            );
        };
        self.publish_inline(&entry, publication).await;
        Ok(())
    }
}

/// Journal a part handed to its publication worker, with the source and
/// packaging times it carried there.
fn journal_enqueued(
    stream: u64,
    seq: u64,
    source_times: &hop_journal::PartSourceTimes,
    packaged_at_unix_ns: i64,
    enqueued_us: u64,
) {
    let Some(journal) = hop_journal::global() else {
        return;
    };
    for event in source_times.events(stream, seq) {
        journal.record(event);
    }
    journal.record(
        HopEvent::new(
            HopStage::Packaged,
            stream,
            seq,
            hop_journal::unix_ns_to_us(packaged_at_unix_ns),
        )
        .with_epoch(source_times.last_epoch),
    );
    journal.record(HopEvent::new(HopStage::Enqueued, stream, seq, enqueued_us));
}

fn enqueue_publication_result(
    result: std::result::Result<(), mpsc::error::TrySendError<QueuedPublication>>,
    stream_id: u64,
    stream_idx: usize,
    sequence: u64,
    telemetry: &IngestTelemetry,
) -> std::result::Result<(), String> {
    match result {
        Ok(()) => {
            PUBLICATION_QUEUE_ENQUEUED.fetch_add(1, Ordering::Relaxed);
            let depth = PUBLICATION_QUEUE_DEPTH.fetch_add(1, Ordering::Relaxed) + 1;
            PUBLICATION_QUEUE_MAX_DEPTH.fetch_max(depth, Ordering::Relaxed);
            Ok(())
        }
        Err(error) => {
            let reason = match error {
                mpsc::error::TrySendError::Full(_) => "publication queue is full",
                mpsc::error::TrySendError::Closed(_) => "publication queue is closed",
            };
            let dropped = PUBLICATION_QUEUE_DROPPED.fetch_add(1, Ordering::Relaxed) + 1;
            telemetry.record_fmp4_publish_error(stream_id, stream_idx, sequence, reason);
            if dropped <= 10 || dropped.is_power_of_two() {
                warn!(
                    stream_id,
                    sequence,
                    dropped,
                    queue_depth = PUBLICATION_QUEUE_DEPTH.load(Ordering::Relaxed),
                    reason,
                    "bounded media publication handoff rejected a part"
                );
            }
            Err(reason.to_owned())
        }
    }
}

fn start_bounded_publication_pipeline(
    inner: Arc<dyn Fmp4PartPublisher>,
    telemetry: Arc<IngestTelemetry>,
    durations: StampDurations,
    shard_mix: bool,
    audio_inline: bool,
    shutdown_rx: watch::Receiver<()>,
) -> (Arc<dyn Fmp4PartPublisher>, Vec<tokio::task::JoinHandle<()>>) {
    assert!(PUBLICATION_WORKERS > 0);
    assert!(PUBLICATION_QUEUE_CAPACITY_PER_WORKER > 0);
    let mut senders = Vec::with_capacity(PUBLICATION_WORKERS);
    let mut tasks = Vec::with_capacity(PUBLICATION_WORKERS);
    for shard in 0..PUBLICATION_WORKERS {
        let (sender, receiver) = mpsc::channel(PUBLICATION_QUEUE_CAPACITY_PER_WORKER);
        senders.push(sender);
        tasks.push(tokio::spawn(run_publication_worker(
            shard,
            Arc::clone(&inner),
            Arc::clone(&telemetry),
            receiver,
            shutdown_rx.clone(),
        )));
    }
    PUBLICATION_QUEUE_CAPACITY_METRIC.store(
        (PUBLICATION_WORKERS * PUBLICATION_QUEUE_CAPACITY_PER_WORKER) as u64,
        Ordering::Relaxed,
    );
    PUBLICATION_QUEUE_DEPTH.store(0, Ordering::Relaxed);
    (
        Arc::new(BoundedFmp4Publisher {
            senders,
            telemetry,
            streams: StreamTable::default(),
            shards: std::sync::Mutex::new(PublicationShards::new(PUBLICATION_WORKERS, shard_mix)),
            durations,
            inline_audio: audio_inline.then(|| Arc::clone(&inner)),
        }),
        tasks,
    )
}

/// Pin the calling thread and give it SCHED_FIFO, as far as the host
/// allows; one info line either way.
fn make_audio_hot_thread_realtime(cpu: Option<usize>, fifo_priority: u8, what: &str) {
    let outcome = class_lanes::make_current_thread_realtime(cpu, fifo_priority);
    if outcome.refused.is_empty() {
        info!(
            cpu = ?outcome.pinned_cpu,
            fifo_priority = ?outcome.fifo_priority,
            "{what} started"
        );
    } else {
        info!(
            cpu = ?outcome.pinned_cpu,
            fifo_priority = ?outcome.fifo_priority,
            refused = ?outcome.refused,
            "{what} started without part of its real-time settings \
             (--audio-hot-cpu, --audio-hot-fifo-priority); it runs on with normal scheduling"
        );
    }
}

/// The audio hot threads (`--audio-hot-thread`): threads of their own, each
/// with a single-threaded runtime, SCHED_FIFO --audio-hot-fifo-priority and
/// thread `i` pinned to --audio-hot-cpu + `i`, where the host allows it (a
/// refusal, in a rootless lab, is one info line, and the thread runs on with
/// normal scheduling, unpinned).
///
/// The first thread runs the DAW media receive and the AEP1 recovery; the
/// rendition workers (the packaging, encode and sends of each format's
/// parts) go to the threads in turn from the second (see
/// [`RenditionRuntimes`]). Every thread is the only FIFO thread on its CPU:
/// no thread waits on a CPU for another of the same priority, which FIFO
/// does not time-slice. A runtime's blocking-pool threads go back to normal
/// scheduling on every CPU.
struct AudioHotRuntime {
    handles: Vec<tokio::runtime::Handle>,
    /// `true` (or dropped) stops the threads, once the work on them is done.
    stop: watch::Sender<bool>,
}

impl AudioHotRuntime {
    fn start(args: &Args) -> Result<Option<Self>> {
        if !args.audio_hot_thread {
            return Ok(None);
        }
        let cpu = usize::try_from(args.audio_hot_cpu).ok();
        let fifo_priority = args.audio_hot_fifo_priority;
        let threads = audio_hot_thread_count(args);
        let (stop, _) = watch::channel(false);
        let mut handles = Vec::with_capacity(threads);
        for index in 0..threads {
            let (handle_tx, handle_rx) = std::sync::mpsc::sync_channel(1);
            let mut stop_rx = stop.subscribe();
            let cpu = cpu.map(|cpu| cpu + index);
            std::thread::Builder::new()
                .name(format!("audio-hot-{index}"))
                .spawn(move || {
                    make_audio_hot_thread_realtime(cpu, fifo_priority, "audio hot thread");
                    let runtime = tokio::runtime::Builder::new_current_thread()
                        .enable_all()
                        .on_thread_start(|| {
                            // Only the blocking pool's threads start here;
                            // they would inherit FIFO and the audio CPU.
                            let _ = class_lanes::reset_current_thread_scheduling();
                        })
                        .build();
                    let runtime = match runtime {
                        Ok(runtime) => runtime,
                        Err(error) => {
                            let _ = handle_tx.send(Err(error));
                            return;
                        }
                    };
                    let _ = handle_tx.send(Ok(runtime.handle().clone()));
                    runtime.block_on(async move {
                        while !*stop_rx.borrow_and_update() {
                            if stop_rx.changed().await.is_err() {
                                break;
                            }
                        }
                    });
                })
                .context("failed to start an audio hot thread")?;
            let handle = handle_rx
                .recv()
                .context("an audio hot thread stopped before its runtime was built")?
                .context("an audio hot thread could not build its runtime")?;
            handles.push(handle);
        }
        info!(
            threads,
            cpu = ?cpu,
            fifo_priority,
            "audio hot threads started"
        );
        Ok(Some(Self { handles, stop }))
    }

    /// The runtime of the receive and recovery.
    fn first(&self) -> &tokio::runtime::Handle {
        &self.handles[0]
    }

    /// The runtimes of the rendition workers.
    fn rendition_runtimes(&self) -> Option<RenditionRuntimes> {
        RenditionRuntimes::new(self.handles.clone())
    }
}

impl Drop for AudioHotRuntime {
    fn drop(&mut self) {
        let _ = self.stop.send(true);
    }
}

/// Audio hot threads: --audio-hot-workers, or with 0 one per configured
/// DAW HLS format, as many as the CPUs from --audio-hot-cpu on hold.
fn audio_hot_thread_count(args: &Args) -> usize {
    if args.audio_hot_workers > 0 {
        return args.audio_hot_workers;
    }
    let formats = args
        .daw_hls_formats
        .iter()
        .map(|format| AudioPayloadKind::from(*format))
        .fold(Vec::new(), |mut unique, format| {
            if !unique.contains(&format) {
                unique.push(format);
            }
            unique
        })
        .len();
    let cpus = std::thread::available_parallelism().map_or(1, std::num::NonZeroUsize::get);
    class_lanes::auto_audio_hot_threads(formats, usize::try_from(args.audio_hot_cpu).ok(), cpus)
}

/// Spawn `work` on the audio hot threads' first runtime, or on the main
/// runtime without them.
fn spawn_audio_hot_task<F>(
    hot: Option<&AudioHotRuntime>,
    work: F,
) -> tokio::task::JoinHandle<F::Output>
where
    F: std::future::Future + Send + 'static,
    F::Output: Send + 'static,
{
    match hot {
        Some(hot) => hot.first().spawn(work),
        None => tokio::spawn(work),
    }
}

async fn run_publication_worker(
    shard: usize,
    inner: Arc<dyn Fmp4PartPublisher>,
    telemetry: Arc<IngestTelemetry>,
    mut input: mpsc::Receiver<QueuedPublication>,
    mut shutdown_rx: watch::Receiver<()>,
) {
    let mut initializations = HashMap::<u64, Bytes>::new();
    let mut force_initialization = std::collections::HashSet::<u64>::new();
    loop {
        tokio::select! {
            _ = shutdown_rx.changed() => break,
            publication = input.recv() => {
                let Some(publication) = publication else { break; };
                process_queued_publication(
                    shard,
                    &inner,
                    &telemetry,
                    &mut initializations,
                    &mut force_initialization,
                    publication,
                ).await;
            }
        }
    }
    while let Ok(publication) = input.try_recv() {
        process_queued_publication(
            shard,
            &inner,
            &telemetry,
            &mut initializations,
            &mut force_initialization,
            publication,
        )
        .await;
    }
}

async fn process_queued_publication(
    shard: usize,
    inner: &Arc<dyn Fmp4PartPublisher>,
    telemetry: &IngestTelemetry,
    initializations: &mut HashMap<u64, Bytes>,
    force_initialization: &mut std::collections::HashSet<u64>,
    publication: QueuedPublication,
) {
    PUBLICATION_QUEUE_DEPTH
        .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |depth| {
            Some(depth.saturating_sub(1))
        })
        .ok();
    hop_journal::record(HopEvent::new(
        HopStage::Dequeued,
        publication.stream_id(),
        publication.sequence(),
        hop_journal::now_unix_us(),
    ));
    publication_queue_age().record(publication.enqueued_at().elapsed());
    publish_publication(
        Some(shard),
        inner,
        telemetry,
        InitializationsRef::Worker(initializations, force_initialization),
        publication,
    )
    .await;
}

/// Initialization payloads that a publisher re-attaches to a stream's next
/// part after a part of the stream failed to publish.
#[derive(Default)]
struct InitializationMemory {
    payloads: HashMap<u64, Bytes>,
    force: HashSet<u64>,
}

/// The initialization memory of a publication worker (its own), or of the
/// inline audio path (shared by the packagers' tasks, locked only around
/// each update).
enum InitializationsRef<'a> {
    Worker(&'a mut HashMap<u64, Bytes>, &'a mut HashSet<u64>),
    Shared(&'a std::sync::Mutex<InitializationMemory>),
}

impl InitializationsRef<'_> {
    fn update<R>(
        &mut self,
        update: impl FnOnce(&mut HashMap<u64, Bytes>, &mut HashSet<u64>) -> R,
    ) -> R {
        match self {
            Self::Worker(payloads, force) => update(payloads, force),
            Self::Shared(memory) => {
                let mut memory = memory
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                let memory = &mut *memory;
                update(&mut memory.payloads, &mut memory.force)
            }
        }
    }
}

/// Publish one part through `inner`: the work of a publication worker
/// (`shard`), or of the packager's own task on the audio hot path (`None`).
/// A failure is counted and logged, never returned: the part has left the
/// packager either way.
async fn publish_publication(
    shard: Option<usize>,
    inner: &Arc<dyn Fmp4PartPublisher>,
    telemetry: &IngestTelemetry,
    mut initializations: InitializationsRef<'_>,
    publication: QueuedPublication,
) {
    let started = Instant::now();
    let stream_id = publication.stream_id();
    let result = match publication {
        QueuedPublication::Fmp4 { mut part, .. } => {
            initializations.update(|payloads, force| {
                if let Some(init) = part.init.as_ref() {
                    payloads.insert(stream_id, init.clone());
                } else if force.contains(&stream_id) {
                    part.init = payloads.get(&stream_id).cloned();
                }
            });
            let stream_idx = part.stream_idx;
            let sequence = part.sequence;
            let bytes = part.bytes.len() as u64;
            let init_bytes = part.init.as_ref().map_or(0, |init| init.len() as u64);
            telemetry.record_fmp4_tracks(
                stream_id,
                stream_idx,
                sequence,
                part.video_width,
                part.video_height,
                part.video_units,
                part.audio_units,
            );
            let carried_initialization = part.init.is_some();
            match inner.publish_fmp4_part(part).await {
                Ok(()) => {
                    if carried_initialization {
                        initializations.update(|_, force| force.remove(&stream_id));
                    }
                    telemetry.record_fmp4_part(stream_id, stream_idx, sequence, bytes, init_bytes);
                    Ok(())
                }
                Err(error) => {
                    initializations.update(|_, force| force.insert(stream_id));
                    telemetry.record_fmp4_publish_error(stream_id, stream_idx, sequence, &error);
                    Err(error)
                }
            }
        }
        QueuedPublication::Opaque { part, .. } => inner.publish_opaque_part(part).await,
    };
    publication_duration().record(started.elapsed());
    match result {
        Ok(()) => {
            PUBLICATION_COMPLETED.fetch_add(1, Ordering::Relaxed);
        }
        Err(error) => {
            let errors = PUBLICATION_ERRORS.fetch_add(1, Ordering::Relaxed) + 1;
            warn!(
                shard = ?shard,
                inline = shard.is_none(),
                stream_id,
                errors,
                error = %error,
                "asynchronous media publication failed"
            );
        }
    }
}

async fn start_rist_ingest(
    config: RistIngestConfig,
    playlists: Arc<playlists::Playlists>,
    publisher: Arc<dyn Fmp4PartPublisher>,
    access_unit_sink: Option<Arc<dyn AccessUnitSink>>,
    publish_source: bool,
    telemetry: Arc<IngestTelemetry>,
    shutdown_rx: watch::Receiver<()>,
) -> Result<watch::Sender<()>> {
    let service = Arc::new(UploadResponseService::new(upload_response_config()));
    let rist_shutdown = UploadRistIngest::new(service.clone())
        .with_profile(config.profile.into())
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
        UploadTsOutputPolicy::FixedConfigured,
        config.output_stream_id,
        config.output_stream_idx,
        config.min_part_ms,
        access_unit_sink,
        publish_source,
        shutdown_rx,
    ));
    info!(
        bind = %config.bind,
        profile = config.profile.as_str(),
        backend = "pure-rust",
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum UploadTsOutputPolicy {
    FixedConfigured,
    #[cfg(any(feature = "srt-ingest", test))]
    ConcurrentFallback,
}

fn select_upload_ts_output_stream_id(
    policy: UploadTsOutputPolicy,
    configured_stream_id: u64,
    request_stream_id: u64,
    has_active_bridge: bool,
) -> u64 {
    #[cfg(not(any(feature = "srt-ingest", test)))]
    let _ = (request_stream_id, has_active_bridge);

    match policy {
        UploadTsOutputPolicy::FixedConfigured => configured_stream_id,
        #[cfg(any(feature = "srt-ingest", test))]
        UploadTsOutputPolicy::ConcurrentFallback if has_active_bridge => request_stream_id,
        #[cfg(any(feature = "srt-ingest", test))]
        UploadTsOutputPolicy::ConcurrentFallback => configured_stream_id,
    }
}

fn upload_output_has_active_successor(
    bridges: &HashMap<u64, UploadTsBridgeState>,
    request_stream_id: u64,
    output_stream_id: u64,
) -> bool {
    bridges.iter().any(|(&other_request_stream_id, state)| {
        other_request_stream_id != request_stream_id
            && !state.ended
            && state.output_stream_id == Some(output_stream_id)
    })
}

async fn advance_upload_request_reader(
    service: &UploadResponseService,
    stream_id: u64,
    slot: usize,
    end_of_request: bool,
) -> bool {
    let advanced = service
        .mark_request_reader_position(stream_id, UPLOAD_RESPONSE_HLS_WORKER_ID, slot)
        .await;
    if end_of_request && advanced {
        service
            .complete_response(
                stream_id,
                Ok(CachedResponse {
                    status: StatusCode::NO_CONTENT,
                    body: Bytes::new(),
                    headers: Vec::new(),
                }),
            )
            .await;
    }
    advanced
}

#[cfg(feature = "srt-ingest")]
#[derive(Debug, Clone, Copy)]
struct SrtIngestConfig {
    bind: SocketAddr,
    output_stream_id: u64,
    min_part_ms: u32,
}

#[cfg(feature = "srt-ingest")]
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
        UploadTsOutputPolicy::ConcurrentFallback,
        config.output_stream_id,
        output_stream_idx,
        config.min_part_ms,
        None,
        true,
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
    output_policy: UploadTsOutputPolicy,
    output_stream_id: u64,
    output_stream_idx: usize,
    min_part_ms: u32,
    access_unit_sink: Option<Arc<dyn AccessUnitSink>>,
    publish_source: bool,
    mut shutdown_rx: watch::Receiver<()>,
) {
    let mut tick = interval(Duration::from_millis(HLS_BRIDGE_POLL_MS));
    tick.set_missed_tick_behavior(MissedTickBehavior::Skip);
    let mut bridges: HashMap<u64, UploadTsBridgeState> = HashMap::new();
    let mut last_poll_at = Instant::now();

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
                let poll_at = Instant::now();
                let poll_gap = poll_at.duration_since(last_poll_at);
                last_poll_at = poll_at;
                if poll_gap >= Duration::from_millis(100) {
                    warn!(
                        protocol,
                        elapsed_ms = poll_gap.as_secs_f64() * 1_000.0,
                        tracked_streams = bridges.len(),
                        "delayed upload-response fMP4 bridge poll"
                    );
                }
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
                            if upload_output_has_active_successor(
                                &bridges,
                                stream_id,
                                output_stream_id,
                            ) {
                                debug!(
                                    stream_id,
                                    output_stream_id,
                                    "preserving upload output for an active successor"
                                );
                            } else {
                                playlists.fin(output_stream_id);
                            }
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

                    if stream.request_last <= state.last_seen {
                        trace!(
                            stream_id,
                            last_seen = state.last_seen,
                            request_last = stream.request_last,
                            "upload-response fMP4 bridge has no new request slots"
                        );
                        continue;
                    }

                    let mut finished_output_stream_id = None;
                    for slot in (state.last_seen + 1)..=stream.request_last {
                        let mut slot_ended = false;
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
                                    let public_stream_id =
                                        select_upload_ts_output_stream_id(
                                            output_policy,
                                            output_stream_id,
                                            stream_id,
                                            has_active_bridge,
                                        );
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
                                    state.bridge = Some(
                                        if public_stream_id == output_stream_id {
                                            if let Some(sink) = &access_unit_sink {
                                                if publish_source {
                                                    TsFmp4Bridge::new_publish_only_with_access_unit_sink(
                                                        public_stream_id,
                                                        public_stream_idx,
                                                        playlists.clone(),
                                                        min_part_ms,
                                                        publisher.clone(),
                                                        sink.clone(),
                                                    )
                                                } else {
                                                    TsFmp4Bridge::new_access_unit_sink_only(
                                                        public_stream_id,
                                                        public_stream_idx,
                                                        playlists.clone(),
                                                        min_part_ms,
                                                        sink.clone(),
                                                    )
                                                }
                                            } else {
                                                TsFmp4Bridge::new_publish_only(
                                                    public_stream_id,
                                                    public_stream_idx,
                                                    playlists.clone(),
                                                    min_part_ms,
                                                    publisher.clone(),
                                                )
                                            }
                                        } else {
                                            TsFmp4Bridge::new_publish_only(
                                                public_stream_id,
                                                public_stream_idx,
                                                playlists.clone(),
                                                min_part_ms,
                                                publisher.clone(),
                                            )
                                        },
                                    );
                                    debug!(
                                        stream_id,
                                        output_stream_id = public_stream_id,
                                        output_stream_idx = public_stream_idx,
                                        "created upload-response MPEG-TS fMP4 bridge after first body slot"
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
                                    let push_started = Instant::now();
                                    bridge.push_ts(data).await;
                                    let push_elapsed = push_started.elapsed();
                                    if push_elapsed >= Duration::from_millis(100) {
                                        warn!(
                                            protocol,
                                            stream_id,
                                            slot,
                                            elapsed_ms = push_elapsed.as_secs_f64() * 1_000.0,
                                            "slow MPEG-TS to fMP4 bridge push"
                                        );
                                    }
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
                                    finished_output_stream_id = state.output_stream_id;
                                } else {
                                    trace!(
                                        stream_id,
                                        slot,
                                        "upload-response stream ended without media body slots"
                                    );
                                }
                                telemetry.end_ingest_session(protocol, stream_id, "ended");
                                state.ended = true;
                                slot_ended = true;
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
                        if !advance_upload_request_reader(
                            service.as_ref(),
                            stream_id,
                            slot,
                            slot_ended,
                        )
                        .await
                            && slot_ended
                        {
                            warn!(
                                stream_id,
                                slot,
                                "upload-response end reader position did not advance"
                            );
                        }
                    }

                    state.last_seen = stream.request_last;
                    if let Some(finished_output_stream_id) = finished_output_stream_id {
                        if upload_output_has_active_successor(
                            &bridges,
                            stream_id,
                            finished_output_stream_id,
                        ) {
                            debug!(
                                stream_id,
                                output_stream_id = finished_output_stream_id,
                                "preserving upload output across an overlapping successor"
                            );
                        } else {
                            playlists.fin(finished_output_stream_id);
                        }
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
                            segmenter: Fmp4Segmenter::new_publish_only(
                                output_stream_id,
                                output_stream_idx,
                                playlists.clone(),
                                TimestampInput::Millis,
                                min_part_ms,
                                publisher.clone(),
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

/// Slots of the ring between an SRT or RIST listener and the fMP4 bridge.
///
/// The bridge reads the ring every few milliseconds, so the ring holds the
/// stream for the bridge and for nothing else. A live stream never ends, and
/// the ring keeps every slot until it wraps: with 32,768 slots of up to 47 KB
/// a 16 Mbit/s stream grew the contributor by 2 MB/s until the kernel killed
/// it at the 1 GB of a small node. 256 slots are 12 MB at most, and several
/// seconds of a 40 Mbit/s stream.
const UPLOAD_RING_SLOTS: usize = 256;
const UPLOAD_RING_SLOT_KB: usize = 47;

fn upload_response_config() -> UploadResponseConfig {
    UploadResponseConfig {
        num_streams: 8,
        slot_size_kb: UPLOAD_RING_SLOT_KB,
        slots_per_stream: UPLOAD_RING_SLOTS,
        response_timeout_ms: 60_000,
    }
}

async fn resolve_output_stream_idx(
    playlists: &Arc<playlists::Playlists>,
    output_stream_id: u64,
) -> usize {
    playlists
        .chunk_cache
        .resolve_or_create_stream(output_stream_id)
        .await
        .index()
}

/// The output stream of an RTMP publish.
///
/// A stream key that is a number names its output stream. An encoder that
/// sends a ladder publishes each rendition under the key of its stream, so
/// the stream of a rendition does not depend on the order in which the
/// encoder connects. Any other key: the first stream is the configured one.
fn rtmp_output_stream_id(
    stream: &RtmpStreamInfo,
    fallback_output_stream_id: u64,
    is_first_stream: bool,
) -> u64 {
    if let Some(named) = stream
        .key
        .parse::<u64>()
        .ok()
        .filter(|stream_id| *stream_id > 0)
    {
        return named;
    }
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
    /// Audio parts (on one lane) whose symbols waited in the lane's
    /// backlog for room in the socket buffer.
    relay_lane_backlog_parts: AtomicU64,
    /// Audio parts (on one lane) dropped because the lane's backlog was
    /// full.
    relay_lane_backlog_dropped: AtomicU64,
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
    relay_direct_lane_objects_succeeded: AtomicU64,
    relay_direct_lane_objects_failed: AtomicU64,
    relay_direct_datagrams: AtomicU64,
    relay_direct_datagram_bytes: AtomicU64,
    relay_direct_send_duration: AtomicDurationHistogram,
    relay_expired_objects: AtomicU64,
    relay_expired_symbols: AtomicU64,
    /// `--relay-video-gso`: GSO messages, and the datagrams they carried.
    relay_gso_messages: AtomicU64,
    relay_gso_datagrams: AtomicU64,
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
                lane_backlog_parts: self.relay_lane_backlog_parts.load(Ordering::Relaxed),
                lane_backlog_dropped: self.relay_lane_backlog_dropped.load(Ordering::Relaxed),
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
                direct_lane_objects_succeeded: self
                    .relay_direct_lane_objects_succeeded
                    .load(Ordering::Relaxed),
                direct_lane_objects_failed: self
                    .relay_direct_lane_objects_failed
                    .load(Ordering::Relaxed),
                direct_datagrams: self.relay_direct_datagrams.load(Ordering::Relaxed),
                direct_datagram_bytes: self.relay_direct_datagram_bytes.load(Ordering::Relaxed),
                direct_send: self.relay_direct_send_duration.snapshot(),
                expired_objects: self.relay_expired_objects.load(Ordering::Relaxed),
                expired_symbols: self.relay_expired_symbols.load(Ordering::Relaxed),
                gso_messages: self.relay_gso_messages.load(Ordering::Relaxed),
                gso_datagrams: self.relay_gso_datagrams.load(Ordering::Relaxed),
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
                relay_direct_targets: args
                    .relay_direct_targets
                    .iter()
                    .map(ToString::to_string)
                    .collect(),
                relay_direct_ids: (0..args.relay_direct_targets.len())
                    .map(|index| direct_lane_id(args, index))
                    .collect(),
                relay_direct_bind: args.relay_direct_bind.map(|bind| bind.to_string()),
                relay_direct_repair: args.relay_direct_repair,
                relay_send_batch: args.relay_send_batch,
                relay_video_gso: args.relay_send_batch && args.relay_video_gso,
                relay_video_gso_segments: args.relay_video_gso_segments,
                relay_video_gso_pace_mbps: args.relay_video_gso_pace_mbps,
                relay_class_lanes: args.relay_class_lanes,
                relay_class_port_offset: args.relay_class_port_offset,
                relay_audio_inline: args.relay_audio_inline,
                relay_repair_both_lanes: args.relay_repair_both_lanes,
                relay_source_first: args.relay_send_batch && args.relay_source_first,
                relay_video_symbol_size: args.relay_video_symbol_size,
                relay_video_plan_grid: args.relay_video_plan_grid,
                relay_repair_alongside_sources: args.relay_repair_alongside_sources,
                audio_hot_thread: args.audio_hot_thread,
                aep1_shared_recovery: args.aep1_shared_recovery,
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
                part_target_ms: args.nominal_audio_part_ms(),
                part_target_us: args.nominal_audio_part_us(),
                audio_part: args.audio_part().to_string(),
                video_part: args.video_part().to_string(),
                segment_target_ms: args.fmp4_segment_ms,
                playlist_target_duration_ms: args.hls_target_duration_ms,
                playlist_count: args.playlist_count,
                playlist_buffer_kb: args.playlist_buffer_kb,
                contributor_local_playback_cache_enabled: false,
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
    let daw_media_socket_drops = daw_media_udp_socket_drops();

    for (name, help, metric_type, value) in [
        (
            "av_contrib_audio_epoch_hls_queue_capacity",
            "Configured capacity of the asynchronous format-preserving AEP1 to LL-HLS handoff.",
            "gauge",
            AUDIO_EPOCH_HLS_QUEUE_CAPACITY.load(Ordering::Relaxed),
        ),
        (
            "av_contrib_publication_queue_capacity",
            "Configured capacity of the bounded media publication handoff.",
            "gauge",
            PUBLICATION_QUEUE_CAPACITY_METRIC.load(Ordering::Relaxed),
        ),
        (
            "av_contrib_publication_queue_enqueued_total",
            "Media parts accepted by the bounded publication handoff.",
            "counter",
            PUBLICATION_QUEUE_ENQUEUED.load(Ordering::Relaxed),
        ),
        (
            "av_contrib_publication_queue_dropped_total",
            "Media parts rejected by a full or closed publication handoff.",
            "counter",
            PUBLICATION_QUEUE_DROPPED.load(Ordering::Relaxed),
        ),
        (
            "av_contrib_publication_queue_depth",
            "Current media publication queue depth.",
            "gauge",
            PUBLICATION_QUEUE_DEPTH.load(Ordering::Relaxed),
        ),
        (
            "av_contrib_publication_queue_max_depth",
            "Maximum media publication queue depth since process start.",
            "gauge",
            PUBLICATION_QUEUE_MAX_DEPTH.load(Ordering::Relaxed),
        ),
        (
            "av_contrib_publication_completed_total",
            "Media parts completed by asynchronous publication workers.",
            "counter",
            PUBLICATION_COMPLETED.load(Ordering::Relaxed),
        ),
        (
            "av_contrib_publication_inline_total",
            "Audio parts published on the packager's own task, without a publication queue.",
            "counter",
            PUBLICATION_INLINE.load(Ordering::Relaxed),
        ),
        (
            "av_contrib_publication_inline_slow_total",
            "Inline audio publications that held their packager 20 ms or more.",
            "counter",
            PUBLICATION_INLINE_SLOW.load(Ordering::Relaxed),
        ),
        (
            "av_contrib_publication_inline_max_us",
            "The longest inline audio publication since process start, in microseconds.",
            "gauge",
            PUBLICATION_INLINE_MAX_US.load(Ordering::Relaxed),
        ),
        (
            "av_contrib_publication_errors_total",
            "Downstream media publication failures in asynchronous workers.",
            "counter",
            PUBLICATION_ERRORS.load(Ordering::Relaxed),
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
            "av_contrib_audio_epoch_hls_queue_depth",
            "Current observed AEP1 LL-HLS handoff depth.",
            "gauge",
            AUDIO_EPOCH_HLS_QUEUE_DEPTH.load(Ordering::Relaxed),
        ),
        (
            "av_contrib_audio_epoch_hls_queue_max_depth",
            "Maximum observed AEP1 LL-HLS handoff depth since process start.",
            "gauge",
            AUDIO_EPOCH_HLS_QUEUE_MAX_DEPTH.load(Ordering::Relaxed),
        ),
        (
            "av_contrib_audio_epoch_mesh_queue_capacity",
            "Configured capacity of the asynchronous AEP1 mesh forward handoff.",
            "gauge",
            AUDIO_EPOCH_MESH_QUEUE_CAPACITY_METRIC.load(Ordering::Relaxed),
        ),
        (
            "av_contrib_audio_epoch_mesh_queue_enqueued_total",
            "AEP1 datagrams accepted by the asynchronous mesh forward handoff.",
            "counter",
            AUDIO_EPOCH_MESH_QUEUE_ENQUEUED.load(Ordering::Relaxed),
        ),
        (
            "av_contrib_audio_epoch_mesh_queue_dropped_total",
            "AEP1 datagrams rejected by a full or closed mesh forward handoff.",
            "counter",
            AUDIO_EPOCH_MESH_QUEUE_DROPPED.load(Ordering::Relaxed),
        ),
        (
            "av_contrib_audio_epoch_mesh_queue_depth",
            "Current observed AEP1 mesh forward handoff depth.",
            "gauge",
            AUDIO_EPOCH_MESH_QUEUE_DEPTH.load(Ordering::Relaxed),
        ),
        (
            "av_contrib_audio_epoch_mesh_queue_max_depth",
            "Maximum observed AEP1 mesh forward handoff depth since process start.",
            "gauge",
            AUDIO_EPOCH_MESH_QUEUE_MAX_DEPTH.load(Ordering::Relaxed),
        ),
        (
            "av_contrib_audio_epoch_mesh_forward_errors_total",
            "AEP1 mesh forward worker errors.",
            "counter",
            AUDIO_EPOCH_MESH_FORWARD_ERRORS.load(Ordering::Relaxed),
        ),
        (
            "av_contrib_audio_epoch_normalized_epochs_total",
            "Recovered contributor epochs re-encoded with codec-neutral SoundKit v2 payloads.",
            "counter",
            AUDIO_EPOCH_NORMALIZED_EPOCHS.load(Ordering::Relaxed),
        ),
        (
            "av_contrib_audio_epoch_normalization_errors_total",
            "Contributor epochs rejected before canonical SoundKit v2 mesh publication.",
            "counter",
            AUDIO_EPOCH_NORMALIZATION_ERRORS.load(Ordering::Relaxed),
        ),
        (
            "av_contrib_audio_epoch_mesh_send_queue_capacity",
            "Bounded capacity between SoundKit v2 normalization and mesh sending.",
            "gauge",
            AUDIO_EPOCH_MESH_SEND_QUEUE_CAPACITY_METRIC.load(Ordering::Relaxed),
        ),
        (
            "av_contrib_audio_epoch_mesh_send_queue_enqueued_total",
            "Canonical SoundKit v2 datagrams admitted for mesh sending.",
            "counter",
            AUDIO_EPOCH_MESH_SEND_QUEUE_ENQUEUED.load(Ordering::Relaxed),
        ),
        (
            "av_contrib_audio_epoch_mesh_send_queue_dropped_total",
            "Canonical SoundKit v2 datagrams rejected by a full mesh-send queue.",
            "counter",
            AUDIO_EPOCH_MESH_SEND_QUEUE_DROPPED.load(Ordering::Relaxed),
        ),
        (
            "av_contrib_audio_epoch_mesh_send_queue_depth",
            "Canonical SoundKit v2 datagrams currently queued for mesh sending.",
            "gauge",
            AUDIO_EPOCH_MESH_SEND_QUEUE_DEPTH.load(Ordering::Relaxed),
        ),
        (
            "av_contrib_audio_epoch_mesh_send_queue_max_depth",
            "Maximum canonical SoundKit v2 mesh-send queue depth since process start.",
            "gauge",
            AUDIO_EPOCH_MESH_SEND_QUEUE_MAX_DEPTH.load(Ordering::Relaxed),
        ),
        (
            "av_contrib_audio_epoch_ingress_targets",
            "Fixed AEP1 origin publication relationships; one normally, at most two with explicit redundancy.",
            "gauge",
            AUDIO_EPOCH_INGRESS_TARGETS.load(Ordering::Relaxed),
        ),
        (
            "av_contrib_audio_epoch_ingress_queue_capacity",
            "Configured capacity of the asynchronous AEP1 origin-to-ingress publication handoff.",
            "gauge",
            AUDIO_EPOCH_MESH_QUEUE_CAPACITY_METRIC.load(Ordering::Relaxed),
        ),
        (
            "av_contrib_audio_epoch_ingress_queue_enqueued_total",
            "AEP1 datagrams accepted for origin-to-ingress publication.",
            "counter",
            AUDIO_EPOCH_MESH_QUEUE_ENQUEUED.load(Ordering::Relaxed),
        ),
        (
            "av_contrib_audio_epoch_ingress_queue_dropped_total",
            "AEP1 datagrams rejected by a full or closed origin-to-ingress publication handoff.",
            "counter",
            AUDIO_EPOCH_MESH_QUEUE_DROPPED.load(Ordering::Relaxed),
        ),
        (
            "av_contrib_audio_epoch_ingress_queue_depth",
            "Current AEP1 origin-to-ingress publication queue depth.",
            "gauge",
            AUDIO_EPOCH_MESH_QUEUE_DEPTH.load(Ordering::Relaxed),
        ),
        (
            "av_contrib_audio_epoch_ingress_queue_max_depth",
            "Maximum AEP1 origin-to-ingress publication queue depth since process start.",
            "gauge",
            AUDIO_EPOCH_MESH_QUEUE_MAX_DEPTH.load(Ordering::Relaxed),
        ),
        (
            "av_contrib_audio_epoch_ingress_errors_total",
            "AEP1 origin-to-ingress publication errors.",
            "counter",
            AUDIO_EPOCH_MESH_FORWARD_ERRORS.load(Ordering::Relaxed),
        ),
        (
            "av_contrib_daw_media_udp_socket_drops_total",
            "Datagrams dropped at the Linux kernel receive queue for the contributor media socket.",
            "counter",
            daw_media_socket_drops.unwrap_or(0),
        ),
        (
            "av_contrib_daw_media_udp_socket_drop_observation_supported",
            "Whether per-socket Linux kernel receive-drop observation is available.",
            "gauge",
            u64::from(daw_media_socket_drops.is_some()),
        ),
        (
            "av_contrib_audio_epoch_hls_worker_datagrams_total",
            "AEP1 datagrams processed by the asynchronous LL-HLS worker.",
            "counter",
            audio_hls_worker.datagrams,
        ),
        (
            "av_contrib_audio_epoch_hls_groups_completed_total",
            "AEP1 audio groups completed for format-preserving LL-HLS packaging.",
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
        (
            "av_contrib_audio_epoch_hls_active_receivers",
            "Current AEP1 source/FEC receiver states retained by the contributor.",
            "gauge",
            audio_hls_worker.active_receivers,
        ),
        (
            "av_contrib_audio_epoch_hls_active_rendition_workers",
            "Current AEP1 LL-HLS rendition workers retained by the contributor.",
            "gauge",
            audio_hls_worker.active_rendition_workers,
        ),
        (
            "av_contrib_audio_epoch_hls_retired_receivers_total",
            "Idle AEP1 source/FEC receiver states retired since process start.",
            "counter",
            audio_hls_worker.retired_receivers,
        ),
        (
            "av_contrib_audio_epoch_hls_retired_rendition_workers_total",
            "Idle AEP1 LL-HLS rendition workers retired since process start.",
            "counter",
            audio_hls_worker.retired_rendition_workers,
        ),
        (
            "av_contrib_audio_epoch_hls_rendition_queue_capacity_per_worker",
            "Bounded recovered-group queue capacity for each AEP1 LL-HLS rendition worker.",
            "gauge",
            audio_hls_worker.rendition_queue_capacity_per_worker,
        ),
        (
            "av_contrib_audio_epoch_hls_rendition_queue_enqueued_total",
            "Recovered AEP1 groups admitted to rendition workers.",
            "counter",
            audio_hls_worker.rendition_queue_enqueued,
        ),
        (
            "av_contrib_audio_epoch_hls_rendition_queue_dropped_total",
            "Recovered AEP1 groups rejected because a rendition queue was full.",
            "counter",
            audio_hls_worker.rendition_queue_dropped,
        ),
        (
            "av_contrib_audio_epoch_hls_rendition_queue_depth",
            "Recovered AEP1 groups currently queued across rendition workers.",
            "gauge",
            audio_hls_worker.rendition_queue_depth,
        ),
        (
            "av_contrib_audio_epoch_hls_rendition_queue_max_depth",
            "Maximum recovered AEP1 groups queued across rendition workers since process start.",
            "gauge",
            audio_hls_worker.rendition_queue_max_depth,
        ),
        (
            "av_contrib_audio_epoch_hls_recovered_epochs_shared_total",
            "Recovered AEP1 epochs that the LL-HLS worker handed to the mesh normaliser (--aep1-shared-recovery).",
            "counter",
            audio_hls_worker.recovered_epochs_shared,
        ),
        (
            "av_contrib_audio_epoch_hls_recovered_epochs_dropped_total",
            "Recovered AEP1 epochs that the full mesh normaliser queue refused (--aep1-shared-recovery).",
            "counter",
            audio_hls_worker.recovered_epochs_dropped,
        ),
        (
            "av_contrib_local_playback_cache_enabled",
            "Whether the contributor retains a viewer-facing LL-HLS playback cache.",
            "gauge",
            u64::from(snapshot.hls.contributor_local_playback_cache_enabled),
        ),
    ] {
        push_prometheus_metric_header(&mut output, name, help, metric_type);
        output.push_str(&format!("{name} {value}\n"));
    }

    let ingress_queue_age = audio_epoch_ingress_queue_age().snapshot();
    push_prometheus_metric_header(
        &mut output,
        "av_contrib_audio_epoch_ingress_queue_age_seconds",
        "Time an AEP1 datagram waits before origin-to-ingress publication begins.",
        "histogram",
    );
    for (upper_bound_us, count) in DURATION_HISTOGRAM_BUCKETS_US
        .iter()
        .zip(ingress_queue_age.buckets.iter())
    {
        output.push_str(&format!(
            "av_contrib_audio_epoch_ingress_queue_age_seconds_bucket{{le=\"{}\"}} {count}\n",
            *upper_bound_us as f64 / 1_000_000.0
        ));
    }
    output.push_str(&format!(
        "av_contrib_audio_epoch_ingress_queue_age_seconds_bucket{{le=\"+Inf\"}} {}\n",
        ingress_queue_age.count
    ));
    output.push_str(&format!(
        "av_contrib_audio_epoch_ingress_queue_age_seconds_sum {}\n",
        ingress_queue_age.sum_us as f64 / 1_000_000.0
    ));
    output.push_str(&format!(
        "av_contrib_audio_epoch_ingress_queue_age_seconds_count {}\n",
        ingress_queue_age.count
    ));

    for (name, help, histogram) in [
        (
            "av_contrib_publication_queue_age_seconds",
            "Time a media part waits before downstream publication begins.",
            publication_queue_age().snapshot(),
        ),
        (
            "av_contrib_publication_duration_seconds",
            "Time required to complete downstream publication for one media part.",
            publication_duration().snapshot(),
        ),
    ] {
        push_prometheus_metric_header(&mut output, name, help, "histogram");
        for (upper_bound_us, count) in DURATION_HISTOGRAM_BUCKETS_US
            .iter()
            .zip(histogram.buckets.iter())
        {
            output.push_str(&format!(
                "{name}_bucket{{le=\"{}\"}} {count}\n",
                *upper_bound_us as f64 / 1_000_000.0
            ));
        }
        output.push_str(&format!(
            "{name}_bucket{{le=\"+Inf\"}} {}\n",
            histogram.count
        ));
        output.push_str(&format!(
            "{name}_sum {}\n",
            histogram.sum_us as f64 / 1_000_000.0
        ));
        output.push_str(&format!("{name}_count {}\n", histogram.count));
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
        snapshot.hls.part_target_us as f64 / 1_000_000.0
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
        (
            "av_contrib_relay_session_lane_backlog_parts_total",
            "Audio parts on one RelaySession lane that waited for room in the socket buffer.",
            runtime.relay_session.lane_backlog_parts,
        ),
        (
            "av_contrib_relay_session_lane_backlog_dropped_total",
            "Audio parts on one RelaySession lane dropped because the lane backlog was full.",
            runtime.relay_session.lane_backlog_dropped,
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
        (
            "av_contrib_relay_session_direct_lane_objects_succeeded_total",
            "Objects handed to the kernel for a direct regional distributor (one per distributor).",
            runtime.relay_session.direct_lane_objects_succeeded,
        ),
        (
            "av_contrib_relay_session_direct_lane_objects_failed_total",
            "Objects that a direct regional distributor lane failed to send (one per distributor).",
            runtime.relay_session.direct_lane_objects_failed,
        ),
        (
            "av_contrib_relay_session_direct_datagrams_total",
            "RelaySession datagrams sent on the direct regional distributor lanes.",
            runtime.relay_session.direct_datagrams,
        ),
        (
            "av_contrib_relay_session_direct_datagram_bytes_total",
            "RelaySession datagram bytes sent on the direct regional distributor lanes.",
            runtime.relay_session.direct_datagram_bytes,
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
    lane_backlog_parts: u64,
    lane_backlog_dropped: u64,
    primary_lane_objects_succeeded: u64,
    primary_lane_objects_failed: u64,
    primary_lane_state: &'static str,
    secondary_lane_objects_succeeded: u64,
    secondary_lane_objects_failed: u64,
    secondary_lane_state: &'static str,
    surviving_lane_objects: u64,
    all_lanes_failed_objects: u64,
    /// One count per direct distributor per object.
    direct_lane_objects_succeeded: u64,
    direct_lane_objects_failed: u64,
    direct_datagrams: u64,
    direct_datagram_bytes: u64,
    direct_send: DurationHistogramSnapshot,
    expired_objects: u64,
    expired_symbols: u64,
    /// `--relay-video-gso`: UDP GSO messages of the video lanes, and the
    /// datagrams the kernel cut them into.
    gso_messages: u64,
    gso_datagrams: u64,
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
    /// `--relay-direct-target`, with the node ids in the same order.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    relay_direct_targets: Vec<String>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    relay_direct_ids: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    relay_direct_bind: Option<String>,
    relay_direct_repair: bool,
    /// `--relay-send-batch`.
    relay_send_batch: bool,
    /// `--relay-video-gso` (with `--relay-send-batch`); the runtime's
    /// `gso_messages` say whether the host took it.
    relay_video_gso: bool,
    /// `--relay-video-gso-segments`.
    relay_video_gso_segments: u16,
    /// `--relay-video-gso-pace-mbps` (0: unpaced).
    relay_video_gso_pace_mbps: u64,
    /// `--relay-class-lanes` and `--relay-class-port-offset`.
    relay_class_lanes: bool,
    relay_class_port_offset: u16,
    /// `--relay-audio-inline`.
    relay_audio_inline: bool,
    /// `--relay-repair-both-lanes`.
    relay_repair_both_lanes: bool,
    /// `--relay-source-first` (with `--relay-send-batch`).
    relay_source_first: bool,
    /// `--relay-video-symbol-size` (0: the encoder's default).
    relay_video_symbol_size: u16,
    /// `--relay-video-plan-grid`.
    relay_video_plan_grid: bool,
    /// `--relay-repair-alongside-sources`.
    relay_repair_alongside_sources: bool,
    /// `--audio-hot-thread`.
    audio_hot_thread: bool,
    /// `--aep1-shared-recovery`.
    aep1_shared_recovery: bool,
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
    /// Audio part in whole milliseconds, rounded up; one epoch counts as
    /// the sender's default epoch of 1 ms.
    part_target_ms: u32,
    part_target_us: u64,
    /// `epoch`, `<N>samples` or `<N>us`.
    audio_part: String,
    /// `frame` or `<N>ms`.
    video_part: String,
    segment_target_ms: u32,
    playlist_target_duration_ms: u32,
    playlist_count: usize,
    playlist_buffer_kb: usize,
    contributor_local_playback_cache_enabled: bool,
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
            backend: Some("pure-rust"),
            profile: Some(args.rist_profile.as_str()),
            flow_id: None,
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
    status: Arc<ContribStatusConfig>,
    publish_ingress_gate: Option<Arc<PublishIngressGate>>,
    needletail_authorizer: Option<NeedletailSessionAuthorizer>,
    audio_epoch_hls_tx: Option<mpsc::Sender<AudioEpochHlsDatagram>>,
    audio_epoch_mesh_tx: Option<mpsc::Sender<QueuedAudioEpochIngressDatagram>>,
    needletail_ingest_active: Arc<AtomicBool>,
}

impl ContribRouter {
    fn new(
        forwarder: Arc<MeshForwarder>,
        default_stream_id: u64,
        status: Arc<ContribStatusConfig>,
        publish_ingress_gate: Option<Arc<PublishIngressGate>>,
        needletail_authorizer: Option<NeedletailSessionAuthorizer>,
        audio_epoch_hls_tx: Option<mpsc::Sender<AudioEpochHlsDatagram>>,
        audio_epoch_mesh_tx: Option<mpsc::Sender<QueuedAudioEpochIngressDatagram>>,
    ) -> Self {
        Self {
            forwarder,
            default_stream_id,
            status,
            publish_ingress_gate,
            needletail_authorizer,
            audio_epoch_hls_tx,
            audio_epoch_mesh_tx,
            needletail_ingest_active: Arc::new(AtomicBool::new(false)),
        }
    }
}

#[derive(Clone)]
struct NeedletailSessionAuthorizer {
    http: reqwest::Client,
    sessions_api_base: reqwest::Url,
    expected_stream_id: u64,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct NeedletailBootstrapResponse {
    session_id: String,
    endpoint_id: String,
    route: NeedletailBootstrapRoute,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct NeedletailBootstrapRoute {
    kind: String,
    stream_id: String,
}

impl NeedletailSessionAuthorizer {
    fn from_environment(expected_stream_id: u64) -> Result<Option<Self>> {
        let Some(value) = std::env::var_os("AV_NEEDLETAIL_SESSION_API_BASE") else {
            return Ok(None);
        };
        let value = value
            .into_string()
            .map_err(|_| anyhow::anyhow!("AV_NEEDLETAIL_SESSION_API_BASE is invalid"))?;
        let sessions_api_base =
            reqwest::Url::parse(&value).context("AV_NEEDLETAIL_SESSION_API_BASE is not a URL")?;
        let local_development = matches!(
            sessions_api_base.host_str(),
            Some("localhost" | "127.0.0.1" | "::1")
        );
        if sessions_api_base.host_str().is_none()
            || (sessions_api_base.scheme() != "https"
                && !(sessions_api_base.scheme() == "http" && local_development))
            || !sessions_api_base.username().is_empty()
            || sessions_api_base.password().is_some()
            || sessions_api_base.query().is_some()
            || sessions_api_base.fragment().is_some()
        {
            bail!("AV_NEEDLETAIL_SESSION_API_BASE must be an HTTPS origin");
        }
        let http = reqwest::Client::builder()
            .timeout(Duration::from_secs(10))
            .build()
            .context("failed to initialize Needletail session authorizer")?;
        Ok(Some(Self {
            http,
            sessions_api_base,
            expected_stream_id,
        }))
    }

    async fn authorize(
        &self,
        bearer: &str,
        session_id: &str,
        endpoint_id: &str,
    ) -> std::result::Result<(), StatusCode> {
        if !ingest_identifier(session_id) || !ingest_identifier(endpoint_id) {
            return Err(StatusCode::BAD_REQUEST);
        }
        let route = format!(
            "v1/sessions/{}/media/publication-bootstrap",
            url_encode_path_segment(session_id)
        );
        let url = self
            .sessions_api_base
            .join(&route)
            .map_err(|_| StatusCode::SERVICE_UNAVAILABLE)?;
        let response = self
            .http
            .post(url)
            .bearer_auth(bearer)
            .json(&serde_json::json!({
                "version": 1,
                "endpointId": endpoint_id,
            }))
            .send()
            .await
            .map_err(|_| StatusCode::SERVICE_UNAVAILABLE)?;
        if !response.status().is_success() {
            return Err(match response.status().as_u16() {
                401 => StatusCode::UNAUTHORIZED,
                403 | 404 => StatusCode::FORBIDDEN,
                409 => StatusCode::CONFLICT,
                _ => StatusCode::SERVICE_UNAVAILABLE,
            });
        }
        if response
            .content_length()
            .is_some_and(|length| length > 32 * 1024)
        {
            return Err(StatusCode::BAD_GATEWAY);
        }
        let bytes = response
            .bytes()
            .await
            .map_err(|_| StatusCode::BAD_GATEWAY)?;
        if bytes.len() > 32 * 1024 {
            return Err(StatusCode::BAD_GATEWAY);
        }
        let bootstrap: NeedletailBootstrapResponse =
            serde_json::from_slice(&bytes).map_err(|_| StatusCode::BAD_GATEWAY)?;
        if bootstrap.session_id != session_id
            || bootstrap.endpoint_id != endpoint_id
            || bootstrap.route.kind != "needletail_london"
            || bootstrap.route.stream_id != self.expected_stream_id.to_string()
        {
            return Err(StatusCode::FORBIDDEN);
        }
        Ok(())
    }
}

fn ingest_identifier(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 128
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
}

fn url_encode_path_segment(value: &str) -> String {
    form_urlencoded::byte_serialize(value.as_bytes()).collect()
}

struct NeedletailIngestGuard(Arc<AtomicBool>);

impl Drop for NeedletailIngestGuard {
    fn drop(&mut self) {
        self.0.store(false, Ordering::Release);
    }
}

fn needletail_ingest_query(
    query: Option<&str>,
) -> std::result::Result<(String, String), StatusCode> {
    let mut session_id = None;
    let mut endpoint_id = None;
    for (name, value) in form_urlencoded::parse(query.unwrap_or_default().as_bytes()) {
        match name.as_ref() {
            "sessionId" if session_id.is_none() => session_id = Some(value.into_owned()),
            "endpointId" if endpoint_id.is_none() => endpoint_id = Some(value.into_owned()),
            _ => return Err(StatusCode::BAD_REQUEST),
        }
    }
    let session_id = session_id.filter(|value| ingest_identifier(value));
    let endpoint_id = endpoint_id.filter(|value| ingest_identifier(value));
    match (session_id, endpoint_id) {
        (Some(session_id), Some(endpoint_id)) => Ok((session_id, endpoint_id)),
        _ => Err(StatusCode::BAD_REQUEST),
    }
}

/// `GET /api/hops`: the journal as NDJSON, oldest first. It reads the ring
/// only; it creates no demand and opens no session. The copy, sort and
/// serialization of up to the whole ring (tens of MB) run on the blocking
/// pool, not on a runtime worker that the publication tasks share.
async fn hop_journal_response(
    journal: Option<&'static HopJournal>,
    query: Option<&str>,
) -> HandlerResponse {
    let Some(journal) = journal else {
        return response(
            StatusCode::NOT_FOUND,
            Some(Bytes::from_static(b"the hop journal is disabled\n")),
            Some("text/plain; charset=utf-8"),
        );
    };
    let query = query.map(str::to_owned);
    match tokio::task::spawn_blocking(move || journal.respond(query.as_deref())).await {
        Ok(Ok(body)) => response(
            StatusCode::OK,
            Some(Bytes::from(body)),
            Some(HOP_JOURNAL_CONTENT_TYPE),
        ),
        Ok(Err(error)) => response(
            StatusCode::BAD_REQUEST,
            Some(Bytes::from(format!("{error}\n"))),
            Some("text/plain; charset=utf-8"),
        ),
        Err(error) => response(
            StatusCode::INTERNAL_SERVER_ERROR,
            Some(Bytes::from(format!("hop journal query failed: {error}\n"))),
            Some("text/plain; charset=utf-8"),
        ),
    }
}

fn needletail_ingest_response(status: StatusCode) -> HandlerResponse {
    let body = serde_json::to_vec(&serde_json::json!({
        "error": "needletail_ingest_rejected",
    }))
    .unwrap_or_else(|_| b"{\"error\":\"needletail_ingest_rejected\"}".to_vec());
    response(status, Some(Bytes::from(body)), Some("application/json"))
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
                    b"av-contrib\n\nPOST /ingest?stream_id=... publishes arbitrary stream bytes\nPOST /media/access-unit forwards detected media access units\nGET /api/status returns service status for Needletail Mission Control\nGET /api/status/events streams service status as SSE\nGET /metrics returns Prometheus metrics\nGET /up checks health\n",
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
            HOP_JOURNAL_PATH => {
                Ok(hop_journal_response(hop_journal::global(), req.uri().query()).await)
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
            _ => Ok(response(
                StatusCode::NOT_FOUND,
                Some(Bytes::from_static(
                    b"viewer delivery is served by a relay or playback edge\n",
                )),
                Some("text/plain; charset=utf-8"),
            )),
        }
    }

    async fn route_body(
        &self,
        req: Request<()>,
        mut body: BodyStream,
    ) -> HandlerResult<HandlerResponse> {
        let path = req.uri().path().to_string();
        if path == NEEDLETAIL_AEP1_INGEST_PATH {
            if req.method() != Method::POST {
                return Ok(response(StatusCode::METHOD_NOT_ALLOWED, None, None));
            }
            let Some(authorizer) = self.needletail_authorizer.as_ref() else {
                return Ok(needletail_ingest_response(StatusCode::SERVICE_UNAVAILABLE));
            };
            let Some(audio_epoch_hls_tx) = self.audio_epoch_hls_tx.as_ref() else {
                return Ok(needletail_ingest_response(StatusCode::SERVICE_UNAVAILABLE));
            };
            // Absent with --aep1-shared-recovery: the LL-HLS worker then
            // feeds the mesh lane with the epochs it recovers.
            let audio_epoch_mesh_tx = self.audio_epoch_mesh_tx.as_ref();
            if req.headers().get_all(AUTHORIZATION).iter().count() != 1 {
                return Ok(needletail_ingest_response(StatusCode::UNAUTHORIZED));
            }
            let bearer = match parse_bearer_header(
                req.headers()
                    .get(AUTHORIZATION)
                    .map(|value| value.as_bytes()),
            ) {
                Ok(value) => value,
                Err(_) => return Ok(needletail_ingest_response(StatusCode::UNAUTHORIZED)),
            };
            let (session_id, endpoint_id) = match needletail_ingest_query(req.uri().query()) {
                Ok(value) => value,
                Err(status) => return Ok(needletail_ingest_response(status)),
            };
            if let Err(status) = authorizer
                .authorize(bearer, &session_id, &endpoint_id)
                .await
            {
                return Ok(needletail_ingest_response(status));
            }
            if req
                .headers()
                .get("x-needletail-ingest-preflight")
                .is_some_and(|value| value.as_bytes() == b"1")
            {
                return Ok(response(StatusCode::NO_CONTENT, None, None));
            }
            if req
                .headers()
                .get("content-type")
                .is_none_or(|value| value.as_bytes() != NEEDLETAIL_AEP1_CONTENT_TYPE.as_bytes())
            {
                return Ok(needletail_ingest_response(
                    StatusCode::UNSUPPORTED_MEDIA_TYPE,
                ));
            }
            if self
                .needletail_ingest_active
                .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
                .is_err()
            {
                return Ok(needletail_ingest_response(StatusCode::CONFLICT));
            }
            let _active = NeedletailIngestGuard(self.needletail_ingest_active.clone());
            let peer = SocketAddr::from(([127, 0, 0, 1], 0));
            let mut pending = BytesMut::new();
            let mut datagrams = 0_u64;
            let mut payload_bytes = 0_u64;
            while let Some(next) = body.next().await {
                let chunk = next?;
                if pending.len().saturating_add(chunk.len()) > NEEDLETAIL_AEP1_MAX_BUFFER_BYTES {
                    return Ok(needletail_ingest_response(StatusCode::PAYLOAD_TOO_LARGE));
                }
                pending.extend_from_slice(&chunk);
                loop {
                    if pending.len() < 4 {
                        break;
                    }
                    let length =
                        u32::from_be_bytes([pending[0], pending[1], pending[2], pending[3]])
                            as usize;
                    if length == 0 || length > NEEDLETAIL_AEP1_MAX_DATAGRAM_BYTES {
                        return Ok(needletail_ingest_response(StatusCode::PAYLOAD_TOO_LARGE));
                    }
                    let framed_length = 4 + length;
                    if pending.len() < framed_length {
                        break;
                    }
                    let frame = pending.split_to(framed_length);
                    let datagram = &frame[4..];
                    if !is_audio_transport_datagram(datagram) {
                        return Ok(needletail_ingest_response(StatusCode::BAD_REQUEST));
                    }
                    handoff_audio_epoch_hls_datagram(
                        Some(audio_epoch_hls_tx),
                        peer,
                        datagram,
                        hop_journal::now_unix_us(),
                    );
                    handoff_audio_epoch_mesh_datagram(audio_epoch_mesh_tx, peer, datagram);
                    datagrams = datagrams.saturating_add(1);
                    payload_bytes = payload_bytes.saturating_add(length as u64);
                }
            }
            if !pending.is_empty() {
                return Ok(needletail_ingest_response(StatusCode::BAD_REQUEST));
            }
            let body = serde_json::to_vec(&serde_json::json!({
                "accepted": true,
                "streamId": authorizer.expected_stream_id.to_string(),
                "datagrams": datagrams,
                "bytes": payload_bytes,
            }))
            .map_err(|error| ServerError::Handler(Box::new(error)))?;
            return Ok(response(
                StatusCode::ACCEPTED,
                Some(Bytes::from(body)),
                Some("application/json"),
            ));
        }
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

        Ok(response(
            StatusCode::NOT_FOUND,
            Some(Bytes::from_static(
                b"viewer delivery is served by a relay or playback edge\n",
            )),
            Some("text/plain; charset=utf-8"),
        ))
    }

    fn has_body_handler(&self, path: &str) -> bool {
        path == "/ingest" || path == MEDIA_ACCESS_UNIT_PATH || path == NEEDLETAIL_AEP1_INGEST_PATH
    }

    fn is_streaming(&self, path: &str) -> bool {
        path == CONTRIB_STATUS_EVENTS_PATH
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

        stream_writer
            .send_response(
                Response::builder()
                    .status(StatusCode::NOT_FOUND)
                    .body(())
                    .map_err(ServerError::Http)?,
            )
            .await
    }

    fn webtransport_handler(&self) -> Option<&dyn WebTransportHandler> {
        None
    }

    fn websocket_handler(&self, _path: &str) -> Option<&dyn WebSocketHandler> {
        None
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "av_contrib=info,web_service=info".into()),
        )
        .init();

    let args = Args::parse();
    av_contrib::fmp4_bridge::set_default_flush_on_complete(args.fmp4_flush_on_complete);
    if let Some(warning) = args.legacy_audio_part_warning() {
        warn!("{warning}");
    }
    // Audio-only parts out of the video ingests (RTMP, SRT, RIST, MPEG-TS)
    // are cut to the audio part, which the stamper declares for them.
    av_contrib::fmp4_bridge::set_default_audio_only_part_us(Some(args.audio_only_part_us()));
    hop_journal::install(HopJournal::new(
        args.relay_local_id.clone(),
        args.hop_journal_capacity,
    ));
    if args.hop_journal_capacity > 0 {
        let parts_per_second = args.nominal_audio_parts_per_second();
        info!(
            capacity = args.hop_journal_capacity,
            parts_per_second,
            seconds = hop_journal::covered_seconds(args.hop_journal_capacity, parts_per_second),
            "hop journal holds this many seconds of the nominal audio parts \
             (--hop-journal-capacity)"
        );
    }
    validate_compiled_ingress_features(&args)?;
    let publish_ingress_gate = load_publish_ingress_gate()?;
    validate_enforced_ingress_adapters(&args, publish_ingress_gate.as_deref())?;
    let (cert, key) = load_tls(&args)?;
    let (publication_shutdown_tx, publication_shutdown_rx) = watch::channel(());
    let telemetry = Arc::new(IngestTelemetry::default());
    let forwarder = Arc::new(MeshForwarder::new(&args, Arc::clone(&telemetry)).await?);
    let mesh_publisher: Arc<dyn Fmp4PartPublisher> = forwarder.clone();
    let (publisher, publication_tasks) = start_bounded_publication_pipeline(
        mesh_publisher,
        telemetry.clone(),
        args.stamp_durations(),
        args.publication_shard_mix,
        args.relay_audio_inline,
        publication_shutdown_rx,
    );
    let (playlists, _chunk_cache, _m3u8_cache) = playlists::Playlists::new(playlist_options(&args));
    #[cfg(all(feature = "quadra-renditions", target_os = "linux"))]
    if args.quadra.derived_only && args.quadra.renditions.is_empty() {
        bail!("--quadra-derived-only requires at least one --quadra-rendition");
    }
    #[cfg(all(feature = "quadra-renditions", target_os = "linux"))]
    let rist_access_unit_sink: Option<Arc<dyn AccessUnitSink>> =
        if args.quadra.renditions.is_empty() {
            None
        } else {
            if args.rist_bind.is_none() {
                bail!("--quadra-rendition requires --rist-bind");
            }
            let config = QuadraRenditionConfig {
                source_stream_id: args.rist_stream_id,
                source_width: args.quadra.source_width,
                source_height: args.quadra.source_height,
                frame_rate_millihertz: args.quadra.frame_rate_millihertz,
                hardware_id: args.quadra.hardware_id,
                queue_capacity: args.quadra.queue_capacity,
                renditions: args.quadra.renditions.clone(),
            };
            config.validate().map_err(|error| {
                anyhow::anyhow!("invalid Quadra rendition configuration: {error}")
            })?;
            let mut targets = Vec::with_capacity(config.renditions.len());
            for spec in &config.renditions {
                targets.push(QuadraRenditionTarget {
                    spec: *spec,
                    stream_idx: resolve_output_stream_idx(&playlists, spec.stream_id).await,
                });
            }
            Some(
                start_quadra_rendition_pipeline(
                    config,
                    targets,
                    playlists.clone(),
                    publisher.clone(),
                    args.video_part_ms(),
                )
                .await
                .map_err(|error| {
                    anyhow::anyhow!("failed to start Quadra rendition pipeline: {error}")
                })?,
            )
        };
    #[cfg(all(feature = "quadra-renditions", target_os = "linux"))]
    let rist_publish_source = !args.quadra.derived_only;
    #[cfg(not(all(feature = "quadra-renditions", target_os = "linux")))]
    let rist_access_unit_sink: Option<Arc<dyn AccessUnitSink>> = None;
    #[cfg(not(all(feature = "quadra-renditions", target_os = "linux")))]
    let rist_publish_source = true;
    let status = Arc::new(ContribStatusConfig::from_args(&args, telemetry.clone()));
    let (shutdown_tx, shutdown_rx) = watch::channel(());
    let rist_shutdown = if let Some(bind) = args.rist_bind {
        let output_stream_idx = resolve_output_stream_idx(&playlists, args.rist_stream_id).await;
        Some(
            start_rist_ingest(
                RistIngestConfig {
                    bind,
                    profile: args.rist_profile,
                    output_stream_id: args.rist_stream_id,
                    output_stream_idx,
                    min_part_ms: args.video_part_ms(),
                },
                playlists.clone(),
                publisher.clone(),
                rist_access_unit_sink,
                rist_publish_source,
                telemetry.clone(),
                shutdown_rx.clone(),
            )
            .await?,
        )
    } else {
        None
    };
    #[cfg(feature = "srt-ingest")]
    let srt_shutdown = if let Some(bind) = args.srt_bind {
        Some(
            start_srt_ingest(
                SrtIngestConfig {
                    bind,
                    output_stream_id: args.srt_stream_id,
                    min_part_ms: args.video_part_ms(),
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
            args.video_part_ms(),
        ));
        (Some(shutdown), Some(finished), Some(task))
    } else {
        (None, None, None)
    };
    let (
        daw_media_task,
        audio_epoch_hls_task,
        audio_epoch_normalizer_task,
        audio_epoch_mesh_task,
        router_audio_epoch_hls_tx,
        router_audio_epoch_mesh_tx,
        // Kept until the end: its threads stop when it is dropped.
        _audio_hot,
    ) = if let Some(bind) = args.daw_media_bind {
        let socket = bind_daw_media_udp_socket(bind).await?;
        let local_addr = socket.local_addr()?;
        let (audio_epoch_hls_tx, audio_epoch_hls_rx) =
            audio_epoch_hls_channel(args.daw_hls_queue_capacity);
        let (audio_epoch_mesh_tx, audio_epoch_mesh_rx) =
            mpsc::channel(AUDIO_EPOCH_MESH_QUEUE_CAPACITY);
        let (audio_epoch_mesh_send_tx, audio_epoch_mesh_send_rx) =
            mpsc::channel(AUDIO_EPOCH_MESH_SEND_QUEUE_CAPACITY);
        AUDIO_EPOCH_HLS_QUEUE_CAPACITY
            .store(audio_epoch_hls_tx.max_capacity() as u64, Ordering::Relaxed);
        AUDIO_EPOCH_MESH_QUEUE_CAPACITY_METRIC
            .store(audio_epoch_mesh_tx.max_capacity() as u64, Ordering::Relaxed);
        AUDIO_EPOCH_MESH_SEND_QUEUE_CAPACITY_METRIC.store(
            audio_epoch_mesh_send_tx.max_capacity() as u64,
            Ordering::Relaxed,
        );
        let mut audio_epoch_hls_config = AudioEpochHlsConfig::new(
            args.daw_hls_base_stream_id,
            args.audio_part(),
            args.daw_hls_packaging.into(),
            args.daw_hls_formats
                .iter()
                .copied()
                .map(AudioPayloadKind::from)
                .collect(),
            playlists.clone(),
            publisher.clone(),
        );
        // With --aep1-shared-recovery the ingest hands each datagram to the
        // LL-HLS worker only, and the mesh normaliser takes the epochs that
        // the worker recovered; else each path decodes its own copy.
        let (audio_epoch_normalizer_task, ingest_audio_epoch_mesh_tx) = if args.aep1_shared_recovery
        {
            drop(audio_epoch_mesh_rx);
            let (recovered_tx, recovered_rx) = mpsc::channel(AUDIO_EPOCH_MESH_QUEUE_CAPACITY);
            audio_epoch_hls_config = audio_epoch_hls_config.with_recovered_epoch_sink(recovered_tx);
            let task = tokio::spawn(run_recovered_audio_epoch_normalizer(
                recovered_rx,
                audio_epoch_mesh_send_tx,
                shutdown_rx.clone(),
            ));
            (task, None)
        } else {
            let task = tokio::spawn(run_audio_epoch_soundkit_normalizer(
                audio_epoch_mesh_rx,
                audio_epoch_mesh_send_tx,
                shutdown_rx.clone(),
            ));
            (task, Some(audio_epoch_mesh_tx))
        };
        // The DAW receive, the AEP1 recovery and the rendition workers run
        // on the audio hot threads: no AEP1 datagram or audio part waits for
        // a worker of the main runtime (4K encodes, the RTMP/TS bridges).
        let audio_hot = AudioHotRuntime::start(&args)?;
        let audio_epoch_hls_config = audio_epoch_hls_config.with_rendition_runtimes(
            audio_hot
                .as_ref()
                .and_then(AudioHotRuntime::rendition_runtimes),
        );
        let audio_epoch_hls_task = spawn_audio_hot_task(
            audio_hot.as_ref(),
            run_audio_epoch_hls_worker(
                audio_epoch_hls_config,
                audio_epoch_hls_rx,
                shutdown_rx.clone(),
            ),
        );
        let audio_epoch_mesh_task = tokio::spawn(run_audio_epoch_mesh_forward_worker(
            forwarder.clone(),
            audio_epoch_mesh_send_rx,
            shutdown_rx.clone(),
        ));
        info!(
            bind = %local_addr,
            audio_epoch_ingress_target = %forwarder.audio_epoch_ingress_targets[0],
            audio_epoch_ingress_targets = AUDIO_EPOCH_INGRESS_TARGETS.load(Ordering::Relaxed),
            hls_base_stream_id = args.stream_id,
            hls_queue_capacity = args.daw_hls_queue_capacity,
            mesh_queue_capacity = AUDIO_EPOCH_MESH_QUEUE_CAPACITY,
            aep1_shared_recovery = args.aep1_shared_recovery,
            "DAW media contributor ingest listening"
        );
        let daw_media_task = if audio_hot.is_some() {
            // The socket moves to the hot runtime's own I/O driver.
            let socket = socket
                .into_std()
                .context("failed to move the DAW media socket to the audio hot thread")?;
            let forwarder = forwarder.clone();
            let audio_epoch_hls_tx = Some(audio_epoch_hls_tx.clone());
            let audio_epoch_mesh_tx = ingest_audio_epoch_mesh_tx.clone();
            let shutdown_rx = shutdown_rx.clone();
            spawn_audio_hot_task(audio_hot.as_ref(), async move {
                let socket = UdpSocket::from_std(socket)
                    .context("failed to register the DAW media socket on the audio hot thread")?;
                run_daw_media_udp_ingest(
                    socket,
                    forwarder,
                    audio_epoch_hls_tx,
                    audio_epoch_mesh_tx,
                    shutdown_rx,
                )
                .await
            })
        } else {
            tokio::spawn(run_daw_media_udp_ingest(
                socket,
                forwarder.clone(),
                Some(audio_epoch_hls_tx.clone()),
                ingest_audio_epoch_mesh_tx.clone(),
                shutdown_rx.clone(),
            ))
        };
        (
            Some(daw_media_task),
            Some(audio_epoch_hls_task),
            Some(audio_epoch_normalizer_task),
            Some(audio_epoch_mesh_task),
            Some(audio_epoch_hls_tx),
            ingest_audio_epoch_mesh_tx,
            audio_hot,
        )
    } else {
        (None, None, None, None, None, None, None)
    };
    let needletail_authorizer = NeedletailSessionAuthorizer::from_environment(args.stream_id)?;
    let router = Box::new(ContribRouter::new(
        forwarder.clone(),
        args.stream_id,
        status,
        publish_ingress_gate,
        needletail_authorizer,
        router_audio_epoch_hls_tx,
        router_audio_epoch_mesh_tx,
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
    println!("ll-hls:  published to the configured mesh ingress");
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
            "rist:    rist://127.0.0.1:{} backend=pure-rust profile={} stream_id={}",
            bind.port(),
            args.rist_profile.as_str(),
            args.rist_stream_id
        );
    }
    #[cfg(feature = "srt-ingest")]
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
    #[cfg(feature = "srt-ingest")]
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
    if let Some(task) = audio_epoch_normalizer_task {
        let _ = task.await;
    }
    if let Some(task) = audio_epoch_mesh_task {
        let _ = task.await;
    }
    let _ = publication_shutdown_tx.send(());
    drop(publisher);
    for task in publication_tasks {
        let _ = task.await;
    }
    Ok(())
}

fn response(
    status: StatusCode,
    body: Option<Bytes>,
    content_type: Option<&'static str>,
) -> HandlerResponse {
    HandlerResponse {
        status,
        body,
        content_type: content_type.map(Into::into),
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

/// Segments and parts per segment of the contributor's local playlist
/// caches.
///
/// Every segmenter of the contributor publishes only (`retain_local_cache`
/// is false outside tests): the local Playlists maps stream ids to cache
/// indexes and nothing more, and its ChunkCache and M3u8Cache are never
/// written or served. The ChunkCache is still built up front, a slot per
/// stream x segment x part. Sized from the 1 ms default part
/// (1,008 parts a segment, 32 segments, 65 streams) they took 2.1M slots
/// each, 276 MB of RSS at start, for memory nobody reads; 128 parts (the
/// old floor) took 34 MB. These sizes take about 1 MB, whatever the part.
const LOCAL_PLAYLIST_SEGMENTS: usize = 4;
const LOCAL_PLAYLIST_PARTS_PER_SEGMENT: usize = 16;

fn playlist_options(args: &Args) -> playlists::Options {
    playlists::Options {
        max_segments: LOCAL_PLAYLIST_SEGMENTS,
        num_playlists: args.playlist_count.max(1),
        max_parts_per_segment: LOCAL_PLAYLIST_PARTS_PER_SEGMENT,
        max_parted_segments: LOCAL_PLAYLIST_SEGMENTS,
        segment_min_ms: args
            .fmp4_segment_ms
            .max(args.nominal_audio_part_ms())
            .max(1),
        target_duration_ms: args.hls_target_duration_ms.max(1_000),
        part_target_us: u32::try_from(args.nominal_audio_part_us()).unwrap_or(u32::MAX),
        buffer_size_kb: args.playlist_buffer_kb.max(1),
        init_size_kb: 5,
        ..Default::default()
    }
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

#[cfg(feature = "srt-ingest")]
fn validate_compiled_ingress_features(_args: &Args) -> Result<()> {
    Ok(())
}

#[cfg(not(feature = "srt-ingest"))]
fn validate_compiled_ingress_features(args: &Args) -> Result<()> {
    if args.srt_bind.is_some() {
        bail!("--srt-bind requires the srt-ingest Cargo feature");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn renditions_that_start_together_number_their_segments_from_one_instant() {
        // Four renditions start within 210 ms, across a two-second boundary
        // of the wall clock (…998 s and …000 s). Times are microseconds.
        let first = 1_795_238_327_990_000_u64;
        let second = shared_video_start(first + 4_000, [first].into_iter());
        let third = shared_video_start(first + 12_000, [first, second].into_iter());
        let fourth = shared_video_start(first + 210_000, [first, second, third].into_iter());
        assert_eq!([second, third, fourth], [first; 3]);
        assert_eq!(first / 2_000_000, fourth / 2_000_000);
        assert_ne!(first / 2_000_000, (first + 210_000) / 2_000_000);
        // A stream that starts a minute later is not a rendition of them.
        let later = first + 60_000_000;
        assert_eq!(shared_video_start(later, [first].into_iter()), later);
        // The first stream of all keeps its own start.
        assert_eq!(shared_video_start(first, std::iter::empty()), first);
    }

    #[test]
    fn a_numeric_rtmp_key_names_the_output_stream() {
        let stream = |key: &str, id: u64| RtmpStreamInfo {
            key: key.to_owned(),
            id,
        };
        // The renditions of a ladder, in any order of arrival.
        assert_eq!(rtmp_output_stream_id(&stream("12", 991), 1, true), 12);
        assert_eq!(rtmp_output_stream_id(&stream("11", 992), 1, false), 11);
        // Any other key keeps the earlier rule.
        assert_eq!(rtmp_output_stream_id(&stream("show", 993), 1, true), 1);
        assert_eq!(rtmp_output_stream_id(&stream("show-b", 994), 1, false), 994);
        assert_eq!(rtmp_output_stream_id(&stream("0", 995), 1, true), 1);
    }
    use av_contrib::hls_stamp::HlsStamp;
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

    fn now_unix_ns() -> i64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|duration| duration.as_nanos().min(i64::MAX as u128) as i64)
            .unwrap_or(0)
    }

    #[tokio::test]
    async fn audio_epoch_normalizer_worker_emits_only_soundkit_v2() {
        let transport = MultichannelAudioTransportAdapter::udp(1_200);
        let fec = transport.prepare_fec_config(MultichannelAudioFecConfig::default());
        let mut source = MultichannelAudioSender::new(MultichannelAudioSessionConfig {
            fec,
            ..MultichannelAudioSessionConfig::default()
        });
        let pcm = vec![0x42; 240 * 2 * 2];
        let framed = av_contrib::soundkit_v2::frame_soundkit_v2_packet(
            av_contrib::soundkit_v2::SoundKitV2PacketMetadata {
                encoding: frame_header::EncodingFlag::PCMSigned,
                frame_count: 240,
                sample_rate: 48_000,
                channels: 2,
                bits_per_sample: 16,
                endianness: frame_header::Endianness::LittleEndian,
                id: Some(4),
                pts: Some(960),
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
        let input = transport
            .wrap_epoch(
                source
                    .encode_epoch(MultichannelAudioEpoch {
                        session_id: 91,
                        config_generation: 1,
                        epoch_id: 4,
                        pts_samples: 960,
                        sample_rate: 48_000,
                        frame_count: 240,
                        groups: &groups,
                    })
                    .unwrap(),
            )
            .unwrap();
        let peer: SocketAddr = "127.0.0.1:41000".parse().unwrap();
        let (input_tx, input_rx) = mpsc::channel(64);
        let (output_tx, mut output_rx) = mpsc::channel(64);
        let (_shutdown_tx, shutdown_rx) = watch::channel(());
        let worker = tokio::spawn(run_audio_epoch_soundkit_normalizer(
            input_rx,
            output_tx,
            shutdown_rx,
        ));
        for datagram in input.datagrams {
            input_tx
                .send(QueuedAudioEpochIngressDatagram {
                    peer,
                    bytes: datagram.payload,
                    enqueued_at: Instant::now(),
                })
                .await
                .unwrap();
        }
        drop(input_tx);
        worker.await.unwrap();

        let mut receiver = music_audio_session::MultichannelAudioReceiver::new(
            MultichannelAudioSessionConfig::default(),
        );
        let mut canonical = None;
        while let Some(datagram) = output_rx.recv().await {
            let payload = transport.payload(&datagram.bytes).unwrap();
            if let Some(epoch) = receiver.push_datagram(payload).unwrap().completed_epoch {
                canonical = Some(epoch);
            }
        }
        let canonical = canonical.expect("normalizer worker emitted a complete epoch");
        assert!(canonical
            .groups
            .iter()
            .all(|group| group.payload_kind == AudioPayloadKind::SoundKitV2));
        let stream =
            av_contrib::soundkit_v2::SoundKitV2StreamRef::parse(&canonical.groups[0].payload, true)
                .unwrap();
        assert_eq!(stream.packets[0].payload, pcm);
    }

    #[tokio::test]
    async fn shared_recovery_gives_the_mesh_the_same_datagrams_as_a_second_decode() {
        let transport = MultichannelAudioTransportAdapter::udp(1_200);
        let fec = transport.prepare_fec_config(MultichannelAudioFecConfig::default());
        let mut source = MultichannelAudioSender::new(MultichannelAudioSessionConfig {
            fec,
            ..MultichannelAudioSessionConfig::default()
        });
        let peer: SocketAddr = "127.0.0.1:41002".parse().unwrap();
        let mut input = Vec::new();
        for epoch_id in 0..4_u64 {
            let pcm = vec![epoch_id as u8 + 1; 48 * 2 * 2];
            let framed = av_contrib::soundkit_v2::frame_soundkit_v2_packet(
                av_contrib::soundkit_v2::SoundKitV2PacketMetadata {
                    encoding: frame_header::EncodingFlag::PCMSigned,
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
            let epoch = source
                .encode_epoch(MultichannelAudioEpoch {
                    session_id: 92,
                    config_generation: 1,
                    epoch_id,
                    pts_samples: epoch_id * 48,
                    sample_rate: 48_000,
                    frame_count: 48,
                    groups: &groups,
                })
                .unwrap();
            input.extend(
                transport
                    .wrap_epoch(epoch)
                    .unwrap()
                    .datagrams
                    .into_iter()
                    .map(|datagram| datagram.payload),
            );
        }

        // Without sharing: the normaliser decodes the datagrams itself.
        let (input_tx, input_rx) = mpsc::channel(256);
        let (output_tx, mut output_rx) = mpsc::channel(256);
        let (_shutdown_tx, shutdown_rx) = watch::channel(());
        let worker = tokio::spawn(run_audio_epoch_soundkit_normalizer(
            input_rx,
            output_tx,
            shutdown_rx,
        ));
        for bytes in &input {
            input_tx
                .send(QueuedAudioEpochIngressDatagram {
                    peer,
                    bytes: bytes.clone(),
                    enqueued_at: Instant::now(),
                })
                .await
                .unwrap();
        }
        drop(input_tx);
        worker.await.unwrap();
        let mut decoded_twice = Vec::new();
        while let Some(datagram) = output_rx.recv().await {
            decoded_twice.push(datagram.bytes);
        }

        // Shared: another receiver (the LL-HLS worker's) recovered them.
        let mut receiver = music_audio_session::MultichannelAudioReceiver::new(
            MultichannelAudioSessionConfig::default(),
        );
        let (recovered_tx, recovered_rx) = mpsc::channel(16);
        let (output_tx, mut output_rx) = mpsc::channel(256);
        let (_shutdown_tx, shutdown_rx) = watch::channel(());
        let worker = tokio::spawn(run_recovered_audio_epoch_normalizer(
            recovered_rx,
            output_tx,
            shutdown_rx,
        ));
        for bytes in &input {
            let payload = transport.payload(bytes).unwrap();
            if let Some(epoch) = receiver.push_datagram(payload).unwrap().completed_epoch {
                recovered_tx
                    .send(RecoveredAudioEpoch {
                        peer,
                        epoch,
                        recovered_at: std::time::Instant::now(),
                    })
                    .await
                    .unwrap();
            }
        }
        drop(recovered_tx);
        worker.await.unwrap();
        let mut decoded_once = Vec::new();
        while let Some(datagram) = output_rx.recv().await {
            assert_eq!(datagram.peer, peer);
            decoded_once.push(datagram.bytes);
        }

        assert!(!decoded_once.is_empty());
        assert_eq!(decoded_once, decoded_twice);
    }

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
            source_epoch: None,
            sequence,
            duration_ms: 67,
            duration_us: 66_733,
            duration_samples: None,
            sample_rate: None,
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
            hls: None,
            source_times: Default::default(),
        }
    }

    fn test_opaque_part(stream_id: u64) -> PublishedOpaquePart {
        PublishedOpaquePart {
            stream_id,
            stream_idx: 0,
            source_epoch: None,
            sequence: 0,
            duration_ms: 5,
            duration_us: 5_000,
            duration_samples: Some(240),
            sample_rate: Some(48_000),
            packaged_at_unix_ns: now_unix_ns(),
            published_at_unix_ns: now_unix_ns(),
            bytes: Bytes::from_static(b"pcm"),
            audio_units: 1,
            hls: None,
            source_times: Default::default(),
        }
    }

    /// A publisher whose shard queues the test reads directly.
    fn test_sharded_publisher(
        mix: bool,
    ) -> (BoundedFmp4Publisher, Vec<mpsc::Receiver<QueuedPublication>>) {
        let mut senders = Vec::new();
        let mut receivers = Vec::new();
        for _ in 0..PUBLICATION_WORKERS {
            let (sender, receiver) = mpsc::channel(16);
            senders.push(sender);
            receivers.push(receiver);
        }
        (
            BoundedFmp4Publisher {
                senders,
                telemetry: Arc::new(IngestTelemetry::default()),
                streams: StreamTable::default(),
                shards: std::sync::Mutex::new(PublicationShards::new(PUBLICATION_WORKERS, mix)),
                durations: StampDurations {
                    video_part_us: 200_000,
                    video_segment_us: 2_000_000,
                    audio_part_us: 5_000,
                    audio_segment_us: 1_000_000,
                },
                inline_audio: None,
            },
            receivers,
        )
    }

    fn drain_shards(
        receivers: &mut [mpsc::Receiver<QueuedPublication>],
    ) -> HashMap<u64, Vec<usize>> {
        let mut shards = HashMap::<u64, Vec<usize>>::new();
        for (shard, receiver) in receivers.iter_mut().enumerate() {
            while let Ok(publication) = receiver.try_recv() {
                shards
                    .entry(publication.stream_id())
                    .or_default()
                    .push(shard);
            }
        }
        shards
    }

    #[test]
    fn part_sizes_default_to_the_smallest_unit_and_keep_the_old_flags() {
        let args = Args::try_parse_from(["av-contrib"]).unwrap();
        assert_eq!(args.audio_part(), AudioPartSize::Epoch);
        assert_eq!(args.video_part(), VideoPartSize::Frame);
        assert_eq!(args.video_part_ms(), 0);
        assert_eq!(
            args.daw_hls_formats
                .iter()
                .copied()
                .map(AudioPayloadKind::from)
                .collect::<Vec<_>>(),
            vec![
                AudioPayloadKind::Flac,
                AudioPayloadKind::Pcm,
                AudioPayloadKind::Opus
            ]
        );
        // PCM 1 ms and 1 ms Opus Custom: what the hop journal is sized for.
        assert_eq!(args.nominal_audio_parts_per_second(), 2_000);
        let durations = args.stamp_durations();
        assert_eq!(durations.audio_part_us, 0);
        assert_eq!(durations.video_part_us, 0);
        assert_eq!(durations.audio_segment_us, 1_000_000);

        // The old millisecond flags are aliases of a duration.
        let args = Args::try_parse_from([
            "av-contrib",
            "--fmp4-part-ms",
            "5",
            "--fmp4-video-part-ms",
            "200",
        ])
        .unwrap();
        assert_eq!(args.audio_part(), AudioPartSize::Micros(5_000));
        assert_eq!(args.video_part(), VideoPartSize::Millis(200));
        assert_eq!(args.stamp_durations().audio_part_us, 5_000);
        assert_eq!(args.stamp_durations().video_part_us, 200_000);
        assert_eq!(args.nominal_audio_part_ms(), 5);

        // The new flags win over the old ones.
        let args = Args::try_parse_from([
            "av-contrib",
            "--fmp4-part-ms",
            "5",
            "--audio-part",
            "120samples",
            "--fmp4-video-part-ms",
            "200",
            "--video-part",
            "frame",
        ])
        .unwrap();
        assert_eq!(args.audio_part(), AudioPartSize::Samples(120));
        assert_eq!(args.video_part(), VideoPartSize::Frame);
        assert_eq!(args.nominal_audio_part_us(), 2_500);
        assert_eq!(args.nominal_audio_part_ms(), 3);
        assert!(args.legacy_audio_part_warning().is_none());
        // 2.5 ms parts of a lossless rendition and of Opus.
        assert_eq!(args.nominal_audio_parts_per_second(), 800);
        let args = Args::try_parse_from(["av-contrib", "--fmp4-part-ms", "50"]).unwrap();
        assert_eq!(args.nominal_audio_parts_per_second(), 40);
        assert!(args
            .legacy_audio_part_warning()
            .is_some_and(|warning| warning.contains("50 ms")));
        assert!(Args::try_parse_from(["av-contrib"])
            .unwrap()
            .legacy_audio_part_warning()
            .is_none());
        assert!(Args::try_parse_from(["av-contrib", "--audio-part", "0us"]).is_err());
        assert!(Args::try_parse_from(["av-contrib", "--video-part", "epoch"]).is_err());
    }

    fn stamping_publisher(
        durations: StampDurations,
    ) -> (BoundedFmp4Publisher, Vec<mpsc::Receiver<QueuedPublication>>) {
        let mut senders = Vec::new();
        let mut receivers = Vec::new();
        for _ in 0..PUBLICATION_WORKERS {
            let (sender, receiver) = mpsc::channel(4_096);
            senders.push(sender);
            receivers.push(receiver);
        }
        (
            BoundedFmp4Publisher {
                senders,
                telemetry: Arc::new(IngestTelemetry::default()),
                streams: StreamTable::default(),
                shards: std::sync::Mutex::new(PublicationShards::new(PUBLICATION_WORKERS, true)),
                durations,
                inline_audio: None,
            },
            receivers,
        )
    }

    fn drained_stamps(receivers: &mut [mpsc::Receiver<QueuedPublication>]) -> Vec<HlsStamp> {
        let mut stamps = Vec::new();
        for receiver in receivers.iter_mut() {
            while let Ok(publication) = receiver.try_recv() {
                let stamp = match publication {
                    QueuedPublication::Fmp4 { part, .. } => part.hls,
                    QueuedPublication::Opaque { part, .. } => part.hls,
                };
                stamps.push(stamp.unwrap());
            }
        }
        stamps
    }

    #[tokio::test]
    async fn epoch_parts_of_two_and_a_half_ms_are_stamped_in_microseconds() {
        let args = Args::try_parse_from(["av-contrib"]).unwrap();
        let (publisher, mut receivers) = stamping_publisher(args.stamp_durations());
        let start_ns = 1_721_000_000_000_000_000_i64;
        for index in 0..801_i64 {
            let mut part = test_opaque_part(3_001);
            part.duration_ms = 3;
            part.duration_us = 2_500;
            part.duration_samples = Some(120);
            part.published_at_unix_ns = start_ns + index * 2_500_000;
            publisher.publish_opaque_part(part).await.unwrap();
        }
        let stamps = drained_stamps(&mut receivers);
        assert_eq!(stamps.len(), 801);
        assert_eq!(stamps[0].msn, 1_721_000_000);
        // 400 parts of 2.5 ms are one second: the segment number keeps pace
        // with the wall clock.
        assert_eq!((stamps[399].msn, stamps[399].part), (1_721_000_000, 399));
        assert_eq!((stamps[400].msn, stamps[400].part), (1_721_000_001, 0));
        assert_eq!((stamps[800].msn, stamps[800].part), (1_721_000_002, 0));
        assert!(stamps.iter().all(|stamp| stamp.part_target_us == 2_500));
        assert!(stamps.iter().all(|stamp| stamp.part_target_ms() == 3));
    }

    #[tokio::test]
    async fn frame_parts_declare_the_longest_frame_as_part_target() {
        let args = Args::try_parse_from(["av-contrib"]).unwrap();
        let (publisher, mut receivers) = stamping_publisher(args.stamp_durations());
        let start_ns = 1_721_000_000_000_000_000_i64;
        let mut durations = Vec::new();
        for index in 0..120_i64 {
            let mut part = test_fmp4_part(
                1,
                0,
                index % 60 == 0,
                1,
                0,
                None,
                Bytes::from_static(b"frame"),
                start_ns + index * 33_366_667,
            );
            // A millisecond source clock: frames of 33 and 34 ms.
            part.duration_us = if index % 3 == 2 { 34_000 } else { 33_000 };
            part.duration_ms = (part.duration_us / 1_000) as u32;
            durations.push(part.duration_us);
            publisher.publish_fmp4_part(part).await.unwrap();
        }
        let stamps = drained_stamps(&mut receivers);
        assert_eq!(stamps.len(), 120);
        // Every part fits under the target the stamp carries, and the target
        // is a frame, not twice a 1 ms setting.
        for (stamp, duration_us) in stamps.iter().zip(&durations) {
            assert!(*duration_us <= stamp.part_target_us);
        }
        assert_eq!(stamps.last().unwrap().part_target_us, 34_000);
        assert_eq!(stamps[60].part, 0);
        assert_eq!(stamps[60].msn, stamps[0].msn + 1);
    }

    /// An ADTS AAC frame of 1,024 samples at 48 kHz (21,333 µs), at 90 kHz
    /// decode time `index * 1,920`.
    fn aac_frame(index: u64) -> access_unit::AccessUnit {
        let payload = [index as u8, 0x22, 0x33, 0x44];
        let mut data = access_unit::aac::create_adts_header(0x66, 2, 48_000, payload.len(), false);
        data.extend_from_slice(&payload);
        access_unit::AccessUnit {
            key: false,
            pts: index * 1_920,
            dts: index * 1_920,
            data: Bytes::from(data),
            stream_type: access_unit::PSI_STREAM_AAC,
            id: index,
        }
    }

    #[tokio::test]
    async fn audio_only_parts_of_a_video_ingest_follow_the_audio_part_size() {
        // The local stack passes --fmp4-part-ms 50. The RTMP/SRT/RIST
        // segmenters are sized by the video part (one frame), and cut one
        // AAC frame of 21 ms per audio-only part, while the stamper declared
        // 50 ms parts: every part was short, and each one started a segment.
        for flags in [
            vec!["av-contrib", "--fmp4-part-ms", "50"],
            vec!["av-contrib", "--audio-part", "epoch"],
            vec!["av-contrib"],
        ] {
            let args = Args::try_parse_from(&flags).unwrap();
            let (publisher, mut receivers) = stamping_publisher(args.stamp_durations());
            let publisher = Arc::new(publisher);
            let (playlists, _, _) = playlists::Playlists::new(playlists::Options::default());
            let mut segmenter = Fmp4Segmenter::new_publish_only(
                9,
                0,
                playlists,
                TimestampInput::Ticks90Khz,
                args.video_part_ms(),
                publisher.clone(),
            );
            segmenter.set_audio_only_min_part_us(args.audio_only_part_us());
            // Three seconds of AAC.
            for index in 0..141 {
                segmenter.push_access_unit(aac_frame(index)).await;
            }
            segmenter.finish().await;
            let stamps = drained_stamps(&mut receivers);
            let segments = stamps
                .iter()
                .map(|stamp| stamp.msn)
                .collect::<HashSet<_>>()
                .len();
            assert!(
                segments <= 4,
                "{flags:?}: {segments} segments of {} parts",
                stamps.len()
            );
            if args.audio_only_part_us() == 0 {
                assert_eq!(stamps.len(), 141, "{flags:?}: one frame per part");
                assert!(stamps.iter().all(|stamp| stamp.part_target_us == 21_333));
            } else {
                // Three frames (64 ms) per part reach 50 ms.
                assert_eq!(stamps.len(), 47, "{flags:?}");
            }
        }
    }

    #[test]
    fn publication_shards_separate_audio_from_video_and_spread_new_streams() {
        let mut shards = PublicationShards::new(PUBLICATION_WORKERS, true);
        let video = shards.assign(1, PublicationClass::Video);
        let flac = shards.assign(2001, PublicationClass::Audio);
        let opus = shards.assign(3001, PublicationClass::Audio);
        let pcm = shards.assign(4001, PublicationClass::Audio);
        let all = [video, flac, opus, pcm];
        for (index, shard) in all.iter().enumerate() {
            assert!(*shard < PUBLICATION_WORKERS);
            assert!(!all[index + 1..].contains(shard), "{all:?} share a worker");
        }
        // However many streams arrive, audio and video never share a worker.
        let audio_pool = shards.audio_pool();
        let video_pool = shards.video_pool();
        for stream_id in 0..64 {
            assert!(audio_pool.contains(&shards.assign(stream_id, PublicationClass::Audio)));
            assert!(video_pool.contains(&shards.assign(stream_id, PublicationClass::Video)));
        }
        assert!(audio_pool.end <= video_pool.start);

        // The old rule puts all four on worker 1.
        let mut modulo = PublicationShards::new(PUBLICATION_WORKERS, false);
        for stream_id in [1, 2001, 3001, 4001] {
            assert_eq!(modulo.assign(stream_id, PublicationClass::Audio), 1);
        }
    }

    #[tokio::test]
    async fn bounded_publication_routes_each_stream_to_one_worker_of_its_pool() {
        let (publisher, mut receivers) = test_sharded_publisher(true);
        for round in 0..3_u64 {
            publisher
                .publish_fmp4_part(test_fmp4_part(
                    1,
                    round,
                    round == 0,
                    1,
                    1,
                    None,
                    Bytes::from_static(b"video"),
                    now_unix_ns(),
                ))
                .await
                .unwrap();
            for stream_id in [2001, 3001, 4001] {
                publisher
                    .publish_opaque_part(test_opaque_part(stream_id))
                    .await
                    .unwrap();
            }
            // An fMP4 audio-only stream (for example AAC) is audio as well.
            publisher
                .publish_fmp4_part(test_fmp4_part(
                    5001,
                    round,
                    true,
                    0,
                    1,
                    None,
                    Bytes::from_static(b"aac"),
                    now_unix_ns(),
                ))
                .await
                .unwrap();
        }
        let routed = drain_shards(&mut receivers);
        let mut per_stream = HashMap::new();
        for (stream_id, shards) in &routed {
            assert_eq!(shards.len(), 3);
            assert!(
                shards.iter().all(|shard| *shard == shards[0]),
                "stream {stream_id} moved between workers: {shards:?}"
            );
            per_stream.insert(*stream_id, shards[0]);
        }
        let video = per_stream[&1];
        let audio = [2001, 3001, 4001].map(|stream_id| per_stream[&stream_id]);
        assert!(!audio.contains(&video));
        assert!(per_stream[&5001] != video);
        assert!(audio[0] != audio[1] && audio[1] != audio[2] && audio[0] != audio[2]);

        let (publisher, mut receivers) = test_sharded_publisher(false);
        publisher
            .publish_opaque_part(test_opaque_part(4001))
            .await
            .unwrap();
        assert_eq!(drain_shards(&mut receivers)[&4001], vec![1]);
    }

    #[tokio::test]
    async fn muxed_stream_that_starts_audio_only_moves_to_the_video_pool_in_order() {
        let (publisher, mut receivers) = test_sharded_publisher(true);
        let audio_pool = PublicationShards::new(PUBLICATION_WORKERS, true).audio_pool();
        let video_pool = PublicationShards::new(PUBLICATION_WORKERS, true).video_pool();
        let part = |sequence: u64, video_units: usize| {
            test_fmp4_part(
                9,
                sequence,
                video_units > 0,
                video_units,
                1,
                None,
                Bytes::from_static(b"muxed"),
                now_unix_ns(),
            )
        };
        let queued = |receivers: &mut [mpsc::Receiver<QueuedPublication>]| {
            let mut queued = Vec::new();
            for (shard, receiver) in receivers.iter_mut().enumerate() {
                while let Ok(publication) = receiver.try_recv() {
                    queued.push((shard, publication));
                }
            }
            queued
        };

        // The demuxer had audio before the first whole video access unit: the
        // first part is audio only and goes to the audio pool.
        publisher.publish_fmp4_part(part(0, 0)).await.unwrap();
        // The first video part arrives while the audio-only part is still
        // queued: it stays on the same worker, behind it, so they leave in
        // order.
        publisher.publish_fmp4_part(part(1, 1)).await.unwrap();
        let held = queued(&mut receivers);
        assert_eq!(held.len(), 2);
        let audio_shard = held[0].0;
        assert!(audio_pool.contains(&audio_shard));
        assert!(held.iter().all(|(shard, _)| *shard == audio_shard));

        // Both still in flight (taken, not yet published): no move.
        publisher.publish_fmp4_part(part(2, 1)).await.unwrap();
        let next = queued(&mut receivers);
        assert_eq!(next.len(), 1);
        assert_eq!(next[0].0, audio_shard);

        // Once the worker has published them, the next video part moves the
        // stream to the video pool, and it stays there, audio-only parts too.
        drop(held);
        drop(next);
        publisher.publish_fmp4_part(part(3, 1)).await.unwrap();
        publisher.publish_fmp4_part(part(4, 0)).await.unwrap();
        publisher.publish_fmp4_part(part(5, 1)).await.unwrap();
        let moved = queued(&mut receivers);
        assert_eq!(moved.len(), 3);
        let video_shard = moved[0].0;
        assert!(video_pool.contains(&video_shard), "{video_shard}");
        assert!(moved.iter().all(|(shard, _)| *shard == video_shard));
        assert_eq!(
            moved
                .iter()
                .map(|(_, publication)| publication.sequence())
                .collect::<Vec<_>>(),
            vec![3, 4, 5]
        );
    }

    /// The process journal, installed for tests that read it.
    fn test_hop_journal() -> &'static HopJournal {
        hop_journal::install(HopJournal::new("contrib-test", 1 << 16))
    }

    #[tokio::test]
    async fn publication_journals_each_part_from_source_to_worker() {
        let journal = test_hop_journal();
        // The source and packaging times precede the hand-off.
        let before_us = hop_journal::now_unix_us() - 100;
        let (publisher, mut receivers) = test_sharded_publisher(true);
        // A stream id of this test only: other tests share the process journal.
        let stream_id = 4_000_000_041;
        let mut part = test_opaque_part(stream_id);
        part.packaged_at_unix_ns = i64::try_from((before_us + 70) * 1_000).unwrap();
        // A part of two AEP1 epochs, 1,234 and 1,235.
        part.source_times = hop_journal::PartSourceTimes {
            first_epoch: Some(1_234),
            last_epoch: Some(1_235),
            src_first_us: Some(before_us + 10),
            src_last_us: Some(before_us + 20),
            recovered_us: Some(before_us + 25),
        };
        publisher.publish_opaque_part(part.clone()).await.unwrap();
        publisher.publish_opaque_part(part).await.unwrap();

        // The worker side: take both parts as a publication worker would.
        let inner: Arc<dyn Fmp4PartPublisher> = Arc::new(BlockingPublicationSink {
            blocked: std::sync::atomic::AtomicBool::new(true),
            started: tokio::sync::Notify::new(),
            release: tokio::sync::Notify::new(),
        });
        let telemetry = IngestTelemetry::default();
        for receiver in receivers.iter_mut() {
            while let Ok(publication) = receiver.try_recv() {
                process_queued_publication(
                    0,
                    &inner,
                    &telemetry,
                    &mut HashMap::new(),
                    &mut std::collections::HashSet::new(),
                    publication,
                )
                .await;
            }
        }

        let events = journal.query(Some(before_us), Some(stream_id));
        let stages_of = |seq: u64| {
            events
                .iter()
                .filter(|event| event.seq == seq)
                .map(|event| event.stage)
                .collect::<Vec<_>>()
        };
        for seq in [0, 1] {
            assert_eq!(
                stages_of(seq),
                vec![
                    HopStage::SrcFirst,
                    HopStage::SrcLast,
                    HopStage::Recovered,
                    HopStage::Packaged,
                    HopStage::Enqueued,
                    HopStage::Dequeued,
                ],
                "{events:?}"
            );
        }
        let first = events.iter().find(|event| event.seq == 0).unwrap();
        assert_eq!(first.t_us, before_us + 10);
        assert_eq!(first.epoch, Some(1_234));
        // The stages after src_first name the part's last epoch, whose last
        // sample ends the part.
        for stage in [HopStage::SrcLast, HopStage::Recovered, HopStage::Packaged] {
            let event = events
                .iter()
                .find(|event| event.seq == 0 && event.stage == stage)
                .unwrap();
            assert_eq!(event.epoch, Some(1_235), "{stage:?}");
        }
        let packaged = events
            .iter()
            .find(|event| event.stage == HopStage::Packaged)
            .unwrap();
        assert_eq!(packaged.t_us, before_us + 70);

        // GET /api/hops serves the same events as NDJSON, oldest first.
        let response = hop_journal_response(
            Some(journal),
            Some(&format!("since_us={before_us}&stream={stream_id}")),
        )
        .await;
        assert_eq!(response.status, StatusCode::OK);
        assert_eq!(
            response.content_type.as_deref(),
            Some(HOP_JOURNAL_CONTENT_TYPE)
        );
        let body = String::from_utf8(response.body.unwrap().to_vec()).unwrap();
        assert_eq!(body.lines().count(), events.len());
        let first_line: serde_json::Value =
            serde_json::from_str(body.lines().next().unwrap()).unwrap();
        assert_eq!(first_line["node"], "contrib-test");
        assert_eq!(first_line["stage"], "src_first");
        assert_eq!(first_line["stream"], stream_id);
        assert_eq!(first_line["seq"], 0);
        assert_eq!(first_line["epoch"], 1_234);
        assert_eq!(
            hop_journal_response(Some(journal), Some("since_us=x"))
                .await
                .status,
            StatusCode::BAD_REQUEST
        );
        assert_eq!(
            hop_journal_response(None, None).await.status,
            StatusCode::NOT_FOUND
        );
    }

    struct BlockingPublicationSink {
        blocked: std::sync::atomic::AtomicBool,
        started: tokio::sync::Notify,
        release: tokio::sync::Notify,
    }

    #[async_trait::async_trait]
    impl Fmp4PartPublisher for BlockingPublicationSink {
        async fn publish_fmp4_part(
            &self,
            _part: PublishedFmp4Part,
        ) -> std::result::Result<(), String> {
            if !self.blocked.swap(true, Ordering::AcqRel) {
                self.started.notify_one();
                self.release.notified().await;
            }
            Ok(())
        }
    }

    #[tokio::test]
    async fn bounded_publication_pipeline_keeps_packaging_nonblocking_and_reports_overflow() {
        let sink = Arc::new(BlockingPublicationSink {
            blocked: std::sync::atomic::AtomicBool::new(false),
            started: tokio::sync::Notify::new(),
            release: tokio::sync::Notify::new(),
        });
        let (shutdown_tx, shutdown_rx) = watch::channel(());
        let inner: Arc<dyn Fmp4PartPublisher> = sink.clone();
        let telemetry = Arc::new(IngestTelemetry::default());
        let (publisher, tasks) = start_bounded_publication_pipeline(
            inner,
            telemetry,
            StampDurations {
                video_part_us: 200_000,
                video_segment_us: 2_000_000,
                audio_part_us: 5_000,
                audio_segment_us: 1_000_000,
            },
            true,
            false,
            shutdown_rx,
        );
        let first = test_fmp4_part(
            91,
            0,
            false,
            0,
            1,
            Some(Bytes::from_static(b"init")),
            Bytes::from_static(b"part-0"),
            now_unix_ns(),
        );
        timeout(
            Duration::from_millis(50),
            publisher.publish_fmp4_part(first),
        )
        .await
        .expect("publication handoff blocked packaging")
        .unwrap();
        timeout(Duration::from_secs(1), sink.started.notified())
            .await
            .expect("publication worker did not start");

        for sequence in 1..=PUBLICATION_QUEUE_CAPACITY_PER_WORKER as u64 {
            publisher
                .publish_fmp4_part(test_fmp4_part(
                    91,
                    sequence,
                    false,
                    0,
                    1,
                    None,
                    Bytes::from_static(b"part"),
                    now_unix_ns(),
                ))
                .await
                .unwrap();
        }
        let overflow = publisher
            .publish_fmp4_part(test_fmp4_part(
                91,
                999,
                false,
                0,
                1,
                None,
                Bytes::from_static(b"overflow"),
                now_unix_ns(),
            ))
            .await
            .unwrap_err();
        assert_eq!(overflow, "publication queue is full");

        sink.release.notify_one();
        let _ = shutdown_tx.send(());
        for task in tasks {
            timeout(Duration::from_secs(1), task)
                .await
                .expect("publication worker did not drain")
                .unwrap();
        }
    }

    #[test]
    fn fixed_upload_output_policy_preserves_the_configured_stream_across_rollovers() {
        for request_stream_id in [1, 2, 3] {
            assert_eq!(
                select_upload_ts_output_stream_id(
                    UploadTsOutputPolicy::FixedConfigured,
                    1,
                    request_stream_id,
                    false,
                ),
                1
            );
            assert_eq!(
                select_upload_ts_output_stream_id(
                    UploadTsOutputPolicy::FixedConfigured,
                    1,
                    request_stream_id,
                    true,
                ),
                1
            );
        }
    }

    #[test]
    fn concurrent_upload_output_policy_uses_request_identity_only_during_overlap() {
        assert_eq!(
            select_upload_ts_output_stream_id(
                UploadTsOutputPolicy::ConcurrentFallback,
                6,
                41,
                false,
            ),
            6
        );
        assert_eq!(
            select_upload_ts_output_stream_id(
                UploadTsOutputPolicy::ConcurrentFallback,
                6,
                42,
                true,
            ),
            42
        );
    }

    #[test]
    fn fixed_output_is_not_finalized_while_a_successor_is_active() {
        let state = |output_stream_id, ended| UploadTsBridgeState {
            output_stream_id,
            output_stream_idx: output_stream_id.map(|id| id as usize),
            last_seen: 0,
            reader_registered: false,
            body_slots: 0,
            ended,
            bridge: None,
        };
        let mut bridges = HashMap::from([(2, state(Some(1), false)), (3, state(Some(3), false))]);

        assert!(upload_output_has_active_successor(&bridges, 1, 1));
        bridges.get_mut(&2).unwrap().ended = true;
        assert!(!upload_output_has_active_successor(&bridges, 1, 1));
    }

    #[tokio::test]
    async fn upload_response_completes_only_after_the_end_reader_position_advances() {
        let service = Arc::new(UploadResponseService::new(UploadResponseConfig {
            num_streams: 1,
            slot_size_kb: 1,
            slots_per_stream: 8,
            response_timeout_ms: 1_000,
        }));
        let stream = service.open_stream().await.unwrap();
        let stream_id = stream.stream_id();
        let mut response_rx = service.register_response(stream_id).await;
        assert!(
            service
                .register_request_reader(stream_id, UPLOAD_RESPONSE_HLS_WORKER_ID)
                .await
        );

        assert!(advance_upload_request_reader(service.as_ref(), stream_id, 1, false).await);
        assert!(timeout(Duration::from_millis(10), &mut response_rx)
            .await
            .is_err());
        assert!(advance_upload_request_reader(service.as_ref(), stream_id, 2, true).await);

        let response = timeout(Duration::from_secs(1), response_rx)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(response.status, StatusCode::NO_CONTENT);
        assert!(response.body.is_empty());
        assert!(response.headers.is_empty());
        stream.close().await;
    }

    #[test]
    fn the_ingest_ring_of_a_live_stream_fits_a_small_node() {
        let config = upload_response_config();
        let bytes_per_stream = config.slots_per_stream * config.slot_size_kb * 1024;
        // Every stream of the listener at once stays far below 1 GB.
        assert!(bytes_per_stream <= 16 * 1024 * 1024, "{bytes_per_stream}");
        assert!(bytes_per_stream * config.num_streams <= 128 * 1024 * 1024);
        // The ring still holds two seconds of a 40 Mbit/s stream for the
        // bridge, which reads it every few milliseconds.
        assert!(bytes_per_stream >= 2 * 40_000_000 / 8);
    }

    #[test]
    fn the_unused_local_playlist_caches_stay_small_whatever_the_part() {
        // The caches are never written (every segmenter publishes only), so
        // their size does not follow the part: 1 ms parts sized them at
        // 2.1M slots each.
        for flags in [
            vec!["av-contrib"],
            vec!["av-contrib", "--audio-part", "12samples"],
            vec!["av-contrib", "--fmp4-part-ms", "50"],
        ] {
            let args = Args::try_parse_from(&flags).unwrap();
            let options = playlist_options(&args);
            let slots =
                options.num_playlists * options.max_segments * options.max_parts_per_segment;
            assert!(slots <= 65 * 64, "{flags:?}: {slots} slots");
            assert_eq!(options.num_playlists, args.playlist_count);
        }
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
    fn needletail_ingest_query_is_exact_and_scoped() {
        assert_eq!(
            needletail_ingest_query(Some("sessionId=ses_1&endpointId=end_nexus")).unwrap(),
            ("ses_1".to_owned(), "end_nexus".to_owned())
        );
        assert!(needletail_ingest_query(None).is_err());
        assert!(needletail_ingest_query(Some("sessionId=ses_1")).is_err());
        assert!(
            needletail_ingest_query(Some("sessionId=ses_1&endpointId=end_nexus&extra=1")).is_err()
        );
        assert!(needletail_ingest_query(Some(
            "sessionId=ses_1&sessionId=ses_2&endpointId=end_nexus"
        ))
        .is_err());
        assert!(needletail_ingest_query(Some("sessionId=../ses&endpointId=end_nexus")).is_err());
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
    fn canonical_opaque_object_preserves_arbitrary_bytes_without_format_metadata() {
        const PACKAGED_NS: i64 = 1_721_000_000_298_000_000;
        const PUBLISHED_NS: i64 = 1_721_000_000_300_000_000;
        let payload = Bytes::from_static(b"not-soundkit-not-pcm-not-a-codec");
        let part = PublishedOpaquePart {
            stream_id: 91,
            stream_idx: 91,
            source_epoch: None,
            sequence: 17,
            duration_ms: 3,
            duration_us: 2_500,
            duration_samples: Some(120),
            sample_rate: Some(48_000),
            packaged_at_unix_ns: PACKAGED_NS,
            published_at_unix_ns: PUBLISHED_NS,
            bytes: payload.clone(),
            audio_units: 1,
            hls: Some(HlsStamp {
                msn: 41,
                part: 7,
                part_target_us: 2_500,
                target_duration_us: 1_000_000,
            }),
            source_times: Default::default(),
        };
        let object = build_opaque_media_object(&part, TEST_SOURCE_EPOCH, 1_000, 250_000).unwrap();
        // The place of the part in the playlist travels with the object.
        assert_eq!(object.metadata().get("hls-msn").unwrap(), b"41");
        assert_eq!(object.metadata().get("hls-part").unwrap(), b"7");
        // Old caches read whole milliseconds (the part target rounded up so
        // that the part fits); new ones the exact microseconds and samples.
        assert_eq!(object.metadata().get("hls-part-target-ms").unwrap(), b"3");
        assert_eq!(
            object.metadata().get("hls-part-target-us").unwrap(),
            b"2500"
        );
        assert_eq!(
            object.metadata().get("hls-target-duration-ms").unwrap(),
            b"1000"
        );
        assert_eq!(
            object.metadata().get("hls-target-duration-us").unwrap(),
            b"1000000"
        );
        assert_eq!(object.metadata().get("duration-ms").unwrap(), b"3");
        assert_eq!(object.metadata().get("duration-us").unwrap(), b"2500");
        assert_eq!(object.metadata().get("duration-samples").unwrap(), b"120");
        assert_eq!(object.metadata().get("sample-rate").unwrap(), b"48000");

        assert_eq!(object.key().track(), OPAQUE_AUDIO_TRACK);
        assert_eq!(object.kind(), ObjectKind::Media);
        assert_eq!(object.payload(), payload.as_ref());
        assert!(object.dependencies().is_empty());
        assert_eq!(object.metadata().get("container").unwrap(), b"opaque");
        assert_eq!(
            object.metadata().get("payload-format").unwrap(),
            b"opaque-part-v1"
        );
        assert_eq!(
            object.metadata().get("content-type").unwrap(),
            b"application/octet-stream"
        );
        assert_eq!(object.metadata().get("file-extension").unwrap(), b"bin");
        assert!(!object.metadata().contains_key("audio-codec"));
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

    /// Not a check, a benchmark: thread CPU per part of the contributor's
    /// publication of an opaque audio part (canonical media object, its
    /// encoding, and the RelaySession FEC encode and sends of
    /// `publish_part_object`) for the DAW lane's Opus parts of 1, 2.5 and
    /// 5 ms (one 320 kb/s stereo SoundKit Opus packet each) and 1 ms PCM,
    /// to one lane and to a primary and a secondary lane. Run with
    /// `cargo test --release --bin av-contrib publication_cpu_per_part -- --ignored --nocapture`.
    #[tokio::test(flavor = "current_thread")]
    #[ignore = "benchmark"]
    async fn publication_cpu_per_part_at_each_opus_frame() {
        const PARTS: u64 = 4_000;
        for secondary in [false, true] {
            let primary_receiver = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
            let secondary_receiver = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
            let mut args = contrib_status_args();
            args.relay_primary_target = Some(primary_receiver.local_addr().unwrap());
            if secondary {
                args.relay_secondary_target = Some(secondary_receiver.local_addr().unwrap());
                args.relay_secondary_seed_source = true;
            }
            args.relay_send_batch = true;
            args.relay_class_lanes = false;
            args.relay_repair_both_lanes = false;
            args.relay_deadline_ms = 5_000;
            let telemetry = Arc::new(IngestTelemetry::default());
            let relay = RelaySessionPublisher::new(&args, Arc::clone(&telemetry))
                .await
                .unwrap()
                .unwrap();
            for (label, samples, bytes) in [
                // 16-byte SoundKit v2 header and the 320 kb/s packet.
                ("opus 1 ms", 48_u64, 16 + 40),
                ("opus 2.5 ms", 120, 16 + 100),
                ("opus 5 ms", 240, 16 + 200),
                ("pcm 1 ms", 48, 16 + 288),
            ] {
                let payload = Bytes::from(vec![0x5a_u8; bytes]);
                let mut object_ns = 0;
                let mut encode_ns = 0;
                let mut relay_ns = 0;
                let mut symbols = 0;
                for sequence in 0..PARTS {
                    let mut part = test_opaque_part(91_000 + samples);
                    part.sequence = sequence;
                    part.duration_samples = Some(samples);
                    part.duration_us = samples * 1_000_000 / 48_000;
                    part.duration_ms = (part.duration_us / 1_000).max(1) as u32;
                    part.bytes = payload.clone();
                    let begin = thread_cpu_ns();
                    let object =
                        build_opaque_media_object(&part, TEST_SOURCE_EPOCH, 5_000, 1_000_000)
                            .unwrap();
                    let built = thread_cpu_ns();
                    let encoded = encode_canonical_media_object(&object).unwrap();
                    let encoded_at = thread_cpu_ns();
                    std::hint::black_box(&encoded);
                    let outcome = relay
                        .publish_part_object(&object, None, PublicationClass::Audio)
                        .await
                        .unwrap();
                    let sent = thread_cpu_ns();
                    object_ns += built - begin;
                    encode_ns += encoded_at - built;
                    relay_ns += sent - encoded_at;
                    symbols += (outcome.source_symbols + outcome.repair_symbols) as u64;
                }
                let per_part = (object_ns + encode_ns + relay_ns) / PARTS;
                let per_second = 48_000 / samples;
                eprintln!(
                    "{} lane(s), {label}: object {} ns, canonical encode {} ns, RelaySession \
                     {} ns ({:.1} symbols) = {} ns/part; {:.1} ms CPU per second of audio \
                     at {per_second} parts/s",
                    1 + usize::from(secondary),
                    object_ns / PARTS,
                    encode_ns / PARTS,
                    relay_ns / PARTS,
                    symbols as f64 / PARTS as f64,
                    per_part,
                    (per_part * per_second) as f64 / 1e6,
                );
            }
        }
    }

    #[tokio::test]
    async fn relay_session_routes_source_and_secondary_repair_and_recovers_exact_object() {
        relay_session_routes_source_and_secondary_repair(
            true,
            true,
            true,
            DEFAULT_RELAY_VIDEO_GSO_PACE_MBPS,
        )
        .await;
    }

    /// `--relay-source-first false`: relay-session's encoder, source and
    /// repair together; the receivers see the same.
    #[tokio::test]
    async fn relay_session_without_source_first_sends_the_same_symbols() {
        relay_session_routes_source_and_secondary_repair(
            true,
            true,
            false,
            DEFAULT_RELAY_VIDEO_GSO_PACE_MBPS,
        )
        .await;
    }

    #[tokio::test]
    async fn relay_session_without_send_batching_sends_the_same_symbols() {
        relay_session_routes_source_and_secondary_repair(
            false,
            true,
            true,
            DEFAULT_RELAY_VIDEO_GSO_PACE_MBPS,
        )
        .await;
    }

    /// `--relay-video-gso false`: the video lanes send one datagram per
    /// symbol, and the receivers see exactly what they see with GSO.
    #[tokio::test]
    async fn relay_session_video_without_gso_sends_the_same_symbols() {
        relay_session_routes_source_and_secondary_repair(
            true,
            false,
            true,
            DEFAULT_RELAY_VIDEO_GSO_PACE_MBPS,
        )
        .await;
    }

    /// `--relay-video-gso-pace-mbps 0`: GSO messages handed over as fast as
    /// the kernel takes them, from the publishing task; the same symbols.
    #[tokio::test]
    async fn relay_session_video_with_unpaced_gso_sends_the_same_symbols() {
        relay_session_routes_source_and_secondary_repair(true, true, true, 0).await;
    }

    /// Off by default until a lab A/B shows a gain; runs of 16 by default,
    /// 1-64 accepted.
    #[test]
    fn the_relay_video_gso_flag_defaults_off_and_can_be_turned_on() {
        let parsed = Args::try_parse_from(["av-contrib"]).unwrap();
        assert!(!parsed.relay_video_gso);
        assert_eq!(parsed.relay_video_gso_segments, 16);
        assert_eq!(parsed.relay_video_gso_pace_mbps, 2_000);
        let parsed = Args::try_parse_from([
            "av-contrib",
            "--relay-video-gso",
            "true",
            "--relay-video-gso-segments",
            "64",
            "--relay-video-gso-pace-mbps",
            "0",
        ])
        .unwrap();
        assert!(parsed.relay_video_gso);
        assert_eq!(parsed.relay_video_gso_segments, 64);
        assert_eq!(parsed.relay_video_gso_pace_mbps, 0);
        for refused in ["0", "65"] {
            assert!(
                Args::try_parse_from(["av-contrib", "--relay-video-gso-segments", refused])
                    .is_err()
            );
        }
    }

    /// A paced video socket gets GSO messages of about one millisecond at
    /// its pacing rate, so `fq` spaces bursts of a few symbols rather than
    /// of a whole run; unpaced lanes keep the segment cap.
    #[tokio::test]
    async fn video_gso_messages_are_one_millisecond_of_the_pacing_rate() {
        let primary_receiver = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let reservation = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
        let bind = reservation.local_addr().unwrap();
        drop(reservation);
        for (class_lanes, pacing_mbps, expected) in [
            (
                true,
                40,
                udp_batch::GsoLimits {
                    segments: 16,
                    bytes: 5_000,
                },
            ),
            (
                true,
                0,
                udp_batch::GsoLimits {
                    segments: 16,
                    bytes: udp_batch::GSO_MAX_BYTES,
                },
            ),
            // Without class lanes the video socket is not paced.
            (
                false,
                40,
                udp_batch::GsoLimits {
                    segments: 16,
                    bytes: udp_batch::GSO_MAX_BYTES,
                },
            ),
        ] {
            let mut args = contrib_status_args();
            args.relay_primary_target = Some(primary_receiver.local_addr().unwrap());
            args.relay_primary_bind = Some(bind);
            args.relay_send_batch = true;
            args.relay_video_gso = true;
            // The fq pacing alone (see the paced GSO send for its own).
            args.relay_video_gso_pace_mbps = 0;
            args.relay_class_lanes = class_lanes;
            args.relay_video_pacing_mbps = pacing_mbps;
            let relay = RelaySessionPublisher::new(&args, Arc::new(IngestTelemetry::default()))
                .await
                .unwrap()
                .unwrap();
            assert_eq!(
                relay.video_gso_limits, expected,
                "{class_lanes} {pacing_mbps}"
            );
        }
        // Paced GSO (the default 2,000 Mbit/s): messages of 50 us of it,
        // 12,500 bytes, under the fq pacing's millisecond as well.
        for (pacing_mbps, bytes) in [(0, 12_500), (40, 5_000)] {
            let mut args = contrib_status_args();
            args.relay_primary_target = Some(primary_receiver.local_addr().unwrap());
            args.relay_primary_bind = Some(reserved_loopback_addr());
            args.relay_video_gso = true;
            args.relay_video_pacing_mbps = pacing_mbps;
            let relay = RelaySessionPublisher::new(&args, Arc::new(IngestTelemetry::default()))
                .await
                .unwrap()
                .unwrap();
            assert_eq!(
                relay.video_gso_limits,
                udp_batch::GsoLimits {
                    segments: 16,
                    bytes
                },
                "{pacing_mbps}"
            );
            assert_eq!(relay.video_gso_pace_bytes_per_second, 250_000_000);
        }
    }

    fn reserved_loopback_addr() -> SocketAddr {
        let reservation = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
        reservation.local_addr().unwrap()
    }

    async fn receive_relay_datagrams(
        socket: &UdpSocket,
        count: usize,
    ) -> Vec<(relay_session::RelayDatagram, SocketAddr)> {
        let limits = RelayLimits::default();
        let mut wire = vec![0u8; limits.max_datagram_bytes];
        let mut out = Vec::with_capacity(count);
        for _ in 0..count {
            let (len, from) = timeout(Duration::from_secs(3), socket.recv_from(&mut wire))
                .await
                .expect("RelaySession datagram timeout")
                .expect("RelaySession UDP receive");
            out.push((
                relay_session::decode_datagram(&wire[..len], limits).unwrap(),
                from,
            ));
        }
        out
    }

    #[tokio::test]
    async fn direct_lanes_give_each_regional_distributor_the_primary_source_set() {
        for (send_batch, direct_repair) in [(true, false), (false, false), (true, true)] {
            let primary_receiver = UdpSocket::bind("127.0.0.1:0").await.unwrap();
            let secondary_receiver = UdpSocket::bind("127.0.0.1:0").await.unwrap();
            let distributors = [
                UdpSocket::bind("127.0.0.1:0").await.unwrap(),
                UdpSocket::bind("127.0.0.1:0").await.unwrap(),
            ];
            let mut args = contrib_status_args();
            args.relay_primary_target = Some(primary_receiver.local_addr().unwrap());
            args.relay_primary_bind = Some(reserved_loopback_addr());
            args.relay_secondary_target = Some(secondary_receiver.local_addr().unwrap());
            args.relay_secondary_bind = Some(reserved_loopback_addr());
            args.relay_direct_targets = distributors
                .iter()
                .map(|socket| socket.local_addr().unwrap())
                .collect();
            let direct_bind = reserved_loopback_addr();
            args.relay_direct_bind = Some(direct_bind);
            args.relay_direct_ids = vec!["lon-dist-a".into(), "lon-dist-b".into()];
            args.relay_direct_repair = direct_repair;
            args.relay_send_batch = send_batch;
            args.relay_deadline_ms = 5_000;
            args.relay_min_repair_symbols = 2;
            let telemetry = Arc::new(IngestTelemetry::default());
            let relay = RelaySessionPublisher::new(&args, Arc::clone(&telemetry))
                .await
                .unwrap()
                .unwrap();
            assert_eq!(
                relay.direct.as_ref().unwrap().local_addr().unwrap(),
                direct_bind
            );

            let part = test_opaque_part(4_000_000_090 + u64::from(send_batch));
            let object =
                build_opaque_media_object(&part, TEST_SOURCE_EPOCH, args.relay_deadline_ms, 1_000)
                    .unwrap();
            let journal = test_hop_journal();
            let before_us = hop_journal::now_unix_us();
            let journal_stream =
                88_000_000_000 + u64::from(send_batch) * 2 + u64::from(direct_repair);
            let outcome = relay
                .publish_part_object(&object, Some((journal_stream, 5)), PublicationClass::Audio)
                .await
                .unwrap();
            assert!(outcome.repair_symbols >= 2);

            let primary = receive_relay_datagrams(&primary_receiver, outcome.source_symbols).await;
            let per_distributor = if direct_repair {
                outcome.source_symbols + outcome.repair_symbols
            } else {
                outcome.source_symbols
            };
            for distributor in &distributors {
                let direct = receive_relay_datagrams(distributor, per_distributor).await;
                // The same symbols as the primary's, from the one direct
                // source endpoint that the distributor registers.
                assert!(direct.iter().all(|(_, from)| *from == direct_bind));
                for ((symbol, _), (primary, _)) in direct.iter().zip(&primary) {
                    assert_eq!(symbol, primary);
                    assert_eq!(
                        symbol.path_intent,
                        relay_session::MediaPathIntent::PrimarySource
                    );
                }
                let repairs = direct
                    .iter()
                    .filter(|(symbol, _)| symbol.role == MediaDatagramRole::Repair)
                    .count();
                assert_eq!(repairs, per_distributor - outcome.source_symbols);
                // Nothing more arrives: no repair unless asked for.
                let mut wire = [0_u8; 2048];
                assert!(
                    timeout(Duration::from_millis(50), distributor.recv_from(&mut wire))
                        .await
                        .is_err()
                );
            }

            let direct_events = journal
                .query(Some(before_us), Some(journal_stream))
                .into_iter()
                .filter(|event| event.stage == HopStage::SentDirect)
                .map(|event| event.peer)
                .collect::<Vec<_>>();
            assert_eq!(
                direct_events,
                args.relay_direct_targets
                    .iter()
                    .copied()
                    .map(Some)
                    .collect::<Vec<_>>()
            );
            let snapshot = telemetry.snapshot();
            assert_eq!(snapshot.relay_session.direct_lane_objects_succeeded, 2);
            assert_eq!(snapshot.relay_session.direct_lane_objects_failed, 0);
            assert_eq!(
                snapshot.relay_session.direct_datagrams,
                (per_distributor * 2) as u64
            );
            // The canonical lane counters are the backbone's only.
            assert_eq!(snapshot.relay_session.primary_lane_objects_succeeded, 1);
            assert_eq!(
                snapshot.relay_session.source_datagrams,
                outcome.source_symbols as u64
            );
            let status = ContribStatusConfig::from_args(&args, Arc::clone(&telemetry)).snapshot();
            assert_eq!(status.mesh.relay_direct_ids, ["lon-dist-a", "lon-dist-b"]);
            assert_eq!(status.mesh.relay_direct_targets.len(), 2);
        }
    }

    #[tokio::test]
    async fn a_refused_direct_target_leaves_the_other_distributors_their_symbols() {
        for send_batch in [true, false] {
            let primary_receiver = UdpSocket::bind("127.0.0.1:0").await.unwrap();
            let distributor = UdpSocket::bind("127.0.0.1:0").await.unwrap();
            let mut args = contrib_status_args();
            args.relay_primary_target = Some(primary_receiver.local_addr().unwrap());
            args.relay_primary_bind = Some(reserved_loopback_addr());
            args.relay_direct_targets = vec![distributor.local_addr().unwrap()];
            args.relay_direct_bind = Some(reserved_loopback_addr());
            args.relay_send_batch = send_batch;
            args.relay_deadline_ms = 5_000;
            let telemetry = Arc::new(IngestTelemetry::default());
            let mut relay = RelaySessionPublisher::new(&args, Arc::clone(&telemetry))
                .await
                .unwrap()
                .unwrap();
            // A distributor that the kernel refuses at once (port 0 is
            // EINVAL, as no route is ENETUNREACH), listed first.
            let refused: SocketAddr = "127.0.0.1:0".parse().unwrap();
            relay.direct.as_mut().unwrap().targets.insert(0, refused);

            let part = test_opaque_part(4_000_000_190 + u64::from(send_batch));
            let object =
                build_opaque_media_object(&part, TEST_SOURCE_EPOCH, args.relay_deadline_ms, 1_000)
                    .unwrap();
            let journal = test_hop_journal();
            let before_us = hop_journal::now_unix_us();
            let journal_stream = 88_100_000_000 + u64::from(send_batch);
            let outcome = relay
                .publish_part_object(&object, Some((journal_stream, 9)), PublicationClass::Audio)
                .await
                .unwrap();
            let primary = receive_relay_datagrams(&primary_receiver, outcome.source_symbols).await;
            let direct = receive_relay_datagrams(&distributor, outcome.source_symbols).await;
            for ((symbol, _), (primary, _)) in direct.iter().zip(&primary) {
                assert_eq!(symbol, primary);
            }
            let direct_events = journal
                .query(Some(before_us), Some(journal_stream))
                .into_iter()
                .filter(|event| event.stage == HopStage::SentDirect)
                .map(|event| event.peer)
                .collect::<Vec<_>>();
            assert_eq!(direct_events, vec![Some(distributor.local_addr().unwrap())]);
            let snapshot = telemetry.snapshot();
            assert_eq!(snapshot.relay_session.direct_lane_objects_succeeded, 1);
            assert_eq!(snapshot.relay_session.direct_lane_objects_failed, 1);
            assert_eq!(
                snapshot.relay_session.direct_datagrams,
                outcome.source_symbols as u64
            );
            assert_eq!(snapshot.relay_session.primary_lane_objects_succeeded, 1);
        }
    }

    #[tokio::test]
    async fn direct_lane_settings_are_checked() {
        let primary = reserved_loopback_addr();
        let distributor = reserved_loopback_addr();
        let telemetry = Arc::new(IngestTelemetry::default());

        // Without a primary there is no RelaySession at all.
        let mut args = contrib_status_args();
        args.relay_direct_targets = vec![distributor];
        assert!(RelaySessionPublisher::new(&args, Arc::clone(&telemetry))
            .await
            .is_err());

        // One id for each target.
        let mut args = contrib_status_args();
        args.relay_primary_target = Some(primary);
        args.relay_direct_targets = vec![distributor];
        args.relay_direct_ids = vec!["a".into(), "b".into()];
        let error = RelaySessionPublisher::new(&args, Arc::clone(&telemetry))
            .await
            .err()
            .unwrap();
        assert!(format!("{error:#}").contains("once for each"), "{error:#}");

        // A direct target is not a backbone parent.
        let mut args = contrib_status_args();
        args.relay_primary_target = Some(primary);
        args.relay_direct_targets = vec![primary];
        assert!(RelaySessionPublisher::new(&args, Arc::clone(&telemetry))
            .await
            .is_err());

        // A bind or ids without a target.
        let mut args = contrib_status_args();
        args.relay_primary_target = Some(primary);
        args.relay_direct_bind = Some(reserved_loopback_addr());
        assert!(RelaySessionPublisher::new(&args, Arc::clone(&telemetry))
            .await
            .is_err());

        // Repeated --relay-direct-target and the env list both parse.
        let parsed = Args::try_parse_from([
            "av-contrib",
            "--relay-direct-target",
            "10.77.1.11:22200",
            "--relay-direct-target",
            "10.77.1.12:22210",
            "--relay-direct-id",
            "lon-dist-a,lon-dist-b",
        ])
        .unwrap();
        assert_eq!(parsed.relay_direct_targets.len(), 2);
        assert_eq!(parsed.relay_direct_ids, ["lon-dist-a", "lon-dist-b"]);
        assert!(!parsed.relay_direct_repair);
        assert!(parsed.relay_send_batch);
        assert!(parsed.aep1_shared_recovery);
    }

    /// With `--relay-send-batch` and without it, each lane carries the same
    /// symbols in the same order; only the number of send calls differs.
    async fn relay_session_routes_source_and_secondary_repair(
        send_batch: bool,
        video_gso: bool,
        source_first: bool,
        gso_pace_mbps: u64,
    ) {
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
        args.relay_send_batch = send_batch;
        args.relay_video_gso = video_gso;
        args.relay_video_gso_segments = 16;
        args.relay_video_gso_pace_mbps = gso_pace_mbps;
        args.relay_source_first = source_first;
        // Source first needs the batched sends.
        let source_first = source_first && send_batch;
        // Today's lanes: one socket per lane, no repair on the primary.
        // The class lanes and repair on both lanes have their own tests.
        args.relay_class_lanes = false;
        args.relay_repair_both_lanes = false;
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

        let journal = test_hop_journal();
        let before_us = hop_journal::now_unix_us();
        // A stream id of this test only: other tests share the process journal.
        let journal_stream = 77_000_000_077
            + u64::from(send_batch)
            + 2 * u64::from(video_gso)
            + 4 * u64::from(source_first)
            + 8 * u64::from(gso_pace_mbps == 0);
        let outcome = relay
            .publish_part_object(&object, Some((journal_stream, 42)), PublicationClass::Video)
            .await
            .unwrap();
        let stages = journal
            .query(Some(before_us), Some(journal_stream))
            .into_iter()
            .map(|event| (event.stage, event.seq, event.peer))
            .collect::<Vec<_>>();
        // The encode comes first; the two lanes send concurrently, and the
        // first of them to hand the kernel a datagram is `sent_first`.
        assert_eq!(stages.len(), 4, "{stages:?}");
        assert_eq!(stages[0], (HopStage::Encoded, 42, None));
        assert_eq!(
            stages
                .iter()
                .filter(|(stage, seq, peer)| *stage == HopStage::SentFirst
                    && *seq == 42
                    && peer.is_some())
                .count(),
            1,
            "{stages:?}"
        );
        assert!(stages.contains(&(
            HopStage::Sent,
            42,
            Some(primary_receiver.local_addr().unwrap())
        )));
        assert!(stages.contains(&(
            HopStage::SentSecondary,
            42,
            Some(secondary_receiver.local_addr().unwrap())
        )));
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
        // The batch keeps the encoder's order on the wire.
        let sequences = primary
            .iter()
            .map(|symbol| {
                DatagramFecHeader::decode(&symbol.fec_datagram)
                    .unwrap()
                    .packet_sequence
            })
            .collect::<Vec<_>>();
        assert!(
            sequences.windows(2).all(|pair| pair[0] < pair[1]),
            "{sequences:?}"
        );
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
        // A video part: with GSO (where the host has it) each lane's
        // symbols leave in a few GSO messages; without it, none.
        let symbols = (primary.len() + secondary_source.len() + secondary_repair.len()) as u64;
        if send_batch && video_gso && udp_batch::gso_available() {
            // A host with UDP GSO keeps it: a silent fallback fails here.
            assert!(relay.video_gso.load(Ordering::Relaxed));
        }
        if send_batch && video_gso && relay.video_gso.load(Ordering::Relaxed) {
            let messages = snapshot.relay_session.gso_messages;
            eprintln!("{symbols} symbols of one video part in {messages} GSO messages");
            assert_eq!(snapshot.relay_session.gso_datagrams, symbols);
            // At most 16 symbols a message, and runs that fill it: the
            // part costs a few passes through the stack, not one a symbol.
            assert!(messages >= symbols.div_ceil(16), "{messages} {symbols}");
            assert!(messages * 4 <= symbols, "{messages} {symbols}");
        } else {
            assert_eq!(snapshot.relay_session.gso_messages, 0);
            assert_eq!(snapshot.relay_session.gso_datagrams, 0);
        }
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
        // Source first takes no encoder lock and has no separate schedule.
        let old_stages = u64::from(!source_first);
        assert_eq!(snapshot.relay_session.stages.encode_wait.count, old_stages);
        assert_eq!(snapshot.relay_session.stages.encode.count, 1);
        assert_eq!(snapshot.relay_session.stages.schedule.count, old_stages);
        // One send stage per symbol, or one per lane batch (timed under the
        // role of the batch's first symbol); source first hands the
        // secondary its sources and then its repair.
        let (primary_sends, secondary_source_sends, secondary_repair_sends) = if source_first {
            (1, 1, usize::from(!secondary_repair.is_empty()))
        } else if send_batch {
            (1, 1, 0)
        } else {
            (
                primary.len(),
                secondary_source.len(),
                secondary_repair.len(),
            )
        };
        assert_eq!(
            snapshot.relay_session.stages.primary_source_send.count,
            primary_sends as u64
        );
        assert_eq!(
            snapshot.relay_session.stages.secondary_source_send.count,
            secondary_source_sends as u64
        );
        assert_eq!(
            snapshot.relay_session.stages.secondary_repair_send.count,
            secondary_repair_sends as u64
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
            secondary_repair_sends
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
        assert_eq!(status.mesh.relay_send_batch, send_batch);
        assert!(status.mesh.aep1_shared_recovery);
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
    async fn audio_part_encode_does_not_wait_for_a_video_encode_in_flight() {
        let receiver = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let object =
            build_opaque_media_object(&test_opaque_part(4001), TEST_SOURCE_EPOCH, 5_000, 1_000_000)
                .unwrap();
        for shard_mix in [true, false] {
            let mut args = contrib_status_args();
            args.relay_primary_target = Some(receiver.local_addr().unwrap());
            args.publication_shard_mix = shard_mix;
            // relay-session's encoders, behind their locks.
            args.relay_source_first = false;
            let relay = RelaySessionPublisher::new(&args, Arc::new(IngestTelemetry::default()))
                .await
                .unwrap()
                .unwrap();
            // A video part's encode holds its encoder, as a 4K part's does
            // for milliseconds.
            let video_encode = relay.encoder(PublicationClass::Video).lock().await;
            let audio = timeout(
                Duration::from_millis(500),
                relay.publish_part_object(&object, Some((4001, 7)), PublicationClass::Audio),
            )
            .await;
            if shard_mix {
                // The audio part has its own encoder and leaves at once.
                let outcome = audio.expect("audio waited for the video encode").unwrap();
                assert!(outcome.source_symbols > 0);
            } else {
                // One encoder for all: the audio part waits behind the video.
                assert!(
                    audio.is_err(),
                    "the shared encoder did not hold the audio part"
                );
            }
            drop(video_encode);
        }
    }

    #[tokio::test]
    async fn relay_session_applies_and_bounds_the_initial_repair_floor() {
        let receiver = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let mut args = contrib_status_args();
        args.relay_primary_target = Some(receiver.local_addr().unwrap());
        args.relay_min_repair_symbols = 3;

        let relay = RelaySessionPublisher::new(&args, Arc::new(IngestTelemetry::default()))
            .await
            .unwrap()
            .unwrap();
        for class in [PublicationClass::Audio, PublicationClass::Video] {
            assert_eq!(
                relay
                    .encoder(class)
                    .lock()
                    .await
                    .controller()
                    .policy()
                    .min_repair_symbols,
                3
            );
        }

        args.relay_min_repair_symbols = AdaptiveFecPolicy::default().max_repair_symbols + 1;
        let error = RelaySessionPublisher::new(&args, Arc::new(IngestTelemetry::default()))
            .await
            .err()
            .expect("an excessive repair floor must fail");
        assert!(error
            .to_string()
            .contains("--relay-min-repair-symbols cannot exceed"));
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

        let error = relay
            .publish_object(&object, 1, PublicationClass::Video)
            .await
            .unwrap_err();
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
        // A video part, on the one socket of the lane (class lanes off).
        args.relay_class_lanes = false;
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

        let outcome = relay
            .publish_object(&object, 1, PublicationClass::Video)
            .await
            .unwrap();
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
            relay_min_repair_symbols: 0,
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
            audio_epoch_ingress_target: None,
            audio_epoch_redundant_ingress_target: None,
            daw_hls_queue_capacity: DEFAULT_AUDIO_EPOCH_HLS_QUEUE_CAPACITY,
            daw_hls_packaging: DawHlsPackaging::Opaque,
            daw_hls_formats: vec![DawHlsFormat::Flac],
            daw_hls_base_stream_id: 2001,
            stream_id: 1,
            rist_stream_id: 1,
            srt_stream_id: 1,
            rtmp_stream_id: 1,
            repair_symbols: 3,
            symbol_size: DEFAULT_SYMBOL_SIZE,
            rist_bind: Some(SocketAddr::from((Ipv4Addr::LOCALHOST, 27_000))),
            rist_profile: RistProfile::Main,
            srt_bind: Some(SocketAddr::from((Ipv4Addr::LOCALHOST, 27_001))),
            rtmp_bind: Some(SocketAddr::from((Ipv4Addr::LOCALHOST, 19_350))),
            audio_part: None,
            fmp4_part_ms: Some(50),
            fmp4_segment_ms: DEFAULT_SEGMENT_MS,
            video_part: None,
            fmp4_video_part_ms: None,
            fmp4_video_segment_ms: None,
            fmp4_flush_on_complete: true,
            hls_target_duration_ms: DEFAULT_TARGET_DURATION_MS,
            playlist_count: 65,
            playlist_buffer_kb: 800,
            publication_shard_mix: true,
            relay_send_batch: true,
            relay_video_gso: false,
            relay_video_gso_segments: DEFAULT_RELAY_VIDEO_GSO_SEGMENTS,
            relay_video_gso_pace_mbps: DEFAULT_RELAY_VIDEO_GSO_PACE_MBPS,
            aep1_shared_recovery: true,
            relay_direct_targets: Vec::new(),
            relay_direct_bind: None,
            relay_direct_ids: Vec::new(),
            relay_direct_repair: false,
            relay_class_lanes: true,
            relay_class_port_offset: class_lanes::DEFAULT_CLASS_PORT_OFFSET,
            relay_audio_socket_priority: class_lanes::DEFAULT_AUDIO_SOCKET_PRIORITY,
            relay_video_pacing_mbps: 0,
            relay_audio_inline: true,
            relay_repair_both_lanes: true,
            relay_source_first: true,
            relay_video_symbol_size: 0,
            relay_video_plan_grid: true,
            relay_repair_alongside_sources: true,
            audio_hot_thread: false,
            audio_hot_cpu: -1,
            audio_hot_fifo_priority: 0,
            audio_hot_workers: 1,
            hop_journal_capacity: DEFAULT_HOP_JOURNAL_CAPACITY,
        }
    }

    #[cfg(feature = "srt-ingest")]
    #[test]
    fn default_build_accepts_an_srt_listener() {
        assert!(validate_compiled_ingress_features(&contrib_status_args()).is_ok());
    }

    #[cfg(not(feature = "srt-ingest"))]
    #[test]
    fn rist_only_build_rejects_an_srt_listener() {
        let error = validate_compiled_ingress_features(&contrib_status_args()).unwrap_err();
        assert!(error
            .to_string()
            .contains("--srt-bind requires the srt-ingest Cargo feature"));
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
            relay_min_repair_symbols: 0,
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
            audio_epoch_ingress_target: None,
            audio_epoch_redundant_ingress_target: None,
            daw_hls_queue_capacity: DEFAULT_AUDIO_EPOCH_HLS_QUEUE_CAPACITY,
            daw_hls_packaging: DawHlsPackaging::Opaque,
            daw_hls_formats: vec![DawHlsFormat::Flac],
            daw_hls_base_stream_id: 2001,
            stream_id: 9_007_199_254_741_993,
            rist_stream_id: 9_007_199_254_741_994,
            srt_stream_id: 9_007_199_254_741_995,
            rtmp_stream_id: 9_007_199_254_741_996,
            repair_symbols: 3,
            symbol_size: DEFAULT_SYMBOL_SIZE,
            rist_bind: Some(SocketAddr::from((Ipv4Addr::LOCALHOST, 27_000))),
            rist_profile: RistProfile::Main,
            srt_bind: Some(SocketAddr::from((Ipv4Addr::LOCALHOST, 27_001))),
            rtmp_bind: None,
            audio_part: None,
            fmp4_part_ms: Some(50),
            fmp4_segment_ms: DEFAULT_SEGMENT_MS,
            video_part: None,
            fmp4_video_part_ms: None,
            fmp4_video_segment_ms: None,
            fmp4_flush_on_complete: true,
            hls_target_duration_ms: DEFAULT_TARGET_DURATION_MS,
            playlist_count: 65,
            playlist_buffer_kb: 800,
            publication_shard_mix: true,
            relay_send_batch: true,
            relay_video_gso: false,
            relay_video_gso_segments: DEFAULT_RELAY_VIDEO_GSO_SEGMENTS,
            relay_video_gso_pace_mbps: DEFAULT_RELAY_VIDEO_GSO_PACE_MBPS,
            aep1_shared_recovery: true,
            relay_direct_targets: Vec::new(),
            relay_direct_bind: None,
            relay_direct_ids: Vec::new(),
            relay_direct_repair: false,
            relay_class_lanes: true,
            relay_class_port_offset: class_lanes::DEFAULT_CLASS_PORT_OFFSET,
            relay_audio_socket_priority: class_lanes::DEFAULT_AUDIO_SOCKET_PRIORITY,
            relay_video_pacing_mbps: 0,
            relay_audio_inline: true,
            relay_repair_both_lanes: true,
            relay_source_first: true,
            relay_video_symbol_size: 0,
            relay_video_plan_grid: true,
            relay_repair_alongside_sources: true,
            audio_hot_thread: false,
            audio_hot_cpu: -1,
            audio_hot_fifo_priority: 0,
            audio_hot_workers: 1,
            hop_journal_capacity: DEFAULT_HOP_JOURNAL_CAPACITY,
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
        assert_eq!(rist.backend, Some("pure-rust"));
        assert!(rist.flow_id.is_none());
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
        assert!(metrics.contains("# TYPE av_contrib_audio_epoch_ingress_targets gauge\n"));
        assert!(
            metrics.contains("# TYPE av_contrib_audio_epoch_ingress_queue_age_seconds histogram\n")
        );
        assert_eq!(prometheus_label_value("node\\\"\n"), "node\\\\\\\"\\n");
    }

    #[test]
    fn audio_epoch_origin_publication_defaults_to_one_ingress() {
        let mut args = contrib_status_args();
        args.mesh_media_fec_target = "127.0.0.1:12001".parse().unwrap();
        args.relay_primary_target = Some("127.0.0.1:12001".parse().unwrap());
        args.relay_secondary_target = Some("127.0.0.1:12002".parse().unwrap());

        assert_eq!(
            configured_audio_epoch_ingress_targets(&args).unwrap(),
            vec!["127.0.0.1:12001".parse().unwrap()]
        );

        args.audio_epoch_redundant_ingress_target = Some("127.0.0.1:12002".parse().unwrap());
        assert_eq!(
            configured_audio_epoch_ingress_targets(&args).unwrap(),
            vec![
                "127.0.0.1:12001".parse().unwrap(),
                "127.0.0.1:12002".parse().unwrap()
            ]
        );

        args.audio_epoch_ingress_target = Some("127.0.0.1:12002".parse().unwrap());
        assert!(configured_audio_epoch_ingress_targets(&args).is_err());
    }

    #[test]
    fn parses_linux_udp_socket_drop_counter() {
        let link = std::path::Path::new("socket:[12345]");
        assert_eq!(linux_socket_inode(link), Some(12_345));
        assert_eq!(
            linux_socket_inode(std::path::Path::new("pipe:[12345]")),
            None
        );

        let table = "  sl  local_address rem_address   st tx_queue rx_queue tr tm->when retrnsmt   uid  timeout inode\n\
          46: 0100007F:69DC 00000000:0000 07 00000000:00000000 00:00000000 00000000  1000        0 12345 2 0000000000000000 17\n";
        assert_eq!(linux_udp_socket_drops_from_table(table, 12_345), Some(17));
        assert_eq!(linux_udp_socket_drops_from_table(table, 54_321), None);
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
            Arc::new(ContribStatusConfig::from_args(&args, telemetry)),
            None,
            None,
            None,
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

    #[tokio::test]
    async fn contributor_http_does_not_serve_viewer_media() {
        let args = contrib_status_args();
        let telemetry = Arc::new(IngestTelemetry::default());
        let forwarder = Arc::new(
            MeshForwarder::new(&args, Arc::clone(&telemetry))
                .await
                .unwrap(),
        );
        let router = ContribRouter::new(
            forwarder,
            args.stream_id,
            Arc::new(ContribStatusConfig::from_args(&args, telemetry)),
            None,
            None,
            None,
            None,
        );
        let req = Request::builder()
            .method(Method::GET)
            .uri("/1/stream.m3u8")
            .body(())
            .unwrap();

        let response = router.route(req).await.unwrap();

        assert_eq!(response.status, StatusCode::NOT_FOUND);
        assert!(router.webtransport_handler().is_none());
        assert!(router.websocket_handler("/live/1").is_none());
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
            relay_min_repair_symbols: 0,
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
            audio_epoch_ingress_target: None,
            audio_epoch_redundant_ingress_target: None,
            daw_hls_queue_capacity: DEFAULT_AUDIO_EPOCH_HLS_QUEUE_CAPACITY,
            daw_hls_packaging: DawHlsPackaging::Opaque,
            daw_hls_formats: vec![DawHlsFormat::Flac],
            daw_hls_base_stream_id: 2001,
            stream_id: 1,
            rist_stream_id: 0,
            srt_stream_id: 6,
            rtmp_stream_id: 7,
            repair_symbols: 1,
            symbol_size: DEFAULT_SYMBOL_SIZE,
            rist_bind: None,
            rist_profile: RistProfile::Main,
            srt_bind: None,
            rtmp_bind: None,
            audio_part: None,
            fmp4_part_ms: None,
            fmp4_segment_ms: DEFAULT_SEGMENT_MS,
            video_part: None,
            fmp4_video_part_ms: None,
            fmp4_video_segment_ms: None,
            fmp4_flush_on_complete: true,
            hls_target_duration_ms: DEFAULT_TARGET_DURATION_MS,
            playlist_count: 65,
            playlist_buffer_kb: 800,
            publication_shard_mix: true,
            relay_send_batch: true,
            relay_video_gso: false,
            relay_video_gso_segments: DEFAULT_RELAY_VIDEO_GSO_SEGMENTS,
            relay_video_gso_pace_mbps: DEFAULT_RELAY_VIDEO_GSO_PACE_MBPS,
            aep1_shared_recovery: true,
            relay_direct_targets: Vec::new(),
            relay_direct_bind: None,
            relay_direct_ids: Vec::new(),
            relay_direct_repair: false,
            relay_class_lanes: true,
            relay_class_port_offset: class_lanes::DEFAULT_CLASS_PORT_OFFSET,
            relay_audio_socket_priority: class_lanes::DEFAULT_AUDIO_SOCKET_PRIORITY,
            relay_video_pacing_mbps: 0,
            relay_audio_inline: true,
            relay_repair_both_lanes: true,
            relay_source_first: true,
            relay_video_symbol_size: 0,
            relay_video_plan_grid: true,
            relay_repair_alongside_sources: true,
            audio_hot_thread: false,
            audio_hot_cpu: -1,
            audio_hot_fifo_priority: 0,
            audio_hot_workers: 1,
            hop_journal_capacity: DEFAULT_HOP_JOURNAL_CAPACITY,
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
        SockRef::from(&mesh_socket)
            .set_recv_buffer_size(4 * 1024 * 1024)
            .unwrap();
        let mesh_socket = Arc::new(mesh_socket);
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
        let payload_count = 16u32;
        let expected_datagrams = usize::try_from(
            stream_fec_geometry(4_096, args.symbol_size, args.repair_symbols)
                .unwrap()
                .packet_count,
        )
        .unwrap()
            * payload_count as usize;
        let receive_socket = Arc::clone(&mesh_socket);
        let (datagram_tx, mut datagram_rx) = mpsc::unbounded_channel();
        let receiver = tokio::spawn(async move {
            let mut buf = vec![0u8; 65_536];
            for _ in 0..expected_datagrams {
                let (len, _peer) = receive_socket.recv_from(&mut buf).await.unwrap();
                datagram_tx.send(buf[..len].to_vec()).unwrap();
            }
        });
        tokio::task::yield_now().await;
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

        let mut sent_datagrams = 0usize;
        for task in tasks {
            sent_datagrams += task.await.unwrap();
        }
        assert_eq!(sent_datagrams, expected_datagrams);
        timeout(Duration::from_secs(3), receiver)
            .await
            .unwrap()
            .unwrap();
        let mut decoder = FecDatagramDecoder::webtransport_with_stream_prefix(stream_id);
        let mut block_ids = HashSet::new();
        let mut packet_sequences = HashSet::new();
        let mut decoded_payloads = HashSet::new();
        while let Some(datagram) = datagram_rx.recv().await {
            let (decoded_stream_id, raw) = split_stream_id_prefix(&datagram).unwrap();
            assert_eq!(decoded_stream_id, stream_id);
            let header = DatagramFecHeader::decode(raw).unwrap();
            header.payload(raw).unwrap();
            block_ids.insert(header.block_id);
            assert!(packet_sequences.insert(header.packet_sequence));
            if let Some(payload) = decoder.push_datagram(&datagram).unwrap() {
                decoded_payloads.insert(payload);
            }
        }

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
    async fn daw_media_udp_ingest_publishes_decoded_media_to_ingress() {
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
            relay_min_repair_symbols: 0,
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
            audio_epoch_ingress_target: None,
            audio_epoch_redundant_ingress_target: None,
            daw_hls_queue_capacity: DEFAULT_AUDIO_EPOCH_HLS_QUEUE_CAPACITY,
            daw_hls_packaging: DawHlsPackaging::Opaque,
            daw_hls_formats: vec![DawHlsFormat::Flac],
            daw_hls_base_stream_id: 2001,
            stream_id: 1,
            rist_stream_id: 0,
            srt_stream_id: 6,
            rtmp_stream_id: 7,
            repair_symbols: 1,
            symbol_size: DEFAULT_SYMBOL_SIZE,
            rist_bind: None,
            rist_profile: RistProfile::Main,
            srt_bind: None,
            rtmp_bind: None,
            audio_part: None,
            fmp4_part_ms: None,
            fmp4_segment_ms: DEFAULT_SEGMENT_MS,
            video_part: None,
            fmp4_video_part_ms: None,
            fmp4_video_segment_ms: None,
            fmp4_flush_on_complete: true,
            hls_target_duration_ms: DEFAULT_TARGET_DURATION_MS,
            playlist_count: 65,
            playlist_buffer_kb: 800,
            publication_shard_mix: true,
            relay_send_batch: true,
            relay_video_gso: false,
            relay_video_gso_segments: DEFAULT_RELAY_VIDEO_GSO_SEGMENTS,
            relay_video_gso_pace_mbps: DEFAULT_RELAY_VIDEO_GSO_PACE_MBPS,
            aep1_shared_recovery: true,
            relay_direct_targets: Vec::new(),
            relay_direct_bind: None,
            relay_direct_ids: Vec::new(),
            relay_direct_repair: false,
            relay_class_lanes: true,
            relay_class_port_offset: class_lanes::DEFAULT_CLASS_PORT_OFFSET,
            relay_audio_socket_priority: class_lanes::DEFAULT_AUDIO_SOCKET_PRIORITY,
            relay_video_pacing_mbps: 0,
            relay_audio_inline: true,
            relay_repair_both_lanes: true,
            relay_source_first: true,
            relay_video_symbol_size: 0,
            relay_video_plan_grid: true,
            relay_repair_alongside_sources: true,
            audio_hot_thread: false,
            audio_hot_cpu: -1,
            audio_hot_fifo_priority: 0,
            audio_hot_workers: 1,
            hop_journal_capacity: DEFAULT_HOP_JOURNAL_CAPACITY,
        };
        let telemetry = Arc::new(IngestTelemetry::default());
        let forwarder = Arc::new(MeshForwarder::new(&args, telemetry).await.unwrap());
        let (shutdown_tx, shutdown_rx) = watch::channel(());
        let (audio_epoch_mesh_tx, audio_epoch_mesh_rx) = mpsc::channel(256);
        let mesh_task = tokio::spawn(run_audio_epoch_mesh_forward_worker(
            forwarder.clone(),
            audio_epoch_mesh_rx,
            shutdown_rx.clone(),
        ));
        let task = tokio::spawn(run_daw_media_udp_ingest(
            daw_socket,
            forwarder,
            None,
            Some(audio_epoch_mesh_tx),
            shutdown_rx,
        ));

        let metadata = MediaFrameMetadata::new(9, 4, 100, MediaCodec::Opus);
        let mut source_encoder = MediaFecEncoder::default();
        let encoded = source_encoder
            .encode_frame(MediaFrame {
                metadata,
                payload: b"opus-soundkit-frame",
            })
            .unwrap();
        let source = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        for datagram in &encoded.datagrams {
            source.send_to(datagram, daw_bind).await.unwrap();
        }

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
        mesh_task.await.unwrap();
    }

    #[tokio::test]
    async fn daw_media_udp_ingest_publishes_audio_epoch_once_and_serves_no_viewers() {
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
            relay_min_repair_symbols: 0,
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
            audio_epoch_ingress_target: None,
            audio_epoch_redundant_ingress_target: None,
            daw_hls_queue_capacity: DEFAULT_AUDIO_EPOCH_HLS_QUEUE_CAPACITY,
            daw_hls_packaging: DawHlsPackaging::Opaque,
            daw_hls_formats: vec![DawHlsFormat::Flac],
            daw_hls_base_stream_id: 2001,
            stream_id: 1,
            rist_stream_id: 0,
            srt_stream_id: 6,
            rtmp_stream_id: 7,
            repair_symbols: 1,
            symbol_size: DEFAULT_SYMBOL_SIZE,
            rist_bind: None,
            rist_profile: RistProfile::Main,
            srt_bind: None,
            rtmp_bind: None,
            audio_part: None,
            fmp4_part_ms: None,
            fmp4_segment_ms: DEFAULT_SEGMENT_MS,
            video_part: None,
            fmp4_video_part_ms: None,
            fmp4_video_segment_ms: None,
            fmp4_flush_on_complete: true,
            hls_target_duration_ms: DEFAULT_TARGET_DURATION_MS,
            playlist_count: 65,
            playlist_buffer_kb: 800,
            publication_shard_mix: true,
            relay_send_batch: true,
            relay_video_gso: false,
            relay_video_gso_segments: DEFAULT_RELAY_VIDEO_GSO_SEGMENTS,
            relay_video_gso_pace_mbps: DEFAULT_RELAY_VIDEO_GSO_PACE_MBPS,
            aep1_shared_recovery: true,
            relay_direct_targets: Vec::new(),
            relay_direct_bind: None,
            relay_direct_ids: Vec::new(),
            relay_direct_repair: false,
            relay_class_lanes: true,
            relay_class_port_offset: class_lanes::DEFAULT_CLASS_PORT_OFFSET,
            relay_audio_socket_priority: class_lanes::DEFAULT_AUDIO_SOCKET_PRIORITY,
            relay_video_pacing_mbps: 0,
            relay_audio_inline: true,
            relay_repair_both_lanes: true,
            relay_source_first: true,
            relay_video_symbol_size: 0,
            relay_video_plan_grid: true,
            relay_repair_alongside_sources: true,
            audio_hot_thread: false,
            audio_hot_cpu: -1,
            audio_hot_fifo_priority: 0,
            audio_hot_workers: 1,
            hop_journal_capacity: DEFAULT_HOP_JOURNAL_CAPACITY,
        };
        let telemetry = Arc::new(IngestTelemetry::default());
        let forwarder = Arc::new(MeshForwarder::new(&args, telemetry).await.unwrap());
        let (shutdown_tx, shutdown_rx) = watch::channel(());
        let (audio_epoch_mesh_tx, audio_epoch_mesh_rx) = mpsc::channel(256);
        let mesh_task = tokio::spawn(run_audio_epoch_mesh_forward_worker(
            forwarder.clone(),
            audio_epoch_mesh_rx,
            shutdown_rx.clone(),
        ));
        let task = tokio::spawn(run_daw_media_udp_ingest(
            daw_socket,
            forwarder,
            None,
            Some(audio_epoch_mesh_tx),
            shutdown_rx,
        ));

        let would_be_viewer = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        would_be_viewer
            .send_to(b"WAVEY-DAW-SUBSCRIBE/1", daw_bind)
            .await
            .unwrap();
        let mut viewer_buf = vec![0u8; 65_536];
        assert!(
            timeout(
                Duration::from_millis(100),
                would_be_viewer.recv_from(&mut viewer_buf)
            )
            .await
            .is_err(),
            "contributor acknowledged a legacy relay subscription"
        );
        would_be_viewer
            .send_to(b"WAVEY-DAW-SUBSCRIBE/2 91", daw_bind)
            .await
            .unwrap();
        assert!(
            timeout(
                Duration::from_millis(100),
                would_be_viewer.recv_from(&mut viewer_buf)
            )
            .await
            .is_err(),
            "contributor acknowledged a viewer subscription"
        );

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
        assert!(is_audio_transport_datagram(&epoch_datagram));
        assert!(is_audio_transport_datagram(
            b"ACE1\x01\x00\x00\x00opaque-authenticated-audio"
        ));
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

        assert!(
            timeout(
                Duration::from_millis(100),
                would_be_viewer.recv_from(&mut viewer_buf)
            )
            .await
            .is_err(),
            "contributor sent media directly to a viewer"
        );

        let _ = shutdown_tx.send(());
        task.await.unwrap().unwrap();
        mesh_task.await.unwrap();
    }

    // Class lanes, repair on both lanes and the audio hot path.

    /// A loopback receiver pair on P and P + the default class offset.
    async fn class_port_receivers() -> (UdpSocket, UdpSocket) {
        for _ in 0..64 {
            let audio = UdpSocket::bind("127.0.0.1:0").await.unwrap();
            let port = audio.local_addr().unwrap().port();
            let Some(video_port) = port.checked_add(class_lanes::DEFAULT_CLASS_PORT_OFFSET) else {
                continue;
            };
            if let Ok(video) = UdpSocket::bind(("127.0.0.1", video_port)).await {
                for socket in [&audio, &video] {
                    SockRef::from(socket).set_recv_buffer_size(4 << 20).unwrap();
                }
                return (audio, video);
            }
        }
        panic!("no free loopback port pair P, P+1000");
    }

    async fn assert_silent(socket: &UdpSocket) {
        let mut wire = [0_u8; 2_048];
        assert!(
            timeout(Duration::from_millis(50), socket.recv_from(&mut wire))
                .await
                .is_err(),
            "unexpected datagram on {}",
            socket.local_addr().unwrap()
        );
    }

    fn test_video_object(stream_id: u64, sequence: u64, video_units: usize) -> MediaObject {
        test_video_object_of(stream_id, sequence, video_units, 12_000)
    }

    fn test_video_object_of(
        stream_id: u64,
        sequence: u64,
        video_units: usize,
        bytes: usize,
    ) -> MediaObject {
        let payload = vec![0x5a; bytes];
        let (initialization, configuration_epoch) =
            build_fmp4_initialization_object(stream_id, TEST_SOURCE_EPOCH, b"class-lane-init")
                .unwrap();
        let part = test_fmp4_part(
            stream_id,
            sequence,
            true,
            video_units,
            1,
            None,
            Bytes::copy_from_slice(&payload),
            i64::try_from(now_unix_us().saturating_mul(1_000)).unwrap(),
        );
        build_fmp4_media_object(
            &part,
            &payload,
            initialization.key().clone(),
            configuration_epoch,
            TEST_SOURCE_EPOCH,
            5_000,
            1_000_000,
        )
        .unwrap()
    }

    fn test_audio_object(stream_id: u64, bytes: usize) -> MediaObject {
        let mut part = test_opaque_part(stream_id);
        part.bytes = Bytes::from(
            (0..bytes)
                .map(|index| (index * 7 + 3) as u8)
                .collect::<Vec<_>>(),
        );
        build_opaque_media_object(&part, TEST_SOURCE_EPOCH, 5_000, 1_000).unwrap()
    }

    #[tokio::test]
    async fn class_lanes_send_audio_to_p_and_video_to_p_plus_the_offset_from_one_endpoint() {
        let (primary_audio, primary_video) = class_port_receivers().await;
        let (secondary_audio, secondary_video) = class_port_receivers().await;
        let primary_bind = reserved_loopback_addr();
        let secondary_bind = reserved_loopback_addr();
        let mut args = contrib_status_args();
        args.relay_primary_target = Some(primary_audio.local_addr().unwrap());
        args.relay_primary_bind = Some(primary_bind);
        args.relay_secondary_target = Some(secondary_audio.local_addr().unwrap());
        args.relay_secondary_bind = Some(secondary_bind);
        args.relay_secondary_seed_source = true;
        args.relay_deadline_ms = 5_000;
        args.relay_min_repair_symbols = 1;
        assert!(args.relay_class_lanes && args.relay_repair_both_lanes);
        let telemetry = Arc::new(IngestTelemetry::default());
        let relay = RelaySessionPublisher::new(&args, Arc::clone(&telemetry))
            .await
            .unwrap()
            .unwrap();
        #[cfg(target_os = "linux")]
        {
            assert_eq!(
                class_lanes::socket_priority(&*relay.primary.socket).unwrap(),
                6
            );
            let video = relay.primary.video.as_ref().unwrap();
            assert_eq!(class_lanes::socket_priority(&**video).unwrap(), 0);
            assert_eq!(video.local_addr().unwrap(), primary_bind);
        }

        // An audio part: both lanes, the audio ports, the source set and
        // the repair symbols each.
        let audio = test_audio_object(91_000_000_001, 64);
        let outcome = relay
            .publish_part_object(&audio, Some((91_000_000_001, 0)), PublicationClass::Audio)
            .await
            .unwrap();
        let all = outcome.source_symbols + outcome.repair_symbols;
        assert!(outcome.repair_symbols >= 1);
        for (receiver, bind) in [
            (&primary_audio, primary_bind),
            (&secondary_audio, secondary_bind),
        ] {
            let symbols = receive_relay_datagrams(receiver, all).await;
            assert!(symbols
                .iter()
                .all(|(symbol, from)| *from == bind && symbol.object_key == *audio.key()));
        }

        // A video part: the video ports; its repair stays off the primary.
        let video_stream = 91_000_000_002;
        let video = test_video_object(video_stream, 0, 3);
        let outcome = relay
            .publish_part_object(&video, Some((video_stream, 0)), PublicationClass::Video)
            .await
            .unwrap();
        let all = outcome.source_symbols + outcome.repair_symbols;
        for (receiver, bind, count) in [
            (&primary_video, primary_bind, outcome.source_symbols),
            (&secondary_video, secondary_bind, all),
        ] {
            let symbols = receive_relay_datagrams(receiver, count).await;
            assert!(symbols
                .iter()
                .all(|(symbol, from)| *from == bind && symbol.object_key == *video.key()));
        }

        // An audio-only part of the video stream (a muxed delta) stays on
        // the video class.
        let delta = test_video_object(video_stream, 1, 0);
        let outcome = relay
            .publish_part_object(&delta, Some((video_stream, 1)), PublicationClass::Audio)
            .await
            .unwrap();
        let all = outcome.source_symbols + outcome.repair_symbols;
        for (receiver, count) in [
            (&primary_video, outcome.source_symbols),
            (&secondary_video, all),
        ] {
            let symbols = receive_relay_datagrams(receiver, count).await;
            assert!(symbols
                .iter()
                .all(|(symbol, _)| symbol.object_key == *delta.key()));
        }
        for receiver in [
            &primary_audio,
            &primary_video,
            &secondary_audio,
            &secondary_video,
        ] {
            assert_silent(receiver).await;
        }
        let snapshot = telemetry.snapshot();
        assert_eq!(snapshot.relay_session.primary_lane_objects_succeeded, 3);
        assert_eq!(snapshot.relay_session.secondary_lane_objects_succeeded, 3);
        assert_eq!(snapshot.relay_session.repair_primary_fallback_objects, 0);
    }

    #[tokio::test]
    async fn without_class_lanes_every_stream_goes_to_the_one_port() {
        let (primary_audio, primary_video) = class_port_receivers().await;
        let mut args = contrib_status_args();
        args.relay_primary_target = Some(primary_audio.local_addr().unwrap());
        args.relay_primary_bind = Some(reserved_loopback_addr());
        args.relay_deadline_ms = 5_000;
        args.relay_class_lanes = false;
        let relay = RelaySessionPublisher::new(&args, Arc::new(IngestTelemetry::default()))
            .await
            .unwrap()
            .unwrap();
        assert!(relay.primary.video.is_none());
        let video = test_video_object(91_000_000_010, 0, 3);
        let outcome = relay
            .publish_part_object(&video, Some((91_000_000_010, 0)), PublicationClass::Video)
            .await
            .unwrap();
        receive_relay_datagrams(
            &primary_audio,
            outcome.source_symbols + outcome.repair_symbols,
        )
        .await;
        assert_silent(&primary_video).await;
    }

    #[tokio::test]
    async fn class_lane_settings_are_checked() {
        let telemetry = Arc::new(IngestTelemetry::default());
        let mut args = contrib_status_args();
        args.relay_primary_target = Some("127.0.0.1:22001".parse().unwrap());
        args.relay_class_port_offset = 0;
        let error = RelaySessionPublisher::new(&args, Arc::clone(&telemetry))
            .await
            .err()
            .unwrap();
        assert!(
            format!("{error:#}").contains("class-port-offset"),
            "{error:#}"
        );

        // The primary's video port is the secondary's audio port.
        let mut args = contrib_status_args();
        args.relay_primary_target = Some("127.0.0.1:22001".parse().unwrap());
        args.relay_secondary_target = Some("127.0.0.1:23001".parse().unwrap());
        let error = RelaySessionPublisher::new(&args, Arc::clone(&telemetry))
            .await
            .err()
            .unwrap();
        assert!(
            format!("{error:#}").contains("video class port"),
            "{error:#}"
        );

        let parsed = Args::try_parse_from(["av-contrib"]).unwrap();
        // Class lanes and repair on both lanes need a parent configured for
        // them (the deploy renders both on with the parent's flags): off by
        // default, so a default contributor works with a default parent.
        assert!(!parsed.relay_class_lanes);
        assert_eq!(parsed.relay_class_port_offset, 1_000);
        assert_eq!(parsed.relay_audio_socket_priority, 6);
        assert_eq!(parsed.relay_video_pacing_mbps, 0);
        assert!(parsed.relay_audio_inline);
        assert!(!parsed.relay_repair_both_lanes);
        assert!(parsed.audio_hot_thread);
        assert_eq!(parsed.audio_hot_cpu, 2);
        assert_eq!(parsed.audio_hot_fifo_priority, 10);
        assert_eq!(parsed.audio_hot_workers, 0);
        let parsed = Args::try_parse_from([
            "av-contrib",
            "--relay-class-lanes",
            "true",
            "--relay-repair-both-lanes",
            "true",
        ])
        .unwrap();
        assert!(parsed.relay_class_lanes);
        assert!(parsed.relay_repair_both_lanes);
        let parsed = Args::try_parse_from([
            "av-contrib",
            "--relay-class-lanes",
            "false",
            "--relay-audio-inline",
            "false",
            "--relay-repair-both-lanes",
            "false",
            "--audio-hot-thread",
            "false",
            "--audio-hot-cpu",
            "-1",
            "--relay-class-port-offset",
            "2000",
        ])
        .unwrap();
        assert!(!parsed.relay_class_lanes);
        assert!(!parsed.relay_audio_inline);
        assert!(!parsed.relay_repair_both_lanes);
        assert!(!parsed.audio_hot_thread);
        assert_eq!(parsed.audio_hot_cpu, -1);
        assert_eq!(parsed.relay_class_port_offset, 2_000);
    }

    /// Every datagram a lane receives, raw, in arrival order.
    async fn receive_wires(socket: &UdpSocket, count: usize) -> Vec<Vec<u8>> {
        let mut wire = vec![0_u8; 65_536];
        let mut out = Vec::with_capacity(count);
        for _ in 0..count {
            let (len, _) = timeout(Duration::from_secs(3), socket.recv_from(&mut wire))
                .await
                .expect("RelaySession datagram timeout")
                .expect("RelaySession UDP receive");
            out.push(wire[..len].to_vec());
        }
        out
    }

    /// `--relay-source-first`: each lane receives byte for byte what
    /// relay-session's encoder gives it (same objects in the same order),
    /// on the audio hot path (repair on both lanes) and on the joined video
    /// path (repair from the secondary); a lane's source symbols come
    /// before its repair symbols. With the video repair computed alongside
    /// the sources (`--relay-repair-alongside-sources`, a part of 31
    /// sources) the lanes are the same. (`--relay-video-plan-grid` off: it
    /// trims a video part's symbol size, see
    /// grid_video_parts_decode_from_the_lanes.)
    #[tokio::test]
    async fn source_first_lanes_carry_relay_sessions_datagrams() {
        let audio = test_audio_object(91_000_000_090, 3_000);
        let video = test_video_object_of(91_000_000_091, 0, 2, 40_000);
        let mut lanes = Vec::new();
        for (source_first, alongside) in [(false, false), (true, false), (true, true)] {
            let (primary_audio, primary_video) = class_port_receivers().await;
            let (secondary_audio, secondary_video) = class_port_receivers().await;
            let mut args = contrib_status_args();
            args.relay_primary_target = Some(primary_audio.local_addr().unwrap());
            args.relay_primary_bind = Some(reserved_loopback_addr());
            args.relay_secondary_target = Some(secondary_audio.local_addr().unwrap());
            args.relay_secondary_bind = Some(reserved_loopback_addr());
            args.relay_secondary_seed_source = true;
            args.relay_deadline_ms = 5_000;
            args.relay_min_repair_symbols = 2;
            args.relay_source_first = source_first;
            args.relay_video_plan_grid = false;
            args.relay_repair_alongside_sources = alongside;
            let relay = RelaySessionPublisher::new(&args, Arc::new(IngestTelemetry::default()))
                .await
                .unwrap()
                .unwrap();
            let audio_out = relay
                .publish_part_object(&audio, Some((91_000_000_090, 0)), PublicationClass::Audio)
                .await
                .unwrap();
            let video_out = relay
                .publish_part_object(&video, Some((91_000_000_091, 0)), PublicationClass::Video)
                .await
                .unwrap();
            assert!(audio_out.repair_symbols >= 2 && video_out.repair_symbols >= 2);
            assert!(video_out.source_symbols >= REPAIR_ALONGSIDE_MIN_SOURCES);
            let audio_all = audio_out.source_symbols + audio_out.repair_symbols;
            lanes.push((
                receive_wires(&primary_audio, audio_all).await,
                receive_wires(&secondary_audio, audio_all).await,
                receive_wires(&primary_video, video_out.source_symbols).await,
                receive_wires(
                    &secondary_video,
                    video_out.source_symbols + video_out.repair_symbols,
                )
                .await,
            ));
            for socket in [&primary_audio, &secondary_audio, &primary_video] {
                assert_silent(socket).await;
            }
            // Sources lead on every lane.
            let limits = RelayLimits::default();
            let (primary_audio_wires, _, _, secondary_video_wires) = lanes.last().unwrap();
            for (wires, sources) in [
                (primary_audio_wires, audio_out.source_symbols),
                (secondary_video_wires, video_out.source_symbols),
            ] {
                let roles = wires
                    .iter()
                    .map(|wire| relay_session::decode_datagram(wire, limits).unwrap().role)
                    .collect::<Vec<_>>();
                assert!(roles[..sources]
                    .iter()
                    .all(|role| *role == MediaDatagramRole::Source));
                assert!(roles[sources..]
                    .iter()
                    .all(|role| *role == MediaDatagramRole::Repair));
            }
        }
        assert_eq!(lanes[0], lanes[1]);
        assert_eq!(lanes[0], lanes[2]);
    }

    /// `--relay-video-plan-grid` (default on): video parts of varied sizes
    /// leave with a symbol count on the grid and symbols no larger than
    /// without it, their sources ahead of their repair (computed alongside),
    /// and a receiver that lost sources decodes each from the secondary
    /// lane; an audio part's datagrams are as without the grid.
    #[tokio::test]
    async fn grid_video_parts_decode_from_the_lanes() {
        let (primary_audio, primary_video) = class_port_receivers().await;
        let (secondary_audio, secondary_video) = class_port_receivers().await;
        let mut args = contrib_status_args();
        args.relay_primary_target = Some(primary_audio.local_addr().unwrap());
        args.relay_primary_bind = Some(reserved_loopback_addr());
        args.relay_secondary_target = Some(secondary_audio.local_addr().unwrap());
        args.relay_secondary_bind = Some(reserved_loopback_addr());
        args.relay_secondary_seed_source = true;
        args.relay_deadline_ms = 5_000;
        args.relay_min_repair_symbols = 4;
        assert!(args.relay_video_plan_grid && args.relay_repair_alongside_sources);
        let relay = RelaySessionPublisher::new(&args, Arc::new(IngestTelemetry::default()))
            .await
            .unwrap()
            .unwrap();
        let limits = RelayLimits::default();
        let stream = 91_000_000_095;
        for (sequence, bytes) in [100_000_usize, 61_234, 260_000].into_iter().enumerate() {
            let video = test_video_object_of(stream, sequence as u64, 2, bytes);
            let out = relay
                .publish_part_object(
                    &video,
                    Some((stream, sequence as u64)),
                    PublicationClass::Video,
                )
                .await
                .unwrap();
            let symbols = u16::try_from(out.source_symbols).unwrap();
            assert_eq!(
                av_contrib::source_first::grid_symbol_count(symbols),
                symbols
            );
            assert!(symbols > av_contrib::source_first::GRID_EXACT_SYMBOLS);
            assert!(out.announcement.coding.symbol_size() <= 1_316);
            receive_wires(&primary_video, out.source_symbols).await;
            let wires = receive_wires(&secondary_video, out.source_symbols + out.repair_symbols)
                .await
                .iter()
                .map(|wire| relay_session::decode_datagram(wire, limits).unwrap())
                .collect::<Vec<_>>();
            assert!(wires[..out.source_symbols]
                .iter()
                .all(|symbol| symbol.role == MediaDatagramRole::Source));
            let mut assembler =
                relay_session::ObjectAssembler::new(out.announcement.clone(), limits).unwrap();
            let mut recovered = None;
            // The first four sources are lost.
            for symbol in wires.iter().skip(4) {
                if recovered.is_none() {
                    recovered = assembler.push_symbol(symbol).unwrap();
                }
            }
            assert_eq!(recovered.unwrap(), video);
        }
        let audio = test_audio_object(91_000_000_096, 3_000);
        let out = relay
            .publish_part_object(&audio, Some((91_000_000_096, 0)), PublicationClass::Audio)
            .await
            .unwrap();
        let expected = SourceFirstEncoder::new(
            relay
                .source_encoder(PublicationClass::Audio)
                .unwrap()
                .controller()
                .clone(),
            limits,
        )
        .unwrap()
        .encode_sources(
            &audio,
            relay.generation,
            relay.subscription_id,
            out.announcement.deadline,
        )
        .unwrap();
        assert_eq!(out.announcement.coding, expected.coding());
        for socket in [&primary_audio, &secondary_audio] {
            receive_wires(socket, out.source_symbols + out.repair_symbols).await;
        }
        for socket in [
            &primary_audio,
            &secondary_audio,
            &primary_video,
            &secondary_video,
        ] {
            assert_silent(socket).await;
        }
    }

    /// Source first takes no encoder lock: a part leaves while
    /// relay-session's encoders are held (as a 4K encode held them).
    #[tokio::test]
    async fn source_first_parts_never_wait_for_an_encoder_lock() {
        let (receiver, _video_receiver) = class_port_receivers().await;
        for shard_mix in [true, false] {
            let mut args = contrib_status_args();
            args.relay_primary_target = Some(receiver.local_addr().unwrap());
            args.publication_shard_mix = shard_mix;
            assert!(args.relay_source_first);
            let relay = RelaySessionPublisher::new(&args, Arc::new(IngestTelemetry::default()))
                .await
                .unwrap()
                .unwrap();
            let held_audio = relay.encoder(PublicationClass::Audio).try_lock().ok();
            let held_video = relay.encoder(PublicationClass::Video).try_lock().ok();
            assert!(held_audio.is_some() || held_video.is_some());
            for (object, class) in [
                (test_audio_object(4_001, 300), PublicationClass::Audio),
                (test_video_object(1, 0, 1), PublicationClass::Video),
            ] {
                let outcome = timeout(
                    Duration::from_millis(500),
                    relay.publish_part_object(&object, None, class),
                )
                .await
                .expect("a source-first part waited for an encoder lock")
                .unwrap();
                assert!(outcome.source_symbols > 0);
            }
            drop((held_audio, held_video));
        }
    }

    /// The flags: source first on by default, the video symbol size off.
    #[test]
    fn source_first_is_on_by_default_and_the_video_symbol_size_is_off() {
        let parsed = Args::try_parse_from(["av-contrib"]).unwrap();
        assert!(parsed.relay_source_first);
        assert_eq!(parsed.relay_video_symbol_size, 0);
        assert!(parsed.relay_video_plan_grid);
        assert!(parsed.relay_repair_alongside_sources);
        let parsed = Args::try_parse_from([
            "av-contrib",
            "--relay-source-first",
            "false",
            "--relay-video-symbol-size",
            "1088",
            "--relay-video-plan-grid",
            "false",
            "--relay-repair-alongside-sources",
            "false",
        ])
        .unwrap();
        assert!(!parsed.relay_source_first);
        assert_eq!(parsed.relay_video_symbol_size, 1_088);
        assert!(!parsed.relay_video_plan_grid);
        assert!(!parsed.relay_repair_alongside_sources);
    }

    /// The audio hot path with repair on both lanes: the primary alone
    /// recovers a part whose source symbol it lost, both lanes carry the
    /// same packet sequences, and `sent_first` leads `sent`.
    #[tokio::test]
    async fn repair_on_both_lanes_lets_the_primary_alone_recover_a_lost_source() {
        let (primary_audio, _primary_video) = class_port_receivers().await;
        let (secondary_audio, _secondary_video) = class_port_receivers().await;
        let mut args = contrib_status_args();
        args.relay_primary_target = Some(primary_audio.local_addr().unwrap());
        args.relay_primary_bind = Some(reserved_loopback_addr());
        args.relay_secondary_target = Some(secondary_audio.local_addr().unwrap());
        args.relay_secondary_bind = Some(reserved_loopback_addr());
        args.relay_secondary_seed_source = true;
        args.relay_deadline_ms = 5_000;
        args.relay_min_repair_symbols = 2;
        let telemetry = Arc::new(IngestTelemetry::default());
        let relay = RelaySessionPublisher::new(&args, Arc::clone(&telemetry))
            .await
            .unwrap()
            .unwrap();
        let journal = test_hop_journal();
        let before_us = hop_journal::now_unix_us();
        let stream = 91_000_000_020;
        let object = test_audio_object(stream, 3_000);
        let outcome = relay
            .publish_part_object(&object, Some((stream, 4)), PublicationClass::Audio)
            .await
            .unwrap();
        assert!(outcome.source_symbols >= 2, "{}", outcome.source_symbols);
        let all = outcome.source_symbols + outcome.repair_symbols;
        let primary = receive_relay_datagrams(&primary_audio, all).await;
        let secondary = receive_relay_datagrams(&secondary_audio, all).await;
        let sequences = |symbols: &[(relay_session::RelayDatagram, SocketAddr)]| {
            symbols
                .iter()
                .map(|(symbol, _)| {
                    DatagramFecHeader::decode(&symbol.fec_datagram)
                        .unwrap()
                        .packet_sequence
                })
                .collect::<Vec<_>>()
        };
        assert_eq!(sequences(&primary), sequences(&secondary));
        let repairs = primary
            .iter()
            .filter(|(symbol, _)| symbol.role == MediaDatagramRole::Repair)
            .count();
        assert_eq!(repairs, outcome.repair_symbols);

        // The first source symbol is lost on the way to the primary.
        let mut assembler =
            relay_session::ObjectAssembler::new(outcome.announcement, RelayLimits::default())
                .unwrap();
        let mut recovered = None;
        for (symbol, _) in primary.iter().skip(1) {
            if recovered.is_none() {
                recovered = assembler.push_symbol(symbol).unwrap();
            }
        }
        assert_eq!(
            recovered.expect("primary repair recovers the source"),
            object
        );

        let events = journal.query(Some(before_us), Some(stream));
        let at = |stage: HopStage| {
            let matching = events
                .iter()
                .filter(|event| event.stage == stage && event.seq == 4)
                .collect::<Vec<_>>();
            assert_eq!(matching.len(), 1, "{stage:?}: {events:?}");
            matching[0].t_us
        };
        let first = events
            .iter()
            .find(|event| event.stage == HopStage::SentFirst)
            .unwrap_or_else(|| panic!("no sent_first: {events:?}"));
        assert_eq!(first.peer, Some(primary_audio.local_addr().unwrap()));
        assert!(at(HopStage::Encoded) <= at(HopStage::SentFirst));
        assert!(at(HopStage::SentFirst) <= at(HopStage::Sent));
        assert!(at(HopStage::Sent) <= at(HopStage::SentSecondary));
        let snapshot = telemetry.snapshot();
        assert_eq!(snapshot.relay_session.lane_backlog_parts, 0);
        assert_eq!(
            snapshot.relay_session.repair_datagrams,
            (outcome.repair_symbols * 2) as u64
        );
    }

    /// Not a check, a benchmark (wave 5 W1): wall time of an audio part's
    /// publication on the audio hot path with the deploy's lanes (class
    /// lanes, repair on both lanes, seeded secondary), from the call to
    /// `sent` (the journal) and to the return, with relay-session's encoder
    /// and with source first; and the steps of relay-session's path (the
    /// canonical object, validation, envelope, its encode, the RLS1
    /// datagrams). Run with
    /// `cargo test --release --bin av-contrib hot_path_breakdown -- --ignored --nocapture`.
    #[tokio::test(flavor = "current_thread")]
    #[ignore = "benchmark"]
    async fn hot_path_breakdown_of_an_audio_part() {
        const PARTS: u32 = 4_000;
        let journal = test_hop_journal();
        for (source_first, min_repair) in [(false, 0), (true, 0), (false, 2), (true, 2)] {
            let (primary_audio, _primary_video) = class_port_receivers().await;
            let (secondary_audio, _secondary_video) = class_port_receivers().await;
            for socket in [&primary_audio, &secondary_audio] {
                SockRef::from(socket)
                    .set_recv_buffer_size(64 << 20)
                    .unwrap();
            }
            let mut args = contrib_status_args();
            args.relay_primary_target = Some(primary_audio.local_addr().unwrap());
            args.relay_primary_bind = Some(reserved_loopback_addr());
            args.relay_secondary_target = Some(secondary_audio.local_addr().unwrap());
            args.relay_secondary_bind = Some(reserved_loopback_addr());
            args.relay_secondary_seed_source = true;
            args.relay_exclusive = true;
            args.relay_deadline_ms = 5_000;
            args.relay_source_first = source_first;
            args.relay_min_repair_symbols = min_repair;
            let telemetry = Arc::new(IngestTelemetry::default());
            let relay = RelaySessionPublisher::new(&args, Arc::clone(&telemetry))
                .await
                .unwrap()
                .unwrap();
            let drain = |socket: &UdpSocket| {
                let mut wire = [0_u8; 2_048];
                while socket.try_recv_from(&mut wire).is_ok() {}
            };
            for (label, bytes) in [
                ("opus 2.5 ms", 16 + 100),
                ("pcm 1 ms", 16 + 288),
                ("pcm 5 ms", 16 + 1_440),
            ] {
                let stream = 91_000_700 + u64::from(source_first) * 10 + u64::from(min_repair);
                let mut encoder = relay.encoder(PublicationClass::Audio).lock().await.clone();
                let mut totals = [0_u128; 5];
                let mut to_sent = Vec::with_capacity(PARTS as usize);
                let mut to_return = Vec::with_capacity(PARTS as usize);
                let mut symbols = 0;
                for sequence in 0..PARTS {
                    let mut part = test_opaque_part(stream);
                    part.sequence = u64::from(sequence);
                    part.bytes = Bytes::from(vec![(sequence % 251) as u8; bytes]);
                    let t0 = std::time::Instant::now();
                    let object =
                        build_opaque_media_object(&part, TEST_SOURCE_EPOCH, 5_000, 1_000).unwrap();
                    let t1 = std::time::Instant::now();
                    object.validate().unwrap();
                    let t2 = std::time::Instant::now();
                    let envelope = media_object::encode(&object).unwrap();
                    let t3 = std::time::Instant::now();
                    let deadline = relay_deadline_for_object(&object).unwrap();
                    let encoded = encoder
                        .encode_object_with_inferred_priority(
                            &object,
                            relay.generation,
                            relay.subscription_id,
                            deadline,
                        )
                        .unwrap();
                    let t4 = std::time::Instant::now();
                    let mut wires = Vec::new();
                    for symbol in encoded
                        .source_symbols
                        .iter()
                        .chain(encoded.repair_symbols.iter())
                    {
                        wires.push(encode_relay_datagram(symbol, RelayLimits::default()).unwrap());
                    }
                    let t5 = std::time::Instant::now();
                    std::hint::black_box((&envelope, &wires));
                    let seq = u64::from(sequence) + u64::from(PARTS) * bytes as u64;
                    let called_us = hop_journal::now_unix_us();
                    let outcome = relay
                        .publish_part_object(&object, Some((stream, seq)), PublicationClass::Audio)
                        .await
                        .unwrap();
                    let returned_us = hop_journal::now_unix_us();
                    symbols += outcome.source_symbols + outcome.repair_symbols;
                    let sent_us = journal
                        .query(Some(called_us), Some(stream))
                        .into_iter()
                        .find(|event| event.stage == HopStage::Sent && event.seq == seq)
                        .map(|event| event.t_us)
                        .unwrap();
                    to_sent.push(sent_us - called_us);
                    to_return.push(returned_us - called_us);
                    drain(&primary_audio);
                    drain(&secondary_audio);
                    for (total, (from, to)) in
                        totals
                            .iter_mut()
                            .zip([(t0, t1), (t1, t2), (t2, t3), (t3, t4), (t4, t5)])
                    {
                        *total += to.duration_since(from).as_nanos();
                    }
                }
                to_sent.sort_unstable();
                to_return.sort_unstable();
                let pct = |values: &[u64], q: f64| values[((values.len() - 1) as f64 * q) as usize];
                let per = |index: usize| totals[index] / u128::from(PARTS);
                eprintln!(
                    "source_first={source_first} min_repair={min_repair} {label} ({:.1} symbols): \
                     call->sent p50/p99 {}/{} us, call->return p50/p99 {}/{} us | relay-session \
                     steps: object {} ns, validate {} ns, envelope {} ns, encode {} ns, RLS1 {} ns",
                    symbols as f64 / f64::from(PARTS),
                    pct(&to_sent, 0.5),
                    pct(&to_sent, 0.99),
                    pct(&to_return, 0.5),
                    pct(&to_return, 0.99),
                    per(0),
                    per(1),
                    per(2),
                    per(3),
                    per(4),
                );
            }
        }
    }

    /// Not a check, a benchmark (wave 5 W1): the journal's `packaged` ->
    /// `sent` of three audio renditions (PCM 1 ms, FLAC, Opus 2.5 ms sizes)
    /// whose parts are cut at the same instant on three threads, as the
    /// lab's DAW renditions are, through the inline publication and the
    /// deploy's lanes; relay-session's encoder against source first. Run
    /// with `cargo test --release --bin av-contrib renditions_packaged_to_sent
    /// -- --ignored --nocapture`.
    #[test]
    #[ignore = "benchmark"]
    fn renditions_packaged_to_sent() {
        const PARTS: u64 = 3_000;
        let journal = test_hop_journal();
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        for source_first in [false, true] {
            let (receivers, publisher, _shutdown) = runtime.block_on(async {
                let (primary_audio, primary_video) = class_port_receivers().await;
                let (secondary_audio, secondary_video) = class_port_receivers().await;
                for socket in [&primary_audio, &secondary_audio] {
                    SockRef::from(socket)
                        .set_recv_buffer_size(64 << 20)
                        .unwrap();
                }
                let mut args = contrib_status_args();
                args.relay_primary_target = Some(primary_audio.local_addr().unwrap());
                args.relay_primary_bind = Some(reserved_loopback_addr());
                args.relay_secondary_target = Some(secondary_audio.local_addr().unwrap());
                args.relay_secondary_bind = Some(reserved_loopback_addr());
                args.relay_secondary_seed_source = true;
                args.relay_exclusive = true;
                args.relay_deadline_ms = 5_000;
                args.relay_source_first = source_first;
                let telemetry = Arc::new(IngestTelemetry::default());
                let forwarder: Arc<dyn Fmp4PartPublisher> = Arc::new(
                    MeshForwarder::new(&args, Arc::clone(&telemetry))
                        .await
                        .unwrap(),
                );
                let (shutdown_tx, shutdown_rx) = watch::channel(());
                let (publisher, _tasks) = start_bounded_publication_pipeline(
                    forwarder,
                    telemetry,
                    StampDurations {
                        video_part_us: 0,
                        video_segment_us: 2_000_000,
                        audio_part_us: 0,
                        audio_segment_us: 1_000_000,
                    },
                    true,
                    true,
                    shutdown_rx,
                );
                (
                    [
                        primary_audio,
                        primary_video,
                        secondary_audio,
                        secondary_video,
                    ],
                    publisher,
                    shutdown_tx,
                )
            });
            let receivers = receivers
                .into_iter()
                .map(|socket| socket.into_std().unwrap())
                .collect::<Vec<_>>();
            let stop = Arc::new(AtomicBool::new(false));
            let drains = receivers
                .into_iter()
                .map(|socket| {
                    let stop = Arc::clone(&stop);
                    std::thread::spawn(move || {
                        socket.set_nonblocking(false).unwrap();
                        socket
                            .set_read_timeout(Some(Duration::from_millis(20)))
                            .unwrap();
                        let mut wire = [0_u8; 2_048];
                        while !stop.load(Ordering::Relaxed) {
                            let _ = socket.recv_from(&mut wire);
                        }
                    })
                })
                .collect::<Vec<_>>();
            let barrier = Arc::new(std::sync::Barrier::new(3));
            let base = 92_000_000 + u64::from(source_first) * 10;
            let renditions = [(base, 16 + 288), (base + 1, 16 + 600), (base + 2, 16 + 100)]
                .into_iter()
                .map(|(stream, bytes)| {
                    let publisher = Arc::clone(&publisher);
                    let barrier = Arc::clone(&barrier);
                    std::thread::spawn(move || {
                        let runtime = tokio::runtime::Builder::new_current_thread()
                            .enable_all()
                            .build()
                            .unwrap();
                        for sequence in 0..PARTS {
                            barrier.wait();
                            let mut part = test_opaque_part(stream);
                            part.duration_us = 1_000;
                            part.duration_samples = Some(48);
                            part.bytes = Bytes::from(vec![(sequence % 251) as u8; bytes]);
                            part.packaged_at_unix_ns = now_unix_ns();
                            part.published_at_unix_ns = part.packaged_at_unix_ns;
                            runtime
                                .block_on(publisher.publish_opaque_part(part))
                                .unwrap();
                            std::thread::sleep(Duration::from_micros(500));
                        }
                    })
                })
                .collect::<Vec<_>>();
            for rendition in renditions {
                rendition.join().unwrap();
            }
            stop.store(true, Ordering::Relaxed);
            for drain in drains {
                drain.join().unwrap();
            }
            let mut all = Vec::new();
            for stream in base..base + 3 {
                let events = journal.query(None, Some(stream));
                let at = |stage: HopStage| {
                    events
                        .iter()
                        .filter(|event| event.stage == stage)
                        .map(|event| (event.seq, event.t_us))
                        .collect::<HashMap<_, _>>()
                };
                let packaged = at(HopStage::Packaged);
                let sent = at(HopStage::Sent);
                let median_between = |from: HopStage, to: HopStage| {
                    let from = at(from);
                    let to = at(to);
                    let mut deltas = from
                        .iter()
                        .filter_map(|(seq, from)| Some(to.get(seq)?.saturating_sub(*from)))
                        .collect::<Vec<_>>();
                    deltas.sort_unstable();
                    deltas.get(deltas.len() / 2).copied().unwrap_or_default()
                };
                eprintln!(
                    "  medians: packaged->enqueued {} us, enqueued->dequeued {} us, \
                     dequeued->encoded {} us, encoded->sent_first {} us, sent_first->sent {} us",
                    median_between(HopStage::Packaged, HopStage::Enqueued),
                    median_between(HopStage::Enqueued, HopStage::Dequeued),
                    median_between(HopStage::Dequeued, HopStage::Encoded),
                    median_between(HopStage::Encoded, HopStage::SentFirst),
                    median_between(HopStage::SentFirst, HopStage::Sent),
                );
                let mut deltas = packaged
                    .iter()
                    .filter_map(|(seq, packaged)| Some(sent.get(seq)?.saturating_sub(*packaged)))
                    .collect::<Vec<_>>();
                deltas.sort_unstable();
                all.extend_from_slice(&deltas);
                let pct = |q: f64| deltas[((deltas.len() - 1) as f64 * q) as usize];
                eprintln!(
                    "source_first={source_first} stream {stream}: {} parts, packaged->sent p50/p95/p99/max {}/{}/{}/{} us",
                    deltas.len(),
                    pct(0.5),
                    pct(0.95),
                    pct(0.99),
                    deltas[deltas.len() - 1],
                );
            }
            all.sort_unstable();
            let pct = |q: f64| all[((all.len() - 1) as f64 * q) as usize];
            eprintln!(
                "source_first={source_first} all renditions: packaged->sent p50/p95/p99 {}/{}/{} us",
                pct(0.5),
                pct(0.95),
                pct(0.99)
            );
        }
    }

    /// Process CPU time in nanoseconds (the publishing thread and the
    /// blocking thread that may compute the repair), for the benchmarks.
    fn process_cpu_ns() -> u64 {
        let mut spec = libc::timespec {
            tv_sec: 0,
            tv_nsec: 0,
        };
        // SAFETY: clock_gettime writes the timespec it is given.
        unsafe { libc::clock_gettime(libc::CLOCK_PROCESS_CPUTIME_ID, &mut spec) };
        spec.tv_sec as u64 * 1_000_000_000 + spec.tv_nsec as u64
    }

    /// 4K part sizes: `bytes` plus or minus up to 20 % every frame (a fixed
    /// pseudo-random sequence) and a keyframe three times as large every
    /// 50th part, so the RaptorQ symbol count changes from part to part as
    /// a real encoder's does.
    fn varied_part_bytes(bytes: usize, sequence: u64) -> usize {
        if sequence % 50 == 0 {
            return bytes * 3;
        }
        // splitmix64 of the sequence: sizes independent frame to frame.
        let mut mixed = sequence.wrapping_add(0x9e37_79b9_7f4a_7c15);
        mixed = (mixed ^ (mixed >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        mixed = (mixed ^ (mixed >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        mixed ^= mixed >> 31;
        bytes * (800 + ((mixed >> 33) % 401) as usize) / 1_000
    }

    /// Not a check, a benchmark (wave 5 W3): the contributor's cost of a 4K
    /// part on the video lanes (class lanes, a seeded secondary that also
    /// carries the repair) at 25, 80 and 200 Mbit/s of 50 fps parts, of one
    /// size ("fixed": every repair finds its encoding plan cached) and of
    /// sizes that vary as a real encoder's do ("varied", see
    /// `varied_part_bytes`): the call's wall time to `sent` (last source on
    /// the primary) and to its return (the last repair symbol handed to the
    /// lanes: "repair sent"), and the process CPU per part (the publishing
    /// thread and the repair's blocking thread), with relay-session's
    /// encoder or source first; source first as first committed (repair
    /// after the sources' join, symbol counts as they fall), with
    /// --relay-video-plan-grid and --relay-repair-alongside-sources, and
    /// with GSO. Run with `cargo test --release --bin av-contrib
    /// video_lane_cost -- --ignored --nocapture`.
    #[tokio::test(flavor = "current_thread")]
    #[ignore = "benchmark"]
    async fn video_lane_cost_per_part() {
        let journal = test_hop_journal();
        for (index, (source_first, grid, alongside, gso, symbol_size)) in [
            (false, false, false, false, 0),
            (true, false, false, false, 0),
            (true, true, false, false, 0),
            (true, true, true, false, 0),
            (true, true, true, true, 0),
            (true, true, true, true, 1_088),
        ]
        .into_iter()
        .enumerate()
        {
            for varied in [false, true] {
                let (primary_audio, primary_video) = class_port_receivers().await;
                let (secondary_audio, secondary_video) = class_port_receivers().await;
                for socket in [&primary_video, &secondary_video] {
                    SockRef::from(socket)
                        .set_recv_buffer_size(64 << 20)
                        .unwrap();
                }
                let mut args = contrib_status_args();
                args.relay_primary_target = Some(primary_audio.local_addr().unwrap());
                args.relay_primary_bind = Some(reserved_loopback_addr());
                args.relay_secondary_target = Some(secondary_audio.local_addr().unwrap());
                args.relay_secondary_bind = Some(reserved_loopback_addr());
                args.relay_secondary_seed_source = true;
                args.relay_exclusive = true;
                args.relay_deadline_ms = 5_000;
                args.relay_source_first = source_first;
                args.relay_video_plan_grid = grid;
                args.relay_repair_alongside_sources = alongside;
                args.relay_video_gso = gso;
                args.relay_video_symbol_size = symbol_size;
                let relay = RelaySessionPublisher::new(&args, Arc::new(IngestTelemetry::default()))
                    .await
                    .unwrap()
                    .unwrap();
                let primary_video = primary_video.into_std().unwrap();
                let secondary_video = secondary_video.into_std().unwrap();
                let drain = |socket: &std::net::UdpSocket| {
                    let mut wire = [0_u8; 2_048];
                    let mut count = 0;
                    while socket.recv_from(&mut wire).is_ok() {
                        count += 1;
                    }
                    count
                };
                for (label, bytes, parts) in [
                    ("25 Mbit/s", 62_500, 300_u64),
                    ("80 Mbit/s", 200_000, 200),
                    ("200 Mbit/s", 500_000, 150),
                ] {
                    let stream = 93_000_000 + bytes as u64;
                    let objects = (0..parts)
                        .map(|sequence| {
                            let size = if varied {
                                varied_part_bytes(bytes, sequence)
                            } else {
                                bytes
                            };
                            test_video_object_of(stream, sequence, 1, size)
                        })
                        .collect::<Vec<_>>();
                    let mut to_sent = Vec::new();
                    let mut wall = Vec::new();
                    let mut cpu = 0;
                    let mut received = 0;
                    for (sequence, object) in objects.iter().enumerate() {
                        let seq =
                            sequence as u64 + (index as u64 * 2 + u64::from(varied)) * 1_000_000;
                        let called_us = hop_journal::now_unix_us();
                        let cpu_before = process_cpu_ns();
                        relay
                            .publish_part_object(
                                object,
                                Some((stream, seq)),
                                PublicationClass::Video,
                            )
                            .await
                            .unwrap();
                        cpu += process_cpu_ns() - cpu_before;
                        wall.push(hop_journal::now_unix_us() - called_us);
                        let sent_us = journal
                            .query(Some(called_us), Some(stream))
                            .into_iter()
                            .find(|event| event.stage == HopStage::Sent && event.seq == seq)
                            .map(|event| event.t_us)
                            .unwrap();
                        to_sent.push(sent_us - called_us);
                        received += drain(&primary_video) + drain(&secondary_video);
                    }
                    to_sent.sort_unstable();
                    wall.sort_unstable();
                    eprintln!(
                        "source_first={source_first} grid={grid} alongside={alongside} gso={gso} \
                         symbol={symbol_size} {} 4K {label} ({bytes} B parts): call->sent p50 {} \
                         us, call->repair sent p50 {} p95 {} us, process CPU {} us/part = {:.1}% \
                         of a core at 50 fps; {} datagrams received/part",
                        if varied { "varied" } else { "fixed" },
                        to_sent[to_sent.len() / 2],
                        wall[wall.len() / 2],
                        wall[wall.len() * 95 / 100],
                        cpu / parts / 1_000,
                        (cpu / parts) as f64 * 50.0 / 1e7,
                        received / parts as usize,
                    );
                }
                drop((primary_audio, secondary_audio));
            }
        }
    }

    /// A receiver of one video lane for [`video_gso_cpu_and_bursts`]: the
    /// kernel's receive time of every datagram (SO_TIMESTAMPNS), and the
    /// thread's own CPU, which the benchmark leaves out of the contributor's.
    fn video_lane_sink(
        socket: std::net::UdpSocket,
        stop: Arc<AtomicBool>,
    ) -> std::thread::JoinHandle<(Vec<u64>, u64)> {
        use std::os::fd::AsRawFd;
        std::thread::spawn(move || {
            let cpu_before = thread_cpu_ns();
            socket.set_nonblocking(false).unwrap();
            socket
                .set_read_timeout(Some(Duration::from_millis(20)))
                .unwrap();
            let on: libc::c_int = 1;
            // SAFETY: an int option of a socket this thread owns.
            unsafe {
                libc::setsockopt(
                    socket.as_raw_fd(),
                    libc::SOL_SOCKET,
                    libc::SO_TIMESTAMPNS,
                    std::ptr::addr_of!(on).cast(),
                    std::mem::size_of::<libc::c_int>() as libc::socklen_t,
                );
            }
            let mut times = Vec::with_capacity(1 << 20);
            let mut buffer = [0_u8; 2_048];
            let mut control = [0_u64; 16];
            loop {
                let mut iov = libc::iovec {
                    iov_base: buffer.as_mut_ptr().cast(),
                    iov_len: buffer.len(),
                };
                // SAFETY: a zeroed msghdr is valid; the buffers outlive the
                // call.
                let mut header: libc::msghdr = unsafe { std::mem::zeroed() };
                header.msg_iov = &mut iov;
                header.msg_iovlen = 1;
                header.msg_control = control.as_mut_ptr().cast();
                header.msg_controllen = std::mem::size_of_val(&control) as _;
                // SAFETY: as above.
                let rc = unsafe { libc::recvmsg(socket.as_raw_fd(), &mut header, 0) };
                if rc < 0 {
                    if stop.load(Ordering::Acquire) {
                        break;
                    }
                    continue;
                }
                // SAFETY: the kernel filled `control` as `header` says.
                let mut cmsg = unsafe { libc::CMSG_FIRSTHDR(&header) };
                while !cmsg.is_null() {
                    // SAFETY: a control message of the kernel's.
                    unsafe {
                        if (*cmsg).cmsg_level == libc::SOL_SOCKET
                            && (*cmsg).cmsg_type == libc::SCM_TIMESTAMPNS
                        {
                            let spec = std::ptr::read_unaligned(
                                libc::CMSG_DATA(cmsg).cast::<libc::timespec>(),
                            );
                            times.push(spec.tv_sec as u64 * 1_000_000_000 + spec.tv_nsec as u64);
                        }
                        cmsg = libc::CMSG_NXTHDR(&header, cmsg);
                    }
                }
            }
            (times, thread_cpu_ns() - cpu_before)
        })
    }

    /// Most datagrams within any `window_ns` of sorted receive times.
    fn max_in_window(times: &[u64], window_ns: u64) -> usize {
        let mut best = 0;
        let mut start = 0;
        for end in 0..times.len() {
            while times[end] - times[start] >= window_ns {
                start += 1;
            }
            best = best.max(end - start + 1);
        }
        best
    }

    /// Not a check, a benchmark: the contributor's CPU and the burst
    /// profile of 4K parts on the video lanes, `--relay-video-gso` off, on
    /// unpaced, and on paced at `--relay-video-gso-pace-mbps` (its default,
    /// or AV_GSO_BENCH_PACE_MBPS). Parts of one 24 fps frame at 80, 122 and 160 Mbit/s are
    /// published in real time for five seconds each to a primary and a
    /// seeded secondary (class lanes, as the lab deploys it), each drained
    /// by its own thread. Reported: the process CPU less the sinks' threads,
    /// as % of one core, and per lane the most datagrams the kernel took in
    /// within any 100 us and any 1 ms (loopback: the receive time is the
    /// send time). Loopback frees a datagram's send buffer charge at once, so
    /// the socket never pushes back here as a NIC's queue does. Run with
    /// `cargo test --release --bin av-contrib video_gso_cpu_and_bursts --
    /// --ignored --nocapture` (AV_GSO_BENCH_SECONDS to change the length).
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    #[ignore = "benchmark"]
    async fn video_gso_cpu_and_bursts() {
        let seconds: u64 = std::env::var("AV_GSO_BENCH_SECONDS")
            .ok()
            .and_then(|value| value.parse().ok())
            .unwrap_or(5);
        let pace_mbps: u64 = std::env::var("AV_GSO_BENCH_PACE_MBPS")
            .ok()
            .and_then(|value| value.parse().ok())
            .unwrap_or(DEFAULT_RELAY_VIDEO_GSO_PACE_MBPS);
        const FPS: u64 = 24;
        for mbps in [80_u64, 122, 160] {
            for (gso, pace) in [(false, 0), (true, 0), (true, pace_mbps)] {
                let (primary_audio, primary_video) = class_port_receivers().await;
                let (secondary_audio, secondary_video) = class_port_receivers().await;
                for socket in [&primary_video, &secondary_video] {
                    SockRef::from(socket)
                        .set_recv_buffer_size(64 << 20)
                        .unwrap();
                }
                let mut args = contrib_status_args();
                args.relay_primary_target = Some(primary_audio.local_addr().unwrap());
                args.relay_primary_bind = Some(reserved_loopback_addr());
                args.relay_secondary_target = Some(secondary_audio.local_addr().unwrap());
                args.relay_secondary_bind = Some(reserved_loopback_addr());
                args.relay_secondary_seed_source = true;
                args.relay_exclusive = true;
                args.relay_deadline_ms = 5_000;
                args.relay_video_gso = gso;
                args.relay_video_gso_pace_mbps = pace;
                let telemetry = Arc::new(IngestTelemetry::default());
                let relay = RelaySessionPublisher::new(&args, Arc::clone(&telemetry))
                    .await
                    .unwrap()
                    .unwrap();
                let stop = Arc::new(AtomicBool::new(false));
                let sinks = [primary_video, secondary_video]
                    .map(|socket| video_lane_sink(socket.into_std().unwrap(), Arc::clone(&stop)));
                let bytes = (mbps * 1_000_000 / 8 / FPS) as usize;
                let parts = seconds * FPS;
                let stream = 94_000_000 + mbps * 100_000 + pace * 10 + u64::from(gso);
                let frame = Duration::from_nanos(1_000_000_000 / FPS);
                tokio::time::sleep(Duration::from_millis(50)).await;
                let cpu_before = process_cpu_ns();
                let started = tokio::time::Instant::now();
                let mut late = 0;
                // The parts are built as they are due (their deadline runs
                // from their time), on this thread: its CPU is left out.
                let mut build_cpu = 0;
                for sequence in 0..parts as usize {
                    tokio::time::sleep_until(started + frame * sequence as u32).await;
                    let build_started = thread_cpu_ns();
                    let object = test_video_object_of(stream, sequence as u64, 1, bytes);
                    build_cpu += thread_cpu_ns() - build_started;
                    relay
                        .publish_part_object(
                            &object,
                            Some((stream, sequence as u64)),
                            PublicationClass::Video,
                        )
                        .await
                        .unwrap();
                    if started.elapsed() > frame * (sequence as u32 + 1) {
                        late += 1;
                    }
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
                let wall = started.elapsed();
                let cpu = (process_cpu_ns() - cpu_before).saturating_sub(build_cpu);
                stop.store(true, Ordering::Release);
                let mut sink_cpu = 0;
                let mut lanes = Vec::new();
                for sink in sinks {
                    let (mut times, cpu) = sink.join().unwrap();
                    sink_cpu += cpu;
                    times.sort_unstable();
                    lanes.push(format!(
                        "{} datagrams/part, max {}/100us {}/1ms",
                        times.len() as u64 / parts,
                        max_in_window(&times, 100_000),
                        max_in_window(&times, 1_000_000),
                    ));
                }
                let snapshot = telemetry.snapshot();
                eprintln!(
                    "gso={gso} pace={pace} {mbps} Mbit/s ({bytes} B parts at {FPS} fps): contributor CPU \
                     {:.1}% of a core ({:.2}% per 10 Mbit/s); {} GSO messages/part; parts over \
                     a frame {late}/{parts}; primary {}; secondary {}",
                    (cpu.saturating_sub(sink_cpu)) as f64 * 100.0 / wall.as_nanos() as f64,
                    (cpu.saturating_sub(sink_cpu)) as f64 * 1_000.0
                        / wall.as_nanos() as f64
                        / mbps as f64,
                    snapshot.relay_session.gso_messages / parts,
                    lanes[0],
                    lanes[1],
                );
                drop((primary_audio, secondary_audio));
            }
        }
    }

    /// While a lane has a part waiting for room in its socket buffer, the
    /// lane's next audio part queues behind it and leaves after it.
    #[tokio::test]
    async fn an_audio_part_queues_behind_a_waiting_part_of_its_lane() {
        let (primary_audio, _primary_video) = class_port_receivers().await;
        let mut args = contrib_status_args();
        args.relay_primary_target = Some(primary_audio.local_addr().unwrap());
        args.relay_primary_bind = Some(reserved_loopback_addr());
        args.relay_deadline_ms = 5_000;
        let telemetry = Arc::new(IngestTelemetry::default());
        let relay = RelaySessionPublisher::new(&args, Arc::clone(&telemetry))
            .await
            .unwrap()
            .unwrap();
        // A part of the lane is waiting (as after a full socket buffer).
        relay.primary.backlog.pending.fetch_add(1, Ordering::AcqRel);
        let journal = test_hop_journal();
        let before_us = hop_journal::now_unix_us();
        let stream = 91_000_000_030;
        let object = test_audio_object(stream, 64);
        let outcome = relay
            .publish_part_object(&object, Some((stream, 2)), PublicationClass::Audio)
            .await
            .unwrap();
        // The publish returned without sending: the backlog task sends it.
        let all = outcome.source_symbols + outcome.repair_symbols;
        let symbols = receive_relay_datagrams(&primary_audio, all).await;
        assert!(symbols
            .iter()
            .all(|(symbol, _)| symbol.object_key == *object.key()));
        assert_eq!(
            symbols
                .iter()
                .take(outcome.source_symbols)
                .filter(|(symbol, _)| symbol.role == MediaDatagramRole::Source)
                .count(),
            outcome.source_symbols,
            "the source set leads"
        );
        let snapshot = telemetry.snapshot();
        assert_eq!(snapshot.relay_session.lane_backlog_parts, 1);
        assert_eq!(snapshot.relay_session.lane_backlog_dropped, 0);
        // `sent` is journalled by the backlog once the sources are out.
        let mut sent = Vec::new();
        for _ in 0..100 {
            sent = journal
                .query(Some(before_us), Some(stream))
                .into_iter()
                .filter(|event| event.stage == HopStage::Sent)
                .collect::<Vec<_>>();
            if !sent.is_empty() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        assert_eq!(sent.len(), 1);
        assert_eq!(sent[0].peer, Some(primary_audio.local_addr().unwrap()));
        // The backlog sent the part's first datagram: it journals
        // `sent_first` too.
        let first = journal
            .query(Some(before_us), Some(stream))
            .into_iter()
            .filter(|event| event.stage == HopStage::SentFirst)
            .collect::<Vec<_>>();
        assert_eq!(first.len(), 1);
        assert!(first[0].t_us <= sent[0].t_us);
        // Only the part's own (simulated) predecessor is still counted.
        for _ in 0..100 {
            if relay.primary.backlog.pending.load(Ordering::Acquire) == 1 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        assert_eq!(relay.primary.backlog.pending.load(Ordering::Acquire), 1);
    }

    #[derive(Default)]
    struct RecordingPublicationSink {
        published: std::sync::Mutex<Vec<(u64, u64)>>,
    }

    #[async_trait::async_trait]
    impl Fmp4PartPublisher for RecordingPublicationSink {
        async fn publish_fmp4_part(
            &self,
            part: PublishedFmp4Part,
        ) -> std::result::Result<(), String> {
            self.published
                .lock()
                .unwrap()
                .push((part.stream_id, part.sequence));
            Ok(())
        }

        async fn publish_opaque_part(
            &self,
            part: PublishedOpaquePart,
        ) -> std::result::Result<(), String> {
            self.published
                .lock()
                .unwrap()
                .push((part.stream_id, part.sequence));
            Ok(())
        }
    }

    /// `--relay-audio-inline`: an audio part is published before the
    /// packager's call returns, without a worker queue; a video part still
    /// queues for its worker.
    #[tokio::test]
    async fn audio_parts_are_published_on_the_packagers_task() {
        let sink = Arc::new(RecordingPublicationSink::default());
        let inner: Arc<dyn Fmp4PartPublisher> = sink.clone();
        let (publisher, mut receivers) = test_sharded_publisher(true);
        let publisher = BoundedFmp4Publisher {
            inline_audio: Some(inner),
            ..publisher
        };
        let journal = test_hop_journal();
        let before_us = hop_journal::now_unix_us() - 1;
        let audio_stream = 91_000_000_040;
        for _ in 0..3 {
            publisher
                .publish_opaque_part(test_opaque_part(audio_stream))
                .await
                .unwrap();
        }
        assert_eq!(
            *sink.published.lock().unwrap(),
            [(audio_stream, 0), (audio_stream, 1), (audio_stream, 2)]
        );
        let audio_fmp4 = test_fmp4_part(
            91_000_000_041,
            0,
            true,
            0,
            1,
            Some(Bytes::from_static(b"init")),
            Bytes::from_static(b"aac"),
            now_unix_ns(),
        );
        publisher.publish_fmp4_part(audio_fmp4).await.unwrap();
        assert_eq!(sink.published.lock().unwrap().len(), 4);
        let video = test_fmp4_part(
            91_000_000_042,
            0,
            true,
            3,
            1,
            Some(Bytes::from_static(b"init")),
            Bytes::from_static(b"h264"),
            now_unix_ns(),
        );
        publisher.publish_fmp4_part(video).await.unwrap();
        assert_eq!(sink.published.lock().unwrap().len(), 4);
        let queued = receivers
            .iter_mut()
            .filter_map(|receiver| receiver.try_recv().ok())
            .map(|publication| publication.stream_id())
            .collect::<Vec<_>>();
        assert_eq!(queued, [91_000_000_042]);

        // The hop journal still has packaged, enqueued and dequeued.
        let stages = journal
            .query(Some(before_us), Some(audio_stream))
            .into_iter()
            .filter(|event| event.seq == 1)
            .map(|event| event.stage)
            .collect::<Vec<_>>();
        for stage in [HopStage::Packaged, HopStage::Enqueued, HopStage::Dequeued] {
            assert!(stages.contains(&stage), "{stage:?}: {stages:?}");
        }
    }

    /// A stream with a part still in a worker's queue keeps queueing, so
    /// that its parts leave in order.
    #[tokio::test]
    async fn an_audio_stream_with_a_queued_part_stays_queued() {
        let sink = Arc::new(RecordingPublicationSink::default());
        let inner: Arc<dyn Fmp4PartPublisher> = sink.clone();
        let (publisher, mut receivers) = test_sharded_publisher(true);
        let stream = 91_000_000_050;
        // The first part queues (inline off), the second must follow it.
        publisher
            .publish_opaque_part(test_opaque_part(stream))
            .await
            .unwrap();
        let publisher = BoundedFmp4Publisher {
            inline_audio: Some(inner),
            ..publisher
        };
        publisher
            .publish_opaque_part(test_opaque_part(stream))
            .await
            .unwrap();
        assert!(sink.published.lock().unwrap().is_empty());
        let queued = receivers
            .iter_mut()
            .flat_map(|receiver| std::iter::from_fn(move || receiver.try_recv().ok()))
            .map(|publication| publication.sequence())
            .collect::<Vec<_>>();
        assert_eq!(queued, [0, 1]);
        // Both are published: the next part is inline.
        drop(queued);
        publisher
            .publish_opaque_part(test_opaque_part(stream))
            .await
            .unwrap();
        assert_eq!(*sink.published.lock().unwrap(), [(stream, 2)]);
    }

    // Review fixes: the class of a stream, repair on both lanes for audio
    // only, the lane backlog's outcome, isolation from video, the audio hot
    // threads.

    #[test]
    fn a_stream_is_video_for_its_epoch_while_it_sends_video() {
        let classes = VideoStreamClasses::default();
        let audio = PublicationClass::Audio;
        let video = PublicationClass::Video;
        let t = 1_000_000_000_u64;
        assert_eq!(classes.class_of(7, 1, audio, t), audio);
        assert_eq!(classes.class_of(7, 1, video, t), video);
        // A muxed audio-only delta of the stream rides the video class.
        assert_eq!(classes.class_of(7, 1, audio, t + 1), video);
        // The same stream id in a new source epoch (a restart) is audio.
        assert_eq!(classes.class_of(7, 2, audio, t + 2), audio);
        // A stream whose video stopped long ago goes back to audio.
        assert_eq!(
            classes.class_of(7, 1, audio, t + VIDEO_CLASS_HOLD_US),
            audio
        );
        // A video part refreshes it.
        assert_eq!(
            classes.class_of(7, 1, video, t + VIDEO_CLASS_HOLD_US),
            video
        );
        assert_eq!(
            classes.class_of(7, 1, audio, t + VIDEO_CLASS_HOLD_US + 1),
            video
        );
        // Video in a new epoch replaces the old epoch.
        assert_eq!(
            classes.class_of(7, 2, video, t + VIDEO_CLASS_HOLD_US + 2),
            video
        );
        assert_eq!(
            classes.class_of(7, 1, audio, t + VIDEO_CLASS_HOLD_US + 3),
            audio
        );
        assert_eq!(
            classes.class_of(7, 2, audio, t + VIDEO_CLASS_HOLD_US + 3),
            video
        );
        // Other streams are not touched.
        assert_eq!(classes.class_of(8, 2, audio, t), audio);
    }

    /// With every new flag off, a part goes through the encoder of its own
    /// class, as before class lanes: the stream's video does not move its
    /// audio-only parts to the video encoder.
    #[tokio::test]
    async fn without_class_lanes_a_part_keeps_its_own_class() {
        let (primary_audio, _primary_video) = class_port_receivers().await;
        let mut args = contrib_status_args();
        args.relay_primary_target = Some(primary_audio.local_addr().unwrap());
        args.relay_primary_bind = Some(reserved_loopback_addr());
        args.relay_class_lanes = false;
        args.relay_audio_inline = false;
        args.relay_repair_both_lanes = false;
        let relay = RelaySessionPublisher::new(&args, Arc::new(IngestTelemetry::default()))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            relay.stream_class(5, 1, PublicationClass::Video),
            PublicationClass::Video
        );
        assert_eq!(
            relay.stream_class(5, 1, PublicationClass::Audio),
            PublicationClass::Audio
        );

        args.relay_class_lanes = true;
        args.relay_primary_bind = Some(reserved_loopback_addr());
        let relay = RelaySessionPublisher::new(&args, Arc::new(IngestTelemetry::default()))
            .await
            .unwrap()
            .unwrap();
        relay.stream_class(5, 1, PublicationClass::Video);
        assert_eq!(
            relay.stream_class(5, 1, PublicationClass::Audio),
            PublicationClass::Video
        );
    }

    /// --relay-repair-both-lanes adds repair to the primary for audio parts
    /// only: a 4K part's primary lane carries its source set alone.
    #[tokio::test]
    async fn repair_on_both_lanes_leaves_video_repair_to_the_secondary() {
        let (primary_audio, primary_video) = class_port_receivers().await;
        let (secondary_audio, secondary_video) = class_port_receivers().await;
        let mut args = contrib_status_args();
        args.relay_primary_target = Some(primary_audio.local_addr().unwrap());
        args.relay_primary_bind = Some(reserved_loopback_addr());
        args.relay_secondary_target = Some(secondary_audio.local_addr().unwrap());
        args.relay_secondary_bind = Some(reserved_loopback_addr());
        args.relay_secondary_seed_source = true;
        args.relay_deadline_ms = 5_000;
        args.relay_min_repair_symbols = 2;
        args.relay_class_lanes = true;
        args.relay_repair_both_lanes = true;
        let relay = RelaySessionPublisher::new(&args, Arc::new(IngestTelemetry::default()))
            .await
            .unwrap()
            .unwrap();
        let stream = 91_000_000_060;
        let video = test_video_object(stream, 0, 3);
        let outcome = relay
            .publish_part_object(&video, Some((stream, 0)), PublicationClass::Video)
            .await
            .unwrap();
        assert!(outcome.repair_symbols > 0);
        let primary = receive_relay_datagrams(&primary_video, outcome.source_symbols).await;
        assert!(primary
            .iter()
            .all(|(symbol, _)| symbol.role == MediaDatagramRole::Source));
        assert_silent(&primary_video).await;
        let secondary = receive_relay_datagrams(
            &secondary_video,
            outcome.source_symbols + outcome.repair_symbols,
        )
        .await;
        assert_eq!(
            secondary
                .iter()
                .filter(|(symbol, _)| symbol.role == MediaDatagramRole::Repair)
                .count(),
            outcome.repair_symbols
        );

        // An audio part of another stream: repair on both.
        let audio = test_audio_object(stream + 1, 3_000);
        let outcome = relay
            .publish_part_object(&audio, Some((stream + 1, 0)), PublicationClass::Audio)
            .await
            .unwrap();
        let primary = receive_relay_datagrams(
            &primary_audio,
            outcome.source_symbols + outcome.repair_symbols,
        )
        .await;
        assert_eq!(
            primary
                .iter()
                .filter(|(symbol, _)| symbol.role == MediaDatagramRole::Repair)
                .count(),
            outcome.repair_symbols
        );
    }

    /// A publisher with the audio hot path, on the source-first encoder or
    /// (`source_first` false) relay-session's per-class encoders.
    async fn inline_audio_publisher(
        telemetry: &Arc<IngestTelemetry>,
        source_first: bool,
    ) -> (RelaySessionPublisher, UdpSocket) {
        let (primary_audio, primary_video) = class_port_receivers().await;
        drop(primary_video);
        let mut args = contrib_status_args();
        args.relay_primary_target = Some(primary_audio.local_addr().unwrap());
        args.relay_primary_bind = Some(reserved_loopback_addr());
        args.relay_deadline_ms = 5_000;
        args.relay_source_first = source_first;
        assert!(args.relay_audio_inline);
        let relay = RelaySessionPublisher::new(&args, Arc::clone(telemetry))
            .await
            .unwrap()
            .unwrap();
        (relay, primary_audio)
    }

    /// A part dropped by a full lane backlog is not counted as delivered:
    /// the lane records it failed.
    #[tokio::test]
    async fn a_full_lane_backlog_drops_the_part_and_records_the_lane_failed() {
        for source_first in [true, false] {
            let telemetry = Arc::new(IngestTelemetry::default());
            let (relay, primary_audio) = inline_audio_publisher(&telemetry, source_first).await;
            // A part of the lane is waiting, and the backlog takes no more.
            relay.primary.backlog.pending.fetch_add(1, Ordering::AcqRel);
            let (full, _held) = std::sync::mpsc::sync_channel(0);
            assert!(relay.primary.backlog.sender.set(full).is_ok());
            let stream = 91_000_000_070;
            relay
                .publish_part_object(
                    &test_audio_object(stream, 64),
                    Some((stream, 0)),
                    PublicationClass::Audio,
                )
                .await
                .unwrap();
            let snapshot = telemetry.snapshot().relay_session;
            assert_eq!(
                snapshot.lane_backlog_parts, 1,
                "source_first={source_first}"
            );
            assert_eq!(snapshot.lane_backlog_dropped, 1);
            assert_eq!(snapshot.primary_lane_objects_succeeded, 0);
            assert_eq!(snapshot.primary_lane_objects_failed, 1);
            assert_silent(&primary_audio).await;
        }
    }

    /// A backlog part whose deadline passed is not sent, and the lane
    /// records it failed and its symbols expired.
    #[tokio::test]
    async fn an_expired_backlog_part_is_counted_and_records_the_lane_failed() {
        let telemetry = Arc::new(IngestTelemetry::default());
        let (relay, primary_audio) = inline_audio_publisher(&telemetry, true).await;
        let (socket, peer) = relay.primary.class_socket(PublicationClass::Audio);
        let part = BacklogPart {
            wires: vec![
                (MediaDatagramRole::Source, Bytes::from(vec![1_u8; 32])),
                (MediaDatagramRole::Repair, Bytes::from(vec![2_u8; 32])),
            ],
            sources: 1,
            deadline: MediaDeadline::from_micros(now_unix_us() - 1_000),
            journal: None,
            part: None,
            sent_first: None,
            stage: HopStage::Sent,
            peer,
            path: RelayCarrierPath::Primary,
            records_outcome: true,
        };
        send_backlog_part(socket, &telemetry, part);
        let snapshot = telemetry.snapshot().relay_session;
        assert_eq!(snapshot.primary_lane_objects_failed, 1);
        assert_eq!(snapshot.primary_lane_objects_succeeded, 0);
        assert_eq!(telemetry.relay_expired_symbols.load(Ordering::Relaxed), 2);
        assert_silent(&primary_audio).await;

        // A live part is sent, and the lane records it delivered.
        let part = BacklogPart {
            wires: vec![(MediaDatagramRole::Source, Bytes::from(vec![3_u8; 32]))],
            sources: 1,
            deadline: MediaDeadline::from_micros(now_unix_us() + 1_000_000),
            journal: None,
            part: None,
            sent_first: None,
            stage: HopStage::Sent,
            peer,
            path: RelayCarrierPath::Primary,
            records_outcome: true,
        };
        send_backlog_part(socket, &telemetry, part);
        let mut wire = [0_u8; 64];
        let (len, _) = timeout(Duration::from_secs(2), primary_audio.recv_from(&mut wire))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(&wire[..len], &[3_u8; 32]);
        assert_eq!(
            telemetry
                .snapshot()
                .relay_session
                .primary_lane_objects_succeeded,
            1
        );
    }

    /// A queued part's lane outcome is recorded by the backlog when it has
    /// sent the part, not when the part was queued.
    #[tokio::test]
    async fn a_queued_part_is_recorded_delivered_once_the_backlog_sent_it() {
        for source_first in [true, false] {
            let telemetry = Arc::new(IngestTelemetry::default());
            let (relay, primary_audio) = inline_audio_publisher(&telemetry, source_first).await;
            relay.primary.backlog.pending.fetch_add(1, Ordering::AcqRel);
            let stream = 91_000_000_075;
            let object = test_audio_object(stream, 64);
            let outcome = relay
                .publish_part_object(&object, Some((stream, 0)), PublicationClass::Audio)
                .await
                .unwrap();
            receive_relay_datagrams(
                &primary_audio,
                outcome.source_symbols + outcome.repair_symbols,
            )
            .await;
            for _ in 0..200 {
                if telemetry
                    .snapshot()
                    .relay_session
                    .primary_lane_objects_succeeded
                    == 1
                {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
            let snapshot = telemetry.snapshot().relay_session;
            assert_eq!(
                snapshot.primary_lane_objects_succeeded, 1,
                "source_first={source_first}"
            );
            assert_eq!(snapshot.primary_lane_objects_failed, 0);
        }
    }

    /// An audio part never waits for the video encoder: the classes have
    /// encoders of their own, and a 4K encode that holds the video one does
    /// not hold an audio part back. This is the inline hot path with
    /// relay-session's encoders (`--relay-source-first false`), the one
    /// that takes an encoder lock: holding the audio encoder does hold the
    /// part, so the lock is on the path this test exercises. (Source-first
    /// parts take no lock at all:
    /// source_first_parts_never_wait_for_an_encoder_lock.)
    #[tokio::test]
    async fn an_audio_part_does_not_wait_for_a_busy_video_encoder() {
        let telemetry = Arc::new(IngestTelemetry::default());
        let (relay, primary_audio) = inline_audio_publisher(&telemetry, false).await;
        assert!(relay.source_encoder(PublicationClass::Audio).is_none());
        assert!(!std::ptr::eq(
            relay.encoder(PublicationClass::Audio),
            relay.encoder(PublicationClass::Video)
        ));
        {
            let held_audio = relay.encoder(PublicationClass::Audio).lock().await;
            let object = test_audio_object(91_000_000_081, 64);
            assert!(
                timeout(
                    Duration::from_millis(100),
                    relay.publish_part_object(
                        &object,
                        Some((91_000_000_081, 0)),
                        PublicationClass::Audio
                    ),
                )
                .await
                .is_err(),
                "the audio part did not take the audio encoder's lock"
            );
            drop(held_audio);
        }
        let held = relay.encoder(PublicationClass::Video).lock().await;
        let stream = 91_000_000_080;
        let object = test_audio_object(stream, 64);
        let outcome = timeout(
            Duration::from_secs(1),
            relay.publish_part_object(&object, Some((stream, 0)), PublicationClass::Audio),
        )
        .await
        .expect("an audio part waited for the video encoder")
        .unwrap();
        receive_relay_datagrams(
            &primary_audio,
            outcome.source_symbols + outcome.repair_symbols,
        )
        .await;
        drop(held);
        assert_eq!(telemetry.snapshot().relay_session.lane_backlog_parts, 0);
    }

    /// With class lanes, a 4K part in flight on the same lanes goes to the
    /// video port from the video socket, and an audio part published beside
    /// it leaves at once from the audio socket, with nothing queued. (The
    /// socket-buffer half of the property, a 4K burst filling a send
    /// buffer, cannot be produced on loopback, whose sends never return
    /// EAGAIN; the sockets are separate, which the class lane test checks.)
    #[tokio::test]
    async fn an_audio_part_leaves_beside_a_4k_part_in_flight() {
        let (primary_audio, primary_video) = class_port_receivers().await;
        let mut args = contrib_status_args();
        args.relay_primary_target = Some(primary_audio.local_addr().unwrap());
        args.relay_primary_bind = Some(reserved_loopback_addr());
        args.relay_deadline_ms = 5_000;
        args.relay_class_lanes = true;
        let telemetry = Arc::new(IngestTelemetry::default());
        let relay = RelaySessionPublisher::new(&args, Arc::clone(&telemetry))
            .await
            .unwrap()
            .unwrap();
        let video_stream = 91_000_000_090;
        let video = test_video_object_of(video_stream, 0, 3, 60_000);
        let audio_stream = 91_000_000_091;
        let audio = test_audio_object(audio_stream, 64);
        let primary_video = Arc::new(primary_video);
        let video_reader = {
            let primary_video = Arc::clone(&primary_video);
            tokio::spawn(async move {
                let mut wire = [0_u8; 2_048];
                let mut count = 0_usize;
                while timeout(
                    Duration::from_millis(300),
                    primary_video.recv_from(&mut wire),
                )
                .await
                .is_ok()
                {
                    count += 1;
                }
                count
            })
        };
        let (video_outcome, audio_outcome) = tokio::join!(
            relay.publish_part_object(&video, Some((video_stream, 0)), PublicationClass::Video),
            relay.publish_part_object(&audio, Some((audio_stream, 0)), PublicationClass::Audio),
        );
        let (video_outcome, audio_outcome) = (video_outcome.unwrap(), audio_outcome.unwrap());
        let audio_symbols = receive_relay_datagrams(
            &primary_audio,
            audio_outcome.source_symbols + audio_outcome.repair_symbols,
        )
        .await;
        assert!(audio_symbols
            .iter()
            .all(|(symbol, _)| symbol.object_key == *audio.key()));
        assert_silent(&primary_audio).await;
        assert_eq!(telemetry.snapshot().relay_session.lane_backlog_parts, 0);
        // Only the video port had the 4K part (the primary carries its
        // source set; without a secondary, its repair too).
        let video_count = video_reader.await.unwrap();
        assert_eq!(
            video_count,
            video_outcome.source_symbols + video_outcome.repair_symbols
        );
    }

    /// The publisher's streams share no lock: an audio part is published
    /// while another stream's state (or the video start clocks) is locked.
    #[tokio::test]
    // The locks are held across the publish on purpose: it must not wait.
    #[allow(clippy::await_holding_lock)]
    async fn an_audio_part_does_not_wait_for_another_streams_lock() {
        let sink = Arc::new(RecordingPublicationSink::default());
        let inner: Arc<dyn Fmp4PartPublisher> = sink.clone();
        let (publisher, _receivers) = test_sharded_publisher(true);
        let publisher = BoundedFmp4Publisher {
            inline_audio: Some(inner),
            ..publisher
        };
        let video_stream = 91_000_000_100;
        let video = test_fmp4_part(
            video_stream,
            0,
            true,
            3,
            1,
            Some(Bytes::from_static(b"init")),
            Bytes::from_static(b"h264"),
            now_unix_ns(),
        );
        publisher.publish_fmp4_part(video).await.unwrap();
        let entry = publisher.streams.entry(video_stream);
        let held = entry.publication();
        let held_starts = publisher.streams.video_starts();
        let audio_stream = 91_000_000_101;
        timeout(
            Duration::from_secs(1),
            publisher.publish_opaque_part(test_opaque_part(audio_stream)),
        )
        .await
        .expect("an audio part waited for a video stream's lock")
        .unwrap();
        drop(held_starts);
        drop(held);
        assert_eq!(*sink.published.lock().unwrap(), [(audio_stream, 0)]);
    }

    fn audio_hot_args(workers: usize) -> Args {
        let mut args = contrib_status_args();
        args.audio_hot_thread = true;
        args.audio_hot_cpu = -1;
        args.audio_hot_fifo_priority = 0;
        args.audio_hot_workers = workers;
        args
    }

    #[test]
    fn audio_hot_threads_default_to_one_per_format() {
        let mut args = audio_hot_args(0);
        args.daw_hls_formats = vec![
            DawHlsFormat::Flac,
            DawHlsFormat::Pcm,
            DawHlsFormat::Opus,
            DawHlsFormat::Flac,
        ];
        assert_eq!(audio_hot_thread_count(&args), 3);
        args.audio_hot_workers = 2;
        assert_eq!(audio_hot_thread_count(&args), 2);
        args.audio_hot_workers = 0;
        args.daw_hls_formats = vec![DawHlsFormat::Opus];
        assert_eq!(audio_hot_thread_count(&args), 1);
        let cpus = std::thread::available_parallelism().map_or(1, std::num::NonZeroUsize::get);
        args.daw_hls_formats = vec![DawHlsFormat::Flac, DawHlsFormat::Pcm, DawHlsFormat::Opus];
        args.audio_hot_cpu = i32::try_from(cpus - 1).unwrap();
        assert_eq!(audio_hot_thread_count(&args), 1);
    }

    /// The receive and recovery run on the first audio hot thread, each
    /// rendition worker on the next thread in turn from the second, and the
    /// threads' blocking pools run with normal scheduling.
    #[tokio::test]
    async fn audio_hot_threads_spread_the_renditions_over_their_threads() {
        let thread_name = || async { std::thread::current().name().map(str::to_owned) };
        assert!(AudioHotRuntime::start(&{
            let mut args = audio_hot_args(3);
            args.audio_hot_thread = false;
            args
        })
        .unwrap()
        .is_none());
        let hot = AudioHotRuntime::start(&audio_hot_args(3)).unwrap().unwrap();
        assert_eq!(hot.handles.len(), 3);
        let first = spawn_audio_hot_task(Some(&hot), thread_name())
            .await
            .unwrap();
        assert_eq!(first.as_deref(), Some("audio-hot-0"));
        let renditions = hot.rendition_runtimes().unwrap();
        let mut names = Vec::new();
        for _ in 0..3 {
            names.push(renditions.next_handle().spawn(thread_name()).await.unwrap());
        }
        assert_eq!(
            names,
            [
                Some("audio-hot-1".to_owned()),
                Some("audio-hot-2".to_owned()),
                Some("audio-hot-0".to_owned())
            ]
        );
        let blocking_fifo = spawn_audio_hot_task(Some(&hot), async {
            tokio::task::spawn_blocking(class_lanes::current_thread_is_fifo)
                .await
                .unwrap()
        })
        .await
        .unwrap();
        assert!(!blocking_fifo);
        // Without the hot threads the work is a task of the caller's runtime.
        let name = spawn_audio_hot_task(None, thread_name()).await.unwrap();
        assert_ne!(name.as_deref(), Some("audio-hot-0"));
        drop(hot);
    }

    /// The DAW media receive runs on the audio hot thread from a socket
    /// moved to its runtime, and hands AEP1 datagrams to the recovery.
    #[tokio::test]
    async fn the_daw_receive_runs_on_the_audio_hot_thread() {
        let hot = AudioHotRuntime::start(&audio_hot_args(1)).unwrap().unwrap();
        let socket = bind_daw_media_udp_socket("127.0.0.1:0".parse().unwrap())
            .await
            .unwrap();
        let bind = socket.local_addr().unwrap();
        let socket = socket.into_std().unwrap();
        let args = contrib_status_args();
        let telemetry = Arc::new(IngestTelemetry::default());
        let forwarder = Arc::new(MeshForwarder::new(&args, telemetry).await.unwrap());
        let (hls_tx, mut hls_rx) = audio_epoch_hls_channel(16);
        let (shutdown_tx, shutdown_rx) = watch::channel(());
        let task = spawn_audio_hot_task(Some(&hot), async move {
            let socket = UdpSocket::from_std(socket)?;
            assert_eq!(
                std::thread::current().name(),
                Some("audio-hot-0"),
                "the receive runs on the audio hot thread"
            );
            run_daw_media_udp_ingest(socket, forwarder, Some(hls_tx), None, shutdown_rx).await
        });
        let sender = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
        // The receive hands anything with the AEP1 magic to the recovery.
        let datagram = [MULTICHANNEL_AUDIO_TRANSPORT_MAGIC, &[1, 2, 3, 4]].concat();
        let received = timeout(Duration::from_secs(2), async {
            loop {
                sender.send_to(&datagram, bind).unwrap();
                if let Ok(Some(received)) = timeout(Duration::from_millis(50), hls_rx.recv()).await
                {
                    break received;
                }
            }
        })
        .await
        .expect("no AEP1 datagram reached the recovery");
        assert_eq!(received.bytes.as_ref(), datagram.as_slice());
        let _ = shutdown_tx.send(());
        task.await.unwrap().unwrap();
    }
}
