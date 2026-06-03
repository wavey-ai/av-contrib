use anyhow::{bail, Context, Result};
use av_contrib::fmp4_bridge::{
    Fmp4PartPublisher, Fmp4Segmenter, MpegTsContinuityIssue, MpegTsPayloadDrop, PublishedFmp4Part,
    TimestampInput, TsFmp4Bridge, DEFAULT_MIN_PART_MS,
};
use av_contrib::{codec_name, MediaAccessUnitParams};
use bytes::{Bytes, BytesMut};
use clap::{Parser, ValueEnum};
use futures_util::StreamExt;
use hls::{HlsHandler, HlsRouter};
use http::{Method, Request, Response, StatusCode};
use raptorq_datagram_fec::{MediaFecEncoder, MediaFrame, MediaFrameMetadata, DEFAULT_SYMBOL_SIZE};
use raptorq_fec_transport::FecDatagramEncoder;
use rtmp_ingress::ingress::start_rtmp_listener;
use rtmp_ingress::{RtmpIngestEvent, RtmpStreamInfo};
use serde::Serialize;
use std::collections::{HashMap, HashSet, VecDeque};
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc, Mutex as StdMutex,
};
use std::time::Duration;
use tokio::net::UdpSocket;
use tokio::sync::{watch, Mutex};
use tokio::time::{interval, MissedTickBehavior};
use tracing::{debug, info, trace};
use upload_response::{
    PureRistIngest as UploadPureRistIngest, PureRistProfile as UploadPureRistProfile,
    RistIngest as UploadRistIngest, RistProfile as UploadRistProfile, SrtIngest as UploadSrtIngest,
    TailSlot, UploadResponseConfig, UploadResponseService,
};
use web_service::{
    load_default_tls_base64, load_tls_base64_from_paths, BodyStream, H2H3Server, HandlerResponse,
    HandlerResult, Router, Server, ServerBuilder, ServerError, StreamWriter, WebSocketHandler,
    WebTransportHandler,
};

const DEFAULT_FLOW_ID: u32 = 0x1122_3344;
const MEDIA_ACCESS_UNIT_PATH: &str = "/media/access-unit";
const CONTRIB_STATUS_PATH: &str = "/api/status";
const CONTRIB_STATUS_EVENTS_PATH: &str = "/api/status/events";
const MESH_FMP4_SLOT_MAGIC: &[u8; 8] = b"AVFMP4S1";
const MESH_FMP4_SLOT_HEADER_LEN: usize = 16;

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
const UPLOAD_RESPONSE_HLS_WORKER_ID: &str = "av-contrib-upload-response-fmp4-bridge";
const HLS_BRIDGE_POLL_MS: u64 = 5;
const DEFAULT_SEGMENT_MS: u32 = 1_000;
const DEFAULT_TARGET_DURATION_MS: u32 = 6_000;
const CONTRIB_ACTIVITY_LIMIT: usize = 64;
const CONTRIB_HLS_RESPONSE_LIMIT: usize = 32;
const CONTRIB_INGEST_SESSION_LIMIT: usize = 48;
const CONTRIB_MIN_STALE_OUTPUT_MS: u64 = 5_000;

#[derive(Debug, Parser)]
#[command(
    name = "av-contrib",
    about = "Run a contributor-facing web-service that forwards bytes into av-mesh"
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

impl From<RistProfile> for UploadRistProfile {
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
    Librist,
}

impl RistBackend {
    fn as_str(self) -> &'static str {
        match self {
            Self::Pure => "pure",
            Self::Librist => "librist",
        }
    }
}

#[derive(Debug, Clone, Copy)]
struct RistIngestConfig {
    bind: SocketAddr,
    profile: RistProfile,
    backend: RistBackend,
    flow_id: u32,
    output_stream_id: u64,
    output_stream_idx: usize,
    min_part_ms: u32,
}

#[derive(Clone)]
struct MeshForwarder {
    byte_socket: Arc<UdpSocket>,
    byte_target: SocketAddr,
    byte_encoders: Arc<Mutex<HashMap<u64, FecDatagramEncoder>>>,
    repair_symbols: u32,
    symbol_size: u16,
    media_encoder: Arc<Mutex<MediaFecEncoder>>,
    media_socket: Arc<UdpSocket>,
    media_target: SocketAddr,
    next_media_sequence: Arc<AtomicU64>,
    telemetry: Arc<IngestTelemetry>,
}

impl MeshForwarder {
    async fn new(args: &Args, telemetry: Arc<IngestTelemetry>) -> Result<Self> {
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

        Ok(Self {
            byte_socket: Arc::new(byte_socket),
            byte_target: args.mesh_fec_target,
            byte_encoders: Arc::new(Mutex::new(HashMap::new())),
            repair_symbols: args.repair_symbols,
            symbol_size: args.symbol_size,
            media_encoder: Arc::new(Mutex::new(MediaFecEncoder::default())),
            media_socket: Arc::new(media_socket),
            media_target: args.mesh_media_fec_target,
            next_media_sequence: Arc::new(AtomicU64::new(0)),
            telemetry,
        })
    }

    fn allocate_media_sequence(&self) -> u64 {
        self.next_media_sequence.fetch_add(1, Ordering::Relaxed)
    }

    async fn forward_stream_slot(&self, stream_id: u64, bytes: &[u8]) -> Result<usize> {
        if bytes.is_empty() {
            return Ok(0);
        }
        let datagrams = match {
            let mut encoders = self.byte_encoders.lock().await;
            let encoder = encoders.entry(stream_id).or_insert_with(|| {
                let mut encoder = FecDatagramEncoder::webtransport_with_stream_prefix(stream_id);
                encoder
                    .fec_encoder_mut()
                    .set_repair_symbols(self.repair_symbols);
                encoder.fec_encoder_mut().set_symbol_size(self.symbol_size);
                encoder
            });
            encoder
                .encode_payload(bytes)
                .context("failed to encode stream slot for mesh RaptorQ-FEC")
        } {
            Ok(datagrams) => datagrams,
            Err(error) => {
                self.telemetry.record_mesh_forward_error(
                    "stream",
                    stream_id,
                    self.byte_target,
                    &error,
                );
                return Err(error);
            }
        };
        let datagram_bytes = datagrams
            .iter()
            .map(|datagram| datagram.len() as u64)
            .sum::<u64>();
        for datagram in &datagrams {
            if let Err(error) = self
                .byte_socket
                .send_to(datagram, self.byte_target)
                .await
                .with_context(|| format!("failed to forward stream slot to {}", self.byte_target))
            {
                self.telemetry.record_mesh_forward_error(
                    "stream",
                    stream_id,
                    self.byte_target,
                    &error,
                );
                return Err(error);
            }
        }
        self.telemetry.record_mesh_forward_success(
            "stream",
            stream_id,
            self.byte_target,
            bytes.len() as u64,
            datagrams.len() as u64,
            datagram_bytes,
        );
        Ok(datagrams.len())
    }

    async fn forward_media_access_unit(
        &self,
        metadata: MediaFrameMetadata,
        payload: &[u8],
    ) -> Result<usize> {
        let stream_id = metadata.stream_id;
        let datagrams = match {
            let mut encoder = self.media_encoder.lock().await;
            encoder
                .encode_frame(MediaFrame { metadata, payload })
                .context("failed to encode media access unit for mesh RaptorQ-FEC")
        } {
            Ok(encoded) => encoded.datagrams,
            Err(error) => {
                self.telemetry.record_mesh_forward_error(
                    "media",
                    stream_id,
                    self.media_target,
                    &error,
                );
                return Err(error);
            }
        };
        let datagram_bytes = datagrams
            .iter()
            .map(|datagram| datagram.len() as u64)
            .sum::<u64>();
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
                self.telemetry.record_mesh_forward_error(
                    "media",
                    stream_id,
                    self.media_target,
                    &error,
                );
                return Err(error);
            }
        }
        self.telemetry.record_mesh_forward_success(
            "media",
            stream_id,
            self.media_target,
            payload.len() as u64,
            datagrams.len() as u64,
            datagram_bytes,
        );
        Ok(datagrams.len())
    }
}

#[async_trait::async_trait]
impl Fmp4PartPublisher for MeshForwarder {
    async fn publish_fmp4_part(&self, part: PublishedFmp4Part) -> std::result::Result<(), String> {
        let payload = encode_mesh_fmp4_slot(part.init.as_ref(), &part.bytes)
            .map_err(|error| error.to_string())?;
        self.forward_stream_slot(part.stream_id, &payload)
            .await
            .map(|_| ())
            .map_err(|error| error.to_string())
    }
}

struct TelemetryFmp4Publisher {
    inner: Arc<dyn Fmp4PartPublisher>,
    telemetry: Arc<IngestTelemetry>,
}

#[async_trait::async_trait]
impl Fmp4PartPublisher for TelemetryFmp4Publisher {
    async fn publish_fmp4_part(&self, part: PublishedFmp4Part) -> std::result::Result<(), String> {
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
    let rist_shutdown = match config.backend {
        RistBackend::Pure => UploadPureRistIngest::new(service.clone())
            .with_profile(config.profile.into())
            .with_flow_id(config.flow_id)
            .start(config.bind)
            .await
            .map_err(|error| {
                anyhow::anyhow!("failed to bind pure Rust RIST contributor frontend: {error}")
            })?,
        RistBackend::Librist => UploadRistIngest::new(service.clone())
            .with_profile(config.profile.into())
            .start(config.bind)
            .await
            .map_err(|error| {
                anyhow::anyhow!("failed to bind librist contributor frontend: {error}")
            })?,
    };
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
        backend = config.backend.as_str(),
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
                    if !bridges.contains_key(&stream_id) {
                        bridges.insert(
                            stream_id,
                            UploadTsBridgeState {
                                output_stream_id: None,
                                output_stream_idx: None,
                                last_seen: 0,
                                reader_registered: false,
                                body_slots: 0,
                                ended: false,
                                bridge: None,
                            },
                        );
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

#[derive(Debug, Default)]
struct IngestTelemetry {
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
    mesh_media_payloads: AtomicU64,
    mesh_media_payload_bytes: AtomicU64,
    mesh_media_datagrams: AtomicU64,
    mesh_media_datagram_bytes: AtomicU64,
    mesh_media_errors: AtomicU64,
    mesh_media_last_unix_ms: AtomicU64,
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
                .then_with(|| left.protocol.cmp(&right.protocol))
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
                media_payloads: self.mesh_media_payloads.load(Ordering::Relaxed),
                media_payload_bytes: self.mesh_media_payload_bytes.load(Ordering::Relaxed),
                media_datagrams: self.mesh_media_datagrams.load(Ordering::Relaxed),
                media_datagram_bytes: self.mesh_media_datagram_bytes.load(Ordering::Relaxed),
                media_errors: self.mesh_media_errors.load(Ordering::Relaxed),
                media_last_unix_ms: nonzero_unix_ms(
                    self.mesh_media_last_unix_ms.load(Ordering::Relaxed),
                ),
                media_last_age_ms: age_from_atomic_ms(now_ms, &self.mesh_media_last_unix_ms),
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
    count <= 3 || count % interval == 0
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
        let mut listeners = Vec::with_capacity(3);
        listeners.push(ListenerStatus::rist(args));
        listeners.push(ListenerStatus::srt(args));
        listeners.push(ListenerStatus::rtmp(args));

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
    let state = if runtime.fmp4.publish_errors > 0 || mesh_forward_errors > 0 {
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
    media_payloads: u64,
    media_payload_bytes: u64,
    media_datagrams: u64,
    media_datagram_bytes: u64,
    media_errors: u64,
    media_last_unix_ms: Option<u64>,
    media_last_age_ms: Option<u64>,
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
}

impl ContribRouter {
    fn new(
        forwarder: Arc<MeshForwarder>,
        default_stream_id: u64,
        hls_router: Arc<HlsRouter>,
        status: Arc<ContribStatusConfig>,
    ) -> Self {
        Self {
            forwarder,
            default_stream_id,
            hls_router,
            status,
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
                    b"av-contrib\n\nPOST /ingest?stream_id=... publishes arbitrary stream bytes\nPOST /media/access-unit forwards detected media access units\nGET /<stream_id>/stream.m3u8 serves local LL-HLS\nGET /api/status returns service status for dashboards\nGET /api/status/events streams service status as SSE\nGET /up checks health\n",
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

            let params = MediaAccessUnitParams::parse(
                req.uri().query(),
                self.default_stream_id,
                now_unix_ms(),
            )
            .map_err(ServerError::Config)?;
            let payload = read_body_bytes(&mut body).await?;
            let sequence = params
                .sequence
                .unwrap_or_else(|| self.forwarder.allocate_media_sequence());
            let metadata = params
                .metadata_for_payload(sequence, &payload)
                .map_err(ServerError::Config)?;
            let datagrams = self
                .forwarder
                .forward_media_access_unit(metadata, &payload)
                .await
                .map_err(|err| ServerError::Config(err.to_string()))?;
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
                .unwrap_or_else(|_| "av_contrib=info,web_service=info".into()),
        )
        .init();

    let args = Args::parse();
    let (cert, key) = load_tls(&args)?;
    let telemetry = Arc::new(IngestTelemetry::default());
    let forwarder = Arc::new(MeshForwarder::new(&args, Arc::clone(&telemetry)).await?);
    let mesh_publisher: Arc<dyn Fmp4PartPublisher> = forwarder.clone();
    let publisher: Arc<dyn Fmp4PartPublisher> = Arc::new(TelemetryFmp4Publisher {
        inner: mesh_publisher,
        telemetry: telemetry.clone(),
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
                    backend: args.rist_backend,
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
    let router = Box::new(ContribRouter::new(
        forwarder.clone(),
        args.stream_id,
        hls_router,
        status,
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

async fn read_body_bytes(body: &mut BodyStream) -> HandlerResult<Bytes> {
    let mut bytes = BytesMut::new();
    while let Some(next) = body.next().await {
        bytes.extend_from_slice(&next?);
    }
    Ok(bytes.freeze())
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
        max_parts_per_segment: 128,
        max_parted_segments: 32,
        segment_min_ms: args.fmp4_segment_ms.max(args.fmp4_part_ms).max(1),
        target_duration_ms: args.hls_target_duration_ms.max(1_000),
        part_target_ms: args.fmp4_part_ms.max(1),
        buffer_size_kb: args.playlist_buffer_kb.max(1),
        init_size_kb: 5,
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
    use raptorq_fec_transport::FecDatagramDecoder;
    use std::net::Ipv4Addr;
    use tokio::time::timeout;

    #[test]
    fn parses_decimal_and_hex_rist_flow_ids() {
        assert_eq!(parse_u32_auto("0x11223344").unwrap(), DEFAULT_FLOW_ID);
        assert_eq!(parse_u32_auto("287454020").unwrap(), DEFAULT_FLOW_ID);
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

    fn contrib_status_args() -> Args {
        Args {
            http_port: 0,
            cert: None,
            key: None,
            mesh_fec_target: SocketAddr::from((Ipv4Addr::LOCALHOST, 22_001)),
            mesh_media_fec_target: SocketAddr::from((Ipv4Addr::LOCALHOST, 22_101)),
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
    async fn stream_slot_forwarder_uses_stream_prefixed_fec() {
        let mesh_socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let mesh_target = mesh_socket.local_addr().unwrap();
        let args = Args {
            http_port: 0,
            cert: None,
            key: None,
            mesh_fec_target: mesh_target,
            mesh_media_fec_target: SocketAddr::from((Ipv4Addr::LOCALHOST, 0)),
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
        forwarder
            .forward_stream_slot(stream_id, b"non-obs-stream-bytes")
            .await
            .unwrap();

        let mut decoder = FecDatagramDecoder::webtransport_with_stream_prefix(stream_id);
        let mut buf = vec![0u8; 65_536];
        let payload = timeout(Duration::from_secs(3), async {
            loop {
                let (len, _peer) = mesh_socket.recv_from(&mut buf).await.unwrap();
                if let Some(payload) = decoder.push_datagram(&buf[..len]).unwrap() {
                    break payload;
                }
            }
        })
        .await
        .unwrap();

        assert_eq!(payload, b"non-obs-stream-bytes");
        let snapshot = telemetry.snapshot();
        assert_eq!(snapshot.mesh_forward.stream_payloads, 1);
        assert!(snapshot.mesh_forward.stream_datagrams > 0);
        assert!(snapshot.mesh_forward.stream_payload_bytes > 0);
        assert!(snapshot.mesh_forward.stream_datagram_bytes > 0);
        assert_eq!(snapshot.mesh_forward.stream_errors, 0);
    }
}
