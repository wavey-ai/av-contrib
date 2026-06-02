use anyhow::{bail, Context, Result};
use av_contrib::fmp4_bridge::{
    Fmp4PartPublisher, Fmp4Segmenter, PublishedFmp4Part, TimestampInput, TsFmp4Bridge,
    DEFAULT_MIN_PART_MS,
};
use av_contrib::{codec_name, MediaAccessUnitParams};
use bytes::{Bytes, BytesMut};
use clap::{Parser, ValueEnum};
use futures_util::StreamExt;
use hls::{HlsHandler, HlsRouter};
use http::{Method, Request, StatusCode};
use raptorq_datagram_fec::{MediaFecEncoder, MediaFrame, MediaFrameMetadata, DEFAULT_SYMBOL_SIZE};
use raptorq_fec_transport::FecDatagramEncoder;
use rist_core_pure::{packet::rtcp::NackMode, time::ntp_now, ReceivedPayload};
use rist_mio_pure::{MainMioReceiver, SimpleMioReceiver};
use rtmp_ingress::ingress::start_rtmp_listener;
use rtmp_ingress::{RtmpIngestEvent, RtmpStreamInfo};
use serde::Serialize;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::io;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc,
};
use std::time::{Duration, Instant};
use tokio::net::UdpSocket;
use tokio::sync::{watch, Mutex};
use tokio::time::{interval, MissedTickBehavior};
use tracing::{debug, info, trace, warn};
use upload_response::{
    SrtIngest as UploadSrtIngest, TailSlot, UploadResponseConfig, UploadResponseService,
};
use web_service::{
    load_default_tls_base64, load_tls_base64_from_paths, BodyStream, H2H3Server, HandlerResponse,
    HandlerResult, Router, Server, ServerBuilder, ServerError, StreamWriter, WebSocketHandler,
    WebTransportHandler,
};

const DEFAULT_FLOW_ID: u32 = 0x7273_7401;
const MAX_RIST_DRAIN_PER_TICK: usize = 128;
const MEDIA_ACCESS_UNIT_PATH: &str = "/media/access-unit";
const RIST_POLL_MS: u64 = 1;
const RIST_REORDER_WAIT_MS: u64 = 80;
const RIST_REORDER_MAX_PENDING: usize = 512;
const RTCP_INTERVAL_MS: u64 = 20;
const SRT_HLS_WORKER_ID: &str = "av-contrib-srt-fmp4-bridge";
const HLS_BRIDGE_POLL_MS: u64 = 5;

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

    #[arg(long, value_parser = parse_u32_auto, default_value_t = DEFAULT_FLOW_ID)]
    rist_flow_id: u32,

    #[arg(long)]
    srt_bind: Option<SocketAddr>,

    #[arg(long)]
    rtmp_bind: Option<SocketAddr>,

    #[arg(long, default_value_t = DEFAULT_MIN_PART_MS)]
    fmp4_part_ms: u32,

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

#[derive(Debug, Clone, Copy)]
struct RistIngestConfig {
    bind: SocketAddr,
    profile: RistProfile,
    flow_id: u32,
    output_stream_id: u64,
    output_stream_idx: usize,
    min_part_ms: u32,
}

enum RistReceiver {
    Simple(SimpleMioReceiver),
    Main(MainMioReceiver),
}

impl RistReceiver {
    fn bind(profile: RistProfile, addr: SocketAddr, flow_id: u32) -> io::Result<Self> {
        match profile {
            RistProfile::Simple => {
                SimpleMioReceiver::bind(addr, flow_id, "av-contrib", NackMode::Range)
                    .map(Self::Simple)
            }
            RistProfile::Main => {
                MainMioReceiver::bind(addr, flow_id, "av-contrib", NackMode::Range).map(Self::Main)
            }
        }
    }

    fn try_recv_payload(
        &mut self,
        buf: &mut [u8],
    ) -> io::Result<Option<(SocketAddr, ReceivedPayload)>> {
        match self {
            Self::Simple(receiver) => receiver.try_recv_payload(buf),
            Self::Main(receiver) => receiver.try_recv_payload(buf),
        }
    }

    fn poll_rtcp_and_send(&mut self, now: Instant, now_ntp: u64) -> io::Result<()> {
        match self {
            Self::Simple(receiver) => receiver.poll_rtcp_and_send(now, now_ntp).map(|_| ()),
            Self::Main(receiver) => receiver.poll_rtcp_and_send(now, now_ntp).map(|_| ()),
        }
    }
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
}

impl MeshForwarder {
    async fn new(args: &Args) -> Result<Self> {
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
        })
    }

    fn allocate_media_sequence(&self) -> u64 {
        self.next_media_sequence.fetch_add(1, Ordering::Relaxed)
    }

    async fn forward_stream_slot(&self, stream_id: u64, bytes: &[u8]) -> Result<usize> {
        if bytes.is_empty() {
            return Ok(0);
        }
        let datagrams = {
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
                .context("failed to encode stream slot for mesh RaptorQ-FEC")?
        };
        for datagram in &datagrams {
            self.byte_socket
                .send_to(datagram, self.byte_target)
                .await
                .with_context(|| {
                    format!("failed to forward stream slot to {}", self.byte_target)
                })?;
        }
        Ok(datagrams.len())
    }

    async fn forward_media_access_unit(
        &self,
        metadata: MediaFrameMetadata,
        payload: &[u8],
    ) -> Result<usize> {
        let datagrams = {
            let mut encoder = self.media_encoder.lock().await;
            encoder
                .encode_frame(MediaFrame { metadata, payload })
                .context("failed to encode media access unit for mesh RaptorQ-FEC")?
                .datagrams
        };
        for datagram in &datagrams {
            self.media_socket
                .send_to(datagram, self.media_target)
                .await
                .with_context(|| {
                    format!(
                        "failed to forward media access unit to {}",
                        self.media_target
                    )
                })?;
        }
        Ok(datagrams.len())
    }
}

#[async_trait::async_trait]
impl Fmp4PartPublisher for MeshForwarder {
    async fn publish_fmp4_part(&self, part: PublishedFmp4Part) -> std::result::Result<(), String> {
        self.forward_stream_slot(part.stream_id, &part.bytes)
            .await
            .map(|_| ())
            .map_err(|error| error.to_string())
    }
}

struct RistPayloadReorderer {
    next_sequence: Option<u32>,
    pending: BTreeMap<u32, (SocketAddr, ReceivedPayload)>,
    gap_started_at: Option<Instant>,
}

impl RistPayloadReorderer {
    fn new() -> Self {
        Self {
            next_sequence: None,
            pending: BTreeMap::new(),
            gap_started_at: None,
        }
    }

    fn insert(
        &mut self,
        peer: SocketAddr,
        payload: ReceivedPayload,
        now: Instant,
    ) -> Vec<(SocketAddr, ReceivedPayload)> {
        if payload.duplicate {
            return Vec::new();
        }

        let sequence = payload.sequence;
        if self.next_sequence.is_some_and(|next| sequence < next) {
            trace!(
                peer = %peer,
                sequence,
                "dropping late RIST payload behind reorder window"
            );
            return Vec::new();
        }

        self.next_sequence.get_or_insert(sequence);
        self.pending.entry(sequence).or_insert((peer, payload));
        self.drain_ready(now)
    }

    fn drain_due(&mut self, now: Instant) -> Vec<(SocketAddr, ReceivedPayload)> {
        if self.pending.is_empty() {
            self.gap_started_at = None;
            return Vec::new();
        }

        let gap_elapsed = self
            .gap_started_at
            .map(|started| now.saturating_duration_since(started))
            .unwrap_or_default();
        if gap_elapsed >= Duration::from_millis(RIST_REORDER_WAIT_MS)
            || self.pending.len() >= RIST_REORDER_MAX_PENDING
        {
            let first_pending = *self.pending.keys().next().unwrap();
            if let Some(next) = self.next_sequence {
                warn!(
                    next_sequence = next,
                    first_pending_sequence = first_pending,
                    pending_payloads = self.pending.len(),
                    waited_ms = gap_elapsed.as_millis(),
                    "RIST reorder gap timed out; releasing recovered stream with missing payloads"
                );
            }
            self.next_sequence = Some(first_pending);
            self.gap_started_at = None;
        }

        self.drain_ready(now)
    }

    fn drain_ready(&mut self, now: Instant) -> Vec<(SocketAddr, ReceivedPayload)> {
        let mut ready = Vec::new();
        while let Some(next) = self.next_sequence {
            let Some((peer, payload)) = self.pending.remove(&next) else {
                if self.pending.is_empty() {
                    self.gap_started_at = None;
                } else {
                    self.gap_started_at.get_or_insert(now);
                }
                break;
            };
            ready.push((peer, payload));
            self.next_sequence = Some(next.wrapping_add(1));
            self.gap_started_at = None;
        }
        ready
    }
}

async fn run_rist_ingest(
    mut receiver: RistReceiver,
    config: RistIngestConfig,
    playlists: Arc<playlists::Playlists>,
    publisher: Arc<dyn Fmp4PartPublisher>,
    mut shutdown_rx: watch::Receiver<()>,
) -> Result<()> {
    let mut bridge = TsFmp4Bridge::new_with_publisher(
        config.output_stream_id,
        config.output_stream_idx,
        playlists,
        config.min_part_ms,
        Some(publisher),
    );
    let mut buf = vec![0u8; 65_536];
    let mut poll = interval(Duration::from_millis(RIST_POLL_MS));
    poll.set_missed_tick_behavior(MissedTickBehavior::Skip);
    let mut last_rtcp = Instant::now();
    let mut reorderer = RistPayloadReorderer::new();

    loop {
        tokio::select! {
            _ = shutdown_rx.changed() => {
                bridge.finish().await;
                info!("RIST contributor frontend shutting down");
                return Ok(());
            }
            _ = poll.tick() => {
                let now = Instant::now();
                for _ in 0..MAX_RIST_DRAIN_PER_TICK {
                    match receiver.try_recv_payload(&mut buf) {
                        Ok(Some((peer, payload))) => {
                            let ready = reorderer.insert(peer, payload, now);
                            for (peer, payload) in ready {
                                push_rist_payload(&mut bridge, peer, payload).await;
                            }
                        }
                        Ok(None) => break,
                        Err(error) if error.kind() == io::ErrorKind::WouldBlock => break,
                        Err(error) => {
                            warn!(bind = %config.bind, error = %error, "RIST receive failed");
                            break;
                        }
                    }
                }

                let now = Instant::now();
                for (peer, payload) in reorderer.drain_due(now) {
                    push_rist_payload(&mut bridge, peer, payload).await;
                }

                if now.duration_since(last_rtcp) >= Duration::from_millis(RTCP_INTERVAL_MS) {
                    if let Err(error) = receiver.poll_rtcp_and_send(now, ntp_now()) {
                        if error.kind() != io::ErrorKind::WouldBlock {
                            debug!(error = %error, "RIST RTCP poll failed");
                        }
                    }
                    last_rtcp = now;
                }
            }
        }
    }
}

async fn push_rist_payload(bridge: &mut TsFmp4Bridge, peer: SocketAddr, payload: ReceivedPayload) {
    if payload.recovered {
        trace!(
            peer = %peer,
            sequence = payload.sequence,
            "RIST payload recovered by protocol repair"
        );
    }
    bridge.push_ts(Bytes::from(payload.payload)).await;
}

struct UploadTsBridgeState {
    output_stream_id: u64,
    last_seen: usize,
    reader_registered: bool,
    bridge: TsFmp4Bridge,
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
                for (_, mut state) in bridges.drain() {
                    state.bridge.finish().await;
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
                        state.bridge.finish().await;
                        playlists.fin(state.output_stream_id);
                    }
                }

                for stream in streams {
                    let stream_id = stream.stream_id;
                    if !bridges.contains_key(&stream_id) {
                        let public_stream_id = if bridges.is_empty() {
                            output_stream_id
                        } else {
                            stream_id
                        };
                        let public_stream_idx = if public_stream_id == output_stream_id {
                            output_stream_idx
                        } else {
                            resolve_output_stream_idx(&playlists, public_stream_id).await
                        };
                        bridges.insert(
                            stream_id,
                            UploadTsBridgeState {
                                output_stream_id: public_stream_id,
                                last_seen: 0,
                                reader_registered: false,
                                bridge: TsFmp4Bridge::new_with_publisher(
                                    public_stream_id,
                                    public_stream_idx,
                                    playlists.clone(),
                                    min_part_ms,
                                    Some(publisher.clone()),
                                ),
                            },
                        );
                    }
                    let state = bridges.get_mut(&stream_id).expect("bridge state");

                    if !state.reader_registered {
                        service
                            .register_request_reader(stream_id, SRT_HLS_WORKER_ID)
                            .await;
                        state.reader_registered = true;
                    }

                    if stream.request_last <= state.last_seen {
                        continue;
                    }

                    let mut stream_ended = false;
                    for slot in (state.last_seen + 1)..=stream.request_last {
                        match service.tail_request(stream_id, slot).await {
                            Some(TailSlot::Headers(headers)) => {
                                debug!(
                                    stream_id,
                                    path = %String::from_utf8_lossy(&headers.path),
                                    "SRT stream headers"
                                );
                            }
                            Some(TailSlot::Body(data)) => {
                                state.bridge.push_ts(data).await;
                            }
                            Some(TailSlot::End) => {
                                state.bridge.finish().await;
                                playlists.fin(state.output_stream_id);
                                stream_ended = true;
                            }
                            Some(TailSlot::Control(_)) | None => {}
                        }
                        service
                            .mark_request_reader_position(stream_id, SRT_HLS_WORKER_ID, slot)
                            .await;
                    }

                    state.last_seen = stream.request_last;
                    if stream_ended {
                        bridges.remove(&stream_id);
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
                if !segmenters.contains_key(&stream.id) {
                    let output_stream_id = rtmp_output_stream_id(
                        &stream,
                        fallback_output_stream_id,
                        segmenters.is_empty(),
                    );
                    let output_stream_idx =
                        resolve_output_stream_idx(&playlists, output_stream_id).await;
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

                if let Some(state) = segmenters.get_mut(&stream.id) {
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

struct ContribRouter {
    forwarder: Arc<MeshForwarder>,
    default_stream_id: u64,
    hls_router: Arc<HlsRouter>,
}

impl ContribRouter {
    fn new(
        forwarder: Arc<MeshForwarder>,
        default_stream_id: u64,
        hls_router: Arc<HlsRouter>,
    ) -> Self {
        Self {
            forwarder,
            default_stream_id,
            hls_router,
        }
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
                    b"av-contrib\n\nPOST /ingest?stream_id=... publishes arbitrary stream bytes\nPOST /media/access-unit forwards detected media access units\nGET /<stream_id>/stream.m3u8 serves local LL-HLS\nGET /up checks health\n",
                )),
                Some("text/plain; charset=utf-8"),
            )),
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
            _ => self.hls_router.route(req).await,
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
            let json =
                serde_json::to_vec(&ack).map_err(|err| ServerError::Handler(Box::new(err)))?;
            return Ok(response(
                StatusCode::ACCEPTED,
                Some(Bytes::from(json)),
                Some("application/json"),
            ));
        }

        self.hls_router.route(req).await
    }

    fn has_body_handler(&self, path: &str) -> bool {
        path == "/ingest" || path == MEDIA_ACCESS_UNIT_PATH
    }

    fn is_streaming(&self, path: &str) -> bool {
        self.hls_router.is_streaming(path)
    }

    async fn route_stream(
        &self,
        req: Request<()>,
        stream_writer: Box<dyn StreamWriter>,
    ) -> HandlerResult<()> {
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
    let forwarder = Arc::new(MeshForwarder::new(&args).await?);
    let publisher: Arc<dyn Fmp4PartPublisher> = forwarder.clone();
    let (playlists, chunk_cache, m3u8_cache) = playlists::Playlists::new(playlist_options(&args));
    let hls_router =
        Arc::new(HlsRouter::new().add_handler(Box::new(HlsHandler::new(chunk_cache, m3u8_cache))));
    let (shutdown_tx, shutdown_rx) = watch::channel(());
    let rist_task = if let Some(bind) = args.rist_bind {
        let output_stream_idx = resolve_output_stream_idx(&playlists, args.rist_stream_id).await;
        let config = RistIngestConfig {
            bind,
            profile: args.rist_profile,
            flow_id: args.rist_flow_id,
            output_stream_id: args.rist_stream_id,
            output_stream_idx,
            min_part_ms: args.fmp4_part_ms,
        };
        let receiver = RistReceiver::bind(config.profile, config.bind, config.flow_id)
            .with_context(|| format!("failed to bind RIST contributor frontend on {bind}"))?;
        info!(
            bind = %config.bind,
            profile = config.profile.as_str(),
            flow_id = format_args!("0x{:08x}", config.flow_id),
            output_stream_id = config.output_stream_id,
            output_stream_idx,
            "RIST contributor frontend listening"
        );
        Some(tokio::spawn(run_rist_ingest(
            receiver,
            config,
            playlists.clone(),
            publisher.clone(),
            shutdown_rx.clone(),
        )))
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
    println!(
        "ll-hls:  https://127.0.0.1:{}/{}/stream.m3u8",
        args.http_port, args.rist_stream_id
    );
    println!("bytes:   udp+stream-fec://{}", args.mesh_fec_target);
    println!("media:   udp+media-fec://{}", args.mesh_media_fec_target);
    if let Some(bind) = args.rist_bind {
        println!(
            "rist:    rist://127.0.0.1:{} profile={} flow_id=0x{:08x} stream_id={}",
            bind.port(),
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
    println!("health:  https://127.0.0.1:{}/up", args.http_port);

    tokio::signal::ctrl_c().await?;
    let _ = handle.shutdown_tx.send(());
    let _ = shutdown_tx.send(());
    if let Some(shutdown) = srt_shutdown {
        let _ = shutdown.send(());
    }
    if let Some(shutdown) = rtmp_shutdown {
        let _ = shutdown.send(());
    }
    let _ = handle.finished_rx.await;
    if let Some(task) = rist_task {
        let _ = task.await;
    }
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
        segment_min_ms: args.fmp4_part_ms.max(1),
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
        assert_eq!(parse_u32_auto("0x72737401").unwrap(), DEFAULT_FLOW_ID);
        assert_eq!(parse_u32_auto("1920168961").unwrap(), DEFAULT_FLOW_ID);
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
    fn rist_reorderer_holds_out_of_order_payloads_until_gap_recovers() {
        let peer = SocketAddr::from((Ipv4Addr::LOCALHOST, 5000));
        let now = Instant::now();
        let mut reorderer = RistPayloadReorderer::new();

        let ready = reorderer.insert(peer, rist_payload(10, false, b"ten"), now);
        assert_eq!(payload_sequences(&ready), vec![10]);

        let ready = reorderer.insert(peer, rist_payload(12, false, b"twelve"), now);
        assert!(ready.is_empty());

        let ready = reorderer.insert(
            peer,
            rist_payload(11, true, b"eleven"),
            now + Duration::from_millis(5),
        );
        assert_eq!(payload_sequences(&ready), vec![11, 12]);
        assert_eq!(ready[0].1.payload, b"eleven");
        assert_eq!(ready[1].1.payload, b"twelve");
    }

    #[test]
    fn rist_reorderer_releases_gap_after_timeout() {
        let peer = SocketAddr::from((Ipv4Addr::LOCALHOST, 5000));
        let now = Instant::now();
        let mut reorderer = RistPayloadReorderer::new();

        assert_eq!(
            payload_sequences(&reorderer.insert(peer, rist_payload(20, false, b"twenty"), now)),
            vec![20]
        );
        assert!(reorderer
            .insert(peer, rist_payload(22, false, b"twenty-two"), now)
            .is_empty());
        assert!(reorderer
            .drain_due(now + Duration::from_millis(RIST_REORDER_WAIT_MS - 1))
            .is_empty());

        let ready = reorderer.drain_due(now + Duration::from_millis(RIST_REORDER_WAIT_MS + 1));
        assert_eq!(payload_sequences(&ready), vec![22]);
        assert_eq!(ready[0].1.payload, b"twenty-two");
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
            rist_flow_id: DEFAULT_FLOW_ID,
            srt_bind: None,
            rtmp_bind: None,
            fmp4_part_ms: DEFAULT_MIN_PART_MS,
            playlist_count: 65,
            playlist_buffer_kb: 800,
        };
        let forwarder = MeshForwarder::new(&args).await.unwrap();
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
    }

    fn rist_payload(sequence: u32, recovered: bool, payload: &[u8]) -> ReceivedPayload {
        ReceivedPayload {
            sequence,
            recovered,
            duplicate: false,
            newly_missing: Vec::new(),
            payload: payload.to_vec(),
        }
    }

    fn payload_sequences(ready: &[(SocketAddr, ReceivedPayload)]) -> Vec<u32> {
        ready
            .iter()
            .map(|(_peer, payload)| payload.sequence)
            .collect()
    }
}
