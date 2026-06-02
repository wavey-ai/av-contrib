use anyhow::{bail, Context, Result};
use av_contrib::{codec_name, MediaAccessUnitParams};
use bytes::{Bytes, BytesMut};
use clap::{Parser, ValueEnum};
use futures_util::StreamExt;
use http::{Method, Request, StatusCode};
use raptorq_datagram_fec::{
    MediaFecEncoder, MediaFrame, MediaFrameMetadata, UdpFecSender, DEFAULT_SYMBOL_SIZE,
};
use rist_core_pure::{packet::rtcp::NackMode, time::ntp_now, ReceivedPayload};
use rist_mio_pure::{MainMioReceiver, SimpleMioReceiver};
use serde::Serialize;
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
use tracing::{debug, info, warn};
use web_service::{
    load_default_tls_base64, load_tls_base64_from_paths, BodyStream, H2H3Server, HandlerResponse,
    HandlerResult, Router, Server, ServerBuilder, ServerError, StreamWriter, WebSocketHandler,
    WebTransportHandler,
};

const DEFAULT_FLOW_ID: u32 = 0x7273_7401;
const MAX_RIST_DRAIN_PER_TICK: usize = 128;
const MEDIA_ACCESS_UNIT_PATH: &str = "/media/access-unit";
const RIST_POLL_MS: u64 = 1;
const RTCP_INTERVAL_MS: u64 = 20;

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
    byte_sender: Arc<Mutex<UdpFecSender>>,
    media_encoder: Arc<Mutex<MediaFecEncoder>>,
    media_socket: Arc<UdpSocket>,
    media_target: SocketAddr,
    next_media_sequence: Arc<AtomicU64>,
}

impl MeshForwarder {
    async fn new(args: &Args) -> Result<Self> {
        let byte_sender = UdpFecSender::new(args.mesh_fec_target)
            .await
            .with_context(|| {
                format!(
                    "failed to create mesh byte FEC sender for {}",
                    args.mesh_fec_target
                )
            })?
            .with_repair_symbols(args.repair_symbols)
            .with_symbol_size(args.symbol_size);
        let media_socket = UdpSocket::bind(local_sender_addr(args.mesh_media_fec_target))
            .await
            .with_context(|| {
                format!(
                    "failed to bind mesh media FEC sender for {}",
                    args.mesh_media_fec_target
                )
            })?;

        Ok(Self {
            byte_sender: Arc::new(Mutex::new(byte_sender)),
            media_encoder: Arc::new(Mutex::new(MediaFecEncoder::default())),
            media_socket: Arc::new(media_socket),
            media_target: args.mesh_media_fec_target,
            next_media_sequence: Arc::new(AtomicU64::new(0)),
        })
    }

    fn allocate_media_sequence(&self) -> u64 {
        self.next_media_sequence.fetch_add(1, Ordering::Relaxed)
    }

    async fn forward_bytes(&self, bytes: &[u8]) -> Result<()> {
        if bytes.is_empty() {
            return Ok(());
        }
        self.byte_sender
            .lock()
            .await
            .send(bytes)
            .await
            .context("failed to forward contributor bytes over mesh RaptorQ-FEC")
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

async fn run_rist_ingest(
    mut receiver: RistReceiver,
    config: RistIngestConfig,
    forwarder: MeshForwarder,
    mut shutdown_rx: watch::Receiver<()>,
) -> Result<()> {
    let mut buf = vec![0u8; 65_536];
    let mut poll = interval(Duration::from_millis(RIST_POLL_MS));
    poll.set_missed_tick_behavior(MissedTickBehavior::Skip);
    let mut last_rtcp = Instant::now();

    loop {
        tokio::select! {
            _ = shutdown_rx.changed() => {
                info!("RIST contributor frontend shutting down");
                return Ok(());
            }
            _ = poll.tick() => {
                for _ in 0..MAX_RIST_DRAIN_PER_TICK {
                    match receiver.try_recv_payload(&mut buf) {
                        Ok(Some((peer, payload))) => {
                            if payload.duplicate {
                                continue;
                            }
                            if payload.recovered {
                                debug!(peer = %peer, "RIST payload recovered by protocol repair");
                            }
                            if let Err(error) = forwarder.forward_bytes(&payload.payload).await {
                                warn!(peer = %peer, error = %error, "failed to forward RIST payload into mesh FEC");
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

#[derive(Clone)]
struct ContribRouter {
    forwarder: MeshForwarder,
    default_stream_id: u64,
}

impl ContribRouter {
    fn new(forwarder: MeshForwarder, default_stream_id: u64) -> Self {
        Self {
            forwarder,
            default_stream_id,
        }
    }
}

#[derive(Debug, Serialize)]
struct MediaAck {
    stream_id: u64,
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
                    b"av-contrib\n\nPOST /ingest forwards opaque bytes\nPOST /media/access-unit forwards detected media access units\nGET /up checks health\n",
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
                Some(Bytes::from_static(b"use POST or PUT /ingest\n")),
                Some("text/plain"),
            )),
            MEDIA_ACCESS_UNIT_PATH => Ok(response(
                StatusCode::METHOD_NOT_ALLOWED,
                Some(Bytes::from_static(
                    b"use POST or PUT /media/access-unit?stream_id=...&codec=auto\n",
                )),
                Some("text/plain"),
            )),
            _ => Ok(response(StatusCode::NOT_FOUND, None, None)),
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

            let mut chunks = 0u64;
            let mut bytes = 0u64;
            while let Some(next) = body.next().await {
                let chunk = next?;
                if chunk.is_empty() {
                    continue;
                }
                bytes = bytes.saturating_add(chunk.len() as u64);
                chunks = chunks.saturating_add(1);
                self.forwarder
                    .forward_bytes(&chunk)
                    .await
                    .map_err(|err| ServerError::Config(err.to_string()))?;
            }

            return Ok(response(
                StatusCode::ACCEPTED,
                Some(Bytes::from(format!(
                    "forwarded {bytes} bytes in {chunks} chunks\n"
                ))),
                Some("text/plain"),
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

        self.route(req).await
    }

    fn has_body_handler(&self, path: &str) -> bool {
        path == "/ingest" || path == MEDIA_ACCESS_UNIT_PATH
    }

    fn is_streaming(&self, _path: &str) -> bool {
        false
    }

    async fn route_stream(
        &self,
        _req: Request<()>,
        _stream_writer: Box<dyn StreamWriter>,
    ) -> HandlerResult<()> {
        Err(ServerError::Config(
            "streaming endpoints are not enabled".into(),
        ))
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
    let (cert, key) = load_tls(&args)?;
    let forwarder = MeshForwarder::new(&args).await?;
    let (shutdown_tx, shutdown_rx) = watch::channel(());
    let rist_task = if let Some(bind) = args.rist_bind {
        let config = RistIngestConfig {
            bind,
            profile: args.rist_profile,
            flow_id: args.rist_flow_id,
        };
        let receiver = RistReceiver::bind(config.profile, config.bind, config.flow_id)
            .with_context(|| format!("failed to bind RIST contributor frontend on {bind}"))?;
        info!(
            bind = %config.bind,
            profile = config.profile.as_str(),
            flow_id = format_args!("0x{:08x}", config.flow_id),
            "RIST contributor frontend listening"
        );
        Some(tokio::spawn(run_rist_ingest(
            receiver,
            config,
            forwarder.clone(),
            shutdown_rx.clone(),
        )))
    } else {
        None
    };
    let router = Box::new(ContribRouter::new(forwarder, args.stream_id));
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
        "av-contrib ready"
    );
    println!("contrib: https://127.0.0.1:{}", args.http_port);
    println!("bytes:   udp+fec://{}", args.mesh_fec_target);
    println!("media:   udp+media-fec://{}", args.mesh_media_fec_target);
    if let Some(bind) = args.rist_bind {
        println!(
            "rist:    rist://127.0.0.1:{} profile={} flow_id=0x{:08x}",
            bind.port(),
            args.rist_profile.as_str(),
            args.rist_flow_id
        );
    }
    println!("health:  https://127.0.0.1:{}/up", args.http_port);

    tokio::signal::ctrl_c().await?;
    let _ = handle.shutdown_tx.send(());
    let _ = shutdown_tx.send(());
    let _ = handle.finished_rx.await;
    if let Some(task) = rist_task {
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
    use raptorq_datagram_fec::DatagramFecDecoder;
    use rist_mio_pure::MainMioSender;
    use std::net::{Ipv4Addr, SocketAddrV4};
    use tokio::time::timeout;

    #[test]
    fn parses_decimal_and_hex_rist_flow_ids() {
        assert_eq!(parse_u32_auto("0x72737401").unwrap(), DEFAULT_FLOW_ID);
        assert_eq!(parse_u32_auto("1920168961").unwrap(), DEFAULT_FLOW_ID);
    }

    #[tokio::test]
    async fn rist_ingest_forwards_recovered_payloads_over_mesh_fec() {
        let mesh_socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let mesh_target = mesh_socket.local_addr().unwrap();
        let args = Args {
            http_port: 0,
            cert: None,
            key: None,
            mesh_fec_target: mesh_target,
            mesh_media_fec_target: SocketAddr::from((Ipv4Addr::LOCALHOST, 0)),
            stream_id: 1,
            repair_symbols: 1,
            symbol_size: DEFAULT_SYMBOL_SIZE,
            rist_bind: None,
            rist_profile: RistProfile::Main,
            rist_flow_id: DEFAULT_FLOW_ID,
        };
        let forwarder = MeshForwarder::new(&args).await.unwrap();
        let bind = unused_loopback_addr();
        let receiver = RistReceiver::bind(RistProfile::Main, bind, DEFAULT_FLOW_ID).unwrap();
        let config = RistIngestConfig {
            bind,
            profile: RistProfile::Main,
            flow_id: DEFAULT_FLOW_ID,
        };
        let (shutdown_tx, shutdown_rx) = watch::channel(());
        let task = tokio::spawn(run_rist_ingest(receiver, config, forwarder, shutdown_rx));

        send_rist_payload(bind, b"rist-contrib-front").await;

        let mut decoder = DatagramFecDecoder::new();
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

        assert_eq!(payload, Bytes::from_static(b"rist-contrib-front"));
        let _ = shutdown_tx.send(());
        task.await.unwrap().unwrap();
    }

    fn unused_loopback_addr() -> SocketAddr {
        let socket = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
        let addr = socket.local_addr().unwrap();
        drop(socket);
        addr
    }

    async fn send_rist_payload(peer: SocketAddr, payload: &[u8]) {
        let local = SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 0));
        let mut sender = MainMioSender::connect(local, peer, DEFAULT_FLOW_ID, 8192).unwrap();
        let mut feedback_buf = vec![0u8; 65_536];

        loop {
            match sender.send_payload(payload, ntp_now(), Instant::now()) {
                Ok(_) => break,
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                    drive_rist_feedback(&mut sender, &mut feedback_buf);
                    tokio::task::yield_now().await;
                }
                Err(error) => panic!("RIST send failed: {error}"),
            }
        }
        drive_rist_feedback(&mut sender, &mut feedback_buf);
    }

    fn drive_rist_feedback(sender: &mut MainMioSender, buf: &mut [u8]) {
        for _ in 0..32 {
            match sender.try_recv_feedback_and_retransmit(buf) {
                Ok(Some(_)) => {}
                Ok(None) => break,
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => break,
                Err(error) => panic!("RIST feedback failed: {error}"),
            }
        }
    }
}
