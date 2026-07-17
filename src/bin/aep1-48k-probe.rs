use anyhow::{bail, Context, Result};
use bytes::{Buf, Bytes};
use clap::{Parser, Subcommand, ValueEnum};
use music_audio_session::{
    MultichannelAudioReceiver, MultichannelAudioSender, MultichannelAudioSessionConfig,
};
use raptorq_datagram_fec::{
    AudioPayloadKind, AudioSampleFormat, MultichannelAudioEpoch, MultichannelAudioFecConfig,
    MultichannelAudioGroup,
};
use raptorq_fec_transport::MultichannelAudioTransportAdapter;
use serde::Serialize;
use soundkit_flac::{FlacFrameConfig, FlacFrameEncoder, FlacProfile};
use std::collections::{HashMap, HashSet};
use std::io;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Once};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpStream, UdpSocket};
use tokio::time::{interval_at, sleep_until, timeout, Instant as TokioInstant, MissedTickBehavior};

const SAMPLE_RATE: u32 = 48_000;
const CHANNELS: u16 = 2;
const FRAME_COUNT: u32 = 240;
const FRAME_DURATION: Duration = Duration::from_millis(5);
const MAX_DATAGRAM_BYTES: usize = 1_200;

#[derive(Debug, Parser)]
#[command(name = "aep1-48k-probe")]
#[command(about = "Generate or receive a deterministic 48 kHz lossless AEP1 stream")]
struct Args {
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    Send {
        #[arg(long)]
        target: SocketAddr,
        #[arg(long, default_value_t = 10)]
        duration_seconds: u64,
        /// Also defines the origin Unix-nanosecond clock. Defaults one second ahead.
        #[arg(long)]
        session_id: Option<u64>,
        #[arg(long, value_enum, default_value_t = ProbePayload::Flac)]
        payload: ProbePayload,
        #[arg(long, default_value_t = 0)]
        group_id: u16,
        #[arg(long, default_value_t = 12)]
        repair_percent: u32,
        #[arg(long, default_value_t = 1)]
        min_repair_symbols: u32,
    },
    ReceiveUdp {
        #[arg(long)]
        relay: SocketAddr,
        #[arg(long)]
        bind: Option<SocketAddr>,
        #[arg(long)]
        session_id: u64,
        #[arg(long, default_value_t = 0)]
        group_id: u16,
        #[arg(long, default_value_t = 10)]
        duration_seconds: u64,
        #[arg(long, default_value_t = 25)]
        deadline_ms: u64,
        #[arg(long, default_value_t = 3)]
        tail_seconds: u64,
    },
    ReceiveWebtransport {
        #[arg(long)]
        edge: SocketAddr,
        #[arg(long, default_value = "local.bitneedle.com")]
        server_name: String,
        /// Additional PEM certificate authority for private/local qualification endpoints.
        #[arg(long)]
        tls_ca: Option<PathBuf>,
        #[arg(long)]
        session_id: u64,
        #[arg(long, default_value_t = 0)]
        group_id: u16,
        #[arg(long, default_value_t = 10)]
        duration_seconds: u64,
        #[arg(long, default_value_t = 250)]
        deadline_ms: u64,
        #[arg(long, default_value_t = 3)]
        tail_seconds: u64,
    },
    ReceiveHls {
        #[arg(long)]
        edge: SocketAddr,
        #[arg(long, default_value = "local.bitneedle.com")]
        server_name: String,
        /// Additional PEM certificate authority for private/local qualification endpoints.
        #[arg(long)]
        tls_ca: Option<PathBuf>,
        #[arg(long, value_enum, default_value_t = HlsTransport::H3)]
        transport: HlsTransport,
        #[arg(long)]
        stream_id: u64,
        #[arg(long)]
        session_id: u64,
        #[arg(long, default_value_t = 10)]
        duration_seconds: u64,
        #[arg(long, default_value_t = 50)]
        part_ms: u64,
        #[arg(long, default_value_t = 1000)]
        deadline_ms: u64,
        #[arg(long, default_value_t = 150)]
        render_buffer_ms: u64,
        #[arg(long, default_value_t = 3)]
        tail_seconds: u64,
    },
}

#[derive(Debug, Clone, Copy, ValueEnum)]
enum ProbePayload {
    Flac,
    Pcm,
}

#[derive(Debug, Clone, Copy, ValueEnum)]
enum HlsTransport {
    H3,
    Http1,
}

impl HlsTransport {
    fn as_str(self) -> &'static str {
        match self {
            Self::H3 => "h3",
            Self::Http1 => "http/1.1",
        }
    }
}

impl ProbePayload {
    fn as_str(self) -> &'static str {
        match self {
            Self::Flac => "flac",
            Self::Pcm => "pcm_s24le",
        }
    }
}

#[derive(Debug, Serialize)]
struct SendReport {
    schema: &'static str,
    lane: &'static str,
    payload: &'static str,
    session_id: u64,
    group_id: u16,
    sample_rate: u32,
    channels: u16,
    frame_count: u32,
    duration_seconds: u64,
    epochs: u64,
    source_datagrams: u64,
    repair_datagrams: u64,
    wire_bytes: u64,
    lossless_payload_bytes: u64,
    pcm_reference_bytes: u64,
    wire_overhead_ratio: f64,
    elapsed_ms: u64,
}

#[derive(Debug, Serialize)]
struct ReceiveReport {
    schema: &'static str,
    lane: &'static str,
    session_id: u64,
    group_id: u16,
    sample_rate: u32,
    duration_seconds: u64,
    expected_epochs: u64,
    received_epochs: u64,
    missing_epochs: u64,
    deadline_ms: u64,
    deadline_misses: u64,
    datagrams_received: u64,
    systematic_shards_received: u64,
    raptorq_shards_recovered: u64,
    duplicate_or_late_epochs: u64,
    wire_bytes: u64,
    /// Capture-to-exact-decode latency at the point the epoch can be rendered.
    latency_ms: Percentiles,
    render_ready_latency_ms: Percentiles,
}

#[derive(Debug, Serialize)]
struct HlsReceiveReport {
    schema: &'static str,
    lane: &'static str,
    transport: &'static str,
    tls_protocol: &'static str,
    tls_certificate_verified: bool,
    persistent_connection: bool,
    connection_setup_ms: Option<f64>,
    stream_id: u64,
    session_id: u64,
    sample_rate: u32,
    duration_seconds: u64,
    part_ms: u64,
    expected_parts: u64,
    received_parts: u64,
    missing_parts: u64,
    deadline_ms: u64,
    deadline_misses: u64,
    render_buffer_ms: u64,
    wire_bytes: u64,
    init_has_flac: bool,
    playlist_has_ll_hls_tags: bool,
    availability_latency_ms: Percentiles,
    estimated_render_latency_ms: Percentiles,
}

#[derive(Debug, Clone, Serialize)]
struct Percentiles {
    count: usize,
    min: f64,
    p50: f64,
    p95: f64,
    p99: f64,
    max: f64,
}

struct WebTransportReceiveOptions<'a> {
    edge: SocketAddr,
    server_name: &'a str,
    tls_ca: Option<&'a Path>,
    session_id: u64,
    group_id: u16,
    duration_seconds: u64,
    deadline_ms: u64,
    tail_seconds: u64,
}

struct HlsReceiveOptions<'a> {
    edge: SocketAddr,
    server_name: &'a str,
    tls_ca: Option<&'a Path>,
    transport: HlsTransport,
    stream_id: u64,
    session_id: u64,
    duration_seconds: u64,
    part_ms: u64,
    deadline_ms: u64,
    render_buffer_ms: u64,
    tail_seconds: u64,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    match args.command {
        Command::Send {
            target,
            duration_seconds,
            session_id,
            payload,
            group_id,
            repair_percent,
            min_repair_symbols,
        } => {
            let report = send(
                target,
                duration_seconds,
                session_id,
                payload,
                group_id,
                repair_percent,
                min_repair_symbols,
            )
            .await?;
            println!("{}", serde_json::to_string_pretty(&report)?);
        }
        Command::ReceiveUdp {
            relay,
            bind,
            session_id,
            group_id,
            duration_seconds,
            deadline_ms,
            tail_seconds,
        } => {
            let report = timeout(
                receive_command_timeout(session_id, duration_seconds, tail_seconds)?,
                receive_udp(
                    relay,
                    bind,
                    session_id,
                    group_id,
                    duration_seconds,
                    deadline_ms,
                    tail_seconds,
                ),
            )
            .await
            .context("native UDP probe exceeded its overall deadline")??;
            println!("{}", serde_json::to_string_pretty(&report)?);
        }
        Command::ReceiveWebtransport {
            edge,
            server_name,
            tls_ca,
            session_id,
            group_id,
            duration_seconds,
            deadline_ms,
            tail_seconds,
        } => {
            let report = timeout(
                receive_command_timeout(session_id, duration_seconds, tail_seconds)?,
                receive_webtransport(WebTransportReceiveOptions {
                    edge,
                    server_name: &server_name,
                    tls_ca: tls_ca.as_deref(),
                    session_id,
                    group_id,
                    duration_seconds,
                    deadline_ms,
                    tail_seconds,
                }),
            )
            .await
            .context("WebTransport probe exceeded its overall deadline")??;
            println!("{}", serde_json::to_string_pretty(&report)?);
        }
        Command::ReceiveHls {
            edge,
            server_name,
            tls_ca,
            transport,
            stream_id,
            session_id,
            duration_seconds,
            part_ms,
            deadline_ms,
            render_buffer_ms,
            tail_seconds,
        } => {
            let report = timeout(
                receive_command_timeout(session_id, duration_seconds, tail_seconds)?,
                receive_hls(HlsReceiveOptions {
                    edge,
                    server_name: &server_name,
                    tls_ca: tls_ca.as_deref(),
                    transport,
                    stream_id,
                    session_id,
                    duration_seconds,
                    part_ms,
                    deadline_ms,
                    render_buffer_ms,
                    tail_seconds,
                }),
            )
            .await
            .context("LL-HLS probe exceeded its overall deadline")??;
            println!("{}", serde_json::to_string_pretty(&report)?);
        }
    }
    Ok(())
}

fn receive_command_timeout(
    session_id: u64,
    duration_seconds: u64,
    tail_seconds: u64,
) -> Result<Duration> {
    let stop_ns =
        session_id.saturating_add(duration_seconds.saturating_add(tail_seconds) * 1_000_000_000);
    let remaining_ns = stop_ns.saturating_sub(now_unix_ns()?);
    // Leave a bounded allowance for subscription setup, clock skew, and orderly QUIC close.
    Ok(Duration::from_nanos(remaining_ns).saturating_add(Duration::from_secs(5)))
}

async fn send(
    target: SocketAddr,
    duration_seconds: u64,
    session_id: Option<u64>,
    payload_kind: ProbePayload,
    group_id: u16,
    repair_percent: u32,
    min_repair_symbols: u32,
) -> Result<SendReport> {
    if duration_seconds == 0 {
        bail!("--duration-seconds must be positive");
    }
    let session_id = session_id.unwrap_or(now_unix_ns()?.saturating_add(1_000_000_000));
    let now_ns = now_unix_ns()?;
    if session_id + 60_000_000_000 < now_ns {
        bail!("--session-id is too far in the past to be an origin clock");
    }
    if session_id > now_ns {
        sleep_until(TokioInstant::now() + Duration::from_nanos(session_id - now_ns)).await;
    }

    let transport = MultichannelAudioTransportAdapter::udp(MAX_DATAGRAM_BYTES);
    let mut fec = transport.prepare_fec_config(MultichannelAudioFecConfig::default());
    let source_payload_budget = fec
        .max_fragment_payload()
        .context("invalid AEP1 geometry")?;
    let pcm_bytes_per_epoch = usize::from(CHANNELS) * FRAME_COUNT as usize * 3;
    let source_symbols = pcm_bytes_per_epoch.div_ceil(source_payload_budget).max(1);
    let proportional = (source_symbols as u64)
        .saturating_mul(u64::from(repair_percent.min(100)))
        .div_ceil(100) as u32;
    fec.repair_symbols = proportional.max(min_repair_symbols.max(1));
    let mut sender = MultichannelAudioSender::new(MultichannelAudioSessionConfig {
        fec,
        ..MultichannelAudioSessionConfig::default()
    });
    let socket = UdpSocket::bind(if target.is_ipv4() {
        "0.0.0.0:0"
    } else {
        "[::]:0"
    })
    .await?;
    let flac_config = FlacFrameConfig::new(
        SAMPLE_RATE,
        CHANNELS,
        24,
        FRAME_COUNT,
        FlacProfile::Realtime,
    )?;
    let mut flac = FlacFrameEncoder::new(flac_config)?;
    let epochs = duration_seconds.saturating_mul(1_000) / 5;
    let started = Instant::now();
    let mut ticker = interval_at(TokioInstant::now(), FRAME_DURATION);
    ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);
    let mut source_datagrams = 0_u64;
    let mut repair_datagrams = 0_u64;
    let mut wire_bytes = 0_u64;
    let mut lossless_payload_bytes = 0_u64;

    for epoch_id in 0..epochs {
        ticker.tick().await;
        let pcm = sine_s24le(epoch_id * u64::from(FRAME_COUNT));
        let payload = match payload_kind {
            ProbePayload::Flac => Bytes::from(flac.encode_s24le(&pcm)?.payload),
            ProbePayload::Pcm => Bytes::from(pcm),
        };
        lossless_payload_bytes = lossless_payload_bytes.saturating_add(payload.len() as u64);
        let groups = [MultichannelAudioGroup {
            group_id,
            channel_start: 0,
            channel_count: CHANNELS,
            payload_kind: match payload_kind {
                ProbePayload::Flac => AudioPayloadKind::Flac,
                ProbePayload::Pcm => AudioPayloadKind::Pcm,
            },
            sample_format: AudioSampleFormat::S24Le,
            flags: 0,
            payload: &payload,
        }];
        let encoded = sender.encode_epoch(MultichannelAudioEpoch {
            session_id,
            config_generation: 1,
            epoch_id,
            pts_samples: epoch_id.saturating_mul(u64::from(FRAME_COUNT)),
            sample_rate: SAMPLE_RATE,
            frame_count: FRAME_COUNT,
            groups: &groups,
        })?;
        let wrapped = transport.wrap_epoch(encoded)?;
        for datagram in wrapped.datagrams {
            let sent = socket.send_to(&datagram.payload, target).await?;
            if sent != datagram.payload.len() {
                bail!(
                    "partial AEP1 datagram send: {sent} of {}",
                    datagram.payload.len()
                );
            }
            wire_bytes = wire_bytes.saturating_add(sent as u64);
            match datagram.role {
                raptorq_datagram_fec::MultichannelAudioDatagramRole::Source { .. } => {
                    source_datagrams = source_datagrams.saturating_add(1)
                }
                raptorq_datagram_fec::MultichannelAudioDatagramRole::Repair { .. } => {
                    repair_datagrams = repair_datagrams.saturating_add(1)
                }
            }
        }
    }

    let pcm_reference_bytes = epochs
        .saturating_mul(u64::from(FRAME_COUNT))
        .saturating_mul(u64::from(CHANNELS))
        .saturating_mul(3);
    Ok(SendReport {
        schema: "needletail.aep1-48k-probe.send.v1",
        lane: "source",
        payload: payload_kind.as_str(),
        session_id,
        group_id,
        sample_rate: SAMPLE_RATE,
        channels: CHANNELS,
        frame_count: FRAME_COUNT,
        duration_seconds,
        epochs,
        source_datagrams,
        repair_datagrams,
        wire_bytes,
        lossless_payload_bytes,
        pcm_reference_bytes,
        wire_overhead_ratio: wire_bytes as f64 / lossless_payload_bytes.max(1) as f64,
        elapsed_ms: started.elapsed().as_millis().min(u128::from(u64::MAX)) as u64,
    })
}

async fn receive_udp(
    relay: SocketAddr,
    bind: Option<SocketAddr>,
    session_id: u64,
    group_id: u16,
    duration_seconds: u64,
    deadline_ms: u64,
    tail_seconds: u64,
) -> Result<ReceiveReport> {
    let default_bind = if relay.is_ipv4() {
        "0.0.0.0:0"
    } else {
        "[::]:0"
    };
    let socket = UdpSocket::bind(bind.unwrap_or(default_bind.parse()?)).await?;
    let subscribe = format!("WAVEY-DAW-SUBSCRIBE/2 {session_id}");
    socket.send_to(subscribe.as_bytes(), relay).await?;
    let expected_ack = format!("WAVEY-DAW-SUBSCRIBED/2 {session_id}");
    let mut buf = vec![0_u8; 65_536];
    let (ack_len, peer) = timeout(Duration::from_secs(3), socket.recv_from(&mut buf))
        .await
        .context("timed out waiting for DAW relay subscription")??;
    if peer != relay || &buf[..ack_len] != expected_ack.as_bytes() {
        bail!("unexpected DAW relay subscription acknowledgement");
    }

    let transport = MultichannelAudioTransportAdapter::udp(65_535);
    let mut receiver = MultichannelAudioReceiver::new(MultichannelAudioSessionConfig::default());
    let stop_ns =
        session_id.saturating_add(duration_seconds.saturating_add(tail_seconds) * 1_000_000_000);
    let refresh = Duration::from_secs(5);
    let mut next_refresh = TokioInstant::now() + refresh;
    let mut latencies_ns = Vec::new();
    let mut epochs = HashSet::new();
    let mut deadline_misses = 0_u64;
    let mut wire_bytes = 0_u64;

    while now_unix_ns()? < stop_ns {
        let remaining = Duration::from_nanos(stop_ns.saturating_sub(now_unix_ns()?));
        let wait = remaining.min(Duration::from_millis(250));
        match timeout(wait, socket.recv_from(&mut buf)).await {
            Ok(Ok((len, _))) => {
                let payload = match transport.payload(&buf[..len]) {
                    Ok(payload) => payload,
                    Err(_) => continue,
                };
                wire_bytes = wire_bytes.saturating_add(len as u64);
                let outcome = receiver.push_datagram(payload)?;
                let arrival_ns = now_unix_ns()?;
                for group in outcome.completed_groups {
                    if group.session_id != session_id || group.group_id != group_id {
                        continue;
                    }
                    let capture_ns = session_id.saturating_add(
                        group.pts_samples.saturating_mul(1_000_000_000)
                            / u64::from(group.sample_rate),
                    );
                    let latency_ns = arrival_ns.saturating_sub(capture_ns);
                    if latency_ns > deadline_ms.saturating_mul(1_000_000) {
                        deadline_misses = deadline_misses.saturating_add(1);
                    }
                    if epochs.insert(group.epoch_id) {
                        latencies_ns.push(latency_ns);
                    }
                }
            }
            Ok(Err(error)) => return Err(error.into()),
            Err(_) => {}
        }
        if TokioInstant::now() >= next_refresh {
            socket.send_to(subscribe.as_bytes(), relay).await?;
            next_refresh += refresh;
        }
    }

    let stats = receiver.stats();
    let expected_epochs = duration_seconds.saturating_mul(1_000) / 5;
    let latency_ms = percentiles_ms(latencies_ns);
    Ok(ReceiveReport {
        schema: "needletail.aep1-48k-probe.receive.v1",
        lane: "native_udp_fec",
        session_id,
        group_id,
        sample_rate: SAMPLE_RATE,
        duration_seconds,
        expected_epochs,
        received_epochs: epochs.len() as u64,
        missing_epochs: expected_epochs.saturating_sub(epochs.len() as u64),
        deadline_ms,
        deadline_misses,
        datagrams_received: stats.datagrams_received,
        systematic_shards_received: stats.systematic_shards_received,
        raptorq_shards_recovered: stats.raptorq_shards_recovered,
        duplicate_or_late_epochs: stats.duplicate_or_late_epochs,
        wire_bytes,
        render_ready_latency_ms: latency_ms.clone(),
        latency_ms,
    })
}

async fn receive_webtransport(options: WebTransportReceiveOptions<'_>) -> Result<ReceiveReport> {
    let WebTransportReceiveOptions {
        edge,
        server_name,
        tls_ca,
        session_id,
        group_id,
        duration_seconds,
        deadline_ms,
        tail_seconds,
    } = options;
    let crypto = tls_client_config(tls_ca, b"h3")?;
    let client_config = h3_quinn::quinn::ClientConfig::new(Arc::new(
        h3_quinn::quinn::crypto::rustls::QuicClientConfig::try_from(crypto)?,
    ));
    let bind: SocketAddr = if edge.is_ipv4() {
        "0.0.0.0:0".parse().unwrap()
    } else {
        "[::]:0".parse().unwrap()
    };
    let mut endpoint = h3_quinn::quinn::Endpoint::client(bind)?;
    endpoint.set_default_client_config(client_config);
    let connection = endpoint.connect(edge, server_name)?.await?;

    let mut control = connection.open_uni().await?;
    let mut settings = Vec::new();
    encode_varint(0x00, &mut settings);
    let mut settings_payload = Vec::new();
    encode_varint(0x08, &mut settings_payload);
    encode_varint(1, &mut settings_payload);
    encode_varint(0x33, &mut settings_payload);
    encode_varint(1, &mut settings_payload);
    encode_varint(0x2b60_3742, &mut settings_payload);
    encode_varint(1, &mut settings_payload);
    encode_varint(0x2b60_3743, &mut settings_payload);
    encode_varint(16, &mut settings_payload);
    encode_varint(0x04, &mut settings);
    encode_varint(settings_payload.len() as u64, &mut settings);
    settings.extend_from_slice(&settings_payload);
    control.write_all(&settings).await?;

    let (mut connect_send, mut connect_recv) = connection.open_bi().await?;
    let headers = encode_webtransport_connect_headers(&format!("{server_name}:{}", edge.port()));
    let mut request = Vec::new();
    encode_varint(0x01, &mut request);
    encode_varint(headers.len() as u64, &mut request);
    request.extend_from_slice(&headers);
    connect_send.write_all(&request).await?;
    let (frame_type, response_headers) = read_h3_frame(&mut connect_recv).await?;
    if frame_type != 0x01 || !qpack_block_has_static_status_200(&response_headers) {
        bail!("WebTransport CONNECT did not return HTTP 200");
    }

    let subscription = format!("WAVEY-AUDIO-EPOCH/2 {session_id}");
    let mut subscription_datagram = vec![0_u8];
    subscription_datagram.extend_from_slice(subscription.as_bytes());
    connection.send_datagram(Bytes::from(subscription_datagram))?;

    let transport = MultichannelAudioTransportAdapter::udp(65_535);
    let mut receiver = MultichannelAudioReceiver::new(MultichannelAudioSessionConfig::default());
    let stop_ns =
        session_id.saturating_add(duration_seconds.saturating_add(tail_seconds) * 1_000_000_000);
    let mut latencies_ns = Vec::new();
    let mut epochs = HashSet::new();
    let mut deadline_misses = 0_u64;
    let mut wire_bytes = 0_u64;

    while now_unix_ns()? < stop_ns {
        let remaining = Duration::from_nanos(stop_ns.saturating_sub(now_unix_ns()?));
        let wire = match timeout(
            remaining.min(Duration::from_millis(250)),
            connection.read_datagram(),
        )
        .await
        {
            Ok(Ok(wire)) => wire,
            Ok(Err(error)) => return Err(error.into()),
            Err(_) => continue,
        };
        let payload = strip_h3_datagram_context(&wire)?;
        let payload = match transport.payload(payload) {
            Ok(payload) => payload,
            Err(_) => continue,
        };
        wire_bytes = wire_bytes.saturating_add(wire.len() as u64);
        let outcome = receiver.push_datagram(payload)?;
        let arrival_ns = now_unix_ns()?;
        for group in outcome.completed_groups {
            if group.session_id != session_id || group.group_id != group_id {
                continue;
            }
            let capture_ns = session_id.saturating_add(
                group.pts_samples.saturating_mul(1_000_000_000) / u64::from(group.sample_rate),
            );
            let latency_ns = arrival_ns.saturating_sub(capture_ns);
            if latency_ns > deadline_ms.saturating_mul(1_000_000) {
                deadline_misses = deadline_misses.saturating_add(1);
            }
            if epochs.insert(group.epoch_id) {
                latencies_ns.push(latency_ns);
            }
        }
    }
    endpoint.close(0_u32.into(), b"probe complete");

    let stats = receiver.stats();
    let expected_epochs = duration_seconds.saturating_mul(1_000) / 5;
    let latency_ms = percentiles_ms(latencies_ns);
    Ok(ReceiveReport {
        schema: "needletail.aep1-48k-probe.receive.v1",
        lane: "webtransport",
        session_id,
        group_id,
        sample_rate: SAMPLE_RATE,
        duration_seconds,
        expected_epochs,
        received_epochs: epochs.len() as u64,
        missing_epochs: expected_epochs.saturating_sub(epochs.len() as u64),
        deadline_ms,
        deadline_misses,
        datagrams_received: stats.datagrams_received,
        systematic_shards_received: stats.systematic_shards_received,
        raptorq_shards_recovered: stats.raptorq_shards_recovered,
        duplicate_or_late_epochs: stats.duplicate_or_late_epochs,
        wire_bytes,
        render_ready_latency_ms: latency_ms.clone(),
        latency_ms,
    })
}

async fn receive_hls(options: HlsReceiveOptions<'_>) -> Result<HlsReceiveReport> {
    let HlsReceiveOptions {
        edge,
        server_name,
        tls_ca,
        transport,
        stream_id,
        session_id,
        duration_seconds,
        part_ms,
        deadline_ms,
        render_buffer_ms,
        tail_seconds,
    } = options;
    if part_ms == 0 {
        bail!("--part-ms must be positive");
    }
    let stop_ns =
        session_id.saturating_add(duration_seconds.saturating_add(tail_seconds) * 1_000_000_000);
    let media_duration_ms = duration_seconds.saturating_mul(1_000);
    let mut after_sequence = None;
    let mut part_pts = HashSet::new();
    let mut availability_latencies_ns = Vec::new();
    let mut render_latencies_ns = Vec::new();
    let mut deadline_misses = 0_u64;
    let mut wire_bytes = 0_u64;
    let mut init_has_flac = false;
    let mut playlist_has_ll_hls_tags = false;
    let mut h3_client = match transport {
        HlsTransport::H3 => Some(H3HttpsClient::connect(edge, server_name, tls_ca).await?),
        HlsTransport::Http1 => None,
    };
    let connection_setup_ms = h3_client.as_ref().map(|client| client.connection_setup_ms);

    while now_unix_ns()? < stop_ns {
        let path = after_sequence.map_or_else(
            || format!("/live/{stream_id}/tail?from=0"),
            |sequence| format!("/live/{stream_id}/tail?after={sequence}"),
        );
        let response = hls_https_get(&mut h3_client, edge, server_name, tls_ca, &path).await?;
        wire_bytes = wire_bytes.saturating_add(response.wire_bytes as u64);
        match response.status {
            200 => {
                let sequence = response
                    .headers
                    .get("x-sequence")
                    .context("LL-HLS tail response omitted x-sequence")?
                    .parse::<u64>()
                    .context("LL-HLS tail returned an invalid x-sequence")?;
                after_sequence = Some(sequence);
                let pts_ms = parse_fmp4_tfdt_ms(&response.body)
                    .context("LL-HLS fMP4 part omitted a valid tfdt")?;
                let arrival_ns = now_unix_ns()?;
                if arrival_ns >= session_id && pts_ms < media_duration_ms && part_pts.insert(pts_ms)
                {
                    let capture_ns = session_id.saturating_add(pts_ms.saturating_mul(1_000_000));
                    let latency_ns = arrival_ns.saturating_sub(capture_ns);
                    if latency_ns > deadline_ms.saturating_mul(1_000_000) {
                        deadline_misses = deadline_misses.saturating_add(1);
                    }
                    availability_latencies_ns.push(latency_ns);
                    render_latencies_ns.push(
                        latency_ns.saturating_add(render_buffer_ms.saturating_mul(1_000_000)),
                    );
                }
                if !init_has_flac {
                    let init = hls_https_get(
                        &mut h3_client,
                        edge,
                        server_name,
                        tls_ca,
                        &format!("/live/{stream_id}/init.mp4"),
                    )
                    .await?;
                    wire_bytes = wire_bytes.saturating_add(init.wire_bytes as u64);
                    if init.status == 200 {
                        init_has_flac = init.body.windows(4).any(|window| window == b"fLaC");
                    }
                }
                if !playlist_has_ll_hls_tags {
                    let playlist = hls_https_get(
                        &mut h3_client,
                        edge,
                        server_name,
                        tls_ca,
                        &format!("/live/{stream_id}/stream.m3u8"),
                    )
                    .await?;
                    wire_bytes = wire_bytes.saturating_add(playlist.wire_bytes as u64);
                    if playlist.status == 200 {
                        playlist_has_ll_hls_tags = playlist
                            .body
                            .windows(b"#EXT-X-PART:".len())
                            .any(|window| window == b"#EXT-X-PART:")
                            && playlist
                                .body
                                .windows(b"CAN-BLOCK-RELOAD=YES".len())
                                .any(|window| window == b"CAN-BLOCK-RELOAD=YES");
                    }
                }
            }
            204 | 404 => tokio::task::yield_now().await,
            status => bail!("LL-HLS tail returned HTTP {status}"),
        }
    }

    // Known-duration audio closes on the access unit that reaches the target,
    // so a duration aligned to the part boundary has no open tail part.
    let expected_parts = media_duration_ms / part_ms;
    if let Some(client) = h3_client.as_ref() {
        wire_bytes = client.wire_bytes();
    }
    Ok(HlsReceiveReport {
        schema: "needletail.aep1-48k-probe.hls-receive.v1",
        lane: "ll_hls",
        transport: transport.as_str(),
        tls_protocol: "TLSv1.3",
        tls_certificate_verified: true,
        persistent_connection: matches!(transport, HlsTransport::H3),
        connection_setup_ms,
        stream_id,
        session_id,
        sample_rate: SAMPLE_RATE,
        duration_seconds,
        part_ms,
        expected_parts,
        received_parts: part_pts.len() as u64,
        missing_parts: expected_parts.saturating_sub(part_pts.len() as u64),
        deadline_ms,
        deadline_misses,
        render_buffer_ms,
        wire_bytes,
        init_has_flac,
        playlist_has_ll_hls_tags,
        availability_latency_ms: percentiles_ms(availability_latencies_ns),
        estimated_render_latency_ms: percentiles_ms(render_latencies_ns),
    })
}

struct SimpleHttpResponse {
    status: u16,
    headers: HashMap<String, String>,
    body: Vec<u8>,
    wire_bytes: usize,
}

struct H3HttpsClient {
    _endpoint: h3_quinn::quinn::Endpoint,
    connection: h3_quinn::quinn::Connection,
    send_request: h3::client::SendRequest<h3_quinn::OpenStreams, Bytes>,
    _driver_task: tokio::task::JoinHandle<()>,
    authority: String,
    connection_setup_ms: f64,
}

impl H3HttpsClient {
    async fn connect(edge: SocketAddr, server_name: &str, tls_ca: Option<&Path>) -> Result<Self> {
        let setup_started = Instant::now();
        let crypto = tls_client_config(tls_ca, b"h3")?;
        let client_config = h3_quinn::quinn::ClientConfig::new(Arc::new(
            h3_quinn::quinn::crypto::rustls::QuicClientConfig::try_from(crypto)?,
        ));
        let bind: SocketAddr = if edge.is_ipv4() {
            "0.0.0.0:0".parse().unwrap()
        } else {
            "[::]:0".parse().unwrap()
        };
        let mut endpoint = h3_quinn::quinn::Endpoint::client(bind)?;
        endpoint.set_default_client_config(client_config);
        let connection = endpoint.connect(edge, server_name)?.await?;
        let handshake = connection
            .handshake_data()
            .context("HTTP/3 connection omitted TLS handshake data")?
            .downcast::<h3_quinn::quinn::crypto::rustls::HandshakeData>()
            .map_err(|_| {
                anyhow::anyhow!("HTTP/3 connection returned unknown TLS handshake data")
            })?;
        if handshake.protocol.as_deref() != Some(b"h3") {
            bail!("LL-HLS connection did not negotiate HTTP/3 via ALPN");
        }

        let (mut driver, send_request) =
            h3::client::new(h3_quinn::Connection::new(connection.clone())).await?;
        let driver_task = tokio::spawn(async move {
            let _ = driver.wait_idle().await;
        });
        Ok(Self {
            _endpoint: endpoint,
            connection,
            send_request,
            _driver_task: driver_task,
            authority: format!("{server_name}:{}", edge.port()),
            connection_setup_ms: setup_started.elapsed().as_secs_f64() * 1_000.0,
        })
    }

    async fn get(&mut self, path: &str) -> Result<SimpleHttpResponse> {
        let request = http::Request::builder()
            .method(http::Method::GET)
            .uri(format!("https://{}{path}", self.authority))
            .header(http::header::ACCEPT, "*/*")
            .body(())?;
        let mut stream = self.send_request.send_request(request).await?;
        stream.finish().await?;
        let response = stream.recv_response().await?;
        if response.version() != http::Version::HTTP_3 {
            bail!("LL-HLS response was not HTTP/3");
        }
        let status = response.status().as_u16();
        let mut headers = HashMap::new();
        for (name, value) in response.headers() {
            if let Ok(value) = value.to_str() {
                headers.insert(name.as_str().to_ascii_lowercase(), value.to_owned());
            }
        }
        let mut body = Vec::new();
        while let Some(mut chunk) = stream.recv_data().await? {
            let remaining = chunk.remaining();
            body.extend_from_slice(&chunk.copy_to_bytes(remaining));
        }
        Ok(SimpleHttpResponse {
            status,
            headers,
            body,
            // The report uses Quinn's connection-level UDP counters for H3.
            wire_bytes: 0,
        })
    }

    fn wire_bytes(&self) -> u64 {
        let stats = self.connection.stats();
        stats.udp_tx.bytes.saturating_add(stats.udp_rx.bytes)
    }
}

impl Drop for H3HttpsClient {
    fn drop(&mut self) {
        self.connection.close(0_u32.into(), b"probe complete");
        self._driver_task.abort();
    }
}

async fn hls_https_get(
    h3_client: &mut Option<H3HttpsClient>,
    edge: SocketAddr,
    server_name: &str,
    tls_ca: Option<&Path>,
    path: &str,
) -> Result<SimpleHttpResponse> {
    match h3_client {
        Some(client) => client.get(path).await,
        None => https_get_http1(edge, server_name, tls_ca, path).await,
    }
}

async fn https_get_http1(
    edge: SocketAddr,
    server_name: &str,
    tls_ca: Option<&Path>,
    path: &str,
) -> Result<SimpleHttpResponse> {
    let crypto = tls_client_config(tls_ca, b"http/1.1")?;
    let connector = tokio_rustls::TlsConnector::from(Arc::new(crypto));
    let tcp = TcpStream::connect(edge).await?;
    let dns_name = rustls::pki_types::ServerName::try_from(server_name.to_owned())
        .context("invalid LL-HLS TLS server name")?;
    let mut tls = connector.connect(dns_name, tcp).await?;
    let request = format!(
        "GET {path} HTTP/1.1\r\nHost: {server_name}:{}\r\nAccept: */*\r\nConnection: close\r\n\r\n",
        edge.port()
    );
    tls.write_all(request.as_bytes()).await?;
    let mut wire = Vec::new();
    tls.read_to_end(&mut wire).await?;
    parse_http_response(wire)
}

fn parse_http_response(wire: Vec<u8>) -> Result<SimpleHttpResponse> {
    let header_end = wire
        .windows(4)
        .position(|window| window == b"\r\n\r\n")
        .context("HTTP response omitted the header terminator")?;
    let head = std::str::from_utf8(&wire[..header_end]).context("HTTP headers were not UTF-8")?;
    let mut lines = head.split("\r\n");
    let status = lines
        .next()
        .and_then(|line| line.split_whitespace().nth(1))
        .context("HTTP response omitted a status code")?
        .parse::<u16>()
        .context("HTTP response returned an invalid status code")?;
    let mut headers = HashMap::new();
    for line in lines {
        if let Some((name, value)) = line.split_once(':') {
            headers.insert(name.trim().to_ascii_lowercase(), value.trim().to_owned());
        }
    }
    let encoded_body = &wire[header_end + 4..];
    let body = if headers
        .get("transfer-encoding")
        .is_some_and(|value| value.eq_ignore_ascii_case("chunked"))
    {
        decode_chunked_body(encoded_body)?
    } else {
        encoded_body.to_vec()
    };
    Ok(SimpleHttpResponse {
        status,
        headers,
        body,
        wire_bytes: wire.len(),
    })
}

fn decode_chunked_body(mut wire: &[u8]) -> Result<Vec<u8>> {
    let mut body = Vec::new();
    loop {
        let line_end = wire
            .windows(2)
            .position(|window| window == b"\r\n")
            .context("chunked HTTP body omitted a size terminator")?;
        let size_text = std::str::from_utf8(&wire[..line_end])?
            .split(';')
            .next()
            .unwrap_or_default();
        let size = usize::from_str_radix(size_text.trim(), 16)
            .context("chunked HTTP body returned an invalid size")?;
        wire = &wire[line_end + 2..];
        if size == 0 {
            break;
        }
        if wire.len() < size + 2 || &wire[size..size + 2] != b"\r\n" {
            bail!("chunked HTTP body was truncated");
        }
        body.extend_from_slice(&wire[..size]);
        wire = &wire[size + 2..];
    }
    Ok(body)
}

fn parse_fmp4_tfdt_ms(bytes: &[u8]) -> Option<u64> {
    let type_offset = bytes.windows(4).position(|window| window == b"tfdt")?;
    let version = *bytes.get(type_offset + 4)?;
    let value_offset = type_offset + 8;
    match version {
        0 => Some(u64::from(u32::from_be_bytes(
            bytes.get(value_offset..value_offset + 4)?.try_into().ok()?,
        ))),
        1 => Some(u64::from_be_bytes(
            bytes.get(value_offset..value_offset + 8)?.try_into().ok()?,
        )),
        _ => None,
    }
}

fn install_rustls_provider() {
    static INSTALL: Once = Once::new();
    INSTALL.call_once(|| {
        let _ = rustls::crypto::ring::default_provider().install_default();
    });
}

fn tls_client_config(tls_ca: Option<&Path>, alpn: &[u8]) -> Result<rustls::ClientConfig> {
    install_rustls_provider();
    let mut roots = rustls::RootCertStore::empty();
    roots.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
    if let Some(path) = tls_ca {
        let file = std::fs::File::open(path)
            .with_context(|| format!("failed to open TLS CA PEM: {}", path.display()))?;
        let mut reader = std::io::BufReader::new(file);
        let certs = rustls_pemfile::certs(&mut reader)
            .collect::<std::result::Result<Vec<_>, _>>()
            .with_context(|| format!("failed to parse TLS CA PEM: {}", path.display()))?;
        if certs.is_empty() {
            bail!("TLS CA PEM contained no certificates: {}", path.display());
        }
        for cert in certs {
            roots
                .add(cert)
                .with_context(|| format!("failed to trust TLS CA PEM: {}", path.display()))?;
        }
    }
    let mut crypto = rustls::ClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth();
    crypto.alpn_protocols = vec![alpn.to_vec()];
    Ok(crypto)
}

fn encode_varint(value: u64, out: &mut Vec<u8>) {
    if value < 64 {
        out.push(value as u8);
    } else if value < 16_384 {
        out.extend_from_slice(&((value | 0x4000) as u16).to_be_bytes());
    } else if value < 1_073_741_824 {
        out.extend_from_slice(&((value | 0x8000_0000) as u32).to_be_bytes());
    } else {
        out.extend_from_slice(&(value | 0xc000_0000_0000_0000).to_be_bytes());
    }
}

async fn read_varint<R: AsyncRead + Unpin>(reader: &mut R) -> io::Result<u64> {
    let mut first = [0_u8; 1];
    reader.read_exact(&mut first).await?;
    let encoded_len = 1_usize << (first[0] >> 6);
    let mut value = u64::from(first[0] & 0x3f);
    for _ in 1..encoded_len {
        let mut byte = [0_u8; 1];
        reader.read_exact(&mut byte).await?;
        value = (value << 8) | u64::from(byte[0]);
    }
    Ok(value)
}

fn encode_qpack_prefixed_int(prefix_bits: u8, flags: u8, value: u64, out: &mut Vec<u8>) {
    let mask = ((1_u16 << prefix_bits) - 1) as u8;
    let flags = flags << prefix_bits;
    if value < u64::from(mask) {
        out.push(flags | value as u8);
        return;
    }
    out.push(flags | mask);
    let mut remaining = value - u64::from(mask);
    while remaining >= 128 {
        out.push((remaining as u8 & 0x7f) | 0x80);
        remaining >>= 7;
    }
    out.push(remaining as u8);
}

fn encode_qpack_string(prefix_bits: u8, flags: u8, value: &[u8], out: &mut Vec<u8>) {
    encode_qpack_prefixed_int(prefix_bits - 1, flags << 1, value.len() as u64, out);
    out.extend_from_slice(value);
}

fn encode_qpack_indexed_static(index: u64, out: &mut Vec<u8>) {
    encode_qpack_prefixed_int(6, 0b11, index, out);
}

fn encode_qpack_literal_static_name(index: u64, value: &[u8], out: &mut Vec<u8>) {
    encode_qpack_prefixed_int(4, 0b0101, index, out);
    encode_qpack_string(8, 0, value, out);
}

fn encode_qpack_literal(name: &[u8], value: &[u8], out: &mut Vec<u8>) {
    encode_qpack_string(4, 0b0010, name, out);
    encode_qpack_string(8, 0, value, out);
}

fn encode_webtransport_connect_headers(authority: &str) -> Vec<u8> {
    let mut block = vec![0, 0];
    encode_qpack_indexed_static(15, &mut block);
    encode_qpack_indexed_static(23, &mut block);
    encode_qpack_literal_static_name(0, authority.as_bytes(), &mut block);
    encode_qpack_literal_static_name(1, b"/wt", &mut block);
    encode_qpack_literal(b":protocol", b"webtransport", &mut block);
    block
}

async fn read_h3_frame<R: AsyncRead + Unpin>(reader: &mut R) -> io::Result<(u64, Vec<u8>)> {
    let frame_type = read_varint(reader).await?;
    let frame_len = read_varint(reader).await? as usize;
    let mut payload = vec![0_u8; frame_len];
    reader.read_exact(&mut payload).await?;
    Ok((frame_type, payload))
}

fn qpack_block_has_static_status_200(block: &[u8]) -> bool {
    block.len() >= 3 && block[0] == 0 && block[1] == 0 && block[2..].contains(&0xd9)
}

fn strip_h3_datagram_context(wire: &[u8]) -> Result<&[u8]> {
    let Some(first) = wire.first().copied() else {
        bail!("empty HTTP/3 datagram");
    };
    let encoded_len = 1_usize << (first >> 6);
    if wire.len() < encoded_len {
        bail!("truncated HTTP/3 datagram context");
    }
    Ok(&wire[encoded_len..])
}

fn sine_s24le(first_sample: u64) -> Vec<u8> {
    let mut pcm = Vec::with_capacity(FRAME_COUNT as usize * usize::from(CHANNELS) * 3);
    for frame in 0..FRAME_COUNT {
        let sample_index = first_sample.saturating_add(u64::from(frame));
        let phase =
            sample_index as f64 * 2.0 * std::f64::consts::PI * 997.0 / f64::from(SAMPLE_RATE);
        let sample = (phase.sin() * 0.5 * 8_388_607.0).round() as i32;
        let bytes = sample.to_le_bytes();
        for _ in 0..CHANNELS {
            pcm.extend_from_slice(&bytes[..3]);
        }
    }
    pcm
}

fn percentiles_ms(mut values_ns: Vec<u64>) -> Percentiles {
    if values_ns.is_empty() {
        return Percentiles {
            count: 0,
            min: 0.0,
            p50: 0.0,
            p95: 0.0,
            p99: 0.0,
            max: 0.0,
        };
    }
    values_ns.sort_unstable();
    let at = |quantile: f64| {
        let rank = (values_ns.len() as f64 * quantile).ceil() as usize;
        let index = rank.clamp(1, values_ns.len()) - 1;
        values_ns[index] as f64 / 1_000_000.0
    };
    Percentiles {
        count: values_ns.len(),
        min: values_ns[0] as f64 / 1_000_000.0,
        p50: at(0.50),
        p95: at(0.95),
        p99: at(0.99),
        max: *values_ns.last().unwrap() as f64 / 1_000_000.0,
    }
}

fn now_unix_ns() -> Result<u64> {
    Ok(SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .context("system clock is before Unix epoch")?
        .as_nanos()
        .min(u128::from(u64::MAX)) as u64)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn percentile_summary_is_deterministic() {
        let summary = percentiles_ms(vec![1_000_000, 2_000_000, 3_000_000, 4_000_000]);
        assert_eq!(summary.count, 4);
        assert_eq!(summary.min, 1.0);
        assert_eq!(summary.p50, 2.0);
        assert_eq!(summary.p95, 4.0);
        assert_eq!(summary.p99, 4.0);
        assert_eq!(summary.max, 4.0);
    }

    #[test]
    fn generated_pcm_is_exact_stereo_s24le_geometry() {
        assert_eq!(
            sine_s24le(0).len(),
            FRAME_COUNT as usize * usize::from(CHANNELS) * 3
        );
    }

    #[test]
    fn parses_fmp4_tfdt_versions_zero_and_one() {
        let v0 = [0, 0, 0, 16, b't', b'f', b'd', b't', 0, 0, 0, 0, 0, 0, 0, 50];
        let v1 = [
            0, 0, 0, 20, b't', b'f', b'd', b't', 1, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 25,
        ];
        assert_eq!(parse_fmp4_tfdt_ms(&v0), Some(50));
        assert_eq!(parse_fmp4_tfdt_ms(&v1), Some((1_u64 << 32) + 25));
    }

    #[test]
    fn parses_chunked_http_tail_response() {
        let response = parse_http_response(
            b"HTTP/1.1 200 OK\r\nx-sequence: 7\r\ntransfer-encoding: chunked\r\n\r\n4\r\ntest\r\n0\r\n\r\n"
                .to_vec(),
        )
        .unwrap();
        assert_eq!(response.status, 200);
        assert_eq!(response.headers.get("x-sequence").unwrap(), "7");
        assert_eq!(response.body, b"test");
    }
}
