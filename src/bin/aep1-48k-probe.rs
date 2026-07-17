use anyhow::{bail, Context, Result};
use bytes::{Buf, Bytes};
use clap::{Parser, Subcommand, ValueEnum};
use futures_util::{future::BoxFuture, stream::FuturesUnordered, FutureExt, StreamExt};
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
const DEFAULT_CHANNELS: u16 = 2;
const DEFAULT_GROUP_CHANNELS: u16 = 8;
const FLAC_MAX_CHANNELS: u16 = 8;
const LOGICAL_MAX_CHANNELS: u16 = 128;
const FRAME_COUNT: u32 = 240;
const FRAME_DURATION: Duration = Duration::from_millis(5);
const MAX_DATAGRAM_BYTES: usize = 1_200;
// Cover 40 ms of bandwidth-delay product at the qualified 5 ms part size.
// Viewers use a nearby edge cache; a deeper intercontinental prefetch adds
// latency and is not a substitute for selecting the local mesh edge.
const H3_PART_PIPELINE_DEPTH: usize = 8;
const H3_PART_PRELOAD_LEAD: Duration = Duration::from_millis(100);

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
        /// Logical channel count for the generated publication.
        #[arg(long, default_value_t = DEFAULT_CHANNELS)]
        channels: u16,
        /// Maximum channels per AEP1 group / LL-HLS FLAC rendition.
        #[arg(long, default_value_t = DEFAULT_GROUP_CHANNELS)]
        group_channels: u16,
        /// Deterministic source signal used to size codec and transport work.
        #[arg(long, value_enum, default_value_t = ProbeSignal::Decorrelated)]
        signal: ProbeSignal,
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
        /// Route prefix before the stream id. Edges use /live; contributors use an empty prefix.
        #[arg(long, default_value = "/live")]
        path_prefix: String,
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
        /// Ignore media before this offset when qualifying a late join.
        #[arg(long, default_value_t = 0)]
        start_offset_ms: u64,
        #[arg(long, default_value_t = 3)]
        tail_seconds: u64,
        /// Codec that must be declared by the fMP4 init segment.
        #[arg(long, value_enum, default_value_t = HlsAudioCodec::Flac)]
        expected_audio_codec: HlsAudioCodec,
        /// PCM channels carried by this LL-HLS rendition; required for ipcm size checks.
        #[arg(long, default_value_t = 0)]
        expected_pcm_channels: u16,
    },
    /// Run many independent persistent LL-HLS clients in one Rust process.
    LoadHls {
        #[arg(long)]
        edge: SocketAddr,
        #[arg(long, default_value = "local.bitneedle.com")]
        server_name: String,
        /// Additional PEM certificate authority for private/local qualification endpoints.
        #[arg(long)]
        tls_ca: Option<PathBuf>,
        #[arg(long, value_enum, default_value_t = HlsTransport::H3)]
        transport: HlsTransport,
        #[arg(long, default_value = "/live")]
        path_prefix: String,
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
        #[arg(long, default_value_t = 0)]
        start_offset_ms: u64,
        #[arg(long, default_value_t = 3)]
        tail_seconds: u64,
        #[arg(long, default_value_t = 100)]
        readers: usize,
        /// Codec that every reader must verify in the fMP4 init segment.
        #[arg(long, value_enum, default_value_t = HlsAudioCodec::Flac)]
        expected_audio_codec: HlsAudioCodec,
        /// PCM channels carried by this LL-HLS rendition; required for ipcm size checks.
        #[arg(long, default_value_t = 0)]
        expected_pcm_channels: u16,
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

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
enum HlsAudioCodec {
    Flac,
    Ipcm,
    Fpcm,
}

impl HlsAudioCodec {
    fn as_str(self) -> &'static str {
        match self {
            Self::Flac => "flac",
            Self::Ipcm => "ipcm_s24le",
            Self::Fpcm => "fpcm_f32le",
        }
    }

    fn init_marker(self) -> &'static [u8; 4] {
        match self {
            Self::Flac => b"fLaC",
            Self::Ipcm => b"ipcm",
            Self::Fpcm => b"fpcm",
        }
    }
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

#[derive(Debug, Clone, Copy, ValueEnum)]
enum ProbeSignal {
    /// Per-channel tones plus deterministic dither. More realistic than duplicated stereo sine.
    Decorrelated,
    /// Deterministic pseudo-random S24 samples. Useful as a near-worst-case lossless payload.
    Noise,
    /// Legacy duplicated sine wave.
    Sine,
}

impl ProbeSignal {
    fn as_str(self) -> &'static str {
        match self {
            Self::Decorrelated => "decorrelated",
            Self::Noise => "noise",
            Self::Sine => "sine",
        }
    }
}

#[derive(Debug, Serialize)]
struct SendReport {
    schema: &'static str,
    lane: &'static str,
    payload: &'static str,
    signal: &'static str,
    session_id: u64,
    group_id: u16,
    group_count: u16,
    group_channels: u16,
    group_ids: Vec<u16>,
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
    wire_to_pcm_ratio: f64,
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
    path_prefix: String,
    stream_id: u64,
    session_id: u64,
    sample_rate: u32,
    duration_seconds: u64,
    part_ms: u64,
    expected_parts: u64,
    received_parts: u64,
    missing_parts: u64,
    first_pts_ms: Option<u64>,
    last_pts_ms: Option<u64>,
    non_contiguous_pts: u64,
    deadline_ms: u64,
    deadline_misses: u64,
    render_buffer_ms: u64,
    start_offset_ms: u64,
    wire_bytes: u64,
    init_has_flac: bool,
    expected_audio_codec: &'static str,
    init_audio_codec: Option<&'static str>,
    init_audio_codec_verified: bool,
    expected_pcm_channels: u16,
    pcm_media_parts_verified: u64,
    pcm_media_size_mismatches: u64,
    playlist_has_ll_hls_tags: bool,
    publication_to_cache_latency_ms: Percentiles,
    cache_to_client_latency_ms: Percentiles,
    availability_latency_ms: Percentiles,
    estimated_render_latency_ms: Percentiles,
}

#[derive(Debug, Serialize)]
struct HlsLoadReport {
    schema: &'static str,
    transport: &'static str,
    tls_protocol: &'static str,
    persistent_connections: bool,
    edge: SocketAddr,
    stream_id: u64,
    session_id: u64,
    duration_seconds: u64,
    part_ms: u64,
    expected_audio_codec: &'static str,
    expected_pcm_channels: u16,
    readers_requested: usize,
    readers_completed: usize,
    readers_failed: usize,
    readers_with_incomplete_media: usize,
    expected_parts_per_reader: u64,
    received_parts_total: u64,
    missing_parts_total: u64,
    non_contiguous_pts_total: u64,
    deadline_misses_total: u64,
    init_verified_readers: usize,
    pcm_media_size_mismatches_total: u64,
    playlist_verified_readers: usize,
    wire_bytes_total: u64,
    connection_setup_ms: Percentiles,
    availability_p99_ms_across_readers: Percentiles,
    cache_to_client_p99_ms_across_readers: Percentiles,
    elapsed_ms: u64,
    passed: bool,
    errors: Vec<String>,
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
    path_prefix: &'a str,
    stream_id: u64,
    session_id: u64,
    duration_seconds: u64,
    part_ms: u64,
    deadline_ms: u64,
    render_buffer_ms: u64,
    start_offset_ms: u64,
    tail_seconds: u64,
    expected_audio_codec: HlsAudioCodec,
    expected_pcm_channels: u16,
}

#[derive(Clone)]
struct HlsLoadOptions {
    edge: SocketAddr,
    server_name: String,
    tls_ca: Option<PathBuf>,
    transport: HlsTransport,
    path_prefix: String,
    stream_id: u64,
    session_id: u64,
    duration_seconds: u64,
    part_ms: u64,
    deadline_ms: u64,
    start_offset_ms: u64,
    tail_seconds: u64,
    readers: usize,
    expected_audio_codec: HlsAudioCodec,
    expected_pcm_channels: u16,
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
            channels,
            group_channels,
            signal,
            group_id,
            repair_percent,
            min_repair_symbols,
        } => {
            let report = send(
                target,
                duration_seconds,
                session_id,
                payload,
                channels,
                group_channels,
                signal,
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
            path_prefix,
            stream_id,
            session_id,
            duration_seconds,
            part_ms,
            deadline_ms,
            render_buffer_ms,
            start_offset_ms,
            tail_seconds,
            expected_audio_codec,
            expected_pcm_channels,
        } => {
            let report = timeout(
                receive_command_timeout(session_id, duration_seconds, tail_seconds)?,
                receive_hls(HlsReceiveOptions {
                    edge,
                    server_name: &server_name,
                    tls_ca: tls_ca.as_deref(),
                    transport,
                    path_prefix: &path_prefix,
                    stream_id,
                    session_id,
                    duration_seconds,
                    part_ms,
                    deadline_ms,
                    render_buffer_ms,
                    start_offset_ms,
                    tail_seconds,
                    expected_audio_codec,
                    expected_pcm_channels,
                }),
            )
            .await
            .context("LL-HLS probe exceeded its overall deadline")??;
            println!("{}", serde_json::to_string_pretty(&report)?);
            if !report.init_audio_codec_verified || report.pcm_media_size_mismatches > 0 {
                bail!(
                    "LL-HLS codec verification failed: expected {}, received {:?}, {} PCM media size mismatches",
                    report.expected_audio_codec,
                    report.init_audio_codec,
                    report.pcm_media_size_mismatches
                );
            }
        }
        Command::LoadHls {
            edge,
            server_name,
            tls_ca,
            transport,
            path_prefix,
            stream_id,
            session_id,
            duration_seconds,
            part_ms,
            deadline_ms,
            start_offset_ms,
            tail_seconds,
            readers,
            expected_audio_codec,
            expected_pcm_channels,
        } => {
            let report = load_hls(HlsLoadOptions {
                edge,
                server_name,
                tls_ca,
                transport,
                path_prefix,
                stream_id,
                session_id,
                duration_seconds,
                part_ms,
                deadline_ms,
                start_offset_ms,
                tail_seconds,
                readers,
                expected_audio_codec,
                expected_pcm_channels,
            })
            .await?;
            println!("{}", serde_json::to_string_pretty(&report)?);
            if !report.passed {
                bail!(
                    "LL-HLS load qualification failed: {} failed readers, {} incomplete readers, {} missing parts",
                    report.readers_failed,
                    report.readers_with_incomplete_media,
                    report.missing_parts_total
                );
            }
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
    channels: u16,
    group_channels: u16,
    signal: ProbeSignal,
    group_id: u16,
    repair_percent: u32,
    min_repair_symbols: u32,
) -> Result<SendReport> {
    if duration_seconds == 0 {
        bail!("--duration-seconds must be positive");
    }
    let group_plan = source_group_plan(group_id, channels, group_channels)?;
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
    let max_pcm_bytes_per_group = group_plan
        .iter()
        .map(|group| usize::from(group.channel_count) * FRAME_COUNT as usize * 3)
        .max()
        .unwrap_or(0);
    let source_symbols = max_pcm_bytes_per_group
        .div_ceil(source_payload_budget)
        .max(1);
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
    let mut flac_encoders = if matches!(payload_kind, ProbePayload::Flac) {
        group_plan
            .iter()
            .map(|group| {
                FlacFrameConfig::new(
                    SAMPLE_RATE,
                    group.channel_count,
                    24,
                    FRAME_COUNT,
                    FlacProfile::Realtime,
                )
                .and_then(FlacFrameEncoder::new)
            })
            .collect::<Result<Vec<_>, _>>()?
    } else {
        Vec::new()
    };
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
        let first_sample = epoch_id.saturating_mul(u64::from(FRAME_COUNT));
        let mut payloads = Vec::with_capacity(group_plan.len());
        for (index, group) in group_plan.iter().enumerate() {
            let pcm = signal_s24le(
                first_sample,
                group.channel_start,
                group.channel_count,
                signal,
            );
            let payload = match payload_kind {
                ProbePayload::Flac => flac_encoders[index].encode_s24le(&pcm)?.payload,
                ProbePayload::Pcm => pcm,
            };
            lossless_payload_bytes = lossless_payload_bytes.saturating_add(payload.len() as u64);
            payloads.push(payload);
        }
        let groups = group_plan
            .iter()
            .zip(payloads.iter())
            .map(|(group, payload)| MultichannelAudioGroup {
                group_id: group.group_id,
                channel_start: group.channel_start,
                channel_count: group.channel_count,
                payload_kind: match payload_kind {
                    ProbePayload::Flac => AudioPayloadKind::Flac,
                    ProbePayload::Pcm => AudioPayloadKind::Pcm,
                },
                sample_format: AudioSampleFormat::S24Le,
                flags: 0,
                payload,
            })
            .collect::<Vec<_>>();
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
        .saturating_mul(u64::from(channels))
        .saturating_mul(3);
    Ok(SendReport {
        schema: "needletail.aep1-48k-probe.send.v2",
        lane: "source",
        payload: payload_kind.as_str(),
        signal: signal.as_str(),
        session_id,
        group_id,
        group_count: group_plan.len() as u16,
        group_channels: group_channels.min(FLAC_MAX_CHANNELS),
        group_ids: group_plan.iter().map(|group| group.group_id).collect(),
        sample_rate: SAMPLE_RATE,
        channels,
        frame_count: FRAME_COUNT,
        duration_seconds,
        epochs,
        source_datagrams,
        repair_datagrams,
        wire_bytes,
        lossless_payload_bytes,
        pcm_reference_bytes,
        wire_overhead_ratio: wire_bytes as f64 / lossless_payload_bytes.max(1) as f64,
        wire_to_pcm_ratio: wire_bytes as f64 / pcm_reference_bytes.max(1) as f64,
        elapsed_ms: started.elapsed().as_millis().min(u128::from(u64::MAX)) as u64,
    })
}

#[derive(Debug, Clone, Copy)]
struct SourceGroupPlan {
    group_id: u16,
    channel_start: u16,
    channel_count: u16,
}

fn source_group_plan(
    base_group_id: u16,
    channels: u16,
    requested_group_channels: u16,
) -> Result<Vec<SourceGroupPlan>> {
    if channels == 0 {
        bail!("--channels must be positive");
    }
    if channels > LOGICAL_MAX_CHANNELS {
        bail!("--channels may not exceed {LOGICAL_MAX_CHANNELS}");
    }
    if requested_group_channels == 0 {
        bail!("--group-channels must be positive");
    }
    if requested_group_channels > FLAC_MAX_CHANNELS {
        bail!("--group-channels may not exceed {FLAC_MAX_CHANNELS}; split wider logical streams across multiple FLAC-safe AEP1 groups");
    }
    let group_channels = requested_group_channels;
    let group_count = channels.div_ceil(group_channels);
    if u32::from(base_group_id) + u32::from(group_count) > u32::from(u16::MAX) + 1 {
        bail!("--group-id plus derived group count exceeds u16 range");
    }
    let mut groups = Vec::with_capacity(usize::from(group_count));
    let mut remaining = channels;
    let mut channel_start = 0_u16;
    for index in 0..group_count {
        let channel_count = remaining.min(group_channels);
        groups.push(SourceGroupPlan {
            group_id: base_group_id + index,
            channel_start,
            channel_count,
        });
        channel_start = channel_start.saturating_add(channel_count);
        remaining -= channel_count;
    }
    Ok(groups)
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
    let media_end_ns = session_id.saturating_add(duration_seconds * 1_000_000_000);
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
            Ok(Err(error)) => {
                // A peer may close an otherwise complete WebTransport session
                // at its QUIC idle timeout during the bounded receiver tail.
                // Preserve the completed report so the normal completeness
                // gates decide whether any media was actually lost.
                if now_unix_ns()? >= media_end_ns {
                    break;
                }
                return Err(error.into());
            }
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
        path_prefix,
        stream_id,
        session_id,
        duration_seconds,
        part_ms,
        deadline_ms,
        render_buffer_ms,
        start_offset_ms,
        tail_seconds,
        expected_audio_codec,
        expected_pcm_channels,
    } = options;
    if part_ms == 0 {
        bail!("--part-ms must be positive");
    }
    if !path_prefix.is_empty() && (!path_prefix.starts_with('/') || path_prefix.ends_with('/')) {
        bail!("--path-prefix must be empty or start with one slash and have no trailing slash");
    }
    let stop_ns =
        session_id.saturating_add(duration_seconds.saturating_add(tail_seconds) * 1_000_000_000);
    let media_duration_ms = duration_seconds.saturating_mul(1_000);
    if start_offset_ms >= media_duration_ms {
        bail!("--start-offset-ms must be smaller than the media duration");
    }
    if !start_offset_ms.is_multiple_of(part_ms) {
        bail!("--start-offset-ms must align to the configured part duration");
    }
    if matches!(
        expected_audio_codec,
        HlsAudioCodec::Ipcm | HlsAudioCodec::Fpcm
    ) && expected_pcm_channels == 0
    {
        bail!("--expected-pcm-channels must be positive for PCM LL-HLS");
    }
    // Edge `from` is inclusive, while direct-origin part IDs start at one.
    // Begin at the requested PTS without draining the retained prefix.
    let initial_from_sequence = start_offset_ms.saturating_div(part_ms);
    let mut after_sequence: Option<u64> = None;
    let mut part_pts = HashSet::new();
    let mut availability_latencies_ns = Vec::new();
    let mut publication_to_cache_latencies_ns = Vec::new();
    let mut cache_to_client_latencies_ns = Vec::new();
    let mut render_latencies_ns = Vec::new();
    let mut deadline_misses = 0_u64;
    let mut wire_bytes = 0_u64;
    let mut init_audio_codec = None;
    let mut init_audio_codec_verified = false;
    let mut pcm_media_parts_verified = 0_u64;
    let mut pcm_media_size_mismatches = 0_u64;
    let mut playlist_has_ll_hls_tags = false;
    let direct_part_route = path_prefix.is_empty();
    let mut h3_client = match transport {
        HlsTransport::H3 => Some(H3HttpsClient::connect(edge, server_name, tls_ca).await?),
        HlsTransport::Http1 => None,
    };
    let connection_setup_ms = h3_client.as_ref().map(|client| client.connection_setup_ms);

    if !matches!(transport, HlsTransport::H3) || direct_part_route {
        while now_unix_ns()? < stop_ns {
            let requested_sequence = after_sequence
                .unwrap_or(initial_from_sequence)
                .saturating_add(1);
            let path = if direct_part_route {
                hls_path(
                    path_prefix,
                    stream_id,
                    &format!("p{requested_sequence}.mp4"),
                )
            } else {
                after_sequence.map_or_else(
                    || {
                        hls_path(
                            path_prefix,
                            stream_id,
                            &format!("tail?from={initial_from_sequence}"),
                        )
                    },
                    |sequence| hls_path(path_prefix, stream_id, &format!("tail?after={sequence}")),
                )
            };
            let response = hls_https_get(&mut h3_client, edge, server_name, tls_ca, &path).await?;
            wire_bytes = wire_bytes.saturating_add(response.wire_bytes as u64);
            match response.status {
                200 => {
                    let sequence = if direct_part_route {
                        requested_sequence
                    } else {
                        response
                            .headers
                            .get("x-sequence")
                            .context("LL-HLS tail response omitted x-sequence")?
                            .parse::<u64>()
                            .context("LL-HLS tail returned an invalid x-sequence")?
                    };
                    after_sequence = Some(sequence);
                    let pts_ms = parse_fmp4_tfdt_ms(&response.body)
                        .context("LL-HLS fMP4 part omitted a valid tfdt")?;
                    let arrival_ns = now_unix_ns()?;
                    if arrival_ns >= session_id
                        && pts_ms >= start_offset_ms
                        && pts_ms < media_duration_ms
                        && part_pts.insert(pts_ms)
                    {
                        let capture_ns =
                            session_id.saturating_add(pts_ms.saturating_mul(1_000_000));
                        let latency_ns = arrival_ns.saturating_sub(capture_ns);
                        if latency_ns > deadline_ms.saturating_mul(1_000_000) {
                            deadline_misses = deadline_misses.saturating_add(1);
                        }
                        availability_latencies_ns.push(latency_ns);
                        if let Some(expected_bytes) = expected_pcm_part_bytes(
                            expected_audio_codec,
                            expected_pcm_channels,
                            part_ms,
                        ) {
                            if fmp4_mdat_payload(&response.body)
                                .is_some_and(|payload| payload.len() as u64 == expected_bytes)
                            {
                                pcm_media_parts_verified =
                                    pcm_media_parts_verified.saturating_add(1);
                            } else {
                                pcm_media_size_mismatches =
                                    pcm_media_size_mismatches.saturating_add(1);
                            }
                        }
                        if let Some((publication_to_cache_ns, cache_to_client_ns)) =
                            split_cache_latency_ns(&response.headers, capture_ns, arrival_ns)
                        {
                            publication_to_cache_latencies_ns.push(publication_to_cache_ns);
                            cache_to_client_latencies_ns.push(cache_to_client_ns);
                        }
                        render_latencies_ns.push(
                            latency_ns.saturating_add(render_buffer_ms.saturating_mul(1_000_000)),
                        );
                    }
                    if init_audio_codec.is_none() {
                        let init = hls_https_get(
                            &mut h3_client,
                            edge,
                            server_name,
                            tls_ca,
                            &hls_path(path_prefix, stream_id, "init.mp4"),
                        )
                        .await?;
                        wire_bytes = wire_bytes.saturating_add(init.wire_bytes as u64);
                        if init.status == 200 {
                            init_audio_codec = detect_init_audio_codec(&init.body);
                            init_audio_codec_verified = init_audio_codec
                                .is_some_and(|actual| actual == expected_audio_codec)
                                && pcm_init_parameters_match(
                                    &init.body,
                                    expected_audio_codec,
                                    expected_pcm_channels,
                                );
                        }
                    }
                    if !playlist_has_ll_hls_tags {
                        let playlist = hls_https_get(
                            &mut h3_client,
                            edge,
                            server_name,
                            tls_ca,
                            &hls_path(path_prefix, stream_id, "stream.m3u8"),
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
    } else {
        let preload_at_ns = session_id
            .saturating_sub(H3_PART_PRELOAD_LEAD.as_nanos().min(u128::from(u64::MAX)) as u64);
        let now_ns = now_unix_ns()?;
        if now_ns < preload_at_ns {
            sleep_until(TokioInstant::now() + Duration::from_nanos(preload_at_ns - now_ns)).await;
        }

        type PartRequest = BoxFuture<'static, (u64, Result<SimpleHttpResponse>)>;
        let final_sequence = media_duration_ms.saturating_div(part_ms);
        let mut next_sequence = initial_from_sequence;
        let mut in_flight = FuturesUnordered::<PartRequest>::new();
        while in_flight.len() < H3_PART_PIPELINE_DEPTH && next_sequence < final_sequence {
            let requested_sequence = next_sequence;
            next_sequence = next_sequence.saturating_add(1);
            let path = hls_path(
                path_prefix,
                stream_id,
                &format!("part{requested_sequence}.mp4"),
            );
            let sender = h3_client
                .as_ref()
                .context("HTTP/3 media pipeline omitted its connection")?
                .request_sender();
            in_flight.push(async move { (requested_sequence, sender.get(path).await) }.boxed());
        }

        while now_unix_ns()? < stop_ns && !in_flight.is_empty() {
            let remaining = Duration::from_nanos(stop_ns.saturating_sub(now_unix_ns()?));
            let Some((requested_sequence, response)) =
                timeout(remaining, in_flight.next()).await.ok().flatten()
            else {
                break;
            };
            let response = response?;
            wire_bytes = wire_bytes.saturating_add(response.wire_bytes as u64);
            let mut retry_sequence = None;
            match response.status {
                200 => {
                    let pts_ms = parse_fmp4_tfdt_ms(&response.body)
                        .context("LL-HLS fMP4 part omitted a valid tfdt")?;
                    let expected_pts_ms = requested_sequence.saturating_mul(part_ms);
                    if pts_ms != expected_pts_ms {
                        bail!(
                            "pipelined LL-HLS part {requested_sequence} carried PTS {pts_ms} ms, expected {expected_pts_ms} ms"
                        );
                    }
                    let arrival_ns = now_unix_ns()?;
                    if arrival_ns >= session_id
                        && pts_ms >= start_offset_ms
                        && pts_ms < media_duration_ms
                        && part_pts.insert(pts_ms)
                    {
                        let capture_ns =
                            session_id.saturating_add(pts_ms.saturating_mul(1_000_000));
                        let latency_ns = arrival_ns.saturating_sub(capture_ns);
                        if latency_ns > deadline_ms.saturating_mul(1_000_000) {
                            deadline_misses = deadline_misses.saturating_add(1);
                        }
                        availability_latencies_ns.push(latency_ns);
                        if let Some(expected_bytes) = expected_pcm_part_bytes(
                            expected_audio_codec,
                            expected_pcm_channels,
                            part_ms,
                        ) {
                            if fmp4_mdat_payload(&response.body)
                                .is_some_and(|payload| payload.len() as u64 == expected_bytes)
                            {
                                pcm_media_parts_verified =
                                    pcm_media_parts_verified.saturating_add(1);
                            } else {
                                pcm_media_size_mismatches =
                                    pcm_media_size_mismatches.saturating_add(1);
                            }
                        }
                        if let Some((publication_to_cache_ns, cache_to_client_ns)) =
                            split_cache_latency_ns(&response.headers, capture_ns, arrival_ns)
                        {
                            publication_to_cache_latencies_ns.push(publication_to_cache_ns);
                            cache_to_client_latencies_ns.push(cache_to_client_ns);
                        }
                        render_latencies_ns.push(
                            latency_ns.saturating_add(render_buffer_ms.saturating_mul(1_000_000)),
                        );
                    }
                    if init_audio_codec.is_none() {
                        let init = hls_https_get(
                            &mut h3_client,
                            edge,
                            server_name,
                            tls_ca,
                            &hls_path(path_prefix, stream_id, "init.mp4"),
                        )
                        .await?;
                        wire_bytes = wire_bytes.saturating_add(init.wire_bytes as u64);
                        if init.status == 200 {
                            init_audio_codec = detect_init_audio_codec(&init.body);
                            init_audio_codec_verified = init_audio_codec
                                .is_some_and(|actual| actual == expected_audio_codec)
                                && pcm_init_parameters_match(
                                    &init.body,
                                    expected_audio_codec,
                                    expected_pcm_channels,
                                );
                        }
                    }
                    if !playlist_has_ll_hls_tags {
                        let playlist = hls_https_get(
                            &mut h3_client,
                            edge,
                            server_name,
                            tls_ca,
                            &hls_path(path_prefix, stream_id, "stream.m3u8"),
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
                204 | 404 => retry_sequence = Some(requested_sequence),
                status => bail!("LL-HLS tail returned HTTP {status}"),
            }

            let sequence_to_schedule = retry_sequence.or_else(|| {
                if next_sequence < final_sequence {
                    let sequence = next_sequence;
                    next_sequence = next_sequence.saturating_add(1);
                    Some(sequence)
                } else {
                    None
                }
            });
            if let Some(requested_sequence) = sequence_to_schedule {
                let path = hls_path(
                    path_prefix,
                    stream_id,
                    &format!("part{requested_sequence}.mp4"),
                );
                let sender = h3_client
                    .as_ref()
                    .context("HTTP/3 media pipeline omitted its connection")?
                    .request_sender();
                in_flight.push(async move { (requested_sequence, sender.get(path).await) }.boxed());
            }
        }
    }

    let mut ordered_part_pts = part_pts.iter().copied().collect::<Vec<_>>();
    ordered_part_pts.sort_unstable();
    let non_contiguous_pts = ordered_part_pts
        .windows(2)
        .filter(|pair| pair[1].saturating_sub(pair[0]) != part_ms)
        .count() as u64;

    // Known-duration audio closes on the access unit that reaches the target,
    // so a duration aligned to the part boundary has no open tail part.
    let expected_parts = media_duration_ms.saturating_sub(start_offset_ms) / part_ms;
    if let Some(client) = h3_client.as_ref() {
        wire_bytes = client.wire_bytes();
    }
    Ok(HlsReceiveReport {
        schema: "needletail.aep1-48k-probe.hls-receive.v4",
        lane: "ll_hls",
        transport: transport.as_str(),
        tls_protocol: "TLSv1.3",
        tls_certificate_verified: true,
        persistent_connection: matches!(transport, HlsTransport::H3),
        connection_setup_ms,
        path_prefix: path_prefix.to_owned(),
        stream_id,
        session_id,
        sample_rate: SAMPLE_RATE,
        duration_seconds,
        part_ms,
        expected_parts,
        received_parts: part_pts.len() as u64,
        missing_parts: expected_parts.saturating_sub(part_pts.len() as u64),
        first_pts_ms: ordered_part_pts.first().copied(),
        last_pts_ms: ordered_part_pts.last().copied(),
        non_contiguous_pts,
        deadline_ms,
        deadline_misses,
        render_buffer_ms,
        start_offset_ms,
        wire_bytes,
        init_has_flac: init_audio_codec == Some(HlsAudioCodec::Flac),
        expected_audio_codec: expected_audio_codec.as_str(),
        init_audio_codec: init_audio_codec.map(HlsAudioCodec::as_str),
        init_audio_codec_verified,
        expected_pcm_channels,
        pcm_media_parts_verified,
        pcm_media_size_mismatches,
        playlist_has_ll_hls_tags,
        publication_to_cache_latency_ms: percentiles_ms(publication_to_cache_latencies_ns),
        cache_to_client_latency_ms: percentiles_ms(cache_to_client_latencies_ns),
        availability_latency_ms: percentiles_ms(availability_latencies_ns),
        estimated_render_latency_ms: percentiles_ms(render_latencies_ns),
    })
}

async fn load_hls(options: HlsLoadOptions) -> Result<HlsLoadReport> {
    if options.readers == 0 || options.readers > 4_096 {
        bail!("--readers must be between 1 and 4096");
    }
    if options.part_ms == 0 {
        bail!("--part-ms must be positive");
    }
    let media_duration_ms = options.duration_seconds.saturating_mul(1_000);
    if options.start_offset_ms >= media_duration_ms {
        bail!("--start-offset-ms must be smaller than the media duration");
    }
    if !options.start_offset_ms.is_multiple_of(options.part_ms) {
        bail!("--start-offset-ms must align to the configured part duration");
    }

    let started = Instant::now();
    let per_reader_timeout = receive_command_timeout(
        options.session_id,
        options.duration_seconds,
        options.tail_seconds,
    )?;
    let mut tasks = tokio::task::JoinSet::new();
    for reader_id in 0..options.readers {
        let reader = options.clone();
        tasks.spawn(async move {
            let result = timeout(
                per_reader_timeout,
                receive_hls(HlsReceiveOptions {
                    edge: reader.edge,
                    server_name: &reader.server_name,
                    tls_ca: reader.tls_ca.as_deref(),
                    transport: reader.transport,
                    path_prefix: &reader.path_prefix,
                    stream_id: reader.stream_id,
                    session_id: reader.session_id,
                    duration_seconds: reader.duration_seconds,
                    part_ms: reader.part_ms,
                    deadline_ms: reader.deadline_ms,
                    render_buffer_ms: 0,
                    start_offset_ms: reader.start_offset_ms,
                    tail_seconds: reader.tail_seconds,
                    expected_audio_codec: reader.expected_audio_codec,
                    expected_pcm_channels: reader.expected_pcm_channels,
                }),
            )
            .await
            .context("reader exceeded its overall deadline")
            .and_then(|result| result);
            (reader_id, result)
        });
    }

    let mut reports = Vec::with_capacity(options.readers);
    let mut errors = Vec::new();
    while let Some(outcome) = tasks.join_next().await {
        match outcome {
            Ok((_reader_id, Ok(report))) => reports.push(report),
            Ok((reader_id, Err(error))) => {
                if errors.len() < 20 {
                    errors.push(format!("reader {reader_id}: {error}"));
                }
            }
            Err(error) => {
                if errors.len() < 20 {
                    errors.push(format!("reader task failed: {error}"));
                }
            }
        }
    }

    let readers_completed = reports.len();
    let readers_failed = options.readers.saturating_sub(readers_completed);
    let readers_with_incomplete_media = reports
        .iter()
        .filter(|report| report.missing_parts > 0 || report.non_contiguous_pts > 0)
        .count();
    let expected_parts_per_reader =
        media_duration_ms.saturating_sub(options.start_offset_ms) / options.part_ms;
    let received_parts_total = reports
        .iter()
        .map(|report| report.received_parts)
        .sum::<u64>();
    let missing_parts_total = reports
        .iter()
        .map(|report| report.missing_parts)
        .sum::<u64>()
        .saturating_add((readers_failed as u64).saturating_mul(expected_parts_per_reader));
    let non_contiguous_pts_total = reports
        .iter()
        .map(|report| report.non_contiguous_pts)
        .sum::<u64>();
    let deadline_misses_total = reports
        .iter()
        .map(|report| report.deadline_misses)
        .sum::<u64>();
    let init_verified_readers = reports
        .iter()
        .filter(|report| report.init_audio_codec_verified)
        .count();
    let pcm_media_size_mismatches_total = reports
        .iter()
        .map(|report| report.pcm_media_size_mismatches)
        .sum::<u64>();
    let playlist_verified_readers = reports
        .iter()
        .filter(|report| report.playlist_has_ll_hls_tags)
        .count();
    let wire_bytes_total = reports.iter().map(|report| report.wire_bytes).sum::<u64>();
    let connection_setup_ms = percentiles_ms(
        reports
            .iter()
            .filter_map(|report| report.connection_setup_ms)
            .map(ms_f64_to_ns)
            .collect(),
    );
    let availability_p99_ms_across_readers = percentiles_ms(
        reports
            .iter()
            .filter(|report| report.availability_latency_ms.count > 0)
            .map(|report| ms_f64_to_ns(report.availability_latency_ms.p99))
            .collect(),
    );
    let cache_to_client_p99_ms_across_readers = percentiles_ms(
        reports
            .iter()
            .filter(|report| report.cache_to_client_latency_ms.count > 0)
            .map(|report| ms_f64_to_ns(report.cache_to_client_latency_ms.p99))
            .collect(),
    );
    let passed = readers_failed == 0
        && readers_with_incomplete_media == 0
        && deadline_misses_total == 0
        && init_verified_readers == options.readers
        && pcm_media_size_mismatches_total == 0
        && playlist_verified_readers == options.readers;

    Ok(HlsLoadReport {
        schema: "needletail.aep1-48k-probe.hls-load.v2",
        transport: options.transport.as_str(),
        tls_protocol: "TLSv1.3",
        persistent_connections: matches!(options.transport, HlsTransport::H3),
        edge: options.edge,
        stream_id: options.stream_id,
        session_id: options.session_id,
        duration_seconds: options.duration_seconds,
        part_ms: options.part_ms,
        expected_audio_codec: options.expected_audio_codec.as_str(),
        expected_pcm_channels: options.expected_pcm_channels,
        readers_requested: options.readers,
        readers_completed,
        readers_failed,
        readers_with_incomplete_media,
        expected_parts_per_reader,
        received_parts_total,
        missing_parts_total,
        non_contiguous_pts_total,
        deadline_misses_total,
        init_verified_readers,
        pcm_media_size_mismatches_total,
        playlist_verified_readers,
        wire_bytes_total,
        connection_setup_ms,
        availability_p99_ms_across_readers,
        cache_to_client_p99_ms_across_readers,
        elapsed_ms: started.elapsed().as_millis().min(u128::from(u64::MAX)) as u64,
        passed,
        errors,
    })
}

fn hls_path(prefix: &str, stream_id: u64, suffix: &str) -> String {
    format!("{prefix}/{stream_id}/{suffix}")
}

fn split_cache_latency_ns(
    headers: &HashMap<String, String>,
    published_ns: u64,
    received_ns: u64,
) -> Option<(u64, u64)> {
    let available_us = headers
        .get("x-needletail-cache-available-unix-us")?
        .parse::<u64>()
        .ok()?;
    let available_ns = available_us.checked_mul(1_000)?;
    if available_ns < published_ns || received_ns < available_ns {
        return None;
    }
    Some((available_ns - published_ns, received_ns - available_ns))
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

#[derive(Clone)]
struct H3RequestSender {
    send_request: h3::client::SendRequest<h3_quinn::OpenStreams, Bytes>,
    authority: String,
}

impl H3RequestSender {
    async fn get(mut self, path: String) -> Result<SimpleHttpResponse> {
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

    fn request_sender(&self) -> H3RequestSender {
        H3RequestSender {
            send_request: self.send_request.clone(),
            authority: self.authority.clone(),
        }
    }

    async fn get(&self, path: &str) -> Result<SimpleHttpResponse> {
        self.request_sender().get(path.to_owned()).await
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

fn detect_init_audio_codec(bytes: &[u8]) -> Option<HlsAudioCodec> {
    [
        HlsAudioCodec::Ipcm,
        HlsAudioCodec::Fpcm,
        HlsAudioCodec::Flac,
    ]
    .into_iter()
    .find(|codec| {
        bytes
            .windows(codec.init_marker().len())
            .any(|window| window == codec.init_marker())
    })
}

fn pcm_init_parameters_match(bytes: &[u8], codec: HlsAudioCodec, expected_channels: u16) -> bool {
    let (sample_size, marker) = match codec {
        HlsAudioCodec::Flac => return true,
        HlsAudioCodec::Ipcm => (24, b"ipcm"),
        HlsAudioCodec::Fpcm => (32, b"fpcm"),
    };
    let Some(sample_entry) = bytes.windows(4).position(|window| window == marker) else {
        return false;
    };
    let Some(channel_bytes) = bytes.get(sample_entry + 20..sample_entry + 22) else {
        return false;
    };
    let channels = u16::from_be_bytes([channel_bytes[0], channel_bytes[1]]);
    let Some(pcmc) = bytes.windows(4).position(|window| window == b"pcmC") else {
        return false;
    };
    channels == expected_channels
        && bytes.get(pcmc + 8) == Some(&1)
        && bytes.get(pcmc + 9) == Some(&sample_size)
}

fn expected_pcm_part_bytes(codec: HlsAudioCodec, channels: u16, part_ms: u64) -> Option<u64> {
    let bytes_per_sample = match codec {
        HlsAudioCodec::Flac => return None,
        HlsAudioCodec::Ipcm => 3_u64,
        HlsAudioCodec::Fpcm => 4_u64,
    };
    u64::from(SAMPLE_RATE)
        .checked_mul(part_ms)?
        .checked_div(1_000)?
        .checked_mul(u64::from(channels))?
        .checked_mul(bytes_per_sample)
}

fn fmp4_mdat_payload(bytes: &[u8]) -> Option<&[u8]> {
    let mut offset = 0_usize;
    while offset.checked_add(8)? <= bytes.len() {
        let size = u32::from_be_bytes(bytes.get(offset..offset + 4)?.try_into().ok()?) as usize;
        if size < 8 || offset.checked_add(size)? > bytes.len() {
            return None;
        }
        if bytes.get(offset + 4..offset + 8)? == b"mdat" {
            return bytes.get(offset + 8..offset + size);
        }
        offset += size;
    }
    None
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

fn signal_s24le(
    first_sample: u64,
    channel_start: u16,
    channels: u16,
    signal: ProbeSignal,
) -> Vec<u8> {
    let mut pcm = Vec::with_capacity(FRAME_COUNT as usize * usize::from(channels) * 3);
    for frame in 0..FRAME_COUNT {
        let sample_index = first_sample.saturating_add(u64::from(frame));
        for local_channel in 0..channels {
            let channel = channel_start.saturating_add(local_channel);
            let sample = match signal {
                ProbeSignal::Sine => duplicated_sine_sample(sample_index),
                ProbeSignal::Decorrelated => decorrelated_sample(sample_index, channel),
                ProbeSignal::Noise => deterministic_noise_sample(sample_index, channel),
            };
            let bytes = sample.to_le_bytes();
            pcm.extend_from_slice(&bytes[..3]);
        }
    }
    pcm
}

#[cfg(test)]
fn sine_s24le(first_sample: u64) -> Vec<u8> {
    signal_s24le(first_sample, 0, DEFAULT_CHANNELS, ProbeSignal::Sine)
}

fn duplicated_sine_sample(sample_index: u64) -> i32 {
    let phase = sample_index as f64 * 2.0 * std::f64::consts::PI * 997.0 / f64::from(SAMPLE_RATE);
    s24_from_unit(phase.sin() * 0.5)
}

fn decorrelated_sample(sample_index: u64, channel: u16) -> i32 {
    let channel_f = f64::from(channel);
    let base_hz = 147.0 + f64::from((u32::from(channel) * 37) % 1_400);
    let overtone_hz = base_hz * 1.5 + f64::from((u32::from(channel) * 17) % 300);
    let t = sample_index as f64 / f64::from(SAMPLE_RATE);
    let phase_a = 2.0 * std::f64::consts::PI * base_hz * t + channel_f * 0.173;
    let phase_b = 2.0 * std::f64::consts::PI * overtone_hz * t + channel_f * 0.071;
    s24_from_unit(phase_a.sin() * 0.46 + phase_b.sin() * 0.21)
}

fn deterministic_noise_sample(sample_index: u64, channel: u16) -> i32 {
    let mut value = sample_index
        .wrapping_mul(0x9e37_79b9_7f4a_7c15)
        .wrapping_add(u64::from(channel).wrapping_mul(0xbf58_476d_1ce4_e5b9));
    value ^= value >> 30;
    value = value.wrapping_mul(0xbf58_476d_1ce4_e5b9);
    value ^= value >> 27;
    value = value.wrapping_mul(0x94d0_49bb_1331_11eb);
    value ^= value >> 31;
    ((value >> 40) as i32) - 8_388_608
}

fn s24_from_unit(value: f64) -> i32 {
    (value.clamp(-1.0, 1.0) * 8_388_607.0).round() as i32
}

fn ms_f64_to_ns(value_ms: f64) -> u64 {
    if !value_ms.is_finite() || value_ms <= 0.0 {
        return 0;
    }
    (value_ms * 1_000_000.0).round().min(u64::MAX as f64) as u64
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
    fn cache_commit_header_splits_publication_and_delivery_latency() {
        let headers = HashMap::from([(
            "x-needletail-cache-available-unix-us".to_owned(),
            "1250000".to_owned(),
        )]);
        assert_eq!(
            split_cache_latency_ns(&headers, 1_000_000_000, 1_275_000_000),
            Some((250_000_000, 25_000_000))
        );
        assert_eq!(
            split_cache_latency_ns(&headers, 1_300_000_000, 1_400_000_000),
            None
        );
    }

    #[test]
    fn hls_path_supports_edge_and_direct_origin_routes() {
        assert_eq!(
            hls_path("/live", 24_001, "tail?from=0"),
            "/live/24001/tail?from=0"
        );
        assert_eq!(hls_path("", 24_001, "stream.m3u8"), "/24001/stream.m3u8");
    }

    #[test]
    fn pcm_init_and_media_geometry_checks_are_strict() {
        let mut init = vec![0_u8; 64];
        init[4..8].copy_from_slice(b"ipcm");
        init[24..26].copy_from_slice(&8_u16.to_be_bytes());
        init[36..40].copy_from_slice(b"pcmC");
        init[44] = 1;
        init[45] = 24;

        assert_eq!(detect_init_audio_codec(&init), Some(HlsAudioCodec::Ipcm));
        assert!(pcm_init_parameters_match(&init, HlsAudioCodec::Ipcm, 8));
        assert!(!pcm_init_parameters_match(&init, HlsAudioCodec::Ipcm, 16));
        assert_eq!(
            expected_pcm_part_bytes(HlsAudioCodec::Ipcm, 8, 5),
            Some(5_760)
        );

        let mut media = Vec::new();
        media.extend_from_slice(&13_u32.to_be_bytes());
        media.extend_from_slice(b"free");
        media.extend_from_slice(b"hello");
        media.extend_from_slice(&12_u32.to_be_bytes());
        media.extend_from_slice(b"mdat");
        media.extend_from_slice(&[1, 2, 3, 4]);
        assert_eq!(fmp4_mdat_payload(&media), Some(&[1, 2, 3, 4][..]));
    }

    #[test]
    fn generated_pcm_is_exact_stereo_s24le_geometry() {
        assert_eq!(
            sine_s24le(0).len(),
            FRAME_COUNT as usize * usize::from(DEFAULT_CHANNELS) * 3
        );
    }

    #[test]
    fn source_group_plan_splits_16_channels_for_flac_safe_ll_hls() {
        let groups = source_group_plan(40_000, 16, 8).unwrap();
        assert_eq!(groups.len(), 2);
        assert_eq!(groups[0].group_id, 40_000);
        assert_eq!(groups[0].channel_start, 0);
        assert_eq!(groups[0].channel_count, 8);
        assert_eq!(groups[1].group_id, 40_001);
        assert_eq!(groups[1].channel_start, 8);
        assert_eq!(groups[1].channel_count, 8);
    }

    #[test]
    fn source_group_plan_splits_128_channels_for_sizing() {
        let groups = source_group_plan(10_000, 128, 8).unwrap();
        assert_eq!(groups.len(), 16);
        assert_eq!(groups.first().unwrap().channel_start, 0);
        assert_eq!(groups.last().unwrap().group_id, 10_015);
        assert_eq!(groups.last().unwrap().channel_start, 120);
        assert_eq!(groups.last().unwrap().channel_count, 8);
    }

    #[test]
    fn decorrelated_multichannel_signal_has_expected_s24le_geometry() {
        let pcm = signal_s24le(0, 8, 16, ProbeSignal::Decorrelated);
        assert_eq!(pcm.len(), FRAME_COUNT as usize * 16 * 3);
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
