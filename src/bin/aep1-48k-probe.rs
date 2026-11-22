use anyhow::{bail, Context, Result};
use av_contrib::fixture_replay::{
    capture_fixture, replay_fixture, CaptureFixtureOptions, ReplayFixtureOptions,
};
use av_contrib::opus_custom::{
    lane_opus_decoder_matches, new_lane_opus_decoder, new_lane_opus_encoder, opus_mode,
};
use boxer::fmp4::opus_packet_info;
use bytes::{Buf, Bytes, BytesMut};
use clap::{Parser, Subcommand, ValueEnum};
use frame_header::{EncodingFlag, FrameHeaderV2};
use futures_util::{
    future::{join_all, BoxFuture},
    stream::FuturesUnordered,
    FutureExt, StreamExt,
};
use music_audio_session::{
    DecodedMultichannelAudioGroup, MultichannelAudioReceiver, MultichannelAudioSender,
    MultichannelAudioSessionConfig,
};
use playlists::tail_bundle::{
    decode_tail_bundle, decode_tail_bundle_stream_frame, TailBundleEntry, MAX_TAIL_BUNDLE_ENTRIES,
    TAIL_BUNDLE_STREAM_CONTENT_TYPE,
};
use raptorq_datagram_fec::{
    inspect_multichannel_audio_datagram, AudioPayloadKind, AudioSampleFormat,
    MultichannelAudioEpoch, MultichannelAudioFecConfig, MultichannelAudioGroup,
};
use raptorq_fec_transport::MultichannelAudioTransportAdapter;
use serde::Serialize;
use soundkit::audio_packet::Decoder as _;
use soundkit::crypto::ChaCha20Poly1305PacketCipher;
use soundkit::frame_stream::{SoundKitFrameStream, SoundKitFrameStreamOptions};
use soundkit_flac::{FlacFrameConfig, FlacFrameEncoder, FlacProfile};
use soundkit_opus::{OpusDecoder, OpusEncoder};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::io::{self, Write};
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering as AtomicOrdering};
use std::sync::{Arc, Once};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpStream, UdpSocket};
use tokio::time::{
    interval_at, sleep, sleep_until, timeout, Instant as TokioInstant, MissedTickBehavior,
};

const SAMPLE_RATE: u32 = 48_000;
const DEFAULT_CHANNELS: u16 = 2;
const DEFAULT_GROUP_CHANNELS: u16 = 8;
const FLAC_MAX_CHANNELS: u16 = 8;
const LOGICAL_MAX_CHANNELS: u16 = 128;
const FRAME_COUNT: u32 = 240;
/// Default epoch of `send`: 48 samples (1 ms) of PCM or FLAC, and the DAW
/// lane's Opus frame, 48 samples (1 ms Opus Custom), of Opus.
const DEFAULT_PCM_EPOCH_SAMPLES: u32 = 48;
const DEFAULT_OPUS_EPOCH_SAMPLES: u32 = av_contrib::opus_custom::LANE_DEFAULT_OPUS_FRAME_SAMPLES;
/// Smallest epochs `send` makes: PCM has no codec floor and soundkit-flac's
/// encoder refuses blocks under 32 samples. Opus takes the frames of
/// `OPUS_EPOCH_SAMPLES` only.
const MIN_PCM_EPOCH_SAMPLES: u32 = 12;
const MIN_FLAC_EPOCH_SAMPLES: u32 = 32;
const MAX_EPOCH_SAMPLES: u32 = 48_000;
/// Opus frames the send command codes: Opus Custom (48, 64, 72 samples: 1,
/// 1.33, 1.5 ms, with libopus's signalling byte, as the DAW lane sends them)
/// and standard Opus (120 to 960 samples).
const OPUS_EPOCH_SAMPLES: [u32; 7] = [48, 64, 72, 120, 240, 480, 960];
/// Opus channels per AEP1 group: one Opus packet per group, as a DAW track
/// sends it, and libopus codes one or two channels.
const OPUS_MAX_GROUP_CHANNELS: u16 = 2;
/// The DAW lane's default Opus bitrate (daw-nexus `LANE_DEFAULT_OPUS_BITRATE`):
/// 320 kb/s CBR. `send --opus-bitrate 256000` is the lower-bandwidth option.
const PROBE_OPUS_BITRATE: u32 = av_contrib::opus_custom::LANE_DEFAULT_OPUS_BITRATE;
const MAX_DATAGRAM_BYTES: usize = 1_200;
const AUDIO_GROUP_FLAG_DISCONTINUITY: u8 = 1 << 0;
const AUDIO_GROUP_FLAG_ERASURE: u8 = 1 << 1;
// Cover 40 ms of bandwidth-delay product at the qualified 5 ms part size.
// Viewers use a nearby edge cache; a deeper intercontinental prefetch adds
// latency and is not a substitute for selecting the local mesh edge.
const H3_PART_PIPELINE_DEPTH: usize = 8;
const H3_PART_PRELOAD_LEAD: Duration = Duration::from_millis(100);
const HLS_CONTROL_REQUEST_TIMEOUT: Duration = Duration::from_secs(2);
const H3_REQUEST_RECONNECT_LIMIT: u64 = 3;
/// With --abandon-after-ms, a request this long past the nominal completion of
/// its part no longer holds back the requests for later parts.
const H3_PART_OVERDUE_GRACE_NS: u64 = 20_000_000;
/// Upper bound on unanswered part requests of one reader.
const H3_PART_PENDING_LIMIT: usize = 4_096;

/// Reconnects that one HTTP/3 reader may make before it fails. A multi-hour
/// soak raises the limit with NEEDLETAIL_PROBE_H3_RECONNECT_LIMIT, so that a
/// reader outlives a rare connection reset. The report counts each reconnect.
fn h3_request_reconnect_limit() -> u64 {
    static LIMIT: std::sync::OnceLock<u64> = std::sync::OnceLock::new();
    *LIMIT.get_or_init(|| {
        std::env::var("NEEDLETAIL_PROBE_H3_RECONNECT_LIMIT")
            .ok()
            .and_then(|value| value.parse().ok())
            .unwrap_or(H3_REQUEST_RECONNECT_LIMIT)
    })
}
// Keep four-hour load qualifications bounded while retaining a deterministic,
// uniform reservoir for latency percentiles. Exact continuity uses a bitset and
// is never sampled.
const MAX_LATENCY_SAMPLES: usize = 65_536;
const MAX_LATE_BUNDLE_OBSERVATIONS: usize = 512;

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
        /// Samples (at 48 kHz) in each epoch. Defaults to 48 (1 ms) for PCM
        /// and FLAC and to the DAW lane's 48 (1 ms Opus Custom) for Opus.
        /// PCM takes any value from 12, FLAC from 32, Opus one of 48, 64 or
        /// 72 (Opus Custom, with the signalling byte) or 120, 240, 480 or
        /// 960 (standard Opus; 120 is the interop setting).
        #[arg(long)]
        epoch_samples: Option<u32>,
        /// Opus bitrate, CBR, bits per second: the DAW lane's 320000 by
        /// default; 256000 is the lower-bandwidth option at 1 ms.
        #[arg(long, default_value_t = PROBE_OPUS_BITRATE,
              value_parser = clap::value_parser!(u32).range(6_000..=512_000))]
        opus_bitrate: u32,
        /// Logical channel count for the generated publication.
        #[arg(long, default_value_t = DEFAULT_CHANNELS)]
        channels: u16,
        /// Maximum channels per AEP1 group / LL-HLS FLAC rendition.
        #[arg(long, default_value_t = DEFAULT_GROUP_CHANNELS)]
        group_channels: u16,
        /// Deterministic source signal used to size codec and transport work.
        #[arg(long, value_enum, default_value_t = ProbeSignal::Decorrelated)]
        signal: ProbeSignal,
        /// Interleaved raw S24LE PCM to loop for the complete publication.
        #[arg(long, value_name = "PATH", conflicts_with = "signal")]
        pcm_s24le_file: Option<PathBuf>,
        #[arg(long, default_value_t = 0)]
        group_id: u16,
        #[arg(long, default_value_t = 12)]
        repair_percent: u32,
        #[arg(long, default_value_t = 1)]
        min_repair_symbols: u32,
        /// `precise`: absolute deadlines on a dedicated thread (sleep, then
        /// spin the last min(300 µs, epoch / 4)). `tokio`: a tokio interval
        /// of one epoch, which needs epochs of at least 1 ms.
        #[arg(long, value_enum, default_value_t = SendPace::Precise)]
        pace: SendPace,
    },
    /// Capture immutable codec-valid SoundKit v2 FLAC+Opus records from a live publication.
    CaptureFixture {
        #[arg(long)]
        relay: SocketAddr,
        #[arg(long)]
        bind: Option<SocketAddr>,
        #[arg(long)]
        session_id: u64,
        #[arg(long, default_value_t = 0)]
        group_id: u16,
        #[arg(long, default_value_t = 60)]
        duration_seconds: u64,
        #[arg(long, default_value_t = 8)]
        tail_seconds: u64,
        #[arg(long)]
        producer_sha256: String,
        #[arg(long)]
        output: PathBuf,
    },
    /// Replay fixture codec bytes with fresh AEP1/RaptorQ identity and absolute pacing.
    ReplayFixture {
        #[arg(long)]
        fixture: PathBuf,
        #[arg(long)]
        bind: SocketAddr,
        #[arg(long)]
        target: SocketAddr,
        #[arg(long)]
        secondary_bind: SocketAddr,
        #[arg(long)]
        secondary_target: SocketAddr,
        #[arg(long)]
        local_node_id: String,
        #[arg(long)]
        primary_node_id: String,
        #[arg(long)]
        secondary_node_id: String,
        #[arg(long)]
        topology_generation: u64,
        #[arg(long)]
        subscription_id: u64,
        #[arg(long, default_value_t = 10)]
        duration_seconds: u64,
        #[arg(long, default_value_t = 1)]
        tracks: u16,
        #[arg(long, default_value_t = 0)]
        group_id: u16,
        #[arg(long)]
        session_id: Option<u64>,
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
        /// Number of consecutive groups to observe through one relay subscription.
        #[arg(long, default_value_t = 1)]
        group_count: u16,
        /// Audio representations to include in exact-delivery totals.
        #[arg(long, value_enum, value_delimiter = ',', default_value = "flac")]
        formats: Vec<ReceiveAudioFormat>,
        #[arg(long, default_value_t = 10)]
        duration_seconds: u64,
        #[arg(long, default_value_t = 25)]
        deadline_ms: u64,
        #[arg(long, default_value_t = 3)]
        tail_seconds: u64,
        /// Decode every plain SoundKit Opus epoch received (in arrival
        /// order), choosing Opus Custom or standard Opus by its frame count.
        #[arg(long, default_value_t = false)]
        decode_soundkit_opus: bool,
        /// Every SoundKit Opus packet must declare this many samples per frame.
        #[arg(long)]
        expected_opus_frame_samples: Option<u32>,
        /// Write the decoded interleaved S16LE PCM (single group only).
        #[arg(long)]
        decoded_pcm_output: Option<PathBuf>,
    },
    ReceiveWebtransport {
        #[arg(long)]
        edge: SocketAddr,
        #[arg(long, default_value = "local.infidelity.io")]
        server_name: String,
        /// Additional PEM certificate authority for private/local qualification endpoints.
        #[arg(long)]
        tls_ca: Option<PathBuf>,
        #[arg(long)]
        session_id: u64,
        #[arg(long, default_value_t = 0)]
        group_id: u16,
        /// Audio representations to include in exact-delivery totals.
        #[arg(long, value_enum, value_delimiter = ',', default_value = "flac")]
        formats: Vec<ReceiveAudioFormat>,
        #[arg(long, default_value_t = 10)]
        duration_seconds: u64,
        #[arg(long, default_value_t = 250)]
        deadline_ms: u64,
        #[arg(long, default_value_t = 3)]
        tail_seconds: u64,
        /// Samples per epoch of the sender, for the expected epoch count.
        /// By default the frame count of the first epoch received (and 240,
        /// 5 ms, if none arrives).
        #[arg(long)]
        epoch_samples: Option<u32>,
    },
    ReceiveHls {
        #[arg(long)]
        edge: SocketAddr,
        #[arg(long, default_value = "local.infidelity.io")]
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
        /// HTTP response duration. A value larger than part-ms requests a
        /// blocking tail response containing consecutive cached media units.
        #[arg(long)]
        response_ms: Option<u64>,
        #[arg(long, default_value_t = 1000)]
        deadline_ms: u64,
        #[arg(long, default_value_t = 150)]
        render_buffer_ms: u64,
        /// Ignore media before this offset when qualifying a late join.
        #[arg(long, default_value_t = 0)]
        start_offset_ms: u64,
        /// Qualify only this many seconds after the start offset.
        #[arg(long)]
        window_seconds: Option<u64>,
        #[arg(long, default_value_t = 3)]
        tail_seconds: u64,
        /// Audio representation that every media part must preserve.
        #[arg(long, value_enum, default_value_t = HlsAudioCodec::Flac)]
        expected_audio_codec: HlsAudioCodec,
        /// PCM channels carried by this LL-HLS rendition; required for ipcm size checks.
        #[arg(long, default_value_t = 0)]
        expected_pcm_channels: u16,
        /// Packet protection the SoundKit Opus rendition must carry. Production
        /// publishers encrypt every packet. The direct qualification source
        /// publishes plain packets, which must then carry a valid packet CRC32.
        #[arg(long, value_enum, default_value_t = SoundkitOpusProtection::Encrypted)]
        soundkit_opus_protection: SoundkitOpusProtection,
        /// Object sequence of the part at offset zero of the publication. A
        /// reader that joins at --start-offset-ms then starts at exactly this
        /// sequence plus the offset in parts. Without it a late reader takes
        /// the newest playlist part as its first part, which places an opaque
        /// part (no timestamp of its own) only as exactly as the join time.
        #[arg(long)]
        object_sequence_origin: Option<u64>,
        /// Milliseconds after its nominal time at which a part that the edge
        /// does not have is given up: it is not requested again and counts as
        /// missing. Later parts are requested on time while an earlier part is
        /// overdue. Without this option a reader asks for a missing part until
        /// the run ends, and every part after it is late or missing.
        #[arg(long)]
        abandon_after_ms: Option<u64>,
        /// Decimal SoundKit media key, read from this file, for consumer-side Opus decode.
        #[arg(long)]
        soundkit_key_file: Option<PathBuf>,
        /// Decode every SoundKit Opus part as a consumer would. Encrypted
        /// packets need --soundkit-key-file; plain packets
        /// (--soundkit-opus-protection plain) need no key. Implied by
        /// --soundkit-key-file, --decoded-pcm-output and --reference-pcm-s16le.
        #[arg(long, default_value_t = false)]
        decode_soundkit_opus: bool,
        /// Every SoundKit Opus packet must declare this many samples per
        /// frame: 48, 64 or 72 for 1, 1.33 or 1.5 ms Opus Custom, 120 or more
        /// for standard Opus. A part with another frame size counts as an
        /// invalid Opus part.
        #[arg(long)]
        expected_opus_frame_samples: Option<u32>,
        /// Write consumer-decoded interleaved S16LE PCM to this path.
        #[arg(long)]
        decoded_pcm_output: Option<PathBuf>,
        /// Save each accepted opaque/media LL-HLS part body for offline inspection.
        #[arg(long)]
        raw_part_output_dir: Option<PathBuf>,
        /// Compare decoded Opus with this interleaved S16LE PCM reference.
        #[arg(long)]
        reference_pcm_s16le: Option<PathBuf>,
        /// Leading silent source frames inserted before the reference stem.
        #[arg(long, default_value_t = 512)]
        reference_leading_silence_frames: usize,
    },
    /// Decode saved opaque SoundKit Opus parts (for example from
    /// receive-hls --raw-part-output-dir), in the order given, with the
    /// consumer receive-hls uses: Opus Custom or standard Opus by each
    /// packet's frame count. Prints the consumer report.
    DecodeSoundkitOpus {
        /// Part files, one opaque LL-HLS part each, in media order.
        #[arg(required = true)]
        parts: Vec<PathBuf>,
        /// Packet protection: plain packets need no key; encrypted packets
        /// need --soundkit-key-file.
        #[arg(long, value_enum, default_value_t = SoundkitOpusProtection::Plain)]
        soundkit_opus_protection: SoundkitOpusProtection,
        #[arg(long)]
        soundkit_key_file: Option<PathBuf>,
        /// Every packet must declare this many samples per frame.
        #[arg(long)]
        expected_opus_frame_samples: Option<u32>,
        /// Write the decoded interleaved S16LE PCM to this path.
        #[arg(long)]
        decoded_pcm_output: Option<PathBuf>,
    },
    /// Run many independent persistent LL-HLS clients in one Rust process.
    LoadHls {
        #[arg(long)]
        edge: SocketAddr,
        #[arg(long, default_value = "local.infidelity.io")]
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
        /// Consecutive track streams multiplexed over each reader's H3
        /// connection. A browser normally reuses one connection per origin.
        #[arg(long, default_value_t = 1)]
        stream_count: usize,
        /// Fetch every requested track at the same media epoch in one opaque
        /// edge-tail response. Individual per-track LL-HLS URLs remain available.
        #[arg(long, default_value_t = false)]
        bundle_streams: bool,
        #[arg(long)]
        session_id: u64,
        #[arg(long, default_value_t = 10)]
        duration_seconds: u64,
        #[arg(long, default_value_t = 50)]
        part_ms: u64,
        /// HTTP response duration. A value larger than part-ms requests a
        /// blocking tail response containing consecutive cached media units.
        #[arg(long)]
        response_ms: Option<u64>,
        /// Maximum permitted capture-to-response latency. Qualification must
        /// declare this explicitly so a permissive generic default cannot be
        /// mistaken for a strict latency gate.
        #[arg(long)]
        deadline_ms: u64,
        #[arg(long, default_value_t = 0)]
        start_offset_ms: u64,
        /// Run each late-joining reader for only this many seconds.
        #[arg(long)]
        window_seconds: Option<u64>,
        #[arg(long, default_value_t = 3)]
        tail_seconds: u64,
        #[arg(long, default_value_t = 100)]
        readers: usize,
        /// Randomize each reader's late-join offset within this window.
        /// Offsets are aligned to media-part boundaries; use the same seed
        /// across stream-specific load processes to model one multitrack customer.
        #[arg(long, default_value_t = 0)]
        arrival_window_ms: u64,
        #[arg(long, default_value_t = 0x4e45_4544_4c45_5441)]
        arrival_seed: u64,
        /// Audio representation that every reader must verify.
        #[arg(long, value_enum, default_value_t = HlsAudioCodec::Flac)]
        expected_audio_codec: HlsAudioCodec,
        /// PCM channels carried by this LL-HLS rendition; required for ipcm size checks.
        #[arg(long, default_value_t = 0)]
        expected_pcm_channels: u16,
        /// Packet protection the SoundKit Opus rendition must carry. Production
        /// publishers encrypt every packet. The direct qualification source
        /// publishes plain packets, which must then carry a valid packet CRC32.
        #[arg(long, value_enum, default_value_t = SoundkitOpusProtection::Encrypted)]
        soundkit_opus_protection: SoundkitOpusProtection,
        /// Object sequence of the part at offset zero of the publication. A
        /// reader that joins at --start-offset-ms then starts at exactly this
        /// sequence plus the offset in parts. Without it a late reader takes
        /// the newest playlist part as its first part, which places an opaque
        /// part (no timestamp of its own) only as exactly as the join time.
        #[arg(long)]
        object_sequence_origin: Option<u64>,
        /// Milliseconds after its nominal time at which a part that the edge
        /// does not have is given up: it is not requested again and counts as
        /// missing. Later parts are requested on time while an earlier part is
        /// overdue. Without this option a reader asks for a missing part until
        /// the run ends, and every part after it is late or missing.
        #[arg(long)]
        abandon_after_ms: Option<u64>,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
enum ProbePayload {
    Flac,
    Pcm,
    /// CELT Opus frames encoded from the same PCM, framed as the DAW
    /// source frames its SoundKit Opus packets.
    Opus,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, ValueEnum)]
#[serde(rename_all = "lowercase")]
enum ReceiveAudioFormat {
    Opus,
    Flac,
    Pcm,
}

impl ReceiveAudioFormat {
    fn from_soundkit_v2_payload(payload: &[u8]) -> Option<Self> {
        let header_size = FrameHeaderV2::header_size(payload).ok()?;
        let header_bytes = payload.get(..header_size)?;
        let header = FrameHeaderV2::decode(&mut &header_bytes[..]).ok()?;
        let payload_size = usize::try_from(header.payload_size()).ok()?;
        let packet_size = header_size.checked_add(payload_size)?;
        if packet_size != payload.len() {
            return None;
        }
        if header.packet_crc32_value().is_some()
            && !matches!(
                header.verify_packet_crc32(header_bytes, &payload[header_size..]),
                Ok(true)
            )
        {
            return None;
        }
        match header.encoding() {
            EncodingFlag::Opus => Some(Self::Opus),
            EncodingFlag::FLAC => Some(Self::Flac),
            EncodingFlag::PCMSigned | EncodingFlag::PCMFloat => Some(Self::Pcm),
            EncodingFlag::AAC | EncodingFlag::H264 => None,
        }
    }
}

#[derive(Debug, Clone, Copy, ValueEnum)]
enum HlsTransport {
    H3,
    Http1,
}

/// Packet protection expected of a SoundKit v2 Opus rendition.
#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
enum SoundkitOpusProtection {
    /// Every packet carries the encrypted flag. A packet CRC32 is verified
    /// when the packet carries one.
    Encrypted,
    /// No packet carries the encrypted flag, and every packet carries a
    /// packet CRC32 that matches its header and payload.
    Plain,
}

impl SoundkitOpusProtection {
    fn as_str(self) -> &'static str {
        match self {
            Self::Encrypted => "encrypted",
            Self::Plain => "plain",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
enum HlsAudioCodec {
    Flac,
    OpaqueFlac,
    Opus,
    SoundkitOpus,
    Ipcm,
    Fpcm,
    /// H.264 video in fMP4 parts, with or without an audio track. The probe
    /// checks the structure and the timeline of each part. It does not decode.
    H264,
}

impl HlsAudioCodec {
    fn as_str(self) -> &'static str {
        match self {
            Self::Flac => "flac",
            Self::OpaqueFlac => "opaque_flac",
            Self::Opus => "opus",
            Self::SoundkitOpus => "soundkit_opus",
            Self::Ipcm => "ipcm_s24le",
            Self::Fpcm => "fpcm_f32le",
            Self::H264 => "h264_fmp4",
        }
    }

    fn media_extension(self) -> &'static str {
        match self {
            Self::OpaqueFlac | Self::SoundkitOpus => "bin",
            Self::Flac | Self::Opus | Self::Ipcm | Self::Fpcm | Self::H264 => "mp4",
        }
    }

    fn init_marker(self) -> Option<&'static [u8; 4]> {
        match self {
            Self::OpaqueFlac | Self::SoundkitOpus => None,
            Self::Flac => Some(b"fLaC"),
            Self::Opus => Some(b"Opus"),
            Self::Ipcm => Some(b"ipcm"),
            Self::Fpcm => Some(b"fpcm"),
            Self::H264 => Some(b"avc1"),
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
            Self::Opus => "opus",
        }
    }

    fn default_epoch_samples(self) -> u32 {
        match self {
            Self::Opus => DEFAULT_OPUS_EPOCH_SAMPLES,
            Self::Flac | Self::Pcm => DEFAULT_PCM_EPOCH_SAMPLES,
        }
    }

    /// The epoch size, or why it cannot be sent in this payload.
    fn epoch_samples(self, requested: Option<u32>) -> Result<u32> {
        let samples = requested.unwrap_or_else(|| self.default_epoch_samples());
        match self {
            Self::Pcm if !(MIN_PCM_EPOCH_SAMPLES..=MAX_EPOCH_SAMPLES).contains(&samples) => bail!(
                "--epoch-samples {samples}: PCM takes {MIN_PCM_EPOCH_SAMPLES} to {MAX_EPOCH_SAMPLES} samples"
            ),
            Self::Flac if !(MIN_FLAC_EPOCH_SAMPLES..=MAX_EPOCH_SAMPLES).contains(&samples) => bail!(
                "--epoch-samples {samples}: FLAC takes {MIN_FLAC_EPOCH_SAMPLES} to {MAX_EPOCH_SAMPLES} samples"
            ),
            Self::Opus if !OPUS_EPOCH_SAMPLES.contains(&samples) => bail!(
                "--epoch-samples {samples}: Opus takes one of 48, 64 or 72 samples (Opus Custom) or 120, 240, 480 or 960 (standard Opus)"
            ),
            _ => Ok(samples),
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

enum PcmSource {
    Generated(ProbeSignal),
    File(LoopingPcm),
}

impl PcmSource {
    fn new(signal: ProbeSignal, path: Option<&Path>, channels: u16) -> Result<Self> {
        path.map_or_else(
            || Ok(Self::Generated(signal)),
            |path| {
                let bytes = std::fs::read(path)
                    .with_context(|| format!("failed to read raw PCM input: {}", path.display()))?;
                LoopingPcm::new(bytes, channels).map(Self::File)
            },
        )
    }

    fn label(&self) -> &'static str {
        match self {
            Self::Generated(signal) => signal.as_str(),
            Self::File(_) => "file_loop",
        }
    }

    fn input_frames(&self) -> Option<u64> {
        match self {
            Self::Generated(_) => None,
            Self::File(source) => Some(source.frames as u64),
        }
    }

    fn group_s24le(
        &self,
        first_sample: u64,
        frame_count: u32,
        channel_start: u16,
        channels: u16,
    ) -> Vec<u8> {
        match self {
            Self::Generated(signal) => {
                signal_s24le(first_sample, frame_count, channel_start, channels, *signal)
            }
            Self::File(source) => {
                source.group_s24le(first_sample, frame_count, channel_start, channels)
            }
        }
    }
}

struct LoopingPcm {
    bytes: Vec<u8>,
    channels: usize,
    frames: usize,
}

impl LoopingPcm {
    fn new(bytes: Vec<u8>, channels: u16) -> Result<Self> {
        let channels = usize::from(channels);
        let frame_bytes = channels
            .checked_mul(3)
            .context("raw PCM frame size overflow")?;
        if bytes.is_empty() {
            bail!("raw PCM input must contain at least one frame");
        }
        if !bytes.len().is_multiple_of(frame_bytes) {
            bail!(
                "raw PCM input has {} bytes; expected complete {}-byte frames for {channels} channels",
                bytes.len(),
                frame_bytes
            );
        }
        Ok(Self {
            frames: bytes.len() / frame_bytes,
            bytes,
            channels,
        })
    }

    fn group_s24le(
        &self,
        first_sample: u64,
        frame_count: u32,
        channel_start: u16,
        channels: u16,
    ) -> Vec<u8> {
        let channel_start = usize::from(channel_start);
        let channels = usize::from(channels);
        debug_assert!(channel_start + channels <= self.channels);
        let frame_bytes = self.channels * 3;
        let group_bytes = channels * 3;
        let channel_offset = channel_start * 3;
        let mut pcm = Vec::with_capacity(frame_count as usize * group_bytes);
        let mut source_frame = (first_sample % self.frames as u64) as usize;
        for _ in 0..frame_count {
            let start = source_frame * frame_bytes + channel_offset;
            pcm.extend_from_slice(&self.bytes[start..start + group_bytes]);
            source_frame += 1;
            if source_frame == self.frames {
                source_frame = 0;
            }
        }
        pcm
    }
}

#[derive(Debug, Serialize)]
struct SendReport {
    schema: &'static str,
    lane: &'static str,
    payload: &'static str,
    signal: &'static str,
    input_pcm_frames: Option<u64>,
    session_id: u64,
    group_id: u16,
    group_count: u16,
    group_channels: u16,
    group_ids: Vec<u16>,
    sample_rate: u32,
    channels: u16,
    /// Samples in each epoch (`--epoch-samples`); `frame_count` is the same
    /// number under its old name.
    frame_count: u32,
    epoch_samples: u32,
    epoch_duration_us: f64,
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
    /// SIGTERM or SIGINT stopped the sender before `duration_seconds`;
    /// `epochs` and the lateness cover the epochs it sent.
    stopped_by_signal: bool,
    /// `precise` or `tokio` (`--pace`).
    pace: &'static str,
    /// How long before each deadline the precise pacer stops sleeping and
    /// spins: min(300 µs, epoch / 4).
    spin_budget_us: f64,
    /// First datagram of each epoch handed to the kernel, after the epoch's
    /// absolute deadline `session_id + epoch * epoch duration`.
    send_lateness_ms: Percentiles,
    /// Last datagram of each epoch handed to the kernel, after the deadline.
    send_complete_lateness_ms: Percentiles,
    /// Time to make the datagrams of each epoch, included in the lateness.
    encode_ms: Percentiles,
}

#[derive(Debug, Serialize)]
struct ReceiveReport {
    schema: &'static str,
    lane: &'static str,
    session_id: u64,
    group_id: u16,
    formats: Vec<ReceiveAudioFormat>,
    sample_rate: u32,
    duration_seconds: u64,
    expected_epochs: u64,
    received_epochs: u64,
    missing_epochs: u64,
    deadline_ms: u64,
    deadline_misses: u64,
    transport_counter_scope: &'static str,
    datagrams_received: u64,
    source_datagrams_received: u64,
    repair_datagrams_received: u64,
    systematic_shards_received: u64,
    raptorq_shards_recovered: u64,
    selected_raptorq_fragments_recovered: u64,
    opus_epochs: u64,
    flac_epochs: u64,
    pcm_fallback_epochs: u64,
    erasure_epochs: u64,
    unexpected_payload_epochs: u64,
    discontinuity_epochs: u64,
    expected_pcm_frames: u64,
    received_pcm_frames: u64,
    missing_pcm_frames: u64,
    duplicate_or_late_epochs: u64,
    transport_duplicate_or_late_epochs: u64,
    wire_bytes: u64,
    /// Capture-to-exact-decode latency at the point the epoch can be rendered.
    latency_ms: Percentiles,
    render_ready_latency_ms: Percentiles,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    latency_time_series: Vec<UdpLatencyBucket>,
    #[serde(flatten)]
    opus_frames: OpusFrameReport,
    #[serde(skip_serializing_if = "Option::is_none")]
    opus_consumer: Option<OpusConsumerReport>,
}

#[derive(Debug, Serialize)]
struct UdpTransportCounters {
    datagrams_received: u64,
    source_datagrams_received: u64,
    repair_datagrams_received: u64,
    systematic_shards_received: u64,
    raptorq_shards_recovered: u64,
    transport_duplicate_or_late_epochs: u64,
    wire_bytes: u64,
}

#[derive(Debug, Serialize)]
struct MultiGroupReceiveReport {
    schema: &'static str,
    lane: &'static str,
    session_id: u64,
    group_id: u16,
    group_count: u16,
    group_ids: Vec<u16>,
    shared_transport: UdpTransportCounters,
    groups: Vec<ReceiveReport>,
}

#[derive(Debug, Serialize)]
#[serde(untagged)]
enum UdpReceiveOutput {
    Single(Box<ReceiveReport>),
    Multiple(MultiGroupReceiveReport),
}

struct ReceiveFormatState {
    formats: Vec<ReceiveAudioFormat>,
    selected_epochs: HashSet<(ReceiveAudioFormat, u64)>,
    opus_epochs: HashSet<u64>,
    flac_epochs: HashSet<u64>,
    pcm_epochs: HashSet<u64>,
    deadline_misses: u64,
    erasure_epochs: u64,
    discontinuity_epochs: u64,
    duplicate_or_late_epochs: u64,
    selected_raptorq_fragments_recovered: u64,
    received_pcm_frames: u64,
    latencies_ns: Vec<u64>,
    latency_buckets: BTreeMap<(u8, u64), UdpLatencyBucketSamples>,
    /// Frame count of the first selected epoch received: the sender's
    /// epoch size (`send --epoch-samples`).
    epoch_frames: Option<u32>,
}

/// The epoch of the old fixed sender, 5 ms: what a receiver assumes when it
/// has neither a setting nor an epoch to go by.
const LEGACY_EPOCH_SAMPLES: u32 = 240;

/// A note for the sender's stderr when its epochs are not 5 ms: receivers
/// that still count 5 ms epochs report wrong expected and missing counts.
fn receiver_epoch_note(epoch_samples: u32) -> Option<String> {
    (epoch_samples != LEGACY_EPOCH_SAMPLES).then(|| {
        format!(
            "note: epochs of {epoch_samples} samples. receive-udp still counts expected and \
             missing epochs (and its per-second buckets) for 240-sample (5 ms) epochs, so its \
             loss figures are wrong for this run; receive-webtransport counts from the epochs \
             it receives (or --epoch-samples); receive-hls tracks parts of whole milliseconds \
             only (--part-ms)."
        )
    })
}

impl ReceiveFormatState {
    /// Epochs of one format that the sender makes in `duration_seconds`:
    /// from `epoch_samples` if given, else from the epochs received, else
    /// 5 ms epochs. A fixed 5 ms hid loss once the sender's default epoch
    /// became 1 ms: 1,000 epochs a second arrived against 200 expected, so
    /// the missing count read 0 at 50% loss.
    fn expected_epochs(&self, duration_seconds: u64, epoch_samples: Option<u32>) -> u64 {
        let epoch_samples = epoch_samples
            .or(self.epoch_frames)
            .filter(|samples| *samples > 0)
            .unwrap_or(LEGACY_EPOCH_SAMPLES);
        duration_seconds
            .saturating_mul(u64::from(SAMPLE_RATE))
            .saturating_div(u64::from(epoch_samples))
    }

    fn new(formats: &[ReceiveAudioFormat]) -> Result<Self> {
        let formats = formats
            .iter()
            .copied()
            .fold(Vec::new(), |mut unique, format| {
                if !unique.contains(&format) {
                    unique.push(format);
                }
                unique
            });
        if formats.is_empty() {
            bail!("--formats must contain at least one audio representation");
        }
        Ok(Self {
            formats,
            selected_epochs: HashSet::new(),
            opus_epochs: HashSet::new(),
            flac_epochs: HashSet::new(),
            pcm_epochs: HashSet::new(),
            deadline_misses: 0,
            erasure_epochs: 0,
            discontinuity_epochs: 0,
            duplicate_or_late_epochs: 0,
            selected_raptorq_fragments_recovered: 0,
            received_pcm_frames: 0,
            latencies_ns: Vec::new(),
            latency_buckets: BTreeMap::new(),
            epoch_frames: None,
        })
    }

    fn observe(
        &mut self,
        group: &DecodedMultichannelAudioGroup,
        arrival_ns: u64,
        deadline_ms: u64,
        window: &ReceiveWindow,
    ) {
        if group.payload_kind != AudioPayloadKind::SoundKitV2 {
            return;
        }
        let capture_ns = group.session_id.saturating_add(
            group.pts_samples.saturating_mul(1_000_000_000) / u64::from(group.sample_rate),
        );
        if !window.counts(capture_ns) {
            return;
        }
        let Some(format) = ReceiveAudioFormat::from_soundkit_v2_payload(&group.payload) else {
            return;
        };
        if group.flags & AUDIO_GROUP_FLAG_ERASURE == 0 {
            match format {
                ReceiveAudioFormat::Opus => &mut self.opus_epochs,
                ReceiveAudioFormat::Flac => &mut self.flac_epochs,
                ReceiveAudioFormat::Pcm => &mut self.pcm_epochs,
            }
            .insert(group.epoch_id);
        }
        if !self.formats.contains(&format) {
            return;
        }
        if self.epoch_frames.is_none() && group.frame_count > 0 {
            self.epoch_frames = Some(group.frame_count);
        }
        if !self.selected_epochs.insert((format, group.epoch_id)) {
            self.duplicate_or_late_epochs = self.duplicate_or_late_epochs.saturating_add(1);
            return;
        }

        let latency_ns = arrival_ns.saturating_sub(capture_ns);
        let deadline_missed = latency_ns > deadline_ms.saturating_mul(1_000_000);
        if deadline_missed {
            self.deadline_misses = self.deadline_misses.saturating_add(1);
        }
        let discontinuity = group.flags & AUDIO_GROUP_FLAG_DISCONTINUITY != 0;
        if discontinuity {
            self.discontinuity_epochs = self.discontinuity_epochs.saturating_add(1);
        }
        let erasure = group.flags & AUDIO_GROUP_FLAG_ERASURE != 0;
        if erasure {
            self.erasure_epochs = self.erasure_epochs.saturating_add(1);
        } else {
            self.received_pcm_frames = self
                .received_pcm_frames
                .saturating_add(u64::from(group.frame_count));
        }
        self.selected_raptorq_fragments_recovered = self
            .selected_raptorq_fragments_recovered
            .saturating_add(u64::from(group.raptorq_recovered_fragments));
        self.latencies_ns.push(latency_ns);
        self.latency_buckets
            .entry((
                receive_audio_format_order(format),
                window.second_of(capture_ns),
            ))
            .or_default()
            .record(latency_ns, deadline_missed, erasure, discontinuity);
    }

    fn selected_epoch_count(&self) -> u64 {
        self.selected_epochs.len() as u64
    }

    fn finish_latency_time_series(
        &mut self,
        session_id: u64,
        duration_seconds: u64,
    ) -> Vec<UdpLatencyBucket> {
        let mut buckets = Vec::new();
        for second in 0..duration_seconds {
            for format in &self.formats {
                let samples = self
                    .latency_buckets
                    .remove(&(receive_audio_format_order(*format), second))
                    .unwrap_or_default();
                let received_epochs = samples.latencies_ns.len() as u64;
                let expected_epochs = 1_000 / 5;
                buckets.push(UdpLatencyBucket {
                    format: *format,
                    start_offset_ms: second.saturating_mul(1_000),
                    start_unix_ns: session_id.saturating_add(second.saturating_mul(1_000_000_000)),
                    bucket_ms: 1_000,
                    expected_epochs,
                    received_epochs,
                    missing_epochs: expected_epochs.saturating_sub(received_epochs),
                    erasure_epochs: samples.erasure_epochs,
                    discontinuity_epochs: samples.discontinuity_epochs,
                    deadline_misses: samples.deadline_misses,
                    render_ready_latency_ms: percentiles_ms(samples.latencies_ns),
                });
            }
        }
        buckets
    }
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
    connection_reconnects: u64,
    path_prefix: String,
    stream_id: u64,
    session_id: u64,
    sample_rate: u32,
    duration_seconds: u64,
    part_ms: u64,
    response_ms: u64,
    parts_per_response: u64,
    media_responses: u64,
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
    end_offset_ms: u64,
    wire_bytes: u64,
    init_has_flac: bool,
    expected_audio_codec: &'static str,
    init_audio_codec: Option<&'static str>,
    init_audio_codec_verified: bool,
    expected_pcm_channels: u16,
    soundkit_opus_protection: &'static str,
    pcm_media_parts_verified: u64,
    pcm_media_size_mismatches: u64,
    opaque_flac_media_parts_verified: u64,
    opaque_flac_frame_mismatches: u64,
    opus_media_parts_verified: u64,
    opus_media_packet_mismatches: u64,
    fmp4_video_parts_verified: u64,
    fmp4_video_part_mismatches: u64,
    #[serde(flatten)]
    opus_frames: OpusFrameReport,
    opus_consumer: Option<OpusConsumerReport>,
    playlist_has_ll_hls_tags: bool,
    publication_to_cache_latency_ms: Percentiles,
    cache_to_client_latency_ms: Percentiles,
    availability_latency_ms: Percentiles,
    first_part_to_response_latency_ms: Percentiles,
    final_part_to_response_latency_ms: Percentiles,
    estimated_render_latency_ms: Percentiles,
    latency_time_series: Vec<HlsLatencyBucket>,
}

#[derive(Debug, Serialize)]
struct OpusConsumerReport {
    protection: &'static str,
    /// Frame counts of the decoded packets (48 for 1 ms Opus Custom).
    opus_frame_samples: Vec<u32>,
    /// `custom`, `standard`, `mixed` or none.
    opus_mode: Option<&'static str>,
    soundkit_v2_packets: u64,
    encrypted_packets: u64,
    plain_packets: u64,
    decoded_opus_packets: u64,
    sample_rate: u32,
    channels: u8,
    decoded_pcm_frames: u64,
    decoded_rms_dbfs: f64,
    decoded_peak: u16,
    pending_out_of_order_parts: usize,
    reference_frames: Option<u64>,
    aligned_reference_frames: Option<u64>,
    alignment_frames: Option<i64>,
    normalized_correlation: Option<f64>,
    waveform_verified: bool,
}

#[derive(Debug, Serialize)]
struct HlsLoadReport {
    schema: &'static str,
    transport: &'static str,
    tls_protocol: &'static str,
    persistent_connections: bool,
    persistent_bundle_response: bool,
    edge: SocketAddr,
    stream_id: u64,
    stream_count: usize,
    session_id: u64,
    duration_seconds: u64,
    part_ms: u64,
    response_ms: u64,
    parts_per_response: u64,
    tail_bundle: bool,
    tracks_per_media_response: usize,
    media_responses_total: u64,
    start_offset_ms: u64,
    end_offset_ms: u64,
    deadline_ms: u64,
    arrival_window_ms: u64,
    arrival_seed: u64,
    expected_audio_codec: &'static str,
    expected_pcm_channels: u16,
    soundkit_opus_protection: &'static str,
    customers_requested: usize,
    h3_connections: usize,
    readers_requested: usize,
    readers_completed: usize,
    readers_failed: usize,
    readers_with_incomplete_media: usize,
    expected_parts_per_reader: u64,
    received_parts_total: u64,
    missing_parts_total: u64,
    non_contiguous_pts_total: u64,
    deadline_misses_total: u64,
    late_bundle_observations: Vec<HlsLateBundleObservation>,
    init_verified_readers: usize,
    pcm_media_size_mismatches_total: u64,
    opaque_flac_frame_mismatches_total: u64,
    opus_media_packet_mismatches_total: u64,
    fmp4_video_part_mismatches_total: u64,
    playlist_verified_readers: usize,
    wire_bytes_total: u64,
    connection_setup_ms: Percentiles,
    availability_p99_ms_across_readers: Percentiles,
    first_part_to_response_p99_ms_across_readers: Percentiles,
    final_part_to_response_p99_ms_across_readers: Percentiles,
    cache_to_client_p99_ms_across_readers: Percentiles,
    elapsed_ms: u64,
    passed: bool,
    errors: Vec<String>,
}

#[derive(Debug, Serialize)]
struct HlsLateBundleObservation {
    customer_id: usize,
    start_sequence: u64,
    end_sequence: u64,
    capture_unix_ns: u64,
    arrival_unix_ns: u64,
    availability_latency_ms: f64,
    lateness_ms: f64,
    publication_to_cache_ms: Option<f64>,
    cache_to_client_ms: Option<f64>,
}

#[derive(Debug, Clone, Serialize)]
struct Percentiles {
    count: usize,
    sample_count: usize,
    min: f64,
    p50: f64,
    p95: f64,
    p99: f64,
    max: f64,
}

#[derive(Debug, Serialize)]
struct UdpLatencyBucket {
    format: ReceiveAudioFormat,
    start_offset_ms: u64,
    start_unix_ns: u64,
    bucket_ms: u64,
    expected_epochs: u64,
    received_epochs: u64,
    missing_epochs: u64,
    erasure_epochs: u64,
    discontinuity_epochs: u64,
    deadline_misses: u64,
    render_ready_latency_ms: Percentiles,
}

#[derive(Debug, Default)]
struct UdpLatencyBucketSamples {
    latencies_ns: Vec<u64>,
    erasure_epochs: u64,
    discontinuity_epochs: u64,
    deadline_misses: u64,
}

impl UdpLatencyBucketSamples {
    fn record(
        &mut self,
        latency_ns: u64,
        deadline_missed: bool,
        erasure: bool,
        discontinuity: bool,
    ) {
        self.latencies_ns.push(latency_ns);
        self.deadline_misses = self
            .deadline_misses
            .saturating_add(u64::from(deadline_missed));
        self.erasure_epochs = self.erasure_epochs.saturating_add(u64::from(erasure));
        self.discontinuity_epochs = self
            .discontinuity_epochs
            .saturating_add(u64::from(discontinuity));
    }
}

#[derive(Debug, Serialize)]
struct HlsLatencyBucket {
    start_offset_ms: u64,
    start_unix_ns: u64,
    bucket_ms: u64,
    expected_parts: u64,
    received_parts: u64,
    missing_parts: u64,
    deadline_misses: u64,
    availability_latency_ms: Percentiles,
    publication_to_cache_latency_ms: Percentiles,
    cache_to_client_latency_ms: Percentiles,
    estimated_render_latency_ms: Percentiles,
}

#[derive(Debug, Default)]
struct HlsLatencyBucketSamples {
    availability_ns: Vec<u64>,
    publication_to_cache_ns: Vec<u64>,
    cache_to_client_ns: Vec<u64>,
    render_ns: Vec<u64>,
    deadline_misses: u64,
}

#[derive(Debug, Default)]
struct HlsLatencyTimeSeries {
    buckets: BTreeMap<u64, HlsLatencyBucketSamples>,
}

impl HlsLatencyTimeSeries {
    fn record_part(
        &mut self,
        pts_ms: u64,
        availability_ns: u64,
        render_ns: u64,
        deadline_missed: bool,
    ) {
        let bucket = self.buckets.entry(pts_ms / 1_000).or_default();
        bucket.availability_ns.push(availability_ns);
        bucket.render_ns.push(render_ns);
        bucket.deadline_misses = bucket
            .deadline_misses
            .saturating_add(u64::from(deadline_missed));
    }

    fn record_cache(&mut self, pts_ms: u64, publication_to_cache_ns: u64, cache_to_client_ns: u64) {
        let bucket = self.buckets.entry(pts_ms / 1_000).or_default();
        bucket.publication_to_cache_ns.push(publication_to_cache_ns);
        bucket.cache_to_client_ns.push(cache_to_client_ns);
    }

    fn finish(
        mut self,
        session_id: u64,
        start_offset_ms: u64,
        end_offset_ms: u64,
        part_ms: u64,
    ) -> Vec<HlsLatencyBucket> {
        let mut output = Vec::new();
        let mut bucket_start_ms = start_offset_ms / 1_000 * 1_000;
        while bucket_start_ms < end_offset_ms {
            let range_start_ms = bucket_start_ms.max(start_offset_ms);
            let range_end_ms = bucket_start_ms.saturating_add(1_000).min(end_offset_ms);
            let expected_parts = range_end_ms
                .saturating_sub(range_start_ms)
                .saturating_div(part_ms);
            let samples = self
                .buckets
                .remove(&(bucket_start_ms / 1_000))
                .unwrap_or_default();
            let received_parts = samples.availability_ns.len() as u64;
            output.push(HlsLatencyBucket {
                start_offset_ms: bucket_start_ms,
                start_unix_ns: session_id.saturating_add(bucket_start_ms.saturating_mul(1_000_000)),
                bucket_ms: 1_000,
                expected_parts,
                received_parts,
                missing_parts: expected_parts.saturating_sub(received_parts),
                deadline_misses: samples.deadline_misses,
                availability_latency_ms: percentiles_ms(samples.availability_ns),
                publication_to_cache_latency_ms: percentiles_ms(samples.publication_to_cache_ns),
                cache_to_client_latency_ms: percentiles_ms(samples.cache_to_client_ns),
                estimated_render_latency_ms: percentiles_ms(samples.render_ns),
            });
            bucket_start_ms = bucket_start_ms.saturating_add(1_000);
        }
        output
    }
}

fn receive_audio_format_order(format: ReceiveAudioFormat) -> u8 {
    match format {
        ReceiveAudioFormat::Opus => 0,
        ReceiveAudioFormat::Flac => 1,
        ReceiveAudioFormat::Pcm => 2,
    }
}

struct PartCoverage {
    start_sequence: u64,
    part_ms: u64,
    expected_parts: u64,
    words: Vec<u64>,
    received_parts: u64,
}

struct MediaTimelineCoverage {
    start_sequence: u64,
    expected_parts: u64,
    start_offset_ms: u64,
    end_offset_ms: u64,
    parts: BTreeMap<u64, (u64, u64)>,
}

impl MediaTimelineCoverage {
    fn new(
        start_sequence: u64,
        expected_parts: u64,
        start_offset_ms: u64,
        end_offset_ms: u64,
    ) -> Self {
        Self {
            start_sequence,
            expected_parts,
            start_offset_ms,
            end_offset_ms,
            parts: BTreeMap::new(),
        }
    }

    fn insert(&mut self, sequence: u64, pts_ms: u64, duration_ms: u64) {
        self.parts.entry(sequence).or_insert((pts_ms, duration_ms));
    }

    fn discontinuities(&self) -> u64 {
        let mut previous: Option<(u64, u64, u64)> = None;
        let mut discontinuities = 0_u64;
        for (&sequence, &(pts_ms, duration_ms)) in &self.parts {
            if let Some((previous_sequence, previous_pts_ms, previous_duration_ms)) = previous {
                if sequence == previous_sequence + 1
                    && pts_ms != previous_pts_ms.saturating_add(previous_duration_ms)
                {
                    discontinuities = discontinuities.saturating_add(1);
                }
            }
            previous = Some((sequence, pts_ms, duration_ms));
        }

        if self.parts.len() as u64 == self.expected_parts {
            let first = self.parts.get(&self.start_sequence);
            let last_sequence = self
                .start_sequence
                .saturating_add(self.expected_parts.saturating_sub(1));
            let last = self.parts.get(&last_sequence);
            if first.is_none_or(|(pts_ms, _)| pts_ms.abs_diff(self.start_offset_ms) > 5) {
                discontinuities = discontinuities.saturating_add(1);
            }
            if last.is_none_or(|(pts_ms, duration_ms)| {
                pts_ms
                    .saturating_add(*duration_ms)
                    .abs_diff(self.end_offset_ms)
                    > 5
            }) {
                discontinuities = discontinuities.saturating_add(1);
            }
        }
        discontinuities
    }
}

impl PartCoverage {
    fn new(start_sequence: u64, expected_parts: u64, part_ms: u64) -> Result<Self> {
        let word_count = expected_parts.saturating_add(63) / 64;
        let word_count = usize::try_from(word_count)
            .context("LL-HLS qualification duration exceeds addressable memory")?;
        let mut words = Vec::new();
        words
            .try_reserve_exact(word_count)
            .context("could not allocate LL-HLS continuity bitset")?;
        words.resize(word_count, 0);
        Ok(Self {
            start_sequence,
            part_ms,
            expected_parts,
            words,
            received_parts: 0,
        })
    }

    fn insert_pts(&mut self, pts_ms: u64) -> bool {
        if !pts_ms.is_multiple_of(self.part_ms) {
            return false;
        }
        let sequence = pts_ms / self.part_ms;
        let Some(relative) = sequence.checked_sub(self.start_sequence) else {
            return false;
        };
        if relative >= self.expected_parts {
            return false;
        }
        let word = usize::try_from(relative / 64).expect("bitset index fits allocated memory");
        let mask = 1_u64 << (relative % 64);
        if self.words[word] & mask != 0 {
            return false;
        }
        self.words[word] |= mask;
        self.received_parts = self.received_parts.saturating_add(1);
        true
    }

    fn contains_relative(&self, relative: u64) -> bool {
        let word = usize::try_from(relative / 64).expect("bitset index fits allocated memory");
        self.words
            .get(word)
            .is_some_and(|value| value & (1_u64 << (relative % 64)) != 0)
    }

    fn first_missing_relative(&self) -> Option<u64> {
        (0..self.expected_parts).find(|relative| !self.contains_relative(*relative))
    }

    fn first_pts_ms(&self) -> Option<u64> {
        (0..self.expected_parts)
            .find(|relative| self.contains_relative(*relative))
            .map(|relative| {
                self.start_sequence
                    .saturating_add(relative)
                    .saturating_mul(self.part_ms)
            })
    }

    fn last_pts_ms(&self) -> Option<u64> {
        (0..self.expected_parts)
            .rev()
            .find(|relative| self.contains_relative(*relative))
            .map(|relative| {
                self.start_sequence
                    .saturating_add(relative)
                    .saturating_mul(self.part_ms)
            })
    }

    fn non_contiguous_pts(&self) -> u64 {
        let mut previous = None;
        let mut gaps = 0_u64;
        for relative in 0..self.expected_parts {
            if !self.contains_relative(relative) {
                continue;
            }
            if previous.is_some_and(|value| relative != value + 1) {
                gaps = gaps.saturating_add(1);
            }
            previous = Some(relative);
        }
        gaps
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct HlsPlaylistObjectWindow {
    first_sequence: Option<u64>,
    last_sequence: Option<u64>,
    next_sequence: u64,
}

struct BoundedLatencySamples {
    observations: usize,
    values_ns: Vec<u64>,
}

impl BoundedLatencySamples {
    fn new() -> Self {
        Self {
            observations: 0,
            values_ns: Vec::with_capacity(MAX_LATENCY_SAMPLES),
        }
    }

    fn record(&mut self, value_ns: u64) {
        self.observations = self.observations.saturating_add(1);
        if self.values_ns.len() < MAX_LATENCY_SAMPLES {
            self.values_ns.push(value_ns);
            return;
        }
        let candidate = deterministic_sample_index(self.observations as u64);
        let slot = candidate % self.observations as u64;
        if slot < MAX_LATENCY_SAMPLES as u64 {
            self.values_ns[slot as usize] = value_ns;
        }
    }

    fn summary(self) -> Percentiles {
        percentiles_ms_with_count(self.values_ns, self.observations)
    }
}

struct WebTransportReceiveOptions<'a> {
    edge: SocketAddr,
    server_name: &'a str,
    tls_ca: Option<&'a Path>,
    session_id: u64,
    group_id: u16,
    formats: &'a [ReceiveAudioFormat],
    duration_seconds: u64,
    deadline_ms: u64,
    tail_seconds: u64,
    epoch_samples: Option<u32>,
}

struct UdpReceiveOptions<'a> {
    relay: SocketAddr,
    bind: Option<SocketAddr>,
    session_id: u64,
    group_id: u16,
    group_count: u16,
    formats: &'a [ReceiveAudioFormat],
    duration_seconds: u64,
    deadline_ms: u64,
    tail_seconds: u64,
    decode_soundkit_opus: bool,
    expected_opus_frame_samples: Option<u32>,
    decoded_pcm_output: Option<&'a Path>,
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
    response_ms: u64,
    deadline_ms: u64,
    render_buffer_ms: u64,
    start_offset_ms: u64,
    window_seconds: Option<u64>,
    tail_seconds: u64,
    expected_audio_codec: HlsAudioCodec,
    expected_pcm_channels: u16,
    soundkit_opus_protection: SoundkitOpusProtection,
    object_sequence_origin: Option<u64>,
    abandon_after_ms: Option<u64>,
    soundkit_key_file: Option<&'a Path>,
    decode_soundkit_opus: bool,
    expected_opus_frame_samples: Option<u32>,
    decoded_pcm_output: Option<&'a Path>,
    raw_part_output_dir: Option<&'a Path>,
    reference_pcm_s16le: Option<&'a Path>,
    reference_leading_silence_frames: usize,
    shared_h3: Option<SharedH3Client>,
}

#[derive(Clone)]
struct HlsLoadOptions {
    edge: SocketAddr,
    server_name: String,
    tls_ca: Option<PathBuf>,
    transport: HlsTransport,
    path_prefix: String,
    stream_id: u64,
    stream_count: usize,
    bundle_streams: bool,
    session_id: u64,
    duration_seconds: u64,
    part_ms: u64,
    response_ms: u64,
    deadline_ms: u64,
    start_offset_ms: u64,
    window_seconds: Option<u64>,
    tail_seconds: u64,
    readers: usize,
    arrival_window_ms: u64,
    arrival_seed: u64,
    expected_audio_codec: HlsAudioCodec,
    expected_pcm_channels: u16,
    soundkit_opus_protection: SoundkitOpusProtection,
    object_sequence_origin: Option<u64>,
    abandon_after_ms: Option<u64>,
}

struct SoundKitOpusConsumer {
    parser: SoundKitFrameStream,
    /// Encrypted (with the media key) or plain packets (CRC required).
    protection: SoundkitOpusProtection,
    decoder: Option<OpusDecoder>,
    /// Frame counts of the decoded packets, and the one every packet must
    /// have, if any.
    frame_samples: std::collections::BTreeSet<u32>,
    expected_frame_samples: Option<u32>,
    next_sequence: u64,
    pending_parts: BTreeMap<u64, Vec<u8>>,
    decoded_pcm: Vec<i16>,
    capture_pcm: bool,
    decoded_pcm_output: Option<PathBuf>,
    reference_pcm_bytes: Option<Vec<u8>>,
    reference_leading_silence_frames: usize,
    reference_start_offset_frames: usize,
    reference_window_frames: Option<usize>,
    soundkit_v2_packets: u64,
    encrypted_packets: u64,
    plain_packets: u64,
    decoded_opus_packets: u64,
    sample_rate: u32,
    channels: u8,
    decoded_pcm_frames: u64,
    sum_squares: f64,
    sample_count: u64,
    peak: u16,
}

/// What the consumer decodes and where it writes and compares the PCM.
struct OpusConsumerOptions<'a> {
    first_sequence: u64,
    decoded_pcm_output: Option<&'a Path>,
    reference_pcm_s16le: Option<&'a Path>,
    reference_leading_silence_frames: usize,
    reference_start_offset_ms: u64,
    reference_window_ms: u64,
    expected_frame_samples: Option<u32>,
}

impl SoundKitOpusConsumer {
    /// A consumer of encrypted packets, with the media key in `key_file`.
    fn from_key_file(key_file: &Path, options: OpusConsumerOptions<'_>) -> Result<Self> {
        let encoded_key = std::fs::read_to_string(key_file)
            .with_context(|| format!("read SoundKit key file {}", key_file.display()))?;
        let cipher = ChaCha20Poly1305PacketCipher::new_from_decimal_key(encoded_key.trim())
            .context("SoundKit key file did not contain a valid decimal key")?;
        Self::new(Some(cipher), SoundkitOpusProtection::Encrypted, options)
    }

    /// A consumer of plain packets (`--soundkit-opus-protection plain`):
    /// every packet must carry a matching packet CRC32 and no encryption.
    fn plain(options: OpusConsumerOptions<'_>) -> Result<Self> {
        Self::new(None, SoundkitOpusProtection::Plain, options)
    }

    fn new(
        cipher: Option<ChaCha20Poly1305PacketCipher>,
        protection: SoundkitOpusProtection,
        options: OpusConsumerOptions<'_>,
    ) -> Result<Self> {
        let OpusConsumerOptions {
            first_sequence,
            decoded_pcm_output,
            reference_pcm_s16le,
            reference_leading_silence_frames,
            reference_start_offset_ms,
            reference_window_ms,
            expected_frame_samples,
        } = options;
        let reference_pcm_bytes = reference_pcm_s16le
            .map(|path| {
                std::fs::read(path)
                    .with_context(|| format!("read PCM reference {}", path.display()))
            })
            .transpose()?;
        Ok(Self {
            parser: SoundKitFrameStream::new(SoundKitFrameStreamOptions {
                cipher,
                ..SoundKitFrameStreamOptions::default()
            }),
            protection,
            decoder: None,
            frame_samples: std::collections::BTreeSet::new(),
            expected_frame_samples,
            next_sequence: first_sequence,
            pending_parts: BTreeMap::new(),
            decoded_pcm: Vec::new(),
            capture_pcm: decoded_pcm_output.is_some() || reference_pcm_bytes.is_some(),
            decoded_pcm_output: decoded_pcm_output.map(Path::to_path_buf),
            reference_pcm_bytes,
            reference_leading_silence_frames,
            reference_start_offset_frames: millis_to_frames(reference_start_offset_ms),
            reference_window_frames: (reference_window_ms > 0)
                .then(|| millis_to_frames(reference_window_ms)),
            soundkit_v2_packets: 0,
            encrypted_packets: 0,
            plain_packets: 0,
            decoded_opus_packets: 0,
            sample_rate: 0,
            channels: 0,
            decoded_pcm_frames: 0,
            sum_squares: 0.0,
            sample_count: 0,
            peak: 0,
        })
    }

    fn push_part(&mut self, sequence: u64, bytes: &[u8]) -> Result<()> {
        if sequence < self.next_sequence {
            return Ok(());
        }
        self.pending_parts
            .entry(sequence)
            .or_insert_with(|| bytes.to_vec());
        while let Some(bytes) = self.pending_parts.remove(&self.next_sequence) {
            self.decode_part(&bytes)?;
            self.next_sequence = self.next_sequence.saturating_add(1);
        }
        Ok(())
    }

    fn rebase_before_media(&mut self, first_sequence: u64) -> Result<()> {
        if self.soundkit_v2_packets > 0 || !self.pending_parts.is_empty() {
            bail!("cannot change the SoundKit object baseline after media");
        }
        self.next_sequence = first_sequence;
        Ok(())
    }

    fn decode_part(&mut self, bytes: &[u8]) -> Result<()> {
        let frames = self
            .parser
            .push(bytes)
            .map_err(anyhow::Error::msg)
            .context("consumer rejected SoundKit v2 bytes")?;
        if frames.is_empty() {
            bail!("opaque LL-HLS part contained no complete SoundKit v2 packet");
        }
        for frame in frames {
            self.soundkit_v2_packets = self.soundkit_v2_packets.saturating_add(1);
            if frame.header.encoding() != &EncodingFlag::Opus {
                bail!(
                    "consumer expected SoundKit v2 Opus but received {:?}",
                    frame.header.encoding()
                );
            }
            match self.protection {
                SoundkitOpusProtection::Encrypted => {
                    if !frame.encrypted {
                        bail!("private SoundKit v2 Opus packet was not encrypted");
                    }
                    self.encrypted_packets = self.encrypted_packets.saturating_add(1);
                }
                SoundkitOpusProtection::Plain => {
                    if frame.encrypted {
                        bail!("plain SoundKit v2 Opus consumer received an encrypted packet");
                    }
                    // The parser verified the CRC32 of every packet that
                    // carries one; plain packets must carry one.
                    if frame.header.packet_crc32_value().is_none() {
                        bail!("plain SoundKit v2 Opus packet carried no packet CRC32");
                    }
                    self.plain_packets = self.plain_packets.saturating_add(1);
                }
            }
            let sample_rate = frame.header.sample_rate();
            let channels = frame.header.channels();
            let frame_count = frame.header.frame_count();
            if sample_rate == 0 || channels == 0 || frame_count == 0 {
                bail!("SoundKit v2 Opus packet declared invalid audio dimensions");
            }
            if let Some(expected) = self.expected_frame_samples {
                if frame_count != expected {
                    bail!(
                        "SoundKit v2 Opus packet of {frame_count} samples ({} Opus); \
                         --expected-opus-frame-samples is {expected}",
                        opus_mode(sample_rate, frame_count)
                    );
                }
            }
            self.frame_samples.insert(frame_count);
            if self.sample_rate == 0 {
                self.sample_rate = sample_rate;
                self.channels = channels;
            } else if self.sample_rate != sample_rate || self.channels != channels {
                bail!(
                    "SoundKit v2 Opus format changed from {} Hz/{} ch to {} Hz/{} ch",
                    self.sample_rate,
                    self.channels,
                    sample_rate,
                    channels
                );
            }
            // The header's frame count chooses Opus Custom (48, 64, 72
            // samples, signalling byte on) or standard Opus; a change of
            // frame size starts a new decoder.
            if !self.decoder.as_ref().is_some_and(|decoder| {
                lane_opus_decoder_matches(decoder, sample_rate, usize::from(channels), frame_count)
            }) {
                self.decoder = Some(
                    new_lane_opus_decoder(sample_rate, usize::from(channels), frame_count)
                        .map_err(anyhow::Error::msg)?,
                );
            }
            let sample_capacity = usize::try_from(frame_count)
                .ok()
                .and_then(|frames| frames.checked_mul(usize::from(channels)))
                .context("SoundKit v2 Opus frame size exceeds this platform")?;
            let mut pcm = vec![0_i16; sample_capacity];
            let decoded_frames = self
                .decoder
                .as_mut()
                .expect("decoder initialized with first packet")
                .decode_i16(&frame.payload, &mut pcm, false)
                .map_err(anyhow::Error::msg)
                .context("pure-Rust consumer failed to decode Opus")?;
            if decoded_frames != frame_count as usize {
                bail!(
                    "consumer decoded {decoded_frames} frames but SoundKit declared {frame_count}"
                );
            }
            pcm.truncate(decoded_frames.saturating_mul(usize::from(channels)));
            for sample in &pcm {
                let normalized = f64::from(*sample) / f64::from(i16::MAX);
                self.sum_squares += normalized * normalized;
                self.sample_count = self.sample_count.saturating_add(1);
                self.peak = self
                    .peak
                    .max(i32::from(*sample).unsigned_abs().min(u32::from(u16::MAX)) as u16);
            }
            if self.capture_pcm {
                self.decoded_pcm.extend_from_slice(&pcm);
            }
            self.decoded_pcm_frames = self
                .decoded_pcm_frames
                .saturating_add(decoded_frames as u64);
            self.decoded_opus_packets = self.decoded_opus_packets.saturating_add(1);
        }
        Ok(())
    }

    fn finish(self) -> Result<OpusConsumerReport> {
        self.parser
            .finish()
            .map_err(anyhow::Error::msg)
            .context("consumer ended with a partial SoundKit v2 packet")?;
        if let Some(path) = &self.decoded_pcm_output {
            write_s16le_pcm(path, &self.decoded_pcm)?;
        }
        let rms = if self.sample_count == 0 {
            0.0
        } else {
            (self.sum_squares / self.sample_count as f64).sqrt()
        };
        let decoded_rms_dbfs = if rms > 0.0 {
            20.0 * rms.log10()
        } else {
            f64::NEG_INFINITY
        };
        let comparison = self
            .reference_pcm_bytes
            .as_deref()
            .map(|bytes| {
                compare_reference_pcm(
                    &self.decoded_pcm,
                    bytes,
                    self.channels,
                    self.reference_leading_silence_frames,
                    self.reference_start_offset_frames,
                    self.reference_window_frames,
                )
            })
            .transpose()?;
        let protected_packets = match self.protection {
            SoundkitOpusProtection::Encrypted => self.encrypted_packets,
            SoundkitOpusProtection::Plain => self.plain_packets,
        };
        let waveform_verified = self.soundkit_v2_packets > 0
            && self.soundkit_v2_packets == protected_packets
            && self.soundkit_v2_packets == self.decoded_opus_packets
            && self.decoded_pcm_frames > 0
            && self.pending_parts.is_empty()
            && decoded_rms_dbfs.is_finite()
            && decoded_rms_dbfs > -60.0
            && comparison
                .as_ref()
                .is_none_or(|comparison| comparison.normalized_correlation >= 0.70);
        let opus_frame_samples = self.frame_samples.iter().copied().collect::<Vec<_>>();
        let opus_mode = opus_modes(self.sample_rate, &opus_frame_samples);
        Ok(OpusConsumerReport {
            protection: self.protection.as_str(),
            opus_frame_samples,
            opus_mode,
            soundkit_v2_packets: self.soundkit_v2_packets,
            encrypted_packets: self.encrypted_packets,
            plain_packets: self.plain_packets,
            decoded_opus_packets: self.decoded_opus_packets,
            sample_rate: self.sample_rate,
            channels: self.channels,
            decoded_pcm_frames: self.decoded_pcm_frames,
            decoded_rms_dbfs,
            decoded_peak: self.peak,
            pending_out_of_order_parts: self.pending_parts.len(),
            reference_frames: comparison.as_ref().map(|value| value.reference_frames),
            aligned_reference_frames: comparison.as_ref().map(|value| value.aligned_frames),
            alignment_frames: comparison.as_ref().map(|value| value.alignment_frames),
            normalized_correlation: comparison
                .as_ref()
                .map(|value| value.normalized_correlation),
            waveform_verified,
        })
    }
}

/// `custom`, `standard` or `mixed` for the Opus frame sizes seen, or none.
fn opus_modes(sample_rate: u32, frame_samples: &[u32]) -> Option<&'static str> {
    let mut modes = frame_samples
        .iter()
        .map(|frames| opus_mode(sample_rate, *frames))
        .collect::<Vec<_>>();
    modes.dedup();
    match modes.as_slice() {
        [] => None,
        [mode] => Some(*mode),
        _ => Some("mixed"),
    }
}

/// The Opus frame sizes of the SoundKit Opus parts a reader received.
struct OpusFrameObservation {
    expected: Option<u32>,
    observed: std::collections::BTreeSet<u32>,
    mismatched_parts: u64,
}

impl OpusFrameObservation {
    fn new(expected: Option<u32>) -> Self {
        Self {
            expected,
            observed: std::collections::BTreeSet::new(),
            mismatched_parts: 0,
        }
    }

    /// Records the frame counts of a SoundKit Opus part's packets; false
    /// when one differs from the expected frame. Other codecs pass.
    fn observe(&mut self, codec: HlsAudioCodec, part: &[u8]) -> bool {
        if codec != HlsAudioCodec::SoundkitOpus {
            return true;
        }
        let mut matches = true;
        for frame_count in soundkit_v2_frame_counts(part) {
            self.observed.insert(frame_count);
            if self
                .expected
                .is_some_and(|expected| expected != frame_count)
            {
                matches = false;
            }
        }
        if !matches {
            self.mismatched_parts = self.mismatched_parts.saturating_add(1);
        }
        matches
    }

    fn report(&self) -> OpusFrameReport {
        let opus_frame_samples = self.observed.iter().copied().collect::<Vec<_>>();
        OpusFrameReport {
            opus_mode: opus_modes(SAMPLE_RATE, &opus_frame_samples),
            opus_frame_samples,
            expected_opus_frame_samples: self.expected,
            opus_frame_samples_mismatched_parts: self.mismatched_parts,
        }
    }
}

#[derive(Debug, Default, Serialize)]
struct OpusFrameReport {
    /// Frame counts of the SoundKit Opus packets received (48 for 1 ms
    /// Opus Custom, 120 for 2.5 ms standard Opus).
    opus_frame_samples: Vec<u32>,
    /// `custom`, `standard`, `mixed` or none.
    opus_mode: Option<&'static str>,
    expected_opus_frame_samples: Option<u32>,
    opus_frame_samples_mismatched_parts: u64,
}

/// The frame counts of the SoundKit v2 packets in `bytes`, up to the first
/// one that does not parse.
fn soundkit_v2_frame_counts(mut bytes: &[u8]) -> Vec<u32> {
    let mut counts = Vec::new();
    while !bytes.is_empty() {
        let Ok(header_size) = FrameHeaderV2::header_size(bytes) else {
            break;
        };
        let Some(header_bytes) = bytes.get(..header_size) else {
            break;
        };
        let Ok(header) = FrameHeaderV2::decode(&mut &header_bytes[..]) else {
            break;
        };
        let Some(packet_size) = usize::try_from(header.payload_size())
            .ok()
            .and_then(|payload| header_size.checked_add(payload))
        else {
            break;
        };
        if packet_size > bytes.len() {
            break;
        }
        counts.push(header.frame_count());
        bytes = &bytes[packet_size..];
    }
    counts
}

struct PcmComparison {
    reference_frames: u64,
    aligned_frames: u64,
    alignment_frames: i64,
    normalized_correlation: f64,
}

fn write_s16le_pcm(path: &Path, samples: &[i16]) -> Result<()> {
    let file = std::fs::File::create(path)
        .with_context(|| format!("create decoded PCM output {}", path.display()))?;
    let mut writer = std::io::BufWriter::new(file);
    for sample in samples {
        writer.write_all(&sample.to_le_bytes())?;
    }
    writer.flush()?;
    Ok(())
}

fn write_raw_hls_part(
    output_dir: Option<&Path>,
    sequence: u64,
    pts_ms: u64,
    bytes: &[u8],
) -> Result<()> {
    let Some(output_dir) = output_dir else {
        return Ok(());
    };
    let path = output_dir.join(format!("part-{sequence:012}-pts-{pts_ms:012}.bin"));
    std::fs::write(&path, bytes)
        .with_context(|| format!("write raw LL-HLS part {}", path.display()))
}

fn compare_reference_pcm(
    decoded: &[i16],
    reference_bytes: &[u8],
    channels: u8,
    leading_silence_frames: usize,
    start_offset_frames: usize,
    window_frames: Option<usize>,
) -> Result<PcmComparison> {
    let channels = usize::from(channels);
    if channels == 0 || !reference_bytes.len().is_multiple_of(2 * channels) {
        bail!("PCM reference is not complete interleaved S16LE audio");
    }
    let mut reference = Vec::with_capacity(
        leading_silence_frames
            .saturating_mul(channels)
            .saturating_add(reference_bytes.len() / 2),
    );
    reference.resize(leading_silence_frames.saturating_mul(channels), 0_i16);
    reference.extend(
        reference_bytes
            .chunks_exact(2)
            .map(|bytes| i16::from_le_bytes([bytes[0], bytes[1]])),
    );
    let total_reference_frames = reference.len() / channels;
    if start_offset_frames >= total_reference_frames {
        bail!(
            "PCM reference starts after the requested verification window: start={} frames, reference={} frames",
            start_offset_frames,
            total_reference_frames
        );
    }
    let end_offset_frames = window_frames
        .and_then(|frames| start_offset_frames.checked_add(frames))
        .unwrap_or(total_reference_frames)
        .min(total_reference_frames);
    reference = reference[start_offset_frames * channels..end_offset_frames * channels].to_vec();
    let decoded_frames = decoded.len() / channels;
    let reference_frames = reference.len() / channels;
    let common_frames = decoded_frames.min(reference_frames);
    if common_frames < SAMPLE_RATE as usize / 2 {
        bail!("consumer/reference comparison requires at least 500 ms of PCM");
    }
    let window_frames = common_frames.min(SAMPLE_RATE as usize * 2);
    let reference_start = highest_energy_window(&reference, channels, window_frames);
    let coarse = best_correlation_in_lag_range(
        decoded,
        &reference,
        channels,
        reference_start,
        window_frames,
        -4_096,
        4_096,
        8,
        8,
    )
    .context("PCM reference did not contain enough signal for alignment")?;
    let refined = best_correlation_in_lag_range(
        decoded,
        &reference,
        channels,
        reference_start,
        window_frames,
        coarse.0.saturating_sub(16),
        coarse.0.saturating_add(16),
        1,
        2,
    )
    .unwrap_or(coarse);
    Ok(PcmComparison {
        reference_frames: reference_frames as u64,
        aligned_frames: refined.2 as u64,
        alignment_frames: refined.0,
        normalized_correlation: refined.1,
    })
}

fn millis_to_frames(ms: u64) -> usize {
    ((u128::from(ms) * u128::from(SAMPLE_RATE)) / 1_000).min(usize::MAX as u128) as usize
}

fn highest_energy_window(reference: &[i16], channels: usize, window_frames: usize) -> usize {
    let frames = reference.len() / channels;
    if frames <= window_frames {
        return 0;
    }
    let hop = (SAMPLE_RATE as usize / 4).max(1);
    let mut best_start = 0;
    let mut best_energy = -1.0_f64;
    for start in (0..=frames - window_frames).step_by(hop) {
        let energy = (start..start + window_frames)
            .step_by(8)
            .map(|frame| {
                let sample = f64::from(reference[frame * channels]);
                sample * sample
            })
            .sum::<f64>();
        if energy > best_energy {
            best_energy = energy;
            best_start = start;
        }
    }
    best_start
}

#[allow(clippy::too_many_arguments)]
fn best_correlation_in_lag_range(
    decoded: &[i16],
    reference: &[i16],
    channels: usize,
    reference_start: usize,
    window_frames: usize,
    min_lag: i64,
    max_lag: i64,
    lag_step: usize,
    sample_step: usize,
) -> Option<(i64, f64, usize)> {
    let decoded_frames = decoded.len() / channels;
    let reference_frames = reference.len() / channels;
    let mut best: Option<(i64, f64, usize)> = None;
    for lag in (min_lag..=max_lag).step_by(lag_step.max(1)) {
        let decoded_start = if lag >= 0 {
            let Some(start) = reference_start.checked_add(lag as usize) else {
                continue;
            };
            start
        } else {
            let Some(start) = reference_start.checked_sub(lag.unsigned_abs() as usize) else {
                continue;
            };
            start
        };
        let available = window_frames
            .min(reference_frames.saturating_sub(reference_start))
            .min(decoded_frames.saturating_sub(decoded_start));
        if available < SAMPLE_RATE as usize / 4 {
            continue;
        }
        let mut count = 0_f64;
        let mut sum_x = 0_f64;
        let mut sum_y = 0_f64;
        let mut sum_x2 = 0_f64;
        let mut sum_y2 = 0_f64;
        let mut sum_xy = 0_f64;
        for offset in (0..available).step_by(sample_step.max(1)) {
            let x = f64::from(reference[(reference_start + offset) * channels]);
            let y = f64::from(decoded[(decoded_start + offset) * channels]);
            count += 1.0;
            sum_x += x;
            sum_y += y;
            sum_x2 += x * x;
            sum_y2 += y * y;
            sum_xy += x * y;
        }
        let covariance = count * sum_xy - sum_x * sum_y;
        let variance_x = count * sum_x2 - sum_x * sum_x;
        let variance_y = count * sum_y2 - sum_y * sum_y;
        let denominator = (variance_x * variance_y).sqrt();
        if denominator <= f64::EPSILON {
            continue;
        }
        let correlation = covariance / denominator;
        if best.as_ref().is_none_or(|current| correlation > current.1) {
            best = Some((lag, correlation, available));
        }
    }
    best
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
            epoch_samples,
            opus_bitrate,
            channels,
            group_channels,
            signal,
            pcm_s24le_file,
            group_id,
            repair_percent,
            min_repair_symbols,
            pace,
        } => {
            let report = send(SendOptions {
                target,
                duration_seconds,
                session_id,
                payload_kind: payload,
                epoch_samples,
                opus_bitrate,
                channels,
                group_channels,
                signal,
                pcm_s24le_file: pcm_s24le_file.as_deref(),
                group_id,
                repair_percent,
                min_repair_symbols,
                pace,
            })
            .await?;
            println!("{}", serde_json::to_string_pretty(&report)?);
        }
        Command::CaptureFixture {
            relay,
            bind,
            session_id,
            group_id,
            duration_seconds,
            tail_seconds,
            producer_sha256,
            output,
        } => {
            let report = capture_fixture(CaptureFixtureOptions {
                relay,
                bind,
                session_id,
                group_id,
                duration_seconds,
                tail_seconds,
                producer_sha256,
                output,
            })
            .await?;
            println!("{}", serde_json::to_string_pretty(&report)?);
        }
        Command::ReplayFixture {
            fixture,
            bind,
            target,
            secondary_bind,
            secondary_target,
            local_node_id,
            primary_node_id,
            secondary_node_id,
            topology_generation,
            subscription_id,
            duration_seconds,
            tracks,
            group_id,
            session_id,
            repair_percent,
            min_repair_symbols,
        } => {
            let report = replay_fixture(ReplayFixtureOptions {
                fixture,
                bind,
                target,
                secondary_bind,
                secondary_target,
                local_node_id,
                primary_node_id,
                secondary_node_id,
                topology_generation,
                subscription_id,
                duration_seconds,
                tracks,
                group_id,
                session_id,
                repair_percent,
                min_repair_symbols,
            })
            .await?;
            println!("{}", serde_json::to_string_pretty(&report)?);
        }
        Command::DecodeSoundkitOpus {
            parts,
            soundkit_opus_protection,
            soundkit_key_file,
            expected_opus_frame_samples,
            decoded_pcm_output,
        } => {
            let options = OpusConsumerOptions {
                first_sequence: 0,
                decoded_pcm_output: decoded_pcm_output.as_deref(),
                reference_pcm_s16le: None,
                reference_leading_silence_frames: 0,
                reference_start_offset_ms: 0,
                reference_window_ms: 0,
                expected_frame_samples: expected_opus_frame_samples,
            };
            let mut consumer = match (soundkit_opus_protection, soundkit_key_file.as_deref()) {
                (SoundkitOpusProtection::Plain, None) => SoundKitOpusConsumer::plain(options)?,
                (SoundkitOpusProtection::Encrypted, Some(key_file)) => {
                    SoundKitOpusConsumer::from_key_file(key_file, options)?
                }
                (SoundkitOpusProtection::Plain, Some(_)) => {
                    bail!("--soundkit-key-file decodes encrypted packets; drop it for plain protection")
                }
                (SoundkitOpusProtection::Encrypted, None) => {
                    bail!("encrypted SoundKit Opus needs --soundkit-key-file")
                }
            };
            for (sequence, path) in parts.iter().enumerate() {
                let bytes = std::fs::read(path)
                    .with_context(|| format!("read SoundKit Opus part {}", path.display()))?;
                consumer
                    .push_part(sequence as u64, &bytes)
                    .with_context(|| format!("decode SoundKit Opus part {}", path.display()))?;
            }
            let report = consumer.finish()?;
            println!("{}", serde_json::to_string_pretty(&report)?);
        }
        Command::ReceiveUdp {
            relay,
            bind,
            session_id,
            group_id,
            group_count,
            formats,
            duration_seconds,
            deadline_ms,
            tail_seconds,
            decode_soundkit_opus,
            expected_opus_frame_samples,
            decoded_pcm_output,
        } => {
            let report = timeout(
                receive_command_timeout(session_id, duration_seconds, tail_seconds)?,
                receive_udp(UdpReceiveOptions {
                    relay,
                    bind,
                    session_id,
                    group_id,
                    group_count,
                    formats: &formats,
                    duration_seconds,
                    deadline_ms,
                    tail_seconds,
                    decode_soundkit_opus,
                    expected_opus_frame_samples,
                    decoded_pcm_output: decoded_pcm_output.as_deref(),
                }),
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
            formats,
            duration_seconds,
            deadline_ms,
            tail_seconds,
            epoch_samples,
        } => {
            let report = timeout(
                receive_command_timeout(session_id, duration_seconds, tail_seconds)?,
                receive_webtransport(WebTransportReceiveOptions {
                    edge,
                    server_name: &server_name,
                    tls_ca: tls_ca.as_deref(),
                    session_id,
                    group_id,
                    formats: &formats,
                    duration_seconds,
                    deadline_ms,
                    tail_seconds,
                    epoch_samples,
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
            response_ms,
            deadline_ms,
            render_buffer_ms,
            start_offset_ms,
            window_seconds,
            tail_seconds,
            expected_audio_codec,
            expected_pcm_channels,
            soundkit_opus_protection,
            object_sequence_origin,
            abandon_after_ms,
            soundkit_key_file,
            decode_soundkit_opus,
            expected_opus_frame_samples,
            decoded_pcm_output,
            raw_part_output_dir,
            reference_pcm_s16le,
            reference_leading_silence_frames,
        } => {
            let end_offset_ms =
                hls_window_end_ms(duration_seconds, start_offset_ms, window_seconds, part_ms)?;
            let report = timeout(
                receive_command_timeout_ms(session_id, end_offset_ms, tail_seconds)?,
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
                    response_ms: response_ms.unwrap_or(part_ms),
                    deadline_ms,
                    render_buffer_ms,
                    start_offset_ms,
                    window_seconds,
                    tail_seconds,
                    expected_audio_codec,
                    expected_pcm_channels,
                    soundkit_opus_protection,
                    object_sequence_origin,
                    abandon_after_ms,
                    soundkit_key_file: soundkit_key_file.as_deref(),
                    decode_soundkit_opus,
                    expected_opus_frame_samples,
                    decoded_pcm_output: decoded_pcm_output.as_deref(),
                    raw_part_output_dir: raw_part_output_dir.as_deref(),
                    reference_pcm_s16le: reference_pcm_s16le.as_deref(),
                    reference_leading_silence_frames,
                    shared_h3: None,
                }),
            )
            .await
            .context("LL-HLS probe exceeded its overall deadline")??;
            println!("{}", serde_json::to_string_pretty(&report)?);
            if !report.init_audio_codec_verified
                || report.pcm_media_size_mismatches > 0
                || report.opaque_flac_frame_mismatches > 0
                || report.opus_media_packet_mismatches > 0
                || report.fmp4_video_part_mismatches > 0
                || report
                    .opus_consumer
                    .as_ref()
                    .is_some_and(|consumer| !consumer.waveform_verified)
            {
                bail!(
                    "LL-HLS codec verification failed: expected {}, received {:?}, {} PCM size mismatches, {} invalid opaque FLAC parts, {} invalid Opus packets",
                    report.expected_audio_codec,
                    report.init_audio_codec,
                    report.pcm_media_size_mismatches,
                    report.opaque_flac_frame_mismatches,
                    report.opus_media_packet_mismatches
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
            stream_count,
            bundle_streams,
            session_id,
            duration_seconds,
            part_ms,
            response_ms,
            deadline_ms,
            start_offset_ms,
            window_seconds,
            tail_seconds,
            readers,
            arrival_window_ms,
            arrival_seed,
            expected_audio_codec,
            expected_pcm_channels,
            soundkit_opus_protection,
            object_sequence_origin,
            abandon_after_ms,
        } => {
            let report = load_hls(HlsLoadOptions {
                edge,
                server_name,
                tls_ca,
                transport,
                path_prefix,
                stream_id,
                stream_count,
                bundle_streams,
                session_id,
                duration_seconds,
                part_ms,
                response_ms: response_ms.unwrap_or(part_ms),
                deadline_ms,
                start_offset_ms,
                window_seconds,
                tail_seconds,
                readers,
                arrival_window_ms,
                arrival_seed,
                expected_audio_codec,
                expected_pcm_channels,
                soundkit_opus_protection,
                object_sequence_origin,
                abandon_after_ms,
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

/// The interval in which a receiver listens and counts epochs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ReceiveWindow {
    /// First capture time that is counted.
    start_ns: u64,
    /// Capture times at or after this are not counted.
    media_end_ns: u64,
    /// The receiver stops listening here.
    stop_ns: u64,
}

impl ReceiveWindow {
    /// A listener counts `duration_seconds` of the session from the moment
    /// it subscribes, or from the start of the session when it subscribes
    /// before that. `--session-id` names the session and its clock; it is
    /// not the start of the measurement.
    fn new(session_id: u64, now_ns: u64, duration_seconds: u64, tail_seconds: u64) -> Self {
        let start_ns = session_id.max(now_ns);
        let media_end_ns = start_ns.saturating_add(duration_seconds.saturating_mul(1_000_000_000));
        let stop_ns = media_end_ns.saturating_add(tail_seconds.saturating_mul(1_000_000_000));
        Self {
            start_ns,
            media_end_ns,
            stop_ns,
        }
    }

    fn counts(&self, capture_ns: u64) -> bool {
        capture_ns >= self.start_ns && capture_ns < self.media_end_ns
    }

    /// Whole seconds of capture time since the start of the window.
    fn second_of(&self, capture_ns: u64) -> u64 {
        capture_ns.saturating_sub(self.start_ns) / 1_000_000_000
    }
}

fn receive_command_timeout(
    session_id: u64,
    duration_seconds: u64,
    tail_seconds: u64,
) -> Result<Duration> {
    // A UDP or WebTransport listener counts from when it joins (see
    // ReceiveWindow), so its deadline is too.
    receive_command_timeout_ms(
        session_id.max(now_unix_ns()?),
        duration_seconds.saturating_mul(1_000),
        tail_seconds,
    )
}

fn receive_command_timeout_ms(
    session_id: u64,
    end_offset_ms: u64,
    tail_seconds: u64,
) -> Result<Duration> {
    let stop_ns = session_id
        .saturating_add(end_offset_ms.saturating_mul(1_000_000))
        .saturating_add(tail_seconds.saturating_mul(1_000_000_000));
    let remaining_ns = stop_ns.saturating_sub(now_unix_ns()?);
    // Leave a bounded allowance for subscription setup, clock skew, and orderly QUIC close.
    Ok(Duration::from_nanos(remaining_ns).saturating_add(Duration::from_secs(5)))
}

fn hls_window_end_ms(
    duration_seconds: u64,
    start_offset_ms: u64,
    window_seconds: Option<u64>,
    part_ms: u64,
) -> Result<u64> {
    if part_ms == 0 {
        bail!("--part-ms must be positive");
    }
    let media_duration_ms = duration_seconds
        .checked_mul(1_000)
        .context("media duration exceeds the supported range")?;
    if start_offset_ms >= media_duration_ms {
        bail!("--start-offset-ms must be smaller than the media duration");
    }
    if !start_offset_ms.is_multiple_of(part_ms) {
        bail!("--start-offset-ms must align to the configured part duration");
    }
    let end_offset_ms = match window_seconds {
        None => media_duration_ms,
        Some(0) => bail!("--window-seconds must be positive"),
        Some(window_seconds) => start_offset_ms
            .checked_add(
                window_seconds
                    .checked_mul(1_000)
                    .context("reader window exceeds the supported range")?,
            )
            .context("reader window exceeds the supported range")?,
    };
    if end_offset_ms > media_duration_ms {
        bail!("reader window must end within the media duration");
    }
    if !end_offset_ms.is_multiple_of(part_ms) {
        bail!("reader window end must align to the configured part duration");
    }
    Ok(end_offset_ms)
}

fn h3_preload_at_ns(session_id: u64, start_offset_ms: u64) -> u64 {
    let start_ns = session_id.saturating_add(start_offset_ms.saturating_mul(1_000_000));
    let preload_lead_ns = H3_PART_PRELOAD_LEAD.as_nanos().min(u128::from(u64::MAX)) as u64;
    start_ns.saturating_sub(preload_lead_ns)
}

struct SendOptions<'a> {
    target: SocketAddr,
    duration_seconds: u64,
    session_id: Option<u64>,
    payload_kind: ProbePayload,
    epoch_samples: Option<u32>,
    opus_bitrate: u32,
    channels: u16,
    group_channels: u16,
    signal: ProbeSignal,
    pcm_s24le_file: Option<&'a Path>,
    group_id: u16,
    repair_percent: u32,
    min_repair_symbols: u32,
    pace: SendPace,
}

async fn send(options: SendOptions<'_>) -> Result<SendReport> {
    let SendOptions {
        target,
        duration_seconds,
        session_id,
        payload_kind,
        epoch_samples,
        opus_bitrate,
        channels,
        group_channels,
        signal,
        pcm_s24le_file,
        group_id,
        repair_percent,
        min_repair_symbols,
        pace,
    } = options;
    if duration_seconds == 0 {
        bail!("--duration-seconds must be positive");
    }
    let epoch_samples = payload_kind.epoch_samples(epoch_samples)?;
    if let Some(note) = receiver_epoch_note(epoch_samples) {
        eprintln!("{note}");
    }
    let epoch = EpochGeometry::new(epoch_samples);
    if pace == SendPace::Tokio && epoch.duration_ns() < 1_000_000 {
        bail!("--pace tokio needs epochs of at least 1 ms: tokio timers tick in milliseconds");
    }
    let session_id = session_id.unwrap_or(now_unix_ns()?.saturating_add(1_000_000_000));
    let now_ns = now_unix_ns()?;
    if session_id.saturating_add(60_000_000_000) < now_ns {
        bail!("--session-id is too far in the past to be an origin clock");
    }
    let source = EpochSourceOptions {
        session_id,
        payload_kind,
        epoch_samples,
        opus_bitrate,
        channels,
        group_channels,
        signal,
        pcm_s24le_file: pcm_s24le_file.map(Path::to_path_buf),
        group_id,
        repair_percent,
        min_repair_symbols,
    };
    let epochs = epoch.epochs_in(duration_seconds);
    let started = Instant::now();
    // A harness that stops the sender with SIGTERM (or SIGINT) still gets
    // the report of the epochs that were sent.
    let stop = stop_on_signal();
    let (encoder, stats) = match pace {
        SendPace::Tokio => send_tokio_paced(source, target, epochs, &stop).await?,
        SendPace::Precise => {
            // The pacer owns an OS thread: a tokio timer fires on 1 ms ticks
            // and shares its worker with the rest of the runtime.
            let (done_tx, done_rx) = tokio::sync::oneshot::channel();
            let pacer_stop = Arc::clone(&stop);
            std::thread::Builder::new()
                .name("aep1-pacer".into())
                .spawn(move || {
                    let _ = done_tx.send(send_precise_paced(source, target, epochs, &pacer_stop));
                })
                .context("failed to start the AEP1 pacing thread")?;
            done_rx
                .await
                .context("the AEP1 pacing thread stopped without a result")??
        }
    };
    let stopped_by_signal = stop.load(AtomicOrdering::Acquire);
    let epochs = stats.epochs;

    let pcm_reference_bytes = epochs
        .saturating_mul(u64::from(epoch_samples))
        .saturating_mul(u64::from(channels))
        .saturating_mul(3);
    Ok(SendReport {
        schema: "needletail.aep1-48k-probe.send.v3",
        lane: "source",
        payload: payload_kind.as_str(),
        signal: encoder.pcm_source.label(),
        input_pcm_frames: encoder.pcm_source.input_frames(),
        session_id,
        group_id,
        group_count: encoder.group_plan.len() as u16,
        group_channels: effective_group_channels(payload_kind, group_channels),
        group_ids: encoder
            .group_plan
            .iter()
            .map(|group| group.group_id)
            .collect(),
        sample_rate: SAMPLE_RATE,
        channels,
        frame_count: epoch_samples,
        epoch_samples,
        epoch_duration_us: epoch.duration_ns() as f64 / 1_000.0,
        duration_seconds,
        epochs,
        source_datagrams: encoder.source_datagrams,
        repair_datagrams: encoder.repair_datagrams,
        wire_bytes: encoder.wire_bytes,
        lossless_payload_bytes: encoder.lossless_payload_bytes,
        pcm_reference_bytes,
        wire_overhead_ratio: encoder.wire_bytes as f64
            / encoder.lossless_payload_bytes.max(1) as f64,
        wire_to_pcm_ratio: encoder.wire_bytes as f64 / pcm_reference_bytes.max(1) as f64,
        elapsed_ms: started.elapsed().as_millis().min(u128::from(u64::MAX)) as u64,
        stopped_by_signal,
        pace: pace.as_str(),
        spin_budget_us: epoch.spin_budget_ns() as f64 / 1_000.0,
        send_lateness_ms: stats.first_datagram.summary(),
        send_complete_lateness_ms: stats.last_datagram.summary(),
        encode_ms: stats.encode.summary(),
    })
}

/// How the sender waits for the instant each epoch leaves.
#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
enum SendPace {
    /// A dedicated OS thread sleeps to 300 µs before each absolute deadline
    /// and spins the rest. A stall delays only the epochs it covers.
    Precise,
    /// A tokio interval in MissedTickBehavior::Delay: 1 ms timer ticks, and a
    /// stall over 5 ms shifts every later epoch.
    Tokio,
}

impl SendPace {
    fn as_str(self) -> &'static str {
        match self {
            Self::Precise => "precise",
            Self::Tokio => "tokio",
        }
    }
}

/// The precise pacer sleeps until this long before a deadline and spins the
/// rest, so that a late wake-up from the kernel does not make the epoch late.
/// An epoch of under 1.2 ms spins a quarter of itself instead: a 300 µs spin
/// is longer than a 250 µs epoch and would hold a core.
const PRECISE_PACE_SPIN_NS: u64 = 300_000;

/// The size of the epochs a sender makes, at the probe's 48 kHz.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct EpochGeometry {
    samples: u32,
}

impl EpochGeometry {
    fn new(samples: u32) -> Self {
        Self {
            samples: samples.max(1),
        }
    }

    /// Nanoseconds from the session id to the start of epoch `epoch_id`'s
    /// deadline: `epoch_id * samples / 48 kHz`, exact for any sample count.
    fn offset_ns(self, epoch_id: u64) -> u64 {
        let ns = u128::from(epoch_id) * u128::from(self.samples) * 1_000_000_000
            / u128::from(SAMPLE_RATE);
        u64::try_from(ns).unwrap_or(u64::MAX)
    }

    fn duration_ns(self) -> u64 {
        self.offset_ns(1)
    }

    /// Whole epochs in `seconds`.
    fn epochs_in(self, seconds: u64) -> u64 {
        seconds.saturating_mul(u64::from(SAMPLE_RATE)) / u64::from(self.samples)
    }

    /// min(300 µs, epoch / 4).
    fn spin_budget_ns(self) -> u64 {
        PRECISE_PACE_SPIN_NS.min(self.duration_ns() / 4)
    }
}

/// The Unix time at which epoch `epoch_id` leaves the sender.
///
/// Epoch k carries samples [N k, N (k + 1)) and its capture time, as the
/// receivers count it, is `session_id + pts` = `session_id + k N / 48 kHz`.
/// The harness passes a session id one epoch after the start of the audio
/// loop, so this is the instant the last sample of the epoch has been
/// spoken. The sender has always sent epoch k then (epoch 0 at the session
/// id); only the epoch size and the precision change.
fn epoch_deadline_ns(session_id: u64, epoch_id: u64, epoch: EpochGeometry) -> u64 {
    session_id.saturating_add(epoch.offset_ns(epoch_id))
}

/// What the precise pacer does next, `now_ns` before a deadline.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PaceStep {
    /// Sleep this long, then look again.
    Sleep(Duration),
    /// Close enough to spin.
    Spin,
    /// The deadline has come (or passed): send now.
    Send,
}

fn pace_step(now_ns: u64, deadline_ns: u64, spin_ns: u64) -> PaceStep {
    if now_ns >= deadline_ns {
        PaceStep::Send
    } else if deadline_ns - now_ns > spin_ns {
        PaceStep::Sleep(Duration::from_nanos(deadline_ns - now_ns - spin_ns))
    } else {
        PaceStep::Spin
    }
}

/// The longest sleep of a pacer between looks at its stop flag.
const STOP_POLL: Duration = Duration::from_millis(50);

/// A flag that SIGTERM or SIGINT sets. The pacers stop at the next epoch
/// and the sender prints the report of what it sent.
fn stop_on_signal() -> Arc<AtomicBool> {
    let stop = Arc::new(AtomicBool::new(false));
    let flag = Arc::clone(&stop);
    tokio::spawn(async move {
        #[cfg(unix)]
        {
            use tokio::signal::unix::{signal, SignalKind};
            let Ok(mut terminate) = signal(SignalKind::terminate()) else {
                return;
            };
            tokio::select! {
                _ = tokio::signal::ctrl_c() => {}
                _ = terminate.recv() => {}
            }
        }
        #[cfg(not(unix))]
        if tokio::signal::ctrl_c().await.is_err() {
            return;
        }
        flag.store(true, AtomicOrdering::Release);
    });
    stop
}

/// Block the calling thread until the Unix time reaches `deadline_ns`, or
/// until `stop` is set (`false`). It sleeps until `spin_ns` before the
/// deadline and spins the rest.
fn wait_until_unix_ns(deadline_ns: u64, spin_ns: u64, stop: &AtomicBool) -> Result<bool> {
    loop {
        if stop.load(AtomicOrdering::Acquire) {
            return Ok(false);
        }
        match pace_step(now_unix_ns()?, deadline_ns, spin_ns) {
            PaceStep::Send => return Ok(true),
            PaceStep::Sleep(duration) => std::thread::sleep(duration.min(STOP_POLL)),
            PaceStep::Spin => std::hint::spin_loop(),
        }
    }
}

/// Durations of every epoch in a bounded uniform reservoir, with the exact
/// count and maximum: a long run of 4,000 epochs a second would otherwise
/// keep gigabytes of them.
struct EpochDurations {
    samples: BoundedLatencySamples,
    max_ns: u64,
}

impl Default for EpochDurations {
    fn default() -> Self {
        Self {
            samples: BoundedLatencySamples::new(),
            max_ns: 0,
        }
    }
}

impl std::fmt::Debug for EpochDurations {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("EpochDurations")
            .field("count", &self.samples.observations)
            .field("max_ns", &self.max_ns)
            .finish()
    }
}

impl EpochDurations {
    fn record(&mut self, value_ns: u64) {
        self.max_ns = self.max_ns.max(value_ns);
        self.samples.record(value_ns);
    }

    fn count(&self) -> usize {
        self.samples.observations
    }

    #[cfg(test)]
    fn values_ns(&self) -> &[u64] {
        &self.samples.values_ns
    }

    fn summary(self) -> Percentiles {
        let max_ns = self.max_ns;
        let mut summary = self.samples.summary();
        if summary.count > 0 {
            summary.max = max_ns as f64 / 1_000_000.0;
        }
        summary
    }
}

/// How late each epoch left, against its absolute deadline.
#[derive(Debug, Default)]
struct SendLateness {
    /// Epochs sent.
    epochs: u64,
    /// First datagram of the epoch handed to the kernel.
    first_datagram: EpochDurations,
    /// Last datagram of the epoch handed to the kernel.
    last_datagram: EpochDurations,
    /// Time to make the datagrams of the epoch (codec, framing, RaptorQ).
    encode: EpochDurations,
}

impl SendLateness {
    fn record(&mut self, deadline_ns: u64, first_sent_ns: u64, last_sent_ns: u64) {
        self.epochs = self.epochs.saturating_add(1);
        self.first_datagram
            .record(first_sent_ns.saturating_sub(deadline_ns));
        self.last_datagram
            .record(last_sent_ns.saturating_sub(deadline_ns));
    }
}

struct EpochSourceOptions {
    session_id: u64,
    payload_kind: ProbePayload,
    epoch_samples: u32,
    /// Opus bitrate (CBR), bits per second.
    opus_bitrate: u32,
    channels: u16,
    group_channels: u16,
    signal: ProbeSignal,
    pcm_s24le_file: Option<PathBuf>,
    group_id: u16,
    repair_percent: u32,
    min_repair_symbols: u32,
}

/// Makes the AEP1 datagrams of each epoch and counts what was sent.
struct EpochEncoder {
    session_id: u64,
    payload_kind: ProbePayload,
    epoch_samples: u32,
    group_plan: Vec<SourceGroupPlan>,
    pcm_source: PcmSource,
    transport: MultichannelAudioTransportAdapter,
    sender: MultichannelAudioSender,
    flac_encoders: Vec<FlacFrameEncoder>,
    /// One CELT encoder per group, as each DAW track has its own.
    opus_encoders: Vec<OpusEncoder>,
    opus_pcm: Vec<i16>,
    opus_packet: Vec<u8>,
    source_datagrams: u64,
    repair_datagrams: u64,
    wire_bytes: u64,
    lossless_payload_bytes: u64,
}

impl EpochEncoder {
    fn new(options: &EpochSourceOptions) -> Result<Self> {
        let epoch_samples = options
            .payload_kind
            .epoch_samples(Some(options.epoch_samples))?;
        let group_plan = source_group_plan(
            options.group_id,
            options.channels,
            effective_group_channels(options.payload_kind, options.group_channels),
        )?;
        let pcm_source = PcmSource::new(
            options.signal,
            options.pcm_s24le_file.as_deref(),
            options.channels,
        )?;
        let transport = MultichannelAudioTransportAdapter::udp(MAX_DATAGRAM_BYTES);
        let mut fec = transport.prepare_fec_config(MultichannelAudioFecConfig::default());
        let source_payload_budget = fec
            .max_fragment_payload()
            .context("invalid AEP1 geometry")?;
        let max_pcm_bytes_per_group = group_plan
            .iter()
            .map(|group| usize::from(group.channel_count) * epoch_samples as usize * 3)
            .max()
            .unwrap_or(0);
        let source_symbols = max_pcm_bytes_per_group
            .div_ceil(source_payload_budget)
            .max(1);
        let proportional = (source_symbols as u64)
            .saturating_mul(u64::from(options.repair_percent.min(100)))
            .div_ceil(100) as u32;
        fec.repair_symbols = proportional.max(options.min_repair_symbols.max(1));
        let sender = MultichannelAudioSender::new(MultichannelAudioSessionConfig {
            fec,
            ..MultichannelAudioSessionConfig::default()
        });
        let flac_encoders = if matches!(options.payload_kind, ProbePayload::Flac) {
            group_plan
                .iter()
                .map(|group| {
                    FlacFrameConfig::new(
                        SAMPLE_RATE,
                        group.channel_count,
                        24,
                        epoch_samples,
                        FlacProfile::Realtime,
                    )
                    .and_then(FlacFrameEncoder::new)
                })
                .collect::<Result<Vec<_>, _>>()?
        } else {
            Vec::new()
        };
        let opus_encoders = if options.payload_kind == ProbePayload::Opus {
            group_plan
                .iter()
                .map(|group| {
                    // CELT only: Opus Custom (signalling byte on, the
                    // lane's) for 48, 64 and 72 samples, standard Opus from
                    // 120.
                    new_lane_opus_encoder(
                        SAMPLE_RATE,
                        u32::from(group.channel_count),
                        epoch_samples,
                        options.opus_bitrate,
                    )
                    .map_err(|error| anyhow::anyhow!("Opus encoder: {error}"))
                })
                .collect::<Result<Vec<_>>>()?
        } else {
            Vec::new()
        };
        Ok(Self {
            session_id: options.session_id,
            payload_kind: options.payload_kind,
            epoch_samples,
            group_plan,
            pcm_source,
            transport,
            sender,
            flac_encoders,
            opus_encoders,
            opus_pcm: Vec::new(),
            opus_packet: vec![0; 4_000],
            source_datagrams: 0,
            repair_datagrams: 0,
            wire_bytes: 0,
            lossless_payload_bytes: 0,
        })
    }

    /// One CELT frame of a group's 24-bit PCM, coded from its top 16 bits
    /// as the DAW source codes its tracks.
    fn encode_opus(&mut self, index: usize, pcm_s24le: &[u8]) -> Result<Vec<u8>> {
        self.opus_pcm.clear();
        self.opus_pcm.extend(
            pcm_s24le
                .chunks_exact(3)
                .map(|sample| i16::from_le_bytes([sample[1], sample[2]])),
        );
        let len = self.opus_encoders[index]
            .encode_i16(&self.opus_pcm, &mut self.opus_packet)
            .map_err(|error| anyhow::anyhow!("Opus encode: {error}"))?;
        Ok(self.opus_packet[..len].to_vec())
    }

    /// The datagrams of one epoch, in send order.
    fn encode(&mut self, epoch_id: u64) -> Result<Vec<(Bytes, bool)>> {
        let frame_count = self.epoch_samples;
        let first_sample = epoch_id.saturating_mul(u64::from(frame_count));
        let mut payloads = Vec::with_capacity(self.group_plan.len());
        for index in 0..self.group_plan.len() {
            let group = self.group_plan[index];
            let pcm = self.pcm_source.group_s24le(
                first_sample,
                frame_count,
                group.channel_start,
                group.channel_count,
            );
            let payload = match self.payload_kind {
                ProbePayload::Flac => self.flac_encoders[index].encode_s24le(&pcm)?.payload,
                ProbePayload::Pcm => pcm,
                ProbePayload::Opus => self.encode_opus(index, &pcm)?,
            };
            self.lossless_payload_bytes = self
                .lossless_payload_bytes
                .saturating_add(payload.len() as u64);
            payloads.push(payload);
        }
        let payload_kind = self.payload_kind;
        let framed_payloads = self
            .group_plan
            .iter()
            .zip(payloads.iter())
            .map(|(group, payload)| {
                // Opus is framed as the DAW source frames it: no bit depth,
                // a packet id, no pts.
                let (encoding, bits_per_sample, id) = match payload_kind {
                    ProbePayload::Flac => (EncodingFlag::FLAC, 24, None),
                    ProbePayload::Pcm => (EncodingFlag::PCMSigned, 24, None),
                    ProbePayload::Opus => (EncodingFlag::Opus, 0, Some(epoch_id)),
                };
                let header = FrameHeaderV2::new(
                    encoding,
                    u32::try_from(payload.len()).expect("probe payload fits the v2 wire header"),
                    frame_count,
                    SAMPLE_RATE,
                    u8::try_from(group.channel_count)
                        .expect("probe groups are limited to eight channels"),
                    bits_per_sample,
                    frame_header::Endianness::LittleEndian,
                    id,
                    None,
                    None,
                )
                .expect("probe Soundkit v2 header is valid")
                .with_packet_crc32(payload)
                .expect("probe Soundkit v2 payload CRC is valid");
                let mut framed = Vec::with_capacity(header.size() + payload.len());
                header
                    .encode(&mut framed)
                    .expect("probe Soundkit v2 header encodes");
                framed.extend_from_slice(payload);
                framed
            })
            .collect::<Vec<_>>();
        let groups = self
            .group_plan
            .iter()
            .zip(framed_payloads.iter())
            .map(|(group, framed)| MultichannelAudioGroup {
                group_id: group.group_id,
                channel_start: group.channel_start,
                channel_count: group.channel_count,
                payload_kind: AudioPayloadKind::SoundKitV2,
                sample_format: AudioSampleFormat::Unspecified,
                flags: 0,
                payload: framed,
            })
            .collect::<Vec<_>>();
        let encoded = self.sender.encode_epoch(MultichannelAudioEpoch {
            session_id: self.session_id,
            config_generation: 1,
            epoch_id,
            pts_samples: first_sample,
            sample_rate: SAMPLE_RATE,
            frame_count,
            groups: &groups,
        })?;
        let wrapped = self.transport.wrap_epoch(encoded)?;
        Ok(wrapped
            .datagrams
            .into_iter()
            .map(|datagram| {
                let source = matches!(
                    datagram.role,
                    raptorq_datagram_fec::MultichannelAudioDatagramRole::Source { .. }
                );
                (datagram.payload, source)
            })
            .collect())
    }

    fn count_sent(&mut self, bytes: usize, source: bool) {
        self.wire_bytes = self.wire_bytes.saturating_add(bytes as u64);
        if source {
            self.source_datagrams = self.source_datagrams.saturating_add(1);
        } else {
            self.repair_datagrams = self.repair_datagrams.saturating_add(1);
        }
    }
}

/// Channels in each AEP1 group: at most eight (a FLAC rendition), and at
/// most two for Opus, one packet per group.
fn effective_group_channels(payload_kind: ProbePayload, group_channels: u16) -> u16 {
    let group_channels = group_channels.min(FLAC_MAX_CHANNELS);
    match payload_kind {
        ProbePayload::Opus => group_channels.min(OPUS_MAX_GROUP_CHANNELS),
        ProbePayload::Flac | ProbePayload::Pcm => group_channels,
    }
}

fn check_full_send(sent: usize, expected: usize) -> Result<()> {
    if sent != expected {
        bail!("partial AEP1 datagram send: {sent} of {expected}");
    }
    Ok(())
}

/// The sender as it was: a tokio interval from the session id. The source
/// (the PCM file is read whole) and the socket are made before the wait for
/// the session id, as they were, so that only the pacing differs from
/// `--pace precise`.
async fn send_tokio_paced(
    options: EpochSourceOptions,
    target: SocketAddr,
    epochs: u64,
    stop: &AtomicBool,
) -> Result<(EpochEncoder, SendLateness)> {
    let session_id = options.session_id;
    let epoch = EpochGeometry::new(options.epoch_samples);
    let mut encoder = EpochEncoder::new(&options)?;
    let socket = UdpSocket::bind(if target.is_ipv4() {
        "0.0.0.0:0"
    } else {
        "[::]:0"
    })
    .await?;
    let mut lateness = SendLateness::default();
    loop {
        let now_ns = now_unix_ns()?;
        if session_id <= now_ns || stop.load(AtomicOrdering::Acquire) {
            break;
        }
        let wait = Duration::from_nanos(session_id - now_ns).min(STOP_POLL);
        sleep_until(TokioInstant::now() + wait).await;
    }
    let mut ticker = interval_at(
        TokioInstant::now(),
        Duration::from_nanos(epoch.duration_ns()),
    );
    ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);
    for epoch_id in 0..epochs {
        ticker.tick().await;
        if stop.load(AtomicOrdering::Acquire) {
            break;
        }
        let encode_started = Instant::now();
        let datagrams = encoder.encode(epoch_id)?;
        lateness
            .encode
            .record(encode_started.elapsed().as_nanos() as u64);
        let mut first_sent_ns = None;
        let mut last_sent_ns = 0;
        for (payload, source) in datagrams {
            let sent = socket.send_to(&payload, target).await?;
            last_sent_ns = now_unix_ns()?;
            first_sent_ns.get_or_insert(last_sent_ns);
            check_full_send(sent, payload.len())?;
            encoder.count_sent(sent, source);
        }
        lateness.record(
            epoch_deadline_ns(session_id, epoch_id, epoch),
            first_sent_ns.unwrap_or(last_sent_ns),
            last_sent_ns,
        );
    }
    Ok((encoder, lateness))
}

/// The sender on absolute wall-clock deadlines: epoch k leaves at
/// `epoch_deadline_ns(session_id, k, epoch)` whatever happened to epoch
/// k - 1.
fn send_precise_paced(
    options: EpochSourceOptions,
    target: SocketAddr,
    epochs: u64,
    stop: &AtomicBool,
) -> Result<(EpochEncoder, SendLateness)> {
    let session_id = options.session_id;
    let epoch = EpochGeometry::new(options.epoch_samples);
    let spin_ns = epoch.spin_budget_ns();
    let mut encoder = EpochEncoder::new(&options)?;
    let socket = std::net::UdpSocket::bind(if target.is_ipv4() {
        "0.0.0.0:0"
    } else {
        "[::]:0"
    })?;
    let mut lateness = SendLateness::default();
    for epoch_id in 0..epochs {
        let deadline_ns = epoch_deadline_ns(session_id, epoch_id, epoch);
        if !wait_until_unix_ns(deadline_ns, spin_ns, stop)? {
            break;
        }
        // The epoch is encoded when its last sample has been spoken, as a
        // live source would, so the encode is part of the measured lateness.
        let encode_started = Instant::now();
        let datagrams = encoder.encode(epoch_id)?;
        lateness
            .encode
            .record(encode_started.elapsed().as_nanos() as u64);
        let mut first_sent_ns = None;
        let mut last_sent_ns = 0;
        for (payload, source) in datagrams {
            let sent = socket.send_to(&payload, target)?;
            last_sent_ns = now_unix_ns()?;
            first_sent_ns.get_or_insert(last_sent_ns);
            check_full_send(sent, payload.len())?;
            encoder.count_sent(sent, source);
        }
        lateness.record(
            deadline_ns,
            first_sent_ns.unwrap_or(last_sent_ns),
            last_sent_ns,
        );
    }
    Ok((encoder, lateness))
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

fn receive_group_ids(base_group_id: u16, group_count: u16) -> Result<Vec<u16>> {
    if group_count == 0 {
        bail!("--group-count must be positive");
    }
    if u32::from(base_group_id) + u32::from(group_count) > u32::from(u16::MAX) + 1 {
        bail!("--group-id plus --group-count exceeds the u16 group-id range");
    }
    Ok((0..group_count)
        .map(|index| base_group_id + index)
        .collect())
}

async fn receive_udp(options: UdpReceiveOptions<'_>) -> Result<UdpReceiveOutput> {
    let UdpReceiveOptions {
        relay,
        bind,
        session_id,
        group_id,
        group_count,
        formats,
        duration_seconds,
        deadline_ms,
        tail_seconds,
        decode_soundkit_opus,
        expected_opus_frame_samples,
        decoded_pcm_output,
    } = options;
    let group_ids = receive_group_ids(group_id, group_count)?;
    if decoded_pcm_output.is_some() && group_count != 1 {
        bail!("--decoded-pcm-output takes one group");
    }
    let decode_opus = decode_soundkit_opus || decoded_pcm_output.is_some();
    // Per group: the Opus frame sizes seen, and the consumer that decodes
    // the group's plain SoundKit Opus epochs in arrival order.
    let mut opus_states = group_ids
        .iter()
        .map(|_| {
            let consumer = decode_opus
                .then(|| {
                    SoundKitOpusConsumer::plain(OpusConsumerOptions {
                        first_sequence: 0,
                        decoded_pcm_output,
                        reference_pcm_s16le: None,
                        reference_leading_silence_frames: 0,
                        reference_start_offset_ms: 0,
                        reference_window_ms: 0,
                        expected_frame_samples: expected_opus_frame_samples,
                    })
                })
                .transpose()?;
            Ok((
                OpusFrameObservation::new(expected_opus_frame_samples),
                consumer,
            ))
        })
        .collect::<Result<Vec<_>>>()?;
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
    let window = ReceiveWindow::new(session_id, now_unix_ns()?, duration_seconds, tail_seconds);
    let stop_ns = window.stop_ns;
    let refresh = Duration::from_secs(5);
    let mut next_refresh = TokioInstant::now() + refresh;
    let mut format_states = group_ids
        .iter()
        .copied()
        .map(|group_id| Ok((group_id, ReceiveFormatState::new(formats)?)))
        .collect::<Result<Vec<_>>>()?;
    let mut wire_bytes = 0_u64;
    let mut source_datagrams_received = 0_u64;
    let mut repair_datagrams_received = 0_u64;

    while now_unix_ns()? < stop_ns {
        let remaining = Duration::from_nanos(stop_ns.saturating_sub(now_unix_ns()?));
        let wait = remaining.min(Duration::from_millis(250));
        match timeout(wait, socket.recv_from(&mut buf)).await {
            Ok(Ok((len, _))) => {
                let payload = match transport.payload(&buf[..len]) {
                    Ok(payload) => payload,
                    Err(_) => continue,
                };
                if let Ok(identity) = inspect_multichannel_audio_datagram(payload) {
                    if identity.source_index.is_some() {
                        source_datagrams_received = source_datagrams_received.saturating_add(1);
                    } else {
                        repair_datagrams_received = repair_datagrams_received.saturating_add(1);
                    }
                }
                wire_bytes = wire_bytes.saturating_add(len as u64);
                let outcome = receiver.push_datagram(payload)?;
                let arrival_ns = now_unix_ns()?;
                for group in outcome.completed_groups {
                    if group.session_id != session_id {
                        continue;
                    }
                    if let Some(index) = format_states
                        .iter()
                        .position(|(observed_group_id, _)| *observed_group_id == group.group_id)
                    {
                        format_states[index]
                            .1
                            .observe(&group, arrival_ns, deadline_ms, &window);
                        if group.flags & AUDIO_GROUP_FLAG_ERASURE == 0
                            && ReceiveAudioFormat::from_soundkit_v2_payload(&group.payload)
                                == Some(ReceiveAudioFormat::Opus)
                        {
                            let (frames, consumer) = &mut opus_states[index];
                            frames.observe(HlsAudioCodec::SoundkitOpus, &group.payload);
                            if let Some(consumer) = consumer.as_mut() {
                                consumer.decode_part(&group.payload)?;
                            }
                        }
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
    let has_shared_transport_envelope = group_count > 1;
    let reports = format_states
        .into_iter()
        .zip(opus_states)
        .map(
            |((group_id, mut format_state), (opus_frames, opus_consumer))| {
                let selected_formats = format_state.formats.len() as u64;
                // Epochs of the size received (1 ms Opus Custom: 1,000 a
                // second), or 5 ms if none arrived.
                let expected_epochs = format_state
                    .expected_epochs(duration_seconds, None)
                    .saturating_mul(selected_formats);
                let opus_consumer = opus_consumer
                    .map(SoundKitOpusConsumer::finish)
                    .transpose()?;
                let expected_pcm_frames = duration_seconds
                    .saturating_mul(u64::from(SAMPLE_RATE))
                    .saturating_mul(selected_formats);
                let received_epochs = format_state.selected_epoch_count();
                let latency_ms = percentiles_ms(std::mem::take(&mut format_state.latencies_ns));
                let latency_time_series =
                    format_state.finish_latency_time_series(window.start_ns, duration_seconds);
                Ok(ReceiveReport {
                    schema: "needletail.aep1-48k-probe.receive.v2",
                    lane: "native_udp_fec",
                    session_id,
                    group_id,
                    formats: format_state.formats,
                    sample_rate: SAMPLE_RATE,
                    duration_seconds,
                    expected_epochs,
                    received_epochs,
                    missing_epochs: expected_epochs.saturating_sub(received_epochs),
                    deadline_ms,
                    deadline_misses: format_state.deadline_misses,
                    transport_counter_scope: if has_shared_transport_envelope {
                        "multigroup_envelope"
                    } else {
                        "report"
                    },
                    datagrams_received: if has_shared_transport_envelope {
                        0
                    } else {
                        stats.datagrams_received
                    },
                    source_datagrams_received: if has_shared_transport_envelope {
                        0
                    } else {
                        source_datagrams_received
                    },
                    repair_datagrams_received: if has_shared_transport_envelope {
                        0
                    } else {
                        repair_datagrams_received
                    },
                    systematic_shards_received: if has_shared_transport_envelope {
                        0
                    } else {
                        stats.systematic_shards_received
                    },
                    raptorq_shards_recovered: if has_shared_transport_envelope {
                        0
                    } else {
                        stats.raptorq_shards_recovered
                    },
                    selected_raptorq_fragments_recovered: format_state
                        .selected_raptorq_fragments_recovered,
                    opus_epochs: format_state.opus_epochs.len() as u64,
                    flac_epochs: format_state.flac_epochs.len() as u64,
                    pcm_fallback_epochs: format_state.pcm_epochs.len() as u64,
                    erasure_epochs: format_state.erasure_epochs,
                    unexpected_payload_epochs: 0,
                    discontinuity_epochs: format_state.discontinuity_epochs,
                    expected_pcm_frames,
                    received_pcm_frames: format_state.received_pcm_frames,
                    missing_pcm_frames: expected_pcm_frames
                        .saturating_sub(format_state.received_pcm_frames),
                    duplicate_or_late_epochs: format_state.duplicate_or_late_epochs,
                    transport_duplicate_or_late_epochs: if has_shared_transport_envelope {
                        0
                    } else {
                        stats.duplicate_or_late_epochs
                    },
                    wire_bytes: if has_shared_transport_envelope {
                        0
                    } else {
                        wire_bytes
                    },
                    render_ready_latency_ms: latency_ms.clone(),
                    latency_ms,
                    latency_time_series,
                    opus_frames: opus_frames.report(),
                    opus_consumer,
                })
            },
        )
        .collect::<Result<Vec<_>>>()?;
    if group_count == 1 {
        return Ok(UdpReceiveOutput::Single(Box::new(
            reports
                .into_iter()
                .next()
                .context("single-group UDP report is missing")?,
        )));
    }
    Ok(UdpReceiveOutput::Multiple(MultiGroupReceiveReport {
        schema: "needletail.aep1-48k-probe.receive-multigroup.v2",
        lane: "native_udp_fec",
        session_id,
        group_id,
        group_count,
        group_ids,
        shared_transport: UdpTransportCounters {
            datagrams_received: stats.datagrams_received,
            source_datagrams_received,
            repair_datagrams_received,
            systematic_shards_received: stats.systematic_shards_received,
            raptorq_shards_recovered: stats.raptorq_shards_recovered,
            transport_duplicate_or_late_epochs: stats.duplicate_or_late_epochs,
            wire_bytes,
        },
        groups: reports,
    }))
}

async fn receive_webtransport(options: WebTransportReceiveOptions<'_>) -> Result<ReceiveReport> {
    let WebTransportReceiveOptions {
        edge,
        server_name,
        tls_ca,
        session_id,
        group_id,
        formats,
        duration_seconds,
        deadline_ms,
        tail_seconds,
        epoch_samples,
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
    let window = ReceiveWindow::new(session_id, now_unix_ns()?, duration_seconds, tail_seconds);
    let stop_ns = window.stop_ns;
    let media_end_ns = window.media_end_ns;
    let mut format_state = ReceiveFormatState::new(formats)?;
    let mut wire_bytes = 0_u64;
    let mut source_datagrams_received = 0_u64;
    let mut repair_datagrams_received = 0_u64;

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
        if let Ok(identity) = inspect_multichannel_audio_datagram(payload) {
            if identity.source_index.is_some() {
                source_datagrams_received = source_datagrams_received.saturating_add(1);
            } else {
                repair_datagrams_received = repair_datagrams_received.saturating_add(1);
            }
        }
        wire_bytes = wire_bytes.saturating_add(wire.len() as u64);
        let outcome = receiver.push_datagram(payload)?;
        let arrival_ns = now_unix_ns()?;
        for group in outcome.completed_groups {
            if group.session_id != session_id || group.group_id != group_id {
                continue;
            }
            format_state.observe(&group, arrival_ns, deadline_ms, &window);
        }
    }
    endpoint.close(0_u32.into(), b"probe complete");

    let stats = receiver.stats();
    let selected_formats = format_state.formats.len() as u64;
    let expected_epochs = format_state
        .expected_epochs(duration_seconds, epoch_samples)
        .saturating_mul(selected_formats);
    let expected_pcm_frames = duration_seconds
        .saturating_mul(u64::from(SAMPLE_RATE))
        .saturating_mul(selected_formats);
    let received_epochs = format_state.selected_epoch_count();
    let latency_ms = percentiles_ms(std::mem::take(&mut format_state.latencies_ns));
    Ok(ReceiveReport {
        schema: "needletail.aep1-48k-probe.receive.v1",
        lane: "webtransport",
        session_id,
        group_id,
        formats: format_state.formats,
        sample_rate: SAMPLE_RATE,
        duration_seconds,
        expected_epochs,
        received_epochs,
        missing_epochs: expected_epochs.saturating_sub(received_epochs),
        deadline_ms,
        deadline_misses: format_state.deadline_misses,
        transport_counter_scope: "report",
        datagrams_received: stats.datagrams_received,
        source_datagrams_received,
        repair_datagrams_received,
        systematic_shards_received: stats.systematic_shards_received,
        raptorq_shards_recovered: stats.raptorq_shards_recovered,
        selected_raptorq_fragments_recovered: format_state.selected_raptorq_fragments_recovered,
        opus_epochs: format_state.opus_epochs.len() as u64,
        flac_epochs: format_state.flac_epochs.len() as u64,
        pcm_fallback_epochs: format_state.pcm_epochs.len() as u64,
        erasure_epochs: format_state.erasure_epochs,
        unexpected_payload_epochs: 0,
        discontinuity_epochs: format_state.discontinuity_epochs,
        expected_pcm_frames,
        received_pcm_frames: format_state.received_pcm_frames,
        missing_pcm_frames: expected_pcm_frames.saturating_sub(format_state.received_pcm_frames),
        duplicate_or_late_epochs: format_state.duplicate_or_late_epochs,
        transport_duplicate_or_late_epochs: stats.duplicate_or_late_epochs,
        wire_bytes,
        render_ready_latency_ms: latency_ms.clone(),
        latency_ms,
        latency_time_series: Vec::new(),
        opus_frames: OpusFrameReport::default(),
        opus_consumer: None,
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
        response_ms,
        deadline_ms,
        render_buffer_ms,
        start_offset_ms,
        window_seconds,
        tail_seconds,
        expected_audio_codec,
        expected_pcm_channels,
        soundkit_opus_protection,
        object_sequence_origin,
        abandon_after_ms,
        soundkit_key_file,
        decode_soundkit_opus,
        expected_opus_frame_samples,
        decoded_pcm_output,
        raw_part_output_dir,
        reference_pcm_s16le,
        reference_leading_silence_frames,
        shared_h3,
    } = options;
    if part_ms == 0 {
        bail!("--part-ms must be positive");
    }
    if response_ms < part_ms || !response_ms.is_multiple_of(part_ms) {
        bail!("--response-ms must be a positive multiple of --part-ms");
    }
    let parts_per_response = response_ms / part_ms;
    if parts_per_response > 200 {
        bail!("--response-ms may contain at most 200 media parts");
    }
    if parts_per_response > 1 && expected_audio_codec != HlsAudioCodec::SoundkitOpus {
        bail!("aggregated response qualification currently requires SoundKit Opus");
    }
    if !path_prefix.is_empty() && (!path_prefix.starts_with('/') || path_prefix.ends_with('/')) {
        bail!("--path-prefix must be empty or start with one slash and have no trailing slash");
    }
    if parts_per_response > 1 && path_prefix.is_empty() {
        bail!("aggregated responses require the edge tail route");
    }
    let end_offset_ms =
        hls_window_end_ms(duration_seconds, start_offset_ms, window_seconds, part_ms)?;
    let stop_ns = session_id
        .saturating_add(end_offset_ms.saturating_mul(1_000_000))
        .saturating_add(tail_seconds.saturating_mul(1_000_000_000));
    if matches!(
        expected_audio_codec,
        HlsAudioCodec::Ipcm | HlsAudioCodec::Fpcm
    ) && expected_pcm_channels == 0
    {
        bail!("--expected-pcm-channels must be positive for PCM LL-HLS");
    }
    if soundkit_key_file.is_some() && expected_audio_codec != HlsAudioCodec::SoundkitOpus {
        bail!("--soundkit-key-file requires --expected-audio-codec soundkit-opus");
    }
    let decode_opus = soundkit_key_file.is_some()
        || decode_soundkit_opus
        || decoded_pcm_output.is_some()
        || reference_pcm_s16le.is_some();
    if decode_opus && expected_audio_codec != HlsAudioCodec::SoundkitOpus {
        bail!("consumer decode requires --expected-audio-codec soundkit-opus");
    }
    if decode_opus
        && soundkit_key_file.is_none()
        && soundkit_opus_protection != SoundkitOpusProtection::Plain
    {
        bail!(
            "consumer decode of encrypted SoundKit Opus requires --soundkit-key-file; \
             plain packets need --soundkit-opus-protection plain"
        );
    }
    if soundkit_key_file.is_some() && soundkit_opus_protection == SoundkitOpusProtection::Plain {
        bail!("--soundkit-key-file decodes encrypted packets; drop it for plain protection");
    }
    if expected_opus_frame_samples.is_some()
        && !matches!(
            expected_audio_codec,
            HlsAudioCodec::SoundkitOpus | HlsAudioCodec::Opus
        )
    {
        bail!("--expected-opus-frame-samples requires an Opus --expected-audio-codec");
    }
    let mut opus_frames = OpusFrameObservation::new(expected_opus_frame_samples);
    let initial_pts_sequence = start_offset_ms.saturating_div(part_ms);
    let expected_parts = end_offset_ms.saturating_sub(start_offset_ms) / part_ms;
    let mut after_sequence: Option<u64> = None;
    let mut part_coverage = PartCoverage::new(initial_pts_sequence, expected_parts, part_ms)?;
    let mut media_timeline = MediaTimelineCoverage::new(
        initial_pts_sequence,
        expected_parts,
        start_offset_ms,
        end_offset_ms,
    );
    let mut availability_latencies_ns = BoundedLatencySamples::new();
    let mut publication_to_cache_latencies_ns = BoundedLatencySamples::new();
    let mut cache_to_client_latencies_ns = BoundedLatencySamples::new();
    let mut render_latencies_ns = BoundedLatencySamples::new();
    let mut latency_time_series = HlsLatencyTimeSeries::default();
    let mut first_part_to_response_latencies_ns = BoundedLatencySamples::new();
    let mut final_part_to_response_latencies_ns = BoundedLatencySamples::new();
    let mut media_responses = 0_u64;
    let mut deadline_misses = 0_u64;
    let mut wire_bytes = 0_u64;
    let mut init_audio_codec = None;
    let mut init_audio_codec_verified = false;
    let mut pcm_media_parts_verified = 0_u64;
    let mut pcm_media_size_mismatches = 0_u64;
    let mut opaque_flac_media_parts_verified = 0_u64;
    let mut opaque_flac_frame_mismatches = 0_u64;
    let mut opus_media_parts_verified = 0_u64;
    let mut opus_media_packet_mismatches = 0_u64;
    let mut fmp4_video_parts_verified = 0_u64;
    let mut fmp4_video_part_mismatches = 0_u64;
    let mut fmp4_sequence_offset: Option<i64> = None;
    if let Some(dir) = raw_part_output_dir {
        std::fs::create_dir_all(dir)
            .with_context(|| format!("create raw LL-HLS part output dir {}", dir.display()))?;
    }
    let mut playlist_has_ll_hls_tags = false;
    let direct_part_route = path_prefix.is_empty();
    // Do not create an H3 connection and then leave it idle while waiting for
    // a scheduled late-join window. Connect at the preload boundary so the
    // first media request cannot inherit a Quinn idle timeout.
    if shared_h3.is_none() && matches!(transport, HlsTransport::H3) && !direct_part_route {
        let preload_at_ns = h3_preload_at_ns(session_id, start_offset_ms);
        let now_ns = now_unix_ns()?;
        if now_ns < preload_at_ns {
            sleep_until(TokioInstant::now() + Duration::from_nanos(preload_at_ns - now_ns)).await;
        }
    }
    let mut h3_client = match transport {
        HlsTransport::H3 => Some(match shared_h3 {
            Some(client) => ActiveH3Client::Shared(client),
            None => ActiveH3Client::Owned(H3HttpsClient::connect(edge, server_name, tls_ca).await?),
        }),
        HlsTransport::Http1 => None,
    };
    let connection_setup_ms = h3_client.as_ref().map(ActiveH3Client::connection_setup_ms);
    let mut observed_empty_playlist = false;
    let (mut object_sequence_baseline, mut allow_initial_object_rebase) = loop {
        let playlist = timeout(
            HLS_CONTROL_REQUEST_TIMEOUT,
            hls_https_get(
                &mut h3_client,
                edge,
                server_name,
                tls_ca,
                &hls_path(path_prefix, stream_id, "stream.m3u8"),
            ),
        )
        .await
        .context("initial LL-HLS playlist request exceeded two seconds")??;
        wire_bytes = wire_bytes.saturating_add(playlist.wire_bytes as u64);
        if playlist.status == 200 {
            playlist_has_ll_hls_tags |=
                playlist_matches_format(&playlist.body, expected_audio_codec);
            if let Some(window) = hls_playlist_object_window(&playlist.body)? {
                break (
                    initial_hls_object_sequence(window, observed_empty_playlist),
                    window.first_sequence.is_none(),
                );
            }
        }
        observed_empty_playlist = true;
        if now_unix_ns()? >= stop_ns {
            bail!("LL-HLS playlist never advertised a part object sequence");
        }
        sleep(Duration::from_millis(part_ms.clamp(1, 25))).await;
    };
    if let Some(origin) = object_sequence_origin {
        // The caller knows which object carries offset zero, so the first
        // object of this window is exact and is never moved by the playlist.
        object_sequence_baseline = origin
            .checked_add(initial_pts_sequence)
            .context("--object-sequence-origin plus the start offset overflows")?;
        allow_initial_object_rebase = false;
    }
    let mut object_sequence_started = false;
    let mut opus_consumer = if decode_opus {
        let consumer_options = OpusConsumerOptions {
            first_sequence: object_sequence_baseline,
            decoded_pcm_output,
            reference_pcm_s16le,
            reference_leading_silence_frames,
            reference_start_offset_ms: start_offset_ms,
            reference_window_ms: end_offset_ms.saturating_sub(start_offset_ms),
            expected_frame_samples: expected_opus_frame_samples,
        };
        Some(match soundkit_key_file {
            Some(key_file) => SoundKitOpusConsumer::from_key_file(key_file, consumer_options)?,
            None => SoundKitOpusConsumer::plain(consumer_options)?,
        })
    } else {
        None
    };

    if parts_per_response > 1 {
        if expected_parts % parts_per_response != 0 {
            bail!("qualification window must contain a whole number of aggregated responses");
        }
        while now_unix_ns()? < stop_ns && part_coverage.received_parts < expected_parts {
            let path = after_sequence.map_or_else(
                || {
                    hls_path(
                        path_prefix,
                        stream_id,
                        &format!("tail?from={object_sequence_baseline}&parts={parts_per_response}"),
                    )
                },
                |sequence| {
                    hls_path(
                        path_prefix,
                        stream_id,
                        &format!("tail?after={sequence}&parts={parts_per_response}"),
                    )
                },
            );
            let response = hls_https_get(&mut h3_client, edge, server_name, tls_ca, &path).await?;
            wire_bytes = wire_bytes.saturating_add(response.wire_bytes as u64);
            match response.status {
                200 => {
                    media_responses = media_responses.saturating_add(1);
                    let header_u64 = |name: &str| -> Result<u64> {
                        response
                            .headers
                            .get(name)
                            .with_context(|| format!("aggregated LL-HLS response omitted {name}"))?
                            .parse::<u64>()
                            .with_context(|| {
                                format!("aggregated LL-HLS response returned an invalid {name}")
                            })
                    };
                    let start_sequence = header_u64("x-sequence-start")?;
                    let end_sequence = header_u64("x-sequence-end")?;
                    let response_part_count = header_u64("x-part-count")?;
                    let continuation_sequence = header_u64("x-sequence")?;
                    let expected_start = after_sequence
                        .and_then(|sequence| sequence.checked_add(1))
                        .unwrap_or({
                            if allow_initial_object_rebase {
                                start_sequence
                            } else {
                                object_sequence_baseline
                            }
                        });
                    let expected_end = start_sequence
                        .checked_add(parts_per_response.saturating_sub(1))
                        .context("aggregated LL-HLS sequence range overflow")?;
                    if start_sequence != expected_start
                        || end_sequence != expected_end
                        || continuation_sequence != end_sequence
                        || response_part_count != parts_per_response
                    {
                        bail!(
                            "aggregated LL-HLS range {start_sequence}..={end_sequence} ({response_part_count} parts, cursor {continuation_sequence}) did not match expected {expected_start}..={expected_end}"
                        );
                    }
                    let parts = split_soundkit_opus_parts(&response.body, part_ms)?;
                    if parts.len() as u64 != parts_per_response {
                        bail!(
                            "aggregated LL-HLS body contained {} media units, expected {parts_per_response}",
                            parts.len()
                        );
                    }
                    if !object_sequence_started && start_sequence != object_sequence_baseline {
                        object_sequence_baseline = start_sequence;
                        if let Some(consumer) = opus_consumer.as_mut() {
                            consumer.rebase_before_media(start_sequence)?;
                        }
                    }
                    allow_initial_object_rebase = false;
                    object_sequence_started = true;
                    let arrival_ns = now_unix_ns()?;
                    let first_pts_ms = media_part_pts_ms(
                        expected_audio_codec,
                        &[],
                        start_sequence,
                        object_sequence_baseline,
                        start_offset_ms,
                        part_ms,
                    )?;
                    let final_pts_ms = media_part_pts_ms(
                        expected_audio_codec,
                        &[],
                        end_sequence,
                        object_sequence_baseline,
                        start_offset_ms,
                        part_ms,
                    )?;
                    let first_capture_ns =
                        session_id.saturating_add(first_pts_ms.saturating_mul(1_000_000));
                    let final_capture_ns =
                        session_id.saturating_add(final_pts_ms.saturating_mul(1_000_000));
                    if arrival_ns >= first_capture_ns {
                        first_part_to_response_latencies_ns.record(arrival_ns - first_capture_ns);
                    }
                    if arrival_ns >= final_capture_ns {
                        final_part_to_response_latencies_ns.record(arrival_ns - final_capture_ns);
                    }

                    for (offset, part) in parts.into_iter().enumerate() {
                        let sequence = start_sequence.saturating_add(offset as u64);
                        let pts_ms = media_part_pts_ms(
                            expected_audio_codec,
                            part,
                            sequence,
                            object_sequence_baseline,
                            start_offset_ms,
                            part_ms,
                        )?;
                        let coverage_pts_ms = nominal_hls_object_pts_ms(
                            sequence,
                            object_sequence_baseline,
                            start_offset_ms,
                            part_ms,
                        )?;
                        if coverage_pts_ms < start_offset_ms || coverage_pts_ms >= end_offset_ms {
                            continue;
                        }
                        if !part_coverage.insert_pts(coverage_pts_ms) {
                            bail!(
                                "aggregated LL-HLS returned duplicate or invalid object {sequence}"
                            );
                        }
                        media_timeline.insert(
                            sequence,
                            pts_ms,
                            media_part_duration_ms(expected_audio_codec, part, part_ms)?,
                        );
                        let capture_ns =
                            session_id.saturating_add(pts_ms.saturating_mul(1_000_000));
                        let latency_ns = arrival_ns.saturating_sub(capture_ns);
                        let deadline_missed = latency_ns > deadline_ms.saturating_mul(1_000_000);
                        if deadline_missed {
                            deadline_misses = deadline_misses.saturating_add(1);
                        }
                        availability_latencies_ns.record(latency_ns);
                        if soundkit_opus_part_is_valid(part, part_ms, soundkit_opus_protection)
                            && opus_frames.observe(expected_audio_codec, part)
                        {
                            opus_media_parts_verified = opus_media_parts_verified.saturating_add(1);
                            init_audio_codec_verified = true;
                        } else {
                            opus_media_packet_mismatches =
                                opus_media_packet_mismatches.saturating_add(1);
                        }
                        write_raw_hls_part(raw_part_output_dir, sequence, pts_ms, part)?;
                        if let Some(consumer) = opus_consumer.as_mut() {
                            consumer.push_part(sequence, part)?;
                        }
                        let render_latency_ns =
                            latency_ns.saturating_add(render_buffer_ms.saturating_mul(1_000_000));
                        render_latencies_ns.record(render_latency_ns);
                        latency_time_series.record_part(
                            coverage_pts_ms,
                            latency_ns,
                            render_latency_ns,
                            deadline_missed,
                        );
                    }
                    if let Some((publication_to_cache_ns, cache_to_client_ns)) =
                        split_cache_latency_ns(&response.headers, final_capture_ns, arrival_ns)
                    {
                        publication_to_cache_latencies_ns.record(publication_to_cache_ns);
                        cache_to_client_latencies_ns.record(cache_to_client_ns);
                        latency_time_series.record_cache(
                            final_pts_ms,
                            publication_to_cache_ns,
                            cache_to_client_ns,
                        );
                    }
                    after_sequence = Some(end_sequence);
                }
                204 | 404 => tokio::task::yield_now().await,
                status => bail!("aggregated LL-HLS tail returned HTTP {status}"),
            }
        }
    } else if !matches!(transport, HlsTransport::H3) || direct_part_route {
        while now_unix_ns()? < stop_ns && part_coverage.received_parts < expected_parts {
            let requested_sequence = after_sequence.map_or(object_sequence_baseline, |sequence| {
                sequence.saturating_add(1)
            });
            let path = if direct_part_route {
                hls_path(
                    path_prefix,
                    stream_id,
                    &format!(
                        "p{requested_sequence}.{}",
                        expected_audio_codec.media_extension()
                    ),
                )
            } else {
                after_sequence.map_or_else(
                    || {
                        hls_path(
                            path_prefix,
                            stream_id,
                            &format!("tail?from={object_sequence_baseline}"),
                        )
                    },
                    |sequence| hls_path(path_prefix, stream_id, &format!("tail?after={sequence}")),
                )
            };
            let response = hls_https_get(&mut h3_client, edge, server_name, tls_ca, &path).await?;
            wire_bytes = wire_bytes.saturating_add(response.wire_bytes as u64);
            match response.status {
                200 => {
                    media_responses = media_responses.saturating_add(1);
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
                    if !object_sequence_started && sequence != object_sequence_baseline {
                        if !allow_initial_object_rebase {
                            bail!(
                                "LL-HLS tail started at object {sequence}, expected playlist object {object_sequence_baseline}"
                            );
                        }
                        object_sequence_baseline = sequence;
                        if let Some(consumer) = opus_consumer.as_mut() {
                            consumer.rebase_before_media(sequence)?;
                        }
                    }
                    allow_initial_object_rebase = false;
                    object_sequence_started = true;
                    after_sequence = Some(sequence);
                    let opus_valid = opus_media_part_is_valid(
                        expected_audio_codec,
                        &response.body,
                        part_ms,
                        soundkit_opus_protection,
                    )
                    .map(|valid| {
                        valid && opus_frames.observe(expected_audio_codec, &response.body)
                    });
                    let opaque_flac_valid = expected_audio_codec == HlsAudioCodec::OpaqueFlac
                        && opaque_flac_part_is_valid(&response.body);
                    let pts_ms = media_part_pts_ms(
                        expected_audio_codec,
                        &response.body,
                        sequence,
                        object_sequence_baseline,
                        start_offset_ms,
                        part_ms,
                    )?;
                    let coverage_pts_ms = nominal_hls_object_pts_ms(
                        sequence,
                        object_sequence_baseline,
                        start_offset_ms,
                        part_ms,
                    )?;
                    let arrival_ns = now_unix_ns()?;
                    if arrival_ns >= session_id
                        && coverage_pts_ms >= start_offset_ms
                        && coverage_pts_ms < end_offset_ms
                        && part_coverage.insert_pts(coverage_pts_ms)
                    {
                        media_timeline.insert(
                            sequence,
                            pts_ms,
                            media_part_duration_ms(expected_audio_codec, &response.body, part_ms)?,
                        );
                        let capture_ns =
                            session_id.saturating_add(pts_ms.saturating_mul(1_000_000));
                        let latency_ns = arrival_ns.saturating_sub(capture_ns);
                        let deadline_missed = latency_ns > deadline_ms.saturating_mul(1_000_000);
                        if deadline_missed {
                            deadline_misses = deadline_misses.saturating_add(1);
                        }
                        availability_latencies_ns.record(latency_ns);
                        first_part_to_response_latencies_ns.record(latency_ns);
                        final_part_to_response_latencies_ns.record(latency_ns);
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
                        if let Some(opus_valid) = opus_valid {
                            if opus_valid {
                                opus_media_parts_verified =
                                    opus_media_parts_verified.saturating_add(1);
                                if expected_audio_codec == HlsAudioCodec::SoundkitOpus {
                                    init_audio_codec_verified = true;
                                }
                            } else {
                                opus_media_packet_mismatches =
                                    opus_media_packet_mismatches.saturating_add(1);
                            }
                        }
                        if expected_audio_codec == HlsAudioCodec::H264 {
                            if h264_part_is_valid(
                                &response.body,
                                sequence,
                                &mut fmp4_sequence_offset,
                            ) {
                                fmp4_video_parts_verified =
                                    fmp4_video_parts_verified.saturating_add(1);
                            } else {
                                fmp4_video_part_mismatches =
                                    fmp4_video_part_mismatches.saturating_add(1);
                            }
                        }
                        if expected_audio_codec == HlsAudioCodec::OpaqueFlac {
                            if opaque_flac_valid {
                                opaque_flac_media_parts_verified =
                                    opaque_flac_media_parts_verified.saturating_add(1);
                                init_audio_codec_verified = true;
                            } else {
                                opaque_flac_frame_mismatches =
                                    opaque_flac_frame_mismatches.saturating_add(1);
                            }
                        }
                        write_raw_hls_part(raw_part_output_dir, sequence, pts_ms, &response.body)?;
                        if let Some(consumer) = opus_consumer.as_mut() {
                            consumer.push_part(sequence, &response.body)?;
                        }
                        if let Some((publication_to_cache_ns, cache_to_client_ns)) =
                            split_cache_latency_ns(&response.headers, capture_ns, arrival_ns)
                        {
                            publication_to_cache_latencies_ns.record(publication_to_cache_ns);
                            cache_to_client_latencies_ns.record(cache_to_client_ns);
                            latency_time_series.record_cache(
                                coverage_pts_ms,
                                publication_to_cache_ns,
                                cache_to_client_ns,
                            );
                        }
                        let render_latency_ns =
                            latency_ns.saturating_add(render_buffer_ms.saturating_mul(1_000_000));
                        render_latencies_ns.record(render_latency_ns);
                        latency_time_series.record_part(
                            coverage_pts_ms,
                            latency_ns,
                            render_latency_ns,
                            deadline_missed,
                        );
                    }
                    if expected_audio_codec.init_marker().is_some() && init_audio_codec.is_none() {
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
                            playlist_has_ll_hls_tags =
                                playlist_matches_format(&playlist.body, expected_audio_codec);
                        }
                    }
                }
                204 | 404 => {
                    if !object_sequence_started {
                        let playlist = timeout(
                            HLS_CONTROL_REQUEST_TIMEOUT,
                            hls_https_get(
                                &mut h3_client,
                                edge,
                                server_name,
                                tls_ca,
                                &hls_path(path_prefix, stream_id, "stream.m3u8"),
                            ),
                        )
                        .await
                        .context("LL-HLS playlist baseline refresh exceeded two seconds")??;
                        wire_bytes = wire_bytes.saturating_add(playlist.wire_bytes as u64);
                        if playlist.status == 200 {
                            playlist_has_ll_hls_tags |=
                                playlist_matches_format(&playlist.body, expected_audio_codec);
                            if let Some(window) = hls_playlist_object_window(&playlist.body)? {
                                let refreshed_sequence = if object_sequence_origin.is_some() {
                                    object_sequence_baseline
                                } else {
                                    refreshed_hls_object_sequence(object_sequence_baseline, window)
                                };
                                if refreshed_sequence != object_sequence_baseline {
                                    object_sequence_baseline = refreshed_sequence;
                                    after_sequence = object_sequence_baseline.checked_sub(1);
                                    allow_initial_object_rebase = window.first_sequence.is_none();
                                    if let Some(consumer) = opus_consumer.as_mut() {
                                        consumer.rebase_before_media(object_sequence_baseline)?;
                                    }
                                }
                            }
                        }
                    }
                    tokio::task::yield_now().await;
                }
                status => bail!("LL-HLS tail returned HTTP {status}"),
            }
        }
    } else {
        type PartRequest = BoxFuture<'static, (u64, Result<SimpleHttpResponse>)>;
        let mut next_sequence = object_sequence_baseline;
        let mut in_flight = FuturesUnordered::<PartRequest>::new();
        // Requested sequences that have no answer yet.
        let mut pending = std::collections::BTreeSet::<u64>::new();
        let abandon_after_ns = abandon_after_ms.map(|ms| ms.saturating_mul(1_000_000));
        // Nominal time at which an object is complete at the source.
        let complete_ns = |sequence: u64, baseline: u64| -> u64 {
            session_id.saturating_add(
                start_offset_ms
                    .saturating_add(
                        sequence
                            .saturating_sub(baseline)
                            .saturating_add(1)
                            .saturating_mul(part_ms),
                    )
                    .saturating_mul(1_000_000),
            )
        };
        while in_flight.len() < H3_PART_PIPELINE_DEPTH {
            let requested_sequence = next_sequence;
            next_sequence = next_sequence.saturating_add(1);
            pending.insert(requested_sequence);
            let path = hls_path(
                path_prefix,
                stream_id,
                &format!(
                    "part{requested_sequence}.{}",
                    expected_audio_codec.media_extension()
                ),
            );
            let sender = h3_client
                .as_ref()
                .context("HTTP/3 media pipeline omitted its connection")?
                .request_sender();
            in_flight.push(async move { (requested_sequence, sender.get(path).await) }.boxed());
        }

        // Keep H3_PART_PIPELINE_DEPTH requests outstanding that are not
        // overdue. Used with --abandon-after-ms only: an overdue request does
        // not hold back later parts, and a reader that was behind does not
        // request further ahead than the pipeline depth afterwards.
        macro_rules! top_up_part_requests {
            () => {{
                let now_ns = now_unix_ns()?;
                loop {
                    let active = pending
                        .iter()
                        .rev()
                        .take_while(|sequence| {
                            complete_ns(**sequence, object_sequence_baseline)
                                .saturating_add(H3_PART_OVERDUE_GRACE_NS)
                                > now_ns
                        })
                        .count();
                    if active >= H3_PART_PIPELINE_DEPTH
                        || pending.len() >= H3_PART_PENDING_LIMIT
                        || part_coverage.received_parts >= expected_parts
                    {
                        break;
                    }
                    let sequence = next_sequence;
                    next_sequence = next_sequence.saturating_add(1);
                    let path = hls_path(
                        path_prefix,
                        stream_id,
                        &format!("part{sequence}.{}", expected_audio_codec.media_extension()),
                    );
                    let sender = h3_client
                        .as_ref()
                        .context("HTTP/3 media pipeline omitted its connection")?
                        .request_sender();
                    pending.insert(sequence);
                    in_flight.push(async move { (sequence, sender.get(path).await) }.boxed());
                }
            }};
        }

        while now_unix_ns()? < stop_ns
            && !in_flight.is_empty()
            && part_coverage.received_parts < expected_parts
        {
            let remaining = Duration::from_nanos(stop_ns.saturating_sub(now_unix_ns()?));
            let (requested_sequence, response) = if abandon_after_ns.is_some() {
                // Wake often enough to request a part on time while an earlier
                // request is overdue.
                let tick = Duration::from_millis(part_ms.clamp(1, 20)).min(remaining);
                match timeout(tick, in_flight.next()).await {
                    Ok(Some(answer)) => answer,
                    Ok(None) => break,
                    Err(_) => {
                        top_up_part_requests!();
                        continue;
                    }
                }
            } else {
                let Some(answer) = timeout(remaining, in_flight.next()).await.ok().flatten() else {
                    break;
                };
                answer
            };
            pending.remove(&requested_sequence);
            let response = match response {
                Ok(response) => response,
                Err(first_error) => {
                    let restart_sequence = object_sequence_baseline.saturating_add(
                        part_coverage
                            .first_missing_relative()
                            .unwrap_or(expected_parts),
                    );
                    let client = h3_client
                        .as_mut()
                        .context("HTTP/3 media pipeline omitted its connection")?;
                    let ActiveH3Client::Owned(client) = client else {
                        bail!(
                            "shared HTTP/3 media pipeline cannot reconnect after request failure: {first_error:#}"
                        );
                    };
                    client
                        .reconnect(edge, server_name, tls_ca)
                        .await
                        .with_context(|| {
                            format!(
                                "HTTP/3 media pipeline reconnect failed after object {requested_sequence}: {first_error:#}"
                            )
                        })?;

                    // Every pending request belongs to the closed connection.
                    // Restart at the earliest part that has not passed validation
                    // and has not been given up.
                    in_flight = FuturesUnordered::<PartRequest>::new();
                    pending.clear();
                    next_sequence = restart_sequence;
                    if let Some(abandon_ns) = abandon_after_ns {
                        let now_ns = now_unix_ns()?;
                        while complete_ns(next_sequence, object_sequence_baseline)
                            .saturating_add(abandon_ns)
                            <= now_ns
                        {
                            next_sequence = next_sequence.saturating_add(1);
                        }
                    }
                    while in_flight.len() < H3_PART_PIPELINE_DEPTH {
                        let requested_sequence = next_sequence;
                        next_sequence = next_sequence.saturating_add(1);
                        pending.insert(requested_sequence);
                        let path = hls_path(
                            path_prefix,
                            stream_id,
                            &format!(
                                "part{requested_sequence}.{}",
                                expected_audio_codec.media_extension()
                            ),
                        );
                        let sender = client.request_sender();
                        in_flight.push(
                            async move { (requested_sequence, sender.get(path).await) }.boxed(),
                        );
                    }
                    continue;
                }
            };
            wire_bytes = wire_bytes.saturating_add(response.wire_bytes as u64);
            let mut retry_sequence = None;
            let arrival_or_now_ns = now_unix_ns()?;
            match response.status {
                200 => {
                    media_responses = media_responses.saturating_add(1);
                    object_sequence_started = true;
                    let opus_valid = opus_media_part_is_valid(
                        expected_audio_codec,
                        &response.body,
                        part_ms,
                        soundkit_opus_protection,
                    )
                    .map(|valid| {
                        valid && opus_frames.observe(expected_audio_codec, &response.body)
                    });
                    let opaque_flac_valid = expected_audio_codec == HlsAudioCodec::OpaqueFlac
                        && opaque_flac_part_is_valid(&response.body);
                    let pts_ms = media_part_pts_ms(
                        expected_audio_codec,
                        &response.body,
                        requested_sequence,
                        object_sequence_baseline,
                        start_offset_ms,
                        part_ms,
                    )?;
                    let coverage_pts_ms = nominal_hls_object_pts_ms(
                        requested_sequence,
                        object_sequence_baseline,
                        start_offset_ms,
                        part_ms,
                    )?;
                    let arrival_ns = now_unix_ns()?;
                    if arrival_ns >= session_id
                        && coverage_pts_ms >= start_offset_ms
                        && coverage_pts_ms < end_offset_ms
                        && part_coverage.insert_pts(coverage_pts_ms)
                    {
                        media_timeline.insert(
                            requested_sequence,
                            pts_ms,
                            media_part_duration_ms(expected_audio_codec, &response.body, part_ms)?,
                        );
                        let capture_ns =
                            session_id.saturating_add(pts_ms.saturating_mul(1_000_000));
                        let latency_ns = arrival_ns.saturating_sub(capture_ns);
                        let deadline_missed = latency_ns > deadline_ms.saturating_mul(1_000_000);
                        if deadline_missed {
                            deadline_misses = deadline_misses.saturating_add(1);
                        }
                        availability_latencies_ns.record(latency_ns);
                        first_part_to_response_latencies_ns.record(latency_ns);
                        final_part_to_response_latencies_ns.record(latency_ns);
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
                        if let Some(opus_valid) = opus_valid {
                            if opus_valid {
                                opus_media_parts_verified =
                                    opus_media_parts_verified.saturating_add(1);
                                if expected_audio_codec == HlsAudioCodec::SoundkitOpus {
                                    init_audio_codec_verified = true;
                                }
                            } else {
                                opus_media_packet_mismatches =
                                    opus_media_packet_mismatches.saturating_add(1);
                            }
                        }
                        if expected_audio_codec == HlsAudioCodec::H264 {
                            if h264_part_is_valid(
                                &response.body,
                                requested_sequence,
                                &mut fmp4_sequence_offset,
                            ) {
                                fmp4_video_parts_verified =
                                    fmp4_video_parts_verified.saturating_add(1);
                            } else {
                                fmp4_video_part_mismatches =
                                    fmp4_video_part_mismatches.saturating_add(1);
                            }
                        }
                        if expected_audio_codec == HlsAudioCodec::OpaqueFlac {
                            if opaque_flac_valid {
                                opaque_flac_media_parts_verified =
                                    opaque_flac_media_parts_verified.saturating_add(1);
                                init_audio_codec_verified = true;
                            } else {
                                opaque_flac_frame_mismatches =
                                    opaque_flac_frame_mismatches.saturating_add(1);
                            }
                        }
                        write_raw_hls_part(
                            raw_part_output_dir,
                            requested_sequence,
                            pts_ms,
                            &response.body,
                        )?;
                        if let Some(consumer) = opus_consumer.as_mut() {
                            consumer.push_part(requested_sequence, &response.body)?;
                        }
                        if let Some((publication_to_cache_ns, cache_to_client_ns)) =
                            split_cache_latency_ns(&response.headers, capture_ns, arrival_ns)
                        {
                            publication_to_cache_latencies_ns.record(publication_to_cache_ns);
                            cache_to_client_latencies_ns.record(cache_to_client_ns);
                            latency_time_series.record_cache(
                                coverage_pts_ms,
                                publication_to_cache_ns,
                                cache_to_client_ns,
                            );
                        }
                        let render_latency_ns =
                            latency_ns.saturating_add(render_buffer_ms.saturating_mul(1_000_000));
                        render_latencies_ns.record(render_latency_ns);
                        latency_time_series.record_part(
                            coverage_pts_ms,
                            latency_ns,
                            render_latency_ns,
                            deadline_missed,
                        );
                    }
                    if expected_audio_codec.init_marker().is_some() && init_audio_codec.is_none() {
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
                }
                204 | 404 => {
                    if !object_sequence_started {
                        let playlist = timeout(
                            HLS_CONTROL_REQUEST_TIMEOUT,
                            hls_https_get(
                                &mut h3_client,
                                edge,
                                server_name,
                                tls_ca,
                                &hls_path(path_prefix, stream_id, "stream.m3u8"),
                            ),
                        )
                        .await
                        .context("LL-HLS playlist baseline refresh exceeded two seconds")??;
                        wire_bytes = wire_bytes.saturating_add(playlist.wire_bytes as u64);
                        if playlist.status == 200 {
                            playlist_has_ll_hls_tags |=
                                playlist_matches_format(&playlist.body, expected_audio_codec);
                            if let Some(window) = hls_playlist_object_window(&playlist.body)? {
                                let refreshed_sequence = if object_sequence_origin.is_some() {
                                    object_sequence_baseline
                                } else {
                                    refreshed_hls_object_sequence(object_sequence_baseline, window)
                                };
                                if refreshed_sequence != object_sequence_baseline {
                                    object_sequence_baseline = refreshed_sequence;
                                    next_sequence = next_sequence.max(object_sequence_baseline);
                                    if let Some(consumer) = opus_consumer.as_mut() {
                                        consumer.rebase_before_media(object_sequence_baseline)?;
                                    }
                                }
                            }
                        }
                    }
                    // A part the edge still lacks after the abandon time is not
                    // requested again. It stays missing in the report.
                    let given_up = abandon_after_ns.is_some_and(|abandon_ns| {
                        complete_ns(requested_sequence, object_sequence_baseline)
                            .saturating_add(abandon_ns)
                            <= arrival_or_now_ns
                    });
                    if requested_sequence >= object_sequence_baseline && !given_up {
                        retry_sequence = Some(requested_sequence);
                    }
                }
                status => bail!("LL-HLS tail returned HTTP {status}"),
            }

            let sequence_to_schedule = if abandon_after_ns.is_some() {
                // The top-up below requests the next parts, bounded by the
                // pipeline depth.
                retry_sequence
            } else {
                retry_sequence.or_else(|| {
                    if part_coverage.received_parts < expected_parts {
                        let sequence = next_sequence;
                        next_sequence = next_sequence.saturating_add(1);
                        Some(sequence)
                    } else {
                        None
                    }
                })
            };
            if let Some(requested_sequence) = sequence_to_schedule {
                let path = hls_path(
                    path_prefix,
                    stream_id,
                    &format!(
                        "part{requested_sequence}.{}",
                        expected_audio_codec.media_extension()
                    ),
                );
                let sender = h3_client
                    .as_ref()
                    .context("HTTP/3 media pipeline omitted its connection")?
                    .request_sender();
                pending.insert(requested_sequence);
                in_flight.push(async move { (requested_sequence, sender.get(path).await) }.boxed());
            }
            if abandon_after_ns.is_some() {
                top_up_part_requests!();
            }
        }
    }

    if !playlist_has_ll_hls_tags {
        let playlist = timeout(
            HLS_CONTROL_REQUEST_TIMEOUT,
            hls_https_get(
                &mut h3_client,
                edge,
                server_name,
                tls_ca,
                &hls_path(path_prefix, stream_id, "stream.m3u8"),
            ),
        )
        .await
        .context("LL-HLS playlist verification exceeded two seconds")??;
        wire_bytes = wire_bytes.saturating_add(playlist.wire_bytes as u64);
        if playlist.status == 200 {
            playlist_has_ll_hls_tags =
                playlist_matches_format(&playlist.body, expected_audio_codec);
        }
    }

    let non_contiguous_pts = media_timeline.discontinuities();

    // Known-duration audio closes on the access unit that reaches the target,
    // so a duration aligned to the part boundary has no open tail part.
    if let Some(client) = h3_client.as_ref() {
        wire_bytes = client.wire_bytes();
    }
    let connection_reconnects = h3_client
        .as_ref()
        .map_or(0, ActiveH3Client::connection_reconnects);
    let opus_consumer = opus_consumer
        .map(SoundKitOpusConsumer::finish)
        .transpose()?;
    let latency_time_series =
        latency_time_series.finish(session_id, start_offset_ms, end_offset_ms, part_ms);
    Ok(HlsReceiveReport {
        schema: "needletail.aep1-48k-probe.hls-receive.v6",
        lane: "ll_hls",
        transport: transport.as_str(),
        tls_protocol: "TLSv1.3",
        tls_certificate_verified: true,
        persistent_connection: matches!(transport, HlsTransport::H3),
        connection_setup_ms,
        connection_reconnects,
        path_prefix: path_prefix.to_owned(),
        stream_id,
        session_id,
        sample_rate: SAMPLE_RATE,
        duration_seconds,
        part_ms,
        response_ms,
        parts_per_response,
        media_responses,
        expected_parts,
        received_parts: part_coverage.received_parts,
        missing_parts: expected_parts.saturating_sub(part_coverage.received_parts),
        first_pts_ms: part_coverage.first_pts_ms(),
        last_pts_ms: part_coverage.last_pts_ms(),
        non_contiguous_pts,
        deadline_ms,
        deadline_misses,
        render_buffer_ms,
        start_offset_ms,
        end_offset_ms,
        wire_bytes,
        init_has_flac: init_audio_codec == Some(HlsAudioCodec::Flac),
        expected_audio_codec: expected_audio_codec.as_str(),
        init_audio_codec: init_audio_codec.map(HlsAudioCodec::as_str),
        init_audio_codec_verified,
        expected_pcm_channels,
        soundkit_opus_protection: soundkit_opus_protection.as_str(),
        pcm_media_parts_verified,
        pcm_media_size_mismatches,
        opaque_flac_media_parts_verified,
        opaque_flac_frame_mismatches,
        opus_media_parts_verified,
        opus_media_packet_mismatches,
        fmp4_video_parts_verified,
        fmp4_video_part_mismatches,
        opus_frames: opus_frames.report(),
        opus_consumer,
        playlist_has_ll_hls_tags,
        publication_to_cache_latency_ms: publication_to_cache_latencies_ns.summary(),
        cache_to_client_latency_ms: cache_to_client_latencies_ns.summary(),
        availability_latency_ms: availability_latencies_ns.summary(),
        first_part_to_response_latency_ms: first_part_to_response_latencies_ns.summary(),
        final_part_to_response_latency_ms: final_part_to_response_latencies_ns.summary(),
        estimated_render_latency_ms: render_latencies_ns.summary(),
        latency_time_series,
    })
}

struct HlsBundleStreamState {
    stream_id: u64,
    coverage: PartCoverage,
    media_responses: u64,
    deadline_misses: u64,
    opus_media_parts_verified: u64,
    opus_media_packet_mismatches: u64,
    playlist_has_ll_hls_tags: bool,
    publication_to_cache_latencies_ns: BoundedLatencySamples,
    cache_to_client_latencies_ns: BoundedLatencySamples,
    availability_latencies_ns: BoundedLatencySamples,
    first_part_to_response_latencies_ns: BoundedLatencySamples,
    final_part_to_response_latencies_ns: BoundedLatencySamples,
    render_latencies_ns: BoundedLatencySamples,
    latency_time_series: HlsLatencyTimeSeries,
}

impl HlsBundleStreamState {
    fn new(stream_id: u64, start_sequence: u64, expected_parts: u64, part_ms: u64) -> Result<Self> {
        Ok(Self {
            stream_id,
            coverage: PartCoverage::new(start_sequence, expected_parts, part_ms)?,
            media_responses: 0,
            deadline_misses: 0,
            opus_media_parts_verified: 0,
            opus_media_packet_mismatches: 0,
            playlist_has_ll_hls_tags: false,
            publication_to_cache_latencies_ns: BoundedLatencySamples::new(),
            cache_to_client_latencies_ns: BoundedLatencySamples::new(),
            availability_latencies_ns: BoundedLatencySamples::new(),
            first_part_to_response_latencies_ns: BoundedLatencySamples::new(),
            final_part_to_response_latencies_ns: BoundedLatencySamples::new(),
            render_latencies_ns: BoundedLatencySamples::new(),
            latency_time_series: HlsLatencyTimeSeries::default(),
        })
    }

    fn finish(
        self,
        reader: &HlsLoadOptions,
        start_offset_ms: u64,
        end_offset_ms: u64,
    ) -> HlsReceiveReport {
        let expected_parts = self.coverage.expected_parts;
        let received_parts = self.coverage.received_parts;
        let first_pts_ms = self.coverage.first_pts_ms();
        let last_pts_ms = self.coverage.last_pts_ms();
        let non_contiguous_pts = self.coverage.non_contiguous_pts();
        let latency_time_series = self.latency_time_series.finish(
            reader.session_id,
            start_offset_ms,
            end_offset_ms,
            reader.part_ms,
        );
        HlsReceiveReport {
            schema: "needletail.aep1-48k-probe.hls-receive.v6",
            lane: "ll_hls",
            transport: reader.transport.as_str(),
            tls_protocol: "TLSv1.3",
            tls_certificate_verified: true,
            persistent_connection: true,
            connection_setup_ms: None,
            connection_reconnects: 0,
            path_prefix: reader.path_prefix.clone(),
            stream_id: self.stream_id,
            session_id: reader.session_id,
            sample_rate: SAMPLE_RATE,
            duration_seconds: reader.duration_seconds,
            part_ms: reader.part_ms,
            response_ms: reader.response_ms,
            parts_per_response: reader.response_ms / reader.part_ms,
            media_responses: self.media_responses,
            expected_parts,
            received_parts,
            missing_parts: expected_parts.saturating_sub(received_parts),
            first_pts_ms,
            last_pts_ms,
            non_contiguous_pts,
            deadline_ms: reader.deadline_ms,
            deadline_misses: self.deadline_misses,
            render_buffer_ms: 0,
            start_offset_ms,
            end_offset_ms,
            wire_bytes: 0,
            init_has_flac: false,
            expected_audio_codec: reader.expected_audio_codec.as_str(),
            init_audio_codec: None,
            init_audio_codec_verified: self.opus_media_parts_verified > 0
                && self.opus_media_packet_mismatches == 0,
            expected_pcm_channels: reader.expected_pcm_channels,
            soundkit_opus_protection: reader.soundkit_opus_protection.as_str(),
            pcm_media_parts_verified: 0,
            pcm_media_size_mismatches: 0,
            opaque_flac_media_parts_verified: 0,
            opaque_flac_frame_mismatches: 0,
            opus_media_parts_verified: self.opus_media_parts_verified,
            opus_media_packet_mismatches: self.opus_media_packet_mismatches,
            fmp4_video_parts_verified: 0,
            fmp4_video_part_mismatches: 0,
            opus_frames: OpusFrameReport::default(),
            opus_consumer: None,
            playlist_has_ll_hls_tags: self.playlist_has_ll_hls_tags,
            publication_to_cache_latency_ms: self.publication_to_cache_latencies_ns.summary(),
            cache_to_client_latency_ms: self.cache_to_client_latencies_ns.summary(),
            availability_latency_ms: self.availability_latencies_ns.summary(),
            first_part_to_response_latency_ms: self.first_part_to_response_latencies_ns.summary(),
            final_part_to_response_latency_ms: self.final_part_to_response_latencies_ns.summary(),
            estimated_render_latency_ms: self.render_latencies_ns.summary(),
            latency_time_series,
        }
    }
}

struct HlsBundleCustomerResult {
    states: Vec<HlsBundleStreamState>,
    end_offset_ms: u64,
    connection_setup_ms: f64,
    wire_bytes: u64,
    media_responses: u64,
    late_bundle_observations: Vec<HlsLateBundleObservation>,
}

async fn receive_hls_bundle_customer(
    customer_id: usize,
    reader: &HlsLoadOptions,
    start_offset_ms: u64,
) -> Result<HlsBundleCustomerResult> {
    let parts_per_response = reader.response_ms / reader.part_ms;
    let end_offset_ms = hls_window_end_ms(
        reader.duration_seconds,
        start_offset_ms,
        reader.window_seconds,
        reader.part_ms,
    )?;
    let initial_from_sequence = start_offset_ms / reader.part_ms;
    let expected_parts = end_offset_ms.saturating_sub(start_offset_ms) / reader.part_ms;
    if !expected_parts.is_multiple_of(parts_per_response) {
        bail!("qualification window must contain a whole number of bundled responses");
    }
    let stop_ns = reader
        .session_id
        .saturating_add(end_offset_ms.saturating_mul(1_000_000))
        .saturating_add(reader.tail_seconds.saturating_mul(1_000_000_000));
    let stream_ids = (0..reader.stream_count)
        .map(|offset| {
            reader
                .stream_id
                .checked_add(offset as u64)
                .context("stream id range overflow")
        })
        .collect::<Result<Vec<_>>>()?;
    let stream_query = stream_ids
        .iter()
        .map(u64::to_string)
        .collect::<Vec<_>>()
        .join(",");
    let mut states = stream_ids
        .iter()
        .copied()
        .map(|stream_id| {
            HlsBundleStreamState::new(
                stream_id,
                initial_from_sequence,
                expected_parts,
                reader.part_ms,
            )
        })
        .collect::<Result<Vec<_>>>()?;

    let preload_at_ns = h3_preload_at_ns(reader.session_id, start_offset_ms);
    let now_ns = now_unix_ns()?;
    if now_ns < preload_at_ns {
        sleep_until(TokioInstant::now() + Duration::from_nanos(preload_at_ns - now_ns)).await;
    }
    let owner =
        H3HttpsClient::connect(reader.edge, &reader.server_name, reader.tls_ca.as_deref()).await?;
    let connection_setup_ms = owner.connection_setup_ms;

    // Verify metadata before this customer's timed media window. Running these
    // requests after one customer finishes overlaps report work with later
    // customers and can create false delivery-tail measurements.
    let playlist_requests = states.iter().map(|state| {
        let path = hls_path(&reader.path_prefix, state.stream_id, "stream.m3u8");
        owner.request_sender().get(path)
    });
    let playlists = join_all(playlist_requests).await;
    for (state, playlist) in states.iter_mut().zip(playlists) {
        let playlist = playlist?;
        if playlist.status != 200 {
            bail!(
                "LL-HLS playlist for stream {} returned HTTP {}",
                state.stream_id,
                playlist.status
            );
        }
        if !playlist_matches_format(&playlist.body, reader.expected_audio_codec) {
            bail!(
                "LL-HLS playlist for stream {} did not advertise the expected format",
                state.stream_id
            );
        }
        state.playlist_has_ll_hls_tags = true;
    }

    let final_sequence = end_offset_ms.saturating_div(reader.part_ms);
    let path = format!(
        "{}/tail-bundle-stream?streams={stream_query}&from={initial_from_sequence}&parts={parts_per_response}",
        reader.path_prefix
    );
    let mut response_stream = owner.request_sender().open_stream(path).await?;
    if response_stream.status != 200 {
        bail!(
            "LL-HLS persistent tail bundle returned HTTP {}",
            response_stream.status
        );
    }
    if response_stream
        .headers
        .get("content-type")
        .is_none_or(|content_type| content_type != TAIL_BUNDLE_STREAM_CONTENT_TYPE)
    {
        bail!("LL-HLS persistent tail bundle returned an invalid content type");
    }

    let mut media_responses = 0_u64;
    let mut late_bundle_observations = Vec::new();
    let mut requested_sequence = initial_from_sequence;
    while now_unix_ns()? < stop_ns
        && states
            .first()
            .is_some_and(|state| state.coverage.received_parts < expected_parts)
        && requested_sequence < final_sequence
    {
        let remaining = Duration::from_nanos(stop_ns.saturating_sub(now_unix_ns()?));
        let Some((encoded_bundle, arrival_ns)) = timeout(remaining, response_stream.next_frame())
            .await
            .context("persistent tail bundle exceeded its media deadline")??
        else {
            break;
        };
        let entries = decode_tail_bundle(encoded_bundle)?;
        let expected_after = (requested_sequence != initial_from_sequence)
            .then(|| requested_sequence.saturating_sub(1));
        validate_tail_bundle_entries(
            &stream_ids,
            &entries,
            expected_after,
            initial_from_sequence,
            parts_per_response,
        )?;
        media_responses = media_responses.saturating_add(1);
        let final_capture_ns = entries.first().map(|entry| {
            reader.session_id.saturating_add(
                entry
                    .end_sequence
                    .saturating_mul(reader.part_ms)
                    .saturating_mul(1_000_000),
            )
        });
        if let Some(final_capture_ns) = final_capture_ns {
            let availability_latency_ns = arrival_ns.saturating_sub(final_capture_ns);
            let deadline_ns = reader.deadline_ms.saturating_mul(1_000_000);
            if availability_latency_ns > deadline_ns
                && late_bundle_observations.len() < MAX_LATE_BUNDLE_OBSERVATIONS
            {
                let available_ns = entries
                    .iter()
                    .filter_map(|entry| entry.available_unix_us)
                    .max()
                    .and_then(|available_us| available_us.checked_mul(1_000));
                late_bundle_observations.push(HlsLateBundleObservation {
                    customer_id,
                    start_sequence: entries
                        .first()
                        .map_or(requested_sequence, |entry| entry.start_sequence),
                    end_sequence: entries
                        .first()
                        .map_or(requested_sequence, |entry| entry.end_sequence),
                    capture_unix_ns: final_capture_ns,
                    arrival_unix_ns: arrival_ns,
                    availability_latency_ms: availability_latency_ns as f64 / 1_000_000.0,
                    lateness_ms: availability_latency_ns.saturating_sub(deadline_ns) as f64
                        / 1_000_000.0,
                    publication_to_cache_ms: available_ns
                        .filter(|&available_ns| available_ns >= final_capture_ns)
                        .map(|available_ns| (available_ns - final_capture_ns) as f64 / 1_000_000.0),
                    cache_to_client_ms: available_ns
                        .filter(|&available_ns| arrival_ns >= available_ns)
                        .map(|available_ns| (arrival_ns - available_ns) as f64 / 1_000_000.0),
                });
            }
        }
        for (state, entry) in states.iter_mut().zip(&entries) {
            state.media_responses = state.media_responses.saturating_add(1);
            let parts = if parts_per_response == 1 {
                vec![entry.payload.as_ref()]
            } else {
                split_soundkit_opus_parts(&entry.payload, reader.part_ms)?
            };
            if parts.len() as u64 != parts_per_response {
                bail!(
                            "stream {} bundle entry contained {} media units, expected {parts_per_response}",
                            state.stream_id,
                            parts.len()
                        );
            }
            let first_capture_ns = reader.session_id.saturating_add(
                entry
                    .start_sequence
                    .saturating_mul(reader.part_ms)
                    .saturating_mul(1_000_000),
            );
            let final_capture_ns = reader.session_id.saturating_add(
                entry
                    .end_sequence
                    .saturating_mul(reader.part_ms)
                    .saturating_mul(1_000_000),
            );
            if arrival_ns >= first_capture_ns {
                state
                    .first_part_to_response_latencies_ns
                    .record(arrival_ns - first_capture_ns);
            }
            if arrival_ns >= final_capture_ns {
                state
                    .final_part_to_response_latencies_ns
                    .record(arrival_ns - final_capture_ns);
            }
            for (offset, part) in parts.into_iter().enumerate() {
                let sequence = entry.start_sequence.saturating_add(offset as u64);
                let pts_ms = sequence.saturating_mul(reader.part_ms);
                if !state.coverage.insert_pts(pts_ms) {
                    bail!(
                        "stream {} returned duplicate or invalid PTS {pts_ms} ms",
                        state.stream_id
                    );
                }
                let capture_ns = reader
                    .session_id
                    .saturating_add(pts_ms.saturating_mul(1_000_000));
                let latency_ns = arrival_ns.saturating_sub(capture_ns);
                let deadline_missed = latency_ns > reader.deadline_ms.saturating_mul(1_000_000);
                if deadline_missed {
                    state.deadline_misses = state.deadline_misses.saturating_add(1);
                }
                state.availability_latencies_ns.record(latency_ns);
                state.render_latencies_ns.record(latency_ns);
                state.latency_time_series.record_part(
                    pts_ms,
                    latency_ns,
                    latency_ns,
                    deadline_missed,
                );
                if soundkit_opus_part_is_valid(
                    part,
                    reader.part_ms,
                    reader.soundkit_opus_protection,
                ) {
                    state.opus_media_parts_verified =
                        state.opus_media_parts_verified.saturating_add(1);
                } else {
                    state.opus_media_packet_mismatches =
                        state.opus_media_packet_mismatches.saturating_add(1);
                }
            }
            if let Some(available_ns) = entry
                .available_unix_us
                .and_then(|available_us| available_us.checked_mul(1_000))
            {
                if available_ns >= final_capture_ns && arrival_ns >= available_ns {
                    state
                        .publication_to_cache_latencies_ns
                        .record(available_ns - final_capture_ns);
                    state
                        .cache_to_client_latencies_ns
                        .record(arrival_ns - available_ns);
                    state.latency_time_series.record_cache(
                        entry.end_sequence.saturating_mul(reader.part_ms),
                        available_ns - final_capture_ns,
                        arrival_ns - available_ns,
                    );
                }
            }
        }
        requested_sequence = requested_sequence.saturating_add(parts_per_response);
    }

    let wire_bytes = owner.wire_bytes();
    Ok(HlsBundleCustomerResult {
        states,
        end_offset_ms,
        connection_setup_ms,
        wire_bytes,
        media_responses,
        late_bundle_observations,
    })
}

fn validate_tail_bundle_entries(
    stream_ids: &[u64],
    entries: &[TailBundleEntry],
    after_sequence: Option<u64>,
    initial_from_sequence: u64,
    parts_per_response: u64,
) -> Result<()> {
    if entries.len() != stream_ids.len() {
        bail!(
            "LL-HLS tail bundle returned {} streams, expected {}",
            entries.len(),
            stream_ids.len()
        );
    }
    let expected_start = after_sequence
        .and_then(|sequence| sequence.checked_add(1))
        .unwrap_or(initial_from_sequence);
    let expected_end = expected_start
        .checked_add(parts_per_response.saturating_sub(1))
        .context("LL-HLS tail bundle sequence range overflow")?;
    for (expected_stream_id, entry) in stream_ids.iter().zip(entries) {
        if entry.stream_id != *expected_stream_id {
            bail!(
                "LL-HLS tail bundle returned stream {}, expected {}",
                entry.stream_id,
                expected_stream_id
            );
        }
        if entry.start_sequence != expected_start || entry.end_sequence != expected_end {
            bail!(
                "stream {} bundle range {}..={} did not match expected {expected_start}..={expected_end}",
                entry.stream_id,
                entry.start_sequence,
                entry.end_sequence
            );
        }
    }
    Ok(())
}

struct HlsDeferredBundleReports {
    states: Vec<HlsBundleStreamState>,
    reader: HlsLoadOptions,
    start_offset_ms: u64,
    end_offset_ms: u64,
}

struct HlsCustomerLoadResult {
    customer_id: usize,
    streams: Vec<(u64, Result<HlsReceiveReport>)>,
    deferred_bundle_reports: Option<HlsDeferredBundleReports>,
    shared_connection_setup_ms: Option<f64>,
    shared_wire_bytes: u64,
    media_responses: u64,
    late_bundle_observations: Vec<HlsLateBundleObservation>,
}

async fn receive_hls_load_customer(
    customer_id: usize,
    reader: HlsLoadOptions,
    per_reader_timeout: Duration,
) -> HlsCustomerLoadResult {
    let reader_start_offset_ms = randomized_reader_start_offset_ms(
        reader.start_offset_ms,
        reader.part_ms,
        reader.arrival_window_ms,
        customer_id,
        reader.arrival_seed,
    );
    if reader.bundle_streams {
        let result = timeout(
            per_reader_timeout,
            receive_hls_bundle_customer(customer_id, &reader, reader_start_offset_ms),
        )
        .await
        .context("bundled reader exceeded its overall deadline")
        .and_then(|result| result);
        return match result {
            Ok(bundle) => {
                let HlsBundleCustomerResult {
                    states,
                    end_offset_ms,
                    connection_setup_ms,
                    wire_bytes,
                    media_responses,
                    late_bundle_observations,
                } = bundle;
                HlsCustomerLoadResult {
                    customer_id,
                    streams: Vec::new(),
                    deferred_bundle_reports: Some(HlsDeferredBundleReports {
                        states,
                        reader,
                        start_offset_ms: reader_start_offset_ms,
                        end_offset_ms,
                    }),
                    shared_connection_setup_ms: Some(connection_setup_ms),
                    shared_wire_bytes: wire_bytes,
                    media_responses,
                    late_bundle_observations,
                }
            }
            Err(error) => {
                let message = format!("bundled H3 reader failed: {error:#}");
                HlsCustomerLoadResult {
                    customer_id,
                    streams: (0..reader.stream_count)
                        .map(|offset| {
                            (
                                reader.stream_id.saturating_add(offset as u64),
                                Err(anyhow::anyhow!(message.clone())),
                            )
                        })
                        .collect(),
                    deferred_bundle_reports: None,
                    shared_connection_setup_ms: None,
                    shared_wire_bytes: 0,
                    media_responses: 0,
                    late_bundle_observations: Vec::new(),
                }
            }
        };
    }
    if reader.stream_count == 1 {
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
                response_ms: reader.response_ms,
                deadline_ms: reader.deadline_ms,
                render_buffer_ms: 0,
                start_offset_ms: reader_start_offset_ms,
                window_seconds: reader.window_seconds,
                tail_seconds: reader.tail_seconds,
                expected_audio_codec: reader.expected_audio_codec,
                expected_pcm_channels: reader.expected_pcm_channels,
                soundkit_opus_protection: reader.soundkit_opus_protection,
                object_sequence_origin: reader.object_sequence_origin,
                abandon_after_ms: reader.abandon_after_ms,
                soundkit_key_file: None,
                decode_soundkit_opus: false,
                expected_opus_frame_samples: None,
                decoded_pcm_output: None,
                raw_part_output_dir: None,
                reference_pcm_s16le: None,
                reference_leading_silence_frames: 0,
                shared_h3: None,
            }),
        )
        .await
        .context("reader exceeded its overall deadline")
        .and_then(|result| result);
        let media_responses = result
            .as_ref()
            .map(|report| report.media_responses)
            .unwrap_or(0);
        return HlsCustomerLoadResult {
            customer_id,
            streams: vec![(reader.stream_id, result)],
            deferred_bundle_reports: None,
            shared_connection_setup_ms: None,
            shared_wire_bytes: 0,
            media_responses,
            late_bundle_observations: Vec::new(),
        };
    }

    let preload_at_ns = h3_preload_at_ns(reader.session_id, reader_start_offset_ms);
    if let Ok(now_ns) = now_unix_ns() {
        if now_ns < preload_at_ns {
            sleep_until(TokioInstant::now() + Duration::from_nanos(preload_at_ns - now_ns)).await;
        }
    }
    let owner =
        match H3HttpsClient::connect(reader.edge, &reader.server_name, reader.tls_ca.as_deref())
            .await
        {
            Ok(client) => client,
            Err(error) => {
                let message = format!("shared H3 connection failed: {error:#}");
                let streams = (0..reader.stream_count)
                    .map(|offset| {
                        (
                            reader.stream_id.saturating_add(offset as u64),
                            Err(anyhow::anyhow!(message.clone())),
                        )
                    })
                    .collect();
                return HlsCustomerLoadResult {
                    customer_id,
                    streams,
                    deferred_bundle_reports: None,
                    shared_connection_setup_ms: None,
                    shared_wire_bytes: 0,
                    media_responses: 0,
                    late_bundle_observations: Vec::new(),
                };
            }
        };
    let connection_setup_ms = owner.connection_setup_ms;
    let shared = owner.shared();
    let mut stream_tasks = tokio::task::JoinSet::new();
    for offset in 0..reader.stream_count {
        let stream_reader = reader.clone();
        let stream_h3 = shared.clone();
        let stream_id = reader.stream_id.saturating_add(offset as u64);
        stream_tasks.spawn(async move {
            let result = timeout(
                per_reader_timeout,
                receive_hls(HlsReceiveOptions {
                    edge: stream_reader.edge,
                    server_name: &stream_reader.server_name,
                    tls_ca: stream_reader.tls_ca.as_deref(),
                    transport: stream_reader.transport,
                    path_prefix: &stream_reader.path_prefix,
                    stream_id,
                    session_id: stream_reader.session_id,
                    duration_seconds: stream_reader.duration_seconds,
                    part_ms: stream_reader.part_ms,
                    response_ms: stream_reader.response_ms,
                    deadline_ms: stream_reader.deadline_ms,
                    render_buffer_ms: 0,
                    start_offset_ms: reader_start_offset_ms,
                    window_seconds: stream_reader.window_seconds,
                    tail_seconds: stream_reader.tail_seconds,
                    expected_audio_codec: stream_reader.expected_audio_codec,
                    expected_pcm_channels: stream_reader.expected_pcm_channels,
                    soundkit_opus_protection: stream_reader.soundkit_opus_protection,
                    object_sequence_origin: stream_reader.object_sequence_origin,
                    abandon_after_ms: stream_reader.abandon_after_ms,
                    soundkit_key_file: None,
                    decode_soundkit_opus: false,
                    expected_opus_frame_samples: None,
                    decoded_pcm_output: None,
                    raw_part_output_dir: None,
                    reference_pcm_s16le: None,
                    reference_leading_silence_frames: 0,
                    shared_h3: Some(stream_h3),
                }),
            )
            .await
            .context("reader stream exceeded its overall deadline")
            .and_then(|result| result);
            (stream_id, result)
        });
    }
    let mut streams = Vec::with_capacity(reader.stream_count);
    while let Some(outcome) = stream_tasks.join_next().await {
        match outcome {
            Ok(stream) => streams.push(stream),
            Err(error) => streams.push((
                0,
                Err(anyhow::anyhow!("reader stream task failed: {error}")),
            )),
        }
    }
    let shared_wire_bytes = owner.wire_bytes();
    let media_responses = streams
        .iter()
        .filter_map(|(_stream_id, result)| result.as_ref().ok())
        .map(|report| report.media_responses)
        .sum();
    HlsCustomerLoadResult {
        customer_id,
        streams,
        deferred_bundle_reports: None,
        shared_connection_setup_ms: Some(connection_setup_ms),
        shared_wire_bytes,
        media_responses,
        late_bundle_observations: Vec::new(),
    }
}

async fn load_hls(options: HlsLoadOptions) -> Result<HlsLoadReport> {
    if options.readers == 0 || options.readers > 4_096 {
        bail!("--readers must be between 1 and 4096");
    }
    if options.stream_count == 0 || options.stream_count > 128 {
        bail!("--stream-count must be between 1 and 128");
    }
    if options.bundle_streams && options.stream_count < 2 {
        bail!("--bundle-streams requires at least two streams");
    }
    if options.bundle_streams && options.stream_count > MAX_TAIL_BUNDLE_ENTRIES {
        bail!("--bundle-streams exceeds the tail-bundle stream limit");
    }
    if options.bundle_streams && !matches!(options.transport, HlsTransport::H3) {
        bail!("--bundle-streams requires HTTP/3");
    }
    if options.bundle_streams && options.expected_audio_codec != HlsAudioCodec::SoundkitOpus {
        bail!("--bundle-streams qualification currently requires SoundKit Opus");
    }
    if options.bundle_streams
        && (options.path_prefix.is_empty()
            || !options.path_prefix.starts_with('/')
            || options.path_prefix.ends_with('/'))
    {
        bail!("--bundle-streams requires an edge path prefix with no trailing slash");
    }
    if options.stream_count > 1 && !matches!(options.transport, HlsTransport::H3) {
        bail!("--stream-count greater than one requires HTTP/3 connection multiplexing");
    }
    let final_stream_offset =
        u64::try_from(options.stream_count - 1).context("--stream-count exceeds this platform")?;
    options
        .stream_id
        .checked_add(final_stream_offset)
        .context("stream id range overflow")?;
    if options.part_ms == 0 {
        bail!("--part-ms must be positive");
    }
    if options.response_ms < options.part_ms || !options.response_ms.is_multiple_of(options.part_ms)
    {
        bail!("--response-ms must be a positive multiple of --part-ms");
    }
    if options.response_ms / options.part_ms > 200 {
        bail!("--response-ms may contain at most 200 media parts");
    }
    if options.arrival_window_ms > 0 && options.window_seconds.is_none() {
        bail!("--arrival-window-ms requires --window-seconds so every reader requests the same amount of media");
    }
    let maximum_start_offset_ms = options.start_offset_ms.saturating_add(
        options
            .arrival_window_ms
            .saturating_div(options.part_ms)
            .saturating_mul(options.part_ms),
    );
    let end_offset_ms = hls_window_end_ms(
        options.duration_seconds,
        maximum_start_offset_ms,
        options.window_seconds,
        options.part_ms,
    )?;

    let started = Instant::now();
    let per_reader_timeout =
        receive_command_timeout_ms(options.session_id, end_offset_ms, options.tail_seconds)?;
    let mut tasks = tokio::task::JoinSet::new();
    for reader_id in 0..options.readers {
        let reader = options.clone();
        tasks.spawn(receive_hls_load_customer(
            reader_id,
            reader,
            per_reader_timeout,
        ));
    }

    let requested_track_readers = options
        .readers
        .checked_mul(options.stream_count)
        .context("requested customer/stream product exceeds this platform")?;
    let mut reports = Vec::with_capacity(requested_track_readers);
    let mut errors = Vec::new();
    let mut shared_connection_setup_ms = Vec::new();
    let mut shared_wire_bytes_total = 0_u64;
    let mut media_responses_total = 0_u64;
    let mut late_bundle_observations = Vec::new();
    let mut deferred_bundle_reports = Vec::new();
    while let Some(outcome) = tasks.join_next().await {
        match outcome {
            Ok(customer) => {
                let customer_id = customer.customer_id;
                if let Some(setup_ms) = customer.shared_connection_setup_ms {
                    shared_connection_setup_ms.push(setup_ms);
                }
                shared_wire_bytes_total =
                    shared_wire_bytes_total.saturating_add(customer.shared_wire_bytes);
                media_responses_total =
                    media_responses_total.saturating_add(customer.media_responses);
                let remaining_observations =
                    MAX_LATE_BUNDLE_OBSERVATIONS.saturating_sub(late_bundle_observations.len());
                late_bundle_observations.extend(
                    customer
                        .late_bundle_observations
                        .into_iter()
                        .take(remaining_observations),
                );
                if let Some(deferred) = customer.deferred_bundle_reports {
                    deferred_bundle_reports.push((customer_id, deferred));
                }
                for (stream_id, result) in customer.streams {
                    match result {
                        Ok(report) => {
                            if (report.missing_parts > 0
                                || report.non_contiguous_pts > 0
                                || report.deadline_misses > 0)
                                && errors.len() < 20
                            {
                                errors.push(format!(
                                    "customer {} stream {stream_id}: {} missing parts, {} non-contiguous PTS, {} deadline misses, {} media responses",
                                    customer_id,
                                    report.missing_parts,
                                    report.non_contiguous_pts,
                                    report.deadline_misses,
                                    report.media_responses
                                ));
                            }
                            reports.push(report);
                        }
                        Err(error) => {
                            if errors.len() < 20 {
                                errors.push(format!(
                                    "customer {} stream {stream_id}: {error}",
                                    customer_id
                                ));
                            }
                        }
                    }
                }
            }
            Err(error) => {
                if errors.len() < 20 {
                    errors.push(format!("reader task failed: {error}"));
                }
            }
        }
    }
    // Finalize bounded percentile reservoirs only after every timed receive
    // task has stopped. Sorting them in each finishing customer task can starve
    // later H3 clients during a staggered completion window.
    for (customer_id, deferred) in deferred_bundle_reports {
        for state in deferred.states {
            let stream_id = state.stream_id;
            let report = state.finish(
                &deferred.reader,
                deferred.start_offset_ms,
                deferred.end_offset_ms,
            );
            if (report.missing_parts > 0
                || report.non_contiguous_pts > 0
                || report.deadline_misses > 0)
                && errors.len() < 20
            {
                errors.push(format!(
                    "customer {customer_id} stream {stream_id}: {} missing parts, {} non-contiguous PTS, {} deadline misses, {} media responses",
                    report.missing_parts,
                    report.non_contiguous_pts,
                    report.deadline_misses,
                    report.media_responses
                ));
            }
            reports.push(report);
        }
    }
    late_bundle_observations.sort_unstable_by_key(|observation| observation.arrival_unix_ns);

    let readers_completed = reports.len();
    let readers_failed = requested_track_readers.saturating_sub(readers_completed);
    let readers_with_incomplete_media = reports
        .iter()
        .filter(|report| report.missing_parts > 0 || report.non_contiguous_pts > 0)
        .count();
    let expected_parts_per_reader = options.window_seconds.map_or_else(
        || end_offset_ms.saturating_sub(options.start_offset_ms) / options.part_ms,
        |window_seconds| window_seconds.saturating_mul(1_000) / options.part_ms,
    );
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
    let opaque_flac_frame_mismatches_total = reports
        .iter()
        .map(|report| report.opaque_flac_frame_mismatches)
        .sum::<u64>();
    let opus_media_packet_mismatches_total = reports
        .iter()
        .map(|report| report.opus_media_packet_mismatches)
        .sum::<u64>();
    let fmp4_video_part_mismatches_total = reports
        .iter()
        .map(|report| report.fmp4_video_part_mismatches)
        .sum::<u64>();
    let playlist_verified_readers = reports
        .iter()
        .filter(|report| report.playlist_has_ll_hls_tags)
        .count();
    let wire_bytes_total = reports
        .iter()
        .map(|report| report.wire_bytes)
        .sum::<u64>()
        .saturating_add(shared_wire_bytes_total);
    let connection_setup_samples = if shared_connection_setup_ms.is_empty() {
        reports
            .iter()
            .filter_map(|report| report.connection_setup_ms)
            .map(ms_f64_to_ns)
            .collect()
    } else {
        shared_connection_setup_ms
            .into_iter()
            .map(ms_f64_to_ns)
            .collect()
    };
    let connection_setup_ms = percentiles_ms(connection_setup_samples);
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
    let first_part_to_response_p99_ms_across_readers = percentiles_ms(
        reports
            .iter()
            .filter(|report| report.first_part_to_response_latency_ms.count > 0)
            .map(|report| ms_f64_to_ns(report.first_part_to_response_latency_ms.p99))
            .collect(),
    );
    let final_part_to_response_p99_ms_across_readers = percentiles_ms(
        reports
            .iter()
            .filter(|report| report.final_part_to_response_latency_ms.count > 0)
            .map(|report| ms_f64_to_ns(report.final_part_to_response_latency_ms.p99))
            .collect(),
    );
    let passed = readers_failed == 0
        && readers_with_incomplete_media == 0
        && deadline_misses_total == 0
        && init_verified_readers == requested_track_readers
        && pcm_media_size_mismatches_total == 0
        && opaque_flac_frame_mismatches_total == 0
        && opus_media_packet_mismatches_total == 0
        && fmp4_video_part_mismatches_total == 0
        && playlist_verified_readers == requested_track_readers;

    Ok(HlsLoadReport {
        schema: "needletail.aep1-48k-probe.hls-load.v8",
        transport: options.transport.as_str(),
        tls_protocol: "TLSv1.3",
        persistent_connections: matches!(options.transport, HlsTransport::H3),
        persistent_bundle_response: options.bundle_streams,
        edge: options.edge,
        stream_id: options.stream_id,
        stream_count: options.stream_count,
        session_id: options.session_id,
        duration_seconds: options.duration_seconds,
        part_ms: options.part_ms,
        response_ms: options.response_ms,
        parts_per_response: options.response_ms / options.part_ms,
        tail_bundle: options.bundle_streams,
        tracks_per_media_response: if options.bundle_streams {
            options.stream_count
        } else {
            1
        },
        media_responses_total,
        start_offset_ms: options.start_offset_ms,
        end_offset_ms,
        deadline_ms: options.deadline_ms,
        arrival_window_ms: options.arrival_window_ms,
        arrival_seed: options.arrival_seed,
        expected_audio_codec: options.expected_audio_codec.as_str(),
        expected_pcm_channels: options.expected_pcm_channels,
        soundkit_opus_protection: options.soundkit_opus_protection.as_str(),
        customers_requested: options.readers,
        h3_connections: matches!(options.transport, HlsTransport::H3)
            .then_some(options.readers)
            .unwrap_or(0),
        readers_requested: requested_track_readers,
        readers_completed,
        readers_failed,
        readers_with_incomplete_media,
        expected_parts_per_reader,
        received_parts_total,
        missing_parts_total,
        non_contiguous_pts_total,
        deadline_misses_total,
        late_bundle_observations,
        init_verified_readers,
        pcm_media_size_mismatches_total,
        opaque_flac_frame_mismatches_total,
        opus_media_packet_mismatches_total,
        fmp4_video_part_mismatches_total,
        playlist_verified_readers,
        wire_bytes_total,
        connection_setup_ms,
        availability_p99_ms_across_readers,
        first_part_to_response_p99_ms_across_readers,
        final_part_to_response_p99_ms_across_readers,
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

struct H3ResponseStream {
    status: u16,
    headers: HashMap<String, String>,
    stream: h3::client::RequestStream<h3_quinn::BidiStream<Bytes>, Bytes>,
    buffered: BytesMut,
}

impl H3ResponseStream {
    async fn next_frame(&mut self) -> Result<Option<(Bytes, u64)>> {
        loop {
            if let Some(bundle) = decode_tail_bundle_stream_frame(&mut self.buffered)? {
                return Ok(Some((bundle, now_unix_ns()?)));
            }
            let Some(mut chunk) = self.stream.recv_data().await? else {
                if self.buffered.is_empty() {
                    return Ok(None);
                }
                bail!("persistent tail bundle ended with a truncated frame");
            };
            let remaining = chunk.remaining();
            self.buffered
                .extend_from_slice(&chunk.copy_to_bytes(remaining));
        }
    }
}

struct H3HttpsClient {
    _endpoint: h3_quinn::quinn::Endpoint,
    connection: h3_quinn::quinn::Connection,
    send_request: h3::client::SendRequest<h3_quinn::OpenStreams, Bytes>,
    _driver_task: tokio::task::JoinHandle<()>,
    authority: String,
    connection_setup_ms: f64,
    prior_wire_bytes: u64,
    reconnects: u64,
}

#[derive(Clone)]
struct H3RequestSender {
    send_request: h3::client::SendRequest<h3_quinn::OpenStreams, Bytes>,
    authority: String,
}

#[derive(Clone)]
struct SharedH3Client {
    request_sender: H3RequestSender,
    connection_setup_ms: f64,
}

enum ActiveH3Client {
    Owned(H3HttpsClient),
    Shared(SharedH3Client),
}

impl ActiveH3Client {
    async fn get(&self, path: &str) -> Result<SimpleHttpResponse> {
        match self {
            Self::Owned(client) => client.get(path).await,
            Self::Shared(client) => client.request_sender.clone().get(path.to_owned()).await,
        }
    }

    fn request_sender(&self) -> H3RequestSender {
        match self {
            Self::Owned(client) => client.request_sender(),
            Self::Shared(client) => client.request_sender.clone(),
        }
    }

    fn connection_setup_ms(&self) -> f64 {
        match self {
            Self::Owned(client) => client.connection_setup_ms,
            Self::Shared(client) => client.connection_setup_ms,
        }
    }

    fn wire_bytes(&self) -> u64 {
        match self {
            Self::Owned(client) => client.wire_bytes(),
            // The load caller records a shared connection exactly once.
            Self::Shared(_) => 0,
        }
    }

    fn connection_reconnects(&self) -> u64 {
        match self {
            Self::Owned(client) => client.reconnects,
            Self::Shared(_) => 0,
        }
    }
}

impl H3RequestSender {
    async fn open_stream(mut self, path: String) -> Result<H3ResponseStream> {
        let request = http::Request::builder()
            .method(http::Method::GET)
            .uri(format!("https://{}{path}", self.authority))
            .header(http::header::ACCEPT, TAIL_BUNDLE_STREAM_CONTENT_TYPE)
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
        Ok(H3ResponseStream {
            status,
            headers,
            stream,
            buffered: BytesMut::new(),
        })
    }

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
        let mut client_config = h3_quinn::quinn::ClientConfig::new(Arc::new(
            h3_quinn::quinn::crypto::rustls::QuicClientConfig::try_from(crypto)?,
        ));
        let mut transport_config = h3_quinn::quinn::TransportConfig::default();
        transport_config.keep_alive_interval(Some(Duration::from_secs(2)));
        client_config.transport_config(Arc::new(transport_config));
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
            prior_wire_bytes: 0,
            reconnects: 0,
        })
    }

    async fn reconnect(
        &mut self,
        edge: SocketAddr,
        server_name: &str,
        tls_ca: Option<&Path>,
    ) -> Result<()> {
        let limit = h3_request_reconnect_limit();
        if self.reconnects >= limit {
            bail!("HTTP/3 connection exceeded {limit} reconnects");
        }
        let prior_wire_bytes = self.wire_bytes();
        let reconnects = self.reconnects.saturating_add(1);
        let mut replacement = Self::connect(edge, server_name, tls_ca).await?;
        replacement.prior_wire_bytes = prior_wire_bytes;
        replacement.reconnects = reconnects;
        *self = replacement;
        Ok(())
    }

    fn request_sender(&self) -> H3RequestSender {
        H3RequestSender {
            send_request: self.send_request.clone(),
            authority: self.authority.clone(),
        }
    }

    fn shared(&self) -> SharedH3Client {
        SharedH3Client {
            request_sender: self.request_sender(),
            connection_setup_ms: self.connection_setup_ms,
        }
    }

    async fn get(&self, path: &str) -> Result<SimpleHttpResponse> {
        self.request_sender().get(path.to_owned()).await
    }

    fn wire_bytes(&self) -> u64 {
        let stats = self.connection.stats();
        self.prior_wire_bytes
            .saturating_add(stats.udp_tx.bytes)
            .saturating_add(stats.udp_rx.bytes)
    }
}

impl Drop for H3HttpsClient {
    fn drop(&mut self) {
        self.connection.close(0_u32.into(), b"probe complete");
        self._driver_task.abort();
    }
}

async fn hls_https_get(
    h3_client: &mut Option<ActiveH3Client>,
    edge: SocketAddr,
    server_name: &str,
    tls_ca: Option<&Path>,
    path: &str,
) -> Result<SimpleHttpResponse> {
    match h3_client {
        Some(client) => {
            let first_error = match client.get(path).await {
                Ok(response) => return Ok(response),
                Err(error) => error,
            };
            let ActiveH3Client::Owned(client) = client else {
                return Err(first_error);
            };
            client
                .reconnect(edge, server_name, tls_ca)
                .await
                .with_context(|| {
                    format!(
                        "HTTP/3 reconnect failed after the request for {path} failed: {first_error:#}"
                    )
                })?;
            client.get(path).await.with_context(|| {
                format!(
                    "HTTP/3 retry for {path} failed after reconnect; first error: {first_error:#}"
                )
            })
        }
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

fn media_part_pts_ms(
    codec: HlsAudioCodec,
    bytes: &[u8],
    object_sequence: u64,
    object_sequence_baseline: u64,
    baseline_pts_ms: u64,
    part_ms: u64,
) -> Result<u64> {
    if matches!(
        codec,
        HlsAudioCodec::OpaqueFlac | HlsAudioCodec::SoundkitOpus
    ) {
        return nominal_hls_object_pts_ms(
            object_sequence,
            object_sequence_baseline,
            baseline_pts_ms,
            part_ms,
        );
    }
    let decode_time = parse_fmp4_tfdt_ms(bytes).context("LL-HLS fMP4 part omitted a valid tfdt")?;
    if codec == HlsAudioCodec::H264 {
        return decode_time
            .checked_mul(1_000)
            .map(|ticks| ticks / VIDEO_TIMESCALE)
            .context("H.264 LL-HLS part PTS overflow");
    }
    if codec == HlsAudioCodec::Opus {
        return decode_time
            .checked_mul(1_000)
            .and_then(|samples| samples.checked_div(u64::from(SAMPLE_RATE)))
            .context("Opus LL-HLS part PTS overflow");
    }
    Ok(decode_time)
}

fn media_part_duration_ms(codec: HlsAudioCodec, bytes: &[u8], part_ms: u64) -> Result<u64> {
    if matches!(
        codec,
        HlsAudioCodec::OpaqueFlac | HlsAudioCodec::SoundkitOpus
    ) {
        return Ok(part_ms);
    }
    let duration = fmp4_trun_sample_geometry(bytes)
        .context("LL-HLS fMP4 part omitted valid sample durations")?
        .into_iter()
        .try_fold(0_u64, |total, (sample_duration, _)| {
            total
                .checked_add(u64::from(sample_duration))
                .context("LL-HLS fMP4 sample duration overflow")
        })?;
    if codec == HlsAudioCodec::H264 {
        return duration
            .checked_mul(1_000)
            .map(|ticks| ticks / VIDEO_TIMESCALE)
            .context("H.264 LL-HLS part duration overflow");
    }
    if codec == HlsAudioCodec::Opus {
        return duration
            .checked_mul(1_000)
            .and_then(|samples| samples.checked_div(u64::from(SAMPLE_RATE)))
            .context("Opus LL-HLS part duration overflow");
    }
    Ok(duration)
}

fn nominal_hls_object_pts_ms(
    object_sequence: u64,
    object_sequence_baseline: u64,
    baseline_pts_ms: u64,
    part_ms: u64,
) -> Result<u64> {
    object_sequence
        .checked_sub(object_sequence_baseline)
        .context("LL-HLS object preceded the playlist baseline")?
        .checked_mul(part_ms)
        .and_then(|relative_pts_ms| baseline_pts_ms.checked_add(relative_pts_ms))
        .context("LL-HLS object position overflow")
}

fn hls_playlist_object_window(bytes: &[u8]) -> Result<Option<HlsPlaylistObjectWindow>> {
    let playlist = std::str::from_utf8(bytes).context("LL-HLS playlist was not UTF-8")?;
    let mut first_sequence = None;
    let mut last_sequence = None;
    let mut preload_sequence = None;

    for line in playlist.lines() {
        let is_completed_part = line.starts_with("#EXT-X-PART:");
        let preload_attributes = line.strip_prefix("#EXT-X-PRELOAD-HINT:");
        let is_preload_part = preload_attributes.is_some_and(|attributes| {
            attributes
                .split(',')
                .any(|attribute| attribute.trim() == "TYPE=PART")
        });
        if !is_completed_part && !is_preload_part {
            continue;
        }
        let uri = hls_attribute_uri(line)
            .with_context(|| format!("LL-HLS part tag omitted its URI: {line}"))?;
        let sequence = hls_part_object_sequence(uri)
            .with_context(|| format!("LL-HLS part URI has no object sequence: {uri}"))?;
        if is_completed_part {
            first_sequence =
                Some(first_sequence.map_or(sequence, |first: u64| first.min(sequence)));
            last_sequence = Some(last_sequence.map_or(sequence, |last: u64| last.max(sequence)));
        } else if preload_sequence.replace(sequence).is_some() {
            bail!("LL-HLS playlist advertised more than one part preload hint");
        }
    }

    let Some(next_sequence) =
        preload_sequence.or_else(|| last_sequence.and_then(|sequence| sequence.checked_add(1)))
    else {
        return Ok(None);
    };
    if last_sequence.is_some_and(|last| next_sequence <= last) {
        bail!(
            "LL-HLS part preload sequence {next_sequence} did not follow completed object {}",
            last_sequence.unwrap()
        );
    }
    Ok(Some(HlsPlaylistObjectWindow {
        first_sequence,
        last_sequence,
        next_sequence,
    }))
}

fn initial_hls_object_sequence(
    window: HlsPlaylistObjectWindow,
    observed_empty_playlist: bool,
) -> u64 {
    if observed_empty_playlist {
        window.first_sequence.unwrap_or(window.next_sequence)
    } else {
        window.next_sequence
    }
}

fn refreshed_hls_object_sequence(current_sequence: u64, window: HlsPlaylistObjectWindow) -> u64 {
    match window.first_sequence {
        Some(first_sequence) if current_sequence < first_sequence => first_sequence,
        None if current_sequence < window.next_sequence => window.next_sequence,
        _ => current_sequence,
    }
}

fn hls_attribute_uri(line: &str) -> Option<&str> {
    let start = line.find("URI=\"")?.checked_add("URI=\"".len())?;
    let remainder = line.get(start..)?;
    let end = remainder.find('"')?;
    remainder.get(..end)
}

fn hls_part_object_sequence(uri: &str) -> Option<u64> {
    let path = uri.split(['?', '#']).next()?;
    let file_name = path.rsplit('/').next()?;
    let stem = file_name
        .rsplit_once('.')
        .map_or(file_name, |(stem, _)| stem);
    let sequence = stem
        .strip_prefix("part")
        .or_else(|| stem.strip_prefix('p'))?;
    if sequence.is_empty() || !sequence.bytes().all(|byte| byte.is_ascii_digit()) {
        return None;
    }
    sequence.parse().ok()
}

fn playlist_matches_format(bytes: &[u8], codec: HlsAudioCodec) -> bool {
    let has_part = bytes
        .windows(b"#EXT-X-PART:".len())
        .any(|window| window == b"#EXT-X-PART:");
    let can_block = bytes
        .windows(b"CAN-BLOCK-RELOAD=YES".len())
        .any(|window| window == b"CAN-BLOCK-RELOAD=YES");
    if !has_part || !can_block {
        return false;
    }
    if !matches!(
        codec,
        HlsAudioCodec::OpaqueFlac | HlsAudioCodec::SoundkitOpus
    ) {
        return true;
    }
    bytes.windows(b".bin".len()).any(|window| window == b".bin")
        && !bytes
            .windows(b"#EXT-X-MAP:".len())
            .any(|window| window == b"#EXT-X-MAP:")
}

fn detect_init_audio_codec(bytes: &[u8]) -> Option<HlsAudioCodec> {
    [
        // A video initialization segment may also carry an audio track.
        HlsAudioCodec::H264,
        HlsAudioCodec::Opus,
        HlsAudioCodec::Ipcm,
        HlsAudioCodec::Fpcm,
        HlsAudioCodec::Flac,
    ]
    .into_iter()
    .find(|codec| {
        let marker = codec
            .init_marker()
            .expect("fMP4 codec has an initialization marker");
        bytes.windows(marker.len()).any(|window| window == marker)
    })
}

fn pcm_init_parameters_match(bytes: &[u8], codec: HlsAudioCodec, expected_channels: u16) -> bool {
    let (sample_size, marker) = match codec {
        HlsAudioCodec::Flac | HlsAudioCodec::Opus => return true,
        HlsAudioCodec::H264 => return fmp4_first_mdhd_timescale(bytes) == Some(VIDEO_TIMESCALE),
        HlsAudioCodec::OpaqueFlac | HlsAudioCodec::SoundkitOpus => return false,
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
        HlsAudioCodec::Flac
        | HlsAudioCodec::OpaqueFlac
        | HlsAudioCodec::Opus
        | HlsAudioCodec::SoundkitOpus
        | HlsAudioCodec::H264 => return None,
        HlsAudioCodec::Ipcm => 3_u64,
        HlsAudioCodec::Fpcm => 4_u64,
    };
    u64::from(SAMPLE_RATE)
        .checked_mul(part_ms)?
        .checked_div(1_000)?
        .checked_mul(u64::from(channels))?
        .checked_mul(bytes_per_sample)
}

fn opaque_flac_part_is_valid(bytes: &[u8]) -> bool {
    bytes.len() >= 2 && bytes[0] == 0xff && bytes[1] & 0xfe == 0xf8
}

fn split_soundkit_opus_parts(bytes: &[u8], part_ms: u64) -> Result<Vec<&[u8]>> {
    let scaled_frames = u64::from(SAMPLE_RATE)
        .checked_mul(part_ms)
        .context("SoundKit response duration overflow")?;
    if part_ms == 0 || !scaled_frames.is_multiple_of(1_000) {
        bail!("SoundKit part duration does not contain a whole number of audio frames");
    }
    let expected_frames = scaled_frames / 1_000;
    let mut parts = Vec::new();
    let mut part_start = 0_usize;
    let mut offset = 0_usize;
    let mut part_frames = 0_u64;
    while offset < bytes.len() {
        let header_size = FrameHeaderV2::header_size(&bytes[offset..])
            .map_err(anyhow::Error::msg)
            .context("aggregated SoundKit body contained an invalid header")?;
        let header_end = offset
            .checked_add(header_size)
            .context("aggregated SoundKit header offset overflow")?;
        let header_bytes = bytes
            .get(offset..header_end)
            .context("aggregated SoundKit body truncated a header")?;
        let header = FrameHeaderV2::decode(&mut &header_bytes[..])
            .map_err(anyhow::Error::msg)
            .context("aggregated SoundKit body contained an invalid frame")?;
        let payload_size = usize::try_from(header.payload_size())
            .context("aggregated SoundKit payload exceeds this platform")?;
        let packet_end = header_end
            .checked_add(payload_size)
            .context("aggregated SoundKit packet offset overflow")?;
        if packet_end > bytes.len() {
            bail!("aggregated SoundKit body truncated a payload");
        }
        part_frames = part_frames.saturating_add(u64::from(header.frame_count()));
        if part_frames > expected_frames {
            bail!("aggregated SoundKit packet crossed a cached-part boundary");
        }
        offset = packet_end;
        if part_frames == expected_frames {
            parts.push(&bytes[part_start..offset]);
            part_start = offset;
            part_frames = 0;
        }
    }
    if part_frames != 0 || part_start != bytes.len() {
        bail!("aggregated SoundKit body ended with a partial cached media unit");
    }
    Ok(parts)
}

/// Validate every SoundKit v2 Opus packet of one cached media part.
///
/// The packet CRC32 is written by the publisher over the header and the
/// payload, so a match proves the bytes are the ones that were published.
/// Plain packets have no AEAD tag to protect them, so plain protection
/// requires the CRC32 on every packet.
fn soundkit_opus_part_is_valid(
    mut bytes: &[u8],
    part_ms: u64,
    protection: SoundkitOpusProtection,
) -> bool {
    if bytes.is_empty() || part_ms == 0 {
        return false;
    }
    let mut total_frames = 0_u64;
    let mut packets = 0_u64;
    while !bytes.is_empty() {
        let Ok(header_size) = FrameHeaderV2::header_size(bytes) else {
            return false;
        };
        let Ok(header) = FrameHeaderV2::decode(&mut &bytes[..header_size]) else {
            return false;
        };
        let Ok(payload_size) = usize::try_from(header.payload_size()) else {
            return false;
        };
        let Some(packet_size) = header_size.checked_add(payload_size) else {
            return false;
        };
        if packet_size > bytes.len()
            || header.encoding() != &EncodingFlag::Opus
            || header.sample_rate() != SAMPLE_RATE
            || !(1..=2).contains(&header.channels())
            || header.frame_count() == 0
            || payload_size == 0
        {
            return false;
        }
        let crc_required = match protection {
            SoundkitOpusProtection::Encrypted => {
                if !header.is_encrypted() {
                    return false;
                }
                false
            }
            SoundkitOpusProtection::Plain => {
                if header.is_encrypted() {
                    return false;
                }
                true
            }
        };
        if header.packet_crc32_value().is_some() {
            if !matches!(
                header.verify_packet_crc32(&bytes[..header_size], &bytes[header_size..packet_size]),
                Ok(true)
            ) {
                return false;
            }
        } else if crc_required {
            return false;
        }
        total_frames = total_frames.saturating_add(u64::from(header.frame_count()));
        packets = packets.saturating_add(1);
        bytes = &bytes[packet_size..];
    }
    packets > 0
        && total_frames.saturating_mul(1_000) == u64::from(SAMPLE_RATE).saturating_mul(part_ms)
}

fn opus_media_part_is_valid(
    codec: HlsAudioCodec,
    bytes: &[u8],
    part_ms: u64,
    soundkit_opus_protection: SoundkitOpusProtection,
) -> Option<bool> {
    match codec {
        HlsAudioCodec::Opus => Some(fmp4_opus_part_is_valid(bytes, part_ms)),
        HlsAudioCodec::SoundkitOpus => Some(soundkit_opus_part_is_valid(
            bytes,
            part_ms,
            soundkit_opus_protection,
        )),
        _ => None,
    }
}

fn fmp4_opus_part_is_valid(bytes: &[u8], part_ms: u64) -> bool {
    if part_ms == 0 {
        return false;
    }
    let Some(samples) = fmp4_trun_sample_geometry(bytes) else {
        return false;
    };
    let Some(payload) = fmp4_mdat_payload(bytes) else {
        return false;
    };
    let mut offset = 0_usize;
    let mut total_duration = 0_u64;
    for (duration, size) in samples {
        let Ok(size) = usize::try_from(size) else {
            return false;
        };
        let Some(end) = offset.checked_add(size) else {
            return false;
        };
        let Some(packet) = payload.get(offset..end) else {
            return false;
        };
        let Some(packet_info) = opus_packet_info(packet) else {
            return false;
        };
        if packet_info.duration_samples != duration {
            return false;
        }
        total_duration = total_duration.saturating_add(u64::from(duration));
        offset = end;
    }
    offset == payload.len()
        && total_duration.saturating_mul(1_000) == u64::from(SAMPLE_RATE).saturating_mul(part_ms)
}

fn fmp4_trun_sample_geometry(bytes: &[u8]) -> Option<Vec<(u32, u32)>> {
    let type_offset = bytes.windows(4).position(|window| window == b"trun")?;
    let box_start = type_offset.checked_sub(4)?;
    let box_size = usize::try_from(u32::from_be_bytes(
        bytes.get(box_start..type_offset)?.try_into().ok()?,
    ))
    .ok()?;
    let box_end = box_start.checked_add(box_size)?;
    if box_size < 16 || box_end > bytes.len() {
        return None;
    }

    let version_and_flags = u32::from_be_bytes(
        bytes
            .get(type_offset + 4..type_offset + 8)?
            .try_into()
            .ok()?,
    );
    let flags = version_and_flags & 0x00ff_ffff;
    if flags & 0x000100 == 0 || flags & 0x000200 == 0 {
        return None;
    }
    let sample_count = usize::try_from(u32::from_be_bytes(
        bytes
            .get(type_offset + 8..type_offset + 12)?
            .try_into()
            .ok()?,
    ))
    .ok()?;
    if sample_count == 0 {
        return None;
    }

    let mut cursor = type_offset.checked_add(12)?;
    if flags & 0x000001 != 0 {
        cursor = cursor.checked_add(4)?;
    }
    if flags & 0x000004 != 0 {
        cursor = cursor.checked_add(4)?;
    }
    let fields_per_sample = 2_usize
        .checked_add(usize::from(flags & 0x000400 != 0))?
        .checked_add(usize::from(flags & 0x000800 != 0))?;
    let bytes_per_sample = fields_per_sample.checked_mul(4)?;
    if sample_count.checked_mul(bytes_per_sample)? > box_end.checked_sub(cursor)? {
        return None;
    }

    let mut samples = Vec::with_capacity(sample_count);
    for _ in 0..sample_count {
        let duration = u32::from_be_bytes(bytes.get(cursor..cursor + 4)?.try_into().ok()?);
        cursor += 4;
        let size = u32::from_be_bytes(bytes.get(cursor..cursor + 4)?.try_into().ok()?);
        cursor += 4;
        if flags & 0x000400 != 0 {
            cursor += 4;
        }
        if flags & 0x000800 != 0 {
            cursor += 4;
        }
        samples.push((duration, size));
    }
    (cursor == box_end).then_some(samples)
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

/// Timescale of the video track that the contributor's fMP4 bridge writes.
const VIDEO_TIMESCALE: u64 = 90_000;

/// One fMP4 media part: its fragment sequence number and whether the sample
/// sizes of every track add up to the media data that follows.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Fmp4FragmentSummary {
    fragment_sequence: u32,
    sample_bytes: u64,
    media_data_bytes: u64,
    tracks: u32,
}

fn fmp4_child_boxes(bytes: &[u8]) -> Option<Vec<([u8; 4], &[u8])>> {
    let mut boxes = Vec::new();
    let mut offset = 0_usize;
    while offset < bytes.len() {
        let header = bytes.get(offset..offset.checked_add(8)?)?;
        let size = usize::try_from(u32::from_be_bytes(header[..4].try_into().ok()?)).ok()?;
        let kind: [u8; 4] = header[4..8].try_into().ok()?;
        let end = offset.checked_add(size)?;
        if size < 8 || end > bytes.len() {
            return None;
        }
        boxes.push((kind, &bytes[offset + 8..end]));
        offset = end;
    }
    Some(boxes)
}

/// Parse a part that must be exactly one `moof` followed by one `mdat`.
fn fmp4_fragment_summary(bytes: &[u8]) -> Option<Fmp4FragmentSummary> {
    let top = fmp4_child_boxes(bytes)?;
    let [(moof_kind, moof), (mdat_kind, mdat)] = top.as_slice() else {
        return None;
    };
    if moof_kind != b"moof" || mdat_kind != b"mdat" {
        return None;
    }
    let mut fragment_sequence = None;
    let mut sample_bytes = 0_u64;
    let mut tracks = 0_u32;
    for (kind, body) in fmp4_child_boxes(moof)? {
        match &kind {
            b"mfhd" => {
                fragment_sequence = Some(u32::from_be_bytes(body.get(4..8)?.try_into().ok()?));
            }
            b"traf" => {
                let mut has_decode_time = false;
                let mut has_run = false;
                for (kind, body) in fmp4_child_boxes(body)? {
                    match &kind {
                        b"tfdt" => has_decode_time = true,
                        b"trun" => {
                            // Rebuild the box header that the geometry parser expects.
                            let mut boxed = Vec::with_capacity(body.len() + 8);
                            boxed.extend_from_slice(&((body.len() + 8) as u32).to_be_bytes());
                            boxed.extend_from_slice(b"trun");
                            boxed.extend_from_slice(body);
                            for (_duration, size) in fmp4_trun_sample_geometry(&boxed)? {
                                sample_bytes = sample_bytes.checked_add(u64::from(size))?;
                            }
                            has_run = true;
                        }
                        _ => {}
                    }
                }
                if !has_decode_time || !has_run {
                    return None;
                }
                tracks = tracks.checked_add(1)?;
            }
            _ => {}
        }
    }
    Some(Fmp4FragmentSummary {
        fragment_sequence: fragment_sequence?,
        sample_bytes,
        media_data_bytes: mdat.len() as u64,
        tracks,
    })
}

/// Structure check of one H.264 fMP4 part. `sequence_offset` is the difference
/// between the fragment sequence number and the object sequence. The first
/// valid part sets it, and every later part must keep it.
fn h264_part_is_valid(
    bytes: &[u8],
    object_sequence: u64,
    sequence_offset: &mut Option<i64>,
) -> bool {
    let Some(summary) = fmp4_fragment_summary(bytes) else {
        return false;
    };
    if summary.tracks == 0
        || summary.sample_bytes == 0
        || summary.sample_bytes != summary.media_data_bytes
    {
        return false;
    }
    let Ok(object_sequence) = i64::try_from(object_sequence) else {
        return false;
    };
    let offset = i64::from(summary.fragment_sequence) - object_sequence;
    *sequence_offset.get_or_insert(offset) == offset
}

fn fmp4_first_mdhd_timescale(bytes: &[u8]) -> Option<u64> {
    let type_offset = bytes.windows(4).position(|window| window == b"mdhd")?;
    let version = *bytes.get(type_offset + 4)?;
    let value_offset = type_offset + 8 + if version == 1 { 16 } else { 8 };
    Some(u64::from(u32::from_be_bytes(
        bytes.get(value_offset..value_offset + 4)?.try_into().ok()?,
    )))
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
    frame_count: u32,
    channel_start: u16,
    channels: u16,
    signal: ProbeSignal,
) -> Vec<u8> {
    let mut pcm = Vec::with_capacity(frame_count as usize * usize::from(channels) * 3);
    for frame in 0..frame_count {
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
    signal_s24le(
        first_sample,
        FRAME_COUNT,
        0,
        DEFAULT_CHANNELS,
        ProbeSignal::Sine,
    )
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

fn deterministic_sample_index(mut value: u64) -> u64 {
    value = value.wrapping_add(0x9e37_79b9_7f4a_7c15);
    value = (value ^ (value >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    value = (value ^ (value >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    value ^ (value >> 31)
}

fn randomized_reader_start_offset_ms(
    base_offset_ms: u64,
    part_ms: u64,
    arrival_window_ms: u64,
    reader_id: usize,
    seed: u64,
) -> u64 {
    if part_ms == 0 || arrival_window_ms < part_ms {
        return base_offset_ms;
    }
    let slots = arrival_window_ms / part_ms;
    let slot = deterministic_sample_index(seed ^ reader_id as u64) % slots.saturating_add(1);
    base_offset_ms.saturating_add(slot.saturating_mul(part_ms))
}

fn percentiles_ms(values_ns: Vec<u64>) -> Percentiles {
    let count = values_ns.len();
    percentiles_ms_with_count(values_ns, count)
}

fn percentiles_ms_with_count(mut values_ns: Vec<u64>, count: usize) -> Percentiles {
    if values_ns.is_empty() {
        return Percentiles {
            count,
            sample_count: 0,
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
        count,
        sample_count: values_ns.len(),
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
    #[test]
    fn a_receiver_that_joins_a_running_session_listens_for_its_whole_duration() {
        // The session started a minute ago; the listener joins now and asks
        // for ten seconds. It must listen for ten seconds and a tail from
        // now, not stop at once because the session's first ten seconds are
        // over (the zero of the 21 November comparison).
        let now_ns = 1_795_272_252_000_000_000_u64;
        let session_id = now_ns - 60_000_000_000;
        let window = super::ReceiveWindow::new(session_id, now_ns, 10, 3);
        assert_eq!(window.start_ns, now_ns);
        assert_eq!(window.media_end_ns, now_ns + 10_000_000_000);
        assert_eq!(window.stop_ns, now_ns + 13_000_000_000);
    }

    #[test]
    fn a_receiver_that_subscribes_before_the_session_starts_counts_from_its_start() {
        let now_ns = 1_795_272_252_000_000_000_u64;
        let session_id = now_ns + 2_000_000_000;
        let window = super::ReceiveWindow::new(session_id, now_ns, 10, 3);
        assert_eq!(window.start_ns, session_id);
        assert_eq!(window.media_end_ns, session_id + 10_000_000_000);
        assert_eq!(window.stop_ns, session_id + 13_000_000_000);
    }

    use super::*;

    fn test_soundkit_opus_part(id: u8) -> Vec<u8> {
        let payload = [id; 12];
        let header = FrameHeaderV2::new(
            EncodingFlag::Opus,
            payload.len() as u32,
            FRAME_COUNT,
            SAMPLE_RATE,
            2,
            16,
            frame_header::Endianness::LittleEndian,
            None,
            None,
            None,
        )
        .unwrap()
        .with_packet_flags(FrameHeaderV2::FLAG_ENCRYPTED)
        .unwrap();
        let mut bytes = Vec::with_capacity(header.size() + payload.len());
        header.encode(&mut bytes).unwrap();
        bytes.extend_from_slice(&payload);
        bytes
    }

    #[test]
    fn splits_aggregated_soundkit_body_on_exact_cached_part_boundaries() {
        let expected: Vec<Vec<u8>> = (0..40).map(test_soundkit_opus_part).collect();
        let body: Vec<u8> = expected.iter().flatten().copied().collect();
        let parts = split_soundkit_opus_parts(&body, 5).unwrap();
        assert_eq!(parts.len(), 40);
        for (actual, expected) in parts.into_iter().zip(&expected) {
            assert_eq!(actual, expected);
            assert!(soundkit_opus_part_is_valid(
                actual,
                5,
                SoundkitOpusProtection::Encrypted
            ));
        }
    }

    /// One plain SoundKit Opus packet as the direct qualification source
    /// publishes it: no encrypted flag, and a packet CRC32 when requested.
    fn test_plain_soundkit_opus_part(id: u8, with_crc: bool) -> Vec<u8> {
        let payload = [id; 12];
        let mut header = FrameHeaderV2::new(
            EncodingFlag::Opus,
            payload.len() as u32,
            FRAME_COUNT,
            SAMPLE_RATE,
            2,
            16,
            frame_header::Endianness::LittleEndian,
            None,
            None,
            None,
        )
        .unwrap();
        if with_crc {
            header = header.with_packet_crc32(&payload).unwrap();
        }
        let mut bytes = Vec::with_capacity(header.size() + payload.len());
        header.encode(&mut bytes).unwrap();
        bytes.extend_from_slice(&payload);
        bytes
    }

    /// One fMP4 part with a video and an audio track fragment.
    fn test_fmp4_av_part(
        fragment_sequence: u32,
        decode_time: u64,
        video: &[u32],
        audio: &[u32],
    ) -> Vec<u8> {
        fn boxed(kind: &[u8; 4], body: &[u8]) -> Vec<u8> {
            let mut out = ((body.len() + 8) as u32).to_be_bytes().to_vec();
            out.extend_from_slice(kind);
            out.extend_from_slice(body);
            out
        }
        fn traf(track: u32, decode_time: u64, duration: u32, sizes: &[u32]) -> Vec<u8> {
            let mut tfhd = vec![0, 0, 0, 0];
            tfhd.extend_from_slice(&track.to_be_bytes());
            let mut tfdt = vec![1, 0, 0, 0];
            tfdt.extend_from_slice(&decode_time.to_be_bytes());
            // Flags: data offset, sample duration, sample size.
            let mut trun = vec![0, 0, 0x03, 0x01];
            trun.extend_from_slice(&(sizes.len() as u32).to_be_bytes());
            trun.extend_from_slice(&0_u32.to_be_bytes());
            for size in sizes {
                trun.extend_from_slice(&duration.to_be_bytes());
                trun.extend_from_slice(&size.to_be_bytes());
            }
            let mut body = boxed(b"tfhd", &tfhd);
            body.extend_from_slice(&boxed(b"tfdt", &tfdt));
            body.extend_from_slice(&boxed(b"trun", &trun));
            boxed(b"traf", &body)
        }
        let mut mfhd = vec![0, 0, 0, 0];
        mfhd.extend_from_slice(&fragment_sequence.to_be_bytes());
        let mut moof = boxed(b"mfhd", &mfhd);
        moof.extend_from_slice(&traf(1, decode_time, 3_750, video));
        moof.extend_from_slice(&traf(2, 0, 1_024, audio));
        let media = vec![0x5a; (video.iter().sum::<u32>() + audio.iter().sum::<u32>()) as usize];
        let mut part = boxed(b"moof", &moof);
        part.extend_from_slice(&boxed(b"mdat", &media));
        part
    }

    #[test]
    fn h264_part_check_requires_whole_samples_and_a_steady_fragment_sequence() {
        let part = test_fmp4_av_part(61, 1_351_920, &[900, 400, 300, 200, 100, 100], &[40, 41]);
        let summary = fmp4_fragment_summary(&part).unwrap();
        assert_eq!(summary.fragment_sequence, 61);
        assert_eq!(summary.tracks, 2);
        assert_eq!(summary.sample_bytes, 2_081);
        assert_eq!(summary.media_data_bytes, 2_081);

        // The first valid part fixes the fragment-to-object sequence offset.
        let mut offset = None;
        assert!(h264_part_is_valid(&part, 60, &mut offset));
        assert_eq!(offset, Some(1));
        let next = test_fmp4_av_part(62, 1_374_420, &[500, 500], &[40]);
        assert!(h264_part_is_valid(&next, 61, &mut offset));
        // A part that belongs to another object fails the check.
        assert!(!h264_part_is_valid(&next, 63, &mut offset));

        // A truncated or extended part fails, and so does a lost byte of media.
        assert!(!h264_part_is_valid(&part[..part.len() - 1], 60, &mut None));
        let mut extended = part.clone();
        extended.push(0);
        assert!(!h264_part_is_valid(&extended, 60, &mut None));

        // The video timeline comes from the track fragment decode time.
        assert_eq!(
            media_part_pts_ms(HlsAudioCodec::H264, &part, 60, 0, 0, 250).unwrap(),
            15_021
        );
        assert_eq!(
            media_part_duration_ms(HlsAudioCodec::H264, &part, 250).unwrap(),
            250
        );
    }

    #[test]
    fn soundkit_opus_protection_selects_the_packet_contract() {
        let encrypted = test_soundkit_opus_part(3);
        let plain = test_plain_soundkit_opus_part(3, true);
        let plain_without_crc = test_plain_soundkit_opus_part(3, false);

        // The production contract is unchanged: encrypted packets only.
        assert!(soundkit_opus_part_is_valid(
            &encrypted,
            5,
            SoundkitOpusProtection::Encrypted
        ));
        assert!(!soundkit_opus_part_is_valid(
            &plain,
            5,
            SoundkitOpusProtection::Encrypted
        ));

        // The qualification contract accepts plain packets with a CRC32 and
        // nothing else.
        assert!(soundkit_opus_part_is_valid(
            &plain,
            5,
            SoundkitOpusProtection::Plain
        ));
        assert!(!soundkit_opus_part_is_valid(
            &encrypted,
            5,
            SoundkitOpusProtection::Plain
        ));
        assert!(!soundkit_opus_part_is_valid(
            &plain_without_crc,
            5,
            SoundkitOpusProtection::Plain
        ));
    }

    #[test]
    fn plain_soundkit_opus_part_rejects_any_changed_byte() {
        let plain = test_plain_soundkit_opus_part(9, true);
        for index in 0..plain.len() {
            let mut corrupt = plain.clone();
            corrupt[index] ^= 0x01;
            assert!(
                !soundkit_opus_part_is_valid(&corrupt, 5, SoundkitOpusProtection::Plain),
                "a changed bit in byte {index} must fail validation"
            );
        }
        // A truncated or extended part is never a whole number of packets.
        assert!(!soundkit_opus_part_is_valid(
            &plain[..plain.len() - 1],
            5,
            SoundkitOpusProtection::Plain
        ));
        let mut extended = plain.clone();
        extended.push(0);
        assert!(!soundkit_opus_part_is_valid(
            &extended,
            5,
            SoundkitOpusProtection::Plain
        ));
    }

    #[test]
    fn standard_opus_fmp4_requires_valid_raw_packets_and_48khz_timestamps() {
        use access_unit::{AccessUnit, PSI_STREAM_AUDIO_OPUS};
        use boxer::fmp4::{
            box_fmp4_with_init_and_audio_config, AudioTrackConfig, Config, OpusAudioConfig,
        };

        let units = (0_u64..50)
            .map(|index| AccessUnit {
                key: true,
                pts: 250 + index * 5,
                dts: 250 + index * 5,
                data: Bytes::from(vec![(17 << 3) | (1 << 2), index as u8]),
                stream_type: PSI_STREAM_AUDIO_OPUS,
                id: index,
            })
            .collect();
        let fmp4 = box_fmp4_with_init_and_audio_config(
            6,
            Config {
                width: 0,
                height: 0,
                avcc: None,
            },
            Vec::new(),
            units,
            0,
            true,
            Some(AudioTrackConfig::Opus(OpusAudioConfig {
                input_sample_rate: SAMPLE_RATE,
                channel_count: 2,
                pre_skip: 0,
                output_gain: 0,
            })),
        );
        let init = fmp4.init.expect("Opus init segment");

        assert_eq!(HlsAudioCodec::Opus.media_extension(), "mp4");
        assert_eq!(detect_init_audio_codec(&init), Some(HlsAudioCodec::Opus));
        assert!(pcm_init_parameters_match(&init, HlsAudioCodec::Opus, 2));
        assert_eq!(
            fmp4_trun_sample_geometry(&fmp4.data)
                .expect("Opus sample geometry")
                .len(),
            50
        );
        assert!(fmp4_opus_part_is_valid(&fmp4.data, 250));
        assert_eq!(
            media_part_pts_ms(HlsAudioCodec::Opus, &fmp4.data, 3_984, 3_984, 0, 250).unwrap(),
            250
        );
        assert_eq!(
            media_part_duration_ms(HlsAudioCodec::Opus, &fmp4.data, 250).unwrap(),
            250
        );

        let mut truncated = fmp4.data.to_vec();
        truncated.pop();
        assert!(!fmp4_opus_part_is_valid(&truncated, 250));
    }

    #[test]
    fn bundled_tail_entries_require_exact_stream_order_and_shared_sequence() {
        let entries = vec![
            TailBundleEntry {
                stream_id: 77,
                start_sequence: 10,
                end_sequence: 10,
                available_unix_us: Some(1_000),
                payload: Bytes::from_static(b"track-a"),
            },
            TailBundleEntry {
                stream_id: 78,
                start_sequence: 10,
                end_sequence: 10,
                available_unix_us: Some(1_001),
                payload: Bytes::from_static(b"track-b"),
            },
        ];
        validate_tail_bundle_entries(&[77, 78], &entries, Some(9), 0, 1).unwrap();

        let mut wrong_stream = entries.clone();
        wrong_stream.swap(0, 1);
        assert!(validate_tail_bundle_entries(&[77, 78], &wrong_stream, Some(9), 0, 1).is_err());

        let mut wrong_sequence = entries;
        wrong_sequence[1].end_sequence = 11;
        assert!(validate_tail_bundle_entries(&[77, 78], &wrong_sequence, Some(9), 0, 1).is_err());
    }

    const FIVE_MS: EpochGeometry = EpochGeometry { samples: 240 };

    #[test]
    fn epochs_leave_on_absolute_deadlines_of_their_size() {
        let session_id = 1_700_000_000_005_000_000;
        assert_eq!(epoch_deadline_ns(session_id, 0, FIVE_MS), session_id);
        assert_eq!(
            epoch_deadline_ns(session_id, 1, FIVE_MS),
            session_id + 5_000_000
        );
        // The deadline of an epoch is the capture time the receivers count:
        // session id plus the epoch's pts.
        for samples in [12, 24, 48, 120, 240, 7] {
            let epoch = EpochGeometry::new(samples);
            for epoch_id in [0, 1, 7, 200, 379_999] {
                let pts_samples = epoch_id * u64::from(samples);
                assert_eq!(
                    epoch_deadline_ns(session_id, epoch_id, epoch),
                    session_id + pts_samples * 1_000_000_000 / u64::from(SAMPLE_RATE)
                );
            }
        }
        assert_eq!(EpochGeometry::new(12).duration_ns(), 250_000);
        assert_eq!(EpochGeometry::new(24).duration_ns(), 500_000);
        assert_eq!(EpochGeometry::new(48).duration_ns(), 1_000_000);
        assert_eq!(EpochGeometry::new(120).duration_ns(), 2_500_000);
        // 4,000 epochs a second of 12 samples, 400 of 120.
        assert_eq!(EpochGeometry::new(12).epochs_in(2), 8_000);
        assert_eq!(EpochGeometry::new(120).epochs_in(10), 4_000);
        assert_eq!(FIVE_MS.epochs_in(10), 2_000);
    }

    #[test]
    fn the_spin_budget_is_a_quarter_of_a_small_epoch() {
        assert_eq!(FIVE_MS.spin_budget_ns(), PRECISE_PACE_SPIN_NS);
        assert_eq!(EpochGeometry::new(120).spin_budget_ns(), 300_000);
        assert_eq!(EpochGeometry::new(48).spin_budget_ns(), 250_000);
        assert_eq!(EpochGeometry::new(24).spin_budget_ns(), 125_000);
        assert_eq!(EpochGeometry::new(12).spin_budget_ns(), 62_500);
        // A 250 µs epoch sleeps most of its interval.
        let epoch = EpochGeometry::new(12);
        assert_eq!(
            pace_step(0, epoch.duration_ns(), epoch.spin_budget_ns()),
            PaceStep::Sleep(Duration::from_nanos(187_500))
        );
    }

    #[test]
    fn epoch_sizes_follow_the_payload() {
        assert_eq!(ProbePayload::Pcm.epoch_samples(None).unwrap(), 48);
        assert_eq!(ProbePayload::Flac.epoch_samples(None).unwrap(), 48);
        assert_eq!(ProbePayload::Opus.epoch_samples(None).unwrap(), 48);
        assert_eq!(ProbePayload::Pcm.epoch_samples(Some(12)).unwrap(), 12);
        assert!(ProbePayload::Pcm.epoch_samples(Some(11)).is_err());
        assert_eq!(ProbePayload::Flac.epoch_samples(Some(32)).unwrap(), 32);
        assert!(ProbePayload::Flac.epoch_samples(Some(24)).is_err());
        for samples in [48, 64, 72, 120, 240, 480, 960] {
            assert_eq!(
                ProbePayload::Opus.epoch_samples(Some(samples)).unwrap(),
                samples
            );
        }
        assert!(ProbePayload::Opus.epoch_samples(Some(96)).is_err());
        assert!(ProbePayload::Opus.epoch_samples(Some(100)).is_err());
        assert_eq!(effective_group_channels(ProbePayload::Opus, 8), 2);
        assert_eq!(effective_group_channels(ProbePayload::Pcm, 8), 8);
        assert_eq!(effective_group_channels(ProbePayload::Flac, 16), 8);
    }

    #[test]
    fn precise_pacing_sleeps_then_spins_and_never_carries_lateness() {
        let spin = PRECISE_PACE_SPIN_NS;
        let deadline = 10_000_000_000;
        assert_eq!(
            pace_step(deadline - 5_000_000, deadline, spin),
            PaceStep::Sleep(Duration::from_nanos(5_000_000 - PRECISE_PACE_SPIN_NS))
        );
        assert_eq!(
            pace_step(deadline - PRECISE_PACE_SPIN_NS, deadline, spin),
            PaceStep::Spin
        );
        assert_eq!(pace_step(deadline - 1, deadline, spin), PaceStep::Spin);
        assert_eq!(pace_step(deadline, deadline, spin), PaceStep::Send);
        assert_eq!(pace_step(deadline + 1, deadline, spin), PaceStep::Send);

        // A stall of 23 ms during epoch 3: epochs 4..=7 are due at once and
        // epoch 8 is on its own deadline again, not 23 ms later.
        let session_id = 1_000_000_000;
        let woke_at = epoch_deadline_ns(session_id, 3, FIVE_MS) + 23_000_000;
        for epoch_id in 4..=7 {
            assert_eq!(
                pace_step(
                    woke_at,
                    epoch_deadline_ns(session_id, epoch_id, FIVE_MS),
                    spin
                ),
                PaceStep::Send
            );
        }
        assert_eq!(
            pace_step(woke_at, epoch_deadline_ns(session_id, 8, FIVE_MS), spin),
            PaceStep::Sleep(Duration::from_nanos(2_000_000 - PRECISE_PACE_SPIN_NS))
        );

        let mut lateness = SendLateness::default();
        lateness.record(100, 90, 130);
        lateness.record(100, 150, 170);
        assert_eq!(lateness.epochs, 2);
        assert_eq!(lateness.first_datagram.values_ns(), [0, 50]);
        assert_eq!(lateness.last_datagram.values_ns(), [30, 70]);
    }

    #[test]
    fn lateness_of_a_long_run_is_bounded_and_keeps_its_maximum() {
        let mut lateness = SendLateness::default();
        let epochs = MAX_LATENCY_SAMPLES as u64 * 3;
        for epoch_id in 0..epochs {
            let late = if epoch_id == 17 {
                9_000_000
            } else {
                epoch_id % 1_000
            };
            lateness.record(0, late, late + 10);
        }
        assert_eq!(lateness.epochs, epochs);
        assert_eq!(lateness.first_datagram.count(), epochs as usize);
        assert_eq!(
            lateness.first_datagram.values_ns().len(),
            MAX_LATENCY_SAMPLES
        );
        let summary = lateness.first_datagram.summary();
        assert_eq!(summary.count, epochs as usize);
        assert_eq!(summary.max, 9.0);
    }

    /// The groups the contributor recovers from the datagrams of `epochs`
    /// epochs, after it has taken the SoundKit v2 framing off.
    fn contributor_groups(
        payload_kind: ProbePayload,
        epoch_samples: u32,
        epochs: u64,
    ) -> Vec<DecodedMultichannelAudioGroup> {
        let mut options = sine_source(1_700_000_000_000_000_000);
        options.payload_kind = payload_kind;
        options.epoch_samples = epoch_samples;
        let mut encoder = EpochEncoder::new(&options).unwrap();
        let transport = MultichannelAudioTransportAdapter::udp(65_535);
        let mut receiver =
            MultichannelAudioReceiver::new(MultichannelAudioSessionConfig::default());
        let mut groups = Vec::new();
        for epoch_id in 0..epochs {
            for (datagram, _) in encoder.encode(epoch_id).unwrap() {
                let payload = transport.payload(&datagram).unwrap();
                for group in receiver.push_datagram(payload).unwrap().completed_groups {
                    groups
                        .push(av_contrib::soundkit_v2::terminalize_soundkit_group(group).unwrap());
                }
            }
        }
        groups
    }

    #[test]
    fn opus_epochs_reach_the_contributor_as_opus_groups() {
        // 1, 1.33 and 1.5 ms Opus Custom (signalling byte on) and 2.5 ms
        // standard Opus.
        for frame in [48_u32, 64, 72, 120] {
            let groups = contributor_groups(ProbePayload::Opus, frame, 8);
            assert_eq!(groups.len(), 8);
            let mut decoder = new_lane_opus_decoder(SAMPLE_RATE, 2, frame).unwrap();
            let custom = av_contrib::opus_custom::is_custom_opus_frame(SAMPLE_RATE, frame);
            assert_eq!(custom, frame < 120);
            for (epoch_id, group) in groups.iter().enumerate() {
                assert_eq!(group.payload_kind, AudioPayloadKind::Opus);
                assert_eq!(group.frame_count, frame);
                assert_eq!(group.pts_samples, epoch_id as u64 * u64::from(frame));
                assert_eq!(group.channel_count, 2);
                // One SoundKit v2 Opus packet of one CELT frame, framed as
                // the DAW source frames it.
                let stream =
                    av_contrib::soundkit_v2::SoundKitV2StreamRef::parse(&group.payload, true)
                        .unwrap();
                assert_eq!(stream.packets.len(), 1);
                let header = &stream.packets[0].header;
                assert_eq!(header.encoding(), &EncodingFlag::Opus);
                assert_eq!(header.frame_count(), frame);
                assert_eq!(header.bits_per_sample(), 0);
                assert_eq!(header.id(), Some(epoch_id as u64));
                assert!(!header.is_encrypted());
                let packet = stream.packets[0].payload;
                if custom {
                    // CBR at the lane's 320 kb/s, signalling byte included
                    // (bit 2: stereo).
                    assert_eq!(
                        packet.len(),
                        (320_000 * frame as usize + 4 * 48_000) / (8 * 48_000)
                    );
                    assert_eq!(packet[0] & 0x04, 0x04);
                } else {
                    assert!(packet[0] & 0x80 != 0, "CELT-only TOC");
                }
                let mut pcm = vec![0_i16; frame as usize * 2];
                let decoded = decoder.decode_i16(packet, &mut pcm, false).unwrap();
                assert_eq!(decoded, frame as usize);
            }
        }
    }

    fn plain_consumer(expected_frame_samples: Option<u32>) -> SoundKitOpusConsumer {
        SoundKitOpusConsumer::plain(OpusConsumerOptions {
            first_sequence: 0,
            decoded_pcm_output: None,
            reference_pcm_s16le: None,
            reference_leading_silence_frames: 0,
            reference_start_offset_ms: 0,
            reference_window_ms: 0,
            expected_frame_samples,
        })
        .unwrap()
    }

    #[test]
    fn consumer_decodes_plain_custom_and_standard_parts() {
        for (frame, mode) in [
            (48_u32, "custom"),
            (64, "custom"),
            (120, "standard"),
            (240, "standard"),
        ] {
            let mut consumer = plain_consumer(Some(frame));
            for (sequence, group) in contributor_groups(ProbePayload::Opus, frame, 40)
                .iter()
                .enumerate()
            {
                // A whole-millisecond frame is a valid part of its length.
                if frame % 48 == 0 {
                    assert!(soundkit_opus_part_is_valid(
                        &group.payload,
                        u64::from(frame / 48),
                        SoundkitOpusProtection::Plain
                    ));
                }
                consumer.push_part(sequence as u64, &group.payload).unwrap();
            }
            let report = consumer.finish().unwrap();
            assert_eq!(report.protection, "plain");
            assert_eq!(report.opus_frame_samples, vec![frame]);
            assert_eq!(report.opus_mode, Some(mode));
            assert_eq!(report.soundkit_v2_packets, 40);
            assert_eq!(report.plain_packets, 40);
            assert_eq!(report.decoded_opus_packets, 40);
            assert_eq!(report.decoded_pcm_frames, 40 * u64::from(frame));
            assert!(report.waveform_verified, "{report:?}");
        }

        // A part of another frame size fails the expected-frame check.
        let mut consumer = plain_consumer(Some(48));
        let groups = contributor_groups(ProbePayload::Opus, 120, 1);
        let error = consumer.push_part(0, &groups[0].payload).unwrap_err();
        assert!(format!("{error:#}").contains("--expected-opus-frame-samples is 48"));

        let mut frames = OpusFrameObservation::new(Some(48));
        let custom = contributor_groups(ProbePayload::Opus, 48, 1);
        assert!(frames.observe(HlsAudioCodec::SoundkitOpus, &custom[0].payload));
        assert!(!frames.observe(HlsAudioCodec::SoundkitOpus, &groups[0].payload));
        let report = frames.report();
        assert_eq!(report.opus_frame_samples, vec![48, 120]);
        assert_eq!(report.opus_mode, Some("mixed"));
        assert_eq!(report.opus_frame_samples_mismatched_parts, 1);
    }

    #[test]
    fn one_ms_custom_packets_split_into_one_and_five_ms_parts() {
        let groups = contributor_groups(ProbePayload::Opus, 48, 10);
        let mut body = Vec::new();
        for group in &groups {
            body.extend_from_slice(&group.payload);
        }
        let one_ms = split_soundkit_opus_parts(&body, 1).unwrap();
        assert_eq!(one_ms.len(), 10);
        for (part, group) in one_ms.iter().zip(&groups) {
            assert_eq!(*part, group.payload.as_ref());
            assert!(soundkit_opus_part_is_valid(
                part,
                1,
                SoundkitOpusProtection::Plain
            ));
            assert!(!soundkit_opus_part_is_valid(
                part,
                1,
                SoundkitOpusProtection::Encrypted
            ));
        }
        let five_ms = split_soundkit_opus_parts(&body, 5).unwrap();
        assert_eq!(five_ms.len(), 2);
        for part in five_ms {
            assert!(soundkit_opus_part_is_valid(
                part,
                5,
                SoundkitOpusProtection::Plain
            ));
            assert_eq!(soundkit_v2_frame_counts(part), vec![48; 5]);
        }
        // 1.33 ms frames fill 4 ms parts (three frames) but not 5 ms parts.
        let groups = contributor_groups(ProbePayload::Opus, 64, 6);
        let body = groups
            .iter()
            .flat_map(|group| group.payload.iter().copied())
            .collect::<Vec<_>>();
        assert_eq!(split_soundkit_opus_parts(&body, 4).unwrap().len(), 2);
        assert!(split_soundkit_opus_parts(&body, 5).is_err());
    }

    #[test]
    fn small_lossless_epochs_carry_their_exact_sample_counts() {
        for (payload, samples) in [
            (ProbePayload::Pcm, 12),
            (ProbePayload::Pcm, 24),
            (ProbePayload::Pcm, 48),
            (ProbePayload::Flac, 32),
            (ProbePayload::Flac, 48),
        ] {
            let groups = contributor_groups(payload, samples, 4);
            assert_eq!(groups.len(), 4, "{payload:?} {samples}");
            for (epoch_id, group) in groups.iter().enumerate() {
                assert_eq!(group.frame_count, samples);
                assert_eq!(group.pts_samples, epoch_id as u64 * u64::from(samples));
                if payload == ProbePayload::Pcm {
                    assert_eq!(group.payload_kind, AudioPayloadKind::Pcm);
                    assert_eq!(group.payload.len(), samples as usize * 2 * 3);
                } else {
                    assert_eq!(group.payload_kind, AudioPayloadKind::Flac);
                }
            }
        }
    }

    #[test]
    fn precise_sender_keeps_quarter_millisecond_epochs_on_their_deadlines() {
        let receiver = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
        receiver
            .set_read_timeout(Some(Duration::from_millis(300)))
            .unwrap();
        let target = receiver.local_addr().unwrap();
        // Read while the sender sends: 800 datagrams overflow a default
        // socket buffer that nobody drains.
        let reader = std::thread::spawn(move || {
            let mut buffer = [0_u8; 2_048];
            let mut received = 0_u64;
            while receiver.recv_from(&mut buffer).is_ok() {
                received += 1;
            }
            received
        });
        let epochs = 400;
        let mut options = sine_source(now_unix_ns().unwrap() + 20_000_000);
        options.epoch_samples = 12;
        let session_id = options.session_id;
        let epoch = EpochGeometry::new(12);
        let (encoder, lateness) =
            send_precise_paced(options, target, epochs, &AtomicBool::new(false)).unwrap();
        let finished = now_unix_ns().unwrap();
        assert_eq!(lateness.epochs, epochs);
        // 400 epochs of 250 µs: 100 ms, ending on the last deadline.
        let last = epoch_deadline_ns(session_id, epochs - 1, epoch);
        assert_eq!(last - session_id, 99_750_000);
        assert!(finished >= last);
        assert!(finished < last + 50_000_000);
        assert_eq!(
            reader.join().unwrap(),
            encoder.source_datagrams + encoder.repair_datagrams
        );
        // Every epoch left close to its deadline: the pacer did not fall
        // behind 4,000 epochs a second.
        let summary = lateness.first_datagram.summary();
        assert!(summary.p50 < 1.0, "p50 {} ms", summary.p50);
    }

    #[test]
    fn precise_sender_sends_every_epoch_on_its_deadline() {
        let receiver = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
        receiver
            .set_read_timeout(Some(Duration::from_secs(2)))
            .unwrap();
        let target = receiver.local_addr().unwrap();
        let epochs = 20;
        let options = EpochSourceOptions {
            session_id: now_unix_ns().unwrap() + 20_000_000,
            payload_kind: ProbePayload::Pcm,
            epoch_samples: 240,
            opus_bitrate: PROBE_OPUS_BITRATE,
            channels: 2,
            group_channels: 8,
            signal: ProbeSignal::Sine,
            pcm_s24le_file: None,
            group_id: 0,
            repair_percent: 12,
            min_repair_symbols: 1,
        };
        let session_id = options.session_id;
        let (encoder, lateness) =
            send_precise_paced(options, target, epochs, &AtomicBool::new(false)).unwrap();
        let finished = now_unix_ns().unwrap();
        assert_eq!(lateness.epochs, epochs);
        // The run ends with the last deadline, not later by the sum of the
        // per-epoch lateness.
        assert!(finished >= epoch_deadline_ns(session_id, epochs - 1, FIVE_MS));
        assert!(finished < epoch_deadline_ns(session_id, epochs - 1, FIVE_MS) + 50_000_000);
        let datagrams = encoder.source_datagrams + encoder.repair_datagrams;
        assert!(encoder.source_datagrams >= epochs);
        let mut buffer = [0_u8; 2_048];
        for _ in 0..datagrams {
            receiver.recv_from(&mut buffer).unwrap();
        }
    }

    fn sine_source(session_id: u64) -> EpochSourceOptions {
        EpochSourceOptions {
            session_id,
            payload_kind: ProbePayload::Pcm,
            epoch_samples: 240,
            opus_bitrate: PROBE_OPUS_BITRATE,
            channels: 2,
            group_channels: 8,
            signal: ProbeSignal::Sine,
            pcm_s24le_file: None,
            group_id: 0,
            repair_percent: 12,
            min_repair_symbols: 1,
        }
    }

    #[test]
    fn precise_sender_stops_when_asked_and_keeps_what_it_sent() {
        let receiver = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
        let target = receiver.local_addr().unwrap();
        let stop = Arc::new(AtomicBool::new(false));
        let session_id = now_unix_ns().unwrap() + 10_000_000;
        let flag = Arc::clone(&stop);
        let stopper = std::thread::spawn(move || {
            // About ten epochs in, as a SIGTERM from the harness would.
            std::thread::sleep(Duration::from_millis(60));
            flag.store(true, AtomicOrdering::Release);
        });
        let started = Instant::now();
        let (_, lateness) =
            send_precise_paced(sine_source(session_id), target, 200_000, &stop).unwrap();
        stopper.join().unwrap();
        assert!(started.elapsed() < Duration::from_secs(2));
        let sent = lateness.epochs as usize;
        assert!((1..200_000).contains(&sent), "{sent} epochs");
        assert_eq!(lateness.last_datagram.count(), sent);

        // A stop during the wait for the session id ends the wait.
        let stop = AtomicBool::new(true);
        let far = now_unix_ns().unwrap() + 30_000_000_000;
        let (_, lateness) = send_precise_paced(sine_source(far), target, 10, &stop).unwrap();
        assert_eq!(lateness.epochs, 0);
    }

    #[tokio::test]
    async fn tokio_sender_reads_its_source_before_the_session_starts() {
        let receiver = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
        let target = receiver.local_addr().unwrap();
        // A missing PCM file fails at once, not after the wait for a session
        // id 30 s away: the source is read before the wait, as it always was.
        let mut options = sine_source(now_unix_ns().unwrap() + 30_000_000_000);
        options.pcm_s24le_file = Some(PathBuf::from("/nonexistent/aep1-probe-audio.s24le"));
        let result = tokio::time::timeout(
            Duration::from_secs(5),
            send_tokio_paced(options, target, 10, &AtomicBool::new(false)),
        )
        .await
        .expect("the tokio sender waited for the session before reading its source");
        assert!(result.is_err());

        // With the source ready, the stop flag ends the run.
        let stop = Arc::new(AtomicBool::new(false));
        let flag = Arc::clone(&stop);
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(60)).await;
            flag.store(true, AtomicOrdering::Release);
        });
        let session_id = now_unix_ns().unwrap() + 10_000_000;
        let (_, lateness) = tokio::time::timeout(
            Duration::from_secs(5),
            send_tokio_paced(sine_source(session_id), target, 200_000, &stop),
        )
        .await
        .unwrap()
        .unwrap();
        let sent = lateness.epochs as usize;
        assert!((1..200_000).contains(&sent), "{sent} epochs");
    }

    #[test]
    fn percentile_summary_is_deterministic() {
        let summary = percentiles_ms(vec![1_000_000, 2_000_000, 3_000_000, 4_000_000]);
        assert_eq!(summary.count, 4);
        assert_eq!(summary.sample_count, 4);
        assert_eq!(summary.min, 1.0);
        assert_eq!(summary.p50, 2.0);
        assert_eq!(summary.p95, 4.0);
        assert_eq!(summary.p99, 4.0);
        assert_eq!(summary.max, 4.0);
    }

    #[test]
    fn load_hls_requires_an_explicit_deadline() {
        let required = [
            "aep1-48k-probe",
            "load-hls",
            "--edge",
            "127.0.0.1:19444",
            "--stream-id",
            "1",
            "--session-id",
            "1",
        ];
        assert!(Args::try_parse_from(required).is_err());

        let mut declared = required.to_vec();
        declared.extend(["--deadline-ms", "20"]);
        assert!(Args::try_parse_from(declared).is_ok());
    }

    #[test]
    fn long_hls_continuity_uses_compact_exact_coverage() {
        let expected_parts = 4 * 60 * 60 * 200;
        let mut coverage = PartCoverage::new(0, expected_parts, 5).unwrap();
        assert!(coverage.words.len() * std::mem::size_of::<u64>() < 400_000);
        assert_eq!(coverage.first_missing_relative(), Some(0));
        assert!(coverage.insert_pts(0));
        assert!(coverage.insert_pts(5));
        assert!(coverage.insert_pts((expected_parts - 1) * 5));
        assert!(!coverage.insert_pts(5));
        assert_eq!(coverage.received_parts, 3);
        assert_eq!(coverage.first_missing_relative(), Some(2));
        assert_eq!(coverage.first_pts_ms(), Some(0));
        assert_eq!(coverage.last_pts_ms(), Some((expected_parts - 1) * 5));
        assert_eq!(coverage.non_contiguous_pts(), 1);
    }

    #[test]
    fn expected_epochs_follow_the_sender_epoch() {
        let mut state = ReceiveFormatState::new(&[ReceiveAudioFormat::Pcm]).unwrap();
        // Nothing to go by: the old 5 ms epochs.
        assert_eq!(state.expected_epochs(10, None), 2_000);
        // 1 ms epochs: 1,000 a second, so 50% loss is 5,000 missing in 10 s,
        // where 5 ms counting read 0.
        state.epoch_frames = Some(48);
        assert_eq!(state.expected_epochs(10, None), 10_000);
        // 0.25 ms and 2.5 ms epochs, and a setting that wins.
        state.epoch_frames = Some(12);
        assert_eq!(state.expected_epochs(10, None), 40_000);
        assert_eq!(state.expected_epochs(10, Some(120)), 4_000);
        assert!(receiver_epoch_note(240).is_none());
        assert!(receiver_epoch_note(48).is_some_and(|note| note.contains("receive-udp")));
    }

    #[test]
    fn udp_time_series_includes_empty_media_seconds() {
        let mut state = ReceiveFormatState::new(&[ReceiveAudioFormat::Flac]).unwrap();
        state
            .latency_buckets
            .entry((receive_audio_format_order(ReceiveAudioFormat::Flac), 0))
            .or_default()
            .record(12_000_000, false, false, false);

        let buckets = state.finish_latency_time_series(1_000_000_000, 2);

        assert_eq!(buckets.len(), 2);
        assert_eq!(buckets[0].expected_epochs, 200);
        assert_eq!(buckets[0].received_epochs, 1);
        assert_eq!(buckets[0].missing_epochs, 199);
        assert_eq!(buckets[0].render_ready_latency_ms.p99, 12.0);
        assert_eq!(buckets[1].received_epochs, 0);
        assert_eq!(buckets[1].missing_epochs, 200);
    }

    #[test]
    fn hls_time_series_uses_source_pts_and_preserves_missing_parts() {
        let mut timeline = HlsLatencyTimeSeries::default();
        timeline.record_part(0, 20_000_000, 170_000_000, false);
        timeline.record_cache(0, 12_000_000, 8_000_000);
        timeline.record_part(250, 30_000_000, 180_000_000, true);

        let buckets = timeline.finish(5_000_000_000, 0, 2_000, 250);

        assert_eq!(buckets.len(), 2);
        assert_eq!(buckets[0].start_unix_ns, 5_000_000_000);
        assert_eq!(buckets[0].expected_parts, 4);
        assert_eq!(buckets[0].received_parts, 2);
        assert_eq!(buckets[0].missing_parts, 2);
        assert_eq!(buckets[0].deadline_misses, 1);
        assert_eq!(buckets[0].availability_latency_ms.p99, 30.0);
        assert_eq!(buckets[0].publication_to_cache_latency_ms.p99, 12.0);
        assert_eq!(buckets[1].expected_parts, 4);
        assert_eq!(buckets[1].received_parts, 0);
    }

    #[test]
    fn long_latency_summary_has_bounded_uniform_reservoir() {
        let observations = MAX_LATENCY_SAMPLES * 3;
        let mut samples = BoundedLatencySamples::new();
        for value in 0..observations {
            samples.record(value as u64 * 1_000_000);
        }
        let summary = samples.summary();
        assert_eq!(summary.count, observations);
        assert_eq!(summary.sample_count, MAX_LATENCY_SAMPLES);
        assert!(summary.p50 > observations as f64 * 0.45);
        assert!(summary.p50 < observations as f64 * 0.55);
        assert!(summary.p99 > observations as f64 * 0.95);
    }

    #[test]
    fn late_join_window_bounds_a_long_publication() {
        assert_eq!(
            hls_window_end_ms(4 * 60 * 60, 300_000, Some(4), 5).unwrap(),
            304_000
        );
        assert_eq!(
            (304_000_u64 - 300_000) / 5,
            800,
            "a four-second 5 ms window has 800 parts"
        );
        assert_eq!(hls_window_end_ms(10, 2_000, None, 5).unwrap(), 10_000);
    }

    #[test]
    fn late_join_window_rejects_invalid_bounds() {
        assert!(hls_window_end_ms(10, 2_000, Some(0), 5).is_err());
        assert!(hls_window_end_ms(10, 8_000, Some(3), 5).is_err());
        assert!(hls_window_end_ms(10, 2_001, Some(1), 5).is_err());
        assert!(hls_window_end_ms(u64::MAX, 0, Some(1), 5).is_err());
    }

    #[test]
    fn h3_late_join_preload_tracks_the_requested_window() {
        let session_id = 1_000_000_000_000;
        assert_eq!(h3_preload_at_ns(session_id, 0), session_id - 100_000_000);
        assert_eq!(
            h3_preload_at_ns(session_id, 36_000),
            session_id + 35_900_000_000
        );
    }

    #[test]
    fn randomized_reader_arrivals_are_part_aligned_and_reproducible() {
        let offsets = (0..64)
            .map(|reader_id| randomized_reader_start_offset_ms(1_000, 5, 250, reader_id, 91))
            .collect::<Vec<_>>();
        assert!(offsets
            .iter()
            .all(|offset| { (1_000..=1_250).contains(offset) && (offset - 1_000) % 5 == 0 }));
        assert!(offsets.windows(2).any(|pair| pair[0] != pair[1]));
        assert_eq!(
            offsets,
            (0..64)
                .map(|reader_id| {
                    randomized_reader_start_offset_ms(1_000, 5, 250, reader_id, 91)
                })
                .collect::<Vec<_>>()
        );
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
        assert_eq!(
            hls_path("/live", 24_001, "part3984.mp4"),
            "/live/24001/part3984.mp4"
        );
        assert_eq!(hls_path("", 24_001, "stream.m3u8"), "/24001/stream.m3u8");
        assert_eq!(hls_path("", 24_001, "p72.mp4"), "/24001/p72.mp4");
    }

    #[test]
    fn playlist_object_window_uses_part_uris_not_media_sequence() {
        let playlist = b"#EXTM3U\n\
#EXT-X-MEDIA-SEQUENCE:996\n\
#EXT-X-PART:DURATION=0.005,URI=\"part3982.mp4\",INDEPENDENT=YES\n\
#EXT-X-PART:DURATION=0.005,URI=\"part3983.mp4\",INDEPENDENT=YES\n\
#EXT-X-PRELOAD-HINT:TYPE=PART,URI=\"part3984.mp4\"\n";

        assert_eq!(
            hls_playlist_object_window(playlist).unwrap(),
            Some(HlsPlaylistObjectWindow {
                first_sequence: Some(3_982),
                last_sequence: Some(3_983),
                next_sequence: 3_984,
            })
        );
    }

    #[test]
    fn playlist_object_window_supports_direct_origin_part_names() {
        let playlist = b"#EXTM3U\n\
#EXT-X-PART:DURATION=0.005,URI=\"tracks/p71.mp4?token=test\"\n\
#EXT-X-PRELOAD-HINT:TYPE=PART,URI=\"p72.mp4\"\n";

        assert_eq!(
            hls_playlist_object_window(playlist).unwrap(),
            Some(HlsPlaylistObjectWindow {
                first_sequence: Some(71),
                last_sequence: Some(71),
                next_sequence: 72,
            })
        );
    }

    #[test]
    fn playlist_object_window_reads_a_preload_only_baseline() {
        let playlist = b"#EXTM3U\n#EXT-X-PRELOAD-HINT:TYPE=PART,URI=\"part3984.mp4\"\n";

        assert_eq!(
            hls_playlist_object_window(playlist).unwrap(),
            Some(HlsPlaylistObjectWindow {
                first_sequence: None,
                last_sequence: None,
                next_sequence: 3_984,
            })
        );
    }

    #[test]
    fn playlist_baseline_preserves_the_first_part_after_an_empty_snapshot() {
        let window = HlsPlaylistObjectWindow {
            first_sequence: Some(3_984),
            last_sequence: Some(3_985),
            next_sequence: 3_986,
        };

        assert_eq!(initial_hls_object_sequence(window, false), 3_986);
        assert_eq!(initial_hls_object_sequence(window, true), 3_984);
        assert_eq!(refreshed_hls_object_sequence(0, window), 3_984);
        assert_eq!(refreshed_hls_object_sequence(3_984, window), 3_984);
    }

    #[test]
    fn playlist_object_window_rejects_a_receding_preload_hint() {
        let playlist = b"#EXTM3U\n\
#EXT-X-PART:DURATION=0.005,URI=\"part3984.mp4\"\n\
#EXT-X-PRELOAD-HINT:TYPE=PART,URI=\"part3983.mp4\"\n";

        assert!(hls_playlist_object_window(playlist).is_err());
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
    fn opaque_flac_uses_binary_parts_without_fmp4_metadata() {
        assert_eq!(HlsAudioCodec::OpaqueFlac.media_extension(), "bin");
        assert_eq!(HlsAudioCodec::OpaqueFlac.init_marker(), None);
        assert_eq!(
            media_part_pts_ms(
                HlsAudioCodec::OpaqueFlac,
                &[0xff, 0xf8],
                3_991,
                3_984,
                0,
                50,
            )
            .unwrap(),
            350
        );
        assert!(media_part_pts_ms(
            HlsAudioCodec::OpaqueFlac,
            &[0xff, 0xf8],
            3_983,
            3_984,
            0,
            50,
        )
        .is_err());
        assert!(opaque_flac_part_is_valid(&[0xff, 0xf8, 0x01]));
        assert!(opaque_flac_part_is_valid(&[0xff, 0xf9, 0x01]));
        assert!(!opaque_flac_part_is_valid(b"fLaC"));

        let playlist = b"#EXTM3U\n#EXT-X-PART:DURATION=0.05,URI=\"part7.bin\"\n\
#EXT-X-SERVER-CONTROL:CAN-BLOCK-RELOAD=YES\n";
        assert!(playlist_matches_format(playlist, HlsAudioCodec::OpaqueFlac));
    }

    #[test]
    fn generated_pcm_is_exact_stereo_s24le_geometry() {
        assert_eq!(
            sine_s24le(0).len(),
            FRAME_COUNT as usize * usize::from(DEFAULT_CHANNELS) * 3
        );
    }

    #[test]
    fn raw_pcm_source_loops_frames_and_selects_channel_groups() {
        let source = LoopingPcm::new(
            vec![
                0, 0, 0, 1, 1, 1, 2, 2, 2, 10, 10, 10, 11, 11, 11, 12, 12, 12,
            ],
            3,
        )
        .unwrap();
        let group = source.group_s24le(1, FRAME_COUNT, 1, 2);

        assert_eq!(group.len(), FRAME_COUNT as usize * 2 * 3);
        assert_eq!(&group[..12], &[11, 11, 11, 12, 12, 12, 1, 1, 1, 2, 2, 2]);
        assert_eq!(&group[12..18], &[11, 11, 11, 12, 12, 12]);
    }

    #[test]
    fn raw_pcm_source_rejects_empty_or_partial_frames() {
        assert!(LoopingPcm::new(Vec::new(), 2).is_err());
        assert!(LoopingPcm::new(vec![0; 7], 2).is_err());
    }

    #[test]
    fn flac_selection_counts_the_opus_companion_without_selecting_it() {
        let mut state = ReceiveFormatState::new(&[ReceiveAudioFormat::Flac]).unwrap();
        for encoding in [EncodingFlag::Opus, EncodingFlag::FLAC] {
            let encrypted = encoding == EncodingFlag::Opus;
            let payload = [7_u8; 12];
            let header = FrameHeaderV2::new(
                encoding,
                payload.len() as u32,
                FRAME_COUNT,
                SAMPLE_RATE,
                2,
                24,
                frame_header::Endianness::LittleEndian,
                None,
                None,
                None,
            )
            .unwrap();
            let header = if encrypted {
                header
                    .with_packet_flags(FrameHeaderV2::FLAG_ENCRYPTED)
                    .unwrap()
            } else {
                header
            }
            .with_packet_crc32(&payload)
            .unwrap();
            let mut packet = Vec::with_capacity(header.size() + payload.len());
            header.encode(&mut packet).unwrap();
            packet.extend_from_slice(&payload);
            state.observe(
                &DecodedMultichannelAudioGroup {
                    session_id: 1_000_000_000,
                    config_generation: 1,
                    epoch_id: 0,
                    pts_samples: 0,
                    sample_rate: SAMPLE_RATE,
                    frame_count: FRAME_COUNT,
                    group_count: 1,
                    group_id: 0,
                    group_index: 0,
                    channel_start: 0,
                    channel_count: 2,
                    payload_kind: AudioPayloadKind::SoundKitV2,
                    sample_format: AudioSampleFormat::Unspecified,
                    flags: 0,
                    payload: Bytes::from(packet),
                    raptorq_recovered_fragments: 1,
                },
                1_001_000_000,
                25,
                &ReceiveWindow::new(1_000_000_000, 0, 10, 0),
            );
        }

        assert_eq!(state.selected_epoch_count(), 1);
        assert_eq!(state.opus_epochs.len(), 1);
        assert_eq!(state.flac_epochs.len(), 1);
        assert_eq!(state.received_pcm_frames, u64::from(FRAME_COUNT));
        assert_eq!(state.duplicate_or_late_epochs, 0);
    }

    #[test]
    fn reference_alignment_skips_out_of_range_negative_lags() {
        let reference = (0..24_000)
            .map(|index| ((index as f64 * 0.013).sin() * 12_000.0) as i16)
            .collect::<Vec<_>>();
        let best =
            best_correlation_in_lag_range(&reference, &reference, 1, 0, 24_000, -16, 16, 1, 1)
                .expect("zero-lag reference should align even when early negative lags underflow");

        assert_eq!(best.0, 0);
        assert!(best.1 > 0.99);
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
    fn receive_group_plan_uses_one_subscription_for_consecutive_groups() {
        assert_eq!(receive_group_ids(40, 4).unwrap(), vec![40, 41, 42, 43]);
        assert_eq!(receive_group_ids(u16::MAX, 1).unwrap(), vec![u16::MAX]);
        assert!(receive_group_ids(0, 0).is_err());
        assert!(receive_group_ids(u16::MAX, 2).is_err());
    }

    #[test]
    fn decorrelated_multichannel_signal_has_expected_s24le_geometry() {
        let pcm = signal_s24le(0, FRAME_COUNT, 8, 16, ProbeSignal::Decorrelated);
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
    fn fmp4_pts_is_independent_of_retained_object_sequence() {
        let part = [0, 0, 0, 16, b't', b'f', b'd', b't', 0, 0, 0, 0, 0, 0, 0, 50];
        let pts_ms = media_part_pts_ms(HlsAudioCodec::Flac, &part, 3_984, 3_984, 0, 5).unwrap();
        let mut coverage = PartCoverage::new(10, 1, 5).unwrap();

        assert_eq!(pts_ms, 50);
        assert!(coverage.insert_pts(pts_ms));
    }

    #[test]
    fn fmp4_coverage_uses_object_position_after_a_short_boundary_part() {
        let mut coverage = PartCoverage::new(0, 2_400, 250).unwrap();
        let media_pts_ms = 597_755;
        let object_sequence = 2_392;

        assert!(!coverage.insert_pts(media_pts_ms));
        let coverage_pts_ms = nominal_hls_object_pts_ms(object_sequence, 0, 0, 250).unwrap();
        assert_eq!(coverage_pts_ms, 598_000);
        assert!(coverage.insert_pts(coverage_pts_ms));
    }

    #[test]
    fn media_timeline_accepts_a_contiguous_short_boundary_part() {
        let mut timeline = MediaTimelineCoverage::new(10, 3, 0, 610);
        timeline.insert(10, 0, 250);
        timeline.insert(11, 250, 110);
        timeline.insert(12, 360, 250);

        assert_eq!(timeline.discontinuities(), 0);
    }

    #[test]
    fn media_timeline_rejects_missing_media_time_inside_complete_objects() {
        let mut timeline = MediaTimelineCoverage::new(10, 3, 0, 615);
        timeline.insert(10, 0, 250);
        timeline.insert(11, 250, 110);
        timeline.insert(12, 365, 250);

        assert_eq!(timeline.discontinuities(), 1);
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
