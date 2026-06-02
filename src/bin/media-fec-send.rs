use anyhow::{bail, Context, Result};
use av_contrib::{codec_name, infer_media_codec};
use clap::{Parser, ValueEnum};
use raptorq_datagram_fec::{
    MediaCodec, MediaFecEncoder, MediaFrame, MediaFrameFlags, MediaFrameMetadata,
};
use std::net::SocketAddr;
use tokio::io::{self, AsyncReadExt};
use tokio::net::UdpSocket;

#[derive(Debug, Parser)]
#[command(
    name = "media-fec-send",
    about = "Send one non-TS media access unit to an av-mesh media UDP-FEC ingest socket"
)]
struct Args {
    target: SocketAddr,

    #[arg(long, default_value_t = 1)]
    stream_id: u64,

    #[arg(long, default_value_t = 0)]
    sequence: u64,

    #[arg(long, default_value_t = 0)]
    pts_ms: u64,

    #[arg(long)]
    dts_ms: Option<u64>,

    #[arg(long, default_value_t = 0)]
    duration_ms: u32,

    #[arg(long, value_enum, default_value = "auto")]
    codec: CodecArg,

    #[arg(long)]
    keyframe: bool,

    #[arg(long)]
    codec_config: bool,

    #[arg(long)]
    discontinuity: bool,

    #[arg(long)]
    end_of_stream: bool,
}

#[derive(Debug, Clone, Copy, ValueEnum)]
enum CodecArg {
    Auto,
    Unknown,
    H264,
    Opus,
    Aac,
    Data,
}

impl From<CodecArg> for MediaCodec {
    fn from(value: CodecArg) -> Self {
        match value {
            CodecArg::Auto => Self::Data,
            CodecArg::Unknown => Self::Unknown,
            CodecArg::H264 => Self::H264,
            CodecArg::Opus => Self::Opus,
            CodecArg::Aac => Self::Aac,
            CodecArg::Data => Self::Data,
        }
    }
}

impl CodecArg {
    fn resolve(self, payload: &[u8]) -> MediaCodec {
        match self {
            Self::Auto => infer_media_codec(payload),
            _ => self.into(),
        }
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    if args.duration_ms > u32::from(u16::MAX) {
        bail!("--duration-ms must fit in u16 for media-FEC headers");
    }

    let mut input = Vec::new();
    io::stdin()
        .read_to_end(&mut input)
        .await
        .context("failed to read stdin")?;

    let mut flags = MediaFrameFlags::default();
    if args.keyframe {
        flags = flags.with(MediaFrameFlags::KEYFRAME);
    }
    if args.codec_config {
        flags = flags.with(MediaFrameFlags::CODEC_CONFIG);
    }
    if args.discontinuity {
        flags = flags.with(MediaFrameFlags::DISCONTINUITY);
    }
    if args.end_of_stream {
        flags = flags.with(MediaFrameFlags::END_OF_STREAM);
    }

    let codec = args.codec.resolve(&input);
    let mut metadata = MediaFrameMetadata::new(args.stream_id, args.sequence, args.pts_ms, codec);
    metadata.dts_ms = args.dts_ms;
    metadata.duration_ms = args.duration_ms;
    metadata.flags = flags;

    let mut encoder = MediaFecEncoder::default();
    let encoded = encoder
        .encode_frame(MediaFrame {
            metadata,
            payload: &input,
        })
        .context("failed to encode media access unit with RaptorQ-FEC")?;

    let socket = UdpSocket::bind("0.0.0.0:0")
        .await
        .context("failed to bind UDP sender socket")?;
    for datagram in &encoded.datagrams {
        socket
            .send_to(datagram, args.target)
            .await
            .with_context(|| format!("failed to send media-FEC datagram to {}", args.target))?;
    }

    println!(
        "sent {} bytes as stream {} sequence {} codec {} to {} using {} media-FEC datagrams",
        input.len(),
        args.stream_id,
        args.sequence,
        codec_name(codec),
        args.target,
        encoded.datagrams.len()
    );
    Ok(())
}
