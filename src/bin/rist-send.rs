use anyhow::{Context, Result};
use clap::{Parser, ValueEnum};
use rist_core_pure::packet::gre::{BufferNegotiation, GreKeepalive};
use rist_core_pure::time::ntp_now;
use rist_mio_pure::{MainMioSender, SimpleMioSender};
use std::io;
use std::net::{Ipv4Addr, Ipv6Addr, SocketAddr};
use std::time::{Duration, Instant};
use tokio::io::{self as tokio_io, AsyncReadExt};

const DEFAULT_FLOW_ID: u32 = 0x1122_3344;

#[derive(Debug, Clone, Copy, ValueEnum)]
enum RistProfile {
    Simple,
    Main,
}

#[derive(Debug, Parser)]
#[command(
    name = "rist-send",
    about = "Send stdin to an av-mesh RIST ingest socket"
)]
struct Args {
    target: SocketAddr,

    #[arg(long, value_enum, default_value = "main")]
    profile: RistProfile,

    #[arg(long, value_parser = parse_u32_auto, default_value_t = DEFAULT_FLOW_ID)]
    flow_id: u32,

    #[arg(long, default_value_t = 1316)]
    chunk_bytes: usize,

    #[arg(long, default_value_t = 8192)]
    history_packets: usize,

    /// Keep the sender alive after EOF so the receiver can request final repairs.
    #[arg(long, default_value_t = 250)]
    final_repair_ms: u64,
}

#[allow(clippy::large_enum_variant)]
enum Sender {
    Simple(SimpleMioSender),
    Main(MainMioSender),
}

impl Sender {
    fn connect(args: &Args) -> io::Result<Self> {
        let local = local_sender_addr(args.target);
        let mut sender = match args.profile {
            RistProfile::Simple => {
                SimpleMioSender::connect(local, args.target, args.flow_id, args.history_packets)
                    .map(Self::Simple)
            }
            RistProfile::Main => {
                MainMioSender::connect(local, args.target, args.flow_id, args.history_packets)
                    .map(Self::Main)
            }
        }?;
        sender.prime_session()?;
        Ok(sender)
    }

    fn prime_session(&mut self) -> io::Result<()> {
        let Self::Main(sender) = self else {
            return Ok(());
        };

        sender.send_keepalive(GreKeepalive::librist_default([1, 2, 3, 4, 5, 6]))?;
        sender.send_buffer_negotiation(BufferNegotiation::session(1000, 250))?;
        let now = Instant::now();
        sender.poll_rtcp_and_send(now, ntp_now())?;
        sender.poll_rtcp_and_send(now + Duration::from_secs(1), ntp_now())?;
        std::thread::sleep(Duration::from_millis(20));
        Ok(())
    }

    fn send_payload(&mut self, payload: &[u8]) -> io::Result<()> {
        match self {
            Self::Simple(sender) => sender
                .send_payload(payload, ntp_now(), Instant::now())
                .map(|_| ()),
            Self::Main(sender) => sender
                .send_payload(payload, ntp_now(), Instant::now())
                .map(|_| ()),
        }
    }

    fn drain_feedback(&mut self, buf: &mut [u8]) -> io::Result<()> {
        for _ in 0..32 {
            match self {
                Self::Simple(sender) => match sender.try_recv_feedback_and_retransmit(buf) {
                    Ok(Some(_)) => {}
                    Ok(None) => return Ok(()),
                    Err(error) if error.kind() == io::ErrorKind::WouldBlock => return Ok(()),
                    Err(error) => return Err(error),
                },
                Self::Main(sender) => match sender.try_recv_feedback_and_retransmit(buf) {
                    Ok(Some(_)) => {}
                    Ok(None) => return Ok(()),
                    Err(error) if error.kind() == io::ErrorKind::WouldBlock => return Ok(()),
                    Err(error) => return Err(error),
                },
            }
        }
        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    let mut input = tokio_io::stdin();

    let mut sender = Sender::connect(&args)
        .with_context(|| format!("failed to create RIST sender for {}", args.target))?;
    let mut feedback_buf = vec![0u8; 65_536];
    let chunk_bytes = args.chunk_bytes.max(1);
    let mut chunk = vec![0u8; chunk_bytes];
    let mut sent_bytes = 0usize;

    loop {
        let read = read_chunk(&mut input, &mut chunk).await?;
        if read == 0 {
            break;
        }
        let payload = &chunk[..read];
        loop {
            match sender.send_payload(payload) {
                Ok(()) => break,
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                    sender.drain_feedback(&mut feedback_buf)?;
                    tokio::task::yield_now().await;
                }
                Err(error) => return Err(error).context("failed to send RIST payload"),
            }
        }
        sender.drain_feedback(&mut feedback_buf)?;
        sent_bytes += read;
    }

    let repair_deadline = Instant::now() + Duration::from_millis(args.final_repair_ms);
    while Instant::now() < repair_deadline {
        sender.drain_feedback(&mut feedback_buf)?;
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
    sender.drain_feedback(&mut feedback_buf)?;

    println!(
        "sent {} bytes to {} using RIST chunks of {} bytes",
        sent_bytes, args.target, chunk_bytes
    );
    Ok(())
}

async fn read_chunk<R>(input: &mut R, chunk: &mut [u8]) -> Result<usize>
where
    R: tokio_io::AsyncRead + Unpin,
{
    let mut filled = 0usize;
    while filled < chunk.len() {
        let read = input
            .read(&mut chunk[filled..])
            .await
            .context("failed to read stdin")?;
        if read == 0 {
            break;
        }
        filled += read;
    }
    Ok(filled)
}

fn local_sender_addr(peer: SocketAddr) -> SocketAddr {
    match peer {
        SocketAddr::V4(addr) => {
            let ip = if addr.ip().is_loopback() {
                Ipv4Addr::LOCALHOST
            } else {
                Ipv4Addr::UNSPECIFIED
            };
            SocketAddr::new(ip.into(), 0)
        }
        SocketAddr::V6(addr) => {
            let ip = if addr.ip().is_loopback() {
                Ipv6Addr::LOCALHOST
            } else {
                Ipv6Addr::UNSPECIFIED
            };
            SocketAddr::new(ip.into(), 0)
        }
    }
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
