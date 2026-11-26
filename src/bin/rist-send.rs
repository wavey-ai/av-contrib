use anyhow::{Context, Result};
use clap::{Parser, ValueEnum};
use rist_core_pure::packet::gre::{BufferNegotiation, GreKeepalive};
use rist_core_pure::time::ntp_now;
use rist_core_pure::{
    CongestionControlMode, RecoveryConfig, RecoveryTuning, SenderStats, MAX_ADDRESSABLE_HISTORY,
};
use rist_mio_pure::{MainMioSender, SimpleMioSender};
use std::io;
use std::net::{Ipv4Addr, Ipv6Addr, SocketAddr};
use std::time::{Duration, Instant};
use tokio::io::{self as tokio_io, AsyncReadExt};

const DEFAULT_FLOW_ID: u32 = 0x1122_3344;
const INITIAL_UDP_RETRY_DELAY: Duration = Duration::from_millis(1);
const MAX_UDP_RETRY_DELAY: Duration = Duration::from_millis(32);
/// rist-core's default buffer (`RecoveryConfig::default()`), which the
/// product ingest runs with.
const DEFAULT_RIST_BUFFER_MS: u64 = 1_000;
/// The sender socket is not registered with a reactor, so feedback (NACKs,
/// echo requests) that arrives while stdin is quiet is read at least this
/// often. While data flows, the send path also reads it every 32 packets.
const FEEDBACK_POLL_INTERVAL: Duration = Duration::from_millis(1);
const KEEPALIVE_MAC: [u8; 6] = [1, 2, 3, 4, 5, 6];
/// What the sender tells a Main receiver its current buffer is, in ms.
const NEGOTIATED_RECEIVER_BUFFER_MS: u16 = 250;

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

    /// Initial send-history size in packets. The history keeps every packet
    /// for at least the buffer plus 2 x rtt_max, growing up to the 16-bit
    /// NACK horizon (32 768 packets); a larger value is kept as a fixed size.
    #[arg(long, default_value_t = 8192)]
    history_packets: usize,

    /// RIST buffer in ms (librist `buffer=`): how long the sender keeps and
    /// repairs a packet. Use the receiver's buffer.
    #[arg(
        long,
        default_value_t = DEFAULT_RIST_BUFFER_MS,
        value_parser = clap::value_parser!(u64).range(1..=60_000)
    )]
    rist_buffer_ms: u64,

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
        sender.configure_recovery(args.rist_buffer_ms);
        sender.prime_session(args.rist_buffer_ms)?;
        Ok(sender)
    }

    /// The buffer the receiver runs with, and a history kept by time, so a
    /// repair is never refused for want of the packet while it can still
    /// arrive in time.
    fn configure_recovery(&mut self, buffer_ms: u64) {
        let (recovery, tuning) = recovery_settings(buffer_ms);
        match self {
            Self::Simple(sender) => {
                sender.set_recovery_config(recovery, CongestionControlMode::default());
                sender.set_recovery_tuning(tuning);
            }
            Self::Main(sender) => {
                sender.set_recovery_config(recovery, CongestionControlMode::default());
                sender.set_recovery_tuning(tuning);
            }
        }
    }

    fn prime_session(&mut self, buffer_ms: u64) -> io::Result<()> {
        if let Self::Main(sender) = self {
            let keepalive = GreKeepalive::librist_default(KEEPALIVE_MAC);
            sender.send_keepalive(keepalive)?;
            // From here send_payload and service() send keepalives when due.
            sender.set_auto_keepalive(keepalive);
            let sender_buffer_ms = u16::try_from(buffer_ms).unwrap_or(u16::MAX);
            sender.send_buffer_negotiation(BufferNegotiation::session(
                sender_buffer_ms,
                NEGOTIATED_RECEIVER_BUFFER_MS.min(sender_buffer_ms),
            ))?;
        }
        // The first SR and echo request go out now, not a period later.
        let mut buf = vec![0u8; 2048];
        self.service(Instant::now(), &mut buf)?;
        std::thread::sleep(Duration::from_millis(20));
        Ok(())
    }

    /// Sends one payload, straight from its history slot. `WouldBlock` means
    /// the send queue was full and nothing was committed, so it is safe to
    /// retry; the SR/echo request and keepalive go out with the data when due.
    fn send_payload(&mut self, payload: &[u8], now: Instant) -> io::Result<()> {
        match self {
            Self::Simple(sender) => sender
                .send_payload_meta(payload, ntp_now(), now)
                .map(|_| ()),
            Self::Main(sender) => sender
                .send_payload_meta(payload, ntp_now(), now)
                .map(|_| ()),
        }
    }

    /// Flushes queued datagrams, serves feedback (NACKs repaired, echo
    /// requests answered) and sends the SR/echo request, keepalive and
    /// scheduled repairs that are due. Returns when it wants to be called
    /// again.
    fn service(&mut self, now: Instant, buf: &mut [u8]) -> io::Result<Option<Instant>> {
        match self {
            Self::Simple(sender) => sender.service(now, ntp_now(), buf),
            Self::Main(sender) => sender.service(now, ntp_now(), buf),
        }
    }

    fn next_timeout(&self) -> Option<Instant> {
        match self {
            Self::Simple(sender) => sender.next_timeout(),
            Self::Main(sender) => sender.next_timeout(),
        }
    }

    fn stats(&self) -> SenderStats {
        match self {
            Self::Simple(sender) => sender.stats(),
            Self::Main(sender) => sender.stats(),
        }
    }
}

fn recovery_settings(buffer_ms: u64) -> (RecoveryConfig, RecoveryTuning) {
    let buffer = Duration::from_millis(buffer_ms);
    let recovery = RecoveryConfig {
        length_min: buffer,
        length_max: buffer,
        ..RecoveryConfig::default()
    };
    let tuning = RecoveryTuning {
        history_by_time: true,
        max_history_packets: MAX_ADDRESSABLE_HISTORY,
        ..RecoveryTuning::default()
    };
    (recovery, tuning)
}

/// Serves the sender and returns when to serve it next: its own deadline,
/// but no later than the feedback poll interval.
fn service_sender(sender: &mut Sender, buf: &mut [u8]) -> io::Result<Instant> {
    let now = Instant::now();
    let poll_at = now + FEEDBACK_POLL_INTERVAL;
    match sender.service(now, buf) {
        Ok(due) => Ok(due.map_or(poll_at, |due| due.min(poll_at))),
        Err(error) if is_transient_udp_error(&error) => Ok(now + INITIAL_UDP_RETRY_DELAY),
        Err(error) => Err(error),
    }
}

/// Sends one payload, serving the sender while its send queue is full.
async fn send_payload(sender: &mut Sender, payload: &[u8], buf: &mut [u8]) -> io::Result<()> {
    retry_would_block(|attempt| {
        if attempt > 0 {
            service_sender(sender, buf)?;
        }
        sender.send_payload(payload, Instant::now())
    })
    .await
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
    let mut filled = 0usize;
    let mut sent_bytes = 0usize;
    let mut next_service = Instant::now();

    loop {
        if Instant::now() >= next_service {
            next_service = service_sender(&mut sender, &mut feedback_buf)
                .context("failed to serve RIST sender")?;
        }
        tokio::select! {
            biased;
            // Cancel safe: no bytes are lost when the timer wins.
            read = input.read(&mut chunk[filled..]) => {
                let read = read.context("failed to read stdin")?;
                filled += read;
                let eof = read == 0;
                if filled == chunk_bytes || (eof && filled > 0) {
                    send_payload(&mut sender, &chunk[..filled], &mut feedback_buf)
                        .await
                        .context("failed to send RIST payload")?;
                    sent_bytes += filled;
                    filled = 0;
                    // The send path read feedback it could not finish.
                    if sender.next_timeout().is_some_and(|at| at < next_service) {
                        next_service = Instant::now();
                    }
                }
                if eof {
                    break;
                }
            }
            _ = tokio::time::sleep_until(next_service.into()) => {}
        }
    }

    let repair_deadline = Instant::now() + Duration::from_millis(args.final_repair_ms);
    loop {
        let next = service_sender(&mut sender, &mut feedback_buf)
            .context("failed to serve RIST sender after EOF")?;
        if Instant::now() >= repair_deadline {
            break;
        }
        tokio::time::sleep_until(next.min(repair_deadline).into()).await;
    }

    let stats = sender.stats();
    println!(
        "sent {} bytes to {} using RIST chunks of {} bytes",
        sent_bytes, args.target, chunk_bytes
    );
    eprintln!(
        "rist-send: packets {} retransmitted {} nack_requests {} refused_not_in_history {} \
         refused_too_old {} echo_responses {} rtt_us {}",
        stats.sent_packets,
        stats.retransmitted_packets,
        stats.nack_requests,
        stats.retransmit_refused_not_in_history,
        stats.retransmit_refused_too_old,
        stats.echo_responses,
        stats
            .rtt_micros
            .map_or_else(|| "-".to_owned(), |rtt| rtt.to_string()),
    );
    Ok(())
}

/// Runs `operation` until it does not return `WouldBlock`, backing off
/// between attempts. `WouldBlock` from a send means nothing was committed (a
/// full send queue; rist-mio reports `ENOBUFS` the same way), so a retry
/// cannot duplicate a sequence. The attempt number is passed in.
async fn retry_would_block<T>(mut operation: impl FnMut(u32) -> io::Result<T>) -> io::Result<T> {
    let mut delay = INITIAL_UDP_RETRY_DELAY;
    let mut attempt = 0u32;
    loop {
        match operation(attempt) {
            Ok(value) => return Ok(value),
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                tokio::time::sleep(delay).await;
                delay = delay.saturating_mul(2).min(MAX_UDP_RETRY_DELAY);
                attempt = attempt.saturating_add(1);
            }
            Err(error) => return Err(error),
        }
    }
}

fn is_transient_udp_error(error: &io::Error) -> bool {
    matches!(
        error.kind(),
        io::ErrorKind::WouldBlock | io::ErrorKind::Interrupted
    ) || is_enobufs(error.raw_os_error())
}

#[cfg(any(
    target_vendor = "apple",
    target_os = "dragonfly",
    target_os = "freebsd",
    target_os = "netbsd",
    target_os = "openbsd"
))]
fn is_enobufs(raw_os_error: Option<i32>) -> bool {
    raw_os_error == Some(55)
}

#[cfg(any(target_os = "android", target_os = "linux"))]
fn is_enobufs(raw_os_error: Option<i32>) -> bool {
    raw_os_error == Some(105)
}

#[cfg(windows)]
fn is_enobufs(raw_os_error: Option<i32>) -> bool {
    raw_os_error == Some(10_055)
}

#[cfg(not(any(
    target_vendor = "apple",
    target_os = "android",
    target_os = "dragonfly",
    target_os = "freebsd",
    target_os = "linux",
    target_os = "netbsd",
    target_os = "openbsd",
    windows
)))]
fn is_enobufs(_raw_os_error: Option<i32>) -> bool {
    false
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::cell::Cell;

    #[tokio::test]
    async fn retries_would_block_until_success() {
        let attempts = Cell::new(0);
        let result = retry_would_block(|attempt| {
            attempts.set(attempts.get() + 1);
            assert_eq!(attempt + 1, attempts.get());
            if attempt < 2 {
                Err(io::Error::from(io::ErrorKind::WouldBlock))
            } else {
                Ok(42)
            }
        })
        .await
        .unwrap();

        assert_eq!(result, 42);
        assert_eq!(attempts.get(), 3);
    }

    #[tokio::test]
    async fn returns_fatal_errors_without_retry() {
        let attempts = Cell::new(0);
        let error = retry_would_block(|_| {
            attempts.set(attempts.get() + 1);
            Err::<(), _>(io::Error::from(io::ErrorKind::NetworkUnreachable))
        })
        .await
        .unwrap_err();

        assert_eq!(error.kind(), io::ErrorKind::NetworkUnreachable);
        assert_eq!(attempts.get(), 1);
    }

    #[test]
    fn default_buffer_is_rist_core_default() {
        assert_eq!(
            Duration::from_millis(DEFAULT_RIST_BUFFER_MS),
            RecoveryConfig::default().length_max
        );
        let (recovery, tuning) = recovery_settings(DEFAULT_RIST_BUFFER_MS);
        assert_eq!(recovery, RecoveryConfig::default());
        assert!(tuning.history_by_time);
        assert_eq!(tuning.max_history_packets, MAX_ADDRESSABLE_HISTORY);
    }

    #[test]
    fn buffer_flag_sets_both_recovery_lengths() {
        let args =
            Args::try_parse_from(["rist-send", "127.0.0.1:27000", "--rist-buffer-ms", "250"])
                .unwrap();
        let (recovery, _) = recovery_settings(args.rist_buffer_ms);
        assert_eq!(recovery.length_min, Duration::from_millis(250));
        assert_eq!(recovery.length_max, Duration::from_millis(250));
        assert!(
            Args::try_parse_from(["rist-send", "127.0.0.1:27000", "--rist-buffer-ms", "0"])
                .is_err()
        );
    }

    #[test]
    fn recognizes_platform_enobufs() {
        let raw_os_error = if cfg!(target_vendor = "apple")
            || cfg!(any(
                target_os = "dragonfly",
                target_os = "freebsd",
                target_os = "netbsd",
                target_os = "openbsd"
            )) {
            55
        } else if cfg!(any(target_os = "android", target_os = "linux")) {
            105
        } else if cfg!(windows) {
            10_055
        } else {
            return;
        };

        assert!(is_transient_udp_error(&io::Error::from_raw_os_error(
            raw_os_error
        )));
    }
}
