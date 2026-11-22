//! Several UDP datagrams handed to the kernel in one system call.
//!
//! On Linux a batch leaves with `sendmmsg`: the RaptorQ symbols of one part
//! cost one call per lane instead of one awaited `send` each. Elsewhere, and
//! when batching is switched off, every datagram is one `send`/`send_to`.
//! A datagram is never split or merged: the receiver sees exactly the
//! datagrams that a loop of single sends would have produced, in order.

use std::io;
use std::net::SocketAddr;
use tokio::net::UdpSocket;

/// Messages handed to the kernel in one `sendmmsg` call. A part of a few
/// hundred kB has more symbols than this and takes several calls.
pub const MESSAGES_PER_CALL: usize = 64;

/// One datagram of a batch. `target` is `None` on a connected socket.
#[derive(Debug, Clone, Copy)]
pub struct Outbound<'a> {
    pub target: Option<SocketAddr>,
    pub bytes: &'a [u8],
}

impl<'a> Outbound<'a> {
    pub const fn connected(bytes: &'a [u8]) -> Self {
        Self {
            target: None,
            bytes,
        }
    }

    pub const fn to(target: SocketAddr, bytes: &'a [u8]) -> Self {
        Self {
            target: Some(target),
            bytes,
        }
    }
}

/// Send every datagram of `messages` in order. With `batch` (and on Linux)
/// up to [`MESSAGES_PER_CALL`] datagrams leave in one `sendmmsg` call; the
/// task waits for the socket only when the kernel's buffer is full. Returns
/// the number of system calls made, or the first error: the datagrams
/// before the failed one have been sent, the ones after it have not.
pub async fn send_all(
    socket: &UdpSocket,
    messages: &[Outbound<'_>],
    batch: bool,
) -> io::Result<usize> {
    send_until_refused(socket, messages, batch)
        .await
        .map_err(|stopped| stopped.error)
}

/// [`send_all`] that says, on an error, how many datagrams went before it.
pub async fn send_all_until_refused(
    socket: &UdpSocket,
    messages: &[Outbound<'_>],
    batch: bool,
) -> Result<usize, Stopped> {
    send_until_refused(socket, messages, batch).await
}

/// A batch that stopped at a datagram the kernel refused.
#[derive(Debug)]
pub struct Stopped {
    /// Datagrams sent before the refused one: the refused one's index.
    pub sent: usize,
    /// System calls made.
    pub calls: usize,
    pub error: io::Error,
}

async fn send_until_refused(
    socket: &UdpSocket,
    messages: &[Outbound<'_>],
    batch: bool,
) -> Result<usize, Stopped> {
    #[cfg(target_os = "linux")]
    if batch {
        return linux::send_all(socket, messages).await;
    }
    let _ = batch;
    send_each_until_refused(socket, messages).await
}

/// The outcome of [`send_all_per_target`].
#[derive(Debug, Default)]
pub struct PerTargetOutcome {
    /// System calls made.
    pub calls: usize,
    /// Datagrams handed to the kernel.
    pub sent: usize,
    /// Each target that the kernel refused a datagram for, with the error.
    /// It was sent none of its later datagrams. `None` is the peer of a
    /// connected socket.
    pub refused: Vec<(Option<SocketAddr>, io::Error)>,
}

impl PerTargetOutcome {
    pub fn refused(&self, target: SocketAddr) -> bool {
        self.refused
            .iter()
            .any(|(refused, _)| *refused == Some(target))
    }
}

/// [`send_all`] for a batch to several targets on one unconnected socket,
/// with each target's errors kept to that target. A datagram that the
/// kernel refuses at once (no route to its address, a firewall rule, an
/// invalid port) stops the datagrams of its target only: the batch goes on
/// with the datagrams of the other targets, in order. A full socket buffer
/// is waited for as in [`send_all`], never taken as a refusal.
pub async fn send_all_per_target(
    socket: &UdpSocket,
    messages: &[Outbound<'_>],
    batch: bool,
) -> PerTargetOutcome {
    let mut outcome = PerTargetOutcome::default();
    let mut remaining = std::borrow::Cow::Borrowed(messages);
    loop {
        match send_until_refused(socket, &remaining, batch).await {
            Ok(calls) => {
                outcome.calls += calls;
                outcome.sent += remaining.len();
                return outcome;
            }
            Err(stopped) => {
                outcome.calls += stopped.calls;
                outcome.sent += stopped.sent;
                let target = remaining[stopped.sent].target;
                outcome.refused.push((target, stopped.error));
                // Refusals are rare: a new list of what is left is cheaper
                // than a check on every datagram of every batch.
                remaining = std::borrow::Cow::Owned(
                    remaining[stopped.sent + 1..]
                        .iter()
                        .filter(|message| message.target != target)
                        .copied()
                        .collect(),
                );
            }
        }
    }
}

/// The outcome of [`try_send_all`]: how far a batch got without waiting.
#[derive(Debug, Default)]
pub struct TrySent {
    /// Datagrams handed to the kernel, from the first.
    pub sent: usize,
    /// System calls made.
    pub calls: usize,
    /// The error that stopped the batch: `None` when every datagram was
    /// sent, `WouldBlock` when the socket's buffer was full (the rest is the
    /// caller's to send later), any other kind when the kernel refused the
    /// datagram at index `sent`.
    pub error: Option<io::Error>,
}

impl TrySent {
    /// The socket's buffer filled before the batch was through.
    pub fn would_block(&self) -> bool {
        self.error
            .as_ref()
            .is_some_and(|error| error.kind() == io::ErrorKind::WouldBlock)
    }
}

/// Hand as much of `messages` to the kernel as it takes now, in order,
/// without ever waiting for the socket: `sendmmsg` calls of up to
/// [`MESSAGES_PER_CALL`] datagrams (Linux), else one send each. The calls go
/// straight to the (non-blocking) socket, not through the runtime's cached
/// readiness, which a socket only gets once the reactor has polled it: the
/// kernel alone says whether its buffer has room. A full buffer stops the
/// batch with `WouldBlock` in [`TrySent::error`]; the datagrams from
/// [`TrySent::sent`] on were not sent.
pub fn try_send_all(socket: &UdpSocket, messages: &[Outbound<'_>]) -> TrySent {
    let mut outcome = TrySent::default();
    while outcome.sent < messages.len() {
        #[cfg(target_os = "linux")]
        let result = linux::sendmmsg(socket, &messages[outcome.sent..]);
        #[cfg(not(target_os = "linux"))]
        let result = {
            let message = &messages[outcome.sent];
            let socket = socket2::SockRef::from(socket);
            match message.target {
                Some(target) => socket.send_to(message.bytes, &target.into()),
                None => socket.send(message.bytes),
            }
            .and_then(|sent| {
                if sent == message.bytes.len() {
                    Ok(1)
                } else {
                    Err(io::Error::new(
                        io::ErrorKind::WriteZero,
                        "partial UDP datagram send",
                    ))
                }
            })
        };
        outcome.calls += 1;
        match result {
            Ok(0) => {
                outcome.error = Some(io::Error::new(
                    io::ErrorKind::WriteZero,
                    "sendmmsg took no datagram",
                ));
                return outcome;
            }
            Ok(count) => outcome.sent += count,
            Err(error) => {
                outcome.error = Some(error);
                return outcome;
            }
        }
    }
    outcome
}

/// Wait, blocking the calling thread, until the kernel has room in
/// `socket`'s send buffer or `timeout` has passed (`Ok(false)`). For a
/// thread of its own (a lane backlog): it asks the kernel, not a runtime's
/// I/O driver, so no runtime has to be polling for the wake-up to come.
#[cfg(unix)]
pub fn wait_writable(socket: &UdpSocket, timeout: std::time::Duration) -> io::Result<bool> {
    use std::os::fd::AsRawFd;
    let mut descriptor = libc::pollfd {
        fd: socket.as_raw_fd(),
        events: libc::POLLOUT,
        revents: 0,
    };
    let timeout_ms = libc::c_int::try_from(timeout.as_millis()).unwrap_or(libc::c_int::MAX);
    loop {
        // SAFETY: one pollfd that outlives the call, and its count.
        let rc = unsafe { libc::poll(&mut descriptor, 1, timeout_ms) };
        if rc >= 0 {
            return Ok(rc > 0);
        }
        let error = io::Error::last_os_error();
        if error.kind() != io::ErrorKind::Interrupted {
            return Err(error);
        }
    }
}

#[cfg(not(unix))]
pub fn wait_writable(_socket: &UdpSocket, timeout: std::time::Duration) -> io::Result<bool> {
    std::thread::sleep(timeout.min(std::time::Duration::from_millis(1)));
    Ok(true)
}

/// One awaited send per datagram: the behaviour without batching.
pub async fn send_each(socket: &UdpSocket, messages: &[Outbound<'_>]) -> io::Result<usize> {
    send_each_until_refused(socket, messages)
        .await
        .map_err(|stopped| stopped.error)
}

async fn send_each_until_refused(
    socket: &UdpSocket,
    messages: &[Outbound<'_>],
) -> Result<usize, Stopped> {
    for (index, message) in messages.iter().enumerate() {
        let stopped = |error| Stopped {
            sent: index,
            calls: index + 1,
            error,
        };
        let sent = match message.target {
            Some(target) => socket.send_to(message.bytes, target).await,
            None => socket.send(message.bytes).await,
        }
        .map_err(stopped)?;
        if sent != message.bytes.len() {
            return Err(stopped(io::Error::new(
                io::ErrorKind::WriteZero,
                "partial UDP datagram send",
            )));
        }
    }
    Ok(messages.len())
}

/// Datagrams of one UDP GSO message at most (UDP_MAX_SEGMENTS is 64 on
/// older kernels).
pub const GSO_MAX_SEGMENTS: usize = 64;
/// Payload bytes of one UDP GSO message at most, below an IPv4 datagram's
/// 65,507 with a margin.
pub const GSO_MAX_BYTES: usize = 63 * 1024;
/// GSO messages handed to the kernel in one `sendmmsg` call at most.
const GSO_MESSAGES_PER_CALL: usize = 16;

/// How large one UDP GSO message may be. A GSO message is one packet to
/// the qdisc: a `prio` qdisc lets another band in only between packets,
/// and `fq` charges a paced socket's whole message at once and then lets
/// the device send its datagrams back to back. So a run is kept short
/// (`segments`), and on a paced socket to about one millisecond at the
/// pacing rate, as TCP sizes its own GSO packets (`tcp_tso_autosize`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct GsoLimits {
    /// Datagrams of one message, `1..=GSO_MAX_SEGMENTS` (1: no GSO).
    pub segments: usize,
    /// Payload bytes of one message; a run always takes its first datagram.
    pub bytes: usize,
}

impl GsoLimits {
    /// The kernel's own limits.
    pub const MAX: Self = Self {
        segments: GSO_MAX_SEGMENTS,
        bytes: GSO_MAX_BYTES,
    };

    /// At most `segments` datagrams a message and, when the socket is paced
    /// at `pacing_bytes_per_second`, at most one millisecond of that rate.
    pub fn new(segments: usize, pacing_bytes_per_second: Option<u64>) -> Self {
        let bytes =
            pacing_bytes_per_second
                .filter(|rate| *rate > 0)
                .map_or(GSO_MAX_BYTES, |rate| {
                    usize::try_from(rate / 1_000)
                        .map_or(GSO_MAX_BYTES, |bytes| bytes.min(GSO_MAX_BYTES))
                });
        Self {
            segments: segments.clamp(1, GSO_MAX_SEGMENTS),
            bytes,
        }
    }

    /// These limits for a send paced at `bytes_per_second` by
    /// [`send_all_gso_or_plain_paced`]: a message is at most
    /// [`GSO_PACED_MESSAGE_US`] of the pace, so that no message, which the
    /// device sends back to back, is a burst denser than the pace.
    pub fn paced(self, bytes_per_second: u64) -> Self {
        if bytes_per_second == 0 {
            return self;
        }
        let bytes =
            usize::try_from(bytes_per_second.saturating_mul(GSO_PACED_MESSAGE_US) / 1_000_000)
                .unwrap_or(GSO_MAX_BYTES);
        Self {
            segments: self.segments,
            bytes: self.bytes.min(bytes),
        }
    }
}

/// The longest stretch of the pace that one paced GSO message carries.
pub const GSO_PACED_MESSAGE_US: u64 = 50;

/// The runs of `messages` that one UDP GSO message each can carry:
/// consecutive datagrams to one target, every one but the last exactly as
/// long as the first, within `limits` (a run always holds at least its
/// first datagram). `(first, count)` of each run, in order, covering every
/// message once.
pub fn gso_runs(messages: &[Outbound<'_>], limits: GsoLimits) -> Vec<(usize, usize)> {
    let max_segments = limits.segments.clamp(1, GSO_MAX_SEGMENTS);
    let max_bytes = limits.bytes.min(GSO_MAX_BYTES);
    let mut runs = Vec::new();
    let mut index = 0;
    while index < messages.len() {
        let first = &messages[index];
        let size = first.bytes.len();
        let mut length = 1;
        let mut bytes = size;
        if size > 0 {
            while index + length < messages.len() && length < max_segments {
                let next = &messages[index + length];
                let next_size = next.bytes.len();
                if next.target != first.target
                    || next_size == 0
                    || next_size > size
                    || bytes + next_size > max_bytes
                {
                    break;
                }
                length += 1;
                bytes += next_size;
                if next_size < size {
                    break;
                }
            }
        }
        runs.push((index, length));
        index += length;
    }
    runs
}

/// Whether a send failed because this host cannot do UDP GSO on the socket
/// (no `UDP_SEGMENT`, a device without checksum offload, or a segment that
/// does not fit the path's MTU): send without it from then on.
pub fn gso_unsupported(error: &io::Error) -> bool {
    #[cfg(target_os = "linux")]
    {
        matches!(
            error.raw_os_error(),
            Some(libc::EIO | libc::EINVAL | libc::EOPNOTSUPP | libc::ENOPROTOOPT)
        ) || error.kind() == io::ErrorKind::Unsupported
    }
    #[cfg(not(target_os = "linux"))]
    {
        let _ = error;
        true
    }
}

/// Whether this kernel has UDP GSO at all (`UDP_SEGMENT`, Linux 4.18): a
/// socket takes the option. A device may still refuse a GSO send.
pub fn gso_available() -> bool {
    #[cfg(target_os = "linux")]
    {
        use std::os::fd::AsRawFd;
        let Ok(socket) = std::net::UdpSocket::bind("127.0.0.1:0") else {
            return false;
        };
        let segment: libc::c_int = 1_200;
        // SAFETY: an int option of a socket this function owns.
        let rc = unsafe {
            libc::setsockopt(
                socket.as_raw_fd(),
                libc::SOL_UDP,
                libc::UDP_SEGMENT,
                std::ptr::addr_of!(segment).cast(),
                std::mem::size_of::<libc::c_int>() as libc::socklen_t,
            )
        };
        rc == 0
    }
    #[cfg(not(target_os = "linux"))]
    {
        false
    }
}

/// What [`send_all_gso`] did.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct GsoSent {
    /// `sendmmsg` calls.
    pub calls: usize,
    /// GSO messages: passes through the stack, each cut by the kernel into
    /// up to [`GsoLimits::segments`] datagrams.
    pub messages: usize,
}

/// [`send_all`] with UDP GSO: each run of [`gso_runs`] is one message that
/// the kernel cuts into the run's datagrams (`UDP_SEGMENT`), so the symbols
/// of a 4K part cost a few passes through the stack instead of one each.
/// The receiver sees exactly the datagrams of [`send_all`], in order. Waits
/// for room in the socket's buffer as [`send_all`] does. A [`Stopped`]
/// error says how many datagrams went before it.
pub async fn send_all_gso(
    socket: &UdpSocket,
    messages: &[Outbound<'_>],
    limits: GsoLimits,
) -> Result<GsoSent, Stopped> {
    send_all_gso_with(socket, messages, limits, &mut sendmmsg_gso).await
}

/// One `sendmmsg` call of GSO messages, one per run: how many runs the
/// kernel took.
fn sendmmsg_gso(
    socket: &UdpSocket,
    messages: &[Outbound<'_>],
    runs: &[(usize, usize)],
) -> io::Result<usize> {
    #[cfg(target_os = "linux")]
    {
        linux::sendmmsg_gso(socket, messages, runs)
    }
    #[cfg(not(target_os = "linux"))]
    {
        let _ = (socket, messages, runs);
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "UDP GSO is Linux only",
        ))
    }
}

/// [`send_all_gso`] with the `sendmmsg` call given: tests stand in a kernel
/// that refuses GSO part way through a batch.
async fn send_all_gso_with<F>(
    socket: &UdpSocket,
    messages: &[Outbound<'_>],
    limits: GsoLimits,
    call: &mut F,
) -> Result<GsoSent, Stopped>
where
    F: FnMut(&UdpSocket, &[Outbound<'_>], &[(usize, usize)]) -> io::Result<usize>,
{
    use tokio::io::Interest;
    let runs = gso_runs(messages, limits);
    let mut done = GsoSent::default();
    let mut next_run = 0;
    let mut sent = 0;
    while next_run < runs.len() {
        let end = (next_run + GSO_MESSAGES_PER_CALL).min(runs.len());
        match socket.try_io(Interest::WRITABLE, || {
            call(socket, messages, &runs[next_run..end])
        }) {
            Ok(0) => {
                return Err(Stopped {
                    sent,
                    calls: done.calls + 1,
                    error: io::Error::new(io::ErrorKind::WriteZero, "sendmmsg took no GSO message"),
                })
            }
            Ok(taken) => {
                done.calls += 1;
                done.messages += taken;
                sent += runs[next_run..next_run + taken]
                    .iter()
                    .map(|run| run.1)
                    .sum::<usize>();
                next_run += taken;
            }
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                if let Err(error) = socket.writable().await {
                    return Err(Stopped {
                        sent,
                        calls: done.calls,
                        error,
                    });
                }
            }
            Err(error) => {
                return Err(Stopped {
                    sent,
                    calls: done.calls + 1,
                    error,
                })
            }
        }
    }
    Ok(done)
}

/// What [`send_all_gso_or_plain`] did.
#[derive(Debug, Default)]
pub struct GsoOrPlainSent {
    /// GSO messages, and the datagrams they carried.
    pub gso_messages: usize,
    pub gso_datagrams: usize,
    /// Datagrams that left one each (after a refusal, or with GSO off).
    pub plain_datagrams: usize,
    /// The refusal that turned GSO off in this call (the caller's to log
    /// once): the rest of the batch went without GSO.
    pub turned_off: Option<io::Error>,
}

/// Send every datagram of `messages` in order: as UDP GSO messages while
/// `gso` is set, then one datagram each. When the kernel refuses GSO
/// ([`gso_unsupported`]) part way, `gso` is cleared for good and the
/// datagrams from the refused message on are sent with [`send_all`]
/// (batched), so each still arrives once and in order. A [`Stopped`] error
/// gives the index, in `messages`, of the datagram that failed.
pub async fn send_all_gso_or_plain(
    socket: &UdpSocket,
    messages: &[Outbound<'_>],
    limits: GsoLimits,
    gso: &std::sync::atomic::AtomicBool,
) -> Result<GsoOrPlainSent, Stopped> {
    send_all_gso_or_plain_with(socket, messages, limits, gso, &mut sendmmsg_gso).await
}

async fn send_all_gso_or_plain_with<F>(
    socket: &UdpSocket,
    messages: &[Outbound<'_>],
    limits: GsoLimits,
    gso: &std::sync::atomic::AtomicBool,
    call: &mut F,
) -> Result<GsoOrPlainSent, Stopped>
where
    F: FnMut(&UdpSocket, &[Outbound<'_>], &[(usize, usize)]) -> io::Result<usize>,
{
    use std::sync::atomic::Ordering;
    let mut outcome = GsoOrPlainSent::default();
    let mut from = 0;
    if gso.load(Ordering::Relaxed) {
        match send_all_gso_with(socket, messages, limits, call).await {
            Ok(sent) => {
                outcome.gso_messages = sent.messages;
                outcome.gso_datagrams = messages.len();
                return Ok(outcome);
            }
            Err(stopped) if gso_unsupported(&stopped.error) => {
                if gso.swap(false, Ordering::Relaxed) {
                    outcome.turned_off = Some(stopped.error);
                }
                outcome.gso_datagrams = stopped.sent;
                from = stopped.sent;
            }
            Err(stopped) => return Err(stopped),
        }
    }
    match send_until_refused(socket, &messages[from..], true).await {
        Ok(_) => {
            outcome.plain_datagrams = messages.len() - from;
            Ok(outcome)
        }
        Err(stopped) => Err(Stopped {
            sent: from + stopped.sent,
            calls: stopped.calls,
            error: stopped.error,
        }),
    }
}

/// [`send_all_gso_or_plain`] paced, for a blocking thread: the GSO messages
/// are handed to the kernel no faster than `bytes_per_second` of payload,
/// the thread sleeping (not spinning) between them for as long as the bytes
/// already handed over take at that pace. Unpaced, GSO hands a whole 4K part
/// to the kernel in well under a millisecond, several times denser than
/// the one-datagram-each batch, whose own cost per datagram spreads it out:
/// a NIC slower than that queues it, the socket's buffer fills, and the
/// part reaches the wire and the next hop as one line-rate burst. Paced, the
/// kernel sees a part no faster than the pace, and the GSO saving (one pass
/// through the stack per message) is kept. Waits for room in the socket's
/// buffer with `poll(2)` until `give_up` (then `TimedOut`). After a refusal
/// of GSO the rest goes one datagram each, unpaced, as with GSO off.
pub fn send_all_gso_or_plain_paced(
    socket: &UdpSocket,
    messages: &[Outbound<'_>],
    limits: GsoLimits,
    bytes_per_second: u64,
    gso: &std::sync::atomic::AtomicBool,
    give_up: std::time::Instant,
) -> Result<GsoOrPlainSent, Stopped> {
    send_all_gso_or_plain_paced_with(
        socket,
        messages,
        limits,
        bytes_per_second,
        gso,
        give_up,
        &mut sendmmsg_gso,
    )
}

fn send_all_gso_or_plain_paced_with<F>(
    socket: &UdpSocket,
    messages: &[Outbound<'_>],
    limits: GsoLimits,
    bytes_per_second: u64,
    gso: &std::sync::atomic::AtomicBool,
    give_up: std::time::Instant,
    call: &mut F,
) -> Result<GsoOrPlainSent, Stopped>
where
    F: FnMut(&UdpSocket, &[Outbound<'_>], &[(usize, usize)]) -> io::Result<usize>,
{
    use std::sync::atomic::Ordering;
    let mut outcome = GsoOrPlainSent::default();
    let mut from = 0;
    if gso.load(Ordering::Relaxed) {
        match send_all_gso_paced_with(socket, messages, limits, bytes_per_second, give_up, call) {
            Ok(sent) => {
                outcome.gso_messages = sent.messages;
                outcome.gso_datagrams = messages.len();
                return Ok(outcome);
            }
            Err(stopped) if gso_unsupported(&stopped.error) => {
                if gso.swap(false, Ordering::Relaxed) {
                    outcome.turned_off = Some(stopped.error);
                }
                outcome.gso_datagrams = stopped.sent;
                from = stopped.sent;
            }
            Err(stopped) => return Err(stopped),
        }
    }
    let mut sent = from;
    let mut calls = 0;
    while sent < messages.len() {
        let outcome = try_send_all(socket, &messages[sent..]);
        sent += outcome.sent;
        calls += outcome.calls;
        if outcome.would_block() {
            wait_writable_until(socket, give_up).map_err(|error| Stopped { sent, calls, error })?;
        } else if let Some(error) = outcome.error {
            return Err(Stopped { sent, calls, error });
        }
    }
    outcome.plain_datagrams = messages.len() - from;
    Ok(outcome)
}

/// Wait for room in `socket`'s buffer until `give_up`.
fn wait_writable_until(socket: &UdpSocket, give_up: std::time::Instant) -> io::Result<()> {
    let now = std::time::Instant::now();
    if now >= give_up {
        return Err(io::Error::new(
            io::ErrorKind::TimedOut,
            "gave up waiting for room in the socket buffer",
        ));
    }
    wait_writable(
        socket,
        (give_up - now)
            .min(std::time::Duration::from_millis(100))
            .max(std::time::Duration::from_millis(1)),
    )
    .map(drop)
}

/// The paced GSO send of [`send_all_gso_or_plain_paced`].
fn send_all_gso_paced_with<F>(
    socket: &UdpSocket,
    messages: &[Outbound<'_>],
    limits: GsoLimits,
    bytes_per_second: u64,
    give_up: std::time::Instant,
    call: &mut F,
) -> Result<GsoSent, Stopped>
where
    F: FnMut(&UdpSocket, &[Outbound<'_>], &[(usize, usize)]) -> io::Result<usize>,
{
    use std::time::{Duration, Instant};
    precise_sleeps();
    let runs = gso_runs(messages, limits);
    let run_bytes = |run: &(usize, usize)| -> u64 {
        messages[run.0..run.0 + run.1]
            .iter()
            .map(|message| message.bytes.len() as u64)
            .sum()
    };
    let rate = u128::from(bytes_per_second.max(1));
    // How long `bytes` take at the pace.
    let span = |bytes: u64| Duration::from_nanos((u128::from(bytes) * 1_000_000_000 / rate) as u64);
    let mut done = GsoSent::default();
    let mut start = Instant::now();
    let mut paced_bytes = 0_u64;
    let mut next_run = 0;
    let mut sent = 0;
    while next_run < runs.len() {
        // One message a call. A late wake-up is not made up for with a
        // burst: the pace runs on from now.
        let now = Instant::now();
        let behind = start + span(paced_bytes);
        if now > behind {
            start = now.checked_sub(span(paced_bytes)).unwrap_or(start);
        }
        match call(socket, messages, &runs[next_run..next_run + 1]) {
            Ok(0) => {
                return Err(Stopped {
                    sent,
                    calls: done.calls + 1,
                    error: io::Error::new(io::ErrorKind::WriteZero, "sendmmsg took no GSO message"),
                })
            }
            Ok(taken) => {
                done.calls += 1;
                done.messages += taken;
                for run in &runs[next_run..next_run + taken] {
                    sent += run.1;
                    paced_bytes += run_bytes(run);
                }
                next_run += taken;
            }
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                wait_writable_until(socket, give_up).map_err(|error| Stopped {
                    sent,
                    calls: done.calls + 1,
                    error,
                })?;
                continue;
            }
            Err(error) => {
                return Err(Stopped {
                    sent,
                    calls: done.calls + 1,
                    error,
                })
            }
        }
        if next_run < runs.len() {
            let wake = start + span(paced_bytes);
            let now = Instant::now();
            if wake > now {
                std::thread::sleep(wake - now);
            }
        }
    }
    Ok(done)
}

/// Sleeps of this thread end within a microsecond or so of when they are
/// asked to, not the default 50 us timer slack later: the paced send sleeps
/// tens of microseconds at a time.
fn precise_sleeps() {
    #[cfg(target_os = "linux")]
    {
        thread_local! {
            static SET: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
        }
        SET.with(|set| {
            if !set.replace(true) {
                // SAFETY: PR_SET_TIMERSLACK takes the slack in nanoseconds.
                unsafe { libc::prctl(libc::PR_SET_TIMERSLACK, 1_000_u64) };
            }
        });
    }
}

#[cfg(target_os = "linux")]
mod linux {
    use super::{Outbound, Stopped, GSO_MESSAGES_PER_CALL, MESSAGES_PER_CALL};
    use std::io;
    use std::os::fd::AsRawFd;
    use tokio::io::Interest;
    use tokio::net::UdpSocket;

    pub(super) async fn send_all(
        socket: &UdpSocket,
        messages: &[Outbound<'_>],
    ) -> Result<usize, Stopped> {
        let mut sent = 0;
        let mut calls = 0;
        while sent < messages.len() {
            // Try first: a socket with room in its buffer takes the batch
            // without a trip through the reactor.
            match socket.try_io(Interest::WRITABLE, || sendmmsg(socket, &messages[sent..])) {
                Ok(0) => {
                    return Err(Stopped {
                        sent,
                        calls: calls + 1,
                        error: io::Error::new(
                            io::ErrorKind::WriteZero,
                            "sendmmsg took no datagram",
                        ),
                    })
                }
                Ok(count) => {
                    sent += count;
                    calls += 1;
                }
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                    if let Err(error) = socket.writable().await {
                        return Err(Stopped { sent, calls, error });
                    }
                }
                // sendmmsg reports a refused datagram after the ones before
                // it: as the error of the call that starts with it.
                Err(error) => {
                    return Err(Stopped {
                        sent,
                        calls: calls + 1,
                        error,
                    })
                }
            }
        }
        Ok(calls)
    }

    /// One `sendmmsg` call of GSO messages, one per run. Returns how many
    /// runs the kernel took.
    pub(super) fn sendmmsg_gso(
        socket: &UdpSocket,
        messages: &[Outbound<'_>],
        runs: &[(usize, usize)],
    ) -> io::Result<usize> {
        const CONTROL: usize = 32;
        #[repr(C, align(8))]
        #[derive(Clone, Copy)]
        struct Control([u8; CONTROL]);
        let count = runs.len().min(GSO_MESSAGES_PER_CALL);
        if count == 0 {
            return Ok(0);
        }
        let datagrams: usize = runs[..count].iter().map(|run| run.1).sum();
        let addresses = runs[..count]
            .iter()
            .map(|run| messages[run.0].target.map(socket2::SockAddr::from))
            .collect::<Vec<_>>();
        let mut iovecs: Vec<libc::iovec> = Vec::with_capacity(datagrams);
        // SAFETY: zeroed mmsghdr values and byte arrays are valid; every
        // pointer stored below is to memory that outlives the call
        // (`messages`, `addresses`, `iovecs` which is never reallocated
        // after its pointers are taken, `controls`), and the kernel only
        // reads the buffers of a send.
        unsafe {
            for run in &runs[..count] {
                for message in &messages[run.0..run.0 + run.1] {
                    iovecs.push(libc::iovec {
                        iov_base: message.bytes.as_ptr() as *mut libc::c_void,
                        iov_len: message.bytes.len(),
                    });
                }
            }
            let mut headers: [libc::mmsghdr; GSO_MESSAGES_PER_CALL] = std::mem::zeroed();
            let mut controls = [Control([0; CONTROL]); GSO_MESSAGES_PER_CALL];
            let space = libc::CMSG_SPACE(std::mem::size_of::<u16>() as u32) as usize;
            debug_assert!(space <= CONTROL);
            let mut first_iov = 0;
            for (slot, run) in runs[..count].iter().enumerate() {
                let header = &mut headers[slot].msg_hdr;
                if let Some(address) = &addresses[slot] {
                    header.msg_name = address.as_ptr() as *mut libc::c_void;
                    header.msg_namelen = address.len();
                }
                header.msg_iov = iovecs.as_mut_ptr().add(first_iov);
                header.msg_iovlen = run.1 as _;
                first_iov += run.1;
                if run.1 > 1 {
                    let segment = u16::try_from(messages[run.0].bytes.len()).map_err(|_| {
                        io::Error::new(io::ErrorKind::InvalidInput, "GSO segment over 64 KiB")
                    })?;
                    header.msg_control = controls[slot].0.as_mut_ptr().cast();
                    header.msg_controllen = space as _;
                    let cmsg = libc::CMSG_FIRSTHDR(header);
                    (*cmsg).cmsg_level = libc::SOL_UDP;
                    (*cmsg).cmsg_type = libc::UDP_SEGMENT;
                    (*cmsg).cmsg_len = libc::CMSG_LEN(std::mem::size_of::<u16>() as u32) as _;
                    std::ptr::write_unaligned(libc::CMSG_DATA(cmsg).cast::<u16>(), segment);
                }
            }
            let rc = libc::sendmmsg(
                socket.as_raw_fd(),
                headers.as_mut_ptr(),
                count as libc::c_uint,
                0,
            );
            if rc < 0 {
                return Err(io::Error::last_os_error());
            }
            Ok(rc as usize)
        }
    }

    /// One `sendmmsg` call for up to [`MESSAGES_PER_CALL`] messages. Returns
    /// how many the kernel took.
    pub(super) fn sendmmsg(socket: &UdpSocket, messages: &[Outbound<'_>]) -> io::Result<usize> {
        let count = messages.len().min(MESSAGES_PER_CALL);
        if count == 0 {
            return Ok(0);
        }
        let addresses = messages[..count]
            .iter()
            .map(|message| message.target.map(socket2::SockAddr::from))
            .collect::<Vec<_>>();
        // SAFETY: zeroed iovec and mmsghdr values are valid (null pointers,
        // zero lengths). Every pointer stored below is to memory that
        // outlives the call (`messages`, `addresses`, `iovecs`), and the
        // kernel only reads the buffers of a send.
        unsafe {
            let mut iovecs: [libc::iovec; MESSAGES_PER_CALL] = std::mem::zeroed();
            let mut headers: [libc::mmsghdr; MESSAGES_PER_CALL] = std::mem::zeroed();
            for (index, message) in messages[..count].iter().enumerate() {
                iovecs[index] = libc::iovec {
                    iov_base: message.bytes.as_ptr() as *mut libc::c_void,
                    iov_len: message.bytes.len(),
                };
                let header = &mut headers[index].msg_hdr;
                if let Some(address) = &addresses[index] {
                    header.msg_name = address.as_ptr() as *mut libc::c_void;
                    header.msg_namelen = address.len();
                }
                header.msg_iov = &mut iovecs[index];
                header.msg_iovlen = 1;
            }
            let rc = libc::sendmmsg(
                socket.as_raw_fd(),
                headers.as_mut_ptr(),
                count as libc::c_uint,
                0,
            );
            if rc < 0 {
                return Err(io::Error::last_os_error());
            }
            let taken = rc as usize;
            for (index, header) in headers[..taken].iter().enumerate() {
                if header.msg_len as usize != messages[index].bytes.len() {
                    return Err(io::Error::new(
                        io::ErrorKind::WriteZero,
                        "partial UDP datagram send",
                    ));
                }
            }
            Ok(taken)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    async fn receive(socket: &UdpSocket, count: usize) -> Vec<(Vec<u8>, SocketAddr)> {
        let mut out = Vec::new();
        let mut buffer = vec![0_u8; 65_536];
        while out.len() < count {
            let (len, from) =
                tokio::time::timeout(Duration::from_secs(2), socket.recv_from(&mut buffer))
                    .await
                    .expect("datagram arrives")
                    .unwrap();
            out.push((buffer[..len].to_vec(), from));
        }
        out
    }

    /// A loopback receiver whose buffer holds a whole test batch: the
    /// default 208 kB holds fewer than a hundred small datagrams.
    async fn receiver() -> UdpSocket {
        let socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        socket2::SockRef::from(&socket)
            .set_recv_buffer_size(4 << 20)
            .unwrap();
        socket
    }

    fn payloads(count: usize) -> Vec<Vec<u8>> {
        (0..count)
            .map(|index| {
                let len = 20 + (index * 37) % 1_300;
                (0..len).map(|byte| (byte + index) as u8).collect()
            })
            .collect()
    }

    #[tokio::test]
    async fn a_batch_arrives_whole_and_in_order_on_a_connected_socket() {
        for batch in [true, false] {
            let receiver = receiver().await;
            let sender = UdpSocket::bind("127.0.0.1:0").await.unwrap();
            sender
                .connect(receiver.local_addr().unwrap())
                .await
                .unwrap();
            // More than one call's worth, so the batch is cut in calls.
            let data = payloads(MESSAGES_PER_CALL * 2 + 5);
            let messages = data
                .iter()
                .map(|bytes| Outbound::connected(bytes))
                .collect::<Vec<_>>();
            let calls = send_all(&sender, &messages, batch).await.unwrap();
            if batch && cfg!(target_os = "linux") {
                assert_eq!(calls, 3, "{} datagrams in calls of 64", data.len());
            } else {
                assert_eq!(calls, data.len());
            }
            let received = receive(&receiver, data.len()).await;
            for (index, (bytes, from)) in received.into_iter().enumerate() {
                assert_eq!(bytes, data[index], "datagram {index}, batch {batch}");
                assert_eq!(from, sender.local_addr().unwrap());
            }
        }
    }

    #[test]
    fn gso_runs_join_equal_datagrams_to_one_target_within_the_limits() {
        let a: SocketAddr = "127.0.0.1:1000".parse().unwrap();
        let b: SocketAddr = "127.0.0.1:2000".parse().unwrap();
        let full = vec![1_u8; 1_200];
        let short = vec![2_u8; 300];
        let mut messages = vec![Outbound::connected(&full); 10];
        messages.push(Outbound::connected(&short));
        messages.push(Outbound::connected(&full));
        messages.extend([
            Outbound::to(a, &full),
            Outbound::to(a, &full),
            Outbound::to(b, &full),
        ]);
        assert_eq!(
            gso_runs(&messages, GsoLimits::MAX),
            vec![(0, 11), (11, 1), (12, 2), (14, 1)]
        );
        let longer = vec![3_u8; 1_300];
        let grow = [Outbound::connected(&full), Outbound::connected(&longer)];
        assert_eq!(gso_runs(&grow, GsoLimits::MAX), vec![(0, 1), (1, 1)]);
        let many = vec![Outbound::connected(&full); 200];
        let runs = gso_runs(&many, GsoLimits::MAX);
        assert!(runs.iter().all(|run| run.1 <= GSO_MAX_SEGMENTS));
        assert!(runs.iter().all(|run| run.1 * full.len() <= GSO_MAX_BYTES));
        assert_eq!(runs.iter().map(|run| run.1).sum::<usize>(), 200);
        assert!(gso_runs(&[], GsoLimits::MAX).is_empty());
    }

    #[test]
    fn gso_runs_keep_to_the_segment_cap_and_to_a_millisecond_of_the_pacing_rate() {
        let full = vec![1_u8; 1_190];
        let many = vec![Outbound::connected(&full); 100];
        // The segment cap: runs of 16, then the rest.
        let capped = gso_runs(&many, GsoLimits::new(16, None));
        assert_eq!(capped.len(), 7);
        assert!(capped.iter().all(|run| run.1 <= 16));
        assert_eq!(capped.last(), Some(&(96, 4)));
        // 40 Mbit/s of pacing is 5,000 bytes a millisecond: 4 symbols of
        // 1,190 bytes a message, whatever the segment cap says.
        let paced = GsoLimits::new(64, Some(40_000_000 / 8));
        assert_eq!(paced.bytes, 5_000);
        let runs = gso_runs(&many, paced);
        assert!(runs.iter().all(|run| run.1 == 4), "{runs:?}");
        assert_eq!(runs.len(), 25);
        // Pacing below one symbol a millisecond: one datagram a message
        // (no GSO), never an empty run.
        let slow = GsoLimits::new(64, Some(500_000 / 8));
        let runs = gso_runs(&many, slow);
        assert_eq!(runs.len(), 100);
        assert!(runs.iter().all(|run| run.1 == 1));
        // Fast pacing and out-of-range caps stay within the kernel's limits.
        assert_eq!(GsoLimits::new(1_000, Some(10_000_000_000)), GsoLimits::MAX);
        assert_eq!(GsoLimits::new(0, None).segments, 1);
        assert_eq!(GsoLimits::new(16, Some(0)).bytes, GSO_MAX_BYTES);
    }

    /// A receiver with the buffer of a host left at the default
    /// net.core.rmem_max (212,992 bytes, doubled by the kernel), so a test
    /// that passes here passes on any host.
    async fn default_receiver() -> UdpSocket {
        let socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        socket2::SockRef::from(&socket)
            .set_recv_buffer_size(212_992)
            .unwrap();
        socket
    }

    /// The symbols of a 4K part to one lane: `count - 1` of one size, then a
    /// short last one, as RLS1 datagrams of one object are.
    fn part_symbols(count: usize) -> Vec<Vec<u8>> {
        (0..count)
            .map(|index| {
                let len = if index + 1 == count { 77 } else { 1_190 };
                (0..len).map(|byte| (byte * 3 + index) as u8).collect()
            })
            .collect()
    }

    #[tokio::test]
    async fn a_gso_batch_arrives_as_the_same_datagrams_in_order() {
        let receiver = default_receiver().await;
        let sender = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        sender
            .connect(receiver.local_addr().unwrap())
            .await
            .unwrap();
        // 121 datagrams fit a default receive buffer before it is read.
        let data = part_symbols(121);
        let messages = data
            .iter()
            .map(|bytes| Outbound::connected(bytes))
            .collect::<Vec<_>>();
        for limits in [GsoLimits::MAX, GsoLimits::new(16, None)] {
            let sent = match send_all_gso(&sender, &messages, limits).await {
                Ok(sent) => sent,
                Err(stopped) if stopped.sent == 0 && gso_unsupported(&stopped.error) => {
                    assert!(!gso_available(), "UDP GSO refused: {}", stopped.error);
                    eprintln!("UDP GSO is not available here: {}", stopped.error);
                    return;
                }
                Err(stopped) => panic!("GSO send failed: {:?}", stopped.error),
            };
            // Runs of 52 by bytes: 3 messages; runs of 16: 8 messages.
            let expected = if limits == GsoLimits::MAX { 3 } else { 8 };
            assert_eq!(sent.messages, expected, "{sent:?}");
            assert_eq!(sent.calls, 1, "{sent:?}");
            let received = receive(&receiver, data.len()).await;
            for (index, (bytes, from)) in received.into_iter().enumerate() {
                assert_eq!(bytes, data[index], "datagram {index}");
                assert_eq!(from, sender.local_addr().unwrap());
            }
        }
    }

    /// A kernel that refuses GSO (EINVAL, as for a segment over the path's
    /// MTU) on the second `sendmmsg` call of a batch: the first call's
    /// messages have gone, the rest goes one datagram each, and every
    /// datagram arrives once and in order. GSO stays off afterwards.
    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn a_gso_refusal_part_way_sends_the_rest_once_and_in_order_and_turns_gso_off() {
        use std::sync::atomic::{AtomicBool, Ordering};
        let receiver = default_receiver().await;
        let sender = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        sender
            .connect(receiver.local_addr().unwrap())
            .await
            .unwrap();
        let data = part_symbols(121);
        let messages = data
            .iter()
            .map(|bytes| Outbound::connected(bytes))
            .collect::<Vec<_>>();
        // Runs of 4: 31 messages, 16 a call, so the refusal is the second
        // call's after 64 datagrams have gone.
        let limits = GsoLimits::new(4, None);
        let gso = AtomicBool::new(true);
        let calls = std::cell::Cell::new(0);
        let mut refusing =
            |socket: &UdpSocket, messages: &[Outbound<'_>], runs: &[(usize, usize)]| {
                calls.set(calls.get() + 1);
                if calls.get() == 1 {
                    // The real call; without kernel GSO, one datagram a run.
                    if gso_available() {
                        linux::sendmmsg_gso(socket, messages, runs)
                    } else {
                        let plain = runs
                            .iter()
                            .flat_map(|run| &messages[run.0..run.0 + run.1])
                            .copied()
                            .collect::<Vec<_>>();
                        let sent = linux::sendmmsg(socket, &plain)?;
                        assert_eq!(sent, plain.len());
                        Ok(runs.len())
                    }
                } else {
                    Err(io::Error::from_raw_os_error(libc::EINVAL))
                }
            };
        let outcome = send_all_gso_or_plain_with(&sender, &messages, limits, &gso, &mut refusing)
            .await
            .unwrap();
        assert_eq!(calls.get(), 2);
        assert_eq!(outcome.gso_messages, 0, "the GSO send did not finish");
        assert_eq!(outcome.gso_datagrams, 64);
        assert_eq!(outcome.plain_datagrams, 121 - 64);
        assert_eq!(
            outcome
                .turned_off
                .as_ref()
                .and_then(io::Error::raw_os_error),
            Some(libc::EINVAL)
        );
        assert!(!gso.load(Ordering::Relaxed));
        let received = receive(&receiver, data.len()).await;
        for (index, (bytes, _)) in received.into_iter().enumerate() {
            assert_eq!(bytes, data[index], "datagram {index}");
        }
        // The next batch goes without GSO: the refusing call is not made.
        let outcome =
            send_all_gso_or_plain_with(&sender, &messages[..10], limits, &gso, &mut refusing)
                .await
                .unwrap();
        assert_eq!(calls.get(), 2);
        assert_eq!(outcome.plain_datagrams, 10);
        assert_eq!(outcome.gso_datagrams, 0);
        assert!(outcome.turned_off.is_none());
        let received = receive(&receiver, 10).await;
        for (index, (bytes, _)) in received.into_iter().enumerate() {
            assert_eq!(bytes, data[index], "datagram {index} of the second batch");
        }
        // Nothing arrived twice.
        let mut buffer = [0_u8; 2_048];
        assert!(receiver.try_recv_from(&mut buffer).is_err());
    }

    /// Any other error stops the batch and is the caller's, with the index
    /// of the failed datagram; GSO stays on.
    #[tokio::test]
    async fn a_gso_send_error_that_is_not_a_refusal_of_gso_is_returned() {
        use std::sync::atomic::{AtomicBool, Ordering};
        let sender = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        sender.connect("127.0.0.1:9").await.unwrap();
        let data = part_symbols(20);
        let messages = data
            .iter()
            .map(|bytes| Outbound::connected(bytes))
            .collect::<Vec<_>>();
        let gso = AtomicBool::new(true);
        let mut refused = |_: &UdpSocket, _: &[Outbound<'_>], _: &[(usize, usize)]| {
            Err::<usize, _>(io::Error::from(io::ErrorKind::ConnectionRefused))
        };
        let stopped = send_all_gso_or_plain_with(
            &sender,
            &messages,
            GsoLimits::new(8, None),
            &gso,
            &mut refused,
        )
        .await
        .unwrap_err();
        assert_eq!(stopped.sent, 0);
        assert_eq!(stopped.error.kind(), io::ErrorKind::ConnectionRefused);
        assert!(gso.load(Ordering::Relaxed));
    }

    #[test]
    fn paced_gso_messages_carry_at_most_50_us_of_the_pace() {
        let limits = GsoLimits::new(16, None);
        // 2,000 Mbit/s: 12,500 bytes, 10 symbols of 1,190 bytes.
        assert_eq!(limits.paced(250_000_000).bytes, 12_500);
        assert_eq!(limits.paced(250_000_000).segments, 16);
        let full = vec![1_u8; 1_190];
        let many = vec![Outbound::connected(&full); 100];
        let runs = gso_runs(&many, limits.paced(250_000_000));
        assert!(runs.iter().all(|run| run.1 == 10), "{runs:?}");
        // Unpaced: unchanged; the fq millisecond stays when it is shorter.
        assert_eq!(limits.paced(0), limits);
        assert_eq!(
            GsoLimits::new(16, Some(5_000_000)).paced(250_000_000).bytes,
            5_000
        );
        // A pace below a symbol in 50 us: one datagram a message.
        let runs = gso_runs(&many, limits.paced(10_000_000));
        assert!(runs.iter().all(|run| run.1 == 1));
    }

    /// A paced GSO send: every datagram once and in order, no sooner than
    /// the pace allows, in messages of 50 us of it, one a call.
    #[tokio::test]
    async fn a_paced_gso_batch_arrives_whole_in_order_and_keeps_to_the_pace() {
        use std::sync::atomic::{AtomicBool, Ordering};
        let receiver = default_receiver().await;
        let sender = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        sender
            .connect(receiver.local_addr().unwrap())
            .await
            .unwrap();
        let data = part_symbols(121);
        let messages = data
            .iter()
            .map(|bytes| Outbound::connected(bytes))
            .collect::<Vec<_>>();
        let total: usize = data.iter().map(Vec::len).sum();
        // 160 Mbit/s: 1,000 bytes in 50 us is less than a symbol, so the
        // pace is kept with one datagram a message; at 1,600 Mbit/s, 10,000
        // bytes, 8 symbols a message.
        for (rate, per_message) in [(20_000_000_u64, 1), (200_000_000, 8)] {
            let limits = GsoLimits::new(16, None).paced(rate);
            let gso = AtomicBool::new(true);
            let started = std::time::Instant::now();
            let outcome = send_all_gso_or_plain_paced(
                &sender,
                &messages,
                limits,
                rate,
                &gso,
                started + Duration::from_secs(2),
            )
            .unwrap();
            let took = started.elapsed();
            if !gso.load(Ordering::Relaxed) {
                assert!(
                    !gso_available(),
                    "UDP GSO refused: {:?}",
                    outcome.turned_off
                );
            } else {
                assert_eq!(outcome.gso_datagrams, data.len());
                // The short last symbol joins the last run.
                let runs = gso_runs(&messages, limits);
                assert!(runs[..runs.len() - 1]
                    .iter()
                    .all(|run| run.1 == per_message));
                assert_eq!(outcome.gso_messages, runs.len());
            }
            // The last message leaves when the rest are due at the pace.
            let floor =
                Duration::from_nanos(((total - per_message * 1_190) as u64) * 1_000_000_000 / rate);
            assert!(took >= floor, "{took:?} < {floor:?} at {rate} B/s");
            let received = receive(&receiver, data.len()).await;
            for (index, (bytes, _)) in received.into_iter().enumerate() {
                assert_eq!(bytes, data[index], "datagram {index} at {rate} B/s");
            }
        }
    }

    /// A kernel that refuses GSO on the second paced message: the first went
    /// by GSO, the rest one datagram each, every datagram once and in order,
    /// and GSO is off. Any other error is returned with GSO left on.
    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn a_paced_gso_refusal_sends_the_rest_once_and_in_order() {
        use std::sync::atomic::{AtomicBool, Ordering};
        let receiver = default_receiver().await;
        let sender = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        sender
            .connect(receiver.local_addr().unwrap())
            .await
            .unwrap();
        let data = part_symbols(121);
        let messages = data
            .iter()
            .map(|bytes| Outbound::connected(bytes))
            .collect::<Vec<_>>();
        let limits = GsoLimits::new(4, None);
        let gso = AtomicBool::new(true);
        let calls = std::cell::Cell::new(0);
        let mut refusing =
            |socket: &UdpSocket, messages: &[Outbound<'_>], runs: &[(usize, usize)]| {
                calls.set(calls.get() + 1);
                if calls.get() == 1 {
                    let plain = runs
                        .iter()
                        .flat_map(|run| &messages[run.0..run.0 + run.1])
                        .copied()
                        .collect::<Vec<_>>();
                    assert_eq!(linux::sendmmsg(socket, &plain)?, plain.len());
                    Ok(runs.len())
                } else {
                    Err(io::Error::from_raw_os_error(libc::EINVAL))
                }
            };
        let outcome = send_all_gso_or_plain_paced_with(
            &sender,
            &messages,
            limits,
            1_000_000_000,
            &gso,
            std::time::Instant::now() + Duration::from_secs(2),
            &mut refusing,
        )
        .unwrap();
        assert_eq!(calls.get(), 2);
        assert_eq!(outcome.gso_datagrams, 4);
        assert_eq!(outcome.plain_datagrams, 117);
        assert!(outcome.turned_off.is_some());
        assert!(!gso.load(Ordering::Relaxed));
        let received = receive(&receiver, data.len()).await;
        for (index, (bytes, _)) in received.into_iter().enumerate() {
            assert_eq!(bytes, data[index], "datagram {index}");
        }
        let mut buffer = [0_u8; 2_048];
        assert!(receiver.try_recv_from(&mut buffer).is_err());

        let gso = AtomicBool::new(true);
        let mut refused = |_: &UdpSocket, _: &[Outbound<'_>], _: &[(usize, usize)]| {
            Err::<usize, _>(io::Error::from(io::ErrorKind::ConnectionRefused))
        };
        let stopped = send_all_gso_or_plain_paced_with(
            &sender,
            &messages,
            limits,
            1_000_000_000,
            &gso,
            std::time::Instant::now() + Duration::from_secs(2),
            &mut refused,
        )
        .unwrap_err();
        assert_eq!(stopped.sent, 0);
        assert_eq!(stopped.error.kind(), io::ErrorKind::ConnectionRefused);
        assert!(gso.load(Ordering::Relaxed));
    }

    #[tokio::test]
    async fn one_batch_reaches_several_targets_from_one_source() {
        let first = receiver().await;
        let second = receiver().await;
        let sender = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let data = payloads(6);
        let mut messages = Vec::new();
        for bytes in &data {
            messages.push(Outbound::to(first.local_addr().unwrap(), bytes));
            messages.push(Outbound::to(second.local_addr().unwrap(), bytes));
        }
        let calls = send_all(&sender, &messages, true).await.unwrap();
        if cfg!(target_os = "linux") {
            assert_eq!(calls, 1);
        }
        for receiver in [&first, &second] {
            let received = receive(receiver, data.len()).await;
            for (index, (bytes, from)) in received.into_iter().enumerate() {
                assert_eq!(bytes, data[index]);
                assert_eq!(from, sender.local_addr().unwrap());
            }
        }
    }

    #[tokio::test]
    async fn a_refused_target_stops_only_its_own_datagrams() {
        for batch in [true, false] {
            let first = receiver().await;
            let second = receiver().await;
            let sender = UdpSocket::bind("127.0.0.1:0").await.unwrap();
            // The kernel refuses port 0 at once (EINVAL), as it refuses a
            // target without a route or behind a firewall rule.
            let refused: SocketAddr = "127.0.0.1:0".parse().unwrap();
            let data = payloads(MESSAGES_PER_CALL + 7);
            let mut messages = Vec::new();
            for bytes in &data {
                messages.push(Outbound::to(first.local_addr().unwrap(), bytes));
                messages.push(Outbound::to(refused, bytes));
                messages.push(Outbound::to(second.local_addr().unwrap(), bytes));
            }
            // The plain batch stops at the first refusal: the second target
            // never gets its first datagram.
            let error = send_all(&sender, &messages, batch).await.unwrap_err();
            assert_ne!(error.kind(), io::ErrorKind::WouldBlock);
            let _ = receive(&first, 1).await;

            let outcome = send_all_per_target(&sender, &messages, batch).await;
            assert_eq!(outcome.refused.len(), 1, "batch {batch}");
            assert_eq!(outcome.refused[0].0, Some(refused));
            assert!(outcome.refused(refused));
            assert!(!outcome.refused(first.local_addr().unwrap()));
            assert_eq!(outcome.sent, data.len() * 2);
            for receiver in [&first, &second] {
                let received = receive(receiver, data.len()).await;
                for (index, (bytes, _)) in received.into_iter().enumerate() {
                    assert_eq!(bytes, data[index], "datagram {index}, batch {batch}");
                }
            }
        }
    }

    #[tokio::test]
    async fn an_unreachable_target_is_an_error_not_a_hang() {
        let sender = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        // An IPv6 target on an IPv4 socket fails in the kernel at once.
        let target: SocketAddr = "[::1]:9".parse().unwrap();
        let bytes = [1_u8; 8];
        let error = send_all(&sender, &[Outbound::to(target, &bytes)], true)
            .await
            .unwrap_err();
        assert_ne!(error.kind(), io::ErrorKind::WouldBlock);
    }

    #[tokio::test]
    async fn try_send_all_sends_on_a_new_socket_without_waiting() {
        let receiver = receiver().await;
        // A socket that the reactor has not polled yet: its cached
        // readiness is not writable, the kernel's buffer is empty.
        let sender = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let target = receiver.local_addr().unwrap();
        let data = payloads(MESSAGES_PER_CALL + 3);
        let messages = data
            .iter()
            .map(|bytes| Outbound::to(target, bytes))
            .collect::<Vec<_>>();
        let outcome = try_send_all(&sender, &messages);
        assert!(outcome.error.is_none(), "{:?}", outcome.error);
        assert!(!outcome.would_block());
        assert_eq!(outcome.sent, data.len());
        if cfg!(target_os = "linux") {
            assert_eq!(outcome.calls, 2);
        }
        let received = receive(&receiver, data.len()).await;
        for (index, (bytes, _)) in received.into_iter().enumerate() {
            assert_eq!(bytes, data[index]);
        }
        // A refused datagram stops the batch at its index.
        let refused: SocketAddr = "127.0.0.1:0".parse().unwrap();
        let messages = [
            Outbound::to(target, &data[0]),
            Outbound::to(refused, &data[1]),
        ];
        let outcome = try_send_all(&sender, &messages);
        assert_eq!(outcome.sent, 1);
        assert!(outcome.error.is_some() && !outcome.would_block());
    }
}
