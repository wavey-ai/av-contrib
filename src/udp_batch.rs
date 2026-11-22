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

/// The runs of `messages` that one UDP GSO message each can carry:
/// consecutive datagrams to one target, every one but the last exactly as
/// long as the first, within [`GSO_MAX_SEGMENTS`] and [`GSO_MAX_BYTES`].
/// `(first, count)` of each run, in order, covering every message once.
pub fn gso_runs(messages: &[Outbound<'_>]) -> Vec<(usize, usize)> {
    let mut runs = Vec::new();
    let mut index = 0;
    while index < messages.len() {
        let first = &messages[index];
        let size = first.bytes.len();
        let mut length = 1;
        let mut bytes = size;
        if size > 0 {
            while index + length < messages.len() && length < GSO_MAX_SEGMENTS {
                let next = &messages[index + length];
                let next_size = next.bytes.len();
                if next.target != first.target
                    || next_size == 0
                    || next_size > size
                    || bytes + next_size > GSO_MAX_BYTES
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
/// (no `UDP_SEGMENT`, or a device without checksum offload): send without
/// it from then on.
pub fn gso_unsupported(error: &io::Error) -> bool {
    #[cfg(target_os = "linux")]
    {
        matches!(
            error.raw_os_error(),
            Some(libc::EIO | libc::EINVAL | libc::EOPNOTSUPP | libc::ENOPROTOOPT)
        )
    }
    #[cfg(not(target_os = "linux"))]
    {
        let _ = error;
        true
    }
}

/// What [`send_all_gso`] did.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct GsoSent {
    /// `sendmmsg` calls.
    pub calls: usize,
    /// GSO messages: passes through the stack, each cut by the kernel into
    /// up to [`GSO_MAX_SEGMENTS`] datagrams.
    pub messages: usize,
}

/// [`send_all`] with UDP GSO: each run of [`gso_runs`] is one message that
/// the kernel cuts into the run's datagrams (`UDP_SEGMENT`), so the symbols
/// of a 4K part cost a few passes through the stack instead of one each.
/// The receiver sees exactly the datagrams of [`send_all`], in order. Waits
/// for room in the socket's buffer as [`send_all`] does. A [`Stopped`]
/// error says how many datagrams went before it; with
/// [`gso_unsupported`] and none sent, send the batch with [`send_all`].
pub async fn send_all_gso(
    socket: &UdpSocket,
    messages: &[Outbound<'_>],
) -> Result<GsoSent, Stopped> {
    #[cfg(target_os = "linux")]
    {
        linux::send_all_gso(socket, messages).await
    }
    #[cfg(not(target_os = "linux"))]
    {
        let _ = (socket, messages);
        Err(Stopped {
            sent: 0,
            calls: 0,
            error: io::Error::new(io::ErrorKind::Unsupported, "UDP GSO is Linux only"),
        })
    }
}

#[cfg(target_os = "linux")]
mod linux {
    use super::{GsoSent, Outbound, Stopped, GSO_MESSAGES_PER_CALL, MESSAGES_PER_CALL};
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

    pub(super) async fn send_all_gso(
        socket: &UdpSocket,
        messages: &[Outbound<'_>],
    ) -> Result<GsoSent, Stopped> {
        let runs = super::gso_runs(messages);
        let mut done = GsoSent::default();
        let mut next_run = 0;
        let mut sent = 0;
        while next_run < runs.len() {
            let end = (next_run + GSO_MESSAGES_PER_CALL).min(runs.len());
            match socket.try_io(Interest::WRITABLE, || {
                sendmmsg_gso(socket, messages, &runs[next_run..end])
            }) {
                Ok(0) => {
                    return Err(Stopped {
                        sent,
                        calls: done.calls + 1,
                        error: io::Error::new(
                            io::ErrorKind::WriteZero,
                            "sendmmsg took no GSO message",
                        ),
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

    /// One `sendmmsg` call of GSO messages, one per run. Returns how many
    /// runs the kernel took.
    fn sendmmsg_gso(
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
            gso_runs(&messages),
            vec![(0, 11), (11, 1), (12, 2), (14, 1)]
        );
        let longer = vec![3_u8; 1_300];
        let grow = [Outbound::connected(&full), Outbound::connected(&longer)];
        assert_eq!(gso_runs(&grow), vec![(0, 1), (1, 1)]);
        let many = vec![Outbound::connected(&full); 200];
        let runs = gso_runs(&many);
        assert!(runs.iter().all(|run| run.1 <= GSO_MAX_SEGMENTS));
        assert!(runs.iter().all(|run| run.1 * full.len() <= GSO_MAX_BYTES));
        assert_eq!(runs.iter().map(|run| run.1).sum::<usize>(), 200);
        assert!(gso_runs(&[]).is_empty());
    }

    #[tokio::test]
    async fn a_gso_batch_arrives_as_the_same_datagrams_in_order() {
        let receiver = receiver().await;
        let sender = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        sender
            .connect(receiver.local_addr().unwrap())
            .await
            .unwrap();
        // A 4K part's symbols: 400 of one size, then a short last one, as
        // RLS1 datagrams of one object are.
        let data: Vec<Vec<u8>> = (0..401_usize)
            .map(|index| {
                let len = if index == 400 { 77 } else { 1_190 };
                (0..len).map(|byte| (byte * 3 + index) as u8).collect()
            })
            .collect();
        let messages = data
            .iter()
            .map(|bytes| Outbound::connected(bytes))
            .collect::<Vec<_>>();
        let sent = match send_all_gso(&sender, &messages).await {
            Ok(sent) => sent,
            Err(stopped) if stopped.sent == 0 && gso_unsupported(&stopped.error) => {
                eprintln!("UDP GSO is not available here: {}", stopped.error);
                return;
            }
            Err(stopped) => panic!("GSO send failed: {:?}", stopped.error),
        };
        // 401 datagrams in 8 messages (runs of 52 by bytes) and one call.
        assert!(sent.messages <= 8, "{sent:?}");
        assert!(sent.calls <= 1, "{sent:?}");
        let received = receive(&receiver, data.len()).await;
        for (index, (bytes, from)) in received.into_iter().enumerate() {
            assert_eq!(bytes, data[index], "datagram {index}");
            assert_eq!(from, sender.local_addr().unwrap());
        }
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
