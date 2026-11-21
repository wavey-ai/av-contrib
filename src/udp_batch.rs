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
    #[cfg(target_os = "linux")]
    if batch {
        return linux::send_all(socket, messages).await;
    }
    let _ = batch;
    send_each(socket, messages).await
}

/// One awaited send per datagram: the behaviour without batching.
pub async fn send_each(socket: &UdpSocket, messages: &[Outbound<'_>]) -> io::Result<usize> {
    for message in messages {
        let sent = match message.target {
            Some(target) => socket.send_to(message.bytes, target).await?,
            None => socket.send(message.bytes).await?,
        };
        if sent != message.bytes.len() {
            return Err(io::Error::new(
                io::ErrorKind::WriteZero,
                "partial UDP datagram send",
            ));
        }
    }
    Ok(messages.len())
}

#[cfg(target_os = "linux")]
mod linux {
    use super::{Outbound, MESSAGES_PER_CALL};
    use std::io;
    use std::os::fd::AsRawFd;
    use tokio::io::Interest;
    use tokio::net::UdpSocket;

    pub(super) async fn send_all(
        socket: &UdpSocket,
        messages: &[Outbound<'_>],
    ) -> io::Result<usize> {
        let mut sent = 0;
        let mut calls = 0;
        while sent < messages.len() {
            // Try first: a socket with room in its buffer takes the batch
            // without a trip through the reactor.
            match socket.try_io(Interest::WRITABLE, || sendmmsg(socket, &messages[sent..])) {
                Ok(0) => {
                    return Err(io::Error::new(
                        io::ErrorKind::WriteZero,
                        "sendmmsg took no datagram",
                    ))
                }
                Ok(count) => {
                    sent += count;
                    calls += 1;
                }
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                    socket.writable().await?;
                }
                Err(error) => return Err(error),
            }
        }
        Ok(calls)
    }

    /// One `sendmmsg` call for up to [`MESSAGES_PER_CALL`] messages. Returns
    /// how many the kernel took.
    fn sendmmsg(socket: &UdpSocket, messages: &[Outbound<'_>]) -> io::Result<usize> {
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
}
