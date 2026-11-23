//! When the AEP1 source's epochs reached the contributor, on one clock
//! (D-hunt 2.8, S2).
//!
//! The kernel stamps each datagram of the DAW media socket as it takes it
//! in (`SO_TIMESTAMPNS`, CLOCK_REALTIME). That stamp, not the time the
//! receive loop read the datagram, is the datagram's arrival: `src_first`
//! and `src_last` in the hop journal, and every figure here. Three things
//! are then told apart:
//!
//! - `rx_wait`: the read less the kernel stamp, per datagram: the receive
//!   thread came late to its socket. A property of the reader, so it is
//!   kept per socket.
//! - `src_gap`: an epoch's first datagram arrived this long after its turn,
//!   the turn being the previous epoch's first arrival plus one epoch (plus
//!   one epoch for each epoch between them that never arrived). One clock,
//!   this host's: the source sent late or the link held it.
//! - `src_late`: the first arrival less the epoch's own instant on the
//!   source's timeline (`session_id + pts`). Two clocks, the source's and
//!   this host's: a figure to watch, never to split less than 0.3 ms.
//!
//! An `rx_wait` of 1 ms or more and a `src_gap` of 2 ms or more are also
//! events of the stall journal (`/api/stalls`). Each second's figures,
//! per socket and per source session, are kept for the status. Linux only
//! (`SO_TIMESTAMPNS`), as the contributor is.

use crate::stall_journal::{self, StallKind, StallThread};
use serde::Serialize;
use std::io;
use std::net::SocketAddr;
use std::sync::{Mutex, PoisonError};

/// Samples of one second that are kept for its median and its 99th
/// percentile; past this the second's count and maximum still grow.
const RECEIVE_SAMPLES: usize = 65_536;
/// Epochs of one source in a second whose lateness is kept: 250 us epochs.
const SOURCE_SAMPLES: usize = 4_096;
/// An epoch this many blocks after the last one seen starts a new baseline
/// for `src_gap`: a new session, or a jump that is not a gap.
const MAX_BLOCKS_AHEAD: u32 = 64;
/// A source session whose last second is this old leaves the status.
const SOURCE_KEPT_S: u64 = 60;

/// One second's figures of the DAW media socket's reads.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct ReceiveSecond {
    pub bind: SocketAddr,
    /// The second (Unix seconds, by the read) these figures cover.
    pub unix_s: u64,
    pub datagrams: u64,
    /// Read less kernel stamp.
    pub rx_wait_p50_us: u64,
    pub rx_wait_p99_us: u64,
    pub rx_wait_max_us: u64,
    /// Datagrams that came without a kernel stamp (their read time stands
    /// for their arrival).
    pub unstamped: u64,
}

/// One second's figures of one source session's epochs.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct SourceSecond {
    pub session_id: u64,
    /// The second (Unix seconds, by the kernel arrival) these cover.
    pub unix_s: u64,
    /// Epochs that completed in the second.
    pub epochs: u64,
    /// The source's epoch, as its last completed epoch gave it.
    pub epoch_us: u64,
    /// The longest an epoch's first arrival came after its turn (one
    /// clock).
    pub src_gap_max_us: u64,
    /// First arrival less `session_id + pts`: two clocks.
    pub src_late_p50_us: Option<i64>,
    pub src_late_max_us: Option<i64>,
}

/// What `/api/status` shows: the last whole second of each.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
pub struct SourceTimingSnapshot {
    pub receive: Vec<ReceiveSecond>,
    pub sources: Vec<SourceSecond>,
}

static RECEIVE: Mutex<Vec<ReceiveSecond>> = Mutex::new(Vec::new());
static SOURCES: Mutex<Vec<SourceSecond>> = Mutex::new(Vec::new());

/// The last whole second of every socket and of every source session
/// heard in the last minute.
pub fn snapshot() -> SourceTimingSnapshot {
    SourceTimingSnapshot {
        receive: RECEIVE
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone(),
        sources: SOURCES
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone(),
    }
}

/// Once a second per socket.
fn publish_receive(second: ReceiveSecond) {
    let mut table = RECEIVE.lock().unwrap_or_else(PoisonError::into_inner);
    match table.iter_mut().find(|kept| kept.bind == second.bind) {
        Some(kept) => *kept = second,
        None => table.push(second),
    }
}

/// Once a second per source session.
fn publish_source(second: SourceSecond) {
    let mut table = SOURCES.lock().unwrap_or_else(PoisonError::into_inner);
    table.retain(|kept| {
        kept.session_id != second.session_id && kept.unix_s + SOURCE_KEPT_S > second.unix_s
    });
    table.push(second);
}

/// The values of one second, kept without allocating after the start.
#[derive(Debug)]
struct SecondSamples {
    values: Vec<i64>,
    count: u64,
    max: Option<i64>,
}

impl SecondSamples {
    fn with_capacity(capacity: usize) -> Self {
        Self {
            values: Vec::with_capacity(capacity),
            count: 0,
            max: None,
        }
    }

    fn add(&mut self, value: i64) {
        self.count += 1;
        self.max = Some(self.max.map_or(value, |max| max.max(value)));
        if self.values.len() < self.values.capacity() {
            self.values.push(value);
        }
    }

    /// The second's count, median, 99th percentile and maximum, and a
    /// fresh second.
    fn take(&mut self) -> (u64, Option<(i64, i64, i64)>) {
        let count = std::mem::take(&mut self.count);
        let max = self.max.take();
        let figures = max.and_then(|max| {
            let values = &mut self.values;
            if values.is_empty() {
                return None;
            }
            let p99_at = (values.len() * 99 / 100).min(values.len() - 1);
            let p99 = *values.select_nth_unstable(p99_at).1;
            let p50_at = values.len() / 2;
            let p50 = *values[..=p99_at].select_nth_unstable(p50_at.min(p99_at)).1;
            Some((p50, p99, max))
        });
        self.values.clear();
        (count, figures)
    }
}

/// The receive loop's view of its socket: the arrival of each datagram and
/// its wait, a second at a time.
#[derive(Debug)]
pub struct ReceiveTiming {
    bind: SocketAddr,
    unix_s: u64,
    waits: SecondSamples,
    unstamped: u64,
}

impl ReceiveTiming {
    pub fn new(bind: SocketAddr) -> Self {
        Self {
            bind,
            unix_s: 0,
            waits: SecondSamples::with_capacity(RECEIVE_SAMPLES),
            unstamped: 0,
        }
    }

    /// A datagram that the loop read at `read_us`, which the kernel took in
    /// at `kernel_us`. Returns its arrival: the kernel's stamp, or the read
    /// when there is none. No allocation, lock or system call, but once a
    /// second and at an `rx_wait` event.
    pub fn datagram(&mut self, kernel_us: Option<u64>, read_us: u64) -> u64 {
        let second = read_us / 1_000_000;
        if second != self.unix_s {
            self.close_second();
            self.unix_s = second;
        }
        let Some(kernel_us) = kernel_us else {
            self.unstamped += 1;
            self.waits.count += 1;
            return read_us;
        };
        let wait_us = read_us.saturating_sub(kernel_us);
        self.waits.add(i64::try_from(wait_us).unwrap_or(i64::MAX));
        stall_journal::note(StallKind::RxWait, StallThread::Receive, wait_us, None);
        kernel_us
    }

    fn close_second(&mut self) {
        let (datagrams, figures) = self.waits.take();
        let unstamped = std::mem::take(&mut self.unstamped);
        if datagrams == 0 {
            return;
        }
        let (p50, p99, max) = figures.unwrap_or((0, 0, 0));
        let us = |value: i64| u64::try_from(value).unwrap_or(0);
        publish_receive(ReceiveSecond {
            bind: self.bind,
            unix_s: self.unix_s,
            datagrams,
            rx_wait_p50_us: us(p50),
            rx_wait_p99_us: us(p99),
            rx_wait_max_us: us(max),
            unstamped,
        });
    }
}

/// One source's epochs as they arrived, for `src_gap` and `src_late`. The
/// AEP1 worker keeps one per source address, beside its FEC receiver.
#[derive(Debug)]
pub struct SourceTiming {
    /// The newest block (epoch) seen and its first arrival.
    last_block: Option<(u32, u64)>,
    /// The source's epoch in nanoseconds, from its last completed epoch.
    epoch_ns: u64,
    session_id: u64,
    unix_s: u64,
    epochs: u64,
    gap_max_us: u64,
    late: SecondSamples,
}

impl Default for SourceTiming {
    fn default() -> Self {
        Self::new()
    }
}

impl SourceTiming {
    pub fn new() -> Self {
        Self {
            last_block: None,
            epoch_ns: 0,
            session_id: 0,
            unix_s: 0,
            epochs: 0,
            gap_max_us: 0,
            late: SecondSamples::with_capacity(SOURCE_SAMPLES),
        }
    }

    fn roll(&mut self, arrival_us: u64) {
        let second = arrival_us / 1_000_000;
        if second == self.unix_s {
            return;
        }
        let (_, figures) = self.late.take();
        let epochs = std::mem::take(&mut self.epochs);
        let gap_max_us = std::mem::take(&mut self.gap_max_us);
        if epochs > 0 || gap_max_us > 0 {
            publish_source(SourceSecond {
                session_id: self.session_id,
                unix_s: self.unix_s,
                epochs,
                epoch_us: self.epoch_ns / 1_000,
                src_gap_max_us: gap_max_us,
                src_late_p50_us: figures.map(|(p50, _, _)| p50),
                src_late_max_us: figures.map(|(_, _, max)| max),
            });
        }
        self.unix_s = second;
    }

    /// The first datagram of block `block_id` arrived at `arrival_us` (the
    /// kernel's stamp). Returns the block's `src_gap` when its turn is
    /// known: the previous block's arrival plus one epoch a block.
    pub fn block_opened(&mut self, block_id: u32, arrival_us: u64) -> Option<u64> {
        self.roll(arrival_us);
        let mut gap = None;
        if let Some((last, last_us)) = self.last_block {
            let ahead = block_id.wrapping_sub(last);
            if ahead == 0 || ahead > u32::MAX / 2 {
                // An older block, reordered or repaired: not a turn.
                return None;
            }
            if ahead <= MAX_BLOCKS_AHEAD && self.epoch_ns > 0 {
                let turn_us = last_us + u64::from(ahead) * self.epoch_ns / 1_000;
                let gap_us = arrival_us.saturating_sub(turn_us);
                self.gap_max_us = self.gap_max_us.max(gap_us);
                stall_journal::note(StallKind::SrcGap, StallThread::Receive, gap_us, None);
                gap = Some(gap_us);
            }
        }
        self.last_block = Some((block_id, arrival_us));
        gap
    }

    /// An epoch completed whose first datagram arrived at `first_us`.
    pub fn epoch_completed(
        &mut self,
        first_us: u64,
        session_id: u64,
        pts_samples: u64,
        sample_rate: u32,
        frame_count: u32,
    ) {
        self.roll(first_us);
        let rate = u128::from(sample_rate.max(1));
        self.epoch_ns = u64::try_from(u128::from(frame_count) * 1_000_000_000 / rate).unwrap_or(0);
        self.session_id = session_id;
        self.epochs += 1;
        let instant_us =
            (u128::from(session_id) / 1_000 + u128::from(pts_samples) * 1_000_000 / rate) as i128;
        let late = i128::from(first_us) - instant_us;
        self.late
            .add(i64::try_from(late).unwrap_or(if late < 0 { i64::MIN } else { i64::MAX }));
    }
}

/// Have the kernel stamp each datagram the socket takes in.
pub fn enable_kernel_stamps(socket: &impl std::os::fd::AsRawFd) -> io::Result<()> {
    let on: libc::c_int = 1;
    // SAFETY: an int option of an open socket.
    let rc = unsafe {
        libc::setsockopt(
            socket.as_raw_fd(),
            libc::SOL_SOCKET,
            libc::SO_TIMESTAMPNS,
            std::ptr::addr_of!(on).cast(),
            std::mem::size_of::<libc::c_int>() as libc::socklen_t,
        )
    };
    if rc != 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(())
}

/// Read one datagram without waiting: its length, its sender and the
/// kernel's stamp (CLOCK_REALTIME, us) when it carries one. One `recvmsg`,
/// with the control buffer on the stack.
pub fn recv_stamped(
    socket: &impl std::os::fd::AsRawFd,
    buf: &mut [u8],
) -> io::Result<(usize, SocketAddr, Option<u64>)> {
    let fd = socket.as_raw_fd();
    let mut control = [0_u64; 8];
    // SAFETY: the storage and its length are the ones `try_init` hands over,
    // and `recvmsg` writes the sender's address into them as a sockaddr of
    // its family, with its length.
    let ((read, stamp), peer) = unsafe {
        socket2::SockAddr::try_init(|storage, len| {
            let mut iov = libc::iovec {
                iov_base: buf.as_mut_ptr().cast(),
                iov_len: buf.len(),
            };
            // SAFETY: a zeroed header is valid; every buffer it names
            // outlives the call.
            let mut header: libc::msghdr = std::mem::zeroed();
            header.msg_name = storage.cast();
            header.msg_namelen = *len;
            header.msg_iov = &mut iov;
            header.msg_iovlen = 1;
            header.msg_control = control.as_mut_ptr().cast();
            header.msg_controllen = std::mem::size_of_val(&control) as _;
            let read = libc::recvmsg(fd, &mut header, libc::MSG_DONTWAIT);
            if read < 0 {
                return Err(io::Error::last_os_error());
            }
            *len = header.msg_namelen;
            let mut stamp = None;
            // SAFETY: the kernel filled the control buffer as the header
            // says; each message is one it wrote.
            let mut cmsg = libc::CMSG_FIRSTHDR(&header);
            while !cmsg.is_null() {
                if (*cmsg).cmsg_level == libc::SOL_SOCKET
                    && (*cmsg).cmsg_type == libc::SCM_TIMESTAMPNS
                {
                    let spec =
                        std::ptr::read_unaligned(libc::CMSG_DATA(cmsg).cast::<libc::timespec>());
                    stamp = Some(spec.tv_sec as u64 * 1_000_000 + spec.tv_nsec as u64 / 1_000);
                }
                cmsg = libc::CMSG_NXTHDR(&header, cmsg);
            }
            Ok((read as usize, stamp))
        })?
    };
    let peer = peer.as_socket().ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            "a datagram from a non-IP sender",
        )
    })?;
    Ok((read, peer, stamp))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{Duration, SystemTime, UNIX_EPOCH};

    fn now_us() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_micros() as u64
    }

    #[test]
    fn the_daw_socket_stamps_in_the_kernel() {
        // Its events go to the process's journal, if a test installed one.
        let _turn = stall_journal::PROCESS_JOURNAL
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        let receiver = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
        enable_kernel_stamps(&receiver).unwrap();
        let sender = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
        let sent_us = now_us();
        sender
            .send_to(b"epoch", receiver.local_addr().unwrap())
            .unwrap();
        // The datagram waits in the socket while nobody reads it.
        std::thread::sleep(Duration::from_millis(30));
        let read_us = now_us();
        let mut buf = [0_u8; 64];
        let (len, peer, stamp) = recv_stamped(&receiver, &mut buf).unwrap();
        assert_eq!(&buf[..len], b"epoch");
        assert_eq!(peer, sender.local_addr().unwrap());
        let stamp = stamp.expect("the kernel stamped the datagram");
        assert!(
            stamp >= sent_us && stamp < sent_us + 10_000,
            "{stamp} {sent_us}"
        );
        assert!(read_us - stamp >= 29_000, "the wait is in the socket");

        // The arrival is the stamp, and the wait was booked.
        let mut timing = ReceiveTiming::new(receiver.local_addr().unwrap());
        assert_eq!(timing.datagram(Some(stamp), read_us), stamp);
        assert_eq!(timing.datagram(None, read_us), read_us);
        assert_eq!(timing.waits.count, 2);
        assert_eq!(timing.waits.max, Some((read_us - stamp) as i64));

        // Nothing waiting: the read does not block.
        let error = recv_stamped(&receiver, &mut buf).unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::WouldBlock);
    }

    #[test]
    fn a_source_gap_is_booked_on_one_clock() {
        // Its events go to the process's journal, if a test installed one.
        let _turn = stall_journal::PROCESS_JOURNAL
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        let mut timing = SourceTiming::new();
        let base = 1_795_381_726_000_000_u64;
        // Epochs of 1 ms (48 samples at 48 kHz), each on time.
        timing.block_opened(10, base);
        timing.epoch_completed(base, 1_000, 480, 48_000, 48);
        assert_eq!(timing.block_opened(11, base + 1_000), Some(0));
        // Block 12 arrives 7.5 ms after its turn.
        assert_eq!(timing.block_opened(12, base + 2_000 + 7_500), Some(7_500));
        // Block 13 left at once behind it: early against its turn.
        assert_eq!(timing.block_opened(13, base + 9_510), Some(0));
        // Block 14 never came; block 15's turn is two epochs on.
        assert_eq!(timing.block_opened(15, base + 11_610), Some(100));
        // An older block (a late repair) is not a turn.
        assert_eq!(timing.block_opened(14, base + 11_700), None);
        assert_eq!(timing.gap_max_us, 7_500);
        // A jump of more than 64 blocks starts a new baseline.
        assert_eq!(timing.block_opened(500, base + 900_000), None);
        assert_eq!(timing.block_opened(501, base + 901_000), Some(0));

        // The second closes when an arrival moves into the next one, and
        // its figures are published under the session.
        let session_id = 7_777_000_u64;
        timing.epoch_completed(base + 901_000, session_id, 48, 48_000, 48);
        timing.block_opened(502, base + 1_000_000 + 2_000);
        let second = snapshot()
            .sources
            .into_iter()
            .find(|second| second.session_id == session_id)
            .expect("the second was published");
        assert_eq!(second.unix_s, base / 1_000_000);
        assert_eq!(second.epochs, 2);
        assert_eq!(second.epoch_us, 1_000);
        assert_eq!(second.src_gap_max_us, 7_500);
        assert!(second.src_late_max_us.is_some());
    }

    #[test]
    fn the_lateness_is_the_arrival_less_the_epoch_s_instant() {
        let mut timing = SourceTiming::new();
        // A session that started at Unix 1,000 s; the epoch at pts 48,000
        // (1 s in) arrived 1.25 ms after its instant.
        let session_ns = 1_000_000_000_000_u64;
        timing.epoch_completed(1_001_001_250, session_ns, 48_000, 48_000, 120);
        // And one sent 300 us ahead of its time.
        timing.epoch_completed(1_001_002_200, session_ns, 48_120, 48_000, 120);
        assert_eq!(timing.epoch_ns, 2_500_000);
        let (count, figures) = timing.late.take();
        assert_eq!(count, 2);
        let (_, _, max) = figures.unwrap();
        assert_eq!(max, 1_250);
        assert_eq!(timing.late.count, 0);
    }

    #[test]
    fn a_second_s_percentiles_come_from_its_samples() {
        let mut samples = SecondSamples::with_capacity(1_000);
        for value in 1..=100 {
            samples.add(value);
        }
        let (count, figures) = samples.take();
        assert_eq!(count, 100);
        assert_eq!(figures, Some((51, 100, 100)));
        // Past its capacity the count and maximum still grow.
        let mut samples = SecondSamples::with_capacity(4);
        for value in [5, 1, 9, 3, 50, 70] {
            samples.add(value);
        }
        let (count, figures) = samples.take();
        assert_eq!(count, 6);
        assert_eq!(figures.unwrap().2, 70);
        assert_eq!(samples.take(), (0, None));
    }
}
