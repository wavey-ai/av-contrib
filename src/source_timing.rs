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
//! events of the stall journal (`/api/stalls`). Each second's figures, per
//! socket and per source session, are kept for [`PACING_KEPT_S`] seconds:
//! the contributor's pacing record (`GET /api/pacing?since_s=`), which the
//! lab reads every few seconds without losing one. Linux only
//! (`SO_TIMESTAMPNS`), as the contributor is.

use crate::stall_journal::{self, StallKind, StallThread};
use serde::Serialize;
use std::collections::VecDeque;
use std::io;
use std::net::SocketAddr;
use std::sync::{Mutex, PoisonError};

/// Samples of one second that are kept for its percentiles; past this the
/// second's count, its count over the mark and its maximum still grow.
const RECEIVE_SAMPLES: usize = 65_536;
/// Epochs of one source in a second whose lateness is kept: 250 us epochs.
const SOURCE_SAMPLES: usize = 4_096;
/// An epoch this many blocks after the last one seen starts a new baseline
/// for `src_gap`: a jump that is not a gap.
const MAX_BLOCKS_AHEAD: u32 = 64;
/// A block this many blocks or fewer before the last one seen is a late
/// one (reordered or repaired) and not a turn; one further back starts a
/// new baseline: the source began again from block 0. The AEP1 worker
/// remembers the first arrival of as many blocks.
const MAX_BLOCKS_BEHIND: u32 = 2_048;
/// Seconds the pacing record keeps of each socket and source session.
pub const PACING_KEPT_S: u64 = 300;
/// Rows of the pacing record: 300 s of up to 6 DAW sockets, and of up to
/// 27 source sessions at once. Past this the oldest go first.
const RECEIVE_ROWS: usize = 2_048;
const SOURCE_ROWS: usize = 8_192;
/// A datagram that waited this long in the socket is counted in its
/// second (`rx_wait_over_200us`): D-hunt S3's rule is a p99.9 of 200 us or
/// more over a session, which the count decides exactly.
pub const RX_WAIT_MARK_US: u64 = 200;

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
    pub rx_wait_p999_us: u64,
    pub rx_wait_max_us: u64,
    /// Datagrams that waited [`RX_WAIT_MARK_US`] or more.
    pub rx_wait_over_200us: u64,
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

/// The contributor's pacing record (`GET /api/pacing`): every second kept
/// of each socket and source session, oldest first.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
pub struct PacingRecord {
    /// Seconds the record keeps.
    pub kept_s: u64,
    pub receive: Vec<ReceiveSecond>,
    pub sources: Vec<SourceSecond>,
}

static RECEIVE: Mutex<VecDeque<ReceiveSecond>> = Mutex::new(VecDeque::new());
static SOURCES: Mutex<VecDeque<SourceSecond>> = Mutex::new(VecDeque::new());

/// Every kept second from `since_s` on (Unix seconds, all when `None`)
/// that is younger than [`PACING_KEPT_S`] at `now_s`, oldest first. A
/// reader that comes back with `since_s` one after the last `unix_s` it saw
/// misses nothing: a second is published as the next one begins.
pub fn record(since_s: Option<u64>, now_s: u64) -> PacingRecord {
    fn rows<T: Copy>(table: &Mutex<VecDeque<T>>, keep: impl Fn(&T) -> bool) -> Vec<T> {
        table
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .iter()
            .copied()
            .filter(|row| keep(row))
            .collect()
    }
    let since_s = since_s.unwrap_or(0);
    let keep = |unix_s: u64| unix_s >= since_s && unix_s + PACING_KEPT_S > now_s;
    let mut receive = rows(&RECEIVE, |row: &ReceiveSecond| keep(row.unix_s));
    let mut sources = rows(&SOURCES, |row: &SourceSecond| keep(row.unix_s));
    receive.sort_by_key(|row| row.unix_s);
    sources.sort_by_key(|row| row.unix_s);
    PacingRecord {
        kept_s: PACING_KEPT_S,
        receive,
        sources,
    }
}

/// Add a second to its table, once a second per socket or source session.
/// The table is allocated whole the first time; past its rows, or older
/// than the kept seconds, the oldest go.
fn keep_row<T>(table: &Mutex<VecDeque<T>>, capacity: usize, row: T, unix_s: impl Fn(&T) -> u64) {
    let mut rows = table.lock().unwrap_or_else(PoisonError::into_inner);
    if rows.capacity() < capacity {
        rows.reserve_exact(capacity);
    }
    let newest = unix_s(&row);
    while rows.len() >= capacity
        || rows
            .front()
            .is_some_and(|oldest| unix_s(oldest) + PACING_KEPT_S <= newest)
    {
        rows.pop_front();
    }
    rows.push_back(row);
}

fn publish_receive(second: ReceiveSecond) {
    keep_row(&RECEIVE, RECEIVE_ROWS, second, |row| row.unix_s);
}

fn publish_source(second: SourceSecond) {
    keep_row(&SOURCES, SOURCE_ROWS, second, |row| row.unix_s);
}

/// A second's figures: its count, its count at or over the mark, and,
/// from its kept samples, its median, 99th and 99.9th percentiles (nearest
/// rank) and its maximum.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct SecondFigures {
    count: u64,
    over: u64,
    percentiles: Option<Percentiles>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Percentiles {
    p50: i64,
    p99: i64,
    p999: i64,
    max: i64,
}

/// The values of one second, kept without allocating after the start.
#[derive(Debug)]
struct SecondSamples {
    values: Vec<i64>,
    count: u64,
    /// Values at or over `mark` (`i64::MAX`: none counted).
    mark: i64,
    over: u64,
    max: Option<i64>,
}

impl SecondSamples {
    fn with_capacity(capacity: usize, mark: i64) -> Self {
        Self {
            values: Vec::with_capacity(capacity),
            count: 0,
            mark,
            over: 0,
            max: None,
        }
    }

    fn add(&mut self, value: i64) {
        self.count += 1;
        self.over += u64::from(value >= self.mark);
        self.max = Some(self.max.map_or(value, |max| max.max(value)));
        if self.values.len() < self.values.capacity() {
            self.values.push(value);
        }
    }

    /// The second's figures, and a fresh second.
    fn take(&mut self) -> SecondFigures {
        let count = std::mem::take(&mut self.count);
        let over = std::mem::take(&mut self.over);
        let max = self.max.take();
        let values = &mut self.values;
        let percentiles = max.filter(|_| !values.is_empty()).map(|max| {
            let n = values.len();
            let at = |per_mille: usize| (n * per_mille / 1_000).min(n - 1);
            let p999_at = at(999);
            let p999 = *values.select_nth_unstable(p999_at).1;
            let p99_at = at(990);
            let p99 = *values[..=p999_at].select_nth_unstable(p99_at).1;
            let p50_at = at(500);
            let p50 = *values[..=p99_at].select_nth_unstable(p50_at).1;
            Percentiles {
                p50,
                p99,
                p999,
                max,
            }
        });
        self.values.clear();
        SecondFigures {
            count,
            over,
            percentiles,
        }
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
            waits: SecondSamples::with_capacity(RECEIVE_SAMPLES, RX_WAIT_MARK_US as i64),
            unstamped: 0,
        }
    }

    /// A datagram that the loop read at `read_us`, which the kernel took in
    /// at `kernel_us`. Returns its arrival: the kernel's stamp, or the read
    /// when there is none. No allocation, lock or system call, but once a
    /// second and at an `rx_wait` event. Seconds only move forward: a read
    /// clock that steps back counts in the open second.
    pub fn datagram(&mut self, kernel_us: Option<u64>, read_us: u64) -> u64 {
        let second = read_us / 1_000_000;
        if second > self.unix_s {
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
        let figures = self.waits.take();
        let unstamped = std::mem::take(&mut self.unstamped);
        if figures.count == 0 {
            return;
        }
        let us = |value: i64| u64::try_from(value).unwrap_or(0);
        let waits = figures.percentiles.unwrap_or(Percentiles {
            p50: 0,
            p99: 0,
            p999: 0,
            max: 0,
        });
        publish_receive(ReceiveSecond {
            bind: self.bind,
            unix_s: self.unix_s,
            datagrams: figures.count,
            rx_wait_p50_us: us(waits.p50),
            rx_wait_p99_us: us(waits.p99),
            rx_wait_p999_us: us(waits.p999),
            rx_wait_max_us: us(waits.max),
            rx_wait_over_200us: figures.over,
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
            late: SecondSamples::with_capacity(SOURCE_SAMPLES, i64::MAX),
        }
    }

    /// Publish the figures gathered so far under the open second and the
    /// current session, and start again.
    fn publish(&mut self) {
        let figures = self.late.take();
        let epochs = std::mem::take(&mut self.epochs);
        let gap_max_us = std::mem::take(&mut self.gap_max_us);
        if epochs > 0 || gap_max_us > 0 {
            publish_source(SourceSecond {
                session_id: self.session_id,
                unix_s: self.unix_s,
                epochs,
                epoch_us: self.epoch_ns / 1_000,
                src_gap_max_us: gap_max_us,
                src_late_p50_us: figures.percentiles.map(|late| late.p50),
                src_late_max_us: figures.percentiles.map(|late| late.max),
            });
        }
    }

    /// Close the open second when `arrival_us` lies in a later one. Seconds
    /// only move forward: an epoch whose first arrival lies in a second
    /// already closed (completed after the next block opened) counts in
    /// the open one.
    fn roll(&mut self, arrival_us: u64) {
        let second = arrival_us / 1_000_000;
        if second <= self.unix_s {
            return;
        }
        self.publish();
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
            let behind = last.wrapping_sub(block_id);
            if ahead == 0 || (ahead > u32::MAX / 2 && behind <= MAX_BLOCKS_BEHIND) {
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

    /// An epoch completed whose first datagram arrived at `first_us`. An
    /// epoch of another session than the last (the source began again from
    /// the same address) closes the old session's figures and starts a new
    /// baseline for `src_gap`.
    pub fn epoch_completed(
        &mut self,
        first_us: u64,
        session_id: u64,
        pts_samples: u64,
        sample_rate: u32,
        frame_count: u32,
    ) {
        self.roll(first_us);
        if session_id != self.session_id {
            if self.session_id != 0 {
                self.publish();
                self.last_block = None;
            }
            self.session_id = session_id;
        }
        let rate = u128::from(sample_rate.max(1));
        self.epoch_ns = u64::try_from(u128::from(frame_count) * 1_000_000_000 / rate).unwrap_or(0);
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

    /// Tests that note events, or count on the kept seconds, take turns.
    fn turn() -> std::sync::MutexGuard<'static, ()> {
        stall_journal::PROCESS_JOURNAL
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
    }

    fn sources_of(session_id: u64, since_s: u64, now_s: u64) -> Vec<SourceSecond> {
        record(Some(since_s), now_s)
            .sources
            .into_iter()
            .filter(|second| second.session_id == session_id)
            .collect()
    }

    #[test]
    fn the_daw_socket_stamps_in_the_kernel() {
        let _turn = turn();
        let receiver = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
        enable_kernel_stamps(&receiver).unwrap();
        let sender = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
        let before_send_us = now_us();
        sender
            .send_to(b"epoch", receiver.local_addr().unwrap())
            .unwrap();
        // Loopback stamps the datagram as it is queued to the receiver,
        // inside the send.
        let after_send_us = now_us();
        // The datagram waits in the socket while nobody reads it.
        std::thread::sleep(Duration::from_millis(30));
        let read_us = now_us();
        let mut buf = [0_u8; 64];
        let (len, peer, stamp) = recv_stamped(&receiver, &mut buf).unwrap();
        assert_eq!(&buf[..len], b"epoch");
        assert_eq!(peer, sender.local_addr().unwrap());
        let stamp = stamp.expect("the kernel stamped the datagram");
        assert!(
            (before_send_us..=after_send_us).contains(&stamp),
            "{before_send_us} <= {stamp} <= {after_send_us}"
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
        let _turn = turn();
        let mut timing = SourceTiming::new();
        let base_s = now_us() / 1_000_000;
        let base = base_s * 1_000_000;
        let session_id = 7_777_000_u64;
        // Epochs of 1 ms (48 samples at 48 kHz), each on time.
        timing.block_opened(10, base);
        timing.epoch_completed(base, session_id, 480, 48_000, 48);
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
        // its figures are kept under the session.
        timing.epoch_completed(base + 901_000, session_id, 528, 48_000, 48);
        timing.block_opened(502, base + 1_000_000 + 2_000);
        let kept = sources_of(session_id, base_s, base_s + 1);
        assert_eq!(kept.len(), 1, "{kept:?}");
        let second = kept[0];
        assert_eq!(second.unix_s, base_s);
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
        let figures = timing.late.take();
        assert_eq!(figures.count, 2);
        assert_eq!(figures.percentiles.unwrap().max, 1_250);
        assert_eq!(timing.late.count, 0);
    }

    #[test]
    fn a_second_s_percentiles_come_from_its_samples() {
        let mut samples = SecondSamples::with_capacity(1_000, 90);
        for value in 1..=100 {
            samples.add(value);
        }
        let figures = samples.take();
        assert_eq!((figures.count, figures.over), (100, 11));
        assert_eq!(
            figures.percentiles,
            Some(Percentiles {
                p50: 51,
                p99: 100,
                p999: 100,
                max: 100
            })
        );
        // The 99.9th percentile of 2,000 values is the 1,999th.
        let mut samples = SecondSamples::with_capacity(2_000, 90);
        for value in 1..=2_000 {
            samples.add(value);
        }
        let figures = samples.take().percentiles.unwrap();
        assert_eq!(
            (figures.p99, figures.p999, figures.max),
            (1_981, 1_999, 2_000)
        );
        // Past its capacity the count, the count over the mark and the
        // maximum still grow.
        let mut samples = SecondSamples::with_capacity(4, 60);
        for value in [5, 1, 9, 3, 50, 70] {
            samples.add(value);
        }
        let figures = samples.take();
        assert_eq!((figures.count, figures.over), (6, 1));
        assert_eq!(figures.percentiles.unwrap().max, 70);
        assert_eq!(
            samples.take(),
            SecondFigures {
                count: 0,
                over: 0,
                percentiles: None
            }
        );
    }

    /// The lab reads the pacing record every 5 s: every second of the
    /// socket and of the source is there, with the rx_wait p99.9 and count
    /// over 200 us that S3's rule needs, and a cursor that takes the rest.
    #[test]
    fn every_second_is_kept_and_served_with_its_p999() {
        let _turn = turn();
        let bind: SocketAddr = "192.0.2.77:27100".parse().unwrap();
        let session_id = 9_191_000_u64;
        let base_s = now_us() / 1_000_000;
        let mut reads = ReceiveTiming::new(bind);
        let mut source = SourceTiming::new();
        for second in 0..8_u64 {
            let start = (base_s + second) * 1_000_000;
            // 2,000 datagrams a second: 1,991 read 10 us after their stamp,
            // 8 after 300 us and one after 5 ms.
            for index in 0..2_000_u64 {
                let wait = match index {
                    0..=1_990 => 10,
                    1_991..=1_998 => 300,
                    _ => 5_000,
                };
                let stamp = start + index * 400;
                reads.datagram(Some(stamp), stamp + wait);
            }
            // 1,000 epochs of 1 ms a second, on time.
            for epoch in 0..1_000_u64 {
                let block = second * 1_000 + epoch;
                let arrival = start + epoch * 1_000;
                source.block_opened(block as u32, arrival);
                source.epoch_completed(arrival, session_id, block * 48, 48_000, 48);
            }
        }
        // The first datagram and epoch of second 8 close second 7.
        let start = (base_s + 8) * 1_000_000;
        reads.datagram(Some(start), start + 10);
        source.block_opened(8_000, start);

        let now_s = base_s + 8;
        let receive = record(Some(base_s), now_s)
            .receive
            .into_iter()
            .filter(|second| second.bind == bind)
            .collect::<Vec<_>>();
        assert_eq!(
            receive
                .iter()
                .map(|second| second.unix_s)
                .collect::<Vec<_>>(),
            (base_s..base_s + 8).collect::<Vec<_>>()
        );
        for second in &receive {
            assert_eq!(second.datagrams, 2_000);
            assert_eq!(second.rx_wait_p50_us, 10);
            assert_eq!(second.rx_wait_p99_us, 10);
            assert_eq!(second.rx_wait_p999_us, 300);
            assert_eq!(second.rx_wait_max_us, 5_000);
            assert_eq!(second.rx_wait_over_200us, 9);
        }
        let sources = sources_of(session_id, base_s, now_s);
        assert_eq!(sources.len(), 8, "{sources:?}");
        assert!(sources
            .iter()
            .all(|second| second.epochs == 1_000 && second.src_gap_max_us == 0));

        // A reader that comes back after second 4 gets 5, 6 and 7.
        assert_eq!(sources_of(session_id, base_s + 5, now_s).len(), 3);
        // Seconds older than the record keeps leave it, though no source
        // has published since.
        assert!(sources_of(session_id, base_s, base_s + 7 + PACING_KEPT_S).is_empty());
        assert_eq!(
            sources_of(session_id, base_s, base_s + 6 + PACING_KEPT_S).len(),
            1
        );
    }

    /// An epoch that completes after the next block has opened in a later
    /// second counts in the open second, and the closed second's row stays
    /// whole.
    #[test]
    fn seconds_only_move_forward() {
        let _turn = turn();
        let session_id = 4_242_000_u64;
        let base_s = now_us() / 1_000_000;
        let s41 = base_s * 1_000_000;
        let mut timing = SourceTiming::new();
        // Second 41: 999 epochs of 1 ms, the last block opened at 41.9996.
        for epoch in 0..999_u64 {
            let arrival = s41 + epoch * 1_000;
            timing.block_opened(epoch as u32, arrival);
            timing.epoch_completed(arrival, session_id, epoch * 48, 48_000, 48);
        }
        timing.block_opened(999, s41 + 999_600);
        // Block 1000 opens at 42.0004 and closes second 41.
        timing.block_opened(1_000, s41 + 1_000_400);
        // Epoch 999 then completes, first arrival 41.9996.
        timing.epoch_completed(s41 + 999_600, session_id, 999 * 48, 48_000, 48);
        timing.block_opened(1_001, s41 + 1_001_400);
        let kept = sources_of(session_id, base_s, base_s + 1);
        assert_eq!(kept.len(), 1, "{kept:?}");
        assert_eq!((kept[0].unix_s, kept[0].epochs), (base_s, 999));
        // The late epoch is in second 42's row.
        timing.block_opened(2_000, s41 + 2_000_000);
        let kept = sources_of(session_id, base_s, base_s + 2);
        assert_eq!(kept.len(), 2, "{kept:?}");
        assert_eq!((kept[1].unix_s, kept[1].epochs), (base_s + 1, 1));
    }

    /// A source that begins again from block 0 on the same address (a new
    /// session) gets `src_gap` again from its second block, whether its
    /// old session was long or short.
    #[test]
    fn a_source_that_begins_again_gets_a_new_baseline() {
        let _turn = turn();
        let base = now_us() / 1_000_000 * 1_000_000;
        for (old_last, old, new) in [
            (5_000_u32, 6_161_000_u64, 6_162_000_u64),
            (10, 6_163_000, 6_164_000),
        ] {
            let mut timing = SourceTiming::new();
            for block in 0..=old_last {
                let arrival = base + u64::from(block) * 1_000;
                timing.block_opened(block, arrival);
                timing.epoch_completed(arrival, old, u64::from(block) * 48, 48_000, 48);
            }
            let restart = base + u64::from(old_last) * 1_000 + 20_000;
            assert_eq!(timing.block_opened(0, restart), None);
            timing.epoch_completed(restart, new, 0, 48_000, 48);
            assert_eq!(timing.block_opened(1, restart + 1_000), None);
            assert_eq!(timing.block_opened(2, restart + 2_000), Some(0));
            assert_eq!(timing.block_opened(3, restart + 6_000), Some(3_000));
            // The old session's figures were closed under its own id.
            let at_s = restart / 1_000_000;
            assert!(!sources_of(old, base / 1_000_000, at_s).is_empty());
        }
    }
}
