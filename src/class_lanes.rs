//! Class isolation of the contributor's RelaySession lanes.
//!
//! With class lanes each lane is a pair of UDP sockets bound to the same
//! source endpoint (`SO_REUSEPORT`), so the parent's peer binding of the
//! contributor is unchanged: socket A carries audio streams to the parent's
//! port `P` with `SO_PRIORITY` 6 (band 0 of a `prio` qdisc), socket B
//! carries video streams to `P + offset` (1000 by default), optionally
//! paced with `SO_MAX_PACING_RATE` under `fq`. A 400-datagram 4K burst then
//! never sits ahead of an audio datagram in a socket buffer, and the parent
//! receives the classes on separate sockets.
//!
//! The thread helpers pin the calling thread to a CPU and give it
//! `SCHED_FIFO`; both are best effort, and a refusal (a rootless lab, a CPU
//! outside the cgroup) is reported for one log line and otherwise ignored.

use std::io;
use std::net::SocketAddr;

/// Default port offset of the video class: audio streams go to the
/// parent's port `P`, video streams to `P + 1000`.
pub const DEFAULT_CLASS_PORT_OFFSET: u16 = 1_000;

/// `SO_PRIORITY` of audio class sockets. 6 is the highest an unprivileged
/// process may set, and it maps to band 0 of the default `prio` priomap.
pub const DEFAULT_AUDIO_SOCKET_PRIORITY: u32 = 6;

/// Socket options of one class socket.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ClassSocketOptions {
    /// `SO_REUSEPORT`, so that the other class's socket can share the
    /// source endpoint.
    pub reuse_port: bool,
    /// `SO_PRIORITY`.
    pub priority: Option<u32>,
    /// `SO_MAX_PACING_RATE` in bytes per second (enforced by `fq`).
    pub max_pacing_rate_bytes_per_second: Option<u64>,
}

/// The video class port of a parent whose audio port is `audio`.
pub fn video_class_addr(audio: SocketAddr, offset: u16) -> io::Result<SocketAddr> {
    let port = audio.port().checked_add(offset).ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("{audio} plus the class port offset {offset} is not a port"),
        )
    })?;
    let mut video = audio;
    video.set_port(port);
    Ok(video)
}

/// A non-blocking UDP socket bound to `bind` with `options`, connected to
/// `peer` when one is given.
pub fn bind_class_socket(
    bind: SocketAddr,
    peer: Option<SocketAddr>,
    options: ClassSocketOptions,
) -> io::Result<std::net::UdpSocket> {
    let socket = socket2::Socket::new(
        socket2::Domain::for_address(bind),
        socket2::Type::DGRAM,
        Some(socket2::Protocol::UDP),
    )?;
    if options.reuse_port {
        set_reuse_port(&socket)?;
    }
    if let Some(priority) = options.priority {
        set_priority(&socket, priority)?;
    }
    if let Some(rate) = options.max_pacing_rate_bytes_per_second {
        set_max_pacing_rate(&socket, rate)?;
    }
    socket.set_nonblocking(true)?;
    socket.bind(&bind.into())?;
    if let Some(peer) = peer {
        socket.connect(&peer.into())?;
    }
    Ok(socket.into())
}

#[cfg(target_os = "linux")]
fn setsockopt_int<T: Copy>(
    socket: &socket2::Socket,
    level: libc::c_int,
    name: libc::c_int,
    value: T,
) -> io::Result<()> {
    use std::os::fd::AsRawFd;
    // SAFETY: `value` lives for the call, and its size is passed with it.
    let rc = unsafe {
        libc::setsockopt(
            socket.as_raw_fd(),
            level,
            name,
            std::ptr::addr_of!(value).cast(),
            std::mem::size_of::<T>() as libc::socklen_t,
        )
    };
    if rc < 0 {
        Err(io::Error::last_os_error())
    } else {
        Ok(())
    }
}

#[cfg(target_os = "linux")]
fn set_reuse_port(socket: &socket2::Socket) -> io::Result<()> {
    setsockopt_int(
        socket,
        libc::SOL_SOCKET,
        libc::SO_REUSEPORT,
        1 as libc::c_int,
    )
}

#[cfg(target_os = "linux")]
fn set_priority(socket: &socket2::Socket, priority: u32) -> io::Result<()> {
    let priority = libc::c_int::try_from(priority)
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "SO_PRIORITY out of range"))?;
    setsockopt_int(socket, libc::SOL_SOCKET, libc::SO_PRIORITY, priority)
}

#[cfg(target_os = "linux")]
fn set_max_pacing_rate(socket: &socket2::Socket, bytes_per_second: u64) -> io::Result<()> {
    // The kernel takes a 32-bit rate, or a 64-bit one since 4.13.
    setsockopt_int(
        socket,
        libc::SOL_SOCKET,
        libc::SO_MAX_PACING_RATE,
        bytes_per_second,
    )
}

#[cfg(not(target_os = "linux"))]
fn set_reuse_port(socket: &socket2::Socket) -> io::Result<()> {
    // SO_REUSEADDR lets a second UDP socket share the endpoint on the BSDs.
    socket.set_reuse_address(true)
}

#[cfg(not(target_os = "linux"))]
fn set_priority(_socket: &socket2::Socket, _priority: u32) -> io::Result<()> {
    Ok(())
}

#[cfg(not(target_os = "linux"))]
fn set_max_pacing_rate(_socket: &socket2::Socket, _rate: u64) -> io::Result<()> {
    Ok(())
}

/// `SO_PRIORITY` of a socket, for tests and status.
#[cfg(target_os = "linux")]
pub fn socket_priority(socket: &impl std::os::fd::AsRawFd) -> io::Result<u32> {
    let mut value: libc::c_int = 0;
    let mut len = std::mem::size_of::<libc::c_int>() as libc::socklen_t;
    // SAFETY: `value` and `len` outlive the call and describe the buffer.
    let rc = unsafe {
        libc::getsockopt(
            socket.as_raw_fd(),
            libc::SOL_SOCKET,
            libc::SO_PRIORITY,
            std::ptr::addr_of_mut!(value).cast(),
            &mut len,
        )
    };
    if rc < 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(u32::try_from(value).unwrap_or(0))
}

/// What a real-time thread got of what it asked for.
#[derive(Debug, Default, Clone)]
pub struct RealtimeOutcome {
    /// The CPU the thread is pinned to.
    pub pinned_cpu: Option<usize>,
    /// The `SCHED_FIFO` priority the thread runs at.
    pub fifo_priority: Option<u8>,
    /// Why a request was not granted, one entry per refusal.
    pub refused: Vec<String>,
}

/// Give the calling thread `SCHED_FIFO` at `fifo_priority` (when
/// non-zero) and pin it to `cpu` (when given). Every refusal is recorded
/// and the thread runs on as it was. A thread that asked for `SCHED_FIFO`
/// and was refused it is not pinned either: without the priority a pinned
/// thread only waits for whatever else runs on its CPU (a rootless lab
/// shares its CPUs among all of its nodes).
pub fn make_current_thread_realtime(cpu: Option<usize>, fifo_priority: u8) -> RealtimeOutcome {
    let mut outcome = RealtimeOutcome::default();
    if fifo_priority > 0 {
        match set_current_thread_fifo(fifo_priority) {
            Ok(()) => outcome.fifo_priority = Some(fifo_priority),
            Err(error) => outcome
                .refused
                .push(format!("SCHED_FIFO {fifo_priority}: {error}")),
        }
    }
    if let Some(cpu) = cpu {
        if fifo_priority > 0 && outcome.fifo_priority.is_none() {
            outcome
                .refused
                .push(format!("CPU {cpu}: not pinned without SCHED_FIFO"));
        } else {
            match pin_current_thread(cpu) {
                Ok(()) => outcome.pinned_cpu = Some(cpu),
                Err(error) => outcome.refused.push(format!("CPU {cpu}: {error}")),
            }
        }
    }
    outcome
}

/// Put the calling thread back to normal scheduling (`SCHED_OTHER`) on
/// the CPUs of the process's main thread. A thread started by a pinned
/// `SCHED_FIFO` thread inherits both (glibc's `PTHREAD_INHERIT_SCHED`):
/// a runtime's blocking pool under an audio hot thread would otherwise run
/// at real-time priority on the audio CPU. Best effort: the outcome lists
/// what was refused.
pub fn reset_current_thread_scheduling() -> RealtimeOutcome {
    let mut outcome = RealtimeOutcome::default();
    if let Err(error) = set_current_thread_normal() {
        outcome.refused.push(format!("SCHED_OTHER: {error}"));
    }
    if let Err(error) = unpin_current_thread() {
        outcome.refused.push(format!("CPU mask: {error}"));
    }
    outcome
}

/// Whether the calling thread runs with `SCHED_FIFO`.
#[cfg(target_os = "linux")]
pub fn current_thread_is_fifo() -> bool {
    // SAFETY: 0 is the calling thread.
    unsafe { libc::sched_getscheduler(0) == libc::SCHED_FIFO }
}

#[cfg(not(target_os = "linux"))]
pub fn current_thread_is_fifo() -> bool {
    false
}

#[cfg(target_os = "linux")]
fn set_current_thread_normal() -> io::Result<()> {
    let param = libc::sched_param { sched_priority: 0 };
    // SAFETY: `param` outlives the call; 0 is the calling thread. Leaving
    // a real-time policy needs no privilege.
    if unsafe { libc::sched_setscheduler(0, libc::SCHED_OTHER, &param) } != 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(())
}

/// Give the calling thread the CPU mask of the process's main thread
/// (whose thread id is the process id), which nothing here pins.
#[cfg(target_os = "linux")]
fn unpin_current_thread() -> io::Result<()> {
    // SAFETY: a zeroed cpu_set_t is a valid (empty) set; both calls get
    // its true size, and the set outlives them.
    unsafe {
        let mut set: libc::cpu_set_t = std::mem::zeroed();
        if libc::sched_getaffinity(
            libc::getpid(),
            std::mem::size_of::<libc::cpu_set_t>(),
            &mut set,
        ) != 0
        {
            return Err(io::Error::last_os_error());
        }
        if libc::sched_setaffinity(0, std::mem::size_of::<libc::cpu_set_t>(), &set) != 0 {
            return Err(io::Error::last_os_error());
        }
    }
    Ok(())
}

#[cfg(not(target_os = "linux"))]
fn set_current_thread_normal() -> io::Result<()> {
    Ok(())
}

#[cfg(not(target_os = "linux"))]
fn unpin_current_thread() -> io::Result<()> {
    Ok(())
}

/// Audio hot threads for `formats` renditions when the first is pinned to
/// `first_cpu` of a host with `cpus` CPUs: one per format, as many as the
/// CPUs from `first_cpu` on hold (each thread has a CPU of its own), at
/// least one.
pub fn auto_audio_hot_threads(formats: usize, first_cpu: Option<usize>, cpus: usize) -> usize {
    let formats = formats.max(1);
    match first_cpu {
        Some(cpu) if cpu < cpus => formats.min(cpus - cpu),
        _ => formats,
    }
}

#[cfg(target_os = "linux")]
fn pin_current_thread(cpu: usize) -> io::Result<()> {
    if cpu >= libc::CPU_SETSIZE as usize {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "CPU index beyond CPU_SETSIZE",
        ));
    }
    // SAFETY: a zeroed cpu_set_t is the empty set; CPU_SET writes within it
    // (the index is checked above), and the set outlives the call.
    unsafe {
        let mut set: libc::cpu_set_t = std::mem::zeroed();
        libc::CPU_SET(cpu, &mut set);
        if libc::sched_setaffinity(0, std::mem::size_of::<libc::cpu_set_t>(), &set) != 0 {
            return Err(io::Error::last_os_error());
        }
    }
    Ok(())
}

#[cfg(target_os = "linux")]
fn set_current_thread_fifo(priority: u8) -> io::Result<()> {
    let param = libc::sched_param {
        sched_priority: libc::c_int::from(priority),
    };
    // SAFETY: `param` outlives the call; 0 is the calling thread.
    if unsafe { libc::sched_setscheduler(0, libc::SCHED_FIFO, &param) } != 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(())
}

#[cfg(not(target_os = "linux"))]
fn pin_current_thread(_cpu: usize) -> io::Result<()> {
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        "CPU pinning is Linux only",
    ))
}

#[cfg(not(target_os = "linux"))]
fn set_current_thread_fifo(_priority: u8) -> io::Result<()> {
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        "SCHED_FIFO is Linux only",
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_video_port_is_the_audio_port_plus_the_offset() {
        let audio: SocketAddr = "10.0.0.2:22001".parse().unwrap();
        assert_eq!(
            video_class_addr(audio, DEFAULT_CLASS_PORT_OFFSET).unwrap(),
            "10.0.0.2:23001".parse::<SocketAddr>().unwrap()
        );
        let high: SocketAddr = "10.0.0.2:65000".parse().unwrap();
        assert!(video_class_addr(high, 1_000).is_err());
    }

    #[test]
    fn two_class_sockets_share_one_source_endpoint() {
        let audio_peer = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
        let video_peer = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
        let reserve = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
        let bind = reserve.local_addr().unwrap();
        drop(reserve);
        let audio = bind_class_socket(
            bind,
            Some(audio_peer.local_addr().unwrap()),
            ClassSocketOptions {
                reuse_port: true,
                priority: Some(DEFAULT_AUDIO_SOCKET_PRIORITY),
                max_pacing_rate_bytes_per_second: None,
            },
        )
        .unwrap();
        let video = bind_class_socket(
            bind,
            Some(video_peer.local_addr().unwrap()),
            ClassSocketOptions {
                reuse_port: true,
                priority: None,
                max_pacing_rate_bytes_per_second: Some(50_000_000),
            },
        )
        .unwrap();
        assert_eq!(audio.local_addr().unwrap(), bind);
        assert_eq!(video.local_addr().unwrap(), bind);
        #[cfg(target_os = "linux")]
        {
            assert_eq!(socket_priority(&audio).unwrap(), 6);
            assert_eq!(socket_priority(&video).unwrap(), 0);
        }
        audio.send(b"audio").unwrap();
        video.send(b"video").unwrap();
        let mut buffer = [0_u8; 16];
        audio_peer
            .set_read_timeout(Some(std::time::Duration::from_secs(2)))
            .unwrap();
        video_peer
            .set_read_timeout(Some(std::time::Duration::from_secs(2)))
            .unwrap();
        let (len, from) = audio_peer.recv_from(&mut buffer).unwrap();
        assert_eq!((&buffer[..len], from), (&b"audio"[..], bind));
        let (len, from) = video_peer.recv_from(&mut buffer).unwrap();
        assert_eq!((&buffer[..len], from), (&b"video"[..], bind));
    }

    #[test]
    fn a_refused_realtime_request_is_reported_not_fatal() {
        std::thread::spawn(|| {
            // CPU_SETSIZE is never a CPU of this machine.
            let outcome = make_current_thread_realtime(Some(libc_cpu_setsize()), 0);
            assert!(outcome.pinned_cpu.is_none());
            assert_eq!(outcome.refused.len(), 1);
        })
        .join()
        .unwrap();
    }

    #[test]
    fn a_thread_refused_fifo_is_not_pinned() {
        std::thread::spawn(|| {
            let outcome = make_current_thread_realtime(Some(0), 1);
            if outcome.fifo_priority.is_some() {
                // A privileged run: both granted.
                assert_eq!(outcome.pinned_cpu, Some(0));
            } else {
                assert!(outcome.pinned_cpu.is_none());
                assert_eq!(outcome.refused.len(), 2, "{:?}", outcome.refused);
                assert!(outcome.refused[1].contains("not pinned"));
            }
        })
        .join()
        .unwrap();
    }

    #[test]
    fn audio_hot_threads_are_one_per_format_within_the_cpus_left() {
        // flac, pcm and opus on a 4-CPU contributor from CPU 2: CPUs 2, 3.
        assert_eq!(auto_audio_hot_threads(3, Some(2), 4), 2);
        assert_eq!(auto_audio_hot_threads(3, Some(2), 8), 3);
        assert_eq!(auto_audio_hot_threads(1, Some(2), 8), 1);
        assert_eq!(auto_audio_hot_threads(0, Some(2), 8), 1);
        // Not pinned, or pinned beyond the CPUs: one per format.
        assert_eq!(auto_audio_hot_threads(3, None, 2), 3);
        assert_eq!(auto_audio_hot_threads(3, Some(9), 4), 3);
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn a_reset_thread_runs_normally_on_the_process_cpus() {
        std::thread::spawn(|| {
            // Whatever the host grants (a pin to CPU 0, FIFO as root), the
            // reset undoes it.
            let _ = make_current_thread_realtime(Some(0), 1);
            let outcome = reset_current_thread_scheduling();
            assert!(outcome.refused.is_empty(), "{:?}", outcome.refused);
            assert!(!current_thread_is_fifo());
            // SAFETY: zeroed sets are valid; the calls get their size.
            unsafe {
                let mut own: libc::cpu_set_t = std::mem::zeroed();
                let mut main: libc::cpu_set_t = std::mem::zeroed();
                let size = std::mem::size_of::<libc::cpu_set_t>();
                assert_eq!(libc::sched_getaffinity(0, size, &mut own), 0);
                assert_eq!(libc::sched_getaffinity(libc::getpid(), size, &mut main), 0);
                assert_eq!(libc::CPU_COUNT(&own), libc::CPU_COUNT(&main));
            }
        })
        .join()
        .unwrap();
    }

    fn libc_cpu_setsize() -> usize {
        #[cfg(target_os = "linux")]
        {
            libc::CPU_SETSIZE as usize
        }
        #[cfg(not(target_os = "linux"))]
        {
            1_024
        }
    }
}
