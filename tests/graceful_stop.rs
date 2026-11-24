//! The contributor's stop (A 2.6, AC7), on a child process: SIGTERM ends
//! ingest, drains the lanes, sends a last ledger (RLL1) on each lane and
//! exits 0; a second signal during the stop exits at once.

use std::io::{BufRead, BufReader};
use std::net::{TcpListener, UdpSocket};
use std::path::PathBuf;
use std::process::{Child, Command, ExitStatus, Stdio};
use std::sync::mpsc;
use std::time::{Duration, Instant};

/// How long a child may take to start or to stop.
const PATIENCE: Duration = Duration::from_secs(30);

/// A throwaway self-signed certificate and key for the child's HTTP server,
/// in a directory of the test's own.
fn tls(test: &str) -> (PathBuf, PathBuf) {
    let dir = PathBuf::from(env!("CARGO_TARGET_TMPDIR")).join(format!("graceful-stop-{test}"));
    std::fs::create_dir_all(&dir).unwrap();
    let (cert, key) = (dir.join("cert.pem"), dir.join("key.pem"));
    let made = Command::new("openssl")
        .args(["req", "-x509", "-newkey", "ec", "-pkeyopt"])
        .arg("ec_paramgen_curve:prime256v1")
        .args(["-nodes", "-days", "1", "-subj", "/CN=localhost", "-keyout"])
        .arg(&key)
        .arg("-out")
        .arg(&cert)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .expect("the test makes its certificate with openssl");
    assert!(made.success(), "openssl could not make a certificate");
    (cert, key)
}

/// An av-contrib with one RelaySession lane to `lane`, and its log lines.
struct Contributor {
    child: Child,
    lines: mpsc::Receiver<String>,
    log: Vec<String>,
}

impl Contributor {
    /// Start it and wait until it serves.
    fn start(test: &str, lane: &UdpSocket) -> Self {
        let (cert, key) = tls(test);
        let http_port = TcpListener::bind("127.0.0.1:0")
            .unwrap()
            .local_addr()
            .unwrap()
            .port();
        let mut child = Command::new(env!("CARGO_BIN_EXE_av-contrib"))
            .arg("--http-port")
            .arg(http_port.to_string())
            .arg("--cert")
            .arg(&cert)
            .arg("--key")
            .arg(&key)
            .arg("--relay-primary-target")
            .arg(lane.local_addr().unwrap().to_string())
            .args(["--node-id", "contrib-stop-test"])
            .env("RUST_LOG", "av_contrib=info")
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .spawn()
            .unwrap();
        let stdout = child.stdout.take().unwrap();
        let (sender, lines) = mpsc::channel();
        std::thread::spawn(move || {
            for line in BufReader::new(stdout).lines() {
                let Ok(line) = line else { break };
                if sender.send(line).is_err() {
                    break;
                }
            }
        });
        let mut contributor = Self {
            child,
            lines,
            log: Vec::new(),
        };
        contributor.wait_for_line("health:");
        contributor
    }

    fn wait_for_line(&mut self, text: &str) {
        let deadline = Instant::now() + PATIENCE;
        while !self.log.iter().any(|line| line.contains(text)) {
            let left = deadline.saturating_duration_since(Instant::now());
            match self.lines.recv_timeout(left) {
                Ok(line) => self.log.push(line),
                Err(_) => panic!("no line with {text:?}: {:#?}", self.log),
            }
        }
    }

    fn signal(&self, signal: libc::c_int) {
        let pid = libc::pid_t::try_from(self.child.id()).unwrap();
        // SAFETY: kill(2) on the child's pid, which the test owns.
        assert_eq!(unsafe { libc::kill(pid, signal) }, 0);
    }

    /// Wait for it to exit; its status, and every line it wrote.
    fn wait(mut self) -> (ExitStatus, Vec<String>) {
        let deadline = Instant::now() + PATIENCE;
        let status = loop {
            if let Some(status) = self.child.try_wait().unwrap() {
                break status;
            }
            if Instant::now() > deadline {
                let _ = self.child.kill();
                panic!("the contributor did not exit: {:#?}", self.log);
            }
            std::thread::sleep(Duration::from_millis(10));
        };
        // The reader ends with the pipe.
        while let Ok(line) = self.lines.recv_timeout(Duration::from_secs(5)) {
            self.log.push(line);
        }
        (status, self.log)
    }
}

/// Every datagram waiting on `socket`.
fn datagrams(socket: &UdpSocket) -> Vec<Vec<u8>> {
    socket
        .set_read_timeout(Some(Duration::from_millis(200)))
        .unwrap();
    let mut received = Vec::new();
    let mut buffer = [0_u8; 2_048];
    while let Ok(len) = socket.recv(&mut buffer) {
        received.push(buffer[..len].to_vec());
    }
    received
}

#[test]
fn sigterm_sends_a_last_ledger_on_each_lane_and_exits_0() {
    let lane = UdpSocket::bind("127.0.0.1:0").unwrap();
    let contributor = Contributor::start("ledger", &lane);
    assert!(
        datagrams(&lane).is_empty(),
        "nothing is published, so nothing is sent before the stop"
    );

    contributor.signal(libc::SIGTERM);
    let (status, log) = contributor.wait();

    assert!(status.success(), "{status:?}: {log:#?}");
    let has = |text: &str| log.iter().any(|line| line.contains(text));
    assert!(has("stopping: ingest ends"), "{log:#?}");
    assert!(has("stop complete"), "{log:#?}");
    assert!(!has("a second stop signal"), "{log:#?}");
    let received = datagrams(&lane);
    let last = received
        .last()
        .expect("the lane's forwarder hears the stop");
    assert_eq!(&last[..5], b"RLL1\x01", "the last datagram is the ledger");
    assert_eq!(last.len(), 16);
    assert_eq!(
        u64::from_be_bytes(last[8..16].try_into().unwrap()),
        0,
        "the ledger counts what the lane sent: nothing"
    );
}

#[test]
fn a_second_signal_during_the_stop_exits_at_once() {
    let lane = UdpSocket::bind("127.0.0.1:0").unwrap();
    let contributor = Contributor::start("second", &lane);
    // Both signals are pending when it runs again: the second is there
    // before the stop has done anything it could wait for.
    contributor.signal(libc::SIGSTOP);
    contributor.signal(libc::SIGTERM);
    contributor.signal(libc::SIGINT);
    contributor.signal(libc::SIGCONT);
    let (status, log) = contributor.wait();

    assert!(status.success(), "{status:?}: {log:#?}");
    let has = |text: &str| log.iter().any(|line| line.contains(text));
    assert!(has("stopping: ingest ends"), "{log:#?}");
    assert!(has("a second stop signal: exiting at once"), "{log:#?}");
    assert!(!has("stop complete"), "the stop did not finish: {log:#?}");
}
