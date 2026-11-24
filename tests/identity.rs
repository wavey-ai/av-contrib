//! The contributor's identity (A 2.4, 2.6, AC6), on child processes:
//! `--print-fingerprint` binds nothing, reads no file and prints only the
//! identity; and two contributors fed one AEP1 datagram file report equal
//! window digests on `GET /api/node`.

use std::io::{BufRead, BufReader, Read, Write};
use std::net::{SocketAddr, TcpListener, UdpSocket};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

/// How long a child may take to start, or a report to show what the test
/// waits for.
const PATIENCE: Duration = Duration::from_secs(30);

/// A directory of the test's own.
fn dir(test: &str) -> PathBuf {
    let dir = PathBuf::from(env!("CARGO_TARGET_TMPDIR")).join(format!("identity-{test}"));
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

/// A throwaway self-signed certificate and key for a child's HTTP server.
fn tls(dir: &Path) -> (PathBuf, PathBuf) {
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

fn free_tcp_port() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

/// `av-contrib ARGS --print-fingerprint`: (exit status, stdout, stderr).
fn print_fingerprint(args: &[String]) -> (std::process::ExitStatus, String, String) {
    let output = Command::new(env!("CARGO_BIN_EXE_av-contrib"))
        .args(args)
        .arg("--print-fingerprint")
        .env_remove("RUST_LOG")
        .output()
        .unwrap();
    (
        output.status,
        String::from_utf8(output.stdout).unwrap(),
        String::from_utf8(output.stderr).unwrap(),
    )
}

/// A contributor's arguments as the deploy renders them: every address
/// one that this test holds, and certificate files that do not exist.
fn rendered(held: &[&UdpSocket], http: &TcpListener, node: &str, missing: &Path) -> Vec<String> {
    let addr = |socket: &UdpSocket| socket.local_addr().unwrap().to_string();
    [
        "--cert",
        &missing.join("fullchain.pem").display().to_string(),
        "--key",
        &missing.join("privkey.pem").display().to_string(),
        "--http-port",
        &http.local_addr().unwrap().port().to_string(),
        "--daw-media-bind",
        &addr(held[0]),
        "--daw-hls-packaging",
        "opaque",
        "--daw-hls-formats",
        "flac,opus,pcm",
        "--audio-part",
        "epoch",
        "--relay-local-id",
        node,
        "--node-id",
        node,
        "--relay-primary-bind",
        &addr(held[1]),
        "--relay-primary-target",
        &addr(held[2]),
        "--relay-exclusive",
        "--relay-topology-generation",
        "1",
        "--relay-subscription-id",
        "1",
        "--relay-deadline-ms",
        "1000",
        "--wall-clock-estimated-error-ms",
        "5",
    ]
    .map(str::to_owned)
    .to_vec()
}

#[test]
fn print_fingerprint_binds_nothing() {
    // Every address it would bind is taken, and the certificate files are
    // not there: a run that bound or read anything would fail.
    let held = [(); 3].map(|()| UdpSocket::bind("127.0.0.1:0").unwrap());
    let http = TcpListener::bind("0.0.0.0:0").unwrap();
    let missing = dir("print").join("no-such-directory");
    let started = Instant::now();
    let (status, stdout, stderr) = print_fingerprint(&rendered(
        &held.each_ref(),
        &http,
        "contrib-lon",
        &missing,
    ));
    assert!(status.success(), "{status:?}: {stderr}");
    assert!(stderr.is_empty(), "nothing on stderr: {stderr}");
    assert!(
        started.elapsed() < Duration::from_secs(10),
        "{:?}",
        started.elapsed()
    );
    // stdout is the identity and nothing else.
    assert_eq!(stdout.lines().count(), 1, "{stdout}");
    let identity: serde_json::Value = serde_json::from_str(&stdout).unwrap();
    let hex16 = |field: &str| {
        let value = identity[field].as_str().unwrap_or_else(|| panic!("{field}"));
        assert_eq!(value.len(), 16, "{field}");
        u64::from_str_radix(value, 16).unwrap()
    };
    let fingerprint = hex16("fingerprint");
    for field in ["numbering", "envelope", "wire"] {
        hex16(field);
    }
    assert_eq!(
        identity["object_version"].as_u64(),
        Some((2 << 16) | (fingerprint & 0xffff))
    );
    assert_eq!(identity["fields"]["generation"], 1);
    assert_eq!(identity["fields"]["clock_error_ns"], 5_000_000);
    // Nothing reached the addresses it was given.
    for socket in &held {
        socket.set_nonblocking(true).unwrap();
        assert!(socket.recv(&mut [0; 64]).is_err());
    }
    assert!(!missing.exists());

    // The other contributor of the pair, with its own id and addresses,
    // has the same identity: the deploy compares the two.
    let others = [(); 3].map(|()| UdpSocket::bind("127.0.0.1:0").unwrap());
    let (status, other, _) = print_fingerprint(&rendered(
        &others.each_ref(),
        &http,
        "contrib-lon-b",
        &missing,
    ));
    assert!(status.success());
    assert_eq!(other, stdout);
    // A setting of one of them alone is another identity.
    let mut changed = rendered(&others.each_ref(), &http, "contrib-lon-b", &missing);
    let at = changed
        .iter()
        .position(|arg| arg == "--wall-clock-estimated-error-ms")
        .unwrap();
    changed[at + 1] = "6".to_owned();
    let (_, changed, _) = print_fingerprint(&changed);
    let changed: serde_json::Value = serde_json::from_str(&changed).unwrap();
    assert_ne!(changed["fingerprint"], identity["fingerprint"]);
    assert_eq!(changed["numbering"], identity["numbering"]);
}

/// AEP1 datagrams from the probe (1 ms PCM, stereo, 2 s), as they reached
/// one socket, written to a file of length-prefixed datagrams and read
/// back.
fn datagram_file(dir: &Path) -> Vec<Vec<u8>> {
    let capture = UdpSocket::bind("127.0.0.1:0").unwrap();
    capture
        .set_read_timeout(Some(Duration::from_millis(500)))
        .unwrap();
    let target = capture.local_addr().unwrap().to_string();
    let mut probe = Command::new(env!("CARGO_BIN_EXE_aep1-48k-probe"))
        .args(["send", "--target", &target, "--duration-seconds", "2"])
        .args(["--payload", "pcm", "--epoch-samples", "48", "--channels", "2"])
        .args(["--group-channels", "2", "--realtime-priority", "0"])
        .args(["--mlock", "false", "--preroll-epochs", "0"])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();
    let mut datagrams = Vec::new();
    let mut buffer = vec![0_u8; 65_536];
    loop {
        match capture.recv(&mut buffer) {
            Ok(len) => datagrams.push(buffer[..len].to_vec()),
            Err(_) if probe.try_wait().unwrap().is_some() => break,
            Err(_) => {}
        }
    }
    assert!(probe.wait().unwrap().success());
    assert!(datagrams.len() >= 1_900, "{} datagrams", datagrams.len());
    let path = dir.join("aep1.datagrams");
    let mut file = std::fs::File::create(&path).unwrap();
    for datagram in &datagrams {
        file.write_all(&(datagram.len() as u32).to_le_bytes())
            .unwrap();
        file.write_all(datagram).unwrap();
    }
    drop(file);
    let mut bytes = Vec::new();
    std::fs::File::open(&path)
        .unwrap()
        .read_to_end(&mut bytes)
        .unwrap();
    let mut read = Vec::new();
    let mut rest = bytes.as_slice();
    while let Some((len, tail)) = rest.split_first_chunk::<4>() {
        let len = u32::from_le_bytes(*len) as usize;
        read.push(tail[..len].to_vec());
        rest = &tail[len..];
    }
    assert_eq!(read, datagrams);
    read
}

/// An av-contrib that takes AEP1 on `daw` and publishes its parts on one
/// lane to `lane`.
struct Contributor {
    child: Child,
    http_port: u16,
    daw: SocketAddr,
}

impl Contributor {
    fn start(dir: &Path, node: &str, lane: &UdpSocket) -> Self {
        let (cert, key) = tls(dir);
        let http_port = free_tcp_port();
        let daw = UdpSocket::bind("127.0.0.1:0")
            .unwrap()
            .local_addr()
            .unwrap();
        let mut child = Command::new(env!("CARGO_BIN_EXE_av-contrib"))
            .arg("--http-port")
            .arg(http_port.to_string())
            .arg("--cert")
            .arg(&cert)
            .arg("--key")
            .arg(&key)
            .args(["--daw-media-bind", &daw.to_string()])
            .args(["--daw-hls-packaging", "opaque", "--daw-hls-formats", "pcm"])
            .args(["--relay-primary-target", &lane.local_addr().unwrap().to_string()])
            .args(["--relay-exclusive", "--node-id", node])
            .args(["--stall-tick-us", "0", "--mlock", "off"])
            .args(["--hop-journal-capacity", "4096"])
            .env("RUST_LOG", "av_contrib=warn")
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .spawn()
            .unwrap();
        let stdout = child.stdout.take().unwrap();
        let (sender, lines) = std::sync::mpsc::channel();
        std::thread::spawn(move || {
            for line in BufReader::new(stdout).lines() {
                let Ok(line) = line else { break };
                if sender.send(line).is_err() {
                    break;
                }
            }
        });
        let deadline = Instant::now() + PATIENCE;
        loop {
            match lines.recv_timeout(deadline.saturating_duration_since(Instant::now())) {
                Ok(line) if line.contains("health:") => break,
                Ok(_) => {}
                Err(_) => {
                    let _ = child.kill();
                    panic!("{node} did not start");
                }
            }
        }
        Self {
            child,
            http_port,
            daw,
        }
    }

    async fn node(&self, client: &reqwest::Client) -> serde_json::Value {
        client
            .get(format!("https://127.0.0.1:{}/api/node", self.http_port))
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap()
    }
}

impl Drop for Contributor {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

/// Every closed window of every stream of a node: (stream, first part) to
/// (present, xor).
fn windows(node: &serde_json::Value) -> Vec<((u64, u64), (u64, String))> {
    node["streams"]
        .as_array()
        .unwrap()
        .iter()
        .flat_map(|stream| {
            let id = stream["stream_id"].as_u64().unwrap();
            stream["windows"]
                .as_array()
                .unwrap()
                .iter()
                .map(move |window| {
                    (
                        (id, window["first_part"].as_u64().unwrap()),
                        (
                            window["present"].as_u64().unwrap(),
                            window["xor"].as_str().unwrap().to_owned(),
                        ),
                    )
                })
        })
        .collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn two_processes_report_equal_windows() {
    let dir = dir("windows");
    let datagrams = datagram_file(&dir);
    let lanes = [(); 2].map(|()| UdpSocket::bind("127.0.0.1:0").unwrap());
    let contributors = [
        Contributor::start(&dir, "contrib-a", &lanes[0]),
        Contributor::start(&dir, "contrib-b", &lanes[1]),
    ];
    let client = reqwest::Client::builder()
        .danger_accept_invalid_certs(true)
        .build()
        .unwrap();
    let identities = [
        contributors[0].node(&client).await,
        contributors[1].node(&client).await,
    ]
    .map(|node| node["identity"].clone());
    assert_eq!(identities[0], identities[1]);
    assert!(identities[0]["fingerprint"].is_string());

    // The file, each datagram to one and then to the other, a few at a
    // time so that no socket buffer overflows.
    let sender = UdpSocket::bind("127.0.0.1:0").unwrap();
    for chunk in datagrams.chunks(16) {
        for datagram in chunk {
            for contributor in &contributors {
                sender.send_to(datagram, contributor.daw).unwrap();
            }
        }
        std::thread::sleep(Duration::from_millis(2));
    }

    // 2 s of 1 ms parts: windows 0 to 5 close (a window closes when a part
    // two windows later is published).
    let deadline = Instant::now() + PATIENCE;
    let (a, b) = loop {
        let (a, b) = (
            windows(&contributors[0].node(&client).await),
            windows(&contributors[1].node(&client).await),
        );
        if a.len() >= 6 && b.len() >= 6 {
            break (a, b);
        }
        assert!(Instant::now() < deadline, "{a:?} {b:?}");
        tokio::time::sleep(Duration::from_millis(100)).await;
    };
    let mut compared = 0;
    for (key, (present, xor)) in &a {
        let Some((_, (other_present, other_xor))) = b.iter().find(|(other, _)| other == key)
        else {
            continue;
        };
        if present == other_present {
            assert_eq!(xor, other_xor, "window {key:?}");
            compared += 1;
        }
    }
    assert!(compared >= 6, "{compared} windows compared: {a:?} {b:?}");
    assert!(
        a.iter().filter(|(_, (present, _))| *present == 256).count() >= 5,
        "whole windows: {a:?}"
    );
}
