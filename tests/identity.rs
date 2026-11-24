//! The contributor's identity (A 2.4, 2.6, AC6), on child processes:
//! `--print-fingerprint` binds nothing, reads no file and prints only the
//! identity.

use std::net::{TcpListener, UdpSocket};
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::{Duration, Instant};

/// A directory of the test's own.
fn dir(test: &str) -> PathBuf {
    let dir = PathBuf::from(env!("CARGO_TARGET_TMPDIR")).join(format!("identity-{test}"));
    std::fs::create_dir_all(&dir).unwrap();
    dir
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
