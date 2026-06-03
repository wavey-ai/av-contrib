use anyhow::{anyhow, bail, Context, Result};
use clap::Parser;
use std::ffi::OsStr;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::process::{ExitStatus, Stdio};
use std::time::{Duration, Instant};
use tokio::io::{AsyncBufReadExt, AsyncRead, BufReader};
use tokio::process::{Child, Command};
use tokio::task::JoinHandle;
use tokio::time::{sleep, timeout};

const DEFAULT_HOST: &str = "local.bitneedle.com";

#[derive(Debug, Parser)]
#[command(
    name = "local-obs-stack",
    about = "Run two local av-mesh nodes plus one av-contrib OBS ingress"
)]
struct Args {
    #[arg(long)]
    contrib_root: Option<PathBuf>,

    #[arg(long)]
    mesh_root: Option<PathBuf>,

    #[arg(long, default_value = DEFAULT_HOST)]
    host: String,

    #[arg(long)]
    cert: Option<PathBuf>,

    #[arg(long)]
    key: Option<PathBuf>,

    #[arg(long)]
    no_build: bool,

    #[arg(long, default_value_t = 1)]
    stream_id: u64,

    #[arg(long, env = "AV_LL_HLS_PART_MS", default_value_t = 50)]
    part_ms: u64,

    #[arg(long, default_value_t = 19444)]
    uk_http_port: u16,

    #[arg(long, default_value_t = 19445)]
    us_http_port: u16,

    #[arg(long, default_value_t = 19443)]
    contrib_http_port: u16,

    #[arg(long, default_value = "127.0.0.1:29101")]
    uk_mesh: SocketAddr,

    #[arg(long, default_value = "127.0.0.1:29201")]
    us_mesh: SocketAddr,

    #[arg(long, default_value = "127.0.0.1:22001")]
    uk_fec: SocketAddr,

    #[arg(long, default_value = "127.0.0.1:22002")]
    us_fec: SocketAddr,

    #[arg(long, default_value = "127.0.0.1:22101")]
    uk_media_fec: SocketAddr,

    #[arg(long, default_value = "127.0.0.1:22102")]
    us_media_fec: SocketAddr,

    #[arg(long, default_value = "127.0.0.1:27300")]
    uk_telemetry: SocketAddr,

    #[arg(long, default_value = "127.0.0.1:27301")]
    us_telemetry: SocketAddr,

    #[arg(long, default_value = "127.0.0.1:27000")]
    rist_bind: SocketAddr,

    #[arg(long, default_value = "0x11223344")]
    rist_flow_id: String,

    #[arg(long, default_value = "127.0.0.1:27001")]
    srt_bind: SocketAddr,

    #[arg(long, default_value = "127.0.0.1:19350")]
    rtmp_bind: SocketAddr,

    #[arg(long, default_value = "obs-local")]
    rtmp_stream_key: String,

    #[arg(long, default_value_t = 25)]
    health_timeout_seconds: u64,

    #[arg(long, hide = true)]
    exit_after_ready: bool,
}

struct Service {
    name: String,
    child: Child,
    stdout_task: Option<JoinHandle<Result<()>>>,
    stderr_task: Option<JoinHandle<Result<()>>>,
}

struct TlsMaterial {
    cert: PathBuf,
    key: PathBuf,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    let default_contrib_root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let contrib_root = resolve_contrib_root(&args, &default_contrib_root)?;
    let mesh_root = resolve_mesh_root(&args, &contrib_root)?;
    let tls = resolve_tls_material(&args, &contrib_root)?;
    let rust_log = std::env::var("RUST_LOG").unwrap_or_else(|_| {
        "av_mesh=debug,av_contrib=debug,hls=debug,rtmp_ingress=debug,upload_response=debug,playlists=debug,web_service=debug,rist_mio=info,rist_core=info".into()
    });
    println!(
        "[orchestrator] config host={} stream_id={} part_ms={} rust_log={}",
        args.host, args.stream_id, args.part_ms, rust_log
    );
    if let Ok(value) = std::env::var("AV_LL_HLS_PART_MS") {
        println!("[orchestrator] AV_LL_HLS_PART_MS={value}");
    }

    if !args.no_build {
        run_build(
            &mesh_root,
            ["build", "--locked", "--release", "--bin", "av-mesh"],
            "av-mesh build",
        )
        .await?;
        run_build(
            &contrib_root,
            ["build", "--locked", "--release", "--bin", "av-contrib"],
            "av-contrib build",
        )
        .await?;
    }

    let mesh_bin = target_release_bin(&mesh_root, "av-mesh");
    let contrib_bin = target_release_bin(&contrib_root, "av-contrib");
    ensure_executable(&mesh_bin, "av-mesh")?;
    ensure_executable(&contrib_bin, "av-contrib")?;

    let mut services = Vec::new();
    let result = async {
        services.push(
            spawn_service(
                "mesh-uk",
                &mesh_bin,
                &mesh_root,
                mesh_node_args(
                    "uk",
                    "uk-local",
                    args.uk_mesh,
                    args.us_mesh,
                    args.uk_http_port,
                    args.uk_fec,
                    args.uk_media_fec,
                    args.uk_telemetry,
                    args.us_telemetry,
                    args.stream_id,
                    args.part_ms,
                    &args.host,
                    &tls.cert,
                    &tls.key,
                ),
                &rust_log,
            )
            .await?,
        );
        services.push(
            spawn_service(
                "mesh-us",
                &mesh_bin,
                &mesh_root,
                mesh_node_args(
                    "us",
                    "us-local",
                    args.us_mesh,
                    args.uk_mesh,
                    args.us_http_port,
                    args.us_fec,
                    args.us_media_fec,
                    args.us_telemetry,
                    args.uk_telemetry,
                    args.stream_id,
                    args.part_ms,
                    &args.host,
                    &tls.cert,
                    &tls.key,
                ),
                &rust_log,
            )
            .await?,
        );

        wait_for_health(
            "mesh-uk",
            args.uk_http_port,
            Duration::from_secs(args.health_timeout_seconds),
            &args.host,
            &mut services,
        )
        .await?;
        wait_for_health(
            "mesh-us",
            args.us_http_port,
            Duration::from_secs(args.health_timeout_seconds),
            &args.host,
            &mut services,
        )
        .await?;

        services.push(
            spawn_service(
                "contrib",
                &contrib_bin,
                &contrib_root,
                contrib_args(&args, &tls.cert, &tls.key),
                &rust_log,
            )
            .await?,
        );
        wait_for_health(
            "contrib",
            args.contrib_http_port,
            Duration::from_secs(args.health_timeout_seconds),
            &args.host,
            &mut services,
        )
        .await?;

        print_ready(&args);

        if args.exit_after_ready {
            shutdown_services(&mut services).await;
            return Ok(());
        }

        supervise_until_exit_or_ctrl_c(&mut services).await
    }
    .await;

    if result.is_err() {
        shutdown_services(&mut services).await;
    }
    result
}

fn resolve_contrib_root(args: &Args, default_contrib_root: &Path) -> Result<PathBuf> {
    let root = args
        .contrib_root
        .clone()
        .or_else(|| std::env::var_os("AV_CONTRIB_ROOT").map(PathBuf::from))
        .unwrap_or_else(|| default_contrib_root.to_path_buf());
    let manifest = root.join("Cargo.toml");
    if !manifest.exists() {
        bail!(
            "could not find av-contrib Cargo.toml at {}; pass --contrib-root or set AV_CONTRIB_ROOT",
            manifest.display()
        );
    }
    Ok(root)
}

fn resolve_mesh_root(args: &Args, contrib_root: &Path) -> Result<PathBuf> {
    let root = args
        .mesh_root
        .clone()
        .or_else(|| std::env::var_os("AV_MESH_ROOT").map(PathBuf::from))
        .unwrap_or_else(|| contrib_root.join("..").join("av-mesh"));
    let manifest = root.join("Cargo.toml");
    if !manifest.exists() {
        bail!(
            "could not find av-mesh Cargo.toml at {}; pass --mesh-root or set AV_MESH_ROOT",
            manifest.display()
        );
    }
    Ok(root)
}

async fn run_build<I, S>(cwd: &Path, args: I, name: &str) -> Result<()>
where
    I: IntoIterator<Item = S>,
    S: AsRef<OsStr>,
{
    println!("[orchestrator] running {name} in {}", cwd.display());
    let status = Command::new("cargo")
        .args(args)
        .current_dir(cwd)
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit())
        .status()
        .await
        .with_context(|| format!("failed to start {name}"))?;
    if !status.success() {
        bail!("{name} failed with {status}");
    }
    Ok(())
}

fn target_release_bin(root: &Path, name: &str) -> PathBuf {
    let target_dir = std::env::var_os("CARGO_TARGET_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|| root.join("target"));
    target_dir
        .join("release")
        .join(format!("{}{}", name, std::env::consts::EXE_SUFFIX))
}

fn ensure_executable(path: &Path, name: &str) -> Result<()> {
    if path.exists() {
        Ok(())
    } else {
        bail!(
            "{name} release binary not found at {}; run without --no-build first",
            path.display()
        )
    }
}

fn resolve_tls_material(args: &Args, contrib_root: &Path) -> Result<TlsMaterial> {
    let default_tls_dir = contrib_root.join("..").join("tls").join(DEFAULT_HOST);
    let cert = args
        .cert
        .clone()
        .unwrap_or_else(|| default_tls_dir.join("fullchain.pem"));
    let key = args
        .key
        .clone()
        .unwrap_or_else(|| default_tls_dir.join("privkey.pem"));

    if !cert.exists() {
        bail!(
            "TLS certificate not found at {}; pass --cert or restore ../tls/{}/fullchain.pem",
            cert.display(),
            DEFAULT_HOST
        );
    }
    if !key.exists() {
        bail!(
            "TLS key not found at {}; pass --key or restore ../tls/{}/privkey.pem",
            key.display(),
            DEFAULT_HOST
        );
    }

    Ok(TlsMaterial { cert, key })
}

fn mesh_node_args(
    region: &str,
    node_id: &str,
    mesh_bind: SocketAddr,
    peer: SocketAddr,
    http_port: u16,
    fec_bind: SocketAddr,
    media_fec_bind: SocketAddr,
    telemetry_bind: SocketAddr,
    telemetry_peer: SocketAddr,
    stream_id: u64,
    part_ms: u64,
    host: &str,
    cert: &Path,
    key: &Path,
) -> Vec<String> {
    vec![
        "--cert".into(),
        cert.display().to_string(),
        "--key".into(),
        key.display().to_string(),
        "--region".into(),
        region.into(),
        "--node-id".into(),
        node_id.into(),
        "--mesh-bind".into(),
        mesh_bind.to_string(),
        "--peer".into(),
        peer.to_string(),
        "--http-port".into(),
        http_port.to_string(),
        "--playback-base-url".into(),
        format!("https://{host}:{http_port}/live"),
        "--fec-bind".into(),
        fec_bind.to_string(),
        "--media-fec-bind".into(),
        media_fec_bind.to_string(),
        "--telemetry-bind".into(),
        telemetry_bind.to_string(),
        "--telemetry-peer".into(),
        telemetry_peer.to_string(),
        "--telemetry-dns-name".into(),
        host.into(),
        "--telemetry-interval-ms".into(),
        "250".into(),
        "--stream-id".into(),
        stream_id.to_string(),
        "--part-ms".into(),
        part_ms.to_string(),
        "--parts-per-segment".into(),
        "2".into(),
        "--window-parts".into(),
        "24".into(),
        "--slot-kb".into(),
        "2048".into(),
    ]
}

fn contrib_args(args: &Args, cert: &Path, key: &Path) -> Vec<String> {
    vec![
        "--cert".into(),
        cert.display().to_string(),
        "--key".into(),
        key.display().to_string(),
        "--http-port".into(),
        args.contrib_http_port.to_string(),
        "--mesh-fec-target".into(),
        args.uk_fec.to_string(),
        "--mesh-media-fec-target".into(),
        args.uk_media_fec.to_string(),
        "--stream-id".into(),
        args.stream_id.to_string(),
        "--rist-stream-id".into(),
        args.stream_id.to_string(),
        "--srt-stream-id".into(),
        args.stream_id.to_string(),
        "--rtmp-stream-id".into(),
        args.stream_id.to_string(),
        "--fmp4-part-ms".into(),
        args.part_ms.to_string(),
        "--rist-bind".into(),
        args.rist_bind.to_string(),
        "--rist-flow-id".into(),
        args.rist_flow_id.clone(),
        "--srt-bind".into(),
        args.srt_bind.to_string(),
        "--rtmp-bind".into(),
        args.rtmp_bind.to_string(),
    ]
}

async fn spawn_service(
    name: &str,
    binary: &Path,
    cwd: &Path,
    args: Vec<String>,
    rust_log: &str,
) -> Result<Service> {
    println!(
        "[orchestrator] starting {name}: {} {}",
        binary.display(),
        args.join(" ")
    );
    println!("[orchestrator] {name} RUST_LOG={rust_log}");
    let mut child = Command::new(binary)
        .args(&args)
        .current_dir(cwd)
        .env("RUST_LOG", rust_log)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true)
        .spawn()
        .with_context(|| format!("failed to start {name}"))?;

    let stdout = child
        .stdout
        .take()
        .ok_or_else(|| anyhow!("failed to capture stdout for {name}"))?;
    let stderr = child
        .stderr
        .take()
        .ok_or_else(|| anyhow!("failed to capture stderr for {name}"))?;

    Ok(Service {
        name: name.to_owned(),
        child,
        stdout_task: Some(tokio::spawn(prefix_lines(name.to_owned(), stdout))),
        stderr_task: Some(tokio::spawn(prefix_lines(name.to_owned(), stderr))),
    })
}

async fn prefix_lines<R>(name: String, reader: R) -> Result<()>
where
    R: AsyncRead + Unpin,
{
    let mut lines = BufReader::new(reader).lines();
    while let Some(line) = lines.next_line().await? {
        println!("[{name}] {line}");
    }
    Ok(())
}

async fn wait_for_health(
    name: &str,
    port: u16,
    timeout_duration: Duration,
    host: &str,
    services: &mut [Service],
) -> Result<()> {
    let deadline = Instant::now() + timeout_duration;
    let url = format!("https://{host}:{port}/up");
    let mut attempt: u64 = 0;
    while Instant::now() < deadline {
        attempt = attempt.saturating_add(1);
        if let Some((service, status)) = first_exited(services)? {
            bail!("{service} exited while waiting for {name} health: {status}");
        }
        println!("[orchestrator] health check attempt {attempt} for {name}: {url}");
        if curl_ok(host, port, &url).await {
            println!("[orchestrator] {name} healthy at {url}");
            return Ok(());
        }
        sleep(Duration::from_millis(250)).await;
    }
    bail!("{name} did not become healthy at {url}");
}

async fn curl_ok(host: &str, port: u16, url: &str) -> bool {
    let resolve = format!("{host}:{port}:127.0.0.1");
    match Command::new("curl")
        .args(["-fs", "--resolve", &resolve, url])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .await
    {
        Ok(status) => status.success(),
        Err(_) => false,
    }
}

fn first_exited(services: &mut [Service]) -> Result<Option<(String, ExitStatus)>> {
    for service in services {
        if let Some(status) = service
            .child
            .try_wait()
            .with_context(|| format!("failed to poll {}", service.name))?
        {
            return Ok(Some((service.name.clone(), status)));
        }
    }
    Ok(None)
}

fn print_ready(args: &Args) {
    println!();
    println!("[orchestrator] local OBS stack ready");
    println!(
        "[orchestrator] OBS RTMP server: rtmp://{}:{}/live",
        args.host,
        args.rtmp_bind.port()
    );
    println!(
        "[orchestrator] OBS RTMP stream key: {}",
        args.rtmp_stream_key
    );
    println!(
        "[orchestrator] OBS SRT caller URL: srt://{}:{}?mode=caller",
        args.host,
        args.srt_bind.port()
    );
    println!(
        "[orchestrator] RIST URL: rist://{}:{} profile=main flow_id={}",
        args.host,
        args.rist_bind.port(),
        args.rist_flow_id
    );
    println!(
        "[orchestrator] UK player: https://{}:{}/live/{}/stream.m3u8",
        args.host, args.uk_http_port, args.stream_id
    );
    println!(
        "[orchestrator] US player: https://{}:{}/live/{}/stream.m3u8",
        args.host, args.us_http_port, args.stream_id
    );
    println!(
        "[orchestrator] LL-HLS part target: {}ms (override with AV_LL_HLS_PART_MS or --part-ms)",
        args.part_ms
    );
    println!(
        "[orchestrator] LL-HLS tail path for stream {}: /live/{}/tail?mode=part",
        args.stream_id, args.stream_id
    );
    println!(
        "[orchestrator] UK mesh UI: https://{}:{}/mesh",
        args.host, args.uk_http_port
    );
    println!(
        "[orchestrator] US mesh UI: https://{}:{}/mesh",
        args.host, args.us_http_port
    );
    println!("[orchestrator] logs from all services are prefixed below");
    println!();
}

async fn supervise_until_exit_or_ctrl_c(services: &mut Vec<Service>) -> Result<()> {
    loop {
        tokio::select! {
            signal = tokio::signal::ctrl_c() => {
                signal.context("failed to wait for ctrl-c")?;
                println!("[orchestrator] ctrl-c received, stopping services");
                shutdown_services(services).await;
                return Ok(());
            }
            _ = sleep(Duration::from_millis(250)) => {
                if let Some((service, status)) = first_exited(services)? {
                    println!("[orchestrator] {service} exited with {status}, stopping stack");
                    shutdown_services(services).await;
                    if status.success() {
                        return Ok(());
                    }
                    bail!("{service} exited with {status}");
                }
            }
        }
    }
}

async fn shutdown_services(services: &mut [Service]) {
    for service in services.iter_mut() {
        match service.child.try_wait() {
            Ok(Some(_)) => {}
            Ok(None) => {
                println!("[orchestrator] stopping {}", service.name);
                let _ = service.child.start_kill();
            }
            Err(_) => {}
        }
    }

    for service in services.iter_mut() {
        match timeout(Duration::from_secs(5), service.child.wait()).await {
            Ok(Ok(status)) => println!("[orchestrator] {} stopped with {status}", service.name),
            Ok(Err(error)) => println!(
                "[orchestrator] failed waiting for {}: {error}",
                service.name
            ),
            Err(_) => println!("[orchestrator] timed out waiting for {}", service.name),
        }
    }

    for service in services.iter_mut() {
        if let Some(task) = service.stdout_task.take() {
            let _ = task.await;
        }
        if let Some(task) = service.stderr_task.take() {
            let _ = task.await;
        }
    }
}
