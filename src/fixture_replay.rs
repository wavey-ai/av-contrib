//! Immutable SoundKit v2 fixture capture and freshly timed AEP1/RaptorQ replay.

use crate::soundkit_v2::SoundKitV2StreamRef;
use anyhow::{bail, Context, Result};
use frame_header::EncodingFlag;
use media_object::{ClockConfidence, ClockTimestamp, MediaObjectBuilder, ObjectKey, ObjectKind};
use music_audio_session::{MultichannelAudioReceiver, MultichannelAudioSessionConfig};
use raptorq_datagram_fec::AudioPayloadKind;
use raptorq_fec_transport::MultichannelAudioTransportAdapter;
use relay_session::{
    encoded_datagram_len, AdaptiveFecController, AdaptiveFecPolicy, CongestionConfig,
    MediaDeadline, PrivateUdpConfig, PrivateUdpTransport, RaptorQObjectEncoder, RelayLimits,
    RelayTransport, SubscriptionId, TopologyGeneration,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::net::UdpSocket;
use tokio::time::{sleep_until, timeout, Instant as TokioInstant};

const MAGIC: &[u8; 8] = b"NTV2FIX1";
const SCHEMA: &str = "needletail.soundkit-v2-fixture.v1";
const CAPTURE_REPORT_SCHEMA: &str = "needletail.soundkit-v2-fixture.capture.v1";
const REPLAY_REPORT_SCHEMA: &str = "needletail.soundkit-v2-fixture.replay.v1";
const FRAME_DURATION: Duration = Duration::from_millis(5);
const RELAY_PART_EPOCHS: u64 = 10;
const MAX_MANIFEST_BYTES: usize = 64 * 1024;
const MAX_RECORD_BYTES: usize = 16 * 1024 * 1024;

#[derive(Debug, Clone)]
pub struct CaptureFixtureOptions {
    pub relay: SocketAddr,
    pub bind: Option<SocketAddr>,
    pub session_id: u64,
    pub group_id: u16,
    pub duration_seconds: u64,
    pub tail_seconds: u64,
    pub producer_sha256: String,
    pub output: PathBuf,
}

#[derive(Debug, Clone)]
pub struct ReplayFixtureOptions {
    pub fixture: PathBuf,
    pub bind: SocketAddr,
    pub target: SocketAddr,
    pub secondary_bind: SocketAddr,
    pub secondary_target: SocketAddr,
    pub local_node_id: String,
    pub primary_node_id: String,
    pub secondary_node_id: String,
    pub topology_generation: u64,
    pub subscription_id: u64,
    pub duration_seconds: u64,
    pub tracks: u16,
    pub group_id: u16,
    pub session_id: Option<u64>,
    pub repair_percent: u32,
    pub min_repair_symbols: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct FixtureManifest {
    schema: String,
    producer_sha256: String,
    captured_session_id: u64,
    source_group_id: u16,
    sample_rate: u32,
    frame_count: u32,
    channel_count: u16,
    epoch_count: u64,
    record_count: u64,
    duration_ms: u64,
    codecs: Vec<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum FixtureCodec {
    Flac,
    Opus,
}

impl FixtureCodec {
    fn wire(self) -> u8 {
        match self {
            Self::Flac => 1,
            Self::Opus => 2,
        }
    }

    fn from_wire(value: u8) -> Result<Self> {
        match value {
            1 => Ok(Self::Flac),
            2 => Ok(Self::Opus),
            _ => bail!("unsupported fixture codec tag {value}"),
        }
    }
}

#[derive(Debug, Clone)]
struct FixtureRecord {
    relative_pts_samples: u64,
    codec: FixtureCodec,
    channel_count: u16,
    payload: Vec<u8>,
}

#[derive(Debug, Clone)]
struct Fixture {
    manifest: FixtureManifest,
    records: Vec<FixtureRecord>,
    sha256: String,
}

#[derive(Debug, Serialize)]
pub struct CaptureFixtureReport {
    schema: &'static str,
    fixture_schema: &'static str,
    output: String,
    fixture_sha256: String,
    producer_sha256: String,
    captured_session_id: u64,
    source_group_id: u16,
    duration_ms: u64,
    epochs: u64,
    records: u64,
    codecs: [&'static str; 2],
    sample_rate: u32,
    frame_count: u32,
    channel_count: u16,
    datagrams_received: u64,
    raptorq_recovered_fragments: u64,
}

#[derive(Debug, Serialize)]
pub struct ReplayFixtureReport {
    schema: &'static str,
    fixture_sha256: String,
    session_id: u64,
    target: String,
    secondary_target: String,
    tracks: u16,
    codec_representations: [&'static str; 2],
    duration_seconds: u64,
    scheduled_epochs: u64,
    encoded_representation_epochs: u64,
    source_datagrams: u64,
    repair_datagrams: u64,
    wire_bytes: u64,
    pacing_error_p50_us: u64,
    pacing_error_p95_us: u64,
    pacing_error_p99_us: u64,
    pacing_error_max_us: u64,
    udp_send_errors: u64,
    process_cpu_seconds: f64,
    wall_seconds: f64,
}

pub async fn capture_fixture(options: CaptureFixtureOptions) -> Result<CaptureFixtureReport> {
    if options.duration_seconds == 0 {
        bail!("--duration-seconds must be positive");
    }
    validate_sha256("--producer-sha256", &options.producer_sha256)?;
    let bind = options.bind.unwrap_or_else(|| {
        if options.relay.is_ipv4() {
            "0.0.0.0:0".parse().expect("valid IPv4 wildcard")
        } else {
            "[::]:0".parse().expect("valid IPv6 wildcard")
        }
    });
    let socket = UdpSocket::bind(bind).await?;
    let subscribe = format!("WAVEY-DAW-SUBSCRIBE/2 {}", options.session_id);
    socket.send_to(subscribe.as_bytes(), options.relay).await?;
    let expected_ack = format!("WAVEY-DAW-SUBSCRIBED/2 {}", options.session_id);
    let mut datagram = vec![0_u8; 65_536];
    let (ack_len, peer) = timeout(Duration::from_secs(3), socket.recv_from(&mut datagram))
        .await
        .context("timed out waiting for fixture relay subscription")??;
    if peer != options.relay || &datagram[..ack_len] != expected_ack.as_bytes() {
        bail!("unexpected fixture relay subscription acknowledgement");
    }

    let transport = MultichannelAudioTransportAdapter::udp(65_535);
    let mut receiver = MultichannelAudioReceiver::new(MultichannelAudioSessionConfig::default());
    let wanted_epochs = options.duration_seconds.saturating_mul(200);
    let stop_ns = options.session_id.saturating_add(
        options
            .duration_seconds
            .saturating_add(options.tail_seconds)
            * 1_000_000_000,
    );
    let mut records = BTreeMap::<(u64, FixtureCodec), FixtureRecord>::new();
    let mut datagrams_received = 0_u64;
    let mut recovered_fragments = 0_u64;
    let mut next_refresh = TokioInstant::now() + Duration::from_secs(5);

    while now_unix_ns()? < stop_ns {
        let remaining = Duration::from_nanos(stop_ns.saturating_sub(now_unix_ns()?));
        match timeout(
            remaining.min(Duration::from_millis(250)),
            socket.recv_from(&mut datagram),
        )
        .await
        {
            Ok(Ok((len, _))) => {
                let Ok(payload) = transport.payload(&datagram[..len]) else {
                    continue;
                };
                datagrams_received = datagrams_received.saturating_add(1);
                let outcome = receiver.push_datagram(payload)?;
                for group in outcome.completed_groups {
                    if group.session_id != options.session_id || group.group_id != options.group_id
                    {
                        continue;
                    }
                    if group.payload_kind != AudioPayloadKind::SoundKitV2 {
                        bail!("fixture capture observed non-SoundKit-v2 payload");
                    }
                    let codec = soundkit_codec(&group.payload)?;
                    recovered_fragments = recovered_fragments
                        .saturating_add(u64::from(group.raptorq_recovered_fragments));
                    records
                        .entry((group.pts_samples, codec))
                        .or_insert(FixtureRecord {
                            relative_pts_samples: group.pts_samples,
                            codec,
                            channel_count: group.channel_count,
                            payload: group.payload.to_vec(),
                        });
                }
                if select_window(&records, wanted_epochs).is_some() {
                    break;
                }
            }
            Ok(Err(error)) => return Err(error.into()),
            Err(_) => {}
        }
        if TokioInstant::now() >= next_refresh {
            socket.send_to(subscribe.as_bytes(), options.relay).await?;
            next_refresh += Duration::from_secs(5);
        }
    }

    let (sample_rate, frame_count, channel_count, selected) =
        select_window(&records, wanted_epochs).context(
            "capture did not contain a complete contiguous FLAC+Opus SoundKit v2 window",
        )?;
    let manifest = FixtureManifest {
        schema: SCHEMA.to_string(),
        producer_sha256: options.producer_sha256.clone(),
        captured_session_id: options.session_id,
        source_group_id: options.group_id,
        sample_rate,
        frame_count,
        channel_count,
        epoch_count: wanted_epochs,
        record_count: wanted_epochs.saturating_mul(2),
        duration_ms: options.duration_seconds.saturating_mul(1_000),
        codecs: vec!["flac".to_string(), "opus".to_string()],
    };
    let fixture_sha256 = write_fixture(&options.output, &manifest, &selected)?;
    Ok(CaptureFixtureReport {
        schema: CAPTURE_REPORT_SCHEMA,
        fixture_schema: SCHEMA,
        output: options.output.display().to_string(),
        fixture_sha256,
        producer_sha256: options.producer_sha256,
        captured_session_id: options.session_id,
        source_group_id: options.group_id,
        duration_ms: manifest.duration_ms,
        epochs: manifest.epoch_count,
        records: manifest.record_count,
        codecs: ["flac", "opus"],
        sample_rate,
        frame_count,
        channel_count,
        datagrams_received,
        raptorq_recovered_fragments: recovered_fragments,
    })
}

pub async fn replay_fixture(options: ReplayFixtureOptions) -> Result<ReplayFixtureReport> {
    if options.duration_seconds == 0 || options.tracks == 0 {
        bail!("--duration-seconds and --tracks must be positive");
    }
    let fixture = read_fixture(&options.fixture)?;
    options
        .group_id
        .checked_add(options.tracks - 1)
        .context("fixture replay group id range overflows u16")?;
    let total_channels = u32::from(fixture.manifest.channel_count)
        .checked_mul(u32::from(options.tracks))
        .context("fixture replay channel count overflow")?;
    if total_channels > u32::from(u16::MAX) {
        bail!("fixture replay channel count exceeds AEP1 range");
    }
    let session_id = options
        .session_id
        .unwrap_or(now_unix_ns()?.saturating_add(1_000_000_000));
    let now_ns = now_unix_ns()?;
    if session_id.saturating_add(60_000_000_000) < now_ns {
        bail!("--session-id is too far in the past to be an origin clock");
    }

    let by_pts = fixture_records_by_pts(&fixture)?;
    let limits = RelayLimits::default();
    let primary = PrivateUdpTransport::bind(PrivateUdpConfig::new(
        options.bind,
        options.target,
        options.local_node_id.clone(),
        options.primary_node_id.clone(),
        limits,
    )?)
    .await?;
    let secondary = PrivateUdpTransport::bind(PrivateUdpConfig::new(
        options.secondary_bind,
        options.secondary_target,
        options.local_node_id.clone(),
        options.secondary_node_id.clone(),
        limits,
    )?)
    .await?;
    let mut policy = AdaptiveFecPolicy::default();
    let requested_repairs = options
        .min_repair_symbols
        .max(options.repair_percent.min(100).div_ceil(4));
    policy.min_repair_symbols = requested_repairs.min(policy.max_repair_symbols);
    let mut encoder = RaptorQObjectEncoder::new(
        AdaptiveFecController::new(policy, CongestionConfig::default()),
        limits,
    )?;
    let generation = TopologyGeneration::new(options.topology_generation)?;
    let subscription_id = SubscriptionId::new(options.subscription_id)?;
    let cpu_start = process_cpu_seconds();
    let wall_start = Instant::now();
    let start =
        TokioInstant::now() + Duration::from_nanos(session_id.saturating_sub(now_unix_ns()?));
    let scheduled_epochs = options.duration_seconds.saturating_mul(200);
    let fixture_pts = by_pts.keys().copied().collect::<Vec<_>>();
    let scheduled_parts = scheduled_epochs.div_ceil(RELAY_PART_EPOCHS);
    let source_epoch = session_id / 1_000;
    let mut source_datagrams = 0_u64;
    let mut repair_datagrams = 0_u64;
    let mut wire_bytes = 0_u64;
    let mut pacing_errors = Vec::new();

    for part_id in 0..scheduled_parts {
        let first_epoch = part_id.saturating_mul(RELAY_PART_EPOCHS);
        let epoch_count = RELAY_PART_EPOCHS.min(scheduled_epochs.saturating_sub(first_epoch));
        let part_at = start + FRAME_DURATION.saturating_mul(first_epoch as u32);
        let capture_ns = session_id.saturating_add(
            FRAME_DURATION.as_nanos().min(u128::from(u64::MAX)) as u64 * first_epoch,
        );
        let deadline_ns = capture_ns.saturating_add(1_000_000_000);
        let deadline = MediaDeadline::from_micros(deadline_ns.div_ceil(1_000));
        let mut paced = Vec::new();
        for codec in [FixtureCodec::Flac, FixtureCodec::Opus] {
            let payload = fixture_part_payload(
                &by_pts,
                &fixture_pts,
                first_epoch,
                epoch_count,
                codec,
                options.group_id,
                options.tracks,
            )?;
            let stream_id = match codec {
                FixtureCodec::Flac => 2_001_u64,
                FixtureCodec::Opus => 3_001_u64,
            };
            let key = ObjectKey::for_payload(
                "needletail",
                stream_id.to_string(),
                format!("aep1-{}-tracks-{}", options.tracks, codec_name(codec)),
                source_epoch,
                u64::from(options.group_id),
                part_id,
                1,
                &payload,
            )?;
            let capture_timestamp = ClockTimestamp::new(
                i64::try_from(capture_ns).context("fixture capture time exceeds i64")?,
                "fixture-replay-clock",
                ClockConfidence::estimated(5_000_000),
            )?;
            let deadline_timestamp = ClockTimestamp::new(
                i64::try_from(deadline_ns).context("fixture deadline exceeds i64")?,
                "fixture-replay-clock",
                ClockConfidence::estimated(5_000_000),
            )?;
            let object = MediaObjectBuilder::new(key, ObjectKind::Media, payload)
                .with_capture_timestamp(capture_timestamp)
                .with_deadline(deadline_timestamp)
                .with_metadata("scheduler-class", b"audio".to_vec())
                .with_metadata("payload-format", b"soundkit-v2-bundle".to_vec())
                .build()?;
            let encoded = encoder.encode_object_with_inferred_priority(
                &object,
                generation,
                subscription_id,
                deadline,
            )?;
            source_datagrams = source_datagrams
                .saturating_add((encoded.source_symbols.len() as u64).saturating_mul(2));
            repair_datagrams = repair_datagrams.saturating_add(encoded.repair_symbols.len() as u64);
            for datagram in encoded.source_symbols {
                paced.push((datagram.clone(), true));
                paced.push((datagram, false));
            }
            for datagram in encoded.repair_symbols {
                paced.push((datagram, false));
            }
        }
        let count = paced.len().max(1) as u32;
        for (index, (datagram, primary_lane)) in paced.into_iter().enumerate() {
            let scheduled_at = part_at
                + FRAME_DURATION
                    .saturating_mul(RELAY_PART_EPOCHS as u32)
                    .saturating_mul(index as u32)
                    / count;
            sleep_until(scheduled_at).await;
            pacing_errors.push(
                TokioInstant::now()
                    .saturating_duration_since(scheduled_at)
                    .as_micros()
                    .min(u128::from(u64::MAX)) as u64,
            );
            wire_bytes = wire_bytes.saturating_add(encoded_datagram_len(&datagram)? as u64);
            if primary_lane {
                primary.send_datagram(datagram).await?;
            } else {
                secondary.send_datagram(datagram).await?;
            }
        }
    }
    pacing_errors.sort_unstable();
    Ok(ReplayFixtureReport {
        schema: REPLAY_REPORT_SCHEMA,
        fixture_sha256: fixture.sha256,
        session_id,
        target: options.target.to_string(),
        secondary_target: options.secondary_target.to_string(),
        tracks: options.tracks,
        codec_representations: ["flac", "opus"],
        duration_seconds: options.duration_seconds,
        scheduled_epochs,
        encoded_representation_epochs: scheduled_epochs
            .saturating_mul(2)
            .saturating_mul(u64::from(options.tracks)),
        source_datagrams,
        repair_datagrams,
        wire_bytes,
        pacing_error_p50_us: percentile(&pacing_errors, 50),
        pacing_error_p95_us: percentile(&pacing_errors, 95),
        pacing_error_p99_us: percentile(&pacing_errors, 99),
        pacing_error_max_us: pacing_errors.last().copied().unwrap_or(0),
        udp_send_errors: 0,
        process_cpu_seconds: (process_cpu_seconds() - cpu_start).max(0.0),
        wall_seconds: wall_start.elapsed().as_secs_f64(),
    })
}

fn fixture_part_payload(
    by_pts: &BTreeMap<u64, BTreeMap<FixtureCodec, &FixtureRecord>>,
    fixture_pts: &[u64],
    first_epoch: u64,
    epoch_count: u64,
    codec: FixtureCodec,
    group_id: u16,
    tracks: u16,
) -> Result<Vec<u8>> {
    let mut payload = b"NTV2PART".to_vec();
    payload.push(codec.wire());
    payload.extend_from_slice(&epoch_count.to_be_bytes());
    payload.extend_from_slice(&group_id.to_be_bytes());
    payload.extend_from_slice(&tracks.to_be_bytes());
    for offset in 0..epoch_count {
        let pts = fixture_pts[((first_epoch + offset) as usize) % fixture_pts.len()];
        let record = by_pts
            .get(&pts)
            .and_then(|records| records.get(&codec))
            .context("fixture part record disappeared")?;
        for track in 0..tracks {
            payload.extend_from_slice(&(group_id + track).to_be_bytes());
            payload.extend_from_slice(&(record.payload.len() as u32).to_be_bytes());
            payload.extend_from_slice(&record.payload);
        }
    }
    Ok(payload)
}

fn codec_name(codec: FixtureCodec) -> &'static str {
    match codec {
        FixtureCodec::Flac => "flac",
        FixtureCodec::Opus => "opus",
    }
}

fn soundkit_codec(payload: &[u8]) -> Result<FixtureCodec> {
    let stream = SoundKitV2StreamRef::parse(payload, true).map_err(anyhow::Error::msg)?;
    let first = stream
        .packets
        .first()
        .context("SoundKit v2 stream is empty")?;
    let codec = match first.header.encoding() {
        EncodingFlag::FLAC => FixtureCodec::Flac,
        EncodingFlag::Opus => FixtureCodec::Opus,
        other => bail!("fixture capture rejects SoundKit v2 encoding {other:?}"),
    };
    if stream
        .packets
        .iter()
        .any(|packet| packet.header.encoding() != first.header.encoding())
    {
        bail!("fixture record mixes SoundKit v2 codec identities");
    }
    Ok(codec)
}

fn soundkit_geometry(payload: &[u8]) -> Result<(u32, u32, u16)> {
    let stream = SoundKitV2StreamRef::parse(payload, true).map_err(anyhow::Error::msg)?;
    let first = stream
        .packets
        .first()
        .context("SoundKit v2 stream is empty")?;
    let sample_rate = first.header.sample_rate();
    let channels = u16::from(first.header.channels());
    if stream.packets.iter().any(|packet| {
        packet.header.sample_rate() != sample_rate
            || u16::from(packet.header.channels()) != channels
    }) {
        bail!("fixture record changes SoundKit v2 audio geometry");
    }
    let frame_count = u32::try_from(stream.total_frame_count)
        .context("fixture SoundKit v2 frame count exceeds u32")?;
    Ok((sample_rate, frame_count, channels))
}

fn select_window(
    records: &BTreeMap<(u64, FixtureCodec), FixtureRecord>,
    wanted_epochs: u64,
) -> Option<(u32, u32, u16, Vec<FixtureRecord>)> {
    let wanted = usize::try_from(wanted_epochs).ok()?;
    let codec_records = |codec| {
        records
            .iter()
            .filter_map(|((_, record_codec), record)| (*record_codec == codec).then_some(record))
            .collect::<Vec<_>>()
    };
    let flac = codec_records(FixtureCodec::Flac);
    let opus = codec_records(FixtureCodec::Opus);
    if flac.len() < wanted || opus.len() < wanted {
        return None;
    }
    let flac_geometry = soundkit_geometry(&flac[0].payload).ok()?;
    let opus_geometry = soundkit_geometry(&opus[0].payload).ok()?;
    let (sample_rate, _, channel_count) = flac_geometry;
    if opus_geometry.0 != sample_rate
        || opus_geometry.2 != channel_count
        || flac[0].channel_count != channel_count
        || opus[0].channel_count != channel_count
        || flac[..wanted].iter().any(|record| {
            record.channel_count != channel_count
                || soundkit_geometry(&record.payload).ok() != Some(flac_geometry)
        })
        || opus[..wanted].iter().any(|record| {
            record.channel_count != channel_count
                || soundkit_geometry(&record.payload).ok() != Some(opus_geometry)
        })
    {
        return None;
    }
    let frame_count = sample_rate.checked_div(200)?;
    if frame_count == 0 {
        return None;
    }
    let mut selected = Vec::with_capacity(wanted.saturating_mul(2));
    for index in 0..wanted {
        let relative_pts_samples = (index as u64).saturating_mul(u64::from(frame_count));
        for source in [flac[index], opus[index]] {
            let mut record = source.clone();
            record.relative_pts_samples = relative_pts_samples;
            selected.push(record);
        }
    }
    Some((sample_rate, frame_count, channel_count, selected))
}

fn write_fixture(
    path: &Path,
    manifest: &FixtureManifest,
    records: &[FixtureRecord],
) -> Result<String> {
    let manifest_bytes = serde_json::to_vec(manifest)?;
    if manifest_bytes.len() > MAX_MANIFEST_BYTES {
        bail!("fixture manifest exceeds {MAX_MANIFEST_BYTES} bytes");
    }
    let mut bytes = Vec::new();
    bytes.extend_from_slice(MAGIC);
    bytes.extend_from_slice(&(manifest_bytes.len() as u32).to_be_bytes());
    bytes.extend_from_slice(&manifest_bytes);
    bytes.extend_from_slice(&(records.len() as u64).to_be_bytes());
    for record in records {
        if record.payload.len() > MAX_RECORD_BYTES {
            bail!("fixture record exceeds {MAX_RECORD_BYTES} bytes");
        }
        bytes.extend_from_slice(&record.relative_pts_samples.to_be_bytes());
        bytes.push(record.codec.wire());
        bytes.extend_from_slice(&record.channel_count.to_be_bytes());
        bytes.extend_from_slice(&(record.payload.len() as u32).to_be_bytes());
        bytes.extend_from_slice(&record.payload);
    }
    let digest = Sha256::digest(&bytes);
    let sha256 = hex_digest(&digest);
    bytes.extend_from_slice(&digest);
    let temporary = path.with_extension(format!("tmp.{}", std::process::id()));
    std::fs::write(&temporary, bytes).with_context(|| {
        format!(
            "failed to write fixture temporary file {}",
            temporary.display()
        )
    })?;
    std::fs::rename(&temporary, path)
        .with_context(|| format!("failed to publish fixture {}", path.display()))?;
    Ok(sha256)
}

fn read_fixture(path: &Path) -> Result<Fixture> {
    let bytes = std::fs::read(path)
        .with_context(|| format!("failed to read fixture {}", path.display()))?;
    if bytes.len() < MAGIC.len() + 4 + 8 + 32 || &bytes[..MAGIC.len()] != MAGIC {
        bail!("fixture has an invalid or missing NTV2FIX1 header");
    }
    let body_len = bytes.len() - 32;
    let actual = Sha256::digest(&bytes[..body_len]);
    if actual.as_slice() != &bytes[body_len..] {
        bail!("fixture SHA-256 trailer mismatch");
    }
    let mut offset = MAGIC.len();
    let manifest_len = take_u32(&bytes, &mut offset)? as usize;
    if manifest_len > MAX_MANIFEST_BYTES || offset + manifest_len > body_len {
        bail!("fixture manifest length is invalid");
    }
    let manifest: FixtureManifest = serde_json::from_slice(&bytes[offset..offset + manifest_len])?;
    offset += manifest_len;
    if manifest.schema != SCHEMA {
        bail!("unsupported fixture schema {}", manifest.schema);
    }
    validate_sha256("fixture producer_sha256", &manifest.producer_sha256)?;
    let record_count = take_u64(&bytes, &mut offset)?;
    if record_count != manifest.record_count
        || record_count != manifest.epoch_count.saturating_mul(2)
    {
        bail!("fixture record count does not match its manifest");
    }
    let mut records = Vec::with_capacity(
        usize::try_from(record_count).context("fixture record count exceeds this platform")?,
    );
    for _ in 0..record_count {
        let relative_pts_samples = take_u64(&bytes, &mut offset)?;
        let codec = FixtureCodec::from_wire(take(&bytes, &mut offset, 1)?[0])?;
        let channel_count = take_u16(&bytes, &mut offset)?;
        let payload_len = take_u32(&bytes, &mut offset)? as usize;
        if payload_len > MAX_RECORD_BYTES {
            bail!("fixture record exceeds {MAX_RECORD_BYTES} bytes");
        }
        let payload = take_before(&bytes, &mut offset, payload_len, body_len)?.to_vec();
        if soundkit_codec(&payload)? != codec {
            bail!("fixture record codec tag disagrees with SoundKit v2 header");
        }
        let (sample_rate, payload_frame_count, payload_channels) = soundkit_geometry(&payload)?;
        if sample_rate != manifest.sample_rate
            || payload_frame_count == 0
            || payload_channels != channel_count
            || channel_count != manifest.channel_count
        {
            bail!("fixture record audio geometry disagrees with its manifest");
        }
        records.push(FixtureRecord {
            relative_pts_samples,
            codec,
            channel_count,
            payload,
        });
    }
    if offset != body_len {
        bail!("fixture contains trailing data before its SHA-256 trailer");
    }
    let fixture = Fixture {
        manifest,
        records,
        sha256: hex_digest(&actual),
    };
    fixture_records_by_pts(&fixture)?;
    Ok(fixture)
}

fn fixture_records_by_pts(
    fixture: &Fixture,
) -> Result<BTreeMap<u64, BTreeMap<FixtureCodec, &FixtureRecord>>> {
    let mut result = BTreeMap::<u64, BTreeMap<FixtureCodec, &FixtureRecord>>::new();
    for record in &fixture.records {
        if result
            .entry(record.relative_pts_samples)
            .or_default()
            .insert(record.codec, record)
            .is_some()
        {
            bail!("fixture contains a duplicate codec record at one PTS");
        }
    }
    if result.len() as u64 != fixture.manifest.epoch_count {
        bail!("fixture epoch count does not match its manifest");
    }
    for (index, (pts, codecs)) in result.iter().enumerate() {
        let expected = (index as u64).saturating_mul(u64::from(fixture.manifest.frame_count));
        if *pts != expected
            || codecs.len() != 2
            || !codecs.contains_key(&FixtureCodec::Flac)
            || !codecs.contains_key(&FixtureCodec::Opus)
        {
            bail!("fixture does not contain a contiguous paired FLAC+Opus timeline");
        }
    }
    Ok(result)
}

fn take<'a>(bytes: &'a [u8], offset: &mut usize, len: usize) -> Result<&'a [u8]> {
    take_before(bytes, offset, len, bytes.len())
}

fn take_before<'a>(
    bytes: &'a [u8],
    offset: &mut usize,
    len: usize,
    limit: usize,
) -> Result<&'a [u8]> {
    let end = offset.checked_add(len).context("fixture offset overflow")?;
    if end > limit {
        bail!("fixture is truncated");
    }
    let value = &bytes[*offset..end];
    *offset = end;
    Ok(value)
}

fn take_u16(bytes: &[u8], offset: &mut usize) -> Result<u16> {
    Ok(u16::from_be_bytes(
        take(bytes, offset, 2)?.try_into().unwrap(),
    ))
}

fn take_u32(bytes: &[u8], offset: &mut usize) -> Result<u32> {
    Ok(u32::from_be_bytes(
        take(bytes, offset, 4)?.try_into().unwrap(),
    ))
}

fn take_u64(bytes: &[u8], offset: &mut usize) -> Result<u64> {
    Ok(u64::from_be_bytes(
        take(bytes, offset, 8)?.try_into().unwrap(),
    ))
}

fn validate_sha256(label: &str, value: &str) -> Result<()> {
    if value.len() != 64
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
    {
        bail!("{label} must be a lowercase SHA-256 digest");
    }
    Ok(())
}

fn hex_digest(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut output = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        output.push(HEX[(byte >> 4) as usize] as char);
        output.push(HEX[(byte & 0x0f) as usize] as char);
    }
    output
}

fn percentile(sorted: &[u64], percentile: usize) -> u64 {
    if sorted.is_empty() {
        return 0;
    }
    let index = (sorted.len() - 1).saturating_mul(percentile).div_ceil(100);
    sorted[index.min(sorted.len() - 1)]
}

fn now_unix_ns() -> Result<u64> {
    Ok(SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .context("system clock is before the Unix epoch")?
        .as_nanos()
        .min(u128::from(u64::MAX)) as u64)
}

fn process_cpu_seconds() -> f64 {
    #[cfg(unix)]
    {
        let mut usage = std::mem::MaybeUninit::<libc::rusage>::uninit();
        // SAFETY: getrusage initializes the provided rusage on success.
        if unsafe { libc::getrusage(libc::RUSAGE_SELF, usage.as_mut_ptr()) } == 0 {
            // SAFETY: the successful call initialized usage.
            let usage = unsafe { usage.assume_init() };
            let user = usage.ru_utime.tv_sec as f64 + usage.ru_utime.tv_usec as f64 / 1_000_000.0;
            let system = usage.ru_stime.tv_sec as f64 + usage.ru_stime.tv_usec as f64 / 1_000_000.0;
            return user + system;
        }
    }
    0.0
}
