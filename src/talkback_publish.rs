//! Typed handoff from the capability-gated ingress lease to the live talkback lane.

use bytes::Bytes;
use media_object::MediaClass;
use talkback_media::{
    EphemeralTalkbackFrameV1, TalkbackCodecV1, TalkbackFrameV1, TalkbackFrameV1Params,
    TALKBACK_CHANNELS, TALKBACK_FRAME_SAMPLES, TALKBACK_SAMPLE_RATE,
};

use crate::ingress_authorization::{PublishIngressError, PublishLease, PublishRejectionCode};

/// Talkback frames may wait at most 100 ms after contributor admission.
pub const TALKBACK_CONTRIBUTOR_LIFETIME_US: u64 = 100_000;

/// Convert one fully authorized ingress lease into the only talkback media type
/// accepted by the relay lane. This bypasses the generic `MediaObject` path.
pub fn ephemeral_talkback_frame(
    lease: &PublishLease,
    payload: &[u8],
    accepted_at_unix_us: u64,
) -> Result<EphemeralTalkbackFrameV1, PublishIngressError> {
    if lease.identity().media_class() != MediaClass::Talkback {
        return Err(isolation_error("media_class"));
    }
    let audience_id = lease
        .identity()
        .audience_id()
        .ok_or_else(|| isolation_error("audience_id"))?;
    let talkback_epoch = lease
        .class_authorization_epoch()
        .ok_or_else(|| isolation_error("talkback_epoch"))?;
    let envelope = lease.envelope();
    if envelope.duration_ticks() != TALKBACK_FRAME_SAMPLES
        || payload.len() != envelope.payload_bytes() as usize
    {
        return Err(isolation_error("frame_profile"));
    }
    let capture_pts =
        u64::try_from(envelope.capture_pts()).map_err(|_| isolation_error("capture_pts"))?;
    let capture_pts_us = capture_pts
        .checked_mul(1_000_000)
        .map(|value| value / u64::from(TALKBACK_SAMPLE_RATE))
        .and_then(|value| i64::try_from(value).ok())
        .ok_or_else(|| isolation_error("capture_pts"))?;
    let frame = TalkbackFrameV1::new(TalkbackFrameV1Params {
        session_id: lease.identity().session_id().as_str().to_owned(),
        session_epoch: lease.identity().session_epoch(),
        media_authorization_epoch: lease.media_authorization_epoch(),
        subject_grant_epoch: lease.subject_grant_epoch(),
        talkback_epoch,
        policy_version: lease.media_policy_version(),
        publisher_participant_id: lease.identity().participant_id().as_str().to_owned(),
        publisher_endpoint_id: lease.identity().endpoint_id().as_str().to_owned(),
        audience_id: audience_id.as_str().to_owned(),
        sequence: envelope.sequence(),
        capture_pts_us,
        codec: TalkbackCodecV1::Opus,
        sample_rate: TALKBACK_SAMPLE_RATE,
        channels: TALKBACK_CHANNELS,
        frame_samples: TALKBACK_FRAME_SAMPLES,
        payload: Bytes::copy_from_slice(payload),
    })
    .map_err(|_| isolation_error("talkback_frame"))?;
    let capability_deadline = u64::try_from(lease.expires_at())
        .ok()
        .and_then(|seconds| seconds.checked_mul(1_000_000))
        .ok_or_else(|| isolation_error("expires_at"))?;
    let live_deadline = accepted_at_unix_us
        .checked_add(TALKBACK_CONTRIBUTOR_LIFETIME_US)
        .ok_or_else(|| isolation_error("deadline"))?;
    let deadline = capability_deadline.min(live_deadline);
    EphemeralTalkbackFrameV1::new(frame, accepted_at_unix_us, deadline)
        .map_err(|_| isolation_error("deadline"))
}

fn isolation_error(field: &'static str) -> PublishIngressError {
    PublishIngressError::integration(PublishRejectionCode::TalkbackIsolation, field)
}
