//! Capability-gated publication admission at the first contributor boundary.
//!
//! This module deliberately owns no signing or cryptographic policy. It adapts
//! the shared `media-capability` verifier, resolves the compact frozen
//! `media-object` frame envelope against a control-installed binding, and
//! returns an immutable lease before callers read or inspect payload bytes.

use std::collections::{btree_map::Entry, BTreeMap};
use std::fmt;
use std::io::Read as _;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock};

use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use base64::Engine as _;
use media_capability::{
    AuthorizedMediaCapability, CapabilityVerifierError, CapabilityVerifierErrorCode,
    CurrentMediaAuthorizationContextV1, EdgeId, MediaCapabilityVerifier, ReplayAdmissionGuard,
    ReplayAdmissionRejection, ReplayAdmissionV1, VerificationKeyring, MAX_COMPACT_JWS_BYTES,
};
use media_object::{
    ClockConfidence, ClockTimestamp, MediaCaptureDisposition, MediaClass, MediaControlErrorCode,
    MediaFrameConfigurationV1, MediaFrameEnvelopeV1, MediaFramePayloadFormat, MediaObject,
    ObjectKey, ObjectKind, Operation, SessionMediaIdentityV1, MEDIA_CONTROL_MAX_CLOCK_SKEW_SECONDS,
    MEDIA_CONTROL_MAX_GENERATION, MEDIA_CONTROL_MAX_JSON_BYTES,
};
use serde::Deserialize;

/// HTTP header containing an unpadded base64url canonical frame-envelope JSON.
pub const MEDIA_FRAME_ENVELOPE_HEADER: &str = "x-infidelity-media-frame-envelope";
/// Maximum trusted startup-bundle bytes. The reader never allocates beyond this plus one.
pub const MAX_PUBLISH_AUTHORIZATION_BUNDLE_BYTES: usize = 1024 * 1024;
const MAX_BOOTSTRAP_KEYS: usize = 16;
const MAX_BOOTSTRAP_BINDINGS: usize = 4096;
const MAX_ACTIVE_PUBLISH_LEASES: usize = 65_536;
const SEQUENCE_WINDOW_BITS: u64 = 128;
const REJECTION_CODE_COUNT: usize = 25;

/// Runtime migration state. `Observe` never rejects traffic; `Enforce` does.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PublishAuthorizationMode {
    Off,
    Observe,
    Enforce,
}

impl PublishAuthorizationMode {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Off => "off",
            Self::Observe => "observe",
            Self::Enforce => "enforce",
        }
    }
}

/// Finite, non-sensitive rejection labels safe for logs and metrics.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
#[repr(u8)]
pub enum PublishRejectionCode {
    LegacyPath = 0,
    MissingCapability = 1,
    MalformedAuthorization = 2,
    CapabilityTooLarge = 3,
    MissingEnvelope = 4,
    EnvelopeTooLarge = 5,
    MalformedEnvelope = 6,
    NonCanonicalEnvelope = 7,
    MissingContentLength = 8,
    InvalidContentLength = 9,
    PayloadLengthMismatch = 10,
    UnknownBinding = 11,
    RevokedBinding = 12,
    ConfigurationMismatch = 13,
    InvalidSignature = 14,
    WrongScope = 15,
    CapabilityRejected = 16,
    CapabilityReplay = 17,
    FrameReplay = 18,
    CapabilityExpired = 19,
    ChannelLimit = 20,
    BitrateLimit = 21,
    StreamMismatch = 22,
    TalkbackIsolation = 23,
    DatagramLimit = 24,
}

impl PublishRejectionCode {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::LegacyPath => "legacy_path",
            Self::MissingCapability => "missing_capability",
            Self::MalformedAuthorization => "malformed_authorization",
            Self::CapabilityTooLarge => "capability_too_large",
            Self::MissingEnvelope => "missing_envelope",
            Self::EnvelopeTooLarge => "envelope_too_large",
            Self::MalformedEnvelope => "malformed_envelope",
            Self::NonCanonicalEnvelope => "non_canonical_envelope",
            Self::MissingContentLength => "missing_content_length",
            Self::InvalidContentLength => "invalid_content_length",
            Self::PayloadLengthMismatch => "payload_length_mismatch",
            Self::UnknownBinding => "unknown_binding",
            Self::RevokedBinding => "revoked_binding",
            Self::ConfigurationMismatch => "configuration_mismatch",
            Self::InvalidSignature => "invalid_signature",
            Self::WrongScope => "wrong_scope",
            Self::CapabilityRejected => "capability_rejected",
            Self::CapabilityReplay => "capability_replay",
            Self::FrameReplay => "frame_replay",
            Self::CapabilityExpired => "capability_expired",
            Self::ChannelLimit => "channel_limit",
            Self::BitrateLimit => "bitrate_limit",
            Self::StreamMismatch => "stream_mismatch",
            Self::TalkbackIsolation => "talkback_isolation",
            Self::DatagramLimit => "datagram_limit",
        }
    }
}

/// A bounded, value-free ingress error. It cannot print token or identity material.
#[derive(Clone, Eq, PartialEq)]
pub struct PublishIngressError {
    code: PublishRejectionCode,
    field: &'static str,
}

impl PublishIngressError {
    const fn new(code: PublishRejectionCode, field: &'static str) -> Self {
        Self { code, field }
    }

    /// Construct a value-free integration error for a fixed protocol field.
    #[must_use]
    pub const fn integration(code: PublishRejectionCode, field: &'static str) -> Self {
        Self::new(code, field)
    }

    #[must_use]
    pub const fn code(&self) -> PublishRejectionCode {
        self.code
    }

    #[must_use]
    pub const fn field(&self) -> &'static str {
        self.field
    }
}

impl fmt::Debug for PublishIngressError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PublishIngressError")
            .field("code", &self.code)
            .field("field", &self.field)
            .finish()
    }
}

impl fmt::Display for PublishIngressError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}: request rejected", self.code.as_str())
    }
}

impl std::error::Error for PublishIngressError {}

/// Current control-plane facts paired with one frozen frame configuration.
#[derive(Clone)]
pub struct CurrentPublishBinding {
    configuration: Arc<MediaFrameConfigurationV1>,
    legacy_stream_id: u64,
    media_authorization_epoch: u64,
    subject_grant_epoch: u64,
    media_policy_version: u64,
    class_authorization_epoch: Option<u64>,
    operation: Operation,
    edge_id: EdgeId,
    clock_skew_seconds: i64,
}

impl CurrentPublishBinding {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        configuration: MediaFrameConfigurationV1,
        legacy_stream_id: u64,
        media_authorization_epoch: u64,
        subject_grant_epoch: u64,
        media_policy_version: u64,
        class_authorization_epoch: Option<u64>,
        operation: Operation,
        edge_id: EdgeId,
        clock_skew_seconds: i64,
    ) -> Result<Self, PublishIngressError> {
        for generation in [
            media_authorization_epoch,
            subject_grant_epoch,
            media_policy_version,
        ] {
            if generation == 0 || generation > MEDIA_CONTROL_MAX_GENERATION {
                return Err(PublishIngressError::new(
                    PublishRejectionCode::ConfigurationMismatch,
                    "authorization_generation",
                ));
            }
        }
        if class_authorization_epoch
            .is_some_and(|value| value == 0 || value > MEDIA_CONTROL_MAX_GENERATION)
        {
            return Err(PublishIngressError::new(
                PublishRejectionCode::ConfigurationMismatch,
                "class_authorization_epoch",
            ));
        }
        if !(0..=MEDIA_CONTROL_MAX_CLOCK_SKEW_SECONDS).contains(&clock_skew_seconds) {
            return Err(PublishIngressError::new(
                PublishRejectionCode::ConfigurationMismatch,
                "clock_skew_seconds",
            ));
        }
        let expected_operation = if configuration.identity().media_class() == MediaClass::TakeChunk
        {
            Operation::UploadTake
        } else {
            Operation::Publish
        };
        if operation != expected_operation {
            return Err(PublishIngressError::new(
                PublishRejectionCode::ConfigurationMismatch,
                "operation",
            ));
        }
        validate_talkback_isolation(&configuration)?;
        Ok(Self {
            configuration: Arc::new(configuration),
            legacy_stream_id,
            media_authorization_epoch,
            subject_grant_epoch,
            media_policy_version,
            class_authorization_epoch,
            operation,
            edge_id,
            clock_skew_seconds,
        })
    }

    #[must_use]
    pub fn configuration(&self) -> &MediaFrameConfigurationV1 {
        &self.configuration
    }

    #[must_use]
    pub const fn legacy_stream_id(&self) -> u64 {
        self.legacy_stream_id
    }

    fn key(&self) -> BindingKey {
        BindingKey::from_configuration(&self.configuration)
    }
}

impl fmt::Debug for CurrentPublishBinding {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CurrentPublishBinding")
            .field("configuration", &self.configuration)
            .field("operation", &self.operation)
            .finish_non_exhaustive()
    }
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct BindingKey {
    binding_generation: u64,
    configuration_ref: u32,
    configuration_epoch: u64,
}

impl BindingKey {
    fn from_configuration(configuration: &MediaFrameConfigurationV1) -> Self {
        Self {
            binding_generation: configuration.binding_generation(),
            configuration_ref: configuration.configuration_ref(),
            configuration_epoch: configuration.configuration_epoch(),
        }
    }

    fn from_envelope(envelope: &MediaFrameEnvelopeV1) -> Self {
        Self {
            binding_generation: envelope.binding_generation(),
            configuration_ref: envelope.configuration_ref(),
            configuration_epoch: envelope.configuration_epoch(),
        }
    }
}

#[derive(Clone)]
struct RegisteredBinding {
    binding: Arc<CurrentPublishBinding>,
    revision: u64,
    active: bool,
}

#[derive(Default)]
struct BindingRegistryState {
    next_revision: u64,
    bindings: BTreeMap<BindingKey, RegisteredBinding>,
}

/// Authenticated-control-installed bindings with explicit, synchronous invalidation.
#[derive(Default)]
pub struct PublishBindingRegistry {
    state: RwLock<BindingRegistryState>,
}

impl PublishBindingRegistry {
    #[must_use]
    pub const fn new() -> Self {
        Self {
            state: RwLock::new(BindingRegistryState {
                next_revision: 0,
                bindings: BTreeMap::new(),
            }),
        }
    }

    pub fn install(&self, binding: CurrentPublishBinding) -> Result<u64, PublishIngressError> {
        let key = binding.key();
        let mut state = self
            .state
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if state.bindings.values().any(|entry| {
            entry.binding.legacy_stream_id == binding.legacy_stream_id
                && entry.binding.configuration.identity() != binding.configuration.identity()
        }) {
            return Err(PublishIngressError::new(
                PublishRejectionCode::StreamMismatch,
                "legacy_stream_id",
            ));
        }
        // Exact coordinates are immutable for the process lifetime. Revoked
        // coordinates require a newer epoch/generation and cannot be revived
        // by a delayed control event.
        if state.bindings.contains_key(&key) {
            return Err(PublishIngressError::new(
                PublishRejectionCode::ConfigurationMismatch,
                "configuration_coordinates",
            ));
        }
        state.next_revision = state.next_revision.saturating_add(1).max(1);
        let revision = state.next_revision;
        state.bindings.insert(
            key,
            RegisteredBinding {
                binding: Arc::new(binding),
                revision,
                active: true,
            },
        );
        Ok(revision)
    }

    /// Immediately fence one exact configuration. Older and repeated events are idempotent.
    pub fn invalidate(
        &self,
        binding_generation: u64,
        configuration_ref: u32,
        configuration_epoch: u64,
    ) -> bool {
        let key = BindingKey {
            binding_generation,
            configuration_ref,
            configuration_epoch,
        };
        let mut state = self
            .state
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let should_invalidate = state.bindings.get(&key).is_some_and(|entry| entry.active);
        if !should_invalidate {
            return false;
        }
        state.next_revision = state.next_revision.saturating_add(1).max(1);
        let revision = state.next_revision;
        if let Some(entry) = state.bindings.get_mut(&key) {
            entry.active = false;
            entry.revision = revision;
        }
        true
    }

    fn resolve(&self, key: BindingKey) -> Result<RegisteredBinding, PublishIngressError> {
        let state = self
            .state
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let entry = state.bindings.get(&key).ok_or_else(|| {
            PublishIngressError::new(PublishRejectionCode::UnknownBinding, "configuration_ref")
        })?;
        if !entry.active {
            return Err(PublishIngressError::new(
                PublishRejectionCode::RevokedBinding,
                "configuration_ref",
            ));
        }
        Ok(entry.clone())
    }

    fn revalidate(&self, key: BindingKey, revision: u64) -> Result<(), PublishIngressError> {
        let entry = self.resolve(key)?;
        if entry.revision != revision {
            return Err(PublishIngressError::new(
                PublishRejectionCode::RevokedBinding,
                "configuration_ref",
            ));
        }
        Ok(())
    }
}

/// Minimal adapter seam around the shared verifier; implementations must not sign.
pub trait PublishLeaseVerifier: Send + Sync {
    fn authorize(
        &self,
        compact_jws: &str,
        context: &CurrentMediaAuthorizationContextV1<'_>,
        guard: &mut dyn ReplayAdmissionGuard,
    ) -> Result<VerifiedPublishCapability, CapabilityVerifierError>;
}

impl PublishLeaseVerifier for MediaCapabilityVerifier {
    fn authorize(
        &self,
        compact_jws: &str,
        context: &CurrentMediaAuthorizationContextV1<'_>,
        guard: &mut dyn ReplayAdmissionGuard,
    ) -> Result<VerifiedPublishCapability, CapabilityVerifierError> {
        self.authorize(compact_jws, context, guard)
            .map(VerifiedPublishCapability::from_authorized)
    }
}

/// Redacted verified limits copied out of the shared verifier's immutable result.
pub struct VerifiedPublishCapability {
    capability_id: String,
    expires_at: i64,
    max_channels: u16,
    max_bitrate: u64,
    max_datagram_bytes: u32,
}

impl VerifiedPublishCapability {
    fn from_authorized(authorized: AuthorizedMediaCapability) -> Self {
        let claims = authorized.claims();
        Self {
            capability_id: claims.capability_id().as_str().to_owned(),
            expires_at: claims.expires_at(),
            max_channels: claims.max_channels(),
            max_bitrate: claims.max_bitrate(),
            max_datagram_bytes: claims.max_datagram_bytes(),
        }
    }
}

impl fmt::Debug for VerifiedPublishCapability {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("VerifiedPublishCapability")
            .field("capability_id", &"[REDACTED]")
            .field("expires_at", &self.expires_at)
            .field("max_channels", &self.max_channels)
            .field("max_bitrate", &self.max_bitrate)
            .field("max_datagram_bytes", &self.max_datagram_bytes)
            .finish()
    }
}

#[derive(Clone, Eq, PartialEq)]
struct AdmissionFingerprint {
    session_id: String,
    endpoint_id: String,
    edge_id: String,
    operation: Operation,
    key: BindingKey,
    source_id: Option<String>,
    audience_id: Option<String>,
    take_id: Option<String>,
}

struct CapabilityAdmission {
    fingerprint: AdmissionFingerprint,
    expires_at: i64,
}

struct AdmissionGuard<'a> {
    admissions: &'a mut BTreeMap<String, CapabilityAdmission>,
    fingerprint: AdmissionFingerprint,
    now: i64,
}

impl ReplayAdmissionGuard for AdmissionGuard<'_> {
    fn check_and_admit(
        &mut self,
        admission: ReplayAdmissionV1<'_>,
    ) -> Result<(), ReplayAdmissionRejection> {
        self.admissions
            .retain(|_, prior| prior.expires_at > self.now);
        let capability_id = admission.capability_id.as_str().to_owned();
        if !self.admissions.contains_key(&capability_id)
            && self.admissions.len() >= MAX_ACTIVE_PUBLISH_LEASES
        {
            return Err(ReplayAdmissionRejection::Capacity);
        }
        match self.admissions.entry(capability_id) {
            Entry::Vacant(entry) => {
                entry.insert(CapabilityAdmission {
                    fingerprint: self.fingerprint.clone(),
                    expires_at: admission.expires_at,
                });
                Ok(())
            }
            Entry::Occupied(mut entry) if entry.get().fingerprint == self.fingerprint => {
                entry.get_mut().expires_at = admission.expires_at;
                Ok(())
            }
            Entry::Occupied(_) => Err(ReplayAdmissionRejection::Replay),
        }
    }
}

#[derive(Clone, Copy)]
struct SequenceWindow {
    highest: u64,
    seen: u128,
}

struct SequenceAdmission {
    window: SequenceWindow,
    expires_at: i64,
}

impl SequenceWindow {
    const fn first(sequence: u64) -> Self {
        Self {
            highest: sequence,
            seen: 1,
        }
    }

    fn admit(&mut self, sequence: u64) -> bool {
        if sequence > self.highest {
            let shift = sequence - self.highest;
            self.seen = if shift >= SEQUENCE_WINDOW_BITS {
                1
            } else {
                (self.seen << shift) | 1
            };
            self.highest = sequence;
            return true;
        }
        let distance = self.highest - sequence;
        if distance >= SEQUENCE_WINDOW_BITS {
            return false;
        }
        let bit = 1_u128 << distance;
        if self.seen & bit != 0 {
            return false;
        }
        self.seen |= bit;
        true
    }
}

/// Borrowed inputs collected from headers before a request body is read.
pub struct PublishIngressRequest<'a> {
    pub compact_jws: &'a str,
    pub envelope_json: &'a [u8],
    pub content_length: u64,
    pub legacy_stream_id: u64,
    pub now_unix_seconds: i64,
}

/// An immutable, exact authorization snapshot for one admitted frame.
#[derive(Clone)]
pub struct PublishLease {
    binding: Arc<CurrentPublishBinding>,
    envelope: MediaFrameEnvelopeV1,
    binding_revision: u64,
    expires_at: i64,
    max_datagram_bytes: u32,
}

impl PublishLease {
    #[must_use]
    pub fn identity(&self) -> &SessionMediaIdentityV1 {
        self.binding.configuration.identity()
    }

    #[must_use]
    pub fn configuration(&self) -> &MediaFrameConfigurationV1 {
        &self.binding.configuration
    }

    #[must_use]
    pub const fn envelope(&self) -> &MediaFrameEnvelopeV1 {
        &self.envelope
    }

    #[must_use]
    pub const fn max_datagram_bytes(&self) -> u32 {
        self.max_datagram_bytes
    }

    #[must_use]
    pub const fn expires_at(&self) -> i64 {
        self.expires_at
    }

    /// Return the non-authorizing numeric carrier route installed by control.
    #[must_use]
    pub fn carrier_stream_id(&self) -> u64 {
        self.binding.legacy_stream_id
    }

    /// Wrap admitted bytes in the immutable canonical object carried to mesh.
    ///
    /// The complete frozen configuration and envelope survive the wire handoff.
    /// The numeric stream is a routing handle only and never replaces the
    /// tenant/session/contributor/source or audience identity in configuration.
    pub fn canonical_media_object(
        &self,
        payload: &[u8],
    ) -> Result<MediaObject, PublishIngressError> {
        if self.identity().media_class() == MediaClass::Talkback {
            return Err(PublishIngressError::new(
                PublishRejectionCode::TalkbackIsolation,
                "canonical_media_object",
            ));
        }
        if payload.len() != self.envelope.payload_bytes() as usize {
            return Err(PublishIngressError::new(
                PublishRejectionCode::PayloadLengthMismatch,
                "payload_bytes",
            ));
        }
        let identity = self.binding.configuration.identity();
        let key = ObjectKey::for_payload(
            identity.tenant_id().as_str(),
            self.binding.legacy_stream_id.to_string(),
            self.binding.configuration.configuration_id().as_str(),
            self.envelope.binding_generation(),
            u64::from(self.envelope.configuration_ref()),
            self.envelope.sequence(),
            1,
            payload,
        )
        .map_err(|_| {
            PublishIngressError::new(
                PublishRejectionCode::ConfigurationMismatch,
                "canonical_object_key",
            )
        })?;
        let deadline_ns = self.expires_at.checked_mul(1_000_000_000).ok_or_else(|| {
            PublishIngressError::new(PublishRejectionCode::ConfigurationMismatch, "expires_at")
        })?;
        let deadline = ClockTimestamp::new(
            deadline_ns,
            "media-capability:issuer",
            ClockConfidence::unknown(),
        )
        .map_err(|_| {
            PublishIngressError::new(PublishRejectionCode::ConfigurationMismatch, "expires_at")
        })?;
        let configuration = self
            .binding
            .configuration
            .to_canonical_json_vec()
            .map_err(map_envelope_error)?;
        let envelope = self
            .envelope
            .to_canonical_json_vec()
            .map_err(map_envelope_error)?;
        let operation: &[u8] = match self.binding.operation {
            Operation::Publish => b"publish",
            Operation::UploadTake => b"upload_take",
            _ => {
                return Err(PublishIngressError::new(
                    PublishRejectionCode::ConfigurationMismatch,
                    "operation",
                ));
            }
        };
        MediaObject::builder(key, ObjectKind::Media, payload.to_vec())
            .with_configuration_epoch(self.envelope.configuration_epoch())
            .with_deadline(deadline)
            .with_metadata("media-control-contract", b"v1".to_vec())
            .with_metadata("media-operation-v1", operation.to_vec())
            .with_metadata("media-frame-configuration-v1", configuration)
            .with_metadata("media-frame-envelope-v1", envelope)
            .build()
            .map_err(|_| {
                PublishIngressError::new(
                    PublishRejectionCode::ConfigurationMismatch,
                    "canonical_media_object",
                )
            })
    }

    #[must_use]
    pub fn media_authorization_epoch(&self) -> u64 {
        self.binding.media_authorization_epoch
    }

    #[must_use]
    pub fn subject_grant_epoch(&self) -> u64 {
        self.binding.subject_grant_epoch
    }

    #[must_use]
    pub fn media_policy_version(&self) -> u64 {
        self.binding.media_policy_version
    }

    #[must_use]
    pub fn class_authorization_epoch(&self) -> Option<u64> {
        self.binding.class_authorization_epoch
    }
}

impl fmt::Debug for PublishLease {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PublishLease")
            .field("configuration", &self.binding.configuration)
            .field("envelope", &self.envelope)
            .field("expires_at", &self.expires_at)
            .field("max_datagram_bytes", &self.max_datagram_bytes)
            .finish_non_exhaustive()
    }
}

/// Result in dark mode carries the exact mismatch while allowing legacy behavior.
pub struct PublishAdmission {
    lease: Option<PublishLease>,
    observed_rejection: Option<PublishIngressError>,
}

impl PublishAdmission {
    #[must_use]
    pub fn lease(&self) -> Option<&PublishLease> {
        self.lease.as_ref()
    }

    #[must_use]
    pub fn observed_rejection(&self) -> Option<&PublishIngressError> {
        self.observed_rejection.as_ref()
    }
}

struct GateMetrics {
    admitted: AtomicU64,
    observed_mismatches: AtomicU64,
    enforced_rejections: AtomicU64,
    rejection_reasons: [AtomicU64; REJECTION_CODE_COUNT],
}

impl Default for GateMetrics {
    fn default() -> Self {
        Self {
            admitted: AtomicU64::new(0),
            observed_mismatches: AtomicU64::new(0),
            enforced_rejections: AtomicU64::new(0),
            rejection_reasons: std::array::from_fn(|_| AtomicU64::new(0)),
        }
    }
}

/// Thread-safe first-boundary publication gate.
pub struct PublishIngressGate {
    mode: PublishAuthorizationMode,
    verifier: Arc<dyn PublishLeaseVerifier>,
    registry: Arc<PublishBindingRegistry>,
    capability_admissions: Mutex<BTreeMap<String, CapabilityAdmission>>,
    sequence_windows: Mutex<BTreeMap<(String, BindingKey), SequenceAdmission>>,
    metrics: GateMetrics,
}

impl PublishIngressGate {
    #[must_use]
    pub fn new(
        mode: PublishAuthorizationMode,
        verifier: Arc<dyn PublishLeaseVerifier>,
        registry: Arc<PublishBindingRegistry>,
    ) -> Self {
        Self {
            mode,
            verifier,
            registry,
            capability_admissions: Mutex::new(BTreeMap::new()),
            sequence_windows: Mutex::new(BTreeMap::new()),
            metrics: GateMetrics::default(),
        }
    }

    #[must_use]
    pub const fn mode(&self) -> PublishAuthorizationMode {
        self.mode
    }

    #[must_use]
    pub fn registry(&self) -> &Arc<PublishBindingRegistry> {
        &self.registry
    }

    /// Validate and consume a frame sequence without touching its body.
    pub fn authorize(
        &self,
        request: &PublishIngressRequest<'_>,
    ) -> Result<PublishAdmission, PublishIngressError> {
        if self.mode == PublishAuthorizationMode::Off {
            return Ok(PublishAdmission {
                lease: None,
                observed_rejection: None,
            });
        }
        let result = self.authorize_strict(request);
        match (self.mode, result) {
            (PublishAuthorizationMode::Off, _) => unreachable!("off mode returned above"),
            (PublishAuthorizationMode::Observe, Ok(lease)) => {
                self.metrics.admitted.fetch_add(1, Ordering::Relaxed);
                Ok(PublishAdmission {
                    lease: Some(lease),
                    observed_rejection: None,
                })
            }
            (PublishAuthorizationMode::Observe, Err(error)) => {
                self.record_rejection(&error, false);
                Ok(PublishAdmission {
                    lease: None,
                    observed_rejection: Some(error),
                })
            }
            (PublishAuthorizationMode::Enforce, Ok(lease)) => {
                self.metrics.admitted.fetch_add(1, Ordering::Relaxed);
                Ok(PublishAdmission {
                    lease: Some(lease),
                    observed_rejection: None,
                })
            }
            (PublishAuthorizationMode::Enforce, Err(error)) => {
                self.record_rejection(&error, true);
                Err(error)
            }
        }
    }

    /// Reject raw or pre-contract adapters before they consume their first byte.
    pub fn authorize_legacy_path(&self) -> Result<(), PublishIngressError> {
        if self.mode == PublishAuthorizationMode::Enforce {
            let error = PublishIngressError::new(PublishRejectionCode::LegacyPath, "ingress_path");
            self.record_rejection(&error, true);
            Err(error)
        } else {
            if self.mode == PublishAuthorizationMode::Observe {
                let error =
                    PublishIngressError::new(PublishRejectionCode::LegacyPath, "ingress_path");
                self.record_rejection(&error, false);
            }
            Ok(())
        }
    }

    /// Recheck expiry, invalidation, and exact bytes immediately before forwarding.
    pub fn revalidate_before_forward(
        &self,
        lease: &PublishLease,
        actual_payload_bytes: usize,
        now_unix_seconds: i64,
    ) -> Result<(), PublishIngressError> {
        let result = self.revalidate_strict(lease, actual_payload_bytes, now_unix_seconds);
        match (self.mode, result) {
            (PublishAuthorizationMode::Off, _) | (_, Ok(())) => Ok(()),
            (PublishAuthorizationMode::Observe, Err(error)) => {
                self.record_rejection(&error, false);
                Ok(())
            }
            (PublishAuthorizationMode::Enforce, Err(error)) => {
                self.record_rejection(&error, true);
                Err(error)
            }
        }
    }

    fn revalidate_strict(
        &self,
        lease: &PublishLease,
        actual_payload_bytes: usize,
        now_unix_seconds: i64,
    ) -> Result<(), PublishIngressError> {
        if now_unix_seconds >= lease.expires_at {
            return Err(PublishIngressError::new(
                PublishRejectionCode::CapabilityExpired,
                "expires_at",
            ));
        }
        if actual_payload_bytes != lease.envelope.payload_bytes() as usize {
            return Err(PublishIngressError::new(
                PublishRejectionCode::PayloadLengthMismatch,
                "payload_bytes",
            ));
        }
        self.registry.revalidate(
            BindingKey::from_envelope(&lease.envelope),
            lease.binding_revision,
        )
    }

    /// Render only finite labels and counts; no capability or identity values.
    #[must_use]
    pub fn prometheus_metrics(&self) -> String {
        let mut output = format!(
            "# HELP av_contrib_publish_authorization_total Publish authorization decisions.\n# TYPE av_contrib_publish_authorization_total counter\nav_contrib_publish_authorization_total{{mode=\"{}\",decision=\"admit\"}} {}\nav_contrib_publish_authorization_total{{mode=\"observe\",decision=\"mismatch\"}} {}\nav_contrib_publish_authorization_total{{mode=\"enforce\",decision=\"reject\"}} {}\n# HELP av_contrib_publish_authorization_rejections_total Publish authorization rejections by stable reason.\n# TYPE av_contrib_publish_authorization_rejections_total counter\n",
            self.mode.as_str(),
            self.metrics.admitted.load(Ordering::Relaxed),
            self.metrics.observed_mismatches.load(Ordering::Relaxed),
            self.metrics.enforced_rejections.load(Ordering::Relaxed),
        );
        for code in ALL_REJECTION_CODES {
            let count = self.metrics.rejection_reasons[code as usize].load(Ordering::Relaxed);
            output.push_str(&format!(
                "av_contrib_publish_authorization_rejections_total{{reason=\"{}\"}} {}\n",
                code.as_str(),
                count
            ));
        }
        output
    }

    fn authorize_strict(
        &self,
        request: &PublishIngressRequest<'_>,
    ) -> Result<PublishLease, PublishIngressError> {
        if request.compact_jws.is_empty() {
            return Err(PublishIngressError::new(
                PublishRejectionCode::MissingCapability,
                "authorization",
            ));
        }
        if request.compact_jws.len() > MAX_COMPACT_JWS_BYTES {
            return Err(PublishIngressError::new(
                PublishRejectionCode::CapabilityTooLarge,
                "authorization",
            ));
        }
        if request.envelope_json.is_empty() {
            return Err(PublishIngressError::new(
                PublishRejectionCode::MissingEnvelope,
                "frame_envelope",
            ));
        }
        if request.envelope_json.len() > MEDIA_CONTROL_MAX_JSON_BYTES {
            return Err(PublishIngressError::new(
                PublishRejectionCode::EnvelopeTooLarge,
                "frame_envelope",
            ));
        }
        let envelope = MediaFrameEnvelopeV1::from_json_slice(request.envelope_json)
            .map_err(map_envelope_error)?;
        let canonical = envelope
            .to_canonical_json_vec()
            .map_err(map_envelope_error)?;
        if canonical != request.envelope_json {
            return Err(PublishIngressError::new(
                PublishRejectionCode::NonCanonicalEnvelope,
                "frame_envelope",
            ));
        }
        if request.content_length != u64::from(envelope.payload_bytes()) {
            return Err(PublishIngressError::new(
                PublishRejectionCode::PayloadLengthMismatch,
                "content_length",
            ));
        }

        let key = BindingKey::from_envelope(&envelope);
        let registered = self.registry.resolve(key)?;
        let binding = &registered.binding;
        envelope
            .resolve(&binding.configuration)
            .map_err(map_envelope_error)?;
        if request.legacy_stream_id != binding.legacy_stream_id {
            return Err(PublishIngressError::new(
                PublishRejectionCode::StreamMismatch,
                "stream_id",
            ));
        }
        validate_talkback_isolation(&binding.configuration)?;

        let identity = binding.configuration.identity();
        let fingerprint = AdmissionFingerprint {
            session_id: identity.session_id().as_str().to_owned(),
            endpoint_id: identity.endpoint_id().as_str().to_owned(),
            edge_id: binding.edge_id.as_str().to_owned(),
            operation: binding.operation,
            key,
            source_id: identity.source_id().map(|id| id.as_str().to_owned()),
            audience_id: identity.audience_id().map(|id| id.as_str().to_owned()),
            take_id: identity.take_id().map(|id| id.as_str().to_owned()),
        };
        let context = CurrentMediaAuthorizationContextV1 {
            tenant_id: identity.tenant_id(),
            session_id: identity.session_id(),
            session_epoch: identity.session_epoch(),
            media_authorization_epoch: binding.media_authorization_epoch,
            subject_grant_epoch: binding.subject_grant_epoch,
            media_policy_version: binding.media_policy_version,
            class_authorization_epoch: binding.class_authorization_epoch,
            binding_generation: binding.configuration.binding_generation(),
            topology_generation: identity.topology_generation(),
            participant_id: identity.participant_id(),
            endpoint_id: identity.endpoint_id(),
            contributor_id: Some(identity.contributor_id()),
            operation: binding.operation,
            media_class: identity.media_class(),
            source_id: identity.source_id(),
            audience_id: identity.audience_id(),
            take_id: identity.take_id(),
            edge_id: &binding.edge_id,
            now: request.now_unix_seconds,
            clock_skew_seconds: binding.clock_skew_seconds,
        };
        let mut admissions = self
            .capability_admissions
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let mut guard = AdmissionGuard {
            admissions: &mut admissions,
            fingerprint,
            now: request.now_unix_seconds,
        };
        let verified = self
            .verifier
            .authorize(request.compact_jws, &context, &mut guard)
            .map_err(map_verifier_error)?;
        drop(admissions);

        if request.now_unix_seconds >= verified.expires_at {
            return Err(PublishIngressError::new(
                PublishRejectionCode::CapabilityExpired,
                "expires_at",
            ));
        }
        if binding.configuration.channel_count() > verified.max_channels {
            return Err(PublishIngressError::new(
                PublishRejectionCode::ChannelLimit,
                "channel_count",
            ));
        }
        let required_bits = u128::from(envelope.payload_bytes())
            .saturating_mul(8)
            .saturating_mul(u128::from(binding.configuration.capture_timebase_hz()));
        let allowed_bits =
            u128::from(verified.max_bitrate).saturating_mul(u128::from(envelope.duration_ticks()));
        if required_bits > allowed_bits {
            return Err(PublishIngressError::new(
                PublishRejectionCode::BitrateLimit,
                "max_bitrate",
            ));
        }

        let mut sequences = self
            .sequence_windows
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        sequences.retain(|_, prior| prior.expires_at > request.now_unix_seconds);
        let sequence_key = (verified.capability_id, key);
        if !sequences.contains_key(&sequence_key) && sequences.len() >= MAX_ACTIVE_PUBLISH_LEASES {
            return Err(PublishIngressError::new(
                PublishRejectionCode::CapabilityRejected,
                "admission_capacity",
            ));
        }
        let sequence_admitted = match sequences.entry(sequence_key) {
            Entry::Vacant(entry) => {
                entry.insert(SequenceAdmission {
                    window: SequenceWindow::first(envelope.sequence()),
                    expires_at: verified.expires_at,
                });
                true
            }
            Entry::Occupied(mut entry) => {
                entry.get_mut().expires_at = verified.expires_at;
                entry.get_mut().window.admit(envelope.sequence())
            }
        };
        if !sequence_admitted {
            return Err(PublishIngressError::new(
                PublishRejectionCode::FrameReplay,
                "sequence",
            ));
        }

        Ok(PublishLease {
            binding: Arc::clone(binding),
            envelope,
            binding_revision: registered.revision,
            expires_at: verified.expires_at,
            max_datagram_bytes: verified.max_datagram_bytes,
        })
    }

    fn record_rejection(&self, error: &PublishIngressError, enforced: bool) {
        self.metrics.rejection_reasons[error.code as usize].fetch_add(1, Ordering::Relaxed);
        if enforced {
            self.metrics
                .enforced_rejections
                .fetch_add(1, Ordering::Relaxed);
        } else {
            self.metrics
                .observed_mismatches
                .fetch_add(1, Ordering::Relaxed);
        }
    }
}

const ALL_REJECTION_CODES: [PublishRejectionCode; REJECTION_CODE_COUNT] = [
    PublishRejectionCode::LegacyPath,
    PublishRejectionCode::MissingCapability,
    PublishRejectionCode::MalformedAuthorization,
    PublishRejectionCode::CapabilityTooLarge,
    PublishRejectionCode::MissingEnvelope,
    PublishRejectionCode::EnvelopeTooLarge,
    PublishRejectionCode::MalformedEnvelope,
    PublishRejectionCode::NonCanonicalEnvelope,
    PublishRejectionCode::MissingContentLength,
    PublishRejectionCode::InvalidContentLength,
    PublishRejectionCode::PayloadLengthMismatch,
    PublishRejectionCode::UnknownBinding,
    PublishRejectionCode::RevokedBinding,
    PublishRejectionCode::ConfigurationMismatch,
    PublishRejectionCode::InvalidSignature,
    PublishRejectionCode::WrongScope,
    PublishRejectionCode::CapabilityRejected,
    PublishRejectionCode::CapabilityReplay,
    PublishRejectionCode::FrameReplay,
    PublishRejectionCode::CapabilityExpired,
    PublishRejectionCode::ChannelLimit,
    PublishRejectionCode::BitrateLimit,
    PublishRejectionCode::StreamMismatch,
    PublishRejectionCode::TalkbackIsolation,
    PublishRejectionCode::DatagramLimit,
];

fn validate_talkback_isolation(
    configuration: &MediaFrameConfigurationV1,
) -> Result<(), PublishIngressError> {
    if configuration.identity().media_class() == MediaClass::Talkback
        && (configuration.capture_disposition() != MediaCaptureDisposition::MonitorOnly
            || configuration.payload_format() != MediaFramePayloadFormat::Opus
            || configuration.capture_timebase_hz() != 48_000
            || configuration.channel_count() != 1)
    {
        return Err(PublishIngressError::new(
            PublishRejectionCode::TalkbackIsolation,
            "capture_disposition",
        ));
    }
    Ok(())
}

fn map_envelope_error(error: media_object::MediaControlError) -> PublishIngressError {
    let code = match error.code() {
        MediaControlErrorCode::ConfigurationMismatch => PublishRejectionCode::ConfigurationMismatch,
        MediaControlErrorCode::LimitExceeded => PublishRejectionCode::EnvelopeTooLarge,
        _ => PublishRejectionCode::MalformedEnvelope,
    };
    PublishIngressError::new(code, error.field())
}

fn map_verifier_error(error: CapabilityVerifierError) -> PublishIngressError {
    let code = if error.claims_code() == Some(MediaControlErrorCode::Expired) {
        PublishRejectionCode::CapabilityExpired
    } else {
        match error.code() {
            CapabilityVerifierErrorCode::InvalidSignature => PublishRejectionCode::InvalidSignature,
            CapabilityVerifierErrorCode::AuthorizationRejected => PublishRejectionCode::WrongScope,
            CapabilityVerifierErrorCode::ReplayAdmissionRejected => {
                PublishRejectionCode::CapabilityReplay
            }
            CapabilityVerifierErrorCode::SegmentTooLarge => {
                PublishRejectionCode::CapabilityTooLarge
            }
            _ => PublishRejectionCode::CapabilityRejected,
        }
    };
    PublishIngressError::new(code, error.field())
}

/// Decode the bounded canonical-envelope header without allocating from attacker lengths.
pub fn decode_envelope_header(value: &str) -> Result<Vec<u8>, PublishIngressError> {
    let max_encoded = MEDIA_CONTROL_MAX_JSON_BYTES.div_ceil(3) * 4;
    if value.len() > max_encoded {
        return Err(PublishIngressError::new(
            PublishRejectionCode::EnvelopeTooLarge,
            "frame_envelope",
        ));
    }
    let decoded = URL_SAFE_NO_PAD.decode(value).map_err(|_| {
        PublishIngressError::new(PublishRejectionCode::MalformedEnvelope, "frame_envelope")
    })?;
    if decoded.len() > MEDIA_CONTROL_MAX_JSON_BYTES || URL_SAFE_NO_PAD.encode(&decoded) != value {
        return Err(PublishIngressError::new(
            PublishRejectionCode::MalformedEnvelope,
            "frame_envelope",
        ));
    }
    Ok(decoded)
}

/// Decode an optional raw HTTP frame-envelope header with fixed bounds.
pub fn decode_envelope_header_bytes(value: Option<&[u8]>) -> Result<Vec<u8>, PublishIngressError> {
    let value = value.ok_or_else(|| {
        PublishIngressError::new(PublishRejectionCode::MissingEnvelope, "frame_envelope")
    })?;
    let value = std::str::from_utf8(value).map_err(|_| {
        PublishIngressError::new(PublishRejectionCode::MalformedEnvelope, "frame_envelope")
    })?;
    decode_envelope_header(value)
}

impl PublishIngressGate {
    /// Apply migration semantics to a header/integration failure found before verification.
    pub fn handle_integration_error(
        &self,
        error: PublishIngressError,
    ) -> Result<(), PublishIngressError> {
        match self.mode {
            PublishAuthorizationMode::Off => Ok(()),
            PublishAuthorizationMode::Observe => {
                self.record_rejection(&error, false);
                Ok(())
            }
            PublishAuthorizationMode::Enforce => {
                self.record_rejection(&error, true);
                Err(error)
            }
        }
    }
}

/// Parse the exact bearer scheme from raw HTTP header bytes without echoing it.
pub fn parse_bearer_header(value: Option<&[u8]>) -> Result<&str, PublishIngressError> {
    let value = value.ok_or_else(|| {
        PublishIngressError::new(PublishRejectionCode::MissingCapability, "authorization")
    })?;
    let value = std::str::from_utf8(value).map_err(|_| {
        PublishIngressError::new(
            PublishRejectionCode::MalformedAuthorization,
            "authorization",
        )
    })?;
    let token = value.strip_prefix("Bearer ").ok_or_else(|| {
        PublishIngressError::new(
            PublishRejectionCode::MalformedAuthorization,
            "authorization",
        )
    })?;
    if token.is_empty()
        || token
            .as_bytes()
            .iter()
            .any(|byte| byte.is_ascii_whitespace())
    {
        return Err(PublishIngressError::new(
            PublishRejectionCode::MalformedAuthorization,
            "authorization",
        ));
    }
    Ok(token)
}

/// Require a single exact decimal body length before allocating the body.
pub fn parse_content_length_header(value: Option<&[u8]>) -> Result<u64, PublishIngressError> {
    let value = value.ok_or_else(|| {
        PublishIngressError::new(PublishRejectionCode::MissingContentLength, "content_length")
    })?;
    let value = std::str::from_utf8(value).map_err(|_| {
        PublishIngressError::new(PublishRejectionCode::InvalidContentLength, "content_length")
    })?;
    if value.is_empty()
        || (value.len() > 1 && value.starts_with('0'))
        || !value.bytes().all(|byte| byte.is_ascii_digit())
    {
        return Err(PublishIngressError::new(
            PublishRejectionCode::InvalidContentLength,
            "content_length",
        ));
    }
    value.parse().map_err(|_| {
        PublishIngressError::new(PublishRejectionCode::InvalidContentLength, "content_length")
    })
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct BootstrapV1 {
    version: u16,
    issuer: String,
    audience: String,
    keys: Vec<BootstrapKeyV1>,
    bindings: Vec<BootstrapBindingV1>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct BootstrapKeyV1 {
    kid: String,
    public_key_base64url: String,
    #[serde(default)]
    retiring_accept_until: Option<i64>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct BootstrapBindingV1 {
    configuration: MediaFrameConfigurationV1,
    legacy_stream_id: u64,
    media_authorization_epoch: u64,
    subject_grant_epoch: u64,
    media_policy_version: u64,
    #[serde(default)]
    class_authorization_epoch: Option<u64>,
    operation: Operation,
    edge_id: EdgeId,
    #[serde(default)]
    clock_skew_seconds: i64,
}

/// Build a gate from a bounded public-key-only local control snapshot.
pub fn gate_from_bootstrap_path(
    path: &Path,
    mode: PublishAuthorizationMode,
) -> Result<PublishIngressGate, PublishIngressError> {
    let file = std::fs::File::open(path).map_err(|_| {
        PublishIngressError::new(
            PublishRejectionCode::ConfigurationMismatch,
            "bootstrap_path",
        )
    })?;
    let mut bytes = Vec::with_capacity(MAX_PUBLISH_AUTHORIZATION_BUNDLE_BYTES.min(64 * 1024));
    file.take((MAX_PUBLISH_AUTHORIZATION_BUNDLE_BYTES + 1) as u64)
        .read_to_end(&mut bytes)
        .map_err(|_| {
            PublishIngressError::new(
                PublishRejectionCode::ConfigurationMismatch,
                "bootstrap_path",
            )
        })?;
    gate_from_bootstrap_json(&bytes, mode)
}

pub fn gate_from_bootstrap_json(
    bytes: &[u8],
    mode: PublishAuthorizationMode,
) -> Result<PublishIngressGate, PublishIngressError> {
    if bytes.len() > MAX_PUBLISH_AUTHORIZATION_BUNDLE_BYTES {
        return Err(PublishIngressError::new(
            PublishRejectionCode::ConfigurationMismatch,
            "bootstrap_json",
        ));
    }
    let bootstrap: BootstrapV1 = serde_json::from_slice(bytes).map_err(|_| {
        PublishIngressError::new(
            PublishRejectionCode::ConfigurationMismatch,
            "bootstrap_json",
        )
    })?;
    if bootstrap.version != 1
        || bootstrap.keys.is_empty()
        || bootstrap.keys.len() > MAX_BOOTSTRAP_KEYS
        || bootstrap.bindings.is_empty()
        || bootstrap.bindings.len() > MAX_BOOTSTRAP_BINDINGS
    {
        return Err(PublishIngressError::new(
            PublishRejectionCode::ConfigurationMismatch,
            "bootstrap_json",
        ));
    }
    let mut keyring = VerificationKeyring::new();
    for key in bootstrap.keys {
        let decoded = URL_SAFE_NO_PAD
            .decode(&key.public_key_base64url)
            .map_err(|_| {
                PublishIngressError::new(PublishRejectionCode::ConfigurationMismatch, "public_key")
            })?;
        let public_key: [u8; 32] = decoded.try_into().map_err(|_| {
            PublishIngressError::new(PublishRejectionCode::ConfigurationMismatch, "public_key")
        })?;
        let result = if let Some(accept_until) = key.retiring_accept_until {
            keyring.insert_retiring(key.kid, public_key, accept_until)
        } else {
            keyring.insert_active(key.kid, public_key)
        };
        result.map_err(|error| {
            PublishIngressError::new(PublishRejectionCode::ConfigurationMismatch, error.field())
        })?;
    }
    let verifier = MediaCapabilityVerifier::new(keyring, bootstrap.issuer, bootstrap.audience)
        .map_err(|error| {
            PublishIngressError::new(PublishRejectionCode::ConfigurationMismatch, error.field())
        })?;
    let registry = Arc::new(PublishBindingRegistry::new());
    for binding in bootstrap.bindings {
        registry.install(CurrentPublishBinding::new(
            binding.configuration,
            binding.legacy_stream_id,
            binding.media_authorization_epoch,
            binding.subject_grant_epoch,
            binding.media_policy_version,
            binding.class_authorization_epoch,
            binding.operation,
            binding.edge_id,
            binding.clock_skew_seconds,
        )?)?;
    }
    Ok(PublishIngressGate::new(mode, Arc::new(verifier), registry))
}
