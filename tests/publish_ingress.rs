use std::sync::Arc;

use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use base64::Engine as _;
use ed25519_dalek::{Signer as _, SigningKey};
use media_capability::{
    EdgeId, MediaCapabilityVerifier, VerificationKeyring, PROTECTED_ALGORITHM, PROTECTED_TOKEN_TYPE,
};
use media_object::{
    AudienceId, CapabilityId, ContributorId, EndpointId, MediaCapabilityClaimsV1,
    MediaCapabilityClaimsV1Params, MediaCaptureDisposition, MediaClass, MediaConfigurationId,
    MediaFrameConfigurationV1, MediaFrameConfigurationV1Params, MediaFrameEnvelopeV1,
    MediaFrameEnvelopeV1Params, MediaFramePayloadFormat, Operation, ParticipantId, SessionId,
    SessionMediaIdentityV1, SessionMediaIdentityV1Params, SourceId, TenantId,
    MEDIA_CONTROL_MAX_JSON_BYTES,
};

use av_contrib::ingress_authorization::{
    decode_envelope_header, gate_from_bootstrap_json, parse_bearer_header,
    parse_content_length_header, CurrentPublishBinding, PublishAdmission, PublishAuthorizationMode,
    PublishBindingRegistry, PublishIngressError, PublishIngressGate, PublishIngressRequest,
    PublishRejectionCode,
};

const NOW: i64 = 1_784_131_220;
const ISSUER: &str = "https://control.infidelity.io";
const AUDIENCE: &str = "av-contrib";
const KID: &str = "key_active_01";

struct Fixture {
    gate: PublishIngressGate,
    registry: Arc<PublishBindingRegistry>,
    signing_key: SigningKey,
}

fn configuration(configuration_ref: u32) -> MediaFrameConfigurationV1 {
    let identity = SessionMediaIdentityV1::new(SessionMediaIdentityV1Params {
        tenant_id: TenantId::new("ten_wavey").unwrap(),
        session_id: SessionId::new("ses_mix").unwrap(),
        session_epoch: 9,
        participant_id: ParticipantId::new("par_producer").unwrap(),
        endpoint_id: EndpointId::new("ep_logic").unwrap(),
        contributor_id: ContributorId::new("con_logic").unwrap(),
        source_id: Some(SourceId::new("src_mix").unwrap()),
        media_class: MediaClass::Source,
        audience_id: None,
        take_id: None,
        topology_generation: 52,
    })
    .unwrap();
    MediaFrameConfigurationV1::new(MediaFrameConfigurationV1Params {
        configuration_id: MediaConfigurationId::new(format!("cfg_{configuration_ref}")).unwrap(),
        binding_generation: 8,
        configuration_ref,
        configuration_epoch: 11,
        identity,
        payload_format: MediaFramePayloadFormat::Opus,
        capture_timebase_hz: 48_000,
        channel_count: 2,
        max_payload_bytes: 4_096,
        capture_disposition: MediaCaptureDisposition::Recordable,
    })
    .unwrap()
}

fn binding(configuration: MediaFrameConfigurationV1) -> CurrentPublishBinding {
    CurrentPublishBinding::new(
        configuration,
        77,
        14,
        3,
        7,
        Some(4),
        Operation::Publish,
        EdgeId::new("edge_lon").unwrap(),
        0,
    )
    .unwrap()
}

fn new_fixture(mode: PublishAuthorizationMode) -> Fixture {
    let signing_key = SigningKey::from_bytes(&[7; 32]);
    let mut keyring = VerificationKeyring::new();
    keyring
        .insert_active(KID, signing_key.verifying_key().to_bytes())
        .unwrap();
    let verifier = MediaCapabilityVerifier::new(keyring, ISSUER, AUDIENCE).unwrap();
    let registry = Arc::new(PublishBindingRegistry::new());
    registry.install(binding(configuration(1))).unwrap();
    Fixture {
        gate: PublishIngressGate::new(mode, Arc::new(verifier), Arc::clone(&registry)),
        registry,
        signing_key,
    }
}

fn claims_params(capability_id: &str) -> MediaCapabilityClaimsV1Params {
    MediaCapabilityClaimsV1Params {
        issuer: ISSUER.to_owned(),
        audience: AUDIENCE.to_owned(),
        capability_id: CapabilityId::new(capability_id).unwrap(),
        tenant_id: TenantId::new("ten_wavey").unwrap(),
        session_id: SessionId::new("ses_mix").unwrap(),
        session_epoch: 9,
        media_authorization_epoch: 14,
        subject_grant_epoch: 3,
        media_policy_version: 7,
        class_authorization_epoch: Some(4),
        binding_generation: 8,
        participant_id: ParticipantId::new("par_producer").unwrap(),
        endpoint_id: EndpointId::new("ep_logic").unwrap(),
        contributor_id: Some(ContributorId::new("con_logic").unwrap()),
        operation: Operation::Publish,
        media_class: MediaClass::Source,
        source_ids: vec![SourceId::new("src_mix").unwrap()],
        audience_ids: Vec::new(),
        take_id: None,
        topology_generation: 52,
        edge_ids: vec![EdgeId::new("edge_lon").unwrap()],
        max_channels: 2,
        max_bitrate: 512_000,
        max_datagram_bytes: 1_200,
        client_key_thumbprint: None,
        issued_at: NOW - 20,
        not_before: NOW - 20,
        expires_at: NOW + 40,
    }
}

fn sign(signing_key: &SigningKey, params: MediaCapabilityClaimsV1Params) -> String {
    let header = format!(
        r#"{{"alg":"{PROTECTED_ALGORITHM}","kid":"{KID}","typ":"{PROTECTED_TOKEN_TYPE}"}}"#
    );
    let claims = MediaCapabilityClaimsV1::new(params)
        .unwrap()
        .to_canonical_json_vec()
        .unwrap();
    let protected = URL_SAFE_NO_PAD.encode(header.as_bytes());
    let claims = URL_SAFE_NO_PAD.encode(claims);
    let input = format!("{protected}.{claims}");
    let signature = URL_SAFE_NO_PAD.encode(signing_key.sign(input.as_bytes()).to_bytes());
    format!("{input}.{signature}")
}

fn envelope(configuration_ref: u32, sequence: u64) -> Vec<u8> {
    MediaFrameEnvelopeV1::new(MediaFrameEnvelopeV1Params {
        binding_generation: 8,
        configuration_ref,
        configuration_epoch: 11,
        sequence,
        capture_pts: 48_000,
        duration_ticks: 960,
        payload_bytes: 480,
    })
    .unwrap()
    .to_canonical_json_vec()
    .unwrap()
}

fn authorize(
    fixture: &Fixture,
    token: &str,
    envelope: &[u8],
    now: i64,
) -> Result<PublishAdmission, PublishIngressError> {
    fixture.gate.authorize(&PublishIngressRequest {
        compact_jws: token,
        envelope_json: envelope,
        content_length: 480,
        legacy_stream_id: 77,
        now_unix_seconds: now,
    })
}

fn rejected(result: Result<PublishAdmission, PublishIngressError>) -> PublishIngressError {
    match result {
        Ok(_) => panic!("request unexpectedly admitted"),
        Err(error) => error,
    }
}

#[test]
fn exact_signed_scope_admits_before_body_and_sequence_replay_is_rejected() {
    let fixture = new_fixture(PublishAuthorizationMode::Enforce);
    let token = sign(&fixture.signing_key, claims_params("cap_exact"));
    let frame = envelope(1, 9);
    let admission = authorize(&fixture, &token, &frame, NOW).unwrap();
    let lease = admission.lease().unwrap();
    assert_eq!(lease.envelope().sequence(), 9);
    assert_eq!(lease.identity().source_id().unwrap().as_str(), "src_mix");
    assert_eq!(lease.max_datagram_bytes(), 1_200);
    let payload = vec![0x5a; 480];
    let wire = media_object::encode(&lease.canonical_media_object(&payload).unwrap()).unwrap();
    let object = media_object::decode(&wire).unwrap();
    assert_eq!(object.key().tenant(), "ten_wavey");
    assert_eq!(object.key().stream(), "77");
    assert_eq!(object.key().track(), "cfg_1");
    assert_eq!(object.key().epoch(), 8);
    assert_eq!(object.key().group(), 1);
    assert_eq!(object.key().object(), 9);
    assert_eq!(object.configuration_epoch(), 11);
    assert_eq!(object.payload(), payload);
    assert_eq!(
        object.deadline().unwrap().unix_time_ns(),
        (NOW + 40) * 1_000_000_000
    );
    let wire_configuration = MediaFrameConfigurationV1::from_json_slice(
        object
            .metadata()
            .get("media-frame-configuration-v1")
            .unwrap(),
    )
    .unwrap();
    let wire_envelope = MediaFrameEnvelopeV1::from_json_slice(
        object.metadata().get("media-frame-envelope-v1").unwrap(),
    )
    .unwrap();
    assert_eq!(wire_configuration.binding_generation(), 8);
    assert_eq!(wire_configuration.configuration_ref(), 1);
    assert_eq!(wire_configuration.configuration_epoch(), 11);
    assert_eq!(wire_configuration.identity(), lease.identity());
    assert_eq!(wire_envelope, *lease.envelope());
    assert_eq!(
        rejected(authorize(&fixture, &token, &frame, NOW)).code(),
        PublishRejectionCode::FrameReplay
    );
}

#[test]
fn a_numeric_carrier_stream_cannot_alias_a_different_canonical_identity() {
    let fixture = new_fixture(PublishAuthorizationMode::Enforce);
    let identity = SessionMediaIdentityV1::new(SessionMediaIdentityV1Params {
        tenant_id: TenantId::new("ten_wavey").unwrap(),
        session_id: SessionId::new("ses_other").unwrap(),
        session_epoch: 1,
        participant_id: ParticipantId::new("par_other").unwrap(),
        endpoint_id: EndpointId::new("ep_other").unwrap(),
        contributor_id: ContributorId::new("con_other").unwrap(),
        source_id: Some(SourceId::new("src_other").unwrap()),
        media_class: MediaClass::Source,
        audience_id: None,
        take_id: None,
        topology_generation: 1,
    })
    .unwrap();
    let other = MediaFrameConfigurationV1::new(MediaFrameConfigurationV1Params {
        configuration_id: MediaConfigurationId::new("cfg_other").unwrap(),
        binding_generation: 9,
        configuration_ref: 2,
        configuration_epoch: 1,
        identity,
        payload_format: MediaFramePayloadFormat::Opus,
        capture_timebase_hz: 48_000,
        channel_count: 1,
        max_payload_bytes: 4_096,
        capture_disposition: MediaCaptureDisposition::Recordable,
    })
    .unwrap();
    let error = fixture
        .registry
        .install(
            CurrentPublishBinding::new(
                other,
                77,
                1,
                1,
                1,
                None,
                Operation::Publish,
                EdgeId::new("edge_lon").unwrap(),
                0,
            )
            .unwrap(),
        )
        .unwrap_err();
    assert_eq!(error.code(), PublishRejectionCode::StreamMismatch);
    assert_eq!(error.field(), "legacy_stream_id");
}

#[test]
fn every_current_identity_epoch_generation_and_scope_is_exact() {
    let fixture = new_fixture(PublishAuthorizationMode::Enforce);
    let cases: Vec<(&str, MediaCapabilityClaimsV1Params)> = vec![
        ("tenant_id", {
            let mut p = claims_params("c_tenant");
            p.tenant_id = TenantId::new("ten_other").unwrap();
            p
        }),
        ("session_id", {
            let mut p = claims_params("c_session");
            p.session_id = SessionId::new("ses_other").unwrap();
            p
        }),
        ("session_epoch", {
            let mut p = claims_params("c_session_epoch");
            p.session_epoch += 1;
            p
        }),
        ("media_authorization_epoch", {
            let mut p = claims_params("c_auth_epoch");
            p.media_authorization_epoch += 1;
            p
        }),
        ("subject_grant_epoch", {
            let mut p = claims_params("c_grant_epoch");
            p.subject_grant_epoch += 1;
            p
        }),
        ("media_policy_version", {
            let mut p = claims_params("c_policy");
            p.media_policy_version += 1;
            p
        }),
        ("class_authorization_epoch", {
            let mut p = claims_params("c_class_epoch");
            p.class_authorization_epoch = Some(5);
            p
        }),
        ("binding_generation", {
            let mut p = claims_params("c_binding");
            p.binding_generation += 1;
            p
        }),
        ("topology_generation", {
            let mut p = claims_params("c_topology");
            p.topology_generation += 1;
            p
        }),
        ("participant_id", {
            let mut p = claims_params("c_participant");
            p.participant_id = ParticipantId::new("par_other").unwrap();
            p
        }),
        ("endpoint_id", {
            let mut p = claims_params("c_endpoint");
            p.endpoint_id = EndpointId::new("ep_other").unwrap();
            p
        }),
        ("contributor_id", {
            let mut p = claims_params("c_contributor");
            p.contributor_id = Some(ContributorId::new("con_other").unwrap());
            p
        }),
        ("operation", {
            let mut p = claims_params("c_operation");
            p.operation = Operation::Subscribe;
            p
        }),
        ("media_class", {
            let mut p = claims_params("c_class");
            p.media_class = MediaClass::Program;
            p
        }),
        ("source_ids", {
            let mut p = claims_params("c_source");
            p.source_ids = vec![SourceId::new("src_other").unwrap()];
            p
        }),
        ("edge_ids", {
            let mut p = claims_params("c_edge");
            p.edge_ids = vec![EdgeId::new("edge_other").unwrap()];
            p
        }),
    ];
    for (field, params) in cases {
        let token = sign(&fixture.signing_key, params);
        let error = rejected(authorize(&fixture, &token, &envelope(1, 1), NOW));
        assert_eq!(error.code(), PublishRejectionCode::WrongScope, "{field}");
        assert_eq!(error.field(), field);
    }
}

#[test]
fn signature_expiry_revocation_and_cross_binding_replay_fail_closed() {
    let fixture = new_fixture(PublishAuthorizationMode::Enforce);
    let attacker = SigningKey::from_bytes(&[11; 32]);
    let forged = sign(&attacker, claims_params("cap_forged"));
    assert_eq!(
        rejected(authorize(&fixture, &forged, &envelope(1, 1), NOW)).code(),
        PublishRejectionCode::InvalidSignature
    );

    let mut expired = claims_params("cap_expired");
    expired.expires_at = NOW;
    let expired = sign(&fixture.signing_key, expired);
    assert_eq!(
        rejected(authorize(&fixture, &expired, &envelope(1, 2), NOW)).code(),
        PublishRejectionCode::CapabilityExpired
    );

    let token = sign(&fixture.signing_key, claims_params("cap_revoke"));
    let admission = authorize(&fixture, &token, &envelope(1, 3), NOW).unwrap();
    assert!(fixture.registry.invalidate(8, 1, 11));
    assert_eq!(
        fixture
            .gate
            .revalidate_before_forward(admission.lease().unwrap(), 480, NOW)
            .unwrap_err()
            .code(),
        PublishRejectionCode::RevokedBinding
    );

    let replay = new_fixture(PublishAuthorizationMode::Enforce);
    replay.registry.install(binding(configuration(2))).unwrap();
    let token = sign(&replay.signing_key, claims_params("cap_cross_binding"));
    authorize(&replay, &token, &envelope(1, 4), NOW).unwrap();
    assert_eq!(
        rejected(authorize(&replay, &token, &envelope(2, 5), NOW)).code(),
        PublishRejectionCode::CapabilityReplay
    );
}

#[test]
fn bounded_canonical_headers_dark_mode_and_limits_are_deterministic() {
    assert_eq!(
        decode_envelope_header(&"A".repeat(MEDIA_CONTROL_MAX_JSON_BYTES * 2))
            .unwrap_err()
            .code(),
        PublishRejectionCode::EnvelopeTooLarge
    );
    let padded = format!("{}=", URL_SAFE_NO_PAD.encode(b"{}"));
    assert_eq!(
        decode_envelope_header(&padded).unwrap_err().code(),
        PublishRejectionCode::MalformedEnvelope
    );
    assert_eq!(
        parse_bearer_header(Some(b"Basic abc")).unwrap_err().code(),
        PublishRejectionCode::MalformedAuthorization
    );
    assert_eq!(
        parse_content_length_header(Some(b"0480"))
            .unwrap_err()
            .code(),
        PublishRejectionCode::InvalidContentLength
    );

    let fixture = new_fixture(PublishAuthorizationMode::Enforce);
    let token = sign(&fixture.signing_key, claims_params("cap_noncanonical"));
    let mut noncanonical = envelope(1, 6);
    noncanonical.pop();
    assert_eq!(
        rejected(authorize(&fixture, &token, &noncanonical, NOW)).code(),
        PublishRejectionCode::NonCanonicalEnvelope
    );
    let length_error = fixture.gate.authorize(&PublishIngressRequest {
        compact_jws: &token,
        envelope_json: &envelope(1, 7),
        content_length: 479,
        legacy_stream_id: 77,
        now_unix_seconds: NOW,
    });
    assert_eq!(
        rejected(length_error).code(),
        PublishRejectionCode::PayloadLengthMismatch
    );

    let dark = new_fixture(PublishAuthorizationMode::Observe);
    let observed = authorize(&dark, "not-a-jws", &envelope(1, 8), NOW).unwrap();
    assert!(observed.lease().is_none());
    assert_eq!(
        observed.observed_rejection().unwrap().code(),
        PublishRejectionCode::CapabilityRejected
    );
    assert!(dark
        .gate
        .prometheus_metrics()
        .contains("decision=\"mismatch\""));

    let limits = new_fixture(PublishAuthorizationMode::Enforce);
    let mut low_bitrate = claims_params("cap_low_bitrate");
    low_bitrate.max_bitrate = 100_000;
    let token = sign(&limits.signing_key, low_bitrate);
    assert_eq!(
        rejected(authorize(&limits, &token, &envelope(1, 9), NOW)).code(),
        PublishRejectionCode::BitrateLimit
    );
}

#[test]
fn old_sequences_and_raw_legacy_ingress_are_fenced_in_enforce_mode() {
    let fixture = new_fixture(PublishAuthorizationMode::Enforce);
    let token = sign(&fixture.signing_key, claims_params("cap_window"));
    authorize(&fixture, &token, &envelope(1, 200), NOW).unwrap();
    authorize(&fixture, &token, &envelope(1, 328), NOW).unwrap();
    assert_eq!(
        rejected(authorize(&fixture, &token, &envelope(1, 199), NOW)).code(),
        PublishRejectionCode::FrameReplay
    );
    assert_eq!(
        fixture.gate.authorize_legacy_path().unwrap_err().code(),
        PublishRejectionCode::LegacyPath
    );
}

#[test]
fn talkback_is_an_exact_monitor_only_audience_lane() {
    let fixture = new_fixture(PublishAuthorizationMode::Enforce);
    let identity = SessionMediaIdentityV1::new(SessionMediaIdentityV1Params {
        tenant_id: TenantId::new("ten_wavey").unwrap(),
        session_id: SessionId::new("ses_mix").unwrap(),
        session_epoch: 9,
        participant_id: ParticipantId::new("par_producer").unwrap(),
        endpoint_id: EndpointId::new("ep_logic").unwrap(),
        contributor_id: ContributorId::new("con_logic").unwrap(),
        source_id: None,
        media_class: MediaClass::Talkback,
        audience_id: Some(AudienceId::new("aud_producer_return").unwrap()),
        take_id: None,
        topology_generation: 52,
    })
    .unwrap();
    let configuration = MediaFrameConfigurationV1::new(MediaFrameConfigurationV1Params {
        configuration_id: MediaConfigurationId::new("cfg_talkback").unwrap(),
        binding_generation: 8,
        configuration_ref: 3,
        configuration_epoch: 11,
        identity,
        payload_format: MediaFramePayloadFormat::Opus,
        capture_timebase_hz: 48_000,
        channel_count: 1,
        max_payload_bytes: 4_096,
        capture_disposition: MediaCaptureDisposition::MonitorOnly,
    })
    .unwrap();
    fixture
        .registry
        .install(
            CurrentPublishBinding::new(
                configuration,
                88,
                14,
                3,
                7,
                Some(4),
                Operation::Publish,
                EdgeId::new("edge_lon").unwrap(),
                0,
            )
            .unwrap(),
        )
        .unwrap();
    let mut claims = claims_params("cap_talkback");
    claims.media_class = MediaClass::Talkback;
    claims.source_ids.clear();
    claims.audience_ids = vec![AudienceId::new("aud_producer_return").unwrap()];
    claims.max_channels = 1;
    let token = sign(&fixture.signing_key, claims);
    let frame = envelope(3, 1);
    let admission = fixture
        .gate
        .authorize(&PublishIngressRequest {
            compact_jws: &token,
            envelope_json: &frame,
            content_length: 480,
            legacy_stream_id: 88,
            now_unix_seconds: NOW,
        })
        .unwrap();
    let lease = admission.lease().unwrap();
    assert_eq!(lease.identity().media_class(), MediaClass::Talkback);
    assert_eq!(
        lease.configuration().capture_disposition(),
        MediaCaptureDisposition::MonitorOnly
    );

    let mut wrong_audience = claims_params("cap_wrong_audience");
    wrong_audience.media_class = MediaClass::Talkback;
    wrong_audience.source_ids.clear();
    wrong_audience.audience_ids = vec![AudienceId::new("aud_other").unwrap()];
    wrong_audience.max_channels = 1;
    let token = sign(&fixture.signing_key, wrong_audience);
    let error = rejected(fixture.gate.authorize(&PublishIngressRequest {
        compact_jws: &token,
        envelope_json: &envelope(3, 2),
        content_length: 480,
        legacy_stream_id: 88,
        now_unix_seconds: NOW,
    }));
    assert_eq!(error.code(), PublishRejectionCode::WrongScope);
    assert_eq!(error.field(), "audience_ids");
}

#[test]
fn public_only_bootstrap_installs_shared_verifier_and_frozen_binding() {
    let signing_key = SigningKey::from_bytes(&[7; 32]);
    let bundle = serde_json::json!({
        "version": 1,
        "issuer": ISSUER,
        "audience": AUDIENCE,
        "keys": [{
            "kid": KID,
            "public_key_base64url": URL_SAFE_NO_PAD.encode(signing_key.verifying_key().to_bytes()),
            "retiring_accept_until": null
        }],
        "bindings": [{
            "configuration": configuration(1),
            "legacy_stream_id": 77,
            "media_authorization_epoch": 14,
            "subject_grant_epoch": 3,
            "media_policy_version": 7,
            "class_authorization_epoch": 4,
            "operation": "publish",
            "edge_id": "edge_lon",
            "clock_skew_seconds": 0
        }]
    });
    let gate = gate_from_bootstrap_json(
        &serde_json::to_vec(&bundle).unwrap(),
        PublishAuthorizationMode::Enforce,
    )
    .unwrap();
    let token = sign(&signing_key, claims_params("cap_bootstrap"));
    let frame = envelope(1, 1);
    assert!(gate
        .authorize(&PublishIngressRequest {
            compact_jws: &token,
            envelope_json: &frame,
            content_length: 480,
            legacy_stream_id: 77,
            now_unix_seconds: NOW,
        })
        .unwrap()
        .lease()
        .is_some());
}
