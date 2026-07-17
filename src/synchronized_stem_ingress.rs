//! Capability-derived admission for the additive `SST1` publication path.
//!
//! Signature, current-state and replay checks remain owned by
//! `media-capability`. This adapter consumes only its unforgeable authorized
//! result, binds it to one authoritative synchronized-stems config and requires
//! AEAD authentication before exposing a symbol.

use media_capability::AuthorizedMediaCapability;
use media_object::{MediaClass, Operation};
use std::fmt;
use synchronized_stems_media::{
    open_authenticated_datagram, AuthenticatedStemSymbol, AuthoritativeStemConfig,
    AuthorizationMediaClass, AuthorizationOperation, CompositeIdentity, StemAeadOpener,
    StemAuthorization, StemErrorCode,
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum StemPublishIngressErrorCode {
    InvalidConfig,
    CapabilityScope,
    CapabilityExpired,
    ChannelLimit,
    DatagramLimit,
    Media(StemErrorCode),
}

#[derive(Clone, Eq, PartialEq)]
pub struct StemPublishIngressError {
    code: StemPublishIngressErrorCode,
    field: &'static str,
}

impl StemPublishIngressError {
    const fn new(code: StemPublishIngressErrorCode, field: &'static str) -> Self {
        Self { code, field }
    }

    #[must_use]
    pub const fn code(&self) -> StemPublishIngressErrorCode {
        self.code
    }

    #[must_use]
    pub const fn field(&self) -> &'static str {
        self.field
    }
}

impl fmt::Debug for StemPublishIngressError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("StemPublishIngressError")
            .field("code", &self.code)
            .field("field", &self.field)
            .finish()
    }
}

impl fmt::Display for StemPublishIngressError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "synchronized-stem publish rejected: {:?}",
            self.code
        )
    }
}

impl std::error::Error for StemPublishIngressError {}

struct VerifiedClaimsView {
    identity: CompositeIdentity,
    topology_generation: u64,
    binding_generation: u64,
    operation: AuthorizationOperation,
    media_class: AuthorizationMediaClass,
    source_ids: Vec<String>,
    expires_at: i64,
    max_channels: u16,
    max_datagram_bytes: u32,
}

impl VerifiedClaimsView {
    fn from_authorized(
        authorized: &AuthorizedMediaCapability,
    ) -> Result<Self, StemPublishIngressError> {
        let claims = authorized.claims();
        let contributor_id = claims.contributor_id().ok_or_else(|| {
            StemPublishIngressError::new(
                StemPublishIngressErrorCode::CapabilityScope,
                "contributorId",
            )
        })?;
        let identity = CompositeIdentity::new(
            claims.tenant_id().as_str().to_string(),
            claims.session_id().as_str().to_string(),
            claims.session_epoch(),
            contributor_id.as_str().to_string(),
        )
        .map_err(|_| {
            StemPublishIngressError::new(StemPublishIngressErrorCode::CapabilityScope, "identity")
        })?;
        Ok(Self {
            identity,
            topology_generation: claims.topology_generation(),
            binding_generation: claims.binding_generation(),
            operation: match claims.operation() {
                Operation::Publish => AuthorizationOperation::Publish,
                _ => AuthorizationOperation::Subscribe,
            },
            media_class: match claims.media_class() {
                MediaClass::Program => AuthorizationMediaClass::Program,
                _ => AuthorizationMediaClass::Talkback,
            },
            source_ids: claims
                .source_ids()
                .iter()
                .map(|source| source.as_str().to_string())
                .collect(),
            expires_at: claims.expires_at(),
            max_channels: claims.max_channels(),
            max_datagram_bytes: claims.max_datagram_bytes(),
        })
    }
}

/// Immutable, capability-bounded publication context for one exact config.
pub struct AuthoritativeStemIngress {
    config: AuthoritativeStemConfig,
    authorization: StemAuthorization,
    expires_at: i64,
    max_datagram_bytes: u32,
}

impl AuthoritativeStemIngress {
    /// Consume an already signature/current-state/replay-authorized capability.
    ///
    /// # Errors
    ///
    /// Returns a value-free error when the reliable config is invalid or its
    /// identity, generations, source/channel set or MTU exceeds verified claims.
    pub fn from_authorized(
        config_json: &[u8],
        authorized: AuthorizedMediaCapability,
    ) -> Result<Self, StemPublishIngressError> {
        let claims = VerifiedClaimsView::from_authorized(&authorized)?;
        Self::from_verified_claims(config_json, claims)
    }

    fn from_verified_claims(
        config_json: &[u8],
        claims: VerifiedClaimsView,
    ) -> Result<Self, StemPublishIngressError> {
        let config = AuthoritativeStemConfig::from_json(config_json).map_err(|_| {
            StemPublishIngressError::new(
                StemPublishIngressErrorCode::InvalidConfig,
                "stemStreamConfig",
            )
        })?;
        if claims.identity != *config.identity()
            || claims.topology_generation != config.topology_generation()
            || claims.binding_generation != config.binding_generation()
            || claims.operation != AuthorizationOperation::Publish
            || claims.media_class != AuthorizationMediaClass::Program
        {
            return Err(StemPublishIngressError::new(
                StemPublishIngressErrorCode::CapabilityScope,
                "capability",
            ));
        }
        let admitted_channels = config
            .expected_sources()
            .iter()
            .try_fold(0u32, |sum, source| {
                sum.checked_add(u32::from(source.channel_layout().channel_count()))
            })
            .ok_or_else(|| {
                StemPublishIngressError::new(
                    StemPublishIngressErrorCode::ChannelLimit,
                    "maxChannels",
                )
            })?;
        if admitted_channels > u32::from(claims.max_channels) {
            return Err(StemPublishIngressError::new(
                StemPublishIngressErrorCode::ChannelLimit,
                "maxChannels",
            ));
        }
        if u32::from(config.carrier().max_datagram_bytes()) > claims.max_datagram_bytes {
            return Err(StemPublishIngressError::new(
                StemPublishIngressErrorCode::DatagramLimit,
                "maxDatagramBytes",
            ));
        }
        let authorization = StemAuthorization::new(
            claims.identity,
            claims.topology_generation,
            claims.binding_generation,
            claims.operation,
            claims.media_class,
            claims.source_ids,
        );
        Ok(Self {
            config,
            authorization,
            expires_at: claims.expires_at,
            max_datagram_bytes: claims.max_datagram_bytes,
        })
    }

    #[must_use]
    pub const fn config(&self) -> &AuthoritativeStemConfig {
        &self.config
    }

    /// Validate the capability lifetime/size, then bind and AEAD-open one symbol.
    ///
    /// # Errors
    ///
    /// Returns a closed error for expiry, size, binding, scope or AEAD failure.
    pub fn open(
        &self,
        datagram: &[u8],
        now_unix_seconds: i64,
        opener: &impl StemAeadOpener,
    ) -> Result<AuthenticatedStemSymbol, StemPublishIngressError> {
        if now_unix_seconds >= self.expires_at {
            return Err(StemPublishIngressError::new(
                StemPublishIngressErrorCode::CapabilityExpired,
                "expiresAt",
            ));
        }
        if datagram.len() > usize::try_from(self.max_datagram_bytes).unwrap_or(usize::MAX) {
            return Err(StemPublishIngressError::new(
                StemPublishIngressErrorCode::DatagramLimit,
                "maxDatagramBytes",
            ));
        }
        open_authenticated_datagram(datagram, &self.config, &self.authorization, opener).map_err(
            |error| {
                StemPublishIngressError::new(
                    StemPublishIngressErrorCode::Media(error.code()),
                    error.field(),
                )
            },
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use synchronized_stems_media::{
        AeadOpenError, StemDatagramHeader, SST1_AEAD_TAG_BYTES, SST1_HEADER_BYTES,
    };

    const CONFIG_JSON: &[u8] = include_bytes!(
        "../../av-api/synchronized-stems-media/tests/fixtures/stem-stream-config.json"
    );

    #[derive(Clone, Copy)]
    struct TestAead {
        accept: bool,
    }

    impl StemAeadOpener for TestAead {
        fn open(
            &self,
            key_epoch: u32,
            associated_data: &[u8; SST1_HEADER_BYTES],
            ciphertext: &[u8],
            tag: &[u8; SST1_AEAD_TAG_BYTES],
        ) -> Result<Vec<u8>, AeadOpenError> {
            if self.accept
                && key_epoch == 3
                && associated_data[..4] == *b"SST1"
                && tag.iter().all(|byte| *byte == 0xa5)
            {
                Ok(ciphertext.to_vec())
            } else {
                Err(AeadOpenError)
            }
        }
    }

    fn claims() -> VerifiedClaimsView {
        VerifiedClaimsView {
            identity: CompositeIdentity::new(
                "ten_demo".to_string(),
                "ses_demo".to_string(),
                9,
                "con_demo".to_string(),
            )
            .unwrap(),
            topology_generation: 11,
            binding_generation: 13,
            operation: AuthorizationOperation::Publish,
            media_class: AuthorizationMediaClass::Program,
            source_ids: vec!["src_guitar".to_string(), "src_vocal".to_string()],
            expires_at: 200,
            max_channels: 2,
            max_datagram_bytes: 1_200,
        }
    }

    fn decode_hex(value: &str) -> Vec<u8> {
        value
            .as_bytes()
            .chunks_exact(2)
            .map(|pair| {
                let digit = |byte: u8| match byte {
                    b'0'..=b'9' => byte - b'0',
                    b'a'..=b'f' => byte - b'a' + 10,
                    _ => panic!("invalid hex"),
                };
                digit(pair[0]) * 16 + digit(pair[1])
            })
            .collect()
    }

    fn datagram() -> Vec<u8> {
        let header = decode_hex(
            "5353543101000100000000000000000b000000000000000d0000002900000000000000070000000400000003000100000008000200000000000000640000000000005dc00000000000000064000000f002d00334",
        );
        assert_eq!(
            StemDatagramHeader::decode(&header)
                .unwrap()
                .datagram_byte_count,
            820
        );
        let mut value = header;
        value.extend(std::iter::repeat_n(0x5c, 720));
        value.extend(std::iter::repeat_n(0xa5, SST1_AEAD_TAG_BYTES));
        value
    }

    #[test]
    fn reliable_config_is_bound_to_exact_capability_limits() {
        let admitted = AuthoritativeStemIngress::from_verified_claims(CONFIG_JSON, claims())
            .expect("exact verified claims admit the config");
        assert_eq!(admitted.config().sources().len(), 2);

        let mut wrong_identity = claims();
        wrong_identity.identity = CompositeIdentity::new(
            "ten_demo".to_string(),
            "ses_other".to_string(),
            9,
            "con_demo".to_string(),
        )
        .unwrap();
        assert_eq!(
            AuthoritativeStemIngress::from_verified_claims(CONFIG_JSON, wrong_identity)
                .err()
                .unwrap()
                .code(),
            StemPublishIngressErrorCode::CapabilityScope
        );

        let mut wrong_generation = claims();
        wrong_generation.binding_generation = 12;
        assert_eq!(
            AuthoritativeStemIngress::from_verified_claims(CONFIG_JSON, wrong_generation)
                .err()
                .unwrap()
                .code(),
            StemPublishIngressErrorCode::CapabilityScope
        );

        let mut wrong_operation = claims();
        wrong_operation.operation = AuthorizationOperation::Subscribe;
        assert_eq!(
            AuthoritativeStemIngress::from_verified_claims(CONFIG_JSON, wrong_operation)
                .err()
                .unwrap()
                .code(),
            StemPublishIngressErrorCode::CapabilityScope
        );

        let mut too_many_channels = claims();
        too_many_channels.max_channels = 1;
        assert_eq!(
            AuthoritativeStemIngress::from_verified_claims(CONFIG_JSON, too_many_channels)
                .err()
                .unwrap()
                .code(),
            StemPublishIngressErrorCode::ChannelLimit
        );

        let mut mtu_too_small = claims();
        mtu_too_small.max_datagram_bytes = 1_199;
        assert_eq!(
            AuthoritativeStemIngress::from_verified_claims(CONFIG_JSON, mtu_too_small)
                .err()
                .unwrap()
                .code(),
            StemPublishIngressErrorCode::DatagramLimit
        );
    }

    #[test]
    fn payload_is_exposed_only_after_live_scope_and_aead_succeed() {
        let ingress =
            AuthoritativeStemIngress::from_verified_claims(CONFIG_JSON, claims()).unwrap();
        let input = datagram();
        let opened = ingress
            .open(&input, 199, &TestAead { accept: true })
            .unwrap();
        assert_eq!(opened.source_id(), "src_vocal");
        assert_eq!(opened.payload(), vec![0x5c; 720]);

        assert_eq!(
            ingress
                .open(&input, 200, &TestAead { accept: true })
                .unwrap_err()
                .code(),
            StemPublishIngressErrorCode::CapabilityExpired
        );
        assert_eq!(
            ingress
                .open(&input, 199, &TestAead { accept: false })
                .unwrap_err()
                .code(),
            StemPublishIngressErrorCode::Media(StemErrorCode::AuthenticationFailed)
        );

        let oversized = vec![0; 1_201];
        assert_eq!(
            ingress
                .open(&oversized, 199, &TestAead { accept: true })
                .unwrap_err()
                .code(),
            StemPublishIngressErrorCode::DatagramLimit
        );

        let mut guitar_only = claims();
        guitar_only.source_ids = vec!["src_guitar".to_string()];
        let restricted =
            AuthoritativeStemIngress::from_verified_claims(CONFIG_JSON, guitar_only).unwrap();
        assert_eq!(
            restricted
                .open(&input, 199, &TestAead { accept: true })
                .unwrap_err()
                .code(),
            StemPublishIngressErrorCode::Media(StemErrorCode::SourceNotAuthorized)
        );
    }
}
