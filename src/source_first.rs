//! Source-first RelaySession encoding (wave 5 W1/W3, `--relay-source-first`).
//!
//! `relay_session::RaptorQObjectEncoder` builds an object's source and
//! repair symbols together: `raptorq::Encoder::new` generates the RaptorQ
//! encoding plan for the block's symbol count on every call (it caches it only
//! within one `Encoder`) and solves the intermediate symbols before the first
//! source symbol can leave. For a one-symbol audio part that is about 200 µs
//! of the contributor's `packaged` -> `sent` (most of it), even when the part
//! has no repair symbol at all; for a 4K part it is the plan of hundreds of
//! symbols, every frame.
//!
//! RaptorQ is systematic: source symbol `i` of a one-block object is bytes
//! `i*T..(i+1)*T` of the envelope, zero padded. [`SourceFirstEncoder`] builds
//! the source datagrams straight from the envelope, so they can be sent at
//! once, and computes the repair symbols afterwards ([`SourcePart::repair`])
//! with the encoding plan from `raptorq`'s process-wide plan cache. The
//! datagrams are byte for byte those of `RaptorQObjectEncoder::encode_object`
//! for the same block id and packet sequence (checked in the tests below);
//! the encoder's counters are atomics, so publishing threads share it
//! without a lock.

use std::sync::atomic::{AtomicU32, Ordering};

use bytes::Bytes;
use media_object::{MediaObject, ObjectKey, ObjectKind};
use raptorq::{EncodingPacket, ObjectTransmissionInformation, PayloadId, SourceBlockEncoder};
use raptorq_datagram_fec::{source_symbol_count, DatagramFecHeader, HEADER_LEN};
use relay_session::{
    decode_datagram, encode_datagram, priority_for_object, AdaptiveFecController, CodingProfile,
    Error, MediaDatagramRole, MediaDeadline, MediaObjectKind, MediaPathIntent, MediaPriority,
    ObjectAnnouncement, RelayDatagram, RelayLimits, Result, SubscriptionId, TopologyGeneration,
};

/// RelaySession v1 overhead of one datagram, without the identifier bytes
/// (it includes RQD2's 36 bytes); relay-session's own constant.
const RELAY_DATAGRAM_OVERHEAD: usize = 140;
/// RQD2 header and RaptorQ payload id.
const FEC_DATAGRAM_OVERHEAD: usize = 36;

/// Encodes canonical media objects into RelaySession symbols, source first.
#[derive(Debug)]
pub struct SourceFirstEncoder {
    controller: AdaptiveFecController,
    limits: RelayLimits,
    /// The configured symbol size of non-audio objects, in place of the
    /// controller's (`--relay-video-symbol-size`; `None` keeps it).
    video_symbol_size: Option<u16>,
    block_id: AtomicU32,
    packet_sequence: AtomicU32,
}

impl SourceFirstEncoder {
    /// An encoder with `controller`'s policy, whose block ids and packet
    /// sequences start at 0 as `DatagramFecEncoder::new`'s do.
    ///
    /// # Errors
    ///
    /// Returns an error for invalid relay limits.
    pub fn new(controller: AdaptiveFecController, limits: RelayLimits) -> Result<Self> {
        limits.validate()?;
        Ok(Self {
            controller,
            limits,
            video_symbol_size: None,
            block_id: AtomicU32::new(0),
            packet_sequence: AtomicU32::new(0),
        })
    }

    /// Non-audio objects use symbols of at most `symbol_size` bytes (still
    /// within the carrier budget). `0` keeps the controller's size.
    #[must_use]
    pub fn with_video_symbol_size(mut self, symbol_size: u16) -> Self {
        self.video_symbol_size = (symbol_size > 0).then_some(symbol_size);
        self
    }

    /// Continue from these counters (tests compare with an encoder that has
    /// already encoded objects).
    #[must_use]
    pub fn with_counters(self, block_id: u32, packet_sequence: u32) -> Self {
        self.block_id.store(block_id, Ordering::Relaxed);
        self.packet_sequence
            .store(packet_sequence, Ordering::Relaxed);
        self
    }

    pub const fn controller(&self) -> &AdaptiveFecController {
        &self.controller
    }

    /// The source symbols of `object`, ready to send, and what its repair
    /// symbols need. The object's block id and its packet sequences (source
    /// and repair) are taken here.
    ///
    /// # Errors
    ///
    /// The errors of `RaptorQObjectEncoder::encode_object`: an invalid
    /// object, an object or repair count over the limits, or a symbol that
    /// does not fit the carrier.
    pub fn encode_sources(
        &self,
        object: &MediaObject,
        generation: TopologyGeneration,
        subscription_id: SubscriptionId,
        deadline: MediaDeadline,
    ) -> Result<SourcePart> {
        let priority = priority_for_object(object);
        // `encode` validates the object (its payload hash among the rest).
        let envelope = media_object::encode(object)?;
        if envelope.len() > self.limits.max_protected_object_bytes {
            return Err(Error::LimitExceeded {
                field: "protected_object",
                actual: envelope.len(),
                maximum: self.limits.max_protected_object_bytes,
            });
        }
        let decided = self.controller.decide(envelope.len(), priority);
        let preferred = match (priority, self.video_symbol_size) {
            (MediaPriority::Audio, _) | (_, None) => decided.config.symbol_size,
            (_, Some(symbol_size)) => symbol_size,
        };
        let symbol_size = symbol_size_for_key(object.key(), preferred, self.limits)?;
        let source_symbols = source_symbol_count(envelope.len(), symbol_size);
        if envelope.len().div_ceil(usize::from(symbol_size))
            > raptorq_datagram_fec::MAX_SOURCE_SYMBOLS_PER_BLOCK as usize
        {
            return Err(Error::LimitExceeded {
                field: "source_symbols",
                actual: envelope.len().div_ceil(usize::from(symbol_size)),
                maximum: raptorq_datagram_fec::MAX_SOURCE_SYMBOLS_PER_BLOCK as usize,
            });
        }
        let repair_symbols = self.controller.repair_symbols_for(source_symbols, priority);
        if repair_symbols > self.limits.max_extra_repair_symbols {
            return Err(Error::LimitExceeded {
                field: "initial_repair_symbols",
                actual: repair_symbols as usize,
                maximum: self.limits.max_extra_repair_symbols as usize,
            });
        }
        let transfer_length = u32::try_from(envelope.len()).map_err(|_| Error::LimitExceeded {
            field: "protected_object",
            actual: envelope.len(),
            maximum: u32::MAX as usize,
        })?;
        let block_id = self.block_id.fetch_add(1, Ordering::Relaxed);
        let first_sequence = self.packet_sequence.fetch_add(
            u32::from(source_symbols) + repair_symbols,
            Ordering::Relaxed,
        );
        let kind = scheduler_kind(object, priority);
        let header = SymbolHeader {
            generation,
            subscription_id,
            key: object.key().clone(),
            kind,
            deadline,
            block_id,
            transfer_length,
            source_symbols,
            symbol_size,
            limits: self.limits,
        };
        let size = usize::from(symbol_size);
        let mut source = Vec::with_capacity(usize::from(source_symbols));
        let mut template: Option<Vec<u8>> = None;
        let mut coding = None;
        for index in 0..usize::from(source_symbols) {
            let start = (index * size).min(envelope.len());
            let end = envelope.len().min(start + size);
            let data = &envelope[start..end];
            let sequence = first_sequence.wrapping_add(index as u32);
            let wire = match template.as_ref() {
                Some(prefix) => header.wire(prefix, index as u32, sequence, data)?,
                None => {
                    let (wire, prefix, profile) = header.validated_wire(
                        MediaDatagramRole::Source,
                        index as u32,
                        sequence,
                        data,
                    )?;
                    template = Some(prefix);
                    coding = Some(profile);
                    wire
                }
            };
            source.push(wire);
        }
        let coding = coding.ok_or(Error::InvalidField {
            field: "raptorq_symbols",
            reason: "encoder produced an empty symbol set",
        })?;
        let next_packet_sequence = first_sequence
            .wrapping_add(u32::from(source_symbols))
            .wrapping_add(repair_symbols);
        Ok(SourcePart {
            announcement: ObjectAnnouncement {
                generation,
                subscription_id,
                key: object.key().clone(),
                kind,
                deadline,
                coding,
                initial_repair_symbols: repair_symbols,
                next_packet_sequence,
            },
            source,
            repair_symbols,
            first_repair_sequence: first_sequence.wrapping_add(u32::from(source_symbols)),
            envelope,
            header,
        })
    }
}

/// The source symbols of one object, as RLS1 datagrams ready to send, and
/// what its repair symbols need.
#[derive(Debug)]
pub struct SourcePart {
    pub announcement: ObjectAnnouncement,
    source: Vec<Bytes>,
    repair_symbols: u32,
    first_repair_sequence: u32,
    envelope: Vec<u8>,
    header: SymbolHeader,
}

impl SourcePart {
    /// The source symbols' RLS1 datagrams (the same for every lane).
    pub fn source_wires(&self) -> &[Bytes] {
        &self.source
    }

    /// Repair symbols of the object (the controller's count when it was
    /// encoded).
    pub const fn repair_count(&self) -> u32 {
        self.repair_symbols
    }

    /// The object's deadline, which every symbol carries.
    pub const fn deadline(&self) -> MediaDeadline {
        self.announcement.deadline
    }

    /// The coding of the object, as its symbols carry it.
    pub const fn coding(&self) -> CodingProfile {
        self.announcement.coding
    }

    /// The source symbols, decoded from their datagrams.
    ///
    /// # Errors
    ///
    /// None in practice: the datagrams were built valid.
    pub fn source(&self) -> Result<Vec<RelayDatagram>> {
        self.source
            .iter()
            .map(|wire| decode_datagram(wire, self.header.limits))
            .collect()
    }

    /// The object's repair symbols, decoded from [`Self::repair_wires`].
    ///
    /// # Errors
    ///
    /// Those of [`Self::repair_wires`].
    pub fn repair(&self) -> Result<Vec<RelayDatagram>> {
        self.repair_wires()?
            .iter()
            .map(|wire| decode_datagram(wire, self.header.limits))
            .collect()
    }

    /// The RLS1 datagrams of the object's repair symbols: ESIs K..,
    /// packet sequences right after its source symbols. The RaptorQ
    /// encoding plan of K source symbols comes from `raptorq`'s
    /// process-wide cache, so only the first object of each K (since its
    /// eviction) generates one.
    ///
    /// # Errors
    ///
    /// Returns an error when a datagram does not validate (it did not for
    /// the source symbols, so it does not here).
    pub fn repair_wires(&self) -> Result<Vec<Bytes>> {
        if self.repair_symbols == 0 {
            return Ok(Vec::new());
        }
        let size = usize::from(self.header.symbol_size);
        let padded_len = usize::from(self.header.source_symbols) * size;
        let padded;
        let block: &[u8] = if self.envelope.len() == padded_len {
            &self.envelope
        } else {
            let mut bytes = Vec::with_capacity(padded_len);
            bytes.extend_from_slice(&self.envelope);
            bytes.resize(padded_len, 0);
            padded = bytes;
            &padded
        };
        let oti = ObjectTransmissionInformation::new(
            u64::from(self.header.transfer_length),
            self.header.symbol_size,
            1,
            1,
            1,
        );
        let encoder = SourceBlockEncoder::new(0, &oti, block);
        let mut template: Option<Vec<u8>> = None;
        let mut wires = Vec::with_capacity(self.repair_symbols as usize);
        for (index, packet) in encoder
            .repair_packets(0, self.repair_symbols)
            .into_iter()
            .enumerate()
        {
            let esi = packet.payload_id().encoding_symbol_id();
            let sequence = self.first_repair_sequence.wrapping_add(index as u32);
            let wire = match template.as_ref() {
                Some(prefix) => self.header.wire(prefix, esi, sequence, packet.data())?,
                None => {
                    let (wire, prefix, _) = self.header.validated_wire(
                        MediaDatagramRole::Repair,
                        esi,
                        sequence,
                        packet.data(),
                    )?;
                    template = Some(prefix);
                    wire
                }
            };
            wires.push(wire);
        }
        Ok(wires)
    }
}

/// What every symbol of one object shares.
#[derive(Debug)]
struct SymbolHeader {
    generation: TopologyGeneration,
    subscription_id: SubscriptionId,
    key: ObjectKey,
    kind: MediaObjectKind,
    deadline: MediaDeadline,
    block_id: u32,
    transfer_length: u32,
    source_symbols: u16,
    symbol_size: u16,
    limits: RelayLimits,
}

impl SymbolHeader {
    /// The RLS1 datagram of one symbol, through relay-session's own
    /// `RelayDatagram::new` (which validates it) and `encode_datagram`;
    /// with the RLS1 bytes ahead of its RQD2 datagram, which every symbol
    /// of the object with the same role shares, and its coding.
    fn validated_wire(
        &self,
        role: MediaDatagramRole,
        encoding_symbol_id: u32,
        packet_sequence: u32,
        data: &[u8],
    ) -> Result<(Bytes, Vec<u8>, CodingProfile)> {
        let mut symbol = Vec::with_capacity(usize::from(self.symbol_size));
        symbol.extend_from_slice(data);
        symbol.resize(usize::from(self.symbol_size), 0);
        let packet = EncodingPacket::new(PayloadId::new(0, encoding_symbol_id), symbol);
        let serialized = packet.serialize();
        let header = DatagramFecHeader::raptorq(
            self.block_id,
            self.transfer_length,
            packet_sequence,
            self.source_symbols,
            self.symbol_size,
            &serialized,
        )?;
        let mut fec_datagram = Vec::with_capacity(HEADER_LEN + serialized.len());
        fec_datagram.resize(HEADER_LEN, 0);
        header.encode(&mut fec_datagram[..HEADER_LEN])?;
        fec_datagram.extend_from_slice(&serialized);
        let path_intent = match role {
            MediaDatagramRole::Source => MediaPathIntent::PrimarySource,
            MediaDatagramRole::Repair => MediaPathIntent::SecondaryRepair,
        };
        let datagram = RelayDatagram::new(
            self.generation,
            self.subscription_id,
            self.key.clone(),
            self.kind,
            role,
            path_intent,
            self.deadline,
            fec_datagram,
            self.limits,
        )?;
        let wire = encode_datagram(&datagram, self.limits)?;
        let prefix_len = wire.len() - datagram.fec_datagram.len();
        let prefix = wire[..prefix_len].to_vec();
        Ok((Bytes::from(wire), prefix, datagram.coding))
    }

    /// The RLS1 datagram of another symbol of the object: `prefix` (from
    /// [`Self::validated_wire`] of the same role; the RQD2 datagrams of an
    /// object all have one length) and its RQD2 datagram, written in place.
    fn wire(
        &self,
        prefix: &[u8],
        encoding_symbol_id: u32,
        packet_sequence: u32,
        data: &[u8],
    ) -> Result<Bytes> {
        let size = usize::from(self.symbol_size);
        let header_at = prefix.len();
        let payload_at = header_at + HEADER_LEN;
        let mut wire = Vec::with_capacity(payload_at + 4 + size);
        wire.extend_from_slice(prefix);
        wire.resize(payload_at, 0);
        wire.extend_from_slice(&PayloadId::new(0, encoding_symbol_id).serialize());
        wire.extend_from_slice(data);
        wire.resize(payload_at + 4 + size, 0);
        let header = DatagramFecHeader::raptorq(
            self.block_id,
            self.transfer_length,
            packet_sequence,
            self.source_symbols,
            self.symbol_size,
            &wire[payload_at..],
        )?;
        header.encode(&mut wire[header_at..payload_at])?;
        Ok(Bytes::from(wire))
    }
}

/// relay-session's symbol size rule (`symbol_size_for_key`): the preferred
/// size, within the RLS1 datagram and the RQD2 datagram budgets.
fn symbol_size_for_key(key: &ObjectKey, preferred: u16, limits: RelayLimits) -> Result<u16> {
    let identifiers = key
        .tenant()
        .len()
        .checked_add(key.stream().len())
        .and_then(|size| size.checked_add(key.track().len()))
        .ok_or(Error::LimitExceeded {
            field: "relay_datagram",
            actual: usize::MAX,
            maximum: RelayLimits::HARD.max_datagram_bytes,
        })?;
    let relay_budget = limits
        .max_datagram_bytes
        .checked_sub(RELAY_DATAGRAM_OVERHEAD + identifiers)
        .ok_or(Error::LimitExceeded {
            field: "relay_datagram_overhead",
            actual: RELAY_DATAGRAM_OVERHEAD + identifiers,
            maximum: limits.max_datagram_bytes,
        })?;
    let fec_budget = limits
        .max_fec_datagram_bytes
        .checked_sub(FEC_DATAGRAM_OVERHEAD)
        .ok_or(Error::InvalidField {
            field: "max_fec_datagram_bytes",
            reason: "FEC datagram bound cannot contain the RQD2 header",
        })?;
    let selected = usize::from(preferred).min(relay_budget).min(fec_budget);
    let selected = u16::try_from(selected).unwrap_or(u16::MAX);
    if selected == 0 {
        return Err(Error::InvalidField {
            field: "symbol_size",
            reason: "carrier budget leaves no room for a RaptorQ symbol",
        });
    }
    Ok(selected)
}

/// relay-session's wire kind of an object (`scheduler_kind`).
fn scheduler_kind(object: &MediaObject, priority: MediaPriority) -> MediaObjectKind {
    match object.kind() {
        ObjectKind::Initialization => MediaObjectKind::Initialization,
        ObjectKind::CodecConfiguration => MediaObjectKind::CodecConfig,
        ObjectKind::Discontinuity => MediaObjectKind::Data,
        ObjectKind::Media if object.is_keyframe() => MediaObjectKind::VideoKeyframe,
        ObjectKind::Media => match priority {
            MediaPriority::Audio => MediaObjectKind::Audio,
            MediaPriority::VideoKey => MediaObjectKind::VideoKeyframe,
            MediaPriority::VideoDelta => MediaObjectKind::VideoDelta,
            MediaPriority::Data => MediaObjectKind::Data,
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use relay_session::{
        encode_datagram, AdaptiveFecPolicy, CongestionConfig, ObjectAssembler, RaptorQObjectEncoder,
    };

    fn object(
        stream: u64,
        sequence: u64,
        bytes: usize,
        class: &[u8],
        keyframe: bool,
    ) -> MediaObject {
        let payload = (0..bytes)
            .map(|index| (index * 31 + sequence as usize * 7 + 1) as u8)
            .collect::<Vec<_>>();
        let key = ObjectKey::for_payload(
            "needletail",
            stream.to_string(),
            "audio",
            1_784_151_600_000_001,
            0,
            sequence,
            1,
            &payload,
        )
        .unwrap();
        MediaObject::builder(key, ObjectKind::Media, payload)
            .with_keyframe(keyframe)
            .with_metadata("scheduler-class", class.to_vec())
            .build()
            .unwrap()
    }

    fn controller(min_repair_symbols: u32) -> AdaptiveFecController {
        let policy = AdaptiveFecPolicy {
            min_repair_symbols,
            ..AdaptiveFecPolicy::default()
        };
        AdaptiveFecController::new(policy, CongestionConfig::default())
    }

    fn ids() -> (TopologyGeneration, SubscriptionId, MediaDeadline) {
        (
            TopologyGeneration::new(7).unwrap(),
            SubscriptionId::new(9).unwrap(),
            MediaDeadline::from_micros(4_000_000_000_000_000),
        )
    }

    /// Every symbol, its wire bytes and the announcement equal those of
    /// relay-session's encoder, object after object (so the block ids and
    /// packet sequences stay in step), for audio parts of one symbol, 4K
    /// parts of hundreds and repair counts from none to many.
    #[test]
    fn source_first_symbols_are_relay_sessions_byte_for_byte() {
        let limits = RelayLimits::default();
        let (generation, subscription, deadline) = ids();
        for min_repair in [0, 1, 3, 9] {
            let mut reference = RaptorQObjectEncoder::new(controller(min_repair), limits).unwrap();
            let encoder = SourceFirstEncoder::new(controller(min_repair), limits).unwrap();
            let cases: [(usize, &[u8], bool); 8] = [
                (116, b"audio", false),
                (304, b"audio", false),
                (1_456, b"audio", false),
                (3_000, b"audio", false),
                (1, b"video-delta", false),
                (1_300, b"video-delta", false),
                (64_000, b"video-delta", false),
                (420_000, b"video-keyframe", true),
            ];
            for (sequence, (bytes, class, keyframe)) in cases.into_iter().enumerate() {
                let object = object(4_001, sequence as u64, bytes, class, keyframe);
                let expected = reference
                    .encode_object_with_inferred_priority(
                        &object,
                        generation,
                        subscription,
                        deadline,
                    )
                    .unwrap();
                let part = encoder
                    .encode_sources(&object, generation, subscription, deadline)
                    .unwrap();
                let repair = part.repair().unwrap();
                assert_eq!(part.announcement, expected.announcement, "{bytes} bytes");
                assert_eq!(
                    part.source().unwrap(),
                    expected.source_symbols,
                    "{bytes} bytes"
                );
                assert_eq!(repair, expected.repair_symbols, "{bytes} bytes");
                assert_eq!(part.repair_count() as usize, repair.len());
                let ours = part
                    .source_wires()
                    .iter()
                    .cloned()
                    .chain(part.repair_wires().unwrap())
                    .collect::<Vec<_>>();
                let theirs = expected
                    .source_symbols
                    .iter()
                    .chain(&expected.repair_symbols)
                    .map(|symbol| encode_datagram(symbol, limits).unwrap())
                    .collect::<Vec<_>>();
                assert_eq!(ours.len(), theirs.len());
                for (ours, theirs) in ours.iter().zip(&theirs) {
                    assert_eq!(ours.as_ref(), theirs.as_slice(), "{bytes} bytes");
                }
            }
        }
    }

    /// A receiver that lost sources recovers the object from the repair
    /// symbols computed after the sources left.
    #[test]
    fn late_repair_recovers_lost_sources() {
        let limits = RelayLimits::default();
        let (generation, subscription, deadline) = ids();
        let encoder = SourceFirstEncoder::new(controller(4), limits).unwrap();
        let object = object(1, 3, 20_000, b"video-delta", false);
        let part = encoder
            .encode_sources(&object, generation, subscription, deadline)
            .unwrap();
        let repair = part.repair().unwrap();
        assert!(repair.len() >= 4);
        let mut assembler = ObjectAssembler::new(part.announcement.clone(), limits).unwrap();
        let mut recovered = None;
        // The first three sources never arrive.
        for symbol in part.source().unwrap().iter().skip(3).chain(&repair) {
            if recovered.is_none() {
                recovered = assembler.push_symbol(symbol).unwrap();
            }
        }
        assert_eq!(recovered.unwrap(), object);
    }

    /// `--relay-video-symbol-size`: video objects take the configured
    /// symbol size; audio parts keep their exact fit.
    #[test]
    fn the_video_symbol_size_applies_to_video_only() {
        let limits = RelayLimits::default();
        let (generation, subscription, deadline) = ids();
        let encoder = SourceFirstEncoder::new(controller(1), limits)
            .unwrap()
            .with_video_symbol_size(1_088);
        let video = encoder
            .encode_sources(
                &object(1, 0, 50_000, b"video-delta", false),
                generation,
                subscription,
                deadline,
            )
            .unwrap();
        assert_eq!(video.coding().symbol_size(), 1_088);
        assert!(video
            .source()
            .unwrap()
            .iter()
            .chain(&video.repair().unwrap())
            .all(|symbol| symbol.fec_datagram.len() == HEADER_LEN + 4 + 1_088));
        let audio = encoder
            .encode_sources(
                &object(4_001, 0, 300, b"audio", false),
                generation,
                subscription,
                deadline,
            )
            .unwrap();
        assert_eq!(audio.source_wires().len(), 1);
        assert!(audio.coding().symbol_size() > 300);
    }

    /// Threads that encode at once never share a block id or a packet
    /// sequence.
    #[test]
    fn concurrent_encodes_take_distinct_counters() {
        let limits = RelayLimits::default();
        let encoder = std::sync::Arc::new(SourceFirstEncoder::new(controller(2), limits).unwrap());
        let threads = (0..4)
            .map(|thread| {
                let encoder = std::sync::Arc::clone(&encoder);
                std::thread::spawn(move || {
                    let (generation, subscription, deadline) = ids();
                    let mut seen = Vec::new();
                    for sequence in 0..200 {
                        let part = encoder
                            .encode_sources(
                                &object(thread, sequence, 300, b"audio", false),
                                generation,
                                subscription,
                                deadline,
                            )
                            .unwrap();
                        for symbol in part.source().unwrap().iter().chain(&part.repair().unwrap()) {
                            let header = DatagramFecHeader::decode(&symbol.fec_datagram).unwrap();
                            seen.push((header.block_id, header.packet_sequence));
                        }
                    }
                    seen
                })
            })
            .collect::<Vec<_>>();
        let mut blocks = std::collections::HashSet::new();
        let mut sequences = std::collections::HashSet::new();
        let mut per_block = std::collections::HashMap::<u32, usize>::new();
        for thread in threads {
            for (block, sequence) in thread.join().unwrap() {
                blocks.insert(block);
                assert!(
                    sequences.insert(sequence),
                    "packet sequence {sequence} twice"
                );
                *per_block.entry(block).or_default() += 1;
            }
        }
        assert_eq!(blocks.len(), 800);
        assert!(per_block.values().all(|count| *count == per_block[&0]));
    }

    /// Not a check, a benchmark: wall time per object of relay-session's
    /// encode against the source symbols and the repair symbols of this
    /// encoder. Run with `cargo test --release --lib source_first_bench --
    /// --ignored --nocapture`.
    #[test]
    #[ignore = "benchmark"]
    fn source_first_bench() {
        let limits = RelayLimits::default();
        let (generation, subscription, deadline) = ids();
        for (label, bytes, class, rounds) in [
            ("opus 2.5 ms", 116, &b"audio"[..], 4_000),
            ("pcm 1 ms", 304, &b"audio"[..], 4_000),
            ("pcm 5 ms", 1_456, &b"audio"[..], 4_000),
            ("4K 25 Mbit/s 50 fps", 62_500, &b"video-delta"[..], 200),
            ("4K 80 Mbit/s 50 fps", 200_000, &b"video-delta"[..], 100),
            ("4K 200 Mbit/s 50 fps", 500_000, &b"video-delta"[..], 50),
        ] {
            for min_repair in [0, 2] {
                let mut reference =
                    RaptorQObjectEncoder::new(controller(min_repair), limits).unwrap();
                let encoder = SourceFirstEncoder::new(controller(min_repair), limits).unwrap();
                let objects = (0..rounds)
                    .map(|sequence| object(4_001, sequence, bytes, class, false))
                    .collect::<Vec<_>>();
                let started = std::time::Instant::now();
                let mut symbols = 0;
                for object in &objects {
                    let encoded = reference
                        .encode_object_with_inferred_priority(
                            object,
                            generation,
                            subscription,
                            deadline,
                        )
                        .unwrap();
                    symbols += encoded.source_symbols.len() + encoded.repair_symbols.len();
                }
                let old = started.elapsed().as_nanos() / u128::from(rounds as u32);
                let mut sources_ns = 0;
                let mut repair_ns = 0;
                for object in &objects {
                    let started = std::time::Instant::now();
                    let part = encoder
                        .encode_sources(object, generation, subscription, deadline)
                        .unwrap();
                    let built = std::time::Instant::now();
                    std::hint::black_box(part.repair_wires().unwrap());
                    sources_ns += built.duration_since(started).as_nanos();
                    repair_ns += built.elapsed().as_nanos();
                }
                eprintln!(
                    "{label}, min repair {min_repair}: relay-session {old} ns; source first: sources {} ns, then repair {} ns ({:.1} symbols)",
                    sources_ns / u128::from(rounds as u32),
                    repair_ns / u128::from(rounds as u32),
                    symbols as f64 / f64::from(rounds as u32),
                );
            }
        }
    }
}
