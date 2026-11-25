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
//! for the same block id and packet sequence (checked in the tests below).
//!
//! Part ids. A source-anchored audio part (A 2.2) names its datagrams from
//! its part number alone ([`PartIds`]): block id `N`, packet sequences
//! `N + i` for source symbol `i` and `N + K + j` for repair symbol or copy
//! `j`, and the compact sender's clock is the part's cell on a grid of the
//! source's media time. Two contributors fed the same source therefore emit
//! the same datagram for every part they both send in the same form, however
//! long each has run and whatever each lost. Other objects (video,
//! initialization) take a block id and packet sequences from the encoder's
//! counters, atomics that publishing threads share without a lock.
//!
//! Encoding plans. `raptorq`'s own plan cache is one FIFO of 64 plans keyed
//! by the source-symbol count K, shared by the whole process: 4K parts vary
//! in size frame to frame, so their K changes almost every frame, the cache
//! mostly misses (a plan of K = 380 is about 1.2 ms to generate) and video
//! pushes audio's K = 1..3 plans out of it. Each [`SourceFirstEncoder`] keeps
//! its own plans instead ([`PlanCache`]): small K are generated once and
//! never evicted, larger K in a bounded FIFO. With
//! [`SourceFirstEncoder::with_video_plan_grid`] (`--relay-video-plan-grid`) a
//! video object's symbol size is trimmed a little so its K lands on a
//! coarse grid (16 values per octave), so the plans of every 4K part size
//! fit the FIFO and a part's repair never pays for a plan after warm-up.

use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex, OnceLock};

use bytes::Bytes;
use media_object::{MediaObject, ObjectKey, ObjectKind};

use crate::part_timing::SourceAnchor;
use playlists::compact_object::{self, CompactDecision, CompactHeader, CompactSender};
use raptorq::{
    EncodingPacket, ObjectTransmissionInformation, PayloadId, SourceBlockEncoder,
    SourceBlockEncodingPlan,
};
use raptorq_datagram_fec::{
    source_symbol_count, DatagramFecHeader, RaptorQBlockProfile, HEADER_LEN,
};
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
    /// `--relay-video-plan-grid`: video objects' K on [`grid_symbol_count`].
    video_plan_grid: bool,
    plans: Arc<PlanCache>,
    /// Counters of objects encoded without [`PartIds`].
    block_id: AtomicU32,
    packet_sequence: AtomicU32,
    /// `--relay-compact-audio`: one-datagram source-anchored audio parts go
    /// as RLC1 datagrams of a slim object (wave 7 item 3), whole the first
    /// time in each cell of `compact_cell_us`.
    compact: Option<Mutex<CompactSender>>,
    /// The compact sender's cell on the source's timeline, in microseconds.
    compact_cell_us: u64,
    /// `--relay-repair-sizing measured` (4k-repair): a part of at least
    /// `min_symbols` source symbols takes repair sized to the loss its
    /// caller measured on the part's lane, not the controller's policy.
    measured_repair: Option<MeasuredRepair>,
}

/// How a part's repair is sized to the measured loss (4k-repair), from its
/// size in symbols and that loss alone (no media kind, no key frames): the
/// fewest symbols that leave at most [`TARGET_PART_FAILURE`] of parts
/// undecodable from one lane's symbols at [`planned_loss`], at least
/// `floor_permille` of K (and 2), at most `cap_percent` of K. Parts under
/// `min_symbols` (an audio part's one to three) keep the controller's
/// policy and the one-symbol copies rule.
#[derive(Debug, Clone, Copy)]
pub struct MeasuredRepair {
    pub floor_permille: u32,
    pub cap_percent: u32,
    pub min_symbols: u16,
}

/// At most this share of parts undecodable from one lane's symbols.
pub const TARGET_PART_FAILURE: f64 = 1e-4;

/// The loss a plan takes for a measured `loss_ppm`: half as much again,
/// and 0.1 % (av-mesh's link repair takes the same).
#[must_use]
pub fn planned_loss(loss_ppm: u32) -> f64 {
    (f64::from(loss_ppm) / 1e6 * 1.5 + 0.001).min(0.5)
}

/// Smallest `r` such that a block of `k` source symbols sent with `r`
/// repair symbols is undecodable with probability at most `target` when
/// each datagram is lost with probability `p` (a block of more than one
/// symbol counted as needing one more than `k`), at most `limit`.
#[must_use]
pub fn repair_for_loss(k: u32, p: f64, target: f64, limit: u32) -> u32 {
    if !(p > 0.0) || k == 0 {
        return 0;
    }
    let p = p.min(0.5);
    let margin = u32::from(k > 1);
    let (ln_p, ln_q) = (p.ln(), (1.0 - p).ln());
    let mean = (f64::from(k) * p / (1.0 - p)).floor() as u32;
    let mut r = margin.max(mean.saturating_sub(1));
    while r < limit {
        let n = k + r;
        let allowed = r - margin;
        let mut ln_term = f64::from(n) * ln_q;
        let mut delivered = ln_term.exp();
        for j in 0..allowed {
            ln_term += (f64::from(n - j) / f64::from(j + 1)).ln() + ln_p - ln_q;
            delivered += ln_term.exp();
        }
        if 1.0 - delivered <= target {
            return r;
        }
        r += 1;
    }
    limit
}

/// The repair of a part of `k` symbols at `loss_ppm` (see
/// [`MeasuredRepair`]).
#[must_use]
pub fn measured_repair_symbols(k: u32, loss_ppm: u32, floor_permille: u32, cap_percent: u32) -> u32 {
    let cap = (k * cap_percent / 100).max(16);
    let floor = (k * floor_permille).div_ceil(1_000).max(2).min(cap);
    repair_for_loss(k, planned_loss(loss_ppm), TARGET_PART_FAILURE, cap).max(floor)
}

/// The ids of a source-anchored part's datagrams (A 2.2): functions of its
/// part number and its place on the source's timeline, so that every
/// contributor fed the same source gives the part the same ones.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PartIds {
    /// The RQD2 block id: the part number, truncated to 32 bits.
    pub block_id: u32,
    /// The packet sequence of source symbol 0; source symbol `i` takes
    /// `first_sequence + i`, repair symbol or copy `j` `first_sequence + K
    /// + j` (wrapping). Unique within the object, which is all a receiver's
    /// dedup reads.
    pub first_sequence: u32,
    /// The compact sender's clock for the part: the start of its cell on
    /// the source's timeline ([`SourceAnchor::grid_us`]).
    pub grid_us: u64,
}

/// The largest compact datagram the contributor sends: under 1,280 bytes
/// (the smallest IPv6 path MTU) with its IP and UDP headers.
pub const MAX_COMPACT_DATAGRAM: usize = 1_200;
/// The track of the parts that go compact: opaque audio parts.
pub const COMPACT_TRACK: &str = "opaque-audio";

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
            video_plan_grid: false,
            plans: Arc::new(PlanCache::default()),
            block_id: AtomicU32::new(0),
            packet_sequence: AtomicU32::new(0),
            compact: None,
            compact_cell_us: 1,
            measured_repair: None,
        })
    }

    /// Parts of `min_symbols` or more take repair sized to the loss their
    /// caller measured (4k-repair; [`Self::encode_sources_measured`]).
    #[must_use]
    pub const fn with_measured_repair(mut self, repair: Option<MeasuredRepair>) -> Self {
        self.measured_repair = repair;
        self
    }

    /// One-datagram source-anchored audio parts go as compact RLC1
    /// datagrams (wave 7 item 3): a slim object of about 30 bytes and the
    /// part against the stream's descriptor, which the part sent whole
    /// carries. A stream's part goes whole when its shape is new or changed,
    /// and when it is the first part this encoder sends in a cell of
    /// `whole_every_us` of the source's media time: two encoders fed the
    /// same parts send the same ones whole. `None` sends every part whole
    /// (RLS1).
    #[must_use]
    pub fn with_compact_audio(mut self, whole_every_us: Option<u64>) -> Self {
        self.compact = whole_every_us.map(|every| Mutex::new(CompactSender::new(every)));
        self.compact_cell_us = whole_every_us.unwrap_or(1).max(1);
        self
    }

    /// The ids of the datagrams of the part at `anchor`.
    #[must_use]
    pub fn part_ids(&self, anchor: &SourceAnchor) -> PartIds {
        // Truncated on purpose: a 32-bit id wraps after 49 days of 1 ms
        // parts, and a receiver keys an object by its full key.
        let number = anchor.part_number() as u32;
        PartIds {
            block_id: number,
            first_sequence: number,
            grid_us: anchor.grid_us(self.compact_cell_us),
        }
    }

    /// The compact sender's counts (`None` without compact audio).
    pub fn compact_counts(&self) -> Option<compact_object::CompactSenderCounts> {
        self.compact
            .as_ref()
            .map(|sender| sender.lock().unwrap_or_else(|e| e.into_inner()).counts())
    }

    /// Non-audio objects use symbols of at most `symbol_size` bytes (still
    /// within the carrier budget). `0` keeps the controller's size.
    #[must_use]
    pub fn with_video_symbol_size(mut self, symbol_size: u16) -> Self {
        self.video_symbol_size = (symbol_size > 0).then_some(symbol_size);
        self
    }

    /// Non-audio objects of more than [`GRID_EXACT_SYMBOLS`] symbols take a
    /// symbol size a little under the configured one, so that their symbol
    /// count is on [`grid_symbol_count`]'s grid and the repair finds its
    /// encoding plan cached. The bytes are the same; a part has at most
    /// 1/16 more datagrams (about 3 % on average), each a few bytes shorter.
    #[must_use]
    pub const fn with_video_plan_grid(mut self, enabled: bool) -> Self {
        self.video_plan_grid = enabled;
        self
    }

    /// The encoding plans this encoder's repair uses.
    pub fn plans(&self) -> &PlanCache {
        &self.plans
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
    /// and repair) are taken from the encoder's counters here.
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
        self.encode_sources_with(object, generation, subscription_id, deadline, None, None)
    }

    /// [`Self::encode_sources`], with the repair count of a one-symbol
    /// audio object given (`single_symbol_audio_repair`, wave 7 item 2):
    /// its repair symbols are full copies, sized by the caller to the
    /// loss of its lanes instead of the controller's policy. Other objects
    /// keep the policy's count.
    ///
    /// `ids`: a source-anchored part's datagram ids ([`Self::part_ids`]),
    /// in place of the encoder's counters. Only such a part can go compact.
    ///
    /// # Errors
    ///
    /// As [`Self::encode_sources`].
    pub fn encode_sources_with(
        &self,
        object: &MediaObject,
        generation: TopologyGeneration,
        subscription_id: SubscriptionId,
        deadline: MediaDeadline,
        single_symbol_audio_repair: Option<u32>,
        ids: Option<PartIds>,
    ) -> Result<SourcePart> {
        self.encode_sources_measured(
            object,
            generation,
            subscription_id,
            deadline,
            single_symbol_audio_repair,
            ids,
            None,
        )
    }

    /// [`Self::encode_sources_with`], with the loss (ppm) measured on the
    /// lane the part goes on: with [`Self::with_measured_repair`], a part
    /// of at least its `min_symbols` takes [`measured_repair_symbols`].
    #[allow(clippy::too_many_arguments)]
    pub fn encode_sources_measured(
        &self,
        object: &MediaObject,
        generation: TopologyGeneration,
        subscription_id: SubscriptionId,
        deadline: MediaDeadline,
        single_symbol_audio_repair: Option<u32>,
        ids: Option<PartIds>,
        measured_loss_ppm: Option<u32>,
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
        let mut symbol_size = symbol_size_for_key(object.key(), preferred, self.limits)?;
        if self.video_plan_grid && priority != MediaPriority::Audio {
            symbol_size = grid_symbol_size(envelope.len(), symbol_size);
        }
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
        let repair_symbols = match (single_symbol_audio_repair, self.measured_repair, measured_loss_ppm) {
            (Some(repair), _, _) if priority == MediaPriority::Audio && source_symbols == 1 => {
                repair
            }
            (_, Some(measured), Some(loss_ppm)) if source_symbols >= measured.min_symbols => {
                measured_repair_symbols(
                    u32::from(source_symbols),
                    loss_ppm,
                    measured.floor_permille,
                    measured.cap_percent,
                )
                .min(self.limits.max_extra_repair_symbols)
            }
            _ => self.controller.repair_symbols_for(source_symbols, priority),
        };
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
        if let (Some(ids), MediaPriority::Audio, 1) = (ids, priority, source_symbols) {
            if let Some(part) = self.encode_compact(
                object,
                &envelope,
                generation,
                subscription_id,
                deadline,
                transfer_length,
                symbol_size,
                repair_symbols,
                ids,
            )? {
                return Ok(part);
            }
        }
        let (block_id, first_sequence) = match ids {
            Some(ids) => (ids.block_id, ids.first_sequence),
            None => (
                self.block_id.fetch_add(1, Ordering::Relaxed),
                self.packet_sequence.fetch_add(
                    u32::from(source_symbols) + repair_symbols,
                    Ordering::Relaxed,
                ),
            ),
        };
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
            plans: Arc::clone(&self.plans),
            compact: None,
        })
    }

    /// The compact form of a one-symbol source-anchored audio object, when
    /// the compact sender, on the part's cell of the source's timeline,
    /// gives it a slim object and its datagram fits; `None` sends it whole.
    /// Its block id and packet sequences are those of `ids`, as the whole
    /// object's would be.
    #[allow(clippy::too_many_arguments)]
    fn encode_compact(
        &self,
        object: &MediaObject,
        envelope: &[u8],
        generation: TopologyGeneration,
        subscription_id: SubscriptionId,
        deadline: MediaDeadline,
        transfer_length: u32,
        symbol_size: u16,
        repair_symbols: u32,
        ids: PartIds,
    ) -> Result<Option<SourcePart>> {
        let Some(sender) = self.compact.as_ref() else {
            return Ok(None);
        };
        // Opaque parts only: their payload is the LL-HLS part, which an
        // edge reads straight out of the slim object for its UDP clients.
        if object.key().track() != COMPACT_TRACK
            || object.metadata().get("payload-format").map(Vec::as_slice)
                == Some(b"soundkit-v2-bundle".as_slice())
        {
            return Ok(None);
        }
        // The compact sender's clock is the source's: a late fill has an
        // earlier cell than the part sent before it and goes compact.
        let slim = match sender.lock().unwrap_or_else(|e| e.into_inner()).decide(
            object,
            envelope,
            ids.grid_us,
        ) {
            CompactDecision::Whole => return Ok(None),
            CompactDecision::Slim(slim) => slim,
        };
        let key = object.key();
        let (Some(tenant), Some(track)) = (
            compact_object::static_name(&compact_object::TENANT_CODES, key.tenant()),
            compact_object::static_name(&compact_object::TRACK_CODES, key.track()),
        ) else {
            return Ok(None);
        };
        let kind = scheduler_kind(object, MediaPriority::Audio);
        let PartIds {
            block_id,
            first_sequence,
            ..
        } = ids;
        let header = CompactHeader {
            generation: generation.get(),
            subscription_id: subscription_id.get(),
            deadline_us: deadline.expires_at_us,
            role: 0,
            kind: kind_tag(kind),
            path: 0,
            tenant,
            stream: key.stream(),
            track,
            epoch: key.epoch(),
            group: key.group(),
            object: key.object(),
            version: key.version(),
            packet_sequence: first_sequence,
            esi: 0,
        };
        let Some(wire) = compact_object::encode_datagram(&header, &slim) else {
            return Ok(None);
        };
        if wire.len() > MAX_COMPACT_DATAGRAM.min(self.limits.max_datagram_bytes) {
            return Ok(None);
        }
        let block = RaptorQBlockProfile::new(block_id, transfer_length, 1, symbol_size)
            .map_err(Error::from)?;
        let symbol_header = SymbolHeader {
            generation,
            subscription_id,
            key: key.clone(),
            kind,
            deadline,
            block_id,
            transfer_length,
            source_symbols: 1,
            symbol_size,
            limits: self.limits,
        };
        Ok(Some(SourcePart {
            announcement: ObjectAnnouncement {
                generation,
                subscription_id,
                key: key.clone(),
                kind,
                deadline,
                coding: CodingProfile::from_block(block),
                initial_repair_symbols: repair_symbols,
                next_packet_sequence: first_sequence.wrapping_add(1 + repair_symbols),
            },
            source: vec![Bytes::from(wire.clone())],
            repair_symbols,
            first_repair_sequence: first_sequence.wrapping_add(1),
            envelope: envelope.to_vec(),
            header: symbol_header,
            plans: Arc::clone(&self.plans),
            compact: Some(CompactCopies { wire }),
        }))
    }
}

/// The RLS1 tag of an object kind, as the compact datagram carries it.
const fn kind_tag(kind: MediaObjectKind) -> u8 {
    match kind {
        MediaObjectKind::Initialization => 0,
        MediaObjectKind::CodecConfig => 1,
        MediaObjectKind::VideoKeyframe => 2,
        MediaObjectKind::Audio => 3,
        MediaObjectKind::VideoDelta => 4,
        MediaObjectKind::Data => 5,
    }
}

/// A compact part's source datagram, from which its copies are made.
#[derive(Debug)]
struct CompactCopies {
    wire: Vec<u8>,
}

impl CompactCopies {
    /// Copy `index` (from 0) of the part, a repair: the same slim object
    /// under a repair header, ESI `1 + index`, its own packet sequence.
    fn copy(&self, index: u32, packet_sequence: u32) -> Option<Bytes> {
        let (source, slim) = compact_object::parse_datagram(&self.wire).ok()?;
        let header = CompactHeader {
            role: 1,
            path: 1,
            packet_sequence,
            esi: 1 + index,
            ..source
        };
        compact_object::encode_datagram(&header, slim).map(Bytes::from)
    }
}

/// Symbol counts up to this have a grid point each.
pub const GRID_EXACT_SYMBOLS: u16 = 32;

/// The grid point at or above `symbols`: `symbols` itself up to
/// [`GRID_EXACT_SYMBOLS`], above it rounded up to 1/16 of its octave (a
/// multiple of 2 from 32, 4 from 64, ... 32 from 512).
#[must_use]
pub fn grid_symbol_count(symbols: u16) -> u16 {
    if symbols <= GRID_EXACT_SYMBOLS {
        return symbols;
    }
    let octave = 15 - symbols.leading_zeros();
    let step = 1_u32 << (octave - 4);
    let rounded = u32::from(symbols).div_ceil(step) * step;
    u16::try_from(rounded).unwrap_or(symbols)
}

/// The symbol size, at most `max_symbol_size`, that gives an object of
/// `len` bytes a symbol count on the grid; `max_symbol_size` when its count
/// is already on it or no size gives exactly the grid's count.
#[must_use]
pub fn grid_symbol_size(len: usize, max_symbol_size: u16) -> u16 {
    let size = usize::from(max_symbol_size);
    let Ok(symbols) = u16::try_from(len.div_ceil(size)) else {
        return max_symbol_size;
    };
    let target = grid_symbol_count(symbols);
    if target == symbols {
        return max_symbol_size;
    }
    let trimmed = len.div_ceil(usize::from(target));
    if trimmed == 0 || trimmed > size || len.div_ceil(trimmed) != usize::from(target) {
        return max_symbol_size;
    }
    u16::try_from(trimmed).unwrap_or(max_symbol_size)
}

/// Symbol counts below this have a plan slot of their own, generated once and
/// never evicted (audio parts, small video objects).
const SMALL_PLAN_SYMBOLS: usize = 16;
/// Larger plans kept, oldest evicted first; and their bound in total source
/// symbols (a plan of K symbols is roughly 0.75 K KiB: about 24 MiB here).
const LARGE_PLAN_CAPACITY: usize = 64;
const LARGE_PLAN_SYMBOL_BUDGET: usize = 32_768;

/// RaptorQ encoding plans by source-symbol count, for one encoder.
#[derive(Default)]
pub struct PlanCache {
    small: [OnceLock<Arc<SourceBlockEncodingPlan>>; SMALL_PLAN_SYMBOLS],
    large: Mutex<LargePlans>,
    generated: AtomicU32,
}

#[derive(Default)]
struct LargePlans {
    plans: HashMap<u16, Arc<SourceBlockEncodingPlan>>,
    order: VecDeque<u16>,
    symbols: usize,
}

impl std::fmt::Debug for PlanCache {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("PlanCache")
            .field("generated", &self.generated())
            .finish_non_exhaustive()
    }
}

impl PlanCache {
    /// The plan of `symbols` source symbols, generated on a miss (outside
    /// the lock: another encode of the same count may generate it too).
    pub fn plan(&self, symbols: u16) -> Arc<SourceBlockEncodingPlan> {
        if let Some(slot) = self.small.get(usize::from(symbols)) {
            return Arc::clone(slot.get_or_init(|| self.generate(symbols)));
        }
        if let Some(plan) = self.lock().plans.get(&symbols) {
            return Arc::clone(plan);
        }
        let plan = self.generate(symbols);
        let mut large = self.lock();
        if let Some(plan) = large.plans.get(&symbols) {
            return Arc::clone(plan);
        }
        while large.order.len() >= LARGE_PLAN_CAPACITY
            || (!large.order.is_empty()
                && large.symbols + usize::from(symbols) > LARGE_PLAN_SYMBOL_BUDGET)
        {
            let Some(evicted) = large.order.pop_front() else {
                break;
            };
            large.plans.remove(&evicted);
            large.symbols -= usize::from(evicted);
        }
        large.order.push_back(symbols);
        large.symbols += usize::from(symbols);
        large.plans.insert(symbols, Arc::clone(&plan));
        plan
    }

    /// Plans generated so far (each a cache miss).
    pub fn generated(&self) -> u32 {
        self.generated.load(Ordering::Relaxed)
    }

    fn generate(&self, symbols: u16) -> Arc<SourceBlockEncodingPlan> {
        self.generated.fetch_add(1, Ordering::Relaxed);
        Arc::new(SourceBlockEncodingPlan::generate(symbols))
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, LargePlans> {
        self.large
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
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
    plans: Arc<PlanCache>,
    /// A compact part (RLC1): its copies are its datagram's.
    compact: Option<CompactCopies>,
}

impl SourcePart {
    /// The object's canonical envelope (`media_object::encode`), the bytes
    /// its symbols carry.
    pub fn envelope(&self) -> &[u8] {
        &self.envelope
    }

    /// Whether the part goes as compact RLC1 datagrams.
    pub const fn is_compact(&self) -> bool {
        self.compact.is_some()
    }

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
    /// encoding plan of K source symbols comes from the encoder's
    /// [`PlanCache`], so only the first object of each K (since its
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
        if let Some(compact) = &self.compact {
            return (0..self.repair_symbols)
                .map(|index| {
                    compact
                        .copy(index, self.first_repair_sequence.wrapping_add(index))
                        .ok_or(Error::InvalidField {
                            field: "compact_copy",
                            reason: "the compact datagram does not read back",
                        })
                })
                .collect();
        }
        self.planned_repair_wires(0, self.repair_symbols)
    }

    /// `count` repair symbols from repair ordinal `first_ordinal` on (4k-repair:
    /// the answers to a forwarder's ask take ordinals of their own, disjoint
    /// from the part's repair), with the part's own packet sequences (its
    /// first source's plus the ESI). None for a compact part.
    ///
    /// # Errors
    ///
    /// Returns an error when a datagram does not validate.
    pub fn extra_repair_wires(&self, first_ordinal: u32, count: u32) -> Result<Vec<Bytes>> {
        if self.compact.is_some() || count == 0 {
            return Ok(Vec::new());
        }
        self.planned_repair_wires(first_ordinal, count)
    }

    /// The source symbols of the object (none for a compact part).
    pub fn source_symbol_count(&self) -> u32 {
        u32::from(self.header.source_symbols)
    }

    fn planned_repair_wires(&self, first_ordinal: u32, count: u32) -> Result<Vec<Bytes>> {
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
        let plan = self.plans.plan(self.header.source_symbols);
        let encoder = SourceBlockEncoder::with_encoding_plan(0, &oti, block, &plan);
        let mut template: Option<Vec<u8>> = None;
        let mut wires = Vec::with_capacity(count as usize);
        for (index, packet) in encoder
            .repair_packets(first_ordinal, count)
            .into_iter()
            .enumerate()
        {
            let esi = packet.payload_id().encoding_symbol_id();
            let sequence = self
                .first_repair_sequence
                .wrapping_add(first_ordinal)
                .wrapping_add(index as u32);
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
    fn a_one_symbol_audio_part_takes_the_repair_count_it_is_given() {
        let (generation, subscription, deadline) = ids();
        // The policy gives audio one repair symbol at least.
        let encoder = SourceFirstEncoder::new(controller(1), RelayLimits::default()).unwrap();
        let audio = object(3001, 0, 60, b"audio", false);
        let policy = encoder
            .encode_sources(&audio, generation, subscription, deadline)
            .unwrap();
        assert!(policy.repair_count() >= 1);
        for given in [0_u32, 1, 3] {
            let part = encoder
                .encode_sources_with(
                    &audio,
                    generation,
                    subscription,
                    deadline,
                    Some(given),
                    None,
                )
                .unwrap();
            assert_eq!(part.source_wires().len(), 1);
            assert_eq!(part.repair_count(), given);
            assert_eq!(part.repair_wires().unwrap().len(), given as usize);
        }
        // A part of several symbols keeps the policy.
        let large = object(3001, 1, 4_000, b"audio", false);
        let policy = encoder
            .encode_sources(&large, generation, subscription, deadline)
            .unwrap();
        let given = encoder
            .encode_sources_with(&large, generation, subscription, deadline, Some(0), None)
            .unwrap();
        assert!(given.source_wires().len() > 1);
        assert_eq!(given.repair_count(), policy.repair_count());
    }

    /// The session of the anchored test parts: 14 November 2023.
    const SESSION_NS: u64 = 1_700_000_000_000_000_000;

    /// Part `number` of a session of one-epoch opaque audio parts of
    /// `samples` at 48 kHz, as the contributor builds it: named and timed
    /// from its anchor alone.
    fn anchored_part(number: u64, samples: u64) -> (MediaObject, SourceAnchor) {
        use media_object::{ClockConfidence, ClockTimestamp, Stage, StageTimestamp};
        let anchor = SourceAnchor {
            session_id_ns: SESSION_NS,
            first_pts_samples: number * samples,
            last_pts_samples: number * samples,
            part_samples: samples,
            sample_rate: 48_000,
        };
        let payload = (0..56)
            .map(|index| (index * 13 + number as usize) as u8)
            .collect::<Vec<_>>();
        let key = ObjectKey::for_payload(
            "default",
            "3001",
            "opaque-audio",
            anchor.key_epoch(),
            0,
            anchor.part_number(),
            2 << 16,
            &payload,
        )
        .unwrap();
        let stamp = |ns| {
            ClockTimestamp::new(ns, "source:aep1", ClockConfidence::estimated(5_000_000)).unwrap()
        };
        let source_time = stamp(anchor.source_time_ns());
        let object = MediaObject::builder(key, ObjectKind::Media, payload)
            .with_deadline(stamp(anchor.deadline_ns(1_000)))
            .with_stage_timestamp(StageTimestamp::new(Stage::Packaged, source_time.clone()))
            .with_stage_timestamp(StageTimestamp::new(Stage::Published, source_time))
            .with_metadata("scheduler-class", b"audio".to_vec())
            .with_metadata(
                "duration-us",
                (samples * 1_000 / 48).to_string().into_bytes(),
            )
            .with_metadata("hls-msn", (number / 1_000).to_string().into_bytes())
            .with_metadata("hls-part", (number % 1_000).to_string().into_bytes())
            .build()
            .unwrap();
        (object, anchor)
    }

    /// Every datagram of one part as an encoder sends it, sources first,
    /// and whether it went compact.
    fn datagrams(
        encoder: &SourceFirstEncoder,
        object: &MediaObject,
        anchor: &SourceAnchor,
        copies: u32,
    ) -> (bool, Vec<Bytes>) {
        let (generation, subscription, deadline) = ids();
        let part = encoder
            .encode_sources_with(
                object,
                generation,
                subscription,
                deadline,
                Some(copies),
                Some(encoder.part_ids(anchor)),
            )
            .unwrap();
        let mut wires = part.source_wires().to_vec();
        wires.extend(part.repair_wires().unwrap());
        (part.is_compact(), wires)
    }

    /// An encoder as the contributor's: compact audio, whole once in each
    /// 100 ms cell of the source's timeline.
    fn compact_encoder() -> SourceFirstEncoder {
        SourceFirstEncoder::new(controller(0), RelayLimits::default())
            .unwrap()
            .with_compact_audio(Some(100_000))
    }

    /// `--relay-compact-audio`: the first part of a stream goes whole (the
    /// descriptor), the next ones as RLC1 datagrams of a few dozen bytes
    /// and the part, whose copies are repair headers over the same slim
    /// object; a receiver that learnt the descriptor rebuilds each part's
    /// envelope byte for byte. Its ids are the part number's. Without
    /// compact audio, or without part ids, every part goes whole.
    #[test]
    fn compact_audio_parts_rebuild_the_whole_envelope() {
        use playlists::compact_object::{parse_datagram, CompactTemplates};
        let limits = RelayLimits::default();
        let (generation, subscription, deadline) = ids();
        let encoder = SourceFirstEncoder::new(controller(0), limits)
            .unwrap()
            .with_compact_audio(Some(60_000_000));
        let mut templates = CompactTemplates::new();
        for number in 998..1_003_u64 {
            let (object, anchor) = anchored_part(number, 48);
            let envelope = media_object::encode(&object).unwrap();
            let (compact, wires) = datagrams(&encoder, &object, &anchor, 2);
            assert_eq!(wires.len(), 3);
            if number == 998 {
                assert!(!compact);
                assert!(wires.iter().all(|wire| wire.starts_with(b"RLS1")));
                templates.learn(&object).unwrap();
                continue;
            }
            assert!(compact);
            for (index, wire) in wires.iter().enumerate() {
                assert!(wire.len() <= 56 + 80, "{} B", wire.len());
                let (header, slim) = parse_datagram(wire).unwrap();
                assert_eq!(header.stream, "3001");
                assert_eq!(header.object, number);
                assert_eq!(header.version, 2 << 16);
                assert_eq!(header.generation, 7);
                assert_eq!(header.subscription_id, 9);
                assert_eq!(header.kind, 3);
                assert_eq!(header.esi, index as u32);
                assert_eq!(header.role, u8::from(index > 0));
                assert_eq!(header.path, u8::from(index > 0));
                assert_eq!(header.packet_sequence, number as u32 + index as u32);
                let rebuilt = templates.expand("3001", slim).unwrap();
                assert_eq!(media_object::encode(&rebuilt).unwrap(), envelope);
            }
        }
        let counts = encoder.compact_counts().unwrap();
        assert_eq!((counts.whole, counts.slim), (1, 4));

        // Without part ids (an object not anchored on its source) the part
        // goes whole and takes the encoder's counters.
        let (object, _) = anchored_part(1_003, 48);
        let unanchored = encoder
            .encode_sources_with(&object, generation, subscription, deadline, Some(0), None)
            .unwrap();
        assert!(!unanchored.is_compact());

        let whole = SourceFirstEncoder::new(controller(0), limits).unwrap();
        assert!(whole.compact_counts().is_none());
        let (object, anchor) = anchored_part(5, 48);
        assert!(!datagrams(&whole, &object, &anchor, 0).0);
    }

    /// A source-anchored part's block id and packet sequences are its part
    /// number's, whatever the encoder encoded before: RLS1 sources and
    /// repair of a part of several symbols, and the whole form of a part
    /// of one.
    #[test]
    fn part_ids_name_the_datagrams_of_whole_parts() {
        let limits = RelayLimits::default();
        let (generation, subscription, deadline) = ids();
        let encoder = SourceFirstEncoder::new(controller(2), limits).unwrap();
        // Counters moved by objects encoded without ids change nothing.
        for sequence in 0..3 {
            encoder
                .encode_sources(
                    &object(1, sequence, 9_000, b"video-delta", false),
                    generation,
                    subscription,
                    deadline,
                )
                .unwrap();
        }
        let (object, anchor) = anchored_part(70_000, 48);
        let large = MediaObject::builder(
            ObjectKey::for_payload(
                "default",
                "2001",
                "opaque-audio",
                anchor.key_epoch(),
                0,
                anchor.part_number(),
                2 << 16,
                &[7_u8; 3_000],
            )
            .unwrap(),
            ObjectKind::Media,
            vec![7_u8; 3_000],
        )
        .with_deadline(object.deadline().unwrap().clone())
        .with_metadata("scheduler-class", b"audio".to_vec())
        .build()
        .unwrap();
        let ids = encoder.part_ids(&anchor);
        assert_eq!((ids.block_id, ids.first_sequence), (70_000, 70_000));
        let part = encoder
            .encode_sources_with(&large, generation, subscription, deadline, None, Some(ids))
            .unwrap();
        let k = part.source_wires().len() as u32;
        assert_eq!(k, 3);
        assert!(part.repair_count() >= 2);
        for (index, symbol) in part
            .source()
            .unwrap()
            .iter()
            .chain(&part.repair().unwrap())
            .enumerate()
        {
            let fec = raptorq_datagram_fec::decode_header(&symbol.fec_datagram).unwrap();
            assert_eq!(fec.block_id, 70_000);
            assert_eq!(fec.packet_sequence, 70_000 + index as u32);
        }
        assert_eq!(
            part.announcement.next_packet_sequence,
            70_000 + k + part.repair_count()
        );
        let whole = encoder
            .encode_sources_with(
                &object,
                generation,
                subscription,
                deadline,
                Some(1),
                Some(ids),
            )
            .unwrap();
        assert!(!whole.is_compact());
        for (index, symbol) in whole
            .source()
            .unwrap()
            .iter()
            .chain(&whole.repair().unwrap())
            .enumerate()
        {
            let fec = raptorq_datagram_fec::decode_header(&symbol.fec_datagram).unwrap();
            assert_eq!(fec.block_id, 70_000);
            assert_eq!(fec.packet_sequence, 70_000 + index as u32);
        }
    }

    /// The compact sender's clock is the source's timeline: of 1 ms parts,
    /// those that open a 100 ms cell go whole (and the first of the
    /// stream), every other part compact.
    #[test]
    fn the_first_part_of_each_grid_cell_goes_whole() {
        let encoder = compact_encoder();
        let whole = (0..350_u64)
            .filter(|number| {
                let (object, anchor) = anchored_part(*number, 48);
                !datagrams(&encoder, &object, &anchor, 1).0
            })
            .collect::<Vec<_>>();
        assert_eq!(whole, [0, 100, 200, 300]);
        // 2.5 ms parts: a cell is 40 parts.
        let encoder = compact_encoder();
        let whole = (0..130_u64)
            .filter(|number| {
                let (object, anchor) = anchored_part(*number, 120);
                !datagrams(&encoder, &object, &anchor, 1).0
            })
            .collect::<Vec<_>>();
        assert_eq!(whole, [0, 40, 80, 120]);
    }

    /// An encoder that starts while the source runs (a restarted
    /// contributor) sends its first part whole where one that ran all along
    /// sends it compact; from its next part on, every datagram of every part
    /// is the same bytes, copy for copy, whatever the two sent before.
    #[test]
    fn a_restarted_encoder_differs_only_in_its_first_part() {
        let running = compact_encoder();
        let restarted = compact_encoder();
        for number in 0..450_u64 {
            let (object, anchor) = anchored_part(number, 48);
            let (running_compact, running_wires) = datagrams(&running, &object, &anchor, 3);
            if number < 150 {
                continue;
            }
            let (restarted_compact, restarted_wires) = datagrams(&restarted, &object, &anchor, 3);
            if number == 150 {
                assert!(running_compact);
                assert!(!restarted_compact, "the restarted encoder's first part");
                assert!(restarted_wires.iter().all(|wire| wire.starts_with(b"RLS1")));
                continue;
            }
            assert_eq!(running_compact, restarted_compact, "part {number}");
            assert_eq!(running_wires, restarted_wires, "part {number}");
        }
    }

    /// A part that completes late, after a later part was sent, belongs to
    /// an earlier cell: it goes compact, the same datagrams as an encoder
    /// that had it in order sends, and it does not move the cell of the
    /// parts after it.
    #[test]
    fn a_late_fill_goes_compact() {
        let in_order = compact_encoder();
        let late = compact_encoder();
        let mut sent_late = std::collections::BTreeMap::new();
        // Part 99 closes cell 0 and part 100 opens cell 1: the late encoder
        // has part 99 only after 100 and 101.
        for number in (0..99_u64).chain([100, 101, 99, 102, 103]) {
            let (object, anchor) = anchored_part(number, 48);
            sent_late.insert(number, datagrams(&late, &object, &anchor, 2));
        }
        for number in 0..104_u64 {
            let (object, anchor) = anchored_part(number, 48);
            let expected = datagrams(&in_order, &object, &anchor, 2);
            assert_eq!(sent_late[&number], expected, "part {number}");
        }
        assert!(sent_late[&99].0, "the late fill goes compact");
        assert!(!sent_late[&100].0, "the cell after it opened whole");
        assert!(sent_late[&102].0);
    }

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

    /// Part sizes of a 4K stream: `bytes` plus or minus up to 20 % every
    /// frame (a fixed pseudo-random sequence) and a keyframe three times as
    /// large every 50th part. Returns (bytes, keyframe).
    fn varied_part_size(bytes: usize, sequence: u64) -> (usize, bool) {
        if sequence % 50 == 0 {
            return (bytes * 3, true);
        }
        // splitmix64 of the sequence: sizes independent frame to frame.
        let mut mixed = sequence.wrapping_add(0x9e37_79b9_7f4a_7c15);
        mixed = (mixed ^ (mixed >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        mixed = (mixed ^ (mixed >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        mixed ^= mixed >> 31;
        let permille = 800 + (mixed >> 33) % 401;
        (bytes * permille as usize / 1_000, false)
    }

    #[test]
    fn the_grid_rounds_up_within_a_sixteenth() {
        for symbols in 1..=8_192_u16 {
            let grid = grid_symbol_count(symbols);
            assert!(grid >= symbols);
            if symbols <= GRID_EXACT_SYMBOLS {
                assert_eq!(grid, symbols);
            } else {
                assert!(
                    u32::from(grid) * 16 <= u32::from(symbols) * 17 + 16,
                    "{symbols} -> {grid}"
                );
                assert_eq!(grid_symbol_count(grid), grid, "{symbols} -> {grid}");
            }
        }
        // 4K parts of 500 kB +- 20 % in 1,316-byte symbols: 304..456.
        let points = (304..=456)
            .map(grid_symbol_count)
            .collect::<std::collections::BTreeSet<_>>();
        assert!(points.len() <= 12, "{points:?}");
    }

    #[test]
    fn a_grid_symbol_size_gives_the_grid_count_and_never_grows() {
        for max in [1_088_u16, 1_316, 1_400] {
            for len in (1..2_000_000).step_by(997) {
                let size = grid_symbol_size(len, max);
                assert!(size <= max && size > 0);
                let symbols = u16::try_from(len.div_ceil(usize::from(size))).unwrap();
                let natural = u16::try_from(len.div_ceil(usize::from(max))).unwrap();
                assert!(
                    symbols == grid_symbol_count(natural) || size == max,
                    "{len} {max}"
                );
                // A size giving exactly the grid's count exists while the
                // count is below the symbol size (L / K'(K'-1) >= 1): every
                // 4K part. Above it the size may stay (off the grid).
                if grid_symbol_count(natural) < max {
                    assert_eq!(symbols, grid_symbol_count(natural), "{len} {max}");
                }
            }
        }
    }

    /// With the grid, a varied 4K stream's parts decode after losses, their
    /// symbols stay within the configured size, audio keeps its exact fit,
    /// and after warm-up the repair never generates a plan.
    #[test]
    fn grid_parts_decode_and_their_plans_stay_cached() {
        let limits = RelayLimits::default();
        let (generation, subscription, deadline) = ids();
        let encoder = SourceFirstEncoder::new(controller(2), limits)
            .unwrap()
            .with_video_plan_grid(true);
        let plain = SourceFirstEncoder::new(controller(2), limits).unwrap();
        let audio = object(4_001, 0, 300, b"audio", false);
        let audio_part = encoder
            .encode_sources(&audio, generation, subscription, deadline)
            .unwrap();
        assert_eq!(audio_part.source_wires().len(), 1);
        assert_eq!(
            audio_part.coding(),
            plain
                .encode_sources(&audio, generation, subscription, deadline)
                .unwrap()
                .coding()
        );
        let mut counts = std::collections::BTreeSet::new();
        for sequence in 0..300_u64 {
            let (bytes, keyframe) = varied_part_size(60_000, sequence);
            let class: &[u8] = if keyframe {
                b"video-keyframe"
            } else {
                b"video-delta"
            };
            let object = object(1, sequence, bytes, class, keyframe);
            let part = encoder
                .encode_sources(&object, generation, subscription, deadline)
                .unwrap();
            let symbols = u16::try_from(part.source_wires().len()).unwrap();
            assert_eq!(grid_symbol_count(symbols), symbols);
            assert!(part.coding().symbol_size() <= 1_316);
            counts.insert(symbols);
            if sequence < 3 {
                let repair = part.repair().unwrap();
                let mut assembler =
                    ObjectAssembler::new(part.announcement.clone(), limits).unwrap();
                let mut recovered = None;
                for symbol in part.source().unwrap().iter().skip(2).chain(&repair) {
                    if recovered.is_none() {
                        recovered = assembler.push_symbol(symbol).unwrap();
                    }
                }
                assert_eq!(recovered.unwrap(), object);
            } else {
                part.repair_wires().unwrap();
            }
        }
        // Every plan the stream needed was generated once (the audio part's
        // repair was never computed).
        assert_eq!(encoder.plans().generated() as usize, counts.len());
        assert!(counts.len() <= 16, "{counts:?}");
    }

    /// Video plans of many symbol counts never evict a small (audio) plan:
    /// the audio part's plan is generated once.
    #[test]
    fn video_plans_never_evict_audio_plans() {
        let limits = RelayLimits::default();
        let (generation, subscription, deadline) = ids();
        let encoder = SourceFirstEncoder::new(controller(1), limits).unwrap();
        let audio = |sequence| {
            encoder
                .encode_sources(
                    &object(4_001, sequence, 300, b"audio", false),
                    generation,
                    subscription,
                    deadline,
                )
                .unwrap()
                .repair_wires()
                .unwrap()
        };
        audio(0);
        let after_audio = encoder.plans().generated();
        for sequence in 0..(LARGE_PLAN_CAPACITY as u64 + 20) {
            // 1,316-byte symbols: K = 17, 18, ... (all distinct).
            let bytes = (17 + sequence as usize) * 1_316 - 900;
            encoder
                .encode_sources(
                    &object(1, sequence, bytes, b"video-delta", false),
                    generation,
                    subscription,
                    deadline,
                )
                .unwrap()
                .repair_wires()
                .unwrap();
        }
        let after_video = encoder.plans().generated();
        assert_eq!(after_video - after_audio, LARGE_PLAN_CAPACITY as u32 + 20);
        audio(1);
        assert_eq!(encoder.plans().generated(), after_video);
    }

    /// Not a check, a benchmark: wall time per object of relay-session's
    /// encode against the source symbols and the repair symbols of this
    /// encoder, for parts of one size ("fixed": every repair after the first
    /// finds its plan cached) and of 4K-like varied sizes (+-20 % per frame,
    /// a keyframe 3x every 50th: "varied"), the latter without and with
    /// `--relay-video-plan-grid`. "repair ready" is sources + repair: when
    /// the last repair symbol could be handed to a lane if they are computed
    /// one after the other. Run with `cargo test --release --lib
    /// source_first_bench -- --ignored --nocapture`.
    #[test]
    #[ignore = "benchmark"]
    fn source_first_bench() {
        let limits = RelayLimits::default();
        let (generation, subscription, deadline) = ids();
        for (label, bytes, class, rounds) in [
            ("opus 2.5 ms", 116, &b"audio"[..], 4_000),
            ("pcm 1 ms", 304, &b"audio"[..], 4_000),
            ("pcm 5 ms", 1_456, &b"audio"[..], 4_000),
            ("4K 25 Mbit/s 50 fps", 62_500, &b"video-delta"[..], 1_000),
            ("4K 80 Mbit/s 50 fps", 200_000, &b"video-delta"[..], 500),
            ("4K 200 Mbit/s 50 fps", 500_000, &b"video-delta"[..], 300),
        ] {
            let audio = class == b"audio";
            let modes: &[(&str, bool, bool)] = if audio {
                &[("fixed", false, false)]
            } else {
                &[
                    ("fixed", false, false),
                    ("varied", true, false),
                    ("varied+grid", true, true),
                ]
            };
            for &(mode, varied, grid) in modes {
                for min_repair in [0, 2] {
                    let mut reference =
                        RaptorQObjectEncoder::new(controller(min_repair), limits).unwrap();
                    let encoder = SourceFirstEncoder::new(controller(min_repair), limits)
                        .unwrap()
                        .with_video_plan_grid(grid);
                    let objects = (0..rounds)
                        .map(|sequence| {
                            if varied {
                                let (size, keyframe) = varied_part_size(bytes, sequence);
                                let class: &[u8] = if keyframe { b"video-keyframe" } else { class };
                                object(4_001, sequence, size, class, keyframe)
                            } else {
                                object(4_001, sequence, bytes, class, false)
                            }
                        })
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
                    let mut repair_ns = Vec::new();
                    let mut ours = 0;
                    for object in &objects {
                        let started = std::time::Instant::now();
                        let part = encoder
                            .encode_sources(object, generation, subscription, deadline)
                            .unwrap();
                        let built = std::time::Instant::now();
                        let repair = part.repair_wires().unwrap();
                        sources_ns += built.duration_since(started).as_nanos();
                        repair_ns.push(built.elapsed().as_nanos());
                        ours += part.source_wires().len() + repair.len();
                        std::hint::black_box(repair);
                    }
                    let per = |total: u128| total / u128::from(rounds as u32);
                    let repair_mean = per(repair_ns.iter().sum());
                    repair_ns.sort_unstable();
                    eprintln!(
                        "{label} {mode}, min repair {min_repair}: relay-session {old} ns ({:.1} \
                         symbols); source first: sources {} ns, repair mean {repair_mean} ns \
                         (p50 {} p95 {}), repair ready {} ns ({:.1} symbols, {} plans generated)",
                        symbols as f64 / f64::from(rounds as u32),
                        per(sources_ns),
                        repair_ns[repair_ns.len() / 2],
                        repair_ns[repair_ns.len() * 95 / 100],
                        per(sources_ns) + repair_mean,
                        ours as f64 / f64::from(rounds as u32),
                        encoder.plans().generated(),
                    );
                }
            }
        }
    }

    /// 4k-repair (a): a part's repair follows the measured loss at one part
    /// in ten thousand, between its floor and its cap, where the policy
    /// gave a large part at most 16 symbols whatever the loss.
    #[test]
    fn repair_is_sized_to_the_measured_loss() {
        // An average 247 Mbit/s part (K 1,088): clean, 1 %, 5 %.
        // Clean: the 0.1 % planned loss at 1e-4 (8 symbols), over the
        // floor of 0.5 % (6).
        assert_eq!(measured_repair_symbols(1_088, 0, 5, 30), 8);
        assert_eq!(measured_repair_symbols(1_088, 0, 10, 30), 11, "a floor of 1 %");
        let one = measured_repair_symbols(1_088, 10_000, 5, 30);
        assert!((30..=50).contains(&one), "{one}");
        let five = measured_repair_symbols(1_088, 50_000, 5, 30);
        assert!((110..=150).contains(&five), "{five}");
        // A 6 MB loop-point frame (K 4,736) at 1 %: more symbols, a smaller
        // share; the policy's 16 were 0.3 %.
        let big = measured_repair_symbols(4_736, 10_000, 5, 30);
        assert!(big > 90 && big * 1_000 / 4_736 < one * 1_000 / 1_088, "{big}");
        // Capped.
        assert_eq!(measured_repair_symbols(1_000, 400_000, 5, 30), 300);
        // The residual: at the planned loss, r leaves at most 1e-4.
        let p = planned_loss(10_000);
        assert!(repair_for_loss(1_088, p, 1e-3, 10_000) < one);
    }

    /// By size alone: a part of at least `min_symbols` takes the measured
    /// repair whatever its kind; a smaller one (an audio part's one to
    /// three symbols) keeps the policy.
    #[test]
    fn a_large_part_takes_the_measured_repair_and_a_small_one_keeps_the_policy() {
        let limits = RelayLimits {
            max_extra_repair_symbols: 4_096,
            ..RelayLimits::default()
        };
        let measured = MeasuredRepair {
            floor_permille: 5,
            cap_percent: 30,
            min_symbols: 32,
        };
        let encoder = SourceFirstEncoder::new(controller(0), limits)
            .unwrap()
            .with_measured_repair(Some(measured));
        let reference = SourceFirstEncoder::new(controller(0), limits).unwrap();
        let (generation, subscription, deadline) = ids();
        for (sequence, class) in [(1, &b"video-delta"[..]), (2, &b"audio"[..])] {
            let large = object(4_001, sequence, 600_000, class, false);
            let part = encoder
                .encode_sources_measured(&large, generation, subscription, deadline, None, None, Some(10_000))
                .unwrap();
            let k = part.source_wires().len() as u32;
            assert_eq!(part.repair_count(), measured_repair_symbols(k, 10_000, 5, 30));
            assert!(part.repair_count() > 16);
            assert_eq!(part.repair().unwrap().len() as u32, part.repair_count());
        }
        let small = object(4_001, 3, 3_000, b"video-delta", false);
        assert_eq!(
            encoder
                .encode_sources_measured(&small, generation, subscription, deadline, None, None, Some(10_000))
                .unwrap()
                .repair_count(),
            reference
                .encode_sources(&small, generation, subscription, deadline)
                .unwrap()
                .repair_count()
        );
    }
}
