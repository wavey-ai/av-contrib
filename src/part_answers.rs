//! The contributor's answers to its forwarders' asks (4k-repair).
//!
//! A first forwarder that is still short of a part once its datagrams have
//! stopped asks the contributor on the lane (av-mesh `RLQ1`: "N more
//! symbols of part X"). The contributor keeps the parts it published (the
//! envelopes of the last [`STORE_PARTS`] at most, within a byte budget,
//! `--relay-part-store-mb`, [`DEFAULT_STORE_BYTES`]) and answers with fresh
//! repair symbols of the part, from repair ordinal [`WINDOW`] on (its own
//! repair is far below; a forwarder's answers start at 65,536), each
//! answer's ordinals new; `RLA1` (status, the echoed time, the window) goes
//! first. A probe is answered at once with its time, for the round trip.
//! Wire formats: av-mesh src/fast_plane/part_repair.rs.
//!
//! Bounds: an ask is answered only when it comes from the lane's own
//! forwarder (the socket's configured peer); an answer is at most what the
//! ask lacks (`wanted`, and never more than K plus the whole-part margin);
//! one asker gets at most 2 K + [`PER_PART_EXTRA`] symbols of one part in all,
//! and at most [`ASKER_BYTES_PER_SECOND`] of datagrams a second (a bucket
//! of [`ASKER_BURST_BYTES`]). A limited ask is answered `Busy`.

use crate::source_first::{RepairSource, SourcePart};
use bytes::Bytes;
use std::collections::{HashMap, VecDeque};
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use xxhash_rust::xxh3::xxh3_64;

pub const REQUEST_LEN: usize = 48;
pub const ANSWER_LEN: usize = 48;
/// The contributor's first answer ordinal.
pub const WINDOW: u32 = 1 << 15;
/// The answer ordinals of a part, at most: the contributor's window is
/// [32,768, 65,536), below the forwarders' (from 65,536), so an ESI
/// (K + ordinal) stays far below raptorq's 2^24. A part whose window is
/// spent is answered `Missing`.
pub const WINDOW_LEN: u32 = 1 << 15;
/// The most parts kept: the byte budget is the bound that counts; this
/// one keeps the store small when the parts are (a part costs ~200 bytes
/// beside its envelope).
pub const STORE_PARTS: usize = 1_024;
/// The default budget (`--relay-part-store-mb`): 48 MiB of envelopes holds
/// 1.63 s of parts at 247 Mbit/s and 1.26 s at 320 Mbit/s, past the 1 s
/// media deadline that bounds a useful answer.
pub const DEFAULT_STORE_BYTES: usize = 48 << 20;
/// One asker gets at most 2 K + this many symbols of a part in all.
pub const PER_PART_EXTRA: u32 = 16;
/// One asker's answers: at most this many bytes of datagrams at once...
pub const ASKER_BURST_BYTES: u64 = 8 << 20;
/// ...and this many a second.
pub const ASKER_BYTES_PER_SECOND: u64 = 8 << 20;
/// Askers with a bucket (only a lane's forwarders get one).
const MAX_ASKERS: usize = 64;
/// Symbols one answer carries at most.
pub const MAX_ANSWER_SYMBOLS: u32 = 4_096;

/// A part as av-mesh names it: its stream (tenant, stream and track
/// hashed as av-mesh's media clocks hash them), epoch and object.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct PartKey {
    pub stream: u64,
    pub epoch: u64,
    pub object: u64,
}

impl PartKey {
    pub fn of(key: &media_object::ObjectKey) -> Self {
        let clock = (xxh3_64(key.stream().as_bytes())
            ^ xxh3_64(key.tenant().as_bytes()).rotate_left(32))
            | 1;
        Self {
            stream: clock ^ xxh3_64(key.track().as_bytes()).rotate_left(17),
            epoch: key.epoch(),
            object: key.object(),
        }
    }
}

/// An `RLQ1` ask.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Request {
    pub part: PartKey,
    pub wanted: u16,
    pub probe: bool,
    pub sent_us: u64,
}

impl Request {
    pub fn decode(datagram: &[u8]) -> Option<Self> {
        if datagram.len() != REQUEST_LEN || &datagram[..4] != b"RLQ1" || datagram[4] != 1 {
            return None;
        }
        let u64_at = |at: usize| u64::from_be_bytes(datagram[at..at + 8].try_into().unwrap());
        Some(Self {
            probe: datagram[5] & 1 != 0,
            wanted: u16::from_be_bytes([datagram[6], datagram[7]]),
            sent_us: u64_at(8),
            part: PartKey {
                stream: u64_at(16),
                epoch: u64_at(24),
                object: u64_at(32),
            },
        })
    }
}

/// `RLA1` statuses (av-mesh `AnswerStatus`).
pub const SENDING: u8 = 0;
pub const MISSING: u8 = 1;
pub const PROBE: u8 = 2;
pub const BUSY: u8 = 3;

pub fn encode_answer(request: &Request, status: u8, count: u16) -> [u8; ANSWER_LEN] {
    let mut out = [0_u8; ANSWER_LEN];
    out[..4].copy_from_slice(b"RLA1");
    out[4] = 1;
    out[5] = status;
    out[6..8].copy_from_slice(&count.to_be_bytes());
    out[8..16].copy_from_slice(&request.sent_us.to_be_bytes());
    out[16..24].copy_from_slice(&request.part.stream.to_be_bytes());
    out[24..32].copy_from_slice(&request.part.epoch.to_be_bytes());
    out[32..40].copy_from_slice(&request.part.object.to_be_bytes());
    out[40..44].copy_from_slice(&WINDOW.to_be_bytes());
    out
}

struct Stored {
    key: PartKey,
    part: Arc<RepairSource>,
    /// What `part` holds ([`RepairSource::held_bytes`]).
    held: usize,
    /// The next answer ordinal, past [`WINDOW`]: at most [`WINDOW_LEN`].
    next: u32,
    /// Symbols answered to each asker.
    sent: Vec<(SocketAddr, u32)>,
}

/// One asker's bytes a second.
#[derive(Debug, Clone, Copy)]
struct Bucket {
    bytes: u64,
    at_us: u64,
}

#[derive(Default)]
struct Kept {
    parts: VecDeque<Stored>,
    bytes: usize,
    askers: HashMap<SocketAddr, Bucket>,
}

/// The parts the contributor published lately, and its answers' bounds.
pub struct PartStore {
    kept: Mutex<Kept>,
    budget_bytes: usize,
    max_parts: usize,
    held_bytes: AtomicUsize,
    held_parts: AtomicUsize,
    refused_not_admitted: AtomicU64,
    refused_limited: AtomicU64,
    refused_window_spent: AtomicU64,
}

impl Default for PartStore {
    fn default() -> Self {
        Self::new(DEFAULT_STORE_BYTES, STORE_PARTS)
    }
}

/// The symbols that the ask for a part of `k` source symbols lacks, at
/// most: K, two and K/50 (av-mesh's whole-part answer).
const fn whole(k: u32) -> u32 {
    k + 2 + k / 50
}

impl PartStore {
    /// A store of at most `budget_bytes` ([`RepairSource::held_bytes`]) and
    /// `max_parts` parts.
    pub fn new(budget_bytes: usize, max_parts: usize) -> Self {
        Self {
            kept: Mutex::default(),
            budget_bytes,
            max_parts,
            held_bytes: AtomicUsize::new(0),
            held_parts: AtomicUsize::new(0),
            refused_not_admitted: AtomicU64::new(0),
            refused_limited: AtomicU64::new(0),
            refused_window_spent: AtomicU64::new(0),
        }
    }

    pub const fn budget_bytes(&self) -> usize {
        self.budget_bytes
    }

    /// The bytes the kept parts hold.
    pub fn held_bytes(&self) -> usize {
        self.held_bytes.load(Ordering::Relaxed)
    }

    pub fn held_parts(&self) -> usize {
        self.held_parts.load(Ordering::Relaxed)
    }

    /// Asks not from the lane's forwarder, dropped unanswered.
    pub fn refused_not_admitted(&self) -> u64 {
        self.refused_not_admitted.load(Ordering::Relaxed)
    }

    /// Asks answered `Busy`: the asker had its part's or its second's share.
    pub fn refused_limited(&self) -> u64 {
        self.refused_limited.load(Ordering::Relaxed)
    }

    /// Asks answered `Missing` because the part's answer window
    /// ([`WINDOW_LEN`] ordinals) is spent.
    pub fn refused_window_spent(&self) -> u64 {
        self.refused_window_spent.load(Ordering::Relaxed)
    }

    /// Keep a part of two source symbols or more (not a compact one): its
    /// envelope, which the part shares until its datagrams are gone. The
    /// oldest parts go to keep within the budget.
    pub fn keep(&self, part: &SourcePart) {
        if part.source_symbol_count() < 2 {
            return;
        }
        let Some(part) = part.repair_source() else {
            return;
        };
        let held = part.held_bytes();
        if held > self.budget_bytes || self.max_parts == 0 {
            return;
        }
        let key = PartKey::of(part.key());
        let mut guard = self.kept.lock().unwrap_or_else(|e| e.into_inner());
        let kept = &mut *guard;
        while kept.parts.len() >= self.max_parts || kept.bytes + held > self.budget_bytes {
            let Some(old) = kept.parts.pop_front() else {
                break;
            };
            kept.bytes -= old.held;
        }
        kept.parts.push_back(Stored {
            key,
            part: Arc::new(part),
            held,
            next: 0,
            sent: Vec::new(),
        });
        kept.bytes += held;
        self.held_bytes.store(kept.bytes, Ordering::Relaxed);
        self.held_parts.store(kept.parts.len(), Ordering::Relaxed);
    }

    /// The answer to `request`, which came from `from` on a socket whose
    /// forwarder is `peer`, at `now_us`: the RLA1 datagram and the repair
    /// symbols (none when the part is not kept here, or the asker had its
    /// share). Nothing at all when `from` is not `peer`.
    pub fn answer(
        &self,
        request: &Request,
        from: SocketAddr,
        peer: SocketAddr,
        now_us: u64,
    ) -> (Option<[u8; ANSWER_LEN]>, Vec<Bytes>) {
        if canonical(from) != canonical(peer) {
            self.refused_not_admitted.fetch_add(1, Ordering::Relaxed);
            return (None, Vec::new());
        }
        if request.probe {
            return (Some(encode_answer(request, PROBE, 0)), Vec::new());
        }
        let from = canonical(from);
        let found = {
            let mut guard = self.kept.lock().unwrap_or_else(|e| e.into_inner());
            let kept = &mut *guard;
            let Some(stored) = kept.parts.iter_mut().rev().find(|stored| stored.key == request.part)
            else {
                return (Some(encode_answer(request, MISSING, 0)), Vec::new());
            };
            let window_left = WINDOW_LEN.saturating_sub(stored.next);
            if window_left == 0 {
                self.refused_window_spent.fetch_add(1, Ordering::Relaxed);
                return (Some(encode_answer(request, MISSING, 0)), Vec::new());
            }
            let k = stored.part.source_symbol_count();
            let lacks = if request.wanted == 0 {
                whole(k)
            } else {
                u32::from(request.wanted).min(whole(k))
            }
            .min(MAX_ANSWER_SYMBOLS);
            let sent = match stored.sent.iter().position(|(asker, _)| *asker == from) {
                Some(index) => index,
                None => {
                    stored.sent.push((from, 0));
                    stored.sent.len() - 1
                }
            };
            let part_left = (2 * k + PER_PART_EXTRA).saturating_sub(stored.sent[sent].1);
            if kept.askers.len() >= MAX_ASKERS && !kept.askers.contains_key(&from) {
                kept.askers.clear();
            }
            let bucket = kept.askers.entry(from).or_insert(Bucket {
                bytes: ASKER_BURST_BYTES,
                at_us: now_us,
            });
            let refill = u128::from(now_us.saturating_sub(bucket.at_us))
                * u128::from(ASKER_BYTES_PER_SECOND)
                / 1_000_000;
            bucket.bytes = u64::try_from(u128::from(bucket.bytes) + refill)
                .unwrap_or(u64::MAX)
                .min(ASKER_BURST_BYTES);
            bucket.at_us = bucket.at_us.max(now_us);
            let wire_len = stored.part.wire_len().max(1) as u64;
            let bucket_left = u32::try_from(bucket.bytes / wire_len).unwrap_or(u32::MAX);
            let count = lacks.min(part_left).min(bucket_left).min(window_left);
            if count == 0 {
                self.refused_limited.fetch_add(1, Ordering::Relaxed);
                return (Some(encode_answer(request, BUSY, 0)), Vec::new());
            }
            bucket.bytes -= u64::from(count) * wire_len;
            stored.sent[sent].1 += count;
            let first = WINDOW + stored.next;
            stored.next += count;
            (Arc::clone(&stored.part), first, count)
        };
        let (part, first, count) = found;
        let wires = part.repair_wires(first, count).unwrap_or_default();
        let status = if wires.is_empty() { MISSING } else { SENDING };
        (
            Some(encode_answer(
                request,
                status,
                u16::try_from(wires.len()).unwrap_or(u16::MAX),
            )),
            wires,
        )
    }
}

/// `addr` with an IPv4-mapped IPv6 address as IPv4.
fn canonical(addr: SocketAddr) -> SocketAddr {
    SocketAddr::new(addr.ip().to_canonical(), addr.port())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::source_first::SourceFirstEncoder;
    use media_object::{MediaObject, ObjectKey, ObjectKind};
    use relay_session::{
        AdaptiveFecController, AdaptiveFecPolicy, CongestionConfig, MediaDeadline, RelayLimits,
        SubscriptionId, TopologyGeneration,
    };

    const PEER: &str = "10.0.0.2:4000";

    fn peer() -> SocketAddr {
        PEER.parse().unwrap()
    }

    fn encoder() -> SourceFirstEncoder {
        let policy = AdaptiveFecPolicy {
            min_repair_symbols: 1,
            ..AdaptiveFecPolicy::default()
        };
        SourceFirstEncoder::new(
            AdaptiveFecController::new(policy, CongestionConfig::default()),
            RelayLimits::default(),
        )
        .unwrap()
    }

    /// A video part of `bytes` of payload, object `sequence`.
    fn part(encoder: &SourceFirstEncoder, sequence: u64, bytes: usize) -> SourcePart {
        let payload = (0..bytes)
            .map(|index| (index * 31 + sequence as usize * 7 + 1) as u8)
            .collect::<Vec<_>>();
        let key = ObjectKey::for_payload(
            "needletail",
            "91",
            "video",
            1_784_151_600_000_001,
            0,
            sequence,
            1,
            &payload,
        )
        .unwrap();
        let object = MediaObject::builder(key, ObjectKind::Media, payload)
            .with_metadata("scheduler-class", b"video".to_vec())
            .build()
            .unwrap();
        encoder
            .encode_sources(
                &object,
                TopologyGeneration::new(7).unwrap(),
                SubscriptionId::new(9).unwrap(),
                MediaDeadline::from_micros(4_000_000_000_000_000),
            )
            .unwrap()
    }

    fn ask(part: &SourcePart, wanted: u16, probe: bool) -> Request {
        Request {
            part: PartKey::of(&part.announcement.key),
            wanted,
            probe,
            sent_us: 777,
        }
    }

    fn status(answer: Option<[u8; ANSWER_LEN]>) -> u8 {
        answer.expect("an RLA1")[5]
    }

    /// The store keeps parts within its byte budget and its part cap, the
    /// oldest going first, and counts what the kept parts hold: the
    /// envelope (shared with the part, not a copy) and a little beside it.
    #[test]
    fn the_store_is_bounded_in_bytes_and_in_parts() {
        let encoder = encoder();
        let parts = (0..6).map(|index| part(&encoder, index, 100_000)).collect::<Vec<_>>();
        let held = parts[0].repair_source().unwrap().held_bytes();
        assert!(held >= parts[0].envelope().len() && held <= parts[0].envelope().len() + 4_096);
        let store = PartStore::new(3 * held + held / 2, 1_024);
        for part in &parts {
            store.keep(part);
        }
        assert_eq!(store.held_parts(), 3);
        assert!(store.held_bytes() <= store.budget_bytes());
        let expected = parts[3..]
            .iter()
            .map(|part| part.repair_source().unwrap().held_bytes())
            .sum::<usize>();
        assert_eq!(store.held_bytes(), expected);
        let (answer, wires) = store.answer(&ask(&parts[2], 4, false), peer(), peer(), 0);
        assert_eq!((status(answer), wires.len()), (MISSING, 0), "the oldest went");
        let (answer, wires) = store.answer(&ask(&parts[5], 4, false), peer(), peer(), 0);
        assert_eq!((status(answer), wires.len()), (SENDING, 4));

        let store = PartStore::new(64 << 20, 2);
        for part in &parts {
            store.keep(part);
        }
        assert_eq!(store.held_parts(), 2);
        // A part larger than the whole budget is not kept.
        let store = PartStore::new(held / 2, 1_024);
        store.keep(&parts[0]);
        assert_eq!((store.held_parts(), store.held_bytes()), (0, 0));
    }

    /// An ask from any address but the lane's forwarder is dropped
    /// unanswered (a probe too) and counted.
    #[test]
    fn an_ask_from_another_address_is_not_answered() {
        let encoder = encoder();
        let part = part(&encoder, 1, 50_000);
        let store = PartStore::default();
        store.keep(&part);
        let stranger: SocketAddr = "10.0.0.9:4000".parse().unwrap();
        let other_port: SocketAddr = "10.0.0.2:4001".parse().unwrap();
        for from in [stranger, other_port] {
            for probe in [false, true] {
                let (answer, wires) = store.answer(&ask(&part, 0, probe), from, peer(), 0);
                assert!(answer.is_none() && wires.is_empty());
            }
        }
        assert_eq!(store.refused_not_admitted(), 4);
        // The forwarder, also as an IPv4-mapped address, is answered.
        let mapped: SocketAddr = "[::ffff:10.0.0.2]:4000".parse().unwrap();
        let (answer, wires) = store.answer(&ask(&part, 3, false), mapped, peer(), 0);
        assert_eq!((status(answer), wires.len()), (SENDING, 3));
    }

    /// An answer is at most what the ask lacks: `wanted`, and never more
    /// than K and the whole-part margin; a whole-part ask gets that.
    #[test]
    fn an_answer_is_at_most_what_the_ask_lacks() {
        let encoder = encoder();
        let part = part(&encoder, 2, 60_000);
        let k = part.source_symbol_count();
        let store = PartStore::default();
        store.keep(&part);
        let (answer, wires) = store.answer(&ask(&part, 7, false), peer(), peer(), 0);
        assert_eq!((status(answer), wires.len()), (SENDING, 7));
        let other: SocketAddr = "10.0.0.3:4000".parse().unwrap();
        let (_, wires) = store.answer(&ask(&part, u16::MAX, false), other, other, 0);
        assert_eq!(wires.len() as u32, whole(k), "wanted beyond K");
        let third: SocketAddr = "10.0.0.4:4000".parse().unwrap();
        let (answer, wires) = store.answer(&ask(&part, 0, false), third, third, 0);
        assert_eq!(wires.len() as u32, whole(k), "whole");
        assert_eq!(u16::from_be_bytes([answer.unwrap()[6], answer.unwrap()[7]]) as u32, whole(k));
    }

    /// One asker gets at most 2 K + 16 symbols of a part in all, then
    /// `Busy`; another asker's share of the part is its own.
    #[test]
    fn one_asker_gets_at_most_2k_plus_16_symbols_of_a_part() {
        let encoder = encoder();
        let part = part(&encoder, 3, 40_000);
        let k = part.source_symbol_count();
        let store = PartStore::default();
        store.keep(&part);
        let mut total = 0;
        let mut ordinals = std::collections::HashSet::new();
        for _ in 0..4 {
            let (answer, wires) = store.answer(&ask(&part, 0, false), peer(), peer(), 0);
            if wires.is_empty() {
                assert_eq!(status(answer), BUSY);
                break;
            }
            total += wires.len() as u32;
            for wire in &wires {
                assert!(ordinals.insert(wire.clone()), "every answer's symbols are new");
            }
        }
        assert_eq!(total, 2 * k + PER_PART_EXTRA);
        assert_eq!(store.refused_limited(), 1);
        let other: SocketAddr = "10.0.0.3:4000".parse().unwrap();
        let (answer, wires) = store.answer(&ask(&part, 0, false), other, other, 0);
        assert_eq!((status(answer), wires.len() as u32), (SENDING, whole(k)));
    }

    /// A part's answers take ordinals from 32,768 up to 65,536 at most (the
    /// forwarders' windows start there), whoever asks: then `Missing`,
    /// counted, never a wrap or an ordinal past the window.
    #[test]
    fn a_parts_answers_stay_in_the_contributors_window() {
        use raptorq_datagram_fec::HEADER_LEN;
        let encoder = encoder();
        let part = part(&encoder, 4, 30_000);
        let k = part.source_symbol_count();
        let store = PartStore::default();
        store.keep(&part);
        let limits = RelayLimits::default();
        let mut total = 0_u32;
        let mut spent = false;
        for asker in 0..2_000_u32 {
            let from = SocketAddr::from(([10, 1, (asker >> 8) as u8, asker as u8], 4000));
            let (answer, wires) = store.answer(&ask(&part, 0, false), from, from, 0);
            if wires.is_empty() {
                assert_eq!(status(answer), MISSING);
                spent = true;
                break;
            }
            for wire in &wires {
                let symbol = relay_session::decode_datagram(wire, limits).unwrap();
                let id = &symbol.fec_datagram[HEADER_LEN..HEADER_LEN + 4];
                let esi = u32::from_be_bytes([0, id[1], id[2], id[3]]);
                assert!(
                    (k + WINDOW..k + WINDOW + WINDOW_LEN).contains(&esi),
                    "ESI {esi} outside the window"
                );
            }
            total += wires.len() as u32;
        }
        assert!(spent, "the window is spent");
        assert_eq!(total, WINDOW_LEN);
        assert_eq!(store.refused_window_spent(), 1);
    }

    /// One asker's answers are held to 8 MiB at once and 8 MiB a second.
    #[test]
    fn one_asker_is_held_to_8_mib_a_second() {
        let encoder = encoder();
        let parts = (0..16).map(|index| part(&encoder, 10 + index, 1_000_000)).collect::<Vec<_>>();
        let store = PartStore::default();
        for part in &parts {
            store.keep(part);
        }
        let mut sent = 0_usize;
        let mut asked = 0;
        for part in &parts {
            asked += 1;
            let (answer, wires) = store.answer(&ask(part, 0, false), peer(), peer(), 5_000_000);
            if wires.is_empty() {
                assert_eq!(status(answer), BUSY);
                break;
            }
            sent += wires.iter().map(Bytes::len).sum::<usize>();
        }
        assert!(asked < parts.len(), "the bucket ran out");
        assert!(sent <= ASKER_BURST_BYTES as usize, "{sent}");
        assert!(sent + 1_200_000 >= ASKER_BURST_BYTES as usize, "{sent}");
        assert_eq!(store.refused_limited(), 1);
        // Half a second later: about 4 MiB more.
        let mut later = 0_usize;
        for part in &parts[asked - 1..] {
            let (_, wires) = store.answer(&ask(part, 0, false), peer(), peer(), 5_500_000);
            later += wires.iter().map(Bytes::len).sum::<usize>();
        }
        let half = (ASKER_BYTES_PER_SECOND / 2) as usize;
        assert!(later <= half + 1_200_000 && later + 1_200_000 >= half, "{later}");
    }
}
