//! The contributor's answers to its forwarders' asks (4k-repair).
//!
//! A first forwarder that is still short of a part once its datagrams have
//! stopped asks the contributor on the lane (av-mesh `RLQ1`: "N more
//! symbols of part X"). The contributor keeps the parts it published (the
//! last [`STORE_PARTS`], at most [`STORE_BYTES`] of envelopes) and answers
//! with fresh repair symbols of the part, from repair ordinal [`WINDOW`]
//! on (its own repair is far below; a forwarder's answers start at 65,536),
//! each answer's ordinals new; `RLA1` (status, the echoed time, the window)
//! goes first. A probe is answered at once with its time, for the round
//! trip. Wire formats: av-mesh src/fast_plane/part_repair.rs.

use crate::source_first::SourcePart;
use bytes::Bytes;
use std::collections::VecDeque;
use std::sync::{Arc, Mutex};
use xxhash_rust::xxh3::xxh3_64;

pub const REQUEST_LEN: usize = 48;
pub const ANSWER_LEN: usize = 48;
/// The contributor's first answer ordinal.
pub const WINDOW: u32 = 1 << 15;
pub const STORE_PARTS: usize = 128;
pub const STORE_BYTES: usize = 160 << 20;
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
    part: Arc<SourcePart>,
    /// The next answer ordinal, past [`WINDOW`].
    next: u32,
}

/// The parts the contributor published lately.
#[derive(Default)]
pub struct PartStore {
    parts: Mutex<(VecDeque<Stored>, usize)>,
}

impl PartStore {
    /// Keep a part of two source symbols or more.
    pub fn keep(&self, part: Arc<SourcePart>) {
        if part.is_compact() || part.source_symbol_count() < 2 {
            return;
        }
        let key = PartKey::of(&part.announcement.key);
        let bytes = part.envelope().len();
        let mut guard = self.parts.lock().unwrap_or_else(|e| e.into_inner());
        let (parts, total) = &mut *guard;
        parts.push_back(Stored { key, part, next: 0 });
        *total += bytes;
        while parts.len() > STORE_PARTS || *total > STORE_BYTES {
            let Some(old) = parts.pop_front() else {
                break;
            };
            *total -= old.part.envelope().len();
        }
    }

    /// The answer to `request`: the RLA1 datagram and the repair symbols
    /// (none when the part is not kept here).
    pub fn answer(&self, request: &Request) -> (Option<[u8; ANSWER_LEN]>, Vec<Bytes>) {
        if request.probe {
            return (Some(encode_answer(request, PROBE, 0)), Vec::new());
        }
        let found = {
            let mut guard = self.parts.lock().unwrap_or_else(|e| e.into_inner());
            guard.0.iter_mut().rev().find(|stored| stored.key == request.part).map(|stored| {
                let k = stored.part.source_symbol_count();
                let wanted = if request.wanted == 0 {
                    k + 2 + k / 50
                } else {
                    u32::from(request.wanted)
                }
                .min(MAX_ANSWER_SYMBOLS);
                let first = WINDOW + stored.next;
                stored.next += wanted;
                (Arc::clone(&stored.part), first, wanted)
            })
        };
        let Some((part, first, wanted)) = found else {
            return (Some(encode_answer(request, MISSING, 0)), Vec::new());
        };
        let wires = part.extra_repair_wires(first, wanted).unwrap_or_default();
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
