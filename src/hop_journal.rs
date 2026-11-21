//! The hop journal: an in-memory ring of part events that every node keeps
//! and serves as NDJSON at `GET /api/hops`.
//!
//! One line per event:
//!
//! ```text
//! {"node":"<node id>","stage":"<stage>","stream":<u64>,"seq":<u64>,"t_us":<unix µs>,
//!  "peer":"<addr>"?, "epoch":<u64>?, "n":<u32>?}
//! ```
//!
//! `seq` is the part sequence: the RelaySession object sequence, the
//! ChunkCache slot id and the LL-HLS part number. Every node's events join on
//! `(stream, seq)`. `t_us` is CLOCK_REALTIME in microseconds, taken as close
//! to the event as the code can.
//!
//! Recording takes a `std::sync::Mutex` for a few instructions and writes one
//! fixed slot of a ring allocated up front: no allocation, no logging, no
//! await. A journal of capacity 0 records nothing. A query copies the ring a
//! few thousand events at a time and lets go of the lock between copies, so
//! a reader never holds up the recording paths for longer than one copy.

use serde::Serialize;
use std::collections::VecDeque;
use std::net::SocketAddr;
use std::sync::{Mutex, OnceLock, PoisonError};
use std::time::{SystemTime, UNIX_EPOCH};

/// Default ring capacity, in events (`--hop-journal-capacity`).
pub const DEFAULT_HOP_JOURNAL_CAPACITY: usize = 262_144;

/// Content type of `GET /api/hops`.
pub const HOP_JOURNAL_CONTENT_TYPE: &str = "application/x-ndjson";

/// Path of the journal on a node's HTTP port.
pub const HOP_JOURNAL_PATH: &str = "/api/hops";

/// Events a query copies under one hold of the ring lock: about 350 KB, a
/// few tens of microseconds.
const QUERY_CHUNK_EVENTS: usize = 4_096;

/// Contributor stages of a part, in the order a part passes them.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum HopStage {
    /// First AEP1 datagram of the epoch(s) that make the part arrived.
    SrcFirst,
    /// Last source datagram used by the part arrived.
    SrcLast,
    /// AEP1 group complete (systematic or RaptorQ).
    Recovered,
    /// Part cut by the segmenter.
    Packaged,
    /// Handed to the publication shard queue.
    Enqueued,
    /// The publication worker took it.
    Dequeued,
    /// RaptorQ encode of the object finished.
    Encoded,
    /// Last source symbol of the part handed to the kernel on the primary lane.
    Sent,
    /// The same for the secondary lane.
    SentSecondary,
    /// The same for a direct lane to a distributor of the contributor's
    /// region (`peer` is the distributor).
    SentDirect,
}

impl HopStage {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::SrcFirst => "src_first",
            Self::SrcLast => "src_last",
            Self::Recovered => "recovered",
            Self::Packaged => "packaged",
            Self::Enqueued => "enqueued",
            Self::Dequeued => "dequeued",
            Self::Encoded => "encoded",
            Self::Sent => "sent",
            Self::SentSecondary => "sent_secondary",
            Self::SentDirect => "sent_direct",
        }
    }
}

/// One event of the journal.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HopEvent {
    pub stage: HopStage,
    pub stream: u64,
    pub seq: u64,
    pub t_us: u64,
    pub peer: Option<SocketAddr>,
    pub epoch: Option<u64>,
    pub n: Option<u32>,
}

impl HopEvent {
    pub fn new(stage: HopStage, stream: u64, seq: u64, t_us: u64) -> Self {
        Self {
            stage,
            stream,
            seq,
            t_us,
            peer: None,
            epoch: None,
            n: None,
        }
    }

    pub fn with_epoch(mut self, epoch: Option<u64>) -> Self {
        self.epoch = epoch;
        self
    }

    pub fn with_peer(mut self, peer: Option<SocketAddr>) -> Self {
        self.peer = peer;
        self
    }
}

/// Source timing of a part, gathered before the part has its sequence. The
/// part carries it to the publisher, which journals it under the sequence.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct PartSourceTimes {
    /// First AEP1 epoch of the part: the epoch of `src_first`.
    pub first_epoch: Option<u64>,
    /// Last AEP1 epoch of the part, whose last sample ends it: the epoch of
    /// `src_last`, `recovered` and `packaged`.
    pub last_epoch: Option<u64>,
    /// First AEP1 datagram of the part's first epoch arrived.
    pub src_first_us: Option<u64>,
    /// The datagram that completed the part's last epoch arrived.
    pub src_last_us: Option<u64>,
    /// The part's last AEP1 group was recovered.
    pub recovered_us: Option<u64>,
}

impl PartSourceTimes {
    /// The times of a part of the single AEP1 epoch `epoch`.
    pub fn of_epoch(epoch: u64) -> Self {
        Self {
            first_epoch: Some(epoch),
            last_epoch: Some(epoch),
            ..Self::default()
        }
    }

    /// Add a later unit of the same part.
    pub fn merge(&mut self, later: &PartSourceTimes) {
        self.first_epoch = min_option(self.first_epoch, later.first_epoch);
        self.last_epoch = self.last_epoch.max(later.last_epoch);
        self.src_first_us = min_option(self.src_first_us, later.src_first_us);
        self.src_last_us = self.src_last_us.max(later.src_last_us);
        self.recovered_us = self.recovered_us.max(later.recovered_us);
    }

    pub fn is_empty(&self) -> bool {
        *self == Self::default()
    }

    /// The source events of part `seq` of `stream`. `src_first` carries the
    /// part's first epoch; `src_last` and `recovered` its last, whose last
    /// sample ends the part.
    pub fn events(&self, stream: u64, seq: u64) -> impl Iterator<Item = HopEvent> + '_ {
        [
            (HopStage::SrcFirst, self.src_first_us, self.first_epoch),
            (HopStage::SrcLast, self.src_last_us, self.last_epoch),
            (HopStage::Recovered, self.recovered_us, self.last_epoch),
        ]
        .into_iter()
        .filter_map(move |(stage, t_us, epoch)| {
            t_us.map(|t_us| HopEvent::new(stage, stream, seq, t_us).with_epoch(epoch))
        })
    }
}

fn min_option(a: Option<u64>, b: Option<u64>) -> Option<u64> {
    match (a, b) {
        (Some(a), Some(b)) => Some(a.min(b)),
        (a, b) => a.or(b),
    }
}

/// The ring of one node.
pub struct HopJournal {
    node: String,
    capacity: usize,
    ring: Mutex<Ring>,
}

struct Ring {
    events: VecDeque<HopEvent>,
    /// Events ever recorded. The oldest event in the ring is number
    /// `recorded - events.len()`.
    recorded: u64,
}

impl Ring {
    fn oldest(&self) -> u64 {
        self.recorded - self.events.len() as u64
    }
}

impl HopJournal {
    pub fn new(node: impl Into<String>, capacity: usize) -> Self {
        Self {
            node: node.into(),
            capacity,
            ring: Mutex::new(Ring {
                events: VecDeque::with_capacity(capacity),
                recorded: 0,
            }),
        }
    }

    pub fn node(&self) -> &str {
        &self.node
    }

    pub fn capacity(&self) -> usize {
        self.capacity
    }

    pub fn enabled(&self) -> bool {
        self.capacity > 0
    }

    pub fn record(&self, event: HopEvent) {
        if self.capacity == 0 {
            return;
        }
        let mut ring = self.ring.lock().unwrap_or_else(PoisonError::into_inner);
        if ring.events.len() == self.capacity {
            ring.events.pop_front();
        }
        ring.events.push_back(event);
        ring.recorded += 1;
    }

    /// Events at or after `since_us`, of `stream` when given, oldest first.
    pub fn query(&self, since_us: Option<u64>, stream: Option<u64>) -> Vec<HopEvent> {
        self.query_in_chunks(since_us, stream, QUERY_CHUNK_EVENTS, || {})
    }

    /// `query`, copying at most `chunk` events per hold of the lock and
    /// calling `between` with the lock released after each copy. The events
    /// are those in the ring when the query starts that are still there when
    /// their chunk is copied; later events are left for the next query.
    fn query_in_chunks(
        &self,
        since_us: Option<u64>,
        stream: Option<u64>,
        chunk: usize,
        mut between: impl FnMut(),
    ) -> Vec<HopEvent> {
        let chunk = chunk.max(1);
        let mut events = Vec::new();
        let mut next = 0_u64;
        let mut stop = None;
        loop {
            // Room for a whole chunk, so that nothing allocates under the lock.
            events.reserve(chunk);
            {
                let ring = self.ring.lock().unwrap_or_else(PoisonError::into_inner);
                let stop = *stop.get_or_insert(ring.recorded);
                let oldest = ring.oldest();
                let start = next.max(oldest);
                let end = stop.min(start.saturating_add(chunk as u64));
                if start < end {
                    let (first, second) = ring.events.as_slices();
                    let from = (start - oldest) as usize;
                    let to = (end - oldest) as usize;
                    let copy = |slice: &[HopEvent], offset: usize, events: &mut Vec<HopEvent>| {
                        let lo = from.saturating_sub(offset).min(slice.len());
                        let hi = to.saturating_sub(offset).min(slice.len());
                        events.extend(slice[lo..hi].iter().copied().filter(|event| {
                            since_us.is_none_or(|since| event.t_us >= since)
                                && stream.is_none_or(|stream| event.stream == stream)
                        }));
                    };
                    copy(first, 0, &mut events);
                    copy(second, first.len(), &mut events);
                }
                next = end;
                if next >= stop {
                    break;
                }
            }
            between();
        }
        // Source events are journalled when the part gets its sequence,
        // after the events they describe.
        events.sort_by_key(|event| event.t_us);
        events
    }

    /// The events as NDJSON, one line each.
    pub fn to_ndjson(&self, events: &[HopEvent]) -> Vec<u8> {
        let mut out = Vec::with_capacity(events.len() * 112);
        for event in events {
            let line = HopLine {
                node: &self.node,
                stage: event.stage.as_str(),
                stream: event.stream,
                seq: event.seq,
                t_us: event.t_us,
                peer: event.peer.map(|peer| peer.to_string()),
                epoch: event.epoch,
                n: event.n,
            };
            if serde_json::to_writer(&mut out, &line).is_ok() {
                out.push(b'\n');
            }
        }
        out
    }

    /// Answer `GET /api/hops?since_us=<t>&stream=<id>` (both optional).
    pub fn respond(&self, query: Option<&str>) -> Result<Vec<u8>, String> {
        let (since_us, stream) = parse_hop_query(query)?;
        Ok(self.to_ndjson(&self.query(since_us, stream)))
    }
}

#[derive(Serialize)]
struct HopLine<'a> {
    node: &'a str,
    stage: &'static str,
    stream: u64,
    seq: u64,
    t_us: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    peer: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    epoch: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    n: Option<u32>,
}

/// `since_us` and `stream` of a `/api/hops` query.
pub fn parse_hop_query(query: Option<&str>) -> Result<(Option<u64>, Option<u64>), String> {
    let mut since_us = None;
    let mut stream = None;
    for (key, value) in form_urlencoded::parse(query.unwrap_or_default().as_bytes()) {
        let parsed = || {
            value
                .parse::<u64>()
                .map_err(|error| format!("invalid {key} `{value}`: {error}"))
        };
        match key.as_ref() {
            "since_us" => since_us = Some(parsed()?),
            "stream" => stream = Some(parsed()?),
            _ => {}
        }
    }
    Ok((since_us, stream))
}

static GLOBAL: OnceLock<HopJournal> = OnceLock::new();

/// Install the process journal. The first call wins.
pub fn install(journal: HopJournal) -> &'static HopJournal {
    let _ = GLOBAL.set(journal);
    GLOBAL.get().expect("hop journal installed")
}

/// The process journal, if one was installed and is enabled.
pub fn global() -> Option<&'static HopJournal> {
    GLOBAL.get().filter(|journal| journal.enabled())
}

/// Record into the process journal, if there is one.
#[inline]
pub fn record(event: HopEvent) {
    if let Some(journal) = global() {
        journal.record(event);
    }
}

/// CLOCK_REALTIME in microseconds.
#[inline]
pub fn now_unix_us() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_micros().min(u128::from(u64::MAX)) as u64)
        .unwrap_or(0)
}

/// Convert a Unix-nanosecond stamp to microseconds.
#[inline]
pub fn unix_ns_to_us(unix_ns: i64) -> u64 {
    u64::try_from(unix_ns / 1_000).unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ring_keeps_the_newest_events_without_growing() {
        let journal = HopJournal::new("contrib-lon", 3);
        let allocated = journal.ring.lock().unwrap().events.capacity();
        assert!(allocated >= 3);
        for seq in 0..5 {
            journal.record(HopEvent::new(HopStage::Enqueued, 4001, seq, 100 + seq));
        }
        let events = journal.query(None, None);
        assert_eq!(
            events.iter().map(|event| event.seq).collect::<Vec<_>>(),
            vec![2, 3, 4]
        );
        // The ring was allocated once and never grew.
        assert_eq!(journal.ring.lock().unwrap().events.capacity(), allocated);

        let disabled = HopJournal::new("contrib-lon", 0);
        disabled.record(HopEvent::new(HopStage::Sent, 1, 1, 1));
        assert!(disabled.query(None, None).is_empty());
        assert!(!disabled.enabled());
    }

    #[test]
    fn query_filters_by_time_and_stream_and_returns_oldest_first() {
        let journal = HopJournal::new("contrib-lon", 16);
        journal.record(HopEvent::new(HopStage::Enqueued, 4001, 7, 1_000));
        // Source events of the same part are journalled later but are older.
        journal.record(HopEvent::new(HopStage::SrcFirst, 4001, 7, 400).with_epoch(Some(77)));
        journal.record(HopEvent::new(HopStage::Enqueued, 2001, 7, 1_100));
        journal.record(HopEvent::new(HopStage::Sent, 4001, 7, 1_200));

        let all = journal.query(None, Some(4001));
        assert_eq!(
            all.iter().map(|event| event.stage).collect::<Vec<_>>(),
            vec![HopStage::SrcFirst, HopStage::Enqueued, HopStage::Sent]
        );
        let since = journal.query(Some(1_100), None);
        assert_eq!(
            since.iter().map(|event| event.t_us).collect::<Vec<_>>(),
            vec![1_100, 1_200]
        );
    }

    #[test]
    fn query_copies_the_ring_in_chunks_while_recording_goes_on() {
        let journal = HopJournal::new("contrib-lon", 10);
        for seq in 0..10 {
            journal.record(HopEvent::new(HopStage::Sent, 4001, seq, 1_000 + seq));
        }
        // Between chunks of 3 the recording paths run and push 2 events, each
        // evicting the oldest. The query returns what was in the ring when
        // it started and was still there when its chunk was read, once each,
        // and leaves the events recorded during the query for the next one.
        let mut next_seq = 10;
        let mut pauses = 0;
        let events = journal.query_in_chunks(None, Some(4001), 3, || {
            pauses += 1;
            for _ in 0..2 {
                journal.record(HopEvent::new(
                    HopStage::Sent,
                    4001,
                    next_seq,
                    1_000 + next_seq,
                ));
                next_seq += 1;
            }
        });
        let seqs = events.iter().map(|event| event.seq).collect::<Vec<_>>();
        // Chunk 1 reads 0..3; 10, 11 evict 0, 1. Chunk 2 reads 3..6; 12, 13
        // evict 2, 3. Chunk 3 reads 6..9; 14, 15 evict 4, 5. Chunk 4 reads 9.
        assert_eq!(seqs, vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9]);
        assert_eq!(pauses, 3);

        // When the recording evicts events before their chunk is read, they
        // are skipped, not duplicated or read past the end. After 0 is read,
        // 4, 5 and 6 evict 0, 1 and 2; 3 is read and the query stops there.
        let journal = HopJournal::new("contrib-lon", 4);
        for seq in 0..4 {
            journal.record(HopEvent::new(HopStage::Sent, 1, seq, seq));
        }
        let mut next_seq = 4;
        let events = journal.query_in_chunks(None, None, 1, || {
            for _ in 0..3 {
                journal.record(HopEvent::new(HopStage::Sent, 1, next_seq, next_seq));
                next_seq += 1;
            }
        });
        assert_eq!(
            events.iter().map(|event| event.seq).collect::<Vec<_>>(),
            vec![0, 3]
        );
        // The whole query with no interleaving sees the latest ring.
        assert_eq!(journal.query(None, None).len(), 4);
    }

    #[test]
    fn ndjson_lines_have_the_shared_format() {
        let journal = HopJournal::new("contrib-\"lon\"", 16);
        journal.record(
            HopEvent::new(HopStage::SrcFirst, 4001, 9, 1_700_000_000_000_000).with_epoch(Some(42)),
        );
        journal.record(
            HopEvent::new(HopStage::SentSecondary, 4001, 9, 1_700_000_000_000_100)
                .with_peer(Some("192.0.2.1:27200".parse().unwrap())),
        );
        let body = journal.respond(Some("stream=4001&since_us=0")).unwrap();
        let text = String::from_utf8(body).unwrap();
        let lines = text.lines().collect::<Vec<_>>();
        assert_eq!(lines.len(), 2);
        assert_eq!(
            lines[0],
            r#"{"node":"contrib-\"lon\"","stage":"src_first","stream":4001,"seq":9,"t_us":1700000000000000,"epoch":42}"#
        );
        assert_eq!(
            lines[1],
            r#"{"node":"contrib-\"lon\"","stage":"sent_secondary","stream":4001,"seq":9,"t_us":1700000000000100,"peer":"192.0.2.1:27200"}"#
        );
        assert!(text.ends_with('\n'));
        assert!(journal.respond(Some("stream=abc")).is_err());
        assert_eq!(parse_hop_query(None).unwrap(), (None, None));
    }

    #[test]
    fn source_times_of_a_part_span_its_units() {
        let mut part = PartSourceTimes {
            src_first_us: Some(100),
            src_last_us: Some(120),
            recovered_us: Some(125),
            ..PartSourceTimes::of_epoch(10)
        };
        part.merge(&PartSourceTimes {
            src_first_us: Some(105),
            src_last_us: Some(170),
            recovered_us: Some(171),
            ..PartSourceTimes::of_epoch(11)
        });
        assert_eq!(
            part,
            PartSourceTimes {
                first_epoch: Some(10),
                last_epoch: Some(11),
                src_first_us: Some(100),
                src_last_us: Some(170),
                recovered_us: Some(171),
            }
        );
        // src_first names the first epoch; src_last and recovered the last,
        // whose last sample ends the part.
        let events = part.events(4001, 3).collect::<Vec<_>>();
        assert_eq!(
            events
                .iter()
                .map(|event| (event.stage, event.epoch, event.seq))
                .collect::<Vec<_>>(),
            vec![
                (HopStage::SrcFirst, Some(10), 3),
                (HopStage::SrcLast, Some(11), 3),
                (HopStage::Recovered, Some(11), 3),
            ]
        );
        assert!(PartSourceTimes::default().is_empty());
        assert_eq!(PartSourceTimes::default().events(1, 1).count(), 0);
    }
}
