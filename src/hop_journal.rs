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
//! await. A journal of capacity 0 records nothing.

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
    /// First AEP1 epoch of the part.
    pub epoch: Option<u64>,
    /// First AEP1 datagram of the part's first epoch arrived.
    pub src_first_us: Option<u64>,
    /// The datagram that completed the part's last epoch arrived.
    pub src_last_us: Option<u64>,
    /// The part's last AEP1 group was recovered.
    pub recovered_us: Option<u64>,
}

impl PartSourceTimes {
    /// Add a later unit of the same part.
    pub fn merge(&mut self, later: &PartSourceTimes) {
        self.epoch = self.epoch.or(later.epoch);
        self.src_first_us = min_option(self.src_first_us, later.src_first_us);
        self.src_last_us = self.src_last_us.max(later.src_last_us);
        self.recovered_us = self.recovered_us.max(later.recovered_us);
    }

    pub fn is_empty(&self) -> bool {
        *self == Self::default()
    }

    /// The source events of part `seq` of `stream`.
    pub fn events(&self, stream: u64, seq: u64) -> impl Iterator<Item = HopEvent> + '_ {
        [
            (HopStage::SrcFirst, self.src_first_us),
            (HopStage::SrcLast, self.src_last_us),
            (HopStage::Recovered, self.recovered_us),
        ]
        .into_iter()
        .filter_map(move |(stage, t_us)| {
            t_us.map(|t_us| HopEvent::new(stage, stream, seq, t_us).with_epoch(self.epoch))
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
    ring: Mutex<VecDeque<HopEvent>>,
}

impl HopJournal {
    pub fn new(node: impl Into<String>, capacity: usize) -> Self {
        Self {
            node: node.into(),
            capacity,
            ring: Mutex::new(VecDeque::with_capacity(capacity)),
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
        if ring.len() == self.capacity {
            ring.pop_front();
        }
        ring.push_back(event);
    }

    /// Events at or after `since_us`, of `stream` when given, oldest first.
    pub fn query(&self, since_us: Option<u64>, stream: Option<u64>) -> Vec<HopEvent> {
        let mut events = {
            let ring = self.ring.lock().unwrap_or_else(PoisonError::into_inner);
            ring.iter()
                .filter(|event| since_us.is_none_or(|since| event.t_us >= since))
                .filter(|event| stream.is_none_or(|stream| event.stream == stream))
                .copied()
                .collect::<Vec<_>>()
        };
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
        let allocated = journal.ring.lock().unwrap().capacity();
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
        assert_eq!(journal.ring.lock().unwrap().capacity(), allocated);

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
            epoch: Some(10),
            src_first_us: Some(100),
            src_last_us: Some(120),
            recovered_us: Some(125),
        };
        part.merge(&PartSourceTimes {
            epoch: Some(11),
            src_first_us: Some(105),
            src_last_us: Some(170),
            recovered_us: Some(171),
        });
        assert_eq!(
            part,
            PartSourceTimes {
                epoch: Some(10),
                src_first_us: Some(100),
                src_last_us: Some(170),
                recovered_us: Some(171),
            }
        );
        let events = part.events(4001, 3).collect::<Vec<_>>();
        assert_eq!(events.len(), 3);
        assert!(events
            .iter()
            .all(|event| event.epoch == Some(10) && event.seq == 3));
        assert!(PartSourceTimes::default().is_empty());
        assert_eq!(PartSourceTimes::default().events(1, 1).count(), 0);
    }
}
