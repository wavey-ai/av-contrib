//! What names a contributor's opaque audio parts, and how two contributors
//! show that they publish the same bytes (A 2.4, 2.6).
//!
//! The first forwarders take the first copy of a part under its key and
//! compare nothing else: the key must say what produced the bytes. The
//! contributor runs a self-test through its real encode path at start, and
//! takes three digests of what it made:
//!
//! - `numbering`: per part, its stream, key epoch, number, playlist place,
//!   source time, deadline and size;
//! - `envelope`: each part's canonical object (`media_object::encode`);
//! - `wire`: every datagram (RLS1 sources and repair, RLC1 and copies).
//!
//! The fingerprint is the digest of the three. The object version of every
//! opaque audio part carries its low 16 bits ([`object_version`]), so two
//! contributors whose bytes could differ never share a key. The three
//! digests say what kind of change a roll would be: the same numbering with
//! another envelope or wire can travel beside the old one; another
//! numbering cannot.
//!
//! [`WindowTable`]: what is measured while running. Per stream, windows of
//! 256 part numbers, each with the count of parts published in it and the
//! XOR of their envelopes' digests: two contributors' windows of equal
//! `first_part` and `present` must have equal `xor`.

use serde::Serialize;
use std::sync::atomic::{fence, AtomicU64, Ordering};
use xxhash_rust::xxh3::{xxh3_64, Xxh3};

/// The high half of an opaque audio part's object version: the part is
/// named from its source (A 2.2).
pub const OPAQUE_AUDIO_FORMAT: u32 = 2;

/// The object version of opaque audio parts made by a contributor of
/// `fingerprint`: [`OPAQUE_AUDIO_FORMAT`] above its low 16 bits.
pub const fn object_version(fingerprint: u64) -> u32 {
    (OPAQUE_AUDIO_FORMAT << 16) | (fingerprint & 0xffff) as u32
}

/// `value` as 16 hex digits.
pub fn hex16(value: u64) -> String {
    format!("{value:016x}")
}

/// The three running digests of a self-test.
pub struct Digests {
    numbering: Xxh3,
    envelope: Xxh3,
    wire: Xxh3,
}

impl Default for Digests {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Debug for Digests {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.debug_struct("Digests").finish_non_exhaustive()
    }
}

/// What names one part, as the `numbering` digest takes it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PartNumbering {
    pub stream_id: u64,
    pub key_epoch: u64,
    pub number: u64,
    pub hls_msn: u64,
    pub hls_part: u32,
    /// Source time (Packaged and Published), Unix nanoseconds.
    pub source_time_ns: i64,
    /// Deadline on the stream's timeline, Unix nanoseconds.
    pub deadline_ns: i64,
    pub part_samples: u64,
}

impl Digests {
    pub fn new() -> Self {
        Self {
            numbering: Xxh3::new(),
            envelope: Xxh3::new(),
            wire: Xxh3::new(),
        }
    }

    /// One part's names.
    pub fn numbering(&mut self, part: &PartNumbering) {
        for word in [
            part.stream_id,
            part.key_epoch,
            part.number,
            part.hls_msn,
            u64::from(part.hls_part),
            part.source_time_ns as u64,
            part.deadline_ns as u64,
            part.part_samples,
        ] {
            self.numbering.update(&word.to_le_bytes());
        }
    }

    /// One part's canonical object.
    pub fn envelope(&mut self, envelope: &[u8]) {
        framed(&mut self.envelope, envelope);
    }

    /// One datagram.
    pub fn wire(&mut self, datagram: &[u8]) {
        framed(&mut self.wire, datagram);
    }

    /// The identity of what was digested, with the inputs in words.
    pub fn finish(self, fields: IdentityFields) -> Identity {
        let numbering = self.numbering.digest();
        let envelope = self.envelope.digest();
        let wire = self.wire.digest();
        let mut all = [0_u8; 24];
        all[..8].copy_from_slice(&numbering.to_le_bytes());
        all[8..16].copy_from_slice(&envelope.to_le_bytes());
        all[16..].copy_from_slice(&wire.to_le_bytes());
        let fingerprint = xxh3_64(&all);
        Identity {
            fingerprint: hex16(fingerprint),
            object_version: object_version(fingerprint),
            numbering: hex16(numbering),
            envelope: hex16(envelope),
            wire: hex16(wire),
            fields,
        }
    }
}

/// A length before the bytes: two datagrams are never read as one.
fn framed(hasher: &mut Xxh3, bytes: &[u8]) {
    hasher.update(&(bytes.len() as u64).to_le_bytes());
    hasher.update(bytes);
}

/// The inputs of the identity, in words for the operator (A 2.6).
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct IdentityFields {
    /// [`OPAQUE_AUDIO_FORMAT`].
    pub fmt: u32,
    pub base_stream_id: u64,
    /// `--audio-part` as given (`epoch`, `5ms`, `240samples`).
    pub audio_part: String,
    /// `--fmp4-segment-ms`, in microseconds.
    pub segment_us: u64,
    /// `--relay-deadline-ms`.
    pub deadline_ms: u64,
    /// The clock of the parts' times.
    pub clock: &'static str,
    /// `--wall-clock-estimated-error-ms`, in nanoseconds.
    pub clock_error_ns: u64,
    /// `--relay-topology-generation`.
    pub generation: u64,
    /// `--relay-subscription-id`.
    pub subscription: u64,
    /// The configured RaptorQ symbol size (an audio part's symbol is at
    /// most this).
    pub symbol_size: u16,
    pub max_datagram_bytes: usize,
    /// `--relay-compact-audio`.
    pub compact: bool,
}

/// `identity` of `GET /api/node`, and what `--print-fingerprint` prints.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Identity {
    /// 16 hex digits.
    pub fingerprint: String,
    /// The key version of every opaque audio part.
    pub object_version: u32,
    pub numbering: String,
    pub envelope: String,
    pub wire: String,
    pub fields: IdentityFields,
}

/// Part numbers in a window: `number >> WINDOW_SHIFT` names its window.
pub const WINDOW_SHIFT: u32 = 8;
/// Parts in a window.
pub const WINDOW_PARTS: u64 = 1 << WINDOW_SHIFT;
/// Closed windows that a stream reports.
pub const WINDOWS_REPORTED: usize = 8;
/// Windows a table holds: the two open ones, the reported closed ones and
/// room for the sampler (ten times a second) to read each closed one
/// before it is reused, at the shortest parts (0.25 ms: a window is 64 ms).
const WINDOW_SLOTS: usize = 16;
/// An empty slot.
const NO_WINDOW: u64 = u64::MAX;

/// One window of a stream, as `streams[].windows` shows it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct WindowRow {
    pub first_part: u64,
    pub parts: u64,
    /// Parts of the window published.
    pub present: u64,
    /// XOR of `xxh3_64` of the envelopes of those parts, 16 hex digits.
    pub xor: String,
}

#[derive(Debug)]
struct WindowSlot {
    /// Odd while the writer changes the slot.
    seq: AtomicU64,
    window: AtomicU64,
    present: AtomicU64,
    xor: AtomicU64,
}

impl Default for WindowSlot {
    fn default() -> Self {
        Self {
            seq: AtomicU64::new(0),
            window: AtomicU64::new(NO_WINDOW),
            present: AtomicU64::new(0),
            xor: AtomicU64::new(0),
        }
    }
}

impl WindowSlot {
    /// Change the slot (the one writer).
    fn write(&self, change: impl FnOnce(&Self)) {
        let seq = self.seq.load(Ordering::Relaxed);
        self.seq.store(seq.wrapping_add(1), Ordering::Relaxed);
        fence(Ordering::Release);
        change(self);
        self.seq.store(seq.wrapping_add(2), Ordering::Release);
    }

    /// The slot's (window, present, xor), read whole.
    fn read(&self) -> (u64, u64, u64) {
        loop {
            let before = self.seq.load(Ordering::Acquire);
            if before % 2 == 1 {
                std::hint::spin_loop();
                continue;
            }
            let read = (
                self.window.load(Ordering::Relaxed),
                self.present.load(Ordering::Relaxed),
                self.xor.load(Ordering::Relaxed),
            );
            fence(Ordering::Acquire);
            if self.seq.load(Ordering::Relaxed) == before {
                return read;
            }
        }
    }
}

/// The windows of one stream's published parts.
///
/// One writer: a stream's parts are published one after another (by its
/// rendition worker, or by its one publication shard). The writer takes no
/// lock and allocates nothing; the sampler reads with a sequence check.
///
/// A window closes when a part two windows later is published; a part of a
/// closed window (a late fill after that) is not counted, so a closed
/// window never changes. A higher key epoch (a new source session) starts
/// the table again; a part of a lower one is not counted.
#[derive(Debug)]
pub struct WindowTable {
    epoch: AtomicU64,
    /// The newest window plus one; 0 before the first part.
    head: AtomicU64,
    slots: [WindowSlot; WINDOW_SLOTS],
}

impl Default for WindowTable {
    fn default() -> Self {
        Self {
            epoch: AtomicU64::new(0),
            head: AtomicU64::new(0),
            slots: std::array::from_fn(|_| WindowSlot::default()),
        }
    }
}

impl WindowTable {
    /// Part `number` of key epoch `epoch` was published; `digest` is
    /// `xxh3_64` of its envelope.
    pub fn published(&self, epoch: u64, number: u64, digest: u64) {
        let current = self.epoch.load(Ordering::Relaxed);
        if epoch < current {
            return;
        }
        if epoch > current {
            for slot in &self.slots {
                slot.write(|slot| {
                    slot.window.store(NO_WINDOW, Ordering::Relaxed);
                    slot.present.store(0, Ordering::Relaxed);
                    slot.xor.store(0, Ordering::Relaxed);
                });
            }
            self.head.store(0, Ordering::Release);
            self.epoch.store(epoch, Ordering::Relaxed);
        }
        let window = number >> WINDOW_SHIFT;
        let head = self.head.load(Ordering::Relaxed);
        if head != 0 && window.saturating_add(2) <= head - 1 {
            // A closed window does not change.
            return;
        }
        let slot = &self.slots[(window % WINDOW_SLOTS as u64) as usize];
        slot.write(|slot| {
            if slot.window.load(Ordering::Relaxed) != window {
                slot.window.store(window, Ordering::Relaxed);
                slot.present.store(0, Ordering::Relaxed);
                slot.xor.store(0, Ordering::Relaxed);
            }
            slot.present.fetch_add(1, Ordering::Relaxed);
            slot.xor.fetch_xor(digest, Ordering::Relaxed);
        });
        if head == 0 || window > head - 1 {
            self.head.store(window + 1, Ordering::Release);
        }
    }

    /// The last [`WINDOWS_REPORTED`] closed windows, oldest first.
    pub fn closed(&self) -> Vec<WindowRow> {
        let head = self.head.load(Ordering::Acquire);
        if head == 0 {
            return Vec::new();
        }
        let newest = head - 1;
        let mut rows = self
            .slots
            .iter()
            .map(WindowSlot::read)
            .filter(|&(window, _, _)| window != NO_WINDOW && window.saturating_add(2) <= newest)
            .collect::<Vec<_>>();
        rows.sort_unstable_by_key(|&(window, _, _)| window);
        let skip = rows.len().saturating_sub(WINDOWS_REPORTED);
        rows.into_iter()
            .skip(skip)
            .map(|(window, present, xor)| WindowRow {
                first_part: window << WINDOW_SHIFT,
                parts: WINDOW_PARTS,
                present,
                xor: hex16(xor),
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn digest(number: u64) -> u64 {
        xxh3_64(&number.to_le_bytes())
    }

    fn publish(table: &WindowTable, epoch: u64, numbers: impl IntoIterator<Item = u64>) {
        for number in numbers {
            table.published(epoch, number, digest(number));
        }
    }

    #[test]
    fn the_version_carries_the_low_bits_of_the_fingerprint() {
        assert_eq!(object_version(0), 2 << 16);
        assert_eq!(object_version(0x9f2c_41aa_7d10_e6b3), (2 << 16) | 0xe6b3);
        // A's example: 190131.
        assert_eq!(object_version(0x9f2c_41aa_7d10_e6b3), 190_131);
    }

    #[test]
    fn each_digest_moves_alone() {
        let numbering = PartNumbering {
            stream_id: 3001,
            key_epoch: 1,
            number: 0,
            hls_msn: 0,
            hls_part: 0,
            source_time_ns: 1,
            deadline_ns: 2,
            part_samples: 48,
        };
        let fields = IdentityFields {
            fmt: 2,
            base_stream_id: 2001,
            audio_part: "epoch".to_owned(),
            segment_us: 1_000_000,
            deadline_ms: 1_000,
            clock: "source:aep1",
            clock_error_ns: 1,
            generation: 1,
            subscription: 1,
            symbol_size: 1_200,
            max_datagram_bytes: 1_400,
            compact: true,
        };
        let run = |number: u64, envelope: &[u8], wire: &[&[u8]]| {
            let mut digests = Digests::new();
            digests.numbering(&PartNumbering {
                number,
                ..numbering
            });
            digests.envelope(envelope);
            for datagram in wire {
                digests.wire(datagram);
            }
            digests.finish(fields.clone())
        };
        let base = run(0, b"env", &[b"ab", b"c"]);
        let renumbered = run(1, b"env", &[b"ab", b"c"]);
        let enveloped = run(0, b"enw", &[b"ab", b"c"]);
        // The same bytes cut into other datagrams are another wire.
        let rewired = run(0, b"env", &[b"a", b"bc"]);
        assert_ne!(base.numbering, renumbered.numbering);
        assert_eq!(
            (&base.envelope, &base.wire),
            (&renumbered.envelope, &renumbered.wire)
        );
        assert_ne!(base.envelope, enveloped.envelope);
        assert_eq!(
            (&base.numbering, &base.wire),
            (&enveloped.numbering, &enveloped.wire)
        );
        assert_ne!(base.wire, rewired.wire);
        assert_eq!(
            (&base.numbering, &base.envelope),
            (&rewired.numbering, &rewired.envelope)
        );
        for other in [&renumbered, &enveloped, &rewired] {
            assert_ne!(base.fingerprint, other.fingerprint);
        }
        assert_eq!(base, run(0, b"env", &[b"ab", b"c"]));
        assert_eq!(base.fingerprint.len(), 16);
        assert_eq!(
            base.object_version,
            object_version(u64::from_str_radix(&base.fingerprint, 16).unwrap())
        );
    }

    #[test]
    fn a_window_closes_when_a_part_two_windows_later_is_published() {
        let table = WindowTable::default();
        publish(&table, 7, 0..WINDOW_PARTS * 2);
        assert!(table.closed().is_empty(), "windows 0 and 1 are open");
        publish(&table, 7, [WINDOW_PARTS * 2]);
        let closed = table.closed();
        assert_eq!(closed.len(), 1);
        assert_eq!((closed[0].first_part, closed[0].parts), (0, 256));
        assert_eq!(closed[0].present, 256);
        let xor = (0..WINDOW_PARTS).fold(0, |xor, number| xor ^ digest(number));
        assert_eq!(closed[0].xor, hex16(xor));
    }

    #[test]
    fn a_window_with_a_missing_part_has_a_lower_present() {
        let full = WindowTable::default();
        let missing = WindowTable::default();
        publish(&full, 7, 0..WINDOW_PARTS * 3);
        publish(
            &missing,
            7,
            (0..WINDOW_PARTS * 3).filter(|&number| number != 100),
        );
        let (full, missing) = (full.closed(), missing.closed());
        assert_eq!(full[0].present, 256);
        assert_eq!(missing[0].present, 255);
        assert_ne!(full[0].xor, missing[0].xor);
        // The other windows are the same.
        assert_eq!(full[1..], missing[1..]);
    }

    #[test]
    fn a_closed_window_does_not_change_and_a_late_part_of_an_open_one_counts() {
        let table = WindowTable::default();
        publish(
            &table,
            7,
            (0..WINDOW_PARTS * 3).filter(|&number| number != 5 && number != 300),
        );
        let before = table.closed();
        // Window 0 is closed: its late part is not counted.
        publish(&table, 7, [5]);
        assert_eq!(table.closed(), before);
        assert_eq!(before[0].present, 255);
        // Window 1 is open: its late part is.
        publish(&table, 7, [300, WINDOW_PARTS * 3]);
        let after = table.closed();
        assert_eq!(after[1].first_part, 256);
        assert_eq!(after[1].present, 256);
    }

    #[test]
    fn the_last_eight_closed_windows_are_reported_oldest_first() {
        let table = WindowTable::default();
        publish(&table, 7, 0..WINDOW_PARTS * 20);
        let closed = table.closed();
        assert_eq!(closed.len(), WINDOWS_REPORTED);
        let firsts = closed.iter().map(|row| row.first_part).collect::<Vec<_>>();
        assert_eq!(
            firsts,
            (10..18).map(|window| window * WINDOW_PARTS).collect::<Vec<_>>()
        );
    }

    #[test]
    fn a_new_session_starts_the_windows_again_and_the_old_one_counts_no_more() {
        let table = WindowTable::default();
        publish(&table, 7, 0..WINDOW_PARTS * 4);
        publish(&table, 8, 0..WINDOW_PARTS * 3);
        let closed = table.closed();
        assert_eq!(closed.len(), 1);
        assert_eq!(closed[0].first_part, 0);
        assert_eq!(closed[0].present, 256);
        publish(&table, 7, [WINDOW_PARTS * 9]);
        assert_eq!(table.closed(), closed);
    }

    #[test]
    fn a_reader_never_sees_a_window_half_written() {
        let table = std::sync::Arc::new(WindowTable::default());
        let writer = {
            let table = std::sync::Arc::clone(&table);
            std::thread::spawn(move || publish(&table, 7, 0..WINDOW_PARTS * 400))
        };
        while !writer.is_finished() {
            for row in table.closed() {
                assert_eq!(row.present, 256);
                let window = row.first_part >> WINDOW_SHIFT;
                let xor = (0..WINDOW_PARTS)
                    .map(|place| digest(window * WINDOW_PARTS + place))
                    .fold(0, |xor, digest| xor ^ digest);
                assert_eq!(row.xor, hex16(xor), "{window}");
            }
        }
        writer.join().unwrap();
    }
}
