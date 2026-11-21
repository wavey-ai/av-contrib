//! Playlist coordinates that the contributor stamps on every part.
//!
//! A cache that derives a segment number from the parts it happens to hold
//! numbers the segments of a stream differently from a cache that joined the
//! stream at another moment. A reader that moves between caches then asks the
//! second cache for a segment number that it learned from the first. The
//! contributor is the one place that sees every part, so it names the place
//! of each part in the playlist: the Media Sequence Number of its segment
//! and its index in that segment. It also declares the part target and the
//! target duration, so that every cache writes the same `EXT-X-PART-INF` and
//! `EXT-X-TARGETDURATION` and sizes its window from the same numbers.
//!
//! The first segment number of a stream is taken from the wall clock. A
//! contributor that starts again therefore continues above the numbers of
//! its earlier run, and a playlist never goes back.

/// Metadata keys of a media object.
pub const META_MSN: &str = "hls-msn";
pub const META_PART: &str = "hls-part";
pub const META_PART_TARGET_MS: &str = "hls-part-target-ms";
pub const META_TARGET_DURATION_MS: &str = "hls-target-duration-ms";

/// The place of one part in the Media Playlist of its stream.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HlsStamp {
    /// Media Sequence Number of the segment that the part belongs to.
    pub msn: u64,
    /// Index of the part in its segment.
    pub part: u32,
    /// Longest part that the stream produces, in milliseconds.
    pub part_target_ms: u32,
    /// Longest segment that the stream has produced, in milliseconds.
    pub target_duration_ms: u32,
}

impl HlsStamp {
    pub fn metadata(&self) -> [(&'static str, Vec<u8>); 4] {
        [
            (META_MSN, self.msn.to_string().into_bytes()),
            (META_PART, self.part.to_string().into_bytes()),
            (
                META_PART_TARGET_MS,
                self.part_target_ms.to_string().into_bytes(),
            ),
            (
                META_TARGET_DURATION_MS,
                self.target_duration_ms.to_string().into_bytes(),
            ),
        ]
    }
}

/// Stamps the parts of one stream in publication order.
#[derive(Debug, Clone)]
pub struct StreamStamper {
    part_ms: u32,
    segment_ms: u32,
    msn: Option<u64>,
    part: u32,
    segment_elapsed_ms: u64,
    part_target_ms: u32,
    target_duration_ms: u32,
    /// The segment that is open began with a part that a reader can start
    /// from.
    segment_starts_clean: bool,
    /// Duration of the part before this one.
    last_part_ms: u32,
    /// Recent parts that were longer than the declared part target, and the
    /// number of parts since the first of them.
    longer_parts: Vec<u32>,
    parts_since_longer: u32,
}

/// Parts longer than the declared target that widen it.
const LONGER_PARTS_TO_WIDEN: usize = 3;
/// Parts within which they must come.
const LONGER_PARTS_WINDOW: u32 = 100;

impl StreamStamper {
    /// `part_ms` and `segment_ms` are the configured part and segment
    /// durations of the track.
    pub fn new(part_ms: u32, segment_ms: u32) -> Self {
        let part_ms = part_ms.max(1);
        let segment_ms = segment_ms.max(part_ms);
        Self {
            part_ms,
            segment_ms,
            msn: None,
            part: 0,
            segment_elapsed_ms: 0,
            part_target_ms: part_ms,
            target_duration_ms: segment_ms,
            segment_starts_clean: true,
            last_part_ms: part_ms,
            longer_parts: Vec::with_capacity(LONGER_PARTS_TO_WIDEN),
            parts_since_longer: 0,
        }
    }

    /// Stamp the next part.
    ///
    /// `can_start_segment` is true for a part that a reader can start from:
    /// a video part that begins with a key frame, and every audio part. A
    /// segment ends at the first such part after the configured segment
    /// duration, so a video segment is a whole number of groups of pictures.
    ///
    /// A segment also ends at such a part in two other cases. The stream
    /// began in the middle of a group of pictures: its first segment ends at
    /// the first key frame, and does not grow to the second one and widen
    /// the target duration for good. And the part before a key frame was cut
    /// short: a short part is allowed only as the last part of a segment.
    pub fn stamp(
        &mut self,
        duration_ms: u32,
        can_start_segment: bool,
        published_unix_ms: u64,
    ) -> HlsStamp {
        let msn = match self.msn {
            None => {
                let first = published_unix_ms / u64::from(self.segment_ms);
                self.msn = Some(first);
                self.segment_starts_clean = can_start_segment;
                first
            }
            Some(current) => {
                // Half a part of slack, so that a segment of 1.96 s closes a
                // 2 s segment.
                let full = self.segment_elapsed_ms + u64::from(self.part_ms / 2)
                    >= u64::from(self.segment_ms);
                let after_short_part =
                    u64::from(self.last_part_ms) * 100 < u64::from(self.part_target_ms) * 85;
                if can_start_segment && (full || !self.segment_starts_clean || after_short_part) {
                    let finished = u32::try_from(self.segment_elapsed_ms).unwrap_or(u32::MAX);
                    self.target_duration_ms = self.target_duration_ms.max(finished);
                    self.segment_elapsed_ms = 0;
                    self.part = 0;
                    self.segment_starts_clean = true;
                    self.msn = Some(current + 1);
                    current + 1
                } else {
                    current
                }
            }
        };
        self.last_part_ms = duration_ms;
        // A part is as long as its last frame makes it: 208 ms for five
        // frames at 24 frames a second, when 200 ms is configured. The
        // declared target follows a duration that recurs. One long part,
        // such as the first part of a stream, does not widen it: every
        // later part would then be shorter than 85% of the target, which a
        // playlist allows only for the last part of a segment. A part of
        // more than twice the configured duration is a fault of the source.
        if !self.longer_parts.is_empty() {
            self.parts_since_longer += 1;
            if self.parts_since_longer > LONGER_PARTS_WINDOW {
                self.longer_parts.clear();
            }
        }
        if duration_ms > self.part_target_ms {
            if self.longer_parts.is_empty() {
                self.parts_since_longer = 0;
            }
            self.longer_parts
                .push(duration_ms.min(self.part_ms.saturating_mul(2)));
            if self.longer_parts.len() >= LONGER_PARTS_TO_WIDEN {
                let recurring = self.longer_parts.iter().copied().min().unwrap_or(0);
                self.part_target_ms = self.part_target_ms.max(recurring);
                self.longer_parts.clear();
            }
        }
        let stamp = HlsStamp {
            msn,
            part: self.part,
            part_target_ms: self.part_target_ms,
            target_duration_ms: self.target_duration_ms,
        };
        self.part += 1;
        self.segment_elapsed_ms += u64::from(duration_ms);
        stamp
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn audio_parts_fill_fixed_segments() {
        let mut stamper = StreamStamper::new(5, 1_000);
        let mut stamps = Vec::new();
        for _ in 0..450 {
            stamps.push(stamper.stamp(5, true, 1_700_000_000_250));
        }
        // The first segment number is the wall clock in segment durations.
        assert_eq!(stamps[0].msn, 1_700_000_000);
        assert_eq!(stamps[0].part, 0);
        assert_eq!(stamps[199].msn, 1_700_000_000);
        assert_eq!(stamps[199].part, 199);
        assert_eq!(stamps[200].msn, 1_700_000_001);
        assert_eq!(stamps[200].part, 0);
        assert_eq!(stamps[449].msn, 1_700_000_002);
        assert_eq!(stamps[449].part, 49);
        assert!(stamps.iter().all(|stamp| stamp.part_target_ms == 5));
        assert!(stamps.iter().all(|stamp| stamp.target_duration_ms == 1_000));
    }

    #[test]
    fn a_video_segment_ends_at_a_key_frame() {
        // 24 frames a second, parts of five frames, a key frame every 48
        // frames: the last part of a group has three frames.
        let mut stamper = StreamStamper::new(200, 2_000);
        let mut stamps = Vec::new();
        for group in 0..3 {
            for part in 0..10 {
                let frames = if part == 9 { 3 } else { 5 };
                let duration_ms = frames * 1_000 / 24;
                let _ = group;
                stamps.push(stamper.stamp(duration_ms, part == 0, 1_700_000_000_000));
            }
        }
        let first = stamps[0].msn;
        assert_eq!(first, 850_000_000);
        assert_eq!(stamps[9].msn, first);
        assert_eq!(stamps[9].part, 9);
        assert_eq!(stamps[10].msn, first + 1);
        assert_eq!(stamps[10].part, 0);
        assert_eq!(stamps[20].msn, first + 2);
        // 208 ms parts of five frames.
        assert_eq!(stamps[29].part_target_ms, 208);
        // A group of 48 frames is 1,997 ms. The target stays at 2 s.
        assert_eq!(stamps[29].target_duration_ms, 2_000);
    }

    #[test]
    fn a_stream_that_begins_inside_a_group_ends_its_first_segment_at_the_key_frame() {
        let mut stamper = StreamStamper::new(200, 2_000);
        // Three parts of a group whose key frame came before the stream.
        let lead = [
            stamper.stamp(208, false, 10_000),
            stamper.stamp(208, false, 10_208),
            stamper.stamp(125, false, 10_416),
        ];
        // The first key frame starts the second segment, 541 ms in.
        let first_key = stamper.stamp(208, true, 10_541);
        assert_eq!(lead[0].msn, 5);
        assert_eq!(lead[2].part, 2);
        assert_eq!(first_key.msn, 6);
        assert_eq!(first_key.part, 0);
        // The short first segment does not change the target duration, and
        // the stream goes on in segments of one group.
        assert_eq!(first_key.target_duration_ms, 2_000);
        let mut last = first_key;
        for part in 1..10 {
            last = stamper.stamp(if part == 9 { 125 } else { 208 }, false, 0);
        }
        assert_eq!(last.msn, 6);
        let next = stamper.stamp(208, true, 0);
        assert_eq!(next.msn, 7);
        assert_eq!(next.target_duration_ms, 2_000);
    }

    #[test]
    fn a_key_frame_after_a_short_part_starts_a_segment() {
        // A scene cut puts a key frame 0.7 s into a group. The part before it
        // is cut short, and a short part is the last part of its segment.
        let mut stamper = StreamStamper::new(200, 2_000);
        stamper.stamp(208, true, 0);
        stamper.stamp(208, false, 0);
        stamper.stamp(208, false, 0);
        let short = stamper.stamp(83, false, 0);
        let cut = stamper.stamp(208, true, 0);
        assert_eq!(short.msn, 0);
        assert_eq!(cut.msn, 1);
        assert_eq!(cut.part, 0);
        // A key frame after a whole part, early in a segment, does not: the
        // parts on both sides are whole.
        let mut stamper = StreamStamper::new(200, 2_000);
        stamper.stamp(208, true, 0);
        stamper.stamp(208, false, 0);
        let key = stamper.stamp(208, true, 0);
        assert_eq!(key.msn, 0);
        assert_eq!(key.part, 2);
    }

    #[test]
    fn a_long_group_of_pictures_makes_a_long_segment() {
        let mut stamper = StreamStamper::new(200, 2_000);
        let mut last = stamper.stamp(200, true, 0);
        for _ in 0..19 {
            last = stamper.stamp(200, false, 0);
        }
        // Four seconds without a key frame: still the first segment.
        assert_eq!(last.msn, 0);
        assert_eq!(last.part, 19);
        let next = stamper.stamp(200, true, 0);
        assert_eq!(next.msn, 1);
        assert_eq!(next.target_duration_ms, 4_000);
    }

    #[test]
    fn a_restart_continues_above_the_earlier_numbers() {
        let mut first = StreamStamper::new(200, 2_000);
        let started_ms = 1_700_000_000_000_u64;
        let mut last = first.stamp(200, true, started_ms);
        // Ten minutes of two-second segments.
        for index in 1..3_000_u64 {
            last = first.stamp(200, index % 10 == 0, started_ms + index * 200);
        }
        let mut second = StreamStamper::new(200, 2_000);
        let restarted = second.stamp(200, true, started_ms + 3_000 * 200 + 1_500);
        assert!(restarted.msn > last.msn, "{} > {}", restarted.msn, last.msn);
    }

    #[test]
    fn one_long_part_does_not_widen_the_part_target() {
        let mut stamper = StreamStamper::new(200, 2_000);
        // The first part of a stream is long, and the parts after it are
        // five frames each.
        let first = stamper.stamp(400, true, 0);
        assert_eq!(first.part_target_ms, 200);
        let mut last = first;
        for _ in 0..3 {
            last = stamper.stamp(208, false, 0);
        }
        // The duration that recurs is the target. The long part is not.
        assert_eq!(last.part_target_ms, 208);
        let late = stamper.stamp(1_500, false, 0);
        assert_eq!(late.part_target_ms, 208);
        for _ in 0..300 {
            last = stamper.stamp(208, false, 0);
        }
        assert_eq!(last.part_target_ms, 208);
    }

    #[test]
    fn a_longer_part_that_recurs_widens_the_part_target_within_its_bound() {
        let mut stamper = StreamStamper::new(200, 2_000);
        let mut last = stamper.stamp(208, true, 0);
        for _ in 0..5 {
            last = stamper.stamp(1_500, false, 0);
        }
        // Never more than twice the configured duration.
        assert_eq!(last.part_target_ms, 400);
    }
}
