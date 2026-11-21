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

/// Metadata keys of a media object. The `-ms` keys are whole milliseconds
/// for caches that do not read the exact `-us` keys.
pub const META_MSN: &str = "hls-msn";
pub const META_PART: &str = "hls-part";
pub const META_PART_TARGET_MS: &str = "hls-part-target-ms";
pub const META_PART_TARGET_US: &str = "hls-part-target-us";
pub const META_TARGET_DURATION_MS: &str = "hls-target-duration-ms";
pub const META_TARGET_DURATION_US: &str = "hls-target-duration-us";

use crate::part_timing::ceil_ms;

/// The place of one part in the Media Playlist of its stream.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HlsStamp {
    /// Media Sequence Number of the segment that the part belongs to.
    pub msn: u64,
    /// Index of the part in its segment.
    pub part: u32,
    /// Longest part that the stream produces, in microseconds.
    pub part_target_us: u64,
    /// Longest segment that the stream has produced, in microseconds.
    pub target_duration_us: u64,
}

impl HlsStamp {
    /// The part target in whole milliseconds, rounded up so that every part
    /// fits under it: 2.5 ms parts declare 3.
    pub fn part_target_ms(&self) -> u32 {
        ceil_ms(self.part_target_us)
    }

    /// The target duration in whole milliseconds, rounded up.
    pub fn target_duration_ms(&self) -> u32 {
        ceil_ms(self.target_duration_us)
    }

    pub fn metadata(&self) -> [(&'static str, Vec<u8>); 6] {
        [
            (META_MSN, self.msn.to_string().into_bytes()),
            (META_PART, self.part.to_string().into_bytes()),
            (
                META_PART_TARGET_MS,
                self.part_target_ms().to_string().into_bytes(),
            ),
            (
                META_PART_TARGET_US,
                self.part_target_us.to_string().into_bytes(),
            ),
            (
                META_TARGET_DURATION_MS,
                self.target_duration_ms().to_string().into_bytes(),
            ),
            (
                META_TARGET_DURATION_US,
                self.target_duration_us.to_string().into_bytes(),
            ),
        ]
    }
}

/// Stamps the parts of one stream in publication order. All durations are
/// microseconds of media.
#[derive(Debug, Clone)]
pub struct StreamStamper {
    /// Configured part duration; `0` for one unit (epoch, frame) per part.
    part_us: u64,
    segment_us: u64,
    msn: Option<u64>,
    part: u32,
    segment_elapsed_us: u64,
    /// `0` until the first part of a stream of one unit per part.
    part_target_us: u64,
    target_duration_us: u64,
    /// The segment that is open began with a part that a reader can start
    /// from.
    segment_starts_clean: bool,
    /// Duration of the part before this one.
    last_part_us: u64,
    /// Recent parts that were longer than the declared part target, and the
    /// media time since the first of them.
    longer_parts: Vec<u64>,
    since_longer_us: u64,
    /// Recent segments that were longer than the declared target duration,
    /// and the number of segments since the first of them.
    longer_segments: Vec<u64>,
    segments_since_longer: u32,
}

/// Parts longer than the declared target that widen it.
const LONGER_PARTS_TO_WIDEN: usize = 3;
/// Media time within which they must come: a hundred part targets, and at
/// least two seconds, so that tiny parts do not shrink it to milliseconds.
const LONGER_PARTS_WINDOW_PARTS: u64 = 100;
const LONGER_PARTS_WINDOW_MIN_US: u64 = 2_000_000;
/// Segments longer than the declared target duration that widen it, and the
/// segments within which they must come.
const LONGER_SEGMENTS_TO_WIDEN: usize = 3;
const LONGER_SEGMENTS_WINDOW: u32 = 20;
/// A target duration is a whole number of seconds; a segment this much over
/// it is still a segment of the target.
const SEGMENT_SLACK_US: u64 = 499_999;

impl StreamStamper {
    /// `part_us` and `segment_us` are the configured part and segment
    /// durations of the track. A `part_us` of `0` is one unit per part: the
    /// part target is then the longest part seen (the longest frame of a
    /// stream of one frame per part), rather than a configured value.
    pub fn new(part_us: u64, segment_us: u64) -> Self {
        let segment_us = segment_us.max(part_us).max(1);
        Self {
            part_us,
            segment_us,
            msn: None,
            part: 0,
            segment_elapsed_us: 0,
            part_target_us: part_us,
            target_duration_us: segment_us,
            segment_starts_clean: true,
            last_part_us: part_us,
            longer_parts: Vec::with_capacity(LONGER_PARTS_TO_WIDEN),
            since_longer_us: 0,
            longer_segments: Vec::with_capacity(LONGER_SEGMENTS_TO_WIDEN),
            segments_since_longer: 0,
        }
    }

    fn one_unit_per_part(&self) -> bool {
        self.part_us == 0
    }

    /// Take the duration of a finished segment into the declared target
    /// duration.
    ///
    /// A stream whose groups of pictures are longer than the configured
    /// segment has long segments all the time, and its target duration
    /// follows. One long segment is a fault upstream, for example parts that
    /// were lost with their key frame. It widened the target duration from
    /// 2 s to 8 s for the life of a stream, and every cache then held 80 s
    /// of the stream instead of 20 s: 190 MB more on each node for one 4K
    /// stream. The target now follows a duration that recurs.
    fn segment_finished(&mut self, duration_us: u64) {
        if !self.longer_segments.is_empty() {
            self.segments_since_longer += 1;
            if self.segments_since_longer > LONGER_SEGMENTS_WINDOW {
                self.longer_segments.clear();
            }
        }
        // Half a second of slack: a target duration is a whole number of
        // seconds, and 2.04 s is a segment of 2 s.
        if duration_us > self.target_duration_us.saturating_add(SEGMENT_SLACK_US) {
            if self.longer_segments.is_empty() {
                self.segments_since_longer = 0;
            }
            self.longer_segments.push(duration_us);
            if self.longer_segments.len() >= LONGER_SEGMENTS_TO_WIDEN {
                let recurring = self.longer_segments.iter().copied().min().unwrap_or(0);
                self.target_duration_us = self.target_duration_us.max(recurring);
                self.longer_segments.clear();
            }
        }
    }

    /// Take the duration of a part into the declared part target.
    ///
    /// A part is as long as its last frame makes it: 208 ms for five frames
    /// at 24 frames a second, when 200 ms is configured. The declared target
    /// follows a duration that recurs. One long part, such as the first part
    /// of a stream, does not widen it: every later part would then be
    /// shorter than 85% of the target, which a playlist allows only for the
    /// last part of a segment. With a configured part, a part of more than
    /// twice the configured duration is a fault of the source.
    ///
    /// With one unit per part the target is the longest unit seen: a frame
    /// of 34 ms among frames of 33 ms widens it at once, since a part may
    /// never be longer than the target. A part of more than twice the
    /// target (a stall, a dropped run of frames) widens it only when it
    /// recurs, as above, without a bound.
    fn part_finished(&mut self, duration_us: u64) {
        if !self.longer_parts.is_empty() {
            self.since_longer_us = self.since_longer_us.saturating_add(duration_us);
            let window = self
                .part_target_us
                .max(self.part_us)
                .saturating_mul(LONGER_PARTS_WINDOW_PARTS)
                .max(LONGER_PARTS_WINDOW_MIN_US);
            if self.since_longer_us > window {
                self.longer_parts.clear();
            }
        }
        if duration_us <= self.part_target_us {
            return;
        }
        if self.one_unit_per_part()
            && (self.part_target_us == 0 || duration_us <= self.part_target_us.saturating_mul(2))
        {
            self.part_target_us = duration_us;
            return;
        }
        if self.longer_parts.is_empty() {
            self.since_longer_us = 0;
        }
        let bounded = if self.one_unit_per_part() {
            duration_us
        } else {
            duration_us.min(self.part_us.saturating_mul(2))
        };
        self.longer_parts.push(bounded);
        if self.longer_parts.len() >= LONGER_PARTS_TO_WIDEN {
            let recurring = self.longer_parts.iter().copied().min().unwrap_or(0);
            self.part_target_us = self.part_target_us.max(recurring);
            self.longer_parts.clear();
        }
    }

    /// Stamp the next part, of `duration_us` of media, published at
    /// `published_unix_us`.
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
        duration_us: u64,
        can_start_segment: bool,
        published_unix_us: u64,
    ) -> HlsStamp {
        let msn = match self.msn {
            None => {
                // The first segment number is the wall clock in segment
                // durations, so a restart continues above the earlier run.
                let first = published_unix_us / self.segment_us;
                self.msn = Some(first);
                self.segment_starts_clean = can_start_segment;
                first
            }
            Some(current) => {
                // Half a part of slack, so that a segment of 1.96 s closes a
                // 2 s segment.
                let full = self.segment_elapsed_us + self.part_target_us / 2 >= self.segment_us;
                let after_short_part =
                    u128::from(self.last_part_us) * 100 < u128::from(self.part_target_us) * 85;
                if can_start_segment && (full || !self.segment_starts_clean || after_short_part) {
                    let finished = self.segment_elapsed_us;
                    self.segment_finished(finished);
                    self.segment_elapsed_us = 0;
                    self.part = 0;
                    self.segment_starts_clean = true;
                    self.msn = Some(current + 1);
                    current + 1
                } else {
                    current
                }
            }
        };
        self.last_part_us = duration_us;
        self.part_finished(duration_us);
        let stamp = HlsStamp {
            msn,
            part: self.part,
            part_target_us: self.part_target_us,
            target_duration_us: self.target_duration_us,
        };
        self.part += 1;
        self.segment_elapsed_us += duration_us;
        stamp
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn new_ms(part_ms: u64, segment_ms: u64) -> StreamStamper {
        StreamStamper::new(part_ms * 1_000, segment_ms * 1_000)
    }

    trait StampMs {
        fn stamp_ms(&mut self, duration_ms: u32, can_start: bool, unix_ms: u64) -> HlsStamp;
    }

    impl StampMs for StreamStamper {
        fn stamp_ms(&mut self, duration_ms: u32, can_start: bool, unix_ms: u64) -> HlsStamp {
            self.stamp(u64::from(duration_ms) * 1_000, can_start, unix_ms * 1_000)
        }
    }

    #[test]
    fn audio_parts_fill_fixed_segments() {
        let mut stamper = new_ms(5, 1_000);
        let mut stamps = Vec::new();
        for _ in 0..450 {
            stamps.push(stamper.stamp_ms(5, true, 1_700_000_000_250));
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
        assert!(stamps.iter().all(|stamp| stamp.part_target_ms() == 5));
        assert!(stamps
            .iter()
            .all(|stamp| stamp.target_duration_ms() == 1_000));
    }

    #[test]
    fn a_video_segment_ends_at_a_key_frame() {
        // 24 frames a second, parts of five frames, a key frame every 48
        // frames: the last part of a group has three frames.
        let mut stamper = new_ms(200, 2_000);
        let mut stamps = Vec::new();
        for group in 0..3 {
            for part in 0..10 {
                let frames = if part == 9 { 3 } else { 5 };
                let duration_ms = frames * 1_000 / 24;
                let _ = group;
                stamps.push(stamper.stamp_ms(duration_ms, part == 0, 1_700_000_000_000));
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
        assert_eq!(stamps[29].part_target_ms(), 208);
        // A group of 48 frames is 1,997 ms. The target stays at 2 s.
        assert_eq!(stamps[29].target_duration_ms(), 2_000);
    }

    #[test]
    fn a_stream_that_begins_inside_a_group_ends_its_first_segment_at_the_key_frame() {
        let mut stamper = new_ms(200, 2_000);
        // Three parts of a group whose key frame came before the stream.
        let lead = [
            stamper.stamp_ms(208, false, 10_000),
            stamper.stamp_ms(208, false, 10_208),
            stamper.stamp_ms(125, false, 10_416),
        ];
        // The first key frame starts the second segment, 541 ms in.
        let first_key = stamper.stamp_ms(208, true, 10_541);
        assert_eq!(lead[0].msn, 5);
        assert_eq!(lead[2].part, 2);
        assert_eq!(first_key.msn, 6);
        assert_eq!(first_key.part, 0);
        // The short first segment does not change the target duration, and
        // the stream goes on in segments of one group.
        assert_eq!(first_key.target_duration_ms(), 2_000);
        let mut last = first_key;
        for part in 1..10 {
            last = stamper.stamp_ms(if part == 9 { 125 } else { 208 }, false, 0);
        }
        assert_eq!(last.msn, 6);
        let next = stamper.stamp_ms(208, true, 0);
        assert_eq!(next.msn, 7);
        assert_eq!(next.target_duration_ms(), 2_000);
    }

    #[test]
    fn a_key_frame_after_a_short_part_starts_a_segment() {
        // A scene cut puts a key frame 0.7 s into a group. The part before it
        // is cut short, and a short part is the last part of its segment.
        let mut stamper = new_ms(200, 2_000);
        stamper.stamp_ms(208, true, 0);
        stamper.stamp_ms(208, false, 0);
        stamper.stamp_ms(208, false, 0);
        let short = stamper.stamp_ms(83, false, 0);
        let cut = stamper.stamp_ms(208, true, 0);
        assert_eq!(short.msn, 0);
        assert_eq!(cut.msn, 1);
        assert_eq!(cut.part, 0);
        // A key frame after a whole part, early in a segment, does not: the
        // parts on both sides are whole.
        let mut stamper = new_ms(200, 2_000);
        stamper.stamp_ms(208, true, 0);
        stamper.stamp_ms(208, false, 0);
        let key = stamper.stamp_ms(208, true, 0);
        assert_eq!(key.msn, 0);
        assert_eq!(key.part, 2);
    }

    #[test]
    fn a_long_group_of_pictures_makes_a_long_segment() {
        let mut stamper = new_ms(200, 2_000);
        let mut last = stamper.stamp_ms(200, true, 0);
        for _ in 0..19 {
            last = stamper.stamp_ms(200, false, 0);
        }
        // Four seconds without a key frame: still the first segment.
        assert_eq!(last.msn, 0);
        assert_eq!(last.part, 19);
        let next = stamper.stamp_ms(200, true, 0);
        assert_eq!(next.msn, 1);
        // The stream goes on in groups of four seconds: the target duration
        // follows after the third such segment.
        assert_eq!(next.target_duration_ms(), 2_000);
        let mut last = next;
        for part in 1..=60 {
            last = stamper.stamp_ms(200, part % 20 == 0, 0);
        }
        assert_eq!(last.msn, 4);
        assert_eq!(last.target_duration_ms(), 4_000);
    }

    #[test]
    fn one_long_segment_does_not_widen_the_target_duration() {
        let mut stamper = new_ms(200, 2_000);
        let mut last = stamper.stamp_ms(200, true, 0);
        // A fault upstream: 7.6 s pass before the next key frame arrives.
        for _ in 0..37 {
            last = stamper.stamp_ms(200, false, 0);
        }
        assert_eq!(last.msn, 0);
        // An hour of two-second segments after it.
        for part in 0..18_000 {
            last = stamper.stamp_ms(200, part % 10 == 0, 0);
        }
        assert_eq!(last.target_duration_ms(), 2_000);
        // And another fault, long after the first.
        for _ in 0..30 {
            last = stamper.stamp_ms(200, false, 0);
        }
        for part in 0..100 {
            last = stamper.stamp_ms(200, part % 10 == 0, 0);
        }
        assert_eq!(last.target_duration_ms(), 2_000);
    }

    #[test]
    fn a_restart_continues_above_the_earlier_numbers() {
        let mut first = new_ms(200, 2_000);
        let started_ms = 1_700_000_000_000_u64;
        let mut last = first.stamp_ms(200, true, started_ms);
        // Ten minutes of two-second segments.
        for index in 1..3_000_u64 {
            last = first.stamp_ms(200, index % 10 == 0, started_ms + index * 200);
        }
        let mut second = new_ms(200, 2_000);
        let restarted = second.stamp_ms(200, true, started_ms + 3_000 * 200 + 1_500);
        assert!(restarted.msn > last.msn, "{} > {}", restarted.msn, last.msn);
    }

    #[test]
    fn one_long_part_does_not_widen_the_part_target() {
        let mut stamper = new_ms(200, 2_000);
        // The first part of a stream is long, and the parts after it are
        // five frames each.
        let first = stamper.stamp_ms(400, true, 0);
        assert_eq!(first.part_target_ms(), 200);
        let mut last = first;
        for _ in 0..3 {
            last = stamper.stamp_ms(208, false, 0);
        }
        // The duration that recurs is the target. The long part is not.
        assert_eq!(last.part_target_ms(), 208);
        let late = stamper.stamp_ms(1_500, false, 0);
        assert_eq!(late.part_target_ms(), 208);
        for _ in 0..300 {
            last = stamper.stamp_ms(208, false, 0);
        }
        assert_eq!(last.part_target_ms(), 208);
    }

    #[test]
    fn a_longer_part_that_recurs_widens_the_part_target_within_its_bound() {
        let mut stamper = new_ms(200, 2_000);
        let mut last = stamper.stamp_ms(208, true, 0);
        for _ in 0..5 {
            last = stamper.stamp_ms(1_500, false, 0);
        }
        // Never more than twice the configured duration.
        assert_eq!(last.part_target_ms(), 400);
    }

    #[test]
    fn two_and_a_half_ms_parts_fill_one_second_segments_exactly() {
        // 120-sample Opus parts at 48 kHz, one epoch per part.
        let mut stamper = StreamStamper::new(0, 1_000_000);
        let start_us = 1_700_000_000_000_000_u64;
        let stamps = (0..1_200_u64)
            .map(|index| stamper.stamp(2_500, true, start_us + index * 2_500))
            .collect::<Vec<_>>();
        assert_eq!(stamps[0].msn, 1_700_000_000);
        // 400 parts make one second, not 334 parts of "3 ms".
        assert_eq!(stamps[399].msn, stamps[0].msn);
        assert_eq!(stamps[399].part, 399);
        assert_eq!(stamps[400].msn, stamps[0].msn + 1);
        assert_eq!(stamps[400].part, 0);
        assert_eq!(stamps[800].msn, stamps[0].msn + 2);
        assert!(stamps.iter().all(|stamp| stamp.part_target_us == 2_500));
        assert!(stamps
            .iter()
            .all(|stamp| stamp.target_duration_us == 1_000_000));
        // Old caches read whole milliseconds, rounded up to fit every part.
        let metadata = stamps[1].metadata();
        let value = |key: &str| {
            metadata
                .iter()
                .find(|(name, _)| *name == key)
                .map(|(_, value)| String::from_utf8(value.clone()).unwrap())
                .unwrap()
        };
        assert_eq!(value(META_PART_TARGET_US), "2500");
        assert_eq!(value(META_PART_TARGET_MS), "3");
        assert_eq!(value(META_TARGET_DURATION_US), "1000000");
        assert_eq!(value(META_TARGET_DURATION_MS), "1000");
        assert_eq!(value(META_PART), "1");
    }

    #[test]
    fn quarter_ms_parts_keep_their_place_against_the_wall_clock() {
        // 12-sample PCM parts: 4,000 a second. A restart one second later
        // continues above the numbers of the first run.
        let mut stamper = StreamStamper::new(0, 1_000_000);
        let start_us = 1_700_000_000_000_000_u64;
        let mut last = stamper.stamp(250, true, start_us);
        for index in 1..8_000_u64 {
            last = stamper.stamp(250, true, start_us + index * 250);
        }
        assert_eq!(last.msn, 1_700_000_001);
        assert_eq!(last.part, 3_999);
        assert_eq!(last.part_target_us, 250);
        let mut restarted = StreamStamper::new(0, 1_000_000);
        let first = restarted.stamp(250, true, start_us + 2_000_000);
        assert!(first.msn > last.msn);
    }

    #[test]
    fn one_frame_parts_declare_the_longest_frame_as_the_part_target() {
        // 30000/1001 frames a second in a 90 kHz clock: 3,003 ticks are
        // 33,367 µs; a millisecond clock gives some frames of 34 ms.
        let mut stamper = StreamStamper::new(0, 2_000_000);
        let first = stamper.stamp(33_367, true, 0);
        assert_eq!(first.part_target_us, 33_367);
        let mut last = first;
        for index in 1..60 {
            let duration_us = if index % 3 == 0 { 34_000 } else { 33_000 };
            last = stamper.stamp(duration_us, false, 0);
            assert!(duration_us <= last.part_target_us);
        }
        assert_eq!(last.part_target_us, 34_000);
        // Not twice a 1 ms setting: the target is a frame, and every part fits.
        assert!(last.part_target_us > 2_000);
        // A stall of half a second is a fault: it does not widen the target.
        let stalled = stamper.stamp(500_000, false, 0);
        assert_eq!(stalled.part_target_us, 34_000);
        let next = stamper.stamp(33_367, false, 0);
        assert_eq!(next.part_target_us, 34_000);
    }

    #[test]
    fn one_frame_parts_cut_segments_at_key_frames() {
        // 25 frames a second, a key frame every 50 frames, 2 s segments.
        let mut stamper = StreamStamper::new(0, 2_000_000);
        let stamps = (0..150)
            .map(|frame| stamper.stamp(40_000, frame % 50 == 0, 0))
            .collect::<Vec<_>>();
        assert_eq!(stamps[49].msn, stamps[0].msn);
        assert_eq!(stamps[49].part, 49);
        assert_eq!(stamps[50].msn, stamps[0].msn + 1);
        assert_eq!(stamps[100].msn, stamps[0].msn + 2);
        assert!(stamps.iter().all(|stamp| stamp.part_target_us == 40_000));
        assert!(stamps
            .iter()
            .all(|stamp| stamp.target_duration_us == 2_000_000));
    }
}
