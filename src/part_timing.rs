//! Part sizes and media time in exact units.
//!
//! A part of 120 samples at 48 kHz lasts 2.5 ms, and one of 12 samples
//! 0.25 ms. Whole milliseconds cannot carry either: 2.5 ms rounds to 3 ms and
//! 0.25 ms to 0. The contributor therefore counts audio in samples, declares
//! every part in microseconds, and writes the old millisecond keys only for
//! caches that do not read the new ones.
//!
//! The default part is the smallest unit the source makes: one AEP1 epoch of
//! audio, whatever its sample count, and one access unit (frame) of video.
//! A larger part is a setting.
//!
//! An opaque audio part is named from its source alone ([`SourceAnchor`]):
//! its number, its place in the playlist, its times and its deadline are
//! arithmetic on the AEP1 session id and sample count. Two contributors fed
//! the same epochs therefore name every part alike, whenever each started
//! and whatever each lost.

use std::fmt;
use std::str::FromStr;

/// FLAC cannot code a block of fewer samples than this (soundkit-flac's
/// encoder floor). A FLAC part is never shorter, whatever the setting; PCM
/// and Opus have no such floor here.
pub const FLAC_MIN_PART_SAMPLES: u64 = 32;

/// `min_part_ms` of a segmenter that closes a part on every access unit.
pub const ONE_UNIT_PER_PART_MS: u32 = 0;

/// Duration of `samples` at `sample_rate`, in microseconds, rounded to the
/// nearest: 120 samples at 48 kHz are 2,500 µs, 12 samples 250 µs.
pub fn samples_to_us(samples: u64, sample_rate: u32) -> u64 {
    if sample_rate == 0 {
        return 0;
    }
    let rate = u128::from(sample_rate);
    let us = (u128::from(samples) * 1_000_000 + rate / 2) / rate;
    u64::try_from(us).unwrap_or(u64::MAX)
}

/// Microseconds as the whole milliseconds that old caches read, rounded to
/// the nearest and never 0 for a part that has a duration: the value of the
/// `duration-ms` key.
pub fn legacy_ms(us: u64) -> u32 {
    if us == 0 {
        return 0;
    }
    u32::try_from(us.saturating_add(500) / 1_000)
        .unwrap_or(u32::MAX)
        .max(1)
}

/// Microseconds rounded up to whole milliseconds, never 0: a target in the
/// old millisecond keys, which every part must fit under.
pub fn ceil_ms(us: u64) -> u32 {
    u32::try_from(us.div_ceil(1_000)).unwrap_or(u32::MAX).max(1)
}

/// Size of an audio part.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum AudioPartSize {
    /// One recovered AEP1 epoch (or one access unit) per part, whatever its
    /// sample count.
    #[default]
    Epoch,
    /// At least this many samples per part, in whole epochs.
    Samples(u32),
    /// At least this many microseconds per part, in whole epochs.
    Micros(u64),
}

impl AudioPartSize {
    /// Samples at which a part of a track at `sample_rate` closes: `None`
    /// closes it after every epoch. A FLAC part is at least
    /// [`FLAC_MIN_PART_SAMPLES`].
    pub fn target_samples(self, sample_rate: u32, flac: bool) -> Option<u64> {
        let floor = if flac { FLAC_MIN_PART_SAMPLES } else { 1 };
        match self {
            Self::Epoch => None,
            Self::Samples(samples) => Some(u64::from(samples).max(floor)),
            Self::Micros(us) => {
                let samples = (u128::from(us) * u128::from(sample_rate)).div_ceil(1_000_000);
                Some(u64::try_from(samples).unwrap_or(u64::MAX).max(floor))
            }
        }
    }

    /// Duration of a part that closes at a duration, for segmenters that
    /// count time rather than samples (the fMP4 path): `0` closes a part on
    /// every access unit. A size in samples is taken at `sample_rate`.
    pub fn min_part_us(self, sample_rate: u32) -> u64 {
        match self {
            Self::Epoch => 0,
            Self::Samples(samples) => samples_to_us(u64::from(samples), sample_rate),
            Self::Micros(us) => us,
        }
    }

    /// The configured part duration, or `0` for one epoch per part: the
    /// stamper then takes the part target from the parts it sees.
    pub fn nominal_us(self, sample_rate: u32) -> u64 {
        self.min_part_us(sample_rate)
    }

    /// The old `--fmp4-part-ms`: a part of at least this duration.
    pub fn from_legacy_ms(ms: u32) -> Self {
        Self::Micros(u64::from(ms.max(1)) * 1_000)
    }
}

impl fmt::Display for AudioPartSize {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Epoch => formatter.write_str("epoch"),
            Self::Samples(samples) => write!(formatter, "{samples}samples"),
            Self::Micros(us) => write!(formatter, "{us}us"),
        }
    }
}

impl FromStr for AudioPartSize {
    type Err = String;

    /// `epoch`, `<N>samples`, `<N>us` or `<N>ms`, with N > 0.
    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let value = value.trim();
        if value.eq_ignore_ascii_case("epoch") {
            return Ok(Self::Epoch);
        }
        let positive = |number: &str| -> Result<u64, String> {
            let number = number
                .trim()
                .parse::<u64>()
                .map_err(|_| format!("audio part {value:?}: not a whole number"))?;
            if number == 0 {
                return Err(format!("audio part {value:?} must be positive"));
            }
            Ok(number)
        };
        if let Some(number) = value.strip_suffix("samples") {
            let samples = u32::try_from(positive(number)?)
                .map_err(|_| format!("audio part {value:?} is too large"))?;
            return Ok(Self::Samples(samples));
        }
        if let Some(number) = value.strip_suffix("us") {
            return Ok(Self::Micros(positive(number)?));
        }
        if let Some(number) = value.strip_suffix("ms") {
            return Ok(Self::Micros(positive(number)?.saturating_mul(1_000)));
        }
        Err(format!(
            "audio part {value:?}: expected epoch, <N>samples, <N>us or <N>ms"
        ))
    }
}

/// Samples of every part of a rendition whose epochs are `frame_count`
/// samples: one epoch for [`AudioPartSize::Epoch`], otherwise the configured
/// size rounded up to whole epochs (a FLAC part keeps its floor). Never 0.
pub fn part_samples(size: AudioPartSize, sample_rate: u32, frame_count: u32, flac: bool) -> u64 {
    let frame = u64::from(frame_count.max(1));
    match size.target_samples(sample_rate, flac) {
        None => frame,
        Some(target) => target.div_ceil(frame).max(1).saturating_mul(frame),
    }
}

/// The part that the epoch at `pts_samples` belongs to, and the epoch's
/// place in it, for parts of `part_samples` cut on absolute boundaries of the
/// session's sample count: part `pts / P`, place `(pts % P) / frame_count`.
pub fn part_place(pts_samples: u64, part_samples: u64, frame_count: u32) -> (u64, u64) {
    let part = part_samples.max(1);
    (
        pts_samples / part,
        (pts_samples % part) / u64::from(frame_count.max(1)),
    )
}

/// Where one opaque audio part sits on its source's timeline: everything
/// that names the part. All of it comes from the AEP1 header fields of the
/// part's epochs and from settings that every contributor of a stream shares,
/// so two contributors compute the same anchor for the same epochs.
///
/// The arithmetic is in `u128` with floor division and then narrowed: a
/// `u64` product of a sample count and 10^9 wraps after 106 hours at 48 kHz.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SourceAnchor {
    /// The AEP1 session id: the session's start in Unix nanoseconds.
    pub session_id_ns: u64,
    /// `pts_samples` of the part's first epoch: a multiple of
    /// `part_samples` plus the session's phase.
    pub first_pts_samples: u64,
    /// `pts_samples` of the part's last epoch.
    pub last_pts_samples: u64,
    /// Samples of every part of the rendition ([`part_samples`]).
    pub part_samples: u64,
    pub sample_rate: u32,
}

impl SourceAnchor {
    /// The part number: the RelaySession object number, the cache slot id
    /// and the LL-HLS part number.
    pub fn part_number(&self) -> u64 {
        self.first_pts_samples / self.part_samples.max(1)
    }

    /// The key epoch of the part's session: the session id in microseconds,
    /// never 0.
    pub fn key_epoch(&self) -> u64 {
        (self.session_id_ns / 1_000).max(1)
    }

    /// `samples` of the session as time at `per_second` units a second.
    fn media_time(&self, samples: u64, per_second: u128) -> u128 {
        if self.sample_rate == 0 {
            return 0;
        }
        u128::from(samples) * per_second / u128::from(self.sample_rate)
    }

    /// The part's time on the source's clock, in Unix nanoseconds: the
    /// instant its last epoch is due to leave the source,
    /// `session_id + last_pts / rate`.
    pub fn source_time_ns(&self) -> i64 {
        let ns =
            u128::from(self.session_id_ns) + self.media_time(self.last_pts_samples, 1_000_000_000);
        i64::try_from(ns).unwrap_or(i64::MAX)
    }

    /// The part's deadline, `budget_ms` after its source time: a point on
    /// the stream's own timeline, not on any host's clock.
    pub fn deadline_ns(&self, budget_ms: u64) -> i64 {
        let ns = u128::from(self.session_id_ns)
            + self.media_time(self.last_pts_samples, 1_000_000_000)
            + u128::from(budget_ms) * 1_000_000;
        i64::try_from(ns).unwrap_or(i64::MAX)
    }

    /// The start of the part's cell on a grid of `cell_us` over the
    /// session's media time, in microseconds from the session's start. The
    /// first part a contributor publishes in a cell carries the stream's
    /// descriptor whole.
    pub fn grid_us(&self, cell_us: u64) -> u64 {
        let cell = u128::from(cell_us.max(1));
        let us = self.media_time(self.last_pts_samples, 1_000_000) / cell * cell;
        u64::try_from(us).unwrap_or(u64::MAX)
    }
}

/// Size of a video part.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum VideoPartSize {
    /// One access unit (frame) per part.
    #[default]
    Frame,
    /// At least this many milliseconds per part, in whole frames.
    Millis(u32),
}

impl VideoPartSize {
    /// `min_part_ms` of the segmenter: [`ONE_UNIT_PER_PART_MS`] for frames.
    pub fn min_part_ms(self) -> u32 {
        match self {
            Self::Frame => ONE_UNIT_PER_PART_MS,
            Self::Millis(ms) => ms.max(1),
        }
    }

    /// The configured part duration, or `0` for one frame per part: the
    /// stamper then takes the part target from the frame durations.
    pub fn nominal_us(self) -> u64 {
        u64::from(self.min_part_ms()) * 1_000
    }
}

impl fmt::Display for VideoPartSize {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Frame => formatter.write_str("frame"),
            Self::Millis(ms) => write!(formatter, "{ms}ms"),
        }
    }
}

impl FromStr for VideoPartSize {
    type Err = String;

    /// `frame` or `<N>ms`, with N > 0.
    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let value = value.trim();
        if value.eq_ignore_ascii_case("frame") {
            return Ok(Self::Frame);
        }
        if let Some(number) = value.strip_suffix("ms") {
            let ms = number
                .trim()
                .parse::<u32>()
                .map_err(|_| format!("video part {value:?}: not a whole number"))?;
            if ms == 0 {
                return Err(format!("video part {value:?} must be positive"));
            }
            return Ok(Self::Millis(ms));
        }
        Err(format!("video part {value:?}: expected frame or <N>ms"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sample_counts_map_to_exact_microseconds() {
        assert_eq!(samples_to_us(120, 48_000), 2_500);
        assert_eq!(samples_to_us(48, 48_000), 1_000);
        assert_eq!(samples_to_us(24, 48_000), 500);
        assert_eq!(samples_to_us(12, 48_000), 250);
        assert_eq!(samples_to_us(240, 48_000), 5_000);
        // 44.1 kHz: 1 ms is not a whole number of samples.
        assert_eq!(samples_to_us(44, 44_100), 998);
        assert_eq!(samples_to_us(1, 0), 0);
    }

    #[test]
    fn legacy_milliseconds_round_and_never_reach_zero() {
        assert_eq!(legacy_ms(2_500), 3);
        assert_eq!(legacy_ms(1_000), 1);
        assert_eq!(legacy_ms(500), 1);
        assert_eq!(legacy_ms(250), 1);
        assert_eq!(legacy_ms(5_000), 5);
        assert_eq!(legacy_ms(41_708), 42);
        assert_eq!(legacy_ms(0), 0);
        assert_eq!(ceil_ms(2_500), 3);
        assert_eq!(ceil_ms(250), 1);
        assert_eq!(ceil_ms(5_000), 5);
        assert_eq!(ceil_ms(0), 1);
    }

    #[test]
    fn audio_part_sizes_parse_and_print() {
        for (text, size) in [
            ("epoch", AudioPartSize::Epoch),
            ("48samples", AudioPartSize::Samples(48)),
            ("2500us", AudioPartSize::Micros(2_500)),
            ("5ms", AudioPartSize::Micros(5_000)),
        ] {
            assert_eq!(text.parse::<AudioPartSize>().unwrap(), size);
        }
        for size in [
            AudioPartSize::Epoch,
            AudioPartSize::Samples(12),
            AudioPartSize::Micros(250),
        ] {
            assert_eq!(size.to_string().parse::<AudioPartSize>().unwrap(), size);
        }
        for bad in ["", "0samples", "0us", "2.5ms", "48", "frame"] {
            assert!(bad.parse::<AudioPartSize>().is_err(), "{bad}");
        }
        assert_eq!(
            AudioPartSize::from_legacy_ms(5),
            AudioPartSize::Micros(5_000)
        );
        assert_eq!(AudioPartSize::default(), AudioPartSize::Epoch);
    }

    #[test]
    fn audio_part_targets_in_samples_keep_the_flac_floor() {
        assert_eq!(AudioPartSize::Epoch.target_samples(48_000, false), None);
        assert_eq!(AudioPartSize::Epoch.target_samples(48_000, true), None);
        assert_eq!(
            AudioPartSize::Samples(12).target_samples(48_000, false),
            Some(12)
        );
        assert_eq!(
            AudioPartSize::Samples(12).target_samples(48_000, true),
            Some(FLAC_MIN_PART_SAMPLES)
        );
        assert_eq!(
            AudioPartSize::Micros(2_500).target_samples(48_000, false),
            Some(120)
        );
        assert_eq!(
            AudioPartSize::Micros(250).target_samples(48_000, true),
            Some(32)
        );
        // 1 ms at 44.1 kHz rounds up to whole samples.
        assert_eq!(
            AudioPartSize::Micros(1_000).target_samples(44_100, false),
            Some(45)
        );
        assert_eq!(AudioPartSize::Samples(120).min_part_us(48_000), 2_500);
        assert_eq!(AudioPartSize::Epoch.min_part_us(48_000), 0);
    }

    fn anchor(session_id_ns: u64, pts: u64, part: u64) -> SourceAnchor {
        SourceAnchor {
            session_id_ns,
            first_pts_samples: pts,
            last_pts_samples: pts,
            part_samples: part,
            sample_rate: 48_000,
        }
    }

    #[test]
    fn a_part_is_whole_epochs_of_the_source() {
        // One epoch per part, whatever the epoch.
        for frame in [12_u32, 48, 72, 120, 240] {
            assert_eq!(
                part_samples(AudioPartSize::Epoch, 48_000, frame, false),
                u64::from(frame)
            );
            assert_eq!(
                part_samples(AudioPartSize::Epoch, 48_000, frame, true),
                u64::from(frame)
            );
        }
        // A size is rounded up to whole epochs.
        assert_eq!(
            part_samples(AudioPartSize::Samples(48), 48_000, 12, false),
            48
        );
        assert_eq!(
            part_samples(AudioPartSize::Samples(100), 48_000, 48, false),
            144
        );
        assert_eq!(
            part_samples(AudioPartSize::Micros(5_000), 48_000, 48, false),
            240
        );
        assert_eq!(
            part_samples(AudioPartSize::Micros(4_000), 48_000, 64, false),
            192
        );
        // FLAC's floor of 32 samples, in whole epochs.
        assert_eq!(
            part_samples(AudioPartSize::Samples(12), 48_000, 16, true),
            32
        );
        assert_eq!(
            part_samples(AudioPartSize::Samples(12), 48_000, 12, true),
            36
        );
        // Never 0, whatever the header says.
        assert_eq!(part_samples(AudioPartSize::Epoch, 48_000, 0, false), 1);
    }

    #[test]
    fn an_epoch_has_one_part_and_one_place_whatever_came_before() {
        // Parts of five 48-sample epochs.
        assert_eq!(part_place(0, 240, 48), (0, 0));
        assert_eq!(part_place(192, 240, 48), (0, 4));
        assert_eq!(part_place(240, 240, 48), (1, 0));
        assert_eq!(part_place(500 * 48, 240, 48), (100, 0));
        assert_eq!(part_place(503 * 48, 240, 48), (100, 3));
        // One epoch per part: the number is the epoch's index.
        for frame in [12_u32, 48, 72, 120, 240] {
            let part = u64::from(frame);
            assert_eq!(part_place(7 * part, part, frame), (7, 0));
        }
        // A session whose pts has a phase that is not a multiple of the
        // frame still gives every epoch one part and one place.
        assert_eq!(part_place(7, 48, 48), (0, 0));
        assert_eq!(part_place(55, 48, 48), (1, 0));
        assert_eq!(part_place(7, 240, 48), (0, 0));
        assert_eq!(part_place(199, 240, 48), (0, 4));
        assert_eq!(part_place(247, 240, 48), (1, 0));
    }

    #[test]
    fn a_part_is_numbered_and_timed_from_its_source() {
        let session = 1_700_000_000_000_000_000_u64;
        // (epoch samples, epochs a second).
        for (frame, per_second) in [(12_u64, 4_000_u64), (48, 1_000), (120, 400), (240, 200)] {
            for index in [0_u64, 1, 499, per_second, 123_456_789] {
                let part = anchor(session, index * frame, frame);
                assert_eq!(part.part_number(), index, "{frame}");
                assert_eq!(part.key_epoch(), 1_700_000_000_000_000);
                let media_ns = index * 1_000_000_000 / per_second;
                assert_eq!(part.source_time_ns(), (session + media_ns) as i64);
                assert_eq!(
                    part.deadline_ns(1_000),
                    (session + media_ns + 1_000_000_000) as i64
                );
            }
        }
        // 72 samples are 1.5 ms: epoch 3 is due 4.5 ms in.
        let part = anchor(session, 3 * 72, 72);
        assert_eq!(part.part_number(), 3);
        assert_eq!(part.source_time_ns(), (session + 4_500_000) as i64);
        // A part of five epochs: numbered by its first, timed by its last.
        let part = SourceAnchor {
            session_id_ns: session,
            first_pts_samples: 500 * 48,
            last_pts_samples: 504 * 48,
            part_samples: 240,
            sample_rate: 48_000,
        };
        assert_eq!(part.part_number(), 100);
        assert_eq!(part.source_time_ns(), (session + 504_000_000) as i64);
    }

    #[test]
    fn a_pts_phase_floors_to_its_part_and_keeps_its_exact_time() {
        let session = 1_700_000_000_000_000_000_u64;
        // The session's pts began at 7, not at a multiple of the frame.
        let part = anchor(session, 7 + 5 * 48, 48);
        assert_eq!(part.part_number(), 5);
        // 247 samples: 5,145,833.3 ns, floored.
        assert_eq!(part.source_time_ns(), (session + 5_145_833) as i64);
        assert_eq!(part.grid_us(1_000), 5_000);
    }

    #[test]
    fn a_long_session_does_not_wrap_the_arithmetic() {
        let session = 1_700_000_000_000_000_000_u64;
        // 2^40 samples at 48 kHz are 265 days; times 10^9 it is past u64.
        let pts = 1_u64 << 40;
        assert!(pts.checked_mul(1_000_000_000).is_none());
        let part = anchor(session, pts, 48);
        assert_eq!(part.part_number(), pts / 48);
        assert_eq!(
            part.source_time_ns(),
            1_700_000_000_000_000_000 + 22_906_492_245_333_333
        );
        assert_eq!(
            part.deadline_ns(1_000),
            1_700_000_000_000_000_000 + 22_906_492_245_333_333 + 1_000_000_000
        );
        assert_eq!(part.grid_us(100_000), 22_906_492_200_000);
        // Out of range saturates; it never wraps.
        let part = anchor(u64::MAX, u64::MAX - 47, 48);
        assert_eq!(part.source_time_ns(), i64::MAX);
        assert_eq!(part.deadline_ns(u64::MAX), i64::MAX);
        // A rate of 0 has no media time.
        let part = SourceAnchor {
            sample_rate: 0,
            ..anchor(session, 480, 48)
        };
        assert_eq!(part.source_time_ns(), session as i64);
        assert_eq!(part.grid_us(100_000), 0);
    }

    #[test]
    fn the_grid_cell_is_the_same_for_every_part_inside_it() {
        let session = 1_700_000_000_000_000_000_u64;
        // 1 ms parts on a 100 ms grid: parts 0..=99 share cell 0.
        for index in 0..100_u64 {
            assert_eq!(anchor(session, index * 48, 48).grid_us(100_000), 0);
        }
        assert_eq!(anchor(session, 100 * 48, 48).grid_us(100_000), 100_000);
        assert_eq!(anchor(session, 250 * 48, 48).grid_us(100_000), 200_000);
        // 2.5 ms parts: 40 a cell.
        assert_eq!(anchor(session, 39 * 120, 120).grid_us(100_000), 0);
        assert_eq!(anchor(session, 40 * 120, 120).grid_us(100_000), 100_000);
        // The session id does not move the grid: it is media time.
        assert_eq!(anchor(session + 37, 100 * 48, 48).grid_us(100_000), 100_000);
        // A cell of 0 is taken as 1 µs.
        assert_eq!(anchor(session, 48, 48).grid_us(0), 1_000);
    }

    #[test]
    fn video_part_sizes_parse_and_print() {
        assert_eq!(
            "frame".parse::<VideoPartSize>().unwrap(),
            VideoPartSize::Frame
        );
        assert_eq!(
            "200ms".parse::<VideoPartSize>().unwrap(),
            VideoPartSize::Millis(200)
        );
        assert!("0ms".parse::<VideoPartSize>().is_err());
        assert!("epoch".parse::<VideoPartSize>().is_err());
        assert_eq!(VideoPartSize::Frame.min_part_ms(), ONE_UNIT_PER_PART_MS);
        assert_eq!(VideoPartSize::Frame.nominal_us(), 0);
        assert_eq!(VideoPartSize::Millis(40).nominal_us(), 40_000);
        assert_eq!(VideoPartSize::Frame.to_string(), "frame");
        assert_eq!(VideoPartSize::Millis(40).to_string(), "40ms");
    }
}
