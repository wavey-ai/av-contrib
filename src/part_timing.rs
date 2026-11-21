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
