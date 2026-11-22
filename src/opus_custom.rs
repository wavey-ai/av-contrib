//! Opus Custom frames in the private SoundKit Opus rendition.
//!
//! The low-latency Opus lane can carry Opus Custom CELT frames smaller than
//! standard Opus allows: 48, 64 or 72 samples at 48 kHz (1, 1.33 and
//! 1.5 ms), from SoundKit Opus on the branch `opus-custom-1ms`. The
//! contract, shared with the producer (daw-nexus `src/opus_lane.rs`):
//!
//! - The SoundKit v2 header stays `EncodingFlag::Opus`; `frame_count` is the
//!   frame size. [`soundkit_opus::custom_frame_size`] tells a custom count
//!   (48 kHz, not a multiple of 120) from a standard one; every multiple of
//!   120 is standard Opus with a TOC byte.
//! - Custom packets carry libopus's one-byte signalling header, the libopus
//!   default (`CELT_SET_SIGNALLING(1)`, [`LANE_OPUS_CUSTOM_SIGNALLING`]), so
//!   every SoundKit or libopus custom decoder (the browser's WASM
//!   `Decoder(channels, 48000, frame_count)` included) reads them with its
//!   defaults, and the byte checks each packet's frame size and channels.
//!   It costs 8 kb/s at 1 ms. The constant is the lane's one switch, shared
//!   by the encoder and decoders here.
//! - Opus Custom has no OpusHead/dOps and no RTP payload format, so fMP4 and
//!   Ogg boxing, WebRTC/RTP, talkback and the public programme rendition
//!   take standard Opus only ([`refuse_custom_opus`]).

use soundkit_opus::{custom_frame_size, OpusDecoder, OpusEncoder};

/// Whether custom packets of the lane carry libopus's signalling byte: on,
/// the libopus default (daw-nexus `opus_lane::LANE_OPUS_CUSTOM_SIGNALLING`).
pub const LANE_OPUS_CUSTOM_SIGNALLING: bool = true;
/// The lane's default Opus frame (daw-nexus
/// `LANE_DEFAULT_OPUS_FRAME_SAMPLES`): 48 samples, 1 ms Opus Custom.
pub const LANE_DEFAULT_OPUS_FRAME_SAMPLES: u32 = 48;
/// The lane's default Opus bitrate, CBR (daw-nexus
/// `LANE_DEFAULT_OPUS_BITRATE`): the smallest frame and rate that meet the
/// quality bar (at least standard 5 ms at 192 kb/s on PEAQ and ViSQOL).
pub const LANE_DEFAULT_OPUS_BITRATE: u32 = 320_000;
/// The documented lower-bandwidth option at 1 ms (about standard 2.5 ms at
/// 192 kb/s; below the lane's quality bar).
pub const LOWER_BANDWIDTH_OPUS_BITRATE: u32 = 256_000;

/// True when `frame_count` samples at `sample_rate` is an Opus Custom frame.
pub fn is_custom_opus_frame(sample_rate: u32, frame_count: u32) -> bool {
    custom_frame_size(sample_rate, frame_count as usize).is_some()
}

/// `"custom"` or `"standard"`, for reports.
pub fn opus_mode(sample_rate: u32, frame_count: u32) -> &'static str {
    if is_custom_opus_frame(sample_rate, frame_count) {
        "custom"
    } else {
        "standard"
    }
}

/// Refuses an Opus Custom frame for an output that only standard Opus can
/// describe; `output` names that output in the error.
pub fn refuse_custom_opus(sample_rate: u32, frame_count: u32, output: &str) -> Result<(), String> {
    if is_custom_opus_frame(sample_rate, frame_count) {
        return Err(format!(
            "{output} needs standard Opus, but this rendition carries Opus Custom frames of \
             {frame_count} samples ({:.2} ms at {sample_rate} Hz), which have no OpusHead and no \
             RTP payload format; use --daw-hls-packaging opaque for this rendition, or have the \
             producer send standard Opus (--opus-frame-ms 2.5 or larger)",
            f64::from(frame_count) * 1_000.0 / f64::from(sample_rate.max(1))
        ));
    }
    Ok(())
}

/// The decoder for packets whose SoundKit v2 header declares `frame_count`
/// samples: Opus Custom (signalling as [`LANE_OPUS_CUSTOM_SIGNALLING`]) for a
/// custom count, the standard
/// decoder (any standard duration) otherwise.
pub fn new_lane_opus_decoder(
    sample_rate: u32,
    channels: usize,
    frame_count: u32,
) -> Result<OpusDecoder, String> {
    let rate = i32::try_from(sample_rate).map_err(|_| "Opus sample rate is invalid".to_string())?;
    let mut decoder = OpusDecoder::new_for_frame_count(rate, channels, frame_count as usize)
        .map_err(|error| format!("Opus decoder for {frame_count}-sample frames: {error}"))?;
    if decoder.is_custom() {
        decoder
            .set_custom_signalling(LANE_OPUS_CUSTOM_SIGNALLING)
            .map_err(|error| format!("Opus Custom signalling: {error}"))?;
    }
    Ok(decoder)
}

/// Whether `decoder` still fits packets of this format: the same rate and
/// channels, and the same custom frame (or both standard).
pub fn lane_opus_decoder_matches(
    decoder: &OpusDecoder,
    sample_rate: u32,
    channels: usize,
    frame_count: u32,
) -> bool {
    i64::from(decoder.sample_rate()) == i64::from(sample_rate)
        && decoder.channels() == channels
        && decoder.custom_frame_size() == custom_frame_size(sample_rate, frame_count as usize)
}

/// The lane's encoder: Opus Custom (signalling as
/// [`LANE_OPUS_CUSTOM_SIGNALLING`]) for a custom frame,
/// standard CELT-only Opus otherwise, CBR at `bitrate`, 16-bit input.
pub fn new_lane_opus_encoder(
    sample_rate: u32,
    channels: u32,
    frame_count: u32,
    bitrate: u32,
) -> Result<OpusEncoder, String> {
    let mut encoder = OpusEncoder::try_new(sample_rate, 16, channels, frame_count, bitrate)?;
    encoder.init()?;
    if encoder.is_custom() {
        encoder
            .set_custom_signalling(LANE_OPUS_CUSTOM_SIGNALLING)
            .map_err(|error| format!("Opus Custom signalling: {error}"))?;
    }
    Ok(encoder)
}

#[cfg(test)]
mod tests {
    use super::*;
    use soundkit::audio_packet::Decoder as _;

    #[test]
    fn frame_counts_choose_custom_or_standard_opus() {
        for frame in [48, 64, 72] {
            assert!(is_custom_opus_frame(48_000, frame));
            assert_eq!(opus_mode(48_000, frame), "custom");
            let error = refuse_custom_opus(48_000, frame, "fMP4").unwrap_err();
            assert!(error.contains("Opus Custom"), "{error}");
        }
        for frame in [120, 240, 480, 960] {
            assert!(!is_custom_opus_frame(48_000, frame));
            assert_eq!(opus_mode(48_000, frame), "standard");
            refuse_custom_opus(48_000, frame, "fMP4").unwrap();
        }
    }

    #[test]
    fn lane_codecs_round_trip() {
        for frame in [48u32, 64, 72, 120, 240] {
            let mut encoder =
                new_lane_opus_encoder(48_000, 2, frame, LANE_DEFAULT_OPUS_BITRATE).unwrap();
            let mut decoder = new_lane_opus_decoder(48_000, 2, frame).unwrap();
            assert!(lane_opus_decoder_matches(&decoder, 48_000, 2, frame));
            // A decoder with SoundKit's defaults (what a browser builds from
            // the header's frame count) reads the lane's packets too.
            let mut default_decoder =
                OpusDecoder::new_for_frame_count(48_000, 2, frame as usize).unwrap();
            let pcm = vec![1_000i16; frame as usize * 2];
            let mut packet = vec![0u8; 1_500];
            let len = encoder.encode_i16(&pcm, &mut packet).unwrap();
            let mut out = vec![0i16; frame as usize * 2];
            let decoded = decoder.decode_i16(&packet[..len], &mut out, false).unwrap();
            assert_eq!(decoded, frame as usize);
            let mut default_out = vec![0i16; frame as usize * 2];
            let decoded = default_decoder
                .decode_i16(&packet[..len], &mut default_out, false)
                .unwrap();
            assert_eq!(decoded, frame as usize);
            assert_eq!(out, default_out);
            if is_custom_opus_frame(48_000, frame) {
                assert!(decoder.custom_signalling());
                // CBR at 320 kb/s, signalling byte included; bit 2 of the
                // byte is the stereo flag.
                assert_eq!(len, (320_000 * frame as usize + 4 * 48_000) / (8 * 48_000));
                assert_eq!(packet[0] & 0x04, 0x04);
            }
        }
        let custom = new_lane_opus_decoder(48_000, 2, 48).unwrap();
        assert!(!lane_opus_decoder_matches(&custom, 48_000, 2, 120));
        assert!(!lane_opus_decoder_matches(&custom, 48_000, 2, 64));
    }
}
