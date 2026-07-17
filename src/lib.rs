pub mod audio_epoch_hls;
pub mod fmp4_bridge;
pub mod ingress_authorization;

use access_unit::{detect_audio, h264, AudioType};
#[cfg(test)]
use bytes::Bytes;
pub use raptorq_datagram_fec::{decode_serialized_media_access_unit, SerializedMediaAccessUnit};
use raptorq_datagram_fec::{MediaCodec, MediaFrameFlags, MediaFrameMetadata};
#[cfg(test)]
use raptorq_datagram_fec::{MediaFragmentHeader, MEDIA_FRAME_HEADER_LEN};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MediaAccessUnitParams {
    pub stream_id: u64,
    pub sequence: Option<u64>,
    pub pts_ms: u64,
    pub dts_ms: Option<u64>,
    pub duration_ms: u32,
    pub codec: MediaCodec,
    pub codec_explicit: bool,
    pub flags: MediaFrameFlags,
}

impl MediaAccessUnitParams {
    pub fn parse(
        query: Option<&str>,
        default_stream_id: u64,
        default_pts_ms: u64,
    ) -> Result<Self, String> {
        let mut params = Self {
            stream_id: default_stream_id,
            sequence: None,
            pts_ms: default_pts_ms,
            dts_ms: None,
            duration_ms: 0,
            codec: MediaCodec::Data,
            codec_explicit: false,
            flags: MediaFrameFlags::default(),
        };

        for (key, value) in form_urlencoded::parse(query.unwrap_or("").as_bytes()) {
            match key.as_ref() {
                "stream_id" | "stream" => {
                    params.stream_id = parse_query_u64("stream_id", &value)?;
                }
                "sequence" | "seq" => {
                    params.sequence = Some(parse_query_u64("sequence", &value)?);
                }
                "pts_ms" | "pts" => {
                    params.pts_ms = parse_query_u64("pts_ms", &value)?;
                }
                "dts_ms" | "dts" => {
                    params.dts_ms = Some(parse_query_u64("dts_ms", &value)?);
                }
                "duration_ms" | "duration" => {
                    params.duration_ms = parse_query_u32("duration_ms", &value)?;
                }
                "codec" => {
                    if value.eq_ignore_ascii_case("auto") {
                        params.codec = MediaCodec::Data;
                        params.codec_explicit = false;
                    } else {
                        params.codec = parse_media_codec(&value)?;
                        params.codec_explicit = true;
                    }
                }
                "flags" => {
                    params.flags = MediaFrameFlags::new(parse_query_u16("flags", &value)?);
                }
                "keyframe" => {
                    if parse_query_bool("keyframe", &value)? {
                        params.flags = params.flags.with(MediaFrameFlags::KEYFRAME);
                    }
                }
                "codec_config" => {
                    if parse_query_bool("codec_config", &value)? {
                        params.flags = params.flags.with(MediaFrameFlags::CODEC_CONFIG);
                    }
                }
                "discontinuity" => {
                    if parse_query_bool("discontinuity", &value)? {
                        params.flags = params.flags.with(MediaFrameFlags::DISCONTINUITY);
                    }
                }
                "end_of_stream" | "eos" => {
                    if parse_query_bool("end_of_stream", &value)? {
                        params.flags = params.flags.with(MediaFrameFlags::END_OF_STREAM);
                    }
                }
                "" => {}
                other => {
                    return Err(format!(
                        "unsupported media access-unit query field `{other}`"
                    ))
                }
            }
        }

        if params.duration_ms > u32::from(u16::MAX) {
            return Err("duration_ms must fit in u16 for media access-unit metadata".into());
        }
        if params.flags.bits() > u16::from(u8::MAX) {
            return Err("flags must fit in u8 for media access-unit metadata".into());
        }
        if let Some(sequence) = params.sequence {
            usize::try_from(sequence)
                .map_err(|_| "sequence is too large for this platform".to_string())?;
        }

        Ok(params)
    }

    pub fn metadata(&self, sequence: u64) -> Result<MediaFrameMetadata, String> {
        self.metadata_with_codec(sequence, self.codec)
    }

    pub fn metadata_for_payload(
        &self,
        sequence: u64,
        payload: &[u8],
    ) -> Result<MediaFrameMetadata, String> {
        let codec = if self.codec_explicit {
            self.codec
        } else {
            infer_media_codec(payload)
        };
        self.metadata_with_codec(sequence, codec)
    }

    fn metadata_with_codec(
        &self,
        sequence: u64,
        codec: MediaCodec,
    ) -> Result<MediaFrameMetadata, String> {
        usize::try_from(sequence)
            .map_err(|_| "sequence is too large for this platform".to_string())?;
        let mut metadata = MediaFrameMetadata::new(self.stream_id, sequence, self.pts_ms, codec);
        metadata.dts_ms = self.dts_ms;
        metadata.duration_ms = self.duration_ms;
        metadata.flags = self.flags;
        Ok(metadata)
    }
}

pub fn infer_media_codec(payload: &[u8]) -> MediaCodec {
    if h264::is_nalu(payload) {
        return MediaCodec::H264;
    }

    match detect_audio(payload) {
        AudioType::AAC | AudioType::M4A => MediaCodec::Aac,
        AudioType::Opus | AudioType::OggOpus => MediaCodec::Opus,
        _ => MediaCodec::Data,
    }
}

pub fn parse_media_codec(value: &str) -> Result<MediaCodec, String> {
    match value.to_ascii_lowercase().as_str() {
        "auto" => Ok(MediaCodec::Data),
        "unknown" => Ok(MediaCodec::Unknown),
        "h264" | "avc" | "video/h264" => Ok(MediaCodec::H264),
        "opus" | "audio/opus" => Ok(MediaCodec::Opus),
        "aac" | "audio/aac" => Ok(MediaCodec::Aac),
        "data" | "application/octet-stream" => Ok(MediaCodec::Data),
        other => Err(format!("unsupported media codec `{other}`")),
    }
}

pub fn codec_name(codec: MediaCodec) -> &'static str {
    match codec {
        MediaCodec::Unknown => "unknown",
        MediaCodec::H264 => "h264",
        MediaCodec::Opus => "opus",
        MediaCodec::Aac => "aac",
        MediaCodec::Data => "data",
    }
}

fn parse_query_u64(field: &str, value: &str) -> Result<u64, String> {
    value
        .parse()
        .map_err(|err| format!("invalid {field} `{value}`: {err}"))
}

fn parse_query_u32(field: &str, value: &str) -> Result<u32, String> {
    value
        .parse()
        .map_err(|err| format!("invalid {field} `{value}`: {err}"))
}

fn parse_query_u16(field: &str, value: &str) -> Result<u16, String> {
    value
        .parse()
        .map_err(|err| format!("invalid {field} `{value}`: {err}"))
}

fn parse_query_bool(field: &str, value: &str) -> Result<bool, String> {
    match value.to_ascii_lowercase().as_str() {
        "1" | "true" | "yes" | "on" => Ok(true),
        "0" | "false" | "no" | "off" => Ok(false),
        _ => Err(format!("invalid {field} `{value}`: expected boolean")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_media_access_unit_query() {
        let params = MediaAccessUnitParams::parse(
            Some("stream_id=4294967351&sequence=7&codec=h264&pts_ms=1234&duration_ms=33&keyframe=true"),
            1,
            999,
        )
        .unwrap();

        assert_eq!(params.stream_id, u64::from(u32::MAX) + 56);
        assert_eq!(params.sequence, Some(7));
        assert_eq!(params.pts_ms, 1234);
        assert_eq!(params.duration_ms, 33);
        assert_eq!(params.codec, MediaCodec::H264);
        assert!(params.codec_explicit);
        assert!(params.flags.is_keyframe());

        let metadata = params.metadata(7).unwrap();
        assert_eq!(metadata.stream_id, u64::from(u32::MAX) + 56);
        assert_eq!(metadata.sequence, 7);
        assert_eq!(metadata.codec, MediaCodec::H264);
    }

    #[test]
    fn infers_media_codec_with_access_unit() {
        let params = MediaAccessUnitParams::parse(Some("stream_id=55&sequence=7"), 1, 999).unwrap();

        let metadata = params
            .metadata_for_payload(7, &[0x00, 0x00, 0x01, 0x65, 0x88])
            .unwrap();
        assert_eq!(metadata.codec, MediaCodec::H264);

        let metadata = params
            .metadata_for_payload(
                8,
                &[
                    0xff, 0xf1, 0x50, 0x80, 0x01, 0x7f, 0xfc, 0x21, 0x10, 0x04, 0x60,
                ],
            )
            .unwrap();
        assert_eq!(metadata.codec, MediaCodec::Aac);
    }

    #[test]
    fn decodes_serialized_media_access_unit() {
        let metadata = MediaFrameMetadata {
            duration_ms: 20,
            flags: MediaFrameFlags::keyframe(),
            ..MediaFrameMetadata::new(91, 2, 400, MediaCodec::Opus)
        };
        let header = MediaFragmentHeader {
            metadata,
            fragment_index: 0,
            fragment_count: 1,
            access_unit_len: b"opus-frame".len() as u32,
            fragment_offset: 0,
        };
        let mut bytes = vec![0; MEDIA_FRAME_HEADER_LEN];
        header.encode(&mut bytes[..]).unwrap();
        bytes.extend_from_slice(b"opus-frame");

        let unit = decode_serialized_media_access_unit(Bytes::from(bytes))
            .unwrap()
            .unwrap();
        assert_eq!(unit.metadata, metadata);
        assert_eq!(unit.payload, Bytes::from_static(b"opus-frame"));
    }

    #[test]
    fn rejects_payload_length_mismatch() {
        let metadata = MediaFrameMetadata::new(1, 0, 0, MediaCodec::Data);
        let header = MediaFragmentHeader {
            metadata,
            fragment_index: 0,
            fragment_count: 1,
            access_unit_len: 99,
            fragment_offset: 0,
        };
        let mut bytes = vec![0; MEDIA_FRAME_HEADER_LEN];
        header.encode(&mut bytes[..]).unwrap();
        bytes.extend_from_slice(b"short");

        let error = decode_serialized_media_access_unit(Bytes::from(bytes)).unwrap_err();
        assert!(error.contains("payload length mismatch"));
    }
}
