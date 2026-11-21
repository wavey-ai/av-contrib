//! Codec-neutral SoundKit v2 framing at the contributor boundary.
//!
//! Mesh and FEC stages carry these packets as opaque bytes. Codec-specific
//! interpretation belongs only in a terminal decoder or packager.

use bytes::Bytes;
use frame_header::{EncodingFlag, Endianness, FrameHeaderV2};
use music_audio_session::{
    DecodedMultichannelAudioEpoch, DecodedMultichannelAudioGroup, MultichannelAudioReceiver,
    MultichannelAudioSender, MultichannelAudioSessionConfig,
};
use raptorq_datagram_fec::{
    AudioPayloadKind, AudioSampleFormat, MultichannelAudioEpoch, MultichannelAudioFecConfig,
    MultichannelAudioGroup,
};
use raptorq_fec_transport::MultichannelAudioTransportAdapter;
use std::collections::HashMap;
use std::io::Cursor;
use std::net::SocketAddr;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SoundKitV2PacketMetadata {
    pub encoding: EncodingFlag,
    pub frame_count: u32,
    pub sample_rate: u32,
    pub channels: u8,
    pub bits_per_sample: u8,
    pub endianness: Endianness,
    pub id: Option<u64>,
    pub pts: Option<u64>,
}

#[derive(Debug, Clone)]
pub struct SoundKitV2PacketRef<'a> {
    pub header: FrameHeaderV2,
    pub encoded_header: &'a [u8],
    pub payload: &'a [u8],
}

#[derive(Debug, Clone)]
pub struct SoundKitV2StreamRef<'a> {
    pub packets: Vec<SoundKitV2PacketRef<'a>>,
    pub total_frame_count: u64,
}

impl<'a> SoundKitV2StreamRef<'a> {
    pub fn parse(bytes: &'a [u8], require_crc32: bool) -> Result<Self, String> {
        if bytes.is_empty() {
            return Err("SoundKit v2 stream is empty".to_string());
        }

        let mut packets = Vec::new();
        let mut total_frame_count = 0_u64;
        let mut offset = 0_usize;
        while offset < bytes.len() {
            let remaining = &bytes[offset..];
            let header_size = FrameHeaderV2::header_size(remaining)
                .map_err(|error| format!("invalid SoundKit v2 header at byte {offset}: {error}"))?;
            let encoded_header = remaining.get(..header_size).ok_or_else(|| {
                format!(
                    "truncated SoundKit v2 header at byte {offset}: need {header_size}, have {}",
                    remaining.len()
                )
            })?;
            let mut reader = Cursor::new(encoded_header);
            let header = FrameHeaderV2::decode(&mut reader)
                .map_err(|error| format!("invalid SoundKit v2 header at byte {offset}: {error}"))?;
            if header.frame_count() == 0 {
                return Err(format!(
                    "SoundKit v2 packet at byte {offset} declares zero frames"
                ));
            }
            let payload_size = usize::try_from(header.payload_size())
                .map_err(|_| "SoundKit v2 payload size exceeds this platform".to_string())?;
            let packet_size = header_size
                .checked_add(payload_size)
                .ok_or_else(|| "SoundKit v2 packet size overflow".to_string())?;
            let packet = remaining.get(..packet_size).ok_or_else(|| {
                format!(
                    "truncated SoundKit v2 payload at byte {offset}: need {payload_size}, have {}",
                    remaining.len().saturating_sub(header_size)
                )
            })?;
            let payload = &packet[header_size..];
            match header.packet_crc32_value() {
                Some(_) => {
                    if !header.verify_packet_crc32(encoded_header, payload)? {
                        return Err(format!("SoundKit v2 packet CRC mismatch at byte {offset}"));
                    }
                }
                None if require_crc32 => {
                    return Err(format!(
                        "SoundKit v2 packet at byte {offset} does not carry a CRC"
                    ));
                }
                None => {}
            }

            total_frame_count = total_frame_count
                .checked_add(u64::from(header.frame_count()))
                .ok_or_else(|| "SoundKit v2 stream frame count overflow".to_string())?;
            packets.push(SoundKitV2PacketRef {
                header,
                encoded_header,
                payload,
            });
            offset += packet_size;
        }

        Ok(Self {
            packets,
            total_frame_count,
        })
    }
}

pub fn frame_soundkit_v2_packet(
    metadata: SoundKitV2PacketMetadata,
    payload: &[u8],
) -> Result<Bytes, String> {
    let payload_size = u32::try_from(payload.len())
        .map_err(|_| "SoundKit v2 payload exceeds the 32-bit wire limit".to_string())?;
    let header = FrameHeaderV2::new(
        metadata.encoding,
        payload_size,
        metadata.frame_count,
        metadata.sample_rate,
        metadata.channels,
        metadata.bits_per_sample,
        metadata.endianness,
        metadata.id,
        metadata.pts,
        None,
    )?
    .with_packet_crc32(payload)?;
    let mut packet = Vec::with_capacity(header.size().saturating_add(payload.len()));
    header
        .encode(&mut packet)
        .map_err(|error| format!("failed to encode SoundKit v2 header: {error}"))?;
    packet.extend_from_slice(payload);
    Ok(Bytes::from(packet))
}

pub struct SoundKitV2EpochNormalizer {
    transport: MultichannelAudioTransportAdapter,
    receivers: HashMap<SocketAddr, MultichannelAudioReceiver>,
    sender: MultichannelAudioSender,
}

impl SoundKitV2EpochNormalizer {
    pub fn udp(max_datagram_size: usize) -> Self {
        let transport = MultichannelAudioTransportAdapter::udp(max_datagram_size);
        let fec = transport.prepare_fec_config(MultichannelAudioFecConfig::default());
        Self {
            transport,
            receivers: HashMap::new(),
            sender: MultichannelAudioSender::new(MultichannelAudioSessionConfig {
                fec,
                ..MultichannelAudioSessionConfig::default()
            }),
        }
    }

    pub fn push_datagram(
        &mut self,
        peer: SocketAddr,
        datagram: &[u8],
    ) -> Result<Option<Vec<Bytes>>, String> {
        let payload = self
            .transport
            .payload(datagram)
            .map_err(|error| format!("invalid AEP1 transport datagram: {error}"))?;
        let outcome = self
            .receivers
            .entry(peer)
            .or_insert_with(|| {
                MultichannelAudioReceiver::new(MultichannelAudioSessionConfig::default())
            })
            .push_datagram(payload)
            .map_err(|error| format!("cannot recover contributor AEP1 epoch: {error}"))?;
        let Some(epoch) = outcome.completed_epoch else {
            return Ok(None);
        };
        self.normalize_epoch(epoch).map(Some)
    }

    /// Re-frame an epoch that another FEC receiver already recovered (the
    /// LL-HLS worker's, with `--aep1-shared-recovery`): the same output as
    /// [`Self::push_datagram`] gives for its last datagram.
    pub fn push_recovered_epoch(
        &mut self,
        epoch: DecodedMultichannelAudioEpoch,
    ) -> Result<Vec<Bytes>, String> {
        self.normalize_epoch(epoch)
    }

    fn normalize_epoch(
        &mut self,
        epoch: DecodedMultichannelAudioEpoch,
    ) -> Result<Vec<Bytes>, String> {
        let payloads = epoch
            .groups
            .iter()
            .map(canonicalize_group_payload)
            .collect::<Result<Vec<_>, _>>()?;
        let groups = epoch
            .groups
            .iter()
            .zip(payloads.iter())
            .map(|(group, payload)| MultichannelAudioGroup {
                group_id: group.group_id,
                channel_start: group.channel_start,
                channel_count: group.channel_count,
                payload_kind: AudioPayloadKind::SoundKitV2,
                sample_format: AudioSampleFormat::Unspecified,
                flags: group.flags,
                payload,
            })
            .collect::<Vec<_>>();
        let encoded = self
            .sender
            .encode_epoch(MultichannelAudioEpoch {
                session_id: epoch.session_id,
                config_generation: epoch.config_generation,
                epoch_id: epoch.epoch_id,
                pts_samples: epoch.pts_samples,
                sample_rate: epoch.sample_rate,
                frame_count: epoch.frame_count,
                groups: &groups,
            })
            .map_err(|error| format!("cannot encode canonical SoundKit v2 epoch: {error}"))?;
        let wrapped = self
            .transport
            .wrap_epoch(encoded)
            .map_err(|error| format!("cannot frame canonical SoundKit v2 epoch: {error}"))?;
        Ok(wrapped
            .datagrams
            .into_iter()
            .map(|datagram| datagram.payload)
            .collect())
    }
}

fn canonicalize_group_payload(group: &DecodedMultichannelAudioGroup) -> Result<Bytes, String> {
    if group.payload_kind != AudioPayloadKind::SoundKitV2 {
        return Err("DAW mesh audio requires canonical SoundKit v2 framing".to_string());
    }
    validate_soundkit_group(group, true)?;
    Ok(group.payload.clone())
}

fn validate_soundkit_group(
    group: &DecodedMultichannelAudioGroup,
    require_crc32: bool,
) -> Result<EncodingFlag, String> {
    let stream = SoundKitV2StreamRef::parse(&group.payload, require_crc32)?;
    if stream.total_frame_count != u64::from(group.frame_count) {
        return Err("SoundKit v2 frame counts do not add up to the AEP1 group".to_string());
    }
    let mut encoding = None;
    let mut expected_pts = group.pts_samples;
    for packet in &stream.packets {
        let header = &packet.header;
        if header.sample_rate() != group.sample_rate
            || u16::from(header.channels()) != group.channel_count
        {
            return Err("SoundKit v2 metadata does not match its AEP1 group".to_string());
        }
        if header.pts().is_some_and(|pts| pts != expected_pts) {
            return Err("SoundKit v2 packet PTS is not contiguous".to_string());
        }
        if !matches!(
            header.encoding(),
            EncodingFlag::PCMSigned
                | EncodingFlag::PCMFloat
                | EncodingFlag::Opus
                | EncodingFlag::FLAC
                | EncodingFlag::AAC
        ) {
            return Err("SoundKit v2 mesh audio uses an unsupported encoding".to_string());
        }
        if encoding.is_some_and(|value| value != *header.encoding()) {
            return Err("SoundKit v2 group mixes encodings".to_string());
        }
        encoding = Some(*header.encoding());
        expected_pts = expected_pts
            .checked_add(u64::from(header.frame_count()))
            .ok_or_else(|| "SoundKit v2 PTS overflow".to_string())?;
    }
    encoding.ok_or_else(|| "SoundKit v2 group contains no packets".to_string())
}

pub fn terminalize_soundkit_group(
    mut group: DecodedMultichannelAudioGroup,
) -> Result<DecodedMultichannelAudioGroup, String> {
    if group.payload_kind != AudioPayloadKind::SoundKitV2 {
        return Err("DAW HLS input requires canonical SoundKit v2 framing".to_string());
    }
    let encoding = validate_soundkit_group(&group, true)?;
    let stream = SoundKitV2StreamRef::parse(&group.payload, true)?;
    group.payload_kind = match encoding {
        EncodingFlag::Opus => AudioPayloadKind::Opus,
        EncodingFlag::FLAC => AudioPayloadKind::Flac,
        EncodingFlag::PCMSigned | EncodingFlag::PCMFloat => AudioPayloadKind::Pcm,
        _ => return Err("unsupported SoundKit v2 terminal encoding".to_string()),
    };
    group.sample_format = match (encoding, stream.packets[0].header.bits_per_sample()) {
        (EncodingFlag::PCMSigned, 16) => AudioSampleFormat::S16Le,
        (EncodingFlag::PCMSigned, 24) => AudioSampleFormat::S24Le,
        (EncodingFlag::PCMSigned, 32) => AudioSampleFormat::S32Le,
        (EncodingFlag::PCMFloat, 32) => AudioSampleFormat::F32Le,
        _ => AudioSampleFormat::Unspecified,
    };
    if encoding != EncodingFlag::Opus {
        let mut payload = Vec::new();
        for packet in stream.packets {
            payload.extend_from_slice(packet.payload);
        }
        group.payload = Bytes::from(payload);
    }
    Ok(group)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn metadata(encoding: EncodingFlag, id: u64, pts: u64) -> SoundKitV2PacketMetadata {
        SoundKitV2PacketMetadata {
            encoding,
            frame_count: 240,
            sample_rate: 48_000,
            channels: 2,
            bits_per_sample: if matches!(encoding, EncodingFlag::PCMSigned) {
                24
            } else {
                0
            },
            endianness: Endianness::LittleEndian,
            id: Some(id),
            pts: Some(pts),
        }
    }

    #[test]
    fn pcm_flac_and_opus_use_the_same_packet_contract() {
        for (index, encoding) in [
            EncodingFlag::PCMSigned,
            EncodingFlag::FLAC,
            EncodingFlag::Opus,
        ]
        .into_iter()
        .enumerate()
        {
            let payload = vec![index as u8 + 1; 37 + index];
            let packet = frame_soundkit_v2_packet(
                metadata(encoding, index as u64, index as u64 * 240),
                &payload,
            )
            .unwrap();
            let stream = SoundKitV2StreamRef::parse(&packet, true).unwrap();

            assert_eq!(stream.packets.len(), 1);
            assert_eq!(stream.total_frame_count, 240);
            assert_eq!(*stream.packets[0].header.encoding(), encoding);
            assert_eq!(stream.packets[0].payload, payload);
        }
    }

    #[test]
    fn concatenated_opus_packets_keep_individual_boundaries() {
        let first =
            frame_soundkit_v2_packet(metadata(EncodingFlag::Opus, 1, 0), &[0x88, 0x11, 0x22])
                .unwrap();
        let second = frame_soundkit_v2_packet(
            metadata(EncodingFlag::Opus, 2, 240),
            &[0x88, 0x33, 0x44, 0x55],
        )
        .unwrap();
        let mut bytes = Vec::from(first.as_ref());
        bytes.extend_from_slice(&second);

        let stream = SoundKitV2StreamRef::parse(&bytes, true).unwrap();
        assert_eq!(stream.packets.len(), 2);
        assert_eq!(stream.total_frame_count, 480);
        assert_eq!(stream.packets[0].payload, &[0x88, 0x11, 0x22]);
        assert_eq!(stream.packets[1].payload, &[0x88, 0x33, 0x44, 0x55]);
    }

    #[test]
    fn rejects_truncated_or_corrupt_packets() {
        let packet =
            frame_soundkit_v2_packet(metadata(EncodingFlag::FLAC, 1, 0), b"lossless-frame")
                .unwrap();
        assert!(SoundKitV2StreamRef::parse(&packet[..packet.len() - 1], true).is_err());

        let mut corrupt = packet.to_vec();
        *corrupt.last_mut().unwrap() ^= 0xff;
        assert!(SoundKitV2StreamRef::parse(&corrupt, true)
            .unwrap_err()
            .contains("CRC mismatch"));
    }

    #[test]
    fn raw_pcm_epoch_is_rejected_at_the_mesh_boundary() {
        let transport = MultichannelAudioTransportAdapter::udp(1_200);
        let fec = transport.prepare_fec_config(MultichannelAudioFecConfig::default());
        let mut source = MultichannelAudioSender::new(MultichannelAudioSessionConfig {
            fec,
            ..MultichannelAudioSessionConfig::default()
        });
        let pcm = vec![0x5a; 240 * 2 * 2];
        let groups = [MultichannelAudioGroup {
            group_id: 7,
            channel_start: 0,
            channel_count: 2,
            payload_kind: AudioPayloadKind::Pcm,
            sample_format: AudioSampleFormat::S16Le,
            flags: 0,
            payload: &pcm,
        }];
        let input = transport
            .wrap_epoch(
                source
                    .encode_epoch(MultichannelAudioEpoch {
                        session_id: 91,
                        config_generation: 3,
                        epoch_id: 12,
                        pts_samples: 2_880,
                        sample_rate: 48_000,
                        frame_count: 240,
                        groups: &groups,
                    })
                    .unwrap(),
            )
            .unwrap();
        let peer: SocketAddr = "127.0.0.1:41000".parse().unwrap();
        let mut normalizer = SoundKitV2EpochNormalizer::udp(1_200);
        let mut rejection = None;
        for datagram in input.datagrams {
            if let Err(error) = normalizer.push_datagram(peer, &datagram.payload) {
                rejection = Some(error);
                break;
            }
        }
        assert_eq!(
            rejection.as_deref(),
            Some("DAW mesh audio requires canonical SoundKit v2 framing")
        );
    }
}
