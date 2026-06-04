//! MPEG-TS to fragmented MP4 bridge for browser HLS playback.
//!
//! SRT and RIST ingest deliver MPEG-TS byte chunks. hls.js can parse MPEG-TS
//! when segments are cut by a muxer, but arbitrary network/cache chunks are
//! not valid HLS media fragments. This bridge demuxes H.264 access units,
//! boxes them as fMP4/CMAF parts, and updates the shared playlist cache.

use access_unit::{
    AccessUnit, PSI_STREAM_AAC, PSI_STREAM_H264, PSI_STREAM_MPEG4_AAC, PSI_STREAM_PRIVATE_DATA,
};
use boxer::fmp4::{box_fmp4_with_init, AdtsHeader, AvcDecoderConfigurationRecord, Config};
use bytes::{Bytes, BytesMut};
use h264::{
    Bitstream, Decode, NALUnit, SequenceParameterSet, NAL_UNIT_TYPE_CODED_SLICE_OF_IDR_PICTURE,
    NAL_UNIT_TYPE_MASK, NAL_UNIT_TYPE_PICTURE_PARAMETER_SET, NAL_UNIT_TYPE_SEQUENCE_PARAMETER_SET,
    NAL_UNIT_TYPE_SUPPLEMENTAL_ENHANCEMENT_INFORMATION,
};
use mpeg2ts_reader::demultiplex::{self, FilterChangeset};
use mpeg2ts_reader::packet_filter_switch;
use mpeg2ts_reader::pes;
use mpeg2ts_reader::psi;
use mpeg2ts_reader::StreamType;
use playlists::Playlists;
use std::sync::Arc;
use tracing::{debug, error, info, warn};

const TICKS_PER_SECOND: u64 = 90_000;
pub const DEFAULT_MIN_PART_MS: u32 = 50;
const MAX_PART_MS_WITHOUT_KEY: u32 = 2_000;
const MAX_PENDING_AUS_WITHOUT_CONFIG: usize = 180;
const MAX_PES_PAYLOAD_BYTES: usize = 8 * 1024 * 1024;
const MIN_H264_WIDTH: u16 = 160;
const MIN_H264_HEIGHT: u16 = 90;
const MAX_H264_DIMENSION: u16 = 8_192;

#[derive(Debug, Clone)]
pub struct PublishedFmp4Part {
    pub stream_id: u64,
    pub stream_idx: usize,
    pub sequence: u64,
    pub init: Option<Bytes>,
    pub bytes: Bytes,
    pub video_codec: Option<&'static str>,
    pub video_width: Option<u16>,
    pub video_height: Option<u16>,
    pub video_units: usize,
    pub audio_codec: Option<&'static str>,
    pub audio_units: usize,
}

#[derive(Debug, Clone, Copy)]
pub struct MpegTsContinuityIssue {
    pub stream_type: &'static str,
    pub dropped_payload_bytes: usize,
}

#[derive(Debug, Clone, Copy)]
pub struct MpegTsPayloadDrop {
    pub stream_type: &'static str,
    pub bytes: usize,
}

#[async_trait::async_trait]
pub trait Fmp4PartPublisher: Send + Sync {
    async fn publish_fmp4_part(&self, part: PublishedFmp4Part) -> Result<(), String>;

    fn record_mpeg_ts_continuity_issue(&self, _issue: MpegTsContinuityIssue) {}

    fn record_mpeg_ts_payload_drop(&self, _drop: MpegTsPayloadDrop) {}
}

#[derive(Debug, Clone, Copy)]
pub enum TimestampInput {
    Ticks90Khz,
    Millis,
}

impl TimestampInput {
    fn scale_video(self, value: u64) -> u64 {
        match self {
            Self::Ticks90Khz => value,
            Self::Millis => ms_to_ticks_u64(value),
        }
    }

    fn scale_audio(self, value: u64) -> u64 {
        match self {
            Self::Ticks90Khz => ticks_to_ms(value),
            Self::Millis => value,
        }
    }
}

pub struct TsFmp4Bridge {
    context: TsDemuxContext,
    demux: demultiplex::Demultiplex<TsDemuxContext>,
    segmenter: Fmp4Segmenter,
    drained_access_units: Vec<AccessUnit>,
}

impl TsFmp4Bridge {
    pub fn new(output_stream_id: u64, output_stream_idx: usize, playlists: Arc<Playlists>) -> Self {
        Self::new_with_options(
            output_stream_id,
            output_stream_idx,
            playlists,
            DEFAULT_MIN_PART_MS,
            None,
        )
    }

    pub fn new_with_publisher(
        output_stream_id: u64,
        output_stream_idx: usize,
        playlists: Arc<Playlists>,
        min_part_ms: u32,
        publisher: Option<Arc<dyn Fmp4PartPublisher>>,
    ) -> Self {
        Self::new_with_options(
            output_stream_id,
            output_stream_idx,
            playlists,
            min_part_ms,
            publisher,
        )
    }

    fn new_with_options(
        output_stream_id: u64,
        output_stream_idx: usize,
        playlists: Arc<Playlists>,
        min_part_ms: u32,
        publisher: Option<Arc<dyn Fmp4PartPublisher>>,
    ) -> Self {
        let mut context = TsDemuxContext::new(publisher.clone());
        let demux = demultiplex::Demultiplex::new(&mut context);
        let segmenter = Fmp4Segmenter::new_with_publisher(
            output_stream_id,
            output_stream_idx,
            playlists,
            TimestampInput::Ticks90Khz,
            min_part_ms,
            publisher,
        );

        Self {
            context,
            demux,
            segmenter,
            drained_access_units: Vec::new(),
        }
    }

    pub async fn push_ts(&mut self, bytes: Bytes) {
        let input_bytes = bytes.len();
        self.demux.push(&mut self.context, &bytes);
        self.context
            .drain_access_units_into(&mut self.drained_access_units);
        if !self.drained_access_units.is_empty() {
            debug!(
                output_stream_id = self.segmenter.output_stream_id,
                output_stream_idx = self.segmenter.output_stream_idx,
                input_bytes,
                access_units = self.drained_access_units.len(),
                "MPEG-TS chunk demuxed into access units"
            );
        } else {
            debug!(
                output_stream_id = self.segmenter.output_stream_id,
                output_stream_idx = self.segmenter.output_stream_idx,
                input_bytes,
                "MPEG-TS chunk buffered without complete access unit"
            );
        }
        for access_unit in self.drained_access_units.drain(..) {
            self.segmenter.push_access_unit(access_unit).await;
        }
    }

    pub async fn finish(&mut self) {
        self.segmenter.finish().await;
    }

    pub fn reset(&mut self) {
        self.context = TsDemuxContext::new(self.segmenter.publisher.clone());
        self.demux = demultiplex::Demultiplex::new(&mut self.context);
        self.drained_access_units.clear();
        self.segmenter.reset();
    }
}

#[derive(Clone, PartialEq, Eq)]
struct H264ConfigSignature {
    sps: Bytes,
    pps: Bytes,
}

#[derive(Clone, Copy, PartialEq, Eq)]
struct AudioConfigSignature {
    stream_type: u8,
    profile: u8,
    sampling_frequency: u32,
    channel_configuration: u8,
}

#[derive(Clone, PartialEq, Eq)]
struct InitSignature {
    video: Option<H264ConfigSignature>,
    audio: Option<AudioConfigSignature>,
}

pub struct Fmp4Segmenter {
    output_stream_id: u64,
    output_stream_idx: usize,
    playlists: Arc<Playlists>,
    input_timestamps: TimestampInput,
    publisher: Option<Arc<dyn Fmp4PartPublisher>>,
    min_part_ticks: u64,
    max_part_ticks_without_key: u64,
    video_buf: Vec<AccessUnit>,
    video_timestamps: Vec<u64>,
    audio_buf: Vec<AccessUnit>,
    audio_timestamps: Vec<u64>,
    config: Config,
    seg_seq: u32,
    sps: Option<Bytes>,
    pps: Option<Bytes>,
    config_signature: Option<H264ConfigSignature>,
    last_init_signature: Option<InitSignature>,
    force_next_init: bool,
    seen_video: bool,
    started_video: bool,
    published_parts: u64,
    warned_no_config: bool,
    timestamp_base_input: Option<u64>,
}

impl Fmp4Segmenter {
    pub fn new(
        output_stream_id: u64,
        output_stream_idx: usize,
        playlists: Arc<Playlists>,
        input_timestamps: TimestampInput,
        min_part_ms: u32,
    ) -> Self {
        Self::new_with_publisher(
            output_stream_id,
            output_stream_idx,
            playlists,
            input_timestamps,
            min_part_ms,
            None,
        )
    }

    pub fn new_with_publisher(
        output_stream_id: u64,
        output_stream_idx: usize,
        playlists: Arc<Playlists>,
        input_timestamps: TimestampInput,
        min_part_ms: u32,
        publisher: Option<Arc<dyn Fmp4PartPublisher>>,
    ) -> Self {
        Self {
            output_stream_id,
            output_stream_idx,
            playlists,
            input_timestamps,
            publisher,
            min_part_ticks: ms_to_ticks(min_part_ms),
            max_part_ticks_without_key: ms_to_ticks(MAX_PART_MS_WITHOUT_KEY),
            video_buf: Vec::new(),
            video_timestamps: Vec::new(),
            audio_buf: Vec::new(),
            audio_timestamps: Vec::new(),
            config: Config {
                width: 0,
                height: 0,
                avcc: None,
            },
            seg_seq: 1,
            sps: None,
            pps: None,
            config_signature: None,
            last_init_signature: None,
            force_next_init: true,
            seen_video: false,
            started_video: false,
            published_parts: 0,
            warned_no_config: false,
            timestamp_base_input: None,
        }
    }

    pub async fn push_access_unit(&mut self, mut access_unit: AccessUnit) {
        debug!(
            output_stream_id = self.output_stream_id,
            output_stream_idx = self.output_stream_idx,
            stream_type = ?access_unit.stream_type,
            key = access_unit.key,
            pts = access_unit.pts,
            dts = access_unit.dts,
            bytes = access_unit.data.len(),
            "fMP4 segmenter received access unit"
        );

        if is_h264(access_unit.stream_type) {
            self.seen_video = true;
            if !ensure_h264_length_prefixed(&mut access_unit) {
                return;
            }
        } else if !is_supported_audio(access_unit.stream_type) {
            return;
        }

        if self.timestamp_went_backwards(&access_unit) {
            warn!(
                output_stream_id = self.output_stream_id,
                dts = access_unit.dts,
                base = self.timestamp_base_input.unwrap_or_default(),
                "input timestamp reset detected; resetting fMP4 segmenter"
            );
            self.finish().await;
            self.reset();
        }

        self.normalize_timestamps(&mut access_unit);

        if is_h264(access_unit.stream_type) {
            self.push_video(access_unit).await;
        } else {
            self.push_audio(access_unit).await;
        }
    }

    pub async fn finish(&mut self) {
        self.flush_current().await;
    }

    pub fn reset(&mut self) {
        debug!(
            output_stream_id = self.output_stream_id,
            output_stream_idx = self.output_stream_idx,
            buffered_video = self.video_buf.len(),
            buffered_audio = self.audio_buf.len(),
            published_parts = self.published_parts,
            "resetting fMP4 segmenter state"
        );
        self.video_buf.clear();
        self.video_timestamps.clear();
        self.audio_buf.clear();
        self.audio_timestamps.clear();
        self.config = Config {
            width: 0,
            height: 0,
            avcc: None,
        };
        self.seg_seq = 1;
        self.sps = None;
        self.pps = None;
        self.config_signature = None;
        self.last_init_signature = None;
        self.force_next_init = true;
        self.seen_video = false;
        self.started_video = false;
        self.published_parts = 0;
        self.warned_no_config = false;
        self.timestamp_base_input = None;
    }

    async fn push_video(&mut self, mut access_unit: AccessUnit) {
        let had_config = self.config.avcc.is_some();
        let mut accepted_config_changed = false;
        if let Some((config, signature)) = self.parse_h264_config(&access_unit) {
            let config_changed = self.config_signature.as_ref() != Some(&signature);
            if config_changed {
                if had_config {
                    let current_width = self.config.width;
                    let current_height = self.config.height;
                    let same_dimensions =
                        current_width == config.width && current_height == config.height;

                    if !access_unit.key {
                        if same_dimensions {
                            debug!(
                                output_stream_id = self.output_stream_id,
                                current_width,
                                current_height,
                                "ignoring same-resolution non-key H.264 config update"
                            );
                        } else {
                            warn!(
                                output_stream_id = self.output_stream_id,
                                current_width,
                                current_height,
                                new_width = config.width,
                                new_height = config.height,
                                "dropping non-key access unit carrying mid-stream H.264 resolution change"
                            );
                            return;
                        }
                    } else if same_dimensions {
                        self.flush_current().await;
                        self.clear_pending_media();
                        self.install_h264_config(config, signature);
                        accepted_config_changed = true;

                        debug!(
                            output_stream_id = self.output_stream_id,
                            current_width,
                            current_height,
                            "accepted keyframe same-resolution H.264 config update"
                        );
                    } else {
                        info!(
                            output_stream_id = self.output_stream_id,
                            current_width,
                            current_height,
                            new_width = config.width,
                            new_height = config.height,
                            "accepted keyframe H.264 resolution change"
                        );
                        self.flush_current().await;
                        self.clear_pending_media();
                        self.install_h264_config(config, signature);
                        accepted_config_changed = true;
                    }
                } else {
                    self.clear_pending_media();
                    self.install_h264_config(config, signature);
                    accepted_config_changed = true;

                    info!(
                        output_stream_id = self.output_stream_id,
                        width = self.config.width,
                        height = self.config.height,
                        "configured H.264 fMP4 track"
                    );
                }
            }
        }

        match strip_h264_parameter_sets(&mut access_unit) {
            Ok(true) => {}
            Ok(false) => return,
            Err(error) => {
                warn!(
                    output_stream_id = self.output_stream_id,
                    output_stream_idx = self.output_stream_idx,
                    bytes = access_unit.data.len(),
                    error = ?error,
                    "dropping malformed length-prefixed H.264 sample"
                );
                return;
            }
        }

        if self.config.avcc.is_none() {
            self.video_timestamps.push(access_unit.dts);
            self.video_buf.push(access_unit);
            debug!(
                output_stream_id = self.output_stream_id,
                output_stream_idx = self.output_stream_idx,
                buffered_video = self.video_buf.len(),
                buffered_audio = self.audio_buf.len(),
                "buffering video while waiting for H.264 SPS/PPS"
            );
            self.drop_pending_without_config_if_needed();
            return;
        }

        if (!had_config || accepted_config_changed) && !access_unit.key {
            debug!(
                output_stream_id = self.output_stream_id,
                output_stream_idx = self.output_stream_idx,
                key = access_unit.key,
                config_changed = accepted_config_changed,
                "dropping non-key video access unit until first keyframe after H.264 config"
            );
            return;
        }

        if let Some(reason) = self.flush_reason_before(&access_unit) {
            let first_dts = self
                .video_timestamps
                .first()
                .copied()
                .unwrap_or(access_unit.dts);
            let elapsed = access_unit.dts.saturating_sub(first_dts);
            debug!(
                output_stream_id = self.output_stream_id,
                output_stream_idx = self.output_stream_idx,
                reason,
                key = access_unit.key,
                buffered_video = self.video_buf.len(),
                buffered_audio = self.audio_buf.len(),
                elapsed_ms = ticks_to_ms(elapsed),
                target_ms = ticks_to_ms(self.min_part_ticks),
                max_without_key_ms = ticks_to_ms(self.max_part_ticks_without_key),
                next_dts = access_unit.dts,
                "flushing fMP4 part before next video access unit"
            );
            self.flush_with_next_dts(access_unit.dts).await;
        }

        self.video_timestamps.push(access_unit.dts);
        self.video_buf.push(access_unit);
        self.started_video = true;
    }

    fn install_h264_config(&mut self, config: Config, signature: H264ConfigSignature) {
        self.sps = Some(signature.sps.clone());
        self.pps = Some(signature.pps.clone());
        self.config = config;
        self.config_signature = Some(signature);
        self.force_next_init = true;
        self.started_video = false;
        self.warned_no_config = false;
    }

    async fn push_audio(&mut self, access_unit: AccessUnit) {
        if self.seen_video && (self.config.avcc.is_none() || !self.started_video) {
            self.audio_timestamps.push(access_unit.pts);
            self.audio_buf.push(access_unit);
            debug!(
                output_stream_id = self.output_stream_id,
                output_stream_idx = self.output_stream_idx,
                buffered_video = self.video_buf.len(),
                buffered_audio = self.audio_buf.len(),
                "buffering audio until video track is configured and started"
            );
            self.drop_pending_without_config_if_needed();
            return;
        }

        if self.video_buf.is_empty() && self.should_flush_audio_only_before(&access_unit) {
            let first_pts = self
                .audio_timestamps
                .first()
                .copied()
                .unwrap_or(access_unit.pts);
            debug!(
                output_stream_id = self.output_stream_id,
                output_stream_idx = self.output_stream_idx,
                buffered_audio = self.audio_buf.len(),
                elapsed_ms = access_unit.pts.saturating_sub(first_pts),
                target_ms = ticks_to_ms(self.min_part_ticks),
                "flushing audio-only fMP4 part before next access unit"
            );
            self.flush_with_next_dts(0).await;
        }

        self.audio_timestamps.push(access_unit.pts);
        self.audio_buf.push(access_unit);
    }

    fn timestamp_went_backwards(&self, access_unit: &AccessUnit) -> bool {
        if !is_h264(access_unit.stream_type) {
            return false;
        }
        self.timestamp_base_input
            .is_some_and(|base| access_unit.dts < base)
    }

    fn normalize_timestamps(&mut self, access_unit: &mut AccessUnit) {
        let base_dts = *self.timestamp_base_input.get_or_insert(access_unit.dts);
        access_unit.pts = access_unit.pts.saturating_sub(base_dts);
        access_unit.dts = access_unit.dts.saturating_sub(base_dts);

        if is_h264(access_unit.stream_type) {
            access_unit.pts = self.input_timestamps.scale_video(access_unit.pts);
            access_unit.dts = self.input_timestamps.scale_video(access_unit.dts);
        } else {
            access_unit.pts = self.input_timestamps.scale_audio(access_unit.pts);
            access_unit.dts = self.input_timestamps.scale_audio(access_unit.dts);
        }
    }

    async fn flush_current(&mut self) {
        if let Some(last_dts) = self.video_timestamps.last().copied() {
            let next_dts = if self.video_timestamps.len() >= 2 {
                let prev = self.video_timestamps[self.video_timestamps.len() - 2];
                last_dts + last_dts.saturating_sub(prev).max(1)
            } else {
                last_dts + ms_to_ticks(DEFAULT_MIN_PART_MS)
            };
            self.flush_with_next_dts(next_dts).await;
        } else if !self.audio_buf.is_empty() {
            self.flush_with_next_dts(0).await;
        };
    }

    fn flush_reason_before(&self, access_unit: &AccessUnit) -> Option<&'static str> {
        let Some(first_dts) = self.video_timestamps.first().copied() else {
            return None;
        };
        let elapsed = access_unit.dts.saturating_sub(first_dts);
        if elapsed >= self.min_part_ticks {
            Some(if access_unit.key {
                "target-keyframe"
            } else {
                "target-duration"
            })
        } else if elapsed >= self.max_part_ticks_without_key {
            Some("max-duration-without-key")
        } else {
            None
        }
    }

    fn should_flush_audio_only_before(&self, access_unit: &AccessUnit) -> bool {
        let Some(first_pts) = self.audio_timestamps.first().copied() else {
            return false;
        };
        access_unit.pts.saturating_sub(first_pts) >= ticks_to_ms(self.min_part_ticks)
    }

    async fn flush_with_next_dts(&mut self, next_dts: u64) {
        if self.video_buf.is_empty() && self.audio_buf.is_empty() {
            return;
        }

        if self.config.avcc.is_none() && self.seen_video {
            if !self.warned_no_config {
                warn!(
                    output_stream_id = self.output_stream_id,
                    "waiting for H.264 SPS/PPS before publishing fMP4"
                );
                self.warned_no_config = true;
            }
            self.clear_pending_media();
            return;
        }

        let init_signature = self.current_init_signature();
        let include_init =
            self.force_next_init || self.last_init_signature.as_ref() != Some(&init_signature);
        let video_units = self.video_buf.len();
        let audio_units = self.audio_buf.len();

        let fmp4 = box_fmp4_with_init(
            self.seg_seq,
            self.config.clone(),
            std::mem::take(&mut self.video_buf),
            std::mem::take(&mut self.audio_buf),
            next_dts,
            include_init,
        );
        self.video_timestamps.clear();
        self.audio_timestamps.clear();

        if fmp4.data.is_empty() {
            warn!(
                output_stream_id = self.output_stream_id,
                seq = self.seg_seq,
                "boxed empty fMP4 part"
            );
            self.seg_seq = self.seg_seq.wrapping_add(1);
            return;
        }

        let init_for_mesh = fmp4.init.clone();
        let init_published = init_for_mesh.is_some();
        let duration = fmp4.duration;
        let key = fmp4.key;
        let part_bytes = fmp4.data.clone();
        let bytes = part_bytes.len();
        if let Err(error) = self
            .playlists
            .chunk_cache
            .append(self.output_stream_idx, part_bytes.clone())
            .await
        {
            error!(
                output_stream_id = self.output_stream_id,
                output_stream_idx = self.output_stream_idx,
                "fMP4 chunk_cache append error: {}",
                error
            );
            return;
        }

        if !self.playlists.add(self.output_stream_id, fmp4) {
            error!(
                output_stream_id = self.output_stream_id,
                "fMP4 playlist add failed"
            );
            return;
        }

        if let Some(publisher) = &self.publisher {
            let part = PublishedFmp4Part {
                stream_id: self.output_stream_id,
                stream_idx: self.output_stream_idx,
                sequence: self.published_parts,
                init: init_for_mesh,
                bytes: part_bytes,
                video_codec: (video_units > 0).then_some("h264"),
                video_width: (video_units > 0).then_some(self.config.width),
                video_height: (video_units > 0).then_some(self.config.height),
                video_units,
                audio_codec: (audio_units > 0).then_some("aac"),
                audio_units,
            };
            if let Err(error) = publisher.publish_fmp4_part(part).await {
                warn!(
                    output_stream_id = self.output_stream_id,
                    output_stream_idx = self.output_stream_idx,
                    error = %error,
                    "failed to publish fMP4 part into mesh"
                );
            } else {
                debug!(
                    output_stream_id = self.output_stream_id,
                    output_stream_idx = self.output_stream_idx,
                    sequence = self.published_parts,
                    bytes,
                    "published fMP4 part into mesh"
                );
            }
        }

        if init_published {
            self.last_init_signature = Some(init_signature);
            self.force_next_init = false;
        }

        self.published_parts += 1;
        debug!(
            output_stream_id = self.output_stream_id,
            output_stream_idx = self.output_stream_idx,
            seq = self.seg_seq,
            duration_ms = duration,
            key,
            bytes,
            video_units,
            audio_units,
            include_init = init_published,
            published_parts = self.published_parts,
            "published fMP4 HLS part details"
        );
        if self.published_parts <= 3 || self.published_parts % 25 == 0 {
            info!(
                output_stream_id = self.output_stream_id,
                output_stream_idx = self.output_stream_idx,
                seq = self.seg_seq,
                duration_ms = duration,
                key,
                bytes,
                "published fMP4 HLS part"
            );
        }
        self.seg_seq = self.seg_seq.wrapping_add(1);
    }

    fn clear_pending_media(&mut self) {
        self.video_buf.clear();
        self.video_timestamps.clear();
        self.audio_buf.clear();
        self.audio_timestamps.clear();
    }

    fn drop_pending_without_config_if_needed(&mut self) {
        let pending = self.video_buf.len().saturating_add(self.audio_buf.len());
        if pending <= MAX_PENDING_AUS_WITHOUT_CONFIG {
            return;
        }

        warn!(
            output_stream_id = self.output_stream_id,
            pending, "dropping buffered media while waiting for H.264 SPS/PPS and first keyframe"
        );
        self.clear_pending_media();
    }

    fn current_init_signature(&self) -> InitSignature {
        InitSignature {
            video: self.config_signature.clone(),
            audio: audio_config_signature(&self.audio_buf),
        }
    }

    fn parse_h264_config(
        &mut self,
        access_unit: &AccessUnit,
    ) -> Option<(Config, H264ConfigSignature)> {
        let mut found_config_nalu = false;
        let mut candidate_sps = self.sps.clone();
        let mut candidate_pps = self.pps.clone();
        let mut data = access_unit.data.as_ref();
        loop {
            let nalu = match next_h264_length_prefixed_nalu(&mut data) {
                Ok(Some(nalu)) => nalu,
                Ok(None) => break,
                Err(error) => {
                    warn!(
                        output_stream_id = self.output_stream_id,
                        output_stream_idx = self.output_stream_idx,
                        bytes = access_unit.data.len(),
                        error = ?error,
                        "rejecting malformed length-prefixed H.264 config sample"
                    );
                    return None;
                }
            };
            if nalu.is_empty() {
                continue;
            }

            match nalu[0] & NAL_UNIT_TYPE_MASK {
                NAL_UNIT_TYPE_SEQUENCE_PARAMETER_SET => {
                    candidate_sps = Some(Bytes::copy_from_slice(nalu));
                    found_config_nalu = true;
                }
                NAL_UNIT_TYPE_PICTURE_PARAMETER_SET => {
                    candidate_pps = Some(Bytes::copy_from_slice(nalu));
                    found_config_nalu = true;
                }
                _ => {}
            }
        }

        if !found_config_nalu {
            return None;
        }

        let (Some(sps), Some(pps)) = (&candidate_sps, &candidate_pps) else {
            return None;
        };
        let (decoded_sps, width, height) = match decode_h264_sps(sps) {
            Ok(decoded) => decoded,
            Err(error) => {
                warn!(
                    output_stream_id = self.output_stream_id,
                    output_stream_idx = self.output_stream_idx,
                    sps_bytes = sps.len(),
                    error = ?error,
                    "rejecting invalid H.264 SPS"
                );
                return None;
            }
        };
        Some((
            Config {
                width,
                height,
                avcc: Some(AvcDecoderConfigurationRecord {
                    profile_idc: decoded_sps.profile_idc.0,
                    constraint_set_flag: decoded_sps.constraint_set0_flag.0,
                    level_idc: decoded_sps.level_idc.0,
                    sequence_parameter_set: sps.clone(),
                    picture_parameter_set: pps.clone(),
                }),
            },
            H264ConfigSignature {
                sps: sps.clone(),
                pps: pps.clone(),
            },
        ))
    }
}

fn ms_to_ticks(ms: u32) -> u64 {
    u64::from(ms) * TICKS_PER_SECOND / 1_000
}

fn ms_to_ticks_u64(ms: u64) -> u64 {
    ms.saturating_mul(TICKS_PER_SECOND) / 1_000
}

fn ticks_to_ms(ticks: u64) -> u64 {
    ticks
        .saturating_mul(1_000)
        .saturating_add(TICKS_PER_SECOND / 2)
        / TICKS_PER_SECOND
}

fn is_h264(stream_type: u8) -> bool {
    stream_type == PSI_STREAM_H264
}

fn is_supported_audio(stream_type: u8) -> bool {
    matches!(
        stream_type,
        PSI_STREAM_AAC | PSI_STREAM_MPEG4_AAC | PSI_STREAM_PRIVATE_DATA
    )
}

fn h264_display_dimensions(sps: &SequenceParameterSet) -> Option<(u16, u16)> {
    let width_crop = sps
        .frame_crop_left_offset
        .0
        .checked_add(sps.frame_crop_right_offset.0)?
        .checked_mul(sps.crop_unit_x())?;
    let height_crop = sps
        .frame_crop_top_offset
        .0
        .checked_add(sps.frame_crop_bottom_offset.0)?
        .checked_mul(sps.crop_unit_y())?;
    let width = sps
        .pic_width_in_samples()
        .checked_sub(width_crop)
        .and_then(|value| u16::try_from(value).ok())?;
    let height = sps
        .frame_height_in_mbs()
        .checked_mul(16)?
        .checked_sub(height_crop)
        .and_then(|value| u16::try_from(value).ok())?;
    Some((width, height))
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum H264SpsValidationError {
    UndecodableNalu,
    UndecodableRbsp,
    InvalidDimensions,
    ImplausibleDimensions { width: u16, height: u16 },
}

fn decode_h264_sps(sps: &[u8]) -> Result<(SequenceParameterSet, u16, u16), H264SpsValidationError> {
    let bitstream = Bitstream::new(sps.iter().copied());
    let mut nalu =
        NALUnit::decode(bitstream).map_err(|_| H264SpsValidationError::UndecodableNalu)?;
    let mut rbsp = Bitstream::new(&mut nalu.rbsp_byte);
    let decoded_sps = SequenceParameterSet::decode(&mut rbsp)
        .map_err(|_| H264SpsValidationError::UndecodableRbsp)?;

    let (width, height) =
        h264_display_dimensions(&decoded_sps).ok_or(H264SpsValidationError::InvalidDimensions)?;
    if width < MIN_H264_WIDTH
        || height < MIN_H264_HEIGHT
        || width > MAX_H264_DIMENSION
        || height > MAX_H264_DIMENSION
    {
        return Err(H264SpsValidationError::ImplausibleDimensions { width, height });
    }

    Ok((decoded_sps, width, height))
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum H264SampleError {
    TruncatedLengthPrefix { remaining: usize },
    TruncatedNalu { declared: usize, remaining: usize },
}

fn next_h264_length_prefixed_nalu<'a>(
    data: &mut &'a [u8],
) -> Result<Option<&'a [u8]>, H264SampleError> {
    if data.is_empty() {
        return Ok(None);
    }
    if data.len() < 4 {
        return Err(H264SampleError::TruncatedLengthPrefix {
            remaining: data.len(),
        });
    }

    let nalu_len = u32::from_be_bytes([data[0], data[1], data[2], data[3]]) as usize;
    *data = &data[4..];
    if data.len() < nalu_len {
        return Err(H264SampleError::TruncatedNalu {
            declared: nalu_len,
            remaining: data.len(),
        });
    }

    let nalu = &data[..nalu_len];
    *data = &data[nalu_len..];
    Ok(Some(nalu))
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum H264NaluAppend {
    Appended { keyframe: bool },
    RejectedSps(H264SpsValidationError),
    Ignored,
}

fn append_h264_nalu_to_avcc_sample(sample: &mut BytesMut, nalu: &[u8]) -> H264NaluAppend {
    if nalu.is_empty() || (nalu[0] & 0x80) != 0 {
        return H264NaluAppend::Ignored;
    }

    let nalu_type = nalu[0] & NAL_UNIT_TYPE_MASK;
    match nalu_type {
        1
        | NAL_UNIT_TYPE_CODED_SLICE_OF_IDR_PICTURE
        | NAL_UNIT_TYPE_SUPPLEMENTAL_ENHANCEMENT_INFORMATION
        | NAL_UNIT_TYPE_PICTURE_PARAMETER_SET => {
            sample.extend_from_slice(&(nalu.len() as u32).to_be_bytes());
            sample.extend_from_slice(nalu);
            H264NaluAppend::Appended {
                keyframe: nalu_type == NAL_UNIT_TYPE_CODED_SLICE_OF_IDR_PICTURE,
            }
        }
        NAL_UNIT_TYPE_SEQUENCE_PARAMETER_SET => match decode_h264_sps(nalu) {
            Ok(_) => {
                sample.extend_from_slice(&(nalu.len() as u32).to_be_bytes());
                sample.extend_from_slice(nalu);
                H264NaluAppend::Appended { keyframe: false }
            }
            Err(error) => H264NaluAppend::RejectedSps(error),
        },
        _ => H264NaluAppend::Ignored,
    }
}

fn ensure_h264_length_prefixed(access_unit: &mut AccessUnit) -> bool {
    if !looks_like_annex_b(&access_unit.data) {
        return true;
    }

    let mut sample = BytesMut::with_capacity(access_unit.data.len());
    let mut key = access_unit.key;

    for nalu in h264::iterate_annex_b(&access_unit.data) {
        if nalu.is_empty() {
            continue;
        }

        match append_h264_nalu_to_avcc_sample(&mut sample, nalu) {
            H264NaluAppend::Appended { keyframe } => key |= keyframe,
            H264NaluAppend::RejectedSps(error) => {
                debug!(
                    sps_bytes = nalu.len(),
                    error = ?error,
                    "dropping invalid H.264 SPS NALU before AVCC packetization"
                );
            }
            H264NaluAppend::Ignored => {}
        }
    }

    if sample.is_empty() {
        return false;
    }

    access_unit.data = sample.freeze();
    access_unit.key = key;
    true
}

fn looks_like_annex_b(data: &[u8]) -> bool {
    data.starts_with(&[0, 0, 1]) || data.starts_with(&[0, 0, 0, 1])
}

fn strip_h264_parameter_sets(access_unit: &mut AccessUnit) -> Result<bool, H264SampleError> {
    let mut data = access_unit.data.as_ref();
    let mut sample = BytesMut::with_capacity(access_unit.data.len());
    loop {
        let Some(nalu) = next_h264_length_prefixed_nalu(&mut data)? else {
            break;
        };
        if nalu.is_empty() {
            continue;
        }

        match nalu[0] & NAL_UNIT_TYPE_MASK {
            NAL_UNIT_TYPE_SEQUENCE_PARAMETER_SET | NAL_UNIT_TYPE_PICTURE_PARAMETER_SET => {}
            _ => {
                sample.extend_from_slice(&(nalu.len() as u32).to_be_bytes());
                sample.extend_from_slice(nalu);
            }
        }
    }

    if sample.is_empty() {
        return Ok(false);
    }

    access_unit.data = sample.freeze();
    Ok(true)
}

fn audio_config_signature(audio_units: &[AccessUnit]) -> Option<AudioConfigSignature> {
    for unit in audio_units {
        let Some(header) = AdtsHeader::read_from(&unit.data) else {
            continue;
        };
        return Some(AudioConfigSignature {
            stream_type: unit.stream_type,
            profile: header.profile as u8,
            sampling_frequency: header.sampling_frequency.as_u32(),
            channel_configuration: header.channel_configuration as u8,
        });
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use playlists::Options;

    fn h264_access_unit(pts: u64, dts: u64) -> AccessUnit {
        AccessUnit {
            key: true,
            pts,
            dts,
            data: Bytes::new(),
            stream_type: PSI_STREAM_H264,
            id: 0,
        }
    }

    fn audio_access_unit(pts: u64, dts: u64) -> AccessUnit {
        AccessUnit {
            key: false,
            pts,
            dts,
            data: Bytes::new(),
            stream_type: PSI_STREAM_AAC,
            id: 0,
        }
    }

    fn push_len_prefixed_nalu(out: &mut BytesMut, nalu: &[u8]) {
        out.extend_from_slice(&(nalu.len() as u32).to_be_bytes());
        out.extend_from_slice(nalu);
    }

    fn h264_annex_b_sample(nalus: &[&[u8]]) -> Bytes {
        let mut data = BytesMut::new();
        for nalu in nalus {
            data.extend_from_slice(&[0x00, 0x00, 0x01]);
            data.extend_from_slice(nalu);
        }
        data.freeze()
    }

    const H264_SPS_720P: &[u8] = &[
        0x67, 0x42, 0xc0, 0x1f, 0xda, 0x01, 0x40, 0x16, 0xec, 0x05, 0xa8, 0x08, 0x08, 0x0a, 0x00,
        0x00, 0x03, 0x00, 0x02, 0x00, 0x00, 0x03, 0x00, 0x65, 0x1e, 0x30, 0x65, 0x40,
    ];
    const H264_SPS_360P: &[u8] = &[
        0x67, 0x42, 0xc0, 0x1e, 0xda, 0x02, 0x80, 0xbf, 0xe5, 0xc0, 0x5a, 0x80, 0x80, 0x80, 0xa0,
        0x00, 0x00, 0x03, 0x00, 0x20, 0x00, 0x00, 0x06, 0x51, 0xe2, 0xc5, 0xd4,
    ];
    const H264_PPS: &[u8] = &[0x68, 0xce, 0x3c, 0x80];
    const H264_CHANGED_PPS: &[u8] = &[0x68, 0xce, 0x3c, 0x81];
    const H264_IDR: &[u8] = &[0x65, 0x88, 0x84, 0x00];
    const H264_NON_IDR: &[u8] = &[0x41, 0x9a, 0x22];
    const H264_INVALID_SPS: &[u8] = &[0x67, 0x00];

    fn h264_sample(sps: &[u8], pps: &[u8], media: &[u8]) -> Bytes {
        let mut data = BytesMut::new();
        push_len_prefixed_nalu(&mut data, sps);
        push_len_prefixed_nalu(&mut data, pps);
        push_len_prefixed_nalu(&mut data, media);
        data.freeze()
    }

    #[test]
    fn fmp4_segmenter_rebases_large_mpeg_ts_timestamps() {
        let (playlists, _, _) = Playlists::new(Options::default());
        let mut segmenter = Fmp4Segmenter::new(
            0,
            0,
            playlists,
            TimestampInput::Ticks90Khz,
            DEFAULT_MIN_PART_MS,
        );
        let mut first = h264_access_unit(9_000_000_000, 8_999_997_000);
        let mut second = h264_access_unit(9_000_003_000, 9_000_000_000);

        segmenter.normalize_timestamps(&mut first);
        segmenter.normalize_timestamps(&mut second);

        assert_eq!(first.dts, 0);
        assert_eq!(first.pts, 3_000);
        assert_eq!(second.dts, 3_000);
        assert_eq!(second.pts, 6_000);
    }

    #[test]
    fn fmp4_segmenter_allows_audio_to_start_before_first_video_dts() {
        let (playlists, _, _) = Playlists::new(Options::default());
        let mut segmenter = Fmp4Segmenter::new(
            0,
            0,
            playlists,
            TimestampInput::Ticks90Khz,
            DEFAULT_MIN_PART_MS,
        );
        segmenter.timestamp_base_input = Some(127_920);

        assert!(!segmenter.timestamp_went_backwards(&audio_access_unit(126_000, 126_000)));
        assert!(segmenter.timestamp_went_backwards(&h264_access_unit(126_000, 126_000)));
    }

    #[test]
    fn strips_h264_parameter_sets_from_media_sample() {
        let mut data = BytesMut::new();
        push_len_prefixed_nalu(&mut data, &[0x67, 0x01, 0x02]);
        push_len_prefixed_nalu(&mut data, &[0x68, 0x03, 0x04]);
        push_len_prefixed_nalu(&mut data, &[0x65, 0x05, 0x06]);
        let mut access_unit = h264_access_unit(0, 0);
        access_unit.data = data.freeze();

        assert!(strip_h264_parameter_sets(&mut access_unit).unwrap());

        let nalus: Vec<&[u8]> = h264::iterate_avcc(&access_unit.data, 4).collect();
        assert_eq!(nalus, vec![&[0x65, 0x05, 0x06][..]]);
    }

    #[test]
    fn rejects_truncated_h264_length_prefixed_sample() {
        let mut data = BytesMut::new();
        data.extend_from_slice(&5u32.to_be_bytes());
        data.extend_from_slice(&[0x65, 0x88]);
        let mut access_unit = h264_access_unit(0, 0);
        access_unit.data = data.freeze();

        assert_eq!(
            strip_h264_parameter_sets(&mut access_unit),
            Err(H264SampleError::TruncatedNalu {
                declared: 5,
                remaining: 2
            })
        );
    }

    #[test]
    fn annex_b_packetization_keeps_valid_h264_sps() {
        let mut access_unit = h264_access_unit(0, 0);
        access_unit.key = false;
        access_unit.data = h264_annex_b_sample(&[H264_SPS_720P, H264_PPS, H264_IDR]);

        assert!(ensure_h264_length_prefixed(&mut access_unit));
        assert!(access_unit.key);

        let nalus: Vec<&[u8]> = h264::iterate_avcc(&access_unit.data, 4).collect();
        assert_eq!(
            nalus,
            vec![H264_SPS_720P, H264_PPS, &[0x65, 0x88, 0x84][..]]
        );
    }

    #[test]
    fn annex_b_packetization_drops_invalid_h264_sps_candidates() {
        let mut access_unit = h264_access_unit(0, 0);
        access_unit.key = false;
        access_unit.data = h264_annex_b_sample(&[H264_INVALID_SPS, H264_NON_IDR]);

        assert!(ensure_h264_length_prefixed(&mut access_unit));
        assert!(!access_unit.key);

        let nalus: Vec<&[u8]> = h264::iterate_avcc(&access_unit.data, 4).collect();
        assert_eq!(nalus, vec![H264_NON_IDR]);
    }

    #[test]
    fn parses_h264_sps_display_dimensions_with_spec_crop_units() {
        let (playlists, _, _) = Playlists::new(Options::default());
        let mut segmenter = Fmp4Segmenter::new(
            0,
            0,
            playlists,
            TimestampInput::Ticks90Khz,
            DEFAULT_MIN_PART_MS,
        );
        let mut access_unit = h264_access_unit(0, 0);
        access_unit.data = h264_sample(H264_SPS_720P, H264_PPS, H264_IDR);

        let (config, _) = segmenter.parse_h264_config(&access_unit).unwrap();
        assert_eq!((config.width, config.height), (1280, 720));

        access_unit.data = h264_sample(H264_SPS_360P, H264_PPS, H264_IDR);
        let (config, _) = segmenter.parse_h264_config(&access_unit).unwrap();
        assert_eq!((config.width, config.height), (640, 360));
    }

    #[tokio::test]
    async fn accepts_keyframe_h264_config_update() {
        let (playlists, _, _) = Playlists::new(Options::default());
        let mut segmenter = Fmp4Segmenter::new(
            0,
            0,
            playlists,
            TimestampInput::Ticks90Khz,
            DEFAULT_MIN_PART_MS,
        );
        let mut initial = h264_access_unit(0, 0);
        initial.data = h264_sample(H264_SPS_720P, H264_PPS, H264_IDR);
        let (config, signature) = segmenter.parse_h264_config(&initial).unwrap();
        segmenter.install_h264_config(config, signature);
        assert_eq!(
            (segmenter.config.width, segmenter.config.height),
            (1280, 720)
        );

        let mut changed = h264_access_unit(90_000, 90_000);
        changed.key = true;
        changed.data = h264_sample(H264_SPS_360P, H264_PPS, H264_IDR);
        segmenter.push_video(changed).await;

        assert_eq!(
            (segmenter.config.width, segmenter.config.height),
            (640, 360)
        );
        assert_eq!(segmenter.video_buf.len(), 1);
        assert!(segmenter.force_next_init);
    }

    #[tokio::test]
    async fn drops_non_key_h264_config_update() {
        let (playlists, _, _) = Playlists::new(Options::default());
        let mut segmenter = Fmp4Segmenter::new(
            0,
            0,
            playlists,
            TimestampInput::Ticks90Khz,
            DEFAULT_MIN_PART_MS,
        );
        let mut initial = h264_access_unit(0, 0);
        initial.data = h264_sample(H264_SPS_720P, H264_PPS, H264_IDR);
        let (config, signature) = segmenter.parse_h264_config(&initial).unwrap();
        segmenter.install_h264_config(config, signature);

        let mut changed = h264_access_unit(90_000, 90_000);
        changed.key = false;
        changed.data = h264_sample(H264_SPS_360P, H264_PPS, H264_NON_IDR);
        segmenter.push_video(changed).await;

        assert_eq!(
            (segmenter.config.width, segmenter.config.height),
            (1280, 720)
        );
        assert!(segmenter.video_buf.is_empty());
    }

    #[tokio::test]
    async fn keeps_same_resolution_non_key_h264_config_update() {
        let (playlists, _, _) = Playlists::new(Options::default());
        let mut segmenter = Fmp4Segmenter::new(
            0,
            0,
            playlists,
            TimestampInput::Ticks90Khz,
            DEFAULT_MIN_PART_MS,
        );
        let mut initial = h264_access_unit(0, 0);
        initial.data = h264_sample(H264_SPS_720P, H264_PPS, H264_IDR);
        let (config, signature) = segmenter.parse_h264_config(&initial).unwrap();
        segmenter.install_h264_config(config, signature);

        let media = &[0x41, 0x9a, 0x22];
        let mut changed = h264_access_unit(90_000, 90_000);
        changed.key = false;
        changed.data = h264_sample(H264_SPS_720P, H264_CHANGED_PPS, media);
        segmenter.push_video(changed).await;

        assert_eq!(
            (segmenter.config.width, segmenter.config.height),
            (1280, 720)
        );
        assert_eq!(segmenter.video_buf.len(), 1);
        let nalus: Vec<&[u8]> = h264::iterate_avcc(&segmenter.video_buf[0].data, 4).collect();
        assert_eq!(nalus, vec![&media[..]]);
    }
}

pub struct TsDemuxContext {
    changeset: FilterChangeset<TsFilterSwitch>,
    access_units: Vec<AccessUnit>,
    observer: Option<Arc<dyn Fmp4PartPublisher>>,
}

impl TsDemuxContext {
    fn new(observer: Option<Arc<dyn Fmp4PartPublisher>>) -> Self {
        Self {
            changeset: FilterChangeset::default(),
            access_units: Vec::new(),
            observer,
        }
    }

    fn drain_access_units_into(&mut self, out: &mut Vec<AccessUnit>) {
        out.extend(self.access_units.drain(..));
    }

    fn construct_filter(&mut self, request: demultiplex::FilterRequest<'_, '_>) -> TsFilterSwitch {
        match request {
            demultiplex::FilterRequest::ByPid(psi::pat::PAT_PID) => {
                TsFilterSwitch::Pat(demultiplex::PatPacketFilter::default())
            }
            demultiplex::FilterRequest::ByPid(mpeg2ts_reader::STUFFING_PID) => {
                TsFilterSwitch::Null(demultiplex::NullPacketFilter::default())
            }
            demultiplex::FilterRequest::ByPid(_) => {
                TsFilterSwitch::Null(demultiplex::NullPacketFilter::default())
            }
            demultiplex::FilterRequest::ByStream {
                stream_type: StreamType::H264,
                stream_info,
                ..
            } => ElementaryStreamConsumer::construct(
                StreamType::H264,
                stream_info,
                self.observer.clone(),
            ),
            demultiplex::FilterRequest::ByStream {
                stream_type,
                stream_info,
                ..
            } if matches!(
                stream_type,
                StreamType::ADTS
                    | StreamType::H222_0_PES_PRIVATE_DATA
                    | StreamType::AUDIO_WITHOUT_TRANSPORT_SYNTAX
            ) =>
            {
                ElementaryStreamConsumer::construct(stream_type, stream_info, self.observer.clone())
            }
            demultiplex::FilterRequest::ByStream { stream_type, .. } => {
                debug!(stream_type = ?stream_type, "ignoring MPEG-TS stream");
                TsFilterSwitch::Null(demultiplex::NullPacketFilter::default())
            }
            demultiplex::FilterRequest::Pmt {
                pid,
                program_number,
            } => TsFilterSwitch::Pmt(demultiplex::PmtPacketFilter::new(pid, program_number)),
            demultiplex::FilterRequest::Nit { .. } => {
                TsFilterSwitch::Null(demultiplex::NullPacketFilter::default())
            }
        }
    }
}

impl demultiplex::DemuxContext for TsDemuxContext {
    type F = TsFilterSwitch;

    fn filter_changeset(&mut self) -> &mut FilterChangeset<Self::F> {
        &mut self.changeset
    }

    fn construct(&mut self, request: demultiplex::FilterRequest<'_, '_>) -> Self::F {
        self.construct_filter(request)
    }
}

packet_filter_switch! {
    TsFilterSwitch<TsDemuxContext> {
        Pes: pes::PesPacketFilter<TsDemuxContext, ElementaryStreamConsumer>,
        Pat: demultiplex::PatPacketFilter<TsDemuxContext>,
        Pmt: demultiplex::PmtPacketFilter<TsDemuxContext>,
        Null: demultiplex::NullPacketFilter<TsDemuxContext>,
    }
}

pub struct ElementaryStreamConsumer {
    stream_type: StreamType,
    stream_type_label: &'static str,
    observer: Option<Arc<dyn Fmp4PartPublisher>>,
    accumulated_payload: Vec<u8>,
    pts: u64,
    dts: u64,
}

impl ElementaryStreamConsumer {
    fn construct(
        stream_type: StreamType,
        stream_info: &psi::pmt::StreamInfo,
        observer: Option<Arc<dyn Fmp4PartPublisher>>,
    ) -> TsFilterSwitch {
        debug!(
            pid = ?stream_info.elementary_pid(),
            stream_type = ?stream_type,
            "registered MPEG-TS elementary stream"
        );
        TsFilterSwitch::Pes(pes::PesPacketFilter::new(Self {
            stream_type_label: stream_type_label(stream_type),
            stream_type,
            observer,
            accumulated_payload: Vec::new(),
            pts: 0,
            dts: 0,
        }))
    }

    fn append_payload(&mut self, data: &[u8]) {
        let new_len = self.accumulated_payload.len().saturating_add(data.len());
        if new_len > MAX_PES_PAYLOAD_BYTES {
            warn!(
                stream_type = ?self.stream_type,
                bytes = new_len,
                "dropping oversized MPEG-TS PES payload"
            );
            if let Some(observer) = &self.observer {
                observer.record_mpeg_ts_payload_drop(MpegTsPayloadDrop {
                    stream_type: self.stream_type_label,
                    bytes: new_len,
                });
            }
            self.accumulated_payload.clear();
            return;
        }
        self.accumulated_payload.extend_from_slice(data);
    }
}

impl pes::ElementaryStreamConsumer<TsDemuxContext> for ElementaryStreamConsumer {
    fn start_stream(&mut self, _ctx: &mut TsDemuxContext) {}

    fn begin_packet(&mut self, _ctx: &mut TsDemuxContext, header: pes::PesHeader) {
        match header.contents() {
            pes::PesContents::Parsed(Some(parsed)) => {
                match parsed.pts_dts() {
                    Ok(pes::PtsDts::PtsOnly(Ok(pts))) => {
                        self.pts = pts.value();
                        self.dts = pts.value();
                    }
                    Ok(pes::PtsDts::Both {
                        pts: Ok(pts),
                        dts: Ok(dts),
                    }) => {
                        self.pts = pts.value();
                        self.dts = dts.value();
                    }
                    _ => {}
                }
                self.append_payload(parsed.payload());
            }
            pes::PesContents::Parsed(None) => {}
            pes::PesContents::Payload(payload) => {
                self.append_payload(payload);
            }
        }
    }

    fn continue_packet(&mut self, _ctx: &mut TsDemuxContext, data: &[u8]) {
        self.append_payload(data);
    }

    fn end_packet(&mut self, ctx: &mut TsDemuxContext) {
        match self.stream_type {
            StreamType::H264 => {
                let mut sample = BytesMut::with_capacity(self.accumulated_payload.len());
                let mut key = false;

                for nalu in h264::iterate_annex_b(&self.accumulated_payload) {
                    if nalu.is_empty() {
                        continue;
                    }

                    match append_h264_nalu_to_avcc_sample(&mut sample, nalu) {
                        H264NaluAppend::Appended { keyframe } => key |= keyframe,
                        H264NaluAppend::RejectedSps(error) => {
                            debug!(
                                stream_type = ?self.stream_type,
                                sps_bytes = nalu.len(),
                                error = ?error,
                                "dropping invalid H.264 SPS NALU before MPEG-TS access unit emission"
                            );
                        }
                        H264NaluAppend::Ignored => {}
                    }
                }

                if !sample.is_empty() {
                    ctx.access_units.push(AccessUnit {
                        key,
                        pts: self.pts,
                        dts: self.dts,
                        data: sample.freeze(),
                        stream_type: self.stream_type.into(),
                        id: 0,
                    });
                }
            }
            StreamType::ADTS
            | StreamType::H222_0_PES_PRIVATE_DATA
            | StreamType::AUDIO_WITHOUT_TRANSPORT_SYNTAX => {
                if !self.accumulated_payload.is_empty() {
                    ctx.access_units.push(AccessUnit {
                        key: false,
                        pts: self.pts,
                        dts: self.dts,
                        data: Bytes::from(std::mem::take(&mut self.accumulated_payload)),
                        stream_type: self.stream_type.into(),
                        id: 0,
                    });
                }
            }
            _ => {}
        }

        self.accumulated_payload.clear();
    }

    fn continuity_error(&mut self, _ctx: &mut TsDemuxContext) {
        let dropped_payload_bytes = self.accumulated_payload.len();
        self.accumulated_payload.clear();
        if let Some(observer) = &self.observer {
            observer.record_mpeg_ts_continuity_issue(MpegTsContinuityIssue {
                stream_type: self.stream_type_label,
                dropped_payload_bytes,
            });
        }
        warn!(
            stream_type = ?self.stream_type,
            dropped_payload_bytes,
            "MPEG-TS continuity error; dropping partial elementary stream payload"
        );
    }
}

fn stream_type_label(stream_type: StreamType) -> &'static str {
    match stream_type {
        StreamType::H264 => "h264",
        StreamType::ADTS => "adts",
        StreamType::H222_0_PES_PRIVATE_DATA => "private_data",
        StreamType::AUDIO_WITHOUT_TRANSPORT_SYNTAX => "audio",
        _ => "other",
    }
}
