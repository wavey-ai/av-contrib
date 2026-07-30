use std::collections::HashSet;
use std::str::FromStr;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct QuadraRenditionSpec {
    pub stream_id: u64,
    pub width: u16,
    pub height: u16,
    pub average_bandwidth_bps: u32,
    pub peak_bandwidth_bps: u32,
}

impl QuadraRenditionSpec {
    pub fn hls_rendition_argument(
        self,
        master_stream_id: u64,
        frame_rate_millihertz: u32,
    ) -> String {
        format!(
            "{master_stream_id}:{}:{}:{}:{}x{}:{frame_rate_millihertz}",
            self.stream_id,
            self.peak_bandwidth_bps,
            self.average_bandwidth_bps,
            self.width,
            self.height,
        )
    }
}

impl FromStr for QuadraRenditionSpec {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let fields = value.split(':').collect::<Vec<_>>();
        if fields.len() != 4 {
            return Err(
                "expected STREAM_ID:WIDTHxHEIGHT:AVERAGE_BANDWIDTH:PEAK_BANDWIDTH".to_owned(),
            );
        }
        let stream_id = parse_positive_u64(fields[0], "stream ID")?;
        let (width, height) = fields[1]
            .split_once('x')
            .ok_or_else(|| "Quadra rendition resolution must be WIDTHxHEIGHT".to_owned())?;
        let average_bandwidth_bps = parse_positive_u32(fields[2], "average bandwidth")?;
        let peak_bandwidth_bps = parse_positive_u32(fields[3], "peak bandwidth")?;
        if average_bandwidth_bps > peak_bandwidth_bps {
            return Err("average bandwidth cannot exceed peak bandwidth".to_owned());
        }
        Ok(Self {
            stream_id,
            width: parse_positive_u16(width, "width")?,
            height: parse_positive_u16(height, "height")?,
            average_bandwidth_bps,
            peak_bandwidth_bps,
        })
    }
}

#[derive(Debug, Clone)]
pub struct QuadraRenditionConfig {
    pub source_stream_id: u64,
    pub source_width: u16,
    pub source_height: u16,
    pub frame_rate_millihertz: u32,
    pub hardware_id: Option<i32>,
    pub queue_capacity: usize,
    pub renditions: Vec<QuadraRenditionSpec>,
}

impl QuadraRenditionConfig {
    pub fn validate(&self) -> Result<(), String> {
        if self.source_stream_id == 0 {
            return Err("Quadra source stream ID must be positive".to_owned());
        }
        if self.source_width == 0 || self.source_height == 0 {
            return Err("Quadra source dimensions must be positive".to_owned());
        }
        if self.source_width % 2 != 0 || self.source_height % 2 != 0 {
            return Err("Quadra source dimensions must be even".to_owned());
        }
        if self.frame_rate_millihertz == 0 {
            return Err("Quadra frame rate must be positive".to_owned());
        }
        if self.queue_capacity == 0 {
            return Err("Quadra access-unit queue capacity must be positive".to_owned());
        }
        if self.renditions.is_empty() {
            return Err("at least one Quadra rendition is required".to_owned());
        }

        let mut stream_ids = HashSet::with_capacity(self.renditions.len());
        let mut resolutions = HashSet::with_capacity(self.renditions.len());
        for rendition in &self.renditions {
            if rendition.stream_id == self.source_stream_id {
                return Err(format!(
                    "Quadra rendition stream {} conflicts with the source stream",
                    rendition.stream_id
                ));
            }
            if !stream_ids.insert(rendition.stream_id) {
                return Err(format!(
                    "duplicate Quadra rendition stream {}",
                    rendition.stream_id
                ));
            }
            if !resolutions.insert((rendition.width, rendition.height)) {
                return Err(format!(
                    "duplicate Quadra rendition resolution {}x{}",
                    rendition.width, rendition.height
                ));
            }
            if rendition.width % 2 != 0 || rendition.height % 2 != 0 {
                return Err(format!(
                    "Quadra rendition {} dimensions must be even",
                    rendition.stream_id
                ));
            }
            if rendition.width > self.source_width || rendition.height > self.source_height {
                return Err(format!(
                    "Quadra rendition {} cannot upscale beyond source {}x{}",
                    rendition.stream_id, self.source_width, self.source_height
                ));
            }
        }
        Ok(())
    }

    pub fn frame_rate(&self) -> f64 {
        f64::from(self.frame_rate_millihertz) / 1_000.0
    }
}

fn parse_positive_u64(value: &str, label: &str) -> Result<u64, String> {
    value
        .parse::<u64>()
        .map_err(|error| format!("invalid Quadra rendition {label} {value}: {error}"))
        .and_then(|parsed| {
            (parsed > 0)
                .then_some(parsed)
                .ok_or_else(|| format!("Quadra rendition {label} must be positive"))
        })
}

fn parse_positive_u32(value: &str, label: &str) -> Result<u32, String> {
    value
        .parse::<u32>()
        .map_err(|error| format!("invalid Quadra rendition {label} {value}: {error}"))
        .and_then(|parsed| {
            (parsed > 0)
                .then_some(parsed)
                .ok_or_else(|| format!("Quadra rendition {label} must be positive"))
        })
}

fn parse_positive_u16(value: &str, label: &str) -> Result<u16, String> {
    value
        .parse::<u16>()
        .map_err(|error| format!("invalid Quadra rendition {label} {value}: {error}"))
        .and_then(|parsed| {
            (parsed > 0)
                .then_some(parsed)
                .ok_or_else(|| format!("Quadra rendition {label} must be positive"))
        })
}

#[cfg(all(feature = "quadra-renditions", target_os = "linux"))]
mod linux {
    use super::{QuadraRenditionConfig, QuadraRenditionSpec};
    use crate::fmp4_bridge::{AccessUnitSink, Fmp4PartPublisher, Fmp4Segmenter, TimestampInput};
    use access_unit::{AccessUnit, PSI_STREAM_H264};
    use av_traits::EncodedFrameType;
    use bytes::Bytes;
    use playlists::Playlists;
    use std::collections::VecDeque;
    use std::convert::Infallible;
    use std::sync::Arc;
    use tokio::sync::{mpsc, oneshot};
    use tracing::{error, info, warn};
    use xcoder_quadra::{
        init, XcoderDecoder, XcoderDecoderCodec, XcoderDecoderConfig, XcoderDecoderInputFrame,
        XcoderEncoder, XcoderEncoderCodec, XcoderEncoderConfig, XcoderHardwareFrame,
        XcoderPixelFormat, XcoderScaler, XcoderScalerConfig,
    };

    #[derive(Debug, Clone, Copy)]
    pub struct QuadraRenditionTarget {
        pub spec: QuadraRenditionSpec,
        pub stream_idx: usize,
    }

    #[derive(Clone)]
    struct FrameTiming {
        key: bool,
        pts: u64,
        dts: u64,
        id: u64,
    }

    enum RenditionEvent {
        Video {
            rendition_index: usize,
            access_unit: AccessUnit,
        },
        Audio(AccessUnit),
    }

    struct QuadraAccessUnitSink {
        input: mpsc::Sender<AccessUnit>,
    }

    #[async_trait::async_trait]
    impl AccessUnitSink for QuadraAccessUnitSink {
        async fn push_access_unit(&self, access_unit: AccessUnit) -> Result<(), String> {
            self.input
                .send(access_unit)
                .await
                .map_err(|_| "Quadra rendition worker stopped".to_owned())
        }
    }

    pub async fn start_quadra_rendition_pipeline(
        config: QuadraRenditionConfig,
        targets: Vec<QuadraRenditionTarget>,
        playlists: Arc<Playlists>,
        publisher: Arc<dyn Fmp4PartPublisher>,
        min_part_ms: u32,
    ) -> Result<Arc<dyn AccessUnitSink>, String> {
        config.validate()?;
        if targets.len() != config.renditions.len()
            || targets
                .iter()
                .zip(&config.renditions)
                .any(|(target, rendition)| target.spec != *rendition)
        {
            return Err("Quadra rendition targets do not match the validated ladder".to_owned());
        }

        let (input_tx, mut input_rx) = mpsc::channel::<AccessUnit>(config.queue_capacity);
        let (event_tx, mut event_rx) = mpsc::channel(config.queue_capacity.saturating_mul(2));
        let (ready_tx, ready_rx) = oneshot::channel();

        let packaging_targets = targets.clone();
        tokio::spawn(async move {
            let mut segmenters = packaging_targets
                .iter()
                .map(|target| {
                    Fmp4Segmenter::new_publish_only(
                        target.spec.stream_id,
                        target.stream_idx,
                        playlists.clone(),
                        TimestampInput::Ticks90Khz,
                        min_part_ms,
                        publisher.clone(),
                    )
                })
                .collect::<Vec<_>>();
            while let Some(event) = event_rx.recv().await {
                match event {
                    RenditionEvent::Video {
                        rendition_index,
                        access_unit,
                    } => {
                        if let Some(segmenter) = segmenters.get_mut(rendition_index) {
                            segmenter.push_access_unit(access_unit).await;
                        }
                    }
                    RenditionEvent::Audio(access_unit) => {
                        for segmenter in &mut segmenters {
                            segmenter.push_access_unit(access_unit.clone()).await;
                        }
                    }
                }
            }
            for segmenter in &mut segmenters {
                segmenter.finish().await;
            }
        });

        let hardware_config = config.clone();
        tokio::task::spawn_blocking(move || {
            let mut ready_tx = Some(ready_tx);
            let result = (|| -> Result<(), String> {
                init(true, 30).map_err(|error| format!("failed to initialize Quadra: {error}"))?;
                let (decoder_tx, decoder_rx) =
                    std::sync::mpsc::channel::<Result<XcoderDecoderInputFrame, Infallible>>();
                let mut decoder = XcoderDecoder::new(
                    XcoderDecoderConfig {
                        width: i32::from(hardware_config.source_width),
                        height: i32::from(hardware_config.source_height),
                        bit_depth: 8,
                        fps: hardware_config.frame_rate(),
                        codec: XcoderDecoderCodec::H264,
                        hardware_id: hardware_config.hardware_id,
                        multicore_joint_mode: false,
                    },
                    decoder_rx,
                )
                .map_err(|error| format!("failed to open Quadra decoder: {error}"))?;

                struct Encoding {
                    scaler: XcoderScaler,
                    encoder: XcoderEncoder<FrameTiming>,
                }

                let mut encodings = targets
                    .iter()
                    .map(|target| {
                        let scaler = XcoderScaler::new(XcoderScalerConfig {
                            hardware: decoder.hardware(),
                            width: i32::from(target.spec.width),
                            height: i32::from(target.spec.height),
                            bit_depth: 8,
                        })
                        .map_err(|error| {
                            format!(
                                "failed to open Quadra scaler for stream {}: {error}",
                                target.spec.stream_id
                            )
                        })?;
                        let encoder = XcoderEncoder::new(XcoderEncoderConfig {
                            width: target.spec.width,
                            height: target.spec.height,
                            fps: hardware_config.frame_rate(),
                            bitrate: Some(target.spec.average_bandwidth_bps),
                            codec: XcoderEncoderCodec::H264 {
                                profile: None,
                                level_idc: None,
                            },
                            pixel_format: XcoderPixelFormat::Yuv420Planar,
                            bit_depth: 8,
                            hardware: Some(decoder.hardware()),
                            multicore_joint_mode: false,
                        })
                        .map_err(|error| {
                            format!(
                                "failed to open Quadra encoder for stream {}: {error}",
                                target.spec.stream_id
                            )
                        })?;
                        Ok(Encoding { scaler, encoder })
                    })
                    .collect::<Result<Vec<_>, String>>()?;

                let _ = ready_tx.take().expect("readiness sender").send(Ok(()));
                info!(
                    source_stream_id = hardware_config.source_stream_id,
                    renditions = encodings.len(),
                    "Quadra adaptive rendition pipeline ready"
                );

                let mut pending_video = VecDeque::new();
                let mut pending_audio = VecDeque::new();
                while let Some(access_unit) = input_rx.blocking_recv() {
                    if access_unit.stream_type != PSI_STREAM_H264 {
                        if pending_audio.len() >= hardware_config.queue_capacity {
                            pending_audio.pop_front();
                            warn!("Quadra audio alignment queue reached its bound");
                        }
                        pending_audio.push_back(access_unit);
                        continue;
                    }

                    let timing = FrameTiming {
                        key: access_unit.key,
                        pts: access_unit.pts,
                        dts: access_unit.dts,
                        id: access_unit.id,
                    };
                    decoder_tx
                        .send(Ok(XcoderDecoderInputFrame {
                            data: access_unit.data.to_vec(),
                            dts: access_unit.dts,
                            pts: access_unit.pts,
                        }))
                        .map_err(|_| "Quadra decoder input closed".to_owned())?;
                    pending_video.push_back(timing);

                    let Some(decoded) = decoder
                        .try_read_decoded_frame()
                        .map_err(|error| format!("Quadra decode failed: {error}"))?
                    else {
                        continue;
                    };
                    let timing = pending_video
                        .pop_front()
                        .ok_or_else(|| "Quadra decoder produced an untracked frame".to_owned())?;
                    let hardware_frame: XcoderHardwareFrame = decoded.into();
                    let mut emitted_pts = Vec::with_capacity(encodings.len());
                    for (rendition_index, encoding) in encodings.iter_mut().enumerate() {
                        let scaled = encoding
                            .scaler
                            .scale(&hardware_frame)
                            .map_err(|error| format!("Quadra scale failed: {error}"))?;
                        let frame_type = if timing.key {
                            EncodedFrameType::Key
                        } else {
                            EncodedFrameType::Auto
                        };
                        if let Some(output) = encoding
                            .encoder
                            .encode_hardware_frame_with_type(timing.clone(), scaled, frame_type)
                            .map_err(|error| format!("Quadra encode failed: {error}"))?
                        {
                            if let Some(encoded) = output.encoded_frame {
                                let output_timing = output.raw_frame;
                                emitted_pts.push(output_timing.pts);
                                event_tx
                                    .blocking_send(RenditionEvent::Video {
                                        rendition_index,
                                        access_unit: AccessUnit {
                                            key: encoded.is_keyframe,
                                            pts: output_timing.pts,
                                            dts: output_timing.dts,
                                            data: Bytes::from(encoded.data),
                                            stream_type: PSI_STREAM_H264,
                                            id: output_timing.id,
                                        },
                                    })
                                    .map_err(|_| "Quadra rendition packaging stopped".to_owned())?;
                            }
                        }
                    }

                    if emitted_pts.len() == encodings.len() {
                        let audio_cutoff = emitted_pts.into_iter().min().unwrap_or(timing.pts);
                        while pending_audio
                            .front()
                            .is_some_and(|audio| audio.pts <= audio_cutoff)
                        {
                            let audio = pending_audio.pop_front().expect("front was present");
                            event_tx
                                .blocking_send(RenditionEvent::Audio(audio))
                                .map_err(|_| "Quadra rendition packaging stopped".to_owned())?;
                        }
                    }
                }
                Ok(())
            })();

            if let Err(error) = result {
                if let Some(ready_tx) = ready_tx.take() {
                    let _ = ready_tx.send(Err(error.clone()));
                }
                error!(error, "Quadra adaptive rendition pipeline stopped");
            }
        });

        ready_rx
            .await
            .map_err(|_| "Quadra rendition worker exited during startup".to_owned())??;
        Ok(Arc::new(QuadraAccessUnitSink { input: input_tx }))
    }
}

#[cfg(all(feature = "quadra-renditions", target_os = "linux"))]
pub use linux::{start_quadra_rendition_pipeline, QuadraRenditionTarget};

#[cfg(test)]
mod tests {
    use super::*;

    fn config() -> QuadraRenditionConfig {
        QuadraRenditionConfig {
            source_stream_id: 1,
            source_width: 3_840,
            source_height: 2_160,
            frame_rate_millihertz: 59_940,
            hardware_id: None,
            queue_capacity: 128,
            renditions: vec![
                "101:1920x1080:7000000:8000000".parse().unwrap(),
                "102:1280x720:3500000:4000000".parse().unwrap(),
            ],
        }
    }

    #[test]
    fn parses_and_validates_a_ladder() {
        let config = config();
        config.validate().unwrap();
        assert_eq!(config.frame_rate(), 59.94);
        assert_eq!(
            config.renditions[0].hls_rendition_argument(1, 59_940),
            "1:101:8000000:7000000:1920x1080:59940"
        );
    }

    #[test]
    fn rejects_invalid_or_ambiguous_ladders() {
        assert!("101:1920x1080:8000000:7000000"
            .parse::<QuadraRenditionSpec>()
            .is_err());
        assert!("101:1920:7000000:8000000"
            .parse::<QuadraRenditionSpec>()
            .is_err());

        let mut invalid = config();
        invalid.renditions[1].stream_id = 101;
        assert!(invalid.validate().unwrap_err().contains("duplicate"));

        let mut upscale = config();
        upscale.renditions[0].width = 4_096;
        assert!(upscale.validate().unwrap_err().contains("upscale"));
    }
}
