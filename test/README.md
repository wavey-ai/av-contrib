# av-contrib Local Video Pipeline

This folder contains the local full-video protocol harness. Generated media and
logs are written to `test/work/`, which is ignored by git.

Prepare H.264/AAC MPEG-TS fixtures from the local LORI source:

```sh
test/local-video-pipeline.sh prepare all
```

Run the full prepared video through the generic live-ingest path:

```sh
test/local-video-pipeline.sh run srt 720p
test/local-video-pipeline.sh run rist-ffmpeg-pure 720p
test/local-video-pipeline.sh run rist-ffmpeg-librist 720p
test/local-video-pipeline.sh run rist-rust-pure 720p
test/local-video-pipeline.sh run rist-rust-librist 720p
```

RIST modes name the sender first and the av-contrib receiver backend second.
`matrix` runs SRT plus every RIST sender/backend combination for each variant.

Unset `AV_CONTRIB_TEST_LIMIT_SECONDS` for full-video runs. Set it only for quick
smoke tests.

## Exact MPEG-TS to fMP4 check

Use this check after a demuxer, access-unit, or packager change. The fixture must
contain H.264 video and ADTS AAC audio.

```sh
AV_CONTRIB_MPEG_TS_FIXTURE=target/diagnostics/testsrc-720p-remux.ts \
AV_CONTRIB_EXPECTED_WIDTH=1280 \
AV_CONTRIB_EXPECTED_HEIGHT=720 \
AV_CONTRIB_EXPECTED_VIDEO_FRAMES=625 \
AV_CONTRIB_EXPECTED_AUDIO_FRAMES=1173 \
AV_CONTRIB_FMP4_OUTPUT=target/diagnostics/testsrc-720p-fragmented.mp4 \
  cargo test --lib mpeg_ts_fixture -- --ignored
```

The first test verifies complete AVCC access units and source dimensions. The
second test verifies the video and audio sample counts in generated fMP4 parts.

Decode the generated output with strict error handling:

```sh
ffmpeg -v error -xerror \
  -i target/diagnostics/testsrc-720p-fragmented.mp4 \
  -f null -
```

Inspect the stream types, dimensions, and frame counts:

```sh
ffprobe -v error -count_frames \
  -show_entries stream=codec_name,width,height,nb_read_frames \
  -of json target/diagnostics/testsrc-720p-fragmented.mp4
```

For the command above, FFmpeg must report no decode error. `ffprobe` must report
625 H.264 frames at 1280x720 and 1,173 AAC frames.

See `test/STATUS.md` for the latest local matrix results and known remaining
protocol gaps.
