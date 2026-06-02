# av-contrib Local Video Pipeline

This folder contains the local full-video protocol harness. Generated media and
logs are written to `test/work/`, which is ignored by git.

Prepare H.264/AAC MPEG-TS fixtures from the local LORI source:

```sh
test/local-video-pipeline.sh prepare all
```

Run the full prepared video through the OBS-style ingest path:

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

See `test/STATUS.md` for the latest local matrix results and known remaining
protocol gaps.
