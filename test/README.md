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
test/local-video-pipeline.sh run rist-pure 720p
test/local-video-pipeline.sh run rist-librist 720p
```

By default RIST sends with FFmpeg/librist so `rist-pure` and `rist-librist`
compare receiver backends. To test the native Rust sender as well:

```sh
AV_CONTRIB_TEST_RIST_SENDER=rust test/local-video-pipeline.sh run rist-pure 720p
```

Unset `AV_CONTRIB_TEST_LIMIT_SECONDS` for full-video runs. Set it only for quick
smoke tests.
