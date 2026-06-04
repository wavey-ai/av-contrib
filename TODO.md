# TODO

## Local OBS-like ingest harness

- Keep generated fixtures and logs under ignored `test/work/`.
- Current prepared fixtures:
  - `test/work/media/lori-360p.ts`
  - `test/work/media/lori-720p.ts`
  - `test/work/media/lori-1080p.ts`
- Current good 360p, 720p, and 1080p full-video paths:
  - `srt`
  - `rist-rust-pure`
  - `rist-rust-librist`
- Current partially good 360p, 720p, and 1080p paths:
  - `rist-ffmpeg-pure` serves LL-HLS but FFmpeg/librist exits `187` at close
    with `Error closing file`.
  - `rist-ffmpeg-librist` serves LL-HLS but FFmpeg/librist exits `187` at
    close with `Error closing file`.
- Treat the FFmpeg/librist close status as unresolved until isolated; do not
  hide it as a pass.
- Current fMP4/H.264 bridge status:
  - Length-prefixed H.264 access units now reject truncated length prefixes and
    truncated NALUs explicitly instead of silently disappearing.
  - SPS display dimensions now use the H.264 crop units with checked arithmetic.
  - Annex B to AVCC packetization validates SPS NALUs before emitting access
    units, so false SPS candidates from MPEG-TS payload scanning do not reach
    the fMP4 config parser.
  - Keyframe AVC config changes force a fresh init segment; non-key resolution
    changes are still dropped; same-resolution non-key SPS/PPS churn is stripped
    and kept as media.
  - Verified with `cargo test --locked fmp4_bridge -- --nocapture`,
    `cargo test --locked`, and a 30-second `srt 360p` smoke. The smoke stayed
    LL-HLS-readable and the latest log no longer contains fMP4 `WARN`, SPS
    rejection, implausible SPS, or same-resolution non-key config-drop messages.
  - Remaining protocol issue: the FFmpeg/librist close status below is still
    unresolved and should stay visible until isolated.

## Next Protocol Checks

Re-run these exact modes after touching RIST:

```sh
AV_CONTRIB_TEST_LIMIT_SECONDS=80 \
AV_CONTRIB_TEST_POST_SEND_WATCH_SECONDS=2 \
AV_CONTRIB_TEST_FFMPEG_LOGLEVEL=warning \
AV_CONTRIB_TEST_RUST_LOG=av_contrib=info,upload_response=info,playlists=debug,web_service=info,hls=debug,info \
test/local-video-pipeline.sh run rist-rust-pure 360p
```

```sh
AV_CONTRIB_TEST_LIMIT_SECONDS=80 \
AV_CONTRIB_TEST_POST_SEND_WATCH_SECONDS=2 \
AV_CONTRIB_TEST_FFMPEG_LOGLEVEL=warning \
AV_CONTRIB_TEST_RUST_LOG=av_contrib=info,upload_response=info,playlists=debug,web_service=info,hls=debug,info \
test/local-video-pipeline.sh run rist-rust-librist 360p
```

If either run fails, inspect the matching log under `test/work/logs/` and search for:

```sh
rg 'ERROR|WARN|reorder|continuity|panic|RIST|rist' test/work/logs
```

## Fixture Preparation

Prepare or refresh fixtures:

```sh
AV_CONTRIB_TEST_X264_PRESET=ultrafast test/local-video-pipeline.sh prepare 720p
AV_CONTRIB_TEST_X264_PRESET=ultrafast test/local-video-pipeline.sh prepare 1080p
AV_CONTRIB_TEST_X264_PRESET=ultrafast test/local-video-pipeline.sh prepare 2160p
```

Use 720p first for resource isolation before spending time on 1080p/2160p.

## Full-Video Matrix

```sh
unset AV_CONTRIB_TEST_LIMIT_SECONDS
AV_CONTRIB_TEST_POST_SEND_WATCH_SECONDS=2 \
AV_CONTRIB_TEST_FFMPEG_LOGLEVEL=warning \
AV_CONTRIB_TEST_RUST_LOG=av_contrib=info,upload_response=info,playlists=debug,web_service=info,hls=debug,info \
test/local-video-pipeline.sh matrix 720p
```

Prepared 360p, 720p, and 1080p matrices have been run. Prepare and run 2160p
only if we need a local resource-pressure check.

## Already Fixed And Pushed

- `playlists` fixed the LL-HLS open segment ring-boundary bug that advertised segment IDs whose slot still held stale boundaries.
- `web-services` added retained-window and open-ring-boundary coverage.
- `av-contrib` updated dependency tips and the local harness now probes the actual advertised `EXT-X-BYTERANGE` instead of `0-0`.
- `av-mesh` updated to the current dependency tips.

## Verification Already Run

```sh
cargo test --locked
cargo test -p hls --locked
cargo fmt --check && cargo test --locked
```

These passed in the relevant repos before this TODO was written.
