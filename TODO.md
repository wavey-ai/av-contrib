# TODO

## Local OBS-like ingest harness

- Continue from the 360p fixture at `test/work/media/lori-360p.ts`.
- Keep test artifacts under ignored `test/work/`.
- Current good path: SRT 360p ran for 80 seconds, crossed the segment ring boundary, and served LL-HLS media byte ranges cleanly.
- Current RIST status:
  - `rist-ffmpeg-pure` served LL-HLS cleanly through the retained window.
  - `rist-ffmpeg-librist` served LL-HLS cleanly through the retained window.
  - `rist-rust-pure` served LL-HLS cleanly through the retained window and the native sender exited cleanly.
  - `rist-rust-librist` sent the full 360p byte count, but librist ingest never produced an upload-response stream or LL-HLS playlist.
  - FFmpeg/librist sender exits with status `187` at close after HLS is already readable: `Error closing file: Generic error in an external library`.
  - Treat that as unresolved until isolated; do not hide it as a pass.

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

Prepare larger fixtures once the 360p protocol matrix is understood:

```sh
AV_CONTRIB_TEST_X264_PRESET=ultrafast test/local-video-pipeline.sh prepare 720p
AV_CONTRIB_TEST_X264_PRESET=ultrafast test/local-video-pipeline.sh prepare 1080p
AV_CONTRIB_TEST_X264_PRESET=ultrafast test/local-video-pipeline.sh prepare 2160p
```

Use 720p first for resource isolation before spending time on 1080p/2160p.

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
