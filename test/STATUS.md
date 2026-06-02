# av-contrib Local Video Pipeline Status

Generated fixtures live under ignored `test/work/media/` and are not committed.
Current prepared LORI MPEG-TS fixtures on this machine:

| Variant | File | Size | Notes |
| --- | --- | ---: | --- |
| 360p | `test/work/media/lori-360p.ts` | 29M | Prepared and used for smoke matrix |
| 720p | `test/work/media/lori-720p.ts` | 71M | Prepared |
| 1080p | `test/work/media/lori-1080p.ts` | 152M | Prepared |

Latest current-code evidence, run on 2026-06-02:

- 30s 360p smoke matrix passed the live HLS readability checks for every mode.
- Full 360p matrix log:
  `test/work/logs/matrix-360p-full-20260602-224147.log` (ignored, not
  committed).
- Full 720p matrix log:
  `test/work/logs/matrix-720p-full-20260602-230233.log` (ignored, not
  committed).
- Full 1080p matrix log:
  `test/work/logs/matrix-1080p-full-20260602-232129.log` (ignored, not
  committed).

| Mode | Result | Notes |
| --- | --- | --- |
| `srt` | Full 360p, 720p, and 1080p status 0, HLS readable | Playlist currently drops after ingest stream end |
| `rist-ffmpeg-pure` | Full 360p, 720p, and 1080p HLS readable, sender status 187 | FFmpeg/librist exits `187` with `Error closing file` after HLS became readable |
| `rist-ffmpeg-librist` | Full 360p, 720p, and 1080p HLS readable, sender status 187 | FFmpeg/librist exits `187` with `Error closing file` after HLS became readable |
| `rist-rust-pure` | Full 360p, 720p, and 1080p status 0, HLS readable | Native sender sent 30,170,992 bytes for 360p, 74,551,588 bytes for 720p, and 158,918,092 bytes for 1080p |
| `rist-rust-librist` | Full 360p, 720p, and 1080p status 0, HLS readable | Native Rust sender fed librist receiver using `0x11223344` flow id and sent the full prepared fixtures |

Known remaining issues:

- FFmpeg/librist sender close returns status `187` after HLS is already
  readable. This repeats on 360p, 720p, and 1080p against both pure Rust and
  librist receivers, so it appears isolated to FFmpeg/librist sender shutdown
  rather than an ingest or HLS stall.
- The live playlist goes 404 after SRT ingest ends. That may be acceptable for a
  live-only endpoint, but it is still a behavior decision.
- The fMP4 bridge logs implausible H.264 SPS guards and ignored mid-stream
  resolution changes for some payloads while still continuing playback; this
  needs a real parser/packetization audit before calling it clean.
- 2160p is not prepared or run yet; use it only if we need a local
  resource-pressure check beyond 1080p.
