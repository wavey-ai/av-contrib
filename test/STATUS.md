# av-contrib Local Video Pipeline Status

Generated fixtures live under ignored `test/work/media/` and are not committed.
Current prepared LORI MPEG-TS fixtures on this machine:

| Variant | File | Size | Notes |
| --- | --- | ---: | --- |
| 360p | `test/work/media/lori-360p.ts` | 29M | Prepared and used for smoke matrix |
| 720p | `test/work/media/lori-720p.ts` | 71M | Prepared |
| 1080p | `test/work/media/lori-1080p.ts` | 152M | Prepared |

Latest current-code smoke evidence, run on 2026-06-02 with
`AV_CONTRIB_TEST_LIMIT_SECONDS=30` and 360p:

| Mode | Result | Notes |
| --- | --- | --- |
| `srt` | HLS readable during send | Playlist currently drops after ingest stream end |
| `rist-ffmpeg-pure` | HLS readable during and after send | FFmpeg/librist exits `187` with `Error closing file` |
| `rist-ffmpeg-librist` | HLS readable during and after send | FFmpeg/librist exits `187` with `Error closing file` |
| `rist-rust-pure` | Exit 0, HLS readable | Native sender sent 4,279,068 bytes |
| `rist-rust-librist` | Exit 0, HLS readable | Confirms native Rust sender can feed librist receiver using `0x11223344` flow id |

Known remaining issues:

- FFmpeg/librist sender close returns status `187` after HLS is already readable.
  This appears isolated to FFmpeg/librist sender shutdown rather than an ingest or
  HLS stall.
- The live playlist goes 404 after SRT ingest ends. That may be acceptable for a
  live-only endpoint, but it is still a behavior decision.
- The fMP4 bridge logs implausible H.264 SPS guards for some payloads while still
  continuing playback; keep an eye on this during longer/full-video runs.
- Full-video matrix runs are still needed for 360p/720p/1080p before calling the
  LORI no-OBS pipeline fully proven.
