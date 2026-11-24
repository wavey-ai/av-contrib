# av-contrib

Contributor-facing web-service and sender tools for `av-mesh`.

`av-mesh` should stay focused on cache streams, RaptorQ mesh sync, telemetry,
replication, and serving. This repo owns edge-facing contributor formats and
tools.

The main `av-contrib` binary accepts arbitrary contributor byte streams.
It terminates RIST/SRT MPEG-TS and RTMP/FLV inputs from any compatible sender.
It also accepts
AEP1/RaptorQ DAW audio and produces format-preserving LL-HLS artifacts with
`playlists`. It publishes only stream-addressed artifact bytes into mesh ingest.
Raw RIST, SRT, RTMP, and MPEG-TS payloads do not cross the mesh boundary.

Each AEP1 contribution route chooses its LL-HLS packaging policy. The default
`opaque` policy uses AEP1 stream identity, timing, continuity, and FEC recovery
to publish every recovered payload byte-for-byte, without interpreting its
inner format. The explicit `fmp4` policy keeps boxing in `av-contrib` for raw
PCM and FLAC sources that need CMAF/fMP4 output. It never silently converts one
codec into another, and framed or encrypted Opus must use `opaque`.

At 5 ms an opaque part contains one recovered media unit. A configured larger part may
concatenate consecutive self-delimiting units. A future producer-authored,
unencrypted Opus CMAF/fMP4 program rendition can provide generic-player
compatibility without changing the private path.

Reliability boundary: RIST and SRT belong here at the contributor edge because
they are mature WAN ingest protocols with retransmission history. The mesh hot
path is still RaptorQ-FEC over stream-addressed artifacts because it gives fixed
low-latency recovery for bounded loss. FEC is not magic reliability. If repair
budget is exceeded, the mesh needs a separate slot repair/backfill path rather
than pushing raw RIST/SRT semantics through every mesh node.

## Measured DAW audio capacity

One track is one independent stereo, 48 kHz, 24-bit DAW source.

The test produced FLAC and Opus LL-HLS renditions for each track.

The contributor used a GCP `n2-standard-2` VM in London.

The VM had two vCPUs on Intel Cascade Lake.

Both tests used 250 ms parts and the same RaptorQ relay fan-out.

| Measurement | Eight tracks | Sixteen tracks |
| --- | ---: | ---: |
| Measurement window | 600 seconds | 106 seconds |
| LL-HLS output streams | 16 | 32 |
| Contributor CPU | P99 83.92% | Maximum 198.07% |
| Contributor host capacity | P99 41.96% | Maximum 99.03% |
| Maximum service memory | 175.28 MiB | 208.02 MiB |
| LL-HLS queue capacity | 4,096 | 4,096 |
| LL-HLS queue drops | 0 | 480,572 |
| Mesh queue capacity | 32,768 | 32,768 |
| Mesh queue drops | 0 | 84,423 |

The eight-track contributor window had no queue drops or worker errors.

It also had no ingress errors, mesh forwarding errors, or kernel UDP socket drops.

The eight-track result proves contributor capacity for this configuration.

It does not prove playback qualification.

Reused stream IDs caused invalid edge sequence state during the playback checks.

The sixteen-track LL-HLS queue first became full approximately five seconds after media started.

The mesh forwarding queue became full approximately 12 seconds after media started.

The contributor then used both vCPUs and could not drain either handoff queue.

The source kept all 16 tracks connected during the partial run.

The source reported no dropped frames, connection failures, or UDP send errors.

Its latest host sample used 11.85% CPU.

The DAW source process used 7.37% of the 16-vCPU source host.

The Linux UDP counters reported no receive-buffer or send-buffer errors.

These results locate the sixteen-track limit in contributor processing.

### Why sixteen tracks failed

The eight-track source sent approximately 5,762 AEP1 datagrams each second.

The contributor accepted all these datagrams.

The sixteen-track source sent approximately 10,632 AEP1 datagrams each second.

The LL-HLS handoff accepted approximately 6,100 datagrams each second.

It rejected approximately 4,532 datagrams each second.

The mesh handoff accepted approximately 9,836 datagrams each second.

It rejected approximately 796 datagrams each second.

The UDP receive loop copies each datagram into one LL-HLS queue and one mesh queue.

One LL-HLS recovery task owns the FEC state for all datagrams from the source peer.

That task sends recovered groups to 32 rendition tasks.

Each rendition task boxes 250 ms fMP4 parts and creates canonical media objects.

It then applies RaptorQ and waits for relay publication.

One mesh task also forwards each source datagram to two relay ingress targets.

The default Tokio runtime had two worker threads on the two-vCPU host.

Slow fMP4 publication and two renditions for each track kept both worker threads busy.

The UDP receive loop remained live because both handoffs use nonblocking `try_send`.

The bounded queues absorbed the initial processing deficit.

They rejected new datagrams after they became full.

The kernel socket, source, and network did not cause these drops.

### Limits for the next capacity test

Treat approximately 6,100 LL-HLS input datagrams each second as the measured limit for this two-vCPU configuration.

Limit this configuration to eight tracks until another complete test passes.

Use a dedicated 16-vCPU contributor for the next sixteen-track qualification.

Test smaller machine sizes separately if the minimum production size is required.

Do not increase queue capacity as the primary correction.

A larger queue can delay the first drop.

It cannot correct sustained processing overload.

Keep the 4,096-entry LL-HLS queue as an early overload detector during qualification.

Keep the 32,768-entry mesh queue unless measured network bursts require a different value.

Add LL-HLS queue-age and drain-rate metrics before changing either queue.

Add FEC recovery, rendition dispatch, fMP4 boxing, and relay publication stage metrics.

If a larger host still reaches one-core recovery saturation, divide FEC recovery by object identity.

Keep publication order for each stream when work moves between worker shards.

Support a per-format packaging policy.

Publish FLAC as opaque byte-exact parts and publish Opus as fMP4.

This policy removes unnecessary FLAC boxing without removing the required Opus rendition.

Require zero handoff drops before the timed measurement starts.

```sh
cargo run --bin av-contrib -- \
  --http-port 9443 \
  --mesh-fec-target 127.0.0.1:12001 \
  --mesh-media-fec-target 127.0.0.1:12101 \
  --rist-bind 127.0.0.1:7000 \
  --srt-bind 127.0.0.1:7002 \
  --rtmp-bind 127.0.0.1:1935
```

Needletail local composition can also enable the RaptorQ-first RelaySession
lane. Each target is the assigned `av-mesh --fec-bind` address. Source symbols
flow to the primary and initial repair symbols flow to the warm secondary.
With one parent, repair symbols use the same long-lived primary carrier.

```sh
cargo run --bin av-contrib -- \
  --relay-primary-bind 127.0.0.1:13001 \
  --relay-primary-target 127.0.0.1:12001 \
  --relay-secondary-bind 127.0.0.1:13002 \
  --relay-secondary-target 127.0.0.1:12002 \
  --relay-local-id contributor-london \
  --relay-primary-id relay-amsterdam \
  --relay-secondary-id relay-paris \
  --relay-topology-generation 7 \
  --relay-subscription-id 19 \
  --relay-deadline-ms 1000 \
  --wall-clock-estimated-error-ms 1000
```

Both relay targets default to disabled, keeping the compatibility UDP-FEC lane
as the default. Equivalent `AV_RELAY_*` environment variables are available to
the Needletail host-agent composer. Needletail assigns fixed primary and
secondary bind ports and registers those exact source endpoints with the two
receiving relay sessions. Test and authenticated-session setups may omit the
bind flags to receive family-correct ephemeral ports. The live lane protects
the complete canonical MOBJ envelope with adaptive RaptorQ.

Each RelaySession
datagram carries its object key and coding geometry. It also carries the expiry,
generation, subscription, and source or repair path intent. Initialization,
catalog, subscription, and bounded backfill messages belong to the reliable
RelaySession channel used by the controller-managed rollout.

Canonical media publication carries the packager-reported `duration-ms`, bounded
`track-composition`, codec, and `scheduler-class` metadata. Each media object
that needs an initialization object depends on its complete immutable
`ObjectKey`. A stable SHA-256-derived configuration epoch keeps that identity
consistent across retries and later parts using the same configuration.
Opaque private parts declare no initialization dependency. Muxed delta
parts containing audio use audio scheduling priority, while keyframes retain
the strongest media priority.

The object envelope records `Packaged` and publication-handoff `Published`
timestamps from the contributor host realtime clock. `--relay-deadline-ms` is
the canonical delivery budget added to that immutable `Published` timestamp,
and RelaySession carries the same expiry rounded up to Unix microseconds.
`--wall-clock-estimated-error-ms` records the host's explicit estimated clock
error in every timestamp. `/api/status` and `/metrics` expose that provenance.
An opaque AEP1 audio part is the exception: it is named and timed from its
source alone (number `pts / P`, `Packaged` and `Published` the source's
instant on clock `source:aep1`, the deadline a budget on the stream's own
timeline), so two contributors fed the same source publish the same bytes;
see "Two contributors" below.
Capture-capable ingest adapters populate the separate capture timestamp from
source-provided timing.

Live multi-region qualification has a synchronized-clock deployment gate.
Needletail verifies host synchronization and measures the maximum error. It
configures the declared bound. It promotes synchronized or traceable provenance
after it verifies the clock source. Deadline-hit and glass-to-glass comparisons
use only hosts that pass that gate.

Two contributors (the resilient mesh). Each flag is described in `--help`;
this is where they belong:

- Identity: no flag chooses how an opaque part is named. At start a
  self-test digests what this build makes for fixed parts (`numbering`,
  `envelope`, `wire`); the fingerprint's low 16 bits are in every opaque
  part's object version. `--print-fingerprint` prints that identity as JSON
  and exits without binding or reading anything; the deploy compares the two
  contributors' with it.
- Node: `--node-id` and `--region` name it in `GET /api/node`, and
  `--version` prints `av-contrib 0.1.0 (<commit> <unix>)`.
- Stop: SIGTERM or SIGINT stops ingest, drains the lanes for at most
  `--shutdown-grace-ms` (500), sends each lane a last ledger and exits 0; a
  second signal exits at once.
- Stall instruments: `--stall-journal-capacity`, `--stall-tick-us`,
  `--mlock off|rings`, `--prefault`, `--malloc-tune`.
- Source: `aep1-48k-probe send --target A --target B` feeds both
  contributors from one encoder.

Useful endpoints:

- `POST /ingest?stream_id=55`: publishes arbitrary request body chunks as
  stream-addressed mesh byte slots. Stream ids should be decimal strings when
  sent from browser-facing code.
- `POST /media/access-unit?stream_id=55&codec=auto`: detects codec with
  `access-unit`, wraps the payload in the Wavey media/FEC header, and forwards
  it to mesh media UDP-FEC.
- `GET /<stream_id>/stream.m3u8`: serves the local LL-HLS playlist generated by
  RIST/SRT/RTMP ingest.
- `GET /api/status`: returns Mission Control JSON. It includes relay targets,
  LL-HLS timing, FEC settings, contributor listeners, and browser-safe stream
  ID strings. It also includes runtime counters, protocol sessions, publish
  errors, and current alerts.
- `GET /api/node`: the node's build, process, readiness state (`starting`,
  `standing_by`, `proving`, `ready`, `stopping`), identity, sources, streams
  (with their window digests) and lanes, schema `needletail.node.v1`.
- `GET /api/stalls?since_us=T`: the stall journal (NDJSON, the same lines as
  av-mesh's). `GET /api/pacing?since_s=S`: each second of the DAW socket's
  reads and of each AEP1 source session, kept 300 s.
- `GET /api/status/events`: streams the same status snapshot once per second as
  Server-Sent Events using the named event `contrib`.
- `GET /metrics`: exposes Prometheus text metrics for ingest, protocol sessions,
  MPEG-TS damage, fMP4 publication, FEC traffic, errors, freshness, and latency.
  It includes bounded `encode_wait`, `encode`, `send`, and `telemetry` stage
  histograms. The same histograms publish p95 latency in `/api/status` for
  Mission Control. Raw stream requests use atomics to reserve globally unique
  FEC block and packet sequences. They then encode concurrently without a
  per-stream encoder lock.

  RelaySession metrics add bounded `role="source|repair"` datagram, byte, and
  send-error counters plus object, encode-error, and primary-repair-fallback
  counters. Carrier configuration uses the bounded `path="primary|secondary"`
  gauge. Deadline-budget, latest-expiry, and remaining-headroom gauges feed the
  Needletail realtime view. Canonical clock id, confidence, configured maximum
  error, object metadata, and dependency/timing fields feed the same view.
  `/api/status` carries the configured targets/carrier state and latest deadline
  headroom.
- `rist://<rist-bind>`: accepts RIST MPEG-TS through librist and demuxes
  H.264/AAC. It boxes fMP4/CMAF parts and serves LL-HLS locally. It publishes
  fMP4 part bytes to mesh under `--rist-stream-id` (default `0`).
- `srt://<srt-bind>`: accepts SRT MPEG-TS and follows the same fMP4
  path under `--srt-stream-id` (default `6`).
- `rtmp://<rtmp-bind>`: accepts RTMP/FLV access units and boxes them
  as fMP4 under `--rtmp-stream-id` (default `7`).
  A stream key that is a number names the output stream: an encoder that
  sends a ladder publishes `…/live/11`, `…/live/12` and so on, one rendition
  each. Video streams that start within a second of each other number their
  segments from the same instant.

Each part is stamped with its place in the LL-HLS playlist: Media Sequence
Number, part index, declared part target and declared target duration. The
caches of the mesh render the playlist from the stamps.

Parts are the smallest unit the source makes, by default. `--audio-part`
is `epoch` (one AEP1 epoch per part, whatever its sample count: 120 samples
of Opus are a 2.5 ms part, 48 samples of PCM a 1 ms part, 12 samples a
0.25 ms part), `<N>samples` or `<N>us`; a FLAC part is never under 32
samples. `--video-part` is `frame` (one access unit per part) or `<N>ms`.
The old `--fmp4-part-ms` and `--fmp4-video-part-ms` remain as aliases of a
duration; `--fmp4-part-ms` no longer sets the video part. Audio time is
counted in samples, so a part's duration is exact: each media object carries
`duration-us` (and `duration-samples` with `sample-rate` for audio),
`hls-part-target-us` and `hls-target-duration-us`, beside the old whole
millisecond keys (`duration-ms` rounded, the targets rounded up) for caches
that do not read the new ones. With one unit per part the declared part
target is the longest part seen (the longest frame of a video stream), not
a configured value. `--fmp4-video-segment-ms` sets the segment duration of a
video stream; set it to the key-frame interval of the encoder. A key frame
starts a part, and a segment is a whole number of groups of pictures. One
long segment, or one part longer than twice the target, does not change the
declared durations.

`--daw-hls-formats` defaults to `flac,pcm,opus`: PCM and Opus were added
to the old default of `flac`, which stays, so a source that sends FLAC (the
default payload of `aep1-48k-probe send`) still gets its rendition. Below
64 samples FLAC is written as verbatim frames, larger than PCM, so the
lossless rendition of the smallest parts is PCM; a source that sends all
three formats can leave FLAC out with `--daw-hls-formats pcm,opus`. fMP4 audio packaging (`--daw-hls-packaging fmp4`)
still goes through wavey-boxer's millisecond timeline: FLAC and PCM tracks
use a timescale of 1,000 and Opus decode times are taken from milliseconds.
The boxer drops a PCM access unit of under 1 ms, so the fMP4 rendition joins
sub-millisecond PCM epochs into access units of whole milliseconds (four
12-sample epochs make one 1 ms part): the default one-epoch part becomes
1 ms there. FLAC frames cannot be joined; FLAC epochs that are not whole
milliseconds (32 samples) are rounded by the boxer, with a warning. Exact
2.5 ms Opus, sub-millisecond PCM and 32-sample FLAC parts need the default
opaque path, or a boxer with a timescale of the sample rate.

Opus Custom frames. The DAW lane's Opus defaults to 1 ms Opus Custom CELT
frames at 320 kb/s CBR (48 samples; 64 and 72 samples, 1.33 and 1.5 ms, are
settings of the producer, daw-nexus `--opus-frame-ms`), the smallest frame
that is at least as good as standard 5 ms at 192 kb/s on PEAQ and ViSQOL;
1 ms at 256 kb/s is the lower-bandwidth option. A SoundKit v2 Opus packet
whose `frame_count` is not a multiple of 120 at 48 kHz is Opus Custom
(`soundkit_opus::custom_frame_size`); it carries no TOC but keeps libopus's
one-byte signalling header (the libopus default), so a decoder built with
SoundKit's defaults for that frame count reads it, and the byte checks the
frame size and channels (`opus_custom::LANE_OPUS_CUSTOM_SIGNALLING`). The
opaque rendition carries these packets unchanged, one frame per part by
default (`--audio-part epoch`). A part of a set size must hold
whole Opus frames: 64-sample frames fit 4 ms parts (three frames) but not
5 ms parts, and such a rendition is refused with an error naming the
frame. fMP4 packaging refuses Opus Custom (OpusHead and dOps cannot describe
it); anything that needs standard Opus (fMP4/Ogg, WebRTC/RTP, talkback, the
public programme rendition) needs the producer at `--opus-frame-ms 2.5` or
larger. `aep1-48k-probe` decodes either kind by the frame count of each
packet: `receive-hls --decode-soundkit-opus` (plain packets with
`--soundkit-opus-protection plain`, encrypted ones with
`--soundkit-key-file`), `receive-udp --decode-soundkit-opus`, and
`decode-soundkit-opus PART...` for saved parts; `--expected-opus-frame-samples`
makes any other frame size a failure, and the reports give
`opus_frame_samples` and `opus_mode` (`custom` or `standard`). `send
--payload opus` sends the lane's default, 1 ms Opus Custom at 320 kb/s
(`--opus-bitrate 256000` for the lower-bandwidth option, `--epoch-samples 120`
for standard 2.5 ms Opus).

The per-part send path is short. `--relay-send-batch` (default `true`)
hands the RelaySession symbols of a part to the kernel in one `sendmmsg`
call per lane instead of one awaited send per symbol. `--aep1-shared-recovery`
(default `true`) decodes each AEP1 datagram once: the LL-HLS worker's FEC
receiver hands every completed epoch to the SoundKit v2 normaliser of the
mesh lane, which only re-frames it; with `false` the normaliser decodes its
own copy of every datagram. With `--relay-exclusive` the canonical envelope
of the byte-FEC cache lane is no longer built for each part.

`--relay-direct-target ADDR` (repeatable, with `--relay-direct-id` per
target and one `--relay-direct-bind`) also sends each part's source symbols
straight to the distributors of the contributor's own region, beside the two
backbone lanes: one hop less for that region. All direct lanes share one
socket, so a part reaches every direct distributor in one `sendmmsg` call;
`--relay-direct-repair true` adds the repair symbols. A direct lane never
fails a part (`sent_direct` is journalled per distributor; failures are
counted). The symbols are the primary lane's, byte for byte, so a
distributor that has the contributor registered as a parent keeps whichever
copy arrives first. av-mesh does not yet register such a parent: a
distributor rejects RelaySession datagrams from an address that is not one
of its two backbone parents (`--relay-primary-peer`,
`--relay-secondary-peer`), at most two parents may join one object, and two
parents may not share a role. Until av-mesh accepts a third, direct parent,
leave the direct targets unset.

Audio is kept apart from video on the way out, so that a 4K part's
hundreds of symbols never hold an audio part back:

- `--relay-class-lanes` (default `false`): each lane is a socket pair on its
  source endpoint (`SO_REUSEPORT`; the parent's binding of the contributor
  does not change). Audio streams leave on one socket with `SO_PRIORITY`
  `--relay-audio-socket-priority` (6: band 0 of a `prio` qdisc) to the
  parent's port P; video streams leave on the other to
  P + `--relay-class-port-offset` (1000, the av-mesh receiver's convention),
  optionally paced with `--relay-video-pacing-mbps` (`SO_MAX_PACING_RATE`,
  under `fq`). A stream is video from its first part with video, for its
  source epoch and while it keeps sending video. With `false` every stream
  shares one socket and goes to P, and each part keeps its own class. It is
  off by default because only a parent that listens on P + 1000 (av-mesh
  `--relay-class-threads` with `--relay-controlled-local`, on Linux) takes
  the video class; the deploy and the lab render it on with their parents.
- `--relay-audio-inline` (default `true`): audio parts are published on the
  packager's own task, without a publication worker's queue, and each lane
  is handed an audio part's symbols with non-blocking `sendmmsg`, one lane
  after the other with no join; what a full socket buffer does not take
  waits, in order, in that lane's backlog thread (counted as
  `lane_backlog_parts`; the lane's object outcome is recorded when the
  backlog has sent, dropped or expired the part). The journal's
  `sent_first` is the first datagram of a part handed to the kernel on any
  lane.
- `--relay-repair-both-lanes` (default `false`): the primary lane carries
  the repair symbols of audio parts too, so the primary parent alone
  recovers a lost audio source (a 4K part's repair stays on the secondary).
  The parent must accept repair on that lane (av-mesh
  `--relay-primary-promoted`, a `source_and_repair` carrier link); a parent
  that does not counts them as unauthorized and drops them, so the deploy
  renders the two together.
- `--audio-hot-thread` (default `true`): the DAW media receive, the AEP1
  recovery and packagers and the inline audio publication run on audio hot
  threads, each with a single-threaded runtime, `SCHED_FIFO`
  `--audio-hot-fifo-priority` (10) and thread `i` pinned to
  `--audio-hot-cpu` (2) + `i` where the host allows it (`CAP_SYS_NICE` or
  root). Refused, a thread runs with normal scheduling and unpinned, and
  says so in one info line. The first thread receives and recovers; the
  rendition workers (one per source, group and format) take the threads in
  turn from the second. `--audio-hot-workers` sets the number of threads;
  0 (default) is one per `--daw-hls-formats` format, at most as many as
  there are CPUs from `--audio-hot-cpu` on.

Each part's RelaySession symbols are encoded source first:

- `--relay-source-first` (default `true`, needs `--relay-send-batch`): a
  part's source symbols are cut straight from its envelope (RaptorQ is
  systematic) and handed to every lane that carries them before its repair
  symbols are computed; the repair then uses `raptorq`'s cached encoding
  plan for the part's symbol count. relay-session's encoder generates that
  plan for every object and solves the whole block before the first symbol
  can leave, which was most of an audio part's `packaged` -> `sent` (about
  0.2 ms of the 0.23 ms median with three DAW renditions cut together, in
  the in-process bench `renditions_packaged_to_sent`) and about 5 ms of a
  500 kB 4K frame. The datagrams are byte for byte relay-session's
  (tested), each RLS1 datagram is built once for every lane, and the
  encoder's block ids and packet sequences are atomics, so no publishing
  thread waits for another's encode. The journal's `encoded` is the
  source symbols ready; `sent` is still the last source symbol with the
  kernel on the primary.
- `--relay-video-symbol-size` (default `0`, the largest symbol that fits a
  1,472-byte datagram): the RaptorQ symbol size of non-audio objects (video
  parts and initializations). 1,088 keeps an edge's fast-lane datagram to
  a client at 1,128 bytes, which fits a WebTransport datagram; it costs
  about 15% more datagrams per 4K part.

The benchmarks behind these numbers are ignored tests:
`cargo test --release --bin av-contrib -- --ignored --nocapture
--test-threads 1` (`hot_path_breakdown_of_an_audio_part`,
`renditions_packaged_to_sent`, `video_lane_cost_per_part`) and
`cargo test --release --lib source_first_bench -- --ignored --nocapture`.

The stdin senders are kept for local smoke tests and protocol debugging:

```sh
cargo run --bin udp-fec-send -- 127.0.0.1:12001
cargo run --bin media-fec-send -- --stream-id 55 --codec auto 127.0.0.1:12101
```

Full-video local SRT/RIST pipeline tests live in `test/`. The generated MPEG-TS
fixtures and logs are written under ignored `test/work/`:

```sh
test/local-video-pipeline.sh prepare all
test/local-video-pipeline.sh run srt 720p
test/local-video-pipeline.sh run rist-ffmpeg-librist 720p
```

For local live-ingest testing with both mesh nodes and the contributor ingress
under one
Rust supervisor, run from this repo:

```sh
make stack
```

The supervisor builds release `av-contrib`, release `../av-mesh`, and
Needletail Mission Control. It passes those product assets to each playback edge
with `NEEDLETAIL_MISSION_CONTROL_DIST`. It uses the Infidelity wildcard TLS material
from `../tls/local.infidelity.io`. It starts UK and US mesh nodes plus one
`av-contrib` ingress. It prefixes each child process output line with its source.

By default, it uses stream ID `1`, UK egress
`https://local.infidelity.io:19444/live/1/stream.m3u8`, US egress
`https://local.infidelity.io:19445/live/1/stream.m3u8`, and Mission Control at
`/mesh` on both ports. The contributor status endpoints are available at
`https://local.infidelity.io:19443/api/status` and
`https://local.infidelity.io:19443/api/status/events`.

Any compatible sender can publish SRT to
`srt://local.infidelity.io:27001?mode=caller` or RIST to
`local.infidelity.io:27000` with the main profile. RTMP
compatibility remains available at `rtmp://local.infidelity.io:19350/live` with
stream key `live-local`.
The supervisor passes its LL-HLS part target as `AV_LL_HLS_PART_MS` or
`--part-ms`. `make service` runs av-contrib alone with `AUDIO_PART=epoch` and
`VIDEO_PART=frame`.

Useful overrides:

```sh
PART_MS=67 \
RUST_LOG=av_mesh=trace,av_contrib=trace,rtmp_ingress=debug \
  STACK_ARGS="--rtmp-bind 127.0.0.1:19351 --srt-bind 127.0.0.1:27011" \
  make stack STREAM_ID=4294967351 HOST=local.infidelity.io
```

Use `--mission-control-dist /path/to/dist` to reuse a specific asset build. Use
`--no-mission-control-build` to reuse existing assets. `--no-build` skips the
component release builds. The same flags can be passed through `STACK_ARGS`.
Run `make help` for service and Mission Control tasks.

## License

av-contrib is available under the [MIT License](LICENSE).
