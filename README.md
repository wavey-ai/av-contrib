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
Capture-capable ingest adapters populate the separate capture timestamp from
source-provided timing.

Live multi-region qualification has a synchronized-clock deployment gate.
Needletail verifies host synchronization and measures the maximum error. It
configures the declared bound. It promotes synchronized or traceable provenance
after it verifies the clock source. Deadline-hit and glass-to-glass comparisons
use only hosts that pass that gate.

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
The supervisor defaults the LL-HLS part target to 50 ms. Override it with
`AV_LL_HLS_PART_MS` or `--part-ms`.

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
