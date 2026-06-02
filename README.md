# av-contrib

Contributor-facing web-service and sender tools for `av-mesh`.

`av-mesh` should stay focused on cache streams, RaptorQ mesh sync, telemetry,
replication, and serving. This repo owns edge-facing contributor formats and
tools. The main `av-contrib` binary accepts contributor HTTP uploads with
`web-service`, accepts optional RIST MPEG-TS input, uses the existing
`access-unit` crate for media detection, and forwards opaque bytes or media
access units into mesh RaptorQ/FEC ingest sockets.

```sh
cargo run --bin av-contrib -- \
  --http-port 9443 \
  --mesh-fec-target 127.0.0.1:12001 \
  --mesh-media-fec-target 127.0.0.1:12101 \
  --rist-bind 127.0.0.1:7000
```

Useful endpoints:

- `POST /ingest`: forwards request body chunks as opaque bytes over mesh UDP-FEC.
- `POST /media/access-unit?stream_id=55&codec=auto`: detects codec with
  `access-unit`, wraps the payload in the Wavey media/FEC header, and forwards
  it to mesh media UDP-FEC.
- `rist://<rist-bind>`: accepts RIST MPEG-TS contributor input and forwards
  recovered payload bytes to mesh UDP-FEC.

The stdin senders are kept for local smoke tests and protocol debugging:

```sh
cargo run --bin udp-fec-send -- 127.0.0.1:12001
cargo run --bin rist-send -- 127.0.0.1:7000
cargo run --bin media-fec-send -- --stream-id 55 --codec auto 127.0.0.1:12101
```
