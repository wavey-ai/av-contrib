#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
WORK_DIR="${AV_CONTRIB_TEST_WORK_DIR:-"$ROOT/test/work"}"
MEDIA_DIR="$WORK_DIR/media"
LOG_DIR="$WORK_DIR/logs"
SOURCE="${LORI_SOURCE:-"$HOME/Documents/LORI 4k no grain hum.m4v"}"

STREAM_ID="${AV_CONTRIB_TEST_STREAM_ID:-1}"
HTTP_PORT="${AV_CONTRIB_TEST_HTTP_PORT:-19443}"
SRT_PORT="${AV_CONTRIB_TEST_SRT_PORT:-27001}"
RIST_PORT="${AV_CONTRIB_TEST_RIST_PORT:-27000}"
MESH_FEC_TARGET="${AV_CONTRIB_TEST_MESH_FEC_TARGET:-127.0.0.1:12001}"
MESH_MEDIA_FEC_TARGET="${AV_CONTRIB_TEST_MESH_MEDIA_FEC_TARGET:-127.0.0.1:12101}"

PART_MS="${AV_LL_HLS_PART_MS:-50}"
SEGMENT_MS="${AV_LL_HLS_SEGMENT_MS:-1000}"
TARGET_DURATION_MS="${AV_LL_HLS_TARGET_DURATION_MS:-6000}"
PLAYLIST_COUNT="${AV_CONTRIB_TEST_PLAYLIST_COUNT:-120}"
PLAYLIST_BUFFER_KB="${AV_CONTRIB_TEST_PLAYLIST_BUFFER_KB:-2048}"

RIST_PROFILE="${AV_CONTRIB_TEST_RIST_PROFILE:-main}"
RIST_FLOW_ID="${AV_CONTRIB_TEST_RIST_FLOW_ID:-0x11223344}"
RIST_BUFFER_MS="${AV_CONTRIB_TEST_RIST_BUFFER_MS:-120}"

FFMPEG_LOGLEVEL="${AV_CONTRIB_TEST_FFMPEG_LOGLEVEL:-info}"
X264_PRESET="${AV_CONTRIB_TEST_X264_PRESET:-veryfast}"
GOP_FRAMES="${AV_CONTRIB_TEST_GOP_FRAMES:-25}"
AUDIO_BITRATE="${AV_CONTRIB_TEST_AUDIO_BITRATE:-128k}"
HLS_POLL_SECONDS="${AV_CONTRIB_TEST_HLS_POLL_SECONDS:-1}"
POST_SEND_WATCH_SECONDS="${AV_CONTRIB_TEST_POST_SEND_WATCH_SECONDS:-5}"

CONTRIB_PID=""
LOG_TAIL_PID=""
HLS_WATCH_PID=""
HLS_OK_FILE=""

usage() {
  cat <<USAGE
Usage:
  test/local-video-pipeline.sh prepare [360p|720p|1080p|2160p|all...]
  test/local-video-pipeline.sh run <mode> [360p|720p|1080p|2160p]
  test/local-video-pipeline.sh matrix [360p|720p|1080p|2160p...]

Modes:
  srt
  rist-ffmpeg-pure
  rist-ffmpeg-librist
  rist-rust-pure
  rist-rust-librist

Defaults:
  source:     $SOURCE
  work dir:   $WORK_DIR
  full video: yes

Useful environment:
  LORI_SOURCE=/path/to/LORI.m4v
  AV_CONTRIB_TEST_LIMIT_SECONDS=30       # optional smoke limit; unset means full video
  AV_LL_HLS_PART_MS=50

The run modes start av-contrib locally, send a prepared MPEG-TS fixture in
real time, and poll the generated LL-HLS playlist plus latest media URI.
USAGE
}

log() {
  printf '[local-video] %s\n' "$*"
}

die() {
  printf '[local-video] ERROR: %s\n' "$*" >&2
  exit 1
}

require_cmd() {
  command -v "$1" >/dev/null 2>&1 || die "missing required command: $1"
}

all_variants() {
  printf '%s\n' 360p 720p 1080p 2160p
}

variant_height() {
  case "$1" in
    360p) printf '360\n' ;;
    720p) printf '720\n' ;;
    1080p) printf '1080\n' ;;
    2160p) printf '2160\n' ;;
    *) die "unknown variant: $1" ;;
  esac
}

variant_bitrate() {
  case "$1" in
    360p) printf '900k\n' ;;
    720p) printf '2500k\n' ;;
    1080p) printf '5500k\n' ;;
    2160p) printf '12000k\n' ;;
    *) die "unknown variant: $1" ;;
  esac
}

variant_bufsize() {
  case "$1" in
    360p) printf '1800k\n' ;;
    720p) printf '5000k\n' ;;
    1080p) printf '11000k\n' ;;
    2160p) printf '24000k\n' ;;
    *) die "unknown variant: $1" ;;
  esac
}

variant_file() {
  printf '%s/lori-%s.ts\n' "$MEDIA_DIR" "$1"
}

expand_variants() {
  if [[ "$#" -eq 0 ]]; then
    all_variants
    return
  fi
  local variant
  for variant in "$@"; do
    if [[ "$variant" == "all" ]]; then
      all_variants
    else
      variant_height "$variant" >/dev/null
      printf '%s\n' "$variant"
    fi
  done
}

prepare_variant() {
  local variant="$1"
  local height bitrate bufsize out
  height="$(variant_height "$variant")"
  bitrate="$(variant_bitrate "$variant")"
  bufsize="$(variant_bufsize "$variant")"
  out="$(variant_file "$variant")"

  mkdir -p "$MEDIA_DIR"
  [[ -f "$SOURCE" ]] || die "source video not found: $SOURCE"
  if [[ -s "$out" && "${AV_CONTRIB_TEST_FORCE_PREPARE:-0}" != "1" ]]; then
    log "using existing $variant fixture: $out"
    return
  fi

  log "preparing $variant full-video MPEG-TS fixture"
  ffmpeg -hide_banner -y \
    -i "$SOURCE" \
    $(ffmpeg_limit_args) \
    -map 0:v:0 -map 0:a:0 -sn -dn \
    -vf "scale=-2:${height}:flags=bicubic" \
    -c:v libx264 -preset "$X264_PRESET" -tune zerolatency \
    -profile:v high -pix_fmt yuv420p -bf 0 \
    -b:v "$bitrate" -maxrate "$bitrate" -bufsize "$bufsize" \
    -g "$GOP_FRAMES" -keyint_min "$GOP_FRAMES" -sc_threshold 0 \
    -c:a aac -b:a "$AUDIO_BITRATE" -ar 48000 -ac 2 \
    -mpegts_flags +resend_headers \
    -f mpegts "$out"

  ffprobe -hide_banner -v error \
    -show_entries stream=index,codec_type,codec_name,width,height,avg_frame_rate:format=duration,size,bit_rate \
    -of compact=p=0:nk=0 "$out"
}

prepare() {
  require_cmd ffmpeg
  require_cmd ffprobe
  local variant
  while IFS= read -r variant; do
    prepare_variant "$variant"
  done < <(expand_variants "$@")
}

base_url_host() {
  local cert="$ROOT/../tls/local.bitneedle.com/fullchain.pem"
  local key="$ROOT/../tls/local.bitneedle.com/privkey.pem"
  if [[ -f "$cert" && -f "$key" ]]; then
    printf 'local.bitneedle.com\n'
  else
    printf '127.0.0.1\n'
  fi
}

tls_args() {
  local cert="$ROOT/../tls/local.bitneedle.com/fullchain.pem"
  local key="$ROOT/../tls/local.bitneedle.com/privkey.pem"
  if [[ -f "$cert" && -f "$key" ]]; then
    printf '%s\n' --cert "$cert" --key "$key"
  fi
}

hls_url() {
  printf 'https://%s:%s/%s/stream.m3u8\n' "$(base_url_host)" "$HTTP_PORT" "$STREAM_ID"
}

cleanup() {
  local status=$?
  if [[ -n "$HLS_WATCH_PID" ]]; then
    kill "$HLS_WATCH_PID" >/dev/null 2>&1 || true
    wait "$HLS_WATCH_PID" >/dev/null 2>&1 || true
  fi
  if [[ -n "$CONTRIB_PID" ]]; then
    kill "$CONTRIB_PID" >/dev/null 2>&1 || true
    wait "$CONTRIB_PID" >/dev/null 2>&1 || true
  fi
  if [[ -n "$LOG_TAIL_PID" ]]; then
    kill "$LOG_TAIL_PID" >/dev/null 2>&1 || true
    wait "$LOG_TAIL_PID" >/dev/null 2>&1 || true
  fi
  trap - EXIT INT TERM
  return "$status"
}

wait_for_health() {
  local url="https://$(base_url_host):$HTTP_PORT/up"
  local attempt
  for attempt in $(seq 1 150); do
    if curl -kfsS "$url" >/dev/null 2>&1; then
      log "av-contrib healthy at $url"
      return
    fi
    sleep 0.2
  done
  die "av-contrib did not become healthy at $url"
}

start_contrib() {
  local mode="$1"
  local backend="$2"
  mkdir -p "$LOG_DIR"
  cargo build --bin av-contrib

  local bin="$ROOT/target/debug/av-contrib"
  local log_file="$LOG_DIR/av-contrib-${mode}-$(date +%Y%m%d-%H%M%S).log"
  local args=(
    --http-port "$HTTP_PORT"
    --mesh-fec-target "$MESH_FEC_TARGET"
    --mesh-media-fec-target "$MESH_MEDIA_FEC_TARGET"
    --stream-id "$STREAM_ID"
    --fmp4-part-ms "$PART_MS"
    --fmp4-segment-ms "$SEGMENT_MS"
    --hls-target-duration-ms "$TARGET_DURATION_MS"
    --playlist-count "$PLAYLIST_COUNT"
    --playlist-buffer-kb "$PLAYLIST_BUFFER_KB"
  )

  while IFS= read -r tls_arg; do
    args+=("$tls_arg")
  done < <(tls_args)

  if [[ "$mode" == "srt" ]]; then
    args+=(--srt-bind "127.0.0.1:$SRT_PORT" --srt-stream-id "$STREAM_ID")
  else
    args+=(
      --rist-bind "127.0.0.1:$RIST_PORT"
      --rist-stream-id "$STREAM_ID"
      --rist-profile "$RIST_PROFILE"
      --rist-backend "$backend"
      --rist-flow-id "$RIST_FLOW_ID"
    )
  fi

  log "starting av-contrib; logs: $log_file"
  RUST_LOG="${RUST_LOG:-${AV_CONTRIB_TEST_RUST_LOG:-av_contrib=debug,upload_response=info,playlists=debug,web_service=info,info}}" \
    "$bin" "${args[@]}" >"$log_file" 2>&1 &
  CONTRIB_PID=$!
  tail -n +1 -f "$log_file" &
  LOG_TAIL_PID=$!
  wait_for_health
}

watch_hls() {
  local url="$1"
  local last_marker=""
  local base="${url%/*}"
  local tmp status seq probe uri range marker
  while true; do
    tmp="$(mktemp)"
    status="$(curl -k -sS -w '%{http_code}' -o "$tmp" "$url" || true)"
    if [[ "$status" == "200" || "$status" == "206" ]]; then
      seq="$(awk -F: '/#EXT-X-MEDIA-SEQUENCE/{print $2}' "$tmp" | tail -n 1)"
      probe="$(awk '
        function byterange(line, fallback, parts, len, off) {
          if (line == "") return fallback
          split(line, parts, ":")
          split(parts[2], parts, "@")
          len = parts[1] + 0
          off = (parts[2] == "" ? 0 : parts[2] + 0)
          if (len <= 0) return fallback
          return off "-" (off + len - 1)
        }
        /#EXT-X-BYTERANGE:/ { segment_range = byterange($0, "0-0") }
        /#EXT-X-PART/ {
          split($0, uri_parts, "URI=\"")
          split(uri_parts[2], uri_value, "\"")
          uri = uri_value[1]
          range = "0-0"
          if (index($0, "BYTERANGE=\"") > 0) {
            split($0, range_parts, "BYTERANGE=\"")
            split(range_parts[2], range_value, "\"")
            range = byterange("#EXT-X-BYTERANGE:" range_value[1], range)
          }
        }
        /^[^#].*\.mp4/ {
          uri = $0
          range = segment_range == "" ? "0-0" : segment_range
          segment_range = ""
        }
        END {
          if (uri != "") print uri " " range
        }
      ' "$tmp")"
      uri="${probe%% *}"
      range="${probe#* }"
      if [[ "$range" == "$probe" ]]; then
        range="0-0"
      fi
      marker="${seq:-?}:${uri:-?}:${range:-?}"
      if [[ "$marker" != "$last_marker" ]]; then
        printf '[hls] playlist ok media_sequence=%s latest=%s range=%s\n' "${seq:-?}" "${uri:-?}" "${range:-?}"
        last_marker="$marker"
      else
        printf '[hls] playlist unchanged media_sequence=%s latest=%s range=%s\n' "${seq:-?}" "${uri:-?}" "${range:-?}"
      fi
      if [[ -n "$uri" ]]; then
        if curl -kfsS -r "$range" -o /dev/null "$base/$uri"; then
          if [[ -n "$HLS_OK_FILE" ]]; then
            printf 'ok\n' >"$HLS_OK_FILE"
          fi
        else
          printf '[hls] latest media range probe failed uri=%s range=%s\n' "$uri" "$range"
        fi
      fi
    else
      printf '[hls] playlist HTTP %s\n' "$status"
    fi
    rm -f "$tmp"
    sleep "$HLS_POLL_SECONDS"
  done
}

start_hls_watch() {
  HLS_OK_FILE="$(mktemp "$LOG_DIR/hls-ok.XXXXXX")"
  rm -f "$HLS_OK_FILE"
  watch_hls "$(hls_url)" &
  HLS_WATCH_PID=$!
}

ffmpeg_input_args() {
  printf '%s\n' -re
  if [[ -n "${AV_CONTRIB_TEST_STREAM_LOOP:-}" ]]; then
    printf '%s\n' -stream_loop "$AV_CONTRIB_TEST_STREAM_LOOP"
  fi
}

ffmpeg_limit_args() {
  if [[ -n "${AV_CONTRIB_TEST_LIMIT_SECONDS:-}" ]]; then
    printf '%s\n' -t "$AV_CONTRIB_TEST_LIMIT_SECONDS"
  fi
}

send_srt() {
  local fixture="$1"
  local url="srt://127.0.0.1:$SRT_PORT?mode=caller&transtype=live&pkt_size=1316&latency=120000&connect_timeout=3000"
  log "sending SRT fixture: $fixture"
  ffmpeg -hide_banner -nostdin -loglevel "$FFMPEG_LOGLEVEL" \
    $(ffmpeg_input_args) -i "$fixture" $(ffmpeg_limit_args) \
    -map 0 -c copy -f mpegts "$url"
}

send_rist_ffmpeg() {
  local fixture="$1"
  local url="rist://127.0.0.1:$RIST_PORT"
  log "sending RIST fixture with FFmpeg/librist: $fixture"
  ffmpeg -hide_banner -nostdin -loglevel "$FFMPEG_LOGLEVEL" \
    $(ffmpeg_input_args) -i "$fixture" $(ffmpeg_limit_args) \
    -map 0 -c copy \
    -rist_profile "$RIST_PROFILE" -buffer_size "$RIST_BUFFER_MS" -pkt_size 1316 \
    -f mpegts "$url"
}

send_rist_rust() {
  local fixture="$1"
  log "sending RIST fixture with native Rust sender: $fixture"
  cargo build --bin rist-send
  ffmpeg -hide_banner -nostdin -loglevel "$FFMPEG_LOGLEVEL" \
    $(ffmpeg_input_args) -i "$fixture" $(ffmpeg_limit_args) \
    -map 0 -c copy -f mpegts pipe:1 | \
    "$ROOT/target/debug/rist-send" \
      --profile "$RIST_PROFILE" \
      --flow-id "$RIST_FLOW_ID" \
      "127.0.0.1:$RIST_PORT"
}

send_rist() {
  local fixture="$1"
  local sender="$2"
  case "$sender" in
    ffmpeg) send_rist_ffmpeg "$fixture" ;;
    rust) send_rist_rust "$fixture" ;;
    *) die "unknown RIST sender: $sender" ;;
  esac
}

mode_backend() {
  case "$1" in
    srt) printf '\n' ;;
    rist-ffmpeg-pure|rist-rust-pure) printf 'pure\n' ;;
    rist-ffmpeg-librist|rist-rust-librist) printf 'librist\n' ;;
    *) die "unknown run mode: $1" ;;
  esac
}

mode_sender() {
  case "$1" in
    srt) printf '\n' ;;
    rist-ffmpeg-pure|rist-ffmpeg-librist) printf 'ffmpeg\n' ;;
    rist-rust-pure|rist-rust-librist) printf 'rust\n' ;;
    *) die "unknown run mode: $1" ;;
  esac
}

run_once() {
  local mode="$1"
  local variant="${2:-720p}"
  local backend sender fixture
  backend="$(mode_backend "$mode")"
  sender="$(mode_sender "$mode")"
  fixture="$(variant_file "$variant")"
  [[ -s "$fixture" ]] || prepare_variant "$variant"

  trap cleanup EXIT INT TERM
  start_contrib "$mode" "$backend"
  log "HLS URL: $(hls_url)"
  start_hls_watch

  local sender_status=0
  set +e
  if [[ "$mode" == "srt" ]]; then
    send_srt "$fixture"
  else
    send_rist "$fixture" "$sender"
  fi
  sender_status=$?
  set -e

  log "sender finished; keeping HLS watcher alive for ${POST_SEND_WATCH_SECONDS}s"
  sleep "$POST_SEND_WATCH_SECONDS"
  local final_status="$sender_status"
  if [[ ! -s "$HLS_OK_FILE" ]]; then
    log "HLS never became readable for $(hls_url)"
    final_status=86
  elif [[ "$sender_status" -ne 0 ]]; then
    log "sender exited with status $sender_status after HLS became readable"
  fi
  cleanup
  trap - EXIT INT TERM
  return "$final_status"
}

matrix() {
  local variant
  while IFS= read -r variant; do
    run_once srt "$variant"
    run_once rist-ffmpeg-pure "$variant"
    run_once rist-ffmpeg-librist "$variant"
    run_once rist-rust-pure "$variant"
    run_once rist-rust-librist "$variant"
  done < <(expand_variants "$@")
}

main() {
  local command="${1:-}"
  shift || true
  case "$command" in
    prepare) prepare "$@" ;;
    run)
      [[ "$#" -ge 1 ]] || die "run needs a mode"
      run_once "$1" "${2:-720p}"
      ;;
    matrix) matrix "$@" ;;
    -h|--help|help|"") usage ;;
    *) die "unknown command: $command" ;;
  esac
}

main "$@"
