SHELL := /bin/sh

CARGO ?= cargo
MAKE ?= make

RUST_LOG ?= info
HOST ?= local.bitneedle.com
STREAM_ID ?= 1
PART_MS ?= 50

TLS_DIR ?= ../tls/$(HOST)
CERT ?= $(TLS_DIR)/fullchain.pem
KEY ?= $(TLS_DIR)/privkey.pem

CONTRIB_HTTP_PORT ?= 19443
MESH_FEC_TARGET ?= 127.0.0.1:22001
MESH_MEDIA_FEC_TARGET ?= 127.0.0.1:22101
RIST_BIND ?= 127.0.0.1:27000
RIST_FLOW_ID ?= 0x11223344
SRT_BIND ?= 127.0.0.1:27001
RTMP_BIND ?= 127.0.0.1:19350

STACK_ARGS ?=
SERVICE_ARGS ?=

.DEFAULT_GOAL := help

.PHONY: help stack stack-debug stack-fast service build build-release fmt test \
	dashboard-build dashboard-serve dashboard-check

help:
	@printf '%s\n' 'av-contrib tasks'
	@printf '%s\n' ''
	@printf '%s\n' '  make stack             Run release local OBS stack: 2 mesh nodes + contrib + dashboard'
	@printf '%s\n' '  make stack-debug       Run debug local OBS stack'
	@printf '%s\n' '  make stack-fast        Run existing release binaries and existing dashboard dist'
	@printf '%s\n' '  make service           Run only av-contrib against the local UK mesh sockets'
	@printf '%s\n' '  make dashboard-build   Build the av-mesh Leptos dashboard'
	@printf '%s\n' '  make dashboard-serve   Serve the av-mesh Leptos dashboard with Trunk'
	@printf '%s\n' '  make build-release     Build av-contrib release binaries'
	@printf '%s\n' '  make test              Run cargo test --locked'
	@printf '%s\n' ''
	@printf '%s\n' 'Common overrides: STREAM_ID=1 PART_MS=50 RUST_LOG=info HOST=local.bitneedle.com'

stack:
	AV_LL_HLS_PART_MS=$(PART_MS) RUST_LOG=$(RUST_LOG) \
	$(CARGO) run --bin local-obs-stack --release -- \
		--host $(HOST) \
		--stream-id $(STREAM_ID) \
		--part-ms $(PART_MS) \
		$(STACK_ARGS)

stack-debug:
	AV_LL_HLS_PART_MS=$(PART_MS) RUST_LOG=$(RUST_LOG) \
	$(CARGO) run --bin local-obs-stack -- \
		--host $(HOST) \
		--stream-id $(STREAM_ID) \
		--part-ms $(PART_MS) \
		$(STACK_ARGS)

stack-fast:
	AV_LL_HLS_PART_MS=$(PART_MS) RUST_LOG=$(RUST_LOG) \
	$(CARGO) run --bin local-obs-stack --release -- \
		--host $(HOST) \
		--stream-id $(STREAM_ID) \
		--part-ms $(PART_MS) \
		--no-build \
		--no-dashboard-build \
		$(STACK_ARGS)

service:
	AV_LL_HLS_PART_MS=$(PART_MS) RUST_LOG=$(RUST_LOG) \
	$(CARGO) run --bin av-contrib -- \
		--cert $(CERT) \
		--key $(KEY) \
		--http-port $(CONTRIB_HTTP_PORT) \
		--mesh-fec-target $(MESH_FEC_TARGET) \
		--mesh-media-fec-target $(MESH_MEDIA_FEC_TARGET) \
		--stream-id $(STREAM_ID) \
		--rist-stream-id $(STREAM_ID) \
		--srt-stream-id $(STREAM_ID) \
		--rtmp-stream-id $(STREAM_ID) \
		--fmp4-part-ms $(PART_MS) \
		--rist-bind $(RIST_BIND) \
		--rist-flow-id $(RIST_FLOW_ID) \
		--srt-bind $(SRT_BIND) \
		--rtmp-bind $(RTMP_BIND) \
		$(SERVICE_ARGS)

build:
	$(CARGO) build --locked

build-release:
	$(CARGO) build --locked --release

fmt:
	$(CARGO) fmt

test:
	$(CARGO) test --locked

dashboard-build:
	$(MAKE) -C ../av-mesh dashboard-build

dashboard-serve:
	$(MAKE) -C ../av-mesh dashboard-serve

dashboard-check:
	$(MAKE) -C ../av-mesh dashboard-check
