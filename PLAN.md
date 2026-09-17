# Vaani Build Phases

Each phase is a verifiable milestone with a concrete "done" signal — see each phase's
acceptance criteria.

## Phase 1 — Skeleton + ARI Wiring + RTP Loop-back (externalMedia)
Status: implemented, not yet verified against a live Asterisk (no Docker daemon
available in the environment this was built in -- `docker compose config` validates
clean, `go build`/`go vet`/`go test ./...` all pass, but acceptance criteria that
require an actual call -- dialing 1001, the 60s packet-count check, the
docker-restart reconnect test -- still need to be run against `make up` once
Docker is available).
Goal: dial extension 1001, hear your own voice echoed back over ARI externalMedia
(UDP/RTP, slin16), with metrics, tests, and an empirically-verified RTP payload
endianness documented in `docs/AUDIO_PIPELINE.md`.

## Phase 2 — Jitter Buffer + Session State Machine
TODO: not yet scoped.

## Phase 3 — STT + LLM + TTS Integration
TODO: not yet scoped. Depends on the endianness finding from Phase 1.

## Phase 4 — Barge-in Orchestration
TODO: not yet scoped.
