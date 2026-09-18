# Vaani Build Phases

Each phase is a verifiable milestone with a concrete "done" signal — see each phase's
acceptance criteria.

## Phase 1 — Skeleton + ARI Wiring + RTP Loop-back (externalMedia)
Status: verified against a live Asterisk (real production server, not the
Docker-based `deploy/` stack). Bidirectional RTP confirmed clean via `tcpdump` on
the Asterisk box itself (correct 640-byte slin16 frames, 0 malformed, 0 seq gaps,
matching pacer cadence both ways). The one real issue found (audio arriving fine
at Asterisk but the caller hearing silence) was root-caused to Asterisk-side
`direct_media`/NAT handling on the caller's own SIP leg -- outside Vaani's code
entirely. `docker compose config` still validates clean for the from-scratch
Docker path, but hasn't been built/run end-to-end (no Docker daemon in the
environment this was built in). Endianness probe (`cmd/endianness-check`) has not
yet been run against a live stack -- see `docs/AUDIO_PIPELINE.md`.

## Phase 2 — Media Plane Hardening: Jitter Buffer, Handler Seam, Load Testing
Status: implemented and unit-tested (`go build`/`go vet`/`golangci-lint`/
`go test ./...` all clean), not yet run against a live stack. Done:
- Phase 1 carryover fixes (item 0): outbound packet counting only on real sends,
  `vaani_rtp_send_errors_total`, "remote locked" logging, WARN on
  never-locked teardown.
- Jitter buffer (`internal/media/jitterbuffer.go`): reorder, late/duplicate
  discard, silence-concealment, SSRC-reset, zero-alloc steady state.
- Byte-order normalization (`internal/media/byteorder.go`) driven by
  `AUDIO_L16_ENDIANNESS`.
- The `Handler` seam (`internal/media/handler.go`) with `LoopbackHandler`
  preserving Phase 1 behavior; pipeline restructured into independent
  release/write tickers so a slow Handler can't stall RTP pacing.
- Debug WAV recording (`internal/media/wav.go`), media watchdog
  (`internal/media/watchdog.go`), full Phase 2 metrics set, pprof mounted.
- Session lifecycle (`internal/session/call.go`): once-guaranteed teardown,
  making the duration-metric double/zero-count bug structurally impossible
  (proven under a 50-goroutine concurrent-teardown test).
- `cmd/loadgen` (control-plane load) and `deploy/sipp/` (media load) tooling.

**Not done: the actual load test run.** `docs/LOADTEST.md` is a template with no
fabricated numbers -- acceptance criteria 5-8 (25 concurrent SIPp calls, 128
control-plane calls, SIGTERM-under-load, real CPU/RSS numbers) need to be run
against a live Asterisk + SIPp environment and the results recorded there.

## Phase 3 — STT + LLM + TTS Integration
TODO: not yet scoped. Depends on the endianness finding from Phase 1.

## Phase 4 — Barge-in Orchestration
TODO: not yet scoped.
