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
Status: **done and running in production** (`APP_MODE=agent`). What shipped:
- `internal/ai/stt|llm|tts`: Sarvam `saaras:v4` STT (WebSocket, per-call),
  OpenAI-compatible streaming LLM (SSE over the shared HTTP/2 client), Sarvam
  Bulbul v3 TTS (WebSocket; one connection per call reused across turns,
  cancelled and reopened on barge-in).
- `internal/ai/agent`: per-call Handler state machine
  (Listening/Transcribing/Thinking/Speaking), sentence chunking feeding TTS,
  a 30s playout queue drained at exactly real-time pace (synthesis finishes
  far earlier than the caller hears it — delivery and playout are tracked
  separately), post-cut silence gate, drain-timeout dead-call safeguard.
- Diagnostics: rejected TTS text logging, provider-initiated close logging,
  per-call `speech started`/`speech ended` transitions.

## Phase 4 — Barge-in Orchestration
Status: **done**. Barge-in runs behind the `BargeInDetector` seam
(`internal/ai/agent/bargein.go`): `VAD_MODE=energy` (RMS threshold, default)
or `VAD_MODE=ten` (TEN Framework's TEN VAD neural model, vendored under
`third_party/ten-vad`, Linux/cgo). Cuts implemented: LLM context cancel + TTS
connection teardown + outbound queue drain + playback state flip, with the
write pacer continuing to emit silence per the invariant #1/#7 resolution in
`AGENTS.md`. `BARGE_IN_ENABLED=0` puts the detector in observe-only mode
(VAD logs keep flowing, nothing is ever cut). Still open: acoustic echo
suppression — a caller on speakerphone leaks the agent's own voice into the
mic and can false-trigger interrupts; no code-side fix yet.

**Not done: the load test run** (Phase 2 carry-over) — `docs/LOADTEST.md` is
still a template awaiting real numbers.
