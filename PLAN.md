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

## Phase 5 — Dograh-driven agent
Goal: a Dograh-like voice agent whose configuration lives in a self-hosted
Dograh (its visual editor stays a separate app); Vaani reads Dograh's
Postgres directly. Design in `docs/ARCHITECTURE.md` ("Configured in Dograh")
and `docs/AI_PROVIDERS.md` ("Where the models come from").

Status, item by item — "live" means seen working on a real call:
- **History records the agent's own replies**, only what the caller heard —
  done, live.
- **Tool calling** in the LLM client (OpenAI-style, fragmented or whole
  calls, Gemini thought signature kept) and the agent turn loop — done, live
  through workflow edges. The Gemini thought-signature round trip is
  unit-tested only.
- **Workflow engine** — published definition, global + node prompts, edge
  transitions, LLM opening, end node + hangup — done, live (workflow 19,
  `sarvam-v2/gemma4` via Bifrost). `{{variables}}` (incl. Dograh's date/time
  built-ins) — done, unit-tested; workflow 19 uses none, so not yet seen live.
- **Models and keys from Dograh** (owner's configuration + workflow
  overrides) — done, live with Bifrost LLM + Sarvam STT/TTS. The `google`
  LLM provider path has not been tried live.
- **`end_call` tool** — done, unit-tested; not yet seen live (calls so far
  ended through the End Call node instead).
- **`transfer_call` tool** over ARI with hold ring + beep — done,
  unit-tested; **not yet tried live**.
- **Caller silence handling and call length** from the workflow's settings —
  done, unit-tested; not yet triggered live.
- **Turn latency from Sarvam's end of speech** (`endpoint_ms`,
  `since_speech_end_ms`) — done, live (2026-09-30 and 2026-10-01 calls).
- **Caller's last sound timed by TEN VAD while listening** (`end_detect_ms`,
  `since_last_speech_ms`) — done, live (2026-10-01: Sarvam's end-of-speech
  wait ~600-680ms). On a noisy line the "last sound" can be noise, so the
  figures are unreliable there.
- **Conversation lines** in the logs (`[User]`, `[Agent]`) — done, live
  (2026-10-01).
- **Per-node interruption** (Dograh's `allow_interrupt`, under the
  `BARGE_IN_ENABLED` master switch), in three rounds, all on 2026-10-01:
  1. First live call: far too sensitive (about 50ms of sound cut a reply;
     noise cut 6 of 7 replies), a false cut left 13s of dead air, and an
     interrupted goodbye cancelled the hangup.
  2. Fixed, then live: `BARGE_IN_MIN_SPEECH_MS` (default 200) and
     `TEN_VAD_THRESHOLD` default 0.7, matching Dograh's live VAD; an
     interruption pauses the reply first (a transcript confirms the cut,
     otherwise it resumes after 2s of quiet); an End node, the `end_call`
     tool and a call Vaani is ending can't be interrupted, as in Dograh.
     Result: no more noise cuts, both pauses resumed, and the goodbye was
     no longer interruptible.
  3. That call showed four more problems -- fixed, unit-tested, **not yet
     tried live**:
     - the 1.2s post-pause gate threw away real short answers -- now
       300ms, plus junk recognised by content (a lone letter);
     - text was split into TTS requests at exactly 80 characters, mid-word
       ("बिजल" + "ी"), heard as a broken word -- now cut only between words;
     - a resumed reply restarted mid-word -- now from the start of the
       interrupted sentence;
     - the reply playing when the caller hangs up wasn't logged, and there
       were no pause / false-interruption counters -- both added.
  4. Tried live (13:11, 2026-10-01): no broken words, and all three pauses
     were real and confirmed. But "महंगाई" was transcribed as "हाँ जी" /
     "नहीं" three times, though the recording had it clearly: the STT was
     fed only while listening, plus a 200ms buffer at an interruption,
     which a 300ms speech trigger outran -- the first syllable never
     reached Sarvam. Now the STT is fed continuously, as in Dograh, and a
     transcript said entirely over the agent is discarded and logged.
  5. Tried live (16:55 and 16:58, 2026-10-01): "महंगाई" said over the agent
     came through whole; remarks said over a non-interruptible reply were
     ignored as intended. Remaining issues are short answers misheard or
     misread ("आप" for AAP as "हाँ" / "you") and two prompt issues on the
     Dograh side. Fixed in Vaani: a missing space between two LLM rounds
     in the recorded reply ("नमस्ते।Thank").
- **Early STT finalization** (`STT_FLUSH_AFTER_MS`, default 400 since
  2026-10-05, `0` = off; needs `VAD_MODE=ten`): flush
  Sarvam on TEN VAD's end of speech instead of waiting ~600-700ms for
  Sarvam's own, as Dograh does — **confirmed live at 400ms** (2026-10-03,
  2026-10-05): Sarvam honours the flush with `vad_signals` on, answering
  with `END_SPEECH` ~30-90ms later; `end_detect_ms` ~430-520 (was
  ~580-720), replies ~0.2-0.3s sooner (`since_last_speech_ms` ~0.94-1.21s),
  no split sentences seen. Unlike Dograh (which then waits 0.6s to gather
  more speech into the turn), Vaani has no gathering window, so a long
  mid-sentence pause can split a sentence.

- **`http_api` custom tools** (Step 3b part 1; e.g. web search, credentials
  from `external_credentials`) — done, unit-tested against a local HTTP
  server; **not yet tried live**.

- **Calls in Dograh's call history** (Step 3b part 2): each call written to
  `workflow_runs` as Dograh writes its own (events for the run page,
  disposition, duration), with the mixed recording and the transcript
  uploaded to Dograh's MinIO — done, unit-tested (the MinIO signature
  against Dograh's own MinIO client); **not yet tried live**, and the SQL
  hasn't run against a real Dograh database yet.

- **Variable extraction** (Step 3b part 3; survey answers): a node's
  `extraction_variables` are pulled from the conversation by the call's own
  LLM, on leaving the node and again for the ending node at call end, with
  Dograh's exact prompts and JSON-parsing fallbacks, merged into the run's
  `gathered_context` — done, unit-tested (prompt text, tool-response
  formatting, JSON-reply parsing all checked against Dograh's own code);
  **not yet tried live**.

**Step 3b is done.**

- **Robustness pass** (2026-10-06, after an external code review, each
  finding verified against the code first): fixes for calls that could get
  stuck (a dropped barge-in signal leaving a reply paused, a dead TTS
  connection reused for the rest of the call, an LLM stream with no
  deadline), leaks (the STT connection never closed at call end; a transfer
  race leaving the answered destination up), scaling (AudioSocket call
  setup serialized behind the manager's lock), and Dograh run integrity
  (completion written before uploads, each step its own budget, shutdown
  writes the completion); log timestamps now UTC with milliseconds. See
  "Failure handling" in `docs/ARCHITECTURE.md` — unit-tested; **not yet
  tried live**, and `go test -race ./...` must be run on Linux (this
  Windows toolchain can't).

Not done yet:
- **Step 4:** knowledge base (Dograh's documents, pgvector).
- Dograh features Vaani skips for now: muting the caller during queued
  messages (tool messages, transition speech) regardless of the node,
  audio greetings and recordings, `delayed_start`, voicemail detection,
  pre-call fetch, context summarization, realtime (speech-to-speech) models.
