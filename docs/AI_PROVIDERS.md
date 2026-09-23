# AI Provider Specs

**Status: Phase 3 design placeholder. Nothing in this document is implemented yet.**
`APP_MODE=agent` (the config value that would select the Handler described here)
is rejected at startup with "not implemented yet" -- see `internal/config/config.go`
and `cmd/server/main.go`. This file exists so the intended shape is written down
before implementation starts, and so `README.md`/`CLAUDE.md`/`AGENTS.md`'s Docs
Index links to something real instead of a 404.

Phase 1/2 (ARI wiring, RTP/AudioSocket media planes, jitter buffer, the Handler
seam) are done and documented in `docs/ARCHITECTURE.md` and
`docs/AUDIO_PIPELINE.md`. Everything below is the plan for the three provider
clients that will live under `internal/ai/stt|llm|tts/` and drive an `agent`
Handler implementing the same `media.Handler` interface `LoopbackHandler` does
today (`ProcessFrame(ctx, callID, pcm) [][]byte`, one call per 20ms release
tick -- see "The Handler Contract" in `docs/AUDIO_PIPELINE.md`).

## STT — Sarvam `saaras:v4`

- Transport: WebSocket, streaming.
- Input format: 16kHz PCM s16le -- exactly what `ProcessFrame` already receives
  (post jitter-buffer, post byte-order normalization), so the agent Handler can
  forward `pcm` straight into the STT stream with no reformatting.
- One STT connection per active call, opened when the call's media plane starts
  and closed on teardown -- not a shared/pooled connection, since STT state
  (partial transcript, language context) is inherently per-call.
- Barge-in: STT cancel is one of invariant #7's five required cuts (see
  `CLAUDE.md`) -- when the caller interrupts TTS playback, the in-flight STT
  request for the *previous* turn must be cancelled, not just ignored, or it
  keeps consuming quota/billing after its answer is no longer wanted.

## LLM — OpenAI-compatible streaming API

- Transport: HTTP, streaming (SSE-style chunked response), over the shared
  `http.Client` invariant (`MaxIdleConnsPerHost=200`, `ForceAttemptHTTP2=true`,
  never a per-request client -- invariant #5 in `CLAUDE.md`).
- Consumes STT's finalized transcript per turn, streams tokens back as they
  arrive so TTS can start speaking before the full response is generated
  (first-token latency matters more than total latency for a voice UI).
- Barge-in: LLM cancel (invariant #7) means cancelling the in-flight HTTP
  request's context the instant the caller starts talking over the agent, not
  waiting for the response to finish and discarding it client-side -- the
  latter still burns provider billing for tokens nobody will hear.

## TTS — Sarvam Bulbul v3

- Transport: WebSocket, streaming, requesting 16kHz output -- matching the
  pipeline's slin16 format end to end (invariant #8) so no resampling step is
  needed before frames go into the outbound queue.
- Streamed audio arrives in provider-defined chunks, not necessarily aligned to
  20ms; the agent Handler is responsible for re-chunking into the outbound
  `[][]byte` frames `ProcessFrame` returns, same as `LoopbackHandler` does
  trivially today by returning its input frame unchanged.
- Barge-in: TTS stream close + queue flush (invariant #7's remaining two cuts)
  -- closing the WebSocket alone isn't enough if frames already queued for the
  writer would still play out after the caller starts talking; the outbound
  queue must be drained too.

## Open questions for implementation time

- Exact turn-taking state machine (when does STT's "final" transcript trigger
  the LLM call vs. treating a pause as still-speaking) -- belongs in
  `internal/session`, not any one provider client.
- Per-call STT/LLM/TTS error handling: does a provider outage during a live
  call fail the call, fall back to a canned message, or retry -- needs a
  decision before `agent` Handler ships, not deferred to code review.
- Cost/latency tracing: whether provider call spans get their own metrics
  series (mirroring `vaani_audio_rms`-style transport-neutral naming) or reuse
  existing ones.
