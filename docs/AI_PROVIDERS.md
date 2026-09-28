# AI Provider Specs

**Status: Phase 3 is implemented and running** (`APP_MODE=agent`). This document
is the design record the STT/LLM/TTS clients and the agent Handler were built
against; where it and the code disagree, the code and `.env.example` win.

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

## Barge-in configuration

The caller-interrupts-agent switch and its tuning knobs. All are read at
startup -- changing any of them requires a restart.

| Env | Meaning |
|---|---|
| `BARGE_IN_ENABLED` | `1` (default) = interruption on. `0` = off: the agent never gets interrupted and talks over any inbound audio until its turn finishes (a no-op detector is wired in; all VAD logging disappears with it). |
| `VAD_MODE` | `energy` (default): RMS-threshold detector, no native dependency. `ten`: TEN VAD, a real neural VAD that tells speech from coughs/music/line noise (native library vendored under `third_party/ten-vad`; Linux/cgo only -- elsewhere, or if the library can't load, calls fall back to `energy` with a loud warning). |
| `TEN_VAD_THRESHOLD` | TEN VAD speech-probability threshold in [0,1], default 0.5. Only used when `VAD_MODE=ten`. |
| `BARGE_IN_RMS_FLOOR` | Energy detector's RMS speech threshold. Only used when `VAD_MODE=energy`. |
| `BARGE_IN_GUARD_MS` | Ignores detection this long after playback starts, so the agent's own voice leaking into the mic can't immediately self-trigger. Default 300. |
| `POST_CUT_SILENCE_MS` | After a cut, how long before a new turn is accepted: the cut flushes a junk partial-utterance transcript to STT ~0.5-1.5s later, and this gate drops exactly those while still catching the caller's real next utterance. Default 1200. |

While the agent is speaking, the active detector logs `speech started` /
`speech ended` transitions (one per utterance boundary; per-hop verdicts are
never logged), and a sustained interrupt fires `agent barge-in detected`,
which cuts playback, cancels the in-flight LLM stream and TTS connection, and
flushes queued audio -- after which the caller's utterance becomes the next
turn. Turning `BARGE_IN_ENABLED` to `0` silences all of it for a quiet test
run.

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
