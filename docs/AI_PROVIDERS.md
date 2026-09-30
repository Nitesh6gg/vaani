# AI Provider Specs

**Status: Phase 3 is implemented and running** (`APP_MODE=agent`). This document
is the design record the STT/LLM/TTS clients and the agent Handler were built
against; where it and the code disagree, the code and `.env.example` win.

Phase 1/2 (ARI wiring, RTP/AudioSocket media planes, jitter buffer, the Handler
seam) are done and documented in `docs/ARCHITECTURE.md` and
`docs/AUDIO_PIPELINE.md`. The three provider clients live under
`internal/ai/stt|llm|tts/` and are driven by the `agent` Handler, which
implements the same `media.Handler` interface `LoopbackHandler` does
(`ProcessFrame(ctx, callID, pcm) [][]byte`, one call per 20ms release tick --
see "The Handler Contract" in `docs/AUDIO_PIPELINE.md`).

## Where the models come from (Dograh)

Which LLM, STT and TTS a call uses -- provider, model, voice, language, API
keys -- is configured in Dograh, not `.env`, and read per call together with
the workflow (`internal/dograh/services.go`), the way Dograh's own pipeline
resolves it (`run_pipeline.py`, `configuration/resolve.py`):

1. Start from the **workflow owner's** model configuration
   (`user_configurations.configuration` for `workflows.user_id` -- the user
   Dograh uses for telephony calls).
2. Merge the workflow's **`model_overrides`** (definition's
   `workflow_configurations`) on top, per service: an override with a
   different provider replaces that service's settings entirely; with the
   same provider it changes only the fields it gives.
3. An `api_key` stored as a list: one key is picked at random per call, as
   Dograh does.

What Vaani can run (anything else hangs the call up, with the reason logged):

| Service | Supported | Notes |
|---|---|---|
| STT | `sarvam` | `model`, `language`; Dograh's `segmented` mode isn't supported (warning, streams instead) |
| TTS | `sarvam` | `model`, `voice`, `language` |
| LLM | `openai`, `google`, `groq`, `openrouter`, `sarvam`, `bifrost`, `speaches` | Any OpenAI-compatible endpoint: `openai` -> `https://api.openai.com/v1`, `groq` -> `https://api.groq.com/openai/v1`, `google` -> Google's OpenAI-compatible endpoint `https://generativelanguage.googleapis.com/v1beta/openai` (Dograh itself uses Google's native client; this path has not yet been tried live); the others use their stored `base_url`. Not supported: Azure, AWS Bedrock, Dograh's hosted models, realtime (speech-to-speech) mode. |

For a Sarvam field missing from a stored config, Dograh's field defaults
apply (STT `saaras:v3`/`hi-IN`, TTS `bulbul:v2`/`suhani`/`hi-IN`). Each call
logs what it resolved to, never the keys:
`agent models llm=<provider>/<model> llm_url=... stt=sarvam/<model> stt_language=... tts=sarvam/<model> tts_voice=... tts_language=...`.

## STT — Sarvam (e.g. `saaras:v4`)

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
- VAD signals: with `vad_signals=true` Sarvam also sends
  `{"type":"events","data":{"signal_type":"START_SPEECH"|"END_SPEECH",...}}`.
  They reach the Handler as `stt.Result.Signal` on the **same** channel as
  transcripts, because Sarvam sends START -> END -> transcript in order and
  a second channel would let Go's `select` reorder them. They drive the
  caller-silence clock and the latency fields below -- not barge-in, which
  stays local (START_SPEECH also fires on background conversation).
- If the STT connection can't be opened (after a few quick retries), the call
  currently falls back to `LoopbackHandler` (the caller hears their own
  voice) rather than being hung up -- logged as
  `agent: STT connect failed after retries, falling back to loopback`.

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
- Tool calling: the request carries OpenAI-style `tools`; streamed
  `delta.tool_calls` are assembled whether a provider sends them in fragments
  (OpenAI) or whole (Gemini). Text the model streams before a call is spoken
  right away, not held back. Gemini 3's thought signature
  (`extra_content`) is kept byte-for-byte and sent back with the tool
  result. That round trip is unit-tested only; it hasn't been confirmed on a
  live call with a Gemini model.
- On an LLM error the turn simply ends: whatever was already said stays
  said, nothing else is spoken, and the caller-silence clock starts. There is
  no retry or spoken fallback yet.

## TTS — Sarvam Bulbul (e.g. `bulbul:v3`)

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
| `BARGE_IN_ENABLED` | `1` (default) = interruption on. `0` = never cut, two flavors: with `VAD_MODE=ten` the VAD still runs in observe-only mode and keeps logging `speech started`/`speech ended`; with `VAD_MODE=energy` barge-in is fully off (no-op detector). |
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

## Turn latency in the logs

Per turn, all in milliseconds:

| Log line | Field | From -> to |
|---|---|---|
| `stt final transcript` | `endpoint_ms` | Sarvam's `END_SPEECH` -> its transcript |
| `llm first token` | `latency_ms` | transcript accepted -> first LLM token |
| `tts first audio` | `latency_ms` | transcript accepted -> first TTS audio received |
| `tts first audio` | `since_speech_end_ms` | Sarvam's `END_SPEECH` -> first TTS audio received: the caller's wait |
| `turn complete` | `total_latency_ms` | transcript accepted -> the reply has fully played |

`endpoint_ms` and `since_speech_end_ms` appear only on turns started by the
caller speaking, and only when Sarvam sent `END_SPEECH` for that utterance --
never borrowed from an earlier one (START_SPEECH clears it; the transcript
consumes it even if it's dropped). They use the time `END_SPEECH` *arrived*,
on this process's clock, not Sarvam's `occured_at` field: a sibling project
measured Sarvam's clock ~3.9s off (`D:\go-agent-worker` ADR-017). Two things
they leave out: `END_SPEECH` is Sarvam's *detection* time, which trails the
caller's actual last sound (by ~400ms in that project's one measurement), and
"first audio received" is when the first TTS chunk reached Vaani -- it goes
out on the next 20ms tick. So the caller's real wait is somewhat longer than
`since_speech_end_ms`.

## Open questions

- Per-call provider error handling: a failed LLM request currently ends the
  turn silently, and a failed STT connect falls back to loopback (see above)
  -- neither retries nor says a fallback line yet.
- Cost/latency tracing: whether provider call spans get their own metrics
  series (mirroring `vaani_audio_rms`-style transport-neutral naming) or reuse
  existing ones. Today latency is only in the logs above.
