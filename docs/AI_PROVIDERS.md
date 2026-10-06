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
- **Fed continuously, as in Dograh.** The caller's audio goes to Sarvam on
  every 20ms tick in every state -- listening, while the agent prepares or
  speaks a reply, and while a reply is paused (only a transfer's hold audio
  is excluded). Dograh works the same way: speech-to-text comes first in its
  pipeline (`pipeline_builder.py`), and its "mute" only discards transcripts
  afterwards. What to do with each transcript is decided when it arrives:

  | When the transcript arrives | What happens |
  |---|---|
  | Agent listening | A new turn |
  | Reply paused by a possible interruption | Confirms it -- unless junk or too soon (see "Pause first") |
  | Agent preparing or speaking a reply, not paused | Discarded and logged: `transcript ignored: the agent is speaking` (or `thinking`) |

  An answer that starts over the agent but ends after it finishes arrives
  while listening, and is transcribed whole. Until 2026-10-01 Vaani fed the
  STT only while listening, plus a 200ms buffer of the caller's audio sent
  when an interruption began; that clipped the start of answers given over
  the agent (live: "महंगाई" transcribed as "हाँ जी" and "नहीं", its first
  syllable missing). Two consequences to watch: if the agent's own voice
  echoes back down the caller's line, Sarvam transcribes that too (discarded
  while the agent speaks, but able to confirm a pause at an interruptible
  node -- Dograh has the same exposure); and Sarvam now receives the whole
  call's audio, whose effect on Sarvam's billing hasn't been checked.
- **Early finalization (`STT_FLUSH_AFTER_MS`, default 400; `0` turns it
  off; only with `VAD_MODE=ten`).** Sarvam decides by itself when the caller has stopped
  (`END_SPEECH`), which took ~600-700ms in live calls (`end_detect_ms`) --
  the largest part of the reply delay. Dograh doesn't wait for it: it opens
  the connection with `flush_signal=true` and, when its local Silero VAD
  hears 0.2s of silence, sends `{"type":"flush"}`, which the Sarvam SDK
  documents as "flush the audio buffer and force finalize partial
  transcriptions" (`patches/pipecat_sarvam_stt.py`,
  `sarvamai/types/stt_flush_signal.py`). With `STT_FLUSH_AFTER_MS` above 0
  and `VAD_MODE=ten`, Vaani does the same (without TEN VAD it can't tell
  when the caller stopped, so the connection isn't even opened with
  `flush_signal`): once Sarvam has
  reported the caller speaking (`START_SPEECH`) and TEN VAD has heard that
  much quiet after speech that belongs to this utterance, it sends one
  flush (`stt flush sent quiet_ms=...`). If TEN VAD didn't hear the
  utterance, nothing is sent and Sarvam decides as before.

  **Confirmed live at 400ms (2026-10-03 and 2026-10-05).** Dograh turns
  Sarvam's own `vad_signals` off when it uses flush; Vaani keeps them on
  (they drive silence handling and some latency fields), and Sarvam
  honours the flush anyway: it answers with its `END_SPEECH` signal within
  ~30-90ms of the flush (`since_flush_ms` minus `endpoint_ms`), then the
  transcript. So `endpoint_ms` still appears; the evidence is `END_SPEECH`
  arriving right after the flush -- `end_detect_ms` ~430-520 instead of
  ~580-720 before. Result: the reply starts ~0.2-0.3s sooner
  (`since_last_speech_ms` ~0.94-1.21s, from ~1.3-1.4s).

  **How Dograh compares.** Dograh always flushes, on every call (no
  setting), when its Silero VAD hears 0.2s of silence. But its turn-end
  rule (pipecat's `SpeechTimeoutUserTurnStopStrategy`, default
  `user_speech_timeout=0.6`) then waits 0.6s more in case the caller says
  more, gathering every transcript in that window into one turn, before
  starting the LLM -- ~0.8s after the caller's last sound. Vaani at 400ms
  starts it ~0.5s after, sooner than Dograh, but has no such window: a
  caller pausing mid-sentence for longer than the setting has the sentence
  split -- the first part becomes the turn and the rest is logged as
  `transcript ignored: the agent is thinking`. None seen in the live tests
  so far (including a 1.3s sentence with pauses in it).
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

### How the LLM's text becomes audio

1. **Splitting** (`internal/ai/agent/chunker.go`): the LLM's streamed text is
   cut into pieces as it arrives. A piece ends at a sentence end (`.`, `!`,
   `?`, `।`, `॥`, a new line; a `.` right after a digit doesn't count), or
   once it passes 80 characters -- then at the last word break: after a
   comma/semicolon/colon in the piece's second half if there is one, else
   after the last space. A word is never split; only a piece with no space
   in 160 characters is cut without one, and even then never before a vowel
   sign or after a virama. There is no minimum length, and a comma alone
   doesn't end a piece.
2. **Synthesis** (`internal/ai/tts/sarvam.go`): each piece is sent as soon as
   it's ready, as its own request on the call's one TTS connection (`text`
   then `flush`), so Sarvam speaks each piece **on its own**. That's why a
   word split across two pieces was heard broken in half (live, 2026-10-01:
   "बिजल" + "ी", fixed by rule 1). A piece spoken on its own (transition
   speech, a tool message) is sent with a trailing space so the recorded
   reply doesn't run it into the next sentence.
3. **Playback** (`internal/ai/agent/handler.go`): Sarvam streams audio back
   in its own chunk sizes; it's cut into 20ms frames (640 bytes) in a queue
   holding up to 30s, and playback starts with the first frame -- nothing
   waits for the whole reply. One frame goes out per 20ms tick. Sarvam
   delivers far faster than real time (`tts delivery complete` comes long
   before the reply has played), so the queue never runs dry mid-reply; the
   turn ends when the queue is empty and delivery is complete.

## Barge-in configuration

The caller-interrupts-agent switch and its tuning knobs. All are read at
startup -- changing any of them requires a restart.

| Env | Meaning |
|---|---|
| `BARGE_IN_ENABLED` | Master switch. `1` (default) = interruption on, **at workflow nodes whose `allow_interrupt` is on in Dograh** (see below). `0` = never cut, whatever the nodes say, two flavors: with `VAD_MODE=ten` the VAD still runs in observe-only mode and keeps logging `speech started`/`speech ended`; with `VAD_MODE=energy` barge-in is fully off (no-op detector). |
| `VAD_MODE` | `energy` (default): RMS-threshold detector, no native dependency. `ten`: TEN VAD, a real neural VAD that tells speech from coughs/music/line noise (native library vendored under `third_party/ten-vad`; Linux/cgo only -- elsewhere, or if the library can't load, calls fall back to `energy` with a loud warning). |
| `TEN_VAD_THRESHOLD` | TEN VAD speech-probability threshold in [0,1], default 0.7 -- the confidence Dograh's VAD runs with (TEN VAD's own example uses 0.5). Only used when `VAD_MODE=ten`. |
| `BARGE_IN_RMS_FLOOR` | Energy detector's RMS speech threshold. Only used when `VAD_MODE=energy`. |
| `BARGE_IN_GUARD_MS` | Ignores detection this long after playback starts, so the agent's own voice leaking into the mic can't immediately self-trigger. Default 300. |
| `BARGE_IN_MIN_SPEECH_MS` | How long the detector must report speech without a break before it counts as an interruption. Default 200, matching Dograh. Before this setting existed, about 50ms was enough (3 of 5 TEN VAD readings of 16ms each), and noise cut 6 of 7 replies in one live call. |
| `POST_CUT_SILENCE_MS` | After an interruption pauses the reply, how long transcripts are ignored (`transcript ignored: too soon after an interruption`): one arriving that soon was already on its way, since Sarvam needs ~0.7s after the caller stops just to send a transcript. Default 300. It was 1200, to drop the junk a cut used to produce 0.5-1.5s later (commit `249a6c4`), but on 2026-10-01 that threw away callers' real short answers ("बेनी", "ते भी नहीं"), so junk is now recognised by content instead (see "Pause first" below). |

**How Dograh does it, for comparison.** Dograh's live VAD is Silero with
pipecat's defaults: confidence 0.7, speech must last 0.2s to count as started
(that's what interrupts the bot), 0.2s of silence to count as stopped, and a
minimum volume of 0.6 (`run_pipeline.py`:
`SileroVADAnalyzer(params=VADParams(stop_secs=0.2))`; defaults in
`pipecat/audio/vad/vad_analyzer.py`). The VAD settings on a workflow's
Settings page (`vad_configuration`: confidence, start and stop seconds,
minimum volume) are stored, but in this version of Dograh
nothing applies them: the transport setup functions accept the value and
never use it. Vaani matches the live confidence (`TEN_VAD_THRESHOLD`) and
start time (`BARGE_IN_MIN_SPEECH_MS`). It has no equivalent of the minimum
volume, because pipecat's volume scale doesn't map directly onto Vaani's
audio. TEN VAD's and Silero's probabilities aren't directly comparable
either, so the same 0.7 may behave differently.

**Per node (Dograh's `allow_interrupt`).** Even with `BARGE_IN_ENABLED=1`, the
caller can only cut in while the agent speaks at a workflow node whose
`allow_interrupt` is on; a node without the field counts as off, Dograh's
default. At other nodes the caller's speech is ignored until the agent
finishes -- Dograh's `should_mute_user` (`pipecat_engine.py`). The node that
counts is the one the conversation is at when the caller speaks. One
difference from Dograh: Dograh also ignores the caller while its own queued
messages play (a tool's custom message, an edge's transition speech) at any
node; Vaani applies the current node's setting to those too. During a
transfer's hold audio there is no barge-in at all, in both. The logs show
which applies: `node transition ... allow_interrupt=true interrupt=on`
(`interrupt` is the effective result, `off` whenever `BARGE_IN_ENABLED=0`),
and `start_allow_interrupt`/`start_interrupt` on `agent workflow loaded` for
the start node.

**Once the call is ending, nothing interrupts it**, as in Dograh (whose
`end_call_with_reason` mutes the caller): at an End node -- whose editor has
no interruption setting, so an `allow_interrupt` stored on it is ignored and
the log shows `interrupt=off` -- after the `end_call` tool, and once Vaani is
ending the call itself (caller silent twice, maximum call length). The
goodbye always plays and the call always hangs up. A transcript that arrives
while a reply is paused but the call is ending is ignored
(`transcript ignored: the call is ending`) and the goodbye resumes.

**Pause first, then cut or resume.** While the agent is speaking, the active
detector logs `speech started` / `speech ended` transitions (one per
utterance boundary; per-hop verdicts are never logged). Sustained speech at
an interruptible node first **pauses** the reply (`agent barge-in detected;
pausing the reply`): nothing plays, the caller's audio goes to STT, and the
reply keeps generating and queueing. Then one of two things happens:

- **Confirmed:** a real transcript arrives (after the `POST_CUT_SILENCE_MS`
  gate). The reply is cut -- LLM stream and TTS connection cancelled, queued
  audio dropped, only the part the caller heard recorded -- and the
  transcript becomes the next turn (`agent barge-in confirmed; reply cut`).
  A **junk** transcript doesn't count (`transcript ignored: too short to be
  speech`): no digit and fewer than two letters/vowel signs, i.e. a lone
  letter like "ह", which is what noise transcribes to. The rule is
  deliberately narrow so one-word answers such as "जी", "ना", "हाँ" or "5"
  still confirm.
- **False alarm:** no transcript, and the caller has been quiet for 2s (by
  TEN VAD's last speech, Sarvam's `END_SPEECH`, and Sarvam not reporting
  them mid-utterance). The reply resumes **from the start of the sentence it
  was paused in** (`false interruption; resuming the reply`), not mid-word;
  nothing was cut, so nothing is re-generated. If the pause came right
  after a sentence finished, that sentence is repeated. The wait is capped
  at 10s, so a stuck signal can't leave the call silent.

Metrics: `vaani_agent_interruption_pauses_total` counts pauses,
`vaani_agent_false_interruptions_total` those that resumed, and
`vaani_agent_bargein_total` those confirmed and cut.

With `VAD_MODE=ten` the detector
also runs while the agent is listening (for the latency fields below), so
its `vad.speech_started`/`vad.speech_ended` lines (DEBUG level) appear
between replies too; that
listening-time verdict never interrupts anything. Turning `BARGE_IN_ENABLED`
to `0` stops all interruptions (no pause, no cut); with `VAD_MODE=energy` it
also silences the logging.

## Turn latency in the logs

Per turn, all in milliseconds. At `LOG_LEVEL=info` the `turn.user` and
`turn.agent` lines carry what matters; `debug` adds the per-step lines.

| Log line (event) | Level | Field | From -> to |
|---|---|---|---|
| `turn.user` | info | `end_detect_ms` | the caller's last sound (TEN VAD) -> Sarvam's `END_SPEECH`: how long Sarvam took to decide they'd stopped |
| `turn.user` | info | `endpoint_ms` | Sarvam's `END_SPEECH` -> its transcript |
| `turn.user` | info | `since_flush_ms` | the early-finalize flush -> this transcript (only with `STT_FLUSH_AFTER_MS`) |
| `turn.agent` | info | `llm_ttft_ms` | transcript accepted -> first LLM token |
| `turn.agent` | info | `tts_ttfa_ms` | transcript accepted -> first TTS audio received |
| `turn.agent` | info | `reply_latency_ms` | the caller's last sound (TEN VAD; else Sarvam's `END_SPEECH`) -> first TTS audio: the caller's wait. Its p50/p95 over the call are on `call.summary` |
| `tts.first_audio` | debug | `since_speech_end_ms`, `since_last_speech_ms` | the two sources of `reply_latency_ms`, separately |
| `agent.turn.completed` | debug | `total_latency_ms` | transcript accepted -> the reply has fully played |
| `stt.speech_started` | debug | `vad_last_speech_ago_ms` | Sarvam's `START_SPEECH` arriving, against the local VAD's last speech |
| `stt.speech_ended` | debug | `utterance_ms`, `end_detect_ms` | Sarvam's utterance length; the caller's last sound -> its `END_SPEECH` |
| `stt.flush_skipped` | debug | `vad_last_speech_before_onset_ms` | no early flush for this utterance because the local VAD didn't hear its onset (its last speech was this long before Sarvam's `START_SPEECH`) -- the usual cause of a slow reply with no `stt.flush` line |

**Sarvam-based fields** (`endpoint_ms`, `since_speech_end_ms`) appear only
on turns started by the caller speaking, and only when Sarvam sent
`END_SPEECH` for that utterance -- never borrowed from an earlier one
(START_SPEECH clears it; the transcript consumes it even if it's dropped).
They use the time `END_SPEECH` *arrived*, on this process's clock, not
Sarvam's `occured_at` field: a sibling project measured Sarvam's clock ~3.9s
off (`D:\go-agent-worker` ADR-017). `END_SPEECH` is Sarvam's *detection*
time, which trails the caller's actual last sound -- which is what the two
VAD-based fields measure.

**VAD-based fields** (`end_detect_ms`, `since_last_speech_ms`) need
`VAD_MODE=ten` with the native library loaded (with or without
`BARGE_IN_ENABLED`): TEN VAD then also runs
while the agent is listening, on the same 20ms frames that go to Sarvam, and
records when it last heard speech (any single 16ms hop). At `END_SPEECH` that
time is taken as the caller's last sound -- but only if it is later than the
previous `END_SPEECH` and later than when the agent last stopped speaking, so
a stale value or the agent's own echo is never used (correlating two
independently-timed signals after the fact broke twice in that sibling
project, ADR-016). Otherwise both fields are left out for that turn. "Last
sound" is when Vaani processed that audio, which trails the caller's mouth
by the network path (plus the jitter buffer for RTP), and "first audio
received" is when the first TTS chunk reached Vaani -- it goes out on the
next 20ms tick. So the caller's real wait is somewhat longer than
`since_last_speech_ms`, by roughly the network round trip.

## Open questions

- Per-call provider error handling: a failed LLM request currently ends the
  turn silently, and a failed STT connect falls back to loopback (see above)
  -- neither retries nor says a fallback line yet.
- Cost/latency tracing: whether provider call spans get their own metrics
  series (mirroring `vaani_audio_rms`-style transport-neutral naming) or reuse
  existing ones. Today latency is only in the logs above.
