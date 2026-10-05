# System Design

## Call flow (Phase 1/2: answer + loop-back, either transport)

1. A SIP caller dials extension `1001`, which is routed by
   `deploy/asterisk/extensions.conf` into `Stasis(vaani)`.
2. `internal/ari.Manager` (`handlers.go`) receives the `StasisStart`, answers the
   caller channel, allocates a port/socket from `internal/media.PortAllocator`,
   and creates an `externalMedia` channel pointed at `MEDIA_IP:<port>`. The
   transport is selectable via `MEDIA_ENCAPSULATION` (see `CLAUDE.md` invariant
   #2 and "AudioSocket Transport" in `docs/AUDIO_PIPELINE.md`):
   - `rtp` (default): `transport=udp`, `format=slin16`. The UDP socket is bound
     *before* Asterisk is told the address (invariant #9).
   - `audiosocket`: `transport=tcp`, Asterisk's res_audiosocket framed protocol.
     Requires a `data` UUID field the vendored ARI library has no field for, so
     `internal/ari/externalmedia.go` makes that one call as a raw REST request
     instead of through `Channel().ExternalMedia()`.
3. That externalMedia channel itself enters the same Stasis app with a second
   `StasisStart`; the Manager correlates it back to the caller via the channel ID
   it generated, creates a mixing bridge, and adds both channels to it.
4. `internal/media.CallMedia` (RTP, `loopback.go`) or `AudioSocketCallMedia`
   (AudioSocket, `audiosocket_call.go`) runs the media pipeline for the call's
   duration -- one reader goroutine, one writer goroutine, connected by buffered
   channels, each on its own 20ms `Pacer` tick so a slow `Handler` never stalls
   the outbound stream (see "Pipeline (RTP)" and "The Handler Contract" in
   `docs/AUDIO_PIPELINE.md`). RTP additionally passes inbound audio through a
   jitter buffer (reorder window + loss concealment + byte-order normalization,
   see "Jitter Buffer" in the same doc) before it reaches the `Handler`;
   AudioSocket needs neither (TCP already guarantees order, and its payload is
   little-endian by protocol definition). Phase 1/2's `Handler` is
   `LoopbackHandler`: echo the caller's own voice back unchanged. Phase 3 plugs
   an STT/LLM/TTS-backed `Handler` into the exact same seam -- see
   `docs/AI_PROVIDERS.md` -- with no transport-layer changes required.
5. `StasisEnd` on either channel tears the call down (`Manager.teardown`, guarded
   by `session.Call`'s once-guard so a double `StasisEnd` can't double-fire it):
   media plane stopped, bridge deleted, both channels hung up, socket/port freed.
   A dropped ARI WebSocket triggers the same teardown for every active/pending
   call on reconnect, since events during the gap (including the real
   `StasisEnd`) are unrecoverable.

## Process-wide singletons

- One ARI WebSocket connection for the whole process (`internal/ari.Connect`),
  reconnected with backoff at startup and observed for the life of the process
  via `ari.Client.Connected()` -- also what backs `/healthz` (see "Operability
  endpoints" below).
- One `PortAllocator` guarding the configured UDP range so ports are never
  double-assigned across concurrent calls.
- One `sync.Pool` of 640-byte frame buffers (`internal/media/pool.go`), shared by
  every call's reader/writer goroutines to keep the hot path allocation-free.
- In agent mode, one Postgres connection pool to Dograh's database
  (`internal/dograh.Store`, `DOGRAH_DB_URL`), opened and pinged at startup --
  an unreachable database stops the server from starting.

## Per-call state

Exactly two goroutines per call (reader, writer) connected by a capacity-5 buffered
channel -- never a goroutine per frame. See `CLAUDE.md` for the full list of
invariants this design exists to preserve.

## Agent mode (`APP_MODE=agent`, Phase 3/4)

Per call, `internal/ai/agent.Handler` implements the same `media.Handler` seam
and drives a four-state turn loop over the media frames:

```
Every state: inbound audio -> Sarvam STT (WebSocket, per call, fed continuously)
  final transcript (used while listening; see below) -> LLM (OpenAI-compatible SSE stream)
    -> sentence chunks -> Sarvam TTS (WebSocket, one connection per call)
Speaking: TTS PCM re-framed to 640B frames -> played out at 20ms/tick
  (one frame per ProcessFrame call; synthesis finishes long before the
  caller has heard it all, so delivery-complete and playout-complete are
  tracked separately)
```

Barge-in while speaking: a per-call VAD -- TEN VAD (`VAD_MODE=ten`, vendored
under `third_party/ten-vad/`, `internal/ai/agent/tenvad`) or an RMS energy
gate -- watches the inbound audio. At workflow nodes whose Dograh
`allow_interrupt` is on (elsewhere, and once the call is ending, the caller
is ignored until the agent finishes), unbroken speech for
`BARGE_IN_MIN_SPEECH_MS` first **pauses** the reply: nothing plays, while
the caller's audio keeps going to STT as always. A transcript (other than junk like a lone
letter) confirms the interruption and the turn is cut (LLM stream and TTS
connection cancelled, queue drained), with the caller's utterance as the
next turn; no transcript and 2s of quiet means it was noise, and the reply
resumes from the start of the sentence it was paused in. A transcript of
something said entirely while the agent spoke, with no interruption, is
discarded (`transcript ignored: the agent is speaking`) -- Dograh's mute; an
answer that started over the agent and ended after it is transcribed whole
(see "Fed continuously" in `docs/AI_PROVIDERS.md`).
`BARGE_IN_ENABLED=0` puts the detector in observe-only mode: transitions keep
logging, nothing is ever interrupted. Full knob table and the rules in
"Barge-in configuration" in `docs/AI_PROVIDERS.md`; state machine in
`internal/ai/agent/handler.go`.

History: the conversation the LLM sees records only what the caller actually
heard. A reply that plays out is recorded whole; one cut short (barge-in, dead
TTS connection) is recorded only up to the sentence that was playing. Tool
calls and their results are recorded ahead of the spoken reply.

Conversation in the logs: each accepted caller transcript is logged as
`[User] call_id=... text="..."`, and each agent reply -- exactly the part the
caller heard, greeting, transition speech and tool messages included -- as
`[Agent] call_id=... gen=N text="..." cut=false|true` once it has finished
playing or been cut, so the two read in conversation order. If the call ends
during a reply (usually the caller hanging up), what they heard of it is
logged with `cut=true` as the call closes.

### Configured in Dograh

Everything that defines *the agent* is read from a self-hosted Dograh's
Postgres (`internal/dograh`) -- the visual editor stays Dograh's; Vaani only
reads its tables, never writes. On every call, before the STT connection
opens, `Manager.loadWorkflow` (3s timeout) loads workflow `DOGRAH_WORKFLOW_ID`:

- **Which version:** the published definition (`workflows.released_definition_id`),
  else the legacy `is_current` row -- as Dograh picks it for a real call. Read
  fresh per call, so a publish in the editor applies to the next call.
- **The graph** (`workflow_json`): nodes, edges, prompts, the start node's
  greeting, each node's tools (see "Workflow walk" below).
- **Variables:** `{{...}}` in prompts, the greeting and edge transition
  speech are filled in once, at call start, following Dograh's
  `render_template`: `caller_number`/`called_number` from the call, the
  definition's `template_context_variables` (else the workflow's), and
  Dograh's built-ins `{{current_time}}`, `{{current_time_<Zone>}}`,
  `{{current_weekday}}`, `{{current_weekday_<Zone>}}`; dotted paths and
  `{{var | default}}` / `{{var | fallback:default}}` fallbacks work; a
  missing variable becomes empty text. (Dograh renders on entering each
  node, so `{{current_time}}` there can be minutes later on a long call.)
- **Tools:** the rows of `tools` referenced by the nodes' `tool_uuids`,
  limited to active ones in the workflow's organization (Dograh's own lookup),
  each with the `external_credentials` row its `credential_uuid` names (same
  organization, active).
- **Models and keys:** see "Where the models come from" in
  `docs/AI_PROVIDERS.md`.
- **Call limits:** `max_user_idle_timeout` and `max_call_duration` from the
  definition's `workflow_configurations` (see below).

If any of it can't be used -- workflow not found or unpublished, no start
node, the owner has no model configuration, a provider Vaani can't run --
the call is hung up (`agent: loading the dograh workflow failed; hanging up`
with the reason). There is deliberately no fallback to `.env` settings.
Problems that shouldn't stop a call -- a tool that's missing, archived, of a
category Vaani doesn't run yet (e.g. `calculator`) or misconfigured, an audio
greeting -- are logged as `agent: workflow: ...` warnings and skipped. An
`http_api` tool whose credential is missing or inactive is kept and called
without it, as Dograh does, with a warning.

### Workflow walk

`internal/ai/agent/workflow.go` (`Node`, `Edge`) mirrors Dograh's engine
(`pipecat_engine.py`, `pipecat_engine_context_composer.py`):

- **System prompt per node:** the global node's prompt + a blank line + the
  node's own prompt; a node with `add_global_prompt=false` gets only its own.
- **Functions per node:** the node's tools, then one function per outgoing
  edge -- named from the edge label the way Dograh does it (lowercase, every
  character outside `a-z0-9` becomes `_`: "Move to Main Agenda" ->
  `move_to_main_agenda`), described by the edge's condition, no parameters.
- **Taking an edge:** the edge's transition speech (if any) is spoken, the
  conversation moves to the target node (`node transition` log), the LLM gets
  Dograh's result `{"status":"done"}`, and runs again at once, in the same
  turn, with the new node's prompt and functions. History carries over. The
  move sticks even if the caller then interrupts.
- **Opening:** a start node with a greeting speaks it as-is, bypassing the
  LLM. Without one, the LLM speaks first from the start node's prompt; since
  nobody has spoken yet, that request repeats the prompt as the only user
  message, as pipecat's Google service does for a system-only context.
- **End node:** the reply generated there is the closing line; the call
  hangs up once it has played (`agent ending call reason=end_call`). It
  can't be interrupted -- Dograh mutes the caller once the call is ending,
  and its End Call node has no interruption setting -- so the hangup always
  happens.
- **Interruption:** each node's `allow_interrupt` (absent = off, Dograh's
  default; ignored on an End node) decides whether the caller can cut in
  while the agent speaks there -- see "Barge-in configuration" in
  `docs/AI_PROVIDERS.md`. Each
  `node transition` line shows the new node's setting and the effective
  result with the `BARGE_IN_ENABLED` master switch applied
  (`allow_interrupt=true interrupt=on`); `agent workflow loaded` shows the
  same for the start node (`start_allow_interrupt`, `start_interrupt`).
- Up to 5 LLM rounds per turn (`maxToolRounds`), so a model stuck calling
  functions can't keep the caller waiting indefinitely.

### Tools

Supported Dograh tool categories (`internal/dograh/tools.go`). Tool function
names follow Dograh's rule: lowercase, runs of characters outside `a-z0-9_`
become one `_`, trimmed ("Transfer Call to Support Team" ->
`transfer_call_to_support_team`). A tool's custom message (`messageType`
"custom"; for `http_api`, `customMessage` unless `customMessageType` is
"audio") is spoken before it acts. An unknown function name gets an error
result the LLM can react to.

- **`end_call`:** the LLM is not asked again, and the call hangs up once
  everything said so far has played; from the moment the tool runs the
  caller can't interrupt, as in Dograh. With `endCallReason` on, the LLM must
  pass a `reason` (described by `endCallReasonDescription`), which is logged
  (`end call requested`).
- **`transfer_call`:** dials `destination` (an Asterisk dial string, e.g.
  `PJSIP/<number>@<endpoint>` -- chan_sip's `SIP/...` is gone in Asterisk 21+)
  and rings it for `timeout` seconds (default 30). `internal/ari/transfer.go`
  mirrors Dograh's ARI transfer:
  1. The destination is originated into the same Stasis app with app args
     `transfer,<call id>`, so its `StasisStart` (sent only once it answers)
     is recognised as a transfer leg, not a new call.
  2. Meanwhile the caller hears the queued custom message, then the hold ring
     looping (`assets/transfer_hold_ring_8000.wav`); no STT, no barge-in.
  3. On answer, the beep (`assets/beep.wav`) plays to the caller (waited on
     for up to 2s), then the destination is added to the call's bridge, the
     agent's externalMedia leg is removed and hung up, and the agent stops.
     Teardown later ends both remaining legs together.
  4. Busy, rejected, not answered in time (our own wait is the timeout + 5s),
     or the caller hung up: the ring stops and the LLM gets
     `{"status":"failed","reason":...}` and carries on.
  Both sounds are 8kHz mono 16-bit WAVs embedded at build time and upsampled
  to 16kHz; if one can't be decoded the server logs a warning at startup and
  that sound is silence.
- **`http_api`** (e.g. web search; `internal/dograh/httptool.go`, ported
  from Dograh's `custom_tool.py` and `credential_auth.py`):
  - **Function parameters:** from the tool's `parameters` (`name`, `type`
    string/number/boolean/array/object, `description`, `required` -- default
    true); an array's items and an object's fields come from `item_schema`,
    one level deep.
  - **Request:** `method` (default POST) to `url` with the config's
    `headers`, over the shared HTTP client. POST/PUT/PATCH send the LLM's
    arguments as a JSON body (`{}` if none); GET/DELETE send them as query
    parameters, added to any already in the URL.
  - **Credential:** `bearer_token` -> `Authorization: Bearer <token>`;
    `api_key` -> `<header_name, default X-API-Key>: <api_key>`;
    `basic_auth` -> `Authorization: Basic <base64 user:password>`;
    `custom_header` -> `<header_name, default X-Custom>: <header_value>`.
  - **Timeout:** `timeout_ms`, default 5000. The agent's own 10s limit per
    tool call still applies on top.
  - **Result sent to the LLM** (Dograh's): any HTTP status is
    `{"status":"success","status_code":N,"data":<the JSON reply, else
    {"raw_response":"<text>"}>}`; a failure is `{"status":"error","error":
    "Request timed out after 5.0 seconds"}` or `"Request failed: ..."`.
    Only the first 1 MiB of a reply is read (Dograh reads it all).
  - Logs: `tool call` with the arguments, then `tool result` with the
    duration and result.

### Caller silence and call length

Both mirror Dograh, with its settings from the workflow's Settings page
(`workflow_configurations`); never saved, Dograh's backend defaults apply.

- **Silence** (`max_user_idle_timeout`, default 10s; 0 disables) -- pipecat's
  `UserIdleController`: the clock starts each time the agent finishes
  speaking and stops when the caller starts (Sarvam's `START_SPEECH`). If it
  runs out, the LLM gets Dograh's instruction to briefly ask whether they're
  still there; the second time in a row, Dograh's goodbye instruction, and
  the call hangs up after that reply (`caller silent`, then
  `agent ending call reason=caller_silent`). If speech is heard but never
  becomes a transcript (noise), the clock restarts at Sarvam's `END_SPEECH`.
- **Call length** (`max_call_duration`, default 300s; Vaani treats <= 0 as no
  limit): counted from the start of the call; hangs up at once if no reply is
  in progress, otherwise once the current reply has played
  (`reason=max_call_duration`). No goodbye is generated, as in Dograh.

This is separate from `MAX_CALL_DURATION_SECONDS` in `.env`: that one is a
transport-level backstop (off by default) that tears the call down the
moment it's reached, whatever is playing, and applies in every mode.

## Operability endpoints

All served by `internal/metrics.Serve` on `METRICS_ADDR` (default `:9091`), with
**no authentication of any kind** -- acceptable for a single-tenant deployment
behind a private network today, but a real gap before this is exposed anywhere
less trusted:

- `/metrics` -- Prometheus text format, always on.
- `/healthz` -- JSON `{status, ari_connected, active_calls, uptime_seconds}`;
  200 while the ARI WebSocket is connected, 503 otherwise (e.g. for a container
  orchestrator's readiness probe).
- `/debug/pprof/*` -- Go's standard profiling endpoints, always on.
- `/debug/audio/{callID}` -- live raw-PCM tap for one call, **only mounted at
  all** when `DEBUG_AUDIO=1` (off by default) -- see "Live Audio Tap" in
  `docs/AUDIO_PIPELINE.md`.

## Deferred (post Phase 3/4)

Dockerfile/go.mod toolchain fixes, GitHub Actions CI, docker-compose changes,
and load testing (`docs/LOADTEST.md`) are intentionally out of scope until after
Phase 3 (STT/LLM/TTS) and Phase 4 (barge-in) land -- adding them earlier would
mean redoing them once the agent Handler changes what "working" looks like.
