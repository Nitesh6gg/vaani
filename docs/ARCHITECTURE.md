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
letter, or only an acknowledgement like "जी", "हाँ", "ok" in any of the
Indian languages Sarvam transcribes -- that resumes the reply, and becomes
the caller's turn when the reply ends only if said during its last sentence)
confirms the
interruption and the turn is cut (LLM stream and TTS
connection cancelled, queue drained), with the caller's utterance as the
next turn; no transcript and 2s of quiet means it was noise, and the reply
resumes from the start of the sentence it was paused in. A transcript of
something said entirely while the agent spoke, with no interruption, is
discarded (`transcript ignored: the agent is speaking`) -- Dograh's mute --
except at a node that allows interruption, where it's held and becomes the
caller's turn if said during the reply's last sentence; an
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

Conversation in the logs: each accepted caller transcript is one
`event=turn.user` line (`turn`, `node`, `text`, and the STT timings
`end_detect_ms`/`endpoint_ms`/`since_flush_ms`), and each agent reply --
exactly the part the caller heard, greeting, transition speech and tool
messages included -- one `event=turn.agent` line (`turn`, `node`, `text`,
`cut`, and `llm_ttft_ms`, `tts_ttfa_ms`, `reply_latency_ms`) once it has
finished playing or been cut, so the two read in conversation order. If the
call ends during a reply (usually the caller hanging up), what they heard of
it is logged with `cut=true` as the call closes. Each call ends with one
`event=call.summary` line: duration, turns, interruptions, reply latency
p50/p95, errors by stage, disposition and nodes. Every line about a call
carries `call_id`, `trace_id` and (once Dograh's run exists) `run_id`.
`LOG_PII=0` (the default outside development) masks phone numbers and
replaces text with its length.

### Configured in Dograh

Everything that defines *the agent* is read from a self-hosted Dograh's
Postgres (`internal/dograh`) -- the visual editor stays Dograh's. The only
writes are each call's own record (see "Call history in Dograh" below). On
every call, before the STT connection
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
  The time-zone database is built into the binary (`time/tzdata`, about
  450 KB), so `<Zone>` variables work even on a host or slim image without
  `/usr/share/zoneinfo` -- without it they silently rendered empty there.
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

### Call history in Dograh

Every agent call is recorded in Dograh's `workflow_runs`, the way Dograh
records its own ARI calls, so it appears in Dograh's call list and run page
(`Manager.recordRun`, `internal/dograh/run.go`). Writing it never affects the
call: failures are logged (`dograh run: ...`) and the call carries on.

- **At call start** (once the workflow has loaded; in the background): a row
  as Dograh's `ari_manager.py` creates it -- name `ARI Inbound <caller>`,
  mode `ari`, call type `inbound`, the published definition, initial context
  `{caller_number, called_number, direction: inbound, provider: ari}`,
  gathered context `{call_id}` -- with state `running`. Log:
  `dograh run created run_id=...`.
- **During the call**, `agent.CallLog` collects the events Dograh's run page
  shows (`realtime_feedback_events`, shaped as Dograh's logs buffer stores
  them -- timestamp, turn, node):
  - `rtf-node-transition` at the start node and on every edge taken;
  - `rtf-user-transcription` for each caller turn (the `turn.user` line; each
    one starts a new turn);
  - `rtf-bot-text` for each reply as heard (the `turn.agent` line -- greeting,
    transition speech and tool messages included, as one entry);
  - `rtf-function-call-start` / `-end` for every function the LLM calls,
    tools and edges alike (Dograh registers both as functions).

  **Times, as Dograh's:** each event's own `timestamp` is when it was logged
  (UTC, microseconds); a caller or agent line also carries a
  `payload.timestamp` (UTC, milliseconds) -- the time its transcript line
  shows -- which is when that turn **started**, not when it was logged:
  pipecat's `UserTurnStoppedMessage` / `AssistantTurnStoppedMessage`
  timestamps, "when the turn started". For a caller line, when the STT
  signalled the utterance began (`sttUttStart`; "now" if it never did); for
  an agent line, when the LLM response that first produced its spoken text
  began (pipecat's `LLMFullResponseStartFrame`; a greeting, its turn's
  start). Seen on Dograh's own run 484: a caller line stamped 4.7 s and an
  agent line 9.7 s before they were logged. Vaani used to stamp both at the
  end, so its lines sat 1-10 s late on Dograh's run page.

  **Order, as Dograh's** (`InMemoryLogsBuffer._sorted_events`): the events
  are stored, and the transcript built, sorted by `payload.timestamp` (else
  the event's own), stably -- so a reply cut by the caller sits before the
  caller's words, and a goodbye said in the same LLM response as its
  `end_call` sits before that function call. Compared as times at the
  millisecond, not as strings as Python does: within one millisecond events
  keep the order they were logged in. One difference remains: Dograh writes
  one agent line per LLM response, Vaani one per turn (text before and after
  a tool call is one line, timed by the first).
- **When the agent leaves the call** (hangup, or a transfer handing the
  caller over), as Dograh's `on_pipeline_finished` and its completion job:
  - **How it ended** (`call_disposition`), whichever of these happened
    first: `user_qualified` (End node reached), `end_call_tool`,
    `transfer_call`, `user_idle_max_duration_exceeded` (caller silent
    twice), `call_duration_exceeded`; if none did, `user_hangup`.
    `mapped_call_disposition` is that, mapped through the organization's
    `DISPOSITION_CODE_MAPPING` if it has one, and is added to the workflow's
    `call_disposition_codes` (for the call list's filter).
  - Gathered context, merged into the row's: `nodes_visited`, the extracted
    variables (see below) at the top level and under `extracted_variables`,
    `call_disposition` (an extracted `call_disposition` wins over the reason
    above, as `extracted_call_disposition`), `mapped_call_disposition`,
    `call_tags` (the disposition, `user_speech` if the caller said anything,
    then every extracted `tag_*` variable's value).
  - `call_duration_seconds` in `usage_info` and `cost_info` (where the call
    list reads the duration), whole seconds from when the agent started.
  - `logs.realtime_feedback_events`, `is_completed`, state `completed`.
  - **Then** the recording and transcript, uploaded to Dograh's MinIO
    (`MINIO_*`, `internal/dograh/storage.go`, S3 Signature V4 as the MinIO
    client signs) at Dograh's paths, `recordings/<run id>.wav` and
    `transcripts/<run id>.txt`, and attached to the run (`recording_url`,
    `transcript_url`, `storage_backend` `minio`) in a second update. The
    recording is one mono 16kHz track, caller and agent mixed
    (`media.MixRecorder`, around the agent's handler -- as Dograh's is
    mono), capped at 100MB (~54 minutes; past it the rest is missing,
    logged and counted in `vaani_recording_truncated_total`); the
    transcript is Dograh's text format, `[timestamp] User: ...` /
    `[timestamp] Agent: ...`, one line per event above, in that order, with
    the turn's start time (`payload.timestamp`).
  - Log: `dograh run completed run_id=... disposition=... variables=N`.
- **Then QA Analysis**, if the workflow has any QA nodes (`internal/dograh/
  qa.go`): Dograh's own calls trigger this from a background job on
  completion (`run_integrations_post_workflow_run`) that Vaani's calls, written
  straight to the database, never reach -- so `callagent.Builder.recordRun`
  calls `Store.RunQA` itself, right after the run above completes (skipped if
  Vaani is already shutting down; bounded to 5 minutes regardless, so a stuck
  LLM provider can't hold up the shutdown drain any further than necessary).
  A from-scratch Go port of `api/services/workflow/qa/*.py`, read against
  Dograh's source rather than guessed at:
  - **Per QA node**, unless disabled (`qa_enabled`) or skipped (too short --
    `qa_min_call_duration`, default 15s; voicemail with `qa_voicemail_calls`
    off; excluded by `qa_sample_rate`'s random sampling) -- logged either way,
    `qa.completed`/`qa.skipped`/`qa.failed`.
  - The call's events, split by node (falling back to one whole-call review
    if an event ever lacks a node id -- doesn't happen for Vaani: every event
    is tagged from the first, see `CallLog.NodeEntered`), each segment
    reviewed by an LLM against the node's `qa_system_prompt`, filled in with
    `{{node_summary}}` (a cached one-time summary of that node's script,
    generated and persisted to `workflow_definitions.workflow_json.
    node_summaries` the first time any call needs it -- Dograh regenerates
    per QA node if more than one shares a missing summary; Vaani does it once
    per call instead, a deliberate, behavior-preserving simplification),
    `{{previous_conversation_summary}}` (the earlier nodes' transcript,
    summarized), `{{transcript}}` (`[12.3s] user: ...` lines, timed like the
    stored transcript above) and `{{metrics}}` (turn count, average/max reply
    latency and TTFB -- Vaani's own `turn.agent` timings, logged as
    `rtf-latency-measured`/`rtf-ttfb-metric` events purely for this).
  - Which LLM: the QA node's own (`qa_use_workflow_llm` off: openai,
    openrouter or anthropic -- azure isn't supported yet, a different
    auth shape) or, by default, the workflow **owner's** raw LLM
    configuration -- deliberately *not* the conversation's effective
    LLM (`resolveOwnerLLM`, not `resolveServices`): Dograh's own
    `resolve_user_llm_config` ignores the workflow's `model_overrides` too.
  - Each node's result (tags, a 1-10 quality score, sentiment, a summary --
    parsed from the review LLM's JSON reply with `agent.ParseLLMJSON`, the
    same parser variable extraction uses) is merged into the run's
    `annotations` as `qa_<node id>`, plus a top-level `tags` list across every
    QA node (Dograh's run list filters by this).
- **Time budgets:** creating the run is tried twice, 10s each. Completing it
  comes first and gets its own 10s; each upload 20s, the file update 10s --
  so a slow MinIO costs only the files, never leaves a run at `running`
  (Dograh likewise uploads in a later job). When Vaani is stopping (SIGTERM,
  a deploy), the end-of-call variable extraction is skipped so the
  completion is written inside the shutdown window; QA Analysis (above) is
  skipped outright rather than started.
- **Not done** (Dograh's completion job, which Vaani can't run): cost
  calculation, integrations/webhooks. LLM/TTS/STT usage in `usage_info` is
  left empty (QA's own token usage isn't currently counted either -- Dograh's
  job computes it, but never actually records a non-zero value either).

### Variable extraction

Ported from Dograh's `pipecat_engine_variable_extractor.py`
(`internal/ai/agent/extract.go`): a node with `extraction_enabled` and at
least one `extraction_variables` entry has its variables pulled from the
conversation so far, using the call's own LLM, at the same two moments
Dograh does:

- **Leaving the node**, the moment an edge is taken -- in the background, so
  it never delays the transition speech or the next turn.
- **The node the call ends at**, once every background extraction already
  running has finished (bounded by `extractionTimeout`, 30s, as Dograh's
  `_await_pending_extractions`) -- after `Handler.Done()`, before the run's
  `gathered_context` is written, so this one's result is always included.

Each run is a plain completion on the call's own LLM (no tools offered),
sent Dograh's exact two messages: a system prompt ("You are an assistant
tasked with extracting structured data...") plus the node's own
`extraction_prompt`, and a user message listing the variables (`- name
(VariableType.type): prompt`) followed by the conversation so far --
`user:`/`assistant:` lines, and each tool's result as `[Tool Response:
name]` (an `http_api` tool's `data` field if it has one, truncated at 2000
characters; an edge's `{"status":"done"}` left out, as Dograh does). The
reply is parsed as JSON (allowing a ```` ```json ```` block or surrounding
text, Dograh's `parse_llm_json`); anything else is dropped with a warning,
never fed into `gathered_context` as junk. Variables from multiple
extractions merge key by key, a later one overwriting an earlier value for
the same key (so the end-of-call extraction's view of the whole call can
correct an earlier node's). A background extraction starting in the
narrow window after the call has already finished (the handler's goroutine
can be mid-turn for a few statements past the call ending) is refused
rather than run unobserved, logged as `variable extraction skipped: the
call already finished`.

### Failure handling (nothing may strand a call)

- **Signals from the 20ms frame goroutine** (a possible interruption, the
  reply finished playing, the dead-TTS drain timeout) each have their own
  1-slot channel, never the shared event channel: that goroutine must not
  block, and a dropped event there used to leave a reply paused -- dead air
  -- for the rest of the call. A full slot means that same signal is already
  pending, so nothing is lost.
- **TTS connection lost** (the provider closed it): the handler forgets the
  dead client -- it used to be reused by every later turn, all failing --
  and ends the current turn after playing whatever audio already arrived;
  the next turn opens a fresh connection (`tts connection lost ...`).
- **LLM:** the shared HTTP client bounds each connection phase (dial 5s,
  TLS 5s, response headers 30s) but never the whole request, which would
  kill long streamed replies and uploads; a stream that sends nothing for
  30s ends the turn with an error (`llm: stream idle timeout`). An
  `{"error":...}` object inside a 200 stream, or a stream that ends without
  `[DONE]` having delivered nothing, is an error, not an empty reply; SSE
  `data:` lines are read with or without the space.
- **STT:** the connection is closed when the call ends (it used to stay
  open until Sarvam dropped it, holding a stream slot of the API key's
  concurrency limit); a failed redial backs off 200ms -> 5s instead of
  redialing on every 20ms frame; one handshake is bounded at 10s.
- **Call setup** (reading the workflow from Dograh, dialing the STT) runs
  on the call's own goroutine and outside the manager's lock, in both
  transports: in AudioSocket mode it used to hold the lock, so calls were
  set up one at a time and every other call's events waited.
- **Transfer handover:** the call being torn down is re-checked under the
  manager's lock at the moment the destination is registered, so a caller
  hanging up mid-handover can't leave the answered destination up (billable)
  and registered forever.
- **`end_call` then an interruption:** the end-call still happens even if
  the interruption cancelled the turn the tool ran in.

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
