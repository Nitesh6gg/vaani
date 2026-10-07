# FreeSWITCH support — Phase 1 research and design

Status: **F1 code done (loopback), awaiting the lab call** — see section 7.
Each fact below is marked **[source]** (read in mod_earshot `c5847cc` or
FreeSWITCH `v1.10.12`/`v1.11.3` source) or **[lab]** (a design assumption
still to prove on a real call).

Target: **FreeSWITCH 1.11.x** (the lab box runs 1.11.3: SignalWire's
`debian-release` repo now ships 1.11; the design was first checked against
1.10.12, and mod_event_socket's message types and commands are identical in
both) + **mod_earshot** (MIT,
github.com/wiringai/mod_earshot) for media, and FreeSWITCH's built-in
**Event Socket (ESL)** for call control. Asterisk support stays as it is;
`TELEPHONY=asterisk|freeswitch` picks one per process.

The agent itself does not change: workflow, prompts, tools, models and keys
still come from Dograh's Postgres, and each call is still written to Dograh's
call history and MinIO. Only the telephony side is new — and it is designed
from FreeSWITCH's and Earshot's own docs/source, **not** copied from Dograh's
FreeSWITCH integration (which uses `mod_audio_fork`, not part of stock
FreeSWITCH).

## 1. The big picture

```
 caller ──SIP/RTP──► FreeSWITCH 1.11
                       │  dialplan: answer → earshot start → silence_stream
                       │
                       ├── mod_earshot ══ WebSocket (one per call) ══►  Vaani :9095
                       │     caller audio  → binary L16 16 kHz frames      (media)
                       │     agent audio   ← binary L16 16 kHz frames
                       │     barge-in      ← {"type":"clear"}
                       │
                       └── mod_event_socket :8021 ◄══ TCP (ONE per Vaani) ══ Vaani
                             events → CHANNEL_ANSWER, CHANNEL_HANGUP_COMPLETE, ...
                             commands ← uuid_kill, originate, uuid_bridge, earshot ...
```

Compared with Asterisk:

| | Asterisk (today) | FreeSWITCH (new) |
|---|---|---|
| Call control | ARI WebSocket, one per process | ESL TCP, one per process (inbound mode) |
| Who starts a call's media | Vaani asks for `externalMedia` | the dialplan runs `earshot start` |
| Media connection | Asterisk → Vaani (AudioSocket TCP) or RTP | **FreeSWITCH connects to Vaani** (WebSocket) |
| Frame on the wire | AudioSocket TLV / RTP, exactly 640 B | WebSocket binary message, raw L16 |
| Hang up | ARI `channel.Hangup` | ESL `api uuid_kill <uuid> NORMAL_CLEARING` |
| Transfer | ARI originate + bridge swap | ESL `originate ... &park()` + `uuid_bridge` |

## 2. mod_earshot — what it does (verified)

- Runs on FreeSWITCH 1.10 and 1.11 (its CI builds both; a `.deb` "for FS
  1.10/1.11"). **[source]** `.github/workflows/ci.yml`, `CHANGELOG.md`
- Needs FreeSWITCH dev headers (`libfreeswitch-dev`, from SignalWire's apt
  repo — needs a free SignalWire token) and `libwebsockets-dev`. Build:
  `cmake -S . -B build && cmake --build build && sudo cmake --install build`,
  then `fs_cli -x "load mod_earshot"`. **[source]** `README.md`, `Dockerfile`
- It is a **WebSocket client**: on `start <url>`, FreeSWITCH connects to the
  URL. Vaani is the server. **[source]** `es_ws.c`
- Handshake headers Vaani receives: `X-Call-ID` (SIP Call-ID),
  `X-Channel-UUID` (FreeSWITCH channel UUID), `X-Correlation-ID`, and
  `X-Earshot-Meta` (the `EARSHOT_META` channel variable, verbatim).
  **[source]** `es_ws.c:207-217`

### `proto=native codec=l16 rate=16000` — the wire format we use

- **Caller → Vaani:** one binary message per media-bug read, raw L16, no
  header, no JSON preamble ("native / pipecat: no preamble"). L16 is a
  `memcpy` of host-order samples — **little-endian** on x86-64/ARM64, same as
  Vaani's pipeline. **[source]** `es_proto.c:247,367-369`, `es_codec.c:141`
- The channel is usually 8 kHz (G.711); Earshot resamples to the wire rate
  (16 kHz) with FreeSWITCH's speex resampler. **[source]** `es_proto.c:160-163`
- So messages are *about* 20 ms (640 B) each, but **not guaranteed exactly
  640 B** (resampler output can vary per call). Vaani must **re-frame**
  (accumulate and cut 640-byte frames) instead of dropping odd sizes as the
  RTP path does. **[lab]** confirm actual sizes.
- **Vaani → caller:** binary L16 of **any length**; Earshot decodes it into a
  play buffer (cap 2 MB per call) and plays whole frames on FreeSWITCH's
  write clock. **[source]** `es_proto.c:790-791`, `mod_earshot.c:42,973-1001`
- **Barge-in:** text `{"type":"clear"}` empties Earshot's play buffer.
  **[source]** `es_proto.c:748-749`
- When the play buffer is empty, Earshot leaves FreeSWITCH's own audio
  untouched (300 ms of silence first, to hide gaps between chunks).
  **[source]** `mod_earshot.c:46,994-1000`
- The channel must keep writing frames for playback to happen — every
  Earshot recipe runs `playback silence_stream://-1` after `start`.
  **[source]** `QUICKSTART.md`
- Auto-reconnect is **on by default**; `EARSHOT_NO_RECONNECT=true` turns it
  off. **[source]** `mod_earshot.c:1183`
- Events: `earshot::connected`, `disconnected`, `error`, `ready`, `metrics`,
  `speech_started`, `speech_stopped`, `dtmf`, `command`, `transcript`.
  **[source]** `mod_earshot.c:22-31`

### Earshot features we deliberately leave OFF

- `vad`/`vad_barge`/`interruptible`: Vaani already does barge-in with its own
  VAD (TEN/energy) and the pause → confirm → 5-cuts flow. Two detectors would
  fight (Earshot's own `docs/MODULE-VS-AGENT.md` says the same).
- `commands=true` (agent drives hangup/transfer over the media socket): Vaani
  uses ESL for control, so the media socket never carries control.
- `greeting=`: the opening line comes from the Dograh workflow.

## 3. Event Socket (ESL) — what we use (verified, FreeSWITCH v1.10.12 and v1.11.3)

From `mod_event_socket.c` and the stock `event_socket.conf.xml`:

- Default: listen `::` port **8021**, password **ClueCon** (change it!),
  `apply-inbound-acl` commented out — lock it down to Vaani's IP.
- Framing: headers `Name: value\n`, blank line, then `Content-Length` bytes of
  body if that header is present.
- Handshake: server sends `Content-Type: auth/request`; client sends
  `auth <password>\n\n`; reply `Content-Type: command/reply` with
  `Reply-Text: +OK accepted` or `-ERR ...`.
- `api <cmd>` → `Content-Type: api/response` (blocks the socket until done).
- `bgapi <cmd>` → immediate `Reply-Text: +OK Job-UUID: <id>`, and later a
  `BACKGROUND_JOB` event with that `Job-UUID` and the result. Use for
  anything slow (originate).
- `event plain <names...>` subscribes; `CUSTOM earshot::connected ...` for
  module events. Events arrive as `Content-Type: text/event-plain`.
- `text/disconnect-notice` when FreeSWITCH drops us.

**Inbound mode** (Vaani connects to FreeSWITCH, one connection for the
process) — same shape as invariant #6 for ARI. Not outbound mode (`socket`
app, one TCP connection per call), because Earshot already gives us the
per-call connection; ESL only needs events + commands.

ESL client: our own, `internal/esl` (the protocol above is all of it).
`percipia/eslgo` was reviewed and not used: it calls each event listener in
a new goroutine (events for one call can arrive out of order), and
concurrent or timed-out commands can be handed each other's replies.
`internal/esl` delivers events in order on one channel, runs one command at
a time, closes the connection when a reply misses its deadline, and rejects
CR/LF in commands (ESL command injection). Reconnecting is the caller's job,
as with ARI.

## 4. One call, step by step

Dialplan (on FreeSWITCH):

```xml
<extension name="vaani">
  <condition field="destination_number" expression="^(1001)$">
    <action application="set" data="EARSHOT_NO_RECONNECT=true"/>
    <action application="set" data="EARSHOT_META={&quot;from&quot;:&quot;${caller_id_number}&quot;,&quot;to&quot;:&quot;${destination_number}&quot;}"/>
    <action application="answer"/>
    <action application="earshot" data="start ws://VAANI_IP:9095/call proto=native codec=l16 rate=16000"/>
    <action application="playback" data="silence_stream://-1"/>
  </condition>
</extension>
```

1. Caller dials 1001; FreeSWITCH answers and Earshot opens
   `ws://VAANI_IP:9095/call`.
2. Vaani accepts. `call_id` = `X-Channel-UUID`; caller/called number from
   `X-Earshot-Meta` (validated — digits/`+` only, as today). The SIP Call-ID
   is logged too.
3. Vaani loads the Dograh workflow and starts the same agent Handler used for
   Asterisk calls. The media pipeline is the AudioSocket one with WebSocket
   framing: reader goroutine (re-frame to 640 B) → 20 ms release tick →
   Handler → 20 ms write tick → one binary message per frame.
4. Vaani **keeps pacing at 20 ms** (invariant #1), so Earshot's buffer holds
   only a frame or two. Why: Vaani's pause/resume for barge-in works by not
   sending; if we dumped whole sentences into Earshot's 2 MB buffer, a
   "pause" would be impossible to do and a cut would depend on `clear`
   alone.
5. Barge-in confirmed → the 5 cuts as today **plus** `{"type":"clear"}` to
   drop the frame or two already queued in Earshot.
6. Agent ends the call → ESL `api uuid_kill <uuid> NORMAL_CLEARING`.
7. Caller hangs up → ESL `CHANNEL_HANGUP_COMPLETE` (with `Hangup-Cause`) and
   the WebSocket closes → same teardown path as today (once-guaranteed),
   then `recordRun` writes Dograh's `workflow_runs` + MinIO upload.

**Transfer** (Dograh transfer tool; destination is now a FreeSWITCH dial
string, e.g. `sofia/gateway/<gw>/<number>` or `user/1002`):

1. Hold ring plays from Vaani over the media stream (unchanged).
2. `bgapi originate {origination_uuid=<new>,origination_caller_id_number=<caller>,originate_timeout=<s>}<dial-string> &park()`
3. `CHANNEL_ANSWER` for `<new>` → `api earshot <caller-uuid> stop` →
   `api uuid_bridge <caller-uuid> <new>`. The agent stops; the call is now
   caller ↔ destination. **[lab]** confirm `uuid_bridge` cleanly takes the
   caller out of `silence_stream`.
4. No answer / failure → `BACKGROUND_JOB` `-ERR <cause>` or
   `CHANNEL_HANGUP_COMPLETE` on `<new>` → agent says so, as today.

## 5. Things to prove in the lab (Phase F1)

| Question | Why it matters |
|---|---|
| Inbound message size with `codec=l16 rate=16000` on an 8 kHz channel | re-framing design |
| Clock drift: Vaani's 20 ms ticker vs FreeSWITCH's write clock over a 10-min call (`earshot <uuid> status` → `play_buffered`) | if Vaani runs fast, Earshot's buffer slowly grows = growing delay; fix would be to pace writes off inbound frames |
| `uuid_bridge` while the dialplan runs `silence_stream` | transfer |
| What Earshot sends on `uuid_kill` (WS close code) | clean teardown, no "media dead" warnings |
| Audio quality: 8 kHz G.711 caller → 16 kHz resample → Sarvam STT | STT accuracy vs Asterisk path |

## 6. Code

Done in F1:

- `internal/config`: `TELEPHONY` (`asterisk` default), `ESL_ADDR`,
  `ESL_PASSWORD`, `EARSHOT_LISTEN_ADDR` (`:9095`), `EARSHOT_AUTH_TOKEN`.
  `TELEPHONY=freeswitch` accepts only `APP_MODE=loopback` until F2.
- `internal/esl/`: inbound ESL client (auth, api, bgapi, ordered events).
- `internal/media/earshot.go`: the earshot wire for the existing framed-call
  pipeline (same pacer, pool, Handler, recorder, watchdog as AudioSocket):
  inbound binary messages re-framed into 640-byte frames, one binary message
  per outbound frame, close code recorded.
- `internal/freeswitch/`: call manager — accepts `/call` (auth token,
  `X-Channel-UUID` validated before it's used anywhere), one call per channel
  UUID, ESL connect with backoff and `CHANNEL_HANGUP_COMPLETE` → call ends;
  any other end (socket closed, media dead, max duration, shutdown) →
  `uuid_kill`. Lab logging: `call.ended` carries earshot message counts, odd
  sizes and close code; `earshot.status` logs `play_buffered` every 30 s.

Still to do:

- F2: move the telephony-neutral parts of `internal/ari/handlers.go`
  (`loadWorkflow`, `newAgentHandler`, `recordRun`, `logCallSummary`, barge-in
  detector) into a shared package so both call managers use them; send
  `{"type":"clear"}` on a confirmed barge-in.
- F3: transfer (section 4).
## 7. Milestones

- **F1 — lab proof:** FreeSWITCH 1.11 + Earshot installed; Vaani
  `APP_MODE=loopback TELEPHONY=freeswitch`. Done = you hear your own voice;
  section 5 questions answered with real numbers.
- **F2 — agent calls:** a Dograh workflow call end-to-end on FreeSWITCH;
  barge-in; hangup either side; call appears in Dograh history with
  recording + transcript.
- **F3 — transfer:** Dograh transfer tool to a FreeSWITCH dial string; ring,
  answer, bridge, no-answer path.
- **F4 — load:** SIPp against FreeSWITCH, same targets as `docs/LOADTEST.md`.
