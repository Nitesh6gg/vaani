# Asterisk Setup

## Local dev via Docker Compose

```bash
make up      # builds and starts asterisk + vaani, waits for healthchecks
make logs    # follow both services
make down    # stop and remove
```

This uses `deploy/docker-compose.yml`, which puts both services on a dedicated
`vaani_net` bridge network with static IPs (`asterisk` at `172.28.0.10`, `vaani` at
`172.28.0.20`). `MEDIA_IP` is set to vaani's own static IP explicitly -- see the
decision recorded in `internal/config/config.go` for why this isn't auto-detected.

## Dialing in

Register a softphone (Zoiper, Linphone, ...) against the Asterisk container:

- SIP server: `localhost:5060` (UDP)
- Username / password: `1001` / `vaani1001` (`deploy/asterisk/pjsip.conf`)

Dial extension `1001`. You should hear your own voice echoed back -- that's the
loop-back path (`deploy/asterisk/extensions.conf` -> `Stasis(vaani)` ->
externalMedia RTP -> `internal/media` pipeline -> back to you).

**Expected latency (Phase 2):** perceived round-trip latency is higher than
Phase 1's direct passthrough by roughly one jitter buffer window --
**~60ms at the default `JITTER_BUFFER_PACKETS=3`** -- since audio is now held
briefly for possible reordering before release (see "Jitter Buffer" in
`docs/AUDIO_PIPELINE.md`). This is expected, not a regression. For latency-
sensitive manual ear-testing, set `JITTER_BUFFER_PACKETS=1` (20ms window); leave
it at the default for load testing, since loss concealment is what that
validates.

## Agent mode: connecting to Dograh

`APP_MODE=agent` needs a self-hosted Dograh; the agent's workflow, tools,
models and keys are all configured there (see "Configured in Dograh" in
`docs/ARCHITECTURE.md`). In Vaani's `.env`:

- `DOGRAH_DB_URL` -- Dograh's Postgres, e.g.
  `postgresql://user:pass@host:5432/dograh`. Vaani reads the workflow from
  it and writes each call's record to `workflow_runs` (and adds disposition
  codes to `workflows.call_disposition_codes`), so the user needs INSERT and
  UPDATE on those two tables. Checked at startup (10s): unreachable = the
  server doesn't start.
- `DOGRAH_WORKFLOW_ID` -- the workflow to run: the number in its editor URL
  (`/workflow/19` -> `19`).
- `MINIO_ENDPOINT`, `MINIO_ACCESS_KEY`, `MINIO_SECRET_KEY`, `MINIO_BUCKET`
  (default `voice-audio`), `MINIO_SECURE` (default `false`) -- Dograh's
  MinIO, for each call's recording and transcript: copy the values from
  Dograh's own environment, with `MINIO_ENDPOINT` as `host:port` reachable
  from Vaani. Without `MINIO_ENDPOINT` calls still appear in Dograh's call
  history, without recording or transcript.

No `LLM_*` or `SARVAM_*` settings -- they were removed; if still present in
an old `.env` they're ignored.

In Dograh:

- **Publish** the workflow -- Vaani runs the published version (or the legacy
  "current" one), not a draft.
- The **workflow's owner** needs a model configuration (LLM, STT, TTS with
  keys). STT and TTS must be Sarvam; the LLM an OpenAI-compatible provider
  (list in `docs/AI_PROVIDERS.md`). A workflow's own model overrides take
  precedence, per service.
- **Tools** reach the agent only when attached to a node (the node's Tools
  setting). A transfer tool's destination must be an Asterisk dial string
  that exists on this Asterisk, e.g. `PJSIP/<number>@<endpoint>` -- chan_sip
  (`SIP/...`) doesn't exist in Asterisk 21+.
- **Call limits** (caller silence, maximum call length) come from the
  workflow's Settings page; never saved, 10s and 300s apply.
- **Interruption** is per node: switch on a node's `allow_interrupt` where
  the caller may cut in while the agent speaks. It only takes effect with
  `BARGE_IN_ENABLED=1` in Vaani's `.env` (the master switch); with `0`
  nothing is ever cut.

Network: the Vaani host must reach Dograh's Postgres and the LLM endpoint
itself (for a gateway such as Bifrost, its `base_url`, e.g.
`curl http://<bifrost-host>:8080/v1/models` from the Vaani host), plus
Sarvam's API over the internet.

What a healthy call logs at its start:

    agent workflow loaded ... start_node="Start Call" opening=llm start_tools="[...]" start_allow_interrupt=false start_interrupt=off idle_timeout_s=10 max_duration_s=300
    agent models ... llm=bifrost/<model> llm_url=... stt=sarvam/saaras:v4 ... tts_voice=...

## Metrics and health

`http://localhost:9091/metrics` (Prometheus format) -- see `internal/metrics` for
the full list of series. `http://localhost:9091/healthz` returns 200 JSON while
the ARI WebSocket is connected, 503 otherwise. Neither endpoint requires
authentication -- see "Operability endpoints" in `docs/ARCHITECTURE.md`.

## Memory limit (`GOMEMLIMIT`)

Set Go's soft memory limit to about **90% of the memory Vaani may use**
(its container limit, or its share of the host), so the garbage collector
works harder as memory gets tight instead of the process being OOM-killed
mid-call:

```bash
# systemd unit
Environment=GOMEMLIMIT=3600MiB      # e.g. 4 GiB available to Vaani
# docker / compose
-e GOMEMLIMIT=3600MiB               # with --memory=4g
```

It's a runtime environment variable -- no code or config change. It's a
*soft* limit: it makes the GC run more often near it, but if the live data
really exceeds it the process still runs out, so size the box for the load.

What grows per agent call (from the code):

- The call recording for Dograh (`media.MixRecorder`): 32,000 bytes per
  second of call (16 kHz mono, 16-bit), about 1.9 MB a minute, held in
  memory until the call ends; capped at 100 MB (about 54 minutes,
  `vaani_recording_truncated_total` counts calls that hit it).
- The agent's outbound audio queue: up to 30 s of frames, about 0.96 MB
  when full.
- Plus the STT and TTS WebSocket connections and the conversation history.

So a 5-minute call holds roughly 10 MB of recording at its end; 500 such
calls ending together need about 5 GB for recordings alone. Measure under
your real load (`docs/LOADTEST.md`) before choosing the box.

## Port ranges

- Asterisk's own SIP-leg RTP: `10000-19999` (`deploy/asterisk/rtp.conf`)
- Vaani's externalMedia RTP: `20000-20255` (`MEDIA_PORT_BASE`/`MEDIA_PORT_COUNT`)

These must never overlap.

## Orphaned-call cleanup (`rtptimeout` + `MEDIA_DEAD_TIMEOUT_SECONDS`)

Two independent layers catch a call whose media has gone dead, for two
different reasons -- see "Dead-Call Auto-Hangup" in `docs/AUDIO_PIPELINE.md`
for the full explanation:

- `deploy/asterisk/rtp.conf` sets `rtptimeout=60`/`rtpholdtimeout=300`:
  Asterisk hangs up a channel that goes that long with no RTP activity on the
  *caller's* SIP-side leg (network drop, crashed client, a killed load-test
  tool -- no SIP BYE ever sent).
- Vaani's own `MEDIA_DEAD_TIMEOUT_SECONDS` (default 30, `.env.example`) hangs
  the call up if *its own* externalMedia leg goes fully silent, independent of
  whether the caller's leg looks fine to Asterisk -- catches an
  Asterisk-internal bridging/mixing failure that `rtptimeout` can't see.

Without either, an orphaned call's externalMedia leg would stay open
indefinitely (no `StasisEnd` ever fires) instead of tearing down normally.

## Endianness check

See `docs/AUDIO_PIPELINE.md` -- run `go run ./cmd/endianness-check` against a live
`make up` stack.

## Barge-in / TEN VAD (`VAD_MODE=ten`)

The barge-in detector is selectable via `VAD_MODE` (full knob table in
`.env.example` and `docs/AI_PROVIDERS.md`):

- `energy` (default): RMS threshold, pure Go, no extra dependencies.
- `ten`: TEN Framework's TEN VAD neural model. Linux x86-64 + cgo only (the
  deployment target); the native library is vendored under
  `third_party/ten-vad/` -- no clone or download needed, but the box must have
  `gcc` (any cgo build) and LLVM libc++, which Debian does not ship by default:

      apt-get install -y libc++1 libc++abi1

  Without it the link fails with "libc++.so.1, needed by libten_vad.so, not
  found" and a wall of `std::__1::...` undefined references.

On non-Linux builds (or if the library can't load) the server starts fine and
each call falls back to the energy detector with a loud warning --
`BARGE_IN_ENABLED=0` + `VAD_MODE=ten` still runs the VAD in observe-only mode
(speech started/ended logs, no cuts).
