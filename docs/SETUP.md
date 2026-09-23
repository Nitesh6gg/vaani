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

## Metrics and health

`http://localhost:9091/metrics` (Prometheus format) -- see `internal/metrics` for
the full list of series. `http://localhost:9091/healthz` returns 200 JSON while
the ARI WebSocket is connected, 503 otherwise. Neither endpoint requires
authentication -- see "Operability endpoints" in `docs/ARCHITECTURE.md`.

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
