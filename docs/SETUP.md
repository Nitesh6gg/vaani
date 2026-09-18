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

## Metrics

`http://localhost:9091/metrics` (Prometheus format) -- see `internal/metrics` for
the full list of series.

## Port ranges

- Asterisk's own SIP-leg RTP: `10000-19999` (`deploy/asterisk/rtp.conf`)
- Vaani's externalMedia RTP: `20000-20255` (`MEDIA_PORT_BASE`/`MEDIA_PORT_COUNT`)

These must never overlap.

## Endianness check

See `docs/AUDIO_PIPELINE.md` -- run `go run ./cmd/endianness-check` against a live
`make up` stack.
