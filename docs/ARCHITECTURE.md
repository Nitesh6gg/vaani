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

## Per-call state

Exactly two goroutines per call (reader, writer) connected by a capacity-5 buffered
channel -- never a goroutine per frame. See `CLAUDE.md` for the full list of
invariants this design exists to preserve.

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
