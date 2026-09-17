# System Design

## Call flow (Phase 1: answer + RTP loop-back)

1. A SIP caller dials extension `1001`, which is routed by
   `deploy/asterisk/extensions.conf` into `Stasis(vaani)`.
2. `internal/ari.Manager` (`handlers.go`) receives the `StasisStart`, answers the
   caller channel, allocates a UDP port from `internal/media.PortAllocator`, and
   creates an `externalMedia` channel pointed at `MEDIA_IP:<port>`
   (`transport=udp`, `encapsulation=rtp`, `format=slin16`).
3. That externalMedia channel itself enters the same Stasis app with a second
   `StasisStart`; the Manager correlates it back to the caller via the channel ID
   it generated, creates a mixing bridge, and adds both channels to it.
4. `internal/media.CallMedia` (`loopback.go`) binds the allocated UDP port and runs
   the loop-back media plane for the call's duration: one reader goroutine parses
   and validates inbound RTP and feeds a buffered channel, one writer goroutine
   drains it on a 20ms `Pacer` and re-frames the audio as outbound RTP via `Sender`,
   echoing the caller's own voice back to them.
5. `StasisEnd` on either channel tears the call down: media plane stopped, bridge
   deleted, both channels hung up, UDP port freed.

## Process-wide singletons

- One ARI WebSocket connection for the whole process (`internal/ari.Connect`),
  reconnected with backoff at startup and observed for the life of the process.
- One `PortAllocator` guarding the configured UDP range so ports are never
  double-assigned across concurrent calls.
- One `sync.Pool` of 640-byte frame buffers (`internal/media/pool.go`), shared by
  every call's reader/writer goroutines to keep the hot path allocation-free.

## Per-call state

Exactly two goroutines per call (reader, writer) connected by a capacity-5 buffered
channel -- never a goroutine per frame. See `CLAUDE.md` for the full list of
invariants this design exists to preserve.
