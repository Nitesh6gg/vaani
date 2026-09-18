# Media Pipeline

Audio path: Asterisk PJSIP endpoint (ulaw) -> Asterisk transcodes -> externalMedia
RTP/UDP leg to the Go service, slin16 (16kHz 16-bit mono PCM) end-to-end on that leg.
See `internal/media/` for the RTP endpoint, pacer, buffer pool, jitter buffer, and
Handler pipeline, and `CLAUDE.md` for the invariants they implement.

## Pipeline (Phase 2)

```
UDP reader -> jitter buffer -> (20ms release tick) -> byte-order normalize
  -> Handler.ProcessFrame -> outbound queue -> (20ms write tick) -> UDP
```

Release and write run on independent 20ms tickers (`internal/media/pacer.go`),
connected by a small buffered channel. This decouples Handler processing time
from RTP pacing: once Phase 3 puts an LLM/TTS call inside `ProcessFrame`, a slow
tick just falls back to a silence-filled outbound frame for that interval rather
than delaying or bursting the paced RTP stream. See `internal/media/loopback.go`.

**RTP must never starve on an active call.** Once a remote is locked, the write
tick always transmits exactly one packet every 20ms: a real frame from the
handler's outbound queue, or a zeroed LE PCM16 silence frame
(`vaani_rtp_silence_sent_total`) if the queue is empty that tick -- handler
underrun, startup, or a Handler that legitimately produced nothing. Set
`TEST_SILENT_HANDLER=1` to force every call's Handler to produce zero frames on
every tick, deterministically exercising this path (never set it in production --
calls carry no audio at all); see `TestCallMedia_SilentHandlerAlwaysTransmitsSilence`
in `internal/media/loopback_test.go`.

## Jitter Buffer

`internal/media/jitterbuffer.go` is a fixed-size ring buffer (`JITTER_BUFFER_PACKETS`,
default 3 = 60ms) keyed by RTP sequence number, using RFC 1982 serial-number
arithmetic so 16-bit sequence wraparound is handled correctly:

- **Reorder**: packets arriving within the window ahead of the next-expected
  sequence are held and released in order, not lateness.
- **Late** (`vaani_rtp_late_total`): a packet behind the window's current
  position is discarded -- releasing it now would be out of order.
- **Duplicate** (`vaani_rtp_duplicates_total`): a second packet for a sequence
  number already buffered is discarded.
- **Loss concealment** (`vaani_rtp_silence_inserted_total`): if the
  next-expected sequence hasn't arrived by its release tick, a pooled, zeroed
  silence frame is released instead -- one per missing packet, not a stall.
- **SSRC change** (`vaani_rtp_ssrc_changes_total`): a new SSRC drops every
  buffered frame and re-anchors the window on the new stream's sequence number.
- **Priming**: releases are a pure no-op (no metric, no state change) until at
  least `JITTER_BUFFER_PACKETS` real packets are buffered. Without this, the
  release ticker -- which starts firing the instant the media plane starts, on
  its own fixed schedule, independent of when RTP actually begins flowing --
  could advance past sequence numbers no packet had reached yet, permanently
  racing ahead of the real stream. This was a real production bug (see below).
- **Starve-freeze + re-anchor** (`vaani_jb_reanchors_total`): Vaani's release
  ticker and Asterisk's RTP pacing clock are two independent, unsynchronized
  20ms clocks that drift relative to each other over a long call. After 3
  consecutive missed releases, the window freezes (stops advancing) rather than
  keep guessing; the next packet that arrives at or after the frozen position
  re-anchors the window there and re-primes.

**Production incident (fixed in the commit that added priming/starve-freeze/
re-anchor):** a live call showed `vaani_rtp_late_total` at 94.5% of inbound
packets, `vaani_audio_rms` pinned at 0, and total silence for the caller, despite
clean bidirectional RTP confirmed by `tcpdump` on the Asterisk box. Root cause:
the release ticker fired before the first packet ever arrived and kept advancing
`expected` with no way to recover once out of phase with the real stream --
exactly the failure priming and starve-freeze/re-anchor exist to prevent. Verified
by reproducing the old (pre-fix) logic against the same adversarial-timing test
and confirming it hit 99.98% late on a scenario the fixed buffer bounds to ~12%
(`internal/media/jitterbuffer_test.go`).

It owns pooled frame buffers end-to-end (`internal/media/pool.go`): frames pushed
in are either stored or immediately returned to the pool if discarded; frames
released out are the caller's responsibility to return. Zero allocations in
steady state, verified by `TestJitterBuffer_ZeroAllocInSteadyState`.

Ear-test note: the jitter buffer adds up to one window's worth of latency (60ms
at the default `JITTER_BUFFER_PACKETS=3`) versus Phase 1's direct passthrough,
since audio is now held for possible reordering before release. Set
`JITTER_BUFFER_PACKETS=1` for latency-sensitive manual testing; the load test
should use the default, since loss concealment is exactly what it validates.

## The Handler Contract

`internal/media/handler.go` defines the seam Phase 3 plugs into:

```go
type Handler interface {
    ProcessFrame(ctx context.Context, callID string, pcm []byte) [][]byte
}
```

`pcm` is always exactly one 20ms frame (640 bytes) of **little-endian PCM16**,
already jitter-buffered and byte-order-normalized -- Handler implementations
never see raw wire bytes or need to know the actual `AUDIO_L16_ENDIANNESS`.
Called once per release tick, from a single goroutine per call, so
implementations need no internal locking. `LoopbackHandler`
(`internal/media/handler.go`) is Phase 1's behavior preserved as the first
implementation: return the input frame unchanged. When Phase 3 arrives, STT
feeding, LLM streaming, and TTS playout become one `ProcessFrame` implementation
plus per-call state -- no transport code changes.

## Endianness Verification

RTP L16 payloads are nominally big-endian per RFC 3551, but that's a generic
profile default, not a guarantee about a specific Asterisk build's externalMedia
implementation -- so it's checked empirically rather than assumed, since Phase 3
(STT) needs to know which byte order it's receiving.

The result feeds `AUDIO_L16_ENDIANNESS` (`le` or `be`, see `.env.example`), which
`internal/media.NormalizeToLE` uses to convert inbound payloads to little-endian
before they ever reach the jitter buffer's release or a Handler -- see "The
Handler Contract" below. **Status: defaults to `le` pending an actual probe run
against a live stack; the probe below has not yet been executed and appended.**
Run it and set `AUDIO_L16_ENDIANNESS` accordingly before trusting Phase 3 audio.
Until it's explicitly set (real environment or `.env`), `cmd/server` logs a
startup warning (`internal/media.WarnIfUnset`) so an unverified assumption never
passes silently.

Run `cmd/endianness-check` against a live `make up` stack:

```bash
go run ./cmd/endianness-check
```

It originates a dialplan `Echo()` leg (`deploy/asterisk/extensions.conf`, context
`vaani-tools`, extension `9001`), bridges it with a second externalMedia channel,
sends a 2s 440Hz tone as little-endian PCM16, and scores what echoes back under
both byte-order interpretations by mean absolute sample-to-sample delta -- a real
waveform has small deltas, and byte-swapping a smooth waveform scrambles it into
large ones, so the lower-scoring interpretation is the correct one. On success it
prints its verdict, an `AUDIO_L16_ENDIANNESS=le`/`be` line ready to paste into the
service's environment, and appends the verdict below.

<!-- cmd/endianness-check appends its result below this line; do not hand-edit -->
