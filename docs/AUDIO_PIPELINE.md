# Media Pipeline

Audio path: Asterisk PJSIP endpoint (ulaw) -> Asterisk transcodes -> externalMedia
leg to the Go service, slin16 (16kHz 16-bit mono PCM) end-to-end on that leg. The
externalMedia transport is selectable via `MEDIA_ENCAPSULATION`: **RTP over UDP**
(default) or **AudioSocket over TCP** (Asterisk's res_audiosocket protocol) -- see
"AudioSocket Transport" below. See `internal/media/` for both transports' readers,
the shared pacer/pool/Handler pipeline, and `CLAUDE.md` for the invariants they
implement.

## Pipeline (RTP)

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

## Near-Silence Warning at Teardown

`internal/ari/handlers.go`'s `teardown` already warns when a call's RTP remote
was never locked (no audio at all). That check can't catch a subtler failure:
transport-healthy audio -- remote locked, packets arriving on schedule -- that
still comes out near-silent end to end. That's exactly what happened in the
jitter-buffer production incident above: Asterisk really was sending RTP, so
`RemoteLocked()` was true, but the buffer's permanent desync meant almost every
release was concealment silence instead of real audio.

`CallMedia`/`AudioSocketCallMedia` now track the mean RMS (`internal/media.rms`,
already computed per tick for `vaani_audio_rms`) across every release tick of a
call's lifetime. At teardown, if at least `nearSilenceMinTicks` (50, ~1s) ticks
were observed and the average stays below `nearSilenceRMSThreshold` (50, out of
a max-amplitude PCM16 sample of 32767), `teardown` logs `call carried
near-silent audio throughout; audio pipeline may be misconfigured` with the
call ID and average RMS -- independently of, and in addition to, the
remote-locked check. A short call (fewer than 50 ticks) is never judged, to
avoid false positives on calls that hang up almost immediately.

## Live Audio Tap (`DEBUG_AUDIO=1`)

Set `DEBUG_AUDIO=1` to mount `/debug/audio/{callID}` on `METRICS_ADDR`: while a
call is active, `GET` that path streams its inbound audio (raw s16le PCM,
post-jitter-buffer, post-normalization -- the same bytes written to the WAV
recorder and handed to the Handler) in real time, for listening without waiting
for `RECORD_DIR`'s WAV file to close:

```bash
curl -sN http://localhost:9091/debug/audio/<callID> | aplay -f S16_LE -r 16000 -c1 -
```

`internal/media/audiotap.go`'s `AudioTap` fans a call's release-tick PCM out to
however many concurrent listeners are subscribed (zero-cost when none are);
`RegisterAudioTap`/`UnregisterAudioTap` key a process-wide registry by call ID so
the HTTP handler (`internal/metrics`) doesn't need a reference to the specific
`CallMedia`/`AudioSocketCallMedia` instance. `UnregisterAudioTap` closes the tap,
which closes every subscriber's channel -- without that, a streaming client would
block past the call's actual lifetime instead of seeing its stream end. A slow
subscriber drops frames rather than stalling the call's release tick (same
non-blocking-send philosophy as the outbound queues in "Pipeline" above).

Never set `DEBUG_AUDIO=1` in production: anyone who can reach `METRICS_ADDR` can
listen to any active call's audio.

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

## AudioSocket Transport

Set `MEDIA_ENCAPSULATION=audiosocket` to use Asterisk's res_audiosocket protocol
(TCP) instead of RTP/UDP for the externalMedia leg:

```
POST /ari/channels/externalMedia
{ "app": "vaani", "external_host": "<ip>:<port>",
  "encapsulation": "audiosocket", "transport": "tcp", "format": "slin16",
  "data": "<uuid>" }
```

**`data` is mandatory for this encapsulation** -- confirmed live via `asterisk -rx
"ari set debug vaani on"`: without it, Asterisk's chan_audiosocket returns `400
Bad Request: data can not be empty`. It's the UUID Asterisk echoes back as the
connection's first AudioSocket frame (kind `0x01`). The vendored ARI library
(`github.com/CyCoreSystems/ari/v5`)'s `ExternalMediaOptions` struct has no field
for it at all -- not a bug in our usage, a real gap in the library -- so
`internal/ari/externalmedia.go` makes this one raw REST call itself (generating
the UUID, building the JSON body, POSTing with Basic Auth) instead of going
through `Channel().ExternalMedia()`, which the RTP path still uses unchanged.

Wire format (`internal/media/audiosocket.go`), one length-prefixed frame at a time
on a single TCP stream:

```
+------------------+-----------------------+--------------------------+
| Type (1 Byte)    | Payload Length (2B)   | Payload (Variable Length)|
+------------------+-----------------------+--------------------------+
| 0x12 for slin16  | big-endian uint16     | Raw PCM16 (Little-Endian)|
+------------------+-----------------------+--------------------------+
```

Vaani binds a TCP listener the same "before Asterisk knows the address" way it
binds the UDP socket for RTP (see the invariant in `CLAUDE.md`); once the call is
bridged, it accepts Asterisk's one incoming connection (10s timeout) and hands it
to `internal/media.AudioSocketCallMedia`.

That pipeline (`reader -> inbound queue -> release tick -> Handler.ProcessFrame ->
outbound queue -> write tick -> writer`) deliberately mirrors the RTP pipeline's
shape -- independent release/write tickers, the same "never starve the outbound
stream" silence-fallback guarantee, WAV recording, watchdog -- because the
`Handler` contract must not care which transport it's running over. It's simpler
in exactly the ways AudioSocket itself is simpler than RTP:

- **No jitter buffer.** TCP already guarantees in-order, lossless delivery --
  there's nothing to reorder, no duplicates, no loss to conceal. `JITTER_BUFFER_PACKETS`
  is ignored.
- **No byte-order question.** The protocol's audio payload is little-endian by
  definition, not a per-deployment fact to verify. `AUDIO_L16_ENDIANNESS` is
  ignored.
- **No "remote locked" ambiguity.** A UDP socket accepts packets from anywhere
  until an address is learned from the first one; a TCP `Accept()` only succeeds
  once Asterisk has actually connected, so there's no equivalent state to track.

Metrics are transport-specific where the concept is (`vaani_audiosocket_frames_in_total`,
`_frames_out_total`, `_silence_sent_total`, `_send_errors_total`, `_malformed_total`
-- reusing `vaani_rtp_*` names would be misleading for a transport that carries no
RTP at all) and shared where it's transport-neutral (`vaani_audio_rms`,
`vaani_pacer_drift_ms`, `vaani_media_watchdog_timeouts_total`).

**Why this might matter for your network:** RTP is a UDP flow with no persistent
connection state; a stateful firewall or asymmetric route on a multi-site link can
silently drop it (this happened in production -- see the port-binding invariant in
`CLAUDE.md`). AudioSocket's single TCP connection tends to traverse routed/
firewalled paths more reliably, at the cost of TCP's own head-of-line blocking
under real packet loss (irrelevant on a healthy LAN, worth knowing about on a lossy
WAN link).

## Endianness Verification

RTP L16 payloads are nominally big-endian per RFC 3551, but that's a generic
profile default, not a guarantee about a specific Asterisk build's externalMedia
implementation -- so it's checked empirically rather than assumed, since Phase 3
(STT) needs to know which byte order it's receiving. **It has also been observed
to differ by Asterisk build** (see "Confirmed finding" below), so a verdict from
one deployment must never be copied to another without re-running the check.

The result feeds `AUDIO_L16_ENDIANNESS` (`le` or `be`, see `.env.example`), which
`internal/media.NormalizeToLE` uses to convert inbound payloads to little-endian
before they ever reach the jitter buffer's release or a Handler -- see "The
Handler Contract" below. Until it's explicitly set (real environment or `.env`),
`cmd/server` logs a startup warning (`internal/media.WarnIfUnset`) so an
unverified assumption never passes silently. **Running `cmd/endianness-check` (or
equivalent live confirmation) is mandatory before trusting any Phase 3 (STT)
audio on a given deployment** -- a wrong value doesn't fail loudly on its own; it
silently hands STT a scrambled signal that still looks like "audio arrived."

### Confirmed finding (this deployment, 2026-09-22)

Live call testing (not the tool below, which was broken at the time -- see "Tool
bug fixed" next) confirmed `AUDIO_L16_ENDIANNESS=le` decodes to noise and
`AUDIO_L16_ENDIANNESS=be` decodes to real speech on this deployment's Asterisk
build. This deployment's `.env` is set to `be` accordingly. This does **not**
change `.env.example`'s default: the value is a per-deployment fact, not a
universal one -- treat any other deployment's correct value as unknown until it
runs its own check.

### Tool bug fixed: was printing a verdict from zero captured packets

The first live run of `cmd/endianness-check` captured **zero** RTP packets
(`n=0`) and still printed a confident-looking verdict, because `analyze()` scored
two empty sample slices as equally "smooth" (`0.0 <= 0.0`, which
`Verdict.LittleEndian()` resolves to `le`) with nothing to distinguish them.
Root cause was a real deadlock in `probe()`: the tone-sending goroutine calls
`Endpoint.WriteTo`, which (correctly, for the production `CallMedia` path -- see
the port-binding invariant in `CLAUDE.md`) refuses to send until `LockRemote` has
fired from an inbound packet -- but the reader goroutine only called
`LockRemote` after successfully parsing non-RTCP RTP audio. If Asterisk's first
traffic on that leg is RTCP-only (or nothing, until it has real content to mix),
that gate can never open: our tone can't reach Asterisk without a lock that only
an already-flowing stream would produce. Fixed two ways:

- `probe()` now locks the remote address from **any** inbound datagram on that
  leg's muxed RTP/RTCP socket, not only a successfully-parsed RTP payload --
  since RTP and RTCP share one port here, an RTCP packet's source address is
  already the right one to send the tone to.
- The tool now fails loudly instead of guessing: `requireCaptured` returns `no
  RTP captured — probe did not flow` if zero bytes were captured, and every run
  logs per-stage packet counts (`raw_datagrams`, `rtcp_filtered`, `parse_failed`,
  `rtp_packets_captured`) so a future zero-capture run points at exactly where
  the flow stopped instead of failing mysteriously.

Covered by `TestAnalyze_LittleEndianTone_ScoresLittleEndianSmoother`,
`TestAnalyze_BigEndianTone_ScoresBigEndianSmoother` (synthetic tones, both byte
orders), and `TestRequireCaptured_*` (zero-capture hard error) in
`cmd/endianness-check/main_test.go`.

Run `cmd/endianness-check` against a live `make up` stack:

```bash
go run ./cmd/endianness-check
```

It creates two externalMedia channels -- one in slin16 (where it sends a 2s 440Hz
tone and captures the result) and one in ulaw (a different codec, forcing Asterisk
to genuinely transcode when it bridges the two, exactly as it does for a real SIP
call) -- bridges them, blindly relays whatever bytes arrive on the ulaw channel
straight back out (no need to understand ulaw: Asterisk does the actual decode/
re-encode on both sides), and scores what comes back on the slin16 channel under
both byte-order interpretations by mean absolute sample-to-sample delta -- a real
waveform has small deltas, and byte-swapping a smooth waveform scrambles it into
large ones, so the lower-scoring interpretation is the correct one. On success it
prints its verdict, an `AUDIO_L16_ENDIANNESS=le`/`be` line ready to paste into the
service's environment, and appends the verdict below.

(An earlier version bridged a single externalMedia channel with a dialplan
`Echo()` leg. That doesn't work: ARI's bridge API only accepts channels under
Stasis application control, and a channel running `Echo()` in the dialplan isn't
-- `AddChannel` fails with `400 Bad Request`. Being in Stasis and running a
dialplan app are mutually exclusive for the same channel, so the two-externalMedia
design isn't an optimization, it's the only way this works at all.)

<!-- cmd/endianness-check appends its result below this line; do not hand-edit -->
