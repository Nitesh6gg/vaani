# Vaani — Agent Guidance

Real-time voice agent: Asterisk ARI (telephony) + Go media plane + Sarvam STT/TTS + OpenAI-compatible LLM.

## Stack
- Go 1.26+, Asterisk 22.8+ (PJSIP, ARI, externalMedia)
- Agent configuration: a self-hosted Dograh, read from its Postgres per call
  (workflow, prompts, tools, models, keys) — no model settings in `.env`
- STT: Sarvam, e.g. `saaras:v4` (WebSocket, 16kHz PCM s16le; model set in Dograh)
- TTS: Sarvam Bulbul, e.g. v3 (WebSocket streaming, request 16kHz output; set in Dograh)
- LLM: OpenAI-compatible streaming API set in Dograh (shared http.Client, HTTP/2)
- Barge-in VAD: TEN VAD (`VAD_MODE=ten`, Linux/cgo, vendored under
  `third_party/ten-vad/`) or RMS energy (`VAD_MODE=energy`, default)

## Package Layout
- `cmd/server/` — single entry point (monolith, NOT microservices)
- `internal/ari/` — ARI WebSocket client, Stasis handlers
- `internal/media/` — RTP over UDP and AudioSocket over TCP (both via ARI
  externalMedia), port allocator, 20ms pacer, jitter buffer (RTP only), sync.Pool
- `internal/ai/stt|llm|tts/` — provider clients
- `internal/ai/agent/` — per-call agent Handler (STT→LLM→TTS state machine,
  barge-in, playout queue); `internal/ai/agent/tenvad/` — TEN VAD cgo wrapper
  (Linux only; native library vendored under `third_party/ten-vad/`);
  `workflow.go` — workflow nodes/edges; `tools.go` — tool kinds
- `internal/dograh/` — read-only access to Dograh's Postgres: workflow graph,
  `{{variable}}` rendering, tools, models/keys, call limits
- `internal/session/` — per-call state machine, barge-in orchestration
- `assets/` — embedded transfer sounds (hold ring, beep)

## Critical Invariants (NEVER violate these)
1. **20ms pacer**: use `time.NewTicker` (`internal/media/pacer.go`) and pass each
   tick's own fire time to the callback — never a computed "next deadline"
   variable. Never `time.Sleep(20 * time.Millisecond)`: unlike a ticker, it has no
   built-in coalescing, so after any stall it delivers a burst of catch-up sleeps
   instead of just costing the missed ticks.
2. **Media path: ARI externalMedia**, selectable via `MEDIA_ENCAPSULATION`: `rtp`
   (default, `transport=udp`, `format=slin16`) or `audiosocket` (`transport=tcp`,
   Asterisk's res_audiosocket framed protocol — see `internal/media/audiosocket.go`).
   This reverses an earlier "AudioSocket is permanently out of scope" decision from
   Phase 1 — that's no longer true; both transports are supported and tested.
3. **sync.Pool for frames**: 640-byte buffers (16kHz × 2 bytes × 20ms). No `make([]byte)` in hot path.
4. **No goroutine per frame**: one reader goroutine + one writer goroutine per call,
   connected by buffered `chan []byte` (capacity ~5).
5. **Shared http.Client**: one global client with `MaxIdleConnsPerHost=200`,
   `ForceAttemptHTTP2=true`. Never create per-request clients.
6. **One ARI WebSocket** for the whole process — never per-call.
7. **Barge-in is 5 cuts**: pacer stop, STT cancel, LLM cancel, TTS stream close, queue flush.
   Missing any one causes leaked audio or billing waste. Phase 4 resolution of the
   old "pacer stop vs RTP must never starve" tension: the write tick keeps firing
   and emits silence per invariant #1/#9 — "pacer stop" is implemented as *stop
   feeding the outbound queue*, i.e. the queue drain + playback state flip in
   `internal/ai/agent/handler.go`, together with STT/LLM context cancellation,
   TTS connection teardown, and `ttsBuf` clearing.
8. **Audio format**: slin16 (16kHz 16-bit mono PCM) end-to-end. Asterisk handles μ-law transcoding.
9. **One media socket per call**, bound to a port from the managed pool (never shared
   across calls), and bound/listening *before* the externalMedia channel is created —
   telling Asterisk an address before something is listening on it caused a real
   production incident (RTP hitting a closed UDP port triggers an ICMP
   port-unreachable that can kill the flow across a routed/firewalled path). For RTP,
   also lock the remote address to the source of the first valid inbound packet.
10. **RTP framing** (RTP only): sequence +1 and timestamp +320 per 20ms packet @
    16kHz; validate version 2 on inbound, and drop/count any inbound payload
    that isn't exactly 640 bytes — the whole pipeline has exactly one frame
    shape. Payload endianness (s16le vs s16be) is verified empirically via
    `cmd/endianness-check` — see `docs/AUDIO_PIPELINE.md` before assuming
    either; the verified order applies SYMMETRICALLY (handler output is LE by
    contract and must be converted back to the wire order on the way out).
    AudioSocket has neither sequencing nor an endianness question: TCP
    guarantees order, and its audio payload is little-endian by protocol
    definition.
11. **AudioSocket frames go out in ONE `conn.Write`** (`internal/media/audiosocket.go`):
    header and payload written separately can land in separate TCP packets, and
    Asterisk's res_audiosocket allows only 5ms between reading a frame's header
    and its payload before it hangs the channel up ("Poll timed out while
    waiting for data" — a real cross-subnet production incident, invisible on
    localhost).

## Conventions
- Errors: wrap with `fmt.Errorf("...: %w", err)`, never panic in request path.
- Logging: structured (`log/slog`), include `call_id` in every log line.
- Tests: table-driven, `testify/assert`. Integration tests skip if `ASTERISK_HOST` unset.

## Docs Index
- System design: `docs/ARCHITECTURE.md`
- Asterisk setup: `docs/SETUP.md`
- AI provider specs: `docs/AI_PROVIDERS.md`
- Media pipeline: `docs/AUDIO_PIPELINE.md`
- Build phases: `PLAN.md`

## Testing
- Unit: `go test ./...`
- Load: `sipp -sf deploy/sipp/uac_pcap.xml <asterisk_host>`
- Verify audio: `tcpdump -i lo -w /tmp/call.pcap port 9092`

## Workflow Tip for Agentic Development
Structure `PLAN.md` as a sequence of **verifiable milestones** rather than a feature list —
agents work best when each task has a clear "done" signal.
