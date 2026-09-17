# Vaani — Agent Guidance

Real-time voice agent: Asterisk ARI (telephony) + Go media plane + Sarvam STT/TTS + OpenAI-compatible LLM.

## Stack
- Go 1.22+, Asterisk 22.8+ (PJSIP, ARI, externalMedia)
- STT: Sarvam `saaras:v4` (WebSocket, 16kHz PCM s16le)
- TTS: Sarvam Bulbul v3 (WebSocket streaming, request 16kHz output)
- LLM: OpenAI-compatible streaming API (shared http.Client, HTTP/2)

## Package Layout
- `cmd/server/` — single entry point (monolith, NOT microservices)
- `internal/ari/` — ARI WebSocket client, Stasis handlers
- `internal/media/` — RTP over UDP, port allocator, 20ms pacer, jitter buffer, sync.Pool
- `internal/ai/stt|llm|tts/` — provider clients
- `internal/session/` — per-call state machine, barge-in orchestration

## Critical Invariants (NEVER violate these)
1. **20ms pacer**: use `time.NewTicker`, re-clamp deadline to `time.Now()` on every tick.
   Never `time.Sleep(20 * time.Millisecond)` — it drifts and bursts after stalls.
2. **Media path: ARI externalMedia**, `transport=udp`, `encapsulation=rtp`, `format=slin16`.
   AudioSocket is permanently out of scope for this project.
3. **sync.Pool for frames**: 640-byte buffers (16kHz × 2 bytes × 20ms). No `make([]byte)` in hot path.
4. **No goroutine per frame**: one reader goroutine + one writer goroutine per call,
   connected by buffered `chan []byte` (capacity ~5).
5. **Shared http.Client**: one global client with `MaxIdleConnsPerHost=200`,
   `ForceAttemptHTTP2=true`. Never create per-request clients.
6. **One ARI WebSocket** for the whole process — never per-call.
7. **Barge-in is 5 cuts**: pacer stop, STT cancel, LLM cancel, TTS stream close, queue flush.
   Missing any one causes leaked audio or billing waste.
8. **Audio format**: slin16 (16kHz 16-bit mono PCM) end-to-end. Asterisk handles μ-law transcoding.
9. **One UDP socket per call**, bound to a port from the managed pool (never shared across
   calls); lock the remote RTP address to the source of the first valid inbound packet.
10. **RTP framing**: sequence +1 and timestamp +320 per 20ms packet @ 16kHz; validate version 2
    on inbound. Payload endianness (s16le vs s16be) is verified empirically in Phase 1 —
    see `docs/AUDIO_PIPELINE.md` before assuming either.

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

<!-- Kept identical to CLAUDE.md. If you edit one, mirror the change in the other. -->
