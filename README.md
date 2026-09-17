# Vaani — Go + Asterisk ARI Voice AI Agent

Real-time voice agent: Asterisk ARI (telephony) + Go media plane + Sarvam STT/TTS + OpenAI-compatible LLM.

## Stack
- Go 1.22+, Asterisk 22.8+ (PJSIP, ARI, AudioSocket/externalMedia)
- STT: Sarvam `saaras:v4` (WebSocket, 16kHz PCM s16le)
- TTS: Sarvam Bulbul v3 (WebSocket streaming, request 16kHz output)
- LLM: OpenAI-compatible streaming API (shared http.Client, HTTP/2)

## Getting Started
```bash
go build ./cmd/server
go test ./...
```
See `docs/SETUP.md` for Asterisk configuration.

## Docs
- System design: `docs/ARCHITECTURE.md`
- Asterisk setup: `docs/SETUP.md`
- AI provider specs: `docs/AI_PROVIDERS.md`
- Media pipeline: `docs/AUDIO_PIPELINE.md`
- Build phases: `PLAN.md`
- Agent/contributor guidance (architecture invariants, conventions): `CLAUDE.md` / `AGENTS.md`

## Testing
- Unit: `go test ./...`
- Load: `sipp -sf deploy/sipp/uac_pcap.xml <asterisk_host>`
- Verify audio: `tcpdump -i lo -w /tmp/call.pcap port 9092`
