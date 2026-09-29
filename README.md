# Vaani — Go + Asterisk ARI Voice AI Agent

Real-time voice agent: Asterisk ARI (telephony) + Go media plane + Sarvam STT/TTS + OpenAI-compatible LLM.

## Stack
- Go 1.26+, Asterisk 22.8+ (PJSIP, ARI, AudioSocket/externalMedia)
- STT: Sarvam `saaras:v4` (WebSocket, 16kHz PCM s16le)
- TTS: Sarvam Bulbul v3 (WebSocket streaming, request 16kHz output)
- LLM: OpenAI-compatible streaming API (shared http.Client, HTTP/2)
- Barge-in VAD: TEN VAD (`VAD_MODE=ten`, Linux/cgo, vendored under
  `third_party/ten-vad/`) or RMS energy (`VAD_MODE=energy`, default)

## Agent mode
`APP_MODE=agent` turns the loopback pipeline into a full voice agent: Sarvam
STT → OpenAI-compatible LLM → Sarvam TTS, with caller interruption (barge-in)
detectable by TEN VAD or a plain RMS gate. The agent itself — workflow,
prompts, tools, and the LLM/STT/TTS models and keys — is configured in a
self-hosted Dograh and read from its Postgres per call; Vaani needs only
`DOGRAH_DB_URL` and `DOGRAH_WORKFLOW_ID` (validated at startup). Every knob
is documented in `.env.example`, and the barge-in on/off + tuning knobs in
`docs/AI_PROVIDERS.md`.

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
