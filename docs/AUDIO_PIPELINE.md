# Media Pipeline

Audio path: Asterisk PJSIP endpoint (ulaw) -> Asterisk transcodes -> externalMedia
RTP/UDP leg to the Go service, slin16 (16kHz 16-bit mono PCM) end-to-end on that leg.
See `internal/media/` for the RTP endpoint, pacer, buffer pool, and loop-back
handler, and `CLAUDE.md` for the invariants they implement.

## Endianness Verification

RTP L16 payloads are nominally big-endian per RFC 3551, but that's a generic
profile default, not a guarantee about a specific Asterisk build's externalMedia
implementation -- so it's checked empirically rather than assumed, since Phase 3
(STT) needs to know which byte order it's receiving.

Run `cmd/endianness-check` against a live `make up` stack:

```bash
go run ./cmd/endianness-check
```

It originates a dialplan `Echo()` leg (`deploy/asterisk/extensions.conf`, context
`vaani-tools`, extension `9001`), bridges it with a second externalMedia channel,
sends a 2s 440Hz tone as little-endian PCM16, and scores what echoes back under
both byte-order interpretations by mean absolute sample-to-sample delta -- a real
waveform has small deltas, and byte-swapping a smooth waveform scrambles it into
large ones, so the lower-scoring interpretation is the correct one. The tool prints
its verdict and appends it below.

<!-- cmd/endianness-check appends its result below this line; do not hand-edit -->
