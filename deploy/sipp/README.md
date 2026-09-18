# Media Load Test (SIPp)

`uac_pcap.xml` establishes a call, plays a real RTP audio pcap into it, and holds
for the call duration. Combined with SIPp's `-rtp_echo` flag (SIPp echoes back
whatever RTP it receives), this becomes a self-sustaining closed audio loop for
the whole call:

```
SIPp --(RTP)--> Asterisk --(externalMedia RTP)--> Vaani (echo)
  ^                                                          |
  |------------------- Asterisk --(RTP)------------------<---+
```

This is real bidirectional media the whole way through -- unlike
`cmd/loadgen` (control-plane only, no audio), this is what validates the actual
Phase 2 hardening: jitter buffer behavior, byte-order normalization, and the
media plane under real concurrent load.

## Prerequisites

- `sipp` installed on a machine that can reach the Asterisk box's SIP port.
- A pcap of real G.711a RTP. Debian/Ubuntu SIPp packages ship one at
  `/usr/share/sipp/pcap/g711a.pcap`; if yours doesn't have it, record your own
  from a real call (`tcpdump -i any -w mycall.pcap udp portrange 10000-19999`
  during a call, then trim to just the RTP stream) and place it at
  `pcap/g711a.pcap` relative to wherever you invoke `sipp` from.
- A dialplan extension on the Asterisk box that reaches `Stasis(vaani)` -- reuse
  `1001` (`deploy/asterisk/extensions.conf`) or whatever your real server routes
  into your app (see the ARI app collision notes in project history if you're
  pointing this at a server that also runs other ARI apps).

## Running 25 concurrent calls, 60s each

```bash
sipp <asterisk_host>:5060 \
  -sn uac_pcap -sf deploy/sipp/uac_pcap.xml \
  -mi <sipp_media_ip> \
  -s <extension_that_reaches_stasis_vaani> \
  -d 60000 \
  -l 25 -r 1 -rp 1000 \
  -rtp_echo
```

- `-mi <sipp_media_ip>`: the IP SIPp advertises in its own SDP for RTP -- must be
  reachable from the Asterisk box, same constraint as Vaani's own `MEDIA_IP`
  (see `internal/config/config.go`).
- `-s <extension>`: the number to dial (must land in `Stasis(vaani)`).
- `-d 60000`: hold each call 60s (fills the `[decimal]` pause in the scenario).
- `-l 25`: cap 25 concurrent calls.
- `-r 1 -rp 1000`: ramp at 1 call/second.
- `-rtp_echo`: SIPp echoes inbound RTP back out, closing the audio loop for the
  full call duration instead of just playing the pcap once and going silent.

## Reading the results

Cross-reference SIPp's own summary (successful calls, retransmissions, response
times) against Vaani's `:9091/metrics` taken during the same window:

- `vaani_rtp_silence_inserted_total` / total released frames ≈ loss %
- `vaani_rtp_late_total` + `vaani_rtp_duplicates_total` / inbound packets ≈
  late+duplicate %
- `vaani_pacer_drift_ms` histogram, p50/p99 (via `histogram_quantile` if scraped
  into Prometheus, or read the bucket counts directly from `/metrics`)
- Process CPU/RSS: `ps -o %cpu,rss -p <pid>` (Linux) sampled during the run
- Goroutine count before/after: `curl localhost:9091/debug/pprof/goroutine?debug=1`

Record real numbers from an actual run in `docs/LOADTEST.md` -- never fabricate
these.
