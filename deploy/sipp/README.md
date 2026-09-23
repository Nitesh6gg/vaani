# Media Load Test (SIPp)

`uac_pcap.xml` establishes a call, plays a real RTP audio pcap into it, and holds
for the call duration. Combined with SIPp's `-rtp_echo` flag (SIPp echoes back
whatever RTP it receives), this becomes a self-sustaining closed audio loop for
the whole call:

```
SIPp --(RTP)--> Asterisk --(externalMedia: RTP or AudioSocket)--> Vaani (echo)
  ^                                                                        |
  |----------------------- Asterisk --(RTP)---------------------------<---+
```

This is real bidirectional media the whole way through -- unlike
`cmd/loadgen` (control-plane only, no audio), this is what validates the actual
Phase 2 hardening: jitter buffer behavior, byte-order normalization, and the
media plane under real concurrent load.

**This scenario tests both `MEDIA_ENCAPSULATION` transports with zero changes.**
SIPp only ever speaks SIP/RTP to *Asterisk* -- it has no visibility into whether
Vaani's externalMedia leg to Asterisk is RTP/UDP or AudioSocket/TCP, since that
leg is entirely internal between Asterisk and Vaani. To load-test AudioSocket
instead of RTP, set `MEDIA_ENCAPSULATION=audiosocket` in Vaani's environment
before starting it, then run the exact same `sipp` command below -- just read
the AudioSocket-specific metrics in "Reading the results" instead of the RTP
ones. Run it once per transport if you want numbers for both; nothing in this
file or `uac_pcap.xml` needs to change either way.

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
times) against Vaani's `:9091/metrics` taken during the same window. Which
counters matter depends on which `MEDIA_ENCAPSULATION` Vaani was running with
for this call.

### RTP (`MEDIA_ENCAPSULATION=rtp`, default)

- `vaani_rtp_malformed_total` -- should stay 0; a misconfigured packetization
  corrupting the pipeline shows up here.
- `vaani_rtp_silence_inserted_total` / total released frames ≈ loss %
- `vaani_rtp_late_total` + `vaani_rtp_duplicates_total` + `vaani_rtp_out_of_window_total`
  / inbound packets ≈ late+duplicate+out-of-window %
- `vaani_jb_reanchors_total` -- should stay near 0 on a healthy network; sustained
  growth means the jitter buffer is fighting real loss, not just absorbing jitter.

### AudioSocket (`MEDIA_ENCAPSULATION=audiosocket`)

No jitter buffer exists on this transport (TCP already guarantees in-order,
lossless delivery -- see "AudioSocket Transport" in `docs/AUDIO_PIPELINE.md`),
so there's no late/duplicate/out-of-window/reanchor equivalent to check. Instead:

- `vaani_audiosocket_malformed_total` -- should stay 0; counts frames with an
  unexpected size.
- `vaani_audiosocket_send_errors_total` -- should stay 0; a nonzero count means
  the TCP connection is failing writes (fatal to that call's media -- see the
  partial-write-desync fix in `internal/media/audiosocket.go`).
- `vaani_audiosocket_frames_in_total` / expected frame count (60s ÷ 20ms × calls)
  ≈ how much inbound audio actually arrived.

### Shared, either transport

- `vaani_pacer_drift_ms` histogram, p50/p99 (via `histogram_quantile` if scraped
  into Prometheus, or read the bucket counts directly from `/metrics`)
- `vaani_audio_rms` -- should be well above 0 throughout a call carrying the
  pcap's real audio; pinned at 0 for the whole call means no real audio ever
  arrived (see the near-silence teardown warning in `docs/AUDIO_PIPELINE.md`).
- `vaani_media_watchdog_timeouts_total` -- should stay 0 during the run.
- `GET :9091/healthz` -- its `active_calls` field should track SIPp's
  concurrent-call count in real time and drop back to 0 shortly after the run ends.
- Process CPU/RSS: `ps -o %cpu,rss -p <pid>` (Linux) sampled during the run.
- Goroutine count before/after: `curl localhost:9091/debug/pprof/goroutine?debug=1`.

Record real numbers from an actual run in `docs/LOADTEST.md` -- never fabricate
these.
