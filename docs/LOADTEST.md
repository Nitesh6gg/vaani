# Load Test Results

**Status: not yet run, for either transport.** The tables below are the template
Phase 2's acceptance criteria require filling in with real numbers from an
actual run against a live Asterisk stack (see `deploy/sipp/README.md` and
`cmd/loadgen`) -- this file intentionally contains no fabricated data. Replace
this section once a run has been done, and note the date, hardware, and
Asterisk/Vaani versions involved.

## Media load test (SIPp, 25 concurrent calls x 30s)

Command used (identical for both transports -- SIPp only ever speaks SIP/RTP to
*Asterisk*, so it's transparent to which `MEDIA_ENCAPSULATION` Vaani's
externalMedia leg uses; see `deploy/sipp/README.md`). Hold duration (30s) is a
fixed literal in `uac_pcap.xml`, not a command-line flag -- there's no SIPp
substitution keyword for that, despite an earlier version of this doc claiming
otherwise:
```
sipp <asterisk_host>:5060 -sf deploy/sipp/uac_pcap.xml \
  -mi <sipp_media_ip> -s <extension> -l 25 -r 1 -rp 1000 -rtp_echo
```

### Transport: RTP (`MEDIA_ENCAPSULATION=rtp`, default)

| Metric | Target | Actual |
|---|---|---|
| Calls completed | 25 | |
| Crashes | 0 | |
| Malformed RTP (`vaani_rtp_malformed_total`) | 0 | |
| Loss (silence-inserted / inbound packets) | < 0.5% | |
| Late + duplicates + out-of-window (of inbound packets) | < 1% | |
| Jitter buffer re-anchors (`vaani_jb_reanchors_total`) | ~0 | |
| Pacer drift p50 (`vaani_pacer_drift_ms`) | -- | |
| Pacer drift p99 | ≤ 2ms | |
| Ports in use after teardown (`vaani_media_ports_in_use`) | 0 | |
| Goroutine count: baseline | -- | |
| Goroutine count: after teardown | == baseline | |
| Total CPU (process, over the run) | -- | |
| Peak RSS | -- | |
| CPU per call (estimate) | -- | |
| RSS per call (estimate) | -- | |

### Transport: AudioSocket (`MEDIA_ENCAPSULATION=audiosocket`)

No jitter buffer/late/duplicate/out-of-window/reanchor concepts apply here (TCP
already guarantees in-order, lossless delivery) -- see "AudioSocket Transport"
in `docs/AUDIO_PIPELINE.md`.

| Metric | Target | Actual |
|---|---|---|
| Calls completed | 25 | |
| Crashes | 0 | |
| Malformed frames (`vaani_audiosocket_malformed_total`) | 0 | |
| Send errors (`vaani_audiosocket_send_errors_total`) | 0 | |
| Frames in vs. expected (30s ÷ 20ms × 25 calls) | ~100% | |
| Pacer drift p50 (`vaani_pacer_drift_ms`) | -- | |
| Pacer drift p99 | ≤ 2ms | |
| Sockets/listeners leaked after teardown | 0 | |
| Goroutine count: baseline | -- | |
| Goroutine count: after teardown | == baseline | |
| Total CPU (process, over the run) | -- | |
| Peak RSS | -- | |
| CPU per call (estimate) | -- | |
| RSS per call (estimate) | -- | |

## Control-plane load test (`cmd/loadgen`)

Command used:
```
go run ./cmd/loadgen -n 128 -ramp 10 -hold 5s
```
(Note: each origination drives ~2 Manager-tracked calls via the Local channel
double-entry -- see the comment in `deploy/asterisk/extensions.conf` -- so `-n
128` stresses roughly 256 concurrent bridges.)

Also transport-agnostic: it drives whatever `MEDIA_ENCAPSULATION` Vaani is
configured with, so the same command exercises AudioSocket's TCP accept/
teardown lifecycle under concurrency too -- but per its own doc comment it
carries no real media either way, so it says nothing about audio quality for
either transport, only whether the control plane and port/socket allocator
hold up.

| Metric | Target | Actual |
|---|---|---|
| Requested | 128 | |
| Succeeded | 128 | |
| Failed | 0 | |
| Ports in use after teardown | 0 | |
| Goroutine count: after teardown | == baseline | |

## SIGTERM-during-load test

| Metric | Target | Actual |
|---|---|---|
| Process exit time after SIGTERM | ≤ 10s | |
| Port leaks | 0 | |
| ARI channels left hung | 0 | |

## Hardware / environment

- CPU:
- RAM:
- OS / kernel:
- Go version:
- Asterisk version:
- Network path between Vaani and Asterisk (same host / same LAN / VPN):

## Why this number matters

This is the number that justifies building Vaani instead of using an existing
framework like Pipecat or LiveKit: CPU and memory per call on real hardware,
measured directly rather than inferred from a managed platform's billing.
