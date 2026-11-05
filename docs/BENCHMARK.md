# Vaani Benchmark Log

Measured results of load tests on the Vaani engine. **Update this file after every test** (add a row to each table and a section under "Test log"). Never put an estimate in a table of measured numbers: estimates go in "Capacity estimates" and say so.

## Deployment under test

| Component | Where |
|---|---|
| Vaani engine | **192.168.36.19 — alone on the box**: 16 cores (`GOMAXPROCS` = 16), 32 GB RAM |
| Asterisk (ARI + AudioSocket) | separate server |
| Postgres (Dograh DB) | separate server |
| Redis | separate server |
| Dograh API/UI, Bifrost, SigNoz | separate (not on the Vaani box) |

So the Vaani process's CPU and memory are the whole load on that machine: the CPU budget is 16 cores, RAM 32 GB. Asterisk CPU, the SIP trunk, Postgres and Redis load are **not** visible from Vaani's metrics and are not covered here (check them on their own servers during a test).

Providers during these tests: STT Sarvam; TTS Sarvam Bulbul; LLM Sarvam sarvam-105b (Oct 27–31), Gemini 3.1 Flash-Lite via Bifrost (Nov 2).

## How the numbers are measured

Source: SigNoz, service `vaani-engine`.

| Column | Metric / formula |
|---|---|
| Answered calls | `increase(vaani_call_duration_seconds.count)` over the window |
| Talk time | `increase(vaani_call_duration_seconds.sum)` over the window |
| Peak talking | max of `vaani_calls_active` |
| Peak in flight | max of `vaani_campaign_calls_in_flight` (includes ringing) |
| Peak CPU | max 1-minute `rate(process_cpu_seconds_total)`, in cores |
| **Core per talking call** | `increase(process_cpu_seconds_total)` over the window ÷ talk time (seconds). Counts *all* Vaani CPU in the window (dialing, ringing, post-call work), so it is the honest per-call figure |
| Peak RSS / heap / goroutines | max of `process_resident_memory_bytes` / `go_memstats_heap_inuse_bytes` / `go_goroutines` |

Do **not** divide a one-minute CPU peak by the instantaneous peak call count: that understates the cost (it gave a wrong 0.027 core/call on Nov 2; per-minute CPU lags the call count: CPU ÷ average active calls was 0.036 in the peak minute and 0.053 / 0.071 / 0.075 in the next minutes as calls ended, so work after calls end is plausible but was not profiled).

Always query **one window total** (scalar, `increase` over the whole window), never sum hourly time-series buckets: bucketed `increase` is unreliable around process restarts (Oct 31: buckets summed to 861 s of talk time, the window total and the logs say ~5,150 s).

The engine is a single instance (`192.168.36.19:9091`), so nothing is double counted. Metrics are scraped every 10 s (360 samples/hour), so "peak" values are 10-second samples and can miss a shorter spike. The engine is restarted by hand before each test (start times Oct 27 05:23 and 11:35 UTC, Oct 28 09:41, Oct 31 09:41 UTC — inside that window, before any call —, Nov 2 10:34); the idle baseline (0.002 core ≈ 7.5 CPU-seconds per idle hour) is included in every CPU total.

## Results (measured)

Times are IST. Windows are 1 hour (Oct 27 evening: 30 min).

| Test (IST) | LLM | Answered | Talk time | Peak talking | Peak in flight | Peak CPU (cores / % of 16) | **Core / talking call** | Peak RSS | Peak heap | Peak goroutines |
|---|---|---|---|---|---|---|---|---|---|---|
| Oct 27 11:00 | Sarvam | 29 | 16 min | 11 | 20 | 0.67 / 4.2% | 0.085 | 130 MB | 65 MB | 186 |
| Oct 27 18:00 | Sarvam | 62 | 43 min | 11 | 20 | 0.62 / 3.9% | 0.079 | 140 MB | 67 MB | 186 |
| Oct 28 16:00 | Sarvam | 137 | 72 min | 16 | 35 | 0.78 / 4.9% | 0.070 | 182 MB | 95 MB | 257 |
| Oct 31 15:00 | Sarvam | 116 | 86 min | 23 | 41 | 1.08 / 6.7% | 0.062 | 229 MB | 127 MB | 360 |
| Nov 2 16:30 (campaign 19) | Gemini | 300 | 146 min | 53 | 136 | 1.41 / 8.8% | 0.058 | 428 MB | 228 MB | 761 |
| Nov 3 14:55–15:28 (campaign 20) | Gemini | 426 | 234 min | ~52 (1-min avg) | 132 | 1.84 / 11.5% | 0.044 | 485 MB | n/a | 918 |
| Nov 4 17:05–17:25 (campaign "test_up_2000", paused at 391 dials) | sarvam/gemma 4 | 216 | 152 min | 63 (1-min max) | 129 | 1.81 / 11.3% | 0.044 | 534 MB | n/a | 989 |

Idle Vaani: 0.002 core, ~51 MB RSS, 19 goroutines, 23 OS threads (constant under load).
After a run RSS stays high (~300 MB on Nov 2) because Go keeps freed memory; the heap itself returned to ~32 MB within ~6 minutes, so there is no leak.

Trend: cost per call falls as concurrency grows (fixed overhead is shared).

### Cross-check against logs (verified Nov 3)

The per-call `call.summary` log (`duration_s` is whole seconds) agrees with the metrics: calls match exactly, talk time within about 2%.

| Test | Calls (metric / logs) | Talk time s (metric / logs) |
|---|---|---|
| Oct 27 11:00 | 29 / 29 | 959.7 / 949 |
| Oct 27 18:00 | 62 / 62 | 2577 / 2556 |
| Oct 28 16:00 | 137 / 137 | 4294 / 4230 |
| Oct 31 15:00 | 116 / 116 | 5155 / 5115 |
| Nov 2 16:30 | 300 / 300 | 8779 / 8593 |

"Talking call" = a call bridged to the media plane (`vaani_calls_active`), which includes the ~19% of answered calls where the caller said nothing.

### Network (process total, all links combined)

`process_network_*` counters; includes Asterisk audio, Sarvam, Bifrost/LLM, Dograh recording upload and DB/Redis traffic.

| Test | Received | Sent | Per talking call, received | Per talking call, sent |
|---|---|---|---|---|
| Oct 28 16:00 | 242 MB | 423 MB | 56 kB/s (451 kbps) | 98 kB/s (788 kbps) |
| Oct 31 15:00 | 290 MB | 513 MB | 56 kB/s (451 kbps) | 100 kB/s (797 kbps) |
| Nov 2 16:30 | 442 MB | 843 MB | 50 kB/s (403 kbps) | 96 kB/s (768 kbps) |
| Nov 3 14:55 (campaign 20) | 712 MB | 1344 MB | 51 kB/s (406 kbps) | 96 kB/s (767 kbps) |
| Nov 4 17:05 (test_up_2000) | 431 MB | 917 MB | 47 kB/s (378 kbps) | 100 kB/s (803 kbps) |

Stable across three tests. Extrapolated to 250 talking calls (not measured): ~100 Mbps received, ~190 Mbps sent in total. Inference (not measured): the sent side is about 32 kB/s of audio to Asterisk + gated STT audio + the call recording upload (`recording_bytes` is ~32 kB per call-second). Oct 27 windows not re-queried.

## Health and latency per test

| Test | Agent errors | LLM retries / fallbacks | Dropped audio frames | Pacer drift p99 | LLM TTFT p50 / p95 | TTS TTFA p50 / p95 | Media watchdog timeouts |
|---|---|---|---|---|---|---|---|
| Nov 2 (campaign 19) | 0 | 0 / 0 | 0 | 0.495 ms | ~1.2 s / ~3 s (Gemini) | ~1.1 s / ~3 s | 6 (3 calls hung up) |
| Nov 3 (campaign 20) | 0 | 0 / 0 | 0 | mean 0.003 ms (p99 not pulled) | mean 1.22 s (Gemini; p50/p95 not pulled) | mean 0.92 s | 21 (cause not checked) |
| Nov 4 (test_up_2000, gemma 4) | 6 (llm 4, tts_drain_timeout 2) | 44 (39 recovered, 5 exhausted) / 4 | 0 | not pulled | mean 5.0 s; 15% ≤1.6 s, 37% ≤3.2 s, 70% ≤6.4 s, 98% ≤12.8 s (p50 ~4–5 s, p95 ~10–12 s, read from histogram buckets) | mean 3.8 s | 5 |

Reference, Oct 28 (Sarvam LLM): LLM TTFT p50 ~200 ms / p95 ~390 ms; TTS TTFA p95 ~760 ms; STT endpoint p95 ~100–390 ms. Bifrost dashboard (Nov 1–2): gemini-3.1-flash-lite avg 1.08 s per request, 99.7% success; sarvam-105b avg 187 ms. Older tests' errors / drift were not re-pulled: fill the blanks in when re-checking.

## Test log

### Nov 2, 2026 — campaign 19 (peak load so far)

- Phase 1 (rate 1/s): 327 dials, peak 38 in flight, 18 talking. Phase 2 (`rate_limit_per_second` 5): 454 dials, ~4.6 dials/s sustained for about a minute, then the contact list ran out; a retry round followed after the 120 s delay.
- Peak 136 in flight, 53 talking. `max_concurrency` 300, org limit 200: neither was reached.
- Outcomes in the fast phase: no-answer 227, connected 156, rejected 42, failed 29 (21 cause 1 "Unallocated number", 8 cause 38 "Network out of order"), busy 0. Whole window: 300 answered (~38%), average ~29 s per call.
- STT audio billed: 4607 s for 8779 s of calls (speech-only gating, ~52%).
- Findings: Vaani used under 9% of its 16 cores; LLM TTFT (Gemini) is the quality problem, not capacity; recording `34bk399q` still missing (80 fallbacks to spoken text); 57 calls (19%) with near-silent caller audio.

### Nov 3, 2026 — campaign 20 (`test_1000`, first test with the org dial rate)

- Org settings: concurrency limit 200, **dial rate 20** (superadmin modal; `campaigns.rate_limit_per_second` stayed 1 and was overridden). 1000 contacts; retries 1 (120 s delay), so 1429 dials in all (1000 + 429 retries). Metric dials = Dograh total = logs.
- Two runs: ~14:57 IST (49 dials in ~11 s, then paused by hand: no `campaign.paused` log), resumed ~15:16, finished ~15:24 (the retry round after it).
- **The dial rate setting works but is not the limit**: dials were ~0.22 s apart, ~4.6/s (not 1/s, not 20/s). One dial loop per campaign does each dial in sequence (DB lookups plus the ARI originate), so a single campaign is capped near 4.5 dials/s whatever the rate. Not profiled which step takes the time. Peak in flight plateaued at ~130, consistent with ~4.5/s × 30 s ring timeout (inference).
- Outcomes (1429 dials): no-answer 743, rejected (cause 21) 152, failed 107 (cause 1 unallocated 102, cause 38 5), busy 1, answered 426 (29.8%). Of the answered: 300 caller hung up, 92 qualified, 34 idle timeout. Average 33 s per call.
- Peak talking did **not** exceed Nov 2 (~52): the answer rate (30%) and short calls (33 s) cap talking calls, not the dialer. To reach ~120 talking calls a campaign needs a higher answer rate or longer calls, or several campaigns dialing at once.
- Core per talking call fell again, 0.058 → 0.044 (619 CPU-s ÷ 14,012 talk-s). Peak CPU 1.84 cores (11.5% of 16) at the peak minute.
- STT audio 7307 s for 14,012 s of calls (52%, speech-only gating). 0 agent errors, 0 LLM retries/fallbacks, 0 dropped frames. Media watchdog timeouts 21 (was 6): not investigated.
- Not pulled: heap, p50/p95 of LLM/TTS, pacer p99, log cross-check of talk time.

### Nov 4, 2026 — campaign "test_up_2000" (LLM sarvam/gemma 4)

- 2000 contacts, paused by hand after 391 dials (~4 min of dialing, 17:08–17:13 IST): 391 dials = Dograh's total; 216 answered (55.2%), 119 no-answer, 54 rejected (cause 21), 2 failed (cause 1), 0 busy. Answered: 184 caller hung up, 26 qualified, 6 idle timeout. Average 42 s.
- Best contact list so far: answer rate 55% (campaign 20: 30%), only 2 invalid numbers (campaign 20: 102). Result: **63 talking calls**, the highest yet.
- Peak CPU 1.81 cores (11.3% of 16); core per talking call 0.044 (401 CPU-s ÷ 9131 talk-s), same as Nov 3. RSS 534 MB, 989 goroutines.
- **The LLM is the problem, not capacity**: gemma 4 TTFT mean 5.0 s (Gemini: 1.2 s), only 37% under 3.2 s; 44 retried requests (9%), 5 exhausted, 4 turns answered with the fallback line; TTS first audio mean 3.8 s (it includes the LLM time). 2 `tts_drain_timeout` errors. Recording `34bk399q` still missing (`tts.clip_missing` warnings).
- STT audio 5685 s for 9131 s of calls (62%). 0 dropped frames. Media watchdog timeouts 5.
- Window is approximate (metrics queried 17:00–17:30 IST); heap and pacer p99 not pulled.

## Capacity estimates (NOT measured)

Extrapolated from 0.058 core/talking call, linear, Vaani box only (Asterisk, DB, Redis are elsewhere):

| CPU target | Talking calls | Note |
|---|---|---|
| 70% (11.2 cores) | ~190 | recommended planning point |
| 90% (14.4 cores) | ~250 | at 90% the 20 ms pacer risks slipping |

Caveats on these estimates (checked Nov 3):
- The cost per talking call is **not constant**: it fell from 0.085 to 0.058 as load rose, and a straight-line fit of CPU against calls and talk time does not match the five tests (errors of 3–20%). At 250 calls it could be lower (economy of scale) or higher (GC/scheduler contention near saturation). Only a test settles it.
- **vCPU layout (lscpu, Nov 3):** Intel Xeon Gold 6330 @ 2.00 GHz, VMware guest, 16 vCPUs shown as 8 sockets × 2 cores × 1 thread. The guest sees no hyper-threading, but that is a virtual layout: whether the host maps these vCPUs onto hyper-threads of shared physical cores is **unknown** (ask the VMware admin: CPU reservation, host overcommit, HT). If they share cores, or the host is busy, the real limit is below ~250. A neighbour VM's load would not show in Vaani's metrics.
- Engine CPU only; the box has no other load, but Asterisk/trunk are separate and untested at this scale.

Memory is not the constraint: ~4 MB heap per talking call, so 250 calls is roughly 1–1.5 GB of 32 GB. The largest tested load is 53 talking calls, so these are ~5x extrapolations; the true cost per call has been falling with load, so the real number may be higher. Treat them as unproven until a test reaches them.

## Next test (planned)

Reach ~120 talking calls: dial rate 8–10 calls/s (superadmin → Organizations → Concurrency; see `docs/OUTBOUND.md`), ~2,000 contacts, `max_concurrency` 300 (check the org limit too). During the run record on the Vaani box (`top`/`htop`): total CPU, `vaani_pacer_drift_ms` p99, dropped frames, errors; and, on their servers, Asterisk CPU and channels, trunk CPS and rejects, Postgres and Redis load; Sarvam stream errors and Gemini 429s. Stop and note the point where drift p99 exceeds ~5 ms, frames drop, or errors appear.

## Limits outside Vaani (to confirm, not measured)

SIP trunk calls-per-second and max channels; Sarvam STT/TTS concurrent streams on the plan; Gemini requests-per-minute quota at the current tier; Asterisk CPU and the Asterisk-to-Vaani link (the raw audio alone is ~256 kbps per call each way; the engine's total network per call is in the Network table above).
