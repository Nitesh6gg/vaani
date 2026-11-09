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

Providers during these tests: STT Sarvam; TTS Sarvam Bulbul; LLM Sarvam sarvam-105b (Oct 27–31), Gemini 3.1 Flash-Lite via Bifrost (Nov 2–3), sarvam/gemma 4 via Bifrost (Nov 4), sarvam/sarvam-105b-conversations via Bifrost (Nov 5).

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
| Nov 5 18:07–18:10 (campaign 21, "test_up_2000" resumed, paused after 726 dials) | sarvam-105b-conversations | 378 | 231 min | **94** (1-min max) | 179 | 2.49 / 15.5% | **0.036** | 696 MB | 447 MB | 1442 |
| Nov 6 14:50–14:55 (campaign 21 again, 1197 dials, paused by hand) | sarvam-105b-conversations | 631 | 261 min | 70 (1-min max) | 169 | 2.19 / 13.7% | 0.038 | 585 MB | 382 MB | 1120 |
| Nov 9 12:28 (campaign 21, **parallel dial workers**, contact list ran out: 299 dials) | sarvam-105b-conversations | 137 | 58 min | 73 (1-min max) | **266** | 1.03 / 6.5% (1-min avg; the dial burst lasted ~10 s) | 0.048 (short run, see log) | 512 MB | 314 MB | 1089 |
| Nov 9 16:30 (campaign 22, **475 dials in 10 s**, paused by the circuit breaker) | sarvam-105b-conversations | 150 | 5,167 s (86 min) | **94** (1-min max) | **468** | 1.50 / 9.4% (1-min avg) | **0.037** | not pulled | not pulled | not pulled |

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
| Nov 5 18:07 | 378 / 378 | 13,875 / 13,701 |
| Nov 6 14:50 | 631 / 631 | 15,650 / 15,431 |
| Nov 9 12:28 | not queried / 137 | 3,485 / 3,291 (6% apart, more than usual) |

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
| Nov 5 18:07 (campaign 21) | 794 MB | 1403 MB | 57 kB/s (458 kbps) | 101 kB/s (809 kbps) |
| Nov 6 14:50 (campaign 21) | 856 MB | 1542 MB | 55 kB/s (437 kbps) | 99 kB/s (789 kbps) |
| Nov 9 12:28 (campaign 21, parallel dialing) | 183 MB | 325 MB | 53 kB/s (420 kbps) | 93 kB/s (746 kbps) |

Stable across three tests. Extrapolated to 250 talking calls (not measured): ~100 Mbps received, ~190 Mbps sent in total. Inference (not measured): the sent side is about 32 kB/s of audio to Asterisk + gated STT audio + the call recording upload (`recording_bytes` is ~32 kB per call-second). Oct 27 windows not re-queried.

## Health and latency per test

| Test | Agent errors | LLM retries / fallbacks | Dropped audio frames | Pacer drift p99 | LLM TTFT p50 / p95 | TTS TTFA p50 / p95 | Media watchdog timeouts |
|---|---|---|---|---|---|---|---|
| Nov 2 (campaign 19) | 0 | 0 / 0 | 0 | 0.495 ms | ~1.2 s / ~3 s (Gemini) | ~1.1 s / ~3 s | 6 (3 calls hung up) |
| Nov 3 (campaign 20) | 0 | 0 / 0 | 0 | mean 0.003 ms (p99 not pulled) | mean 1.22 s (Gemini; p50/p95 not pulled) | mean 0.92 s | 21 (cause not checked) |
| Nov 4 (test_up_2000, gemma 4) | 6 (llm 4, tts_drain_timeout 2) | 44 (39 recovered, 5 exhausted) / 4 | 0 | not pulled | mean 5.0 s; 15% ≤1.6 s, 37% ≤3.2 s, 70% ≤6.4 s, 98% ≤12.8 s (p50 ~4–5 s, p95 ~10–12 s, read from histogram buckets) | mean 3.8 s | 5 |
| Nov 5 (campaign 21, sarvam-105b) | 9 (stt 6, tts_open 2, tts_drain_timeout 1) | 0 / 0 | 0 | not available (SigNoz: `vaani_pacer_drift_ms` never received) | 259 ms / 976 ms (from `llm.first_token` logs) | 398 ms / 1.14 s (from `tts.first_audio` logs) | 17 (4 calls hung up as media dead) |
| Nov 6 (campaign 21, sarvam-105b) | 5 (all `tts_closed`; plus 9 "tts websocket closed by provider" ERROR logs) | 0 / 0 | 0 | not available (never received) | 238 ms / 488 ms (from `llm.first_token` logs) | 340 ms / 683 ms (from `tts.first_audio` logs) | 23 (2 calls hung up as media dead) |
| Nov 9 (campaign 21, parallel dialing, sarvam-105b) | 0 | 0 / 0 | 0 (not queried; no `media dropped` logs) | not available | 195 ms / 367 ms (from `llm.first_token` logs) | p95 576 ms (p50 not pulled) | 3 (1 call hung up as media dead) |
| Nov 9 16:30 (campaign 22, 475 dials in 10 s) | 0 | 0 / 0 | not queried | not available | p95 438 ms (p50 not pulled) | not pulled | 72 (34 calls hung up as media dead, 47 `media.silent`) |

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

### Nov 5, 2026 — campaign 21 ("test_up_2000" resumed, LLM sarvam-105b-conversations)

- Resumed the Nov 4 campaign, dialed 18:07:00–18:09:50 IST, then paused by hand; the last calls ended ~18:13. Metrics window 18:05–18:20 IST (all calls inside it, idle before and after).
- 726 dials (metric = logs), steady 44–48 per 10 s (~4.6/s, the one-loop cap again). Answered 378 (52.1%), no-answer 253, rejected (cause 21) 90, failed (cause 38) 5, 0 invalid numbers (cause 1). 192 retries counted by `vaani_campaign_retries_total`; whether any of the 726 dials were retries was not checked. Answered: 271 caller hung up, 84 qualified, 23 idle timeout. Average 36 s.
- **New peak: 94 talking calls** (1-min max; was 63), 179 in flight (talking + ringing). Peak CPU 2.49 cores (15.5% of 16). Core per talking call **0.036** (504 CPU-s ÷ 13,875 talk-s), down from 0.044: the cost per call keeps falling with load. Peak heap 447 MB (~4.8 MB per talking call), RSS 696 MB, 1442 goroutines.
- **LLM fast again**: 1847 requests, first token p50 259 ms / p95 976 ms (gemma 4 on Nov 4: mean 5.0 s); response headers ≤216 ms for 90% of requests. 0 retries, 0 fallbacks. 24 requests (1.3%) took 3.2–6.7 s, all status 200 on the first attempt, in **two bursts**: 18:08:55 (12) and 18:10:51 (12–13), each a dozen different calls finishing within ~0.5 s of each other. This points at a short stall upstream (Bifrost or Sarvam), not at the engine (inference, not proven). Their ids are in the `llm.request` logs (`completion_id` = `request_id` = Sarvam's `X-Request-Id`, e.g. `20261105_dd8e29e6-902b-44ff-aca6-1037cd8e0e3a`, 4.7 s).
- Reply latency per call: median of per-call p50s 986 ms; 36 of 378 calls (10%) had a per-call p95 ≥3 s (the bursts). TTS first audio p50 398 ms / p95 1.14 s.
- Errors: 6 `stt` agent errors, all 18:09:17–18:09:35 (cause not checked); 2 `tts_open` (one "context canceled" during teardown); 1 `tts_drain_timeout`. 0 dropped frames. Media watchdog timeouts 17, 4 calls hung up as media dead (cause still not investigated). 97 calls with a silent caller. Recording `34bk399q` still missing (57 `tts.clip_missing`).
- STT audio 8391 s for 13,875 s of calls (60%). Received network per talking call rose to 57 kB/s (47–51 before); not investigated.
- (Nov 6 follow-up run of the same campaign is in the next section.)
- First test on build `4d712c0`: `llm.request`, `stt.connected`/`tts.connected` and `agent.error` logs all present. DEBUG lines were being logged during this run (`LOG_LEVEL`), which adds some CPU and log export; not measured.

### Nov 6, 2026 — campaign 21 again (org limit 400, dial rate 60)

- Resumed the same campaign, dialed 14:50–14:55 IST (09:20–09:25 UTC), paused by hand. Metrics window 14:48–15:08 IST. Org settings set in the table before: `CONCURRENT_CALL_LIMIT` 400, `DIAL_RATE_LIMIT_PER_SECOND` 60.
- **This was not a higher-concurrency test.** Logs show a single campaign (21), so the plan of two campaigns at once was not used. 1197 dials = metric = logs, steady 280 per minute (~4.7/s): the one-loop dial cap again, so the dial rate of 60 changed nothing (confirms it is not the limit). Peak in flight was 169, so neither the old (200) nor the new (400) org limit was reached.
- Outcomes (1197 dials): answered 631 (52.7%), no-answer 411, rejected (cause 21) 141, failed 12 (cause 38 ×10, cause 1 ×2), busy 2. 274 retries counted by `vaani_campaign_retries_total` (whether they are inside the 1197 was not checked). Answered: 509 caller hung up, 93 qualified, 29 idle timeout. Average call **25 s** (36 s on Nov 5).
- **Peak talking was lower, 70 (Nov 5: 94)**, at the same dial rate, because calls were shorter (25 s vs 36 s): talking ≈ answered per second × call length. Peak CPU 2.19 cores (13.7%). Core per talking call 0.038 (588 CPU-s ÷ 15,650 talk-s), in line with Nov 5 (0.036). Peak heap 382 MB (5.5 MB per talking call), RSS 585 MB, 1120 goroutines. Network per talking call 55 kB/s received / 99 kB/s sent, as before.
- LLM: 2261 requests, first token p50 238 ms / p95 488 ms, 0 retries or fallbacks, max request 5.3 s. 18 requests (0.8%) took ≥3 s, **all in one 30-second window (09:24:00–09:24:30 UTC, 14:54 IST)**. TTS first audio p50 340 ms / p95 683 ms. Average per-call reply latency p50 881 ms; 16 calls (2.5%) had a p95 ≥3 s.
- **New problem — Sarvam TTS closed connections abnormally.** 9 ERROR logs "tts websocket closed by provider" (`websocket: close 1006 (abnormal closure): unexpected EOF`) between 09:24:08 and 09:24:52 UTC, i.e. in the same minute as the slow-LLM burst and with ~70 calls talking; 5 `agent.error` `tts_closed` and 5 `tts.lost`. Not seen on Nov 5. Timing suggests a provider-side event or a stream limit near ~70 concurrent calls; **not confirmed** (no provider message, request ids not pulled). Needs a look at the 9 call ids and a ticket to Sarvam if it repeats.
- Other: 23 media watchdog timeouts, 2 calls hung up as media dead (cause still not investigated); 63 `tts.clip_missing` (recording `34bk399q`); 0 dropped frames; STT audio 8792 s for 15,431 s of calls (57%). Pacer drift p99 not available.

### Nov 9, 2026 — campaign 21, first run with parallel dial workers

- Build: the dialer with `CAMPAIGN_DIAL_WORKERS` (default 10) and the one-query `DialCheck`; org settings `CONCURRENT_CALL_LIMIT` 600, `DIAL_RATE_LIMIT_PER_SECOND` 100 (the rate actually stored during the run was not re-read). Resumed 12:28 IST (06:58 UTC).
- **Dial speed: 271 calls in under 10 seconds** (189 in the first 5-s bucket = ~38/s, 82 in the next ~16/s), against ~4.7/s before: **about 8× faster**, as planned. The rest of the run dialed only the 28 retries of no-answers (120 s later, at 12:30). Per-second resolution was not available (SigNoz minimum bucket 5 s), so the true peak rate is unknown.
- **The campaign finished by itself, not by a pause**: `campaign.completed` at 12:32:44 IST, after 299 dials (271 first-time contacts + 28 retries). The contact list was used up, so the test lasted only ~10 s of dialing and did **not** reach a steady state.
- **Record 266 calls in flight** (previous best 179). Calls talking at once peaked at 73 (1-min max): the burst put 271 calls ringing at once, only 46% answered (137 of 299; no-answer 123, rejected cause 21 ×37, busy 1, cause 1 ×1), average call 25 s, so the talking count stays low. Answered: 112 caller hung up, 18 qualified, 7 idle timeout. No unusual failures from the trunk or Asterisk (no cause 38) at this dial speed.
- CPU: 168 CPU-s for 3,485 talk-s = **0.048 core per talking call**, higher than 0.036–0.038 only because the run was short: the dial burst and ring-time work is spread over few talking seconds. Not comparable with the longer tests. Peak 1.03 cores in a 1-minute average (the burst itself, ~10 s, was not measured finer). Heap 314 MB, RSS 512 MB, 1089 goroutines at 266 in flight.
- LLM first token p50 195 ms / p95 367 ms; TTS first audio p95 576 ms; 0 LLM requests ≥3 s; 0 retries, 0 fallbacks; 0 agent errors, no "tts websocket closed" (Nov 6 had 9, at ~70 talking). 3 media watchdog timeouts, 1 call hung up as media dead. 13 `tts.clip_missing` (recording `34bk399q`).
- **What this does not show**: how Asterisk, the SIP trunk and Sarvam behave with a *sustained* high dial rate and 150+ talking calls. For that: a campaign with at least ~3,000 contacts so it keeps dialing for several minutes, and a dial rate stepped up 10 → 15 → 20.

### Nov 9, 2026 — campaign 22, 475 dials in 10 seconds (breaker paused it)

- Build: parallel dial workers; org `CONCURRENT_CALL_LIMIT` 600, `DIAL_RATE_LIMIT_PER_SECOND` 100 (stored value not re-read). Dialed 16:30:10–16:30:20 IST (11:00 UTC): 174, 261, 40 per 5-s bucket, ~50/s peak. **468 calls in flight**, record.
- **The breaker paused the campaign at 16:30:20** ("circuit breaker: 3 of 5 calls failed"), but all 475 calls were already out. It judges the last 5 results, so with parallel dialing it trips after the burst, not during it.
- **Funnel of the 475 dials:** 197 no result from Asterisk (`outbound.timeout`, hung up ~16:31), 63 no-answer cause 19, 16 rejected cause 21, 47 failed (36 cause 0, 8 cause 16, 3 cause 38), 188 answered. Of those 188: 26 `call.setup_failed` (24 "add caller to bridge" 400, 2 "answer" 404) and 12 abandoned, so **150 were bridged** (31% of dials, vs 46% answered on the 271-dial run).
- **Media failures: 34 of the 150 bridged calls (23%) hung up as media dead** (no inbound audio), 72 "no inbound rtp" watchdog warnings, 47 `media.silent`. The timeouts, setup failures and dead-media hangups all fell between 16:30:30 and 16:31:30, right after the burst. No pipeline error alongside them (0 agent errors in every stage).
- **Reading (not proven):** Asterisk or the SIP trunk could not set up ~190 answered calls at once; the agent side is not the cause. The 197 no-result calls could also be genuine ring timeouts. Needs Asterisk and trunk logs for 16:30:30–16:31:30 to settle.
- CPU: 192 CPU-s for 5,167 talk-s = **0.037 core per talking call** (same as Nov 5, 0.036). Peak 1.50 cores in a 1-minute average at 94 talking. LLM first token p95 438 ms, 0 retries, 0 fallbacks, 0 agent errors.
- **Next:** rerun at a lower dial rate, 10 → 15 → 20/s; confirm the trunk's calls-per-second and channel limits; make the breaker work on a per-second dial budget (not agreed yet).

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

Memory is not the constraint: ~4 MB heap per talking call, so 250 calls is roughly 1–1.5 GB of 32 GB. The largest tested load is 94 talking calls (Nov 5, 0.036 core each, 15.5% CPU), so these are ~2–3x extrapolations from a cost (0.058) that has since fallen; the true cost per call has been falling with load, so the real number may be higher. Treat them as unproven until a test reaches them.

## Next test (planned)

Reach ~120 talking calls: dial rate 8–10 calls/s (superadmin → Organizations → Concurrency; see `docs/OUTBOUND.md`), ~2,000 contacts, `max_concurrency` 300 (check the org limit too). During the run record on the Vaani box (`top`/`htop`): total CPU, `vaani_pacer_drift_ms` p99, dropped frames, errors; and, on their servers, Asterisk CPU and channels, trunk CPS and rejects, Postgres and Redis load; Sarvam stream errors and Gemini 429s. Stop and note the point where drift p99 exceeds ~5 ms, frames drop, or errors appear.

## Limits outside Vaani (to confirm, not measured)

SIP trunk calls-per-second and max channels; Sarvam STT/TTS concurrent streams on the plan; Gemini requests-per-minute quota at the current tier; Asterisk CPU and the Asterisk-to-Vaani link (the raw audio alone is ~256 kbps per call each way; the engine's total network per call is in the Network table above).
