# Outbound campaigns — plan (draft, awaiting approval)

Vaani becomes the dialer for organizations whose media engine is `vaani`.
Dograh stays the control panel (create / upload / Start / Pause / reports).
Asterisk ARI + AudioSocket only. FreeSWITCH and diago later.

## Design in one page

- **Queue**: Dograh's Postgres `queued_runs` (claimed with `FOR UPDATE SKIP LOCKED`). No broker.
- **Counters**: Dograh's Redis, same keys, so an org's limit is shared by both engines.
  - `rate_limit:{org}` ZSET, 1 s sliding window.
  - `concurrent_calls:{org}` ZSET, score = acquire time, member = slot id, stale after 1200 s.
  - `cb_failures:{campaign}` / `cb_successes:{campaign}` ZSETs, circuit breaker.
  - NOT `fleet_capacity:calls`: that cap is sized for Dograh's workers. Vaani gets its own cap (`OUTBOUND_MAX_CALLS`).
- **Per-org switch**: new org configuration key `MEDIA_ENGINE` = `pipecat` | `vaani`. Missing = `pipecat`, so nothing changes until a superadmin flips it.
- **Dograh stops dialing for `vaani` orgs** (skip in the orchestrator and in `process_batch`); its sheet sync still runs and fills `queued_runs`.
- **Vaani** polls `running` campaigns of `vaani` orgs and wakes early on Dograh's Redis "sync completed" event.

## Facts checked in Dograh's code (D:\voice-ai-agent)

- Retry defaults: enabled, max_retries 1, delay 120 s, busy + no-answer yes, voicemail no.
- Circuit breaker defaults: 50 % failures, 120 s window, at least 5 calls.
- Org concurrency default 2 (`DEFAULT_ORG_CONCURRENCY_LIMIT`), campaign `max_concurrency` can only lower it.
- Schedule: `orchestrator_metadata.schedule_config` {enabled, timezone, slots[{day_of_week 0=Mon, start_time, end_time "HH:MM"}]}; no config / invalid timezone = always allowed.
- Unanswered outbound: `ChannelDestroyed` before `StasisStart`; cause 17 = busy, 19 = no-answer, 21 = rejected.
- Dial string: `{tech}/{number minus strip_number_prefix}@{outbound_trunk}`, `callerId` = CLI mapped to the workflow, else first `from_numbers`.
- Superadmin exists (`users.is_superuser`, `api/routes/superuser.py`); org settings are rows in `organization_configurations` (enum key, no migration).

## Milestones (each ends with a check that fails if it is broken)

**M0 — Dograh: per-org engine switch** (Python, D:\voice-ai-agent)
- Add `MEDIA_ENGINE` to `OrganizationConfigurationKey`; superadmin-only GET/PUT in `routes/superuser.py`; a toggle on the superadmin org screen.
- `_schedule_next_batch` and `process_batch` return early when the org's engine is `vaani`.
- Done when: pytest shows a `vaani` org's campaign queues no batch, a `pipecat` org's still does, and a missing key means `pipecat`.

**M1 — Vaani: read the campaign world** (Go, `internal/campaign/`)
- Store: running campaigns of `vaani` orgs, claim due rows (retries first, then regular), update `queued_runs` / counters, schedule-window check, retry config, org + campaign concurrency limit.
- Done when: tests against a throwaway Postgres show two claimers never get the same row, a retry due later is not claimed, outside-window campaigns are skipped.

**M2 — Vaani: Redis limiter** (Go)
- Port the rate-limit, concurrent-slot and circuit-breaker Lua scripts verbatim; own cap `OUTBOUND_MAX_CALLS`.
- Done when: tests against real Redis show the Go and Python scripts agree on the same keys (a slot taken by one is seen by the other), stale slots expire, breaker trips at 50 % of >= 5.

**M3 — Vaani: place one call** (Go, `internal/ari/`)
- `Originate(number, runID, ...)`: bind media socket first (invariant #9), `POST /channels` with the dial string, app `vaani`, appArgs carrying the run id; call set up on answer; direction `outbound`; contact variables rendered into the prompts; agent speaks only after answer.
- Unanswered: `ChannelDestroyed` cause 17 / 19 / 21 mapped to busy / no-answer / rejected; run marked not-connected with Dograh's disposition names.
- Done when: a real test call to your phone connects and talks, and calls to a busy, unanswered and rejected number each end with the right status in `workflow_runs`.

**M4 — Vaani: the dialer loop** (Go)
- Per campaign: window check → breaker check → wallet check → claim batch of 10 → rate token → slot → originate; release slot on every end path.
- Outcomes: busy / no-answer create a retry row (`is_retry`, `retry_attempt`, due after the delay); failed / rejected feed the breaker; trip = campaign `paused`.
- Pause stops new dials (calls in flight finish). Resume resets the breaker.
- Done when: a 5-number campaign from a CSV runs end to end in Dograh's UI with progress, runs and report pages correct, and Pause / Resume behave.

**M5 — Vaani: recovery and completion** (Go)
- Startup: rows left `processing` with no `workflow_run_id` go back to `queued`; rows with a run are left alone (never redial a number that may have been called).
- Completion: no queued rows, no retries due or scheduled, and no calls in flight → `completed`. (Dograh completes while calls are still in flight; ours waits, so a late busy still gets its retry.)
- Done when: killing Vaani mid-campaign and restarting loses no contact and redials none.

**M6 — Observability and docs**
- Metrics: campaign dials, outcomes by status, slots in use, breaker trips, claim lag. Logs with `campaign_id`, `queued_run_id`.
- `docs/` update and a CLAUDE.md package-layout line. Done when the SigNoz panels show a test campaign.

## Open decisions (defaults if you don't say)

1. Answering-machine detection: NOT in the first version (`retry_on_voicemail` stays off).
2. Completion waits for in-flight calls (differs from Dograh, see M5).
3. Outbound cap `OUTBOUND_MAX_CALLS` default 20 until the load test says more.
4. Vaani polls every 5 s; Redis event only makes it start sooner.
