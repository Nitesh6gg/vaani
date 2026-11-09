package campaign

import (
	"context"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"strconv"
	"sync"
	"time"

	"github.com/nitesh/vaani/internal/ari"
	"github.com/nitesh/vaani/internal/dograh"
	"github.com/nitesh/vaani/internal/metrics"
)

// Store is the part of Dograh's database the dialer uses (*dograh.Store).
type Store interface {
	runStore

	VaaniCampaigns(ctx context.Context) ([]dograh.Campaign, error)
	DialCheck(ctx context.Context, campaignID, orgID int) (dograh.DialCheck, error)
	SetCampaignState(ctx context.Context, id int, from, to string) (bool, error)
	ClaimRuns(ctx context.Context, campaignID, limit int, now time.Time) ([]dograh.QueuedRun, error)
	SetRunState(ctx context.Context, runID int, state string) error
	ScheduleRetry(ctx context.Context, run dograh.QueuedRun, reason string, delay time.Duration, now time.Time) error
	AddFailedRow(ctx context.Context, campaignID int) error

	OpenWork(ctx context.Context, campaignID int) (queued, processing int, err error)
	CompleteCampaign(ctx context.Context, id int) (bool, error)
	RecoverContacts(ctx context.Context) (requeued, processed int, err error)
	StaleRuns(ctx context.Context, initializedBefore time.Time, includeRunning bool, live []int64) ([]dograh.StaleRun, error)
	MarkInterrupted(ctx context.Context, runID int64, orgID int) error
}

// RunnerConfig tunes the Runner.
type RunnerConfig struct {
	// Poll is how often Dograh's database is scanned for campaigns to dial.
	Poll time.Duration
	// DefaultOrgLimit is the concurrent-call limit of an organization that has
	// none set (Dograh's DEFAULT_ORG_CONCURRENCY_LIMIT, 2).
	DefaultOrgLimit int
	RingTimeout     time.Duration
	// Workers is how many contacts of one campaign are dialed at the same
	// time (CAMPAIGN_DIAL_WORKERS); 0 means 1. Dialing one contact takes
	// ~10 round trips to Postgres, Redis and Asterisk, so one worker tops out
	// near 5 calls a second whatever the dial rate; the organization's rate
	// and concurrency limits still decide the real speed. At most twice this
	// many workers run across all campaigns, so they can't take the whole
	// database pool from the calls being set up.
	Workers int
	// Overloaded, when set, says the machine is too busy for another call: the
	// dialer then places none and claims no contact (LOAD_SHED_CPU_PERCENT).
	Overloaded func() bool
}

// Runner dials the campaigns of organizations whose media engine is "vaani":
// it claims contacts from Dograh's queue, each under the
// organization's rate and concurrency limits, places the call, and decides
// retries and the circuit breaker from how it ends. Dograh's UI starts, pauses
// and resumes campaigns; the Runner only follows their state.
type Runner struct {
	store  Store
	lim    *Limiter
	placer CallPlacer
	cfg    RunnerConfig
	now    func() time.Time

	// ctx is the process's context; set by Run. Call results are written even
	// while it is being cancelled (see finished).
	ctx context.Context

	mu       sync.Mutex
	workers  map[int]int // dial workers running, per campaign
	total    int         // ... and in all
	inflight map[int]int // calls placed and not yet over, per campaign
	liveRuns map[int64]bool
	wakeCh   chan struct{}
	wg       sync.WaitGroup
}

// NewRunner returns a Runner.
func NewRunner(store Store, lim *Limiter, placer CallPlacer, cfg RunnerConfig) *Runner {
	cfg.Workers = max(cfg.Workers, 1)

	return &Runner{
		store: store, lim: lim, placer: placer, cfg: cfg, now: time.Now, ctx: context.Background(),
		workers: map[int]int{}, inflight: map[int]int{}, liveRuns: map[int64]bool{}, wakeCh: make(chan struct{}, 1),
	}
}

// Run scans for campaigns until ctx is cancelled, then waits for the dialing
// goroutines to stop. Calls already placed carry on (the ARI manager ends them
// at shutdown).
func (r *Runner) Run(ctx context.Context) {
	r.ctx = ctx

	r.recover(ctx)

	ticker := time.NewTicker(r.cfg.Poll)
	defer ticker.Stop()

	sweepTicker := time.NewTicker(time.Minute)
	defer sweepTicker.Stop()

	for {
		r.scan(ctx)

		select {
		case <-ctx.Done():
			r.wg.Wait()
			return
		case <-ticker.C:
		case <-r.wakeCh:
		case <-sweepTicker.C:
			r.sweep(ctx, false)
		}
	}
}

// wake makes the next scan happen now (a call ended, so a slot is free).
func (r *Runner) wake() {
	select {
	case r.wakeCh <- struct{}{}:
	default:
	}
}

func (r *Runner) scan(ctx context.Context) {
	campaigns, err := r.store.VaaniCampaigns(ctx)
	if err != nil && ctx.Err() == nil {
		slog.Error("campaign scan: some campaigns could not be read", "component", "campaign", "event", "campaign.scan_failed", "error", err)
	}

	for _, c := range campaigns {
		r.startWorker(ctx, c, false)
	}
}

// startWorker starts a dial worker for campaign c, unless it has its share
// already. A scan starts one; each worker that dials a contact starts the next
// (grow), up to Workers, so a campaign with no room (a full slot limit) costs
// one failed attempt per scan, not Workers of them.
func (r *Runner) startWorker(ctx context.Context, c dograh.Campaign, grow bool) {
	r.mu.Lock()

	if r.workers[c.ID] >= r.cfg.Workers || r.total >= 2*r.cfg.Workers || (!grow && r.workers[c.ID] > 0) {
		r.mu.Unlock()
		return
	}

	r.workers[c.ID]++
	r.total++
	r.wg.Add(1)
	r.mu.Unlock()

	go func() {
		defer r.wg.Done()
		defer func() {
			r.mu.Lock()
			defer r.mu.Unlock()

			if r.workers[c.ID]--; r.workers[c.ID] <= 0 {
				delete(r.workers, c.ID)
			}

			r.total--
		}()

		r.drain(ctx, c)
	}()
}

// step is what one attempt to dial a contact came to.
type step int

const (
	stepDialed step = iota // a contact was handled: try the next
	stepWait               // the rate window is full: wait, then try again
	stepStop               // nothing more to do now: wait for the next scan
)

// drain dials campaign c's contacts until it runs out of work or room.
func (r *Runner) drain(ctx context.Context, c dograh.Campaign) {
	for ctx.Err() == nil {
		res, wait := r.dialOne(ctx, c)

		switch res {
		case stepDialed:
			r.startWorker(ctx, c, true)
		case stepWait:
			wait = min(max(wait, 10*time.Millisecond), time.Second)
			wait += rand.N(wait/4 + 1) // workers waiting for the same token don't all ask again at once

			select {
			case <-time.After(wait):
			case <-ctx.Done():
			}
		default:
			return
		}
	}
}

func (r *Runner) log(c dograh.Campaign, msg, event string, args ...any) {
	slog.Info(msg, append([]any{"component", "campaign", "event", event, "campaign_id", c.ID, "org_id", c.OrganizationID}, args...)...)
}

// pause moves a running campaign to paused (unless someone changed it meanwhile).
func (r *Runner) pause(ctx context.Context, c dograh.Campaign, kind, why string) {
	moved, err := r.store.SetCampaignState(ctx, c.ID, "running", "paused")
	if err != nil {
		slog.Error("campaign: pausing failed", "component", "campaign", "event", "campaign.pause_failed", "campaign_id", c.ID, "error", err)
		return
	}

	if moved {
		metrics.CampaignPausesTotal.WithLabelValues(kind).Inc()
		slog.Warn("campaign paused", "component", "campaign", "event", "campaign.paused", "campaign_id", c.ID, "org_id", c.OrganizationID, "reason", why)
	}
}

// orgLimit is how many calls campaign c's organization may have at once: its
// CONCURRENT_CALL_LIMIT or the default, lowered by the campaign's own
// max_concurrency.
func (r *Runner) orgLimit(c dograh.Campaign, chk dograh.DialCheck) int {
	limit := r.cfg.DefaultOrgLimit
	if chk.HasConcurrent {
		limit = chk.ConcurrentMax
	}

	if c.MaxConcurrency > 0 && c.MaxConcurrency < limit {
		limit = c.MaxConcurrency
	}

	return limit
}

// dialRate is how many calls per second campaign c's organization may start:
// its DIAL_RATE_LIMIT_PER_SECOND, which overrides the campaign's own
// rate_limit_per_second, or that rate when the organization has none set.
func dialRate(c dograh.Campaign, chk dograh.DialCheck) int {
	if !chk.HasDialRate {
		return c.RatePerSecond
	}

	return max(chk.DialRate, 1)
}

// dialOne takes the next contact of campaign c and places the call, if the
// campaign is running, inside its window, healthy, paid for, and under the
// organization's rate and concurrency limits.
func (r *Runner) dialOne(ctx context.Context, c dograh.Campaign) (step, time.Duration) {
	// Before anything is read, taken or claimed: nothing to give back, and the
	// campaign carries on by itself once the CPU is back under the limit.
	if r.cfg.Overloaded != nil && r.cfg.Overloaded() {
		metrics.CallsRejectedTotal.WithLabelValues("cpu", "outbound").Inc()
		return stepWait, time.Second
	}

	// The breaker (Redis) and the database checks don't depend on each other,
	// so they are read at the same time.
	type breaker struct {
		open  bool
		stats BreakerStats
		err   error
	}

	br := make(chan breaker, 1)

	go func() {
		open, stats, err := r.lim.BreakerOpen(ctx, c.ID, c.Breaker)
		br <- breaker{open, stats, err}
	}()

	chk, err := r.store.DialCheck(ctx, c.ID, c.OrganizationID)
	b := <-br

	if err != nil || !chk.Running {
		return stepStop, 0
	}

	if !c.Schedule.Allows(r.now()) {
		return stepStop, 0
	}

	if b.err == nil && b.open {
		r.pause(ctx, c, "breaker", fmt.Sprintf("circuit breaker: %d of %d calls failed", b.stats.Failures, b.stats.Total()))
		return stepStop, 0
	}

	if chk.WalletBlocks {
		r.pause(ctx, c, "wallet", "the organization's wallet is out of credit")
		return stepStop, 0
	}

	// Checked before a contact is claimed, so there is nothing to give back.
	if !chk.HasSettings {
		slog.Error("campaign: the organization's telephony settings are unreadable", "component", "campaign", "event", "campaign.place_failed",
			"campaign_id", c.ID, "error", fmt.Errorf("organization %d has no telephony configuration", c.OrganizationID))

		return stepStop, 0
	}

	if chk.Settings.Provider != "ari" {
		r.pause(ctx, c, "provider", fmt.Sprintf("the organization's telephony provider is %q, not ari", chk.Settings.Provider))
		slog.Error("campaign: cannot dial over ARI", "component", "campaign", "event", "campaign.place_failed",
			"campaign_id", c.ID, "error", fmt.Errorf("provider %q", chk.Settings.Provider))

		return stepStop, 0
	}

	limit, rate := r.orgLimit(c, chk), dialRate(c, chk)

	ok, err := r.lim.AcquireToken(ctx, c.OrganizationID, rate)
	if err != nil {
		slog.Error("campaign: rate limiter failed", "component", "campaign", "event", "campaign.limiter_failed", "campaign_id", c.ID, "error", err)
		return stepStop, 0
	}

	if !ok {
		wait, _ := r.lim.NextTokenWait(ctx, c.OrganizationID)
		return stepWait, wait
	}

	slot, ok, err := r.lim.AcquireSlot(ctx, c.OrganizationID, limit)
	if err != nil {
		slog.Error("campaign: slot limiter failed", "component", "campaign", "event", "campaign.limiter_failed", "campaign_id", c.ID, "error", err)
		return stepStop, 0
	}

	if !ok {
		return stepStop, 0 // full: a call ending wakes the next scan
	}

	runs, err := r.store.ClaimRuns(ctx, c.ID, 1, r.now())
	if err != nil || len(runs) == 0 {
		r.release(slot)

		if err != nil {
			slog.Error("campaign: claiming a contact failed", "component", "campaign", "event", "campaign.claim_failed", "campaign_id", c.ID, "error", err)
		} else {
			r.maybeComplete(ctx, c)
		}

		return stepStop, 0
	}

	return r.place(ctx, c, runs[0], chk.Settings, slot), 0
}

func (r *Runner) release(slot Slot) {
	if err := r.lim.ReleaseSlot(context.WithoutCancel(r.ctx), slot); err != nil {
		slog.Error("campaign: releasing a slot failed", "component", "campaign", "event", "campaign.release_failed", "slot", slot.ID, "error", err)
	}
}

// phoneOf is the contact's number from its variables (a text cell, or a
// number a sheet delivered as one).
func phoneOf(vars map[string]any) string {
	switch v := vars["phone_number"].(type) {
	case string:
		return v
	case float64:
		return strconv.FormatFloat(v, 'f', -1, 64)
	}

	return ""
}

// place dials claimed contact q. It owns slot from here: released when the
// call is over, or at once if it was never placed.
func (r *Runner) place(ctx context.Context, c dograh.Campaign, q dograh.QueuedRun, settings dograh.DialSettings, slot Slot) step {
	phone := phoneOf(q.Variables)
	if phone == "" {
		slog.Warn("campaign: contact has no phone_number; failing it", "component", "campaign", "event", "campaign.no_number",
			"campaign_id", c.ID, "queued_run_id", q.ID)
		r.release(slot)
		r.setRunState(ctx, q, dograh.RunFailed)

		return stepDialed
	}

	// Anything that stops the call from being placed gives the contact back,
	// to be tried again at the next scan.
	giveBack := func(why string, err error) step {
		slog.Error("campaign: "+why, "component", "campaign", "event", "campaign.place_failed",
			"campaign_id", c.ID, "queued_run_id", q.ID, "error", err)
		r.release(slot)
		r.setRunState(ctx, q, dograh.RunQueued)

		return stepStop
	}

	caller := settings.CallerID(c.WorkflowID)

	runID, err := r.store.CreateOutboundRun(ctx, c, q, phone, caller)
	if err != nil {
		return giveBack("creating the call's Dograh run failed", err)
	}

	r.addInflight(c.ID, 1)
	r.track(runID, true)

	_, err = r.placer.Dial(ctx, ari.DialRequest{
		Endpoint: settings.Endpoint(phone), CallerID: caller, Called: phone, RingTimeout: r.cfg.RingTimeout,
		WorkflowID: c.WorkflowID, RunID: runID, Variables: q.Variables,
		Done: func(out ari.Outcome) { r.finished(c, q, runID, slot, out) },
	})
	if err != nil {
		// The ARI call itself failed: nothing is ringing, and Done won't come.
		slog.Error("campaign: dialing failed", "component", "campaign", "event", "campaign.dial_failed",
			"campaign_id", c.ID, "queued_run_id", q.ID, "run_id", runID, "error", err)
		r.addInflight(c.ID, -1)
		r.track(runID, false)
		r.release(slot)
		metrics.CampaignOutcomesTotal.WithLabelValues(dograh.StatusFailed).Inc()
		r.recordOutcome(c, true)
		r.markNotConnected(c, runID, dograh.StatusFailed)
		r.setRunState(ctx, q, dograh.RunFailed)

		return stepDialed
	}

	metrics.CampaignDialsTotal.Inc()
	r.setRunState(ctx, q, dograh.RunProcessed)
	r.log(c, "campaign call placed", "campaign.dialed", "queued_run_id", q.ID, "run_id", runID, "called_number", phone,
		"retry_count", q.RetryCount)

	return stepDialed
}

func (r *Runner) setRunState(ctx context.Context, q dograh.QueuedRun, state string) {
	if err := r.store.SetRunState(context.WithoutCancel(ctx), q.ID, state); err != nil {
		slog.Error("campaign: updating a contact failed", "component", "campaign", "event", "campaign.row_update_failed",
			"queued_run_id", q.ID, "state", state, "error", err)
	}
}

func (r *Runner) markNotConnected(c dograh.Campaign, runID int64, status string) {
	if err := r.store.MarkNotConnected(context.WithoutCancel(r.ctx), runID, c.OrganizationID, status); err != nil {
		slog.Error("campaign: completing an unconnected call's run failed", "component", "campaign",
			"event", "campaign.run_update_failed", "run_id", runID, "error", err)
	}
}

// recordOutcome adds a finished call to the circuit breaker's window and
// pauses the campaign if that trips it.
func (r *Runner) recordOutcome(c dograh.Campaign, failure bool) {
	ctx := context.WithoutCancel(r.ctx)

	tripped, stats, err := r.lim.RecordOutcome(ctx, c.ID, failure, c.Breaker)
	if err != nil {
		slog.Error("campaign: circuit breaker failed", "component", "campaign", "event", "campaign.breaker_failed", "campaign_id", c.ID, "error", err)
		return
	}

	if tripped {
		r.pause(ctx, c, "breaker", fmt.Sprintf("circuit breaker: %d of %d calls failed", stats.Failures, stats.Total()))
	}
}

func (r *Runner) addInflight(campaignID, n int) {
	r.mu.Lock()
	defer r.mu.Unlock()

	metrics.CampaignCallsInFlight.Add(float64(n))

	if r.inflight[campaignID] += n; r.inflight[campaignID] <= 0 {
		delete(r.inflight, campaignID)
	}
}

// retryReason is the retry setting a status stands for; "" = not retried.
func retryReason(status string) string {
	switch status {
	case dograh.StatusBusy:
		return "busy"
	case dograh.StatusNoAnswer:
		return "no_answer"
	}

	return ""
}

// finished is the end of a placed call (on its own goroutine, so the ARI event
// loop never waits on the database): free the slot, tell the circuit breaker,
// and for a call that never connected decide its retry and complete its run.
func (r *Runner) finished(c dograh.Campaign, q dograh.QueuedRun, runID int64, slot Slot, out ari.Outcome) {
	defer r.wake()
	defer r.addInflight(c.ID, -1)
	defer r.track(runID, false)

	r.release(slot)

	if out.Connected {
		metrics.CampaignOutcomesTotal.WithLabelValues("connected").Inc()
		r.log(c, "campaign call finished", "campaign.finished", "queued_run_id", q.ID, "run_id", runID)
		r.recordOutcome(c, false)

		return
	}

	metrics.CampaignOutcomesTotal.WithLabelValues(out.Status).Inc()
	r.log(c, "campaign call did not connect", "campaign.not_connected", "queued_run_id", q.ID, "run_id", runID,
		"status", out.Status, "cause", out.Cause)

	// As Dograh: only a failed or rejected call counts against the breaker.
	r.recordOutcome(c, out.Status == dograh.StatusFailed || out.Status == dograh.StatusRejected)

	ctx := context.WithoutCancel(r.ctx)

	if reason := retryReason(out.Status); reason != "" && c.Retry.Eligible(reason) {
		var err error

		if q.RetryCount >= c.Retry.MaxRetries {
			err = r.store.AddFailedRow(ctx, c.ID) // retries used up
		} else {
			err = r.store.ScheduleRetry(ctx, q, reason, c.Retry.Delay, r.now())
			if err == nil {
				metrics.CampaignRetriesTotal.Inc()
			}
		}

		if err != nil {
			slog.Error("campaign: retry bookkeeping failed", "component", "campaign", "event", "campaign.retry_failed",
				"campaign_id", c.ID, "queued_run_id", q.ID, "error", err)
		}
	}

	r.markNotConnected(c, runID, out.Status)
}

// InFlight is how many placed calls of campaign id are not over yet.
func (r *Runner) InFlight(id int) int {
	r.mu.Lock()
	defer r.mu.Unlock()

	return r.inflight[id]
}

func (r *Runner) track(runID int64, live bool) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if live {
		r.liveRuns[runID] = true
	} else {
		delete(r.liveRuns, runID)
	}
}

// maybeComplete completes campaign c when nothing is left to do: no contact
// queued (a retry due later counts), none being handled, no call in flight.
// Dograh completes as soon as nothing is queued, even with calls still ringing,
// whose busy or no-answer would then find no campaign to retry in.
func (r *Runner) maybeComplete(ctx context.Context, c dograh.Campaign) {
	if r.InFlight(c.ID) > 0 {
		return
	}

	queued, processing, err := r.store.OpenWork(ctx, c.ID)
	if err != nil || queued > 0 || processing > 0 {
		return
	}

	// Re-checked: a call may have been placed since the count was taken.
	if r.InFlight(c.ID) > 0 {
		return
	}

	done, err := r.store.CompleteCampaign(ctx, c.ID)
	if err != nil {
		slog.Error("campaign: completing it failed", "component", "campaign", "event", "campaign.complete_failed", "campaign_id", c.ID, "error", err)
		return
	}

	if done {
		metrics.CampaignsCompletedTotal.Inc()
		r.log(c, "campaign completed", "campaign.completed")
	}
}

// recover settles what a previous Vaani left behind, once, before dialing:
// contacts claimed but never dialed go back to the queue, contacts that were
// dialed stay processed, and runs of calls that died with it are completed.
// Assumes no other Vaani is dialing the same campaigns.
func (r *Runner) recover(ctx context.Context) {
	requeued, processed, err := r.store.RecoverContacts(ctx)
	if err != nil {
		slog.Error("campaign recovery: contacts", "component", "campaign", "event", "campaign.recover_failed", "error", err)
	} else if requeued+processed > 0 {
		slog.Warn("campaign recovery: contacts a previous run left in progress", "component", "campaign", "event", "campaign.recovered",
			"requeued", requeued, "already_dialed", processed)
	}

	r.sweep(ctx, true)
}

// sweep completes the Dograh runs of calls nothing is running any more: a call
// that never reached the agent after the ring timeout, and -- right after a
// restart only -- calls the restart cut off.
func (r *Runner) sweep(ctx context.Context, startup bool) {
	r.mu.Lock()
	live := make([]int64, 0, len(r.liveRuns))
	for id := range r.liveRuns {
		live = append(live, id)
	}
	r.mu.Unlock()

	stale, err := r.store.StaleRuns(ctx, r.now().Add(-(r.cfg.RingTimeout + time.Minute)), startup, live)
	if err != nil {
		slog.Error("campaign sweep failed", "component", "campaign", "event", "campaign.sweep_failed", "error", err)
		return
	}

	for _, run := range stale {
		if run.State == "running" {
			err = r.store.MarkInterrupted(ctx, run.ID, run.OrgID)
		} else {
			err = r.store.MarkNotConnected(ctx, run.ID, run.OrgID, dograh.StatusFailed)
		}

		if err != nil {
			slog.Error("campaign sweep: completing a run failed", "component", "campaign", "event", "campaign.sweep_failed", "run_id", run.ID, "error", err)
			continue
		}

		slog.Warn("campaign sweep: completed a run nothing is running", "component", "campaign", "event", "campaign.swept",
			"run_id", run.ID, "was", run.State)
	}
}
