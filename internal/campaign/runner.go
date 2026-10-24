package campaign

import (
	"context"
	"fmt"
	"log/slog"
	"strconv"
	"sync"
	"time"

	"github.com/nitesh/vaani/internal/ari"
	"github.com/nitesh/vaani/internal/dograh"
)

// Store is the part of Dograh's database the dialer uses (*dograh.Store).
type Store interface {
	runStore

	VaaniCampaigns(ctx context.Context) ([]dograh.Campaign, error)
	CampaignRunning(ctx context.Context, id int) (bool, error)
	SetCampaignState(ctx context.Context, id int, from, to string) (bool, error)
	OrgConcurrencyLimit(ctx context.Context, orgID int) (int, bool, error)
	WalletBlocks(ctx context.Context, orgID int) bool
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
}

// Runner dials the campaigns of organizations whose media engine is "vaani":
// it claims contacts from Dograh's queue one at a time, each under the
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
	draining map[int]bool // campaigns a drain goroutine is working on
	inflight map[int]int  // calls placed and not yet over, per campaign
	liveRuns map[int64]bool
	wakeCh   chan struct{}
	wg       sync.WaitGroup
}

// NewRunner returns a Runner.
func NewRunner(store Store, lim *Limiter, placer CallPlacer, cfg RunnerConfig) *Runner {
	return &Runner{
		store: store, lim: lim, placer: placer, cfg: cfg, now: time.Now, ctx: context.Background(),
		draining: map[int]bool{}, inflight: map[int]int{}, liveRuns: map[int64]bool{}, wakeCh: make(chan struct{}, 1),
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
		r.mu.Lock()
		busy := r.draining[c.ID]
		r.draining[c.ID] = true
		r.mu.Unlock()

		if busy {
			continue
		}

		r.wg.Add(1)

		go func() {
			defer r.wg.Done()
			defer func() {
				r.mu.Lock()
				delete(r.draining, c.ID)
				r.mu.Unlock()
			}()

			r.drain(ctx, c)
		}()
	}
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
			continue
		case stepWait:
			select {
			case <-time.After(min(max(wait, 10*time.Millisecond), time.Second)):
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
func (r *Runner) pause(ctx context.Context, c dograh.Campaign, why string) {
	moved, err := r.store.SetCampaignState(ctx, c.ID, "running", "paused")
	if err != nil {
		slog.Error("campaign: pausing failed", "component", "campaign", "event", "campaign.pause_failed", "campaign_id", c.ID, "error", err)
		return
	}

	if moved {
		slog.Warn("campaign paused", "component", "campaign", "event", "campaign.paused", "campaign_id", c.ID, "org_id", c.OrganizationID, "reason", why)
	}
}

// orgLimit is how many calls campaign c's organization may have at once: its
// CONCURRENT_CALL_LIMIT or the default, lowered by the campaign's own
// max_concurrency.
func (r *Runner) orgLimit(ctx context.Context, c dograh.Campaign) (int, error) {
	limit, ok, err := r.store.OrgConcurrencyLimit(ctx, c.OrganizationID)
	if err != nil {
		return 0, err
	}

	if !ok {
		limit = r.cfg.DefaultOrgLimit
	}

	if c.MaxConcurrency > 0 && c.MaxConcurrency < limit {
		limit = c.MaxConcurrency
	}

	return limit, nil
}

// dialOne takes the next contact of campaign c and places the call, if the
// campaign is running, inside its window, healthy, paid for, and under the
// organization's rate and concurrency limits.
func (r *Runner) dialOne(ctx context.Context, c dograh.Campaign) (step, time.Duration) {
	running, err := r.store.CampaignRunning(ctx, c.ID)
	if err != nil || !running {
		return stepStop, 0
	}

	if !c.Schedule.Allows(r.now()) {
		return stepStop, 0
	}

	if open, stats, err := r.lim.BreakerOpen(ctx, c.ID, c.Breaker); err == nil && open {
		r.pause(ctx, c, fmt.Sprintf("circuit breaker: %d of %d calls failed", stats.Failures, stats.Total()))
		return stepStop, 0
	}

	if r.store.WalletBlocks(ctx, c.OrganizationID) {
		r.pause(ctx, c, "the organization's wallet is out of credit")
		return stepStop, 0
	}

	limit, err := r.orgLimit(ctx, c)
	if err != nil {
		slog.Error("campaign: organization limit unreadable", "component", "campaign", "event", "campaign.limit_failed", "campaign_id", c.ID, "error", err)
		return stepStop, 0
	}

	ok, err := r.lim.AcquireToken(ctx, c.OrganizationID, c.RatePerSecond)
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

	return r.place(ctx, c, runs[0], slot), 0
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
func (r *Runner) place(ctx context.Context, c dograh.Campaign, q dograh.QueuedRun, slot Slot) step {
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

	settings, err := r.store.DialSettings(ctx, c.OrganizationID)
	if err != nil {
		return giveBack("the organization's telephony settings are unreadable", err)
	}

	if settings.Provider != "ari" {
		r.pause(ctx, c, fmt.Sprintf("the organization's telephony provider is %q, not ari", settings.Provider))
		return giveBack("cannot dial over ARI", fmt.Errorf("provider %q", settings.Provider))
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
		r.recordOutcome(c, true)
		r.markNotConnected(c, runID, dograh.StatusFailed)
		r.setRunState(ctx, q, dograh.RunFailed)

		return stepDialed
	}

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
		r.pause(ctx, c, fmt.Sprintf("circuit breaker: %d of %d calls failed", stats.Failures, stats.Total()))
	}
}

func (r *Runner) addInflight(campaignID, n int) {
	r.mu.Lock()
	defer r.mu.Unlock()

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
		r.log(c, "campaign call finished", "campaign.finished", "queued_run_id", q.ID, "run_id", runID)
		r.recordOutcome(c, false)

		return
	}

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
