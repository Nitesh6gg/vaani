package campaign

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/nitesh/vaani/internal/ari"
	"github.com/nitesh/vaani/internal/dograh"
	"github.com/nitesh/vaani/internal/metrics"
)

// memStore is Dograh's database in memory: one campaign's contacts and what
// the Runner did to them.
type memStore struct {
	mu sync.Mutex

	state      string // the campaign's state
	rows       []dograh.QueuedRun
	rowState   map[int]string
	retries    []string // reasons scheduled
	failedRows int
	notConn    []string // statuses of runs completed as not connected
	orgLimit   *int
	wallet     bool
	settings   dograh.DialSettings
	paused     []string
	nextRun    int64

	future      map[int]bool // retry rows not due yet: ClaimRuns skips them
	recovered   int
	staleRuns   []dograh.StaleRun
	staleArgs   []bool // includeRunning of each StaleRuns call
	staleLive   [][]int64
	interrupted []int64
}

func newMemStore(contacts int) *memStore {
	s := &memStore{
		state: "running", rowState: map[int]string{}, nextRun: 100, future: map[int]bool{},
		settings: dograh.DialSettings{Provider: "ari", Technology: "PJSIP", Trunk: "trunk", FromNumbers: []string{"7317185732"}},
	}

	for i := 1; i <= contacts; i++ {
		s.rows = append(s.rows, dograh.QueuedRun{ID: i, CampaignID: 1, SourceUUID: "row", Variables: map[string]any{"phone_number": "90000000" + string(rune('0'+i%10)) + "0"}})
		s.rowState[i] = "queued"
	}

	return s
}

func (s *memStore) VaaniCampaigns(context.Context) ([]dograh.Campaign, error) { return nil, nil }

func (s *memStore) CampaignRunning(context.Context, int) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.state == "running", nil
}

func (s *memStore) SetCampaignState(_ context.Context, _ int, from, to string) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.state != from {
		return false, nil
	}

	s.state = to
	s.paused = append(s.paused, to)

	return true, nil
}

func (s *memStore) OrgConcurrencyLimit(context.Context, int) (int, bool, error) {
	if s.orgLimit == nil {
		return 0, false, nil
	}

	return *s.orgLimit, true, nil
}

func (s *memStore) WalletBlocks(context.Context, int) bool { return s.wallet }

func (s *memStore) ClaimRuns(_ context.Context, _ int, _ int, _ time.Time) ([]dograh.QueuedRun, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	for _, r := range s.rows {
		if s.rowState[r.ID] == "queued" && !s.future[r.ID] {
			s.rowState[r.ID] = "processing"
			return []dograh.QueuedRun{r}, nil
		}
	}

	return nil, nil
}

func (s *memStore) SetRunState(_ context.Context, id int, state string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.rowState[id] = state

	return nil
}

func (s *memStore) ScheduleRetry(_ context.Context, _ dograh.QueuedRun, reason string, _ time.Duration, _ time.Time) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.retries = append(s.retries, reason)

	id := len(s.rows) + 1
	s.rows = append(s.rows, dograh.QueuedRun{ID: id, CampaignID: 1, SourceUUID: "row_retry", RetryCount: 1, Variables: map[string]any{"phone_number": "9000000099"}})
	s.rowState[id], s.future[id] = "queued", true

	return nil
}

func (s *memStore) OpenWork(context.Context, int) (queued, processing int, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	for _, st := range s.rowState {
		switch st {
		case "queued":
			queued++
		case "processing":
			processing++
		}
	}

	return queued, processing, nil
}

func (s *memStore) CompleteCampaign(context.Context, int) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.state != "running" {
		return false, nil
	}

	s.state = "completed"

	return true, nil
}

func (s *memStore) RecoverContacts(context.Context) (int, int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.recovered++

	return 1, 2, nil
}

func (s *memStore) StaleRuns(_ context.Context, _ time.Time, includeRunning bool, live []int64) ([]dograh.StaleRun, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.staleArgs = append(s.staleArgs, includeRunning)
	s.staleLive = append(s.staleLive, live)

	return s.staleRuns, nil
}

func (s *memStore) MarkInterrupted(_ context.Context, runID int64, _ int) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.interrupted = append(s.interrupted, runID)

	return nil
}

func (s *memStore) AddFailedRow(context.Context, int) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.failedRows++

	return nil
}

func (s *memStore) DialSettings(context.Context, int) (dograh.DialSettings, error) {
	return s.settings, nil
}

func (s *memStore) CreateOutboundRun(context.Context, dograh.Campaign, dograh.QueuedRun, string, string) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.nextRun++

	return s.nextRun, nil
}

func (s *memStore) MarkNotConnected(_ context.Context, _ int64, _ int, status string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.notConn = append(s.notConn, status)

	return nil
}

func (s *memStore) stateOf(id int) string {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.rowState[id]
}

// recPlacer records the calls the Runner places.
type recPlacer struct {
	mu   sync.Mutex
	reqs []ari.DialRequest
	err  error
}

func (p *recPlacer) Dial(_ context.Context, req ari.DialRequest) (string, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.err != nil {
		return "", p.err
	}

	p.reqs = append(p.reqs, req)

	return "chan", nil
}

func (p *recPlacer) dialed() []ari.DialRequest {
	p.mu.Lock()
	defer p.mu.Unlock()

	return append([]ari.DialRequest(nil), p.reqs...)
}

type runnerRig struct {
	*limiterRig
	store  *memStore
	placer *recPlacer
	r      *Runner
	camp   dograh.Campaign
}

func newRunnerRig(t *testing.T, contacts int) *runnerRig {
	t.Helper()

	lr := newRig(t, 0)
	st := newMemStore(contacts)
	pl := &recPlacer{}

	rr := &runnerRig{limiterRig: lr, store: st, placer: pl}
	rr.r = NewRunner(st, lr.l, pl, RunnerConfig{Poll: time.Hour, DefaultOrgLimit: 2, RingTimeout: 30 * time.Second})
	rr.r.now = func() time.Time { return lr.now }

	rr.camp = dograh.Campaign{
		ID: 1, OrganizationID: 7, WorkflowID: 19, RatePerSecond: 100,
		Retry:   dograh.RetryConfig{Enabled: true, MaxRetries: 1, Delay: 2 * time.Minute, OnBusy: true, OnNoAnswer: true},
		Breaker: dograh.BreakerConfig{Enabled: true, Threshold: 0.5, Window: 120 * time.Second, MinCalls: 5},
	}

	return rr
}

// dialAll runs dialOne until the Runner stops or the rate window is full.
func (rr *runnerRig) dialAll(t *testing.T) {
	t.Helper()

	for i := 0; i < 50; i++ {
		res, _ := rr.r.dialOne(context.Background(), rr.camp)
		if res != stepDialed {
			return
		}

		rr.advance(time.Millisecond) // breaker and rate entries are stored under their timestamp
	}

	t.Fatal("dialOne never stopped")
}

func TestRunner_DialsUnderTheOrgLimitInLoadingOrder(t *testing.T) {
	rr := newRunnerRig(t, 5)
	rr.dialAll(t)

	got := rr.placer.dialed()
	require.Len(t, got, 2, "the default organization limit is 2")

	assert.Equal(t, "PJSIP/9000000010@trunk", got[0].Endpoint)
	assert.Equal(t, "7317185732", got[0].CallerID)
	assert.Equal(t, 19, got[0].WorkflowID)
	assert.EqualValues(t, 101, got[0].RunID)
	assert.Equal(t, 30*time.Second, got[0].RingTimeout)
	assert.Equal(t, "processed", rr.store.stateOf(1))
	assert.Equal(t, "processed", rr.store.stateOf(2))
	assert.Equal(t, "queued", rr.store.stateOf(3), "later contacts wait: none is claimed without a free slot")

	n, _ := rr.l.ActiveSlots(context.Background(), 7)
	assert.Equal(t, 2, n)

	// A call ending frees its slot; the next one is dialed.
	got[0].Done(ari.Outcome{Connected: true})
	require.Eventually(t, func() bool { n, _ := rr.l.ActiveSlots(context.Background(), 7); return n == 1 }, time.Second, 5*time.Millisecond)

	rr.dialAll(t)
	assert.Len(t, rr.placer.dialed(), 3)
}

func TestRunner_CampaignAndOrgLimitsTakeTheLower(t *testing.T) {
	rr := newRunnerRig(t, 6)
	ten := 10
	rr.store.orgLimit = &ten
	rr.camp.MaxConcurrency = 3

	rr.dialAll(t)
	assert.Len(t, rr.placer.dialed(), 3, "the campaign's own max_concurrency is lower than the org's 10")
}

func TestRunner_RateLimitWaits(t *testing.T) {
	rr := newRunnerRig(t, 5)
	ten := 10
	rr.store.orgLimit = &ten
	rr.camp.RatePerSecond = 2

	ctx := context.Background()

	res, _ := rr.r.dialOne(ctx, rr.camp)
	assert.Equal(t, stepDialed, res)

	rr.advance(10 * time.Millisecond) // a start is stored under its timestamp: each needs its own

	res, _ = rr.r.dialOne(ctx, rr.camp)
	assert.Equal(t, stepDialed, res)

	rr.advance(10 * time.Millisecond)

	res, wait := rr.r.dialOne(ctx, rr.camp)
	assert.Equal(t, stepWait, res, "a third start inside the second")
	assert.Positive(t, wait)
	assert.Len(t, rr.placer.dialed(), 2)
	assert.Equal(t, "queued", rr.store.stateOf(3), "no contact is claimed while waiting")

	rr.advance(1100 * time.Millisecond)

	res, _ = rr.r.dialOne(ctx, rr.camp)
	assert.Equal(t, stepDialed, res)
}

func TestRunner_DoesNotDialWhenNotAllowed(t *testing.T) {
	ist, err := time.LoadLocation("Asia/Kolkata")
	require.NoError(t, err)

	for name, setup := range map[string]func(*runnerRig){
		"campaign paused in the UI": func(rr *runnerRig) { rr.store.state = "paused" },
		"outside the schedule window": func(rr *runnerRig) {
			// now is 2027-01-15 08:00 UTC = 13:30 IST, a Friday (day 4)
			rr.camp.Schedule = dograh.Schedule{Enabled: true, Location: ist,
				Slots: []dograh.ScheduleSlot{{Day: 0, Start: "09:00", End: "18:00"}}}
		},
	} {
		t.Run(name, func(t *testing.T) {
			rr := newRunnerRig(t, 3)
			rr.limiterRig.now = time.Date(2027, 1, 15, 8, 0, 0, 0, time.UTC)
			setup(rr)

			rr.dialAll(t)
			assert.Empty(t, rr.placer.dialed())
			assert.Equal(t, "queued", rr.store.stateOf(1))
		})
	}
}

func TestRunner_PausesWhenWalletIsOut(t *testing.T) {
	rr := newRunnerRig(t, 3)
	rr.store.wallet = true

	rr.dialAll(t)

	assert.Empty(t, rr.placer.dialed())
	assert.Equal(t, "paused", rr.store.state)
}

func TestRunner_PausesWhenTheBreakerIsOpen(t *testing.T) {
	rr := newRunnerRig(t, 3)

	for i := 0; i < 5; i++ {
		_, _, err := rr.l.RecordOutcome(context.Background(), 1, true, rr.camp.Breaker)
		require.NoError(t, err)
		rr.advance(time.Millisecond)
	}

	rr.dialAll(t)

	assert.Empty(t, rr.placer.dialed())
	assert.Equal(t, "paused", rr.store.state)
}

func TestRunner_FailedCallsTripTheBreaker(t *testing.T) {
	rr := newRunnerRig(t, 12)
	ten := 10
	rr.store.orgLimit = &ten
	rr.camp.RatePerSecond = 1000

	rr.dialAll(t)
	require.GreaterOrEqual(t, len(rr.placer.dialed()), 5)

	for i, req := range rr.placer.dialed()[:5] {
		req.Done(ari.Outcome{Status: dograh.StatusFailed, Cause: 34})
		rr.advance(time.Millisecond)

		if i < 4 {
			assert.Equal(t, "running", rr.store.state, "%d failures are below min_calls", i+1)
		}
	}

	require.Eventually(t, func() bool {
		ok, _ := rr.store.CampaignRunning(context.Background(), 1)
		return !ok
	}, time.Second, 5*time.Millisecond, "5 of 5 failed: paused")
}

func TestRunner_OutcomesDriveRetries(t *testing.T) {
	type want struct {
		retries    []string
		failedRows int
		notConn    string
	}

	for name, tc := range map[string]struct {
		out        ari.Outcome
		retryCount int
		retry      dograh.RetryConfig
		want       want
	}{
		"busy is retried":            {ari.Outcome{Status: dograh.StatusBusy, Cause: 17}, 0, dograh.RetryConfig{Enabled: true, MaxRetries: 1, OnBusy: true}, want{[]string{"busy"}, 0, "busy"}},
		"no answer is retried":       {ari.Outcome{Status: dograh.StatusNoAnswer, Cause: 19}, 0, dograh.RetryConfig{Enabled: true, MaxRetries: 1, OnNoAnswer: true}, want{[]string{"no_answer"}, 0, "no-answer"}},
		"retries used up":            {ari.Outcome{Status: dograh.StatusBusy, Cause: 17}, 1, dograh.RetryConfig{Enabled: true, MaxRetries: 1, OnBusy: true}, want{nil, 1, "busy"}},
		"busy not retried by choice": {ari.Outcome{Status: dograh.StatusBusy, Cause: 17}, 0, dograh.RetryConfig{Enabled: true, MaxRetries: 1}, want{nil, 0, "busy"}},
		"retries off":                {ari.Outcome{Status: dograh.StatusBusy, Cause: 17}, 0, dograh.RetryConfig{OnBusy: true, MaxRetries: 1}, want{nil, 0, "busy"}},
		"rejected is not retried":    {ari.Outcome{Status: dograh.StatusRejected, Cause: 21}, 0, dograh.RetryConfig{Enabled: true, MaxRetries: 1, OnBusy: true}, want{nil, 0, "rejected"}},
		"failed is not retried":      {ari.Outcome{Status: dograh.StatusFailed}, 0, dograh.RetryConfig{Enabled: true, MaxRetries: 1, OnBusy: true}, want{nil, 0, "failed"}},
	} {
		t.Run(name, func(t *testing.T) {
			rr := newRunnerRig(t, 1)
			rr.camp.Retry = tc.retry
			rr.store.rows[0].RetryCount = tc.retryCount

			rr.dialAll(t)
			require.Len(t, rr.placer.dialed(), 1)

			rr.placer.dialed()[0].Done(tc.out)

			require.Eventually(t, func() bool {
				rr.store.mu.Lock()
				defer rr.store.mu.Unlock()

				return len(rr.store.notConn) == 1
			}, time.Second, 5*time.Millisecond)

			rr.store.mu.Lock()
			defer rr.store.mu.Unlock()

			assert.Equal(t, tc.want.retries, rr.store.retries)
			assert.Equal(t, tc.want.failedRows, rr.store.failedRows)
			assert.Equal(t, tc.want.notConn, rr.store.notConn[0])

			n, _ := rr.l.ActiveSlots(context.Background(), 7)
			assert.Zero(t, n, "the slot is back")
			assert.Zero(t, rr.r.InFlight(1))
		})
	}
}

func TestRunner_ConnectedCallDoesNotTouchTheRun(t *testing.T) {
	rr := newRunnerRig(t, 1)
	rr.dialAll(t)

	rr.placer.dialed()[0].Done(ari.Outcome{Connected: true})

	require.Eventually(t, func() bool { return rr.r.InFlight(1) == 0 }, time.Second, 5*time.Millisecond)

	rr.store.mu.Lock()
	defer rr.store.mu.Unlock()

	assert.Empty(t, rr.store.notConn, "the agent completes a connected call's run")
	assert.Empty(t, rr.store.retries)
}

func TestRunner_ContactWithoutANumberFails(t *testing.T) {
	rr := newRunnerRig(t, 2)
	rr.store.rows[0].Variables = map[string]any{"name": "no number"}

	rr.dialAll(t)

	assert.Equal(t, "failed", rr.store.stateOf(1))
	assert.Equal(t, "processed", rr.store.stateOf(2), "the next contact is still dialed")
	assert.Len(t, rr.placer.dialed(), 1)
}

func TestRunner_NumberDeliveredAsANumber(t *testing.T) {
	assert.Equal(t, "919876543210", phoneOf(map[string]any{"phone_number": float64(919876543210)}))
	assert.Equal(t, "+91 98765", phoneOf(map[string]any{"phone_number": "+91 98765"}))
	assert.Empty(t, phoneOf(map[string]any{"phone_number": nil}))
}

func TestRunner_NonARIProviderPausesAndKeepsTheContact(t *testing.T) {
	rr := newRunnerRig(t, 2)
	rr.store.settings.Provider = "twilio"

	rr.dialAll(t)

	assert.Empty(t, rr.placer.dialed())
	assert.Equal(t, "paused", rr.store.state)
	assert.Equal(t, "queued", rr.store.stateOf(1), "given back, not lost")

	n, _ := rr.l.ActiveSlots(context.Background(), 7)
	assert.Zero(t, n)
}

func TestRunner_DialErrorFailsTheContactAndFreesTheSlot(t *testing.T) {
	rr := newRunnerRig(t, 1)
	rr.placer.err = errors.New("ari: 500")

	rr.dialAll(t)

	assert.Equal(t, "failed", rr.store.stateOf(1))
	assert.Equal(t, []string{"failed"}, rr.store.notConn, "its run is completed, not left initialized")

	n, _ := rr.l.ActiveSlots(context.Background(), 7)
	assert.Zero(t, n)
	assert.Zero(t, rr.r.InFlight(1))

	open, stats, err := rr.l.BreakerOpen(context.Background(), 1, rr.camp.Breaker)
	require.NoError(t, err)
	assert.False(t, open)
	assert.Equal(t, 1, stats.Failures, "an initiation failure counts against the breaker")
}

func TestRunner_RunScansAndDials(t *testing.T) {
	rr := newRunnerRig(t, 1)
	camp := rr.camp

	scanning := &scanStore{memStore: rr.store, campaigns: []dograh.Campaign{camp}}
	rr.r.store = scanning

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})

	go func() { rr.r.Run(ctx); close(done) }()

	require.Eventually(t, func() bool { return len(rr.placer.dialed()) == 1 }, 2*time.Second, 5*time.Millisecond)

	cancel()
	<-done
}

type scanStore struct {
	*memStore
	campaigns []dograh.Campaign
}

func (s *scanStore) VaaniCampaigns(context.Context) ([]dograh.Campaign, error) {
	return s.campaigns, nil
}

func TestRunner_CompletesOnlyWhenNothingIsLeft(t *testing.T) {
	rr := newRunnerRig(t, 1)
	ctx := context.Background()

	rr.dialAll(t)
	require.Len(t, rr.placer.dialed(), 1)
	assert.Equal(t, "running", rr.store.state, "the one call is still ringing")

	rr.placer.dialed()[0].Done(ari.Outcome{Connected: true})
	require.Eventually(t, func() bool { return rr.r.InFlight(1) == 0 }, time.Second, 5*time.Millisecond)

	res, _ := rr.r.dialOne(ctx, rr.camp)
	assert.Equal(t, stepStop, res)
	assert.Equal(t, "completed", rr.store.state)
}

func TestRunner_WaitsForARetryBeforeCompleting(t *testing.T) {
	rr := newRunnerRig(t, 1)

	rr.dialAll(t)
	rr.placer.dialed()[0].Done(ari.Outcome{Status: dograh.StatusBusy, Cause: 17})
	require.Eventually(t, func() bool { return rr.r.InFlight(1) == 0 }, time.Second, 5*time.Millisecond)

	rr.dialAll(t)
	assert.Equal(t, "running", rr.store.state, "the busy contact's retry is queued for later")
	assert.Len(t, rr.placer.dialed(), 1, "and not due yet")
}

func TestRunner_RecoveryAndSweep(t *testing.T) {
	rr := newRunnerRig(t, 1)
	ctx := context.Background()

	rr.store.staleRuns = []dograh.StaleRun{{ID: 5, OrgID: 7, State: "initialized"}, {ID: 6, OrgID: 7, State: "running"}}

	rr.r.recover(ctx)

	assert.Equal(t, 1, rr.store.recovered)
	assert.Equal(t, []bool{true}, rr.store.staleArgs, "right after a restart 'running' runs are stale too")
	assert.Equal(t, []string{"failed"}, rr.store.notConn, "a call that never reached the agent")
	assert.Equal(t, []int64{6}, rr.store.interrupted, "a call the restart cut off")

	// Later sweeps leave 'running' runs alone, and skip the calls in flight.
	rr.dialAll(t)
	rr.r.sweep(ctx, false)

	assert.Equal(t, []bool{true, false}, rr.store.staleArgs)
	assert.Equal(t, []int64{101}, rr.store.staleLive[1], "the call in flight is not stale")
}

func counterValue(t *testing.T, c prometheus.Collector) float64 {
	t.Helper()

	ch := make(chan prometheus.Metric, 1)
	c.Collect(ch)

	var m dto.Metric
	require.NoError(t, (<-ch).Write(&m))

	if m.Counter != nil {
		return m.Counter.GetValue()
	}

	return m.Gauge.GetValue()
}

func TestRunner_CountsDialsOutcomesAndRetries(t *testing.T) {
	rr := newRunnerRig(t, 1)

	dials := counterValue(t, metrics.CampaignDialsTotal)
	busy := counterValue(t, metrics.CampaignOutcomesTotal.WithLabelValues("busy"))
	retries := counterValue(t, metrics.CampaignRetriesTotal)
	inflight := counterValue(t, metrics.CampaignCallsInFlight)

	rr.dialAll(t)

	assert.Equal(t, dials+1, counterValue(t, metrics.CampaignDialsTotal))
	assert.Equal(t, inflight+1, counterValue(t, metrics.CampaignCallsInFlight))

	rr.placer.dialed()[0].Done(ari.Outcome{Status: dograh.StatusBusy, Cause: 17})
	require.Eventually(t, func() bool { return rr.r.InFlight(1) == 0 }, time.Second, 5*time.Millisecond)

	assert.Equal(t, busy+1, counterValue(t, metrics.CampaignOutcomesTotal.WithLabelValues("busy")))
	assert.Equal(t, retries+1, counterValue(t, metrics.CampaignRetriesTotal))
	assert.Equal(t, inflight, counterValue(t, metrics.CampaignCallsInFlight))
}
