package dograh

import (
	"context"
	"fmt"
	"os"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSchedule_Allows(t *testing.T) {
	ist, err := time.LoadLocation("Asia/Kolkata")
	require.NoError(t, err)

	weekdays := Schedule{Enabled: true, Location: ist, Slots: []ScheduleSlot{
		{Day: 0, Start: "09:00", End: "18:00"}, // Monday
		{Day: 2, Start: "10:00", End: "12:00"}, // Wednesday
	}}

	// 2026-10-26 is a Monday; 04:00 UTC = 09:30 IST.
	at := func(day, hour, min int) time.Time { return time.Date(2026, 10, day, hour, min, 0, 0, time.UTC) }

	for _, tc := range []struct {
		name string
		s    Schedule
		now  time.Time
		want bool
	}{
		{"inside the Monday window (IST)", weekdays, at(26, 4, 0), true},
		{"window start is inclusive", weekdays, at(26, 3, 30), true},
		{"window end is exclusive", weekdays, at(26, 12, 30), false},
		{"before the window", weekdays, at(26, 3, 29), false},
		{"Tuesday has no slot", weekdays, at(27, 5, 0), false},
		{"Wednesday slot", weekdays, at(28, 5, 0), true},
		{"Sunday maps to 6, no slot", weekdays, at(25, 5, 0), false},
		{"disabled allows all", Schedule{Enabled: false, Location: ist, Slots: weekdays.Slots}, at(27, 5, 0), true},
		{"no slots allows all", Schedule{Enabled: true, Location: ist}, at(27, 5, 0), true},
		{"unknown timezone allows all", Schedule{Enabled: true, Slots: weekdays.Slots}, at(27, 5, 0), true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, tc.s.Allows(tc.now))
		})
	}
}

func TestRetryConfig_Retries(t *testing.T) {
	all := RetryConfig{Enabled: true, MaxRetries: 2, OnBusy: true, OnNoAnswer: true, OnVoicemail: false}

	for _, tc := range []struct {
		name     string
		cfg      RetryConfig
		reason   string
		attempts int
		want     bool
	}{
		{"busy, first failure", all, "busy", 0, true},
		{"no answer, second try", all, "no_answer", 1, true},
		{"max reached", all, "busy", 2, false},
		{"voicemail off", all, "voicemail", 0, false},
		{"a failed call is never retried", all, "failed", 0, false},
		{"disabled", RetryConfig{MaxRetries: 5, OnBusy: true}, "busy", 0, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, tc.cfg.Retries(tc.reason, tc.attempts))
		})
	}
}

func TestParseCampaign(t *testing.T) {
	t.Run("empty JSON gets Dograh's defaults", func(t *testing.T) {
		var c Campaign
		require.NoError(t, parseCampaign(&c, "{}", "{}"))

		assert.Equal(t, RetryConfig{Enabled: true, MaxRetries: 1, Delay: 120 * time.Second,
			OnBusy: true, OnNoAnswer: true, OnVoicemail: true}, c.Retry)
		assert.Equal(t, BreakerConfig{Enabled: true, Threshold: 0.5, Window: 120 * time.Second, MinCalls: 5}, c.Breaker)
		assert.Zero(t, c.MaxConcurrency)
		assert.False(t, c.Schedule.Enabled)
		assert.True(t, c.Schedule.Allows(time.Now()))
	})

	t.Run("settings override", func(t *testing.T) {
		var c Campaign
		require.NoError(t, parseCampaign(&c,
			`{"enabled": true, "max_retries": 3, "retry_delay_seconds": 60, "retry_on_busy": false, "retry_on_voicemail": false}`,
			`{"max_concurrency": 4,
			  "circuit_breaker": {"failure_threshold": 0.8, "window_seconds": 30, "min_calls_in_window": 10},
			  "schedule_config": {"enabled": true, "timezone": "Asia/Kolkata",
				"slots": [{"day_of_week": 0, "start_time": "09:00", "end_time": "18:00"}]}}`))

		assert.Equal(t, 3, c.Retry.MaxRetries)
		assert.Equal(t, time.Minute, c.Retry.Delay)
		assert.False(t, c.Retry.OnBusy)
		assert.True(t, c.Retry.OnNoAnswer, "unset keeps the default")
		assert.False(t, c.Retry.OnVoicemail)
		assert.Equal(t, 4, c.MaxConcurrency)
		assert.Equal(t, BreakerConfig{Enabled: true, Threshold: 0.8, Window: 30 * time.Second, MinCalls: 10}, c.Breaker)
		require.True(t, c.Schedule.Enabled)
		assert.Equal(t, "Asia/Kolkata", c.Schedule.Location.String())
		assert.Equal(t, []ScheduleSlot{{Day: 0, Start: "09:00", End: "18:00"}}, c.Schedule.Slots)
	})

	t.Run("bad timezone leaves the schedule open", func(t *testing.T) {
		var c Campaign
		require.NoError(t, parseCampaign(&c, "{}",
			`{"schedule_config": {"enabled": true, "timezone": "Mars/Base", "slots": [{"day_of_week": 0, "start_time": "09:00", "end_time": "10:00"}]}}`))
		assert.True(t, c.Schedule.Allows(time.Now()))
	})

	t.Run("broken JSON is an error", func(t *testing.T) {
		var c Campaign
		assert.Error(t, parseCampaign(&c, "{nope", "{}"))
		assert.Error(t, parseCampaign(&c, "{}", "[1"))
	})
}

// campaignDB is a Store on a throwaway schema holding just the Dograh columns
// the campaign code uses. It needs CAMPAIGN_TEST_DATABASE_URL (any Postgres
// the test may create and drop a schema in) and is skipped without it.
func campaignDB(t *testing.T) (*Store, *pgxpool.Pool) {
	t.Helper()

	url := os.Getenv("CAMPAIGN_TEST_DATABASE_URL")
	if url == "" {
		t.Skip("CAMPAIGN_TEST_DATABASE_URL not set")
	}

	ctx := context.Background()
	schema := fmt.Sprintf("vaani_test_%d", time.Now().UnixNano())

	admin, err := pgxpool.New(ctx, url)
	require.NoError(t, err)
	t.Cleanup(admin.Close)

	_, err = admin.Exec(ctx, "CREATE SCHEMA "+schema)
	require.NoError(t, err)
	t.Cleanup(func() { _, _ = admin.Exec(ctx, "DROP SCHEMA "+schema+" CASCADE") })

	cfg, err := pgxpool.ParseConfig(url)
	require.NoError(t, err)

	cfg.ConnConfig.RuntimeParams["search_path"] = schema
	cfg.MaxConns = 8

	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	require.NoError(t, err)
	t.Cleanup(pool.Close)

	_, err = pool.Exec(ctx, `
		CREATE TYPE queued_run_state AS ENUM ('queued', 'processed', 'processing', 'failed');
		CREATE TYPE campaign_state AS ENUM ('created', 'syncing', 'running', 'paused', 'completed', 'failed');
		CREATE TABLE organization_configurations (
			id serial PRIMARY KEY, organization_id int NOT NULL, key text NOT NULL, value json NOT NULL,
			UNIQUE (organization_id, key));
		CREATE TABLE campaigns (
			id serial PRIMARY KEY, organization_id int NOT NULL, workflow_id int NOT NULL DEFAULT 1,
			created_by int NOT NULL DEFAULT 1, state campaign_state NOT NULL DEFAULT 'created',
			rate_limit_per_second int NOT NULL DEFAULT 1, processed_rows int NOT NULL DEFAULT 0,
			failed_rows int NOT NULL DEFAULT 0, retry_config json, orchestrator_metadata json, completed_at timestamptz);
		CREATE TABLE queued_runs (
			id serial PRIMARY KEY, campaign_id int NOT NULL REFERENCES campaigns(id),
			source_uuid text NOT NULL, context_variables json NOT NULL DEFAULT '{}',
			state queued_run_state NOT NULL DEFAULT 'queued', created_at timestamptz DEFAULT now(),
			processed_at timestamptz, retry_count int NOT NULL DEFAULT 0,
			parent_queued_run_id int REFERENCES queued_runs(id), scheduled_for timestamptz,
			retry_reason text, UNIQUE (campaign_id, source_uuid, retry_count));
		CREATE TABLE organizations (id int PRIMARY KEY, billing_mode varchar(10), billing_rate_per_min_inr numeric(10,4),
			credit_limit_inr numeric(14,2), wallet_balance_inr numeric(14,2) NOT NULL DEFAULT 0);
		CREATE TABLE workflows (id serial PRIMARY KEY, organization_id int, released_definition_id int);
		CREATE TABLE workflow_definitions (id serial PRIMARY KEY, workflow_id int NOT NULL, is_current bool DEFAULT false);
		CREATE TABLE workflow_runs (
			id serial PRIMARY KEY, name text, workflow_id int, definition_id int, mode text, call_type text,
			state text, is_completed bool, usage_info json, cost_info json, initial_context json,
			gathered_context json, logs json, annotations json, created_at timestamptz,
			campaign_id int, queued_run_id int)`)
	require.NoError(t, err)

	return &Store{pool: pool}, pool
}

func sqlExec(t *testing.T, pool *pgxpool.Pool, sql string, args ...any) {
	t.Helper()

	_, err := pool.Exec(context.Background(), sql, args...)
	require.NoError(t, err)
}

func TestVaaniCampaigns_OnlyRunningCampaignsOfVaaniOrgs(t *testing.T) {
	s, pool := campaignDB(t)

	sqlExec(t, pool, `
		INSERT INTO organization_configurations (organization_id, key, value) VALUES
			(1, 'MEDIA_ENGINE', '{"value": "vaani"}'), (2, 'MEDIA_ENGINE', '{"value": "pipecat"}');
		INSERT INTO campaigns (organization_id, state, rate_limit_per_second, orchestrator_metadata) VALUES
			(1, 'running', 3, '{"max_concurrency": 5}'),   -- 1: dialed by Vaani
			(1, 'paused',  1, '{}'),                        -- 2: not running
			(2, 'running', 1, '{}'),                        -- 3: pipecat org
			(3, 'running', 1, '{}'),                        -- 4: org with no setting
			(1, 'running', 1, '{"max_concurrency": "x"}')   -- 5: unreadable settings`)

	got, err := s.VaaniCampaigns(context.Background())
	require.Error(t, err, "the unreadable campaign is reported...")
	require.Len(t, got, 1, "...and the good one is still returned")

	assert.Equal(t, 1, got[0].ID)
	assert.Equal(t, 3, got[0].RatePerSecond)
	assert.Equal(t, 5, got[0].MaxConcurrency)
}

func TestOrgConcurrencyLimit(t *testing.T) {
	s, pool := campaignDB(t)
	sqlExec(t, pool, `INSERT INTO organization_configurations (organization_id, key, value)
		VALUES (1, 'CONCURRENT_CALL_LIMIT', '{"value": 12}')`)

	n, ok, err := s.OrgConcurrencyLimit(context.Background(), 1)
	require.NoError(t, err)
	assert.True(t, ok)
	assert.Equal(t, 12, n)

	_, ok, err = s.OrgConcurrencyLimit(context.Background(), 2)
	require.NoError(t, err)
	assert.False(t, ok, "unset: the caller's default applies")
}

func TestClaimRuns_RetriesFirstThenLoadingOrder(t *testing.T) {
	s, pool := campaignDB(t)
	now := time.Now()

	sqlExec(t, pool, `INSERT INTO campaigns (organization_id, state) VALUES (1, 'running')`)
	sqlExec(t, pool, `
		INSERT INTO queued_runs (campaign_id, source_uuid, scheduled_for, retry_count) VALUES
			(1, 'a', NULL, 0), (1, 'b', NULL, 0), (1, 'c', NULL, 0),
			(1, 'a_retry_1', $1, 1),             -- due retry
			(1, 'z_retry_1', $2, 1)              -- retry not due yet`,
		now.Add(-time.Minute), now.Add(time.Hour))

	got, err := s.ClaimRuns(context.Background(), 1, 3, now)
	require.NoError(t, err)

	var sources []string
	for _, r := range got {
		sources = append(sources, r.SourceUUID)
	}

	sort.Strings(sources)
	assert.Equal(t, []string{"a", "a_retry_1", "b"}, sources, "the due retry first, then rows in loading order; the future retry stays")

	again, err := s.ClaimRuns(context.Background(), 1, 10, now)
	require.NoError(t, err)
	require.Len(t, again, 1, "only 'c' is left: claimed rows are 'processing', the future retry isn't due")
	assert.Equal(t, "c", again[0].SourceUUID)
}

func TestClaimRuns_TwoClaimersNeverShareARow(t *testing.T) {
	s, pool := campaignDB(t)

	sqlExec(t, pool, `INSERT INTO campaigns (organization_id, state) VALUES (1, 'running')`)
	sqlExec(t, pool, `INSERT INTO queued_runs (campaign_id, source_uuid)
		SELECT 1, 'row' || g FROM generate_series(1, 200) g`)

	var (
		mu   sync.Mutex
		seen = map[int]int{}
		wg   sync.WaitGroup
	)

	for w := 0; w < 8; w++ {
		wg.Add(1)

		go func() {
			defer wg.Done()

			for {
				runs, err := s.ClaimRuns(context.Background(), 1, 7, time.Now())
				assert.NoError(t, err)

				if len(runs) == 0 {
					return
				}

				mu.Lock()
				for _, r := range runs {
					seen[r.ID]++
				}
				mu.Unlock()
			}
		}()
	}

	wg.Wait()

	assert.Len(t, seen, 200, "every row was claimed")

	for id, n := range seen {
		assert.Equal(t, 1, n, "row %d was claimed %d times", id, n)
	}
}

func TestSetRunState_CountsInTheCampaign(t *testing.T) {
	s, pool := campaignDB(t)
	ctx := context.Background()

	sqlExec(t, pool, `INSERT INTO campaigns (organization_id, state) VALUES (1, 'running')`)
	sqlExec(t, pool, `INSERT INTO queued_runs (campaign_id, source_uuid, state)
		VALUES (1, 'a', 'processing'), (1, 'b', 'processing'), (1, 'c', 'processing')`)

	require.NoError(t, s.SetRunState(ctx, 1, RunProcessed))
	require.NoError(t, s.SetRunState(ctx, 2, RunFailed))
	require.NoError(t, s.SetRunState(ctx, 3, RunQueued))
	assert.Error(t, s.SetRunState(ctx, 3, "weird"))
	assert.Error(t, s.SetRunState(ctx, 99, RunProcessed), "no such run")

	var processed, failed int
	require.NoError(t, pool.QueryRow(ctx, `SELECT processed_rows, failed_rows FROM campaigns WHERE id = 1`).Scan(&processed, &failed))
	assert.Equal(t, 1, processed)
	assert.Equal(t, 1, failed)

	var state string
	require.NoError(t, pool.QueryRow(ctx, `SELECT state::text FROM queued_runs WHERE id = 3`).Scan(&state))
	assert.Equal(t, "queued", state, "given back")
}

func TestScheduleRetry(t *testing.T) {
	s, pool := campaignDB(t)
	ctx := context.Background()
	now := time.Now()

	sqlExec(t, pool, `INSERT INTO campaigns (organization_id, state) VALUES (1, 'running')`)
	sqlExec(t, pool, `INSERT INTO queued_runs (campaign_id, source_uuid, context_variables, state)
		VALUES (1, 'row1', '{"phone_number": "+911234567890"}', 'processing')`)

	orig := QueuedRun{ID: 1, CampaignID: 1, SourceUUID: "row1", Variables: map[string]any{"phone_number": "+911234567890"}}

	require.NoError(t, s.ScheduleRetry(ctx, orig, "busy", 2*time.Minute, now))
	require.NoError(t, s.ScheduleRetry(ctx, orig, "busy", 2*time.Minute, now), "a repeat queues nothing new")

	var (
		n       int
		source  string
		state   string
		count   int
		parent  int
		reason  string
		isRetry bool
		due     time.Time
	)

	require.NoError(t, pool.QueryRow(ctx, `SELECT count(*) FROM queued_runs WHERE campaign_id = 1`).Scan(&n))
	assert.Equal(t, 2, n)

	require.NoError(t, pool.QueryRow(ctx, `
		SELECT source_uuid, state::text, retry_count, parent_queued_run_id, retry_reason,
			(context_variables->>'is_retry')::bool, scheduled_for
		FROM queued_runs WHERE id = 2`).Scan(&source, &state, &count, &parent, &reason, &isRetry, &due))

	assert.Equal(t, "row1_retry_1", source)
	assert.Equal(t, "queued", state)
	assert.Equal(t, 1, count)
	assert.Equal(t, 1, parent)
	assert.Equal(t, "busy", reason)
	assert.True(t, isRetry)
	assert.WithinDuration(t, now.Add(2*time.Minute), due, time.Second)

	// Not claimable before it is due, claimable after.
	got, err := s.ClaimRuns(ctx, 1, 10, now)
	require.NoError(t, err)
	assert.Empty(t, got)

	got, err = s.ClaimRuns(ctx, 1, 10, now.Add(3*time.Minute))
	require.NoError(t, err)
	require.Len(t, got, 1)
	assert.Equal(t, true, got[0].Variables["is_retry"])
	assert.EqualValues(t, 1, got[0].Variables["retry_attempt"])
	assert.Equal(t, "+911234567890", got[0].Variables["phone_number"])
}

func TestDialSettings_EndpointAndCallerID(t *testing.T) {
	d := DialSettings{Technology: "PJSIP", Trunk: "voice_agent_241", StripPrefix: "+91",
		FromNumbers: []string{"7317185732", "7000000000"}, WorkflowCLI: map[int]string{19: "7317185799"}}

	for number, want := range map[string]string{
		"+919876543210":    "PJSIP/9876543210@voice_agent_241",
		"9876543210":       "PJSIP/9876543210@voice_agent_241",
		"PJSIP/100@pbx":    "PJSIP/100@pbx",
		"SIP/200":          "SIP/200",
		"+91+919876543210": "PJSIP/+919876543210@voice_agent_241",
	} {
		assert.Equal(t, want, d.Endpoint(number), number)
	}

	assert.Equal(t, "SIP/12345", DialSettings{Technology: "SIP"}.Endpoint("12345"), "no trunk")
	assert.Equal(t, "PJSIP/12345", DialSettings{}.Endpoint("12345"), "technology defaults to PJSIP")

	assert.Equal(t, "7317185799", d.CallerID(19), "the number mapped to the workflow")
	assert.Equal(t, "7317185732", d.CallerID(20), "else the first from-number")
	assert.Empty(t, DialSettings{}.CallerID(19))
}

func TestStatusForCause(t *testing.T) {
	for cause, want := range map[int]string{
		17: StatusBusy, 19: StatusNoAnswer, 21: StatusRejected,
		16: StatusFailed, 18: StatusFailed, 34: StatusFailed, 0: StatusFailed,
	} {
		assert.Equal(t, want, StatusForCause(cause), "cause %d", cause)
	}
}

func TestNotConnectedDisposition(t *testing.T) {
	for status, want := range map[string]string{
		"busy": "BUSY", "no-answer": "NO_ANSWER", "rejected": "REJECTED", "failed": "FAILED",
		"error": "FAILED", "canceled": "CANCELED", "some-new-one": "SOME_NEW_ONE",
	} {
		assert.Equal(t, want, notConnectedDisposition(status), status)
	}
}

func TestOutboundRun_Lifecycle(t *testing.T) {
	s, pool := campaignDB(t)
	ctx := context.Background()

	sqlExec(t, pool, `
		INSERT INTO workflows (id, organization_id, released_definition_id) VALUES (5, 1, 77);
		INSERT INTO workflow_definitions (workflow_id, is_current) VALUES (5, true);
		INSERT INTO campaigns (organization_id, workflow_id, state) VALUES (1, 5, 'running');
		INSERT INTO queued_runs (campaign_id, source_uuid) VALUES (1, 'row1');
		INSERT INTO organization_configurations (organization_id, key, value) VALUES
			(1, 'DISPOSITION_CODE_MAPPING', '{"BUSY": "B1"}'),
			(1, 'TELEPHONY_CONFIGURATION', '{"provider": "ari", "ari_endpoint": "http://x", "app_password": "secret",
				"outbound_trunk": "trunkA", "strip_number_prefix": "0", "from_numbers": "7317185732",
				"number_workflow_map": {"7317185799": "5"}}')`)

	ds, err := s.DialSettings(ctx, 1)
	require.NoError(t, err)
	assert.Equal(t, DialSettings{Provider: "ari", Technology: "PJSIP", Trunk: "trunkA", StripPrefix: "0",
		FromNumbers: []string{"7317185732"}, WorkflowCLI: map[int]string{5: "7317185799"}}, ds)

	_, err = s.DialSettings(ctx, 2)
	assert.Error(t, err, "no telephony configuration")

	camp := Campaign{ID: 1, OrganizationID: 1, WorkflowID: 5}
	q := QueuedRun{ID: 1, CampaignID: 1, SourceUUID: "row1", Variables: map[string]any{"name": "Asha"}}

	id, err := s.CreateOutboundRun(ctx, camp, q, "9876543210", "7317185799")
	require.NoError(t, err)

	var (
		name, mode, callType, state string
		def, campaignID, queuedID   int
		completed                   bool
		initial                     string
	)

	row := func() {
		require.NoError(t, pool.QueryRow(ctx, `SELECT name, mode, call_type, state, definition_id, campaign_id,
			queued_run_id, is_completed, initial_context::text FROM workflow_runs WHERE id = $1`, id).
			Scan(&name, &mode, &callType, &state, &def, &campaignID, &queuedID, &completed, &initial))
	}
	row()

	assert.Equal(t, "WR-CAMPAIGN-1-1", name)
	assert.Equal(t, []string{"ari", "outbound", "initialized"}, []string{mode, callType, state})
	assert.Equal(t, 77, def, "the published definition")
	assert.Equal(t, [3]int{1, 1, 0}, [3]int{campaignID, queuedID, 0})
	assert.False(t, completed)
	assert.JSONEq(t, `{"name": "Asha", "campaign_id": 1, "provider": "ari", "source_uuid": "row1",
		"caller_number": "7317185799", "called_number": "9876543210"}`, initial)

	_, err = s.CreateOutboundRun(ctx, Campaign{ID: 1, WorkflowID: 99}, q, "1", "2")
	assert.Error(t, err, "unknown workflow")

	require.NoError(t, s.MarkRunRunning(ctx, id))
	row()
	assert.Equal(t, "running", state)

	// A call nobody answered: completed, tagged, disposition mapped by the org.
	sqlExec(t, pool, `UPDATE workflow_runs SET gathered_context = '{"call_tags": ["retry"], "provider": "ari"}' WHERE id = $1`, id)
	require.NoError(t, s.MarkNotConnected(ctx, id, 1, StatusBusy))

	var gathered string
	require.NoError(t, pool.QueryRow(ctx, `SELECT gathered_context::text, state, is_completed FROM workflow_runs WHERE id = $1`, id).
		Scan(&gathered, &state, &completed))

	assert.Equal(t, "completed", state)
	assert.True(t, completed)
	assert.JSONEq(t, `{"call_tags": ["retry", "not_connected", "telephony_busy"], "provider": "ari",
		"call_disposition": "BUSY", "mapped_call_disposition": "B1"}`, gathered)
}

func TestCampaignState(t *testing.T) {
	s, pool := campaignDB(t)
	ctx := context.Background()

	sqlExec(t, pool, `INSERT INTO campaigns (organization_id, state) VALUES (1, 'running')`)

	running, err := s.CampaignRunning(ctx, 1)
	require.NoError(t, err)
	assert.True(t, running)

	running, err = s.CampaignRunning(ctx, 99)
	require.NoError(t, err)
	assert.False(t, running, "no such campaign")

	moved, err := s.SetCampaignState(ctx, 1, "paused", "completed")
	require.NoError(t, err)
	assert.False(t, moved, "it is not paused: a state changed meanwhile wins")

	moved, err = s.SetCampaignState(ctx, 1, "running", "paused")
	require.NoError(t, err)
	assert.True(t, moved)

	running, _ = s.CampaignRunning(ctx, 1)
	assert.False(t, running)

	require.NoError(t, s.AddFailedRow(ctx, 1))
	require.NoError(t, s.AddFailedRow(ctx, 1))

	var failed int
	require.NoError(t, pool.QueryRow(ctx, `SELECT failed_rows FROM campaigns WHERE id = 1`).Scan(&failed))
	assert.Equal(t, 2, failed)
}

func TestWalletBlocks(t *testing.T) {
	s, pool := campaignDB(t)
	ctx := context.Background()

	sqlExec(t, pool, `INSERT INTO organizations (id, billing_mode, billing_rate_per_min_inr, credit_limit_inr, wallet_balance_inr) VALUES
		(1, NULL, NULL, NULL, 0),            -- billing off
		(2, 'prepaid', 1.5, NULL, 1.4),      -- less than a minute left
		(3, 'prepaid', 1.5, NULL, 1.5),      -- exactly a minute
		(4, 'postpaid', 1.5, 100, -100),     -- at the credit limit
		(5, 'postpaid', 1.5, 100, -99.99),
		(6, 'postpaid', 1.5, NULL, -5000)    -- no limit`)

	for org, want := range map[int]bool{1: false, 2: true, 3: false, 4: true, 5: false, 6: false, 99: false} {
		assert.Equal(t, want, s.WalletBlocks(ctx, org), "org %d", org)
	}
}

func TestRecoverContacts_OnlyVaaniCampaigns(t *testing.T) {
	s, pool := campaignDB(t)
	ctx := context.Background()

	sqlExec(t, pool, `
		INSERT INTO organization_configurations (organization_id, key, value) VALUES
			(1, 'MEDIA_ENGINE', '{"value": "vaani"}'), (2, 'MEDIA_ENGINE', '{"value": "pipecat"}');
		INSERT INTO campaigns (organization_id, state) VALUES (1, 'running'), (2, 'running');
		INSERT INTO queued_runs (campaign_id, source_uuid, state) VALUES
			(1, 'never-dialed', 'processing'),   -- 1
			(1, 'dialed', 'processing'),         -- 2: has a run
			(1, 'fine', 'queued'),               -- 3
			(2, 'dograhs-own', 'processing');    -- 4: a pipecat org's, in flight in Dograh
		INSERT INTO workflow_runs (queued_run_id, campaign_id, call_type, state, is_completed) VALUES (2, 1, 'outbound', 'initialized', false)`)

	requeued, processed, err := s.RecoverContacts(ctx)
	require.NoError(t, err)
	assert.Equal(t, 1, requeued)
	assert.Equal(t, 1, processed)

	states := map[int]string{}
	rows, err := pool.Query(ctx, `SELECT id, state::text FROM queued_runs`)
	require.NoError(t, err)

	for rows.Next() {
		var id int
		var st string
		require.NoError(t, rows.Scan(&id, &st))
		states[id] = st
	}

	rows.Close()

	assert.Equal(t, map[int]string{1: "queued", 2: "processed", 3: "queued", 4: "processing"}, states,
		"a contact with a run is never redialed; Dograh's own campaign is untouched")
}

func TestOpenWorkAndComplete(t *testing.T) {
	s, pool := campaignDB(t)
	ctx := context.Background()

	sqlExec(t, pool, `
		INSERT INTO campaigns (organization_id, state) VALUES (1, 'running'), (1, 'paused');
		INSERT INTO queued_runs (campaign_id, source_uuid, state, scheduled_for) VALUES
			(1, 'a', 'queued', NULL), (1, 'b', 'queued', now() + interval '1 hour'),
			(1, 'c', 'processing', NULL), (1, 'd', 'processed', NULL), (1, 'e', 'failed', NULL)`)

	queued, processing, err := s.OpenWork(ctx, 1)
	require.NoError(t, err)
	assert.Equal(t, 2, queued, "a retry due in an hour still counts")
	assert.Equal(t, 1, processing)

	done, err := s.CompleteCampaign(ctx, 2)
	require.NoError(t, err)
	assert.False(t, done, "only a running campaign completes")

	done, err = s.CompleteCampaign(ctx, 1)
	require.NoError(t, err)
	assert.True(t, done)

	var state string
	var completedAt *time.Time
	require.NoError(t, pool.QueryRow(ctx, `SELECT state::text, completed_at FROM campaigns WHERE id = 1`).Scan(&state, &completedAt))
	assert.Equal(t, "completed", state)
	assert.NotNil(t, completedAt)
}

func TestStaleRuns(t *testing.T) {
	s, pool := campaignDB(t)
	ctx := context.Background()
	now := time.Now()

	sqlExec(t, pool, `
		INSERT INTO organization_configurations (organization_id, key, value) VALUES
			(1, 'MEDIA_ENGINE', '{"value": "vaani"}'), (2, 'MEDIA_ENGINE', '{"value": "pipecat"}');
		INSERT INTO campaigns (organization_id, state) VALUES (1, 'running'), (2, 'running');
		INSERT INTO workflow_runs (campaign_id, call_type, state, is_completed, created_at, gathered_context) VALUES
			(1, 'outbound', 'initialized', false, now() - interval '10 minutes', '{}'),  -- 1: never reached the agent
			(1, 'outbound', 'initialized', false, now(), '{}'),                          -- 2: ringing now
			(1, 'outbound', 'running',     false, now() - interval '10 minutes', '{}'),  -- 3: cut off, or still going
			(1, 'outbound', 'initialized', false, now() - interval '10 minutes', '{}'),  -- 4: live in this process
			(1, 'outbound', 'initialized', true,  now() - interval '10 minutes', '{}'),  -- 5: completed
			(2, 'outbound', 'initialized', false, now() - interval '10 minutes', '{}'),  -- 6: a pipecat org's
			(NULL, 'outbound', 'initialized', false, now() - interval '10 minutes', '{}') -- 7: no campaign`)

	ids := func(runs []StaleRun) []int64 {
		var out []int64
		for _, r := range runs {
			out = append(out, r.ID)
		}

		return out
	}

	got, err := s.StaleRuns(ctx, now.Add(-time.Minute), false, []int64{4})
	require.NoError(t, err)
	assert.Equal(t, []int64{1}, ids(got), "later sweeps: only a call that never reached the agent")

	got, err = s.StaleRuns(ctx, now.Add(-time.Minute), true, nil)
	require.NoError(t, err)
	assert.Equal(t, []int64{1, 3, 4}, ids(got), "after a restart: a running run is stale too, and nothing is live")
	assert.Equal(t, StaleRun{ID: 3, OrgID: 1, State: "running"}, got[1])

	require.NoError(t, s.MarkInterrupted(ctx, 3, 1))

	var gathered, state string
	require.NoError(t, pool.QueryRow(ctx, `SELECT gathered_context::text, state FROM workflow_runs WHERE id = 3`).Scan(&gathered, &state))
	assert.Equal(t, "completed", state)
	assert.JSONEq(t, `{"call_tags": ["interrupted"], "call_disposition": "INTERRUPTED", "mapped_call_disposition": "INTERRUPTED"}`, gathered)
}
