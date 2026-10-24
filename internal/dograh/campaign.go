package dograh

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"
	_ "time/tzdata" // schedule timezones must resolve on hosts without a zone database

	"github.com/jackc/pgx/v5"
)

// Campaign tables are Dograh's (campaigns, queued_runs); Vaani dials the
// campaigns of organizations whose MEDIA_ENGINE setting is "vaani" and Dograh
// leaves those alone (api/services/campaign/media_engine.py). The defaults
// below are Dograh's, so a campaign behaves the same whichever engine dials.

// Campaign is a running campaign Vaani dials.
type Campaign struct {
	ID             int
	OrganizationID int
	WorkflowID     int
	CreatedBy      int
	RatePerSecond  int
	// MaxConcurrency lowers the organization's limit for this campaign; 0 = none.
	MaxConcurrency int
	Retry          RetryConfig
	Schedule       Schedule
	Breaker        BreakerConfig
}

// RetryConfig is campaigns.retry_config.
type RetryConfig struct {
	Enabled     bool
	MaxRetries  int
	Delay       time.Duration
	OnBusy      bool
	OnNoAnswer  bool
	OnVoicemail bool
}

// Retries says whether a call that ended for reason ("busy", "no_answer",
// "voicemail") is tried again after attempts earlier tries.
func (r RetryConfig) Retries(reason string, attempts int) bool {
	if !r.Enabled || attempts >= r.MaxRetries {
		return false
	}

	switch reason {
	case "busy":
		return r.OnBusy
	case "no_answer":
		return r.OnNoAnswer
	case "voicemail":
		return r.OnVoicemail
	}

	return false
}

// BreakerConfig is orchestrator_metadata.circuit_breaker.
type BreakerConfig struct {
	Enabled   bool
	Threshold float64
	Window    time.Duration
	MinCalls  int
}

// Schedule is orchestrator_metadata.schedule_config: when calls may be placed.
type Schedule struct {
	Enabled  bool
	Location *time.Location
	Slots    []ScheduleSlot
}

// ScheduleSlot is one weekly window; Day is Python's weekday (0 = Monday) and
// Start/End are "HH:MM" strings, compared as text as Dograh does.
type ScheduleSlot struct {
	Day        int
	Start, End string
}

// Allows says whether now is inside a window. No schedule, no slots or an
// unknown timezone allow every time, as Dograh does.
func (s Schedule) Allows(now time.Time) bool {
	if !s.Enabled || len(s.Slots) == 0 || s.Location == nil {
		return true
	}

	local := now.In(s.Location)
	day := (int(local.Weekday()) + 6) % 7
	hm := local.Format("15:04")

	for _, slot := range s.Slots {
		if slot.Day == day && slot.Start <= hm && hm < slot.End {
			return true
		}
	}

	return false
}

// QueuedRun is one contact to call, a row of queued_runs.
type QueuedRun struct {
	ID          int
	CampaignID  int
	SourceUUID  string
	Variables   map[string]any
	RetryCount  int
	RetryReason string
}

type campaignJSON struct {
	Retry struct {
		Enabled     *bool `json:"enabled"`
		MaxRetries  *int  `json:"max_retries"`
		Delay       *int  `json:"retry_delay_seconds"`
		OnBusy      *bool `json:"retry_on_busy"`
		OnNoAnswer  *bool `json:"retry_on_no_answer"`
		OnVoicemail *bool `json:"retry_on_voicemail"`
	}
	Meta struct {
		MaxConcurrency *int `json:"max_concurrency"`
		Schedule       *struct {
			Enabled  bool   `json:"enabled"`
			Timezone string `json:"timezone"`
			Slots    []struct {
				Day   int    `json:"day_of_week"`
				Start string `json:"start_time"`
				End   string `json:"end_time"`
			} `json:"slots"`
		} `json:"schedule_config"`
		Breaker *struct {
			Enabled   *bool    `json:"enabled"`
			Threshold *float64 `json:"failure_threshold"`
			Window    *int     `json:"window_seconds"`
			MinCalls  *int     `json:"min_calls_in_window"`
		} `json:"circuit_breaker"`
	}
}

func orBool(p *bool, def bool) bool {
	if p == nil {
		return def
	}

	return *p
}

func orInt(p *int, def int) int {
	if p == nil {
		return def
	}

	return *p
}

// parseCampaign fills c's retry, schedule and breaker settings from the two
// JSON columns, with Dograh's defaults for anything missing.
func parseCampaign(c *Campaign, retryJSON, metaJSON string) error {
	var j campaignJSON

	if retryJSON != "" {
		if err := json.Unmarshal([]byte(retryJSON), &j.Retry); err != nil {
			return fmt.Errorf("retry_config: %w", err)
		}
	}

	if metaJSON != "" {
		if err := json.Unmarshal([]byte(metaJSON), &j.Meta); err != nil {
			return fmt.Errorf("orchestrator_metadata: %w", err)
		}
	}

	r := j.Retry
	c.Retry = RetryConfig{
		Enabled:     orBool(r.Enabled, true),
		MaxRetries:  orInt(r.MaxRetries, 1),
		Delay:       time.Duration(orInt(r.Delay, 120)) * time.Second,
		OnBusy:      orBool(r.OnBusy, true),
		OnNoAnswer:  orBool(r.OnNoAnswer, true),
		OnVoicemail: orBool(r.OnVoicemail, true),
	}

	c.MaxConcurrency = orInt(j.Meta.MaxConcurrency, 0)

	c.Breaker = BreakerConfig{Enabled: true, Threshold: 0.5, Window: 120 * time.Second, MinCalls: 5}
	if b := j.Meta.Breaker; b != nil {
		c.Breaker.Enabled = orBool(b.Enabled, true)
		if b.Threshold != nil {
			c.Breaker.Threshold = *b.Threshold
		}

		c.Breaker.Window = time.Duration(orInt(b.Window, 120)) * time.Second
		c.Breaker.MinCalls = orInt(b.MinCalls, 5)
	}

	if sc := j.Meta.Schedule; sc != nil {
		c.Schedule.Enabled = sc.Enabled

		// An unknown timezone leaves Location nil: every time is allowed.
		if loc, err := time.LoadLocation(orStr(sc.Timezone, "UTC")); err == nil {
			c.Schedule.Location = loc
		}

		for _, s := range sc.Slots {
			c.Schedule.Slots = append(c.Schedule.Slots, ScheduleSlot{Day: s.Day, Start: s.Start, End: s.End})
		}
	}

	return nil
}

func orStr(s, def string) string {
	if s == "" {
		return def
	}

	return s
}

// VaaniCampaigns lists the running campaigns of organizations whose media
// engine is "vaani". A campaign with unreadable settings is skipped (and
// reported in the error) rather than blocking the others.
func (s *Store) VaaniCampaigns(ctx context.Context) ([]Campaign, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT c.id, c.organization_id, c.workflow_id, c.created_by, c.rate_limit_per_second,
			COALESCE(c.retry_config::text, ''), COALESCE(c.orchestrator_metadata::text, '')
		FROM campaigns c
		JOIN organization_configurations oc
			ON oc.organization_id = c.organization_id AND oc.key = 'MEDIA_ENGINE'
		WHERE c.state = 'running' AND oc.value::text::jsonb ->> 'value' = 'vaani'
		ORDER BY c.id`)
	if err != nil {
		return nil, fmt.Errorf("dograh: list campaigns: %w", err)
	}
	defer rows.Close()

	var (
		out  []Campaign
		bad  []error
		rate int
	)

	for rows.Next() {
		var (
			c               Campaign
			retryJSON, meta string
		)

		if err := rows.Scan(&c.ID, &c.OrganizationID, &c.WorkflowID, &c.CreatedBy, &rate, &retryJSON, &meta); err != nil {
			return out, fmt.Errorf("dograh: read campaign: %w", err)
		}

		c.RatePerSecond = max(rate, 1)

		if err := parseCampaign(&c, retryJSON, meta); err != nil {
			bad = append(bad, fmt.Errorf("campaign %d: %w", c.ID, err))

			continue
		}

		out = append(out, c)
	}

	if err := rows.Err(); err != nil {
		return out, fmt.Errorf("dograh: read campaigns: %w", err)
	}

	return out, errors.Join(bad...)
}

// OrgConcurrencyLimit is the organization's CONCURRENT_CALL_LIMIT, and false
// when it has none set (the caller then uses its default).
func (s *Store) OrgConcurrencyLimit(ctx context.Context, orgID int) (int, bool, error) {
	var raw string

	err := s.pool.QueryRow(ctx, `
		SELECT value::text FROM organization_configurations
		WHERE organization_id = $1 AND key = 'CONCURRENT_CALL_LIMIT'`, orgID).Scan(&raw)
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, false, nil
	}

	if err != nil {
		return 0, false, fmt.Errorf("dograh: org %d concurrency: %w", orgID, err)
	}

	var v struct {
		Value *int `json:"value"`
	}

	if json.Unmarshal([]byte(raw), &v) != nil || v.Value == nil {
		return 0, false, nil
	}

	return *v.Value, true, nil
}

// ClaimRuns takes up to limit runs that are due, scheduled retries first (the
// earliest first), then the rest in the order they were loaded, and marks them
// 'processing'. FOR UPDATE SKIP LOCKED means two claimers never get the same
// row. Dograh picks the regular rows at random; loading order is kept here so
// a CSV is called top to bottom.
func (s *Store) ClaimRuns(ctx context.Context, campaignID, limit int, now time.Time) ([]QueuedRun, error) {
	rows, err := s.pool.Query(ctx, `
		WITH due AS (
			SELECT id FROM queued_runs
			WHERE campaign_id = $1 AND state = 'queued'
				AND (scheduled_for IS NULL OR scheduled_for <= $2)
			ORDER BY (scheduled_for IS NULL), scheduled_for, id
			LIMIT $3
			FOR UPDATE SKIP LOCKED)
		UPDATE queued_runs q SET state = 'processing'
		FROM due WHERE q.id = due.id
		RETURNING q.id, q.campaign_id, q.source_uuid, COALESCE(q.context_variables::text, '{}'),
			q.retry_count, COALESCE(q.retry_reason, '')`, campaignID, now, limit)
	if err != nil {
		return nil, fmt.Errorf("dograh: claim runs: %w", err)
	}
	defer rows.Close()

	var out []QueuedRun

	for rows.Next() {
		var (
			r    QueuedRun
			vars string
		)

		if err := rows.Scan(&r.ID, &r.CampaignID, &r.SourceUUID, &vars, &r.RetryCount, &r.RetryReason); err != nil {
			return out, fmt.Errorf("dograh: read run: %w", err)
		}

		if err := json.Unmarshal([]byte(vars), &r.Variables); err != nil {
			return out, fmt.Errorf("dograh: run %d variables: %w", r.ID, err)
		}

		out = append(out, r)
	}

	return out, rows.Err()
}

// FinishRun states a claimed row can be moved to.
const (
	RunProcessed = "processed" // the call was placed
	RunFailed    = "failed"    // it never will be
	RunQueued    = "queued"    // give it back
)

// SetRunState moves a claimed run to state. 'processed' also counts it in the
// campaign (processed_rows) and stamps processed_at; 'failed' counts it in
// failed_rows. Both counters are incremented in SQL, so concurrent updates
// can't lose one.
func (s *Store) SetRunState(ctx context.Context, runID int, state string) error {
	switch state {
	case RunProcessed, RunFailed, RunQueued:
	default:
		return fmt.Errorf("dograh: run %d: unknown state %q", runID, state)
	}

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("dograh: run %d: %w", runID, err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	var campaignID int

	err = tx.QueryRow(ctx, `
		UPDATE queued_runs SET state = $2::queued_run_state,
			processed_at = CASE WHEN $2 = 'queued' THEN NULL ELSE now() END
		WHERE id = $1 RETURNING campaign_id`, runID, state).Scan(&campaignID)
	if err != nil {
		return fmt.Errorf("dograh: run %d to %s: %w", runID, state, err)
	}

	column := map[string]string{RunProcessed: "processed_rows", RunFailed: "failed_rows"}[state]
	if column != "" {
		if _, err := tx.Exec(ctx, "UPDATE campaigns SET "+column+" = "+column+" + 1 WHERE id = $1", campaignID); err != nil {
			return fmt.Errorf("dograh: campaign %d counter: %w", campaignID, err)
		}
	}

	return tx.Commit(ctx)
}

// ScheduleRetry adds a new run for run's contact, due after delay, as Dograh's
// orchestrator does: the same variables plus is_retry / retry_attempt /
// retry_reason, source_uuid "<original>_retry_<n>", the attempt counted in
// retry_count. A retry that already exists (same campaign, source and
// attempt) is left alone, so a repeated call can't queue two.
func (s *Store) ScheduleRetry(ctx context.Context, run QueuedRun, reason string, delay time.Duration, now time.Time) error {
	vars := map[string]any{}
	for k, v := range run.Variables {
		vars[k] = v
	}

	vars["is_retry"] = true
	vars["retry_attempt"] = run.RetryCount + 1
	vars["retry_reason"] = reason

	raw, err := json.Marshal(vars)
	if err != nil {
		return fmt.Errorf("dograh: retry variables: %w", err)
	}

	_, err = s.pool.Exec(ctx, `
		INSERT INTO queued_runs (campaign_id, source_uuid, context_variables, state, retry_count,
			parent_queued_run_id, scheduled_for, retry_reason, created_at)
		VALUES ($1, $2, $3::text::json, 'queued', $4, $5, $6, $7, now())
		ON CONFLICT (campaign_id, source_uuid, retry_count) DO NOTHING`,
		run.CampaignID, fmt.Sprintf("%s_retry_%d", run.SourceUUID, run.RetryCount+1), string(raw),
		run.RetryCount+1, run.ID, now.Add(delay), reason)
	if err != nil {
		return fmt.Errorf("dograh: schedule retry of run %d: %w", run.ID, err)
	}

	return nil
}
