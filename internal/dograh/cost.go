package dograh

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"math"
	"time"

	"github.com/nitesh/vaani/internal/ai/agent"
	"github.com/nitesh/vaani/internal/ai/llm"
)

// buildUsageInfo is the call's usage_info in Dograh's shape: LLM tokens by
// key (the call's own plus extra, e.g. QA's), TTS characters, and STT billed
// as the whole call, as Dograh bills streaming STT (pipeline_metrics_
// aggregator.py's set_stt_service) -- unless only the speech was sent to the
// STT (STT_SPEECH_ONLY): then the seconds actually sent, rounded up as Sarvam
// bills them, so the cost shown is the cost incurred.
func buildUsageInfo(wf *Workflow, sum agent.CallSummary, extra map[string]llm.Usage, duration time.Duration) usageInfo {
	seconds := int(math.Round(duration.Seconds()))

	sttSeconds := seconds
	if sum.STTGated {
		sttSeconds = int(math.Ceil(sum.STTAudioSeconds))
	}

	u := usageInfo{
		LLM:                 map[string]tokenUsage{},
		TTS:                 map[string]int{},
		STT:                 map[string]float64{usageKeySTT + wf.Services.STT.Model: float64(sttSeconds)},
		CallDurationSeconds: seconds,
	}

	add := func(usage map[string]llm.Usage) {
		for k, v := range usage {
			t := u.LLM[k]
			t.PromptTokens += v.PromptTokens
			t.CompletionTokens += v.CompletionTokens
			t.TotalTokens += v.TotalTokens
			t.CacheReadInputTokens += v.CachedTokens
			u.LLM[k] = t
		}
	}

	add(sum.LLMUsage)
	add(extra)

	if sum.TTSChars > 0 {
		u.TTS[usageKeyTTS+wf.Services.TTS.Model] = sum.TTSChars
	}

	return u
}

// RecordCost is Dograh's calculate_workflow_run_cost job (pricing/
// workflow_run_cost.py) for a Vaani call, which never triggers that job: it
// rewrites run runID's usage_info with QA's tokens (qaUsage) added, prices
// it into cost_info -- the cost_breakdown Dograh's live monitor and run
// pages show -- and adds the call to its organization's current usage cycle.
// Run after RunQA, as Dograh prices after QA. Failures are logged only.
func (s *Store) RecordCost(ctx context.Context, wf *Workflow, callID string, runID int64, sum agent.CallSummary, qaUsage map[string]llm.Usage, duration time.Duration) {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), runDBTimeout)
	defer cancel()

	u := buildUsageInfo(wf, sum, qaUsage, duration)
	c := calculateCost(u)
	tokens := math.Round(c.Total*100*100) / 100 // 1 cent = 1 Dograh token

	if err := s.recordCost(ctx, runID, u, c, tokens); err != nil {
		slog.Warn("dograh: recording the call's cost failed", "call_id", callID, "component", "dograh",
			"event", "run.cost_failed", "error", err)

		return
	}

	slog.Info("dograh run cost", "call_id", callID, "component", "dograh", "event", "run.cost",
		"total_usd", c.Total, "llm_usd", c.LLM, "tts_usd", c.TTS, "stt_usd", c.STT, "dograh_tokens", tokens)
}

func (s *Store) recordCost(ctx context.Context, runID int64, u usageInfo, c costBreakdown, tokens float64) error {
	// Dograh charges the workflow owner's selected organization.
	var (
		createdAt   time.Time
		orgID       *int64
		pricePerSec *float64
	)

	err := s.pool.QueryRow(ctx, `
		SELECT r.created_at, o.id, o.price_per_second_usd
		FROM workflow_runs r
		JOIN workflows w ON w.id = r.workflow_id
		LEFT JOIN users usr ON usr.id = w.user_id
		LEFT JOIN organizations o ON o.id = usr.selected_organization_id
		WHERE r.id = $1`, runID).Scan(&createdAt, &orgID, &pricePerSec)
	if err != nil {
		return fmt.Errorf("read run %d: %w", runID, err)
	}

	costInfo := map[string]any{
		"cost_breakdown":        c,
		"total_cost_usd":        c.Total,
		"dograh_token_usage":    tokens,
		"calculated_at":         createdAt.UTC().Format("2006-01-02T15:04:05.000000-07:00"), // Python isoformat()
		"call_duration_seconds": u.CallDurationSeconds,
	}

	var charge *float64

	if pricePerSec != nil && *pricePerSec != 0 {
		v := float64(u.CallDurationSeconds) * *pricePerSec
		charge = &v
		costInfo["charge_usd"] = v
		costInfo["price_per_second_usd"] = *pricePerSec
	}

	usage, _ := json.Marshal(u)
	cost, _ := json.Marshal(costInfo)

	if _, err := s.pool.Exec(ctx, `
		UPDATE workflow_runs SET
			usage_info = $2::text::json,
			cost_info = (cost_info::jsonb || $3::text::jsonb)::json
		WHERE id = $1`, runID, string(usage), string(cost)); err != nil {
		return fmt.Errorf("update run %d: %w", runID, err)
	}

	if orgID == nil {
		return nil
	}

	// Dograh's update_usage_after_run, on the cycle covering now. Creating a
	// missing cycle is left to Dograh, which owns the quota/period rules.
	tag, err := s.pool.Exec(ctx, `
		UPDATE organization_usage_cycles SET
			used_dograh_tokens = used_dograh_tokens + $2,
			total_duration_seconds = total_duration_seconds + $3,
			used_amount_usd = CASE WHEN $4::float8 IS NULL THEN used_amount_usd
				ELSE COALESCE(used_amount_usd, 0) + $4::float8 END
		WHERE organization_id = $1 AND period_start <= now() AND period_end >= now()`,
		*orgID, tokens, u.CallDurationSeconds, charge)
	if err != nil {
		return fmt.Errorf("update org %d usage: %w", *orgID, err)
	}

	if tag.RowsAffected() == 0 {
		slog.Info("dograh: no current usage cycle for the organization; its usage isn't updated",
			"component", "dograh", "event", "run.usage_cycle_missing", "org_id", *orgID, "run_id", runID)
	}

	return nil
}
