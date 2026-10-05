package dograh

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/nitesh/vaani/internal/ai/agent"
)

// Each call is recorded in Dograh's workflow_runs, so it shows in Dograh's
// call history like one of its own: created when the call starts
// (ari_manager.py, inbound ARI call), completed when the agent leaves it
// (event_handlers.py on_pipeline_finished, plus the recording and transcript
// uploads its process_workflow_completion job makes).

// RunCall is the call a run is created for.
type RunCall struct {
	CallID       string // the caller's channel id
	CallerNumber string
	CalledNumber string
}

// StartRun creates the call's run and returns its id.
func (s *Store) StartRun(ctx context.Context, wf *Workflow, c RunCall) (int64, error) {
	initial, _ := json.Marshal(map[string]any{
		"caller_number": c.CallerNumber, "called_number": c.CalledNumber,
		"direction": "inbound", "provider": "ari",
	})
	gathered, _ := json.Marshal(map[string]any{"call_id": c.CallID})

	var id int64

	err := s.pool.QueryRow(ctx, `
		INSERT INTO workflow_runs (name, workflow_id, definition_id, mode, call_type, state,
			is_completed, usage_info, cost_info, initial_context, gathered_context, logs,
			annotations, created_at)
		VALUES ($1, $2, $3, 'ari', 'inbound', 'running', false, '{}', '{}',
			$4::text::json, $5::text::json, '{}', '{}', now())
		RETURNING id`,
		"ARI Inbound "+c.CallerNumber, wf.ID, wf.DefinitionID, string(initial), string(gathered)).Scan(&id)
	if err != nil {
		return 0, fmt.Errorf("dograh: create run: %w", err)
	}

	return id, nil
}

// RunEnd is what a finished call leaves behind.
type RunEnd struct {
	Summary   agent.CallSummary
	Duration  time.Duration
	Recording []byte // WAV; nil when there's none
}

// FinishRun completes run runID: what the call gathered, its length, its
// conversation (the run page's events), and -- with storage -- its recording
// and transcript, uploaded where Dograh keeps them. A failed upload doesn't
// stop the rest; every failure is in the returned error.
func (s *Store) FinishRun(ctx context.Context, wf *Workflow, runID int64, end RunEnd, storage *Storage) error {
	sum := end.Summary

	g, mapped := gatheredContext(sum, func(v string) string { return s.mapDisposition(ctx, wf.OrgID, v) })
	gathered, _ := json.Marshal(g)

	// pipeline_metrics_aggregator: whole seconds; Dograh's cost job copies it
	// into cost_info, which is where its call list reads the duration.
	seconds := int(math.Round(end.Duration.Seconds()))
	usage, _ := json.Marshal(map[string]any{
		"llm": map[string]any{}, "tts": map[string]any{}, "stt": map[string]any{},
		"call_duration_seconds": seconds,
	})
	cost, _ := json.Marshal(map[string]any{"call_duration_seconds": seconds})

	logs := []byte("{}") // Dograh leaves it empty when nothing was logged
	if len(sum.Events) > 0 {
		logs, _ = json.Marshal(map[string]any{"realtime_feedback_events": sum.Events})
	}

	var (
		errs                        []error
		recordingURL, transcriptURL *string
	)

	if storage != nil {
		id := strconv.FormatInt(runID, 10)

		if len(end.Recording) > 0 {
			key := "recordings/" + id + ".wav"
			if err := storage.Put(ctx, key, "audio/wav", end.Recording); err != nil {
				errs = append(errs, err)
			} else {
				recordingURL = &key
			}
		}

		if text := transcriptText(sum.Events); text != "" {
			key := "transcripts/" + id + ".txt"
			if err := storage.Put(ctx, key, "text/plain; charset=utf-8", []byte(text)); err != nil {
				errs = append(errs, err)
			} else {
				transcriptURL = &key
			}
		}
	}

	_, err := s.pool.Exec(ctx, `
		UPDATE workflow_runs SET
			usage_info = $2::text::json,
			cost_info = (cost_info::jsonb || $3::text::jsonb)::json,
			gathered_context = (gathered_context::jsonb || $4::text::jsonb)::json,
			logs = $5::text::json,
			is_completed = true,
			state = 'completed',
			recording_url = COALESCE($6::text, recording_url),
			transcript_url = COALESCE($7::text, transcript_url),
			storage_backend = CASE WHEN $6::text IS NULL AND $7::text IS NULL
				THEN storage_backend ELSE 'minio' END
		WHERE id = $1`,
		runID, string(usage), string(cost), string(gathered), string(logs), recordingURL, transcriptURL)
	if err != nil {
		errs = append(errs, fmt.Errorf("dograh: complete run %d: %w", runID, err))
	}

	// Lets Dograh's call list filter by this disposition code.
	if mapped != "" {
		if _, err := s.pool.Exec(ctx, `
			UPDATE workflows SET call_disposition_codes = jsonb_build_object('disposition_codes',
				COALESCE(call_disposition_codes::jsonb->'disposition_codes', '[]'::jsonb) || to_jsonb($2::text))::json
			WHERE id = $1
				AND NOT COALESCE(call_disposition_codes::jsonb->'disposition_codes', '[]'::jsonb) ? $2::text`,
			wf.ID, mapped); err != nil {
			errs = append(errs, fmt.Errorf("dograh: add disposition code: %w", err))
		}
	}

	return errors.Join(errs...)
}

// gatheredContext is what the call adds to the run's gathered_context, as
// Dograh's engine builds it: nodes_visited; the extracted variables, at the
// top level and under extracted_variables; the disposition
// (end_call_with_reason) -- an extracted call_disposition if there is one,
// else why the call ended -- and mapDisposition's mapping of it; and the
// call tags: the disposition, user_speech if the caller said anything, then
// (on_pipeline_finished) the value of every tag_* variable.
func gatheredContext(sum agent.CallSummary, mapDisposition func(string) string) (g map[string]any, mapped string) {
	visited := sum.NodesVisited
	if visited == nil {
		visited = []string{}
	}

	g = map[string]any{"nodes_visited": visited}

	if sum.Extracted != nil {
		for k, v := range sum.Extracted {
			g[k] = v
		}

		g["extracted_variables"] = sum.Extracted
	}

	// An extracted one counts only as non-empty text (Dograh: any truthy value).
	d, extracted := sum.Extracted["call_disposition"].(string)
	if extracted && d != "" {
		g["extracted_call_disposition"] = d
	} else if d = sum.EndReason; d == "" {
		d = agent.EndReasonUserHangup
	}

	mapped = mapDisposition(d)
	g["call_disposition"] = d
	g["mapped_call_disposition"] = mapped

	tags := []any{d}
	if sum.UserSpoke {
		tags = append(tags, "user_speech")
	}

	// Dograh's own check compares the key, not the value, with the tags.
	for _, k := range sum.ExtractedKeys {
		if strings.HasPrefix(k, "tag_") && !containsTag(tags, k) {
			tags = append(tags, sum.Extracted[k])
		}
	}

	g["call_tags"] = tags

	return g, mapped
}

func containsTag(tags []any, s string) bool {
	for _, t := range tags {
		if t == s {
			return true
		}
	}

	return false
}

// mapDisposition is Dograh's apply_disposition_mapping: the organization's
// DISPOSITION_CODE_MAPPING ({"user_hangup": "HU", ...}) applied to value,
// which comes back unchanged when there's no mapping for it.
func (s *Store) mapDisposition(ctx context.Context, orgID *int64, value string) string {
	if orgID == nil || value == "" {
		return value
	}

	var raw string

	if err := s.pool.QueryRow(ctx, `
		SELECT value::text FROM organization_configurations
		WHERE organization_id = $1 AND key = 'DISPOSITION_CODE_MAPPING'
		ORDER BY id LIMIT 1`, *orgID).Scan(&raw); err != nil {
		return value
	}

	var mapping map[string]any
	_ = json.Unmarshal([]byte(raw), &mapping)

	if v, ok := mapping[value].(string); ok {
		return v
	}

	return value
}

// transcriptText is Dograh's generate_transcript_text: one line per caller
// turn and agent reply, "[timestamp] User: ..." / "[timestamp] Agent: ...".
func transcriptText(events []agent.LogEvent) string {
	var b strings.Builder

	for _, e := range events {
		var who string

		switch {
		case e.Type == "rtf-user-transcription" && e.Payload["final"] == true:
			who = "User"
		case e.Type == "rtf-bot-text":
			who = "Agent"
		default:
			continue
		}

		ts, _ := e.Payload["timestamp"].(string)
		if ts == "" {
			ts = e.Timestamp
		}

		if ts != "" {
			b.WriteString("[" + ts + "] ")
		}

		text, _ := e.Payload["text"].(string)
		b.WriteString(who + ": " + text + "\n")
	}

	return b.String()
}
