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

// StartRun creates the call's run and returns its id, trying a second time
// after a failure (each attempt bounded by runDBTimeout): one dropped
// connection must not cost the call its place in Dograh's history.
func (s *Store) StartRun(ctx context.Context, wf *Workflow, c RunCall) (int64, error) {
	id, err := s.startRun(ctx, wf, c)
	if err == nil || ctx.Err() != nil {
		return id, err
	}

	select {
	case <-time.After(500 * time.Millisecond):
	case <-ctx.Done():
		return 0, err
	}

	return s.startRun(ctx, wf, c)
}

func (s *Store) startRun(ctx context.Context, wf *Workflow, c RunCall) (int64, error) {
	ctx, cancel := context.WithTimeout(ctx, runDBTimeout)
	defer cancel()

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

// Each FinishRun step gets its own time budget, so a slow one can't use up
// the next one's (a slow MinIO used to leave runs stuck at 'running').
const (
	runDBTimeout     = 10 * time.Second
	runUploadTimeout = 20 * time.Second
)

// FinishRun completes run runID: what the call gathered, its length, its
// conversation (the run page's events), and -- with storage -- its recording
// and transcript, uploaded where Dograh keeps them.
//
// The run is marked completed first and the files attached after, as
// Dograh's own completion job does: a slow or failing upload, or the process
// being stopped meanwhile, then costs only the files, never leaves the run at
// 'running'. Each step runs on its own budget, detached from ctx's
// cancellation (ctx only carries values); every failure is in the returned
// error.
func (s *Store) FinishRun(ctx context.Context, wf *Workflow, runID int64, end RunEnd, storage *Storage) error {
	ctx = context.WithoutCancel(ctx)
	sum := end.Summary

	step := func(d time.Duration) (context.Context, context.CancelFunc) { return context.WithTimeout(ctx, d) }

	mctx, cancel := step(runDBTimeout)
	g, mapped := gatheredContext(sum, func(v string) string { return s.mapDisposition(mctx, wf.OrgID, v) })
	cancel()

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

	var errs []error

	dctx, cancel := step(runDBTimeout)
	_, err := s.pool.Exec(dctx, `
		UPDATE workflow_runs SET
			usage_info = $2::text::json,
			cost_info = (cost_info::jsonb || $3::text::jsonb)::json,
			gathered_context = (gathered_context::jsonb || $4::text::jsonb)::json,
			logs = $5::text::json,
			is_completed = true,
			state = 'completed'
		WHERE id = $1`,
		runID, string(usage), string(cost), string(gathered), string(logs))
	cancel()

	if err != nil {
		errs = append(errs, fmt.Errorf("dograh: complete run %d: %w", runID, err))
	}

	// Lets Dograh's call list filter by this disposition code.
	if mapped != "" {
		dctx, cancel := step(runDBTimeout)
		if _, err := s.pool.Exec(dctx, `
			UPDATE workflows SET call_disposition_codes = jsonb_build_object('disposition_codes',
				COALESCE(call_disposition_codes::jsonb->'disposition_codes', '[]'::jsonb) || to_jsonb($2::text))::json
			WHERE id = $1
				AND NOT COALESCE(call_disposition_codes::jsonb->'disposition_codes', '[]'::jsonb) ? $2::text`,
			wf.ID, mapped); err != nil {
			errs = append(errs, fmt.Errorf("dograh: add disposition code: %w", err))
		}
		cancel()
	}

	if storage == nil {
		return errors.Join(errs...)
	}

	var recordingURL, transcriptURL *string

	put := func(key, contentType string, body []byte) *string {
		uctx, cancel := step(runUploadTimeout)
		defer cancel()

		if err := storage.Put(uctx, key, contentType, body); err != nil {
			errs = append(errs, err)
			return nil
		}

		return &key
	}

	id := strconv.FormatInt(runID, 10)

	if len(end.Recording) > 0 {
		recordingURL = put("recordings/"+id+".wav", "audio/wav", end.Recording)
	}

	if text := transcriptText(sum.Events); text != "" {
		transcriptURL = put("transcripts/"+id+".txt", "text/plain; charset=utf-8", []byte(text))
	}

	if recordingURL != nil || transcriptURL != nil {
		dctx, cancel := step(runDBTimeout)
		_, err := s.pool.Exec(dctx, `
			UPDATE workflow_runs SET
				recording_url = COALESCE($2::text, recording_url),
				transcript_url = COALESCE($3::text, transcript_url),
				storage_backend = 'minio'
			WHERE id = $1`,
			runID, recordingURL, transcriptURL)
		cancel()

		if err != nil {
			errs = append(errs, fmt.Errorf("dograh: attach files to run %d: %w", runID, err))
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
	if ed, extracted := sum.Extracted["call_disposition"].(string); extracted && ed != "" {
		g["extracted_call_disposition"] = ed
	}

	d := callDisposition(sum)
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

// callDisposition is the call's outcome as Dograh records and QA's
// voicemail skip rule reads it (gatheredContext's call_disposition; QA's
// _should_skip_qa compares it to EndTaskReason.VOICEMAIL_DETECTED): an
// extracted call_disposition variable if there is one and it's non-empty,
// else why the call ended, else "user_hangup".
func callDisposition(sum agent.CallSummary) string {
	if d, extracted := sum.Extracted["call_disposition"].(string); extracted && d != "" {
		return d
	}

	if sum.EndReason != "" {
		return sum.EndReason
	}

	return agent.EndReasonUserHangup
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
