package dograh

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/jackc/pgx/v5"
)

// An outbound call is dialed as Dograh's ARI provider does
// (api/services/telephony/providers/ari_provider.py), from the organization's
// TELEPHONY_CONFIGURATION. Only the dial-string fields are read here: Vaani
// has its own ARI connection, so the config's ARI password is never touched.

// DialSettings are the organization's ARI dial-string settings.
type DialSettings struct {
	Provider    string // must be "ari" for Vaani to dial
	Technology  string // "PJSIP" or "SIP"
	Trunk       string
	StripPrefix string
	FromNumbers []string
	// WorkflowCLI is the caller ID mapped to a workflow (number_workflow_map is
	// stored {phone: workflow_id}).
	WorkflowCLI map[int]string
}

// DialSettings reads organization orgID's telephony configuration.
func (s *Store) DialSettings(ctx context.Context, orgID int) (DialSettings, error) {
	var (
		d                 DialSettings
		fromJSON, mapJSON string
	)

	err := s.pool.QueryRow(ctx, `
		SELECT COALESCE(v->>'provider', 'twilio'), COALESCE(v->>'sip_technology', 'PJSIP'),
			COALESCE(v->>'outbound_trunk', ''), COALESCE(v->>'strip_number_prefix', ''),
			COALESCE((v->'from_numbers')::text, '[]'), COALESCE((v->'number_workflow_map')::text, '{}')
		FROM (SELECT value::text::jsonb AS v FROM organization_configurations
			WHERE organization_id = $1 AND key = 'TELEPHONY_CONFIGURATION') t`, orgID).
		Scan(&d.Provider, &d.Technology, &d.Trunk, &d.StripPrefix, &fromJSON, &mapJSON)
	if errors.Is(err, pgx.ErrNoRows) {
		return d, fmt.Errorf("dograh: organization %d has no telephony configuration", orgID)
	}

	if err != nil {
		return d, fmt.Errorf("dograh: telephony configuration of organization %d: %w", orgID, err)
	}

	d.Technology = strings.ToUpper(strings.TrimSpace(d.Technology))
	if d.Technology == "" {
		d.Technology = "PJSIP"
	}

	d.Trunk = strings.TrimSpace(d.Trunk)
	d.StripPrefix = strings.TrimSpace(d.StripPrefix)

	// from_numbers is a list, or one string (Dograh accepts both).
	if json.Unmarshal([]byte(fromJSON), &d.FromNumbers) != nil {
		var one string
		if json.Unmarshal([]byte(fromJSON), &one) == nil && one != "" {
			d.FromNumbers = []string{one}
		}
	}

	d.WorkflowCLI = map[int]string{}

	var byPhone map[string]any
	_ = json.Unmarshal([]byte(mapJSON), &byPhone)

	for phone, w := range byPhone {
		switch id := w.(type) {
		case float64:
			d.WorkflowCLI[int(id)] = phone
		case string:
			if n, err := strconv.Atoi(id); err == nil {
				d.WorkflowCLI[n] = phone
			}
		}
	}

	return d, nil
}

// Endpoint is the Asterisk dial string for number: used as given when it
// already names a technology (SIP/ or PJSIP/), else
// "<technology>/<number minus the strip prefix>[@<trunk>]".
func (d DialSettings) Endpoint(number string) string {
	if strings.HasPrefix(number, "SIP/") || strings.HasPrefix(number, "PJSIP/") {
		return number
	}

	if d.StripPrefix != "" {
		number = strings.TrimPrefix(number, d.StripPrefix)
	}

	tech := d.Technology
	if tech == "" {
		tech = "PJSIP"
	}

	if d.Trunk != "" {
		return tech + "/" + number + "@" + d.Trunk
	}

	return tech + "/" + number
}

// CallerID is the number shown to the callee: the one mapped to the workflow,
// else the first configured number, else none.
func (d DialSettings) CallerID(workflowID int) string {
	if cli := d.WorkflowCLI[workflowID]; cli != "" {
		return cli
	}

	if len(d.FromNumbers) > 0 {
		return d.FromNumbers[0]
	}

	return ""
}

// CreateOutboundRun records a campaign call before it is dialed, as Dograh's
// dispatcher does, so even a call nobody answers leaves a run in Dograh. The
// run starts 'initialized'; MarkRunRunning moves it on when answered.
// initial_context is the contact's variables plus the campaign's, as Dograh
// writes it. A Campaign with ID 0 is a call outside any campaign: no
// campaign or queued-run link.
func (s *Store) CreateOutboundRun(ctx context.Context, c Campaign, q QueuedRun, called, caller string) (int64, error) {
	initial := map[string]any{}
	for k, v := range q.Variables {
		initial[k] = v
	}

	name := "ARI Outbound " + called // a call placed outside a campaign
	if c.ID > 0 {
		initial["campaign_id"] = c.ID
		name = fmt.Sprintf("WR-CAMPAIGN-%d-%d", c.ID, q.ID)
	}

	initial["provider"] = "ari"
	initial["source_uuid"] = q.SourceUUID
	initial["caller_number"] = caller
	initial["called_number"] = called

	raw, err := json.Marshal(initial)
	if err != nil {
		return 0, fmt.Errorf("dograh: outbound run context: %w", err)
	}

	ctx, cancel := context.WithTimeout(ctx, runDBTimeout)
	defer cancel()

	var id int64

	err = s.pool.QueryRow(ctx, `
		INSERT INTO workflow_runs (name, workflow_id, definition_id, mode, call_type, state,
			is_completed, usage_info, cost_info, initial_context, gathered_context, logs,
			annotations, created_at, campaign_id, queued_run_id)
		SELECT $1, w.id, COALESCE(w.released_definition_id,
				(SELECT id FROM workflow_definitions WHERE workflow_id = w.id AND is_current LIMIT 1)),
			'ari', 'outbound', 'initialized', false, '{}', '{}', $3::text::json, '{}', '{}', '{}',
			now(), NULLIF($4::int, 0), NULLIF($5::int, 0)
		FROM workflows w WHERE w.id = $2
		RETURNING id`,
		name, c.WorkflowID, string(raw), c.ID, q.ID).Scan(&id)
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, fmt.Errorf("dograh: workflow %d not found", c.WorkflowID)
	}

	if err != nil {
		return 0, fmt.Errorf("dograh: create outbound run: %w", err)
	}

	return id, nil
}

// MarkRunRunning notes that the callee answered and the agent is on the call.
func (s *Store) MarkRunRunning(ctx context.Context, runID int64) error {
	ctx, cancel := context.WithTimeout(ctx, runDBTimeout)
	defer cancel()

	if _, err := s.pool.Exec(ctx, `UPDATE workflow_runs SET state = 'running' WHERE id = $1`, runID); err != nil {
		return fmt.Errorf("dograh: run %d running: %w", runID, err)
	}

	return nil
}

// Statuses a call that never connected can end with (Dograh's names).
const (
	StatusBusy     = "busy"
	StatusNoAnswer = "no-answer"
	StatusRejected = "rejected"
	StatusFailed   = "failed"
)

// StatusForCause maps an Asterisk hangup cause of an unanswered call to its
// status, as Dograh does: 17 busy, 19 no answer, 21 rejected, anything else
// failed.
func StatusForCause(cause int) string {
	switch cause {
	case 17:
		return StatusBusy
	case 19:
		return StatusNoAnswer
	case 21:
		return StatusRejected
	}

	return StatusFailed
}

// notConnectedDisposition is Dograh's mark_workflow_run_not_connected mapping.
func notConnectedDisposition(status string) string {
	switch status {
	case "no-answer":
		return "NO_ANSWER"
	case "busy":
		return "BUSY"
	case "failed", "error":
		return "FAILED"
	case "canceled":
		return "CANCELED"
	case "rejected":
		return "REJECTED"
	}

	return strings.ToUpper(strings.ReplaceAll(status, "-", "_"))
}

// MarkNotConnected completes the run of a call that never connected, with the
// tags and disposition Dograh gives it, so it doesn't stay 'initialized' and
// shows in the call list: tags "not_connected" and "telephony_<status>" added
// to any it has, call_disposition BUSY / NO_ANSWER / ..., and the
// organization's mapping of it.
func (s *Store) MarkNotConnected(ctx context.Context, runID int64, orgID int, status string) error {
	return s.completeRun(ctx, runID, orgID,
		[]string{"not_connected", "telephony_" + strings.ToLower(status)}, notConnectedDisposition(status))
}

// MarkInterrupted completes the run of a call Vaani lost (it was restarted, or
// the agent never started), so it doesn't stay 'running' for ever.
func (s *Store) MarkInterrupted(ctx context.Context, runID int64, orgID int) error {
	return s.completeRun(ctx, runID, orgID, []string{"interrupted"}, "INTERRUPTED")
}

func (s *Store) completeRun(ctx context.Context, runID int64, orgID int, tagList []string, disposition string) error {
	ctx = context.WithoutCancel(ctx)

	mctx, cancel := context.WithTimeout(ctx, runDBTimeout)
	oid := int64(orgID)
	mapped := s.mapDisposition(mctx, &oid, disposition)

	cancel()

	tags, _ := json.Marshal(tagList)

	ctx, cancel = context.WithTimeout(ctx, runDBTimeout)
	defer cancel()

	_, err := s.pool.Exec(ctx, `
		UPDATE workflow_runs SET is_completed = true, state = 'completed',
			gathered_context = (COALESCE(gathered_context::jsonb, '{}'::jsonb) || jsonb_build_object(
				'call_tags', COALESCE(gathered_context::jsonb->'call_tags', '[]'::jsonb) || $2::text::jsonb,
				'call_disposition', $3::text, 'mapped_call_disposition', $4::text))::json
		WHERE id = $1`, runID, string(tags), disposition, mapped)
	if err != nil {
		return fmt.Errorf("dograh: complete run %d: %w", runID, err)
	}

	return nil
}
