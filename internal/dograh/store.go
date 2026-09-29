// Package dograh reads agent configuration straight from a self-hosted
// Dograh deployment's Postgres database, so the Dograh visual editor stays
// the single place where agents are configured. Read-only: Vaani never writes
// to Dograh's tables.
package dograh

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/nitesh/vaani/internal/ai/agent"
)

// Store is a connection pool to Dograh's database, shared by every call --
// one pool per process, like the shared LLM http.Client.
type Store struct {
	pool *pgxpool.Pool
}

// Open connects to Dograh's Postgres at url and verifies the connection.
func Open(ctx context.Context, url string) (*Store, error) {
	pool, err := pgxpool.New(ctx, url)
	if err != nil {
		return nil, fmt.Errorf("dograh: connect: %w", err)
	}

	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("dograh: ping: %w", err)
	}

	return &Store{pool: pool}, nil
}

// Close releases the pool.
func (s *Store) Close() { s.pool.Close() }

// ToolRow is one row of Dograh's tools table.
type ToolRow struct {
	UUID        string
	Name        string
	Description string
	Category    string
	Definition  json.RawMessage
}

// Workflow loads Dograh workflow workflowID for one call and returns its
// start node (see buildWorkflow). callVars are the call's own template
// variables (caller_number, called_number), layered over the workflow's
// template_context_variables. Read fresh per call, so a publish in Dograh's
// editor applies to the next call.
//
// The definition is resolved like Dograh does for a real call: the
// published one (released_definition_id), else the legacy is_current row;
// its template variables, else the workflow's. Tools are scoped to the
// workflow's organization and must be active, as in Dograh's lookup.
func (s *Store) Workflow(ctx context.Context, workflowID int, callVars map[string]any) (*Workflow, []string, error) {
	var (
		defJSON, defVars, wfVars, defConfig, userConfig string
		orgID                                           *int64
	)

	// The models come from the workflow owner's user_configurations, as
	// Dograh does for telephony calls (ari_manager.py: workflow.user_id).
	err := s.pool.QueryRow(ctx, `
		SELECT d.workflow_json::text, COALESCE(d.template_context_variables::text, ''),
			COALESCE(w.template_context_variables::text, ''),
			COALESCE(d.workflow_configurations::text, ''), w.organization_id,
			COALESCE((SELECT configuration::text FROM user_configurations
				WHERE user_id = w.user_id ORDER BY id LIMIT 1), '')
		FROM workflows w
		JOIN workflow_definitions d ON d.id = COALESCE(w.released_definition_id,
			(SELECT id FROM workflow_definitions WHERE workflow_id = w.id AND is_current LIMIT 1))
		WHERE w.id = $1`, workflowID).Scan(&defJSON, &defVars, &wfVars, &defConfig, &orgID, &userConfig)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil, fmt.Errorf("dograh: workflow %d not found or has no published definition", workflowID)
	}

	if err != nil {
		return nil, nil, fmt.Errorf("dograh: query workflow %d: %w", workflowID, err)
	}

	wf, err := parseWorkflow([]byte(defJSON))
	if err != nil {
		return nil, nil, fmt.Errorf("dograh: workflow %d: %w", workflowID, err)
	}

	vars := templateVars(defVars)
	if len(vars) == 0 {
		vars = templateVars(wfVars)
	}

	for k, v := range callVars {
		vars[k] = v
	}

	tools, err := s.tools(ctx, wf.toolUUIDs(), orgID)
	if err != nil {
		return nil, nil, err
	}

	start, warnings, err := buildWorkflow(wf, vars, tools, time.Now())
	if err != nil {
		return nil, nil, fmt.Errorf("dograh: workflow %d: %w", workflowID, err)
	}

	services, serviceWarnings, err := resolveServices(userConfig, defConfig)
	if err != nil {
		return nil, nil, fmt.Errorf("dograh: workflow %d: models: %w", workflowID, err)
	}

	warnings = append(warnings, serviceWarnings...)

	idle, maxDuration, warn := callLimits(defConfig)
	if warn != "" {
		warnings = append(warnings, warn)
	}

	return &Workflow{Start: start, Services: services, IdleTimeout: idle, MaxDuration: maxDuration}, warnings, nil
}

// Workflow is what one call runs.
type Workflow struct {
	Start    *agent.Node
	Services Services
	// From the workflow's settings (see callLimits); 0 disables.
	IdleTimeout time.Duration
	MaxDuration time.Duration
}

// Dograh's backend defaults (api/services/pipecat/run_pipeline.py), used
// when the workflow's settings were never saved.
const (
	defaultIdleTimeout = 10 * time.Second
	defaultMaxDuration = 300 * time.Second
)

// callLimits reads max_user_idle_timeout and max_call_duration (seconds)
// from a definition's workflow_configurations, as Dograh does. Idle 0
// disables silence handling (pipecat's rule); a max duration <= 0 is taken
// as "no limit". Unreadable settings fall back to the defaults, with a
// warning.
func callLimits(configJSON string) (idle, maxDuration time.Duration, warning string) {
	idle, maxDuration = defaultIdleTimeout, defaultMaxDuration

	var c struct {
		Idle *float64 `json:"max_user_idle_timeout"`
		Max  *float64 `json:"max_call_duration"`
	}

	if configJSON != "" {
		if err := json.Unmarshal([]byte(configJSON), &c); err != nil {
			return idle, maxDuration, "unreadable workflow_configurations, using default call limits: " + err.Error()
		}
	}

	seconds := func(v float64) time.Duration {
		if v <= 0 {
			return 0
		}

		return time.Duration(v * float64(time.Second))
	}

	if c.Idle != nil {
		idle = seconds(*c.Idle)
	}

	if c.Max != nil {
		maxDuration = seconds(*c.Max)
	}

	return idle, maxDuration, ""
}

// templateVars decodes a template_context_variables JSON object; anything
// else (empty, null, malformed) is no variables.
func templateVars(s string) map[string]any {
	vars := map[string]any{}
	_ = json.Unmarshal([]byte(s), &vars)

	if vars == nil { // JSON null
		vars = map[string]any{}
	}

	return vars
}

// tools returns the active tools among uuids that belong to orgID.
func (s *Store) tools(ctx context.Context, uuids []string, orgID *int64) ([]ToolRow, error) {
	if len(uuids) == 0 {
		return nil, nil
	}

	rows, err := s.pool.Query(ctx, `
		SELECT tool_uuid, name, COALESCE(description, ''), category::text, definition::text
		FROM tools
		WHERE tool_uuid = ANY($1) AND organization_id IS NOT DISTINCT FROM $2
			AND status::text = 'active'`, uuids, orgID)
	if err != nil {
		return nil, fmt.Errorf("dograh: query tools: %w", err)
	}
	defer rows.Close()

	var out []ToolRow

	for rows.Next() {
		var (
			r   ToolRow
			def string
		)

		if err := rows.Scan(&r.UUID, &r.Name, &r.Description, &r.Category, &def); err != nil {
			return nil, fmt.Errorf("dograh: scan tool: %w", err)
		}

		r.Definition = json.RawMessage(def)
		out = append(out, r)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("dograh: read tools: %w", err)
	}

	return out, nil
}
