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

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
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

// WorkflowTools returns the active tools attached to any node of workflow
// workflowID's live definition, plus how many tool UUIDs the nodes listed
// (so the caller can tell when some are missing or archived).
//
// The live definition is resolved like Dograh does for a real call: the
// published one (released_definition_id), else the legacy is_current row.
// Tools are scoped to the workflow's organization, as in Dograh's lookup.
func (s *Store) WorkflowTools(ctx context.Context, workflowID int) ([]ToolRow, int, error) {
	var (
		workflowJSON string
		orgID        *int64
	)

	err := s.pool.QueryRow(ctx, `
		SELECT d.workflow_json::text, w.organization_id
		FROM workflows w
		JOIN workflow_definitions d ON d.id = COALESCE(w.released_definition_id,
			(SELECT id FROM workflow_definitions WHERE workflow_id = w.id AND is_current LIMIT 1))
		WHERE w.id = $1`, workflowID).Scan(&workflowJSON, &orgID)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, 0, fmt.Errorf("dograh: workflow %d not found or has no published definition", workflowID)
	}

	if err != nil {
		return nil, 0, fmt.Errorf("dograh: query workflow %d: %w", workflowID, err)
	}

	uuids, err := nodeToolUUIDs([]byte(workflowJSON))
	if err != nil {
		return nil, 0, fmt.Errorf("dograh: workflow %d: %w", workflowID, err)
	}

	if len(uuids) == 0 {
		return nil, 0, nil
	}

	rows, err := s.pool.Query(ctx, `
		SELECT tool_uuid, name, COALESCE(description, ''), category::text, definition::text
		FROM tools
		WHERE tool_uuid = ANY($1) AND organization_id IS NOT DISTINCT FROM $2
			AND status::text = 'active'`, uuids, orgID)
	if err != nil {
		return nil, 0, fmt.Errorf("dograh: query tools: %w", err)
	}
	defer rows.Close()

	var out []ToolRow

	for rows.Next() {
		var (
			r   ToolRow
			def string
		)

		if err := rows.Scan(&r.UUID, &r.Name, &r.Description, &r.Category, &def); err != nil {
			return nil, 0, fmt.Errorf("dograh: scan tool: %w", err)
		}

		r.Definition = json.RawMessage(def)
		out = append(out, r)
	}

	if err := rows.Err(); err != nil {
		return nil, 0, fmt.Errorf("dograh: read tools: %w", err)
	}

	return out, len(uuids), nil
}

// nodeToolUUIDs collects the distinct tool UUIDs of every node in a Dograh
// workflow_json ({"nodes":[{"data":{"tool_uuids":[...]}}]}), in node order.
// ponytail: every node's tools at once; per-node tools come with the
// workflow engine (Step 3).
func nodeToolUUIDs(workflowJSON []byte) ([]string, error) {
	var wf struct {
		Nodes []struct {
			Data struct {
				ToolUUIDs []string `json:"tool_uuids"`
			} `json:"data"`
		} `json:"nodes"`
	}

	if err := json.Unmarshal(workflowJSON, &wf); err != nil {
		return nil, fmt.Errorf("parse workflow_json: %w", err)
	}

	seen := map[string]bool{}

	var out []string

	for _, n := range wf.Nodes {
		for _, u := range n.Data.ToolUUIDs {
			if !seen[u] {
				seen[u] = true
				out = append(out, u)
			}
		}
	}

	return out, nil
}
