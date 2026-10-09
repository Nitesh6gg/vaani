package dograh

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"math/rand/v2"
	"slices"
	"sort"
	"time"

	"github.com/nitesh/vaani/internal/ai/agent"
	"github.com/nitesh/vaani/internal/ai/llm"
)

// QA Analysis (api/services/workflow/node_specs/qa.py,
// api/services/workflow/qa/*.py, api/tasks/run_integrations.py): after a
// call, an LLM reviews its transcript (split by node) and tags problems --
// ASSISTANT_IN_LOOP, HEARING_ISSUES, USER_FRUSTRATED and the like -- plus a
// quality score and sentiment, saved to the run's annotations for Dograh's
// UI. Dograh runs this from a background job its own calls enqueue on
// completion; Vaani's calls, written straight to Dograh's database, never
// trigger that job, so this is a from-scratch Go port -- RunQA below,
// called right after FinishRun (internal/callagent) -- read against
// Dograh's Python source rather than guessed at.

// qaCallTimeout bounds one LLM call (node summary, conversation summary, or
// a node's review); qaOverallTimeout the whole RunQA pass, a backstop so a
// stuck provider can't hold a goroutine (and shutdown's drain, since RunQA
// is tracked by callagent.Builder's WaitGroup) forever.
const (
	qaCallTimeout    = 30 * time.Second
	qaOverallTimeout = 5 * time.Minute
)

// QAConfig is a workflow's QA Analysis setup, built once per call alongside
// its agent graph (store.go's Workflow): its QA nodes, the node summaries
// it already has cached, the script info needed to generate any that are
// missing, and the workflow owner's own (unmerged) LLM -- see
// resolveOwnerLLM. Zero value (no QA nodes) is a no-op throughout.
type QAConfig struct {
	Nodes         []QANode
	NodeSummaries map[string]string       // node_id -> cached summary text (key present = cached, even if "")
	NodeScripts   map[string]qaNodeScript // node_id -> info to summarize (agentNode/startCall only)
	OwnerLLM      LLMConfig
	ownerLLMOK    bool
}

// QANode is one QA Analysis node's configuration (node_specs/qa.py's
// QANodeData), with Dograh's own defaults filled in for anything the
// editor's form never saved.
type QANode struct {
	ID              string
	Name            string
	Enabled         bool
	SystemPrompt    string
	MinCallDuration time.Duration
	VoicemailCalls  bool
	SampleRate      int // percent, 1-100
	UseWorkflowLLM  bool
	Provider        string // qa_use_workflow_llm=false only
	Model           string
	APIKey          string
	Endpoint        string // azure only; not supported yet (qaProviderBaseURL)
}

// qaNodeScript is what a node-summary LLM call is told about one
// agentNode/startCall node (node_summary.py's node_info text).
type qaNodeScript struct {
	Name string
	Info string
}

// parseQANodes extracts a workflow's QA Analysis nodes ("qa" type).
func parseQANodes(wf workflowJSON) []QANode {
	var out []QANode

	for _, n := range wf.Nodes {
		if n.Type != "qa" {
			continue
		}

		d := n.Data

		minDur := 15.0
		if d.QAMinCallDuration != nil {
			minDur = *d.QAMinCallDuration
		}

		sampleRate := 100.0
		if d.QASampleRate != nil {
			sampleRate = *d.QASampleRate
		}

		name := d.Name
		if name == "" {
			name = "QA Analysis"
		}

		out = append(out, QANode{
			ID:              n.ID,
			Name:            name,
			Enabled:         d.QAEnabled == nil || *d.QAEnabled,
			SystemPrompt:    d.QASystemPrompt,
			MinCallDuration: time.Duration(minDur * float64(time.Second)),
			VoicemailCalls:  d.QAVoicemailCalls,
			SampleRate:      int(sampleRate),
			UseWorkflowLLM:  d.QAUseWorkflowLLM == nil || *d.QAUseWorkflowLLM,
			Provider:        d.QAProvider,
			Model:           d.QAModel,
			APIKey:          d.QAAPIKey,
			Endpoint:        d.QAEndpoint,
		})
	}

	return out
}

// existingNodeSummaries reads workflow_json's cached node_summaries. The map
// has an entry for every cached node id, even one cached as "" (a prior
// failed generation Dograh never retries -- see ensureNodeSummaries), so
// "cached" must be checked with the comma-ok form, not a non-empty value.
func existingNodeSummaries(wf workflowJSON) map[string]string {
	out := make(map[string]string, len(wf.NodeSummaries))
	for id, e := range wf.NodeSummaries {
		out[id] = e.Summary
	}

	return out
}

// buildNodeScripts is node_summary.py's per-node node_info text for every
// agentNode/startCall node: its name, its prompt, and its available tools
// (custom tools + outgoing edges, both by description) -- what the
// node-summary LLM call is told to summarize.
func buildNodeScripts(wf workflowJSON, tools []ToolRow) map[string]qaNodeScript {
	byUUID := make(map[string]ToolRow, len(tools))
	for _, t := range tools {
		byUUID[t.UUID] = t
	}

	type edgeDesc struct{ Label, Condition string }

	outgoing := map[string][]edgeDesc{}

	for _, e := range wf.Edges {
		outgoing[e.Source] = append(outgoing[e.Source], edgeDesc{e.Data.Label, e.Data.Condition})
	}

	out := map[string]qaNodeScript{}

	for _, n := range wf.Nodes {
		if n.Type != "agentNode" && n.Type != "startCall" {
			continue
		}

		d := n.Data

		name := d.Name
		if name == "" {
			name = "Unnamed"
		}

		info := "Node name: " + name
		if d.Prompt != "" {
			info += "\nAgent prompt:\n" + d.Prompt
		}

		var avail []string

		for _, uuid := range d.ToolUUIDs {
			if t, ok := byUUID[uuid]; ok {
				desc := "- " + t.Name
				if t.Description != "" {
					desc += ": " + t.Description
				}

				avail = append(avail, desc)
			}
		}

		for _, e := range outgoing[n.ID] {
			desc := "- " + e.Label
			if e.Condition != "" {
				desc += ": " + e.Condition
			}

			avail = append(avail, desc)
		}

		if len(avail) > 0 {
			info += "\nAvailable tools:\n"
			for i, a := range avail {
				if i > 0 {
					info += "\n"
				}

				info += a
			}
		}

		out[n.ID] = qaNodeScript{Name: name, Info: info}
	}

	return out
}

// buildQAConfig assembles a workflow's QA setup. Returns the zero QAConfig
// (no further work anywhere) when the workflow has no QA nodes at all --
// the overwhelmingly common case, so nothing else here runs per call.
func buildQAConfig(wf workflowJSON, tools []ToolRow, ownerLLM LLMConfig, ownerLLMOK bool) QAConfig {
	nodes := parseQANodes(wf)
	if len(nodes) == 0 {
		return QAConfig{}
	}

	return QAConfig{
		Nodes:         nodes,
		NodeSummaries: existingNodeSummaries(wf),
		NodeScripts:   buildNodeScripts(wf, tools),
		OwnerLLM:      ownerLLM,
		ownerLLMOK:    ownerLLMOK,
	}
}

// shouldSkipQA is Dograh's _should_skip_qa: the reason to skip this QA
// node's review for this call, or "" to proceed.
func shouldSkipQA(n QANode, duration time.Duration, disposition string) string {
	if duration < n.MinCallDuration {
		return fmt.Sprintf("call duration (%.1fs) below minimum (%.0fs)", duration.Seconds(), n.MinCallDuration.Seconds())
	}

	if !n.VoicemailCalls && disposition == "voicemail_detected" {
		return "voicemail call and QA voicemail calls is disabled"
	}

	if n.SampleRate < 100 {
		roll := 1 + rand.IntN(100) // Dograh: random.randint(1, 100)
		if roll > n.SampleRate {
			return fmt.Sprintf("excluded by sampling (%d%% sample rate, rolled %d)", n.SampleRate, roll)
		}
	}

	return ""
}

// qaProviderBaseURL is the fixed OpenAI-compatible endpoint for a QA node's
// own LLM provider (qa_use_workflow_llm=false; qa.py's qa_provider options:
// openai, azure, openrouter, anthropic). Azure needs its own endpoint/auth
// shape Vaani doesn't implement, so it's reported as unsupported rather
// than guessed at.
func qaProviderBaseURL(provider string) (string, bool) {
	switch provider {
	case "openai":
		return "https://api.openai.com/v1", true
	case "openrouter":
		return "https://openrouter.ai/api/v1", true
	case "anthropic":
		// Anthropic's OpenAI-compatible endpoint (platform.claude.com/docs/
		// en/api/openai-sdk, confirmed 2026-10-09): Bearer-token auth, same
		// as Vaani's llm.Client already sends -- no special handling needed.
		return "https://api.anthropic.com/v1", true
	default:
		return "", false
	}
}

// resolveQALLM is Dograh's resolve_llm_config: a QA node's own LLM when
// qa_use_workflow_llm is off, else the workflow owner's (ownerLLM, ok --
// resolveOwnerLLM; deliberately not the conversation's effective LLM, see
// its doc comment), with this node's own qa_model substituted in if it set
// one.
func resolveQALLM(n QANode, ownerLLM LLMConfig, ownerOK bool) (LLMConfig, bool) {
	if !n.UseWorkflowLLM {
		url, ok := qaProviderBaseURL(n.Provider)
		if !ok || n.APIKey == "" || n.Model == "" {
			return LLMConfig{}, false
		}

		return LLMConfig{Provider: n.Provider, BaseURL: url, APIKey: n.APIKey, Model: n.Model}, true
	}

	if !ownerOK {
		return LLMConfig{}, false
	}

	cfg := ownerLLM
	if n.Model != "" && n.Model != "default" {
		cfg.Model = n.Model
	}

	return cfg, true
}

// qaTag is one QA tag the reviewing LLM raised (qa.py's DEFAULT_QA_SYSTEM_
// PROMPT output format: {"tag": "...", "reason": "..."}).
type qaTag struct {
	Tag    string `json:"tag"`
	Reason string `json:"reason,omitempty"`
}

// qaNodeResult is one node segment's review, shaped exactly as Dograh's own
// QA job writes it (analysis.py's node_result).
type qaNodeResult struct {
	NodeName         string   `json:"node_name"`
	RawResponse      string   `json:"raw_response,omitempty"`
	Error            string   `json:"error,omitempty"`
	Tags             []qaTag  `json:"tags"`
	Summary          string   `json:"summary"`
	Score            *float64 `json:"score"`
	OverallSentiment string   `json:"overall_sentiment,omitempty"`
}

// qaRunResult is one QA node's overall output, as stored at
// annotations.qa_<node id> -- node_results/model on success, error on a
// run-level failure, skipped/reason when shouldSkipQA applied. Exactly one
// of these three shapes is ever populated.
type qaRunResult struct {
	NodeResults map[string]qaNodeResult `json:"node_results,omitempty"`
	Model       string                  `json:"model,omitempty"`
	Error       string                  `json:"error,omitempty"`
	Skipped     bool                    `json:"skipped,omitempty"`
	Reason      string                  `json:"reason,omitempty"`
}

// RunQA runs every enabled QA Analysis node of wf over call callID's
// finished conversation, as Dograh's run_integrations_post_workflow_run job
// does for its own calls -- which Vaani's calls never trigger (see this
// file's package doc). Each node's result (or why it was skipped/failed) is
// merged into run runID's annotations; nothing here can fail the call,
// already over by the time this runs. ctx is bounded internally
// (qaOverallTimeout); the caller's ctx only decides whether RunQA can start
// at all.
func (s *Store) RunQA(ctx context.Context, wf *Workflow, callID string, runID int64, sum agent.CallSummary, duration time.Duration) {
	if len(wf.QA.Nodes) == 0 {
		return
	}

	ctx, cancel := context.WithTimeout(ctx, qaOverallTimeout)
	defer cancel()

	disposition := callDisposition(sum)
	summaries := s.ensureNodeSummariesIfNeeded(ctx, wf, callID)

	results := map[string]json.RawMessage{}
	var allTags []string

	for _, n := range wf.QA.Nodes {
		if !n.Enabled {
			continue
		}

		if reason := shouldSkipQA(n, duration, disposition); reason != "" {
			slog.Info("qa node skipped", "call_id", callID, "component", "dograh", "event", "qa.skipped",
				"node", n.Name, "reason", reason)
			results["qa_"+n.ID] = mustMarshal(qaRunResult{Skipped: true, Reason: reason})

			continue
		}

		res, err := s.runQANode(ctx, n, wf, sum, duration, summaries)
		if err != nil {
			slog.Warn("qa node failed", "call_id", callID, "component", "dograh", "event", "qa.failed",
				"node", n.Name, "error", err)
			results["qa_"+n.ID] = mustMarshal(qaRunResult{Error: err.Error()})

			continue
		}

		slog.Info("qa node complete", "call_id", callID, "component", "dograh", "event", "qa.completed",
			"node", n.Name, "nodes_analyzed", len(res.NodeResults))
		results["qa_"+n.ID] = mustMarshal(res)

		for _, nr := range res.NodeResults {
			for _, t := range nr.Tags {
				if t.Tag != "" && !slices.Contains(allTags, t.Tag) {
					allTags = append(allTags, t.Tag)
				}
			}
		}
	}

	if len(results) == 0 {
		return
	}

	if len(allTags) > 0 {
		sort.Strings(allTags)
		results["tags"] = mustMarshal(allTags)
	}

	if err := s.updateAnnotations(ctx, runID, results); err != nil {
		slog.Warn("qa: saving results failed", "call_id", callID, "component", "dograh", "event", "qa.save_failed", "error", err)
	}
}

// ensureNodeSummariesIfNeeded generates and persists any node summaries
// missing for this workflow (see ensureNodeSummaries), using whichever QA
// node's LLM resolves first. Dograh instead regenerates per QA node
// (ensure_node_summaries is scoped to one QA node's own LLM each time
// run_per_node_qa_analysis runs, so two QA nodes sharing a missing summary
// each call an LLM for it); doing this once per call, before the per-node
// loop, produces the same cached text with fewer calls -- a deliberate,
// behavior-preserving simplification.
func (s *Store) ensureNodeSummariesIfNeeded(ctx context.Context, wf *Workflow, callID string) map[string]string {
	needed := false

	for id := range wf.QA.NodeScripts {
		if _, cached := wf.QA.NodeSummaries[id]; !cached {
			needed = true
			break
		}
	}

	if !needed {
		return wf.QA.NodeSummaries
	}

	for _, n := range wf.QA.Nodes {
		cfg, ok := resolveQALLM(n, wf.QA.OwnerLLM, wf.QA.ownerLLMOK)
		if !ok {
			continue
		}

		return s.ensureNodeSummaries(ctx, wf, callID, llm.NewClient(cfg.BaseURL, cfg.APIKey, cfg.Model))
	}

	return wf.QA.NodeSummaries // no QA node has a usable LLM: summaries stay empty for this call
}

// ensureNodeSummaries is node_summary.py's ensure_node_summaries: every
// agentNode/startCall node not already cached gets one generated and the
// full map (existing + new) is persisted to the definition. A generation
// failure caches "" for that node (logged, not retried on a later call,
// same as Dograh) rather than leaving it permanently un-cached.
func (s *Store) ensureNodeSummaries(ctx context.Context, wf *Workflow, callID string, client *llm.Client) map[string]string {
	out := maps.Clone(wf.QA.NodeSummaries)
	if out == nil {
		out = map[string]string{}
	}

	changed := false

	for id, script := range wf.QA.NodeScripts {
		if _, cached := out[id]; cached {
			continue
		}

		text, err := runOneShot(ctx, client, nodeSummarySystemPrompt, script.Info)
		if err != nil {
			slog.Warn("qa: node summary generation failed", "call_id", callID, "component", "dograh",
				"event", "qa.node_summary_failed", "node", script.Name, "error", err)
		}

		out[id] = text
		changed = true
	}

	if changed {
		if err := s.updateNodeSummaries(ctx, wf.DefinitionID, out); err != nil {
			slog.Warn("qa: saving node summaries failed", "call_id", callID, "component", "dograh",
				"event", "qa.node_summary_save_failed", "error", err)
		}
	}

	return out
}

// runQANode runs one QA node's review: Dograh's run_per_node_qa_analysis,
// falling back to runWholeCallQA if the call's events carry no node_id
// (shouldn't happen for Vaani calls -- every event is tagged from the
// first, see agent.CallLog.NodeEntered).
func (s *Store) runQANode(ctx context.Context, n QANode, wf *Workflow, sum agent.CallSummary, duration time.Duration, summaries map[string]string) (qaRunResult, error) {
	if n.SystemPrompt == "" {
		return qaRunResult{}, errors.New("no system prompt defined for QA node")
	}

	cfg, ok := resolveQALLM(n, wf.QA.OwnerLLM, wf.QA.ownerLLMOK)
	if !ok {
		if !n.UseWorkflowLLM && n.Provider == "azure" {
			return qaRunResult{}, errors.New("azure QA LLM provider isn't supported yet")
		}

		return qaRunResult{}, errors.New("no LLM API key configured for QA analysis")
	}

	client := llm.NewClient(cfg.BaseURL, cfg.APIKey, cfg.Model)

	splits, ok := splitEventsByNode(sum.Events)
	if !ok {
		return s.runWholeCallQA(ctx, client, cfg.Model, n, sum, duration)
	}

	nodeResults := map[string]qaNodeResult{}

	var prior []transcriptEntry

	for i, sp := range splits {
		convo := buildConversation(sp.Events)

		transcript := formatTranscript(convo)
		if transcript == "" {
			continue
		}

		nodeSummary := summaries[sp.NodeID]

		var prevSummary string

		if i > 0 && len(prior) > 0 {
			prevSummary = s.summarizeConversation(ctx, client, sp.NodeName, formatTranscript(prior))
		}

		system := renderTemplate(n.SystemPrompt, map[string]any{
			"node_summary":                  nodeSummary,
			"previous_conversation_summary": prevSummary,
			"transcript":                    transcript,
			"metrics":                       mustJSON(computeMetrics(sp.Events, nil)),
		}, time.Now())

		reply, err := runOneShot(ctx, client, system, "## Transcript\n"+transcript)
		if err != nil {
			nodeResults[sp.NodeID] = qaNodeResult{NodeName: sp.NodeName, Error: err.Error(), Tags: []qaTag{}}
		} else {
			nodeResults[sp.NodeID] = parseQAReply(sp.NodeName, reply)
		}

		prior = append(prior, convo...)
	}

	return qaRunResult{NodeResults: nodeResults, Model: cfg.Model}, nil
}

// runWholeCallQA is Dograh's _run_whole_call_qa_analysis fallback: one
// review over the entire call, when events carry no node_id to split by.
func (s *Store) runWholeCallQA(ctx context.Context, client *llm.Client, model string, n QANode, sum agent.CallSummary, duration time.Duration) (qaRunResult, error) {
	convo := buildConversation(sum.Events)

	transcript := formatTranscript(convo)
	if transcript == "" {
		return qaRunResult{}, errors.New("empty transcript")
	}

	d := duration.Seconds()
	system := renderTemplate(n.SystemPrompt, map[string]any{
		"node_summary": "", "previous_conversation_summary": "",
		"transcript": transcript, "metrics": mustJSON(computeMetrics(sum.Events, &d)),
	}, time.Now())

	reply, err := runOneShot(ctx, client, system, "## Transcript\n"+transcript)
	if err != nil {
		return qaRunResult{}, err
	}

	return qaRunResult{NodeResults: map[string]qaNodeResult{"whole_call": parseQAReply("whole_call", reply)}, Model: model}, nil
}

// summarizeConversation is Dograh's _generate_conversation_summary: never
// fails the caller -- a summary is a nicety, not required for the review.
func (s *Store) summarizeConversation(ctx context.Context, client *llm.Client, nodeName, transcript string) string {
	text, err := runOneShot(ctx, client, conversationSummarySystemPrompt, "## Conversation\n"+transcript)
	if err != nil {
		slog.Warn("qa: conversation summary failed", "component", "dograh", "event", "qa.conversation_summary_failed",
			"node", nodeName, "error", err)

		return ""
	}

	return text
}

// updateAnnotations merges results into run runID's annotations, as
// Dograh's own completion job does (run.annotations = {**run.annotations,
// **annotations} -- a shallow top-level merge, same pattern as FinishRun's
// cost_info).
func (s *Store) updateAnnotations(ctx context.Context, runID int64, results map[string]json.RawMessage) error {
	payload, err := json.Marshal(results)
	if err != nil {
		return fmt.Errorf("dograh: marshal qa results: %w", err)
	}

	dctx, cancel := context.WithTimeout(ctx, runDBTimeout)
	defer cancel()

	if _, err := s.pool.Exec(dctx, `
		UPDATE workflow_runs SET annotations = (annotations::jsonb || $2::text::jsonb)::json
		WHERE id = $1`, runID, string(payload)); err != nil {
		return fmt.Errorf("dograh: save qa results for run %d: %w", runID, err)
	}

	return nil
}

// updateNodeSummaries persists definitionID's full node_summaries map
// (Dograh's update_definition_node_summaries): the whole cache, since this
// is reused by every future call's QA pass, not an incremental write.
func (s *Store) updateNodeSummaries(ctx context.Context, definitionID int64, summaries map[string]string) error {
	entries := make(map[string]map[string]string, len(summaries))
	for id, text := range summaries {
		entries[id] = map[string]string{"summary": text}
	}

	payload, err := json.Marshal(entries)
	if err != nil {
		return fmt.Errorf("dograh: marshal node summaries: %w", err)
	}

	dctx, cancel := context.WithTimeout(ctx, runDBTimeout)
	defer cancel()

	if _, err := s.pool.Exec(dctx, `
		UPDATE workflow_definitions SET workflow_json = jsonb_set(workflow_json::jsonb, '{node_summaries}', $2::jsonb)::json
		WHERE id = $1`, definitionID, string(payload)); err != nil {
		return fmt.Errorf("dograh: save node summaries for definition %d: %w", definitionID, err)
	}

	return nil
}

func mustMarshal(v any) json.RawMessage {
	b, err := json.Marshal(v)
	if err != nil {
		return json.RawMessage(`{"error":"internal: result not serializable"}`)
	}

	return b
}
