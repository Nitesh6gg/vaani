package dograh

import (
	"encoding/json"
	"fmt"
	"regexp"
	"strings"
	"time"

	"github.com/nitesh/vaani/internal/ai/agent"
	"github.com/nitesh/vaani/internal/ai/llm"
)

// workflowJSON is the part of Dograh's workflow_json Vaani uses (its
// ReactFlow graph: api/services/workflow/dto.py).
type workflowJSON struct {
	Nodes []struct {
		ID   string   `json:"id"`
		Type string   `json:"type"`
		Data nodeData `json:"data"`
	} `json:"nodes"`
	Edges []struct {
		Source string `json:"source"`
		Target string `json:"target"`
		Data   struct {
			Label            string `json:"label"`
			Condition        string `json:"condition"`
			TransitionSpeech string `json:"transition_speech"`
			// "audio": transition_speech_recording_id is played instead.
			TransitionSpeechType        string       `json:"transition_speech_type"`
			TransitionSpeechRecordingID recordingRef `json:"transition_speech_recording_id"`
		} `json:"data"`
	} `json:"edges"`
	// NodeSummaries caches each agentNode/startCall node's QA script summary
	// (qa.go), keyed by node id -- a sibling of "nodes"/"edges" in
	// workflow_json, not a node itself (node_summary.py).
	NodeSummaries map[string]nodeSummaryEntry `json:"node_summaries"`
}

// nodeSummaryEntry is one cached node summary: node_summary.py's
// get_node_summary_text handles both the current shape
// ({"summary": "...", "trace_url": "..."}) and the legacy plain-string one.
type nodeSummaryEntry struct {
	Summary string
}

func (e *nodeSummaryEntry) UnmarshalJSON(b []byte) error {
	var s string
	if json.Unmarshal(b, &s) == nil {
		e.Summary = s
		return nil
	}

	var obj struct {
		Summary string `json:"summary"`
	}

	if err := json.Unmarshal(b, &obj); err != nil {
		return err
	}

	e.Summary = obj.Summary

	return nil
}

type nodeData struct {
	Name            string   `json:"name"`
	Prompt          string   `json:"prompt"`
	IsStart         bool     `json:"is_start"`
	IsEnd           bool     `json:"is_end"`
	AllowInterrupt  bool     `json:"allow_interrupt"`   // absent = false, Dograh's default (dto.py)
	AddGlobalPrompt *bool    `json:"add_global_prompt"` // Dograh's default: true
	Greeting        string   `json:"greeting"`
	GreetingType    string   `json:"greeting_type"`
	ToolUUIDs       []string `json:"tool_uuids"`
	// greeting_type "audio": this recording is played instead of Greeting.
	GreetingRecordingID recordingRef `json:"greeting_recording_id"`

	// Variable extraction (dto.py: extraction_enabled defaults to false).
	ExtractionEnabled   bool   `json:"extraction_enabled"`
	ExtractionPrompt    string `json:"extraction_prompt"`
	ExtractionVariables []struct {
		Name   string  `json:"name"`
		Type   string  `json:"type"`
		Prompt *string `json:"prompt"`
	} `json:"extraction_variables"`

	// QA Analysis node settings (type "qa"; node_specs/qa.py). Harmless on
	// any other node type: the JSON field names don't collide with theirs.
	QAEnabled         *bool    `json:"qa_enabled"` // default true
	QASystemPrompt    string   `json:"qa_system_prompt"`
	QAMinCallDuration *float64 `json:"qa_min_call_duration"` // seconds, default 15
	QAVoicemailCalls  bool     `json:"qa_voicemail_calls"`   // default false
	QASampleRate      *float64 `json:"qa_sample_rate"`       // percent, default 100
	QAUseWorkflowLLM  *bool    `json:"qa_use_workflow_llm"`  // default true
	QAProvider        string   `json:"qa_provider"`
	QAModel           string   `json:"qa_model"`
	QAAPIKey          string   `json:"qa_api_key"`
	QAEndpoint        string   `json:"qa_endpoint"`
}

const globalNodeType = "globalNode"

func parseWorkflow(b []byte) (workflowJSON, error) {
	var wf workflowJSON
	if err := json.Unmarshal(b, &wf); err != nil {
		return wf, fmt.Errorf("parse workflow_json: %w", err)
	}

	return wf, nil
}

// toolUUIDs lists the distinct tools attached to any node.
func (wf workflowJSON) toolUUIDs() []string {
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

	return out
}

// recordingNeeds is which recordings a call of wf may play: the primary keys
// its greeting, transitions and tools reference, and whether any node
// prompt names recordings by recording_id (then all of them are needed).
func (wf workflowJSON) recordingNeeds(tools []ToolRow) (pks map[int64]bool, byID bool) {
	pks = map[int64]bool{}

	for _, n := range wf.Nodes {
		if n.Data.IsStart && n.Data.GreetingType == "audio" && n.Data.GreetingRecordingID > 0 {
			pks[int64(n.Data.GreetingRecordingID)] = true
		}

		if strings.Contains(n.Data.Prompt, promptRecordingRef) {
			byID = true
		}
	}

	for _, e := range wf.Edges {
		if e.Data.TransitionSpeechType == "audio" && e.Data.TransitionSpeechRecordingID > 0 {
			pks[int64(e.Data.TransitionSpeechRecordingID)] = true
		}
	}

	for _, r := range tools {
		if pk := toolClipRef(r); pk > 0 {
			pks[pk] = true
		}
	}

	return pks, byID
}

// buildWorkflow turns a parsed workflow into the agent's node graph and
// returns its start node, composing each node the way Dograh's engine does
// (pipecat_engine_context_composer.py): system prompt = global node prompt
// (unless the node opts out) + "\n\n" + node prompt, both with variables
// filled in; functions = the node's tools + one per outgoing edge.
//
// Variables are filled in once, when the call starts (Dograh renders them
// on entering each node -- for {{current_time}} the difference is the
// length of the call).
//
// Problems that shouldn't stop a call (a tool that's missing, archived, of
// an unsupported category, or misconfigured) come back as warnings.
func buildWorkflow(wf workflowJSON, vars map[string]any, tools []ToolRow, clips clipSet, now time.Time) (start *agent.Node, warnings []string, err error) {
	render := func(s string) string { return renderTemplate(s, vars, now) }

	byUUID := make(map[string]agent.Tool, len(tools))

	for _, r := range tools {
		t, warn, err := buildTool(r, clips)
		if err != nil {
			warnings = append(warnings, err.Error())
			continue
		}

		if warn != "" {
			warnings = append(warnings, warn)
		}

		byUUID[r.UUID] = t
	}

	globalPrompt := ""

	for _, n := range wf.Nodes {
		if n.Type == globalNodeType {
			globalPrompt = render(n.Data.Prompt)
		}
	}

	nodes := make(map[string]*agent.Node, len(wf.Nodes))

	for _, n := range wf.Nodes {
		if n.Type == globalNodeType {
			continue
		}

		d := n.Data
		node := &agent.Node{ID: n.ID, Name: d.Name, End: d.IsEnd, AllowInterrupt: d.AllowInterrupt}

		var parts []string
		if globalPrompt != "" && (d.AddGlobalPrompt == nil || *d.AddGlobalPrompt) {
			parts = append(parts, globalPrompt)
		}

		p := render(d.Prompt)
		if p != "" {
			parts = append(parts, p)
		}

		// Dograh's recording response mode: only for a node whose own prompt
		// names recordings, and only if the organization has any.
		if clips.HasAny && strings.Contains(p, promptRecordingRef) {
			parts = append(parts, recordingInstructions)
			node.Clips = clips.byID
		}

		node.Prompt = strings.Join(parts, "\n\n")

		// Dograh extracts only when enabled and there's something to extract;
		// prompts get the call's {{variables}} (a missing one stays missing).
		if d.ExtractionEnabled && len(d.ExtractionVariables) > 0 {
			x := &agent.Extraction{Prompt: render(d.ExtractionPrompt)}

			for _, v := range d.ExtractionVariables {
				ev := agent.ExtractionVar{Name: v.Name, Type: v.Type}
				if v.Prompt != nil {
					p := *v.Prompt
					if p != "" {
						p = render(p)
					}

					ev.Prompt = &p
				}

				x.Vars = append(x.Vars, ev)
			}

			node.Extraction = x
		}

		for _, u := range d.ToolUUIDs {
			if t, ok := byUUID[u]; ok {
				node.Tools = append(node.Tools, t)
			} else if !hasRow(tools, u) {
				warnings = append(warnings, fmt.Sprintf("node %q: tool %s not found or not active", d.Name, u))
			}
		}

		if d.IsStart {
			if start != nil {
				return nil, nil, fmt.Errorf("workflow has more than one start node")
			}

			start = node

			switch {
			case d.GreetingType == "audio":
				// Without its recording, Dograh falls back to an LLM opening.
				if clip := clips.byPK[int64(d.GreetingRecordingID)]; clip != nil {
					node.GreetingClip = clip
				} else {
					warnings = append(warnings, "audio greeting's recording unavailable; the LLM opens the call instead")
				}
			case d.Greeting != "":
				node.Greeting = render(d.Greeting)
			}
		}

		nodes[n.ID] = node
	}

	if start == nil {
		return nil, nil, fmt.Errorf("workflow has no start node")
	}

	for _, e := range wf.Edges {
		from, to := nodes[e.Source], nodes[e.Target]
		if from == nil || to == nil {
			return nil, nil, fmt.Errorf("edge %q connects unknown nodes %s -> %s", e.Data.Label, e.Source, e.Target)
		}

		name := EdgeFunctionName(e.Data.Label)

		for _, t := range from.Tools {
			if t.Def.Name == name {
				warnings = append(warnings, fmt.Sprintf("node %q: edge %q and a tool share the function name %q; the edge wins", from.Name, e.Data.Label, name))
			}
		}

		edge := agent.Edge{
			Def: llm.FunctionDef{Name: name, Description: e.Data.Condition},
			To:  to,
		}

		// "audio" plays its recording, or nothing if it's unavailable (Dograh
		// doesn't fall back to the text either).
		if e.Data.TransitionSpeechType == "audio" {
			if edge.SpeechClip = clips.byPK[int64(e.Data.TransitionSpeechRecordingID)]; edge.SpeechClip == nil {
				warnings = append(warnings, fmt.Sprintf("edge %q: transition recording unavailable; nothing is said", e.Data.Label))
			}
		} else {
			edge.Speech = render(e.Data.TransitionSpeech)
		}

		from.Edges = append(from.Edges, edge)
	}

	return start, warnings, nil
}

func hasRow(rows []ToolRow, uuid string) bool {
	for _, r := range rows {
		if r.UUID == uuid {
			return true
		}
	}

	return false
}

var nonEdgeChar = regexp.MustCompile(`[^a-z0-9]`)

// EdgeFunctionName is the LLM function name for a workflow edge: Dograh's
// Edge.get_function_name -- lowercase, anything but [a-z0-9] becomes "_"
// (no collapsing, unlike tool names). "Move to Main Agenda" ->
// "move_to_main_agenda".
func EdgeFunctionName(label string) string {
	return nonEdgeChar.ReplaceAllString(strings.ToLower(label), "_")
}
