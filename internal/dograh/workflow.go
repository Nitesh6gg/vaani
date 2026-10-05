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
		} `json:"data"`
	} `json:"edges"`
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
func buildWorkflow(wf workflowJSON, vars map[string]any, tools []ToolRow, now time.Time) (start *agent.Node, warnings []string, err error) {
	render := func(s string) string { return renderTemplate(s, vars, now) }

	byUUID := make(map[string]agent.Tool, len(tools))

	for _, r := range tools {
		t, warn, err := buildTool(r)
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
		node := &agent.Node{Name: d.Name, End: d.IsEnd, AllowInterrupt: d.AllowInterrupt}

		var parts []string
		if globalPrompt != "" && (d.AddGlobalPrompt == nil || *d.AddGlobalPrompt) {
			parts = append(parts, globalPrompt)
		}

		if p := render(d.Prompt); p != "" {
			parts = append(parts, p)
		}

		node.Prompt = strings.Join(parts, "\n\n")

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
				// Recorded greetings aren't supported yet; Dograh itself falls
				// back to an LLM opening when it can't fetch the recording.
				warnings = append(warnings, "audio greeting not supported yet; the LLM opens the call instead")
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

		from.Edges = append(from.Edges, agent.Edge{
			Def:    llm.FunctionDef{Name: name, Description: e.Data.Condition},
			Speech: render(e.Data.TransitionSpeech),
			To:     to,
		})
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
