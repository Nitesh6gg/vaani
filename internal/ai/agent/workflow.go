package agent

import "github.com/nitesh/vaani/internal/ai/llm"

// Node is one step of the call's workflow (a Dograh workflow node). The agent
// starts at Config.Start and moves along Edges when the LLM calls an edge's
// function, exactly like Dograh's engine: each node brings its own system
// prompt and tools, and history carries over.
type Node struct {
	ID   string // Dograh's node id, for the call log
	Name string
	// Prompt is the complete system prompt while at this node (Dograh: the
	// global prompt + the node's own, variables already filled in).
	Prompt string
	// Greeting, start node only: spoken as-is before the caller says
	// anything. Empty means the LLM generates the opening from Prompt.
	Greeting string
	// End marks an end node: reaching it, the LLM says its closing line and
	// the call hangs up once that has played.
	End bool
	// AllowInterrupt is Dograh's allow_interrupt: whether the caller may cut
	// in while the agent speaks at this node. When false their speech is
	// ignored until the agent finishes (Dograh mutes the user). Only takes
	// effect with barge-in enabled (Config.BargeInObserveOnly false).
	AllowInterrupt bool

	Tools []Tool
	Edges []Edge
}

// Edge is a transition out of a node, offered to the LLM as a function with
// no parameters (Dograh: name from the edge label, description from its
// condition).
type Edge struct {
	Def llm.FunctionDef
	// Speech, if set, is spoken before moving to To (Dograh's
	// transition_speech).
	Speech string
	To     *Node
}

// InterruptMode is "on" when the caller can cut in while the agent speaks at
// n: barge-in enabled (BARGE_IN_ENABLED, the master switch), the node's
// allow_interrupt on, and not an end node -- once the call is ending Dograh
// mutes the caller whatever the node says (its End Call node has no
// interruption setting; a stored allow_interrupt there is ignored).
func InterruptMode(bargeInEnabled bool, n *Node) string {
	if bargeInEnabled && n.AllowInterrupt && !n.End {
		return "on"
	}

	return "off"
}

// toolDefs is what the LLM is offered while at n: its tools, then its edges.
func (n *Node) toolDefs() []llm.Tool {
	defs := make([]llm.Tool, 0, len(n.Tools)+len(n.Edges))

	for _, t := range n.Tools {
		defs = append(defs, llm.Tool{Type: "function", Function: t.Def})
	}

	for _, e := range n.Edges {
		defs = append(defs, llm.Tool{Type: "function", Function: e.Def})
	}

	return defs
}

func (n *Node) edge(name string) *Edge {
	for i := range n.Edges {
		if n.Edges[i].Def.Name == name {
			return &n.Edges[i]
		}
	}

	return nil
}

func (n *Node) tool(name string) (Tool, bool) {
	for _, t := range n.Tools {
		if t.Def.Name == name {
			return t, true
		}
	}

	return Tool{}, false
}
