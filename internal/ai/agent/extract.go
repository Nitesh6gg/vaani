package agent

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"regexp"
	"strings"
	"time"

	"github.com/nitesh/vaani/internal/ai/llm"
)

// Variable extraction, ported from Dograh's
// api/services/workflow/pipecat_engine_variable_extractor.py and the engine's
// _perform_variable_extraction_if_needed: when the conversation leaves a node
// with extraction on, the call's own LLM reads the conversation so far and
// returns the node's variables as JSON (in the background); when the call
// ends, after those finish, the same is done for the node it ended at. The
// results go to the call's record (CallLog.Extracted -> gathered_context).

// extractionTimeout bounds one extraction, and how long the end of the call
// waits for those still running -- Dograh waits 30s for them.
const extractionTimeout = 30 * time.Second

// Dograh's extraction system prompt, word for word.
const extractionSystemPrompt = "You are an assistant tasked with extracting structured data from the conversation. " +
	"Return ONLY a valid JSON object with the requested variables as top-level keys. Do not wrap the JSON in markdown."

// extractionMessages builds Dograh's two extraction messages for x over
// history.
func extractionMessages(x *Extraction, history []llm.Message) []llm.Message {
	system := extractionSystemPrompt
	if x.Prompt != "" {
		system += "\n\n" + x.Prompt
	}

	vars := make([]string, len(x.Vars))
	for i, v := range x.Vars {
		// As Dograh's Python 3.12 renders f"- {v.name} ({v.type}): {v.prompt}":
		// the type is an Enum member, printed "VariableType.string", and a
		// missing prompt prints "None".
		prompt := "None"
		if v.Prompt != nil {
			prompt = *v.Prompt
		}

		vars[i] = "- " + v.Name + " (VariableType." + v.Type + "): " + prompt
	}

	user := "\n\nVariables to extract:\n" + strings.Join(vars, "\n") +
		"\n\nConversation history:\n" + conversationText(history)

	return []llm.Message{{Role: "system", Content: system}, {Role: "user", Content: user}}
}

// toolResponseMaxChars is Dograh's cap on one tool response in the history.
const toolResponseMaxChars = 2000

// conversationText is Dograh's _build_conversation_history: "user: ..." and
// "assistant: ..." lines, and each tool result as "[Tool Response: name]"
// plus its data -- except edge transitions' {"status":"done"}.
func conversationText(history []llm.Message) string {
	names := map[string]string{}

	for _, m := range history {
		for _, c := range m.ToolCalls {
			names[c.ID] = c.Function.Name
		}
	}

	var lines []string

	for _, m := range history {
		switch {
		case (m.Role == "user" || m.Role == "assistant") && m.Content != "":
			lines = append(lines, m.Role+": "+m.Content)
		case m.Role == "tool":
			name, ok := names[m.ToolCallID]
			if !ok {
				name = "unknown"
			}

			if text, keep := toolResponseText(m.Content); keep {
				lines = append(lines, "[Tool Response: "+name+"]\n"+text)
			}
		}
	}

	return strings.Join(lines, "\n")
}

// toolResponseText is Dograh's _format_tool_response: a JSON object's "data"
// if it has one (an http_api tool's reply), else the object without its
// status/status_code; anything else as-is; cut at 2000 characters. keep is
// false for an edge transition's result. (Re-encoded by Go: same content,
// though spacing and key order can differ from Python's json.dumps.)
func toolResponseText(raw string) (text string, keep bool) {
	var obj map[string]json.RawMessage

	text = raw

	if err := json.Unmarshal([]byte(raw), &obj); err == nil && obj != nil {
		if len(obj) == 1 && string(obj["status"]) == `"done"` {
			return "", false
		}

		var v any = obj
		if data, ok := obj["data"]; ok {
			v = data
		} else {
			delete(obj, "status")
			delete(obj, "status_code")
		}

		var b bytes.Buffer

		enc := json.NewEncoder(&b)
		enc.SetEscapeHTML(false) // ensure_ascii=False: Hindi stays readable

		if enc.Encode(v) == nil {
			text = strings.TrimSuffix(b.String(), "\n")
		}
	}

	if r := []rune(text); len(r) > toolResponseMaxChars {
		text = string(r[:toolResponseMaxChars]) + "...(truncated)"
	}

	return text, true
}

var codeBlock = regexp.MustCompile("```(?:json)?\\s*([\\s\\S]*?)\\s*```")

// parseLLMJSON is Dograh's parse_llm_json: the reply as JSON, else the JSON
// inside a ``` block, else the first {...} or [...] in it; failing all that,
// {"raw": reply}. Returns the parsed value (a map or a slice) as raw JSON.
func parseLLMJSON(reply string) json.RawMessage {
	s := strings.TrimSpace(reply)
	if s == "" {
		return json.RawMessage(`{}`)
	}

	if v := jsonContainer(s); v != nil {
		return v
	}

	if m := codeBlock.FindStringSubmatch(s); m != nil {
		if v := jsonContainer(strings.TrimSpace(m[1])); v != nil {
			return v
		}
	}

	for _, open := range []string{"{", "["} {
		if i := strings.Index(s, open); i >= 0 {
			var v json.RawMessage
			if json.NewDecoder(strings.NewReader(s[i:])).Decode(&v) == nil {
				return v
			}
		}
	}

	b, _ := json.Marshal(map[string]string{"raw": reply})

	return b
}

// jsonContainer returns s if it is exactly one JSON object or array.
func jsonContainer(s string) json.RawMessage {
	if (strings.HasPrefix(s, "{") || strings.HasPrefix(s, "[")) && json.Valid([]byte(s)) {
		return json.RawMessage(s)
	}

	return nil
}

// extract runs node's extraction over history and hands the variables to
// the call log. Only a JSON object counts, as in Dograh.
func (h *Handler) extract(ctx context.Context, node *Node, history []llm.Message) {
	var reply strings.Builder

	start := time.Now()

	_, err := h.cfg.LLM.Stream(ctx, extractionMessages(node.Extraction, history), nil, func(tok string) {
		reply.WriteString(tok)
	})
	if err != nil {
		slog.Warn("variable extraction failed", "call_id", h.callID, "node", node.Name, "error", err)
		return
	}

	raw := parseLLMJSON(reply.String())

	vars, keys := objectInOrder(raw)
	if vars == nil {
		slog.Warn("variable extraction returned no JSON object; skipped", "call_id", h.callID,
			"node", node.Name, "reply", truncate(reply.String(), 200))

		return
	}

	slog.Info("variables extracted", "call_id", h.callID, "node", node.Name,
		"duration_ms", time.Since(start).Milliseconds(), "variables", truncate(string(raw), 500))
	h.cfg.Log.Extracted(vars, keys)
}

// objectInOrder decodes a JSON object, also returning its keys in the order
// they appear (Python dicts keep it; Dograh's call tags depend on it). Nil
// if raw isn't an object.
func objectInOrder(raw json.RawMessage) (map[string]any, []string) {
	var vars map[string]any
	if json.Unmarshal(raw, &vars) != nil || vars == nil {
		return nil, nil
	}

	dec := json.NewDecoder(bytes.NewReader(raw))
	_, _ = dec.Token() // {

	var keys []string

	for dec.More() {
		t, err := dec.Token()
		if err != nil {
			break
		}

		if k, ok := t.(string); ok {
			keys = append(keys, k)
		}

		var skip json.RawMessage
		if dec.Decode(&skip) != nil {
			break
		}
	}

	return vars, keys
}

// startExtraction runs node's extraction in the background as the
// conversation leaves it (Dograh: before the transition). history is the
// conversation at that moment; it must not be modified afterwards. A no-op
// once FinishExtraction has already taken its final snapshot -- run()'s
// goroutine can be mid-turn for a few statements past baseCtx's
// cancellation, and a background extraction that only started after that
// would never be waited for.
func (h *Handler) startExtraction(node *Node, history []llm.Message) {
	if node.Extraction == nil || h.cfg.Log == nil {
		return
	}

	done := make(chan struct{})

	h.extractMu.Lock()
	closed := h.extractDone
	if !closed {
		h.extracting = append(h.extracting, done)
	}
	h.extractMu.Unlock()

	if closed {
		slog.Warn("variable extraction skipped: the call already finished", "call_id", h.callID, "node", node.Name)
		return
	}

	go func() {
		defer close(done)

		// Not the turn's context: an interruption or the call ending must
		// not lose what the caller already answered.
		ctx, cancel := context.WithTimeout(context.Background(), extractionTimeout)
		defer cancel()

		h.extract(ctx, node, history)
	}()
}

// FinishExtraction is the end-of-call extraction (Dograh's
// end_call_with_reason): it waits for those still running, then runs the
// extraction of the node the call ended at over the whole conversation.
// Call it after Done, before reading the CallLog for the call's record.
func (h *Handler) FinishExtraction(ctx context.Context) {
	if h.cfg.Log == nil {
		return
	}

	h.extractMu.Lock()
	pending := h.extracting
	h.extractDone = true
	h.extractMu.Unlock()

	wait, cancel := context.WithTimeout(ctx, extractionTimeout)
	defer cancel()

	for _, done := range pending {
		select {
		case <-done:
		case <-wait.Done():
			slog.Warn("variable extraction still running at the end of the call; not waiting", "call_id", h.callID)
			return
		}
	}

	if node := h.node.Load(); node.Extraction != nil {
		ctx, cancel := context.WithTimeout(ctx, extractionTimeout)
		defer cancel()

		h.extract(ctx, node, h.history) // run() has returned: history is ours now
	}
}
