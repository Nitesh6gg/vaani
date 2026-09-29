// Package llm streams chat completions from an OpenAI-compatible endpoint.
package llm

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
)

// sharedHTTPClient is the one global client for every LLM request, per
// CLAUDE.md invariant #5: MaxIdleConnsPerHost=200, HTTP/2, never a per-request
// client.
var sharedHTTPClient = &http.Client{
	Transport: &http.Transport{
		MaxIdleConnsPerHost: 200,
		ForceAttemptHTTP2:   true,
	},
}

// Message is one OpenAI-format chat message. An assistant message that
// called tools carries ToolCalls (Content may be empty); each tool's result
// is a Role "tool" message answering one call by ToolCallID.
type Message struct {
	Role       string     `json:"role"`
	Content    string     `json:"content,omitempty"`
	ToolCalls  []ToolCall `json:"tool_calls,omitempty"`
	ToolCallID string     `json:"tool_call_id,omitempty"`
}

// ToolCall is one function call the model asked for.
type ToolCall struct {
	ID       string       `json:"id"`
	Type     string       `json:"type"`
	Function FunctionCall `json:"function"`
	// ExtraContent is kept verbatim and sent back unchanged. Gemini 3's
	// OpenAI-compatible API puts the call's "thought signature" here and
	// rejects the follow-up request (the one carrying the tool result) if
	// it's missing -- so this must never be dropped or rebuilt.
	ExtraContent json.RawMessage `json:"extra_content,omitempty"`
}

// FunctionCall is the called function's name and its JSON arguments, as the
// model wrote them.
type FunctionCall struct {
	Name      string `json:"name"`
	Arguments string `json:"arguments"`
}

// Tool is one function offered to the model.
type Tool struct {
	Type     string      `json:"type"`
	Function FunctionDef `json:"function"`
}

// FunctionDef describes a tool: Parameters is a JSON Schema object (nil
// means "takes no arguments").
type FunctionDef struct {
	Name        string          `json:"name"`
	Description string          `json:"description"`
	Parameters  json.RawMessage `json:"parameters"`
}

// noParams is sent for tools that take no arguments: some OpenAI-compatible
// providers reject a function without a parameters object.
var noParams = json.RawMessage(`{"type":"object","properties":{}}`)

// Client streams chat completions from an OpenAI-compatible /chat/completions
// endpoint (baseURL should NOT include the /chat/completions suffix).
type Client struct {
	baseURL string
	apiKey  string
	model   string
}

// NewClient creates a Client. baseURL is trimmed of a trailing slash.
func NewClient(baseURL, apiKey, model string) *Client {
	return &Client{baseURL: strings.TrimSuffix(baseURL, "/"), apiKey: apiKey, model: model}
}

// Stream sends messages (and tools, if any) with stream:true and calls
// onToken, in order, for each non-empty delta content chunk as it arrives
// over the SSE response. Any tool calls the model makes are assembled from
// their streamed fragments and returned once the stream ends; text the model
// wrote before calling a tool has already gone to onToken by then, so it can
// be spoken while the tool runs. Blocks until the stream ends (a "[DONE]"
// event or EOF), ctx is cancelled, or a request/transport error occurs. A
// malformed individual SSE line is skipped, not fatal -- providers
// occasionally interleave comments/keepalives.
func (c *Client) Stream(ctx context.Context, messages []Message, tools []Tool, onToken func(string)) ([]ToolCall, error) {
	body := map[string]any{
		"model":    c.model,
		"messages": messages,
		"stream":   true,
	}

	if len(tools) > 0 {
		sent := make([]Tool, len(tools))
		for i, t := range tools {
			if t.Type == "" {
				t.Type = "function"
			}

			if len(t.Function.Parameters) == 0 {
				t.Function.Parameters = noParams
			}

			sent[i] = t
		}

		body["tools"] = sent
	}

	reqBody, err := json.Marshal(body)
	if err != nil {
		return nil, fmt.Errorf("llm: marshal request: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+"/chat/completions", bytes.NewReader(reqBody))
	if err != nil {
		return nil, fmt.Errorf("llm: build request: %w", err)
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+c.apiKey)
	req.Header.Set("Accept", "text/event-stream")

	resp, err := sharedHTTPClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("llm: request: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return nil, fmt.Errorf("llm: non-2xx response: %s: %s", resp.Status, body)
	}

	scanner := bufio.NewScanner(resp.Body)
	scanner.Buffer(make([]byte, 0, 64*1024), 1024*1024)

	var calls toolCallAssembler

	for scanner.Scan() {
		line := scanner.Text()

		data, ok := strings.CutPrefix(line, "data: ")
		if !ok {
			continue
		}

		if data == "[DONE]" {
			return calls.done(), nil
		}

		var chunk chatCompletionChunk
		if err := json.Unmarshal([]byte(data), &chunk); err != nil {
			continue
		}

		for _, choice := range chunk.Choices {
			if choice.Delta.Content != "" {
				onToken(choice.Delta.Content)
			}

			for _, d := range choice.Delta.ToolCalls {
				calls.add(d)
			}
		}
	}

	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("llm: reading stream: %w", err)
	}

	return calls.done(), nil
}

type chatCompletionChunk struct {
	Choices []struct {
		Delta struct {
			Content   string          `json:"content"`
			ToolCalls []toolCallDelta `json:"tool_calls"`
		} `json:"delta"`
	} `json:"choices"`
}

// toolCallDelta is one streamed fragment of a tool call. OpenAI streams a
// call across many deltas (id and name first, then the arguments JSON in
// pieces), matched up by Index; some compatible providers (Gemini among
// them) send each call whole in one delta and may omit Index.
type toolCallDelta struct {
	Index    *int   `json:"index"`
	ID       string `json:"id"`
	Type     string `json:"type"`
	Function struct {
		Name      string `json:"name"`
		Arguments string `json:"arguments"`
	} `json:"function"`
	ExtraContent json.RawMessage `json:"extra_content"`
}

// toolCallAssembler joins toolCallDelta fragments back into whole calls.
type toolCallAssembler struct {
	calls []ToolCall
}

func (a *toolCallAssembler) add(d toolCallDelta) {
	i := a.slot(d)
	c := &a.calls[i]

	if d.ID != "" {
		c.ID = d.ID
	}

	if d.Type != "" {
		c.Type = d.Type
	}

	if d.Function.Name != "" {
		c.Function.Name = d.Function.Name
	}

	c.Function.Arguments += d.Function.Arguments

	if len(d.ExtraContent) > 0 && string(d.ExtraContent) != "null" {
		c.ExtraContent = append(json.RawMessage(nil), d.ExtraContent...)
	}
}

// slot picks which call a fragment belongs to: by Index when present --
// unless that slot already holds a different call ID, which means a provider
// that sends whole calls reused index 0 for the next one -- else by ID, else
// the last call (a continuation fragment carrying neither).
func (a *toolCallAssembler) slot(d toolCallDelta) int {
	if d.Index != nil && *d.Index >= 0 {
		i := *d.Index

		if i >= len(a.calls) {
			for len(a.calls) <= i {
				a.calls = append(a.calls, ToolCall{})
			}

			return i
		}

		if d.ID == "" || a.calls[i].ID == "" || a.calls[i].ID == d.ID {
			return i
		}
		// Same index, different call ID: fall through and append it by ID.
	}

	if d.ID != "" {
		for i := range a.calls {
			if a.calls[i].ID == d.ID {
				return i
			}
		}

		a.calls = append(a.calls, ToolCall{})

		return len(a.calls) - 1
	}

	if len(a.calls) == 0 {
		a.calls = append(a.calls, ToolCall{})
	}

	return len(a.calls) - 1
}

// done returns the assembled calls, dropping any fragment that never got a
// function name and defaulting Type and empty arguments.
func (a *toolCallAssembler) done() []ToolCall {
	var out []ToolCall

	for _, c := range a.calls {
		if c.Function.Name == "" {
			continue
		}

		if c.Type == "" {
			c.Type = "function"
		}

		if strings.TrimSpace(c.Function.Arguments) == "" {
			c.Function.Arguments = "{}"
		}

		out = append(out, c)
	}

	return out
}
