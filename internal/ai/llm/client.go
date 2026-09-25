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

// Message is one OpenAI-format chat message.
type Message struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

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

// Stream sends messages with stream:true and calls onToken, in order, for each
// non-empty delta content chunk as it arrives over the SSE response. Blocks
// until the stream ends (a "[DONE]" event or EOF), ctx is cancelled, or a
// request/transport error occurs. A malformed individual SSE line is skipped,
// not fatal -- providers occasionally interleave comments/keepalives.
func (c *Client) Stream(ctx context.Context, messages []Message, onToken func(string)) error {
	reqBody, err := json.Marshal(map[string]any{
		"model":    c.model,
		"messages": messages,
		"stream":   true,
	})
	if err != nil {
		return fmt.Errorf("llm: marshal request: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+"/chat/completions", bytes.NewReader(reqBody))
	if err != nil {
		return fmt.Errorf("llm: build request: %w", err)
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+c.apiKey)
	req.Header.Set("Accept", "text/event-stream")

	resp, err := sharedHTTPClient.Do(req)
	if err != nil {
		return fmt.Errorf("llm: request: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return fmt.Errorf("llm: non-2xx response: %s: %s", resp.Status, body)
	}

	scanner := bufio.NewScanner(resp.Body)
	scanner.Buffer(make([]byte, 0, 64*1024), 1024*1024)

	for scanner.Scan() {
		line := scanner.Text()

		data, ok := strings.CutPrefix(line, "data: ")
		if !ok {
			continue
		}

		if data == "[DONE]" {
			return nil
		}

		var chunk chatCompletionChunk
		if err := json.Unmarshal([]byte(data), &chunk); err != nil {
			continue
		}

		for _, choice := range chunk.Choices {
			if choice.Delta.Content != "" {
				onToken(choice.Delta.Content)
			}
		}
	}

	if err := scanner.Err(); err != nil {
		return fmt.Errorf("llm: reading stream: %w", err)
	}

	return nil
}

type chatCompletionChunk struct {
	Choices []struct {
		Delta struct {
			Content string `json:"content"`
		} `json:"delta"`
	} `json:"choices"`
}
