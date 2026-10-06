package llm

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func sseServer(t *testing.T, lines []string) *httptest.Server {
	t.Helper()

	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		flusher := w.(http.Flusher)

		for _, line := range lines {
			_, _ = fmt.Fprintln(w, line)
			flusher.Flush()
		}
	}))
}

func TestClient_Stream_DeliversTokensInOrder(t *testing.T) {
	srv := sseServer(t, []string{
		`data: {"choices":[{"delta":{"content":"Hel"}}]}`,
		`data: {"choices":[{"delta":{"content":"lo"}}]}`,
		`data: {"choices":[{"delta":{"content":"!"}}]}`,
		`data: [DONE]`,
	})
	defer srv.Close()

	c := NewClient(srv.URL, "test-key", "test-model")

	var got []string
	_, err := c.Stream(context.Background(), []Message{{Role: "user", Content: "hi"}}, nil, func(tok string) {
		got = append(got, tok)
	})

	require.NoError(t, err)
	assert.Equal(t, []string{"Hel", "lo", "!"}, got)
}

func TestClient_Stream_EdgeCases(t *testing.T) {
	cases := []struct {
		name    string
		lines   []string
		want    []string
		wantErr string
	}{
		{"data: without a space (valid SSE)",
			[]string{`data:{"choices":[{"delta":{"content":"a"}}]}`, `data:[DONE]`}, []string{"a"}, ""},
		{"error object inside a 200 stream",
			[]string{`data: {"error":{"message":"model_overloaded","code":503}}`}, nil, "provider error in stream"},
		{"EOF with nothing delivered",
			[]string{`: keepalive`}, nil, "without any reply"},
		{"EOF without [DONE] after a reply is still a reply",
			[]string{`data: {"choices":[{"delta":{"content":"ok"}}]}`}, []string{"ok"}, ""},
		{"empty reply ending in [DONE] is a (legitimately empty) reply",
			[]string{`data: [DONE]`}, nil, ""},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			srv := sseServer(t, tc.lines)
			defer srv.Close()

			var got []string
			_, err := NewClient(srv.URL, "k", "m").Stream(context.Background(), nil, nil, func(tok string) { got = append(got, tok) })

			if tc.wantErr != "" {
				require.ErrorContains(t, err, tc.wantErr)
				return
			}

			require.NoError(t, err)
			assert.Equal(t, tc.want, got)
		})
	}
}

// TestClient_Stream_IdleTimeout: a provider that sends headers and then
// nothing must not hang the turn.
func TestClient_Stream_IdleTimeout(t *testing.T) {
	release := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = fmt.Fprintln(w, `data: {"choices":[{"delta":{"content":"a"}}]}`)
		w.(http.Flusher).Flush()
		select {
		case <-release:
		case <-r.Context().Done():
		}
	}))
	defer srv.Close()
	defer close(release)

	c := NewClient(srv.URL, "k", "m")
	c.idleTimeout = 100 * time.Millisecond

	start := time.Now()
	_, err := c.Stream(context.Background(), nil, nil, func(string) {})

	require.ErrorIs(t, err, errStreamIdle)
	assert.Less(t, time.Since(start), 2*time.Second)
}

func TestClient_Stream_SkipsMalformedLinesWithoutFailing(t *testing.T) {
	srv := sseServer(t, []string{
		`: this is a comment/keepalive`,
		`data: not valid json`,
		`data: {"choices":[{"delta":{"content":"ok"}}]}`,
		`data: [DONE]`,
	})
	defer srv.Close()

	c := NewClient(srv.URL, "test-key", "test-model")

	var got []string
	_, err := c.Stream(context.Background(), nil, nil, func(tok string) { got = append(got, tok) })

	require.NoError(t, err)
	assert.Equal(t, []string{"ok"}, got)
}

func TestClient_Stream_NonTwoXXReturnsError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusUnauthorized)
		_, _ = w.Write([]byte(`{"error":"bad key"}`))
	}))
	defer srv.Close()

	c := NewClient(srv.URL, "bad-key", "test-model")

	_, err := c.Stream(context.Background(), nil, nil, func(string) {})

	require.Error(t, err)
	assert.Contains(t, err.Error(), "401")
}

func TestClient_Stream_ContextCancelStopsPromptly(t *testing.T) {
	// A server that streams forever, one token every call, until the client
	// goes away -- barge-in's LLM cancel must stop reading immediately.
	blockedForever := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		flusher := w.(http.Flusher)
		_, _ = fmt.Fprintln(w, `data: {"choices":[{"delta":{"content":"first"}}]}`)
		flusher.Flush()
		<-r.Context().Done() // hang until the client cancels
		close(blockedForever)
	}))
	defer srv.Close()

	c := NewClient(srv.URL, "test-key", "test-model")

	ctx, cancel := context.WithCancel(context.Background())

	var got []string
	done := make(chan error, 1)

	go func() {
		_, err := c.Stream(ctx, nil, nil, func(tok string) {
			got = append(got, tok)
			cancel() // simulate barge-in firing right after the first token
		})
		done <- err
	}()

	err := <-done
	assert.Error(t, err, "a cancelled context must surface as an error, not a silent success")
	assert.Equal(t, []string{"first"}, got)

	// The server-side goroutine reacts to the same cancellation independently
	// of the client returning, so wait for it rather than checking instantly.
	select {
	case <-blockedForever:
	case <-time.After(time.Second):
		t.Fatal("server's request context should have been cancelled")
	}
}

// TestClient_Stream_AssemblesFragmentedToolCall covers OpenAI-style
// streaming: one call's id/name arrive first, its arguments JSON in pieces
// across later deltas, all matched by index. Text before the call must reach
// onToken so it can be spoken while the tool runs.
func TestClient_Stream_AssemblesFragmentedToolCall(t *testing.T) {
	srv := sseServer(t, []string{
		`data: {"choices":[{"delta":{"content":"Let me check."}}]}`,
		`data: {"choices":[{"delta":{"tool_calls":[{"index":0,"id":"call_1","type":"function","function":{"name":"lookup_pin","arguments":""}}]}}]}`,
		`data: {"choices":[{"delta":{"tool_calls":[{"index":0,"function":{"arguments":"{\"pin\":"}}]}}]}`,
		`data: {"choices":[{"delta":{"tool_calls":[{"index":0,"function":{"arguments":"\"120205\"}"}}]}}]}`,
		`data: [DONE]`,
	})
	defer srv.Close()

	var got []string

	calls, err := NewClient(srv.URL, "k", "m").Stream(context.Background(), nil, nil, func(tok string) {
		got = append(got, tok)
	})

	require.NoError(t, err)
	assert.Equal(t, []string{"Let me check."}, got)
	require.Len(t, calls, 1)
	assert.Equal(t, "call_1", calls[0].ID)
	assert.Equal(t, "lookup_pin", calls[0].Function.Name)
	assert.JSONEq(t, `{"pin":"120205"}`, calls[0].Function.Arguments)
}

// TestClient_Stream_GeminiWholeCallsKeepThoughtSignature covers Gemini's
// OpenAI-compatible style: each call arrives whole in one delta, a second
// parallel call reuses index 0, and each carries extra_content (the thought
// signature) that must be kept byte-for-byte and sent back on the next
// request -- Gemini 3 rejects the follow-up otherwise.
func TestClient_Stream_GeminiWholeCallsKeepThoughtSignature(t *testing.T) {
	srv := sseServer(t, []string{
		`data: {"choices":[{"delta":{"tool_calls":[{"index":0,"id":"a","type":"function","function":{"name":"one","arguments":"{}"},"extra_content":{"google":{"thought_signature":"sigA"}}}]}}]}`,
		`data: {"choices":[{"delta":{"tool_calls":[{"index":0,"id":"b","type":"function","function":{"name":"two","arguments":"{\"x\":1}"}}]}}]}`,
		`data: [DONE]`,
	})
	defer srv.Close()

	calls, err := NewClient(srv.URL, "k", "m").Stream(context.Background(), nil, nil, func(string) {})

	require.NoError(t, err)
	require.Len(t, calls, 2, "a reused index with a different id is a new call, not a continuation")
	assert.Equal(t, "one", calls[0].Function.Name)
	assert.JSONEq(t, `{"google":{"thought_signature":"sigA"}}`, string(calls[0].ExtraContent))
	assert.Equal(t, "two", calls[1].Function.Name)
	assert.JSONEq(t, `{"x":1}`, calls[1].Function.Arguments)

	// Round-trip: the assistant message carrying these calls must serialize
	// the signature back exactly where Gemini expects it.
	raw, err := json.Marshal(Message{Role: "assistant", ToolCalls: calls[:1]})
	require.NoError(t, err)
	assert.JSONEq(t,
		`{"role":"assistant","tool_calls":[{"id":"a","type":"function","function":{"name":"one","arguments":"{}"},"extra_content":{"google":{"thought_signature":"sigA"}}}]}`,
		string(raw))
}

func TestClient_Stream_SendsToolsWithDefaultParameters(t *testing.T) {
	var body map[string]any

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewDecoder(r.Body).Decode(&body)
		_, _ = fmt.Fprintln(w, `data: [DONE]`)
	}))
	defer srv.Close()

	_, err := NewClient(srv.URL, "k", "m").Stream(context.Background(), nil,
		[]Tool{{Function: FunctionDef{Name: "end_call", Description: "End the call."}}}, func(string) {})
	require.NoError(t, err)

	tools, ok := body["tools"].([]any)
	require.True(t, ok, "tools must be sent when provided")
	require.Len(t, tools, 1)

	tool := tools[0].(map[string]any)
	assert.Equal(t, "function", tool["type"])

	fn := tool["function"].(map[string]any)
	assert.Equal(t, "end_call", fn["name"])
	assert.Equal(t, map[string]any{"type": "object", "properties": map[string]any{}}, fn["parameters"],
		"a no-argument tool still needs a parameters object for some providers")
}

func TestClient_Stream_TrimsTrailingSlashFromBaseURL(t *testing.T) {
	var gotPath string

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		_, _ = fmt.Fprintln(w, `data: [DONE]`)
	}))
	defer srv.Close()

	c := NewClient(srv.URL+"/", "k", "m")
	_, err := c.Stream(context.Background(), nil, nil, func(string) {})
	require.NoError(t, err)

	assert.Equal(t, "/chat/completions", gotPath)
	assert.False(t, strings.HasSuffix(c.baseURL, "/"))
}
