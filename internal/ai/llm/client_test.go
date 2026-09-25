package llm

import (
	"context"
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
	err := c.Stream(context.Background(), []Message{{Role: "user", Content: "hi"}}, func(tok string) {
		got = append(got, tok)
	})

	require.NoError(t, err)
	assert.Equal(t, []string{"Hel", "lo", "!"}, got)
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
	err := c.Stream(context.Background(), nil, func(tok string) { got = append(got, tok) })

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

	err := c.Stream(context.Background(), nil, func(string) {})

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
		done <- c.Stream(ctx, nil, func(tok string) {
			got = append(got, tok)
			cancel() // simulate barge-in firing right after the first token
		})
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

func TestClient_Stream_TrimsTrailingSlashFromBaseURL(t *testing.T) {
	var gotPath string

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		_, _ = fmt.Fprintln(w, `data: [DONE]`)
	}))
	defer srv.Close()

	c := NewClient(srv.URL+"/", "k", "m")
	require.NoError(t, c.Stream(context.Background(), nil, func(string) {}))

	assert.Equal(t, "/chat/completions", gotPath)
	assert.False(t, strings.HasSuffix(c.baseURL, "/"))
}
