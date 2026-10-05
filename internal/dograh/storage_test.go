package dograh

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/nitesh/vaani/internal/ai/agent"
)

// TestStorageSignMatchesMinioClient: the expected header was produced by the
// MinIO Python client Dograh uses (minio.signer.sign_v4_s3) for the same
// request -- PUT voice-audio/recordings/42.wav, body "hello", content-type
// audio/wav, minioadmin/minioadmin, 2026-10-05 12:00:00 UTC.
func TestStorageSignMatchesMinioClient(t *testing.T) {
	s := &Storage{Endpoint: "minio:9000", AccessKey: "minioadmin", SecretKey: "minioadmin", Bucket: "voice-audio"}

	req, err := http.NewRequest(http.MethodPut, "http://minio:9000/voice-audio/recordings/42.wav", bytes.NewReader([]byte("hello")))
	require.NoError(t, err)
	req.Header.Set("Content-Type", "audio/wav")

	s.sign(req, []byte("hello"), time.Date(2026, 10, 5, 12, 0, 0, 0, time.UTC))

	assert.Equal(t, "AWS4-HMAC-SHA256 Credential=minioadmin/20261005/us-east-1/s3/aws4_request, "+
		"SignedHeaders=content-type;host;x-amz-content-sha256;x-amz-date, "+
		"Signature=98334d8941ceb5e9e6ee5186909035642d88ff61052e51f5a178729dcb0fdca0",
		req.Header.Get("Authorization"))
	assert.Equal(t, "20261005T120000Z", req.Header.Get("X-Amz-Date"))
}

func TestTranscriptTextMatchesDograh(t *testing.T) {
	events := []agent.LogEvent{
		{Type: "rtf-node-transition", Timestamp: "t0", Payload: map[string]any{"node_name": "Start"}},
		{Type: "rtf-bot-text", Timestamp: "t1", Payload: map[string]any{"text": "नमस्ते", "timestamp": "2026-10-05T12:00:00.000+00:00"}},
		{Type: "rtf-user-transcription", Timestamp: "t2", Payload: map[string]any{"text": "हाँ", "final": true}},
		{Type: "rtf-function-call-start", Timestamp: "t3", Payload: map[string]any{"function_name": "x"}},
	}

	assert.Equal(t, "[2026-10-05T12:00:00.000+00:00] Agent: नमस्ते\n[t2] User: हाँ\n", transcriptText(events))
	assert.Empty(t, transcriptText(nil))
}

func TestGatheredContextMatchesDograh(t *testing.T) {
	mapping := map[string]string{"user_hangup": "HU", "interested": "INT"}
	mapFn := func(v string) string {
		if m, ok := mapping[v]; ok {
			return m
		}

		return v
	}

	cases := []struct {
		name   string
		sum    agent.CallSummary
		want   map[string]any
		mapped string
	}{
		{
			name: "no extraction, caller hung up",
			sum:  agent.CallSummary{NodesVisited: []string{"Start"}},
			want: map[string]any{"nodes_visited": []string{"Start"}, "call_disposition": "user_hangup",
				"mapped_call_disposition": "HU", "call_tags": []any{"user_hangup"}},
			mapped: "HU",
		},
		{
			name: "extracted variables, disposition and tags",
			sum: agent.CallSummary{
				NodesVisited: []string{"Start", "End"}, EndReason: "user_qualified", UserSpoke: true,
				Extracted:     map[string]any{"party": "AAP", "call_disposition": "interested", "tag_x": "voter"},
				ExtractedKeys: []string{"party", "call_disposition", "tag_x"},
			},
			want: map[string]any{
				"nodes_visited": []string{"Start", "End"},
				"party":         "AAP", "call_disposition": "interested", "tag_x": "voter",
				"extracted_variables":        map[string]any{"party": "AAP", "call_disposition": "interested", "tag_x": "voter"},
				"extracted_call_disposition": "interested",
				"mapped_call_disposition":    "INT",
				"call_tags":                  []any{"interested", "user_speech", "voter"},
			},
			mapped: "INT",
		},
		{
			name: "an empty extracted disposition falls back to why the call ended",
			sum: agent.CallSummary{EndReason: "end_call_tool",
				Extracted: map[string]any{"call_disposition": ""}, ExtractedKeys: []string{"call_disposition"}},
			want: map[string]any{"nodes_visited": []string{}, "call_disposition": "end_call_tool",
				"extracted_variables":     map[string]any{"call_disposition": ""},
				"mapped_call_disposition": "end_call_tool", "call_tags": []any{"end_call_tool"}},
			mapped: "end_call_tool",
		},
	}

	for _, tc := range cases {
		g, mapped := gatheredContext(tc.sum, mapFn)
		assert.Equal(t, tc.want, g, tc.name)
		assert.Equal(t, tc.mapped, mapped, tc.name)
	}
}

func TestStoragePut(t *testing.T) {
	var got struct{ path, contentType, auth, body string }

	status := http.StatusOK
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		got.path, got.contentType, got.auth, got.body = r.URL.Path, r.Header.Get("Content-Type"), r.Header.Get("Authorization"), string(b)
		w.WriteHeader(status)
	}))
	defer srv.Close()

	s := &Storage{Endpoint: strings.TrimPrefix(srv.URL, "http://"), AccessKey: "a", SecretKey: "b", Bucket: "voice-audio"}

	require.NoError(t, s.Put(context.Background(), "transcripts/7.txt", "text/plain; charset=utf-8", []byte("User: hi\n")))
	assert.Equal(t, "/voice-audio/transcripts/7.txt", got.path)
	assert.Equal(t, "text/plain; charset=utf-8", got.contentType)
	assert.True(t, strings.HasPrefix(got.auth, "AWS4-HMAC-SHA256 Credential=a/"))
	assert.Equal(t, "User: hi\n", got.body)

	status = http.StatusForbidden
	assert.ErrorContains(t, s.Put(context.Background(), "x", "audio/wav", nil), "403")
}
