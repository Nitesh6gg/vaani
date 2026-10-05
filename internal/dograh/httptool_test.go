package dograh

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHTTPToolSchemaMatchesDograh(t *testing.T) {
	var params []toolParam
	require.NoError(t, json.Unmarshal([]byte(`[
		{"name": "query", "type": "string", "description": "what to search"},
		{"name": "max_results", "type": "number", "description": "how many", "required": false},
		{"name": "odd", "type": "date", "description": "unknown type"},
		{"name": "", "type": "string"},
		{"name": "tags", "type": "array", "description": "tags", "item_schema": [{"name": "label", "type": "string", "description": "l"}]},
		{"name": "addr", "type": "object", "description": "address", "item_schema": [{"name": "pin", "type": "number", "description": "p", "required": false}]}
	]`), &params))

	assert.JSONEq(t, `{"type": "object", "properties": {
		"query": {"type": "string", "description": "what to search"},
		"max_results": {"type": "number", "description": "how many"},
		"odd": {"type": "string", "description": "unknown type"},
		"tags": {"type": "array", "description": "tags", "items": {"type": "object", "properties": {"label": {"type": "string", "description": "l"}}, "required": ["label"]}},
		"addr": {"type": "object", "description": "address", "properties": {"pin": {"type": "number", "description": "p"}}, "required": []}
	}, "required": ["query", "odd", "tags", "addr"]}`, string(httpToolSchema(params)))
}

func TestAuthHeaderMatchesDograh(t *testing.T) {
	cases := []struct {
		typ, data, name, value string
		ok                     bool
	}{
		{"bearer_token", `{"token": "abc"}`, "Authorization", "Bearer abc", true},
		{"api_key", `{"api_key": "k1"}`, "X-API-Key", "k1", true},
		{"api_key", `{"header_name": "x-tavily", "api_key": "k2"}`, "x-tavily", "k2", true},
		{"basic_auth", `{"username": "u", "password": "p"}`, "Authorization", "Basic dTpw", true},
		{"custom_header", `{"header_value": "v"}`, "X-Custom", "v", true},
		{"none", `{}`, "", "", false},
	}
	for _, tc := range cases {
		name, value, ok, err := authHeader(Credential{Type: tc.typ, Data: json.RawMessage(tc.data)})
		require.NoError(t, err, tc.typ)
		assert.Equal(t, tc.ok, ok, tc.typ)
		assert.Equal(t, tc.name, name, tc.typ)
		assert.Equal(t, tc.value, value, tc.typ)
	}
}

func TestHTTPToolRun(t *testing.T) {
	var got struct {
		method, query, body, auth, custom, contentType string
	}

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		got.method, got.query, got.body = r.Method, r.URL.RawQuery, string(b)
		got.auth, got.custom, got.contentType = r.Header.Get("Authorization"), r.Header.Get("X-Team"), r.Header.Get("Content-Type")

		switch r.URL.Path {
		case "/text":
			w.WriteHeader(http.StatusBadGateway)
			_, _ = w.Write([]byte("upstream down"))
		case "/slow":
			time.Sleep(200 * time.Millisecond)
		default:
			_, _ = w.Write([]byte(`{"answer": 42}`))
		}
	}))
	defer srv.Close()

	build := func(config string) func(context.Context, json.RawMessage) (string, error) {
		tool, warn, err := buildTool(ToolRow{
			Name: "web search", Category: "http_api",
			Definition: json.RawMessage(`{"config": ` + config + `}`),
			Credential: &Credential{Type: "bearer_token", Data: json.RawMessage(`{"token": "secret"}`)},
		})
		require.NoError(t, err)
		require.Empty(t, warn)

		return tool.Run
	}

	t.Run("POST sends the arguments as JSON with config and credential headers", func(t *testing.T) {
		run := build(`{"url": "` + srv.URL + `/search", "headers": {"X-Team": "vaani"}, "credential_uuid": "c1"}`)
		res, err := run(context.Background(), json.RawMessage(`{"query": "दिल्ली मौसम", "max_results": 3}`))
		require.NoError(t, err)

		assert.JSONEq(t, `{"status": "success", "status_code": 200, "data": {"answer": 42}}`, res)
		assert.Equal(t, "POST", got.method)
		assert.JSONEq(t, `{"query": "दिल्ली मौसम", "max_results": 3}`, got.body)
		assert.Equal(t, "application/json", got.contentType)
		assert.Equal(t, "Bearer secret", got.auth)
		assert.Equal(t, "vaani", got.custom)
	})

	t.Run("GET sends the arguments as query parameters", func(t *testing.T) {
		run := build(`{"method": "get", "url": "` + srv.URL + `/search?lang=hi"}`)
		_, err := run(context.Background(), json.RawMessage(`{"q": "x", "n": 5, "exact": true, "ids": [1, 2]}`))
		require.NoError(t, err)

		assert.Equal(t, "GET", got.method)
		assert.Equal(t, "exact=true&ids=1&ids=2&lang=hi&n=5&q=x", got.query)
		assert.Empty(t, got.body)
		assert.Empty(t, got.auth, "no credential_uuid in config: no auth header")
	})

	t.Run("a non-JSON reply is a raw_response, whatever the status", func(t *testing.T) {
		run := build(`{"url": "` + srv.URL + `/text"}`)
		res, err := run(context.Background(), nil)
		require.NoError(t, err)

		assert.JSONEq(t, `{"status": "success", "status_code": 502, "data": {"raw_response": "upstream down"}}`, res)
		assert.Equal(t, "{}", got.body, "POST without arguments sends an empty object")
	})

	t.Run("timeout_ms is Dograh's timeout error", func(t *testing.T) {
		run := build(`{"url": "` + srv.URL + `/slow", "timeout_ms": 50}`)
		res, err := run(context.Background(), nil)
		require.NoError(t, err)

		assert.JSONEq(t, `{"status": "error", "error": "Request timed out after 0.05 seconds"}`, res)
	})
}

func TestBuildHTTPToolWarningsAndMessage(t *testing.T) {
	tool, warn, err := buildTool(ToolRow{
		UUID: "u1", Name: "lookup", Category: "http_api",
		Definition: json.RawMessage(`{"config": {"url": "https://x", "credential_uuid": "gone", "customMessage": "एक पल रुकिए", "messageType": "none"}}`),
	})
	require.NoError(t, err)
	assert.Contains(t, warn, "credential gone not found or not active")
	assert.Equal(t, "एक पल रुकिए", tool.Message, "http tools speak customMessage whatever messageType says")

	tool, _, err = buildTool(ToolRow{
		Name: "lookup", Category: "http_api",
		Definition: json.RawMessage(`{"config": {"url": "https://x", "customMessageType": "audio", "customMessage": "x"}}`),
	})
	require.NoError(t, err)
	assert.Empty(t, tool.Message, "audio messages aren't supported yet")

	_, _, err = buildTool(ToolRow{Name: "lookup", Category: "http_api", Definition: json.RawMessage(`{"config": {}}`)})
	assert.Error(t, err, "no url")

	assert.Equal(t, "5.0", pySeconds(5*time.Second))
}
