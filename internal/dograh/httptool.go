package dograh

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/nitesh/vaani/internal/ai/llm"
)

// Dograh's http_api tool, ported from api/services/workflow/tools/
// custom_tool.py (schema + execute_http_tool) and api/utils/credential_auth.py.

// defaultHTTPToolTimeout is Dograh's timeout_ms default.
const defaultHTTPToolTimeout = 5 * time.Second

// maxHTTPToolResponse caps how much of a reply is read. Dograh reads it all;
// ponytail: 1 MiB is far more than an LLM turn can use anyway.
const maxHTTPToolResponse = 1 << 20

// toolParam is one entry of an http_api tool's "parameters" (or of an
// array/object parameter's "item_schema").
type toolParam struct {
	Name        string      `json:"name"`
	Type        string      `json:"type"`
	Description string      `json:"description"`
	Required    *bool       `json:"required"` // Dograh's default: true
	ItemSchema  []toolParam `json:"item_schema"`
}

// paramTypes is Dograh's TYPE_MAP; anything else is a string.
var paramTypes = map[string]string{"string": "string", "number": "number", "boolean": "boolean"}

// httpToolSchema is the LLM parameters schema for an http_api tool, as
// Dograh's tool_to_function_schema builds it.
func httpToolSchema(params []toolParam) json.RawMessage {
	b, _ := json.Marshal(objectSchema(params, true))
	return b
}

// objectSchema builds {"type":"object","properties":...,"required":[...]}.
// Only top-level parameters may be arrays/objects: Dograh allows one level of
// item_schema, whose fields are plain types.
func objectSchema(params []toolParam, top bool) map[string]any {
	props := map[string]any{}
	required := []string{}

	for _, p := range params {
		if p.Name == "" {
			continue
		}

		switch {
		case top && p.Type == "array":
			props[p.Name] = map[string]any{"type": "array", "description": p.Description, "items": objectSchema(p.ItemSchema, false)}
		case top && p.Type == "object":
			s := objectSchema(p.ItemSchema, false)
			s["description"] = p.Description
			props[p.Name] = s
		default:
			t, ok := paramTypes[p.Type]
			if !ok {
				t = "string"
			}

			props[p.Name] = map[string]any{"type": t, "description": p.Description}
		}

		if p.Required == nil || *p.Required {
			required = append(required, p.Name)
		}
	}

	return map[string]any{"type": "object", "properties": props, "required": required}
}

// Credential is a row of Dograh's external_credentials, as attached to a tool
// by its config's credential_uuid.
type Credential struct {
	Type string
	Data json.RawMessage
}

// authHeader is Dograh's build_auth_header: the header a credential adds, or
// ok=false for type "none" / an unknown type.
func authHeader(c Credential) (name, value string, ok bool, err error) {
	var d map[string]string
	if len(c.Data) > 0 {
		if err := json.Unmarshal(c.Data, &d); err != nil {
			return "", "", false, fmt.Errorf("bad credential_data: %w", err)
		}
	}

	// Defaults apply only when the key is absent, as with Python's dict.get.
	get := func(key, def string) string {
		if v, ok := d[key]; ok {
			return v
		}

		return def
	}

	switch c.Type {
	case "bearer_token":
		return "Authorization", "Bearer " + get("token", ""), true, nil
	case "api_key":
		return get("header_name", "X-API-Key"), get("api_key", ""), true, nil
	case "basic_auth":
		return "Authorization", "Basic " + base64.StdEncoding.EncodeToString([]byte(get("username", "")+":"+get("password", ""))), true, nil
	case "custom_header":
		return get("header_name", "X-Custom"), get("header_value", ""), true, nil
	}

	return "", "", false, nil
}

// httpTool is one configured http_api tool.
type httpTool struct {
	method  string
	url     string
	headers map[string]string // the config's headers, then the credential's
	timeout time.Duration
}

// httpToolResult is Dograh's success result; field order as Dograh's dict.
type httpToolResult struct {
	Status     string          `json:"status"`
	StatusCode int             `json:"status_code"`
	Data       json.RawMessage `json:"data"`
}

// httpToolError is Dograh's failure result, with its wording.
func httpToolError(msg string) string {
	b, _ := json.Marshal(map[string]string{"status": "error", "error": msg})
	return string(b)
}

// run is execute_http_tool: the LLM's arguments go as a JSON body for
// POST/PUT/PATCH and as query parameters for GET/DELETE. Like Dograh, it
// never returns an error: any HTTP status is a "success" carrying the status
// code, and a failure is a {"status":"error"} result the LLM can react to.
func (t httpTool) run(ctx context.Context, rawArgs json.RawMessage) (string, error) {
	var args map[string]any

	if len(bytes.TrimSpace(rawArgs)) > 0 {
		dec := json.NewDecoder(bytes.NewReader(rawArgs))
		dec.UseNumber() // 5 stays "5" in a query string, not "5e+00"

		if err := dec.Decode(&args); err != nil {
			return httpToolError("Tool execution failed: bad arguments: " + err.Error()), nil
		}
	}

	u, err := url.Parse(t.url)
	if err != nil {
		return httpToolError("Tool execution failed: " + err.Error()), nil
	}

	var body io.Reader

	switch t.method {
	case http.MethodPost, http.MethodPut, http.MethodPatch:
		if args == nil {
			args = map[string]any{}
		}

		b, _ := json.Marshal(args)
		body = bytes.NewReader(b)
	case http.MethodGet, http.MethodDelete:
		if len(args) > 0 {
			q := u.Query() // merged with the URL's own query, as httpx does
			for k, v := range args {
				addQuery(q, k, v)
			}

			u.RawQuery = q.Encode()
		}
	}

	rctx, cancel := context.WithTimeout(ctx, t.timeout)
	defer cancel()

	req, err := http.NewRequestWithContext(rctx, t.method, u.String(), body)
	if err != nil {
		return httpToolError("Tool execution failed: " + err.Error()), nil
	}

	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}

	for k, v := range t.headers {
		req.Header.Set(k, v)
	}

	resp, err := llm.SharedHTTPClient.Do(req)
	if err == nil {
		defer func() { _ = resp.Body.Close() }()

		var data []byte

		data, err = io.ReadAll(io.LimitReader(resp.Body, maxHTTPToolResponse))
		if err == nil {
			if !json.Valid(data) {
				data, _ = json.Marshal(map[string]string{"raw_response": string(data)})
			}

			b, _ := json.Marshal(httpToolResult{Status: "success", StatusCode: resp.StatusCode, Data: data})

			return string(b), nil
		}
	}

	if errors.Is(err, context.DeadlineExceeded) && ctx.Err() == nil {
		return httpToolError("Request timed out after " + pySeconds(t.timeout) + " seconds"), nil
	}

	return httpToolError("Request failed: " + err.Error()), nil
}

// addQuery adds one argument the way httpx encodes params: lists repeat the
// key, booleans are lowercase, null is empty.
func addQuery(q url.Values, k string, v any) {
	if list, ok := v.([]any); ok {
		for _, item := range list {
			q.Add(k, queryValue(item))
		}

		return
	}

	q.Add(k, queryValue(v))
}

func queryValue(v any) string {
	switch x := v.(type) {
	case nil:
		return ""
	case string:
		return x
	case bool:
		return strconv.FormatBool(x)
	case json.Number:
		return x.String()
	default:
		b, _ := json.Marshal(x)
		return string(b)
	}
}

// pySeconds formats a timeout as Python prints timeout_ms / 1000: 5s -> "5.0".
func pySeconds(d time.Duration) string {
	s := strconv.FormatFloat(d.Seconds(), 'f', -1, 64)
	if !strings.Contains(s, ".") {
		s += ".0"
	}

	return s
}
