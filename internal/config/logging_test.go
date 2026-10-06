package config

import (
	"bytes"
	"encoding/json"
	"log/slog"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/nitesh/vaani/internal/version"
)

// testHandler builds the enriched JSON handler writing to buf, with a trace
// registered for call c1.
func testHandler(t *testing.T, env string) (*bytes.Buffer, slog.Handler) {
	t.Helper()

	buf := &bytes.Buffer{}

	RegisterCallTrace("c1", NewTraceID())
	t.Cleanup(func() { UnregisterCallTrace("c1") })

	return buf, newJSONHandler(buf, env)
}

// TestJSONLineShape is the golden-record test: one emitted line must
// unmarshal into exactly the structure the logging spec promises -- service
// group, ISO-8601 UTC millisecond timestamp, lowercase level, package-derived
// component, and the call's trace_id injected from its call_id.
func TestJSONLineShape(t *testing.T) {
	buf, h := testHandler(t, "production")

	trace := lookupTrace("c1")
	require.Regexp(t, `^[0-9a-f]{32}$`, trace)

	logger := slog.New(h)
	logger.Info("hello", "call_id", "c1", "port", 20000)

	var rec map[string]any
	require.NoError(t, json.Unmarshal(buf.Bytes(), &rec))

	assert.Equal(t, "hello", rec["msg"])

	svc, ok := rec["service"].(map[string]any)
	require.True(t, ok, "service group must be an object: %v", rec)
	assert.Equal(t, ServiceName, svc["name"])
	assert.Equal(t, version.Version, svc["version"])
	assert.Equal(t, "production", svc["environment"])

	assert.Equal(t, "info", rec["level"], "levels must be lowercase")

	ts, ok := rec["time"].(string)
	require.True(t, ok, "time must be a string: %v", rec["time"])
	assert.Regexp(t, regexp.MustCompile(`^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z$`), ts,
		"timestamps must be ISO-8601 UTC with milliseconds")

	assert.Equal(t, trace, rec["trace_id"], "the call's registered trace_id must be stamped on its lines")

	// The log call site lives in this test function, package config.
	assert.Equal(t, "config", rec["component"])

	assert.Equal(t, float64(20000), rec["port"], "other attrs must survive")

	// A line for an unregistered call carries no trace_id.
	logger.Info("other call", "call_id", "unknown-call")
	var other map[string]any
	require.NoError(t, json.Unmarshal([]byte(strings.SplitN(buf.String(), "\n", 2)[1]), &other))
	assert.NotContains(t, other, "trace_id")
}

func TestJSONLineLevelAndEnv(t *testing.T) {
	buf, h := testHandler(t, "staging")

	logger := slog.New(h)
	logger.Warn("degraded", "reason", "jitter")

	var rec map[string]any
	require.NoError(t, json.Unmarshal(buf.Bytes(), &rec))

	assert.Equal(t, "warn", rec["level"])
	assert.Equal(t, "staging", rec["service"].(map[string]any)["environment"])
}

// TestExplicitComponentWins: a call site that names its own component
// overrides the package-derived one.
func TestExplicitComponentWins(t *testing.T) {
	buf, h := testHandler(t, "production")

	slog.New(h).Info("db event", "component", "db")

	var rec map[string]any
	require.NoError(t, json.Unmarshal(buf.Bytes(), &rec))

	assert.Equal(t, "db", rec["component"])
}

func TestUnregisterRemovesTrace(t *testing.T) {
	buf, h := testHandler(t, "production")
	UnregisterCallTrace("c1")

	slog.New(h).Info("hello", "call_id", "c1")

	assert.NotContains(t, buf.String(), "trace_id")
}

// TestComponentFromFunction pins the package->component mapping, longest
// match first.
func TestComponentFromFunction(t *testing.T) {
	cases := []struct {
		fn   string
		want string
	}{
		{"github.com/nitesh/vaani/internal/ai/agent.(*Handler).run", "agent"},
		{"github.com/nitesh/vaani/internal/ai/agent/tenvad.Detect", "vad"},
		{"github.com/nitesh/vaani/internal/ai/llm.(*Client).Stream", "llm"},
		{"github.com/nitesh/vaani/internal/ai/stt.(*sarvamClient).Feed", "stt"},
		{"github.com/nitesh/vaani/internal/ai/tts.(*sarvamClient).Speak", "tts"},
		{"github.com/nitesh/vaani/internal/ari.(*Manager).teardown", "ari"},
		{"github.com/nitesh/vaani/internal/media.(*CallMedia).writeLoop", "media"},
		{"github.com/nitesh/vaani/internal/dograh.(*Store).FinishRun", "dograh"},
		{"github.com/nitesh/vaani/internal/session.(*Call).Teardown", "session"},
		{"github.com/nitesh/vaani/internal/config.Load", "config"},
		{"github.com/nitesh/vaani/cmd/server.main", "service"},
		{"some/foreign/package.Func", "service"},
	}
	for _, tc := range cases {
		t.Run(tc.fn, func(t *testing.T) {
			assert.Equal(t, tc.want, componentFromFunction(tc.fn))
		})
	}
}

// TestNewTraceIDFormat: 32 lowercase hex characters, unique across draws.
func TestNewTraceIDFormat(t *testing.T) {
	a, b := NewTraceID(), NewTraceID()

	assert.Regexp(t, `^[0-9a-f]{32}$`, a)
	assert.NotEqual(t, a, b)
}

// TestConfigureLoggingTimestampKeptMillisecondPrecision documents the old
// text format's replacement: the constant moved to ISO-8601 UTC.
func TestConfigureLoggingTimestampKeptMillisecondPrecision(t *testing.T) {
	ist := time.FixedZone("IST", 5*3600+1800)
	got := time.Date(2026, 10, 6, 11, 24, 14, 651_900_000, ist).UTC().Format(LogTimeFormat)

	assert.Equal(t, "2026-10-06T05:54:14.651Z", got)
}
