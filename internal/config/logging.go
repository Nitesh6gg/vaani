package config

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"io"
	"log/slog"
	"os"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/nitesh/vaani/internal/version"
)

// LogTimeFormat is every log line's timestamp: ISO-8601 UTC with millisecond
// precision ("2026-10-06T09:19:16.278Z").
const LogTimeFormat = "2006-01-02T15:04:05.000Z07:00"

// ServiceName is reported as service.name in every log line.
const ServiceName = "vaani-engine"

// Log level guidelines (who logs what, at which level):
//
//   - DEBUG: high-frequency detail -- streaming ticks, per-packet events,
//     LLM stream chunk processing, VAD hop processing. Off in production
//     unless troubleshooting.
//   - INFO: key state transitions -- call answered, workflow node
//     transitions, turn completed, final transcripts, hangup, and the
//     caller's speech started/ended boundaries (kept at INFO deliberately:
//     they are an operator-facing feature, not an internal tick).
//   - WARN: degraded experience -- barge-in interruptions, transcript
//     filtering during playback, fallback configurations, size-cap hits.
//   - ERROR: non-fatal system failures -- STT socket drops and retries,
//     database write fallbacks, LLM/tool execution errors.

// callTraces maps call_id -> W3C trace_id (32 hex chars) so the log handler
// can stamp every line that carries a call_id with its call's trace.
// Registered at call setup, removed at teardown -- bounded by live calls.
var callTraces sync.Map

// NewTraceID generates a W3C-format trace id: 16 random bytes as 32 hex
// characters.
func NewTraceID() string {
	var b [16]byte

	if _, err := rand.Read(b[:]); err != nil {
		// crypto/rand does not fail on Linux/Windows; a zero trace would
		// still correlate the call's lines with each other.
		return hex.EncodeToString(b[:])
	}

	return hex.EncodeToString(b[:])
}

// RegisterCallTrace associates a call with its trace id: from then on, every
// log line carrying call_id=<callID> is stamped with trace_id automatically.
func RegisterCallTrace(callID, traceID string) {
	callTraces.Store(callID, traceID)
}

// UnregisterCallTrace drops the association at teardown, keeping the registry
// bounded by live calls.
func UnregisterCallTrace(callID string) {
	callTraces.Delete(callID)
}

func lookupTrace(callID string) string {
	if v, ok := callTraces.Load(callID); ok {
		if s, ok := v.(string); ok {
			return s
		}
	}

	return ""
}

// ConfigureLogging installs the process's default slog logger: single-line
// JSON on stderr, UTC ISO-8601 millisecond timestamps, lowercase level names,
// a service group (name/version/deployment.environment) on every line, the
// component derived from the logging call site's package, and trace_id
// stamped on every line that carries a call_id. The deployment tier comes
// from DEPLOYMENT_ENVIRONMENT (default "development"; set "production" in
// deployment .env files).
func ConfigureLogging() {
	env := os.Getenv("DEPLOYMENT_ENVIRONMENT")
	if env == "" {
		env = "development"
	}

	slog.SetDefault(slog.New(newJSONHandler(os.Stderr, env)))
}

// newJSONHandler builds the base JSON handler: UTC ISO-8601 millisecond
// timestamps and lowercase level names, per the logging spec.
func newJSONHandler(w io.Writer, env string) slog.Handler {
	opts := &slog.HandlerOptions{
		ReplaceAttr: func(groups []string, a slog.Attr) slog.Attr {
			if len(groups) != 0 {
				return a
			}

			switch a.Key {
			case slog.TimeKey:
				if t, ok := a.Value.Any().(time.Time); ok {
					a.Value = slog.StringValue(t.UTC().Format(LogTimeFormat))
				}
			case slog.LevelKey:
				if l, ok := a.Value.Any().(slog.Level); ok {
					a.Value = slog.StringValue(strings.ToLower(l.String()))
				}
			}

			return a
		},
	}

	return &enrichHandler{inner: slog.NewJSONHandler(w, opts), env: env}
}

// enrichHandler wraps the JSON handler, prepending what every line carries
// but no call site should have to repeat: the service group, the call's
// trace_id (looked up from the record's call_id), and the component derived
// from the logging call site's package. Explicit attrs at a call site win
// over derived ones.
type enrichHandler struct {
	inner   slog.Handler
	env     string
	presets []slog.Attr
}

func (e *enrichHandler) Enabled(ctx context.Context, l slog.Level) bool {
	return e.inner.Enabled(ctx, l)
}

func (e *enrichHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	presets := make([]slog.Attr, 0, len(e.presets)+len(attrs))
	presets = append(presets, e.presets...)
	presets = append(presets, attrs...)

	return &enrichHandler{inner: e.inner.WithAttrs(attrs), env: e.env, presets: presets}
}

func (e *enrichHandler) WithGroup(name string) slog.Handler {
	// No call site uses groups today; if one appears, the injected fields
	// would nest under that group too. Accepted and documented.
	return &enrichHandler{inner: e.inner.WithGroup(name), env: e.env, presets: e.presets}
}

func (e *enrichHandler) Handle(_ context.Context, r slog.Record) error {
	out := slog.NewRecord(r.Time, r.Level, r.Message, r.PC)

	out.AddAttrs(slog.Group("service",
		slog.String("name", ServiceName),
		slog.String("version", version.Version),
		slog.String("environment", e.env),
	))

	if traceID := e.traceFor(&r); traceID != "" {
		out.AddAttrs(slog.String("trace_id", traceID))
	}

	if component := e.componentFor(&r); component != "" {
		out.AddAttrs(slog.String("component", component))
	}

	for _, a := range e.presets {
		out.AddAttrs(a)
	}

	r.Attrs(func(a slog.Attr) bool {
		out.AddAttrs(a)

		return true
	})

	return e.inner.Handle(context.Background(), out)
}

// traceFor looks up the record's call_id in the trace registry.
func (e *enrichHandler) traceFor(r *slog.Record) string {
	var callID string

	r.Attrs(func(a slog.Attr) bool {
		if a.Key == "call_id" && a.Value.Kind() == slog.KindString {
			callID = a.Value.String()

			return false
		}

		return true
	})

	if callID == "" {
		return ""
	}

	return lookupTrace(callID)
}

// hasExplicitComponent reports whether the record already carries a
// component attr, which wins over the package-derived one.
func (e *enrichHandler) hasExplicitComponent(r *slog.Record) bool {
	exists := false

	r.Attrs(func(a slog.Attr) bool {
		if a.Key == "component" {
			exists = true

			return false
		}

		return true
	})

	return exists
}

// componentFor derives the component from the logging call site's package:
// internal/ai/agent -> "agent", internal/ai/agent/tenvad -> "vad",
// internal/ari -> "ari", and so on. "service" covers cmd/ binaries and
// anything outside the internal tree.
func (e *enrichHandler) componentFor(r *slog.Record) string {
	if e.hasExplicitComponent(r) {
		return ""
	}

	if r.PC == 0 {
		return "service"
	}

	frame, _ := runtime.CallersFrames([]uintptr{r.PC}).Next()

	return componentFromFunction(frame.Function)
}

// componentFromFunction maps a function name like
// "github.com/nitesh/vaani/internal/ai/agent.(*Handler).run" to its
// component. Longest match wins (tenvad before agent).
func componentFromFunction(fn string) string {
	for _, m := range []struct {
		match     string
		component string
	}{
		{"/internal/ai/agent/tenvad", "vad"},
		{"/internal/ai/agent", "agent"},
		{"/internal/ai/llm", "llm"},
		{"/internal/ai/stt", "stt"},
		{"/internal/ai/tts", "tts"},
		{"/internal/ari", "ari"},
		{"/internal/media", "media"},
		{"/internal/dograh", "dograh"},
		{"/internal/session", "session"},
		{"/internal/metrics", "metrics"},
		{"/internal/config", "config"},
		{"/internal/", "app"},
	} {
		if strings.Contains(fn, m.match) {
			return m.component
		}
	}

	return "service"
}
