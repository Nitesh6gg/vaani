package config

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"io"
	"log/slog"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/nitesh/vaani/internal/version"
)

// LogTimeFormat is every log line's timestamp: ISO-8601 UTC with millisecond
// precision ("2026-10-06T09:19:16.278Z").
const LogTimeFormat = "2006-01-02T15:04:05.000Z07:00"

// ServiceName is reported as service.name in every log line.
const ServiceName = "vaani-engine"

// Log level guidelines (who logs what, at which level):
//
//   - DEBUG: per-step and high-frequency detail -- each TTS chunk, LLM first
//     token / stream end, TTS first audio / delivery, STT flushes and the
//     STT's own speech start/end signals, local VAD speech start/end. Their
//     timings are summed up on turn.agent. Off in production unless
//     troubleshooting.
//   - INFO: the call's story -- call answered/bridged/ended, workflow loaded,
//     turn.user / turn.agent, node transitions, tool calls, interruptions
//     (normal conversation, not a fault), transcripts ignored, the Dograh
//     run, and one call.summary per call.
//   - WARN: degraded experience -- fallbacks, a dead TTS connection, media
//     with no audio, size-cap hits, a provider retry.
//   - ERROR: failures -- a call that can't run, a record that can't be
//     written, an LLM/tool error.
//
// Every line about a call carries call_id; the handler adds its trace_id and
// (once the Dograh run exists) run_id. Set component and event explicitly
// where the call site's package isn't what the line is about.

// calls maps call_id -> its log context, so the handler can stamp every line
// carrying a call_id with the call's trace_id and run_id. Reference-counted:
// registered at call setup, held by whatever still logs about the call after
// its teardown (writing the Dograh run), and dropped when the last holder
// lets go -- bounded by live calls.
var (
	callsMu sync.Mutex
	calls   = map[string]*callLogContext{}
)

type callLogContext struct {
	traceID string
	runID   int64
	refs    int
}

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
// It holds one reference (see UnregisterCallTrace).
func RegisterCallTrace(callID, traceID string) {
	callsMu.Lock()
	defer callsMu.Unlock()

	calls[callID] = &callLogContext{traceID: traceID, refs: 1}
}

// HoldCallTrace takes one more reference on a registered call, for work that
// keeps logging about it after its teardown. Release with UnregisterCallTrace.
func HoldCallTrace(callID string) {
	callsMu.Lock()
	defer callsMu.Unlock()

	if c := calls[callID]; c != nil {
		c.refs++
	}
}

// UnregisterCallTrace releases one reference; the call's context is dropped
// when the last one is released.
func UnregisterCallTrace(callID string) {
	callsMu.Lock()
	defer callsMu.Unlock()

	if c := calls[callID]; c != nil {
		if c.refs--; c.refs <= 0 {
			delete(calls, callID)
		}
	}
}

// SetCallRunID records the call's Dograh run id: from then on its lines carry
// run_id too.
func SetCallRunID(callID string, runID int64) {
	callsMu.Lock()
	defer callsMu.Unlock()

	if c := calls[callID]; c != nil {
		c.runID = runID
	}
}

func lookupCall(callID string) (traceID string, runID int64) {
	callsMu.Lock()
	defer callsMu.Unlock()

	if c := calls[callID]; c != nil {
		return c.traceID, c.runID
	}

	return "", 0
}

func lookupTrace(callID string) string {
	t, _ := lookupCall(callID)
	return t
}

// ConfigureLogging installs the process's default slog logger: single-line
// JSON (default) or key=value text on stderr, UTC ISO-8601 millisecond
// timestamps, lowercase level names, a service group
// (name/version/deployment.environment) on every line, the component derived
// from the logging call site's package, and trace_id stamped on every line
// that carries a call_id.
//
// Read from the environment or .env (loaded here first, so .env values work
// even though this runs before config.Load): DEPLOYMENT_ENVIRONMENT
// (default "development"; set "production" in deployment .env files),
// LOG_FORMAT ("json" default, or "text" for development), and LOG_LEVEL
// ("info" default; "debug", "warn", "error" also accepted). Invalid values
// fall back to the defaults with a warning -- never fatal.
//
// If OTEL_EXPORTER_OTLP_LOGS_ENDPOINT or OTEL_EXPORTER_OTLP_ENDPOINT is set
// (e.g. "http://<signoz-host>:4318" -- the same OTLP/HTTP endpoint and env
// var names a Spring Boot app's otlp.logging.endpoint would use), every log
// line is also pushed there via go.opentelemetry.io/contrib/bridges/otelslog,
// alongside (not instead of) the existing stderr output. Call the returned
// shutdown func once, after the last line that should reach it, so the
// batched exporter flushes before the process exits.
func ConfigureLogging() (shutdown func(context.Context) error) {
	// Apply .env before reading logging config: this runs before
	// config.Load, and without this a .env-set LOG_FORMAT/LOG_LEVEL/
	// DEPLOYMENT_ENVIRONMENT would be invisible. Real environment variables
	// still win (loadDotenv never overwrites them).
	loadDotenv()

	env := os.Getenv("DEPLOYMENT_ENVIRONMENT")
	if env == "" {
		env = "development"
	}

	format, formatOK := parseLogFormat(os.Getenv("LOG_FORMAT"))
	level, levelOK := parseLogLevel(os.Getenv("LOG_LEVEL"))

	if !formatOK {
		slog.Warn("LOG_FORMAT invalid; using json", "value", os.Getenv("LOG_FORMAT"), "valid", "json|text")
	}

	if !levelOK {
		slog.Warn("LOG_LEVEL invalid; using info", "value", os.Getenv("LOG_LEVEL"), "valid", "debug|info|warn|error")
	}

	pii, piiOK := parseLogPII(os.Getenv("LOG_PII"), env)
	if !piiOK {
		slog.Warn("LOG_PII invalid; using the environment's default", "value", os.Getenv("LOG_PII"), "valid", "0|1",
			"log_pii", pii)
	}

	otelHandler, otelShutdown, err := otelLogsHandler(context.Background(), env)
	if err != nil {
		slog.Warn("otlp log export disabled; could not configure it", "error", err)
		otelShutdown = func(context.Context) error { return nil }
	}

	slog.SetDefault(slog.New(newHandler(os.Stderr, env, format, level, pii, otelHandler)))

	applied = logSettings{env: env, format: format, level: strings.ToLower(level.String()), pii: pii}

	return otelShutdown
}

// parseLogPII reads LOG_PII: whether logs may carry personal data in full
// (phone numbers, what the caller and agent said, tool arguments). Default:
// yes in development, no anywhere else -- production logs are masked unless
// someone deliberately turns it on.
func parseLogPII(v, env string) (pii, ok bool) {
	switch strings.TrimSpace(v) {
	case "":
		return env == "development", true
	case "1", "true":
		return true, true
	case "0", "false":
		return false, true
	default:
		return env == "development", false
	}
}

// piiKeys are the attributes that can carry personal data: phone numbers
// (masked to their last two digits) and free text (replaced by its length).
var piiKeys = map[string]bool{
	"caller_number": true, "called_number": true, "destination": true,
	"text": true, "rejected_text": true, "args": true, "result": true,
	"reply": true, "variables": true,
}

// redact is a PII attribute's value with LOG_PII=0.
func redact(key string, v slog.Value) slog.Value {
	s := v.String()

	switch key {
	case "caller_number", "called_number", "destination":
		if len(s) <= 2 {
			return slog.StringValue(strings.Repeat("X", len(s)))
		}

		return slog.StringValue(strings.Repeat("X", len(s)-2) + s[len(s)-2:])
	default:
		return slog.StringValue("[redacted " + strconv.Itoa(utf8.RuneCountInString(s)) + " chars]")
	}
}

// logSettings is what ConfigureLogging applied, for the startup banner.
type logSettings struct {
	env, format, level string
	pii                bool
}

// applied is set once by ConfigureLogging, before anything else runs.
var applied = logSettings{env: "development", format: "json", level: "info", pii: true}

// parseLogFormat validates LOG_FORMAT; ok is false when the value should be
// replaced by the default.
func parseLogFormat(v string) (format string, ok bool) {
	switch strings.ToLower(strings.TrimSpace(v)) {
	case "", "json":
		return "json", true
	case "text":
		return "text", true
	default:
		return "json", false
	}
}

// parseLogLevel validates LOG_LEVEL against slog's levels.
func parseLogLevel(v string) (slog.Level, bool) {
	switch strings.ToLower(strings.TrimSpace(v)) {
	case "", "info":
		return slog.LevelInfo, true
	case "debug":
		return slog.LevelDebug, true
	case "warn", "warning":
		return slog.LevelWarn, true
	case "error":
		return slog.LevelError, true
	default:
		return slog.LevelInfo, false
	}
}

// newHandler builds the enriched handler for the requested format and level;
// with pii false, personal data is masked (see piiKeys). otelHandler, if
// non-nil, receives every record too (see ConfigureLogging) -- redaction is
// applied once, centrally in enrichHandler.Handle, rather than relying on
// slog.HandlerOptions.ReplaceAttr (which only base would honour, leaving
// otelHandler an unredacted record).
func newHandler(w io.Writer, env, format string, level slog.Level, pii bool, otelHandler slog.Handler) slog.Handler {
	opts := &slog.HandlerOptions{
		Level:       level,
		ReplaceAttr: replaceAttr,
	}

	var base slog.Handler

	if format == "text" {
		base = slog.NewTextHandler(w, opts)
	} else {
		base = slog.NewJSONHandler(w, opts)
	}

	return &enrichHandler{inner: base, otel: otelHandler, env: env, maskPII: !pii}
}

// replaceAttr rewrites the top-level time (UTC ISO-8601 with milliseconds)
// and level (lowercase) attributes, per the logging spec.
func replaceAttr(groups []string, a slog.Attr) slog.Attr {
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
}

// enrichHandler wraps the JSON handler, prepending what every line carries
// but no call site should have to repeat: the service group, the call's
// trace_id (looked up from the record's call_id), and the component derived
// from the logging call site's package. Explicit attrs at a call site win
// over derived ones.
type enrichHandler struct {
	inner   slog.Handler
	otel    slog.Handler // nil unless OTLP log export is configured; see ConfigureLogging
	env     string
	maskPII bool
	presets []slog.Attr
}

func (e *enrichHandler) Enabled(ctx context.Context, l slog.Level) bool {
	return e.inner.Enabled(ctx, l)
}

func (e *enrichHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	presets := make([]slog.Attr, 0, len(e.presets)+len(attrs))
	presets = append(presets, e.presets...)
	presets = append(presets, attrs...)

	otel := e.otel
	if otel != nil {
		otel = otel.WithAttrs(attrs)
	}

	return &enrichHandler{inner: e.inner.WithAttrs(attrs), otel: otel, env: e.env, maskPII: e.maskPII, presets: presets}
}

func (e *enrichHandler) WithGroup(name string) slog.Handler {
	// No call site uses groups today; if one appears, the injected fields
	// would nest under that group too. Accepted and documented.
	otel := e.otel
	if otel != nil {
		otel = otel.WithGroup(name)
	}

	return &enrichHandler{inner: e.inner.WithGroup(name), otel: otel, env: e.env, maskPII: e.maskPII, presets: e.presets}
}

func (e *enrichHandler) Handle(_ context.Context, r slog.Record) error {
	shared := make([]slog.Attr, 0, len(e.presets)+8)

	callID, hasComponent, hasRunID := scanAttrs(&r)

	if callID != "" {
		traceID, runID := lookupCall(callID)
		if traceID != "" {
			shared = append(shared, slog.String("trace_id", traceID))
		}

		if runID > 0 && !hasRunID {
			shared = append(shared, slog.Int64("run_id", runID))
		}
	}

	if !hasComponent {
		shared = append(shared, slog.String("component", componentForPC(r.PC)))
	}

	for _, a := range e.presets {
		shared = append(shared, e.maskAttr(a))
	}

	r.Attrs(func(a slog.Attr) bool {
		shared = append(shared, e.maskAttr(a))

		return true
	})

	// The stderr line also carries a "service" group: documented, stable
	// shape other tooling may parse. The OTel-bound record skips it -- that
	// information is already on the LoggerProvider's Resource (service.name/
	// version/deployment.environment), so repeating it as a log attribute
	// there would just be a redundant JSON-string blob (SigNoz flattens a
	// group attribute's value, it doesn't keep it as a nested map).
	out := slog.NewRecord(r.Time, r.Level, r.Message, r.PC)
	out.AddAttrs(slog.Group("service",
		slog.String("name", ServiceName),
		slog.String("version", version.Version),
		slog.String("environment", e.env),
	))
	out.AddAttrs(shared...)

	err := e.inner.Handle(context.Background(), out)

	if e.otel != nil {
		otelOut := slog.NewRecord(r.Time, r.Level, r.Message, r.PC)
		otelOut.AddAttrs(shared...)

		if oerr := e.otel.Handle(context.Background(), otelOut); oerr != nil && err == nil {
			err = oerr
		}
	}

	return err
}

// maskAttr redacts a's value if it's a PII-bearing key (see piiKeys) and
// masking is on -- applied once here, centrally, rather than through
// slog.HandlerOptions.ReplaceAttr: that hook only base (stderr) would honour,
// leaving e.otel an unredacted record.
func (e *enrichHandler) maskAttr(a slog.Attr) slog.Attr {
	if e.maskPII && piiKeys[a.Key] {
		a.Value = redact(a.Key, a.Value)
	}

	return a
}

// scanAttrs reads, in one pass, the record's call_id and whether it already
// names its component or run_id (explicit attrs win over derived ones).
func scanAttrs(r *slog.Record) (callID string, hasComponent, hasRunID bool) {
	r.Attrs(func(a slog.Attr) bool {
		switch a.Key {
		case "call_id":
			if a.Value.Kind() == slog.KindString {
				callID = a.Value.String()
			}
		case "component":
			hasComponent = true
		case "run_id":
			hasRunID = true
		}

		return true
	})

	return callID, hasComponent, hasRunID
}

// componentForPC derives the component from the logging call site's
// package: internal/ai/agent -> "agent", internal/ai/agent/tenvad -> "vad",
// internal/ari -> "ari", and so on. "service" covers cmd/ binaries and
// anything outside the internal tree.
func componentForPC(pc uintptr) string {
	if pc == 0 {
		return "service"
	}

	frame, _ := runtime.CallersFrames([]uintptr{pc}).Next()

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
		{"/internal/callagent", "agent"},
		{"/internal/freeswitch", "freeswitch"},
		{"/internal/esl", "freeswitch"},
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
