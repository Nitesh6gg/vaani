package config

import (
	"log/slog"
	"os"
)

// LogTimeFormat is every log line's timestamp: UTC, millisecond precision
// ("2026-10-06 05:54:14.651").
const LogTimeFormat = "2006-01-02 15:04:05.000"

// ConfigureLogging makes the process's default slog logger write slog's
// key=value text format to stderr with LogTimeFormat timestamps, instead of
// log/slog's built-in default (local server time, whole seconds, a
// "2006/01/02" date). Each cmd/ binary calls it first, so even startup
// warnings (config.Load's own) use it.
func ConfigureLogging() {
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{ReplaceAttr: utcTime})))
}

// utcTime rewrites the record's time attribute as LogTimeFormat in UTC.
func utcTime(groups []string, a slog.Attr) slog.Attr {
	if len(groups) == 0 && a.Key == slog.TimeKey && a.Value.Kind() == slog.KindTime {
		a.Value = slog.StringValue(a.Value.Time().UTC().Format(LogTimeFormat))
	}

	return a
}
