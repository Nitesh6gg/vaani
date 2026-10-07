package config

import (
	"fmt"
	"io"
	"log/slog"
	"math"
	"net/url"
	"os"
	"runtime"
	"runtime/debug"
	"strings"

	"github.com/nitesh/vaani/internal/version"
)

// startupField is one line of the startup banner and one attribute of the
// service.starting log line.
type startupField struct{ key, value string }

// Startup reports what this process is about to run with: a human banner on
// stderr when it's a terminal (or LOG_FORMAT=text), and always one
// structured service.starting line. Never includes passwords or keys.
func Startup(cfg Config) {
	fields := startupFields(cfg)
	if applied.format == "text" || isTerminal(os.Stderr) {
		writeBanner(os.Stderr, fields)
	}

	args := []any{"event", "service.starting"}
	for _, f := range fields {
		args = append(args, f.key, f.value)
	}
	slog.Info("vaani starting", args...)
}

func isTerminal(f *os.File) bool {
	st, err := f.Stat()
	return err == nil && st.Mode()&os.ModeCharDevice != 0
}

func writeBanner(w io.Writer, fields []startupField) {
	line := strings.Repeat("=", 54)
	fmt.Fprintf(w, "%s\n    %s %s\n%s\n", line, ServiceName, version.Version, line)
	for _, f := range fields {
		fmt.Fprintf(w, "  %-12s %s\n", f.key, f.value)
	}
	fmt.Fprintln(w, line)
}

func startupFields(cfg Config) []startupField {
	host, _ := os.Hostname()
	fields := []startupField{
		{"pid", fmt.Sprint(os.Getpid())},
		{"host", host},
		{"commit", buildCommit()},
		{"go", fmt.Sprintf("%s %s/%s, %d CPUs, GOMEMLIMIT %s",
			runtime.Version(), runtime.GOOS, runtime.GOARCH, runtime.NumCPU(), memLimit())},
		{"environment", applied.env},
		{"mode", fmt.Sprintf("%s (media: %s)", cfg.AppMode, cfg.MediaEncapsulation)},
		{"ari", fmt.Sprintf("%s app=%s", SafeURL(cfg.AriURL), cfg.AriApp)},
	}
	if cfg.AppMode == "agent" {
		fields = append(fields, startupField{"dograh",
			fmt.Sprintf("workflow %d db=%s", cfg.DograhWorkflowID, SafeURL(cfg.DograhDBURL))})
	}
	if cfg.MinioEndpoint != "" {
		fields = append(fields, startupField{"minio",
			fmt.Sprintf("%s bucket=%s", cfg.MinioEndpoint, cfg.MinioBucket)})
	}

	vad := cfg.VadMode
	if vad == "ten" {
		vad += fmt.Sprintf(" (threshold %g)", cfg.TenVadThreshold)
	}
	bargeIn, pii := "off", "masked"
	if cfg.BargeInEnabled {
		bargeIn = "on"
	}
	if applied.pii {
		pii = "full"
	}

	return append(fields,
		startupField{"media", fmt.Sprintf("ip %s ports %d-%d",
			cfg.MediaIP, cfg.MediaPortBase, cfg.MediaPortBase+cfg.MediaPortCount-1)},
		startupField{"vad", fmt.Sprintf("%s barge-in %s", vad, bargeIn)},
		startupField{"logs", fmt.Sprintf("%s level=%s pii=%s", applied.format, applied.level, pii)},
		startupField{"metrics", cfg.MetricsAddr + " (/metrics, /healthz)"},
	)
}

// buildCommit is the VCS revision stamped by `go build` ("dev" under go run).
func buildCommit() string {
	info, ok := debug.ReadBuildInfo()
	if !ok {
		return "dev"
	}
	var rev, at string
	var dirty bool
	for _, s := range info.Settings {
		switch s.Key {
		case "vcs.revision":
			rev = s.Value
		case "vcs.time":
			at = s.Value
		case "vcs.modified":
			dirty = s.Value == "true"
		}
	}
	if rev == "" {
		return "dev"
	}
	if len(rev) > 7 {
		rev = rev[:7]
	}
	if dirty {
		rev += "-dirty"
	}
	if at != "" {
		rev += " (" + at + ")"
	}
	return rev
}

func memLimit() string {
	limit := debug.SetMemoryLimit(-1) // -1 reads without changing it
	if limit == math.MaxInt64 {
		return "not set"
	}
	return fmt.Sprintf("%.1f GiB", float64(limit)/(1<<30))
}

// SafeURL drops the user/password and query (sslmode etc. may carry a
// password too) from a URL; unparseable input is hidden entirely.
func SafeURL(raw string) string {
	if raw == "" {
		return "-"
	}
	u, err := url.Parse(raw)
	if err != nil || u.Host == "" {
		return "(hidden)"
	}
	return u.Scheme + "://" + u.Host + u.Path
}
