// Package config loads Vaani's environment-based configuration.
package config

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/nitesh/vaani/internal/media"
)

// Config holds all environment-derived settings for the server.
type Config struct {
	// AriURL is the ARI REST base, e.g. http://localhost:8088/ari. The ARI
	// WebSocket events URL is derived from it (see WebsocketURL).
	AriURL  string
	AriUser string
	AriPass string
	// AriApp is the Stasis application name Asterisk routes calls into.
	AriApp string

	// MediaIP is the address advertised to Asterisk as external_host for
	// externalMedia RTP. It is required and never auto-detected: on a multi-NIC
	// host, guessing the "right" local IP is ambiguous, and a wrong guess means
	// Asterisk sends RTP nowhere reachable. In deploy/docker-compose.yml, vaani
	// is given a static IP on a dedicated bridge network and MEDIA_IP is set to
	// that same address; on a real deployment (or when Go and Asterisk run on
	// different hosts) it must be set to the actual routable address.
	MediaIP        string
	MediaPortBase  int
	MediaPortCount int

	// MetricsAddr is the listen address for the Prometheus /metrics endpoint.
	MetricsAddr string

	// AudioL16Endianness is "le" or "be": the wire byte order of inbound RTP L16
	// payloads, from the cmd/endianness-check finding recorded in
	// docs/AUDIO_PIPELINE.md. Everything past the jitter buffer is normalized to LE.
	AudioL16Endianness string

	// JitterBufferPackets is the fixed reorder window size, in 20ms packets
	// (default 3 = 60ms). Range 1-10.
	JitterBufferPackets int

	// RecordDir, if set, enables per-call debug WAV recording (both directions,
	// post-normalization) under this directory. Empty disables recording.
	RecordDir string

	// AppMode selects the per-call Handler: "loopback" (default, Phase 1 behavior)
	// or "agent" (Phase 3).
	AppMode string

	// TestSilentHandler, when true (TEST_SILENT_HANDLER=1), forces every call to
	// use media.SilentHandler regardless of AppMode -- a debug hook for
	// deterministically exercising the write tick's silence-fallback path. Never
	// set this in production; it means calls carry no audio at all.
	TestSilentHandler bool
}

// Load reads configuration from the environment, applying local-dev defaults that
// match deploy/asterisk/ari.conf and deploy/docker-compose.yml. A .env file (see
// .env.example), if found in the working directory or one of its parents, fills in
// anything not already set in the real environment -- real env vars always win.
func Load() (Config, error) {
	loadDotenv()

	cfg := Config{
		AriURL:              getEnv("ARI_URL", "http://localhost:8088/ari"),
		AriUser:             getEnv("ARI_USER", "vaani"),
		AriPass:             getEnv("ARI_PASS", "vaani"),
		AriApp:              getEnv("ARI_APP", "vaani"),
		MediaIP:             getEnv("MEDIA_IP", "127.0.0.1"),
		MediaPortBase:       20000,
		MediaPortCount:      256,
		MetricsAddr:         getEnv("METRICS_ADDR", ":9091"),
		AudioL16Endianness:  getEnv("AUDIO_L16_ENDIANNESS", "le"),
		JitterBufferPackets: 3,
		RecordDir:           getEnv("RECORD_DIR", ""),
		AppMode:             getEnv("APP_MODE", "loopback"),
		TestSilentHandler:   getEnv("TEST_SILENT_HANDLER", "") == "1",
	}

	var err error

	if cfg.MediaPortBase, err = getEnvInt("MEDIA_PORT_BASE", cfg.MediaPortBase); err != nil {
		return Config{}, err
	}

	if cfg.MediaPortCount, err = getEnvInt("MEDIA_PORT_COUNT", cfg.MediaPortCount); err != nil {
		return Config{}, err
	}

	if cfg.MediaPortCount <= 0 {
		return Config{}, fmt.Errorf("config: MEDIA_PORT_COUNT must be positive, got %d", cfg.MediaPortCount)
	}

	if cfg.JitterBufferPackets, err = getEnvInt("JITTER_BUFFER_PACKETS", cfg.JitterBufferPackets); err != nil {
		return Config{}, err
	}

	if cfg.JitterBufferPackets < 1 || cfg.JitterBufferPackets > 10 {
		return Config{}, fmt.Errorf("config: JITTER_BUFFER_PACKETS must be 1-10, got %d", cfg.JitterBufferPackets)
	}

	if _, err := media.ParseEndianness(cfg.AudioL16Endianness); err != nil {
		return Config{}, err
	}

	if cfg.AppMode != "loopback" && cfg.AppMode != "agent" {
		return Config{}, fmt.Errorf("config: APP_MODE must be \"loopback\" or \"agent\", got %q", cfg.AppMode)
	}

	return cfg, nil
}

// loadDotenv searches the working directory and its parents (up to the filesystem
// root) for a .env file and applies its variables, skipping any key already set in
// the real environment. It's a silent no-op if no .env file is found anywhere on
// that path -- .env is a local-dev convenience, not a requirement.
func loadDotenv() {
	dir, err := os.Getwd()
	if err != nil {
		return
	}

	for {
		data, err := os.ReadFile(filepath.Join(dir, ".env"))
		if err == nil {
			applyDotenv(data)
			return
		}

		parent := filepath.Dir(dir)
		if parent == dir {
			return
		}

		dir = parent
	}
}

func applyDotenv(data []byte) {
	for _, line := range strings.Split(string(data), "\n") {
		line = strings.TrimSpace(line)
		line = strings.TrimPrefix(line, "export ")

		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}

		key, value, ok := strings.Cut(line, "=")
		if !ok {
			continue
		}

		key = strings.TrimSpace(key)
		value = unquote(strings.TrimSpace(value))

		if _, exists := os.LookupEnv(key); !exists {
			_ = os.Setenv(key, value)
		}
	}
}

func unquote(v string) string {
	if len(v) >= 2 {
		if (v[0] == '"' && v[len(v)-1] == '"') || (v[0] == '\'' && v[len(v)-1] == '\'') {
			return v[1 : len(v)-1]
		}
	}

	return v
}

func getEnv(key, def string) string {
	if v, ok := os.LookupEnv(key); ok && v != "" {
		return v
	}

	return def
}

func getEnvInt(key string, def int) (int, error) {
	v, ok := os.LookupEnv(key)
	if !ok || v == "" {
		return def, nil
	}

	n, err := strconv.Atoi(v)
	if err != nil {
		return 0, fmt.Errorf("config: invalid %s=%q: %w", key, v, err)
	}

	return n, nil
}
