// Package config loads Vaani's environment-based configuration.
package config

import (
	"errors"
	"fmt"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

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

	// MediaEncapsulation selects the externalMedia transport: "rtp" (default,
	// UDP) or "audiosocket" (TCP, Asterisk's res_audiosocket protocol -- see
	// internal/media/audiosocket.go). AudioSocket carries no RTP sequencing/SSRC
	// at all (TCP already guarantees in-order, lossless delivery), and its audio
	// payload is little-endian by protocol definition, not a per-deployment
	// question -- AUDIO_L16_ENDIANNESS and JITTER_BUFFER_PACKETS are meaningless
	// for it and are ignored when this is "audiosocket".
	MediaEncapsulation string

	// DebugAudio, when true (DEBUG_AUDIO=1), enables the live /debug/audio/{callID}
	// tap on MetricsAddr for real-time listening to a call's inbound audio
	// (post-normalization) without waiting for RecordDir's WAV file to close.
	// Never set this in production -- it lets anyone who can reach METRICS_ADDR
	// listen to live call audio. Leave unset/0 normally.
	DebugAudio bool

	// MaxCallDuration bounds how long a single call may live before Vaani hangs
	// it up, however healthy it looks -- a backstop against a wedged call (a
	// Handler that never yields, media that never stops) burning billable
	// telephony time forever. 0 (default) disables the cap; set
	// MAX_CALL_DURATION_SECONDS to enforce one.
	MaxCallDuration time.Duration

	// MediaDeadTimeout bounds how long a call's inbound media may stay
	// completely silent before Vaani proactively hangs it up -- a second layer
	// alongside Asterisk's own rtptimeout (deploy/asterisk/rtp.conf), which only
	// watches the caller's SIP-side RTP and can't catch a call whose audio never
	// reaches this externalMedia leg due to an Asterisk-internal bridging
	// problem. On by default at 30s, since zero inbound audio for that long has
	// no legitimate case worth preserving (unlike MaxCallDuration, which could
	// cut off a real long conversation); set MEDIA_DEAD_TIMEOUT_SECONDS=0 to
	// disable. Checked on media.WatchdogTimeout's 5s ticks, so values below that
	// just round up to one tick.
	MediaDeadTimeout time.Duration

	// DograhDBURL is the Postgres connection string of a self-hosted Dograh
	// deployment (DOGRAH_DB_URL), and DograhWorkflowID the Dograh workflow
	// (workflows.id) the agent runs (DOGRAH_WORKFLOW_ID): its prompts,
	// greeting, nodes, edges, tools, and its LLM/STT/TTS models and keys.
	// Both required when AppMode is "agent".
	DograhDBURL      string
	DograhWorkflowID int

	// Dograh's MinIO, where each call's recording and transcript are
	// uploaded for its call history -- the same MINIO_* settings (and
	// defaults) as Dograh's own environment. MinioEndpoint empty: no uploads
	// (the call is still recorded in workflow_runs).
	MinioEndpoint  string // host:port
	MinioAccessKey string
	MinioSecretKey string
	MinioBucket    string
	MinioSecure    bool

	// BargeInEnabled toggles caller-interrupts-agent detection (internal/ai/agent).
	// When false, internal/ari wires in a no-op detector instead of EnergyDetector
	// so the agent talks over any inbound audio until it finishes its turn --
	// useful for isolating whether a problem is in barge-in detection itself.
	BargeInEnabled bool
	// BargeInRMSFloor is EnergyDetector's speech threshold (media.RMS units, on
	// the int16 PCM scale). 500 is an untuned starting guess, not a verified
	// number -- like AUDIO_L16_ENDIANNESS, calibrate it against real call audio
	// before trusting it in production.
	BargeInRMSFloor float64
	// BargeInGuard ignores barge-in detection for this long after the agent
	// starts speaking, so its own voice leaking into the mic can't immediately
	// self-trigger. See internal/ai/agent.Config.BargeInGuard.
	BargeInGuard time.Duration
	// BargeInMinSpeech is how long the caller must keep speaking, without a
	// break, before it counts as an interruption (BARGE_IN_MIN_SPEECH_MS,
	// default 200 -- Dograh's live setting, pipecat's VAD start_secs 0.2).
	// See internal/ai/agent.Config.BargeInMinSpeech.
	BargeInMinSpeech time.Duration
	// STTFlushAfter (STT_FLUSH_AFTER_MS, default 400; 0 = off): once TEN VAD
	// has heard this much quiet after the caller spoke, ask Sarvam to
	// finalize the transcript now instead of waiting for its own
	// end-of-speech (~600-700ms). Confirmed live at 400 (2026-10-03/05):
	// replies ~0.2-0.3s sooner. Only with VAD_MODE=ten. See
	// internal/ai/agent.Config.STTFlushAfter.
	STTFlushAfter time.Duration
	// PostCutSilence is how long after an interruption pauses the reply
	// transcripts are ignored. See internal/ai/agent.Config.PostCutSilence.
	PostCutSilence time.Duration
	// VadMode selects the barge-in detector when BargeInEnabled: "energy"
	// (default -- the RMS-threshold EnergyDetector, no native dependency) or
	// "ten" (TEN VAD, a real neural VAD -- Linux/cgo only, vendored under
	// third_party/ten-vad). VAD_MODE=ten on a platform without the native
	// library falls back to the energy detector with a loud warning at call
	// setup, never breaks the call.
	VadMode string
	// TenVadThreshold is TEN VAD's speech-probability threshold in [0,1];
	// the native binary decision is probability >= threshold. Default 0.7,
	// the confidence Dograh's VAD runs with (pipecat's VADParams default);
	// TEN VAD's own example uses 0.5. Only meaningful when VadMode is "ten".
	TenVadThreshold float64
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
		MediaEncapsulation:  getEnv("MEDIA_ENCAPSULATION", "rtp"),
		DebugAudio:          getEnv("DEBUG_AUDIO", "") == "1",
		DograhDBURL:         getEnv("DOGRAH_DB_URL", ""),
		MinioEndpoint:       getEnv("MINIO_ENDPOINT", ""),
		MinioAccessKey:      getEnv("MINIO_ACCESS_KEY", ""),
		MinioSecretKey:      getEnv("MINIO_SECRET_KEY", ""),
		MinioBucket:         getEnv("MINIO_BUCKET", "voice-audio"),
		MinioSecure:         strings.EqualFold(getEnv("MINIO_SECURE", "false"), "true"),
		BargeInEnabled:      getEnv("BARGE_IN_ENABLED", "1") == "1",
		VadMode:             getEnv("VAD_MODE", "energy"),
	}

	var err error

	if cfg.MediaPortBase, err = getEnvInt("MEDIA_PORT_BASE", cfg.MediaPortBase); err != nil {
		return Config{}, err
	}

	if cfg.MediaPortCount, err = getEnvInt("MEDIA_PORT_COUNT", cfg.MediaPortCount); err != nil {
		return Config{}, err
	}

	if cfg.DograhWorkflowID, err = getEnvInt("DOGRAH_WORKFLOW_ID", 0); err != nil {
		return Config{}, err
	}

	if cfg.MediaPortCount <= 0 {
		return Config{}, fmt.Errorf("config: MEDIA_PORT_COUNT must be positive, got %d", cfg.MediaPortCount)
	}

	// Catch an impossible port range at startup instead of as per-call bind
	// failures mid-operation.
	if cfg.MediaPortBase < 1024 {
		return Config{}, fmt.Errorf("config: MEDIA_PORT_BASE must be >= 1024, got %d", cfg.MediaPortBase)
	}

	if cfg.MediaPortBase+cfg.MediaPortCount > 65535 {
		return Config{}, fmt.Errorf("config: MEDIA_PORT_BASE + MEDIA_PORT_COUNT must be <= 65535, got %d + %d",
			cfg.MediaPortBase, cfg.MediaPortCount)
	}

	if cfg.JitterBufferPackets, err = getEnvInt("JITTER_BUFFER_PACKETS", cfg.JitterBufferPackets); err != nil {
		return Config{}, err
	}

	if cfg.JitterBufferPackets < 1 || cfg.JitterBufferPackets > 10 {
		return Config{}, fmt.Errorf("config: JITTER_BUFFER_PACKETS must be 1-10, got %d", cfg.JitterBufferPackets)
	}

	var maxCallSeconds int

	if maxCallSeconds, err = getEnvInt("MAX_CALL_DURATION_SECONDS", 0); err != nil {
		return Config{}, err
	}

	if maxCallSeconds < 0 {
		return Config{}, fmt.Errorf("config: MAX_CALL_DURATION_SECONDS must be >= 0 (0 disables the cap), got %d", maxCallSeconds)
	}

	cfg.MaxCallDuration = time.Duration(maxCallSeconds) * time.Second

	mediaDeadSeconds, err := getEnvInt("MEDIA_DEAD_TIMEOUT_SECONDS", 30)
	if err != nil {
		return Config{}, err
	}

	if mediaDeadSeconds < 0 {
		return Config{}, fmt.Errorf("config: MEDIA_DEAD_TIMEOUT_SECONDS must be >= 0 (0 disables it), got %d", mediaDeadSeconds)
	}

	cfg.MediaDeadTimeout = time.Duration(mediaDeadSeconds) * time.Second

	if _, err := media.ParseEndianness(cfg.AudioL16Endianness); err != nil {
		return Config{}, err
	}

	if cfg.AppMode != "loopback" && cfg.AppMode != "agent" {
		return Config{}, fmt.Errorf("config: APP_MODE must be \"loopback\" or \"agent\", got %q", cfg.AppMode)
	}

	if cfg.MediaEncapsulation != "rtp" && cfg.MediaEncapsulation != "audiosocket" {
		return Config{}, fmt.Errorf("config: MEDIA_ENCAPSULATION must be \"rtp\" or \"audiosocket\", got %q", cfg.MediaEncapsulation)
	}

	if cfg.BargeInRMSFloor, err = getEnvFloat("BARGE_IN_RMS_FLOOR", 500); err != nil {
		return Config{}, err
	}

	if cfg.BargeInRMSFloor < 0 {
		return Config{}, fmt.Errorf("config: BARGE_IN_RMS_FLOOR must be >= 0, got %v", cfg.BargeInRMSFloor)
	}

	if cfg.VadMode != "energy" && cfg.VadMode != "ten" {
		return Config{}, fmt.Errorf("config: VAD_MODE must be \"energy\" or \"ten\", got %q", cfg.VadMode)
	}

	if cfg.TenVadThreshold, err = getEnvFloat("TEN_VAD_THRESHOLD", 0.7); err != nil {
		return Config{}, err
	}

	if cfg.TenVadThreshold < 0 || cfg.TenVadThreshold > 1 {
		return Config{}, fmt.Errorf("config: TEN_VAD_THRESHOLD must be in [0,1], got %v", cfg.TenVadThreshold)
	}

	bargeInGuardMS, err := getEnvInt("BARGE_IN_GUARD_MS", 300)
	if err != nil {
		return Config{}, err
	}

	if bargeInGuardMS < 0 {
		return Config{}, fmt.Errorf("config: BARGE_IN_GUARD_MS must be >= 0, got %d", bargeInGuardMS)
	}

	cfg.BargeInGuard = time.Duration(bargeInGuardMS) * time.Millisecond

	bargeInMinSpeechMS, err := getEnvInt("BARGE_IN_MIN_SPEECH_MS", 200)
	if err != nil {
		return Config{}, err
	}

	if bargeInMinSpeechMS < 0 {
		return Config{}, fmt.Errorf("config: BARGE_IN_MIN_SPEECH_MS must be >= 0, got %d", bargeInMinSpeechMS)
	}

	cfg.BargeInMinSpeech = time.Duration(bargeInMinSpeechMS) * time.Millisecond

	sttFlushAfterMS, err := getEnvInt("STT_FLUSH_AFTER_MS", 400)
	if err != nil {
		return Config{}, err
	}

	if sttFlushAfterMS < 0 {
		return Config{}, fmt.Errorf("config: STT_FLUSH_AFTER_MS must be >= 0 (0 = off), got %d", sttFlushAfterMS)
	}

	cfg.STTFlushAfter = time.Duration(sttFlushAfterMS) * time.Millisecond

	// 300ms: transcripts this soon after an interruption pauses the reply
	// were already on their way (Sarvam needs ~0.7s after the caller stops
	// just to send one) and are ignored. It used to be 1200ms, to drop the
	// junk a cut's preroll flush produced 0.5-1.5s later (1-2 character
	// fragments in random scripts, under STT language "unknown"); now that
	// an interruption only pauses, 1200ms was observed live (2026-10-01)
	// throwing away the caller's real short answers, so junk is caught by
	// content instead (agent.junkTranscript). Overridable via
	// POST_CUT_SILENCE_MS.
	postCutSilenceMS, err := getEnvInt("POST_CUT_SILENCE_MS", 300)
	if err != nil {
		return Config{}, err
	}

	if postCutSilenceMS < 0 {
		return Config{}, fmt.Errorf("config: POST_CUT_SILENCE_MS must be >= 0, got %d", postCutSilenceMS)
	}

	cfg.PostCutSilence = time.Duration(postCutSilenceMS) * time.Millisecond

	// Fail fast at startup rather than at first call. Everything else the
	// agent needs (models, keys, prompts) comes from Dograh per call.
	if cfg.AppMode == "agent" {
		if cfg.DograhDBURL == "" {
			return Config{}, fmt.Errorf("config: DOGRAH_DB_URL must be set when APP_MODE=agent")
		}

		if cfg.DograhWorkflowID <= 0 {
			return Config{}, fmt.Errorf("config: DOGRAH_WORKFLOW_ID must be set when APP_MODE=agent (the Dograh workflow the agent runs)")
		}

		if cfg.MinioEndpoint == "" {
			slog.Warn("MINIO_ENDPOINT not set; call recordings and transcripts won't be uploaded to Dograh's MinIO")
		}
	}

	if cfg.MinioEndpoint != "" {
		normalized, changed, err := normalizeMinioEndpoint(cfg.MinioEndpoint)
		if err != nil {
			return Config{}, err
		}

		if changed {
			slog.Warn("MINIO_ENDPOINT normalized; the uploader adds the scheme itself, so the value must be bare host:port",
				"raw", cfg.MinioEndpoint, "endpoint", normalized)
		}

		cfg.MinioEndpoint = normalized
	}

	if cfg.MinioEndpoint != "" && (cfg.MinioAccessKey == "" || cfg.MinioSecretKey == "") {
		return Config{}, fmt.Errorf("config: MINIO_ACCESS_KEY and MINIO_SECRET_KEY must be set with MINIO_ENDPOINT")
	}

	// getEnv treats an explicitly-empty env var as unset, so these dev defaults
	// silently apply when ARI_PASS/MEDIA_IP are missing. That's fine for local
	// dev but exactly wrong for production, so say so loudly at startup.
	if _, ok := os.LookupEnv("ARI_PASS"); !ok {
		slog.Warn("ARI_PASS not set; using dev default credentials -- set ARI_PASS for anything beyond local dev")
	}

	if _, ok := os.LookupEnv("MEDIA_IP"); !ok {
		slog.Warn("MEDIA_IP not set; defaulting to 127.0.0.1 -- only works when Asterisk and Vaani share a host/network namespace", "event", "config.media_ip_fallback")
	}

	return cfg, nil
}

// normalizeMinioEndpoint reduces a MINIO_ENDPOINT value to the bare
// "host:port" (or bare host) the uploader builds URLs around. Both real
// misconfigurations observed in production are tolerated here rather than
// producing a malformed per-call upload URL: pasting the public
// MINIO_PUBLIC_ENDPOINT value ("https://host/storage") and prepending a
// scheme "for clarity" ("http:host:port"). changed reports whether anything
// was stripped (the caller warns); an error means the remainder is
// structurally unusable and Load must fail fast instead of failing every
// call's upload.
func normalizeMinioEndpoint(raw string) (endpoint string, changed bool, err error) {
	v := strings.TrimSpace(raw)

	// Order matters: "http://" before "http:", or the shorter prefix would
	// leave "//..." behind.
	for _, p := range []string{"http://", "https://", "http:", "https:"} {
		if len(v) >= len(p) && strings.EqualFold(v[:len(p)], p) {
			v = v[len(p):]
			changed = true

			break
		}
	}

	if i := strings.IndexByte(v, '/'); i >= 0 {
		v = v[:i]
		changed = true
	}

	v = strings.TrimSpace(v)

	if v == "" {
		return "", changed, fmt.Errorf("config: MINIO_ENDPOINT must be a bare host:port (e.g. 192.168.26.130:18900), got %q", raw)
	}

	if strings.ContainsAny(v, " \t") {
		return "", changed, fmt.Errorf("config: MINIO_ENDPOINT must be a bare host:port (e.g. 192.168.26.130:18900), got %q", raw)
	}

	if _, _, serr := net.SplitHostPort(v); serr != nil {
		// A bare hostname without a port is fine -- it just produces a
		// port-80 URL. Anything else SplitHostPort rejects (colon soup,
		// unbracketed IPv6) is a real config error.
		var addrErr *net.AddrError

		if !errors.As(serr, &addrErr) || addrErr.Err != "missing port in address" {
			return "", changed, fmt.Errorf("config: MINIO_ENDPOINT must be a bare host:port (e.g. 192.168.26.130:18900), got %q", raw)
		}
	}

	return v, changed, nil
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

func getEnvFloat(key string, def float64) (float64, error) {
	v, ok := os.LookupEnv(key)
	if !ok || v == "" {
		return def, nil
	}

	f, err := strconv.ParseFloat(v, 64)
	if err != nil {
		return 0, fmt.Errorf("config: invalid %s=%q: %w", key, v, err)
	}

	return f, nil
}
