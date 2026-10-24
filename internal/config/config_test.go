package config

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLoad_ReadsDotenvFromParentDir(t *testing.T) {
	root := t.TempDir()
	sub := filepath.Join(root, "cmd", "server")
	require.NoError(t, os.MkdirAll(sub, 0o755))

	envContent := "ARI_URL=http://192.168.1.50:8088/ari\nARI_USER=org5\n# a comment\nARI_PASS=\"secret pass\"\n"
	require.NoError(t, os.WriteFile(filepath.Join(root, ".env"), []byte(envContent), 0o644))

	restoreWD := chdir(t, sub)
	defer restoreWD()

	clearEnv(t, "ARI_URL", "ARI_USER", "ARI_PASS")

	cfg, err := Load()
	require.NoError(t, err)

	assert.Equal(t, "http://192.168.1.50:8088/ari", cfg.AriURL)
	assert.Equal(t, "org5", cfg.AriUser)
	assert.Equal(t, "secret pass", cfg.AriPass)
}

func TestLoad_RealEnvWinsOverDotenv(t *testing.T) {
	root := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(root, ".env"), []byte("ARI_URL=http://from-dotenv:8088/ari\n"), 0o644))

	restoreWD := chdir(t, root)
	defer restoreWD()

	t.Setenv("ARI_URL", "http://from-real-env:8088/ari")

	cfg, err := Load()
	require.NoError(t, err)

	assert.Equal(t, "http://from-real-env:8088/ari", cfg.AriURL)
}

func TestLoad_MediaDeadTimeout_DefaultsTo30Seconds(t *testing.T) {
	restoreWD := chdir(t, t.TempDir())
	defer restoreWD()

	cfg, err := Load()
	require.NoError(t, err)

	assert.Equal(t, 30*time.Second, cfg.MediaDeadTimeout)
}

func TestLoad_MediaDeadTimeout_ReadsFromEnv(t *testing.T) {
	restoreWD := chdir(t, t.TempDir())
	defer restoreWD()

	t.Setenv("MEDIA_DEAD_TIMEOUT_SECONDS", "45")

	cfg, err := Load()
	require.NoError(t, err)

	assert.Equal(t, 45*time.Second, cfg.MediaDeadTimeout)
}

func TestLoad_MediaDeadTimeout_ZeroDisablesIt(t *testing.T) {
	restoreWD := chdir(t, t.TempDir())
	defer restoreWD()

	t.Setenv("MEDIA_DEAD_TIMEOUT_SECONDS", "0")

	cfg, err := Load()
	require.NoError(t, err)

	assert.Zero(t, cfg.MediaDeadTimeout)
}

func TestLoad_MediaDeadTimeout_RejectsNegative(t *testing.T) {
	restoreWD := chdir(t, t.TempDir())
	defer restoreWD()

	t.Setenv("MEDIA_DEAD_TIMEOUT_SECONDS", "-5")

	_, err := Load()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "MEDIA_DEAD_TIMEOUT_SECONDS")
}

func TestLoad_BargeIn_Defaults(t *testing.T) {
	restoreWD := chdir(t, t.TempDir())
	defer restoreWD()

	cfg, err := Load()
	require.NoError(t, err)

	assert.True(t, cfg.BargeInEnabled)
	assert.Equal(t, 500.0, cfg.BargeInRMSFloor)
	assert.Equal(t, 300*time.Millisecond, cfg.BargeInGuard)
	assert.Equal(t, 200*time.Millisecond, cfg.BargeInMinSpeech)
	assert.Equal(t, 0.7, cfg.TenVadThreshold)
	assert.Equal(t, 400*time.Millisecond, cfg.STTFlushAfter)
	assert.Equal(t, 300*time.Millisecond, cfg.PostCutSilence)
}

func TestLoad_STTSpeechOnly(t *testing.T) {
	for _, tc := range []struct {
		name    string
		env     map[string]string
		wantErr string // "" = loads fine
	}{
		{"off by default", nil, ""},
		{"on with TEN VAD", map[string]string{"STT_SPEECH_ONLY": "1", "VAD_MODE": "ten"}, ""},
		{"needs TEN VAD", map[string]string{"STT_SPEECH_ONLY": "1"}, "needs VAD_MODE=ten"},
		{"hangover must outlast the flush wait", map[string]string{"STT_SPEECH_ONLY": "1", "VAD_MODE": "ten", "STT_HANGOVER_MS": "400"}, "must exceed STT_FLUSH_AFTER_MS"},
		{"hangover free when flush is off", map[string]string{"STT_SPEECH_ONLY": "1", "VAD_MODE": "ten", "STT_FLUSH_AFTER_MS": "0", "STT_HANGOVER_MS": "100"}, ""},
		{"negative pre-roll", map[string]string{"STT_PREROLL_MS": "-1"}, "STT_PREROLL_MS"},
		{"negative RMS floor", map[string]string{"STT_GATE_RMS_FLOOR": "-1"}, "STT_GATE_RMS_FLOOR"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			restoreWD := chdir(t, t.TempDir())
			defer restoreWD()

			for k, v := range tc.env {
				t.Setenv(k, v)
			}

			cfg, err := Load()
			if tc.wantErr != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tc.wantErr)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tc.env["STT_SPEECH_ONLY"] == "1", cfg.STTSpeechOnly)
		})
	}

	restoreWD := chdir(t, t.TempDir())
	defer restoreWD()

	cfg, err := Load()
	require.NoError(t, err)
	assert.Equal(t, time.Second, cfg.STTPreroll)
	assert.Equal(t, 700*time.Millisecond, cfg.STTHangover)
	assert.Equal(t, 400.0, cfg.STTGateRMSFloor)
}

func TestLoad_BargeIn_ReadsFromEnv(t *testing.T) {
	restoreWD := chdir(t, t.TempDir())
	defer restoreWD()

	t.Setenv("BARGE_IN_ENABLED", "0")
	t.Setenv("BARGE_IN_RMS_FLOOR", "750.5")
	t.Setenv("BARGE_IN_GUARD_MS", "150")
	t.Setenv("BARGE_IN_MIN_SPEECH_MS", "400")
	t.Setenv("POST_CUT_SILENCE_MS", "400")

	cfg, err := Load()
	require.NoError(t, err)

	assert.False(t, cfg.BargeInEnabled)
	assert.Equal(t, 750.5, cfg.BargeInRMSFloor)
	assert.Equal(t, 150*time.Millisecond, cfg.BargeInGuard)
	assert.Equal(t, 400*time.Millisecond, cfg.BargeInMinSpeech)
	assert.Equal(t, 400*time.Millisecond, cfg.PostCutSilence)
}

func TestLoad_BargeIn_RejectsNegativeRMSFloor(t *testing.T) {
	restoreWD := chdir(t, t.TempDir())
	defer restoreWD()

	t.Setenv("BARGE_IN_RMS_FLOOR", "-1")

	_, err := Load()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "BARGE_IN_RMS_FLOOR")
}

func TestLoad_AgentMode_RequiresDograhSettings(t *testing.T) {
	restoreWD := chdir(t, t.TempDir())
	defer restoreWD()

	t.Setenv("APP_MODE", "agent")

	_, err := Load()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "APP_MODE=agent")
}

func TestLoad_AgentMode_SucceedsOnceAllRequiredSettingsAreSet(t *testing.T) {
	restoreWD := chdir(t, t.TempDir())
	defer restoreWD()

	t.Setenv("APP_MODE", "agent")
	t.Setenv("DOGRAH_DB_URL", "postgresql://u:p@localhost/dograh")

	_, err := Load()
	require.ErrorContains(t, err, "DOGRAH_WORKFLOW_ID", "the agent has nothing to run without a workflow")

	t.Setenv("DOGRAH_WORKFLOW_ID", "19")

	cfg, err := Load()
	require.NoError(t, err)
	assert.Equal(t, "agent", cfg.AppMode)
	assert.Equal(t, 19, cfg.DograhWorkflowID)
}

// TestLoad_DograhDBMaxConns: the Dograh database pool defaults to 50 and
// DOGRAH_DB_MAX_CONNS changes it; zero or garbage is refused.
func TestLoad_DograhDBMaxConns(t *testing.T) {
	restoreWD := chdir(t, t.TempDir())
	defer restoreWD()

	t.Setenv("APP_MODE", "agent")
	t.Setenv("DOGRAH_DB_URL", "postgresql://u:p@localhost/dograh")
	t.Setenv("DOGRAH_WORKFLOW_ID", "19")

	cfg, err := Load()
	require.NoError(t, err)
	assert.Equal(t, 50, cfg.DograhDBMaxConns)

	t.Setenv("DOGRAH_DB_MAX_CONNS", "80")

	cfg, err = Load()
	require.NoError(t, err)
	assert.Equal(t, 80, cfg.DograhDBMaxConns)

	for _, bad := range []string{"0", "-3", "many"} {
		t.Setenv("DOGRAH_DB_MAX_CONNS", bad)

		_, err = Load()
		assert.ErrorContains(t, err, "DOGRAH_DB_MAX_CONNS", bad)
	}
}

func chdir(t *testing.T, dir string) (restore func()) {
	t.Helper()

	old, err := os.Getwd()
	require.NoError(t, err)
	require.NoError(t, os.Chdir(dir))

	return func() { _ = os.Chdir(old) }
}

func clearEnv(t *testing.T, keys ...string) {
	t.Helper()

	for _, k := range keys {
		orig, existed := os.LookupEnv(k)

		require.NoError(t, os.Unsetenv(k))

		t.Cleanup(func() {
			if existed {
				_ = os.Setenv(k, orig)
			} else {
				_ = os.Unsetenv(k)
			}
		})
	}
}

// TestNormalizeMinioEndpoint pins the tolerance for both real production
// misconfigurations: pasting MINIO_PUBLIC_ENDPOINT ("https://host/storage")
// and prepending a scheme ("http:host:port"). The uploader adds the scheme
// itself, so the value must reduce to bare host:port -- anything that can't
// be reduced is a startup-time error, not a per-call upload failure.
func TestNormalizeMinioEndpoint(t *testing.T) {
	cases := []struct {
		name    string
		in      string
		want    string
		changed bool
		wantErr bool
	}{
		{"bare host:port unchanged", "192.168.26.130:18900", "192.168.26.130:18900", false, false},
		{"scheme with slashes stripped", "http://192.168.26.130:18900", "192.168.26.130:18900", true, false},
		{"scheme without slashes stripped", "http:192.168.26.130:18900", "192.168.26.130:18900", true, false},
		{"public endpoint value fully reduced", "https://agent.go2market.in/storage", "agent.go2market.in", true, false},
		{"trailing-slash path stripped", "localhost:9000/", "localhost:9000", true, false},
		{"uppercase scheme stripped", "HTTP://h:9000", "h:9000", true, false},
		{"bare hostname accepted without port", "minio.internal", "minio.internal", false, false},
		{"bracketed ipv6 kept", "[::1]:9000", "[::1]:9000", false, false},
		{"scheme-only rejected", "http://", "", false, true},
		{"colon soup rejected", ":::", "", false, true},
		{"spaces rejected", "host :9000", "", false, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, changed, err := normalizeMinioEndpoint(tc.in)

			if tc.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tc.want, got)
			assert.Equal(t, tc.changed, changed)
		})
	}
}

func TestLoad_MinioEndpointNormalized(t *testing.T) {
	restoreWD := chdir(t, t.TempDir())
	defer restoreWD()

	t.Setenv("MINIO_ENDPOINT", "http://h:9000/x")
	t.Setenv("MINIO_ACCESS_KEY", "k")
	t.Setenv("MINIO_SECRET_KEY", "s")

	cfg, err := Load()
	require.NoError(t, err)

	assert.Equal(t, "h:9000", cfg.MinioEndpoint)
}

func TestLoad_Telephony(t *testing.T) {
	for _, tc := range []struct {
		name    string
		env     map[string]string
		wantErr string
	}{
		{"default is asterisk", nil, ""},
		{"unknown", map[string]string{"TELEPHONY": "kamailio"}, "TELEPHONY must be"},
		{"freeswitch needs ESL password", map[string]string{"TELEPHONY": "freeswitch"}, "ESL_PASSWORD"},
		{"freeswitch agent mode", map[string]string{"TELEPHONY": "freeswitch", "ESL_PASSWORD": "x",
			"APP_MODE": "agent", "DOGRAH_DB_URL": "postgresql://u:p@h/d", "DOGRAH_WORKFLOW_ID": "1"}, ""},
		{"token with a space", map[string]string{"TELEPHONY": "freeswitch", "ESL_PASSWORD": "x",
			"EARSHOT_AUTH_TOKEN": "Bearer abc"}, "must not contain spaces"},
		{"freeswitch ok", map[string]string{"TELEPHONY": "freeswitch", "ESL_PASSWORD": "x", "EARSHOT_AUTH_TOKEN": "abc"}, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			restoreWD := chdir(t, t.TempDir())
			defer restoreWD()
			clearEnv(t, "TELEPHONY", "ESL_PASSWORD", "EARSHOT_AUTH_TOKEN", "APP_MODE")
			for k, v := range tc.env {
				t.Setenv(k, v)
			}

			cfg, err := Load()
			if tc.wantErr != "" {
				assert.ErrorContains(t, err, tc.wantErr)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, "127.0.0.1:8021", cfg.EslAddr)
			assert.Equal(t, ":9095", cfg.EarshotListenAddr)
		})
	}
}

func TestLoad_OutboundSettings(t *testing.T) {
	restoreWD := chdir(t, t.TempDir())
	defer restoreWD()

	cfg, err := Load()
	require.NoError(t, err)
	assert.Equal(t, 30*time.Second, cfg.OutboundRingTimeout, "Asterisk's own default")
	assert.Empty(t, cfg.OutboundTestCall)

	t.Setenv("OUTBOUND_RING_TIMEOUT_SECONDS", "45")
	t.Setenv("OUTBOUND_TEST_CALL", "9876543210,1,19")

	cfg, err = Load()
	require.NoError(t, err)
	assert.Equal(t, 45*time.Second, cfg.OutboundRingTimeout)
	assert.Equal(t, "9876543210,1,19", cfg.OutboundTestCall)

	for _, bad := range []string{"4", "301", "0", "-1"} {
		t.Setenv("OUTBOUND_RING_TIMEOUT_SECONDS", bad)

		_, err = Load()
		assert.ErrorContains(t, err, "OUTBOUND_RING_TIMEOUT_SECONDS", bad)
	}
}
