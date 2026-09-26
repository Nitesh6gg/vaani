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
	assert.Equal(t, 300*time.Millisecond, cfg.PostCutSilence)
}

func TestLoad_BargeIn_ReadsFromEnv(t *testing.T) {
	restoreWD := chdir(t, t.TempDir())
	defer restoreWD()

	t.Setenv("BARGE_IN_ENABLED", "0")
	t.Setenv("BARGE_IN_RMS_FLOOR", "750.5")
	t.Setenv("BARGE_IN_GUARD_MS", "150")
	t.Setenv("POST_CUT_SILENCE_MS", "400")

	cfg, err := Load()
	require.NoError(t, err)

	assert.False(t, cfg.BargeInEnabled)
	assert.Equal(t, 750.5, cfg.BargeInRMSFloor)
	assert.Equal(t, 150*time.Millisecond, cfg.BargeInGuard)
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

func TestLoad_AgentMode_RequiresLLMAndSarvamSettings(t *testing.T) {
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
	t.Setenv("LLM_BASE_URL", "http://localhost:11434/v1")
	t.Setenv("LLM_MODEL", "some-model")
	t.Setenv("SARVAM_API_KEY", "key")
	t.Setenv("SARVAM_TTS_VOICE", "shubh")

	cfg, err := Load()
	require.NoError(t, err)
	assert.Equal(t, "agent", cfg.AppMode)
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
