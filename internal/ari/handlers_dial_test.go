package ari

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDialWithRetry_SucceedsFirstAttempt(t *testing.T) {
	calls := 0

	start := time.Now()
	v, err := dialWithRetry(context.Background(), "call1", "test connect", func() (int, error) {
		calls++
		return 42, nil
	})

	require.NoError(t, err)
	assert.Equal(t, 42, v)
	assert.Equal(t, 1, calls)
	assert.Less(t, time.Since(start), 100*time.Millisecond, "must not sleep when the first attempt succeeds")
}

func TestDialWithRetry_SucceedsAfterFailures(t *testing.T) {
	calls := 0

	v, err := dialWithRetry(context.Background(), "call1", "test connect", func() (int, error) {
		calls++
		if calls < 3 {
			return 0, errors.New("boom")
		}
		return 7, nil
	})

	require.NoError(t, err)
	assert.Equal(t, 7, v)
	assert.Equal(t, connectRetryAttempts, calls)
}

func TestDialWithRetry_ExhaustsAndReturnsWrappedError(t *testing.T) {
	calls := 0

	_, err := dialWithRetry(context.Background(), "call1", "test connect", func() (int, error) {
		calls++
		return 0, errors.New("always fails")
	})

	require.Error(t, err)
	assert.Equal(t, connectRetryAttempts, calls, "must stop at the retry budget, not loop forever")
	assert.Contains(t, err.Error(), "test connect")
	assert.Contains(t, err.Error(), "always fails")
}

func TestDialWithRetry_CtxCancelDuringBackoffReturnsPromptly(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	calls := 0

	start := time.Now()
	_, err := dialWithRetry(ctx, "call1", "test connect", func() (int, error) {
		calls++
		if calls == 1 {
			cancel() // cancel while dialWithRetry is about to back off
		}
		return 0, errors.New("boom")
	})

	require.Error(t, err)
	assert.ErrorIs(t, err, context.Canceled)
	assert.Equal(t, 1, calls, "must not attempt again once ctx is done")
	assert.Less(t, time.Since(start), connectRetryBaseDelay, "ctx cancellation must cut the backoff short, not wait it out")
}
