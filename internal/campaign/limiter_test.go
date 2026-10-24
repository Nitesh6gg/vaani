package campaign

import (
	"context"
	"regexp"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/nitesh/vaani/internal/dograh"
)

type limiterRig struct {
	l   *Limiter
	rdb *redis.Client
	now time.Time
}

func newRig(t *testing.T, maxCalls int) *limiterRig {
	t.Helper()

	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })

	r := &limiterRig{rdb: rdb, now: time.Unix(1_800_000_000, 0)}
	r.l = NewLimiter(rdb, maxCalls)
	r.l.now = func() time.Time { return r.now }

	return r
}

func (r *limiterRig) advance(d time.Duration) { r.now = r.now.Add(d) }

func TestLimiter_RateWindow(t *testing.T) {
	ctx := context.Background()
	r := newRig(t, 0)

	for i := 0; i < 3; i++ {
		ok, err := r.l.AcquireToken(ctx, 7, 3)
		require.NoError(t, err)
		require.True(t, ok, "start %d fits in the window", i+1)

		r.advance(100 * time.Millisecond)
	}

	ok, err := r.l.AcquireToken(ctx, 7, 3)
	require.NoError(t, err)
	assert.False(t, ok, "a 4th start inside the same second is refused")

	wait, err := r.l.NextTokenWait(ctx, 7)
	require.NoError(t, err)
	assert.InDelta(t, 0.7, wait.Seconds(), 0.01, "the oldest start leaves the window 0.7s from now")

	r.advance(wait + time.Millisecond)

	ok, err = r.l.AcquireToken(ctx, 7, 3)
	require.NoError(t, err)
	assert.True(t, ok)

	other, err := r.l.AcquireToken(ctx, 8, 1)
	require.NoError(t, err)
	assert.True(t, other, "windows are per organization")
}

func TestLimiter_SlotsHoldTheOrgLimit(t *testing.T) {
	ctx := context.Background()
	r := newRig(t, 0)

	a, ok, err := r.l.AcquireSlot(ctx, 7, 2)
	require.NoError(t, err)
	require.True(t, ok)

	_, ok, err = r.l.AcquireSlot(ctx, 7, 2)
	require.NoError(t, err)
	require.True(t, ok)

	_, ok, err = r.l.AcquireSlot(ctx, 7, 2)
	require.NoError(t, err)
	assert.False(t, ok, "the third call is over the org limit")

	_, ok, err = r.l.AcquireSlot(ctx, 8, 2)
	require.NoError(t, err)
	assert.True(t, ok, "another org has its own limit")

	require.NoError(t, r.l.ReleaseSlot(ctx, a))
	require.NoError(t, r.l.ReleaseSlot(ctx, a), "releasing twice is harmless")

	n, err := r.l.ActiveSlots(ctx, 7)
	require.NoError(t, err)
	assert.Equal(t, 1, n)

	_, ok, err = r.l.AcquireSlot(ctx, 7, 2)
	require.NoError(t, err)
	assert.True(t, ok, "a released slot is reusable")
}

func TestLimiter_StaleSlotsExpire(t *testing.T) {
	ctx := context.Background()
	r := newRig(t, 0)

	_, ok, err := r.l.AcquireSlot(ctx, 7, 1)
	require.NoError(t, err)
	require.True(t, ok, "the call whose process then crashes")

	r.advance(staleSlotAfter - time.Minute)

	_, ok, err = r.l.AcquireSlot(ctx, 7, 1)
	require.NoError(t, err)
	assert.False(t, ok, "still counted before the timeout")

	r.advance(2 * time.Minute)

	_, ok, err = r.l.AcquireSlot(ctx, 7, 1)
	require.NoError(t, err)
	assert.True(t, ok, "a slot nobody released frees itself after 20 minutes")
}

func TestLimiter_OwnCapAcrossOrgs(t *testing.T) {
	ctx := context.Background()
	r := newRig(t, 2)

	a, ok, _ := r.l.AcquireSlot(ctx, 1, 10)
	require.True(t, ok)

	_, ok, _ = r.l.AcquireSlot(ctx, 2, 10)
	require.True(t, ok)

	_, ok, err := r.l.AcquireSlot(ctx, 3, 10)
	require.NoError(t, err)
	assert.False(t, ok, "Vaani's own cap of 2 is full")

	n, _ := r.l.ActiveSlots(ctx, 3)
	assert.Zero(t, n, "a refused call holds no org slot")

	require.NoError(t, r.l.ReleaseSlot(ctx, a))

	_, ok, _ = r.l.AcquireSlot(ctx, 3, 10)
	assert.True(t, ok, "releasing frees capacity")
}

func TestLimiter_OrgRefusalGivesBackCapacity(t *testing.T) {
	ctx := context.Background()
	r := newRig(t, 5)

	_, ok, _ := r.l.AcquireSlot(ctx, 1, 1)
	require.True(t, ok)

	_, ok, _ = r.l.AcquireSlot(ctx, 1, 1)
	require.False(t, ok, "over the org limit")

	n, err := r.rdb.ZCard(ctx, capacityKey).Result()
	require.NoError(t, err)
	assert.EqualValues(t, 1, n, "the refused attempt did not keep a capacity slot")
}

// The keys and member formats are Dograh's: what Dograh wrote is seen here,
// and what is written here looks the way Dograh's code expects.
func TestLimiter_SharesKeysWithDograh(t *testing.T) {
	ctx := context.Background()
	r := newRig(t, 0)

	// Dograh's try_acquire_concurrent_slot: ZADD concurrent_calls:<org> <now> <ms>_<8 hex>.
	require.NoError(t, r.rdb.ZAdd(ctx, "concurrent_calls:7",
		redis.Z{Score: unixSeconds(r.now), Member: "1800000000000_deadbeef"}).Err())

	_, ok, err := r.l.AcquireSlot(ctx, 7, 1)
	require.NoError(t, err)
	assert.False(t, ok, "a call Dograh is running counts against the same limit")

	slot, ok, err := r.l.AcquireSlot(ctx, 7, 2)
	require.NoError(t, err)
	require.True(t, ok)

	assert.Regexp(t, regexp.MustCompile(`^\d{13}_[0-9a-f]{8}$`), slot.ID)

	score, err := r.rdb.ZScore(ctx, "concurrent_calls:7", slot.ID).Result()
	require.NoError(t, err)
	assert.InDelta(t, unixSeconds(r.now), score, 0.001, "scored by acquire time, in seconds")

	ttl, err := r.rdb.TTL(ctx, "concurrent_calls:7").Result()
	require.NoError(t, err)
	assert.Equal(t, time.Hour, ttl)

	// Dograh's release_concurrent_slot is a plain ZREM on the same key.
	require.NoError(t, r.rdb.ZRem(ctx, "concurrent_calls:7", slot.ID).Err())

	n, _ := r.l.ActiveSlots(ctx, 7)
	assert.Equal(t, 1, n)
}

var breakerCfg = dograh.BreakerConfig{Enabled: true, Threshold: 0.5, Window: 120 * time.Second, MinCalls: 5}

func TestLimiter_BreakerTripsAtThresholdOfMinCalls(t *testing.T) {
	ctx := context.Background()
	r := newRig(t, 0)

	for i := 0; i < 4; i++ {
		tripped, _, err := r.l.RecordOutcome(ctx, 3, true, breakerCfg)
		require.NoError(t, err)
		assert.False(t, tripped, "%d outcomes is below min_calls_in_window", i+1)

		r.advance(time.Millisecond) // an outcome is stored under its timestamp: each needs its own
	}

	tripped, stats, err := r.l.RecordOutcome(ctx, 3, true, breakerCfg)
	require.NoError(t, err)
	assert.True(t, tripped)
	assert.Equal(t, BreakerStats{Failures: 5}, stats)

	open, _, err := r.l.BreakerOpen(ctx, 3, breakerCfg)
	require.NoError(t, err)
	assert.True(t, open, "BreakerOpen sees it without recording")

	other, _, _ := r.l.BreakerOpen(ctx, 4, breakerCfg)
	assert.False(t, other, "per campaign")

	require.NoError(t, r.l.ResetBreaker(ctx, 3))

	open, _, _ = r.l.BreakerOpen(ctx, 3, breakerCfg)
	assert.False(t, open, "resume clears the window")
}

func TestLimiter_BreakerMixAndWindow(t *testing.T) {
	ctx := context.Background()
	r := newRig(t, 0)

	for _, failure := range []bool{true, true, false, false, false} {
		tripped, _, err := r.l.RecordOutcome(ctx, 3, failure, breakerCfg)
		require.NoError(t, err)
		assert.False(t, tripped, "2 of 5 failed: 40%% is under the 50%% threshold")

		r.advance(time.Millisecond)
	}

	tripped, stats, _ := r.l.RecordOutcome(ctx, 3, true, breakerCfg)
	assert.True(t, tripped, "3 of 6 failed: exactly 50%% trips")
	assert.Equal(t, 0.5, stats.Rate())

	r.advance(breakerCfg.Window + time.Second)

	open, stats, err := r.l.BreakerOpen(ctx, 3, breakerCfg)
	require.NoError(t, err)
	assert.False(t, open, "old outcomes slide out of the window")
	assert.Zero(t, stats.Total())
}

func TestLimiter_BreakerDisabledNeverTrips(t *testing.T) {
	ctx := context.Background()
	r := newRig(t, 0)
	off := breakerCfg
	off.Enabled = false

	for i := 0; i < 10; i++ {
		tripped, _, err := r.l.RecordOutcome(ctx, 3, true, off)
		require.NoError(t, err)
		assert.False(t, tripped)
	}
}
