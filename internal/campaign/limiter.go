// Package campaign dials Dograh's outbound campaigns for organizations whose
// media engine is "vaani". limiter.go is the shared counting: the Redis keys
// and Lua scripts are Dograh's (api/services/campaign/rate_limiter.py and
// circuit_breaker.py), copied as they are, so a slot taken here counts for
// Dograh and the other way round -- an organization's concurrent-call limit
// holds across both engines, and a crashed call's slot expires on its own.
package campaign

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/nitesh/vaani/internal/dograh"
)

// staleSlotAfter is Dograh's stale_call_timeout: a slot older than this is
// dropped the next time anyone acquires one. A call running longer than this
// stops counting toward its organization's limit, as in Dograh.
const staleSlotAfter = 20 * time.Minute

// capacityKey is Vaani's own cap on outbound calls (OUTBOUND_MAX_CALLS). It is
// deliberately not Dograh's fleet_capacity:calls, which is sized for Dograh's
// workers, not for this process.
const capacityKey = "vaani_capacity:calls"

// Limiter counts outbound calls in Redis.
type Limiter struct {
	rdb      redis.Scripter
	now      func() time.Time
	maxCalls int // Vaani's own cap; 0 = no cap
}

// NewLimiter returns a Limiter on rdb. maxCalls caps the outbound calls this
// deployment places at once (0 = only the organizations' limits apply).
func NewLimiter(rdb redis.Scripter, maxCalls int) *Limiter {
	return &Limiter{rdb: rdb, now: time.Now, maxCalls: maxCalls}
}

func unixSeconds(t time.Time) float64 { return float64(t.UnixNano()) / 1e9 }

// rateScript is Dograh's acquire_token: a sliding one-second window.
var rateScript = redis.NewScript(`
local key = KEYS[1]
local now = tonumber(ARGV[1])
local window_start = tonumber(ARGV[2])
local max_requests = tonumber(ARGV[3])

-- Remove timestamps older than window
redis.call('ZREMRANGEBYSCORE', key, 0, window_start)

-- Count requests in current window
local current_requests = redis.call('ZCARD', key)

if current_requests < max_requests then
    -- Add current timestamp
    redis.call('ZADD', key, now, now)
    redis.call('EXPIRE', key, 2)  -- Expire after 2 seconds
    return 1
else
    return 0
end
`)

// slotScript is Dograh's concurrent-slot script. Dograh runs the same text on
// the organization's key and on its fleet key; here it runs on the
// organization's key and on Vaani's own capacity key.
var slotScript = redis.NewScript(`
local key = KEYS[1]
local now = tonumber(ARGV[1])
local max_concurrent = tonumber(ARGV[2])
local stale_cutoff = tonumber(ARGV[3])
local slot_id = ARGV[4]

-- Remove stale entries (older than 30 minutes)
redis.call('ZREMRANGEBYSCORE', key, 0, stale_cutoff)

-- Get current count
local current_count = redis.call('ZCARD', key)

if current_count < max_concurrent then
    -- Add new slot
    redis.call('ZADD', key, now, slot_id)
    redis.call('EXPIRE', key, 3600)  -- Expire after 1 hour
    return slot_id
else
    return nil
end
`)

// breakerScript is Dograh's record_call_outcome.
var breakerScript = redis.NewScript(`
local fail_key = KEYS[1]
local succ_key = KEYS[2]
local now = tonumber(ARGV[1])
local window_start = tonumber(ARGV[2])
local is_failure = tonumber(ARGV[3])
local threshold = tonumber(ARGV[4])
local min_calls = tonumber(ARGV[5])
local ttl = tonumber(ARGV[6])

-- Trim both sets to the sliding window
redis.call('ZREMRANGEBYSCORE', fail_key, 0, window_start)
redis.call('ZREMRANGEBYSCORE', succ_key, 0, window_start)

-- Add the new outcome to the appropriate set
if is_failure == 1 then
    redis.call('ZADD', fail_key, now, now)
else
    redis.call('ZADD', succ_key, now, now)
end

-- Refresh TTL on both keys
redis.call('EXPIRE', fail_key, ttl)
redis.call('EXPIRE', succ_key, ttl)

-- Count via ZCARD (O(1))
local failures = redis.call('ZCARD', fail_key)
local successes = redis.call('ZCARD', succ_key)
local total = failures + successes

-- Check trip condition
if total >= min_calls and (failures / total) >= threshold then
    return {1, failures, successes, total}
end

return {0, failures, successes, total}
`)

// breakerCheckScript is Dograh's is_circuit_open (no outcome recorded).
var breakerCheckScript = redis.NewScript(`
local fail_key = KEYS[1]
local succ_key = KEYS[2]
local window_start = tonumber(ARGV[1])
local threshold = tonumber(ARGV[2])
local min_calls = tonumber(ARGV[3])

-- Trim both sets
redis.call('ZREMRANGEBYSCORE', fail_key, 0, window_start)
redis.call('ZREMRANGEBYSCORE', succ_key, 0, window_start)

-- Count via ZCARD
local failures = redis.call('ZCARD', fail_key)
local successes = redis.call('ZCARD', succ_key)
local total = failures + successes

if total >= min_calls and (failures / total) >= threshold then
    return {1, failures, successes, total}
end

return {0, failures, successes, total}
`)

func rateKey(org int) string       { return fmt.Sprintf("rate_limit:%d", org) }
func concurrentKey(org int) string { return fmt.Sprintf("concurrent_calls:%d", org) }

func breakerKeys(campaign int) (string, string) {
	return fmt.Sprintf("cb_failures:%d", campaign), fmt.Sprintf("cb_successes:%d", campaign)
}

// AcquireToken takes one of the organization's perSecond call starts in the
// current one-second window; false means wait (see NextTokenWait). An error
// counts as a refusal, as in Dograh.
func (l *Limiter) AcquireToken(ctx context.Context, org, perSecond int) (bool, error) {
	now := unixSeconds(l.now())

	n, err := rateScript.Run(ctx, l.rdb, []string{rateKey(org)}, now, now-1.0, perSecond).Int()
	if err != nil {
		return false, fmt.Errorf("campaign: rate limit: %w", err)
	}

	return n == 1, nil
}

// NextTokenWait is how long until the organization's window has room again.
func (l *Limiter) NextTokenWait(ctx context.Context, org int) (time.Duration, error) {
	rdb, ok := l.rdb.(redis.Cmdable)
	if !ok {
		return time.Second, nil
	}

	oldest, err := rdb.ZRangeWithScores(ctx, rateKey(org), 0, 0).Result()
	if err != nil {
		return time.Second, fmt.Errorf("campaign: rate limit wait: %w", err)
	}

	if len(oldest) == 0 {
		return 0, nil
	}

	wait := oldest[0].Score + 1.0 - unixSeconds(l.now())

	return max(0, time.Duration(wait*float64(time.Second))), nil
}

// Slot is a reserved place for one call; release it when the call ends or
// never started. The same id releases both the organization's slot and
// Vaani's own cap.
type Slot struct {
	ID  string
	Org int
}

func newSlotID(now time.Time) string {
	var b [4]byte
	_, _ = rand.Read(b[:])

	return fmt.Sprintf("%d_%s", now.UnixMilli(), hex.EncodeToString(b[:]))
}

// AcquireSlot reserves a concurrent-call slot for org, up to limit calls at
// once across both engines, and under Vaani's own cap. ok is false when
// either is full.
func (l *Limiter) AcquireSlot(ctx context.Context, org, limit int) (Slot, bool, error) {
	now := l.now()
	id := newSlotID(now)
	cutoff := unixSeconds(now.Add(-staleSlotAfter))

	// Own cap first: no point holding an organization slot without capacity.
	if l.maxCalls > 0 {
		if _, ok, err := l.takeSlot(ctx, capacityKey, now, l.maxCalls, cutoff, id); err != nil || !ok {
			return Slot{}, false, err
		}
	}

	if _, ok, err := l.takeSlot(ctx, concurrentKey(org), now, limit, cutoff, id); err != nil || !ok {
		l.dropCapacity(ctx, id)

		return Slot{}, false, err
	}

	return Slot{ID: id, Org: org}, true, nil
}

func (l *Limiter) takeSlot(ctx context.Context, key string, now time.Time, limit int, cutoff float64, id string) (string, bool, error) {
	got, err := slotScript.Run(ctx, l.rdb, []string{key}, unixSeconds(now), limit, cutoff, id).Text()
	if errors.Is(err, redis.Nil) {
		return "", false, nil
	}

	if err != nil {
		return "", false, fmt.Errorf("campaign: slot on %s: %w", key, err)
	}

	return got, true, nil
}

func (l *Limiter) dropCapacity(ctx context.Context, id string) {
	if rdb, ok := l.rdb.(redis.Cmdable); ok && l.maxCalls > 0 {
		_ = rdb.ZRem(ctx, capacityKey, id).Err()
	}
}

// ReleaseSlot gives the slot back (the organization's and Vaani's cap).
// Releasing twice, or a slot that already expired, is not an error.
func (l *Limiter) ReleaseSlot(ctx context.Context, s Slot) error {
	rdb, ok := l.rdb.(redis.Cmdable)
	if !ok {
		return errors.New("campaign: limiter has no Redis client")
	}

	l.dropCapacity(ctx, s.ID)

	if err := rdb.ZRem(ctx, concurrentKey(s.Org), s.ID).Err(); err != nil {
		return fmt.Errorf("campaign: release slot %s: %w", s.ID, err)
	}

	return nil
}

// ActiveSlots is how many calls the organization has in flight, both engines
// together (stale slots included until the next acquire trims them).
func (l *Limiter) ActiveSlots(ctx context.Context, org int) (int, error) {
	rdb, ok := l.rdb.(redis.Cmdable)
	if !ok {
		return 0, errors.New("campaign: limiter has no Redis client")
	}

	n, err := rdb.ZCard(ctx, concurrentKey(org)).Result()
	if err != nil {
		return 0, fmt.Errorf("campaign: active slots: %w", err)
	}

	return int(n), nil
}

// BreakerStats is the circuit breaker's sliding window.
type BreakerStats struct {
	Failures, Successes int
}

func (s BreakerStats) Total() int { return s.Failures + s.Successes }

func (s BreakerStats) Rate() float64 {
	if s.Total() == 0 {
		return 0
	}

	return float64(s.Failures) / float64(s.Total())
}

// RecordOutcome adds a finished call to the campaign's window and says whether
// the breaker is now tripped. Like Dograh it fails open: a Redis error never
// trips it.
func (l *Limiter) RecordOutcome(ctx context.Context, campaignID int, failure bool, cfg dograh.BreakerConfig) (bool, BreakerStats, error) {
	if !cfg.Enabled {
		return false, BreakerStats{}, nil
	}

	failKey, succKey := breakerKeys(campaignID)
	now := l.now()
	isFailure := 0

	if failure {
		isFailure = 1
	}

	res, err := breakerScript.Run(ctx, l.rdb, []string{failKey, succKey},
		unixSeconds(now), unixSeconds(now.Add(-cfg.Window)), isFailure,
		strconv.FormatFloat(cfg.Threshold, 'f', -1, 64), cfg.MinCalls, int(cfg.Window.Seconds())+60).Int64Slice()
	if err != nil {
		return false, BreakerStats{}, fmt.Errorf("campaign: circuit breaker: %w", err)
	}

	return breakerResult(res)
}

// BreakerOpen says whether the campaign's breaker is tripped, without
// recording anything.
func (l *Limiter) BreakerOpen(ctx context.Context, campaignID int, cfg dograh.BreakerConfig) (bool, BreakerStats, error) {
	if !cfg.Enabled {
		return false, BreakerStats{}, nil
	}

	failKey, succKey := breakerKeys(campaignID)

	res, err := breakerCheckScript.Run(ctx, l.rdb, []string{failKey, succKey},
		unixSeconds(l.now().Add(-cfg.Window)), strconv.FormatFloat(cfg.Threshold, 'f', -1, 64), cfg.MinCalls).Int64Slice()
	if err != nil {
		return false, BreakerStats{}, fmt.Errorf("campaign: circuit breaker: %w", err)
	}

	return breakerResult(res)
}

func breakerResult(res []int64) (bool, BreakerStats, error) {
	if len(res) != 4 {
		return false, BreakerStats{}, fmt.Errorf("campaign: circuit breaker: unexpected reply %v", res)
	}

	return res[0] == 1, BreakerStats{Failures: int(res[1]), Successes: int(res[2])}, nil
}

// ResetBreaker clears the campaign's window, as Dograh does on resume.
func (l *Limiter) ResetBreaker(ctx context.Context, campaignID int) error {
	rdb, ok := l.rdb.(redis.Cmdable)
	if !ok {
		return errors.New("campaign: limiter has no Redis client")
	}

	failKey, succKey := breakerKeys(campaignID)

	if err := rdb.Del(ctx, failKey, succKey).Err(); err != nil {
		return fmt.Errorf("campaign: reset breaker: %w", err)
	}

	return nil
}
