package limiter

import (
	"context"
	_ "embed"
	"fmt"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
)

var (
	//go:embed scripts/token_bucket.lua
	tokenBucketLua string
	//go:embed scripts/sliding_window.lua
	slidingWindowLua string

	tokenBucketScript   = redis.NewScript(tokenBucketLua)
	slidingWindowScript = redis.NewScript(slidingWindowLua)
)

// Redis is a distributed Limiter backed by Redis. Every decision is a single
// round trip running an atomic Lua script (EVALSHA, falling back to EVAL on
// NOSCRIPT), so many stateless app instances can share one set of limits
// without races.
type Redis struct {
	client redis.Scripter
}

// NewRedis wraps any go-redis client (standalone, Sentinel, or Cluster).
func NewRedis(client redis.Scripter) *Redis {
	return &Redis{client: client}
}

// Allow implements Limiter.
func (r *Redis) Allow(ctx context.Context, rule Rule, key string, cost int64) (Result, error) {
	if err := checkCost(rule, cost); err != nil {
		return Result{}, err
	}
	keys := []string{storageKey(rule, key)}

	var (
		raw any
		err error
	)
	switch rule.Algorithm {
	case TokenBucket:
		rate := float64(rule.Limit) / rule.Window.Seconds()
		raw, err = tokenBucketScript.Run(ctx, r.client, keys,
			rule.Capacity(), strconv.FormatFloat(rate, 'f', -1, 64), cost).Result()
	default:
		raw, err = slidingWindowScript.Run(ctx, r.client, keys,
			rule.Limit, rule.Window.Microseconds(), cost).Result()
	}
	if err != nil {
		return Result{}, fmt.Errorf("redis limiter: %w", err)
	}
	return parseScriptResult(raw, rule.Capacity())
}

func parseScriptResult(raw any, limit int64) (Result, error) {
	vals, ok := raw.([]any)
	if !ok || len(vals) != 4 {
		return Result{}, fmt.Errorf("redis limiter: unexpected script reply %v", raw)
	}
	n := make([]int64, 4)
	for i, v := range vals {
		if n[i], ok = v.(int64); !ok {
			return Result{}, fmt.Errorf("redis limiter: reply[%d] is %T", i, v)
		}
	}
	return Result{
		Allowed:    n[0] == 1,
		Limit:      limit,
		Remaining:  n[1],
		RetryAfter: time.Duration(n[2]) * time.Microsecond,
		ResetAfter: time.Duration(n[3]) * time.Microsecond,
	}, nil
}
