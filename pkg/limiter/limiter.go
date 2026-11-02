// Package limiter implements distributed rate-limiting algorithms with
// pluggable storage backends (in-memory and Redis).
package limiter

import (
	"context"
	"errors"
	"fmt"
	"time"
)

// Algorithm identifies a rate-limiting algorithm.
type Algorithm string

const (
	// TokenBucket allows bursts up to Burst, refilling at Limit tokens per Window.
	TokenBucket Algorithm = "token_bucket"
	// SlidingWindow approximates a true sliding window using a weighted
	// sum of the previous and current fixed windows (O(1) memory per key).
	SlidingWindow Algorithm = "sliding_window"
)

// Rule describes a single rate limit policy.
type Rule struct {
	Name      string        `json:"name"`
	Algorithm Algorithm     `json:"algorithm"`
	Limit     int64         `json:"limit"`  // requests allowed per Window
	Window    time.Duration `json:"window"` // e.g. 1s, 1m
	Burst     int64         `json:"burst"`  // token bucket capacity (defaults to Limit)
}

// Validate checks the rule for obvious misconfiguration.
func (r Rule) Validate() error {
	if r.Name == "" {
		return errors.New("rule name is required")
	}
	if r.Limit <= 0 {
		return fmt.Errorf("rule %q: limit must be > 0", r.Name)
	}
	if r.Window <= 0 {
		return fmt.Errorf("rule %q: window must be > 0", r.Name)
	}
	switch r.Algorithm {
	case TokenBucket, SlidingWindow:
	default:
		return fmt.Errorf("rule %q: unknown algorithm %q", r.Name, r.Algorithm)
	}
	if r.Burst < 0 {
		return fmt.Errorf("rule %q: burst must be >= 0", r.Name)
	}
	return nil
}

// Capacity is the maximum cost that can ever be admitted at once.
func (r Rule) Capacity() int64 {
	if r.Algorithm == TokenBucket && r.Burst > 0 {
		return r.Burst
	}
	return r.Limit
}

// Result is the outcome of a rate-limit decision.
type Result struct {
	Allowed    bool          `json:"allowed"`
	Limit      int64         `json:"limit"`
	Remaining  int64         `json:"remaining"`
	RetryAfter time.Duration `json:"retry_after"` // 0 when allowed
	ResetAfter time.Duration `json:"reset_after"` // time until fully replenished
}

// Limiter decides whether a request identified by key may proceed.
type Limiter interface {
	// Allow consumes cost units for key under rule. It returns an error only
	// when the decision could not be made (e.g. backend unavailable).
	Allow(ctx context.Context, rule Rule, key string, cost int64) (Result, error)
}

// ErrCostExceedsCapacity is returned when a single request asks for more
// units than the rule can ever grant.
var ErrCostExceedsCapacity = errors.New("cost exceeds rule capacity")

func checkCost(rule Rule, cost int64) error {
	if cost <= 0 {
		return errors.New("cost must be > 0")
	}
	if cost > rule.Capacity() {
		return ErrCostExceedsCapacity
	}
	return nil
}

func storageKey(rule Rule, key string) string {
	// Hash-tag the user key so all data for one key lands in the same
	// Redis Cluster slot.
	return "throttle:" + rule.Name + ":{" + key + "}"
}
