package limiter

import (
	"context"
	"errors"
	"fmt"
)

// FailureMode decides what happens when the primary backend errors.
type FailureMode string

const (
	// FailOpen admits every request while the backend is down. Favors
	// availability: an outage of the limiter never becomes an outage of
	// the product.
	FailOpen FailureMode = "open"
	// FailClosed rejects every request while the backend is down. Favors
	// protection of the upstream (e.g. expensive or abusable endpoints).
	FailClosed FailureMode = "closed"
	// FailLocal degrades to a per-instance in-memory limiter. Limits become
	// approximate (each replica enforces them independently) but abuse is
	// still bounded.
	FailLocal FailureMode = "local"
)

// ParseFailureMode validates a mode string.
func ParseFailureMode(s string) (FailureMode, error) {
	switch m := FailureMode(s); m {
	case FailOpen, FailClosed, FailLocal:
		return m, nil
	}
	return "", fmt.Errorf("unknown failure mode %q (want open|closed|local)", s)
}

// Failover wraps a primary Limiter with a policy for backend errors.
type Failover struct {
	Primary  Limiter
	Mode     FailureMode
	Fallback Limiter // used when Mode == FailLocal
	// OnError is invoked for every primary failure (for logs / metrics).
	OnError func(error)
}

// Allow implements Limiter. Caller cancellation and invalid costs are
// returned as-is; only genuine backend failures trigger the failure mode.
func (f *Failover) Allow(ctx context.Context, rule Rule, key string, cost int64) (Result, error) {
	res, err := f.Primary.Allow(ctx, rule, key, cost)
	if err == nil || errors.Is(err, ErrCostExceedsCapacity) || ctx.Err() != nil {
		return res, err
	}
	if f.OnError != nil {
		f.OnError(err)
	}
	switch f.Mode {
	case FailOpen:
		return Result{Allowed: true, Limit: rule.Capacity(), Remaining: rule.Capacity()}, nil
	case FailLocal:
		if f.Fallback != nil {
			return f.Fallback.Allow(ctx, rule, key, cost)
		}
	}
	return Result{Allowed: false, Limit: rule.Capacity(), RetryAfter: rule.Window}, nil
}
