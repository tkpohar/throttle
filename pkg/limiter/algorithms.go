package limiter

import (
	"math"
	"time"
)

// The functions in this file are the pure, clock-injected reference
// implementations of each algorithm. The Redis Lua scripts in scripts/
// mirror this logic exactly, and the test suite checks both backends
// against the same expectations.

// bucketState is the persisted state of a token bucket.
type bucketState struct {
	Tokens float64
	Last   time.Time
}

func tokenBucket(rule Rule, st *bucketState, now time.Time, cost int64) Result {
	capacity := float64(rule.Capacity())
	ratePerSec := float64(rule.Limit) / rule.Window.Seconds()

	if st.Last.IsZero() {
		st.Tokens = capacity
		st.Last = now
	}
	elapsed := now.Sub(st.Last).Seconds()
	if elapsed < 0 {
		elapsed = 0 // tolerate small clock regressions
	}
	st.Tokens = math.Min(capacity, st.Tokens+elapsed*ratePerSec)
	st.Last = now

	res := Result{Limit: rule.Capacity()}
	if st.Tokens >= float64(cost) {
		st.Tokens -= float64(cost)
		res.Allowed = true
	} else {
		need := float64(cost) - st.Tokens
		res.RetryAfter = secs(need / ratePerSec)
	}
	res.Remaining = int64(math.Floor(st.Tokens))
	res.ResetAfter = secs((capacity - st.Tokens) / ratePerSec)
	return res
}

// windowState is the persisted state of a sliding-window counter.
type windowState struct {
	Start int64 // start of current window, unix micros
	Curr  int64
	Prev  int64
}

func slidingWindow(rule Rule, st *windowState, now time.Time, cost int64) Result {
	window := rule.Window.Microseconds()
	nowUs := now.UnixMicro()
	currStart := nowUs - nowUs%window

	switch {
	case st.Start == currStart:
		// same window, nothing to rotate
	case st.Start == currStart-window:
		st.Prev, st.Curr = st.Curr, 0
	default:
		st.Prev, st.Curr = 0, 0
	}
	st.Start = currStart

	elapsed := nowUs - currStart
	weight := float64(window-elapsed) / float64(window)
	estimate := float64(st.Prev)*weight + float64(st.Curr)

	res := Result{Limit: rule.Limit, ResetAfter: time.Duration(window-elapsed) * time.Microsecond}
	if estimate+float64(cost) <= float64(rule.Limit) {
		st.Curr += cost
		estimate += float64(cost)
		res.Allowed = true
	} else {
		res.RetryAfter = slidingRetry(rule, st, weight, elapsed, window, cost)
	}
	res.Remaining = max(0, int64(math.Floor(float64(rule.Limit)-estimate)))
	return res
}

// slidingRetry estimates how long until cost units would fit, assuming no
// other traffic arrives. The previous window's contribution decays linearly.
func slidingRetry(rule Rule, st *windowState, weight float64, elapsed, window, cost int64) time.Duration {
	room := float64(rule.Limit - st.Curr - cost)
	untilNext := time.Duration(window-elapsed) * time.Microsecond
	if room < 0 || st.Prev == 0 {
		return untilNext
	}
	targetWeight := room / float64(st.Prev)
	wait := (weight - targetWeight) * float64(window)
	return max(time.Microsecond, time.Duration(math.Ceil(wait))*time.Microsecond)
}

func secs(s float64) time.Duration {
	return time.Duration(math.Ceil(s * float64(time.Second)))
}
