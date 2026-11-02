package limiter

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

// fakeClock is a manually advanced clock for deterministic tests.
type fakeClock struct {
	mu  sync.Mutex
	now time.Time
}

func newFakeClock() *fakeClock {
	// Align to a window boundary so sliding-window math is predictable.
	return &fakeClock{now: time.Unix(1_700_000_040, 0)} // divisible by 60
}
func (c *fakeClock) Now() time.Time { c.mu.Lock(); defer c.mu.Unlock(); return c.now }
func (c *fakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	c.now = c.now.Add(d)
	c.mu.Unlock()
}

var ctx = context.Background()

func mustAllow(t *testing.T, l Limiter, r Rule, key string, want bool) Result {
	t.Helper()
	res, err := l.Allow(ctx, r, key, 1)
	if err != nil {
		t.Fatalf("Allow: %v", err)
	}
	if res.Allowed != want {
		t.Fatalf("Allow(%q) = %v, want %v (res=%+v)", key, res.Allowed, want, res)
	}
	return res
}

func TestRuleValidate(t *testing.T) {
	cases := []struct {
		name string
		rule Rule
		ok   bool
	}{
		{"valid tb", Rule{Name: "a", Algorithm: TokenBucket, Limit: 1, Window: time.Second}, true},
		{"valid sw", Rule{Name: "a", Algorithm: SlidingWindow, Limit: 1, Window: time.Second}, true},
		{"no name", Rule{Algorithm: TokenBucket, Limit: 1, Window: time.Second}, false},
		{"zero limit", Rule{Name: "a", Algorithm: TokenBucket, Window: time.Second}, false},
		{"zero window", Rule{Name: "a", Algorithm: TokenBucket, Limit: 1}, false},
		{"bad algo", Rule{Name: "a", Algorithm: "leaky", Limit: 1, Window: time.Second}, false},
		{"neg burst", Rule{Name: "a", Algorithm: TokenBucket, Limit: 1, Window: time.Second, Burst: -1}, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if err := tc.rule.Validate(); (err == nil) != tc.ok {
				t.Fatalf("Validate() err=%v, want ok=%v", err, tc.ok)
			}
		})
	}
}

func TestTokenBucketBurstThenRefill(t *testing.T) {
	clk := newFakeClock()
	m := NewMemory(WithClock(clk.Now))
	defer m.Close()
	// 10 req/s steady state, bursts of 5.
	r := Rule{Name: "tb", Algorithm: TokenBucket, Limit: 10, Window: time.Second, Burst: 5}

	for i := 0; i < 5; i++ {
		res := mustAllow(t, m, r, "u1", true)
		if res.Remaining != int64(4-i) {
			t.Fatalf("remaining=%d want %d", res.Remaining, 4-i)
		}
	}
	res := mustAllow(t, m, r, "u1", false)
	if res.RetryAfter != 100*time.Millisecond {
		t.Fatalf("retry after = %v, want 100ms", res.RetryAfter)
	}

	clk.Advance(100 * time.Millisecond) // exactly one token
	mustAllow(t, m, r, "u1", true)
	mustAllow(t, m, r, "u1", false)

	clk.Advance(10 * time.Second) // refill caps at burst
	for i := 0; i < 5; i++ {
		mustAllow(t, m, r, "u1", true)
	}
	mustAllow(t, m, r, "u1", false)
}

func TestKeysAndRulesAreIsolated(t *testing.T) {
	clk := newFakeClock()
	m := NewMemory(WithClock(clk.Now))
	defer m.Close()
	a := Rule{Name: "a", Algorithm: TokenBucket, Limit: 1, Window: time.Minute}
	b := Rule{Name: "b", Algorithm: TokenBucket, Limit: 1, Window: time.Minute}

	mustAllow(t, m, a, "u1", true)
	mustAllow(t, m, a, "u1", false)
	mustAllow(t, m, a, "u2", true) // other key unaffected
	mustAllow(t, m, b, "u1", true) // other rule unaffected
}

func TestSlidingWindowWeightsPreviousWindow(t *testing.T) {
	clk := newFakeClock()
	m := NewMemory(WithClock(clk.Now))
	defer m.Close()
	r := Rule{Name: "sw", Algorithm: SlidingWindow, Limit: 10, Window: time.Minute}

	for i := 0; i < 10; i++ {
		mustAllow(t, m, r, "u", true)
	}
	mustAllow(t, m, r, "u", false)

	// 25% into the next window the previous window still counts 75%:
	// estimate = 10*0.75 + 0 = 7.5, so only 2 more fit (9.5 <= 10).
	clk.Advance(time.Minute + 15*time.Second)
	mustAllow(t, m, r, "u", true)
	mustAllow(t, m, r, "u", true)
	res := mustAllow(t, m, r, "u", false)
	if res.RetryAfter <= 0 || res.RetryAfter > 45*time.Second {
		t.Fatalf("unexpected retry after %v", res.RetryAfter)
	}

	// Two full windows later all history is gone.
	clk.Advance(2 * time.Minute)
	for i := 0; i < 10; i++ {
		mustAllow(t, m, r, "u", true)
	}
}

func TestSlidingWindowNoBoundaryBurst(t *testing.T) {
	// The classic fixed-window flaw: 10 requests at 0:59 and 10 more at
	// 1:00 lets 20 through in one second. The sliding window must not.
	clk := newFakeClock()
	m := NewMemory(WithClock(clk.Now))
	defer m.Close()
	r := Rule{Name: "sw", Algorithm: SlidingWindow, Limit: 10, Window: time.Minute}

	clk.Advance(59 * time.Second)
	for i := 0; i < 10; i++ {
		mustAllow(t, m, r, "u", true)
	}
	clk.Advance(time.Second)
	mustAllow(t, m, r, "u", false)
}

func TestCostValidation(t *testing.T) {
	m := NewMemory()
	defer m.Close()
	r := Rule{Name: "c", Algorithm: TokenBucket, Limit: 5, Window: time.Second}
	if _, err := m.Allow(ctx, r, "k", 6); !errors.Is(err, ErrCostExceedsCapacity) {
		t.Fatalf("want ErrCostExceedsCapacity, got %v", err)
	}
	if _, err := m.Allow(ctx, r, "k", 0); err == nil {
		t.Fatal("want error for zero cost")
	}
	res, err := m.Allow(ctx, r, "k", 5)
	if err != nil || !res.Allowed || res.Remaining != 0 {
		t.Fatalf("weighted cost: res=%+v err=%v", res, err)
	}
}

func TestMemorySweepEvictsIdleKeys(t *testing.T) {
	clk := newFakeClock()
	m := NewMemory(WithClock(clk.Now))
	defer m.Close()
	r := Rule{Name: "e", Algorithm: TokenBucket, Limit: 1, Window: time.Second}
	for i := 0; i < 100; i++ {
		mustAllow(t, m, r, fmt.Sprint("k", i), true)
	}
	if m.Len() != 100 {
		t.Fatalf("len=%d", m.Len())
	}
	clk.Advance(3 * time.Second)
	m.Sweep()
	if m.Len() != 0 {
		t.Fatalf("len after sweep=%d, want 0", m.Len())
	}
}

func TestMemoryConcurrentExactlyLimit(t *testing.T) {
	m := NewMemory()
	defer m.Close()
	r := Rule{Name: "conc", Algorithm: SlidingWindow, Limit: 100, Window: time.Hour}
	assertExactlyLimitUnderConcurrency(t, m, r)
}

// assertExactlyLimitUnderConcurrency fires 1000 concurrent requests at one
// key and asserts that exactly Limit are admitted — no lost updates.
func assertExactlyLimitUnderConcurrency(t *testing.T, l Limiter, r Rule) {
	t.Helper()
	var allowed atomic.Int64
	var wg sync.WaitGroup
	for i := 0; i < 1000; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			res, err := l.Allow(ctx, r, "hot-key", 1)
			if err != nil {
				t.Error(err)
				return
			}
			if res.Allowed {
				allowed.Add(1)
			}
		}()
	}
	wg.Wait()
	if got := allowed.Load(); got != r.Limit {
		t.Fatalf("admitted %d requests, want exactly %d", got, r.Limit)
	}
}

// ---- Failover ----------------------------------------------------------

type brokenLimiter struct{}

func (brokenLimiter) Allow(context.Context, Rule, string, int64) (Result, error) {
	return Result{}, errors.New("connection refused")
}

func TestFailoverModes(t *testing.T) {
	r := Rule{Name: "f", Algorithm: TokenBucket, Limit: 1, Window: time.Minute}

	open := &Failover{Primary: brokenLimiter{}, Mode: FailOpen}
	mustAllow(t, open, r, "k", true)
	mustAllow(t, open, r, "k", true)

	closed := &Failover{Primary: brokenLimiter{}, Mode: FailClosed}
	mustAllow(t, closed, r, "k", false)

	local := NewMemory()
	defer local.Close()
	var errs int
	lf := &Failover{Primary: brokenLimiter{}, Mode: FailLocal, Fallback: local, OnError: func(error) { errs++ }}
	mustAllow(t, lf, r, "k", true)
	mustAllow(t, lf, r, "k", false) // local limiter still enforces
	if errs != 2 {
		t.Fatalf("OnError called %d times, want 2", errs)
	}
}

// ---- Redis integration -------------------------------------------------
// Run with: REDIS_ADDR=localhost:6379 go test ./...

func redisLimiter(t testing.TB) *Redis {
	t.Helper()
	addr := os.Getenv("REDIS_ADDR")
	if addr == "" {
		t.Skip("REDIS_ADDR not set; skipping Redis integration test")
	}
	rdb := redis.NewClient(&redis.Options{Addr: addr, PoolSize: 128})
	if err := rdb.Ping(ctx).Err(); err != nil {
		t.Fatalf("redis ping: %v", err)
	}
	t.Cleanup(func() { rdb.Close() })
	return NewRedis(rdb)
}

func uniqueRule(t testing.TB, algo Algorithm, limit int64, window time.Duration) Rule {
	return Rule{Name: fmt.Sprintf("%s-%d", t.Name(), time.Now().UnixNano()), Algorithm: algo, Limit: limit, Window: window}
}

func TestRedisTokenBucket(t *testing.T) {
	l := redisLimiter(t)
	r := uniqueRule(t, TokenBucket, 5, time.Hour)
	for i := 0; i < 5; i++ {
		res := mustAllow(t, l, r, "u", true)
		if res.Remaining != int64(4-i) {
			t.Fatalf("remaining=%d want %d", res.Remaining, 4-i)
		}
	}
	res := mustAllow(t, l, r, "u", false)
	if res.RetryAfter < 11*time.Minute || res.RetryAfter > 13*time.Minute {
		t.Fatalf("retry after %v, want ~12m", res.RetryAfter)
	}
}

func TestRedisTokenBucketRefills(t *testing.T) {
	l := redisLimiter(t)
	r := uniqueRule(t, TokenBucket, 20, time.Second) // 1 token / 50ms
	r.Burst = 1
	mustAllow(t, l, r, "u", true)
	mustAllow(t, l, r, "u", false)
	time.Sleep(60 * time.Millisecond)
	mustAllow(t, l, r, "u", true)
}

func TestRedisSlidingWindow(t *testing.T) {
	l := redisLimiter(t)
	r := uniqueRule(t, SlidingWindow, 3, time.Hour)
	for i := 0; i < 3; i++ {
		mustAllow(t, l, r, "u", true)
	}
	mustAllow(t, l, r, "u", false)
	mustAllow(t, l, r, "other", true)
}

func TestRedisConcurrentExactlyLimit(t *testing.T) {
	l := redisLimiter(t)
	for _, algo := range []Algorithm{TokenBucket, SlidingWindow} {
		t.Run(string(algo), func(t *testing.T) {
			assertExactlyLimitUnderConcurrency(t, l, uniqueRule(t, algo, 100, time.Hour))
		})
	}
}

// ---- Benchmarks ----------------------------------------------------------

func BenchmarkMemoryTokenBucketParallel(b *testing.B) {
	m := NewMemory()
	defer m.Close()
	r := Rule{Name: "bench", Algorithm: TokenBucket, Limit: 1 << 40, Window: time.Second}
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			_, _ = m.Allow(ctx, r, fmt.Sprint(i%1024), 1)
			i++
		}
	})
}

func BenchmarkMemorySlidingWindowParallel(b *testing.B) {
	m := NewMemory()
	defer m.Close()
	r := Rule{Name: "bench", Algorithm: SlidingWindow, Limit: 1 << 40, Window: time.Second}
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			_, _ = m.Allow(ctx, r, fmt.Sprint(i%1024), 1)
			i++
		}
	})
}

func BenchmarkRedisTokenBucketParallel(b *testing.B) {
	l := redisLimiter(b)
	r := uniqueRule(b, TokenBucket, 1<<40, time.Second)
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			if _, err := l.Allow(ctx, r, fmt.Sprint(i%1024), 1); err != nil {
				b.Error(err)
				return
			}
			i++
		}
	})
}
