package limiter

import (
	"context"
	"hash/maphash"
	"sync"
	"time"
)

const shardCount = 64

// Memory is an in-process Limiter. It is lock-sharded to reduce contention
// and evicts idle keys in the background. Use it for single-instance
// deployments, tests, or as a degraded-mode fallback when Redis is down.
type Memory struct {
	shards [shardCount]memShard
	seed   maphash.Seed
	now    func() time.Time
	stop   chan struct{}
	once   sync.Once
}

type memShard struct {
	mu      sync.Mutex
	buckets map[string]*memEntry[bucketState]
	windows map[string]*memEntry[windowState]
}

type memEntry[T any] struct {
	state   T
	expires time.Time
}

// MemoryOption configures a Memory limiter.
type MemoryOption func(*Memory)

// WithClock overrides the time source (used for deterministic tests).
func WithClock(now func() time.Time) MemoryOption { return func(m *Memory) { m.now = now } }

// NewMemory returns an in-memory limiter. Call Close to stop the janitor.
func NewMemory(opts ...MemoryOption) *Memory {
	m := &Memory{seed: maphash.MakeSeed(), now: time.Now, stop: make(chan struct{})}
	for i := range m.shards {
		m.shards[i].buckets = make(map[string]*memEntry[bucketState])
		m.shards[i].windows = make(map[string]*memEntry[windowState])
	}
	for _, o := range opts {
		o(m)
	}
	go m.janitor(time.Minute)
	return m
}

// Allow implements Limiter.
func (m *Memory) Allow(_ context.Context, rule Rule, key string, cost int64) (Result, error) {
	if err := checkCost(rule, cost); err != nil {
		return Result{}, err
	}
	k := storageKey(rule, key)
	sh := &m.shards[maphash.String(m.seed, k)%shardCount]
	now := m.now()
	ttl := 2 * rule.Window

	sh.mu.Lock()
	defer sh.mu.Unlock()

	switch rule.Algorithm {
	case TokenBucket:
		e := sh.buckets[k]
		if e == nil || now.After(e.expires) {
			e = &memEntry[bucketState]{}
			sh.buckets[k] = e
		}
		e.expires = now.Add(ttl)
		return tokenBucket(rule, &e.state, now, cost), nil
	default:
		e := sh.windows[k]
		if e == nil || now.After(e.expires) {
			e = &memEntry[windowState]{}
			sh.windows[k] = e
		}
		e.expires = now.Add(ttl)
		return slidingWindow(rule, &e.state, now, cost), nil
	}
}

// Len returns the number of tracked keys (for metrics and tests).
func (m *Memory) Len() int {
	n := 0
	for i := range m.shards {
		sh := &m.shards[i]
		sh.mu.Lock()
		n += len(sh.buckets) + len(sh.windows)
		sh.mu.Unlock()
	}
	return n
}

// Sweep removes expired entries. It runs periodically in the background
// and is exported so tests can trigger it deterministically.
func (m *Memory) Sweep() {
	now := m.now()
	for i := range m.shards {
		sh := &m.shards[i]
		sh.mu.Lock()
		for k, e := range sh.buckets {
			if now.After(e.expires) {
				delete(sh.buckets, k)
			}
		}
		for k, e := range sh.windows {
			if now.After(e.expires) {
				delete(sh.windows, k)
			}
		}
		sh.mu.Unlock()
	}
}

func (m *Memory) janitor(every time.Duration) {
	t := time.NewTicker(every)
	defer t.Stop()
	for {
		select {
		case <-t.C:
			m.Sweep()
		case <-m.stop:
			return
		}
	}
}

// Close stops the background janitor.
func (m *Memory) Close() error {
	m.once.Do(func() { close(m.stop) })
	return nil
}
