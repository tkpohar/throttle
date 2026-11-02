# Design notes

This document explains the main trade-offs in Throttle. It covers the questions a system-design interviewer would ask about a distributed rate limiter.

## 1. Requirements

- **Functional**: limit requests per *(rule, key)*. A key can be a user, an API key or an IP. Rules can set different limits, windows and algorithms. Requests can carry a cost (weighted requests).
- **Non-functional**: a decision should take under 1 ms of added latency inside the service. The limit must hold globally across N stateless replicas. The limiter must never become a single point of failure for the product, and must stay correct under heavy concurrency on a hot key.

## 2. Algorithm choice

| Algorithm | Memory per key | Bursts | Accuracy | Used here |
|---|---|---|---|---|
| Fixed window counter | O(1) | Up to 2× the limit at the window boundary | Poor | ✗ |
| Sliding window log (ZSET of timestamps) | O(requests) | None | Exact | ✗ (too much memory at high limits) |
| **Sliding window counter** | **O(1)** | **None** | **Approximate (assumes even spread within the previous window)** | ✓ |
| **Token bucket** | **O(1)** | **Configurable (`burst`)** | **Exact** | ✓ |
| Leaky bucket (queue) | O(queue) | None (smooths output) | Exact | ✗ (shapes traffic rather than admitting or rejecting it) |

- **Token bucket** suits APIs where short bursts are fine, such as a page load firing 10 calls at once.
- **Sliding window** suits security-sensitive limits like login attempts, where a boundary burst would let an attacker double their guesses.

The sliding-window estimate is `prev × (1 − elapsed/window) + curr`. Cloudflare reported that this approximation is within a fraction of a percent of an exact log on real traffic.

## 3. Atomicity: why Lua and not `MULTI` or `WATCH`

A naive `GET` → compute → `SET` lets two replicas both read "4 remaining" and both admit a request. The options:

- **`WATCH`/`MULTI`** (optimistic locking): retries under contention, so a hot key gets slower exactly when it's under attack.
- **`INCR` + `EXPIRE`**: atomic, but only works for fixed windows.
- **Lua scripts** ✓: Redis runs a script atomically. The whole read-compute-write happens in **one round trip** with no retries. `go-redis` uses `EVALSHA` and falls back to `EVAL` on `NOSCRIPT`, so the script body isn't resent on every call.

`TestRedisConcurrentExactlyLimit` proves this works: 1,000 concurrent goroutines get exactly `limit` admissions.

## 4. Clock source

If each replica passed its own `time.Now()`, clock skew between hosts would corrupt the bucket state. A replica 2 s ahead would refill tokens early. The scripts read `redis.call('TIME')`, so there's a single authoritative clock. Redis 5+ replicates script *effects*, not the script itself, which makes this deterministic on replicas.

Detail: Lua formats numbers with `%.14g` by default, which silently truncates 16-digit microsecond timestamps. The scripts store them with `string.format('%.0f', …)`.

## 5. Redis Cluster compatibility

Each script touches exactly one key, declared in `KEYS[1]`. That includes the sliding window: the current and previous counts live in one hash instead of two keys. The user key is hash-tagged (`throttle:login:{user-42}`). This keeps every operation single-slot and lets the keyspace shard across a cluster.

## 6. Memory and expiry

Every key gets a `PEXPIRE` timed to when its state stops mattering: when the bucket would be full again, or after 2 windows. Idle clients cost nothing. The in-memory backend uses the same rule, with a background sweeper over 64 lock-sharded maps.

## 7. Failure modes

When Redis is unreachable (with a 50 ms default timeout, so the limiter can't stall requests), the `Failover` wrapper applies a policy:

| Mode | Behavior | When to use |
|---|---|---|
| `open` | Allow everything | Limits are a nicety, and availability comes first |
| `closed` | Deny everything | Protecting something expensive or abusable (SMS sends, payments) |
| `local` (default) | Each replica enforces the limit in memory | A good middle ground: abuse stays bounded at about N × limit for N replicas |

Every failure increments `throttle_backend_errors_total`, and `/readyz` goes unhealthy so the failure is visible.

## 8. What I'd build next

- **Local pre-check / batching**: keep a small local token cache per hot key and sync with Redis asynchronously. This trades a little accuracy for removing the network hop from most decisions (the approach Stripe and Envoy take).
- **Dynamic rules**: hot-reload rules from a config store and support per-tenant overrides.
- **gRPC API**: compatible with Envoy's `ratelimit` service protocol, so it can plug into a service mesh.
- **Circuit breaker**: skip Redis entirely for a cool-down period after repeated failures, instead of paying the timeout on every request.
- **Load test**: a k6 scenario plus a Grafana dashboard for the exported metrics.
