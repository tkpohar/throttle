-- Token bucket, executed atomically inside Redis.
-- Uses Redis' own clock (TIME) so that every app instance agrees on "now"
-- regardless of local clock skew.
--
-- KEYS[1]  bucket hash key
-- ARGV[1]  capacity (max tokens)
-- ARGV[2]  refill rate, tokens per second
-- ARGV[3]  cost of this request
--
-- Returns {allowed, remaining, retry_after_us, reset_after_us}

local key      = KEYS[1]
local capacity = tonumber(ARGV[1])
local rate     = tonumber(ARGV[2])
local cost     = tonumber(ARGV[3])

local t   = redis.call('TIME')
local now = tonumber(t[1]) * 1000000 + tonumber(t[2])

local state  = redis.call('HMGET', key, 'tokens', 'ts')
local tokens = tonumber(state[1])
local ts     = tonumber(state[2])
if tokens == nil or ts == nil then
  tokens = capacity
  ts = now
end

local elapsed = math.max(0, now - ts)
tokens = math.min(capacity, tokens + elapsed * rate / 1000000)

local allowed, retry = 0, 0
if tokens >= cost then
  tokens = tokens - cost
  allowed = 1
else
  retry = math.ceil((cost - tokens) * 1000000 / rate)
end

-- %.0f: Lua's default number formatting (%.14g) would lose precision on
-- 16-digit microsecond timestamps.
redis.call('HSET', key, 'tokens', tostring(tokens), 'ts', string.format('%.0f', now))
-- Expire once the bucket would be full again (plus slack): an idle key
-- costs nothing.
redis.call('PEXPIRE', key, math.ceil(capacity / rate * 1000) + 1000)

local reset = math.ceil((capacity - tokens) * 1000000 / rate)
return {allowed, math.floor(tokens), retry, reset}
