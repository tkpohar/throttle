-- Sliding-window counter, executed atomically inside Redis.
-- Keeps the current and previous fixed-window counts in ONE hash so the
-- script touches a single declared key (Redis Cluster safe) and uses O(1)
-- memory per client, unlike a sorted-set log which is O(requests).
--
-- KEYS[1]  window hash key
-- ARGV[1]  limit per window
-- ARGV[2]  window length in microseconds
-- ARGV[3]  cost of this request
--
-- Returns {allowed, remaining, retry_after_us, reset_after_us}

local key    = KEYS[1]
local limit  = tonumber(ARGV[1])
local window = tonumber(ARGV[2])
local cost   = tonumber(ARGV[3])

local t   = redis.call('TIME')
local now = tonumber(t[1]) * 1000000 + tonumber(t[2])
local curr_start = now - (now % window)

local state = redis.call('HMGET', key, 'start', 'curr', 'prev')
local start = tonumber(state[1])
local curr  = tonumber(state[2]) or 0
local prev  = tonumber(state[3]) or 0

if start ~= curr_start then
  if start == curr_start - window then
    prev = curr
  else
    prev = 0
  end
  curr = 0
end

local elapsed  = now - curr_start
local weight   = (window - elapsed) / window
local estimate = prev * weight + curr
local until_next = window - elapsed

local allowed, retry = 0, 0
if estimate + cost <= limit then
  curr = curr + cost
  estimate = estimate + cost
  allowed = 1
else
  local room = limit - curr - cost
  if room < 0 or prev == 0 then
    retry = until_next
  else
    retry = math.max(1, math.ceil((weight - room / prev) * window))
  end
end

-- %.0f: avoid Lua's %.14g formatting truncating 16-digit timestamps.
redis.call('HSET', key, 'start', string.format('%.0f', curr_start), 'curr', curr, 'prev', prev)
redis.call('PEXPIRE', key, math.ceil(window * 2 / 1000))

local remaining = math.max(0, math.floor(limit - estimate))
return {allowed, remaining, retry, until_next}
