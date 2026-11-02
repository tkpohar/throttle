#!/usr/bin/env bash
# Sends alternating requests to two replicas. The "login" rule allows 5/min,
# so the 6th request is rejected no matter which replica receives it.
set -euo pipefail
for i in $(seq 1 8); do
  port=$(( i % 2 == 0 ? 8082 : 8081 ))
  code=$(curl -s -o /tmp/throttle-demo.json -w '%{http_code}' \
    -X POST "localhost:${port}/v1/check" -d '{"rule":"login","key":"demo-user"}')
  printf 'request %d -> replica :%d -> HTTP %s  %s\n' "$i" "$port" "$code" "$(cat /tmp/throttle-demo.json)"
done
