#!/usr/bin/env bash
# smoke-test.sh — quick end-to-end test to verify the benchmark platform works.
# Each test runs for 10 seconds; asserts msgs_per_sec > 1000.
set -euo pipefail

PASS=0
FAIL=0

run_smoke() {
  local proto="$1"
  local port="$2"
  echo -n "  $proto ... "

  result=$(./bin/benchmark-runner run \
    --protocol "$proto" \
    --scenario A \
    --msg-size 1024 \
    --duration 10s \
    --warmup 2s \
    --receivers 1 \
    --port "$port" \
    --output json 2>/dev/null) || {
    echo "FAIL (runner error)"
    FAIL=$((FAIL+1))
    return
  }

  msgs_per_sec=$(echo "$result" | jq -r '.[0].msgs_per_sec // 0' 2>/dev/null || echo "0")
  if (( $(echo "$msgs_per_sec > 1000" | bc -l) )); then
    echo "OK (${msgs_per_sec%.0} msgs/sec)"
    PASS=$((PASS+1))
  else
    echo "FAIL (only ${msgs_per_sec%.0} msgs/sec < 1000 threshold)"
    FAIL=$((FAIL+1))
  fi
}

echo "=== Smoke Test Suite ==="
run_smoke udp            9001
run_smoke tcp            9002
run_smoke websocket-gorilla 9003
run_smoke http1          9004
run_smoke http2          9005
run_smoke sse            9007

echo ""
echo "Results: $PASS passed, $FAIL failed"
[ "$FAIL" -eq 0 ]
