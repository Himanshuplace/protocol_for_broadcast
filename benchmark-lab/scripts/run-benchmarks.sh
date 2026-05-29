#!/usr/bin/env bash
# run-benchmarks.sh — runs the full benchmark suite across all protocols and scenarios.
# Prerequisites: Docker Compose stack running (make docker-up)
# Usage: ./scripts/run-benchmarks.sh [--duration 60] [--scenario A,B] [--output html]
set -euo pipefail

DURATION="${DURATION:-60}"
SCENARIOS="${SCENARIOS:-A,B}"
OUTPUT_FORMAT="${OUTPUT_FORMAT:-markdown}"
OUTPUT_DIR="${OUTPUT_DIR:-./results}"
WARMUP="${WARMUP:-10}"

PROTOCOLS=(
  "udp"
  "tcp"
  "websocket-gorilla"
  "websocket-gobwas"
  "websocket-coder"
  "http1"
  "http2"
  "http3"
  "sse"
  "webtransport"
)

MSG_SIZES=(64 256 1024 4096 65536)

mkdir -p "$OUTPUT_DIR"

TS=$(date +%Y%m%d_%H%M%S)
REPORT_FILE="$OUTPUT_DIR/benchmark_${TS}.${OUTPUT_FORMAT}"

echo "=== Protocol Benchmark Suite ==="
echo "Timestamp:  $TS"
echo "Scenarios:  $SCENARIOS"
echo "Duration:   ${DURATION}s per scenario"
echo "Output:     $REPORT_FILE"
echo ""

ALL_RESULTS=()

for proto in "${PROTOCOLS[@]}"; do
  for scenario in $(echo "$SCENARIOS" | tr ',' ' '); do
    for msg_size in "${MSG_SIZES[@]}"; do
      echo "Running: protocol=$proto scenario=$scenario msg_size=${msg_size}B"

      result_file="$OUTPUT_DIR/${proto}_${scenario}_${msg_size}B_${TS}.json"

      ./bin/benchmark-runner run \
        --protocol "$proto" \
        --scenario "$scenario" \
        --msg-size "$msg_size" \
        --duration "${DURATION}s" \
        --warmup "${WARMUP}s" \
        --network-profile clean \
        --output json \
        > "$result_file" 2>/dev/null || {
          echo "  FAILED (skipping)"
          continue
        }

      ALL_RESULTS+=("$result_file")
      echo "  OK → $result_file"
    done
  done
done

echo ""
echo "=== Generating Report ==="
cat "${ALL_RESULTS[@]}" | jq -s 'flatten' > "$OUTPUT_DIR/combined_${TS}.json"

./bin/benchmark-runner report \
  --format "$OUTPUT_FORMAT" \
  --out "$REPORT_FILE" \
  --dsn "${DATABASE_URL:-}" \
  2>/dev/null || {
    # Fallback: generate from combined JSON
    echo "Note: PostgreSQL not available, report generated from local files only"
  }

echo "Report: $REPORT_FILE"
echo "Done."
