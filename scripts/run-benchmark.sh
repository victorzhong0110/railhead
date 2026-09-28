#!/usr/bin/env bash
# 对已经启动的网关和两台 mock 跑一轮压测，把 k6 原文和 benchcheck JSON 写到 docs/benchmark-raw。
# 不负责启动进程。docker compose 或本机二进制都可以，只要下面这几个地址通。
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
OUT="${ROOT}/docs/benchmark-raw"
mkdir -p "$OUT"

BASE_URL="${BASE_URL:-http://127.0.0.1:18080}"
ADMIN_TOKEN="${ADMIN_TOKEN:-dev-admin-token}"
MOCK_A="${MOCK_A:-http://127.0.0.1:18091}"
MOCK_B="${MOCK_B:-http://127.0.0.1:18092}"
MODEL="${MODEL:-railhead-mock}"

{
  echo "date: $(date -u +%Y-%m-%dT%H:%M:%SZ)"
  echo "uname: $(uname -a)"
  echo "nproc: $(nproc)"
  echo "go: $(go version)"
  echo "k6: $(k6 version)"
  lscpu | sed -n '1,15p'
  free -h
} | tee "$OUT/machine.txt"

set_fault() {
  local url="$1" rate="$2" latency="$3"
  curl -fsS -X POST "$url/admin/fault" \
    -H 'Content-Type: application/json' \
    -d "{\"error_rate\":${rate},\"latency_ms\":${latency},\"stream_delay_ms\":0}" >/dev/null
}

wait_ready() {
  local url="$1"
  for _ in $(seq 1 50); do
    if curl -fsS "$url" >/dev/null 2>&1; then
      return 0
    fi
    sleep 0.2
  done
  echo "not ready: $url" >&2
  return 1
}

wait_ready "$BASE_URL/healthz"
wait_ready "$MOCK_A/healthz"
wait_ready "$MOCK_B/healthz"
set_fault "$MOCK_A" 0 0
set_fault "$MOCK_B" 0 0

run_k6() {
  local name="$1"
  shift
  echo "=== k6 $name ==="
  k6 run --summary-trend-stats "avg,min,med,max,p(90),p(95),p(99)" \
    --summary-export "$OUT/${name}.json" \
    "$@" | tee "$OUT/${name}.txt"
}

run_k6 fast "$ROOT/scripts/k6/completions.js" \
  -e BASE_URL="$BASE_URL" -e ADMIN_TOKEN="$ADMIN_TOKEN" -e MODEL="$MODEL" \
  -e KEY_COUNT=32 -e VUS=40 -e DURATION=30s -e MAX_TOKENS=16 -e QUOTA=5000000

set_fault "$MOCK_A" 0 20
set_fault "$MOCK_B" 0 20
run_k6 latency20 "$ROOT/scripts/k6/completions.js" \
  -e BASE_URL="$BASE_URL" -e ADMIN_TOKEN="$ADMIN_TOKEN" -e MODEL="$MODEL" \
  -e KEY_COUNT=32 -e VUS=20 -e DURATION=20s -e MAX_TOKENS=16 -e QUOTA=5000000

set_fault "$MOCK_A" 0 0
set_fault "$MOCK_B" 0 0
run_k6 single-key "$ROOT/scripts/k6/completions.js" \
  -e BASE_URL="$BASE_URL" -e ADMIN_TOKEN="$ADMIN_TOKEN" -e MODEL="$MODEL" \
  -e KEY_COUNT=1 -e VUS=20 -e DURATION=20s -e MAX_TOKENS=16 -e QUOTA=5000000

run_k6 stream "$ROOT/scripts/k6/stream.js" \
  -e BASE_URL="$BASE_URL" -e ADMIN_TOKEN="$ADMIN_TOKEN" -e MODEL="$MODEL" \
  -e VUS=10 -e DURATION=15s -e MAX_TOKENS=16 -e QUOTA=5000000

echo "=== billing ==="
go run ./cmd/benchcheck billing \
  -gateway "$BASE_URL" -admin "$ADMIN_TOKEN" -model "$MODEL" \
  -concurrency 50 -requests 200 -quota 500 -max-tokens 8 \
  | tee "$OUT/billing.json"

echo "=== failover ==="
go run ./cmd/benchcheck failover \
  -gateway "$BASE_URL" -admin "$ADMIN_TOKEN" -mock-a "$MOCK_A" -model "$MODEL" \
  -before 20 -after 40 \
  | tee "$OUT/failover.json"

set_fault "$MOCK_A" 0 0
set_fault "$MOCK_B" 0 0
echo "benchmark raw output is in $OUT"
