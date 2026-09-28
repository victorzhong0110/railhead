#!/usr/bin/env bash
# 现实负载：Azure trace 回放，三网关 + nginx，限 CPU。
# 不负责解释数字。原文落到 docs/benchmark-realistic-raw/。
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
OUT="${ROOT}/docs/benchmark-realistic-raw"
mkdir -p "$OUT"
cd "$ROOT"

COMPOSE=(docker compose -p railhead-realistic -f docker-compose.realistic.yml)
if ! docker info >/dev/null 2>&1; then
  COMPOSE=(sudo docker compose -p railhead-realistic -f docker-compose.realistic.yml)
fi

BASE_URL="${BASE_URL:-http://127.0.0.1:18083}"
ADMIN_TOKEN="${ADMIN_TOKEN:-dev-admin-token}"
MOCK_A="${MOCK_A:-http://127.0.0.1:18093}"
MOCK_B="${MOCK_B:-http://127.0.0.1:18094}"
TRACE="${TRACE:-testdata/azure_llm_2024_conv_sample.csv}"

{
  echo "date: $(date -u +%Y-%m-%dT%H:%M:%SZ)"
  echo "uname: $(uname -s) $(uname -r) $(uname -m)"
  nproc
  go version
  free -h
  lscpu | sed -n '1,18p'
  "${COMPOSE[@]}" version || true
} | tee "$OUT/machine.txt"

wait_ready() {
  local url="$1"
  for _ in $(seq 1 80); do
    if curl -fsS "$url" >/dev/null 2>&1; then
      return 0
    fi
    sleep 0.5
  done
  echo "not ready: $url" >&2
  return 1
}

stats_once() {
  local name="$1"
  if docker info >/dev/null 2>&1; then
    docker stats --no-stream --format '{{.Name}} {{.CPUPerc}} {{.MemUsage}}' > "$OUT/${name}-docker-stats.txt" || true
  else
    sudo docker stats --no-stream --format '{{.Name}} {{.CPUPerc}} {{.MemUsage}}' > "$OUT/${name}-docker-stats.txt" || true
  fi
}

recreate_gateways() {
  echo "=== recreate gateways BILLING_SHARDS=${BILLING_SHARDS} BILLING_BATCH=${BILLING_BATCH} ==="
  "${COMPOSE[@]}" up -d --force-recreate --no-deps gateway-1 gateway-2 gateway-3 nginx
  wait_ready "$BASE_URL/healthz"
}

replay() {
  local name="$1"
  shift
  echo "=== replay $name ==="
  go run ./cmd/replay \
    -trace "$TRACE" -gateway "$BASE_URL" -admin "$ADMIN_TOKEN" \
    "$@" | tee "$OUT/${name}.json"
}

# 默认先按「修复后」把栈拉起来，镜像只构建一次。
export BILLING_SHARDS="${BILLING_SHARDS_BOOT:-32}"
export BILLING_BATCH="${BILLING_BATCH_BOOT:-2ms}"
"${COMPOSE[@]}" up -d --build
wait_ready "$BASE_URL/healthz"
wait_ready "$MOCK_A/healthz"
wait_ready "$MOCK_B/healthz"
{
  echo "bridge_nf_call_iptables=$(sysctl -n net.bridge.bridge-nf-call-iptables 2>/dev/null || true)"
  for c in postgres redis mock-a mock-b gateway-1 gateway-2 gateway-3 nginx; do
    id="$("${COMPOSE[@]}" ps -q "$c" || true)"
    if [[ -n "${id}" ]]; then
      echo -n "$c "
      docker inspect "$id" --format 'NanoCpus={{.HostConfig.NanoCpus}} Memory={{.HostConfig.Memory}}' 2>/dev/null \
        || sudo docker inspect "$id" --format 'NanoCpus={{.HostConfig.NanoCpus}} Memory={{.HostConfig.Memory}}'
    fi
  done
} | tee "$OUT/resource-limits.txt"
"${COMPOSE[@]}" exec -T postgres psql -U railhead -d railhead -c 'SHOW max_connections;' > "$OUT/postgres-max-connections.txt" || true

# 改前：一分片、不合并。同一段 trace、同一把密钥，把行锁打满。
export BILLING_SHARDS=1
export BILLING_BATCH=0s
recreate_gateways
replay before-contention -speed 20 -duration 3m -keys 1 -heavy 0 -stream-frac 0.6 -inflight 1500 -quota 50000000
stats_once before-contention

# 改后：32 分片，2ms 合并提交。
export BILLING_SHARDS=32
export BILLING_BATCH=2ms
recreate_gateways
replay after-contention -speed 20 -duration 3m -keys 1 -heavy 0 -stream-frac 0.6 -inflight 1500 -quota 50000000
stats_once after-contention

# 多租户 Zipf。最重的两把密钥 RPM=120，用来打出限流。
replay mixed-after -speed 20 -duration 3m -keys 64 -zipf 1 -heavy 2 -heavy-rpm 120 -stream-frac 0.6 -inflight 800 -quota 50000000
stats_once mixed-after

export BILLING_SHARDS=1
export BILLING_BATCH=0s
recreate_gateways
replay mixed-before -speed 20 -duration 3m -keys 64 -zipf 1 -heavy 2 -heavy-rpm 120 -stream-frac 0.6 -inflight 800 -quota 50000000
stats_once mixed-before

# 不加速，按 trace 原始到达间隔跑前 90 秒。用修复后的配置。
export BILLING_SHARDS=32
export BILLING_BATCH=2ms
recreate_gateways
replay native-1x -speed 1 -duration 90s -keys 64 -zipf 1 -heavy 2 -heavy-rpm 120 -stream-frac 0.6 -inflight 200 -quota 50000000
stats_once native-1x

# 故障注入。回放在后台跑，脚本记下动作的墙钟。
echo "=== chaos ==="
export BILLING_SHARDS=32
export BILLING_BATCH=2ms
recreate_gateways
# 把主渠道错误率先收回配置值，避免上一轮留下的故障。
curl -fsS -X POST "$MOCK_A/admin/fault" -H 'Content-Type: application/json' \
  -d '{"rate_500":0.003,"rate_429":0.002,"rate_timeout":0.001}' >/dev/null
curl -fsS -X POST "$MOCK_B/admin/fault" -H 'Content-Type: application/json' \
  -d '{"rate_500":0.003,"rate_429":0.002,"rate_timeout":0.001}' >/dev/null

go run ./cmd/replay \
  -trace "$TRACE" -gateway "$BASE_URL" -admin "$ADMIN_TOKEN" \
  -speed 15 -duration 3m -wall 50s -keys 32 -zipf 1 -heavy 1 -heavy-rpm 300 \
  -stream-frac 0.5 -inflight 800 -quota 50000000 \
  -events "$OUT/chaos-events.jsonl" > "$OUT/chaos.json" &
REPLAY_PID=$!
sleep 6
python3 - <<'PY' >> "$OUT/chaos-marks.jsonl"
import json,time
print(json.dumps({"event":"mark_ready","unix_ms":int(time.time()*1000)}))
PY
# 记录回放进程的大致起点：chaos.json 还没写完，用 marks 的相对时间。
MARK_BASE=$(date +%s%3N)
echo "{\"event\":\"clock\",\"unix_ms\":$MARK_BASE}" >> "$OUT/chaos-marks.jsonl"
sleep 4
echo "{\"event\":\"stop_gateway_2\",\"unix_ms\":$(date +%s%3N)}" >> "$OUT/chaos-marks.jsonl"
"${COMPOSE[@]}" stop gateway-2
sleep 8
echo "{\"event\":\"start_gateway_2\",\"unix_ms\":$(date +%s%3N)}" >> "$OUT/chaos-marks.jsonl"
"${COMPOSE[@]}" start gateway-2
sleep 8
echo "{\"event\":\"restart_redis\",\"unix_ms\":$(date +%s%3N)}" >> "$OUT/chaos-marks.jsonl"
"${COMPOSE[@]}" restart redis
sleep 8
echo "{\"event\":\"degrade_mock_a\",\"unix_ms\":$(date +%s%3N)}" >> "$OUT/chaos-marks.jsonl"
curl -fsS -X POST "$MOCK_A/admin/fault" -H 'Content-Type: application/json' -d '{"rate_500":1}' >/dev/null
sleep 8
echo "{\"event\":\"restore_mock_a\",\"unix_ms\":$(date +%s%3N)}" >> "$OUT/chaos-marks.jsonl"
curl -fsS -X POST "$MOCK_A/admin/fault" -H 'Content-Type: application/json' -d '{"rate_500":0.003}' >/dev/null
wait "$REPLAY_PID" || true
stats_once chaos
# 收尾，避免错误率留在 mock 上。
curl -fsS -X POST "$MOCK_A/admin/fault" -H 'Content-Type: application/json' -d '{"rate_500":0.003}' >/dev/null || true
echo "realistic raw output is in $OUT"
