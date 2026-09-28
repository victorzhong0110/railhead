# Railhead

Railhead 是一家 SaaS 公司内部 AI 能力平台的模型接入层。客服、文档、代码助手等业务线共用这一层：每条业务线使用自己的 API Key 调用 OpenAI 兼容的 `/v1/chat/completions`（普通 JSON 和 SSE 流式）。网关按密钥做额度与计费、RPM/TPM 限流，并在多个模型供应商渠道之间自动故障转移。

同一条路径上还有鉴权、并发限制、失败重试、熔断，以及可选的精确响应缓存。设计说明见 [docs/design.md](docs/design.md)。开源网关只作为设计对照，见文末 References。

Go 模块路径是 [`github.com/victorzhong0110/railhead`](https://github.com/victorzhong0110/railhead)。

压测数字：[docs/benchmark.md](docs/benchmark.md)（同机匀速 k6），[docs/benchmark-realistic.md](docs/benchmark-realistic.md)（Azure trace、多副本、分片计费对照），[docs/benchmark-real-upstream.md](docs/benchmark-real-upstream.md)（MiniMax 真实上游）。数字都来自这些文档里的实测。

## 能做什么

- `POST /v1/chat/completions`：非流式 JSON，以及 `stream: true` 的 SSE
- 渠道适配：OpenAI 兼容 HTTP（`base_url` 可配，因此 DeepSeek、vLLM、Ollama 和本仓库的 mock 都走同一条适配器），另有进程内 mock
- API Key 鉴权。明文只在创建时返回一次，库里存 sha256
- 每把密钥的令牌桶限流（RPM 和 TPM）。Redis Lua 原子更新，Redis 失败时退回本进程内存
- 每把密钥的并发上限。Redis ZSET 租约，进程崩溃后租约过期自动释放
- Token 计费：请求前预扣，成功后按上游 usage 结算，失败退款。余额拆在 `quota_shards`，同一分片上的预扣可以按几毫秒合并提交。`CHECK (balance >= 0)` 保证不把分片打成负数，并有对账接口
- 重试、跨渠道故障转移、每渠道熔断、超时
- 可选的精确响应缓存（仅非流式且 `temperature = 0`，按密钥隔离）
- Prometheus `/metrics` 和 JSON 结构化日志
- `docker compose up` 同时拉起网关、Postgres、Redis、两个 mock

限额设为 `0` 表示不限制。这是故意的：压测网关本身时不要让限流先把请求拒绝掉。

## 本地运行

```bash
docker compose up --build
```

默认端口：

| 服务 | 地址 |
|---|---|
| 网关 | http://127.0.0.1:18080 |
| mock-a（高优先级） | http://127.0.0.1:18091 |
| mock-b（备用） | http://127.0.0.1:18092 |
| Postgres | 127.0.0.1:15432，用户/密码/库都是 `railhead` |
| Redis | 127.0.0.1:16379 |

管理令牌是 `dev-admin-token`，只适合本机。种子密钥是 `sk-railhead-demo`。

```bash
curl -s http://127.0.0.1:18080/v1/chat/completions \
  -H 'Authorization: Bearer sk-railhead-demo' \
  -H 'Content-Type: application/json' \
  -d '{"model":"railhead-mock","messages":[{"role":"user","content":"hello"}],"max_tokens":16}'
```

流式把 `stream` 设为 `true`。响应头 `X-Railhead-Channel` 是实际命中的渠道，`X-Railhead-Cache` 是 `HIT`、`MISS` 或 `BYPASS`。

创建一把新密钥：

```bash
curl -s http://127.0.0.1:18080/admin/keys \
  -H 'X-Admin-Token: dev-admin-token' \
  -H 'Content-Type: application/json' \
  -d '{"name":"alice","quota":100000,"rpm_limit":60,"tpm_limit":20000,"concurrency_limit":8}'
```

对账（`drift` 必须是 0）：

```bash
curl -s http://127.0.0.1:18080/admin/keys/1/reconcile \
  -H 'X-Admin-Token: dev-admin-token'
```

把 mock-a 的错误率打到 100%，用来看故障转移：

```bash
curl -s http://127.0.0.1:18091/admin/fault \
  -H 'Content-Type: application/json' \
  -d '{"error_rate":1,"latency_ms":0}'
```

mock 的 `/admin/fault` 没有鉴权，它只是测试替身，不要暴露到公网。

## 配置

环境变量见 [configs/example.env](configs/example.env)。渠道用 JSON 文件或 `CHANNELS_JSON`，字段是 `name`、`kind`（`openai` 或 `mock`）、`base_url`、`api_key`、`models`、`priority`、`weight`、`timeout_ms`。`priority` 越大越优先，同一优先级里按 `weight` 加权随机。

## 测试和压测

```bash
export DATABASE_URL='postgres://railhead:railhead@127.0.0.1:5432/railhead?sslmode=disable'
export REDIS_URL='redis://127.0.0.1:6379/0'
go test -count=1 -race ./...
```

没有 `DATABASE_URL` 时，需要数据库的测试会跳过，纯内存测试仍然运行。CI 用 GitHub Actions 起 Postgres 和 Redis，见 `.github/workflows/ci.yml`。

压测脚本：

```bash
# 先让网关和两个 mock 起来，然后：
BASE_URL=http://127.0.0.1:18080 \
ADMIN_TOKEN=dev-admin-token \
MOCK_A=http://127.0.0.1:18091 \
MOCK_B=http://127.0.0.1:18092 \
  bash scripts/run-benchmark.sh
```

k6 脚本在 `scripts/k6/`。`cmd/benchcheck` 负责两件 k6 不方便做精确的事：并发扣费后的对账，以及把主渠道错误率拉高后的故障转移统计。

## 代码地图

| 路径 | 职责 |
|---|---|
| `cmd/gateway` | 组装进程：迁移、种子数据、后台刷新和清扫 |
| `cmd/mockprovider` | 可注入延迟和错误率的假上游 |
| `cmd/benchcheck` | 计费对账和故障转移检查 |
| `internal/httpserver` | HTTP、鉴权、请求生命周期 |
| `internal/billing` | 预扣、结算、退款、对账 |
| `internal/ratelimit` | 令牌桶和并发租约 |
| `internal/router` | 选渠道、重试、故障转移 |
| `internal/breaker` | 每渠道熔断器 |
| `internal/provider/openai` | OpenAI 兼容 HTTP 适配器 |
| `internal/provider/mock` | 假模型，网关和 mock 进程共用 |
| `internal/cache` | 精确响应缓存 |
| `internal/store` | 建表、密钥、渠道 |

## 已知边界

- 只实现了 Chat Completions 的文本子集，没有图片、tools、embeddings
- 预扣用的是一个很浅的估算器（约 2 个码点 1 个 token），和真实 tokenizer 会有偏差。`RESERVE_MARGIN` 用来留余量；和 mock 对齐时可以设为 0。结算以供应商返回的 usage 为准，不够补扣时也不会把余额打成负数，差额记在 `unbilled`
- 熔断器在每个网关进程内存里，多副本之间不共享
- 响应缓存是精确匹配，不是语义缓存
- 没有做调用方的 Idempotency-Key。网关内部的重试共用同一次预扣，不会重复扣费；客户端自己超时重试是一次新请求
- mock 的管理接口没有鉴权

---

# Railhead (English)

Railhead is the model access layer for a SaaS company's internal AI platform. Support, documentation, and coding assistants share it. Each line of business calls the OpenAI Chat Completions API, including server-sent events, with its own API key. The gateway meters token quota, rate-limits the key, and fails over across model-provider channels.

The Go module path is [`github.com/victorzhong0110/railhead`](https://github.com/victorzhong0110/railhead).

- Design: [docs/design.md](docs/design.md) (Chinese)
- Measured load test: [docs/benchmark.md](docs/benchmark.md) (same-box constant-rate k6), [docs/benchmark-realistic.md](docs/benchmark-realistic.md) (Azure trace, three gateways, sharded billing), and [docs/benchmark-real-upstream.md](docs/benchmark-real-upstream.md) (MiniMax)

## Run

```bash
docker compose up --build
```

The gateway listens on http://127.0.0.1:18080. A demo key `sk-railhead-demo` and admin token `dev-admin-token` are seeded for local use only. Two mock upstreams are included so failover can be exercised without calling a paid model.

```bash
curl -s http://127.0.0.1:18080/v1/chat/completions \
  -H 'Authorization: Bearer sk-railhead-demo' \
  -H 'Content-Type: application/json' \
  -d '{"model":"railhead-mock","messages":[{"role":"user","content":"hello"}],"max_tokens":16}'
```

Set `stream` to `true` for SSE. `X-Railhead-Channel` is the channel that served the request.

## Test

```bash
export DATABASE_URL='postgres://railhead:railhead@127.0.0.1:5432/railhead?sslmode=disable'
export REDIS_URL='redis://127.0.0.1:6379/0'
go test -count=1 -race ./...
```

`bash scripts/run-benchmark.sh` runs k6 plus a billing reconciliation and a failover check against a gateway that is already up. Numbers in `docs/benchmark.md` come from that run. `bash scripts/run-realistic.sh` replays the committed Azure excerpt `testdata/azure_llm_2024_conv_sample.csv` through nginx and three gateways; numbers in `docs/benchmark-realistic.md` come from that run and the follow-up stress, TTFT, and local-model checks. The full conversation trace is about 1.1 GB and is not in this repository. `bash scripts/download-azure-trace.sh` downloads it to a gitignored path.

A limit of `0` means unlimited. Quota is reserved on a `quota_shards` row before the upstream call and settled afterwards. Each shard has `CHECK (balance >= 0)`. With `BILLING_SHARDS=1` and `BILLING_BATCH=0` every reservation hits one row. `docs/benchmark-realistic.md` compares that setting with 32 shards and a 2 ms batch window.

## References

| Project | License | Design reference |
|---|---|---|
| [songquanpeng/one-api](https://github.com/songquanpeng/one-api) | MIT | Channel, token, and quota; pre-consume then settle; priority and retry across channels |
| [BerriAI/litellm](https://github.com/BerriAI/litellm) | NOASSERTION (core is MIT; the repo also contains other terms) | A single provider interface, router fallback, per-key budgets and rate limits, response cache |
| [maximhq/bifrost](https://github.com/maximhq/bifrost) | Apache-2.0 | A Go gateway with a small provider interface, per-upstream health, and a published benchmark method |
| [QuantumNous/new-api](https://github.com/QuantumNous/new-api) | AGPL-3.0 | Out of scope for this repository. Multi-protocol translation (Claude, Gemini) is not implemented |

Related: [higress](https://github.com/higress-group/higress) (token rate limiting at the edge) and [grafana/k6](https://github.com/grafana/k6) (the load generator used in `scripts/k6`).
