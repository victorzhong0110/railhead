# 压测记录

这一份是同机、固定延迟、匀速 k6。更接近生产的 Azure trace、多副本和分片计费对照在 [benchmark-realistic.md](benchmark-realistic.md)。两份都只写实测。

数字只来自 2026-09-28 在本机跑的一轮 `bash scripts/run-benchmark.sh`。原文在 `docs/benchmark-raw/`。没有把控制台输出四舍五入成“好看的整数”再当结果；下表的延迟取 k6 summary export 里的 `med` / `p(95)` / `p(99)`，单位毫秒，保留两位。k6 的 `med` 就是 P50。

## 机器和软件

| 项 | 实测 |
|---|---|
| 时间 | 2026-09-28T06:36:43Z 起，整轮约 87 秒 |
| 系统 | Ubuntu 24.04.4 LTS，内核 `6.12.94+` |
| CPU | 4 vCPU，Intel Xeon（KVM，family 6 model 207，每核 1 线程），BogoMIPS 4800 |
| 内存 | 15 GiB，无 swap。压测开始时 available 约 6.6 GiB |
| Go | 1.22.2 |
| k6 | 0.57.0 |
| Docker | 29.1.3，Compose 2.40.3，存储驱动 `vfs` |
| 数据服务 | 容器内 Postgres 16-alpine、Redis 7-alpine |

k6 和 `cmd/benchcheck` 与容器跑在同一台虚拟机上，共用这 4 个核。请求打的是宿主机发布端口（`127.0.0.1:18080`），经过 Docker 的用户态端口代理，再进网关容器。延迟里包含这一跳。

测量时 `net.bridge.bridge-nf-call-iptables` 和 `net.bridge.bridge-nf-call-ip6tables` 为 0，容器之间才能互通。正常安装的 Docker 会自己写桥接放行规则。下面的数字是在这个网络配置下量到的。

## 怎么跑的

`docker compose up` 起来之后，脚本做了这些事：

1. 把两个 mock 的 `error_rate` 设为 0、`latency_ms` 设为 0、`stream_delay_ms` 设为 0。
2. 四段 k6。请求体里 `temperature` 是 0.7，缓存不会命中，测到的是真正打到 mock 再记账的路径。限流和并发上限都是 0（不限制）。
3. `go run ./cmd/benchcheck billing`：新建一把额度只有 500 的密钥，50 并发打 200 个请求，然后 `GET /admin/keys/{id}/reconcile`。
4. `go run ./cmd/benchcheck failover`：主渠道健康时打 20 次；再把 mock-a 的 `error_rate` 设为 1，打 40 次；看响应头 `X-Railhead-Channel`。

k6 的 `http_reqs` 把 setup 阶段创建密钥的请求也算进去了。聊天补全的吞吐用 `iterations`。两者之差正好是创建的密钥数：fast 和 latency20 各 32，single-key 和 stream 各 1。

## 吞吐和延迟

| 场景 | 条件 | 补全 QPS | HTTP 错误率 | P50 | P95 | P99 |
|---|---|---:|---:|---:|---:|---:|
| fast | 32 把密钥，40 VU，30s，mock 延迟 0 | 2974.56 | 0%（89384/89384 检查通过） | 12.80 ms | 19.17 ms | 24.34 ms |
| latency20 | 32 把密钥，20 VU，20s，两个 mock 都注入 20 ms | 797.76 | 0%（15998/15998） | 24.68 ms | 27.53 ms | 30.04 ms |
| single-key | 1 把密钥，20 VU，20s，mock 延迟 0 | 396.70 | 0%（7953/7953） | 40.93 ms | 123.41 ms | 178.70 ms |
| stream | 1 把密钥，10 VU，15s，SSE，`max_tokens=16` | 389.62 | 0%（状态 200 且 body 含 `data: [DONE]`，5853 次） | 11.25 ms | 92.55 ms | 150.67 ms |

对应的 `http_reqs` 速率（含创建密钥）：fast 2975.62/s，latency20 799.36/s，single-key 396.75/s，stream 389.68/s。`http_req_failed` 的 value 都是 0。

fast 的端到端平均延迟是 13.31 ms（min 0.35 ms，max 102.13 ms）。mock 本身不再额外 sleep，这 13 ms 主要是网关记账、Redis 和 Docker 代理。

latency20 的 P50 是 24.68 ms，比注入的 20 ms 多大约 4.7 ms，和 fast 场景里“网关自己的开销”同一量级。

single-key 把同一行 `api_keys` 上的 `SELECT FOR UPDATE` 打满。QPS 从多密钥的约 2975 掉到 396.70，P99 从 24.34 ms 拉到 178.70 ms。这是这个计费模型的预期瓶颈：额度正确性靠行锁，不靠把余额放进 Redis。

stream 的 P50（11.25 ms）低于 single-key 的非流式 P50，但 P99 有 150.67 ms。检查项是整段 SSE 收完并且看到 `[DONE]`，不是首字节时间。

## 计费一致性

`benchcheck billing`，退出码 0。

| 项 | 值 |
|---|---|
| 请求 / 并发 | 200 / 50 |
| 授予额度 | 500 |
| HTTP 200 | 21 |
| HTTP 429 | 179 |
| 结算 token | 483（21 笔） |
| 余额 | 17 |
| 在途预扣 | 0 |
| 未入账（unbilled） | 0 |
| 退款笔数 | 0 |
| drift | 0 |
| 墙钟 | 93 ms |

不变量 `quota_granted = quota_balance + settled + inflight` 在这里是 `500 = 17 + 483 + 0`。余额没有变成负数。多出来的 179 个请求是额度不够被拒绝，不是扣费失败。21 笔成功把额度用到只剩 17，下一笔预扣装不下，于是返回 429。并发下没有超卖。

## 故障转移

`benchcheck failover`，退出码 0。mock-a 是 `mock-primary`（priority 100），mock-b 是 `mock-backup`（priority 10）。

| 阶段 | 请求 | 成功 | 失败 | 渠道 | P50 | P95 | P99 |
|---|---:|---:|---:|---|---:|---:|---:|
| 主渠道 `error_rate=0` | 20 | 20 | 0 | mock-primary 20 | 3 ms | 4 ms | 4 ms |
| 主渠道 `error_rate=1` | 40 | 40 | 0 | mock-backup 40 | 3 ms | 4 ms | 4 ms |

主渠道被打成必失败之后，40 次请求全部落到备份渠道，没有一次仍打在 mock-primary 上成功，也没有一次对客户端失败。延迟分位是 `benchcheck` 自己用整毫秒算的，和 k6 不是同一套统计。

这把密钥授予 5,000,000，60 次成功结算 1,200，余额 4,998,800，在途 0，unbilled 0，drift 0。失败的上游尝试没有留下未退的预扣。

## 复现

```bash
docker compose up --build -d
BASE_URL=http://127.0.0.1:18080 \
ADMIN_TOKEN=dev-admin-token \
MOCK_A=http://127.0.0.1:18091 \
MOCK_B=http://127.0.0.1:18092 \
  bash scripts/run-benchmark.sh
```

脚本结束时会把两个 mock 的错误率和延迟设回 0。换机器之后数字会变，以新一轮 `docs/benchmark-raw/` 为准，不要沿用本页的数。
