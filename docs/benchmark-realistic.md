# 更接近生产的压测

数字只来自 2026-09-28 在本机跑的回放和故障注入。汇总 JSON、`chaos-marks.jsonl` 和 `docker stats` 原文在 `docs/benchmark-realistic-raw/`。延迟百分位用 `cmd/replay` 的算法：排序后取下标 `int((n-1)*p)`。下表把毫秒保留三位、QPS 保留两位，完整浮点在对应 JSON 里。没有把没跑过的项填成估计值。

同机匀速 k6 仍在 [benchmark.md](benchmark.md)，没有改那些数字。

## 机器

| 项 | 实测 |
|---|---|
| 时间 | 套件从 `docs/benchmark-realistic-raw/machine.txt` 的 2026-09-28T07:05:00Z 开始。60 倍速和 TTFT 是随后单独一轮。Ollama 对照在同一天下午，模型拉取完成后立刻测 |
| 系统 | Linux `6.12.94+`，x86_64 |
| CPU | 4 vCPU，Intel Xeon（KVM，family 6 model 207，每核 1 线程），BogoMIPS 4800 |
| 内存 | 15 GiB，无 swap。套件开始时 available 约 6.2 GiB |
| Go | 1.22.2 |
| Docker | Compose 2.40.3，存储驱动 `vfs` |
| 入口 | 回放打 `http://127.0.0.1:18083`，经 docker-proxy 进 nginx，再进网关容器 |

这台机器上的 dockerd 用 `--iptables=false` 启动，`iptables-legacy` 的 FORWARD 默认 DROP。测量时 `net.bridge.bridge-nf-call-iptables=0`（记在 `resource-limits.txt`）。这是环境补丁，不是产品行为。正常安装的 Docker 会自己写桥接放行规则。

回放进程在宿主机上。下面这些容器有 cgroup 上限，和回放进程不在同一个配额里。`docker stats` 的 CPU% 大约是「占一颗核的百分比」，所以 35% 就是顶到了 0.35 核的配额。

| 容器 | CPU | 内存 |
|---|---|---|
| gateway ×3 | 0.35 核（NanoCpus 350000000） | 256 MiB |
| postgres | 0.70 核 | 512 MiB |
| redis | 0.20 核 | 128 MiB |
| nginx | 0.20 核 | 64 MiB |
| mock ×2 | 0.35 核 | 128 MiB |

合计约 2.85 核，留给宿主机回放和操作系统大约 1 核。Postgres `max_connections=120`，`shared_buffers=128MB`。Redis 关闭 AOF，`maxmemory 96mb`。

`scripts/run-realistic.sh` 里的 `docker stats` 是每轮回放**结束之后**才采的，那些 `before-contention-docker-stats.txt`、`mixed-*-docker-stats.txt`、`native-1x-docker-stats.txt`、`chaos-docker-stats.txt` 是空闲值，不能当成峰值。60 倍速那一轮是在回放开始约 2 秒时采样的，可以当作负载中的 CPU。

## 流量从哪来

来源：[Azure Public Dataset，Azure LLM inference trace 2024](https://github.com/Azure/AzurePublicDataset/blob/master/AzureLLMInferenceDataset2024.md)，文件 `AzureLLMInferenceTrace_conv_1week.csv`（对话服务）。许可是 [CC BY 4.0](https://creativecommons.org/licenses/by/4.0/)。采集窗口 2024-05-10 到 2024-05-19，论文是 Stojkovic 等，DynamoLLM，HPCA 2025。完整 CSV 约 1.1 GB，不进仓库。`bash scripts/download-azure-trace.sh` 会把它下载到被 `.gitignore` 忽略的路径。本仓库只提交切片 `testdata/azure_llm_2024_conv_sample.csv`。说明在 `testdata/AZURE_TRACE.md`。

切片：2024-05-12 01:00:00.088263Z 到 01:03:00.000391Z，跨度 179.912 秒，4706 行。列只有 `TIMESTAMP`、`ContextTokens`、`GeneratedTokens`。没有 prompt 文本，也没有租户 id。按秒分桶共 181 个非空秒，每秒 1 到 37 条，这 181 秒的均值是 26.00。

| | p50 | p90 | 最大 | 均值 |
|---|---|---|---|---|
| ContextTokens | 1040 | 3801 | 7961 | 1675.85 |
| GeneratedTokens | 41 | 402 | 1159 | 119.39 |

`cmd/replay` 用 TIMESTAMP 的间隔做开环发送，`--speed` 把间隔除掉。prompt 按本仓库估算器垫到 ContextTokens（一条 user 消息：`CountPrompt = 8 + CountText`）。`max_tokens` 设成 GeneratedTokens。`temperature=0.7`，现实拓扑里 `CACHE_ENABLED=false`，避免缓存把延迟藏掉。trace 没有租户，密钥按 Zipf 现分配。

在途请求数达到 `-inflight` 时，调度器会停下来等，所以系统排队之后，实测 `offered_qps` 会低于 `speed × 26`。20 倍速的理论上限大约是 523 QPS，下面实测只有 437 到 469。60 倍速的理论上限大约是 1569 QPS，实测大约 536。trace 的形状只在系统跟得上时才保持原样。客户端超时 45 秒。

## 上游和拓扑

mock 在 `TTFT_MS`、`DECODE_US` 或错误率大于 0 时走生成式延迟：先等 TTFT，再按输出 token 数乘解码时间。流式是先等 TTFT，然后逐 token 吐。以 `TAIL_PROB` 的概率把 TTFT 乘上 `TAIL_FACTOR`。429、500、超时各自独立掷骰子。这一轮的环境变量：

| 变量 | 值 |
|---|---|
| TTFT_MS | 25 |
| DECODE_US | 200 |
| TAIL_PROB | 0.02 |
| TAIL_FACTOR | 8 |
| RATE_429 | 0.002 |
| RATE_500 | 0.003 |
| RATE_TIMEOUT | 0.001 |
| TIMEOUT_MS | 1500 |

成功响应带 `X-Mock-TTFT-Us` 和 `X-Mock-Upstream-Us`。开销 = 客户端耗时 − `X-Mock-Upstream-Us`。流式 TTFT 开销 = 读到第一块的时间 − `X-Mock-TTFT-Us`。

20 倍速和 1 倍速那几轮，流式响应没有转出 `X-Mock-TTFT-Us` 和 `X-Mock-Upstream-Us`：`router.boundStream` 嵌了 `provider.Stream`，类型断言拿不到 `Header`。所以那几轮的 `overhead_ms.n` 只覆盖非流式（大约四成，`stream-frac=0.6`），`ttft_overhead_ms.n` 是 0。60 倍速和全流式 TTFT 是转发这两个头之后的结果。混沌那一轮没有流式 TTFT 开销，下面不补一个数。

拓扑是 `docker-compose.realistic.yml`：nginx `least_conn` 后面三台网关。`max_fails=2`、`fail_timeout=5s`，`proxy_next_upstream` 包含 error、timeout、502、504，`proxy_buffering off`。渠道 `mock-primary` 优先级 100，`mock-backup` 优先级 10。熔断阈值 5，冷却 15 秒，状态在每个网关进程自己的内存里。

## 计费对照

热路径原来把一把密钥的余额打在一行上。现在余额在 `quota_shards`，`Reserve` 从请求号哈希挑一个分片，条件更新 `balance >= amount`，放不下就试下一个。单行都放不下但总和够时，按分片序号锁住全部分片再拆开扣。`BILLING_BATCH` 把同一 `(key, shard)` 上几毫秒内的预扣合成一次提交，提交成功才把结果交回调用方。`BILLING_SHARDS=1` 且 `BILLING_BATCH=0` 就是旧路径。

不变量：

```text
quota_granted = SUM(quota_shards.balance) + Σ settled.charged + Σ reserved.reserved
```

每一轮结束都对全部密钥做对账。下面所有轮次的 `billing_drift_sum` 都是 0，负余额密钥数都是 0。

### 单密钥，20 倍速

4706 个请求，1 把密钥，不设 RPM，`stream-frac=0.6`，`inflight=1500`，额度 5e7。

| | 1 分片，不合并 | 32 分片，2ms 合并 |
|---|---|---|
| 墙钟 | 10.777 s | 10.031 s |
| offered = goodput | 436.67 QPS | 469.16 QPS |
| 成功 token/s | 779074 | 842247 |
| 成功 / 错误 | 4706 / 0 | 4706 / 0 |
| 延迟 p50 / p95 / p99 | 229.929 / 2330.203 / 3851.327 ms | 180.800 / 1196.422 / 1955.157 ms |
| 非流式开销 p50 / p95 / p99 | 109.884 / 450.576 / 744.817 ms（n=1925） | 86.688 / 272.121 / 381.174 ms（n=1925） |
| 成功渠道 | primary 4671，backup 35 | primary 4683，backup 23 |
| drift | 0 | 0 |

原文：`before-contention.json`、`after-contention.json`。

### 64 把密钥，Zipf s=1，20 倍速

最重的 2 把密钥 RPM=120，`inflight=800`。成功数两边都是 3495，429 都是 1211。这些 429 是网关 RPM，不是 mock 的 0.2% 429。

| | 1 分片，不合并 | 32 分片，2ms 合并 |
|---|---|---|
| 墙钟 | 10.291 s | 9.669 s |
| offered QPS | 457.29 | 486.69 |
| goodput QPS | 339.62 | 361.45 |
| 成功 token/s | 601873 | 640185 |
| 延迟 p50 / p95 / p99 | 108.959 / 544.415 / 862.902 ms | 93.702 / 478.605 / 752.838 ms |
| 非流式开销 p50 / p95 / p99 | 46.501 / 190.780 / 291.806 ms（n=1427） | 37.363 / 84.415 / 99.757 ms（n=1429） |
| drift | 0 | 0 |

原文：`mixed-before.json`、`mixed-after.json`。两边都是独立重建网关之后的整段 trace。

### 不加速，前 90 秒

32 分片、2ms 合并，64 把密钥，Zipf s=1，最重 2 把 RPM=120。trace 只放了 90 秒，2374 个请求，墙钟 90.396 s。

| 项 | 值 |
|---|---|
| offered / goodput | 26.26 / 24.06 QPS |
| 成功 token/s | 42288 |
| 成功 / 429 | 2175 / 199 |
| 延迟 p50 / p95 / p99 | 53.695 / 483.088 / 743.175 ms |
| 非流式开销 p50 / p95 / p99 | 7.396 / 9.588 / 10.440 ms（n=894） |
| 渠道 | primary 2158，backup 17 |
| drift | 0 |

原文：`native-1x.json`。这一档系统跟得上 trace，非流式网关开销大约是 7.4 ms（p50）到 10.4 ms（p99）。p95/p99 的端到端延迟仍然受输出长度和 2% 长尾 TTFT 支配，不是这 10 毫秒。

### 60 倍速

1 把密钥，`stream-frac=0.6`，`inflight=1500`，额度 8e7。开销样本覆盖全部 4706 个成功请求，TTFT 开销覆盖 2781 个流式成功。两边 drift 都是 0，没有非 200。

| | 1 分片，不合并 | 32 分片，2ms 合并 |
|---|---|---|
| 墙钟 | 8.755 s | 8.783 s |
| goodput | 537.55 QPS | 535.79 QPS |
| 成功 token/s | 885831 | 857026 |
| 延迟 p50 / p95 / p99 | 1300.807 / 5124.608 / 5501.417 ms | 1065.467 / 5339.775 / 5519.722 ms |
| 开销 p50 / p95 / p99 | 1254.705 / 4998.780 / 5387.349 ms | 1019.772 / 5246.759 / 5455.431 ms |
| TTFT 开销 p50 / p95 / p99 | 663.094 / 3575.820 / 3789.468 ms | 493.161 / 4824.039 / 5193.007 ms |
| 渠道 | primary 4659，backup 47 | primary 4634，backup 72 |

回放开始约 2 秒时的 `docker stats`：

| 容器 | 1 分片 | 32 分片 |
|---|---|---|
| gateway-1 / 2 / 3 | 34.75% / 34.58% / 27.33% | 34.40% / 30.37% / 26.72% |
| mock-a | 33.81% | 36.53% |
| mock-b | 3.73% | 1.89% |
| postgres | 45.80% | 40.18% |
| nginx | 18.99% | 20.45% |
| redis | 1.77% | 0.29% |

网关和 mock-a 的 CPU% 已经贴着 0.35 核的配额，nginx 贴着 0.20 核。Postgres 大约 0.46 核，还没顶到 0.70。Redis 几乎空闲。

### 没排队时的流式 TTFT

32 分片，5 倍速，只放 30 秒，16 把密钥，Zipf s=0.8，`stream-frac=1`，不设重密钥，`inflight=200`。763 个请求全部 200，墙钟 6.903 s，goodput 110.54 QPS，drift 0。primary 761，backup 2。

| | p50 | p95 | p99 | min | max |
|---|---|---|---|---|---|
| 端到端 | 69.536 | 450.753 | 695.341 | 33.728 | 1562.182 |
| 整段开销（n=763） | 35.087 | 326.445 | 495.310 | 8.128 | 1532.782 |
| TTFT 开销（n=763） | 6.771 | 17.856 | 38.291 | 4.252 | 1509.092 |

原文：`ttft-stream.json`。整段开销的尾巴是输出长度：网关要等上游把 token 吐完再结束请求，TTFT 之后的时间大部分在上游。首 token 穿过网关的附加延迟，在这一档是 p50 6.771 ms、p99 38.291 ms。最大的 1509 ms 是单点，不是这一档的典型值。

## 瓶颈在哪

20 倍速、单密钥时，一行余额看得到：p99 从 3851 ms 降到 1955 ms，非流式开销 p99 从 745 ms 降到 381 ms，goodput 从 436.67 升到 469.16。多密钥那一轮开销 p99 从 292 ms 降到 100 ms。drift 两边都是 0。

60 倍速时分片不再增加 QPS（537.55 对 535.79）。p50 延迟和 p50 TTFT 开销下来了，p99 没有。CPU 配额先满：三台网关、主 mock、nginx 都贴着自己的 cgroup。分片消的是中等压力下的尾延迟，不是配额打满之后的吞吐。

## 故障

32 分片，15 倍速，`-wall 50s`，trace 重复 5 次，32 把密钥，Zipf s=1，最重 1 把 RPM=300，`stream-frac=0.5`。整轮 23530 个请求，墙钟 61.728 s，offered 381.19 QPS，goodput 301.89 QPS，成功 18635，429 是 4895，没有其他状态码，drift 0。成功渠道 primary 14013、backup 4622。延迟 p50 / p95 / p99 = 66.877 / 360.736 / 561.339 ms。非流式开销 p50 / p95 / p99 = 12.434 / 54.139 / 83.472 ms（n=9327）。流式 TTFT 开销没有，原因见上面。

事件时间是请求**开始**时刻。标记在 `chaos-marks.jsonl`：

| 事件 | unix_ms |
|---|---|
| stop_gateway_2 | 1790579294435 |
| start_gateway_2 | 1790579302760 |
| restart_redis | 1790579311463 |
| degrade_mock_a（rate_500=1） | 1790579319879 |
| restore_mock_a | 1790579327888 |

**停掉 gateway-2。** 停之前 3 秒：1128 个请求，200 有 864，429 有 264，没有其他状态；成功里 primary 861、backup 3。停之后按 200 ms 分桶看了 8 秒，每一桶的非 200/429 都是 0。没有「从 5xx 恢复」的时间，因为没有 5xx。成功请求的延迟：停前 2 秒 p50 / p99 = 53.998 / 481.241 ms（n=547）；停后 0–2 秒 64.965 / 623.433 ms（n=621）；停后 2–4 秒 59.115 / 502.269 ms（n=582）。剩下两台加上 nginx 的 `proxy_next_upstream` 接住了。

**重启 Redis。** 限流是 fail-open 到进程内空桶，计费不在 Redis。重启标记后的第一个 200 ms 桶仍有 16 个 429（77 个请求里）。从 +200 ms 到 +3400 ms 的桶里 429 为 0。+3400 ms 的桶重新出现 9 个 429（66 个请求里）。空窗大约 3.2 秒，没有 5xx。整轮结束 drift 仍是 0。

**主渠道全部 500。** 降级前 3 秒的成功请求是 primary 859、backup 1。从标记后 500 ms 到恢复标记：3003 个请求，200 有 2300，全部在 backup，primary 为 0，没有其他错误。标记之后最先开始的成功请求在 +1 ms，渠道已经是 backup。恢复后 3 秒的成功请求仍全部在 backup（847 个）。第一个重新落到 primary 的请求开始于恢复后 6750 ms，之后一共 2909 个 primary 成功。降级到恢复是 8009 ms，熔断冷却 15 秒，剩余冷却大约 7 秒，和 6750 ms 对得上。客户端没有看到 5xx。

## 本机小模型

没有 GPU。模型是 `qwen2.5:0.5b`，Q4_K_M，397821319 字节，由本机 `ollama serve` 提供。

测法：同一句「用一句话解释什么是 API 网关。」，`temperature=0`，`max_tokens=32`，一次只发一个请求。先各热一次，再交替「直连 `127.0.0.1:11434`，然后走 nginx `127.0.0.1:18083`」。网关容器通过 `172.18.0.1:11434` 访问宿主机上的 Ollama，渠道名 `ollama-qwen`，测量时临时插入，三台网关都 reload 过。Ollama 不发 `X-Mock-*`，所以这里是成对相减，不是响应头相减。百分位同样用 `int((n-1)*p)`。原文 `docs/benchmark-realistic-raw/ollama.json`。

非流式 10 对：

| | p50 | p95 | min | max | 均值 |
|---|---|---|---|---|---|
| 直连 | 347.534 | 390.287 | 333.520 | 430.455 | 362.261 |
| 经网关 | 373.031 | 435.643 | 340.291 | 438.178 | 380.263 |
| 成对差值（网关 − 直连） | 9.747 | 50.178 | -57.424 | 73.588 | 18.001 |

流式 6 对：

| | p50 | p95 | min | max | 均值 |
|---|---|---|---|---|---|
| 直连 TTFT | 14.866 | 31.107 | 14.515 | 48.838 | 24.048 |
| 经网关 TTFT | 21.586 | 24.433 | 20.239 | 36.942 | 24.723 |
| TTFT 成对差值 | 5.398 | 9.568 | -27.560 | 16.822 | 0.675 |
| 整段成对差值 | -33.247 | 39.807 | -76.354 | 79.249 | -2.662 |

在这台 4 核 CPU 上，32 token 的生成时间自己就有几十毫秒的抖动。非流式中位附加延迟是 9.7 ms，流式首 token 中位附加延迟是 5.4 ms。10 对里有 3 对网关比直连更快，所以不能把「网关更快」当成结论，只能说附加延迟小于模型自身的抖动。样本只有 10 和 6，没有放到 Azure trace 的并发下面再跑一遍。

## 没做的事

- 没有回放完整的一周 CSV，只用了三分钟切片。
- 混沌那一轮没有流式 TTFT 开销。
- `run-realistic.sh` 结束之后采的 docker stats 是空闲值，正文没有用它们。
- Ollama 对照没有和 trace 回放叠在一起，也没有 GPU。
