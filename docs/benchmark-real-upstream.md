# MiniMax 真实上游

数字只来自 2026-09-28 打到 `https://api.minimaxi.com/v1/chat/completions` 的请求。原文在 `docs/benchmark-real-upstream-raw/`。密钥从环境变量 `MINIMAX_API_KEY` 读，配置文件里只有 `${MINIMAX_API_KEY}`。百分位算法和回放工具一样：排序后取 `int((n-1)*p)`。延迟保留三位小数。

这一轮故意很小：短 prompt「用一句话说明什么是限流。」，`temperature=0`，`max_tokens=128`。并发只做到 1、4、8、16。撞上 429 就按 0.5、1、2、4、8 秒退避，最多 5 次，然后停下，不再加并发。

## 模型和 `<think>`

同一把 Token Plan 密钥，各发一条 `max_tokens=64` 的非流式请求：

| 模型 | HTTP | total_tokens | 说明 |
|---|---|---|---|
| MiniMax-M3 | 200 | 215 | `content` 含 `<think>`，`completion_tokens_details.reasoning_tokens` 为 31，completion 35 |
| MiniMax-M2.5 | 200 | 109 | `content` 含 `<think>`，另有空的 `audio_content` 和 `name=MiniMax AI` |
| MiniMax-M2.1 | 200 | 104 | 同上 |
| MiniMax-M2 | 200 | 107 | 同上 |
| `minimax-m3`（小写） | 200 | 220 | 供应商接受这个名字，按 M3 计 |
| MiniMax-M3.1 | 400 | 0 | `invalid params, unknown model 'minimax-m3.1' (2013)` |
| MiniMax-M3.1-highspeed | 400 | 0 | 同样是未知模型，错误码 2013 |

M3 的思考文本在 `content` 里，不在单独的 `reasoning_content`。一条 `max_tokens=128` 的流式探针里，`completion_tokens=128`，`reasoning_tokens=127`，首包 delta 里就有 `<think>`，最后一块带 usage。网关把上游 JSON 原样转回去，所以这些字段还在。结算用 `prompt_tokens + completion_tokens`，思考 token 算在 completion 里，不会只数 `</think>` 后面的几个字。

并发阶梯里 69 个 HTTP 200 全部同时含有 `<think>` 和 `</think>`，并且都带 `reasoning_tokens`。该字段最小 0、最大 127；`completion_tokens` 最小 38、最大 128。M3 的 prompt 大约 180 token，哪怕用户只写了一句话。本仓库的估算器远小于这个数，所以这轮网关把 `RESERVE_MARGIN` 设成 512。对账没有出现 unbilled。

## 网关

宿主机进程听 `127.0.0.1:18085`，Postgres 用单独的库 `railhead_upstream`，Redis 用 2 号库。渠道在 `configs/channels.minimax.json`：

| 渠道 | 客户端模型 | upstream_model | 优先级 |
|---|---|---|---|
| minimax-m3 | MiniMax-M3 | MiniMax-M3 | 100 |
| minimax-m27hs | MiniMax-M2.7-highspeed | MiniMax-M2.7-highspeed | 50 |
| minimax-alias-primary | minimax | MiniMax-M3 | 100 |
| minimax-alias-backup | minimax | MiniMax-M2.7-highspeed | 10 |

延迟对照打的是模型名 `MiniMax-M3`，只会进第一条渠道。故障转移打的是别名 `minimax`。

## 并发阶梯

每一档先打直连，再打网关，避免两边同时把供应商并发叠上去。直连这一轮每次都是新的 HTTPS 连接。成功请求都含 `<think>`。

| 路径 | 并发 | 形态 | 成功/请求 | 延迟 p50 / p95 / p99 (ms) | TTFT p50 / p95 / p99 (ms) | usage 合计 |
|---|---|---|---|---|---|---|
| 直连 | 1 | 非流式 | 5/5 | 2959.930 / 3010.897 / 3010.897 | — | 1181 |
| 直连 | 1 | 流式 | 5/5 | 3211.294 / 3289.111 / 3289.111 | 1580.687 / 1720.254 / 1720.254 | 1229 |
| 网关 | 1 | 非流式 | 5/5 | 2864.008 / 3455.308 / 3455.308 | — | 1346 |
| 网关 | 1 | 流式 | 5/5 | 2161.377 / 2713.662 / 2713.662 | 722.961 / 806.393 / 806.393 | 1309 |
| 直连 | 4 | 非流式 | 8/8 | 3692.737 / 4875.556 / 4875.556 | — | 2064 |
| 直连 | 4 | 流式 | 8/8 | 3638.627 / 4514.715 / 4514.715 | 1824.216 / 2007.455 / 2007.455 | 2126 |
| 网关 | 4 | 非流式 | 8/8 | 2355.953 / 3412.643 / 3412.643 | — | 1990 |
| 网关 | 4 | 流式 | 8/8 | 2507.688 / 3631.652 / 3631.652 | 928.799 / 1645.383 / 1645.383 | 2035 |
| 直连 | 8 | 非流式 | 8/8 | 3548.228 / 3564.033 / 3564.033 | — | 2039 |
| 直连 | 8 | 流式 | 8/8 | 3587.657 / 4081.931 / 4081.931 | 1737.853 / 1910.998 / 1910.998 | 2139 |
| 网关 | 8 | 非流式 | 1/8 | 仅 1 个 200：1803.728 | — | 237 |
| 网关 | 8 | 流式 | 0/8 | — | — | 0 |
| 直连 | 16 | 两种 | 0/16 | 全部 429 | — | 0 |
| 网关 | 16 | 两种 | 0/16 | 全部 502 | — | 0 |

n 只有 5 或 8 时，p95 和 p99 会落在同一个样本上。这是算法结果，不是测量把两个分位收成一样。

并发 1 和 4 时，网关队列的 p50 比直连低几百到一千多毫秒。直连没有复用连接，网关的 HTTP 客户端会复用到 MiniMax 的连接，思考时间本身又有一两秒的起伏。这个差值不能当成「网关比直连更快」的开销。

## 复用连接之后的成对开销

两边都保住连接，并发 1，先各丢弃 1 次预热，再交替「直连一次、网关一次」。非流式 6 对，流式 6 对。原文 `pooled.json`。网关渠道都是 `minimax-m3`，6 个非流式响应都还带着 `<think>` 和 `reasoning_tokens`。

| | p50 | p95 | min | max |
|---|---|---|---|---|
| 直连非流式 | 2087.028 | 2216.705 | 1850.894 | 2382.079 |
| 网关非流式 | 1687.886 | 1979.829 | 1640.464 | 9130.169 |
| 成对差值（网关 − 直连） | -467.313 | 101.837 | -741.615 | 7279.275 |
| 直连 TTFT | 910.343 | 1337.857 | 676.510 | 2287.125 |
| 网关 TTFT | 906.246 | 2281.587 | 761.083 | 2883.505 |
| TTFT 成对差值 | 205.007 | 943.731 | -348.872 | 956.258 |
| 流式整段成对差值 | 330.236 | 842.280 | -295.174 | 1958.762 |

TTFT 的中位差值是 +205.007 ms，但 6 对里有一对是 -348.872 ms，有两对超过 +940 ms。非流式有一对网关耗时 9130.169 ms，把平均值拉到 +878.870 ms，中位数却是 -467.313 ms。思考长度带来的波动比网关自己的处理时间大，这组样本不能给出一个稳定的毫秒级开销。

## 429 和错误

直连在并发 16 的 16 个请求全部以 429 结束。每个请求退避重试了 5 次，计数器里一共记下 80 次 429 响应。正文是：`已达到 Token Plan 速率限制：请升级 Token Plan 套餐或切换为按量付费 API 使用。 (2062)`，类型 `rate_limit_error`。

网关在这之前的并发 8 已经开始失败：16 个请求里 1 个 200、7 个 502、8 个 404。502 的日志里上游正文同样是 2062。网关把 429 当成可重试，重试耗尽后调用方看到的是「上游渠道不可用」，HTTP 502，不是 429。404 的正文是「没有可用渠道能服务这个模型」，对应熔断器打开之后没有可选渠道。阈值是 5 次失败，冷却 15 秒。

经网关请求 `MiniMax-M3.1`：HTTP 400，耗时 348.169 ms，没有渠道头。错误被原样包进网关的 `upstream_rejected`，里面是 unknown model，错误码 2013。400 不可重试，没有换到别的模型。

## 故障转移

限流还没过去时，把 `minimax-alias-primary` 的超时改成 1 ms 再打 4 个别名请求，没有成功：2 个 504（「请求超时」，388.875 ms 和 410.036 ms），2 个 502（699.520 ms 和 357.339 ms，日志里是 2062）。备用渠道也处于同一把密钥的限流里，这一次不能当作转移成功。

限流过去之后，把主渠道的 `base_url` 改成 `http://127.0.0.1:9`（连接被拒绝，可重试）。备用渠道的 `upstream_model` 是 MiniMax-M2.5。3 个请求全部 200，渠道都是 `minimax-alias-backup`，响应里的 `model` 都是 `MiniMax-M2.5`：

| 延迟 ms | total_tokens |
|---|---|
| 2684.0 | 109 |
| 1781.5 | 109 |
| 1348.2 | 108 |

## 账单

网关密钥 `minimax-bench`，授予 5000000 token。结束时对账：

| 项 | 值 |
|---|---|
| settled | 14346 |
| unbilled | 0 |
| inflight | 0 |
| drift | 0 |
| settled_count | 58 |
| refunded_count | 36 |

逐条保存下来的网关 usage 合计 14098：冒烟 212，并发阶梯 6917，交替测量 3585，复用连接的成对测量 3058，故障转移 326。对账比这个和多 248。多出来的是复用连接那一轮里没有单独落盘的 1 次网关预热。14346 = 14098 + 248。失败和 429、400、504、502 都退款了，不在 settled 里。

## 消耗了多少 token

下面只加 HTTP 200 上供应商返回的 `total_tokens`。

| 部分 | token |
|---|---|
| 模型探测（含两条未写进探测文件的流式 309 和 M2.5 的 74，以及限流冷却后的 74） | 1212 |
| 并发阶梯，直连 | 10778 |
| 并发阶梯，网关 | 6917 |
| 网关冒烟 | 212 |
| 交替测量，直连 + 网关 | 7167 |
| 复用连接的成对测量，不含预热 | 6021 |
| 网关预热（只在对账差额里） | 248 |
| 故障转移 3 次 | 326 |
| 合计 | 32881 |

复用连接那一轮还有 1 次直连预热，usage 没有落盘，所以上面的合计少了这一次。429 和 400 的响应里没有 usage，没有把它们算进合计。

## 模型列表和备用渠道换成 M2.7-highspeed

对同一把密钥调用 `GET https://api.minimaxi.com/v1/models`，HTTP 200，返回 8 个 id，按响应顺序是：

MiniMax-M3、MiniMax-M2.7、MiniMax-M2.7-highspeed、MiniMax-M2.5、MiniMax-M2.5-highspeed、MiniMax-M2.1、MiniMax-M2.1-highspeed、MiniMax-M2。

没有 MiniMax-M3.1。OpenAI 兼容接口上一次对这个名字的 400 / 2013 仍然有效。Anthropic 兼容接口 `POST https://api.minimaxi.com/anthropic/v1/messages` 接受请求体里的 `model=MiniMax-M3.1`，HTTP 200，响应里的 `model` 是 `MiniMax-M3`。这次 usage 是 `input_tokens=39`、`output_tokens=2`、`cache_read_input_tokens=128`，内容块只有 `text`。

同一轮用 OpenAI 接口打了一条 `MiniMax-M2.7-highspeed`，`max_tokens=32`：HTTP 200，`model` 就是 MiniMax-M2.7-highspeed，`total_tokens=77`（prompt 45，completion 32），`content` 含 `<think>`。

渠道配置已改成主渠道 MiniMax-M3、备用 MiniMax-M2.7-highspeed。再把主渠道指到 `http://127.0.0.1:9`，3 个别名请求全部 200，渠道都是 `minimax-alias-backup`，响应模型都是 MiniMax-M2.7-highspeed，都含 `<think>`。原文 `docs/benchmark-real-upstream-raw/failover-m27hs.json`。

| 延迟 ms | total_tokens |
|---|---|
| 2182.5 | 77 |
| 1482.5 | 77 |
| 2750.9 | 77 |

这 3 次网关结算合计 231。对账从上一节的 settled 14346 增到 14577，unbilled 0，inflight 0，drift 0。14577 − 14346 = 231。

本轮额外记下的供应商用量：OpenAI 探针 77，网关故障转移 231，Anthropic 的 input+output 为 41。和上一节的 32881 相加是 33230。Anthropic 响应里还有 `cache_read_input_tokens=128`，没有并进这个和。直连预热那一次仍然没有 usage。

## MiniMax-M3.1-Flash-Preview

`GET /v1/models` 仍然没有这个 id。同一把密钥对 `MiniMax-M3.1-Flash-Preview` 调用 OpenAI `POST /v1/chat/completions` 和 Anthropic `POST /anthropic/v1/messages` 都返回 HTTP 200。Anthropic 响应里的 `model` 就是 `MiniMax-M3.1-Flash-Preview`。更早的名字 `MiniMax-M3.1` 在 Anthropic 接口上会被改写成 `MiniMax-M3`，这一节的 Flash 名字没有被改写。

推理有两种写法，网关都要留下：

- MiniMax-M3 和 MiniMax-M2.7-highspeed 把 `<think>…</think>` 放在 `content` 里。M2.7-highspeed 的流式 usage 还会带 `completion_tokens_details.reasoning_tokens`。
- MiniMax-M3.1-Flash-Preview 把推理放在单独的 `reasoning_content`。`content` 里没有 `<think>`。这一轮看到的 `reasoning_tokens` 都是 0，即便 `reasoning_content` 很长。供应商给出的 `completion_tokens` 已经是结算数字，不再把推理文本按本地估算加一遍。

OpenAI 适配器继续转发原始 JSON。流式累加器同时拼接 `delta.content` 和 `delta.reasoning_content`。没有 usage 时，回退估算把这两段一起算进去。有 usage 时只取 `prompt_tokens + completion_tokens`。

格式探针没有另存正文，用量是：OpenAI 非流式 `total_tokens=218`（prompt 208，completion 10，`reasoning_tokens=0`，`content` 长度 4，`reasoning_content` 长度 25）；两条流式分别是 211 和 218；Anthropic `input_tokens=80`、`output_tokens=10`，另有 `cache_read_input_tokens=128`。218 + 211 + 218 + 90 = 737。cache_read 没有并进这个和。

## 当前渠道

`configs/channels.minimax.json` 现在是六条。别名 `minimax` 的主渠道是 Flash，失败且错误可重试时换 MiniMax-M3，再失败才到 MiniMax-M2.7-highspeed。

| 渠道 | 客户端模型 | upstream_model | 优先级 |
|---|---|---|---|
| minimax-m31-flash | MiniMax-M3.1-Flash-Preview | MiniMax-M3.1-Flash-Preview | 100 |
| minimax-m3 | MiniMax-M3 | MiniMax-M3 | 80 |
| minimax-m27hs | MiniMax-M2.7-highspeed | MiniMax-M2.7-highspeed | 50 |
| minimax-alias-primary | minimax | MiniMax-M3.1-Flash-Preview | 100 |
| minimax-alias-failover | minimax | MiniMax-M3 | 50 |
| minimax-alias-m27 | minimax | MiniMax-M2.7-highspeed | 10 |

网关冒烟，`max_tokens=32`，原文 `flash-smoke.json`：

| 请求 | 渠道 | 形态 | total_tokens | 推理 |
|---|---|---|---|---|
| MiniMax-M3.1-Flash-Preview | minimax-m31-flash | 非流式 | 243 | `reasoning_content` 长度 102，`content` 长度 0，`reasoning_tokens=0` |
| MiniMax-M3.1-Flash-Preview | minimax-m31-flash | 流式 | 243 | 流里同时有 `reasoning_content`（长度 96）和 `content`（长度 16），没有 `<think>` |
| MiniMax-M3 | minimax-m3 | 非流式 | 215 | `content` 含 `<think>`，没有 `reasoning_content` |

## 三个模型的延迟和 TTFT

同一句 prompt「用一句话说明什么是限流。」，`temperature=0`，`max_tokens=128`，并发 1。直连和网关都复用连接。每个模型先丢掉 1 次非流式预热，再交替测 5 次非流式、5 次流式。百分位仍是 `int((n-1)*p)`，n=5 时 p95 和 p99 落在同一个样本上。原文 `docs/benchmark-real-upstream-raw/models-compare.json`。脚本是 `scripts/minimax_models_compare.py`。66 次请求全部 HTTP 200，没有 429。

网关这一侧强制带了 `stream_options.include_usage`。直连流式这次没有带这个字段，15 条响应里的 `usage` 都是 null，所以直连流式没有 usage 合计。TTFT 仍是响应体里第一行非空内容的时间。

| 模型 | 路径 | 形态 | 延迟 p50 / p95 / p99 (ms) | TTFT p50 / p95 / p99 (ms) | usage 合计 | 推理 |
|---|---|---|---|---|---|---|
| MiniMax-M3.1-Flash-Preview | 网关 | 非流式 | 1938.303 / 2950.160 / 2950.160 | — | 1354 | 5/5 有 `reasoning_content`，`reasoning_tokens` 全是 0 |
| MiniMax-M3.1-Flash-Preview | 直连 | 非流式 | 1555.208 / 2032.067 / 2032.067 | — | 1362 | 同上 |
| MiniMax-M3.1-Flash-Preview | 网关 | 流式 | 1798.157 / 1856.984 / 1856.984 | 1184.855 / 1301.639 / 1301.639 | 1351 | 5/5 有 `reasoning_content`，没有 `<think>` |
| MiniMax-M3.1-Flash-Preview | 直连 | 流式 | 2144.953 / 2229.614 / 2229.614 | 1279.146 / 1423.881 / 1423.881 | 未返回 | 5 次里 3 次有 `reasoning_content` |
| MiniMax-M3 | 网关 | 非流式 | 2095.046 / 2096.872 / 2096.872 | — | 1197 | 5/5 有 `<think>`，没有 `reasoning_content` |
| MiniMax-M3 | 直连 | 非流式 | 2213.575 / 2419.700 / 2419.700 | — | 1207 | 同上 |
| MiniMax-M3 | 网关 | 流式 | 2897.745 / 3739.365 / 3739.365 | 1656.596 / 2550.736 / 2550.736 | 1230 | 5/5 有 `<think>`，`reasoning_tokens` 全是 0 |
| MiniMax-M3 | 直连 | 流式 | 2439.649 / 3055.723 / 3055.723 | 868.213 / 958.320 / 958.320 | 未返回 | 5/5 有 `<think>` |
| MiniMax-M2.7-highspeed | 网关 | 非流式 | 3877.062 / 3987.591 / 3987.591 | — | 880 | 5/5 有 `<think>`。usage 没有 `reasoning_tokens` 字段。每次都是 prompt 48 + completion 128 |
| MiniMax-M2.7-highspeed | 直连 | 非流式 | 3609.672 / 3714.935 / 3714.935 | — | 880 | 同上 |
| MiniMax-M2.7-highspeed | 网关 | 流式 | 3308.744 / 3689.393 / 3689.393 | 1132.223 / 1269.516 / 1269.516 | 880 | `<think>` 在 content 里。`reasoning_tokens` 为 128、128、128、122、128 |
| MiniMax-M2.7-highspeed | 直连 | 流式 | 3668.721 / 3870.094 / 3870.094 | 930.347 / 961.874 / 961.874 | 未返回 | 5/5 有 `<think>` |

预热 6 次另计：网关 256 + 311 + 176 = 743，直连 272 + 259 + 176 = 707。对照表里的 usage 不含预热。有 usage 的全部请求（预热加正式）合计 11791。

这 5 个样本里，整段延迟的中位数以 Flash 最低，M2.7-highspeed 最高。流式 TTFT 的中位数在网关一侧是 M2.7-highspeed 1132.223 ms、Flash 1184.855 ms、M3 1656.596 ms；直连一侧是 M3 868.213 ms、M2.7-highspeed 930.347 ms、Flash 1279.146 ms。同一模型的最大和最小可以差到一两秒，所以这张表是这一轮的样本，不能当成稳定的模型排名。Flash 的 prompt 大约 211 token，M3 大约 183，M2.7-highspeed 是 48。

## 故障转移到 MiniMax-M3

把 `minimax-alias-primary` 的 `base_url` 改成 `http://127.0.0.1:9` 并 reload。别名 `minimax`、`max_tokens=32` 的 3 次非流式请求全部 200，渠道都是 `minimax-alias-failover`，响应模型都是 MiniMax-M3，都含 `<think>`，都没有 `reasoning_content`。原文 `failover-flash.json`。

| 延迟 ms | total_tokens | reasoning_tokens |
|---|---|---|
| 1583.590 | 215 | 31 |
| 1291.428 | 215 | 0 |
| 1226.019 | 215 | 0 |

主渠道 `base_url` 恢复为 `https://api.minimaxi.com/v1` 并 reload 之后，用别名打了 1 次 `max_tokens=16`：HTTP 200，渠道 `minimax-alias-primary`，模型 `MiniMax-M3.1-Flash-Preview`，延迟 1871.468 ms，`total_tokens=227`（prompt 211，completion 16），`reasoning_content` 长度 72，`content` 长度 0，`reasoning_tokens=0`。原文 `alias-restored.json`。

对账：故障转移之前 settled 已经从上一节的 14577 增到 23558，unbilled 0，inflight 0，drift 0。23558 − 14577 = 8981，等于这一轮打进网关的用量：冒烟 701，对照预热 743，对照正式 6892，故障转移 645。确认请求之后 settled 是 23785，23785 − 23558 = 227。`settled_count` 101，`refunded_count` 仍是 36。

对照结束后另有 1 次直连 Flash 流式形状检查，`max_tokens=32`，HTTP 200，delta 里有 `reasoning_content`，`usage` 为 null。这 1 次和上面 15 次直连流式都没有供应商 token 数，没有加进合计。

## 消耗了多少 token（加上 Flash）

只加 HTTP 200 上供应商返回的 token。OpenAI 用 `total_tokens`，Anthropic 用 `input_tokens + output_tokens`。

| 部分 | token |
|---|---|
| 上一节合计 | 33230 |
| 格式探针，OpenAI 218 + 211 + 218 | 647 |
| 格式探针，Anthropic input + output | 90 |
| 网关冒烟 243 + 243 + 215 | 701 |
| 三模型对照里返回了 usage 的请求，含预热 | 11791 |
| 故障转移 3 次 | 645 |
| 恢复后的别名确认 | 227 |
| 合计 | 47331 |

47331 = 33230 + 647 + 90 + 701 + 11791 + 645 + 227。Anthropic 的 `cache_read_input_tokens=128` 仍不在这个和里。15 次直连流式和 1 次形状检查没有 usage，也不在这个和里。更早那次没有落盘的直连预热同样还没算进来。
