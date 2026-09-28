#!/usr/bin/env python3
"""直连 MiniMax 与经过 Railhead 的小规模对照。密钥只从环境变量读取，不写进结果。"""

import json
import os
import subprocess
import time
import urllib.error
import urllib.request
from pathlib import Path

DIRECT = "https://api.minimaxi.com/v1/chat/completions"
GATEWAY = os.environ.get("GATEWAY_URL", "http://127.0.0.1:18085")
ADMIN = os.environ.get("ADMIN_TOKEN", "dev-admin-token")
OUT = Path(os.environ.get("OUT_DIR", "docs/benchmark-real-upstream-raw"))
KEY = os.environ["MINIMAX_API_KEY"]
GW_KEY = os.environ.get("GW_KEY", "sk-railhead-minimax-bench")
MAX_TOKENS = 128
PROMPT = "用一句话说明什么是限流。"

LEVELS = [(1, 10), (4, 16), (8, 16), (16, 16)]


def pct(xs, p):
    if not xs:
        return None
    xs = sorted(xs)
    return xs[int((len(xs) - 1) * p)]


def pack(xs):
    if not xs:
        return {"n": 0}
    return {
        "n": len(xs),
        "avg": sum(xs) / len(xs),
        "p50": pct(xs, 0.50),
        "p95": pct(xs, 0.95),
        "p99": pct(xs, 0.99),
        "min": xs[0] if xs == sorted(xs) else min(xs),
        "max": max(xs),
    }


def post(url, headers, payload, timeout=150):
    data = json.dumps(payload).encode()
    req = urllib.request.Request(url, data=data, headers=headers)
    delay = 0.5
    last = None
    for attempt in range(5):
        t0 = time.perf_counter()
        try:
            with urllib.request.urlopen(req, timeout=timeout) as res:
                raw = res.read()
                return {
                    "status": res.status,
                    "headers": {k.lower(): v for k, v in res.headers.items()},
                    "raw": raw,
                    "latency_ms": (time.perf_counter() - t0) * 1000,
                    "attempts": attempt + 1,
                    "rate_limits_seen": attempt,
                }
        except urllib.error.HTTPError as e:
            body = e.read()
            elapsed = (time.perf_counter() - t0) * 1000
            last = {
                "status": e.code,
                "headers": {k.lower(): v for k, v in e.headers.items()} if e.headers else {},
                "raw": body,
                "latency_ms": elapsed,
                "attempts": attempt + 1,
                "rate_limits_seen": attempt + (1 if e.code == 429 else 0),
            }
            if e.code != 429 or attempt == 4:
                return last
            time.sleep(delay)
            delay = min(delay * 2, 8)
        except Exception as e:
            return {
                "status": 0,
                "headers": {},
                "raw": str(e).encode()[:300],
                "latency_ms": (time.perf_counter() - t0) * 1000,
                "attempts": attempt + 1,
                "rate_limits_seen": attempt,
                "error": type(e).__name__,
            }
    return last


def post_stream(url, headers, payload, timeout=150):
    data = json.dumps(payload).encode()
    delay = 0.5
    for attempt in range(5):
        req = urllib.request.Request(url, data=data, headers=headers)
        t0 = time.perf_counter()
        try:
            with urllib.request.urlopen(req, timeout=timeout) as res:
                ttft = None
                chunks = []
                while True:
                    line = res.readline()
                    if not line:
                        break
                    if ttft is None and line.strip():
                        ttft = (time.perf_counter() - t0) * 1000
                    chunks.append(line)
                return {
                    "status": res.status,
                    "headers": {k.lower(): v for k, v in res.headers.items()},
                    "raw": b"".join(chunks),
                    "latency_ms": (time.perf_counter() - t0) * 1000,
                    "ttft_ms": ttft,
                    "attempts": attempt + 1,
                    "rate_limits_seen": attempt,
                }
        except urllib.error.HTTPError as e:
            body = e.read()
            if e.code != 429 or attempt == 4:
                return {
                    "status": e.code,
                    "headers": {k.lower(): v for k, v in (e.headers.items() if e.headers else [])},
                    "raw": body,
                    "latency_ms": (time.perf_counter() - t0) * 1000,
                    "ttft_ms": None,
                    "attempts": attempt + 1,
                    "rate_limits_seen": attempt + (1 if e.code == 429 else 0),
                }
            time.sleep(delay)
            delay = min(delay * 2, 8)
        except Exception as e:
            return {
                "status": 0,
                "headers": {},
                "raw": str(e).encode()[:300],
                "latency_ms": (time.perf_counter() - t0) * 1000,
                "ttft_ms": None,
                "attempts": attempt + 1,
                "rate_limits_seen": attempt,
                "error": type(e).__name__,
            }


def parse_unary(raw):
    try:
        obj = json.loads(raw)
    except Exception:
        return {"parse_error": True, "usage": None, "has_think": False}
    msg = (((obj.get("choices") or [{}])[0]).get("message") or {})
    content = msg.get("content") or ""
    if not isinstance(content, str):
        content = json.dumps(content, ensure_ascii=False)
    usage = obj.get("usage") or {}
    details = usage.get("completion_tokens_details") or {}
    err = obj.get("error") or {}
    return {
        "usage": usage,
        "has_think": "<think>" in content,
        "has_think_close": "</think>" in content,
        "content_len": len(content),
        "model": obj.get("model"),
        "reasoning_tokens": (details or {}).get("reasoning_tokens"),
        "error_code": err.get("code") if isinstance(err, dict) else None,
        "error_message": (err.get("message") if isinstance(err, dict) else str(err))[:180] if err else None,
    }


def parse_stream(raw):
    text = raw.decode("utf-8", "replace")
    usage = None
    model = None
    content = []
    err = None
    for line in text.splitlines():
        line = line.strip()
        if not line.startswith("data:"):
            continue
        data = line[5:].strip()
        if data == "[DONE]":
            continue
        try:
            obj = json.loads(data)
        except Exception:
            continue
        if obj.get("model"):
            model = obj["model"]
        if obj.get("usage"):
            usage = obj["usage"]
        if obj.get("error"):
            err = obj["error"]
        for ch in obj.get("choices") or []:
            delta = ch.get("delta") or {}
            if delta.get("content"):
                content.append(delta["content"])
    joined = "".join(content)
    details = (usage or {}).get("completion_tokens_details") or {}
    return {
        "usage": usage,
        "has_think": "<think>" in joined,
        "has_think_close": "</think>" in joined,
        "content_len": len(joined),
        "model": model,
        "reasoning_tokens": details.get("reasoning_tokens"),
        "error_code": err.get("code") if isinstance(err, dict) else None,
        "error_message": (err.get("message")[:180] if isinstance(err, dict) and err.get("message") else None),
    }


def summarize(rows, stream):
    ok = [r for r in rows if r["status"] == 200]
    lats = [r["latency_ms"] for r in ok]
    ttfts = [r["ttft_ms"] for r in ok if r.get("ttft_ms")]
    tokens = []
    think = 0
    for r in ok:
        u = r.get("usage") or {}
        if u.get("total_tokens"):
            tokens.append(u["total_tokens"])
        if r.get("has_think"):
            think += 1
    return {
        "n": len(rows),
        "success": len(ok),
        "status_counts": count_status(rows),
        "rate_limits_seen": sum(r.get("rate_limits_seen") or 0 for r in rows),
        "latency_ms": pack(lats),
        "ttft_ms": pack(ttfts) if stream else {"n": 0},
        "usage_total_tokens": sum(tokens),
        "think_responses": think,
    }


def count_status(rows):
    out = {}
    for r in rows:
        k = str(r["status"])
        out[k] = out.get(k, 0) + 1
    return out


def one(path, conc, stream, idx):
    payload = {
        "model": "MiniMax-M3",
        "messages": [{"role": "user", "content": PROMPT}],
        "max_tokens": MAX_TOKENS,
        "temperature": 0,
        "stream": stream,
    }
    if path == "direct":
        headers = {"Content-Type": "application/json", "Authorization": "Bearer " + KEY}
        url = DIRECT
    else:
        headers = {"Content-Type": "application/json", "Authorization": "Bearer " + GW_KEY}
        url = GATEWAY.rstrip("/") + "/v1/chat/completions"
    if stream:
        payload["stream_options"] = {"include_usage": True}
        res = post_stream(url, headers, payload)
        parsed = parse_stream(res["raw"])
    else:
        res = post(url, headers, payload)
        parsed = parse_unary(res["raw"])
    rec = {
        "path": path,
        "concurrency": conc,
        "stream": stream,
        "idx": idx,
        "status": res["status"],
        "latency_ms": res["latency_ms"],
        "ttft_ms": res.get("ttft_ms"),
        "attempts": res.get("attempts"),
        "rate_limits_seen": res.get("rate_limits_seen") or 0,
        "channel": (res.get("headers") or {}).get("x-railhead-channel"),
        "error": res.get("error"),
    }
    rec.update(parsed)
    # 不保留原文，避免把大段思考写进仓库。
    return rec


def run_level(conc, n):
    # 同一时刻只打一边，避免直连和网关叠在一起把供应商并发翻倍。
    rows = []
    print(f"level c={conc} n={n} direct", flush=True)
    rows.extend(run_pool("direct", conc, n))
    print(f"level c={conc} n={n} gateway", flush=True)
    rows.extend(run_pool("gateway", conc, n))
    return rows


def run_pool(path, conc, n):
    import concurrent.futures
    # 一半流式，奇数序号走流。
    jobs = [(i, i % 2 == 1) for i in range(n)]
    out = [None] * n
    with concurrent.futures.ThreadPoolExecutor(max_workers=conc) as ex:
        futs = {ex.submit(one, path, conc, stream, i): i for i, stream in jobs}
        for fut in concurrent.futures.as_completed(futs):
            i = futs[fut]
            out[i] = fut.result()
            print(f"  {path} c={conc} i={i} status={out[i]['status']} ms={out[i]['latency_ms']:.0f} tokens={(out[i].get('usage') or {}).get('total_tokens')}", flush=True)
    return out


def reload_channels():
    req = urllib.request.Request(
        GATEWAY.rstrip("/") + "/admin/channels/reload",
        data=b"",
        headers={"X-Admin-Token": ADMIN},
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=10) as res:
        return json.loads(res.read())


def psql(sql):
    subprocess.check_call([
        "sudo", "docker", "exec", "-i", "railhead-realistic-postgres-1",
        "psql", "-U", "railhead", "-d", "railhead_upstream", "-c", sql,
    ], stdout=subprocess.DEVNULL)


def failover():
    print("failover", flush=True)
    psql("UPDATE channels SET timeout_ms=1 WHERE name='minimax-alias-primary';")
    reloaded = reload_channels()
    rows = []
    for i in range(4):
        payload = {
            "model": "minimax",
            "messages": [{"role": "user", "content": "只回答：pong"}],
            "max_tokens": 64,
            "temperature": 0,
        }
        res = post(
            GATEWAY.rstrip("/") + "/v1/chat/completions",
            {"Content-Type": "application/json", "Authorization": "Bearer " + GW_KEY},
            payload,
        )
        parsed = parse_unary(res["raw"])
        rows.append({
            "idx": i,
            "status": res["status"],
            "latency_ms": res["latency_ms"],
            "channel": (res.get("headers") or {}).get("x-railhead-channel"),
            "model": parsed.get("model"),
            "usage": parsed.get("usage"),
            "has_think": parsed.get("has_think"),
            "error_message": parsed.get("error_message"),
        })
        print("  failover", rows[-1]["status"], rows[-1]["channel"], rows[-1]["model"], (rows[-1].get("usage") or {}).get("total_tokens"), flush=True)
    psql("UPDATE channels SET timeout_ms=120000 WHERE name='minimax-alias-primary';")
    reload_channels()
    return {"reloaded": reloaded, "requests": rows}


def unknown_model():
    payload = {
        "model": "MiniMax-M3.1",
        "messages": [{"role": "user", "content": "ping"}],
        "max_tokens": 16,
        "temperature": 0,
    }
    res = post(
        GATEWAY.rstrip("/") + "/v1/chat/completions",
        {"Content-Type": "application/json", "Authorization": "Bearer " + GW_KEY},
        payload,
    )
    try:
        body = json.loads(res["raw"])
    except Exception:
        body = {"raw": res["raw"][:180].decode("utf-8", "replace")}
    err = body.get("error") if isinstance(body, dict) else None
    return {
        "status": res["status"],
        "latency_ms": res["latency_ms"],
        "channel": (res.get("headers") or {}).get("x-railhead-channel"),
        "error": err if isinstance(err, dict) else body,
    }


def reconcile():
    # 种子密钥名字固定，只取数字 id。
    out = subprocess.check_output([
        "sudo", "docker", "exec", "-i", "railhead-realistic-postgres-1",
        "psql", "-U", "railhead", "-d", "railhead_upstream", "-tA",
        "-c", "SELECT id FROM api_keys WHERE name='minimax-bench' ORDER BY id LIMIT 1;",
    ], text=True).strip()
    key_id = int(out)
    req = urllib.request.Request(
        f"{GATEWAY.rstrip('/')}/admin/keys/{key_id}/reconcile",
        headers={"X-Admin-Token": ADMIN},
    )
    with urllib.request.urlopen(req, timeout=15) as res:
        rec = json.loads(res.read())
    rec["api_key_id"] = key_id
    return rec


def main():
    OUT.mkdir(parents=True, exist_ok=True)
    rows = []
    stop = False
    for conc, n in LEVELS:
        if stop:
            break
        batch = run_level(conc, n)
        rows.extend(batch)
        limits = sum(r.get("rate_limits_seen") or 0 for r in batch)
        fails = sum(1 for r in batch if r["status"] not in (200, 429))
        if limits >= 3:
            print("stopping after repeated 429", flush=True)
            stop = True
        if fails > n:
            print("stopping after errors", flush=True)
            stop = True
    fo = failover()
    unknown = unknown_model()
    rec = reconcile()
    gw_tokens = 0
    direct_tokens = 0
    for r in rows:
        total = ((r.get("usage") or {}).get("total_tokens")) or 0
        if r["path"] == "gateway" and r["status"] == 200:
            gw_tokens += total
        if r["path"] == "direct" and r["status"] == 200:
            direct_tokens += total
    fo_tokens = 0
    for r in fo["requests"]:
        if r["status"] == 200:
            fo_tokens += ((r.get("usage") or {}).get("total_tokens")) or 0
    by = {}
    for path in ("direct", "gateway"):
        for conc, _ in LEVELS:
            for stream in (False, True):
                subset = [r for r in rows if r["path"] == path and r["concurrency"] == conc and r["stream"] == stream]
                if not subset:
                    continue
                by[f"{path}_c{conc}_{'stream' if stream else 'unary'}"] = summarize(subset, stream)
    # 开销 = 同一并发下网关分位 − 直连分位。不是同一条请求相减，除了 c=1 另给成对差值。
    overhead = {}
    for conc, _ in LEVELS:
        for stream in (False, True):
            d = [r["latency_ms"] for r in rows if r["path"] == "direct" and r["concurrency"] == conc and r["stream"] == stream and r["status"] == 200]
            g = [r["latency_ms"] for r in rows if r["path"] == "gateway" and r["concurrency"] == conc and r["stream"] == stream and r["status"] == 200]
            if not d or not g:
                continue
            key = f"c{conc}_{'stream' if stream else 'unary'}"
            overhead[key] = {
                "latency_p50_ms": pct(g, 0.5) - pct(d, 0.5),
                "latency_p95_ms": pct(g, 0.95) - pct(d, 0.95),
                "latency_p99_ms": pct(g, 0.99) - pct(d, 0.99),
            }
            if stream:
                dt = [r["ttft_ms"] for r in rows if r["path"] == "direct" and r["concurrency"] == conc and r["stream"] and r["status"] == 200 and r.get("ttft_ms")]
                gt = [r["ttft_ms"] for r in rows if r["path"] == "gateway" and r["concurrency"] == conc and r["stream"] and r["status"] == 200 and r.get("ttft_ms")]
                if dt and gt:
                    overhead[key]["ttft_p50_ms"] = pct(gt, 0.5) - pct(dt, 0.5)
                    overhead[key]["ttft_p95_ms"] = pct(gt, 0.95) - pct(dt, 0.95)
                    overhead[key]["ttft_p99_ms"] = pct(gt, 0.99) - pct(dt, 0.99)
    paired = []
    d1 = [r for r in rows if r["path"] == "direct" and r["concurrency"] == 1 and r["status"] == 200]
    g1 = [r for r in rows if r["path"] == "gateway" and r["concurrency"] == 1 and r["status"] == 200]
    # c=1 时两边按 idx 对齐，且是先跑完直连再跑网关，不是交错。这里不做伪成对。
    summary = {
        "model": "MiniMax-M3",
        "max_tokens": MAX_TOKENS,
        "prompt": PROMPT,
        "temperature": 0,
        "levels": LEVELS,
        "by": by,
        "overhead_gateway_minus_direct": overhead,
        "direct_usage_total_tokens": direct_tokens,
        "gateway_usage_total_tokens": gw_tokens,
        "failover_usage_total_tokens": fo_tokens,
        "failover": fo,
        "unknown_model_via_gateway": unknown,
        "reconcile": rec,
        "gateway_usage_vs_settled": {
            "reported_usage_total": gw_tokens + fo_tokens,
            "settled": rec.get("settled"),
            "unbilled": rec.get("unbilled"),
            "drift": rec.get("drift"),
            "inflight": rec.get("inflight"),
        },
        "requests": rows,
        "c1_note": {"direct_n": len(d1), "gateway_n": len(g1), "paired": paired},
    }
    (OUT / "results.json").write_text(json.dumps(summary, ensure_ascii=False, indent=2))
    slim = {k: summary[k] for k in summary if k != "requests"}
    print(json.dumps(slim, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
