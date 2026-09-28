#!/usr/bin/env python3
"""三个 MiniMax 模型的延迟和 TTFT 对照。密钥只从文件读取，不写入结果。"""

import http.client
import json
import os
import time
from pathlib import Path

HOST = "api.minimaxi.com"
GATEWAY_HOST = os.environ.get("GATEWAY_HOST", "127.0.0.1")
GATEWAY_PORT = int(os.environ.get("GATEWAY_PORT", "18085"))
ADMIN = os.environ.get("ADMIN_TOKEN", "dev-admin-token")
OUT = Path(os.environ.get("OUT_DIR", "docs/benchmark-real-upstream-raw"))
KEY = Path(os.environ.get("MINIMAX_KEY_FILE", "/workspace/.secrets/minimax.key")).read_text().strip()
GW_KEY = os.environ.get("GW_KEY", "sk-railhead-minimax-bench")
PROMPT = "用一句话说明什么是限流。"
MODELS = [
    "MiniMax-M3.1-Flash-Preview",
    "MiniMax-M3",
    "MiniMax-M2.7-highspeed",
]
REPS = 5
MAX_TOKENS = 128


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
        "min": min(xs),
        "max": max(xs),
    }


class KeepAlive:
    def __init__(self, https, host, port=None):
        self.https = https
        self.host = host
        self.port = port
        self.conn = None

    def connect(self):
        if self.conn is not None:
            self.conn.close()
        if self.https:
            self.conn = http.client.HTTPSConnection(self.host, timeout=180)
        else:
            self.conn = http.client.HTTPConnection(self.host, self.port, timeout=180)

    def request(self, path, payload, headers, stream):
        body = json.dumps(payload).encode()
        delay = 0.5
        last = None
        for attempt in range(5):
            if self.conn is None:
                self.connect()
            t0 = time.perf_counter()
            try:
                self.conn.request("POST", path, body, headers)
                res = self.conn.getresponse()
                status = res.status
                hdrs = {k.lower(): v for k, v in res.getheaders()}
                if stream and status == 200:
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
                        "status": status,
                        "headers": hdrs,
                        "raw": b"".join(chunks),
                        "latency_ms": (time.perf_counter() - t0) * 1000,
                        "ttft_ms": ttft,
                        "attempts": attempt + 1,
                    }
                raw = res.read()
                out = {
                    "status": status,
                    "headers": hdrs,
                    "raw": raw,
                    "latency_ms": (time.perf_counter() - t0) * 1000,
                    "ttft_ms": None,
                    "attempts": attempt + 1,
                }
                if status == 429 and attempt < 4:
                    last = out
                    time.sleep(delay)
                    delay = min(delay * 2, 8)
                    continue
                return out
            except Exception as e:
                try:
                    self.conn.close()
                except Exception:
                    pass
                self.conn = None
                last = {
                    "status": 0,
                    "headers": {},
                    "raw": str(e).encode()[:300],
                    "latency_ms": (time.perf_counter() - t0) * 1000,
                    "ttft_ms": None,
                    "attempts": attempt + 1,
                    "error": type(e).__name__,
                }
                if attempt == 4:
                    return last
                time.sleep(delay)
                delay = min(delay * 2, 8)
        return last


def flags(raw, stream):
    text = raw.decode("utf-8", "replace") if isinstance(raw, (bytes, bytearray)) else str(raw)
    content_parts = []
    reasoning_parts = []
    usage = None
    model = None
    err = None
    if stream:
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
            model = obj.get("model") or model
            if obj.get("usage"):
                usage = obj["usage"]
            if obj.get("error"):
                err = obj["error"]
            for choice in obj.get("choices") or []:
                delta = choice.get("delta") or {}
                if delta.get("content"):
                    content_parts.append(delta["content"])
                if delta.get("reasoning_content"):
                    reasoning_parts.append(delta["reasoning_content"])
    else:
        try:
            obj = json.loads(text)
        except Exception:
            obj = {}
        model = obj.get("model")
        usage = obj.get("usage")
        err = obj.get("error")
        msg = ((obj.get("choices") or [{}])[0]).get("message") or {}
        content = msg.get("content") or ""
        reasoning = msg.get("reasoning_content") or ""
        if not isinstance(content, str):
            content = json.dumps(content, ensure_ascii=False)
        content_parts.append(content)
        if reasoning:
            reasoning_parts.append(reasoning)
    content = "".join(content_parts)
    reasoning = "".join(reasoning_parts)
    details = (usage or {}).get("completion_tokens_details") or {}
    return {
        "resp_model": model,
        "usage": usage,
        "reasoning_tokens": details.get("reasoning_tokens"),
        "has_think": "<think>" in content,
        "has_think_close": "</think>" in content,
        "has_reasoning_content": len(reasoning) > 0,
        "content_len": len(content),
        "reasoning_len": len(reasoning),
        "error": (err.get("message")[:180] if isinstance(err, dict) and err.get("message") else None),
    }


def one(client, path, headers, model, stream, warmup):
    payload = {
        "model": model,
        "messages": [{"role": "user", "content": PROMPT}],
        "temperature": 0,
        "max_tokens": MAX_TOKENS,
        "stream": stream,
    }
    res = client.request(path, payload, headers, stream)
    parsed = flags(res.get("raw") or b"", stream)
    row = {
        "model": model,
        "stream": stream,
        "warmup": warmup,
        "status": res["status"],
        "latency_ms": res["latency_ms"],
        "ttft_ms": res.get("ttft_ms"),
        "attempts": res.get("attempts"),
        "channel": (res.get("headers") or {}).get("x-railhead-channel"),
        **parsed,
    }
    if res["status"] == 429:
        row["rate_limited"] = True
    return row


def summarize(rows):
    out = {}
    for path in ("gateway", "direct"):
        out[path] = {}
        for model in MODELS:
            out[path][model] = {}
            for stream in (False, True):
                ok = [
                    r
                    for r in rows
                    if r["path"] == path
                    and r["model"] == model
                    and r["stream"] is stream
                    and not r["warmup"]
                    and r["status"] == 200
                ]
                lat = [r["latency_ms"] for r in ok]
                ttft = [r["ttft_ms"] for r in ok if r.get("ttft_ms") is not None]
                usage = sum(((r.get("usage") or {}).get("total_tokens") or 0) for r in ok)
                out[path][model]["stream" if stream else "unary"] = {
                    "n_ok": len(ok),
                    "n": sum(
                        1
                        for r in rows
                        if r["path"] == path
                        and r["model"] == model
                        and r["stream"] is stream
                        and not r["warmup"]
                    ),
                    "latency_ms": pack(lat),
                    "ttft_ms": pack(ttft) if stream else {"n": 0},
                    "usage_total_tokens": usage,
                    "has_think": sum(1 for r in ok if r["has_think"]),
                    "has_reasoning_content": sum(1 for r in ok if r["has_reasoning_content"]),
                    "reasoning_tokens_min": min((r["reasoning_tokens"] for r in ok if r["reasoning_tokens"] is not None), default=None),
                    "reasoning_tokens_max": max((r["reasoning_tokens"] for r in ok if r["reasoning_tokens"] is not None), default=None),
                    "channels": sorted({r["channel"] for r in ok if r.get("channel")}),
                    "resp_models": sorted({r["resp_model"] for r in ok if r.get("resp_model")}),
                }
    return out


def main():
    OUT.mkdir(parents=True, exist_ok=True)
    direct = KeepAlive(True, HOST)
    gateway = KeepAlive(False, GATEWAY_HOST, GATEWAY_PORT)
    direct.connect()
    gateway.connect()
    d_headers = {
        "Authorization": "Bearer " + KEY,
        "Content-Type": "application/json",
        "Connection": "keep-alive",
    }
    g_headers = {
        "Authorization": "Bearer " + GW_KEY,
        "Content-Type": "application/json",
        "Connection": "keep-alive",
    }
    rows = []
    stopped = False

    def record(path, row):
        nonlocal stopped
        row["path"] = path
        rows.append(row)
        print(
            f"{path} warmup={row['warmup']} stream={row['stream']} {row['model']} "
            f"status={row['status']} lat={row['latency_ms']:.1f} "
            f"ttft={row.get('ttft_ms')} tokens={(row.get('usage') or {}).get('total_tokens')} "
            f"think={row['has_think']} reasoning={row['has_reasoning_content']}",
            flush=True,
        )
        if row["status"] == 429:
            stopped = True

    for path, client, headers, urlpath in (
        ("gateway", gateway, g_headers, "/v1/chat/completions"),
        ("direct", direct, d_headers, "/v1/chat/completions"),
    ):
        for model in MODELS:
            record(path, one(client, urlpath, headers, model, False, True))
            if stopped:
                break
        if stopped:
            break

    if not stopped:
        for _ in range(REPS):
            for model in MODELS:
                record("gateway", one(gateway, "/v1/chat/completions", g_headers, model, False, False))
                if stopped:
                    break
                record("direct", one(direct, "/v1/chat/completions", d_headers, model, False, False))
                if stopped:
                    break
            if stopped:
                break
    if not stopped:
        for _ in range(REPS):
            for model in MODELS:
                record("gateway", one(gateway, "/v1/chat/completions", g_headers, model, True, False))
                if stopped:
                    break
                record("direct", one(direct, "/v1/chat/completions", d_headers, model, True, False))
                if stopped:
                    break
            if stopped:
                break

    doc = {
        "prompt": PROMPT,
        "temperature": 0,
        "max_tokens": MAX_TOKENS,
        "reps": REPS,
        "stopped_on_429": stopped,
        "summary": summarize(rows),
        "rows": rows,
    }
    dest = OUT / "models-compare.json"
    dest.write_text(json.dumps(doc, ensure_ascii=False, indent=2) + "\n")
    print("wrote", dest, "rows", len(rows), "stopped", stopped, flush=True)


if __name__ == "__main__":
    main()
