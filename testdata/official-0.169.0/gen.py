#!/usr/bin/env python3
"""从 codex-cli 0.169.0 抓包生成脱敏夹具（只在更新夹具时手动运行；测试不依赖本脚本与 /tmp）。

用法：python3 gen.py [抓包目录，默认 /tmp/cx-capture]
"""
import hashlib, json, os, shutil, sys

SRC = sys.argv[1] if len(sys.argv) > 1 else "/tmp/cx-capture"
OUT = os.path.dirname(os.path.abspath(__file__))
LIMIT = 400  # input / tools 里超过该长度（字符）的字符串替换为占位符


def load(name):
    return {d["seq"]: d for d in (json.loads(l) for l in open(os.path.join(SRC, name)))}


def placeholder(s):
    h = hashlib.sha256(s.encode()).hexdigest()[:12]
    return f"<redacted len={len(s)} sha256={h}>"


def redact(v, keep=()):
    if isinstance(v, str):
        # 环境上下文保留：SDK 的环境时间改写要在夹具上真实发生。
        if len(v) > LIMIT and v not in keep and "<environment_context>" not in v:
            return placeholder(v)
        return v
    if isinstance(v, list):
        return [redact(x, keep) for x in v]
    if isinstance(v, dict):
        return {k: redact(x, keep) for k, x in v.items()}
    return v


def body(d, keep=()):
    b = dict(d["body"])
    for k in ("input", "tools"):
        if k in b:
            b[k] = redact(b[k], keep)
    return b


def dump(name, obj):
    with open(os.path.join(OUT, name), "wb") as f:
        f.write(json.dumps(obj, ensure_ascii=False, separators=(",", ":")).encode())


def headers(d):
    h = dict(d["headers"])
    for k in list(h):
        if k.lower() == "authorization":
            h[k] = "Bearer <redacted>"
    return h


tui = load("log169-tui.jsonl")
dump("tui-handshake.headers.json", headers(tui[21]))
dump("tui-prewarm.json", body(tui[29]))
dump("tui-turn.json", body(tui[35]))
dump("tui-followup.json", body(tui[40]))

# 预热链接（log169b-tui：假上游的 completed 带 response id）：预热后的真实帧带
# previous_response_id = 预热 id、全量输入；标题线程（text.format）不带、全量。
tb = load("log169b-tui.jsonl")
dump("tui-chained-handshake.headers.json", headers(tb[21]))
dump("tui-chained-prewarm.json", body(tb[29]))
dump("tui-chained-turn.json", body(tb[35]))
dump("title-prewarm.json", body(tb[39]))
dump("title-turn.json", body(tb[42]))

ws = load("log169-ws.jsonl")
dump("exec-ws-handshake.headers.json", headers(ws[11]))
dump("exec-ws-prewarm.json", body(ws[14]))
dump("exec-ws-turn.json", body(ws[19]))
dump("exec-ws-followup.json", body(ws[21]))

http = load("log169-http.jsonl")
dump("exec-http-turn.json", body(http[18]))
dump("exec-http-turn.headers.json", headers(http[18]))
dump("exec-http-followup.json", body(http[20]))
dump("exec-http-followup.headers.json", headers(http[20]))

g = load("log169-gpt55.jsonl")
instr = g[13]["body"]["input"][0]["content"][0]["text"]
with open(os.path.join(OUT, "gpt55-instructions.txt"), "w") as f:
    f.write(instr)
dump("gpt55-handshake.headers.json", headers(g[10]))
dump("gpt55-prewarm.json", body(g[13], keep=(instr,)))
dump("gpt55-turn.json", body(g[18]))
dump("gpt55-followup.json", body(g[20]))

# 原始 zstd 请求体（HTTP→HTTP，log169-zstd seq 18 / 20），原样复制，不脱敏。
raw = os.path.join(SRC, "raw-zstd")
shutil.copyfile(os.path.join(raw, "body-1794575967084.zst"), os.path.join(OUT, "http-body-turn.zst"))
shutil.copyfile(os.path.join(raw, "body-1794575967182.zst"), os.path.join(OUT, "http-body-followup.zst"))
z = load("log169-zstd.jsonl")
dump("http-body-turn.headers.json", headers(z[18]))
