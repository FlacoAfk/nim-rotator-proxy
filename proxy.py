#!/usr/bin/env python3
"""
nim-rotator-proxy — self-hosted OpenAI-compatible proxy for NVIDIA NIM.

Features:
  - API-key pool with rotation, cooldowns and a circuit breaker (server-side keys)
  - BYOK: callers may send their OWN nvapi-... key; the proxy uses it for their
    requests and never shares it with other callers (unless allow_pool_fallback)
  - Pre-flight context guard: rejects estimated tokens > 90% of the model's
    measured context window (context-limits.json) with a clean 400
  - Big-context stream conversion: streaming requests with an estimated
    prefill >= threshold are converted to buffered upstream and replayed as
    SSE (NIM kills long-prefill streams; buffered requests run minutes fine).
    SSE headers + role chunk are sent immediately and keepalive comments flow
    while NIM computes, so clients with ~60s TTFB limits never time out.
  - Key cooldown: 429 -> 5 min (key) / 1 h (model+key); 401/403 -> 24 h;
    410 -> model EOL cooldown; 404 on every key -> suspected death cooldown
  - Transient-status retry across keys; optional model fallback chain
  - Works with any OpenAI-compatible client (point base_url at the proxy)

Stdlib only. Python 3.9+. Runs on Windows / Linux / macOS.

Usage:
  python proxy.py                 # start the server (reads proxy.json)
  python proxy.py keys            # interactive key manager dashboard
  python proxy.py --port 9000     # one-off overrides

Secrets (data/keys.json) never leave the machine and are never logged.
"""

import json
import os
import re
import sys
import time
import uuid
import math
import select
import socket
import hashlib
import threading
import http.client
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

__version__ = "1.0.0"

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.environ.get("NIM_PROXY_DATA_DIR", os.path.join(BASE_DIR, "data"))
KEYS_FILE = os.path.join(DATA_DIR, "keys.json")
STATE_FILE = os.path.join(DATA_DIR, "state.json")
LOG_FILE = os.path.join(DATA_DIR, "proxy.log")
CATALOG_FILE = os.path.join(DATA_DIR, "catalog.json")
CATALOG_DIFF_FILE = os.path.join(DATA_DIR, "catalog-diff.json")
LIMITS_FILE = os.environ.get(
    "NIM_PROXY_LIMITS_FILE", os.path.join(BASE_DIR, "context-limits.json")
)
CONFIG_FILE = os.environ.get("NIM_PROXY_CONFIG", os.path.join(DATA_DIR, "proxy.json"))

UPSTREAM_HOST = "integrate.api.nvidia.com"

DEFAULT_CONFIG = {
    "port": 8377,
    "host": "127.0.0.1",          # 0.0.0.0 to serve LAN/WAN (keys then required)
    "pool_token": "",              # clients send this Bearer token to use the pool
    "allow_pool_fallback": False,  # BYOK requests may fall back to pool on 429
    "max_stream_keys": 2,          # cap streaming key attempts (client TTFB safety)
    "buffered_min_tokens": 150000, # est prefill at which stream->buffered kicks in
    "keepalive_s": 15.0,
    "ttfb_timeout_s": 300.0,
    "upstream_timeout_s": 600.0,
    "default_context": 131072,     # guard ceiling for models missing from limits
    "guard_ratio": 0.9,
    "fallback_models": [],         # ordered nvidia model ids to try if primary fails
    "slow_request_ms": 30000,
    "catalog_enabled": True,       # watch NIM's /v1/models for added/removed models
    "catalog_refresh_s": 21600,    # 6 h between catalog checks (single cheap GET)
    "catalog_poll_key_id": "",     # optional explicit key id used for polling
}

KEY_COOLDOWN_S = 300
MODEL_KEY_COOLDOWN_S = 3600
REVOKED_COOLDOWN_S = 86400
MODEL_EOL_COOLDOWN_S = 86400
MODEL_DEAD_COOLDOWN_S = 600
LAST_CYCLE_FAIL_BACKOFF_S = 5.0
TRANSIENT_STATUSES = {404, 408, 500, 502, 503, 504, 529}
MAX_LOG_BYTES = 1_000_000
MAX_ERROR_PREVIEW_CHARS = 240

_REQUEST_ID_RE = re.compile(r"[^A-Za-z0-9._:-]")
_SECRET_RE = re.compile(
    r"(?i)\b(authorization|api[-_ ]?key|token|password|secret)\b\s*[:=]\s*"
    r"(?:\"[^\"]*\"|'[^']*'|[^,\s}\]]+)"
)
CLIENT_ABORT_ERRORS = (ConnectionAbortedError, BrokenPipeError, ConnectionResetError)

_lock = threading.Lock()


# --------------------------------------------------------------------------
# config / storage
# --------------------------------------------------------------------------

def _env_positive_float(name, default):
    try:
        v = float(os.environ.get(name, default))
        if math.isfinite(v) and v > 0:
            return v
    except (TypeError, ValueError):
        pass
    return default


def load_config():
    cfg = dict(DEFAULT_CONFIG)
    try:
        with open(CONFIG_FILE, encoding="utf-8-sig") as f:
            user = json.load(f)
        if isinstance(user, dict):
            cfg.update({k: v for k, v in user.items() if k in DEFAULT_CONFIG})
    except (OSError, ValueError):
        pass
    # env overrides
    cfg["port"] = int(os.environ.get("NIM_PROXY_PORT", cfg["port"]))
    cfg["host"] = os.environ.get("NIM_PROXY_HOST", cfg["host"])
    cfg["max_stream_keys"] = int(os.environ.get("NIM_PROXY_MAX_STREAM_KEYS", cfg["max_stream_keys"]) or 0)
    cfg["buffered_min_tokens"] = int(os.environ.get("NIM_PROXY_BUFFERED_MIN_TOKENS", cfg["buffered_min_tokens"]) or 150000)
    cfg["keepalive_s"] = _env_positive_float("NIM_PROXY_STREAM_KEEPALIVE_S", cfg["keepalive_s"])
    cfg["ttfb_timeout_s"] = _env_positive_float("NIM_PROXY_TTFB_TIMEOUT_S", cfg["ttfb_timeout_s"])
    cfg["upstream_timeout_s"] = _env_positive_float("NIM_PROXY_UPSTREAM_TIMEOUT_S", cfg["upstream_timeout_s"])
    cfg["catalog_refresh_s"] = _env_positive_float("NIM_PROXY_CATALOG_REFRESH_S", cfg["catalog_refresh_s"])
    return cfg


CFG = load_config()
STREAM_KEEPALIVE_S = CFG["keepalive_s"]
FIRST_BYTE_TIMEOUT_S = CFG["ttfb_timeout_s"]
UPSTREAM_TIMEOUT_S = CFG["upstream_timeout_s"]
SLOW_REQUEST_MS = CFG["slow_request_ms"]
MAX_STREAM_KEYS = CFG["max_stream_keys"]
BIG_CTX_BUFFERED_MIN_TOKENS = CFG["buffered_min_tokens"]
STREAM_KEEPALIVE_COMMENT = b": nim-rotator-proxy keepalive\r\n"


def ensure_data_dir():
    os.makedirs(DATA_DIR, exist_ok=True)


def default_keys_doc():
    return {"pool_token": "", "allow_pool_fallback": False, "keys": []}


def load_keys_doc():
    """Parse data/keys.json, keeping the last good copy as fallback."""
    try:
        with open(KEYS_FILE, encoding="utf-8-sig") as f:
            doc = json.load(f)
        if not isinstance(doc, dict):
            raise ValueError
    except (OSError, ValueError):
        cached = _JSON_CACHE.get(KEYS_FILE)
        if cached is not None:
            return cached
        return default_keys_doc()
    _JSON_CACHE[KEYS_FILE] = doc
    return doc


_JSON_CACHE = {}
_stale_logged = {"ts": 0.0}


def _log_stale(path):
    now = time.time()
    if now - _stale_logged["ts"] >= 60:
        _stale_logged["ts"] = now
        log("CONFIG | corrupt/unreadable %s -> serving last-good copy" % os.path.basename(path))


def load_pool_keys():
    """Return the list of enabled pool key strings."""
    doc = load_keys_doc()
    out = []
    for k in doc.get("keys") or []:
        if isinstance(k, dict) and k.get("enabled", True) and k.get("key"):
            out.append(k["key"])
    return out


def load_limits():
    try:
        with open(LIMITS_FILE, encoding="utf-8-sig") as f:
            data = json.load(f)
        if isinstance(data, dict) and data:
            _JSON_CACHE[LIMITS_FILE] = data
            return dict(data)
    except (OSError, ValueError):
        cached = _JSON_CACHE.get(LIMITS_FILE)
        if cached:
            _log_stale(LIMITS_FILE)
            return dict(cached)
    return {}


def log(msg, request_id=None):
    if request_id:
        msg += " | req_id=%s" % request_id
    line = "%s | %s\n" % (time.strftime("%Y-%m-%d %H:%M:%S"), msg)
    try:
        if os.path.exists(LOG_FILE) and os.path.getsize(LOG_FILE) > MAX_LOG_BYTES:
            os.replace(LOG_FILE, LOG_FILE + ".old")
        with open(LOG_FILE, "a", encoding="utf-8") as f:
            f.write(line)
    except OSError:
        pass


def key_id(key):
    """Stable non-reversible id for logging/state (never log raw keys)."""
    return hashlib.sha256(key.encode()).hexdigest()[:12]


def mask_key(key):
    return (key[:10] + "..." + key[-4:]) if len(key) > 18 else "***"


# --------------------------------------------------------------------------
# state (cooldowns / rotation)
# --------------------------------------------------------------------------

class State:
    STATE_SAVE_MIN_INTERVAL_S = 2.0

    def __init__(self):
        self.key_cool = {}        # key_id -> until_ts
        self.model_key_cool = {}  # (model, key_id) -> until_ts
        self.last_used = {}       # key_id -> ts
        self.stats = {}           # key_id -> {"req": n, "ok": n, "429": n, "err": n}
        self.model_dead = {}      # model -> until_ts
        self.last_cycle_fail = 0.0
        self._last_save = 0.0
        self._load()

    def _load(self):
        try:
            with open(STATE_FILE, encoding="utf-8-sig") as f:
                st = json.load(f)
            self.key_cool = {k: float(v) for k, v in st.get("key_cool", {}).items()}
            self.model_key_cool = {
                tuple(k.split("|", 1)): float(v) for k, v in st.get("model_key_cool", {}).items()
            }
            self.last_used = {k: float(v) for k, v in st.get("last_used", {}).items()}
            self.model_dead = {k: float(v) for k, v in st.get("model_dead", {}).items()}
            self.stats = st.get("stats", {})
        except Exception:
            pass

    def save(self, force=False):
        now = time.time()
        if not force and now - self._last_save < self.STATE_SAVE_MIN_INTERVAL_S:
            return
        with _lock:
            self._last_save = time.time()
            tmp = STATE_FILE + ".tmp"
            try:
                with open(tmp, "w", encoding="utf-8") as f:
                    json.dump(
                        {
                            "key_cool": self.key_cool,
                            "model_key_cool": {"%s|%s" % k: v for k, v in self.model_key_cool.items()},
                            "last_used": self.last_used,
                            "model_dead": self.model_dead,
                            "stats": self.stats,
                        },
                        f,
                        indent=1,
                    )
                os.replace(tmp, STATE_FILE)
            except OSError:
                pass

    def healthy(self, keys, model=None):
        """Return (key, keyid) pairs that are not cooling, LRU first."""
        now = time.time()
        out = []
        for k in keys:
            kid = key_id(k)
            if self.key_cool.get(kid, 0) <= now:
                if model is not None and self.model_key_cool.get((model, kid), 0) > now:
                    continue
                out.append((k, kid))
        out.sort(key=lambda kv: self.last_used.get(kv[1], 0))
        return out

    def note_use(self, kid):
        self.last_used[kid] = time.time()
        self.stats.setdefault(kid, {"req": 0, "ok": 0, "429": 0, "err": 0})["req"] += 1
        self.save()

    def note_result(self, kid, kind):
        s = self.stats.setdefault(kid, {"req": 0, "ok": 0, "429": 0, "err": 0})
        s[kind] = s.get(kind, 0) + 1

    def note_429(self, kid, model):
        now = time.time()
        self.key_cool[kid] = now + KEY_COOLDOWN_S
        if model:
            self.model_key_cool[(model, kid)] = now + MODEL_KEY_COOLDOWN_S
        self.note_result(kid, "429")
        self.save(force=True)

    def note_revoked(self, kid):
        self.key_cool[kid] = time.time() + REVOKED_COOLDOWN_S
        self.note_result(kid, "err")
        self.save(force=True)

    def note_ok(self, kid):
        self.note_result(kid, "ok")
        self.save()

    def note_model_dead(self, model, seconds):
        self.model_dead[model] = time.time() + seconds
        self.save(force=True)

    def model_dead_remaining(self, model):
        return self.model_dead.get(model, 0) - time.time()


ensure_data_dir()
STATE = State()


# --------------------------------------------------------------------------
# catalog watcher (detect NIM model additions/removals)
# --------------------------------------------------------------------------

CATALOG_STATUS = {
    "enabled": bool(CFG.get("catalog_enabled")),
    "last_check": 0.0,
    "models": 0,
    "added": [],       # most recent first, capped
    "removed": [],
    "last_error": "",
}
_LAST_BYOK_KEY = None       # in-memory only: fallback poll key for BYOK-only setups
_byok_lock = threading.Lock()


def _remember_byok_key(key):
    global _LAST_BYOK_KEY
    with _byok_lock:
        _LAST_BYOK_KEY = key


def _atomic_write_json(path, obj):
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(obj, f, indent=1)
    os.replace(tmp, path)


def fetch_catalog(key, timeout=30):
    """GET /v1/models upstream with a single key; returns [ids] or None."""
    try:
        conn = http.client.HTTPSConnection(UPSTREAM_HOST, timeout=timeout)
        conn.request("GET", "/v1/models", None, {
            "Authorization": "Bearer " + key,
            "Accept": "application/json",
        })
        resp = conn.getresponse()
        raw = resp.read()
        conn.close()
        if resp.status != 200:
            return None
        return [m.get("id") for m in (json.loads(raw).get("data") or []) if m.get("id")]
    except Exception:
        return None


def check_catalog_once():
    """Poll NIM's model list, diff against the stored snapshot, record changes.

    Cheap by design: one metadata GET per interval, no inference requests."""
    prev = {}
    try:
        with open(CATALOG_FILE, encoding="utf-8-sig") as f:
            prev = json.load(f)
    except (OSError, ValueError):
        prev = {}
    prev_models = set(prev.get("models") or [])

    doc = load_keys_doc()
    poll_key = None
    want_id = CFG.get("catalog_poll_key_id") or ""
    for e in doc.get("keys") or []:
        if e.get("enabled", True) and e.get("key") and (not want_id or e.get("id") == want_id):
            poll_key = e["key"]
            break
    if poll_key is None:
        with _byok_lock:
            poll_key = _LAST_BYOK_KEY
    if not poll_key:
        CATALOG_STATUS["last_error"] = "no key available to poll the catalog"
        return

    models = fetch_catalog(poll_key)
    if models is None:
        CATALOG_STATUS["last_error"] = "catalog fetch failed (upstream unreachable or key rejected)"
        log("CATALOG | fetch failed — keeping previous snapshot")
        return
    CATALOG_STATUS["last_error"] = ""

    now = time.time()
    cur = set(models)
    added = sorted(cur - prev_models) if prev_models else []
    removed = sorted(prev_models - cur) if prev_models else []
    _atomic_write_json(CATALOG_FILE, {"checked_at": now, "models": models})

    if added or removed:
        _atomic_write_json(CATALOG_DIFF_FILE, {
            "checked_at": now, "added": added, "removed": removed,
            "previous_count": len(prev_models), "current_count": len(models),
        })
    for m in added:
        log("CATALOG | + %s (new model available upstream)" % m)
    for m in removed:
        log("CATALOG | - %s (removed from NIM catalog)" % m)
        # fail fast for the next hour instead of learning about it per-request
        STATE.note_model_dead(m, 3600)

    CATALOG_STATUS.update({
        "last_check": now,
        "models": len(models),
        "added": (added + CATALOG_STATUS.get("added", []))[:20],
        "removed": (removed + CATALOG_STATUS.get("removed", []))[:20],
    })


def catalog_loop():
    # first check shortly after startup (server must bind first), then sleep long
    time.sleep(60)
    while True:
        try:
            check_catalog_once()
        except Exception as e:
            CATALOG_STATUS["last_error"] = "check crashed: %s" % type(e).__name__
            log("CATALOG | check crashed: %s" % type(e).__name__)
        time.sleep(max(60, float(CFG.get("catalog_refresh_s") or 21600)))


# --------------------------------------------------------------------------
# token estimation + context guard
# --------------------------------------------------------------------------

def estimate_tokens(messages, tools=None):
    """Chars/2.8 calibration against NIM prompt_tokens (2026-09-16).

    Over-counts prose (safe direction); never under-counts code."""
    chars = 0
    count = 0
    for m in messages if isinstance(messages, list) else []:
        c = m.get("content")
        if isinstance(c, str):
            chars += len(c)
        elif isinstance(c, list):
            for part in c:
                if isinstance(part, dict) and isinstance(part.get("text"), str):
                    chars += len(part["text"])
        tool_calls = m.get("tool_calls")
        if isinstance(tool_calls, list):
            for tc in tool_calls:
                if isinstance(tc, dict):
                    fn = tc.get("function") or {}
                    chars += len(fn.get("name") or "")
                    chars += len(fn.get("arguments") or "")
        count += 1
    if isinstance(tools, list):
        for t in tools:
            try:
                chars += len(json.dumps(t))
            except Exception:
                pass
    return int(chars / 2.8) + count * 8


def _status_class(status, detail=None):
    detail = (detail or "").lower()
    if "degraded" in detail:
        return "degraded_model"
    if status == 429:
        return "rate_limit"
    if 500 <= status <= 599:
        return "upstream_5xx"
    if 200 <= status <= 299:
        return "success"
    return "proxy_error"


def _error_detail_preview(body, secrets=()):
    if not body:
        return ""
    try:
        text = body.decode("utf-8", errors="replace")
    except AttributeError:
        text = str(body)
    for secret in secrets:
        if secret:
            text = text.replace(secret, "[REDACTED]")
    try:
        parsed = json.loads(text)
    except (TypeError, ValueError):
        detail = text
    else:
        detail = ""
        if isinstance(parsed, dict):
            error = parsed.get("error", parsed)
            if isinstance(error, dict):
                for field in ("message", "detail", "title"):
                    if isinstance(error.get(field), str):
                        detail = error[field]
                        break
            elif isinstance(error, str):
                detail = error
            if not detail:
                for field in ("message", "detail", "title"):
                    if isinstance(parsed.get(field), str):
                        detail = parsed[field]
                        break
    text = _SECRET_RE.sub(lambda m: "%s=[REDACTED]" % m.group(1), detail)
    text = re.sub(r"(?i)\b(prompt|messages|content|input|request_body|body)\b\s*[:=].*", r"\1=[REDACTED]", text)
    text = re.sub(r"\s+", " ", text)
    text = "".join(c if 32 <= ord(c) < 127 and c != "|" else " " for c in text)
    return text[:MAX_ERROR_PREVIEW_CHARS].strip()


def _wait_readable(sock, timeout_s):
    if sock is None:
        return True
    try:
        if hasattr(sock, "pending") and callable(sock.pending) and sock.pending() > 0:
            return True
        r, _, _ = select.select([sock], [], [], max(0.0, float(timeout_s)))
        return bool(r)
    except (OSError, ValueError):
        return True


# --------------------------------------------------------------------------
# HTTP handler
# --------------------------------------------------------------------------

class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, fmt, *args):
        pass

    # ---- helpers ----

    def _request_id(self):
        incoming = self.headers.get("X-Request-ID", "")
        request_id = _REQUEST_ID_RE.sub("", incoming)[:64]
        return request_id or uuid.uuid4().hex[:12]

    def _client_host(self):
        host, _ = self.client_address[:2]
        return host

    def _is_local(self):
        return self._client_host() in ("127.0.0.1", "::1", "localhost")

    def _send_json(self, code, obj):
        body = json.dumps(obj).encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _sse_error(self, code, message):
        """Relay an error as an SSE envelope (headers already sent)."""
        payload = b"data: " + json.dumps({"error": {"message": message, "code": code}}).encode("utf-8") + b"\n\n"
        try:
            self.wfile.write(("%x" % len(payload)).encode("ascii") + b"\r\n")
            self.wfile.write(payload)
            self.wfile.write(b"\r\n")
            done = b"data: [DONE]\n\n"
            self.wfile.write(("%x" % len(done)).encode("ascii") + b"\r\n")
            self.wfile.write(done)
            self.wfile.write(b"\r\n")
            self.wfile.write(b"0\r\n\r\n")
            self.wfile.flush()
        except Exception:
            pass

    # ---- auth ----

    def _resolve_auth(self):
        """Return (mode, key_or_None). mode in {"byok", "pool", "denied"}."""
        auth = self.headers.get("Authorization", "")
        token = auth[7:].strip() if auth.lower().startswith("bearer ") else ""
        if token.startswith("nvapi-"):
            return "byok", token
        pool_token = load_keys_doc().get("pool_token") or ""
        if token and pool_token and token == pool_token:
            return "pool", None
        if self._is_local():
            # loopback is trusted (local single-user setups)
            return "pool", None
        return "denied", None

    # ---- routes ----

    def do_GET(self):
        if self.path in ("/health", "/healthz"):
            pool = load_pool_keys()
            cs = dict(CATALOG_STATUS)
            cs["next_check_in_s"] = max(0, int(
                (60 if not cs["last_check"] else
                 cs["last_check"] + max(60, float(CFG.get("catalog_refresh_s") or 21600)))
                - time.time())) if cs.get("enabled") else None
            self._send_json(200, {
                "ok": True,
                "service": "nim-rotator-proxy",
                "version": __version__,
                "pool_keys": len(pool),
                "host": CFG["host"],
                "port": CFG["port"],
                "catalog": cs,
            })
            return
        if self.path.startswith("/v1/"):
            self._proxy("GET", self.path, None)
            return
        self._send_json(404, {"error": {"message": "not found"}})

    def do_POST(self):
        length = int(self.headers.get("Content-Length") or 0)
        body = self.rfile.read(length) if length else b""
        self._proxy("POST", self.path, body)

    def do_DELETE(self):
        self._send_json(404, {"error": {"message": "not found"}})

    # ---- core proxy ----

    def _pick_upstream_keys(self, mode, byok_key, model):
        """Return (keys, mode_used, warning)."""
        if mode == "byok":
            if CFG.get("allow_pool_fallback"):
                return [byok_key] + load_pool_keys(), "byok+pool", None
            return [byok_key], "byok", None
        pool = load_pool_keys()
        if not pool:
            return [], "pool", "no pool keys configured (run: python proxy.py keys)"
        return pool, "pool", None

    def _proxy(self, method, path, body):
        request_id = self._request_id()
        self._active_request_id = request_id
        self._client_aborted = False
        self._convert_prelude_done = False
        self._convert_keepalive = None

        mode, byok_key = self._resolve_auth()
        if mode == "denied":
            log("AUTH | denied non-local request without valid key", request_id)
            self._send_json(401, {"error": {
                "message": "[nim-rotator-proxy] unauthorized: send your own nvapi-... key "
                           "as the Bearer token, or the proxy pool_token."}})
            return
        if mode == "byok" and byok_key:
            _remember_byok_key(byok_key)

        # ---- parse chat payload + pre-flight context guard ----
        model = None
        want_stream = False
        payload = None
        est = 0
        converted_stream = False
        if body and path.endswith("/chat/completions"):
            try:
                payload = json.loads(body.decode("utf-8"))
                model = payload.get("model")
                want_stream = bool(payload.get("stream"))
            except Exception:
                payload = None

            if payload is not None:
                # scrub enable_thinking (unsupported parameter on several models)
                if "enable_thinking" in payload:
                    payload.pop("enable_thinking", None)
                    body = json.dumps(payload).encode("utf-8")
                    log("SCRUB | enable_thinking removed", request_id)
                # scrub invalid max_tokens (clients compute negatives from stale windows)
                mt = payload.get("max_tokens")
                if isinstance(mt, (int, float)) and mt < 1:
                    payload.pop("max_tokens", None)
                    body = json.dumps(payload).encode("utf-8")
                    log("SCRUB | invalid max_tokens %s removed" % mt, request_id)
                limits = load_limits()
                ctx = limits.get(model, CFG["default_context"])
                est = estimate_tokens(payload.get("messages"), payload.get("tools"))
                cap = int(ctx * CFG["guard_ratio"])
                if est > cap:
                    log("GUARD | model=%s est=%d cap=%d -> rejected" % (model, est, cap), request_id)
                    self._send_json(400, {"error": {"message": (
                        "[nim-rotator-proxy] context guard: ~%d estimated tokens exceed "
                        "%.0f%% of %d context for '%s'. Reduce the conversation."
                        % (est, CFG["guard_ratio"] * 100, ctx, model))}})
                    return

        # ---- big-context streaming conversion ----
        if want_stream and payload is not None and est >= BIG_CTX_BUFFERED_MIN_TOKENS:
            payload["stream"] = False
            payload.pop("stream_options", None)
            body = json.dumps(payload).encode("utf-8")
            converted_stream = True
            log("STREAM_CONVERT | est=%d >= %d -> buffered upstream + SSE emulation"
                % (est, BIG_CTX_BUFFERED_MIN_TOKENS), request_id)

        if STATE.last_cycle_fail and time.time() - STATE.last_cycle_fail < LAST_CYCLE_FAIL_BACKOFF_S:
            self._send_json(502, {"error": {"message":
                "[nim-rotator-proxy] upstream failed a full key cycle moments ago; retry shortly."}})
            return

        chain = [model]
        if model is not None:
            for mid in CFG.get("fallback_models") or []:
                if mid != model and mid not in chain:
                    chain.append(mid)

        overall_status = None
        for chain_pos, cand in enumerate(chain):
            is_fallback = chain_pos > 0
            if cand is not None:
                rem = STATE.model_dead_remaining(cand)
                if rem > 0:
                    log("MODEL_FALLBACK | skip %s (dead cooldown %.0fs left)" % (cand, rem), request_id)
                    continue
                if is_fallback and payload is not None:
                    ctx = load_limits().get(cand, CFG["default_context"])
                    cap = int(ctx * CFG["guard_ratio"])
                    if est > cap:
                        log("MODEL_FALLBACK | skip %s (est %d > cap %d)" % (cand, est, cap), request_id)
                        continue
            body_cand = body
            if is_fallback and payload is not None and cand is not None:
                payload["model"] = cand
                body_cand = json.dumps(payload).encode("utf-8")

            keys, mode_used, warn = self._pick_upstream_keys(mode, byok_key, cand)
            if warn:
                log("KEYS | %s | model=%s" % (warn, cand), request_id)
                self._send_json(503, {"error": {"message": "[nim-rotator-proxy] " + warn}})
                return
            healthy = STATE.healthy(keys, cand)
            if not healthy:
                log("EXHAUSTED | all keys cooling | model=%s mode=%s" % (cand, mode_used), request_id)
                STATE.last_cycle_fail = time.time()
                self._send_json(429, {"error": {"message":
                    "[nim-rotator-proxy] all available keys are cooling down; retry later."}})
                return

            statuses = []
            saw_410 = False
            last_status = None
            fallback_headers = {"X-Nim-Proxy-Model-Fallback": cand} if is_fallback else None

            attempt_keys = healthy[:MAX_STREAM_KEYS] if (want_stream and MAX_STREAM_KEYS > 0) else healthy

            if converted_stream and not getattr(self, "_convert_prelude_done", False):
                # NIM holds HTTP headers for buffered responses until compute
                # finishes; clients with ~60s TTFB limits abort long before.
                # Ship the SSE prelude now and keep the stream warm from a
                # side thread while upstream (and any key retries) run.
                try:
                    self._send_convert_prelude(fallback_headers, model=cand)
                    keep_stop = threading.Event()
                    keep_thread = threading.Thread(
                        target=self._convert_keepalive_loop, args=(keep_stop,), daemon=True)
                    keep_thread.start()
                    self._convert_keepalive = (keep_stop, keep_thread)
                    log("STREAM_CONVERT | prelude sent + keepalive thread on", request_id)
                except CLIENT_ABORT_ERRORS:
                    self._client_aborted = True
                    log("CLIENT_ABORT | convert prelude", request_id)
                    return
            for pos, (upkey, kid) in enumerate(attempt_keys):
                t0 = time.monotonic()
                self._active_request_secrets = (upkey,)
                try:
                    conn = http.client.HTTPSConnection(UPSTREAM_HOST, timeout=UPSTREAM_TIMEOUT_S)
                    hdrs = {
                        "Content-Type": self.headers.get("Content-Type", "application/json"),
                        "Accept": "application/json",
                        "Accept-Encoding": "identity",
                        "Content-Length": str(len(body_cand) if body_cand else 0),
                        "Authorization": "Bearer " + upkey,
                    }
                    conn.request(method, path, body=body_cand, headers=hdrs)
                    resp = conn.getresponse()
                    status = resp.status

                    if status == 429:
                        STATE.note_429(kid, cand)
                        elapsed_ms = (time.monotonic() - t0) * 1000
                        slow = " | SLOW" if elapsed_ms >= SLOW_REQUEST_MS else ""
                        log("429 | key#%s | model=%s | %.0fms -> cooling | class=rate_limit%s"
                            % (kid, cand, elapsed_ms, slow), request_id)
                        last_status = 429
                        continue
                    if status in (401, 403):
                        STATE.note_revoked(kid)
                        log("%d | key#%s -> revoked, cooling 24h | class=proxy_error" % (status, kid), request_id)
                        last_status = status
                        continue
                    if status == 410:
                        saw_410 = True
                        last_status = 410
                        STATE.note_model_dead(cand, MODEL_EOL_COOLDOWN_S)
                        log("MODEL-EOL | 410 | key#%s | model=%s -> dead %dh"
                            % (kid, cand, MODEL_EOL_COOLDOWN_S // 3600), request_id)
                        try:
                            resp.close()
                        finally:
                            conn.close()
                        break
                    if status in TRANSIENT_STATUSES:
                        log("RETRY | %d | key#%s | model=%s -> trying next key | class=%s"
                            % (status, kid, cand, _status_class(status)), request_id)
                        statuses.append(status)
                        last_status = status
                        try:
                            resp.close()
                        except Exception:
                            pass
                        continue

                    STATE.note_use(kid)
                    elapsed_ms = (time.monotonic() - t0) * 1000
                    slow = " | SLOW" if elapsed_ms >= SLOW_REQUEST_MS else ""
                    log("%d | key#%s | model=%s | mode=%s | stream=%s | %.0fms | class=%s%s"
                        % (status, kid, cand, mode_used, want_stream, elapsed_ms,
                           _status_class(status), slow), request_id)

                    relay_failed = False
                    detail_preview = ""
                    stream_result = "ok"
                    try:
                        if status == 200 and want_stream and not converted_stream:
                            stream_result = self._pipe_stream(
                                resp, model=cand, is_last=(pos + 1 >= len(attempt_keys)),
                                extra_headers=fallback_headers)
                        elif status == 200 and converted_stream:
                            self._stop_convert_keepalive()
                            stream_result = self._emulate_sse(
                                resp, model=cand, is_last=(pos + 1 >= len(attempt_keys)),
                                extra_headers=fallback_headers,
                                headers_sent=self._convert_prelude_done)
                        else:
                            detail_preview = self._pipe_buffered(resp, extra_headers=fallback_headers)
                    except CLIENT_ABORT_ERRORS:
                        self._client_aborted = True
                        pass
                    except Exception as e:
                        if isinstance(e, CLIENT_ABORT_ERRORS):
                            self._client_aborted = True
                        error_class = "client_abort" if isinstance(e, CLIENT_ABORT_ERRORS) else "proxy_error"
                        log("RELAYERR | key#%s | %s | class=%s" % (kid, type(e).__name__, error_class), request_id)
                        relay_failed = True

                    STATE.note_ok(kid) if (status == 200 and not relay_failed) else None

                    if detail_preview:
                        log("UPSTREAM_DETAIL | status=%d | class=%s | preview=%s"
                            % (status, _status_class(status, detail_preview), detail_preview), request_id)
                    if self._client_aborted:
                        log("CLIENT_ABORT | key#%s | class=client_abort" % kid, request_id)

                    if stream_result == "retry" and pos + 1 < len(attempt_keys):
                        log("STREAMRETRY | key#%s | model=%s -> trying next key | class=upstream_5xx"
                            % (kid, cand), request_id)
                        last_status = 502
                        try:
                            resp.close()
                        finally:
                            conn.close()
                        continue

                    try:
                        resp.close()
                    finally:
                        conn.close()

                    if relay_failed and pos + 1 < len(attempt_keys):
                        last_status = 502
                        continue
                    return

                except (OSError, http.client.HTTPException) as e:
                    error_class = "client_abort" if isinstance(e, CLIENT_ABORT_ERRORS) else "network_error"
                    log("NETERR | key#%s | %s | class=%s" % (kid, type(e).__name__, error_class), request_id)
                    last_status = 502
                    continue
                except Exception as e:
                    log("PROXYERR | key#%s | %s | class=proxy_error" % (kid, type(e).__name__), request_id)
                    last_status = 502
                    continue

            overall_status = last_status
            if saw_410:
                continue
            if statuses and all(s == 404 for s in statuses):
                STATE.note_model_dead(cand, MODEL_DEAD_COOLDOWN_S)
                log("MODEL_FALLBACK | %s dead (404 on %d key(s)) -> trying next model"
                    % (cand, len(statuses)), request_id)
                continue
            STATE.last_cycle_fail = time.time()
            msg = "[nim-rotator-proxy] upstream failed after trying %d key(s)" % len(attempt_keys)
            if last_status == 429:
                msg = "[nim-rotator-proxy] all available keys rate-limited (429); retry later."
            if getattr(self, "_convert_prelude_done", False):
                self._stop_convert_keepalive()
                self._sse_error(last_status or 502, msg)
                return
            self._send_json(last_status or 502, {"error": {"message": msg}})
            return

        if getattr(self, "_convert_prelude_done", False):
            self._stop_convert_keepalive()
            self._sse_error(overall_status or 502,
                            "[nim-rotator-proxy] requested model and all fallback models failed upstream")
            return
        self._send_json(overall_status or 502, {"error": {
            "message": "[nim-rotator-proxy] requested model and all fallback models failed upstream"}})

    # ---- converted-stream helpers ----

    def _send_convert_prelude(self, extra_headers=None, model=None):
        """SSE headers + role chunk immediately (NIM holds headers for buffered
        responses; clients with ~60s TTFB limits would abort otherwise)."""
        self.send_response(200)
        for hk, hv in (extra_headers or {}).items():
            self.send_header(hk, hv)
        self.send_header("Content-Type", "text/event-stream")
        self.send_header("Transfer-Encoding", "chunked")
        self.send_header("Cache-Control", "no-cache")
        self.end_headers()
        env = {
            "id": "chatcmpl-proxy",
            "object": "chat.completion.chunk",
            "created": int(time.time()),
            "model": model or "",
            "service_tier": None,
            "system_fingerprint": None,
        }
        env["choices"] = [{
            "index": 0,
            "delta": {"role": "assistant", "content": ""},
            "logprobs": None,
            "finish_reason": None,
        }]
        env["usage"] = None
        payload = ("data: " + json.dumps(env) + "\n\n").encode("utf-8")
        self.wfile.write(("%x" % len(payload)).encode("ascii") + b"\r\n")
        self.wfile.write(payload)
        self.wfile.write(b"\r\n")
        self.wfile.flush()
        self._convert_prelude_done = True

    def _convert_keepalive_loop(self, stop_event):
        comment = STREAM_KEEPALIVE_COMMENT
        while not stop_event.wait(STREAM_KEEPALIVE_S):
            try:
                self.wfile.write(("%x" % len(comment)).encode("ascii") + b"\r\n")
                self.wfile.write(comment)
                self.wfile.write(b"\r\n")
                self.wfile.flush()
                log("STREAM_CONVERT | keepalive", getattr(self, "_active_request_id", None))
            except Exception:
                break

    def _stop_convert_keepalive(self):
        kt = getattr(self, "_convert_keepalive", None)
        if kt:
            stop, th = kt
            stop.set()
            try:
                th.join(timeout=2.0)
            except Exception:
                pass
            self._convert_keepalive = None

    def _emulate_sse(self, upstream_resp, model=None, is_last=True, extra_headers=None, headers_sent=False):
        """Replay a buffered upstream JSON response as OpenAI-style SSE deltas.

        Returns "ok" (replayed or error relayed) or "aborted" (client gone)."""
        NL = chr(10)
        CRLF = chr(13) + chr(10)
        NLB = CRLF.encode("ascii")

        def send_chunk(payload):
            self.wfile.write(("%x" % len(payload)).encode("ascii") + NLB)
            self.wfile.write(payload)
            self.wfile.write(NLB)

        rid = getattr(self, "_active_request_id", None)

        if not headers_sent:
            self.send_response(200)
            for hk, hv in (extra_headers or {}).items():
                self.send_header(hk, hv)
            self.send_header("Content-Type", "text/event-stream")
            self.send_header("Transfer-Encoding", "chunked")
            self.send_header("Cache-Control", "no-cache")
            self.end_headers()

            env0 = {
                "id": "chatcmpl-proxy",
                "object": "chat.completion.chunk",
                "created": int(time.time()),
                "model": model or "",
                "service_tier": None,
                "system_fingerprint": None,
            }

            def delta0(d):
                chunk = dict(env0)
                chunk["choices"] = [{
                    "index": 0,
                    "delta": d,
                    "logprobs": None,
                    "finish_reason": None,
                }]
                chunk["usage"] = None
                send_chunk((("data: " ) + json.dumps(chunk) + NL + NL).encode("utf-8"))

            try:
                delta0({"role": "assistant", "content": ""})
                self.wfile.flush()
            except CLIENT_ABORT_ERRORS:
                self._client_aborted = True
                log("CLIENT_ABORT | before convert headers ack", rid)
                return "aborted"

        SSE = "data: "
        DONE = ("data: [DONE]" + NL + NL)
        env = {
            "id": "chatcmpl-proxy",
            "object": "chat.completion.chunk",
            "created": int(time.time()),
            "model": model or "",
            "service_tier": None,
            "system_fingerprint": None,
        }

        def delta(d, finish_reason=None, usage=None):
            chunk = dict(env)
            chunk["choices"] = [{
                "index": 0,
                "delta": d,
                "logprobs": None,
                "finish_reason": finish_reason,
            }]
            chunk["usage"] = usage
            send_chunk((SSE + json.dumps(chunk) + NL + NL).encode("utf-8"))

        # wait for the buffered body, emitting keepalives while NIM computes
        upstream_fp = getattr(upstream_resp, "fp", None)
        upstream_raw = getattr(upstream_fp, "raw", None)
        socket_candidates = (
            getattr(upstream_resp, "_sock", None),
            getattr(upstream_fp, "_sock", None),
            getattr(upstream_raw, "_sock", None),
        )
        active_sock = None
        for s in socket_candidates:
            if s is not None and hasattr(s, "fileno"):
                active_sock = s
                break
        if active_sock is not None and callable(getattr(active_sock, "settimeout", None)):
            try:
                active_sock.settimeout(UPSTREAM_TIMEOUT_S)
            except (OSError, AttributeError):
                pass

        consecutive_idles = 0
        MAX_CONV_IDLES = 80  # 80 * keepalive_s compute ceiling
        try:
            while True:
                if active_sock is None or _wait_readable(active_sock, STREAM_KEEPALIVE_S):
                    break
                try:
                    send_chunk(STREAM_KEEPALIVE_COMMENT)
                    self.wfile.flush()
                except CLIENT_ABORT_ERRORS:
                    self._client_aborted = True
                    log("CLIENT_ABORT | during convert keepalive", rid)
                    return "aborted"
                consecutive_idles += 1
                log("STREAM_CONVERT | keepalive count=%d" % consecutive_idles, rid)
                if consecutive_idles >= MAX_CONV_IDLES:
                    log("STREAM_CONVERT | idle limit exceeded -> error envelope", rid)
                    try:
                        err = json.dumps({"error": {
                            "message": "[nim-rotator-proxy] upstream timed out during buffered conversion",
                            "type": "timeout", "code": 504}}).encode("utf-8")
                        send_chunk(b"data: " + err + b"\n\n")
                        send_chunk(DONE.encode("ascii"))
                        self.wfile.write(b"0" + bytes([13, 10]) + bytes([13, 10]))
                        self.wfile.flush()
                    except Exception:
                        pass
                    return "ok"
            raw = upstream_resp.read()
        except CLIENT_ABORT_ERRORS:
            self._client_aborted = True
            return "aborted"
        except Exception as e:
            log("STREAM_CONVERT | upstream read failed: %s" % type(e).__name__, rid)
            err = json.dumps({"error": {"message":
                "[nim-rotator-proxy] upstream read failed after buffered conversion."}}).encode("utf-8")
            try:
                send_chunk(b"data: " + err + b"\n\n")
                send_chunk(DONE.encode("ascii"))
                self.wfile.write(b"0" + bytes([13, 10]) + bytes([13, 10]))
                self.wfile.flush()
            except Exception:
                pass
            return "ok"

        try:
            obj = json.loads(raw.decode("utf-8"))
        except Exception:
            obj = None
        if not isinstance(obj, dict) or obj.get("error"):
            log("STREAM_CONVERT | upstream error envelope -> SSE error relay", rid)
            err = obj.get("error") if isinstance(obj, dict) else None
            payload = json.dumps({"error": err or {"message":
                "[nim-rotator-proxy] upstream error envelope in buffered response."}}).encode("utf-8")
            try:
                send_chunk(b"data: " + payload + b"\n\n")
                send_chunk(DONE.encode("ascii"))
                self.wfile.write(b"0" + bytes([13, 10]) + bytes([13, 10]))
                self.wfile.flush()
            except CLIENT_ABORT_ERRORS:
                self._client_aborted = True
            return "ok"

        env["id"] = obj.get("id") or env["id"]
        env["created"] = obj.get("created") or env["created"]
        env["model"] = obj.get("model") or env["model"]

        choices = obj.get("choices") or [{}]
        ch0 = choices[0] if choices else {}
        msg = ch0.get("message") or {}
        content = msg.get("content") or ""
        reasoning = msg.get("reasoning_content") or msg.get("reasoning") or ""
        finish = ch0.get("finish_reason") or "stop"

        if reasoning:
            delta({"reasoning_content": reasoning})
        piece = (len(content) + 7) // 8 if content else 0
        for i in range(0, len(content), piece or 1):
            delta({"content": content[i:i + piece]})
        tool_calls = msg.get("tool_calls") or []
        for ti, tc in enumerate(tool_calls):
            if not isinstance(tc, dict):
                continue
            fn = tc.get("function") or {}
            # First chunk carries id+name with empty arguments; continuations
            # mirror NIM's native style (explicit nulls), which AI-SDK parsers
            # handle natively.
            first_tc = {
                "index": tc.get("index", ti),
                "id": tc.get("id"),
                "type": tc.get("type") or "function",
                "function": {"name": fn.get("name") or "", "arguments": ""},
            }
            delta({"tool_calls": [first_tc]})
            args = fn.get("arguments") or ""
            if args:
                delta({"tool_calls": [{
                    "index": tc.get("index", ti),
                    "id": None,
                    "type": None,
                    "function": {"name": None, "arguments": args},
                }]})
        delta({}, finish_reason=finish, usage=obj.get("usage"))
        send_chunk(DONE.encode("ascii"))
        try:
            self.wfile.write(b"0" + bytes([13, 10]) + bytes([13, 10]))
        except CLIENT_ABORT_ERRORS:
            pass
        tools = msg.get("tool_calls") or []
        tools_dbg = [
            {"name": ((tc.get("function") or {}).get("name") if isinstance(tc, dict) else None),
             "args_len": len((tc.get("function") or {}).get("arguments") or "") if isinstance(tc, dict) else -1}
            for tc in tools if isinstance(tc, dict)
        ]
        log("STREAM_CONVERT | replayed %d content chars | finish=%s | reasoning=%d chars | tools=%s"
            % (len(content), finish, len(reasoning), json.dumps(tools_dbg)), rid)
        return "ok"

    def _pipe_stream(self, upstream_resp, model=None, is_last=True, extra_headers=None):
        """Forward an SSE/chunked upstream body using chunked TE.
        Returns "ok", "retry" (nothing sent yet) or "aborted"."""

        def send_chunk(payload):
            self.wfile.write(("%x\r\n" % len(payload)).encode("ascii"))
            self.wfile.write(payload)
            self.wfile.write(b"\r\n")

        try:
            upstream_fp = getattr(upstream_resp, "fp", None)
            upstream_raw = getattr(upstream_fp, "raw", None)
            socket_candidates = (
                getattr(upstream_resp, "_sock", None),
                getattr(upstream_fp, "_sock", None),
                getattr(upstream_raw, "_sock", None),
            )
            active_sock = None
            for s in socket_candidates:
                if s is not None and hasattr(s, "fileno"):
                    active_sock = s
                    break

            if active_sock is not None and callable(getattr(active_sock, "settimeout", None)):
                try:
                    active_sock.settimeout(UPSTREAM_TIMEOUT_S)
                except (OSError, AttributeError):
                    pass

            def reset_raw_timeout():
                if upstream_raw is not None and hasattr(upstream_raw, "_timeout_occurred"):
                    upstream_raw._timeout_occurred = False

            # ---- first-chunk peek (nothing sent to the client yet) ----
            peek_buf = b""
            t_start = time.monotonic()
            while True:
                reset_raw_timeout()
                readable = _wait_readable(active_sock, 1.0)
                if not readable:
                    if time.monotonic() - t_start >= FIRST_BYTE_TIMEOUT_S:
                        if is_last:
                            self._send_json(504, {"error": {
                                "message": "[nim-rotator-proxy] upstream timed out waiting for first byte (TTFB)",
                                "type": "timeout", "code": 504}})
                            return "ok"
                        return "retry"
                    continue
                try:
                    chunk = upstream_resp.read1(8192)
                except (socket.timeout, TimeoutError):
                    reset_raw_timeout()
                    if time.monotonic() - t_start >= FIRST_BYTE_TIMEOUT_S:
                        if is_last:
                            self._send_json(504, {"error": {
                                "message": "[nim-rotator-proxy] upstream timed out waiting for first byte (TTFB)",
                                "type": "timeout", "code": 504}})
                            return "ok"
                        return "retry"
                    continue
                except Exception as e:
                    reset_raw_timeout()
                    if isinstance(e, CLIENT_ABORT_ERRORS):
                        self._client_aborted = True
                        log("STREAMRETRY | client aborted before first byte: %s" % type(e).__name__,
                            getattr(self, "_active_request_id", None))
                        return "aborted"
                    if is_last:
                        self._send_json(502, {"error": {
                            "message": "[nim-rotator-proxy] upstream closed the stream before sending any data: %s" % str(e),
                            "type": "server_error", "code": 502}})
                        log("STREAMRETRY | died before first byte: %s (%s) | no keys left"
                            % (type(e).__name__, str(e)), getattr(self, "_active_request_id", None))
                        return "ok"
                    log("STREAMRETRY | died before first byte: %s (%s) -> retry"
                        % (type(e).__name__, str(e)), getattr(self, "_active_request_id", None))
                    return "retry"

                if not chunk:
                    if is_last:
                        self._send_json(502, {"error": {
                            "message": "[nim-rotator-proxy] upstream closed the stream before sending any data",
                            "type": "server_error", "code": 502}})
                        return "ok"
                    return "retry"

                peek_buf += chunk
                if b"\n\n" in peek_buf or len(peek_buf) > 65536:
                    break

            first_is_error = False
            if peek_buf:
                head = peek_buf.split(b"\n\n", 1)[0]
                for raw_line in head.split(b"\n"):
                    line = raw_line.strip()
                    if not line or line.startswith(b":"):
                        continue
                    if line.startswith(b"data:"):
                        payload = line[5:].strip()
                        if payload == b"[DONE]":
                            break
                        try:
                            parsed = json.loads(payload.decode("utf-8", errors="replace"))
                        except (ValueError, AttributeError):
                            break
                        if isinstance(parsed, dict) and "error" in parsed:
                            first_is_error = True
                        break
            if first_is_error and not is_last:
                log("STREAMRETRY | first event is upstream error envelope -> trying next key",
                    getattr(self, "_active_request_id", None))
                return "retry"
            if first_is_error:
                log("STREAMRETRY | first event is upstream error envelope, no keys left -> relaying",
                    getattr(self, "_active_request_id", None))

            self.send_response(upstream_resp.status)
            for hk, hv in (extra_headers or {}).items():
                self.send_header(hk, hv)
            ct = upstream_resp.getheader("Content-Type", "text/event-stream")
            self.send_header("Content-Type", ct)
            self.send_header("Transfer-Encoding", "chunked")
            self.send_header("Cache-Control", "no-cache")
            self.end_headers()

            data_bytes = 0
            data_chunks = 0
            keepalive_count = 0
            consecutive_idles = 0
            MAX_CONSECUTIVE_IDLES = 40  # 40 * keepalive_s total silence

            if peek_buf:
                send_chunk(peek_buf)
                self.wfile.flush()
                data_bytes += len(peek_buf)
                data_chunks += 1

            while True:
                reset_raw_timeout()
                readable = _wait_readable(active_sock, STREAM_KEEPALIVE_S)
                if not readable:
                    try:
                        send_chunk(STREAM_KEEPALIVE_COMMENT)
                        self.wfile.flush()
                    except CLIENT_ABORT_ERRORS:
                        self._client_aborted = True
                        log("CLIENT_ABORT | during keepalive", getattr(self, "_active_request_id", None))
                        return "aborted"
                    except Exception as e:
                        if isinstance(e, CLIENT_ABORT_ERRORS):
                            self._client_aborted = True
                            return "aborted"
                        raise
                    keepalive_count += 1
                    consecutive_idles += 1
                    log("STREAM_KEEPALIVE | count=%d | consecutive=%d"
                        % (keepalive_count, consecutive_idles), getattr(self, "_active_request_id", None))
                    if consecutive_idles >= MAX_CONSECUTIVE_IDLES:
                        log("STREAMERR | upstream idle limit exceeded (%d idles)" % consecutive_idles,
                            getattr(self, "_active_request_id", None))
                        err = json.dumps({"error": {
                            "message": "[nim-rotator-proxy] upstream idle timeout; no data received for ~10 minutes",
                            "type": "timeout", "code": 504}}).encode("utf-8")
                        try:
                            send_chunk(b"data: " + err + b"\n\n")
                            self.wfile.write(b"0\r\n\r\n")
                            self.wfile.flush()
                        except Exception:
                            pass
                        return "ok"
                    continue

                try:
                    chunk = upstream_resp.read1(8192)
                except (socket.timeout, TimeoutError):
                    reset_raw_timeout()
                    continue
                except Exception as e:
                    reset_raw_timeout()
                    if isinstance(e, CLIENT_ABORT_ERRORS):
                        self._client_aborted = True
                        return "aborted"
                    log("STREAMERR | upstream died mid-stream: %s (%s)"
                        % (type(e).__name__, str(e)), getattr(self, "_active_request_id", None))
                    err = json.dumps({"error": {
                        "message": "[nim-rotator-proxy] upstream stream terminated unexpectedly; retry the request",
                        "type": "server_error", "code": 500}}).encode("utf-8")
                    try:
                        send_chunk(b"data: " + err + b"\n\n")
                        self.wfile.write(b"0\r\n\r\n")
                        self.wfile.flush()
                    except Exception:
                        self._client_aborted = True
                    return "ok"

                if not chunk:
                    break
                consecutive_idles = 0
                try:
                    send_chunk(chunk)
                    self.wfile.flush()
                except CLIENT_ABORT_ERRORS:
                    self._client_aborted = True
                    return "aborted"
                except Exception as e:
                    if isinstance(e, CLIENT_ABORT_ERRORS):
                        self._client_aborted = True
                        return "aborted"
                    raise
                data_bytes += len(chunk)
                data_chunks += 1

            self.wfile.write(b"0\r\n\r\n")
            self.wfile.flush()
            log("STREAM_END | data_bytes=%d | data_chunks=%d | keepalive_count=%d"
                % (data_bytes, data_chunks, keepalive_count), getattr(self, "_active_request_id", None))
            return "ok"
        except CLIENT_ABORT_ERRORS:
            self._client_aborted = True
            return "aborted"

    def _pipe_buffered(self, upstream_resp, extra_headers=None):
        body = upstream_resp.read()
        detail = ""
        if not 200 <= upstream_resp.status <= 299:
            detail = _error_detail_preview(body, getattr(self, "_active_request_secrets", ()))
        self.send_response(upstream_resp.status)
        for hk, hv in (extra_headers or {}).items():
            self.send_header(hk, hv)
        ct = upstream_resp.getheader("Content-Type", "application/json")
        self.send_header("Content-Type", ct)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        try:
            self.wfile.write(body)
        except CLIENT_ABORT_ERRORS:
            self._client_aborted = True
        return detail


# --------------------------------------------------------------------------
# entry point
# --------------------------------------------------------------------------

def main():
    argv = [a for a in sys.argv[1:]]
    if argv and argv[0] in ("keys", "keymanager"):
        import keymanager
        keymanager.run()
        return
    if argv and argv[0] == "catalog":
        check_catalog_once()
        cs = dict(CATALOG_STATUS)
        print("models: %s | last_check: %s" % (
            cs.get("models"), time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(cs["last_check"])) if cs["last_check"] else "never"))
        print("added:   %s" % (cs.get("added") or "(none)"))
        print("removed: %s" % (cs.get("removed") or "(none)"))
        if cs.get("last_error"):
            print("last_error:", cs["last_error"])
        return
    host = CFG["host"]
    port = CFG["port"]
    if "--port" in argv:
        port = int(argv[argv.index("--port") + 1])
    if "--host" in argv:
        host = argv[argv.index("--host") + 1]
    try:
        srv = ThreadingHTTPServer((host, port), Handler)
    except OSError:
        msg = "port %d busy - another nim-rotator-proxy instance already running; exiting." % port
        log("ALREADY-RUNNING | " + msg)
        print(msg)
        return
    srv.daemon_threads = True
    if CFG.get("catalog_enabled"):
        threading.Thread(target=catalog_loop, daemon=True).start()
    pool_n = len(load_pool_keys())
    log("START | %s:%d | version=%s | pool_keys=%d" % (host, port, __version__, pool_n))
    print("nim-rotator-proxy v%s listening on http://%s:%d" % (__version__, host, port))
    print("  pool keys: %d | guard limits: %s" % (pool_n, os.path.basename(LIMITS_FILE)))
    if CFG.get("catalog_enabled"):
        print("  catalog watcher: on (every %s) — data/catalog.json" % (
            "%dh" % (float(CFG["catalog_refresh_s"]) / 3600) if float(CFG["catalog_refresh_s"]) % 3600 == 0
            else "%ds" % CFG["catalog_refresh_s"]))
    if host not in ("127.0.0.1", "localhost", "::1"):
        print("  WARNING: serving non-local clients. Make sure data/keys.json has a pool_token")
        print("  or rely on BYOK (callers send their own nvapi-... key as the Bearer token).")
    try:
        srv.serve_forever()
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
