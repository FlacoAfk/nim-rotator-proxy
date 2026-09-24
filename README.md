# nim-rotator-proxy

A self-hosted, **OpenAI-compatible proxy for NVIDIA NIM** (`integrate.api.nvidia.com`)
with API-key rotation, a context-window guard, and transparent big-context stream
conversion. Pure Python standard library — no dependencies, one file, cross-platform.

```
any OpenAI client ──►  nim-rotator-proxy  ──►  NVIDIA NIM (integrate.api.nvidia.com)
                        │  key rotation + cooldowns
                        │  context guard (measured windows)
                        │  big-context stream→buffered conversion
                        └─ BYOK: callers can use their own nvapi- key
```

## Why

NVIDIA's NIM endpoint has a few sharp edges that break real-world clients:

| Problem | What the proxy does |
|---|---|
| Streaming requests with a large prefill are killed by the gateway after ~100s with in-stream errors | Requests with an estimated prefill ≥ 150K tokens are converted to **buffered upstream** and replayed to the client as SSE |
| Buffered responses can take minutes before the first byte — clients (ZCode, some SDKs) abort at ~60s of silence | SSE **headers + role chunk are sent immediately**, and keepalive comments flow while NIM computes |
| One API key = one rate-limit bucket | **Key pool with rotation**, per-key cooldowns (429 → 5 min, model+key → 1 h, 401/403 → 24 h) and a circuit breaker |
| Models silently disappear upstream (EOL) or return 404 | Terminal **410/404 detection** with model cooldowns and an optional fallback-model chain |
| Context windows vary per model and change over time | **Pre-flight context guard**: requests over 90% of the model's *measured* window are rejected with a clean, instant 400 (no key burned) |
| Sharing a proxy with other people burns *your* quota | **BYOK mode**: every caller sends their own `nvapi-...` key; the proxy uses it for their requests only |

## Features

- **Rotation & resilience** — least-recently-used key selection, per-key and per-(model,key) cooldowns, 24h revocation cooldown, 410 EOL handling, 404-death detection, transient-status retry across keys, optional model fallback chain.
- **Context guard** — per-model measured windows in `context-limits.json`; rejects oversized requests *before* burning any key with a clear message.
- **Big-context conversion** — buffered upstream + faithful SSE replay (role → reasoning → content slices → `tool_calls` in NIM-native continuation style → finish chunk with `usage` → `[DONE]`), with the strict-SSE format that Vercel AI SDK (ZCode), opencode and kilo parse cleanly.
- **BYOK** — callers may authenticate with their own NIM key; the proxy tracks its health separately from the pool.
- **Interactive key manager** — a small ANSI dashboard to add/validate/test/remove keys, set the pool token, and review **live telemetry: token usage (total and per model), per-key request/token counts, key health (ok / cooling / revoked), guard rejections, stream conversions, fallbacks, client aborts and proxy uptime** (`python proxy.py keys`).
- **Usage telemetry built in** — the proxy extracts `usage` from every response path (buffered, converted and native streams — it injects `stream_options.include_usage` when the client didn't ask for it) and accumulates totals/per-model/per-key counters in `data/state.json`, flushed at least every 30 s.
- **Automatic catalog watcher** — periodically polls NIM's `/v1/models` (one metadata GET, no inference), detects models NVIDIA **adds or removes**, logs the diff, persists a snapshot + diff file, and proactively cools removed models so requests fail fast. `/health` and the key dashboard show the current status. Configure with `catalog_refresh_s` (default 6 h) — near-zero resource usage.
- **Zero dependencies** — Python 3.9+ standard library only. Runs on Windows, Linux and macOS.
- **Secret hygiene** — keys live in `data/keys.json` (gitignored), are never logged (only short hash prefixes), and state files are written atomically.

## Requirements

- Python 3.9+ (no packages to install)
- One or more NVIDIA NIM API keys (free at [build.nvidia.com](https://build.nvidia.com))

## Quickstart

```bash
# 1. start the proxy (default 127.0.0.1:8377)
python proxy.py

# 2. open the key manager and add your NIM key(s)
python proxy.py keys        # choose 1) add key  — input is hidden and validated

# 3. point any OpenAI-compatible client at it
curl http://127.0.0.1:8377/v1/chat/completions \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer rotator-local" \
  -d '{"model":"moonshotai/kimi-k3","messages":[{"role":"user","content":"Hi"}]}'
```

The health endpoint tells you the proxy is alive:

```
curl http://127.0.0.1:8377/health
→ {"ok": true, "service": "nim-rotator-proxy", "version": "1.0.0", "pool_keys": 2, ...}
```

## Serving other PCs (BYOK + pool token)

By default the proxy listens on `127.0.0.1` and trusts localhost. To share it with
other machines, set `"host": "0.0.0.0"` in `data/proxy.json` (or `--host 0.0.0.0`).
Non-local callers must then authenticate one of two ways:

1. **BYOK (recommended for shared setups)** — each user puts *their own* NIM key
   as the Bearer token in their client. The proxy uses that key for that user's
   requests, tracks its cooldowns separately, and never shares it with other
   callers. Your pool is untouched.
2. **Pool token** — set a `pool_token` in `data/keys.json`; callers who send it as
   the Bearer token rotate through the *server's* key pool (use this for your own
   machines). If no `pool_token` is set, the pool is localhost-only.

Optional: `"allow_pool_fallback": true` lets BYOK requests fall back to the pool
when the user's own key is rate-limited (off by default — nobody burns someone
else's quota by accident).

Client-side config is just the standard OpenAI shape:

```jsonc
{
  "baseURL": "http://your-server:8377/v1",
  "apiKey": "nvapi-...your own nim key..."   // BYOK
  // or "apiKey": "the pool_token"           // shared pool
}
```

## Key manager

```bash
python proxy.py keys          # interactive dashboard (same as: python keymanager.py)
```

```
╔══════════════════════════════════════════════════╗
║   nim-rotator-proxy — API key manager            ║
╚══════════════════════════════════════════════════╝
 id         key              label       status      req    ok    429    tokens
 a1b2c3d4e5 nvapi-AbCd...XyZ laptop      ok          412    409   3      1,204,551
 f6e5d4c3b2 nvapi-...        desktop     cooling 1m  ...
 pool_token : rot12...
 pool fallback for BYOK 429s: OFF

 usage since proxy start — uptime 5h12m
--------------------------------------------------------------------------------------------------------------
 total: 1,208 completions | 3,101,442 prompt + 88,204 completion = 3,189,646 tokens

 model                                          req       prompt   completion    total tok
 moonshotai/kimi-k3                             640       ...      ...           ...
 z-ai/glm-5.3-flash                             310       ...      ...           ...
--------------------------------------------------------------------------------------------------------------

 1) add key   2) remove key   3) enable/disable  4) refresh
 5) validate  6) test key     7) set pool_token  8) toggle pool fallback
 9) catalog status            0) quit
```

Non-interactive equivalents (for scripts):

```bash
python keymanager.py add my-laptop     # hidden input; key is validated on save
python keymanager.py list
python keymanager.py validate a1b2     # GET /v1/models with that key
python keymanager.py test a1b2         # tiny chat request with that key
python keymanager.py remove a1b2
python keymanager.py token my-secret   # set pool_token ('-' clears it)
python keymanager.py fallback on|off   # allow BYOK → pool fallback
```

## Configuration

`data/proxy.json` is created on first run; every field is optional. Environment
variables override everything (`NIM_PROXY_*`):

| Field | Env | Default | Meaning |
|---|---|---|---|
| `port` | `NIM_PROXY_PORT` | `8377` | Listen port |
| `host` | `NIM_PROXY_HOST` | `127.0.0.1` | Bind address (`0.0.0.0` = serve LAN/WAN) |
| `pool_token` | — | *(empty)* | Bearer token that grants pool access (in `keys.json`) |
| `allow_pool_fallback` | — | `false` | BYOK requests may fall back to the pool on 429 |
| `max_stream_keys` | `NIM_PROXY_MAX_STREAM_KEYS` | `2` | Cap of streaming key attempts (protects client TTFB) |
| `buffered_min_tokens` | `NIM_PROXY_BUFFERED_MIN_TOKENS` | `150000` | Estimated prefill at which stream→buffered conversion kicks in |
| `keepalive_s` | `NIM_PROXY_STREAM_KEEPALIVE_S` | `15` | Keepalive comment interval |
| `ttfb_timeout_s` | `NIM_PROXY_TTFB_TIMEOUT_S` | `300` | Give-up time waiting for the first upstream byte |
| `upstream_timeout_s` | `NIM_PROXY_UPSTREAM_TIMEOUT_S` | `600` | Upstream socket timeout |
| `default_context` | — | `131072` | Guard ceiling for models missing from the limits file |
| `guard_ratio` | — | `0.9` | Fraction of the window the guard allows |
| `fallback_models` | — | `[]` | Ordered model ids tried when the primary fails upstream |
| `catalog_enabled` | — | `true` | Watch NIM's catalog for added/removed models |
| `catalog_refresh_s` | `NIM_PROXY_CATALOG_REFRESH_S` | `21600` | Seconds between catalog checks (6 h; one metadata GET each) |
| `catalog_poll_key_id` | — | *(empty)* | Specific key id used to poll the catalog (default: first pool key, else the last BYOK key seen) |

Files (all auto-created):

| Path | Purpose |
|---|---|
| `data/keys.json` | **Secrets.** Pool keys, `pool_token`, per-key labels/flags |
| `data/state.json` | Cooldowns, per-key stats, dead-model cooldowns, **usage telemetry** (tokens total/per-model/per-key), counters, proxy start time (atomic writes, flushed every 30 s) |
| `data/proxy.json` | Server config |
| `data/proxy.log` | Log (auto-rotates at 1 MB; keys are never written, only hash ids) |
| `data/catalog.json` | Latest upstream model snapshot (written by the catalog watcher) |
| `data/catalog-diff.json` | Last detected change (added/removed model ids) |
| `context-limits.json` | Measured per-model context windows used by the guard |

### Catalog watcher

NVIDIA adds and removes models without notice. The watcher keeps the proxy aware:

```bash
python proxy.py catalog        # force a check now and print the status/diff
```

- **Model added** → logged as `CATALOG | + model-id`, recorded in `data/catalog-diff.json`, and visible in `/health` and the key dashboard. New models work immediately through the proxy (the guard uses `default_context` until you measure their real window and add it to `context-limits.json`).
- **Model removed** → logged as `CATALOG | - model-id` and proactively cooled for 1 h so clients fail fast (with fallback, if configured) instead of burning a request on a dead model.
- Runs in a daemon thread that sleeps between checks — one metadata GET every `catalog_refresh_s`, essentially zero CPU/memory overhead. Failures are logged and retried next interval; the proxy never crashes or blocks on it.

## Using with clients

Any OpenAI-compatible client works — the proxy speaks the standard
`/v1/chat/completions` (streaming and buffered) and forwards `/v1/models`.

**curl / OpenAI SDK**

```python
from openai import OpenAI
client = OpenAI(base_url="http://127.0.0.1:8377/v1", api_key="rotator-local")
client.chat.completions.create(model="moonshotai/kimi-k3",
                               messages=[{"role": "user", "content": "Hi"}])
```

**opencode / kilo** — set the nvidia provider's `baseURL` to
`http://127.0.0.1:8377/v1` with any stub API key (localhost) or a real
`nvapi-` key / pool token (remote).

**ZCode** — same `baseURL` in the custom provider definition.

## Measured context windows (2026-09)

Real values measured through NIM itself (overshoot-error parsing and needle
tests), used by the guard via `context-limits.json`:

| Model | Window |
|---|---|
| `z-ai/glm-5.3-flash` | 1,048,576 |
| `z-ai/glm-5.3` | 1,047,576 |
| `nvidia/nemotron-3.5-lightning-30b-a3b` | 750,000 |
| `nvidia/nemotron-3-super-120b-a12b` | 750,000 |
| `deepseek-ai/deepseek-v4.1-flash` | 210,000 |
| `meta/muse-glimmer-30b` | 200,000 |
| `poolside/laguna-xs-2.1` | 190,000 |
| `moonshotai/kimi-k3` | 300,000 |
| `nvidia/nemotron-3-nano-omni-30b-a3b-reasoning` | 340,000 |
| `openai/gpt-oss-20b` | 97,000 |
| `nvidia/nemotron-3-ultra-550b-a55b` | 131,072 |
| `google/gemma-4-31b-it` | 131,072 |
| `meta/llama-3.2-11b-vision-instruct` | 131,072 |
| `nvidia/ising-calibration-1.5-31b` | 131,072 |
| *(9 more specialist models)* | 131,072 default |

> NIM capacity varies by day and model. If a model starts rejecting sizes it
> used to accept, re-measure and update `context-limits.json` (the proxy
> re-reads it on every request — no restart needed).

## How the context guard estimates tokens

`estimate_tokens` uses `chars / 2.8` calibrated against NIM's own
`prompt_tokens` counter: it over-counts prose (the safe direction) and never
under-counts code. The guard rejects before any key is contacted, so oversized
requests cost nothing.

## How big-context conversion works

1. A streaming request's estimated prefill ≥ `buffered_min_tokens` triggers conversion.
2. The proxy sends the **SSE headers + role chunk immediately** (clients never sit on silent headers).
3. The upstream request is made with `stream: false` (NIM allows buffered requests to run for minutes; it kills long-prefill *streams* after ~100s).
4. While NIM computes, the proxy emits `: keepalive` comments every 15 s — even across key retries.
5. The buffered JSON is replayed as standard OpenAI deltas, including `tool_calls` continuations in NIM's native style (explicit `null` id/type/name on argument chunks — required by strict AI-SDK parsers) and the final chunk carries `usage` and the real `finish_reason`.

## Troubleshooting

| Symptom | Cause / fix |
|---|---|
| `400 context guard: ~N estimated tokens exceed...` | The conversation is bigger than 90% of that model's measured window — reduce it, compact, or use a larger-window model |
| `429 all available keys are cooling down` | Every key hit NIM rate limits; wait or add more keys (`python proxy.py keys`) |
| `502 ... failed after trying N key(s)` | Upstream failure on all keys — check `data/proxy.log` (`RETRY`/`UPSTREAM_DETAIL` lines) |
| `410` in logs / model marked dead | That model reached EOL upstream; the proxy cools it for 24 h and falls back if `fallback_models` is configured |
| Client aborts around 60 s on huge requests | You're running an old client build against a pre-conversion proxy — this repo sends immediate SSE prelude + keepalives, which fixes exactly that |
| `JSON parsing failed: Unexpected end of JSON input` on clients | Older versions emitted a malformed `data: \n{...}` prefix; this repo emits strictly valid `data: {...}` events |
| Non-local clients get `401` | Send a `nvapi-` key (BYOK) or the configured `pool_token` as the Bearer token |

## FAQ

**Does the proxy store or forward my keys anywhere else?**
No. Keys stay in `data/keys.json` on the machine running the proxy. Logs contain
only 12-char SHA-256 prefixes. BYOK keys are used for the caller's request and
held only in memory for the duration.

**Can I run it behind HTTPS / a reverse tunnel?**
Yes — put nginx/caddy/cloudflared in front and point clients at it; the proxy
speaks plain HTTP on its port.

**What happens if a key dies?** 401/403 cooldowns it for 24 h and it shows as
`cooling` in the dashboard; remove it with `python keymanager.py remove <id>`.

## License

MIT — see [LICENSE](LICENSE).
