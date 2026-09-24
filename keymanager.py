#!/usr/bin/env python3
"""
keymanager — interactive CLI dashboard for nim-rotator-proxy API keys.

Manages data/keys.json (the proxy's key pool). Never prints full keys.

Usage:
  python keymanager.py                 # interactive dashboard
  python keymanager.py add [label]     # add a key (hidden input)
  python keymanager.py list            # one-shot table
  python keymanager.py remove <id>     # remove by id prefix
  python keymanager.py validate <id>   # check a key against NIM /v1/models
  python keymanager.py test <id>       # tiny chat request with that key
  python keymanager.py token <value>   # set the pool_token
"""

import json
import os
import sys
import time
import getpass
import urllib.request
import urllib.error

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.environ.get("NIM_PROXY_DATA_DIR", os.path.join(BASE_DIR, "data"))
KEYS_FILE = os.path.join(DATA_DIR, "keys.json")
UPSTREAM_HOST = "https://integrate.api.nvidia.com"

os.system("")  # enable ANSI escapes on Windows 10+ terminals

RESET, DIM, BOLD = "\033[0m", "\033[2m", "\033[1m"
GREEN, YELLOW, RED, CYAN, MAGENTA = "\033[32m", "\033[33m", "\033[31m", "\033[36m", "\033[35m"


def c(text, color):
    return "%s%s%s" % (color, text, RESET)


def load():
    try:
        with open(KEYS_FILE, encoding="utf-8-sig") as f:
            doc = json.load(f)
        if not isinstance(doc, dict):
            raise ValueError
    except (OSError, ValueError):
        doc = {"pool_token": "", "allow_pool_fallback": False, "keys": []}
    doc.setdefault("pool_token", "")
    doc.setdefault("allow_pool_fallback", False)
    doc.setdefault("keys", [])
    return doc


def save(doc):
    os.makedirs(DATA_DIR, exist_ok=True)
    tmp = KEYS_FILE + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(doc, f, indent=2)
    os.replace(tmp, KEYS_FILE)


def mask(key):
    return key[:8] + "..." + key[-4:] if len(key) > 14 else "***"


def key_id(key):
    import hashlib
    return hashlib.sha256(key.encode()).hexdigest()[:12]


def load_state():
    try:
        with open(os.path.join(DATA_DIR, "state.json"), encoding="utf-8-sig") as f:
            return json.load(f)
    except Exception:
        return {}


def http_json(url, key, method="GET", body=None, timeout=30):
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(url, data=data, method=method, headers={
        "Content-Type": "application/json",
        "Authorization": "Bearer " + key,
    })
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return r.status, json.loads(r.read().decode("utf-8", errors="replace"))
    except urllib.error.HTTPError as e:
        try:
            return e.code, json.loads(e.read().decode("utf-8", errors="replace"))
        except Exception:
            return e.code, {}
    except Exception as e:
        return None, {"error": {"message": str(e)}}


# ----------------------------------------------------------------- actions

def cmd_add(label=None):
    doc = load()
    print(c("Add a NVIDIA NIM API key", BOLD))
    print(dim_text("Get one free at https://build.nvidia.com (nvapi-...). Input is hidden."))
    key = getpass.getpass("API key: ").strip()
    if not key.startswith("nvapi-"):
        print(c("  ✗ key must start with nvapi-", RED))
        return
    status, body = http_json(UPSTREAM_HOST + "/v1/models", key)
    if status == 200:
        n = len((body or {}).get("data") or [])
        print(c("  ✓ key validated (%d models available)" % n, GREEN))
    else:
        print(c("  ✗ validation failed (HTTP %s) — added as DISABLED" % status, YELLOW))
        print(dim_text("  " + json.dumps((body or {}).get("error", {}))[:160]))
    entry = {
        "id": key_id(key),
        "key": key,
        "label": label or ("key-%d" % (len(doc["keys"]) + 1)),
        "added": time.strftime("%Y-%m-%d %H:%M"),
        "enabled": status == 200,
    }
    doc["keys"].append(entry)
    save(doc)
    print(c("  saved id=%s label=%s" % (entry["id"], entry["label"]), CYAN))


def dim_text(s):
    return c(s, DIM)


def status_of(entry, state):
    if not entry.get("enabled", True):
        return c("disabled", YELLOW)
    kid = entry["id"]
    cool = (state.get("key_cool") or {}).get(kid, 0)
    now = time.time()
    if cool > now:
        return c("cooling %dm" % int((cool - now) / 60), YELLOW)
    if cool > now - 86400 and cool != 0 and (cool - now) < -86000:
        return c("revoked?", RED)
    return c("ok", GREEN)


def table(doc, state):
    keys = doc.get("keys") or []
    print()
    print(c(" nim-rotator-proxy keys — %s" % KEYS_FILE, BOLD))
    print(dim_text(" " + "-" * 78))
    print(" %-10s %-18s %-11s %-8s %-6s %-6s %-6s" % ("id", "key", "label", "status", "req", "ok", "429"))
    if not keys:
        print(dim_text(" (empty — add your first NVIDIA NIM key with 'add')"))
    stats = state.get("stats") or {}
    for e in keys:
        s = (stats.get(e["id"]) or {})
        print(" %-10s %-18s %-11s %-8s %-6s %-6s %-6s" % (
            e["id"], mask(e["key"]), (e.get("label") or "")[:11],
            status_of(e, state), s.get("req", 0), s.get("ok", 0), s.get("429", 0)))
    print(dim_text(" " + "-" * 78))
    pt = doc.get("pool_token") or "(none — pool only open to localhost)"
    print(" pool_token : %s" % (c(pt[:6] + "..." if doc.get("pool_token") else pt, CYAN)))
    print(" pool fallback for BYOK 429s: %s" % (c("ON", GREEN) if doc.get("allow_pool_fallback") else c("OFF", DIM)))
    print()


def cmd_list():
    table(load(), load_state())


def find_entry(doc, prefix):
    matches = [e for e in doc["keys"] if e["id"].startswith(prefix)]
    if len(matches) == 1:
        return matches[0]
    if not matches:
        print(c("no key with id prefix %r" % prefix, RED))
    else:
        print(c("ambiguous prefix %r" % prefix, RED))
    return None


def cmd_remove(prefix):
    doc = load()
    e = find_entry(doc, prefix)
    if not e:
        return
    doc["keys"] = [x for x in doc["keys"] if x["id"] != e["id"]]
    save(doc)
    print(c("removed %s (%s)" % (e["id"], e.get("label")), GREEN))


def cmd_toggle(prefix, enable):
    doc = load()
    e = find_entry(doc, prefix)
    if not e:
        return
    e["enabled"] = enable
    save(doc)
    print(c("%s %s" % ("enabled" if enable else "disabled", e["id"]), GREEN))


def cmd_validate(prefix):
    doc = load()
    e = find_entry(doc, prefix)
    if not e:
        return
    status, body = http_json(UPSTREAM_HOST + "/v1/models", e["key"])
    if status == 200:
        print(c("✓ %s (%s) valid — %d models" % (e["id"], e.get("label"),
              len((body or {}).get("data") or [])), GREEN))
    else:
        err = (body or {}).get("error", {})
        print(c("✗ %s HTTP %s" % (e["id"], status), RED), dim_text(str(err)[:200]))


def cmd_test(prefix):
    doc = load()
    e = find_entry(doc, prefix)
    if not e:
        return
    status, body = http_json(UPSTREAM_HOST + "/v1/chat/completions", e["key"], method="POST",
                             body={"model": "meta/muse-glimmer-30b",
                                   "messages": [{"role": "user", "content": "Say OK"}],
                                   "max_tokens": 8}, timeout=60)
    if status == 200:
        served = (body or {}).get("model")
        content = ((body or {}).get("choices") or [{}])[0].get("message", {}).get("content", "")
        print(c("✓ 200 via %s — content=%r" % (served, content[:40]), GREEN))
    else:
        print(c("✗ HTTP %s" % status, RED), dim_text(str((body or {}).get("error", {}))[:200]))


def cmd_token(value):
    doc = load()
    if value in ("-", "clear", "none"):
        doc["pool_token"] = ""
        print(c("pool_token cleared (pool open to localhost only)", YELLOW))
    else:
        doc["pool_token"] = value
        print(c("pool_token set — remote clients must send it as Bearer to use the pool", GREEN))
    save(doc)


def cmd_catalog():
    """Show the catalog watcher status: snapshot age + last diff."""
    cat_path = os.path.join(DATA_DIR, "catalog.json")
    dif_path = os.path.join(DATA_DIR, "catalog-diff.json")
    try:
        with open(cat_path, encoding="utf-8-sig") as f:
            cat = json.load(f)
        age = time.time() - cat.get("checked_at", 0)
        print(c("catalog snapshot: %d models (checked %s, %.1fh ago)" % (
            len(cat.get("models") or []),
            time.strftime("%Y-%m-%d %H:%M", time.localtime(cat.get("checked_at", 0))),
            age / 3600), CYAN))
    except Exception:
        print(c("no catalog snapshot yet — the proxy writes one on its first check", YELLOW))
    try:
        with open(dif_path, encoding="utf-8-sig") as f:
            dif = json.load(f)
        print(c("last diff (%s):" % time.strftime("%Y-%m-%d %H:%M", time.localtime(dif.get("checked_at", 0))), BOLD))
        for m in dif.get("added") or []:
            print("  " + c("+ %s" % m, GREEN))
        for m in dif.get("removed") or []:
            print("  " + c("- %s" % m, RED))
        if not (dif.get("added") or dif.get("removed")):
            print(dim_text("  (no changes)"))
    except Exception:
        print(dim_text("no diff recorded yet"))
    print(dim_text("force a check now:  python proxy.py catalog"))


def cmd_fallback(onoff):
    doc = load()
    doc["allow_pool_fallback"] = onoff in ("on", "true", "1", "yes")
    save(doc)
    print(c("allow_pool_fallback = %s" % doc["allow_pool_fallback"], GREEN))


# ------------------------------------------------------------- dashboard

MENU = """
 {b}1){r} add key            {b}5){r} validate key
 {b}2){r} remove key         {b}6){r} test key (chat)
 {b}3){r} enable/disable     {b}7){r} set pool_token
 {b}4){r} refresh            {b}8){r} toggle pool fallback
 {b}9){r} catalog status     {b}0){r} quit
"""


def dashboard():
    print(c("╔══════════════════════════════════════════════════╗", MAGENTA))
    print(c("║   nim-rotator-proxy — API key manager            ║", MAGENTA))
    print(c("╚══════════════════════════════════════════════════╝", MAGENTA))
    while True:
        doc, state = load(), load_state()
        table(doc, state)
        choice = input(c("choice> ", CYAN)).strip()
        if choice == "1":
            lbl = input("label (optional): ").strip() or None
            cmd_add(lbl)
        elif choice == "2":
            cmd_remove(input("id prefix: ").strip())
        elif choice == "3":
            p = input("id prefix: ").strip()
            cmd_toggle(p, input("enable? (y/n): ").strip().lower() == "y")
        elif choice == "4":
            continue
        elif choice == "5":
            cmd_validate(input("id prefix: ").strip())
        elif choice == "6":
            cmd_test(input("id prefix: ").strip())
        elif choice == "7":
            cmd_token(input("pool_token value ('-' to clear): ").strip())
        elif choice == "8":
            cmd_fallback(input("allow_pool_fallback on/off: ").strip())
        elif choice == "9":
            cmd_catalog()
        elif choice in ("0", "q", ""):
            print(dim_text("bye"))
            return


def run():
    args = sys.argv[1:]
    if not args:
        dashboard()
        return
    cmd, rest = args[0], args[1:]
    if cmd == "add":
        cmd_add(rest[0] if rest else None)
    elif cmd == "list":
        cmd_list()
    elif cmd == "remove":
        cmd_remove(rest[0])
    elif cmd == "validate":
        cmd_validate(rest[0])
    elif cmd == "test":
        cmd_test(rest[0])
    elif cmd == "token":
        cmd_token(rest[0] if rest else "")
    elif cmd == "fallback":
        cmd_fallback(rest[0] if rest else "on")
    elif cmd == "catalog":
        cmd_catalog()
    else:
        print(__doc__)


if __name__ == "__main__":
    run()
