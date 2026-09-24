#!/usr/bin/env bash
# nim-rotator-proxy launcher (Linux/macOS)
cd "$(dirname "$0")"
exec python3 proxy.py "$@"
