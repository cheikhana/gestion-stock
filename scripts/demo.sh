#!/usr/bin/env sh

set -e
cd "$(dirname "$0")/.."
docker compose up -d --build
PY=python3; command -v python3 >/dev/null 2>&1 || PY=python
$PY -m pip install -q -r scripts/requirements.txt
$PY scripts/demo.py "$@"
