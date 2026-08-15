#!/usr/bin/env bash
# Merge legacy range_data digest dirs into the canonical filename-based digest.
#
# Usage:
#   ./scripts/consolidate-range-data.sh
#   ./scripts/consolidate-range-data.sh data/control-server/cache/range_data
#   ./scripts/consolidate-range-data.sh logs/workers/predefined_etag_range_data
#   LOG_DIR=/path/to/logs ./scripts/consolidate-range-data.sh
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
export PYTHONPATH="${ROOT}:${PYTHONPATH:-}"

TARGET="${1:-}"
exec python3 - "$TARGET" <<'PY'
import os
import sys
from pathlib import Path

from neurons.common.byte_range_store import ByteRangeStore

arg = (sys.argv[1] or "").strip()
candidates: list[Path] = []
if arg:
    candidates.append(Path(arg))
else:
    log_root = Path(os.environ.get("LOG_DIR", "logs"))
    candidates.extend(
        [
            Path("data/control-server/cache/range_data"),
            log_root / "workers" / "predefined_etag_range_data",
        ]
    )
    # Also scan common worker log dirs if present.
    workers = log_root / "workers"
    if workers.is_dir():
        for child in workers.iterdir():
            if child.is_dir():
                rd = child / "predefined_etag_range_data"
                if rd.is_dir():
                    candidates.append(rd)

seen: set[Path] = set()
for root in candidates:
    root = root.resolve()
    if root in seen or not root.is_dir():
        continue
    seen.add(root)
    store = ByteRangeStore(root)
    before = sorted(p.name for p in root.iterdir() if p.is_dir() and not p.name.startswith("."))
    result = store.consolidate_signed_url_orphans()
    after = sorted(p.name for p in root.iterdir() if p.is_dir() and not p.name.startswith("."))
    print(f"root={root}")
    print(f"  before={before}")
    print(f"  result={result}")
    print(f"  after={after}")
    print(f"  sources={store.list_sources()}")
PY
