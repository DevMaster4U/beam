#!/usr/bin/env bash
# Delete unused predefined-etag chunk .bin files.
#
# "Unused" = .bin digest is not sha256(cache_key) for any key in
# predefined_etag_chunks.json.
#
# Default paths (control-server on-disk cache):
#   data/control-server/cache/predefined_etag_chunks.json
#   data/control-server/cache/chunk_data/
#
# Usage:
#   ./scripts/delete-unused-chunk-data.sh              # dry-run
#   ./scripts/delete-unused-chunk-data.sh --apply      # delete orphans
#   ./scripts/delete-unused-chunk-data.sh --worker-local --apply
#   ./scripts/delete-unused-chunk-data.sh \
#       --json /path/to/predefined_etag_chunks.json \
#       --chunk-dir /path/to/chunk_data \
#       --apply
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

APPLY=0
WORKER_LOCAL=0
JSON_PATH=""
CHUNK_DIR=""

usage() {
  cat <<EOF
Usage: $0 [--apply] [--worker-local] [--json PATH] [--chunk-dir PATH]

Delete .bin files under chunk-dir whose digests are not referenced by
predefined_etag_chunks.json keys (sha256 of each cache key).

  --apply         Actually delete (default is dry-run)
  --worker-local  Use logs/workers/predefined_etag_chunks.json and
                  logs/workers/predefined_etag_chunk_data/
  --json PATH     Metadata JSON path
  --chunk-dir PATH  Directory of <sha256>.bin files

Defaults (control-server cache):
  JSON:      ${ROOT}/data/control-server/cache/predefined_etag_chunks.json
  chunk-dir: ${ROOT}/data/control-server/cache/chunk_data

Examples:
  $0                          # dry-run control-server orphans
  $0 --apply                  # delete control-server orphans
  $0 --worker-local --apply   # delete orphan local worker downloads
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --apply)
      APPLY=1
      shift
      ;;
    --worker-local)
      WORKER_LOCAL=1
      shift
      ;;
    --json)
      JSON_PATH="${2:-}"
      shift 2
      ;;
    --chunk-dir)
      CHUNK_DIR="${2:-}"
      shift 2
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "Unknown option: $1" >&2
      usage >&2
      exit 1
      ;;
  esac
done

if [[ "$WORKER_LOCAL" -eq 1 ]]; then
  LOG_DIR="${LOG_DIR:-${ROOT}/logs}"
  JSON_PATH="${JSON_PATH:-${LOG_DIR}/workers/predefined_etag_chunks.json}"
  CHUNK_DIR="${CHUNK_DIR:-${LOG_DIR}/workers/predefined_etag_chunk_data}"
else
  JSON_PATH="${JSON_PATH:-${ROOT}/data/control-server/cache/predefined_etag_chunks.json}"
  CHUNK_DIR="${CHUNK_DIR:-${ROOT}/data/control-server/cache/chunk_data}"
fi

if [[ ! -f "$JSON_PATH" ]]; then
  echo "Missing cache JSON: ${JSON_PATH}" >&2
  exit 1
fi
if [[ ! -d "$CHUNK_DIR" ]]; then
  echo "Missing chunk dir: ${CHUNK_DIR}" >&2
  exit 1
fi

export DELETE_UNUSED_JSON="$JSON_PATH"
export DELETE_UNUSED_CHUNK_DIR="$CHUNK_DIR"
export DELETE_UNUSED_APPLY="$APPLY"

python3 - <<'PY'
from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path

json_path = Path(os.environ["DELETE_UNUSED_JSON"])
chunk_dir = Path(os.environ["DELETE_UNUSED_CHUNK_DIR"])
apply = os.environ.get("DELETE_UNUSED_APPLY", "0") == "1"

payload = json.loads(json_path.read_text())
entries = payload.get("entries") if isinstance(payload, dict) else None
if not isinstance(entries, dict):
    entries = payload if isinstance(payload, dict) else {}

keep = {hashlib.sha256(str(key).encode()).hexdigest() for key in entries}

bins = sorted(chunk_dir.glob("*.bin"))
orphans = [path for path in bins if path.stem not in keep]
kept = len(bins) - len(orphans)
orphan_bytes = sum(path.stat().st_size for path in orphans)

print(f"JSON:       {json_path}")
print(f"chunk-dir:  {chunk_dir}")
print(f"entries:    {len(entries)}")
print(f".bin total: {len(bins)}")
print(f"referenced: {kept}")
print(f"unused:     {len(orphans)} ({orphan_bytes / (1024 * 1024):.1f} MiB)")

if not orphans:
    print("Nothing to delete.")
    raise SystemExit(0)

if not apply:
    print("Dry-run (pass --apply to delete). Sample unused:")
    for path in orphans[:10]:
        print(f"  {path.name}")
    if len(orphans) > 10:
        print(f"  ... and {len(orphans) - 10} more")
    raise SystemExit(0)

deleted = 0
for path in orphans:
    path.unlink(missing_ok=True)
    deleted += 1
print(f"Deleted {deleted} unused .bin file(s).")
PY
