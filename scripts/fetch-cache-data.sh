#!/usr/bin/env bash
# Fetch predefined ETag cache metadata + range bytes from control-server.
#
# Usage:
#   ./scripts/fetch-cache-data.sh
#   ./scripts/fetch-cache-data.sh config/workers/worker1.env
#   CONTROL_SERVER_URL=http://host:8010 CONTROL_SERVER_SECRET=secret ./scripts/fetch-cache-data.sh
#
# Writes:
#   logs/workers/predefined_etag_chunks.json
#   logs/workers/predefined_etag_range_data/<digest>/   (merge into continuous store)
#
# Optional:
#   --legacy-chunk-data  also write logs/workers/predefined_etag_chunk_data/<sha256>.bin
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BEAM_ROOT="$ROOT"
# shellcheck disable=SC1091
source "${ROOT}/scripts/lib/systemd.sh"

ENV_FILE="${1:-}"
shift || true

DOWNLOAD_ALL=0
SKIP_CHUNKS=0
LEGACY_CHUNK_DATA=0
EXTRA_ARGS=()

usage() {
  cat <<EOF
Usage: $0 [env-file] [--all-chunks] [--metadata-only] [--legacy-chunk-data]

Fetch predefined ETag cache from control-server into logs/workers/.

  env-file              Optional worker/orchestrator env (CONTROL_SERVER_* vars)
  --all-chunks          Download bytes even when has_chunk_data is unset
  --metadata-only       Fetch JSON metadata only (skip range downloads)
  --legacy-chunk-data   Also write legacy predefined_etag_chunk_data/*.bin

Environment (from env-file or shell):
  CONTROL_SERVER_WS_URL   e.g. ws://host:8010/ws/miners
  CONTROL_SERVER_URL      HTTP base (derived from WS if omitted)
  CONTROL_SERVER_SECRET   Shared secret
  LOG_DIR                 Default: ${ROOT}/logs
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --all-chunks)
      DOWNLOAD_ALL=1
      shift
      ;;
    --metadata-only)
      SKIP_CHUNKS=1
      shift
      ;;
    --legacy-chunk-data)
      LEGACY_CHUNK_DATA=1
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      EXTRA_ARGS+=("$1")
      shift
      ;;
  esac
done

beam_prepare_data_dirs

if [[ -n "$ENV_FILE" ]]; then
  if [[ ! -f "$ENV_FILE" ]]; then
    echo "Missing env file: ${ENV_FILE}" >&2
    exit 1
  fi
  set -a
  # shellcheck disable=SC1090
  source "$ENV_FILE"
  set +a
fi

export PYTHONPATH="${ROOT}:${PYTHONPATH:-}"
export LOG_DIR="${LOG_DIR:-${ROOT}/logs}"

PY="$(beam_python)"
exec "$PY" - "$DOWNLOAD_ALL" "$SKIP_CHUNKS" "$LEGACY_CHUNK_DATA" "${EXTRA_ARGS[@]}" <<'PY'
import hashlib
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import quote

import httpx

from neurons.common.byte_range_store import ByteRangeStore, parse_cache_key_range
from neurons.common.control_client import get_control_server_config

download_all = sys.argv[1] == "1"
skip_chunks = sys.argv[2] == "1"
legacy_chunk_data = sys.argv[3] == "1"

cfg = get_control_server_config()
if not cfg.secret or not (cfg.http_url or cfg.ws_url):
    raise SystemExit(
        "CONTROL_SERVER_SECRET and CONTROL_SERVER_WS_URL (or CONTROL_SERVER_URL) are required"
    )

log_root = Path(os.environ.get("LOG_DIR", "logs"))
workers_dir = log_root / "workers"
cache_path = workers_dir / "predefined_etag_chunks.json"
range_dir = workers_dir / "predefined_etag_range_data"
chunk_dir = workers_dir / "predefined_etag_chunk_data"
workers_dir.mkdir(parents=True, exist_ok=True)
range_dir.mkdir(parents=True, exist_ok=True)
if legacy_chunk_data:
    chunk_dir.mkdir(parents=True, exist_ok=True)

headers = {"X-Control-Server-Secret": cfg.secret}
if cfg.miner_id:
    headers["X-Miner-Id"] = cfg.miner_id

http_url = cfg.http_url.rstrip("/")
print(f"Fetching cache metadata from {http_url}/cache/predefined-etag")
with httpx.Client(timeout=60.0) as client:
    resp = client.get(f"{http_url}/cache/predefined-etag", headers=headers)
    resp.raise_for_status()
    payload = resp.json()

entries = payload.get("entries") or {}
normalized = {
    "entries": {
        str(key): {
            "chunk_hash": str(item.get("chunk_hash") or "").strip(),
            "etag": str(item.get("etag") or "").strip(),
            **(
                {"has_chunk_data": True}
                if bool(item.get("has_chunk_data"))
                else {}
            ),
        }
        for key, item in entries.items()
        if isinstance(item, dict) and str(item.get("chunk_hash") or "").strip()
    },
    "updated_at": payload.get("updated_at") or datetime.now(timezone.utc).isoformat(),
}
cache_path.write_text(json.dumps(normalized, indent=2) + "\n", encoding="utf-8")
print(f"Saved metadata: {cache_path} entries={len(normalized['entries'])}")

if skip_chunks:
    print("Skipping range downloads (--metadata-only)")
    raise SystemExit(0)

store = ByteRangeStore(range_dir)
timeout = float(os.environ.get("CONTROL_SERVER_CHUNK_DATA_TIMEOUT", "180"))

downloaded = 0
skipped = 0
missing = 0
errors = 0
hash_mismatch = 0

print(f"Downloading ranges into {range_dir}")
with httpx.Client(timeout=timeout) as client:
    for key, item in normalized["entries"].items():
        has_data = bool(entries.get(key, {}).get("has_chunk_data"))
        if not has_data and not download_all:
            missing += 1
            continue

        chunk_hash = str(item.get("chunk_hash") or "").strip()
        parsed = parse_cache_key_range(key)
        if parsed is not None:
            source, start, end = parsed
            if store.covers(source, start, end):
                skipped += 1
                continue
        else:
            source = start = end = None

        url = f"{http_url}/cache/predefined-etag/entries/{quote(key, safe='')}/data"
        try:
            resp = client.get(url, headers=headers)
            if resp.status_code == 404:
                missing += 1
                continue
            resp.raise_for_status()
            data = resp.content
            if not data:
                missing += 1
                continue

            if chunk_hash:
                computed = hashlib.sha256(data).hexdigest()
                if computed.lower() != chunk_hash.lower():
                    hash_mismatch += 1
                    print(
                        f"  hash mismatch key={key[:72]} "
                        f"expected={chunk_hash[:16]} got={computed[:16]}"
                    )
                    continue

            if parsed is not None:
                expected = end - start + 1
                if len(data) != expected:
                    errors += 1
                    print(
                        f"  size mismatch key={key[:72]} "
                        f"got={len(data)} expected={expected}"
                    )
                    continue
                store.ingest(source, start, end, data, merge=True)
            else:
                # Non-range key: legacy file only
                if not legacy_chunk_data:
                    errors += 1
                    print(f"  skip non-range key (use --legacy-chunk-data): {key[:72]}")
                    continue

            if legacy_chunk_data:
                digest = hashlib.sha256(key.encode()).hexdigest()
                out_path = chunk_dir / f"{digest}.bin"
                if not out_path.is_file() or out_path.stat().st_size == 0:
                    out_path.write_bytes(data)

            downloaded += 1
            print(f"  range key={key[:96]} bytes={len(data)}")
        except Exception as exc:
            errors += 1
            print(f"  range fetch failed key={key[:72]} err={exc}")

print(
    f"Ranges: downloaded={downloaded} skipped_covered={skipped} "
    f"missing_on_server={missing} hash_mismatch={hash_mismatch} "
    f"errors={errors} dir={range_dir}"
)
PY
