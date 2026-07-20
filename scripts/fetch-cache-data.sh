#!/usr/bin/env bash
# Fetch range_data coverage (segments.json) + bytes from control-server.
#
# Usage:
#   ./scripts/fetch-cache-data.sh
#   ./scripts/fetch-cache-data.sh config/workers/worker1.env
#   CONTROL_SERVER_URL=http://host:8010 CONTROL_SERVER_SECRET=secret ./scripts/fetch-cache-data.sh
#
# Writes:
#   logs/workers/predefined_etag_range_data/<digest>/segments.json + *.bin
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

SKIP_CHUNKS=0
LEGACY_CHUNK_DATA=0

usage() {
  cat <<EOF
Usage: $0 [env-file] [--metadata-only] [--legacy-chunk-data]

Fetch range coverage from control-server into logs/workers/predefined_etag_range_data/.

  env-file              Optional worker/orchestrator env (CONTROL_SERVER_* vars)
  --metadata-only       Print segment coverage only (skip range downloads)
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
      # Kept for CLI compat; ranges snapshot always lists real coverage.
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
      echo "Unknown argument: $1" >&2
      usage >&2
      exit 1
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
exec "$PY" - "$SKIP_CHUNKS" "$LEGACY_CHUNK_DATA" <<'PY'
import hashlib
import os
import sys
from pathlib import Path

import httpx

from neurons.common.byte_range_store import ByteRangeStore
from neurons.common.control_client import get_control_server_config

skip_chunks = sys.argv[1] == "1"
legacy_chunk_data = sys.argv[2] == "1"

cfg = get_control_server_config()
if not cfg.secret or not (cfg.http_url or cfg.ws_url):
    raise SystemExit(
        "CONTROL_SERVER_SECRET and CONTROL_SERVER_WS_URL (or CONTROL_SERVER_URL) are required"
    )

log_root = Path(os.environ.get("LOG_DIR", "logs"))
workers_dir = log_root / "workers"
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
print(f"Fetching range snapshot from {http_url}/cache/predefined-etag/ranges/snapshot")
with httpx.Client(timeout=60.0) as client:
    resp = client.get(
        f"{http_url}/cache/predefined-etag/ranges/snapshot",
        headers=headers,
    )
    resp.raise_for_status()
    payload = resp.json()

sources = payload.get("sources") or []
print(f"Coverage sources={len(sources)} updated_at={payload.get('updated_at')}")
for src in sources:
    segs = src.get("segments") or []
    print(
        f"  digest={src.get('digest')} segments={len(segs)} "
        f"covered_bytes={src.get('covered_bytes')} "
        f"url={(src.get('source_url') or '')[:80]}"
    )

if skip_chunks:
    print("Skipping range downloads (--metadata-only)")
    raise SystemExit(0)

store = ByteRangeStore(range_dir)
timeout = float(os.environ.get("CONTROL_SERVER_CHUNK_DATA_TIMEOUT", "180"))

downloaded = 0
skipped = 0
missing = 0
errors = 0

print(f"Downloading ranges into {range_dir}")
with httpx.Client(timeout=timeout) as client:
    for src in sources:
        source_url = str(src.get("source_url") or "").strip()
        if not source_url:
            continue
        for seg in src.get("segments") or []:
            try:
                start = int(seg["start"])
                end = int(seg["end"])
            except (KeyError, TypeError, ValueError):
                errors += 1
                continue
            if store.covers(source_url, start, end):
                skipped += 1
                continue
            url = f"{http_url}/cache/predefined-etag/ranges/data"
            try:
                resp = client.get(
                    url,
                    headers=headers,
                    params={"source_url": source_url, "start": start, "end": end},
                )
                if resp.status_code == 404:
                    missing += 1
                    continue
                resp.raise_for_status()
                data = resp.content
                expected = end - start + 1
                if not data or len(data) != expected:
                    errors += 1
                    print(
                        f"  size mismatch src={source_url[:72]} "
                        f"range={start}-{end} got={len(data)} expected={expected}"
                    )
                    continue
                store.ingest(source_url, start, end, data, merge=True)
                if legacy_chunk_data:
                    key = f"{source_url}|{start}|{end}"
                    digest = hashlib.sha256(key.encode()).hexdigest()
                    out_path = chunk_dir / f"{digest}.bin"
                    if not out_path.is_file() or out_path.stat().st_size == 0:
                        out_path.write_bytes(data)
                downloaded += 1
                print(
                    f"  range src={source_url[:72]} {start}-{end} bytes={len(data)}"
                )
            except Exception as exc:
                errors += 1
                print(
                    f"  range fetch failed src={source_url[:72]} "
                    f"{start}-{end} err={exc}"
                )

print(
    f"Ranges: downloaded={downloaded} skipped_covered={skipped} "
    f"missing_on_server={missing} errors={errors} dir={range_dir}"
)
PY
