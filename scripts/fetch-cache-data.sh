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
# Streams each segment to disk (no full 1 GiB RAM buffer). Optional parallel:
#   FETCH_CACHE_PARALLEL=2 ./scripts/fetch-cache-data.sh config/workers/worker1.env
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
  FETCH_CACHE_PARALLEL    Concurrent segment downloads (default: 2)
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
import shutil
import sys
import tempfile
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

from neurons.common.byte_range_store import ByteRangeStore, COPY_CHUNK
from neurons.common.control_client import (
    fetch_predefined_etag_range_to_file,
    get_control_server_config,
)

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
tmp_dir = range_dir / ".tmp"
workers_dir.mkdir(parents=True, exist_ok=True)
range_dir.mkdir(parents=True, exist_ok=True)
tmp_dir.mkdir(parents=True, exist_ok=True)
if legacy_chunk_data:
    chunk_dir.mkdir(parents=True, exist_ok=True)

headers = {"X-Control-Server-Secret": cfg.secret}
if cfg.miner_id:
    headers["X-Miner-Id"] = cfg.miner_id

http_url = cfg.http_url.rstrip("/")
print(f"Fetching range snapshot from {http_url}/cache/predefined-etag/ranges/snapshot")

import httpx

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
try:
    parallel = max(1, int(os.environ.get("FETCH_CACHE_PARALLEL", "2")))
except ValueError:
    parallel = 2

jobs: list[tuple[str, int, int]] = []
skipped = 0
for src in sources:
    source_url = str(src.get("source_url") or "").strip()
    if not source_url:
        continue
    for seg in src.get("segments") or []:
        try:
            start = int(seg["start"])
            end = int(seg["end"])
        except (KeyError, TypeError, ValueError):
            continue
        if store.covers(source_url, start, end):
            skipped += 1
            continue
        jobs.append((source_url, start, end))

print(
    f"Downloading ranges into {range_dir} "
    f"(pending={len(jobs)} skipped_covered={skipped} parallel={parallel})"
)


def _download_one(source_url: str, start: int, end: int) -> tuple[str, str, int, int, int]:
    """Returns (status, source_url, start, end, bytes) status in downloaded|missing|error."""
    expected = end - start + 1
    fd, tmp_name = tempfile.mkstemp(prefix=f"fetch_{start}_", suffix=".bin", dir=tmp_dir)
    os.close(fd)
    tmp_path = Path(tmp_name)
    try:
        ok = fetch_predefined_etag_range_to_file(
            source_url, start, end, tmp_path, config=cfg
        )
        if not ok:
            return ("missing", source_url, start, end, 0)
        if tmp_path.stat().st_size != expected:
            return ("error", source_url, start, end, tmp_path.stat().st_size)
        store.ingest_from_file(source_url, start, end, tmp_path, merge=True)
        if legacy_chunk_data:
            key = f"{source_url}|{start}|{end}"
            digest = hashlib.sha256(key.encode()).hexdigest()
            out_path = chunk_dir / f"{digest}.bin"
            if not out_path.is_file() or out_path.stat().st_size == 0:
                with tmp_path.open("rb") as src, out_path.open("wb") as dst:
                    shutil.copyfileobj(src, dst, length=COPY_CHUNK)
        return ("downloaded", source_url, start, end, expected)
    except Exception:
        return ("error", source_url, start, end, 0)
    finally:
        tmp_path.unlink(missing_ok=True)


downloaded = 0
missing = 0
errors = 0

with ThreadPoolExecutor(max_workers=parallel) as pool:
    futures = [
        pool.submit(_download_one, source_url, start, end)
        for source_url, start, end in jobs
    ]
    for fut in as_completed(futures):
        status, source_url, start, end, nbytes = fut.result()
        if status == "downloaded":
            downloaded += 1
            print(f"  range src={source_url[:72]} {start}-{end} bytes={nbytes}")
        elif status == "missing":
            missing += 1
            print(f"  miss src={source_url[:72]} {start}-{end}")
        else:
            errors += 1
            print(f"  error src={source_url[:72]} {start}-{end}")

print(
    f"Ranges: downloaded={downloaded} skipped_covered={skipped} "
    f"missing_on_server={missing} errors={errors} dir={range_dir}"
)
PY
