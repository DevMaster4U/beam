#!/usr/bin/env bash
# Remove predefined ETag chunk .bin files (local and/or control-server remote).
#
# Usage:
#   ./scripts/prune-cache-data.sh config/workers/worker_h1.env --remote
#   ./scripts/prune-cache-data.sh config/workers/worker_h1.env --remote --all
#   ./scripts/prune-cache-data.sh config/workers/worker_h1.env --local
#   ./scripts/prune-cache-data.sh config/workers/worker_h1.env --local --all
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BEAM_ROOT="$ROOT"
# shellcheck disable=SC1091
source "${ROOT}/scripts/lib/systemd.sh"

ENV_FILE="${1:-}"
shift || true

REMOTE=0
LOCAL=0
DELETE_ALL=0

usage() {
  cat <<EOF
Usage: $0 [env-file] [--remote | --local] [--all]

  --remote   Delete chunk .bin files on control-server
  --local    Delete chunk .bin files under logs/workers/predefined_etag_chunk_data/
  --all      Delete every .bin (default: orphans / stale flags only on remote)

Examples:
  $0 config/workers/worker_h1.env --remote          # prune orphan remote .bin files
  $0 config/workers/worker_h1.env --remote --all    # delete ALL remote .bin files
  $0 config/workers/worker_h1.env --local --all     # wipe local downloaded chunks
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --remote)
      REMOTE=1
      shift
      ;;
    --local)
      LOCAL=1
      shift
      ;;
    --all)
      DELETE_ALL=1
      shift
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

if [[ "$REMOTE" -eq 0 && "$LOCAL" -eq 0 ]]; then
  echo "Specify --remote and/or --local" >&2
  usage >&2
  exit 1
fi

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

if [[ "$LOCAL" -eq 1 ]]; then
  CHUNK_DIR="${LOG_DIR}/workers/predefined_etag_chunk_data"
  if [[ ! -d "$CHUNK_DIR" ]]; then
    echo "Local chunk dir missing: ${CHUNK_DIR}"
  elif [[ "$DELETE_ALL" -eq 1 ]]; then
    count="$(find "$CHUNK_DIR" -maxdepth 1 -name '*.bin' | wc -l | tr -d ' ')"
    find "$CHUNK_DIR" -maxdepth 1 -name '*.bin' -delete
    echo "Local: deleted ${count} .bin file(s) from ${CHUNK_DIR}"
  else
    echo "Local: use --all to delete downloaded .bin files under ${CHUNK_DIR}"
  fi
fi

if [[ "$REMOTE" -eq 1 ]]; then
  exec "$PY" - "$DELETE_ALL" <<'PY'
import json
import sys

from neurons.common.control_client import (
    get_control_server_config,
    prune_predefined_etag_chunk_data_remote,
)

delete_all = sys.argv[1] == "1"
cfg = get_control_server_config()
if not cfg.secret or not cfg.http_url:
    raise SystemExit("CONTROL_SERVER_SECRET and CONTROL_SERVER_WS_URL/URL are required")

result = prune_predefined_etag_chunk_data_remote(all_files=delete_all)
if result is None:
    raise SystemExit("Remote prune failed — is control-server running with the delete API?")

print(json.dumps(result, indent=2))
PY
fi
