#!/usr/bin/env bash
# Bootstrap a miner from control-server: fetch env, sync wallet, run orchestrator/worker.
#
# Usage:
#   CONTROL_SERVER_URL=http://control:8010 CONTROL_SERVER_SECRET=secret \
#     ./scripts/run-miner-from-control.sh miner1 orchestrator
#
#   CONTROL_SERVER_URL=... CONTROL_SERVER_SECRET=... \
#     ./scripts/run-miner-from-control.sh miner2 worker --foreground
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
MINER_ID="${1:-}"
ROLE="${2:-orchestrator}"
FOREGROUND=0
EXTRA_ARGS=()

usage() {
  cat <<EOF
Usage: $0 <miner_id> [orchestrator|worker] [--foreground] [extra args...]

Environment:
  CONTROL_SERVER_WS_URL   WebSocket for cache broadcast, e.g. ws://10.0.0.1:8010/ws/miners
  CONTROL_SERVER_SECRET   Shared secret (X-Control-Server-Secret / hello message)
  CONTROL_SERVER_MINER_ID Unique id per miner server
  CONTROL_SERVER_URL      Optional HTTP base for env/wallet bootstrap (derived from WS if omitted)
EOF
}

if [[ -z "$MINER_ID" ]]; then
  usage >&2
  exit 1
fi

shift 2 || true

while [[ $# -gt 0 ]]; do
  case "$1" in
    --foreground|-f)
      FOREGROUND=1
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

: "${CONTROL_SERVER_WS_URL:?CONTROL_SERVER_WS_URL is required (e.g. ws://host:8010/ws/miners)}"
: "${CONTROL_SERVER_SECRET:?CONTROL_SERVER_SECRET is required}"

export CONTROL_SERVER_MINER_ID="$MINER_ID"
export PYTHONPATH="${ROOT}:${PYTHONPATH:-}"

# Derive HTTP base for env/wallet REST if not set
if [[ -z "${CONTROL_SERVER_URL:-}" ]]; then
  CONTROL_SERVER_URL="$(
    python3 - <<'PY'
import os
from neurons.common.control_client import resolve_control_server_urls
http_url, _ = resolve_control_server_urls()
if not http_url:
    raise SystemExit("Failed to derive CONTROL_SERVER_URL from CONTROL_SERVER_WS_URL")
print(http_url)
PY
  )"
  export CONTROL_SERVER_URL
fi

ENV_DIR="${ROOT}/config/miners"
mkdir -p "$ENV_DIR"
ENV_FILE="${ENV_DIR}/${MINER_ID}.env"

echo "Fetching miner env: ${MINER_ID}"
curl -fsS \
  -H "X-Control-Server-Secret: ${CONTROL_SERVER_SECRET}" \
  "${CONTROL_SERVER_URL%/}/miners/${MINER_ID}/env" \
  -o "$ENV_FILE"

set -a
# shellcheck disable=SC1090
source "$ENV_FILE"
set +a

echo "Syncing wallets from control-server..."
python3 - <<'PY'
from neurons.common.wallet_sync import ensure_wallets_from_control_server

ensure_wallets_from_control_server()
print("Wallet sync OK")
PY

export LOG_DIR="${ROOT}/logs"

if [[ "$ROLE" == "worker" ]]; then
  CMD=(python3 "${ROOT}/neurons/worker/worker.py" --env-file "$ENV_FILE")
elif [[ "$ROLE" == "orchestrator" ]]; then
  CMD=(python3 main.py --env-file "$ENV_FILE")
else
  echo "Unknown role: $ROLE (use orchestrator or worker)" >&2
  exit 1
fi

if [[ ${#EXTRA_ARGS[@]} -gt 0 ]]; then
  CMD+=("${EXTRA_ARGS[@]}")
fi

if [[ "$ROLE" == "orchestrator" ]]; then
  cd "${ROOT}/neurons/orchestrator"
fi

if [[ "$FOREGROUND" -eq 1 ]]; then
  exec "${CMD[@]}"
fi

echo "Started ${ROLE} ${MINER_ID} in foreground-only mode for now."
echo "Re-run with --foreground"
exit 1
