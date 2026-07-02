#!/usr/bin/env bash
# Run the BEAM control-server (miner env, shared cache, wallet bundles).
#
# Usage:
#   ./scripts/run-control-server.sh
#   ./scripts/run-control-server.sh --foreground
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
ENV_FILE="${ROOT}/config/control-server.env"
FOREGROUND=0

while [[ $# -gt 0 ]]; do
  case "$1" in
    --foreground|-f)
      FOREGROUND=1
      shift
      ;;
    -h|--help)
      echo "Usage: $0 [--foreground]"
      exit 0
      ;;
    *)
      echo "Unknown argument: $1" >&2
      exit 1
      ;;
  esac
done

if [[ ! -f "$ENV_FILE" ]]; then
  echo "Missing ${ENV_FILE}" >&2
  echo "Copy config/control-server.env.example to config/control-server.env" >&2
  exit 1
fi

set -a
# shellcheck disable=SC1090
source "$ENV_FILE"
set +a

export PYTHONPATH="${ROOT}:${PYTHONPATH:-}"
mkdir -p "${ROOT}/data/control-server/miners" "${ROOT}/data/control-server/cache" "${ROOT}/data/control-server/wallets"

if [[ "$FOREGROUND" -eq 1 ]]; then
  cd "${ROOT}/control-server"
  exec python3 main.py
fi

echo "Use --foreground to run in the terminal."
echo "Install systemd unit: deploy/systemd/beam-control-server.service"
