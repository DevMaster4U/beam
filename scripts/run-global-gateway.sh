#!/usr/bin/env bash
# Run the shared global worker gateway (foreground).
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
ENV_FILE="${ROOT}/config/global-gateway.env"

if [[ -f "$ENV_FILE" ]]; then
  set -a
  # shellcheck disable=SC1090
  source "$ENV_FILE"
  set +a
else
  echo "Warning: $ENV_FILE not found — using environment / defaults" >&2
fi

cd "${ROOT}/global-gateway"
export PYTHONPATH="${ROOT}/global-gateway:${PYTHONPATH:-}"
exec python main.py
