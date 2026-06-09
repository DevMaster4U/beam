#!/usr/bin/env bash
# Manage the dedicated worker-gateway via systemd.
#
# First-time setup:
#   ./scripts/install-systemd.sh --enable
#
# Usage:
#   ./scripts/run-worker-gateway.sh
#   ./scripts/run-worker-gateway.sh --foreground
#   ./scripts/run-worker-gateway.sh --stop
#   ./scripts/run-worker-gateway.sh --restart
#   ./scripts/run-worker-gateway.sh --status
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BEAM_ROOT="$ROOT"
# shellcheck disable=SC1091
source "${ROOT}/scripts/lib/systemd.sh"

SERVICE="beam-worker-gateway.service"
ENV_FILE="${ROOT}/.env"
LOG_FILE="${ROOT}/logs/gateway.log"

usage() {
  cat <<EOF
Usage: $0 [--foreground|-f | --stop | --restart | --status]

  (default)    Start worker-gateway via systemd
  --foreground Run in the foreground (debug; bypasses systemd)
  --stop       Stop worker-gateway
  --restart    Restart worker-gateway
  --status     Show systemd status, log path, and listening port

Install units once with:
  ./scripts/install-systemd.sh --enable

See docs/worker-gateway.md.
EOF
}

FOREGROUND=0
STOP=0
RESTART=0
STATUS=0

while [[ $# -gt 0 ]]; do
  case "$1" in
    --foreground|-f)
      FOREGROUND=1
      shift
      ;;
    --stop)
      STOP=1
      shift
      ;;
    --restart|-r)
      RESTART=1
      shift
      ;;
    --status)
      STATUS=1
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

mkdir -p "${ROOT}/logs"

if [[ "$STOP" -eq 1 ]]; then
  beam_require_unit "$SERVICE"
  beam_systemctl stop "$SERVICE"
  exit 0
fi

if [[ "$STATUS" -eq 1 ]]; then
  if beam_unit_installed "$SERVICE"; then
    beam_systemctl status "$SERVICE" --no-pager || true
  else
    echo "worker-gateway: unit not installed"
    echo "  install: ${ROOT}/scripts/install-systemd.sh --enable"
  fi
  echo "  env: ${ENV_FILE}"
  echo "  log: ${LOG_FILE}"
  if [[ -f "$ENV_FILE" ]]; then
    port="$(grep -E '^GATEWAY_PORT=' "$ENV_FILE" | tail -n1 | cut -d= -f2- || true)"
    port="${port:-8001}"
    echo "  port: ${port}"
  fi
  exit 0
fi

if [[ ! -f "$ENV_FILE" ]]; then
  echo "Missing env file: ${ENV_FILE}" >&2
  echo "Copy .env.example and set GATEWAY_CONTROL_SECRET and GATEWAY_WORKER_SECRET." >&2
  exit 1
fi

if ! grep -qE '^(GATEWAY_CONTROL_SECRET|WORKER_GATEWAY_CONTROL_SECRET)=' "$ENV_FILE"; then
  echo "Missing GATEWAY_CONTROL_SECRET or WORKER_GATEWAY_CONTROL_SECRET in ${ENV_FILE}" >&2
  exit 1
fi

if ! grep -qE '^(GATEWAY_WORKER_SECRET|WORKER_GATEWAY_WORKER_SECRET)=' "$ENV_FILE"; then
  echo "Missing GATEWAY_WORKER_SECRET or WORKER_GATEWAY_WORKER_SECRET in ${ENV_FILE}" >&2
  exit 1
fi

PY="$(beam_python)"
CMD=("$PY" "${ROOT}/worker-gateway/main.py")

if [[ "$FOREGROUND" -eq 1 ]]; then
  cd "${ROOT}/worker-gateway"
  exec "${CMD[@]}"
fi

beam_require_unit "$SERVICE"

if [[ "$RESTART" -eq 1 ]]; then
  beam_systemctl restart "$SERVICE"
  echo "Restarted worker-gateway"
else
  beam_systemctl start "$SERVICE"
  echo "Started worker-gateway"
fi

echo "  service: ${SERVICE}"
echo "  env: ${ENV_FILE}"
echo "  log: ${LOG_FILE}"
