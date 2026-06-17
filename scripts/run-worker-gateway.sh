#!/usr/bin/env bash
# Manage one worker-gateway instance via systemd (beam-worker-gateway@<instance>.service).
#
# First-time setup:
#   ./scripts/install-systemd.sh --enable
#   ./scripts/install-systemd.sh --enable-gateways
#
# Usage:
#   ./scripts/run-worker-gateway.sh gateway1
#   ./scripts/run-worker-gateway.sh gateway1 --foreground
#   ./scripts/run-worker-gateway.sh gateway1 --stop
#   ./scripts/run-worker-gateway.sh gateway1 --restart
#   ./scripts/run-worker-gateway.sh gateway1 --status
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BEAM_ROOT="$ROOT"
# shellcheck disable=SC1091
source "${ROOT}/scripts/lib/systemd.sh"

INSTANCE="${1:-}"

usage() {
  cat <<EOF
Usage: $0 <instance> [--foreground|-f | --stop | --restart | --status]

  <instance>     Name matching config/gateways/<instance>.env
  --foreground   Run in the foreground (debug; bypasses systemd)
  --stop         Stop the gateway via systemd
  --restart      Restart the gateway via systemd
  --status       Show systemd status, log path, and listening port

Install units once with:
  ./scripts/install-systemd.sh --enable
  ./scripts/install-systemd.sh --enable-gateways

Setup:
  cp config/gateways/gateway1.env.example config/gateways/gateway1.env

See docs/worker-gateway.md.
EOF
}

if [[ -z "$INSTANCE" || "$INSTANCE" == "-h" || "$INSTANCE" == "--help" ]]; then
  usage >&2
  exit 1
fi

shift

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

ENV_FILE="${ROOT}/config/gateways/${INSTANCE}.env"
LOG_FILE="${ROOT}/logs/gateways/${INSTANCE}.log"
SERVICE="beam-worker-gateway@${INSTANCE}.service"

mkdir -p "${ROOT}/logs/gateways"

if [[ "$STOP" -eq 1 ]]; then
  beam_require_unit "$SERVICE"
  beam_systemctl stop "$SERVICE"
  exit 0
fi

if [[ "$STATUS" -eq 1 ]]; then
  if beam_unit_installed "$SERVICE"; then
    beam_systemctl status "$SERVICE" --no-pager || true
  else
    echo "${INSTANCE}: unit not installed"
    echo "  install: ${ROOT}/scripts/install-systemd.sh --enable-gateways"
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
  echo "Copy config/gateways/${INSTANCE}.env.example and customize it." >&2
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
CMD=(
  "$PY" "${ROOT}/worker-gateway/main.py"
  --env-file "$ENV_FILE"
)

if [[ "$FOREGROUND" -eq 1 ]]; then
  cd "${ROOT}/worker-gateway"
  exec "${CMD[@]}"
fi

beam_ensure_gateway_instance "$INSTANCE"

if [[ "$RESTART" -eq 1 ]]; then
  beam_systemctl restart "$SERVICE"
  echo "Restarted worker-gateway ${INSTANCE}"
else
  beam_systemctl start "$SERVICE"
  echo "Started worker-gateway ${INSTANCE}"
fi

echo "  service: ${SERVICE}"
echo "  env: ${ENV_FILE}"
echo "  log: ${LOG_FILE}"
