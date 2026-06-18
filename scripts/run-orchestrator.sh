#!/usr/bin/env bash
# Manage one orchestrator instance via systemd (beam-orchestrator@<instance>.service).
#
# First-time setup:
#   ./scripts/install-systemd.sh --enable
#   ./scripts/install-systemd.sh --enable-orchestrators
#
# Usage:
#   ./scripts/run-orchestrator.sh orch1
#   ./scripts/run-orchestrator.sh orch1 --foreground
#   ./scripts/run-orchestrator.sh orch1 --stop
#   ./scripts/run-orchestrator.sh orch1 --restart
#   ./scripts/run-orchestrator.sh orch1 --status
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BEAM_ROOT="$ROOT"
# shellcheck disable=SC1091
source "${ROOT}/scripts/lib/systemd.sh"

INSTANCE="${1:-}"

usage() {
  cat <<EOF
Usage: $0 <instance> [--foreground|-f | --stop | --restart | --status]

  <instance>     Name matching config/orchestrators/<instance>.env
  --foreground   Run in the foreground (debug; bypasses systemd)
  --stop         Stop the orchestrator via systemd
  --restart      Restart the orchestrator via systemd
  --status       Show systemd status, log path, and API port

Install units once with:
  ./scripts/install-systemd.sh --enable
  ./scripts/install-systemd.sh --enable-orchestrators

Setup:
  cp config/orchestrators/orch1.env.example config/orchestrators/orch1.env

See docs/orchestrator.md.
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

ENV_FILE="${ROOT}/config/orchestrators/${INSTANCE}.env"
LOG_FILE="${ROOT}/logs/orchestrators/${INSTANCE}.log"
SERVICE="beam-orchestrator@${INSTANCE}.service"

mkdir -p "${ROOT}/logs/orchestrators"

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
    echo "  install: ${ROOT}/scripts/install-systemd.sh --enable-orchestrators"
  fi
  echo "  env: ${ENV_FILE}"
  echo "  log: ${LOG_FILE}"
  port="9000"
  if [[ -f "$ENV_FILE" ]]; then
    env_port="$(grep -E '^API_PORT=' "$ENV_FILE" | tail -n1 | cut -d= -f2- || true)"
    if [[ -n "$env_port" ]]; then
      port="$env_port"
    fi
  fi
  echo "  api: http://127.0.0.1:${port}"
  exit 0
fi

if [[ ! -f "$ENV_FILE" ]]; then
  echo "Missing env file: ${ENV_FILE}" >&2
  echo "Copy config/orchestrators/${INSTANCE}.env.example and customize it." >&2
  exit 1
fi

if ! grep -qE '^ORCH_GATEWAY_URL=' "$ENV_FILE"; then
  echo "Missing ORCH_GATEWAY_URL in ${ENV_FILE}" >&2
  exit 1
fi

if ! grep -qE '^WALLET_NAME=' "$ENV_FILE"; then
  echo "Missing WALLET_NAME in ${ENV_FILE}" >&2
  exit 1
fi

if ! grep -qE '^WALLET_HOTKEY=' "$ENV_FILE"; then
  echo "Missing WALLET_HOTKEY in ${ENV_FILE}" >&2
  exit 1
fi

beam_validate_orchestrator_configs

PY="$(beam_python)"
CMD=(
  "$PY" main.py
  --env-file "$ENV_FILE"
)

if [[ "$FOREGROUND" -eq 1 ]]; then
  export LOG_DIR="${ROOT}/logs"
  cd "${ROOT}/neurons/orchestrator"
  exec "${CMD[@]}"
fi

beam_ensure_orchestrator_instance "$INSTANCE"

if [[ "$RESTART" -eq 1 ]]; then
  beam_systemctl restart "$SERVICE"
  echo "Restarted orchestrator ${INSTANCE}"
else
  beam_systemctl start "$SERVICE"
  echo "Started orchestrator ${INSTANCE}"
fi

echo "  service: ${SERVICE}"
echo "  env: ${ENV_FILE}"
echo "  log: ${LOG_FILE}"
port="9000"
if [[ -f "$ENV_FILE" ]]; then
  env_port="$(grep -E '^API_PORT=' "$ENV_FILE" | tail -n1 | cut -d= -f2- || true)"
  if [[ -n "$env_port" ]]; then
    port="$env_port"
  fi
fi
echo "  api: http://127.0.0.1:${port}"
