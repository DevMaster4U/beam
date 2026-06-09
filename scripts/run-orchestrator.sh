#!/usr/bin/env bash
# Manage the BEAM orchestrator via systemd.
#
# First-time setup:
#   ./scripts/install-systemd.sh --enable
#
# Usage:
#   ./scripts/run-orchestrator.sh              # start
#   ./scripts/run-orchestrator.sh --foreground # foreground (debug)
#   ./scripts/run-orchestrator.sh --stop
#   ./scripts/run-orchestrator.sh --restart
#   ./scripts/run-orchestrator.sh --status
#   ./scripts/run-orchestrator.sh --env-file path/to/.env
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BEAM_ROOT="$ROOT"
# shellcheck disable=SC1091
source "${ROOT}/scripts/lib/systemd.sh"

SERVICE="beam-orchestrator.service"
DEFAULT_ENV_FILE="${ROOT}/.env"
FALLBACK_ENV_FILE="${ROOT}/neurons/orchestrator/.env"
ENV_FILE="${DEFAULT_ENV_FILE}"
LOG_FILE="${ROOT}/logs/miner.log"

usage() {
  cat <<EOF
Usage: $0 [--foreground|-f | --stop | --restart | --status | --env-file PATH]

  (default)    Start orchestrator via systemd
  --foreground Run in the foreground (debug; bypasses systemd)
  --stop       Stop orchestrator
  --restart    Restart orchestrator
  --status     Show systemd status, log path, and API port
  --env-file   Env file to validate before start (default: ${DEFAULT_ENV_FILE})

Install units once with:
  ./scripts/install-systemd.sh --enable

See docs/orchestrator.md.
EOF
}

load_env_file() {
  local file="$1"
  set -a
  # shellcheck disable=SC1090
  source "$file"
  set +a
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
    --env-file)
      if [[ $# -lt 2 ]]; then
        echo "--env-file requires a path" >&2
        exit 1
      fi
      ENV_FILE="$2"
      shift 2
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
    echo "orchestrator: unit not installed"
    echo "  install: ${ROOT}/scripts/install-systemd.sh --enable"
  fi
  echo "  env: ${ENV_FILE}"
  echo "  log: ${LOG_FILE}"
  port="${API_PORT:-9000}"
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
  if [[ -f "$FALLBACK_ENV_FILE" ]]; then
    ENV_FILE="$FALLBACK_ENV_FILE"
  else
    echo "Missing env file: ${DEFAULT_ENV_FILE}" >&2
    echo "Copy .env.example or neurons/orchestrator/.env.example and customize." >&2
    exit 1
  fi
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

PY="$(beam_python)"
CMD=("$PY" main.py)

if [[ "$FOREGROUND" -eq 1 ]]; then
  load_env_file "$ENV_FILE"
  export LOG_DIR="${ROOT}/logs"
  cd "${ROOT}/neurons/orchestrator"
  exec "${CMD[@]}"
fi

beam_require_unit "$SERVICE"

if [[ "$RESTART" -eq 1 ]]; then
  beam_systemctl restart "$SERVICE"
  echo "Restarted orchestrator"
else
  beam_systemctl start "$SERVICE"
  echo "Started orchestrator"
fi

echo "  service: ${SERVICE}"
echo "  env: ${ENV_FILE}"
echo "  log: ${LOG_FILE}"
port="${API_PORT:-9000}"
if [[ -f "$ENV_FILE" ]]; then
  env_port="$(grep -E '^API_PORT=' "$ENV_FILE" | tail -n1 | cut -d= -f2- || true)"
  if [[ -n "$env_port" ]]; then
    port="$env_port"
  fi
fi
echo "  api: http://127.0.0.1:${port}"
