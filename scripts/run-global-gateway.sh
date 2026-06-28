#!/usr/bin/env bash
# Manage the shared global worker gateway via systemd (beam-global-gateway.service).
#
# First-time setup (AWS EC2 / Ubuntu):
#   ./scripts/setup-ec2.sh
#   sudo ./scripts/install-systemd.sh --enable
#   sudo ./scripts/install-systemd.sh --enable-global-gateway
#
# Usage:
#   ./scripts/run-global-gateway.sh start
#   ./scripts/run-global-gateway.sh stop
#   ./scripts/run-global-gateway.sh restart
#   ./scripts/run-global-gateway.sh foreground
#   ./scripts/run-global-gateway.sh status
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BEAM_ROOT="$ROOT"
# shellcheck disable=SC1091
source "${ROOT}/scripts/lib/systemd.sh"

ACTION="${1:-start}"

usage() {
  cat <<EOF
Usage: $0 [start | stop | restart | foreground | status]

  start        Start via systemd (default)
  stop         Stop via systemd
  restart      Restart via systemd
  foreground   Run in the foreground (debug; bypasses systemd)
  status       Show systemd status, env path, and log path

Install units once with:
  ./scripts/install-systemd.sh --enable
  ./scripts/install-systemd.sh --enable-global-gateway

Setup:
  cp config/global-gateway.env.example config/global-gateway.env

Logs: logs/global-gateway/global-gateway.log
EOF
}

if [[ "$ACTION" == "-h" || "$ACTION" == "--help" ]]; then
  usage
  exit 0
fi

ENV_FILE="${ROOT}/config/global-gateway.env"
LOG_FILE="${ROOT}/logs/global-gateway/global-gateway.log"
SERVICE="beam-global-gateway.service"

beam_prepare_data_dirs

read_gateway_port() {
  local port="8001"
  if [[ -f "$ENV_FILE" ]]; then
    local env_port
    env_port="$(beam_read_env_value "$ENV_FILE" "GATEWAY_PORT")"
    if [[ -n "$env_port" ]]; then
      port="$env_port"
    fi
  fi
  printf '%s' "$port"
}

read_gateway_host() {
  local host="0.0.0.0"
  if [[ -f "$ENV_FILE" ]]; then
    local env_host
    env_host="$(beam_read_env_value "$ENV_FILE" "GATEWAY_HOST")"
    if [[ -n "$env_host" ]]; then
      host="$env_host"
    fi
  fi
  printf '%s' "$host"
}

print_gateway_info() {
  local port host
  port="$(read_gateway_port)"
  host="$(read_gateway_host)"
  echo "  service: ${SERVICE}"
  echo "  env: ${ENV_FILE}"
  echo "  log: ${LOG_FILE}"
  if [[ -f "$LOG_FILE" ]]; then
    echo "  log_bytes: $(wc -c < "$LOG_FILE" | tr -d ' ')"
  else
    echo "  log_bytes: 0 (file missing — service may have failed before logging started)"
    echo "  journal: journalctl -u ${SERVICE} -n 50 --no-pager"
  fi
  if [[ "$host" == "0.0.0.0" || "$host" == "::" ]]; then
    echo "  url: http://127.0.0.1:${port}"
  else
    echo "  url: http://${host}:${port}"
  fi
}

case "$ACTION" in
  stop)
    beam_require_unit "$SERVICE"
    beam_systemctl stop "$SERVICE"
    exit 0
    ;;
  status)
    if beam_unit_installed "$SERVICE"; then
      beam_systemctl status "$SERVICE" --no-pager || true
    else
      echo "global-gateway: unit not installed"
      echo "  install: ${ROOT}/scripts/install-systemd.sh --enable-global-gateway"
    fi
    print_gateway_info
    exit 0
    ;;
  foreground|start|restart)
    ;;
  *)
    echo "Unknown action: ${ACTION}" >&2
    usage >&2
    exit 1
    ;;
esac

if [[ ! -f "$ENV_FILE" ]]; then
  echo "Missing env file: ${ENV_FILE}" >&2
  echo "Copy config/global-gateway.env.example to config/global-gateway.env" >&2
  exit 1
fi

PY="$(beam_python)"
CMD=(
  "$PY" "${ROOT}/global-gateway/main.py"
)

if [[ "$ACTION" == "foreground" ]]; then
  set -a
  # shellcheck disable=SC1090
  source "$ENV_FILE"
  set +a
  export LOG_DIR="${ROOT}/logs"
  cd "${ROOT}/global-gateway"
  export PYTHONPATH="${ROOT}/global-gateway:${PYTHONPATH:-}"
  exec "${CMD[@]}"
fi

beam_ensure_global_gateway

if [[ "$ACTION" == "restart" ]]; then
  beam_systemctl restart "$SERVICE"
  echo "Restarted global gateway"
else
  beam_systemctl start "$SERVICE"
  echo "Started global gateway"
fi

if beam_unit_installed "$SERVICE"; then
  active="$(systemctl is-active "$SERVICE" 2>/dev/null || true)"
  echo "  state: ${active:-unknown}"
  if [[ "${active}" != "active" ]]; then
    echo "  warning: service is not active — check: journalctl -u ${SERVICE} -n 50 --no-pager" >&2
  fi
fi

print_gateway_info
