#!/usr/bin/env bash
# Manage the BEAM control-server via systemd (beam-control-server.service).
#
# First-time setup:
#   cp config/control-server.env.example config/control-server.env
#   sudo ./scripts/install-systemd.sh --enable
#   sudo ./scripts/install-systemd.sh --enable-control-server
#
# Usage:
#   ./scripts/run-control-server.sh start
#   ./scripts/run-control-server.sh stop
#   ./scripts/run-control-server.sh restart
#   ./scripts/run-control-server.sh foreground
#   ./scripts/run-control-server.sh status
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
  sudo ./scripts/install-systemd.sh --enable
  sudo ./scripts/install-systemd.sh --enable-control-server

Setup:
  cp config/control-server.env.example config/control-server.env

Logs: logs/control-server/control-server.log
Data: data/control-server/
EOF
}

if [[ "$ACTION" == "-h" || "$ACTION" == "--help" ]]; then
  usage
  exit 0
fi

ENV_FILE="${ROOT}/config/control-server.env"
LOG_FILE="${ROOT}/logs/control-server/control-server.log"
SERVICE="beam-control-server.service"

beam_prepare_data_dirs

read_control_port() {
  local port="8010"
  if [[ -f "$ENV_FILE" ]]; then
    local env_port
    env_port="$(beam_read_env_value "$ENV_FILE" "CONTROL_SERVER_PORT")"
    if [[ -n "$env_port" ]]; then
      port="$env_port"
    fi
  fi
  printf '%s' "$port"
}

read_control_host() {
  local host="0.0.0.0"
  if [[ -f "$ENV_FILE" ]]; then
    local env_host
    env_host="$(beam_read_env_value "$ENV_FILE" "CONTROL_SERVER_HOST")"
    if [[ -n "$env_host" ]]; then
      host="$env_host"
    fi
  fi
  printf '%s' "$host"
}

print_control_info() {
  local port host
  port="$(read_control_port)"
  host="$(read_control_host)"
  echo "  service: ${SERVICE}"
  echo "  env: ${ENV_FILE}"
  echo "  log: ${LOG_FILE}"
  if [[ -f "$LOG_FILE" ]]; then
    echo "  log_bytes: $(wc -c < "$LOG_FILE" | tr -d ' ')"
  else
    echo "  log_bytes: 0 (file missing — check journal)"
    echo "  journal: journalctl -u ${SERVICE} -n 50 --no-pager"
  fi
  if [[ "$host" == "0.0.0.0" || "$host" == "::" ]]; then
    echo "  http: http://127.0.0.1:${port}  (wallet/env bootstrap only)"
    echo "  ws:   ws://127.0.0.1:${port}/ws/miners  (cache broadcast — miners use this)"
  else
    echo "  http: http://${host}:${port}  (wallet/env bootstrap only)"
    echo "  ws:   ws://${host}:${port}/ws/miners  (cache broadcast — miners use this)"
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
      echo "control-server: unit not installed"
      echo "  install: sudo ${ROOT}/scripts/install-systemd.sh --enable-control-server"
    fi
    print_control_info
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
  echo "Copy config/control-server.env.example to config/control-server.env" >&2
  exit 1
fi

PY="$(beam_python)"
CMD=(
  "$PY" "${ROOT}/control-server/main.py"
)

if [[ "$ACTION" == "foreground" ]]; then
  set -a
  # shellcheck disable=SC1090
  source "$ENV_FILE"
  set +a
  export PYTHONPATH="${ROOT}:${PYTHONPATH:-}"
  cd "${ROOT}/control-server"
  exec "${CMD[@]}"
fi

beam_ensure_control_server

if [[ "$ACTION" == "restart" ]]; then
  beam_systemctl restart "$SERVICE"
  echo "Restarted control server"
else
  beam_systemctl start "$SERVICE"
  echo "Started control server"
fi

if beam_unit_installed "$SERVICE"; then
  active="$(systemctl is-active "$SERVICE" 2>/dev/null || true)"
  echo "  state: ${active:-unknown}"
  if [[ "${active}" != "active" ]]; then
    echo "  warning: service is not active — check: journalctl -u ${SERVICE} -n 50 --no-pager" >&2
  fi
fi

print_control_info
