#!/usr/bin/env bash
# Manage all configured orchestrators via systemd.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BEAM_ROOT="$ROOT"
# shellcheck disable=SC1091
source "${ROOT}/scripts/lib/systemd.sh"

ACTION="${1:-start}"

usage() {
  cat <<EOF
Usage: $0 [start|stop|restart|status]

  start    Start every config/orchestrators/*.env instance via systemd
  stop     Stop every configured instance
  restart  Restart every configured instance
  status   Show systemd status for configured instances

Install units once with:
  ./scripts/install-systemd.sh --enable
  ./scripts/install-systemd.sh --enable-orchestrators
EOF
}

list_instances() {
  beam_list_orchestrator_instances
}

require_instances() {
  local found=0
  for _ in $(list_instances); do
    found=1
    break
  done
  if [[ "$found" -eq 0 ]]; then
    echo "No orchestrator env files found in config/orchestrators/*.env" >&2
    echo "Copy *.env.example files first." >&2
    exit 1
  fi
}

case "$ACTION" in
  start)
    require_instances
    beam_require_unit "beam-orchestrator@.service"
    beam_sync_orchestrators
    beam_systemctl start beam-orchestrators.target
    for instance in $(list_instances); do
      echo "  beam-orchestrator@${instance}.service -> config/orchestrators/${instance}.env"
      echo "  log: ${ROOT}/logs/orchestrators/${instance}.log"
    done
    ;;
  stop)
    for instance in $(list_instances); do
      service="beam-orchestrator@${instance}.service"
      if beam_unit_installed "$service"; then
        beam_systemctl stop "$service" || true
        echo "Stopped orchestrator ${instance}"
      fi
    done
    ;;
  restart)
    require_instances
    beam_require_unit "beam-orchestrator@.service"
    beam_sync_orchestrators
    beam_systemctl restart beam-orchestrators.target
    echo "Restarted all orchestrators via beam-orchestrators.target"
    ;;
  status)
    for instance in $(list_instances); do
      "${ROOT}/scripts/run-orchestrator.sh" "$instance" --status
      echo
    done
    ;;
  -h|--help)
    usage
    ;;
  *)
    usage >&2
    exit 1
    ;;
esac
