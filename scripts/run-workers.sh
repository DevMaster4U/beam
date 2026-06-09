#!/usr/bin/env bash
# Manage all configured workers via systemd.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BEAM_ROOT="$ROOT"
# shellcheck disable=SC1091
source "${ROOT}/scripts/lib/systemd.sh"

ACTION="${1:-start}"

usage() {
  cat <<EOF
Usage: $0 [start|stop|restart|status]

  start    Start every config/workers/*.env instance via systemd
  stop     Stop every configured instance
  restart  Restart every configured instance
  status   Show systemd status for configured instances

Install units once with:
  ./scripts/install-systemd.sh --enable
  ./scripts/install-systemd.sh --enable-workers
EOF
}

list_instances() {
  shopt -s nullglob
  for env_file in "${ROOT}/config/workers/"*.env; do
    basename "$env_file" .env
  done
}

require_instances() {
  local found=0
  for _ in $(list_instances); do
    found=1
    break
  done
  if [[ "$found" -eq 0 ]]; then
    echo "No worker env files found in config/workers/*.env" >&2
    echo "Copy *.env.example files first." >&2
    exit 1
  fi
}

case "$ACTION" in
  start)
    require_instances
    beam_require_unit "beam-worker@.service"
    beam_sync_workers
    beam_systemctl start beam-workers.target
    for instance in $(list_instances); do
      echo "  beam-worker@${instance}.service -> config/workers/${instance}.env"
      echo "  log: ${ROOT}/logs/workers/${instance}.log"
    done
    ;;
  stop)
    for instance in $(list_instances); do
      service="beam-worker@${instance}.service"
      if beam_unit_installed "$service"; then
        beam_systemctl stop "$service" || true
        echo "Stopped worker ${instance}"
      fi
    done
    ;;
  restart)
    require_instances
    beam_require_unit "beam-worker@.service"
    beam_sync_workers
    beam_systemctl restart beam-workers.target
    echo "Restarted all workers via beam-workers.target"
    ;;
  status)
    for instance in $(list_instances); do
      "${ROOT}/scripts/run-worker.sh" "$instance" --status
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
