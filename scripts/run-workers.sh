#!/usr/bin/env bash
# Start or stop all workers listed in config/workers/*.env (excluding *.example).
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
ACTION="${1:-start}"

usage() {
  cat <<EOF
Usage: $0 [start|stop|restart|status]

  start    Start every config/workers/*.env instance in the background
  stop     Stop every running instance
  restart  Stop then start every configured instance
  status   Show pid/log paths for configured instances
EOF
}

list_instances() {
  shopt -s nullglob
  for env_file in "${ROOT}/config/workers/"*.env; do
    basename "$env_file" .env
  done
}

case "$ACTION" in
  start)
    found=0
    for instance in $(list_instances); do
      found=1
      "${ROOT}/scripts/run-worker.sh" "$instance"
    done
    if [[ "$found" -eq 0 ]]; then
      echo "No worker env files found in config/workers/*.env" >&2
      echo "Copy *.env.example files first." >&2
      exit 1
    fi
    ;;
  stop)
    for instance in $(list_instances); do
      if [[ -f "${ROOT}/run/workers/${instance}.pid" ]]; then
        "${ROOT}/scripts/run-worker.sh" "$instance" --stop || true
      fi
    done
    ;;
  restart)
    found=0
    for instance in $(list_instances); do
      found=1
      if [[ -f "${ROOT}/run/workers/${instance}.pid" ]]; then
        "${ROOT}/scripts/run-worker.sh" "$instance" --stop || true
      fi
    done
    if [[ "$found" -eq 0 ]]; then
      echo "No worker env files found in config/workers/*.env" >&2
      echo "Copy *.env.example files first." >&2
      exit 1
    fi
    echo "Waiting 10s before starting workers..."
    sleep 10
    for instance in $(list_instances); do
      "${ROOT}/scripts/run-worker.sh" "$instance"
    done
    ;;
  status)
    for instance in $(list_instances); do
      pid_file="${ROOT}/run/workers/${instance}.pid"
      log_file="${ROOT}/logs/workers/${instance}.log"
      if [[ -f "$pid_file" ]] && kill -0 "$(cat "$pid_file")" 2>/dev/null; then
        echo "${instance}: running (pid $(cat "$pid_file")), log ${log_file}"
      else
        echo "${instance}: stopped, log ${log_file}"
      fi
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
