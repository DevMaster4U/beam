#!/usr/bin/env bash
# Start one worker instance with its own env file, log, and pid file.
#
# Usage:
#   ./scripts/run-worker.sh worker1              # background
#   ./scripts/run-worker.sh worker1 --foreground # foreground
#   ./scripts/run-worker.sh worker1 --stop       # stop background worker
#   ./scripts/run-worker.sh worker1 --restart    # stop then start
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
INSTANCE="${1:-}"

usage() {
  cat <<EOF
Usage: $0 <instance> [--foreground|-f | --stop | --restart] [worker.py args...]

  <instance>     Name matching config/workers/<instance>.env
  --foreground   Run in the foreground (logs to stdout)
  --stop         Stop a background worker for <instance>
  --restart      Stop if running, then start in the background

Setup:
  cp config/workers/worker1.env.example config/workers/worker1.env
  cp config/workers/worker2.env.example config/workers/worker2.env
  # Edit each .env with a unique WORKER_WALLET_HOTKEY
EOF
}

if [[ -z "$INSTANCE" ]]; then
  usage >&2
  exit 1
fi

shift

FOREGROUND=0
STOP=0
RESTART=0
EXTRA_ARGS=()

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
    -h|--help)
      usage
      exit 0
      ;;
    *)
      EXTRA_ARGS+=("$1")
      shift
      ;;
  esac
done

ENV_FILE="${ROOT}/config/workers/${INSTANCE}.env"
LOG_DIR="${ROOT}/logs/workers"
PID_DIR="${ROOT}/run/workers"
LOG_FILE="${LOG_DIR}/${INSTANCE}.log"
PID_FILE="${PID_DIR}/${INSTANCE}.pid"

mkdir -p "$LOG_DIR" "$PID_DIR"

stop_service() {
  local tolerant="${1:-0}"
  if [[ ! -f "$PID_FILE" ]]; then
    if [[ "$tolerant" -eq 1 ]]; then
      return 0
    fi
    echo "No pid file for worker ${INSTANCE} (${PID_FILE})" >&2
    return 1
  fi

  local pid
  pid="$(cat "$PID_FILE")"
  if kill -0 "$pid" 2>/dev/null; then
    kill "$pid"
    echo "Stopped worker ${INSTANCE} (pid ${pid})"
    for _ in $(seq 1 20); do
      kill -0 "$pid" 2>/dev/null || break
      sleep 0.25
    done
  elif [[ "$tolerant" -eq 0 ]]; then
    echo "Worker ${INSTANCE} not running (stale pid ${pid})" >&2
  fi
  rm -f "$PID_FILE"
  return 0
}

if [[ "$STOP" -eq 1 ]]; then
  stop_service 0
  exit $?
fi

if [[ "$RESTART" -eq 1 && "$FOREGROUND" -eq 1 ]]; then
  echo "Use --restart or --foreground, not both" >&2
  exit 1
fi

if [[ "$RESTART" -eq 1 ]]; then
  stop_service 1
  sleep 1
fi

if [[ ! -f "$ENV_FILE" ]]; then
  echo "Missing env file: ${ENV_FILE}" >&2
  echo "Copy config/workers/${INSTANCE}.env.example and customize it." >&2
  exit 1
fi

PY="${ROOT}/venv/bin/python"
if [[ ! -x "$PY" ]]; then
  PY="python3"
fi

CMD=(
  "$PY" "${ROOT}/neurons/worker/worker.py"
  --env-file "$ENV_FILE"
  "${EXTRA_ARGS[@]}"
)

if [[ "$FOREGROUND" -eq 1 ]]; then
  cd "${ROOT}/neurons/worker"
  exec "${CMD[@]}"
fi

if [[ -f "$PID_FILE" ]] && kill -0 "$(cat "$PID_FILE")" 2>/dev/null; then
  echo "Worker ${INSTANCE} already running (pid $(cat "$PID_FILE"))" >&2
  exit 1
fi

if [[ "$RESTART" -eq 1 ]]; then
  echo "Restarting worker ${INSTANCE}..."
fi

cd "${ROOT}/neurons/worker"
PYTHONUNBUFFERED=1 nohup "${CMD[@]}" >> "$LOG_FILE" 2>&1 &
echo $! > "$PID_FILE"
if [[ "$RESTART" -eq 1 ]]; then
  echo "Restarted worker ${INSTANCE} (pid $(cat "$PID_FILE"))"
else
  echo "Started worker ${INSTANCE} (pid $(cat "$PID_FILE"))"
fi
echo "  env: ${ENV_FILE}"
echo "  log: ${LOG_FILE}"
