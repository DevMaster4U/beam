#!/usr/bin/env bash
# Start the dedicated worker-gateway (Option 1).
#
# Configuration is read from the repo root .env (see worker-gateway/gateway/config.py).
#
# Usage:
#   ./scripts/run-worker-gateway.sh              # background
#   ./scripts/run-worker-gateway.sh --foreground # foreground
#   ./scripts/run-worker-gateway.sh --stop       # stop background gateway
#   ./scripts/run-worker-gateway.sh --status     # show running state
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
ENV_FILE="${ROOT}/.env"
LOG_DIR="${ROOT}/logs"
PID_DIR="${ROOT}/run"
LOG_FILE="${LOG_DIR}/worker-gateway.log"
PID_FILE="${PID_DIR}/worker-gateway.pid"

usage() {
  cat <<EOF
Usage: $0 [--foreground|-f | --stop | --status]

  (default)    Start worker-gateway in the background
  --foreground Run in the foreground (logs to stdout)
  --stop       Stop a background worker-gateway
  --status     Show pid, log path, and listening port

Configuration:
  ${ENV_FILE}
  Required: GATEWAY_CONTROL_SECRET or WORKER_GATEWAY_CONTROL_SECRET
            GATEWAY_WORKER_SECRET or WORKER_GATEWAY_WORKER_SECRET
  Optional: GATEWAY_HOST, GATEWAY_PORT (default 8001), LOG_LEVEL

See docs/worker-gateway.md and worker-gateway/README.md.
EOF
}

FOREGROUND=0
STOP=0
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

mkdir -p "$LOG_DIR" "$PID_DIR"

if [[ "$STOP" -eq 1 ]]; then
  if [[ ! -f "$PID_FILE" ]]; then
    echo "No pid file for worker-gateway (${PID_FILE})" >&2
    exit 1
  fi
  PID="$(cat "$PID_FILE")"
  if kill -0 "$PID" 2>/dev/null; then
    kill "$PID"
    echo "Stopped worker-gateway (pid ${PID})"
  else
    echo "Worker-gateway not running (stale pid ${PID})" >&2
  fi
  rm -f "$PID_FILE"
  exit 0
fi

if [[ "$STATUS" -eq 1 ]]; then
  if [[ -f "$PID_FILE" ]] && kill -0 "$(cat "$PID_FILE")" 2>/dev/null; then
    echo "worker-gateway: running (pid $(cat "$PID_FILE"))"
  else
    echo "worker-gateway: stopped"
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

PY="${ROOT}/venv/bin/python"
if [[ ! -x "$PY" ]]; then
  PY="python3"
fi

CMD=("$PY" "${ROOT}/worker-gateway/main.py")

if [[ "$FOREGROUND" -eq 1 ]]; then
  cd "${ROOT}/worker-gateway"
  exec "${CMD[@]}"
fi

if [[ -f "$PID_FILE" ]] && kill -0 "$(cat "$PID_FILE")" 2>/dev/null; then
  echo "Worker-gateway already running (pid $(cat "$PID_FILE"))" >&2
  exit 1
fi

cd "${ROOT}/worker-gateway"
PYTHONUNBUFFERED=1 nohup "${CMD[@]}" >> "$LOG_FILE" 2>&1 &
echo $! > "$PID_FILE"
echo "Started worker-gateway (pid $(cat "$PID_FILE"))"
echo "  env: ${ENV_FILE}"
echo "  log: ${LOG_FILE}"
