#!/usr/bin/env bash
# Start the BEAM orchestrator.
#
# Configuration is loaded from the repo root .env by default (see .env.example).
# Falls back to neurons/orchestrator/.env when the root file is missing.
#
# Usage:
#   ./scripts/run-orchestrator.sh              # background
#   ./scripts/run-orchestrator.sh --foreground # foreground
#   ./scripts/run-orchestrator.sh --stop       # stop background orchestrator
#   ./scripts/run-orchestrator.sh --restart    # stop then start
#   ./scripts/run-orchestrator.sh --status     # show running state
#   ./scripts/run-orchestrator.sh --env-file path/to/.env
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
DEFAULT_ENV_FILE="${ROOT}/.env"
FALLBACK_ENV_FILE="${ROOT}/neurons/orchestrator/.env"
ENV_FILE="${DEFAULT_ENV_FILE}"
LOG_DIR="${ROOT}/logs"
PID_DIR="${ROOT}/run"
LOG_FILE="${LOG_DIR}/orchestrator.log"
PID_FILE="${PID_DIR}/orchestrator.pid"

usage() {
  cat <<EOF
Usage: $0 [--foreground|-f | --stop | --restart | --status | --env-file PATH]

  (default)    Start orchestrator in the background
  --foreground Run in the foreground (logs to stdout)
  --stop       Stop a background orchestrator
  --restart    Stop if running, then start in the background
  --status     Show pid, log path, and API port
  --env-file   Env file to load (default: ${DEFAULT_ENV_FILE})

Configuration:
  Required: ORCH_GATEWAY_URL, WALLET_NAME, WALLET_HOTKEY
  Optional: CORE_SERVER_URL, API_PORT (default 9000), READY, LOG_LEVEL

See docs/orchestrator.md and neurons/orchestrator/README.md.
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

mkdir -p "$LOG_DIR" "$PID_DIR"

stop_service() {
  local tolerant="${1:-0}"
  if [[ ! -f "$PID_FILE" ]]; then
    if [[ "$tolerant" -eq 1 ]]; then
      return 0
    fi
    echo "No pid file for orchestrator (${PID_FILE})" >&2
    return 1
  fi

  local pid
  pid="$(cat "$PID_FILE")"
  if kill -0 "$pid" 2>/dev/null; then
    kill "$pid"
    echo "Stopped orchestrator (pid ${pid})"
    for _ in $(seq 1 20); do
      kill -0 "$pid" 2>/dev/null || break
      sleep 0.25
    done
  elif [[ "$tolerant" -eq 0 ]]; then
    echo "Orchestrator not running (stale pid ${pid})" >&2
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

if [[ "$STATUS" -eq 1 ]]; then
  if [[ -f "$PID_FILE" ]] && kill -0 "$(cat "$PID_FILE")" 2>/dev/null; then
    echo "orchestrator: running (pid $(cat "$PID_FILE"))"
  else
    echo "orchestrator: stopped"
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

load_env_file "$ENV_FILE"
export LOG_DIR

PY="${ROOT}/venv/bin/python"
if [[ ! -x "$PY" ]]; then
  PY="python3"
fi

CMD=("$PY" main.py)

if [[ "$FOREGROUND" -eq 1 ]]; then
  cd "${ROOT}/neurons/orchestrator"
  exec "${CMD[@]}"
fi

if [[ -f "$PID_FILE" ]] && kill -0 "$(cat "$PID_FILE")" 2>/dev/null; then
  echo "Orchestrator already running (pid $(cat "$PID_FILE"))" >&2
  exit 1
fi

if [[ "$RESTART" -eq 1 ]]; then
  echo "Restarting orchestrator..."
fi

cd "${ROOT}/neurons/orchestrator"
PYTHONUNBUFFERED=1 nohup "${CMD[@]}" >> "$LOG_FILE" 2>&1 &
echo $! > "$PID_FILE"
if [[ "$RESTART" -eq 1 ]]; then
  echo "Restarted orchestrator (pid $(cat "$PID_FILE"))"
else
  echo "Started orchestrator (pid $(cat "$PID_FILE"))"
fi
echo "  env: ${ENV_FILE}"
echo "  log: ${LOG_FILE}"
port="${API_PORT:-9000}"
echo "  api: http://127.0.0.1:${port}"
