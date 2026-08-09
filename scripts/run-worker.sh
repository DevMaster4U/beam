#!/usr/bin/env bash
# Manage one worker instance via systemd (beam-worker@<instance>.service).
#
# First-time setup (Ubuntu worker host):
#   ./scripts/setup-worker-host.sh --create-wallet --write-env --install-systemd
# Or EC2 / multi-role host:
#   ./scripts/setup-ec2.sh
#   sudo ./scripts/install-systemd.sh --enable
#   sudo ./scripts/install-systemd.sh --enable-workers
#
# Usage:
#   ./scripts/run-worker.sh worker1
#   ./scripts/run-worker.sh worker1 --foreground
#   ./scripts/run-worker.sh worker1 --stop
#   ./scripts/run-worker.sh worker1 --restart
#   ./scripts/run-worker.sh worker1 --status
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BEAM_ROOT="$ROOT"
# shellcheck disable=SC1091
source "${ROOT}/scripts/lib/systemd.sh"

INSTANCE="${1:-}"

usage() {
  cat <<EOF
Usage: $0 <instance> [--foreground|-f | --stop | --restart | --status] [worker.py args...]

  <instance>     Name matching config/workers/<instance>.env
  --foreground   Run in the foreground (debug; bypasses systemd)
  --stop         Stop the worker via systemd
  --restart      Restart the worker via systemd
  --status       Show systemd status and log path

Install units once with:
  ./scripts/install-systemd.sh --enable
  ./scripts/install-systemd.sh --enable-workers

Setup:
  cp config/workers/worker1.env.example config/workers/worker1.env
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
STATUS=0
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
    --status)
      STATUS=1
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
LOG_FILE="${ROOT}/logs/workers/${INSTANCE}.log"
SERVICE="beam-worker@${INSTANCE}.service"

beam_prepare_data_dirs

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
    echo "  install: ${ROOT}/scripts/install-systemd.sh --enable-workers"
  fi
  echo "  env: ${ENV_FILE}"
  echo "  log: ${LOG_FILE}"
  exit 0
fi

if [[ ! -f "$ENV_FILE" ]]; then
  echo "Missing env file: ${ENV_FILE}" >&2
  echo "Copy config/workers/${INSTANCE}.env.example and customize it." >&2
  exit 1
fi

beam_validate_worker_gateway_env "$ENV_FILE"

PY="$(beam_python)"
# Simple/hidden workers use slim entrypoint (never imports bittensor).
WORKER_ENTRY="${ROOT}/neurons/worker/worker.py"
if grep -Eq '^[[:space:]]*WORKER_HIDDEN[[:space:]]*=[[:space:]]*(true|1|yes)' "$ENV_FILE"; then
  WORKER_ENTRY="${ROOT}/neurons/worker/simple_worker.py"
fi
CMD=(
  "$PY" "$WORKER_ENTRY"
  --env-file "$ENV_FILE"
  "${EXTRA_ARGS[@]}"
)

if [[ "$FOREGROUND" -eq 1 ]]; then
  cd "${ROOT}/neurons/worker"
  exec "${CMD[@]}"
fi

beam_ensure_worker_instance "$INSTANCE"

if [[ "$RESTART" -eq 1 ]]; then
  beam_systemctl restart "$SERVICE"
  echo "Restarted worker ${INSTANCE}"
else
  beam_systemctl start "$SERVICE"
  echo "Started worker ${INSTANCE}"
fi

echo "  service: ${SERVICE}"
echo "  env: ${ENV_FILE}"
echo "  log: ${LOG_FILE}"
