#!/usr/bin/env bash
# Manage one orchestrator instance via systemd (beam-orchestrator@<instance>.service).
#
# First-time setup (AWS EC2 / Ubuntu):
#   ./scripts/setup-ec2.sh
#   sudo ./scripts/install-systemd.sh --enable
#   sudo ./scripts/install-systemd.sh --enable-orchestrators
#
# Usage:
#   ./scripts/run-orchestrator.sh orch1
#   ./scripts/run-orchestrator.sh orch1 --foreground
#   ./scripts/run-orchestrator.sh orch1 --stop
#   ./scripts/run-orchestrator.sh orch1 --restart
#   ./scripts/run-orchestrator.sh orch1 --status
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BEAM_ROOT="$ROOT"
# shellcheck disable=SC1091
source "${ROOT}/scripts/lib/systemd.sh"

INSTANCE="${1:-}"

usage() {
  cat <<EOF
Usage: $0 <instance> [--foreground|-f | --stop | --restart | --status | --clear-affinity]

  <instance>         Name matching config/orchestrators/<instance>.env
  --foreground       Run in the foreground (debug; stops systemd instance first)
  --stop             Stop the orchestrator via systemd
  --restart          Restart the orchestrator via systemd
  --status           Show systemd status, log path, and API port
  --clear-affinity   Wipe dest×worker Mbps EMA on start (also: POST /workers/affinity/clear)

Install units once with:
  ./scripts/install-systemd.sh --enable
  ./scripts/install-systemd.sh --enable-orchestrators

Setup:
  cp config/orchestrators/orch1.env.example config/orchestrators/orch1.env

See docs/orchestrator.md.
EOF
}

if [[ -z "$INSTANCE" || "$INSTANCE" == "-h" || "$INSTANCE" == "--help" ]]; then
  usage >&2
  exit 1
fi

shift

FOREGROUND=0
STOP=0
RESTART=0
STATUS=0
CLEAR_AFFINITY=0

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
    --clear-affinity|--clear-dest-affinity)
      CLEAR_AFFINITY=1
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

ENV_FILE="${ROOT}/config/orchestrators/${INSTANCE}.env"
LOG_FILE="${ROOT}/logs/orchestrators/${INSTANCE}.log"
SERVICE="beam-orchestrator@${INSTANCE}.service"

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
    echo "  install: ${ROOT}/scripts/install-systemd.sh --enable-orchestrators"
  fi
  echo "  env: ${ENV_FILE}"
  echo "  log: ${LOG_FILE}"
  if [[ -f "$LOG_FILE" ]]; then
    echo "  log_bytes: $(wc -c < "$LOG_FILE" | tr -d ' ')"
  else
    echo "  log_bytes: 0 (file missing — service may have failed before logging started)"
    echo "  journal: journalctl -u ${SERVICE} -n 50 --no-pager"
  fi
  port="9000"
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
  echo "Missing env file: ${ENV_FILE}" >&2
  echo "Copy config/orchestrators/${INSTANCE}.env.example and customize it." >&2
  exit 1
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

beam_validate_orchestrator_configs
beam_validate_orchestrator_gateway_env "$ENV_FILE"

PY="$(beam_python)"
CMD=(
  "$PY" main.py
  --env-file "$ENV_FILE"
)
if [[ "$CLEAR_AFFINITY" -eq 1 ]]; then
  CMD+=(--clear-affinity)
  # systemd unit does not forward CLI flags — export for Environment= pick-up when
  # operators add ORCH_DEST_AFFINITY_CLEAR_ON_START; foreground uses argv above.
  export ORCH_DEST_AFFINITY_CLEAR_ON_START=true
fi

# Foreground and systemd must not bind the same API port / BeamCore session.
if [[ "$FOREGROUND" -eq 1 ]]; then
  if beam_unit_installed "$SERVICE" && beam_systemctl is-active --quiet "$SERVICE"; then
    echo "Stopping ${SERVICE} so foreground is the only orchestrator session..."
    beam_systemctl stop "$SERVICE" || true
  fi
  if [[ "${EUID}" -eq 0 ]]; then
    echo "Warning: foreground is running as root." >&2
    echo "  Prefer: sudo -u ubuntu ./scripts/run-orchestrator.sh ${INSTANCE} --foreground" >&2
    echo "  Root uses /root/.bittensor wallets and can break the ubuntu systemd unit." >&2
  fi
  export LOG_DIR="${ROOT}/logs"
  cd "${ROOT}/neurons/orchestrator"
  exec "${CMD[@]}"
fi

beam_ensure_orchestrator_instance "$INSTANCE"

if [[ "$CLEAR_AFFINITY" -eq 1 ]]; then
  # Systemd unit does not forward --clear-affinity; delete JSON before start, then
  # POST /workers/affinity/clear after ready (covers in-memory + optional CSV seed).
  stats_path="$(grep -E '^ORCH_DEST_AFFINITY_STATS_PATH=' "$ENV_FILE" 2>/dev/null | tail -n1 | cut -d= -f2- || true)"
  if [[ -z "$stats_path" ]]; then
    stats_path="${ROOT}/logs/orchestrators/dest_worker_stats.json"
  elif [[ "$stats_path" != /* ]]; then
    stats_path="${ROOT}/${stats_path}"
  fi
  if [[ -f "$stats_path" ]]; then
    rm -f "$stats_path"
    echo "Cleared dest affinity stats file: ${stats_path}"
  else
    echo "Dest affinity stats file already empty: ${stats_path}"
  fi
fi

if [[ "$RESTART" -eq 1 ]]; then
  beam_systemctl restart "$SERVICE"
  echo "Restarted orchestrator ${INSTANCE}"
else
  beam_systemctl start "$SERVICE"
  echo "Started orchestrator ${INSTANCE}"
fi

echo "  service: ${SERVICE}"
echo "  env: ${ENV_FILE}"
echo "  log: ${LOG_FILE}"

sleep 1
if beam_systemctl is-active --quiet "$SERVICE"; then
  echo "  state: active"
else
  echo "  state: NOT active — check:" >&2
  echo "    systemctl status ${SERVICE} --no-pager" >&2
  echo "    journalctl -u ${SERVICE} -n 80 --no-pager" >&2
  echo "    tail -n 80 ${LOG_FILE}" >&2
  beam_systemctl status "$SERVICE" --no-pager || true
  exit 1
fi

if [[ -f "$LOG_FILE" ]]; then
  echo "  log_bytes: $(wc -c < "$LOG_FILE" | tr -d ' ')"
fi
port="9000"
if [[ -f "$ENV_FILE" ]]; then
  env_port="$(grep -E '^API_PORT=' "$ENV_FILE" | tail -n1 | cut -d= -f2- || true)"
  if [[ -n "$env_port" ]]; then
    port="$env_port"
  fi
fi
echo "  api: http://127.0.0.1:${port}"

if [[ "$CLEAR_AFFINITY" -eq 1 ]]; then
  # Best-effort runtime clear once API is up (in-memory rows / CSV re-seed).
  for _ in 1 2 3 4 5 6 7 8 9 10; do
    if curl -fsS -X POST "http://127.0.0.1:${port}/workers/affinity/clear" >/dev/null 2>&1; then
      echo "  affinity: cleared via POST /workers/affinity/clear"
      break
    fi
    sleep 1
  done
fi
