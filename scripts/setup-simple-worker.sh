#!/usr/bin/env bash
# Setup a simple/hidden worker in an existing checkout (transfer + cache only).
#
# Download/checkout the repo first, then run from the tree:
#   git clone -b simple-worker https://github.com/DevMaster4U/beam.git ~/sn105
#   cd ~/sn105
#   ./scripts/setup-simple-worker.sh --server ec2 \
#       --gateway ws://3.21.114.106:8020 \
#       --control-server ws://88.216.195.66:8010/ws/miners \
#       --miner_id worker_h1 \
#       --worker_instance worker_h1 \
#       --start
#
# Defaults: --gateway-secret wgs, --control-secret css
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

SERVER="ec2"
GATEWAY=""
GATEWAY_SECRET="wgs"
CONTROL_SERVER=""
CONTROL_SECRET="css"
MINER_ID=""
WORKER_INSTANCE=""
START=0
FOREGROUND=0
INSTALL_SYSTEMD=1

usage() {
  cat <<EOF
Usage: $0 [options]

Run from a downloaded beam/sn105 checkout (this script does not clone).

  --server TYPE              Host profile: ec2 | local  (default: ec2)
  --gateway URL              Orch worker gateway (http:// or ws:// host:port)
  --gateway-secret SECRET    WORKER_GATEWAY_SECRET (default: wgs)
  --control-server URL       Control-server WS (ws://host:8010/ws/miners)
  --control-secret SECRET    CONTROL_SERVER_SECRET (default: css)
  --miner_id ID              CONTROL_SERVER_MINER_ID (default: worker_instance)
  --worker_instance NAME     config/workers/<NAME>.env + systemd instance
  --start                    Start via systemd after setup
  --foreground               Run worker in foreground after setup
  --no-systemd               Skip systemd unit install
  -h, --help                 Show this help

Example:
  git clone -b simple-worker https://github.com/DevMaster4U/beam.git ~/sn105
  cd ~/sn105
  $0 --server ec2 \\
      --gateway ws://3.21.114.106:8020 \\
      --control-server ws://88.216.195.66:8010/ws/miners \\
      --miner_id worker_h1 \\
      --worker_instance worker_h1 \\
      --start
EOF
}

die() { echo "Error: $*" >&2; exit 1; }

while [[ $# -gt 0 ]]; do
  case "$1" in
    --server)
      SERVER="${2:-}"; shift 2 ;;
    --gateway)
      GATEWAY="${2:-}"; shift 2 ;;
    --gateway-secret|--gateway_secret)
      GATEWAY_SECRET="${2:-}"; shift 2 ;;
    --control-server|--control_server)
      CONTROL_SERVER="${2:-}"; shift 2 ;;
    --control-secret|--control_secret)
      CONTROL_SECRET="${2:-}"; shift 2 ;;
    --miner_id|--miner-id)
      MINER_ID="${2:-}"; shift 2 ;;
    --worker_instance|--worker-instance|--instance)
      WORKER_INSTANCE="${2:-}"; shift 2 ;;
    --start)
      START=1; shift ;;
    --foreground|-f)
      FOREGROUND=1; shift ;;
    --no-systemd)
      INSTALL_SYSTEMD=0; shift ;;
    # Removed; keep friendly errors if old flags are passed.
    --source|--dir|--branch)
      die "$1 removed: clone/checkout the repo yourself first, then re-run this script"
      ;;
    -h|--help)
      usage; exit 0 ;;
    *)
      die "unknown option: $1 (see --help)"
      ;;
  esac
done

[[ -n "$GATEWAY" ]] || die "--gateway is required"
[[ -n "$CONTROL_SERVER" ]] || die "--control-server is required"
[[ -n "$WORKER_INSTANCE" ]] || die "--worker_instance is required"
MINER_ID="${MINER_ID:-$WORKER_INSTANCE}"

case "$SERVER" in
  ec2|local) ;;
  *) die "--server must be ec2 or local" ;;
esac

[[ -f "${ROOT}/neurons/worker/simple_worker.py" ]] \
  || die "missing neurons/worker/simple_worker.py under ${ROOT} (download the repo first)"

# Normalize gateway to http(s) origin for WORKER_GATEWAY_URL.
normalize_gateway() {
  local url="$1"
  url="${url%/}"
  case "$url" in
    ws://*) url="http://${url#ws://}" ;;
    wss://*) url="https://${url#wss://}" ;;
  esac
  url="${url%%/ws*}"
  printf '%s' "$url"
}

# Ensure control WS ends with /ws/miners.
normalize_control() {
  local url="$1"
  url="${url%/}"
  case "$url" in
    http://*) url="ws://${url#http://}" ;;
    https://*) url="wss://${url#https://}" ;;
  esac
  if [[ "$url" != */ws/miners ]]; then
    url="${url%/}/ws/miners"
  fi
  printf '%s' "$url"
}

GATEWAY_URL="$(normalize_gateway "$GATEWAY")"
CONTROL_WS="$(normalize_control "$CONTROL_SERVER")"

install_os_packages() {
  if [[ "$SERVER" != "ec2" ]]; then
    echo "Skipping OS packages (--server local)"
    return 0
  fi
  command -v apt-get >/dev/null 2>&1 || die "ec2 profile expects apt-get (Ubuntu/Debian)"
  echo "Installing OS packages..."
  sudo apt-get update
  sudo DEBIAN_FRONTEND=noninteractive apt-get install -y \
    python3 python3-venv python3-pip curl
}

setup_venv() {
  local venv="${ROOT}/.venv"
  if [[ -x "${ROOT}/venv/bin/python" ]]; then
    venv="${ROOT}/venv"
  elif [[ ! -x "${venv}/bin/python" ]]; then
    echo "Creating virtualenv at ${venv}..."
    python3 -m venv "$venv"
  fi
  echo "Installing simple-worker deps..."
  "${venv}/bin/pip" install -U pip
  "${venv}/bin/pip" install -r "${ROOT}/neurons/worker/requirements-simple.txt"
}

write_env() {
  local env_file="${ROOT}/config/workers/${WORKER_INSTANCE}.env"
  mkdir -p "${ROOT}/config/workers" "${ROOT}/logs/workers"
  cat >"$env_file" <<EOF
# Generated by scripts/setup-simple-worker.sh
WORKER_HIDDEN=true
WORKER_INSTANCE=${WORKER_INSTANCE}

# Orchestrator gateway
WORKER_GATEWAY_URL=${GATEWAY_URL}
WORKER_GATEWAY_SECRET=${GATEWAY_SECRET}

# Control-server cache
CONTROL_SERVER_WS_URL=${CONTROL_WS}
CONTROL_SERVER_SECRET=${CONTROL_SECRET}
CONTROL_SERVER_MINER_ID=${MINER_ID}
EOF
  echo "Wrote ${env_file}"
}

echo "=== BEAM simple-worker setup ==="
echo "  repo:            ${ROOT}"
echo "  server:          ${SERVER}"
echo "  gateway:         ${GATEWAY_URL}"
echo "  control:         ${CONTROL_WS}"
echo "  worker_instance: ${WORKER_INSTANCE}"
echo "  miner_id:        ${MINER_ID}"
echo

install_os_packages
setup_venv
write_env

if [[ "$INSTALL_SYSTEMD" -eq 1 ]]; then
  if [[ -x "${ROOT}/scripts/install-systemd.sh" ]]; then
    echo "Installing systemd worker units..."
    sudo "${ROOT}/scripts/install-systemd.sh" --enable-workers \
      --instances "${WORKER_INSTANCE}" || {
      echo "Warning: systemd install failed; you can still run --foreground" >&2
    }
  else
    echo "Warning: install-systemd.sh missing; skip systemd" >&2
  fi
fi

echo
echo "Setup complete."
echo "  env: ${ROOT}/config/workers/${WORKER_INSTANCE}.env"
echo

if [[ "$FOREGROUND" -eq 1 ]]; then
  exec "${ROOT}/scripts/run-worker.sh" "${WORKER_INSTANCE}" --foreground
fi

if [[ "$START" -eq 1 ]]; then
  "${ROOT}/scripts/run-worker.sh" "${WORKER_INSTANCE}"
  echo "Started. Logs: ${ROOT}/logs/workers/${WORKER_INSTANCE}.log"
  echo "  status: ${ROOT}/scripts/run-worker.sh ${WORKER_INSTANCE} --status"
  exit 0
fi

echo "Next:"
echo "  ${ROOT}/scripts/run-worker.sh ${WORKER_INSTANCE} --foreground"
echo "  # or"
echo "  ${ROOT}/scripts/run-worker.sh ${WORKER_INSTANCE}"
echo "  ${ROOT}/scripts/run-worker.sh ${WORKER_INSTANCE} --status"
