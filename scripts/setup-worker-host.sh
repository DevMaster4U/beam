#!/usr/bin/env bash
# First-time worker host setup (Ubuntu/Debian).
#
# Automates the common flow:
#   apt packages → venv → pip install → optional wallet → worker.env → systemd
#
# Prerequisites (outside this script):
#   git clone https://github.com/DevMaster4U/beam.git sn105
#   cd sn105 && git pull && git checkout controller
#
# Usage:
#   ./scripts/setup-worker-host.sh
#   ./scripts/setup-worker-host.sh --create-wallet --write-env --install-systemd
#   ./scripts/setup-worker-host.sh \
#     --wallet-name sn105_w --wallet-hotkey sn105_w1 \
#     --gateway-url ws://88.216.68.26:9005 --gateway-secret wgs \
#     --write-env --install-systemd
#
# Then edit config/workers/worker1.env if needed and start:
#   ./scripts/run-worker.sh worker1
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BEAM_ROOT="$ROOT"
# shellcheck disable=SC1091
source "${ROOT}/scripts/lib/systemd.sh"

SKIP_APT=0
SKIP_PIP=0
CREATE_WALLET=0
WRITE_ENV=0
FORCE_ENV=0
INSTALL_SYSTEMD=0

WALLET_NAME="${WORKER_WALLET_NAME:-sn105_w}"
WALLET_HOTKEY="${WORKER_WALLET_HOTKEY:-sn105_w1}"
WALLET_PATH="${WALLET_PATH:-${HOME}/.bittensor/wallets}"
BTCLI_VERSION="${BTCLI_VERSION:-9.23.1}"
WORKER_INSTANCE="${WORKER_INSTANCE:-worker1}"

GATEWAY_URL="${WORKER_GATEWAY_URL:-ws://88.216.68.26:9005}"
GATEWAY_SECRET="${WORKER_GATEWAY_SECRET:-wgs}"
CONTROL_WS_URL="${CONTROL_SERVER_WS_URL:-ws://84.32.220.100:8010/ws/miners}"
CONTROL_SECRET="${CONTROL_SERVER_SECRET:-css}"
CONTROL_MINER_ID="${CONTROL_SERVER_MINER_ID:-}"
REQUIRED_PAYMENT="${WORKER_REQUIRED_PAYMENT:-false}"
MAX_CONCURRENT="${WORKER_MAX_CONCURRENT_TASKS:-2}"
MAX_QUEUED="${WORKER_MAX_QUEUED_WS_TASKS:-2}"
MAX_IN_FLIGHT="${WORKER_MAX_IN_FLIGHT_BYTES:-6710886400}"
INITIAL_ORDER="${WORKER_INITIAL_ORDER:-10}"

usage() {
  cat <<EOF
Usage: $0 [options]

Environment install for a Beam worker host.

Options:
  --skip-apt              Skip apt-get package install
  --skip-pip              Skip venv / pip install
  --create-wallet         Create coldkey+hotkey via btcli (no password)
  --wallet-name NAME      Coldkey name (default: sn105_w)
  --wallet-hotkey NAME    Hotkey name (default: sn105_w1)
  --wallet-path PATH      Wallet dir (default: ~/.bittensor/wallets)
  --btcli-version VER     bittensor-cli version (default: 9.23.1)
  --instance NAME         Worker env instance (default: worker1)
  --gateway-url URL       WORKER_GATEWAY_URL
  --gateway-secret SEC    WORKER_GATEWAY_SECRET
  --control-ws URL        CONTROL_SERVER_WS_URL
  --control-secret SEC    CONTROL_SERVER_SECRET
  --miner-id ID           CONTROL_SERVER_MINER_ID (default: worker_<ip_last>_1)
  --write-env             Write config/workers/<instance>.env if missing
  --force-env             Overwrite existing worker env
  --install-systemd       Run install-systemd.sh --enable-workers
  -h, --help              Show this help

Examples:
  $0 --create-wallet --write-env --install-systemd
  $0 --wallet-name sn105_w --wallet-hotkey sn105_w1 \\
     --gateway-url ws://88.216.68.26:9005 --write-env
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --skip-apt) SKIP_APT=1; shift ;;
    --skip-pip) SKIP_PIP=1; shift ;;
    --create-wallet) CREATE_WALLET=1; shift ;;
    --wallet-name) WALLET_NAME="${2:?}"; shift 2 ;;
    --wallet-hotkey) WALLET_HOTKEY="${2:?}"; shift 2 ;;
    --wallet-path) WALLET_PATH="${2:?}"; shift 2 ;;
    --btcli-version) BTCLI_VERSION="${2:?}"; shift 2 ;;
    --instance) WORKER_INSTANCE="${2:?}"; shift 2 ;;
    --gateway-url) GATEWAY_URL="${2:?}"; shift 2 ;;
    --gateway-secret) GATEWAY_SECRET="${2:?}"; shift 2 ;;
    --control-ws) CONTROL_WS_URL="${2:?}"; shift 2 ;;
    --control-secret) CONTROL_SECRET="${2:?}"; shift 2 ;;
    --miner-id) CONTROL_MINER_ID="${2:?}"; shift 2 ;;
    --write-env) WRITE_ENV=1; shift ;;
    --force-env) FORCE_ENV=1; WRITE_ENV=1; shift ;;
    --install-systemd) INSTALL_SYSTEMD=1; shift ;;
    -h|--help) usage; exit 0 ;;
    *)
      echo "Unknown option: $1" >&2
      usage >&2
      exit 1
      ;;
  esac
done

WALLET_PATH="${WALLET_PATH/#\~/${HOME}}"

detect_public_ip_last_octet() {
  local ip=""
  if command -v curl >/dev/null 2>&1; then
    ip="$(curl -4 -fsS --max-time 5 https://api.ipify.org 2>/dev/null || true)"
  fi
  if [[ -z "$ip" ]] && command -v hostname >/dev/null 2>&1; then
    ip="$(hostname -I 2>/dev/null | awk '{print $1}')"
  fi
  if [[ "$ip" =~ ^([0-9]+\.){3}([0-9]+)$ ]]; then
    printf '%s\n' "${BASH_REMATCH[2]}"
    return 0
  fi
  printf '0\n'
}

if [[ -z "$CONTROL_MINER_ID" ]]; then
  CONTROL_MINER_ID="worker_$(detect_public_ip_last_octet)_1"
fi

echo "=== BEAM worker host setup ==="
echo "  repo:     ${ROOT}"
echo "  user:     $(id -un)"
echo "  instance: ${WORKER_INSTANCE}"
echo "  wallet:   ${WALLET_NAME}/${WALLET_HOTKEY}"
echo "  gateway:  ${GATEWAY_URL}"
echo "  miner_id: ${CONTROL_MINER_ID}"
echo

if [[ "$SKIP_APT" -eq 0 ]]; then
  if ! command -v apt-get >/dev/null 2>&1; then
    echo "apt-get not found; use --skip-apt on non-Debian hosts." >&2
    exit 1
  fi
  echo "Installing OS packages..."
  if [[ "$(id -u)" -eq 0 ]]; then
    apt-get update
    DEBIAN_FRONTEND=noninteractive apt-get install -y \
      python3 python3-venv python3-pip python3-dev \
      git build-essential curl jq
  else
    sudo apt-get update
    sudo DEBIAN_FRONTEND=noninteractive apt-get install -y \
      python3 python3-venv python3-pip python3-dev \
      git build-essential curl jq
  fi
fi

VENV="${ROOT}/venv"
if [[ "$SKIP_PIP" -eq 0 ]]; then
  if [[ ! -x "${VENV}/bin/python" ]]; then
    echo "Creating virtualenv at ${VENV}..."
    python3 -m venv "${VENV}"
  fi
  echo "Installing Python dependencies..."
  "${VENV}/bin/pip" install -U pip wheel setuptools
  if [[ -f "${ROOT}/requirements.txt" ]]; then
    "${VENV}/bin/pip" install -r "${ROOT}/requirements.txt"
  fi
  "${VENV}/bin/pip" install -e "${ROOT}"
fi

if [[ "$CREATE_WALLET" -eq 1 ]]; then
  echo "Installing bittensor-cli==${BTCLI_VERSION} (user/system pip)..."
  python3 -m pip install --user "bittensor-cli==${BTCLI_VERSION}" || \
    python3 -m pip install "bittensor-cli==${BTCLI_VERSION}"

  export PATH="${HOME}/.local/bin:${PATH}"
  if ! command -v btcli >/dev/null 2>&1; then
    echo "btcli not found on PATH after install." >&2
    exit 1
  fi

  COLDKEY_FILE="${WALLET_PATH}/${WALLET_NAME}/coldkey"
  HOTKEY_FILE="${WALLET_PATH}/${WALLET_NAME}/hotkeys/${WALLET_HOTKEY}"
  mkdir -p "${WALLET_PATH}"

  if [[ -f "$COLDKEY_FILE" ]]; then
    echo "Coldkey already exists: ${COLDKEY_FILE} (skipping new-coldkey)"
  else
    echo "Creating coldkey ${WALLET_NAME}..."
    btcli w new-coldkey --wallet-name "${WALLET_NAME}" --wallet-path "${WALLET_PATH}" \
      --n-words 12 --no-use-password
  fi

  if [[ -f "$HOTKEY_FILE" ]]; then
    echo "Hotkey already exists: ${HOTKEY_FILE} (skipping new-hotkey)"
  else
    echo "Creating hotkey ${WALLET_HOTKEY}..."
    btcli w new-hotkey --wallet-name "${WALLET_NAME}" --wallet-path "${WALLET_PATH}" \
      --hotkey "${WALLET_HOTKEY}" --n-words 12 --no-use-password
  fi

  if [[ -x "${VENV}/bin/python" ]]; then
    echo -n "  hotkey ss58: "
    "${VENV}/bin/python" - <<EOF
import bittensor as bt
w = bt.Wallet(name="${WALLET_NAME}", hotkey="${WALLET_HOTKEY}", path="${WALLET_PATH}")
print(w.hotkey.ss58_address)
EOF
  fi
fi

ENV_FILE="${ROOT}/config/workers/${WORKER_INSTANCE}.env"
if [[ "$WRITE_ENV" -eq 1 ]]; then
  if [[ -f "$ENV_FILE" && "$FORCE_ENV" -eq 0 ]]; then
    echo "Worker env already exists (use --force-env to overwrite): ${ENV_FILE}"
  else
    mkdir -p "$(dirname "$ENV_FILE")"
    cat >"$ENV_FILE" <<EOF
# Generated by scripts/setup-worker-host.sh
# Run: ./scripts/run-worker.sh ${WORKER_INSTANCE}

WORKER_WALLET_NAME=${WALLET_NAME}
WORKER_WALLET_HOTKEY=${WALLET_HOTKEY}
# WALLET_PATH=${WALLET_PATH}

# Split machine resources across local workers.
WORKER_MAX_CONCURRENT_TASKS=${MAX_CONCURRENT}
WORKER_MAX_QUEUED_WS_TASKS=${MAX_QUEUED}
WORKER_MAX_IN_FLIGHT_BYTES=${MAX_IN_FLIGHT}

# WORKER_EARLY_TRANSFER=true
# Must stay below BeamCore offer TTL (~5s). Fail fast on hung accept relay.
WORKER_TASK_ACCEPT_ACK_TIMEOUT=8.0
# Must exceed orchestrator ORCH_TASK_RESULT_TIMEOUT (30s) plus gateway relay margin.
WORKER_TASK_RESULT_ACK_TIMEOUT=45.0

# Set false when running on your orchestrator's dedicated gateway.
WORKER_REQUIRED_PAYMENT=${REQUIRED_PAYMENT}

WORKER_GATEWAY_URL=${GATEWAY_URL}
WORKER_GATEWAY_SECRET=${GATEWAY_SECRET}

WORKER_INITIAL_ORDER=${INITIAL_ORDER}

WORKER_PREDEFINED_ETAG_EARLY_SUBMIT=false
WORKER_PREDEFINED_ETAG_MAX_PARALLEL=1
WORKER_PREDEFINED_ETAG_MAX_SPEED_MBPS=0
CONTROL_SERVER_WS_URL=${CONTROL_WS_URL}
CONTROL_SERVER_SECRET=${CONTROL_SECRET}
CONTROL_SERVER_MINER_ID=${CONTROL_MINER_ID}
CONTROL_SERVER_CACHE_SYNC_DELAY_SEC=60
WORKER_VERIFY_CHUNK_HASH=false
WORKER_USE_CACHE_FILE=true
EOF
    echo "Wrote ${ENV_FILE}"
  fi
fi

if [[ "$INSTALL_SYSTEMD" -eq 1 ]]; then
  echo "Installing worker systemd units..."
  if [[ "$(id -u)" -eq 0 ]]; then
    "${ROOT}/scripts/install-systemd.sh" --enable --enable-workers --instances "${WORKER_INSTANCE}"
  else
    sudo "${ROOT}/scripts/install-systemd.sh" --enable --enable-workers --instances "${WORKER_INSTANCE}"
  fi
fi

beam_prepare_data_dirs 2>/dev/null || true
beam_prepare_repo_permissions 2>/dev/null || true

echo
echo "Setup complete."
echo
echo "Next steps:"
if [[ ! -f "$ENV_FILE" ]]; then
  echo "  1. Write worker env:"
  echo "       $0 --write-env --wallet-name ${WALLET_NAME} --wallet-hotkey ${WALLET_HOTKEY} \\"
  echo "          --gateway-url ${GATEWAY_URL}"
else
  echo "  1. Review ${ENV_FILE}"
fi
echo "  2. Ensure hotkey is registered on subnet 105"
if [[ "$INSTALL_SYSTEMD" -eq 0 ]]; then
  echo "  3. Install systemd:"
  echo "       sudo ./scripts/install-systemd.sh --enable --enable-workers"
fi
echo "  4. Start worker:"
echo "       ./scripts/run-worker.sh ${WORKER_INSTANCE}"
echo "       # or foreground: ./scripts/run-worker.sh ${WORKER_INSTANCE} --foreground"
echo
echo "Wallets: ${WALLET_PATH}"
