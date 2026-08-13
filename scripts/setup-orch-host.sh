#!/usr/bin/env bash
# First-time orchestrator host setup (Ubuntu/Debian — EC2 or normal VPS).
#
# Role script (orch). For host packages/venv only, use setup-ec2.sh first
# (optional — this script can also install apt/.venv).
#
# Prerequisites:
#   git clone … sn105 && cd sn105 && git checkout <branch>
#
# Usage:
#   ./scripts/setup-orch-host.sh \
#     --create-wallet --wallet-name orchestrator --wallet-hotkey orch1 \
#     --api-port 9005 \
#     --gateway-url http://YOUR_PUBLIC_IP:9005 \
#     --gateway-secret wgs \
#     --write-env --install-systemd
#
# Then:
#   ./scripts/register-orchestrator.sh orch1 --write-env   # BeamCore API key
#   ./scripts/run-orchestrator.sh orch1
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

WALLET_NAME="${WALLET_NAME:-orchestrator}"
WALLET_HOTKEY="${WALLET_HOTKEY:-orch1}"
WALLET_PATH="${WALLET_PATH:-${HOME}/.bittensor/wallets}"
BTCLI_VERSION="${BTCLI_VERSION:-9.23.1}"
ORCH_INSTANCE="${ORCH_INSTANCE:-orch1}"
API_PORT="${API_PORT:-9005}"
GATEWAY_URL="${ORCHESTRATOR_WORKER_GATEWAY_URL:-}"
GATEWAY_SECRET="${WORKER_GATEWAY_SECRET:-wgs}"
READY="${READY:-true}"

usage() {
  cat <<EOF
Usage: $0 [options]

Install a Beam orchestrator on this host (EC2 or normal Ubuntu).

Options:
  --skip-apt              Skip apt-get package install
  --skip-pip              Skip .venv / pip install
  --create-wallet         Create coldkey+hotkey via btcli (no password)
  --wallet-name NAME      Coldkey name (default: orchestrator)
  --wallet-hotkey NAME    Hotkey name (default: orch1)
  --wallet-path PATH      Wallet dir (default: ~/.bittensor/wallets)
  --btcli-version VER     bittensor-cli version (default: 9.23.1)
  --instance NAME         Orch env instance (default: orch1)
  --api-port PORT         API_PORT / worker WS port (default: 9005)
  --gateway-url URL       ORCHESTRATOR_WORKER_GATEWAY_URL (public http/ws origin)
  --gateway-secret SEC    WORKER_GATEWAY_SECRET (must match workers)
  --ready true|false      READY flag (default: true)
  --write-env             Write config/orchestrators/<instance>.env if missing
  --force-env             Overwrite existing orch env
  --install-systemd       Run install-systemd.sh --enable-orchestrators
  -h, --help              Show this help

Examples:
  $0 --create-wallet --write-env --install-systemd \\
     --gateway-url http://1.2.3.4:9005 --gateway-secret wgs
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
    --instance) ORCH_INSTANCE="${2:?}"; shift 2 ;;
    --api-port) API_PORT="${2:?}"; shift 2 ;;
    --gateway-url) GATEWAY_URL="${2:?}"; shift 2 ;;
    --gateway-secret) GATEWAY_SECRET="${2:?}"; shift 2 ;;
    --ready) READY="${2:?}"; shift 2 ;;
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

detect_public_ip() {
  local ip=""
  if command -v curl >/dev/null 2>&1; then
    ip="$(curl -4 -fsS --max-time 5 https://api.ipify.org 2>/dev/null || true)"
  fi
  if [[ -z "$ip" ]] && command -v hostname >/dev/null 2>&1; then
    ip="$(hostname -I 2>/dev/null | awk '{print $1}')"
  fi
  printf '%s\n' "${ip}"
}

if [[ -z "$GATEWAY_URL" ]]; then
  pub="$(detect_public_ip)"
  if [[ -n "$pub" ]]; then
    GATEWAY_URL="http://${pub}:${API_PORT}"
  else
    GATEWAY_URL="http://127.0.0.1:${API_PORT}"
  fi
fi

echo "=== BEAM orchestrator host setup ==="
echo "  repo:     ${ROOT}"
echo "  user:     $(id -un)"
echo "  instance: ${ORCH_INSTANCE}"
echo "  wallet:   ${WALLET_NAME}/${WALLET_HOTKEY}"
echo "  api_port: ${API_PORT}"
echo "  gateway:  ${GATEWAY_URL}"
echo

SERVICE_USER="$(beam_default_service_user)"
SERVICE_HOME="$(getent passwd "$SERVICE_USER" | cut -d: -f6)"
if [[ -z "$SERVICE_HOME" ]]; then
  SERVICE_HOME="${HOME}"
fi
if [[ "${WALLET_PATH}" == "${HOME}/.bittensor/wallets" || "${WALLET_PATH}" == "/root/.bittensor/wallets" ]]; then
  WALLET_PATH="${SERVICE_HOME}/.bittensor/wallets"
fi
echo "  service:  ${SERVICE_USER} (wallets → ${WALLET_PATH})"
echo

if [[ "$(id -u)" -eq 0 && "$SERVICE_USER" != "root" && "$CREATE_WALLET" -eq 1 ]]; then
  echo "Do not run --create-wallet as root." >&2
  echo "Wallets must be owned by ${SERVICE_USER}." >&2
  echo "  sudo -iu ${SERVICE_USER}" >&2
  echo "  cd ${ROOT}" >&2
  echo "  ./scripts/setup-orch-host.sh --create-wallet ..." >&2
  exit 1
fi

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

VENV="${ROOT}/.venv"
if [[ "$SKIP_PIP" -eq 0 ]]; then
  if [[ -x "${ROOT}/venv/bin/python" && ! -x "${VENV}/bin/python" ]]; then
    echo "Note: legacy ${ROOT}/venv exists; creating/using ${VENV} instead."
  fi
  if [[ ! -x "${VENV}/bin/python" ]]; then
    echo "Creating virtualenv at ${VENV}..."
    python3 -m venv "${VENV}"
  fi
  beam_fix_install_permissions
  echo "Installing Python dependencies into ${VENV}..."
  "${VENV}/bin/pip" install -U pip wheel setuptools
  if [[ -f "${ROOT}/requirements.txt" ]]; then
    "${VENV}/bin/pip" install -r "${ROOT}/requirements.txt"
  fi
  "${VENV}/bin/pip" install -e "${ROOT}"
fi

if [[ "$CREATE_WALLET" -eq 1 ]]; then
  BTCLI_VENV="${ROOT}/.venv-btcli"
  if [[ ! -x "${BTCLI_VENV}/bin/btcli" ]]; then
    echo "Creating btcli venv at ${BTCLI_VENV} (isolated from project deps)..."
    python3 -m venv "${BTCLI_VENV}"
    "${BTCLI_VENV}/bin/pip" install -U pip wheel setuptools
    "${BTCLI_VENV}/bin/pip" install "bittensor-cli==${BTCLI_VERSION}"
  fi
  BTCLI="${BTCLI_VENV}/bin/btcli"
  if [[ ! -x "$BTCLI" ]]; then
    echo "btcli not found at ${BTCLI} after install." >&2
    exit 1
  fi
  echo "  btcli: ${BTCLI} ($("${BTCLI}" --version 2>/dev/null || true))"

  COLDKEY_FILE="${WALLET_PATH}/${WALLET_NAME}/coldkey"
  HOTKEY_FILE="${WALLET_PATH}/${WALLET_NAME}/hotkeys/${WALLET_HOTKEY}"
  mkdir -p "${WALLET_PATH}"

  if [[ -f "$COLDKEY_FILE" ]]; then
    echo "Coldkey already exists: ${COLDKEY_FILE} (skipping new-coldkey)"
  else
    echo "Creating coldkey ${WALLET_NAME}..."
    "${BTCLI}" w new-coldkey --wallet-name "${WALLET_NAME}" --wallet-path "${WALLET_PATH}" \
      --n-words 12 --no-use-password
  fi

  if [[ -f "$HOTKEY_FILE" ]]; then
    echo "Hotkey already exists: ${HOTKEY_FILE} (skipping new-hotkey)"
  else
    echo "Creating hotkey ${WALLET_HOTKEY}..."
    "${BTCLI}" w new-hotkey --wallet-name "${WALLET_NAME}" --wallet-path "${WALLET_PATH}" \
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

ENV_FILE="${ROOT}/config/orchestrators/${ORCH_INSTANCE}.env"
if [[ "$WRITE_ENV" -eq 1 ]]; then
  if [[ -f "$ENV_FILE" && "$FORCE_ENV" -eq 0 ]]; then
    echo "Orch env already exists (use --force-env to overwrite): ${ENV_FILE}"
  else
    mkdir -p "$(dirname "$ENV_FILE")"
    cat >"$ENV_FILE" <<EOF
# Generated by scripts/setup-orch-host.sh
# Run: ./scripts/run-orchestrator.sh ${ORCH_INSTANCE}

WALLET_NAME=${WALLET_NAME}
WALLET_HOTKEY=${WALLET_HOTKEY}
WALLET_PATH=${WALLET_PATH}

API_PORT=${API_PORT}
READY=${READY}

ORCH_GATEWAY_URL=tls://orch-gateway.b1m.ai:4222
CORE_SERVER_URL=https://beamcore.b1m.ai

WORKER_GATEWAY_MODE=in_process
ORCHESTRATOR_WORKER_GATEWAY_URL=${GATEWAY_URL}
WORKER_GATEWAY_SECRET=${GATEWAY_SECRET}
ORCH_WORKER_GATEWAY_MAX_WORKERS=100
EOF
    echo "Wrote ${ENV_FILE}"
  fi
fi

if [[ "$INSTALL_SYSTEMD" -eq 1 ]]; then
  echo "Installing orchestrator systemd units..."
  if [[ "$(id -u)" -eq 0 ]]; then
    "${ROOT}/scripts/install-systemd.sh" --enable --enable-orchestrators --instances "${ORCH_INSTANCE}"
  else
    sudo "${ROOT}/scripts/install-systemd.sh" --enable --enable-orchestrators --instances "${ORCH_INSTANCE}"
  fi
fi

beam_prepare_data_dirs 2>/dev/null || true
beam_prepare_repo_permissions 2>/dev/null || true

echo
echo "Setup complete."
echo
echo "Next steps:"
if [[ ! -f "$ENV_FILE" ]]; then
  echo "  1. Write orch env:"
  echo "       $0 --write-env --wallet-name ${WALLET_NAME} --wallet-hotkey ${WALLET_HOTKEY} \\"
  echo "          --gateway-url ${GATEWAY_URL}"
else
  echo "  1. Review ${ENV_FILE} (public gateway URL must be reachable by workers)"
fi
echo "  2. Register on BeamCore (once):"
echo "       ./scripts/register-orchestrator.sh ${ORCH_INSTANCE} --write-env"
echo "  3. Ensure hotkey is registered on subnet 105"
if [[ "$INSTALL_SYSTEMD" -eq 0 ]]; then
  echo "  4. Install systemd:"
  echo "       sudo ./scripts/install-systemd.sh --enable --enable-orchestrators"
fi
echo "  5. Start orchestrator:"
echo "       ./scripts/run-orchestrator.sh ${ORCH_INSTANCE}"
echo "       # or foreground: ./scripts/run-orchestrator.sh ${ORCH_INSTANCE} --foreground"
echo
echo "Open firewall / security group for TCP ${API_PORT} (worker WebSocket)."
echo "Python:  ${VENV}/bin/python"
echo "Wallets: ${WALLET_PATH}"
