#!/usr/bin/env bash
# First-time host setup for AWS EC2 (Ubuntu).
#
# Run as the ubuntu user (not a root login shell):
#   git clone ... && cd sn105
#   ./scripts/setup-ec2.sh
#
# Then install systemd units (sudo preserves ubuntu as the service user):
#   sudo ./scripts/install-systemd.sh --enable
#   sudo ./scripts/install-systemd.sh --enable-global-gateway
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BEAM_ROOT="$ROOT"
# shellcheck disable=SC1091
source "${ROOT}/scripts/lib/systemd.sh"

beam_require_non_root_interactive "setup-ec2.sh"

echo "=== BEAM EC2 setup ==="
echo "  repo: ${ROOT}"
echo "  user: $(id -un)"
echo "  home: ${HOME}"
echo

if ! command -v apt-get >/dev/null 2>&1; then
  echo "This script targets Ubuntu/Debian (apt-get not found)." >&2
  exit 1
fi

echo "Installing OS packages..."
sudo apt-get update
sudo DEBIAN_FRONTEND=noninteractive apt-get install -y \
  python3 python3-venv python3-pip python3-dev \
  git build-essential curl jq

VENV="${ROOT}/.venv"
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
"${VENV}/bin/pip" install -e "${ROOT}"

beam_prepare_repo_permissions

echo
echo "Setup complete."
echo
echo "Python:  ${VENV}/bin/python"
echo "Next steps (as $(id -un)):"
echo "  1. Copy and edit env files under config/ and .env"
echo "  2. Install systemd units:"
echo "       sudo ./scripts/install-systemd.sh --enable"
echo "       sudo ./scripts/install-systemd.sh --enable-global-gateway"
echo "       sudo ./scripts/install-systemd.sh --enable-orchestrators"
echo "       sudo ./scripts/install-systemd.sh --enable-workers"
echo "  3. Start services:"
echo "       ./scripts/run-global-gateway.sh start"
echo "       ./scripts/run-orchestrators.sh start"
echo "       ./scripts/run-workers.sh start"
echo
echo "Wallets live in ~/.bittensor/wallets (${HOME}/.bittensor/wallets)."
