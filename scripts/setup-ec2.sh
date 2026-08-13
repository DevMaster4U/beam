#!/usr/bin/env bash
# Ubuntu host bootstrap (EC2 or normal VPS/bare metal) — role-agnostic.
#
# Installs OS packages + Python venv only. Does NOT create wallets or role env.
# Pick a role script after this (or skip this and run the role script alone;
# setup-orch-host.sh / setup-worker-host.sh can also install apt/.venv).
#
#   Host type:  EC2 (ubuntu user)  OR  normal Ubuntu server
#   Next role:  orch → setup-orch-host.sh   |   worker → setup-worker-host.sh
#
# Run as a normal user (on AWS EC2: ubuntu), not as root:
#   git clone … && cd sn105
#   ./scripts/setup-ec2.sh
#
# Then:
#   # Orchestrator host
#   ./scripts/setup-orch-host.sh --create-wallet --write-env --install-systemd \
#     --gateway-url http://YOUR_PUBLIC_IP:9005 --gateway-secret wgs
#
#   # Worker host
#   ./scripts/setup-worker-host.sh --create-wallet --write-env --install-systemd \
#     --gateway-url ws://ORCH_PUBLIC_IP:9005 --gateway-secret wgs
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BEAM_ROOT="$ROOT"
# shellcheck disable=SC1091
source "${ROOT}/scripts/lib/systemd.sh"

beam_require_non_root_interactive "setup-ec2.sh"

echo "=== BEAM host bootstrap (EC2 or normal Ubuntu) ==="
echo "  repo: ${ROOT}"
echo "  user: $(id -un)"
echo "  home: ${HOME}"
echo "  note: role-agnostic — run setup-orch-host.sh or setup-worker-host.sh next"
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
echo "Host bootstrap complete."
echo
echo "Python:  ${VENV}/bin/python"
echo
echo "Next — choose ONE role on this machine:"
echo
echo "  Orchestrator (EC2 or normal):"
echo "    ./scripts/setup-orch-host.sh --skip-apt --skip-pip \\"
echo "      --create-wallet --write-env --install-systemd \\"
echo "      --gateway-url http://YOUR_PUBLIC_IP:9005 --gateway-secret wgs"
echo "    ./scripts/register-orchestrator.sh orch1 --write-env"
echo "    ./scripts/run-orchestrator.sh orch1"
echo
echo "  Worker (EC2 or normal):"
echo "    ./scripts/setup-worker-host.sh --skip-apt --skip-pip \\"
echo "      --create-wallet --write-env --install-systemd \\"
echo "      --gateway-url ws://ORCH_PUBLIC_IP:9005 --gateway-secret wgs"
echo "    ./scripts/run-worker.sh worker1"
echo
echo "Optional (same host as orch): control-server / global-gateway"
echo "    sudo ./scripts/install-systemd.sh --enable-control-server"
echo "    sudo ./scripts/install-systemd.sh --enable-global-gateway"
echo
echo "Wallets live in ~/.bittensor/wallets (${HOME}/.bittensor/wallets)."
echo "Never run setup/pip as root — it breaks .venv and beam.egg-info ownership."
