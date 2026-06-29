#!/usr/bin/env bash
# Scan all servers for running BEAM orchestrators.
# Usage: ./scripts/scan-orchestrators-remote.sh [credentials-file]
#
# credentials-file format (tab-separated, no header):
#   IP<TAB>password
#
# Default credentials file: config/servers.credentials (create it yourself; gitignored)

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CREDS="${1:-${ROOT}/config/servers.credentials}"

if [[ ! -f "$CREDS" ]]; then
  echo "Missing credentials file: $CREDS" >&2
  echo "Create it with one line per server: IP<TAB>password" >&2
  exit 1
fi

if ! command -v sshpass >/dev/null 2>&1; then
  echo "Installing sshpass..." >&2
  apt-get install -y -qq sshpass
fi

REMOTE_CMD='hostname; echo "--- ORCHESTRATORS ---"; ps aux | grep orchestrator/main.py | grep -v grep || echo "(none)"; echo "--- SYSTEMD ---"; systemctl list-units "beam-orchestrator@*" --all 2>/dev/null | grep beam-orchestrator || echo "(none)"; echo "--- PORTS ---"; ss -tlnp 2>/dev/null | grep -E "900[0-9]|901[0-9]|8005" || echo "(none)"; echo "--- BEAM OTHER ---"; ps aux | grep -E "global-gateway|worker\.py" | grep python | grep -v grep || echo "(none)"'

while IFS=$'\t' read -r ip pass || [[ -n "${ip:-}" ]]; do
  [[ -z "${ip:-}" || "$ip" =~ ^# ]] && continue
  echo ""
  echo "============================================================"
  echo "SERVER: $ip"
  echo "============================================================"
  sshpass -p "$pass" ssh -n -o StrictHostKeyChecking=no -o ConnectTimeout=10 -o BatchMode=no \
    "root@${ip}" "$REMOTE_CMD" 2>&1 || echo "ERROR: could not connect to $ip"
done < "$CREDS"
