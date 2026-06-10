#!/usr/bin/env bash
# Recommend worker concurrency from link speed and optionally install BBR sysctl.
#
# Usage:
#   ./scripts/tune-worker-host.sh                    # print recommendations only
#   ./scripts/tune-worker-host.sh --link-mbps 500    # customize uplink estimate
#   ./scripts/tune-worker-host.sh --workers 2        # split across N local instances
#   ./scripts/tune-worker-host.sh --install-bbr      # copy sysctl snippet (needs sudo)
#
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
LINK_MBPS=""
WORKER_INSTANCES=1
INSTALL_BBR=0

usage() {
  sed -n '2,10p' "$0"
  exit "${1:-0}"
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    -h|--help) usage 0 ;;
    --link-mbps) LINK_MBPS="${2:-}"; shift 2 ;;
    --workers) WORKER_INSTANCES="${2:-1}"; shift 2 ;;
    --install-bbr) INSTALL_BBR=1; shift ;;
    *) echo "Unknown option: $1" >&2; usage 1 ;;
  esac
done

if [[ -z "$LINK_MBPS" ]]; then
  echo "Link speed (symmetric Mbps, upload matters most for relay):"
  read -r -p "  Enter Mbps [100]: " LINK_MBPS
  LINK_MBPS="${LINK_MBPS:-100}"
fi

if ! [[ "$LINK_MBPS" =~ ^[0-9]+([.][0-9]+)?$ ]] || [[ "${LINK_MBPS%%.*}" -lt 1 ]]; then
  echo "Invalid --link-mbps: $LINK_MBPS" >&2
  exit 1
fi

if ! [[ "$WORKER_INSTANCES" =~ ^[0-9]+$ ]] || [[ "$WORKER_INSTANCES" -lt 1 ]]; then
  echo "Invalid --workers: $WORKER_INSTANCES" >&2
  exit 1
fi

cc="$(sysctl -n net.ipv4.tcp_congestion_control 2>/dev/null || echo unknown)"
qdisc="$(sysctl -n net.core.default_qdisc 2>/dev/null || echo unknown)"

echo
echo "=== Host TCP ==="
echo "  congestion_control: $cc"
echo "  default_qdisc:      $qdisc"
if [[ "$cc" != "bbr" ]]; then
  echo "  recommendation:     enable BBR (see deploy/sysctl/99-beam-worker-tcp.conf)"
  echo "                      ./scripts/tune-worker-host.sh --install-bbr"
else
  echo "  recommendation:     BBR already active"
fi

# ~25 Mbps sustained per concurrent relay task (4 MiB chunks, pipelined GET+PUT).
# Uplink is usually the limiter; treat symmetric Mbps as a conservative budget.
MBPS_PER_TASK=25
MAX_TASKS_PER_INSTANCE=16
total_tasks=$(( (LINK_MBPS + MBPS_PER_TASK - 1) / MBPS_PER_TASK ))
if [[ "$total_tasks" -lt 1 ]]; then
  total_tasks=1
fi
if [[ "$total_tasks" -gt 64 ]]; then
  total_tasks=64
fi

per_worker=$(( total_tasks / WORKER_INSTANCES ))
remainder=$(( total_tasks % WORKER_INSTANCES ))
if [[ "$per_worker" -lt 1 ]]; then
  per_worker=1
fi
if [[ "$per_worker" -gt "$MAX_TASKS_PER_INSTANCE" ]]; then
  per_worker=$MAX_TASKS_PER_INSTANCE
fi

# 4 MiB default chunk; keep ~2× headroom for queued accepts.
CHUNK_MIB=4
in_flight_mib=$(( per_worker * CHUNK_MIB * 2 ))
if [[ "$in_flight_mib" -lt 8 ]]; then
  in_flight_mib=8
fi
in_flight_bytes=$(( in_flight_mib * 1024 * 1024 ))

echo
echo "=== Concurrency (link ${LINK_MBPS} Mbps, ${WORKER_INSTANCES} worker instance(s)) ==="
echo "  total concurrent tasks (host):  ~${total_tasks}"
echo "  per instance:"
echo "    WORKER_MAX_CONCURRENT_TASKS=${per_worker}"
echo "    WORKER_MAX_QUEUED_WS_TASKS=${per_worker}"
echo "    WORKER_MAX_IN_FLIGHT_BYTES=${in_flight_bytes}   # ${in_flight_mib} MiB"
if [[ "$remainder" -gt 0 ]]; then
  echo "  note: ${remainder} extra task slot(s) available — add to one instance if CPU allows"
fi
echo
echo "  Each task ≈ 2 TCP flows (source GET + dest PUT). Raise slowly and watch"
echo "  logs/workers/*.log for timeouts or rising fetch_ms/send_ms."
echo

if [[ "$INSTALL_BBR" -eq 1 ]]; then
  src="${ROOT}/deploy/sysctl/99-beam-worker-tcp.conf"
  dst="/etc/sysctl.d/99-beam-worker-tcp.conf"
  if [[ ! -f "$src" ]]; then
    echo "Missing $src" >&2
    exit 1
  fi
  echo "Installing $dst ..."
  sudo cp "$src" "$dst"
  sudo sysctl --system
  echo "Done. congestion_control=$(sysctl -n net.ipv4.tcp_congestion_control)"
fi
