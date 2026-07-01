#!/usr/bin/env bash
# Foreground launcher for the Go embedded worker module (go/beam-embedded).
#
# Usage:
#   ./scripts/run-orchestrator-go.sh orch10
#   ./scripts/run-orchestrator-go.sh orch10 --help
#
# Requires:
#   - go 1.18+
#   - config/orchestrators/<instance>.env with WORKER_GATEWAY_MODE=embedded
#   - WALLET_NAME + WALLET_HOTKEY (same as Python; auto auth/challenge+verify)
#   - optional BEAMCORE_API_KEY to skip auth endpoint
#   - WORKER_1 (or WORKER_1_HOTKEY) and optional WORKER_N_WORKER_ID / WORKER_N_API_KEY
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BEAM_ROOT="$ROOT"

INSTANCE="${1:-}"

usage() {
  cat <<EOF
Usage: $0 <instance>

  <instance>   Name matching config/orchestrators/<instance>.env

Foreground only — loads env vars, then runs:
  go run ./cmd/beam-embedded/ --env-file config/orchestrators/<instance>.env

Example:
  cp config/orchestrators/orch2-embedded.env.example config/orchestrators/orch10.env
  # edit orch10.env (WORKER_GATEWAY_MODE=embedded, WORKER_1, WORKER_2, ...)
  ./scripts/run-orchestrator-go.sh orch10

Logs also append to: logs/orchestrators/<instance>-go.log
EOF
}

if [[ -z "$INSTANCE" || "$INSTANCE" == "-h" || "$INSTANCE" == "--help" ]]; then
  usage
  exit 0
fi

ENV_FILE="${ROOT}/config/orchestrators/${INSTANCE}.env"
LOG_DIR="${ROOT}/logs/orchestrators"
LOG_FILE="${LOG_DIR}/${INSTANCE}-go.log"
GO_DIR="${ROOT}/go/beam-embedded"

if [[ ! -f "$ENV_FILE" ]]; then
  echo "Missing env file: ${ENV_FILE}" >&2
  echo "Copy config/orchestrators/orch2-embedded.env.example and customize." >&2
  exit 1
fi

if ! command -v go >/dev/null 2>&1; then
  echo "go is not installed. Install with: sudo apt install golang-go" >&2
  exit 1
fi

if [[ ! -f "${GO_DIR}/cmd/beam-embedded/main.go" ]]; then
  echo "Missing Go entrypoint: ${GO_DIR}/cmd/beam-embedded/main.go" >&2
  exit 1
fi

mkdir -p "$LOG_DIR"

mode="$(grep -E '^WORKER_GATEWAY_MODE=' "$ENV_FILE" | tail -n1 | cut -d= -f2- | tr -d ' "' || true)"
if [[ "${mode}" != "embedded" ]]; then
  echo "warning: WORKER_GATEWAY_MODE=${mode:-unset} (expected embedded for Go module)" >&2
fi

port="9000"
env_port="$(grep -E '^API_PORT=' "$ENV_FILE" | tail -n1 | cut -d= -f2- | tr -d ' "' || true)"
if [[ -n "$env_port" ]]; then
  port="$env_port"
fi

echo "Go embedded foreground: instance=${INSTANCE} env=${ENV_FILE} log=${LOG_FILE}"
echo "  api_port(from env): ${port}"
echo "  press Ctrl+C to stop"
echo ""

cd "$GO_DIR"
exec go run ./cmd/beam-embedded/ --env-file "$ENV_FILE" 2>&1 | tee -a "$LOG_FILE"
