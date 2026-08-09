#!/usr/bin/env bash
# One-time BeamCore orchestrator HTTP registration.
#
# Creates the orchestrator record and returns api_key (only on first register).
# Must succeed before NATS connect; otherwise BeamCore returns orchestrator_not_routable.
# Docs: https://data.b1m.ai/guide/orchestrators#registration
#
# Usage:
#   ./scripts/register-orchestrator.sh orch20
#   ./scripts/register-orchestrator.sh orch20 --write-env
#   ./scripts/register-orchestrator.sh orch20 --fee 10 --name orch20
#
# Prerequisite: hotkey already on the subnet metagraph.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

usage() {
  cat <<EOF
Usage: $0 <instance> [options]

  <instance>     Name matching config/orchestrators/<instance>.env

Options:
  --write-env    Append/update BEAMCORE_ORCHESTRATOR_API_KEY in the env file
  --fee N        fee_percentage (0-100, default: FEE_PERCENTAGE from env or 10)
  --name NAME    Orchestrator display name (default: <instance>)
  --region R     Region (default: REGION from env or US)
  --url URL      Public orchestrator URL (default: ORCHESTRATOR_WORKER_GATEWAY_URL)
  --max-workers N
                 max_workers (default: MAX_WORKERS from env or 10000)
  --core-url URL CORE_SERVER_URL override (default from env or https://beamcore.b1m.ai)
  -h, --help     Show this help

Reads WALLET_NAME / WALLET_HOTKEY / WALLET_PATH from the instance env file.
Signs message "{hotkey}:{fee_percentage}" and POSTs /orchestrators/register.
EOF
}

INSTANCE="${1:-}"
if [[ -z "$INSTANCE" || "$INSTANCE" == "-h" || "$INSTANCE" == "--help" ]]; then
  usage >&2
  exit 1
fi
shift

WRITE_ENV=0
FEE=""
NAME=""
REGION_OPT=""
URL_OPT=""
MAX_WORKERS_OPT=""
CORE_URL_OPT=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    --write-env)
      WRITE_ENV=1
      shift
      ;;
    --fee)
      FEE="${2:?--fee requires a value}"
      shift 2
      ;;
    --name)
      NAME="${2:?--name requires a value}"
      shift 2
      ;;
    --region)
      REGION_OPT="${2:?--region requires a value}"
      shift 2
      ;;
    --url)
      URL_OPT="${2:?--url requires a value}"
      shift 2
      ;;
    --max-workers)
      MAX_WORKERS_OPT="${2:?--max-workers requires a value}"
      shift 2
      ;;
    --core-url)
      CORE_URL_OPT="${2:?--core-url requires a value}"
      shift 2
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
if [[ ! -f "$ENV_FILE" ]]; then
  echo "Missing env file: ${ENV_FILE}" >&2
  echo "Copy config/orchestrators/orch1.env.example and customize it." >&2
  exit 1
fi

# shellcheck disable=SC1090
set -a
# shellcheck source=/dev/null
source "$ENV_FILE"
set +a

WALLET_NAME="${WALLET_NAME:-}"
WALLET_HOTKEY="${WALLET_HOTKEY:-}"
WALLET_PATH="${WALLET_PATH:-${HOME}/.bittensor/wallets}"
CORE_URL="${CORE_URL_OPT:-${CORE_SERVER_URL:-https://beamcore.b1m.ai}}"
FEE="${FEE:-${FEE_PERCENTAGE:-10}}"
NAME="${NAME:-${INSTANCE}}"
REGION="${REGION_OPT:-${REGION:-US}}"
URL="${URL_OPT:-${ORCHESTRATOR_WORKER_GATEWAY_URL:-${WORKER_GATEWAY_URL:-}}}"
MAX_WORKERS="${MAX_WORKERS_OPT:-${MAX_WORKERS:-10000}}"

if [[ -z "$WALLET_NAME" || -z "$WALLET_HOTKEY" ]]; then
  echo "WALLET_NAME and WALLET_HOTKEY required in ${ENV_FILE}" >&2
  exit 1
fi

# Expand ~ in wallet path
WALLET_PATH="${WALLET_PATH/#\~/${HOME}}"

if [[ -n "${VIRTUAL_ENV:-}" && -x "${VIRTUAL_ENV}/bin/python" ]]; then
  PY="${VIRTUAL_ENV}/bin/python"
elif [[ -x "${ROOT}/venv/bin/python" ]]; then
  PY="${ROOT}/venv/bin/python"
else
  PY="python3"
fi

echo "Registering orchestrator instance=${INSTANCE}"
echo "  wallet: ${WALLET_NAME}/${WALLET_HOTKEY}"
echo "  core:   ${CORE_URL}"
echo "  fee:    ${FEE}"
echo "  region: ${REGION}"
echo "  name:   ${NAME}"
echo "  url:    ${URL:-"(omitted)"}"
echo "  max_workers: ${MAX_WORKERS}"

KEY_FILE="$(mktemp)"
trap 'rm -f "$KEY_FILE"' EXIT

export REG_WALLET_NAME="$WALLET_NAME"
export REG_WALLET_HOTKEY="$WALLET_HOTKEY"
export REG_WALLET_PATH="$WALLET_PATH"
export REG_CORE_URL="$CORE_URL"
export REG_FEE="$FEE"
export REG_NAME="$NAME"
export REG_REGION="$REGION"
export REG_URL="$URL"
export REG_MAX_WORKERS="$MAX_WORKERS"
export REG_KEY_OUT="$KEY_FILE"

JSON_OUT="$("$PY" - <<'PY'
import json
import os
import sys
import urllib.error
import urllib.request

import bittensor as bt

name = os.environ["REG_WALLET_NAME"]
hotkey_name = os.environ["REG_WALLET_HOTKEY"]
path = os.environ["REG_WALLET_PATH"]
core = os.environ["REG_CORE_URL"].rstrip("/")
fee = int(float(os.environ["REG_FEE"]))
display_name = os.environ["REG_NAME"]
region = os.environ["REG_REGION"]
url = (os.environ.get("REG_URL") or "").strip()
max_workers = int(os.environ["REG_MAX_WORKERS"])
key_out = os.environ["REG_KEY_OUT"]

if fee < 0 or fee > 100:
    print("fee_percentage must be 0-100", file=sys.stderr)
    sys.exit(1)

w = bt.Wallet(name=name, hotkey=hotkey_name, path=path)
hotkey = w.hotkey.ss58_address
msg = f"{hotkey}:{fee}"
sig = w.hotkey.sign(msg.encode("utf-8"))
signature = "0x" + (sig.hex() if isinstance(sig, (bytes, bytearray)) else bytes(sig).hex())

payload = {
    "hotkey": hotkey,
    "signature": signature,
    "fee_percentage": fee,
    "name": display_name,
    "region": region,
    "max_workers": max_workers,
}
if url:
    payload["url"] = url

print(f"  hotkey: {hotkey}", file=sys.stderr)
print(f"  signed: {msg}", file=sys.stderr)

body = json.dumps(payload).encode("utf-8")
req = urllib.request.Request(
    f"{core}/orchestrators/register",
    data=body,
    headers={"Content-Type": "application/json"},
    method="POST",
)
try:
    with urllib.request.urlopen(req, timeout=60) as resp:
        raw = resp.read().decode("utf-8")
except urllib.error.HTTPError as e:
    raw = e.read().decode("utf-8", errors="replace")
    print(f"HTTP {e.code}: {raw}", file=sys.stderr)
    sys.exit(1)
except Exception as e:
    print(f"Request failed: {e}", file=sys.stderr)
    sys.exit(1)

try:
    data = json.loads(raw)
except json.JSONDecodeError:
    print(f"Non-JSON response: {raw}", file=sys.stderr)
    sys.exit(1)

print(json.dumps(data, indent=2))
api_key = data.get("api_key")
if not api_key:
    print(
        "No api_key in response (already registered? key is only returned once).",
        file=sys.stderr,
    )
    sys.exit(2)

with open(key_out, "w", encoding="utf-8") as f:
    f.write(api_key)
PY
)"

echo "$JSON_OUT"
API_KEY="$(cat "$KEY_FILE")"

echo
echo "Save this API key now (returned only once):"
echo "  BEAMCORE_ORCHESTRATOR_API_KEY=${API_KEY}"

if [[ "$WRITE_ENV" -eq 1 ]]; then
  if grep -qE '^BEAMCORE_ORCHESTRATOR_API_KEY=' "$ENV_FILE"; then
    TMP="$(mktemp)"
    awk -v key="$API_KEY" '
      BEGIN { done=0 }
      /^BEAMCORE_ORCHESTRATOR_API_KEY=/ {
        print "BEAMCORE_ORCHESTRATOR_API_KEY=" key
        done=1
        next
      }
      { print }
      END {
        if (!done) print "BEAMCORE_ORCHESTRATOR_API_KEY=" key
      }
    ' "$ENV_FILE" >"$TMP"
    mv "$TMP" "$ENV_FILE"
    echo "Updated BEAMCORE_ORCHESTRATOR_API_KEY in ${ENV_FILE}"
  else
    printf '\n# From scripts/register-orchestrator.sh (one-time BeamCore key)\nBEAMCORE_ORCHESTRATOR_API_KEY=%s\n' "$API_KEY" >>"$ENV_FILE"
    echo "Appended BEAMCORE_ORCHESTRATOR_API_KEY to ${ENV_FILE}"
  fi
else
  echo
  echo "To persist into the env file:"
  echo "  ./scripts/register-orchestrator.sh ${INSTANCE} --write-env"
fi

echo
echo "Then start the orchestrator:"
echo "  ./scripts/run-orchestrator.sh ${INSTANCE} --foreground"
