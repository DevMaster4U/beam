#!/usr/bin/env bash
# Upload a local bittensor wallet directory to control-server.
#
# Usage:
#   ./scripts/upload-wallet-to-control.sh beam-21 \
#     --url http://88.216.195.66:8010 --secret css
#
# Env fallbacks (if flags omitted):
#   CONTROL_SERVER_URL, CONTROL_SERVER_SECRET, WALLET_PATH
#
# Stores as data/control-server/wallets/<name>/{coldkey,hotkeys/...}
# (not wallets/<name>/<name>/...).
set -euo pipefail

usage() {
  cat <<EOF
Usage: $0 <wallet_name> [options]

Upload ~/.bittensor/wallets/<wallet_name> to control-server.

Options:
  --url URL         Control-server HTTP base (or CONTROL_SERVER_URL)
  --secret SECRET   Control-server secret (or CONTROL_SERVER_SECRET)
  --wallet-path DIR Wallet root (default: \$WALLET_PATH or ~/.bittensor/wallets)
  -h, --help        Show this help

Examples:
  $0 beam-21 --url http://88.216.195.66:8010 --secret css
  CONTROL_SERVER_URL=http://host:8010 CONTROL_SERVER_SECRET=css $0 beam-21
EOF
}

WALLET_NAME=""
CONTROL_SERVER_URL="${CONTROL_SERVER_URL:-}"
CONTROL_SERVER_SECRET="${CONTROL_SERVER_SECRET:-}"
WALLET_PATH="${WALLET_PATH:-$HOME/.bittensor/wallets}"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --url)
      CONTROL_SERVER_URL="${2:?--url requires a value}"
      shift 2
      ;;
    --secret)
      CONTROL_SERVER_SECRET="${2:?--secret requires a value}"
      shift 2
      ;;
    --wallet-path)
      WALLET_PATH="${2:?--wallet-path requires a value}"
      shift 2
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    -*)
      echo "Unknown option: $1" >&2
      usage >&2
      exit 1
      ;;
    *)
      if [[ -n "$WALLET_NAME" ]]; then
        echo "Unexpected argument: $1" >&2
        usage >&2
        exit 1
      fi
      WALLET_NAME="$1"
      shift
      ;;
  esac
done

if [[ -z "$WALLET_NAME" ]]; then
  echo "Missing <wallet_name>" >&2
  usage >&2
  exit 1
fi
if [[ -z "$CONTROL_SERVER_URL" ]]; then
  echo "CONTROL_SERVER_URL is required (pass --url or set CONTROL_SERVER_URL)" >&2
  exit 1
fi
if [[ -z "$CONTROL_SERVER_SECRET" ]]; then
  echo "CONTROL_SERVER_SECRET is required (pass --secret or set CONTROL_SERVER_SECRET)" >&2
  exit 1
fi

WALLET_PATH="${WALLET_PATH/#\~/${HOME}}"
SRC="${WALLET_PATH}/${WALLET_NAME}"
if [[ ! -d "$SRC" ]]; then
  echo "Wallet not found: ${SRC}" >&2
  exit 1
fi
if [[ ! -d "${SRC}/hotkeys" ]]; then
  echo "Wallet looks incomplete (missing hotkeys/): ${SRC}" >&2
  exit 1
fi

TMP="$(mktemp)"
trap 'rm -f "$TMP"' EXIT
# Pack wallet *contents* (coldkey, hotkeys/...), not the parent folder name.
# Control-server extracts into wallets/<name>/, so including <name>/ would nest.
tar -czf "$TMP" -C "$SRC" .

curl -fsS \
  -X PUT \
  -H "X-Control-Server-Secret: ${CONTROL_SERVER_SECRET}" \
  --data-binary @"$TMP" \
  "${CONTROL_SERVER_URL%/}/wallets/${WALLET_NAME}/bundle"

echo
echo "Uploaded wallet ${WALLET_NAME} to control-server"
