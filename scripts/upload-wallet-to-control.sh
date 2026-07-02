#!/usr/bin/env bash
# Upload a local bittensor wallet directory to control-server.
#
# Usage:
#   CONTROL_SERVER_URL=http://control:8010 CONTROL_SERVER_SECRET=secret \
#     ./scripts/upload-wallet-to-control.sh beam-w1
set -euo pipefail

WALLET_NAME="${1:-}"
if [[ -z "$WALLET_NAME" ]]; then
  echo "Usage: $0 <wallet_name>" >&2
  exit 1
fi

: "${CONTROL_SERVER_URL:?CONTROL_SERVER_URL is required}"
: "${CONTROL_SERVER_SECRET:?CONTROL_SERVER_SECRET is required}"

WALLET_PATH="${WALLET_PATH:-$HOME/.bittensor/wallets}"
SRC="${WALLET_PATH}/${WALLET_NAME}"
if [[ ! -d "$SRC" ]]; then
  echo "Wallet not found: ${SRC}" >&2
  exit 1
fi

TMP="$(mktemp)"
trap 'rm -f "$TMP"' EXIT
tar -czf "$TMP" -C "$WALLET_PATH" "$WALLET_NAME"

curl -fsS \
  -X PUT \
  -H "X-Control-Server-Secret: ${CONTROL_SERVER_SECRET}" \
  --data-binary @"$TMP" \
  "${CONTROL_SERVER_URL%/}/wallets/${WALLET_NAME}/bundle"

echo
echo "Uploaded wallet ${WALLET_NAME} to control-server"
