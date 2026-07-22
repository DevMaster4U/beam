#!/usr/bin/env bash
# Deploy the same transfer-worker.js to one or more Cloudflare Worker names.
#
# Headless servers cannot use `wrangler login` (needs browser / xdg-open).
# Use an API token instead:
#   export CLOUDFLARE_API_TOKEN=...        # required
#   export CLOUDFLARE_ACCOUNT_ID=...      # optional if token is account-scoped
#
# Create token: https://dash.cloudflare.com/profile/api-tokens
#   Template "Edit Cloudflare Workers" (Account → Workers Scripts: Edit)
#
# Usage:
#   ./scripts/cloudflare/deploy-workers.sh still-base-8f94 noisy-union-160b
#   ./scripts/cloudflare/deploy-workers.sh --file scripts/cloudflare/workers.txt
#   CF_TRANSFER_WORKER_NAMES=still-base-8f94,noisy-union-160b ./scripts/cloudflare/deploy-workers.sh
#
# After deploy, verify versions:
#   python3 scripts/cloudflare/check-worker-versions.py --expect 1.1.0

set -euo pipefail

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
SCRIPT="$ROOT/scripts/cloudflare/transfer-worker.js"
CDIR="$ROOT/scripts/cloudflare"
NAMES=()

usage() {
  sed -n '2,20p' "$0" | sed 's/^# \{0,1\}//'
  exit 2
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    -h|--help) usage ;;
    --file)
      shift
      [[ $# -gt 0 ]] || usage
      while IFS= read -r line || [[ -n "$line" ]]; do
        line="${line%%#*}"
        line="$(echo "$line" | xargs)"
        [[ -n "$line" ]] && NAMES+=("$line")
      done < "$1"
      shift
      ;;
    *)
      NAMES+=("$1")
      shift
      ;;
  esac
done

if [[ ${#NAMES[@]} -eq 0 && -n "${CF_TRANSFER_WORKER_NAMES:-}" ]]; then
  IFS=', ' read -r -a NAMES <<< "${CF_TRANSFER_WORKER_NAMES}"
fi

if [[ ${#NAMES[@]} -eq 0 ]]; then
  echo "need worker name(s), --file path, or CF_TRANSFER_WORKER_NAMES" >&2
  usage
fi

if [[ ! -f "$SCRIPT" ]]; then
  echo "missing $SCRIPT" >&2
  exit 1
fi

if [[ -z "${CLOUDFLARE_API_TOKEN:-}" ]]; then
  cat >&2 <<'EOF'
Not authenticated for headless deploy.

This host has no browser (wrangler login → xdg-open fails).

1) Create an API token:
   https://dash.cloudflare.com/profile/api-tokens
   Use template "Edit Cloudflare Workers"

2) Export it, then redeploy:
   export CLOUDFLARE_API_TOKEN='your_token_here'
   # optional if you have multiple accounts:
   # export CLOUDFLARE_ACCOUNT_ID='d5c7459ec862481084f6addb310afbe7'
   ./scripts/cloudflare/deploy-workers.sh --file scripts/cloudflare/workers.txt
EOF
  exit 2
fi

VERSION="$(grep -E 'const WORKER_VERSION' "$SCRIPT" | head -1 | sed -E 's/.*"([^"]+)".*/\1/')"
echo "Deploying transfer-worker.js version=${VERSION} → ${#NAMES[@]} worker(s)"
echo "Auth: CLOUDFLARE_API_TOKEN (len=${#CLOUDFLARE_API_TOKEN})"
if [[ -n "${CLOUDFLARE_ACCOUNT_ID:-}" ]]; then
  echo "Account: ${CLOUDFLARE_ACCOUNT_ID}"
fi

# Avoid OAuth / browser path if somehow triggered.
export WRANGLER_SEND_METRICS="${WRANGLER_SEND_METRICS:-false}"

failed=0
for name in "${NAMES[@]}"; do
  name="$(echo "$name" | xargs)"
  [[ -n "$name" ]] || continue
  echo "--- wrangler deploy --name ${name}"
  if ! (
    cd "$CDIR"
    npx --yes wrangler deploy transfer-worker.js --name "$name"
  ); then
    echo "FAIL deploy name=${name}" >&2
    failed=1
  fi
done

if [[ "$failed" -ne 0 ]]; then
  echo "one or more deploys failed" >&2
  exit 1
fi

echo "All deploys OK. Check versions:"
echo "  python3 scripts/cloudflare/check-worker-versions.py --expect ${VERSION}"
