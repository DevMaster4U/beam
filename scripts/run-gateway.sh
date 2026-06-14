#!/usr/bin/env bash
# Alias for run-worker-gateway.sh (one gateway instance).
exec "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/run-worker-gateway.sh" "$@"
