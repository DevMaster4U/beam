#!/usr/bin/env bash
# Shared helpers for BEAM systemd service scripts.
beam_systemctl() {
  if systemctl "$@" 2>/dev/null; then
    return 0
  fi
  if [[ "${EUID}" -ne 0 ]]; then
    sudo systemctl "$@"
    return $?
  fi
  systemctl "$@"
}

beam_unit_installed() {
  local unit="$1"
  beam_systemctl cat "$unit" &>/dev/null
}

beam_require_unit() {
  local unit="$1"
  if ! beam_unit_installed "$unit"; then
    echo "Systemd unit ${unit} is not installed." >&2
    echo "Run: ${BEAM_ROOT}/scripts/install-systemd.sh" >&2
    exit 1
  fi
}

beam_python() {
  local py="${BEAM_ROOT}/venv/bin/python"
  if [[ ! -x "$py" ]]; then
    py="python3"
  fi
  printf '%s' "$py"
}

beam_list_instances_from_dir() {
  local config_dir="$1"
  shopt -s nullglob
  local env_file
  for env_file in "${config_dir}/"*.env; do
    basename "$env_file" .env
  done
}

beam_list_worker_instances() {
  beam_list_instances_from_dir "${BEAM_ROOT}/config/workers"
}

beam_list_orchestrator_instances() {
  beam_list_instances_from_dir "${BEAM_ROOT}/config/orchestrators"
}

beam_list_gateway_instances() {
  beam_list_instances_from_dir "${BEAM_ROOT}/config/gateways"
}

beam_sync_target() {
  local target_name="$1"
  local unit_prefix="$2"
  shift 2
  local instances=("$@")
  local instance
  local wants=""
  local target_file="/etc/systemd/system/${target_name}"

  if [[ "${#instances[@]}" -eq 0 ]]; then
    return 1
  fi

  for instance in "${instances[@]}"; do
    wants+=" ${unit_prefix}@${instance}.service"
  done

  sudo tee "$target_file" >/dev/null <<EOF
[Unit]
Description=BEAM ${target_name%.target} (all configured instances)
Wants=${wants# }

[Install]
WantedBy=multi-user.target
EOF

  beam_systemctl daemon-reload
  beam_systemctl enable "$target_name"

  for instance in "${instances[@]}"; do
    beam_systemctl enable "${unit_prefix}@${instance}.service"
  done
}

beam_sync_workers() {
  local instances=()
  local instance

  mapfile -t instances < <(beam_list_worker_instances)
  if [[ "${#instances[@]}" -eq 0 ]]; then
    echo "No worker env files in ${BEAM_ROOT}/config/workers/*.env" >&2
    return 1
  fi

  beam_sync_target "beam-workers.target" "beam-worker" "${instances[@]}"
}

beam_sync_orchestrators() {
  local instances=()
  local instance

  mapfile -t instances < <(beam_list_orchestrator_instances)
  if [[ "${#instances[@]}" -eq 0 ]]; then
    echo "No orchestrator env files in ${BEAM_ROOT}/config/orchestrators/*.env" >&2
    return 1
  fi

  beam_sync_target "beam-orchestrators.target" "beam-orchestrator" "${instances[@]}"
}

beam_sync_gateways() {
  local instances=()
  local instance

  mapfile -t instances < <(beam_list_gateway_instances)
  if [[ "${#instances[@]}" -eq 0 ]]; then
    echo "No gateway env files in ${BEAM_ROOT}/config/gateways/*.env" >&2
    return 1
  fi

  beam_sync_target "beam-gateways.target" "beam-worker-gateway" "${instances[@]}"
}

beam_ensure_worker_instance() {
  local instance="$1"
  local env_file="${BEAM_ROOT}/config/workers/${instance}.env"

  if [[ ! -f "$env_file" ]]; then
    echo "Missing env file: ${env_file}" >&2
    echo "Copy config/workers/${instance}.env.example and customize it." >&2
    exit 1
  fi

  beam_require_unit "beam-worker@.service"
  beam_sync_workers
}

beam_ensure_orchestrator_instance() {
  local instance="$1"
  local env_file="${BEAM_ROOT}/config/orchestrators/${instance}.env"

  if [[ ! -f "$env_file" ]]; then
    echo "Missing env file: ${env_file}" >&2
    echo "Copy config/orchestrators/${instance}.env.example and customize it." >&2
    exit 1
  fi

  beam_require_unit "beam-orchestrator@.service"
  beam_sync_orchestrators
}

beam_ensure_gateway_instance() {
  local instance="$1"
  local env_file="${BEAM_ROOT}/config/gateways/${instance}.env"

  if [[ ! -f "$env_file" ]]; then
    echo "Missing env file: ${env_file}" >&2
    echo "Copy config/gateways/${instance}.env.example and customize it." >&2
    exit 1
  fi

  beam_require_unit "beam-worker-gateway@.service"
  beam_sync_gateways
}
