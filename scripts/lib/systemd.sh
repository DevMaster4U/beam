#!/usr/bin/env bash
# Shared helpers for BEAM systemd service scripts.

# Resolve the OS user systemd services should run as.
# Priority: BEAM_USER env > SUDO_USER (sudo ./script) > ubuntu when root on EC2 > $USER
beam_default_service_user() {
  if [[ -n "${BEAM_USER:-}" ]]; then
    printf '%s' "$BEAM_USER"
    return 0
  fi
  if [[ -n "${SUDO_USER:-}" ]]; then
    printf '%s' "$SUDO_USER"
    return 0
  fi
  if [[ "${EUID}" -eq 0 ]] && id -u ubuntu &>/dev/null; then
    printf '%s' "ubuntu"
    return 0
  fi
  printf '%s' "${USER:-$(id -un)}"
}

beam_default_service_group() {
  if [[ -n "${BEAM_GROUP:-}" ]]; then
    printf '%s' "$BEAM_GROUP"
    return 0
  fi
  local user
  user="$(beam_default_service_user)"
  if id -gn "$user" &>/dev/null 2>&1; then
    printf '%s' "$(id -gn "$user")"
    return 0
  fi
  printf '%s' "$user"
}

beam_chown_if_needed() {
  local path="$1"
  local user="$2"
  local group="$3"
  [[ -e "$path" ]] || return 0
  if [[ "$(stat -c '%U' "$path" 2>/dev/null || stat -f '%Su' "$path")" == "$user" ]]; then
    return 0
  fi
  if [[ "${EUID}" -eq 0 ]]; then
    chown -R "${user}:${group}" "$path"
  else
    sudo chown -R "${user}:${group}" "$path"
  fi
}

beam_prepare_data_dirs() {
  local user="${1:-$(beam_default_service_user)}"
  local group="${2:-$(beam_default_service_group)}"

  mkdir -p \
    "${BEAM_ROOT}/logs/orchestrators" \
    "${BEAM_ROOT}/logs/workers" \
    "${BEAM_ROOT}/logs/global-gateway" \
    "${BEAM_ROOT}/logs/control-server" \
    "${BEAM_ROOT}/data/control-server/miners" \
    "${BEAM_ROOT}/data/control-server/cache" \
    "${BEAM_ROOT}/data/control-server/wallets" \
    "${BEAM_ROOT}/run"

  beam_chown_if_needed "${BEAM_ROOT}/logs" "$user" "$group"
  beam_chown_if_needed "${BEAM_ROOT}/run" "$user" "$group"
}

beam_prepare_repo_permissions() {
  local user group
  user="$(beam_default_service_user)"
  group="$(beam_default_service_group)"
  beam_prepare_data_dirs "$user" "$group"

  for dir in venv .venv; do
    beam_chown_if_needed "${BEAM_ROOT}/${dir}" "$user" "$group"
  done
}

beam_require_non_root_interactive() {
  if [[ "${EUID}" -eq 0 && -z "${SUDO_USER:-}" && -z "${BEAM_USER:-}" ]]; then
    echo "Do not run this script as root on EC2." >&2
    echo "Use the ubuntu user instead:" >&2
    echo "  sudo -iu ubuntu" >&2
    echo "  cd ${BEAM_ROOT:-~/beam}" >&2
    echo "  ./scripts/$1" >&2
    exit 1
  fi
}

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

beam_resolve_venv() {
  if [[ -x "${BEAM_ROOT}/venv/bin/python" ]]; then
    printf '%s\n' "${BEAM_ROOT}/venv"
    return 0
  fi
  if [[ -x "${BEAM_ROOT}/.venv/bin/python" ]]; then
    printf '%s\n' "${BEAM_ROOT}/.venv"
    return 0
  fi
  return 1
}

beam_python() {
  local venv_root
  if venv_root="$(beam_resolve_venv)"; then
    printf '%s' "${venv_root}/bin/python"
    return 0
  fi
  printf '%s' "python3"
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
}

beam_enable_service_instances() {
  local unit_prefix="$1"
  shift
  local instances=("$@")
  local instance

  for instance in "${instances[@]}"; do
    [[ -z "$instance" ]] && continue
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

beam_read_env_value() {
  local env_file="$1"
  local key="$2"
  grep -E "^${key}=" "$env_file" | tail -n1 | cut -d= -f2- || true
}

beam_env_files_have_key() {
  local key="$1"
  shift
  local env_file
  for env_file in "$@"; do
    [[ -f "$env_file" ]] || continue
    if grep -qE "^${key}=" "$env_file"; then
      return 0
    fi
  done
  return 1
}

beam_warn_deprecated_gateway_env() {
  local label="$1"
  shift
  local env_files=("$@")
  local deprecated=(
  "WORKER_GATEWAY_WORKER_SECRET:WORKER_GATEWAY_SECRET"
  "GATEWAY_WORKER_SECRET:WORKER_GATEWAY_SECRET"
  )
  local pair old_key new_key env_file
  for pair in "${deprecated[@]}"; do
    old_key="${pair%%:*}"
    new_key="${pair#*:}"
    for env_file in "${env_files[@]}"; do
      [[ -f "$env_file" ]] || continue
      if grep -qE "^${old_key}=" "$env_file"; then
        echo "Warning: ${label} uses deprecated ${old_key}; rename to ${new_key} in ${env_file}" >&2
      fi
    done
  done
}

beam_validate_orchestrator_gateway_env() {
  local instance_env="$1"
  local root_env="${BEAM_ROOT}/.env"
  local env_files=()
  [[ -f "$root_env" ]] && env_files+=("$root_env")
  [[ -f "$instance_env" ]] && env_files+=("$instance_env")

  beam_warn_deprecated_gateway_env "orchestrator" "${env_files[@]}"

  if [[ "${#env_files[@]}" -eq 0 ]]; then
    return 0
  fi

  if ! beam_env_files_have_key "WORKER_GATEWAY_SECRET" "${env_files[@]}" \
    && ! beam_env_files_have_key "WORKER_GATEWAY_WORKER_SECRET" "${env_files[@]}" \
    && ! beam_env_files_have_key "GATEWAY_WORKER_SECRET" "${env_files[@]}"; then
    echo "Warning: no WORKER_GATEWAY_SECRET found in root .env or ${instance_env}" >&2
    echo "  Workers will connect without worker_secret unless set elsewhere." >&2
  fi
}

beam_validate_worker_gateway_env() {
  local instance_env="$1"
  local root_env="${BEAM_ROOT}/.env"
  local env_files=()
  [[ -f "$root_env" ]] && env_files+=("$root_env")
  [[ -f "$instance_env" ]] && env_files+=("$instance_env")

  beam_warn_deprecated_gateway_env "worker" "${env_files[@]}"

  if [[ "${#env_files[@]}" -eq 0 ]]; then
    return 0
  fi

  if ! beam_env_files_have_key "WORKER_GATEWAY_SECRET" "${env_files[@]}" \
    && ! beam_env_files_have_key "WORKER_GATEWAY_WORKER_SECRET" "${env_files[@]}" \
    && ! beam_env_files_have_key "GATEWAY_WORKER_SECRET" "${env_files[@]}"; then
    echo "Warning: no WORKER_GATEWAY_SECRET found in root .env or ${instance_env}" >&2
    echo "  Worker will connect without worker_secret unless set elsewhere." >&2
  fi
}

beam_validate_orchestrator_configs() {
  local config_dir="${BEAM_ROOT}/config/orchestrators"
  local env_file
  declare -A seen_ports=()
  declare -A seen_hotkeys=()
  local port hotkey wallet_name instance

  for env_file in "${config_dir}/"*.env; do
    [[ -f "$env_file" ]] || continue
    instance="$(basename "$env_file" .env)"
    port="$(beam_read_env_value "$env_file" "API_PORT")"
    wallet_name="$(beam_read_env_value "$env_file" "WALLET_NAME")"
    hotkey="$(beam_read_env_value "$env_file" "WALLET_HOTKEY")"

    if [[ -z "$port" ]]; then
      port="9000"
    fi

    if [[ -n "${seen_ports[$port]:-}" ]]; then
      echo "Duplicate API_PORT=${port} in ${instance}.env and ${seen_ports[$port]}.env" >&2
      exit 1
    fi
    seen_ports[$port]="$instance"

    if [[ -n "$wallet_name" && -n "$hotkey" ]]; then
      local wallet_key="${wallet_name}/${hotkey}"
      if [[ -n "${seen_hotkeys[$wallet_key]:-}" ]]; then
        echo "Duplicate wallet ${wallet_key} in ${instance}.env and ${seen_hotkeys[$wallet_key]}.env" >&2
        exit 1
      fi
      seen_hotkeys[$wallet_key]="$instance"
    fi
  done
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
  beam_systemctl enable "beam-worker@${instance}.service"
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
  beam_validate_orchestrator_configs
  beam_sync_orchestrators
  beam_systemctl enable "beam-orchestrator@${instance}.service"
}

beam_ensure_global_gateway() {
  local env_file="${BEAM_ROOT}/config/global-gateway.env"

  if [[ ! -f "$env_file" ]]; then
    echo "Missing env file: ${env_file}" >&2
    echo "Copy config/global-gateway.env.example to config/global-gateway.env" >&2
    exit 1
  fi

  beam_require_unit "beam-global-gateway.service"
  beam_systemctl enable "beam-global-gateway.service"
}

beam_ensure_control_server() {
  local env_file="${BEAM_ROOT}/config/control-server.env"

  if [[ ! -f "$env_file" ]]; then
    echo "Missing env file: ${env_file}" >&2
    echo "Copy config/control-server.env.example to config/control-server.env" >&2
    exit 1
  fi

  beam_require_unit "beam-control-server.service"
  beam_systemctl enable "beam-control-server.service"
}
