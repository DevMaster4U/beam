"""
Orchestrator Configuration

Settings for the BEAM Orchestrator service (single-node deployment).
"""

import logging
import os
import sys
from functools import lru_cache
from pathlib import Path
from typing import List, Optional

from pydantic import AliasChoices, Field
from pydantic_settings import BaseSettings

_WORKSPACE_ROOT = Path(__file__).resolve().parents[3]
_ENV_LOADED = False


def _workspace_root() -> Path:
    return _WORKSPACE_ROOT


def _extract_env_file_arg(argv: list[str]) -> tuple[Optional[Path], list[str]]:
    cleaned: list[str] = []
    env_file: Optional[Path] = None
    idx = 0
    while idx < len(argv):
        arg = argv[idx]
        if arg == "--env-file":
            if idx + 1 >= len(argv):
                print("Error: --env-file requires a path argument", file=sys.stderr)
                sys.exit(2)
            env_file = Path(argv[idx + 1]).expanduser()
            idx += 2
            continue
        if arg.startswith("--env-file="):
            env_file = Path(arg.split("=", 1)[1]).expanduser()
            idx += 1
            continue
        cleaned.append(arg)
        idx += 1
    return env_file, cleaned


def _resolve_env_path(path: Path) -> Path:
    if path.is_absolute():
        return path
    return _workspace_root() / path


def _load_workspace_env() -> None:
    """Load workspace .env, then optional --env-file instance config (override)."""
    global _ENV_LOADED
    if _ENV_LOADED:
        return

    try:
        from dotenv import load_dotenv
    except ImportError:
        _ENV_LOADED = True
        return

    shared_env = _workspace_root() / ".env"
    if shared_env.exists():
        load_dotenv(shared_env, override=False)

    env_file, _ = _extract_env_file_arg(sys.argv[1:])
    if env_file is None:
        env_path = os.environ.get("ORCHESTRATOR_ENV_FILE", "").strip()
        if env_path:
            env_file = Path(env_path).expanduser()

    if env_file is not None:
        env_file = _resolve_env_path(env_file)
        if env_file.exists():
            load_dotenv(env_file, override=True)
        else:
            print(f"Warning: orchestrator env file not found: {env_file}", file=sys.stderr)

    _ENV_LOADED = True


def _orchestrator_instance_name() -> str:
    instance = os.environ.get("ORCHESTRATOR_INSTANCE", "").strip()
    if instance:
        return instance

    env_file, _ = _extract_env_file_arg(sys.argv[1:])
    if env_file is None:
        env_path = os.environ.get("ORCHESTRATOR_ENV_FILE", "").strip()
        if env_path:
            env_file = Path(env_path).expanduser()

    if env_file is not None:
        return _resolve_env_path(env_file).stem
    return "orchestrator"


_LOGGING_CONFIGURED = False


def configure_orchestrator_logging(force: bool = False) -> Path:
    """Write orchestrator logs to logs/orchestrators/<instance>.log."""
    global _LOGGING_CONFIGURED
    instance = _orchestrator_instance_name()
    log_root = Path(os.environ.get("LOG_DIR", _workspace_root() / "logs"))
    log_dir = log_root / "orchestrators"
    log_dir.mkdir(parents=True, exist_ok=True)
    log_path = log_dir / f"{instance}.log"

    if _LOGGING_CONFIGURED and not force:
        return log_path

    log_format = "%(asctime)s.%(msecs)03.0f | %(levelname)s | %(name)s | %(message)s"
    log_datefmt = "%Y-%m-%d %H:%M:%S"
    formatter = logging.Formatter(log_format, datefmt=log_datefmt)

    file_handler = logging.FileHandler(log_path, delay=False)
    file_handler.setFormatter(formatter)

    handlers: list[logging.Handler] = [file_handler]
    # Under systemd (no TTY) stderr is captured by journal — useful when the log file path is wrong.
    if not sys.stderr.isatty():
        stderr_handler = logging.StreamHandler(sys.stderr)
        stderr_handler.setFormatter(formatter)
        handlers.append(stderr_handler)
    else:
        stream_handler = logging.StreamHandler()
        stream_handler.setFormatter(formatter)
        handlers.append(stream_handler)

    logging.basicConfig(
        level=logging.INFO,
        format=log_format,
        datefmt=log_datefmt,
        handlers=handlers,
        force=True,
    )

    _LOGGING_CONFIGURED = True
    logging.getLogger(__name__).info("Orchestrator logging initialized: %s", log_path)
    return log_path


_load_workspace_env()
configure_orchestrator_logging()


class OrchestratorSettings(BaseSettings):
    """Orchestrator configuration settings."""

    # ==========================================================================
    # API Settings
    # ==========================================================================
    api_host: str = Field(default="0.0.0.0", env="ORCHESTRATOR_HOST")
    api_port: int = Field(default=8000, env="API_PORT")  # Also accepts ORCHESTRATOR_PORT
    log_level: str = Field(default="INFO", env="LOG_LEVEL")

    # Local mode - skip Bittensor wallet/subtensor initialization for development
    local_mode: bool = Field(default=False, env="LOCAL_MODE")
    local_orchestrator_hotkey: str = Field(
        default="local-dev-hotkey", env="LOCAL_ORCHESTRATOR_HOTKEY"
    )

    # Add mock worker for testing (use with real wallet but no real miners)
    add_mock_worker: bool = Field(default=False, env="ADD_MOCK_WORKER")

    # Mock worker hotkey (use real worker hotkey for realistic PoB records)
    mock_worker_hotkey: Optional[str] = Field(default=None, env="MOCK_WORKER_HOTKEY")

    region: str = Field(default="US", env="REGION")  # US, EU, APAC, RESERVE (BeamCore registration)

    # ==========================================================================
    # Subnet Settings
    # ==========================================================================
    netuid: int = Field(default=105, env="NETUID")
    subtensor_network: str = Field(default="finney", env="SUBTENSOR_NETWORK")
    subtensor_address: Optional[str] = Field(default=None, env="SUBTENSOR_ADDRESS")

    # ==========================================================================
    # Orchestrator Wallet (for signing reports to validators)
    # ==========================================================================
    wallet_name: str = Field(default="orchestrator", env="WALLET_NAME")
    wallet_hotkey: str = Field(default="default", env="WALLET_HOTKEY")
    wallet_path: str = Field(default="~/.bittensor/wallets", env="WALLET_PATH")

    # ==========================================================================
    # Orchestrator UID (on-chain miner slot)
    # ==========================================================================
    # If set, uses this UID for registration. Otherwise auto-detects from metagraph.
    # Get your UID: btcli subnet metagraph --netuid 105 --subtensor.network finney
    uid: Optional[int] = Field(default=None, env="ORCHESTRATOR_UID")

    # ==========================================================================
    # Fee Settings (% of emission shared with workers)
    # ==========================================================================
    fee_percentage: float = Field(default=0.0, env="FEE_PERCENTAGE")  # 0-100%

    # ==========================================================================
    # Compensation reference settings
    # ==========================================================================
    # Reference per-chunk amount for local accounting and operator-defined compensation workflows.
    alpha_per_chunk: float = Field(default=0.5, env="ALPHA_PER_CHUNK")

    # ==========================================================================
    # Readiness
    # ==========================================================================
    # When True, orchestrator signals BeamCore that it is ready to receive transfers.
    # Set via READY=true env var or the websocket set_ready flow at runtime.
    # Default is False — new orchestrators are excluded from routing until explicit opt-in.
    ready: bool = Field(default=False, env="READY")

    # ==========================================================================
    # Worker Management
    # ==========================================================================
    max_workers: int = Field(default=10000, env="MAX_WORKERS")
    worker_timeout_seconds: int = Field(default=300, env="WORKER_TIMEOUT")
    min_worker_bandwidth_mbps: float = Field(default=10.0, env="MIN_WORKER_BANDWIDTH")
    worker_heartbeat_interval: int = Field(default=30, env="WORKER_HEARTBEAT_INTERVAL")

    # ==========================================================================
    # Task Settings
    # ==========================================================================
    max_concurrent_tasks: int = Field(default=1000, env="MAX_CONCURRENT_TASKS")
    task_timeout_seconds: int = Field(default=120, env="TASK_TIMEOUT")
    chunk_size_bytes: int = Field(default=1024 * 1024, env="CHUNK_SIZE")  # 1 MB

    # ==========================================================================
    # Anti-Fraud Settings
    # ==========================================================================
    enable_geo_verification: bool = Field(default=True, env="ENABLE_GEO_VERIFICATION")
    enable_latency_verification: bool = Field(default=True, env="ENABLE_LATENCY_VERIFICATION")
    max_suspicious_score: float = Field(default=0.3, env="MAX_SUSPICIOUS_SCORE")

    # ==========================================================================
    # BeamCore API
    # ==========================================================================
    core_server_url: str = Field(default="https://beamcore.b1m.ai", env="CORE_SERVER_URL")

    orch_gateway_url: Optional[str] = Field(default=None, env="ORCH_GATEWAY_URL")

    # Orch-gateway WebSocket transport (high-latency / cross-region: increase these)
    orch_ws_open_timeout: float = Field(default=60.0, env="ORCH_WS_OPEN_TIMEOUT")
    orch_ws_close_timeout: float = Field(default=20.0, env="ORCH_WS_CLOSE_TIMEOUT")
    orch_ws_ping_interval: float = Field(default=30.0, env="ORCH_WS_PING_INTERVAL")
    orch_ws_ping_timeout: float = Field(default=45.0, env="ORCH_WS_PING_TIMEOUT")
    # BeamCore control-plane round-trips over the orch-gateway WebSocket.
    # Keep ORCH_TASK_RESULT_TIMEOUT below WORKER_TASK_RESULT_ACK_TIMEOUT on workers.
    orch_ws_request_timeout: float = Field(default=15.0, env="ORCH_WS_REQUEST_TIMEOUT")
    orch_task_accept_timeout: float = Field(default=8.0, env="ORCH_TASK_ACCEPT_TIMEOUT")
    orch_task_result_timeout: float = Field(default=30.0, env="ORCH_TASK_RESULT_TIMEOUT")

    worker_gateway_url: Optional[str] = Field(
        default=None,
        validation_alias=AliasChoices(
            "ORCHESTRATOR_WORKER_GATEWAY_URL",
            "WORKER_GATEWAY_URL",
        ),
    )
    worker_gateway_worker_secret: Optional[str] = Field(
        default=None,
        validation_alias=AliasChoices(
            "WORKER_GATEWAY_SECRET",
            "WORKER_GATEWAY_WORKER_SECRET",
            "GATEWAY_WORKER_SECRET",
        ),
    )
    worker_gateway_mode: str = Field(default="in_process", env="WORKER_GATEWAY_MODE")
    global_gateway_url: Optional[str] = Field(default=None, env="GLOBAL_GATEWAY_URL")
    pool_coordinator_ipc: Optional[str] = Field(
        default=None,
        env="POOL_COORDINATOR_IPC",
        description="Unix socket path for colocated pool coordinator (WORKER_GATEWAY_MODE=coordinator)",
    )
    orchestrator_gateway_secret: Optional[str] = Field(
        default=None,
        validation_alias=AliasChoices(
            "ORCHESTRATOR_GATEWAY_SECRET",
            "ORCHESTRATOR_WORKER_GATEWAY_SECRET",
            "GATEWAY_ORCHESTRATOR_SECRET",
        ),
    )
    # ==========================================================================
    # Worker Scoring Weights (for selection)
    # ==========================================================================
    weight_trust: float = Field(default=0.30, env="WEIGHT_TRUST")
    weight_latency: float = Field(default=0.25, env="WEIGHT_LATENCY")
    weight_load: float = Field(default=0.20, env="WEIGHT_LOAD")
    weight_bandwidth: float = Field(default=0.15, env="WEIGHT_BANDWIDTH")
    weight_success: float = Field(default=0.10, env="WEIGHT_SUCCESS")

    # ==========================================================================
    # Reward Distribution Weights (for epoch-end payment calculation)
    # ==========================================================================
    # Primary factor: bytes relayed (work done)
    reward_weight_bytes: float = Field(default=0.50, env="REWARD_WEIGHT_BYTES")
    # Quality factors
    reward_weight_success_rate: float = Field(default=0.20, env="REWARD_WEIGHT_SUCCESS_RATE")
    reward_weight_latency: float = Field(default=0.15, env="REWARD_WEIGHT_LATENCY")
    reward_weight_trust: float = Field(default=0.15, env="REWARD_WEIGHT_TRUST")

    # ==========================================================================
    # BEAM Storage Settings (Hub)
    # ==========================================================================
    storage_gateway_url: str = Field(
        default="https://storage.beam.network", env="STORAGE_GATEWAY_URL"
    )
    storage_replication_factor: int = Field(default=3, env="STORAGE_REPLICATION_FACTOR")

    # External IP for registration (auto-detected if not set)
    external_ip: Optional[str] = Field(default=None, env="EXTERNAL_IP")

    # ==========================================================================
    # Client Authentication
    # ==========================================================================
    # Master toggle for client authentication
    client_auth_enabled: bool = Field(default=True, env="CLIENT_AUTH_ENABLED")

    # If true, only whitelisted clients can register
    client_whitelist_only: bool = Field(default=False, env="CLIENT_WHITELIST_ONLY")

    # Pre-approved hotkeys (comma-separated SS58 addresses)
    client_pre_approved_hotkeys: Optional[str] = Field(
        default=None, env="CLIENT_PRE_APPROVED_HOTKEYS"
    )

    # Admin hotkeys for client management (comma-separated SS58 addresses)
    client_admin_hotkeys: Optional[str] = Field(default=None, env="CLIENT_ADMIN_HOTKEYS")

    # Signature expiration time (seconds)
    client_signature_max_age_seconds: int = Field(
        default=300, env="CLIENT_SIGNATURE_MAX_AGE_SECONDS"
    )

    # ==========================================================================
    # Subnet Participant Authentication (Validators & Workers)
    # ==========================================================================
    # Master toggle for subnet participant auth (validators and workers)
    subnet_auth_enabled: bool = Field(default=True, env="SUBNET_AUTH_ENABLED")

    # Require metagraph verification (hotkey must be registered on subnet)
    subnet_auth_require_metagraph: bool = Field(default=True, env="SUBNET_AUTH_REQUIRE_METAGRAPH")

    # Whitelisted hotkeys that bypass metagraph check (comma-separated)
    subnet_auth_whitelist: Optional[str] = Field(default=None, env="SUBNET_AUTH_WHITELIST")

    # ==========================================================================
    # Subnet Partner Program (free access for other Bittensor subnets)
    # ==========================================================================
    # Enable subnet partner registration (hotkeys from other subnets get free access)
    subnet_partner_enabled: bool = Field(default=True, env="SUBNET_PARTNER_ENABLED")

    def model_post_init(self, __context) -> None:
        object.__setattr__(self, "log_level", self.log_level.upper())

        if not self.orch_gateway_url:
            self.orch_gateway_url = os.environ.get("ORCHESTRATOR_WS_BASE_URL")

        if not self.orch_gateway_url:
            raise ValueError("ORCH_GATEWAY_URL is required")

        if not self.worker_gateway_worker_secret:
            for alt_name in (
                "WORKER_GATEWAY_SECRET",
                "WORKER_GATEWAY_WORKER_SECRET",
                "GATEWAY_WORKER_SECRET",
            ):
                alt_worker = os.environ.get(alt_name, "").strip()
                if alt_worker:
                    object.__setattr__(self, "worker_gateway_worker_secret", alt_worker)
                    break

        if not self.worker_gateway_url:
            for alt_name in ("ORCHESTRATOR_WORKER_GATEWAY_URL", "WORKER_GATEWAY_URL"):
                alt_url = os.environ.get(alt_name, "").strip()
                if alt_url:
                    object.__setattr__(self, "worker_gateway_url", alt_url)
                    break

        if not self.orchestrator_gateway_secret:
            for alt_name in (
                "ORCHESTRATOR_GATEWAY_SECRET",
                "ORCHESTRATOR_WORKER_GATEWAY_SECRET",
                "GATEWAY_ORCHESTRATOR_SECRET",
            ):
                alt_secret = os.environ.get(alt_name, "").strip()
                if alt_secret:
                    object.__setattr__(self, "orchestrator_gateway_secret", alt_secret)
                    break

    # ==========================================================================
    # Client Tiers
    # ==========================================================================
    # Basic tier
    client_tier_basic_rpm: int = Field(default=30, env="CLIENT_TIER_BASIC_RPM")
    client_tier_basic_daily_bytes: int = Field(
        default=1_073_741_824, env="CLIENT_TIER_BASIC_DAILY_BYTES"
    )  # 1GB
    client_tier_basic_concurrent: int = Field(default=2, env="CLIENT_TIER_BASIC_CONCURRENT")

    # Standard tier
    client_tier_standard_rpm: int = Field(default=120, env="CLIENT_TIER_STANDARD_RPM")
    client_tier_standard_daily_bytes: int = Field(
        default=10_737_418_240, env="CLIENT_TIER_STANDARD_DAILY_BYTES"
    )  # 10GB
    client_tier_standard_concurrent: int = Field(default=10, env="CLIENT_TIER_STANDARD_CONCURRENT")

    # Premium tier
    client_tier_premium_rpm: int = Field(default=600, env="CLIENT_TIER_PREMIUM_RPM")
    client_tier_premium_daily_bytes: int = Field(
        default=107_374_182_400, env="CLIENT_TIER_PREMIUM_DAILY_BYTES"
    )  # 100GB
    client_tier_premium_concurrent: int = Field(default=50, env="CLIENT_TIER_PREMIUM_CONCURRENT")

    # ==========================================================================
    # CORS Settings
    # ==========================================================================
    # Allowed origins for CORS (comma-separated, use "*" for all - NOT RECOMMENDED for production)
    cors_allowed_origins: str = Field(default="", env="CORS_ALLOWED_ORIGINS")

    # Allow credentials (cookies, authorization headers)
    cors_allow_credentials: bool = Field(default=False, env="CORS_ALLOW_CREDENTIALS")

    # Allowed HTTP methods (comma-separated)
    cors_allowed_methods: str = Field(
        default="GET,POST,PUT,DELETE,OPTIONS", env="CORS_ALLOWED_METHODS"
    )

    # Allowed HTTP headers (comma-separated)
    cors_allowed_headers: str = Field(default="*", env="CORS_ALLOWED_HEADERS")

    # ==========================================================================
    # Compliance / Audit Settings
    # ==========================================================================
    # Enable audit event publishing to BeamCore
    audit_enabled: bool = Field(default=True, env="AUDIT_ENABLED")

    # Redis URL for audit event queue (same Redis as BeamCore consumes from)
    audit_redis_url: Optional[str] = Field(default=None, env="AUDIT_REDIS_URL")

    # Redis stream name for audit events
    audit_stream: str = Field(default="audit:events", env="AUDIT_STREAM")

    # Source identifier for audit events
    audit_source: str = Field(default="datapipe_subnet", env="AUDIT_SOURCE")

    class Config:
        env_file = str(_WORKSPACE_ROOT / ".env")
        extra = "ignore"

    def get_pre_approved_hotkeys(self) -> List[str]:
        """Parse pre-approved client hotkeys from comma-separated string."""
        if not self.client_pre_approved_hotkeys:
            return []
        return [h.strip() for h in self.client_pre_approved_hotkeys.split(",") if h.strip()]

    def get_client_admin_hotkeys(self) -> List[str]:
        """Parse client admin hotkeys from comma-separated string."""
        admins = []
        if self.client_admin_hotkeys:
            admins.extend([h.strip() for h in self.client_admin_hotkeys.split(",") if h.strip()])
        return admins

    def get_subnet_auth_whitelist(self) -> set:
        """Parse subnet auth whitelist from comma-separated string."""
        if not self.subnet_auth_whitelist:
            return set()
        return {h.strip() for h in self.subnet_auth_whitelist.split(",") if h.strip()}

    def get_tier_config(self, tier: str) -> dict:
        """
        Get configuration for a specific tier.

        Args:
            tier: "basic", "standard", or "premium"

        Returns:
            Dict with rpm, daily_bytes, concurrent limits
        """
        tier_configs = {
            "basic": {
                "rate_limit_rpm": self.client_tier_basic_rpm,
                "daily_transfer_limit_bytes": self.client_tier_basic_daily_bytes,
                "max_concurrent_transfers": self.client_tier_basic_concurrent,
            },
            "standard": {
                "rate_limit_rpm": self.client_tier_standard_rpm,
                "daily_transfer_limit_bytes": self.client_tier_standard_daily_bytes,
                "max_concurrent_transfers": self.client_tier_standard_concurrent,
            },
            "premium": {
                "rate_limit_rpm": self.client_tier_premium_rpm,
                "daily_transfer_limit_bytes": self.client_tier_premium_daily_bytes,
                "max_concurrent_transfers": self.client_tier_premium_concurrent,
            },
        }
        return tier_configs.get(tier, tier_configs["basic"])

    def get_cors_origins(self) -> List[str]:
        """
        Parse CORS allowed origins from comma-separated string.

        Returns empty list if not configured (CORS disabled).
        """
        if not self.cors_allowed_origins:
            return []
        return [o.strip() for o in self.cors_allowed_origins.split(",") if o.strip()]

    def get_cors_methods(self) -> List[str]:
        """Parse CORS allowed methods from comma-separated string."""
        return [m.strip() for m in self.cors_allowed_methods.split(",") if m.strip()]

    def get_cors_headers(self) -> List[str]:
        """Parse CORS allowed headers from comma-separated string."""
        if self.cors_allowed_headers == "*":
            return ["*"]
        return [h.strip() for h in self.cors_allowed_headers.split(",") if h.strip()]


@lru_cache
def get_settings() -> OrchestratorSettings:
    """Get cached settings instance."""
    _load_workspace_env()
    return OrchestratorSettings()
