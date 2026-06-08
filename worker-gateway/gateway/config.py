"""Gateway configuration."""

import os
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv

_WORKSPACE_ROOT = Path(__file__).resolve().parents[2]
_ENV_FILE = _WORKSPACE_ROOT / ".env"


def _load_env_file() -> None:
    if _ENV_FILE.exists():
        load_dotenv(_ENV_FILE, override=False)


def _get_control_secret() -> str:
    return (
        os.environ.get("GATEWAY_CONTROL_SECRET", "").strip()
        or os.environ.get("WORKER_GATEWAY_CONTROL_SECRET", "").strip()
    )


def _get_worker_secret() -> str:
    return (
        os.environ.get("GATEWAY_WORKER_SECRET", "").strip()
        or os.environ.get("WORKER_GATEWAY_WORKER_SECRET", "").strip()
    )


@dataclass(frozen=True)
class GatewaySettings:
    host: str
    port: int
    control_secret: str
    worker_secret: str
    ws_ping_interval: float
    ws_ping_timeout: float
    log_level: str

    @classmethod
    def from_env(cls) -> "GatewaySettings":
        _load_env_file()

        control_secret = _get_control_secret()
        if not control_secret:
            raise ValueError(
                "GATEWAY_CONTROL_SECRET or WORKER_GATEWAY_CONTROL_SECRET is required"
            )

        worker_secret = _get_worker_secret()
        if not worker_secret:
            raise ValueError(
                "GATEWAY_WORKER_SECRET or WORKER_GATEWAY_WORKER_SECRET is required"
            )

        return cls(
            host=os.environ.get("GATEWAY_HOST", "0.0.0.0"),
            port=int(os.environ.get("GATEWAY_PORT", "8001")),
            control_secret=control_secret,
            worker_secret=worker_secret,
            ws_ping_interval=float(os.environ.get("GATEWAY_WS_PING_INTERVAL", "30")),
            ws_ping_timeout=float(os.environ.get("GATEWAY_WS_PING_TIMEOUT", "10")),
            log_level=os.environ.get("LOG_LEVEL", "INFO").upper(),
        )
