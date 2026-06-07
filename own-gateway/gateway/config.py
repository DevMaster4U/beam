"""Gateway configuration."""

import os
from dataclasses import dataclass


@dataclass(frozen=True)
class GatewaySettings:
    host: str
    port: int
    control_secret: str
    ws_ping_interval: float
    ws_ping_timeout: float
    log_level: str

    @classmethod
    def from_env(cls) -> "GatewaySettings":
        secret = os.environ.get("GATEWAY_CONTROL_SECRET", "").strip()
        if not secret:
            raise ValueError("GATEWAY_CONTROL_SECRET is required")

        return cls(
            host=os.environ.get("GATEWAY_HOST", "0.0.0.0"),
            port=int(os.environ.get("GATEWAY_PORT", "8001")),
            control_secret=secret,
            ws_ping_interval=float(os.environ.get("GATEWAY_WS_PING_INTERVAL", "30")),
            ws_ping_timeout=float(os.environ.get("GATEWAY_WS_PING_TIMEOUT", "10")),
            log_level=os.environ.get("LOG_LEVEL", "INFO").upper(),
        )
