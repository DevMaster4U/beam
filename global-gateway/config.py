"""Global worker gateway configuration."""

import os
from functools import lru_cache
from typing import Optional

from pydantic import AliasChoices, Field
from pydantic_settings import BaseSettings


class GatewaySettings(BaseSettings):
    gateway_host: str = Field(default="0.0.0.0", env="GATEWAY_HOST")
    gateway_port: int = Field(default=8001, env="GATEWAY_PORT")

    core_server_url: str = Field(default="https://beamcore.b1m.ai", env="CORE_SERVER_URL")

    worker_secret: str = Field(
        ...,
        validation_alias=AliasChoices(
            "WORKER_GATEWAY_SECRET",
            "WORKER_GATEWAY_WORKER_SECRET",
            "GATEWAY_WORKER_SECRET",
        ),
    )
    orchestrator_secret: str = Field(
        ...,
        validation_alias=AliasChoices(
            "ORCHESTRATOR_GATEWAY_SECRET",
            "ORCHESTRATOR_WORKER_GATEWAY_SECRET",
            "GATEWAY_ORCHESTRATOR_SECRET",
        ),
    )

    max_workers: int = Field(default=100, env="GATEWAY_MAX_WORKERS")
    worker_history_max: int = Field(default=100, env="GATEWAY_WORKER_HISTORY_MAX")
    log_level: str = Field(default="INFO", env="LOG_LEVEL")

    ws_ping_interval: float = Field(default=30.0, env="GATEWAY_WS_PING_INTERVAL")
    ws_ping_timeout: float = Field(default=45.0, env="GATEWAY_WS_PING_TIMEOUT")

    weight_trust: float = Field(default=0.30, env="WEIGHT_TRUST")
    weight_latency: float = Field(default=0.25, env="WEIGHT_LATENCY")
    weight_load: float = Field(default=0.20, env="WEIGHT_LOAD")
    weight_bandwidth: float = Field(default=0.15, env="WEIGHT_BANDWIDTH")
    weight_success: float = Field(default=0.10, env="WEIGHT_SUCCESS")

    ipc_socket_path: str = Field(
        default="run/pool-coordinator.sock",
        env="GATEWAY_IPC_SOCKET_PATH",
    )
    ipc_enabled: bool = Field(default=True, env="GATEWAY_IPC_ENABLED")

    class Config:
        env_file = ".env"
        extra = "ignore"


@lru_cache
def get_settings() -> GatewaySettings:
    return GatewaySettings()
