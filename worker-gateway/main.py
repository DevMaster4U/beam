"""
Beam Worker Gateway — dedicated worker gateway for Option 1 (orchestrator-direct).

Run:
    cd worker-gateway
    python main.py
"""

from __future__ import annotations

import logging
import sys

import uvicorn

from gateway.config import GatewaySettings
from gateway.app import create_app


def configure_logging(level: str) -> None:
    logging.basicConfig(
        level=getattr(logging, level, logging.INFO),
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )


def main() -> None:
    try:
        settings = GatewaySettings.from_env()
    except ValueError as exc:
        print(f"Configuration error: {exc}", file=sys.stderr)
        sys.exit(1)

    configure_logging(settings.log_level)
    app = create_app(settings)

    print("Beam Worker Gateway")
    print("=" * 40)
    print(f"Listening on {settings.host}:{settings.port}")
    print(f"Worker endpoint:  ws://{settings.host}:{settings.port}/ws/{{worker_id}}")
    print(f"Control endpoint: ws://{settings.host}:{settings.port}/control")
    print()

    uvicorn.run(
        app,
        host=settings.host,
        port=settings.port,
        log_level=settings.log_level.lower(),
        ws_ping_interval=settings.ws_ping_interval,
        ws_ping_timeout=settings.ws_ping_timeout,
    )


if __name__ == "__main__":
    main()
