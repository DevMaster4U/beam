"""FastAPI application for the dedicated worker gateway."""

from __future__ import annotations

import logging

from fastapi import FastAPI, WebSocket

from .config import GatewaySettings
from .control_handler import handle_control_websocket
from .sessions import SessionRegistry
from .worker_handler import handle_worker_websocket

logger = logging.getLogger(__name__)


def create_app(settings: GatewaySettings) -> FastAPI:
    registry = SessionRegistry()
    app = FastAPI(
        title="Beam Worker Gateway",
        description="Dedicated worker gateway for orchestrator-direct (Option 1) topology",
        version="0.1.0",
    )

    @app.get("/health")
    async def health() -> dict:
        workers = registry.list_workers()
        return {
            "status": "healthy",
            "gateway_mode": "orch_owned",
            "workers_connected": len(workers),
            "control_connected": registry.control_connected,
        }

    @app.websocket("/ws/{worker_id}")
    async def worker_ws(websocket: WebSocket, worker_id: str) -> None:
        await handle_worker_websocket(
            websocket, worker_id, registry, settings.worker_secret
        )

    @app.websocket("/control")
    async def control_ws(websocket: WebSocket) -> None:
        await handle_control_websocket(websocket, registry, settings.control_secret)

    return app
