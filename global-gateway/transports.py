"""Orchestrator control channel transports (WebSocket and local IPC)."""

from __future__ import annotations

import json
import logging
from typing import Any, Optional

from fastapi import WebSocket

logger = logging.getLogger(__name__)


class OrchestratorChannel:
    """Outbound control channel to a connected orchestrator."""

    transport: str = "unknown"

    async def send(self, payload: dict) -> bool:
        raise NotImplementedError


class WebSocketOrchestratorChannel(OrchestratorChannel):
    transport = "websocket"

    def __init__(self, websocket: WebSocket) -> None:
        self._websocket = websocket

    async def send(self, payload: dict) -> bool:
        try:
            await self._websocket.send_text(json.dumps(payload))
            return True
        except Exception as exc:
            logger.warning("orchestrator websocket send failed: %s", exc)
            return False


class IpcOrchestratorChannel(OrchestratorChannel):
    transport = "ipc"

    def __init__(self, writer: Any, write_lock: Any) -> None:
        self._writer = writer
        self._write_lock = write_lock

    async def send(self, payload: dict) -> bool:
        line = json.dumps(payload, separators=(",", ":")) + "\n"
        try:
            async with self._write_lock:
                self._writer.write(line.encode("utf-8"))
                await self._writer.drain()
            return True
        except Exception as exc:
            logger.warning("orchestrator ipc send failed: %s", exc)
            return False
