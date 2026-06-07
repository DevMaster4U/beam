"""Worker and orchestrator control session registry."""

from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass, field
from typing import Any, Optional

from fastapi import WebSocket

logger = logging.getLogger(__name__)


@dataclass
class WorkerSession:
    worker_id: str
    websocket: WebSocket
    connected_at: float = field(default_factory=time.time)
    bandwidth_mbps: float = 0.0
    tasks_active: int = 0
    send_lock: asyncio.Lock = field(default_factory=asyncio.Lock)


class SessionRegistry:
    """Tracks connected workers and the orchestrator control channel."""

    def __init__(self) -> None:
        self._workers: dict[str, WorkerSession] = {}
        self._control: Optional[WebSocket] = None
        self._control_lock = asyncio.Lock()

    def get_worker(self, worker_id: str) -> Optional[WorkerSession]:
        return self._workers.get(worker_id)

    def list_workers(self) -> list[dict[str, Any]]:
        return [
            {
                "worker_id": session.worker_id,
                "bandwidth_mbps": session.bandwidth_mbps,
                "tasks_active": session.tasks_active,
                "connected_at": session.connected_at,
            }
            for session in self._workers.values()
        ]

    async def register_worker(self, worker_id: str, websocket: WebSocket) -> WorkerSession:
        existing = self._workers.get(worker_id)
        if existing is not None:
            logger.info("Displacing existing session for worker %s", worker_id[:20])
            try:
                await existing.websocket.send_json(
                    {"type": "session_displaced", "reason": "new_connection"}
                )
            except Exception:
                pass
            try:
                await existing.websocket.close(code=4000, reason="session_displaced")
            except Exception:
                pass

        session = WorkerSession(worker_id=worker_id, websocket=websocket)
        self._workers[worker_id] = session
        logger.info("Worker connected: %s (total=%s)", worker_id[:20], len(self._workers))
        return session

    async def unregister_worker(self, worker_id: str) -> None:
        if worker_id in self._workers:
            del self._workers[worker_id]
            logger.info("Worker disconnected: %s (total=%s)", worker_id[:20], len(self._workers))

    def set_control(self, websocket: WebSocket) -> None:
        self._control = websocket
        logger.info("Orchestrator control channel connected")

    def clear_control(self, websocket: WebSocket) -> None:
        if self._control is websocket:
            self._control = None
            logger.info("Orchestrator control channel disconnected")

    @property
    def control_connected(self) -> bool:
        return self._control is not None

    async def notify_control(self, message: dict[str, Any]) -> bool:
        async with self._control_lock:
            if self._control is None:
                return False
            try:
                await self._control.send_json(message)
                return True
            except Exception as exc:
                logger.warning("Failed to notify control channel: %s", exc)
                return False

    async def send_to_worker(self, worker_id: str, message: dict[str, Any]) -> bool:
        session = self._workers.get(worker_id)
        if session is None:
            logger.warning("Worker %s not connected; cannot deliver message", worker_id[:20])
            return False

        async with session.send_lock:
            try:
                await session.websocket.send_json(message)
                return True
            except Exception as exc:
                logger.warning("Failed to send to worker %s: %s", worker_id[:20], exc)
                return False
