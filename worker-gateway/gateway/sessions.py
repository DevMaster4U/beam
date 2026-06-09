"""Worker and orchestrator control session registry."""

from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass, field
from typing import Any, Optional

from fastapi import WebSocket

logger = logging.getLogger(__name__)


def _client_ip(websocket: WebSocket) -> str:
    forwarded = websocket.headers.get("x-forwarded-for", "").strip()
    if forwarded:
        return forwarded.split(",")[0].strip()
    real_ip = websocket.headers.get("x-real-ip", "").strip()
    if real_ip:
        return real_ip
    client = websocket.client
    if client is None:
        return "unknown"
    host = getattr(client, "host", None)
    return str(host) if host else "unknown"


def _hotkey_label(hotkey: str) -> str:
    return hotkey or "pending"


@dataclass
class WorkerSession:
    worker_id: str
    websocket: WebSocket
    connected_at: float = field(default_factory=time.time)
    client_ip: str = ""
    hotkey: str = ""
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
                "client_ip": session.client_ip,
                "hotkey": session.hotkey,
                "bandwidth_mbps": session.bandwidth_mbps,
                "tasks_active": session.tasks_active,
                "connected_at": session.connected_at,
            }
            for session in self._workers.values()
        ]

    async def register_worker(self, worker_id: str, websocket: WebSocket) -> WorkerSession:
        existing = self._workers.get(worker_id)
        if existing is not None:
            logger.info(
                "Displacing existing session for worker %s ip=%s hotkey=%s",
                worker_id,
                existing.client_ip or "unknown",
                _hotkey_label(existing.hotkey),
            )
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

        client_ip = _client_ip(websocket)
        session = WorkerSession(worker_id=worker_id, websocket=websocket, client_ip=client_ip)
        self._workers[worker_id] = session
        logger.info(
            "Worker connected: %s ip=%s hotkey=%s (total=%s)",
            worker_id,
            client_ip,
            _hotkey_label(session.hotkey),
            len(self._workers),
        )
        return session

    def update_worker_identity(self, worker_id: str, *, hotkey: str = "") -> None:
        session = self._workers.get(worker_id)
        if session is None or not hotkey:
            return
        if session.hotkey == hotkey:
            return
        session.hotkey = hotkey
        logger.info(
            "Worker identity: %s ip=%s hotkey=%s",
            worker_id,
            session.client_ip or "unknown",
            _hotkey_label(hotkey),
        )

    async def unregister_worker(self, worker_id: str) -> None:
        if worker_id in self._workers:
            session = self._workers[worker_id]
            del self._workers[worker_id]
            logger.info(
                "Worker disconnected: %s ip=%s hotkey=%s (total=%s)",
                worker_id,
                session.client_ip or "unknown",
                _hotkey_label(session.hotkey),
                len(self._workers),
            )

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
            logger.warning("Worker %s not connected; cannot deliver message", worker_id)
            return False

        async with session.send_lock:
            try:
                await session.websocket.send_json(message)
                return True
            except Exception as exc:
                logger.warning("Failed to send to worker %s: %s", worker_id, exc)
                return False
