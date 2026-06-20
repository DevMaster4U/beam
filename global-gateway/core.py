"""Shared worker pool and orchestrator routing for the global gateway."""

from __future__ import annotations

import asyncio
import json
import logging
from dataclasses import dataclass, field
from typing import Any, Dict, Optional

logger = logging.getLogger(__name__)


@dataclass
class GlobalGatewayState:
    max_workers: int = 100
    worker_sessions: Dict[str, Any] = field(default_factory=dict)
    orchestrator_sessions: Dict[str, Any] = field(default_factory=dict)
    worker_cursor: int = 0
    offer_routes: Dict[str, str] = field(default_factory=dict)
    task_routes: Dict[str, str] = field(default_factory=dict)

    def worker_count(self) -> int:
        return len(self.worker_sessions)

    def orchestrator_count(self) -> int:
        return len(self.orchestrator_sessions)

    def list_worker_ids(self) -> list[str]:
        return list(self.worker_sessions.keys())

    def get_workers_round_robin(self, n: int = 1) -> list[str]:
        ids = self.list_worker_ids()
        if not ids:
            return []
        selected: list[str] = []
        for _ in range(min(n, len(ids))):
            selected.append(ids[self.worker_cursor % len(ids)])
            self.worker_cursor += 1
        return selected

    def register_route(
        self,
        orchestrator_hotkey: str,
        worker_id: str,
        offer_id: Optional[str],
        task_id: Optional[str],
    ) -> None:
        if offer_id:
            self.offer_routes[str(offer_id)] = orchestrator_hotkey
        if task_id:
            self.task_routes[str(task_id)] = orchestrator_hotkey
        logger.debug(
            "route registered orch=%s worker=%s offer=%s task=%s",
            orchestrator_hotkey[:16],
            worker_id,
            offer_id,
            task_id,
        )

    def resolve_orchestrator_hotkey(self, message: dict) -> Optional[str]:
        offer_id = message.get("offer_id")
        if offer_id and str(offer_id) in self.offer_routes:
            return self.offer_routes[str(offer_id)]
        task_id = message.get("task_id")
        if task_id and str(task_id) in self.task_routes:
            return self.task_routes[str(task_id)]
        return None

    async def send_json(self, ws: Any, payload: dict) -> bool:
        try:
            await ws.send_text(json.dumps(payload))
            return True
        except Exception as exc:
            logger.warning("websocket send failed: %s", exc)
            return False

    async def notify_pool_status(self) -> None:
        payload = {
            "type": "pool_status",
            "worker_count": self.worker_count(),
            "workers": [{"worker_id": wid} for wid in self.list_worker_ids()],
        }
        for hotkey, ws in list(self.orchestrator_sessions.items()):
            if not await self.send_json(ws, payload):
                self.orchestrator_sessions.pop(hotkey, None)

    async def forward_to_orchestrator(self, orchestrator_hotkey: str, message: dict) -> bool:
        ws = self.orchestrator_sessions.get(orchestrator_hotkey)
        if ws is None:
            logger.warning("no orchestrator session for hotkey %s", orchestrator_hotkey)
            return False
        payload = {
            "type": "from_worker",
            "worker_id": message.get("worker_id"),
            "message": message,
        }
        return await self.send_json(ws, payload)

    async def send_to_worker(self, worker_id: str, payload: dict) -> bool:
        ws = self.worker_sessions.get(worker_id)
        if ws is None:
            logger.warning("worker %s not connected", worker_id)
            return False
        return await self.send_json(ws, payload)


gateway_state = GlobalGatewayState()
