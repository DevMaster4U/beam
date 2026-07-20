"""Connected miner WebSocket hub and range-coverage broadcast."""

from __future__ import annotations

import asyncio
import contextlib
import logging
from dataclasses import dataclass, field
from typing import Any, Optional

from fastapi import WebSocket

from storage import range_coverage_snapshot

logger = logging.getLogger(__name__)


@dataclass
class MinerConnection:
    miner_id: str
    websocket: WebSocket
    send_lock: asyncio.Lock = field(default_factory=asyncio.Lock)


class MinerConnectionHub:
    """Track miner WS connections and broadcast range_data coverage updates."""

    def __init__(self) -> None:
        self._connections: dict[str, MinerConnection] = {}

    @property
    def connection_count(self) -> int:
        return len(self._connections)

    def list_miners(self) -> list[str]:
        return sorted(self._connections.keys())

    async def connect(self, miner_id: str, websocket: WebSocket) -> None:
        old = self._connections.pop(miner_id, None)
        if old is not None:
            with contextlib.suppress(Exception):
                await old.websocket.close()
        self._connections[miner_id] = MinerConnection(miner_id=miner_id, websocket=websocket)
        snapshot = await asyncio.to_thread(range_coverage_snapshot)
        await self._send(
            miner_id,
            {
                "type": "range_snapshot",
                "sources": snapshot.get("sources") or [],
                "updated_at": snapshot.get("updated_at"),
            },
        )
        await self._send(miner_id, {"type": "sync_done"})
        logger.info(
            "Miner connected miner_id=%s sources=%d total_connections=%d sync_done=sent",
            miner_id,
            int(snapshot.get("source_count") or 0),
            self.connection_count,
        )

    async def disconnect(self, miner_id: str) -> None:
        if miner_id in self._connections:
            del self._connections[miner_id]
            logger.info(
                "Miner disconnected miner_id=%s total_connections=%d",
                miner_id,
                self.connection_count,
            )

    async def handle_range_update(self, miner_id: str, message: dict[str, Any]) -> None:
        """Announce coverage after a miner uploaded bytes (hash/etag not synced)."""
        source_url = str(message.get("source_url") or "").strip()
        try:
            start = int(message.get("start"))
            end = int(message.get("end"))
        except (TypeError, ValueError):
            await self._send(
                miner_id,
                {"type": "error", "detail": "range_update requires source_url, start, end"},
            )
            return
        if not source_url or end < start:
            await self._send(
                miner_id,
                {"type": "error", "detail": "invalid range_update"},
            )
            return
        broadcast = {
            "type": "range_broadcast",
            "source_url": source_url,
            "start": start,
            "end": end,
            "source_miner": miner_id,
        }
        await self.broadcast(broadcast, exclude_miner=miner_id)
        await self._send(
            miner_id,
            {
                "type": "range_update_ack",
                "source_url": source_url,
                "start": start,
                "end": end,
            },
        )
        logger.info(
            "Range update from miner=%s src=%s range=%s-%s broadcast_to=%d",
            miner_id,
            source_url[:96],
            start,
            end,
            max(0, self.connection_count - 1),
        )

    async def handle_cache_update(self, miner_id: str, message: dict[str, Any]) -> None:
        """Backward-compat: map legacy cache_update key to range_broadcast."""
        key = str(message.get("key") or "").strip()
        from neurons.common.byte_range_store import parse_cache_key_range

        parsed = parse_cache_key_range(key)
        if parsed is None:
            await self._send(
                miner_id,
                {"type": "error", "detail": "cache_update key must be source|start|end"},
            )
            return
        source_url, start, end = parsed
        await self.handle_range_update(
            miner_id,
            {"source_url": source_url, "start": start, "end": end},
        )

    async def broadcast_range(
        self,
        *,
        source_url: str,
        start: int,
        end: int,
        source_miner: str = "http",
        exclude_miner: Optional[str] = None,
    ) -> None:
        await self.broadcast(
            {
                "type": "range_broadcast",
                "source_url": source_url,
                "start": int(start),
                "end": int(end),
                "source_miner": source_miner,
            },
            exclude_miner=exclude_miner,
        )

    async def broadcast(
        self,
        message: dict[str, Any],
        *,
        exclude_miner: Optional[str] = None,
    ) -> None:
        targets = [
            miner_id
            for miner_id in list(self._connections.keys())
            if miner_id != exclude_miner
        ]
        if not targets:
            return
        results = await asyncio.gather(
            *(self._send(miner_id, message) for miner_id in targets),
            return_exceptions=True,
        )
        for miner_id, result in zip(targets, results):
            if isinstance(result, Exception):
                logger.warning(
                    "Broadcast to miner=%s failed: %s",
                    miner_id,
                    result,
                )

    async def _send(self, miner_id: str, message: dict[str, Any]) -> None:
        conn = self._connections.get(miner_id)
        if conn is None:
            return
        async with conn.send_lock:
            try:
                await conn.websocket.send_json(message)
            except Exception as exc:
                logger.warning("Failed to send to miner=%s: %s", miner_id, exc)
                await self.disconnect(miner_id)


miner_hub = MinerConnectionHub()
