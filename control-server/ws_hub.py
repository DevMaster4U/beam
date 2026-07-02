"""Connected miner WebSocket hub and cache broadcast."""

from __future__ import annotations

import asyncio
import contextlib
import logging
from dataclasses import dataclass, field
from typing import Any, Optional

from fastapi import WebSocket
from starlette.websockets import WebSocketDisconnect

from storage import load_predefined_etag_cache, upsert_predefined_etag_entry

logger = logging.getLogger(__name__)


@dataclass
class MinerConnection:
    miner_id: str
    websocket: WebSocket
    send_lock: asyncio.Lock = field(default_factory=asyncio.Lock)


class MinerConnectionHub:
    """Track miner WS connections and broadcast cache updates."""

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
        snapshot = load_predefined_etag_cache()
        await self._send(
            miner_id,
            {
                "type": "cache_snapshot",
                "entries": snapshot.get("entries") or {},
                "updated_at": snapshot.get("updated_at"),
            },
        )
        logger.info(
            "Miner connected miner_id=%s entries=%d total_connections=%d",
            miner_id,
            len(snapshot.get("entries") or {}),
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

    async def handle_cache_update(self, miner_id: str, message: dict[str, Any]) -> None:
        key = str(message.get("key") or "").strip()
        chunk_hash = str(message.get("chunk_hash") or message.get("hash") or "").strip()
        etag = str(message.get("etag") or "").strip()
        if not key or not chunk_hash:
            await self._send(
                miner_id,
                {"type": "error", "detail": "cache_update requires key and chunk_hash"},
            )
            return

        entry = await asyncio.to_thread(
            upsert_predefined_etag_entry, key, chunk_hash, etag
        )
        broadcast = {
            "type": "cache_broadcast",
            "key": key,
            "chunk_hash": entry["chunk_hash"],
            "etag": entry["etag"],
            "source_miner": miner_id,
        }
        if "chunk_index" in entry:
            broadcast["chunk_index"] = entry["chunk_index"]
        await self.broadcast(broadcast, exclude_miner=miner_id)
        await self._send(miner_id, {"type": "cache_update_ack", "key": key})
        logger.info(
            "Cache update from miner=%s key=%s broadcast_to=%d",
            miner_id,
            key[:96],
            max(0, self.connection_count - 1),
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
