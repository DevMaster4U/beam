"""WebSocket client: connect to control-server, sync and broadcast predefined ETag cache."""

from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import os
from typing import Any, Callable, Optional

from neurons.common.control_client import get_control_server_config

logger = logging.getLogger(__name__)

MergeHandler = Callable[[str, str, str], None]
SnapshotHandler = Callable[[dict[str, dict[str, str]]], None]

_merge_handler: Optional[MergeHandler] = None
_snapshot_handler: Optional[SnapshotHandler] = None
_update_queue: Optional[asyncio.Queue] = None
_client_task: Optional[asyncio.Task] = None
_stop_event: Optional[asyncio.Event] = None


def register_cache_merge_handler(handler: MergeHandler) -> None:
    global _merge_handler
    _merge_handler = handler


def register_cache_snapshot_handler(handler: SnapshotHandler) -> None:
    global _snapshot_handler
    _snapshot_handler = handler


try:
    import websockets
    from websockets.exceptions import ConnectionClosed

    WEBSOCKETS_AVAILABLE = True
except ImportError:
    WEBSOCKETS_AVAILABLE = False


def schedule_cache_update(key: str, chunk_hash: str, etag: str) -> None:
    if _update_queue is None or not chunk_hash or not key:
        return
    try:
        _update_queue.put_nowait(
            {
                "type": "cache_update",
                "key": key,
                "chunk_hash": chunk_hash,
                "etag": etag or "",
            }
        )
    except asyncio.QueueFull:
        logger.warning("Control WS update queue full; dropping cache_update key=%s", key[:96])


def _apply_broadcast(key: str, chunk_hash: str, etag: str) -> None:
    if not _merge_handler:
        return
    try:
        _merge_handler(key, chunk_hash, etag)
    except Exception as exc:
        logger.warning("Cache merge handler failed key=%s err=%s", key[:96], exc)


def _apply_snapshot(entries: dict[str, Any]) -> None:
    normalized: dict[str, dict[str, str]] = {}
    for key, item in (entries or {}).items():
        if not isinstance(item, dict):
            continue
        chunk_hash = str(item.get("chunk_hash") or item.get("hash") or "").strip()
        if not chunk_hash:
            continue
        normalized[str(key)] = {
            "chunk_hash": chunk_hash,
            "etag": str(item.get("etag") or ""),
        }
    if _snapshot_handler:
        _snapshot_handler(normalized)
        return
    for key, item in normalized.items():
        _apply_broadcast(key, item["chunk_hash"], item.get("etag") or "")


async def _send_pending_updates(ws) -> None:
    assert _update_queue is not None
    while True:
        try:
            message = _update_queue.get_nowait()
        except asyncio.QueueEmpty:
            break
        await ws.send(json.dumps(message))


async def _client_loop() -> None:
    assert _stop_event is not None
    assert _update_queue is not None

    cfg = get_control_server_config()
    ws_url = cfg.ws_url
    reconnect_delay = float(os.environ.get("CONTROL_SERVER_WS_RECONNECT_SEC", "3.0"))

    while not _stop_event.is_set():
        cfg = get_control_server_config()
        ws_url = cfg.ws_url
        if not cfg.cache_ws_enabled:
            await asyncio.sleep(reconnect_delay)
            continue
        if not WEBSOCKETS_AVAILABLE:
            logger.error("websockets package required for control-server cache sync")
            return

        try:
            async with websockets.connect(
                ws_url,
                ping_interval=30,
                ping_timeout=45,
                max_size=16 * 1024 * 1024,
            ) as ws:
                await ws.send(
                    json.dumps(
                        {
                            "type": "hello",
                            "secret": cfg.secret,
                            "miner_id": cfg.miner_id or "miner",
                        }
                    )
                )
                logger.info(
                    "Connected to control-server WS miner_id=%s url=%s",
                    cfg.miner_id or "miner",
                    ws_url,
                )
                await _send_pending_updates(ws)

                while not _stop_event.is_set():
                    try:
                        raw = await asyncio.wait_for(ws.recv(), timeout=1.0)
                    except asyncio.TimeoutError:
                        await _send_pending_updates(ws)
                        continue

                    message = json.loads(raw)
                    msg_type = str(message.get("type") or "")
                    if msg_type == "cache_snapshot":
                        _apply_snapshot(message.get("entries") or {})
                        logger.info(
                            "Control-server cache snapshot merged entries=%d",
                            len(message.get("entries") or {}),
                        )
                    elif msg_type == "cache_broadcast":
                        key = str(message.get("key") or "")
                        chunk_hash = str(message.get("chunk_hash") or message.get("hash") or "")
                        etag = str(message.get("etag") or "")
                        if key and chunk_hash:
                            _apply_broadcast(key, chunk_hash, etag)
                            source_miner = str(message.get("source_miner") or "?")
                            chunk_index = message.get("chunk_index")
                            idx_label = (
                                f" chunk_index={chunk_index}"
                                if chunk_index is not None
                                else ""
                            )
                            logger.info(
                                "Control-server cache broadcast merged miner_id=%s "
                                "source_miner=%s key=%s hash=%s%s",
                                cfg.miner_id or "miner",
                                source_miner,
                                key[:96],
                                chunk_hash[:16],
                                idx_label,
                            )
                    elif msg_type == "cache_update_ack":
                        logger.info(
                            "Control-server cache push ack miner_id=%s key=%s",
                            cfg.miner_id or "miner",
                            str(message.get("key") or "")[:96],
                        )
                    elif msg_type == "error":
                        logger.warning("Control-server error: %s", message.get("detail"))
        except ConnectionClosed as exc:
            logger.warning("Control-server WS closed: %s", exc)
        except Exception as exc:
            logger.warning("Control-server WS error: %s", exc)

        if _stop_event.is_set():
            break
        await asyncio.sleep(reconnect_delay)


async def start_control_ws_client() -> None:
    global _update_queue, _client_task, _stop_event
    cfg = get_control_server_config()
    if not cfg.cache_ws_enabled:
        logger.info(
            "Control-server WS cache sync disabled (set CONTROL_SERVER_WS_URL=ws://host:port/ws/miners)"
        )
        return
    if _client_task is not None and not _client_task.done():
        return

    _update_queue = asyncio.Queue(maxsize=1000)
    _stop_event = asyncio.Event()
    _client_task = asyncio.create_task(_client_loop(), name="control-ws-client")
    logger.info("Control-server WS client started miner_id=%s", cfg.miner_id or "miner")


async def stop_control_ws_client() -> None:
    global _client_task, _stop_event, _update_queue
    if _stop_event is not None:
        _stop_event.set()
    if _client_task is not None:
        _client_task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await _client_task
    _client_task = None
    _stop_event = None
    _update_queue = None
