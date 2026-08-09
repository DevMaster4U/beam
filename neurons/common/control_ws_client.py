"""WebSocket client: sync range_data coverage from control-server (segments.json)."""

from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import os
from typing import Any, Callable, Optional

from neurons.common.control_client import get_control_server_config

logger = logging.getLogger(__name__)

# sources: list[{source_url, segments:[{start,end}]}]
RangeSnapshotHandler = Callable[[list[dict[str, Any]]], None]
# source_url, start, end
RangeBroadcastHandler = Callable[[str, int, int], None]
SyncDoneHandler = Callable[[], None]

_range_snapshot_handler: Optional[RangeSnapshotHandler] = None
_range_broadcast_handler: Optional[RangeBroadcastHandler] = None
_sync_done_handler: Optional[SyncDoneHandler] = None
_cache_sync_done_event: Optional[asyncio.Event] = None
_update_queue: Optional[asyncio.Queue] = None
_client_task: Optional[asyncio.Task] = None
_stop_event: Optional[asyncio.Event] = None


def register_range_snapshot_handler(handler: RangeSnapshotHandler) -> None:
    global _range_snapshot_handler
    _range_snapshot_handler = handler


def register_range_broadcast_handler(handler: RangeBroadcastHandler) -> None:
    global _range_broadcast_handler
    _range_broadcast_handler = handler


def register_sync_done_handler(handler: SyncDoneHandler) -> None:
    global _sync_done_handler
    _sync_done_handler = handler


# Backward-compat aliases (old metadata handlers unused for sync).
def register_cache_merge_handler(handler: Callable[..., None]) -> None:
    return None


def register_cache_snapshot_handler(handler: Callable[..., None]) -> None:
    return None


def _mark_cache_sync_done() -> None:
    global _cache_sync_done_event
    if _cache_sync_done_event is not None and not _cache_sync_done_event.is_set():
        _cache_sync_done_event.set()


async def wait_for_cache_sync_done(timeout: Optional[float] = None) -> bool:
    """Block until control-server sends sync_done (or cache WS is disabled)."""
    cfg = get_control_server_config()
    if not cfg.cache_ws_enabled:
        return True
    global _cache_sync_done_event
    if _cache_sync_done_event is None:
        _cache_sync_done_event = asyncio.Event()
    if _cache_sync_done_event.is_set():
        return True
    if timeout is None:
        await _cache_sync_done_event.wait()
        return True
    try:
        await asyncio.wait_for(_cache_sync_done_event.wait(), timeout=timeout)
        return True
    except asyncio.TimeoutError:
        return False


try:
    import websockets
    from websockets.exceptions import ConnectionClosed

    WEBSOCKETS_AVAILABLE = True
except ImportError:
    WEBSOCKETS_AVAILABLE = False


def schedule_range_update(source_url: str, start: int, end: int) -> None:
    """Announce local coverage to control-server (bytes already uploaded via HTTP)."""
    if _update_queue is None or not source_url or end < start:
        return
    try:
        _update_queue.put_nowait(
            {
                "type": "range_update",
                "source_url": source_url,
                "start": int(start),
                "end": int(end),
            }
        )
    except asyncio.QueueFull:
        logger.warning(
            "Control WS update queue full; dropping range_update src=%s %s-%s",
            source_url[:96],
            start,
            end,
        )


def schedule_cache_update(key: str, chunk_hash: str, etag: str) -> None:
    """Legacy: map source|start|end key to range_update (hash/etag ignored)."""
    from neurons.common.byte_range_store import parse_cache_key_range

    parsed = parse_cache_key_range(key)
    if parsed is None:
        return
    source_url, start, end = parsed
    schedule_range_update(source_url, start, end)


def _apply_range_snapshot(sources: list[Any]) -> None:
    if not _range_snapshot_handler:
        return
    normalized: list[dict[str, Any]] = []
    for item in sources or []:
        if not isinstance(item, dict):
            continue
        source_url = str(item.get("source_url") or "").strip()
        if not source_url:
            continue
        segs_in = item.get("segments") or []
        segs: list[dict[str, int]] = []
        if isinstance(segs_in, list):
            for seg in segs_in:
                if not isinstance(seg, dict):
                    continue
                try:
                    segs.append({"start": int(seg["start"]), "end": int(seg["end"])})
                except (KeyError, TypeError, ValueError):
                    continue
        normalized.append({"source_url": source_url, "segments": segs})
    try:
        _range_snapshot_handler(normalized)
    except Exception as exc:
        logger.warning("Range snapshot handler failed: %s", exc)


def _apply_range_broadcast(source_url: str, start: int, end: int) -> None:
    if not _range_broadcast_handler:
        return
    try:
        _range_broadcast_handler(source_url, start, end)
    except Exception as exc:
        logger.warning(
            "Range broadcast handler failed src=%s %s-%s err=%s",
            source_url[:96],
            start,
            end,
            exc,
        )


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
                    if msg_type == "range_snapshot":
                        sources = message.get("sources") or []
                        _apply_range_snapshot(sources if isinstance(sources, list) else [])
                        logger.info(
                            "Control-server range snapshot merged sources=%d",
                            len(sources) if isinstance(sources, list) else 0,
                        )
                    elif msg_type == "range_broadcast":
                        source_url = str(message.get("source_url") or "")
                        try:
                            start = int(message.get("start"))
                            end = int(message.get("end"))
                        except (TypeError, ValueError):
                            continue
                        if source_url and end >= start:
                            _apply_range_broadcast(source_url, start, end)
                            logger.info(
                                "Control-server range broadcast miner_id=%s "
                                "source_miner=%s src=%s range=%s-%s",
                                cfg.miner_id or "miner",
                                message.get("source_miner") or "?",
                                source_url[:96],
                                start,
                                end,
                            )
                    elif msg_type in ("cache_snapshot", "cache_broadcast"):
                        # Legacy metadata messages ignored — sync is range_data only.
                        logger.debug("Ignoring legacy %s (range sync active)", msg_type)
                    elif msg_type in ("range_update_ack", "cache_update_ack"):
                        logger.info(
                            "Control-server range push ack miner_id=%s",
                            cfg.miner_id or "miner",
                        )
                    elif msg_type == "sync_done":
                        logger.info(
                            "Control-server cache sync_done miner_id=%s",
                            cfg.miner_id or "miner",
                        )
                        _mark_cache_sync_done()
                        if _sync_done_handler is not None:
                            try:
                                _sync_done_handler()
                            except Exception as exc:
                                logger.warning("sync_done handler failed: %s", exc)
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
    global _cache_sync_done_event
    _cache_sync_done_event = asyncio.Event()
    _client_task = asyncio.create_task(_client_loop(), name="control-ws-client")
    logger.info("Control-server WS client started miner_id=%s", cfg.miner_id or "miner")


async def stop_control_ws_client() -> None:
    global _client_task, _stop_event, _update_queue, _cache_sync_done_event
    if _stop_event is not None:
        _stop_event.set()
    if _client_task is not None:
        _client_task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await _client_task
    _client_task = None
    _stop_event = None
    _update_queue = None
    _cache_sync_done_event = None
