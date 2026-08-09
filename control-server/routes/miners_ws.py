"""Miner WebSocket routes for shared cache sync."""

from __future__ import annotations

import asyncio
import logging

from fastapi import APIRouter, WebSocket, WebSocketDisconnect

from auth import validate_control_secret
from config import get_settings
from ws_hub import miner_hub

logger = logging.getLogger(__name__)

router = APIRouter(tags=["miners-ws"])


@router.websocket("/ws/miners")
async def miner_websocket(websocket: WebSocket) -> None:
    settings = get_settings()
    miner_id = ""
    await websocket.accept()
    try:
        hello = await websocket.receive_json()
        if str(hello.get("type") or "") != "hello":
            await websocket.close(code=4400, reason="expected hello")
            return

        secret = str(hello.get("secret") or "")
        if not validate_control_secret(secret, settings.secret):
            await websocket.close(code=4401, reason="invalid secret")
            return

        miner_id = str(hello.get("miner_id") or "unknown").strip() or "unknown"
        await miner_hub.connect(miner_id, websocket)

        while True:
            message = await websocket.receive_json()
            msg_type = str(message.get("type") or "")
            if msg_type == "range_update":
                asyncio.create_task(
                    miner_hub.handle_range_update(miner_id, message),
                    name=f"range-update-{miner_id}",
                )
            elif msg_type == "cache_update":
                # Legacy: source|start|end key → range_broadcast
                asyncio.create_task(
                    miner_hub.handle_cache_update(miner_id, message),
                    name=f"cache-update-{miner_id}",
                )
            elif msg_type == "ping":
                await miner_hub._send(miner_id, {"type": "pong"})
            else:
                await miner_hub._send(
                    miner_id,
                    {"type": "error", "detail": f"unknown message type: {msg_type}"},
                )
    except WebSocketDisconnect:
        pass
    except Exception as exc:
        logger.warning("Miner websocket error miner_id=%s err=%s", miner_id, exc)
    finally:
        if miner_id:
            await miner_hub.disconnect(miner_id)
