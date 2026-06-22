"""Orchestrator control WebSocket routes for the global gateway."""

import json
import logging

from fastapi import APIRouter, WebSocket, WebSocketDisconnect, status
from fastapi.websockets import WebSocketState

from routes.orchestrator_control import (
    handle_orchestrator_message,
    register_orchestrator_channel,
    unregister_orchestrator_channel,
)
from transports import WebSocketOrchestratorChannel

logger = logging.getLogger(__name__)

router = APIRouter(tags=["orchestrators"])


def _control_secret(websocket: WebSocket) -> str:
    return (
        websocket.query_params.get("control_secret", "").strip()
        or websocket.headers.get("x-control-secret", "").strip()
    )


@router.websocket("/ws/orchestrators/{orchestrator_hotkey}")
async def orchestrator_control_ws(websocket: WebSocket, orchestrator_hotkey: str) -> None:
    api_key = websocket.query_params.get("api_key") or ""
    control_secret = _control_secret(websocket)
    channel = WebSocketOrchestratorChannel(websocket)

    await websocket.accept()
    if not await register_orchestrator_channel(
        orchestrator_hotkey,
        channel,
        control_secret=control_secret,
        api_key=api_key,
    ):
        await websocket.close(code=status.WS_1008_POLICY_VIOLATION)
        return

    try:
        while True:
            raw = await websocket.receive_text()
            try:
                message = json.loads(raw)
            except json.JSONDecodeError:
                logger.warning("invalid JSON on orchestrator control channel")
                continue

            await handle_orchestrator_message(orchestrator_hotkey, message, channel)

    except WebSocketDisconnect:
        pass
    except Exception as exc:
        logger.warning("orchestrator control WS error for %s: %s", orchestrator_hotkey, exc)
    finally:
        unregister_orchestrator_channel(orchestrator_hotkey)
        if websocket.client_state != WebSocketState.DISCONNECTED:
            try:
                await websocket.close()
            except Exception:
                pass
