"""Worker WebSocket routes for the global gateway."""

import logging
from typing import Optional

from fastapi import APIRouter, WebSocket, WebSocketDisconnect, status
from fastapi.websockets import WebSocketState

from auth import validate_worker_api_key
from config import get_settings
from core import gateway_state

logger = logging.getLogger(__name__)

router = APIRouter(tags=["workers"])


def _worker_secret(websocket: WebSocket) -> str:
    return (
        websocket.query_params.get("worker_secret", "").strip()
        or websocket.headers.get("x-worker-secret", "").strip()
    )


@router.websocket("/ws/{worker_id}")
async def worker_ws(websocket: WebSocket, worker_id: str) -> None:
    settings = get_settings()
    gateway_state.max_workers = settings.max_workers

    api_key = websocket.query_params.get("api_key") or ""
    if not api_key:
        await websocket.close(code=status.WS_1008_POLICY_VIOLATION)
        return

    if _worker_secret(websocket) != settings.worker_secret:
        logger.warning("worker %s rejected: invalid worker_secret", worker_id)
        await websocket.close(code=status.WS_1008_POLICY_VIOLATION)
        return

    if not await validate_worker_api_key(settings.core_server_url, worker_id, api_key):
        logger.warning("worker %s rejected: API key validation failed", worker_id)
        await websocket.close(code=status.WS_1008_POLICY_VIOLATION)
        return

    if (
        gateway_state.worker_count() >= settings.max_workers
        and worker_id not in gateway_state.worker_sessions
    ):
        await websocket.close(code=status.WS_1013_TRY_AGAIN_LATER)
        return

    await websocket.accept()
    gateway_state.worker_sessions[worker_id] = websocket
    logger.info(
        "Worker connected: %s (%d/%d)",
        worker_id,
        gateway_state.worker_count(),
        settings.max_workers,
    )
    await gateway_state.notify_pool_status()

    try:
        await websocket.send_text('{"type":"connected"}')
        while True:
            raw = await websocket.receive_text()
            try:
                import json

                message = json.loads(raw)
            except json.JSONDecodeError:
                logger.warning("invalid JSON from worker %s", worker_id)
                continue

            message["worker_id"] = worker_id
            msg_type = message.get("type")
            if msg_type not in ("task_accept", "task_reject", "task_result"):
                logger.debug("ignored worker message type %s from %s", msg_type, worker_id)
                continue

            orch_hotkey = gateway_state.resolve_orchestrator_hotkey(message)
            if not orch_hotkey:
                logger.warning(
                    "no orchestrator route for worker %s message type=%s task=%s offer=%s",
                    worker_id,
                    msg_type,
                    message.get("task_id"),
                    message.get("offer_id"),
                )
                continue

            await gateway_state.forward_to_orchestrator(orch_hotkey, message)

    except WebSocketDisconnect:
        pass
    except Exception as exc:
        logger.warning("worker WS error for %s: %s", worker_id, exc)
    finally:
        gateway_state.worker_sessions.pop(worker_id, None)
        logger.info(
            "Worker disconnected: %s (%d/%d)",
            worker_id,
            gateway_state.worker_count(),
            settings.max_workers,
        )
        await gateway_state.notify_pool_status()
        if websocket.client_state != WebSocketState.DISCONNECTED:
            try:
                await websocket.close()
            except Exception:
                pass
