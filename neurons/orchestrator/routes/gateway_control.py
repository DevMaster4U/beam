"""In-process worker gateway control WebSocket (/ws/{session_id}?control_secret=...)."""

from __future__ import annotations

import json
import logging
import uuid
from typing import Any, Optional

from fastapi import APIRouter, WebSocket, WebSocketDisconnect, status

from core.orchestrator import get_orchestrator

logger = logging.getLogger(__name__)

router = APIRouter(tags=["gateway-control"])


def _provided_control_secret(websocket: WebSocket) -> str:
    return (
        websocket.query_params.get("control_secret", "").strip()
        or websocket.headers.get("x-control-secret", "").strip()
    )


async def _send_json(ws: WebSocket, payload: dict[str, Any]) -> None:
    await ws.send_text(json.dumps(payload))


async def handle_gateway_control_websocket(
    websocket: WebSocket,
    *,
    session_id: str,
    configured_secret: Optional[str],
) -> None:
    orchestrator = get_orchestrator()
    if orchestrator is None:
        await websocket.close(code=status.WS_1013_TRY_AGAIN_LATER)
        return

    gateway = getattr(orchestrator, "worker_gateway", None)
    if gateway is None:
        await websocket.close(code=status.WS_1013_TRY_AGAIN_LATER)
        return

    if not configured_secret:
        logger.warning("Control WS rejected: WORKER_GATEWAY_CONTROL_SECRET not configured")
        await websocket.close(code=status.WS_1008_POLICY_VIOLATION)
        return

    if _provided_control_secret(websocket) != configured_secret:
        logger.warning("Control WS rejected: invalid control_secret for session %s", session_id)
        await websocket.close(code=status.WS_1008_POLICY_VIOLATION)
        return

    await websocket.accept()
    gateway.set_control(websocket)
    logger.info("Worker gateway control connected (session=%s)", session_id)

    await _send_json(
        websocket,
        {
            "type": "control_connected",
            "workers": gateway.list_workers(),
        },
    )

    try:
        while True:
            raw = await websocket.receive_text()
            try:
                message = json.loads(raw)
            except json.JSONDecodeError:
                logger.warning("Invalid JSON on gateway control channel")
                continue

            msg_type = message.get("type")
            request_id = message.get("request_id") or uuid.uuid4().hex

            if msg_type == "list_workers":
                await _send_json(
                    websocket,
                    {
                        "type": "worker_list",
                        "request_id": request_id,
                        "workers": gateway.list_workers(),
                    },
                )
            elif msg_type == "task_offer":
                worker_id = message.get("worker_id")
                offer = message.get("offer") or message
                if worker_id and isinstance(offer, dict):
                    await gateway.deliver_task_offer(worker_id, offer)
            elif msg_type == "task_accept_ack":
                worker_id = message.get("worker_id")
                if worker_id:
                    await gateway.send_worker_payload(
                        worker_id,
                        {
                            "type": "task_accept_ack",
                            "task_id": message.get("task_id"),
                            "offer_id": message.get("offer_id") or message.get("task_id"),
                            "accepted": bool(message.get("accepted", True)),
                            "reason": message.get("reason"),
                        },
                    )
            elif msg_type == "task_result_ack":
                worker_id = message.get("worker_id")
                if worker_id:
                    await gateway.send_worker_payload(
                        worker_id,
                        {
                            "type": "task_result_ack",
                            "task_id": message.get("task_id"),
                            "offer_id": message.get("offer_id") or message.get("task_id"),
                            "received": bool(message.get("received", True)),
                            "completed": message.get("completed"),
                            "reason": message.get("reason"),
                        },
                    )
            elif msg_type == "ping":
                await _send_json(websocket, {"type": "pong", "request_id": request_id})
            else:
                logger.debug("Ignoring unknown gateway control message: %s", msg_type)

    except WebSocketDisconnect:
        pass
    except Exception as exc:
        logger.warning("Gateway control WS error (session=%s): %s", session_id, exc)
    finally:
        gateway.clear_control()
        logger.info("Worker gateway control disconnected (session=%s)", session_id)
