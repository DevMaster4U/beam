"""Orchestrator control WebSocket routes for the global gateway."""

import json
import logging
import uuid
from typing import Any, Optional

from fastapi import APIRouter, WebSocket, WebSocketDisconnect, status
from fastapi.websockets import WebSocketState

from auth import validate_orchestrator_api_key
from config import get_settings
from core import gateway_state

logger = logging.getLogger(__name__)

router = APIRouter(tags=["orchestrators"])


def _control_secret(websocket: WebSocket) -> str:
    return (
        websocket.query_params.get("control_secret", "").strip()
        or websocket.headers.get("x-control-secret", "").strip()
    )


async def _handle_task_offer_batch(orchestrator_hotkey: str, message: dict) -> dict:
    offers = message.get("offers") or []
    batch_id = message.get("batch_id") or uuid.uuid4().hex
    if not isinstance(offers, list) or not offers:
        return {"type": "task_offer_batch_result", "batch_id": batch_id, "delivered": 0, "failed": 0}

    delivered = 0
    failed = 0
    for offer in offers:
        if not isinstance(offer, dict):
            failed += 1
            continue
        workers = gateway_state.get_workers_round_robin(1)
        if not workers:
            logger.warning("task_offer_batch %s: no workers connected", batch_id)
            failed += 1
            continue
        worker_id = workers[0]
        offer_id = offer.get("offer_id") or offer.get("task_id")
        task_id = offer.get("task_id") or offer_id
        gateway_state.register_route(orchestrator_hotkey, worker_id, offer_id, task_id)

        payload = {"type": "task_offer", **offer}
        if await gateway_state.send_to_worker(worker_id, payload):
            delivered += 1
            logger.info(
                "task_offer orch=%s -> worker=%s task=%s offer=%s",
                orchestrator_hotkey[:16],
                worker_id,
                task_id,
                offer_id,
            )
        else:
            failed += 1

    return {
        "type": "task_offer_batch_result",
        "batch_id": batch_id,
        "delivered": delivered,
        "failed": failed,
    }


@router.websocket("/ws/orchestrators/{orchestrator_hotkey}")
async def orchestrator_control_ws(websocket: WebSocket, orchestrator_hotkey: str) -> None:
    settings = get_settings()

    if _control_secret(websocket) != settings.orchestrator_secret:
        logger.warning("control WS rejected for %s: invalid control_secret", orchestrator_hotkey)
        await websocket.close(code=status.WS_1008_POLICY_VIOLATION)
        return

    api_key = websocket.query_params.get("api_key") or ""
    if api_key and not await validate_orchestrator_api_key(
        settings.core_server_url, orchestrator_hotkey, api_key
    ):
        logger.warning("control WS rejected for %s: API key validation failed", orchestrator_hotkey)
        await websocket.close(code=status.WS_1008_POLICY_VIOLATION)
        return

    await websocket.accept()
    gateway_state.orchestrator_sessions[orchestrator_hotkey] = websocket
    logger.info(
        "Orchestrator control connected: %s (orch_sessions=%d workers=%d)",
        orchestrator_hotkey,
        gateway_state.orchestrator_count(),
        gateway_state.worker_count(),
    )

    await gateway_state.send_json(
        websocket,
        {
            "type": "control_connected",
            "orchestrator_hotkey": orchestrator_hotkey,
            "worker_count": gateway_state.worker_count(),
            "workers": [{"worker_id": wid} for wid in gateway_state.list_worker_ids()],
        },
    )

    try:
        while True:
            raw = await websocket.receive_text()
            try:
                message = json.loads(raw)
            except json.JSONDecodeError:
                logger.warning("invalid JSON on orchestrator control channel")
                continue

            msg_type = message.get("type")

            if msg_type == "task_offer_batch":
                result = await _handle_task_offer_batch(orchestrator_hotkey, message)
                await gateway_state.send_json(websocket, result)
                continue

            if msg_type == "to_worker":
                worker_id = str(message.get("worker_id") or "")
                payload = message.get("payload")
                if worker_id and isinstance(payload, dict):
                    await gateway_state.send_to_worker(worker_id, payload)
                continue

            if msg_type == "list_workers":
                await gateway_state.send_json(
                    websocket,
                    {
                        "type": "list_workers",
                        "request_id": message.get("request_id"),
                        "workers": [{"worker_id": wid} for wid in gateway_state.list_worker_ids()],
                    },
                )
                continue

            logger.debug("ignored orchestrator control message type=%s", msg_type)

    except WebSocketDisconnect:
        pass
    except Exception as exc:
        logger.warning("orchestrator control WS error for %s: %s", orchestrator_hotkey, exc)
    finally:
        gateway_state.orchestrator_sessions.pop(orchestrator_hotkey, None)
        logger.info("Orchestrator control disconnected: %s", orchestrator_hotkey)
        if websocket.client_state != WebSocketState.DISCONNECTED:
            try:
                await websocket.close()
            except Exception:
                pass
