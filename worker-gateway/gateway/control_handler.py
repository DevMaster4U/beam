"""Orchestrator control WebSocket handler (/control)."""

from __future__ import annotations

import logging
import uuid
from typing import Any

from fastapi import WebSocket, WebSocketDisconnect

from .protocol import build_task_accept_ack, build_task_offer_for_worker, build_task_result_ack
from .sessions import SessionRegistry

logger = logging.getLogger(__name__)


async def _handle_task_offer(registry: SessionRegistry, message: dict[str, Any]) -> None:
    worker_id = message.get("worker_id")
    offer = message.get("offer") or message
    if not worker_id:
        logger.warning("task_offer missing worker_id")
        return

    payload = build_task_offer_for_worker(offer)
    delivered = await registry.send_to_worker(worker_id, payload)
    if not delivered:
        logger.warning("Failed to deliver task_offer to worker %s", str(worker_id)[:20])


async def _handle_task_accept_ack(registry: SessionRegistry, message: dict[str, Any]) -> None:
    worker_id = message.get("worker_id")
    if not worker_id:
        return
    await registry.send_to_worker(
        worker_id,
        build_task_accept_ack(
            task_id=message.get("task_id"),
            offer_id=message.get("offer_id") or message.get("task_id"),
            accepted=bool(message.get("accepted", True)),
            reason=message.get("reason"),
        ),
    )


async def _handle_task_result_ack(registry: SessionRegistry, message: dict[str, Any]) -> None:
    worker_id = message.get("worker_id")
    if not worker_id:
        return
    await registry.send_to_worker(
        worker_id,
        build_task_result_ack(
            task_id=message.get("task_id"),
            offer_id=message.get("offer_id") or message.get("task_id"),
            received=bool(message.get("received", True)),
            completed=message.get("completed"),
            reason=message.get("reason"),
        ),
    )


# Backward-compatible alias (deprecated)
_handle_task_result_summary_ack = _handle_task_result_ack


async def handle_control_websocket(
    websocket: WebSocket,
    registry: SessionRegistry,
    control_secret: str,
) -> None:
    provided = websocket.headers.get("x-control-secret", "")
    if provided != control_secret:
        await websocket.close(code=4401, reason="unauthorized")
        return

    await websocket.accept()
    registry.set_control(websocket)

    await websocket.send_json(
        {
            "type": "control_connected",
            "workers": registry.list_workers(),
        }
    )

    try:
        while True:
            message = await websocket.receive_json()
            msg_type = message.get("type")
            request_id = message.get("request_id")

            if msg_type == "list_workers":
                await websocket.send_json(
                    {
                        "type": "worker_list",
                        "request_id": request_id or uuid.uuid4().hex,
                        "workers": registry.list_workers(),
                    }
                )

            elif msg_type == "task_offer":
                await _handle_task_offer(registry, message)

            elif msg_type == "task_accept_ack":
                await _handle_task_accept_ack(registry, message)

            elif msg_type in ("task_result_ack", "task_result_summary_ack"):
                await _handle_task_result_ack(registry, message)

            elif msg_type == "ping":
                await websocket.send_json({"type": "pong", "request_id": request_id})

            else:
                logger.debug("Ignoring unknown control message type: %s", msg_type)

    except WebSocketDisconnect:
        logger.info("Control websocket disconnected")
    except Exception:
        logger.exception("Control websocket error")
    finally:
        registry.clear_control(websocket)
