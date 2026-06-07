"""Worker data-path WebSocket handler (/ws/{worker_id})."""

from __future__ import annotations

import logging
from typing import Any

from fastapi import WebSocket, WebSocketDisconnect

from .protocol import (
    build_task_accept_ack,
    build_worker_response_from_accept,
    build_worker_response_from_reject,
)
from .sessions import SessionRegistry

logger = logging.getLogger(__name__)


async def handle_worker_websocket(
    websocket: WebSocket,
    worker_id: str,
    registry: SessionRegistry,
) -> None:
    await websocket.accept()
    session = await registry.register_worker(worker_id, websocket)

    await websocket.send_json(
        {
            "type": "connected",
            "worker_id": worker_id,
            "gateway_mode": "orch_owned",
        }
    )
    await registry.notify_control({"type": "worker_connected", "worker_id": worker_id})

    try:
        while True:
            message = await websocket.receive_json()
            msg_type = message.get("type")

            if msg_type == "task_accept":
                # Relay to orchestrator; task_accept_ack comes back on the control channel
                # after BeamCore confirms the lease (orch-owned gateway semantics).
                await registry.notify_control(build_worker_response_from_accept(message))

            elif msg_type == "task_reject":
                await registry.notify_control(build_worker_response_from_reject(message))
                await websocket.send_json(
                    build_task_accept_ack(
                        task_id=message.get("task_id"),
                        offer_id=message.get("offer_id") or message.get("task_id"),
                        accepted=False,
                        reason=message.get("reason", "rejected"),
                    )
                )

            elif msg_type == "task_result_summary":
                # Relay to orchestrator; summary ack is sent after BeamCore verification.
                await registry.notify_control(message)

            elif msg_type == "stats_snapshot":
                session.bandwidth_mbps = float(message.get("bandwidth_mbps") or 0.0)
                session.tasks_active = int(message.get("tasks_active") or 0)
                await registry.notify_control(
                    {
                        "type": "worker_capacity_update",
                        "worker_id": worker_id,
                        "bandwidth_mbps": session.bandwidth_mbps,
                        "tasks_active": session.tasks_active,
                    }
                )
                await websocket.send_json({"type": "stats_snapshot_ack"})

            elif msg_type == "task_transfer_progress":
                relay = {**message, "worker_id": worker_id}
                await registry.notify_control(relay)

            elif msg_type == "bw_challenge_response":
                await registry.notify_control({**message, "worker_id": worker_id})

            else:
                logger.debug("Ignoring unknown worker message type: %s", msg_type)

    except WebSocketDisconnect:
        logger.info("Worker websocket disconnected: %s", worker_id[:20])
    except Exception:
        logger.exception("Worker websocket error: %s", worker_id[:20])
    finally:
        await registry.unregister_worker(worker_id)
        await registry.notify_control({"type": "worker_disconnected", "worker_id": worker_id})
