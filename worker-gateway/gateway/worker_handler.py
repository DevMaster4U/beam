"""Worker data-path WebSocket handler (/ws/{worker_id})."""

from __future__ import annotations

import logging
from typing import Any

from fastapi import WebSocket, WebSocketDisconnect

from .sessions import SessionRegistry

logger = logging.getLogger(__name__)


async def handle_worker_websocket(
    websocket: WebSocket,
    worker_id: str,
    registry: SessionRegistry,
    worker_secret: str,
) -> None:
    provided = (
        websocket.query_params.get("worker_secret", "").strip()
        or websocket.headers.get("x-worker-secret", "").strip()
    )
    if provided != worker_secret:
        logger.warning("Worker websocket rejected: invalid worker secret for %s", worker_id[:20])
        await websocket.close(code=4401, reason="unauthorized")
        return

    await websocket.accept()
    session = await registry.register_worker(worker_id, websocket)

    await websocket.send_json(
        {
            "type": "connected",
            "worker_id": worker_id,
            "gateway_mode": "orch_owned",
        }
    )
    await registry.notify_control(
        {
            "type": "worker_connected",
            "worker_id": worker_id,
            "client_ip": session.client_ip,
            "hotkey": session.hotkey,
        }
    )

    try:
        while True:
            message = await websocket.receive_json()
            msg_type = message.get("type")

            if msg_type == "task_accept":
                # Relay to orchestrator; task_accept_ack comes back on the control channel
                # after BeamCore confirms the lease (orch-owned gateway semantics).
                await registry.notify_control(message)

            elif msg_type == "task_reject":
                await registry.notify_control(message)

            elif msg_type in ("task_result", "task_result_summary"):
                # Relay to orchestrator; result ack is sent after BeamCore verification.
                await registry.notify_control(message)

            elif msg_type == "stats_snapshot":
                session.bandwidth_mbps = float(message.get("bandwidth_mbps") or 0.0)
                session.tasks_active = int(message.get("tasks_active") or 0)
                hotkey = str(message.get("hotkey") or "").strip()
                if hotkey:
                    registry.update_worker_identity(worker_id, hotkey=hotkey)
                await registry.notify_control(
                    {
                        "type": "worker_capacity_update",
                        "worker_id": worker_id,
                        "bandwidth_mbps": session.bandwidth_mbps,
                        "tasks_active": session.tasks_active,
                        "client_ip": session.client_ip,
                        "hotkey": session.hotkey,
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
        logger.info("Worker websocket disconnected: %s", worker_id)
    except Exception:
        logger.exception("Worker websocket error: %s", worker_id)
    finally:
        session = registry.get_worker(worker_id)
        client_ip = session.client_ip if session else ""
        hotkey = session.hotkey if session else ""
        await registry.unregister_worker(worker_id)
        await registry.notify_control(
            {
                "type": "worker_disconnected",
                "worker_id": worker_id,
                "client_ip": client_ip,
                "hotkey": hotkey,
            }
        )
