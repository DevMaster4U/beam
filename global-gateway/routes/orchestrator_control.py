"""Shared orchestrator control message handling (WebSocket and IPC)."""

from __future__ import annotations

import logging
import uuid
from typing import Optional

from auth import validate_orchestrator_api_key
from config import get_settings
from core import gateway_state
from transports import OrchestratorChannel

logger = logging.getLogger(__name__)



async def handle_task_offer_batch(orchestrator_hotkey: str, message: dict) -> dict:
    offers = message.get("offers") or []
    batch_id = message.get("batch_id") or uuid.uuid4().hex
    if not isinstance(offers, list) or not offers:
        return {"type": "task_offer_batch_result", "batch_id": batch_id, "delivered": 0, "failed": 0}

    total_offers = len(offers)
    logger.info(
        "task_offer_batch %s: %d offer(s) from orch=%s",
        batch_id,
        total_offers,
        orchestrator_hotkey[:16]
    )

    delivered = 0
    failed = 0
    offer_index = 0
    batch_used_ips: set[str] = set()
    batch_assigned_workers: set[str] = set()
    for offer in offers:
        offer_index += 1
        if not isinstance(offer, dict):
            failed += 1
            logger.warning(
                "task_offer_batch %s offer %d/%d: invalid offer payload (%s)",
                batch_id,
                offer_index,
                total_offers,
                gateway_state.worker_pool_summary(),
            )
            continue

        offer_id = offer.get("offer_id") or offer.get("task_id")
        task_id = offer.get("task_id") or offer_id

        worker_id = gateway_state.select_worker(
            batch_used_ips=batch_used_ips,
            batch_assigned_workers=batch_assigned_workers,
        )
        if not worker_id:
            logger.warning(
                "task_offer_batch %s offer %d/%d: no worker with capacity (%s) "
                "task=%s offer=%s",
                batch_id,
                offer_index,
                total_offers,
                gateway_state.worker_pool_summary(),
                task_id,
                offer_id,
            )
            failed += 1
            continue

        gateway_state.register_route(orchestrator_hotkey, worker_id, offer_id, task_id)

        payload = {"type": "task_offer", **offer}
        if await gateway_state.send_to_worker(worker_id, payload):
            gateway_state.mark_worker_busy(worker_id, offer_id)
            batch_assigned_workers.add(worker_id)
            worker_ip = gateway_state.get_profile(worker_id).ip.strip()
            if worker_ip:
                batch_used_ips.add(worker_ip)
            gateway_state.record_task_assigned(
                worker_id,
                task_id=str(task_id),
                offer_id=str(offer_id),
                orchestrator_hotkey=orchestrator_hotkey,
            )
            delivered += 1
            profile = gateway_state.get_profile(worker_id)
            logger.info(
                "task_offer_batch %s offer %d/%d: orch=%s -> worker=%s "
                "selection=%s avg_mbps=%.1f ip=%s active=%d/%d task=%s offer=%s",
                batch_id,
                offer_index,
                total_offers,
                orchestrator_hotkey[:16],
                worker_id,
                gateway_state.worker_selection,
                profile.average_mbps,
                profile.ip or "?",
                profile.active_count,
                profile.max_concurrent_tasks,
                task_id,
                offer_id
            )
        else:
            failed += 1
            logger.warning(
                "task_offer_batch %s offer %d/%d: send failed worker=%s task=%s offer=%s (%s)",
                batch_id,
                offer_index,
                total_offers,
                worker_id,
                task_id,
                offer_id,
                gateway_state.worker_pool_summary(),
            )

    logger.info(
        "task_offer_batch %s done: delivered=%d failed=%d offers=%d",
        batch_id,
        delivered,
        failed,
        total_offers
    )

    return {
        "type": "task_offer_batch_result",
        "batch_id": batch_id,
        "delivered": delivered,
        "failed": failed,
    }


async def handle_orchestrator_message(
    orchestrator_hotkey: str,
    message: dict,
    channel: OrchestratorChannel,
) -> None:
    msg_type = message.get("type")

    if msg_type == "task_offer_batch":
        result = await handle_task_offer_batch(orchestrator_hotkey, message)
        await channel.send(result)
        return

    if msg_type == "to_worker":
        worker_id = str(message.get("worker_id") or "")
        payload = message.get("payload")
        if worker_id and isinstance(payload, dict):
            worker_msg_type = payload.get("type")
            if worker_msg_type == "task_accept_ack":
                ack_task_id = payload.get("task_id")
                ack_offer_id = payload.get("offer_id") or ack_task_id
                server_accepted = payload.get("accepted", True)
                if not server_accepted:
                    gateway_state.mark_worker_idle(worker_id, payload.get("offer_id"))
            
            await gateway_state.send_to_worker(worker_id, payload)
        return

    if msg_type == "list_workers":
        await channel.send(
            {
                "type": "list_workers",
                "request_id": message.get("request_id"),
                "workers": gateway_state.worker_status_payload(),
            }
        )
        return

    logger.debug("ignored orchestrator control message type=%s", msg_type)


async def register_orchestrator_channel(
    orchestrator_hotkey: str,
    channel: OrchestratorChannel,
    *,
    control_secret: str,
    api_key: str = "",
) -> bool:
    settings = get_settings()
    if control_secret != settings.orchestrator_secret:
        logger.warning("orchestrator control rejected for %s: invalid control_secret", orchestrator_hotkey)
        return False

    if api_key and not await validate_orchestrator_api_key(
        settings.core_server_url, orchestrator_hotkey, api_key
    ):
        logger.warning("orchestrator control rejected for %s: API key validation failed", orchestrator_hotkey)
        return False

    gateway_state.orchestrator_sessions[orchestrator_hotkey] = channel
    logger.info(
        "Orchestrator control connected (%s): %s (orch_sessions=%d workers=%d)",
        channel.transport,
        orchestrator_hotkey,
        gateway_state.orchestrator_count(),
        gateway_state.worker_count(),
    )
    await channel.send(
        {
            "type": "control_connected",
            "orchestrator_hotkey": orchestrator_hotkey,
            "worker_count": gateway_state.worker_count(),
            "workers": gateway_state.worker_status_payload(),
        }
    )
    return True


def unregister_orchestrator_channel(orchestrator_hotkey: str) -> None:
    existing = gateway_state.orchestrator_sessions.get(orchestrator_hotkey)
    if existing is not None:
        gateway_state.orchestrator_sessions.pop(orchestrator_hotkey, None)
        logger.info("Orchestrator control disconnected: %s", orchestrator_hotkey)
