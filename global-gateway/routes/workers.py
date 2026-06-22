"""Worker WebSocket routes for the global gateway."""

import json
import logging
from typing import Any, Optional

from fastapi import APIRouter, WebSocket, WebSocketDisconnect, status
from fastapi.websockets import WebSocketState

from auth import fetch_worker_profile
from config import get_settings
from core import gateway_state

logger = logging.getLogger(__name__)

router = APIRouter(tags=["workers"])


def _worker_secret(websocket: WebSocket) -> str:
    return (
        websocket.query_params.get("worker_secret", "").strip()
        or websocket.headers.get("x-worker-secret", "").strip()
    )


def _float_field(data: dict, *keys: str, default: float = 0.0) -> float:
    for key in keys:
        raw = data.get(key)
        if raw is None:
            continue
        try:
            return float(raw)
        except (TypeError, ValueError):
            continue
    return default


def _int_field(data: dict, *keys: str, default: int = 0) -> int:
    for key in keys:
        raw = data.get(key)
        if raw is None:
            continue
        try:
            return int(raw)
        except (TypeError, ValueError):
            continue
    return default


def _extract_ip(profile_data: Optional[dict], peer_ip: str = "") -> str:
    if profile_data:
        for key in ("ip", "public_ip", "external_ip", "ip_address", "host"):
            raw = profile_data.get(key)
            if raw and str(raw).strip():
                return str(raw).strip()
        nested = profile_data.get("worker")
        if isinstance(nested, dict):
            for key in ("ip", "public_ip", "external_ip", "ip_address", "host"):
                raw = nested.get(key)
                if raw and str(raw).strip():
                    return str(raw).strip()
    if peer_ip:
        return peer_ip.strip()
    return ""


def _profile_fields(profile_data: Optional[dict], peer_ip: str = "") -> dict[str, Any]:
    if not profile_data:
        return {
            "ip": peer_ip,
            "claimed_bandwidth_mbps": 0.0,
            "trust_score": 0.5,
            "success_rate": 1.0,
            "max_concurrent_tasks": 5,
        }
    return {
        "ip": _extract_ip(profile_data, peer_ip),
        "claimed_bandwidth_mbps": _float_field(
            profile_data,
            "claimed_bandwidth_mbps",
            "bandwidth_mbps",
            "bandwidth_ema",
        ),
        "trust_score": _float_field(profile_data, "trust_score", default=0.5),
        "success_rate": _float_field(profile_data, "success_rate", default=1.0),
        "max_concurrent_tasks": _int_field(
            profile_data,
            "max_concurrent_tasks",
            default=5,
        ),
    }


async def _handle_worker_message(worker_id: str, message: dict) -> None:
    msg_type = message.get("type")

    if msg_type == "worker_hello":
        ip = str(message.get("ip") or "").strip()
        claimed_raw = message.get("claimed_bandwidth_mbps")
        max_tasks_raw = message.get("max_concurrent_tasks")
        try:
            claimed = float(claimed_raw) if claimed_raw is not None else None
        except (TypeError, ValueError):
            claimed = None
        try:
            max_concurrent = int(max_tasks_raw) if max_tasks_raw is not None else None
        except (TypeError, ValueError):
            max_concurrent = None
        gateway_state.update_worker_hello(
            worker_id,
            ip=ip or None,
            claimed_bandwidth_mbps=claimed,
            max_concurrent_tasks=max_concurrent,
        )
        profile = gateway_state.get_profile(worker_id)
        logger.info(
            "Worker connected: %s ip=%s max_tasks=%d active=%d avg_mbps=%.1f score=%.4f (%d/%d)",
            worker_id,
            profile.ip or "?",
            profile.max_concurrent_tasks,
            profile.active_count,
            profile.average_mbps,
            profile.score(gateway_state.scoring_weights),
            gateway_state.worker_count(),
            gateway_state.max_workers,
        )
        await gateway_state.notify_pool_status()
        return

    if msg_type == "task_accept":
        gateway_state.record_task_accepted(worker_id, message)
    elif msg_type == "task_reject":
        offer_id = str(message.get("offer_id") or message.get("task_id") or "")
        gateway_state.record_task_rejected(worker_id, message)
        gateway_state.mark_worker_idle(worker_id, offer_id or None)
        profile = gateway_state.get_profile(worker_id)
        logger.info(
            "worker %s task_reject offer=%s active_tasks=%d (%s)",
            worker_id,
            offer_id or "?",
            profile.active_count,
            gateway_state.worker_pool_summary(),
        )
    elif msg_type == "task_result":
        offer_id = str(message.get("offer_id") or message.get("task_id") or "")
        duplicate = offer_id and gateway_state.is_duplicate_task_result(offer_id)
        if not duplicate:
            gateway_state.observe_worker_transfer(
                worker_id,
                message.get("transfer_mbps"),
                success=bool(message.get("success", False)),
            )
            gateway_state.record_task_result(worker_id, message)
            gateway_state.mark_worker_idle(worker_id, offer_id or None)
            profile = gateway_state.get_profile(worker_id)
            logger.info(
                "worker %s task_result offer=%s active_tasks=%d avg_mbps=%.1f (n=%d) "
                "success_rate=%.3f transfer_mbps=%s (%s)",
                worker_id,
                offer_id or "?",
                profile.active_count,
                profile.average_mbps,
                profile.transfer_count,
                profile.success_rate,
                message.get("transfer_mbps"),
                gateway_state.worker_pool_summary(),
            )
        else:
            logger.info(
                "worker %s duplicate task_result offer=%s (relay only, stats skipped)",
                worker_id,
                offer_id or "?",
            )

    if msg_type not in ("task_accept", "task_reject", "task_result"):
        return

    orch_hotkey = gateway_state.resolve_orchestrator_hotkey(message)
    if not orch_hotkey:
        logger.warning(
            "no orchestrator route for worker %s message type=%s task=%s offer=%s",
            worker_id,
            msg_type,
            message.get("task_id"),
            message.get("offer_id"),
        )
        return

    await gateway_state.forward_to_orchestrator(orch_hotkey, message)


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

    profile_data = await fetch_worker_profile(settings.core_server_url, worker_id, api_key)
    if not profile_data:
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
    peer_ip = ""
    if websocket.client and websocket.client.host:
        peer_ip = str(websocket.client.host).strip()
    fields = _profile_fields(profile_data, peer_ip=peer_ip)
    gateway_state.register_worker_session(
        worker_id,
        websocket,
        ip=fields["ip"],
        claimed_bandwidth_mbps=fields["claimed_bandwidth_mbps"],
        trust_score=fields["trust_score"],
        success_rate=fields["success_rate"],
        max_concurrent_tasks=fields["max_concurrent_tasks"],
    )
    logger.debug(
        "Worker WS accepted: %s peer=%s (%d/%d)",
        worker_id,
        fields["ip"] or peer_ip or "?",
        gateway_state.worker_count(),
        settings.max_workers,
    )
    await gateway_state.notify_pool_status()

    try:
        await websocket.send_text('{"type":"connected"}')
        while True:
            raw = await websocket.receive_text()
            try:
                message = json.loads(raw)
            except json.JSONDecodeError:
                logger.warning("invalid JSON from worker %s", worker_id)
                continue

            message["worker_id"] = worker_id
            msg_type = message.get("type")
            if msg_type not in (
                "worker_hello",
                "task_accept",
                "task_reject",
                "task_result",
            ):
                logger.debug("ignored worker message type %s from %s", msg_type, worker_id)
                continue

            await _handle_worker_message(worker_id, message)

    except WebSocketDisconnect:
        pass
    except Exception as exc:
        logger.warning("worker WS error for %s: %s", worker_id, exc)
    finally:
        gateway_state.unregister_worker_session(worker_id)
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
