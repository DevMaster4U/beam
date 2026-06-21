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


def _profile_fields(profile_data: Optional[dict]) -> dict[str, Any]:
    if not profile_data:
        return {
            "ip": "",
            "claimed_bandwidth_mbps": 0.0,
            "trust_score": 0.5,
            "success_rate": 1.0,
            "max_concurrent_tasks": 5,
        }
    return {
        "ip": str(profile_data.get("ip") or "").strip(),
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
        try:
            claimed = float(claimed_raw) if claimed_raw is not None else None
        except (TypeError, ValueError):
            claimed = None
        gateway_state.update_worker_hello(worker_id, ip=ip or None, claimed_bandwidth_mbps=claimed)
        profile = gateway_state.get_profile(worker_id)
        logger.info(
            "worker_hello %s ip=%s avg_mbps=%.1f tasks=%d",
            worker_id,
            profile.ip or "?",
            profile.average_mbps,
            profile.transfer_count,
        )
        return

    if msg_type == "task_reject":
        gateway_state.mark_worker_idle(worker_id)
    elif msg_type == "task_result":
        gateway_state.observe_worker_transfer(
            worker_id,
            message.get("transfer_mbps"),
            success=bool(message.get("success", False)),
        )
        gateway_state.mark_worker_idle(worker_id)
        profile = gateway_state.get_profile(worker_id)
        logger.info(
            "worker %s transfer observed avg_mbps=%.1f (n=%d) success_rate=%.3f",
            worker_id,
            profile.average_mbps,
            profile.transfer_count,
            profile.success_rate,
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
    fields = _profile_fields(profile_data)
    gateway_state.register_worker_session(
        worker_id,
        websocket,
        ip=fields["ip"],
        claimed_bandwidth_mbps=fields["claimed_bandwidth_mbps"],
        trust_score=fields["trust_score"],
        success_rate=fields["success_rate"],
        max_concurrent_tasks=fields["max_concurrent_tasks"],
    )
    profile = gateway_state.get_profile(worker_id)
    logger.info(
        "Worker connected: %s ip=%s avg_mbps=%.1f score=%.4f (%d/%d)",
        worker_id,
        profile.ip or "?",
        profile.average_mbps,
        profile.score(gateway_state.scoring_weights),
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
