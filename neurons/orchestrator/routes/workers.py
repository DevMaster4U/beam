"""
Worker WebSocket gateway route (in-process mode).

Workers connect via:
  GET /ws/{worker_id}?api_key=<beamcore_api_key>&worker_secret=<shared_secret>

Hidden / simple workers (transfer-only):
  GET /ws/{worker_id}?hidden=1&worker_secret=<shared_secret>
  — secret required; BeamCore api_key validation is skipped.

When WORKER_GATEWAY_SECRET is set on the gateway, workers must send a matching worker_secret.

Orchestrator talks to WorkerGateway in-process (set_worker_gateway); no control WebSocket.
"""

import logging
from typing import Optional

import httpx
from fastapi import APIRouter, WebSocket, WebSocketDisconnect, status
from fastapi.websockets import WebSocketState

from core.orchestrator import get_orchestrator

logger = logging.getLogger(__name__)

router = APIRouter(tags=["workers"])

_VALIDATE_TIMEOUT = 5.0  # seconds — how long to wait for BeamCore key validation


async def _validate_worker_api_key(core_url: str, worker_id: str, api_key: str) -> bool:
    """Return True iff BeamCore confirms this api_key belongs to worker_id."""
    try:
        async with httpx.AsyncClient(timeout=_VALIDATE_TIMEOUT) as client:
            resp = await client.get(
                f"{core_url.rstrip('/')}/workers/{worker_id}",
                headers={"x-api-key": api_key},
            )
            if resp.status_code == 200:
                data = resp.json()
                return data.get("worker_id") == worker_id
            return False
    except Exception as exc:
        logger.warning("BeamCore key validation failed for %s: %s", worker_id, exc)
        return False


def _provided_worker_secret(websocket: WebSocket) -> str:
    return (
        websocket.query_params.get("worker_secret", "").strip()
        or websocket.headers.get("x-worker-secret", "").strip()
    )


@router.websocket("/ws/{worker_id}")
async def worker_ws(websocket: WebSocket, worker_id: str) -> None:
    orchestrator = get_orchestrator()
    if orchestrator is None:
        await websocket.close(code=status.WS_1013_TRY_AGAIN_LATER)
        return

    gateway = getattr(orchestrator, "worker_gateway", None)
    if gateway is None:
        await websocket.close(code=status.WS_1013_TRY_AGAIN_LATER)
        return

    hidden = websocket.query_params.get("hidden", "").strip().lower() in ("1", "true", "yes")

    configured_secret: Optional[str] = orchestrator.settings.worker_gateway_worker_secret
    if not configured_secret:
        logger.warning(
            "Worker %s rejected: WORKER_GATEWAY_SECRET is not configured on this gateway",
            worker_id,
        )
        await websocket.close(code=status.WS_1008_POLICY_VIOLATION)
        return

    provided_secret = _provided_worker_secret(websocket)
    if not provided_secret or provided_secret != configured_secret:
        logger.warning("Worker %s: worker_secret missing or invalid", worker_id)
        await websocket.close(code=status.WS_1008_POLICY_VIOLATION)
        return

    if hidden:
        if not worker_id or len(worker_id) < 4:
            logger.warning("Hidden worker %s rejected: invalid worker_id", worker_id)
            await websocket.close(code=status.WS_1008_POLICY_VIOLATION)
            return
    else:
        api_key = websocket.query_params.get("api_key") or ""
        if not api_key:
            await websocket.close(code=status.WS_1008_POLICY_VIOLATION)
            return

        core_url = orchestrator.settings.core_server_url
        if not await _validate_worker_api_key(core_url, worker_id, api_key):
            logger.warning("Worker %s: API key validation failed", worker_id)
            await websocket.close(code=status.WS_1008_POLICY_VIOLATION)
            return

    # --- Capacity check ---
    if gateway.is_full() and worker_id not in gateway.worker_ids:
        await websocket.close(code=status.WS_1013_TRY_AGAIN_LATER)
        return

    await websocket.accept()

    peer_ip = ""
    if websocket.client:
        peer_ip = websocket.client.host or ""

    worker_version = str(websocket.query_params.get("worker_version") or "").strip()
    if worker_version:
        gateway.note_worker_version(worker_id, worker_version)

    if not gateway.connect(worker_id, websocket, ip=peer_ip, hidden=hidden):
        await websocket.close(code=status.WS_1013_TRY_AGAIN_LATER)
        return

    try:
        await websocket.send_text('{"type":"connected"}')

        while True:
            raw = await websocket.receive_text()
            await gateway.handle_worker_message(worker_id, raw)

    except WebSocketDisconnect:
        pass
    except Exception as exc:
        logger.warning("Worker WS error for %s: %s", worker_id, exc)
    finally:
        gateway.disconnect(worker_id)
        if websocket.client_state != WebSocketState.DISCONNECTED:
            try:
                await websocket.close()
            except Exception:
                pass
