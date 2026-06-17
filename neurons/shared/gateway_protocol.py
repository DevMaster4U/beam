"""
Beam worker-gateway WebSocket protocol types (public + dedicated).

Reference:
- https://data.b1m.ai/guide/workers
- https://data.b1m.ai/guide/orchestrators#dedicated-gateway-websocket-protocol
"""

from __future__ import annotations

from enum import Enum
from typing import Any, Literal, Optional


class GatewayMode(str, Enum):
    """Orchestrator worker-pool topology."""

    PUBLIC = "public"  # Option 2 — shared BeamCore worker gateway
    DEDICATED = "dedicated"  # Option 1 — orchestrator-owned gateway


def parse_gateway_mode(value: Optional[str]) -> GatewayMode:
    if not value or not str(value).strip():
        return GatewayMode.PUBLIC
    normalized = str(value).strip().lower()
    aliases = {
        "public": GatewayMode.PUBLIC,
        "shared": GatewayMode.PUBLIC,
        "option2": GatewayMode.PUBLIC,
        "2": GatewayMode.PUBLIC,
        "dedicated": GatewayMode.DEDICATED,
        "owned": GatewayMode.DEDICATED,
        "orch_owned": GatewayMode.DEDICATED,
        "option1": GatewayMode.DEDICATED,
        "1": GatewayMode.DEDICATED,
    }
    if normalized not in aliases:
        raise ValueError(
            f"Invalid WORKER_GATEWAY_MODE={value!r}; "
            "use 'public' (Option 2) or 'dedicated' (Option 1)"
        )
    return aliases[normalized]


# ---------------------------------------------------------------------------
# Worker ↔ gateway (data path)
# ---------------------------------------------------------------------------

WorkerToGatewayType = Literal[
    "task_accept",
    "task_reject",
    "task_result",
    "stats_snapshot",
    "task_transfer_progress",
    "bw_challenge_response",
]

GatewayToWorkerType = Literal[
    "connected",
    "task_offer",
    "task_accept_ack",
    "task_result_ack",
    "stats_snapshot_ack",
    "session_displaced",
    "error",
]


def build_task_offer_for_worker(offer: dict[str, Any]) -> dict[str, Any]:
    """Flatten a BeamCore worker_task_offer.offer into a worker task_offer message."""
    if offer.get("type") == "task_offer":
        return offer
    return {"type": "task_offer", **offer}


def build_worker_response_from_accept(message: dict[str, Any]) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "type": "worker_response",
        "task_id": message.get("task_id"),
        "offer_id": message.get("offer_id") or message.get("task_id"),
        "worker_id": message.get("worker_id"),
        "decision": "task_accept",
    }
    if message.get("worker_version"):
        payload["worker_version"] = message["worker_version"]
    return payload


def build_worker_response_from_reject(message: dict[str, Any]) -> dict[str, Any]:
    return {
        "type": "worker_response",
        "task_id": message.get("task_id"),
        "offer_id": message.get("offer_id") or message.get("task_id"),
        "worker_id": message.get("worker_id"),
        "decision": "task_reject",
        "reason": message.get("reason", "rejected"),
    }


def build_task_accept_ack(
    *,
    task_id: str,
    offer_id: str,
    accepted: bool,
    reason: Optional[str] = None,
) -> dict[str, Any]:
    return {
        "type": "task_accept_ack",
        "task_id": task_id,
        "offer_id": offer_id,
        "accepted": accepted,
        "reason": reason,
    }


def build_task_result_ack(
    *,
    task_id: str,
    offer_id: str,
    received: bool,
    completed: Optional[bool] = None,
    reason: Optional[str] = None,
) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "type": "task_result_ack",
        "task_id": task_id,
        "offer_id": offer_id,
        "received": received,
    }
    if completed is not None:
        payload["completed"] = completed
    if reason is not None:
        payload["reason"] = reason
    return payload


# Backward-compatible alias (deprecated)
build_task_result_summary_ack = build_task_result_ack


# ---------------------------------------------------------------------------
# Orchestrator ↔ BeamCore (orch-gateway)
# ---------------------------------------------------------------------------

def build_beamcore_worker_response(message: dict[str, Any]) -> dict[str, Any]:
    """Normalize gateway worker_response before relay to orch-gateway."""
    decision = message.get("decision", "task_accept")
    if decision in ("accept", "task_accept"):
        decision = "task_accept"
    elif decision in ("reject", "task_reject"):
        decision = "task_reject"

    payload: dict[str, Any] = {
        "type": "worker_response",
        "task_id": message.get("task_id"),
        "offer_id": message.get("offer_id") or message.get("task_id"),
        "worker_id": message.get("worker_id"),
        "decision": decision,
    }
    if message.get("reason"):
        payload["reason"] = message["reason"]
    return payload


def build_beamcore_task_result(message: dict[str, Any]) -> dict[str, Any]:
    """
    Normalize worker task_result for BeamCore relay.

    BeamCore derives verified bytes from trusted task metadata; workers send
    success, chunk_hash, etag, and error only (see data.b1m.ai guide).
    """
    payload: dict[str, Any] = {
        "type": "task_result",
        "task_id": message.get("task_id"),
        "offer_id": message.get("offer_id") or message.get("task_id"),
        "worker_id": message.get("worker_id"),
        "success": bool(message.get("success", False)),
    }
    for field in ("chunk_hash", "etag", "error"):
        if message.get(field) is not None:
            payload[field] = message[field]
    return payload


# Backward-compatible alias (deprecated)
build_beamcore_task_result_summary = build_beamcore_task_result


def parse_worker_response_ack(data: dict[str, Any]) -> tuple[bool, Optional[str]]:
    if data.get("type") == "error":
        return False, data.get("message") or data.get("error")
    if data.get("type") == "worker_response_ack":
        return bool(data.get("accepted", False)), data.get("reason")
    return bool(data.get("accepted", True)), data.get("reason")


def parse_task_result_ack(data: dict[str, Any]) -> tuple[bool, Optional[bool], Optional[str]]:
    if data.get("type") == "error":
        return False, None, data.get("message") or data.get("error")
    if data.get("type") in ("task_result_ack", "task_result_summary_ack"):
        received = bool(data.get("received", False))
        completed_raw = data.get("completed")
        completed = bool(completed_raw) if completed_raw is not None else received
        return received, completed, data.get("reason")
    received = bool(data.get("received", True))
    completed_raw = data.get("completed")
    completed = bool(completed_raw) if completed_raw is not None else received
    return received, completed, data.get("reason")


# Backward-compatible alias (deprecated)
parse_task_result_summary_ack = parse_task_result_ack


# ---------------------------------------------------------------------------
# Control channel (orchestrator ↔ worker-gateway)
# ---------------------------------------------------------------------------

ControlToGatewayType = Literal[
    "list_workers",
    "task_offer",
    "task_accept_ack",
    "task_result_ack",
    "ping",
]

GatewayToControlType = Literal[
    "control_connected",
    "worker_list",
    "worker_connected",
    "worker_disconnected",
    "worker_response",
    "task_result",
    "worker_capacity_update",
    "task_transfer_progress",
    "bw_challenge_response",
    "pong",
]


def normalize_worker_for_assignment(
    worker: dict[str, Any],
    *,
    default_trust: float = 0.5,
    default_bandwidth: float = 100.0,
) -> dict[str, Any]:
    """Ensure assignment scoring fields exist for both public and dedicated pools."""
    worker_id = worker.get("worker_id")
    if not worker_id:
        raise ValueError("worker entry missing worker_id")

    trust_raw = worker.get("trust_score")
    bandwidth_raw = worker.get("bandwidth_mbps")
    return {
        **worker,
        "worker_id": worker_id,
        "trust_score": float(trust_raw if trust_raw is not None else default_trust),
        "bandwidth_mbps": float(bandwidth_raw if bandwidth_raw is not None else default_bandwidth),
    }
