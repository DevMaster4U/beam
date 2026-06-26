"""
In-process worker gateway.

Workers connect via WebSocket to /ws/{worker_id}?api_key=...
The orchestrator forwards task offer batch items as task_offer messages,
and relays task_accept / task_reject / task_result upstream.
"""

import asyncio
import json
import logging
import time
from typing import Callable, Dict, Optional

from core.relay_log import defer_relay_log, is_failure_summary, log_relay, relay_summary, short_id

logger = logging.getLogger(__name__)

MAX_WORKERS = 10


class WorkerGateway:
    """Manages WebSocket sessions for locally-connected workers."""

    def __init__(
        self,
        on_ready_change: Optional[Callable[[bool], None]] = None,
    ) -> None:
        self._sessions: Dict[str, object] = {}  # worker_id → WebSocket
        self._worker_versions: Dict[str, str] = {}
        self._cursor = 0
        self._on_ready_change = on_ready_change
        self._upstream: Optional[object] = None  # SubnetCoreClient ref
        self._outbound_send: Optional[Callable] = None

    def set_upstream(self, upstream: object) -> None:
        self._upstream = upstream

    def set_outbound_sender(self, sender: Callable) -> None:
        """Send payloads to workers via global gateway (or other external transport)."""
        self._outbound_send = sender

    @property
    def connected_count(self) -> int:
        return len(self._sessions)

    @property
    def worker_ids(self) -> list:
        return list(self._sessions.keys())

    def is_full(self) -> bool:
        return len(self._sessions) >= MAX_WORKERS

    def connect(self, worker_id: str, ws: object) -> bool:
        if self.is_full() and worker_id not in self._sessions:
            logger.warning("Worker cap reached (%d); rejecting %s", MAX_WORKERS, worker_id)
            return False
        was_empty = len(self._sessions) == 0
        self._sessions[worker_id] = ws
        version = self._worker_versions.get(worker_id, "?")
        logger.info(
            "Worker connected: %s version=%s (%d/%d)",
            worker_id,
            version,
            len(self._sessions),
            MAX_WORKERS,
        )
        if was_empty and self._on_ready_change:
            self._on_ready_change(True)
        return True

    def note_worker_version(self, worker_id: str, worker_version: str) -> None:
        if worker_version.strip():
            self._worker_versions[worker_id] = worker_version.strip()

    def disconnect(self, worker_id: str) -> None:
        self._sessions.pop(worker_id, None)
        self._worker_versions.pop(worker_id, None)
        logger.info("Worker disconnected: %s (%d/%d)", worker_id, len(self._sessions), MAX_WORKERS)
        if len(self._sessions) == 0 and self._on_ready_change:
            self._on_ready_change(False)

    async def deliver_task_offer(self, worker_id: str, offer: dict) -> bool:
        ws = self._sessions.get(worker_id)
        if ws is None:
            logger.warning("deliver_task_offer: worker %s not connected", worker_id)
            return False
        try:
            await ws.send_text(json.dumps({"type": "task_offer", **offer}))
            return True
        except Exception as exc:
            logger.warning("deliver_task_offer send failed for %s: %s", worker_id, exc)
            self._sessions.pop(worker_id, None)
            return False

    def get_workers_round_robin(self, n: int = 1) -> list:
        """Return up to n worker_ids in round-robin order."""
        ids = list(self._sessions.keys())
        if not ids:
            return []
        selected = []
        for _ in range(min(n, len(ids))):
            selected.append(ids[self._cursor % len(ids)])
            self._cursor += 1
        return selected

    async def handle_worker_message(self, worker_id: str, raw: str) -> None:
        """Process an inbound message from a connected worker."""
        try:
            msg = json.loads(raw)
        except json.JSONDecodeError:
            logger.warning("Non-JSON from worker %s", worker_id)
            return

        msg_type = msg.get("type")
        if msg_type == "worker_hello":
            worker_version = str(msg.get("worker_version") or "").strip()
            if worker_version:
                self._worker_versions[worker_id] = worker_version
            logger.info(
                "Worker hello: %s version=%s max_tasks=%s",
                worker_id,
                worker_version or self._worker_versions.get(worker_id) or "?",
                msg.get("max_concurrent_tasks"),
            )
        elif msg_type in ("task_accept", "task_reject"):
            task_id = msg.get("task_id") or msg.get("offer_id")
            log_relay(
                f"worker ws <- recv type={msg_type} worker={short_id(worker_id)} "
                f"task={short_id(task_id)} offer={short_id(msg.get('offer_id') or task_id)}"
            )
            await self._relay_task_decision(worker_id, msg)
        elif msg_type == "task_result":
            log_relay(
                f"worker ws <- recv type=task_result worker={short_id(worker_id)} "
                f"task={short_id(msg.get('task_id'))} offer={short_id(msg.get('offer_id') or msg.get('task_id'))} "
                f"success={msg.get('success')} bytes={msg.get('bytes_transferred')}"
            )
            await self._relay_task_result(worker_id, msg)
        else:
            logger.debug("Unhandled worker message type %s from %s", msg_type, worker_id)

    async def _send_to_worker(self, worker_id: str, payload: dict) -> None:
        if self._outbound_send is not None:
            try:
                result = self._outbound_send(worker_id, payload)
                if asyncio.iscoroutine(result):
                    await result
            except Exception as exc:
                logger.warning("outbound worker send failed for %s: %s", worker_id, exc)
            return

        ws = self._sessions.get(worker_id)
        if ws is None:
            return
        try:
            await ws.send_text(json.dumps(payload))
        except Exception as exc:
            logger.warning("worker ack send failed for %s: %s", worker_id, exc)
            self._sessions.pop(worker_id, None)

    async def _relay_task_decision(self, worker_id: str, msg: dict) -> None:
        ack_type = "task_accept_ack" if msg.get("type") == "task_accept" else "task_reject_ack"
        task_id = msg.get("task_id") or msg.get("offer_id")
        offer_id = msg.get("offer_id") or task_id
        reason = msg.get("reason")
        started = time.monotonic()
        failed = False
        if self._upstream is None:
            logger.warning(
                "worker relay blocked: no beamcore upstream worker=%s type=%s task=%s offer=%s",
                short_id(worker_id),
                msg.get("type"),
                short_id(task_id),
                short_id(offer_id),
            )
            await self._send_to_worker(
                worker_id,
                {"type": ack_type, "task_id": task_id, "offer_id": offer_id, "accepted": False, "reason": "beamcore_unavailable"},
            )
            return
        try:
            if msg.get("type") == "task_accept":
                ack = await self._upstream.send_task_accept(
                    task_id=task_id,
                    worker_id=worker_id,
                    offer_id=offer_id,
                    worker_version=msg.get("worker_version"),
                )
            else:
                ack = await self._upstream.send_task_reject(
                    task_id=task_id,
                    worker_id=worker_id,
                    offer_id=offer_id,
                    reason=reason,
                )
        except Exception as exc:
            failed = True
            elapsed_ms = (time.monotonic() - started) * 1000
            logger.warning(
                "worker relay -> beamcore failed type=%s worker=%s task=%s offer=%s "
                "latency_ms=%.1f err=%s",
                msg.get("type"),
                short_id(worker_id),
                short_id(task_id),
                short_id(offer_id),
                elapsed_ms,
                exc,
            )
            ack = {
                "type": ack_type,
                "task_id": task_id,
                "offer_id": offer_id,
                "accepted": False,
                "reason": "beamcore_decision_forward_failed",
            }
        ack_payload = {
            **(ack if isinstance(ack, dict) else {}),
            "type": ack_type,
            "task_id": task_id,
            "offer_id": offer_id,
        }
        summary = relay_summary(ack_payload)
        failed = failed or is_failure_summary(summary)
        await self._send_to_worker(worker_id, ack_payload)
        elapsed_ms = (time.monotonic() - started) * 1000
        defer_relay_log(
            f"worker ws -> send type={ack_type} worker={short_id(worker_id)} "
            f"task={short_id(task_id)} offer={short_id(offer_id)} {summary}",
            latency_ms=elapsed_ms,
            force_info=failed,
        )

    async def _relay_task_result(self, worker_id: str, msg: dict) -> None:
        started = time.monotonic()
        failed = False
        if self._upstream is None:
            logger.warning(
                "worker relay blocked: no beamcore upstream worker=%s type=task_result task=%s offer=%s",
                short_id(worker_id),
                short_id(msg.get("task_id")),
                short_id(msg.get("offer_id") or msg.get("task_id")),
            )
            await self._send_to_worker(
                worker_id,
                {
                    "type": "task_result_ack",
                    "task_id": msg.get("task_id"),
                    "offer_id": msg.get("offer_id"),
                    "received": False,
                    "completed": False,
                    "reason": "beamcore_unavailable",
                },
            )
            return
        try:
            task_id = msg.get("task_id")
            offer_id = msg.get("offer_id") or task_id
            if not task_id or not offer_id:
                logger.warning("dropping task_result missing task_id/offer_id from worker=%s", worker_id)
                await self._send_to_worker(
                    worker_id,
                    {
                        "type": "task_result_ack",
                        "task_id": task_id,
                        "offer_id": offer_id,
                        "received": False,
                        "completed": False,
                        "reason": "missing_task_or_offer_id",
                    },
                )
                return
            payload = {
                "type": "task_result",
                "task_id": task_id,
                "offer_id": offer_id,
                "worker_id": worker_id,
                "success": bool(msg.get("success")),
            }
            for key in ("etag", "chunk_hash", "error"):
                if msg.get(key) is not None:
                    payload[key] = msg[key]
            ack = await self._upstream.send_task_result(payload)
        except Exception as exc:
            failed = True
            elapsed_ms = (time.monotonic() - started) * 1000
            logger.warning(
                "worker relay -> beamcore failed type=task_result worker=%s task=%s offer=%s "
                "latency_ms=%.1f err=%s",
                short_id(worker_id),
                short_id(msg.get("task_id")),
                short_id(msg.get("offer_id") or msg.get("task_id")),
                elapsed_ms,
                exc,
            )
            ack = {
                "type": "task_result_ack",
                "task_id": msg.get("task_id"),
                "offer_id": msg.get("offer_id") or msg.get("task_id"),
                "received": False,
                "completed": False,
                "reason": "beamcore_result_forward_failed",
            }
        ack_payload = {
            **(ack if isinstance(ack, dict) else {}),
            "type": "task_result_ack",
            "task_id": msg.get("task_id"),
            "offer_id": msg.get("offer_id") or msg.get("task_id"),
        }
        summary = relay_summary(ack_payload)
        failed = failed or is_failure_summary(summary)
        await self._send_to_worker(worker_id, ack_payload)
        elapsed_ms = (time.monotonic() - started) * 1000
        defer_relay_log(
            f"worker ws -> send type=task_result_ack worker={short_id(worker_id)} "
            f"task={short_id(ack_payload.get('task_id'))} offer={short_id(ack_payload.get('offer_id'))} "
            f"{summary}",
            latency_ms=elapsed_ms,
            force_info=failed,
        )
