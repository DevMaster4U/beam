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
from dataclasses import dataclass, field
from typing import Callable, Dict, Optional, Set

from core.relay_log import defer_relay_log, is_failure_summary, log_relay, relay_summary, short_id

logger = logging.getLogger(__name__)

MAX_WORKERS = 10


@dataclass
class _WorkerProfile:
    worker_id: str
    ip: str = ""
    max_concurrent_tasks: int = 5
    worker_version: str = ""
    initial_order: int = 0
    claimed_bandwidth_mbps: float = 0.0
    transfer_mbps_sum: float = 0.0
    transfer_count: int = 0
    active_offer_ids: Set[str] = field(default_factory=set)

    @property
    def active_count(self) -> int:
        return len(self.active_offer_ids)

    @property
    def has_capacity(self) -> bool:
        return self.active_count < self.max_concurrent_tasks

    @property
    def average_mbps(self) -> float:
        if self.transfer_count > 0:
            return self.transfer_mbps_sum / self.transfer_count
        if self.claimed_bandwidth_mbps > 0:
            return self.claimed_bandwidth_mbps
        return 0.0

    def round_robin_sort_key(self) -> tuple:
        if self.active_count == 0:
            return (0, -self.initial_order, self.worker_id)
        return (1, -self.average_mbps, self.worker_id)

    def observe_transfer(self, transfer_mbps: Optional[float]) -> None:
        if transfer_mbps is None:
            return
        try:
            mbps = float(transfer_mbps)
        except (TypeError, ValueError):
            return
        if mbps > 0:
            self.transfer_mbps_sum += mbps
            self.transfer_count += 1


class WorkerGateway:
    """Manages WebSocket sessions for locally-connected workers."""

    def __init__(
        self,
        on_ready_change: Optional[Callable[[bool], None]] = None,
    ) -> None:
        self._sessions: Dict[str, object] = {}  # worker_id → WebSocket
        self._profiles: Dict[str, _WorkerProfile] = {}
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

    def _get_profile(self, worker_id: str) -> _WorkerProfile:
        profile = self._profiles.get(worker_id)
        if profile is None:
            profile = _WorkerProfile(worker_id=worker_id)
            self._profiles[worker_id] = profile
        return profile

    def connect(self, worker_id: str, ws: object, *, ip: str = "") -> bool:
        if self.is_full() and worker_id not in self._sessions:
            logger.warning("Worker cap reached (%d); rejecting %s", MAX_WORKERS, worker_id)
            return False
        was_empty = len(self._sessions) == 0
        self._sessions[worker_id] = ws
        profile = self._get_profile(worker_id)
        if ip.strip():
            profile.ip = ip.strip()
        logger.info(
            "Worker connected: %s version=%s ip=%s (%d/%d)",
            worker_id,
            profile.worker_version or "?",
            profile.ip or "?",
            len(self._sessions),
            MAX_WORKERS,
        )
        if was_empty and self._on_ready_change:
            self._on_ready_change(True)
        return True

    def note_worker_version(self, worker_id: str, worker_version: str) -> None:
        if worker_version.strip():
            self._get_profile(worker_id).worker_version = worker_version.strip()

    def update_worker_hello(
        self,
        worker_id: str,
        *,
        ip: Optional[str] = None,
        max_concurrent_tasks: Optional[int] = None,
        worker_version: Optional[str] = None,
        initial_order: Optional[int] = None,
        claimed_bandwidth_mbps: Optional[float] = None,
    ) -> None:
        profile = self._get_profile(worker_id)
        if ip and ip.strip():
            profile.ip = ip.strip()
        if max_concurrent_tasks is not None and max_concurrent_tasks > 0:
            profile.max_concurrent_tasks = int(max_concurrent_tasks)
        if worker_version and worker_version.strip():
            profile.worker_version = worker_version.strip()
        if initial_order is not None:
            profile.initial_order = int(initial_order)
        if claimed_bandwidth_mbps is not None and claimed_bandwidth_mbps > 0:
            if profile.claimed_bandwidth_mbps <= 0:
                profile.claimed_bandwidth_mbps = float(claimed_bandwidth_mbps)

    def _ordered_connected_worker_ids(self) -> list[str]:
        return sorted(
            self._sessions.keys(),
            key=lambda wid: self._get_profile(wid).round_robin_sort_key(),
        )

    def mark_worker_busy(self, worker_id: str, offer_id: Optional[str] = None) -> None:
        if offer_id:
            self._get_profile(worker_id).active_offer_ids.add(str(offer_id))

    def mark_worker_idle(self, worker_id: str, offer_id: Optional[str] = None) -> None:
        profile = self._get_profile(worker_id)
        if offer_id:
            profile.active_offer_ids.discard(str(offer_id))
        else:
            profile.active_offer_ids.clear()

    def disconnect(self, worker_id: str) -> None:
        self._sessions.pop(worker_id, None)
        profile = self._profiles.get(worker_id)
        if profile:
            profile.active_offer_ids.clear()
        logger.info("Worker disconnected: %s (%d/%d)", worker_id, len(self._sessions), MAX_WORKERS)
        if len(self._sessions) == 0 and self._on_ready_change:
            self._on_ready_change(False)

    async def deliver_task_offer(
        self,
        worker_id: str,
        offer: dict,
        *,
        mark_busy: bool = True,
    ) -> bool:
        ws = self._sessions.get(worker_id)
        if ws is None:
            logger.warning("deliver_task_offer: worker %s not connected", worker_id)
            return False
        try:
            await ws.send_text(json.dumps({"type": "task_offer", **offer}))
            if mark_busy:
                offer_id = offer.get("offer_id") or offer.get("task_id")
                if offer_id:
                    self.mark_worker_busy(worker_id, str(offer_id))
            return True
        except Exception as exc:
            logger.warning("deliver_task_offer send failed for %s: %s", worker_id, exc)
            self._sessions.pop(worker_id, None)
            return False

    def select_worker_round_robin(
        self,
        batch_used_ips: Optional[set[str]] = None,
        batch_assigned_workers: Optional[set[str]] = None,
    ) -> Optional[str]:
        """Pick the next worker with capacity, matching global-gateway batch IP spread."""
        connected = self._ordered_connected_worker_ids()
        if not connected:
            return None

        pool_size = len(connected)
        start = self._cursor % pool_size
        in_batch = batch_used_ips is not None or batch_assigned_workers is not None

        def _eligible(worker_id: str, *, allow_used_ip: bool) -> bool:
            profile = self._get_profile(worker_id)
            if not profile.has_capacity:
                return False
            if batch_assigned_workers and worker_id in batch_assigned_workers:
                return False
            ip = profile.ip.strip()
            if (
                not allow_used_ip
                and batch_used_ips is not None
                and ip
                and ip in batch_used_ips
            ):
                return False
            return True

        def _pick(allow_used_ip: bool) -> Optional[str]:
            for offset in range(pool_size):
                idx = (start + offset) % pool_size
                worker_id = connected[idx]
                if not _eligible(worker_id, allow_used_ip=allow_used_ip):
                    continue
                self._cursor = (idx + 1) % pool_size
                profile = self._get_profile(worker_id)
                logger.debug(
                    "selected worker %s round_robin ip=%s active=%d/%d "
                    "cursor=%d pool=%d batch_ips=%s",
                    worker_id,
                    profile.ip or "?",
                    profile.active_count,
                    profile.max_concurrent_tasks,
                    self._cursor,
                    pool_size,
                    ",".join(sorted(batch_used_ips)) if batch_used_ips else "-",
                )
                return worker_id
            return None

        if in_batch:
            worker_id = _pick(allow_used_ip=False)
            if worker_id:
                return worker_id
            return _pick(allow_used_ip=True)

        return _pick(allow_used_ip=True)

    def get_workers_round_robin(self, n: int = 1) -> list[str]:
        """Return up to n worker_ids with batch-aware round-robin (IP + capacity)."""
        selected: list[str] = []
        if n <= 0:
            return selected

        batch_used_ips: set[str] = set()
        batch_assigned_workers: set[str] = set()
        for _ in range(n):
            worker_id = self.select_worker_round_robin(
                batch_used_ips=batch_used_ips,
                batch_assigned_workers=batch_assigned_workers,
            )
            if not worker_id:
                break
            selected.append(worker_id)
            batch_assigned_workers.add(worker_id)
            ip = self._get_profile(worker_id).ip.strip()
            if ip:
                batch_used_ips.add(ip)
        return selected

    async def deliver_task_offer_batch(self, offers: list[dict]) -> tuple[int, int]:
        """Deliver a task offer batch with global-gateway-style worker selection."""
        delivered = 0
        failed = 0
        batch_used_ips: set[str] = set()
        batch_assigned_workers: set[str] = set()

        for offer in offers:
            if not isinstance(offer, dict):
                failed += 1
                continue

            worker_id = self.select_worker_round_robin(
                batch_used_ips=batch_used_ips,
                batch_assigned_workers=batch_assigned_workers,
            )
            if not worker_id:
                logger.warning(
                    "No local worker with capacity for batch offer task=%s",
                    offer.get("task_id"),
                )
                failed += 1
                continue

            if await self.deliver_task_offer(worker_id, offer):
                delivered += 1
                batch_assigned_workers.add(worker_id)
                ip = self._get_profile(worker_id).ip.strip()
                if ip:
                    batch_used_ips.add(ip)
            else:
                failed += 1
                logger.warning(
                    "Failed to forward task offer to local worker: worker=%s task=%s",
                    worker_id,
                    offer.get("task_id"),
                )

        return delivered, failed

    async def handle_worker_message(self, worker_id: str, raw: str) -> None:
        """Process an inbound message from a connected worker."""
        try:
            msg = json.loads(raw)
        except json.JSONDecodeError:
            logger.warning("Non-JSON from worker %s", worker_id)
            return

        msg_type = msg.get("type")
        if msg_type == "worker_hello":
            ip = str(msg.get("ip") or "").strip()
            max_tasks_raw = msg.get("max_concurrent_tasks")
            worker_version = str(msg.get("worker_version") or "").strip()
            max_concurrent: Optional[int] = None
            if max_tasks_raw is not None:
                try:
                    max_concurrent = int(max_tasks_raw)
                except (TypeError, ValueError):
                    max_concurrent = None
            initial_order_raw = msg.get("initial_order")
            initial_order: Optional[int] = None
            if initial_order_raw is not None:
                try:
                    initial_order = int(initial_order_raw)
                except (TypeError, ValueError):
                    initial_order = None
            claimed_raw = msg.get("claimed_bandwidth_mbps")
            claimed: Optional[float] = None
            if claimed_raw is not None:
                try:
                    claimed = float(claimed_raw)
                except (TypeError, ValueError):
                    claimed = None
            self.update_worker_hello(
                worker_id,
                ip=ip or None,
                max_concurrent_tasks=max_concurrent,
                worker_version=worker_version or None,
                initial_order=initial_order,
                claimed_bandwidth_mbps=claimed,
            )
            profile = self._get_profile(worker_id)
            logger.info(
                "Worker hello: %s version=%s ip=%s max_tasks=%d active=%d initial_order=%d",
                worker_id,
                profile.worker_version or "?",
                profile.ip or "?",
                profile.max_concurrent_tasks,
                profile.active_count,
                profile.initial_order,
            )
        elif msg_type in ("task_accept", "task_reject"):
            task_id = msg.get("task_id") or msg.get("offer_id")
            log_relay(
                f"worker ws <- recv type={msg_type} worker={short_id(worker_id)} "
                f"task={short_id(task_id)} offer={short_id(msg.get('offer_id') or task_id)}"
            )
            await self._relay_task_decision(worker_id, msg)
            if msg_type == "task_reject":
                offer_id = msg.get("offer_id") or msg.get("task_id")
                self.mark_worker_idle(worker_id, str(offer_id) if offer_id else None)
        elif msg_type == "task_result":
            log_relay(
                f"worker ws <- recv type=task_result worker={short_id(worker_id)} "
                f"task={short_id(msg.get('task_id'))} offer={short_id(msg.get('offer_id') or msg.get('task_id'))} "
                f"success={msg.get('success')} bytes={msg.get('bytes_transferred')}"
            )
            await self._relay_task_result(worker_id, msg)
            transfer_mbps = msg.get("transfer_mbps")
            if transfer_mbps is not None:
                self._get_profile(worker_id).observe_transfer(transfer_mbps)
            offer_id = msg.get("offer_id") or msg.get("task_id")
            self.mark_worker_idle(worker_id, str(offer_id) if offer_id else None)
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
