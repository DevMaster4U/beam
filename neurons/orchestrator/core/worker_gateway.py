"""
In-process worker gateway.

Workers connect via WebSocket to /ws/{worker_id}?api_key=...
The orchestrator forwards task offer batch items as task_offer messages,
and relays task_result messages upstream.
"""

import asyncio
import os
from collections import deque
import json
import logging
from dataclasses import dataclass, field
from typing import Awaitable, Callable, Dict, Optional, Set

from core.relay_log import (
    chunk_id_from_transfer_context,
    log_relay,
    short_id,
    transfer_context_range_label,
    transfer_context_urls,
)

logger = logging.getLogger(__name__)

try:
    MAX_WORKERS = max(1, int(os.environ.get("ORCH_WORKER_GATEWAY_MAX_WORKERS", "10")))
except ValueError:
    MAX_WORKERS = 10
RESULT_FORWARD_RETRY_BASE_SECONDS = max(0.0, float(os.environ.get("ORCH_RESULT_FORWARD_RETRY_BASE_SECONDS", "0.25")))
RESULT_FORWARD_RETRY_MAX_SECONDS = max(
    RESULT_FORWARD_RETRY_BASE_SECONDS,
    float(os.environ.get("ORCH_RESULT_FORWARD_RETRY_MAX_SECONDS", "2.0")),
)
RESULT_TERMINAL_CACHE_SIZE = max(1, int(os.environ.get("ORCH_RESULT_TERMINAL_CACHE_SIZE", "100000")))
RESULT_TERMINAL_STATUSES = {
    "owned_processing",
    "completed",
    "failed",
    "late_superseded",
    "late_expired",
    "rejected",
}


def _env_bool(name: str, default: bool) -> bool:
    raw = os.environ.get(name)
    if raw is None:
        return default
    return str(raw).strip().lower() in {"1", "true", "yes", "on"}


# Prefer workers with zero in-flight tasks (protects first-wave Mbps).
# When true and ORCH_ALLOW_BUSY_WORKER_REUSE=false, second-batch overflow waits
# for an idle worker instead of stacking onto a busy first-wave worker.
PREFER_IDLE_WORKERS = _env_bool("ORCH_PREFER_IDLE_WORKERS", True)
ALLOW_BUSY_WORKER_REUSE = _env_bool("ORCH_ALLOW_BUSY_WORKER_REUSE", False)
# Hold undelivered offers locally and push when a worker becomes idle
# (instead of failing / relying on BeamCore reassignment).
OVERFLOW_QUEUE_ENABLED = _env_bool("ORCH_OVERFLOW_QUEUE_ENABLED", True)
try:
    OVERFLOW_QUEUE_MAX = max(0, int(os.environ.get("ORCH_OVERFLOW_QUEUE_MAX", "2000")))
except ValueError:
    OVERFLOW_QUEUE_MAX = 2000


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
        self._result_forward_tasks: Dict[str, asyncio.Task] = {}
        self._terminal_result_acks: Dict[str, dict] = {}
        self._terminal_result_order = deque()
        # offer_id → transfer_context-like dict for completion logs
        self._offer_contexts: Dict[str, dict] = {}
        # offer_id → full task_offer payload for capacity-reject reassignment
        self._pending_offers: Dict[str, dict] = {}
        self._offer_attempted_workers: Dict[str, Set[str]] = {}
        # Offers waiting for an idle worker (prefer-idle overflow).
        self._overflow_offers: deque = deque()
        self._overflow_offer_ids: Set[str] = set()
        self._overflow_drain_pending = False
        self._overflow_drain_running = False

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
        # Fresh session after orch restart / reconnect: drop stale busy marks so
        # scheduling capacity matches the worker's cleared local queue.
        profile.active_offer_ids.clear()
        if ip.strip():
            profile.ip = ip.strip()
        logger.info(
            "Worker connected: %s version=%s ip=%s (%d/%d) queue_cleared=true",
            worker_id,
            profile.worker_version or "?",
            profile.ip or "?",
            len(self._sessions),
            MAX_WORKERS,
        )
        if was_empty and self._on_ready_change:
            self._on_ready_change(True)
        self._schedule_overflow_drain()
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
        self._schedule_overflow_drain()

    def _offer_key(self, offer: dict) -> str:
        return str(offer.get("offer_id") or offer.get("task_id") or "").strip()

    def _enqueue_overflow(self, offer: dict) -> bool:
        """Queue offer for later delivery when an idle worker appears."""
        if not OVERFLOW_QUEUE_ENABLED:
            return False
        if not isinstance(offer, dict):
            return False
        offer_id = self._offer_key(offer)
        if not offer_id:
            return False
        if offer_id in self._overflow_offer_ids:
            return True
        if OVERFLOW_QUEUE_MAX > 0 and len(self._overflow_offers) >= OVERFLOW_QUEUE_MAX:
            logger.warning(
                "overflow queue full (%d); cannot hold task=%s offer=%s",
                OVERFLOW_QUEUE_MAX,
                short_id(offer.get("task_id")),
                short_id(offer_id),
            )
            return False
        payload = dict(offer)
        self._overflow_offers.append(payload)
        self._overflow_offer_ids.add(offer_id)
        # Keep payload for capacity-reject / drain redelivery.
        self._pending_offers[offer_id] = payload
        self._offer_attempted_workers.setdefault(offer_id, set())
        self._remember_offer_context(payload)
        logger.info(
            "_workers | overflow_enqueue task=%s offer=%s pending=%d",
            short_id(offer.get("task_id")),
            short_id(offer_id),
            len(self._overflow_offers),
        )
        return True

    def _schedule_overflow_drain(self) -> None:
        if not OVERFLOW_QUEUE_ENABLED or not self._overflow_offers:
            return
        self._overflow_drain_pending = True
        if self._overflow_drain_running:
            return
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            return
        self._overflow_drain_running = True
        loop.create_task(self._overflow_drain_loop(), name="overflow-drain")

    async def _overflow_drain_loop(self) -> None:
        try:
            while self._overflow_drain_pending:
                self._overflow_drain_pending = False
                await self._drain_overflow_queue()
        finally:
            self._overflow_drain_running = False
            if self._overflow_drain_pending and self._overflow_offers:
                self._schedule_overflow_drain()

    async def _drain_overflow_queue(self) -> int:
        """Deliver queued offers to idle workers. Returns number delivered."""
        delivered = 0
        while self._overflow_offers:
            offer = self._overflow_offers[0]
            offer_id = self._offer_key(offer)
            attempted = set(self._offer_attempted_workers.get(offer_id) or set())
            worker_id = self.select_worker_round_robin(exclude_worker_ids=attempted)
            if not worker_id:
                break
            self._overflow_offers.popleft()
            if offer_id:
                self._overflow_offer_ids.discard(offer_id)
            ok = await self.deliver_task_offer(worker_id, offer)
            if ok:
                delivered += 1
                logger.info(
                    "_workers | overflow_deliver task=%s offer=%s worker=%s pending=%d",
                    short_id(offer.get("task_id")),
                    short_id(offer_id),
                    short_id(worker_id),
                    len(self._overflow_offers),
                )
            else:
                # Put back and stop; worker may have disconnected mid-send.
                if offer_id and offer_id not in self._overflow_offer_ids:
                    self._overflow_offers.appendleft(offer)
                    self._overflow_offer_ids.add(offer_id)
                break
        return delivered

    def disconnect(self, worker_id: str) -> None:
        self._sessions.pop(worker_id, None)
        profile = self._profiles.get(worker_id)
        if profile:
            profile.active_offer_ids.clear()
        logger.info("Worker disconnected: %s (%d/%d)", worker_id, len(self._sessions), MAX_WORKERS)
        if len(self._sessions) == 0 and self._on_ready_change:
            self._on_ready_change(False)

    async def stop(self) -> None:
        if not self._result_forward_tasks:
            return
        tasks = list(self._result_forward_tasks.values())
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        self._result_forward_tasks.clear()

    def _remember_offer_context(self, offer: dict) -> None:
        offer_id = str(offer.get("offer_id") or offer.get("task_id") or "").strip()
        if not offer_id:
            return
        ctx: dict = {}
        source_url = offer.get("source_url")
        dest_url = offer.get("dest_url")
        if isinstance(source_url, str) and source_url.strip():
            ctx["source_url"] = source_url
        if isinstance(dest_url, str) and dest_url.strip():
            ctx["dest_url"] = dest_url
        range_start = offer.get("range_start")
        range_end = offer.get("range_end")
        if range_start is None or range_end is None:
            headers = offer.get("source_headers") or {}
            if isinstance(headers, dict):
                range_hdr = str(headers.get("Range") or headers.get("range") or "")
                # bytes=START-END
                if range_hdr.lower().startswith("bytes="):
                    try:
                        start_s, end_s = range_hdr.split("=", 1)[1].split("-", 1)
                        range_start = int(start_s)
                        range_end = int(end_s)
                    except (TypeError, ValueError):
                        pass
        try:
            if range_start is not None and range_end is not None:
                ctx["range_start"] = int(range_start)
                ctx["range_end"] = int(range_end)
        except (TypeError, ValueError):
            pass
        if ctx:
            self._offer_contexts[offer_id] = ctx
            # Bound memory for long-running orch
            while len(self._offer_contexts) > 5000:
                self._offer_contexts.pop(next(iter(self._offer_contexts)), None)

    def _log_external_task_result(self, worker_id: str, msg: dict) -> None:
        """Log hash/etag for external worker completions (after upload)."""
        task_id = msg.get("task_id")
        offer_id = str(msg.get("offer_id") or task_id or "")
        ctx = self._offer_contexts.pop(offer_id, {}) if offer_id else {}
        chunk_hash = str(msg.get("chunk_hash") or "") or "-"
        etag = str(msg.get("etag") or "") or "-"
        success = bool(msg.get("success"))
        cached = msg.get("cached")
        src, dest = transfer_context_urls(ctx) if ctx else ("-", "-")
        range_label = transfer_context_range_label(ctx) if ctx else "-"
        chunk_id = chunk_id_from_transfer_context(ctx) if ctx else None
        if success:
            try:
                load_ms = float(msg.get("load_ms") or 0.0)
            except (TypeError, ValueError):
                load_ms = 0.0
            try:
                hash_ms = float(msg.get("hash_ms") or 0.0)
            except (TypeError, ValueError):
                hash_ms = 0.0
            try:
                etag_ms = float(msg.get("etag_ms") or 0.0)
            except (TypeError, ValueError):
                etag_ms = 0.0
            try:
                fetch_ms = float(msg.get("fetch_ms") or 0.0)
            except (TypeError, ValueError):
                fetch_ms = 0.0
            try:
                send_ms = float(msg.get("send_ms") or 0.0)
            except (TypeError, ValueError):
                send_ms = 0.0
            total_ms = load_ms + hash_ms + etag_ms + fetch_ms + send_ms
            cached_label = (
                str(bool(cached)).lower() if cached is not None else "?"
            )
            path_label = str(msg.get("path") or "external")
            hash_source = str(msg.get("hash_source") or "-")
            logger.info(
                "_workers | task_done task=%s offer=%s chunk_id=%s worker=%s "
                "src=%s dest=%s range=%s hash=%s etag_real=%s "
                "cached=%s path=%s hash_source=%s "
                "load_ms=%.1f hash_ms=%.1f etag_ms=%.1f fetch_ms=%.1f "
                "send_ms=%.1f wall_ms=%.1f",
                short_id(task_id),
                short_id(offer_id),
                chunk_id if chunk_id is not None else "?",
                short_id(worker_id),
                src,
                dest,
                range_label,
                chunk_hash,
                etag,
                cached_label,
                path_label,
                hash_source,
                load_ms,
                hash_ms,
                etag_ms,
                fetch_ms,
                send_ms,
                total_ms,
            )
        else:
            logger.warning(
                "_workers | failed task=%s offer=%s worker=%s reason=%s "
                "src=%s dest=%s range=%s hash=%s etag=%s",
                short_id(task_id),
                short_id(offer_id),
                short_id(worker_id),
                msg.get("error") or "external_task_failed",
                src,
                dest,
                range_label,
                chunk_hash,
                etag,
            )

    def _remember_pending_offer(self, worker_id: str, offer: dict) -> None:
        """Keep offer payload so queue_full can re-deliver to another worker."""
        offer_id = str(offer.get("offer_id") or offer.get("task_id") or "").strip()
        if not offer_id:
            return
        # Store a shallow copy so later mutations cannot corrupt redelivery.
        self._pending_offers[offer_id] = dict(offer)
        attempted = self._offer_attempted_workers.setdefault(offer_id, set())
        attempted.add(str(worker_id))
        while len(self._pending_offers) > 5000:
            expired = next(iter(self._pending_offers))
            self._pending_offers.pop(expired, None)
            self._offer_attempted_workers.pop(expired, None)

    def _clear_pending_offer(self, offer_id: Optional[str]) -> None:
        if not offer_id:
            return
        key = str(offer_id)
        self._pending_offers.pop(key, None)
        self._offer_attempted_workers.pop(key, None)

    @staticmethod
    def _is_capacity_reject_error(error: object) -> bool:
        text = str(error or "").strip().lower()
        return text.startswith("queue_full") or text.startswith("memory_budget")

    async def _maybe_reassign_on_capacity_reject(
        self, worker_id: str, msg: dict
    ) -> bool:
        """If worker rejected for queue/memory, re-deliver offer to another worker.

        Returns True when the offer was handed off (do not relay failure upstream).
        """
        if bool(msg.get("success")):
            return False
        if not self._is_capacity_reject_error(msg.get("error")):
            return False

        task_id = msg.get("task_id")
        offer_id = str(msg.get("offer_id") or task_id or "").strip()
        if not offer_id:
            return False

        offer = self._pending_offers.get(offer_id)
        if not offer:
            logger.warning(
                "capacity reject without pending offer: task=%s offer=%s worker=%s error=%s",
                short_id(task_id),
                short_id(offer_id),
                short_id(worker_id),
                msg.get("error"),
            )
            return False

        attempted = self._offer_attempted_workers.setdefault(offer_id, set())
        attempted.add(str(worker_id))
        # Free this worker's busy mark before selecting a replacement.
        self.mark_worker_idle(worker_id, offer_id)

        next_worker = self.select_worker_round_robin(exclude_worker_ids=attempted)
        if not next_worker:
            if self._enqueue_overflow(offer):
                logger.info(
                    "capacity reject: overflow queued task=%s offer=%s from=%s "
                    "attempted=%d pending=%d",
                    short_id(task_id),
                    short_id(offer_id),
                    short_id(worker_id),
                    len(attempted),
                    len(self._overflow_offers),
                )
                await self._send_to_worker(
                    worker_id,
                    {
                        "type": "task_result_ack",
                        "task_id": task_id,
                        "offer_id": offer_id,
                        "received": True,
                        "status": "late_superseded",
                        "reason": f"queued_after_capacity:{msg.get('error')}",
                    },
                )
                self._schedule_overflow_drain()
                return True
            logger.warning(
                "capacity reject: no alternate worker task=%s offer=%s from=%s "
                "attempted=%d error=%s",
                short_id(task_id),
                short_id(offer_id),
                short_id(worker_id),
                len(attempted),
                msg.get("error"),
            )
            return False

        delivered = await self.deliver_task_offer(next_worker, offer)
        if not delivered:
            logger.warning(
                "capacity reject re-deliver failed: task=%s offer=%s from=%s to=%s",
                short_id(task_id),
                short_id(offer_id),
                short_id(worker_id),
                short_id(next_worker),
            )
            return False

        # Settle the rejecting worker so it stops waiting for task_result_ack.
        await self._send_to_worker(
            worker_id,
            {
                "type": "task_result_ack",
                "task_id": task_id,
                "offer_id": offer_id,
                "received": True,
                "status": "late_superseded",
                "reason": f"reassigned_after_capacity:{msg.get('error')}",
            },
        )
        logger.info(
            "_workers | reassigned task=%s offer=%s from=%s to=%s reason=%s attempted=%d "
            "src=%s dest=%s",
            task_id,
            offer_id,
            worker_id,
            next_worker,
            msg.get("error"),
            len(attempted),
            offer.get("source_url") or "-",
            offer.get("dest_url") or "-",
        )
        return True

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
            self._remember_offer_context(offer)
            self._remember_pending_offer(worker_id, offer)
            if mark_busy:
                offer_id = offer.get("offer_id") or offer.get("task_id")
                if offer_id:
                    self.mark_worker_busy(worker_id, str(offer_id))
            offer_id = str(offer.get("offer_id") or offer.get("task_id") or "")
            task_id = str(offer.get("task_id") or "")
            ctx = self._offer_contexts.get(offer_id) or {}
            chunk_id = chunk_id_from_transfer_context(ctx) if ctx else None
            range_label = (
                transfer_context_range_label(ctx)
                if ctx
                else (
                    (offer.get("source_headers") or {}).get("Range")
                    or (offer.get("source_headers") or {}).get("range")
                    or "-"
                )
            )
            logger.info(
                "_workers | task_start task=%s offer=%s worker=%s "
                "chunk_id=%s range=%s",
                short_id(task_id),
                short_id(offer_id),
                short_id(worker_id),
                chunk_id if chunk_id is not None else "?",
                range_label,
            )
            return True
        except Exception as exc:
            logger.warning("deliver_task_offer send failed for %s: %s", worker_id, exc)
            self._sessions.pop(worker_id, None)
            return False

    def select_worker_round_robin(
        self,
        batch_used_ips: Optional[set[str]] = None,
        batch_assigned_workers: Optional[set[str]] = None,
        *,
        allow_used_ip: bool = True,
        exclude_worker_ids: Optional[set[str]] = None,
        batch_assigned_counts: Optional[dict[str, int]] = None,
    ) -> Optional[str]:
        """Pick the next worker with capacity, matching global-gateway batch IP spread.

        Goal: protect first-wave single-stream Mbps (makespan of the first
        assignment wave), then finish overflow when workers free.

        When ``ORCH_PREFER_IDLE_WORKERS`` is on (default):
          1. Prefer idle workers (effective load 0) on a fresh IP
          2. Then idle workers on any IP
          3. Only if ``ORCH_ALLOW_BUSY_WORKER_REUSE`` is on, reuse busy workers
             that still have ``active < max_concurrent_tasks``

        Effective load is ``max(active_count, batch_assigned)`` so in-flight
        work from prior NATS batches and this batch both count.

        When ``allow_used_ip`` is False, stop after step 1 (hybrid overflow).
        ``exclude_worker_ids`` skips workers already represented by the embedded pool.
        """
        connected = self._ordered_connected_worker_ids()
        if not connected:
            return None

        pool_size = len(connected)
        start = self._cursor % pool_size
        in_batch = batch_used_ips is not None or batch_assigned_workers is not None
        excluded = exclude_worker_ids or set()
        counts = batch_assigned_counts
        prefer_idle = PREFER_IDLE_WORKERS
        allow_busy_reuse = ALLOW_BUSY_WORKER_REUSE

        def _batch_count(worker_id: str) -> int:
            if counts is not None:
                return int(counts.get(worker_id, 0))
            if batch_assigned_workers and worker_id in batch_assigned_workers:
                return 1
            return 0

        def _load(worker_id: str) -> int:
            profile = self._get_profile(worker_id)
            # max(): deliver marks busy before batch_counts update on some paths
            return max(profile.active_count, _batch_count(worker_id))

        def _eligible(
            worker_id: str,
            *,
            allow_ip_reuse: bool,
            require_idle: bool,
            allow_worker_reuse: bool,
        ) -> bool:
            if worker_id in excluded:
                return False
            profile = self._get_profile(worker_id)
            if not profile.has_capacity:
                return False
            load = _load(worker_id)
            if require_idle and load > 0:
                return False
            if not allow_worker_reuse and _batch_count(worker_id) > 0:
                return False
            ip = profile.ip.strip()
            if (
                not allow_ip_reuse
                and batch_used_ips is not None
                and ip
                and ip in batch_used_ips
            ):
                return False
            return True

        def _pick(
            *,
            allow_ip_reuse: bool,
            require_idle: bool,
            allow_worker_reuse: bool,
        ) -> Optional[str]:
            # (load, batch_n, -mbps, -initial_order, offset, worker_id)
            candidates: list[tuple[int, int, float, int, int, str]] = []
            for offset in range(pool_size):
                idx = (start + offset) % pool_size
                worker_id = connected[idx]
                if not _eligible(
                    worker_id,
                    allow_ip_reuse=allow_ip_reuse,
                    require_idle=require_idle,
                    allow_worker_reuse=allow_worker_reuse,
                ):
                    continue
                profile = self._get_profile(worker_id)
                candidates.append(
                    (
                        _load(worker_id),
                        _batch_count(worker_id),
                        -profile.average_mbps,
                        -profile.initial_order,
                        offset,
                        worker_id,
                    )
                )
            if not candidates:
                return None
            candidates.sort(
                key=lambda item: (item[0], item[1], item[2], item[3], item[4])
            )
            _load_n, _bc, _neg_mbps, _neg_order, offset, worker_id = candidates[0]
            idx = (start + offset) % pool_size
            self._cursor = (idx + 1) % pool_size
            profile = self._get_profile(worker_id)
            logger.debug(
                "selected worker %s round_robin ip=%s active=%d/%d "
                "load=%d batch_n=%d mbps=%.1f cursor=%d pool=%d batch_ips=%s "
                "require_idle=%s reuse_worker=%s",
                worker_id,
                profile.ip or "?",
                profile.active_count,
                profile.max_concurrent_tasks,
                _load_n,
                _bc,
                profile.average_mbps,
                self._cursor,
                pool_size,
                ",".join(sorted(batch_used_ips)) if batch_used_ips else "-",
                require_idle,
                allow_worker_reuse,
            )
            return worker_id

        def _pick_idle(
            *,
            allow_ip_reuse: bool,
        ) -> Optional[str]:
            return _pick(
                allow_ip_reuse=allow_ip_reuse,
                require_idle=True,
                allow_worker_reuse=False,
            )

        def _pick_busy(
            *,
            allow_ip_reuse: bool,
        ) -> Optional[str]:
            return _pick(
                allow_ip_reuse=allow_ip_reuse,
                require_idle=False,
                allow_worker_reuse=True,
            )

        if prefer_idle:
            # Always exhaust idle workers (any IP) before stacking on busy ones.
            worker_id = _pick_idle(allow_ip_reuse=False)
            if worker_id:
                return worker_id
            if allow_used_ip:
                worker_id = _pick_idle(allow_ip_reuse=True)
                if worker_id:
                    return worker_id
            if not allow_busy_reuse:
                return None
            worker_id = _pick_busy(allow_ip_reuse=False)
            if worker_id:
                return worker_id
            if allow_used_ip:
                return _pick_busy(allow_ip_reuse=True)
            return None

        # Legacy: unused-in-batch first, then reuse capacity.
        if in_batch:
            worker_id = _pick(
                allow_ip_reuse=False,
                require_idle=False,
                allow_worker_reuse=False,
            )
            if worker_id:
                return worker_id
            if not allow_used_ip:
                return None
            worker_id = _pick(
                allow_ip_reuse=True,
                require_idle=False,
                allow_worker_reuse=False,
            )
            if worker_id:
                return worker_id
            return _pick(
                allow_ip_reuse=True,
                require_idle=False,
                allow_worker_reuse=True,
            )

        return _pick(
            allow_ip_reuse=True,
            require_idle=False,
            allow_worker_reuse=True,
        )

    def get_workers_round_robin(self, n: int = 1) -> list[str]:
        """Return up to n worker_ids with batch-aware round-robin (IP + capacity)."""
        selected: list[str] = []
        if n <= 0:
            return selected

        batch_used_ips: set[str] = set()
        batch_assigned_workers: set[str] = set()
        batch_assigned_counts: dict[str, int] = {}
        for _ in range(n):
            worker_id = self.select_worker_round_robin(
                batch_used_ips=batch_used_ips,
                batch_assigned_workers=batch_assigned_workers,
                batch_assigned_counts=batch_assigned_counts,
            )
            if not worker_id:
                break
            selected.append(worker_id)
            batch_assigned_workers.add(worker_id)
            batch_assigned_counts[worker_id] = batch_assigned_counts.get(worker_id, 0) + 1
            ip = self._get_profile(worker_id).ip.strip()
            if ip:
                batch_used_ips.add(ip)
        return selected

    async def deliver_task_offer_batch(self, offers: list[dict]) -> tuple[int, int]:
        """Deliver a task offer batch with global-gateway-style worker selection.

        When no idle worker is available, offers are held in the local overflow
        queue and delivered as workers free (not rejected upstream).
        """
        delivered = 0
        failed = 0
        queued = 0
        batch_used_ips: set[str] = set()
        batch_assigned_workers: set[str] = set()
        batch_assigned_counts: dict[str, int] = {}

        for offer in offers:
            if not isinstance(offer, dict):
                failed += 1
                continue

            worker_id = self.select_worker_round_robin(
                batch_used_ips=batch_used_ips,
                batch_assigned_workers=batch_assigned_workers,
                batch_assigned_counts=batch_assigned_counts,
            )
            if not worker_id:
                if self._enqueue_overflow(offer):
                    queued += 1
                else:
                    logger.warning(
                        "No local worker with capacity for batch offer task=%s "
                        "(prefer_idle=%s allow_busy_reuse=%s overflow=%s)",
                        offer.get("task_id"),
                        PREFER_IDLE_WORKERS,
                        ALLOW_BUSY_WORKER_REUSE,
                        OVERFLOW_QUEUE_ENABLED,
                    )
                    failed += 1
                continue

            if await self.deliver_task_offer(worker_id, offer):
                delivered += 1
                batch_assigned_workers.add(worker_id)
                batch_assigned_counts[worker_id] = (
                    batch_assigned_counts.get(worker_id, 0) + 1
                )
                ip = self._get_profile(worker_id).ip.strip()
                if ip:
                    batch_used_ips.add(ip)
            else:
                # Send failed — keep offer for another worker when one frees.
                if self._enqueue_overflow(offer):
                    queued += 1
                else:
                    failed += 1
                    logger.warning(
                        "Failed to forward task offer to local worker: worker=%s task=%s",
                        worker_id,
                        offer.get("task_id"),
                    )

        if queued:
            logger.info(
                "_workers | batch overflow queued=%s delivered=%s failed=%s pending=%s",
                queued,
                delivered,
                failed,
                len(self._overflow_offers),
            )
            self._schedule_overflow_drain()

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
        elif msg_type == "task_result":
            log_relay(
                f"worker ws <- recv type=task_result worker={short_id(worker_id)} "
                f"task={short_id(msg.get('task_id'))} offer={short_id(msg.get('offer_id') or msg.get('task_id'))} "
                f"success={msg.get('success')} bytes={msg.get('bytes_transferred')}"
            )
            offer_id = msg.get("offer_id") or msg.get("task_id")
            if await self._maybe_reassign_on_capacity_reject(worker_id, msg):
                # Busy mark already cleared inside reassignment; do not fail upstream.
                return
            self._log_external_task_result(worker_id, msg)
            await self._relay_task_result(worker_id, msg)
            self._clear_pending_offer(str(offer_id) if offer_id else None)
            transfer_mbps = msg.get("transfer_mbps")
            if transfer_mbps is not None:
                self._get_profile(worker_id).observe_transfer(transfer_mbps)
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

    def _cache_terminal_result_ack(self, result_key: str, ack: dict) -> None:
        if result_key not in self._terminal_result_acks:
            self._terminal_result_order.append(result_key)
        self._terminal_result_acks[result_key] = ack
        while len(self._terminal_result_order) > RESULT_TERMINAL_CACHE_SIZE:
            expired = self._terminal_result_order.popleft()
            self._terminal_result_acks.pop(expired, None)

    def _schedule_result_forward(self, worker_id: str, payload: dict) -> None:
        offer_id = str(payload.get("offer_id") or payload.get("task_id"))
        result_key = f"{offer_id}:{worker_id}"
        if result_key in self._result_forward_tasks:
            return

        task = asyncio.create_task(self._forward_task_result_to_beamcore(worker_id, payload))
        self._result_forward_tasks[result_key] = task

        def _done(done_task: asyncio.Task) -> None:
            if self._result_forward_tasks.get(result_key) is done_task:
                self._result_forward_tasks.pop(result_key, None)
            try:
                exc = done_task.exception()
            except asyncio.CancelledError:
                return
            if exc is not None:
                logger.error("task_result forward crashed: %s", exc)

        task.add_done_callback(_done)

    async def _forward_task_result_to_beamcore(self, worker_id: str, payload: dict) -> None:
        task_id = payload.get("task_id")
        offer_id = str(payload.get("offer_id") or task_id)
        result_key = f"{offer_id}:{worker_id}"
        attempt = 0
        while True:
            attempt += 1
            try:
                if self._upstream is None:
                    raise RuntimeError("beamcore_unavailable")
                sender = getattr(
                    self._upstream,
                    "send_task_result_strict",
                    self._upstream.send_task_result,
                )
                ack = await sender(payload)
                if not isinstance(ack, dict):
                    raise RuntimeError("invalid_beamcore_ack")
                status = str(ack.get("status") or "")
                if status in RESULT_TERMINAL_STATUSES:
                    terminal_ack = {
                        **ack,
                        "type": "task_result_ack",
                        "task_id": task_id,
                        "offer_id": offer_id,
                    }
                    self._cache_terminal_result_ack(result_key, terminal_ack)
                    await self._send_to_worker(worker_id, terminal_ack)
                    logger.debug(
                        "task_result relay terminal: task=%s offer=%s worker=%s status=%s",
                        short_id(task_id),
                        short_id(offer_id),
                        short_id(worker_id),
                        status,
                    )
                    return
                retry_reason = ack.get("reason") or status or "invalid_ack_status"
            except Exception as exc:
                retry_reason = type(exc).__name__

            delay = min(
                RESULT_FORWARD_RETRY_MAX_SECONDS,
                RESULT_FORWARD_RETRY_BASE_SECONDS * (2 ** min(attempt - 1, 16)),
            )
            if attempt == 1 or attempt & (attempt - 1) == 0:
                logger.info(
                    "task_result relay retry: task=%s offer=%s worker=%s attempt=%s delay_s=%.3f reason=%s",
                    short_id(task_id),
                    short_id(offer_id),
                    short_id(worker_id),
                    attempt,
                    delay,
                    retry_reason,
                )
            await asyncio.sleep(delay)

    async def _relay_task_result(self, worker_id: str, msg: dict) -> None:
        task_id = msg.get("task_id")
        offer_id = msg.get("offer_id") or task_id
        if not task_id or not offer_id:
            logger.warning(
                "dropping task_result missing task_id/offer_id from worker=%s",
                short_id(worker_id),
            )
            await self._send_to_worker(
                worker_id,
                {
                    "type": "task_result_ack",
                    "task_id": task_id,
                    "offer_id": offer_id,
                    "received": False,
                    "status": "rejected",
                    "reason": "missing_task_or_offer_id",
                },
            )
            return

        if self._upstream is None:
            logger.warning(
                "worker relay blocked: no beamcore upstream worker=%s type=task_result task=%s offer=%s",
                short_id(worker_id),
                short_id(task_id),
                short_id(offer_id),
            )
            await self._send_to_worker(
                worker_id,
                {
                    "type": "task_result_ack",
                    "task_id": task_id,
                    "offer_id": offer_id,
                    "received": False,
                    "status": "retry",
                    "reason": "beamcore_unavailable",
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

        result_key = f"{offer_id}:{worker_id}"
        terminal_ack = self._terminal_result_acks.get(result_key)
        if terminal_ack is not None:
            await self._send_to_worker(worker_id, terminal_ack)
            return

        if result_key in self._result_forward_tasks:
            logger.debug(
                "task_result relay already active: task=%s offer=%s worker=%s",
                short_id(task_id),
                short_id(offer_id),
                short_id(worker_id),
            )
            return

        log_relay(
            f"worker ws <- recv type=task_result worker={short_id(worker_id)} "
            f"task={short_id(task_id)} offer={short_id(offer_id)} forwarding=1"
        )
        self._schedule_result_forward(worker_id, payload)

