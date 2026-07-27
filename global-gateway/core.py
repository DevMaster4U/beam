"""Shared worker pool and orchestrator routing for the global gateway."""

from __future__ import annotations

import json
import logging
import os
from collections import deque
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from typing import Any, Deque, Dict, List, Optional

logger = logging.getLogger(__name__)


def _env_bool(name: str, default: bool) -> bool:
    raw = os.environ.get(name)
    if raw is None:
        return default
    return str(raw).strip().lower() in {"1", "true", "yes", "on"}


PREFER_IDLE_WORKERS = _env_bool("ORCH_PREFER_IDLE_WORKERS", True)
ALLOW_BUSY_WORKER_REUSE = _env_bool("ORCH_ALLOW_BUSY_WORKER_REUSE", False)


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


@dataclass
class WorkerTaskRecord:
    task_id: str
    offer_id: str
    worker_id: str
    orchestrator_hotkey: str = ""
    assigned_at: str = ""
    completed_at: Optional[str] = None
    status: str = "assigned"  # assigned, accepted, completed, rejected
    success: Optional[bool] = None
    transfer_mbps: Optional[float] = None
    bytes_transferred: Optional[int] = None
    error: Optional[str] = None

    def to_dict(self) -> dict[str, Any]:
        row = asdict(self)
        if row["transfer_mbps"] is not None:
            row["transfer_mbps"] = round(float(row["transfer_mbps"]), 2)
        return row


@dataclass
class WorkerScoringWeights:
    """Match orchestrator task_scheduler._select_best_worker weights."""

    weight_trust: float = 0.30
    weight_latency: float = 0.25
    weight_load: float = 0.20
    weight_bandwidth: float = 0.15
    weight_success: float = 0.10


@dataclass
class WorkerProfile:
    worker_id: str
    ip: str = ""
    claimed_bandwidth_mbps: float = 0.0
    transfer_mbps_sum: float = 0.0
    transfer_count: int = 0
    trust_score: float = 0.5
    success_rate: float = 1.0
    total_tasks: int = 0
    successful_tasks: int = 0
    max_concurrent_tasks: int = 5
    worker_version: str = ""
    initial_order: int = 0
    hidden: bool = False
    active_offer_ids: set[str] = field(default_factory=set)

    @property
    def active_count(self) -> int:
        return len(self.active_offer_ids)

    @property
    def active(self) -> bool:
        return bool(self.active_offer_ids)

    @property
    def has_capacity(self) -> bool:
        return self.active_count < self.max_concurrent_tasks

    @property
    def average_mbps(self) -> float:
        """Mean observed transfer Mbps across completed tasks."""
        if self.transfer_count > 0:
            return self.transfer_mbps_sum / self.transfer_count
        if self.claimed_bandwidth_mbps > 0:
            return self.claimed_bandwidth_mbps
        return 0.0

    @property
    def load_factor(self) -> float:
        if self.max_concurrent_tasks <= 0:
            return 1.0
        return min(1.0, self.active_count / self.max_concurrent_tasks)

    def observe_transfer(self, transfer_mbps: Optional[float], success: bool) -> None:
        if transfer_mbps is not None:
            try:
                mbps = float(transfer_mbps)
            except (TypeError, ValueError):
                mbps = 0.0
            if mbps > 0:
                self.transfer_mbps_sum += mbps
                self.transfer_count += 1
        self.total_tasks += 1
        if success:
            self.successful_tasks += 1
        if self.total_tasks > 0:
            self.success_rate = self.successful_tasks / self.total_tasks

    def round_robin_sort_key(self) -> tuple:
        """Idle workers (n=0) rank by initial_order desc; busy workers by avg Mbps desc."""
        if self.active_count == 0:
            return (0, -self.initial_order, self.worker_id)
        return (1, -self.average_mbps, self.worker_id)

    def score(self, weights: WorkerScoringWeights) -> float:
        """Multi-factor score aligned with orchestrator _select_best_worker."""
        load_score = 1.0 - self.load_factor
        bandwidth = self.average_mbps
        bandwidth_score = min(1.0, bandwidth / 1000.0)
        success_score = self.success_rate
        geo_score = 0.5
        return (
            weights.weight_trust * self.trust_score
            + weights.weight_latency * geo_score
            + weights.weight_load * load_score
            + weights.weight_bandwidth * bandwidth_score
            + weights.weight_success * success_score
        )


@dataclass
class GlobalGatewayState:
    max_workers: int = 100
    worker_history_max: int = 100
    scoring_weights: WorkerScoringWeights = field(default_factory=WorkerScoringWeights)
    worker_sessions: Dict[str, Any] = field(default_factory=dict)
    worker_profiles: Dict[str, WorkerProfile] = field(default_factory=dict)
    orchestrator_sessions: Dict[str, Any] = field(default_factory=dict)
    worker_cursor: int = 0
    worker_selection: str = "round_robin"
    offer_routes: Dict[str, str] = field(default_factory=dict)
    task_routes: Dict[str, str] = field(default_factory=dict)
    active_task_records: Dict[str, WorkerTaskRecord] = field(default_factory=dict)
    worker_histories: Dict[str, Deque[WorkerTaskRecord]] = field(default_factory=dict)
    finalized_offer_ids: set[str] = field(default_factory=set)
    task_result_acks: Dict[str, dict] = field(default_factory=dict)

    def worker_count(self) -> int:
        return len(self.worker_sessions)

    def orchestrator_count(self) -> int:
        return len(self.orchestrator_sessions)

    def list_worker_ids(self) -> list[str]:
        return list(self.worker_sessions.keys())

    def get_profile(self, worker_id: str) -> WorkerProfile:
        profile = self.worker_profiles.get(worker_id)
        if profile is None:
            profile = WorkerProfile(worker_id=worker_id)
            self.worker_profiles[worker_id] = profile
        return profile

    def register_worker_session(
        self,
        worker_id: str,
        websocket: Any,
        *,
        ip: str = "",
        claimed_bandwidth_mbps: float = 0.0,
        trust_score: float = 0.5,
        success_rate: float = 1.0,
        max_concurrent_tasks: int = 5,
        worker_version: str = "",
        hidden: bool = False,
    ) -> None:
        self.worker_sessions[worker_id] = websocket
        profile = self.get_profile(worker_id)
        if ip:
            profile.ip = ip.strip()
        if claimed_bandwidth_mbps > 0 and profile.claimed_bandwidth_mbps <= 0:
            profile.claimed_bandwidth_mbps = float(claimed_bandwidth_mbps)
        profile.trust_score = float(trust_score)
        profile.success_rate = float(success_rate)
        if max_concurrent_tasks > 0:
            profile.max_concurrent_tasks = int(max_concurrent_tasks)
        if worker_version:
            profile.worker_version = worker_version.strip()
        profile.hidden = bool(hidden)

    def unregister_worker_session(self, worker_id: str) -> None:
        self.worker_sessions.pop(worker_id, None)
        profile = self.worker_profiles.get(worker_id)
        if profile:
            profile.active_offer_ids.clear()

    def update_worker_hello(
        self,
        worker_id: str,
        ip: Optional[str] = None,
        claimed_bandwidth_mbps: Optional[float] = None,
        max_concurrent_tasks: Optional[int] = None,
        worker_version: Optional[str] = None,
        initial_order: Optional[int] = None,
    ) -> None:
        profile = self.get_profile(worker_id)
        if ip and ip.strip():
            profile.ip = ip.strip()
        if claimed_bandwidth_mbps is not None and claimed_bandwidth_mbps > 0:
            if profile.claimed_bandwidth_mbps <= 0:
                profile.claimed_bandwidth_mbps = float(claimed_bandwidth_mbps)
        if max_concurrent_tasks is not None and max_concurrent_tasks > 0:
            profile.max_concurrent_tasks = int(max_concurrent_tasks)
        if worker_version and worker_version.strip():
            profile.worker_version = worker_version.strip()
        if initial_order is not None:
            profile.initial_order = int(initial_order)

    def ordered_worker_ids(self, worker_ids: list[str]) -> list[str]:
        """Order pool for round-robin: idle by initial_order, busy by observed Mbps."""
        return sorted(worker_ids, key=lambda wid: self.get_profile(wid).round_robin_sort_key())

    def busy_ips(self) -> set[str]:
        ips: set[str] = set()
        for profile in self.worker_profiles.values():
            if profile.active and profile.ip:
                ips.add(profile.ip)
        return ips

    def worker_pool_stats(self) -> dict[str, Any]:
        """Connected / idle / busy counts for logging."""
        connected_ids = self.list_worker_ids()
        connected = len(connected_ids)
        busy_ids = [
            wid for wid in connected_ids if self.get_profile(wid).active
        ]
        busy = len(busy_ids)
        with_capacity = sum(
            1 for wid in connected_ids if self.get_profile(wid).has_capacity
        )
        idle = sum(1 for wid in connected_ids if self.get_profile(wid).active_count == 0)
        return {
            "connected": connected,
            "idle": idle,
            "busy": busy,
            "with_capacity": with_capacity,
            "busy_worker_ids": busy_ids,
            "busy_ips": sorted(self.busy_ips()),
        }

    def worker_pool_summary(self) -> str:
        stats = self.worker_pool_stats()
        parts = [
            f"connected={stats['connected']}",
            f"idle={stats['idle']}",
            f"busy={stats['busy']}",
            f"with_capacity={stats['with_capacity']}",
        ]
        busy_ips = stats["busy_ips"]
        if busy_ips:
            parts.append(f"busy_ips={','.join(busy_ips)}")
        busy_ids = stats["busy_worker_ids"]
        if busy_ids:
            short = [f"{wid[:8]}..." for wid in busy_ids]
            parts.append(f"busy_workers={','.join(short)}")
        return " ".join(parts)

    def select_best_worker(self, *, hidden_only: bool = False) -> Optional[str]:
        """Pick the best worker that still has capacity (active < max_concurrent_tasks)."""
        connected = self.list_worker_ids()
        if hidden_only:
            connected = [
                wid for wid in connected if self.get_profile(wid).hidden
            ]
        if not connected:
            return None

        capacity_ids = [
            wid for wid in connected if self.get_profile(wid).has_capacity
        ]
        if not capacity_ids:
            return None

        busy = self.busy_ips()
        prefer_other_ip = [
            wid
            for wid in capacity_ids
            if not self.get_profile(wid).ip or self.get_profile(wid).ip not in busy
        ]
        pool = prefer_other_ip if prefer_other_ip else capacity_ids
        prefer_other_ip_used = bool(prefer_other_ip)

        weights = self.scoring_weights
        scored: list[tuple[str, float]] = []
        for worker_id in pool:
            profile = self.get_profile(worker_id)
            scored.append((worker_id, profile.score(weights)))

        if not scored:
            return None

        scored.sort(key=lambda item: item[1], reverse=True)
        best_id, best_score = scored[0]
        profile = self.get_profile(best_id)
        logger.debug(
            "selected worker %s score=%.4f avg_mbps=%.1f ip=%s active=%d/%d "
            "candidates=%d prefer_other_ip=%s",
            best_id,
            best_score,
            profile.average_mbps,
            profile.ip or "?",
            profile.active_count,
            profile.max_concurrent_tasks,
            len(pool),
            prefer_other_ip_used
        )
        return best_id

    def select_worker(
        self,
        batch_used_ips: Optional[set[str]] = None,
        batch_assigned_workers: Optional[set[str]] = None,
        *,
        hidden_only: bool = False,
        batch_assigned_counts: Optional[dict[str, int]] = None,
    ) -> Optional[str]:
        """Pick the next worker for a task offer."""
        if self.worker_selection == "best_score":
            return self.select_best_worker(hidden_only=hidden_only)
        return self.select_worker_round_robin(
            batch_used_ips=batch_used_ips,
            batch_assigned_workers=batch_assigned_workers,
            batch_assigned_counts=batch_assigned_counts,
            hidden_only=hidden_only,
        )

    def select_worker_round_robin(
        self,
        batch_used_ips: Optional[set[str]] = None,
        batch_assigned_workers: Optional[set[str]] = None,
        *,
        hidden_only: bool = False,
        batch_assigned_counts: Optional[dict[str, int]] = None,
    ) -> Optional[str]:
        """Pick the next worker with capacity in round-robin order.

        Prefer idle workers (effective load 0) across NATS batches so first-wave
        single-stream Mbps is not diluted by second-batch stacking. Busy reuse
        only when ``ORCH_ALLOW_BUSY_WORKER_REUSE`` is enabled.
        """
        connected = self.ordered_worker_ids(self.list_worker_ids())
        if hidden_only:
            connected = [
                wid for wid in connected if self.get_profile(wid).hidden
            ]
        if not connected:
            return None

        pool_size = len(connected)
        start = self.worker_cursor % pool_size
        in_batch = batch_used_ips is not None or batch_assigned_workers is not None
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
            profile = self.get_profile(worker_id)
            return max(profile.active_count, _batch_count(worker_id))

        def _eligible(
            worker_id: str,
            *,
            allow_used_ip: bool,
            require_idle: bool,
            allow_worker_reuse: bool,
        ) -> bool:
            profile = self.get_profile(worker_id)
            if hidden_only and not profile.hidden:
                return False
            if not profile.has_capacity:
                return False
            load = _load(worker_id)
            if require_idle and load > 0:
                return False
            if not allow_worker_reuse and _batch_count(worker_id) > 0:
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

        def _pick(
            *,
            allow_used_ip: bool,
            require_idle: bool,
            allow_worker_reuse: bool,
        ) -> Optional[str]:
            candidates: list[tuple[int, int, float, int, str]] = []
            for offset in range(pool_size):
                idx = (start + offset) % pool_size
                worker_id = connected[idx]
                if not _eligible(
                    worker_id,
                    allow_used_ip=allow_used_ip,
                    require_idle=require_idle,
                    allow_worker_reuse=allow_worker_reuse,
                ):
                    continue
                profile = self.get_profile(worker_id)
                candidates.append(
                    (
                        _load(worker_id),
                        _batch_count(worker_id),
                        -profile.average_mbps,
                        offset,
                        worker_id,
                    )
                )
            if not candidates:
                return None
            candidates.sort(key=lambda item: (item[0], item[1], item[2], item[3]))
            _load_n, _bc, _neg_mbps, offset, worker_id = candidates[0]
            idx = (start + offset) % pool_size
            self.worker_cursor = (idx + 1) % pool_size
            profile = self.get_profile(worker_id)
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
                self.worker_cursor,
                pool_size,
                ",".join(sorted(batch_used_ips)) if batch_used_ips else "-",
                require_idle,
                allow_worker_reuse,
            )
            return worker_id

        if prefer_idle:
            worker_id = _pick(
                allow_used_ip=False,
                require_idle=True,
                allow_worker_reuse=False,
            )
            if worker_id:
                return worker_id
            worker_id = _pick(
                allow_used_ip=True,
                require_idle=True,
                allow_worker_reuse=False,
            )
            if worker_id:
                return worker_id
            if not allow_busy_reuse:
                return None
            worker_id = _pick(
                allow_used_ip=False,
                require_idle=False,
                allow_worker_reuse=True,
            )
            if worker_id:
                return worker_id
            return _pick(
                allow_used_ip=True,
                require_idle=False,
                allow_worker_reuse=True,
            )

        if in_batch:
            worker_id = _pick(
                allow_used_ip=False,
                require_idle=False,
                allow_worker_reuse=False,
            )
            if worker_id:
                return worker_id
            worker_id = _pick(
                allow_used_ip=True,
                require_idle=False,
                allow_worker_reuse=False,
            )
            if worker_id:
                return worker_id
            return _pick(
                allow_used_ip=True,
                require_idle=False,
                allow_worker_reuse=True,
            )

        return _pick(
            allow_used_ip=True,
            require_idle=False,
            allow_worker_reuse=True,
        )

    def get_workers_round_robin(self, n: int = 1) -> list[str]:
        """Select up to n workers in round-robin order for one batch."""
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
            ip = self.get_profile(worker_id).ip.strip()
            if ip:
                batch_used_ips.add(ip)
        return selected

    def mark_worker_busy(self, worker_id: str, offer_id: Optional[str] = None) -> None:
        profile = self.get_profile(worker_id)
        if offer_id:
            profile.active_offer_ids.add(str(offer_id))

    def mark_worker_idle(self, worker_id: str, offer_id: Optional[str] = None) -> None:
        profile = self.get_profile(worker_id)
        if offer_id:
            profile.active_offer_ids.discard(str(offer_id))
        else:
            profile.active_offer_ids.clear()

    def observe_worker_transfer(
        self,
        worker_id: str,
        transfer_mbps: Optional[float],
        success: bool = False,
    ) -> None:
        self.get_profile(worker_id).observe_transfer(transfer_mbps, success)

    def is_duplicate_task_result(self, offer_id: str) -> bool:
        key = str(offer_id)
        return key in self.finalized_offer_ids

    def note_task_result_finalized(self, offer_id: str) -> None:
        key = str(offer_id)
        self.finalized_offer_ids.add(key)
        if len(self.finalized_offer_ids) > 10000:
            # Drop arbitrary half when oversized; retries are recent.
            drop = len(self.finalized_offer_ids) // 2
            for old in list(self.finalized_offer_ids)[:drop]:
                self.finalized_offer_ids.discard(old)
                self.task_result_acks.pop(old, None)

    def cache_task_result_ack(self, offer_id: str, payload: dict) -> None:
        key = str(offer_id)
        if key:
            self.task_result_acks[key] = dict(payload)

    def get_task_result_ack(self, offer_id: str) -> Optional[dict]:
        return self.task_result_acks.get(str(offer_id))

    def _history_deque(self, worker_id: str) -> Deque[WorkerTaskRecord]:
        dq = self.worker_histories.get(worker_id)
        if dq is None:
            dq = deque(maxlen=max(1, self.worker_history_max))
            self.worker_histories[worker_id] = dq
        return dq

    def record_task_assigned(
        self,
        worker_id: str,
        *,
        task_id: str,
        offer_id: str,
        orchestrator_hotkey: str = "",
    ) -> None:
        self.get_profile(worker_id)
        offer_key = str(offer_id)
        record = WorkerTaskRecord(
            task_id=str(task_id),
            offer_id=offer_key,
            worker_id=worker_id,
            orchestrator_hotkey=orchestrator_hotkey,
            assigned_at=_utc_now_iso(),
            status="assigned",
        )
        self.active_task_records[offer_key] = record

    def record_task_accepted(self, worker_id: str, message: dict) -> None:
        offer_id = str(message.get("offer_id") or message.get("task_id") or "")
        if not offer_id:
            return
        record = self.active_task_records.get(offer_id)
        if record is None or record.worker_id != worker_id:
            return
        record.status = "accepted"

    def _finalize_task_record(
        self,
        worker_id: str,
        message: dict,
        *,
        status: str,
        success: Optional[bool] = None,
    ) -> None:
        offer_id = str(message.get("offer_id") or message.get("task_id") or "")
        if not offer_id:
            return

        record = self.active_task_records.pop(offer_id, None)
        if record is None:
            record = WorkerTaskRecord(
                task_id=str(message.get("task_id") or offer_id),
                offer_id=offer_id,
                worker_id=worker_id,
                assigned_at=_utc_now_iso(),
            )
        elif record.worker_id != worker_id:
            return

        record.status = status
        record.completed_at = _utc_now_iso()
        record.success = success
        if message.get("transfer_mbps") is not None:
            try:
                record.transfer_mbps = float(message["transfer_mbps"])
            except (TypeError, ValueError):
                record.transfer_mbps = None
        if message.get("bytes_transferred") is not None:
            try:
                record.bytes_transferred = int(message["bytes_transferred"])
            except (TypeError, ValueError):
                record.bytes_transferred = None
        if message.get("error"):
            record.error = str(message["error"])

        orch = self.resolve_orchestrator_hotkey(message)
        if orch and not record.orchestrator_hotkey:
            record.orchestrator_hotkey = orch

        self._history_deque(worker_id).appendleft(record)
        if status in ("completed", "rejected"):
            self.note_task_result_finalized(offer_id)

    def record_task_result(self, worker_id: str, message: dict) -> None:
        self._finalize_task_record(
            worker_id,
            message,
            status="completed",
            success=bool(message.get("success", False)),
        )

    def record_task_rejected(self, worker_id: str, message: dict) -> None:
        self._finalize_task_record(
            worker_id,
            message,
            status="rejected",
            success=False,
        )

    def active_tasks_for_worker(self, worker_id: str) -> List[dict]:
        rows: List[dict] = []
        for record in self.active_task_records.values():
            if record.worker_id == worker_id:
                rows.append(record.to_dict())
        rows.sort(key=lambda row: row.get("assigned_at") or "", reverse=True)
        return rows

    def worker_history(
        self,
        worker_id: Optional[str] = None,
        *,
        limit: int = 50,
    ) -> List[dict]:
        limit = max(1, min(limit, 500))
        if worker_id:
            records = list(self._history_deque(worker_id))
        else:
            records = []
            for dq in self.worker_histories.values():
                records.extend(dq)
            records.sort(
                key=lambda rec: rec.completed_at or rec.assigned_at,
                reverse=True,
            )
        return [rec.to_dict() for rec in records[:limit]]

    def all_worker_ids(self) -> list[str]:
        ids = (
            set(self.worker_sessions.keys())
            | set(self.worker_profiles.keys())
            | set(self.worker_histories.keys())
        )
        return sorted(ids)

    def worker_detail_payload(self, worker_id: str) -> Optional[dict]:
        profile = self.worker_profiles.get(worker_id)
        if profile is None and worker_id not in self.worker_sessions:
            return None
        if profile is None:
            profile = self.get_profile(worker_id)
        connected = worker_id in self.worker_sessions
        return {
            "worker_id": worker_id,
            "connected": connected,
            "worker_version": profile.worker_version,
            "ip": profile.ip,
            "average_mbps": round(profile.average_mbps, 1),
            "transfer_count": profile.transfer_count,
            "claimed_bandwidth_mbps": round(profile.claimed_bandwidth_mbps, 1),
            "trust_score": round(profile.trust_score, 4),
            "success_rate": round(profile.success_rate, 4),
            "total_tasks": profile.total_tasks,
            "successful_tasks": profile.successful_tasks,
            "score": round(profile.score(self.scoring_weights), 4),
            "active": profile.active,
            "active_tasks": profile.active_count,
            "max_concurrent_tasks": profile.max_concurrent_tasks,
            "initial_order": profile.initial_order,
            "hidden": profile.hidden,
            "active_offer_ids": sorted(profile.active_offer_ids),
            "active_task_records": self.active_tasks_for_worker(worker_id),
        }

    def worker_status_payload(self) -> list[dict]:
        rows: list[dict] = []
        for worker_id in self.all_worker_ids():
            detail = self.worker_detail_payload(worker_id)
            if detail:
                rows.append(detail)
        return rows

    def register_route(
        self,
        orchestrator_hotkey: str,
        worker_id: str,
        offer_id: Optional[str],
        task_id: Optional[str],
    ) -> None:
        if offer_id:
            self.offer_routes[str(offer_id)] = orchestrator_hotkey
        if task_id:
            self.task_routes[str(task_id)] = orchestrator_hotkey
        logger.debug(
            "route registered orch=%s worker=%s offer=%s task=%s",
            orchestrator_hotkey[:16],
            worker_id,
            offer_id,
            task_id,
        )

    def resolve_orchestrator_hotkey(self, message: dict) -> Optional[str]:
        offer_id = message.get("offer_id")
        if offer_id and str(offer_id) in self.offer_routes:
            return self.offer_routes[str(offer_id)]
        task_id = message.get("task_id")
        if task_id and str(task_id) in self.task_routes:
            return self.task_routes[str(task_id)]
        return None

    async def send_json(self, ws: Any, payload: dict) -> bool:
        try:
            await ws.send_text(json.dumps(payload))
            return True
        except Exception as exc:
            logger.warning("websocket send failed: %s", exc)
            return False

    async def notify_pool_status(self) -> None:
        payload = {
            "type": "pool_status",
            "worker_count": self.worker_count(),
            "workers": self.worker_status_payload(),
        }
        for hotkey, channel in list(self.orchestrator_sessions.items()):
            if not await channel.send(payload):
                self.orchestrator_sessions.pop(hotkey, None)

    async def forward_to_orchestrator(self, orchestrator_hotkey: str, message: dict) -> bool:
        channel = self.orchestrator_sessions.get(orchestrator_hotkey)
        if channel is None:
            logger.warning("no orchestrator session for hotkey %s", orchestrator_hotkey)
            return False
        payload = {
            "type": "from_worker",
            "worker_id": message.get("worker_id"),
            "message": message,
        }
        return await channel.send(payload)

    async def send_to_worker(self, worker_id: str, payload: dict) -> bool:
        if payload.get("type") == "task_result_ack":
            offer_id = payload.get("offer_id") or payload.get("task_id")
            if offer_id:
                self.cache_task_result_ack(str(offer_id), payload)
        ws = self.worker_sessions.get(worker_id)
        if ws is None:
            logger.warning("worker %s not connected", worker_id)
            return False
        return await self.send_json(ws, payload)


gateway_state = GlobalGatewayState()
