"""Shared worker pool and orchestrator routing for the global gateway."""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from typing import Any, Dict, Optional

logger = logging.getLogger(__name__)


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
    scoring_weights: WorkerScoringWeights = field(default_factory=WorkerScoringWeights)
    worker_sessions: Dict[str, Any] = field(default_factory=dict)
    worker_profiles: Dict[str, WorkerProfile] = field(default_factory=dict)
    orchestrator_sessions: Dict[str, Any] = field(default_factory=dict)
    worker_cursor: int = 0
    offer_routes: Dict[str, str] = field(default_factory=dict)
    task_routes: Dict[str, str] = field(default_factory=dict)

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
    ) -> None:
        profile = self.get_profile(worker_id)
        if ip and ip.strip():
            profile.ip = ip.strip()
        if claimed_bandwidth_mbps is not None and claimed_bandwidth_mbps > 0:
            if profile.claimed_bandwidth_mbps <= 0:
                profile.claimed_bandwidth_mbps = float(claimed_bandwidth_mbps)
        if max_concurrent_tasks is not None and max_concurrent_tasks > 0:
            profile.max_concurrent_tasks = int(max_concurrent_tasks)

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

    def select_best_worker(self) -> Optional[str]:
        """Pick the best worker that still has capacity (active < max_concurrent_tasks)."""
        connected = self.list_worker_ids()
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
            "candidates=%d prefer_other_ip=%s (%s)",
            best_id,
            best_score,
            profile.average_mbps,
            profile.ip or "?",
            profile.active_count,
            profile.max_concurrent_tasks,
            len(pool),
            prefer_other_ip_used,
            self.worker_pool_summary(),
        )
        return best_id

    def get_workers_round_robin(self, n: int = 1) -> list[str]:
        """Select up to n best idle workers (caller marks busy when delivering offers)."""
        selected: list[str] = []
        for _ in range(n):
            worker_id = self.select_best_worker()
            if not worker_id:
                break
            selected.append(worker_id)
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

    def worker_status_payload(self) -> list[dict]:
        rows: list[dict] = []
        for worker_id in self.list_worker_ids():
            profile = self.get_profile(worker_id)
            rows.append(
                {
                    "worker_id": worker_id,
                    "ip": profile.ip,
                    "average_mbps": round(profile.average_mbps, 1),
                    "transfer_count": profile.transfer_count,
                    "claimed_bandwidth_mbps": round(profile.claimed_bandwidth_mbps, 1),
                    "trust_score": round(profile.trust_score, 4),
                    "success_rate": round(profile.success_rate, 4),
                    "score": round(profile.score(self.scoring_weights), 4),
                    "active": profile.active,
                    "active_tasks": profile.active_count,
                    "max_concurrent_tasks": profile.max_concurrent_tasks,
                }
            )
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
        for hotkey, ws in list(self.orchestrator_sessions.items()):
            if not await self.send_json(ws, payload):
                self.orchestrator_sessions.pop(hotkey, None)

    async def forward_to_orchestrator(self, orchestrator_hotkey: str, message: dict) -> bool:
        ws = self.orchestrator_sessions.get(orchestrator_hotkey)
        if ws is None:
            logger.warning("no orchestrator session for hotkey %s", orchestrator_hotkey)
            return False
        payload = {
            "type": "from_worker",
            "worker_id": message.get("worker_id"),
            "message": message,
        }
        return await self.send_json(ws, payload)

    async def send_to_worker(self, worker_id: str, payload: dict) -> bool:
        ws = self.worker_sessions.get(worker_id)
        if ws is None:
            logger.warning("worker %s not connected", worker_id)
            return False
        return await self.send_json(ws, payload)


gateway_state = GlobalGatewayState()
