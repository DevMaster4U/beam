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
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable, Dict, Optional, Set
from urllib.parse import urlsplit

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
# When overflow pending exceeds this, allow stacking onto busy workers
# (up to ORCH_OVERFLOW_BUSY_MAX_PER_WORKER each) to clear backlog.
try:
    OVERFLOW_BUSY_THRESHOLD = max(0, int(os.environ.get("ORCH_OVERFLOW_BUSY_THRESHOLD", "100")))
except ValueError:
    OVERFLOW_BUSY_THRESHOLD = 100
try:
    OVERFLOW_BUSY_MAX_PER_WORKER = max(
        1, int(os.environ.get("ORCH_OVERFLOW_BUSY_MAX_PER_WORKER", "5"))
    )
except ValueError:
    OVERFLOW_BUSY_MAX_PER_WORKER = 5

# Prefer free workers that historically upload faster to this dest_group
# (R2 host / destinations/<group>/…). Improves makespan / prism last-upload.
DEST_AFFINITY_ENABLED = _env_bool("ORCH_DEST_AFFINITY", True)
try:
    DEST_AFFINITY_EMA = min(
        1.0, max(0.05, float(os.environ.get("ORCH_DEST_AFFINITY_EMA", "0.35")))
    )
except ValueError:
    DEST_AFFINITY_EMA = 0.35
try:
    DEST_AFFINITY_MIN_SAMPLES = max(
        1, int(os.environ.get("ORCH_DEST_AFFINITY_MIN_SAMPLES", "1"))
    )
except ValueError:
    DEST_AFFINITY_MIN_SAMPLES = 1
_DEST_STATS_PATH_RAW = os.environ.get(
    "ORCH_DEST_AFFINITY_STATS_PATH",
    "logs/orchestrators/dest_worker_stats.json",
).strip()


def _resolve_stats_path(raw: str) -> Optional[Path]:
    if not raw:
        return None
    path = Path(raw).expanduser()
    if not path.is_absolute():
        path = (Path.cwd() / path).resolve()
    else:
        path = path.resolve()
    return path


DEST_AFFINITY_STATS_PATH = _resolve_stats_path(_DEST_STATS_PATH_RAW)
_DEST_SEED_CSV_RAW = os.environ.get(
    "ORCH_DEST_AFFINITY_SEED_CSV",
    "logs/orchestrators/orch5_worker_dest_avg.csv",
).strip()
DEST_AFFINITY_SEED_CSV = _resolve_stats_path(_DEST_SEED_CSV_RAW) if _DEST_SEED_CSV_RAW else None
try:
    DEST_AFFINITY_SAVE_INTERVAL_S = max(
        1.0, float(os.environ.get("ORCH_DEST_AFFINITY_SAVE_INTERVAL_S", "10"))
    )
except ValueError:
    DEST_AFFINITY_SAVE_INTERVAL_S = 10.0
# Soft-ban workers that are slow on a dest bucket so select/makespan hop away.
# Absolute floor (0=off) and/or relative to best EMA in that bucket (0=off).
try:
    DEST_PENALTY_MBPS = max(
        0.0, float(os.environ.get("ORCH_DEST_PENALTY_MBPS", "150"))
    )
except ValueError:
    DEST_PENALTY_MBPS = 150.0
try:
    DEST_PENALTY_REL = min(
        1.0, max(0.0, float(os.environ.get("ORCH_DEST_PENALTY_REL", "0.65")))
    )
except ValueError:
    DEST_PENALTY_REL = 0.65
try:
    DEST_PENALTY_S = max(
        0.0, float(os.environ.get("ORCH_DEST_PENALTY_S", "900"))
    )
except ValueError:
    DEST_PENALTY_S = 900.0
# Fallback effective Mbps only when forcing a penalty with no sample
# (e.g. slow_net_wait abort). Normal penalties use the observed sample Mbps.
try:
    DEST_PENALTY_FALLBACK_MBPS = max(
        1.0, float(os.environ.get("ORCH_DEST_PENALTY_FALLBACK_MBPS", "8"))
    )
except ValueError:
    DEST_PENALTY_FALLBACK_MBPS = 8.0
# Compat alias
DEST_PENALTY_EFF_MBPS = DEST_PENALTY_FALLBACK_MBPS
# Batch-assign free workers↔offers to minimize expected last completion
# (makespan), not greedy max-Mbps per offer. Binary-search + matching.
DEST_AFFINITY_MAKESPAN = _env_bool("ORCH_DEST_AFFINITY_MAKESPAN", True)
_DEFAULT_OFFER_BYTES = 64.0 * 1024 * 1024


def dest_group_from_url(dest_url: object) -> str:
    """Affinity bucket key for a dest URL.

    Prefer R2 account host (worker speed differs per endpoint). Fall back to
    ``destinations/<group>`` path segment, then bare hostname.
    """
    text = str(dest_url or "").strip()
    if not text:
        return ""
    try:
        parsed = urlsplit(text)
        host = (parsed.hostname or "").lower()
        path_parts = [p for p in parsed.path.split("/") if p]
        path_group = ""
        if "destinations" in path_parts:
            i = path_parts.index("destinations")
            if i + 1 < len(path_parts):
                path_group = path_parts[i + 1]
        # …/<account>.r2.cloudflarestorage.com — account id is the real bucket.
        if host.endswith(".r2.cloudflarestorage.com"):
            account = host.split(".", 1)[0]
            if account:
                return account
        if host and path_group:
            return f"{host}/{path_group}"
        if path_group:
            return path_group
        return host
    except Exception:
        return ""


def dest_group_from_offer(offer: dict) -> str:
    if not isinstance(offer, dict):
        return ""
    return dest_group_from_url(offer.get("dest_url"))


def transfer_mbps_from_result(msg: dict) -> Optional[float]:
    """Prefer transfer_mbps; else derive from bytes_transferred / send_ms."""
    if not isinstance(msg, dict):
        return None
    raw = msg.get("transfer_mbps")
    if raw is not None:
        try:
            mbps = float(raw)
            if mbps > 0:
                return mbps
        except (TypeError, ValueError):
            pass
    try:
        nbytes = float(msg.get("bytes_transferred") or 0.0)
        send_ms = float(msg.get("send_ms") or 0.0)
    except (TypeError, ValueError):
        return None
    if nbytes <= 0 or send_ms <= 0:
        return None
    return (nbytes * 8.0) / (send_ms / 1000.0) / 1_000_000.0


def offer_byte_size(offer: dict) -> float:
    """Best-effort payload size from range fields / Range header."""
    if not isinstance(offer, dict):
        return _DEFAULT_OFFER_BYTES
    start = offer.get("range_start")
    end = offer.get("range_end")
    if start is None or end is None:
        headers = offer.get("source_headers") or {}
        if isinstance(headers, dict):
            range_hdr = str(headers.get("Range") or headers.get("range") or "")
            if range_hdr.lower().startswith("bytes="):
                try:
                    start_s, end_s = range_hdr.split("=", 1)[1].split("-", 1)
                    start = int(start_s)
                    end = int(end_s)
                except (TypeError, ValueError):
                    start, end = None, None
    try:
        if start is not None and end is not None:
            size = float(int(end) - int(start) + 1)
            if size > 0:
                return size
    except (TypeError, ValueError):
        pass
    return _DEFAULT_OFFER_BYTES


def expected_transfer_seconds(nbytes: float, mbps: float) -> float:
    """Wall seconds for nbytes at mbps (megabits/sec)."""
    rate = max(float(mbps), 1.0)
    return (max(float(nbytes), 1.0) * 8.0) / (rate * 1_000_000.0)


def min_makespan_assignment(
    costs: list[list[float]],
) -> tuple[list[int], float]:
    """Assign each task to a distinct worker minimizing max cost (makespan).

    ``costs[t][w]`` = expected seconds. Returns (task→worker index, makespan).
    Requires ``len(tasks) <= len(workers)``. Hungarian minimizes sum; this is the
    bottleneck/makespan analogue (binary search + bipartite matching).
    """
    n_tasks = len(costs)
    if n_tasks == 0:
        return [], 0.0
    n_workers = len(costs[0]) if costs else 0
    if n_workers < n_tasks:
        raise ValueError("min_makespan_assignment needs workers >= tasks")

    thresholds = sorted({float(c) for row in costs for c in row})
    if not thresholds:
        return [-1] * n_tasks, 0.0

    def _matching(limit: float) -> Optional[list[int]]:
        match_w = [-1] * n_workers

        def dfs(task: int, seen: list[bool]) -> bool:
            for w in range(n_workers):
                if costs[task][w] > limit or seen[w]:
                    continue
                seen[w] = True
                if match_w[w] < 0 or dfs(match_w[w], seen):
                    match_w[w] = task
                    return True
            return False

        for t in range(n_tasks):
            if not dfs(t, [False] * n_workers):
                return None
        assign = [-1] * n_tasks
        for w, t in enumerate(match_w):
            if t >= 0:
                assign[t] = w
        return assign

    best_assign: Optional[list[int]] = None
    best_t = thresholds[-1]
    lo, hi = 0, len(thresholds) - 1
    while lo <= hi:
        mid = (lo + hi) // 2
        limit = thresholds[mid]
        assign = _matching(limit)
        if assign is not None:
            best_assign = assign
            best_t = limit
            hi = mid - 1
        else:
            lo = mid + 1

    if best_assign is None:
        # Should not happen if finite costs; fall back to greedy by row min.
        best_assign = []
        used: set[int] = set()
        makespan = 0.0
        for t in range(n_tasks):
            order = sorted(range(n_workers), key=lambda w: costs[t][w])
            pick = next((w for w in order if w not in used), -1)
            best_assign.append(pick)
            if pick >= 0:
                used.add(pick)
                makespan = max(makespan, costs[t][pick])
        return best_assign, makespan

    makespan = 0.0
    for t, w in enumerate(best_assign):
        if w >= 0:
            makespan = max(makespan, costs[t][w])
    return best_assign, makespan


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
        # Offers waiting for an idle worker (single dispatcher queue).
        self._overflow_offers: deque = deque()
        self._overflow_offer_ids: Set[str] = set()
        self._overflow_drain_pending = False
        self._overflow_drain_running = False
        self._overflow_drain_lock = asyncio.Lock()
        self._overflow_prefer_worker: Optional[str] = None
        # Workers that recently returned queue_full — skip until success or cooldown.
        self._dispatch_blocked: Set[str] = set()
        self._dispatch_unblock_tasks: Dict[str, asyncio.Task] = {}
        # dest_group → worker_id → {ema, n, updated_at}
        self._dest_worker_stats: Dict[str, Dict[str, dict]] = {}
        self._dest_stats_dirty = False
        self._dest_stats_last_save = 0.0
        self._load_dest_worker_stats()

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

    def _load_dest_worker_stats(self) -> None:
        """Load dest_group→worker Mbps history from JSON (or CSV seed)."""
        path = DEST_AFFINITY_STATS_PATH
        loaded = 0
        source = ""
        if path is not None and path.is_file():
            try:
                raw = json.loads(path.read_text(encoding="utf-8"))
                loaded = self._ingest_dest_stats_dict(raw)
                source = str(path)
            except Exception as exc:
                logger.warning(
                    "dest affinity stats load failed path=%s: %s", path, exc
                )
        if loaded == 0 and DEST_AFFINITY_SEED_CSV is not None:
            loaded = self._load_dest_worker_stats_from_csv(DEST_AFFINITY_SEED_CSV)
            if loaded:
                source = str(DEST_AFFINITY_SEED_CSV)
                self._dest_stats_dirty = True
                self._maybe_save_dest_worker_stats(force=True)

        if loaded:
            logger.info(
                "dest affinity loaded %d worker×dest rows from %s",
                loaded,
                source or "?",
            )
        elif path is not None:
            logger.info(
                "dest affinity stats empty (will create on uploads) path=%s",
                path,
            )

    def _ingest_dest_stats_dict(self, raw: object) -> int:
        if not isinstance(raw, dict):
            return 0
        loaded = 0
        for dest_group, workers in raw.items():
            if not isinstance(dest_group, str) or not isinstance(workers, dict):
                continue
            group = dest_group.strip()
            if not group or group == "?":
                continue
            bucket = self._dest_worker_stats.setdefault(group, {})
            for worker_id, entry in workers.items():
                if not isinstance(worker_id, str) or not isinstance(entry, dict):
                    continue
                wid = worker_id.strip()
                if not wid:
                    continue
                try:
                    ema = float(entry.get("ema") or entry.get("avg_mbps") or 0.0)
                    n = int(entry.get("n") or 0)
                except (TypeError, ValueError):
                    continue
                if ema <= 0 or n <= 0:
                    continue
                bucket[wid] = {
                    "ema": ema,
                    "n": n,
                    "updated_at": float(entry.get("updated_at") or 0.0),
                    "penalty_until": float(entry.get("penalty_until") or 0.0),
                    "penalty_mbps": float(entry.get("penalty_mbps") or 0.0),
                }
                loaded += 1
        return loaded

    def _load_dest_worker_stats_from_csv(self, csv_path: Path) -> int:
        """Seed from analyze-orch-log --avg-by-worker-dest CSV (may use short ids)."""
        if csv_path is None or not csv_path.is_file():
            return 0
        try:
            text = csv_path.read_text(encoding="utf-8")
        except Exception as exc:
            logger.warning(
                "dest affinity CSV seed failed path=%s: %s", csv_path, exc
            )
            return 0
        lines = text.splitlines()
        if not lines:
            return 0
        loaded = 0
        for line in lines[1:]:
            parts = [p.strip() for p in line.split(",")]
            if len(parts) < 4:
                continue
            wid, group, n_s, avg_s = parts[0], parts[1], parts[2], parts[3]
            if not wid or not group or group == "?":
                continue
            try:
                n = int(float(n_s))
                ema = float(avg_s)
            except (TypeError, ValueError):
                continue
            if n <= 0 or ema <= 0:
                continue
            bucket = self._dest_worker_stats.setdefault(group, {})
            bucket[wid] = {"ema": ema, "n": n, "updated_at": 0.0}
            loaded += 1
        return loaded

    def _remap_dest_stats_worker_id(self, worker_id: str) -> None:
        """Promote short/prefix CSV keys to the live full worker_id."""
        wid = str(worker_id or "").strip()
        if not wid:
            return
        for _group, bucket in self._dest_worker_stats.items():
            if wid in bucket:
                continue
            matches = [
                k
                for k in list(bucket.keys())
                if k != wid and (wid.startswith(k) or k.startswith(wid))
            ]
            if len(matches) != 1:
                continue
            bucket[wid] = bucket.pop(matches[0])
            self._dest_stats_dirty = True

    def _lookup_dest_entry(self, dest_group: str, worker_id: str) -> Optional[dict]:
        bucket = self._dest_worker_stats.get(dest_group) or {}
        entry = bucket.get(worker_id)
        if entry is not None:
            return entry
        for key, ent in bucket.items():
            if worker_id.startswith(key) or key.startswith(worker_id):
                return ent
        return None

    def _maybe_save_dest_worker_stats(self, *, force: bool = False) -> None:
        path = DEST_AFFINITY_STATS_PATH
        if path is None:
            return
        # force=True (orch stop/restart): always write current snapshot.
        if not force and not self._dest_stats_dirty:
            return
        now = time.time()
        if (
            not force
            and (now - self._dest_stats_last_save) < DEST_AFFINITY_SAVE_INTERVAL_S
        ):
            return
        try:
            path.parent.mkdir(parents=True, exist_ok=True)
            payload = json.dumps(self._dest_worker_stats, indent=2, sort_keys=True)
            tmp = path.with_suffix(path.suffix + ".tmp")
            tmp.write_text(payload, encoding="utf-8")
            tmp.replace(path)
            self._dest_stats_dirty = False
            self._dest_stats_last_save = now
            rows = sum(len(v) for v in self._dest_worker_stats.values())
            if force:
                logger.info(
                    "dest affinity flushed %d worker×dest rows → %s",
                    rows,
                    path,
                )
            else:
                logger.debug("dest affinity saved path=%s rows=%d", path, rows)
        except Exception as exc:
            logger.warning("dest affinity stats save failed path=%s: %s", path, exc)

    def flush_dest_worker_stats(self) -> None:
        """Persist dest_group→worker history (call on orch stop/restart)."""
        self._dest_stats_dirty = True
        self._maybe_save_dest_worker_stats(force=True)

    def observe_dest_transfer(
        self,
        dest_group: str,
        worker_id: str,
        transfer_mbps: Optional[float],
    ) -> None:
        """Update EMA Mbps for (dest_group, worker) after a successful upload."""
        if not DEST_AFFINITY_ENABLED:
            return
        group = str(dest_group or "").strip()
        wid = str(worker_id or "").strip()
        if not group or not wid or transfer_mbps is None:
            return
        try:
            mbps = float(transfer_mbps)
        except (TypeError, ValueError):
            return
        if mbps <= 0:
            return
        self._remap_dest_stats_worker_id(wid)
        bucket = self._dest_worker_stats.setdefault(group, {})
        entry = bucket.get(wid)
        if entry is None:
            entry = {"ema": mbps, "n": 1, "updated_at": time.time()}
            bucket[wid] = entry
        else:
            prev = float(entry.get("ema") or mbps)
            n = int(entry.get("n") or 0) + 1
            entry["ema"] = (DEST_AFFINITY_EMA * mbps) + ((1.0 - DEST_AFFINITY_EMA) * prev)
            entry["n"] = n
            entry["updated_at"] = time.time()
        self._maybe_apply_dest_penalty(group, wid, entry, mbps)
        self._dest_stats_dirty = True
        self._maybe_save_dest_worker_stats()

    def _bucket_best_ema(self, dest_group: str) -> float:
        bucket = self._dest_worker_stats.get(dest_group) or {}
        best = 0.0
        for entry in bucket.values():
            if not isinstance(entry, dict):
                continue
            try:
                ema = float(entry.get("ema") or 0.0)
            except (TypeError, ValueError):
                continue
            if ema > best:
                best = ema
        return best

    def _should_penalize_mbps(self, mbps: float, best_ema: float) -> bool:
        if DEST_PENALTY_S <= 0:
            return False
        if DEST_PENALTY_MBPS > 0 and mbps < DEST_PENALTY_MBPS:
            return True
        if (
            DEST_PENALTY_REL > 0
            and best_ema > 0
            and mbps < (best_ema * DEST_PENALTY_REL)
        ):
            return True
        return False

    def _maybe_apply_dest_penalty(
        self,
        dest_group: str,
        worker_id: str,
        entry: dict,
        sample_mbps: float,
    ) -> None:
        """Soft-ban slow workers on this bucket using the observed sample Mbps."""
        if DEST_PENALTY_S <= 0:
            return
        best = self._bucket_best_ema(dest_group)
        now = time.time()
        if self._should_penalize_mbps(sample_mbps, best):
            until = now + DEST_PENALTY_S
            prev_until = float(entry.get("penalty_until") or 0.0)
            entry["penalty_until"] = max(prev_until, until)
            # Penalty strength = observed Mbps (keep the slower sample if already set).
            prev_pen = float(entry.get("penalty_mbps") or 0.0)
            if prev_pen > 0 and prev_until > now:
                entry["penalty_mbps"] = min(prev_pen, float(sample_mbps))
            else:
                entry["penalty_mbps"] = float(sample_mbps)
            logger.info(
                "dest penalty apply dest=%s worker=%s mbps=%.1f best=%.1f "
                "until_in=%.0fs penalty_mbps=%.1f",
                dest_group,
                short_id(worker_id),
                sample_mbps,
                best,
                entry["penalty_until"] - now,
                entry["penalty_mbps"],
            )
        elif float(entry.get("penalty_until") or 0.0) > now:
            # Fast sample clears an active penalty for this bucket.
            entry["penalty_until"] = 0.0
            entry["penalty_mbps"] = 0.0
            logger.info(
                "dest penalty clear dest=%s worker=%s mbps=%.1f best=%.1f",
                dest_group,
                short_id(worker_id),
                sample_mbps,
                best,
            )

    def penalize_dest_worker(
        self,
        dest_group: str,
        worker_id: str,
        *,
        reason: str = "",
        sample_mbps: Optional[float] = None,
    ) -> None:
        """Force a soft-ban for (dest_group, worker) — e.g. slow_net_wait abort."""
        if not DEST_AFFINITY_ENABLED or DEST_PENALTY_S <= 0:
            return
        group = str(dest_group or "").strip()
        wid = str(worker_id or "").strip()
        if not group or not wid:
            return
        self._remap_dest_stats_worker_id(wid)
        bucket = self._dest_worker_stats.setdefault(group, {})
        mbps = (
            float(sample_mbps)
            if sample_mbps is not None and float(sample_mbps) > 0
            else DEST_PENALTY_FALLBACK_MBPS
        )
        entry = bucket.get(wid)
        if entry is None:
            entry = {
                "ema": mbps,
                "n": 1,
                "updated_at": time.time(),
            }
            bucket[wid] = entry
        now = time.time()
        until = now + DEST_PENALTY_S
        prev_until = float(entry.get("penalty_until") or 0.0)
        entry["penalty_until"] = max(prev_until, until)
        prev_pen = float(entry.get("penalty_mbps") or 0.0)
        if prev_pen > 0 and prev_until > now:
            entry["penalty_mbps"] = min(prev_pen, mbps)
        else:
            entry["penalty_mbps"] = mbps
        entry["updated_at"] = now
        # Pull EMA toward the slow sample so ranking stays realistic after cooldown.
        prev_ema = float(entry.get("ema") or mbps)
        entry["ema"] = (DEST_AFFINITY_EMA * mbps) + ((1.0 - DEST_AFFINITY_EMA) * prev_ema)
        entry["n"] = int(entry.get("n") or 0) + 1
        self._dest_stats_dirty = True
        self._maybe_save_dest_worker_stats()
        logger.info(
            "dest penalty force dest=%s worker=%s reason=%s "
            "penalty_mbps=%.1f until_in=%.0fs",
            group,
            short_id(wid),
            reason or "manual",
            entry["penalty_mbps"],
            entry["penalty_until"] - now,
        )

    def dest_worker_penalized(self, dest_group: str, worker_id: str) -> bool:
        entry = self._lookup_dest_entry(dest_group, worker_id)
        if entry is None:
            return False
        try:
            return float(entry.get("penalty_until") or 0.0) > time.time()
        except (TypeError, ValueError):
            return False

    def dest_worker_mbps(self, dest_group: str, worker_id: str) -> float:
        """Historical Mbps for worker on dest_group; falls back to global avg.

        While ``penalty_until`` is active, returns the observed ``penalty_mbps``
        (not a fixed floor) so slower samples rank worse than mildly slow ones.
        """
        group = str(dest_group or "").strip()
        wid = str(worker_id or "").strip()
        if DEST_AFFINITY_ENABLED and group and wid:
            entry = self._lookup_dest_entry(group, wid)
            if entry is not None:
                try:
                    n = int(entry.get("n") or 0)
                    ema = float(entry.get("ema") or 0.0)
                    penalty_until = float(entry.get("penalty_until") or 0.0)
                    penalty_mbps = float(entry.get("penalty_mbps") or 0.0)
                except (TypeError, ValueError):
                    n, ema, penalty_until, penalty_mbps = 0, 0.0, 0.0, 0.0
                if penalty_until > time.time():
                    # Prefer stored sample Mbps; fall back to EMA / constant.
                    if penalty_mbps > 0:
                        return penalty_mbps
                    if ema > 0:
                        return ema
                    return DEST_PENALTY_FALLBACK_MBPS
                if n >= DEST_AFFINITY_MIN_SAMPLES and ema > 0:
                    return ema
        return self._get_profile(wid).average_mbps

    def _free_workers_for_affinity_wave(
        self, *, dest_group: str = ""
    ) -> list[str]:
        """All idle workers for a min-makespan wave (finish the batch ASAP).

        Includes penalized workers. Cost = expected seconds from observed Mbps, so
        matching leaves slow workers idle when there are enough fast free slots,
        but uses them when more offers remain than fast free workers.
        Order: unique-IP non-penalized, unique-IP penalized, then same-IP extras.
        """
        used_ips: set[str] = set()
        unique_good: list[str] = []
        unique_pen: list[str] = []
        extras_good: list[str] = []
        extras_pen: list[str] = []
        dest_g = str(dest_group or "").strip() if DEST_AFFINITY_ENABLED else ""

        for wid in self._ordered_connected_worker_ids():
            if not self._worker_is_free(wid):
                continue
            penalized = bool(dest_g) and self.dest_worker_penalized(dest_g, wid)
            ip = self._get_profile(wid).ip.strip()
            if ip and ip in used_ips:
                (extras_pen if penalized else extras_good).append(wid)
                continue
            if ip:
                used_ips.add(ip)
            (unique_pen if penalized else unique_good).append(wid)

        return unique_good + unique_pen + extras_good + extras_pen

    def _offer_expected_seconds(self, offer: dict, worker_id: str) -> float:
        dest_g = dest_group_from_offer(offer)
        mbps = self.dest_worker_mbps(dest_g, worker_id) if dest_g else (
            self._get_profile(worker_id).average_mbps
        )
        if mbps <= 0:
            mbps = 50.0  # cold-start prior — avoid infinite cost
        return expected_transfer_seconds(offer_byte_size(offer), mbps)

    def _assign_offers_min_makespan(
        self,
        offers: list[dict],
        workers: list[str],
    ) -> tuple[list[str], float]:
        """Map offers → workers minimizing expected last completion time.

        Goal: all tasks in this wave finish as soon as possible (min makespan),
        not max-Mbps on each offer independently.
        """
        if not offers or not workers:
            return [], 0.0
        n = min(len(offers), len(workers))
        offers_n = offers[:n]
        # Full free pool: matching can leave slow workers idle when workers > offers.
        workers_n = list(workers)
        costs = [
            [self._offer_expected_seconds(offer, wid) for wid in workers_n]
            for offer in offers_n
        ]
        assign_idx, makespan = min_makespan_assignment(costs)
        out: list[str] = []
        for w_i in assign_idx:
            if w_i < 0 or w_i >= len(workers_n):
                out.append("")
            else:
                out.append(workers_n[w_i])
        return out, makespan

    async def _drain_affinity_makespan_wave(
        self,
        *,
        batch_used_ips: set[str],
        batch_assigned_counts: dict[str, int],
    ) -> int:
        """Assign next free-worker wave via min-makespan (last-done objective)."""
        if not DEST_AFFINITY_ENABLED or not DEST_AFFINITY_MAKESPAN:
            return 0
        if not self._overflow_offers:
            return 0

        # Head dest only affects penalty labeling in the free pool order.
        head_dest = dest_group_from_offer(self._overflow_offers[0])
        free = self._free_workers_for_affinity_wave(dest_group=head_dest)
        if not free:
            return 0

        n = min(len(free), len(self._overflow_offers))
        # Need ≥2 free workers for combinatorial assignment to matter.
        if n <= 1:
            return 0

        offers = [self._overflow_offers[i] for i in range(n)]
        # Makespan assumes every free worker is eligible for every head offer.
        # Offers with prior failures (slow_net_wait) need per-offer excludes → greedy.
        if any(
            self._offer_attempted_workers.get(self._offer_key(o))
            for o in offers
        ):
            return 0
        # More free workers than offers: pass the full free pool so matching can
        # leave the slowest idle (better makespan than forcing a slow pairing).
        worker_ids, makespan = self._assign_offers_min_makespan(offers, free)
        if len(worker_ids) != n or any(not w for w in worker_ids):
            return 0
        # Guard against duplicate worker assignment (should not happen).
        if len(set(worker_ids)) != n:
            logger.warning(
                "makespan assignment has duplicate workers; falling back to greedy"
            )
            return 0

        for _ in range(n):
            offer = self._overflow_offers.popleft()
            oid = self._offer_key(offer)
            if oid:
                self._overflow_offer_ids.discard(oid)

        # Parallel WS sends — serial await added ~N×RTT delay on first wave.
        async def _deliver_one(
            offer: dict, worker_id: str
        ) -> tuple[dict, str, bool]:
            ok = await self.deliver_task_offer(worker_id, offer)
            return offer, worker_id, ok

        results = await asyncio.gather(
            *[_deliver_one(o, w) for o, w in zip(offers, worker_ids)],
            return_exceptions=True,
        )

        delivered = 0
        undelivered: list[dict] = []
        for item, fallback in zip(results, zip(offers, worker_ids)):
            offer_fb, worker_fb = fallback
            if isinstance(item, BaseException):
                logger.warning(
                    "makespan deliver crashed worker=%s: %s",
                    short_id(worker_fb),
                    item,
                )
                undelivered.append(offer_fb)
                continue
            offer, worker_id, ok = item
            if not ok:
                undelivered.append(offer)
                continue
            delivered += 1
            batch_assigned_counts[worker_id] = (
                batch_assigned_counts.get(worker_id, 0) + 1
            )
            ip = self._get_profile(worker_id).ip.strip()
            if ip:
                batch_used_ips.add(ip)
            profile = self._get_profile(worker_id)
            dest_group = dest_group_from_offer(offer)
            dest_mbps = (
                self.dest_worker_mbps(dest_group, worker_id) if dest_group else 0.0
            )
            exp_s = self._offer_expected_seconds(offer, worker_id)
            logger.info(
                "_workers | queue_deliver task=%s offer=%s worker=%s "
                "active=%d pending=%d pressure=%s dest_group=%s dest_mbps=%.1f "
                "assign=makespan exp_s=%.2f makespan_s=%.2f",
                short_id(offer.get("task_id")),
                short_id(self._offer_key(offer)),
                short_id(worker_id),
                profile.active_count,
                len(self._overflow_offers) + len(undelivered),
                False,
                dest_group or "-",
                dest_mbps,
                exp_s,
                makespan,
            )

        for offer in reversed(undelivered):
            oid = self._offer_key(offer)
            if oid and oid not in self._overflow_offer_ids:
                self._overflow_offers.appendleft(offer)
                self._overflow_offer_ids.add(oid)
            elif not oid:
                self._overflow_offers.appendleft(offer)

        if undelivered:
            logger.warning(
                "_workers | makespan_wave partial delivered=%d requeued=%d "
                "pending=%d",
                delivered,
                len(undelivered),
                len(self._overflow_offers),
            )
        return delivered

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
        self._remap_dest_stats_worker_id(worker_id)
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

    def mark_worker_idle(
        self,
        worker_id: str,
        offer_id: Optional[str] = None,
        *,
        drain_overflow: bool = True,
    ) -> None:
        profile = self._get_profile(worker_id)
        if offer_id:
            profile.active_offer_ids.discard(str(offer_id))
        else:
            profile.active_offer_ids.clear()
        if drain_overflow:
            # Prefer giving the next queued offer to the worker that just freed.
            self._overflow_prefer_worker = worker_id
            self._schedule_overflow_drain()

    def _block_worker_dispatch(self, worker_id: str, *, cooldown_s: float = 1.0) -> None:
        """Skip worker for dispatch after queue_full until cooldown or success."""
        wid = str(worker_id)
        self._dispatch_blocked.add(wid)
        old = self._dispatch_unblock_tasks.pop(wid, None)
        if old and not old.done():
            old.cancel()
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            return

        async def _unblock() -> None:
            try:
                await asyncio.sleep(max(0.05, cooldown_s))
            except asyncio.CancelledError:
                return
            self._dispatch_blocked.discard(wid)
            self._dispatch_unblock_tasks.pop(wid, None)
            if self._overflow_offers:
                self._schedule_overflow_drain()

        self._dispatch_unblock_tasks[wid] = loop.create_task(
            _unblock(), name=f"dispatch-unblock-{wid[:8]}"
        )

    def _clear_worker_dispatch_block(self, worker_id: str) -> None:
        wid = str(worker_id)
        self._dispatch_blocked.discard(wid)
        old = self._dispatch_unblock_tasks.pop(wid, None)
        if old and not old.done():
            old.cancel()

    def _offer_key(self, offer: dict) -> str:
        return str(offer.get("offer_id") or offer.get("task_id") or "").strip()

    def _enqueue_overflow(self, offer: dict, *, quiet: bool = False) -> bool:
        """Queue offer for dispatcher delivery when an idle worker appears."""
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
                "offer queue full (%d); cannot hold task=%s offer=%s",
                OVERFLOW_QUEUE_MAX,
                short_id(offer.get("task_id")),
                short_id(offer_id),
            )
            return False
        payload = dict(offer)
        self._overflow_offers.append(payload)
        self._overflow_offer_ids.add(offer_id)
        self._pending_offers[offer_id] = payload
        self._offer_attempted_workers.setdefault(offer_id, set())
        self._remember_offer_context(payload)
        if not quiet:
            logger.info(
                "_workers | queue_enqueue task=%s offer=%s pending=%d",
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
        loop.create_task(self._overflow_drain_loop(), name="offer-queue-drain")

    async def _overflow_drain_loop(self) -> None:
        try:
            while self._overflow_drain_pending:
                self._overflow_drain_pending = False
                await self._drain_overflow_queue()
        finally:
            self._overflow_drain_running = False
            if self._overflow_drain_pending and self._overflow_offers:
                self._schedule_overflow_drain()

    def _worker_is_free(self, worker_id: str) -> bool:
        if worker_id in self._dispatch_blocked:
            return False
        if worker_id not in self._sessions and self._outbound_send is None:
            return False
        profile = self._get_profile(worker_id)
        return profile.active_count == 0

    async def _drain_overflow_queue(self) -> int:
        """Deliver queued offers only to free workers (unless pressure mode).

        Free = active_count==0 and not dispatch-blocked. When pending exceeds
        ``ORCH_OVERFLOW_BUSY_THRESHOLD``, allow stacking up to
        ``ORCH_OVERFLOW_BUSY_MAX_PER_WORKER``.

        Serialized by ``_overflow_drain_lock`` so batch + task_done drains cannot
        double-deliver the same free worker (that caused queue_full storms).
        """
        async with self._overflow_drain_lock:
            return await self._drain_overflow_queue_locked()

    async def _drain_overflow_queue_locked(self) -> int:
        delivered = 0
        pressure_logged = False
        prefer = self._overflow_prefer_worker
        self._overflow_prefer_worker = None
        batch_used_ips: set[str] = set()
        batch_assigned_counts: dict[str, int] = {}
        # Workers that failed deliver this drain — skip, try other free workers.
        deliver_failed: set[str] = set()

        # First: batch-assign free workers to head offers minimizing expected
        # last completion (makespan) — finish all queued work ASAP. Greedy
        # max-Mbps per offer can leave a bad worker×dest as the straggler.
        pending0 = len(self._overflow_offers)
        pressure0 = (
            OVERFLOW_BUSY_THRESHOLD > 0 and pending0 > OVERFLOW_BUSY_THRESHOLD
        )
        if not pressure0:
            wave = await self._drain_affinity_makespan_wave(
                batch_used_ips=batch_used_ips,
                batch_assigned_counts=batch_assigned_counts,
            )
            delivered += wave

        while self._overflow_offers:
            pending = len(self._overflow_offers)
            pressure = (
                OVERFLOW_BUSY_THRESHOLD > 0 and pending > OVERFLOW_BUSY_THRESHOLD
            )
            offer = self._overflow_offers[0]
            offer_id = self._offer_key(offer)
            dest_group = dest_group_from_offer(offer)
            excluded = set(self._dispatch_blocked) | deliver_failed
            attempted = self._offer_attempted_workers.get(offer_id) or set()
            # Exclude workers that already failed this offer (e.g. slow_net_wait),
            # but allow capacity-retry prefer to re-deliver to the same worker.
            if not (prefer and prefer in attempted):
                excluded |= set(attempted)

            worker_id: Optional[str] = None
            use_prefer = False
            if prefer and prefer not in excluded and (
                self._worker_is_free(prefer)
                or (
                    pressure
                    and prefer in self._sessions
                    and self._get_profile(prefer).active_count
                    < OVERFLOW_BUSY_MAX_PER_WORKER
                )
            ):
                free_n = sum(
                    1
                    for wid in self._sessions
                    if wid not in deliver_failed and self._worker_is_free(wid)
                )
                # Prefer only when it is the sole free slot (or pressure stacking).
                # Do NOT sticky-prefer when dest_group is empty — that pinned all
                # sequential 1-offer batches onto the last finisher.
                if free_n <= 1 or pressure:
                    use_prefer = True
            if use_prefer:
                worker_id = prefer
                prefer = None
            else:
                prefer = None
                worker_id = self.select_worker_round_robin(
                    batch_used_ips=batch_used_ips,
                    batch_assigned_counts=batch_assigned_counts,
                    exclude_worker_ids=excluded,
                    force_allow_busy=pressure,
                    assign_cap=OVERFLOW_BUSY_MAX_PER_WORKER if pressure else None,
                    dest_group=dest_group or None,
                )

            if not worker_id:
                break

            if pressure and not pressure_logged:
                pressure_logged = True
                logger.info(
                    "_workers | queue_pressure pending=%d threshold=%d "
                    "busy_cap=%d — stacking onto busy workers",
                    pending,
                    OVERFLOW_BUSY_THRESHOLD,
                    OVERFLOW_BUSY_MAX_PER_WORKER,
                )

            self._overflow_offers.popleft()
            if offer_id:
                self._overflow_offer_ids.discard(offer_id)

            ok = await self.deliver_task_offer(worker_id, offer)
            if ok:
                delivered += 1
                batch_assigned_counts[worker_id] = (
                    batch_assigned_counts.get(worker_id, 0) + 1
                )
                ip = self._get_profile(worker_id).ip.strip()
                if ip:
                    batch_used_ips.add(ip)
                profile = self._get_profile(worker_id)
                dest_mbps = (
                    self.dest_worker_mbps(dest_group, worker_id) if dest_group else 0.0
                )
                logger.info(
                    "_workers | queue_deliver task=%s offer=%s worker=%s "
                    "active=%d pending=%d pressure=%s dest_group=%s dest_mbps=%.1f "
                    "assign=greedy",
                    short_id(offer.get("task_id")),
                    short_id(offer_id),
                    short_id(worker_id),
                    profile.active_count,
                    len(self._overflow_offers),
                    pressure,
                    dest_group or "-",
                    dest_mbps,
                )
            else:
                # Re-queue and keep draining to other free workers (do not abort).
                if offer_id and offer_id not in self._overflow_offer_ids:
                    self._overflow_offers.appendleft(offer)
                    self._overflow_offer_ids.add(offer_id)
                deliver_failed.add(worker_id)
                logger.warning(
                    "_workers | deliver_fail task=%s offer=%s worker=%s "
                    "requeued; trying other free workers",
                    short_id(offer.get("task_id")),
                    short_id(offer_id),
                    short_id(worker_id),
                )
                continue
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
        self.flush_dest_worker_stats()
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

    @staticmethod
    def _is_slow_net_wait_error(error: object) -> bool:
        text = str(error or "").strip().lower()
        return "slow_net_wait:" in text or text.startswith("slow_net_wait")

    def _schedule_capacity_retry(self, worker_id: str, *, delay_s: float = 0.05) -> None:
        """Retry overflow to the same worker after a brief yield (slot race)."""
        wid = str(worker_id)
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            return

        async def _retry() -> None:
            try:
                await asyncio.sleep(max(0.01, delay_s))
            except asyncio.CancelledError:
                return
            self._clear_worker_dispatch_block(wid)
            if self._overflow_offers:
                self._overflow_prefer_worker = wid
                self._schedule_overflow_drain()

        loop.create_task(_retry(), name=f"capacity-retry-{wid[:8]}")

    async def _maybe_reassign_on_capacity_reject(
        self, worker_id: str, msg: dict
    ) -> bool:
        """If worker rejected for queue/memory, re-queue and retry same worker.

        Do not hop across every finishing worker — that adds multi-ms delay and
        storms. Brief defer lets the worker finish releasing its WS slot.
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
        self.mark_worker_idle(worker_id, offer_id, drain_overflow=False)
        # Short block so concurrent drains skip this worker until deferred retry.
        self._block_worker_dispatch(worker_id, cooldown_s=0.05)

        if self._enqueue_overflow(offer):
            logger.info(
                "capacity reject: re-queued task=%s offer=%s from=%s "
                "attempted=%d pending=%d (retry same worker)",
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
            self._schedule_capacity_retry(worker_id, delay_s=0.05)
            return True

        logger.warning(
            "capacity reject: queue full task=%s offer=%s from=%s "
            "attempted=%d error=%s",
            short_id(task_id),
            short_id(offer_id),
            short_id(worker_id),
            len(attempted),
            msg.get("error"),
        )
        return False

    async def _maybe_reassign_on_slow_net_wait(
        self, worker_id: str, msg: dict
    ) -> bool:
        """If worker aborted for high net_wait, re-queue to a different worker.

        Does not relay the failure upstream while a local redelivery is possible.
        """
        if bool(msg.get("success")):
            return False
        if not self._is_slow_net_wait_error(msg.get("error")):
            return False

        task_id = msg.get("task_id")
        offer_id = str(msg.get("offer_id") or task_id or "").strip()
        if not offer_id:
            return False

        offer = self._pending_offers.get(offer_id)
        if not offer:
            logger.warning(
                "slow_net_wait without pending offer: task=%s offer=%s "
                "worker=%s error=%s",
                short_id(task_id),
                short_id(offer_id),
                short_id(worker_id),
                msg.get("error"),
            )
            return False

        attempted = self._offer_attempted_workers.setdefault(offer_id, set())
        attempted.add(str(worker_id))
        dest_group = dest_group_from_offer(offer)
        if dest_group:
            # Prefer observed transfer_mbps from the abort result when present.
            sample = transfer_mbps_from_result(msg)
            self.penalize_dest_worker(
                dest_group,
                worker_id,
                reason=str(msg.get("error") or "slow_net_wait"),
                sample_mbps=sample if sample is not None else None,
            )
        connected = set(self._sessions.keys())
        # Avoid infinite local loop when every connected worker already failed.
        if connected and attempted.issuperset(connected):
            logger.warning(
                "slow_net_wait: all connected workers attempted task=%s "
                "offer=%s attempted=%d connected=%d — relay failure",
                short_id(task_id),
                short_id(offer_id),
                len(attempted),
                len(connected),
            )
            return False

        self.mark_worker_idle(worker_id, offer_id, drain_overflow=False)
        # Do not prefer the slow worker — drain will exclude attempted set.
        if self._overflow_prefer_worker == worker_id:
            self._overflow_prefer_worker = None

        if self._enqueue_overflow(offer):
            logger.info(
                "slow_net_wait: re-queued task=%s offer=%s from=%s "
                "attempted=%d pending=%d dest_group=%s (other workers)",
                short_id(task_id),
                short_id(offer_id),
                short_id(worker_id),
                len(attempted),
                len(self._overflow_offers),
                dest_group or "-",
            )
            await self._send_to_worker(
                worker_id,
                {
                    "type": "task_result_ack",
                    "task_id": task_id,
                    "offer_id": offer_id,
                    "received": True,
                    "status": "late_superseded",
                    "reason": f"queued_after_slow_net:{msg.get('error')}",
                },
            )
            await self._drain_overflow_queue()
            return True

        logger.warning(
            "slow_net_wait: queue full task=%s offer=%s from=%s "
            "attempted=%d error=%s",
            short_id(task_id),
            short_id(offer_id),
            short_id(worker_id),
            len(attempted),
            msg.get("error"),
        )
        return False

    async def deliver_task_offer(
        self,
        worker_id: str,
        offer: dict,
        *,
        mark_busy: bool = True,
    ) -> bool:
        # Local WS session, or outbound relay (coordinator/global pool).
        ws = self._sessions.get(worker_id)
        if ws is None and self._outbound_send is None:
            logger.warning("deliver_task_offer: worker %s not connected", worker_id)
            return False
        try:
            payload = {"type": "task_offer", **offer}
            if ws is not None:
                await ws.send_text(json.dumps(payload))
            else:
                result = self._outbound_send(worker_id, payload)
                if asyncio.iscoroutine(result):
                    await result
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
        force_allow_busy: bool = False,
        assign_cap: Optional[int] = None,
        dest_group: Optional[str] = None,
    ) -> Optional[str]:
        """Pick the next worker with capacity, matching global-gateway batch IP spread.

        Goal: protect first-wave single-stream Mbps (makespan of the first
        assignment wave), then finish overflow when workers free.

        When ``ORCH_PREFER_IDLE_WORKERS`` is on (default):
          1. Prefer idle workers (effective load 0) on a fresh IP
          2. Then idle workers on any IP
          3. Only if ``ORCH_ALLOW_BUSY_WORKER_REUSE`` / ``force_allow_busy``,
             reuse busy workers that still have capacity

        When ``ORCH_DEST_AFFINITY`` is on and ``dest_group`` is set, among
        equally-idle eligible workers prefer higher historical Mbps for that
        destination group (falls back to global average until samples exist).
        Free non-penalized workers are tried before free penalized ones; if
        every non-penalized worker is busy, a free penalized worker is still
        selected (do not stall the queue).

        ``assign_cap`` (overflow pressure mode): require ``load < assign_cap``
        instead of hello ``max_concurrent_tasks`` so backlog can stack up to N
        tasks per worker.

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
        allow_busy_reuse = ALLOW_BUSY_WORKER_REUSE or force_allow_busy
        cap = int(assign_cap) if assign_cap is not None and assign_cap > 0 else None
        dest_g = str(dest_group or "").strip() if DEST_AFFINITY_ENABLED else ""

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

        def _has_assign_capacity(worker_id: str) -> bool:
            load = _load(worker_id)
            if cap is not None:
                return load < cap
            return self._get_profile(worker_id).has_capacity

        def _score_mbps(worker_id: str) -> float:
            if dest_g:
                return self.dest_worker_mbps(dest_g, worker_id)
            return self._get_profile(worker_id).average_mbps

        def _eligible(
            worker_id: str,
            *,
            allow_ip_reuse: bool,
            require_idle: bool,
            allow_worker_reuse: bool,
            exclude_penalized: bool = False,
            only_penalized: bool = False,
        ) -> bool:
            if worker_id in excluded:
                return False
            if not _has_assign_capacity(worker_id):
                return False
            load = _load(worker_id)
            if require_idle and load > 0:
                return False
            if not allow_worker_reuse and _batch_count(worker_id) > 0:
                return False
            if dest_g and (exclude_penalized or only_penalized):
                penalized = self.dest_worker_penalized(dest_g, worker_id)
                if exclude_penalized and penalized:
                    return False
                if only_penalized and not penalized:
                    return False
            ip = self._get_profile(worker_id).ip.strip()
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
            exclude_penalized: bool = False,
            only_penalized: bool = False,
        ) -> Optional[str]:
            # (load, batch_n, -dest_mbps, -initial_order, offset, worker_id)
            candidates: list[tuple[int, int, float, int, int, str]] = []
            for offset in range(pool_size):
                idx = (start + offset) % pool_size
                worker_id = connected[idx]
                if not _eligible(
                    worker_id,
                    allow_ip_reuse=allow_ip_reuse,
                    require_idle=require_idle,
                    allow_worker_reuse=allow_worker_reuse,
                    exclude_penalized=exclude_penalized,
                    only_penalized=only_penalized,
                ):
                    continue
                profile = self._get_profile(worker_id)
                candidates.append(
                    (
                        _load(worker_id),
                        _batch_count(worker_id),
                        -_score_mbps(worker_id),
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
                "load=%d batch_n=%d mbps=%.1f dest_group=%s dest_mbps=%.1f "
                "cursor=%d pool=%d batch_ips=%s "
                "require_idle=%s reuse_worker=%s assign_cap=%s "
                "exclude_penalized=%s only_penalized=%s",
                worker_id,
                profile.ip or "?",
                profile.active_count,
                profile.max_concurrent_tasks,
                _load_n,
                _bc,
                profile.average_mbps,
                dest_g or "-",
                -_neg_mbps,
                self._cursor,
                pool_size,
                ",".join(sorted(batch_used_ips)) if batch_used_ips else "-",
                require_idle,
                allow_worker_reuse,
                cap if cap is not None else "-",
                exclude_penalized,
                only_penalized,
            )
            return worker_id

        def _pick_idle(
            *,
            allow_ip_reuse: bool,
            exclude_penalized: bool = False,
            only_penalized: bool = False,
        ) -> Optional[str]:
            return _pick(
                allow_ip_reuse=allow_ip_reuse,
                require_idle=True,
                allow_worker_reuse=False,
                exclude_penalized=exclude_penalized,
                only_penalized=only_penalized,
            )

        def _pick_busy(
            *,
            allow_ip_reuse: bool,
            exclude_penalized: bool = False,
        ) -> Optional[str]:
            return _pick(
                allow_ip_reuse=allow_ip_reuse,
                require_idle=False,
                allow_worker_reuse=True,
                exclude_penalized=exclude_penalized,
            )

        def _pick_idle_prefer_unpenalized(*, allow_ip_reuse: bool) -> Optional[str]:
            """Free non-penalized first; if none, free penalized (still assign)."""
            if dest_g:
                worker_id = _pick_idle(
                    allow_ip_reuse=allow_ip_reuse, exclude_penalized=True
                )
                if worker_id:
                    return worker_id
                return _pick_idle(
                    allow_ip_reuse=allow_ip_reuse, only_penalized=True
                )
            return _pick_idle(allow_ip_reuse=allow_ip_reuse)

        if prefer_idle:
            # Exhaust free non-penalized (fresh IP → any IP), then free penalized,
            # then busy reuse. Penalized free workers still get work when needed.
            if dest_g:
                worker_id = _pick_idle(
                    allow_ip_reuse=False, exclude_penalized=True
                )
                if worker_id:
                    return worker_id
                if allow_used_ip:
                    worker_id = _pick_idle(
                        allow_ip_reuse=True, exclude_penalized=True
                    )
                    if worker_id:
                        return worker_id
                worker_id = _pick_idle(
                    allow_ip_reuse=False, only_penalized=True
                )
                if worker_id:
                    return worker_id
                if allow_used_ip:
                    worker_id = _pick_idle(
                        allow_ip_reuse=True, only_penalized=True
                    )
                    if worker_id:
                        return worker_id
            else:
                worker_id = _pick_idle(allow_ip_reuse=False)
                if worker_id:
                    return worker_id
                if allow_used_ip:
                    worker_id = _pick_idle(allow_ip_reuse=True)
                    if worker_id:
                        return worker_id
            if not allow_busy_reuse:
                return None
            # Busy reuse: still prefer non-penalized, then any busy with capacity.
            if dest_g:
                worker_id = _pick_busy(
                    allow_ip_reuse=False, exclude_penalized=True
                )
                if worker_id:
                    return worker_id
            worker_id = _pick_busy(allow_ip_reuse=False)
            if worker_id:
                return worker_id
            if allow_used_ip:
                if dest_g:
                    worker_id = _pick_busy(
                        allow_ip_reuse=True, exclude_penalized=True
                    )
                    if worker_id:
                        return worker_id
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
        """Enqueue all offers, then dispatch to free workers from the queue.

        Model:
          1. Every valid offer goes on the orch queue (never rejected for capacity).
          2. Dispatcher assigns only to free workers (active==0).
          3. When none are free, wait for task_done → deliver next to that worker.
          4. If queue grows past ORCH_OVERFLOW_BUSY_THRESHOLD, allow limited stacking.
        """
        queued = 0
        failed = 0

        for offer in offers:
            if not isinstance(offer, dict):
                failed += 1
                continue
            if self._enqueue_overflow(offer, quiet=True):
                queued += 1
            else:
                failed += 1
                logger.warning(
                    "Offer queue rejected task=%s (queue full or disabled)",
                    offer.get("task_id"),
                )

        delivered = await self._drain_overflow_queue()
        still_pending = len(self._overflow_offers)

        logger.info(
            "_workers | batch queued=%s delivered=%s pending=%s failed=%s "
            "(queue-first dispatch)",
            queued,
            delivered,
            still_pending,
            failed,
        )
        # Remaining offers wait for task_done → mark_worker_idle → drain.
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
                # Offer re-queued; deferred retry handles re-deliver.
                return
            if await self._maybe_reassign_on_slow_net_wait(worker_id, msg):
                # Offer re-queued to a different free worker.
                return
            # Peek dest before _log_external_task_result pops offer context.
            offer_key = str(offer_id) if offer_id else ""
            ctx = self._offer_contexts.get(offer_key) or {}
            dest_group = dest_group_from_url(ctx.get("dest_url"))
            if not dest_group:
                pending = self._pending_offers.get(offer_key) or {}
                dest_group = dest_group_from_offer(pending)

            transfer_mbps = transfer_mbps_from_result(msg)
            if transfer_mbps is not None:
                self._get_profile(worker_id).observe_transfer(transfer_mbps)
            if bool(msg.get("success")) and dest_group and transfer_mbps is not None:
                self.observe_dest_transfer(dest_group, worker_id, transfer_mbps)

            self._log_external_task_result(worker_id, msg)

            # 1) Free slot + deliver next queued offer to this worker FIRST
            # 2) Then relay/submit task_result upstream (BeamCore)
            self._clear_worker_dispatch_block(worker_id)
            self.mark_worker_idle(
                worker_id,
                str(offer_id) if offer_id else None,
                drain_overflow=False,
            )
            self._overflow_prefer_worker = worker_id
            await self._drain_overflow_queue()

            self._clear_pending_offer(str(offer_id) if offer_id else None)
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

