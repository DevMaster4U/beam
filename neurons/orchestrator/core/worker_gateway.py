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
    stamp_offer_task_key,
    task_key_log_label,
    transfer_context_range_label,
    transfer_context_urls,
)
from core.range_coverage import offer_coverage_state

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
# Re-queue offers rejected with cache_miss_not_accepted to workers that
# advertise non_cached_file=true (WORKER_NON_CACHED_FILE).
CACHE_MISS_REOFFER = _env_bool("ORCH_CACHE_MISS_REOFFER", True)
# When true, first-wave assignment prefers workers with non_cached_file=true
# (miss-capable). When false (default), prefer cache-only workers first so
# hits stay on them and misses get rejected+reoffered to miss-capable.
PREFER_NON_CACHED_WORKERS = _env_bool("ORCH_PREFER_NON_CACHED_WORKERS", False)
CACHE_MISS_NOT_ACCEPTED = "cache_miss_not_accepted"
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

# Prefer free workers with the highest average Mbps for this dest_group
# (R2 host / destinations/<group>/…). Stats: running avg per (dest, worker).
DEST_AFFINITY_ENABLED = _env_bool("ORCH_DEST_AFFINITY", True)
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
# Explicit only — a non-empty default used to re-seed after every restart/clear.
_DEST_SEED_CSV_RAW = os.environ.get("ORCH_DEST_AFFINITY_SEED_CSV", "").strip()
DEST_AFFINITY_SEED_CSV = _resolve_stats_path(_DEST_SEED_CSV_RAW) if _DEST_SEED_CSV_RAW else None
# Wipe JSON (+ skip CSV seed) before first load. Also: --clear-affinity on main.py /
# ORCH_DEST_AFFINITY_CLEAR_ON_START=true, or POST /workers/affinity/clear at runtime.
DEST_AFFINITY_CLEAR_ON_START = _env_bool("ORCH_DEST_AFFINITY_CLEAR_ON_START", False)
try:
    DEST_AFFINITY_SAVE_INTERVAL_S = max(
        1.0, float(os.environ.get("ORCH_DEST_AFFINITY_SAVE_INTERVAL_S", "10"))
    )
except ValueError:
    DEST_AFFINITY_SAVE_INTERVAL_S = 10.0


def dest_group_from_url(dest_url: object) -> str:
    """Affinity bucket key for a dest URL.

    Worker speed differs per R2 host × destinations/<group> (backup/primary/…),
    e.g. ``ef88….r2.cloudflarestorage.com/beam-xfer-test/destinations/backup2``.
    """
    text = str(dest_url or "").strip()
    if not text:
        return ""
    try:
        parsed = urlsplit(text)
        host = (parsed.hostname or "").lower()
        path_parts = [p for p in parsed.path.split("/") if p]
        if "destinations" in path_parts:
            i = path_parts.index("destinations")
            if i + 1 < len(path_parts):
                # host + path through destinations/<group> (exclude object key).
                prefix = "/".join(path_parts[: i + 2])
                if host:
                    return f"{host}/{prefix}"
                return prefix
        if host:
            return host
        return ""
    except Exception:
        return ""


def dest_group_short_name(dest_group: object) -> str:
    """``…/destinations/backup2`` → ``backup2`` (CSV seed / legacy keys)."""
    text = str(dest_group or "").strip()
    if not text:
        return ""
    if "/destinations/" in text:
        return text.rsplit("/", 1)[-1]
    if "/" not in text:
        return text
    return text.rsplit("/", 1)[-1]


def offer_queue_wait_ms(offer: dict) -> float:
    """Milliseconds since this offer was last put on the orch overflow queue."""
    raw = offer.get("_orch_queued_at")
    try:
        started = float(raw)
    except (TypeError, ValueError):
        return 0.0
    if started <= 0:
        return 0.0
    return max(0.0, (time.monotonic() - started) * 1000.0)


def stamp_offer_queued(offer: dict) -> dict:
    """Mark enqueue time + cache task_key once (deliver/result never re-hash)."""
    offer["_orch_queued_at"] = time.monotonic()
    stamp_offer_task_key(offer)
    return offer


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


@dataclass
class _WorkerProfile:
    worker_id: str
    ip: str = ""
    max_concurrent_tasks: int = 5
    worker_version: str = ""
    initial_order: int = 0
    # True when worker accepts cache-miss transfers (WORKER_NON_CACHED_FILE).
    non_cached_file: bool = True
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
        # offer_ids that must go to non_cached_file=true workers (after miss reject).
        self._offer_require_non_cached: Set[str] = set()
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
        # dest_group → worker_id → {avg_mbps, n, updated_at}
        self._dest_worker_stats: Dict[str, Dict[str, dict]] = {}
        self._dest_stats_dirty = False
        self._dest_stats_last_save = 0.0
        # Re-read env: --clear-affinity sets ORCH_DEST_AFFINITY_CLEAR_ON_START in config
        # before this module is imported; runtime check covers late env injection too.
        if DEST_AFFINITY_CLEAR_ON_START or _env_bool(
            "ORCH_DEST_AFFINITY_CLEAR_ON_START", False
        ):
            self.clear_dest_worker_stats(reason="clear_on_start")
        else:
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

    def worker_ip_address(self, worker_id: str) -> str:
        """Public / reported IP for logging (from hello/connect profile)."""
        ip = (self._get_profile(worker_id).ip or "").strip()
        return ip or "-"

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
                    avg = float(entry.get("avg_mbps") or entry.get("ema") or 0.0)
                    n = int(entry.get("n") or 0)
                except (TypeError, ValueError):
                    continue
                if avg <= 0 or n <= 0:
                    continue
                bucket[wid] = {
                    "avg_mbps": avg,
                    "n": n,
                    "updated_at": float(entry.get("updated_at") or 0.0),
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
                avg = float(avg_s)
            except (TypeError, ValueError):
                continue
            if n <= 0 or avg <= 0:
                continue
            bucket = self._dest_worker_stats.setdefault(group, {})
            bucket[wid] = {"avg_mbps": avg, "n": n, "updated_at": 0.0}
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
        """Find stats for (dest_group, worker), with short-group / prefix fallbacks.

        Full keys look like ``host/…/destinations/backup2``. CSV seed still uses
        short names (``backup2``) — fall back so first-wave affinity is not cold.
        """
        group = str(dest_group or "").strip()
        wid = str(worker_id or "").strip()
        if not group or not wid:
            return None

        def _from_bucket(bucket: dict) -> Optional[dict]:
            entry = bucket.get(wid)
            if entry is not None:
                return entry
            for key, ent in bucket.items():
                if wid.startswith(key) or key.startswith(wid):
                    return ent
            return None

        bucket = self._dest_worker_stats.get(group)
        if isinstance(bucket, dict):
            hit = _from_bucket(bucket)
            if hit is not None:
                return hit

        short = dest_group_short_name(group)
        if short and short != group:
            bucket = self._dest_worker_stats.get(short)
            if isinstance(bucket, dict):
                return _from_bucket(bucket)
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

    def dest_affinity_stats_summary(self) -> dict:
        """Observability snapshot for /workers/affinity."""
        rows = sum(len(v) for v in self._dest_worker_stats.values())
        return {
            "enabled": DEST_AFFINITY_ENABLED,
            "mode": "fastest_avg_mbps",
            "stats_path": str(DEST_AFFINITY_STATS_PATH) if DEST_AFFINITY_STATS_PATH else None,
            "seed_csv": str(DEST_AFFINITY_SEED_CSV) if DEST_AFFINITY_SEED_CSV else None,
            "dest_groups": len(self._dest_worker_stats),
            "worker_dest_rows": rows,
        }

    def clear_dest_worker_stats(self, *, reason: str = "api") -> dict:
        """Drop in-memory dest affinity + delete JSON file; do not re-seed from CSV.

        Restart alone does not clear affinity — averages reload from
        ``ORCH_DEST_AFFINITY_STATS_PATH`` (and optional seed CSV).
        """
        before = self.dest_affinity_stats_summary()
        self._dest_worker_stats.clear()
        self._dest_stats_dirty = False
        self._dest_stats_last_save = 0.0
        deleted = False
        path = DEST_AFFINITY_STATS_PATH
        if path is not None and path.is_file():
            try:
                path.unlink()
                deleted = True
            except Exception as exc:
                logger.warning(
                    "dest affinity clear: failed to delete path=%s: %s", path, exc
                )
        logger.info(
            "dest affinity cleared reason=%s rows_before=%d file_deleted=%s path=%s",
            reason,
            before.get("worker_dest_rows") or 0,
            deleted,
            path or "-",
        )
        after = self.dest_affinity_stats_summary()
        return {
            "ok": True,
            "reason": reason,
            "file_deleted": deleted,
            "before": before,
            "after": after,
        }

    def observe_dest_transfer(
        self,
        dest_group: str,
        worker_id: str,
        transfer_mbps: Optional[float],
    ) -> None:
        """Update running average Mbps for (dest_group, worker)."""
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
            bucket[wid] = {"avg_mbps": mbps, "n": 1, "updated_at": time.time()}
        else:
            prev = float(entry.get("avg_mbps") or entry.get("ema") or mbps)
            n = int(entry.get("n") or 0)
            n_next = n + 1
            entry["avg_mbps"] = ((prev * n) + mbps) / n_next
            entry["n"] = n_next
            entry.pop("ema", None)
            entry.pop("penalty_until", None)
            entry.pop("penalty_mbps", None)
            entry["updated_at"] = time.time()
        self._dest_stats_dirty = True
        self._maybe_save_dest_worker_stats()

    def dest_worker_mbps(self, dest_group: str, worker_id: str) -> float:
        """Average Mbps for worker on dest_group; falls back to global avg."""
        group = str(dest_group or "").strip()
        wid = str(worker_id or "").strip()
        if DEST_AFFINITY_ENABLED and group and wid:
            entry = self._lookup_dest_entry(group, wid)
            if entry is not None:
                try:
                    n = int(entry.get("n") or 0)
                    avg = float(entry.get("avg_mbps") or entry.get("ema") or 0.0)
                except (TypeError, ValueError):
                    n, avg = 0, 0.0
                if n >= DEST_AFFINITY_MIN_SAMPLES and avg > 0:
                    return avg
        return self._get_profile(wid).average_mbps

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
        non_cached_file: Optional[bool] = None,
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
        if non_cached_file is not None:
            profile.non_cached_file = bool(non_cached_file)
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
                "offer queue full (%d); cannot hold %s",
                OVERFLOW_QUEUE_MAX,
                task_key_log_label(offer, offer_id=offer_id),
            )
            return False
        payload = stamp_offer_queued(dict(offer))
        self._overflow_offers.append(payload)
        self._overflow_offer_ids.add(offer_id)
        self._pending_offers[offer_id] = payload
        self._offer_attempted_workers.setdefault(offer_id, set())
        self._remember_offer_context(payload)
        if not quiet:
            logger.info(
                "_workers | queue_enqueue %s pending=%d",
                task_key_log_label(payload),
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
            coverage = offer_coverage_state(offer)
            # Proactive: control-server coverage miss → only miss-capable workers.
            # Reactive reject→reoffer still sets _offer_require_non_cached.
            if coverage == "miss" and offer_id:
                self._offer_require_non_cached.add(offer_id)
            require_nc = bool(
                offer_id and offer_id in self._offer_require_non_cached
            )
            # Hit → use full worker pool (dest Mbps); miss-capable workers often
            # also have local cache and should not sit idle on hits.
            prefer_nc: Optional[bool]
            if require_nc or coverage == "miss":
                prefer_nc = True
            elif coverage == "hit":
                prefer_nc = None
            else:
                # unknown: keep reject→reoffer bias from env
                prefer_nc = True if PREFER_NON_CACHED_WORKERS else False
            if prefer and prefer not in excluded and (
                self._worker_is_free(prefer)
                or (
                    pressure
                    and prefer in self._sessions
                    and self._get_profile(prefer).active_count
                    < OVERFLOW_BUSY_MAX_PER_WORKER
                )
            ):
                if require_nc and not self._get_profile(prefer).non_cached_file:
                    prefer = None
                else:
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
                    prefer_non_cached_capable=prefer_nc,
                    require_non_cached_capable=require_nc,
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
                assign_mode = (
                    "fastest_dest" if DEST_AFFINITY_ENABLED else "round_robin"
                )
                wait_ms = offer_queue_wait_ms(offer)
                logger.info(
                    "_workers | queue_deliver %s worker=%s worker_ip_address=%s "
                    "active=%d pending=%d pressure=%s dest_group=%s dest_mbps=%.1f "
                    "assign=%s coverage=%s queue_wait_ms=%.0f queue_wait_s=%.3f",
                    task_key_log_label(offer),
                    short_id(worker_id),
                    self.worker_ip_address(worker_id),
                    profile.active_count,
                    len(self._overflow_offers),
                    pressure,
                    dest_group or "-",
                    dest_mbps,
                    assign_mode,
                    coverage,
                    wait_ms,
                    wait_ms / 1000.0,
                )
            else:
                # Re-queue and keep draining to other free workers (do not abort).
                if offer_id and offer_id not in self._overflow_offer_ids:
                    self._overflow_offers.appendleft(offer)
                    self._overflow_offer_ids.add(offer_id)
                deliver_failed.add(worker_id)
                logger.warning(
                    "_workers | deliver_fail %s worker=%s worker_ip_address=%s "
                    "requeued; trying other free workers",
                    task_key_log_label(offer),
                    short_id(worker_id),
                    self.worker_ip_address(worker_id),
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
        # Read-only: task_key was stamped at enqueue (no hashing here).
        cached_key = str(
            offer.get("task_key")
            or offer.get("taskKey")
            or offer.get("_orch_task_key")
            or ""
        ).strip()
        if cached_key:
            ctx["task_key"] = cached_key
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
        # Read-only label from msg/ctx — never hash on the result path.
        id_label = task_key_log_label(
            task_key=(msg.get("task_key") if isinstance(msg, dict) else None)
            or ctx.get("task_key"),
            task_id=task_id,
            offer_id=offer_id,
        )
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
                "_workers | task_done %s chunk_id=%s worker=%s worker_ip_address=%s "
                "src=%s dest=%s range=%s hash=%s etag_real=%s "
                "cached=%s path=%s hash_source=%s "
                "load_ms=%.1f hash_ms=%.1f etag_ms=%.1f fetch_ms=%.1f "
                "send_ms=%.1f wall_ms=%.1f",
                id_label,
                chunk_id if chunk_id is not None else "?",
                short_id(worker_id),
                self.worker_ip_address(worker_id),
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
                "_workers | failed %s worker=%s worker_ip_address=%s reason=%s "
                "src=%s dest=%s range=%s hash=%s etag=%s",
                id_label,
                short_id(worker_id),
                self.worker_ip_address(worker_id),
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
        self._offer_require_non_cached.discard(key)

    @staticmethod
    def _is_capacity_reject_error(error: object) -> bool:
        text = str(error or "").strip().lower()
        return text.startswith("queue_full") or text.startswith("memory_budget")

    @staticmethod
    def _is_cache_miss_reject_error(error: object) -> bool:
        text = str(error or "").strip().lower()
        return text == CACHE_MISS_NOT_ACCEPTED or text.startswith(
            CACHE_MISS_NOT_ACCEPTED
        )

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

    async def _maybe_reassign_on_cache_miss_reject(
        self, worker_id: str, msg: dict
    ) -> bool:
        """Re-queue cache-only rejects to workers with non_cached_file=true."""
        if not CACHE_MISS_REOFFER:
            return False
        if bool(msg.get("success")):
            return False
        if not self._is_cache_miss_reject_error(msg.get("error")):
            return False

        task_id = msg.get("task_id")
        offer_id = str(msg.get("offer_id") or task_id or "").strip()
        if not offer_id:
            return False

        offer = self._pending_offers.get(offer_id)
        if not offer:
            logger.warning(
                "cache_miss reject without pending offer: task=%s offer=%s "
                "worker=%s error=%s",
                short_id(task_id),
                short_id(offer_id),
                short_id(worker_id),
                msg.get("error"),
            )
            return False

        attempted = self._offer_attempted_workers.setdefault(offer_id, set())
        attempted.add(str(worker_id))
        self._offer_require_non_cached.add(offer_id)

        capable = {
            wid
            for wid, profile in self._profiles.items()
            if wid in self._sessions and profile.non_cached_file
        }
        remaining = capable - attempted
        if not remaining:
            logger.warning(
                "cache_miss: no non_cached_file workers left task=%s offer=%s "
                "attempted=%d capable=%d — relay failure",
                short_id(task_id),
                short_id(offer_id),
                len(attempted),
                len(capable),
            )
            return False

        self.mark_worker_idle(worker_id, offer_id, drain_overflow=False)
        if self._overflow_prefer_worker == worker_id:
            self._overflow_prefer_worker = None

        if self._enqueue_overflow(offer):
            logger.info(
                "cache_miss: re-queued task=%s offer=%s from=%s "
                "attempted=%d pending=%d prefer_non_cached=true remaining=%d",
                short_id(task_id),
                short_id(offer_id),
                short_id(worker_id),
                len(attempted),
                len(self._overflow_offers),
                len(remaining),
            )
            await self._send_to_worker(
                worker_id,
                {
                    "type": "task_result_ack",
                    "task_id": task_id,
                    "offer_id": offer_id,
                    "received": True,
                    "status": "late_superseded",
                    "reason": f"queued_after_cache_miss:{msg.get('error')}",
                },
            )
            await self._drain_overflow_queue()
            return True

        logger.warning(
            "cache_miss: queue full task=%s offer=%s from=%s attempted=%d",
            short_id(task_id),
            short_id(offer_id),
            short_id(worker_id),
            len(attempted),
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
            # Fold slow sample into avg so this worker ranks lower next time.
            sample = transfer_mbps_from_result(msg)
            if sample is not None:
                self.observe_dest_transfer(dest_group, worker_id, sample)
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
            # Never send orch-private bookkeeping fields to the worker.
            wire = {
                k: v
                for k, v in offer.items()
                if not (isinstance(k, str) and k.startswith("_orch_"))
            }
            payload = {"type": "task_offer", **wire}
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
                "_workers | task_start %s worker=%s worker_ip_address=%s "
                "chunk_id=%s range=%s",
                task_key_log_label(offer, task_id=task_id, offer_id=offer_id),
                short_id(worker_id),
                self.worker_ip_address(worker_id),
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
        prefer_non_cached_capable: Optional[bool] = None,
        require_non_cached_capable: bool = False,
    ) -> Optional[str]:
        """Pick a worker with capacity.

        When ``ORCH_DEST_AFFINITY`` is on (default) and ``dest_group`` is set:
          deliver to the **fastest free worker** for that dest (highest avg Mbps).
          Idle workers beat busy ones; among equals, prefer unused IP then RR.

        When affinity is off: free-worker round-robin (optional non_cached bias).

        ``require_non_cached_capable``: only workers with non_cached_file=true.
        ``assign_cap``: pressure mode — allow load < cap instead of hello max.
        """
        connected = self._ordered_connected_worker_ids()
        if not connected:
            return None

        pool_size = len(connected)
        start = self._cursor % pool_size
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
            return max(profile.active_count, _batch_count(worker_id))

        def _has_assign_capacity(worker_id: str) -> bool:
            load = _load(worker_id)
            if cap is not None:
                return load < cap
            return self._get_profile(worker_id).has_capacity

        def _score_mbps(worker_id: str) -> float:
            if not DEST_AFFINITY_ENABLED:
                return 0.0
            if dest_g:
                return self.dest_worker_mbps(dest_g, worker_id)
            return self._get_profile(worker_id).average_mbps

        def _non_cached_rank(worker_id: str) -> int:
            if prefer_non_cached_capable is None:
                return 0
            capable = self._get_profile(worker_id).non_cached_file
            if prefer_non_cached_capable:
                return 0 if capable else 1
            return 0 if not capable else 1

        def _eligible(
            worker_id: str,
            *,
            allow_ip_reuse: bool,
            require_idle: bool,
            allow_worker_reuse: bool,
        ) -> bool:
            if worker_id in excluded:
                return False
            if require_non_cached_capable and not self._get_profile(
                worker_id
            ).non_cached_file:
                return False
            if not _has_assign_capacity(worker_id):
                return False
            load = _load(worker_id)
            if require_idle and load > 0:
                return False
            if not allow_worker_reuse and _batch_count(worker_id) > 0:
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
        ) -> Optional[str]:
            # Affinity on: highest dest avg Mbps wins among eligible.
            # Affinity off: non_cached → load → RR offset.
            candidates: list[tuple] = []
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
                mbps = _score_mbps(worker_id)
                candidates.append(
                    (
                        _non_cached_rank(worker_id),
                        _load(worker_id),
                        -mbps,
                        _batch_count(worker_id),
                        offset,
                        worker_id,
                        mbps,
                    )
                )
            if not candidates:
                return None
            if DEST_AFFINITY_ENABLED:
                # non_cached → idle first → fastest dest avg → RR
                candidates.sort(
                    key=lambda item: (item[0], item[1], item[2], item[3], item[4])
                )
            else:
                candidates.sort(key=lambda item: (item[0], item[1], item[3], item[4]))
            (
                _nc_rank,
                _load_n,
                _neg_mbps,
                _bc,
                offset,
                worker_id,
                dest_mbps,
            ) = candidates[0]
            idx = (start + offset) % pool_size
            self._cursor = (idx + 1) % pool_size
            profile = self._get_profile(worker_id)
            logger.debug(
                "selected worker %s ip=%s active=%d/%d load=%d "
                "dest_group=%s dest_mbps=%.1f assign=%s "
                "require_idle=%s non_cached=%s",
                worker_id,
                profile.ip or "?",
                profile.active_count,
                profile.max_concurrent_tasks,
                _load_n,
                dest_g or "-",
                dest_mbps,
                "fastest_dest" if DEST_AFFINITY_ENABLED else "round_robin",
                require_idle,
                profile.non_cached_file,
            )
            return worker_id

        def _pick_idle(*, allow_ip_reuse: bool) -> Optional[str]:
            return _pick(
                allow_ip_reuse=allow_ip_reuse,
                require_idle=True,
                allow_worker_reuse=False,
            )

        def _pick_busy(*, allow_ip_reuse: bool) -> Optional[str]:
            return _pick(
                allow_ip_reuse=allow_ip_reuse,
                require_idle=False,
                allow_worker_reuse=True,
            )

        if prefer_idle:
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
        missing_task_key_sample: Optional[dict] = None

        for offer in offers:
            if not isinstance(offer, dict):
                failed += 1
                continue
            # Cheap pre-stamp presence check (no hashing); stamp happens in enqueue.
            if missing_task_key_sample is None and not (
                offer.get("task_key")
                or offer.get("taskKey")
                or offer.get("idempotency_key")
                or offer.get("idempotencyKey")
            ):
                missing_task_key_sample = offer
            # Per-task: queue_enqueue → (later) queue_deliver + queue_wait_* → task_done
            if self._enqueue_overflow(offer):
                queued += 1
            else:
                failed += 1
                logger.warning(
                    "Offer queue rejected %s (queue full or disabled)",
                    task_key_log_label(offer),
                )

        if missing_task_key_sample is not None and not getattr(
            self, "_warned_missing_task_key", False
        ):
            self._warned_missing_task_key = True
            logger.warning(
                "BeamCore offers missing task_key (dashboard correlation). "
                "offer_keys=%s",
                sorted(str(k) for k in missing_task_key_sample.keys()),
            )

        delivered = await self._drain_overflow_queue()
        still_pending = len(self._overflow_offers)
        oldest_wait_ms = 0.0
        if still_pending and self._overflow_offers:
            oldest_wait_ms = max(
                offer_queue_wait_ms(o) for o in self._overflow_offers
            )

        logger.info(
            "_workers | batch queued=%s delivered=%s pending=%s failed=%s "
            "oldest_queue_wait_ms=%.0f oldest_queue_wait_s=%.3f "
            "(queue-first dispatch)",
            queued,
            delivered,
            still_pending,
            failed,
            oldest_wait_ms,
            oldest_wait_ms / 1000.0,
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
            non_cached_raw = msg.get("non_cached_file")
            non_cached: Optional[bool] = None
            if isinstance(non_cached_raw, bool):
                non_cached = non_cached_raw
            elif non_cached_raw is not None:
                non_cached = str(non_cached_raw).strip().lower() in {
                    "1",
                    "true",
                    "yes",
                    "on",
                }
            self.update_worker_hello(
                worker_id,
                ip=ip or None,
                max_concurrent_tasks=max_concurrent,
                worker_version=worker_version or None,
                initial_order=initial_order,
                claimed_bandwidth_mbps=claimed,
                non_cached_file=non_cached,
            )
            profile = self._get_profile(worker_id)
            logger.info(
                "Worker hello: %s version=%s ip=%s max_tasks=%d active=%d "
                "initial_order=%d non_cached_file=%s",
                worker_id,
                profile.worker_version or "?",
                profile.ip or "?",
                profile.max_concurrent_tasks,
                profile.active_count,
                profile.initial_order,
                profile.non_cached_file,
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
            if await self._maybe_reassign_on_cache_miss_reject(worker_id, msg):
                # Offer re-queued to a non_cached_file=true worker.
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

