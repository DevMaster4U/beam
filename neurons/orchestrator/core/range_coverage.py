"""In-memory control-server range coverage for orch routing (segments only).

Orch listens to control-server WS ``range_snapshot`` / ``range_broadcast`` and
keeps source→[start,end] coverage. It does **not** download range bytes.

Used to proactively send cache-miss offers to ``non_cached_file=true`` workers
instead of waiting for reject→reoffer.
"""

from __future__ import annotations

import logging
import os
import threading
from typing import Any, Optional

from neurons.common.byte_range_store import (
    merge_intervals,
    normalize_source_url,
    source_object_name,
)

logger = logging.getLogger(__name__)


def _env_bool(name: str, default: bool) -> bool:
    raw = os.environ.get(name)
    if raw is None or not str(raw).strip():
        return default
    return str(raw).strip().lower() in ("1", "true", "yes")


# When true, miss coverage → require non_cached workers; hit → prefer cache-only.
COVERAGE_ROUTING = _env_bool("ORCH_COVERAGE_ROUTING", True)
# Optional: also pull range bytes into orch (embedded workers). Default off.
ORCH_DOWNLOAD_RANGE_DATA = _env_bool("ORCH_DOWNLOAD_RANGE_DATA", False)


def _coverage_key(source_url: str) -> str:
    """Index by object filename so eu/apac/xfer aliases share coverage."""
    name = source_object_name(source_url)
    if name:
        return name
    return normalize_source_url(source_url)


class RangeCoverageIndex:
    """Thread-safe coverage map: object filename → merged [start, end] segments."""

    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._segments: dict[str, list[tuple[int, int]]] = {}
        self._ready = False
        self._source_count = 0
        self._segment_count = 0

    @property
    def ready(self) -> bool:
        with self._lock:
            return self._ready

    @property
    def source_count(self) -> int:
        with self._lock:
            return self._source_count

    def apply_snapshot(self, sources: list[dict[str, Any]]) -> None:
        """Replace index from control-server range_snapshot (metadata only)."""
        next_map: dict[str, list[tuple[int, int]]] = {}
        seg_total = 0
        for item in sources or []:
            if not isinstance(item, dict):
                continue
            source_url = str(item.get("source_url") or "")
            key = _coverage_key(source_url) or source_object_name(
                str(item.get("object_name") or "")
            )
            if not key:
                continue
            ranges: list[tuple[int, int]] = []
            for seg in item.get("segments") or []:
                if not isinstance(seg, dict):
                    continue
                try:
                    start = int(seg["start"])
                    end = int(seg["end"])
                except (KeyError, TypeError, ValueError):
                    continue
                if end >= start:
                    ranges.append((start, end))
            merged = merge_intervals(ranges)
            # Same object name from multiple URLs → union coverage.
            if key in next_map:
                merged = merge_intervals(next_map[key] + merged)
            next_map[key] = merged
            seg_total += len(merged)
        with self._lock:
            self._segments = next_map
            self._ready = True
            self._source_count = len(next_map)
            self._segment_count = sum(len(v) for v in next_map.values())
        logger.info(
            "Orch range coverage snapshot sources=%d segments=%d routing=%s",
            len(next_map),
            self._segment_count,
            COVERAGE_ROUTING,
        )

    def add_range(self, source_url: str, start: int, end: int) -> None:
        """Merge a range_broadcast into the index."""
        key = _coverage_key(source_url)
        if not key or end < start:
            return
        with self._lock:
            existing = list(self._segments.get(key) or [])
            existing.append((int(start), int(end)))
            merged = merge_intervals(existing)
            self._segments[key] = merged
            self._ready = True
            self._source_count = len(self._segments)
            self._segment_count = sum(len(v) for v in self._segments.values())

    def covers(self, source_url: str, start: int, end: int) -> bool:
        """True if merged segments fully cover inclusive [start, end]."""
        key = _coverage_key(source_url)
        if not key or end < start:
            return False
        with self._lock:
            segments = self._segments.get(key) or []
        cursor = int(start)
        target = int(end)
        for seg_start, seg_end in segments:
            if seg_end < cursor:
                continue
            if seg_start > cursor:
                return False
            cursor = seg_end + 1
            if cursor > target:
                return True
        return False

    def has_source(self, source_url: str) -> bool:
        key = _coverage_key(source_url)
        if not key:
            return False
        with self._lock:
            return key in self._segments


coverage_index = RangeCoverageIndex()


def offer_source_range(offer: dict) -> Optional[tuple[str, int, int]]:
    """Return (normalized source_url, range_start, range_end) from an offer."""
    if not isinstance(offer, dict):
        return None
    source_url = normalize_source_url(str(offer.get("source_url") or ""))
    if not source_url:
        return None
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
                    return None
    try:
        start_i = int(start)
        end_i = int(end)
    except (TypeError, ValueError):
        return None
    if end_i < start_i:
        return None
    return source_url, start_i, end_i


def offer_coverage_state(offer: dict) -> str:
    """Return ``hit``, ``miss``, or ``unknown`` for routing.

    ``unknown`` when coverage routing is off or snapshot not yet received.
    """
    if not COVERAGE_ROUTING or not coverage_index.ready:
        return "unknown"
    parsed = offer_source_range(offer)
    if parsed is None:
        return "unknown"
    source_url, start, end = parsed
    if coverage_index.covers(source_url, start, end):
        return "hit"
    return "miss"


def setup_orch_range_coverage_sync() -> None:
    """Subscribe to control-server coverage (no byte download by default)."""
    from neurons.common import control_ws_client

    control_ws_client.register_range_snapshot_handler(coverage_index.apply_snapshot)
    control_ws_client.register_range_broadcast_handler(coverage_index.add_range)
    logger.info(
        "Orch range coverage sync registered (metadata only) "
        "routing=%s download_bytes=%s",
        COVERAGE_ROUTING,
        ORCH_DOWNLOAD_RANGE_DATA,
    )
