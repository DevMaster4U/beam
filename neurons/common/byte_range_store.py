"""Continuous byte-range store for Beam chunk cache.

Layout under ``root``::

    <sha256(source_url)[:32]>/
      segments.json
      <start>_<end>.bin

Task flow:
  - If any segment fully covers [start, end] → seek+read slice (no fetch).
  - Else store a new segment file and update segments.json (no auto-merge).

Merging adjacent/overlapping segments is a separate manual step
(``scripts/merge.py`` / ``ByteRangeStore.merge_source``).
"""

from __future__ import annotations

import hashlib
import json
import os
import tempfile
import threading
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable, Iterator, Optional


COPY_CHUNK = 1024 * 1024


@dataclass(frozen=True)
class Segment:
    start: int
    end: int
    file: str

    @property
    def size(self) -> int:
        return self.end - self.start + 1

    def covers(self, start: int, end: int) -> bool:
        return self.start <= start and end <= self.end

    def overlaps_or_adjacent(self, start: int, end: int) -> bool:
        return start <= self.end + 1 and end + 1 >= self.start

    def to_dict(self) -> dict[str, Any]:
        return {"start": self.start, "end": self.end, "file": self.file}


def merge_intervals(ranges: Iterable[tuple[int, int]]) -> list[tuple[int, int]]:
    """Merge overlapping or adjacent inclusive [start, end] ranges."""
    items = sorted((int(s), int(e)) for s, e in ranges if e >= s)
    if not items:
        return []
    out: list[list[int]] = [list(items[0])]
    for start, end in items[1:]:
        if start <= out[-1][1] + 1:
            out[-1][1] = max(out[-1][1], end)
        else:
            out.append([start, end])
    return [(a, b) for a, b in out]


def source_digest(source_url: str) -> str:
    return hashlib.sha256(str(source_url).encode("utf-8")).hexdigest()[:32]


def _segment_filename(start: int, end: int) -> str:
    return f"{start}_{end}.bin"


class ByteRangeStore:
    """Per-source continuous byte-range segment store."""

    def __init__(self, root: Path) -> None:
        self.root = Path(root)
        self.root.mkdir(parents=True, exist_ok=True)
        self._locks: dict[str, threading.Lock] = {}
        self._locks_guard = threading.Lock()

    def _source_lock(self, digest: str) -> threading.Lock:
        with self._locks_guard:
            lock = self._locks.get(digest)
            if lock is None:
                lock = threading.Lock()
                self._locks[digest] = lock
            return lock

    def source_dir(self, source_url: str) -> Path:
        return self.root / source_digest(source_url)

    def _index_path(self, source_url: str) -> Path:
        return self.source_dir(source_url) / "segments.json"

    def _load_segments(self, source_url: str) -> list[Segment]:
        path = self._index_path(source_url)
        if not path.is_file():
            return []
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            return []
        items = data.get("segments") if isinstance(data, dict) else data
        if not isinstance(items, list):
            return []
        segments: list[Segment] = []
        for item in items:
            if not isinstance(item, dict):
                continue
            try:
                start = int(item["start"])
                end = int(item["end"])
                file_name = str(item.get("file") or _segment_filename(start, end))
            except (KeyError, TypeError, ValueError):
                continue
            if end < start:
                continue
            segments.append(Segment(start=start, end=end, file=file_name))
        return sorted(segments, key=lambda s: s.start)

    def _save_segments(self, source_url: str, segments: list[Segment]) -> None:
        directory = self.source_dir(source_url)
        directory.mkdir(parents=True, exist_ok=True)
        payload = {
            "source_url": source_url,
            "segments": [s.to_dict() for s in sorted(segments, key=lambda x: x.start)],
        }
        path = self._index_path(source_url)
        tmp = path.with_suffix(".tmp")
        tmp.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
        os.replace(tmp, path)

    def list_segments(self, source_url: str) -> list[Segment]:
        digest = source_digest(source_url)
        with self._source_lock(digest):
            return self._load_segments(source_url)

    def list_sources(self) -> list[str]:
        """Return source_url values discovered from segments.json under root."""
        sources: list[str] = []
        if not self.root.is_dir():
            return sources
        for child in sorted(self.root.iterdir()):
            if not child.is_dir():
                continue
            index = child / "segments.json"
            if not index.is_file():
                continue
            try:
                data = json.loads(index.read_text(encoding="utf-8"))
            except (OSError, json.JSONDecodeError):
                continue
            src = str(data.get("source_url") or "").strip()
            if src:
                sources.append(src)
        return sources

    def covers(self, source_url: str, start: int, end: int) -> bool:
        if end < start:
            return False
        return self.find_covering_segment(source_url, start, end) is not None

    def find_covering_segment(
        self, source_url: str, start: int, end: int
    ) -> Optional[Segment]:
        if end < start:
            return None
        for seg in self.list_segments(source_url):
            if seg.covers(start, end):
                return seg
        return None

    def segment_path(self, source_url: str, segment: Segment) -> Path:
        return self.source_dir(source_url) / segment.file

    def read_slice(self, source_url: str, start: int, end: int) -> Optional[bytes]:
        """Return exact [start, end] bytes if a segment fully covers the range."""
        segment = self.find_covering_segment(source_url, start, end)
        if segment is None:
            return None
        path = self.segment_path(source_url, segment)
        if not path.is_file():
            return None
        length = end - start + 1
        offset = start - segment.start
        with path.open("rb") as handle:
            handle.seek(offset)
            data = handle.read(length)
        if len(data) != length:
            return None
        return data

    def iter_slice(
        self,
        source_url: str,
        start: int,
        end: int,
        *,
        chunk_size: int = COPY_CHUNK,
    ) -> Optional[Iterator[bytes]]:
        """Yield [start, end] in chunks without loading the whole segment."""
        segment = self.find_covering_segment(source_url, start, end)
        if segment is None:
            return None
        path = self.segment_path(source_url, segment)
        if not path.is_file():
            return None
        length = end - start + 1
        offset = start - segment.start

        def _gen() -> Iterator[bytes]:
            remaining = length
            with path.open("rb") as handle:
                handle.seek(offset)
                while remaining > 0:
                    part = handle.read(min(chunk_size, remaining))
                    if not part:
                        break
                    remaining -= len(part)
                    yield part

        return _gen()

    def ingest(
        self,
        source_url: str,
        start: int,
        end: int,
        data: bytes,
        *,
        merge: bool = False,
    ) -> Segment:
        """Store [start, end] as a segment file and update segments.json.

        Default ``merge=False``: no auto-merge (run ``merge_source`` / merge.py later).
        If an existing segment already fully covers the range, skip write and return it.
        If an exact start/end segment exists, overwrite its file.
        """
        if end < start:
            raise ValueError("invalid range: end < start")
        expected = end - start + 1
        if len(data) != expected:
            raise ValueError(f"data length {len(data)} != range size {expected}")

        digest = source_digest(source_url)
        with self._source_lock(digest):
            if merge:
                return self._ingest_merge_locked(source_url, start, end, data)
            return self._ingest_store_locked(source_url, start, end, data)

    def _ingest_store_locked(
        self,
        source_url: str,
        start: int,
        end: int,
        data: bytes,
    ) -> Segment:
        directory = self.source_dir(source_url)
        directory.mkdir(parents=True, exist_ok=True)
        segments = self._load_segments(source_url)

        for seg in segments:
            if seg.covers(start, end):
                return seg

        file_name = _segment_filename(start, end)
        path = directory / file_name
        path.write_bytes(data)
        new_seg = Segment(start=start, end=end, file=file_name)

        kept = [s for s in segments if not (s.start == start and s.end == end)]
        kept.append(new_seg)
        self._save_segments(source_url, kept)
        return new_seg

    def _ingest_merge_locked(
        self,
        source_url: str,
        start: int,
        end: int,
        data: bytes,
    ) -> Segment:
        """Store and immediately merge with adjacent/overlapping segments."""
        directory = self.source_dir(source_url)
        directory.mkdir(parents=True, exist_ok=True)
        segments = self._load_segments(source_url)

        touching = [s for s in segments if s.overlaps_or_adjacent(start, end)]
        if not touching:
            return self._ingest_store_locked(source_url, start, end, data)

        file_name = _segment_filename(start, end)
        path = directory / file_name
        path.write_bytes(data)
        new_seg = Segment(start=start, end=end, file=file_name)
        kept = [s for s in segments if s not in touching]
        group = [s for s in touching if not (s.start == start and s.end == end)]
        group.append(new_seg)
        merged = self._merge_group_locked(source_url, directory, group)
        kept.append(merged)
        self._save_segments(source_url, kept)
        return merged

    def _merge_group_locked(
        self,
        source_url: str,
        directory: Path,
        group: list[Segment],
    ) -> Segment:
        """Merge one contiguous overlapping/adjacent group into a single file."""
        group = sorted(group, key=lambda s: s.start)
        merged_start = min(s.start for s in group)
        merged_end = max(s.end for s in group)
        file_name = _segment_filename(merged_start, merged_end)
        out_path = directory / file_name

        pieces: list[tuple[int, int, Path]] = []
        for seg in group:
            seg_path = directory / seg.file
            if seg_path.is_file():
                pieces.append((seg.start, seg.end, seg_path))

        resolved: list[tuple[int, int, Path, int]] = []
        for p_start, p_end, p_path in pieces:
            next_resolved: list[tuple[int, int, Path, int]] = []
            for r_start, r_end, r_path, r_file_start in resolved:
                if p_end < r_start or p_start > r_end:
                    next_resolved.append((r_start, r_end, r_path, r_file_start))
                    continue
                if r_start < p_start:
                    next_resolved.append((r_start, p_start - 1, r_path, r_file_start))
                if r_end > p_end:
                    next_resolved.append((p_end + 1, r_end, r_path, r_file_start))
            try:
                file_start = int(p_path.stem.split("_", 1)[0])
            except ValueError:
                file_start = p_start
            next_resolved.append((p_start, p_end, p_path, file_start))
            resolved = sorted(next_resolved, key=lambda x: x[0])

        fd, tmp_name = tempfile.mkstemp(prefix=".range_", suffix=".bin", dir=directory)
        os.close(fd)
        tmp_path = Path(tmp_name)
        try:
            with tmp_path.open("wb") as out:
                cursor = merged_start
                for r_start, r_end, r_path, file_start in resolved:
                    if r_start != cursor:
                        raise RuntimeError(
                            f"non-contiguous merge at {cursor}..{r_start - 1} "
                            f"for {source_url}"
                        )
                    length = r_end - r_start + 1
                    with r_path.open("rb") as src:
                        src.seek(r_start - file_start)
                        remaining = length
                        while remaining > 0:
                            part = src.read(min(COPY_CHUNK, remaining))
                            if not part:
                                break
                            out.write(part)
                            remaining -= len(part)
                    cursor = r_end + 1
                if cursor != merged_end + 1:
                    raise RuntimeError(
                        f"merge incomplete: wrote through {cursor - 1}, expected {merged_end}"
                    )
            os.replace(tmp_path, out_path)
        finally:
            if tmp_path.exists():
                tmp_path.unlink(missing_ok=True)

        for seg in group:
            old = directory / seg.file
            if old != out_path and old.is_file():
                old.unlink(missing_ok=True)

        return Segment(start=merged_start, end=merged_end, file=file_name)

    def merge_source(self, source_url: str) -> dict[str, Any]:
        """Manually compact adjacent/overlapping segments for one source."""
        digest = source_digest(source_url)
        with self._source_lock(digest):
            directory = self.source_dir(source_url)
            segments = self._load_segments(source_url)
            before = len(segments)
            if before <= 1:
                return {
                    "source_url": source_url,
                    "before": before,
                    "after": before,
                    "merged_groups": 0,
                }

            groups: list[list[Segment]] = []
            for seg in sorted(segments, key=lambda s: s.start):
                if not groups:
                    groups.append([seg])
                    continue
                cur = groups[-1]
                lo = min(s.start for s in cur)
                hi = max(s.end for s in cur)
                if seg.overlaps_or_adjacent(lo, hi):
                    cur.append(seg)
                else:
                    groups.append([seg])

            merged_groups = 0
            new_segments: list[Segment] = []
            for group in groups:
                if len(group) == 1:
                    new_segments.append(group[0])
                    continue
                new_segments.append(self._merge_group_locked(source_url, directory, group))
                merged_groups += 1

            self._save_segments(source_url, new_segments)
            return {
                "source_url": source_url,
                "before": before,
                "after": len(new_segments),
                "merged_groups": merged_groups,
                "segments": [s.to_dict() for s in new_segments],
            }

    def merge_all(self) -> list[dict[str, Any]]:
        """Run merge_source for every source under this store root."""
        return [self.merge_source(src) for src in self.list_sources()]

    def ingest_from_file(
        self,
        source_url: str,
        start: int,
        end: int,
        path: Path,
        *,
        merge: bool = False,
    ) -> Segment:
        """Ingest range bytes from an existing file."""
        data = path.read_bytes()
        return self.ingest(source_url, start, end, data, merge=merge)

    def coverage_report(
        self,
        source_url: str,
        *,
        span_start: Optional[int] = None,
        span_end: Optional[int] = None,
    ) -> dict[str, Any]:
        segments = self.list_segments(source_url)
        covered = sum(s.size for s in segments)
        gaps: list[dict[str, int]] = []
        if span_start is not None and span_end is not None and span_end >= span_start:
            cursor = span_start
            for seg in segments:
                if seg.end < span_start or seg.start > span_end:
                    continue
                if seg.start > cursor:
                    gap_end = min(seg.start - 1, span_end)
                    if gap_end >= cursor:
                        gaps.append(
                            {
                                "start": cursor,
                                "end": gap_end,
                                "size": gap_end - cursor + 1,
                            }
                        )
                cursor = max(cursor, seg.end + 1)
            if cursor <= span_end:
                gaps.append(
                    {
                        "start": cursor,
                        "end": span_end,
                        "size": span_end - cursor + 1,
                    }
                )
        return {
            "source_url": source_url,
            "segment_count": len(segments),
            "covered_bytes": covered,
            "segments": [s.to_dict() for s in segments],
            "gaps": gaps,
            "gap_bytes": sum(g["size"] for g in gaps),
        }


def parse_cache_key_range(key: str) -> Optional[tuple[str, int, int]]:
    """Parse ``source|start|end`` cache key."""
    parts = str(key or "").rsplit("|", 2)
    if len(parts) != 3:
        return None
    try:
        start = int(parts[1])
        end = int(parts[2])
    except ValueError:
        return None
    source = parts[0].strip()
    if not source or end < start:
        return None
    return source, start, end
