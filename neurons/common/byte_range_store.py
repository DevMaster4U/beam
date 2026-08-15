"""Continuous byte-range store for Beam chunk cache.

Layout under ``root``::

    <object_filename.bin>/
      segments.json
      <start>_<end>.bin

The store key is the object **filename** (final path segment), not a URL hash
and not the full URL. Same object under different buckets/prefixes
(e.g. ``source-eu/foo.bin`` vs ``source-apac/foo.bin``) share one directory.

Task flow:
  - If contiguous segments cover [start, end] → seek+read slice (upload from cache).
  - Else download/upload, then ingest: store + merge touching segments, packed into
    files of at most ``MAX_SEGMENT_BYTES`` (default 1 GiB), aligned to absolute
    1 GiB boundaries.

Example packing (1 GiB = 2**30)::

    coverage [500 MiB, 1.5 GiB] →
      500MiB_(1GiB-1).bin
      1GiB_(1.5GiB).bin
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
from urllib.parse import urlsplit, urlunsplit


COPY_CHUNK = 1024 * 1024
# Default max on-disk segment size: 1 GiB. Override with RANGE_DATA_MAX_SEGMENT_BYTES.
MAX_SEGMENT_BYTES = max(
    1,
    int(os.environ.get("RANGE_DATA_MAX_SEGMENT_BYTES", str(1 << 30))),
)


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


def normalize_source_url(source_url: str) -> str:
    """Canonical object URL: scheme/host/path only (no query/fragment).

    Presigned R2/S3 URLs change signature query params every request; strip those
    so metadata and lookups refer to a stable object URL. Digests use
    ``source_object_name`` (filename only), not this full URL.
    """
    raw = str(source_url or "").strip()
    if not raw:
        return ""
    parts = urlsplit(raw)
    if not parts.scheme or not parts.netloc:
        # Already path-like / non-URL — keep as-is minus trailing slash.
        return raw.rstrip("/")
    return urlunsplit((parts.scheme, parts.netloc, parts.path, "", "")).rstrip("/")


def source_object_name(source_url: str) -> str:
    """Object filename used as the range_data cache key (final path segment)."""
    canonical = normalize_source_url(source_url)
    if not canonical:
        return ""
    parts = urlsplit(canonical)
    path = parts.path if parts.scheme and parts.netloc else canonical
    name = path.rstrip("/").rsplit("/", 1)[-1].strip()
    return name


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


def shard_bounds(
    start: int,
    end: int,
    *,
    max_bytes: int = MAX_SEGMENT_BYTES,
) -> list[tuple[int, int]]:
    """Split inclusive [start, end] into shards of at most ``max_bytes``.

    Shards align to absolute ``max_bytes`` boundaries so files never cross
    e.g. the 1 GiB, 2 GiB, … offsets.
    """
    if end < start:
        return []
    if max_bytes < 1:
        raise ValueError("max_bytes must be >= 1")
    shards: list[tuple[int, int]] = []
    cursor = start
    while cursor <= end:
        # Last byte of the absolute shard that contains ``cursor``.
        limit = ((cursor // max_bytes) + 1) * max_bytes - 1
        shard_end = min(end, limit)
        shards.append((cursor, shard_end))
        cursor = shard_end + 1
    return shards


def source_cache_dir_name(source_url: str) -> str:
    """range_data subdirectory name: object ``.bin`` filename (not a hash).

    Falls back to sha256 of the normalized URL when the path has no filename.
    """
    name = source_object_name(source_url)
    if name:
        # Keep a single path segment even if a caller passes a weird value.
        return name.replace("\\", "_").replace("/", "_")
    key = normalize_source_url(source_url)
    if not key:
        return ""
    return hashlib.sha256(key.encode("utf-8")).hexdigest()[:32]


def source_digest(source_url: str) -> str:
    """Directory key for range_data (object filename). Alias of ``source_cache_dir_name``."""
    return source_cache_dir_name(source_url)


def _segment_filename(start: int, end: int) -> str:
    return f"{start}_{end}.bin"


class ByteRangeStore:
    """Per-source continuous byte-range segment store (max 1 GiB files)."""

    def __init__(
        self,
        root: Path,
        *,
        max_segment_bytes: int = MAX_SEGMENT_BYTES,
    ) -> None:
        self.root = Path(root)
        self.root.mkdir(parents=True, exist_ok=True)
        self.max_segment_bytes = max(1, int(max_segment_bytes))
        self._locks: dict[str, threading.Lock] = {}
        self._locks_guard = threading.Lock()
        self._consolidated = False

    def _source_lock(self, digest: str) -> threading.Lock:
        with self._locks_guard:
            lock = self._locks.get(digest)
            if lock is None:
                lock = threading.Lock()
                self._locks[digest] = lock
            return lock

    def source_dir(self, source_url: str) -> Path:
        name = source_cache_dir_name(source_url)
        if not name:
            raise ValueError("empty cache dir name; refusing to write at store root")
        return self.root / name

    def _ensure_consolidated(self) -> None:
        """Merge legacy hash-named dirs into object-filename dirs (once per store)."""
        if self._consolidated:
            return
        with self._locks_guard:
            if self._consolidated:
                return
            # Mark first, then release locks_guard before long I/O so ingest
            # can take per-source locks without deadlocking.
            self._consolidated = True
        self._migrate_root_level_segments()
        self.consolidate_signed_url_orphans()

    def _migrate_root_level_segments(self) -> None:
        """Move legacy segments.json + *.bin written at store root into object dirs."""
        index = self.root / "segments.json"
        if not index.is_file():
            return
        try:
            data = json.loads(index.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            return
        raw_src = str(data.get("source_url") or "").strip()
        canonical = normalize_source_url(raw_src)
        if not canonical:
            return
        items = data.get("segments") if isinstance(data, dict) else []
        if not isinstance(items, list):
            items = []
        for item in items:
            if not isinstance(item, dict):
                continue
            try:
                start = int(item["start"])
                end = int(item["end"])
                file_name = str(item.get("file") or _segment_filename(start, end))
            except (KeyError, TypeError, ValueError):
                continue
            src_path = self.root / file_name
            if not src_path.is_file():
                continue
            if self._covers_canonical(canonical, start, end):
                try:
                    src_path.unlink()
                except OSError:
                    pass
                continue
            try:
                self.ingest_from_file(canonical, start, end, src_path, merge=True)
            except Exception:
                continue
            try:
                src_path.unlink()
            except OSError:
                pass
        try:
            index.unlink()
        except OSError:
            pass

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
        source_url = normalize_source_url(source_url)
        directory = self.source_dir(source_url)
        directory.mkdir(parents=True, exist_ok=True)
        payload = {
            "source_url": source_url,
            "object_name": source_object_name(source_url),
            "max_segment_bytes": self.max_segment_bytes,
            "segments": [s.to_dict() for s in sorted(segments, key=lambda x: x.start)],
        }
        path = self._index_path(source_url)
        tmp = path.with_suffix(".tmp")
        tmp.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
        os.replace(tmp, path)

    def list_segments(self, source_url: str) -> list[Segment]:
        self._ensure_consolidated()
        source_url = normalize_source_url(source_url)
        digest = source_digest(source_url)
        with self._source_lock(digest):
            return self._load_segments(source_url)

    def list_sources(self) -> list[str]:
        """Return canonical source_url values discovered from segments.json under root."""
        self._ensure_consolidated()
        sources: list[str] = []
        seen: set[str] = set()
        seen_objects: set[str] = set()
        if not self.root.is_dir():
            return sources
        for child in sorted(self.root.iterdir()):
            if not child.is_dir() or child.name.startswith("."):
                continue
            index = child / "segments.json"
            if not index.is_file():
                continue
            try:
                data = json.loads(index.read_text(encoding="utf-8"))
            except (OSError, json.JSONDecodeError):
                continue
            src = normalize_source_url(str(data.get("source_url") or "").strip())
            if not src:
                continue
            obj = source_object_name(src)
            # Prefer directories whose name matches the canonical filename digest.
            if child.name != source_digest(src):
                continue
            if src in seen or (obj and obj in seen_objects):
                continue
            seen.add(src)
            if obj:
                seen_objects.add(obj)
            sources.append(src)
        return sources

    def _covers_canonical(self, source_url: str, start: int, end: int) -> bool:
        return bool(self.find_covering_segments(source_url, start, end))

    def covers(self, source_url: str, start: int, end: int) -> bool:
        if end < start:
            return False
        self._ensure_consolidated()
        return self._covers_canonical(source_url, start, end)

    def find_covering_segment(
        self, source_url: str, start: int, end: int
    ) -> Optional[Segment]:
        """Return the first segment of a covering contiguous set, if any."""
        covering = self.find_covering_segments(source_url, start, end)
        return covering[0] if covering else None

    def find_covering_segments(
        self, source_url: str, start: int, end: int
    ) -> list[Segment]:
        """Return contiguous segments that together fully cover [start, end]."""
        if end < start:
            return []
        segments = self.list_segments(source_url)
        if not segments:
            return []
        covering: list[Segment] = []
        cursor = start
        for seg in segments:
            if seg.end < cursor:
                continue
            if seg.start > cursor:
                return []
            covering.append(seg)
            cursor = seg.end + 1
            if cursor > end:
                return covering
        return []

    def segment_path(self, source_url: str, segment: Segment) -> Path:
        return self.source_dir(source_url) / segment.file

    def read_slice(self, source_url: str, start: int, end: int) -> Optional[bytes]:
        """Return exact [start, end] bytes if contiguous coverage exists."""
        covering = self.find_covering_segments(source_url, start, end)
        if not covering:
            return None
        parts: list[bytes] = []
        cursor = start
        for seg in covering:
            path = self.segment_path(source_url, seg)
            if not path.is_file():
                return None
            piece_start = cursor
            piece_end = min(end, seg.end)
            length = piece_end - piece_start + 1
            offset = piece_start - seg.start
            with path.open("rb") as handle:
                handle.seek(offset)
                data = handle.read(length)
            if len(data) != length:
                return None
            parts.append(data)
            cursor = piece_end + 1
            if cursor > end:
                break
        if cursor <= end:
            return None
        return b"".join(parts)

    def iter_slice(
        self,
        source_url: str,
        start: int,
        end: int,
        *,
        chunk_size: int = COPY_CHUNK,
    ) -> Optional[Iterator[bytes]]:
        """Yield [start, end] in chunks without loading the whole range."""
        covering = self.find_covering_segments(source_url, start, end)
        if not covering:
            return None
        for seg in covering:
            if not self.segment_path(source_url, seg).is_file():
                return None

        def _gen() -> Iterator[bytes]:
            cursor = start
            for seg in covering:
                path = self.segment_path(source_url, seg)
                piece_end = min(end, seg.end)
                remaining = piece_end - cursor + 1
                offset = cursor - seg.start
                with path.open("rb") as handle:
                    handle.seek(offset)
                    while remaining > 0:
                        part = handle.read(min(chunk_size, remaining))
                        if not part:
                            break
                        remaining -= len(part)
                        yield part
                cursor = piece_end + 1
                if cursor > end:
                    break

        return _gen()

    def ingest(
        self,
        source_url: str,
        start: int,
        end: int,
        data: bytes,
        *,
        merge: bool = True,
    ) -> Segment:
        """Store [start, end] and (by default) merge+pack into ≤1 GiB files.

        If contiguous coverage already exists, skip write and return the first
        covering segment. With ``merge=False``, write a single segment file
        (still split if the range itself exceeds max segment size).
        """
        if end < start:
            raise ValueError("invalid range: end < start")
        expected = end - start + 1
        if len(data) != expected:
            raise ValueError(f"data length {len(data)} != range size {expected}")

        self._ensure_consolidated()
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

        covering = self._covering_segments_locked(segments, start, end)
        if covering:
            return covering[0]

        shards = self._write_data_shards(directory, start, end, data)
        replaced_ranges = {(s.start, s.end) for s in shards}
        kept = [s for s in segments if (s.start, s.end) not in replaced_ranges]
        kept.extend(shards)
        self._save_segments(source_url, kept)
        return next(s for s in shards if s.start <= start <= s.end)

    def _ingest_merge_locked(
        self,
        source_url: str,
        start: int,
        end: int,
        data: bytes,
    ) -> Segment:
        """Store new bytes, merge with touching segments, pack to ≤1 GiB files."""
        directory = self.source_dir(source_url)
        directory.mkdir(parents=True, exist_ok=True)
        segments = self._load_segments(source_url)

        covering = self._covering_segments_locked(segments, start, end)
        if covering:
            return covering[0]

        group = self._touching_group(segments, start, end)
        if not group:
            shards = self._write_data_shards(directory, start, end, data)
            kept = list(segments)
            kept.extend(shards)
            self._save_segments(source_url, kept)
            return next(s for s in shards if s.start <= start <= s.end)

        # Materialize new range to a temp file so merge can stream from pieces.
        fd, tmp_name = tempfile.mkstemp(prefix=".ingest_", suffix=".bin", dir=directory)
        os.close(fd)
        tmp_path = Path(tmp_name)
        try:
            tmp_path.write_bytes(data)
            new_seg = Segment(start=start, end=end, file=tmp_path.name)
            group_files = [s for s in group if not (s.start == start and s.end == end)]
            group_files.append(new_seg)
            kept = [s for s in segments if s not in group]
            shards = self._merge_group_locked(source_url, directory, group_files)
            kept.extend(shards)
            self._save_segments(source_url, kept)
            return next(s for s in shards if s.start <= start <= s.end)
        finally:
            tmp_path.unlink(missing_ok=True)

    @staticmethod
    def _covering_segments_locked(
        segments: list[Segment], start: int, end: int
    ) -> list[Segment]:
        if end < start:
            return []
        covering: list[Segment] = []
        cursor = start
        for seg in segments:
            if seg.end < cursor:
                continue
            if seg.start > cursor:
                return []
            covering.append(seg)
            cursor = seg.end + 1
            if cursor > end:
                return covering
        return []

    @staticmethod
    def _touching_group(
        segments: list[Segment], start: int, end: int
    ) -> list[Segment]:
        """Expand to the full contiguous group that touches [start, end]."""
        touching = [s for s in segments if s.overlaps_or_adjacent(start, end)]
        if not touching:
            return []
        lo = min(min(s.start for s in touching), start)
        hi = max(max(s.end for s in touching), end)
        changed = True
        while changed:
            changed = False
            for seg in segments:
                if seg in touching:
                    continue
                if seg.overlaps_or_adjacent(lo, hi):
                    touching.append(seg)
                    lo = min(lo, seg.start)
                    hi = max(hi, seg.end)
                    changed = True
        return sorted(touching, key=lambda s: s.start)

    def _write_data_shards(
        self,
        directory: Path,
        start: int,
        end: int,
        data: bytes,
    ) -> list[Segment]:
        shards: list[Segment] = []
        for shard_start, shard_end in shard_bounds(
            start, end, max_bytes=self.max_segment_bytes
        ):
            file_name = _segment_filename(shard_start, shard_end)
            path = directory / file_name
            offset = shard_start - start
            length = shard_end - shard_start + 1
            path.write_bytes(data[offset : offset + length])
            shards.append(Segment(start=shard_start, end=shard_end, file=file_name))
        return shards

    def _write_file_shards(
        self,
        directory: Path,
        start: int,
        end: int,
        src_path: Path,
    ) -> list[Segment]:
        """Pack [start, end] from ``src_path`` into ≤1 GiB shards (streamed)."""
        expected = end - start + 1
        if src_path.stat().st_size != expected:
            raise ValueError(
                f"file size {src_path.stat().st_size} != range size {expected}"
            )
        bounds = shard_bounds(start, end, max_bytes=self.max_segment_bytes)
        # Fast path: exact one shard → hardlink/rename instead of rewriting 1 GiB.
        if len(bounds) == 1 and bounds[0] == (start, end):
            file_name = _segment_filename(start, end)
            out_path = directory / file_name
            if out_path.resolve() != src_path.resolve():
                fd, tmp_name = tempfile.mkstemp(
                    prefix=".range_", suffix=".bin", dir=directory
                )
                os.close(fd)
                tmp_path = Path(tmp_name)
                try:
                    try:
                        os.link(src_path, tmp_path)
                    except OSError:
                        with src_path.open("rb") as src, tmp_path.open("wb") as out:
                            while True:
                                part = src.read(COPY_CHUNK)
                                if not part:
                                    break
                                out.write(part)
                    os.replace(tmp_path, out_path)
                finally:
                    tmp_path.unlink(missing_ok=True)
            return [Segment(start=start, end=end, file=file_name)]

        shards: list[Segment] = []
        with src_path.open("rb") as src:
            for shard_start, shard_end in bounds:
                file_name = _segment_filename(shard_start, shard_end)
                out_path = directory / file_name
                offset = shard_start - start
                length = shard_end - shard_start + 1
                src.seek(offset)
                fd, tmp_name = tempfile.mkstemp(
                    prefix=".range_", suffix=".bin", dir=directory
                )
                os.close(fd)
                tmp_path = Path(tmp_name)
                try:
                    remaining = length
                    with tmp_path.open("wb") as out:
                        while remaining > 0:
                            part = src.read(min(COPY_CHUNK, remaining))
                            if not part:
                                break
                            out.write(part)
                            remaining -= len(part)
                    if remaining:
                        raise RuntimeError(
                            f"short read packing {src_path} at offset {offset}"
                        )
                    os.replace(tmp_path, out_path)
                finally:
                    tmp_path.unlink(missing_ok=True)
                shards.append(
                    Segment(start=shard_start, end=shard_end, file=file_name)
                )
        return shards

    def _merge_group_locked(
        self,
        source_url: str,
        directory: Path,
        group: list[Segment],
    ) -> list[Segment]:
        """Merge one contiguous group and pack into ≤ max_segment_bytes files."""
        group = sorted(group, key=lambda s: s.start)
        merged_start = min(s.start for s in group)
        merged_end = max(s.end for s in group)

        pieces: list[tuple[int, int, Path]] = []
        for seg in group:
            seg_path = directory / seg.file
            if seg_path.is_file():
                pieces.append((seg.start, seg.end, seg_path))

        resolved = self._resolve_piece_intervals(pieces)
        bounds = shard_bounds(
            merged_start, merged_end, max_bytes=self.max_segment_bytes
        )
        new_segments: list[Segment] = []
        written_paths: set[Path] = set()

        for shard_start, shard_end in bounds:
            file_name = _segment_filename(shard_start, shard_end)
            out_path = directory / file_name
            fd, tmp_name = tempfile.mkstemp(
                prefix=".range_", suffix=".bin", dir=directory
            )
            os.close(fd)
            tmp_path = Path(tmp_name)
            try:
                with tmp_path.open("wb") as out:
                    self._copy_range_to(
                        out,
                        resolved,
                        shard_start,
                        shard_end,
                        source_url=source_url,
                    )
                os.replace(tmp_path, out_path)
            finally:
                if tmp_path.exists():
                    tmp_path.unlink(missing_ok=True)
            written_paths.add(out_path)
            new_segments.append(
                Segment(start=shard_start, end=shard_end, file=file_name)
            )

        for seg in group:
            old = directory / seg.file
            if old not in written_paths and old.is_file():
                old.unlink(missing_ok=True)

        return new_segments

    @staticmethod
    def _resolve_piece_intervals(
        pieces: list[tuple[int, int, Path]],
    ) -> list[tuple[int, int, Path, int]]:
        """Resolve overlapping pieces so later pieces win on conflicts."""
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
        return resolved

    @staticmethod
    def _copy_range_to(
        out,
        resolved: list[tuple[int, int, Path, int]],
        start: int,
        end: int,
        *,
        source_url: str,
    ) -> None:
        cursor = start
        for r_start, r_end, r_path, file_start in resolved:
            if r_end < cursor:
                continue
            if r_start > cursor:
                raise RuntimeError(
                    f"non-contiguous merge at {cursor}..{r_start - 1} for {source_url}"
                )
            piece_end = min(end, r_end)
            length = piece_end - cursor + 1
            with r_path.open("rb") as src:
                src.seek(cursor - file_start)
                remaining = length
                while remaining > 0:
                    part = src.read(min(COPY_CHUNK, remaining))
                    if not part:
                        break
                    out.write(part)
                    remaining -= len(part)
                if remaining:
                    raise RuntimeError(
                        f"short read merging {source_url} at {cursor}"
                    )
            cursor = piece_end + 1
            if cursor > end:
                break
        if cursor != end + 1:
            raise RuntimeError(
                f"merge incomplete: wrote through {cursor - 1}, expected {end}"
            )

    def merge_source(self, source_url: str) -> dict[str, Any]:
        """Compact adjacent/overlapping segments and pack to ≤1 GiB files."""
        digest = source_digest(source_url)
        with self._source_lock(digest):
            directory = self.source_dir(source_url)
            segments = self._load_segments(source_url)
            before = len(segments)
            if before == 0:
                return {
                    "source_url": source_url,
                    "before": 0,
                    "after": 0,
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
                # Always re-pack so oversized files are split and touching
                # segments are compacted.
                needs_repack = len(group) > 1 or any(
                    s.size > self.max_segment_bytes for s in group
                )
                if not needs_repack:
                    # Still split if a single file crosses an absolute boundary.
                    seg = group[0]
                    expected = shard_bounds(
                        seg.start, seg.end, max_bytes=self.max_segment_bytes
                    )
                    if expected == [(seg.start, seg.end)]:
                        new_segments.append(seg)
                        continue
                shards = self._merge_group_locked(source_url, directory, group)
                if len(group) > 1 or len(shards) != len(group):
                    merged_groups += 1
                new_segments.extend(shards)

            self._save_segments(source_url, new_segments)
            return {
                "source_url": source_url,
                "before": before,
                "after": len(new_segments),
                "merged_groups": merged_groups,
                "segments": [s.to_dict() for s in new_segments],
            }

    def consolidate_signed_url_orphans(self) -> dict[str, Any]:
        """Merge legacy hash-named dirs into the object-filename directory.

        Older code used sha256(full URL) or sha256(filename) as the folder name.
        Those orphans are ingested into ``<object_filename.bin>/`` then deleted.
        """
        if not self.root.is_dir():
            return {"merged_dirs": 0, "ingested_segments": 0, "removed_dirs": []}

        merged_dirs = 0
        ingested = 0
        removed: list[str] = []

        for child in sorted(self.root.iterdir()):
            if not child.is_dir() or child.name.startswith("."):
                continue
            index = child / "segments.json"
            if not index.is_file():
                continue
            try:
                data = json.loads(index.read_text(encoding="utf-8"))
            except (OSError, json.JSONDecodeError):
                continue
            raw_src = str(data.get("source_url") or "").strip()
            obj_meta = str(data.get("object_name") or "").strip()
            canonical = normalize_source_url(raw_src)
            if not canonical and obj_meta:
                # Index only has object_name — still enough for the dir key.
                canonical = obj_meta
            if not canonical:
                continue
            canonical_dir = source_cache_dir_name(canonical)
            if not canonical_dir:
                continue
            if child.name == canonical_dir:
                # Rewrite index if it still stores a signed URL / missing object_name.
                if raw_src and raw_src != normalize_source_url(raw_src):
                    segs = self._load_segments(canonical)
                    self._save_segments(canonical, segs)
                elif not data.get("object_name"):
                    segs = self._load_segments(canonical)
                    self._save_segments(canonical, segs)
                continue

            target = self.root / canonical_dir
            if not target.exists():
                # Fast path: rename hash dir → object filename (no byte copy).
                try:
                    child.rename(target)
                    removed.append(child.name)
                    merged_dirs += 1
                    # Refresh index under the new name.
                    segs = self._load_segments(canonical)
                    self._save_segments(canonical, segs)
                    continue
                except OSError:
                    pass

            items = data.get("segments") if isinstance(data, dict) else []
            if not isinstance(items, list):
                items = []
            for item in items:
                if not isinstance(item, dict):
                    continue
                try:
                    start = int(item["start"])
                    end = int(item["end"])
                    file_name = str(item.get("file") or _segment_filename(start, end))
                except (KeyError, TypeError, ValueError):
                    continue
                src_path = child / file_name
                if not src_path.is_file():
                    continue
                if self._covers_canonical(canonical, start, end):
                    continue
                self.ingest_from_file(canonical, start, end, src_path, merge=True)
                ingested += 1

            # Remove orphan directory after successful merge.
            for leftover in child.iterdir():
                try:
                    leftover.unlink()
                except IsADirectoryError:
                    continue
                except OSError:
                    continue
            try:
                child.rmdir()
                removed.append(child.name)
                merged_dirs += 1
            except OSError:
                pass

        return {
            "merged_dirs": merged_dirs,
            "ingested_segments": ingested,
            "removed_dirs": removed,
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
        merge: bool = True,
    ) -> Segment:
        """Ingest range bytes from an on-disk file without loading it into RAM.

        Streams through temp/shard writes and the existing merge packer so a
        1 GiB segment download stays near COPY_CHUNK peak memory.
        """
        if end < start:
            raise ValueError("invalid range: end < start")
        src = Path(path)
        if not src.is_file():
            raise ValueError(f"ingest file missing: {src}")
        expected = end - start + 1
        size = src.stat().st_size
        if size != expected:
            raise ValueError(f"file size {size} != range size {expected}")

        self._ensure_consolidated()
        source_url = normalize_source_url(source_url)
        digest = source_digest(source_url)
        with self._source_lock(digest):
            directory = self.source_dir(source_url)
            directory.mkdir(parents=True, exist_ok=True)
            segments = self._load_segments(source_url)
            covering = self._covering_segments_locked(segments, start, end)
            if covering:
                return covering[0]

            if not merge:
                shards = self._write_file_shards(directory, start, end, src)
                replaced = {(s.start, s.end) for s in shards}
                kept = [s for s in segments if (s.start, s.end) not in replaced]
                kept.extend(shards)
                self._save_segments(source_url, kept)
                return next(s for s in shards if s.start <= start <= s.end)

            group = self._touching_group(segments, start, end)
            if not group:
                shards = self._write_file_shards(directory, start, end, src)
                kept = list(segments)
                kept.extend(shards)
                self._save_segments(source_url, kept)
                return next(s for s in shards if s.start <= start <= s.end)

            # Materialize into store dir so merge can stream from pieces.
            fd, tmp_name = tempfile.mkstemp(
                prefix=".ingest_", suffix=".bin", dir=directory
            )
            os.close(fd)
            tmp_path = Path(tmp_name)
            try:
                with src.open("rb") as infile, tmp_path.open("wb") as outfile:
                    while True:
                        part = infile.read(COPY_CHUNK)
                        if not part:
                            break
                        outfile.write(part)
                new_seg = Segment(start=start, end=end, file=tmp_path.name)
                group_files = [
                    s for s in group if not (s.start == start and s.end == end)
                ]
                group_files.append(new_seg)
                kept = [s for s in segments if s not in group]
                shards = self._merge_group_locked(source_url, directory, group_files)
                kept.extend(shards)
                self._save_segments(source_url, kept)
                return next(s for s in shards if s.start <= start <= s.end)
            finally:
                tmp_path.unlink(missing_ok=True)

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
            "max_segment_bytes": self.max_segment_bytes,
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
