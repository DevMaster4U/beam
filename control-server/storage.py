"""On-disk storage helpers for control-server."""

from __future__ import annotations

import copy
import hashlib
import io
import json
import sys
import tarfile
import threading
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

_REPO_ROOT = Path(__file__).resolve().parents[1]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from config import get_settings

# Shared range store lives in neurons/common (repo root on sys.path).
from neurons.common.byte_range_store import (  # noqa: E402
    ByteRangeStore,
    parse_cache_key_range,
    source_digest,
)

_cache_lock = threading.Lock()
_cache_memory: dict[str, Any] | None = None
_range_store: ByteRangeStore | None = None


def get_range_store() -> ByteRangeStore:
    global _range_store
    if _range_store is None:
        root = get_settings().cache_dir / "range_data"
        root.mkdir(parents=True, exist_ok=True)
        _range_store = ByteRangeStore(root)
    return _range_store


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _empty_cache_payload() -> dict[str, Any]:
    return {"entries": {}, "updated_at": None}


def chunk_index_from_cache_key(key: str) -> Optional[int]:
    """Derive chunk index from cache key ``source_url|range_start|range_end``.

    chunk_index = range_start // (range_end - range_start)

    Note: this index is derived purely from byte offsets and is NOT unique
    across different chunk sizes for the same source URL — e.g. index 1 for
    a 256KB chunking scheme covers different bytes than index 1 for a 1MB
    scheme. Callers that need an unambiguous identity must also key on
    chunk_size (see ``chunk_size_from_cache_key``).
    """
    parsed = parse_cache_key(key)
    if parsed is None:
        return None
    range_start, range_end = parsed[1], parsed[2]
    span = range_end - range_start
    if span <= 0:
        return None
    return range_start // span


def chunk_size_from_cache_key(key: str) -> Optional[int]:
    """Derive chunk size (inclusive byte range span) from a cache key."""
    parsed = parse_cache_key(key)
    if parsed is None:
        return None
    range_start, range_end = parsed[1], parsed[2]
    size = range_end - range_start + 1
    if size <= 0:
        return None
    return size


def parse_cache_key(key: str) -> Optional[tuple[str, int, int]]:
    """Return (source_url, range_start, range_end) from a cache key."""
    parts = str(key or "").rsplit("|", 2)
    if len(parts) != 3:
        return None
    try:
        range_start = int(parts[1])
        range_end = int(parts[2])
    except ValueError:
        return None
    source_url = parts[0].strip()
    if not source_url:
        return None
    return source_url, range_start, range_end


def _build_cache_entry(
    key: str,
    chunk_hash: str,
    etag: str,
    *,
    has_chunk_data: Optional[bool] = None,
) -> dict[str, Any]:
    entry: dict[str, Any] = {"chunk_hash": chunk_hash, "etag": etag}
    chunk_index = chunk_index_from_cache_key(key)
    if chunk_index is not None:
        entry["chunk_index"] = chunk_index
    chunk_size = chunk_size_from_cache_key(key)
    if chunk_size is not None:
        entry["chunk_size"] = chunk_size
    if has_chunk_data is None:
        has_chunk_data = has_predefined_etag_chunk_data(key)
    if has_chunk_data:
        entry["has_chunk_data"] = True
    return entry


def predefined_etag_chunk_data_dir() -> Path:
    return get_settings().cache_dir / "chunk_data"


def predefined_etag_chunk_data_path(key: str) -> Path:
    digest = hashlib.sha256(str(key).encode()).hexdigest()
    return predefined_etag_chunk_data_dir() / f"{digest}.bin"


def store_predefined_etag_range_data(
    source_url: str, start: int, end: int, data: bytes
) -> dict[str, Any]:
    """Ingest bytes into the continuous range store (merge + ≤1 GiB pack)."""
    from neurons.common.byte_range_store import normalize_source_url

    segment = get_range_store().ingest(
        normalize_source_url(source_url), start, end, data
    )
    return segment.to_dict()


def store_predefined_etag_chunk_data(key: str, data: bytes) -> Path:
    """Store chunk bytes into the continuous range store (legacy key API)."""
    parsed = parse_cache_key_range(key)
    if parsed is None:
        raise ValueError(f"invalid cache key: {key[:96]}")
    source_url, start, end = parsed
    store_predefined_etag_range_data(source_url, start, end, data)
    return predefined_etag_chunk_data_path(key)

def load_predefined_etag_range_data(
    source_url: str, start: int, end: int
) -> Optional[bytes]:
    return get_range_store().read_slice(source_url, start, end)


def load_predefined_etag_chunk_data(key: str) -> Optional[bytes]:
    parsed = parse_cache_key_range(key)
    if parsed is not None:
        source_url, start, end = parsed
        data = load_predefined_etag_range_data(source_url, start, end)
        if data is not None:
            return data
    # Legacy per-key .bin fallback during migration.
    path = predefined_etag_chunk_data_path(key)
    if path.is_file() and path.stat().st_size > 0:
        return path.read_bytes()
    return None


def has_predefined_etag_range_data(source_url: str, start: int, end: int) -> bool:
    return get_range_store().covers(source_url, start, end)


def has_predefined_etag_chunk_data(key: str) -> bool:
    parsed = parse_cache_key_range(key)
    if parsed is not None:
        source_url, start, end = parsed
        if has_predefined_etag_range_data(source_url, start, end):
            return True
    path = predefined_etag_chunk_data_path(key)
    return path.is_file() and path.stat().st_size > 0


def delete_predefined_etag_chunk_data(key: str) -> bool:
    """Remove on-disk chunk bytes and clear has_chunk_data on the metadata entry."""
    path = predefined_etag_chunk_data_path(key)
    removed = False
    if path.is_file():
        path.unlink()
        removed = True
    with _cache_lock:
        payload = _ensure_cache_loaded_locked()
        entries = payload.get("entries") or {}
        item = entries.get(key)
        if isinstance(item, dict) and item.pop("has_chunk_data", None) is not None:
            _persist_cache_payload(payload)
            removed = True
    return removed


def prune_orphan_chunk_data_files() -> dict[str, int]:
    """Delete .bin files with no matching cache entry, or entries without has_chunk_data."""
    chunk_dir = predefined_etag_chunk_data_dir()
    if not chunk_dir.is_dir():
        return {"removed_files": 0, "cleared_flags": 0}

    with _cache_lock:
        payload = _ensure_cache_loaded_locked()
        entries = payload.get("entries") or {}
        keys_with_data = {
            str(key)
            for key, item in entries.items()
            if isinstance(item, dict) and item.get("has_chunk_data")
        }

    digest_to_key: dict[str, str] = {}
    for key in keys_with_data:
        digest = hashlib.sha256(str(key).encode()).hexdigest()
        digest_to_key[digest] = key

    removed_files = 0
    for path in chunk_dir.glob("*.bin"):
        digest = path.stem
        key = digest_to_key.get(digest)
        if key is None or not has_predefined_etag_chunk_data(key):
            if path.is_file():
                path.unlink()
                removed_files += 1

    cleared_flags = 0
    with _cache_lock:
        payload = _ensure_cache_loaded_locked()
        entries = payload.get("entries") or {}
        for key, item in list(entries.items()):
            if not isinstance(item, dict):
                continue
            if item.get("has_chunk_data") and not has_predefined_etag_chunk_data(str(key)):
                item.pop("has_chunk_data", None)
                cleared_flags += 1
        if cleared_flags:
            _persist_cache_payload(payload)

    return {"removed_files": removed_files, "cleared_flags": cleared_flags}


def delete_all_chunk_data_files() -> int:
    """Remove every chunk .bin file and clear has_chunk_data on all entries."""
    chunk_dir = predefined_etag_chunk_data_dir()
    removed = 0
    if chunk_dir.is_dir():
        for path in chunk_dir.glob("*.bin"):
            if path.is_file():
                path.unlink()
                removed += 1
    with _cache_lock:
        payload = _ensure_cache_loaded_locked()
        entries = payload.get("entries") or {}
        changed = False
        for item in entries.values():
            if isinstance(item, dict) and item.pop("has_chunk_data", None) is not None:
                changed = True
        if changed:
            _persist_cache_payload(payload)
    return removed


def _enrich_cache_entries(payload: dict[str, Any]) -> bool:
    """Ensure every entry has chunk_index derived from its key. Returns True if mutated."""
    entries = payload.get("entries")
    if not isinstance(entries, dict):
        return False
    changed = False
    for key, item in entries.items():
        if not isinstance(item, dict):
            continue
        chunk_index = chunk_index_from_cache_key(str(key))
        if chunk_index is None:
            continue
        if item.get("chunk_index") != chunk_index:
            item["chunk_index"] = chunk_index
            changed = True
        chunk_size = chunk_size_from_cache_key(str(key))
        if chunk_size is not None and item.get("chunk_size") != chunk_size:
            item["chunk_size"] = chunk_size
            changed = True
        if has_predefined_etag_chunk_data(str(key)) and not item.get("has_chunk_data"):
            item["has_chunk_data"] = True
            changed = True
    return changed


def range_coverage_snapshot(*, src_url: Optional[str] = None) -> dict[str, Any]:
    """Build sync payload from range_data/*/segments.json only (no hash/etag JSON)."""
    store = get_range_store()
    filter_url = str(src_url or "").strip()
    sources_out: list[dict[str, Any]] = []
    for source_url in store.list_sources():
        if filter_url and source_url != filter_url:
            continue
        segs = store.list_segments(source_url)
        sources_out.append(
            {
                "source_url": source_url,
                "digest": source_digest(source_url),
                "segments": [{"start": s.start, "end": s.end} for s in segs],
                "covered_bytes": sum(s.size for s in segs),
            }
        )
    return {
        "status": "ok",
        "source_count": len(sources_out),
        "sources": sources_out,
        "updated_at": _utc_now(),
    }


def range_store_status(*, src_url: Optional[str] = None) -> dict[str, Any]:
    """Report continuous range-store coverage per source (from segments.json)."""
    store = get_range_store()
    filter_url = str(src_url or "").strip()
    sources: dict[str, Any] = {}
    for source_url in store.list_sources():
        if filter_url and source_url != filter_url:
            continue
        segs = store.list_segments(source_url)
        if not segs and filter_url:
            continue
        span_start = segs[0].start if segs else None
        span_end = segs[-1].end if segs else None
        report = store.coverage_report(
            source_url, span_start=span_start, span_end=span_end
        )
        sources[source_url] = report

    result: dict[str, Any] = {
        "status": "ok",
        "source_count": len(sources),
        "sources": sources,
    }
    if filter_url:
        result["src_url"] = filter_url
        result["coverage"] = sources.get(filter_url)
    return result


def _normalize_cache_payload(data: Any) -> dict[str, Any]:
    if not isinstance(data, dict):
        return _empty_cache_payload()
    entries = data.get("entries")
    if not isinstance(entries, dict):
        data = dict(data)
        data["entries"] = {}
    return data


def _read_cache_from_disk() -> dict[str, Any]:
    path = get_settings().predefined_etag_cache_path
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return _empty_cache_payload()
    return _normalize_cache_payload(data)


def _persist_cache_payload(payload: dict[str, Any]) -> None:
    path = get_settings().predefined_etag_cache_path
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = dict(payload)
    payload["updated_at"] = _utc_now()
    path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")


def _ensure_cache_loaded_locked() -> dict[str, Any]:
    global _cache_memory
    if _cache_memory is None:
        _cache_memory = _read_cache_from_disk()
        if _enrich_cache_entries(_cache_memory):
            _persist_cache_payload(_cache_memory)
    return _cache_memory


def _cache_snapshot_locked() -> dict[str, Any]:
    payload = _ensure_cache_loaded_locked()
    return {
        "entries": copy.deepcopy(payload.get("entries") or {}),
        "updated_at": payload.get("updated_at"),
    }


def load_predefined_etag_cache() -> dict[str, Any]:
    with _cache_lock:
        return _cache_snapshot_locked()


def save_predefined_etag_cache(payload: dict[str, Any]) -> None:
    with _cache_lock:
        normalized = _normalize_cache_payload(payload)
        global _cache_memory
        _cache_memory = normalized
        _persist_cache_payload(normalized)


def upsert_predefined_etag_entry(
    key: str,
    chunk_hash: str,
    etag: str,
    *,
    has_chunk_data: Optional[bool] = None,
) -> dict[str, Any]:
    with _cache_lock:
        payload = _ensure_cache_loaded_locked()
        entries = payload.setdefault("entries", {})
        entries[key] = _build_cache_entry(
            key, chunk_hash, etag, has_chunk_data=has_chunk_data
        )
        _persist_cache_payload(payload)
        return dict(entries[key])


def merge_predefined_etag_entries(new_entries: dict[str, dict[str, str]]) -> dict[str, Any]:
    with _cache_lock:
        payload = _ensure_cache_loaded_locked()
        entries = payload.setdefault("entries", {})
        for key, item in new_entries.items():
            if not isinstance(item, dict):
                continue
            chunk_hash = str(item.get("chunk_hash") or "").strip()
            etag = str(item.get("etag") or "").strip()
            if chunk_hash:
                entries[key] = _build_cache_entry(key, chunk_hash, etag)
        _persist_cache_payload(payload)
        return _cache_snapshot_locked()


def preload_predefined_etag_cache() -> int:
    """Warm in-memory cache at startup; returns entry count."""
    with _cache_lock:
        payload = _ensure_cache_loaded_locked()
        return len(payload.get("entries") or {})


def predefined_etag_cache_status(*, src_url: Optional[str] = None) -> dict[str, Any]:
    """Group cached chunk_index values by normalized source URL and chunk_size.

    chunk_index alone is not unique across chunk sizes (see
    ``chunk_index_from_cache_key``), so entries are grouped by
    (source_url, chunk_size) to avoid reporting ambiguous chunk_indices.
    """
    payload = load_predefined_etag_cache()
    entries = payload.get("entries") or {}
    filter_url = str(src_url or "").strip()

    by_source: dict[str, dict[int, set[int]]] = {}
    for key, item in entries.items():
        if not isinstance(item, dict):
            continue
        parsed = parse_cache_key(str(key))
        if parsed is None:
            continue
        source_url, _, _ = parsed
        if filter_url and source_url != filter_url:
            continue
        chunk_index = item.get("chunk_index")
        if chunk_index is None:
            chunk_index = chunk_index_from_cache_key(str(key))
        if chunk_index is None:
            continue
        chunk_size = item.get("chunk_size")
        if chunk_size is None:
            chunk_size = chunk_size_from_cache_key(str(key))
        if chunk_size is None:
            continue
        by_source.setdefault(source_url, {}).setdefault(int(chunk_size), set()).add(
            int(chunk_index)
        )

    sources: dict[str, dict[str, Any]] = {}
    for source_url in sorted(by_source.keys()):
        by_size = by_source[source_url]
        chunk_sizes: dict[str, Any] = {}
        total_count = 0
        for chunk_size in sorted(by_size.keys()):
            indices = sorted(by_size[chunk_size])
            chunk_sizes[str(chunk_size)] = {
                "chunk_indices": indices,
                "count": len(indices),
            }
            total_count += len(indices)
        sources[source_url] = {
            "chunk_sizes": chunk_sizes,
            "count": total_count,
        }

    result: dict[str, Any] = {
        "status": "ok",
        "updated_at": payload.get("updated_at"),
        "source_count": len(sources),
        "sources": sources,
    }
    if filter_url:
        match = sources.get(filter_url)
        result["src_url"] = filter_url
        result["chunk_sizes"] = match["chunk_sizes"] if match else {}
        result["count"] = match["count"] if match else 0
    return result


def list_miners() -> list[str]:
    miners_dir = get_settings().miners_dir
    return sorted(path.stem for path in miners_dir.glob("*.env"))


def read_miner_env(miner_id: str) -> str:
    path = get_settings().miners_dir / f"{miner_id}.env"
    if not path.is_file():
        raise FileNotFoundError(miner_id)
    return path.read_text(encoding="utf-8")


def write_miner_env(miner_id: str, content: str) -> None:
    path = get_settings().miners_dir / f"{miner_id}.env"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


def list_wallets() -> list[str]:
    wallets_dir = get_settings().wallets_dir
    return sorted(
        path.name
        for path in wallets_dir.iterdir()
        if path.is_dir() and not path.name.startswith(".")
    )


def wallet_dir(wallet_name: str) -> Path:
    return get_settings().wallets_dir / wallet_name


def wallet_exists(wallet_name: str) -> bool:
    return wallet_dir(wallet_name).is_dir()


def list_wallet_hotkeys(wallet_name: str) -> list[str]:
    hotkeys_dir = wallet_dir(wallet_name) / "hotkeys"
    if not hotkeys_dir.is_dir():
        return []
    return sorted(
        path.name
        for path in hotkeys_dir.iterdir()
        if path.is_file() and not path.name.startswith(".")
    )


def wallet_hotkey_exists(wallet_name: str, hotkey: str) -> bool:
    return (wallet_dir(wallet_name) / "hotkeys" / hotkey).is_file()


def build_wallet_tarball(wallet_name: str) -> bytes:
    root = wallet_dir(wallet_name)
    if not root.is_dir():
        raise FileNotFoundError(wallet_name)
    buffer = io.BytesIO()
    with tarfile.open(fileobj=buffer, mode="w:gz") as tar:
        for path in root.rglob("*"):
            if path.is_file():
                tar.add(path, arcname=str(path.relative_to(root)))
    return buffer.getvalue()


def extract_wallet_tarball(wallet_name: str, payload: bytes) -> None:
    root = wallet_dir(wallet_name)
    root.mkdir(parents=True, exist_ok=True)
    with tarfile.open(fileobj=io.BytesIO(payload), mode="r:gz") as tar:
        tar.extractall(path=root)
