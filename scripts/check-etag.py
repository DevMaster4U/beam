#!/usr/bin/env python3
"""Load range bytes from range_data and print MD5 ETag (+ SHA256 chunk_hash).

For Cloudflare R2 / S3 single-part PUT, storage ETag is:
  '"' + md5(bytes).hexdigest() + '"'

Usage:
  python3 scripts/check-etag.py \\
    --key 'https://.../ecx_20_b_gb.bin|9898557440|9940500479'

  python3 scripts/check-etag.py \\
    --source 'https://.../ecx_20_b_gb.bin' --start 9898557440 --end 9940500479

  # Worker local store
  python3 scripts/check-etag.py --worker \\
    --key 'https://.../ecx_20_b_gb.bin|9898557440|9940500479'

  # Also compare against predefined_etag_chunks.json
  python3 scripts/check-etag.py --meta \\
    --key 'https://.../ecx_20_b_gb.bin|9898557440|9940500479'

Exit codes:
  0  range found and hashes printed (and meta matches if --meta and known)
  1  range miss / hash mismatch with --meta
  2  usage error
"""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
from pathlib import Path
from typing import Optional
from urllib.parse import urlsplit, urlunsplit

_REPO = Path(__file__).resolve().parents[1]
if str(_REPO) not in sys.path:
    sys.path.insert(0, str(_REPO))

from neurons.common.byte_range_store import (  # noqa: E402
    ByteRangeStore,
    parse_cache_key_range,
    source_digest,
)


def _normalize_source(url: str) -> str:
    raw = str(url or "").strip()
    if not raw:
        return ""
    parts = urlsplit(raw)
    cleaned = urlunsplit((parts.scheme, parts.netloc, parts.path, "", ""))
    return cleaned.rstrip("/")


def _default_control_root() -> Path:
    return _REPO / "data" / "control-server" / "cache" / "range_data"


def _default_worker_root() -> Path:
    return _REPO / "logs" / "workers" / "predefined_etag_range_data"


def _default_control_meta() -> Path:
    return _REPO / "data" / "control-server" / "cache" / "predefined_etag_chunks.json"


def _default_worker_meta() -> Path:
    return _REPO / "logs" / "workers" / "predefined_etag_chunks.json"


def _resolve_query(
    *,
    key: str,
    source: str,
    start: Optional[int],
    end: Optional[int],
) -> tuple[str, int, int]:
    if key.strip():
        parsed = parse_cache_key_range(key.strip())
        if parsed is None:
            raise SystemExit(f"invalid --key (need source|start|end): {key[:160]!r}")
        src, s, e = parsed
        return _normalize_source(src), s, e

    src = _normalize_source(source)
    if not src:
        raise SystemExit("need --key or --source with --start/--end")
    if start is None or end is None:
        raise SystemExit("need --start and --end (or --key)")
    if end < start:
        raise SystemExit(f"invalid range: end < start ({start}-{end})")
    return src, int(start), int(end)


def _lookup_meta(path: Path, cache_key: str) -> Optional[dict]:
    if not path.is_file():
        return None
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    entries = payload.get("entries") if isinstance(payload, dict) else None
    if not isinstance(entries, dict):
        return None
    item = entries.get(cache_key)
    if not isinstance(item, dict):
        return None
    chunk_hash = str(item.get("chunk_hash") or item.get("hash") or "").strip()
    if not chunk_hash:
        return None
    return {
        "chunk_hash": chunk_hash,
        "etag": str(item.get("etag") or ""),
        "has_chunk_data": bool(item.get("has_chunk_data")),
    }


def main() -> int:
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--key",
        default="",
        help="cache key source|start|end",
    )
    parser.add_argument("--source", default="", help="source URL")
    parser.add_argument("--start", type=int, default=None, help="inclusive start")
    parser.add_argument("--end", type=int, default=None, help="inclusive end")
    parser.add_argument(
        "--root",
        type=Path,
        default=None,
        help="range_data root (default: control-server cache)",
    )
    parser.add_argument(
        "--worker",
        action="store_true",
        help="use logs/workers/predefined_etag_range_data",
    )
    parser.add_argument(
        "--meta",
        action="store_true",
        help="compare against predefined_etag_chunks.json",
    )
    parser.add_argument(
        "--meta-path",
        type=Path,
        default=None,
        help="explicit metadata JSON path",
    )
    args = parser.parse_args()

    try:
        source, start, end = _resolve_query(
            key=args.key,
            source=args.source,
            start=args.start,
            end=args.end,
        )
    except SystemExit as exc:
        print(str(exc), file=sys.stderr)
        return 2

    if args.root is not None:
        root = args.root
    elif args.worker:
        root = _default_worker_root()
    else:
        root = _default_control_root()

    expected = end - start + 1
    cache_key = f"{source}|{start}|{end}"
    digest = source_digest(source)

    print(f"source={source}")
    print(f"digest={digest}")
    print(f"range=[{start},{end}] size={expected}")
    print(f"cache_key={cache_key}")
    print(f"root={root}")

    if not root.is_dir():
        print(f"MISS range_data root missing: {root}", file=sys.stderr)
        return 1

    store = ByteRangeStore(root)
    if not store.covers(source, start, end):
        report = store.coverage_report(source, span_start=start, span_end=end)
        print("MISS bytes not covered in range_data", file=sys.stderr)
        gaps = report.get("gaps") or []
        print(f"  source_segments={report.get('segment_count')} gaps={len(gaps)}")
        for gap in gaps[:8]:
            print(
                f"  gap {gap['start']}-{gap['end']} size={gap['size']}",
                file=sys.stderr,
            )
        return 1

    data = store.read_slice(source, start, end)
    if data is None or len(data) != expected:
        print(
            f"MISS read_slice failed got={0 if data is None else len(data)} "
            f"expected={expected}",
            file=sys.stderr,
        )
        return 1

    etag = f'"{hashlib.md5(data).hexdigest()}"'
    chunk_hash = hashlib.sha256(data).hexdigest()

    print(f"bytes_loaded={len(data)}")
    print(f"etag_from_bytes={etag}")
    print(f"chunk_hash_from_bytes={chunk_hash}")

    if not args.meta and args.meta_path is None:
        return 0

    meta_path = args.meta_path
    if meta_path is None:
        meta_path = _default_worker_meta() if args.worker else _default_control_meta()

    stored = _lookup_meta(meta_path, cache_key)
    print(f"meta_path={meta_path}")
    if stored is None:
        print("meta=UNKNOWN (key not in JSON)")
        return 1

    print(f"etag_from_meta={stored['etag']!r}")
    print(f"chunk_hash_from_meta={stored['chunk_hash']}")
    etag_ok = stored["etag"] == etag
    hash_ok = stored["chunk_hash"].lower() == chunk_hash.lower()
    print(f"etag_match={etag_ok}")
    print(f"chunk_hash_match={hash_ok}")
    return 0 if etag_ok and hash_ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
