#!/usr/bin/env python3
"""Check whether a source byte range is HIT or MISS in range_data.

Uses the same continuous store logic as workers / control-server
(``ByteRangeStore.covers``). Optionally also checks metadata JSON.

Usage:
  # Control-server cache (default root)
  python3 scripts/check-range-cache.py \\
    --source 'https://example.com/bucket/file.bin' --start 0 --end 41943039

  # Cache key form: source|start|end
  python3 scripts/check-range-cache.py --key 'https://.../file.bin|0|41943039'

  # HTTP Range header form
  python3 scripts/check-range-cache.py \\
    --source 'https://.../file.bin' --range 'bytes=0-41943039'

  # Worker local mirror
  python3 scripts/check-range-cache.py --worker \\
    --source 'https://.../file.bin' --start 0 --end 41943039

  # Both roots + metadata
  python3 scripts/check-range-cache.py --both --meta \\
    --key 'https://.../file.bin|0|41943039'

Exit codes:
  0  HIT in every checked store
  1  MISS in at least one checked store
  2  usage / path error
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path
from typing import Any, Optional
from urllib.parse import urlsplit, urlunsplit

_REPO = Path(__file__).resolve().parents[1]
if str(_REPO) not in sys.path:
    sys.path.insert(0, str(_REPO))

from neurons.common.byte_range_store import (  # noqa: E402
    ByteRangeStore,
    parse_cache_key_range,
    source_digest,
)

_RANGE_RE = re.compile(r"(?i)^(?:bytes=)?(\d+)\s*-\s*(\d+)$")


def _normalize_source(url: str) -> str:
    """Match worker keying: drop query/fragment, strip trailing slash."""
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


def _parse_range_arg(value: str) -> tuple[int, int]:
    match = _RANGE_RE.match(value.strip())
    if not match:
        raise argparse.ArgumentTypeError(
            f"invalid --range {value!r} (expected bytes=START-END or START-END)"
        )
    start, end = int(match.group(1)), int(match.group(2))
    if end < start:
        raise argparse.ArgumentTypeError(f"invalid range: end < start ({start}-{end})")
    return start, end


def _resolve_query(
    *,
    key: str,
    source: str,
    start: Optional[int],
    end: Optional[int],
    range_hdr: str,
) -> tuple[str, int, int]:
    if key.strip():
        parsed = parse_cache_key_range(key.strip())
        if parsed is None:
            raise SystemExit(f"invalid --key (need source|start|end): {key[:120]!r}")
        src, s, e = parsed
        return _normalize_source(src), s, e

    src = _normalize_source(source)
    if not src:
        raise SystemExit("need --source (+ --start/--end or --range) or --key")

    if range_hdr.strip():
        s, e = _parse_range_arg(range_hdr)
        return src, s, e

    if start is None or end is None:
        raise SystemExit("need --start and --end (or --range / --key)")
    if end < start:
        raise SystemExit(f"invalid range: end < start ({start}-{end})")
    return src, int(start), int(end)


def _format_bytes(n: int) -> str:
    if n < 1024:
        return f"{n} B"
    if n < 1024**2:
        return f"{n / 1024:.1f} KiB"
    if n < 1024**3:
        return f"{n / (1024**2):.2f} MiB"
    return f"{n / (1024**3):.2f} GiB"


def _check_store(
    label: str,
    root: Path,
    source: str,
    start: int,
    end: int,
    *,
    verify_read: bool,
) -> dict[str, Any]:
    result: dict[str, Any] = {
        "label": label,
        "root": str(root),
        "exists": root.is_dir(),
        "hit": False,
        "segments": [],
        "gaps": [],
        "gap_bytes": 0,
        "read_ok": None,
    }
    if not root.is_dir():
        return result

    store = ByteRangeStore(root)
    digest = source_digest(source)
    source_dir = store.source_dir(source)
    result["digest"] = digest
    result["source_dir"] = str(source_dir)
    result["source_dir_exists"] = source_dir.is_dir()

    covering = store.find_covering_segments(source, start, end)
    hit = bool(covering)
    result["hit"] = hit
    result["segments"] = [
        {"start": s.start, "end": s.end, "file": s.file, "size": s.size}
        for s in covering
    ]

    report = store.coverage_report(source, span_start=start, span_end=end)
    result["gaps"] = report.get("gaps") or []
    result["gap_bytes"] = int(report.get("gap_bytes") or 0)
    result["source_segment_count"] = int(report.get("segment_count") or 0)
    result["source_covered_bytes"] = int(report.get("covered_bytes") or 0)

    if hit and verify_read:
        data = store.read_slice(source, start, end)
        expected = end - start + 1
        result["read_ok"] = data is not None and len(data) == expected
        result["read_bytes"] = len(data) if data is not None else 0

    return result


def _check_meta(path: Path, source: str, start: int, end: int) -> dict[str, Any]:
    key = f"{source}|{start}|{end}"
    out: dict[str, Any] = {
        "path": str(path),
        "exists": path.is_file(),
        "key": key,
        "known": False,
        "entry": None,
    }
    if not path.is_file():
        return out
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        out["error"] = str(exc)
        return out
    entries = payload.get("entries") if isinstance(payload, dict) else None
    if not isinstance(entries, dict):
        return out
    item = entries.get(key)
    if isinstance(item, dict) and str(item.get("chunk_hash") or item.get("hash") or ""):
        out["known"] = True
        out["entry"] = {
            "chunk_hash": str(item.get("chunk_hash") or item.get("hash") or ""),
            "etag": str(item.get("etag") or ""),
            "has_chunk_data": bool(item.get("has_chunk_data")),
        }
    return out


def _print_store(result: dict[str, Any], start: int, end: int) -> None:
    size = end - start + 1
    status = "HIT" if result.get("hit") else "MISS"
    print(f"\n[{result['label']}] {status}")
    print(f"  root={result['root']}")
    if not result.get("exists"):
        print("  (range_data root missing)")
        return
    print(f"  digest={result.get('digest')}")
    print(f"  source_dir={result.get('source_dir')} exists={result.get('source_dir_exists')}")
    print(
        f"  query=[{start},{end}] size={size} ({_format_bytes(size)})"
    )
    print(
        f"  source_segments={result.get('source_segment_count')} "
        f"source_covered={_format_bytes(int(result.get('source_covered_bytes') or 0))}"
    )
    if result.get("hit"):
        for seg in result.get("segments") or []:
            print(
                f"  cover {seg['start']}-{seg['end']} "
                f"file={seg['file']} ({_format_bytes(seg['size'])})"
            )
        if result.get("read_ok") is not None:
            print(
                f"  read_slice={'ok' if result['read_ok'] else 'FAIL'} "
                f"bytes={result.get('read_bytes')}"
            )
    else:
        gaps = result.get("gaps") or []
        gap_bytes = int(result.get("gap_bytes") or 0)
        print(f"  gaps_in_query={len(gaps)} gap_bytes={_format_bytes(gap_bytes)}")
        for gap in gaps[:12]:
            print(
                f"  gap  {gap['start']}-{gap['end']} "
                f"({_format_bytes(gap['size'])})"
            )
        if len(gaps) > 12:
            print(f"  ... {len(gaps) - 12} more gaps")


def _print_meta(label: str, meta: dict[str, Any]) -> None:
    print(f"\n[meta:{label}] {'KNOWN' if meta.get('known') else 'UNKNOWN'}")
    print(f"  path={meta.get('path')} exists={meta.get('exists')}")
    print(f"  key={str(meta.get('key') or '')[:160]}")
    if meta.get("error"):
        print(f"  error={meta['error']}")
    entry = meta.get("entry")
    if entry:
        print(
            f"  hash={entry.get('chunk_hash', '')[:16]}… "
            f"etag={entry.get('etag')!r} "
            f"has_chunk_data={entry.get('has_chunk_data')}"
        )


def main() -> int:
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--source", default="", help="source URL (query stripped)")
    parser.add_argument("--start", type=int, default=None, help="inclusive range start")
    parser.add_argument("--end", type=int, default=None, help="inclusive range end")
    parser.add_argument(
        "--range",
        dest="range_hdr",
        default="",
        help="HTTP Range style: bytes=START-END or START-END",
    )
    parser.add_argument(
        "--key",
        default="",
        help="cache key source|start|end (same as predefined_etag_chunks.json)",
    )
    parser.add_argument(
        "--root",
        type=Path,
        default=None,
        help="explicit range_data root (overrides --worker/--control/--both)",
    )
    parser.add_argument(
        "--control",
        action="store_true",
        help="check control-server cache (default if no --root/--worker/--both)",
    )
    parser.add_argument(
        "--worker",
        action="store_true",
        help="check worker local range_data under logs/workers/",
    )
    parser.add_argument(
        "--both",
        action="store_true",
        help="check both control-server and worker roots",
    )
    parser.add_argument(
        "--meta",
        action="store_true",
        help="also check predefined_etag_chunks.json for exact key",
    )
    parser.add_argument(
        "--meta-path",
        type=Path,
        default=None,
        help="explicit metadata JSON path (with --meta)",
    )
    parser.add_argument(
        "--verify-read",
        action="store_true",
        help="on HIT, also read_slice and verify byte length",
    )
    parser.add_argument(
        "--json-out",
        action="store_true",
        help="print machine-readable JSON summary",
    )
    args = parser.parse_args()

    try:
        source, start, end = _resolve_query(
            key=args.key,
            source=args.source,
            start=args.start,
            end=args.end,
            range_hdr=args.range_hdr,
        )
    except SystemExit as exc:
        print(str(exc), file=sys.stderr)
        return 2
    except argparse.ArgumentTypeError as exc:
        print(str(exc), file=sys.stderr)
        return 2

    roots: list[tuple[str, Path]] = []
    if args.root is not None:
        roots.append(("custom", args.root))
    elif args.both:
        roots.append(("control", _default_control_root()))
        roots.append(("worker", _default_worker_root()))
    elif args.worker:
        roots.append(("worker", _default_worker_root()))
    else:
        # default / --control
        roots.append(("control", _default_control_root()))

    store_results = [
        _check_store(label, root, source, start, end, verify_read=args.verify_read)
        for label, root in roots
    ]

    meta_results: list[tuple[str, dict[str, Any]]] = []
    if args.meta:
        if args.meta_path is not None:
            meta_results.append(("custom", _check_meta(args.meta_path, source, start, end)))
        else:
            labels = {label for label, _ in roots}
            if "control" in labels or args.both or (not args.worker and args.root is None):
                meta_results.append(
                    ("control", _check_meta(_default_control_meta(), source, start, end))
                )
            if "worker" in labels or args.worker or args.both:
                meta_results.append(
                    ("worker", _check_meta(_default_worker_meta(), source, start, end))
                )
            if args.root is not None and not meta_results:
                # custom root only: try sibling JSON if present
                sibling = args.root.parent / "predefined_etag_chunks.json"
                meta_results.append(("sibling", _check_meta(sibling, source, start, end)))

    all_hit = all(r.get("hit") for r in store_results) if store_results else False
    summary = {
        "source": source,
        "start": start,
        "end": end,
        "size": end - start + 1,
        "digest": source_digest(source),
        "cache_key": f"{source}|{start}|{end}",
        "overall": "HIT" if all_hit else "MISS",
        "stores": store_results,
        "meta": [{**m, "label": label} for label, m in meta_results],
    }

    if args.json_out:
        print(json.dumps(summary, indent=2))
    else:
        print(f"source={source}")
        print(f"digest={summary['digest']}")
        print(f"range=[{start},{end}] size={summary['size']} ({_format_bytes(summary['size'])})")
        print(f"cache_key={summary['cache_key'][:160]}")
        for result in store_results:
            _print_store(result, start, end)
        for label, meta in meta_results:
            _print_meta(label, meta)
        print(f"\noverall={summary['overall']}")

    return 0 if all_hit else 1


if __name__ == "__main__":
    raise SystemExit(main())
