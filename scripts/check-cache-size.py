#!/usr/bin/env python3
"""Report size and gaps of cached range_data.

Walks ``range_data/<digest>/`` and prints per-source covered bytes, on-disk
size, span, and holes inside that span (or an optional query span).

Usage:
  # Control-server cache (default)
  python3 scripts/check-cache-size.py

  # Worker local store
  python3 scripts/check-cache-size.py --worker

  # Both roots
  python3 scripts/check-cache-size.py --both

  # One source (URL or digest prefix)
  python3 scripts/check-cache-size.py --source 'https://.../file.bin'
  python3 scripts/check-cache-size.py --digest 7aaf29d2

  # Gaps relative to an expected span (e.g. full object)
  python3 scripts/check-cache-size.py --source 'https://.../file.bin' \\
    --span-start 0 --span-end 21474836479

  # Machine-readable
  python3 scripts/check-cache-size.py --json-out

Exit codes:
  0  ok (even if gaps exist)
  2  usage / missing root
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any, Optional
from urllib.parse import urlsplit, urlunsplit

_REPO = Path(__file__).resolve().parents[1]
if str(_REPO) not in sys.path:
    sys.path.insert(0, str(_REPO))

from neurons.common.byte_range_store import (  # noqa: E402
    ByteRangeStore,
    normalize_source_url,
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


def _format_bytes(n: int) -> str:
    n = int(n)
    if n < 1024:
        return f"{n} B"
    if n < 1024**2:
        return f"{n / 1024:.1f} KiB"
    if n < 1024**3:
        return f"{n / (1024**2):.2f} MiB"
    return f"{n / (1024**3):.2f} GiB"


def _dir_disk_bytes(path: Path) -> int:
    total = 0
    if not path.is_dir():
        return 0
    for child in path.rglob("*"):
        if child.is_file():
            try:
                total += child.stat().st_size
            except OSError:
                continue
    return total


def _load_index(source_dir: Path) -> dict[str, Any]:
    index = source_dir / "segments.json"
    if not index.is_file():
        return {}
    try:
        data = json.loads(index.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return data if isinstance(data, dict) else {}


def _report_source(
    store: ByteRangeStore,
    source_url: str,
    *,
    span_start: Optional[int],
    span_end: Optional[int],
    max_gaps: int,
) -> dict[str, Any]:
    source_url = normalize_source_url(source_url)
    digest = source_digest(source_url)
    source_dir = store.source_dir(source_url)
    segments = store.list_segments(source_url)

    span_s = span_start
    span_e = span_end
    if segments and span_s is None and span_e is None:
        span_s = segments[0].start
        span_e = segments[-1].end

    report = store.coverage_report(source_url, span_start=span_s, span_end=span_e)
    covered = int(report.get("covered_bytes") or 0)
    gaps = list(report.get("gaps") or [])
    gap_bytes = int(report.get("gap_bytes") or 0)
    disk = _dir_disk_bytes(source_dir)

    span_size = 0
    if span_s is not None and span_e is not None and span_e >= span_s:
        span_size = span_e - span_s + 1

    coverage_pct = (100.0 * (span_size - gap_bytes) / span_size) if span_size else 0.0

    return {
        "source_url": source_url,
        "digest": digest,
        "source_dir": str(source_dir),
        "segment_count": len(segments),
        "covered_bytes": covered,
        "disk_bytes": disk,
        "span_start": span_s,
        "span_end": span_e,
        "span_bytes": span_size,
        "gap_count": len(gaps),
        "gap_bytes": gap_bytes,
        "coverage_pct": round(coverage_pct, 3),
        "gaps": gaps[:max_gaps],
        "gaps_truncated": max(0, len(gaps) - max_gaps),
    }


def _scan_root(
    label: str,
    root: Path,
    *,
    source_filter: str,
    digest_filter: str,
    span_start: Optional[int],
    span_end: Optional[int],
    max_gaps: int,
    include_segments: bool,
    include_orphans: bool,
) -> dict[str, Any]:
    out: dict[str, Any] = {
        "label": label,
        "root": str(root),
        "exists": root.is_dir(),
        "sources": [],
        "orphans": [],
        "totals": {
            "sources": 0,
            "segments": 0,
            "covered_bytes": 0,
            "disk_bytes": 0,
            "gap_bytes": 0,
            "gap_count": 0,
        },
    }
    if not root.is_dir():
        return out

    store = ByteRangeStore(root)
    src_filter = _normalize_source(source_filter) if source_filter.strip() else ""
    dig_filter = digest_filter.strip().lower()

    sources = store.list_sources()
    if src_filter:
        sources = [s for s in sources if s == src_filter or src_filter in s]
    if dig_filter:
        sources = [s for s in sources if source_digest(s).startswith(dig_filter)]

    for src in sources:
        item = _report_source(
            store,
            src,
            span_start=span_start,
            span_end=span_end,
            max_gaps=max_gaps,
        )
        if include_segments:
            item["segments"] = [
                {"start": s.start, "end": s.end, "file": s.file, "size": s.size}
                for s in store.list_segments(src)
            ]
        else:
            item.pop("segments", None)
        out["sources"].append(item)
        totals = out["totals"]
        totals["sources"] += 1
        totals["segments"] += int(item["segment_count"])
        totals["covered_bytes"] += int(item["covered_bytes"])
        totals["disk_bytes"] += int(item["disk_bytes"])
        totals["gap_bytes"] += int(item["gap_bytes"])
        totals["gap_count"] += int(item["gap_count"])

    if include_orphans:
        known = {source_digest(s) for s in store.list_sources()}
        for child in sorted(root.iterdir()):
            if not child.is_dir() or child.name.startswith("."):
                continue
            if dig_filter and not child.name.startswith(dig_filter):
                continue
            index = _load_index(child)
            src = normalize_source_url(str(index.get("source_url") or "").strip())
            expected = source_digest(src) if src else ""
            is_orphan = (not src) or (child.name != expected) or (child.name not in known)
            if not is_orphan:
                continue
            if src_filter and src and src != src_filter and src_filter not in src:
                continue
            disk = _dir_disk_bytes(child)
            segs = index.get("segments") if isinstance(index.get("segments"), list) else []
            out["orphans"].append(
                {
                    "digest_dir": child.name,
                    "source_url": src or None,
                    "expected_digest": expected or None,
                    "segment_count": len(segs),
                    "disk_bytes": disk,
                }
            )
            out["totals"]["disk_bytes"] += disk

    return out


def _print_root(report: dict[str, Any], *, verbose: bool) -> None:
    print(f"\n[{report['label']}] root={report['root']}")
    if not report.get("exists"):
        print("  (range_data root missing)")
        return

    totals = report["totals"]
    print(
        f"  sources={totals['sources']} segments={totals['segments']} "
        f"covered={_format_bytes(totals['covered_bytes'])} "
        f"disk={_format_bytes(totals['disk_bytes'])} "
        f"gaps={totals['gap_count']} ({_format_bytes(totals['gap_bytes'])})"
    )

    for item in report.get("sources") or []:
        src = str(item.get("source_url") or "")
        short = src if len(src) <= 80 else src[:77] + "…"
        print(f"\n  source={short}")
        print(f"    digest={item.get('digest')}")
        span_s, span_e = item.get("span_start"), item.get("span_end")
        span_txt = (
            f"[{span_s},{span_e}] ({_format_bytes(int(item.get('span_bytes') or 0))})"
            if span_s is not None and span_e is not None
            else "(empty)"
        )
        print(
            f"    segments={item.get('segment_count')} "
            f"covered={_format_bytes(int(item.get('covered_bytes') or 0))} "
            f"disk={_format_bytes(int(item.get('disk_bytes') or 0))}"
        )
        print(
            f"    span={span_txt} "
            f"coverage={item.get('coverage_pct')}% "
            f"gaps={item.get('gap_count')} ({_format_bytes(int(item.get('gap_bytes') or 0))})"
        )
        gaps = item.get("gaps") or []
        for gap in gaps:
            print(
                f"    gap  {gap['start']}-{gap['end']} "
                f"({_format_bytes(gap['size'])})"
            )
        trunc = int(item.get("gaps_truncated") or 0)
        if trunc:
            print(f"    ... {trunc} more gaps")
        if verbose:
            for seg in item.get("segments") or []:
                print(
                    f"    seg  {seg['start']}-{seg['end']} "
                    f"file={seg['file']} ({_format_bytes(seg['size'])})"
                )

    orphans = report.get("orphans") or []
    if orphans:
        print(f"\n  orphans={len(orphans)}")
        for o in orphans:
            print(
                f"    dir={o.get('digest_dir')} "
                f"disk={_format_bytes(int(o.get('disk_bytes') or 0))} "
                f"segments={o.get('segment_count')} "
                f"source={str(o.get('source_url') or '')[:60]!r}"
            )


def main() -> int:
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
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
        help="scan control-server cache (default)",
    )
    parser.add_argument(
        "--worker",
        action="store_true",
        help="scan worker local range_data under logs/workers/",
    )
    parser.add_argument(
        "--both",
        action="store_true",
        help="scan both control-server and worker roots",
    )
    parser.add_argument(
        "--source",
        default="",
        help="filter by source URL (exact or substring; query stripped)",
    )
    parser.add_argument(
        "--digest",
        default="",
        help="filter by digest prefix (32-hex dir name)",
    )
    parser.add_argument(
        "--span-start",
        type=int,
        default=None,
        help="inclusive start for gap analysis (default: min segment start)",
    )
    parser.add_argument(
        "--span-end",
        type=int,
        default=None,
        help="inclusive end for gap analysis (default: max segment end)",
    )
    parser.add_argument(
        "--max-gaps",
        type=int,
        default=20,
        help="max gaps to list per source (default 20)",
    )
    parser.add_argument(
        "--orphans",
        action="store_true",
        help="also list digest dirs that are non-canonical / missing source_url",
    )
    parser.add_argument(
        "--segments",
        action="store_true",
        help="include segment list in output",
    )
    parser.add_argument(
        "--json-out",
        action="store_true",
        help="print machine-readable JSON",
    )
    args = parser.parse_args()

    if (args.span_start is None) ^ (args.span_end is None):
        print("need both --span-start and --span-end (or neither)", file=sys.stderr)
        return 2
    if (
        args.span_start is not None
        and args.span_end is not None
        and args.span_end < args.span_start
    ):
        print(
            f"invalid span: end < start ({args.span_start}-{args.span_end})",
            file=sys.stderr,
        )
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
        roots.append(("control", _default_control_root()))

    reports = [
        _scan_root(
            label,
            root,
            source_filter=args.source,
            digest_filter=args.digest,
            span_start=args.span_start,
            span_end=args.span_end,
            max_gaps=max(0, args.max_gaps),
            include_segments=args.segments,
            include_orphans=args.orphans,
        )
        for label, root in roots
    ]

    if not any(r.get("exists") for r in reports):
        for r in reports:
            print(f"missing root: {r['root']}", file=sys.stderr)
        return 2

    if args.json_out:
        print(json.dumps({"roots": reports}, indent=2))
    else:
        for report in reports:
            _print_root(report, verbose=args.segments)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
