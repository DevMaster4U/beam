#!/usr/bin/env python3
"""Merge / re-pack range_data segments into continuous ≤1 GiB files.

Task ingest already merges+packs automatically. Use this script to compact
existing stores that were written before auto-merge, or to re-pack oversized
segment files.

Usage:
  # Dry-run: show what would change (control-server cache)
  python3 scripts/merge.py

  # Apply merges / 1 GiB packing
  python3 scripts/merge.py --apply

  # Worker local mirror
  python3 scripts/merge.py --root logs/workers/predefined_etag_range_data --apply

  # One source only
  python3 scripts/merge.py --source 'https://.../ecx_20_b_gb.bin' --apply
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

_REPO = Path(__file__).resolve().parents[1]
if str(_REPO) not in sys.path:
    sys.path.insert(0, str(_REPO))

from neurons.common.byte_range_store import (
    ByteRangeStore,
    MAX_SEGMENT_BYTES,
    source_digest,
)


def _default_root() -> Path:
    return _REPO / "data" / "control-server" / "cache" / "range_data"


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--root",
        type=Path,
        default=None,
        help="range_data root (default: data/control-server/cache/range_data)",
    )
    parser.add_argument(
        "--source",
        type=str,
        default="",
        help="Optional source_url to merge (default: all sources under root)",
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Write merged files and update segments.json (default: dry-run)",
    )
    args = parser.parse_args()

    root = args.root or _default_root()
    if not root.is_dir():
        print(f"range_data root missing: {root}", file=sys.stderr)
        return 1

    store = ByteRangeStore(root)
    sources = [args.source.strip()] if args.source.strip() else store.list_sources()
    if not sources:
        print("no sources found")
        return 0

    print(f"root={root}")
    print(f"max_segment_bytes={MAX_SEGMENT_BYTES} ({MAX_SEGMENT_BYTES / (1 << 30):.2f} GiB)")
    print(f"sources={len(sources)} apply={args.apply}")

    for source_url in sources:
        digest = source_digest(source_url)
        segs = store.list_segments(source_url)
        print(f"\n{digest}  {source_url.split('/')[-1]}")
        print(f"  segments_before={len(segs)}")
        oversized = sum(1 for s in segs if s.size > store.max_segment_bytes)
        if oversized:
            print(f"  oversized_segments={oversized}")

        if not args.apply:
            groups = 0
            if segs:
                cur_hi = segs[0].end
                group_size = 1
                for seg in segs[1:]:
                    if seg.start <= cur_hi + 1:
                        group_size += 1
                        cur_hi = max(cur_hi, seg.end)
                    else:
                        if group_size > 1:
                            groups += 1
                        group_size = 1
                        cur_hi = seg.end
                if group_size > 1:
                    groups += 1
            print(
                f"  mergeable_groups={groups} oversized={oversized} "
                f"(dry-run; pass --apply to compact/pack)"
            )
            continue

        result = store.merge_source(source_url)
        print(
            f"  segments_after={result['after']} "
            f"merged_groups={result['merged_groups']}"
        )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
