#!/usr/bin/env python3
"""Compact legacy per-key chunk_data/*.bin into continuous range_data segments.

Dry-run by default. Pass --apply to write range_data and optionally --delete-legacy
to remove old chunk_data files after verification.

Usage:
  python3 scripts/compact-chunk-data-to-ranges.py
  python3 scripts/compact-chunk-data-to-ranges.py --apply
  python3 scripts/compact-chunk-data-to-ranges.py --apply --delete-legacy
"""

from __future__ import annotations

import argparse
import hashlib
import json
import random
import sys
from collections import defaultdict
from pathlib import Path

_REPO = Path(__file__).resolve().parents[1]
if str(_REPO) not in sys.path:
    sys.path.insert(0, str(_REPO))

from neurons.common.byte_range_store import ByteRangeStore, merge_intervals, parse_cache_key_range


def _default_paths() -> tuple[Path, Path, Path]:
    cache = _REPO / "data" / "control-server" / "cache"
    return (
        cache / "predefined_etag_chunks.json",
        cache / "chunk_data",
        cache / "range_data",
    )


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--json", type=Path, default=None, help="metadata JSON path")
    parser.add_argument("--chunk-dir", type=Path, default=None, help="legacy chunk_data dir")
    parser.add_argument("--range-dir", type=Path, default=None, help="range_data output dir")
    parser.add_argument("--apply", action="store_true", help="write range_data")
    parser.add_argument(
        "--delete-legacy",
        action="store_true",
        help="delete legacy chunk_data/*.bin after successful apply+verify",
    )
    parser.add_argument("--sample", type=int, default=20, help="verification samples")
    args = parser.parse_args()

    json_path, chunk_dir, range_dir = _default_paths()
    if args.json:
        json_path = args.json
    if args.chunk_dir:
        chunk_dir = args.chunk_dir
    if args.range_dir:
        range_dir = args.range_dir

    if not json_path.is_file():
        print(f"missing metadata: {json_path}", file=sys.stderr)
        return 1

    payload = json.loads(json_path.read_text(encoding="utf-8"))
    entries = payload.get("entries") or {}

    by_source: dict[str, list[tuple[int, int, str, Path]]] = defaultdict(list)
    missing_files = 0
    legacy_bytes = 0
    for key, item in entries.items():
        parsed = parse_cache_key_range(str(key))
        if parsed is None:
            continue
        source, start, end = parsed
        digest = hashlib.sha256(str(key).encode()).hexdigest()
        path = chunk_dir / f"{digest}.bin"
        if not path.is_file() or path.stat().st_size <= 0:
            missing_files += 1
            continue
        size = path.stat().st_size
        legacy_bytes += size
        by_source[source].append((start, end, str(key), path))

    print(f"metadata entries={len(entries)}")
    print(f"sources={len(by_source)} legacy_files_with_data={sum(len(v) for v in by_source.values())}")
    print(f"missing_or_empty_legacy_files={missing_files}")
    print(f"legacy_disk_bytes={legacy_bytes} ({legacy_bytes/1e9:.3f} GB)")

    unique_bytes = 0
    merged_segments = 0
    for source, items in by_source.items():
        ranges = [(s, e) for s, e, _, _ in items]
        merged = merge_intervals(ranges)
        unique_bytes += sum(e - s + 1 for s, e in merged)
        merged_segments += len(merged)
        print(
            f"  {source.split('/')[-1]}: files={len(items)} "
            f"merged_segments={len(merged)} "
            f"unique={sum(e-s+1 for s,e in merged)/1e9:.3f}GB"
        )

    print(f"unique_merged_bytes={unique_bytes} ({unique_bytes/1e9:.3f} GB)")
    print(
        f"duplication_waste={(legacy_bytes - unique_bytes)/1e9:.3f} GB "
        f"({100*(legacy_bytes-unique_bytes)/max(legacy_bytes,1):.1f}%)"
    )
    print(f"expected_segment_files≈{merged_segments}")

    if not args.apply:
        print("dry-run only; re-run with --apply to write range_data")
        return 0

    store = ByteRangeStore(range_dir)
    ingested = 0
    # Ingest largest-first within each source so later overlaps overwrite with
    # newer? Actually last-write-wins per ingest order — sort by start then size.
    for source, items in by_source.items():
        items_sorted = sorted(items, key=lambda x: (x[0], -(x[1] - x[0])))
        for start, end, key, path in items_sorted:
            expected = end - start + 1
            actual = path.stat().st_size
            if actual != expected:
                print(
                    f"WARN size mismatch key={key[:80]} expected={expected} actual={actual}; skipping"
                )
                continue
            store.ingest_from_file(source, start, end, path)
            ingested += 1
            if ingested % 50 == 0:
                print(f"  ingested {ingested}...")

    print(f"ingested={ingested}")

    # Verify random samples against legacy files.
    samples = []
    for source, items in by_source.items():
        samples.extend((source, s, e, p) for s, e, _, p in items)
    random.shuffle(samples)
    samples = samples[: max(0, args.sample)]
    mismatches = 0
    for source, start, end, path in samples:
        legacy = path.read_bytes()
        got = store.read_slice(source, start, end)
        if got != legacy:
            mismatches += 1
            print(f"VERIFY FAIL {source.split('/')[-1]} {start}-{end}")
    print(f"verified_samples={len(samples)} mismatches={mismatches}")
    if mismatches:
        print("refusing --delete-legacy due to verification failures", file=sys.stderr)
        return 2

    after_bytes = 0
    after_files = 0
    if range_dir.is_dir():
        for path in range_dir.rglob("*.bin"):
            after_bytes += path.stat().st_size
            after_files += 1
    print(f"range_data_files={after_files} range_data_bytes={after_bytes/1e9:.3f} GB")

    if args.delete_legacy:
        removed = 0
        for path in chunk_dir.glob("*.bin"):
            if path.is_file():
                path.unlink()
                removed += 1
        print(f"deleted_legacy_files={removed}")

    # Refresh has_chunk_data flags via covers().
    try:
        sys.path.insert(0, str(_REPO / "control-server"))
        from storage import (  # type: ignore
            has_predefined_etag_chunk_data,
            load_predefined_etag_cache,
            save_predefined_etag_cache,
        )

        payload = load_predefined_etag_cache()
        entries = payload.get("entries") or {}
        changed = 0
        for key, item in entries.items():
            if not isinstance(item, dict):
                continue
            covered = has_predefined_etag_chunk_data(str(key))
            if covered and not item.get("has_chunk_data"):
                item["has_chunk_data"] = True
                changed += 1
            elif not covered and item.pop("has_chunk_data", None) is not None:
                changed += 1
        if changed:
            save_predefined_etag_cache(payload)
            print(f"updated_metadata_flags={changed}")
    except Exception as exc:
        print(f"metadata flag refresh skipped: {exc}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
