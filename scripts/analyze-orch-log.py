#!/usr/bin/env python3
"""Parse orchestrator logs and export task_done rows to CSV.

Extracts from ``_workers | task_done`` lines (WorkerGateway):
  worker_id, src, dest, wall_ms

Usage:
  python3 scripts/analyze-orch-log.py logs/orchestrators/orch5.log
  python3 scripts/analyze-orch-log.py logs/orchestrators/orch5.log -o /tmp/tasks.csv
  python3 scripts/analyze-orch-log.py logs/orchestrators/*.log -o tasks.csv
  # Also emit timestamp / send_ms / chunk_id / mbps / dest_group:
  python3 scripts/analyze-orch-log.py orch5.log --extra -o tasks.csv
  # Aggregate avg Mbps by worker × dest_group (backup/primary/…):
  python3 scripts/analyze-orch-log.py orch5.log --avg-by-worker-dest -o avg.csv

Exit codes:
  0  wrote CSV (possibly empty)
  2  usage / no input files
"""

from __future__ import annotations

import argparse
import csv
import re
import sys
from pathlib import Path
from typing import Iterable, Iterator, Optional

# 2026-07-28 21:49:25.239 | INFO | ... | _workers | task_done ... worker=... src=... dest=... wall_ms=4501.1
_TASK_DONE_RE = re.compile(
    r"^(?P<ts>\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}(?:\.\d+)?)\s*\|"
    r".*?\btask_done\b"
    r".*?\bworker=(?P<worker>\S+)"
    r".*?\bsrc=(?P<src>\S+)"
    r".*?\bdest=(?P<dest>\S+)"
    r".*?\bwall_ms=(?P<wall_ms>[\d.]+)"
)

_SEND_MS_RE = re.compile(r"\bsend_ms=([\d.]+)")
_CHUNK_ID_RE = re.compile(r"\bchunk_id=(\d+|\?)")
_TASK_ID_RE = re.compile(r"\btask=(\S+)")
_RANGE_BYTES_RE = re.compile(r"\brange=bytes=\d+-\d+\((\d+)\)")


def dest_group(dest: str) -> str:
    """Affinity key: host/…/destinations/<group> (matches orch dest_group_from_url)."""
    try:
        from urllib.parse import urlsplit

        parsed = urlsplit(dest)
        host = (parsed.hostname or "").lower()
        parts = [p for p in parsed.path.split("/") if p]
        if "destinations" in parts:
            i = parts.index("destinations")
            if i + 1 < len(parts):
                prefix = "/".join(parts[: i + 2])
                if host:
                    return f"{host}/{prefix}"
                return prefix
        if host:
            return host
    except Exception:
        pass
    return "?"


def _iter_paths(inputs: list[str]) -> list[Path]:
    paths: list[Path] = []
    for raw in inputs:
        p = Path(raw)
        if p.is_file():
            paths.append(p)
            continue
        # Allow shell-expanded globs that somehow arrive as a single pattern.
        matched = sorted(Path().glob(raw)) if any(ch in raw for ch in "*?[") else []
        paths.extend(m for m in matched if m.is_file())
    # De-dupe preserving order
    seen: set[Path] = set()
    out: list[Path] = []
    for p in paths:
        rp = p.resolve()
        if rp in seen:
            continue
        seen.add(rp)
        out.append(p)
    return out


def parse_task_done_line(line: str) -> Optional[dict[str, str]]:
    if "task_done" not in line or "worker=" not in line:
        return None
    m = _TASK_DONE_RE.search(line)
    if not m:
        return None
    dest = m.group("dest")
    wall_ms = m.group("wall_ms")
    row = {
        "worker_id": m.group("worker").rstrip("."),
        "src": m.group("src"),
        "dest": dest,
        "dest_group": dest_group(dest),
        "wall_ms": wall_ms,
        "timestamp": m.group("ts"),
    }
    send = _SEND_MS_RE.search(line)
    if send:
        row["send_ms"] = send.group(1)
    chunk = _CHUNK_ID_RE.search(line)
    if chunk:
        row["chunk_id"] = chunk.group(1)
    task = _TASK_ID_RE.search(line)
    if task:
        row["task_id"] = task.group(1).rstrip(".")
    nbytes = _RANGE_BYTES_RE.search(line)
    if nbytes:
        row["bytes"] = nbytes.group(1)
        try:
            wall = float(wall_ms)
            n = int(nbytes.group(1))
            row["mbps"] = f"{(n * 8 / wall / 1000):.1f}" if wall > 0 else ""
        except (TypeError, ValueError, ZeroDivisionError):
            row["mbps"] = ""
    return row


def iter_task_done_rows(paths: Iterable[Path]) -> Iterator[dict[str, str]]:
    for path in paths:
        with path.open("r", encoding="utf-8", errors="replace") as handle:
            for line in handle:
                row = parse_task_done_line(line)
                if row:
                    yield row


def main(argv: Optional[list[str]] = None) -> int:
    parser = argparse.ArgumentParser(
        description="Export orch task_done worker_id/src/dest/wall_ms to CSV"
    )
    parser.add_argument(
        "logs",
        nargs="+",
        help="Orchestrator log file(s)",
    )
    parser.add_argument(
        "-o",
        "--output",
        default="-",
        help="Output CSV path (default: stdout)",
    )
    parser.add_argument(
        "--extra",
        action="store_true",
        help="Also include timestamp, send_ms, chunk_id, task_id, bytes, mbps, dest_group",
    )
    parser.add_argument(
        "--avg-by-worker-dest",
        action="store_true",
        help="Aggregate CSV: worker_id, dest_group, n, avg_mbps, min_mbps, max_mbps",
    )
    args = parser.parse_args(argv)

    paths = _iter_paths(args.logs)
    if not paths:
        print("No log files found.", file=sys.stderr)
        return 2

    rows = list(iter_task_done_rows(paths))

    out_fh = (
        sys.stdout
        if args.output == "-"
        else open(args.output, "w", encoding="utf-8", newline="")
    )
    try:
        if args.avg_by_worker_dest:
            from collections import defaultdict

            buckets: dict[tuple[str, str], list[float]] = defaultdict(list)
            for row in rows:
                try:
                    mbps = float(row.get("mbps") or 0.0)
                except ValueError:
                    continue
                if mbps <= 0:
                    continue
                buckets[(row["worker_id"], row.get("dest_group") or "?")].append(mbps)
            fields = [
                "worker_id",
                "dest_group",
                "n",
                "avg_mbps",
                "min_mbps",
                "max_mbps",
            ]
            writer = csv.DictWriter(out_fh, fieldnames=fields)
            writer.writeheader()
            for (wid, group), vals in sorted(
                buckets.items(), key=lambda kv: (kv[0][0], kv[0][1])
            ):
                writer.writerow(
                    {
                        "worker_id": wid,
                        "dest_group": group,
                        "n": len(vals),
                        "avg_mbps": f"{sum(vals) / len(vals):.1f}",
                        "min_mbps": f"{min(vals):.1f}",
                        "max_mbps": f"{max(vals):.1f}",
                    }
                )
        else:
            base_fields = ["worker_id", "src", "dest", "wall_ms"]
            extra_fields = [
                "timestamp",
                "send_ms",
                "chunk_id",
                "task_id",
                "bytes",
                "mbps",
                "dest_group",
            ]
            fields = base_fields + (extra_fields if args.extra else [])
            writer = csv.DictWriter(out_fh, fieldnames=fields, extrasaction="ignore")
            writer.writeheader()
            for row in rows:
                writer.writerow({k: row.get(k, "") for k in fields})
    finally:
        if out_fh is not sys.stdout:
            out_fh.close()

    print(
        f"# parsed {len(rows)} task_done row(s) from {len(paths)} file(s)",
        file=sys.stderr,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
