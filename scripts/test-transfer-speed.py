#!/usr/bin/env python3
"""Benchmark download/upload speed using the worker transfer path.

Runs the same ``fetch_and_send_chunk`` call used by ``execute_transfer`` for a
BeamCore-style task offer (JSON) or CLI-built source/dest/range.

Usage:
  # Task JSON (worker offer shape)
  python3 scripts/test-transfer-speed.py --task /path/to/task.json

  # Build a task from CLI (adds Range header automatically)
  python3 scripts/test-transfer-speed.py \\
    --source 'https://.../file.bin?...' \\
    --dest 'https://.../dest.bin?...' \\
    --start 0 --end 41943039 \\
    --etag-required

  # Repeat and print averages
  python3 scripts/test-transfer-speed.py --task task.json --repeat 3

  # Machine-readable
  python3 scripts/test-transfer-speed.py --task task.json --json-out

Exit codes:
  0  all runs succeeded
  1  transfer failed
  2  usage / task validation error
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
import time
import uuid
from pathlib import Path
from typing import Any, Optional

_REPO = Path(__file__).resolve().parents[1]
if str(_REPO) not in sys.path:
    sys.path.insert(0, str(_REPO))

# Avoid worker bootstrap (dotenv / log redirect / sys.exit on missing env).
os.environ.setdefault("BEAM_SKIP_WORKER_BOOTSTRAP", "1")

import httpx  # noqa: E402

from neurons.worker.worker import (  # noqa: E402
    SEND_TIMEOUT,
    WorkerState,
    build_transfer_context,
    fetch_and_send_chunk,
    redact_url,
    transfer_mbps,
)


def _format_bytes(n: int) -> str:
    n = int(n)
    if n < 1024:
        return f"{n} B"
    if n < 1024**2:
        return f"{n / 1024:.1f} KiB"
    if n < 1024**3:
        return f"{n / (1024**2):.2f} MiB"
    return f"{n / (1024**3):.2f} GiB"


def _load_task_json(path: Path) -> dict:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise SystemExit(f"failed to read --task: {exc}") from exc
    if not isinstance(payload, dict):
        raise SystemExit("--task must be a JSON object")
    # Allow wrapping: {"task": {...}} or {"offer": {...}}
    for key in ("task", "offer", "message"):
        inner = payload.get(key)
        if isinstance(inner, dict) and (
            "source_url" in inner or "dest_url" in inner
        ):
            return inner
    return payload


def _build_task_from_args(args: argparse.Namespace) -> dict:
    source = str(args.source or "").strip()
    dest = str(args.dest or "").strip()
    if not source or not dest:
        raise SystemExit("need --task or both --source and --dest")
    if args.start is None or args.end is None:
        raise SystemExit("need --start and --end with --source/--dest")
    start = int(args.start)
    end = int(args.end)
    if end < start:
        raise SystemExit(f"invalid range: end < start ({start}-{end})")
    chunk_size = end - start + 1
    task: dict[str, Any] = {
        "task_id": args.task_id or f"speedtest-{uuid.uuid4()}",
        "offer_id": args.offer_id or f"speedtest-offer-{uuid.uuid4()}",
        "transfer_id": args.transfer_id or f"speedtest-xfer-{uuid.uuid4()}",
        "source_url": source,
        "dest_url": dest,
        "chunk_size": chunk_size,
        "source_headers": {"Range": f"bytes={start}-{end}"},
        "dest_headers": {},
        "etag_required": bool(args.etag_required),
    }
    if args.chunk_hash:
        task["chunk_hash"] = str(args.chunk_hash).strip()
    if args.total_size is not None:
        task["total_size"] = int(args.total_size)
    return task


def _resolve_task(args: argparse.Namespace) -> dict:
    if args.task is not None:
        task = _load_task_json(args.task)
        # Allow CLI overrides for ids / etag flag
        if args.task_id:
            task["task_id"] = args.task_id
        if args.offer_id:
            task["offer_id"] = args.offer_id
        if args.transfer_id:
            task["transfer_id"] = args.transfer_id
        if args.etag_required:
            task["etag_required"] = True
        if args.chunk_hash:
            task["chunk_hash"] = str(args.chunk_hash).strip()
        return task
    return _build_task_from_args(args)


async def _run_once(
    task: dict,
    *,
    timeout: float,
    log_prefix: str,
) -> dict[str, Any]:
    transfer_context, validation_error = build_transfer_context(task)
    if validation_error or transfer_context is None:
        return {
            "ok": False,
            "error": f"invalid_task:{validation_error or 'unknown'}",
        }

    task_id = str(task.get("task_id") or f"speedtest-{uuid.uuid4()}")
    offer_id = str(task.get("offer_id") or task_id)
    transfer_id = str(
        transfer_context.get("transfer_id") or task.get("transfer_id") or task_id
    )
    source_url = transfer_context["source_url"]
    dest_url = transfer_context["dest_url"]
    chunk_size = int(transfer_context["chunk_size"])
    range_start = int(transfer_context["range_start"])
    source_headers = transfer_context.get("source_headers") or {}
    dest_headers = transfer_context.get("dest_headers") or {}

    expected_hash = None
    if task.get("chunk_hash"):
        expected_hash = str(task["chunk_hash"])
    elif isinstance(task.get("chunk_hashes"), dict):
        expected_hash = task["chunk_hashes"].get(0) or task["chunk_hashes"].get("0")

    limits = httpx.Limits(max_connections=8, max_keepalive_connections=4)
    timeout_cfg = httpx.Timeout(timeout)
    state = WorkerState(api_url="", worker_id="speedtest")

    wall_started = time.perf_counter()
    try:
        async with httpx.AsyncClient(timeout=timeout_cfg, limits=limits) as client:
            state.http_client = client
            (
                bytes_transferred,
                chunk_hash,
                etag,
                response_code,
                fetch_ms,
                send_ms,
            ) = await fetch_and_send_chunk(
                client,
                source_url,
                dest_url,
                transfer_id,
                0,
                total_size=chunk_size,
                expected_max_bytes=chunk_size,
                expected_chunk_hash=expected_hash,
                task_id=task_id,
                offer_id=offer_id,
                extra_fetch_headers=source_headers or None,
                extra_dest_headers=dest_headers or None,
                send_chunk_offset=range_start,
                transfer_context=transfer_context,
                log_prefix=log_prefix,
            )
    except Exception as exc:
        wall_ms = (time.perf_counter() - wall_started) * 1000
        return {
            "ok": False,
            "error": f"{type(exc).__name__}: {exc}",
            "wall_ms": round(wall_ms, 1),
            "task_id": task_id,
            "offer_id": offer_id,
            "bytes": chunk_size,
            "src": redact_url(source_url),
            "dest": redact_url(dest_url),
            "range": f"{range_start}-{transfer_context['range_end']}",
        }

    wall_ms = (time.perf_counter() - wall_started) * 1000
    fetch_mbps = transfer_mbps(bytes_transferred, fetch_ms)
    send_mbps = transfer_mbps(bytes_transferred, send_ms)
    wall_mbps = transfer_mbps(bytes_transferred, wall_ms)
    ok = bytes_transferred == chunk_size
    error = None
    if not ok:
        error = f"bytes_mismatch: got {bytes_transferred} expected {chunk_size}"

    return {
        "ok": ok and error is None,
        "error": error,
        "task_id": task_id,
        "offer_id": offer_id,
        "src": redact_url(source_url),
        "dest": redact_url(dest_url),
        "range": f"{range_start}-{transfer_context['range_end']}",
        "bytes": bytes_transferred,
        "response_code": response_code,
        "chunk_hash": chunk_hash or "",
        "etag": etag or "",
        "fetch_ms": round(fetch_ms, 1),
        "send_ms": round(send_ms, 1),
        "wall_ms": round(wall_ms, 1),
        "fetch_mbps": round(fetch_mbps, 1),
        "send_mbps": round(send_mbps, 1),
        "wall_mbps": round(wall_mbps, 1),
        "etag_required": bool(transfer_context.get("etag_required")),
    }


def _print_result(idx: int, result: dict[str, Any]) -> None:
    label = f"run[{idx}]"
    if not result.get("ok"):
        print(
            f"{label} FAIL error={result.get('error')} "
            f"wall_ms={result.get('wall_ms', '-')}"
        )
        if result.get("src"):
            print(
                f"  src={result.get('src')} dest={result.get('dest')} "
                f"range={result.get('range')}"
            )
        return

    print(
        f"{label} OK bytes={result['bytes']} ({_format_bytes(result['bytes'])}) "
        f"range={result['range']}"
    )
    print(f"  src={result['src']}")
    print(f"  dest={result['dest']}")
    print(
        f"  fetch_ms={result['fetch_ms']:.1f} fetch_mbps={result['fetch_mbps']:.1f} | "
        f"send_ms={result['send_ms']:.1f} send_mbps={result['send_mbps']:.1f} | "
        f"wall_ms={result['wall_ms']:.1f} wall_mbps={result['wall_mbps']:.1f}"
    )
    print(
        f"  response={result.get('response_code')} etag={result.get('etag')!r} "
        f"hash={(result.get('chunk_hash') or '-')[:16]}…"
    )


def _avg(values: list[float]) -> float:
    return sum(values) / len(values) if values else 0.0


async def _amain(args: argparse.Namespace) -> int:
    try:
        task = _resolve_task(args)
    except SystemExit as exc:
        print(str(exc), file=sys.stderr)
        return 2

    transfer_context, validation_error = build_transfer_context(task)
    if validation_error or transfer_context is None:
        print(f"invalid task: {validation_error}", file=sys.stderr)
        return 2

    if args.dry_run:
        print("dry_run ok")
        print(
            f"  src={redact_url(transfer_context['source_url'])} "
            f"dest={redact_url(transfer_context['dest_url'])}"
        )
        print(
            f"  range={transfer_context['range_start']}-{transfer_context['range_end']} "
            f"bytes={transfer_context['chunk_size']} "
            f"({_format_bytes(int(transfer_context['chunk_size']))})"
        )
        print(f"  etag_required={bool(transfer_context.get('etag_required'))}")
        return 0

    repeat = max(1, int(args.repeat))
    results: list[dict[str, Any]] = []
    for i in range(repeat):
        result = await _run_once(
            task,
            timeout=float(args.timeout),
            log_prefix="[SpeedTest]",
        )
        results.append(result)
        if not args.json_out:
            _print_result(i + 1, result)
        if not result.get("ok") and not args.continue_on_error:
            break

    ok_results = [r for r in results if r.get("ok")]
    summary = {
        "runs": len(results),
        "ok": len(ok_results),
        "failed": len(results) - len(ok_results),
        "bytes": int(transfer_context["chunk_size"]),
        "src": redact_url(transfer_context["source_url"]),
        "dest": redact_url(transfer_context["dest_url"]),
        "range": (
            f"{transfer_context['range_start']}-{transfer_context['range_end']}"
        ),
        "results": results,
    }
    if ok_results:
        summary["avg_fetch_ms"] = round(_avg([r["fetch_ms"] for r in ok_results]), 1)
        summary["avg_send_ms"] = round(_avg([r["send_ms"] for r in ok_results]), 1)
        summary["avg_wall_ms"] = round(_avg([r["wall_ms"] for r in ok_results]), 1)
        summary["avg_fetch_mbps"] = round(
            _avg([r["fetch_mbps"] for r in ok_results]), 1
        )
        summary["avg_send_mbps"] = round(_avg([r["send_mbps"] for r in ok_results]), 1)
        summary["avg_wall_mbps"] = round(
            _avg([r["wall_mbps"] for r in ok_results]), 1
        )

    if args.json_out:
        print(json.dumps(summary, indent=2))
    elif ok_results and repeat > 1:
        print(
            f"\navg over {len(ok_results)} ok run(s): "
            f"fetch={summary['avg_fetch_ms']:.1f}ms ({summary['avg_fetch_mbps']:.1f} Mbps) "
            f"send={summary['avg_send_ms']:.1f}ms ({summary['avg_send_mbps']:.1f} Mbps) "
            f"wall={summary['avg_wall_ms']:.1f}ms ({summary['avg_wall_mbps']:.1f} Mbps)"
        )

    return 0 if len(ok_results) == len(results) and results else 1


def main() -> int:
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--task",
        type=Path,
        default=None,
        help="path to worker task/offer JSON (source_url, dest_url, chunk_size, source_headers.Range, …)",
    )
    parser.add_argument("--source", default="", help="source URL (signed GET)")
    parser.add_argument("--dest", default="", help="destination URL (signed PUT)")
    parser.add_argument("--start", type=int, default=None, help="inclusive byte start")
    parser.add_argument("--end", type=int, default=None, help="inclusive byte end")
    parser.add_argument("--total-size", type=int, default=None, help="optional object size")
    parser.add_argument("--task-id", default="", help="override task_id")
    parser.add_argument("--offer-id", default="", help="override offer_id")
    parser.add_argument("--transfer-id", default="", help="override transfer_id")
    parser.add_argument(
        "--chunk-hash",
        default="",
        help="optional expected sha256 (enables worker hash check)",
    )
    parser.add_argument(
        "--etag-required",
        action="store_true",
        help="set etag_required=true on the task (predefined-etag eligibility)",
    )
    parser.add_argument(
        "--repeat",
        type=int,
        default=1,
        help="number of transfer runs (default 1)",
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=float(SEND_TIMEOUT),
        help=f"httpx timeout seconds (default {SEND_TIMEOUT})",
    )
    parser.add_argument(
        "--continue-on-error",
        action="store_true",
        help="keep repeating after a failed run",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="validate task / print plan without transferring",
    )
    parser.add_argument(
        "--json-out",
        action="store_true",
        help="print JSON summary",
    )
    args = parser.parse_args()

    if args.task is None and not (args.source and args.dest):
        parser.error("need --task or --source/--dest/--start/--end")

    return asyncio.run(_amain(args))


if __name__ == "__main__":
    raise SystemExit(main())
