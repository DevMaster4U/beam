#!/usr/bin/env python3
"""Fetch only (no upload) for a BeamCore worker task offer; print fetch_ms.

Uses the same source URL + Range headers as a live worker GET.

Usage:
  python3 scripts/run-worker-task.py --task scripts/fixtures/sample-task-offer.json
  python3 scripts/run-worker-task.py --task - <<'EOF'
  { "source_url": "...", "dest_url": "...", "chunk_size": N, "source_headers": {"Range": "bytes=..."} }
  EOF

Exit codes:
  0  fetch succeeded
  1  fetch failed
  2  usage / validation error
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
import time
from pathlib import Path
from typing import Any

_REPO = Path(__file__).resolve().parents[1]
if str(_REPO) not in sys.path:
    sys.path.insert(0, str(_REPO))

os.environ.setdefault("BEAM_SKIP_WORKER_BOOTSTRAP", "1")

import httpx  # noqa: E402

from neurons.worker.worker import (  # noqa: E402
    FETCH_STREAM_CHUNK_SIZE,
    FETCH_TIMEOUT,
    build_transfer_context,
)


def _load_task(path: str) -> dict:
    if path == "-":
        raw = sys.stdin.read()
    else:
        try:
            raw = Path(path).read_text(encoding="utf-8")
        except OSError as exc:
            raise SystemExit(f"failed to read --task: {exc}") from exc
    try:
        payload = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise SystemExit(f"invalid JSON: {exc}") from exc
    if not isinstance(payload, dict):
        raise SystemExit("task must be a JSON object")
    for key in ("task", "offer", "message"):
        inner = payload.get(key)
        if isinstance(inner, dict) and (
            "source_url" in inner or "dest_url" in inner
        ):
            return inner
    return payload


async def _fetch_only(task: dict, *, timeout: float) -> dict[str, Any]:
    transfer_context, validation_error = build_transfer_context(task)
    if validation_error or transfer_context is None:
        return {"ok": False, "error": f"invalid_task:{validation_error or 'unknown'}"}

    source_url = str(transfer_context["source_url"])
    chunk_size = int(transfer_context["chunk_size"])
    source_headers = dict(transfer_context.get("source_headers") or {})
    # Match worker fetch headers.
    fetch_headers = {"ngrok-skip-browser-warning": "true", **source_headers}

    started = time.perf_counter()
    bytes_read = 0
    try:
        async with httpx.AsyncClient(timeout=httpx.Timeout(timeout)) as client:
            async with client.stream(
                "GET", source_url, headers=fetch_headers, timeout=FETCH_TIMEOUT
            ) as response:
                if response.status_code not in (200, 206):
                    response.raise_for_status()
                async for part in response.aiter_bytes(chunk_size=FETCH_STREAM_CHUNK_SIZE):
                    bytes_read += len(part)
                    if bytes_read > chunk_size:
                        raise ValueError(
                            f"response exceeded expected size: "
                            f"{bytes_read} > {chunk_size}"
                        )
    except Exception as exc:
        fetch_ms = (time.perf_counter() - started) * 1000
        return {
            "ok": False,
            "error": f"{type(exc).__name__}: {exc}",
            "fetch_ms": round(fetch_ms, 1),
            "bytes": bytes_read,
        }

    fetch_ms = (time.perf_counter() - started) * 1000
    ok = bytes_read == chunk_size
    return {
        "ok": ok,
        "error": None
        if ok
        else f"bytes_mismatch: got {bytes_read} expected {chunk_size}",
        "fetch_ms": round(fetch_ms, 1),
        "bytes": bytes_read,
    }


def main() -> int:
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--task",
        required=True,
        help="path to task/offer JSON, or '-' for stdin",
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=float(FETCH_TIMEOUT),
        help=f"httpx timeout seconds (default {FETCH_TIMEOUT})",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="validate offer only (no fetch)",
    )
    args = parser.parse_args()

    try:
        task = _load_task(args.task)
    except SystemExit as exc:
        print(str(exc), file=sys.stderr)
        return 2

    transfer_context, validation_error = build_transfer_context(task)
    if validation_error or transfer_context is None:
        print(f"invalid task: {validation_error}", file=sys.stderr)
        return 2

    if args.dry_run:
        print(
            f"dry_run ok range={transfer_context['range_start']}-"
            f"{transfer_context['range_end']} bytes={transfer_context['chunk_size']}",
            file=sys.stderr,
        )
        return 0

    result = asyncio.run(_fetch_only(task, timeout=float(args.timeout)))
    if not result.get("ok"):
        print(
            f"FAIL error={result.get('error')} fetch_ms={result.get('fetch_ms', '-')}",
            file=sys.stderr,
        )
        return 1

    print(f"fetch_ms={result['fetch_ms']:.1f}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
