#!/usr/bin/env python3
"""POST a BeamCore task/offer JSON to the Cloudflare transfer worker.

Usage:
  export CF_TRANSFER_WORKER_URL=https://your-worker.workers.dev
  python3 scripts/cloudflare/call-transfer-worker.py --task scripts/fixtures/sample-task-offer.json

  # Or set TASK JSON inline via --task -
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
from pathlib import Path

_REPO = Path(__file__).resolve().parents[2]
if str(_REPO) not in sys.path:
    sys.path.insert(0, str(_REPO))
_ORCH = _REPO / "neurons" / "orchestrator"
if str(_ORCH) not in sys.path:
    sys.path.insert(0, str(_ORCH))

from core.cloudflare_transfer import (  # noqa: E402
    call_cloudflare_transfer_worker,
    parse_cf_transfer_urls,
    part_number_from_dest_url,
)


def _load_task(path: str) -> dict:
    raw = sys.stdin.read() if path == "-" else Path(path).read_text(encoding="utf-8")
    payload = json.loads(raw)
    if not isinstance(payload, dict):
        raise SystemExit("task must be a JSON object")
    for key in ("task", "offer", "message"):
        inner = payload.get(key)
        if isinstance(inner, dict) and ("source_url" in inner or "dest_url" in inner):
            return inner
    return payload


async def _amain(args: argparse.Namespace) -> int:
    task = _load_task(args.task)
    urls = parse_cf_transfer_urls(
        args.worker_url,
        os.environ.get("CF_TRANSFER_WORKER_URLS"),
        os.environ.get("CF_TRANSFER_WORKER_URL"),
    )
    if not urls:
        print(
            "need --worker-url or CF_TRANSFER_WORKER_URL(S)",
            file=sys.stderr,
        )
        return 2

    task_id = str(task.get("task_id") or "manual")
    offer_id = str(task.get("offer_id") or task_id)
    part = part_number_from_dest_url(str(task.get("dest_url") or ""))
    # Round-robin across the pool when testing multiple URLs.
    worker_url = urls[0]
    if len(urls) > 1:
        # Stable-ish pick by offer/task id so repeats are reproducible.
        seed = sum(ord(c) for c in (offer_id or task_id))
        worker_url = urls[seed % len(urls)]
        print(f"pool={len(urls)} urls picked={worker_url}")

    print(f"POST {worker_url} task={task_id} offer={offer_id} part={part or '-'}")

    result = await call_cloudflare_transfer_worker(
        worker_url=worker_url,
        offer=task,
        task_id=task_id,
        offer_id=offer_id,
        timeout_sec=float(args.timeout),
    )
    if not result.success:
        print(f"FAIL error={result.error} fetch_ms={result.fetch_ms} send_ms={result.send_ms}")
        return 1

    print(f"etag={result.etag or ''}")
    print(f"part_number={result.part_number or ''}")
    print(f"fetch_ms={result.fetch_ms:.1f}")
    print(f"send_ms={result.send_ms:.1f}")
    print(f"wall_ms={result.wall_ms:.1f}")
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--task", required=True, help="task JSON path or '-'")
    parser.add_argument(
        "--worker-url",
        default="",
        help="CF Worker URL(s), comma-separated (default CF_TRANSFER_WORKER_URLS/URL)",
    )
    parser.add_argument("--timeout", type=float, default=120.0)
    return asyncio.run(_amain(parser.parse_args()))


if __name__ == "__main__":
    raise SystemExit(main())
