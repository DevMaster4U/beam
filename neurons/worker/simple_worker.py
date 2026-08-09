#!/usr/bin/env python3
"""
Simple-worker entrypoint — transfer + cache only.

Never imports bittensor. Deps: httpx, websockets (and python-dotenv optional).

Usage:
    python3 simple_worker.py --env-file config/workers/hidden1.env
    ./scripts/run-worker.sh hidden1 --foreground   # if env has WORKER_HIDDEN=true

Orch submits BeamCore task_result under WORKER_1; this process only does work.
"""

from __future__ import annotations

import asyncio
import os
import sys
from pathlib import Path


def _bootstrap() -> None:
    # Force hidden before worker.py reads env flags at import time.
    os.environ["WORKER_HIDDEN"] = "true"
    # Skip public-worker wallet/bootstrap side effects when present.
    os.environ.setdefault("BEAM_SIMPLE_WORKER", "1")

    here = Path(__file__).resolve().parent
    repo = here.parents[1]
    if str(repo) not in sys.path:
        sys.path.insert(0, str(repo))


def main() -> None:
    _bootstrap()
    # Import after WORKER_HIDDEN is set so module-level flags stay consistent.
    from neurons.worker.worker import main as worker_main  # noqa: WPS433

    try:
        asyncio.run(worker_main())
    except KeyboardInterrupt:
        print("\nExited")


if __name__ == "__main__":
    main()
