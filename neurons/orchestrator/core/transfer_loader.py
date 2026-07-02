"""Load worker transfer helpers without reconfiguring orchestrator logging."""

from __future__ import annotations

import importlib.util
import logging
import os
import sys
import time
from functools import lru_cache
from pathlib import Path
from types import ModuleType

logger = logging.getLogger(__name__)

_TRANSFER_MODULE: ModuleType | None = None


def _worker_module_path() -> Path:
    return Path(__file__).resolve().parents[2] / "worker" / "worker.py"


@lru_cache(maxsize=1)
def get_transfer_module() -> ModuleType:
    """Import neurons/worker/worker.py transfer symbols in-process."""
    global _TRANSFER_MODULE
    if _TRANSFER_MODULE is not None:
        return _TRANSFER_MODULE

    os.environ.setdefault("BEAM_SKIP_WORKER_BOOTSTRAP", "1")
    module_path = _worker_module_path()
    module_name = "beam_worker_transfer_engine"
    spec = importlib.util.spec_from_file_location(module_name, module_path)
    if spec is None or spec.loader is None:
        raise ImportError(f"Unable to load transfer module from {module_path}")

    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    started = time.perf_counter()
    logger.info("Transfer module: executing %s", module_path)
    try:
        spec.loader.exec_module(module)
    except SystemExit as exc:
        raise RuntimeError(
            f"Embedded transfer module exited during import (code={exc.code})"
        ) from exc
    except Exception:
        logger.exception("Embedded transfer module import failed path=%s", module_path)
        raise
    logger.info(
        "Transfer module loaded in %.2fs path=%s",
        time.perf_counter() - started,
        module_path,
    )
    _TRANSFER_MODULE = module
    return module
