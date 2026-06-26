"""Non-blocking relay logging for orchestrator worker ↔ BeamCore paths."""

from __future__ import annotations

import asyncio
import logging
import os
from typing import Any, Optional

_log = logging.getLogger("orchestrator.relay")

# INFO only when relay latency meets/exceeds this (ms). Set ORCH_RELAY_SLOW_LOG_MS=0 to log all at INFO.
SLOW_RELAY_MS = float(os.environ.get("ORCH_RELAY_SLOW_LOG_MS", "1000.0"))
_SLOW_MS = SLOW_RELAY_MS


def short_id(value: Any, length: int = 16) -> str:
    text = str(value or "").strip()
    if not text:
        return "?"
    if len(text) <= length:
        return text
    return f"{text[:length]}..."


def relay_summary(payload: dict) -> str:
    parts: list[str] = []
    for key in ("accepted", "received", "completed", "ready", "status"):
        if key in payload:
            parts.append(f"{key}={payload.get(key)}")
    reason = payload.get("reason") or payload.get("error") or payload.get("message")
    if reason:
        parts.append(f"reason={reason}")
    return " ".join(parts) if parts else "-"


def is_failure_summary(summary: str) -> bool:
    lowered = summary.lower()
    return (
        "accepted=false" in lowered
        or "received=false" in lowered
        or "completed=false" in lowered
    )


def _is_failure_summary(summary: str) -> bool:
    return is_failure_summary(summary)


def _pick_level(
    latency_ms: Optional[float],
    *,
    force_info: bool = False,
) -> int:
    if force_info:
        return logging.INFO
    if latency_ms is not None and _SLOW_MS >= 0 and latency_ms >= _SLOW_MS:
        return logging.INFO
    return logging.DEBUG


def log_relay(
    message: str,
    *,
    latency_ms: Optional[float] = None,
    force_info: bool = False,
) -> None:
    """Log immediately; skips work when neither INFO nor DEBUG is enabled."""
    level = _pick_level(latency_ms, force_info=force_info)
    if not _log.isEnabledFor(level):
        return
    if latency_ms is not None:
        _log.log(level, "%s latency_ms=%.1f", message, latency_ms)
    else:
        _log.log(level, "%s", message)


def defer_relay_log(
    message: str,
    *,
    latency_ms: Optional[float] = None,
    force_info: bool = False,
) -> None:
    """Defer log emission until after the current await (ack/send is not blocked)."""
    level = _pick_level(latency_ms, force_info=force_info)
    if not _log.isEnabledFor(level):
        return

    def _emit() -> None:
        if latency_ms is not None:
            _log.log(level, "%s latency_ms=%.1f", message, latency_ms)
        else:
            _log.log(level, "%s", message)

    try:
        loop = asyncio.get_running_loop()
        loop.call_soon(_emit)
    except RuntimeError:
        _emit()
