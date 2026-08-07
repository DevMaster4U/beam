"""Non-blocking relay logging for orchestrator worker ↔ BeamCore paths."""

from __future__ import annotations

import asyncio
import hashlib
import logging
import os
from typing import Any, Optional
from urllib.parse import urlsplit, urlunsplit

_log = logging.getLogger("orchestrator.relay")

# INFO only when relay latency meets/exceeds this (ms). Set ORCH_RELAY_SLOW_LOG_MS=0 to log all at INFO.
SLOW_RELAY_MS = float(os.environ.get("ORCH_RELAY_SLOW_LOG_MS", "1000.0"))
_SLOW_MS = SLOW_RELAY_MS


def _env_bool(name: str, default: bool) -> bool:
    raw = os.environ.get(name)
    if raw is None or not str(raw).strip():
        return default
    return str(raw).strip().lower() in ("1", "true", "yes")


# Per-offer src/dest/range lines for task_offer_batch (summary line always logged).
BATCH_DETAIL_LOG = _env_bool("ORCH_BATCH_DETAIL_LOG", False)


def short_id(value: Any, length: int = 16) -> str:
    text = str(value or "").strip()
    if not text:
        return "?"
    if len(text) <= length:
        return text
    return f"{text[:length]}..."


def _read_task_key_field(offer: Any) -> str:
    """Cheap read of an already-resolved task_key (no hashing)."""
    if not isinstance(offer, dict):
        return ""
    for key in ("task_key", "taskKey", "_orch_task_key"):
        text = str(offer.get(key) or "").strip()
        if text:
            return text
    return ""


def stamp_offer_task_key(offer: dict) -> str:
    """Resolve dashboard task_key once and cache on the offer (enqueue-time only).

    Prefer BeamCore ``task_key``; else derive sha256(idempotency_key) and store as
    ``_orch_task_key`` so deliver/result paths never hash.
    """
    existing = _read_task_key_field(offer)
    if existing:
        return existing
    idem = str(
        offer.get("idempotency_key") or offer.get("idempotencyKey") or ""
    ).strip()
    if not idem:
        return ""
    derived = hashlib.sha256(idem.encode("utf-8")).hexdigest()
    # Private cache only — do not mutate BeamCore wire fields on the hot path copy.
    offer["_orch_task_key"] = derived
    return derived


def offer_task_key(offer: Any) -> str:
    """Return cached/dashboard task_key. Does not hash unless stamping a dict."""
    existing = _read_task_key_field(offer)
    if existing:
        return existing
    if isinstance(offer, dict):
        return stamp_offer_task_key(offer)
    return ""


def task_key_log_label(
    offer: Any = None,
    *,
    task_key: Any = None,
    task_id: Any = None,
    offer_id: Any = None,
) -> str:
    """Primary log id matching data.b1m.ai task_key. Never hashes (read-only)."""
    key = str(task_key or "").strip() or _read_task_key_field(offer)
    if key:
        return f"task_key={key}"
    tid = ""
    oid = ""
    if isinstance(offer, dict):
        tid = str(offer.get("task_id") or "").strip()
        oid = str(offer.get("offer_id") or "").strip()
    tid = str(task_id or tid or "").strip()
    oid = str(offer_id or oid or "").strip()
    parts: list[str] = []
    if tid:
        parts.append(f"task={tid}")
    if oid:
        parts.append(f"offer={oid}")
    return " ".join(parts) if parts else "task_key=-"

def redact_capability_url(url: Any) -> str:
    """Drop query parameters from signed URLs before logging."""
    text = str(url or "").strip()
    if not text:
        return "?"
    try:
        parts = urlsplit(text)
    except ValueError:
        return text.split("?", 1)[0]
    return urlunsplit((parts.scheme, parts.netloc, parts.path, "", ""))


def _offer_range_label(offer: dict) -> str:
    headers = offer.get("source_headers") or {}
    if isinstance(headers, dict):
        range_header = headers.get("Range") or headers.get("range")
        if range_header:
            return str(range_header).strip()
    range_start = offer.get("range_start")
    range_end = offer.get("range_end")
    if range_start is not None and range_end is not None:
        return f"bytes={range_start}-{range_end}"
    return "-"


def _offer_url_fields(offer: dict) -> tuple[str, str]:
    source_url = offer.get("source_url")
    dest_url = offer.get("dest_url")
    if isinstance(source_url, str) and source_url.strip() and isinstance(dest_url, str) and dest_url.strip():
        return redact_capability_url(source_url), redact_capability_url(dest_url)

    execution_context = offer.get("execution_context")
    if isinstance(execution_context, dict):
        source_urls = execution_context.get("source_urls") or {}
        dest_urls = execution_context.get("dest_urls") or {}
        if isinstance(source_urls, dict) and isinstance(dest_urls, dict):
            for key in sorted(source_urls.keys(), key=lambda value: str(value)):
                src = source_urls.get(key)
                dst = dest_urls.get(key)
                if isinstance(src, str) and src.strip() and isinstance(dst, str) and dst.strip():
                    return redact_capability_url(src), redact_capability_url(dst)
    return "?", "?"


def transfer_context_range_label(transfer_context: dict) -> str:
    try:
        range_start = int(transfer_context["range_start"])
        range_end = int(transfer_context["range_end"])
        size = range_end - range_start + 1
        return f"bytes={range_start}-{range_end}({size})"
    except (KeyError, TypeError, ValueError):
        return "-"


def transfer_context_cache_key(transfer_context: dict) -> str:
    """Cache key form ``source|range_start|range_end`` (query stripped)."""
    try:
        range_start = int(transfer_context["range_start"])
        range_end = int(transfer_context["range_end"])
    except (KeyError, TypeError, ValueError):
        return "-"
    source = redact_capability_url(transfer_context.get("source_url") or "").rstrip("/")
    if not source or source == "?":
        return "-"
    return f"{source}|{range_start}|{range_end}"


def chunk_id_from_transfer_context(transfer_context: dict) -> Optional[int]:
    """chunk_id = range_start // (range_end - range_start)."""
    try:
        range_start = int(transfer_context["range_start"])
        range_end = int(transfer_context["range_end"])
        span = range_end - range_start
        if span <= 0:
            return None
        return range_start // span
    except (KeyError, TypeError, ValueError):
        return None


def transfer_context_urls(transfer_context: dict) -> tuple[str, str]:
    return (
        redact_capability_url(transfer_context.get("source_url") or ""),
        redact_capability_url(transfer_context.get("dest_url") or ""),
    )


def format_task_offer_batch_lines(batch_id: Any, offers: list[dict]) -> list[str]:
    """Build human-readable batch/offer log lines (URLs redacted)."""
    lines = [
        f"task_offer_batch batch={short_id(batch_id, 12)} offers={len(offers)}",
    ]
    for index, offer in enumerate(offers):
        if not isinstance(offer, dict):
            lines.append(f"  offer[{index}] invalid payload type={type(offer).__name__}")
            continue
        source_url, dest_url = _offer_url_fields(offer)
        chunk_size = offer.get("chunk_size")
        transfer_id = offer.get("transfer_id")
        lines.append(
            "  "
            f"offer[{index}] {task_key_log_label(offer)} "
            f"transfer={short_id(transfer_id) if transfer_id else '-'} "
            f"chunk_size={chunk_size if chunk_size is not None else '?'} "
            f"range={_offer_range_label(offer)} "
            f"src={source_url} dest={dest_url}"
        )
    return lines


def log_task_offer_batch(batch_id: Any, offers: list[dict]) -> None:
    """Log batch summary; per-offer src/dest when ORCH_BATCH_DETAIL_LOG=true."""
    if not _log.isEnabledFor(logging.INFO):
        return
    valid_offers = [offer for offer in offers if isinstance(offer, dict)]
    lines = format_task_offer_batch_lines(batch_id, valid_offers)
    if not lines:
        return
    _log.debug(lines[0])
    if BATCH_DETAIL_LOG:
        for line in lines[1:]:
            _log.info(line)
    _log_predefined_etag_batch_skips(batch_id, valid_offers)


def _log_predefined_etag_batch_skips(batch_id: Any, offers: list[dict]) -> None:
    """Log offer payload when predefined ETag fast path is enabled but skipped."""
    try:
        from core.transfer_loader import get_transfer_module

        transfer = get_transfer_module()
    except Exception as exc:
        _log.debug("predefined_etag skip logging unavailable: %s", exc)
        return

    if not transfer.WORKER_PREDEFINED_ETAG_EARLY_SUBMIT:
        return

    for offer in offers:
        transfer_context, validation_error = transfer.build_transfer_context(offer)
        if validation_error or transfer_context is None:
            _log.info(
                "predefined_etag_skipped batch=%s task=%s offer=%s "
                "reasons=invalid_offer:%s offer_msg=%s",
                short_id(batch_id, 12),
                short_id(offer.get("task_id")),
                short_id(offer.get("offer_id")),
                validation_error or "unknown",
                transfer.format_task_offer_log(offer),
            )
            continue

        reasons = transfer.predefined_etag_early_submit_skip_reasons(transfer_context)
        if not reasons:
            continue
        _log.info(
            "predefined_etag_skipped batch=%s task=%s offer=%s reasons=%s offer_msg=%s",
            short_id(batch_id, 12),
            short_id(offer.get("task_id")),
            short_id(offer.get("offer_id")),
            "; ".join(reasons),
            transfer.format_task_offer_log(offer),
        )


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
