"""Cloudflare Worker transfer helper for embedded orchestrator flow.

POSTs a BeamCore task/offer JSON to a Cloudflare Worker that streams
source GET → dest PUT and returns etag (+ timings). Orchestrator then
submits task_result to BeamCore (same as embedded worker success path).
"""

from __future__ import annotations

import logging
import re
import time
from dataclasses import dataclass
from typing import Any, Iterable, Optional
from urllib.parse import parse_qs, urlsplit

import httpx

from core.relay_log import short_id

logger = logging.getLogger(__name__)

_URL_SPLIT_RE = re.compile(r"[\s,;]+")


def parse_cf_transfer_urls(*parts: Optional[str]) -> list[str]:
    """Parse one or more CF Worker URL strings (comma/space/semicolon separated)."""
    out: list[str] = []
    seen: set[str] = set()
    for part in parts:
        if part is None:
            continue
        text = str(part).strip()
        if not text:
            continue
        for token in _URL_SPLIT_RE.split(text):
            url = token.strip()
            if not url or url in seen:
                continue
            seen.add(url)
            out.append(url)
    return out


def merge_cf_transfer_urls(*groups: Iterable[str]) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    for group in groups:
        for url in group:
            clean = str(url or "").strip()
            if not clean or clean in seen:
                continue
            seen.add(clean)
            out.append(clean)
    return out


@dataclass
class CloudflareTransferResult:
    success: bool
    etag: Optional[str] = None
    part_number: Optional[str] = None
    chunk_hash: str = ""
    error: Optional[str] = None
    http_status: int = 0
    fetch_ms: float = 0.0
    send_ms: float = 0.0
    wall_ms: float = 0.0
    json_parse_ms: float = 0.0
    raw: Optional[dict] = None


def part_number_from_dest_url(dest_url: str) -> Optional[str]:
    try:
        query = parse_qs(urlsplit(str(dest_url or "")).query)
    except ValueError:
        return None
    values = query.get("partNumber") or query.get("partnumber")
    if not values:
        return None
    return str(values[0])


def normalize_etag(etag: Optional[str]) -> Optional[str]:
    """BeamCore expects quoted ETags when present."""
    if etag is None:
        return None
    text = str(etag).strip()
    if not text:
        return None
    if text.startswith("W/"):
        return text
    if not text.startswith('"'):
        text = f'"{text}"'
    return text


def _log_step(step: str, *, task_id: str, offer_id: str, **fields: Any) -> None:
    parts = [
        f"_workers | cf_transfer step={step}",
        f"task={short_id(task_id)}",
        f"offer={short_id(offer_id)}",
    ]
    for key, value in fields.items():
        if value is None:
            continue
        parts.append(f"{key}={value}")
    logger.info(" ".join(parts))


async def call_cloudflare_transfer_worker(
    *,
    worker_url: str,
    offer: dict,
    task_id: str,
    offer_id: str,
    timeout_sec: float = 120.0,
    client: Optional[httpx.AsyncClient] = None,
) -> CloudflareTransferResult:
    """POST offer JSON to Cloudflare Worker; return etag / timings."""
    dest_url = str(offer.get("dest_url") or "")
    part_number = part_number_from_dest_url(dest_url)
    url = str(worker_url or "").strip()
    if not url:
        return CloudflareTransferResult(
            success=False,
            error="cf_transfer_worker_url_missing",
            part_number=part_number,
        )

    started = time.perf_counter()
    owns_client = client is None
    http_client = client
    response: Optional[httpx.Response] = None
    wall_ms = 0.0
    try:
        if owns_client:
            http_client = httpx.AsyncClient(timeout=httpx.Timeout(timeout_sec))
        assert http_client is not None
        response = await http_client.post(
            url,
            json=offer,
            timeout=httpx.Timeout(timeout_sec),
        )
        wall_ms = (time.perf_counter() - started) * 1000
        body_text = response.text
        data: Optional[dict] = None
        try:
            parsed = response.json()
            if isinstance(parsed, dict):
                data = parsed
        except Exception:
            data = None
    except Exception as exc:
        wall_ms = (time.perf_counter() - started) * 1000
        _log_step(
            "post_error",
            task_id=str(task_id),
            offer_id=str(offer_id),
            part=part_number or "-",
            error=f"{type(exc).__name__}:{exc}",
            wall_ms=f"{wall_ms:.1f}",
        )
        return CloudflareTransferResult(
            success=False,
            error=f"{type(exc).__name__}: {exc}",
            part_number=part_number,
            wall_ms=round(wall_ms, 1),
        )
    finally:
        if owns_client and http_client is not None:
            await http_client.aclose()

    if response is None:
        return CloudflareTransferResult(
            success=False,
            error="cf_transfer_no_response",
            part_number=part_number,
            wall_ms=round(wall_ms, 1),
        )

    timings = (data or {}).get("timings_ms") if data else None
    if not isinstance(timings, dict):
        timings = {}

    def _timing(key: str, *aliases: str) -> float:
        for name in (key, *aliases):
            raw = timings.get(name)
            if raw is None:
                continue
            try:
                return float(raw)
            except (TypeError, ValueError):
                continue
        return 0.0

    fetch_ms = _timing("source_fetch", "fetch_ms")
    send_ms = _timing("stream_upload", "upload_attempt", "send_ms")
    json_parse_ms = _timing("json_parse")

    if response.status_code != 200 or not data or not data.get("success"):
        error = None
        if data:
            error = str(data.get("error") or data.get("r2_error_body") or "")
        if not error:
            error = body_text[:500] or f"http_{response.status_code}"
        _log_step(
            "worker_fail",
            task_id=str(task_id),
            offer_id=str(offer_id),
            part=part_number or "-",
            http_status=response.status_code,
            error=error.replace("\n", " ")[:300],
            fetch_ms=f"{fetch_ms:.1f}",
            send_ms=f"{send_ms:.1f}",
            wall_ms=f"{wall_ms:.1f}",
        )
        return CloudflareTransferResult(
            success=False,
            error=error,
            part_number=part_number or str(data.get("part_number") or "") or None,
            http_status=response.status_code,
            fetch_ms=round(fetch_ms, 1),
            send_ms=round(send_ms, 1),
            wall_ms=round(wall_ms, 1),
            json_parse_ms=round(json_parse_ms, 1),
            raw=data,
        )

    response_part = str(data.get("part_number") or "").strip() or part_number
    etag = normalize_etag(str(data.get("etag") or "") or None)
    return CloudflareTransferResult(
        success=True,
        etag=etag,
        part_number=response_part,
        http_status=response.status_code,
        fetch_ms=round(fetch_ms, 1),
        send_ms=round(send_ms, 1),
        wall_ms=round(wall_ms, 1),
        json_parse_ms=round(json_parse_ms, 1),
        raw=data,
    )
