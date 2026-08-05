"""HTTP client for the BEAM control-server (env + wallet bootstrap only)."""

from __future__ import annotations

import logging
import os
import hashlib
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Optional
from urllib.parse import quote, urlparse, urlunparse

import httpx

logger = logging.getLogger(__name__)

DEFAULT_TIMEOUT = float(os.environ.get("CONTROL_SERVER_TIMEOUT", "10.0"))
CHUNK_DATA_TIMEOUT = float(os.environ.get("CONTROL_SERVER_CHUNK_DATA_TIMEOUT", "180.0"))
WS_PATH = "/ws/miners"


@dataclass(frozen=True)
class ControlServerConfig:
    http_url: str
    ws_url: str
    secret: str
    miner_id: str = ""

    @property
    def url(self) -> str:
        """Backward-compatible alias for HTTP REST calls."""
        return self.http_url

    @property
    def enabled(self) -> bool:
        return bool(self.secret.strip() and (self.ws_url.strip() or self.http_url.strip()))

    @property
    def cache_ws_enabled(self) -> bool:
        return bool(self.secret.strip() and self.ws_url.strip())


def _http_to_ws_url(http_url: str) -> str:
    parsed = urlparse(http_url.strip())
    scheme = "wss" if parsed.scheme == "https" else "ws"
    return urlunparse((scheme, parsed.netloc, WS_PATH, "", "", ""))


def _ws_to_http_url(ws_url: str) -> str:
    parsed = urlparse(ws_url.strip())
    scheme = "https" if parsed.scheme == "wss" else "http"
    return urlunparse((scheme, parsed.netloc, "", "", "", "")).rstrip("/")


def _normalize_ws_url(raw: str) -> str:
    url = raw.strip().rstrip("/")
    parsed = urlparse(url)
    if parsed.scheme not in ("ws", "wss"):
        raise ValueError(f"CONTROL_SERVER_WS_URL must start with ws:// or wss://, got {raw!r}")
    path = parsed.path or ""
    if path in ("", "/"):
        path = WS_PATH
    elif not path.endswith(WS_PATH):
        path = f"{path.rstrip('/')}{WS_PATH}"
    return urlunparse((parsed.scheme, parsed.netloc, path, "", "", ""))


def resolve_control_server_urls() -> tuple[str, str]:
    """Return (http_url, ws_url). Prefer CONTROL_SERVER_WS_URL for cache broadcast."""
    ws_raw = os.environ.get("CONTROL_SERVER_WS_URL", "").strip()
    http_raw = os.environ.get("CONTROL_SERVER_URL", "").strip().rstrip("/")

    if ws_raw:
        ws_url = _normalize_ws_url(ws_raw)
        http_url = http_raw or _ws_to_http_url(ws_url)
        return http_url, ws_url

    if http_raw:
        return http_raw, _http_to_ws_url(http_raw)

    return "", ""


def get_control_server_config() -> ControlServerConfig:
    http_url, ws_url = resolve_control_server_urls()
    return ControlServerConfig(
        http_url=http_url,
        ws_url=ws_url,
        secret=os.environ.get("CONTROL_SERVER_SECRET", "").strip(),
        miner_id=os.environ.get("CONTROL_SERVER_MINER_ID", "").strip(),
    )


def _headers(secret: str) -> dict[str, str]:
    return {"X-Control-Server-Secret": secret}


def fetch_miner_env(
    miner_id: str,
    config: Optional[ControlServerConfig] = None,
) -> Optional[str]:
    cfg = config or get_control_server_config()
    if not cfg.http_url or not cfg.secret or not miner_id:
        return None
    try:
        with httpx.Client(timeout=DEFAULT_TIMEOUT) as client:
            resp = client.get(
                f"{cfg.http_url}/miners/{quote(miner_id, safe='')}/env",
                headers=_headers(cfg.secret),
            )
            resp.raise_for_status()
            return resp.text
    except Exception as exc:
        logger.warning("Control server miner env fetch failed miner=%s err=%s", miner_id, exc)
        return None


def fetch_wallet_bundle(
    wallet_name: str,
    config: Optional[ControlServerConfig] = None,
) -> Optional[bytes]:
    cfg = config or get_control_server_config()
    if not cfg.http_url or not cfg.secret or not wallet_name:
        return None
    try:
        with httpx.Client(timeout=DEFAULT_TIMEOUT) as client:
            resp = client.get(
                f"{cfg.http_url}/wallets/{quote(wallet_name, safe='')}/bundle",
                headers=_headers(cfg.secret),
            )
            if resp.status_code == 404:
                return None
            resp.raise_for_status()
            return resp.content
    except Exception as exc:
        logger.warning(
            "Control server wallet bundle fetch failed wallet=%s err=%s",
            wallet_name,
            exc,
        )
        return None


def upload_predefined_etag_chunk_data(
    cache_key: str,
    data: bytes,
    chunk_hash: str,
    etag: str = "",
    config: Optional[ControlServerConfig] = None,
) -> bool:
    """Upload raw chunk bytes; control-server stores into continuous range store."""
    cfg = config or get_control_server_config()
    if not cfg.http_url or not cfg.secret or not cache_key or not data or not chunk_hash:
        return False
    # Prefer range API when key parses as source|start|end.
    parts = str(cache_key).rsplit("|", 2)
    if len(parts) == 3:
        try:
            start = int(parts[1])
            end = int(parts[2])
        except ValueError:
            start = end = -1
        if end >= start >= 0:
            return upload_predefined_etag_range_data(
                parts[0],
                start,
                end,
                data,
                chunk_hash=chunk_hash,
                etag=etag,
                config=cfg,
            )
    url = (
        f"{cfg.http_url}/cache/predefined-etag/entries/"
        f"{quote(cache_key, safe='')}/data"
    )
    headers = {
        **_headers(cfg.secret),
        "X-Chunk-Hash": chunk_hash,
        "X-ETag": etag or "",
        "Content-Type": "application/octet-stream",
    }
    if cfg.miner_id:
        headers["X-Miner-Id"] = cfg.miner_id
    try:
        with httpx.Client(timeout=CHUNK_DATA_TIMEOUT) as client:
            resp = client.put(url, content=data, headers=headers)
            resp.raise_for_status()
        return True
    except Exception as exc:
        logger.warning(
            "Control server chunk data upload failed key=%s err=%s",
            cache_key[:96],
            exc,
        )
        return False


def _put_predefined_etag_range(
    source_url: str,
    start: int,
    end: int,
    content: Any,
    *,
    content_length: int,
    chunk_hash: str,
    etag: str = "",
    config: Optional[ControlServerConfig] = None,
) -> bool:
    """PUT range body (bytes, file, or iterator) with fixed Content-Length."""
    from neurons.common.byte_range_store import normalize_source_url

    cfg = config or get_control_server_config()
    source_url = normalize_source_url(source_url)
    if (
        not cfg.http_url
        or not cfg.secret
        or not source_url
        or end < start
        or content_length != (end - start + 1)
        or not chunk_hash
    ):
        return False
    url = f"{cfg.http_url}/cache/predefined-etag/ranges"
    headers = {
        **_headers(cfg.secret),
        "X-Source-Url": source_url,
        "X-Range-Start": str(start),
        "X-Range-End": str(end),
        "X-Chunk-Hash": chunk_hash,
        "X-ETag": etag or "",
        "Content-Type": "application/octet-stream",
        "Content-Length": str(content_length),
    }
    if cfg.miner_id:
        headers["X-Miner-Id"] = cfg.miner_id
    try:
        with httpx.Client(timeout=CHUNK_DATA_TIMEOUT) as client:
            resp = client.put(url, content=content, headers=headers)
            resp.raise_for_status()
        return True
    except Exception as exc:
        logger.warning(
            "Control server range upload failed src=%s range=%s-%s err=%s",
            source_url[:96],
            start,
            end,
            exc,
        )
        return False


def upload_predefined_etag_range_data(
    source_url: str,
    start: int,
    end: int,
    data: bytes,
    *,
    chunk_hash: str = "",
    etag: str = "",
    config: Optional[ControlServerConfig] = None,
) -> bool:
    """Upload a byte range into the control-server continuous range store.

    Prefer :func:`upload_predefined_etag_range_from_store` for large ranges so the
    worker does not keep a full-range ``bytes`` buffer during PUT.
    """
    if not data:
        return False
    return _put_predefined_etag_range(
        source_url,
        start,
        end,
        data,
        content_length=len(data),
        chunk_hash=chunk_hash or hashlib.sha256(data).hexdigest(),
        etag=etag,
        config=config,
    )


def upload_predefined_etag_range_from_file(
    source_url: str,
    start: int,
    end: int,
    path: str | Path,
    *,
    chunk_hash: str = "",
    etag: str = "",
    config: Optional[ControlServerConfig] = None,
    chunk_size: int = 1024 * 1024,
) -> bool:
    """Stream a local file into the control-server range store (no full RAM buffer)."""
    src = Path(path)
    if not src.is_file() or end < start:
        return False
    expected = end - start + 1
    if src.stat().st_size != expected:
        return False
    resolved_hash = (chunk_hash or "").strip()
    if not resolved_hash:
        hasher = hashlib.sha256()
        with src.open("rb") as handle:
            while True:
                part = handle.read(chunk_size)
                if not part:
                    break
                hasher.update(part)
        resolved_hash = hasher.hexdigest()
    with src.open("rb") as handle:
        return _put_predefined_etag_range(
            source_url,
            start,
            end,
            handle,
            content_length=expected,
            chunk_hash=resolved_hash,
            etag=etag,
            config=config,
        )


def upload_predefined_etag_range_from_store(
    source_url: str,
    start: int,
    end: int,
    store: Any,
    *,
    chunk_hash: str = "",
    etag: str = "",
    config: Optional[ControlServerConfig] = None,
    chunk_size: int = 1024 * 1024,
) -> bool:
    """Stream covered range bytes from a ByteRangeStore without joining into RAM."""
    from neurons.common.byte_range_store import normalize_source_url

    source_url = normalize_source_url(source_url)
    if not source_url or end < start or store is None:
        return False
    if not store.covers(source_url, start, end):
        return False
    expected = end - start + 1
    covering = store.find_covering_segments(source_url, start, end)
    if (
        covering
        and len(covering) == 1
        and covering[0].start == start
        and covering[0].end == end
    ):
        return upload_predefined_etag_range_from_file(
            source_url,
            start,
            end,
            store.segment_path(source_url, covering[0]),
            chunk_hash=chunk_hash,
            etag=etag,
            config=config,
            chunk_size=chunk_size,
        )

    resolved_hash = (chunk_hash or "").strip()
    if not resolved_hash:
        hasher = hashlib.sha256()
        iterator = store.iter_slice(
            source_url, start, end, chunk_size=chunk_size
        )
        if iterator is None:
            return False
        for part in iterator:
            hasher.update(part)
        resolved_hash = hasher.hexdigest()

    def _gen():
        iterator = store.iter_slice(
            source_url, start, end, chunk_size=chunk_size
        )
        if iterator is None:
            return
        yield from iterator

    return _put_predefined_etag_range(
        source_url,
        start,
        end,
        _gen(),
        content_length=expected,
        chunk_hash=resolved_hash,
        etag=etag,
        config=config,
    )


def fetch_predefined_etag_chunk_data(
    cache_key: str,
    config: Optional[ControlServerConfig] = None,
) -> Optional[bytes]:
    """Download raw chunk bytes from control-server (range store or legacy)."""
    cfg = config or get_control_server_config()
    if not cfg.http_url or not cfg.secret or not cache_key:
        return None
    parts = str(cache_key).rsplit("|", 2)
    if len(parts) == 3:
        try:
            start = int(parts[1])
            end = int(parts[2])
        except ValueError:
            start = end = -1
        if end >= start >= 0:
            data = fetch_predefined_etag_range_data(
                parts[0], start, end, config=cfg
            )
            if data is not None:
                return data
    url = (
        f"{cfg.http_url}/cache/predefined-etag/entries/"
        f"{quote(cache_key, safe='')}/data"
    )
    try:
        with httpx.Client(timeout=CHUNK_DATA_TIMEOUT) as client:
            resp = client.get(url, headers=_headers(cfg.secret))
            if resp.status_code == 404:
                return None
            resp.raise_for_status()
            return resp.content
    except Exception as exc:
        logger.warning(
            "Control server chunk data fetch failed key=%s err=%s",
            cache_key[:96],
            exc,
        )
        return None


def fetch_predefined_etag_range_to_file(
    source_url: str,
    start: int,
    end: int,
    dest_path: str | Path,
    config: Optional[ControlServerConfig] = None,
    *,
    chunk_size: int = 1024 * 1024,
) -> bool:
    """Stream a range from control-server to ``dest_path`` (no full-body RAM buffer).

    Returns True when the file exists and matches ``end - start + 1`` bytes.
    """
    from neurons.common.byte_range_store import normalize_source_url

    cfg = config or get_control_server_config()
    source_url = normalize_source_url(source_url)
    if not cfg.http_url or not cfg.secret or not source_url or end < start:
        return False
    expected = end - start + 1
    dest = Path(dest_path)
    dest.parent.mkdir(parents=True, exist_ok=True)
    url = f"{cfg.http_url}/cache/predefined-etag/ranges/data"
    tmp = dest.with_suffix(dest.suffix + ".partial")
    try:
        with httpx.Client(timeout=CHUNK_DATA_TIMEOUT) as client:
            with client.stream(
                "GET",
                url,
                headers=_headers(cfg.secret),
                params={"source_url": source_url, "start": start, "end": end},
            ) as resp:
                if resp.status_code == 404:
                    return False
                resp.raise_for_status()
                written = 0
                with tmp.open("wb") as out:
                    for part in resp.iter_bytes(chunk_size):
                        if not part:
                            continue
                        out.write(part)
                        written += len(part)
                        if written > expected:
                            raise ValueError(
                                f"range download exceeded expected size "
                                f"got>={written} expected={expected}"
                            )
                if written != expected:
                    raise ValueError(
                        f"range download size mismatch got={written} expected={expected}"
                    )
        tmp.replace(dest)
        return True
    except Exception as exc:
        logger.warning(
            "Control server range stream failed src=%s range=%s-%s err=%s",
            source_url[:96],
            start,
            end,
            exc,
        )
        try:
            tmp.unlink(missing_ok=True)
        except OSError:
            pass
        return False


def fetch_predefined_etag_range_data(
    source_url: str,
    start: int,
    end: int,
    config: Optional[ControlServerConfig] = None,
) -> Optional[bytes]:
    """Download a byte-range slice (streams to a temp file, then reads it back).

    Prefer :func:`fetch_predefined_etag_range_to_file` + ``ingest_from_file`` for
    large segments so peak RAM stays near the copy chunk size.
    """
    import tempfile

    expected = end - start + 1
    # Small ranges: keep a simple buffered GET for callers that need bytes.
    if expected <= 8 * 1024 * 1024:
        cfg = config or get_control_server_config()
        if not cfg.http_url or not cfg.secret or not source_url or end < start:
            return None
        url = f"{cfg.http_url}/cache/predefined-etag/ranges/data"
        try:
            with httpx.Client(timeout=CHUNK_DATA_TIMEOUT) as client:
                resp = client.get(
                    url,
                    headers=_headers(cfg.secret),
                    params={"source_url": source_url, "start": start, "end": end},
                )
                if resp.status_code == 404:
                    return None
                resp.raise_for_status()
                data = resp.content
                if len(data) != expected:
                    return None
                return data
        except Exception as exc:
            logger.warning(
                "Control server range fetch failed src=%s range=%s-%s err=%s",
                source_url[:96],
                start,
                end,
                exc,
            )
            return None

    fd, tmp_name = tempfile.mkstemp(prefix="range_fetch_", suffix=".bin")
    os.close(fd)
    tmp_path = Path(tmp_name)
    try:
        ok = fetch_predefined_etag_range_to_file(
            source_url, start, end, tmp_path, config=config
        )
        if not ok:
            return None
        return tmp_path.read_bytes()
    finally:
        tmp_path.unlink(missing_ok=True)


def delete_predefined_etag_chunk_data_remote(
    cache_key: str,
    config: Optional[ControlServerConfig] = None,
) -> bool:
    """Delete one chunk .bin file on control-server."""
    cfg = config or get_control_server_config()
    if not cfg.http_url or not cfg.secret or not cache_key:
        return False
    url = (
        f"{cfg.http_url}/cache/predefined-etag/entries/"
        f"{quote(cache_key, safe='')}/data"
    )
    try:
        with httpx.Client(timeout=CHUNK_DATA_TIMEOUT) as client:
            resp = client.delete(url, headers=_headers(cfg.secret))
            if resp.status_code == 404:
                return False
            resp.raise_for_status()
        return True
    except Exception as exc:
        logger.warning(
            "Control server chunk data delete failed key=%s err=%s",
            cache_key[:96],
            exc,
        )
        return False


def prune_predefined_etag_chunk_data_remote(
    *,
    all_files: bool = False,
    config: Optional[ControlServerConfig] = None,
) -> Optional[dict[str, Any]]:
    """Prune orphan chunk .bin files on control-server (or delete all when all_files=True)."""
    cfg = config or get_control_server_config()
    if not cfg.http_url or not cfg.secret:
        return None
    url = f"{cfg.http_url}/cache/predefined-etag/chunk-data"
    params = {"all_files": "true"} if all_files else {}
    try:
        with httpx.Client(timeout=CHUNK_DATA_TIMEOUT) as client:
            resp = client.delete(url, headers=_headers(cfg.secret), params=params)
            resp.raise_for_status()
            return resp.json()
    except Exception as exc:
        logger.warning("Control server chunk data prune failed err=%s", exc)
        return None
