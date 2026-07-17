"""HTTP client for the BEAM control-server (env + wallet bootstrap only)."""

from __future__ import annotations

import logging
import os
import hashlib
from dataclasses import dataclass
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
    """Upload a byte range into the control-server continuous range store."""
    cfg = config or get_control_server_config()
    if not cfg.http_url or not cfg.secret or not source_url or not data:
        return False
    if end < start or len(data) != (end - start + 1):
        return False
    url = f"{cfg.http_url}/cache/predefined-etag/ranges"
    headers = {
        **_headers(cfg.secret),
        "X-Source-Url": source_url,
        "X-Range-Start": str(start),
        "X-Range-End": str(end),
        "X-Chunk-Hash": chunk_hash or hashlib.sha256(data).hexdigest(),
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
            "Control server range upload failed src=%s range=%s-%s err=%s",
            source_url[:96],
            start,
            end,
            exc,
        )
        return False


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


def fetch_predefined_etag_range_data(
    source_url: str,
    start: int,
    end: int,
    config: Optional[ControlServerConfig] = None,
) -> Optional[bytes]:
    """Download a byte-range slice from the control-server continuous store."""
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
            return resp.content
    except Exception as exc:
        logger.warning(
            "Control server range fetch failed src=%s range=%s-%s err=%s",
            source_url[:96],
            start,
            end,
            exc,
        )
        return None


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
