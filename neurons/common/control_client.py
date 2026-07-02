"""HTTP client for the BEAM control-server (env + wallet bootstrap only)."""

from __future__ import annotations

import logging
import os
from dataclasses import dataclass
from typing import Optional
from urllib.parse import quote, urlparse, urlunparse

import httpx

logger = logging.getLogger(__name__)

DEFAULT_TIMEOUT = float(os.environ.get("CONTROL_SERVER_TIMEOUT", "10.0"))
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
