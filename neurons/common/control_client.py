"""HTTP client for the BEAM control-server."""

from __future__ import annotations

import logging
import os
from dataclasses import dataclass
from typing import Optional
from urllib.parse import quote

import httpx

logger = logging.getLogger(__name__)

DEFAULT_TIMEOUT = float(os.environ.get("CONTROL_SERVER_TIMEOUT", "10.0"))


@dataclass(frozen=True)
class ControlServerConfig:
    url: str
    secret: str
    miner_id: str = ""

    @property
    def enabled(self) -> bool:
        return bool(self.url.strip() and self.secret.strip())


def get_control_server_config() -> ControlServerConfig:
    return ControlServerConfig(
        url=os.environ.get("CONTROL_SERVER_URL", "").strip().rstrip("/"),
        secret=os.environ.get("CONTROL_SERVER_SECRET", "").strip(),
        miner_id=os.environ.get("CONTROL_SERVER_MINER_ID", "").strip(),
    )


def _headers(secret: str) -> dict[str, str]:
    return {"X-Control-Server-Secret": secret}


def fetch_predefined_etag_cache(config: Optional[ControlServerConfig] = None) -> dict[str, dict[str, str]]:
    cfg = config or get_control_server_config()
    if not cfg.enabled:
        return {}
    try:
        with httpx.Client(timeout=DEFAULT_TIMEOUT) as client:
            resp = client.get(
                f"{cfg.url}/cache/predefined-etag",
                headers=_headers(cfg.secret),
            )
            resp.raise_for_status()
            payload = resp.json()
    except Exception as exc:
        logger.warning("Control server cache fetch failed: %s", exc)
        return {}
    entries = payload.get("entries") or {}
    if not isinstance(entries, dict):
        return {}
    return {
        str(key): {
            "chunk_hash": str(item.get("chunk_hash") or ""),
            "etag": str(item.get("etag") or ""),
        }
        for key, item in entries.items()
        if isinstance(item, dict) and str(item.get("chunk_hash") or "").strip()
    }


def fetch_predefined_etag_entry(
    cache_key: str,
    config: Optional[ControlServerConfig] = None,
) -> Optional[dict[str, str]]:
    cfg = config or get_control_server_config()
    if not cfg.enabled or not cache_key:
        return None
    try:
        with httpx.Client(timeout=DEFAULT_TIMEOUT) as client:
            resp = client.get(
                f"{cfg.url}/cache/predefined-etag/entries/{quote(cache_key, safe='')}",
                headers=_headers(cfg.secret),
            )
            if resp.status_code == 404:
                return None
            resp.raise_for_status()
            item = resp.json()
    except Exception as exc:
        logger.warning("Control server cache entry fetch failed key=%s err=%s", cache_key, exc)
        return None
    chunk_hash = str(item.get("chunk_hash") or "").strip()
    if not chunk_hash:
        return None
    return {"chunk_hash": chunk_hash, "etag": str(item.get("etag") or "")}


def push_predefined_etag_entry(
    cache_key: str,
    chunk_hash: str,
    etag: str,
    config: Optional[ControlServerConfig] = None,
) -> bool:
    cfg = config or get_control_server_config()
    if not cfg.enabled or not cache_key or not chunk_hash:
        return False
    try:
        with httpx.Client(timeout=DEFAULT_TIMEOUT) as client:
            resp = client.put(
                f"{cfg.url}/cache/predefined-etag/entries/{quote(cache_key, safe='')}",
                headers=_headers(cfg.secret),
                json={"chunk_hash": chunk_hash, "etag": etag or ""},
            )
            resp.raise_for_status()
            return True
    except Exception as exc:
        logger.warning("Control server cache push failed key=%s err=%s", cache_key, exc)
        return False


def fetch_miner_env(
    miner_id: str,
    config: Optional[ControlServerConfig] = None,
) -> Optional[str]:
    cfg = config or get_control_server_config()
    if not cfg.enabled or not miner_id:
        return None
    try:
        with httpx.Client(timeout=DEFAULT_TIMEOUT) as client:
            resp = client.get(
                f"{cfg.url}/miners/{quote(miner_id, safe='')}/env",
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
    if not cfg.enabled or not wallet_name:
        return None
    try:
        with httpx.Client(timeout=DEFAULT_TIMEOUT) as client:
            resp = client.get(
                f"{cfg.url}/wallets/{quote(wallet_name, safe='')}/bundle",
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
