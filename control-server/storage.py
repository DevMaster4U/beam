"""On-disk storage helpers for control-server."""

from __future__ import annotations

import io
import json
import tarfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

from config import get_settings


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def load_predefined_etag_cache() -> dict[str, Any]:
    path = get_settings().predefined_etag_cache_path
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {"entries": {}, "updated_at": None}
    if not isinstance(data, dict):
        return {"entries": {}, "updated_at": None}
    entries = data.get("entries")
    if not isinstance(entries, dict):
        data["entries"] = {}
    return data


def save_predefined_etag_cache(payload: dict[str, Any]) -> None:
    path = get_settings().predefined_etag_cache_path
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = dict(payload)
    payload["updated_at"] = _utc_now()
    path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")


def upsert_predefined_etag_entry(key: str, chunk_hash: str, etag: str) -> dict[str, Any]:
    payload = load_predefined_etag_cache()
    entries = payload.setdefault("entries", {})
    entries[key] = {"chunk_hash": chunk_hash, "etag": etag}
    save_predefined_etag_cache(payload)
    return entries[key]


def merge_predefined_etag_entries(new_entries: dict[str, dict[str, str]]) -> dict[str, Any]:
    payload = load_predefined_etag_cache()
    entries = payload.setdefault("entries", {})
    for key, item in new_entries.items():
        if not isinstance(item, dict):
            continue
        chunk_hash = str(item.get("chunk_hash") or "").strip()
        etag = str(item.get("etag") or "").strip()
        if chunk_hash:
            entries[key] = {"chunk_hash": chunk_hash, "etag": etag}
    save_predefined_etag_cache(payload)
    return payload


def list_miners() -> list[str]:
    miners_dir = get_settings().miners_dir
    return sorted(path.stem for path in miners_dir.glob("*.env"))


def read_miner_env(miner_id: str) -> str:
    path = get_settings().miners_dir / f"{miner_id}.env"
    if not path.is_file():
        raise FileNotFoundError(miner_id)
    return path.read_text(encoding="utf-8")


def write_miner_env(miner_id: str, content: str) -> None:
    path = get_settings().miners_dir / f"{miner_id}.env"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


def list_wallets() -> list[str]:
    wallets_dir = get_settings().wallets_dir
    return sorted(
        path.name
        for path in wallets_dir.iterdir()
        if path.is_dir() and not path.name.startswith(".")
    )


def wallet_dir(wallet_name: str) -> Path:
    return get_settings().wallets_dir / wallet_name


def wallet_exists(wallet_name: str) -> bool:
    return wallet_dir(wallet_name).is_dir()


def list_wallet_hotkeys(wallet_name: str) -> list[str]:
    hotkeys_dir = wallet_dir(wallet_name) / "hotkeys"
    if not hotkeys_dir.is_dir():
        return []
    return sorted(
        path.name
        for path in hotkeys_dir.iterdir()
        if path.is_file() and not path.name.startswith(".")
    )


def wallet_hotkey_exists(wallet_name: str, hotkey: str) -> bool:
    return (wallet_dir(wallet_name) / "hotkeys" / hotkey).is_file()


def build_wallet_tarball(wallet_name: str) -> bytes:
    root = wallet_dir(wallet_name)
    if not root.is_dir():
        raise FileNotFoundError(wallet_name)
    buffer = io.BytesIO()
    with tarfile.open(fileobj=buffer, mode="w:gz") as tar:
        for path in root.rglob("*"):
            if path.is_file():
                tar.add(path, arcname=str(path.relative_to(root)))
    return buffer.getvalue()


def extract_wallet_tarball(wallet_name: str, payload: bytes) -> None:
    root = wallet_dir(wallet_name)
    root.mkdir(parents=True, exist_ok=True)
    with tarfile.open(fileobj=io.BytesIO(payload), mode="r:gz") as tar:
        tar.extractall(path=root)
