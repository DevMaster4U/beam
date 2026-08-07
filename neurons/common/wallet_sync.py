"""Download bittensor wallets from control-server when missing locally."""

from __future__ import annotations

import io
import logging
import os
import shutil
import tarfile
from pathlib import Path
from typing import Iterable

from neurons.common.control_client import fetch_wallet_bundle, get_control_server_config

logger = logging.getLogger(__name__)


def _expand_wallet_path(raw: str) -> Path:
    return Path(raw or "~/.bittensor/wallets").expanduser()


def _collect_wallet_names() -> set[str]:
    names: set[str] = set()
    for key in ("WALLET_NAME", "WORKER_WALLET_NAME"):
        value = os.environ.get(key, "").strip()
        if value:
            names.add(value)

    idx = 1
    while True:
        combined = os.environ.get(f"WORKER_{idx}", "").strip()
        wallet_name = os.environ.get(f"WORKER_{idx}_WALLET_NAME", "").strip()
        if combined and ":" in combined:
            wallet_name = wallet_name or combined.split(":", 1)[0].strip()
        if not wallet_name and not combined:
            break
        if wallet_name:
            names.add(wallet_name)
        idx += 1
    return names


def _collect_required_hotkeys(wallet_name: str) -> set[str]:
    hotkeys: set[str] = set()
    default_wallet = (
        os.environ.get("WALLET_NAME", "").strip()
        or os.environ.get("WORKER_WALLET_NAME", "").strip()
    )
    if wallet_name == default_wallet:
        for key in ("WALLET_HOTKEY", "WORKER_WALLET_HOTKEY"):
            value = os.environ.get(key, "").strip()
            if value:
                hotkeys.add(value)

    idx = 1
    while True:
        combined = os.environ.get(f"WORKER_{idx}", "").strip()
        hotkey = os.environ.get(f"WORKER_{idx}_HOTKEY", "").strip()
        slot_wallet = os.environ.get(f"WORKER_{idx}_WALLET_NAME", "").strip()
        if combined:
            if ":" in combined:
                wallet_part, hotkey_part = combined.split(":", 1)
                slot_wallet = slot_wallet or wallet_part.strip()
                hotkey = hotkey or hotkey_part.strip()
            elif not hotkey:
                hotkey = combined
        if not hotkey:
            break
        if slot_wallet == wallet_name or (not slot_wallet and wallet_name == default_wallet):
            hotkeys.add(hotkey)
        idx += 1
    return hotkeys


def _hotkey_exists(wallet_path: Path, wallet_name: str, hotkey: str) -> bool:
    return (wallet_path / wallet_name / "hotkeys" / hotkey).is_file()


def _extract_wallet_bundle(wallet_path: Path, wallet_name: str, payload: bytes) -> None:
    """Extract into wallets/<name>/{coldkey,hotkeys/...}, unwrapping legacy <name>/ prefix."""
    target = wallet_path / wallet_name
    if target.exists():
        shutil.rmtree(target)
    target.mkdir(parents=True, exist_ok=True)

    prefix = f"{wallet_name}/"
    with tarfile.open(fileobj=io.BytesIO(payload), mode="r:gz") as tar:
        members = [m for m in tar.getmembers() if m.name and m.name not in (".", "./")]
        names = [m.name.replace("\\", "/").lstrip("./") for m in members]
        strip_prefix = bool(names) and all(
            n == wallet_name or n.startswith(prefix) for n in names
        )
        for member in members:
            if not member.isfile() and not member.isdir():
                continue
            name = member.name.replace("\\", "/").lstrip("./")
            if strip_prefix:
                if name == wallet_name:
                    continue
                if name.startswith(prefix):
                    name = name[len(prefix) :]
                else:
                    continue
            if not name or name.startswith("/") or ".." in Path(name).parts:
                continue
            dest = target / name
            if member.isdir():
                dest.mkdir(parents=True, exist_ok=True)
                continue
            dest.parent.mkdir(parents=True, exist_ok=True)
            source = tar.extractfile(member)
            if source is None:
                continue
            with source, dest.open("wb") as out:
                shutil.copyfileobj(source, out)


def ensure_wallet_from_control_server(
    wallet_name: str,
    required_hotkeys: Iterable[str],
    wallet_path: Path | None = None,
) -> bool:
    cfg = get_control_server_config()
    if not cfg.enabled:
        return False

    wallet_root = wallet_path or _expand_wallet_path(os.environ.get("WALLET_PATH", ""))
    missing = [hotkey for hotkey in required_hotkeys if not _hotkey_exists(wallet_root, wallet_name, hotkey)]
    if not missing:
        return True

    logger.info(
        "Wallet missing locally wallet=%s hotkeys=%s; downloading from control-server",
        wallet_name,
        ",".join(missing),
    )
    payload = fetch_wallet_bundle(wallet_name)
    if not payload:
        logger.error("Control server wallet download failed wallet=%s", wallet_name)
        return False

    _extract_wallet_bundle(wallet_root, wallet_name, payload)
    still_missing = [
        hotkey for hotkey in required_hotkeys if not _hotkey_exists(wallet_root, wallet_name, hotkey)
    ]
    if still_missing:
        logger.error(
            "Wallet bundle downloaded but hotkeys still missing wallet=%s hotkeys=%s",
            wallet_name,
            ",".join(still_missing),
        )
        return False

    logger.info("Wallet synced from control-server wallet=%s", wallet_name)
    return True


def ensure_wallets_from_control_server() -> None:
    cfg = get_control_server_config()
    if not cfg.enabled:
        return

    wallet_root = _expand_wallet_path(os.environ.get("WALLET_PATH", ""))
    pending: list[tuple[str, set[str]]] = []
    for wallet_name in sorted(_collect_wallet_names()):
        hotkeys = _collect_required_hotkeys(wallet_name)
        if not hotkeys:
            logger.warning("No hotkeys configured for wallet=%s; skipping wallet sync", wallet_name)
            continue
        missing = {hotkey for hotkey in hotkeys if not _hotkey_exists(wallet_root, wallet_name, hotkey)}
        if not missing:
            logger.info(
                "Wallet already present locally wallet=%s hotkeys=%s; skip control-server fetch",
                wallet_name,
                ",".join(sorted(hotkeys)),
            )
            continue
        pending.append((wallet_name, missing))

    if not pending:
        logger.info("All configured wallets present locally; skip control-server wallet fetch")
        return

    for wallet_name, missing in pending:
        if not ensure_wallet_from_control_server(wallet_name, missing, wallet_root):
            raise RuntimeError(f"Failed to sync wallet {wallet_name} from control-server")
