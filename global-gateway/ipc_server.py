"""Unix-socket IPC control channel for colocated orchestrators."""

from __future__ import annotations

import asyncio
import json
import logging
import os
from pathlib import Path
from typing import Optional

from routes.orchestrator_control import (
    handle_orchestrator_message,
    register_orchestrator_channel,
    unregister_orchestrator_channel,
)
from transports import IpcOrchestratorChannel

logger = logging.getLogger(__name__)


class PoolCoordinatorIpcServer:
    """Line-delimited JSON IPC server for orchestrator ↔ pool coordinator."""

    def __init__(self, socket_path: str) -> None:
        self._socket_path = socket_path
        self._server: Optional[asyncio.AbstractServer] = None

    @property
    def socket_path(self) -> str:
        return self._socket_path

    async def start(self) -> None:
        path = Path(self._socket_path)
        path.parent.mkdir(parents=True, exist_ok=True)
        if path.exists():
            path.unlink()

        self._server = await asyncio.start_unix_server(self._handle_client, str(path))
        os.chmod(path, 0o660)
        sockets = self._server.sockets or []
        host = sockets[0].getsockname() if sockets else self._socket_path
        logger.info("Pool coordinator IPC listening on %s", host)

    async def stop(self) -> None:
        if self._server is not None:
            self._server.close()
            await self._server.wait_closed()
            self._server = None
        path = Path(self._socket_path)
        if path.exists():
            path.unlink(missing_ok=True)
        logger.info("Pool coordinator IPC stopped")

    async def _handle_client(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
    ) -> None:
        orchestrator_hotkey: Optional[str] = None
        write_lock = asyncio.Lock()
        channel = IpcOrchestratorChannel(writer, write_lock)

        try:
            while True:
                raw = await reader.readline()
                if not raw:
                    break
                try:
                    message = json.loads(raw.decode("utf-8"))
                except json.JSONDecodeError:
                    logger.warning("invalid JSON on orchestrator IPC channel")
                    continue

                msg_type = message.get("type")

                if msg_type == "register":
                    orchestrator_hotkey = str(message.get("orchestrator_hotkey") or "").strip()
                    if not orchestrator_hotkey:
                        await channel.send(
                            {
                                "type": "register_error",
                                "reason": "missing_orchestrator_hotkey",
                            }
                        )
                        break

                    control_secret = str(message.get("control_secret") or "").strip()
                    api_key = str(message.get("api_key") or "").strip()
                    ok = await register_orchestrator_channel(
                        orchestrator_hotkey,
                        channel,
                        control_secret=control_secret,
                        api_key=api_key,
                    )
                    if not ok:
                        await channel.send(
                            {
                                "type": "register_error",
                                "reason": "auth_failed",
                            }
                        )
                        break
                    continue

                if orchestrator_hotkey is None:
                    await channel.send(
                        {
                            "type": "error",
                            "reason": "register_required",
                        }
                    )
                    break

                await handle_orchestrator_message(orchestrator_hotkey, message, channel)

        except Exception as exc:
            logger.warning("orchestrator IPC session error for %s: %s", orchestrator_hotkey, exc)
        finally:
            if orchestrator_hotkey:
                unregister_orchestrator_channel(orchestrator_hotkey)
            try:
                writer.close()
                await writer.wait_closed()
            except Exception:
                pass
