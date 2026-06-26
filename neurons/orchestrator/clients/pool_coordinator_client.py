"""
Client for colocated pool coordinator IPC (Phase 1).

Orchestrators on the same host connect via unix socket instead of a control WebSocket
to the global gateway. Workers still use the public worker WebSocket on GATEWAY_PORT.
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
from pathlib import Path
from typing import Any, Awaitable, Callable, Optional

from core.relay_log import defer_relay_log, is_failure_summary, log_relay, relay_summary, short_id, SLOW_RELAY_MS

logger = logging.getLogger(__name__)

ApiKeyProvider = Callable[[], Awaitable[Optional[str]]]
WorkerMessageHandler = Callable[[str, dict], Awaitable[None]]
PoolStatusHandler = Callable[[int], Awaitable[None]]


class PoolCoordinatorClient:
    """Maintains orchestrator ↔ pool-coordinator unix-socket control channel."""

    def __init__(
        self,
        ipc_socket_path: str,
        orchestrator_hotkey: str,
        control_secret: str,
        *,
        api_key_provider: Optional[ApiKeyProvider] = None,
    ) -> None:
        self._ipc_socket_path = str(ipc_socket_path)
        self._orchestrator_hotkey = orchestrator_hotkey
        self._control_secret = control_secret
        self._api_key_provider = api_key_provider

        self._reader: Optional[asyncio.StreamReader] = None
        self._writer: Optional[asyncio.StreamWriter] = None
        self._write_lock = asyncio.Lock()
        self._connected = False
        self._running = False
        self._task: Optional[asyncio.Task] = None
        self._reconnect_delay = 2.0

        self._worker_message_handler: Optional[WorkerMessageHandler] = None
        self._pool_status_handler: Optional[PoolStatusHandler] = None

    @property
    def connected(self) -> bool:
        return self._connected

    def set_worker_message_handler(self, handler: WorkerMessageHandler) -> None:
        self._worker_message_handler = handler

    def set_pool_status_handler(self, handler: PoolStatusHandler) -> None:
        self._pool_status_handler = handler

    async def start(self) -> None:
        if self._running:
            return
        self._running = True
        self._task = asyncio.create_task(self._connection_loop())

    async def stop(self) -> None:
        self._running = False
        if self._task and not self._task.done():
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        await self._close_connection()

    async def send_task_offer_batch(self, batch_id: str, offers: list[dict]) -> tuple[int, int]:
        if not self._connected or self._writer is None:
            logger.warning("Pool coordinator IPC not connected; cannot send task_offer_batch")
            return 0, len(offers)
        try:
            await self._send(
                {
                    "type": "task_offer_batch",
                    "batch_id": batch_id,
                    "offers": offers,
                }
            )
            log_relay(
                f"pool coordinator ipc -> send task_offer_batch batch={short_id(batch_id, 12)} "
                f"offers={len(offers)}",
                force_info=True,
            )
            return len(offers), 0
        except Exception as exc:
            logger.error("Failed to send task_offer_batch via pool coordinator IPC: %s", exc)
            return 0, len(offers)

    async def send_to_worker(self, worker_id: str, payload: dict) -> bool:
        if not self._connected or self._writer is None:
            return False
        msg_type = str(payload.get("type") or "?")
        try:
            await self._send(
                {
                    "type": "to_worker",
                    "worker_id": worker_id,
                    "payload": payload,
                }
            )
            defer_relay_log(
                f"pool coordinator ipc -> send to_worker worker={short_id(worker_id)} "
                f"payload_type={msg_type} task={short_id(payload.get('task_id'))} "
                f"offer={short_id(payload.get('offer_id') or payload.get('task_id'))} "
                f"{relay_summary(payload)}",
                force_info=is_failure_summary(relay_summary(payload)),
            )
            return True
        except Exception as exc:
            logger.warning(
                "send_to_worker via IPC failed for %s payload_type=%s: %s",
                worker_id,
                msg_type,
                exc,
            )
            return False

    async def _send(self, payload: dict) -> None:
        if self._writer is None:
            raise RuntimeError("pool coordinator IPC not connected")
        line = json.dumps(payload, separators=(",", ":")) + "\n"
        async with self._write_lock:
            self._writer.write(line.encode("utf-8"))
            await self._writer.drain()

    async def _close_connection(self) -> None:
        self._connected = False
        if self._writer is not None:
            try:
                self._writer.close()
                await self._writer.wait_closed()
            except Exception:
                pass
        self._reader = None
        self._writer = None

    async def _connection_loop(self) -> None:
        while self._running:
            try:
                path = Path(self._ipc_socket_path)
                if not path.exists():
                    raise FileNotFoundError(f"pool coordinator IPC socket missing: {path}")

                reader, writer = await asyncio.open_unix_connection(str(path))
                self._reader = reader
                self._writer = writer
                self._connected = True
                self._reconnect_delay = 2.0

                api_key = ""
                if self._api_key_provider:
                    key = await self._api_key_provider()
                    if key:
                        api_key = key

                await self._send(
                    {
                        "type": "register",
                        "orchestrator_hotkey": self._orchestrator_hotkey,
                        "control_secret": self._control_secret,
                        "api_key": api_key,
                    }
                )
                logger.info(
                    "Connected to pool coordinator IPC: %s (hotkey=%s)",
                    self._ipc_socket_path,
                    self._orchestrator_hotkey,
                )
                await self._recv_loop(reader)
            except asyncio.CancelledError:
                break
            except Exception as exc:
                logger.warning("Pool coordinator IPC connection error: %s", exc)
            finally:
                await self._close_connection()

            if self._running:
                await asyncio.sleep(self._reconnect_delay)
                self._reconnect_delay = min(30.0, self._reconnect_delay * 1.5)

    async def _recv_loop(self, reader: asyncio.StreamReader) -> None:
        while self._running:
            raw = await reader.readline()
            if not raw:
                break
            try:
                data = json.loads(raw.decode("utf-8"))
            except json.JSONDecodeError:
                logger.warning("Invalid JSON from pool coordinator IPC")
                continue
            await self._handle_message(data)

    async def _handle_message(self, data: dict[str, Any]) -> None:
        msg_type = data.get("type")

        if msg_type == "register_error":
            logger.error(
                "Pool coordinator IPC register failed: %s",
                data.get("reason") or "unknown",
            )
            await self._close_connection()
            return

        if msg_type == "control_connected":
            count = int(data.get("worker_count") or 0)
            if self._pool_status_handler:
                await self._pool_status_handler(count)
            return

        if msg_type == "pool_status":
            count = int(data.get("worker_count") or 0)
            if self._pool_status_handler:
                await self._pool_status_handler(count)
            return

        if msg_type == "from_worker":
            worker_id = str(data.get("worker_id") or "")
            message = data.get("message")
            if worker_id and isinstance(message, dict) and self._worker_message_handler:
                log_relay(
                    f"pool coordinator ipc <- recv from_worker worker={short_id(worker_id)} "
                    f"type={message.get('type') or '?'} task={short_id(message.get('task_id'))} "
                    f"offer={short_id(message.get('offer_id') or message.get('task_id'))}"
                )
                asyncio.create_task(
                    self._dispatch_worker_message(worker_id, message),
                    name=f"pool-coordinator-relay-{worker_id[:8]}",
                )
            return

        if msg_type == "task_offer_batch_result":
            logger.info(
                "pool coordinator batch result: batch=%s delivered=%s failed=%s",
                data.get("batch_id"),
                data.get("delivered"),
                data.get("failed"),
            )
            return

        logger.debug("Unhandled pool coordinator IPC message type=%s", msg_type)

    async def _dispatch_worker_message(self, worker_id: str, message: dict) -> None:
        handler = self._worker_message_handler
        if handler is None:
            return
        msg_type = message.get("type")
        offer_id = message.get("offer_id") or message.get("task_id")
        started = time.monotonic()
        failed = False
        try:
            await handler(worker_id, message)
        except Exception as exc:
            failed = True
            elapsed_ms = (time.monotonic() - started) * 1000
            logger.warning(
                "pool coordinator relay failed worker=%s type=%s offer=%s latency_ms=%.1f: %s",
                short_id(worker_id),
                msg_type,
                short_id(offer_id),
                elapsed_ms,
                exc,
            )
            return
        elapsed_ms = (time.monotonic() - started) * 1000
        defer_relay_log(
            f"pool coordinator relay done worker={short_id(worker_id)} type={msg_type} "
            f"offer={short_id(offer_id)}",
            latency_ms=elapsed_ms,
            force_info=failed or elapsed_ms >= SLOW_RELAY_MS,
        )
