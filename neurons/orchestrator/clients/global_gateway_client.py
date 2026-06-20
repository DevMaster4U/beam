"""
Client for the shared global worker gateway control channel.

Orchestrators connect with control_secret and forward task_offer_batch to the gateway.
Worker accept/reject/result messages are routed back by offer_id/task_id mapping on the gateway.
"""

from __future__ import annotations

import asyncio
import json
import logging
import uuid
from typing import Any, Awaitable, Callable, Optional
from urllib.parse import urlencode

import websockets

logger = logging.getLogger(__name__)

ApiKeyProvider = Callable[[], Awaitable[Optional[str]]]
WorkerMessageHandler = Callable[[str, dict], Awaitable[None]]
PoolStatusHandler = Callable[[int], Awaitable[None]]


class GlobalGatewayClient:
    """Maintains orchestrator ↔ global-gateway control WebSocket."""

    def __init__(
        self,
        control_base_url: str,
        orchestrator_hotkey: str,
        control_secret: str,
        *,
        api_key_provider: Optional[ApiKeyProvider] = None,
        open_timeout: float = 30.0,
        ping_interval: float = 30.0,
        ping_timeout: float = 45.0,
    ) -> None:
        self._control_base_url = control_base_url.rstrip("/")
        self._orchestrator_hotkey = orchestrator_hotkey
        self._control_secret = control_secret
        self._api_key_provider = api_key_provider
        self._open_timeout = open_timeout
        self._ping_interval = ping_interval
        self._ping_timeout = ping_timeout

        self._ws: Optional[websockets.WebSocketClientProtocol] = None
        self._connected = False
        self._running = False
        self._task: Optional[asyncio.Task] = None
        self._reconnect_delay = 5.0

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
        if self._ws:
            await self._ws.close()
        self._ws = None
        self._connected = False

    async def send_task_offer_batch(self, batch_id: str, offers: list[dict]) -> tuple[int, int]:
        if not self._connected or not self._ws:
            logger.warning("Global gateway not connected; cannot send task_offer_batch")
            return 0, len(offers)
        try:
            await self._ws.send(
                json.dumps(
                    {
                        "type": "task_offer_batch",
                        "batch_id": batch_id,
                        "offers": offers,
                    }
                )
            )
            return len(offers), 0
        except Exception as exc:
            logger.error("Failed to send task_offer_batch to global gateway: %s", exc)
            return 0, len(offers)

    async def send_to_worker(self, worker_id: str, payload: dict) -> bool:
        if not self._connected or not self._ws:
            return False
        try:
            await self._ws.send(
                json.dumps(
                    {
                        "type": "to_worker",
                        "worker_id": worker_id,
                        "payload": payload,
                    }
                )
            )
            return True
        except Exception as exc:
            logger.warning("send_to_worker failed for %s: %s", worker_id, exc)
            return False

    async def _build_connect_url(self) -> str:
        ws_base = self._control_base_url
        if ws_base.startswith("https://"):
            ws_base = "wss://" + ws_base[8:]
        elif ws_base.startswith("http://"):
            ws_base = "ws://" + ws_base[7:]

        path = f"/ws/orchestrators/{self._orchestrator_hotkey}"
        if not ws_base.endswith(path):
            ws_base = f"{ws_base.rstrip('/')}{path}"

        params: dict[str, str] = {"control_secret": self._control_secret}
        if self._api_key_provider:
            api_key = await self._api_key_provider()
            if api_key:
                params["api_key"] = api_key

        query = urlencode(params)
        if "?" in ws_base:
            return f"{ws_base}&{query}"
        return f"{ws_base}?{query}"

    async def _connection_loop(self) -> None:
        while self._running:
            try:
                url = await self._build_connect_url()
                async with websockets.connect(
                    url,
                    open_timeout=self._open_timeout,
                    ping_interval=self._ping_interval,
                    ping_timeout=self._ping_timeout,
                ) as ws:
                    self._ws = ws
                    self._connected = True
                    self._reconnect_delay = 5.0
                    logger.info("Connected to global worker gateway control: %s", url)
                    await self._recv_loop(ws)
            except asyncio.CancelledError:
                break
            except Exception as exc:
                logger.warning("Global gateway control connection error: %s", exc)
            finally:
                self._connected = False
                self._ws = None

            if self._running:
                await asyncio.sleep(self._reconnect_delay)
                self._reconnect_delay = min(60.0, self._reconnect_delay * 1.5)

    async def _recv_loop(self, ws: websockets.WebSocketClientProtocol) -> None:
        async for raw in ws:
            try:
                data = json.loads(raw)
            except json.JSONDecodeError:
                logger.warning("Invalid JSON from global gateway control channel")
                continue
            await self._handle_message(data)

    async def _handle_message(self, data: dict[str, Any]) -> None:
        msg_type = data.get("type")

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
                await self._worker_message_handler(worker_id, message)
            return

        if msg_type == "task_offer_batch_result":
            logger.info(
                "global gateway batch result: batch=%s delivered=%s failed=%s",
                data.get("batch_id"),
                data.get("delivered"),
                data.get("failed"),
            )
            return

        logger.debug("Unhandled global gateway message type=%s", msg_type)


def build_global_gateway_control_url(host: str, port: int) -> str:
    return f"http://{host}:{port}"
