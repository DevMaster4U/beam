"""
Client for the in-process worker gateway control channel.

Orchestrator connects to the local worker gateway WebSocket with control_secret:
  ws://127.0.0.1:<API_PORT>/ws/<session_id>?api_key=...&control_secret=...
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


class GatewayControlClient:
    """Maintains the orchestrator ↔ in-process worker-gateway control WebSocket."""

    def __init__(
        self,
        control_url: str,
        control_secret: str,
        *,
        api_key_provider: Optional[ApiKeyProvider] = None,
        open_timeout: float = 30.0,
        ping_interval: float = 30.0,
        ping_timeout: float = 10.0,
    ) -> None:
        self._control_base_url = control_url.rstrip("/")
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

        self._pending_requests: dict[str, asyncio.Future] = {}
        self._event_handler: Optional[Callable] = None
        self._local_workers: dict[str, dict[str, Any]] = {}

    @property
    def connected(self) -> bool:
        return self._connected

    def set_event_handler(self, handler: Callable) -> None:
        self._event_handler = handler

    def get_local_workers(self) -> list[dict[str, Any]]:
        return list(self._local_workers.values())

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

    async def list_workers(self, timeout: float = 10.0) -> list[dict[str, Any]]:
        response = await self._send_request({"type": "list_workers"}, timeout=timeout)
        workers = response.get("workers", [])
        self._sync_local_workers(workers)
        return workers

    async def send_task_offer(self, worker_id: str, offer: dict[str, Any]) -> bool:
        if not self._connected or not self._ws:
            logger.warning("Gateway control not connected; cannot send task_offer")
            return False
        try:
            await self._ws.send(
                json.dumps({"type": "task_offer", "worker_id": worker_id, "offer": offer})
            )
            return True
        except Exception as exc:
            logger.error("Failed to send task_offer to gateway control: %s", exc)
            return False

    async def send_task_accept_ack(
        self,
        worker_id: str,
        task_id: str,
        offer_id: str,
        *,
        accepted: bool,
        reason: Optional[str] = None,
    ) -> None:
        if not self._connected or not self._ws:
            return
        await self._ws.send(
            json.dumps(
                {
                    "type": "task_accept_ack",
                    "worker_id": worker_id,
                    "task_id": task_id,
                    "offer_id": offer_id,
                    "accepted": accepted,
                    "reason": reason,
                }
            )
        )

    async def send_task_result_ack(
        self,
        worker_id: str,
        task_id: str,
        offer_id: str,
        *,
        received: bool,
        completed: Optional[bool] = None,
        reason: Optional[str] = None,
    ) -> None:
        if not self._connected or not self._ws:
            return
        await self._ws.send(
            json.dumps(
                {
                    "type": "task_result_ack",
                    "worker_id": worker_id,
                    "task_id": task_id,
                    "offer_id": offer_id,
                    "received": received,
                    "completed": completed,
                    "reason": reason,
                }
            )
        )

    async def _build_connect_url(self) -> str:
        ws_base = self._control_base_url
        if ws_base.startswith("https://"):
            ws_base = "wss://" + ws_base[8:]
        elif ws_base.startswith("http://"):
            ws_base = "ws://" + ws_base[7:]

        params: dict[str, str] = {"control_secret": self._control_secret}
        if self._api_key_provider:
            api_key = await self._api_key_provider()
            if api_key:
                params["api_key"] = api_key

        query = urlencode(params)
        if "?" in ws_base:
            return f"{ws_base}&{query}"
        return f"{ws_base}?{query}"

    async def _send_request(self, message: dict[str, Any], timeout: float = 10.0) -> dict[str, Any]:
        if not self._ws or not self._connected:
            raise RuntimeError("gateway control websocket is not connected")

        request_id = message.get("request_id") or uuid.uuid4().hex
        future: asyncio.Future = asyncio.get_event_loop().create_future()
        self._pending_requests[request_id] = future

        try:
            await self._ws.send(json.dumps({**message, "request_id": request_id}))
            return await asyncio.wait_for(future, timeout=timeout)
        except Exception:
            self._pending_requests.pop(request_id, None)
            raise

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
                    logger.info("Connected to in-process worker gateway control: %s", url)
                    await self._recv_loop(ws)
            except asyncio.CancelledError:
                break
            except Exception as exc:
                logger.warning("Worker gateway control connection error: %s", exc)
            finally:
                self._connected = False
                self._ws = None
                for fut in self._pending_requests.values():
                    if not fut.done():
                        fut.set_exception(RuntimeError("gateway control disconnected"))
                self._pending_requests.clear()

            if self._running:
                await asyncio.sleep(self._reconnect_delay)
                self._reconnect_delay = min(60.0, self._reconnect_delay * 1.5)

    async def _recv_loop(self, ws: websockets.WebSocketClientProtocol) -> None:
        async for raw in ws:
            try:
                data = json.loads(raw)
            except json.JSONDecodeError:
                logger.warning("Invalid JSON from gateway control channel")
                continue
            await self._handle_message(data)

    async def _handle_message(self, data: dict[str, Any]) -> None:
        msg_type = data.get("type")
        request_id = data.get("request_id")

        if request_id and request_id in self._pending_requests:
            fut = self._pending_requests.pop(request_id)
            if not fut.done():
                fut.set_result(data)
            return

        if msg_type == "control_connected":
            self._sync_local_workers(data.get("workers", []))
            return

        if msg_type == "worker_connected":
            worker_id = data.get("worker_id")
            if worker_id:
                self._local_workers[worker_id] = {"worker_id": worker_id}
            if self._event_handler:
                await self._dispatch_event(data)
            return

        if msg_type == "worker_disconnected":
            worker_id = data.get("worker_id")
            if worker_id:
                self._local_workers.pop(worker_id, None)
            if self._event_handler:
                await self._dispatch_event(data)
            return

        if self._event_handler:
            await self._dispatch_event(data)

    def _sync_local_workers(self, workers: list[dict[str, Any]]) -> None:
        self._local_workers = {
            w["worker_id"]: dict(w)
            for w in workers
            if isinstance(w, dict) and w.get("worker_id")
        }

    async def _dispatch_event(self, data: dict[str, Any]) -> None:
        if not self._event_handler:
            return

        handler = self._event_handler

        async def _run() -> None:
            try:
                result = handler(data)
                if asyncio.iscoroutine(result):
                    await result
            except Exception as exc:
                logger.error("Gateway control event handler error: %s", exc)

        asyncio.create_task(_run())


def build_local_control_ws_url(
    *,
    host: str,
    port: int,
    session_id: str,
) -> str:
    """Build http:// host URL for GatewayControlClient (converted to ws:// internally)."""
    base = f"http://{host}:{port}/ws/{session_id}"
    return base
