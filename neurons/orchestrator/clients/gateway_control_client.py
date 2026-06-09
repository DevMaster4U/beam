"""
Client for the orchestrator-owned worker gateway control channel.

Implements the dedicated-gateway topology from the Beam orchestrator guide:
https://data.b1m.ai/guide/orchestrators#dedicated-gateway-websocket-protocol
"""

from __future__ import annotations

import asyncio
import json
import logging
import uuid
from typing import Any, Callable, Optional

import websockets
from websockets.exceptions import ConnectionClosed

logger = logging.getLogger(__name__)


class GatewayControlClient:
    """Maintains the orchestrator ↔ worker-gateway control WebSocket."""

    def __init__(
        self,
        control_url: str,
        control_secret: str,
        *,
        open_timeout: float = 30.0,
        ping_interval: float = 30.0,
        ping_timeout: float = 10.0,
    ) -> None:
        self._control_url = self._normalize_control_url(control_url)
        self._control_secret = control_secret
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

    @staticmethod
    def _normalize_control_url(url: str) -> str:
        trimmed = url.rstrip("/")
        if trimmed.startswith("https://"):
            return "wss://" + trimmed[8:]
        if trimmed.startswith("http://"):
            return "ws://" + trimmed[7:]
        return trimmed

    @property
    def connected(self) -> bool:
        return self._connected

    def set_event_handler(self, handler: Callable) -> None:
        """Handler for gateway push events (worker_connected, worker_response, etc.)."""
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
        if not self._connected:
            logger.warning("Gateway control not connected; cannot send task_offer")
            return False
        try:
            await self._ws.send(
                json.dumps({"type": "task_offer", "worker_id": worker_id, "offer": offer})
            )
            return True
        except Exception as exc:
            logger.error("Failed to send task_offer to gateway: %s", exc)
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
        if not self._connected:
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

    async def send_task_result_summary_ack(
        self,
        worker_id: str,
        task_id: str,
        offer_id: str,
        *,
        received: bool,
        completed: Optional[bool] = None,
        reason: Optional[str] = None,
    ) -> None:
        if not self._connected:
            return
        await self._ws.send(
            json.dumps(
                {
                    "type": "task_result_summary_ack",
                    "worker_id": worker_id,
                    "task_id": task_id,
                    "offer_id": offer_id,
                    "received": received,
                    "completed": completed,
                    "reason": reason,
                }
            )
        )

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
                headers = {"x-control-secret": self._control_secret}
                async with websockets.connect(
                    self._control_url,
                    additional_headers=headers,
                    open_timeout=self._open_timeout,
                    ping_interval=self._ping_interval,
                    ping_timeout=self._ping_timeout,
                ) as ws:
                    self._ws = ws
                    self._connected = True
                    self._reconnect_delay = 5.0
                    logger.info("Connected to worker-gateway control channel: %s", self._control_url)
                    await self._recv_loop(ws)
            except asyncio.CancelledError:
                break
            except Exception as exc:
                logger.warning("Gateway control connection error: %s", exc)
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
                self._local_workers[worker_id] = {
                    "worker_id": worker_id,
                    "client_ip": str(data.get("client_ip") or ""),
                    "hotkey": str(data.get("hotkey") or ""),
                    "bandwidth_mbps": 0.0,
                    "trust_score": 0.8,
                }
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

        if msg_type == "worker_capacity_update":
            worker_id = data.get("worker_id")
            if worker_id and worker_id in self._local_workers:
                worker = self._local_workers[worker_id]
                worker["bandwidth_mbps"] = float(data.get("bandwidth_mbps") or 0.0)
                if data.get("client_ip"):
                    worker["client_ip"] = str(data["client_ip"])
                if data.get("hotkey"):
                    worker["hotkey"] = str(data["hotkey"])
            if self._event_handler:
                await self._dispatch_event(data)
            return

        if self._event_handler:
            await self._dispatch_event(data)

    def _sync_local_workers(self, workers: list[dict[str, Any]]) -> None:
        self._local_workers = {
            w["worker_id"]: {
                **w,
                "trust_score": float(w.get("trust_score") or 0.8),
                "bandwidth_mbps": float(w.get("bandwidth_mbps") or 0.0),
            }
            for w in workers
            if w.get("worker_id")
        }

    async def _dispatch_event(self, data: dict[str, Any]) -> None:
        if not self._event_handler:
            return
        try:
            result = self._event_handler(data)
            if asyncio.iscoroutine(result):
                await result
        except Exception as exc:
            logger.error("Gateway control event handler error: %s", exc)
