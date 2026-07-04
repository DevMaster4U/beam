"""
Client for the shared global worker gateway control channel.

Orchestrators connect with control_secret and forward task_offer_batch to the gateway.
Worker accept/reject/result messages are routed back by offer_id/task_id mapping on the gateway.
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
import uuid
from typing import Any, Awaitable, Callable, Optional
from urllib.parse import urlencode

import websockets
from websockets.exceptions import ConnectionClosed

from core.relay_log import defer_relay_log, is_failure_summary, log_relay, relay_summary, short_id, SLOW_RELAY_MS

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
        send_api_key: bool = False,
        open_timeout: float = 30.0,
        ping_interval: float = 30.0,
        ping_timeout: float = 45.0,
    ) -> None:
        self._control_base_url = control_base_url.rstrip("/")
        self._orchestrator_hotkey = orchestrator_hotkey
        self._control_secret = control_secret
        self._api_key_provider = api_key_provider
        self._send_api_key = send_api_key
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
            log_relay(
                f"global gateway ws -> send task_offer_batch batch={short_id(batch_id, 12)} "
                f"offers={len(offers)}",
                force_info=True,
            )
            return len(offers), 0
        except Exception as exc:
            logger.error("Failed to send task_offer_batch to global gateway: %s", exc)
            return 0, len(offers)

    async def send_to_worker(self, worker_id: str, payload: dict) -> bool:
        if not self._connected or not self._ws:
            return False
        msg_type = str(payload.get("type") or "?")
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
            defer_relay_log(
                f"global gateway ws -> send to_worker worker={short_id(worker_id)} "
                f"payload_type={msg_type} task={short_id(payload.get('task_id'))} "
                f"offer={short_id(payload.get('offer_id') or payload.get('task_id'))} "
                f"{relay_summary(payload)}",
                force_info=is_failure_summary(relay_summary(payload)),
            )
            return True
        except Exception as exc:
            logger.warning("send_to_worker failed for %s payload_type=%s: %s", worker_id, msg_type, exc)
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
        if self._send_api_key and self._api_key_provider:
            api_key = await self._api_key_provider()
            if api_key:
                params["api_key"] = api_key

        query = urlencode(params)
        if "?" in ws_base:
            return f"{ws_base}&{query}"
        return f"{ws_base}?{query}"

    @staticmethod
    def _redact_connect_url(url: str) -> str:
        """Log-safe control URL (never print api_key query values)."""
        if "api_key=" not in url:
            return url
        base, _, query = url.partition("?")
        parts = []
        for piece in query.split("&"):
            if piece.startswith("api_key="):
                parts.append("api_key=***")
            else:
                parts.append(piece)
        return f"{base}?{'&'.join(parts)}"

    def _log_control_disconnect(self, exc: Exception) -> None:
        if isinstance(exc, ConnectionClosed) and exc.code == 1008:
            logger.warning(
                "Global gateway control connection rejected (1008 policy violation). "
                "Check ORCHESTRATOR_GATEWAY_SECRET matches the gateway's "
                "ORCHESTRATOR_GATEWAY_SECRET on %s. If GLOBAL_GATEWAY_CONTROL_USE_API_KEY=true, "
                "the gateway must reach CORE_SERVER_URL to validate the orchestrator API key.",
                self._control_base_url,
            )
            return
        logger.warning("Global gateway control connection error: %s", exc)

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
                    logger.info(
                        "Connected to global worker gateway control: %s",
                        self._redact_connect_url(url),
                    )
                    await self._recv_loop(ws)
            except asyncio.CancelledError:
                break
            except Exception as exc:
                self._log_control_disconnect(exc)
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
                log_relay(
                    f"global gateway ws <- recv from_worker worker={short_id(worker_id)} "
                    f"type={message.get('type') or '?'} task={short_id(message.get('task_id'))} "
                    f"offer={short_id(message.get('offer_id') or message.get('task_id'))}"
                )
                # Do not block the control recv loop on BeamCore round-trips; workers
                # finish in parallel and each needs its own ack path.
                asyncio.create_task(
                    self._dispatch_worker_message(worker_id, message),
                    name=f"global-gateway-relay-{worker_id[:8]}",
                )
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
                "global gateway relay failed worker=%s type=%s offer=%s latency_ms=%.1f: %s",
                short_id(worker_id),
                msg_type,
                short_id(offer_id),
                elapsed_ms,
                exc,
            )
            return
        elapsed_ms = (time.monotonic() - started) * 1000
        defer_relay_log(
            f"global gateway relay done worker={short_id(worker_id)} type={msg_type} "
            f"offer={short_id(offer_id)}",
            latency_ms=elapsed_ms,
            force_info=failed or elapsed_ms >= SLOW_RELAY_MS,
        )


def build_global_gateway_control_url(host: str, port: int) -> str:
    return f"http://{host}:{port}"
