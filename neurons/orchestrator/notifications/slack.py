"""Slack incoming-webhook notifications for orchestrator events."""

from __future__ import annotations

import logging
from typing import Any, Optional

import httpx

logger = logging.getLogger(__name__)


def _display(value: Optional[str]) -> str:
    text = str(value or "").strip()
    return text or "unknown"


class SlackNotifier:
    """Fire-and-forget Slack webhook notifications (no-op when URL is unset)."""

    def __init__(self, webhook_url: Optional[str] = None) -> None:
        self._webhook_url = (webhook_url or "").strip() or None
        self._client: Optional[httpx.AsyncClient] = None

    @property
    def enabled(self) -> bool:
        return bool(self._webhook_url)

    async def start(self) -> None:
        if self.enabled and self._client is None:
            self._client = httpx.AsyncClient(timeout=10.0)

    async def stop(self) -> None:
        if self._client is not None:
            await self._client.aclose()
            self._client = None

    async def _send(self, title: str, fields: dict[str, Any]) -> None:
        if not self.enabled or self._client is None:
            return

        lines = [f"*{title}*"]
        for key, value in fields.items():
            if value is None or value == "":
                continue
            lines.append(f"• {key}: `{value}`")

        payload = {"text": "\n".join(lines)}
        try:
            response = await self._client.post(self._webhook_url, json=payload)
            response.raise_for_status()
            logger.info("Slack notification sent: %s fields=%s", title, fields)
        except Exception as exc:
            logger.warning("Slack notification failed (%s): %s", title, exc)

    async def notify_worker_connected(
        self,
        *,
        worker_id: str,
        client_ip: str,
        hotkey: str,
        reconnect: bool,
    ) -> None:
        title = "Worker reconnected" if reconnect else "New worker connected"
        await self._send(
            title,
            {
                "worker_id": _display(worker_id),
                "ip": client_ip or "unknown",
                "hotkey": _display(hotkey),
            },
        )

    async def notify_worker_disconnected(
        self,
        *,
        worker_id: str,
        client_ip: str = "",
        hotkey: str = "",
    ) -> None:
        await self._send(
            "Worker disconnected",
            {
                "worker_id": _display(worker_id),
                "ip": client_ip or "unknown",
                "hotkey": _display(hotkey),
            },
        )

    async def notify_task_offer(
        self,
        *,
        task_id: str,
        assignment_id: str,
        worker_id: str,
        client_ip: str,
        hotkey: str,
    ) -> None:
        await self._send(
            "Task offer",
            {
                "task_id": _display(task_id),
                "assignment_id": _display(assignment_id),
                "worker_id": _display(worker_id),
                "worker_ip": client_ip or "unknown",
                "hotkey": _display(hotkey),
            },
        )

    async def notify_task_response(
        self,
        *,
        task_id: str,
        worker_id: str,
        accepted: bool,
        client_ip: str = "",
        hotkey: str = "",
        reason: Optional[str] = None,
    ) -> None:
        title = "Task accepted" if accepted else "Task rejected"
        fields: dict[str, Any] = {
            "task_id": _display(task_id),
            "worker_id": _display(worker_id),
            "worker_ip": client_ip or "unknown",
            "hotkey": _display(hotkey),
        }
        if reason:
            fields["reason"] = reason
        await self._send(title, fields)

    async def notify_task_complete(
        self,
        *,
        task_id: str,
        worker_id: str,
        assignment_id: str = "",
        client_ip: str = "",
        hotkey: str = "",
        bytes_transferred: int = 0,
        bandwidth_mbps: float = 0.0,
    ) -> None:
        await self._send(
            "Task complete",
            {
                "task_id": _display(task_id),
                "assignment_id": _display(assignment_id) if assignment_id else None,
                "worker_id": _display(worker_id),
                "worker_ip": client_ip or "unknown",
                "hotkey": _display(hotkey),
                "bytes": bytes_transferred,
                "bandwidth_mbps": f"{bandwidth_mbps:.2f}",
            },
        )
