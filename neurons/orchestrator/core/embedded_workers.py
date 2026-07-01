"""
In-process embedded workers for WORKER_GATEWAY_MODE=embedded.

Workers register with BeamCore over HTTP at orchestrator startup. Task batches
from BeamCore are executed locally (no worker WebSocket gateway hop).
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import os
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Set

import bittensor as bt
import httpx

from core.config import OrchestratorSettings
from core.relay_log import short_id
from core.transfer_loader import get_transfer_module

logger = logging.getLogger(__name__)


@dataclass
class EmbeddedWorkerConfig:
    slot: int
    wallet_name: str
    hotkey: str
    initial_order: int = 0
    max_concurrent_tasks: int = 4


@dataclass
class EmbeddedWorker:
    slot: int
    wallet: Any
    worker_id: str
    api_key: str
    hotkey: str
    ip: str
    state: Any
    initial_order: int = 0
    max_concurrent_tasks: int = 4
    active_offer_ids: Set[str] = field(default_factory=set)

    @property
    def active_count(self) -> int:
        return len(self.active_offer_ids)

    @property
    def has_capacity(self) -> bool:
        return self.active_count < self.max_concurrent_tasks

    def round_robin_sort_key(self) -> tuple:
        if self.active_count == 0:
            return (0, -self.initial_order, self.worker_id)
        return (1, self.worker_id)


def parse_embedded_worker_configs(settings: OrchestratorSettings) -> List[EmbeddedWorkerConfig]:
    """Parse WORKER_1, WORKER_2, ... from the orchestrator env file."""
    configs: List[EmbeddedWorkerConfig] = []
    default_wallet = os.environ.get("WORKER_WALLET_NAME", settings.wallet_name).strip()
    default_max_tasks = max(1, int(os.environ.get("WORKER_MAX_CONCURRENT_TASKS", "4")))

    idx = 1
    while True:
        combined = os.environ.get(f"WORKER_{idx}", "").strip()
        hotkey = os.environ.get(f"WORKER_{idx}_HOTKEY", "").strip()
        wallet_name = os.environ.get(f"WORKER_{idx}_WALLET_NAME", "").strip()

        if combined:
            if ":" in combined:
                wallet_part, hotkey_part = combined.split(":", 1)
                wallet_name = wallet_name or wallet_part.strip()
                hotkey = hotkey or hotkey_part.strip()
            elif not hotkey:
                hotkey = combined

        if not hotkey:
            break

        if not wallet_name:
            wallet_name = default_wallet

        order_raw = os.environ.get(f"WORKER_{idx}_ORDER", str(idx - 1)).strip()
        try:
            initial_order = int(order_raw)
        except ValueError:
            initial_order = idx - 1

        max_tasks_raw = os.environ.get(f"WORKER_{idx}_MAX_CONCURRENT_TASKS", "").strip()
        if max_tasks_raw:
            try:
                max_concurrent = max(1, int(max_tasks_raw))
            except ValueError:
                max_concurrent = default_max_tasks
        else:
            max_concurrent = default_max_tasks

        configs.append(
            EmbeddedWorkerConfig(
                slot=idx,
                wallet_name=wallet_name,
                hotkey=hotkey,
                initial_order=initial_order,
                max_concurrent_tasks=max_concurrent,
            )
        )
        idx += 1

    return configs


class EmbeddedWorkerPool:
    """Runs worker transfer logic inside the orchestrator process."""

    def __init__(self, settings: OrchestratorSettings, upstream: Any) -> None:
        self.settings = settings
        self.upstream = upstream
        self.workers: List[EmbeddedWorker] = []
        self.http_client: Optional[httpx.AsyncClient] = None
        self._cursor = 0
        self._task_handles: Set[asyncio.Task] = set()

    @property
    def worker_count(self) -> int:
        return len(self.workers)

    async def start(self) -> None:
        transfer = get_transfer_module()
        configs = parse_embedded_worker_configs(self.settings)
        if not configs:
            raise ValueError(
                "WORKER_GATEWAY_MODE=embedded requires WORKER_1 (or WORKER_1_HOTKEY) in env"
            )

        self.http_client = httpx.AsyncClient(
            timeout=httpx.Timeout(120.0, connect=30.0),
            limits=httpx.Limits(max_connections=64, max_keepalive_connections=32),
        )

        for cfg in configs:
            wallet = bt.Wallet(
                name=cfg.wallet_name,
                hotkey=cfg.hotkey,
                path=self.settings.wallet_path,
            )
            hotkey = wallet.hotkey.ss58_address
            state = transfer.WorkerState(
                wallet=wallet,
                api_url=self.settings.core_server_url,
                http_client=self.http_client,
            )
            state.prewarm_origins = transfer.load_prewarm_origins_from_disk()

            logger.info(
                "Registering embedded worker slot=%s wallet=%s hotkey=%s",
                cfg.slot,
                cfg.wallet_name,
                hotkey[:16],
            )
            data = await transfer.register_worker(self.http_client, state)
            worker_id = str(data.get("worker_id") or "")
            api_key = str(data.get("api_key") or "")
            if not worker_id or not api_key:
                raise RuntimeError(f"Embedded worker registration failed for slot {cfg.slot}")

            state.worker_id = worker_id
            state.api_key = api_key
            state.worker_ip = await transfer.get_public_ip()

            worker = EmbeddedWorker(
                slot=cfg.slot,
                wallet=wallet,
                worker_id=worker_id,
                api_key=api_key,
                hotkey=hotkey,
                ip=state.worker_ip or "",
                state=state,
                initial_order=cfg.initial_order,
                max_concurrent_tasks=cfg.max_concurrent_tasks,
            )
            self.workers.append(worker)
            logger.info(
                "Embedded worker ready slot=%s worker_id=%s hotkey=%s",
                cfg.slot,
                short_id(worker_id),
                hotkey[:16],
            )

    async def stop(self) -> None:
        for task in list(self._task_handles):
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await task
        self._task_handles.clear()
        if self.http_client is not None:
            await self.http_client.aclose()
            self.http_client = None

    def _select_worker(
        self,
        batch_used_ips: Optional[set[str]] = None,
        batch_assigned_workers: Optional[set[str]] = None,
    ) -> Optional[EmbeddedWorker]:
        if not self.workers:
            return None

        pool_size = len(self.workers)
        start = self._cursor % pool_size
        in_batch = batch_used_ips is not None or batch_assigned_workers is not None

        def _eligible(worker: EmbeddedWorker, *, allow_used_ip: bool) -> bool:
            if not worker.has_capacity:
                return False
            if batch_assigned_workers and worker.worker_id in batch_assigned_workers:
                return False
            ip = worker.ip.strip()
            if (
                not allow_used_ip
                and batch_used_ips is not None
                and ip
                and ip in batch_used_ips
            ):
                return False
            return True

        def _pick(allow_used_ip: bool) -> Optional[EmbeddedWorker]:
            for offset in range(pool_size):
                idx = (start + offset) % pool_size
                worker = self.workers[idx]
                if not _eligible(worker, allow_used_ip=allow_used_ip):
                    continue
                self._cursor = (idx + 1) % pool_size
                return worker
            return None

        if in_batch:
            worker = _pick(allow_used_ip=False)
            if worker:
                return worker
            return _pick(allow_used_ip=True)
        return _pick(allow_used_ip=True)

    async def deliver_task_offer_batch(self, batch_id: str, offers: list[dict]) -> tuple[int, int]:
        delivered = 0
        failed = 0
        batch_used_ips: set[str] = set()
        batch_assigned_workers: set[str] = set()

        for offer in offers:
            if not isinstance(offer, dict):
                failed += 1
                continue

            worker = self._select_worker(batch_used_ips, batch_assigned_workers)
            if worker is None:
                logger.warning(
                    "No embedded worker capacity for batch=%s task=%s",
                    batch_id,
                    offer.get("task_id"),
                )
                failed += 1
                continue

            offer_id = str(offer.get("offer_id") or offer.get("task_id") or "")
            worker.active_offer_ids.add(offer_id)
            batch_assigned_workers.add(worker.worker_id)
            if worker.ip:
                batch_used_ips.add(worker.ip)

            task = asyncio.create_task(self._handle_offer(worker, offer))
            self._task_handles.add(task)
            task.add_done_callback(self._task_handles.discard)
            delivered += 1

        logger.info(
            "Embedded batch queued: batch=%s offers=%s delivered=%s failed=%s",
            short_id(batch_id, 12),
            len(offers),
            delivered,
            failed,
        )
        return delivered, failed

    async def _handle_offer(self, worker: EmbeddedWorker, offer: dict) -> None:
        transfer = get_transfer_module()
        task_id = offer.get("task_id") or offer.get("offer_id")
        offer_id = offer.get("offer_id") or task_id
        deadline_us = int(offer.get("deadline_us") or 0)
        estimated_bytes = transfer.estimate_task_bytes(offer)

        try:
            transfer_context, validation_error = transfer.build_transfer_context(offer)
            if validation_error or transfer_context is None:
                reason = (
                    validation_error
                    if validation_error == "unsupported_worker_version"
                    else f"invalid_offer:{validation_error or 'unknown'}"
                )
                await self._send_reject(worker, task_id, offer_id, reason)
                return

            capacity_error = self._reserve_capacity(worker, estimated_bytes)
            if capacity_error:
                await self._send_reject(worker, task_id, offer_id, capacity_error)
                return

            transfer.log_predefined_etag_fast_path_skipped(
                offer, transfer_context, log_prefix="[Embedded]"
            )

            if transfer.uses_predefined_etag_early_submit(transfer_context):
                await self._handle_predefined_etag_offer(
                    worker,
                    offer,
                    task_id,
                    offer_id,
                    transfer_context,
                    deadline_us,
                )
            else:
                await self._handle_standard_offer(
                    worker,
                    offer,
                    task_id,
                    offer_id,
                    transfer_context,
                    deadline_us,
                )
        finally:
            worker.active_offer_ids.discard(str(offer_id or ""))

    def _reserve_capacity(self, worker: EmbeddedWorker, estimated_bytes: int) -> Optional[str]:
        transfer = get_transfer_module()
        if estimated_bytes > transfer.MAX_IN_FLIGHT_BYTES:
            return f"task_too_large:{estimated_bytes}"
        if not worker.has_capacity:
            return f"queue_full:{worker.active_count}"
        return None

    async def _send_accept(
        self, worker: EmbeddedWorker, task_id: str, offer_id: str
    ) -> dict:
        transfer = get_transfer_module()
        return await self.upstream.send_task_accept(
            task_id=str(task_id),
            worker_id=worker.worker_id,
            offer_id=str(offer_id),
            worker_version=transfer.WORKER_VERSION,
        )

    async def _send_reject(
        self, worker: EmbeddedWorker, task_id: str, offer_id: str, reason: str
    ) -> None:
        await self.upstream.send_task_reject(
            task_id=str(task_id),
            worker_id=worker.worker_id,
            offer_id=str(offer_id),
            reason=reason,
        )

    async def _send_result(
        self,
        worker: EmbeddedWorker,
        task_id: str,
        offer_id: str,
        *,
        success: bool,
        chunk_hash: str = "",
        etag: Optional[str] = None,
        error: Optional[str] = None,
    ) -> dict:
        payload = {
            "task_id": str(task_id),
            "offer_id": str(offer_id),
            "worker_id": worker.worker_id,
            "success": success,
        }
        if chunk_hash:
            payload["chunk_hash"] = chunk_hash
        if etag:
            payload["etag"] = etag
        if error:
            payload["error"] = error
        ack = await self.upstream.send_task_result(payload)
        if ack.get("completed"):
            logger.info(
                "Embedded task completed on BeamCore: task=%s offer=%s worker=%s",
                short_id(task_id),
                short_id(offer_id),
                short_id(worker.worker_id),
            )
        return ack

    async def _handle_predefined_etag_offer(
        self,
        worker: EmbeddedWorker,
        offer: dict,
        task_id: str,
        offer_id: str,
        transfer_context: dict,
        deadline_us: int,
    ) -> None:
        transfer = get_transfer_module()
        offer_started_at = time.perf_counter()
        fetch_ready = transfer.FetchReadyState()

        exec_task = asyncio.create_task(
            transfer.execute_task_with_metrics(
                worker.state,
                str(task_id),
                offer,
                transfer_context,
                deadline_us,
                log_prefix="[Embedded]",
                fetch_ready=fetch_ready,
            )
        )
        upload_task = asyncio.create_task(
            transfer.run_predefined_etag_background_upload(
                worker.state.http_client,
                fetch_ready,
                transfer_context,
                task_id=str(task_id),
                offer_id=str(offer_id),
            )
        )

        logger.info(
            "Embedded predefined ETag: download + task_accept in parallel: task=%s offer=%s",
            short_id(task_id),
            short_id(offer_id),
        )

        async def _accept_ok() -> bool:
            resp = await self._send_accept(worker, task_id, offer_id)
            return bool(resp.get("accepted"))

        accepted, wait_error = await transfer.wait_accept_and_buffered_fetch(
            _accept_ok(),
            fetch_ready,
            accept_timeout=float(
                os.environ.get("WORKER_TASK_ACCEPT_ACK_TIMEOUT", "8.0")
            ),
            fetch_timeout=transfer.FETCH_TIMEOUT + 5.0,
        )

        if not accepted:
            logger.warning(
                "Embedded stopping download: task=%s offer=%s worker=%s reason=%s",
                short_id(task_id),
                short_id(offer_id),
                short_id(worker.worker_id),
                wait_error,
            )
            exec_task.cancel()
            upload_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await exec_task
                await upload_task
            return

        if wait_error:
            exec_task.cancel()
            upload_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await exec_task
                await upload_task
            await self._send_result(
                worker,
                task_id,
                offer_id,
                success=False,
                chunk_hash=fetch_ready.chunk_hash,
                error=wait_error,
            )
            return

        bytes_error = transfer.validate_fetch_ready_bytes(fetch_ready, transfer_context)
        if bytes_error:
            logger.info(
                "Embedded falling back to standard transfer (%s): task=%s offer=%s",
                bytes_error,
                short_id(task_id),
                short_id(offer_id),
            )
            exec_task.cancel()
            upload_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await exec_task
                await upload_task
            result = await transfer.execute_task_with_metrics(
                worker.state,
                str(task_id),
                offer,
                transfer_context,
                deadline_us,
                log_prefix="[Embedded]",
            )
            await self._send_result(
                worker,
                task_id,
                offer_id,
                success=result.success,
                chunk_hash=result.chunk_hash,
                etag=result.etag,
                error=result.error_msg,
            )
            return

        waited_sec = await transfer.wait_predefined_etag_min_submit_delay(offer_started_at)
        if waited_sec > 0:
            logger.info(
                "Embedded accept_ack + hash ready, waited %.3fs (min_submit=%.3fs) "
                "before submit: task=%s offer=%s",
                waited_sec,
                transfer.PREDEFINED_ETAG_MIN_SUBMIT_SEC,
                short_id(task_id),
                short_id(offer_id),
            )
        else:
            logger.info(
                "Embedded accept_ack + hash ready, submitting: task=%s offer=%s",
                short_id(task_id),
                short_id(offer_id),
            )
        await self._send_result(
            worker,
            task_id,
            offer_id,
            success=True,
            chunk_hash=fetch_ready.chunk_hash,
            etag=fetch_ready.etag or transfer.PREDEFINED_ETAG,
        )
        asyncio.create_task(
            self._await_background_upload_task(upload_task, task_id, offer_id)
        )

    async def _handle_standard_offer(
        self,
        worker: EmbeddedWorker,
        offer: dict,
        task_id: str,
        offer_id: str,
        transfer_context: dict,
        deadline_us: int,
    ) -> None:
        transfer = get_transfer_module()
        accept_resp = await self._send_accept(worker, task_id, offer_id)
        if not accept_resp.get("accepted"):
            reason = accept_resp.get("reason") or "task_accept_rejected"
            logger.warning(
                "Embedded accept rejected: task=%s offer=%s worker=%s reason=%s",
                short_id(task_id),
                short_id(offer_id),
                short_id(worker.worker_id),
                reason,
            )
            return

        result = await transfer.execute_task_with_metrics(
            worker.state,
            str(task_id),
            offer,
            transfer_context,
            deadline_us,
            log_prefix="[Embedded]",
        )
        await self._send_result(
            worker,
            task_id,
            offer_id,
            success=result.success,
            chunk_hash=result.chunk_hash,
            etag=result.etag,
            error=result.error_msg,
        )

    async def _await_background_upload_task(
        self, upload_task: asyncio.Task, task_id: str, offer_id: str
    ) -> None:
        try:
            ok = await upload_task
        except asyncio.CancelledError:
            logger.warning(
                "Embedded upload cancelled: task=%s offer=%s",
                short_id(task_id),
                short_id(offer_id),
            )
            return
        except Exception as exc:
            logger.warning(
                "Embedded upload error: task=%s offer=%s err=%s",
                short_id(task_id),
                short_id(offer_id),
                exc,
            )
            return

        if ok:
            logger.info(
                "Embedded background upload finished after task_result: task=%s offer=%s",
                short_id(task_id),
                short_id(offer_id),
            )
        else:
            logger.warning(
                "Embedded background upload failed after task_result: task=%s offer=%s",
                short_id(task_id),
                short_id(offer_id),
            )
