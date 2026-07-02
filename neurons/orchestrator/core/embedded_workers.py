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
import time
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Any, Awaitable, Callable, List, Optional, Set, TypeVar

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
    http_client: Optional[httpx.AsyncClient] = None
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


T = TypeVar("T")


@dataclass
class _WorkHandle:
    """Awaitable work item scheduled on a pre-created coroutine pool worker."""

    future: asyncio.Future
    cancelled: bool = False
    _task: Optional[asyncio.Task] = None

    def cancel(self) -> None:
        self.cancelled = True
        if self._task is not None and not self._task.done():
            self._task.cancel()
        elif not self.future.done():
            self.future.cancel()


class _CoroutinePool:
    """Fixed pool of asyncio workers pulling callables from a queue."""

    def __init__(self, name: str, concurrency: int) -> None:
        self._name = name
        self._concurrency = max(1, concurrency)
        self._queue: asyncio.Queue = asyncio.Queue()
        self._workers: List[asyncio.Task] = []

    async def start(self) -> None:
        for worker_id in range(self._concurrency):
            self._workers.append(asyncio.create_task(self._worker_loop(worker_id)))

    async def stop(self) -> None:
        for _ in self._workers:
            await self._queue.put(None)
        for worker in self._workers:
            with contextlib.suppress(asyncio.CancelledError):
                await worker
        self._workers.clear()

    async def _worker_loop(self, worker_id: int) -> None:
        while True:
            item = await self._queue.get()
            try:
                if item is None:
                    break
                handle, runner = item
                if handle.cancelled:
                    if not handle.future.done():
                        handle.future.cancel()
                    continue
                handle._task = asyncio.current_task()
                if handle.cancelled:
                    if not handle.future.done():
                        handle.future.cancel()
                    continue
                try:
                    result = await runner()
                    if not handle.future.done():
                        handle.future.set_result(result)
                except asyncio.CancelledError:
                    if not handle.future.done():
                        handle.future.cancel()
                    raise
                except Exception as exc:
                    if not handle.future.done():
                        handle.future.set_exception(exc)
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("%s worker=%s failed", self._name, worker_id)
            finally:
                self._queue.task_done()

    def submit(self, coro_factory: Callable[[], Awaitable[T]]) -> _WorkHandle:
        loop = asyncio.get_running_loop()
        future: asyncio.Future = loop.create_future()
        handle = _WorkHandle(future=future)

        async def runner() -> T:
            return await coro_factory()

        self._queue.put_nowait((handle, runner))
        return handle

    def submit_fire_and_forget(self, coro_factory: Callable[[], Awaitable[Any]]) -> None:
        async def runner() -> None:
            try:
                await coro_factory()
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("%s fire-and-forget failed", self._name)

        loop = asyncio.get_running_loop()
        future: asyncio.Future = loop.create_future()
        handle = _WorkHandle(future=future)
        self._queue.put_nowait((handle, runner))


def _embedded_http_client() -> httpx.AsyncClient:
    max_tasks = max(1, int(os.environ.get("WORKER_MAX_CONCURRENT_TASKS", "4")))
    return httpx.AsyncClient(
        timeout=httpx.Timeout(120.0, connect=30.0),
        limits=httpx.Limits(
            max_connections=max(max_tasks * 4, 16),
            max_keepalive_connections=max(max_tasks * 2, 8),
        ),
    )


class EmbeddedWorkerPool:
    """Runs worker transfer logic inside the orchestrator process."""

    def __init__(self, settings: OrchestratorSettings, upstream: Any) -> None:
        self.settings = settings
        self.upstream = upstream
        self.workers: List[EmbeddedWorker] = []
        self.http_client: Optional[httpx.AsyncClient] = None
        self._cursor = 0
        self._offer_pool: Optional[_CoroutinePool] = None
        self._side_pool: Optional[_CoroutinePool] = None
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
            worker_client = _embedded_http_client()
            state = transfer.WorkerState(
                wallet=wallet,
                api_url=self.settings.core_server_url,
                http_client=worker_client,
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
                http_client=worker_client,
            )
            self.workers.append(worker)
            logger.info(
                "Embedded worker ready slot=%s worker_id=%s hotkey=%s",
                cfg.slot,
                short_id(worker_id),
                hotkey[:16],
            )

        logger.info(
            "Embedded predefined ETag config: early_submit=%s max_parallel=%s "
            "source_prefix=%r file_size=%s",
            transfer.WORKER_PREDEFINED_ETAG_EARLY_SUBMIT,
            transfer.PREDEFINED_ETAG_MAX_PARALLEL,
            transfer.normalized_capability_url(transfer.PREDEFINED_ETAG_SOURCE_URL),
            transfer.PREDEFINED_ETAG_SOURCE_FILE_SIZE,
        )

    def _log_offer_task_done(
        self,
        task: asyncio.Task,
        *,
        task_id: str,
        offer_id: str,
    ) -> None:
        if task.cancelled():
            return
        exc = task.exception()
        if exc is not None:
            logger.error(
                "Embedded offer task failed: task=%s offer=%s error=%s",
                short_id(task_id),
                short_id(offer_id),
                exc,
                exc_info=exc,
            )

    async def stop(self) -> None:
        for task in list(self._task_handles):
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await task
        self._task_handles.clear()
        for worker in self.workers:
            if worker.http_client is not None:
                await worker.http_client.aclose()
                worker.http_client = None
        self.workers.clear()
        if self.http_client is not None:
            await self.http_client.aclose()
            self.http_client = None

    def _select_worker(
        self,
        batch_used_ips: Optional[set[str]] = None,
        batch_assigned_counts: Optional[dict[str, int]] = None,
    ) -> Optional[EmbeddedWorker]:
        if not self.workers:
            return None

        pool_size = len(self.workers)
        start = self._cursor % pool_size
        in_batch = batch_used_ips is not None or batch_assigned_counts is not None

        def _eligible(
            worker: EmbeddedWorker,
            *,
            allow_used_ip: bool,
            allow_reuse_worker: bool,
        ) -> bool:
            if not worker.has_capacity:
                return False
            if batch_assigned_counts is not None:
                assigned = batch_assigned_counts.get(worker.worker_id, 0)
                slots_left = worker.max_concurrent_tasks - worker.active_count - assigned
                if slots_left <= 0:
                    return False
                if not allow_reuse_worker and assigned > 0:
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

        def _pick(
            *,
            allow_used_ip: bool,
            allow_reuse_worker: bool,
        ) -> Optional[EmbeddedWorker]:
            for offset in range(pool_size):
                idx = (start + offset) % pool_size
                worker = self.workers[idx]
                if not _eligible(
                    worker,
                    allow_used_ip=allow_used_ip,
                    allow_reuse_worker=allow_reuse_worker,
                ):
                    continue
                self._cursor = (idx + 1) % pool_size
                return worker
            return None

        if in_batch:
            worker = _pick(allow_used_ip=False, allow_reuse_worker=False)
            if worker:
                return worker
            worker = _pick(allow_used_ip=True, allow_reuse_worker=False)
            if worker:
                return worker
            return _pick(allow_used_ip=True, allow_reuse_worker=True)
        return _pick(allow_used_ip=True, allow_reuse_worker=True)

    @staticmethod
    def _log_task_failed(
        transfer: Any,
        transfer_context: dict,
        *,
        task_id: str,
        offer_id: str,
        reason: str,
        chunk_hash: str = "",
    ) -> None:
        transfer.log_task_chunk_from_context(
            "failed",
            transfer_context,
            task_id=str(task_id),
            offer_id=str(offer_id),
            chunk_hash=chunk_hash,
            log_prefix="[Embedded]",
            detail=f"reason={reason}",
        )

    async def deliver_task_offer_batch(self, batch_id: str, offers: list[dict]) -> tuple[int, int]:
        delivered = 0
        failed = 0
        batch_used_ips: set[str] = set()
        batch_assigned_counts: dict[str, int] = defaultdict(int)
        transfer = get_transfer_module()

        for offer in offers:
            if not isinstance(offer, dict):
                failed += 1
                continue

            task_id = str(offer.get("task_id") or offer.get("offer_id") or "")
            offer_id = str(offer.get("offer_id") or task_id or "")
            transfer_context, validation_error = transfer.build_transfer_context(offer)
            if validation_error or transfer_context is None:
                logger.warning(
                    "Embedded batch offer invalid: batch=%s task=%s offer=%s error=%s",
                    short_id(batch_id, 12),
                    short_id(task_id),
                    short_id(offer_id),
                    validation_error or "unknown",
                )
                failed += 1
                continue
            elif transfer.uses_predefined_etag_early_submit(transfer_context):
                logger.info(
                    "Embedded batch offer fast-path: batch=%s task=%s offer=%s chunk_size=%s",
                    short_id(batch_id, 12),
                    short_id(task_id),
                    short_id(offer_id),
                    transfer_context.get("chunk_size"),
                )
            else:
                skip_reasons = transfer.predefined_etag_early_submit_skip_reasons(
                    transfer_context
                )
                logger.info(
                    "Embedded batch offer standard-path: batch=%s task=%s offer=%s reasons=%s",
                    short_id(batch_id, 12),
                    short_id(task_id),
                    short_id(offer_id),
                    "; ".join(skip_reasons) if skip_reasons else "early_submit_disabled",
                )

            worker = self._select_worker(batch_used_ips, batch_assigned_counts)
            if worker is None:
                reason = "no_embedded_worker_capacity"
                logger.warning(
                    "No embedded worker capacity for batch=%s task=%s offer=%s",
                    short_id(batch_id, 12),
                    short_id(task_id),
                    short_id(offer_id),
                )
                self._log_task_failed(
                    transfer,
                    transfer_context,
                    task_id=task_id,
                    offer_id=offer_id,
                    reason=reason,
                )
                failed += 1
                continue

            offer_id = str(offer.get("offer_id") or offer.get("task_id") or "")
            batch_assigned_counts[worker.worker_id] += 1
            if worker.ip:
                batch_used_ips.add(worker.ip)

            logger.info(
                "Embedded offer assigned: batch=%s task=%s offer=%s worker_slot=%s worker_id=%s",
                short_id(batch_id, 12),
                short_id(task_id),
                short_id(offer_id),
                worker.slot,
                short_id(worker.worker_id),
            )
            task = asyncio.create_task(
                self._handle_offer(worker, offer, transfer_context)
            )
            self._task_handles.add(task)
            task.add_done_callback(
                lambda t, tid=task_id, oid=offer_id: self._log_offer_task_done(
                    t, task_id=tid, offer_id=oid
                )
            )
            task.add_done_callback(self._task_handles.discard)
            delivered += 1

        await asyncio.sleep(0)

        logger.info(
            "Embedded batch queued: batch=%s offers=%s delivered=%s failed=%s",
            short_id(batch_id, 12),
            len(offers),
            delivered,
            failed,
        )
        return delivered, failed

    async def _handle_offer(
        self,
        worker: EmbeddedWorker,
        offer: dict,
        transfer_context: dict,
    ) -> None:
        transfer = get_transfer_module()
        task_id = offer.get("task_id") or offer.get("offer_id")
        offer_id = offer.get("offer_id") or task_id
        worker.active_offer_ids.add(str(offer_id or ""))
        logger.info(
            "Embedded offer handler started: task=%s offer=%s worker_slot=%s",
            short_id(task_id),
            short_id(offer_id),
            worker.slot,
        )

        try:
            deadline_us = int(offer.get("deadline_us") or 0)
        except (TypeError, ValueError):
            deadline_us = 0
        estimated_bytes = transfer.estimate_task_bytes(offer)

        try:
            capacity_error = self._reserve_capacity(worker, estimated_bytes)
            if capacity_error:
                logger.warning(
                    "Embedded rejecting offer: task=%s offer=%s worker_slot=%s reason=%s",
                    short_id(task_id),
                    short_id(offer_id),
                    worker.slot,
                    capacity_error,
                )
                self._log_task_failed(
                    transfer,
                    transfer_context,
                    task_id=str(task_id),
                    offer_id=str(offer_id),
                    reason=capacity_error,
                )
                await self._send_reject(worker, task_id, offer_id, capacity_error)
                return

            skip_reasons = transfer.predefined_etag_early_submit_skip_reasons(transfer_context)
            if skip_reasons:
                logger.info(
                    "Embedded fast path skipped: task=%s offer=%s worker_slot=%s reasons=%s",
                    short_id(task_id),
                    short_id(offer_id),
                    worker.slot,
                    "; ".join(skip_reasons),
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
                logger.info(
                    "Embedded standard transfer: task=%s offer=%s worker_slot=%s",
                    short_id(task_id),
                    short_id(offer_id),
                    worker.slot,
                )
                await self._handle_standard_offer(
                    worker,
                    offer,
                    task_id,
                    offer_id,
                    transfer_context,
                    deadline_us,
                )
        except Exception:
            logger.exception(
                "Embedded offer handler error: task=%s offer=%s worker_slot=%s",
                short_id(task_id),
                short_id(offer_id),
                worker.slot,
            )
            raise
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
        else:
            logger.warning(
                "Embedded task result not completed: task=%s offer=%s worker=%s "
                "received=%s completed=%s reason=%s",
                short_id(task_id),
                short_id(offer_id),
                short_id(worker.worker_id),
                ack.get("received"),
                ack.get("completed"),
                ack.get("reason") or ack.get("error") or ack.get("message"),
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

        if not transfer.uses_predefined_etag_early_submit(transfer_context):
            logger.warning(
                "Embedded predefined handler fast path unavailable; using standard transfer: "
                "task=%s offer=%s reasons=%s",
                short_id(task_id),
                short_id(offer_id),
                "; ".join(
                    transfer.predefined_etag_early_submit_skip_reasons(transfer_context)
                )
                or "early_submit_disabled",
            )
            await self._handle_standard_offer(
                worker,
                offer,
                task_id,
                offer_id,
                transfer_context,
                deadline_us,
            )
            return

        cached = transfer.get_predefined_etag_cache(transfer_context)
        env_configured = bool(transfer.PREDEFINED_ETAG_ENV_CHUNK_HASH)
        logger.info(
            "Embedded predefined ETag: %s task=%s offer=%s",
            (
                "env hash/etag, accept then submit"
                if env_configured
                else "cache hit, accept then submit"
                if cached
                else "cache miss, fetch+upload then submit"
            ),
            short_id(task_id),
            short_id(offer_id),
        )

        async def _accept_task() -> bool:
            resp = await self._send_accept(worker, task_id, offer_id)
            return bool(resp.get("accepted"))

        outcome = await transfer.predefined_etag_submit_flow(
            worker.state,
            str(task_id),
            str(offer_id),
            offer,
            transfer_context,
            deadline_us,
            log_prefix="[Embedded]",
            accept_timeout=float(
                os.environ.get("WORKER_TASK_ACCEPT_ACK_TIMEOUT", "8.0")
            ),
            accept_task=_accept_task,
        )

        if not outcome.success:
            logger.warning(
                "Embedded predefined submit failed: task=%s offer=%s worker=%s reason=%s",
                short_id(task_id),
                short_id(offer_id),
                short_id(worker.worker_id),
                outcome.error,
            )
            self._log_task_failed(
                transfer,
                transfer_context,
                task_id=str(task_id),
                offer_id=str(offer_id),
                reason=str(outcome.error or "predefined_submit_failed"),
                chunk_hash=outcome.chunk_hash,
            )
            if outcome.error:
                await self._send_result(
                    worker,
                    task_id,
                    offer_id,
                    success=False,
                    chunk_hash=outcome.chunk_hash,
                    error=outcome.error,
                )
            return

        await self._send_result(
            worker,
            task_id,
            offer_id,
            success=True,
            chunk_hash=outcome.chunk_hash,
            etag=outcome.etag,
        )

        if outcome.used_cache:
            asyncio.create_task(
                self._await_background_transfer_task(
                    worker,
                    offer,
                    task_id,
                    offer_id,
                    transfer_context,
                    deadline_us,
                )
            )

    async def _await_background_transfer_task(
        self,
        worker: EmbeddedWorker,
        offer: dict,
        task_id: str,
        offer_id: str,
        transfer_context: dict,
        deadline_us: int,
    ) -> None:
        transfer = get_transfer_module()
        try:
            result = await transfer.run_predefined_etag_background_transfer(
                worker.state,
                str(task_id),
                str(offer_id),
                offer,
                transfer_context,
                deadline_us,
                log_prefix="[Embedded]",
            )
        except asyncio.CancelledError:
            logger.warning(
                "Embedded background transfer cancelled: task=%s offer=%s",
                short_id(task_id),
                short_id(offer_id),
            )
            return
        except Exception as exc:
            logger.warning(
                "Embedded background transfer error: task=%s offer=%s err=%s",
                short_id(task_id),
                short_id(offer_id),
                exc,
            )
            self._log_task_failed(
                transfer,
                transfer_context,
                task_id=str(task_id),
                offer_id=str(offer_id),
                reason=str(exc),
            )
            return

        if result.success:
            logger.info(
                "Embedded background transfer finished after task_result: task=%s offer=%s",
                short_id(task_id),
                short_id(offer_id),
            )
        else:
            logger.warning(
                "Embedded background transfer failed after task_result: task=%s offer=%s",
                short_id(task_id),
                short_id(offer_id),
            )
            self._log_task_failed(
                transfer,
                transfer_context,
                task_id=str(task_id),
                offer_id=str(offer_id),
                reason=result.error_msg or "background_transfer_failed",
                chunk_hash=result.chunk_hash,
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
            self._log_task_failed(
                transfer,
                transfer_context,
                task_id=str(task_id),
                offer_id=str(offer_id),
                reason=str(reason),
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
        if result.success:
            transfer.maybe_store_predefined_etag_cache_on_success(
                transfer_context,
                result.chunk_hash,
                result.etag,
                log_prefix="[Embedded]",
                task_id=str(task_id),
                offer_id=str(offer_id),
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
