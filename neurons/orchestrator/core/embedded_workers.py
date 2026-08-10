"""
In-process embedded workers for WORKER_GATEWAY_MODE=embedded|in_process|embedded_global.

Workers register with BeamCore over HTTP at orchestrator startup. Task batches
from BeamCore are executed locally. In in_process mode, a WorkerGateway may be
attached for overflow: prefer a fresh-IP external worker before reusing an
embedded worker IP within the same batch.

embedded_global: after control-server sync_done, all work goes to simple/hidden
workers on orchestrator WS; BeamCore task_result always uses WORKER_S (one hotkey).
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
from core.cloudflare_transfer import (
    call_cloudflare_transfer_worker,
    parse_cf_transfer_urls,
)
from core.relay_log import (
    chunk_id_from_transfer_context,
    format_ts_utc,
    queue_wait_ms,
    short_id,
    transfer_context_range_label,
    transfer_context_urls,
)
from core.transfer_loader import get_transfer_module

logger = logging.getLogger(__name__)

_WORKERS_LOG = "_workers |"


def _stamp_ctx_queued(transfer_context: dict) -> None:
    if "_queued_at" not in transfer_context:
        transfer_context["_queued_at"] = time.time()


def _stamp_ctx_started(transfer_context: dict, worker_id: str = "") -> None:
    now = time.time()
    if "_queued_at" not in transfer_context:
        transfer_context["_queued_at"] = now
    transfer_context["_started_at"] = now
    if worker_id:
        transfer_context["_worker_id"] = str(worker_id)


def _log_embedded_task_offer(
    *,
    task_id: str,
    offer_id: str,
    worker_slot: int,
    transfer_context: dict,
    path: str,
    beamcore_worker_id: str = "",
    hidden_worker_id: str = "",
) -> None:
    # Short start log only; hash/etag land on task_done.
    chunk_id = chunk_id_from_transfer_context(transfer_context)
    fields = [
        f"{_WORKERS_LOG} task_start task={short_id(task_id)}",
        f"offer={short_id(offer_id)}",
        f"worker_slot={worker_slot}",
        f"chunk_id={chunk_id if chunk_id is not None else '?'}",
        f"range={transfer_context_range_label(transfer_context)}",
        f"path={path}",
    ]
    if beamcore_worker_id:
        fields.append(f"beamcore_worker={short_id(beamcore_worker_id)}")
    if hidden_worker_id:
        fields.append(f"hidden_worker={short_id(hidden_worker_id)}")
    logger.info(" ".join(fields))


def _log_embedded_task_done(
    *,
    task_id: str,
    offer_id: str,
    transfer_context: dict,
    chunk_hash: str = "",
    etag: str = "",
    etag_local: str = "",
    cached: bool = False,
    path: str = "",
    hash_source: str = "",
    load_ms: float = 0.0,
    hash_ms: float = 0.0,
    etag_ms: float = 0.0,
    fetch_ms: float = 0.0,
    send_ms: float = 0.0,
    worker_id: str = "",
) -> None:
    chunk_id = chunk_id_from_transfer_context(transfer_context)
    src, dest = transfer_context_urls(transfer_context)
    total_ms = load_ms + hash_ms + etag_ms + fetch_ms + send_ms
    completed_at = time.time()
    started_at = transfer_context.get("_started_at")
    queued_at = transfer_context.get("_queued_at")
    wait_ms = queue_wait_ms(queued_at, started_at)
    try:
        exec_ms = (
            (completed_at - float(started_at)) * 1000.0 if started_at else 0.0
        )
    except (TypeError, ValueError):
        exec_ms = 0.0
    result_worker = str(
        worker_id or transfer_context.get("_worker_id") or ""
    )
    logger.info(
        "%s task_done task=%s offer=%s chunk_id=%s worker_id=%s "
        "started_at=%s completed_at=%s queue_wait_ms=%.0f exec_ms=%.1f "
        "src=%s dest=%s range=%s hash=%s etag_real=%s etag_local=%s "
        "cached=%s path=%s hash_source=%s "
        "load_ms=%.1f hash_ms=%.1f etag_ms=%.1f fetch_ms=%.1f send_ms=%.1f wall_ms=%.1f",
        _WORKERS_LOG,
        short_id(task_id),
        short_id(offer_id),
        chunk_id if chunk_id is not None else "?",
        short_id(result_worker) if result_worker else "-",
        format_ts_utc(started_at),
        format_ts_utc(completed_at),
        wait_ms,
        exec_ms,
        src,
        dest,
        transfer_context_range_label(transfer_context),
        chunk_hash or "-",
        etag or "-",
        etag_local or "-",
        str(cached).lower(),
        path or ("cache" if cached else "miss"),
        hash_source or "-",
        load_ms,
        hash_ms,
        etag_ms,
        fetch_ms,
        send_ms,
        total_ms,
    )


def _log_embedded_task_completed(
    *,
    task_id: str,
    offer_id: str,
    worker_id: str,
    latency_ms: float,
) -> None:
    logger.debug(
        "%s task_completed task=%s offer=%s worker=%s latency_ms=%.1f",
        _WORKERS_LOG,
        short_id(task_id),
        short_id(offer_id),
        short_id(worker_id),
        latency_ms,
    )


def _log_embedded_task_failed(
    *,
    task_id: str,
    offer_id: str,
    transfer_context: dict,
    reason: str,
    chunk_hash: str = "",
    etag: str = "",
    cached: Optional[bool] = None,
) -> None:
    src, dest = transfer_context_urls(transfer_context)
    cached_label = "?" if cached is None else str(cached).lower()
    logger.warning(
        "%s failed task=%s offer=%s reason=%s src=%s dest=%s "
        "range=%s hash=%s etag=%s cached=%s",
        _WORKERS_LOG,
        short_id(task_id),
        short_id(offer_id),
        reason,
        src,
        dest,
        transfer_context_range_label(transfer_context),
        chunk_hash or "-",
        etag or "-",
        cached_label,
    )


def _env_bool(raw: str, default: bool = False) -> bool:
    value = (raw or "").strip().lower()
    if not value:
        return default
    if value in ("1", "true", "yes", "on"):
        return True
    if value in ("0", "false", "no", "off"):
        return False
    return default


@dataclass
class EmbeddedWorkerConfig:
    slot: int
    wallet_name: str
    hotkey: str
    initial_order: int = 0
    max_concurrent_tasks: int = 4
    cf_transfer_enabled: bool = False
    cf_transfer_worker_urls: List[str] = field(default_factory=list)


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
    cf_transfer_enabled: bool = False
    cf_transfer_worker_urls: List[str] = field(default_factory=list)
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


def _parse_worker_env_slot(
    settings: OrchestratorSettings,
    *,
    prefix: str,
    slot: int,
    default_order: int = 0,
) -> Optional[EmbeddedWorkerConfig]:
    """Parse WORKER_S / WORKER_1 / … from env (``wallet:hotkey`` or ``*_HOTKEY``)."""
    default_wallet = os.environ.get("WORKER_WALLET_NAME", settings.wallet_name).strip()
    default_max_tasks = max(1, int(os.environ.get("WORKER_MAX_CONCURRENT_TASKS", "4")))

    combined = os.environ.get(prefix, "").strip()
    hotkey = os.environ.get(f"{prefix}_HOTKEY", "").strip()
    wallet_name = os.environ.get(f"{prefix}_WALLET_NAME", "").strip()

    if combined:
        if ":" in combined:
            wallet_part, hotkey_part = combined.split(":", 1)
            wallet_name = wallet_name or wallet_part.strip()
            hotkey = hotkey or hotkey_part.strip()
        elif not hotkey:
            hotkey = combined

    if not hotkey:
        return None
    if not wallet_name:
        wallet_name = default_wallet

    order_raw = os.environ.get(f"{prefix}_ORDER", str(default_order)).strip()
    try:
        initial_order = int(order_raw)
    except ValueError:
        initial_order = default_order

    max_tasks_raw = os.environ.get(f"{prefix}_MAX_CONCURRENT_TASKS", "").strip()
    if max_tasks_raw:
        try:
            max_concurrent = max(1, int(max_tasks_raw))
        except ValueError:
            max_concurrent = default_max_tasks
    else:
        max_concurrent = default_max_tasks

    cf_raw = os.environ.get(f"{prefix}_CF_TRANSFER_ENABLED", "").strip()
    if cf_raw:
        cf_transfer_enabled = _env_bool(cf_raw, False)
    else:
        cf_transfer_enabled = bool(getattr(settings, "cf_transfer_enabled", False))

    cf_transfer_worker_urls = parse_cf_transfer_urls(
        os.environ.get(f"{prefix}_CF_TRANSFER_WORKER_URLS", ""),
        os.environ.get(f"{prefix}_CF_TRANSFER_WORKER_URL", ""),
    )

    return EmbeddedWorkerConfig(
        slot=slot,
        wallet_name=wallet_name,
        hotkey=hotkey,
        initial_order=initial_order,
        max_concurrent_tasks=max_concurrent,
        cf_transfer_enabled=cf_transfer_enabled,
        cf_transfer_worker_urls=cf_transfer_worker_urls,
    )


def parse_s_worker_config(settings: OrchestratorSettings) -> Optional[EmbeddedWorkerConfig]:
    """BeamCore identity for simple-worker mode: WORKER_S (wallet:hotkey)."""
    return _parse_worker_env_slot(settings, prefix="WORKER_S", slot=1, default_order=0)


def parse_embedded_worker_configs(settings: OrchestratorSettings) -> List[EmbeddedWorkerConfig]:
    """Parse embedded worker slots from the orchestrator env file.

    - ``embedded_global``: prefer ``WORKER_S`` (register + task_result hotkey).
      Falls back to ``WORKER_1`` if ``WORKER_S`` is unset.
    - other modes: ``WORKER_1``, ``WORKER_2``, …
    """
    mode = (getattr(settings, "worker_gateway_mode", None) or "").strip().lower()
    if mode == "embedded_global":
        s_worker = parse_s_worker_config(settings)
        if s_worker is not None:
            return [s_worker]
        fallback = _parse_worker_env_slot(
            settings, prefix="WORKER_1", slot=1, default_order=0
        )
        if fallback is not None:
            logger.warning(
                "embedded_global: WORKER_S unset — falling back to WORKER_1 for "
                "BeamCore register/task_result (set WORKER_S=wallet:hotkey)"
            )
            return [fallback]
        return []

    configs: List[EmbeddedWorkerConfig] = []
    idx = 1
    while True:
        cfg = _parse_worker_env_slot(
            settings,
            prefix=f"WORKER_{idx}",
            slot=idx,
            default_order=idx - 1,
        )
        if cfg is None:
            break
        configs.append(cfg)
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
        self._worker_gateway: Optional[Any] = None
        self._hidden_worker_gateway: Optional[Any] = None
        self._transfer_waiters: dict[str, asyncio.Future] = {}
        self._hybrid_routing_enabled = False
        # Cross-batch IP spread for in_process hybrid (BeamCore often sends offers=1).
        self._hybrid_used_ips: set[str] = set()
        self._cf_urls: List[str] = []
        self._cf_rr_index = 0
        # Offers waiting for an idle embedded (or external) worker.
        self._overflow: list[dict[str, Any]] = []
        self._overflow_ids: Set[str] = set()
        self._overflow_drain_pending = False
        self._overflow_drain_running = False

    @property
    def worker_count(self) -> int:
        return len(self.workers)

    @property
    def hybrid_mode(self) -> bool:
        return (self.settings.worker_gateway_mode or "").strip().lower() == "embedded_global"

    @property
    def uses_overflow_routing(self) -> bool:
        """Hybrid overflow to hidden workers (enabled after control-server sync_done)."""
        return self.hybrid_mode and self._hybrid_routing_enabled

    @property
    def s_worker(self) -> Optional[EmbeddedWorker]:
        """Single BeamCore identity (WORKER_S) for every task_result."""
        return self.workers[0] if self.workers else None

    def mark_cache_sync_done(self) -> None:
        if not self.hybrid_mode or self._hybrid_routing_enabled:
            return
        self._hybrid_routing_enabled = True
        # Local embedded stops taking work; simple-workers do transfers.
        if self.workers:
            self.workers[0].max_concurrent_tasks = 0
        hidden_n = 0
        gateway = self._hidden_worker_gateway
        if gateway is not None and hasattr(gateway, "connected_hidden_worker_ids"):
            try:
                hidden_n = len(gateway.connected_hidden_worker_ids())
            except Exception:
                hidden_n = 0
        logger.info(
            "embedded_global: cache sync done — all tasks via simple-workers; "
            "task_result uses WORKER_S only; hidden_connected=%d",
            hidden_n,
        )
        if hidden_n <= 0:
            logger.warning(
                "embedded_global: no hidden/simple workers connected yet — "
                "offers will queue until a worker connects with ?hidden=1"
            )
        self._schedule_overflow_drain()

    def _routing_workers(self) -> List[EmbeddedWorker]:
        # After sync_done, no local embedded execution — only s-worker identity.
        if self.uses_overflow_routing:
            return []
        return self.workers

    def _beamcore_worker_for_overflow(self, embedded: Optional[EmbeddedWorker] = None) -> EmbeddedWorker:
        worker = self.s_worker or embedded
        if worker is None:
            raise RuntimeError("WORKER_S required for BeamCore task_result identity")
        return worker

    def set_hidden_worker_gateway(self, gateway: Any) -> None:
        """Route overflow transfer work to hidden workers connected on orchestrator WS."""
        self._hidden_worker_gateway = gateway
        if hasattr(gateway, "set_transfer_result_handler"):
            gateway.set_transfer_result_handler(self.handle_transfer_result)
        if hasattr(gateway, "set_hidden_capacity_handler"):
            gateway.set_hidden_capacity_handler(self._schedule_overflow_drain)

    async def handle_transfer_result(self, worker_id: str, message: dict) -> None:
        offer_id = str(message.get("offer_id") or message.get("task_id") or "")
        future = self._transfer_waiters.pop(offer_id, None)
        if future is not None and not future.done():
            future.set_result(message)
        else:
            logger.warning(
                "Unexpected transfer_result from hidden worker=%s offer=%s",
                short_id(worker_id),
                short_id(offer_id),
            )
        # Free slot may unblock offers waiting for hidden capacity.
        self._schedule_overflow_drain()

    def set_worker_gateway(self, gateway: Any) -> None:
        """Overflow task offers to external workers connected on orchestrator WS."""
        self._worker_gateway = gateway
        if gateway is None:
            self._hybrid_used_ips.clear()

    def _cf_url_pool(self, worker: Optional["EmbeddedWorker"] = None) -> List[str]:
        """CF Worker URLs for this slot (per-slot override, else global pool)."""
        if worker is not None and not bool(getattr(worker, "cf_transfer_enabled", False)):
            return []
        if worker is None and not any(
            bool(getattr(w, "cf_transfer_enabled", False)) for w in self.workers
        ):
            return []
        slot_urls = (
            list(getattr(worker, "cf_transfer_worker_urls", None) or [])
            if worker is not None
            else []
        )
        if slot_urls:
            return slot_urls
        return list(self._cf_urls)

    def _cf_transfer_url(self, worker: Optional["EmbeddedWorker"] = None) -> str:
        """Compatibility: first URL when CF is active for this worker."""
        urls = self._cf_url_pool(worker)
        return urls[0] if urls else ""

    def _cf_transfer_active(self, worker: Optional["EmbeddedWorker"] = None) -> bool:
        return bool(self._cf_url_pool(worker))

    def _pick_cf_transfer_url(self, worker: Optional["EmbeddedWorker"] = None) -> str:
        """Round-robin pick from the worker's CF URL pool (sync — no await gap)."""
        urls = self._cf_url_pool(worker)
        if not urls:
            return ""
        if len(urls) == 1:
            return urls[0]
        idx = self._cf_rr_index % len(urls)
        self._cf_rr_index += 1
        return urls[idx]

    def _spread_used_ips(self, batch_used_ips: set[str]) -> set[str]:
        """IPs already used in this batch plus recent cross-batch hybrid assignments."""
        if self._worker_gateway is None:
            return batch_used_ips
        return batch_used_ips | self._hybrid_used_ips

    def _mark_spread_ip(self, ip: str) -> None:
        clean = ip.strip()
        if clean and self._worker_gateway is not None:
            self._hybrid_used_ips.add(clean)

    def _clear_hybrid_spread_ips(self) -> None:
        self._hybrid_used_ips.clear()

    async def start(self) -> None:
        transfer = get_transfer_module()
        configs = parse_embedded_worker_configs(self.settings)
        if not configs:
            mode = (self.settings.worker_gateway_mode or "embedded").strip().lower()
            if mode == "embedded_global":
                raise ValueError(
                    "WORKER_GATEWAY_MODE=embedded_global requires WORKER_S "
                    "(or WORKER_S_HOTKEY); WORKER_1 is accepted as fallback"
                )
            raise ValueError(
                f"WORKER_GATEWAY_MODE={mode} requires WORKER_1 (or WORKER_1_HOTKEY) in env"
            )

        if self.hybrid_mode:
            s = configs[0]
            logger.info(
                "embedded_global: WORKER_S BeamCore hotkey=%s wallet=%s; "
                "embedded transfer until sync_done, then all work via simple-workers",
                s.hotkey,
                s.wallet_name,
            )

        self.http_client = httpx.AsyncClient(
            timeout=httpx.Timeout(120.0, connect=30.0),
            limits=httpx.Limits(max_connections=64, max_keepalive_connections=32),
        )

        self._cf_urls = list(self.settings.get_cf_transfer_worker_urls())
        cf_slots = [cfg.slot for cfg in configs if cfg.cf_transfer_enabled]
        slot_overrides = {
            cfg.slot: cfg.cf_transfer_worker_urls
            for cfg in configs
            if cfg.cf_transfer_enabled and cfg.cf_transfer_worker_urls
        }
        if cf_slots:
            if self._cf_urls or slot_overrides:
                logger.info(
                    "Embedded CF transfer ENABLED slots=%s urls=%s "
                    "slot_url_overrides=%s timeout_s=%.1f send_accept=%s",
                    cf_slots,
                    self._cf_urls,
                    {k: v for k, v in slot_overrides.items()},
                    float(self.settings.cf_transfer_worker_timeout or 120.0),
                    "false",
                )
            else:
                logger.warning(
                    "CF transfer enabled for slots=%s but no CF_TRANSFER_WORKER_URL(S); "
                    "those slots will use normal local transfer",
                    cf_slots,
                )
        elif self._cf_urls:
            logger.info(
                "CF_TRANSFER_WORKER_URL(S) set (%s urls) but no "
                "WORKER_N_CF_TRANSFER_ENABLED (and CF_TRANSFER_ENABLED=false); "
                "using normal local transfer",
                len(self._cf_urls),
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
                cf_transfer_enabled=cfg.cf_transfer_enabled,
                cf_transfer_worker_urls=list(cfg.cf_transfer_worker_urls),
                http_client=worker_client,
            )
            self.workers.append(worker)
            logger.info(
                "Embedded worker ready slot=%s worker_id=%s hotkey=%s "
                "cf_transfer=%s cf_urls=%s",
                cfg.slot,
                short_id(worker_id),
                hotkey[:16],
                str(bool(cfg.cf_transfer_enabled)).lower(),
                cfg.cf_transfer_worker_urls
                if cfg.cf_transfer_worker_urls
                else (self._cf_urls if cfg.cf_transfer_enabled else []),
            )

        logger.info(
            "Embedded predefined ETag config: early_submit=%s max_parallel=%s "
            "env_source=%r env_file_size=%s cache=source_url+byte_range",
            transfer.WORKER_PREDEFINED_ETAG_EARLY_SUBMIT,
            transfer.PREDEFINED_ETAG_MAX_PARALLEL,
            transfer.normalized_capability_url(transfer.PREDEFINED_ETAG_SOURCE_URL)
            or None,
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
        workers_pool = self._routing_workers()
        if not workers_pool:
            return None

        from core.worker_gateway import ALLOW_BUSY_WORKER_REUSE, PREFER_IDLE_WORKERS

        pool_size = len(workers_pool)
        start = self._cursor % pool_size
        in_batch = batch_used_ips is not None or batch_assigned_counts is not None
        counts = batch_assigned_counts
        prefer_idle = PREFER_IDLE_WORKERS
        allow_busy_reuse = ALLOW_BUSY_WORKER_REUSE

        def _batch_count(worker_id: str) -> int:
            if counts is None:
                return 0
            return int(counts.get(worker_id, 0))

        def _load(worker: EmbeddedWorker) -> int:
            return max(worker.active_count, _batch_count(worker.worker_id))

        def _eligible(
            worker: EmbeddedWorker,
            *,
            allow_used_ip: bool,
            require_idle: bool,
            allow_reuse_worker: bool,
        ) -> bool:
            if not worker.has_capacity:
                return False
            load = _load(worker)
            if require_idle and load > 0:
                return False
            assigned = _batch_count(worker.worker_id)
            if counts is not None:
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
            require_idle: bool,
            allow_reuse_worker: bool,
        ) -> Optional[EmbeddedWorker]:
            # Spread load first (makespan), then RR offset.
            candidates: list[tuple[int, int, int, int, EmbeddedWorker]] = []
            for offset in range(pool_size):
                idx = (start + offset) % pool_size
                worker = workers_pool[idx]
                if not _eligible(
                    worker,
                    allow_used_ip=allow_used_ip,
                    require_idle=require_idle,
                    allow_reuse_worker=allow_reuse_worker,
                ):
                    continue
                candidates.append(
                    (
                        _load(worker),
                        _batch_count(worker.worker_id),
                        -worker.initial_order,
                        offset,
                        worker,
                    )
                )
            if not candidates:
                return None
            candidates.sort(key=lambda item: (item[0], item[1], item[2], item[3]))
            _load_n, _bc, _neg_order, offset, worker = candidates[0]
            self._cursor = (start + offset + 1) % pool_size
            return worker

        if prefer_idle:
            worker = _pick(
                allow_used_ip=False,
                require_idle=True,
                allow_reuse_worker=False,
            )
            if worker:
                return worker
            worker = _pick(
                allow_used_ip=True,
                require_idle=True,
                allow_reuse_worker=False,
            )
            if worker:
                return worker
            if not allow_busy_reuse:
                return None
            worker = _pick(
                allow_used_ip=False,
                require_idle=False,
                allow_reuse_worker=True,
            )
            if worker:
                return worker
            return _pick(
                allow_used_ip=True,
                require_idle=False,
                allow_reuse_worker=True,
            )

        if in_batch:
            worker = _pick(
                allow_used_ip=False,
                require_idle=False,
                allow_reuse_worker=False,
            )
            if worker:
                return worker
            worker = _pick(
                allow_used_ip=True,
                require_idle=False,
                allow_reuse_worker=False,
            )
            if worker:
                return worker
            return _pick(
                allow_used_ip=True,
                require_idle=False,
                allow_reuse_worker=True,
            )
        return _pick(
            allow_used_ip=True,
            require_idle=False,
            allow_reuse_worker=True,
        )

    @staticmethod
    def _log_task_failed(
        transfer: Any,
        transfer_context: dict,
        *,
        task_id: str,
        offer_id: str,
        reason: str,
        chunk_hash: str = "",
        etag: str = "",
        cached: Optional[bool] = None,
    ) -> None:
        _log_embedded_task_failed(
            task_id=str(task_id),
            offer_id=str(offer_id),
            transfer_context=transfer_context,
            reason=reason,
            chunk_hash=chunk_hash,
            etag=etag,
            cached=cached,
        )
        transfer.log_task_chunk_from_context(
            "failed",
            transfer_context,
            task_id=str(task_id),
            offer_id=str(offer_id),
            chunk_hash=chunk_hash,
            log_prefix="[Embedded]",
            detail=f"reason={reason}",
        )

    def _select_embedded_unused_ip(
        self,
        batch_used_ips: set[str],
        batch_assigned_counts: dict[str, int],
    ) -> Optional[EmbeddedWorker]:
        """Pick embedded worker with capacity on an IP not yet used in this batch."""
        workers_pool = self.workers
        if not workers_pool:
            return None
        pool_size = len(workers_pool)
        start = self._cursor % pool_size
        for offset in range(pool_size):
            idx = (start + offset) % pool_size
            worker = workers_pool[idx]
            if not worker.has_capacity:
                continue
            assigned = batch_assigned_counts.get(worker.worker_id, 0)
            slots_left = worker.max_concurrent_tasks - worker.active_count - assigned
            if slots_left <= 0 or assigned > 0:
                continue
            ip = worker.ip.strip()
            if ip and ip in batch_used_ips:
                continue
            self._cursor = (idx + 1) % pool_size
            return worker
        return None

    async def _dispatch_external(
        self,
        *,
        offer: dict,
        task_id: str,
        offer_id: str,
        batch_id: str,
        batch_used_ips: set[str],
        batch_gateway_assigned: set[str],
        batch_assigned_counts: dict[str, int],
        allow_used_ip: bool,
        transfer_context: Optional[dict] = None,
        worker_id: Optional[str] = None,
    ) -> bool:
        gateway = self._worker_gateway
        if gateway is None:
            return False
        external_id = worker_id or gateway.select_worker_round_robin(
            batch_used_ips=batch_used_ips,
            batch_assigned_workers=batch_gateway_assigned,
            batch_assigned_counts=batch_assigned_counts,
            allow_used_ip=allow_used_ip,
            exclude_worker_ids={w.worker_id for w in self.workers},
        )
        if not external_id:
            return False
        if await gateway.deliver_task_offer(external_id, offer):
            batch_gateway_assigned.add(external_id)
            batch_assigned_counts[external_id] = (
                batch_assigned_counts.get(external_id, 0) + 1
            )
            profile = gateway._get_profile(external_id)
            if profile.ip:
                batch_used_ips.add(profile.ip)
                self._mark_spread_ip(profile.ip)
            return True
        logger.warning(
            "External worker dispatch failed: batch=%s task=%s offer=%s worker=%s",
            short_id(batch_id, 12),
            short_id(task_id),
            short_id(offer_id),
            short_id(external_id),
        )
        return False

    async def deliver_task_offer_batch(self, batch_id: str, offers: list[dict]) -> tuple[int, int]:
        delivered = 0
        failed = 0
        queued = 0
        batch_used_ips: set[str] = set()
        batch_assigned_counts: dict[str, int] = defaultdict(int)
        batch_gateway_assigned: set[str] = set()
        hidden_batch_assigned: set[str] = set()
        transfer = get_transfer_module()
        has_external = self._worker_gateway is not None

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
            # Path finalized after worker selection (CF enable is per embedded worker).
            path = (
                "predefined_etag"
                if transfer.uses_predefined_etag_early_submit(transfer_context)
                else "standard"
            )

            # After sync_done: every offer → simple-worker queue; drain to free
            # hidden workers (extras wait — no silent drop / no BeamCore fail).
            if (
                self.uses_overflow_routing
                and self._hidden_worker_gateway is not None
                and self.workers
            ):
                beamcore_worker = self._beamcore_worker_for_overflow()
                _log_embedded_task_offer(
                    task_id=str(task_id),
                    offer_id=str(offer_id),
                    worker_slot=beamcore_worker.slot,
                    transfer_context=transfer_context,
                    path=f"{path}:simple",
                    beamcore_worker_id=beamcore_worker.worker_id,
                )
                if self._enqueue_overflow(
                    offer=offer,
                    transfer_context=transfer_context,
                    path=f"{path}:simple",
                    batch_id=str(batch_id or ""),
                ):
                    queued += 1
                    continue
                logger.warning(
                    "Simple-worker overflow queue full: batch=%s task=%s offer=%s",
                    short_id(batch_id, 12),
                    short_id(task_id),
                    short_id(offer_id),
                )
                self._log_task_failed(
                    transfer,
                    transfer_context,
                    task_id=task_id,
                    offer_id=offer_id,
                    reason="no_hidden_worker_capacity",
                )
                await self._send_result(
                    beamcore_worker,
                    task_id,
                    offer_id,
                    success=False,
                    error="simple_overflow_queue_full",
                    transfer_context=transfer_context,
                )
                failed += 1
                continue

            # in_process hybrid: maximize parallel bandwidth so last task finishes
            # sooner. Prefer fresh-IP embedded, then fresh-IP external, then reuse
            # the least-loaded / fastest eligible worker (embedded or external).
            worker: Optional[EmbeddedWorker] = None
            if has_external:
                spread_ips = self._spread_used_ips(batch_used_ips)
                worker = self._select_embedded_unused_ip(
                    spread_ips, batch_assigned_counts
                )
                if worker is None:
                    if await self._dispatch_external(
                        offer=offer,
                        task_id=task_id,
                        offer_id=offer_id,
                        batch_id=batch_id,
                        batch_used_ips=spread_ips,
                        batch_gateway_assigned=batch_gateway_assigned,
                        batch_assigned_counts=batch_assigned_counts,
                        allow_used_ip=False,
                        transfer_context=transfer_context,
                    ):
                        delivered += 1
                        continue
                    # IPs exhausted — reuse for makespan: lowest load, then highest Mbps.
                    self._clear_hybrid_spread_ips()
                    emb = self._select_worker(batch_used_ips, batch_assigned_counts)
                    emb_n = (
                        batch_assigned_counts.get(emb.worker_id, 0) if emb else 10**9
                    )
                    emb_mbps = 0.0
                    gateway = self._worker_gateway
                    ext_id = None
                    ext_n = 10**9
                    ext_mbps = 0.0
                    if gateway is not None:
                        ext_id = gateway.select_worker_round_robin(
                            batch_used_ips=batch_used_ips,
                            batch_assigned_workers=batch_gateway_assigned,
                            batch_assigned_counts=batch_assigned_counts,
                            allow_used_ip=True,
                            exclude_worker_ids={w.worker_id for w in self.workers},
                        )
                        if ext_id:
                            ext_n = batch_assigned_counts.get(ext_id, 0)
                            ext_mbps = gateway._get_profile(ext_id).average_mbps
                    # (load, -mbps): prefer less loaded, then faster — not "local first".
                    if ext_id is not None and (ext_n, -ext_mbps) <= (emb_n, -emb_mbps):
                        if await self._dispatch_external(
                            offer=offer,
                            task_id=task_id,
                            offer_id=offer_id,
                            batch_id=batch_id,
                            batch_used_ips=batch_used_ips,
                            batch_gateway_assigned=batch_gateway_assigned,
                            batch_assigned_counts=batch_assigned_counts,
                            allow_used_ip=True,
                            transfer_context=transfer_context,
                            worker_id=ext_id,
                        ):
                            delivered += 1
                            continue
                    worker = emb
            else:
                worker = self._select_worker(batch_used_ips, batch_assigned_counts)

            if worker is None:
                reason = "no_worker_capacity"
                if self._enqueue_overflow(
                    offer=offer,
                    transfer_context=transfer_context,
                    path=path,
                    batch_id=batch_id,
                ):
                    queued += 1
                    continue
                logger.warning(
                    "No worker capacity for batch=%s task=%s offer=%s",
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
                self._mark_spread_ip(worker.ip)

            if (
                path == "predefined_etag"
                and self._cf_transfer_active(worker)
            ):
                path = "cloudflare_worker"

            _log_embedded_task_offer(
                task_id=str(task_id),
                offer_id=str(offer_id),
                worker_slot=worker.slot,
                transfer_context=transfer_context,
                path=path,
            )
            task = asyncio.create_task(
                self._handle_offer(
                    worker,
                    offer,
                    transfer_context,
                    path=path,
                    batch_used_ips=batch_used_ips,
                    batch_assigned_workers=hidden_batch_assigned,
                )
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

        if queued:
            logger.info(
                "Embedded batch overflow queued=%s delivered=%s failed=%s pending=%s batch=%s",
                queued,
                delivered,
                failed,
                len(self._overflow),
                short_id(batch_id, 12),
            )
            self._schedule_overflow_drain()

        logger.debug(
            "Embedded batch queued: batch=%s offers=%s delivered=%s failed=%s overflow=%s",
            short_id(batch_id, 12),
            len(offers),
            delivered,
            failed,
            queued,
        )
        return delivered, failed

    def _enqueue_overflow(
        self,
        *,
        offer: dict,
        transfer_context: dict,
        path: str,
        batch_id: str,
    ) -> bool:
        from core.worker_gateway import OVERFLOW_QUEUE_ENABLED, OVERFLOW_QUEUE_MAX

        if not OVERFLOW_QUEUE_ENABLED:
            return False
        # Simple-worker mode: always hold on the embedded queue (drained to hidden WS).
        # in_process hybrid: prefer gateway overflow when external workers exist.
        if not self.uses_overflow_routing:
            gateway = self._worker_gateway
            if gateway is not None and hasattr(gateway, "_enqueue_overflow"):
                if gateway._enqueue_overflow(offer):
                    gateway._schedule_overflow_drain()
                    return True
        offer_id = str(offer.get("offer_id") or offer.get("task_id") or "").strip()
        if not offer_id:
            return False
        if offer_id in self._overflow_ids:
            return True
        if OVERFLOW_QUEUE_MAX > 0 and len(self._overflow) >= OVERFLOW_QUEUE_MAX:
            logger.warning(
                "embedded overflow queue full (%d); cannot hold offer=%s",
                OVERFLOW_QUEUE_MAX,
                short_id(offer_id),
            )
            return False
        _stamp_ctx_queued(transfer_context)
        self._overflow.append(
            {
                "offer": dict(offer),
                "transfer_context": dict(transfer_context),
                "path": path,
                "batch_id": batch_id,
            }
        )
        self._overflow_ids.add(offer_id)
        logger.info(
            "_workers | embedded_overflow_enqueue task=%s offer=%s pending=%d path=%s",
            short_id(offer.get("task_id")),
            short_id(offer_id),
            len(self._overflow),
            path,
        )
        return True

    def _schedule_overflow_drain(self) -> None:
        from core.worker_gateway import OVERFLOW_QUEUE_ENABLED

        if not OVERFLOW_QUEUE_ENABLED or not self._overflow:
            return
        self._overflow_drain_pending = True
        if self._overflow_drain_running:
            return
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            return
        self._overflow_drain_running = True
        loop.create_task(self._overflow_drain_loop(), name="embedded-overflow-drain")

    async def _overflow_drain_loop(self) -> None:
        try:
            while self._overflow_drain_pending:
                self._overflow_drain_pending = False
                await self._drain_overflow_queue()
        finally:
            self._overflow_drain_running = False
            if self._overflow_drain_pending and self._overflow:
                self._schedule_overflow_drain()

    async def _drain_overflow_queue(self) -> int:
        delivered = 0
        # Count offers already dispatched in this drain pass so _select_worker
        # does not over-book a worker before active_offer_ids is updated.
        batch_assigned_counts: dict[str, int] = {}
        while self._overflow:
            item = self._overflow[0]
            offer = item["offer"]
            transfer_context = item["transfer_context"]
            path = str(item.get("path") or "standard")
            offer_id = str(offer.get("offer_id") or offer.get("task_id") or "")
            task_id = str(offer.get("task_id") or offer_id)

            # embedded_global after sync_done: drain to free simple/hidden workers.
            if self.uses_overflow_routing and self._hidden_worker_gateway is not None:
                hidden_id = self._hidden_worker_gateway.select_hidden_worker_round_robin()
                if not hidden_id:
                    break
                self._overflow.pop(0)
                self._overflow_ids.discard(offer_id)
                # Reserve immediately so the next select cannot double-book.
                if offer_id and hasattr(self._hidden_worker_gateway, "mark_worker_busy"):
                    self._hidden_worker_gateway.mark_worker_busy(hidden_id, offer_id)
                beamcore_worker = self._beamcore_worker_for_overflow()
                task = asyncio.create_task(
                    self._handle_overflow_offer(
                        beamcore_worker,
                        offer,
                        transfer_context,
                        path=path,
                        hidden_worker_id=hidden_id,
                        enqueue_if_no_capacity=True,
                    )
                )
                self._task_handles.add(task)
                task.add_done_callback(
                    lambda t, tid=task_id, oid=offer_id: self._log_offer_task_done(
                        t, task_id=tid, offer_id=oid
                    )
                )
                task.add_done_callback(self._task_handles.discard)
                delivered += 1
                logger.info(
                    "_workers | simple_overflow_deliver task=%s offer=%s "
                    "hidden_worker=%s pending=%d",
                    short_id(task_id),
                    short_id(offer_id),
                    short_id(hidden_id),
                    len(self._overflow),
                )
                continue

            worker = self._select_worker(batch_assigned_counts=batch_assigned_counts)
            if worker is None:
                gateway = self._worker_gateway
                if gateway is not None and hasattr(gateway, "select_worker_round_robin"):
                    ext_id = gateway.select_worker_round_robin(
                        exclude_worker_ids={w.worker_id for w in self.workers},
                    )
                    if ext_id and await gateway.deliver_task_offer(ext_id, offer):
                        self._overflow.pop(0)
                        self._overflow_ids.discard(offer_id)
                        delivered += 1
                        continue
                break

            self._overflow.pop(0)
            self._overflow_ids.discard(offer_id)
            batch_assigned_counts[worker.worker_id] = (
                batch_assigned_counts.get(worker.worker_id, 0) + 1
            )
            if (
                path == "predefined_etag"
                and self._cf_transfer_active(worker)
            ):
                path = "cloudflare_worker"
            _log_embedded_task_offer(
                task_id=task_id,
                offer_id=offer_id,
                worker_slot=worker.slot,
                transfer_context=transfer_context,
                path=path,
            )
            task = asyncio.create_task(
                self._handle_offer(
                    worker,
                    offer,
                    transfer_context,
                    path=path,
                )
            )
            self._task_handles.add(task)
            task.add_done_callback(
                lambda t, tid=task_id, oid=offer_id: self._log_offer_task_done(
                    t, task_id=tid, offer_id=oid
                )
            )
            task.add_done_callback(self._task_handles.discard)
            delivered += 1
            logger.info(
                "_workers | embedded_overflow_deliver task=%s offer=%s worker=%s pending=%d",
                short_id(task_id),
                short_id(offer_id),
                short_id(worker.worker_id),
                len(self._overflow),
            )
        return delivered

    async def _handle_offer(
        self,
        worker: EmbeddedWorker,
        offer: dict,
        transfer_context: dict,
        *,
        path: str = "standard",
        batch_used_ips: Optional[set[str]] = None,
        batch_assigned_workers: Optional[set[str]] = None,
    ) -> None:
        transfer = get_transfer_module()
        task_id = offer.get("task_id") or offer.get("offer_id")
        offer_id = offer.get("offer_id") or task_id

        try:
            deadline_us = int(offer.get("deadline_us") or 0)
        except (TypeError, ValueError):
            deadline_us = 0
        estimated_bytes = transfer.estimate_task_bytes(offer)

        try:
            capacity_error = self._reserve_capacity(worker, estimated_bytes)
            if capacity_error:
                if (
                    self.uses_overflow_routing
                    and self._hidden_worker_gateway is not None
                    and capacity_error.startswith("queue_full")
                ):
                    logger.info(
                        "Embedded at capacity; overflowing task=%s offer=%s to hidden worker",
                        short_id(task_id),
                        short_id(offer_id),
                    )
                    await self._handle_overflow_offer(
                        worker,
                        offer,
                        transfer_context,
                        path=f"{path}:overflow",
                        batch_used_ips=batch_used_ips,
                        batch_assigned_workers=batch_assigned_workers,
                    )
                    return
                # Race: drained more than capacity — put back on overflow, do not fail BeamCore.
                if capacity_error.startswith("queue_full") and self._enqueue_overflow(
                    offer=offer,
                    transfer_context=transfer_context,
                    path=path,
                    batch_id="",
                ):
                    logger.info(
                        "%s queue_full re-queued task=%s offer=%s worker_slot=%s pending=%d",
                        _WORKERS_LOG,
                        short_id(task_id),
                        short_id(offer_id),
                        worker.slot,
                        len(self._overflow),
                    )
                    self._schedule_overflow_drain()
                    return
                logger.warning(
                    "Embedded failing offer: task=%s offer=%s worker_slot=%s reason=%s",
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
                await self._send_result(
                    worker,
                    task_id,
                    offer_id,
                    success=False,
                    error=capacity_error,
                    transfer_context=transfer_context,
                )
                return

            worker.active_offer_ids.add(str(offer_id or ""))
            _stamp_ctx_started(transfer_context, worker.worker_id)

            skip_reasons = transfer.predefined_etag_early_submit_skip_reasons(transfer_context)
            if skip_reasons:
                logger.debug(
                    "Embedded fast path skipped: task=%s offer=%s worker_slot=%s reasons=%s",
                    short_id(task_id),
                    short_id(offer_id),
                    worker.slot,
                    "; ".join(skip_reasons),
                )

            if transfer.uses_predefined_etag_early_submit(transfer_context):
                cf_url = self._pick_cf_transfer_url(worker)
                if cf_url:
                    await self._handle_cloudflare_transfer_offer(
                        worker,
                        offer,
                        task_id,
                        offer_id,
                        transfer_context,
                        deadline_us,
                        worker_url=cf_url,
                    )
                else:
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
            self._schedule_overflow_drain()

    def _reserve_capacity(self, worker: EmbeddedWorker, estimated_bytes: int) -> Optional[str]:
        transfer = get_transfer_module()
        if estimated_bytes > transfer.MAX_IN_FLIGHT_BYTES:
            return f"task_too_large:{estimated_bytes}"
        if not worker.has_capacity:
            return f"queue_full:{worker.active_count}"
        return None

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
        transfer_context: Optional[dict] = None,
        cached: Optional[bool] = None,
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
        started = time.perf_counter()
        ack = await self.upstream.send_task_result(payload)
        latency_ms = (time.perf_counter() - started) * 1000
        status = ack.get("status")
        if status == "completed":
            _log_embedded_task_completed(
                task_id=str(task_id),
                offer_id=str(offer_id),
                worker_id=worker.worker_id,
                latency_ms=latency_ms,
            )
        else:
            logger.warning(
                "Embedded task result not completed: task=%s offer=%s worker=%s "
                "received=%s status=%s reason=%s",
                short_id(task_id),
                short_id(offer_id),
                short_id(worker.worker_id),
                ack.get("received"),
                status,
                ack.get("reason") or ack.get("error") or ack.get("message"),
            )
        return ack

    async def _handle_overflow_offer(
        self,
        worker: EmbeddedWorker,
        offer: dict,
        transfer_context: dict,
        *,
        path: str,
        batch_used_ips: Optional[set[str]] = None,
        batch_assigned_workers: Optional[set[str]] = None,
        hidden_worker_id: Optional[str] = None,
        enqueue_if_no_capacity: bool = True,
    ) -> None:
        """Simple-worker transfer; orch submits task_result as WORKER_S only.

        When no hidden worker is free, enqueue and wait (do not fail BeamCore)
        unless the overflow queue is full or the gateway is missing.
        """
        transfer = get_transfer_module()
        task_id = str(offer.get("task_id") or offer.get("offer_id") or "")
        offer_id = str(offer.get("offer_id") or task_id or "")
        beamcore_worker = self._beamcore_worker_for_overflow(worker)

        gateway = self._hidden_worker_gateway
        if gateway is None:
            self._log_task_failed(
                transfer,
                transfer_context,
                task_id=task_id,
                offer_id=offer_id,
                reason="hidden_worker_gateway_unavailable",
            )
            await self._send_result(
                beamcore_worker,
                task_id,
                offer_id,
                success=False,
                error="hidden_worker_gateway_unavailable",
                transfer_context=transfer_context,
            )
            return

        if not hidden_worker_id:
            hidden_worker_id = gateway.select_hidden_worker_round_robin(
                batch_used_ips=batch_used_ips,
                batch_assigned_workers=batch_assigned_workers,
            )
        if not hidden_worker_id:
            if enqueue_if_no_capacity and self._enqueue_overflow(
                offer=offer,
                transfer_context=transfer_context,
                path=path,
                batch_id="",
            ):
                logger.info(
                    "%s no_hidden_worker_capacity; queued task=%s offer=%s pending=%d",
                    _WORKERS_LOG,
                    short_id(task_id),
                    short_id(offer_id),
                    len(self._overflow),
                )
                self._schedule_overflow_drain()
                return
            self._log_task_failed(
                transfer,
                transfer_context,
                task_id=task_id,
                offer_id=offer_id,
                reason="no_hidden_worker_capacity",
            )
            await self._send_result(
                beamcore_worker,
                task_id,
                offer_id,
                success=False,
                error="no_hidden_worker_capacity",
                transfer_context=transfer_context,
            )
            return

        loop = asyncio.get_running_loop()
        result_future: asyncio.Future = loop.create_future()
        self._transfer_waiters[offer_id] = result_future
        if batch_assigned_workers is not None:
            batch_assigned_workers.add(hidden_worker_id)
        hidden_ip = ""
        if hasattr(gateway, "_get_profile"):
            hidden_ip = gateway._get_profile(hidden_worker_id).ip.strip()
        if hidden_ip and batch_used_ips is not None:
            batch_used_ips.add(hidden_ip)

        logger.info(
            "%s overflow_dispatch task=%s offer=%s beamcore_worker=%s hidden_worker=%s path=%s",
            _WORKERS_LOG,
            short_id(task_id),
            short_id(offer_id),
            short_id(beamcore_worker.worker_id),
            short_id(hidden_worker_id),
            path,
        )

        # Reserve before await so concurrent drain/batch selects skip this worker.
        if offer_id and hasattr(gateway, "mark_worker_busy"):
            gateway.mark_worker_busy(hidden_worker_id, offer_id)

        dispatched = await gateway.deliver_transfer_offer(hidden_worker_id, offer)
        if not dispatched:
            self._transfer_waiters.pop(offer_id, None)
            if batch_assigned_workers is not None:
                batch_assigned_workers.discard(hidden_worker_id)
            if hasattr(gateway, "mark_worker_idle"):
                gateway.mark_worker_idle(
                    hidden_worker_id, offer_id, drain_overflow=False
                )
            if enqueue_if_no_capacity and self._enqueue_overflow(
                offer=offer,
                transfer_context=transfer_context,
                path=path,
                batch_id="",
            ):
                logger.warning(
                    "%s hidden_worker_dispatch_failed; re-queued task=%s offer=%s "
                    "from=%s pending=%d",
                    _WORKERS_LOG,
                    short_id(task_id),
                    short_id(offer_id),
                    short_id(hidden_worker_id),
                    len(self._overflow),
                )
                self._schedule_overflow_drain()
                return
            await self._send_result(
                beamcore_worker,
                task_id,
                offer_id,
                success=False,
                error="hidden_worker_dispatch_failed",
                transfer_context=transfer_context,
            )
            return

        _stamp_ctx_started(transfer_context, hidden_worker_id)

        timeout = float(os.environ.get("WORKER_OVERFLOW_TRANSFER_TIMEOUT", "120"))
        try:
            result_msg = await asyncio.wait_for(result_future, timeout=timeout)
        except asyncio.TimeoutError:
            self._transfer_waiters.pop(offer_id, None)
            if batch_assigned_workers is not None:
                batch_assigned_workers.discard(hidden_worker_id)
            logger.warning(
                "Overflow transfer timeout: task=%s offer=%s timeout=%.0fs",
                short_id(task_id),
                short_id(offer_id),
                timeout,
            )
            self._log_task_failed(
                transfer,
                transfer_context,
                task_id=task_id,
                offer_id=offer_id,
                reason="overflow_transfer_timeout",
            )
            await self._send_result(
                beamcore_worker,
                task_id,
                offer_id,
                success=False,
                error="overflow_transfer_timeout",
                transfer_context=transfer_context,
            )
            return

        if batch_assigned_workers is not None:
            batch_assigned_workers.discard(hidden_worker_id)

        success = bool(result_msg.get("success", False))
        chunk_hash = str(result_msg.get("chunk_hash") or "")
        etag = result_msg.get("etag")
        error = result_msg.get("error")
        cached = result_msg.get("cached")
        if isinstance(cached, str):
            cached = cached.lower() == "true"

        if success:
            _log_embedded_task_done(
                task_id=task_id,
                offer_id=offer_id,
                transfer_context=transfer_context,
                chunk_hash=chunk_hash,
                etag=str(etag or ""),
                cached=bool(cached),
                fetch_ms=float(result_msg.get("fetch_ms") or 0.0),
                send_ms=float(result_msg.get("put_ms") or result_msg.get("send_ms") or 0.0),
                path=path,
                worker_id=hidden_worker_id or "",
            )
            if chunk_hash and cached is not True:
                transfer.maybe_store_predefined_etag_cache_on_success(
                    transfer_context,
                    chunk_hash,
                    etag,
                    log_prefix="[Embedded/overflow]",
                    task_id=task_id,
                    offer_id=offer_id,
                    push_to_control_server=True,
                )
        else:
            self._log_task_failed(
                transfer,
                transfer_context,
                task_id=task_id,
                offer_id=offer_id,
                reason=str(error or "overflow_transfer_failed"),
                chunk_hash=chunk_hash,
                etag=str(etag or ""),
                cached=cached if isinstance(cached, bool) else None,
            )

        await self._send_result(
            beamcore_worker,
            task_id,
            offer_id,
            success=success,
            chunk_hash=chunk_hash,
            etag=etag,
            error=str(error) if error else None,
            transfer_context=transfer_context,
            cached=cached if isinstance(cached, bool) else None,
        )

    async def _handle_cloudflare_transfer_offer(
        self,
        worker: EmbeddedWorker,
        offer: dict,
        task_id: str,
        offer_id: str,
        transfer_context: dict,
        deadline_us: int,
        *,
        worker_url: str,
    ) -> None:
        """CF Worker does transfer only; embedded worker owns task_result."""
        transfer = get_transfer_module()
        timeout_sec = float(self.settings.cf_transfer_worker_timeout or 120.0)

        remaining = transfer.remaining_deadline_seconds(deadline_us)
        if remaining is not None and remaining < 5:
            reason = f"deadline_too_close:{remaining:.1f}s"
            logger.warning(
                "CF transfer skipped: task=%s offer=%s reason=%s",
                short_id(task_id),
                short_id(offer_id),
                reason,
            )
            self._log_task_failed(
                transfer,
                transfer_context,
                task_id=str(task_id),
                offer_id=str(offer_id),
                reason=reason,
            )
            await self._send_result(
                worker,
                task_id,
                offer_id,
                success=False,
                error=reason,
                transfer_context=transfer_context,
            )
            return

        result = await call_cloudflare_transfer_worker(
            worker_url=worker_url,
            offer=offer,
            task_id=str(task_id),
            offer_id=str(offer_id),
            timeout_sec=timeout_sec,
            client=self.http_client,
        )

        if not result.success:
            reason = str(result.error or "cf_transfer_failed")
            self._log_task_failed(
                transfer,
                transfer_context,
                task_id=str(task_id),
                offer_id=str(offer_id),
                reason=reason,
                etag=result.etag or "",
            )
            await self._send_result(
                worker,
                task_id,
                offer_id,
                success=False,
                error=reason,
                etag=result.etag,
                transfer_context=transfer_context,
                cached=False,
            )
            return

        await self._send_result(
            worker,
            task_id,
            offer_id,
            success=True,
            etag=result.etag,
            transfer_context=transfer_context,
            cached=False,
        )
        _log_embedded_task_done(
            task_id=str(task_id),
            offer_id=str(offer_id),
            transfer_context=transfer_context,
            etag=result.etag or "",
            cached=False,
            path="cloudflare_worker",
            hash_source="response_etag",
            fetch_ms=result.fetch_ms,
            send_ms=result.send_ms,
            worker_id=worker.worker_id,
        )

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

        offer_started_at = time.perf_counter()

        outcome = await transfer.predefined_etag_submit_flow(
            worker.state,
            str(task_id),
            str(offer_id),
            offer,
            transfer_context,
            deadline_us,
            log_prefix="[Embedded]",
            offer_started_at=offer_started_at,
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
                etag=outcome.etag or "",
            )
            if outcome.error:
                await self._send_result(
                    worker,
                    task_id,
                    offer_id,
                    success=False,
                    chunk_hash=outcome.chunk_hash,
                    error=outcome.error,
                    transfer_context=transfer_context,
                    cached=outcome.used_cache,
                )
            return

        await self._send_result(
            worker,
            task_id,
            offer_id,
            success=True,
            chunk_hash=outcome.chunk_hash,
            etag=outcome.etag,
            transfer_context=transfer_context,
            cached=outcome.used_cache,
        )
        path_label = "miss"
        if outcome.used_cache:
            if transfer.WORKER_PREDEFINED_ETAG_EARLY_SUBMIT and (
                getattr(outcome, "send_ms", 0.0) or 0.0
            ) <= 0:
                path_label = "cache_early"
            else:
                path_label = "cache_stream"
        elif hasattr(transfer, "resolve_task_path"):
            path_label = transfer.resolve_task_path(
                transfer_context,
                used_cache=outcome.used_cache,
                send_ms=getattr(outcome, "send_ms", 0.0) or 0.0,
            )
        _log_embedded_task_done(
            task_id=str(task_id),
            offer_id=str(offer_id),
            transfer_context=transfer_context,
            chunk_hash=outcome.chunk_hash,
            etag=outcome.etag or "",
            etag_local=getattr(outcome, "etag_local", "") or "",
            cached=outcome.used_cache,
            path=path_label,
            hash_source=getattr(outcome, "hash_source", "") or "",
            load_ms=getattr(outcome, "load_ms", 0.0) or 0.0,
            hash_ms=getattr(outcome, "hash_ms", 0.0) or 0.0,
            etag_ms=getattr(outcome, "etag_ms", 0.0) or 0.0,
            fetch_ms=outcome.fetch_ms,
            send_ms=outcome.send_ms,
            worker_id=worker.worker_id,
        )
        if outcome.used_cache and transfer.WORKER_PREDEFINED_ETAG_EARLY_SUBMIT and (
            getattr(outcome, "send_ms", 0.0) or 0.0
        ) <= 0:
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
            logger.debug(
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
                push_to_control_server=False,
            )
            _log_embedded_task_done(
                task_id=str(task_id),
                offer_id=str(offer_id),
                transfer_context=transfer_context,
                chunk_hash=result.chunk_hash,
                etag=result.etag or "",
                cached=False,
                path="standard",
                hash_source="response_etag",
                fetch_ms=result.fetch_ms,
                send_ms=result.send_ms,
                worker_id=worker.worker_id,
            )
        else:
            self._log_task_failed(
                transfer,
                transfer_context,
                task_id=str(task_id),
                offer_id=str(offer_id),
                reason=str(result.error_msg or "standard_transfer_failed"),
                chunk_hash=result.chunk_hash,
                etag=result.etag or "",
                cached=False,
            )
        await self._send_result(
            worker,
            task_id,
            offer_id,
            success=result.success,
            chunk_hash=result.chunk_hash,
            etag=result.etag,
            error=result.error_msg,
            transfer_context=transfer_context,
            cached=False,
        )
