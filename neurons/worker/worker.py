#!/usr/bin/env python3
"""
Beam Network Worker

Registers with BeamCore, connects to an orchestrator-owned worker gateway, and handles data transfer tasks.
Uses bittensor wallet for authentication.

Minimum Requirements:
    - CPU: 2 cores
    - RAM: 4 GB
    - Storage: 20 GB SSD
    - Network: 100 Mbps symmetric (upload/download)
    - OS: Ubuntu 22.04+ / Debian 12+ / macOS 13+

Tech Stack:
    - Python 3.10+
    - bittensor >= 10.3.1,<11.0.0
    - httpx >= 0.25.0
    - websockets >= 12.0

Installation:
    pip install bittensor httpx websockets

Usage:
    # Using wallet settings from workspace .env / config/workers/<instance>.env:
    python3 worker.py --env-file config/workers/worker1.env

    # Using custom wallet via CLI (overrides env):
    python3 worker.py --wallet.name my_wallet --wallet.hotkey my_hotkey

    # Mainnet:
    python3 worker.py --subtensor.network finney
"""

import argparse
import asyncio
import contextlib
import hashlib
import json
import logging
import os
import re
import signal
import sys
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from importlib.metadata import PackageNotFoundError, version as package_version
from pathlib import Path
from typing import Any, Awaitable, Callable, Dict, Optional
from urllib.parse import parse_qs, urlencode, urlsplit, urlunsplit

import httpx

try:
    import websockets
    from websockets.exceptions import ConnectionClosed

    try:
        from websockets.exceptions import InvalidStatus
    except ImportError:
        from websockets.exceptions import InvalidStatusCode as InvalidStatus
    WEBSOCKETS_AVAILABLE = True
except ImportError:
    WEBSOCKETS_AVAILABLE = False

try:
    import bittensor as bt

    BITTENSOR_AVAILABLE = True
except ImportError:
    BITTENSOR_AVAILABLE = False
    print("Error: bittensor library not installed.")
    print("Install with: pip install bittensor")
    sys.exit(1)


def _workspace_root() -> Path:
    return Path(__file__).resolve().parents[2]


_REPO_ROOT = _workspace_root()
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))


def _extract_env_file_arg(argv: list[str]) -> tuple[Optional[Path], list[str]]:
    """Pull --env-file from argv before bittensor/argparse runs."""
    cleaned: list[str] = []
    env_file: Optional[Path] = None
    idx = 0
    while idx < len(argv):
        arg = argv[idx]
        if arg == "--env-file":
            if idx + 1 >= len(argv):
                print("Error: --env-file requires a path argument", file=sys.stderr)
                sys.exit(2)
            env_file = Path(argv[idx + 1]).expanduser()
            idx += 2
            continue
        if arg.startswith("--env-file="):
            env_file = Path(arg.split("=", 1)[1]).expanduser()
            idx += 1
            continue
        cleaned.append(arg)
        idx += 1
    return env_file, cleaned


def _resolve_worker_env_file() -> Optional[Path]:
    cli_file, _ = _extract_env_file_arg(sys.argv[1:])
    if cli_file is not None:
        return cli_file

    env_path = os.environ.get("WORKER_ENV_FILE", "").strip()
    if env_path:
        return Path(env_path).expanduser()
    return None


def _resolve_env_path(path: Path) -> Path:
    if path.is_absolute():
        return path
    return _workspace_root() / path


LOADED_ENV_FILES: list[Path] = []


def _worker_instance_name() -> Optional[str]:
    worker_env = _resolve_worker_env_file()
    if worker_env is None:
        return None
    return _resolve_env_path(worker_env).stem


class _StreamToLogger:
    """Capture print() output into the worker log file."""

    _active = False

    def __init__(self, log_fn, mirror):
        self._log_fn = log_fn
        self._mirror = mirror

    def write(self, buf: str) -> None:
        if not buf:
            return
        if _StreamToLogger._active:
            if self._mirror is not None:
                self._mirror.write(buf)
                self._mirror.flush()
            return
        _StreamToLogger._active = True
        try:
            for line in buf.rstrip().splitlines():
                if line:
                    self._log_fn(line)
            if self._mirror is not None:
                self._mirror.write(buf)
                self._mirror.flush()
        finally:
            _StreamToLogger._active = False

    def flush(self) -> None:
        if self._mirror is not None:
            self._mirror.flush()

    def isatty(self) -> bool:
        return bool(self._mirror and self._mirror.isatty())


def configure_worker_logging() -> None:
    """Write worker output to logs/workers/<instance>.log (console when interactive)."""
    instance = _worker_instance_name()
    if not instance:
        return

    original_stdout = sys.stdout
    original_stderr = sys.stderr

    log_root = Path(os.environ.get("LOG_DIR", _workspace_root() / "logs"))
    log_dir = log_root / "workers"
    log_dir.mkdir(parents=True, exist_ok=True)

    log_format = "%(asctime)s.%(msecs)03.0f | %(levelname)s | %(message)s"
    log_datefmt = "%Y-%m-%d %H:%M:%S"
    formatter = logging.Formatter(log_format, datefmt=log_datefmt)

    file_handler = logging.FileHandler(log_dir / f"{instance}.log")
    file_handler.setFormatter(formatter)

    handlers: list[logging.Handler] = [file_handler]
    mirror_out = original_stdout if original_stdout.isatty() else None
    mirror_err = original_stderr if original_stderr.isatty() else None
    # Console output is mirrored by _StreamToLogger; avoid StreamHandler here because
    # it would write through wrapped stderr and can recurse with print capture.

    worker_logger = logging.getLogger("worker")
    worker_logger.handlers.clear()
    worker_logger.propagate = False
    worker_logger.setLevel(logging.INFO)
    for handler in handlers:
        worker_logger.addHandler(handler)

    sys.stdout = _StreamToLogger(worker_logger.info, mirror_out)
    sys.stderr = _StreamToLogger(worker_logger.warning, mirror_err)


def _load_workspace_env() -> None:
    """Load shared .env, then optional per-worker env (override)."""
    try:
        from dotenv import load_dotenv
    except ImportError:
        return

    root = _workspace_root()
    shared_env = root / ".env"
    if shared_env.exists():
        load_dotenv(shared_env, override=False)
        LOADED_ENV_FILES.append(shared_env)

    worker_env = _resolve_worker_env_file()
    if worker_env is None:
        return

    worker_env = _resolve_env_path(worker_env)
    if worker_env.exists():
        load_dotenv(worker_env, override=True)
        LOADED_ENV_FILES.append(worker_env)
    else:
        print(f"Error: worker env file not found: {worker_env}", file=sys.stderr)
        sys.exit(2)


if os.environ.get("BEAM_SKIP_WORKER_BOOTSTRAP") != "1":
    _load_workspace_env()
    configure_worker_logging()

# =============================================================================
# Configuration
# =============================================================================

# Network endpoints
MAINNET_URL = "https://beamcore.b1m.ai"

# Connection mode: worker transport is websocket-only after registration.
CONNECTION_MODE = os.environ.get("CONNECTION_MODE", "websocket").lower()


def resolve_worker_version() -> str:
    try:
        return package_version("beam")
    except PackageNotFoundError:
        return "0.2.1"


def parse_strict_semver(value: str) -> Optional[tuple[int, int, int]]:
    parts = str(value or "").split(".")
    if len(parts) != 3:
        return None
    parsed: list[int] = []
    for part in parts:
        if not part.isdigit():
            return None
        if len(part) > 1 and part.startswith("0"):
            return None
        parsed.append(int(part))
    return parsed[0], parsed[1], parsed[2]


def worker_version_satisfies(minimum_version: str) -> bool:
    current = parse_strict_semver(WORKER_VERSION)
    minimum = parse_strict_semver(minimum_version)
    return bool(current and minimum and current >= minimum)


WORKER_VERSION = resolve_worker_version()

# WebSocket settings
WS_RECONNECT_MIN_DELAY = 12.0  # must exceed server's 10s cooldown
WS_RECONNECT_MAX_DELAY = 60.0
WS_RECONNECT_MULTIPLIER = 2.0
_ws_max_reconnect_attempts = os.environ.get("WS_MAX_RECONNECT_ATTEMPTS", "0").strip()
WS_MAX_RECONNECT_ATTEMPTS = (
    None if not _ws_max_reconnect_attempts or int(_ws_max_reconnect_attempts) <= 0 else int(_ws_max_reconnect_attempts)
)

WS_PING_INTERVAL = 25  # seconds

# Transfer settings
DEFAULT_CHUNK_SIZE_BYTES = 4 * 1024 * 1024
MAX_CONCURRENT_TASKS = max(1, int(os.environ.get("WORKER_MAX_CONCURRENT_TASKS", "4")))
MAX_QUEUED_WS_TASKS = max(
    1, int(os.environ.get("WORKER_MAX_QUEUED_WS_TASKS", str(MAX_CONCURRENT_TASKS)))
)
MAX_IN_FLIGHT_BYTES = max(
    DEFAULT_CHUNK_SIZE_BYTES,
    int(os.environ.get("WORKER_MAX_IN_FLIGHT_BYTES", str(256 * 1024 * 1024))),
)
FETCH_TIMEOUT = 30  # seconds
SEND_TIMEOUT = 120  # seconds — cover large uploads on slow links
MAX_RETRIES = 3
RETRY_BACKOFF = 1.0  # Base backoff in seconds
FETCH_STREAM_CHUNK_SIZE = int(
    os.environ.get("WORKER_FETCH_STREAM_CHUNK_SIZE", str(512 * 1024))
)
# Fixed-size staging uploads always return this ETag; skip PUT after hash verify.
try:
    PREDEFINED_ETAG_CHUNK_SIZE_BYTES = int(
        os.environ.get(
            "WORKER_PREDEFINED_ETAG_CHUNK_SIZE_BYTES",
            str(30 * 1024 * 1024),
        )
    )
except ValueError:
    PREDEFINED_ETAG_CHUNK_SIZE_BYTES = 30 * 1024 * 1024
PREDEFINED_ETAG = '"281ed1d5ae50e8419f9b978aab16de83"'
PREDEFINED_ETAG_ENV_CHUNK_HASH = os.environ.get(
    "WORKER_PREDEFINED_ETAG_CHUNK_HASH",
    os.environ.get("CHUNK_HASH", ""),
).strip()
PREDEFINED_ETAG_ENV_ETAG = (
    os.environ.get("WORKER_PREDEFINED_ETAG_ETAG", os.environ.get("ETAG", "")).strip()
    or (PREDEFINED_ETAG if PREDEFINED_ETAG_ENV_CHUNK_HASH else "")
)
PREDEFINED_ETAG_MIN_SUBMIT_SEC = max(
    0.0,
    float(os.environ.get("WORKER_PREDEFINED_ETAG_MIN_SUBMIT_SEC", "0")),
)
try:
    PREDEFINED_ETAG_MAX_SPEED_MBPS = max(
        0.0,
        float(
            os.environ.get(
                "WORKER_PREDEFINED_ETAG_MAX_SPEED_MBPS",
                os.environ.get("MAX_SPEED_MBPS", "0"),
            )
        ),
    )
except ValueError:
    PREDEFINED_ETAG_MAX_SPEED_MBPS = 0.0
PREDEFINED_ETAG_SOURCE_URL = (
    os.environ.get("WORKER_PREDEFINED_ETAG_SOURCE_URL", "").strip().rstrip("/")
)
try:
    PREDEFINED_ETAG_SOURCE_FILE_SIZE = int(
        os.environ.get(
            "WORKER_PREDEFINED_ETAG_SOURCE_FILE_SIZE",
            str(10 * 1024 * 1024 * 1024),
        )
    )
except ValueError:
    PREDEFINED_ETAG_SOURCE_FILE_SIZE = 0

PREDEFINED_ETAG_CACHE_FILENAME = "predefined_etag_chunks.json"
PREDEFINED_ETAG_CHUNK_DATA_DIRNAME = "predefined_etag_chunk_data"
try:
    CONTROL_SERVER_CACHE_SYNC_DELAY_SEC = max(
        0.0,
        float(os.environ.get("CONTROL_SERVER_CACHE_SYNC_DELAY_SEC", "150")),
    )
except ValueError:
    CONTROL_SERVER_CACHE_SYNC_DELAY_SEC = 150.0


@dataclass(frozen=True)
class PredefinedETagChunkCacheEntry:
    chunk_hash: str
    etag: str


_PREDEFINED_ETAG_CHUNK_CACHE: dict[str, PredefinedETagChunkCacheEntry] = {}


def _env_bool(name: str, default: bool) -> bool:
    raw = os.environ.get(name)
    if raw is None or not str(raw).strip():
        return default
    return str(raw).strip().lower() in ("1", "true", "yes")


WS_TASK_ACCEPT_ACK_TIMEOUT = float(os.environ.get("WORKER_TASK_ACCEPT_ACK_TIMEOUT", "8.0"))
WS_TASK_RESULT_ACK_TIMEOUT = float(os.environ.get("WORKER_TASK_RESULT_ACK_TIMEOUT", "45.0"))
WORKER_EARLY_TRANSFER = _env_bool("WORKER_EARLY_TRANSFER", True)
WORKER_PREDEFINED_ETAG_EARLY_SUBMIT = _env_bool("WORKER_PREDEFINED_ETAG_EARLY_SUBMIT", True)
PREDEFINED_ETAG_MAX_PARALLEL = max(
    1, int(os.environ.get("WORKER_PREDEFINED_ETAG_MAX_PARALLEL", "1"))
)
predefined_etag_fast_path_semaphore = asyncio.Semaphore(PREDEFINED_ETAG_MAX_PARALLEL)
PREWARM_ENABLED = _env_bool("WORKER_PREWARM_ENABLED", True)
PREWARM_TIMEOUT = float(os.environ.get("WORKER_PREWARM_TIMEOUT", "5"))
PREWARM_MAX_ORIGINS = max(1, int(os.environ.get("WORKER_PREWARM_MAX_ORIGINS", "32")))
try:
    WORKER_INITIAL_ORDER = int(os.environ.get("WORKER_INITIAL_ORDER", "0"))
except ValueError:
    WORKER_INITIAL_ORDER = 0


# Global semaphore for task concurrency
task_semaphore = asyncio.Semaphore(MAX_CONCURRENT_TASKS)


@dataclass
class WorkerState:
    """Worker runtime state."""

    api_url: str
    wallet: Optional[Any] = None
    worker_gateway_url: Optional[str] = None
    worker_gateway_secret: Optional[str] = None
    worker_id: Optional[str] = None
    api_key: Optional[str] = None
    worker_ip: Optional[str] = None
    orchestrator_hotkey: Optional[str] = None
    active_tasks: int = 0
    running: bool = True
    http_client: Optional[httpx.AsyncClient] = None
    ws_connected: bool = False
    ws_reconnect_attempts: int = 0
    use_websocket: bool = True
    pending_task_accepts: Dict[str, asyncio.Future] = field(default_factory=dict)
    pending_task_results: Dict[str, asyncio.Future] = field(default_factory=dict)
    active_ws_task_ids: set[str] = field(default_factory=set)
    ws_task_handles: set[asyncio.Task] = field(default_factory=set)
    reserved_ws_slots: int = 0
    reserved_bytes: int = 0
    ws_send_lock: Optional[asyncio.Lock] = None
    prewarm_origins: list[str] = field(default_factory=list)


@dataclass
class TaskExecutionResult:
    """Normalized task execution metrics used by HTTP and WebSocket paths."""

    success: bool
    bytes_transferred: int
    duration_ms: float
    chunk_hash: str = ""
    etag: Optional[str] = None
    error_msg: Optional[str] = None
    fetch_ms: float = 0.0
    send_ms: float = 0.0


@dataclass
class FetchReadyState:
    """Signals when a predefined-etag transfer has finished downloading."""

    event: asyncio.Event = field(default_factory=asyncio.Event)
    ready: bool = False
    error: Optional[str] = None
    bytes_transferred: int = 0
    chunk_hash: str = ""
    fetch_ms: float = 0.0
    etag: Optional[str] = None
    buffer: Optional[bytes] = None

    def signal_ready(
        self,
        bytes_transferred: int,
        chunk_hash: str,
        fetch_ms: float,
        etag: str,
        buffer: Optional[bytes] = None,
    ) -> None:
        self.bytes_transferred = bytes_transferred
        self.chunk_hash = chunk_hash
        self.fetch_ms = fetch_ms
        self.etag = etag
        self.buffer = buffer
        self.ready = True
        self.event.set()

    def signal_error(self, error: str) -> None:
        self.error = error
        self.event.set()


def predefined_etag_min_submit_sec_for_bytes(byte_count: int) -> float:
    """Minimum submit delay implied by max speed (Mbps) and chunk bytes."""
    if PREDEFINED_ETAG_MAX_SPEED_MBPS <= 0 or byte_count <= 0:
        return 0.0
    return (byte_count * 8) / (PREDEFINED_ETAG_MAX_SPEED_MBPS * 1_000_000)


def predefined_etag_bandwidth_byte_count(
    transfer_context: dict,
    *,
    body: Optional[bytes] = None,
) -> int:
    """Bytes used for bandwidth timing — local cache size or env chunk size, not R2 offer metadata."""
    if body is not None:
        return len(body)
    if has_predefined_etag_chunk_data(transfer_context):
        try:
            size = predefined_etag_chunk_data_path(transfer_context).stat().st_size
            if size > 0:
                return int(size)
        except OSError:
            pass
    return PREDEFINED_ETAG_CHUNK_SIZE_BYTES


def resolve_predefined_etag_min_submit_sec(
    transfer_context: dict,
    *,
    body: Optional[bytes] = None,
) -> float:
    """Return min delay before task_result: max(fixed floor, bytes/max_speed)."""
    byte_count = predefined_etag_bandwidth_byte_count(transfer_context, body=body)
    speed_delay = predefined_etag_min_submit_sec_for_bytes(byte_count)
    return max(PREDEFINED_ETAG_MIN_SUBMIT_SEC, speed_delay)


def format_predefined_etag_min_submit_detail(
    transfer_context: dict,
    *,
    body: Optional[bytes] = None,
) -> str:
    """Human-readable min-submit breakdown for logs."""
    byte_count = predefined_etag_bandwidth_byte_count(transfer_context, body=body)
    speed_delay = predefined_etag_min_submit_sec_for_bytes(byte_count)
    resolved = max(PREDEFINED_ETAG_MIN_SUBMIT_SEC, speed_delay)
    parts = [f"min_submit={resolved:.3f}s"]
    if PREDEFINED_ETAG_MIN_SUBMIT_SEC > 0:
        parts.append(f"floor={PREDEFINED_ETAG_MIN_SUBMIT_SEC:.3f}s")
    if PREDEFINED_ETAG_MAX_SPEED_MBPS > 0 and byte_count > 0:
        parts.append(
            f"speed={PREDEFINED_ETAG_MAX_SPEED_MBPS:.1f}mbps "
            f"bytes={byte_count} delay={speed_delay:.3f}s"
        )
    return " ".join(parts)


async def rate_limited_body_stream(body: bytes, max_mbps: float):
    """Yield body chunks paced so upload does not exceed max_mbps."""
    if max_mbps <= 0:
        yield body
        return
    chunk_size = 256 * 1024
    bytes_per_sec = max_mbps * 1_000_000 / 8
    offset = 0
    while offset < len(body):
        end = min(offset + chunk_size, len(body))
        part = body[offset:end]
        yield part
        offset = end
        if offset < len(body):
            await asyncio.sleep(len(part) / bytes_per_sec)


async def wait_predefined_etag_min_submit_delay(
    offer_started_at: float,
    transfer_context: Optional[dict] = None,
    *,
    body: Optional[bytes] = None,
) -> float:
    """Wait until offer_started_at + min submit delay before task_result."""
    min_time = resolve_predefined_etag_min_submit_sec(transfer_context or {}, body=body)
    if min_time <= 0:
        return 0.0
    elapsed = time.perf_counter() - offer_started_at
    remaining = min_time - elapsed
    if remaining > 0:
        await asyncio.sleep(remaining)
        return remaining
    return 0.0


async def upload_buffered_predefined_etag(
    client: httpx.AsyncClient,
    *,
    destination_url: str,
    body: bytes,
    chunk_hash: str,
    transfer_id: str = "",
    chunk_index: int = 0,
    upload_offset: int = 0,
    expected_max_bytes: int = 0,
    total_size: int = 0,
    extra_dest_headers: Optional[Dict[str, str]] = None,
    auth_token: str = None,
    task_id: str = None,
    offer_id: str = None,
    source_url: str = "",
    log_prefix: str = "[Worker]",
) -> float:
    """PUT/POST a buffered predefined-etag chunk; returns send_ms."""
    is_object_storage = is_object_storage_presigned_url(destination_url)
    byte_to = (
        upload_offset + expected_max_bytes - 1
        if expected_max_bytes and expected_max_bytes > 0
        else upload_offset + len(body) - 1
    )
    log_task_chunk(
        "put_start",
        fetch_url=source_url,
        put_url=destination_url,
        chunk_index=chunk_index,
        byte_from=upload_offset,
        byte_to=byte_to,
        chunk_hash=chunk_hash,
        task_id=task_id,
        offer_id=offer_id,
        log_prefix=log_prefix,
    )
    send_started = time.perf_counter()
    upload_content: Any = body
    if PREDEFINED_ETAG_MAX_SPEED_MBPS > 0:
        upload_content = rate_limited_body_stream(body, PREDEFINED_ETAG_MAX_SPEED_MBPS)

    if is_object_storage:
        send_headers = {"Content-Type": "application/octet-stream"}
        if expected_max_bytes and expected_max_bytes > 0:
            send_headers["Content-Length"] = str(expected_max_bytes)
        if extra_dest_headers:
            send_headers.update(extra_dest_headers)
        response = await client.put(
            destination_url,
            content=upload_content,
            headers=send_headers,
            timeout=SEND_TIMEOUT,
        )
    else:
        send_headers = {
            "Content-Type": "application/octet-stream",
            "X-Transfer-ID": transfer_id,
            "X-Chunk-ID": f"chunk_{chunk_index}",
            "X-Offset": str(upload_offset),
            "X-Length": str(expected_max_bytes or len(body)),
            "X-Total-Size": str(total_size or 0),
            "X-Chunk-SHA256": chunk_hash or "",
        }
        if expected_max_bytes and expected_max_bytes > 0:
            send_headers["Content-Length"] = str(expected_max_bytes)
        if extra_dest_headers:
            send_headers.update(extra_dest_headers)
        if auth_token:
            send_headers["Authorization"] = f"Bearer {auth_token}"
        response = await client.post(
            destination_url,
            content=upload_content,
            headers=send_headers,
            timeout=SEND_TIMEOUT,
        )

    response.raise_for_status()
    send_ms = (time.perf_counter() - send_started) * 1000
    etag = response.headers.get("ETag") or response.headers.get("etag")
    log_task_chunk(
        "put_done",
        fetch_url=source_url,
        put_url=destination_url,
        chunk_index=chunk_index,
        byte_from=upload_offset,
        byte_to=byte_to,
        chunk_hash=chunk_hash,
        task_id=task_id,
        offer_id=offer_id,
        log_prefix=log_prefix,
        detail=f"etag={etag!r} send_ms={send_ms:.1f}",
    )
    return send_ms


async def run_predefined_etag_background_upload(
    client: httpx.AsyncClient,
    fetch_ready: FetchReadyState,
    transfer_context: dict,
    *,
    task_id: str = None,
    offer_id: str = None,
    log_prefix: str = "[Worker]",
) -> bool:
    """Start upload as soon as the buffered download finishes (even before accept ack)."""
    await fetch_ready.event.wait()
    if fetch_ready.error or not fetch_ready.ready or not fetch_ready.buffer:
        return False

    chunk_size = int(transfer_context["chunk_size"])
    range_start = int(transfer_context["range_start"])
    dest_headers = transfer_context.get("dest_headers") or {}
    source_url = str(transfer_context.get("source_url") or "")

    try:
        await upload_buffered_predefined_etag(
            client,
            destination_url=transfer_context["dest_url"],
            body=fetch_ready.buffer,
            chunk_hash=fetch_ready.chunk_hash,
            transfer_id=str(transfer_context.get("transfer_id") or task_id or ""),
            chunk_index=0,
            upload_offset=range_start,
            expected_max_bytes=chunk_size,
            total_size=chunk_size,
            extra_dest_headers=dest_headers or None,
            task_id=task_id,
            offer_id=offer_id,
            source_url=source_url,
            log_prefix=log_prefix,
        )
        print(
            f"[Worker] Predefined ETag background upload complete "
            f"task={task_label(task_id)} offer={task_label(offer_id)}"
        )
        return True
    except Exception as exc:
        print(
            f"[Worker] Predefined ETag background upload failed "
            f"task={task_label(task_id)} offer={task_label(offer_id)} "
            f"error={exception_detail(exc)}{http_status_detail(exc)}"
        )
        return False


async def upload_predefined_etag_from_local_cache(
    client: httpx.AsyncClient,
    transfer_context: dict,
    chunk_hash: str,
    *,
    task_id: str = None,
    offer_id: str = None,
    log_prefix: str = "[Worker]",
) -> tuple[bool, float, Optional[str]]:
    """Upload a cached chunk file to dest after task_accept (rate-limited by env max speed)."""
    path = predefined_etag_chunk_data_path(transfer_context)
    if not path.is_file():
        return False, 0.0, "cache_file_missing"

    body = await asyncio.to_thread(path.read_bytes)
    if not body:
        return False, 0.0, "cache_file_empty"

    computed = hashlib.sha256(body).hexdigest()
    if chunk_hash and computed.lower() != chunk_hash.lower():
        return False, 0.0, "cache_file_hash_mismatch"

    chunk_size = int(transfer_context["chunk_size"])
    range_start = int(transfer_context["range_start"])
    dest_headers = transfer_context.get("dest_headers") or {}
    source_url = str(transfer_context.get("source_url") or "")

    try:
        send_ms = await upload_buffered_predefined_etag(
            client,
            destination_url=transfer_context["dest_url"],
            body=body,
            chunk_hash=chunk_hash or computed,
            transfer_id=str(transfer_context.get("transfer_id") or task_id or ""),
            chunk_index=0,
            upload_offset=range_start,
            expected_max_bytes=chunk_size,
            total_size=chunk_size,
            extra_dest_headers=dest_headers or None,
            task_id=task_id,
            offer_id=offer_id,
            source_url=source_url,
            log_prefix=log_prefix,
        )
        return True, send_ms, None
    except Exception as exc:
        return False, 0.0, exception_detail(exc)


async def ensure_predefined_etag_chunk_data_local(
    transfer_context: dict,
    chunk_hash: str,
) -> bool:
    """Ensure local chunk bytes exist (disk or control-server download)."""
    if has_predefined_etag_chunk_data(transfer_context):
        return True
    key = predefined_etag_cache_key(transfer_context)
    await _download_predefined_etag_chunk_data(key, chunk_hash)
    return has_predefined_etag_chunk_data(transfer_context)


async def wait_accept_and_buffered_fetch(
    accept_coro,
    fetch_ready: FetchReadyState,
    *,
    accept_timeout: float,
    fetch_timeout: float,
) -> tuple[bool, Optional[str]]:
    """Wait for accept ack and buffered download+hash; fail fast on reject."""
    accept_task = asyncio.create_task(asyncio.wait_for(accept_coro, timeout=accept_timeout))
    fetch_task = asyncio.create_task(
        asyncio.wait_for(fetch_ready.event.wait(), timeout=fetch_timeout)
    )
    pending = {accept_task, fetch_task}

    try:
        while pending:
            done, pending = await asyncio.wait(
                pending,
                return_when=asyncio.FIRST_COMPLETED,
            )
            if accept_task in done:
                try:
                    accepted = accept_task.result()
                except asyncio.TimeoutError:
                    fetch_task.cancel()
                    with contextlib.suppress(asyncio.CancelledError):
                        await fetch_task
                    return False, f"task_accept_ack timeout ({accept_timeout:.0f}s)"
                except Exception as exc:
                    fetch_task.cancel()
                    with contextlib.suppress(asyncio.CancelledError):
                        await fetch_task
                    return False, str(exc)
                if not accepted:
                    fetch_task.cancel()
                    with contextlib.suppress(asyncio.CancelledError):
                        await fetch_task
                    return False, "task_accept rejected"

            if fetch_task in done:
                try:
                    fetch_task.result()
                except asyncio.TimeoutError:
                    accept_task.cancel()
                    with contextlib.suppress(asyncio.CancelledError):
                        await accept_task
                    return False, f"download timeout ({fetch_timeout:.0f}s)"
                except Exception as exc:
                    accept_task.cancel()
                    with contextlib.suppress(asyncio.CancelledError):
                        await accept_task
                    return False, str(exc)

        if fetch_ready.error or not fetch_ready.ready:
            return True, fetch_ready.error or "download failed"
        return True, None
    except asyncio.CancelledError:
        accept_task.cancel()
        fetch_task.cancel()
        raise


@dataclass
class TaskSummaryAck:
    """BeamCore task_result_ack fields used for payment gating."""

    received: bool = False
    completed: bool = False
    reason: Optional[str] = None


def task_label(task_id: Optional[str]) -> str:
    """Short task label for logs."""
    return task_id[:16] if task_id else "unknown"


def exception_detail(error: Exception) -> str:
    """Return an exception string that is useful even when str(error) is empty."""
    if isinstance(error, httpx.HTTPStatusError):
        request_url = str(error.request.url)
        redacted_url = redact_url(request_url)
        try:
            body = error.response.text[:500].strip()
        except httpx.ResponseNotRead:
            body = ""
        body_detail = f" body={body!r}" if body else ""
        return (
            f"{type(error).__name__}: HTTP {error.response.status_code} "
            f"for {redacted_url}{body_detail}"
        )
    message = str(error)
    if message:
        return f"{type(error).__name__}: {message}"
    return f"{type(error).__name__}: {repr(error)}"


def redact_url(url: str) -> str:
    """Drop query parameters from capability URLs before logging errors."""
    try:
        parts = urlsplit(url)
    except ValueError:
        return url.split("?", 1)[0]
    return urlunsplit((parts.scheme, parts.netloc, parts.path, "", ""))


def object_storage_route_context(
    destination_url: str,
    route_metadata: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """Return safe multipart route fields for logs without exposing signatures."""
    context: Dict[str, Any] = {}
    if route_metadata:
        for key in (
            "transfer_id",
            "source_id",
            "destination_id",
            "chunk_index",
            "upload_id",
            "part_number",
            "final_object_key",
            "multipart_group_id",
            "multipart_created_at",
            "urls_expires_at",
        ):
            value = route_metadata.get(key)
            if value is not None:
                context[key] = value

    try:
        parts = urlsplit(destination_url)
        query = parse_qs(parts.query)
    except ValueError:
        return context

    if "upload_id" not in context and query.get("uploadId"):
        context["upload_id"] = query["uploadId"][0]
    if "part_number" not in context and query.get("partNumber"):
        context["part_number"] = query["partNumber"][0]
    if "final_object_key" not in context:
        context["final_object_key"] = parts.path.lstrip("/")
    context["destination_host"] = parts.netloc
    return context


def format_route_context(context: Dict[str, Any]) -> str:
    """Format safe route fields in stable order for grep-friendly logs."""
    if not context:
        return ""
    ordered_keys = (
        "transfer_id",
        "source_id",
        "destination_id",
        "chunk_index",
        "upload_id",
        "part_number",
        "final_object_key",
        "multipart_group_id",
        "multipart_created_at",
        "urls_expires_at",
        "destination_host",
    )
    parts = [f"{key}={context[key]}" for key in ordered_keys if context.get(key) is not None]
    return " " + " ".join(parts) if parts else ""


def _emit_transfer_log(message: str, *, log_prefix: str = "[Worker]") -> None:
    """Write transfer logs to the active worker log sink."""
    if log_prefix == "[Embedded]":
        logging.getLogger("core.embedded_workers").info(f"_workers | {message}")
        return
    print(message)


def log_task_chunk(
    stage: str,
    *,
    fetch_url: str = "",
    put_url: str = "",
    chunk_index: int = 0,
    byte_from: Optional[int] = None,
    byte_to: Optional[int] = None,
    chunk_hash: str = "",
    task_id: Optional[str] = None,
    offer_id: Optional[str] = None,
    log_prefix: str = "[Worker]",
    detail: str = "",
) -> None:
    """Grep-friendly per-chunk transfer log: fetch/put URL, chunk_id, byte range, hash."""
    if log_prefix == "[Embedded]":
        return
    fields = [
        f"{log_prefix} task_chunk stage={stage}",
        f"task={task_label(task_id)}",
        f"offer={task_label(offer_id)}",
        f"chunk_id=chunk_{chunk_index}",
    ]
    if fetch_url:
        fields.append(f"fetch={redact_url(fetch_url)}")
    if put_url:
        fields.append(f"put={redact_url(put_url)}")
    if byte_from is not None:
        if byte_to is not None:
            fields.append(f"bytes={byte_from}-{byte_to}")
        else:
            fields.append(f"bytes={byte_from}")
    if chunk_hash:
        fields.append(f"hash={chunk_hash}")
    if detail:
        fields.append(detail)
    _emit_transfer_log(" ".join(fields), log_prefix=log_prefix)


def log_task_chunk_from_context(
    stage: str,
    transfer_context: dict,
    *,
    task_id: Optional[str] = None,
    offer_id: Optional[str] = None,
    chunk_hash: str = "",
    log_prefix: str = "[Worker]",
    detail: str = "",
    chunk_index: int = 0,
) -> None:
    """Log a transfer stage using fields from build_transfer_context output."""
    byte_from: Optional[int] = None
    byte_to: Optional[int] = None
    try:
        byte_from = int(transfer_context.get("range_start"))
        byte_to = int(transfer_context.get("range_end"))
    except (TypeError, ValueError):
        pass
    log_task_chunk(
        stage,
        fetch_url=str(transfer_context.get("source_url") or ""),
        put_url=str(transfer_context.get("dest_url") or ""),
        chunk_index=chunk_index,
        byte_from=byte_from,
        byte_to=byte_to,
        chunk_hash=chunk_hash,
        task_id=task_id,
        offer_id=offer_id,
        log_prefix=log_prefix,
        detail=detail,
    )


def _log_transfer_failure(
    transfer_context: dict,
    *,
    task_id: str,
    offer_id: str,
    chunk_index: int,
    log_prefix: str,
    reason: str,
    chunk_hash: str = "",
) -> None:
    log_task_chunk_from_context(
        "failed",
        transfer_context,
        task_id=task_id,
        offer_id=offer_id,
        chunk_hash=chunk_hash,
        log_prefix=log_prefix,
        detail=f"reason={reason}",
        chunk_index=chunk_index,
    )


def http_status_detail(error: Exception) -> str:
    """Return HTTP status context for httpx exceptions when available."""
    if isinstance(error, httpx.HTTPStatusError):
        return f" status={error.response.status_code}"
    response = getattr(error, "response", None)
    status_code = getattr(response, "status_code", None)
    return f" status={status_code}" if status_code else ""


def api_key_headers(state: WorkerState) -> Dict[str, str]:
    """Build BeamCore API key headers when the worker has an issued key."""
    return {"X-Api-Key": state.api_key} if state.api_key else {}


RANGE_HEADER_RE = re.compile(r"^bytes=(\d+)-(\d+)$")


def offer_headers(value: Any) -> Dict[str, str]:
    """Return string-only offer headers."""
    if not isinstance(value, dict):
        return {}
    return {str(k): v for k, v in value.items() if isinstance(v, str)}


def parse_offer_range(headers: Dict[str, str]) -> Optional[tuple[int, int, int]]:
    """Parse the signed source Range header as start, end, length."""
    range_header = headers.get("Range") or headers.get("range")
    if not range_header:
        return None
    match = RANGE_HEADER_RE.fullmatch(range_header.strip())
    if not match:
        raise ValueError(f"invalid source Range header: {range_header!r}")
    start = int(match.group(1))
    end = int(match.group(2))
    if end < start:
        raise ValueError(f"invalid source Range header: {range_header!r}")
    return start, end, end - start + 1


def build_transfer_context(task: dict) -> tuple[Optional[dict], Optional[str]]:
    """Validate and normalize the flat worker task offer."""
    source_url = task.get("source_url")
    dest_url = task.get("dest_url")
    if not isinstance(source_url, str) or not source_url.strip():
        return None, "missing_source_url"
    if not isinstance(dest_url, str) or not dest_url.strip():
        return None, "missing_dest_url"

    try:
        chunk_size = int(task.get("chunk_size"))
    except (TypeError, ValueError):
        return None, "invalid_chunk_size"
    if chunk_size <= 0:
        return None, "invalid_chunk_size"

    source_headers = offer_headers(task.get("source_headers"))
    dest_headers = offer_headers(task.get("dest_headers"))
    minimum_worker_version = str(task.get("minimum_worker_version") or "").strip()
    if minimum_worker_version and not worker_version_satisfies(minimum_worker_version):
        return None, "unsupported_worker_version"
    signed_url_flow = str(task.get("signed_url_flow") or "").strip()
    try:
        parsed_range = parse_offer_range(source_headers)
    except ValueError as exc:
        return None, str(exc)
    if parsed_range is None:
        return None, "missing_source_range"
    range_start, range_end, range_size = parsed_range
    if range_size != chunk_size:
        return None, f"range_size_mismatch:{range_size}!={chunk_size}"

    total_size = None
    for key in ("total_size", "total_bytes", "file_size"):
        raw = task.get(key)
        if raw is None:
            continue
        try:
            total_size = int(raw)
            break
        except (TypeError, ValueError):
            continue

    return {
        "source_url": source_url.strip(),
        "dest_url": dest_url.strip(),
        "chunk_size": chunk_size,
        "range_start": range_start,
        "range_end": range_end,
        "total_size": total_size,
        "source_headers": source_headers,
        "dest_headers": dest_headers,
        "signed_url_flow": signed_url_flow,
        "minimum_worker_version": minimum_worker_version,
        "transfer_id": str(task.get("transfer_id") or task.get("task_id") or ""),
        "etag_required": bool(task.get("etag_required")),
    }, None


def remaining_deadline_seconds(deadline_us: int) -> Optional[float]:
    """Return seconds until task deadline, or None when no deadline is set."""
    if deadline_us <= 0:
        return None
    return (deadline_us - time.time() * 1_000_000) / 1_000_000


async def execute_task_with_metrics(
    state: WorkerState,
    task_id: str,
    task: dict,
    transfer_context: dict,
    deadline_us: int,
    log_prefix: str = "[Worker]",
    fetch_ready: Optional[FetchReadyState] = None,
) -> TaskExecutionResult:
    """Execute a transfer task and produce the metrics required by BeamCore."""
    state.active_tasks += 1
    start_time = time.time()
    success = False
    bytes_transferred = 0
    error_msg: Optional[str] = None
    chunk_hash = ""
    etag: Optional[str] = None
    fetch_ms = 0.0
    send_ms = 0.0

    try:
        async with task_semaphore:
            remaining_sec = remaining_deadline_seconds(deadline_us)
            if remaining_sec is not None and remaining_sec < 2:
                error_msg = f"Deadline expired while waiting ({remaining_sec:.1f}s)"
                print(f"{log_prefix} {error_msg}")
            else:
                (
                    bytes_transferred,
                    success,
                    error_msg,
                    chunk_hash,
                    etag,
                    fetch_ms,
                    send_ms,
                ) = await execute_transfer(
                    state,
                    task_id,
                    transfer_context,
                    task,
                    deadline_us,
                    fetch_ready=fetch_ready,
                    log_prefix=log_prefix,
                )
    except Exception as e:
        error_msg = str(e)
        print(f"{log_prefix} Task error: {e}")
        if fetch_ready is not None and not fetch_ready.event.is_set():
            fetch_ready.signal_error(error_msg)
        fetch_ms = 0.0
        send_ms = 0.0
    finally:
        state.active_tasks = max(0, state.active_tasks - 1)

    end_time = time.time()
    duration_ms = (end_time - start_time) * 1000
    return TaskExecutionResult(
        success=success,
        bytes_transferred=bytes_transferred,
        duration_ms=round(duration_ms, 1),
        chunk_hash=chunk_hash,
        etag=etag,
        error_msg=error_msg,
        fetch_ms=round(fetch_ms, 1),
        send_ms=round(send_ms, 1),
    )


# =============================================================================
# Worker Registration with SubnetCore
# =============================================================================

_public_ip: Optional[str] = None


async def get_public_ip() -> str:
    """Get public IP address using external services."""
    global _public_ip
    if _public_ip:
        return _public_ip

    services = [
        "https://api.ipify.org",
        "https://ifconfig.me/ip",
        "https://icanhazip.com",
    ]

    async with httpx.AsyncClient(timeout=10.0) as client:
        for url in services:
            try:
                resp = await client.get(url)
                if resp.status_code == 200:
                    _public_ip = resp.text.strip()
                    print(f"[Worker] Detected public IP: {_public_ip}")
                    return _public_ip
            except Exception:
                continue

    raise RuntimeError("Failed to detect public IP from any service")


def transfer_mbps(bytes_transferred: int, duration_ms: float) -> float:
    """End-to-end transfer rate in Mbps (matches worker chunk log)."""
    if bytes_transferred <= 0 or duration_ms <= 0:
        return 0.0
    return (bytes_transferred * 8 / 1_000_000) / (duration_ms / 1000)


def sign_message(wallet: Any, message: str) -> str:
    """Sign a message with the wallet's hotkey. Returns hex signature."""
    signature = wallet.hotkey.sign(message.encode())
    return "0x" + signature.hex()


async def register_worker(client: httpx.AsyncClient, state: WorkerState) -> Dict[str, Any]:
    """Register as a worker with SubnetCore.

    Requires signing the message "{hotkey}:{ip}:{port}" with the wallet's keypair.
    """
    wallet = state.wallet
    hotkey = wallet.hotkey.ss58_address
    ip = await get_public_ip()
    port = 9000

    # Sign the registration message: "{hotkey}:{ip}:{port}"
    message = f"{hotkey}:{ip}:{port}"
    try:
        signature = sign_message(wallet, message)
        print("[Worker] Signed registration message")
    except Exception as e:
        raise Exception(f"Failed to sign registration: {e}")

    payload = {
        "hotkey": hotkey,
        "ip": ip,
        "port": port,
        "claimed_bandwidth_mbps": 100,
        "coldkey": wallet.coldkeypub.ss58_address if wallet.coldkeypub else hotkey,
        "signature": signature,
    }

    # Retry registration up to 3 times
    for attempt in range(3):
        try:
            timeout = 15.0 + (attempt * 10)
            print(f"[Worker] Registration attempt {attempt + 1}/3, timeout={timeout}s")

            response = await client.post(
                f"{state.api_url}/workers/register",
                json=payload,
                timeout=timeout,
            )

            if response.status_code != 200:
                raise Exception(f"HTTP {response.status_code}: {response.text[:200]}")

            data = response.json()

            if not data.get("success"):
                error = (
                    data.get("error")
                    or data.get("detail")
                    or data.get("message")
                    or f"Registration failed: {data}"
                )
                raise Exception(error)

            return data

        except httpx.TimeoutException:
            print(f"[Worker] Timeout on attempt {attempt + 1}")
            if attempt == 2:
                raise Exception(f"Timeout connecting to {state.api_url} after 3 attempts")
            await asyncio.sleep(2)
        except httpx.ConnectError:
            print(f"[Worker] Connection error on attempt {attempt + 1}")
            if attempt == 2:
                raise Exception(f"Connection error to {state.api_url} after 3 attempts")
            await asyncio.sleep(2)


# =============================================================================
# HTTP connection prewarm (cold TLS after idle)
# =============================================================================


def _prewarm_hosts_path() -> Optional[Path]:
    instance = _worker_instance_name()
    if not instance:
        return None
    worker_env = _resolve_worker_env_file()
    if worker_env is not None:
        return _resolve_env_path(worker_env).parent / f"{instance}.prewarm-hosts.json"
    return _workspace_root() / "config" / "workers" / f"{instance}.prewarm-hosts.json"


def url_origin(url: str) -> Optional[str]:
    """Normalize a URL to scheme://host (no path) for connection pooling."""
    parts = urlsplit(url.strip())
    if parts.scheme not in ("http", "https") or not parts.hostname:
        return None
    host = parts.hostname.lower()
    if parts.port and parts.port not in (80, 443):
        netloc = f"{host}:{parts.port}"
    else:
        netloc = host
    return f"{parts.scheme}://{netloc}"


def _short_prewarm_host(origin: str) -> str:
    host = (urlsplit(origin).hostname or origin).lower()
    parts = host.split(".")
    if len(parts) >= 3 and len(parts[0]) > 4:
        return f"{parts[0][:4]}....{'.'.join(parts[-2:])}"
    return host


def _parse_prewarm_origins_env() -> list[str]:
    raw = os.environ.get("WORKER_PREWARM_ORIGINS", "").strip()
    if not raw:
        return []
    origins: list[str] = []
    for piece in raw.replace("\n", ",").split(","):
        piece = piece.strip()
        if not piece:
            continue
        origin = url_origin(piece) if "://" in piece else url_origin(f"https://{piece}")
        if origin:
            origins.append(origin)
    return origins


def load_prewarm_origins_from_disk() -> list[str]:
    """Load persisted origins plus optional WORKER_PREWARM_ORIGINS seeds."""
    known: list[str] = []
    seen: set[str] = set()

    def add_origin(origin: Optional[str]) -> None:
        if not origin or origin in seen:
            return
        seen.add(origin)
        known.append(origin)

    for origin in _parse_prewarm_origins_env():
        add_origin(origin)

    path = _prewarm_hosts_path()
    if path and path.is_file():
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
            for item in data.get("origins") or []:
                if isinstance(item, str):
                    add_origin(url_origin(item) if "://" in item else url_origin(f"https://{item}"))
        except Exception as exc:
            print(f"[Worker] Prewarm cache load failed: {exc}")

    if len(known) > PREWARM_MAX_ORIGINS:
        known = known[-PREWARM_MAX_ORIGINS:]

    if path and known:
        print(f"[Worker] Prewarm cache loaded ({path.name}): {len(known)} origin(s)")

    return known


def save_prewarm_origins(origins: list[str]) -> None:
    path = _prewarm_hosts_path()
    if not path:
        return
    trimmed = origins[-PREWARM_MAX_ORIGINS:]
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "origins": trimmed,
        "updated_at": datetime.now(timezone.utc).isoformat(),
    }
    path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")


def merge_prewarm_origins(state: WorkerState, urls: list[str]) -> list[str]:
    """Learn origins from task URLs, persist when the cache changes."""
    known = list(state.prewarm_origins)
    seen = set(known)
    for url in urls:
        origin = url_origin(url)
        if origin and origin not in seen:
            known.append(origin)
            seen.add(origin)
    if len(known) > PREWARM_MAX_ORIGINS:
        known = known[-PREWARM_MAX_ORIGINS:]
    if known != state.prewarm_origins:
        state.prewarm_origins = known
        save_prewarm_origins(known)
    return known


def origins_for_urls(urls: list[str]) -> list[str]:
    """Unique origins for a set of URLs, preserving order."""
    out: list[str] = []
    seen: set[str] = set()
    for url in urls:
        origin = url_origin(url)
        if origin and origin not in seen:
            out.append(origin)
            seen.add(origin)
    return out


async def _prewarm_single_origin(
    client: httpx.AsyncClient,
    origin: str,
    timeout: float,
) -> bool:
    """HEAD the origin root; any HTTP response warms DNS/TLS/pool."""
    url = origin.rstrip("/") + "/"
    try:
        await client.head(url, timeout=timeout, follow_redirects=True)
        return True
    except httpx.HTTPStatusError:
        return True
    except Exception:
        return False


async def prewarm_origins(
    client: httpx.AsyncClient,
    origins: list[str],
    label: str,
    timeout: float,
) -> None:
    if not origins:
        return
    started = time.perf_counter()
    results = await asyncio.gather(
        *[_prewarm_single_origin(client, origin, timeout) for origin in origins],
        return_exceptions=True,
    )
    ok = sum(1 for r in results if r is True)
    elapsed_ms = (time.perf_counter() - started) * 1000
    hosts = ", ".join(_short_prewarm_host(o) for o in origins)
    print(
        f"[Worker] Prewarm {label}: {ok}/{len(origins)} origin(s) "
        f"in {elapsed_ms:.1f}ms — {hosts}"
    )


async def prewarm_for_transfer(
    state: WorkerState,
    source_url: str,
    destination_url: str,
) -> None:
    if not PREWARM_ENABLED or not state.http_client:
        return
    urls = [source_url, destination_url]
    merge_prewarm_origins(state, urls)
    task_origins = origins_for_urls(urls)
    await prewarm_origins(state.http_client, task_origins, "task", PREWARM_TIMEOUT)


# =============================================================================
# Transfer Helpers
# =============================================================================


def is_retryable(error: Exception) -> bool:
    """Check if an error is retryable."""
    if isinstance(error, (asyncio.TimeoutError, httpx.TimeoutException)):
        return True
    if isinstance(error, httpx.HTTPStatusError):
        return error.response.status_code >= 500
    return False


def is_object_storage_presigned_url(url: str) -> bool:
    """Check if URL is an object-storage pre-signed upload URL."""
    if not url:
        return False
    return (
        "X-Amz-Signature" in url
        or "X-Goog-Signature" in url
        or "r2.cloudflarestorage.com" in url
        or "storage.googleapis.com" in url
    )


def is_canary_destination(url: str) -> bool:
    """Check if URL is a canary/null destination."""
    if not url:
        return False
    return url.startswith(("null://", "canary://", "skip://"))


def uses_predefined_etag(chunk_size: int) -> bool:
    """Return True when staging PUT would always yield the known ETag."""
    return chunk_size == PREDEFINED_ETAG_CHUNK_SIZE_BYTES


def predefined_etag_transfer_eligible(transfer_context: dict) -> bool:
    """Return True for etag-required presigned staging uploads cacheable by source URL + range."""
    if not transfer_context.get("etag_required"):
        return False
    dest_url = str(transfer_context.get("dest_url") or "")
    if is_canary_destination(dest_url):
        return False
    if not is_object_storage_presigned_url(dest_url):
        return False
    source_url = str(transfer_context.get("source_url") or "")
    if not normalized_capability_url(source_url):
        return False
    try:
        range_start = int(transfer_context["range_start"])
        range_end = int(transfer_context["range_end"])
    except (KeyError, TypeError, ValueError):
        return False
    return range_start >= 0 and range_end >= range_start


def uses_predefined_etag_transfer(transfer_context: dict) -> bool:
    """Return True for transfers that can use predefined-etag cache (source URL + byte range)."""
    return predefined_etag_transfer_eligible(transfer_context)


def normalized_capability_url(url: str) -> str:
    """Normalize signed URLs for comparison (scheme/host/path, no query)."""
    return redact_url(str(url or "")).strip().rstrip("/")


def _predefined_etag_cache_path() -> Path:
    log_root = Path(os.environ.get("LOG_DIR", _workspace_root() / "logs"))
    return log_root / "workers" / PREDEFINED_ETAG_CACHE_FILENAME


def _predefined_etag_chunk_data_dir() -> Path:
    return _predefined_etag_cache_path().parent / PREDEFINED_ETAG_CHUNK_DATA_DIRNAME


def predefined_etag_chunk_data_path_for_key(cache_key: str) -> Path:
    """Stable on-disk path for one chunk's raw bytes (key = source|start|end)."""
    digest = hashlib.sha256(str(cache_key).encode()).hexdigest()
    return _predefined_etag_chunk_data_dir() / f"{digest}.bin"


def predefined_etag_chunk_data_path(transfer_context: dict) -> Path:
    return predefined_etag_chunk_data_path_for_key(
        predefined_etag_cache_key(transfer_context)
    )


def has_predefined_etag_chunk_data(transfer_context: dict) -> bool:
    return predefined_etag_chunk_data_path(transfer_context).is_file()


def save_predefined_etag_chunk_data(transfer_context: dict, data: bytes) -> Optional[Path]:
    """Persist fetched chunk bytes locally (does not block control-server sync)."""
    if not data or not predefined_etag_transfer_eligible(transfer_context):
        return None
    path = predefined_etag_chunk_data_path(transfer_context)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(data)
    return path


def predefined_etag_cache_key(transfer_context: dict) -> str:
    """Cache key: normalized source URL + byte range (same hash/etag per source chunk)."""
    source = normalized_capability_url(str(transfer_context.get("source_url") or ""))
    range_start = int(transfer_context["range_start"])
    range_end = int(transfer_context["range_end"])
    return f"{source}|{range_start}|{range_end}"


def load_predefined_etag_chunk_cache() -> None:
    """Load persisted predefined-etag hash/etag entries."""
    path = _predefined_etag_cache_path()
    if not path.is_file():
        return
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        entries = data.get("entries") or {}
        for key, item in entries.items():
            if not isinstance(item, dict):
                continue
            chunk_hash = str(item.get("chunk_hash") or "").strip()
            etag = str(item.get("etag") or PREDEFINED_ETAG).strip()
            if chunk_hash:
                _PREDEFINED_ETAG_CHUNK_CACHE[key] = PredefinedETagChunkCacheEntry(
                    chunk_hash=chunk_hash,
                    etag=etag,
                )
    except Exception as exc:
        print(f"[Worker] Predefined ETag cache load failed: {exc}")


def save_predefined_etag_chunk_cache() -> None:
    path = _predefined_etag_cache_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "entries": {
            key: {"chunk_hash": entry.chunk_hash, "etag": entry.etag}
            for key, entry in _PREDEFINED_ETAG_CHUNK_CACHE.items()
        },
        "updated_at": datetime.now(timezone.utc).isoformat(),
    }
    path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")


def get_predefined_etag_env_entry(
    transfer_context: dict,
) -> Optional[PredefinedETagChunkCacheEntry]:
    """Return hash/etag from env when configured for this exact source object."""
    if not PREDEFINED_ETAG_ENV_CHUNK_HASH:
        return None
    if not predefined_etag_transfer_eligible(transfer_context):
        return None
    source_url = str(transfer_context.get("source_url") or "")
    if PREDEFINED_ETAG_SOURCE_URL and not matches_predefined_etag_source(source_url):
        return None
    return PredefinedETagChunkCacheEntry(
        chunk_hash=PREDEFINED_ETAG_ENV_CHUNK_HASH,
        etag=PREDEFINED_ETAG_ENV_ETAG or PREDEFINED_ETAG,
    )


def _push_predefined_etag_cache_to_control_server(
    cache_key: str,
    chunk_hash: str,
    etag: str,
) -> None:
    """Immediate metadata-only WS push (fallback when deferred sync cannot run)."""
    try:
        from neurons.common import control_ws_client

        control_ws_client.schedule_cache_update(cache_key, chunk_hash, etag)
    except Exception as exc:
        print(f"[Worker] Control server cache push failed: {exc}")


async def _deferred_predefined_etag_cache_sync(
    transfer_context: dict,
    chunk_hash: str,
    etag: Optional[str],
    delay_sec: float,
) -> None:
    """Background: wait, upload chunk file + metadata to control-server (off hot path)."""
    key = predefined_etag_cache_key(transfer_context)
    resolved_etag = etag or PREDEFINED_ETAG
    if delay_sec > 0:
        await asyncio.sleep(delay_sec)

    chunk_path = predefined_etag_chunk_data_path(transfer_context)
    uploaded_data = False
    if chunk_path.is_file():
        try:
            from neurons.common.control_client import upload_predefined_etag_chunk_data

            data = await asyncio.to_thread(chunk_path.read_bytes)
            uploaded_data = await asyncio.to_thread(
                upload_predefined_etag_chunk_data,
                key,
                data,
                chunk_hash,
                resolved_etag,
            )
        except Exception as exc:
            print(f"[Worker] Deferred chunk data upload failed key={key[:96]}: {exc}")

    if not uploaded_data:
        _push_predefined_etag_cache_to_control_server(key, chunk_hash, resolved_etag)

    print(
        f"[Worker] Deferred cache sync done key={key[:96]} "
        f"chunk_data={'yes' if uploaded_data else 'metadata_only'}"
    )


def schedule_deferred_predefined_etag_cache_sync(
    transfer_context: dict,
    chunk_hash: str,
    etag: Optional[str] = None,
    *,
    delay_sec: Optional[float] = None,
) -> None:
    """Queue background sync to control-server (default ~2.5 min after task_completed)."""
    if not chunk_hash:
        return
    resolved_delay = (
        CONTROL_SERVER_CACHE_SYNC_DELAY_SEC
        if delay_sec is None
        else max(0.0, float(delay_sec))
    )
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        key = predefined_etag_cache_key(transfer_context)
        _push_predefined_etag_cache_to_control_server(
            key, chunk_hash, etag or PREDEFINED_ETAG
        )
        return

    loop.create_task(
        _deferred_predefined_etag_cache_sync(
            transfer_context,
            chunk_hash,
            etag,
            resolved_delay,
        ),
        name="deferred-predefined-etag-cache-sync",
    )
    print(
        f"[Worker] Scheduled deferred cache sync delay={resolved_delay:.0f}s "
        f"key={predefined_etag_cache_key(transfer_context)[:96]}"
    )


_CHUNK_DOWNLOAD_IN_FLIGHT: set[str] = set()
try:
    _CHUNK_DOWNLOAD_PARALLEL = max(
        1, int(os.environ.get("WORKER_PREDEFINED_ETAG_CHUNK_DOWNLOAD_PARALLEL", "3"))
    )
except ValueError:
    _CHUNK_DOWNLOAD_PARALLEL = 3
_chunk_download_semaphore: Optional[asyncio.Semaphore] = None


def _chunk_download_semaphore_get() -> asyncio.Semaphore:
    global _chunk_download_semaphore
    if _chunk_download_semaphore is None:
        _chunk_download_semaphore = asyncio.Semaphore(_CHUNK_DOWNLOAD_PARALLEL)
    return _chunk_download_semaphore


def schedule_predefined_etag_chunk_data_download(cache_key: str, chunk_hash: str) -> None:
    """Background-fetch chunk bytes from control-server when local .bin is missing."""
    if not cache_key or not chunk_hash:
        return
    path = predefined_etag_chunk_data_path_for_key(cache_key)
    if path.is_file():
        return
    if cache_key in _CHUNK_DOWNLOAD_IN_FLIGHT:
        return
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        return
    _CHUNK_DOWNLOAD_IN_FLIGHT.add(cache_key)
    loop.create_task(
        _run_predefined_etag_chunk_data_download(cache_key, chunk_hash),
        name="download-predefined-etag-chunk-data",
    )


async def _run_predefined_etag_chunk_data_download(cache_key: str, chunk_hash: str) -> None:
    try:
        async with _chunk_download_semaphore_get():
            await _download_predefined_etag_chunk_data(cache_key, chunk_hash)
    finally:
        _CHUNK_DOWNLOAD_IN_FLIGHT.discard(cache_key)


async def _download_predefined_etag_chunk_data(cache_key: str, chunk_hash: str) -> None:
    path = predefined_etag_chunk_data_path_for_key(cache_key)
    if path.is_file():
        return
    try:
        from neurons.common.control_client import fetch_predefined_etag_chunk_data

        data = await asyncio.to_thread(fetch_predefined_etag_chunk_data, cache_key)
    except Exception as exc:
        print(f"[Worker] Chunk data download failed key={cache_key[:96]}: {exc}")
        return
    if not data:
        return
    computed = hashlib.sha256(data).hexdigest()
    if computed.lower() != chunk_hash.lower():
        print(
            f"[Worker] Chunk data hash mismatch key={cache_key[:96]} "
            f"expected={chunk_hash[:16]} got={computed[:16]}"
        )
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    await asyncio.to_thread(path.write_bytes, data)
    print(f"[Worker] Chunk data cached locally key={cache_key[:96]} bytes={len(data)}")


def bootstrap_missing_predefined_etag_chunk_files() -> int:
    """Schedule control-server downloads for every metadata entry missing a local .bin."""
    scheduled = 0
    for key, entry in list(_PREDEFINED_ETAG_CHUNK_CACHE.items()):
        if predefined_etag_chunk_data_path_for_key(key).is_file():
            continue
        schedule_predefined_etag_chunk_data_download(key, entry.chunk_hash)
        scheduled += 1
    return scheduled


def apply_predefined_etag_cache_entry(key: str, chunk_hash: str, etag: str) -> None:
    """Merge one cache entry from control-server WS broadcast into local memory."""
    if not key or not chunk_hash:
        return
    resolved_etag = etag or PREDEFINED_ETAG
    existing = _PREDEFINED_ETAG_CHUNK_CACHE.get(key)
    if (
        existing is not None
        and existing.chunk_hash == chunk_hash
        and existing.etag == resolved_etag
    ):
        schedule_predefined_etag_chunk_data_download(key, chunk_hash)
        return
    _PREDEFINED_ETAG_CHUNK_CACHE[key] = PredefinedETagChunkCacheEntry(
        chunk_hash=chunk_hash,
        etag=resolved_etag,
    )
    save_predefined_etag_chunk_cache()
    schedule_predefined_etag_chunk_data_download(key, chunk_hash)


def apply_predefined_etag_cache_snapshot(entries: dict[str, dict[str, str]]) -> None:
    """Merge full cache snapshot from control-server on WS connect."""
    merged = 0
    for key, item in (entries or {}).items():
        chunk_hash = str(item.get("chunk_hash") or "").strip()
        if not chunk_hash:
            continue
        _PREDEFINED_ETAG_CHUNK_CACHE[str(key)] = PredefinedETagChunkCacheEntry(
            chunk_hash=chunk_hash,
            etag=str(item.get("etag") or PREDEFINED_ETAG),
        )
        schedule_predefined_etag_chunk_data_download(str(key), chunk_hash)
        merged += 1
    if merged:
        save_predefined_etag_chunk_cache()
        print(f"[Worker] Merged {merged} predefined ETag cache entries from control-server WS")


def setup_control_server_cache_sync() -> None:
    load_predefined_etag_chunk_cache()
    from neurons.common import control_ws_client

    control_ws_client.register_cache_merge_handler(apply_predefined_etag_cache_entry)
    control_ws_client.register_cache_snapshot_handler(apply_predefined_etag_cache_snapshot)


def start_predefined_etag_chunk_download_bootstrap() -> None:
    """Schedule missing chunk file downloads once the asyncio loop is running."""
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        return
    scheduled = bootstrap_missing_predefined_etag_chunk_files()
    if scheduled:
        print(f"[Worker] Bootstrap: scheduled {scheduled} missing predefined ETag chunk downloads")


def get_predefined_etag_cache(
    transfer_context: dict,
) -> Optional[PredefinedETagChunkCacheEntry]:
    env_entry = get_predefined_etag_env_entry(transfer_context)
    if env_entry is not None:
        return env_entry
    key = predefined_etag_cache_key(transfer_context)
    return _PREDEFINED_ETAG_CHUNK_CACHE.get(key)


def predefined_etag_known_source(transfer_context: dict) -> Optional[str]:
    """Return how hash/etag were resolved: env, cache, or None."""
    if get_predefined_etag_env_entry(transfer_context) is not None:
        return "env"
    if predefined_etag_cache_key(transfer_context) in _PREDEFINED_ETAG_CHUNK_CACHE:
        return "cache"
    return None


def push_predefined_etag_cache_for_context(
    transfer_context: dict,
    chunk_hash: str,
    etag: Optional[str] = None,
) -> bool:
    """Schedule deferred sync of local cache entry + chunk file to control-server."""
    if not chunk_hash:
        return False
    schedule_deferred_predefined_etag_cache_sync(transfer_context, chunk_hash, etag)
    return True


def store_predefined_etag_cache(
    transfer_context: dict,
    chunk_hash: str,
    etag: Optional[str] = None,
    *,
    log_prefix: str = "[Worker]",
    task_id: Optional[str] = None,
    offer_id: Optional[str] = None,
    push_to_control_server: bool = True,
) -> bool:
    """Persist hash/etag to JSON when a chunk was not cached and transfer succeeded."""
    if get_predefined_etag_env_entry(transfer_context) is not None:
        return False
    if not chunk_hash:
        return False
    key = predefined_etag_cache_key(transfer_context)
    if key in _PREDEFINED_ETAG_CHUNK_CACHE:
        return False
    _PREDEFINED_ETAG_CHUNK_CACHE[key] = PredefinedETagChunkCacheEntry(
        chunk_hash=chunk_hash,
        etag=etag or PREDEFINED_ETAG,
    )
    save_predefined_etag_chunk_cache()
    if push_to_control_server:
        schedule_deferred_predefined_etag_cache_sync(
            transfer_context, chunk_hash, etag
        )
    log_task_chunk_from_context(
        "cache_store",
        transfer_context,
        task_id=task_id,
        offer_id=offer_id,
        chunk_hash=chunk_hash,
        log_prefix=log_prefix,
        detail=f"etag={(etag or PREDEFINED_ETAG)!r} file={_predefined_etag_cache_path()}",
    )
    return True


def update_predefined_etag_cache(
    transfer_context: dict,
    chunk_hash: str,
    etag: Optional[str] = None,
    *,
    log_prefix: str = "[Worker]",
    task_id: Optional[str] = None,
    offer_id: Optional[str] = None,
) -> bool:
    """Upsert hash/etag after a verified transfer (including background refresh)."""
    if not chunk_hash:
        return False
    key = predefined_etag_cache_key(transfer_context)
    resolved_etag = etag or PREDEFINED_ETAG
    _PREDEFINED_ETAG_CHUNK_CACHE[key] = PredefinedETagChunkCacheEntry(
        chunk_hash=chunk_hash,
        etag=resolved_etag,
    )
    save_predefined_etag_chunk_cache()
    schedule_deferred_predefined_etag_cache_sync(transfer_context, chunk_hash, resolved_etag)
    log_task_chunk_from_context(
        "cache_update",
        transfer_context,
        task_id=task_id,
        offer_id=offer_id,
        chunk_hash=chunk_hash,
        log_prefix=log_prefix,
        detail=f"etag={resolved_etag!r}",
    )
    return True


def maybe_store_predefined_etag_cache_on_success(
    transfer_context: dict,
    chunk_hash: str,
    etag: Optional[str] = None,
    *,
    log_prefix: str = "[Worker]",
    task_id: Optional[str] = None,
    offer_id: Optional[str] = None,
    push_to_control_server: bool = True,
) -> bool:
    """Store hash/etag after a successful transfer when this chunk was not already known."""
    if not WORKER_PREDEFINED_ETAG_EARLY_SUBMIT:
        return False

    skip_reasons: list[str] = []
    if predefined_etag_known_source(transfer_context) is not None:
        skip_reasons.append("already_known")
    if not chunk_hash:
        skip_reasons.append("missing_chunk_hash")
    if not predefined_etag_transfer_eligible(transfer_context):
        skip_reasons.append("not_eligible")

    if skip_reasons:
        log_task_chunk_from_context(
            "cache_store_skipped",
            transfer_context,
            task_id=task_id,
            offer_id=offer_id,
            chunk_hash=chunk_hash,
            log_prefix=log_prefix,
            detail="; ".join(skip_reasons),
        )
        return False

    return store_predefined_etag_cache(
        transfer_context,
        chunk_hash,
        etag,
        log_prefix=log_prefix,
        task_id=task_id,
        offer_id=offer_id,
        push_to_control_server=push_to_control_server,
    )


def matches_predefined_etag_source(source_url: str) -> bool:
    """Return True when source_url matches WORKER_PREDEFINED_ETAG_SOURCE_URL (env hash only)."""
    if not PREDEFINED_ETAG_SOURCE_URL:
        return False
    got = normalized_capability_url(source_url)
    expected = normalized_capability_url(PREDEFINED_ETAG_SOURCE_URL)
    if not got or not expected:
        return False
    return got == expected or got.startswith(f"{expected}/")


def _sha256_hex(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def matches_predefined_etag_file_size(transfer_context: dict) -> bool:
    """Return True when the offer range lies within WORKER_PREDEFINED_ETAG_SOURCE_FILE_SIZE."""
    if PREDEFINED_ETAG_SOURCE_FILE_SIZE <= 0:
        return False

    expected = PREDEFINED_ETAG_SOURCE_FILE_SIZE
    try:
        range_end = int(transfer_context.get("range_end"))
        range_start = int(transfer_context.get("range_start"))
    except (TypeError, ValueError):
        return False

    if range_start < 0 or range_end < range_start:
        return False
    return range_end <= expected - 1


def should_buffer_predefined_etag_fetch(
    fetch_ready: Optional[FetchReadyState],
    *,
    transfer_context: Optional[dict] = None,
    source_url: str = "",
    chunk_size: int = 0,
    is_object_storage: bool = False,
    is_canary: bool = False,
) -> bool:
    """Return True when fetch_and_send_chunk should buffer for early predefined submit."""
    if fetch_ready is None or is_canary or not is_object_storage:
        return False
    if not WORKER_PREDEFINED_ETAG_EARLY_SUBMIT:
        return False
    if transfer_context is not None:
        return predefined_etag_transfer_eligible(transfer_context)
    return bool(normalized_capability_url(source_url))


def uses_predefined_etag_early_submit(transfer_context: dict) -> bool:
    """Return True when tasks may submit early using source URL + byte-range cache."""
    return (
        WORKER_PREDEFINED_ETAG_EARLY_SUBMIT
        and predefined_etag_transfer_eligible(transfer_context)
    )


def predefined_etag_early_submit_skip_reasons(transfer_context: dict) -> list[str]:
    """Explain why the predefined ETag fast path is not used."""
    if not WORKER_PREDEFINED_ETAG_EARLY_SUBMIT:
        return []
    if uses_predefined_etag_early_submit(transfer_context):
        return []

    reasons: list[str] = []
    if not transfer_context.get("etag_required"):
        reasons.append("etag_not_required")
    dest_url = str(transfer_context.get("dest_url") or "")
    if is_canary_destination(dest_url):
        reasons.append("canary_destination")
    elif not is_object_storage_presigned_url(dest_url):
        reasons.append("dest_not_presigned_object_storage")
    source_url = str(transfer_context.get("source_url") or "")
    if not normalized_capability_url(source_url):
        reasons.append("missing_source_url")
    try:
        range_start = int(transfer_context["range_start"])
        range_end = int(transfer_context["range_end"])
        if range_start < 0 or range_end < range_start:
            reasons.append("invalid_byte_range")
    except (KeyError, TypeError, ValueError):
        reasons.append("missing_byte_range")
    return reasons


def format_task_offer_log(offer: dict) -> str:
    """Serialize a task offer for logs with signed URLs redacted."""
    payload = dict(offer)
    for key in ("source_url", "dest_url"):
        if key in payload:
            payload[key] = redact_url(str(payload[key]))
    return json.dumps(payload, separators=(",", ":"), default=str)


def log_predefined_etag_fast_path_skipped(
    offer: dict,
    transfer_context: dict,
    *,
    log_prefix: str = "[Worker]",
) -> None:
    """Log when WORKER_PREDEFINED_ETAG_EARLY_SUBMIT is on but fast path cannot run."""
    reasons = predefined_etag_early_submit_skip_reasons(transfer_context)
    if not reasons:
        return
    task_id = offer.get("task_id") or offer.get("offer_id")
    offer_id = offer.get("offer_id") or task_id
    print(
        f"{log_prefix} Predefined ETag fast path skipped "
        f"task={task_label(task_id)} offer={task_label(offer_id)} "
        f"reasons={'; '.join(reasons)} offer_msg={format_task_offer_log(offer)}"
    )


def predefined_etag_bytes_error(bytes_count: int, expected: int) -> Optional[str]:
    """Return an error message when transferred bytes do not match expected size."""
    if expected <= 0:
        return None
    if bytes_count == expected:
        return None
    return f"bytes_mismatch: got {bytes_count} expected {expected}"


def validate_fetch_ready_bytes(
    fetch_ready: FetchReadyState,
    transfer_context: dict,
) -> Optional[str]:
    """Return error when buffered bytes are not exactly 30 MiB (triggers standard fallback)."""
    return predefined_etag_bytes_error(
        fetch_ready.bytes_transferred,
        PREDEFINED_ETAG_CHUNK_SIZE_BYTES,
    )


@dataclass
class PredefinedETagSubmitOutcome:
    success: bool
    chunk_hash: str = ""
    etag: Optional[str] = None
    error: Optional[str] = None
    used_cache: bool = False
    uploaded_from_cache: bool = False
    background_upload_started: bool = False
    hash_source: str = "computed"
    fetch_ms: float = 0.0
    send_ms: float = 0.0


def _log_predefined_etag_submit_ready(
    log_prefix: str,
    task_id: str,
    offer_id: str,
    transfer_context: dict,
    waited_sec: float,
    *,
    hash_source: str,
    result_label: str = "task_result",
) -> None:
    kind = {
        "env": "env hash/etag",
        "cache": "cached hash/etag",
        "computed": "computed hash/etag",
    }.get(hash_source, "hash/etag")
    if waited_sec > 0:
        detail = (
            f"accept_ack + {kind}, waited {waited_sec:.3f}s "
            f"({format_predefined_etag_min_submit_detail(transfer_context)}) before {result_label}"
        )
    else:
        detail = f"accept_ack + {kind}, submitting {result_label}"
    log_task_chunk_from_context(
        "submit_ready",
        transfer_context,
        task_id=task_id,
        offer_id=offer_id,
        log_prefix=log_prefix,
        detail=detail,
    )


async def run_predefined_etag_cached_background_upload(
    state: WorkerState,
    transfer_context: dict,
    chunk_hash: str,
    *,
    task_id: str = None,
    offer_id: str = None,
    log_prefix: str = "[Worker]",
) -> tuple[bool, float, Optional[str]]:
    """Upload local cache file to dest after task_accept (background task)."""
    log_task_chunk_from_context(
        "background_upload_start",
        transfer_context,
        task_id=task_id,
        offer_id=offer_id,
        chunk_hash=chunk_hash,
        log_prefix=log_prefix,
    )
    if not has_predefined_etag_chunk_data(transfer_context):
        return False, 0.0, "cache_file_missing"

    client = state.http_client
    if client is not None:
        ok, send_ms, err = await upload_predefined_etag_from_local_cache(
            client,
            transfer_context,
            chunk_hash,
            task_id=task_id,
            offer_id=offer_id,
            log_prefix=log_prefix,
        )
    else:
        async with httpx.AsyncClient(timeout=SEND_TIMEOUT) as tmp_client:
            ok, send_ms, err = await upload_predefined_etag_from_local_cache(
                tmp_client,
                transfer_context,
                chunk_hash,
                task_id=task_id,
                offer_id=offer_id,
                log_prefix=log_prefix,
            )

    log_task_chunk_from_context(
        "background_upload_done" if ok else "background_upload_failed",
        transfer_context,
        task_id=task_id,
        offer_id=offer_id,
        chunk_hash=chunk_hash,
        log_prefix=log_prefix,
        detail=err or f"send_ms={send_ms:.1f}",
    )
    if ok:
        _emit_transfer_log(
            f"{log_prefix} Cached chunk background upload finished "
            f"task={task_label(task_id)} offer={task_label(offer_id)} "
            f"send_ms={send_ms:.1f}",
            log_prefix=log_prefix,
        )
    else:
        _emit_transfer_log(
            f"{log_prefix} Cached chunk background upload failed "
            f"task={task_label(task_id)} offer={task_label(offer_id)} "
            f"error={err or 'unknown'}",
            log_prefix=log_prefix,
        )
    return ok, send_ms, err


def start_predefined_etag_cached_background_upload(
    state: WorkerState,
    transfer_context: dict,
    chunk_hash: str,
    *,
    task_id: str = None,
    offer_id: str = None,
    log_prefix: str = "[Worker]",
) -> Optional[asyncio.Task]:
    """Start cached upload to dest; caller awaits the task before task_result."""
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        return None
    upload_task = loop.create_task(
        run_predefined_etag_cached_background_upload(
            state,
            transfer_context,
            chunk_hash,
            task_id=task_id,
            offer_id=offer_id,
            log_prefix=log_prefix,
        ),
        name="predefined-etag-cached-background-upload",
    )
    _emit_transfer_log(
        f"{log_prefix} Cached chunk background upload started "
        f"task={task_label(task_id)} offer={task_label(offer_id)}",
        log_prefix=log_prefix,
    )
    return upload_task


async def _await_cached_background_upload_task(
    upload_task: asyncio.Task,
    *,
    task_id: str = None,
    offer_id: str = None,
    log_prefix: str = "[Worker]",
) -> tuple[bool, float, Optional[str]]:
    """Wait for a cached background upload task and return (ok, send_ms, error)."""
    try:
        return await upload_task
    except asyncio.CancelledError:
        _emit_transfer_log(
            f"{log_prefix} Cached chunk background upload cancelled "
            f"task={task_label(task_id)} offer={task_label(offer_id)}",
            log_prefix=log_prefix,
        )
        raise


def _cancel_cached_background_upload(upload_task: Optional[asyncio.Task]) -> None:
    if upload_task is None or upload_task.done():
        return
    upload_task.cancel()


async def _predefined_etag_await_accept(
    accept_task: Callable[[], Awaitable[bool]],
    accept_timeout: float,
) -> tuple[bool, Optional[PredefinedETagSubmitOutcome]]:
    """Return (accepted, failure_outcome). failure_outcome set when accept fails."""
    try:
        accepted = await asyncio.wait_for(accept_task(), timeout=accept_timeout)
    except asyncio.TimeoutError:
        return False, PredefinedETagSubmitOutcome(
            success=False,
            error=f"task_accept_ack timeout ({accept_timeout:.0f}s)",
        )
    except Exception as exc:
        return False, PredefinedETagSubmitOutcome(success=False, error=str(exc))
    if not accepted:
        return False, PredefinedETagSubmitOutcome(
            success=False, error="task_accept rejected"
        )
    return True, None


async def predefined_etag_submit_flow(
    state: WorkerState,
    task_id: str,
    offer_id: str,
    offer: dict,
    transfer_context: dict,
    deadline_us: int,
    *,
    log_prefix: str,
    accept_timeout: float,
    accept_task: Callable[[], Awaitable[bool]],
    push_cache_to_control_server: bool = True,
    offer_started_at: Optional[float] = None,
    result_label: str = "task_result",
) -> PredefinedETagSubmitOutcome:
    """Predefined ETag: cache hit → parallel bg upload + accept, then task_result."""
    started_at = offer_started_at if offer_started_at is not None else time.perf_counter()
    hash_source = predefined_etag_known_source(transfer_context)
    known = get_predefined_etag_cache(transfer_context)
    stage = hash_source or "cache_miss"
    log_task_chunk_from_context(
        stage,
        transfer_context,
        task_id=task_id,
        offer_id=offer_id,
        chunk_hash=known.chunk_hash if known else "",
        log_prefix=log_prefix,
        detail=f"etag={known.etag!r}" if known else "",
    )

    if known:
        if has_predefined_etag_chunk_data(transfer_context):
            upload_task = start_predefined_etag_cached_background_upload(
                state,
                transfer_context,
                known.chunk_hash,
                task_id=task_id,
                offer_id=offer_id,
                log_prefix=log_prefix,
            )
            if upload_task is None:
                return PredefinedETagSubmitOutcome(
                    success=False,
                    error="cache_background_upload_not_started",
                    used_cache=True,
                )

            accepted, fail = await _predefined_etag_await_accept(
                accept_task, accept_timeout
            )
            if fail is not None:
                _cancel_cached_background_upload(upload_task)
                with contextlib.suppress(asyncio.CancelledError):
                    await upload_task
                return fail
            waited_sec = await wait_predefined_etag_min_submit_delay(
                started_at, transfer_context
            )
            try:
                ok, send_ms, err = await _await_cached_background_upload_task(
                    upload_task,
                    task_id=task_id,
                    offer_id=offer_id,
                    log_prefix=log_prefix,
                )
            except asyncio.CancelledError:
                return PredefinedETagSubmitOutcome(
                    success=False,
                    error="cache_background_upload_cancelled",
                    used_cache=True,
                )
            if not ok:
                return PredefinedETagSubmitOutcome(
                    success=False,
                    chunk_hash=known.chunk_hash,
                    etag=known.etag,
                    used_cache=True,
                    error=err or "cache_background_upload_failed",
                    send_ms=send_ms,
                )
            _log_predefined_etag_submit_ready(
                log_prefix,
                task_id,
                offer_id,
                transfer_context,
                waited_sec,
                hash_source=hash_source or "cache",
                result_label=result_label,
            )
            return PredefinedETagSubmitOutcome(
                success=True,
                chunk_hash=known.chunk_hash,
                etag=known.etag,
                used_cache=True,
                uploaded_from_cache=True,
                background_upload_started=True,
                hash_source=hash_source or "cache",
                fetch_ms=0.0,
                send_ms=send_ms,
            )

        if hash_source == "env":
            accepted, fail = await _predefined_etag_await_accept(
                accept_task, accept_timeout
            )
            if fail is not None:
                return fail
            waited_sec = await wait_predefined_etag_min_submit_delay(
                started_at, transfer_context
            )
            _log_predefined_etag_submit_ready(
                log_prefix,
                task_id,
                offer_id,
                transfer_context,
                waited_sec,
                hash_source="env",
                result_label=result_label,
            )
            return PredefinedETagSubmitOutcome(
                success=True,
                chunk_hash=known.chunk_hash,
                etag=known.etag,
                used_cache=True,
                hash_source="env",
                fetch_ms=0.0,
                send_ms=0.0,
            )

        print(
            f"{log_prefix} Cache metadata hit but chunk file missing; "
            f"falling back to source fetch: task={task_label(task_id)} "
            f"offer={task_label(offer_id)}"
        )

    accepted, fail = await _predefined_etag_await_accept(accept_task, accept_timeout)
    if fail is not None:
        return fail

    result = await execute_task_with_metrics(
        state,
        task_id,
        offer,
        transfer_context,
        deadline_us,
        log_prefix=log_prefix,
    )
    if not result.success:
        return PredefinedETagSubmitOutcome(
            success=False,
            chunk_hash=result.chunk_hash,
            error=result.error_msg,
        )

    etag = result.etag or PREDEFINED_ETAG
    maybe_store_predefined_etag_cache_on_success(
        transfer_context,
        result.chunk_hash,
        etag,
        log_prefix=log_prefix,
        task_id=task_id,
        offer_id=offer_id,
        push_to_control_server=push_cache_to_control_server,
    )
    waited_sec = await wait_predefined_etag_min_submit_delay(
        started_at,
        transfer_context,
    )
    _log_predefined_etag_submit_ready(
        log_prefix,
        task_id,
        offer_id,
        transfer_context,
        waited_sec,
        hash_source="computed",
        result_label=result_label,
    )
    return PredefinedETagSubmitOutcome(
        success=True,
        chunk_hash=result.chunk_hash,
        etag=etag,
        used_cache=False,
        hash_source="computed",
        fetch_ms=result.fetch_ms,
        send_ms=result.send_ms,
    )


async def run_predefined_etag_background_transfer(
    state: WorkerState,
    task_id: str,
    offer_id: str,
    offer: dict,
    transfer_context: dict,
    deadline_us: int,
    *,
    log_prefix: str,
) -> TaskExecutionResult:
    """Upload from local cache when available; otherwise fetch from source."""
    log_task_chunk_from_context(
        "background_start",
        transfer_context,
        task_id=task_id,
        offer_id=offer_id,
        log_prefix=log_prefix,
    )
    known = get_predefined_etag_cache(transfer_context)
    if known and has_predefined_etag_chunk_data(transfer_context):
        client = state.http_client
        started = time.perf_counter()
        if client is not None:
            ok, send_ms, err = await upload_predefined_etag_from_local_cache(
                client,
                transfer_context,
                known.chunk_hash,
                task_id=task_id,
                offer_id=offer_id,
                log_prefix=log_prefix,
            )
        else:
            async with httpx.AsyncClient(timeout=SEND_TIMEOUT) as tmp_client:
                ok, send_ms, err = await upload_predefined_etag_from_local_cache(
                    tmp_client,
                    transfer_context,
                    known.chunk_hash,
                    task_id=task_id,
                    offer_id=offer_id,
                    log_prefix=log_prefix,
                )
        duration_ms = (time.perf_counter() - started) * 1000
        if ok:
            update_predefined_etag_cache(
                transfer_context,
                known.chunk_hash,
                known.etag,
                log_prefix=log_prefix,
                task_id=task_id,
                offer_id=offer_id,
            )
        result = TaskExecutionResult(
            success=ok,
            bytes_transferred=predefined_etag_bandwidth_byte_count(transfer_context),
            duration_ms=round(duration_ms, 1),
            chunk_hash=known.chunk_hash,
            etag=known.etag,
            error_msg=err,
            fetch_ms=0.0,
            send_ms=send_ms,
        )
    else:
        result = await execute_task_with_metrics(
            state,
            task_id,
            offer,
            transfer_context,
            deadline_us,
            log_prefix=log_prefix,
        )
        if result.success and result.chunk_hash:
            update_predefined_etag_cache(
                transfer_context,
                result.chunk_hash,
                result.etag,
                log_prefix=log_prefix,
                task_id=task_id,
                offer_id=offer_id,
            )
    log_task_chunk_from_context(
        "background_done" if result.success else "background_failed",
        transfer_context,
        task_id=task_id,
        offer_id=offer_id,
        chunk_hash=result.chunk_hash,
        log_prefix=log_prefix,
        detail=result.error_msg or "",
    )
    return result


def _build_fetch_headers(
    chunk_offset: int = None,
    chunk_size: int = None,
    total_size: int = None,
) -> dict:
    headers = {"ngrok-skip-browser-warning": "true"}
    if chunk_offset is not None and chunk_size is not None:
        if total_size is not None:
            range_end = min(chunk_offset + chunk_size - 1, total_size - 1)
        else:
            range_end = chunk_offset + chunk_size - 1
        headers["Range"] = f"bytes={chunk_offset}-{range_end}"
    return headers




async def fetch_and_send_chunk(
    client: httpx.AsyncClient,
    source_url: str,
    destination_url: str,
    transfer_id: str,
    chunk_index: int,
    *,
    chunk_offset: int = None,
    chunk_size: int = None,
    total_size: int = None,
    expected_max_bytes: int = None,
    expected_chunk_hash: str = None,
    auth_token: str = None,
    task_id: str = None,
    offer_id: str = None,
    route_metadata: Optional[Dict[str, Any]] = None,
    is_canary: bool = False,
    send_chunk_offset: int = None,
    extra_fetch_headers: Optional[Dict[str, str]] = None,
    extra_dest_headers: Optional[Dict[str, str]] = None,
    fetch_ready: Optional[FetchReadyState] = None,
    transfer_context: Optional[dict] = None,
    log_prefix: str = "[Worker]",
) -> tuple[int, str, Optional[str], int, float, float]:
    """Fetch from source and upload to destination.

    Returns:
        (bytes_transferred, chunk_hash, etag, response_code, fetch_ms, send_ms)
    """
    fetch_headers = _build_fetch_headers(chunk_offset, chunk_size, total_size)
    if extra_fetch_headers:
        fetch_headers.update(extra_fetch_headers)
    is_object_storage = is_object_storage_presigned_url(destination_url)
    route_context = (
        object_storage_route_context(destination_url, route_metadata)
        if is_object_storage
        else {}
    )
    upload_offset = (
        send_chunk_offset if send_chunk_offset is not None else (chunk_offset or 0)
    )
    byte_to = (
        upload_offset + expected_max_bytes - 1
        if expected_max_bytes and expected_max_bytes > 0
        else (
            upload_offset + chunk_size - 1
            if chunk_size and chunk_size > 0
            else None
        )
    )
    log_task_chunk(
        "start",
        fetch_url=source_url,
        put_url=destination_url,
        chunk_index=chunk_index,
        byte_from=upload_offset,
        byte_to=byte_to,
        task_id=task_id,
        offer_id=offer_id,
        log_prefix=log_prefix,
    )
    signal_fetch_ready = should_buffer_predefined_etag_fetch(
        fetch_ready,
        transfer_context=transfer_context,
        source_url=source_url,
        chunk_size=int(expected_max_bytes or 0),
        is_object_storage=is_object_storage,
        is_canary=is_canary,
    )

    for attempt in range(MAX_RETRIES):
        queue: asyncio.Queue[Optional[bytes]] = asyncio.Queue(maxsize=32)
        hasher = hashlib.sha256()
        bytes_transferred = 0
        fetch_error: Optional[BaseException] = None
        fetch_started = time.perf_counter()
        send_started = 0.0
        fetch_ms = 0.0
        send_ms = 0.0

        try:
            if signal_fetch_ready:
                buffer = bytearray()
                bytes_transferred = 0
                fetch_started = time.perf_counter()
                try:
                    async with predefined_etag_fast_path_semaphore:
                        async with client.stream(
                            "GET", source_url, headers=fetch_headers, timeout=FETCH_TIMEOUT
                        ) as response:
                            if response.status_code not in (200, 206):
                                response.raise_for_status()

                            if expected_max_bytes and expected_max_bytes > 0:
                                content_length = response.headers.get("Content-Length")
                                if content_length:
                                    response_size = int(content_length)
                                    if response_size > expected_max_bytes:
                                        raise ValueError(
                                            f"response too large: {response_size} bytes > "
                                            f"expected {expected_max_bytes}"
                                        )

                            async for part in response.aiter_bytes(
                                chunk_size=FETCH_STREAM_CHUNK_SIZE
                            ):
                                buffer.extend(part)
                                bytes_transferred += len(part)
                                if (
                                    expected_max_bytes
                                    and expected_max_bytes > 0
                                    and bytes_transferred > expected_max_bytes
                                ):
                                    raise ValueError(
                                        f"response exceeded expected size while buffering: "
                                        f"{bytes_transferred} bytes > expected {expected_max_bytes}"
                                    )
                except Exception as exc:
                    fetch_ready.signal_error(exception_detail(exc))
                    raise

                fetch_ms = (time.perf_counter() - fetch_started) * 1000
                body = bytes(buffer)
                if transfer_context is not None:
                    save_predefined_etag_chunk_data(transfer_context, body)
                chunk_hash = await asyncio.get_running_loop().run_in_executor(
                    None, _sha256_hex, body
                )
                if expected_chunk_hash and chunk_hash.lower() != expected_chunk_hash.lower():
                    mismatch = (
                        f"chunk hash mismatch: expected {expected_chunk_hash}, got {chunk_hash}"
                    )
                    fetch_ready.signal_error(mismatch)
                    raise ValueError(mismatch)

                log_task_chunk(
                    "fetch_done",
                    fetch_url=source_url,
                    put_url=destination_url,
                    chunk_index=chunk_index,
                    byte_from=upload_offset,
                    byte_to=byte_to,
                    chunk_hash=chunk_hash,
                    task_id=task_id,
                    offer_id=offer_id,
                    log_prefix=log_prefix,
                    detail=f"fetch_ms={fetch_ms:.1f} etag={PREDEFINED_ETAG!r} upload_deferred",
                )
                fetch_ready.signal_ready(
                    bytes_transferred,
                    chunk_hash,
                    fetch_ms,
                    PREDEFINED_ETAG,
                    buffer=body,
                )
                return (
                    bytes_transferred,
                    chunk_hash,
                    PREDEFINED_ETAG,
                    200,
                    fetch_ms,
                    0.0,
                )

            save_chunk_data = (
                transfer_context is not None
                and predefined_etag_transfer_eligible(transfer_context)
                and not is_canary
            )
            chunk_data_path = (
                predefined_etag_chunk_data_path(transfer_context)
                if save_chunk_data
                else None
            )
            if chunk_data_path is not None:
                chunk_data_path.parent.mkdir(parents=True, exist_ok=True)

            async def fetch_producer() -> None:
                nonlocal bytes_transferred, fetch_error, fetch_ms
                chunk_data_file = None
                try:
                    if chunk_data_path is not None:
                        chunk_data_file = open(chunk_data_path, "wb")
                    async with client.stream(
                        "GET", source_url, headers=fetch_headers, timeout=FETCH_TIMEOUT
                    ) as response:
                        if response.status_code not in (200, 206):
                            response.raise_for_status()

                        if expected_max_bytes and expected_max_bytes > 0:
                            content_length = response.headers.get("Content-Length")
                            if content_length:
                                response_size = int(content_length)
                                if response_size > expected_max_bytes:
                                    raise ValueError(
                                        f"response too large: {response_size} bytes > "
                                        f"expected {expected_max_bytes}"
                                    )

                        async for part in response.aiter_bytes(
                            chunk_size=FETCH_STREAM_CHUNK_SIZE
                        ):
                            hasher.update(part)
                            bytes_transferred += len(part)
                            if chunk_data_file is not None:
                                chunk_data_file.write(part)
                            if (
                                expected_max_bytes
                                and expected_max_bytes > 0
                                and bytes_transferred > expected_max_bytes
                            ):
                                raise ValueError(
                                    f"response exceeded expected size while streaming: "
                                    f"{bytes_transferred} bytes > expected {expected_max_bytes}"
                                )
                            await queue.put(part)
                except Exception as exc:
                    fetch_error = exc
                finally:
                    if chunk_data_file is not None:
                        chunk_data_file.close()
                    await queue.put(None)
                    fetch_ms = (time.perf_counter() - fetch_started) * 1000

            throttle_upload = (
                PREDEFINED_ETAG_MAX_SPEED_MBPS > 0
                and transfer_context is not None
                and predefined_etag_transfer_eligible(transfer_context)
            )
            upload_bytes_per_sec = (
                PREDEFINED_ETAG_MAX_SPEED_MBPS * 1_000_000 / 8
                if throttle_upload
                else 0.0
            )

            async def body_stream():
                while True:
                    part = await queue.get()
                    if part is None:
                        break
                    yield part
                    if upload_bytes_per_sec > 0 and part:
                        await asyncio.sleep(len(part) / upload_bytes_per_sec)

            async def stream_to_destination(chunk_hash_header: Optional[str]) -> httpx.Response:
                if is_object_storage:
                    send_headers = {"Content-Type": "application/octet-stream"}
                    if expected_max_bytes and expected_max_bytes > 0:
                        send_headers["Content-Length"] = str(expected_max_bytes)
                    if extra_dest_headers:
                        send_headers.update(extra_dest_headers)
                    return await client.put(
                        destination_url,
                        content=body_stream(),
                        headers=send_headers,
                        timeout=SEND_TIMEOUT,
                    )

                send_headers = {
                    "Content-Type": "application/octet-stream",
                    "X-Transfer-ID": transfer_id,
                    "X-Chunk-ID": f"chunk_{chunk_index}",
                    "X-Offset": str(upload_offset),
                    "X-Length": str(expected_max_bytes or bytes_transferred or 0),
                    "X-Total-Size": str(total_size or 0),
                    "X-Chunk-SHA256": chunk_hash_header or "",
                }
                if expected_max_bytes and expected_max_bytes > 0:
                    send_headers["Content-Length"] = str(expected_max_bytes)
                if extra_dest_headers:
                    send_headers.update(extra_dest_headers)
                if auth_token:
                    send_headers["Authorization"] = f"Bearer {auth_token}"
                return await client.post(
                    destination_url,
                    content=body_stream(),
                    headers=send_headers,
                    timeout=SEND_TIMEOUT,
                )

            producer_task = asyncio.create_task(fetch_producer())

            if is_canary:
                async for _ in body_stream():
                    pass
                await producer_task
                if fetch_error:
                    raise fetch_error
                canary_hash = hasher.hexdigest()
                log_task_chunk(
                    "fetch_done",
                    fetch_url=source_url,
                    put_url=destination_url,
                    chunk_index=chunk_index,
                    byte_from=upload_offset,
                    byte_to=byte_to,
                    chunk_hash=canary_hash,
                    task_id=task_id,
                    offer_id=offer_id,
                    log_prefix=log_prefix,
                    detail=f"fetch_ms={fetch_ms:.1f} canary_skip_put",
                )
                return (
                    bytes_transferred,
                    canary_hash,
                    None,
                    0,
                    fetch_ms,
                    0.0,
                )

            send_started = time.perf_counter()
            send_task = asyncio.create_task(stream_to_destination(expected_chunk_hash))
            await producer_task
            if fetch_error:
                send_task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await send_task
                raise fetch_error

            chunk_hash = hasher.hexdigest()
            if expected_chunk_hash and chunk_hash.lower() != expected_chunk_hash.lower():
                send_task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await send_task
                raise ValueError(
                    f"chunk hash mismatch: expected {expected_chunk_hash}, got {chunk_hash}"
                )

            log_task_chunk(
                "fetch_done",
                fetch_url=source_url,
                put_url=destination_url,
                chunk_index=chunk_index,
                byte_from=upload_offset,
                byte_to=byte_to,
                chunk_hash=chunk_hash,
                task_id=task_id,
                offer_id=offer_id,
                log_prefix=log_prefix,
                detail=f"fetch_ms={fetch_ms:.1f}",
            )

            response = await send_task

            response.raise_for_status()
            send_ms = (time.perf_counter() - send_started) * 1000

            etag = response.headers.get("ETag") or response.headers.get("etag")
            log_task_chunk(
                "put_done",
                fetch_url=source_url,
                put_url=destination_url,
                chunk_index=chunk_index,
                byte_from=upload_offset,
                byte_to=byte_to,
                chunk_hash=chunk_hash,
                task_id=task_id,
                offer_id=offer_id,
                log_prefix=log_prefix,
                detail=f"etag={etag!r} send_ms={send_ms:.1f} response={response.status_code}",
            )
            return (
                bytes_transferred,
                chunk_hash,
                etag,
                response.status_code,
                fetch_ms,
                send_ms,
            )

        except Exception as e:
            if "producer_task" in locals() and not producer_task.done():
                producer_task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await producer_task

            is_transient_storage_404 = (
                is_object_storage
                and isinstance(e, httpx.HTTPStatusError)
                and e.response.status_code == 404
                and attempt < 2
            )
            can_retry = is_retryable(e) or is_transient_storage_404
            if is_object_storage and (not can_retry or attempt == MAX_RETRIES - 1):
                print(
                    "[Worker] Object storage upload failed "
                    f"task={task_label(task_id)} offer={task_label(offer_id)} "
                    f"chunk={chunk_index} error={exception_detail(e)}{http_status_detail(e)}"
                    f"{format_route_context(route_context)}"
                )
            if not can_retry or attempt == MAX_RETRIES - 1:
                raise
            print(
                "[Worker] Transfer retry "
                f"task={task_label(task_id)} offer={task_label(offer_id)} "
                f"chunk={chunk_index} attempt={attempt + 1}/{MAX_RETRIES} "
                f"error={exception_detail(e)}{http_status_detail(e)}"
            )
            await asyncio.sleep(RETRY_BACKOFF * (2**attempt))

    raise Exception("Max retries exceeded")


def estimate_task_bytes(task: dict) -> int:
    """Estimate how many bytes this task can hold in memory."""
    raw_chunk_size = task.get("chunk_size") or DEFAULT_CHUNK_SIZE_BYTES

    try:
        chunk_size = max(1, int(raw_chunk_size))
    except (TypeError, ValueError):
        chunk_size = DEFAULT_CHUNK_SIZE_BYTES

    return chunk_size


async def ws_send_task_reject(
    websocket,
    state: WorkerState,
    task_id: str,
    reason: str,
    offer_id: str = None,
) -> bool:
    """Reject a WebSocket task offer so BeamCore can reassign it quickly."""
    try:
        msg = {
            "type": "task_reject",
            "offer_id": offer_id or task_id,
            "task_id": task_id,
            "worker_id": state.worker_id,
            "reason": reason,
        }
        await ws_send_json(websocket, state, msg)
        return True
    except Exception as e:
        print(f"[Worker] WS task_reject error: {e}")
        return False


def try_reserve_ws_capacity(
    state: WorkerState, task_id: str, estimated_bytes: int
) -> Optional[str]:
    """Reserve local capacity for a pushed task before accepting it."""
    if task_id in state.active_ws_task_ids:
        return "duplicate"

    if estimated_bytes > MAX_IN_FLIGHT_BYTES:
        return f"task_too_large:{estimated_bytes}"

    if state.reserved_ws_slots >= MAX_QUEUED_WS_TASKS:
        return f"queue_full:{state.reserved_ws_slots}"

    if state.reserved_bytes + estimated_bytes > MAX_IN_FLIGHT_BYTES:
        return f"memory_budget:{state.reserved_bytes + estimated_bytes}"

    state.active_ws_task_ids.add(task_id)
    state.reserved_ws_slots += 1
    state.reserved_bytes += estimated_bytes
    return None


def release_ws_capacity(
    state: WorkerState, task_id: str, estimated_bytes: int, reserved: bool
) -> None:
    """Release capacity reserved for a pushed task."""
    if reserved:
        state.reserved_ws_slots = max(0, state.reserved_ws_slots - 1)
        state.reserved_bytes = max(0, state.reserved_bytes - max(0, estimated_bytes))
    state.active_ws_task_ids.discard(task_id)


async def execute_transfer(
    state: WorkerState,
    task_id: str,
    transfer_context: dict,
    task_message: dict,
    deadline_us: int,
    fetch_ready: Optional[FetchReadyState] = None,
    log_prefix: str = "[Worker]",
) -> tuple:
    """Execute real data transfer: fetch from source, send to destination.

    Returns: (bytes_transferred, success, error_message, chunk_hash, etag)
    """
    source_url = transfer_context["source_url"]
    destination_url = transfer_context["dest_url"]
    transfer_id = transfer_context.get("transfer_id", "")
    chunk_size = int(transfer_context["chunk_size"])
    range_start = int(transfer_context["range_start"])
    range_end = int(transfer_context["range_end"])
    source_headers_offer = transfer_context.get("source_headers") or {}
    dest_headers_offer = transfer_context.get("dest_headers") or {}
    chunk_index = 0

    # Build per-chunk hash map
    chunk_hashes: dict = {}
    if "chunk_hashes" in task_message and isinstance(task_message["chunk_hashes"], dict):
        for k, v in task_message["chunk_hashes"].items():
            chunk_hashes[int(k)] = v
    elif "chunk_hash" in task_message and task_message["chunk_hash"]:
        chunk_hashes[chunk_index] = task_message["chunk_hash"]

    client = state.http_client
    # Buffered predefined-etag path skips prewarm: HEAD on origin root adds latency
    # without helping the signed Range GET on the object path.
    if fetch_ready is None:
        await prewarm_for_transfer(state, source_url, destination_url)

    total_bytes = 0
    is_canary = is_canary_destination(destination_url)
    computed_chunk_hash = ""
    last_etag: Optional[str] = None
    last_fetch_ms = 0.0
    last_send_ms = 0.0
    offer_id = task_message.get("offer_id") or task_id

    try:
        # Check deadline
        if deadline_us > 0:
            now_us = time.time() * 1_000_000
            remaining_us = deadline_us - now_us
            if remaining_us <= 0:
                reason = f"Deadline exceeded before chunk {chunk_index}"
                _log_transfer_failure(
                    transfer_context,
                    task_id=task_id,
                    offer_id=str(offer_id),
                    chunk_index=chunk_index,
                    log_prefix=log_prefix,
                    reason=reason,
                )
                return (
                    total_bytes,
                    False,
                    reason,
                    "",
                    last_etag,
                    0.0,
                    0.0,
                )

        chunk_started = time.perf_counter()
        (
            bytes_fetched,
            computed_chunk_hash,
            etag,
            response_code,
            fetch_ms,
            send_ms,
        ) = await fetch_and_send_chunk(
            client,
            source_url,
            destination_url,
            transfer_id,
            chunk_index,
            total_size=chunk_size,
            expected_max_bytes=chunk_size,
            expected_chunk_hash=chunk_hashes.get(chunk_index),
            task_id=task_id,
            offer_id=offer_id,
            extra_fetch_headers=source_headers_offer or None,
            extra_dest_headers=dest_headers_offer or None,
            is_canary=is_canary,
            send_chunk_offset=range_start,
            fetch_ready=fetch_ready,
            transfer_context=transfer_context,
            log_prefix=log_prefix,
        )

        last_fetch_ms = fetch_ms
        last_send_ms = send_ms

        if bytes_fetched != chunk_size:
            reason = f"source range returned {bytes_fetched} bytes, expected {chunk_size}"
            _log_transfer_failure(
                transfer_context,
                task_id=task_id,
                offer_id=str(offer_id),
                chunk_index=chunk_index,
                log_prefix=log_prefix,
                reason=reason,
                chunk_hash=computed_chunk_hash or "",
            )
            return (
                total_bytes,
                False,
                reason,
                computed_chunk_hash or "",
                last_etag,
                last_fetch_ms,
                last_send_ms,
            )

        if is_canary:
            total_bytes += bytes_fetched
            total_ms = (time.perf_counter() - chunk_started) * 1000
            log_task_chunk(
                "complete",
                fetch_url=source_url,
                put_url=destination_url,
                chunk_index=chunk_index,
                byte_from=range_start,
                byte_to=range_end,
                chunk_hash=computed_chunk_hash,
                task_id=task_id,
                offer_id=offer_id,
                log_prefix=log_prefix,
                detail=f"canary_skip_put total_ms={total_ms:.1f}",
            )
        else:
            if etag:
                last_etag = etag

            total_bytes += bytes_fetched
            total_ms = (time.perf_counter() - chunk_started) * 1000
            mbps = (bytes_fetched * 8 / 1_000_000) / (total_ms / 1000) if total_ms > 0 else 0
            log_task_chunk(
                "complete",
                fetch_url=source_url,
                put_url=destination_url,
                chunk_index=chunk_index,
                byte_from=range_start,
                byte_to=range_end,
                chunk_hash=computed_chunk_hash,
                task_id=task_id,
                offer_id=offer_id,
                log_prefix=log_prefix,
                detail=(
                    f"fetch_ms={fetch_ms:.1f} send_ms={send_ms:.1f} "
                    f"total_ms={total_ms:.1f} mbps={mbps:.1f} response={response_code}"
                ),
            )

    except asyncio.TimeoutError as e:
        detail = exception_detail(e)
        reason = f"Deadline exceeded at chunk {chunk_index}: {detail}"
        _log_transfer_failure(
            transfer_context,
            task_id=task_id,
            offer_id=str(offer_id),
            chunk_index=chunk_index,
            log_prefix=log_prefix,
            reason=reason,
        )
        return (
            total_bytes,
            False,
            reason,
            "",
            last_etag,
            last_fetch_ms,
            last_send_ms,
        )
    except httpx.HTTPStatusError as e:
        detail = exception_detail(e)
        reason = f"HTTP {e.response.status_code} at chunk {chunk_index}: {detail}"
        _log_transfer_failure(
            transfer_context,
            task_id=task_id,
            offer_id=str(offer_id),
            chunk_index=chunk_index,
            log_prefix=log_prefix,
            reason=reason,
        )
        return (
            total_bytes,
            False,
            reason,
            "",
            last_etag,
            last_fetch_ms,
            last_send_ms,
        )
    except Exception as e:
        detail = exception_detail(e)
        reason = f"Error at chunk {chunk_index}: {detail}"
        _log_transfer_failure(
            transfer_context,
            task_id=task_id,
            offer_id=str(offer_id),
            chunk_index=chunk_index,
            log_prefix=log_prefix,
            reason=reason,
        )
        return (total_bytes, False, reason, "", last_etag, last_fetch_ms, last_send_ms)

    if transfer_context.get("etag_required") and not last_etag:
        reason = "missing ETag from storage PUT response"
        _log_transfer_failure(
            transfer_context,
            task_id=task_id,
            offer_id=str(offer_id),
            chunk_index=chunk_index,
            log_prefix=log_prefix,
            reason=reason,
            chunk_hash=computed_chunk_hash or "",
        )
        return (
            total_bytes,
            False,
            reason,
            computed_chunk_hash or "",
            last_etag,
            last_fetch_ms,
            last_send_ms,
        )

    if log_prefix != "[Embedded]":
        print(f"[Worker] Transfer complete: {total_bytes} bytes")
    return (
        total_bytes,
        True,
        None,
        computed_chunk_hash,
        last_etag,
        last_fetch_ms,
        last_send_ms,
    )


# =============================================================================
# WebSocket Communication
# =============================================================================


def get_ws_url(
    worker_id: str,
    api_key: str,
    gateway_url: str,
    worker_secret: Optional[str] = None,
) -> str:
    """Convert worker gateway URL to the worker WebSocket URL."""
    base = gateway_url.rstrip("/")
    if base.startswith("https://"):
        ws_base = "wss://" + base[8:]
    elif base.startswith("http://"):
        ws_base = "ws://" + base[7:]
    elif base.startswith("wss://"):
        ws_base = "wss://" + base[6:]
    elif base.startswith("ws://"):
        ws_base = "ws://" + base[5:]
    else:
        ws_base = "ws://" + base
    url = f"{ws_base}/ws/{worker_id}"
    params: dict[str, str] = {}
    if api_key:
        params["api_key"] = api_key
    if worker_secret:
        params["worker_secret"] = worker_secret
    params["worker_version"] = WORKER_VERSION
    if params:
        url = f"{url}?{urlencode(params)}"
    return url


def get_ws_status_code(exc: Exception) -> Optional[int]:
    """Extract an HTTP status code from websocket handshake failures."""
    status_code = getattr(exc, "status_code", None)
    if isinstance(status_code, int):
        return status_code

    response = getattr(exc, "response", None)
    response_status = getattr(response, "status_code", None)
    if isinstance(response_status, int):
        return response_status

    message = str(exc)
    for token in message.split():
        stripped = token.rstrip(":,)")
        if stripped.isdigit():
            value = int(stripped)
            if 100 <= value <= 599:
                return value
    return None


async def ws_send_task_accept(
    websocket, state: WorkerState, task_id: str, offer_id: str = None
) -> bool:
    """Send task acceptance over WebSocket."""
    try:
        msg = {
            "type": "task_accept",
            "offer_id": offer_id or task_id,
            "task_id": task_id,
            "worker_id": state.worker_id,
            "worker_version": WORKER_VERSION,
        }
        await ws_send_json(websocket, state, msg)
        return True
    except Exception as e:
        print(f"[Worker] WS task_accept error: {e}")
        return False


async def ws_send_worker_hello(websocket, state: WorkerState) -> bool:
    """Announce worker host metadata to the gateway for pool scheduling."""
    try:
        ip = state.worker_ip or await get_public_ip()
        state.worker_ip = ip
        msg = {
            "type": "worker_hello",
            "worker_id": state.worker_id,
            "worker_version": WORKER_VERSION,
            "ip": ip,
            "claimed_bandwidth_mbps": 100,
            "max_concurrent_tasks": MAX_CONCURRENT_TASKS,
            "initial_order": WORKER_INITIAL_ORDER,
        }
        await ws_send_json(websocket, state, msg)
        return True
    except Exception as e:
        print(f"[Worker] WS worker_hello error: {e}")
        return False


async def ws_send_task_result(
    websocket,
    state: WorkerState,
    task_id: str,
    success: bool,
    bytes_transferred: int,
    chunk_hash: str = "",
    etag: str = None,
    error: str = None,
    offer_id: str = None,
    transfer_mbps: float = 0.0,
) -> bool:
    """Send task completion receipt over WebSocket."""
    try:
        msg = {
            "type": "task_result",
            "task_id": task_id,
            "offer_id": offer_id or task_id,
            "worker_id": state.worker_id,
            "success": success,
            "bytes_transferred": bytes_transferred,
        }
        if transfer_mbps > 0:
            msg["transfer_mbps"] = round(transfer_mbps, 1)
        if chunk_hash:
            msg["chunk_hash"] = chunk_hash
        if etag:
            msg["etag"] = etag
        if error:
            msg["error"] = error
        await ws_send_json(websocket, state, msg)
        return True
    except Exception as e:
        print(f"[Worker] WS task_result error: {e}")
        return False


async def finalize_ws_task_result(
    websocket,
    state: WorkerState,
    task_id: str,
    success: bool,
    bytes_transferred: int,
    chunk_hash: str = "",
    etag: str = None,
    error: str = None,
    offer_id: str = None,
    transfer_mbps: float = 0.0,
) -> TaskSummaryAck:
    """Send task_result and wait for BeamCore ack (received / completed)."""
    result_key = offer_id or task_id
    empty = TaskSummaryAck()

    for attempt in range(3):
        ack_future: asyncio.Future = asyncio.get_event_loop().create_future()
        state.pending_task_results[result_key] = ack_future

        try:
            print(
                f"[Worker] [WS] Sending task_result: task={task_label(task_id)} "
                f"offer={task_label(offer_id)} success={success} "
                f"bytes={bytes_transferred} mbps={round(transfer_mbps, 1)}"
            )
            sent = await ws_send_task_result(
                websocket,
                state,
                task_id,
                success,
                bytes_transferred,
                chunk_hash=chunk_hash,
                etag=etag,
                error=error,
                offer_id=offer_id,
                transfer_mbps=transfer_mbps,
            )
            if not sent:
                continue

            try:
                ack = await asyncio.wait_for(ack_future, timeout=WS_TASK_RESULT_ACK_TIMEOUT)
                if ack.completed:
                    print(
                        f"[Worker] [WS] Task completed on BeamCore: {task_label(task_id)} offer={task_label(offer_id)}"
                    )
                    return ack
                if ack.received:
                    reason = ack.reason or "not_completed"
                    print(
                        f"[Worker] [WS] Task result received but not completed: "
                        f"task={task_label(task_id)} offer={task_label(offer_id)} reason={reason}"
                    )
                    return ack
                print(
                    f"[Worker] [WS] Task result nack from gateway: {task_label(task_id)} offer={task_label(offer_id)}"
                )
            except asyncio.TimeoutError:
                print(
                    f"[Worker] [WS] Task result ack timeout "
                    f"attempt={attempt + 1}/3 task={task_label(task_id)} offer={task_label(offer_id)}"
                )
        finally:
            state.pending_task_results.pop(result_key, None)

    print(
        f"[Worker] [WS] Task result failed after websocket retries: {task_label(task_id)} offer={task_label(offer_id)}"
    )
    return empty


async def ws_send_json(websocket, state: WorkerState, payload: dict) -> None:
    """Serialize worker websocket sends to avoid concurrent-send races."""
    if state.ws_send_lock is None:
        state.ws_send_lock = asyncio.Lock()

    async with state.ws_send_lock:
        await websocket.send(json.dumps(payload))


def track_ws_task(state: WorkerState, coro: asyncio.coroutines) -> None:
    """Track spawned WS task handlers so they are not dropped on loop exit."""
    task = asyncio.create_task(coro)
    state.ws_task_handles.add(task)

    def _on_done(done_task: asyncio.Task) -> None:
        state.ws_task_handles.discard(done_task)
        try:
            exc = done_task.exception()
        except asyncio.CancelledError:
            return
        if exc is not None:
            logging.getLogger("worker").error(
                "[Worker] [WS] Task handler crashed: %s: %s",
                type(exc).__name__,
                exc,
            )

    task.add_done_callback(_on_done)


async def _cancel_exec_task(exec_task: asyncio.Task, task_id: str, offer_id: str) -> None:
    """Stop in-flight transfer when accept is rejected or times out."""
    if exec_task.done():
        return
    exec_task.cancel()
    try:
        await exec_task
    except asyncio.CancelledError:
        print(
            f"[Worker] [WS] Transfer cancelled: task={task_label(task_id)} "
            f"offer={task_label(offer_id)}"
        )


async def _finalize_ws_task(
    websocket,
    state: WorkerState,
    task_id: str,
    offer_id: str,
    result: TaskExecutionResult,
) -> bool:
    await finalize_ws_task_result(
        websocket,
        state,
        task_id,
        result.success,
        result.bytes_transferred,
        chunk_hash=result.chunk_hash,
        etag=result.etag,
        error=result.error_msg,
        offer_id=offer_id,
        transfer_mbps=transfer_mbps(result.bytes_transferred, result.duration_ms),
    )

    status = "OK" if result.success else f"FAIL: {result.error_msg}"
    print(
        f"[Worker] [WS] Task {task_label(task_id)} offer={task_label(offer_id)}: {status} | "
        f"{result.bytes_transferred} bytes"
    )
    return result.success


async def _handle_ws_task_sequential_accept(
    state: WorkerState,
    websocket,
    task: dict,
    task_id: str,
    offer_id: str,
    task_key: str,
    transfer_context: dict,
    deadline_us: int,
) -> bool:
    """Wait for task_accept_ack before starting the transfer (legacy path)."""
    accept_future: asyncio.Future = asyncio.get_event_loop().create_future()
    state.pending_task_accepts[task_key] = accept_future

    accepted = await ws_send_task_accept(websocket, state, task_id, offer_id=offer_id)
    if not accepted:
        state.pending_task_accepts.pop(task_key, None)
        print("[Worker] [WS] Failed to send task_accept")
        return False
    print(
        f"[Worker] [WS] Sent task_accept, awaiting ack: task={task_label(task_id)} "
        f"offer={task_label(offer_id)}"
    )

    try:
        server_accepted = await asyncio.wait_for(
            accept_future, timeout=WS_TASK_ACCEPT_ACK_TIMEOUT
        )
        if not server_accepted:
            print(
                f"[Worker] [WS] task_accept_ack rejected: task={task_label(task_id)} "
                f"offer={task_label(offer_id)}"
            )
            return False
    except asyncio.TimeoutError:
        state.pending_task_accepts.pop(task_key, None)
        print(
            f"[Worker] [WS] task_accept_ack timeout ({WS_TASK_ACCEPT_ACK_TIMEOUT}s): "
            f"task={task_label(task_id)} offer={task_label(offer_id)}"
        )
        return False

    print(
        f"[Worker] [WS] task_accept_ack OK, starting transfer: task={task_label(task_id)} "
        f"offer={task_label(offer_id)}"
    )

    result = await execute_task_with_metrics(
        state,
        task_id,
        task,
        transfer_context,
        deadline_us,
        log_prefix="[Worker] [WS]",
    )
    if result.success:
        maybe_store_predefined_etag_cache_on_success(
            transfer_context,
            result.chunk_hash,
            result.etag,
            log_prefix="[Worker] [WS]",
            task_id=task_id,
            offer_id=offer_id,
        )
    return await _finalize_ws_task(websocket, state, task_id, offer_id, result)


async def _cancel_predefined_etag_tasks(
    exec_task: asyncio.Task,
    upload_task: asyncio.Task,
    *,
    task_id: str,
    offer_id: str,
) -> None:
    if not exec_task.done():
        await _cancel_exec_task(exec_task, task_id, offer_id)
    else:
        with contextlib.suppress(asyncio.CancelledError):
            await exec_task
    upload_task.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await upload_task


async def _finalize_ws_standard_transfer_after_accept(
    state: WorkerState,
    websocket,
    task: dict,
    task_id: str,
    offer_id: str,
    transfer_context: dict,
    deadline_us: int,
    *,
    reason: str,
) -> bool:
    """Run stream download+upload and submit with the real staging etag."""
    print(
        f"[Worker] [WS] Falling back to standard transfer ({reason}): "
        f"task={task_label(task_id)} offer={task_label(offer_id)}"
    )
    result = await execute_task_with_metrics(
        state,
        task_id,
        task,
        transfer_context,
        deadline_us,
        log_prefix="[Worker] [WS]",
    )
    if result.success:
        maybe_store_predefined_etag_cache_on_success(
            transfer_context,
            result.chunk_hash,
            result.etag,
            log_prefix="[Worker] [WS]",
            task_id=task_id,
            offer_id=offer_id,
        )
    return await _finalize_ws_task(websocket, state, task_id, offer_id, result)


async def _handle_ws_task_predefined_etag_early_result(
    state: WorkerState,
    websocket,
    task: dict,
    task_id: str,
    offer_id: str,
    task_key: str,
    transfer_context: dict,
    deadline_us: int,
) -> bool:
    """Predefined ETag: cache hit submits after accept; miss fetch+upload; background if cached."""
    accept_future: asyncio.Future = asyncio.get_event_loop().create_future()
    state.pending_task_accepts[task_key] = accept_future

    cached = get_predefined_etag_cache(transfer_context)
    print(
        f"[Worker] [WS] Predefined ETag: "
        f"{'cache hit, accept + background upload then submit' if cached else 'cache miss, fetch+upload then submit'}: "
        f"task={task_label(task_id)} offer={task_label(offer_id)}"
    )

    async def _accept_task() -> bool:
        if not await ws_send_task_accept(websocket, state, task_id, offer_id=offer_id):
            return False
        try:
            return await asyncio.wait_for(accept_future, timeout=WS_TASK_ACCEPT_ACK_TIMEOUT)
        except asyncio.TimeoutError:
            return False

    try:
        outcome = await predefined_etag_submit_flow(
            state,
            task_id,
            offer_id,
            task,
            transfer_context,
            deadline_us,
            log_prefix="[Worker] [WS]",
            accept_timeout=WS_TASK_ACCEPT_ACK_TIMEOUT,
            accept_task=_accept_task,
        )
    finally:
        state.pending_task_accepts.pop(task_key, None)

    if not outcome.success:
        print(
            f"[Worker] [WS] Predefined submit failed: task={task_label(task_id)} "
            f"offer={task_label(offer_id)} reason={outcome.error}"
        )
        if outcome.error:
            result = TaskExecutionResult(
                success=False,
                bytes_transferred=0,
                duration_ms=0.0,
                chunk_hash=outcome.chunk_hash,
                error_msg=outcome.error,
            )
            return await _finalize_ws_task(websocket, state, task_id, offer_id, result)
        return False

    result = TaskExecutionResult(
        success=True,
        bytes_transferred=int(transfer_context.get("chunk_size") or 0),
        duration_ms=0.0,
        chunk_hash=outcome.chunk_hash,
        etag=outcome.etag,
    )
    finalized = await _finalize_ws_task(websocket, state, task_id, offer_id, result)

    if outcome.used_cache and not outcome.background_upload_started:
        asyncio.create_task(
            _await_predefined_etag_background_transfer_task(
                state,
                task,
                task_id,
                offer_id,
                transfer_context,
                deadline_us,
            )
        )
    return finalized


async def _await_predefined_etag_background_transfer_task(
    state: WorkerState,
    task: dict,
    task_id: str,
    offer_id: str,
    transfer_context: dict,
    deadline_us: int,
) -> None:
    """Wait for background fetch+upload after cached early task_result."""
    try:
        result = await run_predefined_etag_background_transfer(
            state,
            task_id,
            offer_id,
            task,
            transfer_context,
            deadline_us,
            log_prefix="[Worker] [WS]",
        )
    except asyncio.CancelledError:
        print(
            f"[Worker] [WS] Background transfer cancelled: task={task_label(task_id)} "
            f"offer={task_label(offer_id)}"
        )
        return
    except Exception as exc:
        print(
            f"[Worker] [WS] Background transfer error: task={task_label(task_id)} "
            f"offer={task_label(offer_id)} error={exc}"
        )
        return

    if result.success:
        print(
            f"[Worker] [WS] Background transfer finished after task_result: "
            f"task={task_label(task_id)} offer={task_label(offer_id)}"
        )
    else:
        print(
            f"[Worker] [WS] Background transfer failed after task_result: "
            f"task={task_label(task_id)} offer={task_label(offer_id)} "
            f"error={result.error_msg}"
        )


async def _await_predefined_etag_upload_task(
    upload_task: asyncio.Task,
    task_id: str,
    offer_id: str,
) -> None:
    """Wait for background buffered upload after task_result was submitted."""
    try:
        ok = await upload_task
    except asyncio.CancelledError:
        print(
            f"[Worker] [WS] Upload cancelled: task={task_label(task_id)} "
            f"offer={task_label(offer_id)}"
        )
        return
    except Exception as exc:
        print(
            f"[Worker] [WS] Upload error: task={task_label(task_id)} "
            f"offer={task_label(offer_id)} error={exc}"
        )
        return

    if ok:
        print(
            f"[Worker] [WS] Background upload finished after task_result: "
            f"task={task_label(task_id)} offer={task_label(offer_id)}"
        )
    else:
        print(
            f"[Worker] [WS] Background upload failed after task_result: "
            f"task={task_label(task_id)} offer={task_label(offer_id)}"
        )


async def _handle_ws_task_early_transfer(
    state: WorkerState,
    websocket,
    task: dict,
    task_id: str,
    offer_id: str,
    task_key: str,
    transfer_context: dict,
    deadline_us: int,
) -> bool:
    """Start transfer and send task_accept immediately in parallel."""
    accept_future: asyncio.Future = asyncio.get_event_loop().create_future()
    state.pending_task_accepts[task_key] = accept_future

    exec_task = asyncio.create_task(
        execute_task_with_metrics(
            state,
            task_id,
            task,
            transfer_context,
            deadline_us,
            log_prefix="[Worker] [WS]",
        )
    )

    print(
        f"[Worker] [WS] Early transfer + task_accept in parallel: "
        f"task={task_label(task_id)} offer={task_label(offer_id)}"
    )

    try:
        if not await ws_send_task_accept(websocket, state, task_id, offer_id=offer_id):
            state.pending_task_accepts.pop(task_key, None)
            print("[Worker] [WS] Failed to send task_accept")
            await _cancel_exec_task(exec_task, task_id, offer_id)
            return False

        print(
            f"[Worker] [WS] Sent task_accept, awaiting ack: "
            f"task={task_label(task_id)} offer={task_label(offer_id)}"
        )

        try:
            server_accepted = await asyncio.wait_for(
                accept_future, timeout=WS_TASK_ACCEPT_ACK_TIMEOUT
            )
        except asyncio.TimeoutError:
            state.pending_task_accepts.pop(task_key, None)
            print(
                f"[Worker] [WS] task_accept_ack timeout ({WS_TASK_ACCEPT_ACK_TIMEOUT}s): "
                f"task={task_label(task_id)} offer={task_label(offer_id)}"
            )
            await _cancel_exec_task(exec_task, task_id, offer_id)
            return False

        if not server_accepted:
            print(
                f"[Worker] [WS] task_accept_ack rejected, aborting transfer: "
                f"task={task_label(task_id)} offer={task_label(offer_id)}"
            )
            if exec_task.done():
                print(
                    f"[Worker] [WS] Transfer finished before reject — discarding result: "
                    f"task={task_label(task_id)} offer={task_label(offer_id)}"
                )
            else:
                await _cancel_exec_task(exec_task, task_id, offer_id)
            return False

        print(
            f"[Worker] [WS] task_accept_ack OK: task={task_label(task_id)} "
            f"offer={task_label(offer_id)}"
        )

        result = await exec_task
        if result.success:
            maybe_store_predefined_etag_cache_on_success(
                transfer_context,
                result.chunk_hash,
                result.etag,
                log_prefix="[Worker] [WS]",
                task_id=task_id,
                offer_id=offer_id,
            )
        return await _finalize_ws_task(websocket, state, task_id, offer_id, result)
    except asyncio.CancelledError:
        if not exec_task.done():
            exec_task.cancel()
        raise


async def handle_ws_task(state: WorkerState, websocket, task: dict) -> bool:
    """Handle a task received via WebSocket push."""
    task_id = task.get("task_id") or task.get("offer_id")
    offer_id = task.get("offer_id") or task_id
    task_key = offer_id or task_id
    deadline_us = task.get("deadline_us", 0)
    transfer_context, validation_error = build_transfer_context(task)
    estimated_bytes = estimate_task_bytes(task)
    reserved_capacity = False

    print(f"[Worker] [WS] Task: {task_label(task_id)} offer={task_label(offer_id)}...")
    if not task_id:
        print("[Worker] [WS] Skipping task: missing task_id")
        return False
    if validation_error or transfer_context is None:
        reason = validation_error if validation_error == "unsupported_worker_version" else f"invalid_offer:{validation_error or 'unknown'}"
        await ws_send_task_reject(websocket, state, task_id, reason, offer_id=offer_id)
        print(
            f"[Worker] [WS] Rejected task {task_label(task_id)} offer={task_label(offer_id)}: "
            f"{reason}"
        )
        return False

    capacity_error = try_reserve_ws_capacity(state, task_key, estimated_bytes)
    if capacity_error == "duplicate":
        print(
            f"[Worker] [WS] Duplicate task offer ignored: {task_label(task_id)} offer={task_label(offer_id)}"
        )
        return False
    if capacity_error:
        await ws_send_task_reject(websocket, state, task_id, capacity_error, offer_id=offer_id)
        print(
            f"[Worker] [WS] Rejected task {task_label(task_id)} offer={task_label(offer_id)} "
            f"due to capacity guard: {capacity_error} (budget={MAX_IN_FLIGHT_BYTES} bytes)"
        )
        return False
    reserved_capacity = True

    log_predefined_etag_fast_path_skipped(
        task, transfer_context, log_prefix="[Worker] [WS]"
    )

    try:
        remaining_sec = remaining_deadline_seconds(deadline_us)
        if remaining_sec is not None and remaining_sec < 5:
            reason = f"deadline_too_close:{remaining_sec:.1f}s"
            await ws_send_task_reject(websocket, state, task_id, reason, offer_id=offer_id)
            print(
                f"[Worker] [WS] Rejected task {task_label(task_id)} offer={task_label(offer_id)}: "
                f"{reason}"
            )
            return False

        if uses_predefined_etag_early_submit(transfer_context):
            return await _handle_ws_task_predefined_etag_early_result(
                state,
                websocket,
                task,
                task_id,
                offer_id,
                task_key,
                transfer_context,
                deadline_us,
            )

        if not WORKER_EARLY_TRANSFER:
            return await _handle_ws_task_sequential_accept(
                state,
                websocket,
                task,
                task_id,
                offer_id,
                task_key,
                transfer_context,
                deadline_us,
            )

        return await _handle_ws_task_early_transfer(
            state,
            websocket,
            task,
            task_id,
            offer_id,
            task_key,
            transfer_context,
            deadline_us,
        )
    finally:
        state.pending_task_accepts.pop(task_key, None)
        release_ws_capacity(state, task_key, estimated_bytes, reserved_capacity)


async def websocket_loop(state: WorkerState):
    """WebSocket communication loop with automatic reconnection."""
    if not WEBSOCKETS_AVAILABLE:
        raise RuntimeError("websockets library is required for worker gateway transport")

    if not state.worker_gateway_url:
        raise RuntimeError("WORKER_GATEWAY_URL is required for worker gateway transport")

    ws_url = get_ws_url(
        state.worker_id,
        state.api_key or "",
        state.worker_gateway_url,
        state.worker_gateway_secret,
    )
    print(f"[Worker] Connecting to WebSocket: {ws_url.split('?')[0]}")
    reconnect_delay = WS_RECONNECT_MIN_DELAY

    while state.running and state.use_websocket:
        try:
            async with websockets.connect(
                ws_url,
                ping_interval=WS_PING_INTERVAL,
                ping_timeout=10,
                close_timeout=5,
            ) as websocket:
                state.ws_connected = True
                state.ws_reconnect_attempts = 0
                reconnect_delay = WS_RECONNECT_MIN_DELAY
                print("[Worker] [WS] Connected!")

                while state.running:
                    try:
                        try:
                            msg_str = await asyncio.wait_for(
                                websocket.recv(),
                                timeout=WS_PING_INTERVAL,
                            )
                            message = json.loads(msg_str)
                            msg_type = message.get("type")

                            if msg_type == "connected":
                                print("[Worker] [WS] Server confirmed connection")
                                await ws_send_worker_hello(websocket, state)

                            elif msg_type == "task_offer":
                                track_ws_task(state, handle_ws_task(state, websocket, message))

                            elif msg_type == "task_accept_ack":
                                ack_task_id = message.get("task_id")
                                ack_offer_id = message.get("offer_id") or ack_task_id
                                server_accepted = message.get("accepted", True)
                                if ack_offer_id and ack_offer_id in state.pending_task_accepts:
                                    future = state.pending_task_accepts.pop(ack_offer_id)
                                    if not future.done():
                                        future.set_result(server_accepted)
                                if not server_accepted:
                                    print(
                                        f"[Worker] [WS] Task accept rejected: "
                                        f"task={task_label(ack_task_id)} offer={task_label(ack_offer_id)} "
                                        f"reason={message.get('reason', 'unknown')}"
                                    )

                            elif msg_type == "task_result_ack":
                                ack_task_id = message.get("task_id")
                                ack_offer_id = message.get("offer_id") or ack_task_id
                                received = bool(message.get("received", False))
                                completed_raw = message.get("completed")
                                completed = (
                                    bool(completed_raw)
                                    if completed_raw is not None
                                    else received
                                )
                                reason = message.get("reason")
                                ack = TaskSummaryAck(
                                    received=received,
                                    completed=completed,
                                    reason=str(reason) if reason else None,
                                )
                                if ack_offer_id and ack_offer_id in state.pending_task_results:
                                    future = state.pending_task_results.pop(ack_offer_id)
                                    if not future.done():
                                        future.set_result(ack)
                                if not received:
                                    print(
                                        f"[Worker] [WS] Gateway rejected task_result: "
                                        f"task={task_label(ack_task_id)} offer={task_label(ack_offer_id)}"
                                    )

                            elif msg_type == "error":
                                print(
                                    f"[Worker] [WS] Server error: {message.get('message', 'unknown')}"
                                )

                        except asyncio.TimeoutError:
                            pass

                    except ConnectionClosed as e:
                        print(f"[Worker] [WS] Connection closed: {e.code} {e.reason}")
                        break

        except InvalidStatus as e:
            status = get_ws_status_code(e)
            status_label = status if status is not None else "unknown"
            print(f"[Worker] [WS] Connection rejected: HTTP {status_label}")
            if status == 403:
                print(
                    "[Worker] [WS] HTTP 403 usually means worker gateway auth failed. Check:\n"
                    "  - WORKER_GATEWAY_URL points to an orchestrator with WORKER_GATEWAY_MODE=in_process\n"
                    "    (or global-gateway) that serves /ws/{worker_id}\n"
                    "  - WORKER_GATEWAY_SECRET matches the gateway's WORKER_GATEWAY_SECRET\n"
                    "  - Orchestrator WORKER_GATEWAY_MODE=embedded rejects external WS (use in_process)"
                )
            raise RuntimeError(
                f"worker gateway websocket rejected the connection with HTTP {status_label}"
            ) from e

        except ConnectionRefusedError:
            print("[Worker] [WS] Connection refused")

        except Exception as e:
            print(f"[Worker] [WS] Connection error: {type(e).__name__}: {e}")

        state.ws_connected = False
        state.ws_reconnect_attempts += 1

        if (
            WS_MAX_RECONNECT_ATTEMPTS is not None
            and state.ws_reconnect_attempts >= WS_MAX_RECONNECT_ATTEMPTS
        ):
            raise RuntimeError(
                "worker gateway websocket unavailable after maximum reconnect attempts"
            )

        if state.running and not shutdown_event.is_set():
            if WS_MAX_RECONNECT_ATTEMPTS is None:
                print(
                    f"[Worker] [WS] Reconnecting in {reconnect_delay:.1f}s (attempt {state.ws_reconnect_attempts})..."
                )
            else:
                print(
                    f"[Worker] [WS] Reconnecting in {reconnect_delay:.1f}s (attempt {state.ws_reconnect_attempts}/{WS_MAX_RECONNECT_ATTEMPTS})..."
                )
            try:
                await asyncio.wait_for(shutdown_event.wait(), timeout=reconnect_delay)
                break
            except asyncio.TimeoutError:
                pass
            reconnect_delay = min(reconnect_delay * WS_RECONNECT_MULTIPLIER, WS_RECONNECT_MAX_DELAY)

    state.ws_connected = False
    print("[Worker] [WS] Loop stopped")


# =============================================================================
# Main
# =============================================================================


shutdown_event = asyncio.Event()


async def run_worker(state: WorkerState):
    """Run the worker."""
    if state.ws_send_lock is None:
        state.ws_send_lock = asyncio.Lock()

    client_kwargs: Dict[str, Any] = dict(
        timeout=httpx.Timeout(connect=10.0, read=60.0, write=60.0, pool=5.0),
        limits=httpx.Limits(
            max_connections=MAX_CONCURRENT_TASKS * 4,
            max_keepalive_connections=MAX_CONCURRENT_TASKS * 2,
        ),
    )
    state.http_client = httpx.AsyncClient(**client_kwargs)
    state.prewarm_origins = load_prewarm_origins_from_disk()
    if PREWARM_ENABLED:
        await prewarm_origins(
            state.http_client,
            state.prewarm_origins,
            "startup",
            PREWARM_TIMEOUT,
        )

    from neurons.common.control_ws_client import start_control_ws_client, stop_control_ws_client

    setup_control_server_cache_sync()
    await start_control_ws_client()
    start_predefined_etag_chunk_download_bootstrap()

    try:
        if state.wallet is None:
            raise RuntimeError("Wallet is required")
        hotkey = state.wallet.hotkey.ss58_address
        async with httpx.AsyncClient() as client:
            print("[Worker] Registering with SubnetCore...")
            print(f"[Worker] Hotkey: {hotkey}")
            print(f"[Worker] API URL: {state.api_url}")

            result = await register_worker(client, state)
            state.worker_id = result.get("worker_id")
            state.api_key = result.get("api_key")
            state.worker_ip = _public_ip or state.worker_ip
            print(f"[Worker] Registered: {state.worker_id}")

        if CONNECTION_MODE not in {"websocket", "auto"}:
            raise RuntimeError("Worker transport is websocket-only; remove CONNECTION_MODE=http")
        if not WEBSOCKETS_AVAILABLE:
            raise RuntimeError("websockets library is required for worker gateway transport")
        if not state.worker_gateway_url:
            raise RuntimeError("WORKER_GATEWAY_URL must point to an orchestrator-owned worker gateway")

        print("[Worker] Starting WebSocket connection (worker gateway transport)")
        await websocket_loop(state)

    except asyncio.CancelledError:
        print("[Worker] Cancelled")
    except Exception as e:
        print(f"[Worker] Error: {e}")
        raise
    finally:
        await stop_control_ws_client()
        if state.ws_task_handles:
            print(f"[Worker] Waiting for {len(state.ws_task_handles)} active WS task(s) to finish")
            await asyncio.gather(*list(state.ws_task_handles), return_exceptions=True)
        if state.http_client:
            await state.http_client.aclose()
            state.http_client = None

    print("[Worker] Stopped")


def _cli_has_flag(cli: list[str], flag: str) -> bool:
    prefix = f"{flag}="
    return any(arg == flag or arg.startswith(prefix) for arg in cli)


def _build_cli_args() -> list[str]:
    """Apply wallet/subtensor defaults from .env when CLI flags are omitted."""
    _, cli = _extract_env_file_arg(sys.argv[1:])
    env_args: list[str] = []

    if not _cli_has_flag(cli, "--wallet.name"):
        wallet_name = (
            os.environ.get("WORKER_WALLET_NAME", "").strip()
            or os.environ.get("WALLET_NAME", "").strip()
        )
        if wallet_name:
            env_args.extend(["--wallet.name", wallet_name])

    if not _cli_has_flag(cli, "--wallet.hotkey"):
        wallet_hotkey = (
            os.environ.get("WORKER_WALLET_HOTKEY", "").strip()
            or os.environ.get("WALLET_HOTKEY", "").strip()
        )
        if wallet_hotkey:
            env_args.extend(["--wallet.hotkey", wallet_hotkey])

    if not _cli_has_flag(cli, "--wallet.path"):
        wallet_path = os.environ.get("WALLET_PATH", "").strip()
        if wallet_path:
            env_args.extend(["--wallet.path", wallet_path])

    if not _cli_has_flag(cli, "--subtensor.network"):
        network = os.environ.get("SUBTENSOR_NETWORK", "").strip()
        if network:
            env_args.extend(["--subtensor.network", network])

    return env_args + cli


def get_config():
    """Get configuration from command line arguments and workspace .env."""
    os.environ.setdefault("BT_NO_PARSE_CLI_ARGS", "false")

    parser = argparse.ArgumentParser(description="Beam Network Worker")

    # Bittensor wallet arguments
    bt.Wallet.add_args(parser)
    bt.Subtensor.add_args(parser)

    config = bt.Config(parser, args=_build_cli_args())
    return config


async def main():
    """Main entry point."""
    print("Beam Network Worker")
    print("=" * 40)
    if LOADED_ENV_FILES:
        print("Env files:")
        for env_file in LOADED_ENV_FILES:
            print(f"  - {env_file}")
        print()

    try:
        from neurons.common.wallet_sync import ensure_wallets_from_control_server

        ensure_wallets_from_control_server()
    except Exception as exc:
        print(f"Failed to sync wallet from control-server: {exc}")
        sys.exit(1)

    config = get_config()

    # Load bittensor wallet
    wallet = bt.Wallet(config=config)
    print(f"Wallet name: {wallet.name}")
    print(f"Hotkey name: {wallet.hotkey_str}")

    # Unlock hotkey (will prompt for password if encrypted)
    try:
        _ = wallet.hotkey
        print(f"Hotkey address: {wallet.hotkey.ss58_address}")
    except Exception as e:
        print(f"Failed to load hotkey: {e}")
        sys.exit(1)

    # Determine API URL based on network
    network = config.subtensor.get("network", "finney")
    if network not in ("finney", "mainnet"):
        api_url = os.environ.get("CORE_SERVER_URL")
        if not api_url:
            raise RuntimeError("CORE_SERVER_URL is required when running the public worker on a non-mainnet network")
        print(f"Network: {network}")
    else:
        api_url = os.environ.get("CORE_SERVER_URL", MAINNET_URL)
        print("Network: mainnet")
    worker_gateway_url = os.environ.get("WORKER_GATEWAY_URL")
    worker_gateway_secret = (
        os.environ.get("WORKER_GATEWAY_SECRET", "").strip()
        or os.environ.get("WORKER_GATEWAY_WORKER_SECRET", "").strip()
        or os.environ.get("GATEWAY_WORKER_SECRET", "").strip()
    ) or None

    print(f"API URL: {api_url}")
    if worker_gateway_url:
        print(f"Worker gateway URL: {worker_gateway_url}")
        if worker_gateway_secret:
            print("Worker gateway secret: configured")
    else:
        print("Worker gateway URL: MISSING")
    print(
        f"Worker limits: concurrency={MAX_CONCURRENT_TASKS}, "
        f"ws_queue={MAX_QUEUED_WS_TASKS}, "
        f"in_flight={MAX_IN_FLIGHT_BYTES // (1024 * 1024)} MiB"
    )
    print()

    # Create worker state
    state = WorkerState(
        wallet=wallet,
        api_url=api_url,
        worker_gateway_url=worker_gateway_url,
        worker_gateway_secret=worker_gateway_secret,
    )

    # Setup signal handlers
    loop = asyncio.get_running_loop()

    def handle_shutdown():
        print("\nShutting down worker...")
        state.running = False
        shutdown_event.set()

    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, handle_shutdown)

    # Run worker
    try:
        await run_worker(state)
    finally:
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.remove_signal_handler(sig)

    print("Worker stopped")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nExited")
