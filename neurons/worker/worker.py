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
import tempfile
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from importlib.metadata import PackageNotFoundError, version as package_version
from pathlib import Path
from typing import Any, Dict, Optional
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
    try:
        log_dir.mkdir(parents=True, exist_ok=True)
    except OSError as exc:
        print(f"[Worker] Cannot create log dir {log_dir}: {exc}", file=original_stderr)
        return

    log_format = "%(asctime)s.%(msecs)03.0f | %(levelname)s | %(message)s"
    log_datefmt = "%Y-%m-%d %H:%M:%S"
    formatter = logging.Formatter(log_format, datefmt=log_datefmt)

    log_path = log_dir / f"{instance}.log"
    try:
        file_handler = logging.FileHandler(log_path)
    except PermissionError:
        # Common after `sudo … --foreground`: root-owned log; systemd User=ubuntu cannot append.
        print(
            f"[Worker] Permission denied writing {log_path}. "
            f"Fix with: sudo chown -R \"$(id -un)\":\"$(id -gn)\" \"{log_dir}\" "
            f"(or re-run ./scripts/run-worker.sh which chowns logs). "
            "Continuing with journal/stderr only.",
            file=original_stderr,
        )
        return

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
# Orch scheduling capacity (worker_hello.max_concurrent_tasks). Default = queue
# depth so the gateway may deliver up to N offers while this process still runs
# only MAX_CONCURRENT_TASKS at a time (one-by-one when CONCURRENT=1).
_advertised = os.environ.get("WORKER_ADVERTISED_MAX_TASKS", "").strip()
try:
    if _advertised:
        ADVERTISED_MAX_TASKS = max(1, int(_advertised))
    else:
        ADVERTISED_MAX_TASKS = max(MAX_CONCURRENT_TASKS, MAX_QUEUED_WS_TASKS)
except ValueError:
    ADVERTISED_MAX_TASKS = max(MAX_CONCURRENT_TASKS, MAX_QUEUED_WS_TASKS)
# Keep local accept limit at least as large as what we advertise to orch.
MAX_QUEUED_WS_TASKS = max(MAX_QUEUED_WS_TASKS, ADVERTISED_MAX_TASKS)
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
PREDEFINED_ETAG_RANGE_DATA_DIRNAME = "predefined_etag_range_data"
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


WS_TASK_RESULT_ACK_TIMEOUT = float(os.environ.get("WORKER_TASK_RESULT_ACK_TIMEOUT", "45.0"))
WS_TASK_RESULT_SEND_ATTEMPTS = max(3, int(os.environ.get("WORKER_TASK_RESULT_SEND_ATTEMPTS", "8")))
WS_TASK_RESULT_RECONNECT_WAIT_SECONDS = max(
    0.0, float(os.environ.get("WORKER_TASK_RESULT_RECONNECT_WAIT_SECONDS", "2.0"))
)
TASK_RESULT_ACK_STATUSES = {
    "owned_processing",
    "completed",
    "failed",
    "late_superseded",
    "late_expired",
    "retry",
    "rejected",
}
TASK_RESULT_TERMINAL_STATUSES = TASK_RESULT_ACK_STATUSES - {"retry"}
WORKER_PREDEFINED_ETAG_EARLY_SUBMIT = _env_bool("WORKER_PREDEFINED_ETAG_EARLY_SUBMIT", True)
# true: use local range_data/cache when coverage exists; false: always fetch from source.
WORKER_USE_CACHE_FILE = _env_bool("WORKER_USE_CACHE_FILE", True)
# Log disk vs network breakdown for cache→PUT (diagnose rotating slow workers).
WORKER_UPLOAD_PERF_LOG = _env_bool("WORKER_UPLOAD_PERF_LOG", True)
# When >0, always emit upload_perf; when set, also tag slow runs below this Mbps.
try:
    WORKER_UPLOAD_PERF_SLOW_MBPS = float(
        os.environ.get("WORKER_UPLOAD_PERF_SLOW_MBPS", "150")
    )
except ValueError:
    WORKER_UPLOAD_PERF_SLOW_MBPS = 150.0
# Abort cache_stream PUT when cumulative net_wait_ms exceeds this (0 = disabled).
# Orch reassigns the offer to another worker on error prefix slow_net_wait:.
try:
    WORKER_NET_WAIT_ABORT_MS = max(
        0.0, float(os.environ.get("WORKER_NET_WAIT_ABORT_MS", "2000"))
    )
except ValueError:
    WORKER_NET_WAIT_ABORT_MS = 2000.0
# true: compute sha256 and verify against offer chunk_hash/chunk_hashes when present.
WORKER_VERIFY_CHUNK_HASH = _env_bool("WORKER_VERIFY_CHUNK_HASH", True)
PREDEFINED_ETAG_MAX_PARALLEL = max(
    1, int(os.environ.get("WORKER_PREDEFINED_ETAG_MAX_PARALLEL", "1"))
)
# When false (default), chunk .bin files download on task demand only — not on WS snapshot.
WORKER_PREDEFINED_ETAG_AUTO_DOWNLOAD_CHUNKS = _env_bool(
    "WORKER_PREDEFINED_ETAG_AUTO_DOWNLOAD_CHUNKS", False
)
try:
    WORKER_PREDEFINED_ETAG_BOOTSTRAP_MAX_DOWNLOADS = max(
        0, int(os.environ.get("WORKER_PREDEFINED_ETAG_BOOTSTRAP_MAX_DOWNLOADS", "0"))
    )
except ValueError:
    WORKER_PREDEFINED_ETAG_BOOTSTRAP_MAX_DOWNLOADS = 0
predefined_etag_fast_path_semaphore = asyncio.Semaphore(PREDEFINED_ETAG_MAX_PARALLEL)
PREWARM_ENABLED = _env_bool("WORKER_PREWARM_ENABLED", True)
PREWARM_TIMEOUT = float(os.environ.get("WORKER_PREWARM_TIMEOUT", "5"))
PREWARM_MAX_ORIGINS = max(1, int(os.environ.get("WORKER_PREWARM_MAX_ORIGINS", "32")))
# Periodic HEAD to learned R2 origins so TLS/pool stay warm between ~30m transfers.
# CDN/R2 idle keepalive is typically ~60–120s; default 180s refreshes before death.
# 0 disables the interval loop (startup + per-task prewarm still run).
try:
    PREWARM_INTERVAL_S = max(
        0.0, float(os.environ.get("WORKER_PREWARM_INTERVAL_S", "180"))
    )
except ValueError:
    PREWARM_INTERVAL_S = 180.0
try:
    WORKER_INITIAL_ORDER = int(os.environ.get("WORKER_INITIAL_ORDER", "0"))
except ValueError:
    WORKER_INITIAL_ORDER = 0
# When false, reject offers that are not covered by local range cache so the
# orchestrator can re-offer to a worker with WORKER_NON_CACHED_FILE=true.
WORKER_NON_CACHED_FILE = _env_bool("WORKER_NON_CACHED_FILE", True)
CACHE_MISS_NOT_ACCEPTED = "cache_miss_not_accepted"


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
            _, start, end = _transfer_byte_range(transfer_context)
            return int(end - start + 1)
        except (KeyError, TypeError, ValueError, OSError):
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


async def _read_file_chunks(path: Path, chunk_size: int = FETCH_STREAM_CHUNK_SIZE):
    """Stream a file in fixed-size chunks (avoids loading whole chunk into RAM)."""
    with path.open("rb") as handle:
        while True:
            part = handle.read(chunk_size)
            if not part:
                break
            yield part


async def upload_buffered_predefined_etag(
    client: httpx.AsyncClient,
    *,
    destination_url: str,
    body: Any,
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
    quiet: bool = False,
) -> tuple[float, Optional[str]]:
    """PUT/POST a buffered predefined-etag chunk; returns (send_ms, etag from response).

    Callers that pass a one-shot stream should wrap retries themselves (see
    ``stream_cache_upload_to_dest``). Bytes/bytearray bodies may be retried by the
    caller by invoking this function again.
    """
    is_object_storage = is_object_storage_presigned_url(destination_url)
    body_len = len(body) if isinstance(body, (bytes, bytearray)) else int(expected_max_bytes or 0)
    byte_to = (
        upload_offset + expected_max_bytes - 1
        if expected_max_bytes and expected_max_bytes > 0
        else upload_offset + body_len - 1
    )
    if not quiet:
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
    if isinstance(body, (bytes, bytearray)) and PREDEFINED_ETAG_MAX_SPEED_MBPS > 0:
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
            "X-Length": str(expected_max_bytes or body_len),
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
    if not quiet:
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
    return send_ms, etag


async def run_predefined_etag_background_upload(
    client: httpx.AsyncClient,
    fetch_ready: FetchReadyState,
    transfer_context: dict,
    *,
    task_id: str = None,
    offer_id: str = None,
    log_prefix: str = "[Worker]",
) -> bool:
    """Start upload as soon as the buffered download finishes."""
    await fetch_ready.event.wait()
    if fetch_ready.error or not fetch_ready.ready:
        return False

    chunk_size = int(transfer_context["chunk_size"])
    range_start = int(transfer_context["range_start"])
    dest_headers = transfer_context.get("dest_headers") or {}
    source_url = str(transfer_context.get("source_url") or "")

    try:
        if fetch_ready.buffer:
            body: Any = fetch_ready.buffer
        elif has_predefined_etag_chunk_data(transfer_context):
            body = _iter_predefined_etag_range_chunks(transfer_context)
        else:
            return False

        send_ms, _etag = await upload_buffered_predefined_etag(
            client,
            destination_url=transfer_context["dest_url"],
            body=body,
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
            f"task={task_label(task_id)} offer={task_label(offer_id)} send_ms={send_ms:.1f}"
        )
        return True
    except Exception as exc:
        print(
            f"[Worker] Predefined ETag background upload failed "
            f"task={task_label(task_id)} offer={task_label(offer_id)} "
            f"error={exception_detail(exc)}{http_status_detail(exc)}"
        )
        return False
    finally:
        fetch_ready.buffer = None


async def upload_predefined_etag_from_local_cache(
    client: httpx.AsyncClient,
    transfer_context: dict,
    chunk_hash: str,
    *,
    task_id: str = None,
    offer_id: str = None,
    log_prefix: str = "[Worker]",
    data: Optional[bytes] = None,
    etag_local: Optional[str] = None,
    skip_hash: bool = False,
) -> tuple[bool, float, Optional[str], Optional[str], Optional[str]]:
    """Upload cached range bytes to dest. Returns (ok, send_ms, etag_real, etag_local, error).

    When skip_hash=True, do not sha256/md5 before PUT (caller hashes in parallel or skips).
    Prefer streaming from disk when ``data`` is not provided so cache hits do not
    materialize the full range in RAM solely for hashing/upload setup.
    """
    if not has_predefined_etag_chunk_data(transfer_context) and not data:
        return False, 0.0, None, None, "cache_file_missing"

    resolved_etag_local = etag_local
    computed = chunk_hash or ""
    body = data
    if body is None:
        if not skip_hash:
            hashed = await asyncio.to_thread(
                _hash_predefined_etag_range_from_disk, transfer_context
            )
            if not hashed:
                return False, 0.0, None, None, "cache_file_empty"
            computed, disk_etag = hashed
            resolved_etag_local = etag_local or disk_etag
            if (
                WORKER_VERIFY_CHUNK_HASH
                and chunk_hash
                and computed.lower() != chunk_hash.lower()
            ):
                return False, 0.0, None, resolved_etag_local, "cache_file_hash_mismatch"
        # Stream upload from disk via existing buffered helper only when we must
        # materialize; prefer reading once into body only for the PUT API below.
        body = await asyncio.to_thread(read_predefined_etag_range_bytes, transfer_context)
        if not body:
            return False, 0.0, None, None, "cache_file_empty"
    elif not skip_hash:
        computed = hashlib.sha256(body).hexdigest()
        resolved_etag_local = etag_local or _etag_quoted_md5(body)
        if (
            WORKER_VERIFY_CHUNK_HASH
            and chunk_hash
            and computed.lower() != chunk_hash.lower()
        ):
            return False, 0.0, None, resolved_etag_local, "cache_file_hash_mismatch"

    chunk_size = int(transfer_context["chunk_size"])
    range_start = int(transfer_context["range_start"])
    dest_headers = transfer_context.get("dest_headers") or {}
    source_url = str(transfer_context.get("source_url") or "")

    try:
        send_ms, etag_real = await upload_buffered_predefined_etag(
            client,
            destination_url=transfer_context["dest_url"],
            body=body,
            chunk_hash=computed,
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
            quiet=True,
        )
        return True, send_ms, etag_real, resolved_etag_local, None
    except Exception as exc:
        return False, 0.0, None, resolved_etag_local, exception_detail(exc)
    finally:
        body = None
        data = None


async def ensure_predefined_etag_chunk_data_local(
    transfer_context: dict,
    chunk_hash: str = "",
) -> bool:
    """Ensure local range bytes exist (disk or control-server download)."""
    if has_predefined_etag_chunk_data(transfer_context):
        return True
    source, start, end = _transfer_byte_range(transfer_context)
    if not source:
        return False
    await _download_predefined_etag_range(source, start, end)
    return has_predefined_etag_chunk_data(transfer_context)


@dataclass
class TaskSummaryAck:
    """BeamCore task_result_ack ownership fields used by the worker runtime."""

    received: bool = False
    status: Optional[str] = None
    reason: Optional[str] = None


def task_label(task_id: Optional[str]) -> str:
    """Short task label for logs."""
    return task_id[:16] if task_id else "unknown"


class SlowNetWaitAbort(Exception):
    """Abort cache→dest PUT when network backpressure (net_wait_ms) is too high."""

    def __init__(self, net_wait_ms: float) -> None:
        self.net_wait_ms = float(net_wait_ms)
        super().__init__(f"slow_net_wait:{self.net_wait_ms:.0f}")


def exception_detail(error: Exception) -> str:
    """Return an exception string that is useful even when str(error) is empty."""
    if isinstance(error, SlowNetWaitAbort):
        return str(error)
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
    if label == "interval":
        return
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


async def prewarm_interval_loop(state: WorkerState) -> None:
    """Refresh DNS/TLS/keepalive on learned origins between sparse transfers.

    With ~30m between waves, idle connections die in ~1–2m. Re-HEAD on an
    interval shorter than that so the next batch is not cold-start.
    """
    if not PREWARM_ENABLED or PREWARM_INTERVAL_S <= 0:
        return
    while state.running:
        try:
            await asyncio.sleep(PREWARM_INTERVAL_S)
        except asyncio.CancelledError:
            return
        if not state.running or not state.http_client:
            return
        if not state.prewarm_origins:
            state.prewarm_origins = load_prewarm_origins_from_disk()
        origins = list(state.prewarm_origins)
        if not origins:
            continue
        try:
            await prewarm_origins(
                state.http_client,
                origins,
                "interval",
                PREWARM_TIMEOUT,
            )
        except asyncio.CancelledError:
            return
        except Exception:
            pass


# =============================================================================
# Transfer Helpers
# =============================================================================


def is_retryable(error: Exception) -> bool:
    """Check if an error is retryable.

    Includes httpx transport failures (ReadError/ConnectError/WriteError/…) which
    commonly appear under concurrent R2 PUTs when the peer resets mid-upload.
    """
    if isinstance(error, SlowNetWaitAbort):
        return False
    if isinstance(error, (asyncio.TimeoutError, TimeoutError)):
        return True
    # TimeoutException is a TransportError; listed first for clarity.
    if isinstance(error, httpx.TimeoutException):
        return True
    if isinstance(error, httpx.TransportError):
        return True
    if isinstance(error, httpx.HTTPStatusError):
        return error.response.status_code >= 500
    if isinstance(error, (ConnectionError, BrokenPipeError)):
        return True
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


def _predefined_etag_range_data_dir() -> Path:
    return _predefined_etag_cache_path().parent / PREDEFINED_ETAG_RANGE_DATA_DIRNAME


_worker_range_store = None
_worker_range_store_consolidated = False


def get_worker_range_store():
    """Lazy worker-local continuous byte-range store."""
    global _worker_range_store, _worker_range_store_consolidated
    if _worker_range_store is None:
        from neurons.common.byte_range_store import ByteRangeStore

        root = _predefined_etag_range_data_dir()
        root.mkdir(parents=True, exist_ok=True)
        _worker_range_store = ByteRangeStore(root)
    if not _worker_range_store_consolidated:
        _worker_range_store_consolidated = True
        try:
            result = _worker_range_store.consolidate_signed_url_orphans()
            if result.get("merged_dirs") or result.get("ingested_segments"):
                print(
                    f"[Worker] Range store orphan merge: "
                    f"merged_dirs={result.get('merged_dirs')} "
                    f"ingested_segments={result.get('ingested_segments')} "
                    f"removed={result.get('removed_dirs')}"
                )
        except Exception as exc:
            print(f"[Worker] Range store orphan merge failed: {exc}")
    return _worker_range_store


def setup_worker_range_store() -> None:
    """Initialize local range_data and merge signed-URL orphan directories."""
    get_worker_range_store()


def predefined_etag_chunk_data_path_for_key(cache_key: str) -> Path:
    """Legacy per-key .bin path (migration fallback only)."""
    digest = hashlib.sha256(str(cache_key).encode()).hexdigest()
    return _predefined_etag_chunk_data_dir() / f"{digest}.bin"


def predefined_etag_chunk_data_path(transfer_context: dict) -> Path:
    return predefined_etag_chunk_data_path_for_key(
        predefined_etag_cache_key(transfer_context)
    )


def _transfer_byte_range(transfer_context: dict) -> tuple[str, int, int]:
    source = normalized_capability_url(str(transfer_context.get("source_url") or ""))
    range_start = int(transfer_context["range_start"])
    range_end = int(transfer_context["range_end"])
    return source, range_start, range_end


def has_predefined_etag_chunk_data(transfer_context: dict) -> bool:
    source, start, end = _transfer_byte_range(transfer_context)
    if source and get_worker_range_store().covers(source, start, end):
        return True
    # Legacy fallback during migration.
    path = predefined_etag_chunk_data_path(transfer_context)
    return path.is_file() and path.stat().st_size > 0


def uses_local_cache_file(transfer_context: dict) -> bool:
    """True when WORKER_USE_CACHE_FILE is on and local range coverage exists."""
    return WORKER_USE_CACHE_FILE and has_predefined_etag_chunk_data(transfer_context)


def save_predefined_etag_chunk_data(transfer_context: dict, data: bytes) -> Optional[Path]:
    """Persist fetched chunk bytes into the continuous range store (merge + ≤1 GiB).

    Writes through a temp file + ingest_from_file so merge packing streams from disk
    instead of keeping a second full-range copy alive during ingest.
    """
    if not data or not predefined_etag_transfer_eligible(transfer_context):
        return None
    source, start, end = _transfer_byte_range(transfer_context)
    if not source:
        return None
    store = get_worker_range_store()
    expected = end - start + 1
    if len(data) != expected:
        return None
    tmp_dir = _predefined_etag_range_data_dir() / ".tmp"
    tmp_dir.mkdir(parents=True, exist_ok=True)
    fd, tmp_name = tempfile.mkstemp(
        prefix=f"ingest_{start}_", suffix=".bin", dir=tmp_dir
    )
    os.close(fd)
    tmp_path = Path(tmp_name)
    try:
        tmp_path.write_bytes(data)
        segment = store.ingest_from_file(source, start, end, tmp_path)
        return store.segment_path(source, segment)
    finally:
        tmp_path.unlink(missing_ok=True)


def save_predefined_etag_chunk_data_from_file(
    transfer_context: dict, path: Path
) -> Optional[Path]:
    """Persist an on-disk range file into the continuous store without a RAM join."""
    if not predefined_etag_transfer_eligible(transfer_context):
        return None
    source, start, end = _transfer_byte_range(transfer_context)
    if not source:
        return None
    src = Path(path)
    if not src.is_file():
        return None
    store = get_worker_range_store()
    segment = store.ingest_from_file(source, start, end, src)
    return store.segment_path(source, segment)


async def _iter_predefined_etag_range_chunks(transfer_context: dict):
    """Async-iterate cached range bytes (continuous store or legacy file)."""
    source, start, end = _transfer_byte_range(transfer_context)
    store = get_worker_range_store()
    if source and store.covers(source, start, end):
        iterator = store.iter_slice(
            source, start, end, chunk_size=FETCH_STREAM_CHUNK_SIZE
        )
        if iterator is not None:
            for part in iterator:
                yield part
            return
    path = predefined_etag_chunk_data_path(transfer_context)
    if path.is_file() and path.stat().st_size > 0:
        async for part in _read_file_chunks(path):
            yield part


def _cache_range_layout(transfer_context: dict) -> dict:
    """Describe how a cached range is stored (segment count / 1GiB cross)."""
    gib = 1 << 30
    source, start, end = _transfer_byte_range(transfer_context)
    cross_gib = (start // gib) != (end // gib)
    layout: dict = {
        "backend": "miss",
        "seg_n": 0,
        "cross_gib": cross_gib,
        "gib_span": f"{start // gib}-{end // gib}",
        "files": "-",
    }
    if not source:
        return layout
    store = get_worker_range_store()
    if store.covers(source, start, end):
        covering = store.find_covering_segments(source, start, end)
        if covering:
            names: list[str] = []
            for seg in covering:
                path = store.segment_path(source, seg)
                names.append(path.name)
            layout.update(
                {
                    "backend": "range_store",
                    "seg_n": len(covering),
                    "files": ",".join(names),
                }
            )
            return layout
    path = predefined_etag_chunk_data_path(transfer_context)
    if path.is_file() and path.stat().st_size > 0:
        layout.update(
            {
                "backend": "legacy_file",
                "seg_n": 1,
                "files": path.name,
            }
        )
    return layout


def _log_upload_perf(
    *,
    log_prefix: str,
    task_id: Optional[str],
    offer_id: Optional[str],
    transfer_context: dict,
    state: Optional["WorkerState"],
    send_ms: float,
    bytes_sent: int,
    parts: int,
    disk_ms: float,
    net_wait_ms: float,
    first_byte_ms: Optional[float],
    layout: dict,
    ok: bool,
    error: Optional[str] = None,
    attempt: int = 1,
) -> None:
    """One-line disk vs network breakdown for cache_stream PUTs."""
    if not WORKER_UPLOAD_PERF_LOG:
        return
    mbps = transfer_mbps(bytes_sent, send_ms) if send_ms > 0 else 0.0
    disk_mbps = transfer_mbps(bytes_sent, disk_ms) if disk_ms > 0 else 0.0
    # Bound guess: if disk could have fed much faster than PUT, network/R2 dominates.
    if send_ms <= 0:
        bound = "unknown"
    elif disk_ms <= 0:
        bound = "network"
    elif disk_ms >= send_ms * 0.6:
        bound = "disk"
    elif (net_wait_ms / max(send_ms, 1e-6)) >= 0.5:
        bound = "network"
    else:
        bound = "mixed"
    slow = (
        WORKER_UPLOAD_PERF_SLOW_MBPS > 0 and mbps > 0 and mbps < WORKER_UPLOAD_PERF_SLOW_MBPS
    )
    dest_host = "-"
    try:
        dest_host = str(
            object_storage_route_context(str(transfer_context.get("dest_url") or "")).get(
                "destination_host"
            )
            or "-"
        )
    except Exception:
        dest_host = "-"
    dest_path = "-"
    try:
        dest_url = str(transfer_context.get("dest_url") or "")
        parts_url = urlsplit(dest_url)
        segs = [s for s in parts_url.path.split("/") if s]
        dest_path = "/".join(segs[-3:]) if segs else "-"
    except Exception:
        dest_path = "-"
    worker_ip = (state.worker_ip if state is not None else None) or "-"
    in_flight = "-"
    if state is not None:
        in_flight = f"{len(state.active_ws_task_ids)}/{MAX_CONCURRENT_TASKS}"
    chunk_id = chunk_id_from_transfer_context(transfer_context)
    print(
        f"{log_prefix} upload_perf task={task_label(task_id)} offer={task_label(offer_id)} "
        f"chunk_id={chunk_id if chunk_id is not None else '?'} "
        f"ok={str(ok).lower()} slow={str(slow).lower()} bound={bound} "
        f"mbps={mbps:.1f} send_ms={send_ms:.1f} "
        f"first_byte_ms={(first_byte_ms if first_byte_ms is not None else -1):.1f} "
        f"disk_ms={disk_ms:.1f} disk_mbps={disk_mbps:.1f} "
        f"net_wait_ms={net_wait_ms:.1f} "
        f"bytes={bytes_sent} parts={parts} attempt={attempt} "
        f"cache={layout.get('backend')} seg_n={layout.get('seg_n')} "
        f"cross_gib={str(layout.get('cross_gib')).lower()} "
        f"gib_span={layout.get('gib_span')} files={layout.get('files')} "
        f"worker_ip={worker_ip} in_flight={in_flight} "
        f"dest_host={dest_host} dest={dest_path}"
        + (f" error={error}" if error else "")
    )


async def _hash_cache_stream(
    transfer_context: dict,
    *,
    algo: str,
) -> str:
    """One-pass disk stream hash (no full-buffer load). algo: sha256|md5."""
    hasher = hashlib.sha256() if algo == "sha256" else hashlib.md5()
    async for part in _iter_predefined_etag_range_chunks(transfer_context):
        hasher.update(part)
    digest = hasher.hexdigest()
    if algo == "md5":
        return f'"{digest}"'
    return digest


def _sync_iter_predefined_etag_range_chunks(transfer_context: dict):
    """Sync iterator over cached range bytes (for timed disk reads during PUT)."""
    source, start, end = _transfer_byte_range(transfer_context)
    store = get_worker_range_store()
    if source and store.covers(source, start, end):
        iterator = store.iter_slice(
            source, start, end, chunk_size=FETCH_STREAM_CHUNK_SIZE
        )
        if iterator is not None:
            yield from iterator
            return
    path = predefined_etag_chunk_data_path(transfer_context)
    if path.is_file() and path.stat().st_size > 0:
        with path.open("rb") as handle:
            while True:
                part = handle.read(FETCH_STREAM_CHUNK_SIZE)
                if not part:
                    break
                yield part


async def stream_cache_upload_to_dest(
    state: WorkerState,
    transfer_context: dict,
    *,
    chunk_hash: str = "",
    task_id: str = None,
    offer_id: str = None,
    log_prefix: str = "[Worker]",
) -> tuple[bool, float, Optional[str], Optional[str]]:
    """Stream local cache → dest PUT (no full load into RAM). Returns ok, send_ms, etag, error."""
    chunk_size = int(transfer_context["chunk_size"])
    range_start = int(transfer_context["range_start"])
    dest_headers = transfer_context.get("dest_headers") or {}
    source_url = str(transfer_context.get("source_url") or "")
    dest_url = str(transfer_context["dest_url"])
    layout = _cache_range_layout(transfer_context)
    perf = {
        "bytes": 0,
        "parts": 0,
        "disk_ms": 0.0,
        "net_wait_ms": 0.0,
        "first_byte_ms": None,
    }
    put_t0: dict[str, Optional[float]] = {"t": None}

    async def body_stream():
        rate_limited = PREDEFINED_ETAG_MAX_SPEED_MBPS > 0
        bytes_per_sec = (
            PREDEFINED_ETAG_MAX_SPEED_MBPS * 1_000_000 / 8 if rate_limited else 0.0
        )
        sync_parts = _sync_iter_predefined_etag_range_chunks(transfer_context)
        while True:
            t_read0 = time.perf_counter()
            try:
                part = next(sync_parts)
            except StopIteration:
                break
            # Time spent reading this chunk from disk/segment files.
            perf["disk_ms"] += (time.perf_counter() - t_read0) * 1000
            if put_t0["t"] is not None and perf["first_byte_ms"] is None:
                perf["first_byte_ms"] = (time.perf_counter() - put_t0["t"]) * 1000
            perf["bytes"] += len(part)
            perf["parts"] += 1
            t_yield = time.perf_counter()
            yield part
            # Time until httpx asks for the next chunk ≈ network/R2 backpressure.
            perf["net_wait_ms"] += (time.perf_counter() - t_yield) * 1000
            if (
                WORKER_NET_WAIT_ABORT_MS > 0
                and perf["net_wait_ms"] > WORKER_NET_WAIT_ABORT_MS
            ):
                raise SlowNetWaitAbort(perf["net_wait_ms"])
            if rate_limited and bytes_per_sec > 0:
                await asyncio.sleep(len(part) / bytes_per_sec)

    async def _put_once(client: httpx.AsyncClient) -> tuple[float, Optional[str]]:
        put_t0["t"] = time.perf_counter()
        perf["bytes"] = 0
        perf["parts"] = 0
        perf["disk_ms"] = 0.0
        perf["net_wait_ms"] = 0.0
        perf["first_byte_ms"] = None
        return await upload_buffered_predefined_etag(
            client,
            destination_url=dest_url,
            body=body_stream(),
            chunk_hash=chunk_hash or "",
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
            quiet=True,
        )

    try:
        client = state.http_client
        last_error: Optional[BaseException] = None
        for attempt in range(MAX_RETRIES):
            try:
                if client is not None:
                    send_ms, etag = await _put_once(client)
                else:
                    async with httpx.AsyncClient(timeout=SEND_TIMEOUT) as tmp_client:
                        send_ms, etag = await _put_once(tmp_client)
                _log_upload_perf(
                    log_prefix=log_prefix,
                    task_id=task_id,
                    offer_id=offer_id,
                    transfer_context=transfer_context,
                    state=state,
                    send_ms=send_ms,
                    bytes_sent=int(perf["bytes"] or chunk_size),
                    parts=int(perf["parts"]),
                    disk_ms=float(perf["disk_ms"]),
                    net_wait_ms=float(perf["net_wait_ms"]),
                    first_byte_ms=perf["first_byte_ms"],
                    layout=layout,
                    ok=True,
                    attempt=attempt + 1,
                )
                return True, send_ms, etag, None
            except Exception as exc:
                last_error = exc
                send_ms_fail = 0.0
                if put_t0["t"] is not None:
                    send_ms_fail = (time.perf_counter() - put_t0["t"]) * 1000
                can_retry = is_retryable(exc) and attempt < MAX_RETRIES - 1
                if is_object_storage_presigned_url(dest_url) and (
                    not can_retry or attempt == MAX_RETRIES - 1
                ):
                    print(
                        "[Worker] Object storage upload failed "
                        f"task={task_label(task_id)} offer={task_label(offer_id)} "
                        f"chunk=0 error={exception_detail(exc)}"
                        f"{http_status_detail(exc)}"
                        f"{format_route_context(object_storage_route_context(dest_url))}"
                    )
                if not can_retry:
                    _log_upload_perf(
                        log_prefix=log_prefix,
                        task_id=task_id,
                        offer_id=offer_id,
                        transfer_context=transfer_context,
                        state=state,
                        send_ms=send_ms_fail,
                        bytes_sent=int(perf["bytes"] or 0),
                        parts=int(perf["parts"]),
                        disk_ms=float(perf["disk_ms"]),
                        net_wait_ms=float(perf["net_wait_ms"]),
                        first_byte_ms=perf["first_byte_ms"],
                        layout=layout,
                        ok=False,
                        error=exception_detail(exc),
                        attempt=attempt + 1,
                    )
                    return False, 0.0, None, exception_detail(exc)
                print(
                    "[Worker] Transfer retry "
                    f"task={task_label(task_id)} offer={task_label(offer_id)} "
                    f"chunk=0 attempt={attempt + 1}/{MAX_RETRIES} "
                    f"error={exception_detail(exc)}{http_status_detail(exc)}"
                )
                await asyncio.sleep(RETRY_BACKOFF * (2**attempt))
        return False, 0.0, None, exception_detail(last_error or Exception("upload_failed"))
    except Exception as exc:
        return False, 0.0, None, exception_detail(exc)


def read_predefined_etag_range_bytes(transfer_context: dict) -> Optional[bytes]:
    source, start, end = _transfer_byte_range(transfer_context)
    if source:
        data = get_worker_range_store().read_slice(source, start, end)
        if data is not None:
            return data
    path = predefined_etag_chunk_data_path(transfer_context)
    if path.is_file() and path.stat().st_size > 0:
        return path.read_bytes()
    return None


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
    """Background: wait, then upload range bytes to control-server (coverage via segments.json)."""
    from neurons.common.byte_range_store import normalize_source_url

    source_url = normalize_source_url(str(transfer_context.get("source_url") or ""))
    try:
        start = int(transfer_context["range_start"])
        end = int(transfer_context["range_end"])
    except (KeyError, TypeError, ValueError):
        print(
            f"[Worker] Deferred range sync skipped (bad range) "
            f"key={predefined_etag_cache_key(transfer_context)[:96]}"
        )
        return
    if not source_url:
        print(
            f"[Worker] Deferred range sync skipped (empty source) "
            f"key={predefined_etag_cache_key(transfer_context)[:96]}"
        )
        return
    if delay_sec > 0:
        await asyncio.sleep(delay_sec)

    uploaded = await sync_range_to_control_server(transfer_context)
    print(
        f"[Worker] Deferred range sync done src={source_url[:96]} "
        f"range={start}-{end} uploaded={'yes' if uploaded else 'no'} "
        f"(delay={delay_sec:.0f}s)"
    )


def schedule_deferred_predefined_etag_cache_sync(
    transfer_context: dict,
    chunk_hash: str,
    etag: Optional[str] = None,
    *,
    delay_sec: Optional[float] = None,
) -> None:
    """Queue background range sync to control-server after CONTROL_SERVER_CACHE_SYNC_DELAY_SEC.

    Does not block task_result / early submit. Default delay comes from env (often 60–150s).
    chunk_hash may be empty; deferred upload hashes from local range_data.
    """
    resolved_delay = (
        CONTROL_SERVER_CACHE_SYNC_DELAY_SEC
        if delay_sec is None
        else max(0.0, float(delay_sec))
    )
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        if chunk_hash:
            key = predefined_etag_cache_key(transfer_context)
            _push_predefined_etag_cache_to_control_server(
                key, chunk_hash, etag or PREDEFINED_ETAG
            )
        return

    loop.create_task(
        _deferred_predefined_etag_cache_sync(
            transfer_context,
            chunk_hash or "",
            etag,
            resolved_delay,
        ),
        name="deferred-predefined-etag-cache-sync",
    )
    print(
        f"[Worker] Scheduled deferred range sync delay={resolved_delay:.0f}s "
        f"key={predefined_etag_cache_key(transfer_context)[:96]}"
    )


_CHUNK_DOWNLOAD_IN_FLIGHT: set[str] = set()
try:
    _CHUNK_DOWNLOAD_PARALLEL = max(
        1, int(os.environ.get("WORKER_PREDEFINED_ETAG_CHUNK_DOWNLOAD_PARALLEL", "1"))
    )
except ValueError:
    _CHUNK_DOWNLOAD_PARALLEL = 1
_chunk_download_semaphore: Optional[asyncio.Semaphore] = None


def _chunk_download_semaphore_get() -> asyncio.Semaphore:
    global _chunk_download_semaphore
    if _chunk_download_semaphore is None:
        _chunk_download_semaphore = asyncio.Semaphore(_CHUNK_DOWNLOAD_PARALLEL)
    return _chunk_download_semaphore


def schedule_predefined_etag_range_download(
    source_url: str,
    start: int,
    end: int,
    *,
    force: bool = False,
) -> None:
    """Background-fetch range bytes from control-server when local coverage is missing."""
    from neurons.common.byte_range_store import normalize_source_url

    source_url = normalize_source_url(source_url)
    if not force and not WORKER_PREDEFINED_ETAG_AUTO_DOWNLOAD_CHUNKS:
        return
    if not source_url or end < start:
        return
    if get_worker_range_store().covers(source_url, start, end):
        return
    flight_key = f"{source_url}|{start}|{end}"
    if flight_key in _CHUNK_DOWNLOAD_IN_FLIGHT:
        return
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        return
    _CHUNK_DOWNLOAD_IN_FLIGHT.add(flight_key)
    loop.create_task(
        _run_predefined_etag_range_download(source_url, start, end, flight_key),
        name="download-predefined-etag-range",
    )


def schedule_predefined_etag_chunk_data_download(
    cache_key: str,
    chunk_hash: str = "",
    *,
    force: bool = False,
) -> None:
    """Legacy wrapper: map source|start|end key to range download (hash unused)."""
    from neurons.common.byte_range_store import parse_cache_key_range

    parsed = parse_cache_key_range(cache_key)
    if parsed is None:
        return
    source, start, end = parsed
    schedule_predefined_etag_range_download(source, start, end, force=force)


async def _run_predefined_etag_range_download(
    source_url: str, start: int, end: int, flight_key: str
) -> None:
    try:
        async with _chunk_download_semaphore_get():
            await _download_predefined_etag_range(source_url, start, end)
    finally:
        _CHUNK_DOWNLOAD_IN_FLIGHT.discard(flight_key)


async def _download_predefined_etag_range(
    source_url: str, start: int, end: int
) -> None:
    """Stream range from control-server to a temp file, then ingest without full RAM load."""
    if get_worker_range_store().covers(source_url, start, end):
        return
    expected = end - start + 1
    tmp_dir = _predefined_etag_range_data_dir() / ".tmp"
    tmp_dir.mkdir(parents=True, exist_ok=True)
    fd, tmp_name = tempfile.mkstemp(
        prefix=f"dl_{start}_", suffix=".bin", dir=tmp_dir
    )
    os.close(fd)
    tmp_path = Path(tmp_name)
    try:
        from neurons.common.control_client import fetch_predefined_etag_range_to_file

        ok = await asyncio.to_thread(
            fetch_predefined_etag_range_to_file,
            source_url,
            start,
            end,
            tmp_path,
        )
        if not ok:
            return
        if tmp_path.stat().st_size != expected:
            print(
                f"[Worker] Range size mismatch src={source_url[:96]} "
                f"range={start}-{end} got={tmp_path.stat().st_size} expected={expected}"
            )
            return
        await asyncio.to_thread(
            get_worker_range_store().ingest_from_file,
            source_url,
            start,
            end,
            tmp_path,
        )
        print(
            f"[Worker] Range cached in range store src={source_url[:96]} "
            f"range={start}-{end} bytes={expected}"
        )
    except Exception as exc:
        print(
            f"[Worker] Range download failed src={source_url[:96]} "
            f"range={start}-{end}: {exc}"
        )
    finally:
        tmp_path.unlink(missing_ok=True)


def apply_range_coverage_snapshot(sources: list[dict]) -> None:
    """On WS connect: pull missing segments from control-server coverage."""
    max_downloads = WORKER_PREDEFINED_ETAG_BOOTSTRAP_MAX_DOWNLOADS
    force = max_downloads > 0 or WORKER_PREDEFINED_ETAG_AUTO_DOWNLOAD_CHUNKS
    if not force:
        return
    scheduled = 0
    for item in sources or []:
        source_url = str(item.get("source_url") or "").strip()
        if not source_url:
            continue
        for seg in item.get("segments") or []:
            if max_downloads > 0 and scheduled >= max_downloads:
                break
            try:
                start = int(seg["start"])
                end = int(seg["end"])
            except (KeyError, TypeError, ValueError):
                continue
            if get_worker_range_store().covers(source_url, start, end):
                continue
            schedule_predefined_etag_range_download(
                source_url, start, end, force=True
            )
            scheduled += 1
        if max_downloads > 0 and scheduled >= max_downloads:
            break
    if scheduled:
        print(
            f"[Worker] Range snapshot: scheduled {scheduled} missing segment downloads"
        )


async def sync_range_to_control_server(transfer_context: dict) -> bool:
    """Upload local range bytes to control-server and announce coverage.

    Streams from the range store (or a legacy cache file) so a non-cached miss
    that later syncs does not re-load the full range into a long-lived ``bytes``.
    """
    from neurons.common.byte_range_store import normalize_source_url

    source_url = normalize_source_url(str(transfer_context.get("source_url") or ""))
    try:
        start = int(transfer_context["range_start"])
        end = int(transfer_context["range_end"])
    except (KeyError, TypeError, ValueError):
        return False
    if not source_url:
        return False
    try:
        from neurons.common.control_client import (
            upload_predefined_etag_range_from_file,
            upload_predefined_etag_range_from_store,
        )

        store = get_worker_range_store()
        if store.covers(source_url, start, end):
            uploaded = await asyncio.to_thread(
                upload_predefined_etag_range_from_store,
                source_url,
                start,
                end,
                store,
                etag="",
            )
        else:
            path = predefined_etag_chunk_data_path(transfer_context)
            if not path.is_file() or path.stat().st_size != (end - start + 1):
                return False
            uploaded = await asyncio.to_thread(
                upload_predefined_etag_range_from_file,
                source_url,
                start,
                end,
                path,
                etag="",
            )
        if uploaded:
            from neurons.common import control_ws_client

            control_ws_client.schedule_range_update(source_url, start, end)
        return uploaded
    except Exception as exc:
        print(f"[Worker] Range sync to control-server failed: {exc}")
        return False


def apply_range_coverage_broadcast(source_url: str, start: int, end: int) -> None:
    """On WS broadcast: download new coverage from control-server and merge locally."""
    if not source_url or end < start:
        return
    schedule_predefined_etag_range_download(source_url, start, end, force=True)


def bootstrap_missing_predefined_etag_chunk_files() -> int:
    """No-op: bootstrap is driven by range_snapshot from control-server WS."""
    return 0


def setup_control_server_cache_sync() -> None:
    setup_worker_range_store()
    from neurons.common import control_ws_client

    control_ws_client.register_range_snapshot_handler(apply_range_coverage_snapshot)
    control_ws_client.register_range_broadcast_handler(apply_range_coverage_broadcast)


def start_predefined_etag_chunk_download_bootstrap() -> None:
    """Range downloads are scheduled from range_snapshot (see setup_control_server_cache_sync)."""
    return


def get_predefined_etag_cache(
    transfer_context: dict,
) -> Optional[PredefinedETagChunkCacheEntry]:
    entry, _source = resolve_predefined_etag_cache(transfer_context)
    return entry


def predefined_etag_known_source(transfer_context: dict) -> Optional[str]:
    """Return how hash/etag were resolved: env, range_data, or None."""
    _entry, source = resolve_predefined_etag_cache(transfer_context)
    return source


def _etag_quoted_md5(data: bytes) -> str:
    """R2/S3 single-PUT ETag: quoted MD5 of the exact uploaded bytes."""
    return f'"{hashlib.md5(data).hexdigest()}"'


def _hash_predefined_etag_range_from_disk(
    transfer_context: dict,
) -> Optional[tuple[str, str]]:
    """Return (sha256_hex, quoted_md5_etag) by streaming disk, or None on miss."""
    source, start, end = _transfer_byte_range(transfer_context)
    expected = end - start + 1
    if expected <= 0:
        return None
    sha = hashlib.sha256()
    md5 = hashlib.md5()
    written = 0
    store = get_worker_range_store()
    if source and store.covers(source, start, end):
        iterator = store.iter_slice(
            source, start, end, chunk_size=FETCH_STREAM_CHUNK_SIZE
        )
        if iterator is None:
            return None
        for part in iterator:
            sha.update(part)
            md5.update(part)
            written += len(part)
    else:
        path = predefined_etag_chunk_data_path(transfer_context)
        if not path.is_file() or path.stat().st_size != expected:
            return None
        with path.open("rb") as handle:
            while True:
                part = handle.read(FETCH_STREAM_CHUNK_SIZE)
                if not part:
                    break
                sha.update(part)
                md5.update(part)
                written += len(part)
    if written != expected:
        return None
    return sha.hexdigest(), f'"{md5.hexdigest()}"'


def derive_predefined_etag_from_range_data(
    transfer_context: dict,
) -> Optional[PredefinedETagChunkCacheEntry]:
    """If range_data covers this task, stream-hash chunk_hash + etag from disk.

    Always computed from raw bytes (never from predefined_etag_chunks.json).
    """
    if not predefined_etag_transfer_eligible(transfer_context):
        return None
    if not has_predefined_etag_chunk_data(transfer_context):
        return None
    hashed = _hash_predefined_etag_range_from_disk(transfer_context)
    if not hashed:
        return None
    chunk_hash, etag = hashed
    return PredefinedETagChunkCacheEntry(chunk_hash=chunk_hash, etag=etag)


def resolve_predefined_etag_cache(
    transfer_context: dict,
) -> tuple[Optional[PredefinedETagChunkCacheEntry], Optional[str]]:
    """Resolve hash/etag: env → derive from range_data bytes only."""
    env_entry = get_predefined_etag_env_entry(transfer_context)
    if env_entry is not None:
        return env_entry, "env"
    derived = derive_predefined_etag_from_range_data(transfer_context)
    if derived is not None:
        return derived, "range_data"
    return None, None


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


def offer_expected_chunk_hash(offer: dict, chunk_index: int = 0) -> str:
    """Expected sha256 from the offer, if BeamCore provided one."""
    if not isinstance(offer, dict):
        return ""
    raw_map = offer.get("chunk_hashes")
    if isinstance(raw_map, dict):
        for key in (chunk_index, str(chunk_index)):
            value = raw_map.get(key)
            if value:
                return str(value).strip()
    return str(offer.get("chunk_hash") or "").strip()


def _sha256_hex(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _sha256_and_etag_local(data: bytes) -> tuple[str, str]:
    """CPU hash pair for cache-hit path (run in a worker thread)."""
    return _sha256_hex(data), _etag_quoted_md5(data)


def _sha256_file(path: Path) -> str:
    hasher = hashlib.sha256()
    with path.open("rb") as handle:
        while True:
            part = handle.read(FETCH_STREAM_CHUNK_SIZE)
            if not part:
                break
            hasher.update(part)
    return hasher.hexdigest()


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
    """Return True when fetch_and_send_chunk should buffer for predefined-etag path."""
    if fetch_ready is None or is_canary or not is_object_storage:
        return False
    if transfer_context is not None:
        return predefined_etag_transfer_eligible(transfer_context)
    return bool(normalized_capability_url(source_url))


def uses_predefined_etag_early_submit(transfer_context: dict) -> bool:
    """True when the predefined-etag worker path should run (cache hit or miss).

    WORKER_PREDEFINED_ETAG_EARLY_SUBMIT only controls whether cache hits pre-submit
    with a locally computed etag; the path itself runs whenever the transfer is eligible.
    """
    return predefined_etag_transfer_eligible(transfer_context)


def chunk_id_from_transfer_context(transfer_context: dict) -> Optional[int]:
    """chunk_id = range_start // (range_end - range_start + 1 - 1) matching orch relay_log."""
    try:
        range_start = int(transfer_context["range_start"])
        range_end = int(transfer_context["range_end"])
        span = range_end - range_start
        if span <= 0:
            return None
        return range_start // span
    except (KeyError, TypeError, ValueError):
        return None


def resolve_task_path(
    transfer_context: dict,
    *,
    used_cache: Optional[bool] = None,
    send_ms: float = 0.0,
) -> str:
    """Label execution path for logs: cache_early | cache_stream | miss | standard."""
    if used_cache is not None:
        if used_cache:
            if WORKER_PREDEFINED_ETAG_EARLY_SUBMIT and send_ms <= 0:
                return "cache_early"
            return "cache_stream"
        if uses_predefined_etag_early_submit(transfer_context):
            return "miss"
        return "standard"

    if not uses_predefined_etag_early_submit(transfer_context):
        return "standard"
    if uses_local_cache_file(transfer_context):
        if WORKER_PREDEFINED_ETAG_EARLY_SUBMIT:
            return "cache_early"
        return "cache_stream"
    return "miss"


def format_full_task_info(
    task: Optional[dict] = None,
    transfer_context: Optional[dict] = None,
) -> str:
    """Serialize full task/transfer fields for logs (URLs not redacted)."""
    payload: dict[str, Any] = {}
    preferred = (
        "task_id",
        "offer_id",
        "transfer_id",
        "source_url",
        "dest_url",
        "chunk_size",
        "chunk_hash",
        "chunk_hashes",
        "etag_required",
        "deadline_us",
        "source_headers",
        "dest_headers",
        "signed_url_flow",
        "minimum_worker_version",
        "total_size",
        "total_bytes",
        "file_size",
        "worker_id",
        "batch_id",
    )
    if isinstance(task, dict):
        for key in preferred:
            if key in task and task[key] is not None:
                payload[key] = task[key]
        for key, value in task.items():
            if key in payload or key == "type":
                continue
            payload[key] = value
    if isinstance(transfer_context, dict):
        payload["transfer_context"] = {
            "source_url": transfer_context.get("source_url"),
            "dest_url": transfer_context.get("dest_url"),
            "range_start": transfer_context.get("range_start"),
            "range_end": transfer_context.get("range_end"),
            "chunk_size": transfer_context.get("chunk_size"),
            "total_size": transfer_context.get("total_size"),
            "etag_required": transfer_context.get("etag_required"),
            "transfer_id": transfer_context.get("transfer_id"),
            "signed_url_flow": transfer_context.get("signed_url_flow"),
            "minimum_worker_version": transfer_context.get("minimum_worker_version"),
            "source_headers": transfer_context.get("source_headers"),
            "dest_headers": transfer_context.get("dest_headers"),
        }
    return json.dumps(payload, separators=(",", ":"), default=str)


def log_task_start(
    log_prefix: str,
    task_id: str,
    offer_id: str,
    transfer_context: dict,
    *,
    state: Optional["WorkerState"] = None,
    estimated_bytes: int = 0,
    task: Optional[dict] = None,
) -> None:
    """Short task_start log (no signed URLs / no task_info dump)."""
    del task  # kept for call-site compatibility
    try:
        range_start = int(transfer_context["range_start"])
        range_end = int(transfer_context["range_end"])
        range_label = f"bytes={range_start}-{range_end}({range_end - range_start + 1})"
    except (KeyError, TypeError, ValueError):
        range_label = "-"
    chunk_id = chunk_id_from_transfer_context(transfer_context)
    path = resolve_task_path(transfer_context)
    cache_hit = has_predefined_etag_chunk_data(transfer_context)
    fields = [
        f"{log_prefix} task_start task={task_label(task_id)}",
        f"offer={task_label(offer_id)}",
        f"chunk_id={chunk_id if chunk_id is not None else '?'}",
        f"range={range_label}",
        f"path={path}",
        f"cache_hit={str(cache_hit).lower()}",
    ]
    if state is not None:
        fields.append(
            f"in_flight={len(state.active_ws_task_ids)}/"
            f"{MAX_CONCURRENT_TASKS} "
            f"reserved_bytes={state.reserved_bytes}/{MAX_IN_FLIGHT_BYTES} "
            f"reserve={estimated_bytes}"
        )
    print(" ".join(fields))


def predefined_etag_early_submit_skip_reasons(transfer_context: dict) -> list[str]:
    """Explain why the predefined ETag path is not used."""
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


def format_task_offer_log(offer: dict, *, full_urls: bool = False) -> str:
    """Serialize a task offer for logs.

    By default signed URLs are redacted; pass full_urls=True for complete URLs.
    """
    payload = dict(offer)
    if not full_urls:
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
        f"reasons={'; '.join(reasons)} offer_msg={format_task_offer_log(offer, full_urls=True)}"
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
    etag_local: Optional[str] = None
    error: Optional[str] = None
    used_cache: bool = False
    hash_source: str = "computed"
    load_ms: float = 0.0
    hash_ms: float = 0.0  # sha256 only (VERIFY_CHUNK_HASH)
    etag_ms: float = 0.0  # md5 only (EARLY_SUBMIT)
    fetch_ms: float = 0.0
    send_ms: float = 0.0


def _log_task_done(
    log_prefix: str,
    task_id: str,
    offer_id: str,
    transfer_context: dict,
    *,
    chunk_hash: str = "",
    etag_real: str = "",
    etag_local: str = "",
    cached: bool = False,
    hash_source: str = "",
    path: str = "",
    load_ms: float = 0.0,
    hash_ms: float = 0.0,
    etag_ms: float = 0.0,
    fetch_ms: float = 0.0,
    send_ms: float = 0.0,
) -> None:
    bytes_count = 0
    try:
        bytes_count = (
            int(transfer_context["range_end"]) - int(transfer_context["range_start"]) + 1
        )
    except (KeyError, TypeError, ValueError):
        bytes_count = int(transfer_context.get("chunk_size") or 0)
    total_ms = load_ms + hash_ms + etag_ms + fetch_ms + send_ms
    # Upload rate uses send_ms only (dest PUT). wall_ms is end-to-end.
    mbps = transfer_mbps(bytes_count, send_ms if send_ms > 0 else total_ms)
    path_label = path or resolve_task_path(
        transfer_context, used_cache=cached, send_ms=send_ms
    )
    chunk_id = chunk_id_from_transfer_context(transfer_context)
    print(
        f"{log_prefix} task_done task={task_label(task_id)} offer={task_label(offer_id)} "
        f"chunk_id={chunk_id if chunk_id is not None else '?'} "
        f"range={transfer_context.get('range_start')}-{transfer_context.get('range_end')} "
        f"bytes={bytes_count} path={path_label} cached={str(cached).lower()} "
        f"hash_source={hash_source or '-'} "
        f"hash={chunk_hash or '-'} etag_real={etag_real or '-'} "
        f"etag_local={etag_local or '-'} "
        f"load_ms={load_ms:.1f} hash_ms={hash_ms:.1f} etag_ms={etag_ms:.1f} "
        f"fetch_ms={fetch_ms:.1f} send_ms={send_ms:.1f} wall_ms={total_ms:.1f} mbps={mbps:.1f}"
    )


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
            f"{kind}, waited {waited_sec:.3f}s "
            f"({format_predefined_etag_min_submit_detail(transfer_context)}) before {result_label}"
        )
    else:
        detail = f"{kind}, submitting {result_label}"
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
    data: Optional[bytes] = None,
    etag_local: Optional[str] = None,
    skip_hash: bool = False,
) -> tuple[bool, float, Optional[str], Optional[str], Optional[str]]:
    """Upload local range bytes to dest. Returns (ok, send_ms, etag_real, etag_local, error)."""
    if not data and not has_predefined_etag_chunk_data(transfer_context):
        return False, 0.0, None, None, "cache_file_missing"

    client = state.http_client
    kwargs = dict(
        transfer_context=transfer_context,
        chunk_hash=chunk_hash,
        task_id=task_id,
        offer_id=offer_id,
        log_prefix=log_prefix,
        data=data,
        etag_local=etag_local,
        skip_hash=skip_hash,
    )
    if client is not None:
        return await upload_predefined_etag_from_local_cache(client, **kwargs)
    async with httpx.AsyncClient(timeout=SEND_TIMEOUT) as tmp_client:
        return await upload_predefined_etag_from_local_cache(tmp_client, **kwargs)


async def predefined_etag_submit_flow(
    state: WorkerState,
    task_id: str,
    offer_id: str,
    offer: dict,
    transfer_context: dict,
    deadline_us: int,
    *,
    log_prefix: str,
    push_cache_to_control_server: bool = True,
    offer_started_at: Optional[float] = None,
    result_label: str = "task_result",
) -> PredefinedETagSubmitOutcome:
    """Predefined ETag submit paths:

    USE_CACHE_FILE=false: always miss path (fetch from source).
    Cache + EARLY_SUBMIT=true:  stream-md5 etag → task_result → background stream PUT
    Cache + EARLY_SUBMIT=false: stream cache→PUT (no full load / no local etag) → response etag
    Miss: fetch+upload → etag from PUT response → defer control-server sync

    WORKER_VERIFY_CHUNK_HASH=true: stream-sha256 and verify against offer expected hash.
    """
    started_at = offer_started_at if offer_started_at is not None else time.perf_counter()

    if uses_local_cache_file(transfer_context):
        hash_ms = 0.0
        etag_ms = 0.0
        load_ms = 0.0
        chunk_hash = ""
        expected = offer_expected_chunk_hash(offer)

        if WORKER_VERIFY_CHUNK_HASH:
            hash_started = time.perf_counter()
            chunk_hash = await _hash_cache_stream(transfer_context, algo="sha256")
            hash_ms = (time.perf_counter() - hash_started) * 1000
            if expected and chunk_hash.lower() != expected.lower():
                return PredefinedETagSubmitOutcome(
                    success=False,
                    chunk_hash=chunk_hash,
                    used_cache=True,
                    error=(
                        f"chunk hash mismatch: expected {expected}, got {chunk_hash}"
                    ),
                    load_ms=0.0,
                    hash_ms=hash_ms,
                )

        if WORKER_PREDEFINED_ETAG_EARLY_SUBMIT:
            # Pre-submit: md5 over stream (no full RAM buffer) then background PUT.
            etag_started = time.perf_counter()
            etag_local = await _hash_cache_stream(transfer_context, algo="md5")
            etag_ms = (time.perf_counter() - etag_started) * 1000
            await wait_predefined_etag_min_submit_delay(started_at, transfer_context)
            return PredefinedETagSubmitOutcome(
                success=True,
                chunk_hash=chunk_hash,
                etag=etag_local,
                etag_local=etag_local,
                used_cache=True,
                hash_source="range_data",
                load_ms=0.0,
                hash_ms=hash_ms,
                etag_ms=etag_ms,
                fetch_ms=0.0,
                send_ms=0.0,
            )

        # EARLY_SUBMIT=false: stream disk→PUT like fetch→PUT; etag from response only.
        ok, send_ms, etag_real, err = await stream_cache_upload_to_dest(
            state,
            transfer_context,
            chunk_hash=chunk_hash,
            task_id=task_id,
            offer_id=offer_id,
            log_prefix=log_prefix,
        )
        if not ok:
            return PredefinedETagSubmitOutcome(
                success=False,
                chunk_hash=chunk_hash,
                used_cache=True,
                error=err or "cache_upload_failed",
                load_ms=0.0,
                hash_ms=hash_ms,
                send_ms=send_ms,
            )
        await wait_predefined_etag_min_submit_delay(started_at, transfer_context)
        return PredefinedETagSubmitOutcome(
            success=True,
            chunk_hash=chunk_hash,
            etag=etag_real,
            etag_local=None,
            used_cache=True,
            hash_source="response_etag",
            load_ms=0.0,
            hash_ms=hash_ms,
            etag_ms=0.0,
            fetch_ms=0.0,
            send_ms=send_ms,
        )

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
            fetch_ms=result.fetch_ms,
            send_ms=result.send_ms,
        )

    # Miss/original: etag from storage PUT response — do not md5 the file for submit.
    if push_cache_to_control_server:
        schedule_deferred_predefined_etag_cache_sync(
            transfer_context,
            result.chunk_hash or "",
            result.etag,
        )

    await wait_predefined_etag_min_submit_delay(started_at, transfer_context)
    return PredefinedETagSubmitOutcome(
        success=True,
        chunk_hash=result.chunk_hash if WORKER_VERIFY_CHUNK_HASH else "",
        etag=result.etag,
        etag_local=None,
        used_cache=False,
        hash_source="response_etag",
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
    """Background PUT after pre-submit (cache hit) or full miss transfer."""
    log_task_chunk_from_context(
        "background_start",
        transfer_context,
        task_id=task_id,
        offer_id=offer_id,
        log_prefix=log_prefix,
    )
    if has_predefined_etag_chunk_data(transfer_context):
        started = time.perf_counter()
        # Stream disk→PUT; do not re-hash (etag already submitted on early path).
        ok, send_ms, etag_real, err = await stream_cache_upload_to_dest(
            state,
            transfer_context,
            chunk_hash="",
            task_id=task_id,
            offer_id=offer_id,
            log_prefix=log_prefix,
        )
        duration_ms = (time.perf_counter() - started) * 1000
        result = TaskExecutionResult(
            success=ok,
            bytes_transferred=predefined_etag_bandwidth_byte_count(transfer_context),
            duration_ms=round(duration_ms, 1),
            chunk_hash="",
            etag=etag_real,
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
        if result.success:
            schedule_deferred_predefined_etag_cache_sync(
                transfer_context,
                result.chunk_hash or "",
                result.etag,
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
                # Spill buffer to disk before hashing/ingest so we do not keep
                # bytearray + bytes + ingest copies of a new (non-cached) range.
                tmp_dir = _predefined_etag_range_data_dir() / ".tmp"
                tmp_dir.mkdir(parents=True, exist_ok=True)
                fd, tmp_name = tempfile.mkstemp(
                    prefix=f"miss_{upload_offset}_",
                    suffix=".bin",
                    dir=tmp_dir,
                )
                os.close(fd)
                tmp_path = Path(tmp_name)
                try:
                    with tmp_path.open("wb") as out:
                        out.write(buffer)
                    del buffer
                    if transfer_context is not None:
                        save_predefined_etag_chunk_data_from_file(
                            transfer_context, tmp_path
                        )
                    chunk_hash = await asyncio.get_running_loop().run_in_executor(
                        None, _sha256_file, tmp_path
                    )
                finally:
                    tmp_path.unlink(missing_ok=True)
                if (
                    WORKER_VERIFY_CHUNK_HASH
                    and expected_chunk_hash
                    and chunk_hash.lower() != expected_chunk_hash.lower()
                ):
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
                # Chunk is on disk; do not retain body in RAM for background upload.
                fetch_ready.signal_ready(
                    bytes_transferred,
                    chunk_hash,
                    fetch_ms,
                    PREDEFINED_ETAG,
                    buffer=None,
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
            chunk_data_tmp: Optional[Path] = None
            if save_chunk_data:
                tmp_dir = _predefined_etag_range_data_dir() / ".tmp"
                tmp_dir.mkdir(parents=True, exist_ok=True)
                chunk_data_tmp = tmp_dir / f"fetch_{os.getpid()}_{time.time_ns()}.bin"

            async def fetch_producer() -> None:
                nonlocal bytes_transferred, fetch_error, fetch_ms
                chunk_data_file = None
                try:
                    if chunk_data_tmp is not None:
                        chunk_data_file = open(chunk_data_tmp, "wb")
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
                if chunk_data_tmp is not None:
                    chunk_data_tmp.unlink(missing_ok=True)
                raise fetch_error

            chunk_hash = hasher.hexdigest()
            if (
                WORKER_VERIFY_CHUNK_HASH
                and expected_chunk_hash
                and chunk_hash.lower() != expected_chunk_hash.lower()
            ):
                send_task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await send_task
                if chunk_data_tmp is not None:
                    chunk_data_tmp.unlink(missing_ok=True)
                raise ValueError(
                    f"chunk hash mismatch: expected {expected_chunk_hash}, got {chunk_hash}"
                )

            if (
                chunk_data_tmp is not None
                and chunk_data_tmp.is_file()
                and transfer_context is not None
            ):
                try:
                    source, range_start, range_end = _transfer_byte_range(transfer_context)
                    if source:
                        await asyncio.to_thread(
                            get_worker_range_store().ingest_from_file,
                            source,
                            range_start,
                            range_end,
                            chunk_data_tmp,
                        )
                finally:
                    chunk_data_tmp.unlink(missing_ok=True)

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


def try_reserve_ws_capacity(
    state: WorkerState, task_id: str, estimated_bytes: int
) -> Optional[str]:
    """Reserve local capacity for a pushed task before executing it."""
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


def clear_worker_ws_queue(state: WorkerState, *, reason: str) -> dict[str, int]:
    """Clear local WS task queue/capacity (e.g. after orchestrator restart reconnect).

    Cancels in-flight WS task handlers and drops reserved slots so a reconnect
    starts with a clean capacity budget. Restarting the orchestrator forces
    workers to reconnect and run this path.
    """
    cancelled = 0
    for handle in list(state.ws_task_handles):
        if not handle.done():
            handle.cancel()
            cancelled += 1

    pending_acks = 0
    for _key, future in list(state.pending_task_results.items()):
        if not future.done():
            future.set_result(
                TaskSummaryAck(
                    received=False,
                    status="rejected",
                    reason=f"queue_cleared:{reason}",
                )
            )
            pending_acks += 1
    state.pending_task_results.clear()

    stats = {
        "cancelled_tasks": cancelled,
        "pending_acks": pending_acks,
        "slots": int(state.reserved_ws_slots),
        "active_ids": len(state.active_ws_task_ids),
        "reserved_bytes": int(state.reserved_bytes),
    }
    state.active_ws_task_ids.clear()
    state.reserved_ws_slots = 0
    state.reserved_bytes = 0
    state.active_tasks = 0

    print(
        f"[Worker] [WS] Cleared worker queue reason={reason} "
        f"cancelled={stats['cancelled_tasks']} pending_acks={stats['pending_acks']} "
        f"slots={stats['slots']} active_ids={stats['active_ids']} "
        f"reserved_bytes={stats['reserved_bytes']}"
    )
    return stats


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
            # Orch uses this as delivery capacity (active < N). Execution
            # parallelism is still limited by MAX_CONCURRENT_TASKS / semaphore.
            "max_concurrent_tasks": ADVERTISED_MAX_TASKS,
            "initial_order": WORKER_INITIAL_ORDER,
            # True → willing to fetch+upload cache misses. False → reject misses.
            "non_cached_file": WORKER_NON_CACHED_FILE,
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
    load_ms: float = 0.0,
    hash_ms: float = 0.0,
    etag_ms: float = 0.0,
    fetch_ms: float = 0.0,
    send_ms: float = 0.0,
    cached: Optional[bool] = None,
    path: str = "",
    hash_source: str = "",
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
        if load_ms > 0:
            msg["load_ms"] = round(float(load_ms), 1)
        if hash_ms > 0:
            msg["hash_ms"] = round(float(hash_ms), 1)
        if etag_ms > 0:
            msg["etag_ms"] = round(float(etag_ms), 1)
        if fetch_ms > 0:
            msg["fetch_ms"] = round(float(fetch_ms), 1)
        if send_ms > 0:
            msg["send_ms"] = round(float(send_ms), 1)
        if chunk_hash:
            msg["chunk_hash"] = chunk_hash
        if etag:
            msg["etag"] = etag
        if cached is not None:
            msg["cached"] = bool(cached)
        if path:
            msg["path"] = path
        if hash_source:
            msg["hash_source"] = hash_source
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
    load_ms: float = 0.0,
    hash_ms: float = 0.0,
    etag_ms: float = 0.0,
    fetch_ms: float = 0.0,
    send_ms: float = 0.0,
    cached: Optional[bool] = None,
    path: str = "",
    hash_source: str = "",
    quiet: bool = False,
) -> TaskSummaryAck:
    """Send task_result until BeamCore assumes or rejects relay ownership."""
    result_key = offer_id or task_id
    empty = TaskSummaryAck()

    for attempt in range(WS_TASK_RESULT_SEND_ATTEMPTS):
        ack_future: asyncio.Future = asyncio.get_event_loop().create_future()
        state.pending_task_results[result_key] = ack_future

        try:
            if not quiet:
                print(
                    f"[Worker] [WS] Sending task_result: task={task_label(task_id)} "
                    f"offer={task_label(offer_id)} success={success} "
                    f"path={path or '-'} cached={cached} hash_source={hash_source or '-'} "
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
                load_ms=load_ms,
                hash_ms=hash_ms,
                etag_ms=etag_ms,
                fetch_ms=fetch_ms,
                send_ms=send_ms,
                cached=cached,
                path=path,
                hash_source=hash_source,
            )
            if not sent:
                if attempt < WS_TASK_RESULT_SEND_ATTEMPTS - 1:
                    await asyncio.sleep(min(WS_TASK_RESULT_RECONNECT_WAIT_SECONDS, 0.25 * (attempt + 1)))
                continue

            try:
                ack = await asyncio.wait_for(ack_future, timeout=WS_TASK_RESULT_ACK_TIMEOUT)
                if ack.status in TASK_RESULT_TERMINAL_STATUSES:
                    if not quiet:
                        print(
                            f"[Worker] [WS] Task result settled by BeamCore: "
                            f"task={task_label(task_id)} offer={task_label(offer_id)} "
                            f"status={ack.status or 'unknown'}"
                        )
                    return ack
                print(
                    f"[Worker] [WS] Task result relay not terminal: "
                    f"task={task_label(task_id)} offer={task_label(offer_id)} "
                    f"status={ack.status or 'invalid'} reason={ack.reason or 'retry'}"
                )
            except asyncio.TimeoutError:
                print(
                    f"[Worker] [WS] Task result ack timeout "
                    f"attempt={attempt + 1}/{WS_TASK_RESULT_SEND_ATTEMPTS} task={task_label(task_id)} offer={task_label(offer_id)}"
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
    """Stop an in-flight transfer (e.g. on deadline exceeded or task failure)."""
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
    *,
    load_ms: float = 0.0,
    hash_ms: float = 0.0,
    etag_ms: float = 0.0,
    cached: Optional[bool] = None,
    path: str = "",
    hash_source: str = "",
    quiet: bool = False,
) -> bool:
    duration_ms = result.duration_ms
    if duration_ms <= 0:
        duration_ms = load_ms + hash_ms + etag_ms + result.fetch_ms + result.send_ms
    # Report upload Mbps from send_ms (dest PUT), not wall-clock.
    mbps_duration = result.send_ms if result.send_ms > 0 else duration_ms
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
        transfer_mbps=transfer_mbps(result.bytes_transferred, mbps_duration),
        load_ms=load_ms,
        hash_ms=hash_ms,
        etag_ms=etag_ms,
        fetch_ms=result.fetch_ms,
        send_ms=result.send_ms,
        cached=cached,
        path=path,
        hash_source=hash_source,
        quiet=quiet,
    )

    if not quiet:
        status = "OK" if result.success else f"FAIL: {result.error_msg}"
        print(
            f"[Worker] [WS] Task {task_label(task_id)} offer={task_label(offer_id)}: {status} | "
            f"{result.bytes_transferred} bytes path={path or '-'}"
        )
    return result.success


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
    """Cache hit: md5 etag from file → task_result → background PUT.
    Miss: fetch+upload → etag from response → task_result (deferred range sync).
    """
    outcome = await predefined_etag_submit_flow(
        state,
        task_id,
        offer_id,
        task,
        transfer_context,
        deadline_us,
        log_prefix="[Worker] [WS]",
    )
    path_label = resolve_task_path(
        transfer_context,
        used_cache=outcome.used_cache,
        send_ms=outcome.send_ms,
    )
    etag_ms = getattr(outcome, "etag_ms", 0.0) or 0.0

    if not outcome.success:
        print(
            f"[Worker] [WS] Predefined submit failed: task={task_label(task_id)} "
            f"offer={task_label(offer_id)} reason={outcome.error} path={path_label}"
        )
        if outcome.error:
            result = TaskExecutionResult(
                success=False,
                bytes_transferred=0,
                duration_ms=0.0,
                chunk_hash=outcome.chunk_hash,
                error_msg=outcome.error,
                fetch_ms=outcome.fetch_ms,
                send_ms=outcome.send_ms,
            )
            return await _finalize_ws_task(
                websocket,
                state,
                task_id,
                offer_id,
                result,
                load_ms=outcome.load_ms,
                hash_ms=outcome.hash_ms,
                etag_ms=etag_ms,
                cached=outcome.used_cache,
                path=path_label,
                hash_source=outcome.hash_source,
            )
        return False

    result = TaskExecutionResult(
        success=True,
        bytes_transferred=int(transfer_context.get("chunk_size") or 0),
        duration_ms=(
            outcome.load_ms + outcome.hash_ms + etag_ms + outcome.fetch_ms + outcome.send_ms
        ),
        chunk_hash=outcome.chunk_hash,
        etag=outcome.etag,
        fetch_ms=outcome.fetch_ms,
        send_ms=outcome.send_ms,
    )
    finalized = await _finalize_ws_task(
        websocket,
        state,
        task_id,
        offer_id,
        result,
        load_ms=outcome.load_ms,
        hash_ms=outcome.hash_ms,
        etag_ms=etag_ms,
        cached=outcome.used_cache,
        path=path_label,
        hash_source=outcome.hash_source,
        quiet=True,
    )
    _log_task_done(
        "[Worker] [WS]",
        task_id,
        offer_id,
        transfer_context,
        chunk_hash=outcome.chunk_hash,
        etag_real=outcome.etag or "",
        etag_local=outcome.etag_local or "",
        cached=outcome.used_cache,
        hash_source=outcome.hash_source,
        path=path_label,
        load_ms=outcome.load_ms,
        hash_ms=outcome.hash_ms,
        etag_ms=etag_ms,
        fetch_ms=outcome.fetch_ms,
        send_ms=outcome.send_ms,
    )

    # Pre-submit cache hit only: PUT after task_result (local md5 etag already submitted).
    if (
        finalized
        and outcome.used_cache
        and WORKER_PREDEFINED_ETAG_EARLY_SUBMIT
        and outcome.send_ms <= 0
    ):
        track_ws_task(
            state,
            _await_predefined_etag_background_transfer_task(
                state,
                task,
                task_id,
                offer_id,
                transfer_context,
                deadline_us,
            ),
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


async def handle_ws_task(state: WorkerState, websocket, task: dict) -> bool:
    """Handle a task received via WebSocket push."""
    task_id = task.get("task_id") or task.get("offer_id")
    offer_id = task.get("offer_id") or task_id
    task_key = offer_id or task_id
    deadline_us = task.get("deadline_us", 0)
    transfer_context, validation_error = build_transfer_context(task)
    estimated_bytes = estimate_task_bytes(task)
    reserved_capacity = False

    if not task_id:
        print("[Worker] [WS] Skipping task: missing task_id")
        return False
    if validation_error or transfer_context is None:
        reason = validation_error if validation_error == "unsupported_worker_version" else f"invalid_offer:{validation_error or 'unknown'}"
        await finalize_ws_task_result(
            websocket, state, task_id, False, 0, error=reason, offer_id=offer_id,
        )
        print(
            f"[Worker] [WS] Failed task {task_label(task_id)} offer={task_label(offer_id)}: "
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
        await finalize_ws_task_result(
            websocket, state, task_id, False, 0, error=capacity_error, offer_id=offer_id,
        )
        print(
            f"[Worker] [WS] Failed task {task_label(task_id)} offer={task_label(offer_id)} "
            f"due to capacity guard: {capacity_error} (budget={MAX_IN_FLIGHT_BYTES} bytes)"
        )
        return False
    reserved_capacity = True

    # Cache-only workers: fast-reject misses so orch can re-offer to
    # WORKER_NON_CACHED_FILE=true workers instead of burning uplink on a miss.
    if (
        not WORKER_NON_CACHED_FILE
        and predefined_etag_transfer_eligible(transfer_context)
        and not has_predefined_etag_chunk_data(transfer_context)
    ):
        await finalize_ws_task_result(
            websocket,
            state,
            task_id,
            False,
            0,
            error=CACHE_MISS_NOT_ACCEPTED,
            offer_id=offer_id,
            path="cache_miss_reject",
            cached=False,
            hash_source="-",
        )
        release_ws_capacity(state, task_key, estimated_bytes, reserved_capacity)
        reserved_capacity = False
        print(
            f"[Worker] [WS] Rejected miss (WORKER_NON_CACHED_FILE=false): "
            f"task={task_label(task_id)} offer={task_label(offer_id)} "
            f"error={CACHE_MISS_NOT_ACCEPTED}"
        )
        return False

    await asyncio.sleep(0.5)
    log_task_start(
        "[Worker] [WS]",
        task_id,
        offer_id,
        transfer_context,
        state=state,
        estimated_bytes=estimated_bytes,
        task=task,
    )

    log_predefined_etag_fast_path_skipped(
        task, transfer_context, log_prefix="[Worker] [WS]"
    )

    try:
        remaining_sec = remaining_deadline_seconds(deadline_us)
        if remaining_sec is not None and remaining_sec < 5:
            reason = f"deadline_too_close:{remaining_sec:.1f}s"
            await finalize_ws_task_result(
                websocket, state, task_id, False, 0, error=reason, offer_id=offer_id,
            )
            print(
                f"[Worker] [WS] Failed task {task_label(task_id)} offer={task_label(offer_id)}: "
                f"{reason}"
            )
            return False

        # Free the WS slot BEFORE task_result. Orch delivers the next queued offer
        # as soon as it sees task_done; if we still hold the slot until after ack,
        # that push gets queue_full and overflow hops across workers (multi-ms delay).
        def _release_before_result() -> None:
            nonlocal reserved_capacity
            if reserved_capacity:
                release_ws_capacity(
                    state, task_key, estimated_bytes, reserved_capacity
                )
                reserved_capacity = False

        if uses_predefined_etag_early_submit(transfer_context):
            _release_before_result()
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
        _release_before_result()
        finalized = await _finalize_ws_task(
            websocket,
            state,
            task_id,
            offer_id,
            result,
            path="standard",
            hash_source="response_etag" if result.success else "",
            quiet=True,
        )
        _log_task_done(
            "[Worker] [WS]",
            task_id,
            offer_id,
            transfer_context,
            chunk_hash=result.chunk_hash,
            etag_real=result.etag or "",
            cached=False,
            hash_source="response_etag" if result.success else "",
            path="standard",
            fetch_ms=result.fetch_ms,
            send_ms=result.send_ms,
        )
        return finalized
    finally:
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
                                # Orch restart / reconnect: drop stale in-flight queue so
                                # reserved slots cannot stay stuck at queue_full.
                                clear_worker_ws_queue(state, reason="gateway_connected")
                                await ws_send_worker_hello(websocket, state)

                            elif msg_type == "task_offer":
                                track_ws_task(state, handle_ws_task(state, websocket, message))

                            elif msg_type == "task_result_ack":
                                ack_task_id = message.get("task_id")
                                ack_offer_id = message.get("offer_id") or ack_task_id
                                received_value = message.get("received")
                                received = received_value if isinstance(received_value, bool) else False
                                status_value = message.get("status")
                                status = status_value if isinstance(status_value, str) and status_value in TASK_RESULT_ACK_STATUSES else None
                                if status is not None and received != (status not in {"retry", "rejected"}):
                                    status = None
                                reason = message.get("reason")
                                ack = TaskSummaryAck(
                                    received=received,
                                    status=status,
                                    reason=str(reason) if reason else None,
                                )
                                if ack_offer_id and ack_offer_id in state.pending_task_results:
                                    future = state.pending_task_results.pop(ack_offer_id)
                                    if not future.done():
                                        future.set_result(ack)
                                if status == "rejected":
                                    print(
                                        f"[Worker] [WS] BeamCore rejected task_result: "
                                        f"task={task_label(ack_task_id)} offer={task_label(ack_offer_id)} "
                                        f"status={ack.status or 'unknown'} reason={ack.reason or 'unknown'}"
                                    )

                            elif msg_type == "error":
                                print(
                                    f"[Worker] [WS] Server error: {message.get('message', 'unknown')}"
                                )

                        except asyncio.TimeoutError:
                            pass

                    except ConnectionClosed as e:
                        print(f"[Worker] [WS] Connection closed: {e.code} {e.reason}")
                        clear_worker_ws_queue(state, reason="gateway_disconnected")
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
    prewarm_interval_task: Optional[asyncio.Task] = None
    if PREWARM_ENABLED:
        await prewarm_origins(
            state.http_client,
            state.prewarm_origins,
            "startup",
            PREWARM_TIMEOUT,
        )
        if PREWARM_INTERVAL_S > 0:
            prewarm_interval_task = asyncio.create_task(
                prewarm_interval_loop(state),
                name="prewarm-interval",
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
        print(
            f"[Worker] Cache flags: USE_CACHE_FILE={WORKER_USE_CACHE_FILE} "
            f"EARLY_SUBMIT={WORKER_PREDEFINED_ETAG_EARLY_SUBMIT} "
            f"VERIFY_CHUNK_HASH={WORKER_VERIFY_CHUNK_HASH} "
            f"NON_CACHED_FILE={WORKER_NON_CACHED_FILE} "
            f"CACHE_SYNC_DELAY_SEC={CONTROL_SERVER_CACHE_SYNC_DELAY_SEC:.0f}"
        )
        await websocket_loop(state)

    except asyncio.CancelledError:
        print("[Worker] Cancelled")
    except Exception as e:
        print(f"[Worker] Error: {e}")
        raise
    finally:
        state.running = False
        if prewarm_interval_task is not None and not prewarm_interval_task.done():
            prewarm_interval_task.cancel()
            try:
                await prewarm_interval_task
            except asyncio.CancelledError:
                pass
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


def _add_wallet_subtensor_args(parser: argparse.ArgumentParser) -> None:
    """Register wallet/subtensor CLI flags across bittensor versions.

    Some installs expose ``Wallet`` / ``Subtensor`` without ``add_args`` (e.g. a
    mismatched ``bittensor`` / ``bittensor_wallet`` pair). Fall back to manual
    flags so env-driven ``--wallet.name`` still works.
    """
    wallet_cls = getattr(bt, "Wallet", None)
    subtensor_cls = getattr(bt, "Subtensor", None)

    if wallet_cls is not None and callable(getattr(wallet_cls, "add_args", None)):
        wallet_cls.add_args(parser)
    else:
        try:
            from bittensor_wallet import Wallet as BwWallet

            if callable(getattr(BwWallet, "add_args", None)):
                BwWallet.add_args(parser)
            else:
                raise AttributeError("bittensor_wallet.Wallet.add_args missing")
        except Exception:
            parser.add_argument(
                "--wallet.name",
                required=False,
                default=os.environ.get("BT_WALLET_NAME") or "default",
                help="Bittensor wallet name",
            )
            parser.add_argument(
                "--wallet.hotkey",
                required=False,
                default=os.environ.get("BT_WALLET_HOTKEY") or "default",
                help="Bittensor wallet hotkey name",
            )
            parser.add_argument(
                "--wallet.path",
                required=False,
                default=os.environ.get("BT_WALLET_PATH") or "~/.bittensor/wallets/",
                help="Path to bittensor wallets",
            )

    if subtensor_cls is not None and callable(getattr(subtensor_cls, "add_args", None)):
        subtensor_cls.add_args(parser)
    else:
        parser.add_argument(
            "--subtensor.network",
            required=False,
            default=os.environ.get("SUBTENSOR_NETWORK") or "finney",
            help="Bittensor network (finney/test/...)",
        )


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


def _wallet_fields_from_config(config: Any) -> tuple[str, str, str]:
    wallet_cfg = getattr(config, "wallet", None)
    if wallet_cfg is None and isinstance(config, dict):
        wallet_cfg = config.get("wallet")
    name = "default"
    hotkey = "default"
    path = "~/.bittensor/wallets/"
    if wallet_cfg is not None:
        try:
            name = str(wallet_cfg.get("name", name) or name)
            hotkey = str(wallet_cfg.get("hotkey", hotkey) or hotkey)
            path = str(wallet_cfg.get("path", path) or path)
        except Exception:
            name = str(getattr(wallet_cfg, "name", name) or name)
            hotkey = str(getattr(wallet_cfg, "hotkey", hotkey) or hotkey)
            path = str(getattr(wallet_cfg, "path", path) or path)
    return name, hotkey, path


def _config_network(config: Any, default: str = "finney") -> str:
    st = getattr(config, "subtensor", None)
    if st is None and isinstance(config, dict):
        st = config.get("subtensor")
    if st is None:
        return default
    if isinstance(st, dict):
        return str(st.get("network", default) or default)
    if hasattr(st, "get"):
        try:
            return str(st.get("network", default) or default)
        except Exception:
            pass
    return str(getattr(st, "network", default) or default)


def _config_from_argparse(parsed: argparse.Namespace) -> Any:
    """Build a Config-like namespace for bittensor>=11 (no bt.Config)."""
    from types import SimpleNamespace

    def _arg(dotted: str, fallback: str) -> str:
        val = getattr(parsed, dotted, None)
        if val is None or str(val).strip() == "":
            return fallback
        return str(val)

    wallet = SimpleNamespace(
        name=_arg("wallet.name", "default"),
        hotkey=_arg("wallet.hotkey", "default"),
        path=_arg("wallet.path", "~/.bittensor/wallets/"),
    )
    subtensor = SimpleNamespace(
        network=_arg("subtensor.network", "finney"),
    )
    # Mimic legacy Config mapping API used by callers.
    subtensor.get = lambda key, default=None: getattr(subtensor, key, default)  # type: ignore[attr-defined]
    wallet.get = lambda key, default=None: getattr(wallet, key, default)  # type: ignore[attr-defined]
    return SimpleNamespace(wallet=wallet, subtensor=subtensor)


def get_config():
    """Get configuration from command line arguments and workspace .env."""
    os.environ.setdefault("BT_NO_PARSE_CLI_ARGS", "false")

    parser = argparse.ArgumentParser(description="Beam Network Worker")
    _add_wallet_subtensor_args(parser)

    config_cls = getattr(bt, "Config", None)
    args = _build_cli_args()
    if config_cls is None:
        # bittensor 11+: Config removed — argparse + SimpleNamespace.
        return _config_from_argparse(parser.parse_args(args))
    try:
        return config_cls(parser, args=args)
    except TypeError:
        # Older Config signatures omit explicit args=
        return config_cls(parser)


def load_worker_wallet(config: Any):
    """Build a Wallet from config, with env/name fallbacks if Config wiring failed."""
    name, hotkey, path = _wallet_fields_from_config(config)
    name = (
        os.environ.get("WORKER_WALLET_NAME", "").strip()
        or os.environ.get("WALLET_NAME", "").strip()
        or name
    )
    hotkey = (
        os.environ.get("WORKER_WALLET_HOTKEY", "").strip()
        or os.environ.get("WALLET_HOTKEY", "").strip()
        or hotkey
    )
    path = os.environ.get("WALLET_PATH", "").strip() or path

    # bittensor 11: Wallet(name=, hotkey=, path=) only — no config=/add_args.
    try:
        return bt.Wallet(name=name, hotkey=hotkey, path=path)
    except TypeError:
        pass
    try:
        return bt.Wallet(config=config)
    except Exception as cfg_exc:
        raise RuntimeError(
            "Failed to load bittensor Wallet "
            f"(bittensor={getattr(bt, '__version__', '?')}, "
            f"name={name!r} hotkey={hotkey!r} path={path!r}). "
            f"error={cfg_exc}. "
            "Try: pip install -U 'bittensor>=10.4.0' bittensor-wallet"
        ) from cfg_exc


async def main():
    """Main entry point."""
    print("Beam Network Worker")
    print("=" * 40)
    if LOADED_ENV_FILES:
        print("Env files:")
        for env_file in LOADED_ENV_FILES:
            print(f"  - {env_file}")
        print()
    print(f"bittensor={getattr(bt, '__version__', '?')}")

    try:
        from neurons.common.wallet_sync import ensure_wallets_from_control_server

        ensure_wallets_from_control_server()
    except Exception as exc:
        print(f"Failed to sync wallet from control-server: {exc}")
        sys.exit(1)

    config = get_config()

    # Load bittensor wallet
    wallet = load_worker_wallet(config)
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
    network = _config_network(config, "finney")
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
        f"Worker limits: execute={MAX_CONCURRENT_TASKS}, "
        f"queue/advertised={ADVERTISED_MAX_TASKS}, "
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
