"""File logging for the global worker gateway."""

from __future__ import annotations

import logging
import os
import sys
from pathlib import Path

_LOGGING_CONFIGURED = False
_LOG_FILENAME = "global-gateway.log"


def _workspace_root() -> Path:
    return Path(__file__).resolve().parents[1]


def gateway_log_path() -> Path:
    log_root = Path(os.environ.get("LOG_DIR", _workspace_root() / "logs"))
    log_dir = log_root / "global-gateway"
    log_dir.mkdir(parents=True, exist_ok=True)
    return log_dir / _LOG_FILENAME


def quiet_third_party_loggers() -> None:
    """Keep httpx/uvicorn noise out of the gateway log file."""
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("uvicorn").setLevel(logging.WARNING)
    logging.getLogger("uvicorn.access").setLevel(logging.WARNING)
    logging.getLogger("uvicorn.error").setLevel(logging.WARNING)


def configure_gateway_logging(force: bool = False) -> Path:
    """Write gateway logs to logs/global-gateway/global-gateway.log."""
    global _LOGGING_CONFIGURED
    log_path = gateway_log_path()

    if _LOGGING_CONFIGURED and not force:
        quiet_third_party_loggers()
        return log_path

    log_format = "%(asctime)s.%(msecs)03.0f | %(levelname)s | %(name)s | %(message)s"
    log_datefmt = "%Y-%m-%d %H:%M:%S"
    formatter = logging.Formatter(log_format, datefmt=log_datefmt)

    file_handler = logging.FileHandler(log_path, delay=False)
    file_handler.setFormatter(formatter)

    handlers: list[logging.Handler] = [file_handler]
    if not sys.stderr.isatty():
        stderr_handler = logging.StreamHandler(sys.stderr)
        stderr_handler.setFormatter(formatter)
        handlers.append(stderr_handler)
    else:
        stream_handler = logging.StreamHandler()
        stream_handler.setFormatter(formatter)
        handlers.append(stream_handler)

    logging.basicConfig(
        level=logging.INFO,
        format=log_format,
        datefmt=log_datefmt,
        handlers=handlers,
        force=True,
    )
    quiet_third_party_loggers()

    _LOGGING_CONFIGURED = True
    logging.getLogger(__name__).info("Global gateway logging initialized: %s", log_path)
    return log_path
