"""Control server logging."""

import logging
import os
import sys
from pathlib import Path


def configure_logging(level: str = "INFO") -> Path:
    log_root = Path(os.environ.get("LOG_DIR", Path(__file__).resolve().parents[1] / "logs"))
    log_dir = log_root / "control-server"
    log_dir.mkdir(parents=True, exist_ok=True)
    log_path = log_dir / "control-server.log"

    log_format = "%(asctime)s.%(msecs)03.0f | %(levelname)s | %(name)s | %(message)s"
    datefmt = "%Y-%m-%d %H:%M:%S"
    formatter = logging.Formatter(log_format, datefmt=datefmt)

    handlers: list[logging.Handler] = [
        logging.FileHandler(log_path, delay=False),
    ]
    if sys.stderr.isatty():
        stream = logging.StreamHandler()
        stream.setFormatter(formatter)
        handlers.append(stream)

    logging.basicConfig(level=getattr(logging, level.upper(), logging.INFO), handlers=handlers, force=True)
    logging.getLogger(__name__).info("Control server logging initialized: %s", log_path)
    return log_path
