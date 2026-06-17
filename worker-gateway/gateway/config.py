"""Gateway configuration."""

import os
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

from dotenv import load_dotenv

_WORKSPACE_ROOT = Path(__file__).resolve().parents[2]


def _extract_env_file_arg(argv: list[str]) -> tuple[Optional[Path], list[str]]:
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


def _resolve_gateway_env_file() -> Optional[Path]:
    cli_file, _ = _extract_env_file_arg(sys.argv[1:])
    if cli_file is not None:
        return cli_file

    env_path = os.environ.get("GATEWAY_ENV_FILE", "").strip()
    if env_path:
        return Path(env_path).expanduser()
    return None


def _resolve_env_path(path: Path) -> Path:
    if path.is_absolute():
        return path
    return _WORKSPACE_ROOT / path


def _load_env_file() -> None:
    shared_env = _WORKSPACE_ROOT / ".env"
    if shared_env.exists():
        load_dotenv(shared_env, override=False)

    gateway_env = _resolve_gateway_env_file()
    if gateway_env is not None:
        gateway_env = _resolve_env_path(gateway_env)
        if gateway_env.exists():
            load_dotenv(gateway_env, override=True)
        else:
            print(f"Error: gateway env file not found: {gateway_env}", file=sys.stderr)
            sys.exit(2)
        return

    legacy_env = _WORKSPACE_ROOT / ".env"
    if legacy_env.exists():
        load_dotenv(legacy_env, override=False)


def _get_control_secret() -> str:
    return (
        os.environ.get("GATEWAY_CONTROL_SECRET", "").strip()
        or os.environ.get("WORKER_GATEWAY_CONTROL_SECRET", "").strip()
    )


def _get_worker_secret() -> str:
    return (
        os.environ.get("GATEWAY_WORKER_SECRET", "").strip()
        or os.environ.get("WORKER_GATEWAY_WORKER_SECRET", "").strip()
    )


@dataclass(frozen=True)
class GatewaySettings:
    host: str
    port: int
    control_secret: str
    worker_secret: str
    ws_ping_interval: float
    ws_ping_timeout: float
    log_level: str

    @classmethod
    def from_env(cls) -> "GatewaySettings":
        _load_env_file()

        control_secret = _get_control_secret()
        if not control_secret:
            raise ValueError(
                "GATEWAY_CONTROL_SECRET or WORKER_GATEWAY_CONTROL_SECRET is required"
            )

        worker_secret = _get_worker_secret()
        if not worker_secret:
            raise ValueError(
                "GATEWAY_WORKER_SECRET or WORKER_GATEWAY_WORKER_SECRET is required"
            )

        return cls(
            host=os.environ.get("GATEWAY_HOST", "0.0.0.0"),
            port=int(os.environ.get("GATEWAY_PORT", "8001")),
            control_secret=control_secret,
            worker_secret=worker_secret,
            ws_ping_interval=float(os.environ.get("GATEWAY_WS_PING_INTERVAL", "30")),
            ws_ping_timeout=float(os.environ.get("GATEWAY_WS_PING_TIMEOUT", "10")),
            log_level=os.environ.get("LOG_LEVEL", "INFO").upper(),
        )
