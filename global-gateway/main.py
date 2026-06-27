"""BEAM global worker gateway entry point."""

import logging
import os
import sys
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Optional

import uvicorn
from fastapi import FastAPI

# Allow running as `python main.py` from global-gateway/
_ROOT = Path(__file__).resolve().parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from config import get_settings
from core import WorkerScoringWeights, gateway_state
from ipc_server import PoolCoordinatorIpcServer
from logging_config import configure_gateway_logging, quiet_third_party_loggers
from routes.orchestrators import router as orchestrators_router
from routes.status import router as status_router
from routes.workers import router as workers_router

logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(_app: FastAPI):
    settings = get_settings()
    quiet_third_party_loggers()
    gateway_state.scoring_weights = WorkerScoringWeights(
        weight_trust=settings.weight_trust,
        weight_latency=settings.weight_latency,
        weight_load=settings.weight_load,
        weight_bandwidth=settings.weight_bandwidth,
        weight_success=settings.weight_success,
    )
    gateway_state.worker_history_max = settings.worker_history_max
    gateway_state.max_workers = settings.max_workers
    gateway_state.worker_selection = settings.worker_selection.strip().lower()

    ipc_server: Optional[PoolCoordinatorIpcServer] = None
    if settings.ipc_enabled:
        ipc_path = settings.ipc_socket_path
        if not os.path.isabs(ipc_path):
            ipc_path = str(_ROOT.parent / ipc_path)
        ipc_server = PoolCoordinatorIpcServer(ipc_path)
        await ipc_server.start()

    logger.info(
        "Global gateway ready on %s:%s (max_workers=%d worker_selection=%s ipc=%s)",
        settings.gateway_host,
        settings.gateway_port,
        settings.max_workers,
        gateway_state.worker_selection,
        ipc_server.socket_path if ipc_server else "disabled",
    )
    yield
    if ipc_server is not None:
        await ipc_server.stop()
    logger.info("Global gateway stopped")


app = FastAPI(title="BEAM Global Worker Gateway", version="0.1.0", lifespan=lifespan)
app.include_router(workers_router)
app.include_router(orchestrators_router)
app.include_router(status_router)


@app.get("/health")
async def health():
    from core import gateway_state

    return {
        "status": "ok",
        "workers": gateway_state.worker_count(),
        "orchestrators": gateway_state.orchestrator_count(),
    }


def main() -> None:
    settings = get_settings()
    configure_gateway_logging(force=True)
    logging.getLogger().setLevel(settings.log_level)
    quiet_third_party_loggers()
    uvicorn.run(
        "main:app",
        host=settings.gateway_host,
        port=settings.gateway_port,
        log_level="warning",
        log_config=None,
        ws_ping_interval=settings.ws_ping_interval,
        ws_ping_timeout=settings.ws_ping_timeout,
    )


if __name__ == "__main__":
    main()
