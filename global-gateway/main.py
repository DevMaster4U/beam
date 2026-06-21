"""BEAM global worker gateway entry point."""

import logging
import sys
from contextlib import asynccontextmanager
from pathlib import Path

import uvicorn
from fastapi import FastAPI

# Allow running as `python main.py` from global-gateway/
_ROOT = Path(__file__).resolve().parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from config import get_settings
from core import WorkerScoringWeights, gateway_state
from routes.orchestrators import router as orchestrators_router
from routes.workers import router as workers_router

logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(_app: FastAPI):
    settings = get_settings()
    logging.basicConfig(level=settings.log_level)
    gateway_state.scoring_weights = WorkerScoringWeights(
        weight_trust=settings.weight_trust,
        weight_latency=settings.weight_latency,
        weight_load=settings.weight_load,
        weight_bandwidth=settings.weight_bandwidth,
        weight_success=settings.weight_success,
    )
    logger.info(
        "Global gateway starting on %s:%s (max_workers=%d)",
        settings.gateway_host,
        settings.gateway_port,
        settings.max_workers,
    )
    yield
    logger.info("Global gateway stopped")


app = FastAPI(title="BEAM Global Worker Gateway", version="0.1.0", lifespan=lifespan)
app.include_router(workers_router)
app.include_router(orchestrators_router)


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
    uvicorn.run(
        "main:app",
        host=settings.gateway_host,
        port=settings.gateway_port,
        log_level=settings.log_level.lower(),
        ws_ping_interval=settings.ws_ping_interval,
        ws_ping_timeout=settings.ws_ping_timeout,
    )


if __name__ == "__main__":
    main()
