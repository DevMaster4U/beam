"""BEAM miner control-server entry point."""

import logging
import sys
from contextlib import asynccontextmanager
from pathlib import Path

import uvicorn
from fastapi import FastAPI

_ROOT = Path(__file__).resolve().parent
_REPO_ROOT = _ROOT.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from config import get_settings
from logging_config import configure_logging
from routes.cache import router as cache_router
from routes.miners import router as miners_router
from routes.miners_ws import router as miners_ws_router
from routes.status import router as status_router
from routes.wallets import router as wallets_router
from storage import preload_predefined_etag_cache
from ws_hub import miner_hub

logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(_app: FastAPI):
    settings = get_settings()
    configure_logging(settings.log_level)
    cache_entries = preload_predefined_etag_cache()
    logger.info(
        "Control server ready on %s:%s data_dir=%s miners=%d wallets=%d cache_entries=%d ws=/ws/miners",
        settings.host,
        settings.port,
        settings.data_dir,
        len(list(settings.miners_dir.glob("*.env"))),
        len([p for p in settings.wallets_dir.iterdir() if p.is_dir()]),
        cache_entries,
    )
    yield
    logger.info("Control server stopped")


app = FastAPI(title="BEAM Control Server", version="0.1.0", lifespan=lifespan)
app.include_router(miners_router)
app.include_router(cache_router)
app.include_router(wallets_router)
app.include_router(status_router)
app.include_router(miners_ws_router)


@app.get("/health")
async def health():
    settings = get_settings()
    return {
        "status": "ok",
        "data_dir": str(settings.data_dir),
        "miners": len(list(settings.miners_dir.glob("*.env"))),
        "connected_miners": miner_hub.connection_count,
        "connected_miner_ids": miner_hub.list_miners(),
    }


def main() -> None:
    settings = get_settings()
    uvicorn.run(
        "main:app",
        host=settings.host,
        port=settings.port,
        log_level=settings.log_level.lower(),
    )


if __name__ == "__main__":
    main()
