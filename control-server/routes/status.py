"""Control-server status: predefined ETag cache coverage by source URL."""

from __future__ import annotations

from typing import Any, Optional

from fastapi import APIRouter, Query

from storage import predefined_etag_cache_status
from ws_hub import miner_hub

router = APIRouter(tags=["status"])


@router.get("/status")
async def get_status(
    src_url: Optional[str] = Query(
        default=None,
        description=(
            "Filter to one source object URL "
            "(e.g. https://.../beam-xfer-test/source/test10gb-random.bin)"
        ),
    ),
) -> dict[str, Any]:
    """Show cached chunk_index list grouped by source URL."""
    body = predefined_etag_cache_status(src_url=src_url)
    body["connected_miners"] = miner_hub.connection_count
    body["connected_miner_ids"] = miner_hub.list_miners()
    return body
