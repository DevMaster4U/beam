"""HTTP endpoints for worker pool status and task history."""

from __future__ import annotations

from typing import Optional

from fastapi import APIRouter, HTTPException, Query

from core import gateway_state

router = APIRouter(tags=["status"])


@router.get("/workers")
async def list_workers():
    """Current worker status for all known workers (connected and recently seen)."""
    stats = gateway_state.worker_pool_stats()
    return {
        "pool": stats,
        "workers": gateway_state.worker_status_payload(),
    }


@router.get("/workers/history")
async def list_worker_history(
    worker_id: Optional[str] = Query(default=None),
    limit: int = Query(default=50, ge=1, le=500),
):
    """Completed/rejected task history across workers or for one worker."""
    if worker_id and worker_id not in gateway_state.all_worker_ids():
        if worker_id not in gateway_state.worker_histories:
            raise HTTPException(status_code=404, detail="worker not found")
    return {
        "worker_id": worker_id,
        "limit": limit,
        "history": gateway_state.worker_history(worker_id, limit=limit),
    }


@router.get("/workers/{worker_id}")
async def get_worker(worker_id: str):
    """Current status for a single worker."""
    detail = gateway_state.worker_detail_payload(worker_id)
    if detail is None:
        raise HTTPException(status_code=404, detail="worker not found")
    return detail


@router.get("/workers/{worker_id}/history")
async def get_worker_history(
    worker_id: str,
    limit: int = Query(default=50, ge=1, le=500),
):
    """Task history for a single worker."""
    if worker_id not in gateway_state.all_worker_ids() and worker_id not in gateway_state.worker_histories:
        raise HTTPException(status_code=404, detail="worker not found")
    return {
        "worker_id": worker_id,
        "limit": limit,
        "history": gateway_state.worker_history(worker_id, limit=limit),
    }
