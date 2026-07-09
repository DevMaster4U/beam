"""Predefined ETag cache routes (shared across miners)."""

from __future__ import annotations

import hashlib
import asyncio
from typing import Any
from urllib.parse import unquote

from fastapi import APIRouter, Depends, Header, HTTPException, Request
from fastapi.responses import Response
from pydantic import BaseModel, Field

from auth import require_control_secret
from storage import (
    delete_all_chunk_data_files,
    delete_predefined_etag_chunk_data,
    load_predefined_etag_cache,
    load_predefined_etag_chunk_data,
    merge_predefined_etag_entries,
    prune_orphan_chunk_data_files,
    store_predefined_etag_chunk_data,
    upsert_predefined_etag_entry,
)
from ws_hub import miner_hub

router = APIRouter(
    prefix="/cache/predefined-etag",
    tags=["cache"],
    dependencies=[Depends(require_control_secret)],
)


class CacheEntryBody(BaseModel):
    chunk_hash: str
    etag: str


class CacheMergeBody(BaseModel):
    entries: dict[str, CacheEntryBody] = Field(default_factory=dict)


@router.get("")
async def get_cache() -> dict[str, Any]:
    return load_predefined_etag_cache()


@router.put("/entries/{cache_key:path}/data")
async def put_chunk_data(
    cache_key: str,
    request: Request,
    x_chunk_hash: str = Header(default="", alias="X-Chunk-Hash"),
    x_etag: str = Header(default="", alias="X-ETag"),
    x_miner_id: str = Header(default="", alias="X-Miner-Id"),
) -> dict[str, Any]:
    key = unquote(cache_key)
    try:
        data = await request.body()
        if not data:
            raise HTTPException(status_code=400, detail="chunk body required")
        computed_hash = hashlib.sha256(data).hexdigest()
        chunk_hash = x_chunk_hash.strip() or computed_hash
        if chunk_hash.lower() != computed_hash.lower():
            raise HTTPException(status_code=400, detail="chunk hash mismatch")
        etag = x_etag.strip()
        await asyncio.to_thread(store_predefined_etag_chunk_data, key, data)
        entry = await asyncio.to_thread(
            upsert_predefined_etag_entry,
            key,
            chunk_hash,
            etag,
            has_chunk_data=True,
        )
        broadcast = {
            "type": "cache_broadcast",
            "key": key,
            "chunk_hash": entry["chunk_hash"],
            "etag": entry["etag"],
            "source_miner": x_miner_id.strip() or "http",
            "has_chunk_data": True,
        }
        if "chunk_index" in entry:
            broadcast["chunk_index"] = entry["chunk_index"]
        await miner_hub.broadcast(broadcast, exclude_miner=x_miner_id.strip() or None)
        return entry
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"chunk data store failed: {exc}") from exc


@router.get("/entries/{cache_key:path}/data")
async def get_chunk_data(cache_key: str) -> Response:
    key = unquote(cache_key)
    data = await asyncio.to_thread(load_predefined_etag_chunk_data, key)
    if not data:
        raise HTTPException(status_code=404, detail=f"chunk data miss: {key}")
    return Response(content=data, media_type="application/octet-stream")


@router.delete("/entries/{cache_key:path}/data")
async def delete_chunk_data(cache_key: str) -> dict[str, Any]:
    key = unquote(cache_key)
    removed = await asyncio.to_thread(delete_predefined_etag_chunk_data, key)
    if not removed:
        raise HTTPException(status_code=404, detail=f"chunk data miss: {key}")
    return {"key": key, "deleted": True}


@router.delete("/chunk-data")
async def delete_chunk_data_bulk(all_files: bool = False) -> dict[str, Any]:
    """Prune orphan .bin files, or delete all chunk bytes when all_files=true."""
    if all_files:
        removed = await asyncio.to_thread(delete_all_chunk_data_files)
        return {"deleted_files": removed, "mode": "all"}
    result = await asyncio.to_thread(prune_orphan_chunk_data_files)
    return {"mode": "orphans", **result}


@router.get("/entries/{cache_key:path}")
async def get_cache_entry(cache_key: str) -> dict[str, str]:
    key = unquote(cache_key)
    payload = load_predefined_etag_cache()
    entry = (payload.get("entries") or {}).get(key)
    if not entry:
        raise HTTPException(status_code=404, detail=f"cache miss: {key}")
    return entry


@router.put("/entries/{cache_key:path}")
async def put_cache_entry(cache_key: str, body: CacheEntryBody) -> dict[str, str]:
    key = unquote(cache_key)
    chunk_hash = body.chunk_hash.strip()
    etag = body.etag.strip()
    if not chunk_hash:
        raise HTTPException(status_code=400, detail="chunk_hash required")
    return upsert_predefined_etag_entry(key, chunk_hash, etag or "")


@router.post("/merge")
async def post_cache_merge(body: CacheMergeBody) -> dict[str, Any]:
    merged = {
        key: {"chunk_hash": item.chunk_hash.strip(), "etag": item.etag.strip()}
        for key, item in body.entries.items()
        if item.chunk_hash.strip()
    }
    return merge_predefined_etag_entries(merged)
