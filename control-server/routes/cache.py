"""Predefined ETag cache routes (shared across miners)."""

from __future__ import annotations

from typing import Any
from urllib.parse import unquote

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field

from auth import require_control_secret
from storage import (
    load_predefined_etag_cache,
    merge_predefined_etag_entries,
    upsert_predefined_etag_entry,
)

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
