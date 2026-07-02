"""Miner env file routes."""

from fastapi import APIRouter, Depends, HTTPException
from fastapi.responses import PlainTextResponse
from pydantic import BaseModel, Field

from auth import require_control_secret
from storage import list_miners, read_miner_env, write_miner_env

router = APIRouter(prefix="/miners", tags=["miners"], dependencies=[Depends(require_control_secret)])


class MinerEnvBody(BaseModel):
    content: str = Field(..., description="Full .env file contents")


@router.get("")
async def get_miners() -> dict:
    return {"miners": list_miners()}


@router.get("/{miner_id}/env", response_class=PlainTextResponse)
async def get_miner_env(miner_id: str) -> PlainTextResponse:
    try:
        content = read_miner_env(miner_id)
    except FileNotFoundError:
        raise HTTPException(status_code=404, detail=f"miner not found: {miner_id}") from None
    return PlainTextResponse(content)


@router.put("/{miner_id}/env", response_class=PlainTextResponse)
async def put_miner_env(miner_id: str, body: MinerEnvBody) -> PlainTextResponse:
    write_miner_env(miner_id, body.content)
    return PlainTextResponse("ok")
