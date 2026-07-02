"""Bittensor wallet bundle routes."""

from fastapi import APIRouter, Depends, HTTPException, Request, Response
from fastapi.responses import JSONResponse

from auth import require_control_secret
from storage import (
    build_wallet_tarball,
    extract_wallet_tarball,
    list_wallet_hotkeys,
    list_wallets,
    wallet_exists,
    wallet_hotkey_exists,
)

router = APIRouter(prefix="/wallets", tags=["wallets"], dependencies=[Depends(require_control_secret)])


@router.get("")
async def get_wallets() -> dict:
    wallets = []
    for name in list_wallets():
        wallets.append(
            {
                "wallet_name": name,
                "hotkeys": list_wallet_hotkeys(name),
            }
        )
    return {"wallets": wallets}


@router.get("/{wallet_name}/exists")
async def get_wallet_exists(wallet_name: str, hotkey: str = "") -> dict:
    if hotkey:
        return {
            "wallet_name": wallet_name,
            "hotkey": hotkey,
            "exists": wallet_hotkey_exists(wallet_name, hotkey),
        }
    return {"wallet_name": wallet_name, "exists": wallet_exists(wallet_name)}


@router.get("/{wallet_name}/bundle")
async def get_wallet_bundle(wallet_name: str) -> Response:
    try:
        payload = build_wallet_tarball(wallet_name)
    except FileNotFoundError:
        raise HTTPException(status_code=404, detail=f"wallet not found: {wallet_name}") from None
    return Response(
        content=payload,
        media_type="application/gzip",
        headers={"Content-Disposition": f'attachment; filename="{wallet_name}.tar.gz"'},
    )


@router.put("/{wallet_name}/bundle")
async def put_wallet_bundle(wallet_name: str, request: Request) -> JSONResponse:
    body = await request.body()
    if not body:
        raise HTTPException(status_code=400, detail="empty wallet bundle")
    extract_wallet_tarball(wallet_name, body)
    return JSONResponse({"wallet_name": wallet_name, "hotkeys": list_wallet_hotkeys(wallet_name)})
