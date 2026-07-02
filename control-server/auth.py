"""Shared-secret auth for control-server endpoints."""

from fastapi import Header, HTTPException

from config import get_settings


def validate_control_secret(provided: str, expected: str) -> bool:
    expected = (expected or "").strip()
    provided = (provided or "").strip()
    return bool(expected) and provided == expected


def require_control_secret(
    x_control_server_secret: str = Header(default="", alias="X-Control-Server-Secret"),
) -> None:
    if not validate_control_secret(x_control_server_secret, get_settings().secret):
        raise HTTPException(status_code=401, detail="invalid control server secret")
