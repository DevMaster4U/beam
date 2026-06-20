"""BeamCore API key validation for workers and orchestrators."""

import logging

import httpx

logger = logging.getLogger(__name__)

_VALIDATE_TIMEOUT = 5.0


async def validate_worker_api_key(core_url: str, worker_id: str, api_key: str) -> bool:
    try:
        async with httpx.AsyncClient(timeout=_VALIDATE_TIMEOUT) as client:
            resp = await client.get(
                f"{core_url.rstrip('/')}/workers/{worker_id}",
                headers={"x-api-key": api_key},
            )
            if resp.status_code == 200:
                data = resp.json()
                return data.get("worker_id") == worker_id
            return False
    except Exception as exc:
        logger.warning("BeamCore worker key validation failed for %s: %s", worker_id, exc)
        return False


async def validate_orchestrator_api_key(core_url: str, hotkey: str, api_key: str) -> bool:
    try:
        async with httpx.AsyncClient(timeout=_VALIDATE_TIMEOUT) as client:
            resp = await client.get(
                f"{core_url.rstrip('/')}/orchestrators/{hotkey}",
                headers={"x-api-key": api_key},
            )
            if resp.status_code == 200:
                data = resp.json()
                return data.get("hotkey") == hotkey or data.get("orchestrator_hotkey") == hotkey
            return False
    except Exception as exc:
        logger.warning("BeamCore orchestrator key validation failed for %s: %s", hotkey, exc)
        return False
