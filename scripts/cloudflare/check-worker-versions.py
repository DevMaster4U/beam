#!/usr/bin/env python3
"""GET version/health from one or more CF transfer Workers.

Usage:
  python3 scripts/cloudflare/check-worker-versions.py
  python3 scripts/cloudflare/check-worker-versions.py \\
    https://still-base-8f94.example.workers.dev \\
    https://noisy-union-160b.example.workers.dev
  CF_TRANSFER_WORKER_URLS=url1,url2 python3 scripts/cloudflare/check-worker-versions.py
  python3 scripts/cloudflare/check-worker-versions.py --expect 1.1.0

Exit 0 if all reachable (and match --expect when set); else 1.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any


def _split_urls(*parts: str) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    for part in parts:
        if not part:
            continue
        for token in part.replace(";", ",").replace("\n", ",").split(","):
            url = token.strip().rstrip("/")
            if not url or url in seen:
                continue
            seen.add(url)
            out.append(url)
    return out


def _probe(url: str, timeout: float) -> dict[str, Any]:
    req = urllib.request.Request(
        url + "/",
        method="GET",
        headers={"Accept": "application/json", "User-Agent": "beam-cf-version-check"},
    )
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            raw = resp.read().decode("utf-8", errors="replace")
            header_ver = resp.headers.get("X-Beam-Worker-Version") or ""
            status = resp.status
    except urllib.error.HTTPError as exc:
        raw = exc.read().decode("utf-8", errors="replace")
        header_ver = exc.headers.get("X-Beam-Worker-Version") if exc.headers else ""
        status = exc.code
        return {
            "url": url,
            "ok": False,
            "http_status": status,
            "error": f"http_{status}",
            "body": raw[:200],
            "header_version": header_ver or None,
        }
    except Exception as exc:
        return {
            "url": url,
            "ok": False,
            "http_status": 0,
            "error": f"{type(exc).__name__}: {exc}",
        }

    data: Any = None
    try:
        data = json.loads(raw)
    except json.JSONDecodeError:
        data = None

    if not isinstance(data, dict):
        return {
            "url": url,
            "ok": False,
            "http_status": status,
            "error": "non_json_or_hello_world",
            "body": raw[:120].replace("\n", " "),
            "header_version": header_ver or None,
        }

    version = str(data.get("version") or header_ver or "")
    return {
        "url": url,
        "ok": bool(data.get("ok", True)) and bool(version),
        "http_status": status,
        "name": data.get("name"),
        "version": version or None,
        "mode": data.get("mode"),
        "updated_at": data.get("updated_at"),
        "header_version": header_ver or None,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("urls", nargs="*", help="Worker base URLs")
    parser.add_argument(
        "--expect",
        default="",
        help="Require this version on every Worker (e.g. 1.1.0)",
    )
    parser.add_argument("--timeout", type=float, default=10.0)
    args = parser.parse_args()

    urls = _split_urls(
        *args.urls,
        os.environ.get("CF_TRANSFER_WORKER_URLS", ""),
        os.environ.get("CF_TRANSFER_WORKER_URL", ""),
    )
    if not urls:
        print(
            "need URLs as args or CF_TRANSFER_WORKER_URLS / CF_TRANSFER_WORKER_URL",
            file=sys.stderr,
        )
        return 2

    expect = (args.expect or "").strip()
    rows: list[dict[str, Any]] = []
    with ThreadPoolExecutor(max_workers=min(16, len(urls))) as pool:
        futs = {pool.submit(_probe, u, args.timeout): u for u in urls}
        for fut in as_completed(futs):
            rows.append(fut.result())

    rows.sort(key=lambda r: r.get("url") or "")
    bad = 0
    for row in rows:
        url = row.get("url")
        if not row.get("ok"):
            bad += 1
            print(
                f"FAIL  {url}  error={row.get('error')} body={row.get('body') or '-'}"
            )
            continue
        ver = str(row.get("version") or "")
        mismatch = expect and ver != expect
        if mismatch:
            bad += 1
            print(
                f"STALE {url}  version={ver} expected={expect} "
                f"mode={row.get('mode')} updated_at={row.get('updated_at')}"
            )
        else:
            print(
                f"OK    {url}  version={ver} mode={row.get('mode')} "
                f"updated_at={row.get('updated_at')}"
            )

    print(f"checked={len(rows)} bad={bad}" + (f" expect={expect}" if expect else ""))
    return 1 if bad else 0


if __name__ == "__main__":
    raise SystemExit(main())
