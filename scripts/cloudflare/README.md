# Cloudflare transfer Worker

JavaScript Cloudflare Worker:

`GET` → version/health · `POST` → `fetch_and_send_chunk` (stream source GET → dest PUT) → `etag`

## Files

| File | Role |
|------|------|
| `transfer-worker.js` | Worker script (`WORKER_VERSION`) |
| `workers.txt` | List of Worker **names** to deploy |
| `deploy-workers.sh` | Deploy same script to many names |
| `check-worker-versions.py` | GET version from many URLs |
| `call-transfer-worker.py` | Manual POST of a task offer |

## Version probe (GET)

```bash
curl -sS https://still-base-8f94.<account>.workers.dev/
# {"ok":true,"name":"beam-cf-transfer","version":"1.2.0","mode":"fetch_and_send_chunk","updated_at":"2026-07-23"}
```

Also returns header `X-Beam-Worker-Version: 1.2.0`.

Check a pool (uses `CF_TRANSFER_WORKER_URLS` if set):

```bash
export CF_TRANSFER_WORKER_URLS='https://w1.workers.dev,https://w2.workers.dev,https://w3.workers.dev'
python3 scripts/cloudflare/check-worker-versions.py
python3 scripts/cloudflare/check-worker-versions.py --expect 1.2.0
```

## Update multiple Workers

**Headless server:** do **not** use `wrangler login` (needs browser → `xdg-open` error). Use an API token.

1. Create token: https://dash.cloudflare.com/profile/api-tokens → template **Edit Cloudflare Workers**
2. Bump `WORKER_VERSION` in `transfer-worker.js`
3. Put Worker **names** in `workers.txt`
4. Deploy:

```bash
export CLOUDFLARE_API_TOKEN='...'
# optional (from dash URL / account page):
# export CLOUDFLARE_ACCOUNT_ID='d5c7459ec862481084f6addb310afbe7'

./scripts/cloudflare/deploy-workers.sh --file scripts/cloudflare/workers.txt
```

5. Verify:

```bash
export CF_TRANSFER_WORKER_URLS='https://w1.workers.dev,https://w2.workers.dev'
python3 scripts/cloudflare/check-worker-versions.py --expect 1.2.0
```

Orch env (round-robin):

```bash
CF_TRANSFER_WORKER_URLS=https://still-base-8f94....workers.dev,https://noisy-union-160b....workers.dev
CF_TRANSFER_SEND_ACCEPT=false
WORKER_1_CF_TRANSFER_ENABLED=true
```

## Timing model

Same as Python `fetch_and_send_chunk` (concurrent stream):

1. Source GET stream fully consumed → `fetch_ms`
2. Dest PUT start → response → `send_ms` (overlaps fetch)

Uses `TransformStream` + streamed PUT body (no full-buffer RAM copy).
