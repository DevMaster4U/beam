# Cloudflare transfer Worker

JavaScript Cloudflare Worker that streams a Beam chunk:

`source GET (Range)` → `dest PUT (UploadPart)` → returns `etag`.

## Files

| File | Role |
|------|------|
| `transfer-worker.js` | Worker script (ES module) |
| `call-transfer-worker.py` | Local POST helper against a deployed Worker |

## Deploy

```bash
cd scripts/cloudflare
npx wrangler deploy transfer-worker.js
```

Set the returned URL(s) on the orchestrator:

```bash
CF_TRANSFER_WORKER_URLS=https://still-base-8f94.example.workers.dev,https://other.workers.dev
CF_TRANSFER_SEND_ACCEPT=false
WORKER_1_CF_TRANSFER_ENABLED=true
```

## Timing model (same as `fetch_and_send_chunk`)

1. **Download** full source body into memory → `fetch_ms` / `source_fetch`
2. **Upload** that buffer with dest PUT → `send_ms` / `stream_upload`

So orch logs will show real download vs upload, not TTFB + combined stream.

~40 MiB chunks fit Worker memory (128 MiB). Do not raise chunk size past ~80 MiB without switching back to streaming.

## Why older streaming logs looked like slow upload

`source_fetch` was TTFB only; body bytes were counted inside `stream_upload`.
