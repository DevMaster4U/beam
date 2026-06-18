# BEAM Worker Guide

Run a worker on BEAM mainnet.

## Public Endpoints

| Service | Environment variable | URL |
| ------- | -------------------- | --- |
| Core server | `CORE_SERVER_URL` | `https://beamcore.b1m.ai` |
| Worker gateway | `WORKER_GATEWAY_URL` | Your orchestrator HTTP origin (in-process gateway on `API_PORT`) |

## Requirements

- Python 3.10-3.12
- A Bittensor wallet with a registered hotkey on subnet 105
- Stable upload and download bandwidth
- Enough disk space for transfer scratch data

## Install

```bash
git clone https://github.com/Beam-Network/beam.git
cd beam
python3 -m venv .venv
source .venv/bin/activate
pip install -e "."
```

## Register

```bash
btcli subnet register --netuid 105 --subtensor.network finney \
  --wallet.name your_coldkey \
  --wallet.hotkey your_hotkey
```

## Configure

Create or export the worker environment before starting the process:

```bash
CORE_SERVER_URL=https://beamcore.b1m.ai
WORKER_GATEWAY_URL=https://your-orchestrator.example.com
WORKER_GATEWAY_WORKER_SECRET=your-long-random-worker-secret
SUBTENSOR_NETWORK=finney
NETUID=105
```

The worker uses BeamCore HTTP for registration and signed bootstrap calls. Transfer runtime uses `WORKER_GATEWAY_URL` over WebSocket.

## Run

Single worker (uses workspace `.env`):

```bash
cd neurons/worker
python worker.py --wallet.name your_coldkey --wallet.hotkey your_hotkey --subtensor.network finney
```

## Multiple workers on one machine

Each worker needs a **unique registered hotkey** and its own resource limits. Shared network settings stay in the workspace `.env`; per-worker settings live in `config/workers/<name>.env`.

```bash
# 1. Copy and edit one env file per worker (unique WORKER_WALLET_HOTKEY each)
cp config/workers/worker1.env.example config/workers/worker1.env
cp config/workers/worker2.env.example config/workers/worker2.env

# 2. Enable workers (after orchestrator: ./scripts/install-systemd.sh --enable-orchestrators)
./scripts/install-systemd.sh --enable-workers
./scripts/run-workers.sh start

# Or start one instance
./scripts/run-worker.sh worker1
./scripts/run-worker.sh worker2

./scripts/run-workers.sh status
./scripts/run-workers.sh stop
```

Add another worker on the same machine:

```bash
cp config/workers/worker1.env.example config/workers/worker3.env
# edit worker3.env (unique WORKER_WALLET_HOTKEY)

./scripts/install-systemd.sh --enable-workers
./scripts/run-worker.sh worker3
```

Layout:

| Path | Purpose |
| ---- | ------- |
| `.env` | Shared: `CORE_SERVER_URL`, `SUBTENSOR_NETWORK` |
| `config/workers/worker1.env` | Instance 1: wallet hotkey, concurrency limits |
| `config/workers/worker2.env` | Instance 2: unique hotkey and limits |
| `logs/workers/worker1.log` | Instance 1 log (`beam-worker@worker1.service`) |
| `beam-workers.target` | Starts all `config/workers/*.env` instances |

Manual foreground run with a specific env file:

```bash
python neurons/worker/worker.py --env-file config/workers/worker1.env
```

Split `WORKER_MAX_CONCURRENT_TASKS` across instances so total concurrency fits your uplink/CPU (e.g. two workers × 2 tasks = 4 total).

## Host tuning (BBR + concurrency)

Transfers are limited by **per-connection TCP throughput** (BBR helps) and **how many chunks run in parallel** (concurrency helps). Both matter; neither replaces the other.

### BBR (once per worker host)

BBR is a Linux TCP congestion-control setting. It can improve throughput on each GET/PUT leg, especially on cross-region paths to S3/R2. It does not replace raising concurrency when the link is underutilized.

```bash
# Check current setting
sysctl net.ipv4.tcp_congestion_control

# Install (Ubuntu 22.04+; requires sudo)
./scripts/tune-worker-host.sh --install-bbr

# Or manually:
sudo cp deploy/sysctl/99-beam-worker-tcp.conf /etc/sysctl.d/
sudo sysctl --system
```

Expected after install: `net.ipv4.tcp_congestion_control = bbr`.

### Pick `WORKER_MAX_CONCURRENT_TASKS`

Each active task uses about **two HTTP connections** (source GET + destination PUT). Default chunks are **4 MiB**. Uplink is usually the bottleneck.

Interactive helper:

```bash
./scripts/tune-worker-host.sh --link-mbps 500 --workers 2
```

Rule of thumb (symmetric link, 4 MiB chunks):

| Link (Mbps) | Total concurrent tasks (host) | Per worker instance (1 instance) |
| ----------- | ----------------------------- | -------------------------------- |
| 100         | 4                             | 4                                |
| 250         | 10                            | 10                               |
| 500         | 20 → cap ~16                  | 16                               |
| 1000        | 40 → cap ~16                  | 16                               |

Formula: `total_tasks ≈ link_mbps / 25`, split across local worker instances, cap at **16 per instance** unless you have measured headroom.

Also set:

```bash
WORKER_MAX_QUEUED_WS_TASKS=${WORKER_MAX_CONCURRENT_TASKS}
WORKER_MAX_IN_FLIGHT_BYTES=$(( WORKER_MAX_CONCURRENT_TASKS * 8 * 1024 * 1024 ))  # ~2× 4 MiB chunks
```

Example for one 500 Mbps host, two worker processes:

```bash
# worker1.env / worker2.env
WORKER_MAX_CONCURRENT_TASKS=8
WORKER_MAX_QUEUED_WS_TASKS=8
WORKER_MAX_IN_FLIGHT_BYTES=67108864   # 64 MiB
```

Raise concurrency gradually and watch `fetch_ms` / `send_ms` in `logs/workers/*.log`. Back off if you see timeouts or CPU pegged.

### HTTP prewarm (cold connections after idle)

Workers learn HTTP origins from task `source_urls` / `dest_urls` and persist them to:

`config/workers/<instance>.prewarm-hosts.json`

On **startup** and **before each transfer**, the worker issues lightweight `HEAD` requests through the same `httpx` client to warm DNS, TLS, and the connection pool. This helps when tasks arrive infrequently (e.g. every 30 minutes) and the first chunk would otherwise pay full connect cost.

Log examples:

```text
[Worker] Prewarm cache loaded (worker1.prewarm-hosts.json): 1 origin(s)
[Worker] Prewarm startup: 1/1 origin(s) in 42.3ms — ef88....r2.cloudflarestorage.com
[Worker] Prewarm task: 1/1 origin(s) in 38.1ms — ef88....r2.cloudflarestorage.com
```

Optional env (in `config/workers/<instance>.env`):

```bash
WORKER_PREWARM_ENABLED=true
WORKER_PREWARM_TIMEOUT=5
WORKER_PREWARM_MAX_ORIGINS=32
# Optional seed before first task:
# WORKER_PREWARM_ORIGINS=https://account.r2.cloudflarestorage.com
```

## Troubleshooting

- Verify the hotkey is registered on subnet 105.
- Verify `WORKER_GATEWAY_URL` matches the orchestrator's `ORCHESTRATOR_WORKER_GATEWAY_URL`.
- Verify `WORKER_GATEWAY_WORKER_SECRET` matches the orchestrator when using secret auth.
- Verify `CORE_SERVER_URL=https://beamcore.b1m.ai`.
- If the worker starts but receives no tasks, keep it connected and confirm the gateway URL is reachable from the host.
