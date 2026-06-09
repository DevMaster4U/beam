# BEAM Worker Guide

Run a worker on BEAM mainnet.

## Public Endpoints

| Service | Environment variable | URL |
| ------- | -------------------- | --- |
| Core server | `CORE_SERVER_URL` | `https://beamcore.b1m.ai` |
| Worker gateway | `WORKER_GATEWAY_URL` | `https://public-worker-gateway.b1m.ai` |

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
WORKER_GATEWAY_URL=https://public-worker-gateway.b1m.ai
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

# 2. Enable workers (after orchestrator/gateway: ./scripts/install-systemd.sh --enable)
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
| `.env` | Shared: `CORE_SERVER_URL`, `WORKER_GATEWAY_URL`, `SUBTENSOR_NETWORK` |
| `config/workers/worker1.env` | Instance 1: wallet hotkey, concurrency limits |
| `config/workers/worker2.env` | Instance 2: unique hotkey and limits |
| `logs/workers/worker1.log` | Instance 1 log (`beam-worker@worker1.service`) |
| `beam-workers.target` | Starts all `config/workers/*.env` instances |

Manual foreground run with a specific env file:

```bash
python neurons/worker/worker.py --env-file config/workers/worker1.env
```

Split `WORKER_MAX_CONCURRENT_TASKS` across instances so total concurrency fits your uplink/CPU (e.g. two workers × 2 tasks = 4 total).

## Troubleshooting

- Verify the hotkey is registered on subnet 105.
- Verify `WORKER_GATEWAY_URL=https://public-worker-gateway.b1m.ai`.
- Verify `CORE_SERVER_URL=https://beamcore.b1m.ai`.
- If the worker starts but receives no tasks, keep it connected and confirm the gateway URL is reachable from the host.
