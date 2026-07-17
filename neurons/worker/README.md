# Beam Network Worker

A worker node for the Beam Network — an open coordination layer for distributed data transfer built on Bittensor.

Workers receive data transfer tasks, fetch chunks from a source, deliver them to a destination, and report completion with bandwidth metrics.

## Requirements

- Python 3.10+
- CPU: 2+ cores
- RAM: 4 GB+
- Storage: 20 GB SSD
- Network: 100 Mbps symmetric (upload/download)
- OS: Ubuntu 22.04+ / Debian 12+ / macOS 13+

## Installation

From `beam/` (recommended; matches subnet dependencies):

```bash
pip install -e "."
```

The worker runtime also relies on packages declared in [`pyproject.toml`](../../pyproject.toml); for a minimal manual install:

```bash
pip install bittensor httpx websockets
```

## Usage

Run from `beam/neurons/worker`:

```bash
# Default wallet
python worker.py

# Custom wallet
python worker.py --wallet.name my_wallet --wallet.hotkey my_hotkey

# Mainnet
python worker.py --subtensor.network finney

# Multi-worker on one host (per-instance env file)
python worker.py --env-file config/workers/worker1.env
# Or: ./scripts/run-worker.sh worker1
```

See [Worker Guide](../../docs/worker.md#multiple-workers-on-one-machine) for full multi-instance setup.

## Transport

The worker uses BeamCore HTTP only for registration and signed bootstrap calls. Transfer runtime uses a worker gateway WebSocket (`WORKER_GATEWAY_URL` is the gateway HTTP/WebSocket origin — orchestrator in-process gateway or shared global gateway, not BeamCore).

Typical environment (orchestrator in-process gateway):

```bash
export CORE_SERVER_URL=https://beamcore.b1m.ai
export WORKER_GATEWAY_URL=https://your-orchestrator.example.com
export WORKER_GATEWAY_SECRET=your-long-random-worker-secret
export CONNECTION_MODE=auto               # or websocket (see worker.py)
python worker.py --subtensor.network finney
```

Shared global gateway (multiple orchestrators, one worker pool):

```bash
export WORKER_GATEWAY_URL=http://your-global-gateway.example.com:8005
export WORKER_GATEWAY_SECRET=your-long-random-worker-secret
```

## How It Works

1. Registers with the network using your Bittensor wallet (signed authentication)
2. Connects to the worker gateway via WebSocket to receive `task_offer` messages
3. For each offer: executes the transfer immediately (no accept/reject step)
4. Sends `task_result` after the transfer and waits for `task_result_ack`, retrying until the ack carries a terminal `status` (e.g. `completed`, `failed`, `rejected`) for scoring
5. Sends periodic heartbeats to stay registered

## Environment Variables

| Variable            | Required | Description |
| ------------------- | -------- | ----------- |
| `CORE_SERVER_URL`   | no       | BeamCore HTTP base. |
| `WORKER_GATEWAY_URL`        | **yes**  | Worker-gateway base URL (`http(s)://host:port` — orchestrator or global gateway). |
| `WORKER_GATEWAY_SECRET`     | **yes**  | Sent as `worker_secret` on the gateway WebSocket query string. |
| `WORKER_TASK_RESULT_ACK_TIMEOUT` | no | Seconds to wait for `task_result_ack` (default `45`; must exceed orchestrator `ORCH_TASK_RESULT_TIMEOUT`). |
| `WORKER_TASK_RESULT_SEND_ATTEMPTS` | no | Max attempts to send `task_result` / await a terminal ack status (default `8`). |
| `WORKER_TASK_RESULT_RECONNECT_WAIT_SECONDS` | no | Wait between `task_result` retries when a send fails (default `2`). |
| `WORKER_PREWARM_ENABLED` | no | `HEAD` origins on startup and before each transfer to warm DNS/TLS (default `true`). |
| `WORKER_PREWARM_TIMEOUT` | no | Seconds per origin prewarm request (default `5`). |
| `WORKER_PREWARM_MAX_ORIGINS` | no | Max persisted origins in `config/workers/<instance>.prewarm-hosts.json` (default `32`). |
| `WORKER_PREWARM_ORIGINS` | no | Optional comma-separated origin seeds (e.g. R2 bucket host) before first task. |
| `CONNECTION_MODE`   | no       | `websocket` / `polling` / `auto` (default `websocket` in env). Transfer path expects gateway WebSockets. |
