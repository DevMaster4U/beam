# BEAM Orchestrator Onboarding Guide

This guide covers the public mainnet orchestrator path for Beam subnet 105. An orchestrator keeps a WebSocket session to the orchestrator gateway, advertises a worker gateway, selects connected workers for task offers, and forwards worker decisions/results back to BeamCore.

## Runtime Responsibilities

The orchestrator process:

1. Registers on the Beam orchestrator gateway using wallet-signed WebSocket messages.
2. Advertises its HTTP URL and worker gateway URL.
3. Maintains an in-process worker gateway at `/ws/<worker_id>` (auth via `api_key` or `worker_secret`).
4. Receives `worker_task_offer_batch` messages from BeamCore through `ORCH_GATEWAY_URL`.
5. Selects connected local workers and sends `task_offer` messages.
6. Relays `task_accept`, `task_reject`, and `task_result` messages upstream.
7. Stays `READY=true` when it should receive routed production work.

Workers use BeamCore HTTP for registration, but runtime task delivery uses the worker gateway.

## Mainnet Endpoints

| Setting | Value |
|---|---|
| `CORE_SERVER_URL` | `https://beamcore.b1m.ai` |
| `ORCH_GATEWAY_URL` | `tls://orch-gateway.b1m.ai:4222` |
| `ORCHESTRATOR_WORKER_GATEWAY_URL` | Your externally reachable worker gateway origin |
| `SUBTENSOR_NETWORK` | `finney` |
| `NETUID` | `105` |

`ORCHESTRATOR_WORKER_GATEWAY_URL` and worker `WORKER_GATEWAY_URL` must match when workers connect through a public domain or reverse proxy.

## Requirements

| Component | Requirement |
|---|---|
| Python | 3.10-3.12 |
| Wallet | Registered miner hotkey on subnet 105 |
| Network | Stable outbound access to BeamCore, orch-gateway, Bittensor, and storage backends |
| Port | Default orchestrator HTTP/worker-gateway port `9000` unless `API_PORT` is changed |

## Install

```bash
git clone https://github.com/Beam-Network/beam.git
cd beam
python3 -m venv .venv
source .venv/bin/activate
pip install -e "."
```

## Register On Subnet 105

```bash
btcli subnet register --netuid 105 --subtensor.network finney \
  --wallet.name orchestrator --wallet.hotkey default
```

## Configure

### Single instance (dev)

```bash
cp neurons/orchestrator/.env.example neurons/orchestrator/.env
```

Example `neurons/orchestrator/.env`:

```dotenv
WALLET_NAME=orchestrator
WALLET_HOTKEY=default
CORE_SERVER_URL=https://beamcore.b1m.ai
ORCH_GATEWAY_URL=tls://orch-gateway.b1m.ai:4222
ORCHESTRATOR_WORKER_GATEWAY_URL=https://orchestrator.example.com
WORKER_GATEWAY_SECRET=your-long-random-worker-secret
SUBTENSOR_NETWORK=finney
NETUID=105
API_PORT=9000
READY=true
```

### Multiple orchestrators on one host

Each instance needs its own env file with a **unique** `API_PORT`, `WALLET_HOTKEY`, and worker gateway URL:

```bash
cp config/orchestrators/orch1.env.example config/orchestrators/orch1.env
cp config/orchestrators/orch2.env.example config/orchestrators/orch2.env
# edit each file — unique WALLET_HOTKEY, API_PORT, ORCHESTRATOR_WORKER_GATEWAY_URL
```

| Path | Purpose |
|---|---|
| `.env` | Shared: `CORE_SERVER_URL`, `ORCH_GATEWAY_URL`, `SUBTENSOR_NETWORK` |
| `config/orchestrators/orch1.env` | Instance 1 wallet, port, gateway URL |
| `config/orchestrators/orch2.env` | Instance 2 wallet, port, gateway URL |
| `logs/orchestrators/<instance>.log` | Per-instance log file |

Optional: set `ORCHESTRATOR_UID` in each env file to skip slow subtensor bootstrap when the on-chain UID is already known.

Important settings:

| Variable | Purpose |
|---|---|
| `CORE_SERVER_URL` | BeamCore HTTP base used for registration/auth bootstrap |
| `ORCH_GATEWAY_URL` | Orchestrator gateway origin used for the persistent control-plane WebSocket |
| `ORCHESTRATOR_WORKER_GATEWAY_URL` | Public worker gateway origin advertised to BeamCore |
| `WORKER_GATEWAY_SECRET` | Shared secret for worker WebSocket auth on `/ws/{worker_id}` |
| `READY` | `true` opts the orchestrator into routed work; default is `false` |
| `API_PORT` | FastAPI port and in-process worker-gateway port |
| `ORCHESTRATOR_UID` | Optional; skip subtensor metagraph lookup when preset |

## Run

### Single instance (foreground)

```bash
cd neurons/orchestrator
python main.py
```

### Multiple instances (systemd)

```bash
./scripts/install-systemd.sh --enable
./scripts/install-systemd.sh --enable-orchestrators
./scripts/run-orchestrator.sh orch1
./scripts/run-orchestrators.sh start
```

See [deploy/systemd/README.md](../deploy/systemd/README.md).

## Health And Readiness

Replace `9000` with your instance `API_PORT`:

```bash
curl http://localhost:9000/health
curl http://localhost:9000/ready | jq
curl http://localhost:9000/state | jq
```

Logs for multi-instance runs: `logs/orchestrators/<instance>.log`.

## Worker Gateway

The in-process worker gateway accepts:

```text
ws(s)://<worker-gateway-origin>/ws/<worker_id>?api_key=<worker-api-key>&worker_secret=<shared-secret>
```

Workers derive this URL from `WORKER_GATEWAY_URL`. Set matching values on orchestrator and workers:

```dotenv
ORCHESTRATOR_WORKER_GATEWAY_URL=https://orchestrator.example.com
WORKER_GATEWAY_SECRET=your-long-random-worker-secret
```

Worker env:

```dotenv
WORKER_GATEWAY_URL=https://orchestrator.example.com
WORKER_GATEWAY_SECRET=your-long-random-worker-secret
```

## Task Offer Flow

```text
BeamCore -> orch-gateway -> orchestrator -> worker gateway -> worker
worker -> worker gateway -> orchestrator -> orch-gateway -> BeamCore task_result
```

## Troubleshooting

### No tasks are assigned

- Confirm `READY=true`.
- Confirm the orchestrator WebSocket is connected to `ORCH_GATEWAY_URL`.
- Confirm the hotkey is registered on subnet 105.
- Confirm at least one worker is connected to the worker gateway.
- Check `/ready` for failed readiness checks.

### Worker cannot connect

- Confirm `WORKER_GATEWAY_URL` matches `ORCHESTRATOR_WORKER_GATEWAY_URL`.
- Confirm `WORKER_GATEWAY_SECRET` matches when using secret auth.
- Confirm the gateway is reachable from the worker host.

### Multiple orchestrators fail to start

- Each `config/orchestrators/*.env` must use a unique `API_PORT` and `WALLET_HOTKEY`.
- Run `./scripts/run-orchestrators.sh status` to see per-instance ports and logs.
- Scripts validate duplicate ports/wallets before start.

### BeamCore or orch-gateway connection fails

```bash
curl https://beamcore.b1m.ai/health
```

Check network egress, DNS, wallet signing errors, and `ORCH_GATEWAY_URL=tls://orch-gateway.b1m.ai:4222`.
