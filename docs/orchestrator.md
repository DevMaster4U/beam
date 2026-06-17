# BEAM Orchestrator Guide

Run an orchestrator on BEAM mainnet.

## Public Endpoints

| Service | Environment variable | URL |
| ------- | -------------------- | --- |
| Core server | `CORE_SERVER_URL` | `https://beamcore.b1m.ai` |
| Orchestrator gateway | `ORCH_GATEWAY_URL` | `https://orch-gateway.b1m.ai` |

Workers use `WORKER_GATEWAY_URL=https://public-worker-gateway.b1m.ai` (Option 2, default).

For Option 1 (dedicated worker-gateway), set `WORKER_GATEWAY_MODE=dedicated` — see [Worker Gateway Guide](worker-gateway.md).

## Requirements

- Python 3.10-3.12
- A Bittensor wallet with a registered hotkey on subnet 105
- Stable public network connectivity

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

Create a per-instance env file (multi-instance on one host):

```bash
cp config/orchestrators/orch1.env.example config/orchestrators/orch1.env
```

Or for a single dev setup, use `neurons/orchestrator/.env`:

```bash
cp neurons/orchestrator/.env.example neurons/orchestrator/.env
```

Example `config/orchestrators/orch1.env`:

```bash
CORE_SERVER_URL=https://beamcore.b1m.ai
ORCH_GATEWAY_URL=https://orch-gateway.b1m.ai
SUBTENSOR_NETWORK=finney
NETUID=105

WALLET_NAME=your_coldkey
WALLET_HOTKEY=your_hotkey
WALLET_PATH=~/.bittensor/wallets

READY=false
LOG_LEVEL=INFO
```

Set `READY=true` only when the orchestrator is ready to accept transfer work.

## Run

Install systemd unit templates:

```bash
./scripts/install-systemd.sh --enable
./scripts/install-systemd.sh --enable-orchestrators
```

Workers and gateways are installed separately — see [Worker Guide](worker.md) and [Worker Gateway Guide](worker-gateway.md).

Start and manage one instance:

```bash
./scripts/run-orchestrator.sh orch1
./scripts/run-orchestrator.sh orch1 --status
./scripts/run-orchestrator.sh orch1 --restart
./scripts/run-orchestrator.sh orch1 --stop
```

Start all configured orchestrators:

```bash
./scripts/run-orchestrators.sh start
```

Debug in the foreground (bypasses systemd):

```bash
./scripts/run-orchestrator.sh orch1 --foreground
```

Useful health checks:

```bash
curl http://localhost:8000/health
curl http://localhost:8000/state | jq
curl "$CORE_SERVER_URL/health"
```

Application logs are written to `logs/orchestrators/<instance>.log`. Startup errors also appear in the journal:

```bash
journalctl -u beam-orchestrator@orch1 -f
```

See `deploy/systemd/README.md` for all three services.

## Troubleshooting

- Verify the hotkey is registered on subnet 105.
- Verify `CORE_SERVER_URL` and `ORCH_GATEWAY_URL` match the public endpoints above.
- If no tasks arrive, keep the process running and confirm the node has signaled readiness.
