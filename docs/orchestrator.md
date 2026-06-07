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

Create `neurons/orchestrator/.env`:

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

```bash
cd neurons/orchestrator
python main.py
```

Useful health checks:

```bash
curl http://localhost:8000/health
curl http://localhost:8000/state | jq
curl "$CORE_SERVER_URL/health"
```

## systemd

Use `/srv/beam/beam` as the example checkout path below. Adjust paths if you deploy elsewhere.

```ini
[Unit]
Description=BEAM Orchestrator
After=network.target

[Service]
Type=simple
User=beam
WorkingDirectory=/srv/beam/beam/neurons/orchestrator
Environment="PATH=/srv/beam/beam/.venv/bin:/usr/local/bin:/usr/bin:/bin"
EnvironmentFile=/srv/beam/beam/neurons/orchestrator/.env
ExecStart=/srv/beam/beam/.venv/bin/python main.py
Restart=always
RestartSec=10

[Install]
WantedBy=multi-user.target
```

## Troubleshooting

- Verify the hotkey is registered on subnet 105.
- Verify `CORE_SERVER_URL` and `ORCH_GATEWAY_URL` match the public endpoints above.
- If no tasks arrive, keep the process running and confirm the node has signaled readiness.
