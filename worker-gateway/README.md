# Beam Worker Gateway

Dedicated worker gateway for **Option 1 — Orchestrator-Direct** ([Workers guide](https://data.b1m.ai/guide/workers), [Orchestrators guide](https://data.b1m.ai/guide/orchestrators)).

Workers connect to your gateway instead of the shared BeamCore worker gateway. The orchestrator relays task offers and worker responses through a private control channel.

## Architecture

```text
Worker ──WS /ws/{worker_id}──► Worker Gateway ◄──WS /control── Orchestrator
                                   │                           │
                                   │                           └──WS──► orch-gateway (BeamCore)
Worker ──HTTP──► BeamCore (register + PoB evidence)
```

| Link | Endpoint | Auth |
|------|----------|------|
| Worker data | `wss://{host}/ws/{worker_id}` | `?api_key=` from BeamCore registration |
| Orchestrator control | `wss://{host}/control` | `x-control-secret` header |

## Quick start

### 1. Start the gateway

```bash
cd worker-gateway
cp .env.example .env
# Edit GATEWAY_CONTROL_SECRET

export GATEWAY_CONTROL_SECRET=your-long-random-secret
python main.py
```

Or with Docker:

```bash
docker build -t beam-worker-gateway .
docker run --env-file .env -p 8001:8001 beam-worker-gateway
```

Health check:

```bash
curl http://localhost:8001/health
```

### 2. Configure the orchestrator

Add to `neurons/orchestrator/.env`:

```bash
WORKER_GATEWAY_MODE=dedicated
WORKER_GATEWAY_PUBLIC_URL=https://gateway.example.com
WORKER_GATEWAY_CONTROL_URL=ws://localhost:8001/control
WORKER_GATEWAY_CONTROL_SECRET=your-long-random-secret

ORCH_GATEWAY_URL=https://orch-gateway.b1m.ai
CORE_SERVER_URL=https://beamcore.b1m.ai
READY=true
```

Set **`WORKER_GATEWAY_MODE=dedicated`** on the orchestrator (not just `WORKER_GATEWAY_PUBLIC_URL`). All three gateway variables are required together. The orchestrator will:

- Register with `gateway_url` on BeamCore
- Assign chunks from **locally connected workers** (not `list_public_workers`)
- Relay `worker_task_offer` → workers
- Relay `worker_response` / `task_result_summary` → BeamCore

### 3. Configure your worker

```bash
WORKER_GATEWAY_URL=https://gateway.example.com
WORKER_REQUIRED_PAYMENT=false   # own worker — no per-task on-chain payment
CORE_SERVER_URL=https://beamcore.b1m.ai
```

Workers still register and submit PoB evidence to BeamCore HTTP directly.

## Protocol

### Worker → gateway

| Message | Action |
|---------|--------|
| `task_accept` | Relayed as `worker_response` to orchestrator control |
| `task_reject` | Relayed as `worker_response` with `decision: task_reject` |
| `task_result_summary` | Relayed to orchestrator control |
| `stats_snapshot` | Relayed as `worker_capacity_update` |

### Orchestrator → gateway (control)

| Message | Action |
|---------|--------|
| `list_workers` | Returns connected worker sessions |
| `task_offer` | Forwarded verbatim to target worker |
| `task_accept_ack` | Forwarded to worker after BeamCore lease confirmation |
| `task_result_summary_ack` | Forwarded to worker after BeamCore verification |

### Gateway → orchestrator (control push)

| Message | When |
|---------|------|
| `worker_connected` | Worker opens `/ws/{worker_id}` |
| `worker_disconnected` | Worker disconnects |
| `worker_response` | Worker accepts or rejects a task |
| `task_result_summary` | Worker reports chunk completion |
| `worker_capacity_update` | Worker sends `stats_snapshot` |

## Production notes

- Use **TLS** (`wss://`) on a public hostname for `WORKER_GATEWAY_PUBLIC_URL`
- Put a reverse proxy (nginx, Caddy) in front if terminating TLS externally
- `GATEWAY_CONTROL_SECRET` must match on gateway and orchestrator
- Control channel can stay on a private network (`ws://` internally) if workers reach the public data endpoint only
- BeamCore assignment engine uses `gateway_type` and `gateway_url` from orchestrator registration ([beam-core-public](https://github.com/Beam-Network/beam-core-public))

## Related

- [Subnet 105 orchestrator](../docs/orchestrator.md)
- [Subnet 105 worker](../docs/worker.md)
- [Worker gateway mode guide](../docs/worker-gateway.md)
- [Dedicated gateway docs](https://data.b1m.ai/guide/orchestrators#running-your-own-worker-gateway)
