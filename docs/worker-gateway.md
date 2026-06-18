# In-process worker gateway

The orchestrator hosts the worker WebSocket gateway at `/ws/{worker_id}` on `API_PORT`.
Workers connect to the orchestrator directly; no separate gateway process is required.

## Orchestrator

```bash
# config/orchestrators/orch1.env
ORCHESTRATOR_WORKER_GATEWAY_URL=https://your-orchestrator.example.com
WORKER_GATEWAY_WORKER_SECRET=your-long-random-worker-secret
API_PORT=9000
READY=true
ORCH_GATEWAY_URL=https://orch-gateway.b1m.ai
CORE_SERVER_URL=https://beamcore.b1m.ai
```

## Worker

```bash
# config/workers/worker1.env
WORKER_GATEWAY_URL=https://your-orchestrator.example.com
WORKER_GATEWAY_WORKER_SECRET=your-long-random-worker-secret
WORKER_REQUIRED_PAYMENT=false
CORE_SERVER_URL=https://beamcore.b1m.ai
```

`WORKER_GATEWAY_URL` must match `ORCHESTRATOR_WORKER_GATEWAY_URL`.

Workers authenticate with both BeamCore `api_key` and `worker_secret` when configured.

The orchestrator opens a control WebSocket to its own in-process gateway using `control_secret` (defaults to `ws://127.0.0.1:<API_PORT>/ws/<hotkey>?...`).

## Multi-instance

```bash
./scripts/run-orchestrator.sh orch1
./scripts/run-worker.sh worker1
```

See also [Orchestrator guide](orchestrator.md) and [Worker guide](worker.md).
