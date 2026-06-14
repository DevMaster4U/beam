# Option 1 vs Option 2 — Worker Gateway Mode

Control the orchestrator worker-pool topology with **`WORKER_GATEWAY_MODE`**:

| Mode | Value | Option | Worker pool | Task path |
|------|-------|--------|-------------|-----------|
| **Public** (default) | `public` | Option 2 | `list_public_workers` from BeamCore | BeamCore public gateway |
| **Dedicated** | `dedicated` | Option 1 | Local worker-gateway sessions | Orchestrator relay via control WS |

`WORKER_GATEWAY_PUBLIC_URL` alone does **not** switch modes. It is only used when `WORKER_GATEWAY_MODE=dedicated`.

---

## Option 2 — Public gateway (default)

```bash
# neurons/orchestrator/.env
WORKER_GATEWAY_MODE=public   # or omit (default)
ORCH_GATEWAY_URL=https://orch-gateway.b1m.ai
CORE_SERVER_URL=https://beamcore.b1m.ai
READY=true
```

```bash
# worker
WORKER_GATEWAY_URL=https://public-worker-gateway.b1m.ai
CORE_SERVER_URL=https://beamcore.b1m.ai
```

No worker-gateway process required.

---

## Option 1 — Dedicated worker-gateway

### 1. Start worker-gateway

```bash
cd worker-gateway
export GATEWAY_CONTROL_SECRET=your-long-random-control-secret
export GATEWAY_WORKER_SECRET=your-long-random-worker-secret
python main.py
```

Or from the repo root (systemd, multi-instance):

```bash
cp config/gateways/gateway1.env.example config/gateways/gateway1.env
./scripts/install-systemd.sh --enable
./scripts/install-systemd.sh --enable-gateways
./scripts/run-worker-gateway.sh gateway1
./scripts/run-worker-gateway.sh gateway1 --status
```

### 2. Orchestrator

```bash
WORKER_GATEWAY_MODE=dedicated
WORKER_GATEWAY_PUBLIC_URL=https://gateway.example.com
WORKER_GATEWAY_CONTROL_URL=ws://localhost:8001/control
WORKER_GATEWAY_CONTROL_SECRET=your-long-random-secret

ORCH_GATEWAY_URL=https://orch-gateway.b1m.ai
CORE_SERVER_URL=https://beamcore.b1m.ai
READY=true
```

### 3. Worker

```bash
WORKER_GATEWAY_URL=https://gateway.example.com
WORKER_GATEWAY_WORKER_SECRET=your-long-random-worker-secret
WORKER_REQUIRED_PAYMENT=false
CORE_SERVER_URL=https://beamcore.b1m.ai
```

Workers still register and submit PoB to BeamCore HTTP.

---

## Protocol reference

Shared message types and field normalization live in `neurons/shared/gateway_protocol.py`, aligned with:

- [Workers guide](https://data.b1m.ai/guide/workers)
- [Orchestrators — dedicated gateway](https://data.b1m.ai/guide/orchestrators#running-your-own-worker-gateway)
- [BeamCore assignment engine](https://github.com/Beam-Network/beam-core-public)

Key behaviors by mode:

| Message | Option 2 (public) | Option 1 (dedicated) |
|---------|-------------------|----------------------|
| `transfer_assigned` | `list_public_workers` → `chunk_assignments` | Gateway `list_workers` → `chunk_assignments` |
| `worker_task_offer` | Ignored (public gateway delivers) | Relayed to worker-gateway → worker |
| `worker_response` | N/A (public gateway relays) | Worker → gateway → orchestrator → BeamCore |
| `task_result_summary` | BeamCore push to orchestrator | Worker → gateway → orchestrator → BeamCore |
| WS `register` `gateway_url` | Not sent | `WORKER_GATEWAY_PUBLIC_URL` |

---

## Validation rules

Startup fails fast when:

- `WORKER_GATEWAY_MODE=dedicated` but any of `WORKER_GATEWAY_PUBLIC_URL`, `WORKER_GATEWAY_CONTROL_URL`, or `WORKER_GATEWAY_CONTROL_SECRET` is missing
- `WORKER_GATEWAY_MODE=public` but control URL/secret are set (misconfiguration)

See also [worker-gateway/README.md](../worker-gateway/README.md).
