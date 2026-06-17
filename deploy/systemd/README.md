# BEAM systemd units

Production services for orchestrator, worker-gateway, and workers.

All commands run from the **repo root**.

Each service type uses the same multi-instance pattern: one env file per instance under `config/<service>/`, a systemd template unit, and a generated `.target` for all instances.

---

## One-time setup

```bash
# Install unit templates
./scripts/install-systemd.sh --enable

# Enable instances after creating env files
./scripts/install-systemd.sh --enable-gateways
./scripts/install-systemd.sh --enable-orchestrators
./scripts/install-systemd.sh --enable-workers
```

`--enable` installs template unit files only. Instances are enabled separately once their `.env` files exist.

Enable only specific instances:

```bash
./scripts/install-systemd.sh --enable-orchestrators --instances orch1,orch2
./scripts/install-systemd.sh --enable-gateways --instances gateway1,gateway2
./scripts/install-systemd.sh --enable-workers --instances worker1,worker2
```

---

## Orchestrators

| Env file | Systemd unit | Log |
| -------- | ------------ | --- |
| `config/orchestrators/orch1.env` | `beam-orchestrator@orch1.service` | `logs/orchestrators/orch1.log` |
| `config/orchestrators/orch2.env` | `beam-orchestrator@orch2.service` | `logs/orchestrators/orch2.log` |

### Install (first time)

```bash
cp config/orchestrators/orch1.env.example config/orchestrators/orch1.env
# edit orch1.env — unique WALLET_HOTKEY and API_PORT per instance

./scripts/install-systemd.sh --enable-orchestrators
./scripts/run-orchestrators.sh start
```

### All orchestrators

| Action | Command |
| ------ | ------- |
| Start all | `./scripts/run-orchestrators.sh start` |
| Restart all | `./scripts/run-orchestrators.sh restart` |
| Stop all | `./scripts/run-orchestrators.sh stop` |
| Status all | `./scripts/run-orchestrators.sh status` |

Via systemd:

```bash
sudo systemctl start beam-orchestrators.target
sudo systemctl restart beam-orchestrators.target
sudo systemctl stop beam-orchestrators.target
sudo systemctl status beam-orchestrators.target
```

### One orchestrator

| Action | Command |
| ------ | ------- |
| Start | `./scripts/run-orchestrator.sh orch1` |
| Restart | `./scripts/run-orchestrator.sh orch1 --restart` |
| Stop | `./scripts/run-orchestrator.sh orch1 --stop` |
| Status | `./scripts/run-orchestrator.sh orch1 --status` |
| Logs | `tail -f logs/orchestrators/orch1.log` |
| Debug (foreground) | `./scripts/run-orchestrator.sh orch1 --foreground` |

Via systemd:

```bash
sudo systemctl start beam-orchestrator@orch1
sudo systemctl restart beam-orchestrator@orch1
sudo systemctl stop beam-orchestrator@orch1
sudo systemctl status beam-orchestrator@orch1
journalctl -u beam-orchestrator@orch1 -f
```

---

## Gateways

| Env file | Systemd unit | Log |
| -------- | ------------ | --- |
| `config/gateways/gateway1.env` | `beam-worker-gateway@gateway1.service` | `logs/gateways/gateway1.log` |
| `config/gateways/gateway2.env` | `beam-worker-gateway@gateway2.service` | `logs/gateways/gateway2.log` |

### Install (first time)

```bash
cp config/gateways/gateway1.env.example config/gateways/gateway1.env
# edit gateway1.env — unique GATEWAY_PORT and secrets per instance

./scripts/install-systemd.sh --enable-gateways
./scripts/run-gateways.sh start
```

### All gateways

| Action | Command |
| ------ | ------- |
| Start all | `./scripts/run-gateways.sh start` |
| Restart all | `./scripts/run-gateways.sh restart` |
| Stop all | `./scripts/run-gateways.sh stop` |
| Status all | `./scripts/run-gateways.sh status` |

Via systemd:

```bash
sudo systemctl start beam-gateways.target
sudo systemctl restart beam-gateways.target
sudo systemctl stop beam-gateways.target
sudo systemctl status beam-gateways.target
```

### One gateway

| Action | Command |
| ------ | ------- |
| Start | `./scripts/run-worker-gateway.sh gateway1` |
| Restart | `./scripts/run-worker-gateway.sh gateway1 --restart` |
| Stop | `./scripts/run-worker-gateway.sh gateway1 --stop` |
| Status | `./scripts/run-worker-gateway.sh gateway1 --status` |
| Logs | `tail -f logs/gateways/gateway1.log` |
| Debug (foreground) | `./scripts/run-worker-gateway.sh gateway1 --foreground` |

Via systemd:

```bash
sudo systemctl start beam-worker-gateway@gateway1
sudo systemctl restart beam-worker-gateway@gateway1
sudo systemctl stop beam-worker-gateway@gateway1
sudo systemctl status beam-worker-gateway@gateway1
```

Required per gateway env: `GATEWAY_CONTROL_SECRET` and `GATEWAY_WORKER_SECRET` (or `WORKER_GATEWAY_*` variants).

---

## Workers

Each worker instance needs its own env file:

| Env file | Systemd unit | Log |
| -------- | ------------ | --- |
| `config/workers/worker1.env` | `beam-worker@worker1.service` | `logs/workers/worker1.log` |
| `config/workers/worker2.env` | `beam-worker@worker2.service` | `logs/workers/worker2.log` |
| `config/workers/worker3.env` | `beam-worker@worker3.service` | `logs/workers/worker3.log` |

### Install workers (first time)

```bash
cp config/workers/worker1.env.example config/workers/worker1.env
cp config/workers/worker2.env.example config/workers/worker2.env
# edit each file — unique WORKER_WALLET_HOTKEY per instance

./scripts/install-systemd.sh --enable-workers
./scripts/run-workers.sh start
```

### All workers

| Action | Command |
| ------ | ------- |
| Start all | `./scripts/run-workers.sh start` |
| Restart all | `./scripts/run-workers.sh restart` |
| Stop all | `./scripts/run-workers.sh stop` |
| Status all | `./scripts/run-workers.sh status` |

Via systemd:

```bash
sudo systemctl start beam-workers.target
sudo systemctl restart beam-workers.target
sudo systemctl stop beam-workers.target
sudo systemctl status beam-workers.target
```

### One worker

| Action | Command |
| ------ | ------- |
| Start | `./scripts/run-worker.sh worker1` |
| Restart | `./scripts/run-worker.sh worker1 --restart` |
| Stop | `./scripts/run-worker.sh worker1 --stop` |
| Status | `./scripts/run-worker.sh worker1 --status` |
| Logs | `tail -f logs/workers/worker1.log` |
| Debug (foreground) | `./scripts/run-worker.sh worker1 --foreground` |

Via systemd:

```bash
sudo systemctl start beam-worker@worker1
sudo systemctl restart beam-worker@worker1
sudo systemctl stop beam-worker@worker1
sudo systemctl status beam-worker@worker1
```

### Add another worker later

```bash
cp config/workers/worker1.env.example config/workers/worker3.env
# edit worker3.env

./scripts/install-systemd.sh --enable-workers
./scripts/run-worker.sh worker3
```

---

## Typical startup order

```bash
./scripts/install-systemd.sh --enable

./scripts/install-systemd.sh --enable-gateways
./scripts/run-gateways.sh start

./scripts/install-systemd.sh --enable-orchestrators
./scripts/run-orchestrators.sh start

./scripts/install-systemd.sh --enable-workers
./scripts/run-workers.sh start
```

---

## Quick copy-paste

```bash
# Install templates
./scripts/install-systemd.sh --enable

# Enable + start (after creating env files)
./scripts/install-systemd.sh --enable-gateways && ./scripts/run-gateways.sh start
./scripts/install-systemd.sh --enable-orchestrators && ./scripts/run-orchestrators.sh start
./scripts/install-systemd.sh --enable-workers && ./scripts/run-workers.sh start

# Restart
./scripts/run-orchestrators.sh restart
./scripts/run-gateways.sh restart
./scripts/run-workers.sh restart
./scripts/run-orchestrator.sh orch1 --restart
./scripts/run-worker-gateway.sh gateway1 --restart
./scripts/run-worker.sh worker1 --restart

# Stop
./scripts/run-orchestrators.sh stop
./scripts/run-gateways.sh stop
./scripts/run-workers.sh stop

# Status
./scripts/run-orchestrators.sh status
./scripts/run-gateways.sh status
./scripts/run-workers.sh status
```

Use `--foreground` on any per-instance run script to bypass systemd for debugging.

---

## Logs

| Service | Log path |
| ------- | -------- |
| Orchestrator `@instance` | `logs/orchestrators/<instance>.log` |
| Worker gateway `@instance` | `logs/gateways/<instance>.log` |
| Worker `@instance` | `logs/workers/<instance>.log` |

Startup errors for orchestrators also appear in the journal:

```bash
journalctl -u beam-orchestrator@orch1 -f
```
