# BEAM systemd units

Production services for orchestrator, worker-gateway, and workers.

All commands run from the **repo root**.

---

## One-time setup

```bash
# Orchestrator + gateway
./scripts/install-systemd.sh --enable

# Workers (after creating config/workers/*.env)
./scripts/install-systemd.sh --enable-workers
```

`--enable` installs unit files and enables orchestrator + gateway only. Worker unit files are copied (`beam-worker@.service`), but workers are **not** enabled until you run `--enable-workers`.

`--enable-workers` (alias: `--sync-workers`) reads `config/workers/*.env`, generates `beam-workers.target`, and runs `systemctl enable` for each worker. It does **not** start worker processes — use `run-workers.sh start` for that.

Enable only specific workers:

```bash
./scripts/install-systemd.sh --enable-workers --instances worker1,worker2
```

---

## Orchestrator

| Action | Command |
| ------ | ------- |
| Start | `./scripts/run-orchestrator.sh` |
| Restart | `./scripts/run-orchestrator.sh --restart` |
| Stop | `./scripts/run-orchestrator.sh --stop` |
| Status | `./scripts/run-orchestrator.sh --status` |
| Logs | `tail -f logs/miner.log` |
| Debug (foreground) | `./scripts/run-orchestrator.sh --foreground` |

Via systemd:

```bash
sudo systemctl start beam-orchestrator
sudo systemctl restart beam-orchestrator
sudo systemctl stop beam-orchestrator
sudo systemctl status beam-orchestrator
journalctl -u beam-orchestrator -f
```

Config: `.env` (repo root) or `neurons/orchestrator/.env` (fallback).

---

## Gateway

| Action | Command |
| ------ | ------- |
| Start | `./scripts/run-worker-gateway.sh` |
| Restart | `./scripts/run-worker-gateway.sh --restart` |
| Stop | `./scripts/run-worker-gateway.sh --stop` |
| Status | `./scripts/run-worker-gateway.sh --status` |
| Logs | `tail -f logs/gateway.log` |
| Debug (foreground) | `./scripts/run-worker-gateway.sh --foreground` |

Via systemd:

```bash
sudo systemctl start beam-worker-gateway
sudo systemctl restart beam-worker-gateway
sudo systemctl stop beam-worker-gateway
sudo systemctl status beam-worker-gateway
```

Config: `.env` (repo root). Required: `GATEWAY_CONTROL_SECRET` and `GATEWAY_WORKER_SECRET` (or `WORKER_GATEWAY_*` variants).

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
./scripts/run-worker-gateway.sh
./scripts/run-orchestrator.sh

./scripts/install-systemd.sh --enable-workers
./scripts/run-workers.sh start
```

---

## Quick copy-paste

```bash
# Install
./scripts/install-systemd.sh --enable
./scripts/install-systemd.sh --enable-workers

# Start
./scripts/run-worker-gateway.sh
./scripts/run-orchestrator.sh
./scripts/run-workers.sh start

# Restart
./scripts/run-orchestrator.sh --restart
./scripts/run-worker-gateway.sh --restart
./scripts/run-workers.sh restart
./scripts/run-worker.sh worker1 --restart

# Stop
./scripts/run-orchestrator.sh --stop
./scripts/run-worker-gateway.sh --stop
./scripts/run-workers.sh stop
./scripts/run-worker.sh worker1 --stop

# Status
./scripts/run-orchestrator.sh --status
./scripts/run-worker-gateway.sh --status
./scripts/run-workers.sh status
```

Use `--foreground` on any run script to bypass systemd for debugging.

---

## Logs

| Service | Log path |
| ------- | -------- |
| Orchestrator | `logs/miner.log` (application FileHandler) |
| Worker gateway | `logs/gateway.log` (systemd append) |
| Worker `@instance` | `logs/workers/<instance>.log` (systemd append) |

Startup errors for the orchestrator also appear in the journal:

```bash
journalctl -u beam-orchestrator -f
```
