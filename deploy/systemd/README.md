# BEAM systemd units

Production services for orchestrator and workers.

All commands run from the **repo root** as a normal user (on AWS EC2: `ubuntu`). Use `sudo` only for `install-systemd.sh`; service units run as that user, not root.

---

## Install matrix (2 host types × 2 roles)

| | **Orchestrator** | **Worker** |
| --- | --- | --- |
| **EC2** (`ubuntu`) | `setup-ec2.sh` *(optional)* → `setup-orch-host.sh` | `setup-ec2.sh` *(optional)* → `setup-worker-host.sh` |
| **Normal** Ubuntu VPS | same scripts | same scripts |

Host type does **not** change the commands — only the public IP / security group. Role scripts work on both.

| Script | What it does |
| --- | --- |
| `scripts/setup-ec2.sh` | Host bootstrap only: apt + `.venv` + `pip install -e .` (EC2 **or** normal) |
| `scripts/setup-orch-host.sh` | Orch role: deps + wallet + `config/orchestrators/*.env` + systemd |
| `scripts/setup-worker-host.sh` | Worker role: deps + wallet + `config/workers/*.env` + systemd |
| `scripts/install-systemd.sh` | Install/enable unit files (called by role scripts with `--install-systemd`) |

### Orchestrator host (EC2 or normal)

```bash
./scripts/setup-orch-host.sh \
  --create-wallet --wallet-name orchestrator --wallet-hotkey orch1 \
  --api-port 9005 \
  --gateway-url http://YOUR_PUBLIC_IP:9005 \
  --gateway-secret wgs \
  --write-env --install-systemd

./scripts/register-orchestrator.sh orch1 --write-env
./scripts/run-orchestrator.sh orch1
# Open TCP YOUR_PUBLIC_IP:9005 to workers
```

### Worker host (EC2 or normal)

```bash
./scripts/setup-worker-host.sh \
  --create-wallet --wallet-name sn105_w --wallet-hotkey sn105_w1 \
  --gateway-url ws://ORCH_PUBLIC_IP:9005 \
  --gateway-secret wgs \
  --write-env --install-systemd

./scripts/run-worker.sh worker1
```

`WORKER_GATEWAY_URL` / secret must match the orch `ORCHESTRATOR_WORKER_GATEWAY_URL` / `WORKER_GATEWAY_SECRET`.

Do **not** run setup/pip as root (breaks `.venv` / `beam.egg-info` ownership).

---

## One-time setup (manual)

```bash
# Install unit templates
./scripts/install-systemd.sh --enable

# Enable instances after creating env files
./scripts/install-systemd.sh --enable-orchestrators
./scripts/install-systemd.sh --enable-workers
```

`--enable` installs template unit files only. Instances are enabled separately once their `.env` files exist.

Enable only specific instances:

```bash
./scripts/install-systemd.sh --enable-orchestrators --instances orch1,orch2
./scripts/install-systemd.sh --enable-workers --instances worker1,worker2
```

---

## Orchestrators

| Env file | Systemd unit | Log |
| -------- | ------------ | --- |
| `config/orchestrators/orch1.env` | `beam-orchestrator@orch1.service` | `logs/orchestrators/orch1.log` |
| `config/orchestrators/orch2.env` | `beam-orchestrator@orch2.service` | `logs/orchestrators/orch2.log` |

Each orchestrator hosts the worker WebSocket gateway at `/ws/{worker_id}` on `API_PORT`. Set `ORCHESTRATOR_WORKER_GATEWAY_URL` and `WORKER_GATEWAY_SECRET` in the orchestrator env; workers use matching `WORKER_GATEWAY_URL` and `WORKER_GATEWAY_SECRET`.

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
./scripts/install-systemd.sh --enable-orchestrators && ./scripts/run-orchestrators.sh start
./scripts/install-systemd.sh --enable-workers && ./scripts/run-workers.sh start

# Restart
./scripts/run-orchestrators.sh restart
./scripts/run-workers.sh restart
./scripts/run-orchestrator.sh orch1 --restart
./scripts/run-worker.sh worker1 --restart

# Stop
./scripts/run-orchestrators.sh stop
./scripts/run-workers.sh stop

# Status
./scripts/run-orchestrators.sh status
./scripts/run-workers.sh status
```

Use `--foreground` on any per-instance run script to bypass systemd for debugging.

---

## Logs

| Service | Log path |
| ------- | -------- |
| Orchestrator `@instance` | `logs/orchestrators/<instance>.log` |
| Worker `@instance` | `logs/workers/<instance>.log` |

Startup errors for orchestrators also appear in the journal:

```bash
journalctl -u beam-orchestrator@orch1 -f
```
