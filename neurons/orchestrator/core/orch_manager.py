"""In-memory orchestrator registry for local API routes."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from typing import Dict, Optional, Set


class OrchestratorStatus(str, Enum):
    ACTIVE = "active"
    GRACE = "grace"
    INACTIVE = "inactive"


class DatastreamStatus(str, Enum):
    ACTIVE = "active"
    TERMINATED = "terminated"


@dataclass
class ManagedDatastream:
    datastream_id: str
    name: str
    description: Optional[str] = None
    status: DatastreamStatus = DatastreamStatus.ACTIVE
    created_at: datetime = field(default_factory=datetime.utcnow)
    total_bytes_transferred: int = 0
    total_transfers: int = 0
    success_rate: float = 0.0

    @property
    def is_active(self) -> bool:
        return self.status == DatastreamStatus.ACTIVE


@dataclass
class ManagedOrchestrator:
    uid: int
    hotkey: str
    name: Optional[str] = None
    description: Optional[str] = None
    contact: Optional[str] = None
    status: OrchestratorStatus = OrchestratorStatus.ACTIVE
    is_subnet_owned: bool = False
    registered_at: datetime = field(default_factory=datetime.utcnow)
    grace_period_ends: Optional[datetime] = None
    worker_hotkeys: Set[str] = field(default_factory=set)
    datastreams: Dict[str, ManagedDatastream] = field(default_factory=dict)


class OrchestratorManager:
    """Local in-memory orchestrator manager used when beam.orchestrator is unavailable."""

    MAX_DATASTREAMS = 50
    GRACE_PERIOD_DAYS = 7

    def __init__(self) -> None:
        self.orchestrators: Dict[int, ManagedOrchestrator] = {}
        self._seed_subnet_orchestrator()

    def _seed_subnet_orchestrator(self) -> None:
        self.orchestrators[1] = ManagedOrchestrator(
            uid=1,
            hotkey="subnet-orchestrator",
            name="Subnet Orchestrator",
            is_subnet_owned=True,
        )

    def get_orchestrator(self, uid: int) -> Optional[ManagedOrchestrator]:
        return self.orchestrators.get(uid)

    def register_orchestrator(
        self,
        uid: int,
        hotkey: str,
        name: Optional[str] = None,
        description: Optional[str] = None,
        contact: Optional[str] = None,
    ) -> ManagedOrchestrator:
        if uid == 1:
            raise ValueError("UID 1 is reserved for subnet orchestrator")
        if uid in self.orchestrators:
            raise ValueError(f"Orchestrator UID {uid} already registered")

        grace = datetime.utcnow() + timedelta(days=self.GRACE_PERIOD_DAYS)
        orch = ManagedOrchestrator(
            uid=uid,
            hotkey=hotkey,
            name=name,
            description=description,
            contact=contact,
            status=OrchestratorStatus.GRACE,
            grace_period_ends=grace,
        )
        self.orchestrators[uid] = orch
        return orch

    def deregister_orchestrator(self, uid: int) -> bool:
        if uid == 1 or uid not in self.orchestrators:
            return False
        del self.orchestrators[uid]
        return True

    def create_datastream(
        self,
        orchestrator_uid: int,
        datastream_id: str,
        name: str,
        description: Optional[str] = None,
    ) -> ManagedDatastream:
        orch = self.get_orchestrator(orchestrator_uid)
        if not orch:
            raise ValueError(f"Orchestrator UID {orchestrator_uid} not found")

        active = [ds for ds in orch.datastreams.values() if ds.is_active]
        if len(active) >= self.MAX_DATASTREAMS:
            raise ValueError("Maximum datastreams reached")
        if datastream_id in orch.datastreams:
            raise ValueError(f"Datastream {datastream_id} already exists")

        ds = ManagedDatastream(
            datastream_id=datastream_id,
            name=name,
            description=description,
        )
        orch.datastreams[datastream_id] = ds
        return ds

    def terminate_datastream(self, uid: int, datastream_id: str) -> bool:
        orch = self.get_orchestrator(uid)
        if not orch:
            return False
        ds = orch.datastreams.get(datastream_id)
        if not ds:
            return False
        ds.status = DatastreamStatus.TERMINATED
        return True

    def affiliate_worker(self, worker_id: str, worker_hotkey: str, orchestrator_uid: int) -> bool:
        del worker_id  # affiliation tracked by hotkey in local mode
        orch = self.get_orchestrator(orchestrator_uid)
        if not orch:
            return False
        orch.worker_hotkeys.add(worker_hotkey)
        return True
