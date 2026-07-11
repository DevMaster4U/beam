"""
Validator Core Logic - Unified Version

Validates Proof-of-Bandwidth submissions and sets weights on the Bittensor network.
Supports both local mode (HTTP) and mainnet (Bittensor dendrite).

"""

import asyncio
import base64
import logging
import os
import random
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple

import aiohttp
import bittensor as bt
from chain import FiberChain, FiberNode

# Import shared validator protocol/scoring helpers.
from core._beam_stubs import (
    BANDWIDTH_EMA_ALPHA,
    CANARY_SIZE_BYTES,
    # beam.constants
    SUBNET_ORCHESTRATOR_UID,
    # beam.protocol.synapse
    BandwidthChallenge,
    ChunkTransfer,
    OrchestratorManager,
    PoBVerificationResult,
    # beam.protocol.pob
    ProofOfBandwidth,
    ReassignmentManager,
    # Scoring helpers
    Task,
    WorkerRegistry,
    build_merkle_leaf,
    compute_canary_proof,
    get_sybil_detector,
    # beam.crypto.hashing
    sha256,
    # beam.crypto.signatures
    verify_hotkey_signature,
)
from core.config import Settings, get_settings

# Redundancy and failover imports
from core.redundancy import (
    CheckpointManager,
    HealthMonitor,
    RecoveryManager,
    create_redundancy_system,
    initialize_with_recovery,
)

# SubnetCore API client for score submission (replaces direct DB access)
try:
    from clients import SubnetCoreClient

    SUBNET_CORE_AVAILABLE = True
except ImportError:
    SUBNET_CORE_AVAILABLE = False
    SubnetCoreClient = None

logger = logging.getLogger(__name__)


# =============================================================================
# Data Models
# =============================================================================


@dataclass
class OrchestratorInfo:
    """Information about an Orchestrator endpoint."""

    url: str
    hotkey: str
    uid: Optional[int] = None
    last_seen: datetime = field(default_factory=datetime.utcnow)
    registered_at: Optional[datetime] = None
    is_healthy: bool = True
    is_subnet_owned: bool = False  # True for UID #1
    last_score: float = 0.0


@dataclass
class WorkSummary:
    """Work summary received from Orchestrator."""

    epoch: int
    orchestrator_hotkey: str
    total_tasks: int
    successful_tasks: int
    total_bytes_relayed: int
    active_workers: int
    avg_bandwidth_mbps: float
    avg_latency_ms: float
    success_rate: float
    proof_count: int
    worker_regions: Dict[str, int]
    orchestrator_signature: str

    # Extended work metrics
    uptime_percent: float = 100.0  # Orchestrator uptime over 24h window
    acceptance_rate: float = 100.0  # Task acceptance rate
    latency_p95_ms: float = 0.0  # 95th percentile latency

    # Worker contribution breakdown
    worker_contributions: Optional[Dict[str, int]] = None  # worker_id -> bytes

    # Measurement window
    measurement_start: Optional[datetime] = None
    measurement_end: Optional[datetime] = None


@dataclass
class ChallengeResult:
    """Result of a bandwidth challenge."""

    challenge_id: str
    success: bool
    bytes_relayed: int = 0
    bandwidth_mbps: float = 0.0
    latency_ms: float = 0.0
    canary_verified: bool = False
    error: Optional[str] = None


@dataclass
class ProofVerificationResult:
    """Result of verifying a single proof."""

    task_id: str
    valid: bool
    error: Optional[str] = None

    # Verification details
    signature_valid: bool = False
    timing_valid: bool = False
    bandwidth_valid: bool = False
    canary_valid: bool = False
    geo_valid: bool = False

    # Measured latency from proof timing
    latency_ms: Optional[float] = None


@dataclass
class SpotCheckResult:
    """Result of spot-checking proofs for an orchestrator."""

    orchestrator_hotkey: str
    proofs_requested: int
    proofs_received: int
    proofs_valid: int
    proofs_invalid: int
    verification_rate: float = 0.0

    # Details of invalid proofs
    invalid_proof_ids: List[str] = field(default_factory=list)
    invalid_reasons: Dict[str, str] = field(default_factory=dict)  # task_id -> reason

    # Fraud indicators
    fraud_detected: bool = False
    fraud_severity: float = 0.0  # 0.0 to 1.0

    timestamp: datetime = field(default_factory=datetime.utcnow)


@dataclass
class PoBVerificationStats:
    """Aggregated PoB verification stats for an orchestrator in the current epoch."""

    total_proofs: int = 0
    verified_count: int = 0
    rejected_count: int = 0
    total_bytes_verified: int = 0


# =============================================================================
# Unified Validator
# =============================================================================


class Validator:
    """
    BEAM Validator Node - Unified Version

    Responsibilities:
    - Generate bandwidth challenge tasks
    - Verify Proof-of-Bandwidth submissions
    - Fetch and verify proofs from SubnetCore API
    - Track orchestrator work and payment/fraud signals
    - Set weights on Bittensor network

    Supports both:
    - Local mode: HTTP-based communication with orchestrators
    - Mainnet: Bittensor dendrite-based communication
    """

    def __init__(self, settings: Optional[Settings] = None):
        self.settings = settings or get_settings()

        # Bittensor components
        self.wallet: Optional[bt.Wallet] = None
        self.subtensor: Optional[bt.Subtensor] = None
        self.metagraph: Optional[bt.Metagraph] = None
        self.dendrite: Optional[bt.Dendrite] = None

        # Fiber chain interface (for weight setting and node discovery)
        self.fiber_chain: Optional[FiberChain] = None
        self._fiber_nodes: Dict[str, FiberNode] = {}  # hotkey -> FiberNode cache

        # Validator state
        self.uid: Optional[int] = None
        self.hotkey: Optional[str] = None
        self.is_registered: bool = False

        # Connection tracking (for dendrite mode)
        self.connections: Dict[int, dict] = {}  # uid -> connection info
        self.connection_scores: Dict[int, float] = {}  # uid -> score
        self.connection_bandwidth: Dict[int, float] = {}  # uid -> bandwidth EMA

        # Orchestrator tracking (for HTTP mode)
        self.orchestrators: Dict[str, OrchestratorInfo] = {}
        self._beamcore_worker_counts: Dict[int, int] = {}  # uid -> worker_count from BeamCore

        # =====================================================================
        # Orchestrator and Worker Management
        # =====================================================================
        self.orchestrator_manager = OrchestratorManager()
        self.worker_registry = WorkerRegistry()
        self.reassignment_manager = ReassignmentManager(self.worker_registry)

        # Local score cache; BeamCore PRISM remains authoritative.
        self.orchestrator_scores: Dict[str, float] = {}  # hotkey -> final score
        self.payment_penalty_multipliers: Dict[str, float] = (
            {}
        )  # hotkey -> multiplier (1.0 = no penalty)

        # Work summaries (rolling 24h window)
        self.work_summaries: Dict[str, WorkSummary] = {}  # hotkey -> last summary
        self.work_summary_history: Dict[str, List[WorkSummary]] = {}  # hotkey -> history

        # Sybil Detection
        self.sybil_detector = get_sybil_detector()

        # Orchestrator performance tracking (rolling window)
        self.orchestrator_metrics: Dict[int, Dict] = {}  # uid -> metrics accumulator
        self.worker_metrics: Dict[str, Dict] = {}  # worker_id -> metrics accumulator

        # Sybil tracking per orchestrator
        self.sybil_penalties: Dict[int, float] = {}  # uid -> sybil penalty multiplier (0.0-1.0)

        # Redundancy and failover
        self.health_monitor: Optional[HealthMonitor] = None
        self.checkpoint_manager: Optional[CheckpointManager] = None
        self.recovery_manager: Optional[RecoveryManager] = None

        # Penalty tracking
        self.total_redirected_to_one: float = 0.0  # TAO redirected to #1 this epoch
        self.fraud_penalties: Dict[str, float] = {}  # hotkey -> fraud penalty multiplier

        # Task tracking
        self.pending_tasks: Dict[str, Task] = {}  # task_id -> Task
        self.task_results: Dict[str, PoBVerificationResult] = {}  # task_id -> result

        # Challenge tracking
        self.active_challenges: Dict[str, dict] = {}  # task_id -> challenge info
        self.challenge_results: Dict[str, ChallengeResult] = {}  # task_id -> result

        # Proof spot-checking
        self.spot_check_results: Dict[str, SpotCheckResult] = {}  # hotkey -> last result
        self.spot_check_history: List[SpotCheckResult] = []

        # PoB verification stats (populated by _verify_subnet_core_proofs)
        self.pob_verification_results: Dict[str, PoBVerificationStats] = {}  # hotkey -> stats

        # Weight history
        self.last_weight_block: int = 0
        self.weights_history: List[Dict] = []
        self._chain_weights_rate_limit: int = 0  # cached from subtensor at startup

        # Penalty history
        self.penalty_history: List[dict] = []

        # Epoch tracking for emissions
        self.current_epoch: int = 0
        self.epoch_start_block: int = 0
        self.tasks_this_epoch: int = 0
        self.last_emission_check_block: int = 0

        # HTTP session for local mode
        self._http_session: Optional[aiohttp.ClientSession] = None

        # SubnetCore API client for score submission (set by main.py)
        self.subnet_core_client: Optional[SubnetCoreClient] = None

        # Async control
        self._running: bool = False
        self._main_loop_task: Optional[asyncio.Task] = None

    async def initialize(self) -> None:
        """Initialize the Validator node"""
        logger.debug("Initializing Validator node...")

        if self.settings.local_mode:
            # Local development mode - skip Bittensor network connection
            logger.debug("Running in LOCAL MODE - skipping Bittensor network connection")
            await self._initialize_local_mode()
        else:
            # Mainnet mode with Bittensor
            await self._initialize_bittensor_mode()

        logger.debug("Validator node initialized")

    async def _initialize_local_mode(self) -> None:
        """Initialize validator in local development mode (no Bittensor connection)"""
        # Create a mock wallet for local testing
        self.wallet = bt.Wallet(
            name=self.settings.wallet_name,
            hotkey=self.settings.wallet_hotkey,
            path=self.settings.wallet_path,
        )
        self.hotkey = self.wallet.hotkey.ss58_address

        # Set mock values for local development
        self.uid = 1
        self.is_registered = True

        # Connect to subtensor for mainnet
        if self.settings.subtensor_address:
            self.subtensor = bt.Subtensor(network=self.settings.subtensor_address)
        else:
            self.subtensor = bt.Subtensor(network=self.settings.subtensor_network)
        logger.debug(f"Connected to subtensor: {self.subtensor.network}")

        # Load metagraph for mainnet data
        try:
            self.metagraph = bt.Metagraph(
                netuid=self.settings.netuid,
                network=self.subtensor.chain_endpoint,
            )
            self.metagraph.sync(subtensor=self.subtensor)
            logger.debug(f"Metagraph loaded with {len(self.metagraph.hotkeys)} neurons")
        except Exception as e:
            logger.warning(f"Failed to load metagraph: {e}")
            self.metagraph = None

        # Initialize Fiber chain interface
        try:
            self.fiber_chain = FiberChain(
                subtensor_network=self.settings.subtensor_network,
                subtensor_address=self.settings.subtensor_address,
                netuid=self.settings.netuid,
            )
            self._fiber_nodes = self.fiber_chain.get_nodes_by_hotkey()
            logger.debug(f"Fiber chain initialized with {len(self._fiber_nodes)} nodes")
        except Exception as e:
            logger.warning("Fiber chain init failed: %s", e, exc_info=True)
            self.fiber_chain = None

        # Create HTTP session for local mode communication
        self._http_session = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=60))

        # Add local orchestrator as a connection
        self.connections[1] = {
            "uid": 1,
            "hotkey": "local-orchestrator",
            "ip": "127.0.0.1",
            "port": 9000,
            "url": self.settings.orchestrator_url,
            "last_seen": datetime.utcnow(),
            "is_local": True,
        }

        # Discover orchestrators
        await self._discover_orchestrators()

        logger.debug(f"Local mode initialized with hotkey: {self.hotkey}")

    async def _initialize_bittensor_mode(self) -> None:
        """Initialize validator in Bittensor network mode"""
        # Load wallet
        self.wallet = bt.Wallet(
            name=self.settings.wallet_name,
            hotkey=self.settings.wallet_hotkey,
            path=self.settings.wallet_path,
        )
        self.hotkey = self.wallet.hotkey.ss58_address
        logger.debug(f"Wallet loaded: {self.hotkey}")

        # Connect to subtensor
        if self.settings.subtensor_address:
            self.subtensor = bt.Subtensor(network=self.settings.subtensor_address)
        else:
            self.subtensor = bt.Subtensor(network=self.settings.subtensor_network)
        logger.debug(f"Connected to subtensor: {self.subtensor.network}")

        # Load metagraph
        self.metagraph = bt.Metagraph(
            netuid=self.settings.netuid,
            network=self.subtensor.chain_endpoint,
        )
        self.metagraph.sync(subtensor=self.subtensor)

        # Initialize Fiber chain interface
        try:
            self.fiber_chain = FiberChain(
                subtensor_network=self.settings.subtensor_network,
                subtensor_address=self.settings.subtensor_address,
                netuid=self.settings.netuid,
            )
            self._fiber_nodes = self.fiber_chain.get_nodes_by_hotkey()
            logger.debug(f"Fiber chain initialized with {len(self._fiber_nodes)} nodes")
        except Exception as e:
            logger.warning("Fiber chain init failed: %s", e, exc_info=True)
            self.fiber_chain = None

        # Check registration
        await self._check_registration()

        # Seed last_weight_block from chain so a restart doesn't immediately retry set_weights
        if self.uid is not None and self.subtensor is not None:
            try:
                last_update_vec = self.subtensor.query_module(
                    "SubtensorModule", "LastUpdate", [self.settings.netuid]
                )
                vec = (last_update_vec.value or []) if last_update_vec else []
                if self.uid < len(vec):
                    self.last_weight_block = int(vec[self.uid])
                    _sw_rows = [
                        ("UID",        str(self.uid)),
                        ("Last Block", str(self.last_weight_block)),
                    ]
                    _sw_kw = max(len(k) for k, _ in _sw_rows)
                    _sw_vw = max(len(v) for _, v in _sw_rows)
                    _sw_in = _sw_kw + _sw_vw + 5
                    print("\n".join([
                        f"┌{'─' * _sw_in}┐",
                        f"│{'Chain Weight Block':^{_sw_in}}│",
                        f"├{'─' * (_sw_kw + 2)}┬{'─' * (_sw_vw + 2)}┤",
                        *[f"│ {k:<{_sw_kw}} │ {v:<{_sw_vw}} │" for k, v in _sw_rows],
                        f"└{'─' * (_sw_kw + 2)}┴{'─' * (_sw_vw + 2)}┘",
                    ]), flush=True)
            except Exception as _exc:
                logger.debug("Could not read LastUpdate from chain: %s", _exc)

        # Cache the chain's weights_rate_limit so we can compute wait times without hitting chain
        if self.subtensor is not None:
            try:
                self._chain_weights_rate_limit = self.subtensor.weights_rate_limit(self.settings.netuid) or 0
                logger.info("Chain weights_rate_limit for netuid %s: %s blocks", self.settings.netuid, self._chain_weights_rate_limit)
            except Exception as _exc:
                logger.debug("Could not read weights_rate_limit: %s", _exc)

        # Setup dendrite for querying connections
        self.dendrite = bt.Dendrite(wallet=self.wallet)

        # Create HTTP session
        self._http_session = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=60))

    async def _check_registration(self) -> None:
        """Check if this hotkey is registered on the subnet"""
        # Try Fiber first if available
        uid = self._get_uid_for_hotkey(self.hotkey)

        if uid is not None:
            self.uid = uid
            self.is_registered = True
            logger.debug(f"Registered on subnet {self.settings.netuid} with UID {self.uid}")
        else:
            self.is_registered = False
            logger.warning(f"Hotkey {self.hotkey} not registered on subnet")

    def _get_uid_for_hotkey(self, hotkey: str) -> Optional[int]:
        """Get UID for a hotkey using Fiber or metagraph fallback."""
        # Try Fiber first (if available and cached)
        if self._fiber_nodes:
            node = self._fiber_nodes.get(hotkey)
            if node:
                logger.debug(f"_get_uid_for_hotkey: {hotkey[:16]}... -> UID {node.uid} (via Fiber)")
                return node.uid

        # Fallback to metagraph
        if self.metagraph and hotkey in self.metagraph.hotkeys:
            uid = self.metagraph.hotkeys.index(hotkey)
            logger.debug(f"_get_uid_for_hotkey: {hotkey[:16]}... -> UID {uid} (via metagraph)")
            return uid

        logger.warning(
            f"_get_uid_for_hotkey: {hotkey[:16]}... -> NOT FOUND (fiber_nodes={len(self._fiber_nodes)}, metagraph={'yes' if self.metagraph else 'no'})"
        )
        return None

    def _get_node_info(self, hotkey: str) -> Optional[FiberNode]:
        """Get full node info for a hotkey using Fiber."""
        if self._fiber_nodes:
            node = self._fiber_nodes.get(hotkey)
            if node:
                logger.debug(f"_get_node_info: found node for {hotkey[:16]}... uid={node.uid}")
            else:
                logger.debug(f"_get_node_info: no Fiber node for {hotkey[:16]}...")
            return node
        logger.debug(f"_get_node_info: no Fiber nodes available, cannot look up {hotkey[:16]}...")
        return None

    async def start(self) -> None:
        """Start the Validator node"""
        if not self.is_registered:
            logger.error("Cannot start: not registered on subnet")
            return

        # Initialize redundancy system
        await self._initialize_redundancy()

        # Load pending challenges from database (state recovery)
        await self._load_pending_challenges()

        self._running = True
        self._main_loop_task = asyncio.create_task(self._main_loop())
        self._heartbeat_task = asyncio.create_task(self._heartbeat_loop())

        logger.info("Validator node started")

    async def _initialize_redundancy(self) -> None:
        """Initialize redundancy and failover systems"""
        try:
            self.health_monitor, self.checkpoint_manager, self.recovery_manager = (
                create_redundancy_system(self)
            )

            recovered = await initialize_with_recovery(self)
            if recovered:
                logger.info("Validator state restored from checkpoint")

            await self.health_monitor.start()
            await self.checkpoint_manager.start()

            logger.info("Redundancy system initialized")

        except Exception as e:
            logger.warning(f"Failed to initialize redundancy system: {e}")

    async def stop(self) -> None:
        """Stop the Validator node"""
        self._running = False

        # Cancel heartbeat task
        if hasattr(self, "_heartbeat_task") and self._heartbeat_task:
            self._heartbeat_task.cancel()
            try:
                await self._heartbeat_task
            except asyncio.CancelledError:
                pass

        if self._main_loop_task:
            self._main_loop_task.cancel()
            try:
                await self._main_loop_task
            except asyncio.CancelledError:
                pass

        # Stop redundancy systems
        if self.health_monitor:
            await self.health_monitor.stop()
        if self.checkpoint_manager:
            await self.checkpoint_manager.stop()

        # Close HTTP session if open
        if self._http_session and not self._http_session.closed:
            await self._http_session.close()
            self._http_session = None

        logger.info("Validator node stopped")

    async def _submit_beamcore_heartbeat(self) -> None:
        """POST /validators/heartbeat to BeamCore."""
        if not SUBNET_CORE_AVAILABLE or not self.subnet_core_client:
            return

        health_info = None
        if self.health_monitor:
            try:
                report = await self.health_monitor.run_health_checks()
                health_info = {
                    "status": report.get("status"),
                    "checks_passed": report.get("checks_passed", 0),
                    "checks_failed": report.get("checks_failed", 0),
                }
            except Exception:
                pass

        status = "online"
        if health_info and health_info.get("checks_failed", 0) > 0:
            status = "degraded"

        result = await self.subnet_core_client.submit_heartbeat(
            validator_uid=self.uid,
            status=status,
            last_epoch_scored=self.current_epoch or None,
            health_info=health_info,
            external_url=self.settings.external_url,
        )
        api_key = result.get("api_key")
        if api_key:
            self.subnet_core_client._api_key = api_key
        _hb_items = [
            ("Status", status),
            ("UID",    str(self.uid)),
            ("Epoch",  str(self.current_epoch) if self.current_epoch else "—"),
        ]
        if api_key:
            _hb_items.append(("API Key", "received"))
        _hk, _hv = 7, max(len(v) for _, v in _hb_items)
        _hv = max(_hv, 8)
        _top   = f"┌{'─' * (_hk + _hv + 5)}┐"
        _title = f"│{'Heartbeat':^{_hk + _hv + 5}}│"
        _sep   = f"├{'─' * (_hk + 2)}┬{'─' * (_hv + 2)}┤"
        _body  = "\n".join(f"│ {k:<{_hk}} │ {v:<{_hv}} │" for k, v in _hb_items)
        _bot   = f"└{'─' * (_hk + 2)}┴{'─' * (_hv + 2)}┘"
        print("\n".join([_top, _title, _sep, _body, _bot]), flush=True)

    async def _heartbeat_loop(self) -> None:
        """Send periodic heartbeats to SubnetCore while running."""
        heartbeat_interval = 60  # seconds

        while self._running:
            try:
                await asyncio.sleep(heartbeat_interval)

                if not self._running:
                    break

                await self._submit_beamcore_heartbeat()

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.warning(f"Heartbeat error: {e}")
                # Don't break - continue trying

    async def _main_loop(self) -> None:
        """Main validator loop"""
        while self._running:
            try:
                # Sync metagraph
                await self._sync_metagraph()

                # Discover orchestrators from BeamCore first, then filter connections
                await self._discover_orchestrators()
                await self._update_connections()

                # Sync orchestrators from metagraph
                await self._sync_orchestrators()

                # Query orchestrators for work summaries
                await self._query_orchestrators()

                # Generate and send bandwidth challenges
                # DISABLED: Challenges temporarily disabled - endpoint deprecated
                # await self._generate_and_send_challenges()

                # Issue additional challenges
                # DISABLED: Challenges temporarily disabled - endpoint deprecated
                # await self._issue_challenges()

                # Collect and verify PoB
                await self._collect_pob_results()

                # Spot-check proofs
                await self._spot_check_proofs()

                # Update scores
                await self._update_scores()

                # Check for epoch change and broadcast (must run before weight-setting
                # so self.current_epoch is correct on the first main-loop iteration)
                await self._check_epoch()

                # Set weights if needed
                await self._maybe_set_weights()

                # Periodic cleanup
                await self._expire_penalties_and_challenges()

            except Exception as e:
                logger.error(f"Error in main loop: {e}", exc_info=True)

                if self.recovery_manager and self.recovery_manager.should_attempt_recovery():
                    logger.warning("Health issues detected, attempting recovery...")
                    success = await self.recovery_manager.attempt_recovery()
                    if success:
                        logger.info("Recovery successful")
                    else:
                        logger.error("Recovery failed")

            await asyncio.sleep(self.settings.sync_interval)

    # =========================================================================
    # Spot-Check Proofs
    # =========================================================================

    async def _spot_check_proofs(self) -> None:
        """Randomly spot-check proofs from Orchestrators."""
        eligible = [
            (hk, s)
            for hk, s in self.work_summaries.items()
            if s.proof_count > 0
            and self.orchestrators.get(hk)
            and self.orchestrators[hk].is_healthy
        ]
        if eligible:
            logger.info(f"_spot_check_proofs: checking {len(eligible)} eligible orchestrators")

        for hotkey, summary in self.work_summaries.items():
            if summary.proof_count == 0:
                continue

            orchestrator = self.orchestrators.get(hotkey)
            if not orchestrator or not orchestrator.is_healthy:
                continue

            try:
                result = await self._spot_check_orchestrator(
                    hotkey, orchestrator, summary,
                )
                self.spot_check_results[hotkey] = result
                self.spot_check_history.append(result)

                if len(self.spot_check_history) > 1000:
                    self.spot_check_history = self.spot_check_history[-1000:]

                if result.fraud_detected:
                    self.fraud_penalties[hotkey] = result.fraud_severity
                    logger.warning(
                        f"Fraud detected for orchestrator {hotkey[:16]}...: "
                        f"{result.proofs_invalid}/{result.proofs_received} invalid proofs"
                    )
                else:
                    self.fraud_penalties.pop(hotkey, None)

            except Exception as e:
                logger.error(f"Error spot-checking orchestrator {hotkey[:16]}...: {e}")

    async def _spot_check_orchestrator(
        self,
        hotkey: str,
        orchestrator: OrchestratorInfo,
        summary: WorkSummary,
        prefetched_ids: List[str] = None,
    ) -> SpotCheckResult:
        """Perform spot-check verification for a single orchestrator."""
        sample_percent = 0.05 + random.random() * 0.05
        sample_size = max(5, min(50, int(summary.proof_count * sample_percent)))
        logger.info(
            f"_spot_check_orchestrator: {hotkey[:16]}... "
            f"sampling {sample_size} of {summary.proof_count} proofs ({sample_percent:.1%})"
        )

        proof_ids = await self._get_random_proof_ids(orchestrator, sample_size)

        if not proof_ids:
            logger.warning(f"_spot_check_orchestrator: {hotkey[:16]}... no proof IDs returned")
            return SpotCheckResult(
                orchestrator_hotkey=hotkey,
                proofs_requested=sample_size,
                proofs_received=0,
                proofs_valid=0,
                proofs_invalid=0,
            )

        logger.debug(
            f"_spot_check_orchestrator: {hotkey[:16]}... got {len(proof_ids)} proof IDs, requesting full proofs"
        )
        proofs = await self._request_proofs(orchestrator, proof_ids)

        if not proofs:
            logger.warning(
                f"_spot_check_orchestrator: {hotkey[:16]}... returned 0 proofs for {len(proof_ids)} IDs - FRAUD"
            )
            return SpotCheckResult(
                orchestrator_hotkey=hotkey,
                proofs_requested=sample_size,
                proofs_received=0,
                proofs_valid=0,
                proofs_invalid=0,
                fraud_detected=True,
                fraud_severity=1.0,
            )

        valid_count = 0
        invalid_ids = []
        invalid_reasons = {}

        for proof_data in proofs:
            verification = await self._verify_single_subnet_proof(proof_data)

            if verification.valid:
                valid_count += 1
            else:
                invalid_ids.append(verification.task_id)
                invalid_reasons[verification.task_id] = verification.error or "Unknown error"

        verification_rate = valid_count / len(proofs) if proofs else 0
        invalid_rate = 1.0 - verification_rate

        fraud_detected = invalid_rate > 0.05
        fraud_severity = min(1.0, invalid_rate * 2)

        log_fn = logger.warning if fraud_detected else logger.info
        log_fn(
            f"_spot_check_orchestrator: {hotkey[:16]}... "
            f"valid={valid_count}/{len(proofs)} invalid={len(invalid_ids)} "
            f"rate={verification_rate:.2%} fraud={'YES' if fraud_detected else 'no'}"
            + (f" severity={fraud_severity:.4f}" if fraud_detected else "")
            + (
                f" invalid_reasons={dict(list(invalid_reasons.items())[:3])}"
                if invalid_reasons
                else ""
            )
        )

        return SpotCheckResult(
            orchestrator_hotkey=hotkey,
            proofs_requested=sample_size,
            proofs_received=len(proofs),
            proofs_valid=valid_count,
            proofs_invalid=len(invalid_ids),
            verification_rate=verification_rate,
            invalid_proof_ids=invalid_ids,
            invalid_reasons=invalid_reasons,
            fraud_detected=fraud_detected,
            fraud_severity=fraud_severity,
        )

    async def _get_random_proof_ids(
        self,
        orchestrator: OrchestratorInfo,
        sample_size: int,
        prefetched_ids: List[str] = None,
    ) -> List[str]:
        """Return proof IDs for local spot-checking when available."""
        return []

    async def _request_proofs(
        self,
        orchestrator: OrchestratorInfo,
        proof_ids: List[str],
    ) -> List[dict]:
        """Return full local proof data for specific proof IDs when available."""
        return []

    # =========================================================================
    # Scoring
    # =========================================================================

    async def _update_scores(self) -> None:
        """Update local orchestrator scores from work, challenge, fraud, and payment signals."""
        logger.info(
            f"_update_scores: scoring {len(self.orchestrators)} orchestrators, {len(self.work_summaries)} with summaries"
        )

        _baseline_multiplier = 0.1

        for hotkey, info in self.orchestrators.items():
            summary = self.work_summaries.get(hotkey)

            if not summary:
                final_score = 1.0 if info.is_subnet_owned else _baseline_multiplier
                self.orchestrator_scores[hotkey] = final_score
                info.last_score = final_score
                logger.info(
                    f"_update_scores: {hotkey[:16]}... UID={info.uid} "
                    f"no work summary - baseline score={final_score:.4f}"
                )
                continue

            throughput_score = min(max(summary.avg_bandwidth_mbps / 1000.0, 0.0), 1.0)
            reliability_score = min(max(summary.success_rate, 0.0), 1.0)
            work_score = (throughput_score * 0.6) + (reliability_score * 0.4)
            challenge_multiplier = self._calculate_challenge_multiplier(hotkey)
            fraud_multiplier = self._calculate_fraud_multiplier(hotkey)
            payment_multiplier = self.payment_penalty_multipliers.get(hotkey, 1.0)
            final_score = work_score * challenge_multiplier * fraud_multiplier * payment_multiplier

            self.orchestrator_scores[hotkey] = final_score
            info.last_score = final_score

            logger.info(
                f"_update_scores: {hotkey[:16]}... UID={info.uid} "
                f"work_score={work_score:.4f} "
                f"challenge_mult={challenge_multiplier:.4f} "
                f"fraud_mult={fraud_multiplier:.4f} "
                f"payment_mult={payment_multiplier:.4f} "
                f"final_score={final_score:.6f}"
            )

        # Also update connection scores for legacy compatibility
        for uid in self.connections:
            conn_results = [
                r
                for r in self.task_results.values()
                if r.valid and self._get_uid_for_miner(r.pob.miner_id) == uid
            ]

            if not conn_results:
                continue

            bandwidth_scores = [r.calculated_bandwidth for r in conn_results]
            avg_bandwidth = sum(bandwidth_scores) / len(bandwidth_scores)

            bandwidth_normalized = min(avg_bandwidth / self.settings.max_bandwidth_mbps, 1.0)

            total_tasks = len(
                [t for t in self.pending_tasks.values() if self._get_uid_for_task(t) == uid]
            )
            success_rate = len(conn_results) / total_tasks if total_tasks > 0 else 0

            score = (
                self.settings.score_weight_bandwidth * bandwidth_normalized
                + self.settings.score_weight_uptime * success_rate
                + self.settings.score_weight_loss * 1.0
                + self.settings.score_weight_tier * 0.5
            )

            if uid in self.connection_scores:
                old_score = self.connection_scores[uid]
                score = BANDWIDTH_EMA_ALPHA * score + (1 - BANDWIDTH_EMA_ALPHA) * old_score

            self.connection_scores[uid] = score
            self.connection_bandwidth[uid] = avg_bandwidth

    def _calculate_challenge_multiplier(self, hotkey: str) -> float:
        """Calculate challenge verification multiplier."""
        challenge_results = [
            r
            for cid, r in self.challenge_results.items()
            if self.active_challenges.get(cid, {}).get("orchestrator") == hotkey
        ]

        if not challenge_results:
            logger.debug(
                f"_calculate_challenge_multiplier: {hotkey[:16]}... no challenge results, using default 0.9"
            )
            return 0.9

        successful = sum(1 for r in challenge_results if r.success)
        success_rate = successful / len(challenge_results)
        multiplier = 0.5 + (success_rate * 0.5)

        logger.info(
            f"_calculate_challenge_multiplier: {hotkey[:16]}... "
            f"{successful}/{len(challenge_results)} challenges passed "
            f"(rate={success_rate:.2%}) -> multiplier={multiplier:.4f}"
        )

        return multiplier

    def _calculate_fraud_multiplier(self, hotkey: str) -> float:
        """Calculate fraud penalty multiplier from spot-check results."""
        fraud_severity = self.fraud_penalties.get(hotkey, 0.0)

        if fraud_severity <= 0:
            logger.debug(f"_calculate_fraud_multiplier: {hotkey[:16]}... no fraud penalty -> 1.0")
            return 1.0

        multiplier = 1.0 - fraud_severity
        multiplier = max(0.1, multiplier)
        logger.warning(
            f"_calculate_fraud_multiplier: {hotkey[:16]}... "
            f"fraud_severity={fraud_severity:.4f} -> multiplier={multiplier:.4f}"
        )
        return multiplier

    def _get_uid_for_miner(self, miner_id) -> Optional[int]:
        """Get UID for a miner hotkey"""
        hotkey = miner_id.decode() if isinstance(miner_id, bytes) else str(miner_id)
        for uid, conn in self.connections.items():
            if conn["hotkey"] == hotkey:
                logger.debug(f"_get_uid_for_miner: {hotkey[:16]}... -> UID {uid}")
                return uid
        logger.debug(
            f"_get_uid_for_miner: {hotkey[:16]}... -> NOT FOUND in {len(self.connections)} connections"
        )
        return None

    def _get_uid_for_task(self, task: Task) -> Optional[int]:
        """Get UID for a task's assigned connection"""
        if task.path:
            for uid, conn in self.connections.items():
                if conn["hotkey"] == task.path[0]:
                    logger.debug(f"_get_uid_for_task: path[0]={task.path[0][:16]}... -> UID {uid}")
                    return uid
            logger.debug(f"_get_uid_for_task: path[0]={task.path[0][:16]}... -> NOT FOUND")
        return None

    # =========================================================================
    # Weight Setting
    # =========================================================================

    async def _maybe_set_weights(self) -> None:
        """Set weights on chain if enough blocks have passed"""
        if self.subtensor is None:
            logger.debug("Skipping weight setting - no subtensor")
            return

        current_block = self.subtensor.block
        blocks_since = current_block - self.last_weight_block

        def _fmt_wait(blocks: int) -> str:
            secs = blocks * 12
            return f"~{secs // 60}m {secs % 60}s" if secs >= 60 else f"~{secs}s"

        # Chain uses strict `blocks_since > rate_limit`, so we must wait for blocks_since >= rate_limit + 1
        effective_limit = max(self.settings.blocks_between_weights, self._chain_weights_rate_limit)
        if blocks_since <= effective_limit:
            blocks_remaining = effective_limit - blocks_since + 1
            logger.info("Next weight set window in %s (%d blocks)", _fmt_wait(blocks_remaining), blocks_remaining)
            return

        await self._set_weights()

    async def _set_weights(self) -> None:
        """Set weights using BeamCore's persisted epoch snapshot."""
        if not SUBNET_CORE_AVAILABLE or not self.subnet_core_client:
            logger.warning("BeamCore client not available, skipping weight setting")
            return

        weight_snapshot = await self._get_persisted_weight_snapshot()
        if not weight_snapshot:
            logger.warning("No persisted BeamCore weight snapshot available")
            return

        uids, weights, formula_version, params_hash, data_epoch, _no_weight_period, _burn_reason = weight_snapshot

        # Set weights on chain
        try:
            # Check if commit-reveal is enabled on this subnet
            commit_reveal_enabled = False
            if self.subtensor is not None:
                try:
                    cr_result = self.subtensor.query_module(
                        "SubtensorModule", "CommitRevealWeightsEnabled", [self.settings.netuid]
                    )
                    commit_reveal_enabled = bool(cr_result.value) if cr_result else False
                except Exception as e:
                    logger.debug(f"Could not check commit-reveal status: {e}")

            if commit_reveal_enabled and self.subtensor is not None:
                result = self.subtensor.set_weights(
                    wallet=self.wallet,
                    netuid=self.settings.netuid,
                    uids=list(uids),
                    weights=list(weights),
                    wait_for_inclusion=True,
                    wait_for_finalization=False,
                    wait_for_revealed_execution=True,
                )
                success = result.success
                message = result.message or result.error or ""
                weight_method = "timelocked_commit_reveal"
            elif self.fiber_chain is not None and self.uid is not None:
                success, message = self.fiber_chain.set_weights(
                    keypair=self.wallet,  # Pass full wallet, not just hotkey
                    validator_uid=self.uid,
                    uids=uids,
                    weights=weights,
                    wait_for_inclusion=True,
                    wait_for_finalization=False,
                )
                weight_method = "Fiber"
            elif self.subtensor is not None:
                success, message = self.subtensor.set_weights(
                    wallet=self.wallet,
                    netuid=self.settings.netuid,
                    uids=uids,
                    weights=weights,
                    wait_for_inclusion=True,
                    wait_for_finalization=False,
                )
                weight_method = "bittensor"
            else:
                logger.warning("No method available to set weights")
                return

            if success:
                self.last_weight_block = self.subtensor.block if self.subtensor else 0
                _kw, _uw, _ww = 9, 4, 10
                _info_items = [
                    ("Block",   str(self.last_weight_block)),
                    ("Epoch",   str(data_epoch)),
                    ("Method",  weight_method),
                    ("Formula", formula_version),
                ]
                if _no_weight_period:
                    _info_items.append(("Period", f"BURN — {_burn_reason}"))
                _vw = max(len(v) for _, v in _info_items)
                _hw = _kw + _vw - _uw - _ww - 3  # derived: header_outer == uid_outer
                _hw = max(_hw, 20)                # ensure hotkey col is readable
                _vw = _uw + _hw + _ww + 3 - _kw  # recompute in case _hw was clamped
                _hotkeys = getattr(self.metagraph, "hotkeys", []) if self.metagraph else []
                _sorted_w = sorted(zip(uids, weights), key=lambda x: -x[1])
                _uid_rows = []
                for _uid, _w in _sorted_w:
                    if _uid == 0 and _w > 0:
                        _hk = "(burn)"
                    elif 0 <= _uid < len(_hotkeys):
                        _hk = (_hotkeys[_uid][:_hw - 3] + "...") if len(_hotkeys[_uid]) > _hw else _hotkeys[_uid]
                    else:
                        _hk = "unknown"
                    _uid_rows.append(f"│ {_uid:>{_uw}} │ {_hk:<{_hw}} │ {_w:>{_ww}.4f} │")
                _inner = _kw + _vw + 5
                _top    = f"┌{'─' * _inner}┐"
                _title  = f"│{'Weight Set Successfully':^{_inner}}│"
                _hsep1  = f"├{'─' * (_kw + 2)}┬{'─' * (_vw + 2)}┤"
                _info   = "\n".join(f"│ {k:<{_kw}} │ {v:<{_vw}} │" for k, v in _info_items)
                _hsep2  = f"├{'─' * (_uw + 2)}┬{'─' * (_hw + 2)}┬{'─' * (_ww + 2)}┤"
                _uidhdr = f"│ {'UID':>{_uw}} │ {'Hotkey':<{_hw}} │ {'Weight':>{_ww}} │"
                _hsep3  = f"├{'─' * (_uw + 2)}┼{'─' * (_hw + 2)}┼{'─' * (_ww + 2)}┤"
                _bot    = f"└{'─' * (_uw + 2)}┴{'─' * (_hw + 2)}┴{'─' * (_ww + 2)}┘"
                print("\n".join([_top, _title, _hsep1, _info, _hsep2, _uidhdr, _hsep3] + _uid_rows + [_bot]), flush=True)

                self.weights_history.append(
                    {
                        "block": self.last_weight_block,
                        "timestamp": datetime.utcnow().isoformat(),
                        "weights": {uid: round(w, 6) for uid, w in zip(uids, weights)},
                        "weight_method": weight_method,
                        "formula_version": formula_version,
                    }
                )

                if len(self.weights_history) > 100:
                    self.weights_history = self.weights_history[-100:]

                if self.subnet_core_client:
                    try:
                        await self.subnet_core_client.submit_weight_proof(
                            epoch=data_epoch,
                            block_number=self.last_weight_block,
                            netuid=self.settings.netuid,
                            uids=list(uids),
                            weights=list(weights),
                            formula_version=formula_version,
                            params_hash=params_hash,
                        )
                        logger.debug("Weight proof submitted to BeamCore")
                    except Exception as _e:
                        _body = getattr(getattr(_e, "response", None), "text", None)
                        logger.warning("Failed to submit weight proof: %s%s", _e, f" — {_body}" if _body else "")

            else:
                _cur_block = self.subtensor.block if self.subtensor else "?"
                _msg = message or "(empty — likely chain rate limit or rejection)"
                logger.error(
                    "Failed to set weights: method=%s block=%s last_weight_block=%s message=%r",
                    weight_method, _cur_block, self.last_weight_block, _msg,
                )
                # Back off: treat this attempt as the new baseline so we don't
                # retry every block after a chain rejection.
                self.last_weight_block = self.subtensor.block if self.subtensor else self.last_weight_block

        except Exception as e:
            logger.error(f"Error setting weights: {e}", exc_info=True)

    async def _get_persisted_weight_snapshot(
        self,
    ) -> Optional[Tuple[List[int], List[float], str, Optional[str], int, bool, str]]:
        """Fetch recommended weights from BeamCore epoch summary (ops-materialized)."""
        if not self.subnet_core_client:
            return None
        try:
            snapshot = await self.subnet_core_client.get_latest_epoch_summary()
        except Exception as exc:
            logger.warning("BeamCore epoch summary unavailable: %s", exc)
            return None

        uids = snapshot.get("uids") or []
        weights = snapshot.get("weights") or []
        if not uids or len(uids) != len(weights):
            logger.warning("BeamCore epoch summary missing uids/weights vectors")
            return None
        fv = str(snapshot.get("formula_version") or "prism_final_x_task_done_count")
        ph = snapshot.get("params_hash")
        if isinstance(ph, str):
            params_hash: Optional[str] = ph
        else:
            params_hash = None
        data_epoch = int(snapshot.get("epoch", self.current_epoch))

        _no_weight_period = bool(snapshot.get("no_weight_period"))
        _burn_reason = snapshot.get("reason", "") if _no_weight_period else ""
        return (list(uids), list(weights), fv, params_hash, data_epoch, _no_weight_period, _burn_reason)

    # =========================================================================
    # Epoch Management
    # =========================================================================

    async def _check_epoch(self) -> None:
        """Check for epoch changes and broadcast emission info"""
        if self.subtensor is None:
            return

        current_block = self.subtensor.block
        # Use 360 blocks per epoch to match SubnetCore's epoch calculation
        epoch_length_blocks = 360
        current_epoch = current_block // epoch_length_blocks

        # Sync if epoch changed or if current epoch is obviously wrong (old calculation)
        # Old calculation used block//25 which gave epochs like 258007 instead of ~17925
        should_sync = (
            current_epoch > self.current_epoch  # Normal case: new epoch
            or self.current_epoch > 100_000  # Old epoch calculation was used
        )

        if should_sync and current_epoch != self.current_epoch:
            previous_epoch = self.current_epoch
            self.current_epoch = current_epoch
            self.epoch_start_block = current_epoch * epoch_length_blocks

            logger.info("══════════════ EPOCH %s ══════════════ (prev=%s)", self.current_epoch, previous_epoch)
            self.tasks_this_epoch = 0

            # Reset PoB verification stats for the new epoch
            self.pob_verification_results.clear()

    # =========================================================================
    # Cleanup and Maintenance
    # =========================================================================

    async def _load_pending_challenges(self) -> None:
        """Load pending challenges on startup."""
        logger.debug("Challenges are tracked in memory only")

    async def _expire_penalties_and_challenges(self) -> None:
        """Periodic cleanup: timeout stale in-memory challenges."""
        current_time = time.time()
        stale_ids = []

        for challenge_id, info in self.active_challenges.items():
            created_at = info.get("created_at", 0)
            if current_time - created_at > 300:
                stale_ids.append(challenge_id)

        for challenge_id in stale_ids:
            del self.active_challenges[challenge_id]

        if stale_ids:
            logger.debug(f"Cleaned up {len(stale_ids)} stale challenges")

    # =========================================================================
    # State & Metrics
    # =========================================================================

    def get_validator_state(self) -> dict:
        """Get current Validator state"""
        orchestrator_stats = self.orchestrator_manager.get_network_stats()
        worker_stats = self.worker_registry.get_stats()
        reassignment_stats = self.reassignment_manager.get_stats()

        # Use BeamCore worker counts instead of internal tracking
        beamcore_total_workers = sum(self._beamcore_worker_counts.values())
        orchestrator_stats["total_workers"] = beamcore_total_workers
        orchestrator_stats["worker_counts_by_uid"] = dict(self._beamcore_worker_counts)

        return {
            "uid": self.uid,
            "hotkey": self.hotkey,
            "is_registered": self.is_registered,
            "connections_tracked": len(self.connections),
            "pending_tasks": len(self.pending_tasks),
            "verified_pob": len([r for r in self.task_results.values() if r.valid]),
            "last_weight_block": self.last_weight_block,
            "current_block": self.subtensor.block if self.subtensor else 0,
            "orchestrators": orchestrator_stats,
            "workers": worker_stats,
            "reassignments": reassignment_stats,
            "total_redirected_to_one_tao": self.total_redirected_to_one,
        }

    def get_connection_scores(self) -> Dict[int, dict]:
        """Get all connection scores (legacy method)"""
        return {
            uid: {
                "score": round(self.connection_scores.get(uid, 0), 4),
                "bandwidth_mbps": round(self.connection_bandwidth.get(uid, 0), 2),
                "hotkey": conn["hotkey"],
            }
            for uid, conn in self.connections.items()
        }

    def get_orchestrator_scores(self) -> Dict[str, dict]:
        """Get all local orchestrator scores."""
        result = {}

        for hotkey, info in self.orchestrators.items():
            uid = self._get_uid_for_hotkey(hotkey)
            result[hotkey] = {
                "uid": uid,
                "score": round(self.orchestrator_scores.get(hotkey, 0), 4),
                "url": info.url,
                "is_healthy": info.is_healthy,
                "is_subnet_owned": info.is_subnet_owned,
                "last_seen": info.last_seen.isoformat(),
                "registered": uid is not None,
            }

        return result

    def get_spot_check_results(self) -> Dict[str, dict]:
        """Get spot-check results for all orchestrators."""
        results = {}
        for hotkey, result in self.spot_check_results.items():
            results[hotkey[:16]] = {
                "proofs_requested": result.proofs_requested,
                "proofs_received": result.proofs_received,
                "proofs_valid": result.proofs_valid,
                "proofs_invalid": result.proofs_invalid,
                "verification_rate": round(result.verification_rate, 4),
                "fraud_detected": result.fraud_detected,
                "fraud_severity": round(result.fraud_severity, 4),
                "fraud_multiplier": round(self._calculate_fraud_multiplier(hotkey), 4),
                "timestamp": result.timestamp.isoformat(),
            }
        return results

    def get_weights_history(self, limit: int = 10) -> List[dict]:
        """Get recent weights history."""
        return list(reversed(self.weights_history[-limit:]))
