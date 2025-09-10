"""

This provides REST API endpoints and WebSocket connections for:

- Competition management
- Validator connections
- Job distribution
- Result collection
"""

import asyncio
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

from fastapi import (
    WebSocket,
)
from fiber.chain.interface import get_substrate
from fiber.chain.metagraph import Metagraph
from snowflake import SnowflakeGenerator
from sqlalchemy import and_, select
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from backend.constants import (
    CHAIN_SCAN_YIELD_INTERVAL,
    DEFAULT_CHAIN_SYNC_INTERVAL,
    DEFAULT_MAX_COMMITMENT_LOOKBACK,
    HEARTBEAT_INTERVAL,
    MAX_WORKERS,
)
from core.chain import query_commitments_from_substrate
from core.db.models import EvaluationStatus
from core.log import get_logger
from core.messages import (
    EvalJobMessage,
)
from core.schemas import ChainCommitmentResponse

from .config import BackendConfig
from .models import (
    BackendEvaluationJob,
    BackendEvaluationJobStatus,
    BackendState,
    Competition,
    MinerSubmission,
    SS58Address,
    ValidatorConnection,
)

logger = get_logger(__name__)

# TODO: implement weight broadcasting and weight setting to connecting validators
ConnectionId = str  # Unique ID for each WebSocket connection


class BackendService:
    """Core backend service logic."""

    def __init__(self, config: BackendConfig):
        self.config = config
        self.db_url = config.settings.get("database_url")

        # Chain monitoring configuration
        self.max_commitment_lookback = config.settings.get(
            "max_commitment_lookback", DEFAULT_MAX_COMMITMENT_LOOKBACK
        )
        self.chain_sync_interval = config.settings.get(
            "chain_sync_interval", DEFAULT_CHAIN_SYNC_INTERVAL
        )

        # Chain connection objects
        # Using Any since fiber's SubstrateInterface is from async_substrate_interface
        self.substrate: Optional[Any] = (
            None  # async_substrate_interface.sync_substrate.SubstrateInterface
        )
        self.metagraph: Optional[Metagraph] = None

        # WebSocket connections
        self.active_connections: Dict[ConnectionId, WebSocket] = {}
        self.validator_connections: Dict[ConnectionId, SS58Address] = {}

        # Background tasks
        self._running = False
        self._chain_monitor_task = None
        self._heartbeat_monitor_task = None

        # ID generator
        self.id_generator = SnowflakeGenerator(42)

        # Database
        self.engine: AsyncEngine = None
        self.async_session: async_sessionmaker[AsyncSession] = None

        # Thread pool for blocking operations
        self.thread_pool = ThreadPoolExecutor(max_workers=MAX_WORKERS)

    async def startup(self) -> None:
        """Initialize the backend service."""
        logger.info("Starting Kinitro Backend Service")

        # Initialize database first
        await self._init_database()

        # Initialize chain connection
        await self._init_chain()

        # Load backend state
        await self._load_backend_state()

        # Start background tasks
        self._running = True
        self._chain_monitor_task = asyncio.create_task(self._monitor_chain())
        self._heartbeat_monitor_task = asyncio.create_task(
            self._monitor_validator_heartbeats()
        )

        logger.info("Kinitro Backend Service started successfully")

    async def shutdown(self) -> None:
        """Shutdown the backend service."""
        logger.info("Shutting down Kinitro Backend Service")

        self._running = False

        # Cancel background tasks
        if self._chain_monitor_task:
            self._chain_monitor_task.cancel()
            try:
                await self._chain_monitor_task
            except asyncio.CancelledError as e:
                logger.error(f"Chain monitor task cancelled: {e}")

        if self._heartbeat_monitor_task:
            self._heartbeat_monitor_task.cancel()
            try:
                await self._heartbeat_monitor_task
            except asyncio.CancelledError as e:
                logger.error(f"Heartbeat monitor task cancelled: {e}")

        # Close WebSocket connections
        for ws in self.active_connections.values():
            await ws.close()

        # Close database
        if self.engine:
            await self.engine.dispose()

        # Shutdown thread pool
        self.thread_pool.shutdown(wait=True)

        logger.info("Backend Service shut down")

    async def _init_chain(self) -> None:
        """Initialize blockchain connection."""
        try:
            logger.info("Initializing blockchain connection...")

            self.substrate = get_substrate(
                subtensor_network=self.config.settings["subtensor"]["network"],
                subtensor_address=self.config.settings["subtensor"]["address"],
            )

            self.metagraph = Metagraph(
                netuid=self.config.settings["subtensor"]["netuid"],
                substrate=self.substrate,
            )

            logger.info("Blockchain connection initialized")
        except Exception as e:
            logger.error(f"Failed to initialize blockchain connection: {e}")
            raise e

    async def _init_database(self) -> None:
        """Initialize database connection."""
        self.engine = create_async_engine(
            self.db_url, echo=False, pool_pre_ping=True, pool_size=20, max_overflow=0
        )

        self.async_session = async_sessionmaker(
            self.engine, class_=AsyncSession, expire_on_commit=False
        )

        logger.info("Database connection initialized")

    async def _load_backend_state(self) -> None:
        """Load or initialize backend service state.

        This loads the singleton BackendState record which tracks:
        - Chain monitoring state (last seen block number, last chain scan time)
        - Service metadata (version, start time)
        - Persistence across service restarts
        """
        if not self.async_session:
            logger.error("Database not initialized")
            return
        async with self.async_session() as session:
            result = await session.execute(
                select(BackendState).where(BackendState.id == 1)
            )
            state = result.scalar_one_or_none()

            if not state:
                state = BackendState(id=1, last_seen_block=0, service_version="1.0.0")
                session.add(state)
                await session.commit()
                logger.info("Initialized new backend state")
            else:
                state.service_start_time = datetime.now(timezone.utc)
                await session.commit()
                logger.info(
                    f"Loaded backend state: last_seen_block={state.last_seen_block}"
                )

    async def _monitor_chain(self) -> None:
        """Background task to monitor blockchain for commitments."""
        while self._running:
            try:
                if self.substrate and self.metagraph and self.async_session:
                    await self._sync_metagraph()

                    async with self.async_session() as session:
                        # Get backend state
                        state_result = await session.execute(
                            select(BackendState).where(BackendState.id == 1)
                        )
                        state = state_result.scalar_one()

                        # Get latest block
                        latest_block = await self._get_latest_block()
                        start_block = max(
                            state.last_seen_block + 1,
                            latest_block - self.max_commitment_lookback + 1,
                        )

                        logger.info(f"Checking blocks {start_block} to {latest_block}")

                        # Get active competitions
                        comp_result = await session.execute(
                            select(Competition).where(Competition.active)
                        )

                        active_competitions = {c.id: c for c in comp_result.scalars()}
                        # preview active competitions
                        logger.debug(
                            f"Preview of active competitions: {list(active_competitions.keys())[:5]}"
                        )

                        # Query commitments (with yield points to prevent blocking)
                        for i, block_num in enumerate(
                            range(start_block, latest_block + 1)
                        ):
                            commitments = await self._query_block_commitments(block_num)
                            for commitment in commitments:
                                await self._process_commitment(
                                    commitment, block_num, active_competitions
                                )

                            # Yield control periodically to prevent blocking WebSocket connections
                            if i % CHAIN_SCAN_YIELD_INTERVAL == 0:
                                await asyncio.sleep(0)

                        # Update state
                        state.last_seen_block = latest_block
                        state.last_chain_scan = datetime.now(timezone.utc)
                        await session.commit()

                await asyncio.sleep(self.chain_sync_interval)

            except Exception as e:
                logger.error(f"Error monitoring chain: {e}")
                await asyncio.sleep(self.chain_sync_interval)

    async def _monitor_validator_heartbeats(self) -> None:
        """Monitor validator heartbeats and cleanup stale connections."""
        while self._running:
            try:
                current_time = datetime.now(timezone.utc)
                timeout_threshold = current_time - timedelta(minutes=2)

                if self.async_session:
                    async with self.async_session() as session:
                        # Find stale validators
                        result = await session.execute(
                            select(ValidatorConnection).where(
                                and_(
                                    ValidatorConnection.is_connected,
                                    ValidatorConnection.last_heartbeat
                                    < timeout_threshold,
                                )
                            )
                        )

                        for validator in result.scalars():
                            logger.warning(
                                f"Marking validator as disconnected: {validator.validator_hotkey}"
                            )
                            validator.is_connected = False

                            # Close WebSocket if exists
                            for conn_id, hotkey in list(
                                self.validator_connections.items()
                            ):
                                if hotkey == validator.validator_hotkey:
                                    if conn_id in self.active_connections:
                                        await self.active_connections[conn_id].close()
                                        del self.active_connections[conn_id]
                                    del self.validator_connections[conn_id]
                                    break

                        await session.commit()

                await asyncio.sleep(30)

            except Exception as e:
                logger.error(f"Error in heartbeat monitor: {e}")
                await asyncio.sleep(HEARTBEAT_INTERVAL)

    async def _get_latest_block(self) -> int:
        """Get latest block from chain."""
        try:
            if not self.substrate:
                return 0
            # Run in thread pool to avoid blocking
            loop = asyncio.get_event_loop()
            return await loop.run_in_executor(
                self.thread_pool, self.substrate.get_block_number
            )
        except Exception as e:
            logger.error(f"Failed to get latest block: {e}")
            return 0

    async def _sync_metagraph(self) -> None:
        """Sync metagraph nodes."""
        try:
            if self.metagraph:
                # Run in thread pool to avoid blocking
                loop = asyncio.get_event_loop()
                await loop.run_in_executor(self.thread_pool, self.metagraph.sync_nodes)
                logger.debug("Metagraph synced")
        except Exception as e:
            logger.error(f"Failed to sync metagraph: {e}")

    def _query_commitments_sync(
        self, block_num: int, nodes: list
    ) -> List[ChainCommitmentResponse]:
        """Synchronous version of commitment querying for thread pool."""
        commitments = []

        for node in nodes:
            try:
                miner_commitments = query_commitments_from_substrate(
                    self.config, node.hotkey, block=block_num
                )
                if miner_commitments:
                    commitments.extend(miner_commitments)
            except Exception as e:
                logger.debug(f"Failed to query {node.hotkey}: {e}")
                continue

        return commitments

    async def _query_block_commitments(
        self, block_num: int
    ) -> List[ChainCommitmentResponse]:
        """Query commitments for a block."""
        try:
            if not self.metagraph:
                return []

            # Get nodes as a list for thread pool execution
            nodes = list(self.metagraph.nodes.values())

            # Run commitment querying in thread pool to avoid blocking
            loop = asyncio.get_event_loop()
            commitments = await loop.run_in_executor(
                self.thread_pool, self._query_commitments_sync, block_num, nodes
            )

            return commitments

        except Exception as e:
            logger.error(f"Failed to query block {block_num}: {e}")
            return []

    async def _process_commitment(
        self,
        commitment: ChainCommitmentResponse,
        block_num: int,
        active_competitions: dict[str, Competition],
    ):
        """Process a commitment from the chain.

        This function:
        1. Validates the commitment belongs to an active competition
        2. Checks for duplicate submissions from the same miner/competition/version
        3. Creates a new MinerSubmission record in the database
        4. Creates BackendEvaluationJob(s) for each benchmark in the competition
        5. Broadcasts the job(s) to connected validators for evaluation
        """
        try:
            logger.debug(f"Processing commitment for block {block_num}: {commitment}")
            competition_id = getattr(commitment.data, "comp_id", None)

            if not competition_id or competition_id not in active_competitions:
                logger.warning(
                    f"Miner {commitment.hotkey}'s commitment at block {block_num} provided unknown competition {competition_id}"
                )
                return

            competition = active_competitions[competition_id]

            if not self.async_session:
                logger.error("Database not initialized")
                return
            async with self.async_session() as session:
                # Check if submission exists
                existing = await session.execute(
                    select(MinerSubmission).where(
                        and_(
                            MinerSubmission.miner_hotkey == commitment.hotkey,
                            MinerSubmission.competition_id == competition_id,
                            MinerSubmission.version == commitment.data.v,
                        )
                    )
                )

                if existing.scalar_one_or_none():
                    logger.debug(f"Submission already exists for {commitment.hotkey}")
                    return

                # Create submission
                submission = MinerSubmission(
                    id=next(self.id_generator),
                    miner_hotkey=commitment.hotkey,
                    competition_id=competition_id,
                    hf_repo_id=commitment.data.repo_id,
                    version=commitment.data.v,
                    commitment_block=block_num,
                )

                session.add(submission)
                await session.flush()

                # Create job
                job_id = next(self.id_generator)
                for benchmark in competition.benchmarks:
                    # check if the benchmark has a provider or a benchmark name
                    if "provider" not in benchmark or "benchmark_name" not in benchmark:
                        logger.error(
                            f"Benchmark missing provider or benchmark_name: {benchmark}"
                        )
                        continue

                    eval_job = BackendEvaluationJob(
                        id=job_id,
                        submission_id=submission.id,
                        competition_id=competition_id,
                        miner_hotkey=submission.miner_hotkey,
                        hf_repo_id=submission.hf_repo_id,
                        env_provider=benchmark["provider"],
                        benchmark_name=benchmark["benchmark_name"],
                        config=benchmark.get("config", {}),  # Optional config
                    )

                session.add(eval_job)
                await session.commit()

                logger.debug(f"Created evaluation job: {eval_job}")

                # Broadcast to validators
                await self._broadcast_job(eval_job)
                logger.debug(f"Broadcasted job {job_id} to validators")

                logger.info(f"Processed commitment from {commitment.hotkey}")

        except Exception as e:
            logger.error(f"Failed to process commitment: {e}")

    async def _update_job_status(
        self,
        job_id: int,
        validator_hotkey: str,
        status: EvaluationStatus,
        detail: str = None,
    ):
        """Update job status for a specific validator."""
        if not self.async_session:
            logger.error("Database not initialized")
            return

        try:
            async with self.async_session() as session:
                # Create new status record
                status_record = BackendEvaluationJobStatus(
                    id=next(self.id_generator),
                    job_id=job_id,
                    validator_hotkey=validator_hotkey,
                    status=status,
                    detail=detail,
                )
                session.add(status_record)
                await session.commit()

                logger.debug(
                    f"Updated job {job_id} status to {status.value} for validator {validator_hotkey}"
                )

        except Exception as e:
            logger.error(f"Failed to update job status: {e}")

    async def _update_job_status_for_validators(
        self, job_id: int, status: EvaluationStatus, detail: str = None
    ):
        """Update job status for all connected validators."""
        for validator_hotkey in self.validator_connections.values():
            await self._update_job_status(job_id, validator_hotkey, status, detail)

    async def _broadcast_job(self, job: BackendEvaluationJob):
        """Broadcast job to connected validators."""
        if not self.active_connections:
            logger.warning("No validators connected")
            return

        env_config = job.config if job.config else {}

        job_msg = EvalJobMessage(
            job_id=job.id,
            competition_id=job.competition_id,
            submission_id=job.submission_id,
            miner_hotkey=job.miner_hotkey,
            hf_repo_id=job.hf_repo_id,
            env_provider=job.env_provider,
            benchmark_name=job.benchmark_name,
            config=env_config,
        )

        message = job_msg.model_dump_json()
        broadcast_count = 0
        failed_connections = []

        for conn_id, ws in list(self.active_connections.items()):
            try:
                await ws.send_text(message)
                broadcast_count += 1
            except Exception as e:
                logger.error(f"Failed to send to {conn_id}: {e}")
                failed_connections.append(conn_id)

        # Clean up failed connections
        for conn_id in failed_connections:
            if conn_id in self.active_connections:
                del self.active_connections[conn_id]
            if conn_id in self.validator_connections:
                del self.validator_connections[conn_id]

        # Update job status to QUEUED for all validators that received the job
        if broadcast_count > 0 and self.async_session:
            await self._update_job_status_for_validators(
                job.id, EvaluationStatus.QUEUED, "Job queued to validators"
            )

        logger.info(f"Broadcasted job {job.id} to {broadcast_count} validators")
