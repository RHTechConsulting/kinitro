"""
WebSocket-based validator service for Kinitro.

This new validator architecture connects directly to the Kinitro Backend
via WebSocket and receives evaluation jobs from there
"""

import asyncio
import json
from typing import Optional

import asyncpg
import websockets
from pgqueuer import Job, PgQueuer, Queries
from pgqueuer.db import AsyncpgDriver
from websockets.exceptions import ConnectionClosed, WebSocketException

from core.log import get_logger
from core.messages import (
    EpisodeDataMessage,
    EpisodeStepDataMessage,
    EvalJobMessage,
    EvalResultMessage,
    HeartbeatMessage,
    ValidatorRegisterMessage,
)
from core.neuron import Neuron

from .config import ValidatorConfig

logger = get_logger(__name__)


class WebSocketValidator(Neuron):
    """
    WebSocket-based validator that connects to the Kinitro backend.
    """

    def __init__(self, config: ValidatorConfig):
        super().__init__(config)
        self.hotkey = self.keypair.ss58_address

        # Backend connection settings
        self.backend_url = config.settings.get(
            "backend_url", "ws://localhost:8080/ws/validator"
        )
        self.reconnect_interval = config.settings.get("reconnect_interval", 5)
        self.heartbeat_interval = config.settings.get("heartbeat_interval", 30)

        # Connection state
        self.websocket: Optional[websockets.ServerConnection] = None
        self.connected = False
        self._running = False
        self._heartbeat_task = None
        self._result_processor_task = None

        # Database and pgqueue
        self.database_url = config.settings.get(
            "pg_database", "postgresql://myuser:mypassword@localhost/validatordb"
        )

        if self.database_url is None:
            raise Exception("Database URL not provided")

        self.q: Optional[Queries] = None

        # Job tracking
        # TODO: remove?
        # self.active_jobs: Dict[str, EvalJobMessage] = {}

        logger.info(f"WebSocket Validator initialized for hotkey: {self.hotkey}")

    async def start(self):
        """Start the validator service."""
        logger.info("Starting WebSocket Validator")
        self._running = True

        # Start the result processor task
        self._result_processor_task = asyncio.create_task(self._process_results())

        # Connect to backend with auto-reconnect
        while self._running:
            try:
                await self._connect_to_backend()
                # Connection lost, wait before retry
                if self._running:
                    logger.warning(
                        f"Connection lost, reconnecting in {self.reconnect_interval}s"
                    )
                    await asyncio.sleep(self.reconnect_interval)
            except Exception as e:
                logger.error(f"Failed to connect to backend: {e}")
                if self._running:
                    await asyncio.sleep(self.reconnect_interval)

    async def stop(self):
        """Stop the validator service."""
        logger.info("Stopping WebSocket Validator")
        self._running = False

        # Cancel heartbeat
        if self._heartbeat_task:
            self._heartbeat_task.cancel()
            try:
                await self._heartbeat_task
            except asyncio.CancelledError:
                pass

        # Cancel result processor
        if self._result_processor_task:
            self._result_processor_task.cancel()
            try:
                await self._result_processor_task
            except asyncio.CancelledError:
                pass

        # Close WebSocket connection
        if self.websocket:
            await self.websocket.close()
            self.connected = False

        logger.info("WebSocket Validator stopped")

    async def _connect_to_backend(self):
        """Connect to backend and handle messages."""
        try:
            logger.info(f"Connecting to backend: {self.backend_url}")

            async with websockets.connect(
                self.backend_url,
                ping_interval=20,
                ping_timeout=10,
                close_timeout=10,
            ) as websocket:
                self.websocket = websocket

                # Register with backend
                await self._register()

                # Start heartbeat
                self._heartbeat_task = asyncio.create_task(self._heartbeat_loop())

                # Handle messages
                await self._message_loop()

        except ConnectionClosed:
            logger.warning("Backend connection closed")
        except WebSocketException as e:
            logger.error(f"WebSocket error: {e}")
        except Exception as e:
            logger.error(f"Connection error: {e}")
        finally:
            self.connected = False
            if self._heartbeat_task:
                self._heartbeat_task.cancel()

    async def _register(self):
        """Register validator with backend."""
        register_msg = ValidatorRegisterMessage(hotkey=self.hotkey)
        await self._send_message(register_msg.model_dump())

        # Wait for acknowledgment
        response = await self.websocket.recv()
        ack = json.loads(response)

        if (
            ack.get("message_type") == "registration_ack"
            and ack.get("status") == "registered"
        ):
            self.connected = True
            logger.info("Successfully registered with backend")
        else:
            raise Exception(f"Registration failed: {ack}")

    async def _heartbeat_loop(self):
        """Send periodic heartbeats to backend."""
        try:
            while self.connected and not self._heartbeat_task.cancelled():
                # Send heartbeat with queue size
                # queue_size = len(self.active_jobs)
                # TODO: 6969 is a placeholder
                heartbeat = HeartbeatMessage(queue_size=6969)
                await self._send_message(heartbeat.model_dump())

                # Wait for heartbeat interval
                await asyncio.sleep(self.heartbeat_interval)

        except asyncio.CancelledError:
            logger.debug("Heartbeat task cancelled")
        except Exception as e:
            logger.error(f"Heartbeat error: {e}")

    async def _message_loop(self):
        """Handle incoming messages from backend."""
        try:
            async for message in self.websocket:
                try:
                    data = json.loads(message)
                    message_type = data.get("message_type")

                    # TODO: create handler for receiving and setting weights
                    if message_type == "eval_job":
                        await self._handle_eval_job(EvalJobMessage(**data))
                    elif message_type == "heartbeat_ack":
                        logger.debug("Received heartbeat ack")
                    elif message_type == "error":
                        logger.error(f"Backend error: {data.get('error')}")
                    else:
                        logger.warning(f"Unknown message type: {message_type}")

                except json.JSONDecodeError as e:
                    logger.error(f"Failed to decode message: {e}")
                except Exception as e:
                    logger.error(f"Error handling message: {e}")

        except ConnectionClosed:
            logger.info("Message loop ended - connection closed")
        except Exception as e:
            logger.error(f"Message loop error: {e}")

    # TODO: implement proper eval job handling
    async def _handle_eval_job(self, job: EvalJobMessage):
        """Handle evaluation job from backend."""
        logger.info(
            f"Received evaluation job: {job.job_id} for miner {job.miner_hotkey}"
        )

        # Track active job
        # self.active_jobs[job.job_id] = job

        # Queue the job with pgqueuer to the database
        job_bytes = job.to_bytes()
        logger.info(f"Queueing job {job.job_id} to database")
        # Connect to the postgres database
        conn = await asyncpg.connect(dsn=self.database_url)
        driver = AsyncpgDriver(conn)
        q = Queries(driver)
        await q.enqueue(["add_job"], [job_bytes], [0])
        logger.info(f"Job {job.job_id} queued successfully")

    async def _process_results(self):
        """Process evaluation results from pgqueue and send them to the backend."""
        logger.info("Starting result processor task")

        try:
            # Connect to the postgres database
            conn = await asyncpg.connect(dsn=self.database_url)
            driver = AsyncpgDriver(conn)
            pgq = PgQueuer(driver)

            @pgq.entrypoint("eval_result")
            async def process_result(job: Job) -> None:
                """Process an evaluation result from the queue."""
                try:
                    # Parse the result from the job payload
                    result_data = json.loads(job.payload.decode("utf-8"))
                    eval_result = EvalResultMessage(**result_data)

                    logger.info(
                        f"Processing evaluation result for job {eval_result.job_id}"
                    )

                    # Send the result to the backend if connected
                    if self.connected and self.websocket:
                        await self._send_eval_result(eval_result)
                        logger.info(
                            f"Sent evaluation result for job {eval_result.job_id} to backend"
                        )
                    else:
                        # If not connected, the job will remain in queue and be retried
                        logger.warning(
                            f"Not connected to backend, result for job {eval_result.job_id} will be retried"
                        )
                        raise Exception("Not connected to backend")

                except Exception as e:
                    logger.error(f"Failed to process evaluation result: {e}")
                    # Re-raise to let pgqueue handle retry
                    raise

            @pgq.entrypoint("episode_data")
            async def process_episode_data(job: Job) -> None:
                """Process episode data from the queue."""
                try:
                    # Parse the episode data from the job payload
                    episode_data = json.loads(job.payload.decode("utf-8"))
                    episode_msg = EpisodeDataMessage(**episode_data)

                    logger.info(
                        f"Processing episode data for submission {episode_msg.submission_id}, episode {episode_msg.episode_id}"
                    )

                    # Send the episode data to the backend if connected
                    if self.connected and self.websocket:
                        await self._send_episode_data(episode_msg)
                        logger.info(
                            f"Sent episode data for episode {episode_msg.episode_id} to backend"
                        )
                    else:
                        # If not connected, the job will remain in queue and be retried
                        logger.warning(
                            f"Not connected to backend, episode data for episode {episode_msg.episode_id} will be retried"
                        )
                        raise Exception("Not connected to backend")

                except Exception as e:
                    logger.error(f"Failed to process episode data: {e}")
                    # Re-raise to let pgqueue handle retry
                    raise

            @pgq.entrypoint("episode_step_data")
            async def process_episode_step_data(job: Job) -> None:
                """Process episode step data from the queue."""
                try:
                    # Parse the step data from the job payload
                    step_data = json.loads(job.payload.decode("utf-8"))
                    step_msg = EpisodeStepDataMessage(**step_data)

                    logger.info(
                        f"Processing step data for submission {step_msg.submission_id}, episode {step_msg.episode_id}, step {step_msg.step}"
                    )

                    # Send the step data to the backend if connected
                    if self.connected and self.websocket:
                        await self._send_episode_step_data(step_msg)
                        logger.info(
                            f"Sent step data for episode {step_msg.episode_id}, step {step_msg.step} to backend"
                        )
                    else:
                        # If not connected, the job will remain in queue and be retried
                        logger.warning(
                            f"Not connected to backend, step data for episode {step_msg.episode_id}, step {step_msg.step} will be retried"
                        )
                        raise Exception("Not connected to backend")

                except Exception as e:
                    logger.error(f"Failed to process episode step data: {e}")
                    # Re-raise to let pgqueue handle retry
                    raise

            logger.info("Result processor is now listening for evaluation results...")
            await pgq.run()

        except asyncio.CancelledError:
            logger.info("Result processor task cancelled")
            raise
        except Exception as e:
            logger.error(f"Result processor error: {e}")
            # Restart the processor after a delay if still running
            if self._running:
                await asyncio.sleep(5)
                self._result_processor_task = asyncio.create_task(
                    self._process_results()
                )

    async def _send_eval_result(self, result: EvalResultMessage):
        """Send evaluation result to the backend."""
        await self._send_message(result.model_dump())

    async def _send_episode_data(self, episode_data: EpisodeDataMessage):
        """Send episode data to the backend."""
        await self._send_message(episode_data.model_dump())

    async def _send_episode_step_data(self, step_data: EpisodeStepDataMessage):
        """Send episode step data to the backend."""
        await self._send_message(step_data.model_dump())

    async def _send_message(self, message: dict):
        """Send message to backend."""
        if not self.websocket:
            raise Exception("No WebSocket connection")

        try:
            message_json = json.dumps(message, default=str)
            await self.websocket.send(message_json)
        except Exception as e:
            logger.error(f"Failed to send message: {e}")
            raise
