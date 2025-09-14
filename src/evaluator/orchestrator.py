import asyncio
import threading
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple

import asyncpg
import ray
from fiber.chain.chain_utils import load_hotkey_keypair
from kubernetes import client, config
from pgqueuer import PgQueuer
from pgqueuer.db import AsyncpgDriver
from pgqueuer.models import Job
from ray.util.queue import Queue
from snowflake import SnowflakeGenerator

from core.db.models import EvaluationStatus
from core.log import get_logger
from core.messages import EvalJobMessage, EvalResultMessage
from evaluator.config import EvaluatorConfig
from evaluator.containers import Containers
from evaluator.rollout import BenchmarkSpec, RolloutCluster
from evaluator.rollout.envs import EnvResult
from evaluator.rpc.rpc_process import RPCProcess
from validator.db.db_manager import DatabaseManager
from validator.db.models import EvaluationJob

logger = get_logger(__name__)

WAIT_TIME = 5
PROCESS_JOB_WAIT_TIME = 1
QUEUE_MAXSIZE = 100
# TODO: this might be way too long
EVAL_TIMEOUT = 3600


class Orchestrator:
    def __init__(self, config: EvaluatorConfig):
        self.config = config
        logger.info(f"Orchestrator initialized with db: {self.config.pg_database}")
        self.db = DatabaseManager(self.config.pg_database)
        self.id_generator = SnowflakeGenerator(42)
        self.keypair = load_hotkey_keypair(
            wallet_name=config.settings["wallet_name"],
            hotkey_name=config.settings["hotkey_name"],
        )

        # Track running jobs for concurrent execution
        self.running_jobs: Dict[str, Dict] = {}  # job_id -> job_info
        self.max_concurrent_jobs = config.max_concurrent_jobs

        logger.info(f"Orchestrator initialized with config: {self.config}")

    async def setup_job(self, job: Job) -> Optional[Dict]:
        """Setup job infrastructure and return job context for monitoring."""
        logger.info(f"Setting up job: {job.id}")
        if not job.payload:
            return None

        eval_job_msg = EvalJobMessage.from_bytes(job.payload)
        evaluation_job = EvaluationJob(
            id=eval_job_msg.job_id,
            competition_id=eval_job_msg.competition_id,
            submission_id=eval_job_msg.submission_id,
            miner_hotkey=eval_job_msg.miner_hotkey,
            hf_repo_id=eval_job_msg.hf_repo_id,
            env_provider=eval_job_msg.env_provider,
            benchmark_name=eval_job_msg.benchmark_name,
            config=eval_job_msg.config,
            created_at=datetime.now(timezone.utc),
        )

        # Create job entry in the database
        try:
            self.db.create_evaluation_job(evaluation_job)
        except Exception as e:
            logger.error(
                f"Failed to create evaluation job {eval_job_msg.job_id} in DB: {e}"
            )
            return None

        # Start a container for this evaluation job
        repo = "https://huggingface.co/" + eval_job_msg.hf_repo_id
        logger.info(
            f"Creating container for job {eval_job_msg.job_id} with repo {repo}"
        )

        containers = Containers()
        pod = containers.create_container(repo, eval_job_msg.submission_id)
        logger.info(f"Created pod: {pod}")

        # Get NodePort and Node IP for direct TCP connection
        config.load_kube_config()
        k8v1api = client.CoreV1Api()
        v1 = client.CoreV1Api()
        service_name = f"submission-{eval_job_msg.submission_id}"
        svc = k8v1api.read_namespaced_service(service_name, "default")
        node_port = None
        for port in svc.spec.ports:
            if port.node_port:
                node_port = port.node_port
                break
        if not node_port:
            raise RuntimeError(f"No nodePort found for service {service_name}")

        # Get the first node's external IP (or internal if not available)
        nodes = v1.list_node().items
        node_ip = None
        for node in nodes:
            for addr in node.status.addresses:
                if addr.type == "ExternalIP":
                    node_ip = addr.address
                    break
            if not node_ip:
                for addr in node.status.addresses:
                    if addr.type == "InternalIP":
                        node_ip = addr.address
                        break
            if node_ip:
                break
        if not node_ip:
            raise RuntimeError("No node IP found in cluster")

        # Wait for container to be ready
        await asyncio.sleep(WAIT_TIME)

        # Create a benchmark spec for the job
        benchmark_spec = BenchmarkSpec(
            provider=eval_job_msg.env_provider,
            benchmark_name=eval_job_msg.benchmark_name,
            config=eval_job_msg.config,
            enable_image_obs=True,
            render_mode="rgb_array",
        )

        worker_to_rpc_queue = Queue(maxsize=QUEUE_MAXSIZE)
        rpc_to_worker_queue = Queue(maxsize=QUEUE_MAXSIZE)

        cluster = RolloutCluster("eval-cluster")
        worker = cluster.create_worker(
            eval_job_msg.job_id,
            [benchmark_spec],
            node_ip,
            node_port,
            eval_job_msg.submission_id,
            r2_config=self.config.r2_config,
            episode_log_interval=self.config.episode_log_interval,
            step_log_interval=self.config.step_log_interval,
            database_url=self.config.pg_database,
        )

        rpc_thread = threading.Thread(
            target=RPCProcess,
            args=(node_ip, node_port, rpc_to_worker_queue, worker_to_rpc_queue),
            daemon=True,
        )
        rpc_thread.start()

        await asyncio.sleep(PROCESS_JOB_WAIT_TIME)

        # Test RPC connection
        res = await worker.test_rpc.remote(worker_to_rpc_queue, rpc_to_worker_queue)
        logger.info(f"RPC test result for job {eval_job_msg.job_id}: {res}")

        # Start the evaluation (non-blocking)
        logger.info(f"Starting evaluation for job {eval_job_msg.job_id}")
        evaluation_future = worker.run_all_benchmark_tasks.remote(
            worker_to_rpc_queue, rpc_to_worker_queue
        )

        return {
            "job_id": eval_job_msg.job_id,
            "eval_job_msg": eval_job_msg,
            "worker": worker,
            "cluster": cluster,
            "evaluation_future": evaluation_future,
            "start_time": datetime.now(timezone.utc),
        }

    async def monitor_job(self, job_context: Dict):
        """Monitor a running job and handle completion."""
        job_id = job_context["job_id"]
        eval_job_msg = job_context["eval_job_msg"]
        evaluation_future = job_context["evaluation_future"]

        try:
            # Use ray.wait with timeout to check if job is done without blocking
            ready, not_ready = ray.wait([evaluation_future], timeout=0.1)

            if ready:
                # Job completed, get results
                results: List[EnvResult] = ray.get(evaluation_future)

                logger.info(
                    f"Evaluation completed for job {job_id} with {len(results)} results"
                )

                # Calculate metrics
                if results:
                    total_episodes = sum(len(result.episodes) for result in results)
                    avg_success_rate = sum(
                        result.success_rate for result in results
                    ) / len(results)
                    avg_reward = sum(result.mean_reward for result in results) / len(
                        results
                    )

                    logger.info(f"Job {job_id} - Total episodes: {total_episodes}")
                    logger.info(
                        f"Job {job_id} - Average success rate: {avg_success_rate:.3f}"
                    )
                    logger.info(f"Job {job_id} - Average reward: {avg_reward:.3f}")
                else:
                    total_episodes = 0
                    avg_success_rate = 0.0
                    avg_reward = 0.0

                # Update database
                try:
                    self.db.update_evaluation_job(
                        job_id, {"status": EvaluationStatus.COMPLETED}
                    )
                except Exception as e:
                    logger.error(f"Failed to update job status for job {job_id}: {e}")

                # Queue result message
                eval_result_msg = EvalResultMessage(
                    job_id=job_id,
                    validator_hotkey=self.keypair.ss58_address,
                    miner_hotkey=eval_job_msg.miner_hotkey,
                    competition_id=eval_job_msg.competition_id,
                    env_provider=eval_job_msg.env_provider,
                    benchmark_name=eval_job_msg.benchmark_name,
                    config=eval_job_msg.config,
                    score=avg_reward,
                    success_rate=avg_success_rate,
                    avg_reward=avg_reward,
                    total_episodes=total_episodes,
                    logs="Evaluation completed successfully",
                    error=None,
                    extra_data=None,
                )
                await self.db.queue_evaluation_result_msg(eval_result_msg)

                return True  # Job completed

            # Check for timeout
            elapsed = (
                datetime.now(timezone.utc) - job_context["start_time"]
            ).total_seconds()
            if elapsed > EVAL_TIMEOUT:
                logger.error(f"Job {job_id} timed out after {elapsed} seconds")
                ray.cancel(evaluation_future)
                self.db.update_evaluation_job(
                    job_id, {"status": EvaluationStatus.FAILED}
                )
                return True  # Job timed out

            return False  # Job still running

        except Exception as e:
            logger.error(f"Error monitoring job {job_id}: {e}")
            self.db.update_evaluation_job(job_id, {"status": EvaluationStatus.FAILED})
            return True  # Job failed

    async def process_job(self, job: Job):
        """Process a job asynchronously without blocking."""
        job_context = await self.setup_job(job)
        if job_context:
            job_id = job_context["job_id"]
            self.running_jobs[job_id] = job_context
            logger.info(
                f"Job {job_id} added to running jobs. Total running: {len(self.running_jobs)}"
            )

    async def monitor_running_jobs(self):
        """Background task to monitor all running jobs."""
        while True:
            if self.running_jobs:
                completed_jobs = []
                for job_id, job_context in list(self.running_jobs.items()):
                    is_complete = await self.monitor_job(job_context)
                    if is_complete:
                        completed_jobs.append(job_id)

                # Remove completed jobs
                for job_id in completed_jobs:
                    del self.running_jobs[job_id]
                    logger.info(
                        f"Job {job_id} removed from running jobs. Remaining: {len(self.running_jobs)}"
                    )

            await asyncio.sleep(1)  # Check every second

    async def start(self):
        logger.info("Starting orchestrator...")
        conn = await asyncpg.connect(dsn=self.config.pg_database)

        driver = AsyncpgDriver(conn)
        pgq = PgQueuer(driver)

        # Start the job monitoring task
        monitor_task = asyncio.create_task(self.monitor_running_jobs())
        logger.info("Started job monitoring task")

        @pgq.entrypoint("add_job")
        async def process(job: Job) -> None:
            # Check if we can accept more jobs
            if len(self.running_jobs) >= self.max_concurrent_jobs:
                logger.warning(
                    f"Max concurrent jobs ({self.max_concurrent_jobs}) reached. Job {job.id} will be delayed."
                )
                # Wait until a slot becomes available
                while len(self.running_jobs) >= self.max_concurrent_jobs:
                    await asyncio.sleep(1)

            asyncio.get_event_loop().create_task(self.process_job(job))
            logger.info(f"Job {job.id} added to processing queue.")

        logger.info(
            f"Orchestrator is now listening for jobs (max concurrent: {self.max_concurrent_jobs})..."
        )

        try:
            await pgq.run()
        finally:
            monitor_task.cancel()

        await asyncio.Future()

    def stop(self):
        logger.info("Stopping orchestrator...")
        # Add cleanup logic here
        pass


if __name__ == "__main__":
    orc = Orchestrator(EvaluatorConfig())
    asyncio.run(orc.start())
