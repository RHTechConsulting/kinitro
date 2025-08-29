import asyncio
import threading

import asyncpg
import ray
from kubernetes import client, config
from pgqueuer import PgQueuer
from pgqueuer.db import AsyncpgDriver
from pgqueuer.models import Job
import capnp
from ray.util.queue import Queue

from core.db.db_manager import create_database_manager
from core.db.models import EvaluationJob
from core.log import get_logger
from evaluator.config import EvaluatorConfig
from evaluator.containers import Containers
from evaluator.rollout import BenchmarkSpec, RolloutCluster
from evaluator.rpc.client import AgentClient
from evaluator.rpc.rpc_process import start_rpc_process

logger = get_logger(__name__)


class Orchestrator:
    def __init__(self, config: EvaluatorConfig):
        self.config = config
        logger.info(f"Orchestrator initialized with db: {self.config.pg_database}")  # pyright: ignore[reportAttributeAccessIssue]
        self.db = create_database_manager(self.config.pg_database, self.config.duck_db)  # pyright: ignore[reportAttributeAccessIssue]
        logger.info(f"Orchestrator initialized with config: {self.config}")

    async def process_job(self, job: Job):
        logger.info(f"Processing job: {job.id}")
        if job.payload:
            evaluationJob = EvaluationJob.from_bytes(job.payload)
            # start a container for this evaluation job
            repo = "https://huggingface.co/" + evaluationJob.hf_repo_id
            logger.info(f"Creating container for job {evaluationJob.id} with repo {repo}")

            containers = Containers()
            pod = containers.create_container(repo, evaluationJob.submission_id)
            logger.info(f"Created pod: {pod}")

            # Get NodePort and Node IP for direct TCP connection
            config.load_kube_config()
            k8v1api = client.CoreV1Api()
            v1 = client.CoreV1Api()
            service_name = f"submission-{evaluationJob.submission_id}"
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

            submission_address = f"{node_ip}:{node_port}"

            # # Initialize Ray (ignore if already running)
            # if not ray.is_initialized():
            #     ray.init(ignore_reinit_error=True)

            # wait for 3 seconds
            await asyncio.sleep(5)
            
            # Create a benchmark spec for the job (example: MT1, can be customized)
            # Temporarily disable image observations to isolate RPC size issue
            benchmark_spec = BenchmarkSpec(
                provider=evaluationJob.env_provider,
                benchmark_name=evaluationJob.env_name,
                config={"env_name": evaluationJob.env_name}
                if hasattr(evaluationJob, "env_name")
                else {},
                enable_image_obs=False,  # Disable image observations for now
                render_mode=None,  # No rendering
            )

            # Create Ray queues - name them clearly for the worker's perspective
            worker_to_rpc_queue = Queue(maxsize=100)   # Worker sends TO rpc process
            rpc_to_worker_queue = Queue(maxsize=100)   # RPC process sends TO worker

            cluster = RolloutCluster("eval-cluster")
            worker = cluster.create_worker(
                evaluationJob.id, [benchmark_spec], node_ip, node_port, evaluationJob.submission_id
            )

            # RPC process: receives from worker_to_rpc_queue, sends to rpc_to_worker_queue
            rpc_thread = threading.Thread(
                target=start_rpc_process,
                args=(node_ip, node_port, rpc_to_worker_queue, worker_to_rpc_queue),  # SWAPPED!
                daemon=True
            )
            rpc_thread.start()
            await asyncio.sleep(1)

            # Worker: sends to worker_to_rpc_queue, receives from rpc_to_worker_queue  
            res = await worker.test_rpc.remote(worker_to_rpc_queue, rpc_to_worker_queue)
            print(f"rpc test result: {res}")

            # # Create a Ray cluster and worker
            # cluster = RolloutCluster("eval-cluster")
            # # Use the job id as the worker id (SnowflakeId is int)
            # worker = cluster.create_worker(
            #     evaluationJob.id, [benchmark_spec], node_ip, node_port, evaluationJob.submission_id
            # )

            # print(f"Worker created: {worker}")
            # pong = ray.get(worker.test_rpc.remote())
            # print("RPC test response:", pong)
            # Set config for the worker (connect to agent directly)
            # ray.get(worker.set_config.remote(submission_address))

            # Connect the worker to the agent with retry logic
            # This requires the worker's connect_to_agent method to be async, so we use ray's async API
            # Note: Ray remote async methods return an ObjectRef (future)
            # max_retries = 5
            # retry_delay = 5  # seconds
            
            # for attempt in range(max_retries):
            #     try:
            #         print(f"Attempting to connect to agent (attempt {attempt + 1}/{max_retries})...")
            #         connect_future = worker.connect_to_agent.remote()
            #         ray.get(connect_future, timeout=30)  # 30 second timeout
            #         print(f"Successfully connected to agent on attempt {attempt + 1}")
            #         break
            #     except Exception as e:
            #         print(f"Connection attempt {attempt + 1} failed: {e}")
            #         if attempt < max_retries - 1:
            #             print(f"Retrying in {retry_delay} seconds...")
            #             await asyncio.sleep(retry_delay)
            #         else:
            #             print(f"Failed to connect after {max_retries} attempts")
            #             raise

            # print(f"Ray worker created and connected to agent at {submission_address}")
            
            # # Start the actual evaluation
            # print("Starting evaluation...")
            # try:
            #     # Run all benchmark tasks
            #     evaluation_future = worker.run_all_benchmark_tasks.remote()
            #     results = ray.get(evaluation_future, timeout=3600)  # 1 hour timeout for evaluation
                
            #     print(f"Evaluation completed successfully with {len(results)} results")
            #     print(f"Summary: {len(results)} tasks completed")
                
            #     # Log success metrics
            #     if results:
            #         total_episodes = sum(len(result.episodes) for result in results)
            #         avg_success_rate = sum(result.success_rate for result in results) / len(results)
            #         avg_reward = sum(result.mean_reward for result in results) / len(results)
                    
            #         print(f"Total episodes: {total_episodes}")
            #         print(f"Average success rate: {avg_success_rate:.3f}")
            #         print(f"Average reward: {avg_reward:.3f}")
                
            # except Exception as e:
            #     print(f"Evaluation failed: {e}")
            #     logger.error(f"Evaluation failed for job {evaluationJob.id}: {e}")
            #     raise
            
            logger.info(f"Processed: {evaluationJob!r}")

    async def start(self):
        logger.info("Starting orchestrator...")
        conn = await asyncpg.connect(
                dsn="postgresql://myuser:mypassword@localhost:5432/kinitrodb"
                )

        driver = AsyncpgDriver(conn)
        pgq = PgQueuer(driver)

        @pgq.entrypoint("add_job")
        async def process(job: Job) -> None:
            asyncio.get_event_loop().create_task(self.process_job(job))
            logger.info(f"Job {job.id} added to processing queue.")

        await pgq.run()
        await asyncio.Future()

    def stop(self):
        logger.info("Stopping orchestrator...")
        # Add cleanup logic here
        pass


if __name__ == "__main__":
    orc = Orchestrator(EvaluatorConfig())
    asyncio.run(orc.start())
