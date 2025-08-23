import asyncio

import asyncpg
import ray
from kubernetes import client, config
from pgqueuer import PgQueuer
from pgqueuer.db import AsyncpgDriver
from pgqueuer.models import Job

from core.db.db_manager import create_database_manager
from core.db.models import EvaluationJob
from core.log import get_logger
from evaluator.config import EvaluatorConfig
from evaluator.containers import Containers
from evaluator.rollout import BenchmarkSpec, RolloutCluster

logger = get_logger(__name__)


class Orchestrator:
    def __init__(self, config: EvaluatorConfig):
        self.config = config
        print(f"Orchestrator initialized with db: {self.config.pg_database}")  # pyright: ignore[reportAttributeAccessIssue]
        self.db = create_database_manager(self.config.pg_database, self.config.duck_db)  # pyright: ignore[reportAttributeAccessIssue]
        logger.info(f"Orchestrator initialized with config: {self.config}")

    async def process_job(self, job: Job):
        logger.info(f"Processing job: {job.id}")
        if job.payload:
            evaluationJob = EvaluationJob.from_bytes(job.payload)
            # start a container for this evaluation job
            repo = "https://huggingface.co/" + evaluationJob.hf_repo_id
            print(f"Creating container for job {evaluationJob.id} with repo {repo}")

            containers = Containers()
            pod = containers.create_container(repo, evaluationJob.submission_id)
            print(f"Created pod: {pod}")

            # --- NodePort/NodeIP logic ---
            # Get the NodePort and Node IP for the service

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
            print(f"Connecting to agent at {submission_address}")

            # Initialize Ray (ignore if already running)
            if not ray.is_initialized():
                ray.init(ignore_reinit_error=True)

            # Create a benchmark spec for the job (example: MT1, can be customized)
            benchmark_spec = BenchmarkSpec(
                provider=evaluationJob.env_provider,
                benchmark_name=evaluationJob.env_name,
                config={"env_name": evaluationJob.env_name}
                if hasattr(evaluationJob, "env_name")
                else {},
            )

            # Create a Ray cluster and worker
            cluster = RolloutCluster("eval-cluster")
            # Use the job id as the worker id (SnowflakeId is int)
            worker = cluster.create_worker(
                evaluationJob.id, [benchmark_spec], node_ip, node_port
            )
            # Set config for the worker (connect to agent on port 8000)
            ray.get(worker.set_config.remote(submission_address))

            # Optionally, connect the worker to the agent (async)
            # This requires the worker's connect_to_agent method to be async, so we use ray's async API
            # Note: Ray remote async methods return an ObjectRef (future)
            connect_future = worker.connect_to_agent.remote()
            ray.get(connect_future)

            print(f"Ray worker created and connected to agent at {submission_address}")
            print(f"Processed: {evaluationJob!r}")

    async def start(self):
        logger.info("Starting orchestrator...")
        conn = await asyncpg.connect()

        driver = AsyncpgDriver(conn)
        pgq = PgQueuer(driver)

        @pgq.entrypoint("add_job")
        async def process(job: Job) -> None:
            asyncio.get_event_loop().create_task(self.process_job(job))
            print(f"Job {job.id} added to processing queue.")

        await pgq.run()
        return pgq

    def stop(self):
        logger.info("Stopping orchestrator...")
        # Add cleanup logic here
        pass


if __name__ == "__main__":
    orc = Orchestrator(EvaluatorConfig())
    asyncio.run(orc.start())
