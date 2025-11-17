"""Main provisioner worker."""

import signal
import sys
import time
from datetime import datetime
from pathlib import Path

import structlog

from models.database import init_db, get_db
from models.environment import Environment, EnvironmentStatus
from models.config import MetaConfig, from_meta_config
from engine.executor import apply_infra, destroy_infra
from api.config import settings
from api.queue import dequeue_job
from .bootstrap import bootstrap_beta9


logger = structlog.get_logger()


class ProvisionerWorker:
    """Background worker for provisioning operations."""

    def __init__(self):
        """Initialize worker."""
        self.running = True
        self.db = None

    def start(self) -> None:
        """Start the worker."""
        logger.info("Starting provisioner worker")

        # Initialize database
        self.db = init_db(settings.database_url)

        # Setup signal handlers
        signal.signal(signal.SIGINT, self._handle_shutdown)
        signal.signal(signal.SIGTERM, self._handle_shutdown)

        # Main loop
        while self.running:
            try:
                # Dequeue job
                job = dequeue_job()

                if job:
                    self._process_job(job)
                else:
                    # No job available, sleep briefly
                    time.sleep(1)

            except Exception as e:
                logger.error("Error in worker loop", error=str(e), exc_info=True)
                time.sleep(5)  # Back off on errors

        logger.info("Provisioner worker stopped")

    def _handle_shutdown(self, signum: int, frame) -> None:
        """Handle shutdown signals."""
        logger.info("Received shutdown signal", signal=signum)
        self.running = False

    def _process_job(self, job: dict) -> None:
        """
        Process a job from the queue.

        Args:
            job: Job dictionary with environment_id and job_type
        """
        environment_id = job["environment_id"]
        job_type = job["job_type"]

        logger.info(
            "Processing job",
            environment_id=environment_id,
            job_type=job_type,
        )

        if job_type == "provision":
            self._provision_environment(environment_id)
        elif job_type == "destroy":
            self._destroy_environment(environment_id)
        else:
            logger.error("Unknown job type", job_type=job_type)

    def _provision_environment(self, environment_id: str) -> None:
        """
        Provision an environment.

        Args:
            environment_id: Environment identifier
        """
        with self.db.session() as session:
            environment = session.query(Environment).filter_by(id=environment_id).first()

            if not environment:
                logger.error("Environment not found", environment_id=environment_id)
                return

            try:
                # Update status to provisioning
                environment.status = EnvironmentStatus.PROVISIONING
                session.commit()

                logger.info(
                    "Starting provisioning",
                    environment_id=environment_id,
                    name=environment.name,
                )

                # Parse meta-config
                meta = MetaConfig(**environment.meta_config)

                # Convert to infra model
                infra = from_meta_config(meta, environment_id)

                # Store infra model
                environment.infra_model = {
                    "cluster_name": infra.cluster_name,
                    "state_backend_bucket": infra.state_backend_bucket,
                    "state_backend_key": infra.state_backend_key,
                }
                environment.state_backend_config = {
                    "bucket": infra.state_backend_bucket,
                    "key": infra.state_backend_key,
                    "region": infra.state_backend_region,
                }
                session.commit()

                # Create working directory
                workdir = Path(settings.workdir_base) / environment_id
                workdir.mkdir(parents=True, exist_ok=True)

                logger.info(
                    "Applying infrastructure",
                    environment_id=environment_id,
                    workdir=str(workdir),
                )

                # Apply infrastructure
                result = apply_infra(infra, workdir)

                if not result.success:
                    raise Exception(f"Infrastructure apply failed: {result.error}")

                # Store outputs
                environment.outputs = result.outputs
                session.commit()

                logger.info(
                    "Infrastructure provisioned",
                    environment_id=environment_id,
                    outputs=result.outputs,
                )

                # Bootstrap Beta9
                environment.status = EnvironmentStatus.BOOTSTRAPPING
                session.commit()

                logger.info(
                    "Bootstrapping Beta9",
                    environment_id=environment_id,
                )

                bootstrap_beta9(infra, result.outputs)

                # Mark as ready
                environment.status = EnvironmentStatus.READY
                environment.provisioned_at = datetime.utcnow()
                session.commit()

                logger.info(
                    "Environment ready",
                    environment_id=environment_id,
                    name=environment.name,
                )

            except Exception as e:
                logger.error(
                    "Provisioning failed",
                    environment_id=environment_id,
                    error=str(e),
                    exc_info=True,
                )

                environment.status = EnvironmentStatus.FAILED
                environment.error_message = str(e)
                environment.error_details = {
                    "type": type(e).__name__,
                    "message": str(e),
                }
                session.commit()

    def _destroy_environment(self, environment_id: str) -> None:
        """
        Destroy an environment.

        Args:
            environment_id: Environment identifier
        """
        with self.db.session() as session:
            environment = session.query(Environment).filter_by(id=environment_id).first()

            if not environment:
                logger.error("Environment not found", environment_id=environment_id)
                return

            try:
                # Update status to destroying
                environment.status = EnvironmentStatus.DESTROYING
                session.commit()

                logger.info(
                    "Starting destruction",
                    environment_id=environment_id,
                    name=environment.name,
                )

                # Parse meta-config
                meta = MetaConfig(**environment.meta_config)

                # Convert to infra model
                infra = from_meta_config(meta, environment_id)

                # Create working directory
                workdir = Path(settings.workdir_base) / environment_id

                if not workdir.exists():
                    logger.warning(
                        "Working directory not found, creating",
                        environment_id=environment_id,
                        workdir=str(workdir),
                    )
                    workdir.mkdir(parents=True, exist_ok=True)

                logger.info(
                    "Destroying infrastructure",
                    environment_id=environment_id,
                    workdir=str(workdir),
                )

                # Destroy infrastructure
                result = destroy_infra(infra, workdir)

                if not result.success:
                    raise Exception(f"Infrastructure destroy failed: {result.error}")

                # Mark as destroyed
                environment.status = EnvironmentStatus.DESTROYED
                environment.destroyed_at = datetime.utcnow()
                environment.outputs = {}  # Clear outputs
                session.commit()

                logger.info(
                    "Environment destroyed",
                    environment_id=environment_id,
                    name=environment.name,
                )

            except Exception as e:
                logger.error(
                    "Destruction failed",
                    environment_id=environment_id,
                    error=str(e),
                    exc_info=True,
                )

                environment.status = EnvironmentStatus.FAILED
                environment.error_message = f"Destroy failed: {str(e)}"
                environment.error_details = {
                    "type": type(e).__name__,
                    "message": str(e),
                    "operation": "destroy",
                }
                session.commit()


def main():
    """Main entry point for worker."""
    from api.middleware import setup_logging

    setup_logging(settings.log_level)

    worker = ProvisionerWorker()
    worker.start()


if __name__ == "__main__":
    main()
