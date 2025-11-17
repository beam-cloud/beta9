"""Job queue for async provisioning operations."""

import json
import redis
import structlog

from .config import settings


logger = structlog.get_logger()

# Redis client (initialized on first use)
_redis_client: redis.Redis | None = None


def get_redis_client() -> redis.Redis:
    """Get or create Redis client."""
    global _redis_client

    if _redis_client is None:
        _redis_client = redis.from_url(
            settings.redis_url,
            decode_responses=True,
        )

    return _redis_client


def enqueue_provisioning_job(environment_id: str) -> None:
    """
    Enqueue a provisioning job.

    Args:
        environment_id: Environment identifier
    """
    client = get_redis_client()

    job = {
        "environment_id": environment_id,
        "job_type": "provision",
    }

    client.lpush("provisioning:jobs", json.dumps(job))

    logger.info("Provisioning job enqueued", environment_id=environment_id)


def enqueue_destroy_job(environment_id: str) -> None:
    """
    Enqueue a destroy job.

    Args:
        environment_id: Environment identifier
    """
    client = get_redis_client()

    job = {
        "environment_id": environment_id,
        "job_type": "destroy",
    }

    client.lpush("provisioning:jobs", json.dumps(job))

    logger.info("Destroy job enqueued", environment_id=environment_id)


def dequeue_job() -> dict | None:
    """
    Dequeue a job from the queue.

    Returns:
        Job dictionary or None if queue is empty
    """
    client = get_redis_client()

    # Blocking pop with 1 second timeout
    result = client.brpop("provisioning:jobs", timeout=1)

    if result:
        _, job_json = result
        return json.loads(job_json)

    return None
