"""API routes for environment lifecycle."""

import uuid
from datetime import datetime
from pathlib import Path

from fastapi import APIRouter, HTTPException, Depends
from sqlalchemy.orm import Session
import structlog

from models.config import MetaConfig, from_meta_config
from models.environment import (
    Environment,
    EnvironmentStatus,
    EnvironmentCreateRequest,
    EnvironmentCreateResponse,
    EnvironmentGetResponse,
    EnvironmentDeleteResponse,
)
from models.database import get_db
from .config import settings
from .queue import enqueue_provisioning_job, enqueue_destroy_job


logger = structlog.get_logger()
router = APIRouter()


def get_db_session():
    """Dependency to get database session."""
    db = get_db()
    with db.session() as session:
        yield session


@router.post("/environments", response_model=EnvironmentCreateResponse)
async def create_environment(
    request: EnvironmentCreateRequest,
    session: Session = Depends(get_db_session),
):
    """
    Create a new environment.

    This queues a provisioning job that will:
    1. Create VPC, EKS, RDS, Redis, S3 in the user's cloud account
    2. Bootstrap Beta9 into the cluster
    """
    try:
        # Parse and validate meta-config
        meta_config_dict = {
            "name": request.name,
            "cloud": request.cloud,
            "cluster": request.cluster or {},
            "data": request.data or {},
            "beta9": request.beta9,
        }

        try:
            meta = MetaConfig(**meta_config_dict)
        except Exception as e:
            raise HTTPException(status_code=400, detail=f"Invalid configuration: {str(e)}")

        # Validate region
        if meta.cloud.region not in settings.allowed_regions:
            raise HTTPException(
                status_code=400,
                detail=f"Region {meta.cloud.region} not allowed. "
                f"Allowed regions: {settings.allowed_regions}",
            )

        # Check environment limit per user
        # TODO: Implement user authentication and extract user_id
        user_id = "default-user"  # Placeholder

        existing_count = (
            session.query(Environment)
            .filter_by(user_id=user_id)
            .filter(Environment.status.in_([
                EnvironmentStatus.QUEUED,
                EnvironmentStatus.PROVISIONING,
                EnvironmentStatus.BOOTSTRAPPING,
                EnvironmentStatus.READY,
            ]))
            .count()
        )

        if existing_count >= settings.max_environments_per_user:
            raise HTTPException(
                status_code=429,
                detail=f"Maximum environments limit reached ({settings.max_environments_per_user})",
            )

        # Generate environment ID
        environment_id = f"env_{uuid.uuid4().hex[:12]}"

        # Create environment record
        environment = Environment(
            id=environment_id,
            name=request.name,
            user_id=user_id,
            cloud_provider=meta.cloud.provider.value,
            region=meta.cloud.region,
            account_id=meta.cloud.account_id,
            meta_config=meta_config_dict,
            status=EnvironmentStatus.QUEUED,
        )

        session.add(environment)
        session.commit()

        logger.info(
            "Environment created",
            environment_id=environment_id,
            name=request.name,
            cloud=meta.cloud.provider.value,
            region=meta.cloud.region,
        )

        # Enqueue provisioning job
        enqueue_provisioning_job(environment_id)

        return EnvironmentCreateResponse(
            environment_id=environment_id,
            status=EnvironmentStatus.QUEUED.value,
            created_at=environment.created_at.isoformat(),
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.error("Failed to create environment", error=str(e), exc_info=True)
        raise HTTPException(status_code=500, detail="Failed to create environment")


@router.get("/environments/{environment_id}", response_model=EnvironmentGetResponse)
async def get_environment(
    environment_id: str,
    session: Session = Depends(get_db_session),
):
    """
    Get environment status and outputs.

    Returns the current status, configuration, and infrastructure outputs
    (cluster endpoint, database endpoint, etc.).
    """
    environment = session.query(Environment).filter_by(id=environment_id).first()

    if not environment:
        raise HTTPException(status_code=404, detail="Environment not found")

    return EnvironmentGetResponse(
        environment_id=environment.id,
        name=environment.name,
        cloud={
            "provider": environment.cloud_provider,
            "region": environment.region,
            "account_id": environment.account_id,
        },
        status=environment.status.value,
        outputs=environment.outputs or {},
        error_message=environment.error_message,
        created_at=environment.created_at.isoformat(),
        updated_at=environment.updated_at.isoformat(),
        provisioned_at=(
            environment.provisioned_at.isoformat() if environment.provisioned_at else None
        ),
    )


@router.delete("/environments/{environment_id}", response_model=EnvironmentDeleteResponse)
async def delete_environment(
    environment_id: str,
    session: Session = Depends(get_db_session),
):
    """
    Destroy an environment.

    This queues a destroy job that will tear down all infrastructure
    in the user's cloud account.
    """
    environment = session.query(Environment).filter_by(id=environment_id).first()

    if not environment:
        raise HTTPException(status_code=404, detail="Environment not found")

    # Check if environment can be destroyed
    if environment.status in [
        EnvironmentStatus.DESTROYING,
        EnvironmentStatus.DESTROYED,
    ]:
        raise HTTPException(
            status_code=400,
            detail=f"Environment already in {environment.status.value} state",
        )

    # Update status
    environment.status = EnvironmentStatus.DESTROY_QUEUED
    session.commit()

    logger.info(
        "Environment destroy queued",
        environment_id=environment_id,
        name=environment.name,
    )

    # Enqueue destroy job
    enqueue_destroy_job(environment_id)

    return EnvironmentDeleteResponse(
        environment_id=environment_id,
        status=EnvironmentStatus.DESTROY_QUEUED.value,
    )


@router.get("/environments")
async def list_environments(
    status: str = None,
    session: Session = Depends(get_db_session),
):
    """
    List all environments.

    Optionally filter by status.
    """
    # TODO: Add user filtering based on authentication
    query = session.query(Environment)

    if status:
        try:
            status_enum = EnvironmentStatus(status)
            query = query.filter_by(status=status_enum)
        except ValueError:
            raise HTTPException(status_code=400, detail=f"Invalid status: {status}")

    environments = query.order_by(Environment.created_at.desc()).limit(100).all()

    return {
        "environments": [env.to_dict() for env in environments],
        "total": len(environments),
    }
