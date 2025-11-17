"""Environment entity for persistence."""

from datetime import datetime
from enum import Enum
from typing import Optional, Dict, Any

from sqlalchemy import Column, String, DateTime, Text, Enum as SQLEnum, JSON
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.sql import func

Base = declarative_base()


class EnvironmentStatus(str, Enum):
    """Environment lifecycle status."""

    QUEUED = "queued"
    PROVISIONING = "provisioning"
    BOOTSTRAPPING = "bootstrapping"
    READY = "ready"
    FAILED = "failed"
    DESTROY_QUEUED = "destroy_queued"
    DESTROYING = "destroying"
    DESTROYED = "destroyed"


class Environment(Base):
    """
    Environment entity representing a Beta9 cloud instance.

    Stored in the control plane's Postgres database (not the per-user RDS).
    """

    __tablename__ = "environments"

    # Primary key
    id = Column(String(64), primary_key=True)  # Format: env_<uuid>

    # Identity
    name = Column(String(255), nullable=False, index=True)
    user_id = Column(String(64), index=True)  # Optional: for multi-tenancy
    organization_id = Column(String(64), index=True)

    # Cloud configuration
    cloud_provider = Column(String(16), nullable=False)
    region = Column(String(32), nullable=False)
    account_id = Column(String(64), nullable=False)

    # Configuration blobs
    meta_config = Column(JSON, nullable=False)  # Original user MetaConfig
    infra_model = Column(JSON, nullable=True)  # Computed InfraModel

    # State backend
    state_backend_config = Column(JSON, nullable=True)

    # Status
    status = Column(
        SQLEnum(EnvironmentStatus),
        nullable=False,
        default=EnvironmentStatus.QUEUED,
        index=True,
    )

    # Outputs (from Terraform)
    outputs = Column(JSON, nullable=True)  # cluster_endpoint, rds_endpoint, etc.

    # Error tracking
    error_message = Column(Text, nullable=True)
    error_details = Column(JSON, nullable=True)  # Stack traces, Terraform errors

    # Timestamps
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    updated_at = Column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False
    )
    provisioned_at = Column(DateTime(timezone=True), nullable=True)
    destroyed_at = Column(DateTime(timezone=True), nullable=True)

    def __repr__(self) -> str:
        return f"<Environment(id={self.id}, name={self.name}, status={self.status})>"

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for API responses."""
        return {
            "environment_id": self.id,
            "name": self.name,
            "cloud": {
                "provider": self.cloud_provider,
                "region": self.region,
                "account_id": self.account_id,
            },
            "status": self.status.value,
            "outputs": self.outputs or {},
            "error_message": self.error_message,
            "created_at": self.created_at.isoformat() if self.created_at else None,
            "updated_at": self.updated_at.isoformat() if self.updated_at else None,
            "provisioned_at": self.provisioned_at.isoformat() if self.provisioned_at else None,
        }


# Pydantic schemas for API
from pydantic import BaseModel, Field


class EnvironmentCreateRequest(BaseModel):
    """Request to create a new environment."""

    name: str = Field(..., min_length=1, max_length=255)
    cloud: dict = Field(..., description="Cloud configuration")
    cluster: Optional[dict] = None
    data: Optional[dict] = None
    beta9: dict = Field(..., description="Beta9 configuration")


class EnvironmentCreateResponse(BaseModel):
    """Response after creating environment."""

    environment_id: str
    status: str
    created_at: str


class EnvironmentGetResponse(BaseModel):
    """Response for getting environment details."""

    environment_id: str
    name: str
    cloud: dict
    status: str
    outputs: dict
    error_message: Optional[str] = None
    created_at: str
    updated_at: str
    provisioned_at: Optional[str] = None


class EnvironmentDeleteResponse(BaseModel):
    """Response after deleting environment."""

    environment_id: str
    status: str
