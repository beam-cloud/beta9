"""API configuration."""

from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    """Application settings."""

    # Database
    database_url: str = "postgresql://postgres:postgres@localhost:5432/provisioning"

    # Redis
    redis_url: str = "redis://localhost:6379/0"

    # API
    api_host: str = "0.0.0.0"
    api_port: int = 8000
    api_workers: int = 4

    # Working directory for CDKTF operations
    workdir_base: str = "/tmp/beta9-provisioning"

    # Security
    max_environments_per_user: int = 10
    allowed_regions: list[str] = [
        "us-east-1",
        "us-east-2",
        "us-west-2",
        "eu-west-1",
        "eu-central-1",
    ]

    # Logging
    log_level: str = "INFO"

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"


# Global settings instance
settings = Settings()
