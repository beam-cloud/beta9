"""FastAPI application for Beta9 provisioning API."""

from contextlib import asynccontextmanager
from typing import AsyncIterator

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
import structlog

from models.database import init_db
from .config import settings
from .routes import router
from .middleware import setup_logging, logging_middleware, metrics_middleware


# Configure structured logging
setup_logging(settings.log_level)
logger = structlog.get_logger()


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    """
    Application lifespan manager.

    Handles startup and shutdown tasks.
    """
    # Startup
    logger.info("Starting Beta9 provisioning API", version="0.1.0")

    # Initialize database
    try:
        init_db(settings.database_url)
        logger.info("Database initialized", url=settings.database_url)
    except Exception as e:
        logger.error("Failed to initialize database", error=str(e))
        raise

    # TODO: Initialize Redis connection pool

    yield

    # Shutdown
    logger.info("Shutting down Beta9 provisioning API")


# Create FastAPI app
app = FastAPI(
    title="Beta9 Provisioning API",
    description="Provision and manage Beta9 cloud environments",
    version="0.1.0",
    lifespan=lifespan,
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # TODO: Configure properly in production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Add custom middleware
app.middleware("http")(logging_middleware)
app.middleware("http")(metrics_middleware)

# Include routers
app.include_router(router, prefix="/api/v1")


# Health check endpoint
@app.get("/health")
async def health_check():
    """Health check endpoint."""
    return {"status": "healthy", "service": "beta9-provisioning"}


# Error handlers
@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception):
    """Global exception handler."""
    logger.error(
        "Unhandled exception",
        path=request.url.path,
        method=request.method,
        error=str(exc),
        exc_info=True,
    )

    return JSONResponse(
        status_code=500,
        content={
            "error": "Internal server error",
            "message": "An unexpected error occurred",
        },
    )


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(
        "api.main:app",
        host=settings.api_host,
        port=settings.api_port,
        workers=settings.api_workers,
        log_level=settings.log_level.lower(),
    )
