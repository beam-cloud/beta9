"""API middleware for logging, metrics, and error handling."""

import time
from typing import Callable

from fastapi import Request, Response
from prometheus_client import Counter, Histogram
import structlog


# Metrics
REQUEST_COUNT = Counter(
    "api_requests_total",
    "Total API requests",
    ["method", "endpoint", "status"],
)

REQUEST_DURATION = Histogram(
    "api_request_duration_seconds",
    "API request duration",
    ["method", "endpoint"],
)


def setup_logging(log_level: str) -> None:
    """
    Configure structured logging.

    Args:
        log_level: Log level (DEBUG, INFO, WARNING, ERROR)
    """
    structlog.configure(
        processors=[
            structlog.stdlib.filter_by_level,
            structlog.stdlib.add_logger_name,
            structlog.stdlib.add_log_level,
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.processors.StackInfoRenderer(),
            structlog.processors.format_exc_info,
            structlog.processors.UnicodeDecoder(),
            structlog.processors.JSONRenderer(),
        ],
        context_class=dict,
        logger_factory=structlog.stdlib.LoggerFactory(),
        cache_logger_on_first_use=True,
    )


async def logging_middleware(request: Request, call_next: Callable) -> Response:
    """
    Logging middleware.

    Logs all requests with correlation ID and timing.
    """
    logger = structlog.get_logger()

    # Generate correlation ID
    correlation_id = request.headers.get("X-Correlation-ID", f"req-{time.time()}")

    # Bind correlation ID to logger
    logger = logger.bind(correlation_id=correlation_id)

    # Log request
    logger.info(
        "Incoming request",
        method=request.method,
        path=request.url.path,
        client=request.client.host if request.client else None,
    )

    # Process request
    start_time = time.time()
    try:
        response = await call_next(request)

        # Log response
        duration = time.time() - start_time
        logger.info(
            "Request completed",
            method=request.method,
            path=request.url.path,
            status_code=response.status_code,
            duration_ms=f"{duration * 1000:.2f}",
        )

        # Add correlation ID to response headers
        response.headers["X-Correlation-ID"] = correlation_id

        return response

    except Exception as e:
        duration = time.time() - start_time
        logger.error(
            "Request failed",
            method=request.method,
            path=request.url.path,
            duration_ms=f"{duration * 1000:.2f}",
            error=str(e),
            exc_info=True,
        )
        raise


async def metrics_middleware(request: Request, call_next: Callable) -> Response:
    """
    Metrics middleware.

    Records Prometheus metrics for all requests.
    """
    start_time = time.time()

    try:
        response = await call_next(request)

        # Record metrics
        REQUEST_COUNT.labels(
            method=request.method,
            endpoint=request.url.path,
            status=response.status_code,
        ).inc()

        REQUEST_DURATION.labels(
            method=request.method,
            endpoint=request.url.path,
        ).observe(time.time() - start_time)

        return response

    except Exception as e:
        # Record error metrics
        REQUEST_COUNT.labels(
            method=request.method,
            endpoint=request.url.path,
            status=500,
        ).inc()

        raise
