"""Declarative defaults for known application images."""

from typing import Any, Dict

KNOWN_IMAGES: Dict[str, Dict[str, Any]] = {
    "n8n": {"health": ("/healthz", 5678)},
    "docmost": {"health": ("/api/health", 3000)},
    "plausible/community-edition": {"health": ("/api/health", 8000)},
    "minio": {"health": ("/minio/health/live", 9000)},
    "clickhouse-server": {"health": ("/ping", 8123), "cpu": 2, "memory": "4Gi"},
    "grafana": {"health": ("/api/health", 3000)},
    "prometheus": {"health": ("/-/ready", 9090)},
    "qdrant": {"health": ("/healthz", 6333)},
    "meilisearch": {"health": ("/health", 7700)},
    "typesense": {"health": ("/health", 8108)},
    "elasticsearch": {"cpu": 2, "memory": "4Gi"},
    "opensearch": {"cpu": 2, "memory": "4Gi"},
    "langfuse": {
        "health": ("/api/public/health", 3000),
        "redis_server_name": "REDIS_TLS_SERVERNAME",
        "tls_flags": ["CLICKHOUSE_MIGRATION_SSL"],
    },
    "langfuse-worker": {
        "health": ("/api/health", 3030),
        "redis_server_name": "REDIS_TLS_SERVERNAME",
        "tls_flags": ["CLICKHOUSE_MIGRATION_SSL"],
    },
}
