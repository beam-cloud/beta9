.PHONY: help install test lint format clean docker-build docker-push

help: ## Show this help message
	@echo 'Usage: make [target]'
	@echo ''
	@echo 'Available targets:'
	@awk 'BEGIN {FS = ":.*?## "} /^[a-zA-Z_-]+:.*?## / {printf "  %-20s %s\n", $$1, $$2}' $(MAKEFILE_LIST)

install: ## Install dependencies
	pip install -r requirements.txt

install-dev: ## Install development dependencies
	pip install -r requirements.txt
	pip install pytest pytest-asyncio pytest-cov black ruff mypy

test: ## Run unit tests
	pytest tests/unit/ -v

test-integration: ## Run integration tests (WARNING: provisions real infrastructure!)
	BETA9_INTEGRATION_TESTS=1 pytest tests/integration/ -v -s

test-e2e: ## Run end-to-end tests (WARNING: provisions real infrastructure!)
	BETA9_E2E_TESTS=1 pytest tests/e2e/ -v -s

test-all: ## Run all tests
	pytest tests/ -v

test-cov: ## Run tests with coverage
	pytest tests/ --cov=. --cov-report=html --cov-report=term

lint: ## Run linters
	ruff check .
	mypy models/ engine/ api/ worker/

format: ## Format code
	black .
	ruff check --fix .

clean: ## Clean up generated files
	find . -type d -name __pycache__ -exec rm -rf {} +
	find . -type d -name .pytest_cache -exec rm -rf {} +
	find . -type d -name .mypy_cache -exec rm -rf {} +
	find . -type d -name .ruff_cache -exec rm -rf {} +
	find . -type f -name '*.pyc' -delete
	rm -rf htmlcov/
	rm -rf .coverage

docker-build: ## Build Docker images
	docker build -f Dockerfile.api -t beta9-provisioning-api:latest .
	docker build -f Dockerfile.worker -t beta9-provisioning-worker:latest .

docker-push: ## Push Docker images (requires DOCKER_REGISTRY env var)
	@if [ -z "$(DOCKER_REGISTRY)" ]; then \
		echo "Error: DOCKER_REGISTRY not set"; \
		exit 1; \
	fi
	docker tag beta9-provisioning-api:latest $(DOCKER_REGISTRY)/beta9-provisioning-api:latest
	docker tag beta9-provisioning-worker:latest $(DOCKER_REGISTRY)/beta9-provisioning-worker:latest
	docker push $(DOCKER_REGISTRY)/beta9-provisioning-api:latest
	docker push $(DOCKER_REGISTRY)/beta9-provisioning-worker:latest

run-api: ## Run API locally
	uvicorn api.main:app --reload --port 8000

run-worker: ## Run worker locally
	python -m worker.provisioner

db-init: ## Initialize database
	python -c "from models.database import init_db; from api.config import settings; init_db(settings.database_url)"

# Development database (PostgreSQL in Docker)
dev-db-up: ## Start development PostgreSQL database
	docker run -d \
		--name beta9-provisioning-db \
		-e POSTGRES_PASSWORD=postgres \
		-e POSTGRES_DB=provisioning \
		-p 5432:5432 \
		postgres:15

dev-db-down: ## Stop development database
	docker stop beta9-provisioning-db
	docker rm beta9-provisioning-db

# Development Redis
dev-redis-up: ## Start development Redis
	docker run -d \
		--name beta9-provisioning-redis \
		-p 6379:6379 \
		redis:7

dev-redis-down: ## Stop development Redis
	docker stop beta9-provisioning-redis
	docker rm beta9-provisioning-redis

dev-up: dev-db-up dev-redis-up ## Start all development services
	@echo "Development services started"
	@echo "PostgreSQL: postgresql://postgres:postgres@localhost:5432/provisioning"
	@echo "Redis: redis://localhost:6379/0"

dev-down: dev-db-down dev-redis-down ## Stop all development services
	@echo "Development services stopped"
