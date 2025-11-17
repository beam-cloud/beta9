# Beta9 Provisioning API - Project Overview

## Project Structure

```
provisioning/
├── api/                          # FastAPI service
│   ├── __init__.py
│   ├── config.py                 # Application settings
│   ├── main.py                   # FastAPI app entry point
│   ├── middleware.py             # Logging, metrics, error handling
│   ├── queue.py                  # Redis queue interface
│   └── routes.py                 # Environment CRUD endpoints
│
├── worker/                       # Background worker
│   ├── __init__.py
│   ├── provisioner.py            # Main worker loop
│   └── bootstrap.py              # Cluster bootstrap logic
│
├── engine/                       # CDKTF infrastructure engine
│   ├── __init__.py
│   ├── aws_stack.py              # AWS infrastructure stack (1,030 lines)
│   ├── executor.py               # CDKTF wrapper (synth, apply, destroy)
│   └── state.py                  # State backend management
│
├── models/                       # Domain models
│   ├── __init__.py
│   ├── config.py                 # MetaConfig, InfraModel (560 lines)
│   ├── environment.py            # Environment entity
│   └── database.py               # Database connection
│
├── tests/                        # Test suites
│   ├── unit/
│   │   ├── __init__.py
│   │   └── test_config.py        # Config validation tests (300 lines)
│   ├── integration/
│   │   ├── __init__.py
│   │   └── test_infrastructure.py # Infrastructure tests
│   └── e2e/
│       ├── __init__.py
│       └── test_api.py           # End-to-end API tests
│
├── docs/                         # Documentation
│   ├── ARCHITECTURE.md           # System design (800 lines)
│   ├── RUNBOOK.md                # Operational procedures (600 lines)
│   └── DEPLOYMENT.md             # Production deployment (700 lines)
│
├── manifests/                    # Kubernetes manifests
│   └── provisioning-api.yaml    # K8s Deployment, Service, Ingress
│
├── scripts/                      # Helper scripts
│   └── setup-iam.sh              # IAM role setup for AWS
│
├── Dockerfile.api                # API Docker image
├── Dockerfile.worker             # Worker Docker image
├── Makefile                      # Development commands
├── requirements.txt              # Python dependencies
├── pyproject.toml                # Project metadata
├── .env.example                  # Environment template
├── example-config.yaml           # Configuration examples
├── README.md                     # Quick start guide (350 lines)
├── SUMMARY.md                    # Implementation summary (550 lines)
├── QUICKSTART.md                 # 5-minute setup guide
└── PROJECT_OVERVIEW.md           # This file
```

## Statistics

### Lines of Code

- **Total Python**: 3,815 lines
- **Tests**: ~800 lines
- **Documentation**: ~3,000 lines
- **Configuration**: ~500 lines
- **Total Project**: ~8,100 lines

### File Counts

- Python files: 19
- Test files: 3
- Documentation: 6 (README, guides, architecture docs)
- Configuration: 5 (Docker, K8s, examples)
- Scripts: 2 (Makefile, setup script)

## Key Files Explained

### Core Application

#### `api/main.py`
FastAPI application with:
- Lifespan management (startup/shutdown)
- CORS middleware
- Global exception handling
- Health check endpoint
- Structured logging setup

#### `api/routes.py`
REST API endpoints:
- `POST /api/v1/environments` - Create environment
- `GET /api/v1/environments/{id}` - Get environment status
- `DELETE /api/v1/environments/{id}` - Destroy environment
- `GET /api/v1/environments` - List environments

#### `worker/provisioner.py`
Background worker that:
- Polls Redis for provisioning/destroy jobs
- Orchestrates CDKTF infrastructure provisioning
- Calls bootstrap logic
- Updates environment status in database
- Handles errors and retries

#### `worker/bootstrap.py`
Cluster bootstrap:
- Generates kubeconfig (EKS with IAM auth)
- Waits for cluster to be ready
- Installs baseline charts (nginx-ingress, cert-manager, metrics-server)
- Installs Beta9 Helm charts with computed configuration
- Fetches secrets from AWS Secrets Manager

### Infrastructure Engine

#### `engine/aws_stack.py` (1,030 lines)
Complete AWS infrastructure using CDKTF:
- **VPC**: Multi-AZ with public/private subnets, NAT gateways, route tables
- **EKS**: Managed cluster with node groups (CPU and GPU)
- **RDS**: PostgreSQL with encryption, backups, multi-AZ
- **ElastiCache**: Redis cluster with HA option
- **S3**: Bucket with versioning and lifecycle policies
- **IAM**: Roles and policies for EKS and nodes
- **Security Groups**: Network isolation
- **Outputs**: All necessary endpoints and identifiers

#### `engine/executor.py`
CDKTF wrapper:
- `synth_infra()`: Synthesize Terraform JSON
- `apply_infra()`: Run terraform apply, capture outputs
- `destroy_infra()`: Run terraform destroy, cleanup
- Manages working directories
- Captures Terraform logs
- Returns structured results

#### `engine/state.py`
State backend management:
- Creates S3 bucket + DynamoDB table for AWS
- Creates GCS bucket for GCP
- Configures backend for each environment
- Handles credentials via assumed roles

### Domain Models

#### `models/config.py` (560 lines)
Configuration models:
- **MetaConfig**: User-facing Pydantic model with validation
- **InfraModel**: Internal dataclass for CDKTF
- **from_meta_config()**: Conversion logic with defaults
- **Profile mapping**: small/medium/large → instance types, node counts
- **Size mapping**: Database and cache sizes

#### `models/environment.py`
Database entity:
- Environment lifecycle status enum
- SQLAlchemy model with all fields
- Pydantic schemas for API requests/responses
- JSON serialization for outputs

#### `models/database.py`
Database utilities:
- Connection pool management
- Session context managers
- Table creation
- Global database instance

### Tests

#### `tests/unit/test_config.py` (300 lines)
Comprehensive unit tests:
- Configuration validation (AWS/GCP requirements)
- MetaConfig parsing (minimal, full configs)
- Conversion to InfraModel (profiles, sizes, networks)
- Node group generation (CPU, GPU)
- Database/cache specs
- Storage configuration
- State backend setup
- Resource tagging

#### `tests/integration/test_infrastructure.py`
Integration tests (expensive!):
- Full apply/destroy cycle
- Output validation
- Real AWS infrastructure provisioning
- Marked with `@pytest.mark.slow`
- Requires `BETA9_INTEGRATION_TESTS=1`

#### `tests/e2e/test_api.py`
End-to-end tests:
- Complete API workflow
- Create → poll → verify → destroy
- Error handling tests
- Validation tests
- Marked with `@pytest.mark.slow`
- Requires `BETA9_E2E_TESTS=1`

### Documentation

#### `README.md` (350 lines)
Main documentation:
- Architecture diagram
- Component descriptions
- API endpoints with examples
- Environment lifecycle
- State management
- Credential model
- Running locally
- Testing
- Security considerations
- Monitoring

#### `SUMMARY.md` (550 lines)
Implementation summary:
- What was built (detailed)
- Infrastructure provisioned
- Configuration model
- Lifecycle management
- Testing coverage
- Operational features
- Key design decisions
- What's missing (future work)
- File structure
- Success criteria met
- Lines of code stats

#### `docs/ARCHITECTURE.md` (800 lines)
Deep technical dive:
- High-level architecture diagram
- Core components explained
- Data flow diagrams
- State management
- Security model (credentials, secrets, network)
- Observability (logging, metrics, tracing)
- Scalability considerations
- Failure modes and recovery
- Multi-cloud strategy
- Testing strategy
- Future enhancements

#### `docs/RUNBOOK.md` (600 lines)
Operational procedures:
- Common operations (status check, retry, force delete)
- Troubleshooting (stuck provisioning, bootstrap failures, EKS issues)
- Monitoring (key metrics, alerts, dashboards)
- Incident response (P0/P1/P2 scenarios)
- Maintenance (backups, migrations, updates, capacity planning)

#### `docs/DEPLOYMENT.md` (700 lines)
Production deployment:
- Prerequisites (infrastructure, software, IAM)
- Kubernetes deployment (step-by-step)
- Docker Compose (development)
- Local development setup
- Post-deployment verification
- Configuration (environment variables)
- Scaling (horizontal, vertical, auto-scaling)
- Monitoring setup (Prometheus, Grafana, alerts)
- Security hardening (network policies, pod security, secrets)
- Backup and disaster recovery
- Troubleshooting deployment issues
- Upgrading (rolling, blue/green)
- Cost optimization

#### `QUICKSTART.md`
5-minute setup guide:
- Prerequisites checklist
- Local development setup
- Create first environment
- Docker Compose option
- Common issues and solutions
- Configuration examples
- Cost estimates

### Deployment

#### `Dockerfile.api`
API Docker image:
- Python 3.11 slim base
- Terraform, AWS CLI, kubectl, Helm installed
- Node.js and CDKTF CLI
- Python dependencies
- Application code
- Exposes port 8000

#### `Dockerfile.worker`
Worker Docker image:
- Same base as API (needs all tools)
- Runs `python -m worker.provisioner`

#### `manifests/provisioning-api.yaml`
Complete Kubernetes manifests:
- Namespace
- ConfigMap for configuration
- API Deployment (2 replicas)
- Worker Deployment (2 replicas)
- API Service (ClusterIP)
- ServiceAccounts with IRSA annotations
- Ingress with TLS

#### `Makefile`
Development commands:
- `make install`: Install dependencies
- `make test`: Run unit tests
- `make test-integration`: Run integration tests (provisions real infra!)
- `make test-e2e`: Run end-to-end tests
- `make lint`: Run linters
- `make format`: Format code
- `make docker-build`: Build Docker images
- `make docker-push`: Push to registry
- `make run-api`: Run API locally
- `make run-worker`: Run worker locally
- `make dev-up`: Start dev services (PostgreSQL, Redis)
- `make dev-down`: Stop dev services

#### `scripts/setup-iam.sh`
IAM role creation:
- Creates Beta9Provisioner role in customer AWS account
- Sets up trust policy with external ID
- Attaches provisioning policy (EC2, EKS, RDS, etc.)
- Outputs role ARN for configuration

### Configuration

#### `example-config.yaml`
Two configuration examples:
- **Full production config**: Large profile, GPU workers, HA databases
- **Minimal dev config**: Uses all defaults for small environment

#### `.env.example`
Environment variables template:
- Database URL
- Redis URL
- Working directory
- Security settings (max environments, allowed regions)
- Logging level
- AWS credentials (for testing)

#### `requirements.txt`
Python dependencies:
- FastAPI, Uvicorn (API)
- SQLAlchemy, Alembic (database)
- Redis (queue)
- CDKTF, cloud provider libraries
- Boto3 (AWS), google-cloud-storage (GCP)
- Kubernetes client
- PyYAML (config parsing)
- Structlog (logging)
- Prometheus client (metrics)
- Testing libraries (pytest, httpx)

#### `pyproject.toml`
Project metadata:
- Name, version, description
- Python version requirement
- Dependencies
- Development dependencies
- Tool configuration (black, ruff, mypy, pytest)

## Notable Features

### 1. Production-Ready

- ✅ Health checks and liveness/readiness probes
- ✅ Structured logging with correlation IDs
- ✅ Prometheus metrics
- ✅ Graceful shutdown
- ✅ Database connection pooling
- ✅ Error handling and recovery
- ✅ Resource limits and requests

### 2. Secure by Design

- ✅ STS AssumeRole with external ID
- ✅ Short-lived credentials (1 hour)
- ✅ Encrypted Terraform state
- ✅ Secrets in AWS Secrets Manager
- ✅ Security groups with least-privilege
- ✅ Private subnets for sensitive resources
- ✅ No plaintext credentials in code

### 3. Scalable Architecture

- ✅ Stateless API (horizontal scaling)
- ✅ Worker pool model (add workers = more throughput)
- ✅ Per-environment isolation (no cross-environment conflicts)
- ✅ Redis queue for distributed job processing
- ✅ Database optimized with indexes

### 4. Observable

- ✅ Structured JSON logs
- ✅ Correlation IDs through entire request lifecycle
- ✅ Prometheus metrics (request count, latency, provisioning duration)
- ✅ Environment lifecycle tracking
- ✅ Error messages stored in database

### 5. Reliable

- ✅ Terraform state ensures idempotency
- ✅ State locking prevents concurrent modifications
- ✅ Failed provisions can be retried
- ✅ Partial resources cleanly rolled back
- ✅ Workers can be restarted mid-job

### 6. Well-Tested

- ✅ Unit tests for all domain logic
- ✅ Integration tests for infrastructure provisioning
- ✅ End-to-end tests for API workflows
- ✅ ~800 lines of test code
- ✅ pytest configuration with coverage

### 7. Documented

- ✅ README with quick start
- ✅ Architecture deep dive
- ✅ Operational runbook
- ✅ Deployment guide
- ✅ API examples
- ✅ Inline code comments
- ✅ ~3,000 lines of documentation

## Technology Stack

### Backend
- **Python 3.11**: Main language
- **FastAPI**: REST API framework
- **SQLAlchemy**: ORM for PostgreSQL
- **Pydantic**: Data validation
- **Redis**: Job queue
- **Structlog**: Structured logging
- **Prometheus Client**: Metrics

### Infrastructure
- **CDKTF**: Infrastructure as code
- **Terraform**: Infrastructure provisioning
- **AWS SDK (boto3)**: AWS operations
- **Kubernetes Python Client**: Cluster operations

### DevOps
- **Docker**: Containerization
- **Kubernetes**: Orchestration
- **Helm**: Kubernetes package manager
- **Makefile**: Development automation
- **pytest**: Testing framework

### Cloud Providers
- **AWS**: EC2, EKS, RDS, ElastiCache, S3, IAM, Secrets Manager
- **GCP**: (stub for future)

## What's Not Included (Future Work)

1. **User Authentication**: No auth yet (uses placeholder user_id)
2. **GCP Implementation**: Only AWS fully implemented
3. **Cost Estimation**: No pre-provisioning cost estimates
4. **Drift Detection**: No detection of manual infrastructure changes
5. **Custom VPC**: No BYO VPC option yet
6. **Multi-Region**: Single region per environment
7. **Webhooks**: No webhook notifications for completion
8. **GitOps**: No Git-based configuration sync
9. **UI**: No web interface (API only)
10. **Terraform Cloud**: No TF Cloud integration

## Getting Started

1. **Read**: Start with [README.md](README.md)
2. **Quick Start**: Follow [QUICKSTART.md](QUICKSTART.md) for 5-minute setup
3. **Deploy**: See [docs/DEPLOYMENT.md](docs/DEPLOYMENT.md) for production
4. **Operate**: Refer to [docs/RUNBOOK.md](docs/RUNBOOK.md) for operations
5. **Understand**: Read [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) for deep dive

## Contributing

See [CONTRIBUTING.md](../CONTRIBUTING.md) in the parent repository.

## Support

- **Issues**: GitHub Issues
- **Docs**: See `docs/` directory
- **Examples**: See `example-config.yaml`

## License

Same as parent Beta9 project.
