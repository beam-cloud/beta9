# Beta9 Provisioning API

A production-grade provisioning API that uses CDKTF (Cloud Development Kit for Terraform) to provision and manage Beta9 cloud environments in customer AWS/GCP accounts.

## Architecture

```
┌─────────────────┐
│   API Service   │ ← FastAPI REST/gRPC endpoints
│  (POST/GET/DEL) │
└────────┬────────┘
         │
         ↓
┌─────────────────┐
│  Control DB     │ ← Environment state tracking
│  (Postgres)     │
└────────┬────────┘
         │
         ↓
┌─────────────────┐
│ Background      │ ← Async provisioning/destruction
│ Worker (Redis)  │
└────────┬────────┘
         │
         ↓
┌─────────────────┐
│ CDKTF Engine    │ ← Infrastructure synthesis
│ (Python/TF)     │
└────────┬────────┘
         │
         ↓
┌─────────────────┐
│ User's Cloud    │ ← VPC, K8s, RDS, Redis, S3
│ (AWS/GCP)       │
└─────────────────┘
```

## Components

### 1. Domain Models (`models/`)
- `config.py`: Meta-config schema and InfraModel
- `environment.py`: Environment entity for persistence
- `schemas.py`: API request/response schemas

### 2. CDKTF Engine (`engine/`)
- `aws_stack.py`: AWS infrastructure stack (VPC, EKS, RDS, Redis, S3)
- `gcp_stack.py`: GCP infrastructure stack (VPC, GKE, Cloud SQL, etc.)
- `executor.py`: CDKTF wrapper (synth, apply, destroy)
- `state.py`: State backend configuration

### 3. API Service (`api/`)
- `main.py`: FastAPI application
- `routes.py`: Environment lifecycle endpoints
- `deps.py`: Dependencies (DB, auth, etc.)
- `middleware.py`: Logging, metrics, error handling

### 4. Background Worker (`worker/`)
- `provisioner.py`: Provision orchestration
- `bootstrap.py`: Cluster bootstrap (kubeconfig, Helm)
- `destroyer.py`: Infrastructure teardown

### 5. Tests (`tests/`)
- `unit/`: Unit tests for models and helpers
- `integration/`: Infrastructure engine tests
- `e2e/`: Full API workflow tests

## API Endpoints

### POST /environments
Create a new environment.

**Request:**
```json
{
  "name": "prod-us-east",
  "cloud": {
    "provider": "aws",
    "region": "us-east-1",
    "account_id": "123456789012",
    "role_arn": "arn:aws:iam::123456789012:role/Beta9Provisioner"
  },
  "cluster": {
    "profile": "small",
    "use_existing": false
  },
  "data": {
    "postgres": {"size": "small", "multi_az": true},
    "redis": {"size": "small", "ha": true},
    "object_store": {"bucket_prefix": "beta9"}
  },
  "beta9": {
    "domain": "beta9.example.com",
    "workers": {"gpu_enabled": false}
  }
}
```

**Response:**
```json
{
  "environment_id": "env_abc123",
  "status": "provisioning",
  "created_at": "2025-11-17T10:30:00Z"
}
```

### GET /environments/{id}
Get environment status and outputs.

**Response:**
```json
{
  "environment_id": "env_abc123",
  "status": "ready",
  "outputs": {
    "cluster_endpoint": "https://abc123.eks.us-east-1.amazonaws.com",
    "beta9_endpoint": "https://beta9.example.com",
    "rds_endpoint": "beta9-db.abc123.us-east-1.rds.amazonaws.com",
    "redis_endpoint": "beta9-redis.abc123.use1.cache.amazonaws.com",
    "bucket_name": "beta9-prod-us-east-abc123"
  },
  "created_at": "2025-11-17T10:30:00Z",
  "updated_at": "2025-11-17T10:45:00Z"
}
```

### DELETE /environments/{id}
Destroy an environment.

**Response:**
```json
{
  "environment_id": "env_abc123",
  "status": "destroying"
}
```

## Environment Lifecycle

```
queued → provisioning → bootstrapping → ready
                ↓              ↓
              failed        failed
  
ready → destroy_queued → destroying → destroyed
                ↓
              failed
```

## State Management

Each environment maintains its own Terraform state:
- **AWS**: S3 bucket + DynamoDB table in user account
- **GCP**: GCS bucket in user project
- **Path**: `environments/{environment_id}/terraform.tfstate`

## Credential Model

### AWS
- User provides `role_arn` that the provisioning service assumes via STS
- Role must have permissions for EC2, EKS, RDS, ElastiCache, S3
- External ID for additional security

### GCP
- Workload Identity or Service Account impersonation
- User provides service account email with required IAM roles

## Running Locally

### Prerequisites
```bash
# Install Python dependencies
pip install -r requirements.txt

# Install CDKTF CLI
npm install -g cdktf-cli

# Install Terraform
# (follow https://developer.hashicorp.com/terraform/install)
```

### Start Services
```bash
# Start API
uvicorn api.main:app --reload --port 8000

# Start worker
python worker/provisioner.py
```

### Environment Variables
```bash
# Database
DATABASE_URL=postgresql://user:pass@localhost:5432/provisioning

# Redis
REDIS_URL=redis://localhost:6379/0

# AWS (for assuming roles)
AWS_REGION=us-east-1

# Logging
LOG_LEVEL=INFO
```

## Testing

```bash
# Unit tests
pytest tests/unit/

# Integration tests (requires AWS/GCP credentials)
pytest tests/integration/

# E2E tests (provisions real infrastructure!)
pytest tests/e2e/ -m "slow"
```

## Deployment

### Docker
```bash
docker build -t beta9-provisioning-api -f Dockerfile.api .
docker build -t beta9-provisioning-worker -f Dockerfile.worker .
```

### Kubernetes
```bash
kubectl apply -f manifests/provisioning/
```

## Security Considerations

1. **Never store cloud credentials** - only role ARNs/service accounts
2. **Use short-lived tokens** - STS sessions expire in 1 hour
3. **Isolate workdirs** - each environment gets its own temp directory
4. **Audit logging** - all provisioning actions are logged with correlation IDs
5. **Rate limiting** - max environments per user/org
6. **Resource tagging** - all resources tagged with environment_id for tracking

## Monitoring

### Metrics (Prometheus)
- `provisioning_environments_total{status}` - Counter of environments by status
- `provisioning_duration_seconds` - Histogram of provisioning time
- `provisioning_errors_total{cloud,region}` - Counter of failures

### Logs (Structured)
All logs include:
- `environment_id`: Correlation ID
- `cloud`, `region`: Cloud context
- `operation`: Current operation (provision, destroy, bootstrap)
- `status`: Current status

## Troubleshooting

### Environment stuck in "provisioning"
1. Check worker logs: `kubectl logs -l app=provisioning-worker`
2. Check Terraform state: `terraform show` in workdir
3. Check cloud console for partial resources

### Partial destroy
1. Manual cleanup via cloud console
2. Import remaining resources: `terraform import`
3. Re-run destroy

### Bootstrap failures
1. Check kubeconfig generation
2. Verify IAM auth plugin
3. Check Helm release status: `helm list -A`

## License

Same as parent Beta9 project.
