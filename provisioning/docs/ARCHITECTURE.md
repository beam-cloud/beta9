# Beta9 Provisioning API - Architecture

## Overview

The Beta9 Provisioning API is a production-grade system that automates the deployment of Beta9 cloud environments into customer AWS/GCP accounts. It uses CDKTF (Cloud Development Kit for Terraform) as the infrastructure engine and provides a REST API for lifecycle management.

## High-Level Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                         User/Client                          │
└────────────────────┬────────────────────────────────────────┘
                     │ HTTP/REST API
                     ↓
┌─────────────────────────────────────────────────────────────┐
│                    FastAPI Application                       │
│  ┌─────────────┐  ┌──────────────┐  ┌──────────────┐       │
│  │   Routes    │→ │  Validation  │→ │   Database   │       │
│  └─────────────┘  └──────────────┘  └──────────────┘       │
│                           ↓                                  │
│                    ┌──────────────┐                          │
│                    │  Job Queue   │                          │
│                    │   (Redis)    │                          │
│                    └──────────────┘                          │
└─────────────────────────────────────────────────────────────┘
                           ↓
┌─────────────────────────────────────────────────────────────┐
│                   Background Worker(s)                       │
│  ┌──────────────────────────────────────────────────────┐   │
│  │  1. Poll Jobs from Queue                             │   │
│  │  2. Load Environment Config                          │   │
│  │  3. Generate InfraModel                              │   │
│  │  4. Execute CDKTF (synth → terraform apply)          │   │
│  │  5. Bootstrap Cluster (kubeconfig → helm install)    │   │
│  │  6. Update Environment Status                        │   │
│  └──────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────┘
                           ↓
┌─────────────────────────────────────────────────────────────┐
│                    CDKTF Engine (Python)                     │
│  ┌────────────┐  ┌────────────┐  ┌────────────┐            │
│  │ AWS Stack  │  │ GCP Stack  │  │  Executor  │            │
│  │ (VPC, EKS, │  │ (VPC, GKE, │  │  (synth,   │            │
│  │  RDS, S3)  │  │ Cloud SQL) │  │   apply)   │            │
│  └────────────┘  └────────────┘  └────────────┘            │
└─────────────────────────────────────────────────────────────┘
                           ↓
┌─────────────────────────────────────────────────────────────┐
│                   Terraform (via CDK)                        │
│  ┌──────────────────────────────────────────────────────┐   │
│  │  • Synthesize HCL JSON                               │   │
│  │  • Plan infrastructure changes                       │   │
│  │  • Apply resources to cloud                          │   │
│  │  • Store state in S3/GCS                             │   │
│  └──────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────┘
                           ↓
┌─────────────────────────────────────────────────────────────┐
│                User's Cloud Account (AWS/GCP)                │
│  ┌────────┐  ┌────────┐  ┌──────┐  ┌───────┐  ┌──────┐    │
│  │  VPC   │  │  K8s   │  │  DB  │  │ Cache │  │ S3/  │    │
│  │        │  │ (EKS/  │  │(RDS/ │  │(Redis/│  │ GCS  │    │
│  │        │  │  GKE)  │  │Cloud │  │Memcached│  │      │    │
│  │        │  │        │  │ SQL) │  │       │  │      │    │
│  └────────┘  └────────┘  └──────┘  └───────┘  └──────┘    │
│                     ↓                                        │
│              ┌─────────────┐                                │
│              │ Beta9 Pods  │                                │
│              │ (Gateway,   │                                │
│              │  Workers)   │                                │
│              └─────────────┘                                │
└─────────────────────────────────────────────────────────────┘
```

## Core Components

### 1. API Service (`api/`)

**Purpose**: HTTP REST API for environment lifecycle management.

**Key Files**:
- `main.py`: FastAPI application with lifespan management
- `routes.py`: Environment CRUD endpoints
- `middleware.py`: Logging, metrics, error handling
- `queue.py`: Redis queue interface
- `config.py`: Application settings

**Responsibilities**:
- Accept and validate user meta-config
- Persist environment records in control database
- Enqueue provisioning/destroy jobs
- Return environment status and outputs

**Endpoints**:
- `POST /api/v1/environments` - Create environment
- `GET /api/v1/environments/{id}` - Get environment status
- `DELETE /api/v1/environments/{id}` - Destroy environment
- `GET /api/v1/environments` - List environments
- `GET /health` - Health check

### 2. Background Worker (`worker/`)

**Purpose**: Async processing of provisioning and destruction jobs.

**Key Files**:
- `provisioner.py`: Main worker loop
- `bootstrap.py`: Cluster bootstrap logic

**Workflow**:

#### Provisioning:
1. Dequeue job from Redis
2. Load environment record from database
3. Parse user's meta-config
4. Convert to InfraModel
5. Ensure state backend exists (S3/DynamoDB or GCS)
6. Synthesize CDKTF to Terraform JSON
7. Run `terraform apply`
8. Capture outputs (cluster endpoint, DB endpoint, etc.)
9. Generate kubeconfig
10. Install baseline charts (nginx-ingress, cert-manager)
11. Install Beta9 Helm charts
12. Update environment status to "ready"

#### Destruction:
1. Dequeue destroy job
2. Load environment record
3. Synthesize CDKTF (to get current state)
4. Run `terraform destroy`
5. Update status to "destroyed"

### 3. CDKTF Engine (`engine/`)

**Purpose**: Infrastructure-as-code engine using CDKTF and Terraform.

**Key Files**:
- `aws_stack.py`: AWS infrastructure stack (VPC, EKS, RDS, Redis, S3)
- `gcp_stack.py`: GCP infrastructure stack (stub for future)
- `executor.py`: CDKTF execution wrapper (synth, apply, destroy)
- `state.py`: State backend management

**AWS Stack Resources**:
- **VPC**: Multi-AZ VPC with public/private subnets
- **EKS**: Managed Kubernetes cluster with node groups
- **RDS**: PostgreSQL database with encryption and backups
- **ElastiCache**: Redis cluster for caching
- **S3**: Object storage bucket with versioning and lifecycle
- **IAM**: Roles and policies for EKS and nodes
- **Security Groups**: Network isolation

**Outputs**:
- `vpc_id`, `cluster_name`, `cluster_endpoint`
- `rds_endpoint`, `rds_master_user_secret_arn`
- `redis_endpoint`, `redis_port`
- `bucket_name`, `bucket_arn`

### 4. Domain Models (`models/`)

**Purpose**: Data models and schemas for configuration and persistence.

**Key Files**:
- `config.py`: Meta-config schema, InfraModel, conversion logic
- `environment.py`: Environment entity and status enum
- `database.py`: Database connection management

**Key Concepts**:

#### MetaConfig
High-level user configuration (YAML/JSON) with simple, intuitive fields:
- Cloud provider and credentials
- Cluster size profile (small/medium/large)
- Database and cache sizes
- Beta9-specific settings

#### InfraModel
Internal, normalized infrastructure model consumed by CDKTF stacks:
- Cloud-agnostic where possible
- Contains computed values (instance types, node counts, etc.)
- Includes state backend configuration

#### Conversion Pipeline
```
MetaConfig (user input)
    ↓ [from_meta_config()]
InfraModel (normalized)
    ↓ [Beta9AwsStack]
CDKTF Constructs
    ↓ [app.synth()]
Terraform JSON
    ↓ [terraform apply]
Infrastructure
```

## Data Flow

### Environment Creation

```
1. User → POST /environments
2. API validates meta-config
3. API creates Environment record (status: queued)
4. API enqueues provisioning job
5. API returns environment_id
6. Worker picks up job
7. Worker updates status → provisioning
8. Worker runs CDKTF/Terraform
9. Worker captures outputs
10. Worker updates status → bootstrapping
11. Worker generates kubeconfig
12. Worker installs Beta9
13. Worker updates status → ready
14. Worker stores outputs
```

### Environment Destruction

```
1. User → DELETE /environments/{id}
2. API updates status → destroy_queued
3. API enqueues destroy job
4. Worker picks up job
5. Worker updates status → destroying
6. Worker runs terraform destroy
7. Worker updates status → destroyed
8. Worker clears outputs
```

### Polling Pattern

```
User Loop:
  while status not in [ready, failed]:
    GET /environments/{id}
    sleep(10)
```

## State Management

### Terraform State

Each environment maintains its own Terraform state:

**AWS**:
- S3 bucket: `beta9-tf-state-{account_id}-{region}`
- Object key: `environments/{environment_id}/terraform.tfstate`
- DynamoDB table: `beta9-tf-state-{account_id}-{region}-lock`
- State locking prevents concurrent modifications

**GCP**:
- GCS bucket: `beta9-tf-state-{project_id}`
- Prefix: `environments/{environment_id}/`

### Environment State

Stored in control plane PostgreSQL database:

```sql
environments (
  id VARCHAR PRIMARY KEY,
  name VARCHAR,
  user_id VARCHAR,
  cloud_provider VARCHAR,
  region VARCHAR,
  account_id VARCHAR,
  meta_config JSON,
  infra_model JSON,
  state_backend_config JSON,
  status ENUM,
  outputs JSON,
  error_message TEXT,
  created_at TIMESTAMP,
  updated_at TIMESTAMP,
  provisioned_at TIMESTAMP,
  destroyed_at TIMESTAMP
)
```

### Status Lifecycle

```
queued
  ↓
provisioning
  ↓
bootstrapping
  ↓
ready ←─────────────┐
  │                 │
  ↓                 │
destroy_queued      │ (refresh)
  ↓                 │
destroying          │
  ↓                 │
destroyed ──────────┘

        ↓ (any state)
      failed
```

## Security Model

### Cloud Credentials

**AWS**:
- User creates IAM role with `Beta9Provisioner` name
- Role has permissions for EC2, EKS, RDS, ElastiCache, S3, IAM
- User provides `role_arn` and optional `external_id`
- Provisioning service assumes role via STS
- Short-lived credentials (1 hour session)

**GCP**:
- User creates service account with required IAM roles
- User provides `service_account` email
- Provisioning service uses workload identity or impersonation

### Secrets Management

- **Database passwords**: Stored in AWS Secrets Manager (auto-managed by RDS)
- **API keys**: Not stored; passed as environment variables
- **Kubeconfigs**: Temporary files, deleted after bootstrap
- **Terraform state**: Encrypted at rest in S3/GCS

### Network Security

- **Private subnets**: EKS nodes, RDS, Redis in private subnets
- **Security groups**: Least-privilege network rules
- **NAT gateways**: Outbound internet access for private subnets
- **VPC peering**: Optional for connecting to existing VPCs

## Observability

### Logging

Structured JSON logs with:
- `correlation_id`: Request correlation
- `environment_id`: Environment context
- `operation`: Current operation (provision, destroy, bootstrap)
- `status`: Current status
- `error`: Error messages
- `duration_ms`: Operation timing

### Metrics (Prometheus)

- `provisioning_environments_total{status}`: Counter by status
- `provisioning_duration_seconds`: Histogram of provisioning time
- `provisioning_errors_total{cloud, region}`: Error counter
- `api_requests_total{method, endpoint, status}`: API request counter
- `api_request_duration_seconds{method, endpoint}`: API latency

### Tracing

- Correlation IDs propagate through entire request lifecycle
- Logs are queryable by `environment_id` or `correlation_id`

## Scalability

### API Service
- Stateless: can scale horizontally
- Uses Redis for job queue (shared state)
- Database connection pooling

### Background Workers
- Stateless: can scale horizontally
- Pull model from Redis queue
- Each worker processes one job at a time
- Add workers to increase throughput

### Bottlenecks
- Terraform execution is single-threaded per environment
- Large infrastructure takes 15-30 minutes to provision
- Database could become bottleneck at high scale (add read replicas)

## Failure Modes

### Partial Provisioning

**Scenario**: Terraform apply succeeds for VPC/EKS but fails for RDS.

**Recovery**:
1. Environment status set to "failed"
2. Error message stored in database
3. User can:
   - Retry provisioning (Terraform will continue from last state)
   - Delete environment (Terraform will destroy partial resources)

### Bootstrap Failure

**Scenario**: Infrastructure provisioned but Beta9 installation fails.

**Recovery**:
1. Environment status set to "failed" in "bootstrapping" phase
2. Infrastructure still exists (can be accessed via kubeconfig)
3. User can:
   - Manually install Beta9
   - Delete environment and retry

### Worker Crash

**Scenario**: Worker crashes mid-provisioning.

**Recovery**:
1. Job remains in Redis queue or is requeued
2. Another worker picks up job
3. Terraform state ensures idempotency
4. If environment stuck in "provisioning" for >1 hour, alert operator

### State Lock Timeout

**Scenario**: Terraform state is locked (previous operation didn't clean up).

**Recovery**:
1. Check DynamoDB table for lock entry
2. If lock is stale (>1 hour), force-unlock:
   ```bash
   terraform force-unlock <lock-id>
   ```

## Multi-Cloud Strategy

### Current Implementation
- AWS fully implemented
- GCP stub in place

### Adding GCP
1. Implement `gcp_stack.py` with GCP constructs
2. Implement `_generate_gke_kubeconfig()` in bootstrap
3. Update `_ensure_gcp_state_backend()` if needed
4. Add GCP-specific tests

### Cloud Abstraction
- `InfraModel` is cloud-agnostic
- Stack selection based on `cloud_provider` enum
- Bootstrap logic has cloud-specific branches

## Testing Strategy

### Unit Tests
- Domain model validation
- Meta-config → InfraModel conversion
- Size/profile mappings

### Integration Tests
- Provision real infrastructure in sandbox account
- Verify outputs
- Destroy infrastructure
- **WARNING**: Expensive, run sparingly

### E2E Tests
- Full API workflow
- Create → wait for ready → verify → destroy
- **WARNING**: Very expensive, run before releases

## Deployment

### Kubernetes
- Deploy API and worker as separate Deployments
- Use IRSA (IAM Roles for Service Accounts) for AWS credentials
- Mount persistent volume for working directory
- Use HPA (Horizontal Pod Autoscaler) for API
- Scale workers based on queue depth

### Docker Compose (Development)
```yaml
services:
  postgres:
    image: postgres:15
  redis:
    image: redis:7
  api:
    build: .
    dockerfile: Dockerfile.api
  worker:
    build: .
    dockerfile: Dockerfile.worker
```

## Future Enhancements

1. **Multi-tenancy**: User authentication and authorization
2. **Cost estimation**: Predict infrastructure costs before provisioning
3. **Drift detection**: Detect manual changes to infrastructure
4. **Auto-scaling**: Scale infrastructure based on Beta9 usage
5. **Backup/restore**: Snapshot and restore environments
6. **Blue/green deployments**: Zero-downtime Beta9 upgrades
7. **Custom VPC**: Support for bring-your-own VPC
8. **Multi-region**: Distribute Beta9 across multiple regions
9. **Disaster recovery**: Automated failover and recovery
10. **Compliance**: SOC2, HIPAA-compliant configurations
