# Beta9 Provisioning API - Quick Start Guide

## 5-Minute Local Setup

### Prerequisites

- Python 3.10+
- Docker and Docker Compose
- AWS account with credentials

### Step 1: Clone and Setup

```bash
cd provisioning

# Install dependencies
pip install -r requirements.txt

# Copy environment file
cp .env.example .env
```

### Step 2: Start Services

```bash
# Start PostgreSQL and Redis
make dev-up

# Initialize database
make db-init
```

### Step 3: Configure AWS

```bash
# Set up IAM role in your AWS account
./scripts/setup-iam.sh YOUR_ACCOUNT_ID my-external-id

# This creates the Beta9Provisioner role
# Copy the output role ARN for next step
```

### Step 4: Create Config

Create `my-environment.yaml`:

```yaml
name: dev-test
cloud:
  provider: aws
  region: us-east-1
  account_id: "YOUR_ACCOUNT_ID"
  role_arn: "arn:aws:iam::YOUR_ACCOUNT_ID:role/Beta9Provisioner"
  external_id: "my-external-id"
cluster:
  profile: small
beta9:
  domain: dev.beta9.example.com
```

### Step 5: Start API and Worker

```bash
# Terminal 1: Start API
make run-api

# Terminal 2: Start worker
make run-worker
```

### Step 6: Create Environment

```bash
# Create environment
curl -X POST http://localhost:8000/api/v1/environments \
  -H "Content-Type: application/json" \
  -d @my-environment.yaml

# Response: {"environment_id": "env_abc123", "status": "queued", ...}

# Check status
curl http://localhost:8000/api/v1/environments/env_abc123 | jq

# Watch logs
tail -f /tmp/beta9-provisioning/env_abc123/provision.log
```

### Step 7: Wait for Completion

```bash
# Poll until ready (20-30 minutes)
watch -n 10 'curl -s http://localhost:8000/api/v1/environments/env_abc123 | jq .status'

# When status is "ready", check outputs
curl http://localhost:8000/api/v1/environments/env_abc123 | jq .outputs
```

### Step 8: Access Your Beta9

```bash
# Get kubeconfig
aws eks update-kubeconfig \
  --name beta9-dev-test \
  --region us-east-1

# Check cluster
kubectl get nodes
kubectl get pods -n beta9-system

# Access Beta9 API (if DNS configured)
curl https://dev.beta9.example.com/health
```

### Step 9: Cleanup

```bash
# Delete environment
curl -X DELETE http://localhost:8000/api/v1/environments/env_abc123

# Wait for destroy (10-15 minutes)
watch -n 10 'curl -s http://localhost:8000/api/v1/environments/env_abc123 | jq .status'

# Stop services
make dev-down
```

## Docker Compose (All-in-One)

```bash
# Start everything
docker-compose up -d

# Initialize database
docker-compose exec api make db-init

# Check logs
docker-compose logs -f

# Create environment
curl -X POST http://localhost:8000/api/v1/environments \
  -H "Content-Type: application/json" \
  -d @my-environment.yaml

# Stop
docker-compose down
```

## Production Deployment

See [DEPLOYMENT.md](docs/DEPLOYMENT.md) for:
- Kubernetes deployment
- Monitoring setup
- Security hardening
- Auto-scaling
- High availability

## Common Issues

### "terraform: command not found"

```bash
# Install Terraform
wget https://releases.hashicorp.com/terraform/1.6.6/terraform_1.6.6_linux_amd64.zip
unzip terraform_1.6.6_linux_amd64.zip
sudo mv terraform /usr/local/bin/
```

### "aws: command not found"

```bash
# Install AWS CLI
curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip"
unzip awscliv2.zip
sudo ./aws/install
```

### "Cannot assume role"

Check that:
1. IAM role exists in your AWS account
2. Role ARN is correct in config
3. External ID matches (if used)
4. Trust policy allows your user/role to assume it

```bash
# Test assume role
aws sts assume-role \
  --role-arn arn:aws:iam::123456789012:role/Beta9Provisioner \
  --role-session-name test \
  --external-id my-external-id
```

### "Environment stuck in provisioning"

```bash
# Check worker logs
docker-compose logs worker

# Or for local:
# Check /tmp/beta9-provisioning/<env_id>/cdktf.out/stacks/beta9-aws/terraform.log

# Terraform apply takes 20-30 minutes for full infrastructure
# EKS cluster alone takes 15-20 minutes
```

### "Bootstrap failed"

```bash
# Get kubeconfig manually
aws eks update-kubeconfig --name <cluster-name> --region <region>

# Check cluster
kubectl get nodes

# If nodes not ready, check:
kubectl describe nodes

# Check Beta9 installation
helm list -n beta9-system

# Manually install if needed (see worker/bootstrap.py for exact commands)
```

## Configuration Examples

### Minimal (Dev)

```yaml
name: dev
cloud:
  provider: aws
  region: us-east-1
  account_id: "123456789012"
  role_arn: "arn:aws:iam::123456789012:role/Beta9Provisioner"
beta9:
  domain: dev.beta9.example.com
```

### Production (Large with GPU)

```yaml
name: production
cloud:
  provider: aws
  region: us-west-2
  account_id: "123456789012"
  role_arn: "arn:aws:iam::123456789012:role/Beta9Provisioner"
  external_id: "prod-external-id"
cluster:
  profile: large
  node_labels:
    environment: production
    team: platform
data:
  postgres:
    size: large
    multi_az: true
    backup_retention_days: 30
  redis:
    size: large
    ha: true
  object_store:
    bucket_prefix: beta9-prod
    versioning: true
    lifecycle_days: 180
beta9:
  domain: api.beta9.production.com
  version: v1.2.3
  workers:
    gpu_enabled: true
    gpu_type: nvidia-tesla-v100
    min_workers: 5
    max_workers: 50
  enable_metrics: true
  enable_logging: true
```

## Next Steps

1. **Read Architecture**: [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md)
2. **Deploy to Production**: [docs/DEPLOYMENT.md](docs/DEPLOYMENT.md)
3. **Operational Runbook**: [docs/RUNBOOK.md](docs/RUNBOOK.md)
4. **Run Tests**: `make test`
5. **Contribute**: See [CONTRIBUTING.md](../CONTRIBUTING.md)

## Support

- **Issues**: GitHub Issues
- **Docs**: See `docs/` directory
- **Examples**: See `example-config.yaml`
- **Logs**: Check worker logs for detailed provisioning steps

## Costs

Approximate AWS costs for a "small" environment:
- EKS cluster: ~$75/month (control plane)
- EC2 nodes (2x t3.large): ~$120/month
- RDS (db.t3.small): ~$30/month
- ElastiCache (cache.t3.small): ~$20/month
- S3: ~$1/month (minimal data)
- Data transfer: Variable

**Total**: ~$250/month for small dev environment

Production (large) environments: ~$1,500-2,000/month

**Note**: Costs vary by region and usage. Always use AWS Cost Calculator for accurate estimates.
