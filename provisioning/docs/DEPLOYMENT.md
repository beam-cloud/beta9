# Beta9 Provisioning API - Deployment Guide

## Prerequisites

### Infrastructure Requirements

- Kubernetes cluster (for running the provisioning service itself)
- PostgreSQL database (for control plane)
- Redis (for job queue)
- Container registry (Docker Hub, ECR, GCR, etc.)

### Software Requirements

- Docker
- kubectl
- Helm (optional, for managing deployments)
- AWS CLI (if provisioning AWS environments)
- GCP CLI (if provisioning GCP environments)

### IAM Setup

Before deploying, set up IAM roles in your "control" AWS account (the account where the provisioning service runs):

```bash
# Create role that the provisioning service will use to assume customer roles
aws iam create-role \
  --role-name Beta9ProvisioningService \
  --assume-role-policy-document file://trust-policy.json

# Attach policy allowing STS AssumeRole
aws iam put-role-policy \
  --role-name Beta9ProvisioningService \
  --policy-name AssumeCustomerRoles \
  --policy-document file://assume-role-policy.json
```

Example `assume-role-policy.json`:
```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": "sts:AssumeRole",
      "Resource": "arn:aws:iam::*:role/Beta9Provisioner"
    }
  ]
}
```

## Deployment Options

### Option 1: Kubernetes (Production)

#### Step 1: Build and Push Docker Images

```bash
# Build images
make docker-build

# Tag for your registry
export DOCKER_REGISTRY=your-registry.com/beta9
docker tag beta9-provisioning-api:latest $DOCKER_REGISTRY/provisioning-api:v0.1.0
docker tag beta9-provisioning-worker:latest $DOCKER_REGISTRY/provisioning-worker:v0.1.0

# Push to registry
docker push $DOCKER_REGISTRY/provisioning-api:v0.1.0
docker push $DOCKER_REGISTRY/provisioning-worker:v0.1.0
```

#### Step 2: Configure IRSA (IAM Roles for Service Accounts)

For AWS EKS, set up IRSA to allow pods to assume roles:

```bash
# Create OIDC provider for your cluster
eksctl utils associate-iam-oidc-provider \
  --cluster your-cluster-name \
  --approve

# Create IAM role for provisioning service
eksctl create iamserviceaccount \
  --name provisioning-api \
  --namespace beta9-provisioning \
  --cluster your-cluster-name \
  --attach-policy-arn arn:aws:iam::ACCOUNT_ID:policy/Beta9ProvisioningPolicy \
  --approve

eksctl create iamserviceaccount \
  --name provisioning-worker \
  --namespace beta9-provisioning \
  --cluster your-cluster-name \
  --attach-policy-arn arn:aws:iam::ACCOUNT_ID:policy/Beta9ProvisioningPolicy \
  --approve
```

#### Step 3: Deploy Database and Redis

**Option A: Use managed services (recommended)**

```bash
# AWS RDS for PostgreSQL
aws rds create-db-instance \
  --db-instance-identifier beta9-provisioning-db \
  --db-instance-class db.t3.small \
  --engine postgres \
  --master-username postgres \
  --master-user-password <password> \
  --allocated-storage 20

# AWS ElastiCache for Redis
aws elasticache create-cache-cluster \
  --cache-cluster-id beta9-provisioning-redis \
  --cache-node-type cache.t3.small \
  --engine redis \
  --num-cache-nodes 1
```

**Option B: Deploy in Kubernetes**

```bash
# PostgreSQL
helm install postgres bitnami/postgresql \
  --namespace beta9-provisioning \
  --set auth.database=provisioning \
  --set primary.persistence.size=10Gi

# Redis
helm install redis bitnami/redis \
  --namespace beta9-provisioning \
  --set architecture=standalone \
  --set auth.enabled=false
```

#### Step 4: Create ConfigMap and Secrets

```bash
# Create namespace
kubectl create namespace beta9-provisioning

# Create ConfigMap
kubectl create configmap provisioning-config \
  --namespace beta9-provisioning \
  --from-env-file=.env

# Create Secret for database URL (if using external DB)
kubectl create secret generic provisioning-secrets \
  --namespace beta9-provisioning \
  --from-literal=database-url='postgresql://user:pass@host:5432/provisioning'
```

#### Step 5: Deploy Manifests

```bash
# Update image references in manifests/provisioning-api.yaml
sed -i "s|beta9-provisioning-api:latest|$DOCKER_REGISTRY/provisioning-api:v0.1.0|g" \
  manifests/provisioning-api.yaml
sed -i "s|beta9-provisioning-worker:latest|$DOCKER_REGISTRY/provisioning-worker:v0.1.0|g" \
  manifests/provisioning-api.yaml

# Apply manifests
kubectl apply -f manifests/provisioning-api.yaml

# Check status
kubectl get pods -n beta9-provisioning
kubectl logs -n beta9-provisioning -l app=provisioning-api
kubectl logs -n beta9-provisioning -l app=provisioning-worker
```

#### Step 6: Initialize Database

```bash
# Run database migrations
kubectl exec -it -n beta9-provisioning <api-pod> -- \
  python -c "from models.database import init_db; from api.config import settings; init_db(settings.database_url)"
```

#### Step 7: Configure Ingress

```bash
# Update domain in manifests/provisioning-api.yaml
# Then apply:
kubectl apply -f manifests/provisioning-api.yaml

# Verify ingress
kubectl get ingress -n beta9-provisioning

# Test endpoint
curl https://provisioning.beta9.example.com/health
```

### Option 2: Docker Compose (Development)

```bash
# Create .env file
cp .env.example .env
# Edit .env with your settings

# Start services
docker-compose up -d

# Check logs
docker-compose logs -f

# Initialize database
docker-compose exec api python -c "from models.database import init_db; from api.config import settings; init_db(settings.database_url)"

# Test
curl http://localhost:8000/health
```

Example `docker-compose.yaml`:

```yaml
version: '3.8'

services:
  postgres:
    image: postgres:15
    environment:
      POSTGRES_PASSWORD: postgres
      POSTGRES_DB: provisioning
    ports:
      - "5432:5432"
    volumes:
      - postgres-data:/var/lib/postgresql/data

  redis:
    image: redis:7
    ports:
      - "6379:6379"

  api:
    build:
      context: .
      dockerfile: Dockerfile.api
    ports:
      - "8000:8000"
    env_file: .env
    depends_on:
      - postgres
      - redis
    volumes:
      - /var/run/docker.sock:/var/run/docker.sock
      - workdir:/tmp/beta9-provisioning

  worker:
    build:
      context: .
      dockerfile: Dockerfile.worker
    env_file: .env
    depends_on:
      - postgres
      - redis
    volumes:
      - /var/run/docker.sock:/var/run/docker.sock
      - workdir:/tmp/beta9-provisioning

volumes:
  postgres-data:
  workdir:
```

### Option 3: Local Development

```bash
# Install dependencies
pip install -r requirements.txt

# Start PostgreSQL and Redis
make dev-up

# Initialize database
make db-init

# Run API (terminal 1)
make run-api

# Run worker (terminal 2)
make run-worker

# Test
curl http://localhost:8000/health
```

## Post-Deployment Verification

### Health Checks

```bash
# API health
curl https://provisioning.beta9.example.com/health
# Expected: {"status": "healthy", "service": "beta9-provisioning"}

# Metrics endpoint
curl https://provisioning.beta9.example.com/metrics
# Expected: Prometheus metrics

# Database connectivity
kubectl exec -it -n beta9-provisioning <api-pod> -- \
  python -c "from models.database import get_db; db = get_db(); print('DB OK')"
```

### End-to-End Test

```bash
# Create test environment (requires AWS credentials)
curl -X POST https://provisioning.beta9.example.com/api/v1/environments \
  -H "Content-Type: application/json" \
  -d @example-config.yaml

# Response: {"environment_id": "env_abc123", "status": "queued", ...}

# Poll status
watch curl -s https://provisioning.beta9.example.com/api/v1/environments/env_abc123 | jq .status

# Delete environment
curl -X DELETE https://provisioning.beta9.example.com/api/v1/environments/env_abc123
```

## Configuration

### Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `DATABASE_URL` | PostgreSQL connection string | `postgresql://postgres:postgres@localhost:5432/provisioning` |
| `REDIS_URL` | Redis connection string | `redis://localhost:6379/0` |
| `WORKDIR_BASE` | Working directory for CDKTF | `/tmp/beta9-provisioning` |
| `MAX_ENVIRONMENTS_PER_USER` | Max concurrent environments | `10` |
| `ALLOWED_REGIONS` | Comma-separated list | `us-east-1,us-west-2,...` |
| `LOG_LEVEL` | Logging level | `INFO` |

### Scaling

#### Horizontal Scaling

```bash
# Scale API
kubectl scale deployment provisioning-api \
  --replicas=3 \
  -n beta9-provisioning

# Scale workers
kubectl scale deployment provisioning-worker \
  --replicas=5 \
  -n beta9-provisioning
```

#### Vertical Scaling

Edit `manifests/provisioning-api.yaml`:

```yaml
resources:
  requests:
    memory: "1Gi"
    cpu: "1000m"
  limits:
    memory: "2Gi"
    cpu: "2000m"
```

#### Auto-scaling

```bash
# HPA for API
kubectl autoscale deployment provisioning-api \
  --cpu-percent=70 \
  --min=2 \
  --max=10 \
  -n beta9-provisioning

# HPA for workers (based on queue length - requires custom metrics)
kubectl apply -f - <<EOF
apiVersion: autoscaling/v2
kind: HorizontalPodAutoscaler
metadata:
  name: provisioning-worker
  namespace: beta9-provisioning
spec:
  scaleTargetRef:
    apiVersion: apps/v1
    kind: Deployment
    name: provisioning-worker
  minReplicas: 2
  maxReplicas: 10
  metrics:
  - type: External
    external:
      metric:
        name: redis_queue_length
        selector:
          matchLabels:
            queue: provisioning:jobs
      target:
        type: AverageValue
        averageValue: "5"
EOF
```

## Monitoring Setup

### Prometheus

```bash
# Install Prometheus Operator
helm install prometheus prometheus-community/kube-prometheus-stack \
  --namespace monitoring \
  --create-namespace

# Add ServiceMonitor for provisioning API
kubectl apply -f - <<EOF
apiVersion: monitoring.coreos.com/v1
kind: ServiceMonitor
metadata:
  name: provisioning-api
  namespace: beta9-provisioning
spec:
  selector:
    matchLabels:
      app: provisioning-api
  endpoints:
  - port: http
    path: /metrics
    interval: 30s
EOF
```

### Grafana Dashboards

Import pre-built dashboards:

1. Provisioning Overview (ID: TODO)
2. API Performance (ID: TODO)
3. Worker Health (ID: TODO)

### Alerting

Example Prometheus alerts:

```yaml
apiVersion: monitoring.coreos.com/v1
kind: PrometheusRule
metadata:
  name: provisioning-alerts
  namespace: beta9-provisioning
spec:
  groups:
  - name: provisioning
    interval: 30s
    rules:
    - alert: ProvisioningAPIDown
      expr: up{job="provisioning-api"} == 0
      for: 5m
      labels:
        severity: critical
      annotations:
        summary: "Provisioning API is down"

    - alert: HighProvisioningFailureRate
      expr: rate(provisioning_environments_total{status="failed"}[1h]) > 0.1
      for: 10m
      labels:
        severity: warning
      annotations:
        summary: "High provisioning failure rate"

    - alert: EnvironmentStuckInProvisioning
      expr: time() - provisioning_environment_created_at{status="provisioning"} > 3600
      for: 5m
      labels:
        severity: warning
      annotations:
        summary: "Environment stuck in provisioning for >1 hour"
```

## Security Hardening

### Network Policies

```yaml
apiVersion: networking.k8s.io/v1
kind: NetworkPolicy
metadata:
  name: provisioning-api
  namespace: beta9-provisioning
spec:
  podSelector:
    matchLabels:
      app: provisioning-api
  policyTypes:
  - Ingress
  - Egress
  ingress:
  - from:
    - namespaceSelector:
        matchLabels:
          name: ingress-nginx
    ports:
    - protocol: TCP
      port: 8000
  egress:
  - to:
    - podSelector:
        matchLabels:
          app: postgres
    ports:
    - protocol: TCP
      port: 5432
  - to:
    - podSelector:
        matchLabels:
          app: redis
    ports:
    - protocol: TCP
      port: 6379
  - to: []  # Allow all egress for AWS API calls
```

### Pod Security Standards

```yaml
apiVersion: v1
kind: Namespace
metadata:
  name: beta9-provisioning
  labels:
    pod-security.kubernetes.io/enforce: restricted
    pod-security.kubernetes.io/audit: restricted
    pod-security.kubernetes.io/warn: restricted
```

### Secrets Management

Use external secrets operator for sensitive data:

```bash
# Install External Secrets Operator
helm install external-secrets external-secrets/external-secrets \
  --namespace external-secrets-system \
  --create-namespace

# Create SecretStore
kubectl apply -f - <<EOF
apiVersion: external-secrets.io/v1beta1
kind: SecretStore
metadata:
  name: aws-secrets-manager
  namespace: beta9-provisioning
spec:
  provider:
    aws:
      service: SecretsManager
      region: us-east-1
      auth:
        jwt:
          serviceAccountRef:
            name: provisioning-api
EOF

# Create ExternalSecret
kubectl apply -f - <<EOF
apiVersion: external-secrets.io/v1beta1
kind: ExternalSecret
metadata:
  name: provisioning-db-credentials
  namespace: beta9-provisioning
spec:
  refreshInterval: 1h
  secretStoreRef:
    name: aws-secrets-manager
    kind: SecretStore
  target:
    name: provisioning-secrets
  data:
  - secretKey: database-url
    remoteRef:
      key: beta9/provisioning/database-url
EOF
```

## Backup and Disaster Recovery

### Database Backups

```bash
# Enable automated backups (RDS)
aws rds modify-db-instance \
  --db-instance-identifier beta9-provisioning-db \
  --backup-retention-period 7 \
  --preferred-backup-window "03:00-04:00"

# Manual backup
kubectl exec -it -n beta9-provisioning <api-pod> -- \
  pg_dump $DATABASE_URL > backup-$(date +%Y%m%d).sql
```

### Disaster Recovery Plan

1. **RTO (Recovery Time Objective)**: 1 hour
2. **RPO (Recovery Point Objective)**: 1 hour

**Recovery Steps**:

1. Restore database from backup:
   ```bash
   aws rds restore-db-instance-from-db-snapshot \
     --db-instance-identifier beta9-provisioning-db-restored \
     --db-snapshot-identifier <snapshot-id>
   ```

2. Redeploy application:
   ```bash
   kubectl apply -f manifests/provisioning-api.yaml
   ```

3. Verify health:
   ```bash
   curl https://provisioning.beta9.example.com/health
   ```

4. Resume operations

## Troubleshooting Deployment

### Pods Not Starting

```bash
# Check pod status
kubectl get pods -n beta9-provisioning

# Check events
kubectl describe pod <pod-name> -n beta9-provisioning

# Common issues:
# - ImagePullBackOff: Check image name and registry credentials
# - CrashLoopBackOff: Check logs for application errors
# - Pending: Check resource requests and node capacity
```

### Database Connection Issues

```bash
# Test connection from pod
kubectl exec -it -n beta9-provisioning <api-pod> -- \
  psql $DATABASE_URL -c "SELECT 1"

# Check network policies
kubectl get networkpolicies -n beta9-provisioning

# Check security groups (AWS)
aws ec2 describe-security-groups --group-ids <sg-id>
```

### IRSA Not Working

```bash
# Check service account annotation
kubectl describe sa provisioning-api -n beta9-provisioning

# Check pod environment
kubectl exec -it -n beta9-provisioning <api-pod> -- env | grep AWS

# Test IAM role
kubectl exec -it -n beta9-provisioning <api-pod> -- \
  aws sts get-caller-identity
```

## Upgrading

### Rolling Update

```bash
# Update image tag
kubectl set image deployment/provisioning-api \
  api=$DOCKER_REGISTRY/provisioning-api:v0.2.0 \
  -n beta9-provisioning

# Check rollout status
kubectl rollout status deployment/provisioning-api -n beta9-provisioning

# Rollback if needed
kubectl rollout undo deployment/provisioning-api -n beta9-provisioning
```

### Blue/Green Deployment

1. Deploy new version alongside old:
   ```bash
   kubectl apply -f manifests/provisioning-api-v2.yaml
   ```

2. Test new version:
   ```bash
   kubectl port-forward -n beta9-provisioning svc/provisioning-api-v2 8001:80
   curl http://localhost:8001/health
   ```

3. Switch ingress to new version:
   ```bash
   kubectl patch ingress provisioning-api -n beta9-provisioning \
     --type=json -p='[{"op": "replace", "path": "/spec/rules/0/http/paths/0/backend/service/name", "value": "provisioning-api-v2"}]'
   ```

4. Monitor and rollback if issues
5. Decommission old version

## Cost Optimization

- Use spot instances for worker nodes
- Auto-scale workers based on queue length
- Use smaller instance types for non-production
- Enable S3 lifecycle policies for Terraform state
- Use RDS reserved instances for production database
