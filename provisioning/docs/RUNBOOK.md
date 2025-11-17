# Beta9 Provisioning API - Operational Runbook

## Table of Contents

1. [Common Operations](#common-operations)
2. [Troubleshooting](#troubleshooting)
3. [Monitoring](#monitoring)
4. [Incident Response](#incident-response)
5. [Maintenance](#maintenance)

## Common Operations

### Check Environment Status

```bash
# Via API
curl https://provisioning.beta9.example.com/api/v1/environments/<ENV_ID>

# Via database
psql $DATABASE_URL -c "SELECT id, name, status, error_message FROM environments WHERE id='<ENV_ID>';"
```

### Manually Retry Failed Provisioning

1. Check error message:
   ```bash
   curl https://provisioning.beta9.example.com/api/v1/environments/<ENV_ID>
   ```

2. If error is transient (rate limit, network), requeue job:
   ```python
   from api.queue import enqueue_provisioning_job
   enqueue_provisioning_job("<ENV_ID>")
   ```

3. If error is permanent (invalid config), user must delete and recreate.

### Force Delete Stuck Environment

If environment is stuck in "destroying" or "provisioning":

```bash
# 1. Check worker logs
kubectl logs -n beta9-provisioning -l app=provisioning-worker --tail=100

# 2. If Terraform is still running, let it finish
# If worker crashed, manually run destroy:

# Get into worker pod
kubectl exec -it -n beta9-provisioning <worker-pod> -- bash

# Inside pod:
cd /tmp/beta9-provisioning/<ENV_ID>/cdktf.out/stacks/beta9-aws
terraform destroy -auto-approve

# 3. Update database
psql $DATABASE_URL -c "UPDATE environments SET status='destroyed' WHERE id='<ENV_ID>';"
```

### View Terraform State

```bash
# Get state backend config from database
psql $DATABASE_URL -c "SELECT state_backend_config FROM environments WHERE id='<ENV_ID>';"

# Configure AWS CLI
export AWS_PROFILE=<profile>

# Download state
aws s3 cp s3://<bucket>/<key> ./terraform.tfstate

# View state
terraform show ./terraform.tfstate
```

### Unlock Terraform State

If state is locked due to crashed worker:

```bash
# Get lock info
aws dynamodb get-item \
  --table-name <lock-table> \
  --key '{"LockID": {"S": "<bucket>/<key>-md5"}}' \
  --output json

# Force unlock (use lock ID from error message)
cd /tmp/beta9-provisioning/<ENV_ID>/cdktf.out/stacks/beta9-aws
terraform force-unlock <LOCK_ID>
```

### Scale Workers

```bash
# Scale up workers during high load
kubectl scale deployment provisioning-worker \
  -n beta9-provisioning \
  --replicas=5

# Scale down during low load
kubectl scale deployment provisioning-worker \
  -n beta9-provisioning \
  --replicas=2
```

## Troubleshooting

### Environment Stuck in "Provisioning"

**Symptoms**:
- Environment status is "provisioning" for >1 hour
- No recent logs from worker

**Diagnosis**:

1. Check worker logs:
   ```bash
   kubectl logs -n beta9-provisioning -l app=provisioning-worker \
     --tail=500 | grep <ENV_ID>
   ```

2. Check if worker is processing job:
   ```bash
   kubectl get pods -n beta9-provisioning -l app=provisioning-worker
   ```

3. Check Terraform execution:
   ```bash
   # Get into worker pod
   kubectl exec -it -n beta9-provisioning <worker-pod> -- bash
   
   # Check for running terraform processes
   ps aux | grep terraform
   
   # Check Terraform logs
   cat /tmp/beta9-provisioning/<ENV_ID>/cdktf.out/stacks/beta9-aws/terraform.log
   ```

**Resolution**:

- **If Terraform is running**: Wait for completion (can take 30+ minutes)
- **If worker crashed**: Requeue job (will resume from Terraform state)
- **If stuck in Terraform**: Force-unlock state and requeue

### Bootstrap Failure

**Symptoms**:
- Environment status is "failed" in "bootstrapping" phase
- Infrastructure exists but Beta9 not installed

**Diagnosis**:

1. Check error message:
   ```bash
   curl https://provisioning.beta9.example.com/api/v1/environments/<ENV_ID>
   ```

2. Check worker logs for bootstrap errors:
   ```bash
   kubectl logs -n beta9-provisioning -l app=provisioning-worker \
     --tail=500 | grep -A 20 "Bootstrapping Beta9"
   ```

3. Common causes:
   - Cluster not ready (nodes not running)
   - IAM authentication issues (kubeconfig)
   - Helm chart not found
   - RDS password fetch failed

**Resolution**:

1. Get kubeconfig manually:
   ```bash
   aws eks update-kubeconfig --name <cluster-name> --region <region>
   ```

2. Check cluster health:
   ```bash
   kubectl get nodes
   kubectl get pods -A
   ```

3. Manually install Beta9:
   ```bash
   # Get DB password from Secrets Manager
   aws secretsmanager get-secret-value --secret-id <secret-arn>
   
   # Install Beta9 chart
   helm install beta9 ./deploy/charts/beta9 \
     --namespace beta9-system \
     --create-namespace \
     --set database.host=<rds-endpoint> \
     --set database.password=<password> \
     --set redis.host=<redis-endpoint> \
     --set storage.s3.bucket=<bucket-name>
   ```

4. If successful, update environment status:
   ```bash
   psql $DATABASE_URL -c "UPDATE environments SET status='ready' WHERE id='<ENV_ID>';"
   ```

### RDS Creation Timeout

**Symptoms**:
- Terraform stuck on "Creating RDS instance"
- Exceeds 30-minute timeout

**Diagnosis**:

1. Check AWS console for RDS status
2. Common causes:
   - AZ capacity issues
   - Security group misconfiguration
   - Subnet group issues

**Resolution**:

1. If RDS is creating in console, wait
2. If failed in console, check error:
   - Try different AZ
   - Reduce instance size
   - Check VPC/subnet configuration

3. Update meta-config and retry

### EKS Node Group Not Ready

**Symptoms**:
- EKS cluster created but nodes not joining
- `kubectl get nodes` shows no nodes

**Diagnosis**:

1. Check node group status:
   ```bash
   aws eks describe-nodegroup \
     --cluster-name <cluster-name> \
     --nodegroup-name <nodegroup-name>
   ```

2. Check EC2 instances:
   ```bash
   aws ec2 describe-instances \
     --filters "Name=tag:eks:cluster-name,Values=<cluster-name>"
   ```

3. Common causes:
   - IAM role missing permissions
   - Security group blocking kubelet
   - Subnet has no available IPs
   - Instance type not available in AZ

**Resolution**:

1. Check IAM role policies:
   - AmazonEKSWorkerNodePolicy
   - AmazonEKS_CNI_Policy
   - AmazonEC2ContainerRegistryReadOnly

2. Check security group rules:
   - Allow all traffic from cluster security group
   - Allow HTTPS (443) to EKS endpoint

3. Check instance launch logs:
   ```bash
   aws ec2 get-console-output --instance-id <instance-id>
   ```

### High API Latency

**Symptoms**:
- API requests taking >5 seconds
- Prometheus alerts firing

**Diagnosis**:

1. Check API metrics:
   ```bash
   curl https://provisioning.beta9.example.com/metrics | grep api_request_duration
   ```

2. Check database connections:
   ```bash
   psql $DATABASE_URL -c "SELECT count(*) FROM pg_stat_activity;"
   ```

3. Check API pod resources:
   ```bash
   kubectl top pods -n beta9-provisioning -l app=provisioning-api
   ```

**Resolution**:

1. Scale API pods:
   ```bash
   kubectl scale deployment provisioning-api \
     -n beta9-provisioning \
     --replicas=4
   ```

2. Increase database connection pool:
   ```yaml
   # Update ConfigMap
   DATABASE_POOL_SIZE: "20"
   ```

3. Add database read replicas for GET requests

### Redis Queue Backlog

**Symptoms**:
- Many environments stuck in "queued"
- Redis queue length >10

**Diagnosis**:

1. Check queue length:
   ```bash
   redis-cli LLEN provisioning:jobs
   ```

2. Check worker count:
   ```bash
   kubectl get pods -n beta9-provisioning -l app=provisioning-worker
   ```

3. Check worker resource usage:
   ```bash
   kubectl top pods -n beta9-provisioning -l app=provisioning-worker
   ```

**Resolution**:

1. Scale workers:
   ```bash
   kubectl scale deployment provisioning-worker \
     -n beta9-provisioning \
     --replicas=5
   ```

2. If workers are CPU/memory bound, increase resources:
   ```yaml
   resources:
     requests:
       memory: "2Gi"
       cpu: "2000m"
     limits:
       memory: "4Gi"
       cpu: "4000m"
   ```

## Monitoring

### Key Metrics

**API Health**:
- `up{job="provisioning-api"}` - API is running
- `api_requests_total` - Request count by status code
- `api_request_duration_seconds` - API latency

**Provisioning Health**:
- `provisioning_environments_total{status="ready"}` - Successful provisions
- `provisioning_environments_total{status="failed"}` - Failed provisions
- `provisioning_duration_seconds` - Time to provision

**Queue Health**:
- Redis queue length (custom metric)
- Worker pod count
- Worker CPU/memory usage

### Alerts

**Critical**:
- API down (no healthy pods)
- Worker down (no healthy pods)
- Database unreachable
- >5 failed provisions in last hour

**Warning**:
- High API latency (p95 >2s)
- High queue backlog (>10 jobs)
- Environment stuck in provisioning >1 hour
- Worker pod CPU/memory at limits

### Dashboards

**Provisioning Overview**:
- Environments by status (pie chart)
- Provisioning success rate (gauge)
- Average provisioning time (gauge)
- Queue length over time (graph)

**API Performance**:
- Request rate (requests/sec)
- Error rate (%)
- Latency percentiles (p50, p95, p99)
- Active connections

## Incident Response

### P0: All Environments Failing

**Symptoms**: All new provisions fail immediately.

**Steps**:

1. Check API health:
   ```bash
   curl https://provisioning.beta9.example.com/health
   ```

2. Check worker logs for common error:
   ```bash
   kubectl logs -n beta9-provisioning -l app=provisioning-worker --tail=100
   ```

3. Common root causes:
   - AWS API outage (check AWS status)
   - IAM role permission change
   - CDKTF/Terraform upgrade broke compatibility
   - Database migration failed

4. Rollback recent changes:
   ```bash
   kubectl rollout undo deployment provisioning-worker -n beta9-provisioning
   kubectl rollout undo deployment provisioning-api -n beta9-provisioning
   ```

### P1: Single Region Failing

**Symptoms**: All provisions in one region fail.

**Steps**:

1. Check AWS service health for that region
2. Check region-specific quota limits:
   ```bash
   aws service-quotas get-service-quota \
     --service-code eks \
     --quota-code L-1194D53C \
     --region <region>
   ```

3. If quota exceeded, request increase or redirect traffic

### P2: High Latency

**Symptoms**: API requests slow but not failing.

**Steps**:

1. Scale API and workers
2. Check database for slow queries:
   ```bash
   psql $DATABASE_URL -c "SELECT * FROM pg_stat_statements ORDER BY total_time DESC LIMIT 10;"
   ```

3. Add database indexes if needed

## Maintenance

### Database Backups

Automated backups via PostgreSQL provider (AWS RDS or equivalent).

**Manual backup**:
```bash
pg_dump $DATABASE_URL > backup-$(date +%Y%m%d).sql
```

**Restore**:
```bash
psql $DATABASE_URL < backup-20250101.sql
```

### Database Migrations

Use Alembic for schema migrations:

```bash
# Create migration
alembic revision --autogenerate -m "Add new column"

# Apply migration
alembic upgrade head

# Rollback
alembic downgrade -1
```

### Terraform State Cleanup

Remove old state versions (S3 versioning accumulates):

```bash
# List versions
aws s3api list-object-versions \
  --bucket <state-bucket> \
  --prefix environments/

# Delete old versions (keep last 10)
# (Script needed for bulk deletion)
```

### Log Rotation

Logs are stored in CloudWatch/Stackdriver. Configure retention:

```bash
# AWS CloudWatch
aws logs put-retention-policy \
  --log-group-name /beta9/provisioning \
  --retention-in-days 30
```

### Certificate Renewal

If using cert-manager, certificates auto-renew. If manual:

```bash
# Check expiry
kubectl get certificate -n beta9-provisioning

# Force renewal
kubectl delete secret provisioning-api-tls -n beta9-provisioning
# cert-manager will recreate
```

### Dependency Updates

**Monthly**:
- Update Python dependencies: `pip-compile requirements.in`
- Update CDKTF providers: `npm update -g cdktf-cli`
- Update Terraform: Download latest from HashiCorp

**Testing**:
1. Update in dev environment
2. Run unit tests
3. Run integration test (1 environment)
4. Deploy to staging
5. Deploy to production

### Capacity Planning

**Weekly review**:
- Check average provisioning time trend
- Check queue backlog trend
- Check API request rate trend
- Check database size growth

**Scale up if**:
- Queue backlog consistently >5
- API latency p95 >2s
- Worker CPU/memory at 80%

**Scale down if**:
- Queue empty for >1 hour
- API request rate <10/min
- Worker count >2 and CPU <20%
