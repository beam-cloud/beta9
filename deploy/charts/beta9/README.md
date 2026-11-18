# Worker and gateway isolation

The chart grants the gateway permission to create, get, list, and delete Jobs
and list Pods in the worker namespace. Set `workerRBAC.namespace` to match
`worker.namespace` when an external config supplies a different namespace.
The chart otherwise uses `config.worker.namespace` or the built-in `beta9`
namespace. During an upgrade, verify the new RoleBinding and worker Job
creation before removing any legacy wildcard ClusterRoleBinding that remains.

Local GPU pools require an explicit `containerRuntime` of `gvisor` or
`microvm`. For gVisor, set `allowedNvidiaDriverVersions` to the exact approved
loaded NVIDIA module versions. Upgrade the gateway and worker images together.
New gateways wait for runtime and driver verification from GPU workers before
assigning GPU work. Drain old GPU workers before enabling the pool again.

Run the gateway, workers, and cache servers under separate service accounts.
Configure `worker.serviceAccountName` for the worker namespace and bind any
cloud role to the specific namespace and service account. The cache-server
DaemonSet uses its own service account and a separate config Secret.

Shared local workers resolve S3 image-archive credentials through the AWS SDK
default credential chain. Before upgrading an S3 deployment, configure a
dedicated worker service account with workload identity accepted by the archive
endpoint, and scope its role to the archive bucket and required object operations.
Verify cold archive pulls and image publication from a new worker before
recycling existing Jobs.

On multi-tenant clusters, place the gateway and credential-bearing backends on
trusted node pools. Use protected node labels, gateway pod placement values,
worker `jobSpec.nodeSelector`, and cache-server placement values to keep those
nodes separate from tenant compute. A distinct worker namespace limits the
Secrets available to Jobs in that namespace. Apply admission policy to worker
Jobs and Pods to enforce approved Secrets, service accounts, and placement.

Provision the cache-only configuration before upgrading the DaemonSet. Use
matching gateway, cache server, and worker releases for object grants and
streamed cache fills. On a mixed rollout, older workers may miss cache fills
until they are replaced. Check cold archive and workspace-object fills, along
with ordinary peer reads, before completing the rollout.
