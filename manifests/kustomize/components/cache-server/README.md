# Cache server configuration

The cache-server DaemonSet uses the separate `beta9-cache-server-config` Secret
in its own namespace. Provision the Secret with one `config.yaml` entry before
upgrading an existing DaemonSet or enabling `cacheServer.enabled` in Helm. Helm
can reference a different dedicated Secret through `cacheServer.configSecretName`.
If a Helm upgrade reuses old values, set `cacheServer.configSecretName` to the
new Secret explicitly.

Build `config.yaml` from the cache server's settings and a dedicated Beta9 cache
coordinator token. The gateway must accept the same coordinator token. Store the
file through your deployment's secret manager. The DaemonSet mounts only this
file and runs as a dedicated service account with Kubernetes API credential
automount disabled.

If a cache mount source needs cloud credentials, bind a cache-scoped workload
identity to the dedicated service account before rollout. Set Helm's
`cacheServer.serviceAccount.annotations` or patch the Kustomize service account.
For AWS IRSA, update the role's trust to authorize the new service account.

This example uses the default gateway address and port.

```yaml
gateway:
  grpc:
    externalHost: beta9-gateway
    externalPort: 1993
cache:
  coordinator:
    token: <cache-coordinator-token>
```

Carry over the cache-specific disk, pool, and locality settings your deployment
uses. Leave `cache.server.metadata` on the gateway. For S3-backed image archives,
set `imageService.registryStore` to `s3` for gateway-brokered origins.
Reconciliation is off unless you enable it in the cache-only config. To retain
reconciliation during migration, set `cache.reconciliation.enabled` to `true`
and supply a cache-scoped S2 key and basin as `database.s2.apiKey` and
`database.s2.basin`.

The cache server reads the file on startup. Roll the DaemonSet after updating
the Secret so running pods pick up the new configuration.
