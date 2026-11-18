# Cache server configuration

Provision `beta9-cache-server-config` in the DaemonSet's namespace through your
existing secret controller before rollout. Use one `config.yaml` entry in the
standard Beta9 YAML format containing only settings the cache needs. Set Helm's
`cacheServer.configSecretName` to that Secret if your values previously set a
different name.

```yaml
gateway:
  grpc:
    externalHost: beta9-gateway
    externalPort: 1993
cache:
  coordinator:
    token: <cache-coordinator-token>
```

Use the coordinator token accepted by the gateway and carry over any cache
settings the DaemonSet uses, such as pool, storage, and reconciliation settings.
For cloud access, bind a cache-scoped workload identity through Helm's
`cacheServer.serviceAccount.annotations` or patch the Kustomize service account.
The DaemonSet reads `config.yaml` at startup, so roll it after changes.
