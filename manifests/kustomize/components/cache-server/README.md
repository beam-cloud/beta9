# Cache server configuration

Before rollout, provision `beta9-cache-server-config` in the DaemonSet's
namespace. Its `config.yaml` should contain the cache settings, gateway address,
and coordinator token in the standard Beta9 format. Update any Helm override
of `cacheServer.configSecretName` to point to this Secret.

For cloud access, bind a cache-scoped workload identity through Helm's
`cacheServer.serviceAccount.annotations` or patch the Kustomize service account.
Roll the DaemonSet after changes to `config.yaml`.
