# Staging gateway source reload

The release gateway image already includes the Linux Go toolchain. The hosted
Okteto manifest starts its packaged gateway in `postStart`, before source sync,
and compiles source edits beside the running process. A failed compile leaves
the gateway running and retries after 15 seconds, including when the source has
not changed. A successful compile swaps binaries, checks health, and restores
the previous binary if startup fails, then retries after the same delay. Gateway and build logs go to
container stdout so a detached local terminal cannot block requests.

Force the staging AWS ExternalSecret to sync and verify the resulting Kubernetes
secret first. Then prepare the bootstrap config with local Python and PyYAML:

```sh
python3 hack/okteto-hosted-sync/prepare-bootstrap.py
```

This reads the verified `beta9-config` Kubernetes secret using the fixed staging
context and writes `/private/tmp/beta9-hosted-bootstrap.yaml` with mode `0600`.
Only `managedEndpoints.repo.url` changes to an empty string, disabling GitOps in
the packaged gateway during initial compilation or rollback. Routing and model
supervision remain active. Source-built gateways use the normal mounted AWS
config at `/etc/beta9/config.yaml`. Neither AWS config nor its branch selection
needs a compatibility alias. The private bootstrap file never enters source sync
or the repository; Okteto copies it separately. Keep the mode-0600 file for the
whole session: Okteto rereads it when reconnecting. Remove it only after the
session is stopped, and regenerate it from the verified secret for the next one.

Use an explicit staging context and a separate Okteto state directory so local
k3d development remains independent:

```sh
GATEWAY_TAG=codex-hosted-fixes-recovery-20260910 \
OKTETO_FOLDER="$HOME/.okteto-stage" \
okteto up --file hack/okteto.hosted.stage.yaml \
  --context arn:aws:eks:us-east-1:683656326989:cluster/eks-stage-01 \
  --namespace beta9
```

The first session needs private module archives from the local Go cache. This
copies dependencies only; no GitHub token, SSH key, or Git config enters the pod.
Public dependencies download through the Go module proxy. In another terminal:

```sh
set -e
python3 hack/okteto-hosted-sync/seed-modules.py /private/tmp/hosted-go-modules.tar
kubectl --context arn:aws:eks:us-east-1:683656326989:cluster/eks-stage-01 \
  -n beta9 exec -i deployment/beta9-gateway-okteto -c main -- \
  sh -c 'set -e; mkdir -p /go/pkg/mod; tar -xf - -C /go/pkg/mod; date +%s > /var/tmp/beta9-hosted-dev/cache-ready' \
  < /private/tmp/hosted-go-modules.tar
rm /private/tmp/hosted-go-modules.tar
```

Module and compiler caches stay warm within this pod. The manifest intentionally
uses no persistent volume, so repeat seeding if the pod is replaced. Normal
source edits are synced automatically and need no image build, `okteto up`, or
pod restart. Edits arriving during a build are compiled before restarting once.

Keep `/var/tmp/beta9-hosted-dev/build-hold` present while staging serves public
traffic. Source synchronization continues while held. After editing and testing
Go source, use the supported reload command from the repository root:

```sh
python3 hack/okteto-hosted-sync/safe-reload.py \
  --evidence /private/tmp/hosted-safe-reload-$(date +%s).json
```

This requires another compatible gateway selected by the same Services and
healthy in both AWS target groups, plus the staging fixture’s
`/var/tmp/beta9-hosted-dev/grpc-ready` checker. Missing prerequisites fail before
traffic changes. The helper resolves Pod names, removes only
the development gateway from service selection, and waits for complete AWS
deregistration (`unused` or absent) before allowing replacement. It forces and
verifies the staging ExternalSecret, checks the new binary and readiness,
restores the hold, and waits for both targets to become healthy. `--restart`
validates this procedure using the current binary. An unchanged source tree is
a no-op. Public check failures remain in the evidence file.

Do not remove the hold directly on a public gateway. A shorter wait that stopped
at AWS `draining` produced a measured503; that gate is insufficient. Unguarded
single-process hotreload is not a supported zero-downtime workflow.

The watcher records its PID in `/var/tmp/beta9-hosted-dev/reload.pid` and uses a
file lock. Do not casually reconnect with `okteto up`: the installed Okteto
cleanup kills existing user processes before starting its command. A reconnect
needs the same complete target removal and healthy peer protection.

Only `cmd`, `pkg`, `proto`, and this helper directory sync. The Go module files
use Okteto's single-file copy mechanism (`secrets`); these files contain no
credentials. Restart the session after changing `go.mod` or `go.sum`. SDK virtual
environments, generated audit artifacts, `.git`, and local configs never sync.

The existing gateway continues serving while caches warm or builds fail. A
successful binary replacement still briefly restarts the gateway process; it
does not restart model containers or the Kubernetes pod. Controller recovery
must preserve those model containers across this interruption.

The watcher allows 195 seconds for shutdown, matching the gateway pod's normal
10-second readiness propagation, 180-second request drain, and five-second
buffer. `HOSTED_DEV_SHUTDOWN_SECONDS` overrides that budget when the gateway's
shutdown configuration changes. A shorter budget can kill active requests before
the gateway finishes draining.

This single-gateway Okteto workflow does not provide uninterrupted API access
during replacement. Model containers remaining ready and a successful retry do
not prove request continuity. Availability testing requires another compatible,
ready gateway behind the same service, functioning readiness probes, and traffic
checks throughout the replacement, including existing streams. Okteto disables
the original gateway's replicas and may remove the development pod's probes, so
verify the actual service targets before using a reload as a rollout test.
