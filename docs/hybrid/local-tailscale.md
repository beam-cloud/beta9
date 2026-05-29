# Hybrid Local Tailscale Test

This path tests the real hybrid join flow locally:

```text
beam pool join
  -> gateway creates/upserts a private hybrid pool
  -> gateway mints a short-lived join token
  -> installer starts cmd/agent
  -> on macOS, installer runs the Linux agent inside Docker
  -> agent joins the tailnet with tsnet_restricted
  -> one embedded listener on the joined machine handles all backend routes
  -> scheduler creates worker slots and the agent starts worker containers
```

There is no k3s, Flux, system Tailscale daemon, or `local_direct` transport in this flow.

## Tailscale Admin Setup

In the Tailscale admin console, open **Access controls** and merge this into the tailnet policy file. If the policy still has a broad default rule such as `src: ["*"], dst: ["*:*"]`, remove it or the deny tests below should fail.

```jsonc
{
  "tagOwners": {
    "tag:beam-gateway": ["autogroup:admin"],
    "tag:beam-byo-worker": ["autogroup:admin"]
  },
  "grants": [
    {
      "src": ["tag:beam-gateway"],
      "dst": ["tag:beam-byo-worker"],
      "ip": ["tcp:29443"]
    }
  ],
  "tests": [
    {
      "src": "tag:beam-gateway",
      "accept": ["tag:beam-byo-worker:29443"],
      "deny": ["tag:beam-byo-worker:22"]
    },
    {
      "src": "tag:beam-byo-worker",
      "deny": ["tag:beam-gateway:29443", "tag:beam-byo-worker:29443"]
    }
  ]
}
```

Then create two auth keys under **Settings -> Keys -> Generate auth key**:

```text
Gateway key
  Tags: tag:beam-gateway
  Reusable: on
  Ephemeral: on
  Pre-approved: on, if device approval is enabled

Agent key
  Tags: tag:beam-byo-worker
  Reusable: on
  Ephemeral: on
  Pre-approved: on, if device approval is enabled
```

For Tailscale.com, leave `controlUrl` empty. For Headscale, set it to the Headscale control URL.

You can also create the policy entries and the two auth keys from a Tailscale API access token:

```sh
TS_API_KEY="tskey-api-..." ./hack/setup_hybrid_tailscale.sh
```

The helper non-destructively merges the Beam tags and grant into the tailnet policy, creates a tagged gateway auth key and a tagged agent auth key, then writes the local `config.yaml` used by `make start`.

By default the helper does not remove existing broad access rules in your tailnet. To remove the default allow-all ACL and add the deny tests above, run:

```sh
STRICT_TAILSCALE_POLICY=1 TS_API_KEY="tskey-api-..." ./hack/setup_hybrid_tailscale.sh
```

## Local Gateway Config

If you created keys manually, create a small local overlay config at the repo root. The default config is still loaded first, so this file only needs the Tailscale override:

```sh
cat > config.yaml <<'YAML'
tailscale:
  enabled: true
  controlUrl: ""
  authKey: "tskey-auth-REPLACE_WITH_GATEWAY_KEY"
  hybridWorkerAuthKey: "tskey-auth-REPLACE_WITH_WORKER_KEY"
YAML
```

Start the local control plane the same way you normally do, with:

```sh
make worker
make start
```

`make worker` publishes `localhost:5001/beta9-worker:latest`, which the local installer uses automatically when the gateway URL is localhost.

## Single Node Command

On macOS, start Docker Desktop first. The installer uses a Linux agent container and mounts the Docker socket so the agent can start sibling worker containers. On Linux, the same command runs the native agent.

From a second terminal in this repo, run:

```sh
uv run --project ./sdk beam pool join private-dev
```

That command creates or updates `private-dev`, gets a join token, downloads the local installer from the gateway, and starts `cmd/agent join`. Because the gateway URL is localhost, the generated installer command automatically uses `--dev`; the transport is still `tsnet_restricted`.

Expected output:

```text
starting Linux beam-agent in Docker on macOS
joined pool "private-dev" as machine "machine-..."
transport=tsnet_restricted executor=worker-container fallback=internal
agent route listener ready at beam-agent-machine-....<tailnet>:29443
```

Keep that process running while testing workloads and route/proxy behavior. Stop it with `Ctrl-C`; because the agent node is ephemeral, Tailscale removes it after it disconnects.

Useful checks:

```sh
uv run --project ./sdk beam pool join private-dev --print-only
uv run --project ./sdk beam pool list --filter name=private-dev
```

## Notes

- The local command uses the same pool join, tsnet, route, scheduler, and worker-container path as production. macOS gets Linux semantics by running the agent and workers inside Docker.
- Docker Desktop host networking must be available because the agent route listener dials worker and container host-network ports on the Docker Linux VM.
- Linux production validation should run the same `beam pool join <name>` command without a localhost gateway URL. Preflight requires rootful runtime, netns, iptables, FUSE, and GPU/CDI checks as applicable.
- The route proxy uses one machine-level tsnet listener on TCP `29443`. Individual worker and container ports are selected by the gateway route preface after the connection is established.
