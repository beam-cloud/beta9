# Operate

Everything here is an MCP tool; the CLI equivalent is noted where one exists.
Mutations deploy a new version; nothing edits a running container in place.

## Look

| Question | Tool |
|---|---|
| What's here? | `list_apps`, `get_app` (config, env, URL) · `{{cli}} status --json` |
| Versions of an app | `list_deployments`, `get_deployment` · `{{cli}} deployment list` |
| Why is it failing? | `logs` (by app / deployment / task / container; `stream: stdout`, `stderr`, `system`) · `{{cli}} logs --deployment-id <id> -f` |
| Is it healthy / slow? | `metrics` (CPU, memory, GPU memory, containers over a window), `request_stats` (count, 5xx share, p50/p95/p99) |
| One invocation | `get_task`, `list_tasks`, `stop_task` |
| Inside a container | `{{cli}} shell --container-id <id>` |

Read logs before proposing a fix. `stream: system` shows image pulls,
mounts, and exits, which is where "it never started" lives.

## Change

| Intent | Tool |
|---|---|
| Env vars (values may be references) | `set_env { name, env: {...}, unset: [...] }` |
| Resources and scaling | `update_config { name, fields: { "runtime.cpu": 2000, "runtime.memory": 4096, "runtime.gpu": "A10G", "autoscaler.max_containers": 4, "keep_warm_seconds": -1, "ports": [8000] } }` |
| Wire two services | `connect_services { source, target }` |
| Replicas of a pod | `scale_deployment { name, containers }` |
| Restart with the same config | `redeploy { name }` |
| Roll back | `redeploy { deployment_id: <older version> }` |
| Pause / resume | `stop_deployment`, `start_deployment` |
| Call it | `invoke { name, path, method, body }` |

A new version of an always-on app does not stop the previous one (rollout
`auto`): both run, and both consume queues, until the old one is stopped.
Once `wait_deployment` verifies the new version, `stop_deployment` the older
active ones from `list_deployments { name, active: true }`, or deploy with
`rollout: "replace"` when a brief outage is fine. `stack_apply` does this
for a stack's apps itself.

## Secrets

`list_secrets`, `create_secret { name, value }`, `update_secret`,
`delete_secret` (confirm). Bind a secret to an app by name in `secrets`, or
reference it in env as `${{secret.NAME}}`. Deployments pick up a changed
value on their next container start.

## Stacks

A stack is a named board of apps. `create_stack { name, apps: [...] }`,
`update_stack { name, add: [...], remove: [...] }`, `list_stacks`,
`delete_stack` (apps are untouched). After deploying a multi-service app,
put its apps in one stack named after the project so the user sees the
whole thing and the references between the pieces.

A stack can also be deployed from a spec: `stack_plan` validates it,
`stack_apply` advances the plan for up to `wait_seconds` per call (repeat
until `Stack applied`), `stack_status` shows each service's state and error
(`spec: true` adds the applied spec), and `stack_resolve` settles a failed or
uncertain service after you inspect it. To change a stack, edit the spec and
plan again: unchanged applications and jobs that still run or have completed
are kept (list them in `redeploy` to rebuild), changed ones redeploy, and
databases are kept. `spec.secrets` declares secrets the apply generates once
(both settings optional: `length`, default 32, and `alphabet`, default
letters and digits) for services to share as `${{secret.NAME}}`.

## Money and capacity

- `INSUFFICIENT_CREDITS`: the workspace has no prepaid credit. `whoami`
  reports it before you start (`credit.ok: false`), and `deploy`,
  `create_database` and the CLI refuse with the same message. Stop, show
  the user the link in the message (or `{{dashboard_url}}/settings/credits`),
  and continue when they confirm. Never loop on retries.
- Capacity errors name the GPU that is not available now: offer another
  type or a priority list.
- Prefer scale-to-zero for anything not user-facing; say what a change
  costs in resources when you make it.

## Anything else

`api_routes` lists every REST route the gateway has; `api { method, path,
body }` calls one (non-GET needs `confirm: true`). Use it for the rare
operation without a tool, and mention it so the user knows it happened.
