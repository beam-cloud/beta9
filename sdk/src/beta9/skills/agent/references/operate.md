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
| Resources and scaling | `update_config { name, fields: { "runtime.cpu": 2000, "runtime.memory": 4096, "runtime.gpu": "A10G", "autoscaler.max_containers": 4, "autoscaler.min_containers": 0, "keep_warm_seconds": 0, "ports": [8000] } }` |
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

`list_secrets` returns names, approved hosts and placeholders without values.
`create_secret { name, value }`, `update_secret`, `delete_secret` (confirm).
Containers/pods, functions, endpoints and task queues bind names in `secrets`
or env references `${{secret.NAME}}`; they receive plaintext values on their
next container start. Image-build secrets are also plaintext. These workloads
do not require `hosts`; setting hosts does not opt them into proxying.

For sandbox/VM runtime bindings, create with `hosts: ["api.openai.com"]`:
the guest receives a placeholder for literal HTTPS request headers, and the
worker substitutes it only on approved hosts. Bind VM secrets under
`create_vm.spec.secrets` or `vm_exec.secrets`. `update_secret` preserves hosts
when omitted, replaces them when supplied, and clears them with `hosts: []`.
Brokered values refresh on the next request after the worker's 15-second
cache expires. See [VM outbound access and secrets](vms.md#outbound-access-and-secrets)
for network rules, plaintext token opt-ins and client compatibility.

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
- Prefer scale-to-zero for anything not user-facing; say what a change
  costs in resources when you make it.

## Waiting for a machine

A workload is pending until its container runs. Its phase says what it
waits for:

| Phase | What it means | What to do |
| --- | --- | --- |
| `queued` | It is about to be placed on a machine. | Wait. |
| `provisioning` | No warm machine had room, so one is starting. With reason `backup_worker_starting`, the preferred capacity was unavailable and a machine is starting on other capacity. | Wait: this takes minutes. |
| `waiting_for_capacity` | No machine can be had right now. Reason `no_capacity`: none of the kind it asked for. `no_spare_capacity`: the Spot CPU tier runs only on capacity nothing else is using, and there is none. `machine_busy`: it is pinned to one machine, which is full. | Tell the user what it waits for and what else they can do (below). |
| `starting` | A machine has it and is loading the image. | Wait. |

Where to read it:

- `get_task` returns `pending` for a task that has not started: `phase`,
  `reason`, `waited_seconds` (in that phase) and `message`.
- A `wait_deployment` that ends `NOT_READY` returns `pending` when no
  container of the app runs yet. There are no logs to read until one does.
- `{{cli}} container list` shows the phase beside the status, and a script
  that calls a function prints it as it changes.

When there is no capacity:

- Nothing is wrong with the user's code. Do not edit it, and do not retry
  in a loop: a workload keeps waiting on its own, and the scheduler gives
  up on it after about 20 minutes. It then fails with code `CAPACITY`, and
  the task's `failure_reason` says what there was none of.
- Offer the user, in this order: a list of GPU types
  (`gpu: ["A10G", "A100-40"]`: the first with capacity is used); for the
  Spot CPU tier, the shared or performance tier; a reserved machine; or
  trying later.
- `{{cli}} machine list` shows what can be reserved, with the lowest
  hourly price on demand and as spot. A spot machine costs less, and its
  provider may take it back at any time.
  `{{cli}} machine reserve --gpu A10G --ttl 2h --yes` reserves the cheapest
  on-demand one (`--pricing spot` for a spot one) and prints the `pool` to
  run on it. A reserved machine is billed by the hour whether or not
  anything runs on it: ask first, and release it when done
  (`{{cli}} machine release`).
- To limit how long a function call in a script waits, set
  `{{env}}_CAPACITY_WAIT` (`120`, `90s`, `5m`). A call that has no machine
  after that long is cancelled and fails with code `CAPACITY`. In Python it
  raises `NoCapacityError`, which a script can catch to run the call some
  other way. Without it a call keeps waiting.

## Anything else

`api_routes` lists every REST route the gateway has; `api { method, path,
body }` calls one (non-GET needs `confirm: true`). Use it for the rare
operation without a tool, and mention it so the user knows it happened.
