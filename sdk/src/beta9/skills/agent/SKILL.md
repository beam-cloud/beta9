---
name: {{skill}}
description: >
  Deploy and operate applications on {{product}}: sign in, deploy a directory
  or container, provision Postgres/Redis/MySQL/MongoDB, wire services with
  references, group them into a stack, read logs and metrics, roll back,
  manage secrets, and handle credit and capacity errors. Use whenever the user
  mentions {{product}}, the `{{cli}}` CLI, deploying an app, a database for an
  app, GPUs, sandboxes, or "put this online", even if {{product}} is not named.
allowed-tools: Bash({{cli}}:*), Bash(which:*), Bash(command:*), Bash(curl:*), Bash(git:*)
---

# Use {{product}}

{{product}} runs code and containers on serverless CPUs and GPUs. An **app** is
the unit you deploy: a container from a Dockerfile or image, or a Python
function with a `{{product}}` decorator. Every app gets a URL. Apps share a
**workspace** (the token's scope), where **secrets**, **volumes**, **durable
disks**, and managed **databases** also live. A **stack** groups related apps
on one board with the references between them. There is no private network:
apps and databases reach each other only through **references** in env
values, resolved by the platform at deploy time:

    ${{db.<name>.DATABASE_URL}}    also REDIS_URL, HOST, PORT, USERNAME, PASSWORD, DATABASE
    ${{app.<name>.URL}}            another app's public HTTPS URL (first port)
    ${{app.<name>.URL.<port>}}     the public HTTPS URL of one port
    ${{app.<name>.TCP.<port>}}     host:443 of one port on the TLS TCP gateway
    ${{app.<name>.HOST.<port>}}    its host alone (PORT.<port> likewise), for split settings
    ${{secret.<NAME>}}             a workspace secret

A secret or credential reference must be the whole value; build URLs from
`DATABASE_URL`-style references rather than splicing a password into a
string. Clients of a `TCP` address must use TLS with its host as SNI.

## Two ways in: MCP and the CLI

You usually have both. Prefer the **MCP server** (named `{{cli}}`) for anything
about the workspace: discovery, logs, metrics, env, scaling, redeploys,
databases, secrets, stacks. Use the **CLI** when the task depends on local
files or a terminal: `{{cli}} deploy` from a project directory, `{{cli}} serve`
for a live preview, `{{cli}} shell` into a container, `{{cli}} logs -f`.

The MCP server also has local tools: `login` (browser sign-in; poll
`login_status`) and, once signed in, `deploy` (ships a project directory from
this machine; poll `deploy_status`). If the tool list is only `login` and
`login_status`, the user is not signed in yet: sign them in first, then the
workspace tools appear. With those tools you can do the whole job through MCP.

## Before you act

1. `whoami` (MCP) or `{{cli}} whoami --json`: confirm the workspace. If it says
   you are not signed in, run {{login_hint}}. **Never ask the user to paste a
   token**; sign-in is a link they click. When the result has `credit` with
   `ok: false`, nothing will run yet: show the user `credit.message` (it has
   the link to add credits) and wait for them before deploying anything.
2. `list_apps` / `{{cli}} status --json`: what already exists. App names are
   workspace-wide, so reuse or pick a distinct name.
3. Read [references/deploy.md](references/deploy.md) before the first deploy of
   a new codebase, and [references/databases.md](references/databases.md) when
   the app needs one.

## The standard job: put an app online

The shape is always the same; only the framework details change:

1. **Database first**, if needed: `create_database` (MCP) with a kind and a
   name such as `<app>-db`. Credentials become secrets; you never see or copy
   them.
2. **Deploy the app** from its directory: MCP `deploy` with `name`, the
   `dockerfile` (or `image`, or a `handler`), `ports: [<port the server binds>]`,
   and `env` that includes `DATABASE_URL: "${{db.<app>-db.DATABASE_URL}}"`.
   From a terminal that is `{{cli}} deploy --dockerfile Dockerfile --name <app>
   --port <port> --env DATABASE_URL='${{db.<app>-db.DATABASE_URL}}'`.
3. **Wait**: poll `deploy_status` (or `{{cli}} deployment wait <id>`). Builds
   take a minute or two the first time.
4. **Wire anything else** with `connect_services` or `set_env`; each call
   deploys a new version.
5. **Group** with `create_stack` (name it after the project) so the user sees
   one board, then hand back the URL and how to reach the database.

Ask before spending: pick the smallest resources that work (1 CPU, 1–2 Gi for
web apps; a GPU only when the code needs one) and say what you chose.

## Several services: a stack

For a project with more than one service, and always for a docker-compose
project, deploy a **stack** instead of wiring apps by hand:

1. `stack_from_compose { "path": "/abs/project" }` translates the compose
   file into a draft spec: postgres/redis become managed databases, service
   addresses become references, named volumes become disks, and shared
   placeholder passwords become generated secrets. It provisions nothing.
2. Resolve every warning: each marks something compose expresses that this
   platform does not (a client that must enable TLS, a config file that names
   another service by host, a version pin, a missing health path).
3. `stack_plan { "name": "<project>", "spec": ... }`, show the user the plan,
   then call `stack_apply { "plan_id": ... }` repeatedly until it says
   `Stack applied`; each call advances one step. A failed service names its
   error; read its logs, fix the spec, plan again, and apply the new plan.

See [references/deploy.md](references/deploy.md#docker-compose-projects).

## When something fails

- Read logs before guessing: MCP `logs` by app, deployment, task, or container
  (`stream: system` shows image pulls and container exits), or `{{cli}} logs`.
- Code `INSUFFICIENT_CREDITS` (or `insufficient_credits` in a message) means
  the workspace has no prepaid credit; deploys and database creation are
  refused up front. Do not retry in a loop. Tell the user, give them the
  credits link from the message, and continue once they confirm.
- Capacity errors name the GPU; offer a fallback type or a list, e.g.
  `gpu: ["A10G", "A100-40"]`.
- Roll back with `redeploy` and an older `deployment_id`; stop a bad version
  with `stop_deployment`. See [references/operate.md](references/operate.md).

## Before destructive actions

Confirm with the user before deleting an app, deployment, secret, database,
volume or disk, before stopping something in use, before rotating database
credentials (every bound app restarts), and before overwriting env on an
active deployment. Tools that require it take `confirm: true`; that is the
signal to ask first.

## References

- [setup.md](references/setup.md): sign-in, contexts, tokens for CI, MCP registration
- [deploy.md](references/deploy.md): containers, Dockerfiles, handlers, ports, workers, disks, GPUs, compose projects, framework notes
- [databases.md](references/databases.md): managed databases and references
- [operate.md](references/operate.md): logs, metrics, scaling, rollbacks, secrets, stacks
