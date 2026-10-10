# Deploy

## Choose the shape

| The code is… | Deploy it as | How |
|---|---|---|
| A web server in any language (Django, Rails, Express, Go, …) | container | `dockerfile` (a `./Dockerfile` is picked up automatically) |
| A prebuilt image (`ghcr.io/…`, `grafana/grafana`) | container | `image` |
| A queue consumer, scheduler, or bot with no HTTP port | worker | `ports: []`; it runs continuously |
| A Python function with a `{{product}}` decorator (`@endpoint`, `@task_queue`, `@function`, `Pod(...)`) | handler | `handler: "app.py:name"` |
| Several services, or a `docker-compose.yml` | stack | `stack_from_compose`, then `stack_plan` / `stack_apply` (below) |

Containers listen on the ports you pass; the platform routes public HTTPS to
each, and the first gets the app's URL. Without `ports`, the deploy uses the
`EXPOSE` of the image, or of the Dockerfile stage it builds (base images
included), and says so in its `notes`; with no `EXPOSE` the app gets 8000 and
`PORT` is set to it. Pass `ports` when the server binds something else.

Pass `tcp: true` only when every port is raw TCP (SSH, a database). A server
with one HTTP port and other native ports (ClickHouse: HTTP 8123, native
9000) keeps `tcp: false`; other apps reach a native port at
`${{app.<name>.TCP.<port>}}`, a `host:443` address on the TLS TCP gateway, so
the client must enable TLS and send that host as SNI. Apps configured by
separate host and port settings (`DB_HOST`, `DB_PORT`) take
`${{app.<name>.HOST.<port>}}` and `${{app.<name>.PORT.<port>}}`.

## From MCP

```json
deploy {
  "name": "shop-web",
  "directory": "/path/to/project",
  "dockerfile": "Dockerfile",
  "ports": [8000],
  "env": {
    "DATABASE_URL": "${{db.shop-db.DATABASE_URL}}",
    "SECRET_KEY": "${{secret.SHOP_SECRET_KEY}}",
    "ALLOWED_HOSTS": "*"
  },
  "cpu": 1, "memory": "2Gi",
  "keep_warm_seconds": -1
}
```

Always pass `directory` as the project's absolute path; the default is the
MCP server's working directory, which is not necessarily the project.

The call returns a `job_id`. Call `deploy_status` with `wait_seconds: 55` and
the returned `log_cursor`: it returns as soon as the job settles, with the
newest build lines, as `accepted` (the platform took the deployment; `url` is
set) or `failed` (`error` says why); while it is `running`, call it again.
Retrying `deploy` with the same `idempotency_key` and request returns the
same job; one that failed before deploying anything runs again.

Accepted is not serving yet. `wait_deployment` checks the new version: a
container is ready once `/` answers below 500 (a 404 or a login redirect
still means its server is up); pass `path` when the app has a health
endpoint, which must answer 2xx. When a new app answers 503, read its `logs`
(`stream: system` shows container exits) before changing anything.

## From the CLI

```bash
{{cli}} deploy --dockerfile Dockerfile --name shop-web --port 8000 \
  --env DATABASE_URL='${{db.shop-db.DATABASE_URL}}' --cpu 1 --memory 2Gi --json
{{cli}} deploy app.py:handler --name api --json          # decorated function
{{cli}} deployment wait <deployment_id>                   # block until it serves
{{cli}} serve app.py:handler                              # temporary preview, hot reload
```

`--json` prints one object last (`deployment_id`, `stub_id`, `invoke_url`),
and errors as `{"error", "code"}`; branch on `code` (`NOT_AUTHENTICATED`,
`NOT_FOUND`, …).

## Options that matter

- `cpu`, `memory`, `gpu`: per app. `gpu` is a type (`T4`, `A10G`, `A100-40`,
  `H100`) or a priority list. Only add a GPU when the code needs one.
- `context_dir`: the build context, relative to `directory`, when it is not
  the Dockerfile's directory (a monorepo's `docker/web/Dockerfile` that
  copies from the repository root: `"dockerfile": "docker/web/Dockerfile",
  "context_dir": "."`). The context is what `docker build` would send: the
  repository's `.dockerignore` applies (`.beamignore` does not).
- `target`, `build_args`: as `docker build --target prod --build-arg K=V`.
  Without `target` the last stage is built, which in many multi-stage
  Dockerfiles is a dev or test stage; use the stage the README builds.
- `keep_warm_seconds`: `-1` keeps one container running (web apps users hit
  directly); `0`/small values scale to zero when idle and cold start on the
  next request.
- `min_replicas` / `max_replicas`: fixed or autoscaled replica counts.
- `disks`: `["data:/var/lib/app:10Gi"]`, a disk for state a single service
  owns. It is saved when the container stops or exits, crashes included, and
  restored by the next one, so it survives restarts and redeploys; a machine
  failure loses what was written since the container started. Data that must
  not be lost belongs in managed Postgres or Redis, which keep every
  acknowledged write. Shared storage across apps is a volume
  (`Volume(name, mount_path)` in code).
- `secrets`: workspace secret names injected as env vars. Create them with
  `create_secret` (MCP) or `{{cli}} secret create NAME`; never put values in
  `env`.

## Docker Compose projects

`stack_from_compose { "path": "/abs/project" }` reads the compose file (and
`.env`, or `.env.example` when the project ships only that) and returns a
draft stack spec plus warnings. It provisions nothing. When the README deploys
with several files (`docker compose -f docker-compose.yml -f
docker-compose.prod.yml up`), pass the others in `files`.

- `postgres` and `redis` images become managed databases (Postgres 16, Redis
  7); compose's credentials and connection settings become `db` references,
  and TLS flags for them are switched on. Other databases stay containers on
  a disk.
- `image:` and `build:` services become applications. A service that others
  wait on with `service_completed_successfully` becomes a job that runs once
  per apply.
- `http://minio:9000`, `redis:6379`, `DB_HOST=db`, and
  `localhost:<published port>` in env become references; ports others need
  are exposed. A host setting gains the matching port (and, for a managed
  database, password) setting. A port compose publishes only on `127.0.0.1`
  stays unexposed unless another service or a health check needs it.
- An application without ports gets its image's `EXPOSE` at plan time.
  Images on hosts that only proxy Docker Hub (`docker.langfuse.com`,
  `docker.n8n.io`) are pulled from Docker Hub itself: those proxies share
  one anonymous rate limit.
- Proxies that drive containers through the Docker socket (Traefik) and
  `base` services others only extend are left out.
- Named volumes become durable disks (one app each). Bind-mounted files are
  copied into a derived image; writes to them do not persist.
- Placeholder and unset credentials become generated stack secrets
  (`spec.secrets`), created once and never shown; settings read from one
  variable share one secret.
- The compose healthcheck, the image's HEALTHCHECK, or a known image (MinIO,
  ClickHouse, …) supplies `health_path`; otherwise readiness only checks that
  the container runs. Add the app's real health path when you know it.

Read every warning before `stack_plan`; they are the work left to do. The
common ones:

- A client reaching a native port through `${{app.X.TCP.<port>}}` must enable
  TLS: set the app's TLS or `secure` option for that connection.
- A config file that names another service by host (`proxy_pass
  http://api:8000`) must use the public address instead. A reverse proxy that
  only routes to one app is usually unnecessary here: expose the app itself.
- A dependency nothing in env points at is usually reached at an address
  built into the app (`DB_HOSTNAME` defaulting to `database`); find that
  setting in the app's docs and set it to the reference the warning gives.
- A command that waits for another service by host (`/dev/tcp/db/5432`)
  never succeeds; the stack already starts it after its dependencies.
- Every port is public. Ports compose kept private (an admin console, a
  database) need their own password or token.
- `user:`, `ulimits`, `cap_add`, `privileged`, `network_mode` and the Docker
  socket are not available.
- Managed versions differ from compose pins; check the app supports them.

Then `stack_plan`, review the plan with the user, and call `stack_apply`
until it reports `Stack applied`. Applications in a stack run continuously
(`min_replicas: 1`) unless their deploy sets `min_replicas` or
`keep_warm_seconds`.

Each `stack_apply` returns every service's status, with the newest build or
container logs for one still starting. A service the platform never accepted
(a failed build, a rejected config) is `failed`: fix the spec and plan again.
One it accepted that never became ready is `uncertain`: read its `logs`, then
`stack_resolve` it or apply a corrected plan. Planning again keeps
applications and jobs whose spec and source are unchanged and that still run
or have completed, so fixing one service redeploys only that one; name
services in `redeploy` to rebuild them anyway (an image tag that moved).
Changed jobs rerun, so migrations must be idempotent.

## Writing the Dockerfile when there is none

Generate a conventional one for the framework and commit it; do not invent a
platform-specific base image. The container must bind `0.0.0.0` on the port
you pass. Run schema migrations at start (`CMD sh -c "manage.py migrate && gunicorn …"`)
or as a one-off with `{{cli}} run`; there is no separate release phase.

Framework notes:

- **Django**: `gunicorn project.wsgi:application -b 0.0.0.0:8000`; set
  `ALLOWED_HOSTS`, `CSRF_TRUSTED_ORIGINS` to the app URL after the first deploy
  (`set_env`), `DATABASE_URL` via `dj-database-url` or split `${{db.x.HOST}}`
  etc.; `collectstatic` in the image and serve with WhiteNoise.
- **FastAPI / Flask**: `uvicorn app:app --host 0.0.0.0 --port 8000` or a
  `{{product}}` `@asgi`/`@endpoint` handler if the code is already written for it.
- **Node**: respect `PORT` if the app reads it; pass the same number in `ports`.
- **Rails**: `RAILS_ENV=production`, `SECRET_KEY_BASE` from a secret,
  `bin/rails db:prepare` at start.
- **Static sites**: serve the build with a tiny server (`nginx`, `caddy`) on
  a port; do not deploy a build step as the entrypoint.

## After the first deploy

- Read back with `get_app` (MCP): config, env, URLs. `url` is that version's
  own address; `latest_url` follows each new deploy (a multi-port container
  has one per port in `port_urls`).
- Give the user the latest URL and what to expect (first request may cold
  start). Every container URL is public, so an admin console or a database
  port needs its own password or token.
- If the app needs its own URL in env (callbacks, `ALLOWED_HOSTS`), set it as
  `${{app.<name>.URL}}` with `set_env`; the platform fills it in.
