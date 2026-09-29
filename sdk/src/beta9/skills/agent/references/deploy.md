# Deploy

## Choose the shape

| The code is… | Deploy it as | How |
|---|---|---|
| A web server in any language (Django, Rails, Express, Go, …) | container | `dockerfile` (a `./Dockerfile` is picked up automatically) + `ports` |
| A prebuilt image (`postgres:16`, `ghcr.io/…`) | container | `image` + `ports` |
| A Python function with a `{{product}}` decorator (`@endpoint`, `@task_queue`, `@function`, `Pod(...)`) | handler | `handler: "app.py:name"` |

Containers listen on the port you pass; the platform routes HTTPS to it and
gives the app a URL. Pass `tcp: true` for raw TCP (SSH, databases) instead of
HTTP.

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

The call returns a `job_id` after ~20 s; poll `deploy_status` with
`wait_seconds: 50` and the returned `log_cursor` until `status` is
`deployed` (then `url` is set) or `failed` (then `error` says why).
Containers run unprivileged: bind ports above 1024 (an image that listens on
80 fails with `bind: permission denied`; check `logs` when a new app answers 503).

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
- `keep_warm_seconds`: `-1` keeps one container running (web apps users hit
  directly); `0`/small values scale to zero when idle and cold start on the
  next request.
- `min_replicas` / `max_replicas`: fixed or autoscaled replica counts.
- `disks`: `["data:/var/lib/app:10Gi"]`, a durable disk that survives
  restarts, for state a single service owns. Shared storage across apps is a
  volume (`Volume(name, mount_path)` in code).
- `secrets`: workspace secret names injected as env vars. Create them with
  `create_secret` (MCP) or `{{cli}} secret create NAME`; never put values in
  `env`.

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

- Read back with `get_app` (MCP): config, env, URL.
- Give the user the URL and what to expect (first request may cold start).
- If the app needs its own URL in env (callbacks, `ALLOWED_HOSTS`), set it as
  `${{app.<name>.URL}}` with `set_env`; the platform fills it in.
