# Databases

Managed databases are apps of kind `database`: the upstream image on a
durable disk, reached over TLS, with generated credentials stored as
workspace secrets. Kinds: `postgres`, `redis`, `mysql`, `mongo`.

## Create

MCP: `create_database { "kind": "postgres", "name": "shop-db" }`
(add `"always_on": true` for a database that must answer instantly; the
default scales to zero when idle and wakes on the first connection).

CLI: `{{cli}} db create postgres shop-db`.

Names are workspace-wide; convention is `<app>-db` and `<app>-cache`.
Creation takes a minute; the response includes the deployment and the secret
names, never the password.

## Use it from an app

Reference the credentials in the app's env; the platform resolves them on
deploy, so nothing is copied:

```
DATABASE_URL = ${{db.shop-db.DATABASE_URL}}
PGHOST       = ${{db.shop-db.HOST}}        PGPORT = ${{db.shop-db.PORT}}
PGUSER       = ${{db.shop-db.USERNAME}}    PGPASSWORD = ${{db.shop-db.PASSWORD}}
PGDATABASE   = ${{db.shop-db.DATABASE}}
REDIS_URL    = ${{db.shop-cache.REDIS_URL}}
```

`connect_services { "source": "shop-db", "target": "shop-web" }` injects the
standard set for the kind and redeploys the target; `env_name` picks a single
variable name instead.

Connection strings use TLS to the platform's TCP gateway with
`sslmode=require` (Postgres), `ssl-mode=REQUIRED` (MySQL), `tls=true`
(Mongo), `rediss://` (Redis). Clients that pin CA certificates need the
system bundle.

## Read the credentials

`database_credentials { "kind": "postgres", "name": "shop-db" }` returns the
connection string for the user (for `psql`, a GUI, a migration run). Show it
to the user only when they ask; prefer references in apps.

## Rotate, delete

`rotate_database_credentials` sets a new password and restarts the database
and every app bound to it; confirm with the user first.
`delete_database` (requires `confirm: true`) removes the service and its
secrets; the disk is kept.

## Migrations and seed data

Run them from the app's own container at start (see deploy.md), or as a
one-off container with the same image and env:
`{{cli}} run --image <image> --env DATABASE_URL='${{db.shop-db.DATABASE_URL}}' -- python manage.py migrate`.
