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
system bundle. An app configured with split settings (`REDIS_HOST`,
`REDIS_PORT`, `REDIS_PASSWORD`) must turn its own TLS option on. Redis runs
with `maxmemory-policy noeviction`, as job queues such as BullMQ and Sidekiq
require.

The gateway routes by SNI, so the client must send the host as its TLS
server name; one that does not fails with `tlsv1 unrecognized name` (SSL
alert 112). libpq, Prisma, redis-py and Go clients send it. Node's ioredis
(and BullMQ on top of it) does not: give it `tls: { servername: host }`, or
set the app's own setting for it (Langfuse: `REDIS_TLS_SERVERNAME`).

In a stack, a database is a service with `"type": "database"` and
`"deploy": {"kind": "postgres"}`, sized like an application
(`"cpu": 0.5, "memory": "2Gi", "size": "10Gi"`); `stack_from_compose` turns
compose's postgres and redis services into these.

## Read the credentials

`database_credentials { "kind": "postgres", "name": "shop-db" }` returns the
connection string for the user (for `psql`, a GUI, a migration run). Show it
to the user only when they ask; prefer references in apps. The string uses
`sslmode=verify-full`, so a local libpq needs CA roots: append
`&sslrootcert=system` (libpq 16+) or a bundle path such as
`&sslrootcert=/etc/ssl/cert.pem`.

## Rotate, delete

`rotate_database_credentials` sets a new password and restarts the database
and every app bound to it; confirm with the user first.
`delete_database` (requires `confirm: true`) removes the service, its
credential secrets and its durable disk: the data is gone.

## Migrations and seed data

Run them from the app's own container at start (see deploy.md), or as a
one-off container with the same image and env:
`{{cli}} run --image <image> --env DATABASE_URL='${{db.shop-db.DATABASE_URL}}' -- python manage.py migrate`.
