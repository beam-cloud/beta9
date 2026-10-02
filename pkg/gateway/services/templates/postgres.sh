#!/bin/sh
# Managed Postgres lifecycle. The cluster lives on the durable disk, the
# database's only storage.
set -eu

export PATH=/usr/lib/postgresql/16/bin:$PATH
CLUSTER=/var/lib/postgresql/data/pgdata
INITIALIZING=/var/lib/postgresql/data/pgdata.initializing
export PGDATA=$CLUSTER
export POSTGRES_INITDB_ARGS=--data-checksums
export PGHOST=/var/run/postgresql PGUSER="$POSTGRES_USER" PGDATABASE=postgres

sql() {
    gosu postgres psql -X -q -v ON_ERROR_STOP=1 "$@"
}

# initdb creates only template1, template0 and postgres (base/1, 4 and 5), and
# numbers every relation file it writes below 16384. Any other database,
# tablespace or relation was created after initialization. Whatever cannot be
# read is taken to hold data.
holds_data() {
    if [ -e "$1/base" ]; then
        databases=$(ls -A "$1/base") || return 0
        for database in $databases; do
            case $database in 1 | 4 | 5 | pgsql_tmp) ;; *) return 0 ;; esac
        done
        relations=$(find "$1/base" -type f -name '[0-9]*') || return 0
        printf '%s\n' "$relations" | sed 's|.*/||; s|[._].*||' |
            awk '$1 >= 16384 { found = 1 } END { exit !found }' && return 0
    fi
    if [ -e "$1/pg_tblspc" ]; then
        tablespaces=$(ls -A "$1/pg_tblspc") || return 0
        [ -z "$tablespaces" ] || return 0
    fi
    return 1
}

start_pooler() {
    sql -At --set=role="$POSTGRES_USER" > /tmp/pgbouncer-users <<'SQL'
SELECT '"' || rolname || '" "' || rolpassword || '"' FROM pg_authid WHERE rolname = :'role';
SQL
    chmod 600 /tmp/pgbouncer-users
    chown postgres:postgres /tmp/pgbouncer-users
    cat >/tmp/pgbouncer.ini <<EOF
[databases]
$POSTGRES_DB = host=127.0.0.1 port=5432 dbname=$POSTGRES_DB
[pgbouncer]
listen_addr=*
listen_port=$BEAM_DATABASE_POOL_PORT
unix_socket_dir=/tmp
auth_type=scram-sha-256
auth_file=/tmp/pgbouncer-users
pool_mode=transaction
default_pool_size=20
max_client_conn=100
max_prepared_statements=100
pidfile=/tmp/pgbouncer.pid
EOF
    gosu postgres pgbouncer /tmp/pgbouncer.ini &
    POOLER=$!
}

shutdown() {
    trap - TERM INT
    [ -z "${POOLER:-}" ] || kill "$POOLER" 2>/dev/null || true
    if gosu postgres pg_ctl -D "$PGDATA" status >/dev/null 2>&1; then
        gosu postgres pg_ctl -D "$PGDATA" -m fast -w stop
    fi
}

start() {
    mkdir -p "$PGHOST"
    chown postgres:postgres "$PGHOST"
    # Recreate managed access rules on every start, including crash recovery.
    cat >/tmp/beam-pg_hba.conf <<'EOF'
local all all trust
host all all all scram-sha-256
host replication all all scram-sha-256
EOF

    # A new cluster is built beside PGDATA and renamed into place only after
    # its database exists and it shut down cleanly. initdb leaves PG_VERSION
    # behind when it cannot remove a failed attempt, and the entrypoint would
    # then boot those partial files as a database on every restart. Clusters
    # built in place before this can be such partial files: one that holds
    # nothing initdb did not create and lacks either a whole control file
    # (always 8192 bytes) or the application database, which the entrypoint
    # creates last, never finished.
    if [ -s "$CLUSTER/PG_VERSION" ] &&
        { [ "$POSTGRES_DB" != postgres ] ||
            [ "$(stat -c %s "$CLUSTER/global/pg_control" 2>/dev/null)" != 8192 ]; } &&
        ! holds_data "$CLUSTER"; then
        echo "Discarding a database cluster whose initialization never finished" >&2
        rm -rf "$CLUSTER"
    fi
    if [ ! -s "$CLUSTER/PG_VERSION" ]; then
        # Files without PG_VERSION are no cluster Postgres can start, but a
        # database among them is still somebody's data.
        if [ -e "$CLUSTER" ] && holds_data "$CLUSTER"; then
            echo "$CLUSTER has no PG_VERSION but holds data; refusing to initialize over it" >&2
            exit 1
        fi
        rm -rf "$CLUSTER" "$INITIALIZING"
        export PGDATA=$INITIALIZING
    fi

    trap shutdown TERM INT
    # Keep clients out while crash recovery and password rotation finish.
    docker-entrypoint.sh postgres -c listen_addresses=127.0.0.1 -c hba_file=/tmp/beam-pg_hba.conf &
    POSTGRES=$!
    ready=false
    failures=0
    for attempt in $(seq 1 600); do
        kill -0 "$POSTGRES" || { wait "$POSTGRES"; exit 1; }
        sleep 1
        gosu postgres pg_isready -h 127.0.0.1 >/dev/null 2>&1 || continue
        if ! state=$(sql -Atc 'SELECT NOT pg_is_in_recovery()' 2>/tmp/beam-readiness); then
            # A server that accepts connections but fails this query for half
            # a minute is damaged, not recovering.
            failures=$((failures + 1))
            if [ "$failures" -ge 30 ]; then
                echo "Database accepts connections but cannot be queried: $(cat /tmp/beam-readiness)" >&2
                shutdown
                exit 1
            fi
            continue
        fi
        failures=0
        if [ "$state" = t ]; then
            ready=true
            break
        fi
    done
    [ "$ready" = true ] || { echo "Database recovery did not finish within 600 seconds" >&2; exit 1; }

    # PG_VERSION can survive a failed initdb; do not expose a partial cluster.
    if ! sql --dbname="$POSTGRES_DB" -Atc 'SELECT 1' >/dev/null; then
        echo "Managed database is missing or inaccessible; inspect initialization/recovery logs before restoring it." >&2
        shutdown
        exit 1
    fi

    sql --set=role="$POSTGRES_USER" --set=password="$POSTGRES_PASSWORD" <<'SQL'
ALTER ROLE :"role" PASSWORD :'password';
SQL
    gosu postgres pg_ctl -D "$PGDATA" -m fast -w stop
    wait "$POSTGRES"
    if [ "$PGDATA" = "$INITIALIZING" ]; then
        if ! gosu postgres pg_controldata "$PGDATA" | grep -q '^Database cluster state: *shut down$'; then
            echo "The new database did not shut down cleanly; it is initialized again on restart." >&2
            exit 1
        fi
        mv "$INITIALIZING" "$CLUSTER"
        sync /var/lib/postgresql/data
        export PGDATA=$CLUSTER
    fi

    docker-entrypoint.sh postgres -c listen_addresses='*' -c wal_compression=on \
        -c hba_file=/tmp/beam-pg_hba.conf \
        -c fsync=on -c synchronous_commit=on -c full_page_writes=on &
    POSTGRES=$!
    for attempt in $(seq 1 60); do
        sql -Atc 'SELECT 1' >/dev/null 2>&1 && break
        kill -0 "$POSTGRES" || { wait "$POSTGRES"; exit 1; }
        sleep 1
    done
    start_pooler
    wait "$POSTGRES" || status=$?
    shutdown
    exit "${status:-0}"
}

case "${1:-start}" in
    start) start ;;
    *) echo "Expected start" >&2; exit 2 ;;
esac
