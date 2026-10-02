#!/bin/sh
# Managed Postgres lifecycle. Data uses the durable disk; the backup repository
# uses the existing object-backed volume. No storage credentials enter the pod.
set -eu

export PATH=/usr/lib/postgresql/16/bin:$PATH
CLUSTER=/var/lib/postgresql/data/pgdata
INITIALIZING=/var/lib/postgresql/data/pgdata.initializing
export PGDATA=$CLUSTER
export POSTGRES_INITDB_ARGS=--data-checksums
export PGHOST=/var/run/postgresql PGUSER="$POSTGRES_USER" PGDATABASE=postgres
BACKUPS=/volumes/beam-backups
CONFIG=/tmp/pgbackrest.conf
RESTORED=/var/lib/postgresql/data/.beam-restore-complete

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

backrest() {
    gosu postgres pgbackrest --config="$CONFIG" --stanza=db "$@"
}

repository_config() {
    cat >"$CONFIG" <<EOF
[global]
repo1-path=$1/repo
repo1-retention-full-type=time
repo1-retention-full=7
repo1-retention-history=7
log-level-console=warn
log-level-file=off
start-fast=y
process-max=2
[db]
pg1-path=$PGDATA
pg1-user=$POSTGRES_USER
pg1-socket-path=$PGHOST
EOF
}

publish_status() {
    state=$1
    error=$2
    info=$(backrest --output=json info) || return
    jq -n --arg status "$state" --arg error "$error" \
        --arg username "$POSTGRES_USER" --arg database "$POSTGRES_DB" \
        --argjson observed_at "$(date +%s)" --argjson archive_through "${ARCHIVE_THROUGH:-0}" \
        --argjson repository "$info" \
        '{kind:"postgres",status:$status,error:$error,username:$username,database:$database,
          observed_at:$observed_at,archive_through:$archive_through,retention_days:7,
          repository:$repository}' >"$BACKUPS/status.json.tmp" || return
    sync "$BACKUPS/status.json.tmp" || return
    mv "$BACKUPS/status.json.tmp" "$BACKUPS/status.json" || return
    sync "$BACKUPS/status.json" "$BACKUPS"
}

archive_checkpoint() {
    # A commit after the advertised timestamp lets recovery stop at that
    # timestamp even when the application itself has been idle.
    ARCHIVE_THROUGH=$(sql -Atc 'SELECT floor(extract(epoch FROM clock_timestamp()))::bigint') || return
    sql -c "SELECT pg_logical_emit_message(true, 'beam.backup', 'checkpoint')" >/dev/null || return
    backrest check || return
    export ARCHIVE_THROUGH
}

backup() (
    # Serialize scheduled and requested backups within this database container.
    exec 9>/tmp/beam-postgres-backup.lock
    flock -n 9 || { echo "A backup is already running" >&2; return 1; }
    if backrest --type="${1:-full}" backup && archive_checkpoint; then
        publish_status ready ""
    else
        publish_status failed "Backup or WAL archive failed; inspect database logs."
        return 1
    fi
)

checkpoint() (
    exec 9>/tmp/beam-postgres-backup.lock
    flock -n 9 || return 0
    if archive_checkpoint; then
        publish_status ready ""
    else
        publish_status failed "WAL archive failed; inspect database logs."
        return 1
    fi
)

schedule() {
    while :; do
        now=$(date +%s)
        if ! info=$(backrest --output=json info); then
            echo "Cannot read the backup repository; retrying in 60 seconds" >&2
            sleep 60
            continue
        fi
        full=$(printf '%s' "$info" | jq '[.[0].backup[]? | select(.type=="full") | .timestamp.stop] | max // 0')
        latest=$(printf '%s' "$info" | jq '[.[0].backup[]? | .timestamp.stop] | max // 0')
        if [ "$((now - full))" -ge 86400 ]; then
            backup full || true
        elif [ "$((now - latest))" -ge 21600 ]; then
            backup diff || true
        else
            checkpoint || echo "Cannot archive WAL or publish backup status" >&2
        fi
        sleep 60
    done
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
    [ -z "${SCHEDULE:-}" ] || kill "$SCHEDULE" 2>/dev/null || true
    [ -z "${POOLER:-}" ] || kill "$POOLER" 2>/dev/null || true
    if gosu postgres pg_ctl -D "$PGDATA" status >/dev/null 2>&1; then
        gosu postgres pg_ctl -D "$PGDATA" -m fast -w stop
    fi
}

start() {
    mkdir -p "$BACKUPS/repo" "$PGHOST"
    chown postgres:postgres "$PGHOST"
    repository_config "$BACKUPS"
    # Recreate managed access rules on every start, including crash recovery.
    cat >/tmp/beam-pg_hba.conf <<'EOF'
local all all trust
host all all all scram-sha-256
host replication all all scram-sha-256
EOF

    if [ -n "${BEAM_RESTORE_TIME:-}" ] && [ ! -f "$RESTORED" ]; then
        # This is a new restore disk and has never accepted application writes.
        # A partial restore is retried from its source, never booted as complete.
        rm -rf "$PGDATA"
        mkdir -p "$PGDATA"
        chown postgres:postgres "$PGDATA"
        repository_config /volumes/beam-restore
        backrest --type=time --target="$BEAM_RESTORE_TIME" --target-action=promote restore
    fi

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
    # Keep clients out while crash/PITR recovery and password rotation finish.
    docker-entrypoint.sh postgres -c listen_addresses=127.0.0.1 -c archive_mode=off \
        -c hba_file=/tmp/beam-pg_hba.conf &
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
    if [ -n "${BEAM_RESTORE_TIME:-}" ]; then
        touch "$RESTORED"
        sync "$RESTORED" /var/lib/postgresql/data
    fi
    repository_config "$BACKUPS"

    docker-entrypoint.sh postgres -c listen_addresses='*' -c wal_compression=on \
        -c hba_file=/tmp/beam-pg_hba.conf \
        -c fsync=on -c synchronous_commit=on -c full_page_writes=on \
        -c archive_mode=on -c archive_timeout=60 \
        -c "archive_command=pgbackrest --config=$CONFIG --stanza=db archive-push %p" &
    POSTGRES=$!
    for attempt in $(seq 1 60); do
        sql -Atc 'SELECT 1' >/dev/null 2>&1 && break
        kill -0 "$POSTGRES" || { wait "$POSTGRES"; exit 1; }
        sleep 1
    done
    backrest stanza-create
    start_pooler
    schedule &
    SCHEDULE=$!
    wait "$POSTGRES" || status=$?
    shutdown
    exit "${status:-0}"
}

case "${1:-start}" in
    start) start ;;
    backup) backup "${2:-full}" ;;
    checkpoint) checkpoint ;;
    *) echo "Expected start, backup, or checkpoint" >&2; exit 2 ;;
esac
