#!/usr/bin/env bash
# The postStart hook already started the packaged gateway before source sync.
# Compile beside it, then replace it only after a successful, stable build.
set -euo pipefail

runtime_dir=${HOSTED_DEV_RUNTIME_DIR:-/var/tmp/beta9-hosted-dev}
bootstrap_binary=${HOSTED_DEV_BOOTSTRAP_BINARY:-/usr/local/bin/gateway}
bootstrap_config=${HOSTED_DEV_BOOTSTRAP_CONFIG:-/var/tmp/beta9-hosted-dev/bootstrap.yaml}
health_url=${HOSTED_DEV_HEALTH_URL:-http://127.0.0.1:1994/api/v1/health}
retry_seconds=${HOSTED_DEV_RETRY_SECONDS:-15}
[[ "$retry_seconds" =~ ^[1-9][0-9]*$ ]] || { echo 'HOSTED_DEV_RETRY_SECONDS must be a positive integer.'; exit 1; }
mkdir -p "$runtime_dir"
# Detaching the local terminal must not stop source reload or the gateway.
trap '' HUP
exec </dev/null
if [[ ${HOSTED_DEV_LOG_STDOUT:-false} != true ]]; then
    exec >/proc/1/fd/1 2>&1
fi

# Reattaching an Okteto terminal must not start another watcher or gateway.
exec 9>"$runtime_dir/reload.lock"
# A short-lived sleep child may still own the descriptor after terminal exit.
flock -w 2 9 || { echo 'A hosted gateway source watcher is already running.'; exit 1; }
printf '%s\n' "$$" >"$runtime_dir/reload.pid"

log() { printf '[hosted-dev] %s\n' "$*"; }
fingerprint() {
    {
        find cmd pkg proto -type f ! -name '*_test.go' ! -name '.st*' \
            -printf '%p %T@ %s\n' | LC_ALL=C sort
        sha256sum go.mod go.sum
        [[ ! -f "$runtime_dir/cache-ready" ]] || cat "$runtime_dir/cache-ready"
    } | sha256sum | cut -d' ' -f1
}
alive() {
    [[ -n ${gateway_pid:-} ]] && kill -0 "$gateway_pid" 2>/dev/null &&
        [[ $(ps -o stat= -p "$gateway_pid") != Z* ]]
}
start_gateway() {
    if [[ "$1" == "$bootstrap_binary" ]]; then
        CONFIG_PATH="$bootstrap_config" nohup "$1" 9>&- </dev/null &
    else
        nohup "$1" 9>&- </dev/null &
    fi
    gateway_pid=$!
    printf '%s\n' "$gateway_pid" >"$runtime_dir/gateway.pid"
}
stop_gateway() {
    if alive; then
        kill -TERM "$gateway_pid"
        for ((attempt = 0; attempt < 100; attempt++)); do
            alive || break
            sleep 0.1
        done
        if alive; then
            log 'Graceful shutdown exceeded 10 seconds; stopping the old gateway.'
            kill -KILL "$gateway_pid"
        fi
    fi
    wait "${gateway_pid:-}" 2>/dev/null || true
}
healthy() {
    for ((attempt = 0; attempt < 30; attempt++)); do
        alive || return 1
        if curl -fsS --max-time 1 "$health_url" >/dev/null 2>&1; then
            return 0
        fi
        sleep 0.5
    done
    return 1
}

gateway_pid=$(cat "$runtime_dir/gateway.pid" 2>/dev/null || true)
current_binary=$bootstrap_binary
if [[ -f "$runtime_dir/current-binary" ]]; then
    current_binary=$(cat "$runtime_dir/current-binary")
fi
[[ -x "$current_binary" ]] || current_binary=$bootstrap_binary
alive || start_gateway "$current_binary"
successful_fingerprint=$(cat "$runtime_dir/current-fingerprint" 2>/dev/null || true)
attempted_fingerprint=
retry_after=0
log 'Watching source; the current gateway keeps serving during compilation.'

while true; do
    if ! alive; then
        log 'Gateway exited; restarting the last working binary.'
        start_gateway "$current_binary"
    fi
    if [[ -e "$runtime_dir/build-hold" ]]; then
        sleep 1
        continue
    fi
    next_fingerprint=$(fingerprint)
    if [[ "$next_fingerprint" == "$successful_fingerprint" ]] ||
        { [[ "$next_fingerprint" == "$attempted_fingerprint" ]] && (( SECONDS < retry_after )); }; then
        sleep 1
        continue
    fi
    # Wait for the whole sync batch rather than compiling each arriving file.
    sleep 1
    [[ "$next_fingerprint" == "$(fingerprint)" ]] || continue
    attempted_fingerprint=$next_fingerprint
    candidate="$runtime_dir/gateway.$next_fingerprint"
    log 'Building changed gateway source.'
    if ! GOMAXPROCS="${HOSTED_DEV_BUILD_JOBS:-4}" nice -n 10 go build -mod=readonly \
        -p "${HOSTED_DEV_BUILD_JOBS:-4}" -o "$candidate" ./cmd/gateway 9>&-; then
        log "Build failed; the serving gateway was not interrupted. Retrying in $retry_seconds seconds."
        rm -f "$candidate"
        retry_after=$((SECONDS + retry_seconds))
        continue
    fi
    if [[ "$next_fingerprint" != "$(fingerprint)" ]]; then
        log 'Source changed during compilation; building the newest source first.'
        rm -f "$candidate"
        continue
    fi
    if [[ -e "$runtime_dir/build-hold" ]]; then
        log 'Build is held; keeping the current gateway until the hold is removed.'
        rm -f "$candidate"
        continue
    fi
    log 'Build succeeded; restarting the gateway.'
    stop_gateway
    start_gateway "$candidate"
    if healthy; then
        old_binary=$current_binary
        current_binary=$candidate
        successful_fingerprint=$next_fingerprint
        printf '%s\n' "$current_binary" >"$runtime_dir/current-binary"
        printf '%s\n' "$next_fingerprint" >"$runtime_dir/current-fingerprint"
        if [[ "$old_binary" == "$runtime_dir/"* && "$old_binary" != "$current_binary" ]]; then
            rm -f "$old_binary"
        fi
        log "Gateway is healthy; source fingerprint $next_fingerprint."
    else
        log 'New gateway did not become healthy; restoring the last working binary.'
        stop_gateway
        rm -f "$candidate"
        start_gateway "$current_binary"
        retry_after=$((SECONDS + retry_seconds))
        log "Retrying the changed gateway in $retry_seconds seconds."
    fi
done
