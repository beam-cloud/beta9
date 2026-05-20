#!/usr/bin/env bash
set -euo pipefail

GATEWAY_URL="${GATEWAY_URL:-http://localhost:1994}"
API_PREFIX="${API_PREFIX:-/api/v1/gateway}"
HEALTH_PATH="${HEALTH_PATH:-/api/v1/health}"
TOKEN="${TOKEN:-${BETA9_TOKEN:-}}"
STUB_ID="${STUB_ID:-}"
IMAGE_ID="${IMAGE_ID:-}"
TIMEOUT_SECONDS="${TIMEOUT_SECONDS:-120}"
POLL_SECONDS="${POLL_SECONDS:-1}"
RESET_WORKERS="${RESET_WORKERS:-0}"

started_at="$(date +%s)"

timeline() {
  local label="$1"
  local now
  now="$(date +%s)"
  printf "%5ss  %s\n" "$((now - started_at))" "$label"
}

require_cmd() {
  command -v "$1" >/dev/null 2>&1 || {
    echo "missing required command: $1" >&2
    exit 1
  }
}

curl_json() {
  local method="$1"
  local path="$2"
  local body="${3:-}"

  if [[ -n "$body" ]]; then
    curl -fsS -X "$method" \
      -H "Authorization: Bearer ${TOKEN}" \
      -H "Content-Type: application/json" \
      --data "$body" \
      "${GATEWAY_URL}${API_PREFIX}${path}"
  else
    curl -fsS -X "$method" \
      -H "Authorization: Bearer ${TOKEN}" \
      "${GATEWAY_URL}${API_PREFIX}${path}"
  fi
}

redis_cli() {
  if ! command -v kubectl >/dev/null 2>&1; then
    return 0
  fi

  local pod
  pod="$(kubectl get pod -l app.kubernetes.io/name=redis -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || true)"
  if [[ -z "$pod" ]]; then
    pod="$(kubectl get pod redis-master-0 -o jsonpath='{.metadata.name}' 2>/dev/null || true)"
  fi
  if [[ -n "$pod" ]]; then
    kubectl exec "$pod" -- redis-cli "$@" 2>/dev/null || true
  fi
}

print_observations() {
  timeline "observations"

  if command -v kubectl >/dev/null 2>&1; then
    echo
    echo "kubectl jobs,pods"
    kubectl get jobs,pods 2>/dev/null || true
  fi

  echo
  echo "redis scheduler backlog"
  redis_cli zcard scheduler:container_requests || true

  echo
  echo "redis worker queues"
  redis_cli keys 'scheduler:worker:*:requests' | while read -r key; do
    [[ -z "$key" ]] && continue
    printf "%s " "$key"
    redis_cli llen "$key" || true
  done
}

usage() {
  cat <<EOF
Usage:
  TOKEN=<token> STUB_ID=<stub-id> hack/startup_probe.sh

Optional:
  GATEWAY_URL=http://localhost:1994
  API_PREFIX=/api/v1/gateway
  IMAGE_ID=<image-id override>
  RESET_WORKERS=1
  TIMEOUT_SECONDS=120

The probe creates a pod through the gateway HTTP path and polls sandbox status.
It prints a single timeline plus best-effort kubectl and Redis observations.
EOF
}

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
  usage
  exit 0
fi

require_cmd curl

if [[ -z "$TOKEN" ]]; then
  echo "TOKEN or BETA9_TOKEN is required" >&2
  usage
  exit 1
fi

if [[ -z "$STUB_ID" ]]; then
  echo "STUB_ID is required" >&2
  usage
  exit 1
fi

if [[ "$RESET_WORKERS" == "1" ]]; then
  timeline "reset workers"
  bash bin/delete_workers.sh
fi

timeline "wait gateway health"
deadline=$((started_at + TIMEOUT_SECONDS))
until curl -fsS "${GATEWAY_URL}${HEALTH_PATH}" >/dev/null; do
  if (( "$(date +%s)" > deadline )); then
    echo "gateway health check timed out: ${GATEWAY_URL}${HEALTH_PATH}" >&2
    exit 1
  fi
  sleep "$POLL_SECONDS"
done

timeline "authorize token"
curl_json POST "/auth/authorize" '{}' >/dev/null

timeline "create pod request"
payload="{\"stubId\":\"${STUB_ID}\""
if [[ -n "$IMAGE_ID" ]]; then
  payload+=",\"imageId\":\"${IMAGE_ID}\""
fi
payload+="}"

response="$(curl_json POST "/pods" "$payload")"
echo "$response"

container_id="$(printf "%s" "$response" | sed -n 's/.*"container_id"[[:space:]]*:[[:space:]]*"\([^"]*\)".*/\1/p')"
if [[ -z "$container_id" ]]; then
  container_id="$(printf "%s" "$response" | sed -n 's/.*"containerId"[[:space:]]*:[[:space:]]*"\([^"]*\)".*/\1/p')"
fi
if [[ -z "$container_id" ]]; then
  echo "could not parse container id from create response" >&2
  print_observations
  exit 1
fi

timeline "accepted container=${container_id}"

while true; do
  now="$(date +%s)"
  if (( now > deadline )); then
    echo "container did not reach RUNNING before timeout" >&2
    print_observations
    exit 1
  fi

  status_response="$(curl_json GET "/pods/${container_id}/status" || true)"
  status="$(printf "%s" "$status_response" | sed -n 's/.*"status"[[:space:]]*:[[:space:]]*"\([^"]*\)".*/\1/p')"
  if [[ -z "$status" ]]; then
    status="unknown"
  fi
  timeline "status ${status}"

  if [[ "$status" == "RUNNING" || "$status" == "running" ]]; then
    break
  fi

  sleep "$POLL_SECONDS"
done

print_observations
