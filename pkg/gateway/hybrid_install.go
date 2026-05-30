package gateway

import (
	"net/http"

	"github.com/labstack/echo/v4"
)

const agentInstallScript = `#!/usr/bin/env sh
set -eu

GATEWAY=""
JOIN_TOKEN=""
DEV="0"
AGENT_BIN="${BEAM_AGENT_BIN:-}"
AGENT_CONTAINER_IMAGE="${BEAM_AGENT_CONTAINER_IMAGE:-docker:27-dind}"
TRANSPORT=""
EXECUTOR="${BEAM_AGENT_EXECUTOR:-}"
WORKER_IMAGE="${BEAM_WORKER_IMAGE:-}"
MAX_CPU=""
MAX_MEMORY=""
MAX_GPUS=""
GPU_IDS=""
NETWORK_SLOTS=""
CONTAINER_START_CONCURRENCY=""

while [ "$#" -gt 0 ]; do
  case "$1" in
    --gateway)
      GATEWAY="$2"
      shift 2
      ;;
    --join-token)
      JOIN_TOKEN="$2"
      shift 2
      ;;
    --dev)
      DEV="1"
      shift
      ;;
    --agent-bin)
      AGENT_BIN="$2"
      shift 2
      ;;
    --transport)
      TRANSPORT="$2"
      shift 2
      ;;
    --executor)
      EXECUTOR="$2"
      shift 2
      ;;
    --worker-image)
      WORKER_IMAGE="$2"
      shift 2
      ;;
    --max-cpu)
      MAX_CPU="$2"
      shift 2
      ;;
    --max-memory)
      MAX_MEMORY="$2"
      shift 2
      ;;
    --max-gpus)
      MAX_GPUS="$2"
      shift 2
      ;;
    --gpu-ids)
      GPU_IDS="$2"
      shift 2
      ;;
    --network-slots)
      NETWORK_SLOTS="$2"
      shift 2
      ;;
    --container-start-concurrency)
      CONTAINER_START_CONCURRENCY="$2"
      shift 2
      ;;
    *)
      echo "unknown argument: $1" >&2
      exit 2
      ;;
  esac
done

if [ -z "$GATEWAY" ] || [ -z "$JOIN_TOKEN" ]; then
  echo "usage: install/agent --gateway <url> --join-token <token>" >&2
  exit 2
fi

if ! command -v curl >/dev/null 2>&1; then
  echo "curl is required" >&2
  exit 1
fi

OS="$(uname -s | tr '[:upper:]' '[:lower:]')"
ARCH="$(uname -m)"
case "$ARCH" in
  x86_64|amd64) ARCH="amd64" ;;
  aarch64|arm64) ARCH="arm64" ;;
esac

if [ "$DEV" = "1" ] && [ -z "$WORKER_IMAGE" ]; then
  WORKER_IMAGE="${BEAM_DEV_WORKER_IMAGE:-localhost:5001/beta9-worker:latest}"
fi

RUN_GATEWAY="$GATEWAY"
if [ "$OS" = "darwin" ] && [ -z "$AGENT_BIN" ] && [ "${BEAM_AGENT_NATIVE:-0}" != "1" ]; then
  RUN_GATEWAY="$(printf '%s' "$GATEWAY" | sed -e 's#://localhost:#://host.docker.internal:#' -e 's#://localhost/#://host.docker.internal/#' -e 's#://127\.0\.0\.1:#://host.docker.internal:#' -e 's#://127\.0\.0\.1/#://host.docker.internal/#')"
fi

set -- join --gateway "$RUN_GATEWAY" --join-token "$JOIN_TOKEN"
if [ "$DEV" = "1" ]; then
  set -- "$@" --dev
fi
if [ -n "$TRANSPORT" ]; then
  set -- "$@" --transport "$TRANSPORT"
fi
if [ -n "$EXECUTOR" ]; then
  set -- "$@" --executor "$EXECUTOR"
fi
if [ -n "$WORKER_IMAGE" ]; then
  set -- "$@" --worker-image "$WORKER_IMAGE"
fi
if [ -n "$MAX_CPU" ]; then
  set -- "$@" --max-cpu "$MAX_CPU"
fi
if [ -n "$MAX_MEMORY" ]; then
  set -- "$@" --max-memory "$MAX_MEMORY"
fi
if [ -n "$MAX_GPUS" ]; then
  set -- "$@" --max-gpus "$MAX_GPUS"
fi
if [ -n "$GPU_IDS" ]; then
  set -- "$@" --gpu-ids "$GPU_IDS"
fi
if [ -n "$NETWORK_SLOTS" ]; then
  set -- "$@" --network-slots "$NETWORK_SLOTS"
fi
if [ -n "$CONTAINER_START_CONCURRENCY" ]; then
  set -- "$@" --container-start-concurrency "$CONTAINER_START_CONCURRENCY"
fi

if [ -n "$AGENT_BIN" ]; then
  exec "$AGENT_BIN" "$@"
fi

if [ "$OS" = "darwin" ] && [ "${BEAM_AGENT_NATIVE:-0}" != "1" ]; then
  if ! command -v docker >/dev/null 2>&1; then
    echo "Docker is required on macOS for agent worker-container execution" >&2
    exit 1
  fi

  AGENT_LINUX_BIN="${BEAM_AGENT_LINUX_BIN:-${HOME}/.beam/bin/beam-agent-linux-${ARCH}}"
  HOST_STATE_DIR="${BEAM_AGENT_STATE_DIR:-${HOME}/.beam/agent}"
  HOSTNAME_VALUE="$(hostname 2>/dev/null || echo macos-agent)"
  HOST_FINGERPRINT="${BEAM_AGENT_MACHINE_FINGERPRINT:-}"
  if [ -z "$HOST_FINGERPRINT" ] && command -v ioreg >/dev/null 2>&1; then
    HOST_FINGERPRINT="$(ioreg -rd1 -c IOPlatformExpertDevice 2>/dev/null | awk -F'"' '/IOPlatformUUID/{print $(NF-1); exit}')"
  fi
  if [ -z "$HOST_FINGERPRINT" ]; then
    HOST_FINGERPRINT="$HOSTNAME_VALUE"
  fi
  mkdir -p "$(dirname "$AGENT_LINUX_BIN")" "$HOST_STATE_DIR"

  if [ "$DEV" = "1" ] && [ -d "./cmd/agent" ] && command -v go >/dev/null 2>&1; then
    GOOS=linux GOARCH="$ARCH" CGO_ENABLED=0 go build -o "$AGENT_LINUX_BIN" ./cmd/agent
  elif [ ! -x "$AGENT_LINUX_BIN" ]; then
    DOWNLOAD_URL="${BEAM_AGENT_URL:-https://github.com/beam-cloud/beta9/releases/latest/download/agent-linux-${ARCH}}"
    TMP="$(mktemp)"
    curl -fsSL "$DOWNLOAD_URL" -o "$TMP"
    chmod 0755 "$TMP"
    mv "$TMP" "$AGENT_LINUX_BIN"
  fi

  echo "starting Linux beam-agent in Docker on macOS" >&2
  exec docker run --rm \
    --privileged \
    --network host \
    -v /var/run/docker.sock:/var/run/docker.sock \
    -v "$AGENT_LINUX_BIN:/usr/local/bin/beam-agent:ro" \
    -v "$HOST_STATE_DIR:$HOST_STATE_DIR" \
    -e BEAM_AGENT_CONTAINER=1 \
    -e BEAM_AGENT_HOSTNAME="$HOSTNAME_VALUE" \
    -e BEAM_AGENT_MACHINE_FINGERPRINT="$HOST_FINGERPRINT" \
    -e BEAM_AGENT_STATE_DIR="$HOST_STATE_DIR" \
    -e BEAM_WORKER_IMAGE="$WORKER_IMAGE" \
    -e BEAM_AGENT_WORKSPACE_STORAGE_ENDPOINT_URL \
    -e BEAM_AGENT_WORKSPACE_STORAGE_ENDPOINT_PORT \
    -e BEAM_AGENT_WORKSPACE_STORAGE_ENDPOINT_REWRITE_HOSTS \
    -e BEAM_AGENT_OCI_REGISTRY_REWRITE \
    -e BEAM_AGENT_OCI_REGISTRY_ENDPOINT_PORT \
    "$AGENT_CONTAINER_IMAGE" /usr/local/bin/beam-agent "$@"
fi

if command -v beam-agent >/dev/null 2>&1; then
  exec beam-agent "$@"
fi

if [ "$DEV" = "1" ] && [ -d "./cmd/agent" ] && command -v go >/dev/null 2>&1; then
  exec go run ./cmd/agent "$@"
fi

if [ "$DEV" = "1" ]; then
  BIN="${HOME}/.beam/bin/beam-agent"
  mkdir -p "$(dirname "$BIN")"
else
  BIN="/usr/local/bin/beam-agent"
fi
DOWNLOAD_URL="${BEAM_AGENT_URL:-https://github.com/beam-cloud/beta9/releases/latest/download/agent-${OS}-${ARCH}}"

if [ ! -x "$BIN" ]; then
  TMP="$(mktemp)"
  curl -fsSL "$DOWNLOAD_URL" -o "$TMP"
  chmod 0755 "$TMP"
  mv "$TMP" "$BIN"
fi

exec "$BIN" "$@"
`

func agentInstallScriptHandler() echo.HandlerFunc {
	return func(c echo.Context) error {
		c.Response().Header().Set(echo.HeaderContentType, "text/x-shellscript; charset=utf-8")
		return c.String(http.StatusOK, agentInstallScript)
	}
}
