package gateway

import (
	"net/http"

	"github.com/labstack/echo/v4"
)

const hybridWorkerInstallScript = `#!/usr/bin/env sh
set -eu

GATEWAY=""
JOIN_TOKEN=""
DEV="0"
AGENT_BIN="${BEAM_AGENT_BIN:-}"
LISTEN=""
ADVERTISE_HOST=""
TRANSPORT=""

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
    --listen)
      LISTEN="$2"
      shift 2
      ;;
    --advertise-host)
      ADVERTISE_HOST="$2"
      shift 2
      ;;
    --transport)
      TRANSPORT="$2"
      shift 2
      ;;
    *)
      echo "unknown argument: $1" >&2
      exit 2
      ;;
  esac
done

if [ -z "$GATEWAY" ] || [ -z "$JOIN_TOKEN" ]; then
  echo "usage: install/hybrid-worker --gateway <url> --join-token <token>" >&2
  exit 2
fi

if ! command -v curl >/dev/null 2>&1; then
  echo "curl is required" >&2
  exit 1
fi

set -- join --gateway "$GATEWAY" --join-token "$JOIN_TOKEN"
if [ "$DEV" = "1" ]; then
  set -- "$@" --dev
fi
if [ -n "$LISTEN" ]; then
  set -- "$@" --listen "$LISTEN"
fi
if [ -n "$ADVERTISE_HOST" ]; then
  set -- "$@" --advertise-host "$ADVERTISE_HOST"
fi
if [ -n "$TRANSPORT" ]; then
  set -- "$@" --transport "$TRANSPORT"
fi

if [ -n "$AGENT_BIN" ]; then
  exec "$AGENT_BIN" "$@"
fi

if command -v beam-agent >/dev/null 2>&1; then
  exec beam-agent "$@"
fi

if [ "$DEV" = "1" ] && [ -d "./cmd/agent" ] && command -v go >/dev/null 2>&1; then
  exec go run ./cmd/agent "$@"
fi

OS="$(uname -s | tr '[:upper:]' '[:lower:]')"
ARCH="$(uname -m)"
case "$ARCH" in
  x86_64|amd64) ARCH="amd64" ;;
  aarch64|arm64) ARCH="arm64" ;;
esac

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

func hybridWorkerInstallScriptHandler() echo.HandlerFunc {
	return func(c echo.Context) error {
		c.Response().Header().Set(echo.HeaderContentType, "text/x-shellscript; charset=utf-8")
		return c.String(http.StatusOK, hybridWorkerInstallScript)
	}
}
