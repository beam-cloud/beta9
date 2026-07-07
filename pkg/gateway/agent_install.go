package gateway

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/labstack/echo/v4"
)

const agentInstallScript = `#!/usr/bin/env sh
set -eu

# This script intentionally mirrors polished installers like Tailscale's:
# parse and validate first, then dispatch to one clear platform path from main.

GATEWAY=""
JOIN_TOKEN=""
DEV="0"
AGENT_BIN="${BEAM_AGENT_BIN:-}"
AGENT_CONTAINER_IMAGE="${BEAM_AGENT_CONTAINER_IMAGE:-docker:27-dind}"
AGENT_DOWNLOAD_URL="${BEAM_AGENT_URL:-}"
BACKGROUND="${BEAM_AGENT_BACKGROUND:-auto}"
SERVICE_MANAGER="${BEAM_AGENT_SERVICE_MANAGER:-auto}"
SERVICE_NAME="${BEAM_AGENT_SERVICE_NAME:-beam-agent}"
STATE_DIR="${BEAM_AGENT_STATE_DIR:-}"
CACHE_DIR="${BEAM_AGENT_CACHE_DIR:-}"
INSTALL_DOCKER="${BEAM_AGENT_INSTALL_DOCKER:-auto}"
TRANSPORT=""
EXECUTOR="${BEAM_AGENT_EXECUTOR:-}"
WORKER_IMAGE="${BEAM_WORKER_IMAGE:-}"
MAX_CPU=""
MAX_MEMORY=""
MAX_GPUS=""
GPU_IDS=""
NETWORK_SLOTS=""
CONTAINER_START_CONCURRENCY=""
OS=""
ARCH=""
RUN_GATEWAY=""
LOCAL_DEV_GATEWAY="0"

main() {
  parse_args "$@"
  detect_platform
  validate_input
  configure_dev_environment

  set -- join --gateway "$RUN_GATEWAY" --join-token "$JOIN_TOKEN"
  [ "$DEV" = "1" ] && set -- "$@" --dev
  [ -n "$TRANSPORT" ] && set -- "$@" --transport "$TRANSPORT"
  [ -n "$EXECUTOR" ] && set -- "$@" --executor "$EXECUTOR"
  [ -n "$WORKER_IMAGE" ] && set -- "$@" --worker-image "$WORKER_IMAGE"
  [ -n "$CACHE_DIR" ] && set -- "$@" --cache-dir "$CACHE_DIR"
  [ -n "$MAX_CPU" ] && set -- "$@" --max-cpu "$MAX_CPU"
  [ -n "$MAX_MEMORY" ] && set -- "$@" --max-memory "$MAX_MEMORY"
  [ -n "$MAX_GPUS" ] && set -- "$@" --max-gpus "$MAX_GPUS"
  [ -n "$GPU_IDS" ] && set -- "$@" --gpu-ids "$GPU_IDS"
  [ -n "$NETWORK_SLOTS" ] && set -- "$@" --network-slots "$NETWORK_SLOTS"
  [ -n "$CONTAINER_START_CONCURRENCY" ] && set -- "$@" --container-start-concurrency "$CONTAINER_START_CONCURRENCY"

  if [ -n "$AGENT_BIN" ]; then
    run_agent "$AGENT_BIN" "$@"
  fi

  if use_macos_docker_runtime; then
    run_macos_docker_agent "$@"
  fi

  ensure_linux_docker

  if use_source_agent; then
    say "Starting Beam agent from source"
    exec go run ./cmd/agent "$@"
  fi

  BIN="$(native_agent_path)"
  install_agent_binary "$BIN" "$OS" "$ARCH"
  run_agent "$BIN" "$@"
}

parse_args() {
  while [ "$#" -gt 0 ]; do
    case "$1" in
      --gateway) require_value "$1" "${2:-}"; GATEWAY="$2"; shift 2 ;;
      --join-token) require_value "$1" "${2:-}"; JOIN_TOKEN="$2"; shift 2 ;;
      --dev) DEV="1"; shift ;;
      --agent-bin) require_value "$1" "${2:-}"; AGENT_BIN="$2"; shift 2 ;;
      --background) BACKGROUND="1"; shift ;;
      --foreground) BACKGROUND="0"; shift ;;
      --service-manager) require_value "$1" "${2:-}"; SERVICE_MANAGER="$2"; shift 2 ;;
      --service-name) require_value "$1" "${2:-}"; SERVICE_NAME="$2"; shift 2 ;;
      --state-dir) require_value "$1" "${2:-}"; STATE_DIR="$2"; shift 2 ;;
      --cache-dir) require_value "$1" "${2:-}"; CACHE_DIR="$2"; shift 2 ;;
      --transport) require_value "$1" "${2:-}"; TRANSPORT="$2"; shift 2 ;;
      --executor) require_value "$1" "${2:-}"; EXECUTOR="$2"; shift 2 ;;
      --worker-image) require_value "$1" "${2:-}"; WORKER_IMAGE="$2"; shift 2 ;;
      --max-cpu) require_value "$1" "${2:-}"; MAX_CPU="$2"; shift 2 ;;
      --max-memory) require_value "$1" "${2:-}"; MAX_MEMORY="$2"; shift 2 ;;
      --max-gpus) require_value "$1" "${2:-}"; MAX_GPUS="$2"; shift 2 ;;
      --gpu-ids) require_value "$1" "${2:-}"; GPU_IDS="$2"; shift 2 ;;
      --network-slots) require_value "$1" "${2:-}"; NETWORK_SLOTS="$2"; shift 2 ;;
      --container-start-concurrency) require_value "$1" "${2:-}"; CONTAINER_START_CONCURRENCY="$2"; shift 2 ;;
      *) fail "unknown argument: $1" 2 ;;
    esac
  done
}

detect_platform() {
  OS="$(uname -s | tr '[:upper:]' '[:lower:]')"
  ARCH="$(uname -m)"
  case "$ARCH" in
    x86_64|amd64) ARCH="amd64" ;;
    aarch64|arm64) ARCH="arm64" ;;
  esac
}

validate_input() {
  if [ -z "$GATEWAY" ] || [ -z "$JOIN_TOKEN" ]; then
    fail "usage: install/agent --gateway <url> --join-token <token>" 2
  fi
  case "$GATEWAY" in
    http://*|https://*) ;;
    *) fail "--gateway must start with http:// or https://" 2 ;;
  esac
  if [ "$ARCH" != "amd64" ] && [ "$ARCH" != "arm64" ]; then
    fail "unsupported architecture: $ARCH" 1
  fi
  if [ -n "$STATE_DIR" ]; then
    export BEAM_AGENT_STATE_DIR="$STATE_DIR"
  fi
  if [ -n "$CACHE_DIR" ]; then
    export BEAM_AGENT_CACHE_DIR="$CACHE_DIR"
  fi
}

configure_dev_environment() {
  RUN_GATEWAY="$GATEWAY"
  LOCAL_DEV_GATEWAY="0"

  if [ "$DEV" = "1" ] && [ -z "$WORKER_IMAGE" ]; then
    WORKER_IMAGE="${BEAM_DEV_WORKER_IMAGE:-localhost:5001/beta9-worker:latest}"
  fi

  case "$GATEWAY" in
    http://localhost:*|http://localhost/*|http://127.0.0.1:*|http://127.0.0.1/*)
      LOCAL_DEV_GATEWAY="1"
      ;;
  esac

  if [ "$DEV" = "1" ] && [ "$LOCAL_DEV_GATEWAY" = "1" ]; then
    configure_local_gateway_aliases
  fi

  if use_macos_docker_runtime; then
    RUN_GATEWAY="$(docker_reachable_url "$GATEWAY")"
  fi
}

configure_local_gateway_aliases() {
  REGISTRY_PORT="${BEAM_AGENT_OCI_REGISTRY_ENDPOINT_PORT:-5001}"
  if [ -z "${BEAM_AGENT_DOCKER_HOST_ALIASES:-}" ]; then
    export BEAM_AGENT_DOCKER_HOST_ALIASES="registry.localhost:127.0.0.1,localstack:host-gateway,localstack.beta9:host-gateway,localstack.beta9.svc:host-gateway,localstack.beta9.svc.cluster.local:host-gateway"
  fi
  if [ -z "${BEAM_AGENT_LOCAL_REGISTRY_FORWARD:-}" ]; then
    if use_macos_docker_runtime; then
      export BEAM_AGENT_LOCAL_REGISTRY_FORWARD="host.docker.internal:${REGISTRY_PORT}"
    else
      export BEAM_AGENT_LOCAL_REGISTRY_FORWARD="127.0.0.1:${REGISTRY_PORT}"
    fi
  fi
}

use_macos_docker_runtime() {
  [ "$OS" = "darwin" ] && [ "${BEAM_AGENT_NATIVE:-0}" != "1" ] && [ -z "$AGENT_BIN" ]
}

use_source_agent() {
  [ "$DEV" = "1" ] &&
    ! should_install_service &&
    [ -d "./cmd/agent" ] &&
    command -v go >/dev/null 2>&1
}

should_install_service() {
  if [ "$BACKGROUND" = "1" ]; then
    return 0
  fi
  if [ "$BACKGROUND" = "0" ] || [ "$DEV" = "1" ]; then
    return 1
  fi
  if [ "$OS" = "darwin" ] && [ "${BEAM_AGENT_NATIVE:-0}" != "1" ]; then
    return 1
  fi
  return 0
}

run_agent() {
  AGENT_RUN_BIN="$1"
  shift
  if should_install_service; then
    say "Installing Beam agent service"
    if [ "${1:-}" = "join" ]; then
      shift
    fi
    set -- install-service --manager "$SERVICE_MANAGER" --service-name "$SERVICE_NAME" "$@"
    if [ -n "$STATE_DIR" ]; then
      set -- "$@" --state-dir "$STATE_DIR"
    fi
  else
    say "Starting Beam agent"
  fi
  exec "$AGENT_RUN_BIN" "$@"
}

run_macos_docker_agent() {
  if should_install_service; then
    fail "--background on macOS requires BEAM_AGENT_NATIVE=1; the Docker runtime runs in the foreground" 1
  fi

  require_docker_desktop
  HOST_HOME="$(agent_host_home)"
  AGENT_LINUX_BIN="${BEAM_AGENT_LINUX_BIN:-${HOST_HOME}/.beam/bin/beam-agent-linux-${ARCH}}"
  HOST_STATE_DIR="${BEAM_AGENT_STATE_DIR:-${HOST_HOME}/.beam/agent}"
  HOST_CACHE_DIR="${BEAM_AGENT_CACHE_DIR:-${HOST_STATE_DIR}/cache}"
  HOSTNAME_VALUE="$(hostname 2>/dev/null || echo macos-agent)"
  HOST_FINGERPRINT="$(host_fingerprint "$HOSTNAME_VALUE")"
  if [ -z "${BEAM_AGENT_WORKER_PLATFORM:-}" ]; then
    export BEAM_AGENT_WORKER_PLATFORM="linux/amd64"
  fi

  mkdir -p "$(dirname "$AGENT_LINUX_BIN")" "$HOST_STATE_DIR" "$HOST_CACHE_DIR"
  install_linux_agent_for_docker "$AGENT_LINUX_BIN"

  say "Starting Beam agent"
  exec docker run --rm \
    --init \
    --privileged \
    --network host \
    -v /var/run/docker.sock:/var/run/docker.sock \
    -v "$AGENT_LINUX_BIN:/usr/local/bin/beam-agent:ro" \
    -v "$HOST_STATE_DIR:$HOST_STATE_DIR" \
    -v "$HOST_CACHE_DIR:$HOST_CACHE_DIR" \
    -e BEAM_AGENT_CONTAINER=1 \
    -e BEAM_AGENT_HOSTNAME="$HOSTNAME_VALUE" \
    -e BEAM_AGENT_MACHINE_FINGERPRINT="$HOST_FINGERPRINT" \
    -e BEAM_AGENT_STATE_DIR="$HOST_STATE_DIR" \
    -e BEAM_AGENT_CACHE_DIR="$HOST_CACHE_DIR" \
    -e BEAM_WORKER_IMAGE="$WORKER_IMAGE" \
    -e BEAM_AGENT_DOCKER_HOST_ALIASES \
    -e BEAM_AGENT_LOCAL_REGISTRY_FORWARD \
    -e BEAM_AGENT_WORKER_PLATFORM \
    -e BEAM_AGENT_VERBOSE \
    -e NO_COLOR \
    "$AGENT_CONTAINER_IMAGE" /usr/local/bin/beam-agent "$@"
}

require_docker_desktop() {
  say "Checking Docker"
  if ! command -v docker >/dev/null 2>&1; then
    fail "Docker is required on macOS for agent worker-container execution" 1
  fi
  if ! docker info >/dev/null 2>&1; then
    fail "Docker Desktop must be running before joining an agent pool" 1
  fi
  if ! docker image inspect "$AGENT_CONTAINER_IMAGE" >/dev/null 2>&1; then
    say "Downloading agent runtime"
    docker pull -q "$AGENT_CONTAINER_IMAGE" >/dev/null
  fi
  if ! docker run --rm --network host --entrypoint /bin/sh "$AGENT_CONTAINER_IMAGE" -c true >/dev/null 2>&1; then
    fail "Docker host networking is required for local agent workers" 1
  fi
}

install_linux_agent_for_docker() {
  BIN="$1"
  if [ "$DEV" = "1" ] && [ -d "./cmd/agent" ] && command -v go >/dev/null 2>&1; then
    say "Building Beam agent"
    GOOS=linux GOARCH="$ARCH" CGO_ENABLED=0 go build -o "$BIN" ./cmd/agent
    return
  fi

	if [ -n "$AGENT_DOWNLOAD_URL" ]; then
		URL="$AGENT_DOWNLOAD_URL"
	elif [ "$DEV" = "1" ]; then
		URL="${GATEWAY}/install/agent/linux/${ARCH}?dev=1"
	else
		URL="${GATEWAY}/install/agent/linux/${ARCH}"
	fi
	install_from_url "$URL" "$BIN" "Installing Beam agent"
}

ensure_linux_docker() {
  if [ "$OS" != "linux" ] || ! needs_worker_container; then
    return
  fi
  if docker_ready; then
    return
  fi
  if [ "$DEV" = "1" ] || is_false "$INSTALL_DOCKER"; then
    fail "Docker is required for worker-container executor. Install Docker or rerun with BEAM_AGENT_INSTALL_DOCKER=1." 1
  fi

  say "Installing Docker"
  download_to_stdout "https://get.docker.com" | sh
  if command -v systemctl >/dev/null 2>&1; then
    systemctl enable --now docker >/dev/null 2>&1 || systemctl start docker >/dev/null 2>&1 || true
  elif command -v service >/dev/null 2>&1; then
    service docker start >/dev/null 2>&1 || true
  fi
  if ! docker_ready; then
    fail "Docker is installed but not running. Start Docker and rerun this command." 1
  fi
}

needs_worker_container() {
  [ -z "$EXECUTOR" ] || [ "$EXECUTOR" = "worker-container" ]
}

docker_ready() {
  command -v docker >/dev/null 2>&1 && docker info >/dev/null 2>&1
}

native_agent_path() {
  if [ "$DEV" = "1" ]; then
    printf '%s\n' "${HOME}/.beam/bin/beam-agent"
  else
    printf '%s\n' "/usr/local/bin/beam-agent"
  fi
}

install_agent_binary() {
  BIN="$1"
  OS_NAME="$2"
  ARCH_NAME="$3"
  URL="${AGENT_DOWNLOAD_URL:-${GATEWAY}/install/agent/${OS_NAME}/${ARCH_NAME}}"
  install_from_url "$URL" "$BIN" "Installing Beam agent"
}

install_from_url() {
  INSTALL_URL="$1"
  INSTALL_DEST="$2"
  INSTALL_MESSAGE="$3"
  INSTALL_TMP="$(mktemp)"
  say "$INSTALL_MESSAGE"
  if ! download_file "$INSTALL_URL" "$INSTALL_TMP"; then
    rm -f "$INSTALL_TMP"
    fail "Unable to download Beam agent from $INSTALL_URL" 1
  fi
  chmod 0755 "$INSTALL_TMP"
  mkdir -p "$(dirname "$INSTALL_DEST")"
  if [ -d "$INSTALL_DEST" ]; then
    rmdir "$INSTALL_DEST" 2>/dev/null || {
      rm -f "$INSTALL_TMP"
      fail "$INSTALL_DEST is a directory; remove it or set BEAM_AGENT_LINUX_BIN to a file path" 1
    }
  fi
  mv "$INSTALL_TMP" "$INSTALL_DEST"
}

download_file() {
  DOWNLOAD_URL="$1"
  DOWNLOAD_DEST="$2"
  if command -v curl >/dev/null 2>&1; then
    curl -fsSL "$DOWNLOAD_URL" -o "$DOWNLOAD_DEST"
    return
  fi
  if command -v wget >/dev/null 2>&1; then
    wget -q -O "$DOWNLOAD_DEST" "$DOWNLOAD_URL"
    return
  fi
  fail "curl or wget is required to download Beam agent" 1
}

download_to_stdout() {
  STDOUT_URL="$1"
  if command -v curl >/dev/null 2>&1; then
    curl -fsSL "$STDOUT_URL"
    return
  fi
  if command -v wget >/dev/null 2>&1; then
    wget -q -O- "$STDOUT_URL"
    return
  fi
  fail "curl or wget is required to download Docker" 1
}

docker_reachable_url() {
  printf '%s' "$1" |
    sed -e 's#://localhost:#://host.docker.internal:#' \
        -e 's#://localhost/#://host.docker.internal/#' \
        -e 's#://127\.0\.0\.1:#://host.docker.internal:#' \
        -e 's#://127\.0\.0\.1/#://host.docker.internal/#'
}

agent_host_home() {
  if [ -n "${BEAM_AGENT_HOME:-}" ]; then
    printf '%s\n' "$BEAM_AGENT_HOME"
    return
  fi
  if [ "$(id -u)" -eq 0 ] && [ -n "${SUDO_USER:-}" ] && [ "$SUDO_USER" != "root" ]; then
    if command -v dscl >/dev/null 2>&1; then
      HOME_DIR="$(dscl . -read "/Users/${SUDO_USER}" NFSHomeDirectory 2>/dev/null | awk '{print $2; exit}')"
      if [ -n "$HOME_DIR" ]; then
        printf '%s\n' "$HOME_DIR"
        return
      fi
    fi
    if command -v getent >/dev/null 2>&1; then
      HOME_DIR="$(getent passwd "$SUDO_USER" 2>/dev/null | cut -d: -f6)"
      if [ -n "$HOME_DIR" ]; then
        printf '%s\n' "$HOME_DIR"
        return
      fi
    fi
    if [ -d "/Users/${SUDO_USER}" ]; then
      printf '%s\n' "/Users/${SUDO_USER}"
      return
    fi
    if [ -d "/home/${SUDO_USER}" ]; then
      printf '%s\n' "/home/${SUDO_USER}"
      return
    fi
  fi
  printf '%s\n' "$HOME"
}

host_fingerprint() {
  if [ -n "${BEAM_AGENT_MACHINE_FINGERPRINT:-}" ]; then
    printf '%s\n' "$BEAM_AGENT_MACHINE_FINGERPRINT"
    return
  fi
  if command -v ioreg >/dev/null 2>&1; then
    VALUE="$(ioreg -rd1 -c IOPlatformExpertDevice 2>/dev/null | awk -F'"' '/IOPlatformUUID/{print $(NF-1); exit}')"
    if [ -n "$VALUE" ]; then
      printf '%s\n' "$VALUE"
      return
    fi
  fi
  printf '%s\n' "$1"
}

require_value() {
  if [ "$#" -lt 2 ] || [ -z "${2:-}" ]; then
    fail "$1 requires a value" 2
  fi
}

say() {
  printf '=> %s\n' "$*" >&2
}

fail() {
  MSG="$1"
  CODE="${2:-1}"
  printf '%s\n' "$MSG" >&2
  exit "$CODE"
}

is_false() {
  case "$(printf '%s' "${1:-}" | tr '[:upper:]' '[:lower:]')" in
    0|false|no|off|disabled) return 0 ;;
    *) return 1 ;;
  esac
}

main "$@"
`

func agentInstallScriptHandler() echo.HandlerFunc {
	return func(c echo.Context) error {
		c.Response().Header().Set(echo.HeaderContentType, "text/x-shellscript; charset=utf-8")
		c.Response().Header().Set(echo.HeaderCacheControl, "no-store")
		return c.String(http.StatusOK, agentInstallScript)
	}
}

func agentBinaryHandler() echo.HandlerFunc {
	return func(c echo.Context) error {
		osName := strings.ToLower(strings.TrimSpace(c.Param("os")))
		arch := strings.ToLower(strings.TrimSpace(c.Param("arch")))

		path, err := resolveAgentBinary(c.Request().Context(), osName, arch, c.QueryParam("dev") == "1")
		if err != nil {
			return echo.NewHTTPError(http.StatusNotFound, err.Error())
		}

		c.Response().Header().Set(echo.HeaderContentType, "application/octet-stream")
		c.Response().Header().Set(echo.HeaderContentDisposition, `attachment; filename="beam-agent"`)
		c.Response().Header().Set(echo.HeaderCacheControl, "no-store")
		return c.File(path)
	}
}

func resolveAgentBinary(ctx context.Context, osName, arch string, forceBuild bool) (string, error) {
	if forceBuild {
		path, err := buildAgentBinary(ctx, osName, arch)
		if err != nil {
			return "", fmt.Errorf("agent binary source build failed for %s/%s: %w", osName, arch, err)
		}
		return path, nil
	}

	path := agentBinaryPath(osName, arch)
	if fileExists(path) {
		return path, nil
	}

	path, err := buildAgentBinary(ctx, osName, arch)
	if err != nil {
		return "", fmt.Errorf("agent binary is not available for %s/%s: %w", osName, arch, err)
	}
	return path, nil
}

func agentBinaryPath(osName, arch string) string {
	if path := strings.TrimSpace(os.Getenv(types.AgentBinaryPathEnv)); path != "" {
		return path
	}

	if osName != "" && arch != "" {
		path := fmt.Sprintf(types.DefaultAgentBinaryPattern, osName, arch)
		if info, err := os.Stat(path); err == nil && !info.IsDir() {
			return path
		}
		if strings.EqualFold(osName, runtime.GOOS) && strings.EqualFold(arch, runtime.GOARCH) {
			return types.DefaultAgentBinaryPath
		}
		return path
	}
	return types.DefaultAgentBinaryPath
}

func buildAgentBinary(ctx context.Context, osName, arch string) (string, error) {
	if !supportedAgentTarget(osName, arch) {
		return "", fmt.Errorf("unsupported target")
	}

	sourceDir, ok := agentSourceDir()
	if !ok {
		return "", fmt.Errorf("source tree not found")
	}
	if _, err := exec.LookPath("go"); err != nil {
		return "", fmt.Errorf("go toolchain not found")
	}

	cacheDir, err := agentBuildCacheDir()
	if err != nil {
		return "", err
	}
	if err := os.MkdirAll(cacheDir, 0755); err != nil {
		return "", err
	}

	dest := filepath.Join(cacheDir, fmt.Sprintf("beam-agent-%s-%s", osName, arch))
	tmp, err := os.CreateTemp(cacheDir, ".beam-agent-*")
	if err != nil {
		return "", err
	}
	tmpPath := tmp.Name()
	_ = tmp.Close()
	defer os.Remove(tmpPath)

	buildCtx, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()

	cmd := exec.CommandContext(buildCtx, "go", "build", "-o", tmpPath, "./cmd/agent")
	cmd.Dir = sourceDir
	cmd.Env = append(os.Environ(),
		"CGO_ENABLED=0",
		"GOOS="+osName,
		"GOARCH="+arch,
	)
	if output, err := cmd.CombinedOutput(); err != nil {
		return "", fmt.Errorf("build failed: %s", strings.TrimSpace(string(output)))
	}
	if err := os.Chmod(tmpPath, 0755); err != nil {
		return "", err
	}
	if err := os.Rename(tmpPath, dest); err != nil {
		return "", err
	}
	return dest, nil
}

func supportedAgentTarget(osName, arch string) bool {
	switch osName {
	case "linux", "darwin":
	default:
		return false
	}
	return arch == "amd64" || arch == "arm64"
}

func agentSourceDir() (string, bool) {
	if dir := strings.TrimSpace(os.Getenv(types.AgentSourceDirEnv)); dir != "" {
		return dir, agentSourceDirExists(dir)
	}

	dir, err := os.Getwd()
	if err != nil {
		return "", false
	}
	for {
		if agentSourceDirExists(dir) {
			return dir, true
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			return "", false
		}
		dir = parent
	}
}

func agentSourceDirExists(dir string) bool {
	return fileExists(filepath.Join(dir, "go.mod")) && dirExists(filepath.Join(dir, "cmd", "agent"))
}

func agentBuildCacheDir() (string, error) {
	if dir := strings.TrimSpace(os.Getenv(types.AgentBuildCacheDirEnv)); dir != "" {
		return dir, nil
	}
	dir, err := os.UserCacheDir()
	if err != nil {
		return "", err
	}
	return filepath.Join(dir, types.BeamStateDirName, types.AgentStateDirName, "bin"), nil
}

func fileExists(path string) bool {
	info, err := os.Stat(path)
	return err == nil && !info.IsDir()
}

func dirExists(path string) bool {
	info, err := os.Stat(path)
	return err == nil && info.IsDir()
}
