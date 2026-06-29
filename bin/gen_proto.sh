#!/bin/bash

set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"

if [ -z "${PROTOC_INCLUDE_PATH:-}" ]; then
    PROTOC_INCLUDE_PATH="/usr/local/include"
fi

PROTOC_GEN_GO_VERSION="${PROTOC_GEN_GO_VERSION:-v1.31.0}"
PROTOC_GEN_GO_GRPC_VERSION="${PROTOC_GEN_GO_GRPC_VERSION:-v1.3.0}"
GRPC_GATEWAY_VERSION="${GRPC_GATEWAY_VERSION:-v2.27.1}"
GO2PROTO_VERSION="${GO2PROTO_VERSION:-091f2e319b32a829052fdc8f02bbb6561c4f0504}"
PYTHON_BETTERPROTO_VERSION="${PYTHON_BETTERPROTO_VERSION:-2.0.1}"
PYTHON_GRPCIO_VERSION="${PYTHON_GRPCIO_VERSION:-1.69.0}"
PYTHON_BLACK_VERSION="${PYTHON_BLACK_VERSION:-24.8.0}"
PYTHON_ISORT_VERSION="${PYTHON_ISORT_VERSION:-5.13.2}"

proto_tools_bin="${PROTO_TOOLS_BIN:-${XDG_CACHE_HOME:-$HOME/.cache}/beta9/proto-tools/${PROTOC_GEN_GO_VERSION}-${PROTOC_GEN_GO_GRPC_VERSION}-${GRPC_GATEWAY_VERSION}-${GO2PROTO_VERSION}/bin}"
mkdir -p "$proto_tools_bin"
export PATH="$proto_tools_bin:$PATH"

ensure_go_tool_version() {
    local binary="$1"
    local module="$2"
    local version="$3"
    local marker="$proto_tools_bin/.${binary}.version"
    local expected="${module}@${version}"

    if [ -x "$proto_tools_bin/$binary" ] && [ -f "$marker" ] && [ "$(cat "$marker")" = "$expected" ]; then
        return
    fi

    echo "installing $expected"
    GOBIN="$proto_tools_bin" go install "$expected"
    echo "$expected" > "$marker"
}

ensure_go_tool_version protoc-gen-go "google.golang.org/protobuf/cmd/protoc-gen-go" "$PROTOC_GEN_GO_VERSION"
ensure_go_tool_version protoc-gen-go-grpc "google.golang.org/grpc/cmd/protoc-gen-go-grpc" "$PROTOC_GEN_GO_GRPC_VERSION"
ensure_go_tool_version protoc-gen-grpc-gateway "github.com/grpc-ecosystem/grpc-gateway/v2/protoc-gen-grpc-gateway" "$GRPC_GATEWAY_VERSION"
ensure_go_tool_version protoc-gen-openapiv2 "github.com/grpc-ecosystem/grpc-gateway/v2/protoc-gen-openapiv2" "$GRPC_GATEWAY_VERSION"
ensure_go_tool_version go2proto "github.com/beam-cloud/go2proto" "$GO2PROTO_VERSION"

if command -v uv >/dev/null 2>&1; then
    sdk_bin="$(uv run --project ./sdk --locked python -c 'import sysconfig; print(sysconfig.get_path("scripts"))')"
    export PATH="$sdk_bin:$PATH"
elif ! command -v protoc-gen-python_betterproto_beta9 >/dev/null 2>&1; then
    echo "protoc-gen-python_betterproto_beta9 is not installed and uv is unavailable" >&2
    exit 1
else
    export PYTHON_BETTERPROTO_VERSION PYTHON_GRPCIO_VERSION PYTHON_BLACK_VERSION PYTHON_ISORT_VERSION
    python3 - <<'PY'
import importlib.metadata as md
import os
import sys

expected = {
    "betterproto-beta9": os.environ["PYTHON_BETTERPROTO_VERSION"],
    "grpcio": os.environ["PYTHON_GRPCIO_VERSION"],
    "black": os.environ["PYTHON_BLACK_VERSION"],
    "isort": os.environ["PYTHON_ISORT_VERSION"],
}

errors = []
for package, version in expected.items():
    try:
        installed = md.version(package)
    except md.PackageNotFoundError:
        errors.append(f"{package} is not installed")
        continue
    if installed != version:
        errors.append(f"{package}=={installed}, expected {package}=={version}")

if errors:
    print("Python protobuf compiler dependencies are not pinned to the SDK lock:", file=sys.stderr)
    for error in errors:
        print(f"  - {error}", file=sys.stderr)
    print(
        "Install: python3 -m pip install "
        f'"betterproto-beta9[compiler]=={expected["betterproto-beta9"]}" '
        f'"grpcio=={expected["grpcio"]}" '
        f'"black=={expected["black"]}" '
        f'"isort=={expected["isort"]}"',
        file=sys.stderr,
    )
    sys.exit(1)
PY
fi

go2proto -f ./pkg/types/types.proto -p ./pkg/types -n github.com/beam-cloud/beta9/proto -t types

# Generate code for gateway services
protoc -I ./pkg/types/ --go_out=./proto --go_opt=paths=source_relative --go-grpc_out=./proto --go-grpc_opt=paths=source_relative ./pkg/types/types.proto
protoc -I ./pkg/types/ --python_betterproto_beta9_out=./sdk/src/beta9/clients/ ./pkg/types/types.proto

protoc -I ./pkg/scheduler/ --go_out=./proto --go_opt=paths=source_relative --go-grpc_out=./proto --go-grpc_opt=paths=source_relative ./pkg/scheduler/scheduler.proto
protoc -I ./googleapis -I ./pkg/types -I ./pkg/gateway/ -I ./pkg/worker/ --go_out=./proto --go_opt=paths=source_relative --go-grpc_out=./proto --go-grpc_opt=paths=source_relative ./pkg/worker/worker.proto
protoc -I ./googleapis -I ./pkg/types -I ./pkg/gateway/ --go_out=./proto --go_opt=paths=source_relative --go-grpc_out=./proto --go-grpc_opt=paths=source_relative ./pkg/gateway/gateway.proto
protoc -I ./googleapis -I ./pkg/types -I ./pkg/gateway/ --python_betterproto_beta9_out=./sdk/src/beta9/clients/ ./pkg/gateway/gateway.proto

# Internal cache services
protoc -I ./pkg/cache/ --go_out=./proto --go_opt=paths=source_relative --go-grpc_out=./proto --go-grpc_opt=paths=source_relative ./pkg/cache/cache.proto

# Repository services
protoc -I $PROTOC_INCLUDE_PATH -I ./googleapis -I ./pkg/types -I ./pkg/gateway/services/repository/ --go_out=./proto --go_opt=paths=source_relative --go-grpc_out=./proto --go-grpc_opt=paths=source_relative ./pkg/gateway/services/repository/container_repo.proto
protoc -I $PROTOC_INCLUDE_PATH -I ./googleapis -I ./pkg/types -I ./pkg/gateway/services/repository/ --go_out=./proto --go_opt=paths=source_relative --go-grpc_out=./proto --go-grpc_opt=paths=source_relative ./pkg/gateway/services/repository/worker_repo.proto
protoc -I $PROTOC_INCLUDE_PATH -I ./googleapis -I ./pkg/types -I ./pkg/gateway/services/repository/ --go_out=./proto --go_opt=paths=source_relative --go-grpc_out=./proto --go-grpc_opt=paths=source_relative ./pkg/gateway/services/repository/backend_repo.proto

# Generate code for abstractions
protoc -I $PROTOC_INCLUDE_PATH -I ./googleapis -I ./pkg/abstractions/image/ --go_out=./proto --go_opt=paths=source_relative --go-grpc_out=./proto --go-grpc_opt=paths=source_relative ./pkg/abstractions/image/image.proto
protoc -I $PROTOC_INCLUDE_PATH -I ./googleapis -I ./pkg/abstractions/image/ --python_betterproto_beta9_out=./sdk/src/beta9/clients/ ./pkg/abstractions/image/image.proto

protoc -I $PROTOC_INCLUDE_PATH -I ./pkg/abstractions/map/ --go_out=./proto --go_opt=paths=source_relative --go-grpc_out=./proto --go-grpc_opt=paths=source_relative ./pkg/abstractions/map/map.proto
protoc -I $PROTOC_INCLUDE_PATH -I ./pkg/abstractions/map/ --python_betterproto_beta9_out=./sdk/src/beta9/clients/ ./pkg/abstractions/map/map.proto

protoc -I $PROTOC_INCLUDE_PATH -I ./pkg/abstractions/function/ --go_out=./proto --go_opt=paths=source_relative --go-grpc_out=./proto --go-grpc_opt=paths=source_relative ./pkg/abstractions/function/function.proto
protoc -I $PROTOC_INCLUDE_PATH -I ./pkg/abstractions/function/ --python_betterproto_beta9_out=./sdk/src/beta9/clients/ ./pkg/abstractions/function/function.proto

protoc -I $PROTOC_INCLUDE_PATH -I ./pkg/abstractions/queue/ --go_out=./proto --go_opt=paths=source_relative --go-grpc_out=./proto --go-grpc_opt=paths=source_relative ./pkg/abstractions/queue/queue.proto
protoc -I $PROTOC_INCLUDE_PATH -I ./pkg/abstractions/queue/ --python_betterproto_beta9_out=./sdk/src/beta9/clients/ ./pkg/abstractions/queue/queue.proto

protoc -I $PROTOC_INCLUDE_PATH -I ./googleapis -I ./pkg/abstractions/volume/ --go_out=./proto --go_opt=paths=source_relative --go-grpc_out=./proto --go-grpc_opt=paths=source_relative ./pkg/abstractions/volume/volume.proto
protoc -I $PROTOC_INCLUDE_PATH -I ./googleapis -I ./pkg/abstractions/volume/ --python_betterproto_beta9_out=./sdk/src/beta9/clients/ ./pkg/abstractions/volume/volume.proto
protoc -I $PROTOC_INCLUDE_PATH -I ./googleapis -I ./pkg/abstractions/volume/ --grpc-gateway_out=./proto --grpc-gateway_opt paths=source_relative --grpc-gateway_opt generate_unbound_methods=true ./pkg/abstractions/volume/volume.proto
protoc -I $PROTOC_INCLUDE_PATH -I ./googleapis -I ./pkg/abstractions/volume/ --openapiv2_out=./docs/openapi --openapiv2_opt logtostderr=true ./pkg/abstractions/volume/volume.proto

protoc -I $PROTOC_INCLUDE_PATH -I ./googleapis -I ./pkg/abstractions/disk/ --go_out=./proto --go_opt=paths=source_relative --go-grpc_out=./proto --go-grpc_opt=paths=source_relative ./pkg/abstractions/disk/disk.proto
protoc -I $PROTOC_INCLUDE_PATH -I ./googleapis -I ./pkg/abstractions/disk/ --python_betterproto_beta9_out=./sdk/src/beta9/clients/ ./pkg/abstractions/disk/disk.proto
protoc -I $PROTOC_INCLUDE_PATH -I ./googleapis -I ./pkg/abstractions/disk/ --grpc-gateway_out=./proto --grpc-gateway_opt paths=source_relative --grpc-gateway_opt generate_unbound_methods=true ./pkg/abstractions/disk/disk.proto
protoc -I $PROTOC_INCLUDE_PATH -I ./googleapis -I ./pkg/abstractions/disk/ --openapiv2_out=./docs/openapi --openapiv2_opt logtostderr=true ./pkg/abstractions/disk/disk.proto

protoc -I $PROTOC_INCLUDE_PATH -I ./pkg/abstractions/taskqueue/ --go_out=./proto --go_opt=paths=source_relative --go-grpc_out=./proto --go-grpc_opt=paths=source_relative ./pkg/abstractions/taskqueue/taskqueue.proto
protoc -I $PROTOC_INCLUDE_PATH -I ./pkg/abstractions/taskqueue/ --python_betterproto_beta9_out=./sdk/src/beta9/clients/ ./pkg/abstractions/taskqueue/taskqueue.proto

protoc -I $PROTOC_INCLUDE_PATH -I ./pkg/abstractions/endpoint/ --go_out=./proto --go_opt=paths=source_relative --go-grpc_out=./proto --go-grpc_opt=paths=source_relative ./pkg/abstractions/endpoint/endpoint.proto
protoc -I $PROTOC_INCLUDE_PATH -I ./pkg/abstractions/endpoint/ --python_betterproto_beta9_out=./sdk/src/beta9/clients/ ./pkg/abstractions/endpoint/endpoint.proto

protoc -I $PROTOC_INCLUDE_PATH -I ./googleapis -I ./pkg/types -I ./pkg/abstractions/pod/ --go_out=./proto --go_opt=paths=source_relative --go-grpc_out=./proto --go-grpc_opt=paths=source_relative ./pkg/abstractions/pod/pod.proto
protoc -I $PROTOC_INCLUDE_PATH -I ./googleapis -I ./pkg/types -I ./pkg/abstractions/pod/ --grpc-gateway_out=./proto --grpc-gateway_opt paths=source_relative --grpc-gateway_opt generate_unbound_methods=true ./pkg/abstractions/pod/pod.proto 
protoc -I $PROTOC_INCLUDE_PATH -I ./googleapis -I ./pkg/types -I ./pkg/abstractions/pod/ --openapiv2_out=./docs/openapi --openapiv2_opt logtostderr=true ./pkg/abstractions/pod/pod.proto

protoc -I $PROTOC_INCLUDE_PATH -I ./googleapis -I ./pkg/abstractions/image/ --grpc-gateway_out=./proto --grpc-gateway_opt paths=source_relative --grpc-gateway_opt generate_unbound_methods=true ./pkg/abstractions/image/image.proto
protoc -I $PROTOC_INCLUDE_PATH -I ./googleapis -I ./pkg/abstractions/image/ --openapiv2_out=./docs/openapi --openapiv2_opt logtostderr=true ./pkg/abstractions/image/image.proto

protoc -I $PROTOC_INCLUDE_PATH -I ./googleapis -I ./pkg/types -I ./pkg/gateway/ --grpc-gateway_out=./proto --grpc-gateway_opt paths=source_relative --grpc-gateway_opt generate_unbound_methods=true ./pkg/gateway/gateway.proto
protoc -I $PROTOC_INCLUDE_PATH -I ./googleapis -I ./pkg/types -I ./pkg/gateway/ --openapiv2_out=./docs/openapi --openapiv2_opt logtostderr=true ./pkg/gateway/gateway.proto
protoc -I $PROTOC_INCLUDE_PATH -I ./googleapis -I ./pkg/types -I ./pkg/abstractions/pod/ --python_betterproto_beta9_out=./sdk/src/beta9/clients/ ./pkg/abstractions/pod/pod.proto

protoc -I $PROTOC_INCLUDE_PATH -I ./pkg/abstractions/output/ --go_out=./proto --go_opt=paths=source_relative --go-grpc_out=./proto --go-grpc_opt=paths=source_relative ./pkg/abstractions/output/output.proto
protoc -I $PROTOC_INCLUDE_PATH -I ./pkg/abstractions/output/ --python_betterproto_beta9_out=./sdk/src/beta9/clients/ ./pkg/abstractions/output/output.proto

protoc -I $PROTOC_INCLUDE_PATH -I ./pkg/abstractions/secret/ --go_out=./proto --go_opt=paths=source_relative --go-grpc_out=./proto --go-grpc_opt=paths=source_relative ./pkg/abstractions/secret/secret.proto
protoc -I $PROTOC_INCLUDE_PATH -I ./pkg/abstractions/secret/ --python_betterproto_beta9_out=./sdk/src/beta9/clients/ ./pkg/abstractions/secret/secret.proto

protoc -I $PROTOC_INCLUDE_PATH -I ./pkg/abstractions/experimental/signal/ --go_out=./proto --go_opt=paths=source_relative --go-grpc_out=./proto --go-grpc_opt=paths=source_relative ./pkg/abstractions/experimental/signal/signal.proto
protoc -I $PROTOC_INCLUDE_PATH -I ./pkg/abstractions/experimental/signal/ --python_betterproto_beta9_out=./sdk/src/beta9/clients/ ./pkg/abstractions/experimental/signal/signal.proto

protoc -I $PROTOC_INCLUDE_PATH -I ./pkg/abstractions/experimental/bot/ --go_out=./proto --go_opt=paths=source_relative --go-grpc_out=./proto --go-grpc_opt=paths=source_relative ./pkg/abstractions/experimental/bot/bot.proto
protoc -I $PROTOC_INCLUDE_PATH -I ./pkg/abstractions/experimental/bot/ --python_betterproto_beta9_out=./sdk/src/beta9/clients/ ./pkg/abstractions/experimental/bot/bot.proto

protoc -I $PROTOC_INCLUDE_PATH -I ./pkg/abstractions/shell/ --go_out=./proto --go_opt=paths=source_relative --go-grpc_out=./proto --go-grpc_opt=paths=source_relative ./pkg/abstractions/shell/shell.proto
protoc -I $PROTOC_INCLUDE_PATH -I ./pkg/abstractions/shell/ --python_betterproto_beta9_out=./sdk/src/beta9/clients/ ./pkg/abstractions/shell/shell.proto
