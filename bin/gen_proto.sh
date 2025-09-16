#!/bin/bash

if [ -z "$PROTOC_INCLUDE_PATH" ]; then
    PROTOC_INCLUDE_PATH="/usr/local/include"
fi

GRPC_PATH=$(which protoc-gen-go-grpc)
if [[ "$GRPC_PATH" == *"/go/bin/"*  ]]; then
    echo "protoc-gen-go-grpc is a go install"
elif [ -f "$GRPC_PATH" ]; then
    echo "WARNING: protoc-gen-go-grpc is a system install and there might be version conflicts"
else
    echo "protoc-gen-go-grpc is not installed"
    exit 1
fi

go2proto -f ./pkg/types/types.proto -p ./pkg/types -n github.com/beam-cloud/beta9/proto -t types

# Generate code for gateway services
protoc -I ./pkg/types/ --go_out=./proto --go_opt=paths=source_relative --go-grpc_out=./proto --go-grpc_opt=paths=source_relative ./pkg/types/types.proto
protoc -I ./pkg/types/ --python_betterproto_beta9_out=./sdk/src/beta9/clients/ ./pkg/types/types.proto

protoc -I ./pkg/scheduler/ --go_out=./proto --go_opt=paths=source_relative --go-grpc_out=./proto --go-grpc_opt=paths=source_relative ./pkg/scheduler/scheduler.proto
protoc -I ./googleapis -I ./pkg/types -I ./pkg/gateway/ -I ./pkg/worker/ --go_out=./proto --go_opt=paths=source_relative --go-grpc_out=./proto --go-grpc_opt=paths=source_relative ./pkg/worker/worker.proto
protoc -I ./googleapis -I ./pkg/types -I ./pkg/gateway/ --go_out=./proto --go_opt=paths=source_relative --go-grpc_out=./proto --go-grpc_opt=paths=source_relative ./pkg/gateway/gateway.proto
protoc -I ./googleapis -I ./pkg/types -I ./pkg/gateway/ --python_betterproto_beta9_out=./sdk/src/beta9/clients/ ./pkg/gateway/gateway.proto

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
