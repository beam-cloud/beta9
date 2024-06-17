#!/bin/bash

# Generate code for gateway services
protoc -I ./pkg/scheduler/ --go_out=./proto --go_opt=paths=source_relative --go-grpc_out=./proto --go-grpc_opt=paths=source_relative ./pkg/scheduler/scheduler.proto
protoc -I ./pkg/worker/ --go_out=./proto --go_opt=paths=source_relative --go-grpc_out=./proto --go-grpc_opt=paths=source_relative ./pkg/worker/worker.proto

protoc -I ./pkg/gateway/ --go_out=./proto --go_opt=paths=source_relative --go-grpc_out=./proto --go-grpc_opt=paths=source_relative ./pkg/gateway/gateway.proto
protoc -I ./pkg/gateway/ --python_betterproto_beta9_out=./sdk/src/beta9/clients/ ./pkg/gateway/gateway.proto

# Generate code for abstractions
protoc -I ./pkg/abstractions/image/ --go_out=./proto --go_opt=paths=source_relative --go-grpc_out=./proto --go-grpc_opt=paths=source_relative ./pkg/abstractions/image/image.proto
protoc -I ./pkg/abstractions/image/ --python_betterproto_beta9_out=./sdk/src/beta9/clients/ ./pkg/abstractions/image/image.proto

protoc -I ./pkg/abstractions/map/ --go_out=./proto --go_opt=paths=source_relative --go-grpc_out=./proto --go-grpc_opt=paths=source_relative ./pkg/abstractions/map/map.proto
protoc -I ./pkg/abstractions/map/ --python_betterproto_beta9_out=./sdk/src/beta9/clients/ ./pkg/abstractions/map/map.proto

protoc -I ./pkg/abstractions/function/ --go_out=./proto --go_opt=paths=source_relative --go-grpc_out=./proto --go-grpc_opt=paths=source_relative ./pkg/abstractions/function/function.proto
protoc -I ./pkg/abstractions/function/ --python_betterproto_beta9_out=./sdk/src/beta9/clients/ ./pkg/abstractions/function/function.proto

protoc -I ./pkg/abstractions/queue/ --go_out=./proto --go_opt=paths=source_relative --go-grpc_out=./proto --go-grpc_opt=paths=source_relative ./pkg/abstractions/queue/queue.proto
protoc -I ./pkg/abstractions/queue/ --python_betterproto_beta9_out=./sdk/src/beta9/clients/ ./pkg/abstractions/queue/queue.proto

protoc -I ./pkg/abstractions/volume/ --go_out=./proto --go_opt=paths=source_relative --go-grpc_out=./proto --go-grpc_opt=paths=source_relative ./pkg/abstractions/volume/volume.proto
protoc -I ./pkg/abstractions/volume/ --python_betterproto_beta9_out=./sdk/src/beta9/clients/ ./pkg/abstractions/volume/volume.proto

protoc -I ./pkg/abstractions/taskqueue/ --go_out=./proto --go_opt=paths=source_relative --go-grpc_out=./proto --go-grpc_opt=paths=source_relative ./pkg/abstractions/taskqueue/taskqueue.proto
protoc -I ./pkg/abstractions/taskqueue/ --python_betterproto_beta9_out=./sdk/src/beta9/clients/ ./pkg/abstractions/taskqueue/taskqueue.proto

protoc -I ./pkg/abstractions/endpoint/ --go_out=./proto --go_opt=paths=source_relative --go-grpc_out=./proto --go-grpc_opt=paths=source_relative ./pkg/abstractions/endpoint/endpoint.proto
protoc -I ./pkg/abstractions/endpoint/ --python_betterproto_beta9_out=./sdk/src/beta9/clients/ ./pkg/abstractions/endpoint/endpoint.proto

protoc -I ./pkg/abstractions/container/ --go_out=./proto --go_opt=paths=source_relative --go-grpc_out=./proto --go-grpc_opt=paths=source_relative ./pkg/abstractions/container/container.proto
protoc -I ./pkg/abstractions/container/ --python_betterproto_beta9_out=./sdk/src/beta9/clients/ ./pkg/abstractions/container/container.proto

protoc -I ./pkg/abstractions/output/ --go_out=./proto --go_opt=paths=source_relative --go-grpc_out=./proto --go-grpc_opt=paths=source_relative ./pkg/abstractions/output/output.proto
protoc -I ./pkg/abstractions/output/ --python_betterproto_beta9_out=./sdk/src/beta9/clients/ ./pkg/abstractions/output/output.proto

protoc -I ./pkg/abstractions/secret/ --go_out=./proto --go_opt=paths=source_relative --go-grpc_out=./proto --go-grpc_opt=paths=source_relative ./pkg/abstractions/secret/secret.proto
protoc -I ./pkg/abstractions/secret/ --python_betterproto_beta9_out=./sdk/src/beta9/clients/ ./pkg/abstractions/secret/secret.proto