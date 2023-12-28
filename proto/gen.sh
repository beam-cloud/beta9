#!/bin/bash

# Generate Go code
protoc --go_out=. --go_opt=paths=source_relative --go-grpc_out=./ --go-grpc_opt=paths=source_relative ./scheduler.proto
protoc -I ../internal/gateway/ --go_out=. --go_opt=paths=source_relative --go-grpc_out=./ --go-grpc_opt=paths=source_relative ../internal/gateway/gateway.proto
protoc -I ../internal/gateway/ --python_betterproto_out=../sdk/src/beam/clients/  ../internal/gateway/gateway.proto

protoc --go_out=. --go_opt=paths=source_relative --go-grpc_out=./ --go-grpc_opt=paths=source_relative ./cache.proto
protoc --go_out=. --go_opt=paths=source_relative --go-grpc_out=./ --go-grpc_opt=paths=source_relative ./cache.proto
protoc -I ../internal/worker/ --go_out=. --go_opt=paths=source_relative --go-grpc_out=./ --go-grpc_opt=paths=source_relative ../internal/worker/worker.proto

# Generate code for abstractions
protoc -I ../internal/abstractions/image/ --go_out=. --go_opt=paths=source_relative --go-grpc_out=./ --go-grpc_opt=paths=source_relative ../internal/abstractions/image/image.proto
protoc -I ../internal/abstractions/image/ --python_betterproto_out=../sdk/src/beam/clients/  ../internal/abstractions/image/image.proto

protoc -I ../internal/abstractions/map/ --go_out=. --go_opt=paths=source_relative --go-grpc_out=./ --go-grpc_opt=paths=source_relative ../internal/abstractions/map/map.proto
protoc -I ../internal/abstractions/map/ --python_betterproto_out=../sdk/src/beam/clients/  ../internal/abstractions/map/map.proto

protoc -I ../internal/abstractions/function/ --go_out=. --go_opt=paths=source_relative --go-grpc_out=./ --go-grpc_opt=paths=source_relative ../internal/abstractions/function/function.proto
protoc -I ../internal/abstractions/function/ --python_betterproto_out=../sdk/src/beam/clients/  ../internal/abstractions/function/function.proto

protoc -I ../internal/abstractions/queue/ --go_out=. --go_opt=paths=source_relative --go-grpc_out=./ --go-grpc_opt=paths=source_relative ../internal/abstractions/queue/queue.proto
protoc -I ../internal/abstractions/queue/ --python_betterproto_out=../sdk/src/beam/clients/  ../internal/abstractions/queue/queue.proto
