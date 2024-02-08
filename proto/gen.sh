#!/bin/bash

# Generate code for gateway services
protoc -I ../internal/scheduler/ --go_out=. --go_opt=paths=source_relative --go-grpc_out=./ --go-grpc_opt=paths=source_relative ../internal/scheduler/scheduler.proto
protoc -I ../internal/worker/ --go_out=. --go_opt=paths=source_relative --go-grpc_out=./ --go-grpc_opt=paths=source_relative ../internal/worker/worker.proto

protoc -I ../internal/gateway/ --go_out=. --go_opt=paths=source_relative --go-grpc_out=./ --go-grpc_opt=paths=source_relative ../internal/gateway/gateway.proto
protoc -I ../internal/gateway/ --python_betterproto_out=../sdk/src/beta9/clients/  ../internal/gateway/gateway.proto

# Generate code for abstractions
protoc -I ../internal/abstractions/image/ --go_out=. --go_opt=paths=source_relative --go-grpc_out=./ --go-grpc_opt=paths=source_relative ../internal/abstractions/image/image.proto
protoc -I ../internal/abstractions/image/ --python_betterproto_out=../sdk/src/beta9/clients/  ../internal/abstractions/image/image.proto

protoc -I ../internal/abstractions/map/ --go_out=. --go_opt=paths=source_relative --go-grpc_out=./ --go-grpc_opt=paths=source_relative ../internal/abstractions/map/map.proto
protoc -I ../internal/abstractions/map/ --python_betterproto_out=../sdk/src/beta9/clients/  ../internal/abstractions/map/map.proto

protoc -I ../internal/abstractions/function/ --go_out=. --go_opt=paths=source_relative --go-grpc_out=./ --go-grpc_opt=paths=source_relative ../internal/abstractions/function/function.proto
protoc -I ../internal/abstractions/function/ --python_betterproto_out=../sdk/src/beta9/clients/  ../internal/abstractions/function/function.proto

protoc -I ../internal/abstractions/queue/ --go_out=. --go_opt=paths=source_relative --go-grpc_out=./ --go-grpc_opt=paths=source_relative ../internal/abstractions/queue/queue.proto
protoc -I ../internal/abstractions/queue/ --python_betterproto_out=../sdk/src/beta9/clients/  ../internal/abstractions/queue/queue.proto

protoc -I ../internal/abstractions/volume/ --go_out=. --go_opt=paths=source_relative --go-grpc_out=./ --go-grpc_opt=paths=source_relative ../internal/abstractions/volume/volume.proto
protoc -I ../internal/abstractions/volume/ --python_betterproto_out=../sdk/src/beta9/clients/  ../internal/abstractions/volume/volume.proto

protoc -I ../internal/abstractions/taskqueue/ --go_out=. --go_opt=paths=source_relative --go-grpc_out=./ --go-grpc_opt=paths=source_relative ../internal/abstractions/taskqueue/taskqueue.proto
protoc -I ../internal/abstractions/taskqueue/ --python_betterproto_out=../sdk/src/beta9/clients/  ../internal/abstractions/taskqueue/taskqueue.proto