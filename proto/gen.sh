#!/bin/bash

# Generate Go code
protoc --go_out=. --go_opt=paths=source_relative --go-grpc_out=./ --go-grpc_opt=paths=source_relative ./scheduler.proto
protoc --go_out=. --go_opt=paths=source_relative --go-grpc_out=./ --go-grpc_opt=paths=source_relative ./gateway.proto
protoc -I . --python_betterproto_out=../sdk/src/beam/clients/  ./gateway.proto

protoc --go_out=. --go_opt=paths=source_relative --go-grpc_out=./ --go-grpc_opt=paths=source_relative ./cache.proto
protoc --go_out=. --go_opt=paths=source_relative --go-grpc_out=./ --go-grpc_opt=paths=source_relative ./cache.proto
protoc -I ../internal/worker/ --go_out=. --go_opt=paths=source_relative --go-grpc_out=./ --go-grpc_opt=paths=source_relative ../internal/worker/worker.proto

# Generate code for abstractions
protoc -I ../internal/abstractions/image/ --go_out=. --go_opt=paths=source_relative --go-grpc_out=./ --go-grpc_opt=paths=source_relative ../internal/abstractions/image/image.proto
protoc -I ../internal/abstractions/image/ --python_betterproto_out=../sdk/src/beam/clients/  ../internal/abstractions/image/image.proto

protoc -I ../internal/abstractions/map/ --go_out=. --go_opt=paths=source_relative --go-grpc_out=./ --go-grpc_opt=paths=source_relative ../internal/abstractions/map/map.proto
protoc -I ../internal/abstractions/map/ --python_betterproto_out=../sdk/src/beam/clients/  ../internal/abstractions/map/map.proto
