#!/bin/bash

# Generate Go code
protoc --go_out=. --go_opt=paths=source_relative --go-grpc_out=./ --go-grpc_opt=paths=source_relative ./scheduler.proto
protoc --go_out=. --go_opt=paths=source_relative --go-grpc_out=./ --go-grpc_opt=paths=source_relative ./imageservice.proto
protoc --go_out=. --go_opt=paths=source_relative --go-grpc_out=./ --go-grpc_opt=paths=source_relative ./cache.proto

# Generate code for integrations
protoc -I ../internal/abstractions/map/ --go_out=. --go_opt=paths=source_relative --go-grpc_out=./ --go-grpc_opt=paths=source_relative ../internal/abstractions/map/map.proto
protoc -I ../internal/abstractions/map/ --python_betterproto_out=../../beam-sdk/src/beam/clients/  ../internal/abstractions/map/map.proto

protoc -I ../internal/abstractions/bucket/ --go_out=. --go_opt=paths=source_relative --go-grpc_out=./ --go-grpc_opt=paths=source_relative ../internal/abstractions/bucket/bucket.proto
protoc -I ../internal/abstractions/bucket/ --python_betterproto_out=../../beam-sdk/src/beam/clients/  ../internal/abstractions/bucket/bucket.proto