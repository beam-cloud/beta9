#!/bin/bash

# Generate Go code
protoc --go_out=. --go_opt=paths=source_relative --go-grpc_out=./ --go-grpc_opt=paths=source_relative ./scheduler.proto
protoc --go_out=. --go_opt=paths=source_relative --go-grpc_out=./ --go-grpc_opt=paths=source_relative ./imageservice.proto
protoc --go_out=. --go_opt=paths=source_relative --go-grpc_out=./ --go-grpc_opt=paths=source_relative ./cache.proto

# # Generate Python code
# protoc -I . --python_betterproto_out=../python/runner/src/runner/clients ./scheduler.proto