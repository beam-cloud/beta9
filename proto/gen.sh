#!/bin/bash

# Generate Go code
protoc --go_out=. --go_opt=paths=source_relative --go-grpc_out=./ --go-grpc_opt=paths=source_relative ./scheduler.proto
protoc --go_out=. --go_opt=paths=source_relative --go-grpc_out=./ --go-grpc_opt=paths=source_relative ./imageservice.proto
protoc --go_out=. --go_opt=paths=source_relative --go-grpc_out=./ --go-grpc_opt=paths=source_relative ./cache.proto

# Generate code for integrations
protoc -I ../internal/integrations/map/ --go_out=. --go_opt=paths=source_relative --go-grpc_out=./ --go-grpc_opt=paths=source_relative ../internal/integrations/map/map.proto
protoc -I ../internal/integrations/map/ --python_betterproto_out=../../beam-sdk/src/beam/clients/  ../internal/integrations/map/map.proto

protoc -I ../internal/integrations/bucket/ --go_out=. --go_opt=paths=source_relative --go-grpc_out=./ --go-grpc_opt=paths=source_relative ../internal/integrations/bucket/bucket.proto
protoc -I ../internal/integrations/bucket/ --python_betterproto_out=../../beam-sdk/src/beam/clients/  ../internal/integrations/bucket/bucket.proto