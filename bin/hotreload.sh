#!/usr/bin/env bash

/workspace/bin/air.linux \
  --build.cmd "$BUILD_COMMAND" \
  --build.bin $BUILD_BINARY_PATH \
  --build.exclude_dir "deploy,docs,docker,manifests,hack,sdk,test"
