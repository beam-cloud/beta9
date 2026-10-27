#!/usr/bin/env bash

/workspace/bin/air.linux \
  --build.cmd "$BUILD_COMMAND" \
  --build.bin "$BUILD_BINARY_PATH" \
  --build.include_ext "go,tpl,tmpl,html,js,css" \
  --build.exclude_dir ".git,.tmp,benchmarks,bin,deploy,docs,docker,hack,manifests,sdk,test,tmp"
