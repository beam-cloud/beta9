#!/bin/bash

# tailscale
tailscaled --port=41641 --socket=/var/run/tailscale/tailscaled.sock --state=/var/lib/tailscale/tailscaled.state &
tailscale up --authkey=${TAILSCALE_AUTHKEY} --ssh --reset --accept-routes

sleep 5

# juicefs
JUICEFS_ADDRESS=${JUICEFS_ADDRESS:-0.0.0.0:9000}
JUICEFS_BLOCK_SIZE=${JUICEFS_ADDRESS:-4096}
JUICEFS_BUFFER_SIZE=${JUICEFS_BUFFER_SIZE:-300}
JUICEFS_CACHE_SIZE=${JUICEFS_CACHE_SIZE:-0}
JUICEFS_PREFETCH=${JUICEFS_PREFETCH:-1}

juicefs format \
  --storage=s3 \
  --bucket=${JUICEFS_BUCKET} \
  --access-key=${JUICEFS_ACCESS_KEY} \
  --secret-key=${JUICEFS_SECRET_KEY} \
  --block-size=${JUICEFS_BLOCK_SIZE} \
  --no-update \
  ${JUICEFS_REDIS_URI} \
  ${JUICEFS_NAME}

juicefs gateway \
  --storage="s3" \
  --bucket=${JUICEFS_BUCKET} \
  --buffer-size=${JUICEFS_BUFFER_SIZE} \
  --cache-size=${JUICEFS_CACHE_SIZE} \
  --prefetch=${JUICEFS_PREFETCH} \
  --no-usage-report \
  ${JUICEFS_REDIS_URI} \
  ${JUICEFS_ADDRESS}
