#!/usr/bin/env bash

set -Eeuo pipefail

beta9_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
repository="${WORKER_E2E_REPOSITORY:-public.ecr.aws/n4e0e1y0/beta9-worker}"
legacy_runtime="public.ecr.aws/n4e0e1y0/beta9-worker@sha256:5792f3b5c2ce00e37165db3ed1f1d39b245da6ec922a3cf688f437280cae0308"
runtime_image="${WORKER_E2E_RUNTIME_IMAGE:-$legacy_runtime}"

fail() {
  printf 'worker-e2e: %s\n' "$*" >&2
  exit 1
}

require_command() {
  command -v "$1" >/dev/null 2>&1 || fail "missing required command: $1"
}

source_hash() {
  (
    cd "$beta9_dir"
    git ls-files --cached --others --exclude-standard -- \
      .dockerignore docker/Dockerfile.worker-overlay hack/worker-e2e-image.sh \
      cmd pkg proto go.mod go.sum |
      LANG=C LC_ALL=C sort |
      while IFS= read -r source_file; do
        printf '%s\n' "$source_file"
        LANG=C LC_ALL=C shasum -a 256 "$source_file"
      done
  ) | LANG=C LC_ALL=C shasum -a 256 | cut -c1-12
}

image_tag() {
  local revision="${1:-$(git -C "$beta9_dir" rev-parse HEAD)}"
  local hash="${2:-$(source_hash)}"
  local runtime_digest="${runtime_image##*@sha256:}"
  printf 'tama-e2e-%s-%s-%s\n' "${revision:0:8}" "$hash" "${runtime_digest:0:12}"
}

tag_is_absent() {
  [[ "$repository" == public.ecr.aws/* ]] || fail "preflight supports public.ecr.aws repositories only"

  local repository_path token status tag
  repository_path="${repository#public.ecr.aws/}"
  tag="$1"
  token="$(curl --max-time 10 -fsS --get \
    --data-urlencode 'service=public.ecr.aws' \
    --data-urlencode "scope=repository:$repository_path:pull" \
    https://public.ecr.aws/token/ | jq -r '.token // empty')"
  [[ -n "$token" ]] || fail "could not obtain a public ECR pull token"

  status="$(curl --max-time 10 -sS -o /dev/null -w '%{http_code}' \
    -H "Authorization: Bearer $token" \
    -H 'Accept: application/vnd.oci.image.index.v1+json,application/vnd.docker.distribution.manifest.list.v2+json,application/vnd.oci.image.manifest.v1+json,application/vnd.docker.distribution.manifest.v2+json' \
    "https://public.ecr.aws/v2/$repository_path/manifests/$tag")"
  case "$status" in
    404) return 0 ;;
    200) fail "$repository:$tag already exists" ;;
    *) fail "registry preflight returned HTTP $status" ;;
  esac
}

push_image() {
  if [[ "$runtime_image" == "$legacy_runtime" && "${CEDANA_TOKEN_ROTATED:-}" != 1 ]]; then
    fail "rotate the CEDANA token, then rerun with CEDANA_TOKEN_ROTATED=1"
  fi

  local revision hash tag
  revision="$(git -C "$beta9_dir" rev-parse HEAD)"
  hash="$(source_hash)"
  tag="$(image_tag "$revision" "$hash")"
  tag_is_absent "$tag"

  docker buildx build "$beta9_dir" \
    --file "$beta9_dir/docker/Dockerfile.worker-overlay" \
    --platform linux/amd64,linux/arm64 \
    --build-arg "WORKER_RUNTIME_IMAGE=$runtime_image" \
    --build-arg "SOURCE_REVISION=$revision" \
    --build-arg "SOURCE_HASH=$hash" \
    --provenance=false \
    --sbom=false \
    --tag "$repository:$tag" \
    --push

  printf '%s\n' "$tag"
}

for command_name in git shasum; do
  require_command "$command_name"
done
[[ "$runtime_image" =~ @sha256:[0-9a-f]{64}$ ]] || fail "WORKER_E2E_RUNTIME_IMAGE must use a sha256 digest"

case "${1:-tag}" in
  tag) image_tag ;;
  check)
    require_command curl
    require_command jq
    tag="$(image_tag)"
    tag_is_absent "$tag"
    printf '%s\n' "$tag"
    ;;
  push)
    require_command curl
    require_command docker
    require_command jq
    push_image
    ;;
  *) fail "usage: $0 [tag|check|push]" ;;
esac
