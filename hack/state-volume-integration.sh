#!/usr/bin/env bash
set -euo pipefail

repo_root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
context=${LOCAL_CONTEXT:-k3d-beta9}
namespace=${LOCAL_NAMESPACE:-beta9}
report=${STATE_VOLUME_INTEGRATION_REPORT:-}
image=${STATE_VOLUME_INTEGRATION_IMAGE:-localhost:5001/beta9-state-volume-integration:latest}
worker_image=${STATE_VOLUME_INTEGRATION_WORKER_IMAGE:-localhost:5001/beta9-worker:latest}
legacy_source=${STATE_VOLUME_LEGACY_SOURCE:-/private/tmp/beta9-legacy-state-benchmark}
legacy_image=${STATE_VOLUME_LEGACY_IMAGE:-localhost:5001/beta9-state-volume-legacy-benchmark:latest}
legacy_commit=74483d8d7ddbad95fce813681c6026528c1cbe43
legacy_fixture_patch_sha256=b9c6bd76bb19d197378969b5f781c9e29044cff201b553c26d1ad213e56ba3d8
job_ref=
rendered_manifest=

if [[ "$context" != "k3d-beta9" ]]; then
  echo "refusing state-volume integration against non-local Kubernetes context: $context" >&2
  exit 2
fi
if [[ "$namespace" != "beta9" ]]; then
  echo "refusing state-volume integration outside the disposable beta9 namespace: $namespace" >&2
  exit 2
fi
if [[ -z "$report" || "$report" != /* ]]; then
  echo "STATE_VOLUME_INTEGRATION_REPORT must be a new absolute path" >&2
  exit 2
fi
if [[ -e "$report" ]]; then
  echo "refusing pre-existing integration report: $report" >&2
  exit 2
fi

for command_name in awk docker git kubectl openssl rg sed; do
  command -v "$command_name" >/dev/null || {
    echo "missing required command: $command_name" >&2
    exit 2
  }
done

kubectl_local=(kubectl --context "$context" --namespace "$namespace")

pushed_image_digest() {
  local exact_image=$1
  local exact_repository=${exact_image%:*}
  local digest_ref
  while IFS= read -r digest_ref; do
    if [[ "$digest_ref" == "$exact_repository"@sha256:* ]]; then
      local digest=${digest_ref##*@}
      if [[ "$digest" =~ ^sha256:[0-9a-f]{64}$ ]]; then
        printf '%s\n' "$digest"
        return 0
      fi
    fi
  done < <(docker image inspect --format '{{range .RepoDigests}}{{println .}}{{end}}' "$exact_image")
  echo "pushed image has no exact immutable digest: $exact_image" >&2
  return 1
}

cluster_image_ref() {
  local local_image=$1
  local digest=$2
  if [[ "$local_image" != localhost:5001/* ]]; then
    echo "integration images must use the local k3d registry: $local_image" >&2
    return 1
  fi
  local repository=${local_image#localhost:5001/}
  repository=${repository%:*}
  printf 'registry.localhost:5000/%s@%s\n' "$repository" "$digest"
}

delete_job_and_verify() {
  local exact_job_ref=$1
  local exact_job_name=${exact_job_ref#job.batch/}
  "${kubectl_local[@]}" delete "$exact_job_ref" --wait=true --ignore-not-found=true >/dev/null
  local cleanup_deadline=$((SECONDS + 60))
  while [[ $SECONDS -lt $cleanup_deadline ]]; do
    local remaining_job remaining_pods
    remaining_job=$("${kubectl_local[@]}" get job "$exact_job_name" -o name --ignore-not-found=true)
    remaining_pods=$("${kubectl_local[@]}" get pods -l "job-name=$exact_job_name" -o name)
    if [[ -z "$remaining_job" && -z "$remaining_pods" ]]; then
      return 0
    fi
    sleep 1
  done
  echo "generated integration resources remain after deletion: job=$exact_job_name" >&2
  "${kubectl_local[@]}" get job "$exact_job_name" -o wide --ignore-not-found=true >&2 || true
  "${kubectl_local[@]}" get pods -l "job-name=$exact_job_name" -o wide >&2 || true
  return 1
}

cleanup() {
  local original_status=$?
  trap - EXIT INT TERM
  if [[ -n "$job_ref" ]] && ! delete_job_and_verify "$job_ref"; then
    echo "failed to remove the exact generated integration Job and pods" >&2
    if [[ $original_status -eq 0 ]]; then
      original_status=1
    fi
  fi
  if [[ -n "$rendered_manifest" ]]; then
    rm -f "$rendered_manifest"
  fi
  exit "$original_status"
}
trap cleanup EXIT
trap 'exit 130' INT TERM

"${kubectl_local[@]}" get namespace "$namespace" >/dev/null
if [[ -n "$("${kubectl_local[@]}" get jobs -l app.kubernetes.io/component=state-volume-integration -o name)" ]]; then
  echo "refusing concurrent or uncleared state-volume integration Job" >&2
  exit 2
fi
docker image inspect "$worker_image" >/dev/null || {
  echo "missing $worker_image; run 'make worker LOCAL_CONTEXT=k3d-beta9' first" >&2
  exit 2
}

if [[ ! -e "$legacy_source/.git" ]]; then
  echo "missing pinned legacy benchmark worktree: $legacy_source" >&2
  exit 2
fi
if [[ "$(git -C "$legacy_source" rev-parse HEAD)" != "$legacy_commit" ]]; then
  echo "legacy benchmark worktree is not pinned to $legacy_commit" >&2
  exit 2
fi
legacy_status=$(git -C "$legacy_source" status --porcelain --untracked-files=all)
if [[ "$legacy_status" != " M pkg/worker/durable_disk_test.go" ]]; then
  echo "legacy benchmark worktree contains changes outside its exact 100k fixture patch" >&2
  printf '%s\n' "$legacy_status" >&2
  exit 2
fi
actual_legacy_patch_sha256=$(git -C "$legacy_source" diff --no-ext-diff --binary -- pkg/worker/durable_disk_test.go | openssl dgst -sha256 | awk '{print $NF}')
if [[ "$actual_legacy_patch_sha256" != "$legacy_fixture_patch_sha256" ]]; then
  echo "legacy 100k fixture patch digest changed: $actual_legacy_patch_sha256" >&2
  exit 2
fi

platform="linux/$(uname -m | sed 's/x86_64/amd64/; s/aarch64/arm64/')"
docker build "$legacy_source" \
  --platform "$platform" \
  --build-arg "LEGACY_COMMIT=$legacy_commit" \
  --build-arg "LEGACY_FIXTURE_PATCH_SHA256=$legacy_fixture_patch_sha256" \
  -f "$repo_root/docker/Dockerfile.state-volume-legacy-benchmark" \
  -t "$legacy_image"
docker push "$legacy_image"
legacy_digest=$(pushed_image_digest "$legacy_image")
legacy_cluster_image=$(cluster_image_ref "$legacy_image" "$legacy_digest")

docker build "$repo_root" \
  --platform "$platform" \
  --build-arg "WORKER_IMAGE=$worker_image" \
  -f "$repo_root/docker/Dockerfile.state-volume-integration" \
  -t "$image"
docker push "$image"
block_digest=$(pushed_image_digest "$image")
block_cluster_image=$(cluster_image_ref "$image" "$block_digest")

rendered_manifest=$(mktemp /private/tmp/beta9-state-volume-integration-job.XXXXXX.yaml)
sed \
  -e "s#registry.localhost:5000/beta9-state-volume-legacy-benchmark:state-volume-generated#$legacy_cluster_image#" \
  -e "s#registry.localhost:5000/beta9-state-volume-integration:state-volume-generated#$block_cluster_image#" \
  -e "s#sha256:legacy-image-placeholder#$legacy_digest#" \
  -e "s#sha256:block-image-placeholder#$block_digest#" \
  "$repo_root/hack/state-volume-integration-job.yaml" > "$rendered_manifest"
if rg -q 'state-volume-generated|image-placeholder' "$rendered_manifest"; then
  echo "generated integration manifest retained an unresolved image placeholder" >&2
  exit 2
fi
job_ref=$("${kubectl_local[@]}" create -f "$rendered_manifest" -o name)
job_name=${job_ref#job.batch/}
pod_name=
deadline=$((SECONDS + 7200))

while [[ -z "$pod_name" && $SECONDS -lt $deadline ]]; do
  pod_name=$("${kubectl_local[@]}" get pods -l "job-name=$job_name" -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || true)
  [[ -n "$pod_name" ]] || sleep 1
done
if [[ -z "$pod_name" ]]; then
  echo "integration Job did not create a pod" >&2
  exit 1
fi

phase=
while [[ $SECONDS -lt $deadline ]]; do
  phase=$("${kubectl_local[@]}" get pod "$pod_name" -o jsonpath='{.status.phase}')
  case "$phase" in
    Succeeded|Failed) break ;;
  esac
  sleep 2
done
if [[ "$phase" != "Succeeded" && "$phase" != "Failed" ]]; then
  echo "integration pod did not terminate before its deadline (phase=$phase)" >&2
  "${kubectl_local[@]}" describe pod "$pod_name" >&2 || true
  exit 1
fi

mkdir -p "$(dirname "$report")"
temporary_report=$(mktemp "${report}.tmp.XXXXXX")
if ! "${kubectl_local[@]}" cp "$pod_name:/report/state-volume-integration.json" "$temporary_report"; then
  "${kubectl_local[@]}" logs "$pod_name" >&2 || true
  rm -f "$temporary_report"
  echo "integration pod emitted no report" >&2
  exit 1
fi
chmod 0600 "$temporary_report"
mv "$temporary_report" "$report"

"${kubectl_local[@]}" logs "$pod_name"
delete_job_and_verify "$job_ref"
job_ref=
if [[ "$phase" != "Succeeded" ]]; then
  echo "state-volume integration failed; evidence report preserved at $report" >&2
  exit 1
fi

echo "state-volume integration report: $report"
