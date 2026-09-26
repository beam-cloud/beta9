#!/usr/bin/env bash
#
# Mechanics check for the microvm runtime on a bare-metal node.
#
# Syncs the Go tree to the node, overlays freshly built binaries plus the
# Cloud Hypervisor tooling onto the runtime image that is already cached
# there (no CRIU/gVisor/QEMU rebuild), and runs the `microvm`-tagged
# integration tests inside that image with the same privileges the agent
# grants a worker. No gateway, agent, or SDK involved.
#
#   hack/microvm-smoke.sh              # sync, build, rootfs, test
#   hack/microvm-smoke.sh test Boot    # rerun only tests matching Boot
#   hack/microvm-smoke.sh shell        # privileged shell in the dev image
#
# Environment:
#   MICROVM_HOST           ssh target            (required)
#   MICROVM_SSH_KEY        ssh identity          (required)
#   MICROVM_REMOTE_DIR     remote checkout       (~/beta9-microvm)
#   MICROVM_RUNTIME_IMAGE  cached runtime image  (public.ecr.aws/n4e0e1y0/beta9-worker:0.1.764)
#   MICROVM_IMAGE_TAG      dev image tag         (localhost:5000/beta9-worker:microvm-dev)
#   MICROVM_TEST_TIMEOUT   go test timeout       (45m)

set -Eeuo pipefail

beta9_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
host="${MICROVM_HOST:?set MICROVM_HOST to the ssh target of the node}"
ssh_key="${MICROVM_SSH_KEY:?set MICROVM_SSH_KEY to the ssh identity file}"
remote_dir="${MICROVM_REMOTE_DIR:-\$HOME/beta9-microvm}"
runtime_image="${MICROVM_RUNTIME_IMAGE:-public.ecr.aws/n4e0e1y0/beta9-worker:0.1.764}"
image_tag="${MICROVM_IMAGE_TAG:-localhost:5000/beta9-worker:microvm-dev}"

# Rootfs tarballs the harness boots. alpine covers boot/exit/network, dind
# covers Docker-in-VM, python covers the import-loop timing baseline.
rootfs_images=(
  "alpine=docker.io/library/alpine:3.20"
  "dind=docker.io/library/docker:27-dind"
  "python=docker.io/library/python:3.12-slim"
)

log() { printf 'microvm-smoke: %s\n' "$*" >&2; }
fail() { log "$*"; exit 1; }

remote() {
  ssh -i "$ssh_key" -o BatchMode=yes -o ConnectTimeout=15 -o StrictHostKeyChecking=accept-new "$host" "$@"
}

# The agent's docker run flags for a worker (pkg/agent/worker_container.go),
# minus the worker-specific volumes and env; plus the rootfs tarballs and a
# host-backed work dir (overlay upper/work dirs and scratch disks cannot live
# on the container's own overlayfs root, exactly as the worker's /tmp is a
# host bind).
docker_run_flags() {
  cat <<EOF
--privileged --network host --cgroupns host \
-v /var/run/netns:/var/run/netns \
-v /sys/fs/cgroup:/sys/fs/cgroup:rw \
-v /lib/modules:/lib/modules:ro \
-v $remote_dir/rootfs:/microvm/rootfs:ro \
-v $remote_dir/work:/tmp/microvm-test \
-e MICROVM_TEST_ROOTFS_DIR=/microvm/rootfs \
-e MICROVM_TEST_WORK_DIR=/tmp/microvm-test
EOF
}

cmd_sync() {
  log "syncing tree to $host:$remote_dir"
  remote "mkdir -p $remote_dir"
  rsync -az --delete \
    -e "ssh -i $ssh_key -o BatchMode=yes -o StrictHostKeyChecking=accept-new" \
    --exclude '.git' --exclude '__pycache__' --exclude '*.test' \
    "$beta9_dir/cmd" "$beta9_dir/pkg" "$beta9_dir/proto" "$beta9_dir/docker" "$beta9_dir/hack" \
    "$beta9_dir/go.mod" "$beta9_dir/go.sum" \
    "$host:$remote_dir/"
}

cmd_build() {
  log "building $image_tag on $host (overlay on $runtime_image)"
  remote "cd $remote_dir && sudo docker buildx build . \
    -f docker/Dockerfile.worker-overlay \
    --build-arg WORKER_RUNTIME_IMAGE=$runtime_image \
    --build-arg MICROVM_DEV=1 \
    --build-arg SOURCE_REVISION=$(git -C "$beta9_dir" rev-parse --short HEAD 2>/dev/null || echo dev) \
    --load -t $image_tag --progress=plain" 2>&1 | tail -n 40
}

cmd_rootfs() {
  log "exporting rootfs tarballs on $host"
  local script="set -euo pipefail; mkdir -p $remote_dir/rootfs; cd $remote_dir/rootfs;"
  local entry name image
  for entry in "${rootfs_images[@]}"; do
    name="${entry%%=*}"
    image="${entry#*=}"
    script+=" if [ ! -s $name.tar ]; then"
    script+="   sudo docker pull -q $image >/dev/null;"
    script+="   cid=\$(sudo docker create $image true);"
    script+="   sudo docker export \$cid > $name.tar.tmp && mv $name.tar.tmp $name.tar;"
    script+="   sudo docker rm \$cid >/dev/null;"
    script+="   echo exported $name.tar;"
    script+=" fi;"
  done
  remote "$script"
}

cmd_test() {
  local run_filter="${1:-}"
  local filter_args=""
  if [ -n "$run_filter" ]; then
    filter_args="-test.run '$run_filter'"
  fi
  log "running microvm integration tests in $image_tag on $host"
  remote "sudo docker run --rm $(docker_run_flags) $image_tag \
    /usr/local/bin/microvm.test -test.v -test.timeout ${MICROVM_TEST_TIMEOUT:-45m} $filter_args"
}

cmd_shell() {
  remote -t "sudo docker run --rm -it $(docker_run_flags) $image_tag bash"
}

cmd_preflight() {
  log "preflight on $host"
  remote 'set -e; ls -l /dev/kvm /dev/vhost-vsock /dev/net/tun; lsmod | grep -E "^kvm|^vhost"; echo "cgroup=$(stat -fc %T /sys/fs/cgroup)"; sudo docker image inspect '"$runtime_image"' --format "runtime image: {{.Id}} {{.Size}}"'
}

main() {
  local cmd="${1:-all}"
  shift || true
  case "$cmd" in
    all)
      cmd_preflight
      cmd_sync
      cmd_build
      cmd_rootfs
      cmd_test "${1:-}"
      ;;
    preflight) cmd_preflight ;;
    sync) cmd_sync ;;
    build) cmd_sync; cmd_build ;;
    rootfs) cmd_rootfs ;;
    test) cmd_test "${1:-}" ;;
    shell) cmd_shell ;;
    *) fail "unknown command $cmd (all|preflight|sync|build|rootfs|test [regex]|shell)" ;;
  esac
}

main "$@"
