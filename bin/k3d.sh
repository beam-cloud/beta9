#!/usr/bin/env bash

set -eu

check_gpu_linux() {
  if command -v nvidia-smi >/dev/null 2>&1; then
    nvidia-smi -L | grep -q "GPU"
    return $?
  else
    echo "nvidia-smi command not found."
    return 1
  fi
}

k3d_up() {
  os_type="$(uname)"
  case "$os_type" in
    Linux*)
      check_gpu_linux && extra_args="--gpus=all --image=localhost:5001/rancher/k3s:latest" || extra_args=""
      docker build . -f ./docker/Dockerfile.k3d -t localhost:5001/rancher/k3s:latest
      ;;
    Darwin*)
      extra_args=""
      ;;
    *)
      echo "Unsupported OS: $os_type"
      exit 1
      ;;
  esac

  k3d cluster create --config hack/k3d.yaml $extra_args
  kubectl config set contexts.k3d-beam.namespace beam
  okteto context use k3d-beam --namespace beam
}

k3d_down() {
  k3d cluster delete --config hack/k3d.yaml
}

case "$1" in
  up)   k3d_up ;;
  down) k3d_down ;;
  *)    echo "Unsupported command: $1"; exit 1 ;;
esac
