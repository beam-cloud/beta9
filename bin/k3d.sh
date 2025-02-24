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
      if check_gpu_linux; then    
        extra_args="--gpus=all --image=localhost:5001/rancher/k3s:latest"
        docker build . -f ./docker/Dockerfile.k3d -t localhost:5001/rancher/k3s:latest
      else
        extra_args=""
        touch manifests/k3d/nvidia-device-plugin.yaml.skip
      fi
      ;;
    Darwin*)
      extra_args=""
      touch manifests/k3d/nvidia-device-plugin.yaml.skip
      ;;
    *)
      echo "Unsupported OS: $os_type"
      exit 1
      ;;
  esac

  k3d cluster create --config hack/k3d.yaml $extra_args
  kubectl create namespace beta9
  kubectl config set contexts.k3d-beta9.namespace beta9
  okteto context use k3d-beta9 --namespace beta9
}

k3d_down() {
  k3d cluster delete --config hack/k3d.yaml
}

case "$1" in
  up)   k3d_up ;;
  down) k3d_down ;;
  *)    echo "Unsupported command: $1"; exit 1 ;;
esac
