#!/usr/bin/env bash

# ----------------------------------------------
# This script is used to setup the environment for Kubernetes development.
# It installs kubectl, kustomize, stern, helm, okteto, and k3d on the machine.
# It determines the operating system and architecture of the machine to download the appropriate binaries.
# ----------------------------------------------

set +x
set -euo pipefail

os=$(uname -s | tr '[:upper:]' '[:lower:]')
if [ "$(uname -m)" = "arm64" ] || [ "$(uname -m)" = "aarch64" ]; then
  arch="arm64"
elif [ "$(uname -m)" = "x86_64" ]; then
  arch="amd64"
fi

k8s_version=$(curl -sSfL https://cdn.dl.k8s.io/release/stable-1.29.txt)
k3d_version="5.8.1"
kustomize_version="5.6.0"
stern_version="1.32.0"
helm_verision="3.17.0"
okteto_version="3.3.1"

function err() {
  echo -e "\033[0;31m$1\033[0m"
}
function sucess() {
  echo -e "\033[0;32m$1\033[0m"
}
function check_status() {
  if [ $? -eq 0 ]; then
    sucess "Installed successfully"
  else
    err "Installation failed"
  fi
  echo ""
}

echo "=> Installing kubectl ${k8s_version}"
curl -sSfL "https://dl.k8s.io/release/${k8s_version}/bin/${os}/${arch}/kubectl" > /usr/local/bin/kubectl
chmod +x /usr/local/bin/kubectl
check_status

echo "=> Installing kustomize ${kustomize_version}"
curl -sSfL https://github.com/kubernetes-sigs/kustomize/releases/download/kustomize%2Fv${kustomize_version}/kustomize_v${kustomize_version}_${os}_${arch}.tar.gz | tar -xz -C /usr/local/bin kustomize
chmod +x /usr/local/bin/kustomize
check_status

echo "=> Installing stern ${stern_version}"
curl -sSfL "https://github.com/stern/stern/releases/download/v${stern_version}/stern_${stern_version}_${os}_${arch}.tar.gz" | tar -xz -C /usr/local/bin stern
chmod +x /usr/local/bin/stern
check_status

echo "=> Installing helm ${helm_verision}"
curl -sSfL https://raw.githubusercontent.com/helm/helm/main/scripts/get-helm-3 | DESIRED_VERSION=v$helm_verision bash
check_status

echo "=> Installing okteto ${okteto_version}"
curl -sSfL https://get.okteto.com | OKTETO_VERSION=${okteto_version} sh
check_status

echo "=> Installing k3d"
curl -sSfL https://raw.githubusercontent.com/k3d-io/k3d/main/install.sh | TAG=v${k3d_version} bash
check_status

echo "=> Installing go2proto"
# go get github.com/beam-cloud/go2proto@latest
# go get google.golang.org/protobuf/cmd/protoc-gen-go@v1.31.0
go install google.golang.org/protobuf/cmd/protoc-gen-go@v1.31.0
go install github.com/beam-cloud/go2proto@latest
check_status
