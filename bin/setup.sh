#!/usr/bin/env bash

# ----------------------------------------------
# This script is used to setup the environment for Kubernetes development.
# It installs kubectl, kustomize, stern, helm, okteto, and k3d on the machine.
# It determines the operating system and architecture of the machine to download the appropriate binaries.
# ----------------------------------------------

set +xeu

os=$(uname -s | tr '[:upper:]' '[:lower:]')

if [ "$(uname -m)" = "arm64" ]; then
  arch="arm64"
elif [ "$(uname -m)" = "x86_64" ]; then
  arch="amd64"
fi

k8s_version=$(curl -sSfL https://dl.k8s.io/release/stable.txt)
kustomize_version="5.4.2"
stern_version="1.28.0"
helm_verision="3.14.0"

echo "Installing kubectl"
curl -sSfL "https://dl.k8s.io/release/${k8s_version}/bin/${os}/${arch}/kubectl" > /usr/local/bin/kubectl
chmod +x /usr/local/bin/kubectl

echo "Installing kustomize"
curl -sSfL https://github.com/kubernetes-sigs/kustomize/releases/download/kustomize%2Fv${kustomize_version}/kustomize_v${kustomize_version}_${os}_${arch}.tar.gz | tar -xz -C /usr/local/bin kustomize
chmod +x /usr/local/bin/kustomize

echo "Installing stern"
curl -sSfL "https://github.com/stern/stern/releases/download/v${stern_version}/stern_${stern_version}_${os}_${arch}.tar.gz" | tar -xz -C /usr/local/bin stern
chmod +x /usr/local/bin/stern

echo "Installing helm"
curl https://raw.githubusercontent.com/helm/helm/main/scripts/get-helm-3 | DESIRED_VERSION=v$helm_verision bash

echo "Installing okteto"
curl -sSfL https://get.okteto.com | sh

echo "Installing k3d"
curl -sSfL https://raw.githubusercontent.com/k3d-io/k3d/main/install.sh | bash

echo "Installing go2proto"
go get -u github.com/beam-cloud/go2proto@latest
go install github.com/beam-cloud/go2proto