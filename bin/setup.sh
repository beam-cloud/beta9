#!/usr/bin/env bash

set +xeu

os=$(uname -s | tr '[:upper:]' '[:lower:]')

if [ "$(uname -m)" = "arm64" ]; then
  arch="arm64"
elif [ "$(uname -m)" = "x86_64" ]; then
  arch="amd64"
fi

k8s_version=$(curl -sSfL https://dl.k8s.io/release/stable.txt)
stern_version="1.28.0"

echo "Installing kubectl"
curl -sSfL "https://dl.k8s.io/release/${k8s_version}/bin/${os}/${arch}/kubectl" > /usr/local/bin/kubectl
chmod +x /usr/local/bin/kubectl

echo "Installing stern"
curl -sSfL "https://github.com/stern/stern/releases/download/v${stern_version}/stern_${stern_version}_${os}_${arch}.tar.gz" | tar -xz -C /usr/local/bin stern
chmod +x /usr/local/bin/stern

echo "Installing okteto"
curl -sSfL https://get.okteto.com | sh

echo "Installing k3d"
curl -sSfL https://raw.githubusercontent.com/k3d-io/k3d/main/install.sh | bash
