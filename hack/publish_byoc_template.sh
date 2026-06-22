#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<EOF
Usage:
  $0 <bucket> <version> [alias...]

Examples:
  $0 beam-byoc-templates-stage-683656326989-us-east-1 "$(git rev-parse --short HEAD)" byoc-template.yaml
  $0 beam-byoc-templates-prod-187248174200-us-east-1 "0.1.670" byoc-template.yaml latest

Environment:
  AWS_REGION      AWS region for CloudFormation validation and S3 writes. Defaults to us-east-1.
  TEMPLATE_PATH   Template file to publish. Defaults to pkg/gateway/services/compute/templates/aws/byoc.yaml.
EOF
}

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
  usage
  exit 0
fi

if [[ $# -lt 2 ]]; then
  usage >&2
  exit 2
fi

require_cmd() {
  command -v "$1" >/dev/null 2>&1 || {
    echo "missing required command: $1" >&2
    exit 1
  }
}

alias_key() {
  local alias="$1"
  if [[ "$alias" == *.yaml || "$alias" == *.yml ]]; then
    printf "aws/%s" "$alias"
  else
    printf "aws/byoc-template-%s.yaml" "$alias"
  fi
}

require_cmd aws

bucket="$1"
version="$2"
shift 2

region="${AWS_REGION:-us-east-1}"
template_path="${TEMPLATE_PATH:-pkg/gateway/services/compute/templates/aws/byoc.yaml}"

if [[ ! "$version" =~ ^[A-Za-z0-9._-]+$ ]]; then
  echo "version may only contain letters, numbers, dot, underscore, and dash: $version" >&2
  exit 2
fi

if [[ ! -f "$template_path" ]]; then
  echo "template file does not exist: $template_path" >&2
  exit 1
fi

aws cloudformation validate-template \
  --template-body "file://${template_path}" \
  --region "$region" >/dev/null

version_key="aws/byoc-template-${version}.yaml"
version_url="https://s3.${region}.amazonaws.com/${bucket}/${version_key}"

aws s3api put-object \
  --bucket "$bucket" \
  --key "$version_key" \
  --body "$template_path" \
  --acl public-read \
  --content-type "application/yaml" \
  --cache-control "public, max-age=31536000, immutable" \
  --region "$region" >/dev/null

aws s3api head-object \
  --bucket "$bucket" \
  --key "$version_key" \
  --region "$region" >/dev/null

printf "published %s\n" "$version_url"

for alias in "$@"; do
  key="$(alias_key "$alias")"
  url="https://s3.${region}.amazonaws.com/${bucket}/${key}"

  aws s3api put-object \
    --bucket "$bucket" \
    --key "$key" \
    --body "$template_path" \
    --acl public-read \
    --content-type "application/yaml" \
    --cache-control "no-cache" \
    --region "$region" >/dev/null

  aws s3api head-object \
    --bucket "$bucket" \
    --key "$key" \
    --region "$region" >/dev/null

  printf "published %s\n" "$url"
done
