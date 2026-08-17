#!/usr/bin/env bash
# This scripts is automatically run by CI to prevent pull requests missing running genproto.sh
# after changing *.proto file.

set -o errexit
set -o nounset
set -o pipefail

tmpRoot=$(mktemp -d -t 'twd.XXXXXX')
trap 'rm -rf "$tmpRoot"' EXIT
tmpWorkDir="$tmpRoot/beta9"
mkdir "$tmpWorkDir"
rsync -a \
  --exclude '.venv' \
  --exclude '.mypy_cache' \
  --exclude '.pytest_cache' \
  --exclude '__pycache__' \
  pkg proto sdk googleapis "$tmpWorkDir/"
mkdir -p "$tmpWorkDir/bin" "$tmpWorkDir/docs"
rsync -a bin/gen_proto.sh "$tmpWorkDir/bin/"
rsync -a docs/openapi "$tmpWorkDir/docs/"
rsync -a go.mod go.sum "$tmpWorkDir/"
pushd "$tmpWorkDir"
git init -q
git add -A
git -c user.name="proto verifier" -c user.email="proto-verifier@example.com" commit -q --allow-empty -m init
./bin/gen_proto.sh
diff=$(git --no-pager diff )
popd
if [ -z "$diff" ]; then
  echo "PASSED genproto-verification!"
  exit 0
fi
echo "Failed genproto-verification!" >&2
printf "* Found changed files:\n%s\n" "$diff" >&2
echo "* Please rerun genproto.sh after changing *.proto file" >&2
echo "* Run ./scripts/gen_proto.sh" >&2
exit 1
