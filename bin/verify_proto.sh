#!/usr/bin/env bash
# This scripts is automatically run by CI to prevent pull requests missing running genproto.sh
# after changing *.proto file.

set -o errexit
set -o nounset
set -o pipefail

tmpWorkRoot=$(mktemp -d -t 'twd.XXXXXX')
trap 'rm -rf "$tmpWorkRoot"' EXIT
tmpWorkDir="$tmpWorkRoot"
mkdir "$tmpWorkDir/beta9"
tmpWorkDir="$tmpWorkDir/beta9"
cp -r . "$tmpWorkDir"
pushd "$tmpWorkDir"
# Linked worktrees carry a .git pointer to the original checkout.
rm -rf .git sdk/.venv
git init -q
git add -A
git -c user.name="proto verifier" -c user.email="proto-verifier@example.com" commit --allow-empty -m init
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
