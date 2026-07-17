#!/usr/bin/env bash
set -euo pipefail

repo_root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
source "$repo_root/scripts/go-toolchain.sh"
activate_go_patroni_go_toolchain "$repo_root"
go_command=$GO_PATRONI_GO_COMMAND
patroni_source=${PATRONI_SOURCE:-$(cd "$repo_root/../dev/patroni" && pwd)}
work_dir=$(mktemp -d "${TMPDIR:-/tmp}/go-patroni-patronictl-compat.XXXXXX")

cleanup() {
  rm -rf "$work_dir"
}
trap cleanup EXIT

if ! command -v docker >/dev/null 2>&1 || ! docker info >/dev/null 2>&1; then
  printf 'Docker is required for the pinned patronictl semantic oracle\n' >&2
  exit 1
fi
if [[ ! -d $patroni_source/.git ]]; then
  printf 'PATRONI_SOURCE is not a git checkout: %s\n' "$patroni_source" >&2
  exit 1
fi
if [[ -n $(git -C "$patroni_source" status --porcelain=v1) ]]; then
  printf 'refusing patronictl oracle build from a dirty Patroni checkout\n' >&2
  exit 1
fi

declare -A outputs
for tag in v4.0.7 v4.1.3; do
  version=${tag#v}
  commit=$(git -C "$patroni_source" rev-parse "$tag^{commit}")
  context="$work_dir/source-$version"
  mkdir -p "$context"
  git -C "$patroni_source" archive "$tag" | tar -x -C "$context"
  cp "$repo_root/test/compat/oracle/patroni-rest-constraints.txt" "$context/go-patroni-oracle-constraints.txt"

  image="go-patroni-patroni-rest-oracle:${version}-${commit:0:12}"
  docker build \
    --quiet \
    --provenance=false \
    --file "$repo_root/test/compat/oracle/Dockerfile.patroni-rest" \
    --tag "$image" \
    "$context" >/dev/null

  output="$work_dir/patronictl-$version.json"
  docker run --rm --entrypoint sh \
    --mount "type=bind,source=$repo_root/test/compat/oracle/patronictl_semantics.py,target=/oracle.py,readonly" \
    "$image" -c 'cd /src && python /oracle.py' >"$output"
  outputs[$version]=$output

  image_id=$(docker image inspect "$image" --format '{{.Id}}')
  digest=$(shasum -a 256 "$output" | awk '{print $1}')
  printf 'patronictl oracle: version=%s tag=%s commit=%s image=%s facts_sha256=%s\n' \
    "$version" "$tag" "$commit" "$image_id" "$digest"
done

cd "$repo_root"
GO_PATRONI_PATRONICTL_ORACLE_40=${outputs[4.0.7]} \
GO_PATRONI_PATRONICTL_ORACLE_41=${outputs[4.1.3]} \
GOSUMDB=off GOPROXY=off \
"$go_command" test -count=1 -tags=oracle ./internal/cli -run '^TestPatronictlSemanticParity$'

printf 'patronictl semantic differential: 4.0.7=26 cases 4.1.3=29 cases status=PASS\n'
