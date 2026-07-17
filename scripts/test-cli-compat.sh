#!/usr/bin/env bash
set -euo pipefail

repo_root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
source "$repo_root/scripts/go-toolchain.sh"
activate_go_patroni_go_toolchain "$repo_root"
go_command=$GO_PATRONI_GO_COMMAND
patroni_source=${PATRONI_SOURCE:-$(cd "$repo_root/../dev/patroni" && pwd)}
oracle_base_digest=sha256:57c72fd2a128e416c7fcc499958864df5301e940bca0a56f58fddf30ffc07777
oracle_base_image=${GO_PATRONI_ORACLE_BASE_IMAGE:-postgres:16-alpine@$oracle_base_digest}
oracle_build_proxy=${GO_PATRONI_ORACLE_BUILD_PROXY:-}
if [[ ${oracle_base_image##*@} != "$oracle_base_digest" ]]; then
  printf 'refusing patronictl Oracle base without pinned manifest %s: %s\n' "$oracle_base_digest" "$oracle_base_image" >&2
  exit 1
fi
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
printf 'patronictl Oracle base: image=%s manifest=%s\n' "$oracle_base_image" "$oracle_base_digest"
for tag in v4.0.10 v4.1.4; do
  version=${tag#v}
  commit=$(git -C "$patroni_source" rev-parse "$tag^{commit}")
  context="$work_dir/source-$version"
  mkdir -p "$context"
  git -C "$patroni_source" archive "$tag" | tar -x -C "$context"
  cp "$repo_root/test/compat/oracle/patroni-rest-constraints.txt" "$context/go-patroni-oracle-constraints.txt"

  image="go-patroni-patroni-rest-oracle:${version}-${commit:0:12}"
  build_arguments=(--build-arg "GO_PATRONI_ORACLE_BASE_IMAGE=$oracle_base_image")
  if [[ -n $oracle_build_proxy ]]; then
    build_arguments+=(--build-arg "HTTP_PROXY=$oracle_build_proxy" --build-arg "HTTPS_PROXY=$oracle_build_proxy")
  fi
  docker build \
    --quiet \
    --provenance=false \
    "${build_arguments[@]}" \
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
GO_PATRONI_PATRONICTL_ORACLE_40=${outputs[4.0.10]} \
GO_PATRONI_PATRONICTL_ORACLE_41=${outputs[4.1.4]} \
GOSUMDB=off GOPROXY=off \
"$go_command" test -count=1 -tags=oracle ./internal/cli -run '^TestPatronictlSemanticParity$'

printf 'patronictl semantic differential: 4.0.10=26 cases 4.1.4=29 cases status=PASS\n'
