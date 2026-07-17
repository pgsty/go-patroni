#!/usr/bin/env bash
set -euo pipefail

repo_root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
source "$repo_root/scripts/go-toolchain.sh"
activate_go_patroni_go_toolchain "$repo_root"
go_command=$GO_PATRONI_GO_COMMAND
if [[ -n ${GO_PATRONI_ETCD_IMAGE:-} ]]; then
  images=("$GO_PATRONI_ETCD_IMAGE")
else
  read -r -a images <<<"${GO_PATRONI_ETCD_IMAGES:-quay.io/coreos/etcd:v3.5.26 quay.io/coreos/etcd:v3.6.13 quay.io/coreos/etcd:v3.7.0}"
fi
containers=()
test_binary=""

cleanup() {
  for container in "${containers[@]}"; do
    docker rm -f "$container" >/dev/null 2>&1 || true
  done
  if [[ -n $test_binary ]]; then
    rm -f "$test_binary"
  fi
}
trap cleanup EXIT

cd "$repo_root"
test_binary=$(mktemp "${TMPDIR:-/tmp}/go-patroni-integration.XXXXXX")
"$go_command" test -c -tags=integration -o "$test_binary" ./test/integration
for index in "${!images[@]}"; do
  image=${images[$index]}
  container="go-patroni-etcd-${PPID}-${index}-${RANDOM}"
  namespace="go-patroni-test-${PPID}-${index}-${RANDOM}"
  containers+=("$container")

  docker run --detach --rm \
    --name "$container" \
    --publish 127.0.0.1::2379 \
    "$image" \
    /usr/local/bin/etcd \
    --name go-patroni-test \
    --data-dir /tmp/go-patroni-etcd-data \
    --listen-client-urls http://0.0.0.0:2379 \
    --advertise-client-urls http://127.0.0.1:2379 \
    --listen-peer-urls http://0.0.0.0:2380 >/dev/null

  for _ in $(seq 1 60); do
    if docker exec "$container" /usr/local/bin/etcdctl \
      --endpoints=http://127.0.0.1:2379 endpoint health >/dev/null 2>&1; then
      break
    fi
    sleep 0.25
  done
  if ! docker exec "$container" /usr/local/bin/etcdctl \
    --endpoints=http://127.0.0.1:2379 endpoint health >/dev/null 2>&1; then
    docker logs "$container" >&2
    printf 'isolated etcd did not become healthy: %s\n' "$image" >&2
    exit 1
  fi

  published=$(docker port "$container" 2379/tcp | head -n 1)
  if [[ $published != 127.0.0.1:* ]]; then
    printf 'unexpected etcd loopback publication: %s\n' "$published" >&2
    exit 1
  fi

  version=$(docker exec "$container" /usr/local/bin/etcd --version | head -n 1)
  digest=$(docker image inspect "$image" --format '{{index .RepoDigests 0}}')
  printf 'etcd integration: %s image=%s\n' "$version" "$digest"
  GO_PATRONI_TEST_ETCD_ISOLATED=1 \
  GO_PATRONI_TEST_ETCD_ENDPOINT="http://${published}" \
  GO_PATRONI_TEST_ETCD_NAMESPACE="$namespace" \
  "$test_binary" -test.count=1 -test.run '^TestEtcd3PatroniSnapshotDiscoveryCASRemoveAndWatch$'

  docker rm -f "$container" >/dev/null
done
