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
  printf 'refusing Patroni Oracle base without pinned manifest %s: %s\n' "$oracle_base_digest" "$oracle_base_image" >&2
  exit 1
fi
if [[ -n ${GO_PATRONI_PATRONI_TAG:-} ]]; then
  tags=("$GO_PATRONI_PATRONI_TAG")
else
  read -r -a tags <<<"${GO_PATRONI_PATRONI_TAGS:-v3.0.4 v3.1.2 v3.2.2 v3.3.11 v4.0.10 v4.1.4}"
fi
containers=()
networks=()
test_binary=""
lab_dir=""
secrets=()

safe_logs() {
  local container=$1 output secret
  output=$(docker logs --tail 400 "$container" 2>&1 || true)
  for secret in "${secrets[@]}"; do
    output=${output//"$secret"/[REDACTED]}
  done
  printf '%s\n' "$output" >&2
}

cleanup() {
  for container in "${containers[@]}"; do
    docker rm -f "$container" >/dev/null 2>&1 || true
  done
  for network in "${networks[@]}"; do
    docker network rm "$network" >/dev/null 2>&1 || true
  done
  if [[ -n $test_binary ]]; then
    rm -f "$test_binary"
  fi
  if [[ -n $lab_dir ]]; then
    rm -rf "$lab_dir"
  fi
}
trap cleanup EXIT

if [[ ! -d $patroni_source/.git ]]; then
  printf 'PATRONI_SOURCE is not a git checkout: %s\n' "$patroni_source" >&2
  exit 1
fi
if [[ -n $(git -C "$patroni_source" status --porcelain=v1) ]]; then
  printf 'refusing Patroni oracle build from a dirty source checkout\n' >&2
  exit 1
fi

cd "$repo_root"
printf 'Patroni Oracle base: image=%s manifest=%s\n' "$oracle_base_image" "$oracle_base_digest"
test_binary=$(mktemp "${TMPDIR:-/tmp}/go-patroni-patroni-integration.XXXXXX")
lab_dir=$(mktemp -d "${TMPDIR:-/tmp}/go-patroni-patroni-lab.XXXXXX")
"$go_command" test -c -tags=integration -o "$test_binary" ./test/integration

for index in "${!tags[@]}"; do
  tag=${tags[$index]}
  version=${tag#v}
  case "$version" in
    3.0.*|3.1.*|3.2.*|3.3.*|4.0.*|4.1.*) ;;
    *) printf 'refusing unsupported Patroni oracle tag: %s\n' "$tag" >&2; exit 1 ;;
  esac
  commit=$(git -C "$patroni_source" rev-parse "$tag^{commit}")
  context="$lab_dir/source-$index"
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

  instance="$lab_dir/instance-$index"
  mkdir -p "$instance/tls"
  openssl req -x509 -newkey rsa:2048 -nodes -sha256 -days 1 \
    -subj "/CN=go-patroni-patroni-$version-ca" \
    -addext 'basicConstraints=critical,CA:TRUE' \
    -addext 'keyUsage=critical,keyCertSign,cRLSign,digitalSignature' \
    -addext 'subjectKeyIdentifier=hash' \
    -keyout "$instance/tls/ca.key" -out "$instance/tls/ca.crt" >/dev/null 2>&1
  openssl req -newkey rsa:2048 -nodes -sha256 -subj /CN=127.0.0.1 \
    -keyout "$instance/tls/server.key" -out "$instance/tls/server.csr" >/dev/null 2>&1
  openssl x509 -req -sha256 -days 1 \
    -in "$instance/tls/server.csr" -CA "$instance/tls/ca.crt" -CAkey "$instance/tls/ca.key" -CAcreateserial \
    -extfile <(printf 'basicConstraints=critical,CA:FALSE\nkeyUsage=critical,digitalSignature,keyEncipherment\nsubjectAltName=IP:127.0.0.1,DNS:localhost\nextendedKeyUsage=serverAuth\n') \
    -out "$instance/tls/server.crt" >/dev/null 2>&1
  openssl req -newkey rsa:2048 -nodes -sha256 -subj /CN=go-patroni-integration-client \
    -keyout "$instance/tls/client.key" -out "$instance/tls/client.csr" >/dev/null 2>&1
  openssl x509 -req -sha256 -days 1 \
    -in "$instance/tls/client.csr" -CA "$instance/tls/ca.crt" -CAkey "$instance/tls/ca.key" -CAcreateserial \
    -extfile <(printf 'basicConstraints=critical,CA:FALSE\nkeyUsage=critical,digitalSignature,keyEncipherment\nextendedKeyUsage=clientAuth\n') \
    -out "$instance/tls/client.crt" >/dev/null 2>&1
  chmod 600 "$instance/tls/"*.key

  rest_password=$(openssl rand -hex 24)
  superuser_password=$(openssl rand -hex 24)
  replication_password=$(openssl rand -hex 24)
  rewind_password=$(openssl rand -hex 24)
  secrets+=("$rest_password" "$superuser_password" "$replication_password" "$rewind_password")

  network="go-patroni-patroni-net-${PPID}-${index}-${RANDOM}"
  etcd_container="go-patroni-patroni-etcd-${PPID}-${index}-${RANDOM}"
  patroni_container="go-patroni-patroni-node-${PPID}-${index}-${RANDOM}"
  scope="go-patroni-rest-${PPID}-${index}-${RANDOM}"
  networks+=("$network")
  containers+=("$etcd_container" "$patroni_container")

  cat >"$instance/patroni.yml" <<EOF
scope: $scope
namespace: /go-patroni-test/
name: node1
restapi:
  listen: 0.0.0.0:8008
  connect_address: $patroni_container:8008
  cafile: /var/lib/postgresql/go-patroni/lab/ca.crt
  certfile: /var/lib/postgresql/go-patroni/lab/server.crt
  keyfile: /var/lib/postgresql/go-patroni/lab/server.key
  verify_client: required
  authentication:
    username: go-patroni
    password: $rest_password
  https_extra_headers:
    X-GoPatroni-Lab: isolated
ctl:
  cacert: /var/lib/postgresql/go-patroni/lab/ca.crt
  certfile: /var/lib/postgresql/go-patroni/lab/client.crt
  keyfile: /var/lib/postgresql/go-patroni/lab/client.key
  authentication:
    username: go-patroni
    password: $rest_password
etcd3:
  host: $etcd_container:2379
bootstrap:
  dcs:
    ttl: 20
    loop_wait: 2
    retry_timeout: 5
    maximum_lag_on_failover: 1048576
    postgresql:
      use_pg_rewind: false
      use_slots: true
      pg_hba:
        - host replication replicator 0.0.0.0/0 scram-sha-256
        - host all all 0.0.0.0/0 scram-sha-256
  initdb:
    - encoding: UTF8
    - data-checksums
postgresql:
  listen: 0.0.0.0:5432
  connect_address: $patroni_container:5432
  data_dir: /var/lib/postgresql/go-patroni/data
  pgpass: /var/lib/postgresql/go-patroni/lab/pgpass
  authentication:
    superuser:
      username: postgres
      password: $superuser_password
    replication:
      username: replicator
      password: $replication_password
    rewind:
      username: rewind_user
      password: $rewind_password
  parameters:
    unix_socket_directories: /tmp
tags:
  go_patroni_lab: isolated
EOF
  chmod 600 "$instance/patroni.yml"

  # A dedicated bridge keeps lab naming/state isolated. Docker Desktop drops
  # loopback publications for --internal networks, so etcd remains unexposed
  # while only Patroni's random host port is explicitly bound below.
  docker network create "$network" >/dev/null
  docker run --detach --rm \
    --network "$network" --name "$etcd_container" \
    quay.io/coreos/etcd:v3.6.13 \
    /usr/local/bin/etcd \
    --name go-patroni-patroni-etcd \
    --data-dir /tmp/go-patroni-patroni-etcd \
    --listen-client-urls http://0.0.0.0:2379 \
    --advertise-client-urls "http://$etcd_container:2379" \
    --listen-peer-urls http://0.0.0.0:2380 >/dev/null
  for _ in $(seq 1 60); do
    if docker exec "$etcd_container" /usr/local/bin/etcdctl --endpoints=http://127.0.0.1:2379 endpoint health >/dev/null 2>&1; then
      break
    fi
    sleep 0.25
  done
  if ! docker exec "$etcd_container" /usr/local/bin/etcdctl --endpoints=http://127.0.0.1:2379 endpoint health >/dev/null 2>&1; then
    safe_logs "$etcd_container"
    printf 'isolated Patroni etcd did not become healthy\n' >&2
    exit 1
  fi

  docker run --detach --rm \
    --network "$network" --name "$patroni_container" \
    --publish 127.0.0.1::8008 \
    --user root \
    --mount "type=bind,source=$instance,target=/run/go-patroni-source,readonly" \
    --entrypoint /bin/sh \
    "$image" \
    -eu -c 'install -d -o postgres -g postgres -m 700 /var/lib/postgresql/go-patroni/lab
      install -o postgres -g postgres -m 600 /run/go-patroni-source/patroni.yml /var/lib/postgresql/go-patroni/lab/patroni.yml
      install -o postgres -g postgres -m 644 /run/go-patroni-source/tls/ca.crt /var/lib/postgresql/go-patroni/lab/ca.crt
      install -o postgres -g postgres -m 644 /run/go-patroni-source/tls/server.crt /var/lib/postgresql/go-patroni/lab/server.crt
      install -o postgres -g postgres -m 600 /run/go-patroni-source/tls/server.key /var/lib/postgresql/go-patroni/lab/server.key
      install -o postgres -g postgres -m 644 /run/go-patroni-source/tls/client.crt /var/lib/postgresql/go-patroni/lab/client.crt
      install -o postgres -g postgres -m 600 /run/go-patroni-source/tls/client.key /var/lib/postgresql/go-patroni/lab/client.key
      exec gosu postgres patroni /var/lib/postgresql/go-patroni/lab/patroni.yml' >/dev/null

  published=$(docker port "$patroni_container" 8008/tcp | head -n 1)
  if [[ $published != 127.0.0.1:* ]]; then
    printf 'unexpected Patroni loopback publication: %s\n' "$published" >&2
    exit 1
  fi
  base_url="https://$published"
  ready=0
  for _ in $(seq 1 240); do
    if curl --silent --show-error --fail \
      --cacert "$instance/tls/ca.crt" --cert "$instance/tls/client.crt" --key "$instance/tls/client.key" \
      "$base_url/readiness" >/dev/null 2>&1; then
      ready=1
      break
    fi
    if ! docker inspect "$patroni_container" --format '{{.State.Running}}' 2>/dev/null | rg -q '^true$'; then
      break
    fi
    sleep 0.5
  done
  if [[ $ready != 1 ]]; then
    safe_logs "$patroni_container"
    printf 'isolated Patroni did not become ready: %s\n' "$tag" >&2
    exit 1
  fi

  image_id=$(docker image inspect "$image" --format '{{.Id}}')
  printf 'Patroni REST integration: version=%s tag=%s commit=%s image=%s\n' "$version" "$tag" "$commit" "$image_id"
  if ! GO_PATRONI_TEST_PATRONI_ISOLATED=1 \
    GO_PATRONI_TEST_PATRONI_URL="$base_url" \
    GO_PATRONI_TEST_PATRONI_VERSION="$version" \
    GO_PATRONI_TEST_PATRONI_CA="$instance/tls/ca.crt" \
    GO_PATRONI_TEST_PATRONI_CLIENT_CERT="$instance/tls/client.crt" \
    GO_PATRONI_TEST_PATRONI_CLIENT_KEY="$instance/tls/client.key" \
    GO_PATRONI_TEST_PATRONI_USERNAME=go-patroni \
    GO_PATRONI_TEST_PATRONI_PASSWORD="$rest_password" \
    "$test_binary" -test.count=1 -test.run '^TestPatroniRESTInventoryAgainstIsolatedRealPatroni$'; then
    safe_logs "$patroni_container"
    exit 1
  fi

  docker rm -f "$patroni_container" "$etcd_container" >/dev/null
  docker network rm "$network" >/dev/null
done
