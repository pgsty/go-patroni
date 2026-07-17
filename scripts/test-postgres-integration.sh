#!/usr/bin/env bash
set -euo pipefail

repo_root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
source "$repo_root/scripts/go-toolchain.sh"
activate_go_patroni_go_toolchain "$repo_root"
go_command=$GO_PATRONI_GO_COMMAND
if [[ -n ${GO_PATRONI_POSTGRES_IMAGE:-} ]]; then
  images=("$GO_PATRONI_POSTGRES_IMAGE")
else
  read -r -a images <<<"${GO_PATRONI_POSTGRES_IMAGES:-postgres:14-alpine postgres:16-alpine postgres:18-alpine}"
fi
containers=()
test_binary=""
lab_dir=""

cleanup() {
  for container in "${containers[@]}"; do
    docker rm -f "$container" >/dev/null 2>&1 || true
  done
  if [[ -n $test_binary ]]; then
    rm -f "$test_binary"
  fi
  if [[ -n $lab_dir ]]; then
    rm -rf "$lab_dir"
  fi
}
trap cleanup EXIT

cd "$repo_root"
test_binary=$(mktemp "${TMPDIR:-/tmp}/go-patroni-postgres-integration.XXXXXX")
lab_dir=$(mktemp -d "${TMPDIR:-/tmp}/go-patroni-postgres-lab.XXXXXX")
mkdir -p "$lab_dir/tls"

openssl req -x509 -newkey rsa:2048 -nodes -sha256 -days 1 \
  -subj /CN=go-patroni-m2-postgres-ca \
  -keyout "$lab_dir/tls/ca.key" -out "$lab_dir/tls/ca.crt" >/dev/null 2>&1
openssl req -newkey rsa:2048 -nodes -sha256 \
  -subj /CN=127.0.0.1 \
  -keyout "$lab_dir/tls/server.key" -out "$lab_dir/tls/server.csr" >/dev/null 2>&1
openssl x509 -req -sha256 -days 1 \
  -in "$lab_dir/tls/server.csr" -CA "$lab_dir/tls/ca.crt" -CAkey "$lab_dir/tls/ca.key" -CAcreateserial \
  -extfile <(printf 'subjectAltName=IP:127.0.0.1\nextendedKeyUsage=serverAuth\n') \
  -out "$lab_dir/tls/server.crt" >/dev/null 2>&1
chmod 600 "$lab_dir/tls/server.key"

openssl rand -base64 32 | tr -d '\n' >"$lab_dir/postgres-password"
chmod 600 "$lab_dir/postgres-password"

"$go_command" test -c -tags=integration -o "$test_binary" ./test/integration
for index in "${!images[@]}"; do
  image=${images[$index]}
  container="go-patroni-postgres-${PPID}-${index}-${RANDOM}"
  containers+=("$container")

  docker run --detach --rm \
    --name "$container" \
    --publish 127.0.0.1::5432 \
    --env POSTGRES_PASSWORD_FILE=/run/go-patroni-secret/postgres-password \
    --env POSTGRES_INITDB_ARGS=--auth-host=scram-sha-256 \
    --mount "type=bind,source=$lab_dir/postgres-password,target=/run/go-patroni-secret/postgres-password,readonly" \
    --mount "type=bind,source=$lab_dir/tls,target=/run/go-patroni-source-tls,readonly" \
    --entrypoint /bin/sh \
    "$image" \
    -eu -c 'install -d -o postgres -g postgres -m 700 /var/lib/postgresql/go-patroni-tls
      install -o postgres -g postgres -m 600 /run/go-patroni-source-tls/server.key /var/lib/postgresql/go-patroni-tls/server.key
      install -o postgres -g postgres -m 644 /run/go-patroni-source-tls/server.crt /var/lib/postgresql/go-patroni-tls/server.crt
      exec /usr/local/bin/docker-entrypoint.sh postgres -c ssl=on -c ssl_cert_file=/var/lib/postgresql/go-patroni-tls/server.crt -c ssl_key_file=/var/lib/postgresql/go-patroni-tls/server.key' >/dev/null

  for _ in $(seq 1 120); do
    # Probe TCP explicitly. The official entrypoint briefly starts a
    # socket-only bootstrap server; accepting that transient process would
    # race the real TLS-enabled server startup on slower PostgreSQL majors.
    if docker exec "$container" pg_isready -h 127.0.0.1 -p 5432 -U postgres -d postgres >/dev/null 2>&1; then
      break
    fi
    sleep 0.25
  done
  if ! docker exec "$container" pg_isready -h 127.0.0.1 -p 5432 -U postgres -d postgres >/dev/null 2>&1; then
    docker logs "$container" >&2
    printf 'isolated PostgreSQL did not become healthy: %s\n' "$image" >&2
    exit 1
  fi

  published=$(docker port "$container" 5432/tcp | head -n 1)
  if [[ $published != 127.0.0.1:* ]]; then
    printf 'unexpected PostgreSQL loopback publication: %s\n' "$published" >&2
    exit 1
  fi
  port=${published##*:}
  password=$(<"$lab_dir/postgres-password")
  printf '127.0.0.1:%s:postgres:postgres:%s\nlocalhost:%s:postgres:postgres:%s\n' \
    "$port" "$password" "$port" "$password" >"$lab_dir/pgpass"
  unset password
  chmod 600 "$lab_dir/pgpass"

  version=$(docker exec "$container" postgres --version)
  digest=$(docker image inspect "$image" --format '{{index .RepoDigests 0}}')
  printf 'PostgreSQL integration: %s image=%s\n' "$version" "$digest"
  PGPASSFILE="$lab_dir/pgpass" \
  GO_PATRONI_TEST_POSTGRES_ISOLATED=1 \
  GO_PATRONI_TEST_POSTGRES_HOST=127.0.0.1 \
  GO_PATRONI_TEST_POSTGRES_PORT="$port" \
  GO_PATRONI_TEST_POSTGRES_CA="$lab_dir/tls/ca.crt" \
  "$test_binary" -test.count=1 -test.run '^TestPostgreSQLNativeQueryTLSAuthMultiResultLimitsErrorAndCancel$'

  docker rm -f "$container" >/dev/null
done
