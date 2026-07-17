#!/usr/bin/env bash
set -euo pipefail

repo_root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
source "$repo_root/scripts/go-toolchain.sh"
activate_go_patroni_go_toolchain "$repo_root"
go_command=$GO_PATRONI_GO_COMMAND
image=${GO_PATRONI_ETCD_TLS_IMAGE:-quay.io/coreos/etcd:v3.6.13}
container=""
lab_dir=""
test_binary=""
password=""

safe_logs() {
  local output
  if [[ -z $container ]]; then
    return
  fi
  output=$(docker logs --tail 300 "$container" 2>&1 || true)
  if [[ -n $password ]]; then
    output=${output//"$password"/[REDACTED]}
  fi
  printf '%s\n' "$output" >&2
}

cleanup() {
  if [[ -n $container ]]; then
    docker rm -f "$container" >/dev/null 2>&1 || true
  fi
  if [[ -n $test_binary ]]; then
    rm -f "$test_binary"
  fi
  if [[ -n $lab_dir ]]; then
    rm -rf "$lab_dir"
  fi
}
trap cleanup EXIT

for command in docker openssl go; do
  command -v "$command" >/dev/null || {
    printf 'etcd TLS integration requires %s\n' "$command" >&2
    exit 1
  }
done

cd "$repo_root"
lab_dir=$(mktemp -d "${TMPDIR:-/tmp}/go-patroni-etcd-tls-lab.XXXXXX")
test_binary=$(mktemp "${TMPDIR:-/tmp}/go-patroni-etcd-tls-integration.XXXXXX")
mkdir -p "$lab_dir/tls"

openssl req -x509 -newkey rsa:2048 -nodes -sha256 -days 1 \
  -subj /CN=go-patroni-etcd-tls-ca \
  -addext 'basicConstraints=critical,CA:TRUE' \
  -addext 'keyUsage=critical,keyCertSign,cRLSign,digitalSignature' \
  -keyout "$lab_dir/tls/ca.key" -out "$lab_dir/tls/ca.crt" >/dev/null 2>&1
openssl req -newkey rsa:2048 -nodes -sha256 -subj /CN=127.0.0.1 \
  -keyout "$lab_dir/tls/server.key" -out "$lab_dir/tls/server.csr" >/dev/null 2>&1
openssl x509 -req -sha256 -days 1 \
  -in "$lab_dir/tls/server.csr" -CA "$lab_dir/tls/ca.crt" -CAkey "$lab_dir/tls/ca.key" -CAcreateserial \
  -extfile <(printf 'basicConstraints=critical,CA:FALSE\nkeyUsage=critical,digitalSignature,keyEncipherment\nsubjectAltName=IP:127.0.0.1\nextendedKeyUsage=serverAuth\n') \
  -out "$lab_dir/tls/server.crt" >/dev/null 2>&1
openssl req -newkey rsa:2048 -nodes -sha256 -subj /CN=go-patroni-etcd-tls-client \
  -keyout "$lab_dir/tls/client.key" -out "$lab_dir/tls/client.csr" >/dev/null 2>&1
openssl x509 -req -sha256 -days 1 \
  -in "$lab_dir/tls/client.csr" -CA "$lab_dir/tls/ca.crt" -CAkey "$lab_dir/tls/ca.key" -CAcreateserial \
  -extfile <(printf 'basicConstraints=critical,CA:FALSE\nkeyUsage=critical,digitalSignature,keyEncipherment\nextendedKeyUsage=clientAuth\n') \
  -out "$lab_dir/tls/client.crt" >/dev/null 2>&1
chmod 600 "$lab_dir/tls/"*.key

openssl rand -base64 32 | tr -d '\n' >"$lab_dir/etcd-password"
chmod 600 "$lab_dir/etcd-password"
password=$(<"$lab_dir/etcd-password")

container="go-patroni-etcd-tls-${PPID}-${RANDOM}"
docker run --detach --rm \
  --name "$container" \
  --publish 127.0.0.1::2379 \
  --volume "$lab_dir/tls:/certs:ro" \
  "$image" /usr/local/bin/etcd \
  --name go-patroni-etcd-tls \
  --data-dir /tmp/go-patroni-etcd-tls \
  --listen-client-urls https://0.0.0.0:2379 \
  --advertise-client-urls https://127.0.0.1:2379 \
  --listen-peer-urls http://0.0.0.0:2380 \
  --cert-file /certs/server.crt \
  --key-file /certs/server.key \
  --trusted-ca-file /certs/ca.crt \
  --client-cert-auth=true >/dev/null

etcdctl=(docker exec -i "$container" /usr/local/bin/etcdctl
  --endpoints=https://127.0.0.1:2379
  --cacert=/certs/ca.crt --cert=/certs/client.crt --key=/certs/client.key)
healthy=0
for _ in $(seq 1 80); do
  if "${etcdctl[@]}" endpoint health >/dev/null 2>&1; then
    healthy=1
    break
  fi
  sleep 0.25
done
if ((healthy == 0)); then
  safe_logs
  printf 'isolated mTLS etcd did not become healthy\n' >&2
  exit 1
fi

printf '%s\n%s\n' "$password" "$password" |
  "${etcdctl[@]}" user add root --interactive=false >/dev/null
"${etcdctl[@]}" user grant-role root root >/dev/null
"${etcdctl[@]}" auth enable >/dev/null
unset password

published=$(docker port "$container" 2379/tcp | head -n 1)
if [[ $published != 127.0.0.1:* ]]; then
  printf 'unexpected etcd TLS loopback publication: %s\n' "$published" >&2
  exit 1
fi
namespace="go-patroni-test-tls-${PPID}-${RANDOM}"

"$go_command" test -c -tags=integration -o "$test_binary" ./test/integration
GO_PATRONI_TEST_ETCD_TLS_ISOLATED=1 \
GO_PATRONI_TEST_ETCD_TLS_ENDPOINT="https://$published" \
GO_PATRONI_TEST_ETCD_TLS_NAMESPACE="$namespace" \
GO_PATRONI_TEST_ETCD_TLS_CA="$lab_dir/tls/ca.crt" \
GO_PATRONI_TEST_ETCD_TLS_CLIENT_CERT="$lab_dir/tls/client.crt" \
GO_PATRONI_TEST_ETCD_TLS_CLIENT_KEY="$lab_dir/tls/client.key" \
GO_PATRONI_TEST_ETCD_TLS_PASSWORD_FILE="$lab_dir/etcd-password" \
  "$test_binary" -test.count=1 -test.run '^TestEtcd3TLSMutualAuthVerificationAndCredentials$'

version=$(docker exec "$container" /usr/local/bin/etcd --version | head -n 1)
digest=$(docker image inspect "$image" --format '{{index .RepoDigests 0}}')
printf 'etcd TLS matrix PASS: %s image=%s verify=default hostname=checked mtls=required auth=required negative=untrusted,hostname,client-cert,credentials\n' \
  "$version" "$digest"
