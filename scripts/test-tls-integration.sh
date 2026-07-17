#!/usr/bin/env bash
set -euo pipefail

repo_root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
patroni_tag=${GO_PATRONI_TLS_PATRONI_TAG:-v4.1.3}
postgres_image=${GO_PATRONI_TLS_POSTGRES_IMAGE:-postgres:18-alpine}
etcd_image=${GO_PATRONI_TLS_ETCD_IMAGE:-quay.io/coreos/etcd:v3.6.13}

cd "$repo_root"
GO_PATRONI_ETCD_TLS_IMAGE="$etcd_image" ./scripts/test-etcd-tls-integration.sh
GO_PATRONI_PATRONI_TAG="$patroni_tag" ./scripts/test-patroni-integration.sh
GO_PATRONI_POSTGRES_IMAGE="$postgres_image" ./scripts/test-postgres-integration.sh

printf 'combined live TLS matrix PASS: etcd3=verified-mTLS-auth Patroni=verified-mTLS-basic-auth PostgreSQL=verify-full-SCRAM credential_values_in_argv=false\n'
