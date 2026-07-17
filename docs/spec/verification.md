# Verification contract

## Always-on gates

Every change MUST keep the following commands passing from the repository
root:

```bash
go test -mod=readonly ./...
go vet ./...
go test -run '^$' -tags=integration ./test/integration
go run ./tools/machineschema -check
```

Changes involving concurrency, transports, caches, watchers, or shared clients
MUST also run:

```bash
go test -race -mod=readonly ./...
```

The integration compile gate proves that optional test code still builds; it
does not prove protocol compatibility with a running Patroni cluster.

## Generated contracts

Machine schemas and compatibility manifests are generated artifacts. A change
MUST update the generator and generated output together. Generated output MUST
be deterministic. A check against clean pinned Patroni source MUST compare the
full content, not only endpoint or command counts.

The compatibility Oracle is a development/test dependency only. Patroni Python
code MUST never become a runtime dependency of this Go module.

## Upstream and live matrices

Changes to REST, control, DCS, PostgreSQL, CLI, or version logic MUST run the
relevant isolated matrix when infrastructure is available:

```bash
PATRONI_SOURCE=/path/to/patroni ./scripts/test-patroni-integration.sh
PATRONI_SOURCE=/path/to/patroni ./scripts/test-cli-compat.sh
./scripts/test-etcd-tls-integration.sh
./scripts/test-postgres-integration.sh
./scripts/test-tls-integration.sh
```

When Docker Hub metadata is unavailable, the Patroni/CLI harness MAY set
`GO_PATRONI_ORACLE_BASE_IMAGE` to the same immutable digest on a reachable
registry mirror. `GO_PATRONI_ORACLE_BUILD_PROXY` MAY supply an HTTP proxy for
package installation inside the build. Reports MUST record the effective base
reference and verify that its manifest digest equals the pinned default. The
maintained Patroni and CLI harnesses MUST reject an override whose `@sha256`
suffix differs from that pinned manifest.

The live Patroni matrix MUST include the oldest release line still claimed,
each feature boundary affected by the change, and the newest pinned stable
release. Exact accepted versions in the Go integration test and shell script
MUST be updated together. The default matrix is 3.0.4, 3.1.2, 3.2.2, 3.3.11,
4.0.10, and 4.1.4.

The PostgreSQL matrix MUST exercise both TLS and plaintext-only instances for
every configured image. The direct SDK MUST refuse an implicit downgrade from
its `verify-full` default. `TLSFromSource` with `PGSSLMODE=prefer` MUST connect
to the plaintext-only fixture and prove `pg_stat_ssl.ssl = false`.

Test reports MUST distinguish these states:

- passed against real infrastructure;
- compiled but not executed;
- skipped because prerequisites were absent;
- blocked by infrastructure before the SDK was exercised;
- failed because the SDK violated the contract.

## Static and vulnerability analysis

Release work SHOULD run the repository's configured linter and
`govulncheck ./...`. Findings in executable production paths MUST be resolved
or explicitly tracked. The vulnerability scan result is tied to the exact Go
toolchain used; CI and release builds MUST use a currently patched toolchain,
not merely the minimum version declared in `go.mod`.

Unchecked close/flush errors deserve context-specific treatment: response-body
cleanup may be best-effort, while database/file/CLI runtime closure can carry
material failures. Suppressions MUST explain why an error cannot affect the
operation result.

## Coverage expectations

Aggregate coverage is informative, not sufficient. Safety-critical branches
MUST have targeted tests for:

- write delivery state and no-retry behavior;
- plan binding, stale preconditions, and `UNKNOWN` classification;
- exact-prefix DCS deletion and CAS conflicts;
- watcher compaction/resnapshot behavior;
- TLS verification, mTLS, encrypted keys, and certificate rotation;
- PostgreSQL TLS and plaintext modes, same-connection role checks, limits, and
  cancellation;
- mixed and unsupported Patroni versions;
- secret redaction in errors, logs, inspection, and machine output;
- runtime assembly and cleanup failure paths.

Low aggregate coverage in an adapter or assembler is a review signal even when
its interfaces are well tested with fakes.
