# Compatibility contract

## Supported versions

The audited range is Patroni `>=3.0.0,<5.0.0`. Patroni 4.x is the primary
target; Patroni 3.x support is retained where source inventories and tests show
the same contract.

| Patroni version | REST method/path rows | Notable additions                                                 |
|-----------------|----------------------:|-------------------------------------------------------------------|
| 3.0.x-3.2.x     |                    68 | Core health, status, config, restart, failover, switchover, Citus |
| 3.3.x           |                    69 | Generic `POST /mpp` alias                                         |
| 4.0.x           |                    75 | Quorum health aliases/status and failsafe LSN response header     |
| 4.1.x           |                    75 | Readiness lag mode, reinitialize-from-leader, standby-cluster CLI |

Version-gated operations are checked by the high-level control service before
the SDK sends a write. Direct REST callers can use `EndpointCatalogFor`,
`FeatureCatalog`, and `SupportsFeature` when selecting an operation.
`ServiceOptions.SupportedPatroniRange` and
`runtime.EnvironmentOptions.SupportedPatroniRange` allow an embedding product
to narrow, but never widen, this audited range.

## Evidence

The release contract is pinned to official Patroni 4.1.4 source commit
`d701f7b9c3d7e8cb400092d30170ff507697bce9`:

- [`compatibility/patroni-source.yaml`](../compatibility/patroni-source.yaml)
  records the tag, commit, source-file hashes, and extraction procedure.
- [`compatibility/rest-api.yaml`](../compatibility/rest-api.yaml) lists all 75
  REST method/path rows, request and response shapes, risk, introduction
  version, source location, and tests.
- [`compatibility/patronictl.yaml`](../compatibility/patronictl.yaml) lists all
  19 upstream commands, parameters, semantics, data paths, prompts, output
  formats, and test evidence.
- [`compatibility/dcs.yaml`](../compatibility/dcs.yaml) records the Patroni DCS
  paths and operations consumed by the high-level SDK.
- [`compatibility/deviations.yaml`](../compatibility/deviations.yaml) is the
  release gate for any intentional divergence. It is empty for this release.

Unit contract tests make every catalog row callable and verify that status,
headers, and raw response bytes survive failures. Compatibility tests compare
the committed manifests with an independently extracted upstream inventory.
The integration-tag suite compiles without infrastructure and can run against
isolated real Patroni, etcd3, PostgreSQL, TLS, and mTLS fixtures.

The 2026-07-17 live record passed isolated Patroni 3.0.4, 3.1.2, 3.2.2,
3.3.11, 4.0.10, and 4.1.4; the 4.0.10/4.1.4 `patronictl` differential, etcd
3.6.13 mTLS/auth, PostgreSQL 14/16/18 TLS and plaintext paths, and the combined
TLS gate also passed. This is point-in-time evidence and does not replace
rerunning the matrices for a release.

## Compatibility policy

- Patch releases may add newly observed optional response fields and improve
  error classification without removing public contracts.
- A new Patroni minor feature is added to the catalogs with its `since`
  version and source/test evidence.
- A Patroni major outside the audited range is rejected by high-level
  operations. Best-effort reads require an explicit opt-in; writes do not.
- Any human, wire, or machine-output divergence from pinned `patronictl` must
  be declared and tested before release.

## Running the matrices

Compile every opt-in integration test without starting infrastructure:

```bash
go test -run '^$' -tags=integration ./test/integration
```

Run the real Patroni REST and control matrix from a Patroni source checkout
containing the audited tags. The default matrix covers the latest patch of
every supported 3.x minor plus the 4.0 and 4.1 feature boundaries:

```bash
PATRONI_SOURCE=/path/to/patroni ./scripts/test-patroni-integration.sh
```

Run the CLI differential matrix:

```bash
PATRONI_SOURCE=/path/to/patroni ./scripts/test-cli-compat.sh
```

Other isolated fixtures are exposed by `scripts/test-etcd-tls-integration.sh`,
`scripts/test-postgres-integration.sh`, and `scripts/test-tls-integration.sh`.
