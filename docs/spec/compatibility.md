# Compatibility contract

## Supported and evidenced ranges

The declared SDK range is Patroni `>=3.0.0,<5.0.0`; it deliberately excludes
Patroni 1.x, 2.x, and the future 5.x contract. “Backward compatible” in this
repository therefore means compatible within the audited 3.x and 4.x range,
not indefinitely backward compatible with every Patroni release.

| Patroni line | REST rows | Versioned change | Current evidence level |
| --- | ---: | --- | --- |
| 3.0.x-3.2.x | 68 | Core REST, health, configuration, restart, failover, switchover, and Citus | source/contract tests plus isolated 3.0.4, 3.1.2, and 3.2.2 live PASS |
| 3.3.x | 69 | Adds `POST /mpp` | source/contract tests plus isolated 3.3.11 live PASS |
| 4.0.x | 75 | Adds quorum aliases/status and failsafe LSN response header | source/contract and CLI differential tests plus isolated 4.0.10 live PASS |
| 4.1.x | 75 | Adds readiness-lag mode, reinitialize-from-leader, and standby-cluster CLI semantics | pinned source/contract and CLI differential tests plus isolated 4.1.4 live PASS |

The direct REST route inventory is complete for these documented routes. The
high-level runtime is not a native implementation of every Patroni facility:
only etcd v3 is built in, while Patroni also supports other DCS backends. Those
backends require consumer-supplied `dcs` interfaces.

The current point-in-time live record was executed on 2026-07-17 against
official tags 3.0.4 (`a4d29eb99ea4`), 3.1.2 (`710afd59520b`), 3.2.2
(`c8e32775df20`), 3.3.11 (`3ab81653293c`), 4.0.10 (`099db547ad06`), and 4.1.4
(`d701f7b9c3d7`). All six isolated REST/TLS runs passed. The Oracle base came
from a registry mirror at the pinned PostgreSQL manifest digest
`sha256:57c72fd2a128e416c7fcc499958864df5301e940bca0a56f58fddf30ffc07777`;
changing the registry hostname did not change the manifest identity.

## Pinned evidence and current upstream

The committed compatibility artifacts are pinned to official Patroni 4.1.4,
commit `d701f7b9c3d7e8cb400092d30170ff507697bce9`:

- `compatibility/patroni-source.yaml` records source hashes;
- `compatibility/rest-api.yaml` records every method/path row and its tests;
- `compatibility/patronictl.yaml` records the 19 upstream commands;
- `compatibility/dcs.yaml` records the Patroni DCS keys used by the SDK;
- `compatibility/deviations.yaml` is the release gate for intentional drift.

Comparing 4.1.3 to 4.1.4 shows no REST route or `patronictl` command additions
or removals, so the 75-row and 19-command inventories remain structurally
complete. The regenerated 4.1.4 evidence includes the Prometheus fixes for
`patroni_postgres_server_version` and `patroni_postgres_timeline`, updated DCS
source hashes, and the CLI role-error change. A clean git checkout and official
archive now generate byte-identical source manifests.

## Evidence levels

Compatibility claims MUST state which evidence exists:

1. **Inventory evidence**: independently extracted upstream methods, commands,
   DCS paths, source locations, and hashes.
2. **Contract evidence**: every catalog entry is callable and response/error,
   safety, and version semantics have tests.
3. **Differential evidence**: selected behavior is compared against upstream
   Python `patronictl`.
4. **Live evidence**: isolated Patroni, etcd, and PostgreSQL processes exercise
   real protocols for named versions.

A broad declared range MUST NOT be described as fully live-tested merely
because targets are configured. The default script includes 3.0.4, 3.1.2,
3.2.2, 3.3.11, 4.0.10, and 4.1.4. Each release must rerun the real containers;
the point-in-time record above does not make future runs optional.

## Compatibility behavior

- Optional fields added by upstream MUST decode without breaking older
  payloads; raw REST response bytes remain available.
- `/patroni` identity `name` is optional before Patroni 3.2.0; `version` and
  `scope` remain the historical identity fields on 3.0.x and 3.1.x.
- Removed or renamed fields MUST be investigated per version rather than
  hidden behind an unbounded `map[string]any` replacement.
- Versioned endpoints and semantics MUST carry a `since` boundary.
- Direct callers select compatible endpoints explicitly; high-level writes
  fail closed before sending an unsupported feature.
- Mixed-version clusters are supported only when every relevant member is
  inside the configured subrange and supports the requested feature.
- Embedded CLIs retain the canonical machine API. `VersionInfo.application`
  is optional host metadata; the top-level version fields continue to describe
  go-patroni and its audited range.
- Intentional wire, machine-output, command, or security-default divergence
  MUST be recorded in `compatibility/deviations.yaml` with rationale and tests.

Version and feature gates apply SemVer pre-release precedence and fail closed
at an upper boundary: `4.1.0-rc1` does not receive 4.1.0 features and
`5.0.0-rc1` is outside the audited major range. `AuditedPatroniRange` returns a
copy of the canonical policy. The deprecated `SupportedPatroniRange` variable
is retained only as a source-compatible snapshot; mutating it does not change
SDK behavior.

## Known compatibility gaps

The following implementation facts are not permission to weaken the intended
contract:

- `HealthQuery.ReplicationState` remains as a deprecated source-compatible
  field but is ignored and not sent.
- Direct `postgres.ConnectionOptions` keeps its secure `verify-full` default;
  the `patronictl query` adapter explicitly uses `TLSFromSource` to preserve
  libpq/pgx SSL source and plaintext-fallback semantics.
- The built-in high-level runtime supports etcd v3 only.

## Upgrade procedure

To adopt a new Patroni release:

1. Read upstream release notes and compare `patroni/api.py`, `patroni/ctl.py`,
   relevant DCS modules, version metadata, and REST documentation.
2. Update the extractor's exact tag, commit, date, hashes, and supported feature
   boundaries.
3. Regenerate every `compatibility/*.yaml` artifact from clean official source.
4. Review added, removed, and semantically changed routes, fields, commands,
   defaults, errors, DCS paths, and metrics; a matching row count is not enough.
5. Update typed DTOs, catalogs, control gates, deviations, Specs, and user docs.
6. Run unit, race, schema, Oracle, differential, and live matrices, including
   the oldest promised line and the newest stable line.
7. Record the exact evidence and any infrastructure blocker. Never report a
   matrix as passed when it only compiled or could not start.
