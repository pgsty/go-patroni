# High-level control contract

## Purpose

`control.Service` provides reusable Patroni operations without depending on a
CLI framework. It joins three authoritative data paths:

- DCS for discovery, cluster truth, revisions, and compare-and-swap writes;
- Patroni REST for member status and administrative actions;
- PostgreSQL for SQL execution and optional same-connection role verification.

`runtime` is the standard assembler. Consumers using a non-etcd3 DCS can inject
their own capability implementations into `NewService`.

## Results and evidence

Every operation returns `Result[T]` containing an operation ID, full
`model.Target`, data path, typed data, evidence, and optional typed error. A
valid result MUST include at least one evidence record with source and
observation time.

The only outcomes are:

| Outcome | Contract |
| --- | --- |
| `SUCCEEDED` | Authoritative evidence supports success and no operation error is present |
| `FAILED` | Failure is proven and the error category is not `UNKNOWN` |
| `UNKNOWN` | A write may have occurred or was accepted, but verification cannot prove its final effect |

The service MUST prefer `UNKNOWN` to a false failure. Verification is
authoritative: verified success or failure overrides the uncertain send state.
Without verification, `MAYBE_SENT` and `ACCEPTED` writes classify as `UNKNOWN`.

Evidence sources are local validation, DCS, Patroni REST, PostgreSQL, and the
control layer. Evidence summaries MUST be secret-safe and SHOULD include the
revision or path needed to explain the decision.

## Prepared writes

Mutations exposed to applications follow a prepare/execute pair. Preparation
validates the request, reads current state, resolves exact members, applies the
version gate, and returns a `Plan` with risk, retry safety, preconditions, and a
service-bound token. Execution validates that token and re-checks relevant
preconditions before sending.

A plan:

- MUST be executed by the same service instance that prepared it;
- MUST be bound to the target and material request fields;
- MUST expire or fail when required state has drifted;
- MUST NOT turn an unsafe write into an automatically retryable write;
- MUST carry enough detail for a CLI or embedding product to confirm the exact
  action without reimplementing business logic.

## Operation inventory

Read operations:

| Method | Primary path |
| --- | --- |
| `Discover`, `ListAll`, `TopologyAll` | bounded namespace discovery and DCS snapshots |
| `List`, `Topology`, `TopologyGroups` | DCS, optionally augmented by member status |
| `DSN` | DCS member selection; credentials are never returned |
| `ShowConfig`, `History` | DCS |
| `Query` | DCS target selection plus PostgreSQL |
| `Version` | local product version plus Patroni member versions |
| `InspectConfiguration` | local resolved configuration and provenance |

Prepared writes:

| Prepare/execute family | Principal path |
| --- | --- |
| `Reload` | REST to selected members |
| `Restart` | REST to selected members |
| `Reinitialize` | REST to one replica/standby |
| `Failover`, `Switchover` | REST, with command-specific DCS fallback semantics |
| `Flush` | REST cancellation of scheduled restart/switchover |
| `Pause`, `Resume` | dynamic configuration via REST/DCS semantics |
| `EditConfig` | DCS compare-and-swap |
| `Remove` | exact DCS cluster deletion |
| `DemoteCluster`, `PromoteCluster` | Patroni 4.1 standby-cluster orchestration |

The CLI may add confirmation, interactive selection, and rendering, but MUST
not alter these operation semantics.

The high-level restart operation MUST omit optional wire fields that the caller
did not set. In particular, an unset timeout MUST not become `"timeout":""`:
Patroni rejects that payload, whereas an omitted timeout requests its normal
default behavior.

## Version gates

The audited service range is `>=3.0.0,<5.0.0`. An embedding product may narrow
the range per service or environment instance but MUST NOT widen it. High-level
writes MUST fail closed when a member has an unknown or unsupported Patroni
version or when a required feature is absent. Mixed-version clusters MUST be
checked across every member relevant to the action.

Unsupported reads require an explicit best-effort opt-in. Best-effort read
permission MUST NOT leak into writes.

## DCS concurrency

DCS snapshots and entries carry revisions. Config and failover mutations MUST
use compare-and-swap against the intended revision when a precondition exists.
Discovery MUST be bounded to a normalized namespace and deterministic. A watch
must resume after the requested revision; after etcd compaction it MUST emit a
full resnapshot rather than silently losing history.

Configuration editing MUST preserve Patroni's nested dynamic configuration
semantics, bind the preview to the desired value, and verify the resulting DCS
state. Cluster removal MUST target one exact cluster root.

## PostgreSQL query behavior

The `postgres` package supports collected and streaming multi-result queries,
row/byte limits, cancellation, and optional role checks. A checked query MUST
verify the member role on the same physical connection used for the caller SQL
to avoid a time-of-check/time-of-use connection race.

`control.Query` mirrors `patronictl` presentation semantics: a SQL/query error
may be represented inside `QueryData.Error` while the orchestration result is
`SUCCEEDED`, because the control operation successfully reached PostgreSQL and
obtained the server error for rendering. Consumers MUST inspect both the
operation outcome and `QueryData.Error`.

The reusable `postgres.ConnectionOptions` default is TLS `verify-full`.
`patronictl query` explicitly selects `TLSFromSource`, preserving SSL mode,
fallbacks, environment, service-file, and connection-string behavior parsed by
pgx/libpq-compatible sources. Embedding applications choose either policy
explicitly.

## CLI boundary

The Go CLI implements the 19 pinned upstream command names and also provides
`discover`, `inspect-config`, and multi-cluster `--all` workflows. Human output
may evolve within declared compatibility rules. Machine JSON/YAML output MUST
remain inside the versioned `patroni.pgsty.com/v1alpha1` envelopes and schemas.

Exit codes are derived from typed control error categories. Prompts and
`--force` behavior belong to the CLI adapter; bypassing a prompt MUST NOT bypass
control preconditions or version checks.
