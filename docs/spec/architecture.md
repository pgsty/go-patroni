# Architecture contract

## Layer boundaries

The repository is split into deliberately different abstraction levels:

| Layer | Responsibility | Must not assume |
| --- | --- | --- |
| module root (`patroni`) | Exact Patroni REST wire contracts, authentication, TLS transport, error delivery state, endpoint and feature catalogs | DCS access, Patroni YAML, cluster discovery, or PostgreSQL access |
| `model` | Stable cluster/member identity and normalized domain objects | A particular wire protocol or CLI renderer |
| `config` | Tolerant Patroni YAML parsing, context overlays, source precedence, validation, and secret-safe inspection | A live DCS or cluster |
| `dcs` | Capability-scoped Patroni state and mutation interfaces | A particular DCS implementation |
| `dcs/etcd3` | Native etcd v3 implementation of the `dcs` contracts | CLI policy or output formatting |
| `postgres` | Bounded one-shot and streaming SQL execution, optionally verified against a member role | DCS mutation or CLI prompting |
| `control` | Adapter-neutral reads, prepared writes, preconditions, evidence, verification, and outcome classification | Cobra, terminal interaction, or human formatting |
| `runtime` | Resolve configuration and assemble DCS, REST, PostgreSQL, and control clients | Command-specific prompting or rendering |
| `cli` | Public, thin composition facade for embedding the command suite and registering product-owned subcommands | Product-specific behavior or control algorithms |
| `internal/cli`, `cmd/patronictl` | Implement flags, prompts, human/machine rendering, and exit mapping behind the public facade | Business logic that belongs in `control` |

Dependencies MUST flow toward narrower contracts. In particular, reusable
control logic MUST NOT depend on Cobra or terminal state, and the REST client
MUST remain usable without a DCS implementation.

The public `cli` facade MAY depend on Cobra because it is an opt-in adapter.
Core packages (`model`, `config`, `dcs`, `postgres`, `control`, and `runtime`)
MUST NOT depend on `cli` or Cobra. An embedding application MAY customize its
display identity, runtime defaults, I/O, request-ID prefix, and top-level
extensions. Extensions receive normalized root flag state and stable error
mapping, but MUST implement reusable Patroni behavior in `control` first.

## Identity

`model.Target` is the canonical identity of a resource:

```text
(context, namespace, scope, optional Citus group, optional member)
```

Scope alone is not globally unique. Every cross-cluster cache key, operation
result, plan, discovery record, and machine identifier MUST preserve the full
target. `Target.Normalize` supplies the stable defaults `default` and
`/service`; `ClusterID` and `MemberID` are reversible, escaped identifiers.

Code MUST distinguish an absent Citus group from group zero. A member target
MUST include a scope. DCS deletion MUST resolve an exact normalized cluster
root and MUST never delete a textual sibling prefix.

## Three data models

The project intentionally maintains three different representations:

1. REST wire DTOs in the module-root package match Patroni JSON/text payloads.
2. `dcs` types retain revisions, leases, raw entries, and decode issues needed
   for concurrency and forensic evidence.
3. `model` and `control` types provide normalized, adapter-neutral domain data.

These representations MUST NOT be collapsed merely because fields currently
look similar. Unknown REST fields survive in `Response.Raw`; unknown or
partially invalid DCS data is represented by `DecodeIssue`; stable machine
output is governed separately by `schema/machine/v1alpha1`.

Embedded command trees MUST retain the canonical
`patroni.pgsty.com/v1alpha1` machine envelope. The `VersionInfo.application`
object MAY identify the host binary; it is additive metadata and MUST NOT
replace the SDK version, supported range, or machine-schema fields.

## Configuration and assembly

`config.Parse` MUST tolerate normal Patroni keys it does not own and retain the
raw YAML node. Effective configuration is resolved in this order:

```text
SDK defaults -> Patroni file -> named context -> environment -> explicit flags
```

The product-neutral extension is `go_patroni`. The legacy `boar` extension and
`BOAR_CONTEXT` MAY be read for migration but MUST NOT be emitted as the new
canonical form. Inspection output MUST record provenance without revealing
credentials.

`runtime.Environment` owns configuration and factory state. `Environment.Open`
creates a per-operation runtime with an exact target and the clients required
by that operation. Callers MUST close the returned runtime.

## DCS contract

The public DCS boundary is capability-based:

- `SnapshotReader` returns fresh, revisioned cluster state;
- `Discoverer` performs a bounded namespace scan;
- `Watcher` resumes strictly after a revision and resnapshots after compaction;
- `ConfigCAS` and `FailoverCAS` expose only Patroni-specific compare-and-swap
  mutations;
- `ClusterRemover` deletes one exact cluster root.

The SDK MUST NOT expose a general-purpose public DCS `Put`. Control code MUST
not retain a snapshot and later treat it as a fresh write precondition. The
only built-in runtime adapter is currently etcd v3; consumers of other Patroni
DCS backends MAY implement these interfaces and construct `control.Service`
directly.

## Operation lifecycle

Read operations return a typed `control.Result[T]` with target, data path, and
evidence. Mutating operations use two phases:

```text
Prepare -> review/confirm Plan -> Execute -> verify -> classify outcome
```

Plans are bound to the service instance, request, target, observed state, and
preconditions. Execution MUST re-read or re-check state as required; a plan is
not permission to ignore drift. The full lifecycle and classifications are in
[control.md](control.md).

## Extension rules

A new adapter SHOULD implement the smallest `dcs` or control-facing interface
it needs. A new CLI command SHOULD first expose reusable behavior in `control`.
A new REST endpoint belongs in the direct client and catalogs even when no
high-level command uses it. A response-model addition MUST preserve decoding of
older payloads and access to unmodeled raw fields.
