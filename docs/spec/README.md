# go-patroni contract specifications

This directory is the durable contract for `go-patroni`. It records the facts
that must survive implementation refactors, CLI rendering changes, and Patroni
upgrades. The word **MUST** denotes a release requirement, **SHOULD** denotes a
strong default that needs a documented reason to violate, and **MAY** denotes
an optional behavior.

## Contract map

| Specification | Contract owned by the file |
| --- | --- |
| [architecture.md](architecture.md) | Package boundaries, dependency direction, identities, and data-model separation |
| [rest-api.md](rest-api.md) | Direct Patroni HTTP surface, wire behavior, response preservation, and version selection |
| [control.md](control.md) | Adapter-neutral orchestration, plans, evidence, outcomes, DCS, and PostgreSQL behavior |
| [compatibility.md](compatibility.md) | Supported versions, upstream evidence, coverage meaning, and upgrade procedure |
| [security.md](security.md) | Secret handling, transport trust, bounded I/O, and destructive-operation rules |
| [verification.md](verification.md) | Required generation, test, static-analysis, and live-matrix gates |

The practical Chinese user and Agent manual is in
[../user-guide.zh-CN.md](../user-guide.zh-CN.md). The complete Chinese SDK
guide and audit findings are in
[../go-patroni-sdk.zh-CN.md](../go-patroni-sdk.zh-CN.md).

## Sources of truth

No single file is allowed to establish compatibility by assertion alone. The
following sources form one contract system:

1. Official Patroni source defines upstream behavior.
2. `compatibility/*.yaml` records the pinned upstream source inventory and the
   evidence attached to each REST, CLI, and DCS contract.
3. `schema/machine/v1alpha1/` defines the stable machine-output boundary.
4. This directory defines the project-level invariants and the intended public
   behavior.
5. Exported Go APIs and tests implement and verify those contracts.

If two sources disagree, the repository is in contract drift. A change MUST
resolve the disagreement and add evidence; it MUST NOT silently choose the
most convenient source. README files and CLI help are explanatory surfaces,
not substitutes for the contract system above.

## Scope

The SDK has two API levels:

- the module-root package is a typed, direct Patroni REST client;
- `control.Service`, normally assembled by `runtime`, implements higher-level
  `patronictl`-style operations across DCS, Patroni REST, and PostgreSQL.

“Complete Patroni REST coverage” means every documented method/path contract
extracted from the supported Patroni `api.py` source inventory. It does not
mean every response field can never grow, every HTTP method accepted by the
base server is a public endpoint, or every Patroni DCS backend has a native Go
adapter. Those distinctions are normative in the linked specifications.

## Change discipline

Any change to an upstream-visible method, path, parameter, response, command,
DCS key, version boundary, or machine envelope MUST update all affected parts
of the contract system in the same change. New optional upstream response
fields SHOULD be accepted without breaking older servers, and raw REST bytes
MUST remain available as the forward-compatibility escape hatch.

Known implementation gaps belong in the audit section of the Chinese guide or
in `compatibility/deviations.yaml`; they must not be hidden by weakening these
specifications.
