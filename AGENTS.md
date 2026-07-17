# AGENTS.md

This repository is governed by the contract specifications in `docs/spec/`.
Before changing code, read `docs/spec/README.md` and the specification for the
layer being changed.

## Fact and evidence rules

- Official Patroni source defines upstream behavior.
- `compatibility/*.yaml` is the committed, source-pinned inventory.
- `schema/machine/v1alpha1/` is the machine-output contract.
- `docs/spec/` defines project invariants and the meaning of compatibility.
- Exported APIs and tests implement those contracts.
- A disagreement between these sources is contract drift. Resolve it with
  evidence; do not silently update only a version string or row count.

The committed Oracle baseline is Patroni 4.1.4 at commit
`d701f7b9c3d7e8cb400092d30170ff507697bce9`. Its route and command inventories
remain 75 and 19. Source manifests generated from the official archive and a
clean checkout MUST be byte-identical.

## Architecture boundaries

- Keep the module-root package usable as a direct REST client with no DCS,
  Patroni YAML, PostgreSQL, runtime, or CLI dependency.
- Keep normalized identities/domain objects in `model`, wire DTOs at the wire
  boundary, revisioned state in `dcs`, and stable envelopes in machine schemas.
- Put reusable operation policy in `control`, assembly in `runtime`, and only
  parsing/prompting/rendering in `internal/cli`.
- Depend on the narrowest DCS capability interface. Do not add a general public
  DCS put/delete API.
- Preserve the complete `model.Target`; scope alone is not globally unique.

## Safety invariants

- Every I/O path must be caller-cancelable and bounded.
- Never automatically retry a REST write or follow a write redirect.
- Preserve `NOT_SENT`, `MAYBE_SENT`, and response-received delivery evidence.
- High-level mutations use prepare/execute, re-check preconditions, and return
  `UNKNOWN` when an accepted or maybe-sent write cannot be verified.
- Use exact normalized DCS prefixes and compare-and-swap revisions.
- Never emit credentials, request bodies, DCS raw values, connection strings,
  or key passwords in errors, logs, plans, inspection, or machine output.
- `--force` may skip a prompt, never validation, version gates, CAS, or safety
  classification.

## Upstream contract changes

When a Patroni route, field, command, default, DCS key, version boundary, or
semantic changes, update in one change:

1. the pinned extractor metadata and all affected `compatibility/*.yaml` files;
2. direct DTOs/methods/catalogs and high-level gates or operations;
3. unit, Oracle, differential, and relevant live tests;
4. `docs/spec/`, the Chinese SDK guide, and public documentation;
5. `compatibility/deviations.yaml` for every intentional difference.

“All APIs” means all documented method/path contracts in the supported source
inventory. Do not conflate that with native support for every Patroni DCS
backend or every implicit method handled by Patroni's HTTP base class.

## Required verification

Run at minimum:

```bash
go test -mod=readonly ./...
go vet ./...
go test -run '^$' -tags=integration ./test/integration
go run ./tools/machineschema -check
```

Run `go test -race -mod=readonly ./...` for concurrency, transport, cache,
watcher, or client changes. Run the relevant scripts in `scripts/` for live
protocol changes. Report a live matrix as passed only when real infrastructure
started and the tests executed; compilation or an infrastructure failure is a
different result.

Release and upstream-compatibility changes also run `golangci-lint run ./...`,
`govulncheck ./...` with a currently patched Go toolchain, a byte-for-byte
`compatgen -check` against pinned source, and the relevant Oracle tests. A
failure caused by an obsolete local Go standard library is a toolchain failure,
not permission to waive the release gate.

Do not hand-edit generated compatibility or machine-schema output without also
updating and running its generator. Preserve unrelated user changes in a dirty
worktree.
