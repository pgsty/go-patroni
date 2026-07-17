# Security and safety contract

## Bounded operations

All network, file, DCS, and database operations MUST take a caller-owned
`context.Context` or operate under a documented bound. Nil contexts are invalid.
Default timeouts are safety nets, not permission for callers to ignore
cancellation. Response bodies, SQL files, rows, and aggregate query bytes MUST
have finite configurable limits.

Mutating REST requests MUST never be automatically retried. After an ambiguous
send, control MUST verify from an authoritative source or return `UNKNOWN`.
DCS mutations MUST use exact keys and compare-and-swap where a revision is part
of the plan.

## Credential handling

Credentials MUST NOT be accepted in Patroni endpoint URLs. Passwords, private
key passphrases, PostgreSQL connection strings, DCS raw values, and secret
configuration fields MUST be redacted from:

- `String` and `GoString` representations;
- formatted SDK errors and default logs;
- configuration inspection;
- CLI machine output;
- plans and evidence summaries.

An error MAY implement `Unwrap` for explicit diagnostics. Callers that log the
underlying error assume responsibility for its contents. Presence flags such as
`password:true` are acceptable; plaintext or reversible encodings are not.

## REST TLS

Verification is enabled by default and TLS 1.2 is the minimum. Client
certificate and key are an inseparable pair. Encrypted keys MUST be decrypted
only for transport construction, and temporary key/passphrase buffers SHOULD
be cleared as soon as practical. Insecure verification requires an explicit
option and MUST remain visible in secret-safe inspection.

An explicit CA file replaces the system root pool and is therefore an exclusive
trust bundle. `TLSOptions.IncludeSystemCAs` is the explicit opt-in for combining
that bundle with host roots. Both choices participate in the transport-cache
fingerprint.

TLS transport caching fingerprints certificate material so rotation does not
reuse stale credentials. The cache is a bounded LRU. `CloseIdleConnections`
keeps fingerprints reusable; `Purge` closes idle connections and clears them.

## PostgreSQL TLS

The `postgres` package exposes four explicit modes: `verify-full`, `insecure`,
`disable`, and `source`. `source` preserves SSL behavior parsed from libpq/pgx
sources; other modes override it consistently for primary and fallback
connections. The current zero value resolves to `verify-full`.

Applications MUST choose the intended mode rather than assuming upstream
`patronictl` defaults. The CLI uses `source` for compatibility; the reusable Go
API retains `verify-full` as its safe zero-value behavior. TLS-enabled and
plaintext-only PostgreSQL fixtures SHOULD both remain in the live matrix.

## Destructive and availability-sensitive actions

The following actions require an explicit prepared plan and policy decision by
the embedding application or CLI:

- cluster removal;
- failover and switchover;
- restart and reinitialize;
- standby-cluster demotion/promotion;
- dynamic configuration changes that affect availability;
- the test-platform `POST /sigterm` endpoint.

CLI `--force` may suppress a human prompt, but MUST NOT suppress target
validation, version gates, preconditions, CAS checks, or ambiguous-write
classification. Production callers SHOULD require explicit confirmation for
destructive plans and SHOULD display full normalized target identity.

## Logging and observability

Logs SHOULD identify operation ID, method/path (not credential-bearing base
URL), target ID, elapsed time, status, delivery state, and evidence source.
They MUST not include request bodies by default because configuration and
authentication material can be present. Metrics text returned by Patroni is
untrusted bounded input and must not be treated as SDK-generated labels.
