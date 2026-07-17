# Patroni REST API contract

## Inventory boundary

The direct client covers the 75 documented method/path rows in the pinned
Patroni 4.1 source inventory. The count is structural:

```text
18 health aliases * (GET + HEAD + OPTIONS) = 54
8 additional GET endpoints                  =  8
2 configuration writes                      =  2
9 POST and 2 DELETE operations               = 11
                                                --
                                                75
```

Patroni 3.0 through 3.2 expose 68 rows, 3.3 adds `POST /mpp` for 69, and
Patroni 4.0 adds two quorum aliases with three methods each for 75.

## Health and role aliases

The complete alias set is:

```text
/
/primary
/master
/read-write
/leader
/standby-leader
/standby_leader
/replica
/read-only
/quorum
/read-only-quorum
/sync
/synchronous
/read-only-sync
/read-only-synchronous
/async
/asynchronous
/health
```

Every alias supports `GET`, `HEAD`, and `OPTIONS`. `/quorum` and
`/read-only-quorum` are available from Patroni 4.0.0; all other aliases are in
the Patroni 3.0 baseline. Legacy spellings such as `/master` and
`/standby_leader` remain first-class wire contracts even though normalized
domain terminology prefers `primary` and `standby-leader`.

`GET` returns the Patroni status body when the health predicate succeeds;
`HEAD` returns status without a body; `OPTIONS` reports availability. Health
query filtering consists of maximum lag and `tag_<name>=<value>` predicates.
The deprecated `HealthQuery.ReplicationState` field is retained for Go source
compatibility but is deliberately not serialized: Patroni does not implement
it as a health-query filter. Callers inspect `Status.ReplicationState` instead.

## Non-health endpoints

| Method | Path | Direct method | Risk | Since |
| --- | --- | --- | --- | --- |
| GET | `/liveness` | `GetLiveness` | read | 3.0.0 |
| GET | `/readiness` | `GetReadiness` | read | 3.0.0 |
| GET | `/patroni` | `GetPatroni` | read | 3.0.0 |
| GET | `/cluster` | `GetCluster` | read | 3.0.0 |
| GET | `/history` | `GetHistory` | read | 3.0.0 |
| GET | `/config` | `GetConfig` | read | 3.0.0 |
| GET | `/metrics` | `GetMetrics` | read | 3.0.0 |
| GET | `/failsafe` | `GetFailsafe` | peer-internal-read | 3.0.0 |
| PATCH | `/config` | `PatchConfig` | admin-write | 3.0.0 |
| PUT | `/config` | `PutConfig` | admin-write | 3.0.0 |
| POST | `/reload` | `PostReload` | admin-write | 3.0.0 |
| POST | `/failsafe` | `PostFailsafe` | peer-internal | 3.0.0 |
| POST | `/sigterm` | `PostSigterm` | test-platform-dangerous | 3.0.0 |
| POST | `/restart` | `PostRestart` | admin-write | 3.0.0 |
| DELETE | `/restart` | `DeleteRestart` | admin-write | 3.0.0 |
| DELETE | `/switchover` | `DeleteSwitchover` | admin-write | 3.0.0 |
| POST | `/reinitialize` | `PostReinitialize` | admin-write | 3.0.0 |
| POST | `/failover` | `PostFailover` | availability-write | 3.0.0 |
| POST | `/switchover` | `PostSwitchover` | availability-write | 3.0.0 |
| POST | `/citus` | `PostCitus` | peer-internal | 3.0.0 |
| POST | `/mpp` | `PostMPP` | peer-internal | 3.3.0 |

`PostSigterm`, failsafe peer calls, and MPP/Citus event calls are complete wire
contracts but are not ordinary operator workflows. Their risk classification
MUST be preserved in catalogs and higher-level policy.

## Response contract

Every direct method returns `Response[T]`:

```go
type Response[T any] struct {
    StatusCode int
    Header     http.Header
    Raw        []byte
    Data       T
}
```

Status, cloned headers, and the bounded raw body MUST survive successful
decoding and decode errors whenever a response was received. Unknown optional
JSON fields MUST NOT make older SDKs fail. `Raw` is the supported escape hatch
for newer fields that are not yet represented in a DTO.

Inside the `/patroni` status object, `version` and `scope` exist throughout the
audited range, while `name` was added in Patroni 3.2.0. Decoding a 3.0.x or
3.1.x response MUST therefore succeed with `PatroniIdentity.Name == ""`; an
empty name on those releases is not evidence of a malformed response.

An HTTP status outside 2xx is not, by itself, converted into a universal Go
error. Callers MUST inspect `StatusCode`. JSON decoding is performed only for
successful responses on typed JSON endpoints; non-2xx bodies remain available
in `Raw`. The direct client intentionally does not impose high-level Patroni
command semantics.

The default response-body limit is 8 MiB and the default request timeout is 10
seconds. Callers MAY narrow either value. Bodies beyond the configured limit
MUST fail rather than be silently truncated and decoded as complete data.

## Request and transport contract

Every I/O method requires a non-nil caller context and adds its own bounded
timeout. Endpoint URLs MUST use `http` or `https`; embedded URL credentials are
rejected. Authentication is supplied through `Authorizer`, with `BasicAuth` as
the built-in implementation.

The client MUST NOT automatically retry writes. Read redirects may be followed
under normal `net/http` limits; a redirect after a non-read method MUST return
the received redirect response instead of replaying the write. Request bodies
and credential-bearing base URLs MUST not be included in formatted errors or
logs.

`patroni.Error` classifies request, authentication, transport, body-read, and
decode failures. `Delivery` has three meanings:

| State | Meaning |
| --- | --- |
| `NOT_SENT` | The request was proven not to have reached the write step |
| `MAYBE_SENT` | The transport cannot prove whether the server received a write |
| `RESPONSE_RECEIVED` | An HTTP response was received, even if its body could not be decoded |

Callers MUST treat `MAYBE_SENT` writes as ambiguous and MUST NOT blindly retry
them. The high-level control service turns this state into evidence and, when
verification cannot resolve it, an `UNKNOWN` outcome.

## TLS contract

`NewHTTPTransport` supports CA files, client certificates, encrypted private
keys, server-name override, and an explicit insecure-verification option. TLS
1.2 is the minimum. Certificate and key must be supplied together. Secret
formatting MUST indicate presence only and never reveal the key password.

An explicit CA file is the exclusive trust bundle by default;
`IncludeSystemCAs` opts into augmenting it with host roots. `TransportCache`
fingerprints loaded TLS material so rotations create a new transport without
requiring process restart. It is a bounded LRU (eight entries by default), can
be configured with `NewTransportCacheWithOptions`, and `Purge` closes idle
connections and forgets every fingerprint.

## Version selection

The direct client is a wire primitive and does not probe or reject a server
version before sending a method. Callers that select endpoints dynamically
MUST use `EndpointCatalogFor`, `HealthAliasesFor`, or `SupportsFeature`.
High-level writes perform version checks in `control.Service`.

The versioned feature catalog includes core REST (3.0), generic MPP (3.3),
quorum status and failsafe LSN headers (4.0), and readiness-lag,
reinitialize-from-leader, and standby-cluster CLI semantics (4.1).
