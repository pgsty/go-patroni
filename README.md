# go-patroni

`go-patroni` is an independent Go SDK for Patroni and a native Go replacement
for `patronictl`. It can be imported by BOAR, Pig, or any third-party Go
application; it has no dependency on BOAR.

The module provides two API levels:

- a direct, typed REST client for every Patroni 4.1.4 HTTP method/path contract;
- a higher-level `control.Service` and `runtime` that implement Patroni DCS,
  PostgreSQL, Citus, safety, and `patronictl` orchestration semantics.

Patroni `>=3.0.0,<5.0.0` is the audited compatibility range. Patroni 4.x is the
primary target; version-gated features are rejected before an unsupported write
is sent. Embedding products may narrow this range per `control.Service` or
`runtime.Environment` instance without changing package-global state.

## Install

The SDK requires Go 1.25 or newer.

```bash
go get github.com/pgsty/go-patroni@latest
```

Install the command-line client independently:

```bash
go install github.com/pgsty/go-patroni/cmd/patronictl@latest
patronictl --help
```

## Documentation

- [中文用户与 Agent 使用手册](docs/user-guide.zh-CN.md) covers direct REST,
  Patroni YAML/runtime, CLI, and machine-output workflows.
- [中文 SDK 指南与实现审计](docs/go-patroni-sdk.zh-CN.md) records API coverage,
  version compatibility, design details, and review findings.
- [Contract specifications](docs/spec/README.md) define the normative project
  invariants and release evidence.

## Direct REST API

The root package only needs a Patroni REST URL at runtime. It does not require
etcd, a Patroni YAML file, or PostgreSQL access.

```go
package main

import (
    "context"
    "fmt"
    "log"
    "time"

    patroni "github.com/pgsty/go-patroni"
)

func main() {
    client, err := patroni.NewClient(patroni.ClientOptions{
        Timeout:    5 * time.Second,
        Authorizer: patroni.NewBasicAuth("patroni", "secret"),
        UserAgent:  "pig/1.0",
    })
    if err != nil {
        log.Fatal(err)
    }

    response, err := client.GetPatroni(context.Background(), "https://db1:8008")
    if err != nil {
        // A typed error describes request, authentication, transport, response,
        // decode, and ambiguous-write delivery states. The response still
        // retains status, headers, and the raw body when they were received.
        log.Fatal(err)
    }
    fmt.Printf("%s %s %s\n", response.Data.Patroni.Name,
        response.Data.Role, response.Data.Patroni.Version)
}
```

For TLS/mTLS and encrypted private keys, construct a transport with
`patroni.NewHTTPTransport`. `Response[T]` always retains the status code,
headers, and raw response bytes, so newer upstream fields remain available even
before a typed DTO is extended. `EndpointCatalog` exposes all 75 audited
method/path rows and their risk classes.

## High-level SDK

The high-level runtime reads a normal Patroni `patronictl.yaml`, connects to
etcd3, discovers member REST URLs, and assembles the REST, DCS, and PostgreSQL
clients used by `control.Service`:

```go
environment, err := patroniruntime.NewEnvironment(ctx,
    patroniruntime.EnvironmentOptions{
        Load: config.LoadRequest{Path: "/etc/patroni/patronictl.yaml"},
    })
if err != nil {
    return err
}

rt, err := environment.Open(ctx, patroniruntime.RuntimeOptions{
    Context:       "production",
    Operation:     config.OperationClusterRead,
    ExplicitScope: "postgres-ha",
})
if err != nil {
    return err
}
defer rt.Close()

result := rt.Service.List(ctx, control.ListRequest{
    Targets: []model.Target{rt.Target},
})
if result.Outcome != control.Succeeded {
    return result.Error
}
```

The omitted imports in that fragment are:

```go
import (
    "github.com/pgsty/go-patroni/config"
    "github.com/pgsty/go-patroni/control"
    "github.com/pgsty/go-patroni/model"
    patroniruntime "github.com/pgsty/go-patroni/runtime"
)
```

The high-level runtime currently implements Patroni's `etcd3` DCS backend. A
consumer using another DCS can implement the narrow interfaces in `dcs` and
assemble `control.Service` directly.

## Embed the command suite

Applications that want the complete `patronictl` command surface can compose
it through the public `cli` package. Product commands are registered as
extensions; Patroni parsing, prompting, rendering, and exit behavior stay in
one SDK implementation:

```go
root := cli.NewRootCommand(cli.Options{
    Application: cli.Application{
        Name: "my-app", Short: "My Patroni control plane", Version: buildVersion,
        RequestIDPrefix: "my-app-cli",
    },
    Environment: patroniruntime.EnvironmentOptions{
        Load: config.LoadRequest{Path: "/infra/conf/patronictl.yml"},
        UserAgent: "my-app/" + buildVersion,
    },
    Extensions: []cli.Extension{newServeCommand},
})
if err := root.ExecuteContext(ctx); err != nil {
    return err
}
```

An extension receives normalized root state through
`ExtensionContext.Invocation`, so explicit `--config-file`, `--dcs-url`/`--dcs`,
`--insecure`, `--context`, and `--output` values do not need to be reparsed.
Applications such as Pig that already own a command framework can instead use
`control` and `runtime` directly; importing `cli` is optional.

## Configuration

The CLI accepts Patroni's standard `PATRONICTL_CONFIG_FILE`, `DCS_URL`, `-c`,
`-d`/`--dcs-url`, and `-k` inputs. The optional `go_patroni` extension adds
named contexts and network deadlines without changing Patroni's own fields:

```yaml
scope: postgres-ha
namespace: /service/

etcd3:
  hosts: [10.10.10.10:2379, 10.10.10.11:2379]

ctl:
  authentication:
    username: patroni
    password: secret

go_patroni:
  default_context: production
  contexts:
    production: {}
    staging:
      scope: postgres-ha-staging
      etcd3:
        hosts: [10.20.20.10:2379]
  network:
    dns_timeout: 5s
    dcs_dial_timeout: 5s
    dcs_request_timeout: 10s
    patroni_timeout: 10s
    postgres_timeout: 30s
    postgres_close_timeout: 5s
```

Select a context with `--context` or `GO_PATRONI_CONTEXT`. The legacy `boar`
extension and `BOAR_CONTEXT` are accepted for migration, but new consumers
should use the product-neutral names.

## `patronictl` compatibility

The Go CLI implements all 19 commands in Patroni 4.1.4's `patronictl`:
`dsn`, `query`, `remove`, `reload`, `restart`, `reinit`, `failover`,
`switchover`, `list`, `topology`, `flush`, `pause`, `resume`, `edit-config`,
`show-config`, `version`, `history`, `demote-cluster`, and `promote-cluster`.

It also adds `discover`, `inspect-config`, multi-cluster `--all`, and stable
JSON/YAML envelopes through `-o`. Machine output uses the versioned
`patroni.pgsty.com/v1alpha1` schema.

The source-pinned compatibility evidence is in [`compatibility`](compatibility),
with the detailed support matrix in
[`docs/compatibility.md`](docs/compatibility.md). There are currently no
declared CLI deviations from the pinned Patroni 4.1.4 command contract.

## Package map

| Package          | Purpose                                                                      |
|------------------|------------------------------------------------------------------------------|
| module root      | Complete Patroni REST API client, TLS, errors, endpoint and feature catalogs |
| `config`         | Tolerant Patroni YAML loading, context overlays, secret-safe projection      |
| `model`          | Stable Patroni cluster/member identities and domain objects                  |
| `dcs`            | Capability-scoped Patroni DCS contracts and state decoding                   |
| `dcs/etcd3`      | Native etcd3 implementation, transactions, discovery, and watches            |
| `postgres`       | One-shot role-checked PostgreSQL query client                                |
| `control`        | Adapter-neutral, `patronictl`-compatible control operations                  |
| `runtime`        | Configuration-to-client assembly for applications and CLIs                   |
| `cli`            | Public composition facade for the complete command suite and extensions      |
| `cmd/patronictl` | Standalone native Go command-line client                                     |

## Safety model

- Every I/O operation takes a caller-owned `context.Context` and is bounded.
- REST writes are never automatically retried.
- Errors retain whether a write was not sent, may have been sent, or received a
  response.
- High-level writes separate preparation from execution, bind plans to the
  service instance, use DCS compare-and-swap where applicable, and return
  `UNKNOWN` rather than claiming a false failure after an ambiguous send.
- Credentials are not accepted in endpoint URLs and are redacted from string
  representations, configuration inspection, logs, and machine output.

## Development

```bash
go test -mod=readonly ./...
go vet ./...
go test -run '^$' -tags=integration ./test/integration
go run ./tools/machineschema -check
```

The isolated live matrices are opt-in because they start real Patroni, etcd,
and PostgreSQL instances. See `scripts/test-*-integration.sh`.

## License

Apache License 2.0. See [LICENSE](LICENSE).
