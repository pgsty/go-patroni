# go-patroni

go-patroni is a reusable Go SDK for Patroni and a native Go
patronictl-compatible command-line client.

The project is being extracted from BOAR's validated Patroni 4.x control
implementation. Its public layers are designed for independent consumers such
as BOAR, Pig, and other Go applications:

- the root patroni package: complete Patroni REST API wire access;
- config and model: Patroni configuration and normalized cluster identity;
- dcs and dcs/etcd3: Patroni DCS state and transactional operations;
- postgres: one-shot PostgreSQL query support used by patronictl;
- control: adapter-neutral patronictl-compatible orchestration;
- runtime: reusable configuration-to-client assembly;
- cmd/patronictl: the native Go CLI.

Patroni 4.x is the primary compatibility target. Patroni 3.x compatibility is
accepted only where it is covered by explicit source and behavioral evidence.

## Status

Initial extraction is in progress. Early commits intentionally introduce the
SDK by functional group; the final migration gate requires the complete module,
CLI compatibility suite, and BOAR downstream consumer to pass together.

## License

Apache License 2.0. See [LICENSE](LICENSE).
