#!/usr/bin/env python3
"""Emit deterministic patronictl semantic facts from an installed Patroni.

This runs only inside pinned compatibility-oracle images. All DCS, REST and
PostgreSQL boundaries are local fakes; it never contacts a real cluster.
"""

from __future__ import annotations

import argparse
import json
from typing import Any
from unittest.mock import patch

from click.testing import CliRunner

from patroni.ctl import ctl
from patroni.dcs import Cluster, ClusterConfig, Leader, Member, Status, SyncState, TimelineHistory
from patroni.version import __version__


def canonical(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(key): canonical(item) for key, item in sorted(value.items(), key=lambda pair: str(pair[0]))}
    if isinstance(value, (list, tuple)):
        return [canonical(item) for item in value]
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    return str(value)


def cluster_fixture(paused: bool = False) -> Cluster:
    node_a = Member(5, "node-a", 35, {
        "api_url": "http://node-a:8008/patroni",
        "conn_url": "postgres://node-a:5433/postgres",
        "role": "primary",
        "state": "running",
        "timeline": 2,
        "xlog_location": 33554432,
    })
    node_b = Member(6, "node-b", 36, {
        "api_url": "http://node-b:8008/patroni",
        "conn_url": "postgres://node-b:5432/postgres",
        "state": "running",
        "replication_state": "streaming",
        "timeline": 2,
        "xlog_location": 16777216,
        "pause": paused,
        "scheduled_restart": {"schedule": "2100-01-01T10:00:00+00:00", "postgres_version": "99.0"},
        "tags": {},
    })
    configuration = {"loop_wait": 10, "synchronous_mode": True, "ttl": 30}
    if paused:
        configuration["pause"] = True
    history = TimelineHistory(
        7,
        '[[2,16777216,"manual","2026-07-13T10:00:00Z","node-a"]]',
        [(2, 16777216, "manual", "2026-07-13T10:00:00Z", "node-a")],
    )
    return Cluster(
        "12345",
        ClusterConfig(2, configuration, 2),
        Leader(3, 31, node_a),
        Status(33554432, None, []),
        [node_a, node_b],
        None,
        SyncState(4, "node-a", "node-b", 0),
        history,
        None,
        {},
    )


class FakeDCS:
    loop_wait = 10

    def __init__(self, cluster: Cluster) -> None:
        self.cluster = cluster
        self.mutations: list[dict[str, Any]] = []

    def get_cluster(self) -> Cluster:
        return self.cluster

    def delete_cluster(self) -> None:
        self.mutations.append({"operation": "delete_cluster"})

    def manual_failover(self, leader: str, candidate: str, **kwargs: Any) -> None:
        self.mutations.append({
            "operation": "manual_failover",
            "leader": leader,
            "candidate": candidate,
            "kwargs": canonical(kwargs),
        })

    def set_config_value(self, value: str, version: Any) -> bool:
        self.mutations.append({
            "operation": "set_config_value",
            "value": json.loads(value),
            "version": version,
        })
        return True


class FakeResponse:
    def __init__(self, status: int = 200, data: bytes = b"ok") -> None:
        self.status = status
        self.data = data


class FakeREST:
    def __init__(self, status: int = 200) -> None:
        self.status = status
        self.calls: list[dict[str, Any]] = []

    def __call__(self, member: Member, method: str = "get", endpoint: str | None = None,
                 data: Any = None) -> FakeResponse:
        self.calls.append({
            "member": member.name,
            "method": method.lower(),
            "endpoint": endpoint or "patroni",
            "data": canonical(data),
        })
        if endpoint is None:
            return FakeResponse(200, b'{"patroni":{"version":"4.1.0"},"server_version":160001}')
        return FakeResponse(self.status)


def configuration(*_args: Any, **_kwargs: Any) -> dict[str, Any]:
    return {
        "scope": "alpha",
        "namespace": "/service",
        "etcd3": {"host": "oracle.invalid:2379"},
        "postgresql": {"data_dir": "/tmp/oracle", "parameters": {}},
    }


def run_case(case: dict[str, Any]) -> dict[str, Any]:
    dcs = FakeDCS(cluster_fixture(case.get("paused", False)))
    rest = FakeREST(case.get("status", 200))
    query_result = case.get("query_result", ([['42']], ["answer"]))
    with patch("patroni.ctl.load_config", side_effect=configuration), \
            patch("patroni.ctl.get_dcs", return_value=dcs), \
            patch("patroni.ctl.request_patroni", side_effect=rest), \
            patch("patroni.ctl.query_member", return_value=query_result), \
            patch("patroni.ctl.timestamp", return_value="2026-07-13 12:00:00.00000"):
        result = CliRunner().invoke(ctl, case["args"], input=case.get("input", ""))
    return {
        "id": case["id"],
        "args": case["args"],
        "input": case.get("input", ""),
        "exit": result.exit_code,
        "exception": None if result.exception is None else {
            "type": type(result.exception).__name__,
            "message": str(result.exception),
        },
        "output": result.output,
        "rest": rest.calls,
        "dcs": dcs.mutations,
    }


def cases() -> list[dict[str, Any]]:
    matrix = [
        {"id": "dsn_default", "args": ["dsn", "alpha"]},
        {"id": "dsn_selector_conflict", "args": ["dsn", "alpha", "--role", "leader", "--member", "node-a"]},
        {"id": "query_missing_input", "args": ["query", "alpha"]},
        {"id": "query_tsv", "args": ["query", "alpha", "--command", "select oracle"]},
        {"id": "list_json", "args": ["list", "alpha", "--format", "json"]},
        {"id": "list_tsv", "args": ["list", "alpha", "--format", "tsv"]},
        {"id": "topology", "args": ["topology", "alpha"]},
        {"id": "show_config", "args": ["show-config", "alpha"]},
        {"id": "version_local", "args": ["version"]},
        {"id": "version_cluster", "args": ["version", "alpha"]},
        {"id": "history_json", "args": ["history", "alpha", "--format", "json"]},
        {"id": "reload_abort", "args": ["reload", "alpha", "node-a"], "input": "n\n"},
        {"id": "reload_200", "args": ["reload", "alpha", "node-a", "--force"]},
        {"id": "reload_202", "args": ["reload", "alpha", "node-a", "--force"], "status": 202},
        {"id": "restart_immediate", "args": ["restart", "alpha", "node-a", "--force", "--pg-version", "16.1", "--timeout", "10min"]},
        {"id": "restart_scheduled_replace", "args": ["restart", "alpha", "node-b", "--force", "--scheduled", "2100-02-03T04:05:00+00:00"]},
        {"id": "reinit", "args": ["reinit", "alpha", "node-b", "--force", "--from-leader"]},
        {"id": "failover", "args": ["failover", "alpha", "--candidate", "node-b", "--force"]},
        {"id": "switchover", "args": ["switchover", "alpha", "--leader", "node-a", "--candidate", "node-b", "--force"]},
        {"id": "flush_restart", "args": ["flush", "alpha", "node-b", "restart", "--force"]},
        {"id": "flush_switchover_noop", "args": ["flush", "alpha", "switchover", "--force"]},
        {"id": "pause", "args": ["pause", "alpha"]},
        {"id": "resume", "args": ["resume", "alpha"], "paused": True},
        {"id": "edit_config", "args": ["edit-config", "alpha", "--set", "ttl=20", "--force", "--quiet"]},
        {"id": "remove_abort", "args": ["remove", "alpha"], "input": "alpha\nwrong\n"},
        {"id": "remove", "args": ["remove", "alpha"], "input": "alpha\nYes I am aware\nnode-a\n"},
    ]
    if tuple(int(part) for part in __version__.split(".")[:2]) >= (4, 1):
        matrix.extend([
            {"id": "demote_requires_source", "args": ["demote-cluster", "alpha", "--force"]},
            {"id": "demote_abort", "args": ["demote-cluster", "alpha", "--restore-command", "restore-oracle"], "input": "n\n"},
            {"id": "promote_primary_noop", "args": ["promote-cluster", "alpha", "--force"]},
        ])
    return matrix


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--pretty", action="store_true")
    args = parser.parse_args()
    result = {"patroniVersion": __version__, "cases": [run_case(case) for case in cases()]}
    print(json.dumps(result, indent=2 if args.pretty else None, sort_keys=True))


if __name__ == "__main__":
    main()
