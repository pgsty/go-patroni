#!/usr/bin/env python3
"""Extract deterministic go-patroni inventories from pinned Patroni source.

This helper uses only the Python standard library and parses Patroni source
without importing Patroni. It is a build/test oracle, never runtime code.
"""

from __future__ import annotations

import argparse
import ast
import hashlib
import json
import pathlib
import re
import subprocess
import sys
from typing import Any


PINNED_COMMIT = "d35409952f970d7f33d36d8f868eb20fc1e2a7f7"
PINNED_VERSION = "4.1.3"
PINNED_DESCRIBE = "v4.1.3"
PINNED_COMMIT_DATE = "2026-05-05T14:33:39Z"
SUPPORTED_RANGE = ">=3.0.0,<5.0.0"
EXPECTED_COMMANDS = [
    "dsn", "query", "remove", "reload", "restart", "reinit", "failover", "switchover",
    "list", "topology", "flush", "pause", "resume", "edit-config", "show-config", "version",
    "history", "demote-cluster", "promote-cluster",
]
HEALTH_PATHS = [
    "/", "/primary", "/master", "/read-write", "/leader", "/standby-leader", "/standby_leader",
    "/replica", "/read-only", "/quorum", "/read-only-quorum", "/sync", "/synchronous",
    "/read-only-sync", "/read-only-synchronous", "/async", "/asynchronous", "/health",
]


def run_git(source: pathlib.Path, *args: str) -> str:
    command = ["git", "-C", str(source), *args]
    try:
        return subprocess.check_output(command, text=True, stderr=subprocess.PIPE).strip()
    except subprocess.CalledProcessError as exc:
        raise SystemExit(f"git command failed: {' '.join(command)}: {exc.stderr.strip()}") from exc


def line_of(text: str, needle: str) -> int:
    offset = text.find(needle)
    if offset < 0:
        raise SystemExit(f"pinned source contract missing: {needle!r}")
    return text.count("\n", 0, offset) + 1


def source_ref(path: str, line: int) -> str:
    return f"{path}:{line}"


def literal_or_expression(node: ast.AST, source: str) -> Any:
    try:
        return ast.literal_eval(node)
    except (ValueError, TypeError):
        segment = ast.get_source_segment(source, node)
        return {"expression": segment or ast.dump(node, include_attributes=False)}


def qualified_name(node: ast.AST) -> str:
    if isinstance(node, ast.Name):
        return node.id
    if isinstance(node, ast.Attribute):
        return f"{qualified_name(node.value)}.{node.attr}"
    return ast.dump(node, include_attributes=False)


def decode_call(call: ast.Call, source: str) -> dict[str, Any]:
    return {
        "call": qualified_name(call.func),
        "args": [literal_or_expression(value, source) for value in call.args],
        "kwargs": {item.arg: literal_or_expression(item.value, source) for item in call.keywords},
    }


def parameter_from_call(call: dict[str, Any], ref: str) -> dict[str, Any] | None:
    kind = call["call"].rsplit(".", 1)[-1]
    if kind not in {"option", "argument"}:
        return None
    args = call["args"]
    kwargs = call["kwargs"]
    if kind == "argument":
        name = args[0]
        return normalize_parameter({
            "kind": "argument",
            "name": name,
            "required": kwargs.get("required", True),
            "nargs": kwargs.get("nargs", 1),
            "type": kwargs.get("type", "string"),
            "default": kwargs.get("default"),
            "sourceRef": ref,
        })

    declarations = [item for item in args if isinstance(item, str)]
    flags = [item for item in declarations if item.startswith("-")]
    explicit_destination = [item for item in declarations if not item.startswith("-")]
    if explicit_destination:
        destination = explicit_destination[-1]
    else:
        longest = max(flags, key=len)
        destination = longest.lstrip("-").replace("-", "_")
    is_flag = bool(kwargs.get("is_flag", False))
    multiple = bool(kwargs.get("multiple", False))
    if "default" in kwargs:
        default = kwargs["default"]
    elif is_flag:
        default = False
    elif multiple:
        default = []
    else:
        default = None
    return normalize_parameter({
        "kind": "option",
        "name": destination,
        "flags": flags,
        "required": kwargs.get("required", False),
        "type": kwargs.get("type", "boolean" if is_flag else "string"),
        "default": default,
        "env": kwargs.get("envvar"),
        "multiple": multiple,
        "isFlag": is_flag,
        "help": kwargs.get("help", ""),
        "sourceRef": ref,
    })


def normalize_parameter(parameter: dict[str, Any]) -> dict[str, Any]:
    type_value = parameter.get("type")
    expression = type_value.get("expression") if isinstance(type_value, dict) else None
    scalar_types = {"str": "string", "int": "integer", "float": "number"}
    if expression in scalar_types:
        parameter["type"] = scalar_types[expression]
    elif expression == "role_choice":
        parameter["type"] = "string"
        parameter["choices"] = ["leader", "primary", "standby-leader", "replica", "standby", "any"]
    elif expression and expression.startswith("click.Choice("):
        match = re.fullmatch(r"click\.Choice\((\[.*\])\)", expression)
        if match:
            parameter["type"] = "string"
            parameter["choices"] = ast.literal_eval(match.group(1))
    elif expression and expression.startswith("click.File("):
        parameter["type"] = "file"
        parameter["typeSource"] = expression

    default = parameter.get("default")
    default_expression = default.get("expression") if isinstance(default, dict) else None
    if default_expression == "repr(CtlPostgresqlRole.ANY)":
        parameter["default"] = "any"
        parameter["defaultSource"] = default_expression
    elif default_expression:
        parameter["defaultSource"] = default_expression
    return parameter


def command_semantics() -> dict[str, dict[str, Any]]:
    human_exit = {
        "success": "normal return exits 0",
        "usage": "Click usage and PatroniCtlException paths exit non-zero",
        "operation": "preserve source behavior; some per-member HTTP failures are rendered and return 0",
    }
    inventory_test = "test/compat/manifest_test.go#TestPatronictlInventory"
    common = {
        "exitBehavior": human_exit,
        "tests": {"inventory": [inventory_test], "unit": [], "golden": [], "differential": [], "integration": []},
        "deviations": [],
        "status": "pending",
    }

    def entry(*, paths: list[str], writes: bool, risk: str, formats: list[str], prompts: list[dict[str, str]] | None = None,
              fallback: dict[str, Any] | None = None, requirements: list[str] | None = None,
              operation_exit: str | None = None) -> dict[str, Any]:
        value = dict(common)
        value.update({
            "dataPaths": paths,
            "writes": writes,
            "risk": risk,
            "formats": formats,
            "prompts": prompts or [],
            "fallback": fallback or {"mode": "none"},
            "requirements": requirements or [],
        })
        if operation_exit:
            value["exitBehavior"] = dict(human_exit, operation=operation_exit)
        return value

    member_confirm = [{"when": "without --force", "behavior": "confirm selected member action"}]
    commands = {
        "dsn": entry(paths=["dcs-read"], writes=False, risk="read", formats=["text"], requirements=["FR14"]),
        "query": entry(paths=["dcs-read", "postgres-write-or-read"], writes=True, risk="sql",
                       formats=["pretty", "tsv", "json", "yaml"],
                       prompts=[{"when": "--password or server requests a password", "behavior": "hidden password prompt"}],
                       requirements=["FR32", "FR33"],
                       operation_exit="database errors are rendered as result rows by the oracle and normally exit 0"),
        "remove": entry(paths=["dcs-read", "dcs-prefix-delete"], writes=True, risk="destructive",
                        formats=["pretty", "tsv", "json", "yaml", "yml"],
                        prompts=[
                            {"when": "always", "behavior": "type exact cluster name"},
                            {"when": "always", "behavior": "type exact phrase Yes I am aware"},
                            {"when": "cluster has a leader", "behavior": "type exact current leader name"},
                        ], requirements=["FR26"]),
        "reload": entry(paths=["dcs-read", "rest-post-/reload"], writes=True, risk="admin-write",
                        formats=["human"], prompts=member_confirm, requirements=["FR17"],
                        operation_exit="per-member HTTP rejection is rendered and the oracle normally exits 0"),
        "restart": entry(paths=["dcs-read", "rest-post-/restart"], writes=True, risk="admin-write",
                         formats=["human"], prompts=[
                             {"when": "schedule omitted and without --force", "behavior": "prompt when to restart; default now"},
                             {"when": "without --force", "behavior": "confirm members or scheduled restart"},
                             {"when": "PostgreSQL version condition omitted and without --force", "behavior": "prompt optional version condition"},
                         ], requirements=["FR18"],
                         operation_exit="per-member HTTP rejection is rendered and the oracle normally exits 0"),
        "reinit": entry(paths=["dcs-read", "rest-post-/reinitialize"], writes=True, risk="destructive",
                        formats=["human"], prompts=[
                            {"when": "member omitted and without --force", "behavior": "prompt for replica member"},
                            {"when": "without --force", "behavior": "confirm selected replicas"},
                        ], requirements=["FR20"]),
        "failover": entry(paths=["dcs-read", "rest-post-/failover", "dcs-failover-cas"], writes=True,
                          risk="availability", formats=["human"], prompts=[
                              {"when": "Citus group omitted and interactive", "behavior": "prompt for group"},
                              {"when": "candidate omitted and without --force", "behavior": "prompt for candidate"},
                              {"when": "asynchronous candidate under synchronous mode", "behavior": "explicit asynchronous failover confirmation"},
                              {"when": "without --force", "behavior": "confirm failover and current leader demotion"},
                          ], fallback={
                              "mode": "rest-to-dcs",
                              "trigger": "transport exception from Patroni request",
                              "sendState": "fallback is source-compatible even when the REST request may have been sent",
                              "sourceBehavior": "manual_failover write after logging Failing over to DCS",
                          }, requirements=["FR21", "FR28"]),
        "switchover": entry(paths=["dcs-read", "rest-post-/switchover", "rest-post-/failover-legacy", "dcs-failover-cas"],
                            writes=True, risk="availability", formats=["human"], prompts=[
                                {"when": "Citus group omitted and interactive", "behavior": "prompt for group"},
                                {"when": "leader/candidate omitted and without --force", "behavior": "prompt for target values"},
                                {"when": "schedule omitted and without --force", "behavior": "prompt when to switchover; default now"},
                                {"when": "without --force", "behavior": "confirm immediate or scheduled switchover"},
                            ], fallback={
                                "mode": "rest-to-rest-legacy-then-dcs",
                                "trigger": "501 unsupported switchover uses /failover; transport exception writes DCS",
                                "sendState": "DCS fallback remains source-compatible after a possibly-sent REST request",
                            }, requirements=["FR22", "FR28"]),
        "list": entry(paths=["dcs-read"], writes=False, risk="read",
                      formats=["pretty", "tsv", "json", "yaml", "yml"], requirements=["FR10", "FR13"]),
        "topology": entry(paths=["dcs-read"], writes=False, risk="read", formats=["topology"], requirements=["FR12"]),
        "flush": entry(paths=["dcs-read", "rest-delete-/restart", "rest-delete-/switchover", "dcs-failover-cas"],
                       writes=True, risk="admin-write", formats=["human"], prompts=member_confirm,
                       fallback={
                           "mode": "command-specific",
                           "trigger": "scheduled switchover only: no accessible member after probing leader-first",
                           "sendState": "clear failover key with its observed version; restart has no DCS fallback",
                       }, requirements=["FR19", "FR23"]),
        "pause": entry(paths=["dcs-read", "rest-patch-/config"], writes=True, risk="availability",
                       formats=["human"], requirements=["FR24"]),
        "resume": entry(paths=["dcs-read", "rest-patch-/config"], writes=True, risk="availability",
                        formats=["human"], requirements=["FR24"]),
        "edit-config": entry(paths=["dcs-read", "dcs-config-cas"], writes=True, risk="configuration",
                             formats=["human-diff", "yaml-editor"], prompts=[
                                 {"when": "no --set/--pg/--apply/--replace", "behavior": "launch EDITOR/editor/vi"},
                                 {"when": "changes exist and without --force", "behavior": "confirm Apply these changes?"},
                             ], requirements=["FR25"]),
        "show-config": entry(paths=["dcs-read"], writes=False, risk="read", formats=["yaml"], requirements=["FR15", "FR25"]),
        "version": entry(paths=["local", "dcs-read", "rest-get-/"], writes=False, risk="read",
                         formats=["human"], requirements=["FR15", "FR57"]),
        "history": entry(paths=["dcs-read"], writes=False, risk="read",
                         formats=["pretty", "tsv", "json", "yaml", "yml"], requirements=["FR15"]),
        "demote-cluster": entry(paths=["dcs-read", "rest-patch-/config", "dcs-convergence-read"], writes=True,
                                risk="availability", formats=["human"], prompts=[
                                    {"when": "without --force", "behavior": "confirm cluster demotion"},
                                ], requirements=["FR27"]),
        "promote-cluster": entry(paths=["dcs-read", "rest-patch-/config", "dcs-convergence-read"], writes=True,
                                 risk="availability", formats=["human"], prompts=[
                                     {"when": "without --force", "behavior": "confirm cluster promotion"},
                                 ], requirements=["FR27"]),
    }
    # Patroni's Citus behavior is command-specific: option_citus_group expands
    # an omitted group to every group for member-oriented operations,
    # option_default_citus_group selects the configured group, and commands
    # without a group option operate on the coordinator. Keep this explicit in
    # the generated contract so a new adapter cannot silently collapse all
    # group-less operations onto the coordinator root.
    citus_contracts = {
        "dsn": {"groupOption": "explicit", "omittedGroup": "all-groups"},
        "query": {"groupOption": "explicit", "omittedGroup": "all-groups"},
        "remove": {"groupOption": "explicit", "omittedGroup": "rejected"},
        "reload": {"groupOption": "explicit", "omittedGroup": "all-groups"},
        "restart": {"groupOption": "explicit", "omittedGroup": "all-groups"},
        "reinit": {"groupOption": "explicit", "omittedGroup": "all-groups"},
        "failover": {"groupOption": "explicit", "omittedGroup": "prompt-or-reject-with-force"},
        "switchover": {"groupOption": "explicit", "omittedGroup": "prompt-or-reject-with-force"},
        "list": {"groupOption": "explicit", "omittedGroup": "all-groups"},
        "topology": {"groupOption": "explicit", "omittedGroup": "all-groups"},
        "flush": {
            "groupOption": "explicit",
            "omittedGroup": "target-dependent",
            "targets": {"restart": "all-groups", "switchover": "coordinator-group-0"},
        },
        "pause": {"groupOption": "configured-default", "omittedGroup": "configured-group"},
        "resume": {"groupOption": "configured-default", "omittedGroup": "configured-group"},
        "edit-config": {"groupOption": "configured-default", "omittedGroup": "configured-group"},
        "show-config": {"groupOption": "configured-default", "omittedGroup": "configured-group"},
        "version": {"groupOption": "explicit", "omittedGroup": "all-groups"},
        "history": {"groupOption": "configured-default", "omittedGroup": "configured-group"},
        "demote-cluster": {"groupOption": "none", "omittedGroup": "coordinator-group-0"},
        "promote-cluster": {"groupOption": "none", "omittedGroup": "coordinator-group-0"},
    }
    if set(citus_contracts) != set(commands):
        raise RuntimeError("Citus compatibility matrix must cover every patronictl command")
    for name, contract in citus_contracts.items():
        commands[name]["citus"] = contract
    commands["failover"]["tests"] = dict(commands["failover"]["tests"], unit=[
        "control/service_read_test.go#TestFailoverAndSwitchoverPrepareFreezePatronictlSelection",
        "control/service_read_test.go#TestFailoverRESTSuccessDefiniteFailureAndConcurrency",
        "control/service_read_test.go#TestFailoverTransportExceptionFallsBackToExactDCSCAS",
    ], integration=[
        "test/integration/dcs_etcd3_test.go#TestEtcd3PatroniSnapshotDiscoveryCASRemoveAndWatch",
        "test/integration/patroni_rest_test.go#TestPatroniRESTInventoryAgainstIsolatedRealPatroni",
    ])
    commands["switchover"]["tests"] = dict(commands["switchover"]["tests"], unit=[
        "control/service_read_test.go#TestFailoverAndSwitchoverPrepareFreezePatronictlSelection",
        "control/service_read_test.go#TestSwitchoverUsesLegacyEndpointOnlyForExact501Contract",
        "control/service_read_test.go#TestScheduledSwitchoverRESTAcceptanceRequiresDCSReadback",
        "control/service_read_test.go#TestScheduledSwitchoverFallbackAndCancellation",
    ])
    commands["flush"]["tests"] = dict(commands["flush"]["tests"], unit=[
        "control/flush_test.go#TestPrepareFlushFreezesPatronictlSelection",
        "control/flush_test.go#TestFlushRestartClassifiesAndVerifiesOutcomes",
        "control/flush_test.go#TestFlushSwitchoverProbesLeaderFirstAndStopsOnTerminalResponse",
        "control/flush_test.go#TestFlushSwitchoverFallsBackToExactDCSDelete",
        "control/flush_test.go#TestFlushSwitchoverConflictAndCancellationRemainSafe",
        "control/flush_test.go#TestFlushRestartExpandsAndRevalidatesGroupLessCitusScope",
        "control/flush_test.go#TestFlushSwitchoverUsesCitusCoordinatorGroup",
    ], integration=[
        "test/integration/dcs_etcd3_test.go#TestEtcd3PatroniSnapshotDiscoveryCASRemoveAndWatch",
        "test/integration/patroni_rest_test.go#TestPatroniRESTInventoryAgainstIsolatedRealPatroni",
    ])
    pause_tests = [
        "control/pause_test.go#TestPreparePauseResumeFreezesLeaderFirstPlan",
        "control/pause_test.go#TestPauseResumePayloadAndTerminalResponseSemantics",
        "control/pause_test.go#TestPauseResumeProbingAndAmbiguousEvidence",
        "control/pause_test.go#TestPauseResumeWaitConvergenceAndConcurrency",
    ]
    commands["pause"]["tests"] = dict(commands["pause"]["tests"], unit=pause_tests,
        integration=["test/integration/patroni_rest_test.go#TestPatroniRESTInventoryAgainstIsolatedRealPatroni"])
    commands["resume"]["tests"] = dict(commands["resume"]["tests"], unit=pause_tests,
        integration=["test/integration/patroni_rest_test.go#TestPatroniRESTInventoryAgainstIsolatedRealPatroni"])
    commands["edit-config"]["tests"] = dict(commands["edit-config"]["tests"], unit=[
        "control/edit_config_test.go#TestPreviewEditConfigMatchesPatroniPatchSetAndReplaceSemantics",
        "control/edit_config_test.go#TestPrepareEditConfigFreezesSecretSafeCASPlan",
        "control/edit_config_test.go#TestExecuteEditConfigClassifiesCASAndAuthoritativeReadback",
        "control/edit_config_test.go#TestExecuteEditConfigRejectsConcurrencyTamperingAndCancellation",
    ], integration=[
        "test/integration/dcs_etcd3_test.go#TestEtcd3PatroniSnapshotDiscoveryCASRemoveAndWatch",
    ])
    commands["remove"]["tests"] = dict(commands["remove"]["tests"], unit=[
        "control/remove_test.go#TestPrepareRemoveFreezesExactDestructiveConfirmation",
        "control/remove_test.go#TestExecuteRemoveRequiresPatronictlConfirmations",
        "control/remove_test.go#TestExecuteRemoveClassifiesDeleteAndAuthoritativeReadback",
        "control/remove_test.go#TestExecuteRemoveConcurrencyAndCancellationRemainSafe",
    ], integration=[
        "test/integration/dcs_etcd3_test.go#TestEtcd3PatroniSnapshotDiscoveryCASRemoveAndWatch",
    ])
    standby_cluster_tests = [
        "control/standby_cluster_test.go#TestPrepareStandbyClusterTransitionsFreezeLeaderAndSecretSafePlan",
        "control/standby_cluster_test.go#TestStandbyClusterTransitionPayloadAndDefiniteOutcomes",
        "control/standby_cluster_test.go#TestStandbyClusterTransitionAmbiguousReadback",
        "control/standby_cluster_test.go#TestStandbyClusterTransitionConvergenceConcurrencyCancellation",
        "control/standby_cluster_test.go#TestStandbyClusterVerificationPolicyIsBoundedInjectableAndDelayed",
        "control/standby_cluster_test.go#TestStandbyClusterCitusCommandsSelectCoordinatorGroupZero",
    ]
    commands["demote-cluster"]["tests"] = dict(commands["demote-cluster"]["tests"], unit=standby_cluster_tests,
        integration=["test/integration/patroni_rest_test.go#TestPatroniRESTInventoryAgainstIsolatedRealPatroni"])
    commands["promote-cluster"]["tests"] = dict(commands["promote-cluster"]["tests"], unit=standby_cluster_tests,
        integration=["test/integration/patroni_rest_test.go#TestPatroniRESTInventoryAgainstIsolatedRealPatroni"])

    unit_tests = {
        "dsn": [
            "control/service_read_test.go#TestReadServiceDSNMatchesMemberSelectionAndNeverReturnsCredentials",
            "control/discover_test.go#TestListExpandsGroupLessCitusScopeInsideControl",
        ],
        "query": [
            "control/service_read_test.go#TestReadServiceQueryResolvesMemberAndChecksNonLeaderRole",
            "control/service_read_test.go#TestReadServiceQueryPreservesPatronictlResultRowErrorSemantics",
            "control/service_read_test.go#TestControlQueryRequestFormattingAndJSONRedactSQLAndConnection",
            "control/version_gate_test.go#TestUnsupportedPatroniReadRequiresExplicitBestEffortPolicy",
            "control/discover_test.go#TestListExpandsGroupLessCitusScopeInsideControl",
        ],
        "reload": [
            "control/service_read_test.go#TestReloadPrepareBuildsCompleteDeterministicPlan",
            "control/service_read_test.go#TestReloadExecuteClassifiesAcceptedAndDefiniteHTTPFailure",
            "control/service_read_test.go#TestReloadAmbiguousTransportIsUnknownAndNeverRetried",
            "control/service_read_test.go#TestReloadNotSentIsDefiniteAndCancellationStopsSubsequentWrites",
            "control/service_read_test.go#TestReloadFreshSnapshotDetectsConcurrentMembershipChangeBeforeWrite",
            "control/discover_test.go#TestReloadExpandsAndRevalidatesGroupLessCitusScope",
        ],
        "restart": [
            "control/service_read_test.go#TestRestartPrepareFiltersPendingValidatesConditionsAndFreezesAnySelection",
            "control/service_read_test.go#TestRestartImmediateSuccessAndDefiniteConflict",
            "control/service_read_test.go#TestRestartImmediateAmbiguousAndNotSentAreNeverRetried",
            "control/service_read_test.go#TestRestartDetectsPendingConcurrencyAndSelectorTamperingBeforeWrite",
            "control/service_read_test.go#TestRestartScheduledReadAfterWriteResolvesAcceptedAndAmbiguousSend",
            "control/service_read_test.go#TestRestartForcedReplacementStopsAfterAmbiguousFlush",
            "control/service_read_test.go#TestRestartForcedReplacementContinuesAfterDefiniteFlushResponse",
            "control/discover_test.go#TestRestartExpandsAndRevalidatesGroupLessCitusScope",
        ],
        "reinit": [
            "control/service_read_test.go#TestReinitializePrepareIsReplicaOnlyAndPreservesForceNoop",
            "control/service_read_test.go#TestReinitializeSuccessFailureUnknownAndNoRetry",
            "control/service_read_test.go#TestReinitializeWaitUsesPatroniEvidenceToResolveOutcome",
            "control/service_read_test.go#TestReinitializeDetectsReplicaPromotionBeforeWrite",
            "control/discover_test.go#TestReinitializeExpandsAndRevalidatesGroupLessCitusScope",
        ],
        "list": [
            "control/service_read_test.go#TestReadServiceListProjectsPatroniClusterDeterministically",
            "control/discover_test.go#TestListExpandsGroupLessCitusScopeInsideControl",
        ],
        "topology": [
            "control/service_read_test.go#TestReadServiceTopologyUsesReplicateFromTree",
            "control/discover_test.go#TestListExpandsGroupLessCitusScopeInsideControl",
        ],
        "show-config": ["control/service_read_test.go#TestReadServiceShowConfigAndHistoryAreNormalizedCopies"],
        "version": [
            "control/service_read_test.go#TestReadServiceVersionIsolatesPerMemberRESTFailure",
            "control/version_gate_test.go#TestVersionRESTProbeHonorsUnsupportedReadPolicy",
            "control/discover_test.go#TestListExpandsGroupLessCitusScopeInsideControl",
        ],
        "history": ["control/service_read_test.go#TestReadServiceShowConfigAndHistoryAreNormalizedCopies"],
    }
    differential_cases = {
        "dsn": ["dsn_default", "dsn_selector_conflict"],
        "query": ["query_missing_input", "query_tsv"],
        "remove": ["remove_abort", "remove"],
        "reload": ["reload_abort", "reload_200", "reload_202"],
        "restart": ["restart_immediate", "restart_scheduled_replace"],
        "reinit": ["reinit"],
        "failover": ["failover"],
        "switchover": ["switchover"],
        "list": ["list_json", "list_tsv"],
        "topology": ["topology"],
        "flush": ["flush_restart", "flush_switchover_noop"],
        "pause": ["pause"],
        "resume": ["resume"],
        "edit-config": ["edit_config"],
        "show-config": ["show_config"],
        "version": ["version_local", "version_cluster"],
        "history": ["history_json"],
        "demote-cluster": ["demote_requires_source", "demote_abort"],
        "promote-cluster": ["promote_primary_noop"],
    }
    etcd_integration = "test/integration/dcs_etcd3_test.go#TestEtcd3PatroniSnapshotDiscoveryCASRemoveAndWatch"
    patroni_integration = "test/integration/patroni_rest_test.go#TestPatroniRESTInventoryAgainstIsolatedRealPatroni"
    postgres_integration = "test/integration/postgres_test.go#TestPostgreSQLNativeQueryTLSAuthMultiResultLimitsErrorAndCancel"
    shared_cluster_integration = "test/integration/multicluster_cli_test.go#TestM5DefaultContextThreeNodeMultiClusterCLI"
    citus_integration = "test/integration/citus_cli_test.go#TestM8CitusMultiGroupCLI"
    integration_tests = {
        "dsn": [etcd_integration, citus_integration], "query": [etcd_integration, postgres_integration],
        "remove": [etcd_integration], "reload": [etcd_integration, patroni_integration, shared_cluster_integration],
        "restart": [etcd_integration, patroni_integration, shared_cluster_integration], "reinit": [etcd_integration, patroni_integration, shared_cluster_integration],
        "failover": [etcd_integration, patroni_integration, shared_cluster_integration], "switchover": [etcd_integration, patroni_integration, shared_cluster_integration],
        "list": [etcd_integration, shared_cluster_integration, citus_integration], "topology": [etcd_integration, shared_cluster_integration, citus_integration],
        "flush": [etcd_integration, patroni_integration], "pause": [etcd_integration, patroni_integration],
        "resume": [etcd_integration, patroni_integration], "edit-config": [etcd_integration, shared_cluster_integration],
        "show-config": [etcd_integration], "version": [etcd_integration, patroni_integration],
        "history": [etcd_integration], "demote-cluster": [etcd_integration, patroni_integration, shared_cluster_integration],
        "promote-cluster": [etcd_integration, patroni_integration, shared_cluster_integration],
    }
    for name in ("reload", "restart", "failover", "flush", "pause", "resume", "edit-config", "show-config", "version"):
        integration_tests[name].append(citus_integration)

    def merge_links(*groups: list[str]) -> list[str]:
        return list(dict.fromkeys(link for group in groups for link in group))

    machine_contract = "internal/cli/adapter_test.go#TestEveryPatronictlCommandHasVersionedMachineUsageEnvelope"
    cobra_contract = "internal/cli/root_test.go#TestCobraTreeMatchesPinnedPatronictlInventory"
    citus_contract = "test/compat/citus_semantics_test.go#TestPatronictlCitusSemanticsMatrix"
    write_gate = "control/version_gate_test.go#TestEveryWritePlanFailsClosedForUnsupportedPatroni"
    for name, command in commands.items():
        existing = command["tests"]
        units = merge_links(existing["unit"], unit_tests.get(name, []), [cobra_contract, machine_contract, citus_contract])
        if command["writes"] and name != "query":
            units = merge_links(units, [write_gate])
        command["tests"] = {
            "inventory": existing["inventory"],
            "unit": units,
            "golden": merge_links(existing["golden"], [machine_contract]),
            "differential": [
                "internal/cli/compat_oracle_test.go#TestPatronictlSemanticParity/" + case
                for case in differential_cases[name]
            ],
            "integration": merge_links(existing["integration"], integration_tests[name]),
        }
        command["status"] = "complete"
    return commands


class Extractor:
    def __init__(self, source: pathlib.Path) -> None:
        self.source = source
        self.files = {
            "ctl": source / "patroni" / "ctl.py",
            "api": source / "patroni" / "api.py",
            "dcs": source / "patroni" / "dcs" / "__init__.py",
            "etcd3": source / "patroni" / "dcs" / "etcd3.py",
            "version": source / "patroni" / "version.py",
            "restDocs": source / "docs" / "rest_api.rst",
        }
        missing = [str(path) for path in self.files.values() if not path.is_file()]
        if missing:
            raise SystemExit(f"Patroni source is missing required files: {', '.join(missing)}")
        self.text = {name: path.read_text(encoding="utf-8") for name, path in self.files.items()}
        self.git_source = (source / ".git").exists()
        if self.git_source:
            commit = run_git(source, "rev-parse", "HEAD")
            if commit != PINNED_COMMIT:
                raise SystemExit(f"Patroni source commit mismatch: expected {PINNED_COMMIT}, got {commit}")
            dirty = run_git(source, "status", "--porcelain", "--untracked-files=no", "--", *[str(p.relative_to(source)) for p in self.files.values()])
            if dirty:
                raise SystemExit(f"pinned Patroni contract files are dirty:\n{dirty}")
        elif PINNED_COMMIT[:7] not in source.name:
            raise SystemExit(
                f"Patroni source archive directory must include pinned commit {PINNED_COMMIT[:7]} in its name"
            )
        version_match = re.search(r"^__version__\s*=\s*['\"]([^'\"]+)", self.text["version"], re.MULTILINE)
        if not version_match or version_match.group(1) != PINNED_VERSION:
            got = version_match.group(1) if version_match else "missing"
            raise SystemExit(f"Patroni version mismatch: expected {PINNED_VERSION}, got {got}")

    def source_inventory(self) -> dict[str, Any]:
        commit_date = run_git(self.source, "show", "-s", "--format=%aI", "HEAD") if self.git_source else PINNED_COMMIT_DATE
        return {
            "schemaVersion": "patroni.pgsty.com/compatibility/v1alpha1",
            "kind": "PatroniSourcePin",
            "generatedBy": "tools/compatgen + test/compat/oracle/extract_inventory.py",
            "repository": "https://github.com/patroni/patroni",
            "commit": PINNED_COMMIT,
            "describe": run_git(self.source, "describe", "--tags", "--always") if self.git_source else PINNED_DESCRIBE,
            "version": PINNED_VERSION,
            "commitDate": commit_date,
            "supportedRange": SUPPORTED_RANGE,
            "contractFiles": [
                {
                    "path": str(path.relative_to(self.source)),
                    "sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
                }
                for path in self.files.values()
            ],
            "sourceKind": "git-checkout" if self.git_source else "github-tag-archive",
            "worktree": {"contractFilesClean": True},
        }

    def patronictl_inventory(self) -> dict[str, Any]:
        source = self.text["ctl"]
        tree = ast.parse(source)
        aliases: dict[str, tuple[dict[str, Any], int]] = {}
        functions: list[ast.FunctionDef] = []
        for node in tree.body:
            if isinstance(node, (ast.Assign, ast.AnnAssign)):
                value = node.value
                targets = node.targets if isinstance(node, ast.Assign) else [node.target]
                if isinstance(value, ast.Call) and qualified_name(value.func) in {"click.option", "click.argument"}:
                    decoded = decode_call(value, source)
                    for target in targets:
                        if isinstance(target, ast.Name):
                            aliases[target.id] = (decoded, node.lineno)
            elif isinstance(node, ast.FunctionDef):
                functions.append(node)

        root = next((node for node in functions if node.name == "ctl"), None)
        if root is None:
            raise SystemExit("could not locate patronictl root Click group")
        globals_: list[dict[str, Any]] = []
        for decorator in root.decorator_list:
            if isinstance(decorator, ast.Call):
                item = parameter_from_call(decode_call(decorator, source), source_ref("patroni/ctl.py", decorator.lineno))
                if item:
                    globals_.append(item)

        semantics = command_semantics()
        commands: list[dict[str, Any]] = []
        for function in functions:
            command_name = None
            command_help = ""
            command_line = 0
            parameters: list[dict[str, Any]] = []
            for decorator in function.decorator_list:
                decoded = None
                ref_line = decorator.lineno
                if isinstance(decorator, ast.Call):
                    decoded = decode_call(decorator, source)
                elif isinstance(decorator, ast.Name) and decorator.id in aliases:
                    decoded, ref_line = aliases[decorator.id]
                if not decoded:
                    continue
                if decoded["call"] == "ctl.command":
                    command_name = decoded["args"][0]
                    command_help = decoded["kwargs"].get("help", "")
                    command_line = decorator.lineno
                    continue
                item = parameter_from_call(decoded, source_ref("patroni/ctl.py", ref_line))
                if item:
                    parameters.append(item)
            if command_name is None:
                continue
            if command_name not in semantics:
                raise SystemExit(f"unclassified Patroni command: {command_name}")
            command = {
                "command": command_name,
                "function": function.name,
                "help": command_help,
                "sourceRef": source_ref("patroni/ctl.py", command_line),
                "parameters": parameters,
            }
            command.update(semantics[command_name])
            for prompt in command["prompts"]:
                prompt["sourceRef"] = source_ref("patroni/ctl.py", function.lineno)
            commands.append(command)

        names = [item["command"] for item in commands]
        if names != EXPECTED_COMMANDS:
            raise SystemExit(f"Patroni command inventory drift: expected {EXPECTED_COMMANDS}, got {names}")
        return {
            "schemaVersion": "patroni.pgsty.com/compatibility/v1alpha1",
            "kind": "PatronictlCompatibility",
            "generatedBy": "tools/compatgen + test/compat/oracle/extract_inventory.py",
            "source": {"commit": PINNED_COMMIT, "version": PINNED_VERSION, "file": "patroni/ctl.py"},
            "supportedRange": SUPPORTED_RANGE,
            "expectedCommandCount": len(EXPECTED_COMMANDS),
            "rootParameters": globals_,
            "commands": commands,
            "goPatroniAdditions": ["discover", "inspect-config", "list --all", "topology --all", "-o/--output", "--context"],
            "releasePolicy": "all commands must be complete and have non-empty behavior test links before release",
        }

    def rest_inventory(self) -> dict[str, Any]:
        source = self.text["api"]
        tree = ast.parse(source)
        handlers: dict[str, int] = {}
        for node in ast.walk(tree):
            if isinstance(node, ast.FunctionDef) and node.name.startswith("do_"):
                handlers[node.name] = node.lineno
        required_handlers = {
            "do_GET", "do_HEAD", "do_OPTIONS", "do_GET_liveness", "do_GET_readiness", "do_GET_patroni",
            "do_GET_cluster", "do_GET_history", "do_GET_config", "do_GET_metrics", "do_PATCH_config",
            "do_PUT_config", "do_POST_reload", "do_GET_failsafe", "do_POST_failsafe", "do_POST_sigterm",
            "do_POST_restart", "do_DELETE_restart", "do_DELETE_switchover", "do_POST_reinitialize",
            "do_POST_failover", "do_POST_switchover", "do_POST_citus", "do_POST_mpp",
        }
        missing = sorted(required_handlers - handlers.keys())
        if missing:
            raise SystemExit(f"Patroni REST handler drift; missing: {missing}")
        for token in ["'master' in path", "'standby_leader' in path", "'/read-only-synchronous'", "'/async'"]:
            if token not in source and token not in self.text["restDocs"]:
                raise SystemExit(f"Patroni health alias drift; missing source token: {token}")

        tests = {
            "inventory": ["test/compat/manifest_test.go#TestRESTInventory"],
            "contract": [
                "client_test.go#TestEveryCatalogEndpointHasCallableWireContract",
                "client_test.go#TestDecodeErrorPreservesStatusHeadersAndRaw",
                "client_test.go#TestWriteTransportNeverRetries",
                "tls_test.go#TestTLSMutualAuthenticationEncryptedKeyAndRotationCache",
            ],
            "integration": [
                "test/integration/patroni_rest_test.go#TestPatroniRESTInventoryAgainstIsolatedRealPatroni",
                "scripts/test-patroni-integration.sh#v3.3.8-v4.0.7-v4.1.3-matrix",
            ],
        }
        endpoints: list[dict[str, Any]] = []

        def add(method: str, path: str, handler: str, risk: str, response: str, request: str = "none") -> None:
            since = "4.0.0" if path in {"/quorum", "/read-only-quorum"} else \
                "3.3.0" if method == "POST" and path == "/mpp" else "3.0.0"
            endpoints.append({
                "id": f"{method.lower()}-{path.strip('/').replace('/', '-') or 'root'}",
                "method": method,
                "path": path,
                "handler": handler,
                "risk": risk,
                "request": request,
                "response": response,
                "since": since,
                "rawResponse": True,
                "sourceRef": source_ref("patroni/api.py", handlers[handler]),
                "tests": tests,
                "status": "complete",
            })

        for path in HEALTH_PATHS:
            add("GET", path, "do_GET", "read", "status-json")
            add("HEAD", path, "do_HEAD", "read", "status-only")
            add("OPTIONS", path, "do_OPTIONS", "read", "status-only")
        for path, handler, risk, response in [
            ("/liveness", "do_GET_liveness", "read", "status-only"),
            ("/readiness", "do_GET_readiness", "read", "status-only"),
            ("/patroni", "do_GET_patroni", "read", "status-json"),
            ("/cluster", "do_GET_cluster", "read", "cluster-json"),
            ("/history", "do_GET_history", "read", "history-json"),
            ("/config", "do_GET_config", "read", "config-json"),
            ("/metrics", "do_GET_metrics", "read", "prometheus-text"),
            ("/failsafe", "do_GET_failsafe", "peer-internal-read", "failsafe-json"),
        ]:
            add("GET", path, handler, risk, response)
        for method, path, handler, risk, request, response in [
            ("PATCH", "/config", "do_PATCH_config", "admin-write", "config-patch-json", "config-json"),
            ("PUT", "/config", "do_PUT_config", "admin-write", "config-json", "config-json"),
            ("POST", "/reload", "do_POST_reload", "admin-write", "none", "text"),
            ("POST", "/failsafe", "do_POST_failsafe", "peer-internal", "failsafe-peer-json", "text"),
            ("POST", "/sigterm", "do_POST_sigterm", "test-platform-dangerous", "none", "text"),
            ("POST", "/restart", "do_POST_restart", "admin-write", "restart-json", "text"),
            ("DELETE", "/restart", "do_DELETE_restart", "admin-write", "none", "text"),
            ("DELETE", "/switchover", "do_DELETE_switchover", "admin-write", "none", "text"),
            ("POST", "/reinitialize", "do_POST_reinitialize", "admin-write", "reinitialize-json", "text"),
            ("POST", "/failover", "do_POST_failover", "availability-write", "failover-json", "text"),
            ("POST", "/switchover", "do_POST_switchover", "availability-write", "switchover-json", "text"),
            ("POST", "/citus", "do_POST_citus", "peer-internal", "mpp-event-json", "text"),
            ("POST", "/mpp", "do_POST_mpp", "peer-internal", "mpp-event-json", "text"),
        ]:
            add(method, path, handler, risk, response, request)
        if len(endpoints) != 75:
            raise SystemExit(f"internal REST inventory error: expected 75 method/path rows, got {len(endpoints)}")
        return {
            "schemaVersion": "patroni.pgsty.com/compatibility/v1alpha1",
            "kind": "PatroniRESTCompatibility",
            "generatedBy": "tools/compatgen + test/compat/oracle/extract_inventory.py",
            "source": {"commit": PINNED_COMMIT, "version": PINNED_VERSION, "file": "patroni/api.py"},
            "supportedRange": SUPPORTED_RANGE,
            "healthAliases": HEALTH_PATHS,
            "expectedEndpointCount": len(endpoints),
            "endpoints": endpoints,
            "releasePolicy": "every method/path row requires typed SDK and contract tests; raw response is mandatory",
        }

    def dcs_inventory(self) -> dict[str, Any]:
        abstract = self.text["dcs"]
        etcd3 = self.text["etcd3"]
        constants = {
            "initialize": "_INITIALIZE = 'initialize'",
            "config": "_CONFIG = 'config'",
            "members/{name}": "_MEMBERS = 'members/'",
            "leader": "_LEADER = 'leader'",
            "failover": "_FAILOVER = 'failover'",
            "history": "_HISTORY = 'history'",
            "status": "_STATUS = 'status'",
            "optime/leader": "_LEADER_OPTIME = _OPTIME + '/' + _LEADER",
            "sync": "_SYNC = 'sync'",
            "failsafe": "_FAILSAFE = 'failsafe'",
        }
        behavior = {
            "initialize": ("cluster initialization evidence", ["remove-prefix"], "none"),
            "config": ("dynamic configuration and pause state", ["config-cas", "remove-prefix"], "mod_revision"),
            "members/{name}": ("member identity, REST api_url, PostgreSQL connection and state", ["remove-prefix"], "none"),
            "leader": ("leader identity and lease", ["remove-prefix"], "none"),
            "failover": ("manual/scheduled failover or switchover state", ["fallback-cas", "flush-cas", "remove-prefix"], "mod_revision"),
            "history": ("timeline history", ["remove-prefix"], "none"),
            "status": ("leader LSN and retained slot status", ["remove-prefix"], "none"),
            "optime/leader": ("legacy leader LSN fallback", ["remove-prefix"], "none"),
            "sync": ("synchronous/quorum state", ["remove-prefix"], "none"),
            "failsafe": ("failsafe topology", ["remove-prefix"], "none"),
        }
        keys = []
        tests = {
            "inventory": ["test/compat/manifest_test.go#TestDCSInventory"],
            "differential": [
                "test/compat/dcs_oracle_test.go#TestDCSProjectionAgainstPinnedPatroni",
                "test/compat/dcs_oracle_test.go#TestDCSMutationContractAgainstPinnedPatroni",
            ],
            "integration": [
                "test/integration/dcs_etcd3_test.go#TestEtcd3PatroniSnapshotDiscoveryCASRemoveAndWatch"
            ],
        }
        for path, token in constants.items():
            line = line_of(abstract, token)
            read_use, writes, cas = behavior[path]
            keys.append({
                "path": path,
                "readUse": read_use,
                "sdkDirectWrites": writes,
                "cas": cas,
                "sourceRef": source_ref("patroni/dcs/__init__.py", line),
                "etcd3SourceRefs": [],
                "tests": tests,
                "status": "complete",
            })
        # Attach precise etcd3 mutation references without pretending unrelated keys have dedicated writes.
        mutation_needles = {
            "config": ["def set_config_value"],
            "failover": ["def set_failover_value", "def delete_cluster"],
        }
        for item in keys:
            needles = mutation_needles.get(item["path"], ["def delete_cluster"])
            item["etcd3SourceRefs"] = [source_ref("patroni/dcs/etcd3.py", line_of(etcd3, needle)) for needle in needles]
        return {
            "schemaVersion": "patroni.pgsty.com/compatibility/v1alpha1",
            "kind": "PatroniDCSCompatibility",
            "generatedBy": "tools/compatgen + test/compat/oracle/extract_inventory.py",
            "source": {
                "commit": PINNED_COMMIT,
                "version": PINNED_VERSION,
                "files": ["patroni/dcs/__init__.py", "patroni/dcs/etcd3.py"],
            },
            "backend": "etcd3",
            "otherBackendsSupported": [],
            "pathTemplates": {
                "postgresql": "/{trimmedNamespace}/{scope}/{key}",
                "mpp": "/{trimmedNamespace}/{scope}/{group}/{key}",
                "defaultNamespace": "service",
            },
            "consistency": {
                "snapshot": "linearizable etcd v3 range read with header revision",
                "watch": "start after snapshot revision; compaction requires full resnapshot",
                "remove": "exact cluster prefix delete after compatibility confirmations",
            },
            "expectedKeyCount": len(keys),
            "keys": keys,
            "unknownKeys": "retain as raw diagnostics; never treat alone as cluster discovery evidence",
            "releasePolicy": "all keys and command-scoped writes require oracle differential and real etcd3 evidence",
        }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--source", required=True, type=pathlib.Path)
    parser.add_argument("--kind", required=True, choices=["source", "patronictl", "rest-api", "dcs"])
    args = parser.parse_args()
    extractor = Extractor(args.source.resolve())
    if args.kind == "source":
        result = extractor.source_inventory()
    elif args.kind == "patronictl":
        result = extractor.patronictl_inventory()
    elif args.kind == "rest-api":
        result = extractor.rest_inventory()
    else:
        result = extractor.dcs_inventory()
    json.dump(result, sys.stdout, indent=2, sort_keys=False, ensure_ascii=False)
    sys.stdout.write("\n")


if __name__ == "__main__":
    main()
