#!/usr/bin/env python3
"""Project fixtures with the pinned Patroni DCS value objects.

This program is a compatibility oracle only. It is invoked by an oracle-tagged
Go test and is never imported or executed by SDK runtime code.
"""

import json
import sys

from patroni.dcs import ClusterConfig, Failover, Member, Status, SyncState, TimelineHistory
from patroni.utils import parse_int


def project(payload):
    output = {
        "members": [],
        "failovers": [],
        "sync_states": [],
        "statuses": [],
        "configs": [],
        "histories": [],
        "raw_clusters": [],
    }
    for item in payload["members"]:
        member = Member.from_node(7, item["name"], 9, item["value"])
        output["members"].append({
            "name": member.name,
            "conn_url": member.data.get("conn_url") or "",
            "api_url": member.data.get("api_url") or "",
            "state": member.data.get("state") or "",
            "role": member.data.get("role") or "",
        })
    for value in payload["failovers"]:
        failover = Failover.from_node(7, value)
        output["failovers"].append({
            "leader": failover.leader or "",
            "candidate": failover.candidate or "",
        })
    for value in payload["sync_states"]:
        state = SyncState.from_node(7, value)
        output["sync_states"].append({
            "leader": state.leader or "",
            "standbys": state.voters,
            "quorum": state.quorum,
        })
    for value in payload["statuses"]:
        status = Status.from_node(value)
        slots = None
        if status.slots is not None:
            slots = {name: parse_int(raw) or 0 for name, raw in status.slots.items()}
        output["statuses"].append({
            "last_lsn": status.last_lsn,
            "slots": slots,
            "retain_slots": status.retain_slots,
        })
    for value in payload["configs"]:
        output["configs"].append(ClusterConfig.from_node(7, value).data)
    for value in payload["histories"]:
        output["histories"].append(TimelineHistory.from_node(7, value).lines)
    for value in payload["raw_clusters"]:
        status = Status.from_node(value["legacy_optime"])
        try:
            failsafe = json.loads(value["failsafe"])
            if not isinstance(failsafe, dict):
                failsafe = None
        except Exception:
            failsafe = None
        output["raw_clusters"].append({
            "initialize": value["initialize"],
            "leader": value["leader"],
            "legacy_lsn": status.last_lsn,
            "failsafe": failsafe,
        })
    return output


def main():
    payload = json.load(sys.stdin)
    json.dump(project(payload), sys.stdout, sort_keys=True, separators=(",", ":"))
    sys.stdout.write("\n")


if __name__ == "__main__":
    main()
