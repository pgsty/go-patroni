#!/usr/bin/env python3
"""Extract Patroni etcd3 mutation semantics from the pinned source AST."""

import ast
import json
import pathlib
import sys


def class_methods(tree, class_name):
    for node in tree.body:
        if isinstance(node, ast.ClassDef) and node.name == class_name:
            return {item.name: item for item in node.body if isinstance(item, ast.FunctionDef)}
    raise RuntimeError(f"class not found: {class_name}")


def attribute_name(node):
    return node.attr if isinstance(node, ast.Attribute) else ""


def extract_put(method):
    calls = [node for node in ast.walk(method) if isinstance(node, ast.Call) and attribute_name(node.func) == "put"]
    if len(calls) != 1:
        raise RuntimeError(f"{method.name}: expected exactly one put call")
    call = calls[0]
    if not call.args or not isinstance(call.args[0], ast.Attribute):
        raise RuntimeError(f"{method.name}: put path is not an attribute")
    keywords = {item.arg: item.value for item in call.keywords}
    revision = keywords.get("mod_revision")
    if not isinstance(revision, ast.Name) or revision.id != "version":
        raise RuntimeError(f"{method.name}: put does not forward version as mod_revision")
    return {"method": "put", "path": call.args[0].attr, "cas": "mod_revision"}


def extract_remove(method):
    calls = [node for node in ast.walk(method) if isinstance(node, ast.Call) and attribute_name(node.func) == "retry"]
    if len(calls) != 1:
        raise RuntimeError("delete_cluster: expected exactly one retry wrapper")
    outer = calls[0]
    if len(outer.args) < 2 or not isinstance(outer.args[0], ast.Attribute) or outer.args[0].attr != "deleteprefix":
        raise RuntimeError("delete_cluster: retry target is not deleteprefix")
    if not isinstance(outer.args[1], ast.Call) or attribute_name(outer.args[1].func) != "client_path":
        raise RuntimeError("delete_cluster: prefix is not derived from client_path")
    inner = outer.args[1]
    if len(inner.args) != 1 or not isinstance(inner.args[0], ast.Constant) or inner.args[0].value != "":
        raise RuntimeError("delete_cluster: client_path argument changed")
    return {"method": "deleteprefix", "path": "client_path('')"}


def contains_string(method, value):
    return any(isinstance(node, ast.Constant) and node.value == value for node in ast.walk(method))


def main():
    source = pathlib.Path(sys.argv[1])
    tree = ast.parse(source.read_text(encoding="utf-8"), filename=str(source))
    etcd3 = class_methods(tree, "Etcd3")
    client = class_methods(tree, "Etcd3Client")
    put = client["put"]
    delete = client["deleterange"]
    if not all(contains_string(put, value) for value in ("CREATE", "MOD")):
        raise RuntimeError("Etcd3Client.put no longer contains CREATE/MOD transaction targets")
    if not contains_string(delete, "MOD"):
        raise RuntimeError("Etcd3Client.deleterange no longer contains MOD transaction target")
    output = {
        "config": extract_put(etcd3["set_config_value"]),
        "failover": extract_put(etcd3["set_failover_value"]),
        "remove": extract_remove(etcd3["delete_cluster"]),
        "client_put_compare_targets": ["CREATE", "MOD"],
        "client_delete_compare_target": "MOD",
    }
    json.dump(output, sys.stdout, sort_keys=True, separators=(",", ":"))
    sys.stdout.write("\n")


if __name__ == "__main__":
    main()
